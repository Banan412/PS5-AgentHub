use ps5_agent_config::AgentsConfig;
use ps5_agent_core::{
    new_id, now_iso, AgentCardView, AgentDefinition, AgentError, AgentModelConfig, AgentRole,
    AgentRuntimeStats, ApiMode, AuditLogEntry, MemoryPermissions, Result, StorageBackend,
    StreamEvent, TaskRecord, TaskStatus, ThinkingLevel,
};
use ps5_agent_logging::{log_info, log_warn};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::broadcast;

pub const BUILTIN_AGENTS_SCHEMA_VERSION: &str = "7";

fn sha256_hex(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    hex::encode(hasher.finalize())
}

fn tools_list_hash(tools: &[String]) -> String {
    let mut hasher = Sha256::new();
    for tool in tools {
        hasher.update(tool.as_bytes());
        hasher.update(b"\0");
    }
    hex::encode(hasher.finalize())
}

fn normalize_optional_field(raw: String) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

/// Computes a deterministic SHA-256 fingerprint of the built-in agent templates
/// so startup can detect when a new `.elf` binary ships updated prompts, tools,
/// or default model budgets and migrate untouched built-in agents in place.
pub fn builtin_agents_fingerprint() -> String {
    let mut hasher = Sha256::new();
    hasher.update(BUILTIN_AGENTS_SCHEMA_VERSION.as_bytes());
    for def in default_agent_definitions() {
        hasher.update(b"\n");
        hasher.update(def.id.as_bytes());
        hasher.update(b"|");
        hasher.update(def.name.as_bytes());
        hasher.update(b"|");
        hasher.update(def.description.as_bytes());
        hasher.update(b"|");
        hasher.update(def.role_label.as_bytes());
        hasher.update(b"|");
        hasher.update(def.system_prompt.as_bytes());
        hasher.update(b"|");
        for tool in &def.enabled_tools {
            hasher.update(tool.as_bytes());
            hasher.update(b",");
        }
        hasher.update(b"|denied:");
        for tool in &def.denied_tools {
            hasher.update(tool.as_bytes());
            hasher.update(b",");
        }
        hasher.update(b"|sb:");
        if let Some(ref mode) = def.sandbox_mode {
            hasher.update(mode.as_bytes());
        }
        hasher.update(b"|prof:");
        if let Some(ref profile) = def.tool_profile {
            hasher.update(profile.as_bytes());
        }
        if let Ok(model_json) = serde_json::to_string(&def.model_config) {
            hasher.update(model_json.as_bytes());
        }
        hasher.update(format!("|{}|{}", def.max_task_duration_secs, def.max_reasoning_iterations).as_bytes());
    }
    hex::encode(hasher.finalize())
}

/// Hook implemented by the runtime's TaskManager so AgentManager can cancel live
/// task CancellationTokens without introducing a cyclic crate dependency.
pub trait AgentTaskController: Send + Sync {
    fn cancel_agent_tasks(&self, agent_id: &str, reason: &str) -> Result<usize>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateAgentInput {
    pub id: Option<String>,
    pub name: String,
    pub description: String,
    pub role: AgentRole,
    pub role_label: Option<String>,
    pub system_prompt: String,
    pub model_config: Option<AgentModelConfig>,
    pub enabled_tools: Vec<String>,
    #[serde(default)]
    pub denied_tools: Option<Vec<String>>,
    pub memory_permissions: Option<MemoryPermissions>,
    pub max_task_duration_secs: Option<u64>,
    pub max_reasoning_iterations: Option<u32>,
    pub priority: Option<i32>,
    pub can_communicate: Option<bool>,
    pub enabled: Option<bool>,
    #[serde(default)]
    pub sandbox_mode: Option<String>,
    #[serde(default)]
    pub tool_profile: Option<String>,
    pub is_temporary: bool,
    pub spawned_by: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UpdateAgentInput {
    pub name: Option<String>,
    pub description: Option<String>,
    pub role_label: Option<String>,
    pub system_prompt: Option<String>,
    pub model_config: Option<AgentModelConfig>,
    pub enabled_tools: Option<Vec<String>>,
    pub denied_tools: Option<Vec<String>>,
    pub memory_permissions: Option<MemoryPermissions>,
    pub max_task_duration_secs: Option<u64>,
    pub max_reasoning_iterations: Option<u32>,
    pub priority: Option<i32>,
    pub can_communicate: Option<bool>,
    pub sandbox_mode: Option<String>,
    pub tool_profile: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActiveTaskPolicy {
    RejectIfActive,
    CancelActive,
    WaitForActive { timeout_ms: u64 },
}

#[derive(Debug, Clone)]
pub struct DeleteAgentOptions {
    pub active_task_policy: ActiveTaskPolicy,
    pub delete_memory: bool,
    pub actor: String,
}

impl Default for DeleteAgentOptions {
    fn default() -> Self {
        Self {
            active_task_policy: ActiveTaskPolicy::RejectIfActive,
            delete_memory: false,
            actor: "admin".to_string(),
        }
    }
}

pub struct AgentManager {
    storage: Arc<dyn StorageBackend>,
    config: RwLock<AgentsConfig>,
    events_tx: broadcast::Sender<StreamEvent>,
    task_controller: RwLock<Option<Arc<dyn AgentTaskController>>>,
    live_stats: RwLock<HashMap<String, AgentRuntimeStats>>,
}

impl AgentManager {
    pub fn new(
        storage: Arc<dyn StorageBackend>,
        config: AgentsConfig,
        events_tx: broadcast::Sender<StreamEvent>,
    ) -> Self {
        Self {
            storage,
            config: RwLock::new(config),
            events_tx,
            task_controller: RwLock::new(None),
            live_stats: RwLock::new(HashMap::new()),
        }
    }

    pub fn attach_task_controller(&self, controller: Arc<dyn AgentTaskController>) {
        if let Ok(mut guard) = self.task_controller.write() {
            *guard = Some(controller);
        }
    }

    pub fn update_config(&self, config: AgentsConfig) {
        if let Ok(mut guard) = self.config.write() {
            *guard = config;
        }
    }

    pub fn config(&self) -> AgentsConfig {
        self.config
            .read()
            .map(|g| g.clone())
            .unwrap_or_else(|_| ps5_agent_config::AppConfig::default().agents)
    }

    pub fn record_task_started(&self, agent_id: &str, task_id: &str) {
        if let Ok(mut guard) = self.live_stats.write() {
            let entry = guard
                .entry(agent_id.to_string())
                .or_insert_with(|| AgentRuntimeStats {
                    agent_id: agent_id.to_string(),
                    ..Default::default()
                });
            entry.current_task_id = Some(task_id.to_string());
            entry.last_activity_at = Some(now_iso());
        }
    }

    pub fn record_task_finished(&self, agent_id: &str, _success: bool) {
        if let Ok(mut guard) = self.live_stats.write() {
            let entry = guard
                .entry(agent_id.to_string())
                .or_insert_with(|| AgentRuntimeStats {
                    agent_id: agent_id.to_string(),
                    ..Default::default()
                });
            entry.current_task_id = None;
            entry.last_activity_at = Some(now_iso());
        }
    }

    fn record_builtin_baseline(&self, def: &AgentDefinition) -> Result<()> {
        let prompt_key = format!("builtin_prompt_hash.{}", def.id);
        self.storage
            .set_setting(&prompt_key, &sha256_hex(&def.system_prompt))?;
        let tools_key = format!("builtin_tools_hash.{}", def.id);
        self.storage
            .set_setting(&tools_key, &tools_list_hash(&def.enabled_tools))?;
        let known_tools_key = format!("builtin_known_tools.{}", def.id);
        let known_json = serde_json::to_string(&def.enabled_tools).unwrap_or_default();
        self.storage.set_setting(&known_tools_key, &known_json)?;
        Ok(())
    }

    pub fn seed_defaults(&self) -> Result<usize> {
        let already_seeded = self
            .storage
            .get_setting("agents_seeded")?
            .as_deref()
            == Some("true");
        let existing = self.storage.list_agents()?;
        let mut seeded_count = 0;

        if !already_seeded && existing.is_empty() {
            let current_fp = builtin_agents_fingerprint();
            for def in default_agent_definitions() {
                self.storage.upsert_agent(&def)?;
                self.record_builtin_baseline(&def)?;
                seeded_count += 1;
            }
            self.storage.set_setting("agents_seeded", "true")?;
            self.storage.set_setting(
                "builtin_agents_schema_version",
                BUILTIN_AGENTS_SCHEMA_VERSION,
            )?;
            self.storage
                .set_setting("builtin_agents_hash", &current_fp)?;
            log_info("manager", format!("seeded {seeded_count} default agents"));
        } else {
            let defs = default_agent_definitions();
            if !already_seeded {
                // Partial unseeded DB: restore any missing built-in defaults without overwriting existing rows.
                for def in &defs {
                    if self.storage.get_agent(&def.id)?.is_none() {
                        self.storage.upsert_agent(def)?;
                        self.record_builtin_baseline(def)?;
                        seeded_count += 1;
                    }
                }
            } else if self.storage.get_agent("commander")?.is_none() {
                // Ensure protected commander always exists even on partial state.
                if let Some(cmd) = defs.into_iter().find(|a| a.id == "commander") {
                    self.storage.upsert_agent(&cmd)?;
                    self.record_builtin_baseline(&cmd)?;
                    seeded_count += 1;
                }
            }
            let _upgraded = self.upgrade_builtin_agents()?;
        }
        Ok(seeded_count)
    }

    /// Non-destructively upgrades existing built-in agents when a newer release
    /// changes built-in prompts, default tools, or model budgets.
    pub fn upgrade_builtin_agents(&self) -> Result<usize> {
        let current_fp = builtin_agents_fingerprint();
        if self
            .storage
            .get_setting("builtin_agents_hash")?
            .as_deref()
            == Some(current_fp.as_str())
        {
            return Ok(0);
        }

        let mut upgraded = 0usize;
        for def in default_agent_definitions() {
            let Some(mut cur) = self.storage.get_agent(&def.id)? else {
                continue;
            };
            if !cur.is_builtin {
                continue;
            }

            let prompt_key = format!("builtin_prompt_hash.{}", def.id);
            let prev_prompt_hash = self.storage.get_setting(&prompt_key)?;
            let cur_prompt_hash = sha256_hex(&cur.system_prompt);
            let unedited_builtin = cur.created_at == cur.updated_at
                || prev_prompt_hash.as_deref() == Some(cur_prompt_hash.as_str());

            let tools_key = format!("builtin_tools_hash.{}", def.id);
            let prev_tools_hash = self.storage.get_setting(&tools_key)?;
            let cur_tools_hash = tools_list_hash(&cur.enabled_tools);
            let known_tools_key = format!("builtin_known_tools.{}", def.id);
            let prev_known_tools: Option<Vec<String>> = self
                .storage
                .get_setting(&known_tools_key)?
                .and_then(|raw| serde_json::from_str(&raw).ok());

            let mut changed = false;
            if unedited_builtin {
                if cur.name != def.name {
                    cur.name = def.name.clone();
                    changed = true;
                }
                if cur.description != def.description {
                    cur.description = def.description.clone();
                    changed = true;
                }
                if cur.role_label != def.role_label {
                    cur.role_label = def.role_label.clone();
                    changed = true;
                }
                if cur.system_prompt != def.system_prompt {
                    cur.system_prompt = def.system_prompt.clone();
                    changed = true;
                }
                if cur.model_config != def.model_config {
                    cur.model_config = def.model_config.clone();
                    changed = true;
                }
                if cur.max_task_duration_secs != 0
                    && cur.max_task_duration_secs < def.max_task_duration_secs
                {
                    cur.max_task_duration_secs = def.max_task_duration_secs;
                    changed = true;
                }
                if cur.max_reasoning_iterations != 0
                    && cur.max_reasoning_iterations < def.max_reasoning_iterations
                {
                    cur.max_reasoning_iterations = def.max_reasoning_iterations;
                    changed = true;
                }
            }

            if def.max_task_duration_secs == 0
                && matches!(cur.max_task_duration_secs, 180 | 240)
            {
                cur.max_task_duration_secs = 0;
                changed = true;
            }
            if def.max_reasoning_iterations == 0
                && matches!(cur.max_reasoning_iterations, 6 | 8)
            {
                cur.max_reasoning_iterations = 0;
                changed = true;
            }

            if prev_tools_hash.as_deref() == Some(cur_tools_hash.as_str()) {
                if cur.enabled_tools != def.enabled_tools {
                    cur.enabled_tools = def.enabled_tools.clone();
                    changed = true;
                }
            } else {
                for tool in &def.enabled_tools {
                    let previously_known = prev_known_tools
                        .as_ref()
                        .is_some_and(|known| known.iter().any(|k| k == tool));
                    if !previously_known
                        && !cur.enabled_tools.iter().any(|existing| existing == tool)
                    {
                        cur.enabled_tools.push(tool.clone());
                        changed = true;
                    }
                }
            }

            if changed {
                cur.updated_at = now_iso();
                self.storage.upsert_agent(&cur)?;
                upgraded += 1;
                self.emit_agent_updated(&cur.id, "upgraded");
            }

            self.record_builtin_baseline(&def)?;
        }

        self.storage.set_setting("agents_seeded", "true")?;
        self.storage.set_setting(
            "builtin_agents_schema_version",
            BUILTIN_AGENTS_SCHEMA_VERSION,
        )?;
        self.storage
            .set_setting("builtin_agents_hash", &current_fp)?;

        if upgraded > 0 {
            self.append_audit(
                "system",
                "system.builtin_agents_upgrade",
                "builtin_agents",
                "info",
                "ok",
                serde_json::json!({
                    "upgraded_count": upgraded,
                    "schema_version": BUILTIN_AGENTS_SCHEMA_VERSION,
                }),
            );
            log_info(
                "manager",
                format!("upgraded {upgraded} built-in agent(s) to current release"),
            );
        }

        Ok(upgraded)
    }

    pub fn get_agent(&self, agent_id: &str) -> Result<Option<AgentDefinition>> {
        self.storage.get_agent(agent_id)
    }

    pub fn list_agents(&self) -> Result<Vec<AgentDefinition>> {
        let mut agents = self.storage.list_agents()?;
        agents.sort_by(|a, b| {
            b.priority
                .cmp(&a.priority)
                .then_with(|| a.name.cmp(&b.name))
                .then_with(|| a.id.cmp(&b.id))
        });
        Ok(agents)
    }

    pub fn create_agent(&self, input: CreateAgentInput, actor: &str) -> Result<AgentDefinition> {
        let name = input.name.trim().to_string();
        if name.is_empty() {
            return Err(AgentError::InvalidInput("agent name cannot be empty".into()));
        }
        let system_prompt = input.system_prompt.trim().to_string();
        if system_prompt.is_empty() {
            return Err(AgentError::InvalidInput(
                "agent system_prompt cannot be empty".into(),
            ));
        }

        let cfg = self.config();
        if input.is_temporary && !cfg.allow_temporary_specialists {
            return Err(AgentError::PermissionDenied(
                "temporary specialist agents are disabled by configuration".into(),
            ));
        }

        let existing = self.storage.list_agents()?;
        if cfg.max_agents > 0 && existing.len() >= cfg.max_agents {
            return Err(AgentError::LimitExceeded(format!(
                "max_agents limit ({}) reached",
                cfg.max_agents
            )));
        }

        let id = match input.id {
            Some(raw) => sanitize_agent_id(&raw)?,
            None => generate_agent_id(&name, &existing),
        };
        if self.storage.get_agent(&id)?.is_some() {
            return Err(AgentError::Conflict(format!(
                "agent '{id}' already exists"
            )));
        }

        let raw_iters = input
            .max_reasoning_iterations
            .unwrap_or(cfg.max_reasoning_iterations);
        let max_iters = if raw_iters > 0 { raw_iters } else { 0 };
        let raw_duration = input
            .max_task_duration_secs
            .unwrap_or(cfg.default_task_timeout_secs);
        let max_duration = if raw_duration == 0 {
            0
        } else {
            raw_duration.max(5)
        };
        let now = now_iso();

        let agent = AgentDefinition {
            id: id.clone(),
            name,
            description: input.description.trim().to_string(),
            role_label: input
                .role_label
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| default_role_label(&input.role).to_string()),
            role: input.role,
            system_prompt,
            model_config: input.model_config.unwrap_or_default(),
            enabled_tools: input.enabled_tools,
            denied_tools: input.denied_tools.unwrap_or_default(),
            memory_permissions: input.memory_permissions.unwrap_or_default(),
            max_task_duration_secs: max_duration,
            max_reasoning_iterations: max_iters,
            priority: input.priority.unwrap_or(50),
            can_communicate: input.can_communicate.unwrap_or(true),
            enabled: input.enabled.unwrap_or(true),
            sandbox_mode: input.sandbox_mode.and_then(normalize_optional_field),
            tool_profile: input.tool_profile.and_then(normalize_optional_field),
            is_builtin: false,
            is_protected: false,
            is_temporary: input.is_temporary,
            created_at: now.clone(),
            updated_at: now,
        };

        self.storage.upsert_agent(&agent)?;
        self.append_audit(
            actor,
            "agent.create",
            &agent.id,
            "info",
            "ok",
            serde_json::json!({
                "name": agent.name,
                "role": agent.role.as_str(),
                "is_temporary": agent.is_temporary,
                "spawned_by": input.spawned_by,
            }),
        );
        self.emit_agent_updated(&agent.id, "created");
        log_info("manager", format!("created agent '{}' by '{actor}'", agent.id));
        Ok(agent)
    }

    pub fn update_agent(
        &self,
        agent_id: &str,
        input: UpdateAgentInput,
        actor: &str,
    ) -> Result<AgentDefinition> {
        let mut agent = self
            .storage
            .get_agent(agent_id)?
            .ok_or_else(|| AgentError::NotFound(format!("agent '{agent_id}' not found")))?;

        if let Some(name) = input.name {
            let trimmed = name.trim();
            if trimmed.is_empty() {
                return Err(AgentError::InvalidInput("agent name cannot be empty".into()));
            }
            agent.name = trimmed.to_string();
        }
        if let Some(desc) = input.description {
            agent.description = desc.trim().to_string();
        }
        if let Some(label) = input.role_label {
            if !label.trim().is_empty() {
                agent.role_label = label.trim().to_string();
            }
        }
        if let Some(prompt) = input.system_prompt {
            let trimmed = prompt.trim();
            if trimmed.is_empty() {
                return Err(AgentError::InvalidInput(
                    "system_prompt cannot be empty".into(),
                ));
            }
            agent.system_prompt = trimmed.to_string();
        }
        if let Some(model_cfg) = input.model_config {
            agent.model_config = model_cfg;
        }
        if let Some(tools) = input.enabled_tools {
            agent.enabled_tools = tools;
        }
        if let Some(denied) = input.denied_tools {
            agent.denied_tools = denied;
        }
        if let Some(perms) = input.memory_permissions {
            agent.memory_permissions = perms;
        }
        if let Some(dur) = input.max_task_duration_secs {
            agent.max_task_duration_secs = if dur == 0 { 0 } else { dur.max(5) };
        }
        if let Some(iters) = input.max_reasoning_iterations {
            agent.max_reasoning_iterations = if iters > 0 { iters } else { 0 };
        }
        if let Some(priority) = input.priority {
            agent.priority = priority;
        }
        if let Some(can_comm) = input.can_communicate {
            agent.can_communicate = can_comm;
        }
        if let Some(mode) = input.sandbox_mode {
            agent.sandbox_mode = normalize_optional_field(mode);
        }
        if let Some(profile) = input.tool_profile {
            agent.tool_profile = normalize_optional_field(profile);
        }
        agent.updated_at = now_iso();

        self.storage.upsert_agent(&agent)?;
        self.append_audit(
            actor,
            "agent.update",
            &agent.id,
            "info",
            "ok",
            serde_json::json!({ "name": agent.name, "priority": agent.priority }),
        );
        self.emit_agent_updated(&agent.id, "updated");
        Ok(agent)
    }

    pub fn set_enabled(
        &self,
        agent_id: &str,
        enabled: bool,
        actor: &str,
    ) -> Result<AgentDefinition> {
        let mut agent = self
            .storage
            .get_agent(agent_id)?
            .ok_or_else(|| AgentError::NotFound(format!("agent '{agent_id}' not found")))?;

        agent.enabled = enabled;
        agent.updated_at = now_iso();
        self.storage.upsert_agent(&agent)?;

        if !enabled && self.config().on_disable_policy == "cancel" {
            self.cancel_agent_tasks(agent_id, "agent disabled")?;
        }

        let action = if enabled { "enabled" } else { "disabled" };
        self.append_audit(
            actor,
            if enabled { "agent.enable" } else { "agent.disable" },
            agent_id,
            "info",
            "ok",
            serde_json::json!({ "enabled": enabled }),
        );
        self.emit_agent_updated(agent_id, action);
        log_info("manager", format!("agent '{agent_id}' {action} by '{actor}'"));
        Ok(agent)
    }

    pub fn enable_all_agents(&self) -> Result<usize> {
        let agents = self.storage.list_agents()?;
        let mut enabled_count = 0usize;
        let now = now_iso();
        for mut agent in agents {
            if !agent.enabled {
                agent.enabled = true;
                agent.updated_at = now.clone();
                self.storage.upsert_agent(&agent)?;
                self.emit_agent_updated(&agent.id, "enabled");
                enabled_count += 1;
            }
        }
        if enabled_count > 0 {
            self.append_audit(
                "system",
                "system.enable_all_agents",
                "agents",
                "info",
                "ok",
                serde_json::json!({ "enabled_count": enabled_count }),
            );
            log_info("manager", format!("enabled {enabled_count} agent(s)"));
        }
        Ok(enabled_count)
    }

    pub async fn delete_agent(&self, agent_id: &str, opts: DeleteAgentOptions) -> Result<bool> {
        let agent = self
            .storage
            .get_agent(agent_id)?
            .ok_or_else(|| AgentError::NotFound(format!("agent '{agent_id}' not found")))?;

        if agent.is_protected || agent.id == "commander" {
            return Err(AgentError::Protected(format!(
                "agent '{}' is protected and cannot be deleted",
                agent.id
            )));
        }

        let active_tasks = self.active_tasks_for_agent(agent_id)?;
        if !active_tasks.is_empty() {
            match opts.active_task_policy {
                ActiveTaskPolicy::RejectIfActive => {
                    return Err(AgentError::Conflict(format!(
                        "agent '{agent_id}' has {} active task(s)",
                        active_tasks.len()
                    )));
                }
                ActiveTaskPolicy::CancelActive => {
                    self.cancel_agent_tasks(agent_id, "agent deleted")?;
                }
                ActiveTaskPolicy::WaitForActive { timeout_ms } => {
                    let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
                    let mut backoff = Duration::from_millis(25);
                    loop {
                        if !self.has_active_tasks_for_agent(agent_id)? {
                            break;
                        }
                        let now = tokio::time::Instant::now();
                        if now >= deadline {
                            return Err(AgentError::Conflict(format!(
                                "timed out waiting for active tasks on agent '{agent_id}'"
                            )));
                        }
                        let remaining = deadline.saturating_duration_since(now);
                        let sleep_dur = backoff.min(remaining);
                        tokio::time::sleep(sleep_dur).await;
                        backoff = (backoff.saturating_mul(2)).min(Duration::from_millis(100));
                    }
                }
            }
        }

        let deleted_memories = if opts.delete_memory {
            self.storage.delete_agent_memories(agent_id)?
        } else {
            0
        };

        let removed = self.storage.delete_agent(agent_id)?;
        if removed {
            self.remove_live_stats(agent_id);
            self.append_audit(
                &opts.actor,
                "agent.delete",
                agent_id,
                "warn",
                "ok",
                serde_json::json!({
                    "delete_memory": opts.delete_memory,
                    "deleted_memories": deleted_memories,
                }),
            );
            self.emit_agent_updated(agent_id, "deleted");
            log_warn("manager", format!("deleted agent '{agent_id}' by '{}'", opts.actor));
        }
        Ok(removed)
    }

    pub async fn cleanup_temporary_agent(&self, agent_id: &str) -> Result<()> {
        self.remove_live_stats(agent_id);
        let Some(agent) = self.storage.get_agent(agent_id)? else {
            return Ok(());
        };
        if !agent.is_temporary || agent.is_protected || agent.id == "commander" {
            return Ok(());
        }
        let _ = self.storage.delete_agent_memories(agent_id)?;
        if self.storage.delete_agent(agent_id)? {
            self.emit_agent_updated(agent_id, "deleted");
        }
        Ok(())
    }

    fn remove_live_stats(&self, agent_id: &str) {
        if let Ok(mut stats) = self.live_stats.write() {
            stats.remove(agent_id);
            if stats.capacity() > 16 && stats.len().saturating_mul(4) < stats.capacity() {
                stats.shrink_to_fit();
            }
        }
    }

    pub fn list_cards(&self) -> Result<Vec<AgentCardView>> {
        let agents = self.list_agents()?;
        let summary = self.storage.task_counts_summary()?;
        let live = self.live_stats.read().ok().map(|g| g.clone()).unwrap_or_default();

        let mut cards = Vec::with_capacity(agents.len());
        for def in agents {
            let mut stats = AgentRuntimeStats {
                agent_id: def.id.clone(),
                ..Default::default()
            };

            if let Some(agent_summary) = summary.per_agent.get(&def.id) {
                stats.completed_tasks = agent_summary.completed as u64;
                stats.failed_tasks = agent_summary.failed as u64;
                stats.current_task_id = agent_summary.latest_active_task_id.clone();
                stats.last_activity_at = agent_summary.last_activity.clone();
            }

            if let Some(live_entry) = live.get(&def.id) {
                if live_entry.current_task_id.is_some() {
                    stats.current_task_id = live_entry.current_task_id.clone();
                }
                if let Some(ref ts) = live_entry.last_activity_at {
                    if stats
                        .last_activity_at
                        .as_ref()
                        .map_or(true, |cur| ts.as_str() > cur.as_str())
                    {
                        stats.last_activity_at = Some(ts.clone());
                    }
                }
            }

            let (mem_entries, mem_bytes) = self.storage.memory_stats_for_agent(&def.id)?;
            stats.memory_entries = mem_entries;
            stats.memory_bytes = mem_bytes;

            cards.push(AgentCardView {
                definition: def,
                stats,
            });
        }
        Ok(cards)
    }

    fn has_active_tasks_for_agent(&self, agent_id: &str) -> Result<bool> {
        let summary = self.storage.task_counts_summary()?;
        if let Some(agent_stats) = summary.per_agent.get(agent_id) {
            if agent_stats.active > 0 {
                return Ok(true);
            }
        }
        if summary.pending == 0 {
            return Ok(false);
        }
        let tasks = self.storage.list_tasks(200)?;
        Ok(tasks.iter().any(|t| {
            t.assigned_agent_id == agent_id
                && (t.status.is_active() || t.status == TaskStatus::Pending)
        }))
    }

    fn active_tasks_for_agent(&self, agent_id: &str) -> Result<Vec<TaskRecord>> {
        let tasks = self.storage.list_tasks(1000)?;
        Ok(tasks
            .into_iter()
            .filter(|t| {
                t.assigned_agent_id == agent_id
                    && (t.status.is_active() || t.status == TaskStatus::Pending)
            })
            .collect())
    }

    fn cancel_agent_tasks(&self, agent_id: &str, reason: &str) -> Result<usize> {
        let controller = self
            .task_controller
            .read()
            .ok()
            .and_then(|g| g.clone());
        if let Some(ctrl) = controller {
            return ctrl.cancel_agent_tasks(agent_id, reason);
        }

        // Fallback when no live TaskManager controller is attached: mark storage tasks cancelled.
        let mut count = 0;
        for mut task in self.active_tasks_for_agent(agent_id)? {
            task.status = TaskStatus::Cancelled;
            task.completed_at = Some(now_iso());
            task.error_info = Some(reason.to_string());
            self.storage.upsert_task(&task)?;
            count += 1;
        }
        Ok(count)
    }

    fn emit_agent_updated(&self, agent_id: &str, action: &str) {
        let _ = self.events_tx.send(StreamEvent::AgentUpdated {
            agent_id: agent_id.to_string(),
            action: action.to_string(),
        });
    }

    fn append_audit(
        &self,
        actor: &str,
        action: &str,
        target: &str,
        severity: &str,
        outcome: &str,
        details: serde_json::Value,
    ) {
        let _ = self.storage.append_audit_log(&AuditLogEntry {
            id: new_id(),
            timestamp: now_iso(),
            actor: actor.to_string(),
            action: action.to_string(),
            target: target.to_string(),
            severity: severity.to_string(),
            outcome: outcome.to_string(),
            details_json: details.to_string(),
        });
    }
}

fn default_role_label(role: &AgentRole) -> &'static str {
    match role {
        AgentRole::Commander => "Commander",
        AgentRole::Researcher => "Researcher",
        AgentRole::Programmer => "Programmer",
        AgentRole::Analyst => "Analyst",
        AgentRole::Executor => "Executor",
        AgentRole::Custom => "Specialist",
    }
}

fn sanitize_agent_id(raw: &str) -> Result<String> {
    let trimmed = raw.trim().to_ascii_lowercase();
    if trimmed.is_empty() || trimmed.len() > 64 {
        return Err(AgentError::InvalidInput(
            "agent id must be 1..=64 characters".into(),
        ));
    }
    if !trimmed
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(AgentError::InvalidInput(
            "agent id must contain only ASCII alphanumeric, '-' or '_'".into(),
        ));
    }
    Ok(trimmed)
}

fn generate_agent_id(name: &str, existing: &[AgentDefinition]) -> String {
    let base: String = name
        .trim()
        .to_ascii_lowercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect::<String>()
        .trim_matches('-')
        .to_string();
    let base = if base.is_empty() {
        "agent".to_string()
    } else {
        base.chars().take(32).collect()
    };
    if !existing.iter().any(|a| a.id == base) {
        return base;
    }
    format!("{base}-{}", &new_id()[..6])
}

pub fn default_agent_definitions() -> Vec<AgentDefinition> {
    let now = now_iso();
    vec![
        AgentDefinition {
            id: "commander".to_string(),
            name: "Commander".to_string(),
            description: "Lead orchestrator that plans multi-step workflows, delegates to specialists, and synthesizes final answers.".to_string(),
            role: AgentRole::Commander,
            role_label: "Commander".to_string(),
            system_prompt: "I am Commander, the lead orchestrator of this PS5 multi-agent runtime. I always speak directly in the first person ('I...'), never in third person. For greetings, quick checks (like 'Test'), or casual/playful prompts, I respond naturally and concisely in 1-3 sentences without dumping a list of tools or refusing benign requests. For complex or specialized requests, I either handle them directly or delegate focused sub-tasks via assign_agent_task or send_agent_message, verify results, and give a clear answer. When asked to create, build, or write code, a game, an app, a webpage, or a script (for example, a Python script, Bash/PowerShell script, or HTML game), I NEVER draft or write the code or script myself in my thoughts or response, and I NEVER call write_file to create code/scripts myself; instead, I ALWAYS delegate the task immediately to programmer using assign_agent_task (target_agent_id: \"programmer\") so Programmer writes and saves the complete working file, and then I briefly present Programmer's result and saved file path to the user. When asked to schedule recurring, periodic, delayed, or timed tasks to run in the background, I use schedule_create, schedule_list, schedule_update, and schedule_delete (or delegate scheduled execution to Executor when needed).".to_string(),
            model_config: AgentModelConfig {
                provider: "ps5lm".to_string(),
                model_override: None,
                api_mode: ApiMode::ChatCompletions,
                temperature: 0.35,
                max_output_tokens: 0,
                context_limit: 8192,
                thinking_level: ThinkingLevel::Medium,
            },
            enabled_tools: vec![
                "read_file".into(),
                "write_file".into(),
                "edit_file".into(),
                "apply_patch".into(),
                "create_directory".into(),
                "delete_file".into(),
                "list_files".into(),
                "grep_search".into(),
                "glob_files".into(),
                "sandbox_exec".into(),
                "process".into(),
                "web_search".into(),
                "web_fetch".into(),
                "browser".into(),
                "http_get".into(),
                "fetch_website".into(),
                "json_api_request".into(),
                "extract_html_text".into(),
                "memory_read".into(),
                "memory_write".into(),
                "memory_search".into(),
                "memory_get".into(),
                "sessions_list".into(),
                "sessions_spawn".into(),
                "sessions_send".into(),
                "session_status".into(),
                "assign_agent_task".into(),
                "send_agent_message".into(),
                "query_agent_status".into(),
                "request_temporary_specialist".into(),
                "write_artifact".into(),
                "doctor".into(),
                "system_info".into(),
                "ps5_info".into(),
                "schedule_create".into(),
                "schedule_list".into(),
                "schedule_update".into(),
                "schedule_delete".into(),
                "calculator".into(),
                "datetime".into(),
                "json_process".into(),
                "text_process".into(),
                "load_skill".into(),
                "mcp:*".into(),
                "plugin:*".into(),
                "custom:*".into(),
            ],
            denied_tools: Vec::new(),
            memory_permissions: MemoryPermissions {
                read_own: true,
                write_own: true,
                read_shared: true,
                write_shared: true,
                read_user_prefs: true,
                write_user_prefs: true,
                cross_agent_read: true,
            },
            max_task_duration_secs: 0,
            max_reasoning_iterations: 0,
            priority: 100,
            can_communicate: true,
            enabled: true,
            sandbox_mode: None,
            tool_profile: None,
            is_builtin: true,
            is_protected: true,
            is_temporary: false,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        AgentDefinition {
            id: "researcher".to_string(),
            name: "Researcher".to_string(),
            description: "Information-gathering specialist for web research, documentation lookup, and read-only file inspection.".to_string(),
            role: AgentRole::Researcher,
            role_label: "Researcher".to_string(),
            system_prompt: "I am Researcher, focused on web research, documentation lookup, and read-only file inspection. I always speak directly in the first person ('I...'), never in third person. For greetings, quick checks, or casual/playful prompts, I respond naturally and concisely in 1-3 sentences without dumping a list of tools or refusing benign requests. When gathering facts from allowed web sources or workspace files, I cite evidence concisely and never fabricate unverified details.".to_string(),
            model_config: AgentModelConfig {
                temperature: 0.25,
                max_output_tokens: 0,
                thinking_level: ThinkingLevel::Medium,
                ..Default::default()
            },
            enabled_tools: vec![
                "web_search".into(),
                "web_fetch".into(),
                "browser".into(),
                "http_get".into(),
                "extract_html_text".into(),
                "json_api_request".into(),
                "read_file".into(),
                "list_files".into(),
                "grep_search".into(),
                "glob_files".into(),
                "memory_read".into(),
                "memory_write".into(),
                "memory_search".into(),
                "memory_get".into(),
                "write_artifact".into(),
                "datetime".into(),
                "json_process".into(),
                "text_process".into(),
                "load_skill".into(),
                "mcp:*".into(),
                "plugin:*".into(),
                "custom:*".into(),
            ],
            denied_tools: Vec::new(),
            memory_permissions: MemoryPermissions::default(),
            max_task_duration_secs: 0,
            max_reasoning_iterations: 0,
            priority: 70,
            can_communicate: true,
            enabled: true,
            sandbox_mode: None,
            tool_profile: None,
            is_builtin: true,
            is_protected: false,
            is_temporary: false,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        AgentDefinition {
            id: "programmer".to_string(),
            name: "Programmer".to_string(),
            description: "Software engineering specialist for code generation, debugging, refactoring, and workspace file edits.".to_string(),
            role: AgentRole::Programmer,
            role_label: "Programmer".to_string(),
            system_prompt: "I am Programmer, focused on building software, debugging, refactoring, and editing workspace files. I always speak directly in the first person ('I...'), never in third person. For greetings, quick checks (like 'Test'), or casual/playful prompts, I respond naturally and concisely in 1-3 sentences without dumping a list of tools or refusing benign requests. When asked to create, build, or write code, a game, an app, a webpage, or a script (for example, an HTML Flappy Bird game), I always use write_file to save a complete, standalone, 100%-working file in the workspace (like flappy-bird.html) instead of pasting partial code snippets in chat, and briefly confirm the filename and how to run or preview it. Before editing existing code, I inspect it with read_file/list_files and report exact changes made.".to_string(),
            model_config: AgentModelConfig {
                temperature: 0.25,
                max_output_tokens: 0,
                thinking_level: ThinkingLevel::High,
                ..Default::default()
            },
            enabled_tools: vec![
                "read_file".into(),
                "write_file".into(),
                "edit_file".into(),
                "apply_patch".into(),
                "create_directory".into(),
                "delete_file".into(),
                "list_files".into(),
                "grep_search".into(),
                "glob_files".into(),
                "sandbox_exec".into(),
                "process".into(),
                "web_search".into(),
                "web_fetch".into(),
                "http_get".into(),
                "json_process".into(),
                "calculator".into(),
                "memory_read".into(),
                "memory_write".into(),
                "memory_search".into(),
                "memory_get".into(),
                "write_artifact".into(),
                "load_skill".into(),
                "mcp:*".into(),
                "plugin:*".into(),
                "custom:*".into(),
            ],
            denied_tools: Vec::new(),
            memory_permissions: MemoryPermissions::default(),
            max_task_duration_secs: 0,
            max_reasoning_iterations: 0,
            priority: 70,
            can_communicate: true,
            enabled: true,
            sandbox_mode: None,
            tool_profile: None,
            is_builtin: true,
            is_protected: false,
            is_temporary: false,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        AgentDefinition {
            id: "analyst".to_string(),
            name: "Analyst".to_string(),
            description: "Diagnostics and data analysis specialist for logs, metrics, structured outputs, and root-cause analysis.".to_string(),
            role: AgentRole::Analyst,
            role_label: "Analyst".to_string(),
            system_prompt: "I am Analyst, focused on diagnostics, logs, metrics, structured data, and root-cause analysis. I always speak directly in the first person ('I...'), never in third person. For greetings, quick checks, or casual/playful prompts, I respond naturally and concisely in 1-3 sentences without dumping a list of tools or refusing benign requests. When analyzing data or issues, I inspect logs and metrics objectively, compute exact figures, identify root causes, and summarize actionable insights.".to_string(),
            model_config: AgentModelConfig {
                temperature: 0.2,
                max_output_tokens: 0,
                thinking_level: ThinkingLevel::Medium,
                ..Default::default()
            },
            enabled_tools: vec![
                "calculator".into(),
                "json_process".into(),
                "text_process".into(),
                "datetime".into(),
                "read_file".into(),
                "list_files".into(),
                "grep_search".into(),
                "glob_files".into(),
                "sandbox_exec".into(),
                "web_search".into(),
                "web_fetch".into(),
                "system_info".into(),
                "ps5_info".into(),
                "doctor".into(),
                "write_artifact".into(),
                "memory_read".into(),
                "memory_write".into(),
                "memory_search".into(),
                "memory_get".into(),
                "load_skill".into(),
                "mcp:*".into(),
                "plugin:*".into(),
                "custom:*".into(),
            ],
            denied_tools: Vec::new(),
            memory_permissions: MemoryPermissions::default(),
            max_task_duration_secs: 0,
            max_reasoning_iterations: 0,
            priority: 65,
            can_communicate: true,
            enabled: true,
            sandbox_mode: None,
            tool_profile: None,
            is_builtin: true,
            is_protected: false,
            is_temporary: false,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        AgentDefinition {
            id: "executor".to_string(),
            name: "Executor".to_string(),
            description: "Operational specialist for approved workspace mutations, cleanup, and system status verification.".to_string(),
            role: AgentRole::Executor,
            role_label: "Executor".to_string(),
            system_prompt: "I am Executor, focused on concrete operational actions, approved workspace mutations, cleanup, and system status verification. I always speak directly in the first person ('I...'), never in third person. For greetings, quick checks, or casual/playful prompts, I respond naturally and concisely in 1-3 sentences without dumping a list of tools or refusing benign requests. When executing operational tasks, I work safely, respect approval gates for mutating operations, and confirm exact outcomes.".to_string(),
            model_config: AgentModelConfig {
                temperature: 0.15,
                max_output_tokens: 0,
                thinking_level: ThinkingLevel::Medium,
                ..Default::default()
            },
            enabled_tools: vec![
                "read_file".into(),
                "write_file".into(),
                "edit_file".into(),
                "apply_patch".into(),
                "create_directory".into(),
                "delete_file".into(),
                "list_files".into(),
                "grep_search".into(),
                "glob_files".into(),
                "sandbox_exec".into(),
                "process".into(),
                "browser".into(),
                "web_search".into(),
                "web_fetch".into(),
                "http_get".into(),
                "json_api_request".into(),
                "extract_html_text".into(),
                "schedule_create".into(),
                "schedule_list".into(),
                "schedule_update".into(),
                "schedule_delete".into(),
                "system_info".into(),
                "ps5_info".into(),
                "doctor".into(),
                "write_artifact".into(),
                "datetime".into(),
                "json_process".into(),
                "calculator".into(),
                "memory_read".into(),
                "memory_write".into(),
                "memory_search".into(),
                "memory_get".into(),
                "load_skill".into(),
                "mcp:*".into(),
                "plugin:*".into(),
                "custom:*".into(),
            ],
            denied_tools: Vec::new(),
            memory_permissions: MemoryPermissions::default(),
            max_task_duration_secs: 0,
            max_reasoning_iterations: 0,
            priority: 60,
            can_communicate: true,
            enabled: true,
            sandbox_mode: None,
            tool_profile: None,
            is_builtin: true,
            is_protected: false,
            is_temporary: false,
            created_at: now.clone(),
            updated_at: now,
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use ps5_agent_core::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    #[derive(Default)]
    struct MemStorage {
        agents: Mutex<HashMap<String, AgentDefinition>>,
        tasks: Mutex<Vec<TaskRecord>>,
        memories: Mutex<Vec<MemoryItem>>,
        settings: Mutex<HashMap<String, String>>,
        audits: Mutex<Vec<AuditLogEntry>>,
    }

    impl StorageBackend for MemStorage {
        fn driver_name(&self) -> &'static str { "mem" }
        fn run_migrations(&self) -> Result<()> { Ok(()) }
        fn integrity_check(&self) -> Result<String> { Ok("ok".into()) }
        fn checkpoint(&self) -> Result<()> { Ok(()) }
        fn upsert_agent(&self, agent: &AgentDefinition) -> Result<()> {
            self.agents.lock().unwrap().insert(agent.id.clone(), agent.clone());
            Ok(())
        }
        fn get_agent(&self, agent_id: &str) -> Result<Option<AgentDefinition>> {
            Ok(self.agents.lock().unwrap().get(agent_id).cloned())
        }
        fn list_agents(&self) -> Result<Vec<AgentDefinition>> {
            Ok(self.agents.lock().unwrap().values().cloned().collect())
        }
        fn delete_agent(&self, agent_id: &str) -> Result<bool> {
            Ok(self.agents.lock().unwrap().remove(agent_id).is_some())
        }
        fn create_conversation(&self, _: &Conversation) -> Result<()> { Ok(()) }
        fn get_conversation(&self, _: &str) -> Result<Option<Conversation>> { Ok(None) }
        fn list_conversations(&self, _: bool) -> Result<Vec<Conversation>> { Ok(vec![]) }
        fn delete_conversation(&self, _: &str) -> Result<bool> { Ok(false) }
        fn append_chat_message(&self, _: &ChatMessage) -> Result<()> { Ok(()) }
        fn list_chat_messages(&self, _: &str, _: bool, _: usize) -> Result<Vec<ChatMessage>> { Ok(vec![]) }
        fn search_chat_messages(&self, _: &str, _: usize) -> Result<Vec<ChatMessage>> { Ok(vec![]) }
        fn archive_old_chat_messages(&self, _: &str, _: usize) -> Result<usize> { Ok(0) }
        fn upsert_task(&self, task: &TaskRecord) -> Result<()> {
            let mut tasks = self.tasks.lock().unwrap();
            if let Some(existing) = tasks.iter_mut().find(|t| t.id == task.id) {
                *existing = task.clone();
            } else {
                tasks.push(task.clone());
            }
            Ok(())
        }
        fn get_task(&self, task_id: &str) -> Result<Option<TaskRecord>> {
            Ok(self.tasks.lock().unwrap().iter().find(|t| t.id == task_id).cloned())
        }
        fn list_tasks(&self, limit: usize) -> Result<Vec<TaskRecord>> {
            Ok(self.tasks.lock().unwrap().iter().take(limit).cloned().collect())
        }
        fn task_counts_summary(&self) -> Result<TaskCountsSummary> {
            let tasks = self.tasks.lock().unwrap();
            let mut summary = TaskCountsSummary {
                total: tasks.len(),
                ..Default::default()
            };
            for t in tasks.iter() {
                match t.status {
                    TaskStatus::Pending => summary.pending += 1,
                    TaskStatus::Running => summary.running += 1,
                    TaskStatus::Waiting => summary.waiting += 1,
                    TaskStatus::Completed => summary.completed += 1,
                    TaskStatus::Failed => summary.failed += 1,
                    TaskStatus::Cancelled => summary.cancelled += 1,
                    TaskStatus::Assigned => {}
                }
                let entry = summary
                    .per_agent
                    .entry(t.assigned_agent_id.clone())
                    .or_default();
                match t.status {
                    TaskStatus::Completed => entry.completed += 1,
                    TaskStatus::Failed => entry.failed += 1,
                    TaskStatus::Assigned | TaskStatus::Running | TaskStatus::Waiting => {
                        entry.active += 1;
                        if entry.latest_active_task_id.is_none() {
                            entry.latest_active_task_id = Some(t.id.clone());
                        }
                    }
                    _ => {}
                }
                let candidate_ts = t
                    .completed_at
                    .as_ref()
                    .or(t.started_at.as_ref())
                    .unwrap_or(&t.created_at);
                if entry
                    .last_activity
                    .as_ref()
                    .map_or(true, |cur| candidate_ts.as_str() > cur.as_str())
                {
                    entry.last_activity = Some(candidate_ts.clone());
                }
            }
            Ok(summary)
        }
        fn append_task_event(&self, _: &TaskEvent) -> Result<()> { Ok(()) }
        fn list_task_events(&self, _: &str) -> Result<Vec<TaskEvent>> { Ok(vec![]) }
        fn append_agent_message(&self, _: &AgentEnvelope) -> Result<()> { Ok(()) }
        fn update_agent_message_status(&self, _: &str, _: DeliveryStatus) -> Result<()> { Ok(()) }
        fn list_agent_messages(&self, _: Option<&str>, _: Option<&str>, _: usize) -> Result<Vec<AgentEnvelope>> { Ok(vec![]) }
        fn count_task_agent_messages(&self, _: &str) -> Result<u32> { Ok(0) }
        fn upsert_memory(&self, item: &MemoryItem) -> Result<()> {
            self.memories.lock().unwrap().push(item.clone());
            Ok(())
        }
        fn query_memories(&self, _: Option<MemoryScope>, _: Option<&str>, owner: Option<&str>, _: bool, limit: usize) -> Result<Vec<MemoryItem>> {
            Ok(self.memories.lock().unwrap().iter().filter(|m| owner.map_or(true, |o| m.owner_agent_id.as_deref() == Some(o))).take(limit).cloned().collect())
        }
        fn delete_memory(&self, _: &str) -> Result<bool> { Ok(false) }
        fn delete_agent_memories(&self, agent_id: &str) -> Result<usize> {
            let mut mems = self.memories.lock().unwrap();
            let before = mems.len();
            mems.retain(|m| m.owner_agent_id.as_deref() != Some(agent_id));
            Ok(before - mems.len())
        }
        fn memory_stats_for_agent(&self, agent_id: &str) -> Result<(u64, u64)> {
            let mems = self.memories.lock().unwrap();
            let owned: Vec<_> = mems.iter().filter(|m| m.owner_agent_id.as_deref() == Some(agent_id)).collect();
            let bytes: u64 = owned.iter().map(|m| m.content.len() as u64).sum();
            Ok((owned.len() as u64, bytes))
        }
        fn upsert_tool_spec(&self, _: &ToolSpec) -> Result<()> { Ok(()) }
        fn list_tool_specs(&self) -> Result<Vec<ToolSpec>> { Ok(vec![]) }
        fn record_tool_execution(&self, _: &ToolExecutionRecord) -> Result<()> { Ok(()) }
        fn list_tool_executions(&self, _: Option<&str>, _: usize) -> Result<Vec<ToolExecutionRecord>> { Ok(vec![]) }
        fn get_setting(&self, key: &str) -> Result<Option<String>> {
            Ok(self.settings.lock().unwrap().get(key).cloned())
        }
        fn set_setting(&self, key: &str, value: &str) -> Result<()> {
            self.settings.lock().unwrap().insert(key.to_string(), value.to_string());
            Ok(())
        }
        fn list_settings(&self) -> Result<Vec<(String, String)>> { Ok(vec![]) }
        fn create_session(&self, _: &SessionRecord) -> Result<()> { Ok(()) }
        fn get_session_by_hash(&self, _: &str) -> Result<Option<SessionRecord>> { Ok(None) }
        fn revoke_session(&self, _: &str) -> Result<()> { Ok(()) }
        fn purge_expired_sessions(&self, _: &str) -> Result<usize> { Ok(0) }
        fn upsert_approval(&self, _: &ApprovalRecord) -> Result<()> { Ok(()) }
        fn get_approval(&self, _: &str) -> Result<Option<ApprovalRecord>> { Ok(None) }
        fn list_approvals(&self, _: bool, _: usize) -> Result<Vec<ApprovalRecord>> { Ok(vec![]) }
        fn append_audit_log(&self, entry: &AuditLogEntry) -> Result<()> {
            self.audits.lock().unwrap().push(entry.clone());
            Ok(())
        }
        fn list_audit_logs(&self, limit: usize) -> Result<Vec<AuditLogEntry>> {
            Ok(self.audits.lock().unwrap().iter().take(limit).cloned().collect())
        }
    }

    struct MockTaskController {
        storage: Arc<MemStorage>,
        cancelled_calls: AtomicUsize,
    }

    impl AgentTaskController for MockTaskController {
        fn cancel_agent_tasks(&self, agent_id: &str, _reason: &str) -> Result<usize> {
            let mut tasks = self.storage.tasks.lock().unwrap();
            let mut count = 0;
            for t in tasks.iter_mut() {
                if t.assigned_agent_id == agent_id && (t.status.is_active() || t.status == TaskStatus::Pending) {
                    t.status = TaskStatus::Cancelled;
                    t.completed_at = Some(now_iso());
                    count += 1;
                }
            }
            self.cancelled_calls.fetch_add(count, Ordering::SeqCst);
            Ok(count)
        }
    }

    #[test]
    fn seeds_five_default_agents_once_and_protects_commander() {
        let storage = Arc::new(MemStorage::default());
        let (tx, _rx) = broadcast::channel(32);
        let cfg = ps5_agent_config::AppConfig::default().agents;
        let mgr = AgentManager::new(storage.clone(), cfg, tx);

        let seeded = mgr.seed_defaults().expect("seed defaults");
        assert_eq!(seeded, 5);
        let agents = mgr.list_agents().unwrap();
        assert_eq!(agents.len(), 5);

        let commander = mgr.get_agent("commander").unwrap().unwrap();
        assert!(commander.is_protected);
        assert!(commander.is_builtin);
        assert_eq!(commander.role, AgentRole::Commander);

        for id in ["researcher", "programmer", "analyst", "executor"] {
            let a = mgr.get_agent(id).unwrap().expect(id);
            assert!(!a.is_protected);
            assert!(a.is_builtin);
            assert!(a.enabled);
        }

        // Second seed is a no-op
        assert_eq!(mgr.seed_defaults().unwrap(), 0);
    }

    #[tokio::test]
    async fn dynamic_crud_enforces_limits_and_blocks_commander_delete() {
        let storage = Arc::new(MemStorage::default());
        let (tx, mut rx) = broadcast::channel(32);
        let mut cfg = ps5_agent_config::AppConfig::default().agents;
        cfg.max_agents = 6;
        cfg.allow_temporary_specialists = true;
        let mgr = AgentManager::new(storage.clone(), cfg, tx);
        mgr.seed_defaults().unwrap();

        // Create temporary specialist within max_agents=6
        let temp = mgr
            .create_agent(
                CreateAgentInput {
                    id: Some("perf-tuner".into()),
                    name: "Perf Tuner".into(),
                    description: "Temporary specialist".into(),
                    role: AgentRole::Custom,
                    role_label: Some("Specialist".into()),
                    system_prompt: "Optimize performance.".into(),
                    model_config: None,
                    enabled_tools: vec!["file_read".into()],
                    denied_tools: Some(vec!["delete_file".into()]),
                    memory_permissions: None,
                    max_task_duration_secs: Some(90),
                    max_reasoning_iterations: Some(5),
                    priority: Some(55),
                    can_communicate: Some(true),
                    enabled: Some(true),
                    sandbox_mode: Some("read_only".into()),
                    tool_profile: Some("minimal".into()),
                    is_temporary: true,
                    spawned_by: Some("commander".into()),
                },
                "commander",
            )
            .expect("create temporary specialist");
        assert_eq!(temp.id, "perf-tuner");
        assert!(temp.is_temporary);
        assert_eq!(temp.denied_tools, vec!["delete_file".to_string()]);
        assert_eq!(temp.sandbox_mode.as_deref(), Some("read_only"));
        assert_eq!(temp.tool_profile.as_deref(), Some("minimal"));

        // 7th agent rejected because max_agents == 6
        let over = mgr.create_agent(
            CreateAgentInput {
                id: Some("extra".into()),
                name: "Extra".into(),
                description: "Over limit".into(),
                role: AgentRole::Custom,
                role_label: None,
                system_prompt: "hi".into(),
                model_config: None,
                enabled_tools: vec![],
                denied_tools: None,
                memory_permissions: None,
                max_task_duration_secs: None,
                max_reasoning_iterations: None,
                priority: None,
                can_communicate: None,
                enabled: None,
                sandbox_mode: None,
                tool_profile: None,
                is_temporary: false,
                spawned_by: None,
            },
            "admin",
        );
        assert!(matches!(over, Err(AgentError::LimitExceeded(_))));

        // Update agent (including clearing sandbox_mode with whitespace and updating denied_tools/tool_profile)
        let updated = mgr
            .update_agent(
                "perf-tuner",
                UpdateAgentInput {
                    name: Some("Perf Tuner v2".into()),
                    priority: Some(80),
                    can_communicate: Some(false),
                    denied_tools: Some(vec!["process".into()]),
                    sandbox_mode: Some("  ".into()),
                    tool_profile: Some("coding".into()),
                    ..Default::default()
                },
                "admin",
            )
            .unwrap();
        assert_eq!(updated.name, "Perf Tuner v2");
        assert_eq!(updated.priority, 80);
        assert!(!updated.can_communicate);
        assert_eq!(updated.denied_tools, vec!["process".to_string()]);
        assert_eq!(updated.sandbox_mode, None);
        assert_eq!(updated.tool_profile.as_deref(), Some("coding"));

        // Disable & enable
        let disabled = mgr.set_enabled("perf-tuner", false, "admin").unwrap();
        assert!(!disabled.enabled);
        let enabled = mgr.set_enabled("perf-tuner", true, "admin").unwrap();
        assert!(enabled.enabled);

        // Commander cannot be deleted
        let del_cmd = mgr
            .delete_agent("commander", DeleteAgentOptions::default())
            .await;
        assert!(matches!(del_cmd, Err(AgentError::Protected(_))));

        // Delete temp agent with memory cleanup
        storage
            .upsert_memory(&MemoryItem {
                id: "m1".into(),
                scope: MemoryScope::Agent,
                namespace: "agent:perf-tuner".into(),
                owner_agent_id: Some("perf-tuner".into()),
                conversation_id: None,
                task_id: None,
                key: "note".into(),
                content: "hello world".into(),
                summary: None,
                importance: 5,
                tags: vec![],
                archived: false,
                created_at: now_iso(),
                updated_at: now_iso(),
            })
            .unwrap();

        let cards = mgr.list_cards().unwrap();
        let card = cards.iter().find(|c| c.definition.id == "perf-tuner").unwrap();
        assert_eq!(card.stats.memory_entries, 1);
        assert_eq!(card.stats.memory_bytes, 11);

        let deleted = mgr
            .delete_agent(
                "perf-tuner",
                DeleteAgentOptions {
                    active_task_policy: ActiveTaskPolicy::CancelActive,
                    delete_memory: true,
                    actor: "admin".into(),
                },
            )
            .await
            .unwrap();
        assert!(deleted);
        assert!(mgr.get_agent("perf-tuner").unwrap().is_none());
        assert_eq!(storage.memory_stats_for_agent("perf-tuner").unwrap().0, 0);

        // Drain events and check audit logs
        let mut actions = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            if let StreamEvent::AgentUpdated { action, .. } = ev {
                actions.push(action);
            }
        }
        assert!(actions.contains(&"created".to_string()));
        assert!(actions.contains(&"updated".to_string()));
        assert!(actions.contains(&"disabled".to_string()));
        assert!(actions.contains(&"enabled".to_string()));
        assert!(actions.contains(&"deleted".to_string()));
        assert!(!storage.list_audit_logs(20).unwrap().is_empty());
    }

    #[tokio::test]
    async fn delete_agent_handles_active_tasks_reject_cancel_and_wait() {
        let storage = Arc::new(MemStorage::default());
        let (tx, _rx) = broadcast::channel(16);
        let cfg = ps5_agent_config::AppConfig::default().agents;
        let mgr = AgentManager::new(storage.clone(), cfg, tx);
        let ctrl = Arc::new(MockTaskController {
            storage: storage.clone(),
            cancelled_calls: AtomicUsize::new(0),
        });
        mgr.attach_task_controller(ctrl.clone());
        mgr.seed_defaults().unwrap();

        storage
            .upsert_task(&TaskRecord {
                id: "task-exec-1".into(),
                conversation_id: None,
                user_request: "run".into(),
                assigned_agent_id: "executor".into(),
                parent_task_id: None,
                priority: 50,
                created_at: now_iso(),
                started_at: Some(now_iso()),
                completed_at: None,
                status: TaskStatus::Running,
                result: None,
                error_info: None,
                has_side_effects: false,
                progress_pct: 50,
                progress_note: None,
            })
            .unwrap();

        // Reject policy fails
        let res = mgr
            .delete_agent(
                "executor",
                DeleteAgentOptions {
                    active_task_policy: ActiveTaskPolicy::RejectIfActive,
                    delete_memory: false,
                    actor: "admin".into(),
                },
            )
            .await;
        assert!(matches!(res, Err(AgentError::Conflict(_))));

        // Cancel policy cancels active task and deletes agent
        let ok = mgr
            .delete_agent(
                "executor",
                DeleteAgentOptions {
                    active_task_policy: ActiveTaskPolicy::CancelActive,
                    delete_memory: false,
                    actor: "admin".into(),
                },
            )
            .await
            .unwrap();
        assert!(ok);
        assert_eq!(ctrl.cancelled_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            storage.get_task("task-exec-1").unwrap().unwrap().status,
            TaskStatus::Cancelled
        );
    }

    #[test]
    fn list_cards_combines_task_stats_and_memory_stats() {
        let storage = Arc::new(MemStorage::default());
        let (tx, _rx) = broadcast::channel(16);
        let cfg = ps5_agent_config::AppConfig::default().agents;
        let mgr = AgentManager::new(storage.clone(), cfg, tx);
        mgr.seed_defaults().unwrap();

        storage
            .upsert_task(&TaskRecord {
                id: "t1".into(),
                conversation_id: None,
                user_request: "q1".into(),
                assigned_agent_id: "researcher".into(),
                parent_task_id: None,
                priority: 70,
                created_at: now_iso(),
                started_at: Some(now_iso()),
                completed_at: Some(now_iso()),
                status: TaskStatus::Completed,
                result: Some("ok".into()),
                error_info: None,
                has_side_effects: false,
                progress_pct: 100,
                progress_note: None,
            })
            .unwrap();
        storage
            .upsert_task(&TaskRecord {
                id: "t2".into(),
                conversation_id: None,
                user_request: "q2".into(),
                assigned_agent_id: "researcher".into(),
                parent_task_id: None,
                priority: 70,
                created_at: now_iso(),
                started_at: Some(now_iso()),
                completed_at: Some(now_iso()),
                status: TaskStatus::Failed,
                result: None,
                error_info: Some("err".into()),
                has_side_effects: false,
                progress_pct: 20,
                progress_note: None,
            })
            .unwrap();
        mgr.record_task_started("researcher", "t3");

        let cards = mgr.list_cards().unwrap();
        let res_card = cards.iter().find(|c| c.definition.id == "researcher").unwrap();
        assert_eq!(res_card.stats.completed_tasks, 1);
        assert_eq!(res_card.stats.failed_tasks, 1);
        assert_eq!(res_card.stats.current_task_id.as_deref(), Some("t3"));
    }

    #[tokio::test]
    async fn delete_wait_policy_and_temp_specialist_disabled_check() {
        let storage = Arc::new(MemStorage::default());
        let (tx, _rx) = broadcast::channel(16);
        let mut cfg = ps5_agent_config::AppConfig::default().agents;
        cfg.max_agents = 8;
        cfg.allow_temporary_specialists = false;
        let mgr = AgentManager::new(storage.clone(), cfg, tx);
        mgr.seed_defaults().unwrap();

        let err = mgr.create_agent(
            CreateAgentInput {
                id: Some("temp-1".into()),
                name: "Temp".into(),
                description: "blocked".into(),
                role: AgentRole::Custom,
                role_label: None,
                system_prompt: "hi".into(),
                model_config: None,
                enabled_tools: vec![],
                denied_tools: None,
                memory_permissions: None,
                max_task_duration_secs: None,
                max_reasoning_iterations: None,
                priority: None,
                can_communicate: None,
                enabled: None,
                sandbox_mode: None,
                tool_profile: None,
                is_temporary: true,
                spawned_by: Some("commander".into()),
            },
            "commander",
        );
        assert!(matches!(err, Err(AgentError::PermissionDenied(_))));

        storage
            .upsert_task(&TaskRecord {
                id: "t-wait".into(),
                conversation_id: None,
                user_request: "work".into(),
                assigned_agent_id: "analyst".into(),
                parent_task_id: None,
                priority: 50,
                created_at: now_iso(),
                started_at: Some(now_iso()),
                completed_at: None,
                status: TaskStatus::Running,
                result: None,
                error_info: None,
                has_side_effects: false,
                progress_pct: 50,
                progress_note: None,
            })
            .unwrap();

        let st_clone = storage.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(40)).await;
            let mut t = st_clone.get_task("t-wait").unwrap().unwrap();
            t.status = TaskStatus::Completed;
            t.completed_at = Some(now_iso());
            st_clone.upsert_task(&t).unwrap();
        });

        let ok = mgr
            .delete_agent(
                "analyst",
                DeleteAgentOptions {
                    active_task_policy: ActiveTaskPolicy::WaitForActive { timeout_ms: 500 },
                    delete_memory: false,
                    actor: "admin".into(),
                },
            )
            .await
            .unwrap();
        assert!(ok);
    }

    #[tokio::test]
    async fn zero_limits_allow_unlimited_agents_iterations_and_duration_and_cleanup_temp() {
        let storage = Arc::new(MemStorage::default());
        let (tx, _rx) = broadcast::channel(32);
        let mut cfg = ps5_agent_config::AppConfig::default().agents;
        cfg.max_agents = 0;
        cfg.max_reasoning_iterations = 0;
        cfg.default_task_timeout_secs = 0;
        cfg.allow_temporary_specialists = true;
        let mgr = AgentManager::new(storage.clone(), cfg, tx);
        mgr.seed_defaults().unwrap();

        // max_agents == 0 allows creating more than 5 agents without LimitExceeded
        for idx in 0..3 {
            let created = mgr
                .create_agent(
                    CreateAgentInput {
                        id: Some(format!("temp-{idx}")),
                        name: format!("Temp {idx}"),
                        description: "temp".into(),
                        role: AgentRole::Custom,
                        role_label: None,
                        system_prompt: "I am a temporary helper.".into(),
                        model_config: None,
                        enabled_tools: vec!["read_file".into()],
                        denied_tools: None,
                        memory_permissions: None,
                        max_task_duration_secs: Some(0),
                        max_reasoning_iterations: Some(0),
                        priority: None,
                        can_communicate: None,
                        enabled: None,
                        sandbox_mode: None,
                        tool_profile: None,
                        is_temporary: true,
                        spawned_by: Some("commander".into()),
                    },
                    "commander",
                )
                .unwrap();
            assert_eq!(created.max_reasoning_iterations, 0);
            assert_eq!(created.max_task_duration_secs, 0);
        }

        // Remove 32 cap on update_agent and preserve 0 = unlimited
        let updated = mgr
            .update_agent(
                "temp-0",
                UpdateAgentInput {
                    max_reasoning_iterations: Some(128),
                    max_task_duration_secs: Some(0),
                    ..Default::default()
                },
                "admin",
            )
            .unwrap();
        assert_eq!(updated.max_reasoning_iterations, 128);
        assert_eq!(updated.max_task_duration_secs, 0);

        // Populate live_stats and memory for temp-0, then clean up
        mgr.record_task_started("temp-0", "task-temp-0");
        mgr.record_task_finished("temp-0", true);
        storage
            .upsert_memory(&MemoryItem {
                id: "m-temp".into(),
                scope: MemoryScope::Agent,
                namespace: "agent:temp-0".into(),
                owner_agent_id: Some("temp-0".into()),
                conversation_id: None,
                task_id: None,
                key: "k".into(),
                content: "v".into(),
                summary: None,
                importance: 1,
                tags: vec![],
                archived: false,
                created_at: now_iso(),
                updated_at: now_iso(),
            })
            .unwrap();

        mgr.cleanup_temporary_agent("temp-0").await.unwrap();
        assert!(mgr.get_agent("temp-0").unwrap().is_none());
        assert_eq!(storage.memory_stats_for_agent("temp-0").unwrap().0, 0);
        assert!(!mgr.live_stats.read().unwrap().contains_key("temp-0"));

        // Permanent agents are never deleted by cleanup_temporary_agent
        mgr.cleanup_temporary_agent("commander").await.unwrap();
        mgr.cleanup_temporary_agent("programmer").await.unwrap();
        assert!(mgr.get_agent("commander").unwrap().is_some());
        assert!(mgr.get_agent("programmer").unwrap().is_some());
    }

    #[test]
    fn default_agents_use_first_person_prompts_unlimited_output_and_rich_tools() {
        assert_eq!(BUILTIN_AGENTS_SCHEMA_VERSION, "7");
        let defs = default_agent_definitions();
        assert_eq!(defs.len(), 5);
        for d in &defs {
            assert_eq!(d.model_config.max_output_tokens, 0, "{}", d.id);
            assert_eq!(d.max_task_duration_secs, 0, "{}", d.id);
            assert_eq!(d.max_reasoning_iterations, 0, "{}", d.id);
            assert!(d.denied_tools.is_empty(), "{}", d.id);
            assert_eq!(d.sandbox_mode, None, "{}", d.id);
            assert_eq!(d.tool_profile, None, "{}", d.id);
            assert!(
                d.system_prompt.starts_with("I am "),
                "expected 1st-person prompt for {}, got: {}",
                d.id,
                d.system_prompt
            );
            for legacy in [
                "delegate_task",
                "agent_message",
                "spawn_temp_agent",
                "write_agent_memory",
                "read_agent_memory",
                "json_parse_format",
                "timestamp_time_helper",
                "text_transform_summarize_prep",
                "file_read",
                "file_write",
                "file_list",
                "file_delete",
            ] {
                assert!(
                    !d.enabled_tools.iter().any(|t| t == legacy),
                    "{} must not retain redundant legacy alias {legacy}",
                    d.id
                );
            }
            let mut seen = std::collections::HashSet::new();
            for tool in &d.enabled_tools {
                assert!(
                    seen.insert(tool.as_str()),
                    "duplicate tool '{tool}' in {}",
                    d.id
                );
            }
        }
        let cmd = defs.iter().find(|d| d.id == "commander").unwrap();
        let prog = defs.iter().find(|d| d.id == "programmer").unwrap();
        let res = defs.iter().find(|d| d.id == "researcher").unwrap();
        let analyst = defs.iter().find(|d| d.id == "analyst").unwrap();
        let exec = defs.iter().find(|d| d.id == "executor").unwrap();
        assert_eq!(cmd.model_config.thinking_level, ThinkingLevel::Medium);
        assert_eq!(prog.model_config.thinking_level, ThinkingLevel::High);
        assert!(cmd.system_prompt.contains("I am Commander"));
        assert!(cmd.system_prompt.contains("NEVER call write_file"));
        assert!(cmd.system_prompt.contains("assign_agent_task"));
        assert!(cmd.system_prompt.contains("schedule_create"));
        assert!(prog.system_prompt.contains("Flappy Bird"));
        for required in [
            "write_file",
            "read_file",
            "list_files",
            "create_directory",
            "edit_file",
            "apply_patch",
            "grep_search",
            "glob_files",
            "sandbox_exec",
            "process",
            "memory_read",
            "memory_write",
            "load_skill",
            "mcp:*",
            "plugin:*",
            "custom:*",
        ] {
            assert!(
                cmd.enabled_tools.iter().any(|t| t == required),
                "commander missing {required}"
            );
            assert!(
                prog.enabled_tools.iter().any(|t| t == required),
                "programmer missing {required}"
            );
        }
        for cmd_session_tool in [
            "sessions_list",
            "sessions_spawn",
            "sessions_send",
            "session_status",
            "assign_agent_task",
            "send_agent_message",
            "request_temporary_specialist",
            "json_process",
            "text_process",
            "datetime",
            "memory_read",
            "memory_write",
            "doctor",
            "system_info",
            "ps5_info",
            "write_artifact",
        ] {
            assert!(
                cmd.enabled_tools.iter().any(|t| t == cmd_session_tool),
                "commander missing {cmd_session_tool}"
            );
        }
        // Legacy alias calls still match Commander's canonical allowlist via core policy resolution.
        for legacy_alias in [
            "delegate_task",
            "agent_message",
            "spawn_temp_agent",
            "write_agent_memory",
            "read_agent_memory",
            "json_parse_format",
            "timestamp_time_helper",
            "text_transform_summarize_prep",
        ] {
            assert!(
                ps5_agent_core::agent_allows_tool_with_policy(
                    &cmd.enabled_tools,
                    &cmd.denied_tools,
                    legacy_alias,
                    "",
                ),
                "commander should still execute legacy alias {legacy_alias}"
            );
        }
        for analyst_tool in [
            "calculator",
            "json_process",
            "text_process",
            "datetime",
            "memory_read",
            "memory_write",
            "system_info",
            "ps5_info",
            "doctor",
            "write_artifact",
        ] {
            assert!(
                analyst.enabled_tools.iter().any(|t| t == analyst_tool),
                "analyst missing {analyst_tool}"
            );
        }
        for res_tool in ["datetime", "json_process", "text_process", "memory_read", "memory_write"] {
            assert!(
                res.enabled_tools.iter().any(|t| t == res_tool),
                "researcher missing {res_tool}"
            );
        }
        assert!(prog.enabled_tools.iter().any(|t| t == "json_process"));
        for exec_tool in ["datetime", "json_process", "memory_read", "memory_write"] {
            assert!(
                exec.enabled_tools.iter().any(|t| t == exec_tool),
                "executor missing {exec_tool}"
            );
        }
        for res_forbidden in [
            "write_file",
            "edit_file",
            "apply_patch",
            "delete_file",
            "create_directory",
            "sandbox_exec",
            "process",
        ] {
            assert!(
                !res.enabled_tools.iter().any(|t| t == res_forbidden),
                "researcher must remain read-only without {res_forbidden}"
            );
        }
        for sched_tool in [
            "schedule_create",
            "schedule_list",
            "schedule_update",
            "schedule_delete",
        ] {
            assert!(
                cmd.enabled_tools.iter().any(|t| t == sched_tool),
                "commander missing {sched_tool}"
            );
            assert!(
                exec.enabled_tools.iter().any(|t| t == sched_tool),
                "executor missing {sched_tool}"
            );
            for readonly_or_coder in [res, prog, analyst] {
                assert!(
                    !readonly_or_coder.enabled_tools.iter().any(|t| t == sched_tool),
                    "{} should not enable {sched_tool}",
                    readonly_or_coder.id
                );
            }
        }

        let storage = Arc::new(MemStorage::default());
        let (tx, _rx) = broadcast::channel(32);
        let mgr = AgentManager::new(storage.clone(), ps5_agent_config::AppConfig::default().agents, tx);
        let base = defs[0].clone();
        for id in ["z-tie", "a-tie"] {
            let mut a = base.clone();
            a.id = id.to_string();
            a.name = "Same Name".to_string();
            a.priority = 50;
            a.is_builtin = false;
            a.is_protected = false;
            storage.upsert_agent(&a).unwrap();
        }
        let ordered_ids: Vec<String> = mgr.list_agents().unwrap().into_iter().map(|a| a.id).collect();
        assert_eq!(ordered_ids, vec!["a-tie".to_string(), "z-tie".to_string()]);
    }

    #[test]
    fn upgrade_builtin_agents_migrates_legacy_limits_merges_custom_wildcard_and_enables_all() {
        let storage = Arc::new(MemStorage::default());
        let (tx, _rx) = broadcast::channel(32);
        let cfg = ps5_agent_config::AppConfig::default().agents;
        let mgr = AgentManager::new(storage.clone(), cfg, tx);

        // Seed v2-like legacy built-in agents with finite limits, missing custom:*, and one disabled.
        for mut def in default_agent_definitions() {
            match def.id.as_str() {
                "commander" => {
                    def.max_task_duration_secs = 240;
                    def.max_reasoning_iterations = 8;
                    def.enabled_tools.retain(|t| t != "custom:*");
                }
                "programmer" => {
                    def.max_task_duration_secs = 240;
                    def.max_reasoning_iterations = 8;
                    def.enabled_tools.retain(|t| t != "custom:*");
                    def.enabled = false;
                }
                "analyst" => {
                    // Explicit user customization should NOT be overwritten.
                    def.max_task_duration_secs = 300;
                    def.max_reasoning_iterations = 12;
                    def.enabled = false;
                }
                _ => {
                    def.max_task_duration_secs = 180;
                    def.max_reasoning_iterations = 6;
                }
            }
            storage.upsert_agent(&def).unwrap();
        }
        storage.set_setting("agents_seeded", "true").unwrap();
        storage
            .set_setting("builtin_agents_schema_version", "2")
            .unwrap();
        storage.set_setting("builtin_agents_hash", "legacy-v2").unwrap();

        let upgraded = mgr.upgrade_builtin_agents().unwrap();
        assert_eq!(upgraded, 4);

        let cmd = mgr.get_agent("commander").unwrap().unwrap();
        assert_eq!(cmd.max_task_duration_secs, 0);
        assert_eq!(cmd.max_reasoning_iterations, 0);
        assert!(cmd.enabled_tools.iter().any(|t| t == "custom:*"));

        let prog = mgr.get_agent("programmer").unwrap().unwrap();
        assert_eq!(prog.max_task_duration_secs, 0);
        assert_eq!(prog.max_reasoning_iterations, 0);
        assert!(prog.enabled_tools.iter().any(|t| t == "custom:*"));

        let res = mgr.get_agent("researcher").unwrap().unwrap();
        assert_eq!(res.max_task_duration_secs, 0);
        assert_eq!(res.max_reasoning_iterations, 0);

        let analyst = mgr.get_agent("analyst").unwrap().unwrap();
        assert_eq!(analyst.max_task_duration_secs, 300);
        assert_eq!(analyst.max_reasoning_iterations, 12);

        // Second upgrade call is a no-op.
        assert_eq!(mgr.upgrade_builtin_agents().unwrap(), 0);

        // enable_all_agents enables the 2 disabled agents and returns 2, then 0 on repeat.
        assert_eq!(mgr.enable_all_agents().unwrap(), 2);
        assert!(mgr.get_agent("programmer").unwrap().unwrap().enabled);
        assert!(mgr.get_agent("analyst").unwrap().unwrap().enabled);
        assert_eq!(mgr.enable_all_agents().unwrap(), 0);
    }

    #[test]
    fn seed_defaults_upgrades_to_v5_adds_schedule_and_openclaw_tools_and_preserves_custom_you_are_prompt() {
        let storage = Arc::new(MemStorage::default());
        let (tx, _rx) = broadcast::channel(32);
        let cfg = ps5_agent_config::AppConfig::default().agents;
        let mgr = AgentManager::new(storage.clone(), cfg.clone(), tx.clone());

        // Regression test (#394): a user-customized prompt starting with "You are " MUST NOT be overwritten!
        let custom_prompt = "You are a custom strict security commander.".to_string();
        for mut def in default_agent_definitions() {
            if def.id == "commander" {
                def.system_prompt = custom_prompt.clone();
                def.updated_at = "2099-01-01T00:00:00Z".to_string();
                def.enabled_tools.retain(|t| !t.starts_with("schedule_"));
            } else if def.id == "executor" {
                def.enabled_tools.retain(|t| !t.starts_with("schedule_"));
            } else if def.id == "researcher" {
                // Unedited built-in with stale name + stale model_config must sync cleanly.
                def.name = "Old Researcher".to_string();
                def.model_config.temperature = 0.99;
                def.model_config.context_limit = 4096;
            }
            storage.upsert_agent(&def).unwrap();
        }
        storage.set_setting("agents_seeded", "true").unwrap();
        storage
            .set_setting("builtin_agents_schema_version", "4")
            .unwrap();
        storage.set_setting("builtin_agents_hash", "legacy-v4").unwrap();
        storage
            .set_setting("builtin_prompt_hash.commander", &sha256_hex("legacy v4 prompt"))
            .unwrap();

        mgr.seed_defaults().unwrap();

        assert_eq!(
            storage
                .get_setting("builtin_agents_schema_version")
                .unwrap()
                .as_deref(),
            Some("7")
        );
        assert_eq!(
            storage
                .get_setting("builtin_agents_hash")
                .unwrap()
                .as_deref(),
            Some(builtin_agents_fingerprint().as_str())
        );

        let cmd = mgr.get_agent("commander").unwrap().unwrap();
        assert_eq!(cmd.system_prompt, custom_prompt);
        let exec = mgr.get_agent("executor").unwrap().unwrap();
        for sched_tool in [
            "schedule_create",
            "schedule_list",
            "schedule_update",
            "schedule_delete",
        ] {
            assert!(
                cmd.enabled_tools.iter().any(|t| t == sched_tool),
                "upgraded commander missing {sched_tool}"
            );
            assert!(
                exec.enabled_tools.iter().any(|t| t == sched_tool),
                "upgraded executor missing {sched_tool}"
            );
        }
        let synced_res = mgr.get_agent("researcher").unwrap().unwrap();
        assert_eq!(synced_res.name, "Researcher");
        assert!((synced_res.model_config.temperature - 0.25).abs() < 1e-6);
        assert_eq!(synced_res.model_config.context_limit, 8192);
        assert_eq!(mgr.upgrade_builtin_agents().unwrap(), 0);

        // When commander prompt was not customized on v4, seed_defaults upgrades prompt + tools.
        let unedited_storage = Arc::new(MemStorage::default());
        let unedited_mgr = AgentManager::new(unedited_storage.clone(), cfg.clone(), tx.clone());
        let v4_default_prompt = "I am Commander, legacy v4 prompt.".to_string();
        for mut def in default_agent_definitions() {
            if def.id == "commander" {
                def.system_prompt = v4_default_prompt.clone();
                def.updated_at = "2099-01-01T00:00:00Z".to_string();
                def.enabled_tools.retain(|t| !t.starts_with("schedule_"));
            } else if def.id == "executor" {
                def.enabled_tools.retain(|t| !t.starts_with("schedule_"));
            }
            unedited_storage.upsert_agent(&def).unwrap();
        }
        unedited_storage.set_setting("agents_seeded", "true").unwrap();
        unedited_storage
            .set_setting("builtin_agents_schema_version", "4")
            .unwrap();
        unedited_storage
            .set_setting("builtin_agents_hash", "legacy-v4")
            .unwrap();
        unedited_storage
            .set_setting(
                "builtin_prompt_hash.commander",
                &sha256_hex(&v4_default_prompt),
            )
            .unwrap();

        unedited_mgr.seed_defaults().unwrap();
        let upgraded_cmd = unedited_mgr.get_agent("commander").unwrap().unwrap();
        assert!(upgraded_cmd.system_prompt.contains("schedule_create"));
        assert!(upgraded_cmd
            .enabled_tools
            .iter()
            .any(|t| t == "schedule_create"));

        // Verify unedited enabled_tools sync replaces retired baseline tools while customized
        // enabled_tools preserve user choices (additions kept, known baseline removals not re-added).
        let tools_storage = Arc::new(MemStorage::default());
        let tools_mgr = AgentManager::new(tools_storage.clone(), cfg.clone(), tx.clone());
        for mut def in default_agent_definitions() {
            if def.id == "analyst" {
                def.enabled_tools.push("deprecated_legacy_tool".into());
                tools_storage
                    .set_setting(
                        "builtin_tools_hash.analyst",
                        &tools_list_hash(&def.enabled_tools),
                    )
                    .unwrap();
            } else if def.id == "executor" {
                let baseline_json = serde_json::to_string(&def.enabled_tools).unwrap();
                tools_storage
                    .set_setting("builtin_known_tools.executor", &baseline_json)
                    .unwrap();
                tools_storage
                    .set_setting(
                        "builtin_tools_hash.executor",
                        &tools_list_hash(&def.enabled_tools),
                    )
                    .unwrap();
                def.enabled_tools.retain(|t| t != "browser");
                def.enabled_tools.push("custom:user_script".into());
                def.updated_at = "2099-01-01T00:00:00Z".to_string();
            }
            tools_storage.upsert_agent(&def).unwrap();
        }
        tools_storage.set_setting("agents_seeded", "true").unwrap();
        tools_storage
            .set_setting("builtin_agents_hash", "stale-fp")
            .unwrap();

        tools_mgr.upgrade_builtin_agents().unwrap();
        let upgraded_analyst = tools_mgr.get_agent("analyst").unwrap().unwrap();
        assert!(!upgraded_analyst
            .enabled_tools
            .iter()
            .any(|t| t == "deprecated_legacy_tool"));
        let upgraded_executor = tools_mgr.get_agent("executor").unwrap().unwrap();
        assert!(upgraded_executor
            .enabled_tools
            .iter()
            .any(|t| t == "custom:user_script"));
        assert!(!upgraded_executor
            .enabled_tools
            .iter()
            .any(|t| t == "browser"));

        // Partial unseeded DB (e.g., custom agent + commander pre-inserted without agents_seeded):
        // seed_defaults restores all remaining defaults among the 5 built-ins cleanly.
        let partial_storage = Arc::new(MemStorage::default());
        let partial_mgr = AgentManager::new(partial_storage.clone(), cfg, tx);
        let cmd_only = default_agent_definitions()
            .into_iter()
            .find(|d| d.id == "commander")
            .unwrap();
        partial_storage.upsert_agent(&cmd_only).unwrap();
        let seeded_missing = partial_mgr.seed_defaults().unwrap();
        assert_eq!(seeded_missing, 4);
        assert_eq!(partial_mgr.list_agents().unwrap().len(), 5);
    }

    #[test]
    fn commander_prompt_prohibits_self_coding_and_delegates_to_programmer() {
        let defs = default_agent_definitions();
        let cmd = defs.iter().find(|d| d.id == "commander").unwrap();
        let prog = defs.iter().find(|d| d.id == "programmer").unwrap();

        assert!(cmd.system_prompt.starts_with("I am Commander"));
        assert!(cmd
            .system_prompt
            .contains("NEVER draft or write the code or script myself in my thoughts or response"));
        assert!(cmd
            .system_prompt
            .contains("NEVER call write_file to create code/scripts myself"));
        assert!(cmd
            .system_prompt
            .contains("ALWAYS delegate the task immediately to programmer using assign_agent_task (target_agent_id: \"programmer\")"));
        assert!(!cmd
            .system_prompt
            .contains("always use write_file (or have Programmer do so)"));
        assert!(cmd.system_prompt.contains("schedule_create"));
        assert!(prog
            .system_prompt
            .contains("always use write_file to save a complete"));

        // Verify unedited v6 Commander migrates cleanly to v7 delegation prompt on seed_defaults().
        let storage = Arc::new(MemStorage::default());
        let (tx, _rx) = broadcast::channel(16);
        let cfg = ps5_agent_config::AppConfig::default().agents;
        let mgr = AgentManager::new(storage.clone(), cfg, tx);
        let v6_cmd_prompt = "I am Commander, legacy v6 prompt. When asked to create, build, or write code, a game, an app, a webpage, or a script (for example, an HTML Flappy Bird game), I always use write_file (or have Programmer do so) to save a complete, standalone, 100%-working file in the workspace.".to_string();
        for mut def in default_agent_definitions() {
            if def.id == "commander" {
                def.system_prompt = v6_cmd_prompt.clone();
                def.updated_at = "2099-01-01T00:00:00Z".to_string();
            }
            storage.upsert_agent(&def).unwrap();
        }
        storage.set_setting("agents_seeded", "true").unwrap();
        storage
            .set_setting("builtin_agents_schema_version", "6")
            .unwrap();
        storage
            .set_setting("builtin_agents_hash", "legacy-v6")
            .unwrap();
        storage
            .set_setting("builtin_prompt_hash.commander", &sha256_hex(&v6_cmd_prompt))
            .unwrap();

        mgr.seed_defaults().unwrap();

        assert_eq!(
            storage
                .get_setting("builtin_agents_schema_version")
                .unwrap()
                .as_deref(),
            Some("7")
        );
        let upgraded_cmd = mgr.get_agent("commander").unwrap().unwrap();
        assert_eq!(upgraded_cmd.system_prompt, cmd.system_prompt);
        assert!(upgraded_cmd
            .system_prompt
            .contains("NEVER call write_file to create code/scripts myself"));
        assert!(upgraded_cmd
            .system_prompt
            .contains("assign_agent_task (target_agent_id: \"programmer\")"));
    }
}
