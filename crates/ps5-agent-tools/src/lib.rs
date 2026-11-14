pub mod browser;
pub mod calculator;
pub mod custom;
pub mod files;
pub mod mcp;
pub mod patch;
pub mod plugins;
pub mod sandbox_exec;
pub mod skills;
pub mod specs;
pub mod utility;
pub mod web;

pub use browser::{BrowserActivityContext, SandboxBrowserManager};
pub use custom::{CustomToolDefinition, CustomToolManager};
pub use files::WorkspaceEntryInfo;
pub use mcp::{McpManager, McpServerConfig, McpServerView, McpToolInfo};
pub use plugins::{
    PluginBundledSkill, PluginCustomToolSpec, PluginManager, PluginManifest, PluginToolView,
    PluginView,
};
pub use ps5_agent_core::{
    agent_allows_tool_with_policy, canonical_tool_id, SandboxApprovalPolicy, SandboxMode,
};
pub use skills::{ParsedSkillMarkdown, SkillDocument, SkillManager, SkillSummary};

use async_trait::async_trait;
use ps5_agent_config::ToolsConfig;
use ps5_agent_core::{
    new_id, now_iso, AgentDefinition, AgentError, ApprovalRecord, ApprovalStatus, Result,
    StorageBackend, ToolDefinitionSchema, ToolExecutionRecord, ToolPermission, ToolSpec,
};
use ps5_agent_logging::{log_debug, log_info, log_warn};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{Duration, Instant};

const MAX_BUNDLED_SKILL_BYTES: u64 = 512 * 1024;
const BUNDLED_RESOURCE_SUBDIRS: [&str; 4] = ["scripts", "references", "examples", "resources"];

fn builtin_tool_ids() -> &'static HashSet<String> {
    static BUILTIN_IDS: OnceLock<HashSet<String>> = OnceLock::new();
    BUILTIN_IDS.get_or_init(|| {
        specs::builtin_tool_specs(0)
            .into_iter()
            .map(|s| s.id)
            .collect()
    })
}

fn is_builtin_tool(tool_id: &str) -> bool {
    builtin_tool_ids().contains(canonical_tool_id(tool_id))
}

fn is_disallowed_schedule_tool(agent_id: &str, tool_id: &str) -> bool {
    canonical_tool_id(tool_id).starts_with("schedule_")
        && !matches!(agent_id.trim(), "commander" | "executor")
}

/// Matches an `enabled_tools` pattern list against `tool_id`.
/// Supports exact matches, `"*"`, and suffix-wildcard prefixes (e.g. `"mcp:*"`, `"plugin:*"`, `"skill:*"`, `"file_*"`).
pub fn tool_pattern_matches(enabled_tools: &[String], tool_id: &str) -> bool {
    enabled_tools.iter().any(|t| {
        t == tool_id
            || t == "*"
            || t.strip_suffix('*')
                .is_some_and(|prefix| tool_id.starts_with(prefix))
    })
}

/// Returns `true` when `enabled_tools` permits `spec` via exact ID, wildcard (`"*"`),
/// `group:*` expansion, prefix pattern (`"file_*"`, `"mcp:*"`, `"plugin:*"`, etc.),
/// alias match, or `"custom:*"`.
pub fn agent_allows_tool(enabled_tools: &[String], spec: &ToolSpec) -> bool {
    agent_allows_tool_with_policy(enabled_tools, &[], &spec.id, &spec.category)
}

fn derive_data_dir(config: &ToolsConfig) -> PathBuf {
    if let Some(first) = config.allowed_paths.first() {
        let p = PathBuf::from(first);
        if p.file_name().and_then(|n| n.to_str()) == Some("workspace") {
            if let Some(parent) = p.parent() {
                if !parent.as_os_str().is_empty() {
                    return parent.to_path_buf();
                }
            }
        }
        let mut hash: u64 = 0xcbf29ce484222325;
        for &b in p.to_string_lossy().as_bytes() {
            hash ^= u64::from(b);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        return std::env::temp_dir().join(format!("ps5-agent-tools-data-{hash:016x}"));
    }
    std::env::temp_dir().join("ps5-agent-data")
}

#[async_trait]
pub trait AgentToolBridge: Send + Sync {
    async fn send_agent_message(
        &self,
        from_agent_id: &str,
        to_agent_id: &str,
        task_id: Option<&str>,
        content: &str,
    ) -> Result<Value>;

    async fn assign_agent_task(
        &self,
        from_agent_id: &str,
        target_agent_id: &str,
        user_request: &str,
        parent_task_id: Option<&str>,
        conversation_id: Option<&str>,
    ) -> Result<Value>;

    async fn query_agent_status(
        &self,
        requester_agent_id: &str,
        target_agent_id: Option<&str>,
        task_id: Option<&str>,
    ) -> Result<Value>;

    async fn read_agent_memory(
        &self,
        requester_agent_id: &str,
        target_agent_id: &str,
        query: Option<&str>,
        limit: usize,
    ) -> Result<Value>;

    async fn request_temporary_specialist(
        &self,
        requester_agent_id: &str,
        name: &str,
        role_label: &str,
        system_prompt: &str,
        enabled_tools: Vec<String>,
        task_request: Option<&str>,
        parent_task_id: Option<&str>,
    ) -> Result<Value>;

    async fn memory_read(
        &self,
        _agent_id: &str,
        _scope: Option<&str>,
        _namespace: Option<&str>,
        _query: Option<&str>,
        _limit: usize,
    ) -> Result<Value> {
        Err(AgentError::Tool {
            tool_id: "memory_read".into(),
            message: "memory_read is not configured on agent tool bridge".into(),
        })
    }

    async fn memory_write(
        &self,
        _agent_id: &str,
        _scope: &str,
        _namespace: &str,
        _key: &str,
        _content: &str,
        _importance: u8,
        _conversation_id: Option<&str>,
        _task_id: Option<&str>,
    ) -> Result<Value> {
        Err(AgentError::Tool {
            tool_id: "memory_write".into(),
            message: "memory_write is not configured on agent tool bridge".into(),
        })
    }

    async fn memory_search(
        &self,
        _agent_id: &str,
        _query: &str,
        _max_results: usize,
    ) -> Result<Value> {
        Err(AgentError::Tool {
            tool_id: "memory_search".into(),
            message: "memory_search is not configured on agent tool bridge".into(),
        })
    }

    async fn memory_get(
        &self,
        _agent_id: &str,
        _path: &str,
        _from_line: Option<usize>,
        _lines: Option<usize>,
    ) -> Result<Value> {
        Err(AgentError::Tool {
            tool_id: "memory_get".into(),
            message: "memory_get is not configured on agent tool bridge".into(),
        })
    }

    async fn system_info(&self) -> Result<Value> {
        Err(AgentError::Tool {
            tool_id: "system_info".into(),
            message: "system_info is not configured on agent tool bridge".into(),
        })
    }

    async fn ps5_info(&self) -> Result<Value> {
        Err(AgentError::Tool {
            tool_id: "ps5_info".into(),
            message: "ps5_info is not configured on agent tool bridge".into(),
        })
    }

    async fn sessions_list(&self, _limit: usize, _active_only: bool) -> Result<Value> {
        Err(AgentError::Tool {
            tool_id: "sessions_list".into(),
            message: "sessions_list is not configured on agent tool bridge".into(),
        })
    }

    async fn sessions_spawn(
        &self,
        _sender_agent_id: &str,
        _target_agent_id: Option<&str>,
        _conversation_id: Option<&str>,
        _prompt: &str,
    ) -> Result<Value> {
        Err(AgentError::Tool {
            tool_id: "sessions_spawn".into(),
            message: "sessions_spawn is not configured on agent tool bridge".into(),
        })
    }

    async fn sessions_send(
        &self,
        _sender_agent_id: &str,
        _conversation_id: &str,
        _message: &str,
    ) -> Result<Value> {
        Err(AgentError::Tool {
            tool_id: "sessions_send".into(),
            message: "sessions_send is not configured on agent tool bridge".into(),
        })
    }

    async fn session_status(
        &self,
        _conversation_id: Option<&str>,
        _task_id: Option<&str>,
    ) -> Result<Value> {
        Err(AgentError::Tool {
            tool_id: "session_status".into(),
            message: "session_status is not configured on agent tool bridge".into(),
        })
    }

    async fn process_action(&self, _action: &str, _target_id: Option<&str>) -> Result<Value> {
        Err(AgentError::Tool {
            tool_id: "process".into(),
            message: "process_action is not configured on agent tool bridge".into(),
        })
    }

    async fn run_doctor(&self, _fix: bool) -> Result<Value> {
        Err(AgentError::Tool {
            tool_id: "doctor".into(),
            message: "run_doctor is not configured on agent tool bridge".into(),
        })
    }

    async fn notify_artifact_created(
        &self,
        _conversation_id: Option<&str>,
        _task_id: Option<&str>,
        _rel_path: &str,
        _title: &str,
    ) -> Result<()> {
        Ok(())
    }
}

#[derive(Clone)]
pub struct ToolExecContext {
    pub agent_id: String,
    pub task_id: Option<String>,
    pub conversation_id: Option<String>,
    pub approval_granted: bool,
    pub is_operator: bool,
    pub sandbox_mode: SandboxMode,
    pub approval_policy: SandboxApprovalPolicy,
    pub a2a_bridge: Option<Arc<dyn AgentToolBridge>>,
}

impl Default for ToolExecContext {
    fn default() -> Self {
        Self {
            agent_id: "__operator__".into(),
            task_id: None,
            conversation_id: None,
            approval_granted: false,
            is_operator: false,
            sandbox_mode: SandboxMode::FullSandbox,
            approval_policy: SandboxApprovalPolicy::FullAuto,
            a2a_bridge: None,
        }
    }
}

impl fmt::Debug for ToolExecContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ToolExecContext")
            .field("agent_id", &self.agent_id)
            .field("task_id", &self.task_id)
            .field("conversation_id", &self.conversation_id)
            .field("approval_granted", &self.approval_granted)
            .field("is_operator", &self.is_operator)
            .field("sandbox_mode", &self.sandbox_mode)
            .field("approval_policy", &self.approval_policy)
            .field("has_a2a_bridge", &self.a2a_bridge.is_some())
            .finish()
    }
}

pub struct ToolRegistry {
    storage: Arc<dyn StorageBackend>,
    config: RwLock<ToolsConfig>,
    http_client: RwLock<reqwest::Client>,
    skills: Arc<skills::SkillManager>,
    mcp: Arc<mcp::McpManager>,
    plugins: Arc<plugins::PluginManager>,
    custom: Arc<custom::CustomToolManager>,
    browser: Arc<browser::SandboxBrowserManager>,
    cached_specs: RwLock<Vec<ToolSpec>>,
    catalog_revision: AtomicU64,
}

impl ToolRegistry {
    pub fn new(storage: Arc<dyn StorageBackend>, config: ToolsConfig) -> Result<Self> {
        let data_dir = derive_data_dir(&config);
        Self::with_data_dir(storage, config, data_dir)
    }

    pub fn with_data_dir(
        storage: Arc<dyn StorageBackend>,
        config: ToolsConfig,
        data_dir: impl AsRef<Path>,
    ) -> Result<Self> {
        let data_dir = data_dir.as_ref();
        let workspace_dir = config.allowed_paths.first().map(PathBuf::from);
        let skills = Arc::new(skills::SkillManager::new(
            data_dir,
            workspace_dir.as_deref(),
        ));
        let mcp = Arc::new(mcp::McpManager::with_workspace_dir(
            data_dir,
            workspace_dir.as_deref(),
        ));
        let plugins = Arc::new(plugins::PluginManager::new(
            data_dir,
            workspace_dir.as_deref(),
        ));
        let custom = Arc::new(custom::CustomToolManager::new(
            data_dir,
            workspace_dir.as_deref(),
        ));
        let browser = Arc::new(browser::SandboxBrowserManager::new());
        let client = web::build_web_client(config.default_timeout_secs)?;
        let registry = Self {
            storage,
            config: RwLock::new(config),
            http_client: RwLock::new(client),
            skills,
            mcp,
            plugins,
            custom,
            browser,
            cached_specs: RwLock::new(Vec::new()),
            catalog_revision: AtomicU64::new(0),
        };
        registry.seed_tools()?;
        Ok(registry)
    }

    pub fn catalog_revision(&self) -> u64 {
        self.catalog_revision.load(Ordering::Acquire)
    }

    pub fn bump_catalog_revision(&self) -> u64 {
        self.catalog_revision.fetch_add(1, Ordering::AcqRel) + 1
    }

    pub fn skills(&self) -> &Arc<skills::SkillManager> {
        &self.skills
    }

    pub fn mcp(&self) -> &Arc<mcp::McpManager> {
        &self.mcp
    }

    pub fn plugins(&self) -> &Arc<plugins::PluginManager> {
        &self.plugins
    }

    pub fn custom(&self) -> &Arc<custom::CustomToolManager> {
        &self.custom
    }

    pub fn browser_manager(&self) -> &Arc<browser::SandboxBrowserManager> {
        &self.browser
    }

    pub fn discover_mcp_servers(&self) -> Result<Vec<mcp::McpServerView>> {
        let views = self.mcp.discover_all()?;
        self.bump_catalog_revision();
        Ok(views)
    }

    pub fn add_or_import_mcp_servers(&self, raw_json: &str) -> Result<Vec<mcp::McpServerView>> {
        let views = self.mcp.add_or_import(raw_json)?;
        self.bump_catalog_revision();
        Ok(views)
    }

    pub fn add_or_update_mcp_server(
        &self,
        id: &str,
        cfg: mcp::McpServerConfig,
    ) -> Result<mcp::McpServerView> {
        let view = self.mcp.add_or_update_server(id, cfg)?;
        self.bump_catalog_revision();
        Ok(view)
    }

    pub async fn sync_mcp_server(&self, id: &str) -> Result<mcp::McpServerView> {
        let client = self.http_client()?;
        let cfg = self.config();
        let view = self.mcp.sync_server(&client, id, &cfg).await?;
        self.bump_catalog_revision();
        Ok(view)
    }

    pub fn set_mcp_server_enabled(&self, id: &str, enabled: bool) -> Result<mcp::McpServerView> {
        let view = self.mcp.set_enabled(id, enabled)?;
        self.bump_catalog_revision();
        Ok(view)
    }

    pub fn remove_mcp_server(&self, id: &str) -> Result<()> {
        self.mcp.remove_server(id)?;
        self.bump_catalog_revision();
        Ok(())
    }

    pub fn reload_plugins(&self) -> Result<()> {
        self.plugins.reload()?;
        self.bump_catalog_revision();
        Ok(())
    }

    pub fn import_plugin_json(&self, manifest_json: &str) -> Result<plugins::PluginView> {
        let view = self.plugins.import_plugin_json(manifest_json)?;
        self.bump_catalog_revision();
        Ok(view)
    }

    pub fn set_plugin_enabled(&self, id: &str, enabled: bool) -> Result<plugins::PluginView> {
        let view = self.plugins.set_enabled(id, enabled)?;
        self.bump_catalog_revision();
        Ok(view)
    }

    pub fn delete_plugin(&self, id: &str) -> Result<()> {
        self.plugins.delete_plugin(id)?;
        self.bump_catalog_revision();
        Ok(())
    }

    pub fn import_skill(
        &self,
        slug: Option<&str>,
        markdown_content: &str,
    ) -> Result<skills::SkillSummary> {
        let summary = self.skills.import_skill(slug, markdown_content)?;
        self.bump_catalog_revision();
        Ok(summary)
    }

    pub fn set_skill_enabled(&self, id: &str, enabled: bool) -> Result<skills::SkillSummary> {
        let summary = self.skills.set_enabled(id, enabled)?;
        self.bump_catalog_revision();
        Ok(summary)
    }

    pub fn delete_skill(&self, id: &str) -> Result<()> {
        self.skills.delete_skill(id)?;
        self.bump_catalog_revision();
        Ok(())
    }

    pub fn http_client(&self) -> Result<reqwest::Client> {
        self.http_client
            .read()
            .map_err(|_| AgentError::Internal("tools http client lock poisoned".into()))
            .map(|g| g.clone())
    }

    /// Seeds all built-in tools in `StorageBackend::upsert_tool_spec` and merges
    /// file-backed custom tools, preserving existing runtime overrides
    /// (`enabled`, `requires_approval`, `timeout_secs`), and populates the in-memory spec cache.
    pub fn seed_tools(&self) -> Result<()> {
        let default_timeout = self
            .config
            .read()
            .map(|c| c.default_timeout_secs)
            .unwrap_or(20);
        let mut existing: HashMap<String, ToolSpec> = self
            .storage
            .list_tool_specs()?
            .into_iter()
            .map(|s| (s.id.clone(), s))
            .collect();

        let builtins = specs::builtin_tool_specs(default_timeout);
        let builtin_count = builtins.len();
        let mut seeded = Vec::with_capacity(builtin_count);
        for mut spec in builtins {
            if let Some(prev) = existing.remove(&spec.id) {
                spec.enabled = prev.enabled;
                spec.requires_approval = prev.requires_approval;
                spec.timeout_secs = prev.timeout_secs;
            }
            self.storage.upsert_tool_spec(&spec)?;
            seeded.push(spec);
        }

        let custom_defs = self.custom.list_definitions();
        for def in custom_defs {
            let mut spec = def.to_tool_spec();
            if let Some(prev) = existing.remove(&spec.id) {
                if def.source == "workspace" {
                    spec.enabled = prev.enabled;
                    spec.requires_approval = prev.requires_approval;
                    spec.timeout_secs = prev.timeout_secs;
                }
            }
            self.storage.upsert_tool_spec(&spec)?;
            seeded.push(spec);
        }

        let mut extra: Vec<ToolSpec> = existing.into_values().collect();
        extra.sort_by(|a, b| a.id.cmp(&b.id));
        seeded.extend(extra);

        {
            let mut cache = self
                .cached_specs
                .write()
                .map_err(|_| AgentError::Internal("tools spec cache lock poisoned".into()))?;
            *cache = seeded;
        }
        self.bump_catalog_revision();
        log_info("tools", format!("seeded {builtin_count} built-in tool specs"));
        Ok(())
    }

    /// Reloads custom tools from disk and refreshes the in-memory spec cache from
    /// `StorageBackend::list_tool_specs()`, keeping custom tools synchronized.
    pub fn sync_with_storage(&self) -> Result<()> {
        self.custom.reload();
        let mut existing: HashMap<String, ToolSpec> = self
            .storage
            .list_tool_specs()?
            .into_iter()
            .map(|s| (s.id.clone(), s))
            .collect();

        for def in self.custom.list_definitions() {
            let mut spec = def.to_tool_spec();
            if let Some(prev) = existing.get(&spec.id) {
                if def.source == "workspace" {
                    spec.enabled = prev.enabled;
                    spec.requires_approval = prev.requires_approval;
                    spec.timeout_secs = prev.timeout_secs;
                }
            }
            self.storage.upsert_tool_spec(&spec)?;
            existing.insert(spec.id.clone(), spec);
        }

        let mut specs_list: Vec<ToolSpec> = existing.into_values().collect();
        specs_list.sort_by(|a, b| a.id.cmp(&b.id));
        {
            let mut cache = self
                .cached_specs
                .write()
                .map_err(|_| AgentError::Internal("tools spec cache lock poisoned".into()))?;
            *cache = specs_list;
        }
        self.bump_catalog_revision();
        Ok(())
    }

    /// Refreshes the in-memory built-in/stored/custom spec cache from storage and disk.
    pub fn sync_specs(&self) -> Result<()> {
        self.sync_with_storage()
    }

    /// Toggles (or explicitly sets) `enabled` for a stored/built-in/custom tool, updating
    /// `StorageBackend`, custom tool JSON (if applicable), and the in-memory cache.
    pub fn toggle_tool(&self, tool_id: &str, enabled: Option<bool>) -> Result<ToolSpec> {
        let canonical = canonical_tool_id(tool_id);
        let target_enabled = match enabled {
            Some(v) => v,
            None => {
                let current = self
                    .get_spec(tool_id)?
                    .ok_or_else(|| AgentError::NotFound(format!("tool '{tool_id}' not found")))?;
                !current.enabled
            }
        };
        if canonical != tool_id && self.get_spec(&canonical)?.is_some() {
            self.update_tool_policy(&canonical, Some(target_enabled), None, None)
        } else {
            self.update_tool_policy(tool_id, Some(target_enabled), None, None)
        }
    }

    /// Registers or imports one or more custom tools from a JSON payload (`CustomToolDefinition`,
    /// array, `{"tools":[...]}`, or OpenAI `{"type":"function","function":{...}}`),
    /// persists each to `<data_dir>/tools/<sanitized_id>.json` + `StorageBackend`, and updates cache.
    pub fn register_or_import_custom_tools(&self, payload: &Value) -> Result<Vec<ToolSpec>> {
        let defs = self.custom.register_or_import(payload)?;
        let mut specs_out = Vec::with_capacity(defs.len());
        for def in defs {
            let spec = def.to_tool_spec();
            self.upsert_tool_spec(&spec)?;
            specs_out.push(spec);
        }
        Ok(specs_out)
    }

    /// Updates runtime policy flags (`enabled`, `requires_approval`, `timeout_secs`) on a
    /// built-in, stored, or custom tool, persisting to `StorageBackend` (and `<data_dir>/tools/`
    /// for custom tools) plus the in-memory cache.
    pub fn update_tool_policy(
        &self,
        tool_id: &str,
        enabled: Option<bool>,
        requires_approval: Option<bool>,
        timeout_secs: Option<u64>,
    ) -> Result<ToolSpec> {
        let canonical = canonical_tool_id(tool_id);
        if let Some(updated_def) =
            self.custom
                .update_policy(tool_id, enabled, requires_approval, timeout_secs)?
        {
            let spec = updated_def.to_tool_spec();
            self.upsert_tool_spec(&spec)?;
            return Ok(spec);
        }

        let updated = {
            let mut cache = self
                .cached_specs
                .write()
                .map_err(|_| AgentError::Internal("tools spec cache lock poisoned".into()))?;
            let spec = cache
                .iter_mut()
                .find(|s| s.id == tool_id || s.id == canonical)
                .ok_or_else(|| AgentError::NotFound(format!("tool '{tool_id}' not found")))?;
            if let Some(val) = enabled {
                spec.enabled = val;
            }
            if let Some(val) = requires_approval {
                spec.requires_approval = val;
            }
            if let Some(val) = timeout_secs {
                spec.timeout_secs = val;
            }
            spec.clone()
        };
        self.storage.upsert_tool_spec(&updated)?;
        self.bump_catalog_revision();
        Ok(updated)
    }

    /// Deletes a custom tool from `CustomToolManager`, `StorageBackend`, and the in-memory
    /// spec cache. Refuses deleting built-in tools.
    pub fn delete_custom_tool(&self, tool_id: &str) -> Result<bool> {
        let trimmed = tool_id.trim();
        let canonical = canonical_tool_id(trimmed);
        if custom::builtin_tool_id_set().contains(trimmed)
            || custom::builtin_tool_id_set().contains(canonical)
        {
            return Err(AgentError::PermissionDenied(format!(
                "cannot delete built-in tool '{trimmed}'"
            )));
        }

        let deleted_file = self.custom.delete_tool(trimmed)?;
        let deleted_storage = self.storage.delete_tool_spec(trimmed)?;
        let deleted_cache = {
            let mut cache = self
                .cached_specs
                .write()
                .map_err(|_| AgentError::Internal("tools spec cache lock poisoned".into()))?;
            let before = cache.len();
            cache.retain(|s| s.id != trimmed);
            cache.len() != before
        };

        let changed = deleted_file || deleted_storage || deleted_cache;
        if changed {
            self.bump_catalog_revision();
        }
        Ok(changed)
    }

    /// Upserts a `ToolSpec` into `StorageBackend` and updates the in-memory spec cache.
    pub fn upsert_tool_spec(&self, spec: &ToolSpec) -> Result<()> {
        self.storage.upsert_tool_spec(spec)?;
        {
            let mut cache = self
                .cached_specs
                .write()
                .map_err(|_| AgentError::Internal("tools spec cache lock poisoned".into()))?;
            if let Some(existing) = cache.iter_mut().find(|s| s.id == spec.id) {
                *existing = spec.clone();
            } else {
                cache.push(spec.clone());
            }
        }
        self.bump_catalog_revision();
        Ok(())
    }

    pub fn update_config(&self, config: ToolsConfig) -> Result<()> {
        let client = web::build_web_client(config.default_timeout_secs)?;
        {
            let mut c_guard = self
                .config
                .write()
                .map_err(|_| AgentError::Internal("tools config lock poisoned".into()))?;
            *c_guard = config;
        }
        {
            let mut h_guard = self
                .http_client
                .write()
                .map_err(|_| AgentError::Internal("tools http client lock poisoned".into()))?;
            *h_guard = client;
        }
        log_info("tools", "updated ToolRegistry configuration");
        Ok(())
    }

    pub fn config(&self) -> ToolsConfig {
        self.config
            .read()
            .map(|c| c.clone())
            .expect("tools config lock poisoned")
    }

    pub fn list_specs(&self) -> Result<Vec<ToolSpec>> {
        let mut all = self
            .cached_specs
            .read()
            .map_err(|_| AgentError::Internal("tools spec cache lock poisoned".into()))?
            .clone();
        let mut seen: HashSet<String> = all.iter().map(|s| s.id.clone()).collect();
        for spec in self.custom.list_tool_specs() {
            if seen.insert(spec.id.clone()) {
                all.push(spec);
            }
        }
        all.extend(self.mcp.list_tool_specs());
        all.extend(self.plugins.list_tool_specs());
        Ok(all)
    }

    pub fn get_spec(&self, tool_id: &str) -> Result<Option<ToolSpec>> {
        let canonical = canonical_tool_id(tool_id);
        {
            let cache = self
                .cached_specs
                .read()
                .map_err(|_| AgentError::Internal("tools spec cache lock poisoned".into()))?;
            if let Some(found) = cache
                .iter()
                .find(|s| s.id == tool_id || s.id == canonical)
                .cloned()
            {
                return Ok(Some(found));
            }
        }
        if let Some(def) = self.custom.get_definition(tool_id) {
            return Ok(Some(def.to_tool_spec()));
        }
        if tool_id.starts_with("mcp:") {
            return Ok(self
                .mcp
                .list_tool_specs()
                .into_iter()
                .find(|s| s.id == tool_id));
        }
        if tool_id.starts_with("plugin:") {
            return Ok(self
                .plugins
                .list_tool_specs()
                .into_iter()
                .find(|s| s.id == tool_id));
        }
        Ok(None)
    }

    pub fn tool_definitions_for_agent(
        &self,
        agent: &AgentDefinition,
    ) -> Result<Vec<ToolDefinitionSchema>> {
        if !agent.enabled {
            return Ok(Vec::new());
        }
        let specs = self.list_specs()?;
        Ok(specs
            .into_iter()
            .filter(|s| {
                let effective_category = if self.custom.has_tool(&s.id) {
                    "custom"
                } else {
                    s.category.as_str()
                };
                s.enabled
                    && !is_disallowed_schedule_tool(&agent.id, &s.id)
                    && agent_allows_tool_with_policy(
                        &agent.enabled_tools,
                        &agent.denied_tools,
                        &s.id,
                        effective_category,
                    )
            })
            .map(|s| ToolDefinitionSchema {
                name: s.id,
                description: s.description,
                parameters: s.input_schema,
            })
            .collect())
    }

    /// Lists enabled skills from `SkillManager` plus enabled plugin-bundled skills.
    pub fn list_enabled_skills(&self) -> Vec<skills::SkillSummary> {
        let mut by_id: BTreeMap<String, skills::SkillSummary> = BTreeMap::new();
        for summary in self.skills.list_enabled_summaries() {
            by_id.insert(summary.id.clone(), summary);
        }
        for bundled in self.plugins.enabled_bundled_skills() {
            if by_id.contains_key(&bundled.id) {
                continue;
            }
            if let Ok(doc) = load_bundled_plugin_skill(&bundled) {
                by_id.insert(doc.summary.id.clone(), doc.summary);
            }
        }
        by_id.into_values().collect()
    }

    /// Loads an enabled skill by ID or name, searching `SkillManager` first and then
    /// enabled plugin-bundled skills.
    pub fn load_skill(&self, id_or_name: &str) -> Result<skills::SkillDocument> {
        load_skill_across_managers(&self.skills, &self.plugins, id_or_name)
    }

    pub async fn invoke(
        &self,
        tool_id: &str,
        args: Value,
        ctx: &ToolExecContext,
    ) -> Result<Value> {
        let canonical = canonical_tool_id(tool_id);
        let spec = self
            .get_spec(canonical)?
            .ok_or_else(|| AgentError::NotFound(format!("tool '{tool_id}' not found")))?;

        if !spec.enabled {
            return Err(AgentError::PermissionDenied(format!(
                "tool '{tool_id}' is disabled"
            )));
        }

        let is_operator = ctx.is_operator || ctx.agent_id == "__operator__";
        if !is_operator {
            let agent = self
                .storage
                .get_agent(&ctx.agent_id)?
                .ok_or_else(|| {
                    AgentError::PermissionDenied(format!(
                        "agent '{}' not found or not permitted to execute tools",
                        ctx.agent_id
                    ))
                })?;

            if !agent.enabled {
                return Err(AgentError::PermissionDenied(format!(
                    "agent '{}' is disabled",
                    ctx.agent_id
                )));
            }

            if is_disallowed_schedule_tool(&agent.id, &spec.id) {
                return Err(AgentError::PermissionDenied(format!(
                    "tool '{}' is restricted to commander and executor agents",
                    spec.id
                )));
            }

            let effective_category = if self.custom.has_tool(&spec.id) {
                "custom"
            } else {
                spec.category.as_str()
            };
            if !agent_allows_tool_with_policy(
                &agent.enabled_tools,
                &agent.denied_tools,
                &spec.id,
                effective_category,
            ) {
                return Err(AgentError::PermissionDenied(format!(
                    "agent '{}' does not have tool '{}' enabled",
                    ctx.agent_id, tool_id
                )));
            }
        }

        check_sandbox_mode(&spec.id, &spec, &args, ctx.sandbox_mode)?;

        if !(ctx.approval_granted || is_operator) {
            let is_builtin = is_builtin_tool(&spec.id);
            let needs_approval = match ctx.approval_policy {
                SandboxApprovalPolicy::FullAuto => !is_builtin && spec.requires_approval,
                SandboxApprovalPolicy::OnRequest => spec.requires_approval,
                SandboxApprovalPolicy::AlwaysAsk => spec.is_mutating || spec.requires_approval,
                SandboxApprovalPolicy::Never => {
                    if spec.requires_approval {
                        return Err(AgentError::PermissionDenied(format!(
                            "tool '{}' requires approval which is blocked by approval policy 'never'",
                            spec.id
                        )));
                    }
                    false
                }
            };

            if needs_approval {
                let approval_id = new_id();
                let reason = format!(
                    "Tool '{}' requires operator approval before execution",
                    spec.id
                );
                let approval = ApprovalRecord {
                    id: approval_id.clone(),
                    task_id: ctx.task_id.clone(),
                    agent_id: ctx.agent_id.clone(),
                    operation: format!("tool:{}", spec.id),
                    tool_id: Some(spec.id.clone()),
                    arguments_json: truncate_json(&args, 16 * 1024),
                    reason: reason.clone(),
                    status: ApprovalStatus::Pending,
                    created_at: now_iso(),
                    resolved_at: None,
                    resolved_by: None,
                };
                self.storage.upsert_approval(&approval)?;
                log_warn(
                    "tools",
                    format!(
                        "approval required ({approval_id}) for agent='{}' tool='{}'",
                        ctx.agent_id, spec.id
                    ),
                );
                return Err(AgentError::ApprovalRequired {
                    approval_id,
                    reason,
                });
            }
        }

        let cfg = self.config();
        let client = self.http_client()?;
        let started = Instant::now();

        log_debug(
            "tools",
            format!("invoking tool='{}' agent='{}'", spec.id, ctx.agent_id),
        );

        let result: Result<Value> = if spec.timeout_secs == 0 {
            self.dispatch_tool(tool_id, &args, &cfg, &client, ctx)
                .await
        } else {
            let timeout_secs = spec.timeout_secs;
            match tokio::time::timeout(
                Duration::from_secs(timeout_secs),
                self.dispatch_tool(tool_id, &args, &cfg, &client, ctx),
            )
            .await
            {
                Ok(inner) => inner,
                Err(_) => Err(AgentError::Timeout(format!(
                    "tool '{}' timed out after {timeout_secs}s",
                    spec.id
                ))),
            }
        };

        let duration_ms = started.elapsed().as_millis() as u64;

        let (success, output_json, error_msg) = match &result {
            Ok(val) => (true, Some(truncate_json(val, 64 * 1024)), None),
            Err(err) => (false, None, Some(err.to_string())),
        };

        let rec = ToolExecutionRecord {
            id: new_id(),
            tool_id: tool_id.to_string(),
            agent_id: ctx.agent_id.clone(),
            task_id: ctx.task_id.clone(),
            input_json: truncate_json(&args, 16 * 1024),
            output_json,
            success,
            error: error_msg,
            duration_ms,
            created_at: now_iso(),
        };
        if let Err(e) = self.storage.record_tool_execution(&rec) {
            log_warn("tools", format!("failed to record tool execution: {e}"));
        }

        result
    }

    async fn dispatch_tool(
        &self,
        tool_id: &str,
        args: &Value,
        cfg: &ToolsConfig,
        client: &reqwest::Client,
        ctx: &ToolExecContext,
    ) -> Result<Value> {
        let canonical = canonical_tool_id(tool_id);
        match canonical {
            // Web
            "http_get" => web::exec_http_get(args, cfg, client).await,
            "http_post" => web::exec_http_post(args, cfg, client).await,
            "fetch_website" => web::exec_fetch_website(args, cfg, client).await,
            "extract_html_text" => web::exec_extract_html_text(args),
            "json_api_request" => {
                let approved = ctx.approval_granted
                    || ctx.is_operator
                    || ctx.approval_policy == SandboxApprovalPolicy::FullAuto;
                web::exec_json_api_request(args, cfg, client, approved).await
            }
            "web_search" => web::exec_web_search(args, cfg, client).await,
            "web_fetch" => web::exec_web_fetch(args, cfg, client).await,
            "browser" => {
                self.browser
                    .execute_with_meta(
                        args,
                        cfg,
                        client,
                        Some(browser::BrowserActivityContext {
                            conversation_id: ctx.conversation_id.clone(),
                            task_id: ctx.task_id.clone(),
                            agent_id: Some(ctx.agent_id.clone()),
                        }),
                    )
                    .await
            }
            // File & Code Search / Patch (blocking fs wrapped in spawn_blocking)
            "list_files" => {
                run_blocking_fs("list_files", args.clone(), cfg.clone(), files::exec_list_files).await
            }
            "read_file" => {
                run_blocking_fs("read_file", args.clone(), cfg.clone(), files::exec_read_file).await
            }
            "write_file" => {
                run_blocking_fs("write_file", args.clone(), cfg.clone(), files::exec_write_file).await
            }
            "create_directory" => {
                run_blocking_fs(
                    "create_directory",
                    args.clone(),
                    cfg.clone(),
                    files::exec_create_directory,
                )
                .await
            }
            "delete_file" => {
                run_blocking_fs("delete_file", args.clone(), cfg.clone(), files::exec_delete_file).await
            }
            "edit_file" => {
                run_blocking_fs("edit_file", args.clone(), cfg.clone(), patch::exec_edit_file).await
            }
            "apply_patch" => {
                run_blocking_fs("apply_patch", args.clone(), cfg.clone(), patch::exec_apply_patch)
                    .await
            }
            "grep_search" => {
                run_blocking_fs("grep_search", args.clone(), cfg.clone(), patch::exec_grep_search)
                    .await
            }
            "glob_files" => {
                run_blocking_fs("glob_files", args.clone(), cfg.clone(), patch::exec_glob_files)
                    .await
            }
            // Runtime Sandbox Execution & Processes
            "sandbox_exec" => sandbox_exec::exec_sandbox_exec(args, cfg, ctx.sandbox_mode).await,
            "process" => {
                let action = args
                    .get("action")
                    .and_then(|v| v.as_str())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| {
                        AgentError::InvalidInput("missing required string 'action'".into())
                    })?;
                let target_id = args
                    .get("target_id")
                    .and_then(|v| v.as_str())
                    .map(str::trim)
                    .filter(|s| !s.is_empty());
                let bridge = require_bridge(ctx, canonical)?;
                bridge.process_action(action, target_id).await
            }
            // Memory
            "memory_read" => {
                let bridge = require_bridge(ctx, tool_id)?;
                let target_agent_id = args
                    .get("target_agent_id")
                    .and_then(|v| v.as_str())
                    .map(str::trim)
                    .filter(|s| !s.is_empty());
                if tool_id.trim() == "read_agent_memory" || target_agent_id.is_some() {
                    let target = target_agent_id.ok_or_else(|| {
                        AgentError::InvalidInput("missing 'target_agent_id'".into())
                    })?;
                    let query = args.get("query").and_then(|v| v.as_str());
                    let limit = args
                        .get("limit")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(10)
                        .clamp(1, 100) as usize;
                    bridge
                        .read_agent_memory(&ctx.agent_id, target, query, limit)
                        .await
                } else {
                    let scope = args
                        .get("scope")
                        .and_then(|v| v.as_str())
                        .map(str::trim)
                        .filter(|s| !s.is_empty());
                    let namespace = args
                        .get("namespace")
                        .and_then(|v| v.as_str())
                        .map(str::trim)
                        .filter(|s| !s.is_empty());
                    let query = args
                        .get("query")
                        .and_then(|v| v.as_str())
                        .map(str::trim)
                        .filter(|s| !s.is_empty());
                    let limit = args
                        .get("limit")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(20)
                        .clamp(1, 100) as usize;
                    bridge
                        .memory_read(&ctx.agent_id, scope, namespace, query, limit)
                        .await
                }
            }
            "memory_write" => {
                let bridge = require_bridge(ctx, canonical)?;
                let scope = args
                    .get("scope")
                    .and_then(|v| v.as_str())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .unwrap_or("agent");
                let namespace = args
                    .get("namespace")
                    .and_then(|v| v.as_str())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .unwrap_or("general");
                let key = args
                    .get("key")
                    .and_then(|v| v.as_str())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| {
                        AgentError::InvalidInput("missing required string 'key'".into())
                    })?;
                let content = args
                    .get("content")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        AgentError::InvalidInput("missing required string 'content'".into())
                    })?;
                let importance = args
                    .get("importance")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(5)
                    .clamp(1, 10) as u8;
                bridge
                    .memory_write(
                        &ctx.agent_id,
                        scope,
                        namespace,
                        key,
                        content,
                        importance,
                        ctx.conversation_id.as_deref(),
                        ctx.task_id.as_deref(),
                    )
                    .await
            }
            "memory_search" => {
                let bridge = require_bridge(ctx, canonical)?;
                let query = args
                    .get("query")
                    .and_then(|v| v.as_str())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| {
                        AgentError::InvalidInput("missing required string 'query'".into())
                    })?;
                let max_results = args
                    .get("max_results")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(10)
                    .clamp(1, 100) as usize;
                bridge
                    .memory_search(&ctx.agent_id, query, max_results)
                    .await
            }
            "memory_get" => {
                let bridge = require_bridge(ctx, canonical)?;
                let path = args
                    .get("path")
                    .and_then(|v| v.as_str())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| {
                        AgentError::InvalidInput("missing required string 'path'".into())
                    })?;
                let from_line = args
                    .get("from_line")
                    .and_then(|v| v.as_u64())
                    .map(|n| n.max(1) as usize);
                let lines = args
                    .get("lines")
                    .and_then(|v| v.as_u64())
                    .map(|n| n.clamp(1, 2000) as usize);
                bridge
                    .memory_get(&ctx.agent_id, path, from_line, lines)
                    .await
            }
            // Utility / Info / Doctor / Artifacts
            "calculator" => utility::exec_calculator(args),
            "datetime" => utility::exec_datetime(args),
            "json_process" => utility::exec_json_process(args),
            "text_process" => utility::exec_text_process(args),
            "system_info" => {
                if let Some(bridge) = &ctx.a2a_bridge {
                    bridge.system_info().await
                } else {
                    Ok(json!({
                        "os": std::env::consts::OS,
                        "arch": std::env::consts::ARCH,
                        "family": std::env::consts::FAMILY,
                        "sandbox_mode": ctx.sandbox_mode.as_str(),
                        "approval_policy": ctx.approval_policy.as_str(),
                        "workspace_root": cfg.allowed_paths.first(),
                        "network_access": cfg.network_access,
                        "allow_host_shell": cfg.allow_host_shell,
                    }))
                }
            }
            "ps5_info" => {
                if let Some(bridge) = &ctx.a2a_bridge {
                    bridge.ps5_info().await
                } else {
                    Ok(json!({
                        "target_vendor": if cfg!(target_vendor = "sie") { "sie" } else { std::env::consts::OS },
                        "is_ps5": cfg!(target_vendor = "sie"),
                        "virtual_shell_default": true,
                        "host_shell_available": !cfg!(target_vendor = "sie") && cfg.allow_host_shell,
                        "network_access": cfg.network_access,
                        "sandbox_mode": ctx.sandbox_mode.as_str(),
                        "approval_policy": ctx.approval_policy.as_str(),
                    }))
                }
            }
            "doctor" => {
                let bridge = require_bridge(ctx, canonical)?;
                let fix = args.get("fix").and_then(|v| v.as_bool()).unwrap_or(false);
                bridge.run_doctor(fix).await
            }
            "write_artifact" => {
                let args_owned = args.clone();
                let cfg_owned = cfg.clone();
                let (rel_path, title, bytes_written) =
                    tokio::task::spawn_blocking(move || {
                        write_artifact_blocking(&args_owned, &cfg_owned)
                    })
                    .await
                    .map_err(|e| AgentError::Tool {
                        tool_id: "write_artifact".into(),
                        message: format!("blocking artifact task panicked: {e}"),
                    })??;
                if let Some(bridge) = &ctx.a2a_bridge {
                    bridge
                        .notify_artifact_created(
                            ctx.conversation_id.as_deref(),
                            ctx.task_id.as_deref(),
                            &rel_path,
                            &title,
                        )
                        .await?;
                }
                Ok(json!({
                    "ok": true,
                    "path": rel_path,
                    "title": title,
                    "bytes_written": bytes_written,
                    "marker": format!("[ARTIFACT_CREATED: {rel_path}]"),
                }))
            }
            // Skills
            "load_skill" => {
                let skill_query = args
                    .get("skill")
                    .or_else(|| args.get("id"))
                    .or_else(|| args.get("name"))
                    .or_else(|| args.get("skill_id"))
                    .and_then(|v| v.as_str())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| AgentError::InvalidInput("missing required string 'skill'".into()))?
                    .to_string();

                let skills_mgr = self.skills.clone();
                let plugins_mgr = self.plugins.clone();
                let doc = tokio::task::spawn_blocking(move || {
                    load_skill_across_managers(&skills_mgr, &plugins_mgr, &skill_query)
                })
                .await
                .map_err(|e| AgentError::Tool {
                    tool_id: "load_skill".into(),
                    message: format!("blocking skill task panicked: {e}"),
                })??;

                Ok(json!({
                    "id": doc.summary.id,
                    "name": doc.summary.name,
                    "description": doc.summary.description,
                    "source": doc.summary.source,
                    "dir_path": doc.summary.dir_path,
                    "has_scripts": doc.summary.has_scripts,
                    "has_references": doc.summary.has_references,
                    "resources": doc.summary.resources,
                    "instructions": doc.instructions,
                }))
            }
            // Agent & Sessions
            "send_agent_message" => {
                let bridge = require_bridge(ctx, tool_id)?;
                let recipient = args
                    .get("recipient_agent_id")
                    .or_else(|| args.get("to_agent_id"))
                    .or_else(|| args.get("agent_id"))
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| AgentError::InvalidInput("missing 'recipient_agent_id'".into()))?;
                let content = args
                    .get("content")
                    .or_else(|| args.get("message"))
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| AgentError::InvalidInput("missing 'content'".into()))?;
                bridge
                    .send_agent_message(&ctx.agent_id, recipient, ctx.task_id.as_deref(), content)
                    .await
            }
            "assign_agent_task" => {
                let bridge = require_bridge(ctx, tool_id)?;
                let target = args
                    .get("target_agent_id")
                    .or_else(|| args.get("agent_id"))
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| AgentError::InvalidInput("missing 'target_agent_id'".into()))?;
                let user_request = args
                    .get("user_request")
                    .or_else(|| args.get("request"))
                    .or_else(|| args.get("task"))
                    .or_else(|| args.get("prompt"))
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| AgentError::InvalidInput("missing 'user_request'".into()))?;
                bridge
                    .assign_agent_task(
                        &ctx.agent_id,
                        target,
                        user_request,
                        ctx.task_id.as_deref(),
                        ctx.conversation_id.as_deref(),
                    )
                    .await
            }
            "query_agent_status" => {
                let bridge = require_bridge(ctx, tool_id)?;
                let target = args.get("target_agent_id").and_then(|v| v.as_str());
                let task_id = args.get("task_id").and_then(|v| v.as_str());
                bridge
                    .query_agent_status(&ctx.agent_id, target, task_id)
                    .await
            }
            "request_temporary_specialist" => {
                let bridge = require_bridge(ctx, tool_id)?;
                let name = args
                    .get("name")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| AgentError::InvalidInput("missing 'name'".into()))?;
                let role_label = args
                    .get("role_label")
                    .or_else(|| args.get("description"))
                    .and_then(|v| v.as_str())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .or_else(|| (tool_id.trim() == "spawn_temp_agent").then_some("Temporary specialist"))
                    .ok_or_else(|| AgentError::InvalidInput("missing 'role_label'".into()))?;
                let system_prompt = args
                    .get("system_prompt")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| AgentError::InvalidInput("missing 'system_prompt'".into()))?;
                let enabled_tools = args
                    .get("enabled_tools")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str().map(str::to_string))
                            .collect()
                    })
                    .unwrap_or_default();
                let task_request = args
                    .get("task_request")
                    .or_else(|| args.get("task"))
                    .and_then(|v| v.as_str());
                bridge
                    .request_temporary_specialist(
                        &ctx.agent_id,
                        name,
                        role_label,
                        system_prompt,
                        enabled_tools,
                        task_request,
                        ctx.task_id.as_deref(),
                    )
                    .await
            }
            "sessions_list" => {
                let bridge = require_bridge(ctx, canonical)?;
                let limit = args
                    .get("limit")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(50)
                    .clamp(1, 200) as usize;
                let active_only = args
                    .get("active_only")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                bridge.sessions_list(limit, active_only).await
            }
            "sessions_spawn" => {
                let bridge = require_bridge(ctx, canonical)?;
                let target_agent_id = args
                    .get("target_agent_id")
                    .or_else(|| args.get("agent_id"))
                    .and_then(|v| v.as_str())
                    .map(str::trim)
                    .filter(|s| !s.is_empty());
                let conversation_id = args
                    .get("conversation_id")
                    .and_then(|v| v.as_str())
                    .map(str::trim)
                    .filter(|s| !s.is_empty());
                let prompt = args
                    .get("prompt")
                    .or_else(|| args.get("user_request"))
                    .and_then(|v| v.as_str())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| {
                        AgentError::InvalidInput("missing required string 'prompt'".into())
                    })?;
                bridge
                    .sessions_spawn(&ctx.agent_id, target_agent_id, conversation_id, prompt)
                    .await
            }
            "sessions_send" => {
                let bridge = require_bridge(ctx, canonical)?;
                let conversation_id = args
                    .get("conversation_id")
                    .and_then(|v| v.as_str())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| {
                        AgentError::InvalidInput("missing required string 'conversation_id'".into())
                    })?;
                let message = args
                    .get("message")
                    .or_else(|| args.get("content"))
                    .and_then(|v| v.as_str())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| {
                        AgentError::InvalidInput("missing required string 'message'".into())
                    })?;
                bridge
                    .sessions_send(&ctx.agent_id, conversation_id, message)
                    .await
            }
            "session_status" => {
                let bridge = require_bridge(ctx, canonical)?;
                let conversation_id = args
                    .get("conversation_id")
                    .and_then(|v| v.as_str())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .or(ctx.conversation_id.as_deref());
                let task_id = args
                    .get("task_id")
                    .and_then(|v| v.as_str())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .or(ctx.task_id.as_deref());
                bridge.session_status(conversation_id, task_id).await
            }
            id if id.starts_with("mcp:") => {
                self.mcp
                    .call_tool(client, id, args.clone(), cfg)
                    .await
            }
            id if id.starts_with("plugin:") => {
                self.plugins
                    .execute_tool(client, id, args.clone(), cfg)
                    .await
            }
            id if self.custom.has_tool(id) => {
                self.custom
                    .execute_tool(client, id, args, cfg)
                    .await
            }
            other => Err(AgentError::NotFound(format!("unknown tool '{other}'"))),
        }
    }
}

fn load_skill_across_managers(
    skills_mgr: &skills::SkillManager,
    plugins_mgr: &plugins::PluginManager,
    id_or_name: &str,
) -> Result<skills::SkillDocument> {
    let query = id_or_name.trim();
    if query.is_empty() {
        return Err(AgentError::InvalidInput(
            "skill id or name cannot be empty".into(),
        ));
    }

    match skills_mgr.load_skill(query) {
        Ok(doc) => {
            if !doc.summary.enabled {
                return Err(AgentError::PermissionDenied(format!(
                    "skill '{}' is disabled",
                    doc.summary.id
                )));
            }
            return Ok(doc);
        }
        Err(AgentError::NotFound(_)) => {}
        Err(other) => return Err(other),
    }

    let bundled = plugins_mgr.enabled_bundled_skills();
    let mut loaded_docs = Vec::with_capacity(bundled.len());
    for b in &bundled {
        if let Ok(doc) = load_bundled_plugin_skill(b) {
            if b.id == query
                || format!("{}:{}", b.plugin_id, b.id) == query
                || format!("skill:{}:{}", b.plugin_id, b.id) == query
            {
                return Ok(doc);
            }
            loaded_docs.push((b, doc));
        }
    }

    if let Some((_, doc)) = loaded_docs
        .iter()
        .find(|(b, _)| {
            b.id.eq_ignore_ascii_case(query)
                || format!("{}:{}", b.plugin_id, b.id).eq_ignore_ascii_case(query)
                || format!("skill:{}:{}", b.plugin_id, b.id).eq_ignore_ascii_case(query)
        })
        .or_else(|| {
            loaded_docs
                .iter()
                .find(|(_, d)| d.summary.name.eq_ignore_ascii_case(query))
        })
    {
        return Ok(doc.clone());
    }

    Err(AgentError::NotFound(format!("skill '{query}' not found")))
}

fn load_bundled_plugin_skill(
    bundled: &plugins::PluginBundledSkill,
) -> Result<skills::SkillDocument> {
    let meta = fs::symlink_metadata(&bundled.skill_md_path).map_err(|e| {
        AgentError::Storage(format!(
            "failed to stat bundled skill '{}': {e}",
            bundled.skill_md_path.display()
        ))
    })?;
    if meta.file_type().is_symlink() || !meta.is_file() {
        return Err(AgentError::PermissionDenied(
            "bundled skill SKILL.md cannot be a symlink".into(),
        ));
    }
    if meta.len() > MAX_BUNDLED_SKILL_BYTES {
        return Err(AgentError::LimitExceeded(format!(
            "bundled skill '{}' exceeds {MAX_BUNDLED_SKILL_BYTES} bytes",
            bundled.id
        )));
    }

    let raw_markdown = fs::read_to_string(&bundled.skill_md_path).map_err(|e| {
        AgentError::Storage(format!(
            "failed to read bundled skill '{}': {e}",
            bundled.skill_md_path.display()
        ))
    })?;

    let parsed = skills::parse_skill_markdown(&raw_markdown, &bundled.id);
    let has_scripts = bundled.dir_path.join("scripts").is_dir();
    let has_references = bundled.dir_path.join("references").is_dir();
    let resources = collect_bundled_skill_resources(&bundled.dir_path);

    Ok(skills::SkillDocument {
        summary: skills::SkillSummary {
            id: bundled.id.clone(),
            name: parsed.name,
            description: parsed.description,
            enabled: true,
            source: format!("plugin:{}", bundled.plugin_id),
            dir_path: bundled.dir_path.to_string_lossy().into_owned(),
            has_scripts,
            has_references,
            resources,
        },
        instructions: parsed.body,
        raw_markdown,
    })
}

fn collect_bundled_skill_resources(skill_dir: &Path) -> Vec<String> {
    let mut resources = Vec::new();
    for subdir in BUNDLED_RESOURCE_SUBDIRS {
        let base = skill_dir.join(subdir);
        if let Ok(meta) = fs::symlink_metadata(&base) {
            if !meta.file_type().is_symlink() && meta.is_dir() {
                collect_bundled_resources_recursive(&base, subdir, 0, &mut resources);
            }
        }
        if resources.len() >= 256 {
            break;
        }
    }
    resources.sort();
    resources
}

fn collect_bundled_resources_recursive(
    dir: &Path,
    rel_prefix: &str,
    depth: usize,
    out: &mut Vec<String>,
) {
    if depth >= 4 || out.len() >= 256 {
        return;
    }
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        if out.len() >= 256 {
            break;
        }
        let Ok(ft) = entry.file_type() else {
            continue;
        };
        if ft.is_symlink() {
            continue;
        }
        let name = entry.file_name();
        let Some(name_str) = name.to_str() else {
            continue;
        };
        if name_str.starts_with('.') {
            continue;
        }
        let rel = format!("{rel_prefix}/{name_str}");
        if ft.is_file() {
            out.push(rel);
        } else if ft.is_dir() {
            collect_bundled_resources_recursive(&entry.path(), &rel, depth + 1, out);
        }
    }
}

fn require_bridge<'a>(ctx: &'a ToolExecContext, tool_id: &str) -> Result<&'a Arc<dyn AgentToolBridge>> {
    ctx.a2a_bridge.as_ref().ok_or_else(|| AgentError::Tool {
        tool_id: tool_id.to_string(),
        message: "agent tool bridge is not configured in execution context".into(),
    })
}

async fn run_blocking_fs<F>(
    tool_id: &str,
    args: Value,
    cfg: ToolsConfig,
    func: F,
) -> Result<Value>
where
    F: FnOnce(&Value, &ToolsConfig) -> Result<Value> + Send + 'static,
{
    tokio::task::spawn_blocking(move || func(&args, &cfg))
        .await
        .map_err(|e| AgentError::Tool {
            tool_id: tool_id.to_string(),
            message: format!("blocking file task panicked: {e}"),
        })?
}

fn json_api_request_is_mutating(args: &Value) -> bool {
    matches!(
        args.get("method")
            .and_then(|v| v.as_str())
            .unwrap_or("GET")
            .trim()
            .to_ascii_uppercase()
            .as_str(),
        "POST" | "PUT" | "PATCH" | "DELETE"
    )
}

fn check_sandbox_mode(
    canonical_id: &str,
    spec: &ToolSpec,
    args: &Value,
    mode: SandboxMode,
) -> Result<()> {
    match mode {
        SandboxMode::FullSandbox => Ok(()),
        SandboxMode::WorkspaceWrite => {
            if canonical_id == "http_post"
                || (canonical_id == "json_api_request" && json_api_request_is_mutating(args))
                || (!is_builtin_tool(canonical_id)
                    && spec.is_mutating
                    && spec.required_permissions.contains(&ToolPermission::WebWrite))
            {
                return Err(AgentError::PermissionDenied(format!(
                    "mutating web tool '{canonical_id}' is blocked under sandbox_mode 'workspace_write'"
                )));
            }
            Ok(())
        }
        SandboxMode::ReadOnly => {
            if canonical_id == "sandbox_exec" {
                return Ok(());
            }
            if canonical_id == "browser" {
                let action = args
                    .get("action")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .trim()
                    .to_ascii_lowercase();
                if matches!(action.as_str(), "download" | "save_snapshot" | "submit") {
                    return Err(AgentError::PermissionDenied(format!(
                        "browser action '{action}' is blocked under sandbox_mode 'read_only'"
                    )));
                }
                return Ok(());
            }
            if canonical_id == "doctor" {
                if args.get("fix").and_then(|v| v.as_bool()).unwrap_or(false) {
                    return Err(AgentError::PermissionDenied(
                        "doctor fix=true is blocked under sandbox_mode 'read_only'".into(),
                    ));
                }
                return Ok(());
            }
            if canonical_id == "json_api_request" {
                if json_api_request_is_mutating(args) {
                    return Err(AgentError::PermissionDenied(
                        "mutating json_api_request is blocked under sandbox_mode 'read_only'".into(),
                    ));
                }
                return Ok(());
            }
            let has_mutating_perm = spec.required_permissions.iter().any(|p| {
                matches!(
                    p,
                    ToolPermission::FileWrite
                        | ToolPermission::FileDelete
                        | ToolPermission::WebWrite
                )
            });
            if spec.is_mutating || has_mutating_perm {
                return Err(AgentError::PermissionDenied(format!(
                    "mutating tool '{canonical_id}' is blocked under sandbox_mode 'read_only'"
                )));
            }
            Ok(())
        }
    }
}

fn infer_artifact_extension(content: &str) -> &'static str {
    let trimmed = content.trim_start();
    let lower_prefix: String = trimmed.chars().take(32).collect::<String>().to_ascii_lowercase();
    if lower_prefix.starts_with("<svg") {
        "svg"
    } else if lower_prefix.starts_with("<!doctype html") || lower_prefix.starts_with("<html") {
        "html"
    } else if (trimmed.starts_with('{') || trimmed.starts_with('['))
        && serde_json::from_str::<Value>(trimmed).is_ok()
    {
        "json"
    } else {
        "md"
    }
}

fn sanitize_artifact_stem(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len().min(64));
    let mut prev_dash = false;
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            out.push(ch);
            prev_dash = false;
        } else if (ch == '-' || ch == '.' || ch.is_whitespace()) && !out.is_empty() && !prev_dash {
            out.push('-');
            prev_dash = true;
        } else if !out.is_empty() && !prev_dash {
            out.push('-');
            prev_dash = true;
        }
        if out.len() >= 64 {
            break;
        }
    }
    let trimmed = out.trim_matches(|c| c == '-' || c == '_');
    if trimmed.is_empty() {
        "artifact".to_string()
    } else {
        trimmed.to_string()
    }
}

fn sanitize_artifact_filename(
    raw_name: Option<&str>,
    title: Option<&str>,
    content: &str,
) -> Result<String> {
    if let Some(explicit) = raw_name.map(str::trim).filter(|s| !s.is_empty()) {
        let stripped = explicit
            .strip_prefix("artifacts/")
            .or_else(|| explicit.strip_prefix("artifacts\\"))
            .unwrap_or(explicit);
        if stripped.is_empty()
            || stripped.contains('\0')
            || stripped.contains('/')
            || stripped.contains('\\')
            || stripped.contains("..")
            || stripped.contains(':')
        {
            return Err(AgentError::InvalidInput(
                "artifact filename must be a simple filename under artifacts/ without traversal or path separators"
                    .into(),
            ));
        }

        if let Some((raw_stem, raw_ext)) = stripped.rsplit_once('.') {
            if !raw_stem.trim_matches('.').is_empty() && !raw_ext.is_empty() {
                let ext = raw_ext.to_ascii_lowercase();
                if !matches!(ext.as_str(), "md" | "html" | "json" | "svg" | "txt") {
                    return Err(AgentError::InvalidInput(format!(
                        "unsupported artifact extension '.{ext}'; allowed: .md, .html, .json, .svg, .txt"
                    )));
                }
                let stem = sanitize_artifact_stem(raw_stem);
                return Ok(format!("{stem}.{ext}"));
            }
        }

        let stem = sanitize_artifact_stem(stripped.trim_start_matches('.'));
        let ext = infer_artifact_extension(content);
        return Ok(format!("{stem}.{ext}"));
    }

    let stem = title
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(sanitize_artifact_stem)
        .unwrap_or_else(|| "artifact".to_string());
    let ext = infer_artifact_extension(content);
    Ok(format!("{stem}.{ext}"))
}

fn write_artifact_blocking(args: &Value, cfg: &ToolsConfig) -> Result<(String, String, usize)> {
    use std::io::Write;

    let content = args
        .get("content")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AgentError::InvalidInput("missing required string 'content'".into()))?;
    if cfg.max_file_bytes > 0 && content.len() > cfg.max_file_bytes {
        return Err(AgentError::LimitExceeded(format!(
            "artifact content ({} bytes) exceeds max_file_bytes ({})",
            content.len(),
            cfg.max_file_bytes
        )));
    }

    let raw_name = args
        .get("filename")
        .or_else(|| args.get("name"))
        .and_then(|v| v.as_str());
    let title_opt = args.get("title").and_then(|v| v.as_str()).map(str::trim);
    let safe_name = sanitize_artifact_filename(raw_name, title_opt, content)?;
    let title = title_opt
        .filter(|s| !s.is_empty())
        .unwrap_or(&safe_name)
        .to_string();

    let rel_path = format!("artifacts/{safe_name}");
    let (_root, target) = files::resolve_workspace_path_checked(&rel_path, cfg)?;
    let artifacts_dir = target.parent().ok_or_else(|| AgentError::Tool {
        tool_id: "write_artifact".into(),
        message: "resolved artifact path missing parent directory".into(),
    })?;

    if let Ok(meta) = fs::symlink_metadata(artifacts_dir) {
        if meta.file_type().is_symlink() {
            return Err(AgentError::PermissionDenied(
                "artifacts directory cannot be a symlink".into(),
            ));
        }
        if !meta.is_dir() {
            return Err(AgentError::InvalidInput(
                "artifacts path exists and is not a directory".into(),
            ));
        }
    } else {
        fs::create_dir_all(artifacts_dir).map_err(|e| AgentError::Tool {
            tool_id: "write_artifact".into(),
            message: format!("failed to create artifacts directory: {e}"),
        })?;
    }

    if let Ok(meta) = fs::symlink_metadata(&target) {
        if meta.file_type().is_symlink() {
            return Err(AgentError::PermissionDenied(
                "artifact target cannot be a symlink".into(),
            ));
        }
        if meta.is_dir() {
            return Err(AgentError::InvalidInput(
                "cannot overwrite directory as artifact".into(),
            ));
        }
    }

    let tmp_path = artifacts_dir.join(format!(".tmp.{}.{safe_name}", new_id()));
    let write_res: Result<()> = (|| {
        let mut f = fs::File::create(&tmp_path).map_err(|e| AgentError::Tool {
            tool_id: "write_artifact".into(),
            message: format!("failed to create temp artifact file: {e}"),
        })?;
        f.write_all(content.as_bytes()).map_err(|e| AgentError::Tool {
            tool_id: "write_artifact".into(),
            message: format!("failed to write temp artifact file: {e}"),
        })?;
        f.sync_all().map_err(|e| AgentError::Tool {
            tool_id: "write_artifact".into(),
            message: format!("failed to sync temp artifact file: {e}"),
        })?;
        fs::rename(&tmp_path, &target).map_err(|e| AgentError::Tool {
            tool_id: "write_artifact".into(),
            message: format!("failed to finalize artifact file: {e}"),
        })?;
        Ok(())
    })();

    if let Err(err) = write_res {
        let _ = fs::remove_file(&tmp_path);
        return Err(err);
    }

    Ok((rel_path, title, content.len()))
}

fn truncate_json(val: &Value, max_bytes: usize) -> String {
    let s = serde_json::to_string(val).unwrap_or_else(|_| "{}".into());
    if s.len() <= max_bytes {
        s
    } else {
        let mut end = max_bytes;
        while end > 0 && !s.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}...", &s[..end])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ps5_agent_core::{
        AgentEnvelope, AgentModelConfig, AgentRole, AuditLogEntry, ChatMessage, Conversation,
        DeliveryStatus, MemoryItem, MemoryPermissions, MemoryScope, SessionRecord,
        TaskCountsSummary, TaskEvent, TaskRecord,
    };
    use std::sync::atomic::AtomicUsize;
    use std::sync::Mutex;

    #[derive(Default)]
    struct TestMemDb {
        agents: Mutex<HashMap<String, AgentDefinition>>,
        tools: Mutex<HashMap<String, ToolSpec>>,
        approvals: Mutex<HashMap<String, ApprovalRecord>>,
        executions: Mutex<Vec<ToolExecutionRecord>>,
        list_specs_calls: AtomicUsize,
    }

    impl StorageBackend for TestMemDb {
        fn driver_name(&self) -> &'static str {
            "test_mem"
        }
        fn run_migrations(&self) -> Result<()> {
            Ok(())
        }
        fn integrity_check(&self) -> Result<String> {
            Ok("ok".into())
        }
        fn checkpoint(&self) -> Result<()> {
            Ok(())
        }
        fn upsert_agent(&self, agent: &AgentDefinition) -> Result<()> {
            self.agents
                .lock()
                .unwrap()
                .insert(agent.id.clone(), agent.clone());
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
        fn create_conversation(&self, _conv: &Conversation) -> Result<()> {
            Ok(())
        }
        fn get_conversation(&self, _conv_id: &str) -> Result<Option<Conversation>> {
            Ok(None)
        }
        fn list_conversations(&self, _include_archived: bool) -> Result<Vec<Conversation>> {
            Ok(vec![])
        }
        fn delete_conversation(&self, _conv_id: &str) -> Result<bool> {
            Ok(false)
        }
        fn append_chat_message(&self, _msg: &ChatMessage) -> Result<()> {
            Ok(())
        }
        fn list_chat_messages(
            &self,
            _conv_id: &str,
            _include_archived: bool,
            _limit: usize,
        ) -> Result<Vec<ChatMessage>> {
            Ok(vec![])
        }
        fn search_chat_messages(&self, _query: &str, _limit: usize) -> Result<Vec<ChatMessage>> {
            Ok(vec![])
        }
        fn archive_old_chat_messages(&self, _conv_id: &str, _keep_recent: usize) -> Result<usize> {
            Ok(0)
        }
        fn upsert_task(&self, _task: &TaskRecord) -> Result<()> {
            Ok(())
        }
        fn get_task(&self, _task_id: &str) -> Result<Option<TaskRecord>> {
            Ok(None)
        }
        fn list_tasks(&self, _limit: usize) -> Result<Vec<TaskRecord>> {
            Ok(vec![])
        }
        fn task_counts_summary(&self) -> Result<TaskCountsSummary> {
            Ok(TaskCountsSummary::default())
        }
        fn append_task_event(&self, _event: &TaskEvent) -> Result<()> {
            Ok(())
        }
        fn list_task_events(&self, _task_id: &str) -> Result<Vec<TaskEvent>> {
            Ok(vec![])
        }
        fn append_agent_message(&self, _msg: &AgentEnvelope) -> Result<()> {
            Ok(())
        }
        fn update_agent_message_status(&self, _msg_id: &str, _status: DeliveryStatus) -> Result<()> {
            Ok(())
        }
        fn list_agent_messages(
            &self,
            _task_id: Option<&str>,
            _agent_id: Option<&str>,
            _limit: usize,
        ) -> Result<Vec<AgentEnvelope>> {
            Ok(vec![])
        }
        fn count_task_agent_messages(&self, _task_id: &str) -> Result<u32> {
            Ok(0)
        }
        fn upsert_memory(&self, _item: &MemoryItem) -> Result<()> {
            Ok(())
        }
        fn query_memories(
            &self,
            _scope: Option<MemoryScope>,
            _namespace: Option<&str>,
            _owner_agent_id: Option<&str>,
            _include_archived: bool,
            _limit: usize,
        ) -> Result<Vec<MemoryItem>> {
            Ok(vec![])
        }
        fn delete_memory(&self, _memory_id: &str) -> Result<bool> {
            Ok(false)
        }
        fn delete_agent_memories(&self, _agent_id: &str) -> Result<usize> {
            Ok(0)
        }
        fn memory_stats_for_agent(&self, _agent_id: &str) -> Result<(u64, u64)> {
            Ok((0, 0))
        }
        fn upsert_tool_spec(&self, spec: &ToolSpec) -> Result<()> {
            self.tools
                .lock()
                .unwrap()
                .insert(spec.id.clone(), spec.clone());
            Ok(())
        }
        fn list_tool_specs(&self) -> Result<Vec<ToolSpec>> {
            self.list_specs_calls.fetch_add(1, Ordering::Relaxed);
            Ok(self.tools.lock().unwrap().values().cloned().collect())
        }
        fn delete_tool_spec(&self, tool_id: &str) -> Result<bool> {
            Ok(self.tools.lock().unwrap().remove(tool_id).is_some())
        }
        fn record_tool_execution(&self, rec: &ToolExecutionRecord) -> Result<()> {
            self.executions.lock().unwrap().push(rec.clone());
            Ok(())
        }
        fn list_tool_executions(
            &self,
            _agent_id: Option<&str>,
            _limit: usize,
        ) -> Result<Vec<ToolExecutionRecord>> {
            Ok(self.executions.lock().unwrap().clone())
        }
        fn get_setting(&self, _key: &str) -> Result<Option<String>> {
            Ok(None)
        }
        fn set_setting(&self, _key: &str, _value: &str) -> Result<()> {
            Ok(())
        }
        fn list_settings(&self) -> Result<Vec<(String, String)>> {
            Ok(vec![])
        }
        fn create_session(&self, _sess: &SessionRecord) -> Result<()> {
            Ok(())
        }
        fn get_session_by_hash(&self, _token_sha256: &str) -> Result<Option<SessionRecord>> {
            Ok(None)
        }
        fn revoke_session(&self, _session_id: &str) -> Result<()> {
            Ok(())
        }
        fn purge_expired_sessions(&self, _now_iso: &str) -> Result<usize> {
            Ok(0)
        }
        fn upsert_approval(&self, approval: &ApprovalRecord) -> Result<()> {
            self.approvals
                .lock()
                .unwrap()
                .insert(approval.id.clone(), approval.clone());
            Ok(())
        }
        fn get_approval(&self, approval_id: &str) -> Result<Option<ApprovalRecord>> {
            Ok(self.approvals.lock().unwrap().get(approval_id).cloned())
        }
        fn list_approvals(&self, _pending_only: bool, _limit: usize) -> Result<Vec<ApprovalRecord>> {
            Ok(self.approvals.lock().unwrap().values().cloned().collect())
        }
        fn append_audit_log(&self, _entry: &AuditLogEntry) -> Result<()> {
            Ok(())
        }
        fn list_audit_logs(&self, _limit: usize) -> Result<Vec<AuditLogEntry>> {
            Ok(vec![])
        }
    }

    fn make_agent(id: &str, enabled_tools: Vec<String>) -> AgentDefinition {
        AgentDefinition {
            id: id.into(),
            name: id.into(),
            description: "test".into(),
            role: AgentRole::Programmer,
            role_label: "Programmer".into(),
            system_prompt: "sys".into(),
            model_config: AgentModelConfig::default(),
            enabled_tools,
            denied_tools: Vec::new(),
            sandbox_mode: None,
            tool_profile: None,
            memory_permissions: MemoryPermissions::default(),
            max_task_duration_secs: 0,
            max_reasoning_iterations: 0,
            priority: 1,
            can_communicate: true,
            enabled: true,
            is_builtin: false,
            is_protected: false,
            is_temporary: false,
            created_at: "2026-01-01T00:00:00Z".into(),
            updated_at: "2026-01-01T00:00:00Z".into(),
        }
    }

    fn make_cfg(workspace: &Path) -> ToolsConfig {
        ToolsConfig {
            allowed_paths: vec![workspace.to_string_lossy().into_owned()],
            allowed_domains: vec!["*".into()],
            block_private_lan_web_requests: true,
            default_timeout_secs: 10,
            max_file_bytes: 1024 * 1024,
            max_web_bytes: 64 * 1024,
            ..Default::default()
        }
    }

    #[test]
    fn tool_pattern_matches_exact_wildcard_and_prefix() {
        let patterns = vec![
            "calculator".to_string(),
            "mcp:*".to_string(),
            "plugin:my-plug:*".to_string(),
            "skill:*".to_string(),
            "file_*".to_string(),
        ];
        assert!(tool_pattern_matches(&patterns, "calculator"));
        assert!(tool_pattern_matches(&patterns, "mcp:gh:search"));
        assert!(tool_pattern_matches(&patterns, "plugin:my-plug:greet"));
        assert!(tool_pattern_matches(&patterns, "skill:demo"));
        assert!(tool_pattern_matches(&patterns, "file_read"));
        assert!(tool_pattern_matches(&patterns, "file_write"));
        assert!(!tool_pattern_matches(&patterns, "plugin:other:greet"));
        assert!(!tool_pattern_matches(&patterns, "delete_file"));
        assert!(tool_pattern_matches(&["*".to_string()], "anything"));
    }

    #[tokio::test]
    async fn registry_caches_specs_loads_skills_plugins_and_file_aliases() {
        let data_dir = tempfile::tempdir().unwrap();
        let workspace_dir = data_dir.path().join("workspace");
        fs::create_dir_all(&workspace_dir).unwrap();

        let db = Arc::new(TestMemDb::default());
        let agent = make_agent(
            "coder",
            vec![
                "file_*".into(),
                "load_skill".into(),
                "plugin:*".into(),
                "mcp:*".into(),
            ],
        );
        db.upsert_agent(&agent).unwrap();

        let registry =
            ToolRegistry::with_data_dir(db.clone(), make_cfg(&workspace_dir), data_dir.path())
                .unwrap();

        // Only 1 list_tool_specs() call during seed_tools(); subsequent reads hit memory cache.
        assert_eq!(db.list_specs_calls.load(Ordering::Relaxed), 1);
        let initial_specs = registry.list_specs().unwrap();
        assert_eq!(initial_specs.len(), 41);
        assert!(registry.get_spec("load_skill").unwrap().is_some());
        assert!(registry.get_spec("read_skill").unwrap().is_some());
        assert_eq!(db.list_specs_calls.load(Ordering::Relaxed), 1);

        // Import a user skill + a plugin with a bundled skill and a custom template tool.
        registry
            .skills()
            .import_skill(
                Some("rust-style"),
                "---\nname: Rust Style\ndescription: Clean Rust\n---\nUse Result.",
            )
            .unwrap();

        let plug_skill_dir = data_dir
            .path()
            .join("plugins")
            .join("helper-plug")
            .join("skills")
            .join("bundled-tip");
        fs::create_dir_all(&plug_skill_dir).unwrap();
        fs::write(
            plug_skill_dir.join("SKILL.md"),
            "---\nname: Bundled Tip\ndescription: From plugin\n---\nAlways test.",
        )
        .unwrap();
        registry
            .plugins()
            .import_plugin_json(
                r#"{
                    "name": "helper-plug",
                    "description": "Test plugin",
                    "tools": [
                        {
                            "name": "greet",
                            "description": "Greet user",
                            "kind": "template",
                            "body_template": "Hello {{who}}!",
                            "is_mutating": false
                        }
                    ]
                }"#,
            )
            .unwrap();

        let defs = registry.tool_definitions_for_agent(&agent).unwrap();
        assert!(defs.iter().any(|d| d.name == "read_file"));
        assert!(defs.iter().any(|d| d.name == "write_file"));
        assert!(defs.iter().any(|d| d.name == "load_skill"));
        assert!(defs.iter().any(|d| d.name == "plugin:helper-plug:greet"));

        let ctx = ToolExecContext {
            agent_id: "coder".into(),
            task_id: None,
            conversation_id: None,
            approval_granted: false,
            a2a_bridge: None,
            ..Default::default()
        };

        // Invoke load_skill and read_skill alias (including plugin bundled skill).
        let loaded = registry
            .invoke("load_skill", json!({"skill": "rust-style"}), &ctx)
            .await
            .unwrap();
        assert_eq!(loaded["instructions"], "Use Result.");

        let bundled = registry
            .invoke("read_skill", json!({"skill": "Bundled Tip"}), &ctx)
            .await
            .unwrap();
        assert_eq!(bundled["source"], "plugin:helper-plug");
        assert_eq!(bundled["instructions"], "Always test.");

        // Invoke plugin custom tool.
        let plug_out = registry
            .invoke(
                "plugin:helper-plug:greet",
                json!({"who": "PS5"}),
                &ctx,
            )
            .await
            .unwrap();
        assert_eq!(plug_out["output"], "Hello PS5!");

        // Legacy file_write / file_read aliases work and write_file returns size_bytes + created.
        let write_out = registry
            .invoke(
                "file_write",
                json!({"path": "hi.txt", "content": "abcde"}),
                &ctx,
            )
            .await
            .unwrap();
        assert_eq!(write_out["bytes_written"], 5);
        assert_eq!(write_out["size_bytes"], 5);
        assert_eq!(write_out["created"], true);

        let read_out = registry
            .invoke("file_read", json!({"path": "hi.txt"}), &ctx)
            .await
            .unwrap();
        assert_eq!(read_out["content"], "abcde");

        // Toggle tool updates both cache and storage without calling list_tool_specs().
        let toggled = registry.toggle_tool("load_skill", Some(false)).unwrap();
        assert!(!toggled.enabled);
        let err = registry
            .invoke("load_skill", json!({"skill": "rust-style"}), &ctx)
            .await
            .unwrap_err();
        assert!(matches!(err, AgentError::PermissionDenied(_)));
        assert_eq!(db.list_specs_calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn custom_tools_lifecycle_policy_wildcards_and_dirs() {
        let data_dir = tempfile::tempdir().unwrap();
        let workspace_dir = data_dir.path().join("workspace");
        fs::create_dir_all(&workspace_dir).unwrap();

        let db = Arc::new(TestMemDb::default());
        let agent = make_agent("commander", vec!["custom:*".into()]);
        db.upsert_agent(&agent).unwrap();

        let registry =
            ToolRegistry::with_data_dir(db.clone(), make_cfg(&workspace_dir), data_dir.path())
                .unwrap();

        assert!(data_dir.path().join("skills").is_dir());
        assert!(data_dir.path().join("plugins").is_dir());
        assert!(data_dir.path().join("tools").is_dir());

        // Colon-prefixed IDs remain rejected to prevent namespace spoofing.
        assert!(registry
            .register_or_import_custom_tools(&json!({
                "id": "custom:prefixed_helper",
                "kind": "template",
                "template": "blocked"
            }))
            .is_err());

        // Import 25 custom tools (zero 20-tool cap) plus one OpenAI-format custom tool in a non-"custom" category.
        let mut batch: Vec<Value> = (0..25)
            .map(|i| {
                json!({
                    "id": format!("custom_tool_{i:02}"),
                    "description": format!("Custom #{i}"),
                    "kind": "template",
                    "template": format!("out-{i}: {{{{val}}}}")
                })
            })
            .collect();
        batch.push(json!({
            "type": "function",
            "function": {
                "name": "automation_helper",
                "description": "Custom OpenAI-format automation tool",
                "parameters": {
                    "type": "object",
                    "properties": { "val": { "type": "string" } }
                }
            },
            "category": "automation",
            "template": "prefixed={{val}}"
        }));

        let imported = registry
            .register_or_import_custom_tools(&Value::Array(batch))
            .unwrap();
        assert_eq!(imported.len(), 26);

        let defs = registry.tool_definitions_for_agent(&agent).unwrap();
        assert_eq!(defs.len(), 26);
        assert!(defs.iter().any(|d| d.name == "custom_tool_24"));
        assert!(defs.iter().any(|d| d.name == "automation_helper"));

        // Invoke custom tool with `custom:*`.
        let ctx = ToolExecContext {
            agent_id: "commander".into(),
            task_id: None,
            conversation_id: None,
            approval_granted: false,
            a2a_bridge: None,
            ..Default::default()
        };
        let out = registry
            .invoke("custom_tool_24", json!({"val": "ok"}), &ctx)
            .await
            .unwrap();
        assert_eq!(out["rendered"], "out-24: ok");

        // Update policy (disable, require approval, timeout 45s) and verify persistence across sync_with_storage.
        let updated = registry
            .update_tool_policy("custom_tool_24", Some(false), Some(true), Some(45))
            .unwrap();
        assert!(!updated.enabled);
        assert!(updated.requires_approval);
        assert_eq!(updated.timeout_secs, 45);

        registry.sync_with_storage().unwrap();
        let synced = registry.get_spec("custom_tool_24").unwrap().unwrap();
        assert!(!synced.enabled);
        assert!(synced.requires_approval);
        assert_eq!(synced.timeout_secs, 45);

        // Delete custom tool & refuse deleting built-in tool.
        assert!(registry.delete_custom_tool("custom_tool_24").unwrap());
        assert!(registry.get_spec("custom_tool_24").unwrap().is_none());
        assert!(!data_dir.path().join("tools").join("custom_tool_24.json").exists());

        let builtin_del = registry.delete_custom_tool("calculator");
        assert!(matches!(builtin_del, Err(AgentError::PermissionDenied(_))));
    }

    struct MinimalBridge;

    #[async_trait]
    impl AgentToolBridge for MinimalBridge {
        async fn send_agent_message(
            &self,
            _from_agent_id: &str,
            _to_agent_id: &str,
            _task_id: Option<&str>,
            _content: &str,
        ) -> Result<Value> {
            Ok(json!({"sent": true}))
        }

        async fn assign_agent_task(
            &self,
            _from_agent_id: &str,
            _target_agent_id: &str,
            _user_request: &str,
            _parent_task_id: Option<&str>,
            _conversation_id: Option<&str>,
        ) -> Result<Value> {
            Ok(json!({"assigned": true}))
        }

        async fn query_agent_status(
            &self,
            _requester_agent_id: &str,
            _target_agent_id: Option<&str>,
            _task_id: Option<&str>,
        ) -> Result<Value> {
            Ok(json!({"status": "ok"}))
        }

        async fn read_agent_memory(
            &self,
            _requester_agent_id: &str,
            _target_agent_id: &str,
            _query: Option<&str>,
            _limit: usize,
        ) -> Result<Value> {
            Ok(json!({"items": []}))
        }

        async fn request_temporary_specialist(
            &self,
            _requester_agent_id: &str,
            _name: &str,
            _role_label: &str,
            _system_prompt: &str,
            _enabled_tools: Vec<String>,
            _task_request: Option<&str>,
            _parent_task_id: Option<&str>,
        ) -> Result<Value> {
            Ok(json!({"created": true}))
        }
    }

    struct RecordingBridge {
        events: std::sync::Mutex<Vec<String>>,
    }

    impl RecordingBridge {
        fn new() -> Self {
            Self {
                events: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn push(&self, event: String) {
            self.events.lock().unwrap().push(event);
        }

        fn snapshot(&self) -> Vec<String> {
            self.events.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl AgentToolBridge for RecordingBridge {
        async fn send_agent_message(
            &self,
            _from: &str,
            to: &str,
            _task_id: Option<&str>,
            _content: &str,
        ) -> Result<Value> {
            Ok(json!({"to": to}))
        }

        async fn assign_agent_task(
            &self,
            _from: &str,
            target: &str,
            _req: &str,
            _parent: Option<&str>,
            _conv: Option<&str>,
        ) -> Result<Value> {
            Ok(json!({"target": target}))
        }

        async fn query_agent_status(
            &self,
            _req: &str,
            _target: Option<&str>,
            _task_id: Option<&str>,
        ) -> Result<Value> {
            Ok(json!({"ok": true}))
        }

        async fn read_agent_memory(
            &self,
            _req: &str,
            _target: &str,
            _query: Option<&str>,
            _limit: usize,
        ) -> Result<Value> {
            Ok(json!({"items": []}))
        }

        async fn request_temporary_specialist(
            &self,
            _req: &str,
            name: &str,
            _role: &str,
            _prompt: &str,
            _tools: Vec<String>,
            _task: Option<&str>,
            _parent: Option<&str>,
        ) -> Result<Value> {
            Ok(json!({"name": name}))
        }

        async fn memory_read(
            &self,
            agent_id: &str,
            _scope: Option<&str>,
            _namespace: Option<&str>,
            query: Option<&str>,
            limit: usize,
        ) -> Result<Value> {
            self.push(format!("memory_read:{agent_id}:{limit}"));
            Ok(json!({"agent_id": agent_id, "query": query, "limit": limit}))
        }

        async fn memory_write(
            &self,
            agent_id: &str,
            scope: &str,
            namespace: &str,
            key: &str,
            _content: &str,
            importance: u8,
            _conversation_id: Option<&str>,
            _task_id: Option<&str>,
        ) -> Result<Value> {
            self.push(format!("memory_write:{key}:{importance}"));
            Ok(json!({"agent_id": agent_id, "scope": scope, "namespace": namespace, "key": key}))
        }

        async fn memory_search(
            &self,
            _agent_id: &str,
            query: &str,
            max_results: usize,
        ) -> Result<Value> {
            self.push(format!("memory_search:{query}"));
            Ok(json!({"query": query, "max_results": max_results, "results": []}))
        }

        async fn memory_get(
            &self,
            _agent_id: &str,
            path: &str,
            from_line: Option<usize>,
            lines: Option<usize>,
        ) -> Result<Value> {
            self.push(format!("memory_get:{path}"));
            Ok(json!({"path": path, "from_line": from_line, "lines": lines}))
        }

        async fn system_info(&self) -> Result<Value> {
            self.push("system_info".into());
            Ok(json!({"source": "bridge"}))
        }

        async fn ps5_info(&self) -> Result<Value> {
            self.push("ps5_info".into());
            Ok(json!({"platform": "test-ps5"}))
        }

        async fn sessions_list(&self, limit: usize, active_only: bool) -> Result<Value> {
            self.push(format!("sessions_list:{limit}:{active_only}"));
            Ok(json!({"sessions": []}))
        }

        async fn sessions_spawn(
            &self,
            _sender: &str,
            target_agent_id: Option<&str>,
            conversation_id: Option<&str>,
            prompt: &str,
        ) -> Result<Value> {
            self.push(format!("sessions_spawn:{prompt}"));
            Ok(json!({"target": target_agent_id, "conversation_id": conversation_id}))
        }

        async fn sessions_send(
            &self,
            _sender: &str,
            conversation_id: &str,
            message: &str,
        ) -> Result<Value> {
            self.push(format!("sessions_send:{conversation_id}"));
            Ok(json!({"conversation_id": conversation_id, "message": message}))
        }

        async fn session_status(
            &self,
            conversation_id: Option<&str>,
            task_id: Option<&str>,
        ) -> Result<Value> {
            self.push("session_status".into());
            Ok(json!({"conversation_id": conversation_id, "task_id": task_id}))
        }

        async fn process_action(&self, action: &str, target_id: Option<&str>) -> Result<Value> {
            self.push(format!("process:{action}"));
            Ok(json!({"action": action, "target_id": target_id}))
        }

        async fn run_doctor(&self, fix: bool) -> Result<Value> {
            self.push(format!("doctor:{fix}"));
            Ok(json!({"fix": fix, "ok": true}))
        }

        async fn notify_artifact_created(
            &self,
            conversation_id: Option<&str>,
            task_id: Option<&str>,
            rel_path: &str,
            title: &str,
        ) -> Result<()> {
            self.push(format!(
                "artifact:{}:{}:{}:{}",
                conversation_id.unwrap_or("-"),
                task_id.unwrap_or("-"),
                rel_path,
                title
            ));
            Ok(())
        }
    }

    #[tokio::test]
    async fn specs_include_all_41_builtins_and_preserve_zero_timeout_unlimited() {
        let data_dir = tempfile::tempdir().unwrap();
        let workspace_dir = data_dir.path().join("workspace");
        fs::create_dir_all(&workspace_dir).unwrap();

        let mut cfg = make_cfg(&workspace_dir);
        cfg.default_timeout_secs = 0;

        let db = Arc::new(TestMemDb::default());
        let registry = ToolRegistry::with_data_dir(db, cfg, data_dir.path()).unwrap();
        let specs = registry.list_specs().unwrap();
        assert_eq!(specs.len(), 41);
        assert!(specs.iter().all(|s| s.timeout_secs == 0));

        // Explicit policy update with Some(0) preserves 0 (unlimited) instead of clamping to 1.
        let updated = registry
            .update_tool_policy("calculator", Some(true), None, Some(0))
            .unwrap();
        assert_eq!(updated.timeout_secs, 0);

        // Smoke-check ToolRegistry::browser_manager() accessor and unlimited-timeout invoke.
        let sessions = registry.browser_manager().list_sessions();
        assert!(sessions.get("sessions").is_some());

        let ctx = ToolExecContext::default();
        let out = registry
            .invoke("calculator", json!({"expression": "6 * 7"}), &ctx)
            .await
            .unwrap();
        assert_eq!(out["result"], 42.0);
    }

    #[tokio::test]
    async fn policy_supports_aliases_groups_denied_tools_and_schedule_restriction() {
        let data_dir = tempfile::tempdir().unwrap();
        let workspace_dir = data_dir.path().join("workspace");
        fs::create_dir_all(&workspace_dir).unwrap();

        let db = Arc::new(TestMemDb::default());
        let mut coder = make_agent("coder", vec!["group:fs".into(), "canvas".into(), "custom:*".into()]);
        coder.denied_tools = vec!["delete_file".into()];
        db.upsert_agent(&coder).unwrap();

        let mut disabled_agent = make_agent("paused", vec!["*".into()]);
        disabled_agent.enabled = false;
        db.upsert_agent(&disabled_agent).unwrap();

        let commander = make_agent("commander", vec!["*".into()]);
        db.upsert_agent(&commander).unwrap();

        let registry =
            ToolRegistry::with_data_dir(db.clone(), make_cfg(&workspace_dir), data_dir.path())
                .unwrap();

        // Register a schedule_* custom tool to verify non-commander/executor restriction.
        registry
            .register_or_import_custom_tools(&json!([
                {
                    "id": "schedule_create",
                    "description": "Schedule job",
                    "kind": "template",
                    "template": "scheduled={{name}}"
                }
            ]))
            .unwrap();

        let defs = registry.tool_definitions_for_agent(&coder).unwrap();
        assert!(defs.iter().any(|d| d.name == "read_file"));
        assert!(defs.iter().any(|d| d.name == "edit_file"));
        assert!(defs.iter().any(|d| d.name == "write_artifact"));
        assert!(!defs.iter().any(|d| d.name == "delete_file"));
        assert!(!defs.iter().any(|d| d.name == "schedule_create"));
        assert!(registry.tool_definitions_for_agent(&disabled_agent).unwrap().is_empty());

        let coder_ctx = ToolExecContext {
            agent_id: "coder".into(),
            ..Default::default()
        };
        let denied_del = registry
            .invoke("delete_file", json!({"path": "x.txt"}), &coder_ctx)
            .await
            .unwrap_err();
        assert!(matches!(denied_del, AgentError::PermissionDenied(_)));

        let denied_sched = registry
            .invoke("schedule_create", json!({"name": "nightly"}), &coder_ctx)
            .await
            .unwrap_err();
        assert!(matches!(denied_sched, AgentError::PermissionDenied(_)));

        // Disabled agent cannot invoke tools even with "*" enabled.
        let paused_ctx = ToolExecContext {
            agent_id: "paused".into(),
            ..Default::default()
        };
        let denied_paused = registry
            .invoke("calculator", json!({"expression": "1 + 1"}), &paused_ctx)
            .await
            .unwrap_err();
        assert!(matches!(denied_paused, AgentError::PermissionDenied(_)));

        // Commander CAN invoke schedule_create.
        let cmd_ctx = ToolExecContext {
            agent_id: "commander".into(),
            ..Default::default()
        };
        let sched_ok = registry
            .invoke("schedule_create", json!({"name": "nightly"}), &cmd_ctx)
            .await
            .unwrap();
        assert_eq!(sched_ok["rendered"], "scheduled=nightly");
    }

    #[tokio::test]
    async fn sandbox_mode_readonly_and_workspace_write_enforcement() {
        let data_dir = tempfile::tempdir().unwrap();
        let workspace_dir = data_dir.path().join("workspace");
        fs::create_dir_all(&workspace_dir).unwrap();
        fs::write(workspace_dir.join("hello.txt"), "world").unwrap();

        let db = Arc::new(TestMemDb::default());
        db.upsert_agent(&make_agent("coder", vec!["*".into()])).unwrap();
        let registry =
            ToolRegistry::with_data_dir(db, make_cfg(&workspace_dir), data_dir.path()).unwrap();

        let ro_ctx = ToolExecContext {
            agent_id: "coder".into(),
            sandbox_mode: SandboxMode::ReadOnly,
            ..Default::default()
        };

        // Read-only tools pass in ReadOnly mode.
        let read_out = registry
            .invoke("read_file", json!({"path": "hello.txt"}), &ro_ctx)
            .await
            .unwrap();
        assert_eq!(read_out["content"], "world");

        // Mutating tools fail cleanly in ReadOnly mode.
        for (tool, args) in [
            ("write_file", json!({"path": "out.txt", "content": "x"})),
            ("delete_file", json!({"path": "hello.txt"})),
            ("write_artifact", json!({"filename": "a.md", "content": "# A"})),
            ("http_post", json!({"url": "https://example.com", "body": {}})),
            ("json_api_request", json!({"url": "https://example.com", "method": "POST"})),
            ("doctor", json!({"fix": true})),
            ("browser", json!({"action": "download", "url": "https://example.com"})),
        ] {
            let err = registry.invoke(tool, args, &ro_ctx).await.unwrap_err();
            assert!(
                matches!(err, AgentError::PermissionDenied(_)),
                "expected PermissionDenied for {tool} under ReadOnly, got {err:?}"
            );
        }

        // WorkspaceWrite allows local workspace writes while blocking mutating non-workspace web calls.
        let ww_ctx = ToolExecContext {
            agent_id: "coder".into(),
            sandbox_mode: SandboxMode::WorkspaceWrite,
            ..Default::default()
        };
        let art = registry
            .invoke(
                "write_artifact",
                json!({"filename": "report.md", "content": "# OK"}),
                &ww_ctx,
            )
            .await
            .unwrap();
        assert_eq!(art["ok"], true);

        let post_err = registry
            .invoke(
                "http_post",
                json!({"url": "https://example.com", "body": {}}),
                &ww_ctx,
            )
            .await
            .unwrap_err();
        assert!(matches!(post_err, AgentError::PermissionDenied(_)));

        let api_err = registry
            .invoke(
                "json_api_request",
                json!({"url": "https://example.com", "method": "DELETE"}),
                &ww_ctx,
            )
            .await
            .unwrap_err();
        assert!(matches!(api_err, AgentError::PermissionDenied(_)));
    }

    #[tokio::test]
    async fn approval_policy_full_auto_on_request_always_ask_and_never() {
        let data_dir = tempfile::tempdir().unwrap();
        let workspace_dir = data_dir.path().join("workspace");
        fs::create_dir_all(&workspace_dir).unwrap();

        let db = Arc::new(TestMemDb::default());
        db.upsert_agent(&make_agent("coder", vec!["*".into()])).unwrap();
        let registry =
            ToolRegistry::with_data_dir(db.clone(), make_cfg(&workspace_dir), data_dir.path())
                .unwrap();

        // Register a non-builtin custom tool with requires_approval=true to test FullAuto non-builtin handling.
        registry
            .register_or_import_custom_tools(&json!([
                {
                    "id": "custom_sensitive",
                    "description": "Sensitive custom tool",
                    "kind": "template",
                    "template": "ok:{{x}}",
                    "requires_approval": true
                }
            ]))
            .unwrap();

        // FullAuto auto-approves built-in tools (including delete_file), while prompting for requires_approval custom tools.
        fs::write(workspace_dir.join("tmp_auto.txt"), "data").unwrap();
        let full_auto = ToolExecContext {
            agent_id: "coder".into(),
            approval_policy: SandboxApprovalPolicy::FullAuto,
            ..Default::default()
        };
        registry
            .invoke("delete_file", json!({"path": "tmp_auto.txt"}), &full_auto)
            .await
            .unwrap();
        let custom_app = registry
            .invoke("custom_sensitive", json!({"x": "1"}), &full_auto)
            .await
            .unwrap_err();
        assert!(matches!(custom_app, AgentError::ApprovalRequired { .. }));

        // OnRequest requires approval on built-ins with requires_approval=true (delete_file).
        fs::write(workspace_dir.join("tmp_req.txt"), "data").unwrap();
        let on_req = ToolExecContext {
            agent_id: "coder".into(),
            approval_policy: SandboxApprovalPolicy::OnRequest,
            ..Default::default()
        };
        let req_err = registry
            .invoke("delete_file", json!({"path": "tmp_req.txt"}), &on_req)
            .await
            .unwrap_err();
        assert!(matches!(req_err, AgentError::ApprovalRequired { .. }));

        // AlwaysAsk requires approval even on mutating built-ins where requires_approval=false (write_artifact),
        // unless approval_granted=true or is_operator=true.
        let always_ask = ToolExecContext {
            agent_id: "coder".into(),
            approval_policy: SandboxApprovalPolicy::AlwaysAsk,
            ..Default::default()
        };
        let ask_err = registry
            .invoke(
                "write_artifact",
                json!({"filename": "gated.md", "content": "hi"}),
                &always_ask,
            )
            .await
            .unwrap_err();
        assert!(matches!(ask_err, AgentError::ApprovalRequired { .. }));

        let always_ask_granted = ToolExecContext {
            approval_granted: true,
            ..always_ask.clone()
        };
        let granted_ok = registry
            .invoke(
                "write_artifact",
                json!({"filename": "gated.md", "content": "hi"}),
                &always_ask_granted,
            )
            .await
            .unwrap();
        assert_eq!(granted_ok["ok"], true);

        let op_ctx = ToolExecContext {
            is_operator: true,
            ..always_ask
        };
        let op_ok = registry
            .invoke(
                "write_artifact",
                json!({"filename": "op.md", "content": "hi"}),
                &op_ctx,
            )
            .await
            .unwrap();
        assert_eq!(op_ok["ok"], true);

        // Never denies approval-requiring tools with PermissionDenied cleanly.
        let never_ctx = ToolExecContext {
            agent_id: "coder".into(),
            approval_policy: SandboxApprovalPolicy::Never,
            ..Default::default()
        };
        let never_err = registry
            .invoke("delete_file", json!({"path": "tmp_req.txt"}), &never_ctx)
            .await
            .unwrap_err();
        assert!(matches!(never_err, AgentError::PermissionDenied(_)));
    }

    #[tokio::test]
    async fn write_artifact_and_canvas_alias_save_sanitized_files_and_notify_bridge() {
        let data_dir = tempfile::tempdir().unwrap();
        let workspace_dir = data_dir.path().join("workspace");
        fs::create_dir_all(&workspace_dir).unwrap();

        let db = Arc::new(TestMemDb::default());
        db.upsert_agent(&make_agent("coder", vec!["*".into()])).unwrap();
        let registry =
            ToolRegistry::with_data_dir(db, make_cfg(&workspace_dir), data_dir.path()).unwrap();

        let bridge = Arc::new(RecordingBridge::new());
        let ctx = ToolExecContext {
            agent_id: "coder".into(),
            task_id: Some("task-9".into()),
            conversation_id: Some("conv-4".into()),
            a2a_bridge: Some(bridge.clone()),
            ..Default::default()
        };

        // `canvas` alias resolves to `write_artifact` and infers extension from JSON content.
        let res = registry
            .invoke(
                "canvas",
                json!({
                    "title": "Build Report (v2)",
                    "content": "{\"status\":\"ok\"}"
                }),
                &ctx,
            )
            .await
            .unwrap();
        assert_eq!(res["ok"], true);
        let rel = res["path"].as_str().unwrap().to_string();
        assert!(rel.starts_with("artifacts/") && rel.ends_with(".json"));
        assert_eq!(res["marker"], format!("[ARTIFACT_CREATED: {rel}]"));
        assert!(workspace_dir.join(&rel).is_file());
        assert_eq!(
            bridge.snapshot(),
            vec![format!("artifact:conv-4:task-9:{rel}:Build Report (v2)")]
        );

        // Reject traversal and disallowed file extensions.
        let trav = registry
            .invoke(
                "write_artifact",
                json!({"filename": "../escape.md", "content": "bad"}),
                &ctx,
            )
            .await
            .unwrap_err();
        assert!(matches!(trav, AgentError::InvalidInput(_)));

        let bad_ext = registry
            .invoke(
                "write_artifact",
                json!({"filename": "payload.exe", "content": "bad"}),
                &ctx,
            )
            .await
            .unwrap_err();
        assert!(matches!(bad_ext, AgentError::InvalidInput(_)));
    }

    #[tokio::test]
    async fn extended_agent_tool_bridge_methods_work_and_default_cleanly() {
        let data_dir = tempfile::tempdir().unwrap();
        let workspace_dir = data_dir.path().join("workspace");
        fs::create_dir_all(&workspace_dir).unwrap();

        let db = Arc::new(TestMemDb::default());
        db.upsert_agent(&make_agent("coder", vec!["*".into()])).unwrap();
        let registry =
            ToolRegistry::with_data_dir(db, make_cfg(&workspace_dir), data_dir.path()).unwrap();

        // Fallback system_info / ps5_info work even without a bridge; MinimalBridge inherits default Err for memory_read.
        let no_bridge_ctx = ToolExecContext {
            agent_id: "coder".into(),
            ..Default::default()
        };
        let sys_fallback = registry
            .invoke("system_info", json!({}), &no_bridge_ctx)
            .await
            .unwrap();
        assert!(sys_fallback.get("os").is_some());
        let ps5_fallback = registry
            .invoke("ps5_info", json!({}), &no_bridge_ctx)
            .await
            .unwrap();
        assert_eq!(ps5_fallback["virtual_shell_default"], true);

        let minimal_ctx = ToolExecContext {
            agent_id: "coder".into(),
            a2a_bridge: Some(Arc::new(MinimalBridge)),
            ..Default::default()
        };
        let default_mem_err = registry
            .invoke("memory_read", json!({"query": "test"}), &minimal_ctx)
            .await
            .unwrap_err();
        assert!(matches!(default_mem_err, AgentError::Tool { .. }));

        // Full RecordingBridge handles all extended runtime bridge tools.
        let bridge = Arc::new(RecordingBridge::new());
        let ctx = ToolExecContext {
            agent_id: "coder".into(),
            task_id: Some("t1".into()),
            conversation_id: Some("c1".into()),
            a2a_bridge: Some(bridge.clone()),
            ..Default::default()
        };

        assert_eq!(
            registry.invoke("memory_read", json!({"limit": 5}), &ctx).await.unwrap()["limit"],
            5
        );
        assert_eq!(
            registry
                .invoke("memory_write", json!({"key": "k", "content": "v", "importance": 9}), &ctx)
                .await
                .unwrap()["key"],
            "k"
        );
        assert_eq!(
            registry.invoke("memory_search", json!({"query": "ps5"}), &ctx).await.unwrap()["query"],
            "ps5"
        );
        assert_eq!(
            registry.invoke("memory_get", json!({"path": "MEMORY.md", "from_line": 1, "lines": 20}), &ctx).await.unwrap()["path"],
            "MEMORY.md"
        );
        assert_eq!(registry.invoke("system_info", json!({}), &ctx).await.unwrap()["source"], "bridge");
        assert_eq!(registry.invoke("ps5_info", json!({}), &ctx).await.unwrap()["platform"], "test-ps5");
        assert_eq!(registry.invoke("doctor", json!({"fix": false}), &ctx).await.unwrap()["ok"], true);
        assert_eq!(registry.invoke("process", json!({"action": "list"}), &ctx).await.unwrap()["action"], "list");
        assert!(registry.invoke("sessions_list", json!({"limit": 10, "active_only": true}), &ctx).await.unwrap()["sessions"].is_array());
        assert_eq!(registry.invoke("sessions_spawn", json!({"prompt": "hi"}), &ctx).await.unwrap()["target"], Value::Null);
        assert_eq!(
            registry.invoke("sessions_send", json!({"conversation_id": "c2", "message": "ping"}), &ctx).await.unwrap()["conversation_id"],
            "c2"
        );
        assert_eq!(
            registry.invoke("session_status", json!({}), &ctx).await.unwrap()["conversation_id"],
            "c1"
        );
        assert_eq!(bridge.snapshot().len(), 12);
    }
}
