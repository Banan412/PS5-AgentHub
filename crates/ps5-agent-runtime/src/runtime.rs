use crate::scheduler::{
    compute_next_run_at, parse_schedule_with_ai, validate_schedule_fields, AiScheduleDraft,
    CreateScheduleInput, UpdateScheduleInput,
};
use crate::task_manager::{task_status_str, RecoveryReport, TaskManager};
use crate::traits::{RuntimeMemoryProvider, RuntimeToolExecutor, StorageMemoryProvider};
use chrono::{DateTime, Utc};
use ps5_agent_config::AppConfig;
use ps5_agent_core::{
    agent_allows_tool_with_policy, canonical_tool_id, new_id, now_iso, AgentDefinition,
    AgentError, AgentMessageType, AgentRole, ApprovalRecord, ApprovalStatus, AuditLogEntry,
    ChatAttachment, ChatMessage, ChatRole, Conversation, ConversationMode, InferenceMessage,
    InferenceProvider, InferenceRequest, InferenceResponse, InferenceStreamChunk, Result,
    RuntimeLimits, SandboxApprovalPolicy, SandboxMode, ScheduleKind, ScheduleRecord,
    StorageBackend, StreamEvent, TaskRecord, TaskStatus, ThinkingLevel, ToolCallRequest,
    ToolDefinitionSchema, ToolExecutionRecord, ToolPermission, ToolSpec,
};
use ps5_agent_logging::{log_info, log_warn};
use ps5_agent_manager::{AgentManager, CreateAgentInput};
use ps5_agent_messaging::{MessageBus, SendMessageRequest};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

const UNLIMITED_SEMAPHORE_PERMITS: usize = 4096;
const UNLIMITED_REASONING_SAFETY_CAP: u32 = 256;
const MAX_PENDING_APPROVALS: usize = 256;
const MAX_AUTO_CONTINUATIONS: u8 = 4;

/// Matches an `enabled_tools` allowlist pattern list against `tool_id`.
/// Delegates to [`agent_allows_tool_with_policy`] so exact IDs, aliases, `"*"`, `group:*`,
/// and suffix-wildcard prefixes (`"mcp:*"`, `"plugin:*"`, `"file_*"`) behave uniformly.
pub fn tool_pattern_matches(enabled_tools: &[String], tool_id: &str) -> bool {
    agent_allows_tool_with_policy(enabled_tools, &[], tool_id, "")
}

/// Evaluates whether `enabled_tools` permits a `ToolSpec`, including `"custom:*"` and `group:*`.
pub fn tool_spec_matches(enabled_tools: &[String], spec: &ToolSpec) -> bool {
    agent_allows_tool_with_policy(enabled_tools, &[], &spec.id, &spec.category)
}

fn agent_allows_tool_spec(agent: &AgentDefinition, spec: &ToolSpec) -> bool {
    agent_allows_tool_with_policy(&agent.enabled_tools, &agent.denied_tools, &spec.id, &spec.category)
}

fn agent_allows_tool_name(agent: &AgentDefinition, tool_id: &str, category: &str) -> bool {
    agent_allows_tool_with_policy(&agent.enabled_tools, &agent.denied_tools, tool_id, category)
}

fn primary_workspace_root(cfg: &AppConfig) -> PathBuf {
    cfg.tools
        .allowed_paths
        .first()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| cfg.data_dir.join("workspace"))
}

fn resolve_effective_sandbox_mode(agent: &AgentDefinition, cfg: &AppConfig) -> SandboxMode {
    agent
        .sandbox_mode
        .as_deref()
        .and_then(SandboxMode::parse)
        .or_else(|| SandboxMode::parse(&cfg.sandbox.mode))
        .unwrap_or(SandboxMode::FullSandbox)
}

fn resolve_effective_approval_policy(cfg: &AppConfig) -> SandboxApprovalPolicy {
    let parsed = SandboxApprovalPolicy::parse(&cfg.sandbox.approval_policy)
        .unwrap_or(SandboxApprovalPolicy::FullAuto);
    if parsed == SandboxApprovalPolicy::FullAuto && cfg.security.require_approval_for_mutating {
        SandboxApprovalPolicy::OnRequest
    } else {
        parsed
    }
}

fn is_builtin_tool_spec(spec: &ToolSpec) -> bool {
    let id = spec.id.trim();
    !matches!(spec.category.as_str(), "custom" | "mcp" | "plugin")
        && !id.starts_with("custom:")
        && !id.starts_with("mcp:")
        && !id.starts_with("plugin:")
}

fn json_api_method_is_mutating(args: &serde_json::Value) -> bool {
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

fn check_runtime_sandbox_mode(
    spec: &ToolSpec,
    args: &serde_json::Value,
    mode: SandboxMode,
) -> Result<()> {
    let canonical = canonical_tool_id(&spec.id);
    match mode {
        SandboxMode::FullSandbox => Ok(()),
        SandboxMode::WorkspaceWrite => {
            if canonical == "http_post"
                || (canonical == "json_api_request" && json_api_method_is_mutating(args))
                || (!is_builtin_tool_spec(spec)
                    && spec.is_mutating
                    && spec.required_permissions.contains(&ToolPermission::WebWrite))
            {
                return Err(AgentError::PermissionDenied(format!(
                    "mutating web tool '{canonical}' is blocked under sandbox_mode 'workspace_write'"
                )));
            }
            Ok(())
        }
        SandboxMode::ReadOnly => {
            if canonical == "sandbox_exec" {
                return Ok(());
            }
            if canonical == "browser" {
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
            if canonical == "doctor" {
                if args.get("fix").and_then(|v| v.as_bool()).unwrap_or(false) {
                    return Err(AgentError::PermissionDenied(
                        "doctor fix=true is blocked under sandbox_mode 'read_only'".into(),
                    ));
                }
                return Ok(());
            }
            if canonical == "json_api_request" {
                if json_api_method_is_mutating(args) {
                    return Err(AgentError::PermissionDenied(
                        "mutating json_api_request is blocked under sandbox_mode 'read_only'"
                            .into(),
                    ));
                }
                return Ok(());
            }
            let has_mutating_perm = spec.required_permissions.iter().any(|p| {
                matches!(
                    p,
                    ToolPermission::FileWrite | ToolPermission::FileDelete | ToolPermission::WebWrite
                )
            });
            if spec.is_mutating || has_mutating_perm {
                return Err(AgentError::PermissionDenied(format!(
                    "mutating tool '{canonical}' is blocked under sandbox_mode 'read_only'"
                )));
            }
            Ok(())
        }
    }
}

fn evaluate_runtime_approval_policy(
    spec: &ToolSpec,
    cfg: &AppConfig,
    approval_policy: SandboxApprovalPolicy,
    persist_chat_message: bool,
    approved_bypass: bool,
) -> Result<(bool, SandboxApprovalPolicy)> {
    if approved_bypass {
        return Ok((false, approval_policy));
    }
    let is_builtin = is_builtin_tool_spec(spec);
    match approval_policy {
        SandboxApprovalPolicy::Never => {
            if spec.requires_approval {
                return Err(AgentError::PermissionDenied(format!(
                    "tool '{}' requires approval which is blocked by approval policy 'never'",
                    spec.id
                )));
            }
            Ok((false, SandboxApprovalPolicy::Never))
        }
        SandboxApprovalPolicy::FullAuto => {
            let needs = !is_builtin && spec.requires_approval;
            if needs && !persist_chat_message && !cfg.security.require_approval_for_mutating {
                return Ok((false, SandboxApprovalPolicy::FullAuto));
            }
            Ok((needs, SandboxApprovalPolicy::FullAuto))
        }
        SandboxApprovalPolicy::OnRequest => {
            if !persist_chat_message && !cfg.security.require_approval_for_mutating {
                // Non-interactive background/scheduled turns treat on_request autonomously so
                // unattended runs do not deadlock waiting for human input.
                let pass_policy = if is_builtin {
                    SandboxApprovalPolicy::FullAuto
                } else {
                    SandboxApprovalPolicy::OnRequest
                };
                return Ok((false, pass_policy));
            }
            Ok((spec.requires_approval, SandboxApprovalPolicy::OnRequest))
        }
        SandboxApprovalPolicy::AlwaysAsk => Ok((
            spec.is_mutating || spec.requires_approval,
            SandboxApprovalPolicy::AlwaysAsk,
        )),
    }
}

fn compute_delayed_run_at(delay_secs: u64) -> Result<DateTime<Utc>> {
    let secs = i64::try_from(delay_secs)
        .map_err(|_| AgentError::InvalidInput("delay_secs is out of range".into()))?;
    let delta = chrono::TimeDelta::try_seconds(secs)
        .ok_or_else(|| AgentError::InvalidInput("delay_secs is out of range".into()))?;
    Utc::now()
        .checked_add_signed(delta)
        .ok_or_else(|| AgentError::InvalidInput("delay_secs timestamp overflow".into()))
}

fn map_semaphore_permits(limit: usize) -> usize {
    if limit == 0 {
        UNLIMITED_SEMAPHORE_PERMITS
    } else {
        limit
    }
}

fn resolve_max_iterations(agent_limit: u32, global_limit: u32) -> Option<u32> {
    match (agent_limit, global_limit) {
        (0, 0) => None,
        (a, 0) => Some(a),
        (0, g) => Some(g),
        (a, g) => Some(a.min(g)),
    }
}

fn shrink_map_if_sparse<K: Eq + std::hash::Hash, V>(map: &mut HashMap<K, V>) {
    if map.capacity() > 32 && map.len().saturating_mul(4) < map.capacity() {
        map.shrink_to_fit();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatTurnResult {
    pub conversation_id: String,
    pub task_id: String,
    pub agent_id: String,
    pub status: TaskStatus,
    pub message_id: Option<String>,
    pub content: String,
    pub pending_approval_id: Option<String>,
    pub selected_participants: Vec<String>,
}

pub type ChatDispatchOutcome = ChatTurnResult;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskExecutionOutcome {
    pub task_id: String,
    pub status: TaskStatus,
    pub result: Option<String>,
    pub pending_approval_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApprovalResolutionOutcome {
    pub approval_id: String,
    pub approved: bool,
    pub task_id: Option<String>,
    pub task_status: Option<TaskStatus>,
    pub result: Option<String>,
    pub pending_approval_id: Option<String>,
}

struct PausedExecutionState {
    seq: u64,
    task_id: String,
    root_task_id: String,
    conversation_id: Option<String>,
    agent_id: String,
    messages: Vec<InferenceMessage>,
    pending_tool_call: ToolCallRequest,
    remaining_tool_calls: Vec<ToolCallRequest>,
    next_iteration: u32,
    hop_count: u32,
    persist_chat_message: bool,
    thinking_level: Option<ThinkingLevel>,
    raw_user_request: String,
    auto_continuations: u8,
    continuation_parts: Vec<String>,
    did_write_file: bool,
    written_paths: Vec<String>,
}

struct ReactTurnInput {
    task_id: String,
    root_task_id: String,
    conversation_id: Option<String>,
    agent: AgentDefinition,
    user_request: String,
    raw_user_request: String,
    user_message_id: Option<String>,
    thinking_level: Option<ThinkingLevel>,
    attachments: Vec<ChatAttachment>,
    hop_count: u32,
    persist_chat_message: bool,
}

struct ReactLoopOptions {
    start_iter: u32,
    max_iters: Option<u32>,
    thinking_level: Option<ThinkingLevel>,
    raw_user_request: String,
    auto_continuations: u8,
    continuation_parts: Vec<String>,
    did_write_file: bool,
    written_paths: Vec<String>,
}

enum ResolvedDispatchTarget {
    Direct {
        agent: AgentDefinition,
        effective_prompt: String,
    },
    Group {
        commander: AgentDefinition,
        participant_ids: Vec<String>,
    },
}

#[derive(Debug, Deserialize)]
struct GroupCoordinatorDecision {
    #[serde(default)]
    selected_agents: Vec<String>,
}

struct ScheduleRunGuard {
    schedule_id: String,
    running: Arc<Mutex<HashSet<String>>>,
}

impl Drop for ScheduleRunGuard {
    fn drop(&mut self) {
        self.running
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&self.schedule_id);
    }
}

struct InFlightApprovalGuard {
    approval_id: String,
    in_flight: Arc<Mutex<HashSet<String>>>,
}

impl Drop for InFlightApprovalGuard {
    fn drop(&mut self) {
        self.in_flight
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&self.approval_id);
    }
}

pub struct AgentRuntime {
    storage: Arc<dyn StorageBackend>,
    config: RwLock<AppConfig>,
    inference: Arc<dyn InferenceProvider>,
    tool_executor: Arc<dyn RuntimeToolExecutor>,
    memory_provider: Arc<dyn RuntimeMemoryProvider>,
    agent_manager: Arc<AgentManager>,
    message_bus: Arc<MessageBus>,
    task_manager: Arc<TaskManager>,
    events_tx: broadcast::Sender<StreamEvent>,
    task_semaphore: RwLock<Arc<Semaphore>>,
    inference_semaphore: RwLock<Arc<Semaphore>>,
    pending_approvals: RwLock<HashMap<String, PausedExecutionState>>,
    in_flight_approvals: Arc<Mutex<HashSet<String>>>,
    approval_seq: AtomicU64,
    running_schedules: Arc<Mutex<HashSet<String>>>,
    last_heartbeat_at: Mutex<Option<Instant>>,
    self_weak: RwLock<Weak<AgentRuntime>>,
}

impl AgentRuntime {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        storage: Arc<dyn StorageBackend>,
        config: AppConfig,
        inference: Arc<dyn InferenceProvider>,
        tool_executor: Arc<dyn RuntimeToolExecutor>,
        memory_provider: Option<Arc<dyn RuntimeMemoryProvider>>,
        agent_manager: Arc<AgentManager>,
        message_bus: Arc<MessageBus>,
        events_tx: broadcast::Sender<StreamEvent>,
    ) -> Arc<Self> {
        let limits: RuntimeLimits = (&config.agents).into();
        let task_manager = Arc::new(TaskManager::new(
            storage.clone(),
            limits.clone(),
            events_tx.clone(),
        ));
        agent_manager.attach_task_controller(task_manager.clone());

        let mem_provider = memory_provider.unwrap_or_else(|| {
            Arc::new(StorageMemoryProvider::new(
                storage.clone(),
                config.memory.clone(),
            ))
        });

        let task_permits = map_semaphore_permits(limits.max_active_tasks);
        let inf_permits = map_semaphore_permits(limits.max_concurrent_inference);

        let rt = Arc::new(Self {
            storage,
            config: RwLock::new(config),
            inference,
            tool_executor,
            memory_provider: mem_provider,
            agent_manager,
            message_bus,
            task_manager,
            events_tx,
            task_semaphore: RwLock::new(Arc::new(Semaphore::new(task_permits))),
            inference_semaphore: RwLock::new(Arc::new(Semaphore::new(inf_permits))),
            pending_approvals: RwLock::new(HashMap::new()),
            in_flight_approvals: Arc::new(Mutex::new(HashSet::new())),
            approval_seq: AtomicU64::new(1),
            running_schedules: Arc::new(Mutex::new(HashSet::new())),
            last_heartbeat_at: Mutex::new(None),
            self_weak: RwLock::new(Weak::new()),
        });
        if let Ok(mut weak) = rt.self_weak.write() {
            *weak = Arc::downgrade(&rt);
        }
        rt
    }

    fn arc_self(&self) -> Result<Arc<Self>> {
        self.self_weak
            .read()
            .map_err(|_| AgentError::Internal("self_weak lock poisoned".into()))?
            .upgrade()
            .ok_or_else(|| AgentError::Internal("AgentRuntime is shutting down".into()))
    }

    pub fn storage(&self) -> &Arc<dyn StorageBackend> {
        &self.storage
    }

    pub async fn execute_slash_command(
        &self,
        conversation_id: Option<&str>,
        agent_id: Option<&str>,
        input: &str,
        cancel: CancellationToken,
    ) -> Result<ps5_agent_core::SlashCommandResponse> {
        crate::slash::execute_slash_command(
            input,
            conversation_id,
            agent_id,
            &self.config,
            &*self.storage,
            &*self.inference,
            &*self.tool_executor,
            cancel,
        )
        .await
    }

    pub async fn run_doctor_checks(
        &self,
        builtin_tool_ids: &[String],
    ) -> Result<ps5_agent_core::DoctorReport> {
        let cfg = self.config();
        crate::doctor::run_doctor_checks(
            &cfg,
            &*self.storage,
            Some(&*self.inference),
            builtin_tool_ids,
        )
        .await
    }

    pub async fn run_doctor_fixes(
        &self,
        builtin_tool_ids: &[String],
    ) -> Result<ps5_agent_core::DoctorFixReport> {
        let cfg = self.config();
        crate::doctor::run_doctor_fixes(
            &cfg,
            &*self.storage,
            Some(&*self.inference),
            builtin_tool_ids,
        )
        .await
    }

    /// Evaluates `HEARTBEAT.md` when `sandbox.heartbeat_interval_secs > 0` and submits a background
    /// task for Commander when actionable items exist, returning `Ok(Some(task_id))` if triggered.
    pub async fn tick_heartbeat_if_due(&self) -> Result<Option<String>> {
        let cfg = self.config();
        let interval_secs = cfg.sandbox.heartbeat_interval_secs;
        if interval_secs == 0 {
            return Ok(None);
        }
        let interval = Duration::from_secs(interval_secs);
        {
            let mut last = self
                .last_heartbeat_at
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            if let Some(prev) = *last {
                if prev.elapsed() < interval {
                    return Ok(None);
                }
            }
            *last = Some(Instant::now());
        }

        let workspace_root = primary_workspace_root(&cfg);
        let Some(actionable) = crate::brain::read_actionable_heartbeat(&workspace_root) else {
            return Ok(None);
        };

        let target_agent = self.resolve_schedule_target_agent(None)?;
        let rt = self.arc_self()?;
        let prompt = format!(
            "Periodic workspace heartbeat check (`HEARTBEAT.md`). Review and act concisely on any actionable items:\n\n{actionable}"
        );
        let task = rt.submit_task(prompt, target_agent.id, None, None).await?;
        Ok(Some(task.id))
    }

    fn acquire_in_flight_approval(&self, approval_id: &str) -> Result<InFlightApprovalGuard> {
        let mut guard = self
            .in_flight_approvals
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if !guard.insert(approval_id.to_string()) {
            return Err(AgentError::Conflict(format!(
                "approval '{approval_id}' is already being resolved"
            )));
        }
        Ok(InFlightApprovalGuard {
            approval_id: approval_id.to_string(),
            in_flight: Arc::clone(&self.in_flight_approvals),
        })
    }

    fn acquire_schedule_guard(&self, schedule_id: &str) -> Result<Option<ScheduleRunGuard>> {
        let mut guard = self
            .running_schedules
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if !guard.insert(schedule_id.to_string()) {
            return Ok(None);
        }
        Ok(Some(ScheduleRunGuard {
            schedule_id: schedule_id.to_string(),
            running: Arc::clone(&self.running_schedules),
        }))
    }

    fn emit_chat_error_message(&self, conversation_id: &str, detail: &str) {
        let _ = self.events_tx.send(StreamEvent::SystemAlert {
            level: "error".to_string(),
            message: format!("Chat task failed ({conversation_id}): {detail}"),
        });
    }

    fn emit_chat_error_alert(&self, conversation_id: &str, err: &AgentError) {
        if matches!(err, AgentError::Cancelled(_)) {
            return;
        }
        self.emit_chat_error_message(conversation_id, &err.to_string());
    }

    pub fn events(&self) -> broadcast::Receiver<StreamEvent> {
        self.events_tx.subscribe()
    }

    pub fn task_manager(&self) -> &Arc<TaskManager> {
        &self.task_manager
    }

    pub fn agent_manager(&self) -> &Arc<AgentManager> {
        &self.agent_manager
    }

    pub fn message_bus(&self) -> &Arc<MessageBus> {
        &self.message_bus
    }

    pub fn config(&self) -> AppConfig {
        self.config.read().map(|g| g.clone()).unwrap_or_default()
    }

    fn current_task_semaphore(&self) -> Result<Arc<Semaphore>> {
        self.task_semaphore
            .read()
            .map(|g| Arc::clone(&g))
            .map_err(|_| AgentError::Internal("task semaphore lock poisoned".into()))
    }

    fn current_inference_semaphore(&self) -> Result<Arc<Semaphore>> {
        self.inference_semaphore
            .read()
            .map(|g| Arc::clone(&g))
            .map_err(|_| AgentError::Internal("inference semaphore lock poisoned".into()))
    }

    pub fn evict_pending_for_task(&self, task_id: &str) -> usize {
        let Ok(mut guard) = self.pending_approvals.write() else {
            return 0;
        };
        let before = guard.len();
        guard.retain(|_, s| s.task_id != task_id && s.root_task_id != task_id);
        let removed = before.saturating_sub(guard.len());
        shrink_map_if_sparse(&mut guard);
        removed
    }

    pub fn evict_pending_for_conversation(
        &self,
        conv_id: &str,
        agent_id: Option<&str>,
    ) -> usize {
        let Ok(mut guard) = self.pending_approvals.write() else {
            return 0;
        };
        let before = guard.len();
        guard.retain(|_, s| {
            let conv_match = s.conversation_id.as_deref() == Some(conv_id);
            let agent_match = agent_id.is_none_or(|aid| s.agent_id == aid);
            !(conv_match && agent_match)
        });
        let removed = before.saturating_sub(guard.len());
        shrink_map_if_sparse(&mut guard);
        removed
    }

    pub fn evict_pending_for_agent(&self, agent_id: &str) -> usize {
        let Ok(mut guard) = self.pending_approvals.write() else {
            return 0;
        };
        let before = guard.len();
        guard.retain(|_, s| s.agent_id != agent_id);
        let removed = before.saturating_sub(guard.len());
        shrink_map_if_sparse(&mut guard);
        removed
    }

    pub fn pending_approval_count(&self) -> usize {
        self.pending_approvals
            .read()
            .map(|g| g.len())
            .unwrap_or(0)
    }

    pub fn update_config(&self, cfg: AppConfig) {
        let old_limits: RuntimeLimits = (&self.config().agents).into();
        let limits: RuntimeLimits = (&cfg.agents).into();
        let old_task_permits = map_semaphore_permits(old_limits.max_active_tasks);
        let new_task_permits = map_semaphore_permits(limits.max_active_tasks);
        let old_inf_permits = map_semaphore_permits(old_limits.max_concurrent_inference);
        let new_inf_permits = map_semaphore_permits(limits.max_concurrent_inference);

        self.agent_manager.update_config(cfg.agents.clone());
        self.message_bus.update_limits(limits.clone());
        self.task_manager.update_limits(limits);

        if old_task_permits != new_task_permits {
            if let Ok(mut guard) = self.task_semaphore.write() {
                *guard = Arc::new(Semaphore::new(new_task_permits));
            }
        }
        if old_inf_permits != new_inf_permits {
            if let Ok(mut guard) = self.inference_semaphore.write() {
                *guard = Arc::new(Semaphore::new(new_inf_permits));
            }
        }
        if let Ok(mut guard) = self.config.write() {
            *guard = cfg;
        }
    }

    /// Startup recovery: moves side-effecting interrupted tasks to WAITING without
    /// replaying dangerous actions, and spawns execution for read-only resumed tasks.
    pub async fn recover_on_startup(self: &Arc<Self>) -> Result<RecoveryReport> {
        let report = self.task_manager.recover_on_startup()?;
        for task in &report.resumed_tasks {
            let rt = Arc::clone(self);
            let tid = task.id.clone();
            tokio::spawn(async move {
                if let Err(e) = rt.run_task_by_id(&tid).await {
                    log_warn("runtime", format!("recovered task '{tid}' ended with error: {e}"));
                }
            });
        }
        Ok(report)
    }

    pub async fn chat_commander(
        self: &Arc<Self>,
        conversation_id: &str,
        user_content: &str,
        cancel: CancellationToken,
    ) -> Result<ChatTurnResult> {
        self.chat_with_agent_with_options(
            conversation_id,
            "commander",
            user_content,
            None,
            Vec::new(),
            cancel,
        )
        .await
    }

    pub async fn chat_with_agent_with_options(
        self: &Arc<Self>,
        conversation_id: &str,
        agent_id: &str,
        user_content: &str,
        thinking_level: Option<ThinkingLevel>,
        attachments: Vec<ChatAttachment>,
        cancel: CancellationToken,
    ) -> Result<ChatDispatchOutcome> {
        if user_content.trim().is_empty() && attachments.is_empty() {
            return Err(AgentError::InvalidInput("message content cannot be empty".into()));
        }

        let _conv = self
            .storage
            .get_conversation(conversation_id)?
            .ok_or_else(|| {
                AgentError::NotFound(format!("conversation '{conversation_id}' not found"))
            })?;

        let agent = self
            .agent_manager
            .get_agent(agent_id)?
            .ok_or_else(|| AgentError::NotFound(format!("agent '{agent_id}' not found")))?;
        if !agent.enabled {
            return Err(AgentError::PermissionDenied(format!(
                "agent '{agent_id}' is disabled"
            )));
        }

        let effective_prompt = format_explicit_agent_prompt(&agent, user_content);
        let user_msg =
            self.append_user_message_with_attachments(conversation_id, user_content, attachments.clone())?;

        self.run_direct_agent_chat_with_options(
            conversation_id,
            agent,
            &effective_prompt,
            user_content,
            Some(user_msg.id),
            thinking_level,
            attachments,
            cancel,
        )
        .await
    }

    pub async fn handle_chat_message(
        self: &Arc<Self>,
        conversation_id: &str,
        user_content: &str,
        cancel: CancellationToken,
    ) -> Result<ChatTurnResult> {
        self.handle_chat_message_with_options(
            conversation_id,
            user_content,
            None,
            None,
            Vec::new(),
            cancel,
        )
        .await
    }

    pub async fn handle_chat_message_with_options(
        self: &Arc<Self>,
        conversation_id: &str,
        user_content: &str,
        target_agent_id: Option<&str>,
        thinking_level: Option<ThinkingLevel>,
        attachments: Vec<ChatAttachment>,
        cancel: CancellationToken,
    ) -> Result<ChatDispatchOutcome> {
        if let Some(target_id) = target_agent_id.map(str::trim).filter(|s| !s.is_empty()) {
            return self
                .chat_with_agent_with_options(
                    conversation_id,
                    target_id,
                    user_content,
                    thinking_level,
                    attachments,
                    cancel,
                )
                .await;
        }

        if user_content.trim().is_empty() && attachments.is_empty() {
            return Err(AgentError::InvalidInput("message content cannot be empty".into()));
        }

        let conv = self
            .storage
            .get_conversation(conversation_id)?
            .ok_or_else(|| {
                AgentError::NotFound(format!("conversation '{conversation_id}' not found"))
            })?;

        let target = self.resolve_dispatch_target(&conv.mode, None, user_content)?;
        let user_msg =
            self.append_user_message_with_attachments(conversation_id, user_content, attachments.clone())?;

        match target {
            ResolvedDispatchTarget::Direct {
                agent,
                effective_prompt,
            } => {
                self.run_direct_agent_chat_with_options(
                    conversation_id,
                    agent,
                    &effective_prompt,
                    user_content,
                    Some(user_msg.id),
                    thinking_level,
                    attachments,
                    cancel,
                )
                .await
            }
            ResolvedDispatchTarget::Group {
                commander,
                participant_ids,
            } => {
                self.run_group_chat_with_options(
                    conversation_id,
                    commander,
                    &participant_ids,
                    user_content,
                    Some(user_msg.id),
                    thinking_level,
                    attachments,
                    cancel,
                )
                .await
            }
        }
    }

    pub async fn dispatch_chat_background(
        self: &Arc<Self>,
        conversation_id: String,
        user_content: String,
        target_agent_id: Option<String>,
        thinking_level: Option<ThinkingLevel>,
        attachments: Vec<ChatAttachment>,
        cancel: CancellationToken,
    ) -> Result<(ChatMessage, TaskRecord)> {
        self.dispatch_chat_background_with_options(
            conversation_id,
            user_content,
            target_agent_id,
            thinking_level,
            attachments,
            false,
            cancel,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn dispatch_chat_background_with_options(
        self: &Arc<Self>,
        conversation_id: String,
        user_content: String,
        target_agent_id: Option<String>,
        thinking_level: Option<ThinkingLevel>,
        attachments: Vec<ChatAttachment>,
        immediate: bool,
        cancel: CancellationToken,
    ) -> Result<(ChatMessage, TaskRecord)> {
        if user_content.trim().is_empty() && attachments.is_empty() {
            return Err(AgentError::InvalidInput("message content cannot be empty".into()));
        }

        let conv = self
            .storage
            .get_conversation(&conversation_id)?
            .ok_or_else(|| {
                AgentError::NotFound(format!("conversation '{conversation_id}' not found"))
            })?;

        let target = self.resolve_dispatch_target(
            &conv.mode,
            target_agent_id.as_deref(),
            &user_content,
        )?;

        let (assigned_agent_id, priority) = match &target {
            ResolvedDispatchTarget::Direct { agent, .. } => (agent.id.clone(), agent.priority),
            ResolvedDispatchTarget::Group { commander, .. } => {
                (commander.id.clone(), commander.priority)
            }
        };

        let user_message = self.append_user_message_with_attachments(
            &conversation_id,
            &user_content,
            attachments.clone(),
        )?;

        let root_task = self.task_manager.create_task(
            user_content.clone(),
            assigned_agent_id,
            Some(conversation_id.clone()),
            None,
            priority,
            TaskStatus::Pending,
        )?;
        let task_cancel = self.task_manager.register_token(&root_task.id, Some(&cancel));
        let expedite = self.task_manager.register_expedite(&root_task.id);

        let rt = Arc::clone(self);
        let bg_task_id = root_task.id.clone();
        let bg_user_msg_id = user_message.id.clone();
        tokio::spawn(async move {
            let Ok(_permit) = rt
                .await_task_slot_or_expedite(&bg_task_id, immediate, expedite, &task_cancel)
                .await
            else {
                rt.evict_pending_for_task(&bg_task_id);
                rt.task_manager.release_token(&bg_task_id);
                return;
            };

            let bg_conv_id = conversation_id.clone();
            let res = match target {
                ResolvedDispatchTarget::Direct {
                    agent,
                    effective_prompt,
                } => {
                    rt.execute_task_with_timeout(
                        ReactTurnInput {
                            task_id: bg_task_id.clone(),
                            root_task_id: bg_task_id.clone(),
                            conversation_id: Some(conversation_id),
                            agent,
                            user_request: effective_prompt,
                            raw_user_request: user_content,
                            user_message_id: Some(bg_user_msg_id),
                            thinking_level,
                            attachments,
                            hop_count: 0,
                            persist_chat_message: true,
                        },
                        task_cancel,
                    )
                    .await
                }
                ResolvedDispatchTarget::Group {
                    commander,
                    participant_ids,
                } => {
                    rt.run_group_chat_with_task(
                        &conversation_id,
                        commander,
                        &participant_ids,
                        &user_content,
                        Some(bg_user_msg_id),
                        thinking_level,
                        attachments,
                        bg_task_id.clone(),
                        task_cancel,
                    )
                    .await
                }
            };

            if let Err(e) = res {
                log_warn(
                    "runtime",
                    format!("background chat task '{bg_task_id}' failed: {e}"),
                );
                rt.emit_chat_error_alert(&bg_conv_id, &e);
                rt.evict_pending_for_task(&bg_task_id);
                rt.task_manager.release_token(&bg_task_id);
            }
        });

        let latest_task = self
            .storage
            .get_task(&root_task.id)
            .ok()
            .flatten()
            .unwrap_or(root_task);
        Ok((user_message, latest_task))
    }

    pub fn cancel_conversation_tasks(
        &self,
        conv_id: &str,
        agent_id: Option<&str>,
        reason: &str,
    ) -> Result<usize> {
        self.evict_pending_for_conversation(conv_id, agent_id);
        self.task_manager
            .cancel_conversation_tasks(conv_id, agent_id, reason)
    }

    pub fn expedite_task(&self, task_id: &str) -> Result<TaskRecord> {
        self.task_manager.expedite_task(task_id)
    }

    async fn await_task_slot_or_expedite(
        &self,
        task_id: &str,
        immediate: bool,
        expedite: Arc<tokio::sync::Notify>,
        cancel: &CancellationToken,
    ) -> std::result::Result<Option<tokio::sync::OwnedSemaphorePermit>, ()> {
        let permit = if immediate {
            None
        } else {
            let task_sem = match self.current_task_semaphore() {
                Ok(s) => s,
                Err(e) => {
                    let _ = self.task_manager.fail_task(task_id, e.to_string());
                    return Err(());
                }
            };
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    let _ = self.task_manager.cancel_task(task_id, "cancelled while pending");
                    return Err(());
                }
                _ = expedite.notified() => None,
                acq = task_sem.acquire_owned() => match acq {
                    Ok(p) => Some(p),
                    Err(e) => {
                        let _ = self
                            .task_manager
                            .fail_task(task_id, format!("task semaphore closed: {e}"));
                        return Err(());
                    }
                }
            }
        };
        if cancel.is_cancelled() {
            let _ = self
                .task_manager
                .cancel_task(task_id, "cancelled while pending");
            return Err(());
        }
        Ok(permit)
    }

    fn top_conversation_sort_order(&self) -> i32 {
        let min_order = self
            .storage
            .list_conversations(true)
            .unwrap_or_default()
            .into_iter()
            .map(|c| c.sort_order)
            .min()
            .unwrap_or(0);
        min_order.min(0).saturating_sub(10)
    }

    pub fn reorder_conversations(
        &self,
        ordered_ids: Vec<String>,
        move_item: Option<(String, String)>,
    ) -> Result<Vec<Conversation>> {
        let current = self.storage.list_conversations(false)?;
        let final_ids: Vec<String> = if let Some((conv_id, direction)) = move_item {
            let pos = current
                .iter()
                .position(|c| c.id == conv_id)
                .ok_or_else(|| {
                    AgentError::NotFound(format!("conversation '{conv_id}' not found"))
                })?;
            let mut ids: Vec<String> = current.iter().map(|c| c.id.clone()).collect();
            match direction.trim().to_ascii_lowercase().as_str() {
                "up" => {
                    if pos > 0 {
                        ids.swap(pos, pos - 1);
                    }
                }
                "down" => {
                    if pos + 1 < ids.len() {
                        ids.swap(pos, pos + 1);
                    }
                }
                other => {
                    return Err(AgentError::InvalidInput(format!(
                        "invalid move direction '{other}'; expected 'up' or 'down'"
                    )));
                }
            }
            ids
        } else if !ordered_ids.is_empty() {
            let mut ids = Vec::with_capacity(current.len().max(ordered_ids.len()));
            for id in ordered_ids {
                if current.iter().any(|c| c.id == id) && !ids.contains(&id) {
                    ids.push(id);
                }
            }
            for c in &current {
                if !ids.contains(&c.id) {
                    ids.push(c.id.clone());
                }
            }
            ids
        } else {
            return Ok(current);
        };

        self.storage.reorder_conversations(&final_ids)?;
        self.storage.list_conversations(false)
    }

    pub fn fork_conversation(
        &self,
        conv_id: &str,
        up_to_message_id: Option<&str>,
        title_override: Option<String>,
    ) -> Result<Conversation> {
        let src = self
            .storage
            .get_conversation(conv_id)?
            .ok_or_else(|| AgentError::NotFound(format!("conversation '{conv_id}' not found")))?;

        let all_msgs = self.storage.list_chat_messages(conv_id, false, 10_000)?;
        let slice = if let Some(anchor_id) = up_to_message_id.map(str::trim).filter(|s| !s.is_empty())
        {
            let pos = all_msgs
                .iter()
                .position(|m| m.id == anchor_id)
                .ok_or_else(|| {
                    AgentError::NotFound(format!(
                        "message '{anchor_id}' not found in conversation '{conv_id}'"
                    ))
                })?;
            &all_msgs[..=pos]
        } else {
            &all_msgs[..]
        };

        let title = title_override
            .map(|t| t.trim().to_string())
            .filter(|t| !t.is_empty())
            .unwrap_or_else(|| format!("Fork of {}", src.title));

        let now = now_iso();
        let forked = Conversation {
            id: new_id(),
            title,
            mode: src.mode.clone(),
            created_at: now.clone(),
            updated_at: now,
            archived: false,
            sort_order: self.top_conversation_sort_order(),
        };
        self.storage.create_conversation(&forked)?;

        for m in slice {
            let copied = ChatMessage {
                id: new_id(),
                conversation_id: forked.id.clone(),
                role: m.role,
                agent_id: m.agent_id.clone(),
                content: m.content.clone(),
                reasoning: m.reasoning.clone(),
                tool_calls_json: m.tool_calls_json.clone(),
                task_id: None,
                token_count: m.token_count,
                latency_ms: m.latency_ms,
                archived: false,
                created_at: m.created_at.clone(),
                attachments: m.attachments.clone(),
            };
            self.storage.append_chat_message(&copied)?;
        }

        self.append_audit(
            "user",
            "conversation.fork",
            &forked.id,
            "info",
            "SUCCESS",
            serde_json::json!({
                "source_conversation_id": conv_id,
                "copied_messages": slice.len(),
                "up_to_message_id": up_to_message_id,
            }),
        );

        Ok(forked)
    }

    pub fn delete_conversation_message(&self, conv_id: &str, msg_id: &str) -> Result<bool> {
        let _conv = self
            .storage
            .get_conversation(conv_id)?
            .ok_or_else(|| AgentError::NotFound(format!("conversation '{conv_id}' not found")))?;
        let msg = self
            .storage
            .get_chat_message(msg_id)?
            .ok_or_else(|| AgentError::NotFound(format!("message '{msg_id}' not found")))?;
        if msg.conversation_id != conv_id {
            return Err(AgentError::NotFound(format!(
                "message '{msg_id}' not found in conversation '{conv_id}'"
            )));
        }

        let deleted = self.storage.delete_chat_message(msg_id)?;
        self.append_audit(
            "user",
            "message.delete",
            msg_id,
            "info",
            if deleted { "SUCCESS" } else { "NOT_FOUND" },
            serde_json::json!({ "conversation_id": conv_id }),
        );
        Ok(deleted)
    }

    async fn dispatch_retry_payload(
        self: &Arc<Self>,
        conv_id: &str,
        content: String,
        attachments: Vec<ChatAttachment>,
        thinking_override: Option<ThinkingLevel>,
        target_agent_id: Option<String>,
        async_dispatch: bool,
    ) -> Result<(ChatMessage, Option<TaskRecord>)> {
        if async_dispatch {
            let (user_msg, task) = self
                .dispatch_chat_background(
                    conv_id.to_string(),
                    content,
                    target_agent_id,
                    thinking_override,
                    attachments,
                    CancellationToken::new(),
                )
                .await?;
            return Ok((user_msg, Some(task)));
        }

        let fallback_content = content.clone();
        let fallback_attachments = attachments.clone();
        let outcome = self
            .handle_chat_message_with_options(
                conv_id,
                &content,
                target_agent_id.as_deref(),
                thinking_override,
                attachments,
                CancellationToken::new(),
            )
            .await?;
        let user_msg = self
            .storage
            .list_chat_messages(conv_id, false, 20)?
            .into_iter()
            .rev()
            .find(|m| m.role == ChatRole::User)
            .unwrap_or_else(|| ChatMessage {
                id: new_id(),
                conversation_id: conv_id.to_string(),
                role: ChatRole::User,
                agent_id: None,
                content: fallback_content.clone(),
                reasoning: None,
                tool_calls_json: None,
                task_id: None,
                token_count: estimate_tokens(&fallback_content),
                latency_ms: None,
                archived: false,
                created_at: now_iso(),
                attachments: fallback_attachments,
            });
        let task = self.task_manager.get_task(&outcome.task_id)?;
        Ok((user_msg, task))
    }

    pub async fn retry_user_message(
        self: &Arc<Self>,
        conv_id: &str,
        msg_id: &str,
        thinking_override: Option<ThinkingLevel>,
        target_agent_id: Option<String>,
        async_dispatch: bool,
    ) -> Result<(ChatMessage, Option<TaskRecord>)> {
        let _conv = self
            .storage
            .get_conversation(conv_id)?
            .ok_or_else(|| AgentError::NotFound(format!("conversation '{conv_id}' not found")))?;
        let msg = self
            .storage
            .get_chat_message(msg_id)?
            .ok_or_else(|| AgentError::NotFound(format!("message '{msg_id}' not found")))?;
        if msg.conversation_id != conv_id {
            return Err(AgentError::NotFound(format!(
                "message '{msg_id}' not found in conversation '{conv_id}'"
            )));
        }
        if msg.role != ChatRole::User {
            return Err(AgentError::InvalidInput(
                "retry requires a user message".into(),
            ));
        }

        self.storage
            .delete_chat_messages_after(conv_id, msg_id, true)?;
        self.dispatch_retry_payload(
            conv_id,
            msg.content,
            msg.attachments,
            thinking_override,
            target_agent_id,
            async_dispatch,
        )
        .await
    }

    pub async fn regenerate_assistant_message(
        self: &Arc<Self>,
        conv_id: &str,
        msg_id: &str,
        thinking_override: Option<ThinkingLevel>,
        async_dispatch: bool,
    ) -> Result<(ChatMessage, Option<TaskRecord>)> {
        let conv = self
            .storage
            .get_conversation(conv_id)?
            .ok_or_else(|| AgentError::NotFound(format!("conversation '{conv_id}' not found")))?;
        let msg = self
            .storage
            .get_chat_message(msg_id)?
            .ok_or_else(|| AgentError::NotFound(format!("message '{msg_id}' not found")))?;
        if msg.conversation_id != conv_id {
            return Err(AgentError::NotFound(format!(
                "message '{msg_id}' not found in conversation '{conv_id}'"
            )));
        }

        if msg.role == ChatRole::User {
            return self
                .retry_user_message(conv_id, msg_id, thinking_override, None, async_dispatch)
                .await;
        }
        if msg.role != ChatRole::Assistant {
            return Err(AgentError::InvalidInput(
                "regenerate requires an assistant or user message".into(),
            ));
        }

        let history = self.storage.list_chat_messages(conv_id, false, 10_000)?;
        let pos = history
            .iter()
            .position(|m| m.id == msg_id)
            .ok_or_else(|| {
                AgentError::NotFound(format!(
                    "message '{msg_id}' not found in conversation '{conv_id}'"
                ))
            })?;
        let user_msg = history[..pos]
            .iter()
            .rev()
            .find(|m| m.role == ChatRole::User)
            .cloned()
            .ok_or_else(|| {
                AgentError::NotFound(format!(
                    "no preceding user message found before '{msg_id}'"
                ))
            })?;

        let target_agent_id = match &conv.mode {
            ConversationMode::Direct { agent_id } => {
                msg.agent_id.clone().or_else(|| Some(agent_id.clone()))
            }
            ConversationMode::Group { .. } => {
                msg.agent_id.clone().filter(|id| id != "commander")
            }
        };

        self.storage
            .delete_chat_messages_after(conv_id, &user_msg.id, true)?;
        self.dispatch_retry_payload(
            conv_id,
            user_msg.content,
            user_msg.attachments,
            thinking_override,
            target_agent_id,
            async_dispatch,
        )
        .await
    }

    pub async fn continue_assistant_message(
        self: &Arc<Self>,
        conv_id: &str,
        msg_id: &str,
        thinking_override: Option<ThinkingLevel>,
    ) -> Result<(ChatMessage, TaskRecord)> {
        let _conv = self
            .storage
            .get_conversation(conv_id)?
            .ok_or_else(|| AgentError::NotFound(format!("conversation '{conv_id}' not found")))?;
        let existing_msg = self
            .storage
            .get_chat_message(msg_id)?
            .ok_or_else(|| AgentError::NotFound(format!("message '{msg_id}' not found")))?;
        if existing_msg.conversation_id != conv_id {
            return Err(AgentError::NotFound(format!(
                "message '{msg_id}' not found in conversation '{conv_id}'"
            )));
        }
        if existing_msg.role != ChatRole::Assistant {
            return Err(AgentError::InvalidInput(
                "continue requires an assistant message".into(),
            ));
        }

        let preferred_agent_id = existing_msg.agent_id.as_deref().unwrap_or("commander");
        let agent = match self.agent_manager.get_agent(preferred_agent_id)? {
            Some(a) => a,
            None => self
                .agent_manager
                .get_agent("commander")?
                .ok_or_else(|| AgentError::NotFound("commander agent not found".into()))?,
        };
        if !agent.enabled {
            return Err(AgentError::PermissionDenied(format!(
                "agent '{}' is disabled",
                agent.id
            )));
        }

        let root_task = self.task_manager.create_task(
            format!("Continue response ({})", existing_msg.id),
            agent.id.clone(),
            Some(conv_id.to_string()),
            None,
            agent.priority,
            TaskStatus::Pending,
        )?;
        let cancel = self.task_manager.register_token(&root_task.id, None);
        let expedite = self.task_manager.register_expedite(&root_task.id);

        let rt = Arc::clone(self);
        let bg_task_id = root_task.id.clone();
        let bg_conv_id = conv_id.to_string();
        let mut updated_msg = existing_msg.clone();
        let bg_agent = agent.clone();

        tokio::spawn(async move {
            let Ok(_permit) = rt
                .await_task_slot_or_expedite(&bg_task_id, false, expedite, &cancel)
                .await
            else {
                return;
            };

            let eff_thinking = thinking_override.unwrap_or(bg_agent.model_config.thinking_level);
            let start_note = format_thinking_start_note(eff_thinking);
            let _ = rt
                .task_manager
                .mark_running(&bg_task_id, Some(start_note.to_string()));
            rt.agent_manager
                .record_task_started(&bg_agent.id, &bg_task_id);

            let cfg = rt.config();
            let mem_ctx = rt
                .memory_provider
                .build_context_for_agent(&bg_agent, Some(&bg_conv_id), Some(&bg_task_id), "")
                .await
                .unwrap_or_default();
            let system_prompt = match rt.build_system_prompt(&bg_agent, &mem_ctx, true) {
                Ok(p) => p,
                Err(e) => {
                    rt.emit_chat_error_alert(&bg_conv_id, &e);
                    let _ = rt.task_manager.fail_task(&bg_task_id, e.to_string());
                    rt.agent_manager.record_task_finished(&bg_agent.id, false);
                    return;
                }
            };

            let mut messages = vec![InferenceMessage {
                role: "system".to_string(),
                content: system_prompt,
                tool_call_id: None,
                name: None,
                tool_calls: None,
                image_urls: Vec::new(),
            }];

            let mut included_existing = false;
            if let Ok(history) = rt.storage.list_chat_messages(
                &bg_conv_id,
                false,
                cfg.memory.keep_recent_messages.max(100),
            ) {
                for m in history {
                    let role = match m.role {
                        ChatRole::User => "user",
                        ChatRole::Assistant => "assistant",
                        ChatRole::System => "system",
                        ChatRole::Tool => "tool",
                    };
                    let is_target = m.id == updated_msg.id;
                    if m.role == ChatRole::System {
                        let trimmed = m.content.trim();
                        if !trimmed.is_empty() {
                            messages[0].content.push_str("\n\n");
                            messages[0].content.push_str(trimmed);
                        }
                    } else {
                        messages.push(InferenceMessage {
                            role: role.to_string(),
                            content: format_user_message_with_attachments(
                                &m.content,
                                &m.attachments,
                            ),
                            tool_call_id: None,
                            name: m.agent_id,
                            tool_calls: None,
                            image_urls: if m.role == ChatRole::User {
                                extract_image_data_urls(&m.attachments)
                            } else {
                                Vec::new()
                            },
                        });
                    }
                    if is_target {
                        included_existing = true;
                        break;
                    }
                }
            }
            if !included_existing {
                messages.push(InferenceMessage {
                    role: "assistant".to_string(),
                    content: updated_msg.content.clone(),
                    tool_call_id: None,
                    name: Some(bg_agent.id.clone()),
                    tool_calls: None,
                    image_urls: Vec::new(),
                });
            }
            messages.push(InferenceMessage {
                role: "user".to_string(),
                content: "Continue seamlessly from where you stopped without repeating anything already written.".to_string(),
                tool_call_id: None,
                name: None,
                tool_calls: None,
                image_urls: Vec::new(),
            });

            let call_fut = rt.run_inference_call(
                &bg_agent,
                Some(&bg_conv_id),
                &bg_task_id,
                messages,
                Vec::new(),
                thinking_override,
                cancel.clone(),
            );
            let inf_res = if bg_agent.max_task_duration_secs == 0 {
                Ok(call_fut.await)
            } else {
                let secs = bg_agent.max_task_duration_secs.max(5);
                tokio::time::timeout(Duration::from_secs(secs), call_fut).await
            };

            match inf_res {
                Ok(Ok(resp)) => {
                    if !updated_msg.content.is_empty()
                        && !resp.content.is_empty()
                        && !has_unclosed_code_fence(&updated_msg.content)
                        && updated_msg.content.ends_with(['.', '!', '?'])
                        && resp.content.starts_with(|c: char| c.is_ascii_uppercase())
                    {
                        updated_msg.content.push(' ');
                    }
                    updated_msg.content.push_str(&resp.content);
                    if let Some(new_r) = resp
                        .reasoning
                        .as_deref()
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                    {
                        match updated_msg.reasoning.as_mut() {
                            Some(prev) if !prev.trim().is_empty() => {
                                prev.push('\n');
                                prev.push_str(new_r);
                            }
                            _ => {
                                updated_msg.reasoning = Some(new_r.to_string());
                            }
                        }
                    }
                    updated_msg.token_count = estimate_tokens(&updated_msg.content);
                    updated_msg.task_id = Some(bg_task_id.clone());
                    updated_msg.latency_ms = Some(
                        updated_msg
                            .latency_ms
                            .unwrap_or(0)
                            .saturating_add(resp.latency_ms),
                    );
                    if let Err(e) = rt.storage.update_chat_message(&updated_msg) {
                        log_warn(
                            "runtime",
                            format!("failed updating continued message '{}': {e}", updated_msg.id),
                        );
                    }
                    rt.emit_chat_completed(&bg_conv_id, &bg_task_id, &bg_agent.id, &updated_msg);
                    let _ = rt
                        .task_manager
                        .complete_task(&bg_task_id, updated_msg.content.clone());
                    rt.agent_manager.record_task_finished(&bg_agent.id, true);
                    rt.spawn_kv_cache_prefill(&bg_conv_id, &bg_agent, 0);
                }
                Ok(Err(e)) => {
                    rt.agent_manager.record_task_finished(&bg_agent.id, false);
                    if matches!(e, AgentError::Cancelled(_)) {
                        let _ = rt.task_manager.cancel_task(&bg_task_id, &e.to_string());
                    } else {
                        rt.emit_chat_error_alert(&bg_conv_id, &e);
                        let _ = rt.task_manager.fail_task(&bg_task_id, e.to_string());
                    }
                }
                Err(_) => {
                    cancel.cancel();
                    let secs = bg_agent.max_task_duration_secs.max(5);
                    let detail = format!("task exceeded max_task_duration_secs ({secs}s)");
                    rt.emit_chat_error_message(&bg_conv_id, &detail);
                    let _ = rt.task_manager.fail_task(&bg_task_id, detail);
                    rt.agent_manager.record_task_finished(&bg_agent.id, false);
                }
            }
        });

        let latest_task = self
            .storage
            .get_task(&root_task.id)
            .ok()
            .flatten()
            .unwrap_or(root_task);
        Ok((existing_msg, latest_task))
    }

    fn spawn_kv_cache_prefill(
        self: &Arc<Self>,
        conversation_id: &str,
        agent: &AgentDefinition,
        hop_count: u32,
    ) {
        if hop_count != 0 || !self.config().inference.prefill_kv_cache {
            return;
        }
        let rt = Arc::clone(self);
        let conv_id = conversation_id.to_string();
        let agent = agent.clone();
        tokio::spawn(async move {
            let Ok(inf_sem) = rt.current_inference_semaphore() else {
                return;
            };
            let Ok(_permit) = inf_sem.try_acquire_owned() else {
                return;
            };
            let prefill_msgs = match rt.build_prefill_messages(&conv_id, &agent).await {
                Ok(m) if !m.is_empty() => m,
                _ => return,
            };
            if let Err(e) = rt
                .inference
                .prefill_kv_cache(
                    prefill_msgs,
                    agent.model_config.model_override.clone(),
                    CancellationToken::new(),
                )
                .await
            {
                log_warn(
                    "runtime",
                    format!("background kv prefill failed for '{conv_id}': {e}"),
                );
            }
        });
    }

    async fn build_prefill_messages(
        &self,
        conversation_id: &str,
        agent: &AgentDefinition,
    ) -> Result<Vec<InferenceMessage>> {
        let cfg = self.config();
        let mem_ctx = self
            .memory_provider
            .build_context_for_agent(agent, Some(conversation_id), None, "")
            .await
            .unwrap_or_default();
        let mut system_parts = vec![self.build_system_prompt(agent, &mem_ctx, true)?];
        let history = self.storage.list_chat_messages(
            conversation_id,
            false,
            cfg.memory.keep_recent_messages.max(100),
        )?;

        let mut rest = Vec::with_capacity(history.len());
        for m in history {
            let content = format_user_message_with_attachments(&m.content, &m.attachments);
            match m.role {
                ChatRole::System => {
                    let trimmed = content.trim();
                    if !trimmed.is_empty() {
                        system_parts.push(trimmed.to_string());
                    }
                }
                ChatRole::User => rest.push(InferenceMessage {
                    role: "user".to_string(),
                    content,
                    tool_call_id: None,
                    name: m.agent_id,
                    tool_calls: None,
                    image_urls: extract_image_data_urls(&m.attachments),
                }),
                ChatRole::Assistant => rest.push(InferenceMessage {
                    role: "assistant".to_string(),
                    content,
                    tool_call_id: None,
                    name: m.agent_id,
                    tool_calls: None,
                    image_urls: Vec::new(),
                }),
                ChatRole::Tool => rest.push(InferenceMessage {
                    role: "tool".to_string(),
                    content,
                    tool_call_id: None,
                    name: m.agent_id,
                    tool_calls: None,
                    image_urls: Vec::new(),
                }),
            }
        }

        let mut msgs = Vec::with_capacity(rest.len() + 1);
        msgs.push(InferenceMessage {
            role: "system".to_string(),
            content: system_parts.join("\n\n"),
            tool_call_id: None,
            name: None,
            tool_calls: None,
            image_urls: Vec::new(),
        });
        msgs.extend(rest);
        Ok(msgs)
    }

    fn resolve_dispatch_target(
        &self,
        mode: &ConversationMode,
        target_agent_id: Option<&str>,
        user_content: &str,
    ) -> Result<ResolvedDispatchTarget> {
        if let Some(tid) = target_agent_id.map(str::trim).filter(|s| !s.is_empty()) {
            let agent = self
                .agent_manager
                .get_agent(tid)?
                .ok_or_else(|| AgentError::NotFound(format!("agent '{tid}' not found")))?;
            if !agent.enabled {
                return Err(AgentError::PermissionDenied(format!(
                    "agent '{tid}' is disabled"
                )));
            }
            let effective_prompt = format_explicit_agent_prompt(&agent, user_content);
            return Ok(ResolvedDispatchTarget::Direct {
                agent,
                effective_prompt,
            });
        }

        let agents = self.agent_manager.list_agents()?;
        if let Some((agent, effective_prompt)) =
            resolve_agent_routing_for_mode(&agents, mode, user_content)
        {
            return Ok(ResolvedDispatchTarget::Direct {
                agent,
                effective_prompt,
            });
        }

        match mode {
            ConversationMode::Direct { agent_id } => {
                let agent = self
                    .agent_manager
                    .get_agent(agent_id)?
                    .ok_or_else(|| AgentError::NotFound(format!("agent '{agent_id}' not found")))?;
                if !agent.enabled {
                    return Err(AgentError::PermissionDenied(format!(
                        "agent '{agent_id}' is disabled"
                    )));
                }
                Ok(ResolvedDispatchTarget::Direct {
                    agent,
                    effective_prompt: user_content.to_string(),
                })
            }
            ConversationMode::Group {
                participant_agent_ids,
            } => {
                let commander = self
                    .agent_manager
                    .get_agent("commander")?
                    .ok_or_else(|| AgentError::NotFound("commander agent not found".into()))?;
                if !commander.enabled {
                    return Err(AgentError::PermissionDenied(
                        "commander agent is disabled".into(),
                    ));
                }
                Ok(ResolvedDispatchTarget::Group {
                    commander,
                    participant_ids: participant_agent_ids.clone(),
                })
            }
        }
    }

    pub async fn submit_task(
        self: &Arc<Self>,
        user_request: String,
        assigned_agent_id: String,
        conversation_id: Option<String>,
        priority: Option<i32>,
    ) -> Result<TaskRecord> {
        let agent = self
            .agent_manager
            .get_agent(&assigned_agent_id)?
            .ok_or_else(|| AgentError::NotFound(format!("agent '{assigned_agent_id}' not found")))?;
        if !agent.enabled {
            return Err(AgentError::PermissionDenied(format!(
                "agent '{assigned_agent_id}' is disabled"
            )));
        }

        let task = self.task_manager.create_task(
            user_request,
            assigned_agent_id,
            conversation_id,
            None,
            priority.unwrap_or(agent.priority),
            TaskStatus::Pending,
        )?;

        let rt = Arc::clone(self);
        let tid = task.id.clone();
        tokio::spawn(async move {
            if let Err(e) = rt.run_task_by_id(&tid).await {
                log_warn("runtime", format!("submitted task '{tid}' failed: {e}"));
            }
        });

        Ok(task)
    }

    pub async fn run_task_by_id(self: &Arc<Self>, task_id: &str) -> Result<TaskExecutionOutcome> {
        let task = self
            .task_manager
            .get_task(task_id)?
            .ok_or_else(|| AgentError::NotFound(format!("task '{task_id}' not found")))?;
        if task.status.is_terminal() {
            return Ok(TaskExecutionOutcome {
                task_id: task.id,
                status: task.status,
                result: task.result,
                pending_approval_id: None,
            });
        }
        if task.status == TaskStatus::Waiting {
            return Ok(TaskExecutionOutcome {
                task_id: task.id,
                status: TaskStatus::Waiting,
                result: None,
                pending_approval_id: None,
            });
        }

        let agent = self
            .agent_manager
            .get_agent(&task.assigned_agent_id)?
            .ok_or_else(|| {
                AgentError::NotFound(format!("agent '{}' not found", task.assigned_agent_id))
            })?;
        if !agent.enabled {
            let msg = format!("agent '{}' is disabled", agent.id);
            self.task_manager.fail_task(&task.id, msg.clone())?;
            return Err(AgentError::PermissionDenied(msg));
        }

        // Only root tasks acquire `task_semaphore`; child tasks invoked by an active parent
        // (e.g. via assign_agent_task) run under the parent's slot to avoid self-deadlock.
        let _permit = if task.parent_task_id.is_none() {
            let sem = self.current_task_semaphore()?;
            Some(
                sem.acquire_owned()
                    .await
                    .map_err(|e| AgentError::Internal(format!("task semaphore closed: {e}")))?,
            )
        } else {
            None
        };

        let cancel = self.task_manager.register_token(&task.id, None);
        let outcome = self
            .execute_task_with_timeout(
                ReactTurnInput {
                    task_id: task.id.clone(),
                    root_task_id: task.parent_task_id.clone().unwrap_or_else(|| task.id.clone()),
                    conversation_id: task.conversation_id.clone(),
                    agent,
                    user_request: task.user_request.clone(),
                    raw_user_request: task.user_request.clone(),
                    user_message_id: None,
                    thinking_level: None,
                    attachments: Vec::new(),
                    hop_count: 0,
                    persist_chat_message: task.conversation_id.is_some(),
                },
                cancel,
            )
            .await?;

        Ok(TaskExecutionOutcome {
            task_id: outcome.task_id,
            status: outcome.status,
            result: if outcome.status == TaskStatus::Completed {
                Some(outcome.content)
            } else {
                None
            },
            pending_approval_id: outcome.pending_approval_id,
        })
    }

    pub async fn resolve_approval(
        self: &Arc<Self>,
        approval_id: &str,
        approved: bool,
        resolved_by: &str,
    ) -> Result<ApprovalResolutionOutcome> {
        let _in_flight_guard = self.acquire_in_flight_approval(approval_id)?;

        let mut approval = self
            .storage
            .get_approval(approval_id)?
            .ok_or_else(|| AgentError::NotFound(format!("approval '{approval_id}' not found")))?;

        if approval.status != ApprovalStatus::Pending {
            return Err(AgentError::Conflict(format!(
                "approval '{approval_id}' is already resolved ({:?})",
                approval.status
            )));
        }

        approval.status = if approved {
            ApprovalStatus::Approved
        } else {
            ApprovalStatus::Denied
        };
        approval.resolved_at = Some(now_iso());
        approval.resolved_by = Some(resolved_by.to_string());
        self.storage.upsert_approval(&approval)?;

        let status_label = if approved { "APPROVED" } else { "DENIED" };
        let _ = self.events_tx.send(StreamEvent::ApprovalResolved {
            approval_id: approval_id.to_string(),
            status: status_label.to_string(),
        });
        self.append_audit(
            resolved_by,
            if approved {
                "approval.approve"
            } else {
                "approval.deny"
            },
            approval_id,
            "info",
            status_label,
            serde_json::json!({
                "task_id": approval.task_id,
                "agent_id": approval.agent_id,
                "operation": approval.operation,
            }),
        );

        let paused = self.pending_approvals.write().ok().and_then(|mut guard| {
            let item = guard.remove(approval_id);
            shrink_map_if_sparse(&mut guard);
            item
        });

        let Some(mut state) = paused else {
            // No in-memory paused continuation (e.g., after a process restart); update task state cleanly.
            if let Some(ref tid) = approval.task_id {
                if !approved {
                    let t = self
                        .task_manager
                        .fail_task(tid, format!("Approval denied by {resolved_by}"))?;
                    self.task_manager.release_token(tid);
                    self.evict_pending_for_task(tid);
                    self.cleanup_temp_agent_if_needed(&approval.agent_id).await;
                    return Ok(ApprovalResolutionOutcome {
                        approval_id: approval_id.to_string(),
                        approved: false,
                        task_id: Some(t.id),
                        task_status: Some(t.status),
                        result: None,
                        pending_approval_id: None,
                    });
                }
            }
            return Ok(ApprovalResolutionOutcome {
                approval_id: approval_id.to_string(),
                approved,
                task_id: approval.task_id,
                task_status: None,
                result: None,
                pending_approval_id: None,
            });
        };

        if !approved {
            let reason = format!(
                "Approval denied by {resolved_by} for tool '{}'",
                state.pending_tool_call.name
            );
            let failed = self.task_manager.fail_task(&state.task_id, reason.clone())?;
            self.task_manager.release_token(&state.task_id);
            self.agent_manager.record_task_finished(&state.agent_id, false);
            self.evict_pending_for_task(&state.task_id);
            self.cleanup_temp_agent_if_needed(&state.agent_id).await;
            return Ok(ApprovalResolutionOutcome {
                approval_id: approval_id.to_string(),
                approved: false,
                task_id: Some(failed.id),
                task_status: Some(failed.status),
                result: Some(reason),
                pending_approval_id: None,
            });
        }

        let agent = self
            .agent_manager
            .get_agent(&state.agent_id)?
            .ok_or_else(|| AgentError::NotFound(format!("agent '{}' not found", state.agent_id)))?;
        if !agent.enabled {
            let msg = format!("agent '{}' is disabled", agent.id);
            self.task_manager.fail_task(&state.task_id, msg.clone())?;
            self.task_manager.release_token(&state.task_id);
            self.evict_pending_for_task(&state.task_id);
            return Err(AgentError::PermissionDenied(msg));
        }

        let _permit = if state.task_id == state.root_task_id {
            let sem = self.current_task_semaphore()?;
            Some(
                sem.acquire_owned()
                    .await
                    .map_err(|e| AgentError::Internal(format!("task semaphore closed: {e}")))?,
            )
        } else {
            None
        };

        let cancel = self.task_manager.register_token(&state.task_id, None);
        if let Err(e) = self
            .task_manager
            .mark_running(&state.task_id, Some("Approval granted; resuming".into()))
        {
            self.task_manager.release_token(&state.task_id);
            return Err(e);
        }
        self.agent_manager
            .record_task_started(&agent.id, &state.task_id);

        let mut batch = Vec::with_capacity(1 + state.remaining_tool_calls.len());
        batch.push(state.pending_tool_call.clone());
        batch.extend(state.remaining_tool_calls.clone());

        let turn_res = self
            .resume_from_tool_batch(
                &agent,
                &state.task_id,
                &state.root_task_id,
                state.conversation_id.as_deref(),
                &mut state.messages,
                batch,
                ReactLoopOptions {
                    start_iter: state.next_iteration,
                    max_iters: resolve_max_iterations(
                        agent.max_reasoning_iterations,
                        self.config().agents.max_reasoning_iterations,
                    ),
                    thinking_level: state.thinking_level,
                    raw_user_request: state.raw_user_request,
                    auto_continuations: state.auto_continuations,
                    continuation_parts: state.continuation_parts,
                    did_write_file: state.did_write_file,
                    written_paths: state.written_paths,
                },
                state.hop_count,
                state.persist_chat_message,
                true,
                cancel,
            )
            .await;

        match turn_res {
            Ok(res) => {
                if res.status != TaskStatus::Waiting {
                    self.task_manager.release_token(&state.task_id);
                    self.agent_manager
                        .record_task_finished(&agent.id, res.status == TaskStatus::Completed);
                    self.evict_pending_for_task(&state.task_id);
                    if agent.is_temporary {
                        let _ = self.agent_manager.cleanup_temporary_agent(&agent.id).await;
                        self.evict_pending_for_agent(&agent.id);
                    }
                }
                Ok(ApprovalResolutionOutcome {
                    approval_id: approval_id.to_string(),
                    approved: true,
                    task_id: Some(res.task_id),
                    task_status: Some(res.status),
                    result: if res.status == TaskStatus::Completed {
                        Some(res.content)
                    } else {
                        None
                    },
                    pending_approval_id: res.pending_approval_id,
                })
            }
            Err(e) => {
                self.task_manager.release_token(&state.task_id);
                self.agent_manager.record_task_finished(&agent.id, false);
                if matches!(e, AgentError::Cancelled(_)) {
                    let _ = self.task_manager.cancel_task(&state.task_id, &e.to_string());
                } else {
                    let _ = self.task_manager.fail_task(&state.task_id, e.to_string());
                }
                self.evict_pending_for_task(&state.task_id);
                if agent.is_temporary {
                    let _ = self.agent_manager.cleanup_temporary_agent(&agent.id).await;
                    self.evict_pending_for_agent(&agent.id);
                }
                Err(e)
            }
        }
    }

    async fn cleanup_temp_agent_if_needed(&self, agent_id: &str) {
        if let Ok(Some(agent)) = self.agent_manager.get_agent(agent_id) {
            if agent.is_temporary {
                let _ = self.agent_manager.cleanup_temporary_agent(agent_id).await;
                self.evict_pending_for_agent(agent_id);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_direct_agent_chat_with_options(
        self: &Arc<Self>,
        conversation_id: &str,
        agent: AgentDefinition,
        effective_prompt: &str,
        raw_user_content: &str,
        user_message_id: Option<String>,
        thinking_level: Option<ThinkingLevel>,
        attachments: Vec<ChatAttachment>,
        cancel: CancellationToken,
    ) -> Result<ChatTurnResult> {
        let task_sem = self.current_task_semaphore()?;
        let _permit = task_sem
            .acquire_owned()
            .await
            .map_err(|e| AgentError::Internal(format!("task semaphore closed: {e}")))?;

        let task = self.task_manager.create_task(
            raw_user_content.to_string(),
            agent.id.clone(),
            Some(conversation_id.to_string()),
            None,
            agent.priority,
            TaskStatus::Pending,
        )?;
        let task_cancel = self.task_manager.register_token(&task.id, Some(&cancel));

        self.execute_task_with_timeout(
            ReactTurnInput {
                task_id: task.id.clone(),
                root_task_id: task.id.clone(),
                conversation_id: Some(conversation_id.to_string()),
                agent,
                user_request: effective_prompt.to_string(),
                raw_user_request: raw_user_content.to_string(),
                user_message_id,
                thinking_level,
                attachments,
                hop_count: 0,
                persist_chat_message: true,
            },
            task_cancel,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_group_chat_with_options(
        self: &Arc<Self>,
        conversation_id: &str,
        commander: AgentDefinition,
        participant_ids: &[String],
        user_content: &str,
        user_message_id: Option<String>,
        thinking_level: Option<ThinkingLevel>,
        attachments: Vec<ChatAttachment>,
        cancel: CancellationToken,
    ) -> Result<ChatTurnResult> {
        let candidates = self.collect_group_candidates(participant_ids)?;
        if candidates.is_empty() {
            return self
                .run_direct_agent_chat_with_options(
                    conversation_id,
                    commander,
                    user_content,
                    user_content,
                    user_message_id,
                    thinking_level,
                    attachments,
                    cancel,
                )
                .await;
        }

        let task_sem = self.current_task_semaphore()?;
        let _permit = task_sem
            .acquire_owned()
            .await
            .map_err(|e| AgentError::Internal(format!("task semaphore closed: {e}")))?;

        let parent_task = self.task_manager.create_task(
            user_content.to_string(),
            commander.id.clone(),
            Some(conversation_id.to_string()),
            None,
            commander.priority,
            TaskStatus::Running,
        )?;
        let task_cancel = self
            .task_manager
            .register_token(&parent_task.id, Some(&cancel));

        self.run_group_chat_with_task(
            conversation_id,
            commander,
            participant_ids,
            user_content,
            user_message_id,
            thinking_level,
            attachments,
            parent_task.id,
            task_cancel,
        )
        .await
    }

    fn collect_group_candidates(&self, participant_ids: &[String]) -> Result<Vec<AgentDefinition>> {
        let mut candidates = Vec::new();
        for pid in participant_ids {
            if pid == "commander" {
                continue;
            }
            if let Some(def) = self.agent_manager.get_agent(pid)? {
                if def.enabled && !candidates.iter().any(|c: &AgentDefinition| c.id == def.id) {
                    candidates.push(def);
                }
            }
        }
        Ok(candidates)
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_group_chat_with_task(
        self: &Arc<Self>,
        conversation_id: &str,
        commander: AgentDefinition,
        participant_ids: &[String],
        user_content: &str,
        user_message_id: Option<String>,
        thinking_level: Option<ThinkingLevel>,
        attachments: Vec<ChatAttachment>,
        parent_task_id: String,
        task_cancel: CancellationToken,
    ) -> Result<ChatTurnResult> {
        let candidates = self.collect_group_candidates(participant_ids)?;
        if candidates.is_empty() {
            return self
                .execute_task_with_timeout(
                    ReactTurnInput {
                        task_id: parent_task_id.clone(),
                        root_task_id: parent_task_id,
                        conversation_id: Some(conversation_id.to_string()),
                        agent: commander,
                        user_request: user_content.to_string(),
                        raw_user_request: user_content.to_string(),
                        user_message_id,
                        thinking_level,
                        attachments,
                        hop_count: 0,
                        persist_chat_message: true,
                    },
                    task_cancel,
                )
                .await;
        }

        self.task_manager.mark_running(&parent_task_id, None)?;
        self.agent_manager
            .record_task_started(&commander.id, &parent_task_id);

        let selected_ids = match self
            .coordinate_group_participants(
                &commander,
                &candidates,
                user_content,
                &attachments,
                &task_cancel,
            )
            .await
        {
            Ok(ids) => ids,
            Err(e) => {
                if matches!(e, AgentError::Cancelled(_)) {
                    let _ = self
                        .task_manager
                        .cancel_task(&parent_task_id, &e.to_string());
                } else {
                    let _ = self.task_manager.fail_task(&parent_task_id, e.to_string());
                }
                self.agent_manager.record_task_finished(&commander.id, false);
                return Err(e);
            }
        };

        if selected_ids.is_empty() {
            // Release coordinator active mark before handing the same task to Commander ReAct.
            self.agent_manager.record_task_finished(&commander.id, true);
            // Fall through to Commander's streaming ReAct loop so thinking/markdown
            // streaming works identically to Direct Chat.
            return self
                .execute_task_with_timeout(
                    ReactTurnInput {
                        task_id: parent_task_id.clone(),
                        root_task_id: parent_task_id,
                        conversation_id: Some(conversation_id.to_string()),
                        agent: commander,
                        user_request: user_content.to_string(),
                        raw_user_request: user_content.to_string(),
                        user_message_id,
                        thinking_level,
                        attachments,
                        hop_count: 0,
                        persist_chat_message: true,
                    },
                    task_cancel,
                )
                .await;
        }

        if selected_ids.len() == 1 {
            if let Some(specialist) = candidates
                .iter()
                .find(|c| c.id == selected_ids[0])
                .cloned()
            {
                // Single-specialist fast path: avoid A2A delegation + Commander synthesis overhead.
                self.agent_manager.record_task_finished(&commander.id, true);
                let specialist_prompt = format_direct_specialist_prompt(&specialist, user_content);
                let mut res = self
                    .execute_task_with_timeout(
                        ReactTurnInput {
                            task_id: parent_task_id.clone(),
                            root_task_id: parent_task_id,
                            conversation_id: Some(conversation_id.to_string()),
                            agent: specialist,
                            user_request: specialist_prompt,
                            raw_user_request: user_content.to_string(),
                            user_message_id,
                            thinking_level,
                            attachments,
                            hop_count: 0,
                            persist_chat_message: true,
                        },
                        task_cancel,
                    )
                    .await?;
                res.selected_participants = selected_ids;
                return Ok(res);
            }
        }

        if task_cancel.is_cancelled() {
            self.task_manager
                .cancel_task(&parent_task_id, "group chat cancelled")?;
            self.agent_manager.record_task_finished(&commander.id, false);
            return Err(AgentError::Cancelled("group chat cancelled".into()));
        }

        let mut spawn_queue = Vec::with_capacity(selected_ids.len());
        for (idx, sid) in selected_ids.iter().enumerate() {
            let Some(specialist) = candidates.iter().find(|c| c.id == *sid).cloned() else {
                continue;
            };

            let _ = self.message_bus.send(SendMessageRequest {
                sender_agent_id: commander.id.clone(),
                recipient_agent_id: specialist.id.clone(),
                task_id: Some(parent_task_id.clone()),
                message_type: AgentMessageType::TaskAssignment,
                content: user_content.to_string(),
                hop_count: 1,
            })?;

            let child_task = self.task_manager.create_task(
                user_content.to_string(),
                specialist.id.clone(),
                Some(conversation_id.to_string()),
                Some(parent_task_id.clone()),
                specialist.priority,
                TaskStatus::Pending,
            )?;
            let child_cancel = self
                .task_manager
                .register_token(&child_task.id, Some(&task_cancel));
            spawn_queue.push((idx, specialist, child_task.id, child_cancel));
        }

        // Execute all selected specialists concurrently so multiple streams progress in parallel.
        let mut join_set = JoinSet::new();
        for (idx, specialist, child_task_id, child_cancel) in spawn_queue {
            let rt = Arc::clone(self);
            let conv_id = conversation_id.to_string();
            let root_id = parent_task_id.clone();
            let effective_req = format_direct_specialist_prompt(&specialist, user_content);
            let raw_req = user_content.to_string();
            let msg_id = user_message_id.clone();
            let atts = attachments.clone();
            join_set.spawn(async move {
                let res = rt
                    .execute_task_with_timeout(
                        ReactTurnInput {
                            task_id: child_task_id.clone(),
                            root_task_id: root_id,
                            conversation_id: Some(conv_id),
                            agent: specialist.clone(),
                            user_request: effective_req,
                            raw_user_request: raw_req,
                            user_message_id: msg_id,
                            thinking_level,
                            attachments: atts,
                            hop_count: 1,
                            persist_chat_message: true,
                        },
                        child_cancel,
                    )
                    .await;
                (idx, specialist, res)
            });
        }

        let mut results = Vec::new();
        while let Some(joined) = join_set.join_next().await {
            match joined {
                Ok(item) => results.push(item),
                Err(e) => {
                    log_warn("runtime", format!("group specialist task panicked: {e}"));
                }
            }
        }
        results.sort_by_key(|(idx, _, _)| *idx);

        let mut contributions = Vec::new();
        let mut waiting_result: Option<ChatTurnResult> = None;

        for (_idx, specialist, child_res) in results {
            match child_res {
                Ok(res) => {
                    if res.status == TaskStatus::Waiting {
                        if waiting_result.is_none() {
                            waiting_result = Some(res);
                        }
                        continue;
                    }
                    let _ = self.message_bus.send(SendMessageRequest {
                        sender_agent_id: specialist.id.clone(),
                        recipient_agent_id: commander.id.clone(),
                        task_id: Some(parent_task_id.clone()),
                        message_type: AgentMessageType::TaskCompleted,
                        content: res.content.clone(),
                        hop_count: 1,
                    });
                    contributions.push(format!("[{}]: {}", specialist.name, res.content));
                }
                Err(e) => {
                    let _ = self.message_bus.send(SendMessageRequest {
                        sender_agent_id: specialist.id.clone(),
                        recipient_agent_id: commander.id.clone(),
                        task_id: Some(parent_task_id.clone()),
                        message_type: AgentMessageType::ErrorReport,
                        content: e.to_string(),
                        hop_count: 1,
                    });
                    contributions.push(format!("[{} error]: {e}", specialist.name));
                }
            }
        }

        if let Some(res) = waiting_result {
            let _ = self.task_manager.mark_side_effects(&parent_task_id);
            let _ = self
                .task_manager
                .mark_waiting(&parent_task_id, "Waiting for specialist approval");
            self.task_manager.release_token(&parent_task_id);
            return Ok(ChatTurnResult {
                conversation_id: conversation_id.to_string(),
                task_id: res.task_id,
                agent_id: res.agent_id,
                status: TaskStatus::Waiting,
                message_id: None,
                content: res.content,
                pending_approval_id: res.pending_approval_id,
                selected_participants: selected_ids,
            });
        }

        // Commander synthesizes all selected specialist responses concisely
        let synthesis_prompt = format!(
            "User request:\n{user_content}\n\nSpecialist responses:\n{}\n\n\
             Provide a clear, direct final answer in 1st person as Commander.\n\
             Rules:\n\
             - NEVER output boilerplate headings like '# Unified Response', 'Status: No agent has a meaningful response', or 'outside their domain'.\n\
             - If a specialist already answered completely or saved a workspace file, keep your response brief (1-2 sentences confirming the result or filename) and NEVER repeat full code blocks.",
            contributions.join("\n\n")
        );
        let synth_msgs = vec![
            InferenceMessage {
                role: "system".to_string(),
                content: commander.system_prompt.clone(),
                tool_call_id: None,
                name: None,
                tool_calls: None,
                image_urls: Vec::new(),
            },
            InferenceMessage {
                role: "user".to_string(),
                content: synthesis_prompt,
                tool_call_id: None,
                name: None,
                tool_calls: None,
                image_urls: Vec::new(),
            },
        ];

        let synth_resp = self
            .run_inference_call(
                &commander,
                Some(conversation_id),
                &parent_task_id,
                synth_msgs,
                Vec::new(),
                thinking_level,
                task_cancel,
            )
            .await;

        match synth_resp {
            Ok(resp) => {
                let msg = self.persist_assistant_message(
                    conversation_id,
                    &commander.id,
                    &parent_task_id,
                    &resp.content,
                    resp.reasoning.as_deref(),
                    None,
                    resp.latency_ms,
                )?;
                self.emit_chat_completed(conversation_id, &parent_task_id, &commander.id, &msg);
                self.task_manager
                    .complete_task(&parent_task_id, resp.content.clone())?;
                self.agent_manager.record_task_finished(&commander.id, true);
                self.spawn_kv_cache_prefill(conversation_id, &commander, 0);
                Ok(ChatTurnResult {
                    conversation_id: conversation_id.to_string(),
                    task_id: parent_task_id,
                    agent_id: commander.id,
                    status: TaskStatus::Completed,
                    message_id: Some(msg.id),
                    content: resp.content,
                    pending_approval_id: None,
                    selected_participants: selected_ids,
                })
            }
            Err(e) => {
                if matches!(e, AgentError::Cancelled(_)) {
                    let _ = self
                        .task_manager
                        .cancel_task(&parent_task_id, &e.to_string());
                } else {
                    let _ = self.task_manager.fail_task(&parent_task_id, e.to_string());
                }
                self.agent_manager.record_task_finished(&commander.id, false);
                Err(e)
            }
        }
    }

    async fn coordinate_group_participants(
        &self,
        commander: &AgentDefinition,
        candidates: &[AgentDefinition],
        user_content: &str,
        attachments: &[ChatAttachment],
        cancel: &CancellationToken,
    ) -> Result<Vec<String>> {
        if attachments.is_empty() && is_casual_greeting_or_ping(user_content) {
            return Ok(Vec::new());
        }

        let mut participant_desc = String::new();
        for c in candidates {
            participant_desc.push_str(&format!(
                "- id: `{}` | name: {} | role: {} | description: {} | tools: [{}]\n",
                c.id,
                c.name,
                c.role_label,
                c.description,
                c.enabled_tools.join(", ")
            ));
        }

        let coordinator_sys = format!(
            "You are routing a group chat turn. Default to {{\"selected_agents\": []}} so Commander handles the message directly.\n\
             Select participant agent IDs ONLY when their specialized tools or domain are strictly required. Do NOT select irrelevant agents for greetings, small talk, clarification, or general questions.\n\
             Available participants:\n{participant_desc}\n\
             Reply ONLY with valid JSON: {{\"selected_agents\": []}} or {{\"selected_agents\": [\"agent_id\"]}}."
        );

        let req = InferenceRequest {
            model: commander.model_config.model_override.clone(),
            api_mode: commander.model_config.api_mode,
            messages: vec![
                InferenceMessage {
                    role: "system".into(),
                    content: coordinator_sys,
                    tool_call_id: None,
                    name: None,
                    tool_calls: None,
                    image_urls: Vec::new(),
                },
                InferenceMessage {
                    role: "user".into(),
                    content: format_user_message_with_attachments(user_content, attachments),
                    tool_call_id: None,
                    name: None,
                    tool_calls: None,
                    image_urls: extract_image_data_urls(attachments),
                },
            ],
            tools: Vec::new(),
            temperature: 0.0,
            max_output_tokens: 128,
            context_limit: commander.model_config.context_limit,
            stream: false,
            thinking_level: ThinkingLevel::Off,
        };

        let inf_sem = self.current_inference_semaphore()?;
        let _inf_permit = inf_sem
            .acquire_owned()
            .await
            .map_err(|e| AgentError::Internal(format!("inference semaphore closed: {e}")))?;
        let resp = self.inference.generate(req, cancel.clone()).await?;
        drop(_inf_permit);

        let without_think = extract_think_blocks_text(&resp.content);
        let cleaned = strip_json_fences(&without_think);
        let json_slice = extract_first_json_object(cleaned).unwrap_or(cleaned);
        if let Ok(parsed) = serde_json::from_str::<GroupCoordinatorDecision>(json_slice) {
            let mut valid = Vec::new();
            for sid in parsed.selected_agents {
                if candidates.iter().any(|c| c.id == sid) && !valid.contains(&sid) {
                    valid.push(sid);
                }
            }
            return Ok(valid);
        }

        // Fall back to Commander directly on empty/malformed routing JSON.
        Ok(Vec::new())
    }

    async fn execute_task_with_timeout(
        self: &Arc<Self>,
        input: ReactTurnInput,
        cancel: CancellationToken,
    ) -> Result<ChatTurnResult> {
        let raw_timeout_secs = input.agent.max_task_duration_secs;
        let task_id = input.task_id.clone();
        let agent_id = input.agent.id.clone();
        let is_temporary = input.agent.is_temporary;

        self.task_manager.mark_running(&task_id, None)?;
        self.agent_manager.record_task_started(&agent_id, &task_id);

        let fut = self.execute_react_loop(input, cancel.clone());
        let outcome = if raw_timeout_secs == 0 {
            Ok(fut.await)
        } else {
            let timeout_secs = raw_timeout_secs.max(5);
            tokio::time::timeout(Duration::from_secs(timeout_secs), fut).await
        };

        match outcome {
            Ok(res) => {
                if let Err(ref e) = res {
                    if !matches!(e, AgentError::ApprovalRequired { .. }) {
                        self.agent_manager.record_task_finished(&agent_id, false);
                        if matches!(e, AgentError::Cancelled(_)) || cancel.is_cancelled() {
                            let _ = self.task_manager.cancel_task(&task_id, &e.to_string());
                        } else {
                            let _ = self.task_manager.fail_task(&task_id, e.to_string());
                        }
                        self.evict_pending_for_task(&task_id);
                        if is_temporary {
                            let _ = self.agent_manager.cleanup_temporary_agent(&agent_id).await;
                            self.evict_pending_for_agent(&agent_id);
                        }
                    }
                } else if let Ok(ref ok_res) = res {
                    if ok_res.status != TaskStatus::Waiting {
                        self.agent_manager.record_task_finished(&agent_id, true);
                        self.evict_pending_for_task(&task_id);
                        if is_temporary {
                            let _ = self.agent_manager.cleanup_temporary_agent(&agent_id).await;
                            self.evict_pending_for_agent(&agent_id);
                        }
                    }
                }
                res
            }
            Err(_) => {
                let effective_timeout = raw_timeout_secs.max(5);
                cancel.cancel();
                let msg = format!("task exceeded max_task_duration_secs ({effective_timeout}s)");
                let _ = self.task_manager.fail_task(&task_id, msg.clone());
                self.agent_manager.record_task_finished(&agent_id, false);
                self.evict_pending_for_task(&task_id);
                if is_temporary {
                    let _ = self.agent_manager.cleanup_temporary_agent(&agent_id).await;
                    self.evict_pending_for_agent(&agent_id);
                }
                Err(AgentError::Timeout(msg))
            }
        }
    }

    async fn execute_react_loop(
        self: &Arc<Self>,
        input: ReactTurnInput,
        cancel: CancellationToken,
    ) -> Result<ChatTurnResult> {
        let cfg = self.config();
        let max_iters = resolve_max_iterations(
            input.agent.max_reasoning_iterations,
            cfg.agents.max_reasoning_iterations,
        );

        let mem_ctx = self
            .memory_provider
            .build_context_for_agent(
                &input.agent,
                input.conversation_id.as_deref(),
                Some(&input.task_id),
                &input.user_request,
            )
            .await
            .unwrap_or_default();

        let is_main_or_direct = input.hop_count == 0
            || input.agent.id == "commander"
            || input.agent.role == AgentRole::Commander;
        let system_prompt = self.build_system_prompt(&input.agent, &mem_ctx, is_main_or_direct)?;
        let mut messages = vec![InferenceMessage {
            role: "system".to_string(),
            content: system_prompt,
            tool_call_id: None,
            name: None,
            tool_calls: None,
            image_urls: Vec::new(),
        }];

        let current_text =
            format_user_message_with_attachments(&input.user_request, &input.attachments);
        let current_images = extract_image_data_urls(&input.attachments);

        if let Some(ref conv_id) = input.conversation_id {
            let history = self.storage.list_chat_messages(
                conv_id,
                false,
                cfg.memory.keep_recent_messages.max(4),
            )?;
            let mut appended_current = false;
            for (idx, m) in history.iter().enumerate() {
                let role = match m.role {
                    ChatRole::User => "user",
                    ChatRole::Assistant => "assistant",
                    ChatRole::System => "system",
                    ChatRole::Tool => "tool",
                };
                let is_current_user = m.role == ChatRole::User
                    && (input.user_message_id.as_deref() == Some(m.id.as_str())
                        || (input.user_message_id.is_none()
                            && idx + 1 == history.len()
                            && m.content == input.raw_user_request));
                if is_current_user {
                    appended_current = true;
                    messages.push(InferenceMessage {
                        role: "user".to_string(),
                        content: current_text.clone(),
                        tool_call_id: None,
                        name: m.agent_id.clone(),
                        tool_calls: None,
                        image_urls: current_images.clone(),
                    });
                } else {
                    messages.push(InferenceMessage {
                        role: role.to_string(),
                        content: format_user_message_with_attachments(&m.content, &m.attachments),
                        tool_call_id: None,
                        name: m.agent_id.clone(),
                        tool_calls: None,
                        image_urls: Vec::new(),
                    });
                }
            }
            if !appended_current {
                messages.push(InferenceMessage {
                    role: "user".to_string(),
                    content: current_text,
                    tool_call_id: None,
                    name: None,
                    tool_calls: None,
                    image_urls: current_images,
                });
            }
        } else {
            messages.push(InferenceMessage {
                role: "user".to_string(),
                content: current_text,
                tool_call_id: None,
                name: None,
                tool_calls: None,
                image_urls: current_images,
            });
        }

        self.continue_react_loop(
            &input.agent,
            &input.task_id,
            &input.root_task_id,
            input.conversation_id.as_deref(),
            &mut messages,
            ReactLoopOptions {
                start_iter: 0,
                max_iters,
                thinking_level: input.thinking_level,
                raw_user_request: input.raw_user_request,
                auto_continuations: 0,
                continuation_parts: Vec::new(),
                did_write_file: false,
                written_paths: Vec::new(),
            },
            input.hop_count,
            input.persist_chat_message,
            cancel,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn resume_from_tool_batch(
        self: &Arc<Self>,
        agent: &AgentDefinition,
        task_id: &str,
        root_task_id: &str,
        conversation_id: Option<&str>,
        messages: &mut Vec<InferenceMessage>,
        tool_calls: Vec<ToolCallRequest>,
        mut opts: ReactLoopOptions,
        hop_count: u32,
        persist_chat_message: bool,
        first_call_approved: bool,
        cancel: CancellationToken,
    ) -> Result<ChatTurnResult> {
        if let Some(paused_res) = self
            .execute_tool_batch(
                agent,
                task_id,
                root_task_id,
                conversation_id,
                messages,
                tool_calls,
                &mut opts,
                hop_count,
                persist_chat_message,
                first_call_approved,
                cancel.clone(),
            )
            .await?
        {
            return Ok(paused_res);
        }

        let cfg = self.config();
        opts.max_iters = resolve_max_iterations(
            agent.max_reasoning_iterations,
            cfg.agents.max_reasoning_iterations,
        );

        self.continue_react_loop(
            agent,
            task_id,
            root_task_id,
            conversation_id,
            messages,
            opts,
            hop_count,
            persist_chat_message,
            cancel,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    fn pause_script_write_batch(
        &self,
        approval_id: String,
        reason: String,
        agent: &AgentDefinition,
        task_id: &str,
        root_task_id: &str,
        conversation_id: Option<&str>,
        messages: &mut Vec<InferenceMessage>,
        final_content: &str,
        replacements: &[(usize, usize, String)],
        batch_calls: Vec<ToolCallRequest>,
        opts: &mut ReactLoopOptions,
        next_iter: u32,
        hop_count: u32,
        persist_chat_message: bool,
    ) -> Result<ChatTurnResult> {
        let mut pause_content = final_content.to_string();
        for (s, e, rep) in replacements.iter().rev() {
            pause_content.replace_range(*s..*e, rep);
        }
        messages.push(InferenceMessage {
            role: "assistant".to_string(),
            content: pause_content,
            tool_call_id: None,
            name: Some(agent.id.clone()),
            tool_calls: Some(batch_calls.clone()),
            image_urls: Vec::new(),
        });
        opts.start_iter = next_iter;
        opts.continuation_parts.clear();
        let _ = self.task_manager.mark_side_effects(task_id);
        if root_task_id != task_id {
            let _ = self.task_manager.mark_side_effects(root_task_id);
        }
        let first_call = batch_calls
            .first()
            .cloned()
            .ok_or_else(|| AgentError::Internal("empty script transport batch".into()))?;
        let remaining = batch_calls[1..].to_vec();
        self.pause_for_approval(
            approval_id.clone(),
            &reason,
            agent,
            task_id,
            root_task_id,
            conversation_id,
            messages.clone(),
            first_call,
            remaining,
            opts,
            opts.did_write_file,
            hop_count,
            persist_chat_message,
        )?;
        Ok(ChatTurnResult {
            conversation_id: conversation_id.unwrap_or_default().to_string(),
            task_id: task_id.to_string(),
            agent_id: agent.id.clone(),
            status: TaskStatus::Waiting,
            message_id: None,
            content: reason,
            pending_approval_id: Some(approval_id),
            selected_participants: Vec::new(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn continue_react_loop(
        self: &Arc<Self>,
        agent: &AgentDefinition,
        task_id: &str,
        root_task_id: &str,
        conversation_id: Option<&str>,
        messages: &mut Vec<InferenceMessage>,
        mut opts: ReactLoopOptions,
        hop_count: u32,
        persist_chat_message: bool,
        cancel: CancellationToken,
    ) -> Result<ChatTurnResult> {
        let allowed_tools = self.allowed_tool_schemas(agent).await;
        let effective_cap = opts.max_iters.unwrap_or(UNLIMITED_REASONING_SAFETY_CAP);

        for iter in opts.start_iter..effective_cap {
            if cancel.is_cancelled() {
                return Err(AgentError::Cancelled(format!("task '{task_id}' cancelled")));
            }
            let eff_thinking = opts
                .thinking_level
                .unwrap_or(agent.model_config.thinking_level);
            let (pct, note) = if iter == 0 {
                let msg = format_thinking_start_note(eff_thinking).to_string();
                let p = match opts.max_iters {
                    Some(limit) => (((iter + 1) as f32 / (limit as f32 + 1.0)) * 90.0) as u8,
                    None => 10,
                };
                (p, msg)
            } else {
                match opts.max_iters {
                    Some(limit) => (
                        (((iter + 1) as f32 / (limit as f32 + 1.0)) * 90.0) as u8,
                        format!("Tool call step {iter}/{limit}"),
                    ),
                    None => (
                        ((iter + 1).min(90)) as u8,
                        format!("Tool call step {iter}"),
                    ),
                }
            };
            let _ = self.task_manager.update_progress(task_id, pct, Some(note));

            let resp = self
                .run_inference_call(
                    agent,
                    conversation_id,
                    task_id,
                    messages.clone(),
                    allowed_tools.clone(),
                    opts.thinking_level,
                    cancel.clone(),
                )
                .await?;

            if resp.tool_calls.is_empty() {
                let cumulative_preview =
                    join_continuation_parts(&opts.continuation_parts, &resp.content);
                let hit_length = resp.finish_reason.as_deref() == Some("length");
                let has_unclosed = has_unclosed_code_fence(&cumulative_preview);
                if (hit_length || has_unclosed) && opts.auto_continuations < MAX_AUTO_CONTINUATIONS
                {
                    opts.auto_continuations += 1;
                    if !resp.content.is_empty() {
                        opts.continuation_parts.push(resp.content.clone());
                    }
                    messages.push(InferenceMessage {
                        role: "assistant".to_string(),
                        content: resp.content,
                        tool_call_id: None,
                        name: Some(agent.id.clone()),
                        tool_calls: None,
                        image_urls: Vec::new(),
                    });
                    messages.push(InferenceMessage {
                        role: "user".to_string(),
                        content: "Continue seamlessly from where you stopped without repeating anything. If producing a file/app/game, finish writing the complete working file using write_file.".to_string(),
                        tool_call_id: None,
                        name: None,
                        tool_calls: None,
                        image_urls: Vec::new(),
                    });
                    let _ = self.events_tx.send(StreamEvent::ContinuationTriggered {
                        conversation_id: conversation_id.unwrap_or_default().to_string(),
                        task_id: task_id.to_string(),
                        agent_id: agent.id.clone(),
                        reason: if hit_length {
                            "length".to_string()
                        } else {
                            "unclosed_code_fence".to_string()
                        },
                    });
                    continue;
                }

                let mut final_content = cumulative_preview;

                if !opts.did_write_file {
                    let blocks = extract_all_fenced_script_blocks(&final_content, 10);
                    if !blocks.is_empty() {
                        if let Some(write_spec) = self.resolve_enabled_write_tool(agent).await {
                            let write_tool_id = write_spec.id.clone();
                            let cfg = self.config();
                            let sandbox_mode = resolve_effective_sandbox_mode(agent, &cfg);
                            let approval_policy = resolve_effective_approval_policy(&cfg);
                            let multiple_blocks = blocks.len() > 1;
                            let mut used_names = opts.written_paths.clone();
                            let mut prepared_calls: Vec<(
                                usize,
                                usize,
                                usize,
                                String,
                                ToolCallRequest,
                            )> = Vec::with_capacity(blocks.len());
                            for block in &blocks {
                                let base_name = infer_target_filename_for_block(
                                    &opts.raw_user_request,
                                    &block.lang,
                                    multiple_blocks,
                                );
                                let filename = disambiguate_filename(&base_name, &mut used_names);
                                let args = serde_json::json!({
                                    "path": filename,
                                    "content": block.code,
                                });
                                let tc = ToolCallRequest {
                                    id: format!("script-write-{}", new_id()),
                                    name: write_tool_id.clone(),
                                    arguments: args,
                                };
                                prepared_calls.push((
                                    block.start,
                                    block.end,
                                    block.line_count,
                                    filename,
                                    tc,
                                ));
                            }

                            let mut replacements: Vec<(usize, usize, String)> = Vec::new();
                            for (bidx, (bstart, bend, line_count, filename, tc)) in
                                prepared_calls.iter().enumerate()
                            {
                                if cancel.is_cancelled() {
                                    return Err(AgentError::Cancelled(format!(
                                        "task '{task_id}' cancelled"
                                    )));
                                }
                                let args_json = tc.arguments.to_string();
                                let _ = self.events_tx.send(StreamEvent::ToolCallStarted {
                                    task_id: task_id.to_string(),
                                    agent_id: agent.id.clone(),
                                    tool_id: write_tool_id.clone(),
                                    arguments_json: args_json.clone(),
                                });
                                let started = Instant::now();
                                if let Err(e) = check_runtime_sandbox_mode(
                                    &write_spec,
                                    &tc.arguments,
                                    sandbox_mode,
                                ) {
                                    self.record_tool_failure(
                                        agent,
                                        task_id,
                                        &write_tool_id,
                                        &args_json,
                                        &e.to_string(),
                                        started,
                                    );
                                    break;
                                }
                                let (needs_approval, exec_policy) =
                                    match evaluate_runtime_approval_policy(
                                        &write_spec,
                                        &cfg,
                                        approval_policy,
                                        persist_chat_message,
                                        false,
                                    ) {
                                        Ok(v) => v,
                                        Err(e) => {
                                            self.record_tool_failure(
                                                agent,
                                                task_id,
                                                &write_tool_id,
                                                &args_json,
                                                &e.to_string(),
                                                started,
                                            );
                                            break;
                                        }
                                    };

                                if needs_approval {
                                    let approval_id = new_id();
                                    let reason = format!(
                                        "Tool '{}' requires user approval (is_mutating={})",
                                        write_spec.id, write_spec.is_mutating
                                    );
                                    let batch_calls: Vec<ToolCallRequest> = prepared_calls[bidx..]
                                        .iter()
                                        .map(|(_, _, _, _, t)| t.clone())
                                        .collect();
                                    return self.pause_script_write_batch(
                                        approval_id,
                                        reason,
                                        agent,
                                        task_id,
                                        root_task_id,
                                        conversation_id,
                                        messages,
                                        &final_content,
                                        &replacements,
                                        batch_calls,
                                        &mut opts,
                                        iter + 1,
                                        hop_count,
                                        persist_chat_message,
                                    );
                                }

                                match self
                                    .tool_executor
                                    .execute_tool_with_context(
                                        agent,
                                        Some(task_id),
                                        conversation_id,
                                        &write_tool_id,
                                        tc.arguments.clone(),
                                        false,
                                        sandbox_mode,
                                        exec_policy,
                                        cancel.clone(),
                                    )
                                    .await
                                {
                                    Ok(out) => {
                                        opts.did_write_file = true;
                                        if !opts.written_paths.contains(filename) {
                                            opts.written_paths.push(filename.clone());
                                        }
                                        let _ = self.task_manager.mark_side_effects(task_id);
                                        if root_task_id != task_id {
                                            let _ =
                                                self.task_manager.mark_side_effects(root_task_id);
                                        }
                                        let dur = started.elapsed().as_millis() as u64;
                                        self.record_tool_success(
                                            agent,
                                            task_id,
                                            &write_tool_id,
                                            &args_json,
                                            &out.output,
                                            dur,
                                        );
                                        let _ = self.events_tx.send(StreamEvent::FilesChanged {
                                            conversation_id: conversation_id.map(str::to_string),
                                            path: filename.clone(),
                                            action: "write".to_string(),
                                        });
                                        replacements.push((
                                            *bstart,
                                            *bend,
                                            format!(
                                                "Created `{filename}` ({} lines) and transported it to the Files tab.\n[FILE_CREATED: {filename}]",
                                                line_count
                                            ),
                                        ));
                                    }
                                    Err(AgentError::ApprovalRequired {
                                        approval_id,
                                        reason,
                                    }) => {
                                        let batch_calls: Vec<ToolCallRequest> = prepared_calls
                                            [bidx..]
                                            .iter()
                                            .map(|(_, _, _, _, t)| t.clone())
                                            .collect();
                                        return self.pause_script_write_batch(
                                            approval_id,
                                            reason,
                                            agent,
                                            task_id,
                                            root_task_id,
                                            conversation_id,
                                            messages,
                                            &final_content,
                                            &replacements,
                                            batch_calls,
                                            &mut opts,
                                            iter + 1,
                                            hop_count,
                                            persist_chat_message,
                                        );
                                    }
                                    Err(e @ AgentError::Cancelled(_)) => {
                                        self.record_tool_failure(
                                            agent,
                                            task_id,
                                            &write_tool_id,
                                            &args_json,
                                            &e.to_string(),
                                            started,
                                        );
                                        return Err(e);
                                    }
                                    Err(e) => {
                                        self.record_tool_failure(
                                            agent,
                                            task_id,
                                            &write_tool_id,
                                            &args_json,
                                            &e.to_string(),
                                            started,
                                        );
                                    }
                                }
                            }
                            for (s, e, rep) in replacements.into_iter().rev() {
                                final_content.replace_range(s..e, &rep);
                            }
                        }
                    }
                } else {
                    let dup_blocks = extract_all_fenced_script_blocks(&final_content, 15);
                    if !dup_blocks.is_empty() {
                        let mut replacements: Vec<(usize, usize, String)> = Vec::new();
                        let mut mentioned_paths: Vec<String> = Vec::new();
                        for block in &dup_blocks {
                            let wrote_path = resolve_duplicate_written_path(
                                &opts.written_paths,
                                &opts.raw_user_request,
                                &block.lang,
                            );
                            replacements.push((
                                block.start,
                                block.end,
                                format!(
                                    "Created `{wrote_path}` and transported it to the Files tab."
                                ),
                            ));
                            if !mentioned_paths.contains(&wrote_path) {
                                mentioned_paths.push(wrote_path);
                            }
                        }
                        for (s, e, rep) in replacements.into_iter().rev() {
                            final_content.replace_range(s..e, &rep);
                        }
                        for wrote_path in mentioned_paths {
                            let marker = format!("[FILE_CREATED: {wrote_path}]");
                            if !final_content.contains(&marker) {
                                final_content.push('\n');
                                final_content.push_str(&marker);
                            }
                        }
                    }
                }

                let message_id = if persist_chat_message {
                    if let Some(conv_id) = conversation_id {
                        let msg = self.persist_assistant_message(
                            conv_id,
                            &agent.id,
                            task_id,
                            &final_content,
                            resp.reasoning.as_deref(),
                            None,
                            resp.latency_ms,
                        )?;
                        self.emit_chat_completed(conv_id, task_id, &agent.id, &msg);
                        self.spawn_kv_cache_prefill(conv_id, agent, hop_count);
                        Some(msg.id)
                    } else {
                        None
                    }
                } else {
                    None
                };

                self.task_manager
                    .complete_task(task_id, final_content.clone())?;
                return Ok(ChatTurnResult {
                    conversation_id: conversation_id.unwrap_or_default().to_string(),
                    task_id: task_id.to_string(),
                    agent_id: agent.id.clone(),
                    status: TaskStatus::Completed,
                    message_id,
                    content: final_content,
                    pending_approval_id: None,
                    selected_participants: Vec::new(),
                });
            }

            messages.push(InferenceMessage {
                role: "assistant".to_string(),
                content: resp.content.clone(),
                tool_call_id: None,
                name: Some(agent.id.clone()),
                tool_calls: Some(resp.tool_calls.clone()),
                image_urls: Vec::new(),
            });

            opts.start_iter = iter + 1;
            if let Some(paused) = self
                .execute_tool_batch(
                    agent,
                    task_id,
                    root_task_id,
                    conversation_id,
                    messages,
                    resp.tool_calls,
                    &mut opts,
                    hop_count,
                    persist_chat_message,
                    false,
                    cancel.clone(),
                )
                .await?
            {
                return Ok(paused);
            }
        }

        match opts.max_iters {
            Some(limit) => Err(AgentError::LimitExceeded(format!(
                "agent '{}' exceeded max_reasoning_iterations ({limit})",
                agent.id
            ))),
            None => Err(AgentError::LimitExceeded(format!(
                "agent '{}' exceeded unlimited reasoning safety cap ({UNLIMITED_REASONING_SAFETY_CAP})",
                agent.id
            ))),
        }
    }

    async fn resolve_enabled_write_tool(&self, agent: &AgentDefinition) -> Option<ToolSpec> {
        for candidate in ["write_file", "file_write"] {
            if let Some(spec) = self.tool_executor.get_tool(candidate).await {
                if spec.enabled && agent_allows_tool_spec(agent, &spec) {
                    return Some(spec);
                }
            }
        }
        None
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_tool_batch(
        self: &Arc<Self>,
        agent: &AgentDefinition,
        task_id: &str,
        root_task_id: &str,
        conversation_id: Option<&str>,
        messages: &mut Vec<InferenceMessage>,
        tool_calls: Vec<ToolCallRequest>,
        opts: &mut ReactLoopOptions,
        hop_count: u32,
        persist_chat_message: bool,
        first_call_approved: bool,
        cancel: CancellationToken,
    ) -> Result<Option<ChatTurnResult>> {
        let cfg = self.config();
        let sandbox_mode = resolve_effective_sandbox_mode(agent, &cfg);
        let approval_policy = resolve_effective_approval_policy(&cfg);

        for (idx, tc) in tool_calls.iter().enumerate() {
            if cancel.is_cancelled() {
                return Err(AgentError::Cancelled(format!("task '{task_id}' cancelled")));
            }
            let args_json = tc.arguments.to_string();
            let _ = self.events_tx.send(StreamEvent::ToolCallStarted {
                task_id: task_id.to_string(),
                agent_id: agent.id.clone(),
                tool_id: tc.name.clone(),
                arguments_json: args_json.clone(),
            });

            let started = Instant::now();

            let is_builtin_orchestration = matches!(
                tc.name.as_str(),
                "delegate_task"
                    | "agent_message"
                    | "spawn_temp_agent"
                    | "schedule_create"
                    | "schedule_list"
                    | "schedule_update"
                    | "schedule_delete"
            );
            let external_spec = if is_builtin_orchestration {
                None
            } else {
                self.tool_executor.get_tool(&tc.name).await
            };
            let allowed = match external_spec.as_ref() {
                Some(spec) => agent_allows_tool_spec(agent, spec),
                None => {
                    let category = if is_builtin_orchestration {
                        "orchestration"
                    } else {
                        ""
                    };
                    agent_allows_tool_name(agent, &tc.name, category)
                }
            };

            // Explicit allow/deny check (supports exact, "*", `group:*`, "prefix*", "custom:*", and denied_tools)
            if !allowed {
                let err_msg = format!(
                    "tool '{}' is not in agent '{}' enabled_tools allowlist",
                    tc.name, agent.id
                );
                self.record_tool_failure(agent, task_id, &tc.name, &args_json, &err_msg, started);
                messages.push(tool_observation_msg(tc, serde_json::json!({ "error": err_msg })));
                continue;
            }

            // Built-in runtime orchestration and scheduling tools
            if is_builtin_orchestration {
                let res = match tc.name.as_str() {
                    "delegate_task" => {
                        self.handle_delegate_task(
                            agent,
                            task_id,
                            root_task_id,
                            conversation_id,
                            tc.arguments.clone(),
                            hop_count,
                            cancel.clone(),
                        )
                        .await
                    }
                    "agent_message" => self.handle_agent_message(
                        agent,
                        root_task_id,
                        tc.arguments.clone(),
                        hop_count,
                    ),
                    "spawn_temp_agent" => self.handle_spawn_temp_agent(agent, tc.arguments.clone()),
                    "schedule_create" => self.handle_schedule_create_tool(tc.arguments.clone()),
                    "schedule_list" => self.handle_schedule_list_tool(tc.arguments.clone()),
                    "schedule_update" => self.handle_schedule_update_tool(tc.arguments.clone()),
                    "schedule_delete" => self.handle_schedule_delete_tool(tc.arguments.clone()),
                    _ => unreachable!(),
                };

                let dur = started.elapsed().as_millis() as u64;
                match res {
                    Ok(val) => {
                        self.record_tool_success(agent, task_id, &tc.name, &args_json, &val, dur);
                        messages.push(tool_observation_msg(tc, val));
                    }
                    Err(e @ AgentError::Cancelled(_)) => {
                        self.record_tool_failure(
                            agent,
                            task_id,
                            &tc.name,
                            &args_json,
                            &e.to_string(),
                            started,
                        );
                        return Err(e);
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        self.record_tool_failure(agent, task_id, &tc.name, &args_json, &msg, started);
                        messages.push(tool_observation_msg(tc, serde_json::json!({ "error": msg })));
                    }
                }
                continue;
            }

            // External tool lookup and validation
            let Some(spec) = external_spec else {
                let err_msg = format!("unknown tool '{}'", tc.name);
                self.record_tool_failure(agent, task_id, &tc.name, &args_json, &err_msg, started);
                messages.push(tool_observation_msg(tc, serde_json::json!({ "error": err_msg })));
                continue;
            };

            if !spec.enabled {
                let err_msg = format!("tool '{}' is disabled", tc.name);
                self.record_tool_failure(agent, task_id, &tc.name, &args_json, &err_msg, started);
                messages.push(tool_observation_msg(tc, serde_json::json!({ "error": err_msg })));
                continue;
            }

            if let Err(e) = check_runtime_sandbox_mode(&spec, &tc.arguments, sandbox_mode) {
                let msg = e.to_string();
                self.record_tool_failure(agent, task_id, &tc.name, &args_json, &msg, started);
                messages.push(tool_observation_msg(tc, serde_json::json!({ "error": msg })));
                continue;
            }

            let approved_bypass = first_call_approved && idx == 0;
            let (needs_approval, exec_policy) = match evaluate_runtime_approval_policy(
                &spec,
                &cfg,
                approval_policy,
                persist_chat_message,
                approved_bypass,
            ) {
                Ok(v) => v,
                Err(e) => {
                    let msg = e.to_string();
                    self.record_tool_failure(agent, task_id, &tc.name, &args_json, &msg, started);
                    messages.push(tool_observation_msg(tc, serde_json::json!({ "error": msg })));
                    continue;
                }
            };

            if spec.is_mutating {
                let _ = self.task_manager.mark_side_effects(task_id);
                if root_task_id != task_id {
                    let _ = self.task_manager.mark_side_effects(root_task_id);
                }
            }

            if needs_approval {
                let approval_id = new_id();
                let reason = format!(
                    "Tool '{}' requires user approval (is_mutating={})",
                    spec.id, spec.is_mutating
                );
                let remaining = tool_calls[idx + 1..].to_vec();
                self.pause_for_approval(
                    approval_id.clone(),
                    &reason,
                    agent,
                    task_id,
                    root_task_id,
                    conversation_id,
                    messages.clone(),
                    tc.clone(),
                    remaining,
                    opts,
                    opts.did_write_file,
                    hop_count,
                    persist_chat_message,
                )?;
                return Ok(Some(ChatTurnResult {
                    conversation_id: conversation_id.unwrap_or_default().to_string(),
                    task_id: task_id.to_string(),
                    agent_id: agent.id.clone(),
                    status: TaskStatus::Waiting,
                    message_id: None,
                    content: reason,
                    pending_approval_id: Some(approval_id),
                    selected_participants: Vec::new(),
                }));
            }

            match self
                .tool_executor
                .execute_tool_with_context(
                    agent,
                    Some(task_id),
                    conversation_id,
                    &tc.name,
                    tc.arguments.clone(),
                    approved_bypass,
                    sandbox_mode,
                    exec_policy,
                    cancel.clone(),
                )
                .await
            {
                Ok(out) => {
                    if out.is_mutating {
                        let _ = self.task_manager.mark_side_effects(task_id);
                        if root_task_id != task_id {
                            let _ = self.task_manager.mark_side_effects(root_task_id);
                        }
                    }
                    if matches!(tc.name.as_str(), "write_file" | "file_write") {
                        opts.did_write_file = true;
                        if let Some(path) = extract_tool_path(&tc.arguments, &out.output) {
                            let trimmed = path.trim().to_string();
                            if !trimmed.is_empty() && !opts.written_paths.contains(&trimmed) {
                                opts.written_paths.push(trimmed);
                            }
                        }
                    }
                    if let Some(action) = file_tool_action(&tc.name) {
                        if let Some(path) = extract_tool_path(&tc.arguments, &out.output) {
                            let _ = self.events_tx.send(StreamEvent::FilesChanged {
                                conversation_id: conversation_id.map(str::to_string),
                                path,
                                action: action.to_string(),
                            });
                        }
                    }
                    let dur = started.elapsed().as_millis() as u64;
                    self.record_tool_success(
                        agent,
                        task_id,
                        &tc.name,
                        &args_json,
                        &out.output,
                        dur,
                    );
                    messages.push(tool_observation_msg(tc, out.output));
                }
                Err(AgentError::ApprovalRequired {
                    approval_id,
                    reason,
                }) => {
                    let _ = self.task_manager.mark_side_effects(task_id);
                    let remaining = tool_calls[idx + 1..].to_vec();
                    self.pause_for_approval(
                        approval_id.clone(),
                        &reason,
                        agent,
                        task_id,
                        root_task_id,
                        conversation_id,
                        messages.clone(),
                        tc.clone(),
                        remaining,
                        opts,
                        opts.did_write_file,
                        hop_count,
                        persist_chat_message,
                    )?;
                    return Ok(Some(ChatTurnResult {
                        conversation_id: conversation_id.unwrap_or_default().to_string(),
                        task_id: task_id.to_string(),
                        agent_id: agent.id.clone(),
                        status: TaskStatus::Waiting,
                        message_id: None,
                        content: reason,
                        pending_approval_id: Some(approval_id),
                        selected_participants: Vec::new(),
                    }));
                }
                Err(e @ AgentError::Cancelled(_)) => {
                    let msg = e.to_string();
                    self.record_tool_failure(agent, task_id, &tc.name, &args_json, &msg, started);
                    return Err(e);
                }
                Err(e) => {
                    let msg = e.to_string();
                    self.record_tool_failure(agent, task_id, &tc.name, &args_json, &msg, started);
                    messages.push(tool_observation_msg(tc, serde_json::json!({ "error": msg })));
                }
            }
        }

        Ok(None)
    }

    #[allow(clippy::too_many_arguments)]
    fn pause_for_approval(
        &self,
        approval_id: String,
        reason: &str,
        agent: &AgentDefinition,
        task_id: &str,
        root_task_id: &str,
        conversation_id: Option<&str>,
        messages: Vec<InferenceMessage>,
        pending_tool_call: ToolCallRequest,
        remaining_tool_calls: Vec<ToolCallRequest>,
        opts: &ReactLoopOptions,
        did_write_file: bool,
        hop_count: u32,
        persist_chat_message: bool,
    ) -> Result<()> {
        if self.storage.get_approval(&approval_id)?.is_none() {
            self.storage.upsert_approval(&ApprovalRecord {
                id: approval_id.clone(),
                task_id: Some(task_id.to_string()),
                agent_id: agent.id.clone(),
                operation: pending_tool_call.name.clone(),
                tool_id: Some(pending_tool_call.name.clone()),
                arguments_json: pending_tool_call.arguments.to_string(),
                reason: reason.to_string(),
                status: ApprovalStatus::Pending,
                created_at: now_iso(),
                resolved_at: None,
                resolved_by: None,
            })?;
        }

        let seq = self.approval_seq.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut guard) = self.pending_approvals.write() {
            while guard.len() >= MAX_PENDING_APPROVALS {
                let Some(oldest_key) = guard
                    .iter()
                    .min_by_key(|(_, v)| v.seq)
                    .map(|(k, _)| k.clone())
                else {
                    break;
                };
                guard.remove(&oldest_key);
            }
            guard.insert(
                approval_id.clone(),
                PausedExecutionState {
                    seq,
                    task_id: task_id.to_string(),
                    root_task_id: root_task_id.to_string(),
                    conversation_id: conversation_id.map(str::to_string),
                    agent_id: agent.id.clone(),
                    messages,
                    pending_tool_call: pending_tool_call.clone(),
                    remaining_tool_calls,
                    next_iteration: opts.start_iter,
                    hop_count,
                    persist_chat_message,
                    thinking_level: opts.thinking_level,
                    raw_user_request: opts.raw_user_request.clone(),
                    auto_continuations: opts.auto_continuations,
                    continuation_parts: opts.continuation_parts.clone(),
                    did_write_file,
                    written_paths: opts.written_paths.clone(),
                },
            );
        }

        self.task_manager
            .mark_waiting(task_id, &format!("Awaiting approval {approval_id}: {reason}"))?;

        let _ = self.events_tx.send(StreamEvent::ApprovalRequested {
            approval_id: approval_id.clone(),
            task_id: Some(task_id.to_string()),
            agent_id: agent.id.clone(),
            operation: pending_tool_call.name,
            reason: reason.to_string(),
        });
        log_info(
            "runtime",
            format!("task '{task_id}' paused awaiting approval '{approval_id}'"),
        );
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_delegate_task(
        self: &Arc<Self>,
        sender: &AgentDefinition,
        parent_task_id: &str,
        root_task_id: &str,
        conversation_id: Option<&str>,
        args: serde_json::Value,
        hop_count: u32,
        cancel: CancellationToken,
    ) -> Result<serde_json::Value> {
        let target_id = args
            .get("agent_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| AgentError::InvalidInput("delegate_task requires 'agent_id'".into()))?;
        let subtask_prompt = args
            .get("task")
            .and_then(|v| v.as_str())
            .ok_or_else(|| AgentError::InvalidInput("delegate_task requires 'task'".into()))?;

        let next_hop = hop_count + 1;
        // Enforce A2A permissions and loop protection via MessageBus
        self.message_bus.send(SendMessageRequest {
            sender_agent_id: sender.id.clone(),
            recipient_agent_id: target_id.to_string(),
            task_id: Some(root_task_id.to_string()),
            message_type: AgentMessageType::TaskAssignment,
            content: subtask_prompt.to_string(),
            hop_count: next_hop,
        })?;

        let target_agent = self
            .agent_manager
            .get_agent(target_id)?
            .ok_or_else(|| AgentError::NotFound(format!("agent '{target_id}' not found")))?;

        let child_task = self.task_manager.create_task(
            subtask_prompt.to_string(),
            target_agent.id.clone(),
            conversation_id.map(str::to_string),
            Some(parent_task_id.to_string()),
            target_agent.priority,
            TaskStatus::Pending,
        )?;
        let child_cancel = self
            .task_manager
            .register_token(&child_task.id, Some(&cancel));

        // Box::pin required for async recursion across delegation trees.
        let child_res = Box::pin(self.execute_task_with_timeout(
            ReactTurnInput {
                task_id: child_task.id.clone(),
                root_task_id: root_task_id.to_string(),
                conversation_id: None,
                agent: target_agent.clone(),
                user_request: subtask_prompt.to_string(),
                raw_user_request: subtask_prompt.to_string(),
                user_message_id: None,
                thinking_level: None,
                attachments: Vec::new(),
                hop_count: next_hop,
                persist_chat_message: false,
            },
            child_cancel,
        ))
        .await;

        match child_res {
            Ok(res) => {
                if let Ok(Some(ct)) = self.task_manager.get_task(&child_task.id) {
                    if ct.has_side_effects {
                        let _ = self.task_manager.mark_side_effects(parent_task_id);
                        let _ = self.task_manager.mark_side_effects(root_task_id);
                    }
                }
                let _ = self.message_bus.send(SendMessageRequest {
                    sender_agent_id: target_agent.id.clone(),
                    recipient_agent_id: sender.id.clone(),
                    task_id: Some(root_task_id.to_string()),
                    message_type: AgentMessageType::TaskCompleted,
                    content: res.content.clone(),
                    hop_count: next_hop,
                });
                Ok(serde_json::json!({
                    "task_id": child_task.id,
                    "agent_id": target_agent.id,
                    "status": task_status_str(res.status),
                    "result": res.content,
                }))
            }
            Err(e) => {
                let _ = self.message_bus.send(SendMessageRequest {
                    sender_agent_id: target_agent.id.clone(),
                    recipient_agent_id: sender.id.clone(),
                    task_id: Some(root_task_id.to_string()),
                    message_type: AgentMessageType::ErrorReport,
                    content: e.to_string(),
                    hop_count: next_hop,
                });
                Err(e)
            }
        }
    }

    fn handle_agent_message(
        &self,
        sender: &AgentDefinition,
        root_task_id: &str,
        args: serde_json::Value,
        hop_count: u32,
    ) -> Result<serde_json::Value> {
        let recipient = args
            .get("recipient_agent_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                AgentError::InvalidInput("agent_message requires 'recipient_agent_id'".into())
            })?;
        let content = args
            .get("content")
            .and_then(|v| v.as_str())
            .ok_or_else(|| AgentError::InvalidInput("agent_message requires 'content'".into()))?;
        let raw_type = args
            .get("message_type")
            .and_then(|v| v.as_str())
            .unwrap_or("AGENT_MESSAGE");
        let msg_type = ps5_agent_messaging::parse_message_type(raw_type)?;

        let env = self.message_bus.send(SendMessageRequest {
            sender_agent_id: sender.id.clone(),
            recipient_agent_id: recipient.to_string(),
            task_id: Some(root_task_id.to_string()),
            message_type: msg_type,
            content: content.to_string(),
            hop_count: hop_count + 1,
        })?;

        Ok(serde_json::json!({
            "envelope_id": env.id,
            "status": ps5_agent_messaging::delivery_status_str(env.status),
        }))
    }

    fn handle_spawn_temp_agent(
        &self,
        sender: &AgentDefinition,
        args: serde_json::Value,
    ) -> Result<serde_json::Value> {
        if sender.role != AgentRole::Commander {
            return Err(AgentError::PermissionDenied(
                "only Commander may spawn temporary specialist agents".into(),
            ));
        }
        let name = args
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| AgentError::InvalidInput("spawn_temp_agent requires 'name'".into()))?;
        let description = args
            .get("description")
            .and_then(|v| v.as_str())
            .unwrap_or("Temporary specialist");
        let system_prompt = args
            .get("system_prompt")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                AgentError::InvalidInput("spawn_temp_agent requires 'system_prompt'".into())
            })?;
        let id = args
            .get("id")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        let enabled_tools = args
            .get("enabled_tools")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|x| x.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_else(|| {
                vec![
                    "read_file".into(),
                    "list_files".into(),
                    "grep_search".into(),
                    "glob_files".into(),
                    "memory_read".into(),
                    "read_agent_memory".into(),
                ]
            });

        let created = self.agent_manager.create_agent(
            CreateAgentInput {
                id,
                name: name.to_string(),
                description: description.to_string(),
                role: AgentRole::Custom,
                role_label: Some("Temporary Specialist".into()),
                system_prompt: system_prompt.to_string(),
                model_config: None,
                enabled_tools,
                denied_tools: None,
                sandbox_mode: None,
                tool_profile: None,
                memory_permissions: None,
                max_task_duration_secs: None,
                max_reasoning_iterations: None,
                priority: Some(55),
                can_communicate: Some(true),
                enabled: Some(true),
                is_temporary: true,
                spawned_by: Some(sender.id.clone()),
            },
            &sender.id,
        )?;

        Ok(serde_json::json!({
            "agent_id": created.id,
            "name": created.name,
            "is_temporary": created.is_temporary,
        }))
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_inference_call(
        &self,
        agent: &AgentDefinition,
        conversation_id: Option<&str>,
        task_id: &str,
        messages: Vec<InferenceMessage>,
        tools: Vec<ToolDefinitionSchema>,
        thinking_override: Option<ThinkingLevel>,
        cancel: CancellationToken,
    ) -> Result<InferenceResponse> {
        let cfg = self.config();
        let stream_enabled = cfg.inference.stream_enabled;
        let req = InferenceRequest {
            model: agent.model_config.model_override.clone(),
            api_mode: agent.model_config.api_mode,
            messages,
            tools,
            temperature: agent.model_config.temperature,
            max_output_tokens: agent.model_config.max_output_tokens,
            context_limit: agent.model_config.context_limit,
            stream: stream_enabled,
            thinking_level: thinking_override.unwrap_or(agent.model_config.thinking_level),
        };

        let inf_sem = self.current_inference_semaphore()?;
        let _inf_permit = inf_sem
            .acquire_owned()
            .await
            .map_err(|e| AgentError::Internal(format!("inference semaphore closed: {e}")))?;

        if !stream_enabled {
            return self.inference.generate(req, cancel).await;
        }

        let (chunk_tx, mut chunk_rx) = mpsc::channel::<InferenceStreamChunk>(64);
        let events_tx = self.events_tx.clone();
        let conv_id = conversation_id.unwrap_or_default().to_string();
        let tid = task_id.to_string();
        let aid = agent.id.clone();

        let forwarder = tokio::spawn(async move {
            while let Some(chunk) = chunk_rx.recv().await {
                match chunk {
                    InferenceStreamChunk::TextDelta(delta) => {
                        let _ = events_tx.send(StreamEvent::ChatToken {
                            conversation_id: conv_id.clone(),
                            task_id: tid.clone(),
                            agent_id: aid.clone(),
                            delta,
                        });
                    }
                    InferenceStreamChunk::ReasoningDelta(delta) => {
                        let _ = events_tx.send(StreamEvent::ChatReasoning {
                            conversation_id: conv_id.clone(),
                            task_id: tid.clone(),
                            agent_id: aid.clone(),
                            delta,
                        });
                    }
                    InferenceStreamChunk::Error(err) => {
                        log_warn("runtime", format!("stream chunk error for {tid}: {err}"));
                    }
                    InferenceStreamChunk::Completed(_) => {}
                }
            }
        });

        let res = self.inference.generate_stream(req, chunk_tx, cancel).await;
        let _ = forwarder.await;
        res
    }

    async fn allowed_tool_schemas(&self, agent: &AgentDefinition) -> Vec<ToolDefinitionSchema> {
        let mut schemas = Vec::new();
        for builtin in builtin_orchestration_schemas() {
            if agent_allows_tool_name(agent, &builtin.name, "orchestration") {
                schemas.push(builtin);
            }
        }
        for spec in self.tool_executor.list_tools().await {
            if spec.enabled && agent_allows_tool_spec(agent, &spec) {
                schemas.push(ToolDefinitionSchema {
                    name: spec.id,
                    description: spec.description,
                    parameters: spec.input_schema,
                });
            }
        }
        schemas
    }

    fn build_system_prompt(
        &self,
        agent: &AgentDefinition,
        mem_ctx: &str,
        include_user_brain: bool,
    ) -> Result<String> {
        let mut prompt = format!(
            "{}\n\nAgent Identity: {} (id: {}, role: {})\nCurrent UTC time: {}",
            agent.system_prompt,
            agent.name,
            agent.id,
            agent.role_label,
            Utc::now().to_rfc3339()
        );

        prompt.push_str(
            "\n\nCore Response Rules:\n\
             - Always speak directly in the 1st person (\"I...\"), never refer to yourself in the 3rd person.\n\
             - Never dump your tool list unless the user explicitly asks what tools you have.\n\
             - When asked to create or build code, HTML, apps, games, or scripts, save the complete working file via `write_file` instead of pasting long truncated code blocks in chat, then briefly confirm the saved filename.",
        );

        let cfg = self.config();
        if cfg.sandbox.inject_brain_files {
            let workspace_root = primary_workspace_root(&cfg);
            let brain_section = crate::brain::build_brain_prompt_section(
                &workspace_root,
                cfg.sandbox.max_brain_context_chars,
                include_user_brain,
            );
            if !brain_section.trim().is_empty() {
                prompt.push_str("\n\nWorkspace Guidance & Brain Files:\n");
                prompt.push_str(&brain_section);
            }
        }

        if agent.can_communicate {
            let peers: Vec<String> = self
                .agent_manager
                .list_agents()?
                .into_iter()
                .filter(|a| a.enabled && a.id != agent.id)
                .map(|a| format!("{} ({}): {}", a.id, a.role_label, a.description))
                .collect();
            if !peers.is_empty() {
                prompt.push_str("\n\nAvailable peer agents:\n");
                for p in peers {
                    prompt.push_str(&format!("- {p}\n"));
                }
            }
        }

        let (skills, plugin_rules) = self.tool_executor.extension_prompt_context();
        if !skills.is_empty() && agent_allows_tool_name(agent, "load_skill", "skills") {
            prompt.push_str(
                "\n\nAvailable Skills (call `load_skill` with skill id/name when relevant):\n",
            );
            for (name, desc) in skills.into_iter().take(24) {
                prompt.push_str(&format!("- {}: {}\n", name, desc));
            }
        }
        if !plugin_rules.is_empty() {
            prompt.push_str("\n\nActive Plugin Guidelines:\n");
            for (plugin_name, rule) in plugin_rules.into_iter().take(24) {
                prompt.push_str(&format!("- [{}] {}\n", plugin_name, rule));
            }
        }

        if !mem_ctx.trim().is_empty() {
            prompt.push_str("\n\n");
            prompt.push_str(mem_ctx);
        }
        Ok(prompt)
    }

    fn append_user_message_with_attachments(
        &self,
        conversation_id: &str,
        content: &str,
        attachments: Vec<ChatAttachment>,
    ) -> Result<ChatMessage> {
        let msg = ChatMessage {
            id: new_id(),
            conversation_id: conversation_id.to_string(),
            role: ChatRole::User,
            agent_id: None,
            content: content.to_string(),
            reasoning: None,
            tool_calls_json: None,
            task_id: None,
            token_count: estimate_tokens(content),
            latency_ms: None,
            archived: false,
            attachments,
            created_at: now_iso(),
        };
        self.storage.append_chat_message(&msg)?;
        Ok(msg)
    }

    #[allow(clippy::too_many_arguments)]
    fn persist_assistant_message(
        &self,
        conversation_id: &str,
        agent_id: &str,
        task_id: &str,
        content: &str,
        reasoning: Option<&str>,
        tool_calls_json: Option<&str>,
        latency_ms: u64,
    ) -> Result<ChatMessage> {
        let msg = ChatMessage {
            id: new_id(),
            conversation_id: conversation_id.to_string(),
            role: ChatRole::Assistant,
            agent_id: Some(agent_id.to_string()),
            content: content.to_string(),
            reasoning: reasoning.map(str::to_string),
            tool_calls_json: tool_calls_json.map(str::to_string),
            task_id: Some(task_id.to_string()),
            token_count: estimate_tokens(content),
            latency_ms: Some(latency_ms),
            archived: false,
            attachments: Vec::new(),
            created_at: now_iso(),
        };
        self.storage.append_chat_message(&msg)?;
        Ok(msg)
    }

    fn emit_chat_completed(
        &self,
        conversation_id: &str,
        task_id: &str,
        agent_id: &str,
        msg: &ChatMessage,
    ) {
        let _ = self.events_tx.send(StreamEvent::ChatCompleted {
            conversation_id: conversation_id.to_string(),
            task_id: task_id.to_string(),
            agent_id: agent_id.to_string(),
            message_id: msg.id.clone(),
            content: msg.content.clone(),
            reasoning: msg.reasoning.clone(),
        });
    }

    fn record_tool_success(
        &self,
        agent: &AgentDefinition,
        task_id: &str,
        tool_id: &str,
        input_json: &str,
        output: &serde_json::Value,
        duration_ms: u64,
    ) {
        let _ = self.storage.record_tool_execution(&ToolExecutionRecord {
            id: new_id(),
            tool_id: tool_id.to_string(),
            agent_id: agent.id.clone(),
            task_id: Some(task_id.to_string()),
            input_json: input_json.to_string(),
            output_json: Some(output.to_string()),
            success: true,
            error: None,
            duration_ms,
            created_at: now_iso(),
        });
        let _ = self.events_tx.send(StreamEvent::ToolCallFinished {
            task_id: task_id.to_string(),
            agent_id: agent.id.clone(),
            tool_id: tool_id.to_string(),
            success: true,
            duration_ms,
        });
    }

    fn record_tool_failure(
        &self,
        agent: &AgentDefinition,
        task_id: &str,
        tool_id: &str,
        input_json: &str,
        error: &str,
        started: Instant,
    ) {
        let duration_ms = started.elapsed().as_millis() as u64;
        let _ = self.storage.record_tool_execution(&ToolExecutionRecord {
            id: new_id(),
            tool_id: tool_id.to_string(),
            agent_id: agent.id.clone(),
            task_id: Some(task_id.to_string()),
            input_json: input_json.to_string(),
            output_json: None,
            success: false,
            error: Some(error.to_string()),
            duration_ms,
            created_at: now_iso(),
        });
        let _ = self.events_tx.send(StreamEvent::ToolCallFinished {
            task_id: task_id.to_string(),
            agent_id: agent.id.clone(),
            tool_id: tool_id.to_string(),
            success: false,
            duration_ms,
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

    pub fn list_schedules(&self, limit: usize) -> Result<Vec<ScheduleRecord>> {
        self.storage.list_schedules(limit)
    }

    pub fn get_schedule(&self, id: &str) -> Result<Option<ScheduleRecord>> {
        self.storage.get_schedule(id)
    }

    fn validate_schedule_target_agent(&self, agent_id: Option<&str>) -> Result<Option<String>> {
        let Some(tid) = agent_id.map(str::trim).filter(|s| !s.is_empty()) else {
            return Ok(None);
        };
        let agent = self
            .agent_manager
            .get_agent(tid)?
            .ok_or_else(|| AgentError::NotFound(format!("agent '{tid}' not found")))?;
        if !agent.enabled {
            return Err(AgentError::PermissionDenied(format!(
                "agent '{tid}' is disabled"
            )));
        }
        Ok(Some(agent.id))
    }

    pub fn create_schedule(&self, input: CreateScheduleInput) -> Result<ScheduleRecord> {
        let title = input.title.trim().to_string();
        let prompt = input.prompt.trim().to_string();
        if title.is_empty() {
            return Err(AgentError::InvalidInput(
                "schedule title cannot be empty".into(),
            ));
        }
        if prompt.is_empty() {
            return Err(AgentError::InvalidInput(
                "schedule prompt cannot be empty".into(),
            ));
        }

        let target_agent_id =
            self.validate_schedule_target_agent(input.target_agent_id.as_deref())?;
        let conversation_id = input
            .conversation_id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        if let Some(ref cid) = conversation_id {
            self.storage
                .get_conversation(cid)?
                .ok_or_else(|| AgentError::NotFound(format!("conversation '{cid}' not found")))?;
        }

        const MAX_SCHEDULES: usize = 1000;
        if self.storage.list_schedules(MAX_SCHEDULES)?.len() >= MAX_SCHEDULES {
            return Err(AgentError::InvalidInput(format!(
                "maximum schedule limit ({MAX_SCHEDULES}) reached; delete unused schedules first"
            )));
        }

        let now = Utc::now();
        let mut record = ScheduleRecord {
            id: new_id(),
            title,
            prompt,
            target_agent_id,
            conversation_id,
            kind: input.kind,
            run_at: input.run_at,
            interval_secs: input.interval_secs,
            daily_time: input
                .daily_time
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string),
            cron_expr: input
                .cron_expr
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string),
            timezone_offset_mins: input.timezone_offset_mins,
            enabled: input.enabled.unwrap_or(true),
            next_run_at: None,
            last_run_at: None,
            last_status: None,
            last_error: None,
            last_task_id: None,
            run_count: 0,
            created_at: now,
            updated_at: now,
        };

        validate_schedule_fields(&record)?;
        record.next_run_at = compute_next_run_at(&record, now)?;

        self.storage.upsert_schedule(&record)?;
        let _ = self.events_tx.send(StreamEvent::ScheduleUpdated {
            schedule: record.clone(),
        });
        self.append_audit(
            "runtime",
            "schedule_created",
            &record.id,
            "info",
            "ok",
            serde_json::json!({ "title": record.title, "kind": record.kind }),
        );
        Ok(record)
    }

    pub fn update_schedule(&self, id: &str, input: UpdateScheduleInput) -> Result<ScheduleRecord> {
        let mut record = self
            .storage
            .get_schedule(id)?
            .ok_or_else(|| AgentError::NotFound(format!("schedule '{id}' not found")))?;

        if let Some(title) = input.title {
            let trimmed = title.trim().to_string();
            if trimmed.is_empty() {
                return Err(AgentError::InvalidInput(
                    "schedule title cannot be empty".into(),
                ));
            }
            record.title = trimmed;
        }
        if let Some(prompt) = input.prompt {
            let trimmed = prompt.trim().to_string();
            if trimmed.is_empty() {
                return Err(AgentError::InvalidInput(
                    "schedule prompt cannot be empty".into(),
                ));
            }
            record.prompt = trimmed;
        }
        if let Some(target_agent_id) = input.target_agent_id {
            record.target_agent_id =
                self.validate_schedule_target_agent(Some(&target_agent_id))?;
        }
        let reschedule_once = input.run_at.is_some() || input.enabled == Some(true);
        if let Some(kind) = input.kind {
            record.kind = kind;
        }
        if let Some(run_at) = input.run_at {
            record.run_at = Some(run_at);
        }
        if let Some(interval_secs) = input.interval_secs {
            record.interval_secs = Some(interval_secs);
        }
        if let Some(daily_time) = input.daily_time {
            let trimmed = daily_time.trim().to_string();
            record.daily_time = (!trimmed.is_empty()).then_some(trimmed);
        }
        if let Some(cron_expr) = input.cron_expr {
            let trimmed = cron_expr.trim().to_string();
            record.cron_expr = (!trimmed.is_empty()).then_some(trimmed);
        }
        if let Some(tz) = input.timezone_offset_mins {
            record.timezone_offset_mins = tz;
        }
        if let Some(enabled) = input.enabled {
            record.enabled = enabled;
        }

        validate_schedule_fields(&record)?;
        if record.kind == ScheduleKind::Once && reschedule_once {
            record.run_count = 0;
        }
        let now = Utc::now();
        record.next_run_at = compute_next_run_at(&record, now)?;
        record.updated_at = now;

        self.storage.upsert_schedule(&record)?;
        let _ = self.events_tx.send(StreamEvent::ScheduleUpdated {
            schedule: record.clone(),
        });
        Ok(record)
    }

    pub fn toggle_schedule(&self, id: &str, enabled: Option<bool>) -> Result<ScheduleRecord> {
        let mut record = self
            .storage
            .get_schedule(id)?
            .ok_or_else(|| AgentError::NotFound(format!("schedule '{id}' not found")))?;
        record.enabled = enabled.unwrap_or(!record.enabled);
        let now = Utc::now();
        if record.enabled {
            validate_schedule_fields(&record)?;
            if record.kind == ScheduleKind::Once {
                record.run_count = 0;
            }
        }
        record.next_run_at = compute_next_run_at(&record, now)?;
        record.updated_at = now;

        self.storage.upsert_schedule(&record)?;
        let _ = self.events_tx.send(StreamEvent::ScheduleUpdated {
            schedule: record.clone(),
        });
        Ok(record)
    }

    pub fn delete_schedule(&self, id: &str) -> Result<()> {
        if self.storage.get_schedule(id)?.is_none() {
            return Err(AgentError::NotFound(format!("schedule '{id}' not found")));
        }
        self.storage.delete_schedule(id)?;
        self.append_audit(
            "runtime",
            "schedule_deleted",
            id,
            "info",
            "ok",
            serde_json::json!({}),
        );
        Ok(())
    }

    pub async fn parse_schedule_ai(
        &self,
        raw_prompt: &str,
        timezone_offset_mins: i32,
        preferred_agent: Option<String>,
    ) -> AiScheduleDraft {
        let agents = self.agent_manager.list_agents().unwrap_or_default();
        let model_hint = self.config().inference.model;
        let hint_opt = Some(model_hint.as_str()).filter(|s| !s.trim().is_empty());
        parse_schedule_with_ai(
            Some(&self.inference),
            &agents,
            raw_prompt,
            Utc::now(),
            timezone_offset_mins,
            preferred_agent.as_deref(),
            hint_opt,
        )
        .await
    }

    pub async fn trigger_schedule_now(
        &self,
        id: &str,
        cancel: CancellationToken,
    ) -> Result<ScheduleRecord> {
        let Some(guard) = self.acquire_schedule_guard(id)? else {
            return Err(AgentError::InvalidInput(format!(
                "schedule '{id}' is already running"
            )));
        };
        let schedule = self
            .storage
            .get_schedule(id)?
            .ok_or_else(|| AgentError::NotFound(format!("schedule '{id}' not found")))?;
        self.execute_guarded_schedule(schedule, guard, cancel).await
    }

    pub async fn tick_due_schedules(&self, cancel: CancellationToken) -> Result<usize> {
        let now = Utc::now();
        let due = self.storage.list_due_schedules(now, 64)?;
        let due_ids: Vec<String> = due.into_iter().map(|s| s.id).collect();
        if due_ids.is_empty() {
            return Ok(0);
        }

        let rt = self.arc_self()?;
        let mut set = JoinSet::new();
        for id in due_ids {
            let Ok(Some(guard)) = rt.acquire_schedule_guard(&id) else {
                continue;
            };
            let Some(schedule) = rt.storage.get_schedule(&id)? else {
                continue;
            };
            if !schedule.enabled || schedule.next_run_at.is_none_or(|t| t > Utc::now()) {
                continue;
            }
            let rt_bg = Arc::clone(&rt);
            let cancel_bg = cancel.clone();
            set.spawn(async move {
                rt_bg
                    .execute_guarded_schedule(schedule, guard, cancel_bg)
                    .await
            });
        }

        let mut executed = 0usize;
        while let Some(joined) = set.join_next().await {
            match joined {
                Ok(Ok(_)) => executed += 1,
                Ok(Err(e)) => log_warn("runtime", format!("scheduled task tick failed: {e}")),
                Err(e) => log_warn("runtime", format!("scheduled task join failed: {e}")),
            }
        }
        Ok(executed)
    }

    fn resolve_schedule_target_agent(&self, target_agent_id: Option<&str>) -> Result<AgentDefinition> {
        if let Some(tid) = target_agent_id.map(str::trim).filter(|s| !s.is_empty()) {
            let agent = self
                .agent_manager
                .get_agent(tid)?
                .ok_or_else(|| AgentError::NotFound(format!("agent '{tid}' not found")))?;
            if !agent.enabled {
                return Err(AgentError::PermissionDenied(format!(
                    "agent '{tid}' is disabled"
                )));
            }
            return Ok(agent);
        }
        if let Some(cmd) = self
            .agent_manager
            .get_agent("commander")?
            .filter(|a| a.enabled)
        {
            return Ok(cmd);
        }
        self.agent_manager
            .list_agents()?
            .into_iter()
            .find(|a| a.enabled)
            .ok_or_else(|| AgentError::NotFound("no enabled agent available for schedule".into()))
    }

    async fn execute_guarded_schedule(
        &self,
        mut schedule: ScheduleRecord,
        _guard: ScheduleRunGuard,
        cancel: CancellationToken,
    ) -> Result<ScheduleRecord> {
        let rt = self.arc_self()?;
        let target_agent = match self.resolve_schedule_target_agent(schedule.target_agent_id.as_deref()) {
            Ok(agent) => agent,
            Err(e) => {
                let now = Utc::now();
                let target_err = e.to_string();
                let mut latest = self
                    .storage
                    .get_schedule(&schedule.id)?
                    .unwrap_or_else(|| schedule.clone());
                latest.last_run_at = Some(now);
                latest.last_status = Some("failed".to_string());
                latest.last_error = Some(target_err.clone());
                latest.run_count = latest.run_count.saturating_add(1);
                if latest.kind == ScheduleKind::Once {
                    latest.enabled = false;
                    latest.next_run_at = None;
                } else {
                    latest.next_run_at = if latest.enabled {
                        compute_next_run_at(&latest, now).ok().flatten()
                    } else {
                        None
                    };
                }
                latest.updated_at = now;
                if self.storage.get_schedule(&latest.id)?.is_some() {
                    self.storage.upsert_schedule(&latest)?;
                    let _ = self.events_tx.send(StreamEvent::ScheduleUpdated {
                        schedule: latest.clone(),
                    });
                }
                let _ = self.events_tx.send(StreamEvent::SystemAlert {
                    level: "error".to_string(),
                    message: format!("Schedule '{}' failed: {e}", latest.title),
                });
                return Ok(latest);
            }
        };

        let existing_conv = match schedule.conversation_id.as_deref() {
            Some(cid) => self.storage.get_conversation(cid)?,
            None => None,
        };
        let conv_id = if let Some(conv) = existing_conv {
            conv.id
        } else {
            let new_conv_id = new_id();
            let now_s = now_iso();
            let conv = Conversation {
                id: new_conv_id.clone(),
                title: format!("Schedule: {}", schedule.title),
                mode: ConversationMode::Direct {
                    agent_id: target_agent.id.clone(),
                },
                created_at: now_s.clone(),
                updated_at: now_s,
                archived: false,
                sort_order: self.top_conversation_sort_order(),
            };
            self.storage.create_conversation(&conv)?;
            schedule.conversation_id = Some(new_conv_id.clone());
            new_conv_id
        };

        let start_now = Utc::now();
        schedule.last_run_at = Some(start_now);
        schedule.last_status = Some("running".to_string());
        schedule.last_error = None;
        schedule.run_count = schedule.run_count.saturating_add(1);
        if schedule.kind == ScheduleKind::Once {
            schedule.enabled = false;
            schedule.next_run_at = None;
        } else {
            schedule.next_run_at = if schedule.enabled {
                compute_next_run_at(&schedule, start_now)?
            } else {
                None
            };
        }
        schedule.updated_at = start_now;
        self.storage.upsert_schedule(&schedule)?;
        let _ = self.events_tx.send(StreamEvent::ScheduleUpdated {
            schedule: schedule.clone(),
        });

        let prompt_text = schedule.prompt.clone();
        let effective_prompt = format_explicit_agent_prompt(&target_agent, &prompt_text);
        let turn_res = match self.append_user_message_with_attachments(&conv_id, &prompt_text, Vec::new())
        {
            Ok(user_msg) => {
                rt.run_direct_agent_chat_with_options(
                    &conv_id,
                    target_agent,
                    &effective_prompt,
                    &prompt_text,
                    Some(user_msg.id),
                    None,
                    Vec::new(),
                    cancel,
                )
                .await
            }
            Err(e) => Err(e),
        };

        let (last_task_id, last_status, last_error) = match turn_res {
            Ok(turn) => (
                Some(turn.task_id),
                Some(match turn.status {
                    TaskStatus::Waiting => "waiting".to_string(),
                    _ => "completed".to_string(),
                }),
                None,
            ),
            Err(e) => {
                let cancelled = matches!(e, AgentError::Cancelled(_));
                if !cancelled {
                    self.emit_chat_error_alert(&conv_id, &e);
                }
                (
                    None,
                    Some(if cancelled {
                        "cancelled".to_string()
                    } else {
                        "failed".to_string()
                    }),
                    Some(e.to_string()),
                )
            }
        };

        // Reload fresh schedule row so concurrent user edits during execution are preserved.
        if let Some(mut latest) = self.storage.get_schedule(&schedule.id)? {
            latest.last_run_at = schedule.last_run_at;
            if let Some(tid) = last_task_id {
                latest.last_task_id = Some(tid);
            }
            latest.last_status = last_status;
            latest.last_error = last_error;
            if latest.conversation_id.is_none() {
                latest.conversation_id = schedule.conversation_id.clone();
            }
            latest.updated_at = Utc::now();
            self.storage.upsert_schedule(&latest)?;
            let _ = self.events_tx.send(StreamEvent::ScheduleUpdated {
                schedule: latest.clone(),
            });
            return Ok(latest);
        }
        Ok(schedule)
    }

    fn handle_schedule_create_tool(&self, args: serde_json::Value) -> Result<serde_json::Value> {
        let prompt = args
            .get("prompt")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| AgentError::InvalidInput("schedule_create requires 'prompt'".into()))?
            .to_string();
        let title = args
            .get("title")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| {
                let mut t: String = prompt.chars().take(48).collect();
                if prompt.chars().count() > 48 {
                    t.push_str("...");
                }
                t
            });

        let interval_secs = args.get("interval_secs").and_then(|v| v.as_u64());
        let daily_time = args
            .get("daily_time")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        let cron_expr = args
            .get("cron_expr")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        let kind = match args.get("kind").and_then(|v| v.as_str()) {
            Some(raw) => parse_schedule_kind_str(raw)?,
            None => {
                if interval_secs.is_some() {
                    ScheduleKind::Interval
                } else if daily_time.is_some() {
                    ScheduleKind::Daily
                } else if cron_expr.is_some() {
                    ScheduleKind::Cron
                } else {
                    ScheduleKind::Once
                }
            }
        };

        let mut run_at = parse_optional_schedule_run_at(args.get("run_at"))?;
        if run_at.is_none() && kind == ScheduleKind::Once {
            if let Some(delay_secs) = args.get("delay_secs").and_then(|v| v.as_u64()) {
                run_at = Some(compute_delayed_run_at(delay_secs)?);
            }
        }

        let input = CreateScheduleInput {
            title,
            prompt,
            target_agent_id: args
                .get("target_agent_id")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            conversation_id: args
                .get("conversation_id")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            kind,
            run_at,
            interval_secs,
            daily_time,
            cron_expr,
            timezone_offset_mins: args
                .get("timezone_offset_mins")
                .and_then(|v| v.as_i64())
                .unwrap_or(0) as i32,
            enabled: args.get("enabled").and_then(|v| v.as_bool()),
        };

        let record = self.create_schedule(input)?;
        Ok(serde_json::json!({ "schedule": record }))
    }

    fn handle_schedule_list_tool(&self, args: serde_json::Value) -> Result<serde_json::Value> {
        let limit = args.get("limit").and_then(|v| v.as_u64()).unwrap_or(50) as usize;
        let schedules = self.list_schedules(limit)?;
        let count = schedules.len();
        Ok(serde_json::json!({ "schedules": schedules, "count": count }))
    }

    fn handle_schedule_update_tool(&self, args: serde_json::Value) -> Result<serde_json::Value> {
        let id = args
            .get("id")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| AgentError::InvalidInput("schedule_update requires 'id'".into()))?;

        let kind = match args.get("kind").and_then(|v| v.as_str()) {
            Some(raw) => Some(parse_schedule_kind_str(raw)?),
            None => None,
        };
        let mut run_at = parse_optional_schedule_run_at(args.get("run_at"))?;
        if run_at.is_none() {
            if let Some(delay_secs) = args.get("delay_secs").and_then(|v| v.as_u64()) {
                run_at = Some(compute_delayed_run_at(delay_secs)?);
            }
        }

        let input = UpdateScheduleInput {
            title: args
                .get("title")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            prompt: args
                .get("prompt")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            target_agent_id: args
                .get("target_agent_id")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            kind,
            run_at,
            interval_secs: args.get("interval_secs").and_then(|v| v.as_u64()),
            daily_time: args
                .get("daily_time")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            cron_expr: args
                .get("cron_expr")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            timezone_offset_mins: args
                .get("timezone_offset_mins")
                .and_then(|v| v.as_i64())
                .map(|v| v as i32),
            enabled: args.get("enabled").and_then(|v| v.as_bool()),
        };

        let record = self.update_schedule(id, input)?;
        Ok(serde_json::json!({ "schedule": record }))
    }

    fn handle_schedule_delete_tool(&self, args: serde_json::Value) -> Result<serde_json::Value> {
        let id = args
            .get("id")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| AgentError::InvalidInput("schedule_delete requires 'id'".into()))?;
        self.delete_schedule(id)?;
        Ok(serde_json::json!({ "deleted": true, "id": id }))
    }
}

fn parse_schedule_kind_str(raw: &str) -> Result<ScheduleKind> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "once" => Ok(ScheduleKind::Once),
        "interval" => Ok(ScheduleKind::Interval),
        "daily" => Ok(ScheduleKind::Daily),
        "cron" => Ok(ScheduleKind::Cron),
        other => Err(AgentError::InvalidInput(format!(
            "invalid schedule kind '{other}'; expected one of: once, interval, daily, cron"
        ))),
    }
}

fn parse_optional_schedule_run_at(val: Option<&serde_json::Value>) -> Result<Option<DateTime<Utc>>> {
    let Some(raw) = val.and_then(|v| v.as_str()).map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(None);
    };
    DateTime::parse_from_rfc3339(raw)
        .map(|dt| Some(dt.with_timezone(&Utc)))
        .map_err(|e| AgentError::InvalidInput(format!("invalid RFC3339 run_at '{raw}': {e}")))
}

fn fold_polish_ascii(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        let mapped = match ch {
            'ą' | 'Ą' => 'a',
            'ć' | 'Ć' => 'c',
            'ę' | 'Ę' => 'e',
            'ł' | 'Ł' => 'l',
            'ń' | 'Ń' => 'n',
            'ó' | 'Ó' => 'o',
            'ś' | 'Ś' => 's',
            'ź' | 'Ź' | 'ż' | 'Ż' => 'z',
            other => {
                for lc in other.to_lowercase() {
                    out.push(lc);
                }
                continue;
            }
        };
        out.push(mapped);
    }
    out
}

#[derive(Clone, Copy)]
struct WordSpan<'a> {
    raw: &'a str,
    start: usize,
    end: usize,
}

fn whitespace_word_spans(s: &str) -> Vec<WordSpan<'_>> {
    let mut spans = Vec::new();
    let mut in_word = false;
    let mut word_start = 0usize;
    for (idx, ch) in s.char_indices() {
        if ch.is_whitespace() {
            if in_word {
                spans.push(WordSpan {
                    raw: &s[word_start..idx],
                    start: word_start,
                    end: idx,
                });
                in_word = false;
            }
        } else if !in_word {
            word_start = idx;
            in_word = true;
        }
    }
    if in_word {
        spans.push(WordSpan {
            raw: &s[word_start..],
            start: word_start,
            end: s.len(),
        });
    }
    spans
}

fn trim_routing_punct(raw: &str) -> &str {
    raw.trim()
        .trim_matches(|c: char| matches!(c, ':' | ',' | ';' | '.' | '!' | '?' | '"' | '\'' | '(' | ')' | '[' | ']'))
}

fn exact_agent_matches<'a>(
    agents: &'a [AgentDefinition],
    folded_token: &str,
) -> Vec<&'a AgentDefinition> {
    agents
        .iter()
        .filter(|a| {
            if !a.enabled {
                return false;
            }
            let name_folded = fold_polish_ascii(&a.name);
            fold_polish_ascii(&a.id) == folded_token
                || name_folded == folded_token
                || fold_polish_ascii(a.role.as_str()) == folded_token
                || fold_polish_ascii(&a.role_label) == folded_token
                || name_folded.replace(' ', "-") == folded_token
                || name_folded.replace(' ', "_") == folded_token
        })
        .collect()
}

fn match_agent_token(agents: &[AgentDefinition], raw_token: &str) -> Option<AgentDefinition> {
    let tok = fold_polish_ascii(trim_routing_punct(raw_token));
    if tok.is_empty() {
        return None;
    }
    if let Some(exact) = exact_agent_matches(agents, &tok).into_iter().next() {
        return Some(exact.clone());
    }

    // Polish case suffixes (longest-first) on stems >= 4 chars matching one enabled agent cleanly.
    for suffix in ["owi", "iem", "ami", "em", "ie", "om", "u", "a", "y"] {
        if let Some(stem) = tok.strip_suffix(suffix) {
            if stem.chars().count() >= 4 {
                let matches = exact_agent_matches(agents, stem);
                if matches.len() == 1 {
                    return Some(matches[0].clone());
                }
            }
        }
    }
    None
}

fn format_direct_specialist_prompt(agent: &AgentDefinition, request: &str) -> String {
    if agent.id == "commander" {
        return request.trim().to_string();
    }
    format!(
        "The user asks you directly: {}\n\nRespond in 1st person as yourself and fulfill the request directly (if creating code/files/apps/games, write the complete file with write_file first).",
        request.trim()
    )
}

fn strip_delegation_connectors(text: &str) -> &str {
    let trimmed = text.trim_start_matches(|c: char| matches!(c, ':' | ',' | '-' | ' ' | '\t'));
    let spans = whitespace_word_spans(trimmed);
    if spans.is_empty() {
        return trimmed;
    }
    let w0 = fold_polish_ascii(trim_routing_punct(spans[0].raw));
    let w1 = spans
        .get(1)
        .map(|s| fold_polish_ascii(trim_routing_punct(s.raw)));

    let rest = if spans.len() >= 2 && w0 == "ze" && w1.as_deref() == Some("ma") {
        &trimmed[spans[1].end..]
    } else if matches!(
        w0.as_str(),
        "ze" | "zeby" | "aby" | "by" | "to" | "that" | "iz"
    ) {
        &trimmed[spans[0].end..]
    } else {
        trimmed
    };
    rest.trim_start_matches(|c: char| matches!(c, ':' | ',' | '-' | ' ' | '\t'))
        .trim()
}

fn is_multi_agent_list_after(
    agents: &[AgentDefinition],
    spans: &[WordSpan<'_>],
    idx: usize,
    primary_id: &str,
) -> bool {
    let Some(first) = spans.get(idx) else {
        return false;
    };
    let Some(second) = spans.get(idx + 1) else {
        return false;
    };
    let first_has_list_sep = first.raw.ends_with(',') || first.raw.ends_with('/');
    let second_is_other_agent =
        match_agent_token(agents, second.raw).is_some_and(|other| other.id != primary_id);
    if first_has_list_sep && second_is_other_agent {
        return true;
    }

    let second_trim = second.raw.trim();
    let second_folded = fold_polish_ascii(trim_routing_punct(second.raw));
    let is_connector = matches!(second_trim, "," | "/" | "&" | "+")
        || matches!(
            second_folded.as_str(),
            "and" | "or" | "oraz" | "i" | "&" | "+" | "vs" | "albo"
        );
    let third_is_other_agent = spans
        .get(idx + 2)
        .and_then(|s| match_agent_token(agents, s.raw))
        .is_some_and(|other| other.id != primary_id);
    is_connector && third_is_other_agent
}

fn extract_agent_addressed_request(
    agents: &[AgentDefinition],
    text: &str,
) -> Option<(AgentDefinition, String)> {
    extract_agent_addressed_request_with_options(agents, text, true)
}

fn extract_agent_addressed_request_with_options(
    agents: &[AgentDefinition],
    text: &str,
    allow_trailing_vocative: bool,
) -> Option<(AgentDefinition, String)> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    let spans = whitespace_word_spans(trimmed);
    if spans.is_empty() {
        return None;
    }

    let first_folded = fold_polish_ascii(spans[0].raw);

    // 1. `/agent <tok> <rest>`
    if first_folded == "/agent" && spans.len() >= 2 {
        let rest = trimmed[spans[1].end..]
            .trim_start_matches(|c: char| matches!(c, ':' | ',' | '-' | ' ' | '\t'))
            .trim();
        if !rest.is_empty() {
            let tok = spans[1].raw.strip_prefix('@').unwrap_or(spans[1].raw);
            if let Some(agent) = match_agent_token(agents, tok) {
                return Some((agent, rest.to_string()));
            }
        }
        return None;
    }

    // 2. `@<tok> <rest>`
    if let Some(after_at) = spans[0].raw.strip_prefix('@') {
        let rest = trimmed[spans[0].end..]
            .trim_start_matches(|c: char| matches!(c, ':' | ',' | '-' | ' ' | '\t'))
            .trim();
        if !rest.is_empty() {
            if let Some(agent) = match_agent_token(agents, after_at) {
                return Some((agent, rest.to_string()));
            }
        }
        return None;
    }

    // 3. Two-word & one-word English/Polish delegation verbs
    let mut verb_words = 0usize;
    if spans.len() >= 3 {
        let second_folded = fold_polish_ascii(trim_routing_punct(spans[1].raw));
        if matches!(
            (first_folded.as_str(), second_folded.as_str()),
            ("ask", "the")
                | ("tell", "the")
                | ("napisz", "do")
                | ("powiedz", "do")
                | ("przekaz", "do")
        ) {
            verb_words = 2;
        }
    }
    if verb_words == 0
        && spans.len() >= 2
        && matches!(
            first_folded.as_str(),
            "ask"
                | "tell"
                | "have"
                | "let"
                | "powiedz"
                | "zapytaj"
                | "spytaj"
                | "popros"
                | "niech"
                | "zlec"
                | "przekaz"
                | "napisz"
        )
    {
        verb_words = 1;
    }

    if verb_words > 0 && spans.len() > verb_words + 1 {
        let tok_raw = spans[verb_words]
            .raw
            .strip_prefix('@')
            .unwrap_or(spans[verb_words].raw);
        if let Some(agent) = match_agent_token(agents, tok_raw) {
            if !is_multi_agent_list_after(agents, &spans, verb_words, &agent.id) {
                let rest = strip_delegation_connectors(&trimmed[spans[verb_words].end..]);
                if !rest.is_empty() {
                    return Some((agent, rest.to_string()));
                }
            }
        }
    }

    // 4. Leading vocative `<agent> <rest>` (avoid hijacking multi-agent "A and B" / "A, B" prompts)
    if spans.len() >= 2 {
        if let Some(agent) = match_agent_token(agents, spans[0].raw) {
            if !is_multi_agent_list_after(agents, &spans, 0, &agent.id) {
                let rest = trimmed[spans[0].end..]
                    .trim_start_matches(|c: char| matches!(c, ':' | ',' | '-' | ' ' | '\t'))
                    .trim();
                if !rest.is_empty() {
                    return Some((agent, rest.to_string()));
                }
            }
        }
    }

    // 5. Trailing vocative `<text> <agent>` (skip in Direct mode or preposition-preceded nouns unless comma-separated)
    if allow_trailing_vocative && spans.len() >= 2 {
        let last = spans[spans.len() - 1];
        if let Some(agent) = match_agent_token(agents, last.raw) {
            let prev = spans[spans.len() - 2];
            let has_comma = prev.raw.ends_with(',')
                || last.raw.starts_with(',')
                || trimmed[prev.end..last.start].contains(',');
            let prev_folded = fold_polish_ascii(trim_routing_punct(prev.raw));
            let is_preposition = matches!(
                prev_folded.as_str(),
                "for"
                    | "with"
                    | "from"
                    | "of"
                    | "about"
                    | "by"
                    | "to"
                    | "dla"
                    | "od"
                    | "bez"
                    | "z"
                    | "ze"
                    | "przez"
                    | "o"
                    | "do"
                    | "na"
            );
            if has_comma || !is_preposition {
                let body = trimmed[..last.start]
                    .trim_end_matches(|c: char| matches!(c, ',' | ':' | '-' | ' ' | '\t'))
                    .trim();
                if !body.is_empty() {
                    return Some((agent, body.to_string()));
                }
            }
        }
    }

    None
}

fn is_casual_greeting_or_ping(user_content: &str) -> bool {
    let trimmed = user_content.trim();
    if trimmed.is_empty() {
        return false;
    }

    // Any dotted filename or domain token (e.g. index.html, helper.py, example.com) must route normally.
    for raw_tok in trimmed.split_whitespace() {
        let cleaned = trim_routing_punct(raw_tok);
        if let Some(dot_idx) = cleaned.find('.') {
            let before = &cleaned[..dot_idx];
            let after = &cleaned[dot_idx + 1..];
            if before.chars().any(char::is_alphanumeric) && after.chars().any(char::is_alphanumeric)
            {
                return false;
            }
        }
    }

    let folded = fold_polish_ascii(trimmed);
    let words: Vec<String> = folded
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();
    if words.is_empty() || words.len() > 6 {
        return false;
    }

    for w in &words {
        if matches!(
            w.as_str(),
            "code"
                | "kod"
                | "file"
                | "plik"
                | "app"
                | "aplikacja"
                | "aplikacje"
                | "game"
                | "gra"
                | "gry"
                | "html"
                | "css"
                | "js"
                | "rust"
                | "python"
                | "py"
                | "script"
                | "skrypt"
                | "search"
                | "szukaj"
                | "wyszukaj"
                | "web"
                | "internet"
                | "schedule"
                | "harmonogram"
                | "cron"
                | "timer"
                | "tool"
                | "tools"
                | "narzedzie"
                | "narzedzia"
                | "build"
                | "zbuduj"
                | "create"
                | "stworz"
                | "utworz"
                | "write"
                | "napisz"
                | "edit"
                | "edytuj"
                | "fix"
                | "napraw"
                | "debug"
                | "run"
                | "uruchom"
                | "exec"
                | "wykonaj"
                | "analyze"
                | "przeanalizuj"
                | "research"
                | "zbadaj"
                | "summarize"
                | "podsumuj"
                | "project"
                | "projekt"
                | "delete"
                | "usun"
                | "read"
                | "przeczytaj"
                | "workspace"
                | "flappy"
                | "programmer"
                | "researcher"
                | "executor"
                | "reviewer"
                | "sysadmin"
                | "historian"
                | "designer"
                | "agent"
        ) {
            return false;
        }
    }

    let filtered: Vec<&str> = words
        .iter()
        .map(String::as_str)
        .filter(|w| !matches!(*w, "commander" | "komander" | "bot" | "asystent" | "assistant"))
        .collect();
    if filtered.is_empty() {
        return words.iter().any(|w| w == "commander");
    }

    let joined = filtered.join(" ");
    const GREETINGS: &[&str] = &[
        "dzien dobry",
        "dobry wieczor",
        "dobranoc",
        "czesc",
        "hej",
        "hejka",
        "hejo",
        "siema",
        "siemanko",
        "elo",
        "witam",
        "witaj",
        "hello",
        "hi",
        "hey",
        "yo",
        "sup",
        "halo",
        "good morning",
        "good afternoon",
        "good evening",
        "good night",
        "test",
        "ping",
        "co slychac",
        "co tam",
        "jak sie masz",
        "how are you",
        "how r u",
        "whats up",
        "what s up",
        "wszystko ok",
        "dzieki",
        "dziekuje",
        "thanks",
        "thank you",
    ];
    GREETINGS.iter().any(|g| {
        joined == *g
            || joined
                .strip_prefix(g)
                .is_some_and(|tail| tail.is_empty() || tail.starts_with(' '))
    })
}

fn resolve_agent_routing_for_mode(
    agents: &[AgentDefinition],
    mode: &ConversationMode,
    user_content: &str,
) -> Option<(AgentDefinition, String)> {
    let allow_trailing_vocative = !matches!(mode, ConversationMode::Direct { .. });
    let (agent, rest) =
        extract_agent_addressed_request_with_options(agents, user_content, allow_trailing_vocative)?;
    let effective = format_direct_specialist_prompt(&agent, &rest);
    Some((agent, effective))
}

fn format_explicit_agent_prompt(agent: &AgentDefinition, raw_content: &str) -> String {
    if let Some((_, rest)) =
        extract_agent_addressed_request(std::slice::from_ref(agent), raw_content)
    {
        return format_direct_specialist_prompt(agent, &rest);
    }
    if agent.id == "commander" {
        raw_content.to_string()
    } else {
        format_direct_specialist_prompt(agent, raw_content)
    }
}

fn extract_image_data_urls(attachments: &[ChatAttachment]) -> Vec<String> {
    attachments
        .iter()
        .filter(|a| a.kind == "image" || a.mime_type.starts_with("image/"))
        .filter_map(|a| a.data_url.clone())
        .collect()
}

fn format_user_message_with_attachments(content: &str, attachments: &[ChatAttachment]) -> String {
    if attachments.is_empty() {
        return content.to_string();
    }
    let mut non_image_notes = Vec::new();
    for att in attachments {
        let is_img = att.kind == "image" || att.mime_type.starts_with("image/");
        if is_img {
            if let Some(ref wp) = att.workspace_path {
                non_image_notes.push(format!("- image `{}` (saved at `{}`)", att.name, wp));
            }
        } else if let Some(ref wp) = att.workspace_path {
            non_image_notes.push(format!(
                "- file `{}` ({}, {} bytes) saved in workspace at `{}` — inspect via `read_file`",
                att.name, att.mime_type, att.size_bytes, wp
            ));
        } else {
            non_image_notes.push(format!(
                "- file `{}` ({}, {} bytes)",
                att.name, att.mime_type, att.size_bytes
            ));
        }
    }
    if non_image_notes.is_empty() {
        return content.to_string();
    }
    if content.trim().is_empty() {
        format!("Attached files:\n{}", non_image_notes.join("\n"))
    } else {
        format!("{content}\n\nAttached files:\n{}", non_image_notes.join("\n"))
    }
}

fn join_continuation_parts(parts: &[String], tail: &str) -> String {
    if parts.is_empty() {
        return tail.to_string();
    }
    let mut out = parts.join("");
    out.push_str(tail);
    out
}

fn format_thinking_start_note(level: ThinkingLevel) -> &'static str {
    match level {
        ThinkingLevel::Off => "Generating response…",
        ThinkingLevel::Low => "Thinking (low)…",
        ThinkingLevel::Medium => "Thinking (medium)…",
        ThinkingLevel::High => "Thinking (high)…",
        ThinkingLevel::Max => "Thinking (max)…",
    }
}

fn has_unclosed_code_fence(text: &str) -> bool {
    let mut in_fence = false;
    for line in text.lines() {
        let trimmed = line.trim_start();
        if !in_fence {
            if let Some(after_open) = trimmed.strip_prefix("```") {
                if !after_open.contains("```") {
                    in_fence = true;
                }
            }
        } else if trimmed.starts_with("```") || trimmed.trim_end().ends_with("```") {
            in_fence = false;
        }
    }
    in_fence
}

struct FencedCodeBlock {
    start: usize,
    end: usize,
    lang: String,
    code: String,
    line_count: usize,
}

fn extract_all_fenced_script_blocks(content: &str, min_lines: usize) -> Vec<FencedCodeBlock> {
    let mut blocks = Vec::new();
    let mut cursor = 0usize;

    while let Some(rel_start) = content[cursor..].find("```") {
        let start = cursor + rel_start;
        let after_ticks = &content[start + 3..];
        let Some(nl_rel) = after_ticks.find('\n') else {
            break;
        };
        let lang = after_ticks[..nl_rel].trim().to_ascii_lowercase();
        if lang.contains('`') || lang.len() > 24 {
            cursor = start + 3 + nl_rel + 1;
            continue;
        }
        let body_start = start + 3 + nl_rel + 1;
        let (code, end) = if let Some(close_rel) = content[body_start..].find("```") {
            let code_end = body_start + close_rel;
            (&content[body_start..code_end], code_end + 3)
        } else {
            (&content[body_start..], content.len())
        };
        let line_count = code.lines().count();
        if line_count >= min_lines {
            blocks.push(FencedCodeBlock {
                start,
                end,
                lang,
                code: code.to_string(),
                line_count,
            });
        }
        cursor = end;
    }

    blocks
}

fn lang_extensions(lang: &str) -> &'static [&'static str] {
    match lang {
        "html" | "htm" => &[".html", ".htm"],
        "javascript" | "js" | "mjs" | "cjs" => &[".js", ".mjs", ".cjs"],
        "typescript" | "ts" => &[".ts", ".tsx"],
        "python" | "py" => &[".py"],
        "rust" | "rs" => &[".rs"],
        "css" => &[".css"],
        "json" => &[".json"],
        "markdown" | "md" => &[".md"],
        "bash" | "sh" => &[".sh"],
        "powershell" | "ps1" => &[".ps1"],
        "sql" => &[".sql"],
        "yaml" | "yml" => &[".yaml", ".yml"],
        "toml" => &[".toml"],
        "text" | "txt" => &[".txt"],
        _ => &[],
    }
}

fn infer_target_filename_for_block(prompt: &str, lang: &str, multiple_blocks: bool) -> String {
    const EXTS: [&str; 15] = [
        ".html", ".htm", ".js", ".ts", ".py", ".rs", ".css", ".json", ".md", ".txt", ".sh",
        ".ps1", ".sql", ".yaml", ".toml",
    ];
    let expected_exts = lang_extensions(lang);

    for raw_tok in prompt.split_whitespace() {
        let clean = raw_tok
            .trim_matches(|c: char| matches!(c, '`' | '"' | '\'' | ',' | ';' | '(' | ')' | '[' | ']' | ':'));
        let clean = clean.trim_end_matches('.');
        let lower = clean.to_ascii_lowercase();
        let ext_match = if multiple_blocks && !expected_exts.is_empty() {
            expected_exts.iter().any(|ext| lower.ends_with(ext))
        } else {
            EXTS.iter().any(|ext| lower.ends_with(ext))
        };
        if ext_match && clean.len() > 2 {
            let safe: String = clean
                .replace('\\', "/")
                .rsplit('/')
                .next()
                .unwrap_or(clean)
                .chars()
                .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_'))
                .collect();
            if !safe.is_empty() && !safe.starts_with('.') {
                return safe;
            }
        }
    }

    let lower = prompt.to_ascii_lowercase();
    if lower.contains("flappy") && matches!(lang, "" | "html" | "htm") {
        return "flappy-bird.html".to_string();
    }

    match lang {
        "html" | "htm" => "index.html".to_string(),
        "javascript" | "js" | "mjs" => "script.js".to_string(),
        "typescript" | "ts" => "script.ts".to_string(),
        "python" | "py" => "script.py".to_string(),
        "rust" | "rs" => "main.rs".to_string(),
        "css" => "styles.css".to_string(),
        "json" => "data.json".to_string(),
        "markdown" | "md" => "notes.md".to_string(),
        "bash" | "sh" => "script.sh".to_string(),
        "powershell" | "ps1" => "script.ps1".to_string(),
        "sql" => "query.sql".to_string(),
        "yaml" | "yml" => "config.yaml".to_string(),
        "toml" => "config.toml".to_string(),
        _ => {
            if !multiple_blocks
                && (lower.contains("html") || lower.contains("game") || lower.contains("webpage"))
            {
                "index.html".to_string()
            } else {
                "output.txt".to_string()
            }
        }
    }
}

fn disambiguate_filename(base: &str, used: &mut Vec<String>) -> String {
    if !used.iter().any(|u| u.eq_ignore_ascii_case(base)) {
        used.push(base.to_string());
        return base.to_string();
    }
    let (stem, ext) = match base.rfind('.') {
        Some(dot) if dot > 0 => (&base[..dot], &base[dot..]),
        _ => (base, ""),
    };
    let mut n = 2usize;
    loop {
        let candidate = format!("{stem}_{n}{ext}");
        if !used.iter().any(|u| u.eq_ignore_ascii_case(&candidate)) {
            used.push(candidate.clone());
            return candidate;
        }
        n += 1;
    }
}

fn resolve_duplicate_written_path(
    written_paths: &[String],
    prompt: &str,
    lang: &str,
) -> String {
    let expected = lang_extensions(lang);
    if !expected.is_empty() {
        if let Some(found) = written_paths
            .iter()
            .rev()
            .find(|p| expected.iter().any(|e| p.to_ascii_lowercase().ends_with(e)))
        {
            return found.clone();
        }
    }
    written_paths
        .last()
        .cloned()
        .unwrap_or_else(|| infer_target_filename_for_block(prompt, lang, false))
}

fn file_tool_action(tool_id: &str) -> Option<&'static str> {
    match tool_id {
        "write_file" | "file_write" => Some("write"),
        "delete_file" | "file_delete" => Some("delete"),
        "create_directory" | "file_mkdir" | "mkdir" => Some("mkdir"),
        _ => None,
    }
}

fn extract_tool_path(args: &serde_json::Value, output: &serde_json::Value) -> Option<String> {
    args.get("path")
        .and_then(|v| v.as_str())
        .or_else(|| output.get("path").and_then(|v| v.as_str()))
        .map(str::to_string)
}

fn tool_observation_msg(tc: &ToolCallRequest, value: serde_json::Value) -> InferenceMessage {
    InferenceMessage {
        role: "tool".to_string(),
        content: value.to_string(),
        tool_call_id: Some(tc.id.clone()),
        name: Some(tc.name.clone()),
        tool_calls: None,
        image_urls: Vec::new(),
    }
}

fn estimate_tokens(text: &str) -> u32 {
    ((text.len() / 4).max(1)) as u32
}

fn find_ci(haystack: &str, needle: &str, from: usize) -> Option<usize> {
    let sub = haystack.get(from..)?;
    let lower_sub = sub.to_ascii_lowercase();
    let lower_needle = needle.to_ascii_lowercase();
    lower_sub.find(&lower_needle).map(|idx| from + idx)
}

fn find_earliest_ci_tag(haystack: &str, tags: &[&str], from: usize) -> Option<(usize, usize)> {
    let mut best: Option<(usize, usize)> = None;
    for tag in tags {
        if let Some(pos) = find_ci(haystack, tag, from) {
            if best.is_none_or(|(best_pos, _)| pos < best_pos) {
                best = Some((pos, tag.len()));
            }
        }
    }
    best
}

fn extract_think_blocks_text(input: &str) -> String {
    const OPEN_TAGS: [&str; 2] = ["<think>", "<seed:think>"];
    const CLOSE_TAGS: [&str; 2] = ["</think>", "</seed:think>"];

    let mut cleaned = String::with_capacity(input.len());
    let mut cursor = 0usize;

    while cursor < input.len() {
        let open_opt = find_earliest_ci_tag(input, &OPEN_TAGS, cursor);
        let close_opt = find_earliest_ci_tag(input, &CLOSE_TAGS, cursor);

        if let Some((end, end_len)) = close_opt {
            if open_opt.is_none_or(|(open, _)| end < open) {
                let slice = &input[cursor..end];
                if !(cursor == 0 && cleaned.is_empty()) {
                    cleaned.push_str(slice);
                }
                cursor = end + end_len;
                continue;
            }
        }

        if let Some((start, start_len)) = open_opt {
            cleaned.push_str(&input[cursor..start]);
            let body_start = start + start_len;
            if let Some((end, end_len)) = find_earliest_ci_tag(input, &CLOSE_TAGS, body_start) {
                cursor = end + end_len;
            } else {
                // Unclosed <think>/<seed:think> tail (e.g. output truncated mid-reasoning):
                // drop the unclosed thinking block rather than leaking partial reasoning text.
                break;
            }
        } else {
            cleaned.push_str(&input[cursor..]);
            break;
        }
    }

    cleaned
}

fn extract_first_json_object(input: &str) -> Option<&str> {
    let start = input.find('{')?;
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;

    for (rel_idx, ch) in input[start..].char_indices() {
        if in_string {
            if escaped {
                escaped = false;
                continue;
            }
            match ch {
                '\\' => escaped = true,
                '"' => in_string = false,
                _ => {}
            }
            continue;
        }

        match ch {
            '"' => in_string = true,
            '{' => depth += 1,
            '}' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    let end = start + rel_idx + ch.len_utf8();
                    return Some(&input[start..end]);
                }
            }
            _ => {}
        }
    }
    None
}

fn strip_json_fences(raw: &str) -> &str {
    let trimmed = raw.trim();
    if let Some(rest) = trimmed.strip_prefix("```json") {
        if let Some(inner) = rest.strip_suffix("```") {
            return inner.trim();
        }
    } else if let Some(rest) = trimmed.strip_prefix("```") {
        if let Some(inner) = rest.strip_suffix("```") {
            return inner.trim();
        }
    }
    trimmed
}

fn builtin_orchestration_schemas() -> Vec<ToolDefinitionSchema> {
    vec![
        ToolDefinitionSchema {
            name: "delegate_task".to_string(),
            description: "Delegate a subtask to another enabled agent by agent_id and await its result.".to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "agent_id": { "type": "string" },
                    "task": { "type": "string" }
                },
                "required": ["agent_id", "task"]
            }),
        },
        ToolDefinitionSchema {
            name: "agent_message".to_string(),
            description: "Send a persistent A2A message to another agent via MessageBus.".to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "recipient_agent_id": { "type": "string" },
                    "message_type": { "type": "string" },
                    "content": { "type": "string" }
                },
                "required": ["recipient_agent_id", "content"]
            }),
        },
        ToolDefinitionSchema {
            name: "spawn_temp_agent".to_string(),
            description: "Create a temporary specialist agent (Commander only).".to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "id": { "type": "string" },
                    "name": { "type": "string" },
                    "description": { "type": "string" },
                    "system_prompt": { "type": "string" },
                    "enabled_tools": { "type": "array", "items": { "type": "string" } }
                },
                "required": ["name", "description", "system_prompt"]
            }),
        },
        ToolDefinitionSchema {
            name: "schedule_create".to_string(),
            description: "Create a recurring or one-shot scheduled task (once, interval, daily, or cron).".to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "title": { "type": "string" },
                    "prompt": { "type": "string" },
                    "kind": { "type": "string", "enum": ["once", "interval", "daily", "cron"] },
                    "run_at": { "type": "string", "description": "RFC3339 timestamp for one-shot schedules" },
                    "delay_secs": { "type": "integer", "description": "Delay in seconds for one-shot schedules when run_at is omitted" },
                    "interval_secs": { "type": "integer", "description": "Interval in seconds (>= 10)" },
                    "daily_time": { "type": "string", "description": "Time of day HH:MM or HH:MM:SS" },
                    "cron_expr": { "type": "string", "description": "5-field cron expression" },
                    "timezone_offset_mins": { "type": "integer" },
                    "target_agent_id": { "type": "string" },
                    "conversation_id": { "type": "string" },
                    "enabled": { "type": "boolean" }
                },
                "required": ["prompt"]
            }),
        },
        ToolDefinitionSchema {
            name: "schedule_list".to_string(),
            description: "List scheduled background tasks.".to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "limit": { "type": "integer" }
                }
            }),
        },
        ToolDefinitionSchema {
            name: "schedule_update".to_string(),
            description: "Update an existing scheduled background task by id.".to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "id": { "type": "string" },
                    "title": { "type": "string" },
                    "prompt": { "type": "string" },
                    "kind": { "type": "string", "enum": ["once", "interval", "daily", "cron"] },
                    "run_at": { "type": "string" },
                    "delay_secs": { "type": "integer" },
                    "interval_secs": { "type": "integer" },
                    "daily_time": { "type": "string" },
                    "cron_expr": { "type": "string" },
                    "timezone_offset_mins": { "type": "integer" },
                    "target_agent_id": { "type": "string" },
                    "enabled": { "type": "boolean" }
                },
                "required": ["id"]
            }),
        },
        ToolDefinitionSchema {
            name: "schedule_delete".to_string(),
            description: "Delete a scheduled background task by id.".to_string(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "id": { "type": "string" }
                },
                "required": ["id"]
            }),
        },
    ]
}
