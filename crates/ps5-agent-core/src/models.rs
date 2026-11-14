use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub fn now_iso() -> String {
    Utc::now().to_rfc3339()
}

pub fn now_utc() -> DateTime<Utc> {
    Utc::now()
}

pub fn new_id() -> String {
    Uuid::new_v4().to_string()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApiMode {
    ChatCompletions,
    Responses,
    #[serde(alias = "messages")]
    AnthropicMessages,
    Completions,
    #[serde(alias = "systemone")]
    SystemOne,
}

impl Default for ApiMode {
    fn default() -> Self {
        Self::ChatCompletions
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AgentRole {
    Commander,
    Researcher,
    Programmer,
    Analyst,
    Executor,
    Custom,
}

impl AgentRole {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Commander => "COMMANDER",
            Self::Researcher => "RESEARCHER",
            Self::Programmer => "PROGRAMMER",
            Self::Analyst => "ANALYST",
            Self::Executor => "EXECUTOR",
            Self::Custom => "CUSTOM",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryPermissions {
    pub read_own: bool,
    pub write_own: bool,
    pub read_shared: bool,
    pub write_shared: bool,
    pub read_user_prefs: bool,
    pub write_user_prefs: bool,
    pub cross_agent_read: bool,
}

impl Default for MemoryPermissions {
    fn default() -> Self {
        Self {
            read_own: true,
            write_own: true,
            read_shared: true,
            write_shared: true,
            read_user_prefs: true,
            write_user_prefs: false,
            cross_agent_read: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ThinkingLevel {
    Off,
    Low,
    #[default]
    Medium,
    High,
    Max,
}

impl ThinkingLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::Max => "max",
        }
    }
}

impl std::str::FromStr for ThinkingLevel {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "off" => Ok(Self::Off),
            "low" => Ok(Self::Low),
            "medium" => Ok(Self::Medium),
            "high" => Ok(Self::High),
            "max" => Ok(Self::Max),
            _ => Err(format!("invalid thinking level: {s}")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentModelConfig {
    pub provider: String,
    pub model_override: Option<String>,
    pub api_mode: ApiMode,
    pub temperature: f32,
    pub max_output_tokens: u32,
    pub context_limit: u32,
    #[serde(default)]
    pub thinking_level: ThinkingLevel,
}

impl Default for AgentModelConfig {
    fn default() -> Self {
        Self {
            provider: "ps5lm".to_string(),
            model_override: None,
            api_mode: ApiMode::ChatCompletions,
            temperature: 0.4,
            max_output_tokens: 1024,
            context_limit: 8192,
            thinking_level: ThinkingLevel::Medium,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentDefinition {
    pub id: String,
    pub name: String,
    pub description: String,
    pub role: AgentRole,
    pub role_label: String,
    pub system_prompt: String,
    pub model_config: AgentModelConfig,
    pub enabled_tools: Vec<String>,
    #[serde(default)]
    pub denied_tools: Vec<String>,
    #[serde(default)]
    pub sandbox_mode: Option<String>,
    #[serde(default)]
    pub tool_profile: Option<String>,
    pub memory_permissions: MemoryPermissions,
    pub max_task_duration_secs: u64,
    pub max_reasoning_iterations: u32,
    pub priority: i32,
    pub can_communicate: bool,
    pub enabled: bool,
    pub is_builtin: bool,
    pub is_protected: bool,
    pub is_temporary: bool,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AgentRuntimeStats {
    pub agent_id: String,
    pub current_task_id: Option<String>,
    pub last_activity_at: Option<String>,
    pub completed_tasks: u64,
    pub failed_tasks: u64,
    pub memory_entries: u64,
    pub memory_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentCardView {
    #[serde(flatten)]
    pub definition: AgentDefinition,
    pub stats: AgentRuntimeStats,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TaskStatus {
    Pending,
    Assigned,
    Running,
    Waiting,
    Completed,
    Failed,
    Cancelled,
}

impl TaskStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }

    pub fn is_active(self) -> bool {
        matches!(self, Self::Assigned | Self::Running | Self::Waiting)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRecord {
    pub id: String,
    pub conversation_id: Option<String>,
    pub user_request: String,
    pub assigned_agent_id: String,
    pub parent_task_id: Option<String>,
    pub priority: i32,
    pub created_at: String,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub status: TaskStatus,
    pub result: Option<String>,
    pub error_info: Option<String>,
    pub has_side_effects: bool,
    pub progress_pct: u8,
    pub progress_note: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskEvent {
    pub id: String,
    pub task_id: String,
    pub timestamp: String,
    pub event_type: String,
    pub details: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AgentMessageType {
    TaskAssignment,
    TaskUpdate,
    TaskCompleted,
    InformationRequest,
    InformationResponse,
    AgentMessage,
    ErrorReport,
    ApprovalRequest,
    ApprovalResponse,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DeliveryStatus {
    Queued,
    Delivered,
    Processed,
    Rejected,
    BlockedLoop,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentEnvelope {
    pub id: String,
    pub sender_agent_id: String,
    pub recipient_agent_id: String,
    pub task_id: Option<String>,
    pub timestamp: String,
    pub message_type: AgentMessageType,
    pub content: String,
    pub status: DeliveryStatus,
    pub hop_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ConversationMode {
    Direct { agent_id: String },
    Group { participant_agent_ids: Vec<String> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Conversation {
    pub id: String,
    pub title: String,
    pub mode: ConversationMode,
    pub created_at: String,
    pub updated_at: String,
    pub archived: bool,
    #[serde(default)]
    pub sort_order: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChatRole {
    User,
    Assistant,
    System,
    Tool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChatAttachment {
    pub kind: String,
    pub name: String,
    pub mime_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_path: Option<String>,
    #[serde(default)]
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub id: String,
    pub conversation_id: String,
    pub role: ChatRole,
    pub agent_id: Option<String>,
    pub content: String,
    pub reasoning: Option<String>,
    pub tool_calls_json: Option<String>,
    pub task_id: Option<String>,
    pub token_count: u32,
    pub latency_ms: Option<u64>,
    pub archived: bool,
    pub created_at: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<ChatAttachment>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryScope {
    Conversation,
    Agent,
    Project,
    Task,
    UserPreference,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryItem {
    pub id: String,
    pub scope: MemoryScope,
    pub namespace: String,
    pub owner_agent_id: Option<String>,
    pub conversation_id: Option<String>,
    pub task_id: Option<String>,
    pub key: String,
    pub content: String,
    pub summary: Option<String>,
    pub importance: u8,
    pub tags: Vec<String>,
    pub archived: bool,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolPermission {
    WebRead,
    WebWrite,
    FileRead,
    FileWrite,
    FileDelete,
    Utility,
    AgentComm,
    AgentDelegate,
    MemoryCrossRead,
    AgentSpawnTemp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolSpec {
    pub id: String,
    pub name: String,
    pub category: String,
    pub description: String,
    pub input_schema: serde_json::Value,
    pub output_schema: serde_json::Value,
    pub required_permissions: Vec<ToolPermission>,
    pub requires_approval: bool,
    pub is_mutating: bool,
    pub enabled: bool,
    pub timeout_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolExecutionRecord {
    pub id: String,
    pub tool_id: String,
    pub agent_id: String,
    pub task_id: Option<String>,
    pub input_json: String,
    pub output_json: Option<String>,
    pub success: bool,
    pub error: Option<String>,
    pub duration_ms: u64,
    pub created_at: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ApprovalStatus {
    Pending,
    Approved,
    Denied,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApprovalRecord {
    pub id: String,
    pub task_id: Option<String>,
    pub agent_id: String,
    pub operation: String,
    pub tool_id: Option<String>,
    pub arguments_json: String,
    pub reason: String,
    pub status: ApprovalStatus,
    pub created_at: String,
    pub resolved_at: Option<String>,
    pub resolved_by: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionRecord {
    pub id: String,
    pub token_sha256: String,
    pub csrf_token: String,
    pub username: String,
    pub client_ip: String,
    pub created_at: String,
    pub expires_at: String,
    pub revoked: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditLogEntry {
    pub id: String,
    pub timestamp: String,
    pub actor: String,
    pub action: String,
    pub target: String,
    pub severity: String,
    pub outcome: String,
    pub details_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeLimits {
    pub max_agents: usize,
    pub max_active_tasks: usize,
    pub max_queued_tasks: usize,
    pub max_concurrent_inference: usize,
    pub max_messages_per_task: u32,
    pub max_reasoning_iterations: u32,
}

impl Default for RuntimeLimits {
    fn default() -> Self {
        Self {
            max_agents: 5,
            max_active_tasks: 4,
            max_queued_tasks: 64,
            max_concurrent_inference: 1,
            max_messages_per_task: 20,
            max_reasoning_iterations: 8,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub uptime_secs: u64,
    pub storage_driver: String,
    pub inference_requests_total: u64,
    pub inference_errors_total: u64,
    pub inference_p50_ms: u64,
    pub inference_p95_ms: u64,
    pub last_inference_ms: u64,
    pub tokens_prompt_total: u64,
    pub tokens_completion_total: u64,
    pub http_requests_total: u64,
    pub http_avg_ms: u64,
    pub db_ops_total: u64,
    pub db_avg_us: u64,
    pub active_tasks: u64,
    pub queued_tasks: u64,
    pub completed_tasks: u64,
    pub failed_tasks: u64,
    pub memory_rss_bytes: Option<u64>,
    pub ps5_fw_version: Option<String>,
    pub ps5_model: Option<String>,
    pub ps5_cpu_temp_c: Option<i32>,
    pub ps5_soc_temp_c: Option<i32>,
    pub ps5lm_reachable: bool,
    pub ps5lm_loaded_model: Option<String>,
    pub network_degraded: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentTaskStats {
    pub completed: usize,
    pub failed: usize,
    pub active: usize,
    pub latest_active_task_id: Option<String>,
    pub last_activity: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskCountsSummary {
    pub total: usize,
    pub pending: usize,
    pub running: usize,
    pub waiting: usize,
    pub completed: usize,
    pub failed: usize,
    pub cancelled: usize,
    pub per_agent: std::collections::HashMap<String, AgentTaskStats>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct PruneReport {
    pub deleted_tasks: usize,
    pub deleted_task_events: usize,
    pub deleted_agent_messages: usize,
    pub deleted_tool_executions: usize,
    pub deleted_audit_logs: usize,
    pub deleted_chat_messages: usize,
    pub purged_sessions: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Ps5LmCapabilities {
    pub base_url: String,
    pub healthy: bool,
    pub loaded_model: Option<String>,
    pub available_models: Vec<String>,
    pub context_length: Option<u32>,
    pub total_slots: Option<u32>,
    pub supports_vision: bool,
    pub supports_thinking: bool,
    pub synced_at: Option<String>,
}

fn default_true_bool() -> bool {
    true
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScheduleKind {
    Once,
    Interval,
    Daily,
    Cron,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScheduleRecord {
    pub id: String,
    pub title: String,
    pub prompt: String,
    #[serde(default)]
    pub target_agent_id: Option<String>,
    #[serde(default)]
    pub conversation_id: Option<String>,
    pub kind: ScheduleKind,
    #[serde(default)]
    pub run_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub interval_secs: Option<u64>,
    #[serde(default)]
    pub daily_time: Option<String>,
    #[serde(default)]
    pub cron_expr: Option<String>,
    #[serde(default)]
    pub timezone_offset_mins: i32,
    #[serde(default = "default_true_bool")]
    pub enabled: bool,
    #[serde(default)]
    pub next_run_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub last_run_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub last_status: Option<String>,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub last_task_id: Option<String>,
    #[serde(default)]
    pub run_count: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SandboxMode {
    ReadOnly,
    WorkspaceWrite,
    #[default]
    FullSandbox,
}

impl SandboxMode {
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "read_only" | "readonly" | "read-only" => Some(Self::ReadOnly),
            "workspace_write" | "workspacewrite" | "workspace-write" => Some(Self::WorkspaceWrite),
            "full_sandbox" | "fullsandbox" | "full-sandbox" | "full" => Some(Self::FullSandbox),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ReadOnly => "read_only",
            Self::WorkspaceWrite => "workspace_write",
            Self::FullSandbox => "full_sandbox",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SandboxApprovalPolicy {
    #[default]
    FullAuto,
    OnRequest,
    AlwaysAsk,
    Never,
}

impl SandboxApprovalPolicy {
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "full_auto" | "fullauto" | "full-auto" | "auto" => Some(Self::FullAuto),
            "on_request" | "onrequest" | "on-request" => Some(Self::OnRequest),
            "always_ask" | "alwaysask" | "always-ask" => Some(Self::AlwaysAsk),
            "never" => Some(Self::Never),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::FullAuto => "full_auto",
            Self::OnRequest => "on_request",
            Self::AlwaysAsk => "always_ask",
            Self::Never => "never",
        }
    }
}

/// Maps legacy/alias tool names to their canonical built-in tool ID.
pub fn canonical_tool_id(id: &str) -> &str {
    match id.trim() {
        "file_read" => "read_file",
        "file_write" => "write_file",
        "file_list" => "list_files",
        "file_mkdir" | "mkdir" => "create_directory",
        "file_delete" => "delete_file",
        "read_skill" => "load_skill",
        "canvas" => "write_artifact",
        "patch" => "apply_patch",
        "exec" | "shell" | "bash" => "sandbox_exec",
        "search_files" | "grep" => "grep_search",
        "find_files" | "glob" => "glob_files",
        other => other,
    }
}

fn canonical_tool_aliases(canonical_id: &str) -> &'static [&'static str] {
    match canonical_id {
        "read_file" => &["read_file", "file_read"],
        "write_file" => &["write_file", "file_write"],
        "list_files" => &["list_files", "file_list"],
        "create_directory" => &["create_directory", "file_mkdir", "mkdir"],
        "delete_file" => &["delete_file", "file_delete"],
        "load_skill" => &["load_skill", "read_skill"],
        "write_artifact" => &["write_artifact", "canvas"],
        "apply_patch" => &["apply_patch", "patch"],
        "sandbox_exec" => &["sandbox_exec", "exec", "shell", "bash"],
        "grep_search" => &["grep_search", "search_files", "grep"],
        "glob_files" => &["glob_files", "find_files", "glob"],
        _ => &[],
    }
}

/// Expands OpenClaw tool group identifiers (e.g. `"group:fs"`, `"group:openclaw"`)
/// into their canonical member tool IDs.
pub fn expand_tool_group(group: &str) -> Option<&'static [&'static str]> {
    match group.trim() {
        "group:fs" => Some(&[
            "read_file",
            "write_file",
            "edit_file",
            "apply_patch",
            "create_directory",
            "delete_file",
            "list_files",
            "grep_search",
            "glob_files",
        ]),
        "group:web" => Some(&[
            "web_search",
            "web_fetch",
            "browser",
            "http_get",
            "json_api_request",
            "extract_html_text",
        ]),
        "group:runtime" => Some(&["sandbox_exec", "process"]),
        "group:memory" => Some(&[
            "memory_read",
            "memory_write",
            "memory_search",
            "memory_get",
            "read_agent_memory",
            "write_agent_memory",
        ]),
        "group:sessions" => Some(&[
            "sessions_list",
            "sessions_spawn",
            "sessions_send",
            "session_status",
        ]),
        "group:automation" => Some(&[
            "schedule_create",
            "schedule_list",
            "schedule_update",
            "schedule_delete",
        ]),
        "group:utility" => Some(&[
            "calculator",
            "json_parse_format",
            "timestamp_time_helper",
            "text_transform_summarize_prep",
            "system_info",
            "ps5_info",
            "doctor",
            "write_artifact",
        ]),
        "group:openclaw" => Some(&[
            "read_file",
            "write_file",
            "edit_file",
            "apply_patch",
            "create_directory",
            "delete_file",
            "list_files",
            "grep_search",
            "glob_files",
            "web_search",
            "web_fetch",
            "browser",
            "http_get",
            "json_api_request",
            "extract_html_text",
            "sandbox_exec",
            "process",
            "memory_read",
            "memory_write",
            "memory_search",
            "memory_get",
            "read_agent_memory",
            "write_agent_memory",
            "sessions_list",
            "sessions_spawn",
            "sessions_send",
            "session_status",
            "schedule_create",
            "schedule_list",
            "schedule_update",
            "schedule_delete",
            "calculator",
            "json_parse_format",
            "timestamp_time_helper",
            "text_transform_summarize_prep",
            "system_info",
            "ps5_info",
            "doctor",
            "write_artifact",
        ]),
        _ => None,
    }
}

fn tool_pattern_list_matches(
    patterns: &[String],
    raw_id: &str,
    canonical_id: &str,
    is_custom: bool,
) -> bool {
    let aliases = canonical_tool_aliases(canonical_id);
    patterns.iter().any(|raw_pat| {
        let pat = raw_pat.trim();
        if pat.is_empty() {
            return false;
        }
        if pat == "*" {
            return true;
        }
        if pat == "custom:*" {
            return is_custom;
        }
        if pat == "group:*" {
            return expand_tool_group("group:openclaw")
                .is_some_and(|members| members.iter().any(|&m| m == canonical_id || m == raw_id));
        }
        if let Some(members) = expand_tool_group(pat) {
            return members.iter().any(|&m| m == canonical_id || m == raw_id);
        }
        if let Some(prefix) = pat.strip_suffix('*') {
            return raw_id.starts_with(prefix)
                || canonical_id.starts_with(prefix)
                || aliases.iter().any(|alias| alias.starts_with(prefix));
        }
        let pat_canonical = canonical_tool_id(pat);
        pat == raw_id || pat == canonical_id || pat_canonical == canonical_id
    })
}

/// Evaluates whether an agent policy allows `tool_id` (of `category`).
/// Explicit/group/wildcard entries in `denied` always take precedence over `enabled`.
pub fn agent_allows_tool_with_policy(
    enabled: &[String],
    denied: &[String],
    tool_id: &str,
    category: &str,
) -> bool {
    let raw_id = tool_id.trim();
    if raw_id.is_empty() {
        return false;
    }
    let canonical_id = canonical_tool_id(raw_id);
    let is_custom = category.trim().eq_ignore_ascii_case("custom")
        || raw_id.starts_with("custom:")
        || canonical_id.starts_with("custom:");
    if tool_pattern_list_matches(denied, raw_id, canonical_id, is_custom) {
        return false;
    }
    tool_pattern_list_matches(enabled, raw_id, canonical_id, is_custom)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DoctorStatus {
    Pass,
    Warn,
    Fail,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DoctorCheckResult {
    pub id: String,
    pub category: String,
    pub title: String,
    pub status: DoctorStatus,
    pub summary: String,
    pub remediation: Option<String>,
    pub fixable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DoctorReport {
    pub overall_status: DoctorStatus,
    pub checked_at: String,
    pub pass_count: usize,
    pub warn_count: usize,
    pub fail_count: usize,
    pub checks: Vec<DoctorCheckResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DoctorFixReport {
    pub applied_fixes: Vec<String>,
    pub report: DoctorReport,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WebhookRecord {
    pub id: String,
    pub name: String,
    pub enabled: bool,
    pub secret_hash: String,
    pub secret_prefix: String,
    pub target_agent_id: String,
    pub conversation_id: Option<String>,
    pub prompt_template: String,
    pub created_at: String,
    pub updated_at: String,
    pub last_triggered_at: Option<String>,
    pub trigger_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WebhookView {
    pub id: String,
    pub name: String,
    pub enabled: bool,
    pub secret_prefix: String,
    pub target_agent_id: String,
    pub conversation_id: Option<String>,
    pub prompt_template: String,
    pub created_at: String,
    pub updated_at: String,
    pub last_triggered_at: Option<String>,
    pub trigger_count: u64,
}

impl From<WebhookRecord> for WebhookView {
    fn from(w: WebhookRecord) -> Self {
        Self {
            id: w.id,
            name: w.name,
            enabled: w.enabled,
            secret_prefix: w.secret_prefix,
            target_agent_id: w.target_agent_id,
            conversation_id: w.conversation_id,
            prompt_template: w.prompt_template,
            created_at: w.created_at,
            updated_at: w.updated_at,
            last_triggered_at: w.last_triggered_at,
            trigger_count: w.trigger_count,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BrainFileSummary {
    pub name: String,
    pub path: String,
    pub exists: bool,
    pub size_bytes: u64,
    pub updated_at: Option<String>,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BrainFileDetail {
    pub name: String,
    pub path: String,
    pub content: String,
    pub size_bytes: u64,
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SlashCommandResponse {
    pub command: String,
    pub ephemeral: bool,
    pub content: String,
    pub reasoning: Option<String>,
    pub agent_id: Option<String>,
    pub metadata: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sandbox_enums_parse_and_roundtrip() {
        assert_eq!(SandboxMode::parse("read_only"), Some(SandboxMode::ReadOnly));
        assert_eq!(
            SandboxMode::parse("Workspace-Write"),
            Some(SandboxMode::WorkspaceWrite)
        );
        assert_eq!(
            SandboxMode::parse("full_sandbox"),
            Some(SandboxMode::FullSandbox)
        );
        assert_eq!(SandboxMode::parse("invalid"), None);
        assert_eq!(SandboxMode::FullSandbox.as_str(), "full_sandbox");

        assert_eq!(
            SandboxApprovalPolicy::parse("full_auto"),
            Some(SandboxApprovalPolicy::FullAuto)
        );
        assert_eq!(
            SandboxApprovalPolicy::parse("on-request"),
            Some(SandboxApprovalPolicy::OnRequest)
        );
        assert_eq!(
            SandboxApprovalPolicy::parse("ALWAYS_ASK"),
            Some(SandboxApprovalPolicy::AlwaysAsk)
        );
        assert_eq!(
            SandboxApprovalPolicy::parse("never"),
            Some(SandboxApprovalPolicy::Never)
        );
        assert_eq!(SandboxApprovalPolicy::parse("unknown"), None);
        assert_eq!(SandboxApprovalPolicy::OnRequest.as_str(), "on_request");
    }

    #[test]
    fn tool_policy_canonical_groups_and_denials() {
        assert_eq!(canonical_tool_id("file_read"), "read_file");
        assert_eq!(canonical_tool_id("bash"), "sandbox_exec");
        assert_eq!(canonical_tool_id("grep"), "grep_search");
        assert_eq!(canonical_tool_id("glob"), "glob_files");
        assert_eq!(canonical_tool_id("canvas"), "write_artifact");
        assert_eq!(canonical_tool_id("patch"), "apply_patch");

        let fs_group = expand_tool_group("group:fs").expect("group:fs exists");
        assert!(fs_group.contains(&"read_file"));
        assert!(expand_tool_group("group:openclaw")
            .unwrap()
            .contains(&"schedule_create"));

        let enabled = vec![
            "group:fs".to_string(),
            "mcp:*".to_string(),
            "custom:*".to_string(),
        ];
        let denied = vec!["file_delete".to_string()];
        assert!(agent_allows_tool_with_policy(&enabled, &denied, "file_read", "fs"));
        assert!(agent_allows_tool_with_policy(&enabled, &denied, "read_file", "fs"));
        assert!(!agent_allows_tool_with_policy(&enabled, &denied, "delete_file", "fs"));
        assert!(agent_allows_tool_with_policy(&enabled, &denied, "mcp:srv:op", "mcp"));
        assert!(agent_allows_tool_with_policy(&enabled, &denied, "my_tool", "custom"));
        assert!(!agent_allows_tool_with_policy(&enabled, &denied, "sandbox_exec", "runtime"));

        let wildcard_enabled = vec!["*".to_string()];
        let group_denied = vec!["group:runtime".to_string()];
        assert!(!agent_allows_tool_with_policy(
            &wildcard_enabled,
            &group_denied,
            "bash",
            "runtime"
        ));
    }
}
