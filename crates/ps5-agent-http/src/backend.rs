use async_trait::async_trait;
use chrono::{DateTime, Utc};
use ps5_agent_core::{
    AgentCardView, AgentDefinition, AgentEnvelope, AgentError, AgentMessageType, AgentModelConfig,
    AgentRole, ApprovalRecord, BrainFileDetail, BrainFileSummary, ChatAttachment, ChatMessage,
    Conversation, DoctorFixReport, DoctorReport, MemoryItem, MemoryPermissions, MetricsSnapshot,
    Ps5LmCapabilities, Result, ScheduleKind, ScheduleRecord, SlashCommandResponse,
    SystemOneRequest, SystemOneResponse, TaskRecord, ThinkingLevel, ToolExecutionRecord, ToolSpec,
    WebhookView,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy)]
pub struct DashboardAssets {
    pub index_html: &'static str,
    pub styles_css: &'static str,
    pub app_js: &'static str,
}

impl Default for DashboardAssets {
    fn default() -> Self {
        Self {
            index_html: DEFAULT_INDEX_HTML,
            styles_css: DEFAULT_STYLES_CSS,
            app_js: DEFAULT_APP_JS,
        }
    }
}

const DEFAULT_INDEX_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>PS5 Agent Dashboard</title>
  <link rel="stylesheet" href="/styles.css">
</head>
<body>
  <main id="app">
    <h1>PS5 Agent</h1>
    <p>Dashboard assets not bundled; API online at <code>/api/v1</code>.</p>
  </main>
  <script src="/app.js"></script>
</body>
</html>
"#;

const DEFAULT_STYLES_CSS: &str = r#"body { font-family: system-ui, sans-serif; background: #0f141c; color: #e6edf3; margin: 2rem; }"#;
const DEFAULT_APP_JS: &str = r#"console.log("ps5-agent default dashboard shell");"#;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SendMessageRequest {
    pub content: String,
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default)]
    pub thinking_level: Option<ThinkingLevel>,
    #[serde(default)]
    pub attachments: Vec<ChatAttachment>,
    #[serde(default)]
    pub async_dispatch: bool,
    #[serde(default)]
    pub immediate: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CancelConversationTasksRequest {
    #[serde(default)]
    pub agent_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendMessageResponse {
    pub user_message: ChatMessage,
    pub task: Option<TaskRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateAgentRequest {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default = "default_custom_role")]
    pub role: AgentRole,
    #[serde(default)]
    pub role_label: Option<String>,
    pub system_prompt: String,
    #[serde(default)]
    pub model_config: AgentModelConfig,
    #[serde(default)]
    pub enabled_tools: Vec<String>,
    #[serde(default)]
    pub denied_tools: Vec<String>,
    #[serde(default)]
    pub sandbox_mode: Option<String>,
    #[serde(default)]
    pub tool_profile: Option<String>,
    #[serde(default)]
    pub memory_permissions: MemoryPermissions,
    #[serde(default = "default_task_duration")]
    pub max_task_duration_secs: u64,
    #[serde(default = "default_reasoning_iters")]
    pub max_reasoning_iterations: u32,
    #[serde(default)]
    pub priority: i32,
    #[serde(default = "default_true")]
    pub can_communicate: bool,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub is_temporary: bool,
}

fn default_custom_role() -> AgentRole {
    AgentRole::Custom
}

fn default_task_duration() -> u64 {
    0
}

fn default_reasoning_iters() -> u32 {
    0
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UpdateAgentRequest {
    pub name: Option<String>,
    pub description: Option<String>,
    pub role_label: Option<String>,
    pub system_prompt: Option<String>,
    pub model_config: Option<AgentModelConfig>,
    pub enabled_tools: Option<Vec<String>>,
    pub denied_tools: Option<Vec<String>>,
    pub sandbox_mode: Option<String>,
    pub tool_profile: Option<String>,
    pub memory_permissions: Option<MemoryPermissions>,
    pub max_task_duration_secs: Option<u64>,
    pub max_reasoning_iterations: Option<u32>,
    pub priority: Option<i32>,
    pub can_communicate: Option<bool>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendAgentMessageRequest {
    pub sender_agent_id: String,
    pub recipient_agent_id: String,
    #[serde(default)]
    pub task_id: Option<String>,
    #[serde(default = "default_agent_message_type")]
    pub message_type: AgentMessageType,
    pub content: String,
}

fn default_agent_message_type() -> AgentMessageType {
    AgentMessageType::AgentMessage
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTaskRequest {
    #[serde(default)]
    pub conversation_id: Option<String>,
    pub user_request: String,
    #[serde(default)]
    pub assigned_agent_id: Option<String>,
    #[serde(default)]
    pub priority: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteToolRequest {
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default)]
    pub task_id: Option<String>,
    #[serde(default)]
    pub arguments: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceFileEntry {
    pub name: String,
    pub path: String,
    pub is_dir: bool,
    pub size_bytes: u64,
    pub modified_at: Option<String>,
    pub kind: String,
    pub previewable: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkspaceFilesResponse {
    pub current_path: String,
    pub root_label: String,
    pub entries: Vec<WorkspaceFileEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadWorkspaceFileRequest {
    pub path: String,
    #[serde(default)]
    pub content: Option<String>,
    #[serde(default)]
    pub content_base64: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MkdirRequest {
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkillInfoDto {
    pub id: String,
    pub name: String,
    pub description: String,
    pub enabled: bool,
    pub source: String,
    pub dir_path: String,
    pub has_scripts: bool,
    pub has_references: bool,
    pub resources: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginInfoDto {
    pub id: String,
    pub name: String,
    pub display_name: String,
    pub description: String,
    pub version: String,
    pub enabled: bool,
    pub rules_count: usize,
    pub skills_count: usize,
    pub tools: Vec<String>,
    pub has_mcp: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpToolDto {
    pub namespaced_id: String,
    pub server_id: String,
    pub tool_name: String,
    pub description: String,
    pub input_schema: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpServerDto {
    pub id: String,
    pub url: String,
    pub enabled: bool,
    pub trust_local: bool,
    pub has_auth: bool,
    pub masked_headers: BTreeMap<String, String>,
    pub status: String,
    pub last_error: Option<String>,
    pub last_synced_at: Option<String>,
    pub tools: Vec<McpToolDto>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ExtensionsOverviewResponse {
    pub skills: Vec<SkillInfoDto>,
    pub plugins: Vec<PluginInfoDto>,
    pub mcp_servers: Vec<McpServerDto>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportSkillRequest {
    #[serde(default)]
    pub slug: Option<String>,
    pub markdown: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportPluginRequest {
    pub manifest_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportMcpRequest {
    pub raw_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToggleExtensionRequest {
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkillDetailResponse {
    pub id: String,
    pub name: String,
    pub description: String,
    pub instructions: String,
    pub raw_markdown: String,
    pub resources: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReorderConversationsRequest {
    #[serde(default)]
    pub ordered_ids: Vec<String>,
    #[serde(default)]
    pub conversation_id: Option<String>,
    #[serde(default)]
    pub direction: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ForkConversationRequest {
    #[serde(default)]
    pub up_to_message_id: Option<String>,
    #[serde(default)]
    pub title: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MessageActionRequest {
    #[serde(default)]
    pub thinking_level: Option<ThinkingLevel>,
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default)]
    pub async_dispatch: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UpdateToolPolicyRequest {
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub requires_approval: Option<bool>,
    #[serde(default)]
    pub timeout_secs: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ImportMemoriesRequest(pub serde_json::Value);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpCreateSchedule {
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
    #[serde(default)]
    pub enabled: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HttpUpdateSchedule {
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub prompt: Option<String>,
    #[serde(default)]
    pub target_agent_id: Option<String>,
    #[serde(default)]
    pub kind: Option<ScheduleKind>,
    #[serde(default)]
    pub run_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub interval_secs: Option<u64>,
    #[serde(default)]
    pub daily_time: Option<String>,
    #[serde(default)]
    pub cron_expr: Option<String>,
    #[serde(default)]
    pub timezone_offset_mins: Option<i32>,
    #[serde(default)]
    pub enabled: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpScheduleAiRequest {
    pub prompt: String,
    #[serde(default)]
    pub timezone_offset_mins: i32,
    #[serde(default)]
    pub target_agent_id: Option<String>,
    #[serde(default)]
    pub auto_create: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpScheduleAiDraft {
    pub title: String,
    pub prompt: String,
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
    pub target_agent_id: Option<String>,
    #[serde(default)]
    pub timezone_offset_mins: i32,
    pub explanation: String,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpScheduleAiResponse {
    pub draft: HttpScheduleAiDraft,
    #[serde(default)]
    pub created: Option<ScheduleRecord>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HttpToggleScheduleRequest {
    #[serde(default)]
    pub enabled: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlashCommandRequest {
    pub command: String,
    #[serde(default)]
    pub agent_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SandboxStateDto {
    pub mode: String,
    pub approval_policy: String,
    pub network_access: bool,
    pub allow_local_network: bool,
    pub web_search_provider: String,
    pub searxng_url: String,
    pub has_brave_api_key: bool,
    pub has_tavily_api_key: bool,
    pub remote_browser_url: String,
    pub allow_host_shell: bool,
    pub max_exec_output_bytes: usize,
    pub exec_timeout_secs: u64,
    pub inject_brain_files: bool,
    pub max_brain_context_chars: usize,
    pub heartbeat_interval_secs: u64,
    pub allowed_domains: Vec<String>,
    pub blocked_domains: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UpdateSandboxRequest {
    pub mode: Option<String>,
    pub approval_policy: Option<String>,
    pub network_access: Option<bool>,
    pub allow_local_network: Option<bool>,
    pub web_search_provider: Option<String>,
    pub searxng_url: Option<String>,
    pub brave_api_key: Option<String>,
    pub tavily_api_key: Option<String>,
    pub remote_browser_url: Option<String>,
    pub allow_host_shell: Option<bool>,
    pub max_exec_output_bytes: Option<usize>,
    pub exec_timeout_secs: Option<u64>,
    pub inject_brain_files: Option<bool>,
    pub max_brain_context_chars: Option<usize>,
    pub heartbeat_interval_secs: Option<u64>,
    pub allowed_domains: Option<Vec<String>>,
    pub blocked_domains: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteBrainFileRequest {
    #[serde(default)]
    pub name: Option<String>,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateWebhookRequest {
    pub name: String,
    pub target_agent_id: String,
    #[serde(default)]
    pub conversation_id: Option<String>,
    #[serde(default)]
    pub prompt_template: String,
    #[serde(default)]
    pub enabled: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateWebhookResponse {
    pub webhook: WebhookView,
    pub secret: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookTriggerResponse {
    pub ok: bool,
    pub webhook_id: String,
    pub task_id: Option<String>,
    pub message: String,
}

/// Service boundary implemented by the wired runtime (`AppContext`) in Phase C,
/// or by lightweight mock/test backends for Milestone 3 HTTP & security tests.
#[async_trait]
pub trait HttpBackend: Send + Sync {
    async fn metrics(&self) -> MetricsSnapshot;

    async fn send_conversation_message(
        &self,
        conversation_id: &str,
        req: SendMessageRequest,
    ) -> Result<SendMessageResponse>;

    async fn list_agent_cards(&self) -> Result<Vec<AgentCardView>>;
    async fn get_agent_card(&self, agent_id: &str) -> Result<AgentCardView>;
    async fn create_agent(&self, req: CreateAgentRequest) -> Result<AgentDefinition>;
    async fn update_agent(&self, agent_id: &str, req: UpdateAgentRequest) -> Result<AgentDefinition>;
    async fn delete_agent(&self, agent_id: &str) -> Result<bool>;
    async fn set_agent_enabled(&self, agent_id: &str, enabled: bool) -> Result<AgentDefinition>;

    async fn send_agent_message(&self, req: SendAgentMessageRequest) -> Result<AgentEnvelope>;

    async fn create_task(&self, req: CreateTaskRequest) -> Result<TaskRecord>;
    async fn cancel_task(&self, task_id: &str) -> Result<TaskRecord>;

    async fn resolve_approval(
        &self,
        approval_id: &str,
        approved: bool,
        resolved_by: &str,
    ) -> Result<ApprovalRecord>;

    async fn list_tools(&self) -> Result<Vec<ToolSpec>>;
    async fn toggle_tool(&self, tool_id: &str, enabled: Option<bool>) -> Result<ToolSpec>;
    async fn execute_tool(
        &self,
        tool_id: &str,
        req: ExecuteToolRequest,
    ) -> Result<ToolExecutionRecord>;

    async fn apply_settings_update(
        &self,
        _updates: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<()> {
        Ok(())
    }

    fn current_config(&self) -> Option<ps5_agent_config::AppConfig> {
        None
    }

    async fn list_inference_models(&self) -> Result<Vec<String>> {
        Ok(Vec::new())
    }

    async fn evaluate_systemone(&self, _req: SystemOneRequest) -> Result<SystemOneResponse> {
        Err(AgentError::Inference(
            "Inference SystemOne endpoint not configured on backend".into(),
        ))
    }

    async fn list_workspace_files(&self, _path: Option<&str>) -> Result<WorkspaceFilesResponse> {
        Ok(WorkspaceFilesResponse::default())
    }

    async fn read_workspace_file(&self, _path: &str) -> Result<(String, Vec<u8>)> {
        Err(AgentError::InvalidInput(
            "Workspace files API not configured on backend".into(),
        ))
    }

    async fn write_workspace_file(&self, _path: &str, _bytes: &[u8]) -> Result<WorkspaceFileEntry> {
        Err(AgentError::InvalidInput(
            "Workspace files API not configured on backend".into(),
        ))
    }

    async fn create_workspace_folder(&self, _path: &str) -> Result<()> {
        Err(AgentError::InvalidInput(
            "Workspace files API not configured on backend".into(),
        ))
    }

    async fn delete_workspace_file(&self, _path: &str) -> Result<()> {
        Err(AgentError::InvalidInput(
            "Workspace files API not configured on backend".into(),
        ))
    }

    async fn list_extensions(&self) -> Result<ExtensionsOverviewResponse> {
        Ok(ExtensionsOverviewResponse::default())
    }

    async fn get_skill_detail(&self, id: &str) -> Result<SkillDetailResponse> {
        Err(AgentError::NotFound(format!("skill '{id}' not found")))
    }

    async fn import_skill(&self, _req: ImportSkillRequest) -> Result<SkillInfoDto> {
        Err(AgentError::InvalidInput(
            "Extensions API not configured on backend".into(),
        ))
    }

    async fn import_plugin(&self, _req: ImportPluginRequest) -> Result<PluginInfoDto> {
        Err(AgentError::InvalidInput(
            "Extensions API not configured on backend".into(),
        ))
    }

    async fn import_mcp_servers(&self, _req: ImportMcpRequest) -> Result<Vec<McpServerDto>> {
        Err(AgentError::InvalidInput(
            "Extensions API not configured on backend".into(),
        ))
    }

    async fn sync_mcp_server(&self, id: &str) -> Result<McpServerDto> {
        Err(AgentError::NotFound(format!("MCP server '{id}' not found")))
    }

    async fn set_extension_enabled(&self, _kind: &str, _id: &str, _enabled: bool) -> Result<()> {
        Err(AgentError::InvalidInput(
            "Extensions API not configured on backend".into(),
        ))
    }

    async fn delete_extension(&self, _kind: &str, _id: &str) -> Result<()> {
        Err(AgentError::InvalidInput(
            "Extensions API not configured on backend".into(),
        ))
    }

    async fn get_inference_capabilities(&self) -> Result<Ps5LmCapabilities> {
        Ok(Ps5LmCapabilities::default())
    }

    async fn sync_inference_capabilities(&self) -> Result<Ps5LmCapabilities> {
        Err(AgentError::Inference(
            "PS5LM capability sync not configured on backend".into(),
        ))
    }

    /// Imports live KV-cache and sampling settings from the configured PS5LM server.
    async fn import_ps5lm_kv_settings(&self, _actor: &str) -> Result<serde_json::Value> {
        Err(AgentError::Inference(
            "PS5LM KV settings import not configured on backend".into(),
        ))
    }

    async fn reorder_conversations(
        &self,
        _ordered_ids: Vec<String>,
        _conversation_id: Option<String>,
        _direction: Option<String>,
    ) -> Result<Vec<Conversation>> {
        Err(AgentError::InvalidInput(
            "Conversation reorder API not configured on backend".into(),
        ))
    }

    async fn fork_conversation(
        &self,
        _conv_id: &str,
        _up_to_message_id: Option<String>,
        _title: Option<String>,
    ) -> Result<Conversation> {
        Err(AgentError::InvalidInput(
            "Conversation fork API not configured on backend".into(),
        ))
    }

    async fn delete_chat_message(&self, _conv_id: &str, _msg_id: &str) -> Result<bool> {
        Err(AgentError::InvalidInput(
            "Delete chat message API not configured on backend".into(),
        ))
    }

    async fn retry_chat_message(
        &self,
        _conv_id: &str,
        _msg_id: &str,
        _thinking_level: Option<ThinkingLevel>,
        _agent_id: Option<String>,
        _async_dispatch: bool,
    ) -> Result<SendMessageResponse> {
        Err(AgentError::InvalidInput(
            "Retry chat message API not configured on backend".into(),
        ))
    }

    async fn regenerate_chat_message(
        &self,
        _conv_id: &str,
        _msg_id: &str,
        _thinking_level: Option<ThinkingLevel>,
        _async_dispatch: bool,
    ) -> Result<SendMessageResponse> {
        Err(AgentError::InvalidInput(
            "Regenerate chat message API not configured on backend".into(),
        ))
    }

    async fn continue_chat_message(
        &self,
        _conv_id: &str,
        _msg_id: &str,
        _thinking_level: Option<ThinkingLevel>,
    ) -> Result<SendMessageResponse> {
        Err(AgentError::InvalidInput(
            "Continue chat message API not configured on backend".into(),
        ))
    }

    async fn enable_all_agents(&self) -> Result<usize> {
        Err(AgentError::InvalidInput(
            "Enable all agents API not configured on backend".into(),
        ))
    }

    async fn create_or_import_tools(&self, _payload: serde_json::Value) -> Result<Vec<ToolSpec>> {
        Err(AgentError::InvalidInput(
            "Custom tools API not configured on backend".into(),
        ))
    }

    async fn update_tool_policy(
        &self,
        _tool_id: &str,
        _enabled: Option<bool>,
        _requires_approval: Option<bool>,
        _timeout_secs: Option<u64>,
    ) -> Result<ToolSpec> {
        Err(AgentError::InvalidInput(
            "Tool policy API not configured on backend".into(),
        ))
    }

    async fn delete_custom_tool(&self, _tool_id: &str) -> Result<bool> {
        Err(AgentError::InvalidInput(
            "Delete custom tool API not configured on backend".into(),
        ))
    }

    async fn import_memories(&self, _payload: serde_json::Value) -> Result<usize> {
        Err(AgentError::InvalidInput(
            "Import memories API not configured on backend".into(),
        ))
    }

    async fn export_memories(&self) -> Result<Vec<MemoryItem>> {
        Ok(Vec::new())
    }

    async fn cancel_conversation_tasks(
        &self,
        _conv_id: &str,
        _agent_id: Option<String>,
    ) -> Result<usize> {
        Err(AgentError::InvalidInput(
            "Cancel conversation tasks API not configured on backend".into(),
        ))
    }

    async fn expedite_task(&self, _task_id: &str) -> Result<TaskRecord> {
        Err(AgentError::InvalidInput(
            "Expedite task API not configured on backend".into(),
        ))
    }

    async fn list_schedules(&self) -> Result<Vec<ScheduleRecord>> {
        Ok(Vec::new())
    }

    async fn get_schedule(&self, _id: &str) -> Result<Option<ScheduleRecord>> {
        Ok(None)
    }

    async fn create_schedule(&self, _req: HttpCreateSchedule) -> Result<ScheduleRecord> {
        Err(AgentError::Internal(
            "Schedules API not configured on backend".into(),
        ))
    }

    async fn update_schedule(
        &self,
        _id: &str,
        _req: HttpUpdateSchedule,
    ) -> Result<ScheduleRecord> {
        Err(AgentError::Internal(
            "Schedules API not configured on backend".into(),
        ))
    }

    async fn delete_schedule(&self, _id: &str) -> Result<()> {
        Err(AgentError::Internal(
            "Schedules API not configured on backend".into(),
        ))
    }

    async fn toggle_schedule(
        &self,
        _id: &str,
        _enabled: Option<bool>,
    ) -> Result<ScheduleRecord> {
        Err(AgentError::Internal(
            "Schedules API not configured on backend".into(),
        ))
    }

    async fn run_schedule_now(&self, _id: &str) -> Result<ScheduleRecord> {
        Err(AgentError::Internal(
            "Schedules API not configured on backend".into(),
        ))
    }

    async fn parse_schedule_ai(&self, _req: HttpScheduleAiRequest) -> Result<HttpScheduleAiResponse> {
        Err(AgentError::Internal(
            "Schedules AI API not configured on backend".into(),
        ))
    }

    async fn run_doctor(&self) -> Result<DoctorReport> {
        Err(AgentError::Internal(
            "Doctor diagnostics API not configured on backend".into(),
        ))
    }

    async fn fix_doctor(&self) -> Result<DoctorFixReport> {
        Err(AgentError::Internal(
            "Doctor diagnostics API not configured on backend".into(),
        ))
    }

    async fn get_sandbox_state(&self) -> Result<SandboxStateDto> {
        Err(AgentError::Internal(
            "Sandbox API not configured on backend".into(),
        ))
    }

    async fn update_sandbox_policy(&self, _req: UpdateSandboxRequest) -> Result<SandboxStateDto> {
        Err(AgentError::Internal(
            "Sandbox API not configured on backend".into(),
        ))
    }

    async fn list_browser_sessions(&self) -> Result<serde_json::Value> {
        Ok(serde_json::json!({ "count": 0, "sessions": [] }))
    }

    async fn execute_browser_action(&self, _payload: serde_json::Value) -> Result<serde_json::Value> {
        Err(AgentError::Internal(
            "Remote browser API not configured on backend".into(),
        ))
    }

    async fn close_browser_session(&self, _session_id: &str) -> Result<bool> {
        Ok(false)
    }

    /// Returns `(session_id, sanitized_url, sanitized_html)` for an active sandbox browser session.
    async fn preview_browser_session(
        &self,
        _session_id: Option<&str>,
    ) -> Result<(String, String, String)> {
        Err(AgentError::NotFound(
            "No active browser session to preview".into(),
        ))
    }

    async fn list_brain_files(&self) -> Result<Vec<BrainFileSummary>> {
        Ok(Vec::new())
    }

    async fn read_brain_file(&self, _name: &str) -> Result<BrainFileDetail> {
        Err(AgentError::Internal(
            "Brain API not configured on backend".into(),
        ))
    }

    async fn write_brain_file(&self, _name: &str, _content: &str) -> Result<BrainFileDetail> {
        Err(AgentError::Internal(
            "Brain API not configured on backend".into(),
        ))
    }

    async fn execute_conversation_command(
        &self,
        _conversation_id: &str,
        _req: SlashCommandRequest,
    ) -> Result<SlashCommandResponse> {
        Err(AgentError::Internal(
            "Slash commands API not configured on backend".into(),
        ))
    }

    async fn list_webhooks(&self) -> Result<Vec<WebhookView>> {
        Ok(Vec::new())
    }

    async fn create_webhook(&self, _req: CreateWebhookRequest) -> Result<CreateWebhookResponse> {
        Err(AgentError::Internal(
            "Webhooks API not configured on backend".into(),
        ))
    }

    async fn delete_webhook(&self, _id: &str) -> Result<bool> {
        Ok(false)
    }

    async fn trigger_inbound_webhook(
        &self,
        _id: &str,
        _token: &str,
        _payload: serde_json::Value,
    ) -> Result<WebhookTriggerResponse> {
        Err(AgentError::Internal(
            "Webhooks API not configured on backend".into(),
        ))
    }
}
