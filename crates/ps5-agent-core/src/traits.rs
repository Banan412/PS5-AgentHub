use crate::error::Result;
use crate::models::*;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Persistence abstraction implemented by SqliteStorage and JsonlStorage.
pub trait StorageBackend: Send + Sync {
    fn driver_name(&self) -> &'static str;
    fn run_migrations(&self) -> Result<()>;
    fn integrity_check(&self) -> Result<String>;
    fn checkpoint(&self) -> Result<()>;

    // Agents
    fn upsert_agent(&self, agent: &AgentDefinition) -> Result<()>;
    fn get_agent(&self, agent_id: &str) -> Result<Option<AgentDefinition>>;
    fn list_agents(&self) -> Result<Vec<AgentDefinition>>;
    fn delete_agent(&self, agent_id: &str) -> Result<bool>;

    // Conversations & Chat Messages
    fn create_conversation(&self, conv: &Conversation) -> Result<()>;
    fn get_conversation(&self, conv_id: &str) -> Result<Option<Conversation>>;
    fn list_conversations(&self, include_archived: bool) -> Result<Vec<Conversation>>;
    fn delete_conversation(&self, conv_id: &str) -> Result<bool>;
    fn reorder_conversations(&self, _ordered_ids: &[String]) -> Result<()> {
        Ok(())
    }
    fn append_chat_message(&self, msg: &ChatMessage) -> Result<()>;
    fn list_chat_messages(&self, conv_id: &str, include_archived: bool, limit: usize) -> Result<Vec<ChatMessage>>;
    fn search_chat_messages(&self, query: &str, limit: usize) -> Result<Vec<ChatMessage>>;
    fn archive_old_chat_messages(&self, conv_id: &str, keep_recent: usize) -> Result<usize>;
    fn get_chat_message(&self, _msg_id: &str) -> Result<Option<ChatMessage>> {
        Ok(None)
    }
    fn update_chat_message(&self, _msg: &ChatMessage) -> Result<bool> {
        Ok(false)
    }
    fn delete_chat_message(&self, _msg_id: &str) -> Result<bool> {
        Ok(false)
    }
    fn delete_chat_messages_after(
        &self,
        _conversation_id: &str,
        _anchor_msg_id: &str,
        _inclusive: bool,
    ) -> Result<usize> {
        Ok(0)
    }

    // Tasks & Task Events
    fn upsert_task(&self, task: &TaskRecord) -> Result<()>;
    fn get_task(&self, task_id: &str) -> Result<Option<TaskRecord>>;
    fn list_tasks(&self, limit: usize) -> Result<Vec<TaskRecord>>;
    fn task_counts_summary(&self) -> Result<TaskCountsSummary>;
    fn append_task_event(&self, event: &TaskEvent) -> Result<()>;
    fn list_task_events(&self, task_id: &str) -> Result<Vec<TaskEvent>>;

    // Agent-to-Agent Messages
    fn append_agent_message(&self, msg: &AgentEnvelope) -> Result<()>;
    fn update_agent_message_status(&self, msg_id: &str, status: DeliveryStatus) -> Result<()>;
    fn list_agent_messages(&self, task_id: Option<&str>, agent_id: Option<&str>, limit: usize) -> Result<Vec<AgentEnvelope>>;
    fn count_task_agent_messages(&self, task_id: &str) -> Result<u32>;

    // Memories
    fn upsert_memory(&self, item: &MemoryItem) -> Result<()>;
    fn query_memories(
        &self,
        scope: Option<MemoryScope>,
        namespace: Option<&str>,
        owner_agent_id: Option<&str>,
        include_archived: bool,
        limit: usize,
    ) -> Result<Vec<MemoryItem>>;
    fn delete_memory(&self, memory_id: &str) -> Result<bool>;
    fn delete_agent_memories(&self, agent_id: &str) -> Result<usize>;
    fn memory_stats_for_agent(&self, agent_id: &str) -> Result<(u64, u64)>;

    // Tools & Executions
    fn upsert_tool_spec(&self, spec: &ToolSpec) -> Result<()>;
    fn list_tool_specs(&self) -> Result<Vec<ToolSpec>>;
    fn delete_tool_spec(&self, _tool_id: &str) -> Result<bool> {
        Ok(false)
    }
    fn record_tool_execution(&self, rec: &ToolExecutionRecord) -> Result<()>;
    fn list_tool_executions(&self, agent_id: Option<&str>, limit: usize) -> Result<Vec<ToolExecutionRecord>>;

    // Settings
    fn get_setting(&self, key: &str) -> Result<Option<String>>;
    fn set_setting(&self, key: &str, value: &str) -> Result<()>;
    fn list_settings(&self) -> Result<Vec<(String, String)>>;

    // Sessions
    fn create_session(&self, sess: &SessionRecord) -> Result<()>;
    fn get_session_by_hash(&self, token_sha256: &str) -> Result<Option<SessionRecord>>;
    fn revoke_session(&self, session_id: &str) -> Result<()>;
    fn purge_expired_sessions(&self, now_iso: &str) -> Result<usize>;

    // Approvals
    fn upsert_approval(&self, approval: &ApprovalRecord) -> Result<()>;
    fn get_approval(&self, approval_id: &str) -> Result<Option<ApprovalRecord>>;
    fn list_approvals(&self, pending_only: bool, limit: usize) -> Result<Vec<ApprovalRecord>>;

    // Audit Logs
    fn append_audit_log(&self, entry: &AuditLogEntry) -> Result<()>;
    fn list_audit_logs(&self, limit: usize) -> Result<Vec<AuditLogEntry>>;

    // Schedules
    fn upsert_schedule(&self, schedule: &ScheduleRecord) -> Result<()> {
        let _ = schedule;
        Ok(())
    }
    fn get_schedule(&self, id: &str) -> Result<Option<ScheduleRecord>> {
        let _ = id;
        Ok(None)
    }
    fn list_schedules(&self, limit: usize) -> Result<Vec<ScheduleRecord>> {
        let _ = limit;
        Ok(Vec::new())
    }
    fn list_due_schedules(&self, now: DateTime<Utc>, limit: usize) -> Result<Vec<ScheduleRecord>> {
        let cap = if limit == 0 { 64 } else { limit.min(512) };
        let mut due: Vec<ScheduleRecord> = self
            .list_schedules(cap.saturating_mul(8).max(256))?
            .into_iter()
            .filter(|s| s.enabled && s.next_run_at.is_some_and(|t| t <= now))
            .collect();
        due.sort_by(|a, b| a.next_run_at.cmp(&b.next_run_at).then_with(|| a.id.cmp(&b.id)));
        due.truncate(cap);
        Ok(due)
    }
    fn delete_schedule(&self, id: &str) -> Result<()> {
        let _ = id;
        Ok(())
    }

    // Webhooks
    fn list_webhooks(&self) -> Result<Vec<WebhookRecord>> {
        Ok(Vec::new())
    }
    fn get_webhook(&self, _id: &str) -> Result<Option<WebhookRecord>> {
        Ok(None)
    }
    fn upsert_webhook(&self, _hook: &WebhookRecord) -> Result<()> {
        Ok(())
    }
    fn delete_webhook(&self, _id: &str) -> Result<bool> {
        Ok(false)
    }

    // Maintenance & Retention
    fn prune_old_records(&self, retention_days: u32, max_rows_per_table: usize) -> Result<PruneReport> {
        let _ = (retention_days, max_rows_per_table);
        Ok(PruneReport::default())
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InferenceMessage {
    pub role: String,
    pub content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ToolCallRequest>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub image_urls: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolDefinitionSchema {
    pub name: String,
    pub description: String,
    pub parameters: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCallRequest {
    pub id: String,
    pub name: String,
    pub arguments: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceRequest {
    pub model: Option<String>,
    pub api_mode: ApiMode,
    pub messages: Vec<InferenceMessage>,
    pub tools: Vec<ToolDefinitionSchema>,
    pub temperature: f32,
    pub max_output_tokens: u32,
    pub context_limit: u32,
    pub stream: bool,
    #[serde(default)]
    pub thinking_level: ThinkingLevel,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TokenUsage {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceResponse {
    pub content: String,
    pub reasoning: Option<String>,
    pub tool_calls: Vec<ToolCallRequest>,
    pub model: String,
    pub finish_reason: Option<String>,
    pub usage: TokenUsage,
    pub latency_ms: u64,
}

#[derive(Debug, Clone)]
pub enum InferenceStreamChunk {
    TextDelta(String),
    ReasoningDelta(String),
    Completed(InferenceResponse),
    Error(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderHealth {
    pub reachable: bool,
    pub loaded_model: Option<String>,
    pub available_models: Vec<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemOneQuestion {
    #[serde(rename = "type")]
    pub question_type: String,
    pub instructions: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub criteria: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemOneRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub state: String,
    pub questions: std::collections::BTreeMap<String, SystemOneQuestion>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemOneResponse {
    pub model: String,
    pub answers: serde_json::Value,
    pub usage: TokenUsage,
    pub latency_ms: u64,
}

#[async_trait]
pub trait InferenceProvider: Send + Sync {
    async fn generate(
        &self,
        req: InferenceRequest,
        cancel: CancellationToken,
    ) -> Result<InferenceResponse>;

    async fn generate_stream(
        &self,
        req: InferenceRequest,
        tx: mpsc::Sender<InferenceStreamChunk>,
        cancel: CancellationToken,
    ) -> Result<InferenceResponse>;

    async fn system_one(
        &self,
        _req: SystemOneRequest,
        _cancel: CancellationToken,
    ) -> Result<SystemOneResponse> {
        Err(crate::error::AgentError::Inference(
            "provider does not implement /v1/systemone".into(),
        ))
    }

    async fn discover_capabilities(&self) -> Result<Option<Ps5LmCapabilities>> {
        Ok(None)
    }

    /// Re-submits conversation context with `n_predict = 0` and `cache_prompt = true`
    /// so `llama-server` encodes the completed turn into its KV cache immediately.
    async fn prefill_kv_cache(
        &self,
        _messages: Vec<InferenceMessage>,
        _model_override: Option<String>,
        _cancel: CancellationToken,
    ) -> Result<()> {
        Ok(())
    }

    async fn health_check(&self) -> ProviderHealth;
    async fn list_models(&self) -> Result<Vec<String>>;
}
