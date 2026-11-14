use crate::models::ScheduleRecord;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamEvent {
    ChatToken {
        conversation_id: String,
        task_id: String,
        agent_id: String,
        delta: String,
    },
    ChatReasoning {
        conversation_id: String,
        task_id: String,
        agent_id: String,
        delta: String,
    },
    ChatCompleted {
        conversation_id: String,
        task_id: String,
        agent_id: String,
        message_id: String,
        content: String,
        #[serde(default)]
        reasoning: Option<String>,
    },
    ToolCallStarted {
        task_id: String,
        agent_id: String,
        tool_id: String,
        arguments_json: String,
    },
    ToolCallFinished {
        task_id: String,
        agent_id: String,
        tool_id: String,
        success: bool,
        duration_ms: u64,
    },
    TaskUpdated {
        #[serde(default)]
        conversation_id: Option<String>,
        task_id: String,
        agent_id: String,
        status: String,
        progress_pct: u8,
        progress_note: Option<String>,
    },
    AgentMessageSent {
        envelope_id: String,
        sender_agent_id: String,
        recipient_agent_id: String,
        message_type: String,
        status: String,
    },
    ApprovalRequested {
        approval_id: String,
        task_id: Option<String>,
        agent_id: String,
        operation: String,
        reason: String,
    },
    ApprovalResolved {
        approval_id: String,
        status: String,
    },
    AgentUpdated {
        agent_id: String,
        action: String,
    },
    SystemAlert {
        level: String,
        message: String,
    },
    FilesChanged {
        conversation_id: Option<String>,
        path: String,
        action: String,
    },
    Ps5LmSynced {
        loaded_model: Option<String>,
        context_length: Option<u32>,
        total_slots: Option<u32>,
    },
    ContinuationTriggered {
        conversation_id: String,
        task_id: String,
        agent_id: String,
        reason: String,
    },
    ScheduleUpdated {
        schedule: ScheduleRecord,
    },
    SandboxUpdated {
        mode: String,
        approval_policy: String,
        network_access: bool,
    },
    WebhookTriggered {
        webhook_id: String,
        name: String,
        task_id: String,
        conversation_id: String,
    },
}

impl StreamEvent {
    pub fn event_name(&self) -> &'static str {
        match self {
            Self::ChatToken { .. } => "chat_token",
            Self::ChatReasoning { .. } => "chat_reasoning",
            Self::ChatCompleted { .. } => "chat_completed",
            Self::ToolCallStarted { .. } => "tool_call_started",
            Self::ToolCallFinished { .. } => "tool_call_finished",
            Self::TaskUpdated { .. } => "task_updated",
            Self::AgentMessageSent { .. } => "agent_message_sent",
            Self::ApprovalRequested { .. } => "approval_requested",
            Self::ApprovalResolved { .. } => "approval_resolved",
            Self::AgentUpdated { .. } => "agent_updated",
            Self::SystemAlert { .. } => "system_alert",
            Self::FilesChanged { .. } => "files_changed",
            Self::Ps5LmSynced { .. } => "ps5_lm_synced",
            Self::ContinuationTriggered { .. } => "continuation_triggered",
            Self::ScheduleUpdated { .. } => "schedule_updated",
            Self::SandboxUpdated { .. } => "sandbox_updated",
            Self::WebhookTriggered { .. } => "webhook_triggered",
        }
    }
}
