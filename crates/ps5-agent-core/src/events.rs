use crate::models::{InferenceTimings, ScheduleRecord};
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
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timings: Option<InferenceTimings>,
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
    BrowserActivity {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        conversation_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        task_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        agent_id: Option<String>,
        session_id: String,
        action: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        url: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        selector: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        x_pct: Option<f32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        y_pct: Option<f32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        w_pct: Option<f32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        h_pct: Option<f32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        scroll_y: Option<f32>,
        status: String,
        timestamp: String,
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
            Self::BrowserActivity { .. } => "browser_activity",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn browser_activity_and_chat_completed_timings_roundtrip() {
        let ev = StreamEvent::BrowserActivity {
            conversation_id: None,
            task_id: None,
            agent_id: None,
            session_id: "sess-1".to_string(),
            action: "click".to_string(),
            url: Some("https://example.com".to_string()),
            title: None,
            selector: Some("#submit".to_string()),
            x_pct: Some(50.0),
            y_pct: Some(25.0),
            w_pct: None,
            h_pct: None,
            scroll_y: None,
            status: "ok".to_string(),
            timestamp: "2026-11-14T11:00:00Z".to_string(),
        };
        assert_eq!(ev.event_name(), "browser_activity");

        let val = serde_json::to_value(&ev).unwrap();
        assert_eq!(val["type"], "browser_activity");
        assert!(val.get("conversation_id").is_none());
        assert!(val.get("w_pct").is_none());
        assert_eq!(val["session_id"], "sess-1");

        let legacy_completed: StreamEvent = serde_json::from_str(
            r#"{
                "type": "chat_completed",
                "conversation_id": "c1",
                "task_id": "t1",
                "agent_id": "a1",
                "message_id": "m1",
                "content": "hi"
            }"#,
        )
        .unwrap();
        match legacy_completed {
            StreamEvent::ChatCompleted { timings, .. } => assert!(timings.is_none()),
            other => panic!("unexpected variant: {:?}", other),
        }
    }
}
