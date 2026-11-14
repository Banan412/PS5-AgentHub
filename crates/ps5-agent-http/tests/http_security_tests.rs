use async_trait::async_trait;
use axum::body::{to_bytes, Body};
use axum::extract::ConnectInfo;
use axum::http::{header, Method, Request, StatusCode};
use ps5_agent_config::{AppConfig, LoggingConfig};
use ps5_agent_core::{
    new_id, now_iso, AgentCardView, AgentDefinition, AgentEnvelope, AgentError, AgentRole,
    AgentRuntimeStats, ApprovalRecord, ApprovalStatus, AuditLogEntry, ChatMessage, ChatRole,
    Conversation, ConversationMode, DeliveryStatus, MemoryItem, MemoryPermissions, MemoryScope,
    MetricsSnapshot, Result as CoreResult, SessionRecord, StorageBackend, StreamEvent,
    TaskCountsSummary, TaskEvent, TaskRecord, TaskStatus, ToolExecutionRecord, ToolPermission,
    ToolSpec,
};
use ps5_agent_http::{
    build_router, ensure_bootstrap_credentials, serve_with_listener, sha256_hex,
    CreateAgentRequest, CreateTaskRequest, DashboardAssets, ExecuteToolRequest, HttpBackend,
    HttpState, RecoveringTcpListener, SendAgentMessageRequest, SendMessageRequest,
    SendMessageResponse, UpdateAgentRequest, WebhookTriggerResponse,
};
use ps5_agent_logging::AppLogger;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

#[derive(Default)]
struct MemoryStorage {
    agents: Mutex<HashMap<String, AgentDefinition>>,
    conversations: Mutex<HashMap<String, Conversation>>,
    messages: Mutex<Vec<ChatMessage>>,
    tasks: Mutex<HashMap<String, TaskRecord>>,
    task_events: Mutex<Vec<TaskEvent>>,
    agent_messages: Mutex<Vec<AgentEnvelope>>,
    memories: Mutex<HashMap<String, MemoryItem>>,
    tools: Mutex<HashMap<String, ToolSpec>>,
    tool_execs: Mutex<Vec<ToolExecutionRecord>>,
    settings: Mutex<HashMap<String, String>>,
    sessions: Mutex<HashMap<String, SessionRecord>>,
    approvals: Mutex<HashMap<String, ApprovalRecord>>,
    audit_logs: Mutex<Vec<AuditLogEntry>>,
}

impl StorageBackend for MemoryStorage {
    fn driver_name(&self) -> &'static str {
        "memory-mock"
    }
    fn run_migrations(&self) -> CoreResult<()> {
        Ok(())
    }
    fn integrity_check(&self) -> CoreResult<String> {
        Ok("ok".to_string())
    }
    fn checkpoint(&self) -> CoreResult<()> {
        Ok(())
    }

    fn upsert_agent(&self, agent: &AgentDefinition) -> CoreResult<()> {
        self.agents
            .lock()
            .unwrap()
            .insert(agent.id.clone(), agent.clone());
        Ok(())
    }
    fn get_agent(&self, agent_id: &str) -> CoreResult<Option<AgentDefinition>> {
        Ok(self.agents.lock().unwrap().get(agent_id).cloned())
    }
    fn list_agents(&self) -> CoreResult<Vec<AgentDefinition>> {
        Ok(self.agents.lock().unwrap().values().cloned().collect())
    }
    fn delete_agent(&self, agent_id: &str) -> CoreResult<bool> {
        Ok(self.agents.lock().unwrap().remove(agent_id).is_some())
    }

    fn create_conversation(&self, conv: &Conversation) -> CoreResult<()> {
        self.conversations
            .lock()
            .unwrap()
            .insert(conv.id.clone(), conv.clone());
        Ok(())
    }
    fn get_conversation(&self, conv_id: &str) -> CoreResult<Option<Conversation>> {
        Ok(self.conversations.lock().unwrap().get(conv_id).cloned())
    }
    fn list_conversations(&self, include_archived: bool) -> CoreResult<Vec<Conversation>> {
        Ok(self
            .conversations
            .lock()
            .unwrap()
            .values()
            .filter(|c| include_archived || !c.archived)
            .cloned()
            .collect())
    }
    fn delete_conversation(&self, conv_id: &str) -> CoreResult<bool> {
        Ok(self.conversations.lock().unwrap().remove(conv_id).is_some())
    }
    fn append_chat_message(&self, msg: &ChatMessage) -> CoreResult<()> {
        self.messages.lock().unwrap().push(msg.clone());
        Ok(())
    }
    fn list_chat_messages(
        &self,
        conv_id: &str,
        include_archived: bool,
        limit: usize,
    ) -> CoreResult<Vec<ChatMessage>> {
        Ok(self
            .messages
            .lock()
            .unwrap()
            .iter()
            .filter(|m| m.conversation_id == conv_id && (include_archived || !m.archived))
            .take(limit)
            .cloned()
            .collect())
    }
    fn search_chat_messages(&self, query: &str, limit: usize) -> CoreResult<Vec<ChatMessage>> {
        let q = query.to_ascii_lowercase();
        Ok(self
            .messages
            .lock()
            .unwrap()
            .iter()
            .filter(|m| m.content.to_ascii_lowercase().contains(&q))
            .take(limit)
            .cloned()
            .collect())
    }
    fn archive_old_chat_messages(&self, _conv_id: &str, _keep_recent: usize) -> CoreResult<usize> {
        Ok(0)
    }

    fn upsert_task(&self, task: &TaskRecord) -> CoreResult<()> {
        self.tasks
            .lock()
            .unwrap()
            .insert(task.id.clone(), task.clone());
        Ok(())
    }
    fn get_task(&self, task_id: &str) -> CoreResult<Option<TaskRecord>> {
        Ok(self.tasks.lock().unwrap().get(task_id).cloned())
    }
    fn list_tasks(&self, limit: usize) -> CoreResult<Vec<TaskRecord>> {
        Ok(self
            .tasks
            .lock()
            .unwrap()
            .values()
            .take(limit)
            .cloned()
            .collect())
    }
    fn task_counts_summary(&self) -> CoreResult<TaskCountsSummary> {
        Ok(TaskCountsSummary::default())
    }
    fn append_task_event(&self, event: &TaskEvent) -> CoreResult<()> {
        self.task_events.lock().unwrap().push(event.clone());
        Ok(())
    }
    fn list_task_events(&self, task_id: &str) -> CoreResult<Vec<TaskEvent>> {
        Ok(self
            .task_events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.task_id == task_id)
            .cloned()
            .collect())
    }

    fn append_agent_message(&self, msg: &AgentEnvelope) -> CoreResult<()> {
        self.agent_messages.lock().unwrap().push(msg.clone());
        Ok(())
    }
    fn update_agent_message_status(&self, msg_id: &str, status: DeliveryStatus) -> CoreResult<()> {
        if let Some(m) = self
            .agent_messages
            .lock()
            .unwrap()
            .iter_mut()
            .find(|m| m.id == msg_id)
        {
            m.status = status;
        }
        Ok(())
    }
    fn list_agent_messages(
        &self,
        task_id: Option<&str>,
        agent_id: Option<&str>,
        limit: usize,
    ) -> CoreResult<Vec<AgentEnvelope>> {
        Ok(self
            .agent_messages
            .lock()
            .unwrap()
            .iter()
            .filter(|m| task_id.is_none_or(|t| m.task_id.as_deref() == Some(t)))
            .filter(|m| {
                agent_id.is_none_or(|a| m.sender_agent_id == a || m.recipient_agent_id == a)
            })
            .take(limit)
            .cloned()
            .collect())
    }
    fn count_task_agent_messages(&self, task_id: &str) -> CoreResult<u32> {
        Ok(self
            .agent_messages
            .lock()
            .unwrap()
            .iter()
            .filter(|m| m.task_id.as_deref() == Some(task_id))
            .count() as u32)
    }

    fn upsert_memory(&self, item: &MemoryItem) -> CoreResult<()> {
        self.memories
            .lock()
            .unwrap()
            .insert(item.id.clone(), item.clone());
        Ok(())
    }
    fn query_memories(
        &self,
        scope: Option<MemoryScope>,
        namespace: Option<&str>,
        owner_agent_id: Option<&str>,
        include_archived: bool,
        limit: usize,
    ) -> CoreResult<Vec<MemoryItem>> {
        Ok(self
            .memories
            .lock()
            .unwrap()
            .values()
            .filter(|m| include_archived || !m.archived)
            .filter(|m| scope.is_none_or(|s| m.scope == s))
            .filter(|m| namespace.is_none_or(|ns| m.namespace == ns))
            .filter(|m| owner_agent_id.is_none_or(|a| m.owner_agent_id.as_deref() == Some(a)))
            .take(limit)
            .cloned()
            .collect())
    }
    fn delete_memory(&self, memory_id: &str) -> CoreResult<bool> {
        Ok(self.memories.lock().unwrap().remove(memory_id).is_some())
    }
    fn delete_agent_memories(&self, agent_id: &str) -> CoreResult<usize> {
        let mut g = self.memories.lock().unwrap();
        let before = g.len();
        g.retain(|_, m| m.owner_agent_id.as_deref() != Some(agent_id));
        Ok(before - g.len())
    }
    fn memory_stats_for_agent(&self, agent_id: &str) -> CoreResult<(u64, u64)> {
        let g = self.memories.lock().unwrap();
        let mut count = 0u64;
        let mut bytes = 0u64;
        for m in g.values() {
            if m.owner_agent_id.as_deref() == Some(agent_id) {
                count += 1;
                bytes += m.content.len() as u64;
            }
        }
        Ok((count, bytes))
    }

    fn upsert_tool_spec(&self, spec: &ToolSpec) -> CoreResult<()> {
        self.tools
            .lock()
            .unwrap()
            .insert(spec.id.clone(), spec.clone());
        Ok(())
    }
    fn list_tool_specs(&self) -> CoreResult<Vec<ToolSpec>> {
        Ok(self.tools.lock().unwrap().values().cloned().collect())
    }
    fn record_tool_execution(&self, rec: &ToolExecutionRecord) -> CoreResult<()> {
        self.tool_execs.lock().unwrap().push(rec.clone());
        Ok(())
    }
    fn list_tool_executions(
        &self,
        agent_id: Option<&str>,
        limit: usize,
    ) -> CoreResult<Vec<ToolExecutionRecord>> {
        Ok(self
            .tool_execs
            .lock()
            .unwrap()
            .iter()
            .filter(|e| agent_id.is_none_or(|a| e.agent_id == a))
            .take(limit)
            .cloned()
            .collect())
    }

    fn get_setting(&self, key: &str) -> CoreResult<Option<String>> {
        Ok(self.settings.lock().unwrap().get(key).cloned())
    }
    fn set_setting(&self, key: &str, value: &str) -> CoreResult<()> {
        self.settings
            .lock()
            .unwrap()
            .insert(key.to_string(), value.to_string());
        Ok(())
    }
    fn list_settings(&self) -> CoreResult<Vec<(String, String)>> {
        Ok(self
            .settings
            .lock()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }

    fn create_session(&self, sess: &SessionRecord) -> CoreResult<()> {
        self.sessions
            .lock()
            .unwrap()
            .insert(sess.token_sha256.clone(), sess.clone());
        Ok(())
    }
    fn get_session_by_hash(&self, token_sha256: &str) -> CoreResult<Option<SessionRecord>> {
        Ok(self.sessions.lock().unwrap().get(token_sha256).cloned())
    }
    fn revoke_session(&self, session_id: &str) -> CoreResult<()> {
        for sess in self.sessions.lock().unwrap().values_mut() {
            if sess.id == session_id {
                sess.revoked = true;
            }
        }
        Ok(())
    }
    fn purge_expired_sessions(&self, _now_iso: &str) -> CoreResult<usize> {
        Ok(0)
    }

    fn upsert_approval(&self, approval: &ApprovalRecord) -> CoreResult<()> {
        self.approvals
            .lock()
            .unwrap()
            .insert(approval.id.clone(), approval.clone());
        Ok(())
    }
    fn get_approval(&self, approval_id: &str) -> CoreResult<Option<ApprovalRecord>> {
        Ok(self.approvals.lock().unwrap().get(approval_id).cloned())
    }
    fn list_approvals(&self, pending_only: bool, limit: usize) -> CoreResult<Vec<ApprovalRecord>> {
        Ok(self
            .approvals
            .lock()
            .unwrap()
            .values()
            .filter(|a| !pending_only || a.status == ApprovalStatus::Pending)
            .take(limit)
            .cloned()
            .collect())
    }

    fn append_audit_log(&self, entry: &AuditLogEntry) -> CoreResult<()> {
        self.audit_logs.lock().unwrap().push(entry.clone());
        Ok(())
    }
    fn list_audit_logs(&self, limit: usize) -> CoreResult<Vec<AuditLogEntry>> {
        Ok(self
            .audit_logs
            .lock()
            .unwrap()
            .iter()
            .rev()
            .take(limit)
            .cloned()
            .collect())
    }
}

struct MockBackend {
    store: Arc<MemoryStorage>,
    events_tx: broadcast::Sender<StreamEvent>,
}

#[async_trait]
impl HttpBackend for MockBackend {
    async fn metrics(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            uptime_secs: 42,
            storage_driver: self.store.driver_name().to_string(),
            ps5lm_reachable: true,
            ps5lm_loaded_model: Some("qwen-test".to_string()),
            ..Default::default()
        }
    }

    async fn send_conversation_message(
        &self,
        conversation_id: &str,
        req: SendMessageRequest,
    ) -> CoreResult<SendMessageResponse> {
        let conv = self
            .store
            .get_conversation(conversation_id)?
            .ok_or_else(|| AgentError::NotFound(format!("Conversation {conversation_id} not found")))?;
        let target_agent = req.agent_id.unwrap_or_else(|| match conv.mode {
            ConversationMode::Direct { agent_id } => agent_id,
            ConversationMode::Group {
                participant_agent_ids,
            } => participant_agent_ids
                .into_iter()
                .next()
                .unwrap_or_else(|| "commander".to_string()),
        });
        let task_id = new_id();
        let msg = ChatMessage {
            id: new_id(),
            conversation_id: conversation_id.to_string(),
            role: ChatRole::User,
            agent_id: None,
            content: req.content.clone(),
            reasoning: None,
            tool_calls_json: None,
            task_id: Some(task_id.clone()),
            token_count: 4,
            latency_ms: None,
            archived: false,
            attachments: Vec::new(),
            created_at: now_iso(),
        };
        self.store.append_chat_message(&msg)?;

        let task = TaskRecord {
            id: task_id.clone(),
            conversation_id: Some(conversation_id.to_string()),
            user_request: req.content,
            assigned_agent_id: target_agent,
            parent_task_id: None,
            priority: 1,
            created_at: now_iso(),
            started_at: None,
            completed_at: None,
            status: TaskStatus::Pending,
            result: None,
            error_info: None,
            has_side_effects: false,
            progress_pct: 0,
            progress_note: None,
        };
        self.store.upsert_task(&task)?;
        self.store.append_task_event(&TaskEvent {
            id: new_id(),
            task_id,
            timestamp: now_iso(),
            event_type: "created".to_string(),
            details: "Task queued".to_string(),
        })?;

        Ok(SendMessageResponse {
            user_message: msg,
            task: Some(task),
        })
    }

    async fn list_agent_cards(&self) -> CoreResult<Vec<AgentCardView>> {
        let agents = self.store.list_agents()?;
        Ok(agents
            .into_iter()
            .map(|definition| AgentCardView {
                stats: AgentRuntimeStats {
                    agent_id: definition.id.clone(),
                    ..Default::default()
                },
                definition,
            })
            .collect())
    }

    async fn get_agent_card(&self, agent_id: &str) -> CoreResult<AgentCardView> {
        let def = self
            .store
            .get_agent(agent_id)?
            .ok_or_else(|| AgentError::NotFound(format!("Agent {agent_id} not found")))?;
        Ok(AgentCardView {
            stats: AgentRuntimeStats {
                agent_id: def.id.clone(),
                ..Default::default()
            },
            definition: def,
        })
    }

    async fn create_agent(&self, req: CreateAgentRequest) -> CoreResult<AgentDefinition> {
        let now = now_iso();
        let def = AgentDefinition {
            id: new_id(),
            name: req.name,
            description: req.description,
            role_label: req
                .role_label
                .unwrap_or_else(|| req.role.as_str().to_string()),
            role: req.role,
            system_prompt: req.system_prompt,
            model_config: req.model_config,
            enabled_tools: req.enabled_tools,
            denied_tools: req.denied_tools,
            sandbox_mode: req.sandbox_mode,
            tool_profile: req.tool_profile,
            memory_permissions: req.memory_permissions,
            max_task_duration_secs: req.max_task_duration_secs,
            max_reasoning_iterations: req.max_reasoning_iterations,
            priority: req.priority,
            can_communicate: req.can_communicate,
            enabled: req.enabled,
            is_builtin: false,
            is_protected: false,
            is_temporary: req.is_temporary,
            created_at: now.clone(),
            updated_at: now,
        };
        self.store.upsert_agent(&def)?;
        Ok(def)
    }

    async fn update_agent(&self, agent_id: &str, req: UpdateAgentRequest) -> CoreResult<AgentDefinition> {
        let mut def = self
            .store
            .get_agent(agent_id)?
            .ok_or_else(|| AgentError::NotFound(format!("Agent {agent_id} not found")))?;
        if let Some(name) = req.name {
            def.name = name;
        }
        if let Some(desc) = req.description {
            def.description = desc;
        }
        if let Some(enabled) = req.enabled {
            def.enabled = enabled;
        }
        if let Some(denied) = req.denied_tools {
            def.denied_tools = denied;
        }
        if let Some(sandbox_mode) = req.sandbox_mode {
            def.sandbox_mode = Some(sandbox_mode);
        }
        if let Some(tool_profile) = req.tool_profile {
            def.tool_profile = Some(tool_profile);
        }
        def.updated_at = now_iso();
        self.store.upsert_agent(&def)?;
        Ok(def)
    }

    async fn delete_agent(&self, agent_id: &str) -> CoreResult<bool> {
        if let Some(def) = self.store.get_agent(agent_id)? {
            if def.is_protected {
                return Err(AgentError::Protected("Cannot delete protected agent".into()));
            }
            return self.store.delete_agent(agent_id);
        }
        Ok(false)
    }

    async fn set_agent_enabled(&self, agent_id: &str, enabled: bool) -> CoreResult<AgentDefinition> {
        let mut def = self
            .store
            .get_agent(agent_id)?
            .ok_or_else(|| AgentError::NotFound(format!("Agent {agent_id} not found")))?;
        if def.is_protected && !enabled {
            return Err(AgentError::Protected(
                "Cannot disable protected agent".into(),
            ));
        }
        def.enabled = enabled;
        def.updated_at = now_iso();
        self.store.upsert_agent(&def)?;
        Ok(def)
    }

    async fn send_agent_message(&self, req: SendAgentMessageRequest) -> CoreResult<AgentEnvelope> {
        let env = AgentEnvelope {
            id: new_id(),
            sender_agent_id: req.sender_agent_id,
            recipient_agent_id: req.recipient_agent_id,
            task_id: req.task_id,
            timestamp: now_iso(),
            message_type: req.message_type,
            content: req.content,
            status: DeliveryStatus::Delivered,
            hop_count: 1,
        };
        self.store.append_agent_message(&env)?;
        let _ = self.events_tx.send(StreamEvent::AgentMessageSent {
            envelope_id: env.id.clone(),
            sender_agent_id: env.sender_agent_id.clone(),
            recipient_agent_id: env.recipient_agent_id.clone(),
            message_type: "AGENT_MESSAGE".to_string(),
            status: "DELIVERED".to_string(),
        });
        Ok(env)
    }

    async fn create_task(&self, req: CreateTaskRequest) -> CoreResult<TaskRecord> {
        let task = TaskRecord {
            id: new_id(),
            conversation_id: req.conversation_id,
            user_request: req.user_request,
            assigned_agent_id: req
                .assigned_agent_id
                .unwrap_or_else(|| "commander".to_string()),
            parent_task_id: None,
            priority: req.priority.unwrap_or(1),
            created_at: now_iso(),
            started_at: None,
            completed_at: None,
            status: TaskStatus::Pending,
            result: None,
            error_info: None,
            has_side_effects: false,
            progress_pct: 0,
            progress_note: None,
        };
        self.store.upsert_task(&task)?;
        self.store.append_task_event(&TaskEvent {
            id: new_id(),
            task_id: task.id.clone(),
            timestamp: now_iso(),
            event_type: "created".to_string(),
            details: "Task created".to_string(),
        })?;
        Ok(task)
    }

    async fn cancel_task(&self, task_id: &str) -> CoreResult<TaskRecord> {
        let mut task = self
            .store
            .get_task(task_id)?
            .ok_or_else(|| AgentError::NotFound(format!("Task {task_id} not found")))?;
        task.status = TaskStatus::Cancelled;
        task.completed_at = Some(now_iso());
        self.store.upsert_task(&task)?;
        Ok(task)
    }

    async fn resolve_approval(
        &self,
        approval_id: &str,
        approved: bool,
        resolved_by: &str,
    ) -> CoreResult<ApprovalRecord> {
        let mut rec = self
            .store
            .get_approval(approval_id)?
            .ok_or_else(|| AgentError::NotFound(format!("Approval {approval_id} not found")))?;
        rec.status = if approved {
            ApprovalStatus::Approved
        } else {
            ApprovalStatus::Denied
        };
        rec.resolved_at = Some(now_iso());
        rec.resolved_by = Some(resolved_by.to_string());
        self.store.upsert_approval(&rec)?;
        Ok(rec)
    }

    async fn list_tools(&self) -> CoreResult<Vec<ToolSpec>> {
        self.store.list_tool_specs()
    }

    async fn toggle_tool(&self, tool_id: &str, enabled: Option<bool>) -> CoreResult<ToolSpec> {
        let mut tools = self.store.list_tool_specs()?;
        let Some(tool) = tools.iter_mut().find(|t| t.id == tool_id) else {
            return Err(AgentError::NotFound(format!("Tool {tool_id} not found")));
        };
        tool.enabled = enabled.unwrap_or(!tool.enabled);
        let updated = tool.clone();
        self.store.upsert_tool_spec(&updated)?;
        Ok(updated)
    }

    async fn execute_tool(
        &self,
        tool_id: &str,
        req: ExecuteToolRequest,
    ) -> CoreResult<ToolExecutionRecord> {
        let tools = self.store.list_tool_specs()?;
        if !tools.iter().any(|t| t.id == tool_id) {
            return Err(AgentError::NotFound(format!("Tool {tool_id} not found")));
        }
        let rec = ToolExecutionRecord {
            id: new_id(),
            tool_id: tool_id.to_string(),
            agent_id: req.agent_id.unwrap_or_else(|| "commander".to_string()),
            task_id: req.task_id,
            input_json: req.arguments.to_string(),
            output_json: Some(json!({ "ok": true }).to_string()),
            success: true,
            error: None,
            duration_ms: 3,
            created_at: now_iso(),
        };
        self.store.record_tool_execution(&rec)?;
        Ok(rec)
    }

    async fn trigger_inbound_webhook(
        &self,
        id: &str,
        token: &str,
        _payload: Value,
    ) -> CoreResult<WebhookTriggerResponse> {
        if token != "wh-secret-123" {
            return Err(AgentError::PermissionDenied("Invalid webhook token".into()));
        }
        Ok(WebhookTriggerResponse {
            ok: true,
            webhook_id: id.to_string(),
            task_id: Some("task-wh-1".to_string()),
            message: "Webhook triggered".to_string(),
        })
    }
}

struct TestHarness {
    temp_dir: PathBuf,
    state: Arc<HttpState>,
    store: Arc<MemoryStorage>,
    password: String,
}

impl Drop for TestHarness {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.temp_dir);
    }
}

fn new_temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("ps5-http-{name}-{}", new_id()));
    let _ = std::fs::create_dir_all(&dir);
    dir
}

fn setup_harness(name: &str, mut tweak: impl FnMut(&mut AppConfig)) -> TestHarness {
    let temp_dir = new_temp_dir(name);
    let mut cfg = AppConfig::with_data_dir(temp_dir.clone());
    // Fast Argon2id memory for unit test speed while exercising full Argon2id codepath.
    cfg.security.argon2_memory_kib = 4096;
    cfg.logging = LoggingConfig {
        level: "info".to_string(),
        log_dir: temp_dir.join("logs"),
        max_file_bytes: 512 * 1024,
        ring_buffer_capacity: 128,
        enable_klog: false,
        enable_toast: false,
    };
    tweak(&mut cfg);

    let logger = AppLogger::init(&cfg.logging);
    let store = Arc::new(MemoryStorage::default());

    // Seed built-in commander agent, a tool, and two pending approvals for roundtrip tests.
    let now = now_iso();
    store
        .upsert_agent(&AgentDefinition {
            id: "commander".to_string(),
            name: "Commander".to_string(),
            description: "Lead orchestrator".to_string(),
            role: AgentRole::Commander,
            role_label: "COMMANDER".to_string(),
            system_prompt: "Lead".to_string(),
            model_config: Default::default(),
            enabled_tools: vec!["file_read".to_string()],
            denied_tools: vec![],
            sandbox_mode: None,
            tool_profile: None,
            memory_permissions: MemoryPermissions::default(),
            max_task_duration_secs: 120,
            max_reasoning_iterations: 8,
            priority: 10,
            can_communicate: true,
            enabled: true,
            is_builtin: true,
            is_protected: true,
            is_temporary: false,
            created_at: now.clone(),
            updated_at: now.clone(),
        })
        .unwrap();

    store
        .upsert_tool_spec(&ToolSpec {
            id: "file_read".to_string(),
            name: "Read File".to_string(),
            category: "filesystem".to_string(),
            description: "Reads a file".to_string(),
            input_schema: json!({ "type": "object" }),
            output_schema: json!({ "type": "object" }),
            required_permissions: vec![ToolPermission::FileRead],
            requires_approval: false,
            is_mutating: false,
            enabled: true,
            timeout_secs: 10,
        })
        .unwrap();

    for idx in 1..=2 {
        store
            .upsert_approval(&ApprovalRecord {
                id: format!("appr-{idx}"),
                task_id: None,
                agent_id: "commander".to_string(),
                operation: "file_write".to_string(),
                tool_id: Some("file_write".to_string()),
                arguments_json: "{}".to_string(),
                reason: "Mutating action".to_string(),
                status: ApprovalStatus::Pending,
                created_at: now.clone(),
                resolved_at: None,
                resolved_by: None,
            })
            .unwrap();
    }

    ensure_bootstrap_credentials(&cfg, store.as_ref(), &logger).unwrap();
    let creds_raw =
        std::fs::read_to_string(cfg.data_dir.join("bootstrap_credentials.txt")).unwrap();
    let password = creds_raw
        .lines()
        .find_map(|l| l.strip_prefix("password="))
        .unwrap()
        .to_string();

    let (events_tx, _) = broadcast::channel(64);
    let backend: Arc<dyn HttpBackend> = Arc::new(MockBackend {
        store: store.clone(),
        events_tx: events_tx.clone(),
    });
    let shutdown = CancellationToken::new();
    let state = HttpState::new(
        cfg,
        store.clone(),
        backend,
        logger,
        events_tx,
        shutdown,
        DashboardAssets::default(),
    );

    TestHarness {
        temp_dir,
        state,
        store,
        password,
    }
}

fn lan_peer() -> ConnectInfo<SocketAddr> {
    ConnectInfo(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 40001))
}

fn req_with_ip(
    method: Method,
    uri: &str,
    ip: IpAddr,
    body: Body,
) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header(header::HOST, "127.0.0.1:8080")
        .extension(ConnectInfo(SocketAddr::new(ip, 45000)))
        .body(body)
        .unwrap()
}

async fn body_json(resp: axum::response::Response) -> Value {
    let bytes = to_bytes(resp.into_body(), 2 * 1024 * 1024).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn assert_security_headers(headers: &axum::http::HeaderMap, expect_no_store: bool) {
    let csp = headers
        .get(header::CONTENT_SECURITY_POLICY)
        .and_then(|v| v.to_str().ok())
        .expect("Content-Security-Policy header missing");
    assert!(csp.contains("default-src 'self'"), "CSP: {csp}");
    assert!(!csp.contains("unsafe-eval"), "CSP must forbid unsafe-eval");
    assert!(!csp.contains("http:"), "CSP must not allow external origins");
    assert!(!csp.contains("https:"), "CSP must not allow external origins");

    assert_eq!(
        headers
            .get(header::X_CONTENT_TYPE_OPTIONS)
            .and_then(|v| v.to_str().ok()),
        Some("nosniff")
    );
    assert_eq!(
        headers
            .get(header::X_FRAME_OPTIONS)
            .and_then(|v| v.to_str().ok()),
        Some("DENY")
    );
    assert_eq!(
        headers.get("referrer-policy").and_then(|v| v.to_str().ok()),
        Some("no-referrer")
    );
    if expect_no_store {
        assert_eq!(
            headers
                .get(header::CACHE_CONTROL)
                .and_then(|v| v.to_str().ok()),
            Some("no-store")
        );
    }
}

async fn login_cookie_and_csrf(
    state: &Arc<HttpState>,
    username: &str,
    password: &str,
) -> (String, String) {
    let router = build_router(state.clone());
    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/auth/login")
        .header(header::HOST, "127.0.0.1:8080")
        .header(header::CONTENT_TYPE, "application/json")
        .extension(lan_peer())
        .body(Body::from(
            json!({ "username": username, "password": password }).to_string(),
        ))
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_security_headers(resp.headers(), true);

    let set_cookie = resp
        .headers()
        .get(header::SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert!(set_cookie.contains("HttpOnly"), "cookie: {set_cookie}");
    assert!(set_cookie.contains("SameSite=Strict"), "cookie: {set_cookie}");
    assert!(set_cookie.contains("Path=/"), "cookie: {set_cookie}");
    assert!(set_cookie.contains("Max-Age="), "cookie: {set_cookie}");

    let cookie_pair = set_cookie.split(';').next().unwrap().to_string();
    let body = body_json(resp).await;
    let csrf = body["csrf_token"].as_str().unwrap().to_string();
    assert_eq!(body["username"], username);
    assert!(!csrf.is_empty());
    (cookie_pair, csrf)
}

#[tokio::test]
async fn test_bootstrap_credentials_argon2id_and_no_secret_logging() {
    let h = setup_harness("bootstrap", |_| {});
    assert_eq!(h.password.len(), 20);

    let hash = h
        .store
        .get_setting("auth.password_hash")
        .unwrap()
        .expect("password hash must be stored");
    assert!(hash.starts_with("$argon2id$"), "hash={hash}");

    // Ensure logger ring buffer never contains plaintext password or hash.
    let logs = h.state.logger.recent(None, 100);
    for entry in &logs {
        assert!(!entry.message.contains(&h.password));
        assert!(!entry.message.contains(&hash));
    }

    // Calling ensure_bootstrap_credentials again must preserve the existing hash.
    ensure_bootstrap_credentials(&h.state.config, h.store.as_ref(), &h.state.logger).unwrap();
    assert_eq!(
        h.store.get_setting("auth.password_hash").unwrap().as_deref(),
        Some(hash.as_str())
    );
}

#[tokio::test]
async fn test_unauthenticated_401_and_public_routes_and_headers() {
    let h = setup_harness("unauth", |_| {});
    let router = build_router(h.state.clone());

    // Public minimal health
    let resp = router
        .clone()
        .oneshot(req_with_ip(
            Method::GET,
            "/api/v1/health",
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            Body::empty(),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_security_headers(resp.headers(), true);
    let health = body_json(resp).await;
    assert_eq!(health["status"], "ok");
    assert!(health.get("version").is_some());
    assert!(health.get("uptime_secs").is_none());

    // Static dashboard shell accessible unauthenticated
    for (path, expected_ct) in [
        ("/", "text/html"),
        ("/styles.css", "text/css"),
        ("/app.js", "application/javascript"),
    ] {
        let resp = router
            .clone()
            .oneshot(req_with_ip(
                Method::GET,
                path,
                IpAddr::V4(Ipv4Addr::LOCALHOST),
                Body::empty(),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "path {path}");
        assert_security_headers(resp.headers(), false);
        let ct = resp
            .headers()
            .get(header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(ct.starts_with(expected_ct), "path {path} ct={ct}");
    }

    // Protected endpoints must return 401 with security headers
    for (method, path) in [
        (Method::GET, "/api/v1/auth/session"),
        (Method::GET, "/api/v1/metrics"),
        (Method::GET, "/api/v1/conversations"),
        (Method::POST, "/api/v1/conversations"),
        (Method::GET, "/api/v1/agents"),
        (Method::GET, "/api/v1/tasks"),
        (Method::GET, "/api/v1/approvals"),
        (Method::GET, "/api/v1/memories"),
        (Method::GET, "/api/v1/tools"),
        (Method::GET, "/api/v1/logs"),
        (Method::GET, "/api/v1/audit-logs"),
        (Method::GET, "/api/v1/settings"),
    ] {
        let resp = router
            .clone()
            .oneshot(req_with_ip(
                method.clone(),
                path,
                IpAddr::V4(Ipv4Addr::LOCALHOST),
                Body::empty(),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "{method} {path}");
        assert_security_headers(resp.headers(), true);
    }
}

#[tokio::test]
async fn test_csrf_and_origin_enforcement() {
    let h = setup_harness("csrf", |_| {});
    let (cookie, csrf) = login_cookie_and_csrf(&h.state, "admin", &h.password).await;
    let router = build_router(h.state.clone());

    let payload = json!({
        "title": "Test Chat",
        "mode": { "kind": "direct", "agent_id": "commander" }
    })
    .to_string();

    // Missing X-CSRF-Token -> 403
    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/conversations")
        .header(header::HOST, "127.0.0.1:8080")
        .header(header::COOKIE, &cookie)
        .header(header::CONTENT_TYPE, "application/json")
        .extension(lan_peer())
        .body(Body::from(payload.clone()))
        .unwrap();
    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    assert_security_headers(resp.headers(), true);

    // Wrong X-CSRF-Token -> 403
    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/conversations")
        .header(header::HOST, "127.0.0.1:8080")
        .header(header::COOKIE, &cookie)
        .header("x-csrf-token", "0".repeat(64))
        .header(header::CONTENT_TYPE, "application/json")
        .extension(lan_peer())
        .body(Body::from(payload.clone()))
        .unwrap();
    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    // Valid CSRF but evil Origin -> 403
    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/conversations")
        .header(header::HOST, "127.0.0.1:8080")
        .header(header::COOKIE, &cookie)
        .header("x-csrf-token", &csrf)
        .header(header::ORIGIN, "https://evil.example")
        .header(header::CONTENT_TYPE, "application/json")
        .extension(lan_peer())
        .body(Body::from(payload.clone()))
        .unwrap();
    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    // Valid CSRF and evil Referer -> 403
    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/conversations")
        .header(header::HOST, "127.0.0.1:8080")
        .header(header::COOKIE, &cookie)
        .header("x-csrf-token", &csrf)
        .header(header::REFERER, "https://evil.example/phish")
        .header(header::CONTENT_TYPE, "application/json")
        .extension(lan_peer())
        .body(Body::from(payload.clone()))
        .unwrap();
    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    // Valid CSRF and matching Origin -> 201 Created
    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/conversations")
        .header(header::HOST, "127.0.0.1:8080")
        .header(header::COOKIE, &cookie)
        .header("x-csrf-token", &csrf)
        .header(header::ORIGIN, "http://127.0.0.1:8080")
        .header(header::CONTENT_TYPE, "application/json")
        .extension(lan_peer())
        .body(Body::from(payload))
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn test_payload_too_large_413() {
    let h = setup_harness("bodylimit", |_| {});
    assert_eq!(h.state.config.server.max_body_bytes, 1024 * 1024);
    let (cookie, csrf) = login_cookie_and_csrf(&h.state, "admin", &h.password).await;
    let router = build_router(h.state.clone());

    let oversized = "a".repeat(1024 * 1024 + 128);
    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/memories")
        .header(header::HOST, "127.0.0.1:8080")
        .header(header::COOKIE, &cookie)
        .header("x-csrf-token", &csrf)
        .header(header::CONTENT_TYPE, "application/json")
        .extension(lan_peer())
        .body(Body::from(oversized))
        .unwrap();

    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_security_headers(resp.headers(), true);
}

#[tokio::test]
async fn test_login_rate_limit_429_with_retry_after() {
    let h = setup_harness("ratelimit", |cfg| {
        cfg.security.login_rate_limit_per_minute = 3;
    });
    let router = build_router(h.state.clone());
    let attacker_ip = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 77));

    for attempt in 0..3 {
        let mut req = req_with_ip(
            Method::POST,
            "/api/v1/auth/login",
            attacker_ip,
            Body::from(json!({ "username": "admin", "password": "wrong-password" }).to_string()),
        );
        req.headers_mut().insert(
            header::CONTENT_TYPE,
            "application/json".parse().unwrap(),
        );
        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "attempt {attempt}");
    }

    // 4th attempt within 60s must be rate-limited with 429 + Retry-After
    let mut req = req_with_ip(
        Method::POST,
        "/api/v1/auth/login",
        attacker_ip,
        Body::from(json!({ "username": "admin", "password": "wrong-password" }).to_string()),
    );
    req.headers_mut().insert(
        header::CONTENT_TYPE,
        "application/json".parse().unwrap(),
    );
    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_security_headers(resp.headers(), true);

    let retry_after: u64 = resp
        .headers()
        .get(header::RETRY_AFTER)
        .expect("Retry-After header missing")
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!((1..=60).contains(&retry_after), "retry_after={retry_after}");

    // Another LAN IP is isolated and can still log in.
    let other_ip = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 88));
    let mut req = req_with_ip(
        Method::POST,
        "/api/v1/auth/login",
        other_ip,
        Body::from(json!({ "username": "admin", "password": h.password }).to_string()),
    );
    req.headers_mut().insert(
        header::CONTENT_TYPE,
        "application/json".parse().unwrap(),
    );
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_lan_only_ip_filtering() {
    let h = setup_harness("lanonly", |cfg| {
        cfg.server.allow_lan_only = true;
    });
    let router = build_router(h.state.clone());

    for allowed in [
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)),
        IpAddr::V4(Ipv4Addr::new(172, 16, 4, 2)),
        IpAddr::V4(Ipv4Addr::new(192, 168, 1, 10)),
        IpAddr::V6(Ipv6Addr::LOCALHOST),
        "fd12:3456:789a::1".parse().unwrap(),
        "::ffff:192.168.1.25".parse().unwrap(),
    ] {
        let resp = router
            .clone()
            .oneshot(req_with_ip(Method::GET, "/api/v1/health", allowed, Body::empty()))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "allowed IP {allowed}");
    }

    for blocked in [
        IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)),
        IpAddr::V4(Ipv4Addr::new(203, 0, 113, 42)),
        "2001:db8::1".parse().unwrap(),
        "::ffff:8.8.8.8".parse().unwrap(),
    ] {
        let resp = router
            .clone()
            .oneshot(req_with_ip(Method::GET, "/api/v1/health", blocked, Body::empty()))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN, "blocked IP {blocked}");
        assert_security_headers(resp.headers(), true);
    }
}

#[tokio::test]
async fn test_bearer_auth_and_settings_redaction() {
    let bearer_secret = "ps5-cli-secret-token-123456";
    let h = setup_harness("bearer", |cfg| {
        cfg.security.api_token_sha256 = Some(sha256_hex(bearer_secret.as_bytes()));
    });
    let router = build_router(h.state.clone());

    // Wrong bearer -> 401
    let mut req = req_with_ip(
        Method::GET,
        "/api/v1/settings",
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        Body::empty(),
    );
    req.headers_mut()
        .insert(header::AUTHORIZATION, "Bearer wrong-token".parse().unwrap());
    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // Valid bearer -> 200 and auth.password_hash must NOT be exposed
    let mut req = req_with_ip(
        Method::GET,
        "/api/v1/settings",
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        Body::empty(),
    );
    req.headers_mut().insert(
        header::AUTHORIZATION,
        format!("Bearer {bearer_secret}").parse().unwrap(),
    );
    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert!(body["settings"].get("auth.password_hash").is_none());

    // Attempting to overwrite auth.password_hash via PUT /settings -> 403
    let mut req = req_with_ip(
        Method::PUT,
        "/api/v1/settings",
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        Body::from(json!({ "settings": { "auth.password_hash": "pwned" } }).to_string()),
    );
    req.headers_mut().insert(
        header::AUTHORIZATION,
        format!("Bearer {bearer_secret}").parse().unwrap(),
    );
    req.headers_mut()
        .insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn test_authenticated_route_roundtrips_and_sse_over_tcp() {
    let h = setup_harness("roundtrip", |_| {});
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_state = h.state.clone();
    let server_handle = tokio::spawn(async move {
        serve_with_listener(addr.to_string(), listener, server_state)
            .await
            .unwrap();
    });

    // Helper via router with ConnectInfo + real SSE/health TCP verification
    let (cookie, csrf) = login_cookie_and_csrf(&h.state, "admin", &h.password).await;
    let router = build_router(h.state.clone());

    let authed_req = |method: Method, uri: &str, body: Option<Value>| {
        let mut b = Request::builder()
            .method(method)
            .uri(uri)
            .header(header::HOST, "127.0.0.1:8080")
            .header(header::COOKIE, &cookie)
            .header("x-csrf-token", &csrf)
            .extension(lan_peer());
        if let Some(json_val) = body {
            b = b.header(header::CONTENT_TYPE, "application/json");
            b.body(Body::from(json_val.to_string())).unwrap()
        } else {
            b.body(Body::empty()).unwrap()
        }
    };

    // Session & authenticated health & metrics
    let resp = router
        .clone()
        .oneshot(authed_req(Method::GET, "/api/v1/auth/session", None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let sess = body_json(resp).await;
    assert_eq!(sess["authenticated"], true);
    assert_eq!(sess["auth_method"], "cookie");

    let resp = router
        .clone()
        .oneshot(authed_req(Method::GET, "/api/v1/health", None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let health = body_json(resp).await;
    assert_eq!(health["storage_driver"], "memory-mock");

    let resp = router
        .clone()
        .oneshot(authed_req(Method::GET, "/api/v1/metrics", None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let metrics = body_json(resp).await;
    assert_eq!(metrics["uptime_secs"], 42);

    // Conversations CRUD + message + search
    let resp = router
        .clone()
        .oneshot(authed_req(
            Method::POST,
            "/api/v1/conversations",
            Some(json!({
                "title": "PS5 Kernel Notes",
                "mode": { "kind": "direct", "agent_id": "commander" }
            })),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let conv_id = body_json(resp).await["id"].as_str().unwrap().to_string();

    let resp = router
        .clone()
        .oneshot(authed_req(
            Method::POST,
            &format!("/api/v1/conversations/{conv_id}/messages"),
            Some(json!({ "content": "Inspect memory budget please" })),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let msg_resp = body_json(resp).await;
    assert!(msg_resp["task"]["id"].is_string());

    let resp = router
        .clone()
        .oneshot(authed_req(
            Method::GET,
            "/api/v1/conversations/search?q=budget",
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_json(resp).await.as_array().unwrap().len(), 1);

    let resp = router
        .clone()
        .oneshot(authed_req(
            Method::GET,
            &format!("/api/v1/conversations/{conv_id}"),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        body_json(resp).await["messages"].as_array().unwrap().len(),
        1
    );

    // Agents CRUD + enable/disable + memory
    let resp = router
        .clone()
        .oneshot(authed_req(
            Method::POST,
            "/api/v1/agents",
            Some(json!({
                "name": "CustomHelper",
                "description": "Helper",
                "system_prompt": "Assist"
            })),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let agent_id = body_json(resp).await["id"].as_str().unwrap().to_string();

    let resp = router
        .clone()
        .oneshot(authed_req(
            Method::PUT,
            &format!("/api/v1/agents/{agent_id}"),
            Some(json!({ "description": "Updated helper" })),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = router
        .clone()
        .oneshot(authed_req(
            Method::POST,
            &format!("/api/v1/agents/{agent_id}/disable"),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_json(resp).await["enabled"], false);

    let resp = router
        .clone()
        .oneshot(authed_req(
            Method::POST,
            &format!("/api/v1/agents/{agent_id}/enable"),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_json(resp).await["enabled"], true);

    let resp = router
        .clone()
        .oneshot(authed_req(
            Method::GET,
            &format!("/api/v1/agents/{agent_id}/memory"),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Agent messages
    let resp = router
        .clone()
        .oneshot(authed_req(
            Method::POST,
            "/api/v1/agent-messages",
            Some(json!({
                "sender_agent_id": "commander",
                "recipient_agent_id": agent_id,
                "content": "ping"
            })),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let resp = router
        .clone()
        .oneshot(authed_req(Method::GET, "/api/v1/agent-messages", None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_json(resp).await.as_array().unwrap().len(), 1);

    // Tasks
    let resp = router
        .clone()
        .oneshot(authed_req(
            Method::POST,
            "/api/v1/tasks",
            Some(json!({ "user_request": "Run check" })),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let task_id = body_json(resp).await["id"].as_str().unwrap().to_string();

    let resp = router
        .clone()
        .oneshot(authed_req(
            Method::GET,
            &format!("/api/v1/tasks/{task_id}/events"),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = router
        .clone()
        .oneshot(authed_req(
            Method::POST,
            &format!("/api/v1/tasks/{task_id}/cancel"),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_json(resp).await["status"], "CANCELLED");

    // Approvals
    let resp = router
        .clone()
        .oneshot(authed_req(Method::GET, "/api/v1/approvals?pending_only=true", None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_json(resp).await.as_array().unwrap().len(), 2);

    let resp = router
        .clone()
        .oneshot(authed_req(
            Method::POST,
            "/api/v1/approvals/appr-1/approve",
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_json(resp).await["status"], "APPROVED");

    let resp = router
        .clone()
        .oneshot(authed_req(Method::POST, "/api/v1/approvals/appr-2/deny", None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_json(resp).await["status"], "DENIED");

    // Memories
    let resp = router
        .clone()
        .oneshot(authed_req(
            Method::POST,
            "/api/v1/memories",
            Some(json!({
                "scope": "project",
                "namespace": "ps5",
                "key": "fw",
                "content": "13.40"
            })),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let mem_id = body_json(resp).await["id"].as_str().unwrap().to_string();

    let resp = router
        .clone()
        .oneshot(authed_req(
            Method::DELETE,
            &format!("/api/v1/memories/{mem_id}"),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Tools
    let resp = router
        .clone()
        .oneshot(authed_req(Method::GET, "/api/v1/tools", None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = router
        .clone()
        .oneshot(authed_req(
            Method::POST,
            "/api/v1/tools/file_read/toggle",
            Some(json!({ "enabled": false })),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_json(resp).await["enabled"], false);

    let resp = router
        .clone()
        .oneshot(authed_req(
            Method::POST,
            "/api/v1/tools/file_read/execute",
            Some(json!({ "arguments": { "path": "notes.txt" } })),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = router
        .clone()
        .oneshot(authed_req(Method::GET, "/api/v1/tool-executions", None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_json(resp).await.as_array().unwrap().len(), 1);

    // Settings, logs, audit logs, checkpoint
    let resp = router
        .clone()
        .oneshot(authed_req(
            Method::PUT,
            "/api/v1/settings",
            Some(json!({ "settings": { "ui.theme": "midnight" } })),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_json(resp).await["settings"]["ui.theme"], "midnight");

    let resp = router
        .clone()
        .oneshot(authed_req(Method::POST, "/api/v1/system/checkpoint", None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = router
        .clone()
        .oneshot(authed_req(Method::GET, "/api/v1/logs", None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = router
        .clone()
        .oneshot(authed_req(Method::GET, "/api/v1/audit-logs", None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(!body_json(resp).await.as_array().unwrap().is_empty());

    // Delete conversation & custom agent
    let resp = router
        .clone()
        .oneshot(authed_req(
            Method::DELETE,
            &format!("/api/v1/conversations/{conv_id}"),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = router
        .clone()
        .oneshot(authed_req(
            Method::DELETE,
            &format!("/api/v1/agents/{agent_id}"),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Real TCP SSE stream verification
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let sse_req = format!(
        "GET /api/v1/events HTTP/1.1\r\nHost: {addr}\r\nCookie: {cookie}\r\nAccept: text/event-stream\r\n\r\n"
    );
    stream.write_all(sse_req.as_bytes()).await.unwrap();

    // Wait for 200 OK headers so handler has subscribed to events_tx before broadcasting.
    let mut buf = vec![0u8; 4096];
    let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    let headers_chunk = String::from_utf8_lossy(&buf[..n]);
    assert!(headers_chunk.contains("200 OK"), "SSE response: {headers_chunk}");
    assert!(
        headers_chunk.contains("text/event-stream"),
        "SSE response: {headers_chunk}"
    );

    h.state
        .events_tx
        .send(StreamEvent::SystemAlert {
            level: "info".to_string(),
            message: "sse-live-check".to_string(),
        })
        .unwrap();

    let n2 = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    let body_chunk = String::from_utf8_lossy(&buf[..n2]);
    assert!(body_chunk.contains("sse-live-check"), "SSE body: {body_chunk}");
    drop(stream);

    // Password rotation + logout invalidation
    let new_pw = "rotated-strong-passphrase-2026";
    let resp = router
        .clone()
        .oneshot(authed_req(
            Method::POST,
            "/api/v1/auth/password",
            Some(json!({ "current_password": h.password, "new_password": new_pw })),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let (_cookie2, _csrf2) = login_cookie_and_csrf(&h.state, "admin", new_pw).await;

    let resp = router
        .clone()
        .oneshot(authed_req(Method::POST, "/api/v1/auth/logout", None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Revoked cookie must now return 401
    let resp = router
        .clone()
        .oneshot(authed_req(Method::GET, "/api/v1/auth/session", None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // System shutdown cancels token and stops TCP server cleanly
    let (cookie3, csrf3) = login_cookie_and_csrf(&h.state, "admin", new_pw).await;
    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/system/shutdown")
        .header(header::HOST, "127.0.0.1:8080")
        .header(header::COOKIE, cookie3)
        .header("x-csrf-token", csrf3)
        .extension(lan_peer())
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    assert!(h.state.shutdown.is_cancelled());

    tokio::time::timeout(Duration::from_secs(3), server_handle)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn test_ps5_rest_mode_os_error_163_listener_recovery() {
    let h = setup_harness("restmode", |_| {});
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let mut recovering = RecoveringTcpListener::new(addr.to_string(), listener, h.state.clone())
        .unwrap()
        .with_backoff(Duration::from_millis(10), Duration::from_millis(50));

    assert!(!h.state.is_network_degraded());

    // Simulate Sony OS error 163 on accept() after PS5 rest-mode wake.
    let err_163 = io::Error::from_raw_os_error(163);
    let mut consecutive = 0u32;
    let res = recovering
        .handle_accept_result(Err(err_163), &mut consecutive)
        .await;
    assert!(res.is_none());
    // Note: bind_with_recovery rebinds immediately on localhost and clears degraded flag
    // once bound; check that SystemAlert events captured both degraded and recovery!
    let rx = h.state.events_tx.subscribe();
    // Verify listener rebound to a working socket and accepts connections again!
    let bound_addr = axum::serve::Listener::local_addr(&recovering).unwrap();
    let _client = TcpStream::connect(bound_addr).await.unwrap();
    let accepted = tokio::time::timeout(
        Duration::from_secs(2),
        axum::serve::Listener::accept(&mut recovering),
    )
    .await
    .expect("recovered listener should accept new connections");
    assert_eq!(accepted.1.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
    assert!(!h.state.is_network_degraded());
    drop(rx);
}

#[tokio::test]
async fn test_settings_allows_max_output_tokens_and_blocks_api_key() {
    let h = setup_harness("settings_tokens", |_| {});
    let (cookie, csrf) = login_cookie_and_csrf(&h.state, "admin", &h.password).await;
    let router = build_router(h.state.clone());

    let ok_req = Request::builder()
        .method(Method::PUT)
        .uri("/api/v1/settings")
        .header(header::HOST, "127.0.0.1:8080")
        .header(header::COOKIE, cookie.clone())
        .header("x-csrf-token", csrf.clone())
        .header(header::CONTENT_TYPE, "application/json")
        .extension(lan_peer())
        .body(Body::from(
            json!({
                "inference": { "max_output_tokens": 1024, "api_mode": "completions" },
                "agents": { "max_agents": 6 }
            })
            .to_string(),
        ))
        .unwrap();
    let ok_resp = router.clone().oneshot(ok_req).await.unwrap();
    assert_eq!(ok_resp.status(), StatusCode::OK);

    let secret_req = Request::builder()
        .method(Method::PUT)
        .uri("/api/v1/settings")
        .header(header::HOST, "127.0.0.1:8080")
        .header(header::COOKIE, cookie)
        .header("x-csrf-token", csrf)
        .header(header::CONTENT_TYPE, "application/json")
        .extension(lan_peer())
        .body(Body::from(
            json!({
                "inference": { "api_key": "should-be-blocked" }
            })
            .to_string(),
        ))
        .unwrap();
    let secret_resp = router.oneshot(secret_req).await.unwrap();
    assert_eq!(secret_resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn test_tasks_status_filter_scans_before_limit() {
    let h = setup_harness("tasks_filter", |_| {});
    let (cookie, _csrf) = login_cookie_and_csrf(&h.state, "admin", &h.password).await;
    let router = build_router(h.state.clone());

    for idx in 0..50 {
        h.store
            .upsert_task(&TaskRecord {
                id: format!("done-{idx}"),
                conversation_id: Some("conv-done".to_string()),
                user_request: "completed work".to_string(),
                assigned_agent_id: "commander".to_string(),
                parent_task_id: None,
                priority: 0,
                created_at: now_iso(),
                started_at: None,
                completed_at: Some(now_iso()),
                status: TaskStatus::Completed,
                result: Some("ok".to_string()),
                error_info: None,
                has_side_effects: false,
                progress_pct: 100,
                progress_note: None,
            })
            .unwrap();
    }

    for (id, status, agent_id, conv_id) in [
        ("active-pending", TaskStatus::Pending, "commander", "conv-a"),
        ("active-running", TaskStatus::Running, "researcher", "conv-b"),
    ] {
        h.store
            .upsert_task(&TaskRecord {
                id: id.to_string(),
                conversation_id: Some(conv_id.to_string()),
                user_request: "in progress".to_string(),
                assigned_agent_id: agent_id.to_string(),
                parent_task_id: None,
                priority: 1,
                created_at: now_iso(),
                started_at: None,
                completed_at: None,
                status,
                result: None,
                error_info: None,
                has_side_effects: false,
                progress_pct: 10,
                progress_note: None,
            })
            .unwrap();
    }

    // status=active&limit=2 must filter across all tasks BEFORE applying limit=2.
    let active_req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/tasks?status=active&limit=2")
        .header(header::HOST, "127.0.0.1:8080")
        .header(header::COOKIE, &cookie)
        .extension(lan_peer())
        .body(Body::empty())
        .unwrap();
    let active_resp = router.clone().oneshot(active_req).await.unwrap();
    assert_eq!(active_resp.status(), StatusCode::OK);
    let active_tasks = body_json(active_resp).await;
    let active_arr = active_tasks.as_array().unwrap();
    assert_eq!(active_arr.len(), 2);
    let mut ids: Vec<&str> = active_arr
        .iter()
        .map(|t| t["id"].as_str().unwrap())
        .collect();
    ids.sort();
    assert_eq!(ids, vec!["active-pending", "active-running"]);

    // Filter by exact status + agent_id + conversation_id.
    let running_req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/tasks?status=running&agent_id=researcher&conversation_id=conv-b")
        .header(header::HOST, "127.0.0.1:8080")
        .header(header::COOKIE, &cookie)
        .extension(lan_peer())
        .body(Body::empty())
        .unwrap();
    let running_resp = router.clone().oneshot(running_req).await.unwrap();
    assert_eq!(running_resp.status(), StatusCode::OK);
    let running_arr = body_json(running_resp).await;
    assert_eq!(running_arr.as_array().unwrap().len(), 1);
    assert_eq!(running_arr[0]["id"], "active-running");

    // Invalid status must return 400 Bad Request.
    let bad_req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/tasks?status=not-a-status")
        .header(header::HOST, "127.0.0.1:8080")
        .header(header::COOKIE, &cookie)
        .extension(lan_peer())
        .body(Body::empty())
        .unwrap();
    let bad_resp = router.oneshot(bad_req).await.unwrap();
    assert_eq!(bad_resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_inbound_webhook_csrf_exemption_and_token_auth() {
    let h = setup_harness("webhook_sec", |_| {});
    let (cookie, _csrf) = login_cookie_and_csrf(&h.state, "admin", &h.password).await;
    let router = build_router(h.state.clone());

    // Management CRUD remains cookie + CSRF protected.
    let unauth_list = router
        .clone()
        .oneshot(req_with_ip(
            Method::GET,
            "/api/v1/webhooks",
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            Body::empty(),
        ))
        .await
        .unwrap();
    assert_eq!(unauth_list.status(), StatusCode::UNAUTHORIZED);

    let no_csrf_create = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/webhooks")
        .header(header::HOST, "127.0.0.1:8080")
        .header(header::COOKIE, &cookie)
        .header(header::CONTENT_TYPE, "application/json")
        .extension(lan_peer())
        .body(Body::from(
            json!({ "name": "ci", "target_agent_id": "commander" }).to_string(),
        ))
        .unwrap();
    let no_csrf_resp = router.clone().oneshot(no_csrf_create).await.unwrap();
    assert_eq!(no_csrf_resp.status(), StatusCode::FORBIDDEN);

    // Inbound webhook is CSRF-exempt, but fail-closed without webhook token -> 401.
    let missing_tok_resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/webhooks/inbound/wh-1")
                .header(header::HOST, "127.0.0.1:8080")
                .header(header::ORIGIN, "https://external.example")
                .extension(lan_peer())
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(missing_tok_resp.status(), StatusCode::UNAUTHORIZED);

    // Invalid token -> 403 Forbidden.
    let bad_tok_resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/webhooks/inbound/wh-1")
                .header(header::HOST, "127.0.0.1:8080")
                .header("x-ps5-webhook-token", "wrong-secret")
                .extension(lan_peer())
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(bad_tok_resp.status(), StatusCode::FORBIDDEN);

    // Valid X-PS5-Webhook-Token without cookie/CSRF -> 200 OK.
    let ok_header_resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/webhooks/inbound/wh-1")
                .header(header::HOST, "127.0.0.1:8080")
                .header("x-ps5-webhook-token", "wh-secret-123")
                .extension(lan_peer())
                .body(Body::from(json!({ "event": "push" }).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ok_header_resp.status(), StatusCode::OK);
    let ok_json = body_json(ok_header_resp).await;
    assert_eq!(ok_json["ok"], true);
    assert_eq!(ok_json["webhook_id"], "wh-1");

    // Valid Authorization: Bearer token -> 200 OK.
    let ok_bearer_resp = router
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/webhooks/inbound/wh-1")
                .header(header::HOST, "127.0.0.1:8080")
                .header(header::AUTHORIZATION, "Bearer wh-secret-123")
                .extension(lan_peer())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ok_bearer_resp.status(), StatusCode::OK);
}

#[test]
fn test_preview_csp_origin_scheme_port_and_ipv6_bind_format() {
    assert!(ps5_agent_http::security::PREVIEW_CSP_VALUE.contains("sandbox allow-scripts"));
    assert!(!ps5_agent_http::security::PREVIEW_CSP_VALUE.contains("allow-same-origin"));

    let mut headers = axum::http::HeaderMap::new();
    ps5_agent_http::security::apply_security_headers(
        &mut headers,
        "/api/v1/files/preview",
        StatusCode::OK,
    );
    assert_eq!(
        headers
            .get(header::CONTENT_SECURITY_POLICY)
            .and_then(|v| v.to_str().ok()),
        Some(ps5_agent_http::security::PREVIEW_CSP_VALUE)
    );
    assert_eq!(
        headers
            .get(header::X_FRAME_OPTIONS)
            .and_then(|v| v.to_str().ok()),
        Some("SAMEORIGIN")
    );

    // Cross-scheme and subdomain Origin checks must be rejected.
    let mut cross_scheme = axum::http::HeaderMap::new();
    cross_scheme.insert(header::HOST, "127.0.0.1".parse().unwrap());
    cross_scheme.insert(header::ORIGIN, "https://127.0.0.1".parse().unwrap());
    assert!(!ps5_agent_http::security::validate_origin_or_referer(&cross_scheme));

    let mut cross_scheme_port = axum::http::HeaderMap::new();
    cross_scheme_port.insert(header::HOST, "127.0.0.1:8080".parse().unwrap());
    cross_scheme_port.insert(header::ORIGIN, "https://127.0.0.1:8080".parse().unwrap());
    assert!(!ps5_agent_http::security::validate_origin_or_referer(
        &cross_scheme_port
    ));

    let mut subdomain = axum::http::HeaderMap::new();
    subdomain.insert(header::HOST, "localhost:8080".parse().unwrap());
    subdomain.insert(header::ORIGIN, "http://sub.localhost:8080".parse().unwrap());
    assert!(!ps5_agent_http::security::validate_origin_or_referer(&subdomain));

    let mut dup_origin = axum::http::HeaderMap::new();
    dup_origin.insert(header::HOST, "127.0.0.1:8080".parse().unwrap());
    dup_origin.append(header::ORIGIN, "http://127.0.0.1:8080".parse().unwrap());
    dup_origin.append(header::ORIGIN, "http://evil.example".parse().unwrap());
    assert!(!ps5_agent_http::security::validate_origin_or_referer(&dup_origin));

    let mut userinfo_host = axum::http::HeaderMap::new();
    userinfo_host.insert(header::HOST, "evil.example@127.0.0.1:8080".parse().unwrap());
    userinfo_host.insert(header::ORIGIN, "http://127.0.0.1:8080".parse().unwrap());
    assert!(!ps5_agent_http::security::validate_origin_or_referer(&userinfo_host));

    let mut ipv6_ok = axum::http::HeaderMap::new();
    ipv6_ok.insert(header::HOST, "[::1]:8080".parse().unwrap());
    ipv6_ok.insert(header::ORIGIN, "http://[::1]:8080".parse().unwrap());
    assert!(ps5_agent_http::security::validate_origin_or_referer(&ipv6_ok));

    assert_eq!(ps5_agent_http::server::format_bind_addr("::1", 8080), "[::1]:8080");
    assert_eq!(ps5_agent_http::server::format_bind_addr("[::1]", 8080), "[::1]:8080");
    assert_eq!(ps5_agent_http::server::format_bind_addr("0.0.0.0", 8080), "0.0.0.0:8080");
}
