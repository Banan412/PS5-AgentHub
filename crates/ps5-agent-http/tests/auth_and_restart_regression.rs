use async_trait::async_trait;
use axum::body::{to_bytes, Body};
use axum::extract::ConnectInfo;
use axum::http::{header, Method, Request, StatusCode};
use ps5_agent_config::{AppConfig, LoggingConfig};
use ps5_agent_core::{
    new_id, now_iso, AgentCardView, AgentDefinition, AgentEnvelope, AgentError, AgentRole,
    AgentRuntimeStats, ApprovalRecord, AuditLogEntry, ChatMessage, Conversation, DeliveryStatus,
    MemoryItem, MemoryPermissions, MemoryScope, MetricsSnapshot, Result as CoreResult,
    SessionRecord, StorageBackend, TaskCountsSummary, TaskEvent, TaskRecord, ToolExecutionRecord,
    ToolSpec,
};
use ps5_agent_http::server::bind_reusable_listener;
use ps5_agent_http::{
    build_router, ensure_bootstrap_credentials_with_report, CreateAgentRequest, CreateTaskRequest,
    DashboardAssets, ExecuteToolRequest, HttpBackend, HttpState, SendAgentMessageRequest,
    SendMessageRequest, SendMessageResponse, UpdateAgentRequest,
};
use ps5_agent_logging::AppLogger;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
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
            .filter(|a| !pending_only || a.status == ps5_agent_core::ApprovalStatus::Pending)
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

struct LiveConfigMockBackend {
    store: Arc<MemoryStorage>,
    config: RwLock<AppConfig>,
}

#[async_trait]
impl HttpBackend for LiveConfigMockBackend {
    async fn metrics(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            uptime_secs: 1,
            storage_driver: self.store.driver_name().to_string(),
            ..Default::default()
        }
    }

    async fn send_conversation_message(
        &self,
        _conversation_id: &str,
        _req: SendMessageRequest,
    ) -> CoreResult<SendMessageResponse> {
        Err(AgentError::Internal("unused in regression test".into()))
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

    async fn create_agent(&self, _req: CreateAgentRequest) -> CoreResult<AgentDefinition> {
        Err(AgentError::Internal("unused in regression test".into()))
    }
    async fn update_agent(&self, _id: &str, _req: UpdateAgentRequest) -> CoreResult<AgentDefinition> {
        Err(AgentError::Internal("unused in regression test".into()))
    }
    async fn delete_agent(&self, _id: &str) -> CoreResult<bool> {
        Ok(false)
    }
    async fn set_agent_enabled(&self, _id: &str, _enabled: bool) -> CoreResult<AgentDefinition> {
        Err(AgentError::Internal("unused in regression test".into()))
    }
    async fn send_agent_message(&self, _req: SendAgentMessageRequest) -> CoreResult<AgentEnvelope> {
        Err(AgentError::Internal("unused in regression test".into()))
    }
    async fn create_task(&self, _req: CreateTaskRequest) -> CoreResult<TaskRecord> {
        Err(AgentError::Internal("unused in regression test".into()))
    }
    async fn cancel_task(&self, _task_id: &str) -> CoreResult<TaskRecord> {
        Err(AgentError::Internal("unused in regression test".into()))
    }
    async fn resolve_approval(
        &self,
        _approval_id: &str,
        _approved: bool,
        _resolved_by: &str,
    ) -> CoreResult<ApprovalRecord> {
        Err(AgentError::Internal("unused in regression test".into()))
    }
    async fn list_tools(&self) -> CoreResult<Vec<ToolSpec>> {
        Ok(Vec::new())
    }
    async fn toggle_tool(&self, _tool_id: &str, _enabled: Option<bool>) -> CoreResult<ToolSpec> {
        Err(AgentError::Internal("unused in regression test".into()))
    }
    async fn execute_tool(
        &self,
        _tool_id: &str,
        _req: ExecuteToolRequest,
    ) -> CoreResult<ToolExecutionRecord> {
        Err(AgentError::Internal("unused in regression test".into()))
    }

    fn current_config(&self) -> Option<AppConfig> {
        Some(self.config.read().unwrap().clone())
    }

    async fn apply_settings_update(
        &self,
        updates: &serde_json::Map<String, Value>,
    ) -> CoreResult<()> {
        let mut cfg = self.config.write().unwrap();
        if let Some(Value::Object(sec)) = updates.get("security") {
            if let Some(v) = sec.get("auth_enabled").and_then(Value::as_bool) {
                cfg.security.auth_enabled = v;
            }
            if let Some(v) = sec.get("admin_username").and_then(Value::as_str) {
                let trimmed = v.trim();
                if !trimmed.is_empty() {
                    cfg.security.admin_username = trimmed.to_string();
                }
            }
            if let Some(v) = sec
                .get("require_approval_for_mutating")
                .and_then(Value::as_bool)
            {
                cfg.security.require_approval_for_mutating = v;
            }
            if let Some(v) = sec.get("session_ttl_hours").and_then(Value::as_u64) {
                cfg.security.session_ttl_hours = v.max(1);
            }
        }
        Ok(())
    }
}

struct RegressionHarness {
    temp_dir: PathBuf,
    state: Arc<HttpState>,
    store: Arc<MemoryStorage>,
    generated_password: Option<String>,
}

impl Drop for RegressionHarness {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.temp_dir);
    }
}

fn new_temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("ps5-http-reg-{name}-{}", new_id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn base_test_config(temp_dir: &PathBuf) -> AppConfig {
    let mut cfg = AppConfig::with_data_dir(temp_dir.clone());
    cfg.security.argon2_memory_kib = 4096;
    cfg.logging = LoggingConfig {
        level: "info".to_string(),
        log_dir: temp_dir.join("logs"),
        max_file_bytes: 512 * 1024,
        ring_buffer_capacity: 200,
        enable_klog: false,
        enable_toast: false,
    };
    cfg
}

fn seed_commander(store: &MemoryStorage) {
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
            enabled_tools: vec![],
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
            updated_at: now,
        })
        .unwrap();
}

fn setup_regression_harness(
    name: &str,
    run_bootstrap: bool,
    mut tweak: impl FnMut(&mut AppConfig),
) -> RegressionHarness {
    let temp_dir = new_temp_dir(name);
    let mut cfg = base_test_config(&temp_dir);
    tweak(&mut cfg);

    let logger = AppLogger::init(&cfg.logging);
    let store = Arc::new(MemoryStorage::default());
    seed_commander(&store);

    let generated_password = if run_bootstrap {
        ensure_bootstrap_credentials_with_report(&cfg, store.as_ref(), &logger).unwrap()
    } else {
        None
    };

    let (events_tx, _) = broadcast::channel(32);
    let backend: Arc<dyn HttpBackend> = Arc::new(LiveConfigMockBackend {
        store: store.clone(),
        config: RwLock::new(cfg.clone()),
    });
    let state = HttpState::new(
        cfg,
        store.clone(),
        backend,
        logger,
        events_tx,
        CancellationToken::new(),
        DashboardAssets::default(),
    );

    RegressionHarness {
        temp_dir,
        state,
        store,
        generated_password,
    }
}

fn lan_peer() -> ConnectInfo<SocketAddr> {
    ConnectInfo(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 40100))
}

async fn body_json(resp: axum::response::Response) -> Value {
    let bytes = to_bytes(resp.into_body(), 2 * 1024 * 1024).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn assert_no_secret_in_logs(logger: &AppLogger, secret: &str) {
    for entry in logger.recent(None, 500) {
        assert!(
            !entry.message.contains(secret),
            "secret leaked into log entry: {}",
            entry.message
        );
        assert!(
            !entry.message.contains("$argon2"),
            "password hash leaked into log entry: {}",
            entry.message
        );
    }
}

#[tokio::test]
async fn test_case_insensitive_username_and_trimmed_password_login() {
    let h = setup_regression_harness("case-trim-login", true, |_| {});
    let password = h
        .generated_password
        .clone()
        .expect("fresh bootstrap must return generated password");

    // Generated password must be strictly alphanumeric (no punctuation surprises on mobile keyboards).
    assert_eq!(password.len(), 20);
    assert!(
        password.chars().all(|c| c.is_ascii_alphanumeric()),
        "bootstrap password must be alphanumeric: {password}"
    );
    assert_no_secret_in_logs(&h.state.logger, &password);

    let padded_password = format!("{password}\r\n ");
    let router = build_router(h.state.clone());
    let login_req = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/auth/login")
        .header(header::HOST, "127.0.0.1:8080")
        .header(header::CONTENT_TYPE, "application/json")
        .extension(lan_peer())
        .body(Body::from(
            json!({
                "username": "Admin",
                "password": padded_password,
            })
            .to_string(),
        ))
        .unwrap();

    let login_resp = router.clone().oneshot(login_req).await.unwrap();
    assert_eq!(login_resp.status(), StatusCode::OK);

    let set_cookie = login_resp
        .headers()
        .get(header::SET_COOKIE)
        .expect("Set-Cookie header missing")
        .to_str()
        .unwrap()
        .to_string();
    let cookie_pair = set_cookie.split(';').next().unwrap().to_string();
    assert!(
        cookie_pair.starts_with("ps5_agent_session="),
        "unexpected cookie: {set_cookie}"
    );

    let login_body = body_json(login_resp).await;
    assert_eq!(login_body["username"], "admin");

    let session_req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/auth/session")
        .header(header::HOST, "127.0.0.1:8080")
        .header(header::COOKIE, &cookie_pair)
        .extension(lan_peer())
        .body(Body::empty())
        .unwrap();

    let session_resp = router.oneshot(session_req).await.unwrap();
    assert_eq!(session_resp.status(), StatusCode::OK);
    let session_body = body_json(session_resp).await;
    assert_eq!(session_body["authenticated"], true);
    assert_eq!(session_body["username"], "admin");
    assert_eq!(session_body["auth_method"], "cookie");
}

#[tokio::test]
async fn test_admin_password_and_non_phc_password_hash_in_config() {
    // 1. Plaintext `security.admin_password` in config
    let custom_pass = "MyCustomPass123";
    let h1 = setup_regression_harness("admin-password", true, |cfg| {
        cfg.security.admin_password = Some(custom_pass.to_string());
    });
    assert!(
        h1.generated_password.is_none(),
        "admin_password in config should not trigger random password generation"
    );
    let stored_hash = h1
        .store
        .get_setting("auth.password_hash")
        .unwrap()
        .expect("auth.password_hash must be persisted");
    assert!(stored_hash.starts_with("$argon2id$"), "hash={stored_hash}");
    assert_no_secret_in_logs(&h1.state.logger, custom_pass);

    let router1 = build_router(h1.state.clone());
    let resp1 = router1
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/auth/login")
                .header(header::HOST, "127.0.0.1:8080")
                .header(header::CONTENT_TYPE, "application/json")
                .extension(lan_peer())
                .body(Body::from(
                    json!({ "username": "admin", "password": custom_pass }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp1.status(), StatusCode::OK);
    assert_no_secret_in_logs(&h1.state.logger, custom_pass);

    // 2. Non-PHC `security.password_hash` (user put plaintext in `password_hash`)
    let non_phc_pass = "AnotherPlain123";
    let h2 = setup_regression_harness("non-phc-password-hash", true, |cfg| {
        cfg.security.password_hash = Some(non_phc_pass.to_string());
    });
    assert!(h2.generated_password.is_none());
    let stored_hash2 = h2
        .store
        .get_setting("auth.password_hash")
        .unwrap()
        .expect("non-PHC password_hash should be hashed into DB settings");
    assert!(stored_hash2.starts_with("$argon2id$"), "hash={stored_hash2}");
    assert_no_secret_in_logs(&h2.state.logger, non_phc_pass);

    let router2 = build_router(h2.state.clone());
    let resp2 = router2
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/auth/login")
                .header(header::HOST, "127.0.0.1:8080")
                .header(header::CONTENT_TYPE, "application/json")
                .extension(lan_peer())
                .body(Body::from(
                    json!({ "username": "ADMIN", "password": non_phc_pass }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp2.status(), StatusCode::OK);
    assert_no_secret_in_logs(&h2.state.logger, non_phc_pass);
}

#[tokio::test]
async fn test_bootstrap_file_self_heal_when_db_hash_missing() {
    let temp_dir = new_temp_dir("bootstrap-self-heal");
    let cfg = base_test_config(&temp_dir);
    let logger = AppLogger::init(&cfg.logging);

    // Boot A generates bootstrap_credentials.txt and stores its hash in store_a.
    let store_a = Arc::new(MemoryStorage::default());
    let generated_pw = ensure_bootstrap_credentials_with_report(&cfg, store_a.as_ref(), &logger)
        .unwrap()
        .expect("initial boot should generate bootstrap password");
    assert!(temp_dir.join("bootstrap_credentials.txt").exists());

    // Boot B: fresh store with missing `auth.password_hash`, login BEFORE calling ensure_bootstrap_credentials.
    let store_b = Arc::new(MemoryStorage::default());
    seed_commander(&store_b);
    let (events_tx_b, _) = broadcast::channel(16);
    let backend_b: Arc<dyn HttpBackend> = Arc::new(LiveConfigMockBackend {
        store: store_b.clone(),
        config: RwLock::new(cfg.clone()),
    });
    let state_b = HttpState::new(
        cfg.clone(),
        store_b.clone(),
        backend_b,
        logger.clone(),
        events_tx_b,
        CancellationToken::new(),
        DashboardAssets::default(),
    );

    let resp = build_router(state_b)
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/auth/login")
                .header(header::HOST, "127.0.0.1:8080")
                .header(header::CONTENT_TYPE, "application/json")
                .extension(lan_peer())
                .body(Body::from(
                    json!({ "username": "admin", "password": generated_pw }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "login must self-heal from unrotated bootstrap_credentials.txt"
    );
    let healed_hash = store_b
        .get_setting("auth.password_hash")
        .unwrap()
        .expect("login self-heal must persist auth.password_hash");
    assert!(healed_hash.starts_with("$argon2id$"));

    // Boot C: fresh store with missing `auth.password_hash`, calling ensure_bootstrap_credentials_with_report
    // restores from bootstrap_credentials.txt instead of overwriting it with a new random password.
    let store_c = Arc::new(MemoryStorage::default());
    let report_c =
        ensure_bootstrap_credentials_with_report(&cfg, store_c.as_ref(), &logger).unwrap();
    assert!(
        report_c.is_none(),
        "must restore existing bootstrap_credentials.txt rather than generating a new password"
    );
    let restored_hash = store_c
        .get_setting("auth.password_hash")
        .unwrap()
        .expect("ensure_bootstrap_credentials_with_report must restore auth.password_hash");
    assert!(restored_hash.starts_with("$argon2id$"));
    assert_no_secret_in_logs(&logger, &generated_pw);

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_live_toggle_auth_disabled_via_settings() {
    let h = setup_regression_harness("live-auth-toggle", true, |_| {});
    let password = h.generated_password.clone().unwrap();
    let router = build_router(h.state.clone());

    // Initially auth is enabled -> unauthenticated GET /api/v1/agents returns 401.
    let unauth_resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/api/v1/agents")
                .header(header::HOST, "127.0.0.1:8080")
                .extension(lan_peer())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauth_resp.status(), StatusCode::UNAUTHORIZED);

    // Log in to obtain session cookie + CSRF token.
    let login_resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/auth/login")
                .header(header::HOST, "127.0.0.1:8080")
                .header(header::CONTENT_TYPE, "application/json")
                .extension(lan_peer())
                .body(Body::from(
                    json!({ "username": "admin", "password": password }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(login_resp.status(), StatusCode::OK);
    let cookie_pair = login_resp
        .headers()
        .get(header::SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();
    let login_json = body_json(login_resp).await;
    let csrf = login_json["csrf_token"].as_str().unwrap().to_string();

    // Toggle auth_enabled = false via PUT /api/v1/settings.
    let put_resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/api/v1/settings")
                .header(header::HOST, "127.0.0.1:8080")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::COOKIE, &cookie_pair)
                .header("x-csrf-token", &csrf)
                .extension(lan_peer())
                .body(Body::from(
                    json!({ "security": { "auth_enabled": false } }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put_resp.status(), StatusCode::OK);
    let put_json = body_json(put_resp).await;
    assert_eq!(put_json["security"]["auth_enabled"], false);

    // Immediately GET /api/v1/agents WITHOUT any cookie or bearer token -> 200 OK.
    let open_agents_resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/api/v1/agents")
                .header(header::HOST, "127.0.0.1:8080")
                .extension(lan_peer())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(open_agents_resp.status(), StatusCode::OK);

    // Session endpoint also reports auth_method == "disabled".
    let open_session_resp = router
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/api/v1/auth/session")
                .header(header::HOST, "127.0.0.1:8080")
                .extension(lan_peer())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(open_session_resp.status(), StatusCode::OK);
    let open_session_json = body_json(open_session_resp).await;
    assert_eq!(open_session_json["auth_method"], "disabled");
}

#[tokio::test]
async fn test_reusable_listener_rapid_restart_bind() {
    let listener1 = bind_reusable_listener("127.0.0.1:0").await.unwrap();
    let port = listener1.local_addr().unwrap().port();
    let addr = format!("127.0.0.1:{port}");

    // Exercise a real accepted TCP exchange so the socket experiences close/TIME_WAIT teardown.
    let accept_task = tokio::spawn(async move {
        let (mut server_stream, _) = listener1.accept().await.unwrap();
        let mut buf = [0u8; 4];
        let _ = server_stream.read_exact(&mut buf).await;
        let _ = server_stream.write_all(b"pong").await;
        drop(server_stream);
        drop(listener1);
    });

    let mut client = TcpStream::connect(&addr).await.unwrap();
    client.write_all(b"ping").await.unwrap();
    let mut reply = [0u8; 4];
    client.read_exact(&mut reply).await.unwrap();
    assert_eq!(&reply, b"pong");
    drop(client);
    accept_task.await.unwrap();

    // Immediately rebind on the exact same port.
    let listener2 = bind_reusable_listener(&addr)
        .await
        .expect("reusable listener must rebind immediately on the same port");
    assert_eq!(listener2.local_addr().unwrap().port(), port);
    drop(listener2);
}

#[tokio::test]
async fn test_password_rotation_blocks_config_fallback_revokes_other_sessions_and_caps_ttl() {
    let initial_pw = "InitialConfigPass123";
    let rotated_pw = "RotatedStrongPass456!";
    let h = setup_regression_harness("rotation-sec", true, |cfg| {
        cfg.security.admin_password = Some(initial_pw.to_string());
        cfg.security.session_ttl_hours = u64::MAX;
    });
    let router = build_router(h.state.clone());

    let login = |pw: &str| {
        let router = router.clone();
        let body = json!({ "username": "admin", "password": pw }).to_string();
        async move {
            let resp = router
                .oneshot(
                    Request::builder()
                        .method(Method::POST)
                        .uri("/api/v1/auth/login")
                        .header(header::HOST, "127.0.0.1:8080")
                        .header(header::CONTENT_TYPE, "application/json")
                        .extension(lan_peer())
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
            let status = resp.status();
            let cookie = resp
                .headers()
                .get(header::SET_COOKIE)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.split(';').next())
                .unwrap_or("")
                .to_string();
            let json = body_json(resp).await;
            let csrf = json["csrf_token"].as_str().unwrap_or("").to_string();
            (status, cookie, csrf)
        }
    };

    // Extreme session_ttl_hours = u64::MAX must not panic or wrap negative on login.
    let (s1, cookie_other, _) = login(initial_pw).await;
    assert_eq!(s1, StatusCode::OK);
    let (s2, cookie_caller, csrf_caller) = login(initial_pw).await;
    assert_eq!(s2, StatusCode::OK);

    // Rotate password using caller session.
    let rot_resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/v1/auth/password")
                .header(header::HOST, "127.0.0.1:8080")
                .header(header::COOKIE, &cookie_caller)
                .header("x-csrf-token", &csrf_caller)
                .header(header::CONTENT_TYPE, "application/json")
                .extension(lan_peer())
                .body(Body::from(
                    json!({
                        "current_password": initial_pw,
                        "new_password": rotated_pw,
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(rot_resp.status(), StatusCode::OK);

    // Old pre-rotation session must now be revoked (401).
    let stale_resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/api/v1/auth/session")
                .header(header::HOST, "127.0.0.1:8080")
                .header(header::COOKIE, &cookie_other)
                .extension(lan_peer())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(stale_resp.status(), StatusCode::UNAUTHORIZED);

    // Old `security.admin_password` fallback must NOT succeed or revert hash after rotation.
    let (old_status, _, _) = login(initial_pw).await;
    assert_eq!(old_status, StatusCode::UNAUTHORIZED);

    // Rotated password must still log in cleanly.
    let (new_status, cookie_new, _) = login(rotated_pw).await;
    assert_eq!(new_status, StatusCode::OK);
    let new_sess_resp = router
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/api/v1/auth/session")
                .header(header::HOST, "127.0.0.1:8080")
                .header(header::COOKIE, &cookie_new)
                .extension(lan_peer())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(new_sess_resp.status(), StatusCode::OK);
}
