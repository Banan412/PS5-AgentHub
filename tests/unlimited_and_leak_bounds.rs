use async_trait::async_trait;
use mock_ps5lm::{start_ephemeral, MockMode, MockPs5LmHandle};
use ps5_agent::shutdown::finalize_shutdown;
use ps5_agent::startup::{initialize, load_effective_config_with_warnings, CliOptions};
use ps5_agent::AppSystem;
use ps5_agent_config::{AppConfig, DatabaseConfig, InferenceConfig};
use ps5_agent_core::{
    new_id, now_iso, AgentDefinition, AgentEnvelope, AgentMessageType, AgentRole, ApiMode,
    ApprovalRecord, ApprovalStatus, AuditLogEntry, ChatMessage, ChatRole, Conversation,
    ConversationMode, DeliveryStatus, InferenceMessage, InferenceProvider, InferenceRequest,
    InferenceResponse, InferenceStreamChunk, MemoryItem, MemoryScope, ProviderHealth, Result,
    RuntimeLimits, SessionRecord, StorageBackend, StreamEvent, TaskEvent, TaskRecord, TaskStatus,
    ThinkingLevel, TokenUsage, ToolCallRequest, ToolExecutionRecord, ToolPermission, ToolSpec,
};
use ps5_agent_database::{JsonlStorage, SqliteStorage};
use ps5_agent_http::server::bind_reusable_listener;
use ps5_agent_http::{serve_with_listener, SlidingWindowRateLimiter};
use ps5_agent_inference::Ps5LmProvider;
use ps5_agent_manager::{AgentManager, CreateAgentInput};
use ps5_agent_messaging::{MessageBus, SendMessageRequest};
use ps5_agent_runtime::{AgentRuntime, RuntimeToolExecutor, TaskManager, ToolExecutionOutput};
use std::fs;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

fn make_task(
    id: &str,
    agent_id: &str,
    status: TaskStatus,
    created_at: &str,
    completed_at: Option<&str>,
) -> TaskRecord {
    TaskRecord {
        id: id.to_string(),
        conversation_id: None,
        user_request: format!("task {id}"),
        assigned_agent_id: agent_id.to_string(),
        parent_task_id: None,
        priority: 0,
        created_at: created_at.to_string(),
        started_at: None,
        completed_at: completed_at.map(str::to_string),
        status,
        result: None,
        error_info: None,
        has_side_effects: false,
        progress_pct: if status.is_terminal() { 100 } else { 0 },
        progress_note: None,
    }
}

fn make_session(id: &str, expires_at: &str, revoked: bool) -> SessionRecord {
    SessionRecord {
        id: id.to_string(),
        token_sha256: format!("hash-{id}"),
        csrf_token: format!("csrf-{id}"),
        username: "admin".to_string(),
        client_ip: "127.0.0.1".to_string(),
        created_at: "2020-01-01T00:00:00Z".to_string(),
        expires_at: expires_at.to_string(),
        revoked,
    }
}

struct ScriptedMutatingInference;

#[async_trait]
impl InferenceProvider for ScriptedMutatingInference {
    async fn generate(
        &self,
        _req: InferenceRequest,
        _cancel: CancellationToken,
    ) -> Result<InferenceResponse> {
        Ok(InferenceResponse {
            content: String::new(),
            reasoning: None,
            tool_calls: vec![ToolCallRequest {
                id: new_id(),
                name: "file_delete".to_string(),
                arguments: serde_json::json!({ "path": "ephemeral.txt" }),
            }],
            model: "mock".to_string(),
            finish_reason: Some("tool_calls".to_string()),
            usage: TokenUsage::default(),
            latency_ms: 1,
        })
    }

    async fn generate_stream(
        &self,
        req: InferenceRequest,
        tx: mpsc::Sender<InferenceStreamChunk>,
        cancel: CancellationToken,
    ) -> Result<InferenceResponse> {
        let resp = self.generate(req, cancel).await?;
        let _ = tx.send(InferenceStreamChunk::Completed(resp.clone())).await;
        Ok(resp)
    }

    async fn health_check(&self) -> ProviderHealth {
        ProviderHealth {
            reachable: true,
            loaded_model: Some("mock".to_string()),
            available_models: vec!["mock".to_string()],
            error: None,
        }
    }

    async fn list_models(&self) -> Result<Vec<String>> {
        Ok(vec!["mock".to_string()])
    }
}

struct MutatingToolExecutor;

#[async_trait]
impl RuntimeToolExecutor for MutatingToolExecutor {
    async fn list_tools(&self) -> Vec<ToolSpec> {
        vec![self.get_tool("file_delete").await.unwrap()]
    }

    async fn get_tool(&self, tool_id: &str) -> Option<ToolSpec> {
        if tool_id == "file_delete" {
            Some(ToolSpec {
                id: "file_delete".to_string(),
                name: "Delete File".to_string(),
                category: "files".to_string(),
                description: "Delete a workspace file".to_string(),
                input_schema: serde_json::json!({ "type": "object" }),
                output_schema: serde_json::json!({ "type": "object" }),
                required_permissions: vec![ToolPermission::FileDelete],
                requires_approval: true,
                is_mutating: true,
                enabled: true,
                timeout_secs: 10,
            })
        } else {
            None
        }
    }

    async fn execute_tool(
        &self,
        _agent: &AgentDefinition,
        _task_id: Option<&str>,
        _tool_id: &str,
        _arguments: serde_json::Value,
        _approved_bypass: bool,
        _cancel: CancellationToken,
    ) -> Result<ToolExecutionOutput> {
        Ok(ToolExecutionOutput {
            output: serde_json::json!({ "deleted": true }),
            is_mutating: true,
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_zero_limits_parse_without_clamping_and_allow_unlimited_agents_messages_memories_and_http_rate_limit(
) {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().to_path_buf();
    let config_path = data_dir.join("config.toml");

    let mock = start_ephemeral().await.unwrap();
    let probe = bind_reusable_listener("127.0.0.1:0").await.unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);
    let bind_addr = format!("127.0.0.1:{port}");

    let toml_body = format!(
        r#"
[inference]
base_url = "{}"
max_output_tokens = 0
auto_sync_from_ps5lm = false

[server]
bind_host = "127.0.0.1"
port = {}
allow_lan_only = true

[database]
driver = "sqlite"
retention_days = 0
max_rows_per_table = 0

[security]
auth_enabled = false
rate_limit_per_minute = 0
login_rate_limit_per_minute = 0

[agents]
max_agents = 0
max_active_tasks = 0
max_queued_tasks = 0
max_concurrent_inference = 0
max_messages_per_task = 0
max_reasoning_iterations = 0
default_task_timeout_secs = 0

[memory]
max_memories_per_agent = 0

[logging]
enable_klog = false
enable_toast = false

[runtime]
health_poll_interval_secs = 60
"#,
        mock.base_url(),
        port
    );
    fs::write(&config_path, toml_body).unwrap();

    let opts = CliOptions {
        config_path: Some(config_path),
        data_dir: Some(data_dir),
        bind: None,
        ps5lm_url: None,
    };
    let (cfg, warnings) = load_effective_config_with_warnings(&opts).unwrap();
    assert!(
        warnings.is_empty(),
        "setting 0 on all limit fields must parse without clamping warnings, got: {warnings:?}"
    );
    assert_eq!(cfg.agents.max_agents, 0);
    assert_eq!(cfg.agents.max_active_tasks, 0);
    assert_eq!(cfg.agents.max_queued_tasks, 0);
    assert_eq!(cfg.agents.max_concurrent_inference, 0);
    assert_eq!(cfg.agents.max_messages_per_task, 0);
    assert_eq!(cfg.agents.max_reasoning_iterations, 0);
    assert_eq!(cfg.agents.default_task_timeout_secs, 0);
    assert_eq!(cfg.inference.max_output_tokens, 0);
    assert_eq!(cfg.memory.max_memories_per_agent, 0);
    assert_eq!(cfg.security.rate_limit_per_minute, 0);
    assert_eq!(cfg.database.retention_days, 0);
    assert_eq!(cfg.database.max_rows_per_table, 0);

    let system = initialize(cfg).await.unwrap();

    // Verify >64 agents can be created (5 built-in + 70 custom = 75 agents)
    for i in 0..70 {
        system
            .agent_manager
            .create_agent(
                CreateAgentInput {
                    id: Some(format!("unlimited-agent-{i:03}")),
                    name: format!("Unlimited Agent {i}"),
                    description: "Unlimited capacity check".to_string(),
                    role: AgentRole::Custom,
                    role_label: None,
                    system_prompt: "Respond concisely.".to_string(),
                    model_config: None,
                    enabled_tools: vec!["file_read".to_string()],
                    denied_tools: None,
                    memory_permissions: None,
                    max_task_duration_secs: None,
                    max_reasoning_iterations: None,
                    priority: None,
                    can_communicate: Some(true),
                    enabled: Some(true),
                    sandbox_mode: None,
                    tool_profile: None,
                    is_temporary: false,
                    spawned_by: None,
                },
                "admin",
            )
            .unwrap();
    }
    let total_agents = system.agent_manager.list_agents().unwrap().len();
    assert_eq!(total_agents, 75, "expected 5 seeded + 70 created agents");

    // Verify unlimited inter-agent messages per task (50 > default 20)
    let task = system
        .runtime
        .task_manager()
        .create_task(
            "Unlimited messaging test".to_string(),
            "commander".to_string(),
            None,
            None,
            0,
            TaskStatus::Running,
        )
        .unwrap();
    for i in 0..50 {
        let sender = format!("unlimited-agent-{i:03}");
        system
            .message_bus
            .send(SendMessageRequest {
                sender_agent_id: sender,
                recipient_agent_id: "commander".to_string(),
                task_id: Some(task.id.clone()),
                message_type: AgentMessageType::AgentMessage,
                content: format!("unlimited message #{i}"),
                hop_count: 1,
            })
            .unwrap();
    }
    assert_eq!(
        system.message_bus.count_task_messages(&task.id).unwrap(),
        50
    );

    // Verify max_memories_per_agent = 0 does not archive active memories
    let now = now_iso();
    for i in 0..25 {
        system
            .memory
            .store_memory(MemoryItem {
                id: format!("mem-unlim-{i}"),
                scope: MemoryScope::Agent,
                namespace: "default".to_string(),
                owner_agent_id: Some("commander".to_string()),
                conversation_id: None,
                task_id: None,
                key: format!("k-{i}"),
                content: format!("memory value {i}"),
                summary: None,
                importance: 5,
                tags: Vec::new(),
                archived: false,
                created_at: now.clone(),
                updated_at: now.clone(),
            })
            .unwrap();
    }
    let active_mems = system
        .store
        .query_memories(None, None, Some("commander"), false, 100)
        .unwrap();
    assert_eq!(active_mems.len(), 25);

    // Verify rate_limit_per_minute = 0 bypasses HTTP rate limiting across 300 rapid requests
    let listener = bind_reusable_listener(&bind_addr).await.unwrap();
    let state = system.http_state.clone();
    let addr = bind_addr.clone();
    let server = tokio::spawn(async move { serve_with_listener(addr, listener, state).await });

    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(8)
        .build()
        .unwrap();
    for i in 0..300 {
        let resp = client
            .get(format!("http://{bind_addr}/api/v1/health"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::OK,
            "request #{i} should bypass rate limit when rate_limit_per_minute = 0"
        );
    }
    drop(client);

    system.shutdown.cancel();
    server.await.unwrap().unwrap();
    finalize_shutdown(&system);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_zero_max_output_tokens_omits_max_tokens_over_http() {
    let mock = start_ephemeral().await.unwrap();

    let mut inf_cfg = InferenceConfig::default();
    inf_cfg.base_url = mock.base_url();
    inf_cfg.max_output_tokens = 0;

    let provider = Ps5LmProvider::with_concurrency(inf_cfg, 0).unwrap();

    let req_zero_chat = InferenceRequest {
        model: Some("qwen-loaded".to_string()),
        api_mode: ApiMode::ChatCompletions,
        messages: vec![InferenceMessage {
            role: "user".to_string(),
            content: "Hello".to_string(),
            tool_call_id: None,
            name: None,
            tool_calls: None,
            image_urls: Vec::new(),
        }],
        tools: Vec::new(),
        temperature: 0.2,
        max_output_tokens: 0,
        context_limit: 8192,
        stream: false,
        thinking_level: ThinkingLevel::Medium,
    };
    provider
        .generate(req_zero_chat, CancellationToken::new())
        .await
        .unwrap();

    let req_zero_responses = InferenceRequest {
        model: Some("qwen-loaded".to_string()),
        api_mode: ApiMode::Responses,
        messages: vec![InferenceMessage {
            role: "user".to_string(),
            content: "Hello responses".to_string(),
            tool_call_id: None,
            name: None,
            tool_calls: None,
            image_urls: Vec::new(),
        }],
        tools: Vec::new(),
        temperature: 0.2,
        max_output_tokens: 0,
        context_limit: 8192,
        stream: false,
        thinking_level: ThinkingLevel::Low,
    };
    provider
        .generate(req_zero_responses, CancellationToken::new())
        .await
        .unwrap();

    let req_bounded_chat = InferenceRequest {
        model: Some("qwen-loaded".to_string()),
        api_mode: ApiMode::ChatCompletions,
        messages: vec![InferenceMessage {
            role: "user".to_string(),
            content: "Hello bounded".to_string(),
            tool_call_id: None,
            name: None,
            tool_calls: None,
            image_urls: Vec::new(),
        }],
        tools: Vec::new(),
        temperature: 0.2,
        max_output_tokens: 256,
        context_limit: 8192,
        stream: false,
        thinking_level: ThinkingLevel::Medium,
    };
    provider
        .generate(req_bounded_chat, CancellationToken::new())
        .await
        .unwrap();

    let recorded = mock.recorded_requests();
    let chat_reqs: Vec<_> = recorded
        .iter()
        .filter(|r| r.path.ends_with("/chat/completions"))
        .collect();
    assert_eq!(chat_reqs.len(), 2);
    assert!(
        chat_reqs[0].body.get("max_tokens").is_none(),
        "max_tokens must be omitted when max_output_tokens = 0, got: {}",
        chat_reqs[0].body
    );
    assert_eq!(chat_reqs[1].body["max_tokens"], 256);

    let resp_reqs: Vec<_> = recorded
        .iter()
        .filter(|r| r.path.ends_with("/responses"))
        .collect();
    assert_eq!(resp_reqs.len(), 1);
    assert!(
        resp_reqs[0].body.get("max_output_tokens").is_none(),
        "max_output_tokens must be omitted on /responses when 0, got: {}",
        resp_reqs[0].body
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_task_manager_completing_300_tasks_cleans_cancellation_tokens() {
    let temp = TempDir::new().unwrap();
    let db_path = temp.path().join("tasks.db");
    let db_cfg = DatabaseConfig {
        driver: "sqlite".to_string(),
        path: db_path.clone(),
        ..DatabaseConfig::default()
    };
    let store = Arc::new(SqliteStorage::open(&db_path, &db_cfg).unwrap());
    store.run_migrations().unwrap();

    let (events_tx, _) = broadcast::channel::<StreamEvent>(64);
    let limits = RuntimeLimits {
        max_agents: 0,
        max_active_tasks: 0,
        max_queued_tasks: 0,
        max_concurrent_inference: 0,
        max_messages_per_task: 0,
        max_reasoning_iterations: 0,
    };
    let tm = TaskManager::new(store, limits, events_tx);

    let mut task_ids = Vec::with_capacity(300);
    for i in 0..300 {
        let task = tm
            .create_task(
                format!("leak bound task {i}"),
                "commander".to_string(),
                None,
                None,
                0,
                TaskStatus::Pending,
            )
            .unwrap();
        let _tok = tm.register_token(&task.id, None);
        task_ids.push(task.id);
    }
    assert_eq!(tm.active_token_count().await, 300);

    for tid in &task_ids {
        tm.complete_task(tid, "done".to_string()).unwrap();
    }
    assert_eq!(
        tm.active_token_count().await,
        0,
        "all 300 completed tasks must release their CancellationTokens"
    );

    // Also verify fail_task and cancel_task clean up registered tokens
    let mut mixed_ids = Vec::with_capacity(30);
    for i in 0..30 {
        let task = tm
            .create_task(
                format!("mixed terminal task {i}"),
                "commander".to_string(),
                None,
                None,
                0,
                TaskStatus::Pending,
            )
            .unwrap();
        let _tok = tm.register_token(&task.id, None);
        mixed_ids.push(task.id);
    }
    assert_eq!(tm.active_token_count().await, 30);

    for (idx, tid) in mixed_ids.iter().enumerate() {
        if idx % 2 == 0 {
            tm.fail_task(tid, "synthetic error".to_string()).unwrap();
        } else {
            tm.cancel_task(tid, "synthetic cancel").unwrap();
        }
    }
    assert_eq!(tm.active_token_count().await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_agent_runtime_evict_pending_for_agent_and_cap_bounds() {
    let temp = TempDir::new().unwrap();
    let db_path = temp.path().join("runtime.db");
    let db_cfg = DatabaseConfig {
        driver: "sqlite".to_string(),
        path: db_path.clone(),
        ..DatabaseConfig::default()
    };
    let store: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::open(&db_path, &db_cfg).unwrap());
    store.run_migrations().unwrap();

    let mut cfg = AppConfig::with_data_dir(temp.path().to_path_buf());
    cfg.database.path = db_path;
    cfg.logging.log_dir = temp.path().join("logs");
    cfg.logging.enable_klog = false;
    cfg.logging.enable_toast = false;
    cfg.security.require_approval_for_mutating = true;
    cfg.agents.max_agents = 0;
    cfg.agents.max_active_tasks = 0;
    cfg.agents.max_queued_tasks = 0;
    cfg.agents.max_concurrent_inference = 0;
    cfg.agents.max_messages_per_task = 0;
    cfg.agents.max_reasoning_iterations = 0;

    let (events_tx, _) = broadcast::channel::<StreamEvent>(64);
    let agent_manager = Arc::new(AgentManager::new(
        store.clone(),
        cfg.agents.clone(),
        events_tx.clone(),
    ));
    agent_manager.seed_defaults().unwrap();

    agent_manager
        .create_agent(
            CreateAgentInput {
                id: Some("executor-two".to_string()),
                name: "Executor Two".to_string(),
                description: "Secondary executor".to_string(),
                role: AgentRole::Executor,
                role_label: None,
                system_prompt: "Execute actions.".to_string(),
                model_config: None,
                enabled_tools: vec!["file_delete".to_string()],
                denied_tools: None,
                memory_permissions: None,
                max_task_duration_secs: None,
                max_reasoning_iterations: None,
                priority: None,
                can_communicate: Some(true),
                enabled: Some(true),
                sandbox_mode: None,
                tool_profile: None,
                is_temporary: false,
                spawned_by: None,
            },
            "admin",
        )
        .unwrap();

    let message_bus = Arc::new(MessageBus::new(
        store.clone(),
        (&cfg.agents).into(),
        events_tx.clone(),
    ));
    let inference = Arc::new(ScriptedMutatingInference);
    let tools = Arc::new(MutatingToolExecutor);

    let rt = AgentRuntime::new(
        store.clone(),
        cfg,
        inference,
        tools,
        None,
        agent_manager,
        message_bus,
        events_tx,
    );

    let now = now_iso();
    for (cid, aid) in [("conv-exec-1", "executor"), ("conv-exec-2", "executor-two")] {
        store
            .create_conversation(&Conversation {
                id: cid.to_string(),
                title: cid.to_string(),
                mode: ConversationMode::Direct {
                    agent_id: aid.to_string(),
                },
                created_at: now.clone(),
                updated_at: now.clone(),
                archived: false,
                sort_order: 0,
            })
            .unwrap();
    }

    // Populate pending approvals for both agents and verify per-agent eviction
    for _ in 0..2 {
        let res = rt
            .handle_chat_message("conv-exec-1", "delete file", CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(res.status, TaskStatus::Waiting);
    }
    let res_other = rt
        .handle_chat_message("conv-exec-2", "delete file", CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(res_other.status, TaskStatus::Waiting);
    assert_eq!(rt.pending_approval_count(), 3);

    assert_eq!(rt.evict_pending_for_agent("executor"), 2);
    assert_eq!(rt.pending_approval_count(), 1);
    assert_eq!(rt.evict_pending_for_agent("executor-two"), 1);
    assert_eq!(rt.pending_approval_count(), 0);

    // Trigger > MAX_PENDING_APPROVALS (256) pauses and verify bounded map size + cleanup
    for i in 0..270 {
        let res = rt
            .handle_chat_message(
                "conv-exec-1",
                &format!("delete file #{i}"),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(res.status, TaskStatus::Waiting);
    }
    assert_eq!(
        rt.pending_approval_count(),
        256,
        "pending_approvals must stay bounded at MAX_PENDING_APPROVALS (256)"
    );
    assert_eq!(rt.evict_pending_for_agent("executor"), 256);
    assert_eq!(rt.pending_approval_count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_http_rate_limiter_bounded_buckets_and_sse_connection_guard_release() {
    // Part A: SlidingWindowRateLimiter bounds tracked IP buckets to MAX_TRACKED_IPS (4096)
    let limiter = SlidingWindowRateLimiter::new();
    let synthetic_ip = |idx: usize| -> IpAddr {
        let b1 = ((idx >> 8) & 0xff) as u8;
        let b2 = (idx & 0xff) as u8;
        IpAddr::V4(Ipv4Addr::new(10, b1, b2, 1))
    };

    for idx in 0..5000 {
        assert!(
            limiter.check_and_record(synthetic_ip(idx), 1).is_ok(),
            "first request for synthetic IP #{idx} must succeed"
        );
    }

    // Most recent IP must still be tracked and rate-limited
    assert!(
        limiter.check_and_record(synthetic_ip(4999), 1).is_err(),
        "most recent IP must remain tracked in the bounded bucket map"
    );
    // Oldest IP (idx 0) must have been evicted once tracked IPs exceeded 4096
    assert!(
        limiter.check_and_record(synthetic_ip(0), 1).is_ok(),
        "oldest IP must have been evicted to bound rate-limiter bucket memory"
    );

    // Part B: SseConnectionGuard releases slots cleanly when SSE client connections drop
    let temp = TempDir::new().unwrap();
    let mock = start_ephemeral().await.unwrap();
    let probe = bind_reusable_listener("127.0.0.1:0").await.unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);
    let bind_addr = format!("127.0.0.1:{port}");

    let mut cfg = AppConfig::with_data_dir(temp.path().to_path_buf());
    cfg.inference.base_url = mock.base_url();
    cfg.inference.auto_sync_from_ps5lm = false;
    cfg.server.bind_host = "127.0.0.1".to_string();
    cfg.server.port = port;
    cfg.security.auth_enabled = false;
    cfg.security.rate_limit_per_minute = 0;
    cfg.logging.enable_klog = false;
    cfg.logging.enable_toast = false;
    cfg.runtime.health_poll_interval_secs = 60;

    let system = initialize(cfg).await.unwrap();
    let listener = bind_reusable_listener(&bind_addr).await.unwrap();
    let state = system.http_state.clone();
    let addr = bind_addr.clone();
    let server = tokio::spawn(async move { serve_with_listener(addr, listener, state).await });

    // Open 32 concurrent SSE connections (MAX_SSE_CONNECTIONS = 32)
    let mut sse_streams = Vec::with_capacity(32);
    for i in 0..32 {
        let mut stream = TcpStream::connect(&bind_addr).await.unwrap();
        let req = format!(
            "GET /api/v1/events HTTP/1.1\r\nHost: {bind_addr}\r\nAccept: text/event-stream\r\n\r\n"
        );
        stream.write_all(req.as_bytes()).await.unwrap();
        let mut buf = [0u8; 512];
        let n = tokio::time::timeout(Duration::from_secs(3), stream.read(&mut buf))
            .await
            .unwrap()
            .unwrap();
        let head = String::from_utf8_lossy(&buf[..n]);
        assert!(
            head.contains("200 OK"),
            "SSE connection #{i} expected 200 OK, got: {head}"
        );
        sse_streams.push(stream);
    }
    assert_eq!(system.http_state.active_sse_connections(), 32);

    // 33rd SSE request must fail with 429 Too Many Requests
    let client = reqwest::Client::new();
    let overflow_resp = client
        .get(format!("http://{bind_addr}/api/v1/events"))
        .send()
        .await
        .unwrap();
    assert_eq!(overflow_resp.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);

    // Drop all 32 SSE connections and nudge broadcast channel so closed streams unwind
    drop(sse_streams);
    let deadline = Instant::now() + Duration::from_secs(5);
    while system.http_state.active_sse_connections() > 0 && Instant::now() < deadline {
        let _ = system.events_tx.send(StreamEvent::SystemAlert {
            level: "info".to_string(),
            message: "flush-closed-sse".to_string(),
        });
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        system.http_state.active_sse_connections(),
        0,
        "all 32 SSE slots must be released after client disconnects"
    );

    // Re-acquiring an SSE connection succeeds after slots are released
    let mut reopened = TcpStream::connect(&bind_addr).await.unwrap();
    let req = format!(
        "GET /api/v1/events HTTP/1.1\r\nHost: {bind_addr}\r\nAccept: text/event-stream\r\n\r\n"
    );
    reopened.write_all(req.as_bytes()).await.unwrap();
    let mut buf = [0u8; 512];
    let n = tokio::time::timeout(Duration::from_secs(3), reopened.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    let head = String::from_utf8_lossy(&buf[..n]);
    assert!(head.contains("200 OK"), "reopened SSE expected 200 OK, got: {head}");
    assert_eq!(system.http_state.active_sse_connections(), 1);
    drop(reopened);

    let deadline = Instant::now() + Duration::from_secs(5);
    while system.http_state.active_sse_connections() > 0 && Instant::now() < deadline {
        let _ = system.events_tx.send(StreamEvent::SystemAlert {
            level: "info".to_string(),
            message: "flush-reopened-sse".to_string(),
        });
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(system.http_state.active_sse_connections(), 0);

    system.shutdown.cancel();
    server.await.unwrap().unwrap();
    finalize_shutdown(&system);
}

fn verify_storage_summary(store: &dyn StorageBackend) {
    store.run_migrations().unwrap();

    // 1. Verify task_counts_summary() matches inserted tasks accurately
    let tasks = vec![
        make_task("sum-pend", "commander", TaskStatus::Pending, "2025-01-01T10:00:00Z", None),
        make_task("sum-run-1", "programmer", TaskStatus::Running, "2025-01-01T10:01:00Z", None),
        make_task("sum-run-2", "programmer", TaskStatus::Running, "2025-01-01T10:02:00Z", None),
        make_task("sum-wait", "researcher", TaskStatus::Waiting, "2025-01-01T10:03:00Z", None),
        make_task(
            "sum-done",
            "programmer",
            TaskStatus::Completed,
            "2025-01-01T10:04:00Z",
            Some("2025-01-01T10:05:00Z"),
        ),
        make_task(
            "sum-fail",
            "researcher",
            TaskStatus::Failed,
            "2025-01-01T10:04:00Z",
            Some("2025-01-01T10:06:00Z"),
        ),
        make_task(
            "sum-canc",
            "commander",
            TaskStatus::Cancelled,
            "2025-01-01T10:07:00Z",
            Some("2025-01-01T10:08:00Z"),
        ),
    ];
    for t in &tasks {
        store.upsert_task(t).unwrap();
    }

    let summary = store.task_counts_summary().unwrap();
    assert_eq!(summary.total, 7);
    assert_eq!(summary.pending, 1);
    assert_eq!(summary.running, 2);
    assert_eq!(summary.waiting, 1);
    assert_eq!(summary.completed, 1);
    assert_eq!(summary.failed, 1);
    assert_eq!(summary.cancelled, 1);

    let prog = summary.per_agent.get("programmer").unwrap();
    assert_eq!(prog.completed, 1);
    assert_eq!(prog.failed, 0);
    assert_eq!(prog.active, 2);
    assert_eq!(prog.latest_active_task_id.as_deref(), Some("sum-run-2"));
    assert_eq!(prog.last_activity.as_deref(), Some("2025-01-01T10:05:00Z"));

    let res = summary.per_agent.get("researcher").unwrap();
    assert_eq!(res.completed, 0);
    assert_eq!(res.failed, 1);
    assert_eq!(res.active, 1);
    assert_eq!(res.latest_active_task_id.as_deref(), Some("sum-wait"));
    assert_eq!(res.last_activity.as_deref(), Some("2025-01-01T10:06:00Z"));
}

fn verify_storage_pruning(store: &dyn StorageBackend) {
    store.run_migrations().unwrap();

    // 2. Verify prune_old_records(retention_days, max_rows_per_table)
    let old_ts = "2020-01-01T00:00:00Z";
    let fresh_ts = now_iso();

    store.create_session(&make_session("s-exp", old_ts, false)).unwrap();
    store
        .create_session(&make_session("s-rev", "2099-01-01T00:00:00Z", true))
        .unwrap();
    store
        .create_session(&make_session("s-act", "2099-01-01T00:00:00Z", false))
        .unwrap();

    // Old terminal tasks (must be pruned by retention_days = 30) + old running task (must survive)
    store
        .upsert_task(&make_task(
            "old-done-1",
            "commander",
            TaskStatus::Completed,
            old_ts,
            Some(old_ts),
        ))
        .unwrap();
    store
        .upsert_task(&make_task(
            "old-fail-2",
            "commander",
            TaskStatus::Failed,
            old_ts,
            Some(old_ts),
        ))
        .unwrap();
    store
        .upsert_task(&make_task(
            "old-running",
            "commander",
            TaskStatus::Running,
            old_ts,
            None,
        ))
        .unwrap();
    store
        .upsert_task(&make_task(
            "fresh-done",
            "commander",
            TaskStatus::Completed,
            &fresh_ts,
            Some(&fresh_ts),
        ))
        .unwrap();

    for (eid, tid, ts) in [
        ("ev-old-1", "old-done-1", old_ts),
        ("ev-old-2", "old-fail-2", old_ts),
        ("ev-run", "old-running", fresh_ts.as_str()),
    ] {
        store
            .append_task_event(&TaskEvent {
                id: eid.to_string(),
                task_id: tid.to_string(),
                timestamp: ts.to_string(),
                event_type: "state".to_string(),
                details: "test".to_string(),
            })
            .unwrap();
    }

    for i in 0..4 {
        let ts = if i < 2 { old_ts } else { fresh_ts.as_str() };
        store
            .append_audit_log(&AuditLogEntry {
                id: format!("aud-{i}"),
                timestamp: ts.to_string(),
                actor: "admin".to_string(),
                action: "test".to_string(),
                target: "t".to_string(),
                severity: "info".to_string(),
                outcome: "ok".to_string(),
                details_json: "{}".to_string(),
            })
            .unwrap();
        store
            .record_tool_execution(&ToolExecutionRecord {
                id: format!("te-{i}"),
                tool_id: "file_read".to_string(),
                agent_id: "programmer".to_string(),
                task_id: None,
                input_json: "{}".to_string(),
                output_json: None,
                success: true,
                error: None,
                duration_ms: 1,
                created_at: ts.to_string(),
            })
            .unwrap();
        store
            .append_agent_message(&AgentEnvelope {
                id: format!("am-{i}"),
                sender_agent_id: "commander".to_string(),
                recipient_agent_id: "programmer".to_string(),
                task_id: Some(if i < 2 { "old-done-1" } else { "fresh-done" }.to_string()),
                timestamp: ts.to_string(),
                message_type: AgentMessageType::AgentMessage,
                content: format!("msg {i}"),
                status: DeliveryStatus::Delivered,
                hop_count: 1,
            })
            .unwrap();
    }

    store
        .create_conversation(&Conversation {
            id: "conv-prune".to_string(),
            title: "Prune".to_string(),
            mode: ConversationMode::Direct {
                agent_id: "commander".to_string(),
            },
            created_at: old_ts.to_string(),
            updated_at: old_ts.to_string(),
            archived: false,
            sort_order: 0,
        })
        .unwrap();
    for (id, archived, ts) in [
        ("m-arch-old", true, old_ts),
        ("m-live-old", false, old_ts),
    ] {
        store
            .append_chat_message(&ChatMessage {
                id: id.to_string(),
                conversation_id: "conv-prune".to_string(),
                role: ChatRole::User,
                agent_id: None,
                content: id.to_string(),
                reasoning: None,
                tool_calls_json: None,
                task_id: None,
                token_count: 1,
                latency_ms: None,
                archived,
                created_at: ts.to_string(),
                attachments: Vec::new(),
            })
            .unwrap();
    }

    store
        .upsert_approval(&ApprovalRecord {
            id: "ap-old-approved".to_string(),
            task_id: None,
            agent_id: "executor".to_string(),
            operation: "file_delete".to_string(),
            tool_id: None,
            arguments_json: "{}".to_string(),
            reason: "r".to_string(),
            status: ApprovalStatus::Approved,
            created_at: old_ts.to_string(),
            resolved_at: Some(old_ts.to_string()),
            resolved_by: Some("admin".to_string()),
        })
        .unwrap();
    store
        .upsert_approval(&ApprovalRecord {
            id: "ap-old-pending".to_string(),
            task_id: None,
            agent_id: "executor".to_string(),
            operation: "file_delete".to_string(),
            tool_id: None,
            arguments_json: "{}".to_string(),
            reason: "r".to_string(),
            status: ApprovalStatus::Pending,
            created_at: old_ts.to_string(),
            resolved_at: None,
            resolved_by: None,
        })
        .unwrap();

    // (0, 0) purges expired/revoked sessions only and leaves tasks/logs untouched
    let r0 = store.prune_old_records(0, 0).unwrap();
    assert_eq!(r0.purged_sessions, 2);
    assert_eq!(r0.deleted_tasks, 0);
    assert_eq!(r0.deleted_audit_logs, 0);
    assert_eq!(r0.deleted_chat_messages, 0);

    // (30, 2) prunes old terminal records and caps logs/tables to max 2 rows
    let r1 = store.prune_old_records(30, 2).unwrap();
    assert!(r1.deleted_tasks >= 2);
    assert!(r1.deleted_task_events >= 2);
    assert!(r1.deleted_audit_logs >= 2);
    assert!(r1.deleted_tool_executions >= 2);
    assert!(r1.deleted_agent_messages >= 2);
    assert_eq!(r1.deleted_chat_messages, 1);

    assert!(store.get_task("old-done-1").unwrap().is_none());
    assert!(store.get_task("old-fail-2").unwrap().is_none());
    assert!(store.get_task("old-running").unwrap().is_some());
    assert!(store.get_task("fresh-done").unwrap().is_some());
    assert!(store.get_session_by_hash("hash-s-act").unwrap().is_some());
    assert!(store.get_session_by_hash("hash-s-exp").unwrap().is_none());
    assert!(store.get_approval("ap-old-pending").unwrap().is_some());
    assert!(store.get_approval("ap-old-approved").unwrap().is_none());
    assert!(store.list_audit_logs(10).unwrap().len() <= 2);
    assert!(store.list_tool_executions(None, 10).unwrap().len() <= 2);
    assert!(store.list_agent_messages(None, None, 10).unwrap().len() <= 2);
    assert_eq!(store.list_chat_messages("conv-prune", true, 10).unwrap().len(), 1);
    assert_eq!(store.integrity_check().unwrap(), "ok");
}

#[test]
fn test_sqlite_and_jsonl_summary_and_pruning() {
    let sqlite_summary_dir = TempDir::new().unwrap();
    let db_summary_path = sqlite_summary_dir.path().join("summary.db");
    let sqlite_summary_cfg = DatabaseConfig {
        driver: "sqlite".to_string(),
        path: db_summary_path.clone(),
        wal_mode: false,
        ..DatabaseConfig::default()
    };
    let sqlite_summary = SqliteStorage::open(&db_summary_path, &sqlite_summary_cfg).unwrap();
    verify_storage_summary(&sqlite_summary);

    let sqlite_prune_dir = TempDir::new().unwrap();
    let db_prune_path = sqlite_prune_dir.path().join("prune.db");
    let sqlite_prune_cfg = DatabaseConfig {
        driver: "sqlite".to_string(),
        path: db_prune_path.clone(),
        wal_mode: false,
        ..DatabaseConfig::default()
    };
    let sqlite_prune = SqliteStorage::open(&db_prune_path, &sqlite_prune_cfg).unwrap();
    verify_storage_pruning(&sqlite_prune);

    let jsonl_summary_dir = TempDir::new().unwrap();
    let jsonl_summary = JsonlStorage::open(jsonl_summary_dir.path()).unwrap();
    verify_storage_summary(&jsonl_summary);

    let jsonl_prune_dir = TempDir::new().unwrap();
    let jsonl_prune = JsonlStorage::open(jsonl_prune_dir.path()).unwrap();
    verify_storage_pruning(&jsonl_prune);
}

struct QueueTestHarness {
    _temp: TempDir,
    mock: MockPs5LmHandle,
    bind_addr: String,
    system: Arc<AppSystem>,
    client: reqwest::Client,
    server: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl QueueTestHarness {
    async fn boot() -> Self {
        let temp = TempDir::new().unwrap();
        let data_dir = temp.path().to_path_buf();
        let config_path = data_dir.join("config.toml");

        let mock = start_ephemeral().await.unwrap();
        let listener = bind_reusable_listener("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let bind_addr = format!("127.0.0.1:{port}");

        let toml_body = format!(
            r#"
[inference]
base_url = "{}"
auto_sync_from_ps5lm = false
prefill_kv_cache = false

[server]
bind_host = "127.0.0.1"
port = {}
allow_lan_only = true

[database]
driver = "sqlite"

[security]
auth_enabled = false
rate_limit_per_minute = 0
login_rate_limit_per_minute = 0

[agents]
max_active_tasks = 1
max_concurrent_inference = 4
max_queued_tasks = 32

[logging]
enable_klog = false
enable_toast = false

[runtime]
health_poll_interval_secs = 60
"#,
            mock.base_url(),
            port
        );
        fs::write(&config_path, toml_body).unwrap();

        let opts = CliOptions {
            config_path: Some(config_path),
            data_dir: Some(data_dir),
            bind: None,
            ps5lm_url: None,
        };
        let (cfg, _warnings) = load_effective_config_with_warnings(&opts).unwrap();
        assert_eq!(cfg.agents.max_active_tasks, 1);
        assert_eq!(cfg.agents.max_concurrent_inference, 4);

        let system = initialize(cfg).await.unwrap();
        let state = system.http_state.clone();
        let addr = bind_addr.clone();
        let server = tokio::spawn(async move { serve_with_listener(addr, listener, state).await });

        let client = reqwest::Client::builder()
            .pool_max_idle_per_host(8)
            .build()
            .unwrap();
        let health = client
            .get(format!("http://{bind_addr}/api/v1/health"))
            .send()
            .await
            .unwrap();
        assert_eq!(health.status(), reqwest::StatusCode::OK);

        Self {
            _temp: temp,
            mock,
            bind_addr,
            system,
            client,
            server,
        }
    }

    fn create_direct_conversation(&self, conv_id: &str, agent_id: &str) {
        let now = now_iso();
        self.system
            .store
            .create_conversation(&Conversation {
                id: conv_id.to_string(),
                title: conv_id.to_string(),
                mode: ConversationMode::Direct {
                    agent_id: agent_id.to_string(),
                },
                created_at: now.clone(),
                updated_at: now,
                archived: false,
                sort_order: 0,
            })
            .unwrap();
    }

    async fn send_message(
        &self,
        conv_id: &str,
        content: &str,
        agent_id: Option<&str>,
        async_dispatch: bool,
        immediate: bool,
    ) -> String {
        let mut payload = serde_json::json!({
            "content": content,
            "async_dispatch": async_dispatch,
            "immediate": immediate,
        });
        if let Some(aid) = agent_id {
            payload["agent_id"] = serde_json::json!(aid);
        }
        let resp = self
            .client
            .post(format!(
                "http://{}/api/v1/conversations/{conv_id}/messages",
                self.bind_addr
            ))
            .json(&payload)
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::CREATED,
            "POST /conversations/{conv_id}/messages failed"
        );
        let body: serde_json::Value = resp.json().await.unwrap();
        body["task"]["id"]
            .as_str()
            .expect("async/immediate dispatch must return task.id")
            .to_string()
    }

    /// Starts a single slow task that holds the sole `max_active_tasks = 1` semaphore slot
    /// in `MockMode::SlowDelay`, then switches the mock back to `Normal` once the slow request
    /// is sleeping inside the mock server so queued/expedited requests finish fast once admitted.
    async fn dispatch_slow_holder(&self, conv_id: &str, delay: Duration) -> String {
        self.mock.clear_recorded();
        self.mock.set_delay(delay);
        self.mock.set_mode(MockMode::SlowDelay);

        let holder_id = self
            .send_message(conv_id, "hold task queue slot", None, true, false)
            .await;

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let is_running = self.task_status(&holder_id) == TaskStatus::Running;
            if is_running && !self.mock.recorded_requests().is_empty() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for slow holder task {holder_id} to start inference"
            );
            tokio::time::sleep(Duration::from_millis(15)).await;
        }

        // Wait briefly so the mock handler snapshots SlowDelay and enters sleep(delay) before Normal is restored.
        tokio::time::sleep(Duration::from_millis(40)).await;
        self.mock.set_mode(MockMode::Normal);
        holder_id
    }

    async fn cancel_task_http(&self, task_id: &str) -> TaskRecord {
        let resp = self
            .client
            .post(format!(
                "http://{}/api/v1/tasks/{task_id}/cancel",
                self.bind_addr
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        resp.json::<TaskRecord>().await.unwrap()
    }

    async fn cancel_conversation_http(
        &self,
        conv_id: &str,
        body: Option<serde_json::Value>,
    ) -> usize {
        let req = self.client.post(format!(
            "http://{}/api/v1/conversations/{conv_id}/cancel",
            self.bind_addr
        ));
        let resp = match body {
            Some(v) => req.json(&v).send().await.unwrap(),
            None => req.send().await.unwrap(),
        };
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        let parsed: serde_json::Value = resp.json().await.unwrap();
        parsed["cancelled_count"].as_u64().unwrap() as usize
    }

    async fn expedite_task_http(&self, task_id: &str) -> TaskRecord {
        let resp = self
            .client
            .post(format!(
                "http://{}/api/v1/tasks/{task_id}/expedite",
                self.bind_addr
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        let parsed: serde_json::Value = resp.json().await.unwrap();
        serde_json::from_value::<TaskRecord>(parsed["task"].clone()).unwrap()
    }

    fn task_status(&self, task_id: &str) -> TaskStatus {
        self.system
            .store
            .get_task(task_id)
            .unwrap()
            .unwrap_or_else(|| panic!("task '{task_id}' missing from store"))
            .status
    }

    async fn wait_for_task_status(
        &self,
        task_id: &str,
        expected: TaskStatus,
        timeout: Duration,
    ) -> TaskRecord {
        let deadline = Instant::now() + timeout;
        loop {
            let rec = self
                .system
                .store
                .get_task(task_id)
                .unwrap()
                .unwrap_or_else(|| panic!("task '{task_id}' missing from store"));
            if rec.status == expected {
                return rec;
            }
            assert!(
                !rec.status.is_terminal() || rec.status == expected,
                "task '{task_id}' reached unexpected terminal status {:?} while waiting for {:?}",
                rec.status,
                expected
            );
            assert!(
                Instant::now() < deadline,
                "timed out waiting for task '{task_id}' to reach {:?}; current status {:?}",
                expected,
                rec.status
            );
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    }

    async fn shutdown(self) {
        drop(self.client);
        self.system.shutdown.cancel();
        self.server.await.unwrap().unwrap();
        finalize_shutdown(&self.system);
        self.mock.shutdown();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cancel_pending_queued_tasks_never_resurrect() {
    let h = QueueTestHarness::boot().await;
    h.create_direct_conversation("conv-pending-cancel", "commander");

    let holder_id = h
        .dispatch_slow_holder("conv-pending-cancel", Duration::from_millis(2500))
        .await;

    let cancel_1 = h
        .send_message("conv-pending-cancel", "queued cancel 1", None, true, false)
        .await;
    let cancel_2 = h
        .send_message("conv-pending-cancel", "queued cancel 2", None, true, false)
        .await;
    let keep_id = h
        .send_message("conv-pending-cancel", "queued keep running", None, true, false)
        .await;

    assert_eq!(h.task_status(&holder_id), TaskStatus::Running);
    assert_eq!(h.task_status(&cancel_1), TaskStatus::Pending);
    assert_eq!(h.task_status(&cancel_2), TaskStatus::Pending);
    assert_eq!(h.task_status(&keep_id), TaskStatus::Pending);

    let c1_rec = h.cancel_task_http(&cancel_1).await;
    let c2_rec = h.cancel_task_http(&cancel_2).await;
    assert_eq!(c1_rec.status, TaskStatus::Cancelled);
    assert_eq!(c2_rec.status, TaskStatus::Cancelled);

    // Holder is still occupying the sole slot and the uncancelled queued task waits cleanly.
    assert_eq!(h.task_status(&holder_id), TaskStatus::Running);
    assert_eq!(h.task_status(&keep_id), TaskStatus::Pending);

    // Wait for both holder and uncancelled queued task to drain and complete.
    h.wait_for_task_status(&holder_id, TaskStatus::Completed, Duration::from_secs(8))
        .await;
    h.wait_for_task_status(&keep_id, TaskStatus::Completed, Duration::from_secs(8))
        .await;

    // Confirm cancelled pending tasks stay Cancelled even across recovery/drain cycles.
    tokio::time::sleep(Duration::from_millis(150)).await;
    let recovery = h.system.runtime.recover_on_startup().await.unwrap();
    assert!(
        recovery.resumed_tasks.is_empty(),
        "cancelled tasks must not be resumed on recovery: {:?}",
        recovery.resumed_tasks
    );
    assert!(recovery.paused_side_effect_tasks.is_empty());
    assert!(recovery.already_waiting_tasks.is_empty());
    tokio::time::sleep(Duration::from_millis(50)).await;

    for cancelled_id in [&cancel_1, &cancel_2] {
        let final_rec = h.system.store.get_task(cancelled_id).unwrap().unwrap();
        assert_eq!(final_rec.status, TaskStatus::Cancelled);
        assert!(
            final_rec.started_at.is_none(),
            "cancelled pending task {cancelled_id} must never transition to running"
        );
        let events = h.system.store.list_task_events(cancelled_id).unwrap();
        assert!(events.iter().any(|e| e.event_type == "task_cancelled"));
        assert!(
            !events
                .iter()
                .any(|e| e.event_type == "task_running" || e.event_type == "task_completed"),
            "cancelled pending task {cancelled_id} resurrected: {events:?}"
        );
    }

    assert_eq!(h.system.runtime.task_manager().active_token_count().await, 0);
    h.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cancel_conversation_tasks_scopes_by_conversation_and_agent() {
    let h = QueueTestHarness::boot().await;
    h.create_direct_conversation("conv-hold", "commander");
    h.create_direct_conversation("conv-target", "commander");
    h.create_direct_conversation("conv-other", "commander");

    // Part A: optional `agent_id` cancels only that agent within the target conversation.
    let holder_a = h
        .dispatch_slow_holder("conv-hold", Duration::from_millis(2500))
        .await;

    let target_prog_1 = h
        .send_message(
            "conv-target",
            "cancel programmer only",
            Some("programmer"),
            true,
            false,
        )
        .await;
    let target_res_1 = h
        .send_message(
            "conv-target",
            "keep researcher in target conv",
            Some("researcher"),
            true,
            false,
        )
        .await;
    let other_prog_1 = h
        .send_message(
            "conv-other",
            "keep programmer in other conv",
            Some("programmer"),
            true,
            false,
        )
        .await;

    assert_eq!(h.task_status(&holder_a), TaskStatus::Running);
    assert_eq!(h.task_status(&target_prog_1), TaskStatus::Pending);
    assert_eq!(h.task_status(&target_res_1), TaskStatus::Pending);
    assert_eq!(h.task_status(&other_prog_1), TaskStatus::Pending);

    let count_a = h
        .cancel_conversation_http(
            "conv-target",
            Some(serde_json::json!({ "agent_id": "programmer" })),
        )
        .await;
    assert_eq!(count_a, 1);

    assert_eq!(h.task_status(&target_prog_1), TaskStatus::Cancelled);
    assert_eq!(h.task_status(&target_res_1), TaskStatus::Pending);
    assert_eq!(h.task_status(&other_prog_1), TaskStatus::Pending);

    h.wait_for_task_status(&holder_a, TaskStatus::Completed, Duration::from_secs(8))
        .await;
    h.wait_for_task_status(&target_res_1, TaskStatus::Completed, Duration::from_secs(8))
        .await;
    h.wait_for_task_status(&other_prog_1, TaskStatus::Completed, Duration::from_secs(8))
        .await;

    tokio::time::sleep(Duration::from_millis(80)).await;
    let prog_rec = h.system.store.get_task(&target_prog_1).unwrap().unwrap();
    assert_eq!(prog_rec.status, TaskStatus::Cancelled);
    assert!(prog_rec.started_at.is_none());

    // Part B: omitting `agent_id` cancels all agents in target conversation while other conversations finish.
    let holder_b = h
        .dispatch_slow_holder("conv-hold", Duration::from_millis(2500))
        .await;

    let target_prog_2 = h
        .send_message(
            "conv-target",
            "cancel all prog",
            Some("programmer"),
            true,
            false,
        )
        .await;
    let target_res_2 = h
        .send_message(
            "conv-target",
            "cancel all res",
            Some("researcher"),
            true,
            false,
        )
        .await;
    let other_cmd_2 = h
        .send_message(
            "conv-other",
            "other conv keeps running",
            Some("commander"),
            true,
            false,
        )
        .await;

    assert_eq!(h.task_status(&holder_b), TaskStatus::Running);
    let count_b = h.cancel_conversation_http("conv-target", None).await;
    assert_eq!(count_b, 2);

    assert_eq!(h.task_status(&target_prog_2), TaskStatus::Cancelled);
    assert_eq!(h.task_status(&target_res_2), TaskStatus::Cancelled);
    assert_eq!(h.task_status(&other_cmd_2), TaskStatus::Pending);

    h.wait_for_task_status(&holder_b, TaskStatus::Completed, Duration::from_secs(8))
        .await;
    h.wait_for_task_status(&other_cmd_2, TaskStatus::Completed, Duration::from_secs(8))
        .await;

    tokio::time::sleep(Duration::from_millis(100)).await;
    for tid in [&target_prog_2, &target_res_2] {
        let rec = h.system.store.get_task(tid).unwrap().unwrap();
        assert_eq!(rec.status, TaskStatus::Cancelled);
        assert!(rec.started_at.is_none());
    }

    assert_eq!(h.system.runtime.task_manager().active_token_count().await, 0);
    h.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_immediate_send_and_expedite_bypass_queue() {
    let h = QueueTestHarness::boot().await;
    h.create_direct_conversation("conv-hold", "commander");
    h.create_direct_conversation("conv-queue", "commander");

    let holder_id = h
        .dispatch_slow_holder("conv-hold", Duration::from_millis(3000))
        .await;

    let queued_id = h
        .send_message(
            "conv-queue",
            "standard queued message",
            Some("programmer"),
            true,
            false,
        )
        .await;
    assert_eq!(h.task_status(&holder_id), TaskStatus::Running);
    assert_eq!(h.task_status(&queued_id), TaskStatus::Pending);

    // `immediate: true` on SendMessageRequest (even with async_dispatch omitted/false) bypasses queue immediately.
    let immediate_id = h
        .send_message(
            "conv-queue",
            "urgent immediate bypass",
            Some("researcher"),
            false,
            true,
        )
        .await;
    h.wait_for_task_status(&immediate_id, TaskStatus::Completed, Duration::from_secs(2))
        .await;
    assert_eq!(
        h.task_status(&holder_id),
        TaskStatus::Running,
        "immediate task must finish while slow holder still occupies the queue slot"
    );
    assert_eq!(
        h.task_status(&queued_id),
        TaskStatus::Pending,
        "non-expedited queued task must remain pending behind the slow holder"
    );

    // `POST /api/v1/tasks/{id}/expedite` wakes the pending waiter immediately out of the queue.
    let exp_rec = h.expedite_task_http(&queued_id).await;
    assert_eq!(exp_rec.id, queued_id);
    assert_eq!(
        exp_rec.progress_note.as_deref(),
        Some("Expedited — starting immediately")
    );

    h.wait_for_task_status(&queued_id, TaskStatus::Completed, Duration::from_secs(2))
        .await;
    assert_eq!(
        h.task_status(&holder_id),
        TaskStatus::Running,
        "expedited task must complete before slow holder releases the queue slot"
    );

    let queued_events = h.system.store.list_task_events(&queued_id).unwrap();
    assert!(queued_events.iter().any(|e| e.event_type == "task_expedited"));
    assert!(queued_events.iter().any(|e| e.event_type == "task_running"));

    h.wait_for_task_status(&holder_id, TaskStatus::Completed, Duration::from_secs(8))
        .await;
    assert_eq!(h.system.runtime.task_manager().active_token_count().await, 0);
    h.shutdown().await;
}
