use mock_ps5lm::{MockPs5LmHandle, ScriptedResponse, ScriptedToolCall};
use ps5_agent::shutdown::finalize_shutdown;
use ps5_agent::startup::initialize;
use ps5_agent::AppSystem;
use ps5_agent_config::AppConfig;
use ps5_agent_core::{
    ChatMessage, ChatRole, Conversation, ConversationMode, ScheduleKind, ScheduleRecord,
    StorageBackend, StreamEvent, TaskRecord, TaskStatus,
};
use ps5_agent_database::{JsonlStorage, SqliteStorage};
use ps5_agent_http::{
    serve_with_listener, HttpScheduleAiResponse, SendMessageRequest, SendMessageResponse,
};
use ps5_agent_inference::Ps5LmProvider;
use ps5_agent_manager::AgentManager;
use ps5_agent_messaging::MessageBus;
use ps5_agent_runtime::{AgentRuntime, CreateScheduleInput, NoopToolExecutor};
use reqwest::{Client, StatusCode};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

fn make_schedule_record(
    id: &str,
    title: &str,
    prompt: &str,
    kind: ScheduleKind,
    run_at: Option<&str>,
    interval_secs: Option<u64>,
    daily_time: Option<&str>,
    cron_expr: Option<&str>,
    next_run_at: Option<&str>,
    created_at: &str,
) -> ScheduleRecord {
    serde_json::from_value(json!({
        "id": id,
        "title": title,
        "prompt": prompt,
        "target_agent_id": "commander",
        "conversation_id": null,
        "kind": kind,
        "run_at": run_at,
        "interval_secs": interval_secs,
        "daily_time": daily_time,
        "cron_expr": cron_expr,
        "timezone_offset_mins": 60,
        "enabled": true,
        "next_run_at": next_run_at,
        "last_run_at": null,
        "last_status": null,
        "last_error": null,
        "last_task_id": null,
        "run_count": 0,
        "created_at": created_at,
        "updated_at": created_at,
    }))
    .expect("valid ScheduleRecord JSON fixture")
}

fn make_create_schedule_input(
    title: &str,
    prompt: &str,
    target_agent_id: Option<&str>,
    kind: ScheduleKind,
    run_at: Option<&str>,
    interval_secs: Option<u64>,
) -> CreateScheduleInput {
    serde_json::from_value(json!({
        "title": title,
        "prompt": prompt,
        "target_agent_id": target_agent_id,
        "conversation_id": null,
        "kind": kind,
        "run_at": run_at,
        "interval_secs": interval_secs,
        "daily_time": null,
        "cron_expr": null,
        "timezone_offset_mins": 0,
        "enabled": true,
    }))
    .expect("valid CreateScheduleInput JSON fixture")
}

fn build_test_config(data_dir: &TempDir, mock_base_url: &str) -> AppConfig {
    let mut cfg = AppConfig::with_data_dir(data_dir.path().to_path_buf());
    cfg.server.bind_host = "127.0.0.1".to_string();
    cfg.server.port = 0;
    cfg.inference.base_url = mock_base_url.to_string();
    cfg.inference.model = "qwen-loaded".to_string();
    cfg.inference.prefill_kv_cache = false;
    cfg.inference.auto_sync_from_ps5lm = false;
    cfg.inference.max_retries = 0;
    cfg.security.auth_enabled = false;
    cfg.security.require_approval_for_mutating = false;
    cfg.security.rate_limit_per_minute = 0;
    cfg.agents.max_concurrent_inference = 4;
    cfg
}

fn assert_schedule_crud_parity(store: &dyn StorageBackend) -> Vec<ScheduleRecord> {
    assert!(store.list_schedules(0).expect("initial list").is_empty());

    let once = make_schedule_record(
        "sched-once",
        "One-shot check",
        "Run startup diagnostics once",
        ScheduleKind::Once,
        Some("2026-06-01T12:00:00Z"),
        None,
        None,
        None,
        Some("2026-06-01T12:00:00Z"),
        "2026-01-01T00:00:00Z",
    );
    let interval = make_schedule_record(
        "sched-interval",
        "Periodic telemetry",
        "Poll system counters",
        ScheduleKind::Interval,
        None,
        Some(120),
        None,
        None,
        Some("2026-06-01T12:02:00Z"),
        "2026-01-02T00:00:00Z",
    );
    let daily = make_schedule_record(
        "sched-daily",
        "Daily digest",
        "Summarize daily alerts",
        ScheduleKind::Daily,
        None,
        None,
        Some("07:30"),
        None,
        Some("2026-06-02T06:30:00Z"),
        "2026-01-03T00:00:00Z",
    );
    let cron = make_schedule_record(
        "sched-cron",
        "Weekday morning audit",
        "Audit workspace health",
        ScheduleKind::Cron,
        None,
        None,
        None,
        Some("0 9 * * 1-5"),
        Some("2026-06-02T08:00:00Z"),
        "2026-01-04T00:00:00Z",
    );

    store.upsert_schedule(&once).expect("upsert once");
    store.upsert_schedule(&interval).expect("upsert interval");
    store.upsert_schedule(&daily).expect("upsert daily");
    store.upsert_schedule(&cron).expect("upsert cron");

    assert!(store.get_schedule("missing").expect("get missing").is_none());

    let fetched_interval = store
        .get_schedule("sched-interval")
        .expect("get interval")
        .expect("interval exists");
    assert_eq!(fetched_interval.title, "Periodic telemetry");
    assert_eq!(fetched_interval.kind, ScheduleKind::Interval);
    assert_eq!(fetched_interval.interval_secs, Some(120));
    assert_eq!(fetched_interval.timezone_offset_mins, 60);
    assert!(fetched_interval.enabled);
    assert_eq!(fetched_interval.run_count, 0);

    let all = store.list_schedules(0).expect("list all schedules");
    let ordered_ids: Vec<&str> = all.iter().map(|s| s.id.as_str()).collect();
    assert_eq!(
        ordered_ids,
        vec!["sched-cron", "sched-daily", "sched-interval", "sched-once"]
    );

    let limited = store.list_schedules(2).expect("list limited schedules");
    let limited_ids: Vec<&str> = limited.iter().map(|s| s.id.as_str()).collect();
    assert_eq!(limited_ids, vec!["sched-cron", "sched-daily"]);

    let updated_interval: ScheduleRecord = serde_json::from_value(json!({
        "id": "sched-interval",
        "title": "Periodic telemetry v2",
        "prompt": "Poll system counters and temperatures",
        "target_agent_id": "commander",
        "conversation_id": "conv-sched-interval",
        "kind": ScheduleKind::Interval,
        "run_at": null,
        "interval_secs": 300,
        "daily_time": null,
        "cron_expr": null,
        "timezone_offset_mins": 120,
        "enabled": false,
        "next_run_at": null,
        "last_run_at": "2026-06-01T12:00:05Z",
        "last_status": "completed",
        "last_error": null,
        "last_task_id": "task-xyz",
        "run_count": 3,
        "created_at": "2026-01-02T00:00:00Z",
        "updated_at": "2026-06-01T12:00:05Z",
    }))
    .expect("updated interval fixture");
    store
        .upsert_schedule(&updated_interval)
        .expect("overwrite existing schedule");

    let reloaded_interval = store
        .get_schedule("sched-interval")
        .expect("get updated interval")
        .expect("updated interval exists");
    assert_eq!(reloaded_interval.title, "Periodic telemetry v2");
    assert_eq!(reloaded_interval.interval_secs, Some(300));
    assert_eq!(reloaded_interval.timezone_offset_mins, 120);
    assert!(!reloaded_interval.enabled);
    assert!(reloaded_interval.next_run_at.is_none());
    assert_eq!(reloaded_interval.run_count, 3);
    assert_eq!(reloaded_interval.last_status.as_deref(), Some("completed"));
    assert_eq!(reloaded_interval.last_task_id.as_deref(), Some("task-xyz"));
    assert_eq!(
        reloaded_interval.conversation_id.as_deref(),
        Some("conv-sched-interval")
    );
    assert_eq!(
        store.list_schedules(0).expect("list after overwrite").len(),
        4
    );

    store.delete_schedule("sched-daily").expect("delete daily");
    store
        .delete_schedule("does-not-exist")
        .expect("delete missing is ok");
    assert!(store
        .get_schedule("sched-daily")
        .expect("get deleted")
        .is_none());

    let remaining = store.list_schedules(0).expect("list after delete");
    let remaining_ids: Vec<&str> = remaining.iter().map(|s| s.id.as_str()).collect();
    assert_eq!(remaining_ids, vec!["sched-cron", "sched-interval", "sched-once"]);
    remaining
}

struct ScheduleHttpHarness {
    _temp_dir: TempDir,
    mock: MockPs5LmHandle,
    system: Arc<AppSystem>,
    base_url: String,
    client: Client,
    server_task: JoinHandle<()>,
}

impl ScheduleHttpHarness {
    async fn start() -> Self {
        let temp_dir = TempDir::new().expect("create harness temp dir");
        let mock = mock_ps5lm::start_ephemeral()
            .await
            .expect("start mock ps5lm");
        let cfg = build_test_config(&temp_dir, &mock.base_url());
        let system = initialize(cfg).await.expect("initialize system");

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test HTTP listener");
        let addr = listener.local_addr().expect("listener local addr");
        let base_url = format!("http://{addr}");
        let http_state = Arc::clone(&system.http_state);
        let bind_addr = addr.to_string();
        let server_task = tokio::spawn(async move {
            let _ = serve_with_listener(bind_addr, listener, http_state).await;
        });

        let client = Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .expect("build reqwest client");

        Self {
            _temp_dir: temp_dir,
            mock,
            system,
            base_url,
            client,
            server_task,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    async fn wait_task_terminal(&self, task_id: &str) -> TaskRecord {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let resp = self
                .client
                .get(self.url(&format!("/api/v1/tasks/{task_id}")))
                .send()
                .await
                .expect("GET task");
            assert_eq!(resp.status(), StatusCode::OK);
            let task: TaskRecord = resp.json().await.expect("decode TaskRecord");
            if task.status.is_terminal() {
                return task;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for task {task_id} (last status={:?})",
                task.status
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    async fn shutdown(self) {
        finalize_shutdown(&self.system);
        let _ = tokio::time::timeout(Duration::from_secs(5), self.server_task).await;
        self.mock.shutdown();
    }
}

#[test]
fn schedules_sqlite_and_jsonl_persistence_parity() {
    let sqlite_dir = TempDir::new().expect("sqlite temp dir");
    let db_path = sqlite_dir.path().join("schedules_parity.db");
    let db_cfg = AppConfig::with_data_dir(sqlite_dir.path().to_path_buf()).database;
    let sqlite = SqliteStorage::open(&db_path, &db_cfg).expect("open sqlite");
    sqlite.run_migrations().expect("sqlite migrations");
    let expected_sqlite = assert_schedule_crud_parity(&sqlite);
    drop(sqlite);

    let jsonl_dir = TempDir::new().expect("jsonl temp dir");
    let jsonl_path = jsonl_dir.path().join("jsonl_db");
    let jsonl = JsonlStorage::open(&jsonl_path).expect("open jsonl");
    jsonl.run_migrations().expect("jsonl migrations");
    let expected_jsonl = assert_schedule_crud_parity(&jsonl);
    drop(jsonl);

    // Reopen both backends from disk and verify durable persistence and exact cross-backend parity
    let sqlite_reopened = SqliteStorage::open(&db_path, &db_cfg).expect("reopen sqlite");
    let sqlite_after = sqlite_reopened
        .list_schedules(0)
        .expect("list reopened sqlite");

    let jsonl_reopened = JsonlStorage::open(&jsonl_path).expect("reopen jsonl");
    let jsonl_after = jsonl_reopened
        .list_schedules(0)
        .expect("list reopened jsonl");

    assert_eq!(sqlite_after.len(), 3);
    assert_eq!(jsonl_after.len(), 3);
    assert!(sqlite_reopened
        .get_schedule("sched-daily")
        .expect("reopened sqlite deleted check")
        .is_none());
    assert!(jsonl_reopened
        .get_schedule("sched-daily")
        .expect("reopened jsonl deleted check")
        .is_none());

    for (a, b) in expected_sqlite.iter().zip(sqlite_after.iter()) {
        assert_eq!(a.id, b.id);
        assert_eq!(a.title, b.title);
        assert_eq!(a.prompt, b.prompt);
        assert_eq!(a.kind, b.kind);
        assert_eq!(a.interval_secs, b.interval_secs);
        assert_eq!(a.cron_expr, b.cron_expr);
        assert_eq!(a.enabled, b.enabled);
        assert_eq!(a.run_count, b.run_count);
        assert_eq!(a.last_status, b.last_status);
        assert_eq!(a.conversation_id, b.conversation_id);
        assert_eq!(a.created_at, b.created_at);
    }

    for (sq, jl) in sqlite_after.iter().zip(jsonl_after.iter()) {
        assert_eq!(sq.id, jl.id);
        assert_eq!(sq.title, jl.title);
        assert_eq!(sq.prompt, jl.prompt);
        assert_eq!(sq.target_agent_id, jl.target_agent_id);
        assert_eq!(sq.conversation_id, jl.conversation_id);
        assert_eq!(sq.kind, jl.kind);
        assert_eq!(sq.run_at, jl.run_at);
        assert_eq!(sq.interval_secs, jl.interval_secs);
        assert_eq!(sq.daily_time, jl.daily_time);
        assert_eq!(sq.cron_expr, jl.cron_expr);
        assert_eq!(sq.timezone_offset_mins, jl.timezone_offset_mins);
        assert_eq!(sq.enabled, jl.enabled);
        assert_eq!(sq.next_run_at, jl.next_run_at);
        assert_eq!(sq.last_run_at, jl.last_run_at);
        assert_eq!(sq.last_status, jl.last_status);
        assert_eq!(sq.last_task_id, jl.last_task_id);
        assert_eq!(sq.run_count, jl.run_count);
        assert_eq!(sq.created_at, jl.created_at);
        assert_eq!(sq.updated_at, jl.updated_at);
    }
    for (a, b) in expected_jsonl.iter().zip(jsonl_after.iter()) {
        assert_eq!(a.id, b.id);
        assert_eq!(a.title, b.title);
        assert_eq!(a.enabled, b.enabled);
        assert_eq!(a.run_count, b.run_count);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn autonomous_background_scheduler_runs_with_zero_ui_clients() {
    let temp_dir = TempDir::new().expect("scheduler runtime temp dir");
    let mock = mock_ps5lm::start_ephemeral()
        .await
        .expect("start mock ps5lm");
    let cfg = build_test_config(&temp_dir, &mock.base_url());

    let store: Arc<dyn StorageBackend> = Arc::new(
        SqliteStorage::open(&cfg.database.path, &cfg.database).expect("open sqlite for runtime"),
    );
    store.run_migrations().expect("migrations");

    let (events_tx, _) = broadcast::channel::<StreamEvent>(128);
    assert_eq!(
        events_tx.receiver_count(),
        0,
        "must start with zero UI/SSE receivers attached"
    );

    let message_bus = Arc::new(MessageBus::new(
        Arc::clone(&store),
        (&cfg.agents).into(),
        events_tx.clone(),
    ));
    let agent_manager = Arc::new(AgentManager::new(
        Arc::clone(&store),
        cfg.agents.clone(),
        events_tx.clone(),
    ));
    agent_manager.seed_defaults().expect("seed default agents");

    let inference = Arc::new(
        Ps5LmProvider::with_concurrency(cfg.inference.clone(), cfg.agents.max_concurrent_inference)
            .expect("create Ps5LmProvider"),
    );
    let runtime = AgentRuntime::new(
        Arc::clone(&store),
        cfg,
        inference,
        Arc::new(NoopToolExecutor),
        None,
        agent_manager,
        message_bus,
        events_tx.clone(),
    );

    // Phase A: Literal zero broadcast receivers, zero HTTP server, zero UI clients.
    let zero_rx_once = runtime
        .create_schedule(make_create_schedule_input(
            "Zero-receiver once schedule",
            "Run autonomous startup ping without UI listeners",
            Some("commander"),
            ScheduleKind::Once,
            Some("2020-01-01T00:00:00Z"),
            None,
        ))
        .expect("create zero_rx_once");
    mock.push_scripted(ScriptedResponse {
        content: "Autonomous zero-receiver once completed.".to_string(),
        ..Default::default()
    });
    assert_eq!(events_tx.receiver_count(), 0);
    let ran_zero_rx = runtime
        .tick_due_schedules(CancellationToken::new())
        .await
        .expect("tick zero_rx_once");
    assert_eq!(ran_zero_rx, 1);
    let zero_rx_after = runtime
        .get_schedule(&zero_rx_once.id)
        .expect("reload zero_rx_once")
        .expect("zero_rx_once present");
    assert!(!zero_rx_after.enabled);
    assert!(zero_rx_after.next_run_at.is_none());
    assert_eq!(zero_rx_after.run_count, 1);
    assert_eq!(zero_rx_after.last_status.as_deref(), Some("completed"));
    let zero_rx_conv = zero_rx_after
        .conversation_id
        .expect("dedicated schedule conversation created");
    let zero_rx_msgs = store
        .list_chat_messages(&zero_rx_conv, true, 50)
        .expect("zero_rx messages");
    assert!(zero_rx_msgs
        .iter()
        .any(|m| m.role == ChatRole::Assistant && m.content.contains("Autonomous zero-receiver")));

    // Phase B: Subscribe internal runtime receiver (still ZERO HTTP server / UI / SSE clients)
    // and execute a due `Once` + due `Interval` schedule together in one tick.
    let mut rx = runtime.events();
    let due_once = runtime
        .create_schedule(make_create_schedule_input(
            "Due once autonomous job",
            "Inspect disk latency once",
            Some("commander"),
            ScheduleKind::Once,
            Some("2020-01-01T00:00:00Z"),
            None,
        ))
        .expect("create due_once");

    let mut due_interval = runtime
        .create_schedule(make_create_schedule_input(
            "Due interval autonomous job",
            "Collect background heartbeat every 60s",
            Some("commander"),
            ScheduleKind::Interval,
            None,
            Some(60),
        ))
        .expect("create due_interval");
    // Make the interval schedule due right now without mutating interval_secs=60
    let mut due_interval_json = serde_json::to_value(&due_interval).expect("serialize interval");
    due_interval_json["next_run_at"] = json!("2020-01-01T00:00:00Z");
    due_interval = serde_json::from_value(due_interval_json).expect("patch due next_run_at");
    let initial_interval_next = due_interval.next_run_at;
    store
        .upsert_schedule(&due_interval)
        .expect("persist due interval");

    mock.push_scripted(ScriptedResponse {
        content: "Autonomous schedule execution reply A.".to_string(),
        ..Default::default()
    });
    mock.push_scripted(ScriptedResponse {
        content: "Autonomous schedule execution reply B.".to_string(),
        ..Default::default()
    });

    let executed = runtime
        .tick_due_schedules(CancellationToken::new())
        .await
        .expect("tick due once + interval");
    assert_eq!(executed, 2);

    let second_tick = runtime
        .tick_due_schedules(CancellationToken::new())
        .await
        .expect("immediate second tick should find nothing due");
    assert_eq!(second_tick, 0);

    let once_after = runtime
        .get_schedule(&due_once.id)
        .expect("get once_after")
        .expect("once_after present");
    assert!(!once_after.enabled, "Once schedule must auto-disable");
    assert!(once_after.next_run_at.is_none());
    assert_eq!(once_after.run_count, 1);
    assert_eq!(once_after.last_status.as_deref(), Some("completed"));
    assert!(once_after.last_error.is_none());
    assert!(once_after.last_task_id.is_some());
    let once_conv_id = once_after
        .conversation_id
        .clone()
        .expect("once schedule conversation_id");
    let once_conv = store
        .get_conversation(&once_conv_id)
        .expect("get once conversation")
        .expect("once conversation persisted");
    assert_eq!(
        once_conv.mode,
        ConversationMode::Direct {
            agent_id: "commander".to_string()
        }
    );
    let once_msgs = store
        .list_chat_messages(&once_conv_id, true, 50)
        .expect("list once conversation messages");
    assert!(once_msgs
        .iter()
        .any(|m| m.role == ChatRole::User && m.content.contains("Inspect disk latency once")));
    assert!(once_msgs.iter().any(|m| {
        m.role == ChatRole::Assistant && m.content.contains("Autonomous schedule execution reply")
    }));

    let interval_after = runtime
        .get_schedule(&due_interval.id)
        .expect("get interval_after")
        .expect("interval_after present");
    assert!(
        interval_after.enabled,
        "Interval schedule must remain enabled after execution"
    );
    assert_eq!(interval_after.run_count, 1);
    assert_eq!(interval_after.last_status.as_deref(), Some("completed"));
    assert!(interval_after.last_run_at.is_some());
    assert!(interval_after.next_run_at.is_some());
    assert!(interval_after.next_run_at > initial_interval_next);
    assert!(interval_after.next_run_at > interval_after.last_run_at);
    let interval_conv_id = interval_after
        .conversation_id
        .clone()
        .expect("interval conversation_id");
    assert_ne!(interval_conv_id, once_conv_id);
    let interval_msgs = store
        .list_chat_messages(&interval_conv_id, true, 50)
        .expect("list interval conversation messages");
    assert!(interval_msgs.iter().any(|m| {
        m.role == ChatRole::User && m.content.contains("Collect background heartbeat every 60s")
    }));
    assert!(interval_msgs.iter().any(|m| {
        m.role == ChatRole::Assistant && m.content.contains("Autonomous schedule execution reply")
    }));

    let mut updated_events: Vec<ScheduleRecord> = Vec::new();
    while let Ok(ev) = rx.try_recv() {
        if let StreamEvent::ScheduleUpdated { schedule } = ev {
            updated_events.push(schedule);
        }
    }
    assert!(
        updated_events
            .iter()
            .any(|s| s.id == due_once.id && !s.enabled && s.run_count == 1),
        "expected ScheduleUpdated event for completed Once schedule"
    );
    assert!(
        updated_events
            .iter()
            .any(|s| s.id == due_interval.id && s.enabled && s.run_count == 1),
        "expected ScheduleUpdated event for completed Interval schedule"
    );

    mock.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn schedule_http_endpoints_and_ai_builder_end_to_end() {
    let h = ScheduleHttpHarness::start().await;

    // 1. POST /api/v1/schedules (far-future Once prevents background loop interference)
    let create_resp = h
        .client
        .post(h.url("/api/v1/schedules"))
        .json(&json!({
            "title": "Health probe",
            "prompt": "Check PS5LM status now",
            "target_agent_id": "commander",
            "kind": "once",
            "run_at": "2099-01-01T00:00:00Z",
            "timezone_offset_mins": 60,
            "enabled": true
        }))
        .send()
        .await
        .expect("POST /api/v1/schedules");
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    let created: ScheduleRecord = create_resp.json().await.expect("decode created schedule");
    assert_eq!(created.title, "Health probe");
    assert_eq!(created.kind, ScheduleKind::Once);
    assert!(created.enabled);
    assert!(created.next_run_at.is_some());

    // 2. GET /api/v1/schedules
    let list_resp = h
        .client
        .get(h.url("/api/v1/schedules"))
        .send()
        .await
        .expect("GET /api/v1/schedules");
    assert_eq!(list_resp.status(), StatusCode::OK);
    let listed: Vec<ScheduleRecord> = list_resp.json().await.expect("decode schedule list");
    assert!(listed.iter().any(|s| s.id == created.id));

    // 3. POST /api/v1/schedules/{id}/toggle (empty body flips true->false; explicit body sets true)
    let toggle_off_resp = h
        .client
        .post(h.url(&format!("/api/v1/schedules/{}/toggle", created.id)))
        .send()
        .await
        .expect("POST toggle off");
    assert_eq!(toggle_off_resp.status(), StatusCode::OK);
    let toggled_off: ScheduleRecord = toggle_off_resp.json().await.expect("decode toggled_off");
    assert!(!toggled_off.enabled);
    assert!(toggled_off.next_run_at.is_none());

    let toggle_on_resp = h
        .client
        .post(h.url(&format!("/api/v1/schedules/{}/toggle", created.id)))
        .json(&json!({ "enabled": true }))
        .send()
        .await
        .expect("POST toggle on");
    assert_eq!(toggle_on_resp.status(), StatusCode::OK);
    let toggled_on: ScheduleRecord = toggle_on_resp.json().await.expect("decode toggled_on");
    assert!(toggled_on.enabled);
    assert!(toggled_on.next_run_at.is_some());

    // 4. POST /api/v1/schedules/{id}/run-now
    h.mock.push_scripted(ScriptedResponse {
        content: "Manual run-now succeeded cleanly.".to_string(),
        ..Default::default()
    });
    let run_now_resp = h
        .client
        .post(h.url(&format!("/api/v1/schedules/{}/run-now", created.id)))
        .send()
        .await
        .expect("POST run-now");
    assert_eq!(run_now_resp.status(), StatusCode::OK);
    let ran: ScheduleRecord = run_now_resp.json().await.expect("decode run-now schedule");
    assert_eq!(ran.run_count, 1);
    assert_eq!(ran.last_status.as_deref(), Some("completed"));
    let conv_id = ran
        .conversation_id
        .clone()
        .expect("run-now should create schedule conversation");

    let conv_detail_resp = h
        .client
        .get(h.url(&format!("/api/v1/conversations/{conv_id}")))
        .send()
        .await
        .expect("GET schedule conversation");
    assert_eq!(conv_detail_resp.status(), StatusCode::OK);
    let conv_detail: Value = conv_detail_resp.json().await.expect("decode conv detail");
    let conv_msgs: Vec<ChatMessage> =
        serde_json::from_value(conv_detail["messages"].clone()).expect("decode conv messages");
    assert!(conv_msgs.iter().any(|m| {
        m.role == ChatRole::Assistant && m.content.contains("Manual run-now succeeded cleanly.")
    }));

    // 5. PUT /api/v1/schedules/{id}
    let put_resp = h
        .client
        .put(h.url(&format!("/api/v1/schedules/{}", created.id)))
        .json(&json!({
            "title": "Hourly probe",
            "prompt": "Check PS5LM hourly",
            "target_agent_id": "commander",
            "kind": "interval",
            "interval_secs": 3600,
            "timezone_offset_mins": 60,
            "enabled": true
        }))
        .send()
        .await
        .expect("PUT /api/v1/schedules/{id}");
    assert_eq!(put_resp.status(), StatusCode::OK);
    let updated: ScheduleRecord = put_resp.json().await.expect("decode updated schedule");
    assert_eq!(updated.title, "Hourly probe");
    assert_eq!(updated.prompt, "Check PS5LM hourly");
    assert_eq!(updated.kind, ScheduleKind::Interval);
    assert_eq!(updated.interval_secs, Some(3600));
    assert!(updated.enabled);
    assert!(updated.next_run_at.is_some());

    // 6a. POST /api/v1/schedules/ai -> LLM structured JSON extraction (auto_create: false)
    h.mock.push_scripted(ScriptedResponse {
        content: r#"{"title":"Morning GPU audit","prompt":"Inspect PS5LM GPU memory and latency every weekday morning","kind":"cron","cron_expr":"0 8 * * 1-5","target_agent_id":"commander","explanation":"Runs weekdays at 08:00"}"#.to_string(),
        ..Default::default()
    });
    let ai_llm_resp = h
        .client
        .post(h.url("/api/v1/schedules/ai"))
        .json(&json!({
            "prompt": "Every weekday at 8:00 AM inspect PS5LM GPU memory and latency",
            "timezone_offset_mins": 60,
            "auto_create": false
        }))
        .send()
        .await
        .expect("POST /api/v1/schedules/ai (llm)");
    assert_eq!(ai_llm_resp.status(), StatusCode::OK);
    let ai_llm: HttpScheduleAiResponse = ai_llm_resp.json().await.expect("decode ai_llm");
    assert_eq!(ai_llm.draft.source, "llm");
    assert_eq!(ai_llm.draft.kind, ScheduleKind::Cron);
    assert_eq!(ai_llm.draft.cron_expr.as_deref(), Some("0 8 * * 1-5"));
    assert_eq!(ai_llm.draft.timezone_offset_mins, 60);
    assert!(ai_llm.created.is_none());

    // 6b. POST /api/v1/schedules/ai -> Polish prompt heuristic fallback + auto_create: true
    h.mock.push_scripted(ScriptedResponse {
        content: "not-json-trigger-heuristic-fallback".to_string(),
        ..Default::default()
    });
    let ai_pl_resp = h
        .client
        .post(h.url("/api/v1/schedules/ai"))
        .json(&json!({
            "prompt": "Co 30 minut sprawdzaj logi i pamięć",
            "timezone_offset_mins": 60,
            "target_agent_id": "commander",
            "auto_create": true
        }))
        .send()
        .await
        .expect("POST /api/v1/schedules/ai (polish fallback)");
    assert_eq!(ai_pl_resp.status(), StatusCode::OK);
    let ai_pl: HttpScheduleAiResponse = ai_pl_resp.json().await.expect("decode ai_pl");
    assert_eq!(ai_pl.draft.source, "heuristic");
    assert_eq!(ai_pl.draft.kind, ScheduleKind::Interval);
    assert_eq!(ai_pl.draft.interval_secs, Some(1800));
    let auto_created = ai_pl
        .created
        .expect("auto_create: true must persist ScheduleRecord");
    assert_eq!(auto_created.kind, ScheduleKind::Interval);
    assert_eq!(auto_created.interval_secs, Some(1800));
    assert!(auto_created.enabled);

    let list_after_ai: Vec<ScheduleRecord> = h
        .client
        .get(h.url("/api/v1/schedules"))
        .send()
        .await
        .expect("GET schedules after auto_create")
        .json()
        .await
        .expect("decode schedules after auto_create");
    assert!(list_after_ai.iter().any(|s| s.id == auto_created.id));

    // 6c. POST /api/v1/schedules/ai -> English heuristic fallback (auto_create: false)
    h.mock.push_scripted(ScriptedResponse {
        content: "non-json-english-fallback".to_string(),
        ..Default::default()
    });
    let ai_en_resp = h
        .client
        .post(h.url("/api/v1/schedules/ai"))
        .json(&json!({
            "prompt": "Every 15 minutes check PS5LM health",
            "timezone_offset_mins": 0,
            "auto_create": false
        }))
        .send()
        .await
        .expect("POST /api/v1/schedules/ai (english fallback)");
    assert_eq!(ai_en_resp.status(), StatusCode::OK);
    let ai_en: HttpScheduleAiResponse = ai_en_resp.json().await.expect("decode ai_en");
    assert_eq!(ai_en.draft.source, "heuristic");
    assert_eq!(ai_en.draft.kind, ScheduleKind::Interval);
    assert_eq!(ai_en.draft.interval_secs, Some(900));
    assert!(ai_en.created.is_none());

    // 7. DELETE /api/v1/schedules/{id}
    for id in [&created.id, &auto_created.id] {
        let del_resp = h
            .client
            .delete(h.url(&format!("/api/v1/schedules/{id}")))
            .send()
            .await
            .expect("DELETE /api/v1/schedules/{id}");
        assert_eq!(del_resp.status(), StatusCode::OK);
        let del_json: Value = del_resp.json().await.expect("decode delete response");
        assert_eq!(del_json["deleted"], Value::Bool(true));
        assert_eq!(del_json["id"], Value::String(id.clone()));

        let get_deleted = h
            .client
            .get(h.url(&format!("/api/v1/schedules/{id}")))
            .send()
            .await
            .expect("GET deleted schedule");
        assert_eq!(get_deleted.status(), StatusCode::NOT_FOUND);
    }

    let remaining: Vec<ScheduleRecord> = h
        .client
        .get(h.url("/api/v1/schedules"))
        .send()
        .await
        .expect("GET schedules after delete")
        .json()
        .await
        .expect("decode empty schedule list");
    assert!(remaining.is_empty());

    h.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chat_agent_can_create_and_list_schedules_via_builtin_tools() {
    let h = ScheduleHttpHarness::start().await;

    let conv_resp = h
        .client
        .post(h.url("/api/v1/conversations"))
        .json(&json!({
            "title": "Chat Schedule Tools",
            "mode": ConversationMode::Direct {
                agent_id: "commander".to_string()
            }
        }))
        .send()
        .await
        .expect("POST /api/v1/conversations");
    assert_eq!(conv_resp.status(), StatusCode::CREATED);
    let conv: Conversation = conv_resp.json().await.expect("decode Conversation");

    // Script 3-step chat turn: schedule_create -> schedule_list -> final confirmation
    h.mock.push_scripted(ScriptedResponse {
        content: String::new(),
        tool_calls: vec![ScriptedToolCall {
            id: "call_sched_create_1".to_string(),
            name: "schedule_create".to_string(),
            arguments: json!({
                "title": "Task co 60 sekund",
                "prompt": "Sprawdź status agentów co 60 sekund",
                "kind": "interval",
                "interval_secs": 60,
                "target_agent_id": "commander"
            }),
        }],
        ..Default::default()
    });
    h.mock.push_scripted(ScriptedResponse {
        content: String::new(),
        tool_calls: vec![ScriptedToolCall {
            id: "call_sched_list_1".to_string(),
            name: "schedule_list".to_string(),
            arguments: json!({}),
        }],
        ..Default::default()
    });
    h.mock.push_scripted(ScriptedResponse {
        content: "Utworzyłem i zweryfikowałem harmonogram wykonywania co 60 sekund.".to_string(),
        ..Default::default()
    });

    let msg_req = SendMessageRequest {
        content: "zrób ten task co 60 sekund".to_string(),
        agent_id: Some("commander".to_string()),
        async_dispatch: true,
        ..Default::default()
    };
    let send_resp = h
        .client
        .post(h.url(&format!("/api/v1/conversations/{}/messages", conv.id)))
        .json(&msg_req)
        .send()
        .await
        .expect("POST conversation message");
    assert_eq!(send_resp.status(), StatusCode::CREATED);
    let send_body: SendMessageResponse = send_resp.json().await.expect("decode SendMessageResponse");
    let task = send_body.task.expect("chat message spawned task");

    let completed = h.wait_task_terminal(&task.id).await;
    assert_eq!(
        completed.status,
        TaskStatus::Completed,
        "chat schedule creation task failed: error={:?} result={:?}",
        completed.error_info,
        completed.result
    );

    // Confirm schedule is persisted and retrievable via HTTP GET /api/v1/schedules
    let schedules: Vec<ScheduleRecord> = h
        .client
        .get(h.url("/api/v1/schedules"))
        .send()
        .await
        .expect("GET /api/v1/schedules")
        .json()
        .await
        .expect("decode schedule list");
    assert_eq!(schedules.len(), 1);
    let mut created = schedules.into_iter().next().expect("one schedule created");
    assert_eq!(created.title, "Task co 60 sekund");
    assert_eq!(created.kind, ScheduleKind::Interval);
    assert_eq!(created.interval_secs, Some(60));
    assert_eq!(created.target_agent_id.as_deref(), Some("commander"));
    assert!(created.enabled);
    assert_eq!(created.run_count, 0);

    // Confirm schedule_list tool output returned the created schedule back to the model
    let recorded = h.mock.recorded_requests();
    let list_tool_seen = recorded.iter().any(|req| {
        req.body
            .get("messages")
            .and_then(|m| m.as_array())
            .is_some_and(|msgs| {
                msgs.iter().any(|m| {
                    m.get("role").and_then(|r| r.as_str()) == Some("tool")
                        && m.get("content")
                            .and_then(|c| c.as_str())
                            .is_some_and(|text| text.contains(&created.id))
                })
            })
    });
    assert!(
        list_tool_seen,
        "expected schedule_list tool result containing schedule id {} in recorded model requests",
        created.id
    );

    // Make the created interval schedule due and verify execution via tick_due_schedules
    h.mock.push_scripted(ScriptedResponse {
        content: "Chat-created interval schedule executed.".to_string(),
        ..Default::default()
    });
    let mut patched = serde_json::to_value(&created).expect("serialize created schedule");
    patched["next_run_at"] = json!("2020-01-01T00:00:00Z");
    created = serde_json::from_value(patched).expect("patch due next_run_at");
    h.system
        .store
        .upsert_schedule(&created)
        .expect("mark schedule due");

    let executed = h
        .system
        .runtime
        .tick_due_schedules(CancellationToken::new())
        .await
        .expect("tick chat-created due schedule");
    assert!(executed <= 1);

    let deadline = Instant::now() + Duration::from_secs(5);
    let after: ScheduleRecord = loop {
        let s: ScheduleRecord = h
            .client
            .get(h.url(&format!("/api/v1/schedules/{}", created.id)))
            .send()
            .await
            .expect("GET schedule after tick")
            .json()
            .await
            .expect("decode schedule after tick");
        if s.run_count >= 1 {
            break s;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for chat-created schedule {} to complete",
            created.id
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    assert_eq!(after.run_count, 1);
    assert_eq!(after.last_status.as_deref(), Some("completed"));
    assert!(after.enabled);
    assert!(after.next_run_at > after.last_run_at);
    let exec_conv_id = after
        .conversation_id
        .expect("schedule execution conversation created");
    let exec_detail: Value = h
        .client
        .get(h.url(&format!("/api/v1/conversations/{exec_conv_id}")))
        .send()
        .await
        .expect("GET schedule execution conversation")
        .json()
        .await
        .expect("decode execution conversation");
    let exec_msgs: Vec<ChatMessage> =
        serde_json::from_value(exec_detail["messages"].clone()).expect("decode exec messages");
    assert!(exec_msgs.iter().any(|m| {
        m.role == ChatRole::Assistant
            && m.content.contains("Chat-created interval schedule executed.")
    }));

    h.shutdown().await;
}
