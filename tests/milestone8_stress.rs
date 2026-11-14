use mock_ps5lm::{start_ephemeral, MockMode, ScriptedResponse, ScriptedToolCall};
use ps5_agent::startup::initialize;
use ps5_agent_config::AppConfig;
use ps5_agent_core::{
    new_id, now_iso, AgentMessageType, ApiMode, ChatMessage, ChatRole, Conversation,
    ConversationMode, InferenceMessage, InferenceProvider, InferenceRequest, MemoryScope,
    SystemOneQuestion, SystemOneRequest, TaskRecord, TaskStatus, ThinkingLevel,
};
use ps5_agent_http::HttpBackend;
use std::collections::BTreeMap;
use std::path::Path;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

fn build_test_config(dir: &Path, base_url: String) -> AppConfig {
    let mut cfg = AppConfig::default();
    cfg.data_dir = dir.to_path_buf();
    cfg.database.driver = "sqlite".to_string();
    cfg.database.path = dir.join("database").join("agent.db");
    cfg.logging.log_dir = dir.join("logs");
    cfg.logging.enable_klog = false;
    cfg.logging.enable_toast = false;
    cfg.tools.allowed_paths = vec![dir.join("workspace").to_string_lossy().into_owned()];
    cfg.inference.base_url = base_url;
    cfg.runtime.health_poll_interval_secs = 60;
    cfg
}

async fn wait_for_task_status<F>(
    system: &ps5_agent::adapters::AppSystem,
    task_id: &str,
    timeout: Duration,
    mut predicate: F,
) -> TaskRecord
where
    F: FnMut(&TaskRecord) -> bool,
{
    let deadline = Instant::now() + timeout;
    loop {
        let task = system
            .store
            .get_task(task_id)
            .expect("get_task failed")
            .expect("task missing");
        if predicate(&task) {
            return task;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for task '{}' (last status: {:?}, error: {:?})",
            task_id,
            task.status,
            task.error_info
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stress_20_concurrent_tasks_bounded_to_single_inference() {
    let temp = TempDir::new().unwrap();
    let mock = start_ephemeral().await.unwrap();
    mock.set_delay(Duration::from_millis(25));
    mock.set_mode(MockMode::SlowDelay);

    let mut cfg = build_test_config(temp.path(), mock.base_url());
    cfg.agents.max_concurrent_inference = 1;
    cfg.agents.max_active_tasks = 20;
    cfg.agents.max_queued_tasks = 32;

    let system = initialize(cfg).await.unwrap();
    assert_eq!(system.store.driver_name(), "sqlite");
    assert_eq!(system.inference.max_concurrent(), 1);

    let agents = ["commander", "researcher", "programmer", "analyst", "executor"];
    let t0 = Instant::now();

    let mut task_ids = Vec::with_capacity(20);
    for i in 0..20 {
        let agent_id = agents[i % agents.len()];
        let task = system
            .runtime
            .submit_task(
                format!("Stress task {i}: summarize subsystem state"),
                agent_id.to_string(),
                None,
                Some(1),
            )
            .await
            .unwrap();
        task_ids.push(task.id);
    }

    for tid in &task_ids {
        let finished = wait_for_task_status(&system, tid, Duration::from_secs(30), |t| {
            t.status.is_terminal()
        })
        .await;
        assert_eq!(
            finished.status,
            TaskStatus::Completed,
            "task '{}' failed: {:?}",
            tid,
            finished.error_info
        );
    }

    let elapsed = t0.elapsed();
    assert!(
        elapsed >= Duration::from_millis(450),
        "expected >= 450ms for 20 x 25ms serialized inference calls, got {:?}",
        elapsed
    );
    let metrics = system.inference.metrics();
    assert_eq!(metrics.errors_total, 0);
    assert!(
        metrics.requests_total >= 20,
        "expected >= 20 inference requests, got {}",
        metrics.requests_total
    );

    system.shutdown.cancel();
    mock.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stress_120_message_conversation_compaction_and_rolling_summary() {
    let temp = TempDir::new().unwrap();
    let mock = start_ephemeral().await.unwrap();

    let mut cfg = build_test_config(temp.path(), mock.base_url());
    cfg.memory.archive_after_messages = 40;
    cfg.memory.keep_recent_messages = 16;

    let system = initialize(cfg).await.unwrap();

    let conv_id = new_id();
    let now = now_iso();
    system
        .store
        .create_conversation(&Conversation {
            id: conv_id.clone(),
            title: "Compaction Stress".to_string(),
            mode: ConversationMode::Direct {
                agent_id: "commander".to_string(),
            },
            created_at: now.clone(),
            updated_at: now,
            archived: false,
            sort_order: 0,
        })
        .unwrap();

    for i in 0..120 {
        let (role, agent_id) = if i % 2 == 0 {
            (ChatRole::User, None)
        } else {
            (ChatRole::Assistant, Some("commander".to_string()))
        };
        system
            .store
            .append_chat_message(&ChatMessage {
                id: new_id(),
                conversation_id: conv_id.clone(),
                role,
                agent_id,
                content: format!(
                    "Message turn {i:03}: analyzing PS5 firmware 13.40 agent memory compaction and telemetry metrics."
                ),
                reasoning: None,
                tool_calls_json: None,
                task_id: None,
                token_count: 24,
                latency_ms: Some(12),
                archived: false,
                attachments: Vec::new(),
                timings: None,
                created_at: format!("2026-11-13T12:{:02}:{:02}Z", i / 60, i % 60),
            })
            .unwrap();
    }

    let ctx = system
        .memory
        .build_conversation_context(&conv_id, "Commander system prompt", None, 512)
        .unwrap();

    assert_eq!(ctx.archived_count, 104);
    assert!(
        ctx.rolling_summary
            .as_deref()
            .is_some_and(|s| !s.trim().is_empty()),
        "expected non-empty rolling summary"
    );
    assert!(
        ctx.estimated_tokens <= 512,
        "estimated_tokens {} exceeded budget 512",
        ctx.estimated_tokens
    );

    let active_after = system
        .store
        .list_chat_messages(&conv_id, false, 200)
        .unwrap();
    assert_eq!(active_after.len(), 16);

    let all_after = system
        .store
        .list_chat_messages(&conv_id, true, 200)
        .unwrap();
    assert_eq!(all_after.len(), 120);

    let summaries = system
        .store
        .query_memories(
            Some(MemoryScope::Conversation),
            Some("conversation_summary"),
            None,
            false,
            10,
        )
        .unwrap();
    let expected_key = format!("rolling_summary:{conv_id}");
    let expected_id = format!("rolling-summary-{conv_id}");
    assert!(
        summaries
            .iter()
            .any(|m| m.id == expected_id && m.key == expected_key),
        "rolling summary item missing from storage"
    );

    system.shutdown.cancel();
    mock.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chaos_five_agent_workflow_and_all_ps5lm_modes_under_503_recovery() {
    let temp = TempDir::new().unwrap();
    let mock = start_ephemeral().await.unwrap();
    let cfg = build_test_config(temp.path(), mock.base_url());
    let system = initialize(cfg).await.unwrap();

    // Phase A: PS5LM 503 outage
    mock.set_mode(MockMode::Disconnect503);
    let outage_task = system
        .runtime
        .submit_task(
            "Check PS5LM status during outage".to_string(),
            "researcher".to_string(),
            None,
            None,
        )
        .await
        .unwrap();
    let failed = wait_for_task_status(&system, &outage_task.id, Duration::from_secs(15), |t| {
        t.status.is_terminal()
    })
    .await;
    assert_eq!(failed.status, TaskStatus::Failed);

    // Phase B: Recovery + Multi-agent orchestration
    mock.set_mode(MockMode::MultiAgentOrchestration);
    let convs = system.store.list_conversations(false).unwrap();
    assert!(!convs.is_empty(), "expected seeded Commander Console conversation");
    let turn = system
        .runtime
        .chat_commander(
            &convs[0].id,
            "Coordinate team architecture review",
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(turn.status, TaskStatus::Completed);
    assert!(!turn.content.trim().is_empty());

    let a2a = system.store.list_agent_messages(None, None, 50).unwrap();
    assert!(
        a2a.len() >= 2,
        "expected >= 2 A2A envelopes recorded (assignment + completion report), got {}",
        a2a.len()
    );
    assert!(
        a2a.iter().any(|m| {
            m.sender_agent_id == "commander"
                && m.recipient_agent_id == "researcher"
                && m.message_type == AgentMessageType::TaskAssignment
        }),
        "expected TaskAssignment A2A envelope from commander to researcher"
    );
    assert!(
        a2a.iter().any(|m| {
            m.sender_agent_id == "researcher"
                && m.recipient_agent_id == "commander"
                && m.message_type == AgentMessageType::TaskCompleted
        }),
        "expected TaskCompleted A2A envelope from researcher to commander"
    );

    // Phase C: Remaining PS5LM endpoints (/v1/models, /v1/completions, /v1/systemone)
    mock.set_mode(MockMode::Normal);
    let models = system.http_backend.list_inference_models().await.unwrap();
    assert!(!models.is_empty());

    let comp_resp = system
        .inference
        .generate(
            InferenceRequest {
                model: None,
                api_mode: ApiMode::Completions,
                messages: vec![InferenceMessage {
                    role: "user".to_string(),
                    content: "Ping completions endpoint".to_string(),
                    tool_call_id: None,
                    name: None,
                    tool_calls: None,
                    image_urls: Vec::new(),
                }],
                tools: Vec::new(),
                temperature: 0.2,
                max_output_tokens: 64,
                context_limit: 2048,
                stream: false,
                thinking_level: ThinkingLevel::default(),
                slot_id: None,
            },
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert!(!comp_resp.content.trim().is_empty());

    let mut questions = BTreeMap::new();
    questions.insert(
        "route".to_string(),
        SystemOneQuestion {
            question_type: "choice".to_string(),
            instructions: "Pick best specialist agent".to_string(),
            criteria: Some(serde_json::json!(["researcher", "programmer"])),
        },
    );
    let s1 = system
        .http_backend
        .evaluate_systemone(SystemOneRequest {
            model: None,
            state: "Need Rust code fix in SQLite layer".to_string(),
            questions,
        })
        .await
        .unwrap();
    assert!(s1.answers.get("route").is_some());

    let recorded = mock.recorded_requests();
    assert!(
        recorded.iter().any(|r| r.path == "/v1/completions"),
        "expected recorded POST /v1/completions request"
    );
    assert!(
        recorded.iter().any(|r| r.path == "/v1/systemone"),
        "expected recorded POST /v1/systemone request"
    );

    system.shutdown.cancel();
    mock.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_recovery_pauses_side_effecting_and_resumes_readonly() {
    let temp = TempDir::new().unwrap();
    let mock = start_ephemeral().await.unwrap();
    let cfg = build_test_config(temp.path(), mock.base_url());

    let task_mutating_id = new_id();
    let task_readonly_id = new_id();
    {
        let system1 = initialize(cfg.clone()).await.unwrap();
        let now = now_iso();

        let task_mutating = TaskRecord {
            id: task_mutating_id.clone(),
            conversation_id: None,
            user_request: "Write config update to workspace".to_string(),
            assigned_agent_id: "executor".to_string(),
            parent_task_id: None,
            priority: 1,
            created_at: now.clone(),
            started_at: Some(now.clone()),
            completed_at: None,
            status: TaskStatus::Running,
            result: None,
            error_info: None,
            has_side_effects: true,
            progress_pct: 50,
            progress_note: Some("Executing side-effecting step".to_string()),
        };

        let task_readonly = TaskRecord {
            id: task_readonly_id.clone(),
            conversation_id: None,
            user_request: "Summarize PS5 SDK compatibility notes".to_string(),
            assigned_agent_id: "researcher".to_string(),
            parent_task_id: None,
            priority: 1,
            created_at: now.clone(),
            started_at: Some(now),
            completed_at: None,
            status: TaskStatus::Running,
            result: None,
            error_info: None,
            has_side_effects: false,
            progress_pct: 25,
            progress_note: Some("Gathering research notes".to_string()),
        };

        system1.store.upsert_task(&task_mutating).unwrap();
        system1.store.upsert_task(&task_readonly).unwrap();

        // Simulate ungraceful crash/power-loss without running finalize_shutdown()
        system1.shutdown.cancel();
        system1.store.checkpoint().unwrap();
        drop(system1);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let system2 = initialize(cfg).await.unwrap();
    assert!(
        system2
            .recovery_report
            .paused_side_effect_tasks
            .iter()
            .any(|t| t.id == task_mutating_id),
        "expected side-effecting task in paused_side_effect_tasks"
    );
    assert_eq!(
        system2
            .store
            .get_task(&task_mutating_id)
            .unwrap()
            .unwrap()
            .status,
        TaskStatus::Waiting
    );

    assert!(
        system2
            .recovery_report
            .resumed_tasks
            .iter()
            .any(|t| t.id == task_readonly_id),
        "expected read-only task in resumed_tasks"
    );
    let resumed_done = wait_for_task_status(&system2, &task_readonly_id, Duration::from_secs(15), |t| {
        t.status.is_terminal()
    })
    .await;
    assert_eq!(resumed_done.status, TaskStatus::Completed);
    assert_eq!(
        system2
            .store
            .get_task(&task_mutating_id)
            .unwrap()
            .unwrap()
            .status,
        TaskStatus::Waiting,
        "side-effecting task must remain paused in Waiting"
    );

    system2.shutdown.cancel();
    mock.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tool_approval_gate_pause_and_approve_to_completion() {
    let temp = TempDir::new().unwrap();
    let mock = start_ephemeral().await.unwrap();
    let mut cfg = build_test_config(temp.path(), mock.base_url());
    cfg.security.require_approval_for_mutating = true;
    let system = initialize(cfg).await.unwrap();

    let target_file = temp.path().join("workspace").join("target_note.txt");
    std::fs::write(&target_file, "sensitive note").unwrap();
    assert!(target_file.exists());

    mock.push_scripted(ScriptedResponse {
        content: String::new(),
        reasoning: Some("Need operator approval before deleting target_note.txt".to_string()),
        tool_calls: vec![ScriptedToolCall {
            id: "call_del_1".to_string(),
            name: "delete_file".to_string(),
            arguments: serde_json::json!({ "path": "target_note.txt" }),
        }],
        finish_reason: Some("tool_calls".to_string()),
    });
    mock.push_scripted(ScriptedResponse {
        content: "Deleted target_note.txt after operator approval.".to_string(),
        reasoning: None,
        tool_calls: Vec::new(),
        finish_reason: Some("stop".to_string()),
    });

    let submitted = system
        .runtime
        .submit_task(
            "Delete target_note.txt from the workspace".to_string(),
            "executor".to_string(),
            None,
            None,
        )
        .await
        .unwrap();

    let waiting = wait_for_task_status(&system, &submitted.id, Duration::from_secs(15), |t| {
        t.status == TaskStatus::Waiting
    })
    .await;
    assert_eq!(waiting.status, TaskStatus::Waiting);
    assert!(
        target_file.exists(),
        "target_note.txt must NOT be deleted before approval"
    );

    let pending = system.store.list_approvals(true, 10).unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].tool_id.as_deref(), Some("delete_file"));

    let outcome = system
        .runtime
        .resolve_approval(&pending[0].id, true, "admin")
        .await
        .unwrap();
    assert!(outcome.approved);

    let completed = wait_for_task_status(&system, &submitted.id, Duration::from_secs(15), |t| {
        t.status.is_terminal()
    })
    .await;
    assert_eq!(completed.status, TaskStatus::Completed);
    assert!(
        !target_file.exists(),
        "target_note.txt should have been deleted after operator approval"
    );
    assert!(
        system.store.list_approvals(true, 10).unwrap().is_empty(),
        "no pending approvals should remain after resolution"
    );

    system.shutdown.cancel();
    mock.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn post_stress_sqlite_integrity_and_wal_checkpoint() {
    let temp = TempDir::new().unwrap();
    let mock = start_ephemeral().await.unwrap();
    let cfg = build_test_config(temp.path(), mock.base_url());
    let system = initialize(cfg).await.unwrap();

    let agents = ["commander", "researcher", "programmer", "analyst", "executor"];
    let mut ids = Vec::new();
    for (idx, agent_id) in agents.iter().enumerate() {
        let task = system
            .runtime
            .submit_task(
                format!("Integrity smoke check {idx}"),
                (*agent_id).to_string(),
                None,
                Some(idx as i32),
            )
            .await
            .unwrap();
        ids.push(task.id);
    }

    for tid in &ids {
        let done = wait_for_task_status(&system, tid, Duration::from_secs(15), |t| {
            t.status.is_terminal()
        })
        .await;
        assert_eq!(done.status, TaskStatus::Completed);
    }

    assert_eq!(system.store.integrity_check().unwrap(), "ok");
    system.store.checkpoint().unwrap();

    system.shutdown.cancel();
    mock.shutdown();
}
