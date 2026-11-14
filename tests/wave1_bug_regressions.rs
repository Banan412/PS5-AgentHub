//! Wave 1 regression integration suite covering:
//! 1. Bug #3 & #4: Group Auto/Coordinator routing cleanly + vocative direct routing.
//! 2. Bug #5: Conversation-scoped active task rehydration via `GET /api/v1/tasks`.
//! 3. Bug #7: `max_reasoning_iterations` unlimited (`0`) and custom limit `N != 8` progress & cap.
//! 4. Memory eviction tie-breaking and retrieval recency tie-breaking.
//! 5. Split multi-byte UTF-8 SSE frames and prefilled `<think>` / `</think>` tag parsing.
//! 6. Concurrent schedule `run-now` + `PATCH` edit preservation and partial `PUT /api/v1/settings` preservation.

use mock_ps5lm::{MockMode, MockPs5LmHandle, ScriptedResponse, ScriptedToolCall};
use ps5_agent::startup::initialize;
use ps5_agent::AppSystem;
use ps5_agent_config::{AppConfig, DatabaseConfig, InferenceConfig, MemoryConfig};
use ps5_agent_core::{
    now_iso, AgentDefinition, ApiMode, ChatMessage, ChatRole, Conversation, ConversationMode,
    InferenceMessage, InferenceProvider, InferenceRequest, InferenceResponse, InferenceStreamChunk,
    MemoryItem, MemoryPermissions, MemoryScope, ScheduleKind, ScheduleRecord, StorageBackend,
    StreamEvent, TaskRecord, TaskStatus, ThinkingLevel,
};
use ps5_agent_database::SqliteStorage;
use ps5_agent_http::{
    serve_with_listener, HttpCreateSchedule, HttpState, SendMessageResponse, UpdateAgentRequest,
};
use ps5_agent_inference::{parse_model_output, Ps5LmProvider, StreamPiece, ThinkStreamSplitter};
use ps5_agent_memory::{MemoryManager, MemoryQuery};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

struct RegressionHarness {
    _temp_dir: TempDir,
    data_dir: std::path::PathBuf,
    mock: MockPs5LmHandle,
    system: Arc<AppSystem>,
    base_url: String,
    client: reqwest::Client,
    server_task: JoinHandle<()>,
}

impl RegressionHarness {
    async fn start(configure: impl FnOnce(&mut AppConfig)) -> Self {
        let temp_dir = TempDir::new().expect("create temp dir");
        let data_dir = temp_dir.path().to_path_buf();
        let mock = mock_ps5lm::start_ephemeral()
            .await
            .expect("start mock-ps5lm");

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ephemeral HTTP listener");
        let local_addr = listener.local_addr().expect("local addr");
        let base_url = format!("http://{local_addr}");

        let mut cfg = AppConfig::with_data_dir(data_dir.clone());
        cfg.inference.base_url = mock.base_url();
        cfg.inference.stream_enabled = true;
        cfg.inference.auto_sync_from_ps5lm = false;
        cfg.inference.prefill_kv_cache = false;
        cfg.inference.timeout_secs = 30;
        cfg.server.bind_host = "127.0.0.1".to_string();
        cfg.server.port = local_addr.port();
        cfg.security.auth_enabled = false;
        cfg.agents.max_concurrent_inference = 4;
        cfg.logging.enable_klog = false;
        cfg.logging.enable_toast = false;
        configure(&mut cfg);

        cfg.save(&data_dir.join("config.toml"))
            .expect("save initial test config.toml");

        let system = initialize(cfg).await.expect("initialize ps5-agent");
        let state: Arc<HttpState> = system.http_state.clone();
        let bind_addr = local_addr.to_string();
        let server_task = tokio::spawn(async move {
            let _ = serve_with_listener(bind_addr, listener, state).await;
        });

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("build reqwest client");

        // Wait briefly for HTTP listener readiness.
        for _ in 0..40 {
            if let Ok(resp) = client.get(format!("{base_url}/api/v1/health")).send().await {
                if resp.status().is_success() {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        Self {
            _temp_dir: temp_dir,
            data_dir,
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

    async fn create_conversation(&self, title: &str, mode: ConversationMode) -> Conversation {
        let resp = self
            .client
            .post(self.url("/api/v1/conversations"))
            .json(&json!({
                "title": title,
                "mode": mode,
            }))
            .send()
            .await
            .expect("POST /conversations");
        assert!(
            resp.status().is_success(),
            "create_conversation failed with {}",
            resp.status()
        );
        resp.json().await.expect("decode Conversation")
    }

    async fn send_message_raw(&self, conv_id: &str, content: &str) -> reqwest::Response {
        self.client
            .post(self.url(&format!("/api/v1/conversations/{conv_id}/messages")))
            .json(&json!({ "content": content }))
            .send()
            .await
            .expect("POST /conversations/{id}/messages")
    }

    async fn send_message(&self, conv_id: &str, content: &str) -> SendMessageResponse {
        let resp = self.send_message_raw(conv_id, content).await;
        let status = resp.status();
        let text = resp.text().await.expect("read send_message body");
        assert!(
            status.is_success(),
            "send_message failed ({status}): {text}"
        );
        serde_json::from_str(&text).expect("decode SendMessageResponse")
    }

    async fn list_messages(&self, conv_id: &str) -> Vec<ChatMessage> {
        let resp = self
            .client
            .get(self.url(&format!("/api/v1/conversations/{conv_id}/messages")))
            .send()
            .await
            .expect("GET /conversations/{id}/messages");
        assert!(resp.status().is_success(), "list_messages {}", resp.status());
        resp.json().await.expect("decode Vec<ChatMessage>")
    }
}

impl Drop for RegressionHarness {
    fn drop(&mut self) {
        self.system.shutdown.cancel();
        self.server_task.abort();
    }
}

fn spawn_sse_collector(
    client: reqwest::Client,
    events_url: String,
    cancel: CancellationToken,
) -> (Arc<Mutex<Vec<Value>>>, JoinHandle<()>) {
    let collected = Arc::new(Mutex::new(Vec::new()));
    let out = collected.clone();
    let handle = tokio::spawn(async move {
        let Ok(mut resp) = client.get(&events_url).send().await else {
            return;
        };
        let mut buf = String::new();
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                chunk = resp.chunk() => {
                    match chunk {
                        Ok(Some(bytes)) => {
                            buf.push_str(&String::from_utf8_lossy(bytes.as_ref()));
                            while let Some(pos) = buf.find('\n') {
                                let line = buf[..pos].trim_end_matches('\r').to_string();
                                buf.drain(..=pos);
                                if let Some(json_str) = line.strip_prefix("data:") {
                                    let trimmed = json_str.trim();
                                    if !trimmed.is_empty() {
                                        if let Ok(val) = serde_json::from_str::<Value>(trimmed) {
                                            out.lock().await.push(val);
                                        }
                                    }
                                }
                            }
                        }
                        Ok(None) | Err(_) => break,
                    }
                }
            }
        }
    });
    (collected, handle)
}

fn drain_broadcast_events(rx: &mut broadcast::Receiver<StreamEvent>) -> Vec<StreamEvent> {
    let mut events = Vec::new();
    loop {
        match rx.try_recv() {
            Ok(ev) => events.push(ev),
            Err(broadcast::error::TryRecvError::Lagged(_)) => continue,
            Err(broadcast::error::TryRecvError::Empty)
            | Err(broadcast::error::TryRecvError::Closed) => break,
        }
    }
    events
}

fn scripted(content: &str) -> ScriptedResponse {
    ScriptedResponse {
        content: content.to_string(),
        reasoning: None,
        tool_calls: Vec::new(),
        finish_reason: Some("stop".to_string()),
    }
}

fn scripted_tool_call(id: &str, name: &str, arguments: Value) -> ScriptedResponse {
    ScriptedResponse {
        content: String::new(),
        reasoning: None,
        tool_calls: vec![ScriptedToolCall {
            id: id.to_string(),
            name: name.to_string(),
            arguments,
        }],
        finish_reason: Some("tool_calls".to_string()),
    }
}

fn make_task(id: &str, conversation_id: Option<&str>, status: TaskStatus) -> TaskRecord {
    TaskRecord {
        id: id.to_string(),
        conversation_id: conversation_id.map(str::to_string),
        user_request: format!("request for {id}"),
        assigned_agent_id: "commander".to_string(),
        parent_task_id: None,
        priority: 50,
        created_at: now_iso(),
        started_at: (!matches!(status, TaskStatus::Pending)).then(now_iso),
        completed_at: status.is_terminal().then(now_iso),
        status,
        result: matches!(status, TaskStatus::Completed).then(|| "ok".to_string()),
        error_info: matches!(status, TaskStatus::Failed).then(|| "failed".to_string()),
        has_side_effects: false,
        progress_pct: if status.is_terminal() { 100 } else { 25 },
        progress_note: None,
    }
}

fn make_agent_memory(
    id: &str,
    owner: &str,
    importance: u8,
    content: &str,
    ts: &str,
) -> MemoryItem {
    MemoryItem {
        id: id.to_string(),
        scope: MemoryScope::Agent,
        namespace: "default".to_string(),
        owner_agent_id: Some(owner.to_string()),
        conversation_id: None,
        task_id: None,
        key: id.to_string(),
        content: content.to_string(),
        summary: None,
        importance,
        tags: vec!["regression".to_string()],
        archived: false,
        created_at: ts.to_string(),
        updated_at: ts.to_string(),
    }
}

async fn read_http_request_and_verify(stream: &mut TcpStream, expected_path: &str) -> String {
    let mut raw = Vec::new();
    let mut tmp = [0u8; 4096];
    let header_end;
    loop {
        let n = stream.read(&mut tmp).await.expect("read HTTP request");
        assert!(n > 0, "client closed before sending HTTP headers");
        raw.extend_from_slice(&tmp[..n]);
        if let Some(pos) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
            header_end = pos + 4;
            break;
        }
    }

    let headers_str = String::from_utf8_lossy(&raw[..header_end]).to_string();
    let first_line = headers_str.lines().next().unwrap_or_default();
    assert!(
        first_line.contains(expected_path),
        "expected HTTP path '{expected_path}', got '{first_line}'"
    );

    let content_len = headers_str
        .lines()
        .find_map(|line| {
            let (name, val) = line.split_once(':')?;
            if name.trim().eq_ignore_ascii_case("content-length") {
                val.trim().parse::<usize>().ok()
            } else {
                None
            }
        })
        .unwrap_or(0);

    while raw.len() < header_end + content_len {
        let n = stream.read(&mut tmp).await.expect("read HTTP body");
        if n == 0 {
            break;
        }
        raw.extend_from_slice(&tmp[..n]);
    }

    let body_end = (header_end + content_len).min(raw.len());
    String::from_utf8_lossy(&raw[header_end..body_end]).to_string()
}

// =============================================================================
// 1. Bug #3 & #4: Auto/Commander Coordinator Routing & Vocative Direct Routing
// =============================================================================

#[tokio::test]
async fn bug3_4_group_coordinator_routing_and_vocative_direct_routing() {
    let h = RegressionHarness::start(|_| {}).await;

    let sse_cancel = CancellationToken::new();
    let (sse_events, sse_task) = spawn_sse_collector(
        h.client.clone(),
        h.url("/api/v1/events"),
        sse_cancel.clone(),
    );
    tokio::time::sleep(Duration::from_millis(60)).await;
    let mut broadcast_rx = h.system.events_tx.subscribe();

    let group_conv = h
        .create_conversation(
            "Group Bug3 Bug4",
            ConversationMode::Group {
                participant_agent_ids: vec![
                    "commander".to_string(),
                    "researcher".to_string(),
                    "programmer".to_string(),
                    "executor".to_string(),
                ],
            },
        )
        .await;

    // Case 1: Coordinator emits `<think>...</think>` + `{"selected_agents":["researcher"]}`,
    // routing cleanly to Researcher without leaking raw JSON to messages or SSE deltas.
    h.mock.clear_recorded();
    h.mock.clear_scripted();
    h.mock.push_scripted(scripted(
        "<think>Analyzing specialists for this architecture prompt...</think>\n\
         {\"selected_agents\":[\"researcher\"]}",
    ));
    h.mock
        .push_scripted(scripted("Researcher insight: layered modular architecture."));

    h.send_message(&group_conv.id, "Investigate modular architecture trade-offs")
        .await;

    let msgs_after_case1 = h.list_messages(&group_conv.id).await;
    let assistant_msgs_1: Vec<&ChatMessage> = msgs_after_case1
        .iter()
        .filter(|m| m.role == ChatRole::Assistant)
        .collect();
    assert_eq!(
        assistant_msgs_1.len(),
        1,
        "expected exactly one assistant message from selected specialist"
    );
    assert_eq!(assistant_msgs_1[0].agent_id.as_deref(), Some("researcher"));
    assert_eq!(
        assistant_msgs_1[0].content,
        "Researcher insight: layered modular architecture."
    );
    assert!(!assistant_msgs_1[0].content.contains("selected_agents"));

    // Case 2: Coordinator returns plain non-JSON prose fallback -> Commander handles turn cleanly,
    // without leaking coordinator prose/JSON into messages or SSE chat deltas.
    h.mock.clear_recorded();
    h.mock.clear_scripted();
    h.mock.push_scripted(scripted(
        "<think>Thinking without JSON</think>\n\
         Coordinator prose that is not JSON and must never leak.",
    ));
    h.mock
        .push_scripted(scripted("Commander fallback: summarized cleanly."));

    h.send_message(&group_conv.id, "Summarize release readiness across teams")
        .await;

    let msgs_after_case2 = h.list_messages(&group_conv.id).await;
    let assistant_msgs_2: Vec<&ChatMessage> = msgs_after_case2
        .iter()
        .filter(|m| m.role == ChatRole::Assistant)
        .collect();
    assert_eq!(assistant_msgs_2.len(), 2);
    assert_eq!(assistant_msgs_2[1].agent_id.as_deref(), Some("commander"));
    assert_eq!(
        assistant_msgs_2[1].content,
        "Commander fallback: summarized cleanly."
    );

    // Allow SSE stream task a moment to flush frames, then verify zero leaks in wire SSE & broadcast.
    tokio::time::sleep(Duration::from_millis(80)).await;
    let wire_events = sse_events.lock().await.clone();
    let broadcast_events = drain_broadcast_events(&mut broadcast_rx);
    for msg in &msgs_after_case2 {
        assert!(
            !msg.content.contains("selected_agents")
                && !msg.content.contains("Coordinator prose that is not JSON"),
            "raw coordinator output leaked into chat message: {:?}",
            msg.content
        );
    }
    for ev in &wire_events {
        let ev_type = ev.get("type").and_then(Value::as_str).unwrap_or_default();
        if matches!(ev_type, "chat_token" | "chat_completed") {
            let payload = ev
                .get("delta")
                .or_else(|| ev.get("content"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            assert!(
                !payload.contains("selected_agents")
                    && !payload.contains("Coordinator prose that is not JSON"),
                "raw coordinator JSON/prose leaked into SSE event {ev_type}: {payload}"
            );
        }
    }
    for ev in &broadcast_events {
        match ev {
            StreamEvent::ChatToken { delta, .. } => {
                assert!(!delta.contains("selected_agents"));
                assert!(!delta.contains("Coordinator prose that is not JSON"));
            }
            StreamEvent::ChatCompleted { content, .. } => {
                assert!(!content.contains("selected_agents"));
                assert!(!content.contains("Coordinator prose that is not JSON"));
            }
            _ => {}
        }
    }

    // Case 3: Leading vocative `"Executor, hello"` routes directly to `executor`
    // with 1 inference call (no coordinator overhead, no duplicate Commander/Programmer calls).
    h.mock.clear_recorded();
    h.mock.clear_scripted();
    h.mock
        .push_scripted(scripted("Executor online and ready."));
    h.send_message(&group_conv.id, "Executor, hello").await;

    assert_eq!(
        h.mock.recorded_requests().len(),
        1,
        "vocative 'Executor, hello' must trigger a single direct inference call"
    );
    let msgs_after_case3 = h.list_messages(&group_conv.id).await;
    let assistant_msgs_3: Vec<&ChatMessage> = msgs_after_case3
        .iter()
        .filter(|m| m.role == ChatRole::Assistant)
        .collect();
    assert_eq!(assistant_msgs_3.len(), 3);
    assert_eq!(assistant_msgs_3[2].agent_id.as_deref(), Some("executor"));
    assert_eq!(assistant_msgs_3[2].content, "Executor online and ready.");

    // Case 4: Trailing vocative `"dzien dobry commander"` routes directly to `commander`
    // with 1 inference call and no duplicate agent replies.
    h.mock.clear_recorded();
    h.mock.clear_scripted();
    h.mock
        .push_scripted(scripted("Dzien dobry! Commander standing by."));
    h.send_message(&group_conv.id, "dzien dobry commander")
        .await;

    assert_eq!(
        h.mock.recorded_requests().len(),
        1,
        "trailing vocative 'dzien dobry commander' must trigger a single direct inference call"
    );
    let msgs_after_case4 = h.list_messages(&group_conv.id).await;
    let assistant_msgs_4: Vec<&ChatMessage> = msgs_after_case4
        .iter()
        .filter(|m| m.role == ChatRole::Assistant)
        .collect();
    assert_eq!(assistant_msgs_4.len(), 4);
    assert_eq!(assistant_msgs_4[3].agent_id.as_deref(), Some("commander"));
    assert_eq!(
        assistant_msgs_4[3].content,
        "Dzien dobry! Commander standing by."
    );

    sse_cancel.cancel();
    sse_task.abort();
}

// =============================================================================
// 2. Bug #5: Conversation-Scoped Active Task Rehydration
// =============================================================================

#[tokio::test]
async fn bug5_conversation_scoped_active_task_rehydration() {
    let h = RegressionHarness::start(|_| {}).await;

    let conv_a = h
        .create_conversation(
            "Conv A",
            ConversationMode::Direct {
                agent_id: "commander".to_string(),
            },
        )
        .await;
    let conv_b = h
        .create_conversation(
            "Conv B",
            ConversationMode::Direct {
                agent_id: "commander".to_string(),
            },
        )
        .await;

    let seed = [
        // conv_a: 4 active + 3 terminal
        make_task("task-a-pending", Some(&conv_a.id), TaskStatus::Pending),
        make_task("task-a-assigned", Some(&conv_a.id), TaskStatus::Assigned),
        make_task("task-a-running", Some(&conv_a.id), TaskStatus::Running),
        make_task("task-a-waiting", Some(&conv_a.id), TaskStatus::Waiting),
        make_task("task-a-completed", Some(&conv_a.id), TaskStatus::Completed),
        make_task("task-a-failed", Some(&conv_a.id), TaskStatus::Failed),
        make_task("task-a-cancelled", Some(&conv_a.id), TaskStatus::Cancelled),
        // conv_b: 2 active + 1 terminal
        make_task("task-b-pending", Some(&conv_b.id), TaskStatus::Pending),
        make_task("task-b-running", Some(&conv_b.id), TaskStatus::Running),
        make_task("task-b-completed", Some(&conv_b.id), TaskStatus::Completed),
        // global / detached task: active but no conversation_id
        make_task("task-none-running", None, TaskStatus::Running),
    ];
    for t in &seed {
        h.system.store.upsert_task(t).expect("seed task record");
    }

    let resp_a = h
        .client
        .get(h.url(&format!(
            "/api/v1/tasks?status=active&conversation_id={}",
            conv_a.id
        )))
        .send()
        .await
        .expect("GET active tasks for conv_a");
    assert!(resp_a.status().is_success());
    let active_a: Vec<TaskRecord> = resp_a.json().await.expect("decode conv_a tasks");
    let ids_a: BTreeSet<String> = active_a.iter().map(|t| t.id.clone()).collect();
    assert_eq!(
        ids_a,
        BTreeSet::from([
            "task-a-pending".to_string(),
            "task-a-assigned".to_string(),
            "task-a-running".to_string(),
            "task-a-waiting".to_string(),
        ]),
        "conv_a active query must return only conv_a non-terminal tasks"
    );
    assert!(active_a
        .iter()
        .all(|t| t.conversation_id.as_deref() == Some(conv_a.id.as_str()) && !t.status.is_terminal()));

    let resp_b = h
        .client
        .get(h.url(&format!(
            "/api/v1/tasks?status=active&conversation_id={}",
            conv_b.id
        )))
        .send()
        .await
        .expect("GET active tasks for conv_b");
    assert!(resp_b.status().is_success());
    let active_b: Vec<TaskRecord> = resp_b.json().await.expect("decode conv_b tasks");
    let ids_b: BTreeSet<String> = active_b.iter().map(|t| t.id.clone()).collect();
    assert_eq!(
        ids_b,
        BTreeSet::from(["task-b-pending".to_string(), "task-b-running".to_string()]),
        "conv_b active query must return only conv_b non-terminal tasks"
    );

    let resp_all = h
        .client
        .get(h.url("/api/v1/tasks?status=active"))
        .send()
        .await
        .expect("GET all active tasks");
    assert!(resp_all.status().is_success());
    let active_all: Vec<TaskRecord> = resp_all.json().await.expect("decode all active tasks");
    assert_eq!(active_all.len(), 7);
    assert!(active_all.iter().all(|t| !t.status.is_terminal()));
}

// =============================================================================
// 3. Bug #7: Reasoning Step Limit (`0` Unlimited & Custom `N != 8`)
// =============================================================================

#[tokio::test]
async fn bug7_reasoning_step_limit_and_unlimited_progress() {
    let h = RegressionHarness::start(|cfg| {
        cfg.agents.max_reasoning_iterations = 0;
    })
    .await;

    let mut events_rx = h.system.events_tx.subscribe();

    let conv = h
        .create_conversation(
            "Bug7 Step Limits",
            ConversationMode::Direct {
                agent_id: "commander".to_string(),
            },
        )
        .await;

    // Part A: Unlimited (`max_reasoning_iterations = 0`) exceeds the old hardcoded 8-step cap
    // cleanly, emitting `"Tool call step {iter}"` without `/8`.
    h.mock.clear_scripted();
    for step in 1..=9 {
        h.mock.push_scripted(scripted_tool_call(
            &format!("call_unlim_{step}"),
            "calculator",
            json!({ "expression": format!("{step} + 1") }),
        ));
    }
    h.mock
        .push_scripted(scripted("Unlimited steps finished in 9 iterations."));

    let unlim_resp = h
        .send_message(&conv.id, "Execute 9 sequential calculator checks")
        .await;
    let unlim_task_id = unlim_resp.task.expect("task present").id;

    let events_part_a = drain_broadcast_events(&mut events_rx);
    let notes_a: Vec<String> = events_part_a
        .iter()
        .filter_map(|ev| match ev {
            StreamEvent::TaskUpdated {
                task_id,
                progress_note,
                ..
            } if task_id == &unlim_task_id => progress_note.clone(),
            _ => None,
        })
        .collect();

    for step in 1..=9 {
        let expected = format!("Tool call step {step}");
        assert!(
            notes_a.iter().any(|n| n == &expected),
            "expected unlimited progress note '{expected}' in {notes_a:?}"
        );
    }
    assert!(
        notes_a.iter().all(|n| !n.contains('/')),
        "unlimited mode must never emit a hardcoded '/limit' suffix: {notes_a:?}"
    );

    let msgs_a = h.list_messages(&conv.id).await;
    assert!(msgs_a.iter().any(|m| {
        m.role == ChatRole::Assistant && m.content == "Unlimited steps finished in 9 iterations."
    }));

    // Part B: Custom limit `N = 3` (`N != 8`) reflects `/3` in progress metadata and enforces cap.
    let update_req = UpdateAgentRequest {
        max_reasoning_iterations: Some(3),
        ..Default::default()
    };
    let update_resp = h
        .client
        .put(h.url("/api/v1/agents/commander"))
        .json(&update_req)
        .send()
        .await
        .expect("PUT /agents/commander");
    assert!(
        update_resp.status().is_success(),
        "failed updating commander limit: {}",
        update_resp.status()
    );
    let updated_agent: AgentDefinition = update_resp.json().await.expect("decode AgentDefinition");
    assert_eq!(updated_agent.max_reasoning_iterations, 3);

    let _ = drain_broadcast_events(&mut events_rx);
    h.mock.clear_scripted();
    for step in 1..=2 {
        h.mock.push_scripted(scripted_tool_call(
            &format!("call_lim3_ok_{step}"),
            "calculator",
            json!({ "expression": format!("{step} * 2") }),
        ));
    }
    h.mock
        .push_scripted(scripted("Custom limit 3 succeeded in 2 tool steps."));

    let lim3_ok = h
        .send_message(&conv.id, "Execute 2 steps within custom limit 3")
        .await;
    let lim3_ok_task_id = lim3_ok.task.expect("task present").id;

    let events_part_b_ok = drain_broadcast_events(&mut events_rx);
    let notes_b_ok: Vec<String> = events_part_b_ok
        .iter()
        .filter_map(|ev| match ev {
            StreamEvent::TaskUpdated {
                task_id,
                progress_note,
                ..
            } if task_id == &lim3_ok_task_id => progress_note.clone(),
            _ => None,
        })
        .collect();
    assert!(
        notes_b_ok.iter().any(|n| n == "Tool call step 1/3"),
        "expected 'Tool call step 1/3' in {notes_b_ok:?}"
    );
    assert!(
        notes_b_ok.iter().any(|n| n == "Tool call step 2/3"),
        "expected 'Tool call step 2/3' in {notes_b_ok:?}"
    );
    assert!(
        notes_b_ok.iter().all(|n| !n.contains("/8")),
        "custom limit 3 must never mention '/8': {notes_b_ok:?}"
    );

    // Now trigger exceeding `max_reasoning_iterations = 3` (3 consecutive tool calls without answer).
    let _ = drain_broadcast_events(&mut events_rx);
    h.mock.clear_scripted();
    for step in 1..=3 {
        h.mock.push_scripted(scripted_tool_call(
            &format!("call_lim3_over_{step}"),
            "calculator",
            json!({ "expression": format!("{step} + 10") }),
        ));
    }
    let over_resp = h
        .send_message_raw(&conv.id, "Keep calling calculator past limit 3")
        .await;
    let over_status = over_resp.status();
    let over_body = over_resp.text().await.expect("read limit exceeded body");
    assert_eq!(
        over_status,
        reqwest::StatusCode::TOO_MANY_REQUESTS,
        "exceeding custom max_reasoning_iterations=3 should return 429, got {over_status}: {over_body}"
    );
    assert!(
        over_body.contains("Runtime limit exceeded"),
        "expected LimitExceeded HTTP message, got: {over_body}"
    );

    let conv_tasks: Vec<TaskRecord> = h
        .client
        .get(h.url(&format!("/api/v1/tasks?conversation_id={}", conv.id)))
        .send()
        .await
        .expect("GET tasks for conv")
        .json()
        .await
        .expect("decode Vec<TaskRecord>");
    assert!(
        conv_tasks.iter().any(|t| {
            t.status == TaskStatus::Failed
                && t.error_info
                    .as_deref()
                    .is_some_and(|e| e.contains("exceeded max_reasoning_iterations (3)"))
        }),
        "expected Failed task mentioning custom limit (3), tasks: {conv_tasks:?}"
    );
}

// =============================================================================
// 4. Memory Eviction & Recency Tie-Break
// =============================================================================

#[tokio::test]
async fn memory_eviction_and_recency_tie_break() {
    let temp = TempDir::new().expect("temp dir");
    let db_path = temp.path().join("memory_regression.db");
    let storage = Arc::new(
        SqliteStorage::open(&db_path, &DatabaseConfig::default()).expect("open sqlite"),
    );
    storage.run_migrations().expect("run sqlite migrations");

    // Part A: Cap = 3 eviction preserves high-importance and newer records deterministically.
    let capped_mgr = MemoryManager::new(
        storage.clone(),
        MemoryConfig {
            max_memories_per_agent: 3,
            ..MemoryConfig::default()
        },
    );

    capped_mgr
        .store_memory(make_agent_memory(
            "low-old",
            "researcher",
            2,
            "low importance old entry",
            &now_iso(),
        ))
        .expect("store low-old");
    tokio::time::sleep(Duration::from_millis(20)).await;

    capped_mgr
        .store_memory(make_agent_memory(
            "mid-old",
            "researcher",
            5,
            "mid importance older entry",
            &now_iso(),
        ))
        .expect("store mid-old");
    tokio::time::sleep(Duration::from_millis(20)).await;

    capped_mgr
        .store_memory(make_agent_memory(
            "mid-newer",
            "researcher",
            5,
            "mid importance newer entry",
            &now_iso(),
        ))
        .expect("store mid-newer");
    tokio::time::sleep(Duration::from_millis(20)).await;

    capped_mgr
        .store_memory(make_agent_memory(
            "high-keep",
            "researcher",
            9,
            "critical importance entry",
            &now_iso(),
        ))
        .expect("store high-keep");

    let active_after_4 = storage
        .query_memories(Some(MemoryScope::Agent), None, Some("researcher"), false, 32)
        .expect("query active memories after 4 stores");
    let active_ids_4: BTreeSet<String> = active_after_4.iter().map(|m| m.id.clone()).collect();
    assert_eq!(
        active_ids_4,
        BTreeSet::from([
            "mid-old".to_string(),
            "mid-newer".to_string(),
            "high-keep".to_string(),
        ]),
        "lowest importance item 'low-old' must be evicted first"
    );

    tokio::time::sleep(Duration::from_millis(20)).await;
    capped_mgr
        .store_memory(make_agent_memory(
            "mid-newest",
            "researcher",
            5,
            "mid importance newest entry",
            &now_iso(),
        ))
        .expect("store mid-newest");

    let active_after_5 = storage
        .query_memories(Some(MemoryScope::Agent), None, Some("researcher"), false, 32)
        .expect("query active memories after 5 stores");
    let active_ids_5: BTreeSet<String> = active_after_5.iter().map(|m| m.id.clone()).collect();
    assert_eq!(
        active_ids_5,
        BTreeSet::from([
            "mid-newer".to_string(),
            "mid-newest".to_string(),
            "high-keep".to_string(),
        ]),
        "among equal importance (5), oldest 'mid-old' must be evicted while newer ties & high importance survive"
    );

    // Part B: Retrieval recency tie-break in `retrieve_context_scored`.
    let retrieval_mgr = MemoryManager::new(
        storage.clone(),
        MemoryConfig {
            max_memories_per_agent: 20,
            ..MemoryConfig::default()
        },
    );
    let perms = MemoryPermissions::default();

    // B1: Distinct age gap (`2020-...` vs `now_iso()`) breaks identical importance/keywords via recency score.
    storage
        .upsert_memory(&make_agent_memory(
            "ancient-match",
            "analyst",
            7,
            "shader compiler cache heuristic",
            "2020-01-01T00:00:00+00:00",
        ))
        .expect("upsert ancient-match");
    storage
        .upsert_memory(&make_agent_memory(
            "recent-match",
            "analyst",
            7,
            "shader compiler cache heuristic",
            &now_iso(),
        ))
        .expect("upsert recent-match");

    let scored_age = retrieval_mgr
        .retrieve_context_scored(&MemoryQuery {
            reader_agent_id: "analyst",
            permissions: &perms,
            query_text: "shader compiler cache",
            conversation_id: None,
            task_id: None,
            namespace: Some("default"),
            scopes: Some(&[MemoryScope::Agent]),
            limit: Some(5),
        })
        .expect("retrieve scored memories (age tie-break)");
    assert_eq!(scored_age.len(), 2);
    assert_eq!(scored_age[0].item.id, "recent-match");
    assert_eq!(scored_age[1].item.id, "ancient-match");
    assert!(scored_age[0].score > scored_age[1].score);

    // B2: Sub-hour equal score & importance tie-break opposes lexicographical ID (`aaa` vs `zzz`).
    retrieval_mgr
        .store_memory(make_agent_memory(
            "aaa-older",
            "programmer",
            6,
            "vulkan pipeline telemetry",
            &now_iso(),
        ))
        .expect("store aaa-older");
    tokio::time::sleep(Duration::from_millis(30)).await;
    retrieval_mgr
        .store_memory(make_agent_memory(
            "zzz-newer",
            "programmer",
            6,
            "vulkan pipeline telemetry",
            &now_iso(),
        ))
        .expect("store zzz-newer");

    let scored_tie = retrieval_mgr
        .retrieve_context_scored(&MemoryQuery {
            reader_agent_id: "programmer",
            permissions: &perms,
            query_text: "vulkan pipeline telemetry",
            conversation_id: None,
            task_id: None,
            namespace: Some("default"),
            scopes: Some(&[MemoryScope::Agent]),
            limit: Some(5),
        })
        .expect("retrieve scored memories (exact timestamp tie-break)");
    assert_eq!(scored_tie.len(), 2);
    assert_eq!(
        scored_tie[0].item.id, "zzz-newer",
        "recency must break ties ahead of ascending id ('aaa-older' < 'zzz-newer')"
    );
    assert_eq!(scored_tie[1].item.id, "aaa-older");
}

// =============================================================================
// 5. SSE Split UTF-8 & Prefilled `<think>` Parsing
// =============================================================================

async fn spawn_raw_sse_server(
    expected_path: &'static str,
    expected_prompt_suffix: Option<&'static str>,
    chunks: Vec<Vec<u8>>,
) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind raw SSE server");
    let addr = listener.local_addr().expect("raw SSE local_addr");
    let base_url = format!("http://{addr}/v1");

    let handle = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept SSE client");
        let body = read_http_request_and_verify(&mut stream, expected_path).await;
        if let Some(suffix) = expected_prompt_suffix {
            let parsed_req: Value =
                serde_json::from_str(&body).expect("parse outgoing inference JSON");
            let prompt = parsed_req["prompt"]
                .as_str()
                .expect("completions prompt string");
            assert!(
                prompt.ends_with(suffix),
                "expected prompt to end with {suffix:?}, got: {prompt:?}"
            );
        }

        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\n\
                  Content-Type: text/event-stream\r\n\
                  Cache-Control: no-cache\r\n\
                  Connection: close\r\n\r\n",
            )
            .await
            .expect("write SSE headers");
        stream.flush().await.expect("flush SSE headers");

        for chunk in chunks {
            stream.write_all(&chunk).await.expect("write SSE chunk");
            stream.flush().await.expect("flush SSE chunk");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let _ = stream.shutdown().await;
    });

    (base_url, handle)
}

fn split_bytes_across_utf8_boundaries(payload: &[u8], split_points: &[usize]) -> Vec<Vec<u8>> {
    let mut chunks = Vec::new();
    let mut start = 0usize;
    for &pt in split_points {
        assert!(pt > start && pt < payload.len());
        chunks.push(payload[start..pt].to_vec());
        start = pt;
    }
    chunks.push(payload[start..].to_vec());
    chunks
}

async fn collect_stream_chunks(
    provider: &Ps5LmProvider,
    req: InferenceRequest,
) -> (String, String, InferenceResponse) {
    let (tx, mut rx) = mpsc::channel(128);
    let provider_res = provider
        .generate_stream(req, tx, CancellationToken::new())
        .await
        .expect("generate_stream should succeed across split UTF-8 SSE frames");

    let mut streamed_text = String::new();
    let mut streamed_reasoning = String::new();
    let mut completed = None;
    while let Some(chunk) = rx.recv().await {
        match chunk {
            InferenceStreamChunk::TextDelta(t) => streamed_text.push_str(&t),
            InferenceStreamChunk::ReasoningDelta(r) => streamed_reasoning.push_str(&r),
            InferenceStreamChunk::Completed(resp) => completed = Some(resp),
            InferenceStreamChunk::Error(e) => panic!("unexpected InferenceStreamChunk::Error: {e}"),
        }
    }

    (
        streamed_text,
        streamed_reasoning,
        completed.unwrap_or(provider_res),
    )
}

#[tokio::test]
async fn sse_split_utf8_and_prefilled_think_parsing() {
    // Subtest A: ChatCompletions SSE splitting multi-byte UTF-8 sequences (`ż`, `ó`, `ł`, `ć`, `ę`,
    // `ś`, `ą`, `ź`, `ń`, `€`, `🚀`) AND `<think>` / `</think>` tags across chunk writes.
    let chat_payload = concat!(
        "data: {\"id\":\"s1\",\"model\":\"qwen3\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"<th\"}}]}\n\n",
        "data: {\"id\":\"s1\",\"model\":\"qwen3\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ink>Myślę: zażółć\"}}]}\n\n",
        "data: {\"id\":\"s1\",\"model\":\"qwen3\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"</th\"}}]}\n\n",
        "data: {\"id\":\"s1\",\"model\":\"qwen3\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ink>Wynik: gęślą jaźń € 🚀!\"}}]}\n\n",
        "data: {\"id\":\"s1\",\"model\":\"qwen3\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
        "data: [DONE]\n\n"
    );
    let raw_bytes_a = chat_payload.as_bytes();
    // Pick byte offsets directly inside multi-byte UTF-8 codepoints (`ż`, `€`, `🚀`).
    let idx_zh = chat_payload.find('ż').expect("ż offset") + 1;
    let idx_euro_1 = chat_payload.find('€').expect("€ offset") + 1;
    let idx_euro_2 = idx_euro_1 + 1;
    let idx_rocket_1 = chat_payload.find('🚀').expect("🚀 offset") + 1;
    let idx_rocket_2 = idx_rocket_1 + 1;
    let chunks_a = split_bytes_across_utf8_boundaries(
        raw_bytes_a,
        &[idx_zh, idx_euro_1, idx_euro_2, idx_rocket_1, idx_rocket_2],
    );

    let (base_url_a, srv_a) =
        spawn_raw_sse_server("/v1/chat/completions", None, chunks_a).await;
    let cfg_a = InferenceConfig {
        base_url: base_url_a,
        model: "qwen3".to_string(),
        api_mode: ApiMode::ChatCompletions,
        timeout_secs: 15,
        ..InferenceConfig::default()
    };
    let provider_a = Ps5LmProvider::new(cfg_a).expect("provider A");
    let req_a = InferenceRequest {
        model: Some("qwen3".to_string()),
        api_mode: ApiMode::ChatCompletions,
        messages: vec![InferenceMessage {
            role: "user".to_string(),
            content: "Test UTF-8".to_string(),
            ..Default::default()
        }],
        tools: Vec::new(),
        temperature: 0.2,
        max_output_tokens: 256,
        context_limit: 4096,
        stream: true,
        thinking_level: ThinkingLevel::Medium,
    };

    let (text_a, reasoning_a, resp_a) = collect_stream_chunks(&provider_a, req_a).await;
    srv_a.await.expect("raw SSE server A join");

    assert_eq!(reasoning_a, "Myślę: zażółć");
    assert_eq!(text_a, "Wynik: gęślą jaźń € 🚀!");
    assert_eq!(resp_a.reasoning.as_deref(), Some("Myślę: zażółć"));
    assert_eq!(resp_a.content, "Wynik: gęślą jaźń € 🚀!");
    assert!(!text_a.contains('\u{FFFD}') && !reasoning_a.contains('\u{FFFD}'));
    assert!(!text_a.contains("<think>") && !text_a.contains("</think>"));

    // Subtest B: Completions SSE with prefilled `<think>\n` assistant turn + split UTF-8 + `</think>`.
    let comp_payload = concat!(
        "data: {\"id\":\"c1\",\"model\":\"qwen3\",\"choices\":[{\"index\":0,\"text\":\"Krok pierwszy — żółć € </th\"}]}\n\n",
        "data: {\"id\":\"c1\",\"model\":\"qwen3\",\"choices\":[{\"index\":0,\"text\":\"ink>Wynik: 42 🚀\"}]}\n\n",
        "data: {\"id\":\"c1\",\"model\":\"qwen3\",\"choices\":[{\"index\":0,\"text\":\"\",\"finish_reason\":\"stop\"}]}\n\n",
        "data: [DONE]\n\n"
    );
    let c_idx_zh = comp_payload.find('ż').expect("ż offset") + 1;
    let c_idx_euro = comp_payload.find('€').expect("€ offset") + 2;
    let c_idx_rocket = comp_payload.find('🚀').expect("🚀 offset") + 2;
    let chunks_b = split_bytes_across_utf8_boundaries(
        comp_payload.as_bytes(),
        &[c_idx_zh, c_idx_euro, c_idx_rocket],
    );

    let (base_url_b, srv_b) = spawn_raw_sse_server(
        "/v1/completions",
        Some("<|im_start|>assistant\n<think>\n"),
        chunks_b,
    )
    .await;
    let cfg_b = InferenceConfig {
        base_url: base_url_b,
        model: "qwen3".to_string(),
        api_mode: ApiMode::Completions,
        timeout_secs: 15,
        ..InferenceConfig::default()
    };
    let provider_b = Ps5LmProvider::new(cfg_b).expect("provider B");
    let req_b = InferenceRequest {
        model: Some("qwen3".to_string()),
        api_mode: ApiMode::Completions,
        messages: vec![
            InferenceMessage {
                role: "user".to_string(),
                content: "Policz szybko".to_string(),
                ..Default::default()
            },
            InferenceMessage {
                role: "assistant".to_string(),
                content: "<think>\n".to_string(),
                ..Default::default()
            },
        ],
        tools: Vec::new(),
        temperature: 0.2,
        max_output_tokens: 256,
        context_limit: 4096,
        stream: true,
        thinking_level: ThinkingLevel::Medium,
    };

    let (text_b, reasoning_b, resp_b) = collect_stream_chunks(&provider_b, req_b).await;
    srv_b.await.expect("raw SSE server B join");

    assert_eq!(reasoning_b.trim(), "Krok pierwszy — żółć €");
    assert_eq!(text_b, "Wynik: 42 🚀");
    assert_eq!(resp_b.reasoning.as_deref(), Some("Krok pierwszy — żółć €"));
    assert_eq!(resp_b.content, "Wynik: 42 🚀");
    assert!(!text_b.contains('\u{FFFD}') && !reasoning_b.contains('\u{FFFD}'));
    assert!(!text_b.contains("think>"));

    // Subtest C: Direct `ThinkStreamSplitter::with_initial_in_think(true)` & `parse_model_output` parity.
    let mut splitter = ThinkStreamSplitter::with_initial_in_think(true);
    let raw_stream = "Wstępna myśl zażółć</seed:think>Finalna odpowiedź 🚀";
    let mut s_reasoning = String::new();
    let mut s_text = String::new();
    for ch in raw_stream.chars() {
        let mut buf = [0u8; 4];
        let s = ch.encode_utf8(&mut buf);
        for piece in splitter.push(s) {
            match piece {
                StreamPiece::Reasoning(r) => s_reasoning.push_str(&r),
                StreamPiece::Text(t) => s_text.push_str(&t),
            }
        }
    }
    for piece in splitter.finish() {
        match piece {
            StreamPiece::Reasoning(r) => s_reasoning.push_str(&r),
            StreamPiece::Text(t) => s_text.push_str(&t),
        }
    }
    let parsed = parse_model_output(raw_stream, None, Vec::new());
    assert_eq!(s_reasoning, "Wstępna myśl zażółć");
    assert_eq!(s_text, "Finalna odpowiedź 🚀");
    assert_eq!(parsed.reasoning.as_deref(), Some("Wstępna myśl zażółć"));
    assert_eq!(parsed.content, "Finalna odpowiedź 🚀");
}

// =============================================================================
// 6. Concurrent Schedule Edit Preservation & Partial Settings Preservation
// =============================================================================

#[tokio::test]
async fn concurrent_schedule_edit_preservation_and_settings_preservation() {
    let h = RegressionHarness::start(|cfg| {
        cfg.inference.model = "preserved-custom-model".to_string();
        cfg.inference.context_limit = 12288;
        cfg.inference.temperature = 0.75;
        cfg.server.max_body_bytes = 4_194_304;
        cfg.agents.max_agents = 17;
        cfg.agents.max_reasoning_iterations = 11;
        cfg.memory.max_memories_per_agent = 88;
        cfg.tools.default_timeout_secs = 73;
        cfg.logging.level = "debug".to_string();
        cfg.logging.ring_buffer_capacity = 4321;
    })
    .await;

    // Part A: Schedule concurrent `run-now` + `PATCH /api/v1/schedules/{id}` preservation.
    let create_payload = HttpCreateSchedule {
        title: "Original Scheduled Task".to_string(),
        prompt: "Original scheduled prompt".to_string(),
        target_agent_id: Some("commander".to_string()),
        conversation_id: None,
        kind: ScheduleKind::Interval,
        run_at: None,
        interval_secs: Some(3600),
        daily_time: None,
        cron_expr: None,
        timezone_offset_mins: 0,
        enabled: Some(true),
    };
    let created_resp = h
        .client
        .post(h.url("/api/v1/schedules"))
        .json(&create_payload)
        .send()
        .await
        .expect("POST /api/v1/schedules");
    assert!(created_resp.status().is_success());
    let created: ScheduleRecord = created_resp.json().await.expect("decode ScheduleRecord");

    h.mock.set_mode(MockMode::SlowDelay);
    h.mock.set_delay(Duration::from_millis(650));
    h.mock
        .push_scripted(scripted("Slow scheduled execution finished."));

    let run_url = h.url(&format!("/api/v1/schedules/{}/run-now", created.id));
    let run_client = h.client.clone();
    let run_task = tokio::spawn(async move {
        run_client
            .post(run_url)
            .send()
            .await
            .expect("POST run-now")
    });

    // Wait until inference is mid-flight inside SlowDelay before issuing concurrent user PATCH.
    tokio::time::sleep(Duration::from_millis(160)).await;
    let patch_resp = h
        .client
        .patch(h.url(&format!("/api/v1/schedules/{}", created.id)))
        .json(&json!({
            "title": "Concurrent Edited Title",
            "prompt": "Concurrent Edited Prompt",
            "enabled": false,
        }))
        .send()
        .await
        .expect("PATCH /api/v1/schedules/{id}");
    assert!(
        patch_resp.status().is_success(),
        "concurrent PATCH failed: {}",
        patch_resp.status()
    );

    let run_resp = run_task.await.expect("join run-now task");
    assert!(
        run_resp.status().is_success(),
        "concurrent run-now failed: {}",
        run_resp.status()
    );
    h.mock.set_mode(MockMode::Normal);
    h.mock.set_delay(Duration::ZERO);

    let final_sched: ScheduleRecord = h
        .client
        .get(h.url(&format!("/api/v1/schedules/{}", created.id)))
        .send()
        .await
        .expect("GET final schedule")
        .json()
        .await
        .expect("decode final ScheduleRecord");

    assert_eq!(final_sched.title, "Concurrent Edited Title");
    assert_eq!(final_sched.prompt, "Concurrent Edited Prompt");
    assert!(!final_sched.enabled);
    assert_eq!(final_sched.run_count, 1);
    assert_eq!(final_sched.last_status.as_deref(), Some("completed"));
    assert!(final_sched.last_task_id.is_some());
    assert!(final_sched.last_run_at.is_some());

    // Part B: Partial `PUT /api/v1/settings` preserves unedited fields in API + `config.toml`.
    let put_resp = h
        .client
        .put(h.url("/api/v1/settings"))
        .json(&json!({
            "inference": {
                "temperature": 0.25
            },
            "logging": {
                "enable_klog": false
            }
        }))
        .send()
        .await
        .expect("PUT /api/v1/settings");
    assert!(
        put_resp.status().is_success(),
        "PUT /api/v1/settings failed: {}",
        put_resp.status()
    );
    let updated_json: Value = put_resp.json().await.expect("decode PUT settings response");
    let fetched_json: Value = h
        .client
        .get(h.url("/api/v1/settings"))
        .send()
        .await
        .expect("GET /api/v1/settings")
        .json()
        .await
        .expect("decode GET settings response");

    for snapshot in [&updated_json, &fetched_json] {
        let temp = snapshot["inference"]["temperature"]
            .as_f64()
            .expect("temperature f64");
        assert!(
            (temp - 0.25).abs() < 1e-4,
            "expected updated temperature 0.25, got {temp}"
        );
        assert_eq!(
            snapshot["inference"]["model"].as_str(),
            Some("preserved-custom-model")
        );
        assert_eq!(snapshot["inference"]["context_limit"].as_u64(), Some(12288));
        assert_eq!(
            snapshot["server"]["max_body_bytes"].as_u64(),
            Some(4_194_304)
        );
        assert_eq!(snapshot["agents"]["max_agents"].as_u64(), Some(17));
        assert_eq!(
            snapshot["agents"]["max_reasoning_iterations"].as_u64(),
            Some(11)
        );
        assert_eq!(
            snapshot["memory"]["max_memories_per_agent"].as_u64(),
            Some(88)
        );
        assert_eq!(
            snapshot["tools"]["default_timeout_secs"].as_u64(),
            Some(73)
        );
        assert_eq!(snapshot["logging"]["level"].as_str(), Some("debug"));
        assert_eq!(
            snapshot["logging"]["ring_buffer_capacity"].as_u64(),
            Some(4321)
        );
    }

    let persisted = AppConfig::load_or_create(&h.data_dir.join("config.toml"))
        .expect("reload saved config.toml");
    assert!((persisted.inference.temperature - 0.25).abs() < 1e-4);
    assert_eq!(persisted.inference.model, "preserved-custom-model");
    assert_eq!(persisted.inference.context_limit, 12288);
    assert_eq!(persisted.server.max_body_bytes, 4_194_304);
    assert_eq!(persisted.agents.max_agents, 17);
    assert_eq!(persisted.agents.max_reasoning_iterations, 11);
    assert_eq!(persisted.memory.max_memories_per_agent, 88);
    assert_eq!(persisted.tools.default_timeout_secs, 73);
    assert_eq!(persisted.logging.level, "debug");
    assert_eq!(persisted.logging.ring_buffer_capacity, 4321);
}
