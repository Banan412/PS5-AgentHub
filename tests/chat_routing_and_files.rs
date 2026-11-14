use mock_ps5lm::{
    start_ephemeral, MockMode, MockPs5LmHandle, ScriptedResponse, ScriptedToolCall,
};
use ps5_agent::adapters::AppSystem;
use ps5_agent::shutdown::finalize_shutdown;
use ps5_agent::startup::initialize;
use ps5_agent_config::AppConfig;
use ps5_agent_core::{
    ChatMessage, ChatRole, Conversation, ConversationMode, StreamEvent, TaskRecord, TaskStatus,
    ThinkingLevel,
};
use ps5_agent_http::security::PREVIEW_CSP_VALUE;
use ps5_agent_http::server::bind_reusable_listener;
use ps5_agent_http::{
    serve_with_listener, SendMessageRequest, SendMessageResponse, WorkspaceFileEntry,
    WorkspaceFilesResponse,
};
use reqwest::{Client, StatusCode};
use serde_json::{json, Value};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

fn build_test_config(data_dir: &Path, inference_base_url: String) -> AppConfig {
    let mut cfg = AppConfig::with_data_dir(data_dir.to_path_buf());
    cfg.database.driver = "sqlite".to_string();
    cfg.logging.enable_klog = false;
    cfg.logging.enable_toast = false;
    cfg.inference.base_url = inference_base_url;
    cfg.inference.auto_sync_from_ps5lm = false;
    cfg.runtime.health_poll_interval_secs = 60;
    cfg.security.auth_enabled = false;
    cfg.security.require_approval_for_mutating = false;
    cfg.agents.max_concurrent_inference = 4;
    cfg.agents.max_active_tasks = 16;
    cfg
}

struct TestHarness {
    temp: TempDir,
    mock: MockPs5LmHandle,
    system: Arc<AppSystem>,
    base_url: String,
    client: Client,
    server_handle: Option<JoinHandle<std::io::Result<()>>>,
}

impl TestHarness {
    async fn start() -> Self {
        Self::start_with_options(false, "medium", None).await
    }

    async fn start_with_mutating_approval(require_approval: bool) -> Self {
        Self::start_with_options(require_approval, "medium", None).await
    }

    async fn start_with_thinking_off() -> Self {
        Self::start_with_options(false, "off", None).await
    }

    async fn start_with_validating_proxy() -> (Self, ValidatingProxy) {
        let mock = start_ephemeral().await.unwrap();
        let proxy = ValidatingProxy::start(mock.base_url()).await;
        let proxy_url = proxy.base_url.clone();
        let harness = Self::start_with_options(false, "medium", Some((mock, proxy_url))).await;
        (harness, proxy)
    }

    async fn start_with_options(
        require_approval: bool,
        thinking_level: &str,
        custom_mock: Option<(MockPs5LmHandle, String)>,
    ) -> Self {
        let temp = TempDir::new().unwrap();
        let (mock, inference_url) = match custom_mock {
            Some((m, url)) => (m, url),
            None => {
                let m = start_ephemeral().await.unwrap();
                let url = m.base_url();
                (m, url)
            }
        };
        let mut cfg = build_test_config(temp.path(), inference_url);
        cfg.security.require_approval_for_mutating = require_approval;
        cfg.inference.thinking_level = thinking_level.to_string();
        let system = initialize(cfg).await.unwrap();

        let listener = bind_reusable_listener("127.0.0.1:0").await.unwrap();
        let local_addr = listener.local_addr().unwrap();
        let bind_addr = local_addr.to_string();
        let base_url = format!("http://{local_addr}");

        let state = system.http_state.clone();
        let server_handle =
            tokio::spawn(async move { serve_with_listener(bind_addr, listener, state).await });

        let client = Client::builder()
            .pool_max_idle_per_host(0)
            .build()
            .unwrap();

        let health_resp = client
            .get(format!("{base_url}/api/v1/health"))
            .send()
            .await
            .unwrap();
        assert_eq!(health_resp.status(), StatusCode::OK);
        mock.clear_recorded();

        Self {
            temp,
            mock,
            system,
            base_url,
            client,
            server_handle: Some(server_handle),
        }
    }

    fn workspace_dir(&self) -> PathBuf {
        self.temp.path().join("workspace")
    }

    async fn create_group_conversation(&self, title: &str, participants: &[&str]) -> Conversation {
        let mode = ConversationMode::Group {
            participant_agent_ids: participants.iter().map(|s| (*s).to_string()).collect(),
        };
        let resp = self
            .client
            .post(format!("{}/api/v1/conversations", self.base_url))
            .json(&json!({
                "title": title,
                "mode": mode,
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
        resp.json::<Conversation>().await.unwrap()
    }

    async fn send_message(
        &self,
        conversation_id: &str,
        req: &SendMessageRequest,
    ) -> (StatusCode, SendMessageResponse) {
        let resp = self
            .client
            .post(format!(
                "{}/api/v1/conversations/{conversation_id}/messages",
                self.base_url
            ))
            .json(req)
            .send()
            .await
            .unwrap();
        let status = resp.status();
        let body = resp.json::<SendMessageResponse>().await.unwrap();
        (status, body)
    }

    async fn list_messages(&self, conversation_id: &str) -> Vec<ChatMessage> {
        let resp = self
            .client
            .get(format!(
                "{}/api/v1/conversations/{conversation_id}/messages",
                self.base_url
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        resp.json::<Vec<ChatMessage>>().await.unwrap()
    }

    async fn list_conversations(&self) -> Vec<Conversation> {
        let resp = self
            .client
            .get(format!("{}/api/v1/conversations", self.base_url))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        resp.json::<Vec<Conversation>>().await.unwrap()
    }

    fn push_scripted_text(&self, content: &str) {
        self.mock.push_scripted(ScriptedResponse {
            content: content.to_string(),
            reasoning: None,
            tool_calls: Vec::new(),
            finish_reason: Some("stop".to_string()),
        });
    }

    async fn get_task(&self, task_id: &str) -> TaskRecord {
        let resp = self
            .client
            .get(format!("{}/api/v1/tasks/{task_id}", self.base_url))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        resp.json::<TaskRecord>().await.unwrap()
    }

    async fn wait_task_terminal(&self, task_id: &str, timeout: Duration) -> TaskRecord {
        let deadline = Instant::now() + timeout;
        loop {
            let task = self.get_task(task_id).await;
            if task.status.is_terminal() {
                return task;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for task '{task_id}' (last status={:?}, error={:?})",
                task.status,
                task.error_info
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn shutdown(mut self) {
        self.system.shutdown.cancel();
        if let Some(handle) = self.server_handle.take() {
            let _ = handle.await;
        }
        finalize_shutdown(&self.system);
        self.mock.shutdown();
    }
}

fn system_prompt_of(req_body: &Value) -> &str {
    req_body["messages"][0]["content"]
        .as_str()
        .expect("system prompt content must be a string")
}

fn last_user_prompt_of(req_body: &Value) -> String {
    let msgs = req_body["messages"]
        .as_array()
        .expect("messages must be an array");
    for msg in msgs.iter().rev() {
        if msg["role"].as_str() == Some("user") {
            if let Some(s) = msg["content"].as_str() {
                return s.to_string();
            }
        }
    }
    panic!("no user message found in recorded request: {req_body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slash_agent_and_at_mention_route_directly_to_programmer_without_commander() {
    let h = TestHarness::start().await;
    let conv = h
        .create_group_conversation("Direct Routing Test", &["researcher", "programmer", "analyst"])
        .await;

    // 1. `/agent programmer <prompt>` routes directly to Programmer ONLY.
    h.mock.push_scripted(ScriptedResponse {
        content: "I checked the workspace and everything is ready.".to_string(),
        reasoning: None,
        tool_calls: Vec::new(),
        finish_reason: Some("stop".to_string()),
    });

    let (status1, resp1) = h
        .send_message(
            &conv.id,
            &SendMessageRequest {
                content: "/agent programmer Check workspace status".to_string(),
                ..Default::default()
            },
        )
        .await;
    assert_eq!(status1, StatusCode::CREATED);
    assert_eq!(
        resp1.user_message.content,
        "/agent programmer Check workspace status"
    );
    let task1 = resp1.task.expect("task must be returned");
    assert_eq!(task1.assigned_agent_id, "programmer");
    assert_eq!(task1.status, TaskStatus::Completed);

    let recorded1 = h.mock.recorded_requests();
    assert_eq!(
        recorded1.len(),
        1,
        "/agent programmer must execute a single Programmer turn without invoking Commander"
    );
    let sys1 = system_prompt_of(&recorded1[0].body);
    assert!(sys1.contains("Agent Identity: Programmer (id: programmer"));
    assert!(!sys1.contains("Agent Identity: Commander (id: commander"));
    let user1 = last_user_prompt_of(&recorded1[0].body);
    assert!(user1.contains("The user asks you directly: Check workspace status"));

    // 2. `@programmer <prompt>` routes directly to Programmer ONLY; Commander is NOT called.
    h.mock.clear_recorded();
    h.mock.push_scripted(ScriptedResponse {
        content: "I found zero pending tasks in the queue.".to_string(),
        reasoning: None,
        tool_calls: Vec::new(),
        finish_reason: Some("stop".to_string()),
    });

    let (status2, resp2) = h
        .send_message(
            &conv.id,
            &SendMessageRequest {
                content: "@programmer List pending tasks".to_string(),
                ..Default::default()
            },
        )
        .await;
    assert_eq!(status2, StatusCode::CREATED);
    let task2 = resp2.task.expect("task must be returned");
    assert_eq!(task2.assigned_agent_id, "programmer");
    assert_eq!(task2.status, TaskStatus::Completed);

    let recorded2 = h.mock.recorded_requests();
    assert_eq!(
        recorded2.len(),
        1,
        "@programmer must execute a single Programmer turn without a second Commander turn"
    );
    let sys2 = system_prompt_of(&recorded2[0].body);
    assert!(sys2.contains("Agent Identity: Programmer (id: programmer"));
    assert!(!sys2.contains("Agent Identity: Commander (id: commander"));
    let user2 = last_user_prompt_of(&recorded2[0].body);
    assert!(user2.contains("The user asks you directly: List pending tasks"));

    let history = h.list_messages(&conv.id).await;
    let assistant_agents: Vec<_> = history
        .iter()
        .filter(|m| m.role == ChatRole::Assistant)
        .map(|m| m.agent_id.as_deref().unwrap_or(""))
        .collect();
    assert_eq!(assistant_agents, vec!["programmer", "programmer"]);

    h.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn natural_phrasing_routes_to_programmer_and_writes_flappy_bird_html() {
    let h = TestHarness::start().await;
    let conv = h
        .create_group_conversation("Flappy Bird Routing", &["researcher", "programmer"])
        .await;

    let flappy_html = "<!doctype html>\n<html lang=\"en\">\n<head><meta charset=\"utf-8\"><title>Flappy Bird</title></head>\n<body>\n<canvas id=\"game\" width=\"360\" height=\"520\"></canvas>\n<script>const bird={y:200,vy:0};console.log('ready',bird);</script>\n</body>\n</html>";

    // Part A: Natural 3rd-person phrasing routes directly to Programmer, reframes prompt into 2nd person,
    // and Programmer emits write_file creating flappy-bird.html in the workspace.
    h.mock.push_scripted(ScriptedResponse {
        content: String::new(),
        reasoning: None,
        tool_calls: vec![ScriptedToolCall {
            id: "call_write_flappy_1".to_string(),
            name: "write_file".to_string(),
            arguments: json!({
                "path": "flappy-bird.html",
                "content": flappy_html,
            }),
        }],
        finish_reason: Some("tool_calls".to_string()),
    });
    h.mock.push_scripted(ScriptedResponse {
        content: "I created `flappy-bird.html` in the workspace.".to_string(),
        reasoning: None,
        tool_calls: Vec::new(),
        finish_reason: Some("stop".to_string()),
    });

    let natural_prompt =
        "ask programmer can he do simple flappy bird game in html working one 100% sure it works";
    let (status, resp) = h
        .send_message(
            &conv.id,
            &SendMessageRequest {
                content: natural_prompt.to_string(),
                ..Default::default()
            },
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(resp.user_message.content, natural_prompt);
    let task = resp.task.expect("task must be returned");
    assert_eq!(task.assigned_agent_id, "programmer");
    assert_eq!(task.status, TaskStatus::Completed);

    let recorded = h.mock.recorded_requests();
    assert_eq!(recorded.len(), 2, "expected tool-call turn + observation turn");
    for req in &recorded {
        let sys = system_prompt_of(&req.body);
        assert!(sys.contains("Agent Identity: Programmer (id: programmer"));
        assert!(!sys.contains("Agent Identity: Commander (id: commander"));
    }

    let first_user_prompt = last_user_prompt_of(&recorded[0].body);
    assert!(
        first_user_prompt.starts_with(
            "The user asks you directly: can he do simple flappy bird game in html working one 100% sure it works"
        ),
        "expected leading 'ask programmer' stripped and reframed into direct address, got: {first_user_prompt}"
    );
    assert!(first_user_prompt.contains("Respond in 1st person as yourself"));

    let second_msgs = recorded[1].body["messages"].as_array().unwrap();
    assert!(
        second_msgs.iter().any(|m| m["role"].as_str() == Some("tool")),
        "second inference turn must include write_file tool observation"
    );

    let flappy_path = h.workspace_dir().join("flappy-bird.html");
    assert!(flappy_path.exists(), "flappy-bird.html must exist on disk");
    assert_eq!(fs::read_to_string(&flappy_path).unwrap(), flappy_html);

    // Part B: Verify file-first fallback also writes flappy-bird.html when Programmer returns a long fenced HTML block.
    let del_resp = h
        .client
        .delete(format!("{}/api/v1/files", h.base_url))
        .query(&[("path", "flappy-bird.html")])
        .send()
        .await
        .unwrap();
    assert_eq!(del_resp.status(), StatusCode::OK);
    assert!(!flappy_path.exists());

    h.mock.clear_recorded();
    let long_body = (0..22)
        .map(|i| format!("  <div>pipe-{i}</div>"))
        .collect::<Vec<_>>()
        .join("\n");
    h.mock.push_scripted(ScriptedResponse {
        content: format!("Here is the complete game:\n```html\n<!doctype html>\n<html>\n<body>\n{long_body}\n</body>\n</html>\n```"),
        reasoning: None,
        tool_calls: Vec::new(),
        finish_reason: Some("stop".to_string()),
    });

    let (status_fb, resp_fb) = h
        .send_message(
            &conv.id,
            &SendMessageRequest {
                content: "ask programmer create a flappy bird html game".to_string(),
                ..Default::default()
            },
        )
        .await;
    assert_eq!(status_fb, StatusCode::CREATED);
    let task_fb = resp_fb.task.expect("fallback task must be returned");
    assert_eq!(task_fb.assigned_agent_id, "programmer");
    assert_eq!(task_fb.status, TaskStatus::Completed);
    assert!(
        flappy_path.exists(),
        "fallback file creation must write flappy-bird.html to workspace"
    );
    assert!(
        task_fb
            .result
            .as_deref()
            .unwrap_or("")
            .contains("flappy-bird.html"),
        "task result should confirm saved flappy-bird.html"
    );

    h.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn group_message_parallel_specialists_completes_concisely_without_meta_commentary() {
    let h = TestHarness::start().await;
    let conv = h
        .create_group_conversation(
            "Parallel Group Specialists",
            &["researcher", "programmer", "analyst"],
        )
        .await;

    // 1. Coordinator selection JSON
    h.mock.push_scripted(ScriptedResponse {
        content: json!({
            "selected_agents": ["researcher", "programmer"],
            "direct_answer": null
        })
        .to_string(),
        reasoning: None,
        tool_calls: Vec::new(),
        finish_reason: Some("stop".to_string()),
    });
    // 2 & 3. Two parallel specialist replies (order between researcher & programmer may interleave)
    h.mock.push_scripted(ScriptedResponse {
        content: "I checked the runtime logs and confirmed all routes are healthy.".to_string(),
        reasoning: None,
        tool_calls: Vec::new(),
        finish_reason: Some("stop".to_string()),
    });
    h.mock.push_scripted(ScriptedResponse {
        content: "I verified the workspace layout and prepared the config patch.".to_string(),
        reasoning: None,
        tool_calls: Vec::new(),
        finish_reason: Some("stop".to_string()),
    });
    // 4. Commander final 1st-person synthesis (runs after both specialists join)
    h.mock.push_scripted(ScriptedResponse {
        content: "I coordinated with Researcher and Programmer: all routes are healthy and the config patch is ready."
            .to_string(),
        reasoning: None,
        tool_calls: Vec::new(),
        finish_reason: Some("stop".to_string()),
    });

    let (status, resp) = h
        .send_message(
            &conv.id,
            &SendMessageRequest {
                content: "Inspect the system logs and prepare a config patch".to_string(),
                ..Default::default()
            },
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let task = resp.task.expect("group root task must be returned");
    assert_eq!(task.assigned_agent_id, "commander");
    assert_eq!(task.status, TaskStatus::Completed);

    let final_answer = task.result.as_deref().unwrap_or("");
    assert!(final_answer.starts_with("I "));
    assert!(!final_answer.contains("# Unified Response"));
    assert!(!final_answer.contains("Status: No agent has a meaningful response"));
    assert!(!final_answer.contains("outside their domain"));

    let recorded = h.mock.recorded_requests();
    assert_eq!(
        recorded.len(),
        4,
        "expected 1 coordinator + 2 parallel specialists + 1 synthesis inference calls"
    );

    let messages = h.list_messages(&conv.id).await;
    let assistant_msgs: Vec<&ChatMessage> = messages
        .iter()
        .filter(|m| m.role == ChatRole::Assistant)
        .collect();
    assert_eq!(assistant_msgs.len(), 3);
    assert!(assistant_msgs
        .iter()
        .any(|m| m.agent_id.as_deref() == Some("researcher")));
    assert!(assistant_msgs
        .iter()
        .any(|m| m.agent_id.as_deref() == Some("programmer")));
    assert_eq!(
        assistant_msgs.last().unwrap().agent_id.as_deref(),
        Some("commander")
    );
    for msg in &assistant_msgs {
        assert!(
            msg.content.starts_with("I "),
            "expected concise 1st-person answer from {:?}, got: {}",
            msg.agent_id,
            msg.content
        );
        assert!(!msg.content.contains("# Unified Response"));
        assert!(!msg.content.contains("outside their domain"));
    }

    h.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn async_dispatch_returns_201_immediately_and_runs_two_messages_in_parallel() {
    let h = TestHarness::start().await;
    let conv = h
        .create_group_conversation("Async Parallel Dispatch", &["researcher", "programmer"])
        .await;

    h.mock.set_delay(Duration::from_millis(450));
    h.mock.set_mode(MockMode::SlowDelay);

    let req_researcher = SendMessageRequest {
        content: "@researcher Summarize recent metrics".to_string(),
        async_dispatch: true,
        ..Default::default()
    };
    let req_programmer = SendMessageRequest {
        content: "@programmer Check workspace scripts".to_string(),
        async_dispatch: true,
        ..Default::default()
    };

    let t0 = Instant::now();
    let ((s1, r1), (s2, r2)) = tokio::join!(
        h.send_message(&conv.id, &req_researcher),
        h.send_message(&conv.id, &req_programmer),
    );
    let post_elapsed = t0.elapsed();

    assert_eq!(s1, StatusCode::CREATED);
    assert_eq!(s2, StatusCode::CREATED);
    assert!(
        post_elapsed < Duration::from_millis(320),
        "async_dispatch POSTs should return 201 immediately before 450ms mock inference completes, took {post_elapsed:?}"
    );

    let task1 = r1.task.expect("async dispatch 1 must return TaskRecord");
    let task2 = r2.task.expect("async dispatch 2 must return TaskRecord");
    assert_eq!(task1.assigned_agent_id, "researcher");
    assert_eq!(task2.assigned_agent_id, "programmer");
    assert!(!task1.status.is_terminal());
    assert!(!task2.status.is_terminal());

    let (done1, done2) = tokio::join!(
        h.wait_task_terminal(&task1.id, Duration::from_secs(5)),
        h.wait_task_terminal(&task2.id, Duration::from_secs(5)),
    );
    let total_elapsed = t0.elapsed();

    assert_eq!(done1.status, TaskStatus::Completed);
    assert_eq!(done2.status, TaskStatus::Completed);
    assert!(done1.result.as_deref().is_some_and(|s| !s.is_empty()));
    assert!(done2.result.as_deref().is_some_and(|s| !s.is_empty()));
    assert!(
        total_elapsed < Duration::from_millis(850),
        "two 450ms background tasks should execute in parallel (<850ms), took {total_elapsed:?}"
    );

    let messages = h.list_messages(&conv.id).await;
    assert_eq!(messages.len(), 4, "expected 2 user + 2 assistant messages");
    assert!(messages
        .iter()
        .any(|m| m.role == ChatRole::Assistant && m.agent_id.as_deref() == Some("researcher")));
    assert!(messages
        .iter()
        .any(|m| m.role == ChatRole::Assistant && m.agent_id.as_deref() == Some("programmer")));

    h.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workspace_files_crud_preview_csp_shim_and_traversal_rejection() {
    let h = TestHarness::start().await;

    // 1. POST /api/v1/files/mkdir
    let mkdir_resp = h
        .client
        .post(format!("{}/api/v1/files/mkdir", h.base_url))
        .json(&json!({ "path": "arcade" }))
        .send()
        .await
        .unwrap();
    assert_eq!(mkdir_resp.status(), StatusCode::CREATED);
    let mkdir_json: Value = mkdir_resp.json().await.unwrap();
    assert_eq!(mkdir_json["created"], true);
    assert_eq!(mkdir_json["path"], "arcade");

    // 2. POST /api/v1/files/upload
    let html_src = "<!doctype html><html><head><title>Preview Test</title></head><body><h1>Game</h1><script>localStorage.setItem('score','42');</script></body></html>";
    let upload_resp = h
        .client
        .post(format!("{}/api/v1/files/upload", h.base_url))
        .json(&json!({
            "path": "arcade/game.html",
            "content": html_src,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(upload_resp.status(), StatusCode::CREATED);
    let uploaded: WorkspaceFileEntry = upload_resp.json().await.unwrap();
    assert_eq!(uploaded.name, "game.html");
    assert_eq!(uploaded.path, "arcade/game.html");
    assert!(!uploaded.is_dir);
    assert_eq!(uploaded.kind, "html");
    assert!(uploaded.previewable);

    // 3. GET /api/v1/files (root and subdir)
    let list_root_resp = h
        .client
        .get(format!("{}/api/v1/files", h.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(list_root_resp.status(), StatusCode::OK);
    let root_listing: WorkspaceFilesResponse = list_root_resp.json().await.unwrap();
    assert!(root_listing
        .entries
        .iter()
        .any(|e| e.name == "arcade" && e.is_dir));

    let list_sub_resp = h
        .client
        .get(format!("{}/api/v1/files", h.base_url))
        .query(&[("path", "arcade")])
        .send()
        .await
        .unwrap();
    assert_eq!(list_sub_resp.status(), StatusCode::OK);
    let sub_listing: WorkspaceFilesResponse = list_sub_resp.json().await.unwrap();
    assert_eq!(sub_listing.current_path, "arcade");
    assert!(sub_listing
        .entries
        .iter()
        .any(|e| e.path == "arcade/game.html" && !e.is_dir));

    // 4. GET /api/v1/files/content
    let content_resp = h
        .client
        .get(format!("{}/api/v1/files/content", h.base_url))
        .query(&[("path", "arcade/game.html")])
        .send()
        .await
        .unwrap();
    assert_eq!(content_resp.status(), StatusCode::OK);
    let content_json: Value = content_resp.json().await.unwrap();
    assert_eq!(content_json["path"], "arcade/game.html");
    assert_eq!(content_json["name"], "game.html");
    assert_eq!(content_json["language"], "html");
    assert_eq!(content_json["content"], html_src);

    // 5. GET /api/v1/files/download
    let dl_resp = h
        .client
        .get(format!("{}/api/v1/files/download", h.base_url))
        .query(&[("path", "arcade/game.html")])
        .send()
        .await
        .unwrap();
    assert_eq!(dl_resp.status(), StatusCode::OK);
    assert_eq!(
        dl_resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        Some("application/octet-stream")
    );
    let disp = dl_resp
        .headers()
        .get(reqwest::header::CONTENT_DISPOSITION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(disp.contains("attachment; filename=\"game.html\""));
    let dl_bytes = dl_resp.bytes().await.unwrap();
    assert_eq!(dl_bytes.as_ref(), html_src.as_bytes());

    // 6. GET /api/v1/files/preview -> sandbox-compatible CSP, X-Frame-Options: SAMEORIGIN, and HTML storage shim
    let preview_resp = h
        .client
        .get(format!("{}/api/v1/files/preview", h.base_url))
        .query(&[("path", "arcade/game.html")])
        .send()
        .await
        .unwrap();
    assert_eq!(preview_resp.status(), StatusCode::OK);
    assert_eq!(
        preview_resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        Some("text/html; charset=utf-8")
    );
    assert_eq!(
        preview_resp
            .headers()
            .get(reqwest::header::X_FRAME_OPTIONS)
            .and_then(|v| v.to_str().ok()),
        Some("SAMEORIGIN")
    );
    assert_eq!(
        preview_resp
            .headers()
            .get(reqwest::header::CONTENT_SECURITY_POLICY)
            .and_then(|v| v.to_str().ok()),
        Some(PREVIEW_CSP_VALUE)
    );
    let preview_body = preview_resp.text().await.unwrap();
    assert!(
        preview_body.contains("localStorage") && preview_body.contains("sessionStorage"),
        "HTML preview must inject the sandbox localStorage/sessionStorage shim"
    );
    assert!(preview_body.contains("<h1>Game</h1>"));

    // 7. DELETE /api/v1/files
    let del_resp = h
        .client
        .delete(format!("{}/api/v1/files", h.base_url))
        .query(&[("path", "arcade/game.html")])
        .send()
        .await
        .unwrap();
    assert_eq!(del_resp.status(), StatusCode::OK);
    let del_json: Value = del_resp.json().await.unwrap();
    assert_eq!(del_json["deleted"], true);

    let after_del = h
        .client
        .get(format!("{}/api/v1/files/content", h.base_url))
        .query(&[("path", "arcade/game.html")])
        .send()
        .await
        .unwrap();
    assert_eq!(after_del.status(), StatusCode::NOT_FOUND);

    // 8. Path traversal (`../`) rejection -> 400 or 403 across all file endpoints
    let traversal_statuses = vec![
        h.client
            .get(format!("{}/api/v1/files", h.base_url))
            .query(&[("path", "../secret")])
            .send()
            .await
            .unwrap()
            .status(),
        h.client
            .get(format!("{}/api/v1/files/content", h.base_url))
            .query(&[("path", "../config.toml")])
            .send()
            .await
            .unwrap()
            .status(),
        h.client
            .get(format!("{}/api/v1/files/download", h.base_url))
            .query(&[("path", "../../etc/passwd")])
            .send()
            .await
            .unwrap()
            .status(),
        h.client
            .get(format!("{}/api/v1/files/preview", h.base_url))
            .query(&[("path", "../index.html")])
            .send()
            .await
            .unwrap()
            .status(),
        h.client
            .post(format!("{}/api/v1/files/mkdir", h.base_url))
            .json(&json!({ "path": "../escape" }))
            .send()
            .await
            .unwrap()
            .status(),
        h.client
            .post(format!("{}/api/v1/files/upload", h.base_url))
            .json(&json!({ "path": "../escape.txt", "content": "x" }))
            .send()
            .await
            .unwrap()
            .status(),
        h.client
            .delete(format!("{}/api/v1/files", h.base_url))
            .query(&[("path", "../config.toml")])
            .send()
            .await
            .unwrap()
            .status(),
    ];

    for st in traversal_statuses {
        assert!(
            matches!(st, StatusCode::BAD_REQUEST | StatusCode::FORBIDDEN),
            "expected 400 or 403 on ../ traversal, got {st}"
        );
    }

    h.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conversations_reorder_persists_up_down_and_ordered_ids() {
    let h = TestHarness::start().await;
    let seeded_ids: Vec<String> = h.list_conversations().await.into_iter().map(|c| c.id).collect();
    let conv_a = h
        .create_group_conversation("Reorder Conv A", &["programmer"])
        .await;
    let conv_b = h
        .create_group_conversation("Reorder Conv B", &["programmer"])
        .await;
    let conv_c = h
        .create_group_conversation("Reorder Conv C", &["programmer"])
        .await;

    // 1. Explicit partial ordered_ids places requested rooms first and keeps seeded rooms at tail
    let explicit_ids = vec![conv_c.id.clone(), conv_a.id.clone(), conv_b.id.clone()];
    let mut expected_explicit = explicit_ids.clone();
    expected_explicit.extend(seeded_ids.iter().cloned());
    let reorder_resp = h
        .client
        .post(format!("{}/api/v1/conversations/reorder", h.base_url))
        .json(&json!({ "ordered_ids": explicit_ids }))
        .send()
        .await
        .unwrap();
    assert_eq!(reorder_resp.status(), StatusCode::OK);
    let reordered: Vec<Conversation> = reorder_resp.json().await.unwrap();
    let reordered_ids: Vec<String> = reordered.iter().map(|c| c.id.clone()).collect();
    assert_eq!(reordered_ids, expected_explicit);
    assert!(reordered[0].sort_order < reordered[1].sort_order);
    assert!(reordered[1].sort_order < reordered[2].sort_order);

    let listed_1: Vec<String> = h
        .list_conversations()
        .await
        .into_iter()
        .map(|c| c.id)
        .collect();
    assert_eq!(listed_1, expected_explicit);

    // 2. direction = "up" moves conv_b from index 2 -> 1
    let up_resp = h
        .client
        .post(format!("{}/api/v1/conversations/reorder", h.base_url))
        .json(&json!({
            "conversation_id": conv_b.id,
            "direction": "up",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(up_resp.status(), StatusCode::OK);
    let mut expected_after_up = vec![conv_c.id.clone(), conv_b.id.clone(), conv_a.id.clone()];
    expected_after_up.extend(seeded_ids.iter().cloned());
    let listed_2: Vec<String> = h
        .list_conversations()
        .await
        .into_iter()
        .map(|c| c.id)
        .collect();
    assert_eq!(listed_2, expected_after_up);

    // 3. direction = "down" moves conv_c from index 0 -> 1
    let down_resp = h
        .client
        .post(format!("{}/api/v1/conversations/reorder", h.base_url))
        .json(&json!({
            "conversation_id": conv_c.id,
            "direction": "down",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(down_resp.status(), StatusCode::OK);
    let mut expected_after_down = vec![conv_b.id.clone(), conv_c.id.clone(), conv_a.id.clone()];
    expected_after_down.extend(seeded_ids.iter().cloned());
    let listed_3: Vec<String> = h
        .list_conversations()
        .await
        .into_iter()
        .map(|c| c.id)
        .collect();
    assert_eq!(listed_3, expected_after_down);

    // Confirm underlying storage persists the final order as well
    let persisted_ids: Vec<String> = h
        .system
        .store
        .list_conversations(false)
        .unwrap()
        .into_iter()
        .map(|c| c.id)
        .collect();
    assert_eq!(persisted_ids, expected_after_down);

    // Invalid direction must be rejected
    let invalid_resp = h
        .client
        .post(format!("{}/api/v1/conversations/reorder", h.base_url))
        .json(&json!({
            "conversation_id": conv_b.id,
            "direction": "sideways",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(invalid_resp.status(), StatusCode::BAD_REQUEST);

    h.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn message_actions_retry_regenerate_continue_fork_and_delete() {
    let h = TestHarness::start().await;
    let conv = h
        .create_group_conversation(
            "Message Actions Lifecycle",
            &["researcher", "programmer", "analyst"],
        )
        .await;

    // 1. Initial user + assistant turn
    h.push_scripted_text("Initial programmer draft.");
    let (st1, resp1) = h
        .send_message(
            &conv.id,
            &SendMessageRequest {
                content: "@programmer Provide initial draft".to_string(),
                ..Default::default()
            },
        )
        .await;
    assert_eq!(st1, StatusCode::CREATED);
    assert_eq!(resp1.task.as_ref().unwrap().status, TaskStatus::Completed);

    let msgs1 = h.list_messages(&conv.id).await;
    assert_eq!(msgs1.len(), 2);
    assert_eq!(msgs1[0].role, ChatRole::User);
    assert_eq!(msgs1[1].role, ChatRole::Assistant);
    assert_eq!(msgs1[1].content, "Initial programmer draft.");
    let user_msg_1_id = msgs1[0].id.clone();
    let asst_msg_1_id = msgs1[1].id.clone();

    // 2. Retry user message -> replaces turn with refreshed response
    h.push_scripted_text("Retried programmer draft.");
    let retry_http = h
        .client
        .post(format!(
            "{}/api/v1/conversations/{}/messages/{}/retry",
            h.base_url, conv.id, user_msg_1_id
        ))
        .json(&json!({ "async_dispatch": false }))
        .send()
        .await
        .unwrap();
    assert_eq!(retry_http.status(), StatusCode::OK);
    let retry_resp: SendMessageResponse = retry_http.json().await.unwrap();
    assert_eq!(
        retry_resp.task.as_ref().unwrap().status,
        TaskStatus::Completed
    );

    let msgs_after_retry = h.list_messages(&conv.id).await;
    assert_eq!(msgs_after_retry.len(), 2);
    assert_eq!(
        msgs_after_retry[0].content,
        "@programmer Provide initial draft"
    );
    assert_ne!(msgs_after_retry[1].id, asst_msg_1_id);
    assert_eq!(msgs_after_retry[1].content, "Retried programmer draft.");
    let asst_msg_2_id = msgs_after_retry[1].id.clone();

    // 3. Regenerate assistant message -> replaces turn cleanly
    h.push_scripted_text("Regenerated programmer answer.");
    let regen_http = h
        .client
        .post(format!(
            "{}/api/v1/conversations/{}/messages/{}/regenerate",
            h.base_url, conv.id, asst_msg_2_id
        ))
        .json(&json!({ "async_dispatch": false }))
        .send()
        .await
        .unwrap();
    assert_eq!(regen_http.status(), StatusCode::OK);
    let regen_resp: SendMessageResponse = regen_http.json().await.unwrap();
    assert_eq!(
        regen_resp.task.as_ref().unwrap().status,
        TaskStatus::Completed
    );

    let msgs_after_regen = h.list_messages(&conv.id).await;
    assert_eq!(msgs_after_regen.len(), 2);
    assert_ne!(msgs_after_regen[1].id, asst_msg_2_id);
    assert_eq!(
        msgs_after_regen[1].content,
        "Regenerated programmer answer."
    );
    let asst_msg_3_id = msgs_after_regen[1].id.clone();

    // 4. Continue assistant message -> runs async background task & appends to the existing message
    h.push_scripted_text(" Continued details appended.");
    let cont_http = h
        .client
        .post(format!(
            "{}/api/v1/conversations/{}/messages/{}/continue",
            h.base_url, conv.id, asst_msg_3_id
        ))
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(cont_http.status(), StatusCode::OK);
    let cont_resp: SendMessageResponse = cont_http.json().await.unwrap();
    let cont_task = cont_resp
        .task
        .expect("continue action must return background task");
    let done_cont = h
        .wait_task_terminal(&cont_task.id, Duration::from_secs(10))
        .await;
    assert_eq!(done_cont.status, TaskStatus::Completed);

    let msgs_after_cont = h.list_messages(&conv.id).await;
    assert_eq!(msgs_after_cont.len(), 2);
    assert_eq!(msgs_after_cont[1].id, asst_msg_3_id);
    assert_eq!(
        msgs_after_cont[1].content,
        "Regenerated programmer answer. Continued details appended."
    );

    // 5. Add a second turn, then fork up to asst_msg_3_id
    h.push_scripted_text("Second turn response.");
    let (st2, resp2) = h
        .send_message(
            &conv.id,
            &SendMessageRequest {
                content: "@programmer Second turn prompt".to_string(),
                ..Default::default()
            },
        )
        .await;
    assert_eq!(st2, StatusCode::CREATED);
    assert_eq!(resp2.task.as_ref().unwrap().status, TaskStatus::Completed);

    let msgs_before_fork = h.list_messages(&conv.id).await;
    assert_eq!(msgs_before_fork.len(), 4);
    let second_asst_id = msgs_before_fork[3].id.clone();

    let fork_http = h
        .client
        .post(format!(
            "{}/api/v1/conversations/{}/fork",
            h.base_url, conv.id
        ))
        .json(&json!({
            "up_to_message_id": asst_msg_3_id,
            "title": "Branch Alpha",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(fork_http.status(), StatusCode::CREATED);
    let forked: Conversation = fork_http.json().await.unwrap();
    assert_ne!(forked.id, conv.id);
    assert_eq!(forked.title, "Branch Alpha");

    let forked_msgs = h.list_messages(&forked.id).await;
    assert_eq!(forked_msgs.len(), 2);
    assert_eq!(forked_msgs[0].content, "@programmer Provide initial draft");
    assert_eq!(
        forked_msgs[1].content,
        "Regenerated programmer answer. Continued details appended."
    );
    assert_ne!(forked_msgs[1].id, asst_msg_3_id);

    // 6. Delete single message from source conversation and confirm 404 when already deleted
    let del_http = h
        .client
        .delete(format!(
            "{}/api/v1/conversations/{}/messages/{}",
            h.base_url, conv.id, second_asst_id
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(del_http.status(), StatusCode::OK);
    let del_json: Value = del_http.json().await.unwrap();
    assert_eq!(del_json["deleted"], true);
    assert_eq!(del_json["conversation_id"], conv.id);
    assert_eq!(del_json["message_id"], second_asst_id);

    let msgs_after_del = h.list_messages(&conv.id).await;
    assert_eq!(msgs_after_del.len(), 3);
    assert!(msgs_after_del.iter().all(|m| m.id != second_asst_id));

    let del_missing_http = h
        .client
        .delete(format!(
            "{}/api/v1/conversations/{}/messages/{}",
            h.base_url, conv.id, second_asst_id
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(del_missing_http.status(), StatusCode::NOT_FOUND);

    h.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn script_auto_transport_with_mutating_approval_required_and_upload_edit() {
    assert!(
        !AppConfig::with_data_dir(PathBuf::from("."))
            .security
            .require_approval_for_mutating,
        "default config must not require approval for mutating tools"
    );

    let h = TestHarness::start_with_mutating_approval(true).await;
    let conv = h
        .create_group_conversation(
            "Auto Script Transport Approval Test",
            &["researcher", "programmer", "analyst"],
        )
        .await;

    let html_lines = [
        "<!DOCTYPE html>",
        "<html lang=\"en\">",
        "<head>",
        "  <meta charset=\"utf-8\">",
        "  <title>Arcade Canvas</title>",
        "  <style>body{margin:0;background:#0f172a;}</style>",
        "</head>",
        "<body>",
        "  <canvas id=\"stage\" width=\"480\" height=\"320\"></canvas>",
        "  <script>",
        "    const canvas = document.getElementById('stage');",
        "    const ctx = canvas.getContext('2d');",
        "    ctx.fillStyle = '#0f172a';",
        "    ctx.fillRect(0, 0, canvas.width, canvas.height);",
        "    ctx.fillStyle = '#38bdf8';",
        "    ctx.fillRect(40, 40, 120, 80);",
        "  </script>",
        "</body>",
        "</html>",
    ];
    assert!(html_lines.len() >= 15);
    let scripted_html = html_lines.join("\n");
    h.push_scripted_text(&format!(
        "Here is the arcade canvas page:\n```html\n{scripted_html}\n```"
    ));

    let (status, resp) = h
        .send_message(
            &conv.id,
            &SendMessageRequest {
                content: "@programmer Build an arcade canvas webpage".to_string(),
                ..Default::default()
            },
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let task = resp.task.expect("task must be returned");
    assert_eq!(task.assigned_agent_id, "programmer");
    assert_eq!(
        task.status,
        TaskStatus::Completed,
        "auto script transport must complete without blocking on Waiting approval"
    );

    // 1. Fenced HTML block was written directly to workspace/index.html
    let disk_path = h.workspace_dir().join("index.html");
    assert!(
        disk_path.exists(),
        "transported index.html must exist on disk inside workspace"
    );
    let disk_content = fs::read_to_string(&disk_path).unwrap();
    assert!(disk_content.contains("<title>Arcade Canvas</title>"));
    assert!(disk_content.contains("ctx.fillRect(40, 40, 120, 80);"));

    // 2. Persisted assistant msg.content holds [FILE_CREATED: index.html] instead of raw 15+ line block
    let msgs = h.list_messages(&conv.id).await;
    let asst = msgs
        .iter()
        .rev()
        .find(|m| m.role == ChatRole::Assistant)
        .expect("assistant message must be persisted");
    assert!(
        asst.content.contains("[FILE_CREATED: index.html]"),
        "expected [FILE_CREATED: index.html] marker in msg.content, got: {}",
        asst.content
    );
    assert!(
        !asst.content.contains("```html") && !asst.content.contains("ctx.fillRect(40, 40, 120, 80);"),
        "raw 15+ line fenced script block must be replaced in msg.content, got: {}",
        asst.content
    );

    // 3. No pending approval left behind by auto-transported script write
    let approvals_resp = h
        .client
        .get(format!("{}/api/v1/approvals", h.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(approvals_resp.status(), StatusCode::OK);
    let approvals: Vec<Value> = approvals_resp.json().await.unwrap();
    assert!(
        approvals.is_empty(),
        "auto-transported write must not leave pending approval records"
    );

    // 4. Editing via POST /api/v1/files/upload persists updated script content
    let updated_html = "<!DOCTYPE html><html><body><h1>Updated Arcade v2</h1></body></html>\n";
    let upload_resp = h
        .client
        .post(format!("{}/api/v1/files/upload", h.base_url))
        .json(&json!({
            "path": "index.html",
            "content": updated_html,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(upload_resp.status(), StatusCode::CREATED);
    let uploaded_entry: WorkspaceFileEntry = upload_resp.json().await.unwrap();
    assert_eq!(uploaded_entry.path, "index.html");

    let content_resp = h
        .client
        .get(format!("{}/api/v1/files/content", h.base_url))
        .query(&[("path", "index.html")])
        .send()
        .await
        .unwrap();
    assert_eq!(content_resp.status(), StatusCode::OK);
    let content_json: Value = content_resp.json().await.unwrap();
    assert_eq!(content_json["content"], updated_html);
    assert_eq!(fs::read_to_string(&disk_path).unwrap(), updated_html);

    h.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn agents_enable_all_reenables_disabled_agents() {
    let h = TestHarness::start().await;

    for target in ["researcher", "analyst"] {
        let resp = h
            .client
            .post(format!("{}/api/v1/agents/{target}/disable", h.base_url))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let disabled: Value = resp.json().await.unwrap();
        assert_eq!(disabled["id"].as_str(), Some(target));
        assert_eq!(disabled["enabled"].as_bool(), Some(false));
    }

    let list_before: Vec<Value> = h
        .client
        .get(format!("{}/api/v1/agents", h.base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(list_before
        .iter()
        .any(|a| a["id"].as_str() == Some("researcher") && a["enabled"].as_bool() == Some(false)));
    assert!(list_before
        .iter()
        .any(|a| a["id"].as_str() == Some("analyst") && a["enabled"].as_bool() == Some(false)));

    let enable_all_resp = h
        .client
        .post(format!("{}/api/v1/agents/enable-all", h.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(enable_all_resp.status(), StatusCode::OK);
    let enable_json: Value = enable_all_resp.json().await.unwrap();
    assert_eq!(enable_json["enabled_count"], 2);

    let list_after: Vec<Value> = h
        .client
        .get(format!("{}/api/v1/agents", h.base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(!list_after.is_empty());
    assert!(
        list_after.iter().all(|a| a["enabled"].as_bool() == Some(true)),
        "all agents must be enabled after POST /api/v1/agents/enable-all"
    );

    // Second enable-all call when none are disabled reports enabled_count == 0
    let second_resp = h
        .client
        .post(format!("{}/api/v1/agents/enable-all", h.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(second_resp.status(), StatusCode::OK);
    let second_json: Value = second_resp.json().await.unwrap();
    assert_eq!(second_json["enabled_count"], 0);

    h.shutdown().await;
}

struct ValidatingProxy {
    base_url: String,
    validated_chat_count: Arc<AtomicUsize>,
    validated_prefill_count: Arc<AtomicUsize>,
    rejected_400_count: Arc<AtomicUsize>,
    upstream_404_count: Arc<AtomicUsize>,
    rejections: Arc<Mutex<Vec<String>>>,
    accept_handle: JoinHandle<()>,
}

impl Drop for ValidatingProxy {
    fn drop(&mut self) {
        self.accept_handle.abort();
    }
}

impl ValidatingProxy {
    async fn start(upstream_base_url: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{addr}/v1");

        let upstream_trimmed = upstream_base_url.trim_end_matches('/');
        let upstream_origin = upstream_trimmed
            .strip_suffix("/v1")
            .unwrap_or(upstream_trimmed)
            .to_string();

        let validated_chat_count = Arc::new(AtomicUsize::new(0));
        let validated_prefill_count = Arc::new(AtomicUsize::new(0));
        let rejected_400_count = Arc::new(AtomicUsize::new(0));
        let upstream_404_count = Arc::new(AtomicUsize::new(0));
        let rejections = Arc::new(Mutex::new(Vec::new()));

        let upstream_client = Client::builder()
            .pool_max_idle_per_host(0)
            .build()
            .unwrap();

        let chat_cnt = Arc::clone(&validated_chat_count);
        let prefill_cnt = Arc::clone(&validated_prefill_count);
        let rej_cnt = Arc::clone(&rejected_400_count);
        let nf_cnt = Arc::clone(&upstream_404_count);
        let rej_log = Arc::clone(&rejections);

        let accept_handle = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let up_origin = upstream_origin.clone();
                let up_client = upstream_client.clone();
                let c_cnt = Arc::clone(&chat_cnt);
                let p_cnt = Arc::clone(&prefill_cnt);
                let r_cnt = Arc::clone(&rej_cnt);
                let n_cnt = Arc::clone(&nf_cnt);
                let r_log = Arc::clone(&rej_log);
                tokio::spawn(async move {
                    handle_validating_proxy_conn(
                        stream, up_origin, up_client, c_cnt, p_cnt, r_cnt, n_cnt, r_log,
                    )
                    .await;
                });
            }
        });

        Self {
            base_url,
            validated_chat_count,
            validated_prefill_count,
            rejected_400_count,
            upstream_404_count,
            rejections,
            accept_handle,
        }
    }

    fn validated_chat_count(&self) -> usize {
        self.validated_chat_count.load(Ordering::SeqCst)
    }

    fn validated_prefill_count(&self) -> usize {
        self.validated_prefill_count.load(Ordering::SeqCst)
    }

    fn rejected_400_count(&self) -> usize {
        self.rejected_400_count.load(Ordering::SeqCst)
    }

    fn upstream_404_count(&self) -> usize {
        self.upstream_404_count.load(Ordering::SeqCst)
    }

    fn rejections_snapshot(&self) -> Vec<String> {
        self.rejections.lock().unwrap().clone()
    }
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

fn decode_chunked_body(mut raw: &[u8]) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    loop {
        let crlf = raw.windows(2).position(|w| w == b"\r\n")?;
        let size_line = std::str::from_utf8(&raw[..crlf]).ok()?;
        let size_hex = size_line.split(';').next()?.trim();
        let size = usize::from_str_radix(size_hex, 16).ok()?;
        raw = &raw[crlf + 2..];
        if size == 0 {
            if raw.len() < 2 {
                return None;
            }
            return Some(out);
        }
        if raw.len() < size + 2 {
            return None;
        }
        out.extend_from_slice(&raw[..size]);
        raw = &raw[size + 2..];
    }
}

async fn read_one_http_request(
    stream: &mut TcpStream,
) -> Option<(String, String, Vec<(String, String)>, Vec<u8>)> {
    let mut buf = Vec::with_capacity(4096);
    let mut tmp = [0u8; 4096];
    let header_end = loop {
        if let Some(pos) = find_header_end(&buf) {
            break pos;
        }
        let n = stream.read(&mut tmp).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.len() > 4 * 1024 * 1024 {
            return None;
        }
    };

    let head_str = std::str::from_utf8(&buf[..header_end]).ok()?;
    let mut lines = head_str.split("\r\n");
    let req_line = lines.next()?;
    let mut req_parts = req_line.split_whitespace();
    let method = req_parts.next()?.to_string();
    let target = req_parts.next()?.to_string();

    let mut headers = Vec::new();
    let mut content_length: Option<usize> = None;
    let mut is_chunked = false;
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            let name = k.trim().to_string();
            let val = v.trim().to_string();
            if name.eq_ignore_ascii_case("content-length") {
                content_length = val.parse::<usize>().ok();
            } else if name.eq_ignore_ascii_case("transfer-encoding")
                && val.to_ascii_lowercase().contains("chunked")
            {
                is_chunked = true;
            }
            headers.push((name, val));
        }
    }

    let body_start = header_end + 4;
    let body = if is_chunked {
        loop {
            if let Some(decoded) = decode_chunked_body(&buf[body_start..]) {
                break decoded;
            }
            let n = stream.read(&mut tmp).await.ok()?;
            if n == 0 {
                return None;
            }
            buf.extend_from_slice(&tmp[..n]);
        }
    } else if let Some(len) = content_length {
        while buf.len() < body_start + len {
            let n = stream.read(&mut tmp).await.ok()?;
            if n == 0 {
                return None;
            }
            buf.extend_from_slice(&tmp[..n]);
        }
        buf[body_start..body_start + len].to_vec()
    } else {
        Vec::new()
    };

    Some((method, target, headers, body))
}

fn validate_outbound_chat_messages(messages: &[Value]) -> std::result::Result<(), String> {
    let mut prev_role: Option<&str> = None;
    let mut trailing_assistants = 0usize;

    for (idx, msg) in messages.iter().enumerate() {
        let role = msg["role"].as_str().unwrap_or("");
        if role == "assistant" {
            trailing_assistants += 1;
        } else {
            trailing_assistants = 0;
        }

        if let Some(pr) = prev_role {
            if pr == "assistant" && role == "assistant" {
                return Err(format!(
                    "2+ consecutive assistant messages at index {}..={idx}: {}",
                    idx - 1,
                    serde_json::to_string(messages).unwrap_or_default()
                ));
            }
            if pr == "user" && role == "user" {
                return Err(format!(
                    "2+ consecutive user messages at index {}..={idx}: {}",
                    idx - 1,
                    serde_json::to_string(messages).unwrap_or_default()
                ));
            }
            if pr == "assistant" && role == "tool" {
                let has_tc = messages[idx - 1]["tool_calls"]
                    .as_array()
                    .is_some_and(|a| !a.is_empty());
                if !has_tc {
                    return Err(format!(
                        "tool message at index {idx} after assistant without tool_calls"
                    ));
                }
            }
        }
        prev_role = Some(role);
    }

    if trailing_assistants >= 2 {
        return Err(format!(
            "request ends with {trailing_assistants} consecutive assistant messages"
        ));
    }

    Ok(())
}

async fn write_http_response(
    stream: &mut TcpStream,
    status: StatusCode,
    content_type: &str,
    body: &[u8],
) {
    let head = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        status.as_u16(),
        status.canonical_reason().unwrap_or("OK"),
        content_type,
        body.len(),
    );
    let _ = stream.write_all(head.as_bytes()).await;
    let _ = stream.write_all(body).await;
    let _ = stream.flush().await;
}

async fn handle_validating_proxy_conn(
    mut stream: TcpStream,
    upstream_origin: String,
    upstream_client: Client,
    validated_chat_count: Arc<AtomicUsize>,
    validated_prefill_count: Arc<AtomicUsize>,
    rejected_400_count: Arc<AtomicUsize>,
    upstream_404_count: Arc<AtomicUsize>,
    rejections: Arc<Mutex<Vec<String>>>,
) {
    let Some((method, target, headers, body)) = read_one_http_request(&mut stream).await else {
        return;
    };

    let path_only = target.split('?').next().unwrap_or(&target);
    if method.eq_ignore_ascii_case("POST") && path_only == "/v1/chat/completions" {
        let parsed: Value = match serde_json::from_slice(&body) {
            Ok(v) => v,
            Err(e) => {
                let msg = format!("invalid JSON payload to /v1/chat/completions: {e}");
                rejected_400_count.fetch_add(1, Ordering::SeqCst);
                rejections.lock().unwrap().push(msg.clone());
                let err_bytes =
                    serde_json::to_vec(&json!({ "error": { "message": msg } })).unwrap();
                write_http_response(&mut stream, StatusCode::BAD_REQUEST, "application/json", &err_bytes)
                    .await;
                return;
            }
        };

        let Some(msgs) = parsed.get("messages").and_then(|v| v.as_array()) else {
            let msg = "missing messages array in /v1/chat/completions payload".to_string();
            rejected_400_count.fetch_add(1, Ordering::SeqCst);
            rejections.lock().unwrap().push(msg.clone());
            let err_bytes =
                serde_json::to_vec(&json!({ "error": { "message": msg } })).unwrap();
            write_http_response(&mut stream, StatusCode::BAD_REQUEST, "application/json", &err_bytes)
                .await;
            return;
        };

        if let Err(reason) = validate_outbound_chat_messages(msgs) {
            rejected_400_count.fetch_add(1, Ordering::SeqCst);
            rejections.lock().unwrap().push(reason.clone());
            let err_bytes =
                serde_json::to_vec(&json!({ "error": { "message": reason } })).unwrap();
            write_http_response(&mut stream, StatusCode::BAD_REQUEST, "application/json", &err_bytes)
                .await;
            return;
        }

        if parsed.get("n_predict").and_then(|v| v.as_i64()) == Some(0) {
            validated_prefill_count.fetch_add(1, Ordering::SeqCst);
        } else {
            validated_chat_count.fetch_add(1, Ordering::SeqCst);
        }
    }

    let url = format!("{upstream_origin}{target}");
    let req_method = reqwest::Method::from_bytes(method.as_bytes()).unwrap_or(reqwest::Method::GET);
    let mut rb = upstream_client.request(req_method, &url);
    for (k, v) in &headers {
        if k.eq_ignore_ascii_case("content-type")
            || k.eq_ignore_ascii_case("accept")
            || k.eq_ignore_ascii_case("authorization")
        {
            rb = rb.header(k.as_str(), v.as_str());
        }
    }
    if !body.is_empty() {
        rb = rb.body(body);
    }

    match rb.send().await {
        Ok(up_resp) => {
            let status = up_resp.status();
            if status == StatusCode::NOT_FOUND {
                upstream_404_count.fetch_add(1, Ordering::SeqCst);
                rejections
                    .lock()
                    .unwrap()
                    .push(format!("upstream 404 on {method} {target} (url={url})"));
            }
            let content_type = up_resp
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("application/json")
                .to_string();
            let up_bytes = up_resp.bytes().await.unwrap_or_default();
            write_http_response(&mut stream, status, &content_type, &up_bytes).await;
        }
        Err(e) => {
            let err_bytes =
                serde_json::to_vec(&json!({ "error": { "message": format!("upstream proxy error: {e}") } }))
                    .unwrap();
            write_http_response(
                &mut stream,
                StatusCode::BAD_GATEWAY,
                "application/json",
                &err_bytes,
            )
            .await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn thinking_off_greetings_route_only_to_commander() {
    let h = TestHarness::start_with_thinking_off().await;

    // Configure Commander agent itself to ThinkingLevel::Off as well
    let cmd_agent: Value = h
        .client
        .get(format!("{}/api/v1/agents/commander", h.base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let mut cmd_mc = cmd_agent["model_config"].clone();
    cmd_mc["thinking_level"] = json!("off");
    let upd = h
        .client
        .put(format!("{}/api/v1/agents/commander", h.base_url))
        .json(&json!({ "model_config": cmd_mc }))
        .send()
        .await
        .unwrap();
    assert_eq!(upd.status(), StatusCode::OK);

    let conv = h
        .create_group_conversation(
            "Thinking Off Polish Greetings",
            &["researcher", "programmer", "executor", "analyst"],
        )
        .await;

    // Turn 1: "dzien dobry" with explicit ThinkingLevel::Off
    h.push_scripted_text("Dzien dobry! W czym moge Ci dzisiaj pomoc?");
    let (st1, resp1) = h
        .send_message(
            &conv.id,
            &SendMessageRequest {
                content: "dzien dobry".to_string(),
                thinking_level: Some(ThinkingLevel::Off),
                ..Default::default()
            },
        )
        .await;
    assert_eq!(st1, StatusCode::CREATED);
    let task1 = resp1.task.expect("task1 must be returned");
    assert_eq!(task1.assigned_agent_id, "commander");
    assert_eq!(task1.status, TaskStatus::Completed);

    let msgs1 = h.list_messages(&conv.id).await;
    let asst1: Vec<&ChatMessage> = msgs1
        .iter()
        .filter(|m| m.role == ChatRole::Assistant)
        .collect();
    assert_eq!(asst1.len(), 1, "turn 1 must persist exactly 1 assistant message");
    assert_eq!(asst1[0].agent_id.as_deref(), Some("commander"));

    // Turn 2: "dzien dobry commander" using configured ThinkingLevel::Off (no explicit override)
    h.push_scripted_text("Dzien dobry! Commander do uslug.");
    let (st2, resp2) = h
        .send_message(
            &conv.id,
            &SendMessageRequest {
                content: "dzien dobry commander".to_string(),
                ..Default::default()
            },
        )
        .await;
    assert_eq!(st2, StatusCode::CREATED);
    let task2 = resp2.task.expect("task2 must be returned");
    assert_eq!(task2.assigned_agent_id, "commander");
    assert_eq!(task2.status, TaskStatus::Completed);

    let msgs2 = h.list_messages(&conv.id).await;
    let asst2: Vec<&ChatMessage> = msgs2
        .iter()
        .filter(|m| m.role == ChatRole::Assistant)
        .collect();
    assert_eq!(
        asst2.len(),
        2,
        "2 greeting turns must persist exactly 2 total assistant messages (1 per turn)"
    );
    assert!(
        asst2
            .iter()
            .all(|m| m.agent_id.as_deref() == Some("commander")),
        "all greeting replies must belong strictly to commander (never programmer), got: {:?}",
        asst2.iter().map(|m| m.agent_id.as_deref()).collect::<Vec<_>>()
    );

    let recorded = h.mock.recorded_requests();
    assert_eq!(
        recorded.len(),
        2,
        "greetings must invoke only Commander (no coordinator selection LLM or programmer call)"
    );
    for (idx, req) in recorded.iter().enumerate() {
        let sys = system_prompt_of(&req.body);
        assert!(
            sys.contains("Agent Identity: Commander (id: commander"),
            "request {idx} must target Commander"
        );
        assert!(
            !sys.contains("Agent Identity: Programmer (id: programmer"),
            "request {idx} must never invoke Programmer"
        );
        assert_eq!(
            req.body["chat_template_kwargs"]["enable_thinking"],
            json!(false),
            "request {idx} must send ThinkingLevel::Off (enable_thinking=false)"
        );
    }

    h.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn polish_and_vocative_executor_routing_without_commander_duplicate() {
    let h = TestHarness::start().await;
    let conv = h
        .create_group_conversation(
            "Polish Vocative Executor Routing",
            &["commander", "executor", "programmer", "researcher"],
        )
        .await;

    let cases = [
        (
            "executor slyszusz mnie",
            ThinkingLevel::Off,
            "slyszusz mnie",
            "Tak, slysze Cie wyraznie.",
        ),
        (
            "powiedz executorowi ze ma odpisac czesc",
            ThinkingLevel::Low,
            "odpisac czesc",
            "Czesc!",
        ),
        (
            "executor slyszusz mnie",
            ThinkingLevel::Low,
            "slyszusz mnie",
            "Melduje gotowosc do pracy.",
        ),
        (
            "powiedz executorowi ze ma odpisac czesc",
            ThinkingLevel::Off,
            "odpisac czesc",
            "Czesc, gotowy!",
        ),
    ];

    for (idx, (prompt, level, expected_inner, scripted_reply)) in cases.iter().enumerate() {
        h.mock.clear_recorded();
        h.push_scripted_text(scripted_reply);

        let before_count = h
            .list_messages(&conv.id)
            .await
            .into_iter()
            .filter(|m| m.role == ChatRole::Assistant)
            .count();

        let (status, resp) = h
            .send_message(
                &conv.id,
                &SendMessageRequest {
                    content: (*prompt).to_string(),
                    thinking_level: Some(*level),
                    ..Default::default()
                },
            )
            .await;
        assert_eq!(status, StatusCode::CREATED);
        let task = resp.task.expect("task must be returned");
        assert_eq!(
            task.assigned_agent_id, "executor",
            "case {idx} ({prompt}) must assign directly to executor"
        );
        assert_eq!(task.status, TaskStatus::Completed);

        let all_msgs = h.list_messages(&conv.id).await;
        let asst_msgs: Vec<&ChatMessage> = all_msgs
            .iter()
            .filter(|m| m.role == ChatRole::Assistant)
            .collect();
        assert_eq!(
            asst_msgs.len(),
            before_count + 1,
            "case {idx} ({prompt}) must produce exactly 1 assistant message (zero commander duplicate)"
        );
        let latest = asst_msgs.last().unwrap();
        assert_eq!(latest.agent_id.as_deref(), Some("executor"));
        assert_eq!(latest.content, *scripted_reply);

        let recorded = h.mock.recorded_requests();
        assert_eq!(
            recorded.len(),
            1,
            "case {idx} ({prompt}, {level:?}) must execute exactly 1 inference call (executor only)"
        );
        let sys = system_prompt_of(&recorded[0].body);
        assert!(sys.contains("Agent Identity: Executor (id: executor"));
        assert!(!sys.contains("Agent Identity: Commander (id: commander"));
        let user_prompt = last_user_prompt_of(&recorded[0].body);
        assert!(
            user_prompt.contains(expected_inner),
            "expected cleaned prompt '{expected_inner}' in forwarded user prompt, got: {user_prompt}"
        );
    }

    h.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_specialist_group_selection_skips_commander_synthesis() {
    let h = TestHarness::start().await;

    // 1. Single specialist selected by Coordinator -> ONLY programmer runs & persists, no Commander synthesis
    let conv_single = h
        .create_group_conversation(
            "Single Specialist Fast Path",
            &["researcher", "programmer", "analyst"],
        )
        .await;

    h.push_scripted_text(
        &json!({ "selected_agents": ["programmer"] }).to_string(),
    );
    h.push_scripted_text("I wrote the fast Fibonacci helper directly for you.");

    let (st_single, resp_single) = h
        .send_message(
            &conv_single.id,
            &SendMessageRequest {
                content: "Implement a fast iterative Fibonacci helper algorithm in Rust cleanly"
                    .to_string(),
                ..Default::default()
            },
        )
        .await;
    assert_eq!(st_single, StatusCode::CREATED);
    let task_single = resp_single.task.expect("single-specialist task must be returned");
    assert_eq!(task_single.status, TaskStatus::Completed);

    let recorded_single = h.mock.recorded_requests();
    assert_eq!(
        recorded_single.len(),
        2,
        "single-specialist selection must make 2 calls (coordinator + programmer), skipping commander synthesis"
    );
    assert!(system_prompt_of(&recorded_single[0].body).contains("routing a group chat turn"));
    assert!(
        system_prompt_of(&recorded_single[1].body)
            .contains("Agent Identity: Programmer (id: programmer")
    );

    let msgs_single = h.list_messages(&conv_single.id).await;
    let asst_single: Vec<&ChatMessage> = msgs_single
        .iter()
        .filter(|m| m.role == ChatRole::Assistant)
        .collect();
    assert_eq!(
        asst_single.len(),
        1,
        "only programmer must persist an assistant message on single-specialist selection"
    );
    assert_eq!(asst_single[0].agent_id.as_deref(), Some("programmer"));
    assert!(
        !msgs_single
            .iter()
            .any(|m| m.agent_id.as_deref() == Some("commander")),
        "commander must NOT persist a synthesis message for single-specialist selection"
    );

    // 2. Multi-specialist selection -> parallel specialists + Commander synthesis still executes cleanly
    h.mock.clear_recorded();
    let conv_multi = h
        .create_group_conversation(
            "Multi Specialist Synthesis",
            &["researcher", "programmer", "analyst"],
        )
        .await;

    h.push_scripted_text(
        &json!({ "selected_agents": ["researcher", "programmer"] }).to_string(),
    );
    h.push_scripted_text("I audited the cache architecture bottlenecks.");
    h.push_scripted_text("I implemented the cache invalidation refactor.");
    h.push_scripted_text(
        "I synthesized Researcher's audit and Programmer's refactor into a unified rollout plan.",
    );

    let (st_multi, resp_multi) = h
        .send_message(
            &conv_multi.id,
            &SendMessageRequest {
                content: "Audit our cache architecture bottlenecks and implement the refactor"
                    .to_string(),
                ..Default::default()
            },
        )
        .await;
    assert_eq!(st_multi, StatusCode::CREATED);
    let task_multi = resp_multi.task.expect("multi-specialist task must be returned");
    assert_eq!(task_multi.assigned_agent_id, "commander");
    assert_eq!(task_multi.status, TaskStatus::Completed);

    let recorded_multi = h.mock.recorded_requests();
    assert_eq!(
        recorded_multi.len(),
        4,
        "multi-specialist selection must run coordinator + 2 parallel specialists + commander synthesis"
    );

    let msgs_multi = h.list_messages(&conv_multi.id).await;
    let asst_multi: Vec<&ChatMessage> = msgs_multi
        .iter()
        .filter(|m| m.role == ChatRole::Assistant)
        .collect();
    assert_eq!(asst_multi.len(), 3);
    assert!(asst_multi
        .iter()
        .any(|m| m.agent_id.as_deref() == Some("researcher")));
    assert!(asst_multi
        .iter()
        .any(|m| m.agent_id.as_deref() == Some("programmer")));
    assert_eq!(asst_multi.last().unwrap().agent_id.as_deref(), Some("commander"));

    h.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outbound_ps5lm_requests_never_send_consecutive_assistants_or_users() {
    let (h, proxy) = TestHarness::start_with_validating_proxy().await;
    assert!(
        h.system.config.read().unwrap().inference.prefill_kv_cache,
        "kv prefill must be enabled to exercise prefill normalization"
    );

    let conv = h
        .create_group_conversation(
            "Strict Alternation Proxy Validation",
            &["researcher", "programmer", "analyst"],
        )
        .await;

    // Turn 1: Group multi-agent turn -> leaves 3 consecutive assistant messages in DB
    // (researcher, programmer, commander) plus fires background KV cache prefill
    h.push_scripted_text(
        &json!({ "selected_agents": ["researcher", "programmer"] }).to_string(),
    );
    h.push_scripted_text("I collected the system diagnostics cleanly.");
    h.push_scripted_text("I checked workspace config files.");
    h.push_scripted_text("I combined diagnostics and config findings cleanly.");

    let (st1, resp1) = h
        .send_message(
            &conv.id,
            &SendMessageRequest {
                content: "Collect system diagnostics and verify workspace config files".to_string(),
                ..Default::default()
            },
        )
        .await;
    assert_eq!(st1, StatusCode::CREATED);
    assert_eq!(resp1.task.as_ref().unwrap().status, TaskStatus::Completed);

    // Wait briefly for Turn 1 background KV cache prefill to hit the validating proxy
    let prefill_deadline = Instant::now() + Duration::from_secs(3);
    while proxy.validated_prefill_count() == 0 && Instant::now() < prefill_deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Turn 2: Direct multi-turn follow-up on the same conversation containing 3 consecutive assistant rows
    h.push_scripted_text("I summarized those three prior assistant entries into one reply.");
    let (st2, resp2) = h
        .send_message(
            &conv.id,
            &SendMessageRequest {
                content: "@programmer Summarize the previous diagnostics in one sentence"
                    .to_string(),
                ..Default::default()
            },
        )
        .await;
    assert_eq!(st2, StatusCode::CREATED);
    assert_eq!(resp2.task.as_ref().unwrap().status, TaskStatus::Completed);

    // Delete Turn 2 assistant message so DB ends with a user turn -> Turn 3 forces consecutive user rows
    let msgs_turn2 = h.list_messages(&conv.id).await;
    let last_asst = msgs_turn2
        .iter()
        .rev()
        .find(|m| m.role == ChatRole::Assistant)
        .expect("Turn 2 assistant message must exist");
    let del_resp = h
        .client
        .delete(format!(
            "{}/api/v1/conversations/{}/messages/{}",
            h.base_url, conv.id, last_asst.id
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(del_resp.status(), StatusCode::OK);

    // Turn 3: Follow-up user turn immediately after preceding user row -> coalesces consecutive user turns
    h.push_scripted_text("I processed both consecutive user requests cleanly.");
    let (st3, resp3) = h
        .send_message(
            &conv.id,
            &SendMessageRequest {
                content: "@researcher Confirm that consecutive user turns normalized cleanly"
                    .to_string(),
                ..Default::default()
            },
        )
        .await;
    assert_eq!(st3, StatusCode::CREATED);
    assert_eq!(resp3.task.as_ref().unwrap().status, TaskStatus::Completed);

    // Turn 4: Multi-step tool-call chain (assistant(tool_calls) -> tool -> assistant)
    h.mock.push_scripted(ScriptedResponse {
        content: String::new(),
        reasoning: None,
        tool_calls: vec![ScriptedToolCall {
            id: "call_proxy_check_1".to_string(),
            name: "write_file".to_string(),
            arguments: json!({
                "path": "proxy-check.txt",
                "content": "normalized-ok\n",
            }),
        }],
        finish_reason: Some("tool_calls".to_string()),
    });
    h.push_scripted_text("I wrote proxy-check.txt cleanly.");

    let (st4, resp4) = h
        .send_message(
            &conv.id,
            &SendMessageRequest {
                content: "@programmer Write proxy-check.txt with normalized-ok".to_string(),
                ..Default::default()
            },
        )
        .await;
    assert_eq!(st4, StatusCode::CREATED);
    assert_eq!(resp4.task.as_ref().unwrap().status, TaskStatus::Completed);
    assert!(h.workspace_dir().join("proxy-check.txt").exists());

    // Ensure at least one KV prefill request was validated by the proxy as well
    let prefill_deadline2 = Instant::now() + Duration::from_secs(3);
    while proxy.validated_prefill_count() == 0 && Instant::now() < prefill_deadline2 {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert_eq!(
        proxy.rejected_400_count(),
        0,
        "validating proxy rejected outbound /v1/chat/completions payloads: {:?}",
        proxy.rejections_snapshot()
    );
    assert_eq!(
        proxy.upstream_404_count(),
        0,
        "proxy hit unexpected 404 on upstream mock-ps5lm routes: {:?}",
        proxy.rejections_snapshot()
    );
    assert!(
        proxy.validated_chat_count() >= 8,
        "expected >= 8 validated /v1/chat/completions chat calls, got {}",
        proxy.validated_chat_count()
    );
    assert!(
        proxy.validated_prefill_count() >= 1,
        "expected >= 1 validated KV cache prefill request through proxy, got {}",
        proxy.validated_prefill_count()
    );

    drop(proxy);
    h.shutdown().await;
}

fn stream_event_task_id(ev: &StreamEvent) -> Option<&str> {
    match ev {
        StreamEvent::ChatToken { task_id, .. }
        | StreamEvent::ChatReasoning { task_id, .. }
        | StreamEvent::ChatCompleted { task_id, .. }
        | StreamEvent::ToolCallStarted { task_id, .. }
        | StreamEvent::ToolCallFinished { task_id, .. }
        | StreamEvent::TaskUpdated { task_id, .. }
        | StreamEvent::ContinuationTriggered { task_id, .. } => Some(task_id.as_str()),
        StreamEvent::ApprovalRequested { task_id, .. } => task_id.as_deref(),
        _ => None,
    }
}

async fn collect_events_until_task_completed(
    rx: &mut tokio::sync::broadcast::Receiver<StreamEvent>,
    target_task_id: &str,
    timeout: Duration,
) -> Vec<StreamEvent> {
    let mut captured = Vec::new();
    let deadline = Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out waiting for StreamEvent::TaskUpdated(COMPLETED) for '{target_task_id}'; captured={captured:?}"
        );
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Ok(ev)) => {
                let is_completed_target = matches!(
                    &ev,
                    StreamEvent::TaskUpdated { task_id, status, .. }
                        if task_id == target_task_id && status == "COMPLETED"
                );
                if stream_event_task_id(&ev) == Some(target_task_id) {
                    captured.push(ev);
                }
                if is_completed_target {
                    tokio::task::yield_now().await;
                    while let Ok(extra) = rx.try_recv() {
                        if stream_event_task_id(&extra) == Some(target_task_id) {
                            captured.push(extra);
                        }
                    }
                    break;
                }
            }
            Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(n))) => {
                panic!("events broadcast receiver lagged by {n} messages");
            }
            Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => {
                panic!("events broadcast channel closed unexpectedly");
            }
            Err(_) => {
                panic!(
                    "timed out waiting for StreamEvent::TaskUpdated(COMPLETED) for '{target_task_id}'; captured={captured:?}"
                );
            }
        }
    }
    captured
}

fn assert_chat_completed_before_task_completed(
    events: &[StreamEvent],
    expected_conv_id: &str,
    expected_task_id: &str,
    expected_agent_id: &str,
) {
    let chat_completed_indices: Vec<usize> = events
        .iter()
        .enumerate()
        .filter_map(|(idx, ev)| match ev {
            StreamEvent::ChatCompleted {
                conversation_id,
                task_id,
                agent_id,
                ..
            } if task_id == expected_task_id => {
                assert_eq!(conversation_id, expected_conv_id);
                assert_eq!(
                    agent_id, expected_agent_id,
                    "unexpected ChatCompleted agent '{agent_id}' for task '{expected_task_id}'"
                );
                Some(idx)
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        chat_completed_indices.len(),
        1,
        "expected exactly 1 ChatCompleted for task '{expected_task_id}' ({expected_agent_id}), events={events:?}"
    );

    let task_completed_idx = events
        .iter()
        .position(|ev| {
            matches!(
                ev,
                StreamEvent::TaskUpdated {
                    conversation_id,
                    task_id,
                    status,
                    ..
                } if task_id == expected_task_id
                    && status == "COMPLETED"
                    && conversation_id.as_deref() == Some(expected_conv_id)
            )
        })
        .expect("TaskUpdated(COMPLETED) must be present in captured events");

    assert!(
        chat_completed_indices[0] < task_completed_idx,
        "ChatCompleted (idx {}) must be emitted BEFORE TaskUpdated(COMPLETED) (idx {}) for task '{expected_task_id}'; events={events:?}",
        chat_completed_indices[0],
        task_completed_idx
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn group_thinking_off_fast_completion_emits_chat_completed_before_task_completed_with_no_lingering_active_tasks() {
    let h = TestHarness::start_with_thinking_off().await;
    let conv = h
        .create_group_conversation(
            "Step 2 Fast Completion Event Ordering",
            &["commander", "executor", "programmer", "researcher", "analyst"],
        )
        .await;

    let mut events_rx = h.system.runtime.events();

    // Turn 1: "dzien dobry" with ThinkingLevel::Off via dispatch_chat_background_with_options
    h.push_scripted_text("Dzien dobry! W czym moge Ci pomoc?");
    let (_u1, root_task1) = h
        .system
        .runtime
        .dispatch_chat_background_with_options(
            conv.id.clone(),
            "dzien dobry".to_string(),
            None,
            Some(ThinkingLevel::Off),
            Vec::new(),
            false,
            h.system.shutdown.child_token(),
        )
        .await
        .unwrap();

    let events1 =
        collect_events_until_task_completed(&mut events_rx, &root_task1.id, Duration::from_secs(10))
            .await;
    let done1 = h
        .wait_task_terminal(&root_task1.id, Duration::from_secs(10))
        .await;
    assert_eq!(done1.status, TaskStatus::Completed);
    assert_eq!(done1.assigned_agent_id, "commander");

    assert_chat_completed_before_task_completed(&events1, &conv.id, &root_task1.id, "commander");

    let msgs1 = h.list_messages(&conv.id).await;
    let asst1: Vec<&ChatMessage> = msgs1
        .iter()
        .filter(|m| m.role == ChatRole::Assistant)
        .collect();
    assert_eq!(asst1.len(), 1, "turn 1 must persist exactly 1 assistant reply");
    assert_eq!(asst1[0].agent_id.as_deref(), Some("commander"));
    assert_eq!(asst1[0].task_id.as_deref(), Some(root_task1.id.as_str()));
    assert!(
        !msgs1
            .iter()
            .any(|m| m.agent_id.as_deref() == Some("programmer")),
        "programmer must never reply to 'dzien dobry'"
    );

    // Turn 2: "powiedz executorowi ze ma odpisac czesc" with ThinkingLevel::Off
    h.push_scripted_text("Czesc!");
    let (_u2, root_task2) = h
        .system
        .runtime
        .dispatch_chat_background_with_options(
            conv.id.clone(),
            "powiedz executorowi ze ma odpisac czesc".to_string(),
            None,
            Some(ThinkingLevel::Off),
            Vec::new(),
            false,
            h.system.shutdown.child_token(),
        )
        .await
        .unwrap();

    let events2 =
        collect_events_until_task_completed(&mut events_rx, &root_task2.id, Duration::from_secs(10))
            .await;
    let done2 = h
        .wait_task_terminal(&root_task2.id, Duration::from_secs(10))
        .await;
    assert_eq!(done2.status, TaskStatus::Completed);
    assert_eq!(done2.assigned_agent_id, "executor");

    assert_chat_completed_before_task_completed(&events2, &conv.id, &root_task2.id, "executor");

    let msgs2 = h.list_messages(&conv.id).await;
    let asst2: Vec<&ChatMessage> = msgs2
        .iter()
        .filter(|m| m.role == ChatRole::Assistant)
        .collect();
    assert_eq!(
        asst2.len(),
        2,
        "turn 2 must add exactly 1 executor reply without a duplicate commander synthesis reply"
    );
    assert_eq!(asst2[1].agent_id.as_deref(), Some("executor"));
    assert_eq!(asst2[1].task_id.as_deref(), Some(root_task2.id.as_str()));
    assert_eq!(asst2[1].content, "Czesc!");

    // Confirm zero lingering Pending / Running / Waiting tasks remain in storage
    let tasks = h.system.store.list_tasks(50).unwrap();
    assert!(tasks.len() >= 2);
    let lingering: Vec<&TaskRecord> = tasks
        .iter()
        .filter(|t| matches!(t.status, TaskStatus::Pending | TaskStatus::Running | TaskStatus::Waiting))
        .collect();
    assert!(
        lingering.is_empty(),
        "expected 0 lingering Pending/Running/Waiting tasks, found: {lingering:?}"
    );

    h.shutdown().await;
}
