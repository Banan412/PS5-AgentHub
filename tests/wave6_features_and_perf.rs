//! Wave 6 end-to-end integration and performance regression suite covering:
//! 1. BrowserActivity mouse coords + sanitized `/api/v1/sandbox/browser/preview` CSP & files preview isolation.
//! 2. `pldmgr` cold-boot stale PID / `meta.json` startup resilience & ownership-checked PID cleanup.
//! 3. Dynamic EN/PL per-turn language detection & `/api/v1/schedules/ai` JSON prompt isolation.
//! 4. Anti-repetition & `"Got it, Commander!"` loop breaker (streaming + non-streaming) + no non-tool `"name"`.
//! 5. Static KV prefix determinism, `id_slot` affinity, neutral penalty omission, `smart_tool_routing`, and `timings` persistence.
//! 6. `POST /api/v1/inference/import-kv` auth/CSRF/SSRF checks, settings/config.toml persistence, and no `api_key` leakage.
//! 7. `AppLogger` downsize ring bounds and `MemoryManager` zero-archived compaction + SQLite `checkpoint()` bounds.

use mock_ps5lm::{
    MockKvSampling, MockPs5LmHandle, MockTimings, RecordedRequest, ScriptedResponse,
    ScriptedToolCall,
};
use ps5_agent::shutdown::cleanup_pid_file;
use ps5_agent::startup::initialize;
use ps5_agent::AppSystem;
use ps5_agent_config::{AppConfig, LoggingConfig, MemoryConfig};
use ps5_agent_core::{
    ChatMessage, ChatRole, Conversation, ConversationMode, StorageBackend, StreamEvent, TaskStatus,
};
use ps5_agent_database::SqliteStorage;
use ps5_agent_http::security::{BROWSER_PREVIEW_CSP_VALUE, PREVIEW_CSP_VALUE};
use ps5_agent_http::{serve_with_listener, HttpState, SendMessageRequest, SendMessageResponse};
use ps5_agent_logging::{AppLogger, LogLevel};
use ps5_agent_memory::MemoryManager;
use serde_json::{json, Value};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

struct Wave6Harness {
    _temp_dir: TempDir,
    data_dir: std::path::PathBuf,
    mock: MockPs5LmHandle,
    system: Arc<AppSystem>,
    base_url: String,
    client: reqwest::Client,
    server_task: JoinHandle<()>,
}

impl Wave6Harness {
    async fn start(configure: impl FnOnce(&mut AppConfig)) -> Self {
        Self::start_with_pre_init(|_| {}, configure).await
    }

    async fn start_with_pre_init(
        pre_init: impl FnOnce(&Path),
        configure: impl FnOnce(&mut AppConfig),
    ) -> Self {
        Self::start_with_mock_setup(|_| {}, pre_init, configure).await
    }

    async fn start_with_mock_setup(
        setup_mock: impl FnOnce(&MockPs5LmHandle),
        pre_init: impl FnOnce(&Path),
        configure: impl FnOnce(&mut AppConfig),
    ) -> Self {
        let temp_dir = TempDir::new().expect("create temp dir");
        let data_dir = temp_dir.path().to_path_buf();
        let mock = mock_ps5lm::start_ephemeral()
            .await
            .expect("start mock-ps5lm");
        setup_mock(&mock);

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
        cfg.sync_sandbox_and_tools();

        cfg.save(&data_dir.join("config.toml"))
            .expect("save initial test config.toml");
        pre_init(&data_dir);

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

        for _ in 0..50 {
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

    async fn login(&self, username: &str, password: &str) -> (String, String) {
        let resp = self
            .client
            .post(self.url("/api/v1/auth/login"))
            .header(reqwest::header::ORIGIN, &self.base_url)
            .json(&json!({ "username": username, "password": password }))
            .send()
            .await
            .expect("POST /api/v1/auth/login");
        assert_eq!(resp.status(), reqwest::StatusCode::OK, "login must succeed");

        let set_cookie = resp
            .headers()
            .get(reqwest::header::SET_COOKIE)
            .expect("Set-Cookie header on login")
            .to_str()
            .expect("valid UTF-8 Set-Cookie")
            .to_string();
        let cookie_pair = set_cookie
            .split(';')
            .next()
            .expect("cookie key=value")
            .to_string();

        let body: Value = resp.json().await.expect("login JSON");
        let csrf = body["csrf_token"]
            .as_str()
            .expect("csrf_token in login response")
            .to_string();
        (cookie_pair, csrf)
    }

    async fn create_direct_conversation(
        &self,
        title: &str,
        agent_id: &str,
        auth: Option<&(String, String)>,
    ) -> Conversation {
        let mut req = self
            .client
            .post(self.url("/api/v1/conversations"))
            .json(&json!({
                "title": title,
                "mode": ConversationMode::Direct {
                    agent_id: agent_id.to_string(),
                },
            }));
        if let Some((cookie, csrf)) = auth {
            req = req
                .header(reqwest::header::ORIGIN, &self.base_url)
                .header(reqwest::header::COOKIE, cookie)
                .header("x-csrf-token", csrf);
        }
        let resp = req.send().await.expect("create conversation");
        assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
        resp.json().await.expect("parse conversation JSON")
    }

    async fn send_message(
        &self,
        conv_id: &str,
        content: &str,
        agent_id: Option<&str>,
        auth: Option<&(String, String)>,
    ) -> SendMessageResponse {
        let payload = SendMessageRequest {
            content: content.to_string(),
            agent_id: agent_id.map(str::to_string),
            ..Default::default()
        };
        let mut req = self
            .client
            .post(self.url(&format!("/api/v1/conversations/{conv_id}/messages")))
            .json(&payload);
        if let Some((cookie, csrf)) = auth {
            req = req
                .header(reqwest::header::ORIGIN, &self.base_url)
                .header(reqwest::header::COOKIE, cookie)
                .header("x-csrf-token", csrf);
        }
        let resp = req.send().await.expect("POST conversation message");
        assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
        resp.json().await.expect("parse SendMessageResponse")
    }

    async fn list_messages(
        &self,
        conv_id: &str,
        auth: Option<&(String, String)>,
    ) -> Vec<ChatMessage> {
        let mut req = self
            .client
            .get(self.url(&format!("/api/v1/conversations/{conv_id}/messages")));
        if let Some((cookie, _)) = auth {
            req = req.header(reqwest::header::COOKIE, cookie);
        }
        let resp = req.send().await.expect("GET conversation messages");
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        resp.json().await.expect("parse ChatMessage vec")
    }

    async fn shutdown(self) {
        self.system.shutdown.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(2), self.server_task).await;
        self.mock.shutdown();
    }
}

struct HtmlFixtureServer {
    base_url: String,
    shutdown: CancellationToken,
    task: JoinHandle<()>,
}

impl HtmlFixtureServer {
    async fn start(index_html: &'static str, next_html: &'static str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fixture HTTP listener");
        let addr = listener.local_addr().expect("fixture addr");
        let base_url = format!("http://{addr}");
        let shutdown = CancellationToken::new();
        let cancel = shutdown.clone();

        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    accept_res = listener.accept() => {
                        let Ok((mut stream, _)) = accept_res else {
                            break;
                        };
                        tokio::spawn(async move {
                            let mut buf = [0u8; 4096];
                            let n = stream.read(&mut buf).await.unwrap_or(0);
                            let req_head = String::from_utf8_lossy(&buf[..n]);
                            let first_line = req_head.lines().next().unwrap_or("");
                            let path = first_line.split_whitespace().nth(1).unwrap_or("/");
                            let body = if path.starts_with("/submitted") {
                                next_html
                            } else {
                                index_html
                            };
                            let resp = format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                                body.len(),
                                body
                            );
                            let _ = stream.write_all(resp.as_bytes()).await;
                        });
                    }
                }
            }
        });

        Self {
            base_url,
            shutdown,
            task,
        }
    }

    async fn shutdown(self) {
        self.shutdown.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(1), self.task).await;
    }
}

fn req_messages(req: &RecordedRequest) -> &[Value] {
    req.body["messages"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

fn req_system_prompt(req: &RecordedRequest) -> &str {
    req_messages(req)
        .iter()
        .find(|m| m["role"].as_str() == Some("system"))
        .and_then(|m| m["content"].as_str())
        .unwrap_or("")
}

fn req_tools(req: &RecordedRequest) -> &[Value] {
    req.body
        .get("tools")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

fn static_system_prefix(system_prompt: &str) -> &str {
    system_prompt
        .split_once("\n\nLanguage Requirement:")
        .map(|(prefix, _)| prefix)
        .unwrap_or(system_prompt)
}

fn collect_events(rx: &mut broadcast::Receiver<StreamEvent>) -> Vec<StreamEvent> {
    let mut out = Vec::new();
    while let Ok(ev) = rx.try_recv() {
        out.push(ev);
    }
    out
}

#[tokio::test]
async fn deliverable_1_browser_activity_mouse_coords_and_csp_preview() {
    let admin_pw = "Wave6BrowserPass!";
    let h = Wave6Harness::start(|cfg| {
        cfg.security.auth_enabled = true;
        cfg.security.admin_password = Some(admin_pw.to_string());
        cfg.security.argon2_memory_kib = 8192;
        cfg.tools.allow_local_network = true;
        cfg.sandbox.allow_local_network = true;
        cfg.tools.block_private_lan_web_requests = false;
    })
    .await;

    let fixture = HtmlFixtureServer::start(
        r#"<!doctype html>
<html>
<head><title>Wave6 Fixture</title><script>evil()</script></head>
<body>
  <form action="/submitted" method="get">
    <button id="go" type="submit" onclick="alert(1)">Launch</button>
    <input id="q" name="q" placeholder="Search">
  </form>
  <a id="badlink" href="javascript:evil()">bad</a>
</body>
</html>"#,
        r#"<!doctype html>
<html>
<head><title>Wave6 Submitted</title></head>
<body><p>Submitted Target Reached</p></body>
</html>"#,
    )
    .await;

    // Unauthenticated preview request MUST fail with 401 before login.
    let unauth_preview = h
        .client
        .get(h.url("/api/v1/sandbox/browser/preview"))
        .send()
        .await
        .expect("unauth browser preview");
    assert_eq!(unauth_preview.status(), reqwest::StatusCode::UNAUTHORIZED);

    let auth = h.login("admin", admin_pw).await;
    let (ref cookie, ref csrf) = auth;
    let mut ev_rx = h.system.events_tx.subscribe();

    // Open fixture URL carrying a sensitive query param to test BrowserActivity URL redaction.
    let open_resp = h
        .client
        .post(h.url("/api/v1/sandbox/browser/action"))
        .header(reqwest::header::ORIGIN, &h.base_url)
        .header(reqwest::header::COOKIE, cookie)
        .header("x-csrf-token", csrf)
        .json(&json!({
            "action": "open",
            "url": format!("{}/?token=topsecret#frag", fixture.base_url),
        }))
        .send()
        .await
        .expect("browser open");
    assert_eq!(open_resp.status(), reqwest::StatusCode::OK);
    let open_json: Value = open_resp.json().await.expect("open JSON");
    let sid = open_json["session_id"]
        .as_str()
        .expect("session_id in open response")
        .to_string();

    let snap_resp = h
        .client
        .post(h.url("/api/v1/sandbox/browser/action"))
        .header(reqwest::header::ORIGIN, &h.base_url)
        .header(reqwest::header::COOKIE, cookie)
        .header("x-csrf-token", csrf)
        .json(&json!({ "action": "snapshot", "session_id": sid }))
        .send()
        .await
        .expect("browser snapshot");
    assert_eq!(snap_resp.status(), reqwest::StatusCode::OK);
    let snap_json: Value = snap_resp.json().await.expect("snapshot JSON");
    let elements = snap_json["elements"]
        .as_array()
        .expect("snapshot elements array");
    assert!(
        elements.iter().any(|e| e["ref"].as_str() == Some("e1")),
        "snapshot must expose ref e1: {snap_json}"
    );
    for elem in elements {
        let x = elem["x_pct"].as_f64().expect("elem x_pct");
        let y = elem["y_pct"].as_f64().expect("elem y_pct");
        assert!(
            x.is_finite() && (0.0..=100.0).contains(&x),
            "bad x_pct: {x}"
        );
        assert!(
            y.is_finite() && (0.0..=100.0).contains(&y),
            "bad y_pct: {y}"
        );
    }

    // Hover & click e1 before fetching preview on the initial/updated session.
    let click_resp = h
        .client
        .post(h.url("/api/v1/sandbox/browser/action"))
        .header(reqwest::header::ORIGIN, &h.base_url)
        .header(reqwest::header::COOKIE, cookie)
        .header("x-csrf-token", csrf)
        .json(&json!({ "action": "click", "session_id": sid, "ref": "e1" }))
        .send()
        .await
        .expect("browser click");
    assert_eq!(click_resp.status(), reqwest::StatusCode::OK);

    // Re-open the fixture root with evil script/onclick/javascript: so preview validates sanitization directly.
    let open_xss_resp = h
        .client
        .post(h.url("/api/v1/sandbox/browser/action"))
        .header(reqwest::header::ORIGIN, &h.base_url)
        .header(reqwest::header::COOKIE, cookie)
        .header("x-csrf-token", csrf)
        .json(&json!({
            "action": "open",
            "url": format!("{}/?token=topsecret", fixture.base_url),
        }))
        .send()
        .await
        .expect("browser open xss fixture");
    assert_eq!(open_xss_resp.status(), reqwest::StatusCode::OK);
    let xss_sid = open_xss_resp.json::<Value>().await.expect("open xss JSON")["session_id"]
        .as_str()
        .expect("xss session_id")
        .to_string();

    // Dispatch a chat message explicitly choosing agent_id: Some("researcher") calling browser hover.
    h.mock.push_scripted(ScriptedResponse {
        tool_calls: vec![ScriptedToolCall {
            id: "call_browser_hover_1".to_string(),
            name: "browser".to_string(),
            arguments: json!({
                "action": "hover",
                "session_id": xss_sid,
                "ref": "e1",
            }),
        }],
        finish_reason: Some("tool_calls".to_string()),
        ..Default::default()
    });
    h.mock.push_scripted(ScriptedResponse {
        content: "Researcher inspected Launch button e1 cleanly.".to_string(),
        finish_reason: Some("stop".to_string()),
        ..Default::default()
    });

    let conv = h
        .create_direct_conversation("Wave6 Browser Test", "commander", Some(&auth))
        .await;
    let send_resp = h
        .send_message(
            &conv.id,
            "Inspect the open browser page button",
            Some("researcher"),
            Some(&auth),
        )
        .await;
    let task = send_resp.task.expect("task created for researcher turn");
    assert_eq!(task.assigned_agent_id, "researcher");
    assert_eq!(task.status, TaskStatus::Completed);

    // Verify BrowserActivity SSE events from both HTTP actions and researcher agent.
    let events = collect_events(&mut ev_rx);
    let browser_events: Vec<_> = events
        .into_iter()
        .filter_map(|ev| match ev {
            StreamEvent::BrowserActivity {
                agent_id,
                session_id,
                action,
                url,
                x_pct,
                y_pct,
                ..
            } => Some((agent_id, session_id, action, url, x_pct, y_pct)),
            _ => None,
        })
        .collect();
    assert!(
        browser_events
            .iter()
            .any(|(_, s, a, _, _, _)| s == &sid && a == "click"),
        "expected click BrowserActivity event for session {sid}"
    );
    assert!(
        browser_events
            .iter()
            .any(|(aid, s, a, _, x, y)| aid.as_deref() == Some("researcher")
                && s == &xss_sid
                && a == "hover"
                && x.is_some()
                && y.is_some()),
        "expected researcher hover BrowserActivity with coords"
    );
    for (_, _, _, url_opt, x_opt, y_opt) in &browser_events {
        if let Some(ref u) = url_opt {
            assert!(
                !u.contains("topsecret") && !u.contains("token=") && !u.contains("#frag"),
                "BrowserActivity URL leaked query/fragment secret: {u}"
            );
        }
        if let Some(x) = *x_opt {
            assert!(
                x.is_finite() && (0.0..=100.0).contains(&x),
                "out of range x_pct: {x}"
            );
        }
        if let Some(y) = *y_opt {
            assert!(
                y.is_finite() && (0.0..=100.0).contains(&y),
                "out of range y_pct: {y}"
            );
        }
    }

    // Authenticated browser preview MUST serve sanitized HTML + strict BROWSER_PREVIEW_CSP_VALUE.
    let preview_resp = h
        .client
        .get(h.url(&format!(
            "/api/v1/sandbox/browser/preview?session_id={xss_sid}"
        )))
        .header(reqwest::header::COOKIE, cookie)
        .send()
        .await
        .expect("GET browser preview");
    assert_eq!(preview_resp.status(), reqwest::StatusCode::OK);
    let csp = preview_resp
        .headers()
        .get(reqwest::header::CONTENT_SECURITY_POLICY)
        .and_then(|v| v.to_str().ok())
        .expect("CSP header on browser preview")
        .to_string();
    assert_eq!(csp, BROWSER_PREVIEW_CSP_VALUE);
    assert!(csp.contains("default-src 'none'"));
    assert!(!csp.contains("script-src"));
    assert_eq!(
        preview_resp
            .headers()
            .get(reqwest::header::X_FRAME_OPTIONS)
            .and_then(|v| v.to_str().ok()),
        Some("SAMEORIGIN")
    );
    let cache_ctrl = preview_resp
        .headers()
        .get(reqwest::header::CACHE_CONTROL)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        cache_ctrl.contains("no-store"),
        "expected no-store Cache-Control, got {cache_ctrl}"
    );
    let html_body = preview_resp.text().await.expect("preview body text");
    assert!(html_body.contains("Launch"), "expected visible button text");
    let lower_body = html_body.to_ascii_lowercase();
    assert!(
        !lower_body.contains("<script"),
        "script tag survived: {html_body}"
    );
    assert!(
        !lower_body.contains("onclick"),
        "onclick survived: {html_body}"
    );
    assert!(
        !lower_body.contains("javascript:"),
        "javascript URI survived: {html_body}"
    );

    // Ensure `/api/v1/files/preview` keeps PREVIEW_CSP_VALUE (`script-src 'unsafe-inline'`) intact.
    let upload_resp = h
        .client
        .post(h.url("/api/v1/files/upload"))
        .header(reqwest::header::ORIGIN, &h.base_url)
        .header(reqwest::header::COOKIE, cookie)
        .header("x-csrf-token", csrf)
        .json(&json!({
            "path": "wave6_check.html",
            "content": "<!doctype html><html><body><h1>Workspace Preview</h1></body></html>",
        }))
        .send()
        .await
        .expect("upload workspace html");
    assert_eq!(upload_resp.status(), reqwest::StatusCode::CREATED);

    let file_preview_resp = h
        .client
        .get(h.url("/api/v1/files/preview?path=wave6_check.html"))
        .header(reqwest::header::COOKIE, cookie)
        .send()
        .await
        .expect("GET workspace file preview");
    assert_eq!(file_preview_resp.status(), reqwest::StatusCode::OK);
    let file_csp = file_preview_resp
        .headers()
        .get(reqwest::header::CONTENT_SECURITY_POLICY)
        .and_then(|v| v.to_str().ok())
        .expect("CSP header on files preview");
    assert_eq!(file_csp, PREVIEW_CSP_VALUE);
    assert!(file_csp.contains("script-src 'unsafe-inline'"));

    fixture.shutdown().await;
    h.shutdown().await;
}

#[tokio::test]
async fn deliverable_2_pldmgr_cold_boot_pid_and_meta_resilience() {
    let h = Wave6Harness::start_with_pre_init(
        |data_dir| {
            std::fs::write(data_dir.join("ps5-agent.pid"), "999999:1\n")
                .expect("seed stale prior-boot ps5-agent.pid");
            std::fs::write(
                data_dir.join("meta.json"),
                serde_json::to_string_pretty(&json!({
                    "schema": 2,
                    "id": "ps5-agent",
                    "filename": "ps5-agent.elf",
                    "version": "v0.0.1",
                    "checksum": "deadbeef",
                    "daemon": true,
                    "autostart": true
                }))
                .expect("serialize meta.json"),
            )
            .expect("seed meta.json");
        },
        |_cfg| {},
    )
    .await;

    let pid_path = h.data_dir.join("ps5-agent.pid");
    let updated_pid = std::fs::read_to_string(&pid_path).expect("read updated ps5-agent.pid");
    let my_pid = std::process::id();
    let parsed_pid = updated_pid
        .trim()
        .split(':')
        .next()
        .and_then(|p| p.parse::<u32>().ok())
        .expect("parse updated pid");
    assert_eq!(
        parsed_pid, my_pid,
        "stale prior-boot PID must be replaced by current PID {my_pid}, got {updated_pid:?}"
    );

    let meta_raw = std::fs::read_to_string(h.data_dir.join("meta.json")).expect("read meta.json");
    let meta: Value = serde_json::from_str(&meta_raw).expect("parse meta.json");
    assert_eq!(meta["schema"], json!(2));
    assert_eq!(meta["daemon"], json!(true));
    assert_eq!(meta["autostart"], json!(true));

    // Ownership check on cleanup_pid_file: foreign PID is left untouched, own PID is removed.
    std::fs::write(&pid_path, "999999:1\n").expect("write foreign pid");
    cleanup_pid_file(&h.data_dir);
    assert!(
        pid_path.exists(),
        "cleanup_pid_file must not remove a foreign PID file"
    );

    std::fs::write(&pid_path, format!("{my_pid}\n")).expect("write own pid");
    cleanup_pid_file(&h.data_dir);
    assert!(
        !pid_path.exists(),
        "cleanup_pid_file must remove PID file owned by current PID"
    );

    h.shutdown().await;
}

#[tokio::test]
async fn deliverable_3_dynamic_language_detection_and_schedule_prompt_isolation() {
    let h = Wave6Harness::start(|cfg| {
        cfg.inference.model = "qwen3.5-mock".to_string();
        cfg.inference.stream_enabled = false;
    })
    .await;

    let conv = h
        .create_direct_conversation("Dynamic EN/PL Switch", "commander", None)
        .await;

    h.mock.push_scripted(ScriptedResponse {
        content: "Czesc! U mnie wszystko dziala dobrze.".to_string(),
        finish_reason: Some("stop".to_string()),
        ..Default::default()
    });
    h.mock.push_scripted(ScriptedResponse {
        content: "KV cache stores attention keys and values across turns.".to_string(),
        finish_reason: Some("stop".to_string()),
        ..Default::default()
    });
    h.mock.push_scripted(ScriptedResponse {
        content: "Prosze bardzo, sprawdzam status systemu.".to_string(),
        finish_reason: Some("stop".to_string()),
        ..Default::default()
    });

    h.send_message(&conv.id, "Cześć, jak się masz?", None, None)
        .await;
    h.send_message(&conv.id, "Hello, explain KV cache briefly", None, None)
        .await;
    h.send_message(&conv.id, "Dziękuję bardzo, sprawdź status", None, None)
        .await;

    // Schedule AI parser request MUST use JSON extraction prompt without chat prose language rules.
    h.mock.push_scripted(ScriptedResponse {
        content: json!({
            "title": "Health Check",
            "prompt": "Check system health",
            "kind": "interval",
            "interval_secs": 600,
            "run_at": Value::Null,
            "daily_time": Value::Null,
            "cron_expr": Value::Null,
            "target_agent_id": "commander",
            "explanation": "Runs every 10 minutes."
        })
        .to_string(),
        finish_reason: Some("stop".to_string()),
        ..Default::default()
    });

    let sched_resp = h
        .client
        .post(h.url("/api/v1/schedules/ai"))
        .json(&json!({
            "prompt": "Every 10 minutes check system health",
            "timezone_offset_mins": 0,
            "auto_create": false
        }))
        .send()
        .await
        .expect("POST /api/v1/schedules/ai");
    assert_eq!(sched_resp.status(), reqwest::StatusCode::OK);
    let sched_json: Value = sched_resp.json().await.expect("parse schedule AI JSON");
    assert_eq!(sched_json["draft"]["source"], json!("llm"));

    let reqs = h.mock.recorded_requests();
    assert_eq!(
        reqs.len(),
        4,
        "expected 3 chat turns + 1 schedule parse request"
    );

    let sys_t1 = req_system_prompt(&reqs[0]);
    let sys_t2 = req_system_prompt(&reqs[1]);
    let sys_t3 = req_system_prompt(&reqs[2]);
    let sys_sched = req_system_prompt(&reqs[3]);

    assert!(
        sys_t1.contains("Language Requirement: Respond strictly in Polish."),
        "Turn 1 must instruct Polish: {sys_t1}"
    );
    assert!(
        sys_t2.contains("Language Requirement: Respond strictly in English."),
        "Turn 2 must switch dynamically to English: {sys_t2}"
    );
    assert!(
        sys_t3.contains("Language Requirement: Respond strictly in Polish."),
        "Turn 3 must switch back to Polish: {sys_t3}"
    );

    assert!(sys_sched.contains("Extract an autonomous schedule"));
    assert!(
        !sys_sched.contains("Language Requirement:"),
        "schedule AI prompt must not contain chat Language Requirement directive: {sys_sched}"
    );
    assert!(
        !sys_sched.contains("Polish or English"),
        "schedule AI prompt must have zero Polish bias: {sys_sched}"
    );
    assert!(!sys_sched.contains("Got it, Commander"));

    h.shutdown().await;
}

async fn assert_loop_breaker_for_mode(stream_enabled: bool) {
    let unit = "Programming is a fascinating process of creating software. Understanding code is key to effectively solving problems. Every digital fragment has its own characteristics and limitations.";
    let looped = format!(
        "Got it, Commander! Doing it again:\n\n{unit}\n\n\
         Got it, Commander! Doing it again:\n\n{unit}\n\n\
         Got it, Commander! Doing it again:\n\n{unit}"
    );

    let h = Wave6Harness::start(|cfg| {
        cfg.inference.stream_enabled = stream_enabled;
    })
    .await;

    h.mock.push_scripted(ScriptedResponse {
        content: looped,
        finish_reason: Some("stop".to_string()),
        ..Default::default()
    });

    let conv = h
        .create_direct_conversation("Loop Breaker Check", "commander", None)
        .await;
    h.send_message(
        &conv.id,
        "Please summarize software craftsmanship cleanly",
        None,
        None,
    )
    .await;

    let messages = h.list_messages(&conv.id, None).await;
    let assistant = messages
        .iter()
        .rfind(|m| m.role == ChatRole::Assistant)
        .expect("persisted assistant message");

    assert_eq!(
        assistant
            .content
            .matches("Programming is a fascinating process")
            .count(),
        1,
        "expected at most/exactly 1 copy in persisted content (stream={stream_enabled}): {:?}",
        assistant.content
    );
    assert!(
        !assistant.content.contains("Got it, Commander!"),
        "Got it, Commander! opener/loop must not persist (stream={stream_enabled}): {:?}",
        assistant.content
    );

    let reqs = h.mock.recorded_requests();
    assert!(!reqs.is_empty());
    for req in &reqs {
        for msg in req_messages(req) {
            if matches!(msg["role"].as_str(), Some("user") | Some("assistant")) {
                assert!(
                    msg.get("name").is_none(),
                    "single-agent user/assistant wire message must omit 'name': {msg}"
                );
            }
        }
    }

    h.shutdown().await;
}

#[tokio::test]
async fn deliverable_4_repetition_and_got_it_commander_loop_breaker() {
    assert_loop_breaker_for_mode(false).await;
    assert_loop_breaker_for_mode(true).await;
}

#[tokio::test]
async fn deliverable_5_kv_prefix_id_slot_smart_tool_routing_and_timings_persistence() {
    let h = Wave6Harness::start_with_mock_setup(
        |mock| {
            mock.state().set_total_slots(4);
            mock.set_mock_timings(MockTimings {
                predicted_per_second: 118.4,
                prompt_per_second: 420.0,
                predicted_ms: 101.4,
                prompt_ms: 38.1,
                predicted_n: 12,
                prompt_n: 16,
                cache_n: Some(256),
            });
        },
        |_| {},
        |cfg| {
            cfg.inference.auto_sync_from_ps5lm = true;
            cfg.inference.id_slot = -1;
            cfg.inference.smart_tool_routing = true;
            cfg.inference.stream_enabled = true;
            cfg.inference.prefill_kv_cache = false;
        },
    )
    .await;

    let mut ev_rx = h.system.events_tx.subscribe();
    let conv = h
        .create_direct_conversation("KV Prefix + Timings + Smart Routing", "commander", None)
        .await;

    h.mock.push_scripted(ScriptedResponse {
        content: "Hallo! How can I help today?".to_string(),
        finish_reason: Some("stop".to_string()),
        ..Default::default()
    });
    h.mock.push_scripted(ScriptedResponse {
        content: "I checked the workspace and example target.".to_string(),
        finish_reason: Some("stop".to_string()),
        ..Default::default()
    });

    h.send_message(&conv.id, "hallo", None, None).await;
    // Sleep across a full second boundary to prove no per-second timestamp busts the KV prefix.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    h.send_message(
        &conv.id,
        "Please read Cargo.toml in the workspace and browse http://example.com",
        None,
        None,
    )
    .await;

    let reqs = h.mock.recorded_requests();
    assert_eq!(reqs.len(), 2, "expected 2 inference requests");
    let req0 = &reqs[0];
    let req1 = &reqs[1];

    let sys0 = req_system_prompt(req0);
    let sys1 = req_system_prompt(req1);
    assert!(
        !sys0.contains("Current UTC time:"),
        "static prefix must not embed Current UTC time"
    );
    assert_eq!(
        static_system_prefix(sys0),
        static_system_prefix(sys1),
        "static system prefix must remain byte-for-byte identical across turns"
    );

    let slot0 = req0.id_slot().expect("turn 1 deterministic id_slot");
    let slot1 = req1.id_slot().expect("turn 2 deterministic id_slot");
    assert!((0..4).contains(&slot0), "slot0 out of range: {slot0}");
    assert_eq!(
        slot0, slot1,
        "id_slot must stay deterministic per conversation"
    );

    // Neutral penalties omitted; Wave-6 non-default DRY & repeat_last_n kept.
    for neutral_key in [
        "presence_penalty",
        "frequency_penalty",
        "typical_p",
        "typ_p",
        "dynatemp_range",
        "xtc_probability",
    ] {
        assert!(
            !req0.has_field(neutral_key),
            "neutral field {neutral_key} should be omitted from wire JSON"
        );
    }
    assert_eq!(req0.field("repeat_last_n"), Some(&json!(256)));
    let dry = req0
        .field("dry_multiplier")
        .and_then(Value::as_f64)
        .expect("dry_multiplier");
    assert!(
        (dry - 0.8).abs() < 1e-4,
        "expected dry_multiplier ~0.8, got {dry}"
    );

    // Smart tool routing: greeting "hallo" sends fewer tools than "read Cargo.toml ... browse ...".
    let tools0 = req_tools(req0);
    let tools1 = req_tools(req1);
    assert!(
        tools0.len() < tools1.len(),
        "expected smart_tool_routing to slim casual greeting ({}) < tool prompt ({})",
        tools0.len(),
        tools1.len()
    );

    // Timings propagated into StreamEvent::ChatCompleted and persisted SQLite ChatMessage.timings.
    let completed_timings: Vec<_> = collect_events(&mut ev_rx)
        .into_iter()
        .filter_map(|ev| match ev {
            StreamEvent::ChatCompleted { timings, .. } => Some(timings),
            _ => None,
        })
        .collect();
    assert_eq!(completed_timings.len(), 2);
    for t_opt in &completed_timings {
        let t = t_opt.as_ref().expect("ChatCompleted must include timings");
        assert!((t.predicted_per_second.unwrap_or_default() - 118.4).abs() < 1e-3);
        assert!((t.prompt_per_second.unwrap_or_default() - 420.0).abs() < 1e-3);
        assert_eq!(t.cache_n, Some(256));
    }

    let persisted = h
        .system
        .store
        .list_chat_messages(&conv.id, false, 20)
        .expect("list persisted messages");
    let assistant_msgs: Vec<_> = persisted
        .iter()
        .filter(|m| m.role == ChatRole::Assistant)
        .collect();
    assert_eq!(assistant_msgs.len(), 2);
    for msg in assistant_msgs {
        let t = msg
            .timings
            .as_ref()
            .expect("SQLite persisted ChatMessage must include timings");
        assert!((t.predicted_per_second.unwrap_or_default() - 118.4).abs() < 1e-3);
        assert!((t.prompt_per_second.unwrap_or_default() - 420.0).abs() < 1e-3);
        assert_eq!(t.cache_n, Some(256));
    }

    h.shutdown().await;
}

#[tokio::test]
async fn deliverable_6_import_kv_auth_csrf_persistence_and_no_secret_leak() {
    let admin_pw = "Wave6KvImportPass!";
    let secret_api_key = "super-secret-token";
    let h = Wave6Harness::start_with_mock_setup(
        |mock| {
            mock.state().set_n_ctx(16384);
            mock.state().set_total_slots(4);
            mock.set_mock_kv_sampling(MockKvSampling {
                cache_type_k: Some("q8_0".to_string()),
                cache_type_v: Some("q8_0".to_string()),
                flash_attn: Some(true),
                n_keep: 128,
                temperature: 0.6,
                repeat_last_n: 256,
                dry_multiplier: 0.8,
                ..Default::default()
            });
        },
        |_| {},
        |cfg| {
            cfg.security.auth_enabled = true;
            cfg.security.admin_password = Some(admin_pw.to_string());
            cfg.security.argon2_memory_kib = 8192;
            cfg.inference.api_key = Some(secret_api_key.to_string());
        },
    )
    .await;

    // 1. Unauthenticated POST /api/v1/inference/import-kv -> 401.
    let unauth = h
        .client
        .post(h.url("/api/v1/inference/import-kv"))
        .header(reqwest::header::ORIGIN, &h.base_url)
        .send()
        .await
        .expect("unauth import-kv");
    assert_eq!(unauth.status(), reqwest::StatusCode::UNAUTHORIZED);

    let (cookie, csrf) = h.login("admin", admin_pw).await;

    // 2. Cookie without X-CSRF-Token -> 403.
    let no_csrf = h
        .client
        .post(h.url("/api/v1/inference/import-kv"))
        .header(reqwest::header::ORIGIN, &h.base_url)
        .header(reqwest::header::COOKIE, &cookie)
        .send()
        .await
        .expect("import-kv missing csrf");
    assert_eq!(no_csrf.status(), reqwest::StatusCode::FORBIDDEN);

    // 3. Valid cookie + CSRF -> 200 and full persistence without secret leakage.
    let ok = h
        .client
        .post(h.url("/api/v1/inference/import-kv"))
        .header(reqwest::header::ORIGIN, &h.base_url)
        .header(reqwest::header::COOKIE, &cookie)
        .header("x-csrf-token", &csrf)
        .send()
        .await
        .expect("authenticated import-kv");
    assert_eq!(ok.status(), reqwest::StatusCode::OK);
    let raw_body = ok.text().await.expect("import-kv response text");
    assert!(
        !raw_body.contains(secret_api_key) && !raw_body.contains("\"api_key\""),
        "import-kv response must never expose api_key or secret token: {raw_body}"
    );
    let body: Value = serde_json::from_str(&raw_body).expect("parse import-kv JSON");
    assert_eq!(body["ok"], json!(true));
    assert_eq!(body["imported"]["cache_type_k"], json!("q8_0"));
    assert_eq!(body["imported"]["cache_type_v"], json!("q8_0"));
    assert_eq!(body["imported"]["flash_attn"], json!(true));
    assert_eq!(body["applied"]["cache_type_k"], json!("q8_0"));
    assert_eq!(body["applied"]["cache_type_v"], json!("q8_0"));
    assert_eq!(body["applied"]["flash_attn"], json!(true));

    // Verify GET /api/v1/settings matches imported KV settings and never leaks api_key.
    let settings_resp = h
        .client
        .get(h.url("/api/v1/settings"))
        .header(reqwest::header::COOKIE, &cookie)
        .send()
        .await
        .expect("GET /api/v1/settings");
    assert_eq!(settings_resp.status(), reqwest::StatusCode::OK);
    let settings_raw = settings_resp.text().await.expect("settings text");
    assert!(
        !settings_raw.contains(secret_api_key),
        "GET /api/v1/settings leaked api_key"
    );
    let settings_json: Value = serde_json::from_str(&settings_raw).expect("parse settings JSON");
    assert_eq!(settings_json["inference"]["cache_type_k"], json!("q8_0"));
    assert_eq!(settings_json["inference"]["cache_type_v"], json!("q8_0"));
    assert_eq!(settings_json["inference"]["flash_attn"], json!(true));
    assert_eq!(settings_json["inference"]["context_limit"], json!(16384));
    assert!(settings_json["inference"].get("api_key").is_none());

    // Verify SQLite settings store persistence.
    let stored_settings = h.system.store.list_settings().expect("list_settings");
    assert_eq!(
        stored_settings
            .iter()
            .find(|(k, _)| k == "inference.cache_type_k")
            .map(|(_, v)| v.as_str()),
        Some("q8_0")
    );
    assert_eq!(
        stored_settings
            .iter()
            .find(|(k, _)| k == "inference.cache_type_v")
            .map(|(_, v)| v.as_str()),
        Some("q8_0")
    );
    assert_eq!(
        stored_settings
            .iter()
            .find(|(k, _)| k == "inference.flash_attn")
            .map(|(_, v)| v.as_str()),
        Some("true")
    );

    // Verify config.toml on disk persists imported KV settings cleanly across reloads.
    let reloaded =
        AppConfig::load_or_create(&h.data_dir.join("config.toml")).expect("reload config.toml");
    assert_eq!(reloaded.inference.cache_type_k, "q8_0");
    assert_eq!(reloaded.inference.cache_type_v, "q8_0");
    assert!(reloaded.inference.flash_attn);
    assert_eq!(reloaded.inference.context_limit, 16384);

    // Verify audit log records `inference.import_kv` with zero secret exposure.
    let audit_logs = h.system.store.list_audit_logs(50).expect("list_audit_logs");
    let kv_entry = audit_logs
        .iter()
        .find(|e| e.action == "inference.import_kv")
        .expect("expected inference.import_kv audit log entry");
    assert!(!kv_entry.details_json.contains(secret_api_key));
    assert!(!kv_entry.details_json.contains("api_key"));

    h.shutdown().await;
}

#[test]
fn deliverable_7_perf_and_memory_leak_bounds() {
    let dir = TempDir::new().expect("temp dir");

    // 1. AppLogger ring downsize from 512 to 64 keeps strictly <= 64 recent entries.
    // (Note: internal `VecDeque::capacity() <= 64` shrink_to_fit is directly verified in `ps5-agent-logging` crate unit tests.)
    let log_cfg = LoggingConfig {
        level: "info".to_string(),
        log_dir: dir.path().join("logs"),
        max_file_bytes: 512 * 1024,
        ring_buffer_capacity: 512,
        enable_klog: false,
        enable_toast: false,
    };
    let logger = AppLogger::init(&log_cfg);
    for i in 0..512 {
        logger.info("perf", format!("entry-{i:04}"));
    }
    assert_eq!(logger.recent(None, 1000).len(), 512);

    logger.apply_config(LogLevel::Info, 512 * 1024, 64, false, false);
    let kept = logger.recent(None, 1000);
    assert_eq!(kept.len(), 64);
    assert_eq!(kept[0].message, "entry-0511");
    assert_eq!(kept[63].message, "entry-0448");

    for i in 512..600 {
        logger.info("perf", format!("entry-{i:04}"));
    }
    assert_eq!(logger.recent(None, 1000).len(), 64);

    // 2. MemoryManager zero-archived compaction + SqliteStorage::checkpoint() stress loop.
    let db_path = dir.path().join("perf.db");
    let mut db_cfg = AppConfig::with_data_dir(dir.path().to_path_buf()).database;
    db_cfg.path = db_path.clone();
    let storage = Arc::new(SqliteStorage::open(&db_path, &db_cfg).expect("open sqlite"));
    storage.run_migrations().expect("run migrations");

    let conv = Conversation {
        id: "conv-perf".to_string(),
        title: "Perf Bound Check".to_string(),
        mode: ConversationMode::Direct {
            agent_id: "commander".to_string(),
        },
        created_at: "2026-11-14T10:00:00+00:00".to_string(),
        updated_at: "2026-11-14T10:00:00+00:00".to_string(),
        archived: false,
        sort_order: 0,
    };
    storage.create_conversation(&conv).expect("create conv");

    for i in 0..6 {
        storage
            .append_chat_message(&ChatMessage {
                id: format!("m-{i}"),
                conversation_id: conv.id.clone(),
                role: if i % 2 == 0 {
                    ChatRole::User
                } else {
                    ChatRole::Assistant
                },
                agent_id: if i % 2 == 1 {
                    Some("commander".to_string())
                } else {
                    None
                },
                content: format!("Turn {i} zero-archived compaction payload"),
                reasoning: None,
                tool_calls_json: None,
                task_id: None,
                token_count: 16,
                latency_ms: Some(20),
                archived: false,
                attachments: Vec::new(),
                timings: None,
                created_at: format!("2026-11-14T10:00:{i:02}+00:00"),
            })
            .expect("append message");
    }

    let mgr = MemoryManager::new(
        storage.clone(),
        MemoryConfig {
            max_memories_per_agent: 64,
            archive_after_messages: 12,
            keep_recent_messages: 6,
            max_context_memories: 4,
            max_memory_context_chars: 1000,
        },
    );

    for _ in 0..20 {
        let ctx = mgr
            .build_conversation_context(&conv.id, "You are Commander.", None, 2048)
            .expect("zero-archived build_conversation_context");
        assert_eq!(ctx.archived_count, 0);
        assert_eq!(ctx.messages.len(), 7);
    }

    for _ in 0..5 {
        storage
            .checkpoint()
            .expect("sqlite checkpoint shrink_memory");
    }
    assert_eq!(storage.integrity_check().expect("integrity check"), "ok");
}
