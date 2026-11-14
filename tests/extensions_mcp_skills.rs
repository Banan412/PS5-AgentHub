use mock_ps5lm::{start_ephemeral, MockPs5LmHandle, ScriptedResponse, ScriptedToolCall};
use ps5_agent::adapters::AppSystem;
use ps5_agent::shutdown::finalize_shutdown;
use ps5_agent::startup::initialize;
use ps5_agent_config::AppConfig;
use ps5_agent_core::{
    AgentDefinition, ChatRole, Conversation, MemoryItem, MemoryScope, TaskStatus,
    ToolExecutionRecord, ToolSpec,
};
use ps5_agent_http::server::bind_reusable_listener;
use ps5_agent_http::{
    serve_with_listener, ExtensionsOverviewResponse, McpServerDto, PluginInfoDto,
    SendMessageResponse, SkillDetailResponse, SkillInfoDto,
};
use reqwest::{Client, Response, StatusCode};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

const TEST_ADMIN_PASSWORD: &str = "ExtTestPass99!";

#[derive(Debug, Clone)]
struct RecordedHttpCall {
    method: String,
    path: String,
    headers: BTreeMap<String, String>,
    body: Value,
}

struct MockExtensionHttpHandle {
    addr: SocketAddr,
    mcp_calls: Arc<Mutex<Vec<RecordedHttpCall>>>,
    hook_calls: Arc<Mutex<Vec<RecordedHttpCall>>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl MockExtensionHttpHandle {
    fn mcp_url(&self) -> String {
        format!("http://{}/mcp", self.addr)
    }

    fn hook_url(&self) -> String {
        format!("http://{}/hook", self.addr)
    }

    fn mcp_calls(&self) -> Vec<RecordedHttpCall> {
        self.mcp_calls.lock().unwrap().clone()
    }

    fn hook_calls(&self) -> Vec<RecordedHttpCall> {
        self.hook_calls.lock().unwrap().clone()
    }

    fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

struct ParsedHttpRequest {
    method: String,
    path: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

fn decode_chunked_body(raw: &[u8]) -> Option<(Vec<u8>, usize)> {
    let mut pos = 0usize;
    let mut out = Vec::new();
    loop {
        let line_end_rel = raw[pos..].windows(2).position(|w| w == b"\r\n")?;
        let size_line = std::str::from_utf8(&raw[pos..pos + line_end_rel]).ok()?;
        let size_hex = size_line.split(';').next().unwrap_or("").trim();
        let chunk_len = usize::from_str_radix(size_hex, 16).ok()?;
        pos += line_end_rel + 2;
        if chunk_len == 0 {
            if raw.len() < pos + 2 {
                return None;
            }
            return Some((out, pos + 2));
        }
        if raw.len() < pos + chunk_len + 2 {
            return None;
        }
        out.extend_from_slice(&raw[pos..pos + chunk_len]);
        pos += chunk_len + 2;
    }
}

async fn read_http_request(
    stream: &mut TcpStream,
    buf: &mut Vec<u8>,
) -> std::io::Result<Option<ParsedHttpRequest>> {
    let mut tmp = [0u8; 4096];
    let header_end = loop {
        if let Some(idx) = find_header_end(buf) {
            break idx;
        }
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            return Ok(None);
        }
        buf.extend_from_slice(&tmp[..n]);
    };

    let header_text = String::from_utf8_lossy(&buf[..header_end]).into_owned();
    let body_start = header_end + 4;
    buf.drain(..body_start);

    let mut lines = header_text.split("\r\n");
    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("GET").to_string();
    let path = parts
        .next()
        .unwrap_or("/")
        .split('?')
        .next()
        .unwrap_or("/")
        .to_string();

    let mut headers = BTreeMap::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }

    let is_chunked = headers
        .get("transfer-encoding")
        .is_some_and(|v| v.to_ascii_lowercase().contains("chunked"));

    let body = if is_chunked {
        loop {
            if let Some((decoded, consumed)) = decode_chunked_body(buf) {
                buf.drain(..consumed);
                break decoded;
            }
            let n = stream.read(&mut tmp).await?;
            if n == 0 {
                return Ok(None);
            }
            buf.extend_from_slice(&tmp[..n]);
        }
    } else {
        let content_len = headers
            .get("content-length")
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(0);
        while buf.len() < content_len {
            let n = stream.read(&mut tmp).await?;
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&tmp[..n]);
        }
        let take = content_len.min(buf.len());
        buf.drain(..take).collect()
    };

    Ok(Some(ParsedHttpRequest {
        method,
        path,
        headers,
        body,
    }))
}

async fn write_http_response(
    stream: &mut TcpStream,
    status_line: &str,
    extra_headers: &[(&str, &str)],
    body: &[u8],
) -> std::io::Result<()> {
    let mut head = format!(
        "HTTP/1.1 {status_line}\r\nContent-Length: {}\r\nConnection: keep-alive\r\n",
        body.len()
    );
    for (k, v) in extra_headers {
        head.push_str(&format!("{k}: {v}\r\n"));
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).await?;
    if !body.is_empty() {
        stream.write_all(body).await?;
    }
    stream.flush().await
}

async fn handle_mock_connection(
    mut stream: TcpStream,
    mcp_calls: Arc<Mutex<Vec<RecordedHttpCall>>>,
    hook_calls: Arc<Mutex<Vec<RecordedHttpCall>>>,
) {
    let mut buf = Vec::new();
    while let Ok(Some(req)) = read_http_request(&mut stream, &mut buf).await {
        let body_json: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);
        let recorded = RecordedHttpCall {
            method: req.method.clone(),
            path: req.path.clone(),
            headers: req.headers.clone(),
            body: body_json.clone(),
        };

        if req.path == "/mcp" && req.method == "POST" {
            mcp_calls.lock().unwrap().push(recorded);
            let rpc_method = body_json
                .get("method")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let rpc_id = body_json.get("id").cloned().unwrap_or(Value::Null);

            let (status, extra_headers, resp_body) = match rpc_method {
                "initialize" => {
                    let payload = json!({
                        "jsonrpc": "2.0",
                        "id": rpc_id,
                        "result": {
                            "protocolVersion": "2025-03-26",
                            "capabilities": { "tools": {} },
                            "serverInfo": { "name": "mock-mcp-server", "version": "1.0.0" }
                        }
                    });
                    (
                        "200 OK",
                        vec![
                            ("Content-Type", "application/json"),
                            ("Mcp-Session-Id", "mock-mcp-session-42"),
                        ],
                        serde_json::to_vec(&payload).unwrap(),
                    )
                }
                "notifications/initialized" => (
                    "202 Accepted",
                    vec![("Mcp-Session-Id", "mock-mcp-session-42")],
                    Vec::new(),
                ),
                "tools/list" => {
                    let payload = json!({
                        "jsonrpc": "2.0",
                        "id": rpc_id,
                        "result": {
                            "tools": [
                                {
                                    "name": "echo_ping",
                                    "description": "Echo a message back from the mock MCP server",
                                    "inputSchema": {
                                        "type": "object",
                                        "properties": {
                                            "message": { "type": "string" }
                                        },
                                        "required": ["message"]
                                    },
                                    "annotations": { "readOnlyHint": true }
                                },
                                {
                                    "name": "add_numbers",
                                    "description": "Add two numbers together",
                                    "inputSchema": {
                                        "type": "object",
                                        "properties": {
                                            "a": { "type": "number" },
                                            "b": { "type": "number" }
                                        },
                                        "required": ["a", "b"]
                                    },
                                    "annotations": { "readOnlyHint": true }
                                }
                            ]
                        }
                    });
                    (
                        "200 OK",
                        vec![
                            ("Content-Type", "application/json"),
                            ("Mcp-Session-Id", "mock-mcp-session-42"),
                        ],
                        serde_json::to_vec(&payload).unwrap(),
                    )
                }
                "tools/call" => {
                    let params = body_json.get("params").cloned().unwrap_or_else(|| json!({}));
                    let tool_name = params.get("name").and_then(|v| v.as_str()).unwrap_or("");
                    let args = params
                        .get("arguments")
                        .cloned()
                        .unwrap_or_else(|| json!({}));
                    let payload = match tool_name {
                        "echo_ping" => {
                            let msg = args
                                .get("message")
                                .and_then(|v| v.as_str())
                                .unwrap_or("empty");
                            json!({
                                "jsonrpc": "2.0",
                                "id": rpc_id,
                                "result": {
                                    "content": [{ "type": "text", "text": format!("pong: {msg}") }],
                                    "isError": false
                                }
                            })
                        }
                        "add_numbers" => {
                            let a = args.get("a").and_then(|v| v.as_f64()).unwrap_or(0.0);
                            let b = args.get("b").and_then(|v| v.as_f64()).unwrap_or(0.0);
                            let sum = a + b;
                            json!({
                                "jsonrpc": "2.0",
                                "id": rpc_id,
                                "result": {
                                    "content": [{ "type": "text", "text": format!("{{\"sum\":{sum}}}") }],
                                    "structuredContent": { "sum": sum },
                                    "isError": false
                                }
                            })
                        }
                        other => json!({
                            "jsonrpc": "2.0",
                            "id": rpc_id,
                            "error": { "code": -32601, "message": format!("unknown tool '{other}'") }
                        }),
                    };
                    (
                        "200 OK",
                        vec![
                            ("Content-Type", "application/json"),
                            ("Mcp-Session-Id", "mock-mcp-session-42"),
                        ],
                        serde_json::to_vec(&payload).unwrap(),
                    )
                }
                other => {
                    let payload = json!({
                        "jsonrpc": "2.0",
                        "id": rpc_id,
                        "error": { "code": -32601, "message": format!("unknown method '{other}'") }
                    });
                    (
                        "200 OK",
                        vec![("Content-Type", "application/json")],
                        serde_json::to_vec(&payload).unwrap(),
                    )
                }
            };
            if write_http_response(&mut stream, status, &extra_headers, &resp_body)
                .await
                .is_err()
            {
                break;
            }
        } else if req.path == "/hook" {
            hook_calls.lock().unwrap().push(recorded);
            let payload = json!({ "ok": true, "echo": body_json });
            let bytes = serde_json::to_vec(&payload).unwrap();
            if write_http_response(
                &mut stream,
                "200 OK",
                &[("Content-Type", "application/json")],
                &bytes,
            )
            .await
            .is_err()
            {
                break;
            }
        } else {
            let bytes = b"not found";
            if write_http_response(
                &mut stream,
                "404 Not Found",
                &[("Content-Type", "text/plain")],
                bytes,
            )
            .await
            .is_err()
            {
                break;
            }
        }
    }
}

async fn start_mock_extension_server() -> MockExtensionHttpHandle {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mcp_calls = Arc::new(Mutex::new(Vec::new()));
    let hook_calls = Arc::new(Mutex::new(Vec::new()));
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

    let mcp_calls_bg = mcp_calls.clone();
    let hook_calls_bg = hook_calls.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => break,
                accepted = listener.accept() => {
                    let Ok((stream, _peer)) = accepted else {
                        break;
                    };
                    let mcp = mcp_calls_bg.clone();
                    let hook = hook_calls_bg.clone();
                    tokio::spawn(handle_mock_connection(stream, mcp, hook));
                }
            }
        }
    });

    MockExtensionHttpHandle {
        addr,
        mcp_calls,
        hook_calls,
        shutdown_tx: Some(shutdown_tx),
    }
}

struct TestApp {
    temp: TempDir,
    mock_lm: MockPs5LmHandle,
    system: Arc<AppSystem>,
    base_url: String,
    client: Client,
    cookie: String,
    csrf_token: String,
    server_task: JoinHandle<std::io::Result<()>>,
}

impl TestApp {
    async fn start(customize_cfg: impl FnOnce(&mut AppConfig)) -> Self {
        Self::start_with_temp(TempDir::new().unwrap(), customize_cfg).await
    }

    async fn start_with_temp(
        temp: TempDir,
        customize_cfg: impl FnOnce(&mut AppConfig),
    ) -> Self {
        let mock_lm = start_ephemeral().await.unwrap();

        let mut cfg = AppConfig::default();
        cfg.data_dir = temp.path().to_path_buf();
        cfg.database.driver = "sqlite".to_string();
        cfg.database.path = temp.path().join("database").join("agent.db");
        cfg.logging.log_dir = temp.path().join("logs");
        cfg.logging.enable_klog = false;
        cfg.logging.enable_toast = false;
        cfg.tools.allowed_paths = vec![temp.path().join("workspace").to_string_lossy().into_owned()];
        cfg.inference.base_url = mock_lm.base_url();
        cfg.inference.auto_sync_from_ps5lm = false;
        cfg.runtime.health_poll_interval_secs = 60;
        cfg.security.admin_password = Some(TEST_ADMIN_PASSWORD.to_string());
        customize_cfg(&mut cfg);

        let system = initialize(cfg).await.unwrap();

        let listener = bind_reusable_listener("127.0.0.1:0").await.unwrap();
        let local_addr = listener.local_addr().unwrap();
        let base_url = format!("http://{local_addr}");
        let state = system.http_state.clone();
        let bind_str = local_addr.to_string();
        let server_task =
            tokio::spawn(async move { serve_with_listener(bind_str, listener, state).await });

        let client = Client::builder()
            .pool_max_idle_per_host(0)
            .build()
            .unwrap();

        let login_resp = client
            .post(format!("{base_url}/api/v1/auth/login"))
            .json(&json!({
                "username": "admin",
                "password": TEST_ADMIN_PASSWORD,
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(login_resp.status(), StatusCode::OK);

        let set_cookie = login_resp
            .headers()
            .get(reqwest::header::SET_COOKIE)
            .expect("login must set session cookie")
            .to_str()
            .unwrap()
            .to_string();
        let cookie = set_cookie.split(';').next().unwrap().trim().to_string();
        let login_json: Value = login_resp.json().await.unwrap();
        let csrf_token = login_json["csrf_token"]
            .as_str()
            .expect("login must return csrf_token")
            .to_string();

        Self {
            temp,
            mock_lm,
            system,
            base_url,
            client,
            cookie,
            csrf_token,
            server_task,
        }
    }

    async fn get(&self, path: &str) -> Response {
        self.client
            .get(format!("{}{path}", self.base_url))
            .header("cookie", &self.cookie)
            .send()
            .await
            .unwrap()
    }

    async fn post_json(&self, path: &str, body: &Value) -> Response {
        self.client
            .post(format!("{}{path}", self.base_url))
            .header("cookie", &self.cookie)
            .header("x-csrf-token", &self.csrf_token)
            .json(body)
            .send()
            .await
            .unwrap()
    }

    async fn patch_json(&self, path: &str, body: &Value) -> Response {
        self.client
            .patch(format!("{}{path}", self.base_url))
            .header("cookie", &self.cookie)
            .header("x-csrf-token", &self.csrf_token)
            .json(body)
            .send()
            .await
            .unwrap()
    }

    async fn delete(&self, path: &str) -> Response {
        self.client
            .delete(format!("{}{path}", self.base_url))
            .header("cookie", &self.cookie)
            .header("x-csrf-token", &self.csrf_token)
            .send()
            .await
            .unwrap()
    }

    async fn create_agent(&self, name: &str, system_prompt: &str, enabled_tools: &[&str]) -> AgentDefinition {
        let resp = self
            .post_json(
                "/api/v1/agents",
                &json!({
                    "name": name,
                    "description": format!("Test agent {name}"),
                    "system_prompt": system_prompt,
                    "enabled_tools": enabled_tools,
                }),
            )
            .await;
        assert_eq!(resp.status(), StatusCode::CREATED);
        resp.json().await.unwrap()
    }

    async fn create_direct_conversation(&self, title: &str, agent_id: &str) -> Conversation {
        let resp = self
            .post_json(
                "/api/v1/conversations",
                &json!({
                    "title": title,
                    "mode": { "kind": "direct", "agent_id": agent_id },
                }),
            )
            .await;
        assert_eq!(resp.status(), StatusCode::CREATED);
        resp.json().await.unwrap()
    }

    async fn shutdown_into_temp(self) -> TempDir {
        drop(self.client);
        self.system.shutdown.cancel();
        let _ = self.server_task.await;
        finalize_shutdown(&self.system);
        drop(self.system);
        self.mock_lm.shutdown();
        self.temp
    }

    async fn shutdown(self) {
        drop(self.shutdown_into_temp().await);
    }
}

fn chat_completion_system_prompt(req_body: &Value) -> String {
    req_body["messages"]
        .as_array()
        .and_then(|msgs| {
            msgs.iter()
                .find(|m| m.get("role").and_then(|r| r.as_str()) == Some("system"))
        })
        .and_then(|m| m.get("content").and_then(|c| c.as_str()))
        .unwrap_or("")
        .to_string()
}

fn chat_completion_tool_names(req_body: &Value) -> Vec<String> {
    req_body["tools"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|t| {
                    t.get("function")
                        .and_then(|f| f.get("name"))
                        .or_else(|| t.get("name"))
                        .and_then(|n| n.as_str())
                        .map(str::to_string)
                })
                .collect()
        })
        .unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn skills_http_import_toggle_prompt_injection_load_skill_and_delete() {
    let app = TestApp::start(|_| {}).await;

    let skill_md = concat!(
        "---\n",
        "name: ps5-elf-check\n",
        "description: Inspect PS5 ELF headers and entrypoints\n",
        "---\n\n",
        "Always inspect PT_LOAD segments and verify entrypoint alignment.\n"
    );

    // 1. Import SKILL.md via HTTP
    let import_resp = app
        .post_json(
            "/api/v1/extensions/skills/import",
            &json!({
                "slug": "ps5-elf-check",
                "markdown": skill_md,
            }),
        )
        .await;
    assert_eq!(import_resp.status(), StatusCode::CREATED);
    let imported: SkillInfoDto = import_resp.json().await.unwrap();
    assert_eq!(imported.id, "ps5-elf-check");
    assert_eq!(imported.name, "ps5-elf-check");
    assert!(imported.enabled);
    assert_eq!(imported.source, "user");

    // 2. Inspect via GET /api/v1/extensions and GET /api/v1/extensions/skills/{id}
    let overview_resp = app.get("/api/v1/extensions").await;
    assert_eq!(overview_resp.status(), StatusCode::OK);
    let overview: ExtensionsOverviewResponse = overview_resp.json().await.unwrap();
    assert!(overview
        .skills
        .iter()
        .any(|s| s.id == "ps5-elf-check" && s.enabled));

    let detail_resp = app.get("/api/v1/extensions/skills/ps5-elf-check").await;
    assert_eq!(detail_resp.status(), StatusCode::OK);
    let detail: SkillDetailResponse = detail_resp.json().await.unwrap();
    assert_eq!(detail.id, "ps5-elf-check");
    assert!(detail
        .instructions
        .contains("Always inspect PT_LOAD segments"));
    assert_eq!(detail.raw_markdown, skill_md);

    // 3. Create agent with load_skill enabled and run a chat turn while skill is enabled
    let agent = app
        .create_agent(
            "skill-tester",
            "You test skills.",
            &["load_skill", "datetime"],
        )
        .await;
    let conv = app
        .create_direct_conversation("Skill Test Chat", &agent.id)
        .await;

    app.mock_lm.clear_recorded();
    app.mock_lm.push_scripted(ScriptedResponse {
        content: String::new(),
        reasoning: None,
        tool_calls: vec![ScriptedToolCall {
            id: "call_load_skill_1".to_string(),
            name: "load_skill".to_string(),
            arguments: json!({ "skill": "ps5-elf-check" }),
        }],
        finish_reason: Some("tool_calls".to_string()),
    });
    app.mock_lm.push_scripted(ScriptedResponse {
        content: "Loaded ps5-elf-check skill successfully.".to_string(),
        reasoning: None,
        tool_calls: Vec::new(),
        finish_reason: Some("stop".to_string()),
    });

    let chat_resp = app
        .post_json(
            &format!("/api/v1/conversations/{}/messages", conv.id),
            &json!({
                "content": "Check my PS5 ELF using the skill.",
                "async_dispatch": false,
            }),
        )
        .await;
    assert_eq!(chat_resp.status(), StatusCode::CREATED);
    let chat_body: SendMessageResponse = chat_resp.json().await.unwrap();
    assert_eq!(
        chat_body.task.as_ref().map(|t| t.status),
        Some(TaskStatus::Completed)
    );

    let recorded_enabled = app.mock_lm.recorded_requests();
    let first_chat_req = recorded_enabled
        .iter()
        .find(|r| r.path == "/v1/chat/completions")
        .expect("expected /v1/chat/completions request");
    let sys_prompt_enabled = chat_completion_system_prompt(&first_chat_req.body);
    assert!(
        sys_prompt_enabled.contains("Available Skills")
            && sys_prompt_enabled.contains("ps5-elf-check"),
        "enabled skill must be injected into system prompt, got: {sys_prompt_enabled}"
    );

    let execs = app
        .system
        .store
        .list_tool_executions(Some(&agent.id), 20)
        .unwrap();
    assert!(
        execs.iter().any(|e| {
            e.tool_id == "load_skill"
                && e.success
                && e.output_json
                    .as_deref()
                    .is_some_and(|o| o.contains("Always inspect PT_LOAD segments"))
        }),
        "load_skill must succeed and return SKILL.md instructions: {execs:?}"
    );

    // 4. Disable via PATCH /api/v1/extensions/skill/{id}
    let disable_resp = app
        .patch_json(
            "/api/v1/extensions/skill/ps5-elf-check",
            &json!({ "enabled": false }),
        )
        .await;
    assert_eq!(disable_resp.status(), StatusCode::OK);
    let disable_json: Value = disable_resp.json().await.unwrap();
    assert_eq!(disable_json["enabled"], false);

    let overview_disabled: ExtensionsOverviewResponse =
        app.get("/api/v1/extensions").await.json().await.unwrap();
    assert!(overview_disabled
        .skills
        .iter()
        .any(|s| s.id == "ps5-elf-check" && !s.enabled));

    // Verify disabled skill is NOT injected into prompt context on next chat turn
    app.mock_lm.clear_recorded();
    app.mock_lm.push_scripted(ScriptedResponse {
        content: "No skills injected.".to_string(),
        reasoning: None,
        tool_calls: Vec::new(),
        finish_reason: Some("stop".to_string()),
    });
    let chat_resp_disabled = app
        .post_json(
            &format!("/api/v1/conversations/{}/messages", conv.id),
            &json!({
                "content": "Any skills available now?",
                "async_dispatch": false,
            }),
        )
        .await;
    assert_eq!(chat_resp_disabled.status(), StatusCode::CREATED);

    let recorded_disabled = app.mock_lm.recorded_requests();
    let chat_req_disabled = recorded_disabled
        .iter()
        .find(|r| r.path == "/v1/chat/completions")
        .expect("expected /v1/chat/completions request");
    let sys_prompt_disabled = chat_completion_system_prompt(&chat_req_disabled.body);
    assert!(
        !sys_prompt_disabled.contains("ps5-elf-check")
            && !sys_prompt_disabled.contains("Available Skills"),
        "disabled skill must NOT be injected into system prompt, got: {sys_prompt_disabled}"
    );

    // Verify load_skill rejects executing a disabled skill
    let exec_disabled_resp = app
        .post_json(
            "/api/v1/tools/load_skill/execute",
            &json!({ "arguments": { "skill": "ps5-elf-check" } }),
        )
        .await;
    if exec_disabled_resp.status() == StatusCode::OK {
        let rec: ToolExecutionRecord = exec_disabled_resp.json().await.unwrap();
        assert!(!rec.success, "disabled skill must fail load_skill execution");
        assert!(
            rec.error
                .as_deref()
                .is_some_and(|e| e.contains("disabled")),
            "expected disabled error, got: {:?}",
            rec.error
        );
    } else {
        assert_eq!(exec_disabled_resp.status(), StatusCode::FORBIDDEN);
    }

    // 5. Re-enable and then delete skill via DELETE /api/v1/extensions/skill/{id}
    let enable_resp = app
        .patch_json(
            "/api/v1/extensions/skill/ps5-elf-check",
            &json!({ "enabled": true }),
        )
        .await;
    assert_eq!(enable_resp.status(), StatusCode::OK);

    let delete_resp = app.delete("/api/v1/extensions/skill/ps5-elf-check").await;
    assert_eq!(delete_resp.status(), StatusCode::OK);
    let delete_json: Value = delete_resp.json().await.unwrap();
    assert_eq!(delete_json["deleted"], true);

    let after_del: ExtensionsOverviewResponse =
        app.get("/api/v1/extensions").await.json().await.unwrap();
    assert!(
        !after_del.skills.iter().any(|s| s.id == "ps5-elf-check"),
        "deleted skill must not appear in GET /api/v1/extensions"
    );
    let detail_after_del = app.get("/api/v1/extensions/skills/ps5-elf-check").await;
    assert_eq!(detail_after_del.status(), StatusCode::NOT_FOUND);

    app.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mcp_http_ssrf_guard_sync_and_runtime_wildcard_tool_execution() {
    let mock_ext = start_mock_extension_server().await;
    let app = TestApp::start(|cfg| {
        cfg.tools.block_private_lan_web_requests = true;
    })
    .await;

    // 1. Import MCP server pointing at loopback with trustLocal = false -> sync must be SSRF-blocked
    let blocked_payload = json!({
        "mcpServers": {
            "local-blocked": {
                "url": mock_ext.mcp_url(),
                "trustLocal": false
            }
        }
    });
    let import_blocked = app
        .post_json(
            "/api/v1/extensions/mcp",
            &json!({ "raw_json": blocked_payload.to_string() }),
        )
        .await;
    assert_eq!(import_blocked.status(), StatusCode::CREATED);
    let blocked_dtos: Vec<McpServerDto> = import_blocked.json().await.unwrap();
    assert_eq!(blocked_dtos.len(), 1);
    assert_eq!(blocked_dtos[0].id, "local-blocked");
    assert!(!blocked_dtos[0].trust_local);

    let sync_blocked = app
        .post_json("/api/v1/extensions/mcp/local-blocked/sync", &json!({}))
        .await;
    assert_eq!(
        sync_blocked.status(),
        StatusCode::FORBIDDEN,
        "loopback MCP sync with trust_local=false must be blocked by SSRF guard"
    );
    assert!(
        mock_ext.mcp_calls().is_empty(),
        "SSRF guard must block before making any HTTP connection to loopback"
    );

    let overview_after_block: ExtensionsOverviewResponse =
        app.get("/api/v1/extensions").await.json().await.unwrap();
    let blocked_view = overview_after_block
        .mcp_servers
        .iter()
        .find(|s| s.id == "local-blocked")
        .expect("local-blocked server present");
    assert_eq!(blocked_view.status, "error");
    assert!(blocked_view.tools.is_empty());

    // 2. Import Claude/Cursor mcpServers JSON with trustLocal = true + secret headers
    let trusted_payload = json!({
        "mcpServers": {
            "local-echo": {
                "serverUrl": mock_ext.mcp_url(),
                "apiKey": "mcp-secret-api-key-987",
                "headers": {
                    "Authorization": "Bearer mcp-secret-bearer-123",
                    "X-Trace-Id": "suite-1"
                },
                "trustLocal": true
            }
        }
    });
    let import_trusted = app
        .post_json(
            "/api/v1/extensions/mcp",
            &json!({ "raw_json": trusted_payload.to_string() }),
        )
        .await;
    assert_eq!(import_trusted.status(), StatusCode::CREATED);
    let trusted_dtos: Vec<McpServerDto> = import_trusted.json().await.unwrap();
    assert_eq!(trusted_dtos.len(), 1);
    let srv = &trusted_dtos[0];
    assert_eq!(srv.id, "local-echo");
    assert!(srv.trust_local);
    assert!(srv.has_auth);
    assert_eq!(
        srv.masked_headers.get("Authorization").map(String::as_str),
        Some("***REDACTED***")
    );
    assert_eq!(
        srv.masked_headers.get("X-Trace-Id").map(String::as_str),
        Some("suite-1")
    );

    // 3. Sync trusted MCP server -> discovers dynamic tools mcp:local-echo:echo_ping & add_numbers
    let sync_ok = app
        .post_json("/api/v1/extensions/mcp/local-echo/sync", &json!({}))
        .await;
    assert_eq!(sync_ok.status(), StatusCode::OK);
    let synced: McpServerDto = sync_ok.json().await.unwrap();
    assert_eq!(synced.status, "connected");
    assert!(synced
        .tools
        .iter()
        .any(|t| t.namespaced_id == "mcp:local-echo:echo_ping"));
    assert!(synced
        .tools
        .iter()
        .any(|t| t.namespaced_id == "mcp:local-echo:add_numbers"));

    // 4. Create agent with "mcp:*" wildcard and execute MCP tool during a runtime chat turn
    let agent = app
        .create_agent("mcp-runner", "You run MCP tools.", &["mcp:*"])
        .await;
    let conv = app
        .create_direct_conversation("MCP Wildcard Chat", &agent.id)
        .await;

    app.mock_lm.clear_recorded();
    app.mock_lm.push_scripted(ScriptedResponse {
        content: String::new(),
        reasoning: None,
        tool_calls: vec![ScriptedToolCall {
            id: "call_mcp_1".to_string(),
            name: "mcp:local-echo:echo_ping".to_string(),
            arguments: json!({ "message": "hello-ps5" }),
        }],
        finish_reason: Some("tool_calls".to_string()),
    });
    app.mock_lm.push_scripted(ScriptedResponse {
        content: "MCP echo_ping replied pong: hello-ps5.".to_string(),
        reasoning: None,
        tool_calls: Vec::new(),
        finish_reason: Some("stop".to_string()),
    });

    let chat_resp = app
        .post_json(
            &format!("/api/v1/conversations/{}/messages", conv.id),
            &json!({
                "content": "Ping the MCP server with hello-ps5.",
                "async_dispatch": false,
            }),
        )
        .await;
    assert_eq!(chat_resp.status(), StatusCode::CREATED);
    let chat_body: SendMessageResponse = chat_resp.json().await.unwrap();
    assert_eq!(
        chat_body.task.as_ref().map(|t| t.status),
        Some(TaskStatus::Completed)
    );

    // Verify dynamic MCP tools were exposed to the LLM via "mcp:*" wildcard
    let lm_reqs = app.mock_lm.recorded_requests();
    let chat_req = lm_reqs
        .iter()
        .find(|r| r.path == "/v1/chat/completions")
        .expect("expected /v1/chat/completions request");
    let advertised_tools = chat_completion_tool_names(&chat_req.body);
    assert!(
        advertised_tools.iter().any(|n| n == "mcp:local-echo:echo_ping")
            && advertised_tools
                .iter()
                .any(|n| n == "mcp:local-echo:add_numbers"),
        "expected mcp:* wildcard to expose discovered MCP tools, got: {advertised_tools:?}"
    );

    // Verify mock MCP server received initialize -> notifications/initialized -> tools/list -> tools/call
    let mcp_calls = mock_ext.mcp_calls();
    assert!(mcp_calls.iter().all(|c| c.path == "/mcp"));
    assert!(mcp_calls.iter().any(|c| {
        c.path == "/mcp"
            && c.body.get("method").and_then(|m| m.as_str()) == Some("initialize")
            && c.headers.get("authorization").map(String::as_str)
                == Some("Bearer mcp-secret-bearer-123")
    }));
    assert!(mcp_calls
        .iter()
        .any(|c| c.body.get("method").and_then(|m| m.as_str()) == Some("notifications/initialized")));
    assert!(mcp_calls
        .iter()
        .any(|c| c.body.get("method").and_then(|m| m.as_str()) == Some("tools/list")));
    assert!(
        mcp_calls.iter().any(|c| {
            c.path == "/mcp"
                && c.body.get("method").and_then(|m| m.as_str()) == Some("tools/call")
                && c.body["params"]["name"] == "echo_ping"
                && c.body["params"]["arguments"]["message"] == "hello-ps5"
                && c.headers.get("mcp-session-id").map(String::as_str)
                    == Some("mock-mcp-session-42")
        }),
        "expected tools/call for echo_ping with session header: {mcp_calls:?}"
    );

    // Verify tool execution record and assistant message in storage
    let execs = app
        .system
        .store
        .list_tool_executions(Some(&agent.id), 20)
        .unwrap();
    assert!(
        execs.iter().any(|e| {
            e.tool_id == "mcp:local-echo:echo_ping"
                && e.success
                && e.output_json
                    .as_deref()
                    .is_some_and(|o| o.contains("pong: hello-ps5"))
        }),
        "expected successful mcp:local-echo:echo_ping execution: {execs:?}"
    );

    let msgs = app
        .system
        .store
        .list_chat_messages(&conv.id, false, 20)
        .unwrap();
    assert!(msgs.iter().any(|m| {
        m.role == ChatRole::Assistant && m.content.contains("pong: hello-ps5")
    }));

    app.shutdown().await;
    mock_ext.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plugin_import_rules_bundled_skill_custom_tool_wildcard_and_secret_masking() {
    let mock_ext = start_mock_extension_server().await;
    let app = TestApp::start(|cfg| {
        // Allow local HTTP plugin tool test to hit the mock loopback webhook.
        cfg.tools.allowed_domains = vec!["127.0.0.1".to_string()];
        cfg.tools.block_private_lan_web_requests = false;
    })
    .await;

    // Bundle a SKILL.md inside <data_dir>/plugins/ps5-helper/skills/elf-tips/SKILL.md
    let bundled_skill_dir = app
        .temp
        .path()
        .join("plugins")
        .join("ps5-helper")
        .join("skills")
        .join("elf-tips");
    fs::create_dir_all(&bundled_skill_dir).unwrap();
    fs::write(
        bundled_skill_dir.join("SKILL.md"),
        "---\nname: elf-tips\ndescription: Bundled ELF guidance\n---\n\nVerify PT_LOAD flags before deployment.\n",
    )
    .unwrap();

    // Import plugin.json with rules, mcpServers (secret headers), template tool, and HTTP tool (secret headers)
    let plugin_manifest = json!({
        "name": "ps5-helper",
        "display_name": "PS5 Helper",
        "description": "Helper rules, bundled skill, and custom tools",
        "version": "1.2.0",
        "rules": [
            "Always note PS5 FW 13.40 compatibility."
        ],
        "mcpServers": {
            "plugin-mcp": {
                "url": mock_ext.mcp_url(),
                "trustLocal": true,
                "headers": {
                    "Authorization": "Bearer super-secret-plugin-mcp-token",
                    "X-Api-Key": "super-secret-plugin-apikey",
                    "X-Plugin-Mode": "strict"
                }
            }
        },
        "tools": [
            {
                "name": "format_badge",
                "description": "Format a status badge from template",
                "kind": "template",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "env": { "type": "string" },
                        "label": { "type": "string" }
                    },
                    "required": ["env", "label"]
                },
                "body_template": "[{{env}}] {{label}}",
                "is_mutating": false
            },
            {
                "name": "notify_hook",
                "description": "Send JSON payload to webhook",
                "kind": "http",
                "url": mock_ext.hook_url(),
                "method": "POST",
                "headers": {
                    "Authorization": "Bearer super-secret-hook-bearer",
                    "X-Secret-Token": "super-secret-hook-token-999",
                    "Content-Type": "application/json"
                },
                "parameters": {
                    "type": "object",
                    "properties": {
                        "msg": { "type": "string" }
                    },
                    "required": ["msg"]
                },
                "body_template": "{\"msg\":\"{{msg}}\"}",
                "response_template": "status={{status}} ok={{json.ok}}",
                "is_mutating": false
            }
        ]
    });

    let import_resp = app
        .post_json(
            "/api/v1/extensions/plugins/import",
            &json!({ "manifest_json": plugin_manifest.to_string() }),
        )
        .await;
    assert_eq!(import_resp.status(), StatusCode::CREATED);
    let plugin_dto: PluginInfoDto = import_resp.json().await.unwrap();
    assert_eq!(plugin_dto.id, "ps5-helper");
    assert!(plugin_dto.enabled);
    assert_eq!(plugin_dto.rules_count, 1);
    assert_eq!(plugin_dto.skills_count, 1);
    assert!(plugin_dto.has_mcp);
    assert_eq!(
        plugin_dto.tools,
        vec![
            "plugin:ps5-helper:format_badge".to_string(),
            "plugin:ps5-helper:notify_hook".to_string(),
        ]
    );

    // Verify GET /api/v1/extensions masks secret headers and never leaks any secret value
    let overview_resp = app.get("/api/v1/extensions").await;
    assert_eq!(overview_resp.status(), StatusCode::OK);
    let overview_text = overview_resp.text().await.unwrap();
    assert!(!overview_text.contains("super-secret-plugin-mcp-token"));
    assert!(!overview_text.contains("super-secret-plugin-apikey"));
    assert!(!overview_text.contains("super-secret-hook-bearer"));
    assert!(!overview_text.contains("super-secret-hook-token-999"));

    let overview: ExtensionsOverviewResponse = serde_json::from_str(&overview_text).unwrap();
    assert!(overview
        .plugins
        .iter()
        .any(|p| p.id == "ps5-helper" && p.skills_count == 1 && p.rules_count == 1));
    let plugin_mcp = overview
        .mcp_servers
        .iter()
        .find(|m| m.id == "plugin-mcp")
        .expect("plugin mcp server registered in GET /api/v1/extensions");
    assert!(plugin_mcp.has_auth);
    assert_eq!(
        plugin_mcp
            .masked_headers
            .get("Authorization")
            .map(String::as_str),
        Some("***REDACTED***")
    );
    assert_eq!(
        plugin_mcp
            .masked_headers
            .get("X-Api-Key")
            .map(String::as_str),
        Some("***REDACTED***")
    );
    assert_eq!(
        plugin_mcp
            .masked_headers
            .get("X-Plugin-Mode")
            .map(String::as_str),
        Some("strict")
    );

    // Also verify PluginManager tool views mask secret HTTP tool headers
    let tool_views = app
        .system
        .tools
        .plugins()
        .plugin_tool_views("ps5-helper")
        .unwrap();
    let hook_view = tool_views
        .iter()
        .find(|t| t.name == "notify_hook")
        .expect("notify_hook tool view");
    assert_eq!(
        hook_view.headers.get("Authorization").map(String::as_str),
        Some("***")
    );
    assert_eq!(
        hook_view.headers.get("X-Secret-Token").map(String::as_str),
        Some("***")
    );
    assert_eq!(
        hook_view.headers.get("Content-Type").map(String::as_str),
        Some("application/json")
    );

    // Create agent with "plugin:*" wildcard + load_skill and execute both custom tools + bundled skill
    let agent = app
        .create_agent(
            "plugin-runner",
            "You execute plugin tools.",
            &["plugin:*", "load_skill"],
        )
        .await;
    let conv = app
        .create_direct_conversation("Plugin Wildcard Chat", &agent.id)
        .await;

    app.mock_lm.clear_recorded();
    app.mock_lm.push_scripted(ScriptedResponse {
        content: String::new(),
        reasoning: None,
        tool_calls: vec![
            ScriptedToolCall {
                id: "call_plugin_tmpl".to_string(),
                name: "plugin:ps5-helper:format_badge".to_string(),
                arguments: json!({ "env": "ps5", "label": "ready" }),
            },
            ScriptedToolCall {
                id: "call_plugin_http".to_string(),
                name: "plugin:ps5-helper:notify_hook".to_string(),
                arguments: json!({ "msg": "deployed" }),
            },
            ScriptedToolCall {
                id: "call_plugin_skill".to_string(),
                name: "load_skill".to_string(),
                arguments: json!({ "skill": "elf-tips" }),
            },
        ],
        finish_reason: Some("tool_calls".to_string()),
    });
    app.mock_lm.push_scripted(ScriptedResponse {
        content: "Executed template tool, HTTP tool, and bundled skill.".to_string(),
        reasoning: None,
        tool_calls: Vec::new(),
        finish_reason: Some("stop".to_string()),
    });

    let chat_resp = app
        .post_json(
            &format!("/api/v1/conversations/{}/messages", conv.id),
            &json!({
                "content": "Format badge, notify hook, and check bundled skill.",
                "async_dispatch": false,
            }),
        )
        .await;
    assert_eq!(chat_resp.status(), StatusCode::CREATED);
    let chat_body: SendMessageResponse = chat_resp.json().await.unwrap();
    assert_eq!(
        chat_body.task.as_ref().map(|t| t.status),
        Some(TaskStatus::Completed)
    );

    // Verify plugin rules + bundled skill injected into system prompt, and plugin:* tools advertised
    let lm_reqs = app.mock_lm.recorded_requests();
    let chat_req = lm_reqs
        .iter()
        .find(|r| r.path == "/v1/chat/completions")
        .expect("expected /v1/chat/completions request");
    let sys_prompt = chat_completion_system_prompt(&chat_req.body);
    assert!(
        sys_prompt.contains("Active Plugin Guidelines")
            && sys_prompt.contains("Always note PS5 FW 13.40 compatibility."),
        "expected plugin rules in system prompt, got: {sys_prompt}"
    );
    assert!(
        sys_prompt.contains("Available Skills") && sys_prompt.contains("elf-tips"),
        "expected bundled plugin skill in system prompt, got: {sys_prompt}"
    );

    let advertised = chat_completion_tool_names(&chat_req.body);
    assert!(advertised
        .iter()
        .any(|n| n == "plugin:ps5-helper:format_badge"));
    assert!(advertised
        .iter()
        .any(|n| n == "plugin:ps5-helper:notify_hook"));

    // Verify HTTP plugin tool actually called the mock webhook with its secret headers
    let hook_calls = mock_ext.hook_calls();
    assert_eq!(hook_calls.len(), 1);
    assert_eq!(hook_calls[0].method, "POST");
    assert_eq!(hook_calls[0].path, "/hook");
    assert_eq!(
        hook_calls[0].headers.get("authorization").map(String::as_str),
        Some("Bearer super-secret-hook-bearer")
    );
    assert_eq!(
        hook_calls[0]
            .headers
            .get("x-secret-token")
            .map(String::as_str),
        Some("super-secret-hook-token-999")
    );
    assert_eq!(hook_calls[0].body["msg"], "deployed");

    // Verify tool execution records for template tool, HTTP tool, and bundled skill
    let execs = app
        .system
        .store
        .list_tool_executions(Some(&agent.id), 20)
        .unwrap();
    assert!(
        execs.iter().any(|e| {
            e.tool_id == "plugin:ps5-helper:format_badge"
                && e.success
                && e.output_json
                    .as_deref()
                    .is_some_and(|o| o.contains("[ps5] ready"))
        }),
        "expected template tool execution: {execs:?}"
    );
    assert!(
        execs.iter().any(|e| {
            e.tool_id == "plugin:ps5-helper:notify_hook"
                && e.success
                && e.output_json
                    .as_deref()
                    .is_some_and(|o| o.contains("status=200 ok=true"))
        }),
        "expected http plugin tool execution: {execs:?}"
    );
    assert!(
        execs.iter().any(|e| {
            e.tool_id == "load_skill"
                && e.success
                && e.output_json
                    .as_deref()
                    .is_some_and(|o| o.contains("Verify PT_LOAD flags before deployment."))
        }),
        "expected bundled skill load_skill execution: {execs:?}"
    );

    app.shutdown().await;
    mock_ext.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn startup_creates_extension_and_tool_dirs_and_mcp_file_drop_reload_delete() {
    let mock_ext = start_mock_extension_server().await;
    let app = TestApp::start(|_| {}).await;

    // (a) Verify <data_dir>/{mcp,skills,plugins,tools} folders are created automatically on startup
    for subdir in ["mcp", "skills", "plugins", "tools"] {
        assert!(
            app.temp.path().join(subdir).is_dir(),
            "expected startup to create <data_dir>/{subdir}"
        );
    }

    // (b.1) Import an MCP server via HTTP -> verify <data_dir>/mcp/<id>.json is written
    let import_payload = json!({
        "mcpServers": {
            "http-imported": {
                "url": mock_ext.mcp_url(),
                "trustLocal": true,
                "headers": { "X-Origin": "http-import" }
            }
        }
    });
    let import_resp = app
        .post_json(
            "/api/v1/extensions/mcp",
            &json!({ "raw_json": import_payload.to_string() }),
        )
        .await;
    assert_eq!(import_resp.status(), StatusCode::CREATED);
    let imported: Vec<McpServerDto> = import_resp.json().await.unwrap();
    assert_eq!(imported.len(), 1);
    assert_eq!(imported[0].id, "http-imported");

    let imported_file = app.temp.path().join("mcp").join("http-imported.json");
    assert!(
        imported_file.is_file(),
        "expected <data_dir>/mcp/http-imported.json to be created on import"
    );
    let persisted_doc: Value =
        serde_json::from_str(&fs::read_to_string(&imported_file).unwrap()).unwrap();
    assert_eq!(persisted_doc["id"], "http-imported");
    assert_eq!(persisted_doc["url"], mock_ext.mcp_url());
    assert_eq!(persisted_doc["trust_local"], true);

    // (b.2) External file drop into <data_dir>/mcp/<id>.json + live reload via discover_all()
    let live_drop_file = app.temp.path().join("mcp").join("dropped-live.json");
    fs::write(
        &live_drop_file,
        serde_json::to_vec_pretty(&json!({
            "url": mock_ext.mcp_url(),
            "trust_local": true,
            "headers": { "X-Origin": "live-drop" }
        }))
        .unwrap(),
    )
    .unwrap();

    let discovered = app.system.tools.mcp().discover_all().unwrap();
    assert!(
        discovered.iter().any(|s| s.id == "http-imported")
            && discovered.iter().any(|s| s.id == "dropped-live"),
        "expected discover_all() to find both http-imported and dropped-live, got: {discovered:?}"
    );

    let overview_live: ExtensionsOverviewResponse =
        app.get("/api/v1/extensions").await.json().await.unwrap();
    assert!(
        overview_live
            .mcp_servers
            .iter()
            .any(|s| s.id == "dropped-live"),
        "expected dropped-live in GET /api/v1/extensions after reload"
    );

    let sync_dropped = app
        .post_json("/api/v1/extensions/mcp/dropped-live/sync", &json!({}))
        .await;
    assert_eq!(sync_dropped.status(), StatusCode::OK);
    let synced_dto: McpServerDto = sync_dropped.json().await.unwrap();
    assert_eq!(synced_dto.id, "dropped-live");
    assert_eq!(synced_dto.status, "connected");
    assert!(synced_dto
        .tools
        .iter()
        .any(|t| t.namespaced_id == "mcp:dropped-live:echo_ping"));

    // (b.3) External file drop + cold restart reload on the same data_dir
    let restart_drop_file = app.temp.path().join("mcp").join("dropped-restart.json");
    fs::write(
        &restart_drop_file,
        serde_json::to_vec_pretty(&json!({
            "mcpServers": {
                "dropped-restart": {
                    "url": mock_ext.mcp_url(),
                    "trustLocal": true
                }
            }
        }))
        .unwrap(),
    )
    .unwrap();

    let temp = app.shutdown_into_temp().await;
    let app = TestApp::start_with_temp(temp, |_| {}).await;

    let overview_restarted: ExtensionsOverviewResponse =
        app.get("/api/v1/extensions").await.json().await.unwrap();
    for expected_id in ["http-imported", "dropped-live", "dropped-restart"] {
        assert!(
            overview_restarted
                .mcp_servers
                .iter()
                .any(|s| s.id == expected_id),
            "expected '{expected_id}' after restart reload, got: {:?}",
            overview_restarted.mcp_servers
        );
    }

    // (b.4) DELETE /api/v1/extensions/mcp/{id} removes both registry entry and <data_dir>/mcp/<id>.json
    for id in ["http-imported", "dropped-live", "dropped-restart"] {
        let del_resp = app.delete(&format!("/api/v1/extensions/mcp/{id}")).await;
        assert_eq!(del_resp.status(), StatusCode::OK);
        let del_json: Value = del_resp.json().await.unwrap();
        assert_eq!(del_json["deleted"], true);
        assert!(
            !app.temp.path().join("mcp").join(format!("{id}.json")).exists(),
            "expected <data_dir>/mcp/{id}.json to be deleted on DELETE"
        );
    }

    let after_del_discover = app.system.tools.mcp().discover_all().unwrap();
    for id in ["http-imported", "dropped-live", "dropped-restart"] {
        assert!(
            !after_del_discover.iter().any(|s| s.id == id),
            "deleted MCP server '{id}' must not resurrect on discover_all()"
        );
    }

    let overview_after_del: ExtensionsOverviewResponse =
        app.get("/api/v1/extensions").await.json().await.unwrap();
    for id in ["http-imported", "dropped-live", "dropped-restart"] {
        assert!(
            !overview_after_del
                .mcp_servers
                .iter()
                .any(|s| s.id == id),
            "deleted MCP server '{id}' must not appear in GET /api/v1/extensions"
        );
    }

    app.shutdown().await;
    mock_ext.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn custom_tools_import_over_twenty_policy_execute_and_delete() {
    let app = TestApp::start(|_| {}).await;

    // 1. Import 25 (> 20) custom template tools via POST /api/v1/tools
    let batch: Vec<Value> = (0..25usize)
        .map(|i| {
            json!({
                "id": format!("custom_batch_{i:02}"),
                "name": format!("Custom Batch {i:02}"),
                "description": format!("Custom template tool #{i:02}"),
                "kind": "template",
                "template": format!("batch-{i:02}:{{{{msg}}}}"),
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "msg": { "type": "string" }
                    },
                    "required": ["msg"]
                }
            })
        })
        .collect();

    let import_resp = app
        .post_json("/api/v1/tools", &json!({ "tools": batch }))
        .await;
    assert_eq!(import_resp.status(), StatusCode::CREATED);
    let created_specs: Vec<ToolSpec> = import_resp.json().await.unwrap();
    assert_eq!(created_specs.len(), 25);

    for i in 0..25usize {
        let id = format!("custom_batch_{i:02}");
        assert!(
            app.temp.path().join("tools").join(format!("{id}.json")).is_file(),
            "expected <data_dir>/tools/{id}.json to exist on disk"
        );
    }

    // 2. GET /api/v1/tools must return all 25 custom tools (verifying zero 20-tool cap)
    let list_resp = app.get("/api/v1/tools").await;
    assert_eq!(list_resp.status(), StatusCode::OK);
    let listed: Vec<ToolSpec> = list_resp.json().await.unwrap();
    let custom_count = listed
        .iter()
        .filter(|s| s.id.starts_with("custom_batch_"))
        .count();
    assert_eq!(
        custom_count, 25,
        "expected all 25 custom tools in GET /api/v1/tools without 20-tool truncation"
    );
    for i in 0..25usize {
        let expected_id = format!("custom_batch_{i:02}");
        assert!(
            listed.iter().any(|s| s.id == expected_id),
            "missing '{expected_id}' from GET /api/v1/tools"
        );
    }

    // 3. PATCH /api/v1/tools/{id} policy updates + disabled execution check
    let disable_resp = app
        .patch_json(
            "/api/v1/tools/custom_batch_00",
            &json!({
                "enabled": false,
                "requires_approval": true,
                "timeout_secs": 45
            }),
        )
        .await;
    assert_eq!(disable_resp.status(), StatusCode::OK);
    let updated_disabled: ToolSpec = disable_resp.json().await.unwrap();
    assert_eq!(updated_disabled.id, "custom_batch_00");
    assert!(!updated_disabled.enabled);
    assert!(updated_disabled.requires_approval);
    assert_eq!(updated_disabled.timeout_secs, 45);

    let listed_after_patch: Vec<ToolSpec> = app.get("/api/v1/tools").await.json().await.unwrap();
    let patched_in_list = listed_after_patch
        .iter()
        .find(|s| s.id == "custom_batch_00")
        .expect("custom_batch_00 must stay listed after policy update");
    assert!(!patched_in_list.enabled);
    assert!(patched_in_list.requires_approval);
    assert_eq!(patched_in_list.timeout_secs, 45);

    let exec_blocked = app
        .post_json(
            "/api/v1/tools/custom_batch_00/execute",
            &json!({ "arguments": { "msg": "blocked" } }),
        )
        .await;
    assert_eq!(
        exec_blocked.status(),
        StatusCode::FORBIDDEN,
        "disabled custom tool must refuse HTTP execution"
    );

    let reenable_resp = app
        .patch_json(
            "/api/v1/tools/custom_batch_00",
            &json!({
                "enabled": true,
                "requires_approval": false,
                "timeout_secs": 30
            }),
        )
        .await;
    assert_eq!(reenable_resp.status(), StatusCode::OK);
    let reenabled: ToolSpec = reenable_resp.json().await.unwrap();
    assert!(reenabled.enabled);
    assert!(!reenabled.requires_approval);
    assert_eq!(reenabled.timeout_secs, 30);

    // 4. Execute via POST /api/v1/tools/{id}/execute (both index 0 and index 24 > 20)
    let exec_00_resp = app
        .post_json(
            "/api/v1/tools/custom_batch_00/execute",
            &json!({ "arguments": { "msg": "alpha" } }),
        )
        .await;
    assert_eq!(exec_00_resp.status(), StatusCode::OK);
    let rec_00: ToolExecutionRecord = exec_00_resp.json().await.unwrap();
    assert!(rec_00.success);
    assert_eq!(rec_00.tool_id, "custom_batch_00");
    assert!(
        rec_00
            .output_json
            .as_deref()
            .is_some_and(|o| o.contains("batch-00:alpha")),
        "expected rendered template output for custom_batch_00, got: {:?}",
        rec_00.output_json
    );

    let exec_24_resp = app
        .post_json(
            "/api/v1/tools/custom_batch_24/execute",
            &json!({ "arguments": { "msg": "omega" } }),
        )
        .await;
    assert_eq!(exec_24_resp.status(), StatusCode::OK);
    let rec_24: ToolExecutionRecord = exec_24_resp.json().await.unwrap();
    assert!(rec_24.success);
    assert_eq!(rec_24.tool_id, "custom_batch_24");
    assert!(
        rec_24
            .output_json
            .as_deref()
            .is_some_and(|o| o.contains("batch-24:omega")),
        "expected rendered template output for custom_batch_24, got: {:?}",
        rec_24.output_json
    );

    // 5. Delete custom tool via DELETE /api/v1/tools/{id} and protect built-ins
    let del_24_resp = app.delete("/api/v1/tools/custom_batch_24").await;
    assert_eq!(del_24_resp.status(), StatusCode::OK);
    let del_24_json: Value = del_24_resp.json().await.unwrap();
    assert_eq!(del_24_json["deleted"], true);
    assert_eq!(del_24_json["id"], "custom_batch_24");
    assert!(
        !app.temp
            .path()
            .join("tools")
            .join("custom_batch_24.json")
            .exists(),
        "expected <data_dir>/tools/custom_batch_24.json to be removed on DELETE"
    );

    let listed_after_del: Vec<ToolSpec> = app.get("/api/v1/tools").await.json().await.unwrap();
    assert!(
        !listed_after_del.iter().any(|s| s.id == "custom_batch_24"),
        "deleted custom tool must not remain in GET /api/v1/tools"
    );
    assert_eq!(
        listed_after_del
            .iter()
            .filter(|s| s.id.starts_with("custom_batch_"))
            .count(),
        24
    );

    let del_missing = app.delete("/api/v1/tools/custom_batch_24").await;
    assert_eq!(del_missing.status(), StatusCode::NOT_FOUND);

    let del_builtin = app.delete("/api/v1/tools/calculator").await;
    assert_eq!(del_builtin.status(), StatusCode::FORBIDDEN);

    app.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memories_import_list_limit_zero_and_export() {
    let app = TestApp::start(|_| {}).await;

    // 1. Import 25 memory items via POST /api/v1/memories/import
    let memories: Vec<Value> = (0..25usize)
        .map(|i| {
            json!({
                "key": format!("mem-batch-key-{i:02}"),
                "content": format!("Imported batch memory entry #{i:02} for Wave 4B"),
                "summary": format!("Summary #{i:02}"),
                "importance": 7,
                "tags": ["wave4b", "imported-batch"],
            })
        })
        .collect();

    let import_resp = app
        .post_json(
            "/api/v1/memories/import",
            &json!({
                "scope": "project",
                "namespace": "ext-batch",
                "memories": memories,
            }),
        )
        .await;
    assert_eq!(import_resp.status(), StatusCode::OK);
    let import_json: Value = import_resp.json().await.unwrap();
    assert_eq!(import_json["imported"], 25);

    // 2. Verify GET /api/v1/memories?limit=10 truncates while ?limit=0 returns all >= 25 items
    let limited_resp = app
        .get("/api/v1/memories?namespace=ext-batch&limit=10")
        .await;
    assert_eq!(limited_resp.status(), StatusCode::OK);
    let limited_items: Vec<MemoryItem> = limited_resp.json().await.unwrap();
    assert_eq!(limited_items.len(), 10);

    let all_resp = app.get("/api/v1/memories?limit=0").await;
    assert_eq!(all_resp.status(), StatusCode::OK);
    let all_items: Vec<MemoryItem> = all_resp.json().await.unwrap();
    assert!(
        all_items.len() >= 25,
        "expected GET /api/v1/memories?limit=0 to return all >= 25 items, got {}",
        all_items.len()
    );
    for i in 0..25usize {
        let expected_key = format!("mem-batch-key-{i:02}");
        assert!(
            all_items
                .iter()
                .any(|m| m.key == expected_key && m.namespace == "ext-batch"),
            "missing '{expected_key}' from GET /api/v1/memories?limit=0"
        );
    }

    let ns_unlimited_resp = app
        .get("/api/v1/memories?namespace=ext-batch&limit=0")
        .await;
    assert_eq!(ns_unlimited_resp.status(), StatusCode::OK);
    let ns_items: Vec<MemoryItem> = ns_unlimited_resp.json().await.unwrap();
    assert_eq!(ns_items.len(), 25);

    // 3. Verify GET /api/v1/memories/export exports all 25 imported items faithfully
    let export_resp = app.get("/api/v1/memories/export").await;
    assert_eq!(export_resp.status(), StatusCode::OK);
    let exported: Vec<MemoryItem> = export_resp.json().await.unwrap();
    assert!(
        exported.len() >= 25,
        "expected GET /api/v1/memories/export to return >= 25 items, got {}",
        exported.len()
    );
    for i in 0..25usize {
        let expected_key = format!("mem-batch-key-{i:02}");
        let expected_content = format!("Imported batch memory entry #{i:02} for Wave 4B");
        let item = exported
            .iter()
            .find(|m| m.key == expected_key)
            .unwrap_or_else(|| panic!("missing exported memory '{expected_key}'"));
        assert_eq!(item.scope, MemoryScope::Project);
        assert_eq!(item.namespace, "ext-batch");
        assert_eq!(item.content, expected_content);
        assert_eq!(item.importance, 7);
    }

    app.shutdown().await;
}
