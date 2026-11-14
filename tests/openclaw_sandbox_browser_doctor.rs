use mock_ps5lm::{start_ephemeral, MockPs5LmHandle, ScriptedResponse, ScriptedToolCall};
use ps5_agent::adapters::AppSystem;
use ps5_agent::shutdown::finalize_shutdown;
use ps5_agent::startup::initialize;
use ps5_agent_config::AppConfig;
use ps5_agent_core::{
    agent_allows_tool_with_policy, expand_tool_group, AgentDefinition, AgentError,
    ApprovalRecord, ApprovalStatus, BrainFileDetail, BrainFileSummary, ChatMessage, Conversation,
    ConversationMode, DoctorFixReport, DoctorReport, DoctorStatus, SandboxApprovalPolicy,
    SandboxMode, SlashCommandResponse, TaskRecord, TaskStatus, WebhookView,
};
use ps5_agent_http::server::bind_reusable_listener;
use ps5_agent_http::{
    serve_with_listener, CreateWebhookResponse, SandboxStateDto, SendMessageRequest,
    SendMessageResponse, UpdateSandboxRequest, WebhookTriggerResponse,
};
use ps5_agent_runtime::brain::{list_brain_files, read_brain_file};
use ps5_agent_tools::ToolExecContext;
use reqwest::{Client, StatusCode};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
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
    cfg.agents.max_agents = 16;
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
        Self::start_with_web_config(false).await
    }

    async fn start_with_web_sandbox() -> Self {
        Self::start_with_web_config(true).await
    }

    async fn start_with_web_config(allow_local_web: bool) -> Self {
        let temp = TempDir::new().unwrap();
        let mock = start_ephemeral().await.unwrap();
        let mut cfg = build_test_config(temp.path(), mock.base_url());
        if allow_local_web {
            cfg.sandbox.network_access = true;
            cfg.sandbox.allow_local_network = true;
            cfg.tools.allowed_domains = vec!["127.0.0.1".to_string()];
            cfg.sync_sandbox_and_tools();
        }
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

    async fn create_direct_conversation(&self, title: &str, agent_id: &str) -> Conversation {
        let mode = ConversationMode::Direct {
            agent_id: agent_id.to_string(),
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
                "timed out waiting for terminal task '{task_id}' (last status={:?}, error={:?})",
                task.status,
                task.error_info
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn get_sandbox(&self) -> SandboxStateDto {
        let resp = self
            .client
            .get(format!("{}/api/v1/sandbox", self.base_url))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        resp.json::<SandboxStateDto>().await.unwrap()
    }

    async fn put_sandbox(&self, req: &UpdateSandboxRequest) -> SandboxStateDto {
        let resp = self
            .client
            .put(format!("{}/api/v1/sandbox", self.base_url))
            .json(req)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        resp.json::<SandboxStateDto>().await.unwrap()
    }

    async fn execute_command(
        &self,
        conversation_id: &str,
        command: &str,
        agent_id: Option<&str>,
    ) -> SlashCommandResponse {
        let resp = self
            .client
            .post(format!(
                "{}/api/v1/conversations/{conversation_id}/command",
                self.base_url
            ))
            .json(&json!({
                "command": command,
                "agent_id": agent_id,
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "slash command '{command}' failed"
        );
        resp.json::<SlashCommandResponse>().await.unwrap()
    }

    fn push_scripted_text(&self, content: &str) {
        self.mock.push_scripted(ScriptedResponse {
            content: content.to_string(),
            reasoning: None,
            tool_calls: Vec::new(),
            finish_reason: Some("stop".to_string()),
        });
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

struct RecordedHttpSubmission {
    method: String,
    path: String,
    body: String,
}

struct LocalFixtureServer {
    base_url: String,
    submissions: Arc<Mutex<Vec<RecordedHttpSubmission>>>,
    task: JoinHandle<()>,
}

impl LocalFixtureServer {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{addr}");
        let submissions = Arc::new(Mutex::new(Vec::new()));
        let submissions_bg = submissions.clone();

        let task = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let submissions_conn = submissions_bg.clone();
                tokio::spawn(async move {
                    if let Some((method, target, body)) = read_http_request(&mut stream).await {
                        let path = target.split('?').next().unwrap_or("/").to_string();
                        if path == "/submit" {
                            submissions_conn.lock().unwrap().push(RecordedHttpSubmission {
                                method: method.clone(),
                                path: target.clone(),
                                body: body.clone(),
                            });
                        }

                        let (status_line, extra_headers, resp_body) = match (method.as_str(), path.as_str()) {
                            ("GET", "/") => (
                                "HTTP/1.1 200 OK",
                                "",
                                "<!doctype html><html><head><title>Local Fixture Home</title></head>\
                                 <body><h1>Local Fixture Home</h1>\
                                 <p>Welcome to local fixture site.</p>\
                                 <form action=\"/submit\" method=\"POST\">\
                                   <input name=\"note\" type=\"text\" value=\"\">\
                                   <button type=\"submit\">Send Note</button>\
                                 </form>\
                                 <a href=\"/about\">About Fixture</a>\
                                 </body></html>"
                                    .to_string(),
                            ),
                            ("POST", "/submit") => (
                                "HTTP/1.1 200 OK",
                                "Set-Cookie: sid=local_cookie_1; Path=/\r\n",
                                format!(
                                    "<!doctype html><html><head><title>Submitted OK</title></head>\
                                     <body><h1>Form Submitted</h1><p>Received body: {body}</p>\
                                     <a href=\"/\">Back Home</a></body></html>"
                                ),
                            ),
                            ("GET", "/submit") => (
                                "HTTP/1.1 200 OK",
                                "Set-Cookie: sid=local_cookie_1; Path=/\r\n",
                                format!(
                                    "<!doctype html><html><head><title>Submitted OK</title></head>\
                                     <body><h1>Form Submitted</h1><p>Query: {target}</p>\
                                     <a href=\"/\">Back Home</a></body></html>"
                                ),
                            ),
                            ("GET", "/about") => (
                                "HTTP/1.1 200 OK",
                                "",
                                "<!doctype html><html><head><title>About Fixture</title></head>\
                                 <body><h1>About Fixture</h1><p>About details inside sandbox browser.</p>\
                                 <a href=\"/\">Home</a></body></html>"
                                    .to_string(),
                            ),
                            ("GET", "/challenge") => (
                                "HTTP/1.1 200 OK",
                                "",
                                "<!doctype html><html><head><title>Just a moment...</title></head>\
                                 <body><div class=\"cf-turnstile\" data-sitekey=\"1x00000000000000000000AA\"></div>\
                                 <p>Checking your browser. Verify you are human to proceed.</p></body></html>"
                                    .to_string(),
                            ),
                            _ => (
                                "HTTP/1.1 404 Not Found",
                                "",
                                "<html><head><title>Not Found</title></head><body>Not Found</body></html>"
                                    .to_string(),
                            ),
                        };

                        let bytes = resp_body.as_bytes();
                        let reply = format!(
                            "{status_line}\r\n\
                             Content-Type: text/html; charset=utf-8\r\n\
                             Content-Length: {}\r\n\
                             Connection: close\r\n\
                             {extra_headers}\r\n",
                            bytes.len()
                        );
                        let _ = stream.write_all(reply.as_bytes()).await;
                        let _ = stream.write_all(bytes).await;
                        let _ = stream.flush().await;
                        let _ = stream.shutdown().await;
                    }
                });
            }
        });

        Self {
            base_url,
            submissions,
            task,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }
}

impl Drop for LocalFixtureServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn read_http_request(stream: &mut TcpStream) -> Option<(String, String, String)> {
    let mut buf = Vec::with_capacity(2048);
    let mut chunk = [0u8; 1024];
    let header_end = loop {
        let n = tokio::time::timeout(Duration::from_secs(3), stream.read(&mut chunk))
            .await
            .ok()?
            .ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos;
        }
        if buf.len() > 64 * 1024 {
            return None;
        }
    };

    let head_str = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let mut lines = head_str.split("\r\n");
    let request_line = lines.next()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?.to_string();
    let target = parts.next()?.to_string();

    let mut content_len = 0usize;
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            if k.trim().eq_ignore_ascii_case("content-length") {
                content_len = v.trim().parse::<usize>().unwrap_or(0);
            }
        }
    }

    let body_start = header_end + 4;
    while buf.len() < body_start + content_len {
        let n = tokio::time::timeout(Duration::from_secs(3), stream.read(&mut chunk))
            .await
            .ok()?
            .ok()?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
    }

    let body_end = (body_start + content_len).min(buf.len());
    let body = String::from_utf8_lossy(&buf[body_start..body_end]).to_string();
    Some((method, target, body))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sandbox_modes_block_and_allow_tools_and_update_live_over_http() {
    let h = TestHarness::start_with_web_sandbox().await;
    let fixture = LocalFixtureServer::start().await;

    let initial = h.get_sandbox().await;
    assert_eq!(initial.mode, "full_sandbox");
    assert_eq!(initial.approval_policy, "full_auto");

    let seed_ctx = ToolExecContext {
        agent_id: "programmer".to_string(),
        sandbox_mode: SandboxMode::WorkspaceWrite,
        approval_policy: SandboxApprovalPolicy::FullAuto,
        ..Default::default()
    };
    h.system
        .tools
        .invoke(
            "write_file",
            json!({ "path": "ro_seed.txt", "content": "seed-line-1\nseed-line-2\n" }),
            &seed_ctx,
        )
        .await
        .unwrap();

    // 1. ReadOnly blocks write_file, edit_file, apply_patch, and sandbox_exec redirection; allows reads.
    let ro_ctx = ToolExecContext {
        agent_id: "programmer".to_string(),
        sandbox_mode: SandboxMode::ReadOnly,
        approval_policy: SandboxApprovalPolicy::FullAuto,
        ..Default::default()
    };
    assert!(matches!(
        h.system
            .tools
            .invoke(
                "write_file",
                json!({ "path": "ro_blocked.txt", "content": "nope" }),
                &ro_ctx,
            )
            .await,
        Err(AgentError::PermissionDenied(_))
    ));
    assert!(matches!(
        h.system
            .tools
            .invoke(
                "edit_file",
                json!({ "path": "ro_seed.txt", "old_text": "seed-line-1", "new_text": "changed" }),
                &ro_ctx,
            )
            .await,
        Err(AgentError::PermissionDenied(_))
    ));
    assert!(matches!(
        h.system
            .tools
            .invoke(
                "apply_patch",
                json!({ "patch": "*** Begin Patch\n*** Add File: blocked.txt\n+x\n*** End Patch" }),
                &ro_ctx,
            )
            .await,
        Err(AgentError::PermissionDenied(_))
    ));
    assert!(matches!(
        h.system
            .tools
            .invoke(
                "sandbox_exec",
                json!({ "command": "echo blocked > ro_shell.txt" }),
                &ro_ctx,
            )
            .await,
        Err(AgentError::PermissionDenied(_))
    ));
    let read_res = h
        .system
        .tools
        .invoke("read_file", json!({ "path": "ro_seed.txt" }), &ro_ctx)
        .await
        .unwrap();
    assert!(read_res["content"].as_str().unwrap().contains("seed-line-1"));
    let ro_echo = h
        .system
        .tools
        .invoke(
            "sandbox_exec",
            json!({ "command": "echo readonly-ok" }),
            &ro_ctx,
        )
        .await
        .unwrap();
    assert_eq!(ro_echo["exit_code"], 0);
    assert!(ro_echo["stdout"].as_str().unwrap().contains("readonly-ok"));

    // 2. WorkspaceWrite allows workspace writes/edits/patches, blocks mutating web POST.
    let ws_ctx = ToolExecContext {
        agent_id: "programmer".to_string(),
        sandbox_mode: SandboxMode::WorkspaceWrite,
        approval_policy: SandboxApprovalPolicy::FullAuto,
        ..Default::default()
    };
    h.system
        .tools
        .invoke(
            "edit_file",
            json!({ "path": "ro_seed.txt", "old_text": "seed-line-1", "new_text": "ws-line-1" }),
            &ws_ctx,
        )
        .await
        .unwrap();
    assert!(fs::read_to_string(h.workspace_dir().join("ro_seed.txt"))
        .unwrap()
        .contains("ws-line-1"));

    // Check mutating web tool blocked in WorkspaceWrite (operator context avoids programmer missing http_post).
    let ws_web_ctx = ToolExecContext {
        agent_id: "__operator__".to_string(),
        is_operator: true,
        sandbox_mode: SandboxMode::WorkspaceWrite,
        approval_policy: SandboxApprovalPolicy::FullAuto,
        ..Default::default()
    };
    assert!(matches!(
        h.system
            .tools
            .invoke(
                "http_post",
                json!({ "url": fixture.url("/submit"), "body": { "k": "v" } }),
                &ws_web_ctx,
            )
            .await,
        Err(AgentError::PermissionDenied(_))
    ));

    // 3. FullSandbox allows virtual sandbox_exec file creation + reading.
    let full_ctx = ToolExecContext {
        agent_id: "programmer".to_string(),
        sandbox_mode: SandboxMode::FullSandbox,
        approval_policy: SandboxApprovalPolicy::FullAuto,
        ..Default::default()
    };
    let exec_res = h
        .system
        .tools
        .invoke(
            "sandbox_exec",
            json!({ "command": "echo full-sandbox-ok > sandbox/tmp/full_check.txt && cat sandbox/tmp/full_check.txt" }),
            &full_ctx,
        )
        .await
        .unwrap();
    assert_eq!(exec_res["engine"], "virtual");
    assert_eq!(exec_res["exit_code"], 0);
    assert!(exec_res["stdout"].as_str().unwrap().contains("full-sandbox-ok"));

    // 4. Live HTTP PUT /api/v1/sandbox + GET /api/v1/sandbox enforce read_only then workspace_write.
    let updated_ro = h
        .put_sandbox(&UpdateSandboxRequest {
            mode: Some("read_only".to_string()),
            ..Default::default()
        })
        .await;
    assert_eq!(updated_ro.mode, "read_only");
    assert_eq!(h.get_sandbox().await.mode, "read_only");

    let denied_http = h
        .client
        .post(format!("{}/api/v1/tools/write_file/execute", h.base_url))
        .json(&json!({
            "agent_id": "programmer",
            "arguments": { "path": "http_blocked.txt", "content": "should-fail" }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(denied_http.status(), StatusCode::FORBIDDEN);
    assert!(!h.workspace_dir().join("http_blocked.txt").exists());

    let updated_ws = h
        .put_sandbox(&UpdateSandboxRequest {
            mode: Some("workspace_write".to_string()),
            ..Default::default()
        })
        .await;
    assert_eq!(updated_ws.mode, "workspace_write");
    assert_eq!(h.get_sandbox().await.mode, "workspace_write");

    let allowed_http = h
        .client
        .post(format!("{}/api/v1/tools/write_file/execute", h.base_url))
        .json(&json!({
            "agent_id": "programmer",
            "arguments": { "path": "http_allowed.txt", "content": "wrote-in-ws-mode" }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(allowed_http.status(), StatusCode::OK);
    assert_eq!(
        fs::read_to_string(h.workspace_dir().join("http_allowed.txt")).unwrap(),
        "wrote-in-ws-mode"
    );

    h.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sandbox_approval_policies_full_auto_on_request_and_never() {
    let h = TestHarness::start().await;

    fs::write(h.workspace_dir().join("auto_del.txt"), "delete-auto").unwrap();
    fs::write(h.workspace_dir().join("req_del.txt"), "delete-on-request").unwrap();
    fs::write(h.workspace_dir().join("never_del.txt"), "never-delete").unwrap();

    // 1. Direct ToolRegistry policy matrix for non-operator programmer.
    let full_auto_ctx = ToolExecContext {
        agent_id: "programmer".to_string(),
        sandbox_mode: SandboxMode::WorkspaceWrite,
        approval_policy: SandboxApprovalPolicy::FullAuto,
        ..Default::default()
    };
    h.system
        .tools
        .invoke("delete_file", json!({ "path": "auto_del.txt" }), &full_auto_ctx)
        .await
        .unwrap();
    assert!(!h.workspace_dir().join("auto_del.txt").exists());

    // OnRequest creates pending approval for delete_file (requires_approval = true).
    let on_request_ctx = ToolExecContext {
        agent_id: "programmer".to_string(),
        sandbox_mode: SandboxMode::WorkspaceWrite,
        approval_policy: SandboxApprovalPolicy::OnRequest,
        ..Default::default()
    };
    let on_req_err = h
        .system
        .tools
        .invoke("delete_file", json!({ "path": "req_del.txt" }), &on_request_ctx)
        .await
        .unwrap_err();
    let direct_approval_id = match on_req_err {
        AgentError::ApprovalRequired { approval_id, .. } => approval_id,
        other => panic!("expected ApprovalRequired under on_request, got {other:?}"),
    };
    assert!(h.workspace_dir().join("req_del.txt").exists());
    let rec = h
        .system
        .store
        .get_approval(&direct_approval_id)
        .unwrap()
        .expect("pending approval must be recorded");
    assert_eq!(rec.status, ApprovalStatus::Pending);

    // Granting bypass allows deletion to proceed.
    let granted_ctx = ToolExecContext {
        approval_granted: true,
        ..on_request_ctx.clone()
    };
    h.system
        .tools
        .invoke("delete_file", json!({ "path": "req_del.txt" }), &granted_ctx)
        .await
        .unwrap();
    assert!(!h.workspace_dir().join("req_del.txt").exists());

    // Resolve the synthetic record so pending queue starts clean for live HTTP chat flow.
    let deny_direct = h
        .client
        .post(format!(
            "{}/api/v1/approvals/{direct_approval_id}/deny",
            h.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(deny_direct.status(), StatusCode::OK);

    // AlwaysAsk gates even is_mutating tools without requires_approval (write_file).
    let always_ask_ctx = ToolExecContext {
        agent_id: "programmer".to_string(),
        sandbox_mode: SandboxMode::WorkspaceWrite,
        approval_policy: SandboxApprovalPolicy::AlwaysAsk,
        ..Default::default()
    };
    let aa_err = h
        .system
        .tools
        .invoke(
            "write_file",
            json!({ "path": "always_ask.txt", "content": "wait" }),
            &always_ask_ctx,
        )
        .await
        .unwrap_err();
    let aa_approval_id = match aa_err {
        AgentError::ApprovalRequired { approval_id, .. } => approval_id,
        other => panic!("expected ApprovalRequired under always_ask, got {other:?}"),
    };
    let _ = h
        .client
        .post(format!("{}/api/v1/approvals/{aa_approval_id}/deny", h.base_url))
        .send()
        .await
        .unwrap();

    // Never blocks requires_approval tools with PermissionDenied and creates no pending approval.
    let never_ctx = ToolExecContext {
        agent_id: "programmer".to_string(),
        sandbox_mode: SandboxMode::WorkspaceWrite,
        approval_policy: SandboxApprovalPolicy::Never,
        ..Default::default()
    };
    let never_err = h
        .system
        .tools
        .invoke("delete_file", json!({ "path": "never_del.txt" }), &never_ctx)
        .await
        .unwrap_err();
    assert!(matches!(never_err, AgentError::PermissionDenied(_)));
    assert!(h.workspace_dir().join("never_del.txt").exists());

    // 2. End-to-end HTTP chat pause + approve + resume under on_request.
    h.put_sandbox(&UpdateSandboxRequest {
        mode: Some("workspace_write".to_string()),
        approval_policy: Some("on_request".to_string()),
        ..Default::default()
    })
    .await;

    fs::write(h.workspace_dir().join("chat_approval.txt"), "remove-me").unwrap();
    let conv = h
        .create_direct_conversation("Approval Flow", "programmer")
        .await;

    h.mock.push_scripted(ScriptedResponse {
        content: String::new(),
        reasoning: None,
        tool_calls: vec![ScriptedToolCall {
            id: "call_del_chat_1".to_string(),
            name: "delete_file".to_string(),
            arguments: json!({ "path": "chat_approval.txt" }),
        }],
        finish_reason: Some("tool_calls".to_string()),
    });
    h.push_scripted_text("Deleted chat_approval.txt after operator approval.");

    let (status, send_resp) = h
        .send_message(
            &conv.id,
            &SendMessageRequest {
                content: "Delete chat_approval.txt".to_string(),
                async_dispatch: true,
                ..Default::default()
            },
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let task_id = send_resp.task.expect("task returned").id;

    let deadline = Instant::now() + Duration::from_secs(10);
    let pending_approval = loop {
        let resp = h
            .client
            .get(format!("{}/api/v1/approvals?pending_only=true", h.base_url))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let list: Vec<ApprovalRecord> = resp.json().await.unwrap();
        if let Some(found) = list
            .into_iter()
            .find(|a| a.task_id.as_deref() == Some(task_id.as_str()))
        {
            break found;
        }
        assert!(Instant::now() < deadline, "timed out waiting for pending approval");
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    assert_eq!(pending_approval.tool_id.as_deref(), Some("delete_file"));
    assert_eq!(h.get_task(&task_id).await.status, TaskStatus::Waiting);
    assert!(h.workspace_dir().join("chat_approval.txt").exists());

    let approve_resp = h
        .client
        .post(format!(
            "{}/api/v1/approvals/{}/approve",
            h.base_url, pending_approval.id
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(approve_resp.status(), StatusCode::OK);
    let approved_rec: ApprovalRecord = approve_resp.json().await.unwrap();
    assert_eq!(approved_rec.status, ApprovalStatus::Approved);

    let finished = h.wait_task_terminal(&task_id, Duration::from_secs(10)).await;
    assert_eq!(finished.status, TaskStatus::Completed);
    assert!(!h.workspace_dir().join("chat_approval.txt").exists());

    // 3. Under `never`, scripted delete_file is blocked immediately with 0 pending approvals.
    h.put_sandbox(&UpdateSandboxRequest {
        approval_policy: Some("never".to_string()),
        ..Default::default()
    })
    .await;

    h.mock.push_scripted(ScriptedResponse {
        content: String::new(),
        reasoning: None,
        tool_calls: vec![ScriptedToolCall {
            id: "call_del_never_1".to_string(),
            name: "delete_file".to_string(),
            arguments: json!({ "path": "never_del.txt" }),
        }],
        finish_reason: Some("tool_calls".to_string()),
    });
    h.push_scripted_text("Deletion was blocked by approval policy never.");

    let (status_never, resp_never) = h
        .send_message(
            &conv.id,
            &SendMessageRequest {
                content: "Try deleting never_del.txt".to_string(),
                ..Default::default()
            },
        )
        .await;
    assert_eq!(status_never, StatusCode::CREATED);
    assert_eq!(resp_never.task.unwrap().status, TaskStatus::Completed);
    assert!(h.workspace_dir().join("never_del.txt").exists());

    let remaining_pending: Vec<ApprovalRecord> = h
        .client
        .get(format!("{}/api/v1/approvals?pending_only=true", h.base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(remaining_pending.is_empty());

    h.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tool_groups_expand_and_denied_tools_override_enabled_tools() {
    let h = TestHarness::start().await;

    let fs_group = expand_tool_group("group:fs").expect("group:fs must expand");
    assert!(fs_group.contains(&"edit_file"));
    assert!(fs_group.contains(&"apply_patch"));
    assert!(fs_group.contains(&"grep_search"));
    assert!(fs_group.contains(&"glob_files"));

    let web_group = expand_tool_group("group:web").expect("group:web must expand");
    assert!(web_group.contains(&"web_fetch"));
    assert!(web_group.contains(&"browser"));

    let mem_group = expand_tool_group("group:memory").expect("group:memory must expand");
    assert!(mem_group.contains(&"memory_read"));
    assert!(mem_group.contains(&"memory_write"));
    assert!(mem_group.contains(&"memory_search"));

    let enabled = vec![
        "group:fs".to_string(),
        "group:web".to_string(),
        "group:memory".to_string(),
    ];
    let denied = vec!["delete_file".to_string(), "browser".to_string()];

    assert!(agent_allows_tool_with_policy(&enabled, &denied, "edit_file", "filesystem"));
    assert!(agent_allows_tool_with_policy(&enabled, &denied, "web_fetch", "web"));
    assert!(agent_allows_tool_with_policy(&enabled, &denied, "memory_read", "memory"));
    assert!(!agent_allows_tool_with_policy(&enabled, &denied, "delete_file", "filesystem"));
    assert!(!agent_allows_tool_with_policy(&enabled, &denied, "browser", "web"));

    let create_resp = h
        .client
        .post(format!("{}/api/v1/agents", h.base_url))
        .json(&json!({
            "name": "group-policy-tester",
            "description": "Tests group:* expansion and denied_tools override",
            "role": "CUSTOM",
            "system_prompt": "Test agent for group policy.",
            "enabled_tools": enabled,
            "denied_tools": denied,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    let agent: AgentDefinition = create_resp.json().await.unwrap();

    let defs = h.system.tools.tool_definitions_for_agent(&agent).unwrap();
    let names: Vec<&str> = defs.iter().map(|d| d.name.as_str()).collect();
    for expected in ["edit_file", "apply_patch", "grep_search", "glob_files", "web_fetch", "memory_read"] {
        assert!(
            names.contains(&expected),
            "expected expanded tool '{expected}' in {names:?}"
        );
    }
    assert!(!names.contains(&"delete_file"));
    assert!(!names.contains(&"browser"));

    let ctx = ToolExecContext {
        agent_id: agent.id.clone(),
        sandbox_mode: SandboxMode::FullSandbox,
        approval_policy: SandboxApprovalPolicy::FullAuto,
        ..Default::default()
    };
    let denied_browser = h
        .system
        .tools
        .invoke("browser", json!({ "action": "list" }), &ctx)
        .await
        .unwrap_err();
    assert!(matches!(denied_browser, AgentError::PermissionDenied(_)));

    let allowed_glob = h
        .system
        .tools
        .invoke("glob_files", json!({ "pattern": "*.md" }), &ctx)
        .await
        .unwrap();
    assert!(allowed_glob["files"].is_array());

    let conv = h
        .create_direct_conversation("Tools Slash Cmd", &agent.id)
        .await;
    let tools_slash = h
        .execute_command(&conv.id, &format!("/tools {}", agent.id), Some(&agent.id))
        .await;
    assert!(tools_slash.ephemeral);
    assert!(tools_slash.content.contains("delete_file"));
    assert!(tools_slash.content.contains("web_fetch"));

    h.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn web_fetch_stateful_browser_and_challenge_gate_over_local_tcp() {
    let h = TestHarness::start_with_web_sandbox().await;
    let fixture = LocalFixtureServer::start().await;

    let agent_resp = h
        .client
        .post(format!("{}/api/v1/agents", h.base_url))
        .json(&json!({
            "name": "web-browser-tester",
            "role": "CUSTOM",
            "system_prompt": "Web and browser sandbox tester.",
            "enabled_tools": ["group:web"],
            "denied_tools": [],
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(agent_resp.status(), StatusCode::CREATED);
    let agent: AgentDefinition = agent_resp.json().await.unwrap();

    let ctx = ToolExecContext {
        agent_id: agent.id.clone(),
        sandbox_mode: SandboxMode::FullSandbox,
        approval_policy: SandboxApprovalPolicy::FullAuto,
        ..Default::default()
    };

    // 1. web_fetch normal page + challenge gate page.
    let fetch_ok = h
        .system
        .tools
        .invoke(
            "web_fetch",
            json!({ "url": fixture.url("/"), "extract_mode": "markdown" }),
            &ctx,
        )
        .await
        .unwrap();
    assert_eq!(fetch_ok["ok"], true);
    assert_eq!(fetch_ok["blocked_by_challenge"], false);
    assert_eq!(fetch_ok["status"], 200);
    assert!(fetch_ok["content"]
        .as_str()
        .unwrap()
        .contains("Welcome to local fixture site"));

    let fetch_challenge = h
        .system
        .tools
        .invoke(
            "web_fetch",
            json!({ "url": fixture.url("/challenge") }),
            &ctx,
        )
        .await
        .unwrap();
    assert_eq!(fetch_challenge["ok"], false);
    assert_eq!(fetch_challenge["blocked_by_challenge"], true);

    // 2. Stateful browser lifecycle via ToolRegistry: open -> snapshot -> fill -> submit -> links -> click -> close.
    let open_res = h
        .system
        .tools
        .invoke(
            "browser",
            json!({ "action": "open", "url": fixture.url("/") }),
            &ctx,
        )
        .await
        .unwrap();
    assert_eq!(open_res["ok"], true);
    assert_eq!(open_res["status"], 200);
    let sid = open_res["session_id"].as_str().unwrap().to_string();

    let snap = h
        .system
        .tools
        .invoke("browser", json!({ "action": "snapshot", "session_id": sid }), &ctx)
        .await
        .unwrap();
    assert_eq!(snap["ok"], true);
    assert!(snap["snapshot"].as_str().unwrap().contains("[ref=e"));
    let elements = snap["elements"].as_array().unwrap();
    let input_ref = elements
        .iter()
        .find(|e| e["tag"] == "input" && e["name"] == "note")
        .expect("note input must appear in snapshot")["ref"]
        .as_str()
        .unwrap()
        .to_string();
    let submit_ref = elements
        .iter()
        .find(|e| e["tag"] == "button")
        .or_else(|| elements.iter().find(|e| e["tag"] == "form"))
        .expect("submit button or form must appear in snapshot")["ref"]
        .as_str()
        .unwrap()
        .to_string();

    let fill_res = h
        .system
        .tools
        .invoke(
            "browser",
            json!({
                "action": "fill",
                "session_id": sid,
                "ref": input_ref,
                "value": "ps5-hello"
            }),
            &ctx,
        )
        .await
        .unwrap();
    assert_eq!(fill_res["ok"], true);
    assert_eq!(fill_res["value"], "ps5-hello");

    let submit_res = h
        .system
        .tools
        .invoke(
            "browser",
            json!({
                "action": "submit",
                "session_id": sid,
                "ref": submit_ref
            }),
            &ctx,
        )
        .await
        .unwrap();
    assert_eq!(submit_res["ok"], true);
    assert_eq!(submit_res["title"], "Submitted OK");
    assert!(submit_res["cookie_count"].as_u64().unwrap() >= 1);
    {
        let recorded = fixture.submissions.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].method, "POST");
        assert_eq!(recorded[0].path, "/submit");
        assert!(recorded[0].body.contains("note=ps5-hello"));
    }

    // Re-open home on the same session to verify `links` + `click` navigation to `/about`.
    let nav_home = h
        .system
        .tools
        .invoke(
            "browser",
            json!({ "action": "navigate", "session_id": sid, "url": fixture.url("/") }),
            &ctx,
        )
        .await
        .unwrap();
    assert_eq!(nav_home["ok"], true);

    let links_res = h
        .system
        .tools
        .invoke("browser", json!({ "action": "links", "session_id": sid }), &ctx)
        .await
        .unwrap();
    let about_link_ref = links_res["links"]
        .as_array()
        .unwrap()
        .iter()
        .find(|l| l["href"] == "/about")
        .expect("/about link must appear in links")["ref"]
        .as_str()
        .unwrap()
        .to_string();

    let click_res = h
        .system
        .tools
        .invoke(
            "browser",
            json!({ "action": "click", "session_id": sid, "ref": about_link_ref }),
            &ctx,
        )
        .await
        .unwrap();
    assert_eq!(click_res["ok"], true);
    assert_eq!(click_res["title"], "About Fixture");
    assert!(click_res["url"].as_str().unwrap().ends_with("/about"));

    let close_res = h
        .system
        .tools
        .invoke("browser", json!({ "action": "close", "session_id": sid }), &ctx)
        .await
        .unwrap();
    assert_eq!(close_res["ok"], true);
    assert_eq!(close_res["closed"], true);

    // 3. Anti-bot challenge gate via browser open must return blocked_by_challenge without bypassing.
    let challenge_open = h
        .system
        .tools
        .invoke(
            "browser",
            json!({ "action": "open", "url": fixture.url("/challenge") }),
            &ctx,
        )
        .await
        .unwrap();
    assert_eq!(challenge_open["ok"], false);
    assert_eq!(challenge_open["blocked_by_challenge"], true);

    // 4. HTTP /api/v1/sandbox/browser/{action,sessions,sessions/{id}} endpoints.
    let http_open: Value = h
        .client
        .post(format!("{}/api/v1/sandbox/browser/action", h.base_url))
        .json(&json!({ "action": "open", "url": fixture.url("/") }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let http_sid = http_open["session_id"].as_str().unwrap().to_string();

    let sessions_resp: Value = h
        .client
        .get(format!("{}/api/v1/sandbox/browser/sessions", h.base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(sessions_resp["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|s| s["session_id"] == http_sid));

    let del_resp = h
        .client
        .delete(format!(
            "{}/api/v1/sandbox/browser/sessions/{http_sid}",
            h.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(del_resp.status(), StatusCode::OK);

    let after_close: Value = h
        .client
        .get(format!("{}/api/v1/sandbox/browser/sessions", h.base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(!after_close["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|s| s["session_id"] == http_sid));

    h.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn edit_apply_patch_atomic_rollback_search_glob_sandbox_exec_and_artifact() {
    let h = TestHarness::start().await;
    let ctx = ToolExecContext {
        agent_id: "programmer".to_string(),
        sandbox_mode: SandboxMode::FullSandbox,
        approval_policy: SandboxApprovalPolicy::FullAuto,
        ..Default::default()
    };

    h.system
        .tools
        .invoke(
            "write_file",
            json!({ "path": "src_sample.txt", "content": "alpha\nbeta\ngamma\n" }),
            &ctx,
        )
        .await
        .unwrap();
    h.system
        .tools
        .invoke(
            "write_file",
            json!({ "path": "nested/notes.txt", "content": "one\ntwo\nthree\n" }),
            &ctx,
        )
        .await
        .unwrap();

    // 1. edit_file
    let edit_res = h
        .system
        .tools
        .invoke(
            "edit_file",
            json!({ "path": "src_sample.txt", "old_text": "beta", "new_text": "beta_edited" }),
            &ctx,
        )
        .await
        .unwrap();
    assert_eq!(edit_res["edited"], true);
    assert_eq!(edit_res["replacements"], 1);
    assert_eq!(
        fs::read_to_string(h.workspace_dir().join("src_sample.txt")).unwrap(),
        "alpha\nbeta_edited\ngamma\n"
    );

    // 2. apply_patch (Codex Begin/End Patch format)
    let codex_patch = "\
*** Begin Patch
*** Add File: created.txt
+hello-created
*** Update File: src_sample.txt
@@
 alpha
-beta_edited
+beta_codex
 gamma
*** End Patch";
    let patch_res = h
        .system
        .tools
        .invoke("apply_patch", json!({ "patch": codex_patch }), &ctx)
        .await
        .unwrap();
    assert_eq!(patch_res["applied"], true);
    assert_eq!(
        fs::read_to_string(h.workspace_dir().join("created.txt")).unwrap(),
        "hello-created\n"
    );
    assert_eq!(
        fs::read_to_string(h.workspace_dir().join("src_sample.txt")).unwrap(),
        "alpha\nbeta_codex\ngamma\n"
    );

    // 3. apply_patch (unified diff format)
    let unified_patch = "\
--- a/nested/notes.txt
+++ b/nested/notes.txt
@@ -1,3 +1,3 @@
 one
-two
+TWO_UNIFIED
 three
";
    let unified_res = h
        .system
        .tools
        .invoke("apply_patch", json!({ "patch": unified_patch }), &ctx)
        .await
        .unwrap();
    assert_eq!(unified_res["applied"], true);
    assert_eq!(
        fs::read_to_string(h.workspace_dir().join("nested/notes.txt")).unwrap(),
        "one\nTWO_UNIFIED\nthree\n"
    );

    // 4. apply_patch atomic rollback when a second hunk fails.
    let before_rollback = fs::read_to_string(h.workspace_dir().join("src_sample.txt")).unwrap();
    let failing_patch = "\
*** Begin Patch
*** Update File: src_sample.txt
@@
 alpha
-beta_codex
+SHOULD_BE_ROLLED_BACK
 gamma
*** Update File: nested/notes.txt
@@
 nonexistent_context_line
-two
+FAIL_HERE
*** End Patch";
    let rollback_err = h
        .system
        .tools
        .invoke("apply_patch", json!({ "patch": failing_patch }), &ctx)
        .await;
    assert!(rollback_err.is_err());
    assert_eq!(
        fs::read_to_string(h.workspace_dir().join("src_sample.txt")).unwrap(),
        before_rollback
    );

    // 5. grep_search & glob_files
    let grep_res = h
        .system
        .tools
        .invoke(
            "grep_search",
            json!({ "query": "beta_codex", "path": "." }),
            &ctx,
        )
        .await
        .unwrap();
    let matches = grep_res["matches"].as_array().unwrap();
    assert!(matches
        .iter()
        .any(|m| m["path"] == "src_sample.txt" && m["line"] == 2));

    let glob_res = h
        .system
        .tools
        .invoke("glob_files", json!({ "pattern": "**/*.txt" }), &ctx)
        .await
        .unwrap();
    let found_paths: Vec<&str> = glob_res["files"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|f| f["path"].as_str())
        .collect();
    assert!(found_paths.contains(&"src_sample.txt"));
    assert!(found_paths.contains(&"created.txt"));
    assert!(found_paths.contains(&"nested/notes.txt"));

    // 6. sandbox_exec virtual builtins: echo, cat, grep, wc, sha256sum
    let exec_out = h
        .system
        .tools
        .invoke(
            "sandbox_exec",
            json!({
                "command": "echo line1 > exec_sample.txt && cat exec_sample.txt && grep line1 exec_sample.txt && wc -l exec_sample.txt && sha256sum exec_sample.txt"
            }),
            &ctx,
        )
        .await
        .unwrap();
    assert_eq!(exec_out["engine"], "virtual");
    assert_eq!(exec_out["exit_code"], 0);
    let stdout = exec_out["stdout"].as_str().unwrap();
    assert!(stdout.contains("line1"));
    let disk_bytes = fs::read(h.workspace_dir().join("exec_sample.txt")).unwrap();
    let expected_sha = hex::encode(Sha256::digest(&disk_bytes));
    assert!(stdout.contains(&expected_sha));

    // 7. write_artifact
    let artifact_res = h
        .system
        .tools
        .invoke(
            "write_artifact",
            json!({
                "filename": "report.md",
                "title": "Build Report",
                "content": "# Report\nAll checks passed.\n"
            }),
            &ctx,
        )
        .await
        .unwrap();
    assert_eq!(artifact_res["ok"], true);
    assert_eq!(artifact_res["path"], "artifacts/report.md");
    assert_eq!(artifact_res["marker"], "[ARTIFACT_CREATED: artifacts/report.md]");
    assert_eq!(
        fs::read_to_string(h.workspace_dir().join("artifacts/report.md")).unwrap(),
        "# Report\nAll checks passed.\n"
    );

    h.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn brain_files_seeding_http_crud_and_traversal_rejection() {
    let h = TestHarness::start().await;

    let expected_root = [
        "AGENTS.md",
        "SOUL.md",
        "IDENTITY.md",
        "USER.md",
        "TOOLS.md",
        "MEMORY.md",
        "HEARTBEAT.md",
    ];
    for name in expected_root {
        assert!(
            h.workspace_dir().join(name).is_file(),
            "brain file '{name}' must be seeded on startup"
        );
    }
    let disk_list = list_brain_files(&h.workspace_dir()).unwrap();
    for name in expected_root {
        assert!(disk_list.iter().any(|b| b.name == name && b.exists));
    }

    // GET /api/v1/brain lists all seeded brain files.
    let list_resp: Vec<BrainFileSummary> = h
        .client
        .get(format!("{}/api/v1/brain", h.base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    for name in expected_root {
        assert!(list_resp.iter().any(|b| b.name == name && b.exists));
    }

    // PUT /api/v1/brain + GET /api/v1/brain/{name} + GET /api/v1/brain?name=...
    let updated_memory = "# Long Term Memory\nKeep buffers bounded on PS5.\n";
    let put_resp = h
        .client
        .put(format!("{}/api/v1/brain", h.base_url))
        .json(&json!({ "name": "MEMORY.md", "content": updated_memory }))
        .send()
        .await
        .unwrap();
    assert_eq!(put_resp.status(), StatusCode::OK);
    let detail: BrainFileDetail = put_resp.json().await.unwrap();
    assert_eq!(detail.name, "MEMORY.md");
    assert_eq!(detail.content, updated_memory);

    let by_path: BrainFileDetail = h
        .client
        .get(format!("{}/api/v1/brain/MEMORY.md", h.base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(by_path.content, updated_memory);

    let by_query: BrainFileDetail = h
        .client
        .get(format!("{}/api/v1/brain", h.base_url))
        .query(&[("name", "MEMORY.md")])
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(by_query.content, updated_memory);

    let put_path = h
        .client
        .put(format!("{}/api/v1/brain/USER.md", h.base_url))
        .json(&json!({ "content": "# User\nConcise mode.\n" }))
        .send()
        .await
        .unwrap();
    assert_eq!(put_path.status(), StatusCode::OK);
    assert_eq!(
        fs::read_to_string(h.workspace_dir().join("USER.md")).unwrap(),
        "# User\nConcise mode.\n"
    );

    // Path traversal rejection.
    let get_trav = h
        .client
        .get(format!("{}/api/v1/brain", h.base_url))
        .query(&[("name", "../secret.md")])
        .send()
        .await
        .unwrap();
    assert_eq!(get_trav.status(), StatusCode::BAD_REQUEST);

    let put_trav = h
        .client
        .put(format!("{}/api/v1/brain", h.base_url))
        .json(&json!({ "name": "../secret.md", "content": "escape" }))
        .send()
        .await
        .unwrap();
    assert_eq!(put_trav.status(), StatusCode::BAD_REQUEST);

    let encoded_trav = h
        .client
        .get(format!("{}/api/v1/brain/..%2Fsecret.md", h.base_url))
        .send()
        .await
        .unwrap();
    assert!(matches!(
        encoded_trav.status(),
        StatusCode::BAD_REQUEST | StatusCode::FORBIDDEN
    ));
    assert!(read_brain_file(&h.workspace_dir(), "../secret.md").is_err());

    h.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slash_commands_execute_cleanly_and_btw_persists_no_chat_rows() {
    let h = TestHarness::start().await;
    let conv = h
        .create_direct_conversation("Slash Command Session", "programmer")
        .await;

    h.push_scripted_text("Initial greeting acknowledged.");
    let (seed_status, _) = h
        .send_message(
            &conv.id,
            &SendMessageRequest {
                content: "Hello programmer".to_string(),
                ..Default::default()
            },
        )
        .await;
    assert_eq!(seed_status, StatusCode::CREATED);
    let baseline_count = h.list_messages(&conv.id).await.len();
    assert_eq!(baseline_count, 2);

    // /btw <question> calls inference ephemerally without adding any ChatMessage row.
    h.push_scripted_text("Ephemeral answer: 42ms per frame.");
    let btw = h
        .execute_command(&conv.id, "/btw What is our frame budget?", None)
        .await;
    assert_eq!(btw.command, "btw");
    assert!(btw.ephemeral);
    assert!(btw.content.contains("42ms per frame"));
    assert_eq!(h.list_messages(&conv.id).await.len(), baseline_count);

    // /status, /tools, /doctor, /sandbox, /help all run ephemerally without adding messages.
    let status = h.execute_command(&conv.id, "/status", None).await;
    assert_eq!(status.command, "status");
    assert!(status.ephemeral);
    assert!(!status.content.trim().is_empty());

    let tools = h.execute_command(&conv.id, "/tools", None).await;
    assert_eq!(tools.command, "tools");
    assert!(tools.ephemeral);
    assert!(tools.content.contains("write_file"));

    let doctor = h.execute_command(&conv.id, "/doctor", None).await;
    assert_eq!(doctor.command, "doctor");
    assert!(doctor.ephemeral);
    assert!(doctor.content.contains("12"));

    let sandbox = h.execute_command(&conv.id, "/sandbox", None).await;
    assert_eq!(sandbox.command, "sandbox");
    assert!(sandbox.ephemeral);
    assert!(sandbox.content.contains("full_sandbox") || sandbox.content.contains("Mode"));

    let help = h.execute_command(&conv.id, "/help", None).await;
    assert_eq!(help.command, "help");
    assert!(help.ephemeral);
    assert!(help.content.contains("/btw"));
    assert!(help.content.contains("/doctor"));

    assert_eq!(h.list_messages(&conv.id).await.len(), baseline_count);

    // /new creates a fresh conversation cleanly.
    let new_cmd = h.execute_command(&conv.id, "/new Fresh Start", None).await;
    assert_eq!(new_cmd.command, "new");
    assert!(!new_cmd.ephemeral);
    let new_id = new_cmd.metadata["new_conversation_id"]
        .as_str()
        .expect("/new must return new_conversation_id")
        .to_string();
    assert_ne!(new_id, conv.id);

    let new_conv_resp = h
        .client
        .get(format!("{}/api/v1/conversations/{new_id}", h.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(new_conv_resp.status(), StatusCode::OK);

    h.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn system_doctor_runs_12_checks_and_repairs_missing_brain_and_sandbox_tmp() {
    let h = TestHarness::start().await;

    let report: DoctorReport = h
        .client
        .get(format!("{}/api/v1/doctor", h.base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(report.checks.len(), 12);
    for id in [
        "ps5lm_endpoint",
        "storage_integrity",
        "workspace_sandbox",
        "brain_files",
        "sandbox_network_policy",
        "auth_csrf_posture",
        "builtin_agent_tools",
        "scheduler_engine",
        "memory_bounds",
        "ps5_platform_compat",
        "meta_release_sync",
    ] {
        assert!(
            report.checks.iter().any(|c| c.id == id),
            "doctor report missing check '{id}'"
        );
    }

    // Delete TOOLS.md and sandbox/tmp so doctor detects fixable warnings/issues.
    fs::remove_file(h.workspace_dir().join("TOOLS.md")).unwrap();
    fs::remove_dir_all(h.workspace_dir().join("sandbox").join("tmp")).unwrap();
    assert!(!h.workspace_dir().join("TOOLS.md").exists());
    assert!(!h.workspace_dir().join("sandbox").join("tmp").exists());

    let degraded: DoctorReport = h
        .client
        .get(format!("{}/api/v1/doctor", h.base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let brain_check = degraded
        .checks
        .iter()
        .find(|c| c.id == "brain_files")
        .unwrap();
    assert_ne!(brain_check.status, DoctorStatus::Pass);
    assert!(brain_check.fixable);

    let ws_check = degraded
        .checks
        .iter()
        .find(|c| c.id == "workspace_sandbox")
        .unwrap();
    assert_ne!(ws_check.status, DoctorStatus::Pass);
    assert!(ws_check.fixable);

    // POST /api/v1/doctor/fix repairs both TOOLS.md and sandbox/tmp.
    let fix_resp = h
        .client
        .post(format!("{}/api/v1/doctor/fix", h.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(fix_resp.status(), StatusCode::OK);
    let fix_report: DoctorFixReport = fix_resp.json().await.unwrap();
    assert!(!fix_report.applied_fixes.is_empty());
    assert!(fix_report
        .applied_fixes
        .iter()
        .any(|f| f.contains("TOOLS.md")));
    assert!(fix_report
        .applied_fixes
        .iter()
        .any(|f| f.contains("tmp")));

    assert!(h.workspace_dir().join("TOOLS.md").is_file());
    assert!(h.workspace_dir().join("sandbox").join("tmp").is_dir());

    let healed_brain = fix_report
        .report
        .checks
        .iter()
        .find(|c| c.id == "brain_files")
        .unwrap();
    assert_eq!(healed_brain.status, DoctorStatus::Pass);

    let healed_ws = fix_report
        .report
        .checks
        .iter()
        .find(|c| c.id == "workspace_sandbox")
        .unwrap();
    assert_eq!(healed_ws.status, DoctorStatus::Pass);

    h.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn webhooks_create_redact_inbound_auth_and_delete() {
    let h = TestHarness::start().await;
    let conv = h
        .create_direct_conversation("Webhook Events", "programmer")
        .await;

    let create_resp = h
        .client
        .post(format!("{}/api/v1/webhooks", h.base_url))
        .json(&json!({
            "name": "ci-deploy-hook",
            "target_agent_id": "programmer",
            "conversation_id": conv.id,
            "prompt_template": "Build event:\n{{payload}}"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    let created: CreateWebhookResponse = create_resp.json().await.unwrap();
    assert!(!created.secret.is_empty());
    let hook_id = created.webhook.id.clone();

    // GET /api/v1/webhooks redacts the full secret and only exposes secret_prefix.
    let raw_list_text = h
        .client
        .get(format!("{}/api/v1/webhooks", h.base_url))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(!raw_list_text.contains(&created.secret));
    let views: Vec<WebhookView> = serde_json::from_str(&raw_list_text).unwrap();
    assert!(views
        .iter()
        .any(|w| w.id == hook_id && !w.secret_prefix.is_empty()));

    // Missing / unrecognized / wrong token checks.
    let no_hdr = h
        .client
        .post(format!("{}/api/v1/webhooks/inbound/{hook_id}", h.base_url))
        .json(&json!({ "event": "push" }))
        .send()
        .await
        .unwrap();
    assert_eq!(no_hdr.status(), StatusCode::UNAUTHORIZED);

    let wrong_hdr_name = h
        .client
        .post(format!("{}/api/v1/webhooks/inbound/{hook_id}", h.base_url))
        .header("X-Webhook-Token", &created.secret)
        .json(&json!({ "event": "push" }))
        .send()
        .await
        .unwrap();
    assert_eq!(wrong_hdr_name.status(), StatusCode::UNAUTHORIZED);

    // Note: routes::extract_webhook_token uses `x-ps5-webhook-token` (or `Authorization: Bearer`)
    // and adapters::trigger_inbound_webhook returns AgentError::PermissionDenied (403) on token mismatch.
    let wrong_tok = h
        .client
        .post(format!("{}/api/v1/webhooks/inbound/{hook_id}", h.base_url))
        .header("x-ps5-webhook-token", "invalid-secret-token")
        .json(&json!({ "event": "push" }))
        .send()
        .await
        .unwrap();
    assert!(matches!(
        wrong_tok.status(),
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN
    ));

    // Valid `x-ps5-webhook-token` triggers task & dispatches agent turn.
    h.mock.clear_recorded();
    h.push_scripted_text("Processed push webhook cleanly.");
    let ok_trigger = h
        .client
        .post(format!("{}/api/v1/webhooks/inbound/{hook_id}", h.base_url))
        .header("x-ps5-webhook-token", &created.secret)
        .json(&json!({ "event": "push", "repo": "ps5-agent" }))
        .send()
        .await
        .unwrap();
    assert_eq!(ok_trigger.status(), StatusCode::OK);
    let trig: WebhookTriggerResponse = ok_trigger.json().await.unwrap();
    assert!(trig.ok);
    let task_id = trig.task_id.expect("inbound webhook must create task");
    let finished = h.wait_task_terminal(&task_id, Duration::from_secs(10)).await;
    assert_eq!(finished.status, TaskStatus::Completed);

    let recorded = h.mock.recorded_requests();
    assert_eq!(recorded.len(), 1);
    let body_str = recorded[0].body.to_string();
    assert!(body_str.contains("Build event:"));
    assert!(body_str.contains("ps5-agent"));

    // Valid `Authorization: Bearer <secret>` also works.
    h.push_scripted_text("Processed second webhook via bearer auth.");
    let bearer_trigger = h
        .client
        .post(format!("{}/api/v1/webhooks/inbound/{hook_id}", h.base_url))
        .header("Authorization", format!("Bearer {}", created.secret))
        .json(&json!({ "event": "tag" }))
        .send()
        .await
        .unwrap();
    assert_eq!(bearer_trigger.status(), StatusCode::OK);
    let trig2: WebhookTriggerResponse = bearer_trigger.json().await.unwrap();
    let tid2 = trig2.task_id.expect("second webhook must create task");
    assert_eq!(
        h.wait_task_terminal(&tid2, Duration::from_secs(10)).await.status,
        TaskStatus::Completed
    );

    let views_after: Vec<WebhookView> = h
        .client
        .get(format!("{}/api/v1/webhooks", h.base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        views_after
            .iter()
            .find(|w| w.id == hook_id)
            .unwrap()
            .trigger_count,
        2
    );

    // DELETE /api/v1/webhooks/{id} removes webhook.
    let del = h
        .client
        .delete(format!("{}/api/v1/webhooks/{hook_id}", h.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(del.status(), StatusCode::OK);

    let after_del: Vec<WebhookView> = h
        .client
        .get(format!("{}/api/v1/webhooks", h.base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(!after_del.iter().any(|w| w.id == hook_id));

    let post_deleted = h
        .client
        .post(format!("{}/api/v1/webhooks/inbound/{hook_id}", h.base_url))
        .header("x-ps5-webhook-token", &created.secret)
        .json(&json!({ "event": "after-delete" }))
        .send()
        .await
        .unwrap();
    assert_eq!(post_deleted.status(), StatusCode::NOT_FOUND);

    h.shutdown().await;
}
