use mock_ps5lm::{start_ephemeral, MockPs5LmHandle, ScriptedResponse};
use ps5_agent::shutdown::finalize_shutdown;
use ps5_agent::startup::initialize;
use ps5_agent::AppSystem;
use ps5_agent_config::AppConfig;
use ps5_agent_core::{
    AgentCardView, AgentDefinition, ChatAttachment, ChatMessage, ChatRole, Conversation,
    ConversationMode, Ps5LmCapabilities, StreamEvent, ThinkingLevel,
};
use ps5_agent_http::server::bind_reusable_listener;
use ps5_agent_http::{
    serve_with_listener, SendMessageRequest, SendMessageResponse, UpdateAgentRequest,
    WorkspaceFilesResponse,
};
use serde_json::{json, Value};
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

struct TestHarness {
    _temp: TempDir,
    mock: MockPs5LmHandle,
    system: Arc<AppSystem>,
    base_url: String,
    client: reqwest::Client,
    server: JoinHandle<std::io::Result<()>>,
}

impl TestHarness {
    async fn start(configure_mock: impl FnOnce(&MockPs5LmHandle), configure_app: impl FnOnce(&mut AppConfig)) -> Self {
        let temp = TempDir::new().expect("create temp dir");
        let mock = start_ephemeral().await.expect("start mock-ps5lm");
        configure_mock(&mock);

        let listener = bind_reusable_listener("127.0.0.1:0")
            .await
            .expect("bind ephemeral HTTP listener");
        let addr = listener.local_addr().expect("listener local_addr");

        let mut cfg = build_test_config(temp.path(), mock.base_url(), addr.port());
        configure_app(&mut cfg);
        cfg.save(&temp.path().join("config.toml"))
            .expect("save initial config.toml");

        let system = initialize(cfg).await.expect("initialize ps5-agent");
        let state = system.http_state.clone();
        let bind_addr = addr.to_string();
        let server = tokio::spawn(async move { serve_with_listener(bind_addr, listener, state).await });

        let client = reqwest::Client::builder()
            .pool_max_idle_per_host(0)
            .build()
            .expect("build reqwest client");

        Self {
            _temp: temp,
            mock,
            system,
            base_url: format!("http://{addr}"),
            client,
            server,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    async fn shutdown(self) {
        self.system.shutdown.cancel();
        let _ = self.server.await;
        finalize_shutdown(&self.system);
        drop(self.system);
        self.mock.shutdown();
    }
}

fn build_test_config(dir: &Path, inference_base_url: String, port: u16) -> AppConfig {
    let mut cfg = AppConfig::with_data_dir(dir.to_path_buf());
    cfg.database.driver = "sqlite".to_string();
    cfg.logging.enable_klog = false;
    cfg.logging.enable_toast = false;
    cfg.security.auth_enabled = false;
    cfg.server.bind_host = "127.0.0.1".to_string();
    cfg.server.port = port;
    cfg.inference.base_url = inference_base_url;
    cfg.inference.stream_enabled = true;
    cfg.runtime.health_poll_interval_secs = 60;
    cfg
}

fn drain_events(rx: &mut broadcast::Receiver<StreamEvent>) -> Vec<StreamEvent> {
    let mut out = Vec::new();
    while let Ok(ev) = rx.try_recv() {
        out.push(ev);
    }
    out
}

fn parse_sse_blocks(buf: &str) -> Vec<(Option<String>, String)> {
    let normalized = buf.replace("\r\n", "\n");
    let mut events = Vec::new();
    for block in normalized.split("\n\n") {
        let trimmed = block.trim();
        if trimmed.is_empty() {
            continue;
        }
        let mut event_name = None;
        let mut data_lines = Vec::new();
        for line in trimmed.lines() {
            if let Some(ev) = line.strip_prefix("event:") {
                event_name = Some(ev.trim().to_string());
            } else if let Some(d) = line.strip_prefix("data:") {
                data_lines.push(d.trim().to_string());
            }
        }
        if !data_lines.is_empty() {
            events.push((event_name, data_lines.join("\n")));
        }
    }
    events
}

async fn read_sse_until(
    resp: &mut reqwest::Response,
    timeout: Duration,
    mut done: impl FnMut(&[(Option<String>, String)]) -> bool,
) -> Vec<(Option<String>, String)> {
    let mut raw = String::new();
    let deadline = Instant::now() + timeout;
    loop {
        let parsed = parse_sse_blocks(&raw);
        if done(&parsed) {
            return parsed;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out waiting for expected SSE event; received so far: {parsed:?}"
        );
        match tokio::time::timeout(remaining, resp.chunk()).await {
            Ok(Ok(Some(chunk))) => {
                raw.push_str(&String::from_utf8_lossy(&chunk));
            }
            Ok(Ok(None)) => {
                let parsed = parse_sse_blocks(&raw);
                assert!(
                    done(&parsed),
                    "SSE stream closed before expected event; received: {parsed:?}"
                );
                return parsed;
            }
            Ok(Err(e)) => panic!("SSE chunk read error: {e}"),
            Err(_) => {
                let parsed = parse_sse_blocks(&raw);
                panic!("timed out reading SSE chunk; received so far: {parsed:?}");
            }
        }
    }
}

fn encode_base64(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(TABLE[((triple >> 18) & 0x3f) as usize] as char);
        out.push(TABLE[((triple >> 12) & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[((triple >> 6) & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[(triple & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

async fn create_conversation(
    harness: &TestHarness,
    title: &str,
    mode: ConversationMode,
) -> Conversation {
    let resp = harness
        .client
        .post(harness.url("/api/v1/conversations"))
        .json(&json!({
            "title": title,
            "mode": mode,
        }))
        .send()
        .await
        .expect("POST /api/v1/conversations");
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    resp.json().await.expect("parse Conversation")
}

async fn create_direct_conversation(
    harness: &TestHarness,
    title: &str,
    agent_id: &str,
) -> Conversation {
    create_conversation(
        harness,
        title,
        ConversationMode::Direct {
            agent_id: agent_id.to_string(),
        },
    )
    .await
}

async fn create_group_conversation(
    harness: &TestHarness,
    title: &str,
    participant_agent_ids: Vec<&str>,
) -> Conversation {
    create_conversation(
        harness,
        title,
        ConversationMode::Group {
            participant_agent_ids: participant_agent_ids
                .into_iter()
                .map(str::to_string)
                .collect(),
        },
    )
    .await
}

async fn send_chat_message(
    harness: &TestHarness,
    conv_id: &str,
    req: &SendMessageRequest,
) -> SendMessageResponse {
    let resp = harness
        .client
        .post(harness.url(&format!("/api/v1/conversations/{conv_id}/messages")))
        .json(req)
        .send()
        .await
        .expect("POST /api/v1/conversations/{id}/messages");
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    resp.json().await.expect("parse SendMessageResponse")
}

async fn list_messages(harness: &TestHarness, conv_id: &str) -> Vec<ChatMessage> {
    let resp = harness
        .client
        .get(harness.url(&format!("/api/v1/conversations/{conv_id}/messages")))
        .send()
        .await
        .expect("GET /api/v1/conversations/{id}/messages");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    resp.json().await.expect("parse Vec<ChatMessage>")
}

fn assert_thinking_wire_payload(body: &Value, level: ThinkingLevel) {
    assert!(
        body.get("thinking_budget_tokens").is_none(),
        "legacy thinking_budget_tokens must not be sent; got {body}"
    );
    match level {
        ThinkingLevel::Off => {
            assert_eq!(
                body["chat_template_kwargs"]["enable_thinking"],
                json!(false)
            );
            assert!(body.get("reasoning_effort").is_none());
            assert_eq!(body["reasoning_budget"], json!(0));
            assert_eq!(body["reasoning_format"], json!("none"));
        }
        ThinkingLevel::Low => {
            assert_eq!(body["chat_template_kwargs"]["enable_thinking"], json!(true));
            assert_eq!(body["reasoning_effort"], json!("low"));
            assert_eq!(body["reasoning_budget"], json!(512));
        }
        ThinkingLevel::Medium => {
            assert_eq!(body["chat_template_kwargs"]["enable_thinking"], json!(true));
            assert_eq!(body["reasoning_effort"], json!("medium"));
            assert_eq!(body["reasoning_budget"], json!(2048));
        }
        ThinkingLevel::High => {
            assert_eq!(body["chat_template_kwargs"]["enable_thinking"], json!(true));
            assert_eq!(body["reasoning_effort"], json!("high"));
            assert_eq!(body["reasoning_budget"], json!(8192));
        }
        ThinkingLevel::XHigh => {
            assert_eq!(body["chat_template_kwargs"]["enable_thinking"], json!(true));
            assert_eq!(body["reasoning_effort"], json!("xhigh"));
            assert_eq!(body["reasoning_budget"], json!(16384));
        }
        ThinkingLevel::Max => {
            assert_eq!(body["chat_template_kwargs"]["enable_thinking"], json!(true));
            assert_eq!(body["reasoning_effort"], json!("xhigh"));
            assert_eq!(body["reasoning_budget"], json!(-1));
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_thinking_levels_override_and_per_agent_model_config() {
    let harness = TestHarness::start(
        |mock| {
            mock.set_model_name("qwen3-32b-thinking");
            mock.set_props(16384, 4, true, true);
        },
        |_cfg| {},
    )
    .await;

    let conv_cmd = create_direct_conversation(&harness, "Thinking Overrides", "commander").await;
    let mut events_rx = harness.system.events_tx.subscribe();

    // Part 1: Verify all 6 ThinkingLevel chat overrides (`off`, `low`, `medium`, `high`, `xhigh`, `max`)
    let levels = [
        ThinkingLevel::Off,
        ThinkingLevel::Low,
        ThinkingLevel::Medium,
        ThinkingLevel::High,
        ThinkingLevel::XHigh,
        ThinkingLevel::Max,
    ];

    for level in levels {
        harness.mock.clear_recorded();
        let _ = drain_events(&mut events_rx);

        let mut sse_resp = harness
            .client
            .get(harness.url("/api/v1/events"))
            .send()
            .await
            .expect("connect SSE /api/v1/events");
        assert_eq!(sse_resp.status(), reqwest::StatusCode::OK);

        let prompt = format!("Check thinking level {}", level.as_str());
        let sent = send_chat_message(
            &harness,
            &conv_cmd.id,
            &SendMessageRequest {
                content: prompt.clone(),
                thinking_level: Some(level),
                async_dispatch: false,
                ..Default::default()
            },
        )
        .await;
        assert!(sent.task.is_some());

        let sse_events = read_sse_until(&mut sse_resp, Duration::from_secs(5), |evs| {
            evs.iter()
                .any(|(name, _)| name.as_deref() == Some("chat_completed"))
        })
        .await;

        let recorded = harness.mock.recorded_requests();
        let chat_req = recorded
            .iter()
            .rfind(|r| r.path == "/v1/chat/completions")
            .expect("recorded /v1/chat/completions request");

        assert_thinking_wire_payload(&chat_req.body, level);

        let broadcast_events = drain_events(&mut events_rx);
        let reasoning_broadcast_count = broadcast_events
            .iter()
            .filter(|ev| matches!(ev, StreamEvent::ChatReasoning { .. }))
            .count();
        let sse_reasoning_count = sse_events
            .iter()
            .filter(|(name, _)| name.as_deref() == Some("chat_reasoning"))
            .count();

        let msgs = list_messages(&harness, &conv_cmd.id).await;
        let last_assistant = msgs
            .iter()
            .rev()
            .find(|m| m.role == ChatRole::Assistant)
            .expect("assistant reply persisted");

        assert!(
            !last_assistant.content.contains("<think>"),
            "parsed assistant content must strip <think> tags: {}",
            last_assistant.content
        );

        if level == ThinkingLevel::Off {
            assert_eq!(
                reasoning_broadcast_count, 0,
                "ThinkingLevel::Off must emit no StreamEvent::ChatReasoning"
            );
            assert_eq!(
                sse_reasoning_count, 0,
                "ThinkingLevel::Off must emit no SSE chat_reasoning events"
            );
            assert!(
                last_assistant.reasoning.is_none(),
                "ThinkingLevel::Off must persist reasoning=None, got {:?}",
                last_assistant.reasoning
            );
        } else {
            assert!(
                reasoning_broadcast_count > 0,
                "ThinkingLevel::{level:?} must emit StreamEvent::ChatReasoning"
            );
            assert!(
                sse_reasoning_count > 0,
                "ThinkingLevel::{level:?} must emit SSE chat_reasoning events"
            );
            let stored_reasoning = last_assistant
                .reasoning
                .as_deref()
                .expect("reasoning persisted on assistant ChatMessage");
            assert!(
                stored_reasoning.contains(level.as_str()),
                "expected reasoning to reflect '{}', got '{}'",
                level.as_str(),
                stored_reasoning
            );
        }
    }

    // Part 2: Verify per-agent AgentModelConfig.thinking_level (without chat override)
    let card_resp = harness
        .client
        .get(harness.url("/api/v1/agents/programmer"))
        .send()
        .await
        .expect("GET /api/v1/agents/programmer");
    assert_eq!(card_resp.status(), reqwest::StatusCode::OK);
    let programmer_card: AgentCardView = card_resp.json().await.expect("parse AgentCardView");
    assert_eq!(
        programmer_card.definition.model_config.thinking_level,
        ThinkingLevel::High
    );

    let conv_prog = create_direct_conversation(&harness, "Per-Agent Thinking", "programmer").await;

    // Update programmer's model_config.thinking_level -> Off
    let mut cfg_off = programmer_card.definition.model_config.clone();
    cfg_off.thinking_level = ThinkingLevel::Off;
    let upd_off = harness
        .client
        .put(harness.url("/api/v1/agents/programmer"))
        .json(&UpdateAgentRequest {
            model_config: Some(cfg_off),
            ..Default::default()
        })
        .send()
        .await
        .expect("PUT /api/v1/agents/programmer (Off)");
    assert_eq!(upd_off.status(), reqwest::StatusCode::OK);
    let updated_def_off: AgentDefinition = upd_off.json().await.expect("parse AgentDefinition");
    assert_eq!(updated_def_off.model_config.thinking_level, ThinkingLevel::Off);

    harness.mock.clear_recorded();
    let _ = drain_events(&mut events_rx);

    send_chat_message(
        &harness,
        &conv_prog.id,
        &SendMessageRequest {
            content: "Agent default off check".to_string(),
            thinking_level: None,
            async_dispatch: false,
            ..Default::default()
        },
    )
    .await;

    let rec_agent_off = harness.mock.recorded_requests();
    let req_agent_off = rec_agent_off
        .iter()
        .rfind(|r| r.path == "/v1/chat/completions")
        .expect("chat request for programmer Off");
    assert_thinking_wire_payload(&req_agent_off.body, ThinkingLevel::Off);
    let evs_agent_off = drain_events(&mut events_rx);
    assert!(
        !evs_agent_off
            .iter()
            .any(|ev| matches!(ev, StreamEvent::ChatReasoning { .. })),
        "per-agent ThinkingLevel::Off must suppress ChatReasoning"
    );

    // Update programmer's model_config.thinking_level -> Max
    let mut cfg_max = updated_def_off.model_config.clone();
    cfg_max.thinking_level = ThinkingLevel::Max;
    let upd_max = harness
        .client
        .put(harness.url("/api/v1/agents/programmer"))
        .json(&UpdateAgentRequest {
            model_config: Some(cfg_max),
            ..Default::default()
        })
        .send()
        .await
        .expect("PUT /api/v1/agents/programmer (Max)");
    assert_eq!(upd_max.status(), reqwest::StatusCode::OK);

    let verify_card: AgentCardView = harness
        .client
        .get(harness.url("/api/v1/agents/programmer"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        verify_card.definition.model_config.thinking_level,
        ThinkingLevel::Max
    );

    harness.mock.clear_recorded();
    let _ = drain_events(&mut events_rx);

    send_chat_message(
        &harness,
        &conv_prog.id,
        &SendMessageRequest {
            content: "Agent default max check".to_string(),
            thinking_level: None,
            async_dispatch: false,
            ..Default::default()
        },
    )
    .await;

    let rec_agent_max = harness.mock.recorded_requests();
    let req_agent_max = rec_agent_max
        .iter()
        .rfind(|r| r.path == "/v1/chat/completions")
        .expect("chat request for programmer Max");
    assert_thinking_wire_payload(&req_agent_max.body, ThinkingLevel::Max);

    let evs_agent_max = drain_events(&mut events_rx);
    assert!(
        evs_agent_max
            .iter()
            .any(|ev| matches!(ev, StreamEvent::ChatReasoning { .. })),
        "per-agent ThinkingLevel::Max must emit ChatReasoning"
    );

    let prog_msgs = list_messages(&harness, &conv_prog.id).await;
    let last_prog_reply = prog_msgs
        .iter()
        .rev()
        .find(|m| m.role == ChatRole::Assistant)
        .expect("programmer reply persisted");
    assert!(
        last_prog_reply
            .reasoning
            .as_deref()
            .is_some_and(|r| r.contains("max")),
        "expected persisted reasoning for Max, got {:?}",
        last_prog_reply.reasoning
    );

    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_ps5lm_boot_and_sync_capabilities_settings_and_events() {
    let harness = TestHarness::start(
        |mock| {
            mock.set_model_name("qwen3-vl-32b-thinking");
            mock.set_props(32768, 6, true, true);
        },
        |cfg| {
            cfg.inference.model = "uninitialized-placeholder".to_string();
            cfg.inference.context_limit = 4096;
            cfg.agents.max_concurrent_inference = 4;
            cfg.inference.auto_sync_from_ps5lm = true;
        },
    )
    .await;

    // 1. Verify boot-time automatic PS5LM capability discovery & runtime settings sync
    let boot_caps: Ps5LmCapabilities = harness
        .client
        .get(harness.url("/api/v1/inference/capabilities"))
        .send()
        .await
        .expect("GET /api/v1/inference/capabilities")
        .json()
        .await
        .expect("parse Ps5LmCapabilities");
    assert!(boot_caps.healthy);
    assert_eq!(
        boot_caps.loaded_model.as_deref(),
        Some("qwen3-vl-32b-thinking")
    );
    assert_eq!(boot_caps.context_length, Some(32768));
    assert_eq!(boot_caps.total_slots, Some(6));
    assert_eq!(
        boot_caps.available_models,
        vec!["qwen3-vl-32b-thinking".to_string()]
    );
    assert!(boot_caps.supports_vision);
    assert!(boot_caps.supports_thinking);

    let boot_settings: Value = harness
        .client
        .get(harness.url("/api/v1/settings"))
        .send()
        .await
        .expect("GET /api/v1/settings")
        .json()
        .await
        .expect("parse settings JSON");
    assert_eq!(
        boot_settings["inference"]["model"],
        json!("qwen3-vl-32b-thinking")
    );
    assert_eq!(boot_settings["inference"]["context_limit"], json!(32768));
    assert_eq!(
        boot_settings["agents"]["max_concurrent_inference"],
        json!(6)
    );
    assert_eq!(
        boot_settings["runtime"]["inference_model"],
        json!("qwen3-vl-32b-thinking")
    );
    assert_eq!(harness.system.inference.config().model, "qwen3-vl-32b-thinking");
    assert_eq!(harness.system.inference.config().context_limit, 32768);
    assert_eq!(harness.system.inference.max_concurrent(), 6);

    // 2. Reconfigure mock-ps5lm and trigger POST /api/v1/inference/sync while listening on SSE & events_tx
    let mut events_rx = harness.system.events_tx.subscribe();
    let mut sse_resp = harness
        .client
        .get(harness.url("/api/v1/events/stream"))
        .send()
        .await
        .expect("connect SSE /api/v1/events/stream");
    assert_eq!(sse_resp.status(), reqwest::StatusCode::OK);

    harness.mock.set_model_name("deepseek-r1-671b-q4");
    harness.mock.set_props(65536, 8, false, true);

    let sync_resp = harness
        .client
        .post(harness.url("/api/v1/inference/sync"))
        .send()
        .await
        .expect("POST /api/v1/inference/sync");
    assert_eq!(sync_resp.status(), reqwest::StatusCode::OK);
    let synced_caps: Ps5LmCapabilities = sync_resp.json().await.expect("parse synced capabilities");
    assert!(synced_caps.healthy);
    assert_eq!(
        synced_caps.loaded_model.as_deref(),
        Some("deepseek-r1-671b-q4")
    );
    assert_eq!(synced_caps.context_length, Some(65536));
    assert_eq!(synced_caps.total_slots, Some(8));
    assert_eq!(
        synced_caps.available_models,
        vec!["deepseek-r1-671b-q4".to_string()]
    );
    assert!(synced_caps.supports_thinking);

    // 3. Verify GET /api/v1/inference/capabilities and GET /api/v1/settings reflect updated sync
    let caps_after: Ps5LmCapabilities = harness
        .client
        .get(harness.url("/api/v1/inference/capabilities"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(caps_after.loaded_model, synced_caps.loaded_model);
    assert_eq!(caps_after.context_length, Some(65536));
    assert_eq!(caps_after.total_slots, Some(8));
    assert_eq!(caps_after.available_models, vec!["deepseek-r1-671b-q4".to_string()]);

    let settings_after: Value = harness
        .client
        .get(harness.url("/api/v1/settings"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        settings_after["inference"]["model"],
        json!("deepseek-r1-671b-q4")
    );
    assert_eq!(settings_after["inference"]["context_limit"], json!(65536));
    assert_eq!(
        settings_after["agents"]["max_concurrent_inference"],
        json!(8)
    );
    assert_eq!(
        settings_after["runtime"]["inference_model"],
        json!("deepseek-r1-671b-q4")
    );
    assert_eq!(harness.system.inference.config().model, "deepseek-r1-671b-q4");
    assert_eq!(harness.system.inference.config().context_limit, 65536);
    assert_eq!(harness.system.inference.max_concurrent(), 8);

    // 4. Verify audit log persistence for inference.sync
    let audit_logs = harness
        .system
        .store
        .list_audit_logs(20)
        .expect("list_audit_logs");
    assert!(
        audit_logs
            .iter()
            .any(|a| a.action == "inference.sync" && a.details_json.contains("deepseek-r1-671b-q4")),
        "expected persisted inference.sync audit log, got: {audit_logs:?}"
    );

    // 5. Verify StreamEvent::Ps5LmSynced on broadcast and `ps5_lm_synced` on SSE wire
    let broadcast_events = drain_events(&mut events_rx);
    assert!(
        broadcast_events.iter().any(|ev| matches!(
            ev,
            StreamEvent::Ps5LmSynced {
                loaded_model: Some(m),
                context_length: Some(65536),
                total_slots: Some(8),
            } if m == "deepseek-r1-671b-q4"
        )),
        "expected StreamEvent::Ps5LmSynced on events_tx, got: {broadcast_events:?}"
    );

    let sse_events = read_sse_until(&mut sse_resp, Duration::from_secs(5), |evs| {
        evs.iter()
            .any(|(name, _)| name.as_deref() == Some("ps5_lm_synced"))
    })
    .await;
    let (_, synced_data) = sse_events
        .iter()
        .find(|(name, _)| name.as_deref() == Some("ps5_lm_synced"))
        .expect("SSE ps5_lm_synced event");
    let synced_json: Value = serde_json::from_str(synced_data).expect("parse SSE ps5_lm_synced JSON");
    assert_eq!(synced_json["type"], json!("ps5_lm_synced"));
    assert_eq!(synced_json["loaded_model"], json!("deepseek-r1-671b-q4"));
    assert_eq!(synced_json["context_length"], json!(65536));
    assert_eq!(synced_json["total_slots"], json!(8));

    // 6. Verify PUT /api/v1/settings persists synced inference settings to config.toml and DB
    let put_resp = harness
        .client
        .put(harness.url("/api/v1/settings"))
        .json(&json!({
            "inference": {
                "model": "deepseek-r1-671b-q4",
                "context_limit": 65536,
                "thinking_level": "high",
                "auto_sync_from_ps5lm": true
            }
        }))
        .send()
        .await
        .expect("PUT /api/v1/settings");
    assert_eq!(put_resp.status(), reqwest::StatusCode::OK);

    let config_path = harness._temp.path().join("config.toml");
    let (reloaded_cfg, warnings) =
        AppConfig::load_or_create_resilient(&config_path).expect("reload config.toml");
    assert!(warnings.is_empty(), "unexpected config warnings: {warnings:?}");
    assert_eq!(reloaded_cfg.inference.model, "deepseek-r1-671b-q4");
    assert_eq!(reloaded_cfg.inference.context_limit, 65536);
    assert_eq!(reloaded_cfg.inference.thinking_level, "high");
    assert!(reloaded_cfg.inference.auto_sync_from_ps5lm);

    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multimodal_attachments_forward_images_and_save_uploads() {
    let harness = TestHarness::start(
        |mock| {
            mock.set_model_name("qwen3-vl-8b");
            mock.set_props(16384, 4, true, true);
        },
        |_cfg| {},
    )
    .await;

    let conv = create_direct_conversation(&harness, "Multimodal Attachments", "commander").await;
    let mut events_rx = harness.system.events_tx.subscribe();

    let png_data_url = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=".to_string();
    let text_bytes = b"PS5LM multimodal upload check\nLine 2: repro details\n";
    let text_data_url = format!("data:text/plain;base64,{}", encode_base64(text_bytes));

    let sent = send_chat_message(
        &harness,
        &conv.id,
        &SendMessageRequest {
            content: "Inspect the attached PNG and repro-notes.txt.".to_string(),
            attachments: vec![
                ChatAttachment {
                    kind: "image".to_string(),
                    name: "ui-bug.png".to_string(),
                    mime_type: "image/png".to_string(),
                    data_url: Some(png_data_url.clone()),
                    workspace_path: None,
                    size_bytes: 68,
                },
                ChatAttachment {
                    kind: "file".to_string(),
                    name: "repro-notes.txt".to_string(),
                    mime_type: "text/plain".to_string(),
                    data_url: Some(text_data_url),
                    workspace_path: None,
                    size_bytes: 0,
                },
            ],
            async_dispatch: false,
            ..Default::default()
        },
    )
    .await;

    // 1. Verify SendMessageResponse attachment normalization
    assert_eq!(sent.user_message.attachments.len(), 2);
    let img_att = sent
        .user_message
        .attachments
        .iter()
        .find(|a| a.name == "ui-bug.png")
        .expect("image attachment in response");
    assert_eq!(img_att.data_url.as_deref(), Some(png_data_url.as_str()));
    assert!(img_att.workspace_path.is_none());

    let txt_att = sent
        .user_message
        .attachments
        .iter()
        .find(|a| a.name == "repro-notes.txt")
        .expect("text attachment in response");
    assert!(
        txt_att.data_url.is_none(),
        "non-image upload should strip inline data_url once saved"
    );
    let saved_rel_path = txt_att
        .workspace_path
        .as_deref()
        .expect("non-image upload must record workspace_path");
    assert!(
        saved_rel_path.starts_with("uploads/") && saved_rel_path.ends_with("_repro-notes.txt"),
        "unexpected upload path: {saved_rel_path}"
    );
    assert_eq!(txt_att.size_bytes, text_bytes.len() as u64);

    // 2. Verify file saved on disk under workspace/uploads/ and listed via GET /api/v1/files?path=uploads
    let saved_disk_path = harness._temp.path().join("workspace").join(saved_rel_path);
    assert!(
        saved_disk_path.exists(),
        "uploaded file must exist at {}",
        saved_disk_path.display()
    );
    assert_eq!(fs::read(&saved_disk_path).unwrap(), text_bytes);

    let files_resp: WorkspaceFilesResponse = harness
        .client
        .get(harness.url("/api/v1/files?path=uploads"))
        .send()
        .await
        .expect("GET /api/v1/files?path=uploads")
        .json()
        .await
        .expect("parse WorkspaceFilesResponse");
    assert!(
        files_resp.entries.iter().any(|e| e.path == saved_rel_path),
        "uploads listing must contain '{}', got: {:?}",
        saved_rel_path,
        files_resp.entries
    );

    // 3. Verify StreamEvent::FilesChanged upload notification
    let events = drain_events(&mut events_rx);
    assert!(
        events.iter().any(|ev| matches!(
            ev,
            StreamEvent::FilesChanged {
                conversation_id: Some(cid),
                path,
                action,
            } if cid == &conv.id && path == saved_rel_path && action == "upload"
        )),
        "expected FilesChanged upload event, got: {events:?}"
    );

    // 4. Verify mock-ps5lm received multimodal `image_url` part and uploaded file reference
    let recorded = harness.mock.recorded_requests();
    let chat_req = recorded
        .iter()
        .rfind(|r| r.path == "/v1/chat/completions")
        .expect("recorded /v1/chat/completions request");
    let msgs_json = chat_req.body["messages"]
        .as_array()
        .expect("messages array in chat completions payload");
    let user_msg_json = msgs_json
        .iter()
        .rev()
        .find(|m| m["role"] == "user")
        .expect("user message in payload");
    let parts = user_msg_json["content"]
        .as_array()
        .expect("multimodal content must be serialized as array of parts");

    let text_part = parts
        .iter()
        .find(|p| p["type"] == "text")
        .and_then(|p| p["text"].as_str())
        .expect("text part in multimodal content");
    assert!(
        text_part.contains("repro-notes.txt") && text_part.contains(saved_rel_path),
        "text part should reference uploaded file '{}', got: {}",
        saved_rel_path,
        text_part
    );

    let image_part = parts
        .iter()
        .find(|p| p["type"] == "image_url")
        .expect("image_url part in multimodal content");
    assert_eq!(image_part["image_url"]["url"], json!(png_data_url));

    // 5. Verify conversation messages endpoint returns persisted attachments + assistant reply
    let conv_msgs = list_messages(&harness, &conv.id).await;
    let persisted_user = conv_msgs
        .iter()
        .find(|m| m.id == sent.user_message.id)
        .expect("persisted user message");
    assert_eq!(persisted_user.attachments.len(), 2);
    assert!(conv_msgs.iter().any(|m| m.role == ChatRole::Assistant));

    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_kv_prefill_and_llamacpp_settings_sync() {
    let harness = TestHarness::start(
        |mock| {
            mock.set_model_name("qwen3-kv-sync");
            mock.set_props(16384, 4, true, true);
        },
        |cfg| {
            cfg.inference.prefill_kv_cache = false;
        },
    )
    .await;

    // Flat dotted key variant must not trigger sensitive-key 403 rejection.
    let flat_put = harness
        .client
        .put(harness.url("/api/v1/settings"))
        .json(&json!({ "inference.timings_per_token": true }))
        .send()
        .await
        .expect("PUT /api/v1/settings flat timings_per_token");
    assert_eq!(
        flat_put.status(),
        reqwest::StatusCode::OK,
        "flat inference.timings_per_token must return 200 OK (not 403)"
    );

    // Structured inference settings update hot-reloads sampler + prefill config.
    let put_resp = harness
        .client
        .put(harness.url("/api/v1/settings"))
        .json(&json!({
            "inference": {
                "prefill_kv_cache": true,
                "timings_per_token": true,
                "top_k": 32,
                "min_p": 0.08,
                "dry_multiplier": 0.8,
                "mirostat": 2
            }
        }))
        .send()
        .await
        .expect("PUT /api/v1/settings structured inference");
    assert_eq!(
        put_resp.status(),
        reqwest::StatusCode::OK,
        "PUT /api/v1/settings with llama.cpp sampling fields must return 200 OK"
    );

    let updated_settings: Value = put_resp.json().await.expect("parse updated settings");
    assert_eq!(updated_settings["inference"]["timings_per_token"], json!(true));
    assert_eq!(updated_settings["inference"]["top_k"], json!(32));
    assert_eq!(updated_settings["inference"]["mirostat"], json!(2));
    assert_eq!(updated_settings["inference"]["prefill_kv_cache"], json!(true));
    assert!(
        (updated_settings["inference"]["min_p"]
            .as_f64()
            .expect("min_p f64")
            - 0.08)
            .abs()
            < 1e-5
    );
    assert!(
        (updated_settings["inference"]["dry_multiplier"]
            .as_f64()
            .expect("dry_multiplier f64")
            - 0.8)
            .abs()
            < 1e-5
    );

    let conv = create_direct_conversation(&harness, "KV Prefill & Sampler Sync", "commander").await;
    harness.mock.clear_recorded();

    send_chat_message(
        &harness,
        &conv.id,
        &SendMessageRequest {
            content: "Verify sampler forwarding and KV prefill.".to_string(),
            thinking_level: Some(ThinkingLevel::Low),
            async_dispatch: false,
            ..Default::default()
        },
    )
    .await;

    let recorded = harness.mock.recorded_requests();
    let chat_req = recorded
        .iter()
        .rfind(|r| r.path == "/v1/chat/completions")
        .expect("recorded /v1/chat/completions request");
    assert_eq!(chat_req.body["timings_per_token"], json!(true));
    assert_eq!(chat_req.body["top_k"], json!(32));
    assert_eq!(chat_req.body["mirostat"], json!(2));
    assert_eq!(chat_req.body["cache_prompt"], json!(true));
    assert!(
        (chat_req.body["min_p"]
            .as_f64()
            .expect("chat request min_p")
            - 0.08)
            .abs()
            < 1e-5
    );
    assert!(
        (chat_req.body["dry_multiplier"]
            .as_f64()
            .expect("chat request dry_multiplier")
            - 0.8)
            .abs()
            < 1e-5
    );

    // Poll up to 2s for asynchronous background KV cache prefill call.
    let deadline = Instant::now() + Duration::from_secs(2);
    let prefill_reqs = loop {
        let prefills = harness.mock.recorded_prefill_requests();
        if !prefills.is_empty() {
            break prefills;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for background KV cache prefill request"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };

    let prefill = &prefill_reqs[0];
    assert_eq!(prefill.path, "/v1/chat/completions");
    assert_eq!(prefill.body["n_predict"], json!(0));
    assert_eq!(prefill.body["cache_prompt"], json!(true));

    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_auto_group_coordinator_and_thinking_progress_notes() {
    let harness = TestHarness::start(
        |mock| {
            mock.set_model_name("qwen3-group-thinking");
            mock.set_props(16384, 4, true, true);
        },
        |_cfg| {},
    )
    .await;

    let mut events_rx = harness.system.events_tx.subscribe();

    // Part A: Group/Auto coordinator with ThinkingLevel::High falls through directly without leaking JSON or </think>.
    let conv_group = create_group_conversation(
        &harness,
        "Auto Group High",
        vec!["commander", "researcher", "programmer"],
    )
    .await;
    harness.mock.clear_recorded();
    let _ = drain_events(&mut events_rx);

    harness.mock.push_scripted(ScriptedResponse {
        content: "internal routing thought\n</think>\n{\"selected_agents\":[]}".to_string(),
        reasoning: None,
        tool_calls: Vec::new(),
        finish_reason: Some("stop".to_string()),
    });
    harness.mock.push_scripted(ScriptedResponse {
        content: "Commander handled your request directly with clean formatting.".to_string(),
        reasoning: Some("Direct Commander reasoning for group turn.".to_string()),
        tool_calls: Vec::new(),
        finish_reason: Some("stop".to_string()),
    });

    send_chat_message(
        &harness,
        &conv_group.id,
        &SendMessageRequest {
            content: "Summarize the project layout without specialist tools.".to_string(),
            thinking_level: Some(ThinkingLevel::High),
            async_dispatch: false,
            ..Default::default()
        },
    )
    .await;

    let group_reqs: Vec<_> = harness
        .mock
        .recorded_requests()
        .into_iter()
        .filter(|r| r.path == "/v1/chat/completions")
        .collect();
    assert_eq!(
        group_reqs.len(),
        2,
        "expected coordinator request + Commander fallthrough request, got {}",
        group_reqs.len()
    );
    assert_eq!(group_reqs[0].body["stream"], json!(false));
    assert_thinking_wire_payload(&group_reqs[0].body, ThinkingLevel::Off);
    assert_eq!(group_reqs[1].body["stream"], json!(true));
    assert_thinking_wire_payload(&group_reqs[1].body, ThinkingLevel::High);

    let group_events = drain_events(&mut events_rx);
    let mut streamed_tokens = String::new();
    let mut completed_content = None;
    for ev in &group_events {
        match ev {
            StreamEvent::ChatToken {
                conversation_id,
                delta,
                ..
            } if conversation_id == &conv_group.id => {
                streamed_tokens.push_str(delta);
            }
            StreamEvent::ChatCompleted {
                conversation_id,
                content,
                ..
            } if conversation_id == &conv_group.id => {
                completed_content = Some(content.clone());
            }
            _ => {}
        }
    }
    let completed = completed_content.expect("ChatCompleted emitted for group conversation");
    assert!(!completed.trim().is_empty());
    assert!(completed.contains("Commander handled your request directly"));
    assert!(!completed.contains("selected_agents"));
    assert!(!completed.contains("{\"selected_agents\":[]}"));
    assert!(!completed.contains("</think>"));
    assert!(!streamed_tokens.contains("selected_agents"));
    assert!(!streamed_tokens.contains("</think>"));

    let group_msgs = list_messages(&harness, &conv_group.id).await;
    let group_assistant = group_msgs
        .iter()
        .rev()
        .find(|m| m.role == ChatRole::Assistant)
        .expect("assistant message persisted in group conversation");
    assert!(!group_assistant.content.trim().is_empty());
    assert!(group_assistant
        .content
        .contains("Commander handled your request directly"));
    assert!(!group_assistant.content.contains("selected_agents"));
    assert!(!group_assistant.content.contains("</think>"));

    // Part B: TaskUpdated progress notes for Off vs High.
    let conv_notes = create_direct_conversation(&harness, "TaskUpdated Notes", "commander").await;
    fn collect_notes_for(
        events_rx: &mut tokio::sync::broadcast::Receiver<StreamEvent>,
        conv_id: &str,
        level: ThinkingLevel,
    ) -> Vec<String> {
        let evs = drain_events(events_rx);
        let notes: Vec<String> = evs
            .into_iter()
            .filter_map(|ev| match ev {
                StreamEvent::TaskUpdated {
                    conversation_id,
                    progress_note,
                    ..
                } if conversation_id.as_deref() == Some(conv_id) => progress_note,
                _ => None,
            })
            .collect();
        for note in &notes {
            assert!(
                !note.contains("1/6") && !note.contains("1/8"),
                "ThinkingLevel::{level:?} must not emit step counters 1/6 or 1/8; got '{note}'"
            );
        }
        notes
    }

    let _ = drain_events(&mut events_rx);
    send_chat_message(
        &harness,
        &conv_notes.id,
        &SendMessageRequest {
            content: "Check TaskUpdated progress note for Off.".to_string(),
            thinking_level: Some(ThinkingLevel::Off),
            async_dispatch: false,
            ..Default::default()
        },
    )
    .await;
    let off_notes = collect_notes_for(&mut events_rx, &conv_notes.id, ThinkingLevel::Off);
    assert!(
        off_notes.iter().any(|n| n == "Generating response…"),
        "ThinkingLevel::Off must emit TaskUpdated note \"Generating response…\", got {off_notes:?}"
    );
    for note in &off_notes {
        assert!(
            !note.contains("Thinking") && !note.to_ascii_lowercase().contains("thinking"),
            "ThinkingLevel::Off note must never show Thinking; got '{note}'"
        );
    }

    let _ = drain_events(&mut events_rx);
    send_chat_message(
        &harness,
        &conv_notes.id,
        &SendMessageRequest {
            content: "Check TaskUpdated progress note for High.".to_string(),
            thinking_level: Some(ThinkingLevel::High),
            async_dispatch: false,
            ..Default::default()
        },
    )
    .await;
    let high_notes = collect_notes_for(&mut events_rx, &conv_notes.id, ThinkingLevel::High);
    assert!(
        high_notes.iter().any(|n| n == "Thinking (high)…"),
        "ThinkingLevel::High must emit TaskUpdated note \"Thinking (high)…\", got {high_notes:?}"
    );

    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_orphan_closing_think_isolates_draft_html_into_reasoning() {
    let harness = TestHarness::start(
        |mock| {
            mock.set_model_name("qwen3-orphan-think");
            mock.set_props(16384, 4, true, true);
        },
        |_cfg| {},
    )
    .await;

    let mut events_rx = harness.system.events_tx.subscribe();
    let conv = create_direct_conversation(&harness, "Orphan Think Draft HTML", "commander").await;
    let _ = drain_events(&mut events_rx);

    let draft_html = "```html\n<!-- DRAFT_PROTOTYPE -->\n<div>draft prototype</div>\n```";
    let final_html = "```html\n<!-- FINAL_RELEASE -->\n<div>final release</div>\n```";
    harness.mock.push_scripted(ScriptedResponse {
        content: format!("{draft_html}\n</think>\n{final_html}"),
        reasoning: None,
        tool_calls: Vec::new(),
        finish_reason: Some("stop".to_string()),
    });

    send_chat_message(
        &harness,
        &conv.id,
        &SendMessageRequest {
            content: "Render final HTML only after finishing thought.".to_string(),
            thinking_level: Some(ThinkingLevel::High),
            async_dispatch: false,
            ..Default::default()
        },
    )
    .await;

    let (completed_content, completed_reasoning) = drain_events(&mut events_rx)
        .into_iter()
        .find_map(|ev| match ev {
            StreamEvent::ChatCompleted {
                conversation_id,
                content,
                reasoning,
                ..
            } if conversation_id == conv.id => Some((content, reasoning)),
            _ => None,
        })
        .expect("StreamEvent::ChatCompleted emitted");

    let assert_isolated = |label: &str, content: &str, reasoning: Option<&str>| {
        let r = reasoning.unwrap_or_else(|| {
            panic!("{label}: reasoning must contain pre-</think> draft HTML")
        });
        assert!(r.contains("<!-- DRAFT_PROTOTYPE -->"), "{label} reasoning missing draft marker: {r}");
        assert!(r.contains("<div>draft prototype</div>"), "{label} reasoning missing draft div: {r}");
        assert!(!r.contains("</think>"), "{label} reasoning leaked </think>: {r}");
        assert!(!r.contains("<!-- FINAL_RELEASE -->"), "{label} reasoning leaked final HTML: {r}");

        assert!(content.contains("<!-- FINAL_RELEASE -->"), "{label} content missing final marker: {content}");
        assert!(content.contains("<div>final release</div>"), "{label} content missing final div: {content}");
        assert!(!content.contains("<!-- DRAFT_PROTOTYPE -->"), "{label} content leaked draft HTML: {content}");
        assert!(!content.contains("draft prototype"), "{label} content leaked draft text: {content}");
        assert!(!content.contains("</think>"), "{label} content leaked </think>: {content}");
    };

    assert_isolated("ChatCompleted", &completed_content, completed_reasoning.as_deref());

    let msgs = list_messages(&harness, &conv.id).await;
    let assistant = msgs
        .iter()
        .rev()
        .find(|m| m.role == ChatRole::Assistant)
        .expect("persisted assistant message");
    assert_isolated("ChatMessage", &assistant.content, assistant.reasoning.as_deref());

    harness.shutdown().await;
}
