use futures_util::future::join_all;
use mock_ps5lm::{start_ephemeral, MockMode, ScriptedResponse, ScriptedToolCall};
use ps5_agent_config::InferenceConfig;
use ps5_agent_core::{
    AgentError, ApiMode, InferenceMessage, InferenceProvider, InferenceRequest,
    InferenceStreamChunk, SystemOneQuestion, SystemOneRequest, ThinkingLevel,
    ToolDefinitionSchema,
};
use ps5_agent_inference::{fold_system_messages, Ps5LmProvider, UNLIMITED_CONCURRENCY_PERMITS};
use serde_json::json;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

fn base_config(base_url: String) -> InferenceConfig {
    InferenceConfig {
        base_url,
        model: "qwen-loaded".into(),
        api_mode: ApiMode::ChatCompletions,
        api_key: Some("test-key".into()),
        timeout_secs: 10,
        connect_timeout_secs: 2,
        context_limit: 8192,
        max_output_tokens: 512,
        stream_enabled: true,
        max_retries: 1,
        ..Default::default()
    }
}

fn msg(role: &str, content: &str) -> InferenceMessage {
    InferenceMessage {
        role: role.into(),
        content: content.into(),
        ..Default::default()
    }
}

fn simple_request(mode: ApiMode, stream: bool) -> InferenceRequest {
    InferenceRequest {
        model: None,
        api_mode: mode,
        messages: vec![msg("system", "Be concise."), msg("user", "Hello PS5")],
        tools: vec![ToolDefinitionSchema {
            name: "calculator".into(),
            description: "Math".into(),
            parameters: json!({"type": "object"}),
        }],
        temperature: 0.2,
        max_output_tokens: 256,
        context_limit: 4096,
        stream,
        thinking_level: ThinkingLevel::Medium,
    }
}

#[tokio::test]
async fn all_three_api_modes_non_streaming_and_streaming() {
    let mock = start_ephemeral().await.expect("start mock");
    let provider = Ps5LmProvider::new(base_config(mock.base_url())).expect("provider");

    // Health + Models
    let health = provider.health_check().await;
    assert!(health.reachable);
    assert_eq!(health.loaded_model.as_deref(), Some("qwen-loaded"));
    let models = provider.list_models().await.expect("list_models");
    assert_eq!(models, vec!["qwen-loaded".to_string()]);

    for mode in [
        ApiMode::ChatCompletions,
        ApiMode::Responses,
        ApiMode::AnthropicMessages,
        ApiMode::Completions,
    ] {
        // Non-streaming
        let resp = provider
            .generate(simple_request(mode, false), CancellationToken::new())
            .await
            .expect("non-streaming generate");
        assert!(resp.content.contains("Hello PS5"));
        assert_eq!(resp.model, "qwen-loaded");

        // Streaming with <think> reasoning + text
        mock.push_scripted(ScriptedResponse {
            content: "Final streamed text.".into(),
            reasoning: Some("thinking step".into()),
            tool_calls: vec![],
            finish_reason: Some("stop".into()),
        });
        let (tx, mut rx) = mpsc::channel(32);
        let streamed = provider
            .generate_stream(simple_request(mode, true), tx, CancellationToken::new())
            .await
            .expect("streaming generate");
        assert_eq!(streamed.content, "Final streamed text.");
        assert_eq!(streamed.reasoning.as_deref(), Some("thinking step"));

        let mut saw_completed = false;
        let mut saw_reasoning = false;
        let mut saw_text = false;
        while let Ok(chunk) = rx.try_recv() {
            match chunk {
                InferenceStreamChunk::ReasoningDelta(_) => saw_reasoning = true,
                InferenceStreamChunk::TextDelta(_) => saw_text = true,
                InferenceStreamChunk::Completed(_) => saw_completed = true,
                InferenceStreamChunk::Error(e) => panic!("unexpected stream error: {e}"),
            }
        }
        assert!(saw_reasoning);
        assert!(saw_text);
        assert!(saw_completed);
    }

    let metrics = provider.metrics();
    assert_eq!(metrics.requests_total, 8);
    assert_eq!(metrics.errors_total, 0);
    assert!(metrics.prompt_tokens_total > 0);
}

#[tokio::test]
async fn systemone_endpoint_and_api_mode_work() {
    let mock = start_ephemeral().await.expect("start mock");
    let provider = Ps5LmProvider::new(base_config(mock.base_url())).expect("provider");

    let mut questions = std::collections::BTreeMap::new();
    questions.insert(
        "intent".into(),
        SystemOneQuestion {
            question_type: "choice".into(),
            instructions: "Pick the action".into(),
            criteria: Some(json!({
                "research": "Search for docs",
                "code": "Write implementation"
            })),
        },
    );
    questions.insert(
        "escalate".into(),
        SystemOneQuestion {
            question_type: "noul".into(),
            instructions: "Needs approval?".into(),
            criteria: None,
        },
    );
    questions.insert(
        "urgency".into(),
        SystemOneQuestion {
            question_type: "score".into(),
            instructions: "Rate urgency".into(),
            criteria: Some(json!(["Low", "Medium", "High"])),
        },
    );

    let s1 = provider
        .system_one(
            SystemOneRequest {
                model: None,
                state: "User wants PS5 kernel offset documentation.".into(),
                questions,
            },
            CancellationToken::new(),
        )
        .await
        .expect("system_one");
    assert_eq!(s1.model, "qwen-loaded");
    assert_eq!(s1.answers["intent"]["type"], "choice");
    assert_eq!(s1.answers["escalate"]["type"], "noul");
    assert_eq!(s1.answers["urgency"]["type"], "score");

    let gen_resp = provider
        .generate(simple_request(ApiMode::SystemOne, false), CancellationToken::new())
        .await
        .expect("systemone via generate");
    let parsed: serde_json::Value = serde_json::from_str(&gen_resp.content).expect("json answers");
    assert!(parsed.get("selected_tool").is_some());
    assert!(parsed.get("invoke_tool").is_some());

    let (tx, mut rx) = mpsc::channel(8);
    let stream_resp = provider
        .generate_stream(simple_request(ApiMode::SystemOne, true), tx, CancellationToken::new())
        .await
        .expect("systemone via generate_stream");
    assert!(!stream_resp.content.is_empty());
    let mut saw_completed = false;
    while let Ok(chunk) = rx.try_recv() {
        if matches!(chunk, InferenceStreamChunk::Completed(_)) {
            saw_completed = true;
        }
    }
    assert!(saw_completed);
}

#[tokio::test]
async fn qwen_system_folding_verified_in_wire_payload() {
    let mock = start_ephemeral().await.expect("start mock");
    let provider = Ps5LmProvider::new(base_config(mock.base_url())).expect("provider");

    let mut req = simple_request(ApiMode::ChatCompletions, false);
    req.messages = vec![
        msg("user", "u1"),
        msg("system", "sys A"),
        msg("developer", "dev B"),
        msg("assistant", "a1"),
        msg("system", "sys C"),
    ];

    let folded = fold_system_messages(&req.messages);
    assert_eq!(folded[0].role, "system");
    assert_eq!(folded[0].content, "sys A\n\ndev B\n\nsys C");

    provider
        .generate(req, CancellationToken::new())
        .await
        .expect("generate");

    let recorded = mock.recorded_requests();
    assert_eq!(recorded.len(), 1);
    let sent_msgs = recorded[0].body["messages"].as_array().expect("messages array");
    assert_eq!(sent_msgs.len(), 3);
    assert_eq!(sent_msgs[0]["role"], "system");
    assert_eq!(sent_msgs[0]["content"], "sys A\n\ndev B\n\nsys C");
    assert_eq!(sent_msgs[1]["role"], "user");
    assert_eq!(sent_msgs[2]["role"], "assistant");
}

#[tokio::test]
async fn three_tier_tool_extraction_over_mock() {
    let mock = start_ephemeral().await.expect("start mock");
    let provider = Ps5LmProvider::new(base_config(mock.base_url())).expect("provider");

    // Tier 1: Standard OpenAI tool_calls
    mock.set_mode(MockMode::ToolCallScripted);
    let r1 = provider
        .generate(
            simple_request(ApiMode::ChatCompletions, false),
            CancellationToken::new(),
        )
        .await
        .expect("tier 1");
    assert_eq!(r1.tool_calls.len(), 1);
    assert_eq!(r1.tool_calls[0].name, "calculator");
    assert_eq!(r1.tool_calls[0].arguments["expression"], "6 * 7");
    assert_eq!(
        r1.reasoning.as_deref(),
        Some("I should use the calculator tool first.")
    );

    // Tier 2: Qwen <tool_call> XML tag
    mock.set_mode(MockMode::Normal);
    mock.push_scripted(ScriptedResponse {
        content: "<think>need weather</think><tool_call>{\"name\":\"http_get\",\"arguments\":{\"url\":\"https://example.com\"}}</tool_call>".into(),
        reasoning: None,
        tool_calls: vec![],
        finish_reason: Some("stop".into()),
    });
    let r2 = provider
        .generate(
            simple_request(ApiMode::ChatCompletions, false),
            CancellationToken::new(),
        )
        .await
        .expect("tier 2");
    assert_eq!(r2.reasoning.as_deref(), Some("need weather"));
    assert_eq!(r2.tool_calls.len(), 1);
    assert_eq!(r2.tool_calls[0].name, "http_get");
    assert_eq!(r2.tool_calls[0].arguments["url"], "https://example.com");

    // Tier 3: Safe structured JSON fallback
    mock.push_scripted(ScriptedResponse {
        content: r#"{"thought":"run calc","tool_calls":[{"name":"calculator","arguments":{"expression":"2+2"}}],"final_answer":""}"#.into(),
        reasoning: None,
        tool_calls: vec![],
        finish_reason: Some("stop".into()),
    });
    let r3 = provider
        .generate(
            simple_request(ApiMode::ChatCompletions, false),
            CancellationToken::new(),
        )
        .await
        .expect("tier 3");
    assert_eq!(r3.reasoning.as_deref(), Some("run calc"));
    assert_eq!(r3.tool_calls.len(), 1);
    assert_eq!(r3.tool_calls[0].name, "calculator");

    // Tier 1 scripted via Responses API as well
    mock.push_scripted(ScriptedResponse {
        content: String::new(),
        reasoning: None,
        tool_calls: vec![ScriptedToolCall {
            id: "call_r1".into(),
            name: "datetime".into(),
            arguments: json!({"action": "now"}),
        }],
        finish_reason: None,
    });
    let r4 = provider
        .generate(
            simple_request(ApiMode::Responses, false),
            CancellationToken::new(),
        )
        .await
        .expect("responses tool call");
    assert_eq!(r4.tool_calls.len(), 1);
    assert_eq!(r4.tool_calls[0].name, "datetime");
}

#[tokio::test]
async fn error_handling_503_malformed_incomplete_sse_timeout_cancel_and_retry() {
    let mock = start_ephemeral().await.expect("start mock");
    let mut cfg = base_config(mock.base_url());
    cfg.timeout_secs = 1;
    let provider = Ps5LmProvider::new(cfg).expect("provider");

    // 503 Unavailable
    mock.set_mode(MockMode::Disconnect503);
    let err_503 = provider
        .generate(
            simple_request(ApiMode::ChatCompletions, false),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert!(matches!(err_503, AgentError::InferenceUnavailable(_)));
    let h = provider.health_check().await;
    assert!(!h.reachable);

    // Malformed JSON
    mock.set_mode(MockMode::MalformedJson);
    let err_json = provider
        .generate(
            simple_request(ApiMode::ChatCompletions, false),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert!(matches!(err_json, AgentError::Inference(_)));

    // Incomplete SSE
    mock.set_mode(MockMode::IncompleteSse);
    let (tx, mut rx) = mpsc::channel(16);
    let err_sse = provider
        .generate_stream(
            simple_request(ApiMode::ChatCompletions, true),
            tx,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert!(matches!(err_sse, AgentError::Inference(_)));
    let mut saw_err_chunk = false;
    while let Ok(c) = rx.try_recv() {
        if matches!(c, InferenceStreamChunk::Error(_)) {
            saw_err_chunk = true;
        }
    }
    assert!(saw_err_chunk);

    // SlowDelay timeout
    mock.set_mode(MockMode::SlowDelay);
    mock.set_delay(Duration::from_millis(2500));
    let err_timeout = provider
        .generate(
            simple_request(ApiMode::ChatCompletions, false),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert!(matches!(err_timeout, AgentError::Timeout(_)));

    // Cancellation
    let mut long_cfg = base_config(mock.base_url());
    long_cfg.timeout_secs = 10;
    provider.update_config(long_cfg).expect("update_config");
    let cancel = CancellationToken::new();
    let cancel_trigger = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        cancel_trigger.cancel();
    });
    let err_cancel = provider
        .generate(simple_request(ApiMode::ChatCompletions, false), cancel)
        .await
        .unwrap_err();
    assert!(matches!(err_cancel, AgentError::Cancelled(_)));

    // Pre-response connection error on closed port
    let closed_port_url = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        format!("http://{addr}/v1")
    };
    let mut dead_cfg = base_config(closed_port_url);
    dead_cfg.connect_timeout_secs = 5;
    dead_cfg.max_retries = 1;
    let dead_provider = Ps5LmProvider::new(dead_cfg).expect("dead provider");
    let err_conn = dead_provider
        .generate(
            simple_request(ApiMode::ChatCompletions, false),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err_conn, AgentError::InferenceUnavailable(_)),
        "expected InferenceUnavailable, got {err_conn:?}"
    );

    assert!(provider.metrics().errors_total >= 5);
}

#[tokio::test]
async fn semaphore_bounds_concurrent_inference_and_updates_dynamically() {
    let mock = start_ephemeral().await.expect("start mock");
    mock.set_mode(MockMode::SlowDelay);
    mock.set_delay(Duration::from_millis(200));

    let provider = std::sync::Arc::new(
        Ps5LmProvider::with_concurrency(base_config(mock.base_url()), 1).expect("provider"),
    );

    let start = std::time::Instant::now();
    let futs = (0..3).map(|_| {
        let p = provider.clone();
        async move {
            p.generate(
                simple_request(ApiMode::ChatCompletions, false),
                CancellationToken::new(),
            )
            .await
        }
    });
    let results = join_all(futs).await;
    assert!(results.iter().all(|r| r.is_ok()));
    // 3 serialized 200ms requests => >= 550ms
    assert!(start.elapsed() >= Duration::from_millis(500));

    provider.set_max_concurrent(3);
    assert_eq!(provider.max_concurrent(), 3);
    let start2 = std::time::Instant::now();
    let futs2 = (0..3).map(|_| {
        let p = provider.clone();
        async move {
            p.generate(
                simple_request(ApiMode::ChatCompletions, false),
                CancellationToken::new(),
            )
            .await
        }
    });
    let results2 = join_all(futs2).await;
    assert!(results2.iter().all(|r| r.is_ok()));
    // 3 parallel 200ms requests => < 500ms
    assert!(start2.elapsed() < Duration::from_millis(500));

    // 0 = unlimited (4096 permits)
    provider.set_max_concurrent(0);
    assert_eq!(provider.max_concurrent(), UNLIMITED_CONCURRENCY_PERMITS);
    let futs3 = (0..3).map(|_| {
        let p = provider.clone();
        async move {
            p.generate(
                simple_request(ApiMode::ChatCompletions, false),
                CancellationToken::new(),
            )
            .await
        }
    });
    let results3 = join_all(futs3).await;
    assert!(results3.iter().all(|r| r.is_ok()));
}
