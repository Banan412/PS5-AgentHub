use axum::{
    body::Body,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use bytes::Bytes;
use futures_util::stream;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::oneshot;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MockMode {
    Normal,
    ToolCallScripted,
    MultiAgentOrchestration,
    SlowDelay,
    Disconnect503,
    MalformedJson,
    IncompleteSse,
}

impl Default for MockMode {
    fn default() -> Self {
        Self::Normal
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScriptedToolCall {
    pub id: String,
    pub name: String,
    pub arguments: Value,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ScriptedResponse {
    pub content: String,
    pub reasoning: Option<String>,
    pub tool_calls: Vec<ScriptedToolCall>,
    pub finish_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct MockKvSampling {
    pub cache_type_k: Option<String>,
    pub cache_type_v: Option<String>,
    pub flash_attn: Option<bool>,
    pub n_keep: i32,
    pub temperature: f32,
    pub top_k: i32,
    pub top_p: f32,
    pub min_p: f32,
    pub repeat_penalty: f32,
    pub repeat_last_n: i32,
    pub presence_penalty: f32,
    pub frequency_penalty: f32,
    pub dry_multiplier: f32,
    pub dry_base: f32,
    pub dry_allowed_length: u32,
    pub dry_penalty_last_n: i32,
    pub xtc_probability: f32,
    pub xtc_threshold: f32,
    pub samplers: Vec<String>,
}

impl Default for MockKvSampling {
    fn default() -> Self {
        Self {
            cache_type_k: Some("q8_0".to_string()),
            cache_type_v: Some("q8_0".to_string()),
            flash_attn: Some(true),
            n_keep: 0,
            temperature: 0.7,
            top_k: 40,
            top_p: 0.95,
            min_p: 0.05,
            repeat_penalty: 1.05,
            repeat_last_n: 256,
            presence_penalty: 0.0,
            frequency_penalty: 0.0,
            dry_multiplier: 0.8,
            dry_base: 1.75,
            dry_allowed_length: 2,
            dry_penalty_last_n: 4096,
            xtc_probability: 0.0,
            xtc_threshold: 0.1,
            samplers: vec![
                "penalties".to_string(),
                "dry".to_string(),
                "top_k".to_string(),
                "typ_p".to_string(),
                "top_p".to_string(),
                "min_p".to_string(),
                "xtc".to_string(),
                "temperature".to_string(),
            ],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct MockTimings {
    pub predicted_per_second: f64,
    pub prompt_per_second: f64,
    pub predicted_ms: f64,
    pub prompt_ms: f64,
    pub predicted_n: u32,
    pub prompt_n: u32,
    /// Optional explicit `cache_n` override. When `None`, derived from shared leading message prefix.
    pub cache_n: Option<u32>,
}

impl Default for MockTimings {
    fn default() -> Self {
        Self {
            predicted_per_second: 118.4,
            prompt_per_second: 420.0,
            predicted_ms: 101.4,
            prompt_ms: 38.1,
            predicted_n: 12,
            prompt_n: 16,
            cache_n: None,
        }
    }
}

struct MockInner {
    mode: MockMode,
    model_name: String,
    model_path: Option<String>,
    delay: Duration,
    n_ctx: u32,
    total_slots: u32,
    supports_vision: bool,
    supports_thinking: bool,
    kv_sampling: MockKvSampling,
    timings: MockTimings,
    scripted: VecDeque<ScriptedResponse>,
    recorded: Vec<RecordedRequest>,
    recorded_prefills: Vec<RecordedRequest>,
}

struct PropsSnapshot {
    mode: MockMode,
    model_name: String,
    model_path: String,
    delay: Duration,
    n_ctx: u32,
    total_slots: u32,
    supports_vision: bool,
    supports_thinking: bool,
    kv_sampling: MockKvSampling,
}

struct RequestSnapshot {
    mode: MockMode,
    model_name: String,
    delay: Duration,
    supports_thinking: bool,
    scripted: Option<ScriptedResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordedRequest {
    pub path: String,
    pub body: Value,
}

impl RecordedRequest {
    pub fn id_slot(&self) -> Option<i64> {
        self.body.get("id_slot").and_then(Value::as_i64)
    }

    pub fn stream_options(&self) -> Option<&Value> {
        self.body.get("stream_options")
    }

    pub fn has_field(&self, field: &str) -> bool {
        self.body.get(field).is_some()
    }

    pub fn field(&self, field: &str) -> Option<&Value> {
        self.body.get(field)
    }

    /// Collects present sampling / llama.cpp telemetry-related keys recorded in `body`.
    pub fn sampling_fields(&self) -> serde_json::Map<String, Value> {
        const KEYS: &[&str] = &[
            "temperature",
            "top_k",
            "top_p",
            "min_p",
            "typical_p",
            "repeat_penalty",
            "repeat_last_n",
            "presence_penalty",
            "frequency_penalty",
            "dry_multiplier",
            "dry_base",
            "dry_allowed_length",
            "dry_penalty_last_n",
            "xtc_probability",
            "xtc_threshold",
            "samplers",
            "n_keep",
            "seed",
            "id_slot",
            "stream_options",
            "stop",
            "cache_prompt",
            "timings_per_token",
        ];
        let mut out = serde_json::Map::new();
        for &key in KEYS {
            if let Some(val) = self.body.get(key) {
                out.insert(key.to_string(), val.clone());
            }
        }
        out
    }
}

#[derive(Clone)]
pub struct MockState {
    inner: Arc<Mutex<MockInner>>,
}

impl MockState {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(MockInner {
                mode: MockMode::Normal,
                model_name: "qwen-loaded".to_string(),
                model_path: None,
                delay: Duration::from_secs(2),
                n_ctx: 16384,
                total_slots: 4,
                supports_vision: true,
                supports_thinking: true,
                kv_sampling: MockKvSampling::default(),
                timings: MockTimings::default(),
                scripted: VecDeque::new(),
                recorded: Vec::new(),
                recorded_prefills: Vec::new(),
            })),
        }
    }

    pub fn set_mode(&self, mode: MockMode) {
        if let Ok(mut g) = self.inner.lock() {
            g.mode = mode;
        }
    }

    pub fn mode(&self) -> MockMode {
        self.inner.lock().map(|g| g.mode).unwrap_or(MockMode::Normal)
    }

    pub fn set_delay(&self, delay: Duration) {
        if let Ok(mut g) = self.inner.lock() {
            g.delay = delay;
        }
    }

    pub fn set_model_name(&self, name: impl Into<String>) {
        if let Ok(mut g) = self.inner.lock() {
            g.model_name = name.into();
        }
    }

    pub fn set_model_path(&self, path: impl Into<String>) {
        if let Ok(mut g) = self.inner.lock() {
            g.model_path = Some(path.into());
        }
    }

    pub fn set_props(
        &self,
        n_ctx: u32,
        total_slots: u32,
        supports_vision: bool,
        supports_thinking: bool,
    ) {
        if let Ok(mut g) = self.inner.lock() {
            g.n_ctx = n_ctx;
            g.total_slots = total_slots;
            g.supports_vision = supports_vision;
            g.supports_thinking = supports_thinking;
        }
    }

    pub fn set_n_ctx(&self, n_ctx: u32) {
        if let Ok(mut g) = self.inner.lock() {
            g.n_ctx = n_ctx;
        }
    }

    pub fn set_total_slots(&self, total_slots: u32) {
        if let Ok(mut g) = self.inner.lock() {
            g.total_slots = total_slots;
        }
    }

    pub fn set_supports_vision(&self, supports_vision: bool) {
        if let Ok(mut g) = self.inner.lock() {
            g.supports_vision = supports_vision;
        }
    }

    pub fn set_supports_thinking(&self, supports_thinking: bool) {
        if let Ok(mut g) = self.inner.lock() {
            g.supports_thinking = supports_thinking;
        }
    }

    pub fn n_ctx(&self) -> u32 {
        self.inner.lock().map(|g| g.n_ctx).unwrap_or(16384)
    }

    pub fn total_slots(&self) -> u32 {
        self.inner.lock().map(|g| g.total_slots).unwrap_or(4)
    }

    pub fn supports_vision(&self) -> bool {
        self.inner.lock().map(|g| g.supports_vision).unwrap_or(true)
    }

    pub fn supports_thinking(&self) -> bool {
        self.inner.lock().map(|g| g.supports_thinking).unwrap_or(true)
    }

    pub fn set_mock_kv_sampling(&self, kv_sampling: MockKvSampling) {
        if let Ok(mut g) = self.inner.lock() {
            g.kv_sampling = kv_sampling;
        }
    }

    pub fn mock_kv_sampling(&self) -> MockKvSampling {
        self.inner
            .lock()
            .map(|g| g.kv_sampling.clone())
            .unwrap_or_default()
    }

    pub fn set_kv_cache(
        &self,
        cache_type_k: Option<String>,
        cache_type_v: Option<String>,
        flash_attn: Option<bool>,
    ) {
        if let Ok(mut g) = self.inner.lock() {
            g.kv_sampling.cache_type_k = cache_type_k;
            g.kv_sampling.cache_type_v = cache_type_v;
            g.kv_sampling.flash_attn = flash_attn;
        }
    }

    pub fn set_mock_timings(&self, timings: MockTimings) {
        if let Ok(mut g) = self.inner.lock() {
            g.timings = timings;
        }
    }

    pub fn mock_timings(&self) -> MockTimings {
        self.inner
            .lock()
            .map(|g| g.timings.clone())
            .unwrap_or_default()
    }

    pub fn push_scripted(&self, resp: ScriptedResponse) {
        if let Ok(mut g) = self.inner.lock() {
            g.scripted.push_back(resp);
        }
    }

    pub fn clear_scripted(&self) {
        if let Ok(mut g) = self.inner.lock() {
            g.scripted.clear();
        }
    }

    pub fn recorded_requests(&self) -> Vec<RecordedRequest> {
        self.inner
            .lock()
            .map(|g| g.recorded.clone())
            .unwrap_or_default()
    }

    pub fn recorded_prefill_requests(&self) -> Vec<RecordedRequest> {
        self.inner
            .lock()
            .map(|g| g.recorded_prefills.clone())
            .unwrap_or_default()
    }

    pub fn clear_recorded(&self) {
        if let Ok(mut g) = self.inner.lock() {
            g.recorded.clear();
            g.recorded_prefills.clear();
        }
    }

    fn push_recorded(&self, path: &str, body: Value, is_prefill: bool) {
        if let Ok(mut g) = self.inner.lock() {
            let entry = RecordedRequest {
                path: path.to_string(),
                body,
            };
            if is_prefill {
                g.recorded_prefills.push(entry);
            } else {
                g.recorded.push(entry);
            }
        }
    }

    fn record(&self, path: &str, body: Value) {
        self.push_recorded(path, body, false);
    }

    fn record_prefill(&self, path: &str, body: Value) {
        self.push_recorded(path, body, true);
    }

    fn snapshot_props(&self) -> PropsSnapshot {
        let Ok(g) = self.inner.lock() else {
            return PropsSnapshot {
                mode: MockMode::Normal,
                model_name: "qwen-loaded".to_string(),
                model_path: "/data/models/qwen-loaded.gguf".to_string(),
                delay: Duration::from_secs(2),
                n_ctx: 16384,
                total_slots: 4,
                supports_vision: true,
                supports_thinking: true,
                kv_sampling: MockKvSampling::default(),
            };
        };
        let model_path = g
            .model_path
            .clone()
            .unwrap_or_else(|| format!("/data/models/{}.gguf", g.model_name));
        PropsSnapshot {
            mode: g.mode,
            model_name: g.model_name.clone(),
            model_path,
            delay: g.delay,
            n_ctx: g.n_ctx,
            total_slots: g.total_slots,
            supports_vision: g.supports_vision,
            supports_thinking: g.supports_thinking,
            kv_sampling: g.kv_sampling.clone(),
        }
    }

    fn snapshot_and_pop(&self) -> RequestSnapshot {
        let Ok(mut g) = self.inner.lock() else {
            return RequestSnapshot {
                mode: MockMode::Normal,
                model_name: "qwen-loaded".into(),
                delay: Duration::from_secs(2),
                supports_thinking: true,
                scripted: None,
            };
        };
        let scripted = if matches!(g.mode, MockMode::Disconnect503 | MockMode::MalformedJson) {
            None
        } else {
            g.scripted.pop_front()
        };
        RequestSnapshot {
            mode: g.mode,
            model_name: g.model_name.clone(),
            delay: g.delay,
            supports_thinking: g.supports_thinking,
            scripted,
        }
    }

    fn resolve_chat_timings_json(&self, body: &Value, is_prefill: bool) -> Value {
        let (cfg, auto_cache_n) = {
            let Ok(g) = self.inner.lock() else {
                let d = MockTimings::default();
                return build_timings_json(&d, d.cache_n.unwrap_or(0), is_prefill);
            };
            let auto_cache_n = if g.timings.cache_n.is_none() {
                let prior_recorded = if !is_prefill && !g.recorded.is_empty() {
                    &g.recorded[..g.recorded.len() - 1]
                } else {
                    &g.recorded[..]
                };
                let prior_prefills = if is_prefill && !g.recorded_prefills.is_empty() {
                    &g.recorded_prefills[..g.recorded_prefills.len() - 1]
                } else {
                    &g.recorded_prefills[..]
                };
                estimate_shared_prefix_tokens(prior_recorded, prior_prefills, body)
            } else {
                0
            };
            (g.timings.clone(), auto_cache_n)
        };
        let cache_n = cfg.cache_n.unwrap_or(auto_cache_n);
        build_timings_json(&cfg, cache_n, is_prefill)
    }
}

impl Default for MockState {
    fn default() -> Self {
        Self::new()
    }
}

pub struct MockPs5LmHandle {
    addr: SocketAddr,
    state: MockState,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl MockPs5LmHandle {
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Returns `http://127.0.0.1:<port>/v1`
    pub fn base_url(&self) -> String {
        format!("http://{}/v1", self.addr)
    }

    /// Returns `http://127.0.0.1:<port>`
    pub fn root_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub fn state(&self) -> &MockState {
        &self.state
    }

    pub fn set_mode(&self, mode: MockMode) {
        self.state.set_mode(mode);
    }

    pub fn set_delay(&self, delay: Duration) {
        self.state.set_delay(delay);
    }

    pub fn set_model_name(&self, name: impl Into<String>) {
        self.state.set_model_name(name);
    }

    pub fn set_model_path(&self, path: impl Into<String>) {
        self.state.set_model_path(path);
    }

    pub fn set_props(
        &self,
        n_ctx: u32,
        total_slots: u32,
        supports_vision: bool,
        supports_thinking: bool,
    ) {
        self.state
            .set_props(n_ctx, total_slots, supports_vision, supports_thinking);
    }

    pub fn set_supports_vision(&self, supports_vision: bool) {
        self.state.set_supports_vision(supports_vision);
    }

    pub fn set_supports_thinking(&self, supports_thinking: bool) {
        self.state.set_supports_thinking(supports_thinking);
    }

    pub fn set_mock_kv_sampling(&self, kv_sampling: MockKvSampling) {
        self.state.set_mock_kv_sampling(kv_sampling);
    }

    pub fn mock_kv_sampling(&self) -> MockKvSampling {
        self.state.mock_kv_sampling()
    }

    pub fn set_kv_cache(
        &self,
        cache_type_k: Option<String>,
        cache_type_v: Option<String>,
        flash_attn: Option<bool>,
    ) {
        self.state
            .set_kv_cache(cache_type_k, cache_type_v, flash_attn);
    }

    pub fn set_mock_timings(&self, timings: MockTimings) {
        self.state.set_mock_timings(timings);
    }

    pub fn mock_timings(&self) -> MockTimings {
        self.state.mock_timings()
    }

    pub fn push_scripted(&self, resp: ScriptedResponse) {
        self.state.push_scripted(resp);
    }

    pub fn clear_scripted(&self) {
        self.state.clear_scripted();
    }

    pub fn recorded_requests(&self) -> Vec<RecordedRequest> {
        self.state.recorded_requests()
    }

    pub fn recorded_prefill_requests(&self) -> Vec<RecordedRequest> {
        self.state.recorded_prefill_requests()
    }

    pub fn clear_recorded(&self) {
        self.state.clear_recorded();
    }

    pub fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

impl Drop for MockPs5LmHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

pub fn build_router(state: MockState) -> Router {
    Router::new()
        .route("/health", get(handle_health))
        .route("/props", get(handle_props))
        .route("/slots", get(handle_slots))
        .route("/v1/models", get(handle_models))
        .route("/v1/chat/completions", post(handle_chat_completions))
        .route("/v1/responses", post(handle_responses))
        .route("/v1/messages", post(handle_anthropic_messages))
        .route("/v1/completions", post(handle_completions))
        .route("/v1/systemone", post(handle_systemone))
        .with_state(state)
}

pub async fn start_ephemeral() -> std::io::Result<MockPs5LmHandle> {
    start_on(SocketAddr::from(([127, 0, 0, 1], 0))).await
}

pub async fn start_on(bind_addr: SocketAddr) -> std::io::Result<MockPs5LmHandle> {
    let state = MockState::new();
    let app = build_router(state.clone());
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    let addr = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    tokio::spawn(async move {
        let server = axum::serve(listener, app).with_graceful_shutdown(async move {
            let _ = shutdown_rx.await;
        });
        let _ = server.await;
    });

    Ok(MockPs5LmHandle {
        addr,
        state,
        shutdown_tx: Some(shutdown_tx),
    })
}

fn unavailable_503() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({
            "error": {
                "message": "model loading or temporarily unavailable",
                "type": "service_unavailable",
                "code": 503
            }
        })),
    )
        .into_response()
}

fn malformed_json_response() -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(Body::from("{not_valid_json: [truncated"))
        .unwrap()
}

async fn handle_health(State(state): State<MockState>) -> Response {
    let snap = state.snapshot_props();
    match snap.mode {
        MockMode::Disconnect503 => unavailable_503(),
        MockMode::MalformedJson => malformed_json_response(),
        MockMode::SlowDelay => {
            tokio::time::sleep(snap.delay).await;
            Json(json!({
                "status": "ok",
                "version": "0.11.0",
                "loaded_model": snap.model_name
            }))
            .into_response()
        }
        _ => Json(json!({
            "status": "ok",
            "version": "0.11.0",
            "loaded_model": snap.model_name
        }))
        .into_response(),
    }
}

fn sampling_params_value(kv: &MockKvSampling) -> Value {
    json!({
        "n_keep": kv.n_keep,
        "temperature": kv.temperature,
        "top_k": kv.top_k,
        "top_p": kv.top_p,
        "min_p": kv.min_p,
        "repeat_penalty": kv.repeat_penalty,
        "repeat_last_n": kv.repeat_last_n,
        "presence_penalty": kv.presence_penalty,
        "frequency_penalty": kv.frequency_penalty,
        "dry_multiplier": kv.dry_multiplier,
        "dry_base": kv.dry_base,
        "dry_allowed_length": kv.dry_allowed_length,
        "dry_penalty_last_n": kv.dry_penalty_last_n,
        "xtc_probability": kv.xtc_probability,
        "xtc_threshold": kv.xtc_threshold,
        "samplers": kv.samplers,
    })
}

fn insert_kv_metadata(map: &mut serde_json::Map<String, Value>, kv: &MockKvSampling) {
    if let Some(ref k) = kv.cache_type_k {
        map.insert("cache_type_k".into(), json!(k));
    }
    if let Some(ref v) = kv.cache_type_v {
        map.insert("cache_type_v".into(), json!(v));
    }
    if let Some(fa) = kv.flash_attn {
        map.insert("flash_attn".into(), json!(fa));
    }
}

fn build_timings_json(cfg: &MockTimings, cache_n: u32, is_prefill: bool) -> Value {
    if is_prefill {
        json!({
            "predicted_per_second": 0.0,
            "prompt_per_second": cfg.prompt_per_second,
            "predicted_ms": 0.0,
            "prompt_ms": cfg.prompt_ms,
            "predicted_n": 0,
            "prompt_n": cfg.prompt_n,
            "cache_n": cache_n
        })
    } else {
        json!({
            "predicted_per_second": cfg.predicted_per_second,
            "prompt_per_second": cfg.prompt_per_second,
            "predicted_ms": cfg.predicted_ms,
            "prompt_ms": cfg.prompt_ms,
            "predicted_n": cfg.predicted_n,
            "prompt_n": cfg.prompt_n,
            "cache_n": cache_n
        })
    }
}

fn chat_message_prefix_signature(msg: &Value) -> Option<(String, String)> {
    let role = msg.get("role")?.as_str()?.to_string();
    let mut sig = serde_json::Map::new();
    sig.insert("role".into(), json!(role));
    if let Some(c) = msg.get("content") {
        sig.insert("content".into(), c.clone());
    }
    if let Some(tc) = msg.get("tool_calls") {
        sig.insert("tool_calls".into(), tc.clone());
    }
    if let Some(id) = msg.get("tool_call_id") {
        sig.insert("tool_call_id".into(), id.clone());
    }
    Some((role, Value::Object(sig).to_string()))
}

fn estimate_shared_prefix_tokens(
    prior_recorded: &[RecordedRequest],
    prior_prefills: &[RecordedRequest],
    current_body: &Value,
) -> u32 {
    let Some(cur_msgs_raw) = current_body.get("messages").and_then(Value::as_array) else {
        return 0;
    };
    let cur_msgs: Vec<(String, String)> = cur_msgs_raw
        .iter()
        .filter_map(chat_message_prefix_signature)
        .collect();
    if cur_msgs.is_empty() {
        return 0;
    }
    let cur_slot = current_body.get("id_slot").and_then(Value::as_i64);
    let mut best_shared_bytes = 0usize;

    for prev in prior_recorded.iter().chain(prior_prefills.iter()) {
        if prev.path != "/v1/chat/completions" {
            continue;
        }
        let prev_slot = prev.body.get("id_slot").and_then(Value::as_i64);
        if let (Some(a), Some(b)) = (prev_slot, cur_slot) {
            if a != b {
                continue;
            }
        }
        let Some(prev_msgs_raw) = prev.body.get("messages").and_then(Value::as_array) else {
            continue;
        };
        let prev_msgs: Vec<(String, String)> = prev_msgs_raw
            .iter()
            .filter_map(chat_message_prefix_signature)
            .collect();
        let mut shared_bytes = 0usize;
        for (a, b) in prev_msgs.iter().zip(cur_msgs.iter()) {
            if a == b {
                shared_bytes = shared_bytes.saturating_add(a.1.len());
            } else {
                break;
            }
        }
        if shared_bytes > best_shared_bytes {
            best_shared_bytes = shared_bytes;
        }
    }

    if best_shared_bytes == 0 {
        0
    } else {
        ((best_shared_bytes / 4).clamp(16, 1_000_000)) as u32
    }
}

fn props_json(snap: &PropsSnapshot) -> Json<Value> {
    let params = sampling_params_value(&snap.kv_sampling);
    let mut kv_obj = serde_json::Map::new();
    insert_kv_metadata(&mut kv_obj, &snap.kv_sampling);

    let mut root = serde_json::Map::new();
    root.insert(
        "default_generation_settings".into(),
        json!({
            "n_ctx": snap.n_ctx,
            "params": params
        }),
    );
    root.insert("total_slots".into(), json!(snap.total_slots));
    root.insert(
        "modalities".into(),
        json!({
            "vision": snap.supports_vision,
            "audio": false
        }),
    );
    root.insert(
        "chat_template_caps".into(),
        json!({
            "supports_thinking": snap.supports_thinking
        }),
    );
    root.insert("model_alias".into(), json!(snap.model_name));
    root.insert("model_path".into(), json!(snap.model_path));
    insert_kv_metadata(&mut root, &snap.kv_sampling);
    if !kv_obj.is_empty() {
        root.insert("kv_cache".into(), Value::Object(kv_obj));
    }
    Json(Value::Object(root))
}

async fn handle_props(State(state): State<MockState>) -> Response {
    let snap = state.snapshot_props();
    match snap.mode {
        MockMode::Disconnect503 => unavailable_503(),
        MockMode::MalformedJson => malformed_json_response(),
        MockMode::SlowDelay => {
            tokio::time::sleep(snap.delay).await;
            props_json(&snap).into_response()
        }
        _ => props_json(&snap).into_response(),
    }
}

fn slots_json(snap: &PropsSnapshot) -> Json<Value> {
    let params = sampling_params_value(&snap.kv_sampling);
    let slots: Vec<Value> = (0..snap.total_slots)
        .map(|i| {
            let mut slot = serde_json::Map::new();
            slot.insert("id".into(), json!(i));
            slot.insert("n_ctx".into(), json!(snap.n_ctx));
            slot.insert("is_processing".into(), json!(false));
            slot.insert("params".into(), params.clone());
            insert_kv_metadata(&mut slot, &snap.kv_sampling);
            Value::Object(slot)
        })
        .collect();
    Json(Value::Array(slots))
}

async fn handle_slots(State(state): State<MockState>) -> Response {
    let snap = state.snapshot_props();
    match snap.mode {
        MockMode::Disconnect503 => unavailable_503(),
        MockMode::MalformedJson => malformed_json_response(),
        MockMode::SlowDelay => {
            tokio::time::sleep(snap.delay).await;
            slots_json(&snap).into_response()
        }
        _ => slots_json(&snap).into_response(),
    }
}

async fn handle_models(State(state): State<MockState>) -> Response {
    let snap = state.snapshot_props();
    match snap.mode {
        MockMode::Disconnect503 => unavailable_503(),
        MockMode::MalformedJson => malformed_json_response(),
        MockMode::SlowDelay => {
            tokio::time::sleep(snap.delay).await;
            Json(json!({
                "object": "list",
                "data": [{ "id": snap.model_name, "object": "model", "owned_by": "ps5lm" }]
            }))
            .into_response()
        }
        _ => Json(json!({
            "object": "list",
            "data": [{ "id": snap.model_name, "object": "model", "owned_by": "ps5lm" }]
        }))
        .into_response(),
    }
}

fn has_tool_result(body: &Value) -> bool {
    if let Some(msgs) = body.get("messages").and_then(|v| v.as_array()) {
        for m in msgs {
            if m.get("role").and_then(|r| r.as_str()) == Some("tool") {
                return true;
            }
            if let Some(parts) = m.get("content").and_then(|c| c.as_array()) {
                if parts
                    .iter()
                    .any(|p| p.get("type").and_then(|t| t.as_str()) == Some("tool_result"))
                {
                    return true;
                }
            }
        }
    }
    if let Some(items) = body.get("input").and_then(|v| v.as_array()) {
        for item in items {
            if matches!(
                item.get("type").and_then(|t| t.as_str()),
                Some("function_call_output") | Some("tool_result")
            ) {
                return true;
            }
        }
    }
    if let Some(prompt) = body.get("prompt").and_then(|v| v.as_str()) {
        if prompt.contains("<|im_start|>tool") {
            return true;
        }
    }
    false
}

fn extract_content_text(content: &Value) -> Option<String> {
    if let Some(s) = content.as_str() {
        return Some(s.to_string());
    }
    if let Some(parts) = content.as_array() {
        let mut texts = Vec::new();
        for part in parts {
            if let Some(s) = part.as_str() {
                texts.push(s);
                continue;
            }
            let kind = part.get("type").and_then(|t| t.as_str());
            if matches!(kind, Some("text" | "input_text") | None) {
                if let Some(txt) = part.get("text").and_then(|t| t.as_str()) {
                    texts.push(txt);
                }
            }
        }
        if !texts.is_empty() {
            return Some(texts.join("\n"));
        }
    }
    None
}

fn last_user_text(body: &Value) -> String {
    if let Some(msgs) = body.get("messages").and_then(|v| v.as_array()) {
        for m in msgs.iter().rev() {
            if m.get("role").and_then(|r| r.as_str()) == Some("user") {
                if let Some(s) = m.get("content").and_then(extract_content_text) {
                    return s;
                }
            }
        }
    }
    if let Some(prompt) = body.get("prompt").and_then(|v| v.as_str()) {
        if let Some((_, tail)) = prompt.rsplit_once("<|im_start|>user\n") {
            let clean = tail.split("<|im_end|>").next().unwrap_or(tail).trim();
            if !clean.is_empty() {
                return clean.to_string();
            }
        }
        return prompt.to_string();
    }
    if let Some(st) = body.get("state").and_then(|v| v.as_str()) {
        return st.to_string();
    }
    if let Some(s) = body.get("input").and_then(|v| v.as_str()) {
        return s.to_string();
    }
    if let Some(items) = body.get("input").and_then(|v| v.as_array()) {
        for item in items.iter().rev() {
            if item.get("role").and_then(|r| r.as_str()) == Some("user") {
                if let Some(s) = item.get("content").and_then(extract_content_text) {
                    return s;
                }
            }
        }
        if let Some(s) = body.get("input").and_then(extract_content_text) {
            return s;
        }
    }
    "hello".to_string()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestThinking {
    Disabled,
    Requested(&'static str),
    Unspecified,
}

fn inspect_request_thinking(body: &Value) -> RequestThinking {
    let enable_thinking = body
        .get("chat_template_kwargs")
        .and_then(|k| k.get("enable_thinking"))
        .and_then(|v| v.as_bool());
    if enable_thinking == Some(false) {
        return RequestThinking::Disabled;
    }

    if let Some(effort) = body.get("reasoning_effort").and_then(|v| v.as_str()) {
        return match effort.trim().to_ascii_lowercase().as_str() {
            "none" | "off" | "false" => RequestThinking::Disabled,
            "low" => RequestThinking::Requested("low"),
            "medium" => RequestThinking::Requested("medium"),
            "high" => {
                let is_max = body
                    .get("reasoning_budget")
                    .and_then(|v| v.as_i64())
                    .is_some_and(|b| b < 0)
                    || body
                        .get("thinking_budget_tokens")
                        .and_then(|v| v.as_u64())
                        .is_some_and(|b| b >= 16384);
                RequestThinking::Requested(if is_max { "max" } else { "high" })
            }
            "max" | "xhigh" => RequestThinking::Requested("max"),
            s if !s.is_empty() => RequestThinking::Requested("enabled"),
            _ => RequestThinking::Unspecified,
        };
    }

    if let Some(reasoning) = body.get("reasoning") {
        if let Some(effort) = reasoning.get("effort").and_then(|v| v.as_str()) {
            return match effort.trim().to_ascii_lowercase().as_str() {
                "none" | "off" | "false" => RequestThinking::Disabled,
                "low" => RequestThinking::Requested("low"),
                "medium" => RequestThinking::Requested("medium"),
                "high" => RequestThinking::Requested("high"),
                "max" | "xhigh" => RequestThinking::Requested("max"),
                s if !s.is_empty() => RequestThinking::Requested("enabled"),
                _ => RequestThinking::Unspecified,
            };
        }
        if reasoning.is_object() {
            return RequestThinking::Requested("medium");
        }
    }

    if let Some(thinking) = body.get("thinking") {
        if thinking.as_bool() == Some(false) {
            return RequestThinking::Disabled;
        }
        let budget = thinking.get("budget_tokens").and_then(|v| v.as_u64());
        if let Some(kind) = thinking.get("type").and_then(|v| v.as_str()) {
            return match kind.trim().to_ascii_lowercase().as_str() {
                "disabled" | "none" | "off" => RequestThinking::Disabled,
                "enabled" => budget_tokens_to_level(budget),
                _ => RequestThinking::Unspecified,
            };
        }
        if let Some(enabled) = thinking.get("enabled").and_then(|v| v.as_bool()) {
            return if enabled {
                budget_tokens_to_level(budget)
            } else {
                RequestThinking::Disabled
            };
        }
    }

    if enable_thinking == Some(true) {
        return RequestThinking::Requested("enabled");
    }

    if body
        .get("prompt")
        .and_then(|v| v.as_str())
        .is_some_and(prompt_has_no_think_directive)
    {
        return RequestThinking::Disabled;
    }

    RequestThinking::Unspecified
}

fn budget_tokens_to_level(budget: Option<u64>) -> RequestThinking {
    match budget {
        Some(0) => RequestThinking::Disabled,
        Some(b) if b <= 1024 => RequestThinking::Requested("low"),
        Some(b) if b <= 4096 => RequestThinking::Requested("medium"),
        Some(b) if b <= 8192 => RequestThinking::Requested("high"),
        Some(_) => RequestThinking::Requested("max"),
        None => RequestThinking::Requested("enabled"),
    }
}

fn prompt_has_no_think_directive(prompt: &str) -> bool {
    for turn in prompt.split("<|im_end|>") {
        for line in turn.lines() {
            let trimmed = line.trim();
            if trimmed == "/no_think" || trimmed.ends_with(" /no_think") {
                return true;
            }
        }
    }
    false
}

fn is_prefill_request(body: &Value) -> bool {
    body.get("n_predict").and_then(|v| v.as_i64()) == Some(0)
        || body.get("max_tokens").and_then(|v| v.as_i64()) == Some(0)
}

fn has_structured_reasoning_format(body: &Value) -> bool {
    match body.get("reasoning_format").and_then(|v| v.as_str()) {
        Some(rf) => {
            let trimmed = rf.trim();
            !trimmed.is_empty() && !trimmed.eq_ignore_ascii_case("none")
        }
        None => false,
    }
}

fn anthropic_wants_thinking_block(body: &Value) -> bool {
    let explicit_none = body
        .get("reasoning_format")
        .and_then(|v| v.as_str())
        .is_some_and(|rf| rf.trim().eq_ignore_ascii_case("none"));
    if explicit_none {
        return false;
    }
    if has_structured_reasoning_format(body) {
        return true;
    }
    match body.get("thinking") {
        Some(t) => {
            let kind_enabled = t
                .get("type")
                .and_then(|v| v.as_str())
                .is_some_and(|s| s.trim().eq_ignore_ascii_case("enabled"));
            let flag_enabled = t.get("enabled").and_then(|v| v.as_bool()) == Some(true)
                || t.as_bool() == Some(true);
            kind_enabled || flag_enabled
        }
        None => false,
    }
}

fn with_think_prefix(content: &str, reasoning: Option<&str>) -> String {
    match reasoning {
        Some(r) if !r.is_empty() && !content.contains("<think>") => {
            format!("<think>{r}</think>{content}")
        }
        _ => content.to_string(),
    }
}

fn is_specialist_without_delegation_tools(body: &Value) -> bool {
    if let Some(tools) = body.get("tools").and_then(|v| v.as_array()) {
        if tools.is_empty() {
            return false;
        }
        let has_delegation = tools.iter().any(|t| {
            let name = t
                .get("function")
                .and_then(|f| f.get("name"))
                .and_then(|n| n.as_str())
                .or_else(|| t.get("name").and_then(|n| n.as_str()));
            matches!(name, Some("assign_agent_task") | Some("delegate_task"))
        });
        return !has_delegation;
    }

    if let Some(prompt) = body.get("prompt").and_then(|v| v.as_str()) {
        if let Some((_, after)) = prompt.split_once("<tools>") {
            let section = after.split("</tools>").next().unwrap_or(after).trim();
            if !section.is_empty() {
                return !section.contains("assign_agent_task")
                    && !section.contains("delegate_task");
            }
        }
        const TOOLS_MARKER: &str =
            "Available tools (invoke using <tool_call>{\"name\": \"...\", \"arguments\": {...}}</tool_call>):";
        if let Some((_, after)) = prompt.split_once(TOOLS_MARKER) {
            let section = after.split("<|im_end|>").next().unwrap_or(after).trim();
            if !section.is_empty() {
                return !section.contains("assign_agent_task")
                    && !section.contains("delegate_task");
            }
        }
    }

    false
}

fn resolve_scripted_plan(
    mode: MockMode,
    scripted: Option<ScriptedResponse>,
    body: &Value,
    supports_thinking: bool,
) -> ScriptedResponse {
    let req_thinking = inspect_request_thinking(body);
    let thinking_disabled = req_thinking == RequestThinking::Disabled;

    if let Some(mut s) = scripted {
        if thinking_disabled {
            s.reasoning = None;
        }
        return s;
    }
    let tool_done = has_tool_result(body);
    match mode {
        MockMode::ToolCallScripted => {
            if !tool_done {
                ScriptedResponse {
                    content: String::new(),
                    reasoning: if thinking_disabled || !supports_thinking {
                        None
                    } else {
                        Some("I should use the calculator tool first.".to_string())
                    },
                    tool_calls: vec![ScriptedToolCall {
                        id: "call_mock_1".to_string(),
                        name: "calculator".to_string(),
                        arguments: json!({"expression": "6 * 7"}),
                    }],
                    finish_reason: Some("tool_calls".to_string()),
                }
            } else {
                ScriptedResponse {
                    content: "The tool returned 42.".to_string(),
                    reasoning: None,
                    tool_calls: Vec::new(),
                    finish_reason: Some("stop".to_string()),
                }
            }
        }
        MockMode::MultiAgentOrchestration => {
            if is_specialist_without_delegation_tools(body) {
                ScriptedResponse {
                    content: "Specialist research summary completed for Commander.".into(),
                    reasoning: None,
                    tool_calls: Vec::new(),
                    finish_reason: Some("stop".into()),
                }
            } else if !tool_done {
                ScriptedResponse {
                    content: String::new(),
                    reasoning: if thinking_disabled || !supports_thinking {
                        None
                    } else {
                        Some("Delegating sub-task to specialist agent.".to_string())
                    },
                    tool_calls: vec![ScriptedToolCall {
                        id: "call_orch_1".to_string(),
                        name: "assign_agent_task".to_string(),
                        arguments: json!({
                            "target_agent_id": "researcher",
                            "user_request": "Gather summary facts for the user query."
                        }),
                    }],
                    finish_reason: Some("tool_calls".to_string()),
                }
            } else {
                ScriptedResponse {
                    content: "Multi-agent orchestration completed successfully.".to_string(),
                    reasoning: None,
                    tool_calls: Vec::new(),
                    finish_reason: Some("stop".to_string()),
                }
            }
        }
        _ => {
            let prompt = last_user_text(body);
            let default_reasoning = match req_thinking {
                RequestThinking::Requested(level) if supports_thinking => {
                    Some(format!("Reasoning ({level}) for: {prompt}"))
                }
                _ => None,
            };
            if tool_done {
                ScriptedResponse {
                    content: "Completed after observing tool output.".to_string(),
                    reasoning: None,
                    tool_calls: Vec::new(),
                    finish_reason: Some("stop".to_string()),
                }
            } else {
                ScriptedResponse {
                    content: format!("Mock PS5LM response to: {prompt}"),
                    reasoning: default_reasoning,
                    tool_calls: Vec::new(),
                    finish_reason: Some("stop".to_string()),
                }
            }
        }
    }
}

async fn handle_chat_completions(
    State(state): State<MockState>,
    _headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    if is_prefill_request(&body) {
        state.record_prefill("/v1/chat/completions", body.clone());
        let snap = state.snapshot_props();
        if snap.mode == MockMode::Disconnect503 {
            return unavailable_503();
        }
        if snap.mode == MockMode::MalformedJson {
            return malformed_json_response();
        }
        if snap.mode == MockMode::SlowDelay {
            tokio::time::sleep(snap.delay).await;
        }
        let model = body
            .get("model")
            .and_then(|v| v.as_str())
            .unwrap_or(&snap.model_name)
            .to_string();
        let prefill_timings = state.resolve_chat_timings_json(&body, true);
        if body.get("stream").and_then(|v| v.as_bool()).unwrap_or(false) {
            let mut events = vec![Bytes::from(format!(
                "data: {}\n\n",
                json!({
                    "id": "chatcmpl_prefill_1",
                    "object": "chat.completion.chunk",
                    "model": model,
                    "choices": [{
                        "index": 0,
                        "delta": { "role": "assistant", "content": "" },
                        "finish_reason": null
                    }]
                })
            ))];
            if snap.mode != MockMode::IncompleteSse {
                events.push(Bytes::from(format!(
                    "data: {}\n\n",
                    json!({
                        "id": "chatcmpl_prefill_1",
                        "object": "chat.completion.chunk",
                        "model": model,
                        "choices": [{
                            "index": 0,
                            "delta": {},
                            "finish_reason": "length"
                        }],
                        "usage": {
                            "prompt_tokens": 16,
                            "completion_tokens": 0,
                            "total_tokens": 16
                        },
                        "timings": prefill_timings
                    })
                )));
                events.push(Bytes::from_static(b"data: [DONE]\n\n"));
            }
            return Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "text/event-stream")
                .header("cache-control", "no-cache")
                .body(Body::from_stream(stream::iter(
                    events.into_iter().map(Ok::<Bytes, std::convert::Infallible>),
                )))
                .unwrap();
        }
        return Json(json!({
            "id": "chatcmpl_prefill_1",
            "object": "chat.completion",
            "model": model,
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": ""
                },
                "finish_reason": "length"
            }],
            "usage": {
                "prompt_tokens": 16,
                "completion_tokens": 0,
                "total_tokens": 16
            },
            "timings": prefill_timings
        }))
        .into_response();
    }
    state.record("/v1/chat/completions", body.clone());
    let snap = state.snapshot_and_pop();
    if snap.mode == MockMode::Disconnect503 {
        return unavailable_503();
    }
    if snap.mode == MockMode::MalformedJson {
        return malformed_json_response();
    }
    if snap.mode == MockMode::SlowDelay {
        tokio::time::sleep(snap.delay).await;
    }

    let model = body
        .get("model")
        .and_then(|v| v.as_str())
        .unwrap_or(&snap.model_name)
        .to_string();
    let is_stream = body.get("stream").and_then(|v| v.as_bool()).unwrap_or(false);
    let plan = resolve_scripted_plan(snap.mode, snap.scripted, &body, snap.supports_thinking);
    let structured_reasoning = has_structured_reasoning_format(&body);
    let timings = state.resolve_chat_timings_json(&body, false);

    if is_stream {
        if snap.mode == MockMode::IncompleteSse {
            let chunks = vec![
                Ok::<Bytes, std::convert::Infallible>(Bytes::from(format!(
                    "data: {}\n\n",
                    json!({
                        "id": "chatcmpl_incomplete",
                        "object": "chat.completion.chunk",
                        "model": model,
                        "choices": [{
                            "index": 0,
                            "delta": { "role": "assistant", "content": "partial " },
                            "finish_reason": null
                        }]
                    })
                ))),
            ];
            let s = stream::iter(chunks);
            return Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "text/event-stream")
                .body(Body::from_stream(s))
                .unwrap();
        }

        let mut events: Vec<Bytes> = Vec::new();
        let mut sent_role = false;
        if let Some(ref r) = plan.reasoning {
            if !r.is_empty() && !plan.content.contains("<think>") {
                let delta = if structured_reasoning {
                    json!({ "role": "assistant", "reasoning_content": r })
                } else {
                    json!({ "role": "assistant", "content": format!("<think>{r}</think>") })
                };
                events.push(Bytes::from(format!(
                    "data: {}\n\n",
                    json!({
                        "id": "chatcmpl_1",
                        "object": "chat.completion.chunk",
                        "model": model,
                        "choices": [{
                            "index": 0,
                            "delta": delta,
                            "finish_reason": null
                        }]
                    })
                )));
                sent_role = true;
            }
        }

        if !plan.content.is_empty() {
            let mid = plan.content.len() / 2;
            let split_at = plan
                .content
                .char_indices()
                .map(|(i, _)| i)
                .find(|&i| i >= mid)
                .unwrap_or(plan.content.len());
            let (part1, part2) = plan.content.split_at(split_at);
            for part in [part1, part2].into_iter().filter(|p| !p.is_empty()) {
                let mut delta = json!({ "content": part });
                if !sent_role {
                    delta["role"] = json!("assistant");
                    sent_role = true;
                }
                events.push(Bytes::from(format!(
                    "data: {}\n\n",
                    json!({
                        "id": "chatcmpl_1",
                        "object": "chat.completion.chunk",
                        "model": model,
                        "choices": [{
                            "index": 0,
                            "delta": delta,
                            "finish_reason": null
                        }]
                    })
                )));
            }
        }

        if !plan.tool_calls.is_empty() {
            let tc_json: Vec<Value> = plan
                .tool_calls
                .iter()
                .enumerate()
                .map(|(idx, tc)| {
                    json!({
                        "index": idx,
                        "id": tc.id,
                        "type": "function",
                        "function": {
                            "name": tc.name,
                            "arguments": serde_json::to_string(&tc.arguments).unwrap_or_else(|_| "{}".into())
                        }
                    })
                })
                .collect();
            let mut delta = json!({ "tool_calls": tc_json });
            if !sent_role {
                delta["role"] = json!("assistant");
                sent_role = true;
            }
            events.push(Bytes::from(format!(
                "data: {}\n\n",
                json!({
                    "id": "chatcmpl_1",
                    "object": "chat.completion.chunk",
                    "model": model,
                    "choices": [{
                        "index": 0,
                        "delta": delta,
                        "finish_reason": null
                    }]
                })
            )));
        }

        let finish = plan
            .finish_reason
            .clone()
            .unwrap_or_else(|| if plan.tool_calls.is_empty() { "stop" } else { "tool_calls" }.to_string());
        let final_delta = if !sent_role {
            json!({ "role": "assistant" })
        } else {
            json!({})
        };
        events.push(Bytes::from(format!(
            "data: {}\n\n",
            json!({
                "id": "chatcmpl_1",
                "object": "chat.completion.chunk",
                "model": model,
                "choices": [{
                    "index": 0,
                    "delta": final_delta,
                    "finish_reason": finish
                }],
                "usage": {
                    "prompt_tokens": 16,
                    "completion_tokens": 12,
                    "total_tokens": 28
                },
                "timings": timings
            })
        )));
        events.push(Bytes::from_static(b"data: [DONE]\n\n"));

        let s = stream::iter(events.into_iter().map(Ok::<Bytes, std::convert::Infallible>));
        return Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/event-stream")
            .header("cache-control", "no-cache")
            .body(Body::from_stream(s))
            .unwrap();
    }

    if snap.mode == MockMode::IncompleteSse {
        return malformed_json_response();
    }

    let mut message = match plan.reasoning.as_deref() {
        Some(r) if structured_reasoning && !r.is_empty() && !plan.content.contains("<think>") => {
            json!({
                "role": "assistant",
                "content": plan.content,
                "reasoning_content": r,
            })
        }
        other => {
            let content = with_think_prefix(&plan.content, other);
            json!({
                "role": "assistant",
                "content": content,
            })
        }
    };
    if !plan.tool_calls.is_empty() {
        let tc_json: Vec<Value> = plan
            .tool_calls
            .iter()
            .map(|tc| {
                json!({
                    "id": tc.id,
                    "type": "function",
                    "function": {
                        "name": tc.name,
                        "arguments": serde_json::to_string(&tc.arguments).unwrap_or_else(|_| "{}".into())
                    }
                })
            })
            .collect();
        message["tool_calls"] = Value::Array(tc_json);
    }

    let finish = plan
        .finish_reason
        .unwrap_or_else(|| if plan.tool_calls.is_empty() { "stop" } else { "tool_calls" }.to_string());

    Json(json!({
        "id": "chatcmpl_mock_1",
        "object": "chat.completion",
        "model": model,
        "choices": [{
            "index": 0,
            "message": message,
            "finish_reason": finish
        }],
        "usage": {
            "prompt_tokens": 16,
            "completion_tokens": 12,
            "total_tokens": 28
        },
        "timings": timings
    }))
    .into_response()
}

async fn handle_responses(
    State(state): State<MockState>,
    _headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    state.record("/v1/responses", body.clone());
    let snap = state.snapshot_and_pop();
    if snap.mode == MockMode::Disconnect503 {
        return unavailable_503();
    }
    if snap.mode == MockMode::MalformedJson {
        return malformed_json_response();
    }
    if snap.mode == MockMode::SlowDelay {
        tokio::time::sleep(snap.delay).await;
    }

    let model = body
        .get("model")
        .and_then(|v| v.as_str())
        .unwrap_or(&snap.model_name)
        .to_string();
    let is_stream = body.get("stream").and_then(|v| v.as_bool()).unwrap_or(false);
    let plan = resolve_scripted_plan(snap.mode, snap.scripted, &body, snap.supports_thinking);

    let explicit_none = body
        .get("reasoning_format")
        .and_then(|v| v.as_str())
        .is_some_and(|rf| rf.trim().eq_ignore_ascii_case("none"));
    let structured_reasoning = !explicit_none
        && (has_structured_reasoning_format(&body)
            || body.get("reasoning").is_some_and(|v| v.is_object()));

    let (reasoning_text, full_text) = match plan.reasoning.as_deref() {
        Some(r) if structured_reasoning && !r.is_empty() && !plan.content.contains("<think>") => {
            (Some(r.to_string()), plan.content.clone())
        }
        other => (None, with_think_prefix(&plan.content, other)),
    };

    let resp_status = plan
        .finish_reason
        .as_deref()
        .map(|s| match s {
            "length" | "max_tokens" | "incomplete" => "incomplete",
            "failed" | "error" => "failed",
            _ => "completed",
        })
        .unwrap_or("completed");

    let mut output_items = Vec::new();
    if let Some(ref r) = reasoning_text {
        output_items.push(json!({
            "type": "reasoning",
            "id": "rs_mock_1",
            "status": "completed",
            "summary": [{
                "type": "summary_text",
                "text": r
            }],
            "text": r
        }));
    }
    let has_message_item = !full_text.is_empty() || (plan.tool_calls.is_empty() && reasoning_text.is_none());
    if has_message_item {
        output_items.push(json!({
            "type": "message",
            "id": "msg_resp_1",
            "role": "assistant",
            "status": "completed",
            "content": [{
                "type": "output_text",
                "text": full_text
            }]
        }));
    }
    for tc in &plan.tool_calls {
        output_items.push(json!({
            "type": "function_call",
            "id": format!("fc_{}", tc.id),
            "call_id": tc.id,
            "name": tc.name,
            "arguments": serde_json::to_string(&tc.arguments).unwrap_or_else(|_| "{}".into()),
            "status": "completed"
        }));
    }

    let mut resp_obj = json!({
        "id": "resp_mock_1",
        "object": "response",
        "status": resp_status,
        "model": model,
        "output": output_items,
        "output_text": full_text,
        "usage": {
            "input_tokens": 14,
            "output_tokens": 10,
            "total_tokens": 24
        }
    });
    if resp_status == "failed" {
        resp_obj["error"] = json!({ "message": "mock scripted failure" });
    }

    if is_stream {
        if snap.mode == MockMode::IncompleteSse {
            let chunks = vec![Ok::<Bytes, std::convert::Infallible>(Bytes::from(format!(
                "event: response.output_text.delta\ndata: {}\n\n",
                json!({ "type": "response.output_text.delta", "delta": "partial" })
            )))];
            return Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "text/event-stream")
                .body(Body::from_stream(stream::iter(chunks)))
                .unwrap();
        }
        let mut events = Vec::new();
        if let Some(ref r) = reasoning_text {
            events.push(Bytes::from(format!(
                "event: response.reasoning.delta\ndata: {}\n\n",
                json!({
                    "type": "response.reasoning.delta",
                    "delta": r
                })
            )));
        }
        if !full_text.is_empty() {
            events.push(Bytes::from(format!(
                "event: response.output_text.delta\ndata: {}\n\n",
                json!({
                    "type": "response.output_text.delta",
                    "delta": full_text
                })
            )));
        }
        let tc_base_idx = usize::from(reasoning_text.is_some()) + usize::from(has_message_item);
        for (i, tc) in plan.tool_calls.iter().enumerate() {
            let out_idx = tc_base_idx + i;
            let item_id = format!("fc_{}", tc.id);
            let args_str = serde_json::to_string(&tc.arguments).unwrap_or_else(|_| "{}".into());
            events.push(Bytes::from(format!(
                "event: response.output_item.added\ndata: {}\n\n",
                json!({
                    "type": "response.output_item.added",
                    "output_index": out_idx,
                    "item": {
                        "id": item_id,
                        "type": "function_call",
                        "call_id": tc.id,
                        "name": tc.name,
                        "arguments": ""
                    }
                })
            )));
            events.push(Bytes::from(format!(
                "event: response.function_call_arguments.delta\ndata: {}\n\n",
                json!({
                    "type": "response.function_call_arguments.delta",
                    "item_id": item_id,
                    "output_index": out_idx,
                    "delta": args_str
                })
            )));
            events.push(Bytes::from(format!(
                "event: response.function_call_arguments.done\ndata: {}\n\n",
                json!({
                    "type": "response.function_call_arguments.done",
                    "item_id": item_id,
                    "output_index": out_idx,
                    "name": tc.name,
                    "arguments": args_str
                })
            )));
            events.push(Bytes::from(format!(
                "event: response.output_item.done\ndata: {}\n\n",
                json!({
                    "type": "response.output_item.done",
                    "output_index": out_idx,
                    "item": {
                        "id": item_id,
                        "type": "function_call",
                        "call_id": tc.id,
                        "name": tc.name,
                        "arguments": args_str,
                        "status": "completed"
                    }
                })
            )));
        }
        let terminal_ev = match resp_status {
            "incomplete" => "response.incomplete",
            "failed" => "response.failed",
            _ => "response.completed",
        };
        events.push(Bytes::from(format!(
            "event: {terminal_ev}\ndata: {}\n\n",
            json!({
                "type": terminal_ev,
                "response": resp_obj
            })
        )));
        return Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/event-stream")
            .body(Body::from_stream(stream::iter(
                events.into_iter().map(Ok::<Bytes, std::convert::Infallible>),
            )))
            .unwrap();
    }

    Json(resp_obj).into_response()
}

async fn handle_anthropic_messages(
    State(state): State<MockState>,
    _headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    state.record("/v1/messages", body.clone());
    let snap = state.snapshot_and_pop();
    if snap.mode == MockMode::Disconnect503 {
        return unavailable_503();
    }
    if snap.mode == MockMode::MalformedJson {
        return malformed_json_response();
    }
    if snap.mode == MockMode::SlowDelay {
        tokio::time::sleep(snap.delay).await;
    }

    let model = body
        .get("model")
        .and_then(|v| v.as_str())
        .unwrap_or(&snap.model_name)
        .to_string();
    let is_stream = body.get("stream").and_then(|v| v.as_bool()).unwrap_or(false);
    let plan = resolve_scripted_plan(snap.mode, snap.scripted, &body, snap.supports_thinking);

    let structured_thinking = anthropic_wants_thinking_block(&body);
    let (thinking_text, full_text) = match plan.reasoning.as_deref() {
        Some(r) if structured_thinking && !r.is_empty() && !plan.content.contains("<think>") => {
            (Some(r.to_string()), plan.content.clone())
        }
        other => (None, with_think_prefix(&plan.content, other)),
    };

    let mut content_blocks = Vec::new();
    if let Some(ref t) = thinking_text {
        content_blocks.push(json!({
            "type": "thinking",
            "thinking": t
        }));
    }
    if !full_text.is_empty() || (plan.tool_calls.is_empty() && thinking_text.is_none()) {
        content_blocks.push(json!({
            "type": "text",
            "text": full_text
        }));
    }
    for tc in &plan.tool_calls {
        content_blocks.push(json!({
            "type": "tool_use",
            "id": tc.id,
            "name": tc.name,
            "input": tc.arguments
        }));
    }

    let stop_reason = plan
        .finish_reason
        .as_deref()
        .map(|s| match s {
            "length" | "max_tokens" => "max_tokens",
            "tool_calls" | "tool_use" => "tool_use",
            "stop" | "end_turn" => "end_turn",
            other => other,
        })
        .unwrap_or_else(|| {
            if plan.tool_calls.is_empty() {
                "end_turn"
            } else {
                "tool_use"
            }
        });

    if is_stream {
        if snap.mode == MockMode::IncompleteSse {
            let chunks = vec![Ok::<Bytes, std::convert::Infallible>(Bytes::from(format!(
                "event: content_block_delta\ndata: {}\n\n",
                json!({
                    "type": "content_block_delta",
                    "index": 0,
                    "delta": { "type": "text_delta", "text": "partial" }
                })
            )))];
            return Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "text/event-stream")
                .body(Body::from_stream(stream::iter(chunks)))
                .unwrap();
        }
        let mut events = Vec::new();
        events.push(Bytes::from(format!(
            "event: message_start\ndata: {}\n\n",
            json!({
                "type": "message_start",
                "message": {
                    "id": "msg_mock_1",
                    "type": "message",
                    "role": "assistant",
                    "model": model,
                    "usage": { "input_tokens": 15, "output_tokens": 0 }
                }
            })
        )));
        let mut block_idx: usize = 0;
        if let Some(ref t) = thinking_text {
            events.push(Bytes::from(format!(
                "event: content_block_start\ndata: {}\n\n",
                json!({
                    "type": "content_block_start",
                    "index": block_idx,
                    "content_block": { "type": "thinking", "thinking": "" }
                })
            )));
            events.push(Bytes::from(format!(
                "event: content_block_delta\ndata: {}\n\n",
                json!({
                    "type": "content_block_delta",
                    "index": block_idx,
                    "delta": { "type": "thinking_delta", "thinking": t }
                })
            )));
            events.push(Bytes::from(format!(
                "event: content_block_stop\ndata: {}\n\n",
                json!({ "type": "content_block_stop", "index": block_idx })
            )));
            block_idx += 1;
        }
        if !full_text.is_empty() || (plan.tool_calls.is_empty() && thinking_text.is_none()) {
            events.push(Bytes::from(format!(
                "event: content_block_start\ndata: {}\n\n",
                json!({
                    "type": "content_block_start",
                    "index": block_idx,
                    "content_block": { "type": "text", "text": "" }
                })
            )));
            events.push(Bytes::from(format!(
                "event: content_block_delta\ndata: {}\n\n",
                json!({
                    "type": "content_block_delta",
                    "index": block_idx,
                    "delta": { "type": "text_delta", "text": full_text }
                })
            )));
            events.push(Bytes::from(format!(
                "event: content_block_stop\ndata: {}\n\n",
                json!({ "type": "content_block_stop", "index": block_idx })
            )));
            block_idx += 1;
        }
        for tc in &plan.tool_calls {
            events.push(Bytes::from(format!(
                "event: content_block_start\ndata: {}\n\n",
                json!({
                    "type": "content_block_start",
                    "index": block_idx,
                    "content_block": {
                        "type": "tool_use",
                        "id": tc.id,
                        "name": tc.name,
                        "input": {}
                    }
                })
            )));
            let partial = serde_json::to_string(&tc.arguments).unwrap_or_else(|_| "{}".into());
            events.push(Bytes::from(format!(
                "event: content_block_delta\ndata: {}\n\n",
                json!({
                    "type": "content_block_delta",
                    "index": block_idx,
                    "delta": {
                        "type": "input_json_delta",
                        "partial_json": partial
                    }
                })
            )));
            events.push(Bytes::from(format!(
                "event: content_block_stop\ndata: {}\n\n",
                json!({ "type": "content_block_stop", "index": block_idx })
            )));
            block_idx += 1;
        }
        events.push(Bytes::from(format!(
            "event: message_delta\ndata: {}\n\n",
            json!({
                "type": "message_delta",
                "delta": { "stop_reason": stop_reason },
                "usage": { "output_tokens": 11 }
            })
        )));
        events.push(Bytes::from(format!(
            "event: message_stop\ndata: {}\n\n",
            json!({ "type": "message_stop" })
        )));
        return Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/event-stream")
            .body(Body::from_stream(stream::iter(
                events.into_iter().map(Ok::<Bytes, std::convert::Infallible>),
            )))
            .unwrap();
    }

    Json(json!({
        "id": "msg_mock_1",
        "type": "message",
        "role": "assistant",
        "model": model,
        "content": content_blocks,
        "stop_reason": stop_reason,
        "usage": {
            "input_tokens": 15,
            "output_tokens": 11
        }
    }))
    .into_response()
}

async fn handle_completions(
    State(state): State<MockState>,
    _headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    if is_prefill_request(&body) {
        state.record_prefill("/v1/completions", body.clone());
        let snap = state.snapshot_props();
        match snap.mode {
            MockMode::Disconnect503 => return unavailable_503(),
            MockMode::MalformedJson => return malformed_json_response(),
            MockMode::SlowDelay => tokio::time::sleep(snap.delay).await,
            _ => {}
        }
        let model = body
            .get("model")
            .and_then(|v| v.as_str())
            .unwrap_or(&snap.model_name)
            .to_string();
        let is_stream = body.get("stream").and_then(|v| v.as_bool()).unwrap_or(false);
        if is_stream {
            let mut events = vec![Bytes::from(format!(
                "data: {}\n\n",
                json!({
                    "id": "cmpl_prefill_1",
                    "object": "text_completion",
                    "model": model,
                    "choices": [{ "index": 0, "text": "", "finish_reason": "length" }],
                    "usage": { "prompt_tokens": 14, "completion_tokens": 0, "total_tokens": 14 }
                })
            ))];
            if snap.mode != MockMode::IncompleteSse {
                events.push(Bytes::from("data: [DONE]\n\n"));
            }
            return Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "text/event-stream")
                .body(Body::from_stream(stream::iter(
                    events.into_iter().map(Ok::<Bytes, std::convert::Infallible>),
                )))
                .unwrap();
        }
        return Json(json!({
            "id": "cmpl_prefill_1",
            "object": "text_completion",
            "model": model,
            "choices": [{
                "index": 0,
                "text": "",
                "finish_reason": "length"
            }],
            "usage": {
                "prompt_tokens": 14,
                "completion_tokens": 0,
                "total_tokens": 14
            }
        }))
        .into_response();
    }

    state.record("/v1/completions", body.clone());
    let snap = state.snapshot_and_pop();

    match snap.mode {
        MockMode::Disconnect503 => return unavailable_503(),
        MockMode::MalformedJson => return malformed_json_response(),
        MockMode::SlowDelay => tokio::time::sleep(snap.delay).await,
        _ => {}
    }

    let model = body
        .get("model")
        .and_then(|v| v.as_str())
        .unwrap_or(&snap.model_name)
        .to_string();
    let is_stream = body.get("stream").and_then(|v| v.as_bool()).unwrap_or(false);
    let plan = resolve_scripted_plan(snap.mode, snap.scripted, &body, snap.supports_thinking);

    let mut text_out = with_think_prefix(&plan.content, plan.reasoning.as_deref());
    for tc in &plan.tool_calls {
        text_out.push_str("<tool_call>");
        text_out.push_str(
            &json!({
                "name": tc.name,
                "arguments": tc.arguments,
            })
            .to_string(),
        );
        text_out.push_str("</tool_call>");
    }

    let finish_reason = plan.finish_reason.unwrap_or_else(|| "stop".to_string());

    if is_stream {
        if snap.mode == MockMode::IncompleteSse {
            let chunks = vec![Ok::<Bytes, std::convert::Infallible>(Bytes::from(format!(
                "data: {}\n\n",
                json!({
                    "id": "cmpl_mock_inc",
                    "object": "text_completion",
                    "model": model,
                    "choices": [{ "index": 0, "text": "partial ", "finish_reason": null }]
                })
            )))];
            return Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "text/event-stream")
                .body(Body::from_stream(stream::iter(chunks)))
                .unwrap();
        }

        let mid = text_out.len() / 2;
        let split_at = (0..=mid)
            .rev()
            .find(|&i| text_out.is_char_boundary(i))
            .unwrap_or(0);
        let (part1, part2) = text_out.split_at(split_at);
        let mut events = Vec::new();
        if !part1.is_empty() {
            events.push(Bytes::from(format!(
                "data: {}\n\n",
                json!({
                    "id": "cmpl_mock_1",
                    "object": "text_completion",
                    "model": model,
                    "choices": [{ "index": 0, "text": part1, "finish_reason": null }]
                })
            )));
        }
        events.push(Bytes::from(format!(
            "data: {}\n\n",
            json!({
                "id": "cmpl_mock_1",
                "object": "text_completion",
                "model": model,
                "choices": [{ "index": 0, "text": part2, "finish_reason": finish_reason }],
                "usage": { "prompt_tokens": 14, "completion_tokens": 10, "total_tokens": 24 }
            })
        )));
        events.push(Bytes::from("data: [DONE]\n\n"));

        return Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/event-stream")
            .body(Body::from_stream(stream::iter(
                events.into_iter().map(Ok::<Bytes, std::convert::Infallible>),
            )))
            .unwrap();
    }

    Json(json!({
        "id": "cmpl_mock_1",
        "object": "text_completion",
        "model": model,
        "choices": [{
            "index": 0,
            "text": text_out,
            "finish_reason": finish_reason
        }],
        "usage": {
            "prompt_tokens": 14,
            "completion_tokens": 10,
            "total_tokens": 24
        }
    }))
    .into_response()
}

async fn handle_systemone(
    State(state): State<MockState>,
    _headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    state.record("/v1/systemone", body.clone());
    let snap = state.snapshot_props();
    let model = snap.model_name;

    match snap.mode {
        MockMode::Disconnect503 => return unavailable_503(),
        MockMode::MalformedJson => return malformed_json_response(),
        MockMode::SlowDelay => tokio::time::sleep(snap.delay).await,
        _ => {}
    }

    let mut answers = serde_json::Map::new();
    if let Some(questions) = body.get("questions").and_then(|q| q.as_object()) {
        for (qkey, qdef) in questions {
            let qtype = qdef
                .get("type")
                .and_then(|v| v.as_str())
                .unwrap_or("noul");
            match qtype {
                "choice" => {
                    let mut probs = serde_json::Map::new();
                    let mut selected = "default".to_string();
                    if let Some(crit) = qdef.get("criteria").and_then(|c| c.as_object()) {
                        let keys: Vec<&String> = crit.keys().collect();
                        if let Some(first) = keys.first() {
                            selected = (*first).clone();
                        }
                        let n = keys.len().max(1);
                        for (idx, k) in keys.iter().enumerate() {
                            let p = if idx == 0 {
                                0.88
                            } else {
                                0.12 / ((n.saturating_sub(1)).max(1) as f64)
                            };
                            probs.insert((*k).clone(), json!(p));
                        }
                    } else {
                        probs.insert(selected.clone(), json!(0.88));
                    }
                    answers.insert(
                        qkey.clone(),
                        json!({
                            "type": "choice",
                            "choice": selected,
                            "confidence": 0.88,
                            "probabilities": probs,
                        }),
                    );
                }
                "score" => {
                    let mut legend = serde_json::Map::new();
                    let mut probs = serde_json::Map::new();
                    if let Some(arr) = qdef.get("criteria").and_then(|c| c.as_array()) {
                        for (idx, label) in arr.iter().enumerate() {
                            legend.insert(idx.to_string(), label.clone());
                            let p = if idx + 1 == arr.len() { 0.7 } else { 0.3 / ((arr.len().saturating_sub(1)).max(1) as f64) };
                            probs.insert(idx.to_string(), json!(p));
                        }
                    }
                    answers.insert(
                        qkey.clone(),
                        json!({
                            "type": "score",
                            "score": 1.5,
                            "confidence": 0.79,
                            "legend": legend,
                            "probabilities": probs,
                        }),
                    );
                }
                _ => {
                    answers.insert(
                        qkey.clone(),
                        json!({
                            "type": "noul",
                            "noul": 0.84
                        }),
                    );
                }
            }
        }
    }

    Json(json!({
        "model": model,
        "answers": Value::Object(answers),
        "usage": {
            "input_tokens": 32,
            "output_tokens": 0
        }
    }))
    .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn response_json(resp: Response) -> Value {
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        serde_json::from_slice(&bytes).expect("valid json")
    }

    async fn response_text(resp: Response) -> String {
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        String::from_utf8(bytes.to_vec()).expect("utf8")
    }

    #[tokio::test]
    async fn props_and_slots_defaults_and_set_props() {
        let state = MockState::new();
        let props = response_json(handle_props(State(state.clone())).await).await;
        assert_eq!(props["default_generation_settings"]["n_ctx"], 16384);
        assert_eq!(props["total_slots"], 4);
        assert_eq!(props["modalities"]["vision"], true);
        assert_eq!(props["modalities"]["audio"], false);
        assert_eq!(props["chat_template_caps"]["supports_thinking"], true);
        assert_eq!(props["model_path"], "/data/models/qwen-loaded.gguf");

        let slots = response_json(handle_slots(State(state.clone())).await).await;
        let arr = slots.as_array().expect("slots array");
        assert_eq!(arr.len(), 4);
        assert_eq!(arr[0]["id"], 0);
        assert_eq!(arr[0]["n_ctx"], 16384);
        assert_eq!(arr[0]["is_processing"], false);
        assert_eq!(arr[3]["id"], 3);
        assert_eq!(arr[3]["n_ctx"], 16384);
        assert_eq!(arr[3]["is_processing"], false);

        state.set_props(32768, 2, false, false);
        state.set_model_path("/models/custom.gguf");
        let props2 = response_json(handle_props(State(state.clone())).await).await;
        assert_eq!(props2["default_generation_settings"]["n_ctx"], 32768);
        assert_eq!(props2["total_slots"], 2);
        assert_eq!(props2["modalities"]["vision"], false);
        assert_eq!(props2["chat_template_caps"]["supports_thinking"], false);
        assert_eq!(props2["model_path"], "/models/custom.gguf");

        let slots2 = response_json(handle_slots(State(state)).await).await;
        assert_eq!(slots2.as_array().unwrap().len(), 2);
        assert_eq!(slots2[1]["n_ctx"], 32768);
    }

    #[test]
    fn last_user_text_concatenates_multimodal_arrays() {
        let chat_body = json!({
            "messages": [
                {"role": "system", "content": "Be brief"},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "First line"},
                        {"type": "image_url", "image_url": {"url": "data:image/png;base64,AAAA"}},
                        {"type": "text", "text": "Second line"}
                    ]
                }
            ]
        });
        assert_eq!(last_user_text(&chat_body), "First line\nSecond line");

        let resp_body = json!({
            "input": [
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": "Inspect image"},
                        {"type": "input_image", "image_url": "https://example.com/a.png"},
                        {"type": "input_text", "text": "and summarize"}
                    ]
                }
            ]
        });
        assert_eq!(last_user_text(&resp_body), "Inspect image\nand summarize");
    }

    #[tokio::test]
    async fn thinking_controls_and_full_request_recording() {
        let state = MockState::new();

        // 1. reasoning_effort: "high" -> emits <think>...</think> in non-streaming & SSE
        let req_high = json!({
            "model": "qwen-loaded",
            "reasoning_effort": "high",
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "text", "text": "Analyze"},
                    {"type": "image_url", "image_url": {"url": "data:image/png;base64,XYZ"}}
                ]
            }]
        });
        let resp = response_json(
            handle_chat_completions(State(state.clone()), HeaderMap::new(), Json(req_high.clone()))
                .await,
        )
        .await;
        let content = resp["choices"][0]["message"]["content"].as_str().unwrap();
        assert!(content.contains("<think>"));
        assert!(content.contains("Analyze"));

        // 2. chat_template_kwargs.enable_thinking: false suppresses reasoning even on ScriptedResponse
        state.push_scripted(ScriptedResponse {
            content: "Plain answer".into(),
            reasoning: Some("hidden thought".into()),
            tool_calls: vec![],
            finish_reason: Some("stop".into()),
        });
        let req_off = json!({
            "model": "qwen-loaded",
            "stream": true,
            "chat_template_kwargs": {"enable_thinking": false},
            "messages": [{"role": "user", "content": "Hi"}]
        });
        let sse = response_text(
            handle_chat_completions(State(state.clone()), HeaderMap::new(), Json(req_off.clone()))
                .await,
        )
        .await;
        assert!(!sse.contains("<think>"));
        assert!(sse.contains("Plain"));

        // 3. Recorded requests preserve full JSON bodies
        let recorded = state.recorded_requests();
        assert_eq!(recorded.len(), 2);
        assert_eq!(recorded[0].body, req_high);
        assert_eq!(recorded[1].body, req_off);
    }

    #[tokio::test]
    async fn thinking_budget_distinction_and_prefill_preserves_scripted() {
        let high_req = json!({
            "reasoning_effort": "high",
            "reasoning_budget": 8192,
            "chat_template_kwargs": {"enable_thinking": true}
        });
        assert_eq!(inspect_request_thinking(&high_req), RequestThinking::Requested("high"));

        let max_budget_req = json!({
            "reasoning_effort": "high",
            "reasoning_budget": -1,
            "chat_template_kwargs": {"enable_thinking": true}
        });
        assert_eq!(inspect_request_thinking(&max_budget_req), RequestThinking::Requested("max"));

        let state = MockState::new();
        state.push_scripted(ScriptedResponse {
            content: "Next real reply".into(),
            reasoning: None,
            tool_calls: vec![],
            finish_reason: Some("stop".into()),
        });

        let prefill = json!({
            "model": "qwen-loaded",
            "n_predict": 0,
            "messages": [{"role": "user", "content": "Warm KV cache"}]
        });
        let prefill_resp = handle_chat_completions(
            State(state.clone()),
            HeaderMap::new(),
            Json(prefill.clone()),
        )
        .await;
        assert_eq!(prefill_resp.status(), StatusCode::OK);
        let prefill_json = response_json(prefill_resp).await;
        assert_eq!(prefill_json["object"], "chat.completion");
        assert_eq!(prefill_json["choices"][0]["message"]["content"], "");
        assert_eq!(prefill_json["choices"][0]["finish_reason"], "length");
        assert!(state.recorded_requests().is_empty());
        assert_eq!(state.recorded_prefill_requests().len(), 1);
        assert_eq!(state.recorded_prefill_requests()[0].body, prefill);

        // Scripted response must still be available for the following request.
        let follow_up_req = json!({
            "model": "qwen-loaded",
            "messages": [{"role": "user", "content": "Actual prompt"}]
        });
        let follow_up = response_json(
            handle_chat_completions(
                State(state.clone()),
                HeaderMap::new(),
                Json(follow_up_req.clone()),
            )
            .await,
        )
        .await;
        assert_eq!(follow_up["choices"][0]["message"]["content"], "Next real reply");
        assert_eq!(state.recorded_requests().len(), 1);
        assert_eq!(state.recorded_requests()[0].body, follow_up_req);
        assert_eq!(state.recorded_prefill_requests().len(), 1);

        state.clear_recorded();
        assert!(state.recorded_requests().is_empty());
        assert!(state.recorded_prefill_requests().is_empty());
    }

    #[tokio::test]
    async fn fault_modes_preserve_scripted_and_prefill_handles_faults_and_stream() {
        let state = MockState::new();
        state.push_scripted(ScriptedResponse {
            content: "Preserved reply".into(),
            reasoning: None,
            tool_calls: vec![],
            finish_reason: Some("stop".into()),
        });

        state.set_mode(MockMode::Disconnect503);
        for resp in [
            handle_chat_completions(
                State(state.clone()),
                HeaderMap::new(),
                Json(json!({"messages": [{"role": "user", "content": "hi"}]})),
            )
            .await,
            handle_responses(
                State(state.clone()),
                HeaderMap::new(),
                Json(json!({"input": "hi"})),
            )
            .await,
            handle_anthropic_messages(
                State(state.clone()),
                HeaderMap::new(),
                Json(json!({"messages": [{"role": "user", "content": "hi"}]})),
            )
            .await,
            handle_completions(
                State(state.clone()),
                HeaderMap::new(),
                Json(json!({"prompt": "hi"})),
            )
            .await,
        ] {
            assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        }

        // Prefill with max_tokens=0 under 503 returns 503 too.
        let prefill_503 = handle_chat_completions(
            State(state.clone()),
            HeaderMap::new(),
            Json(json!({"max_tokens": 0, "messages": [{"role": "user", "content": "warm"}]})),
        )
        .await;
        assert_eq!(prefill_503.status(), StatusCode::SERVICE_UNAVAILABLE);

        state.set_mode(MockMode::Normal);
        state.clear_recorded();

        // Streaming max_tokens=0 prefill on chat and completions
        let sse_chat_prefill = response_text(
            handle_chat_completions(
                State(state.clone()),
                HeaderMap::new(),
                Json(json!({"max_tokens": 0, "stream": true, "messages": [{"role": "user", "content": "warm"}]})),
            )
            .await,
        )
        .await;
        assert!(sse_chat_prefill.contains("data: [DONE]"));

        let sse_cmpl_prefill = response_text(
            handle_completions(
                State(state.clone()),
                HeaderMap::new(),
                Json(json!({"n_predict": 0, "stream": true, "model": "cmpl-m", "prompt": "warm"})),
            )
            .await,
        )
        .await;
        assert!(sse_cmpl_prefill.contains("cmpl-m"));
        assert!(sse_cmpl_prefill.contains("data: [DONE]"));
        assert!(state.recorded_requests().is_empty());
        assert_eq!(state.recorded_prefill_requests().len(), 2);

        // Preserved scripted reply is still popped next
        let ok = response_json(
            handle_chat_completions(
                State(state.clone()),
                HeaderMap::new(),
                Json(json!({"messages": [{"role": "user", "content": "now"}]})),
            )
            .await,
        )
        .await;
        assert_eq!(ok["choices"][0]["message"]["content"], "Preserved reply");
    }

    #[tokio::test]
    async fn structured_reasoning_sse_compliance_props_alias_and_thinking_directives() {
        let state = MockState::new();
        let props = props_json(&state.snapshot_props()).0;
        assert_eq!(props["model_alias"], "qwen-loaded");

        assert_eq!(
            inspect_request_thinking(&json!({
                "thinking": { "type": "enabled", "budget_tokens": 2048 }
            })),
            RequestThinking::Requested("medium")
        );
        assert_eq!(
            inspect_request_thinking(&json!({
                "thinking": { "type": "enabled", "budget_tokens": 0 }
            })),
            RequestThinking::Disabled
        );
        assert_eq!(
            inspect_request_thinking(&json!({
                "prompt": "<|im_start|>user\nPlease explain /no_think in docs<|im_end|>"
            })),
            RequestThinking::Unspecified
        );
        assert_eq!(
            inspect_request_thinking(&json!({
                "prompt": "<|im_start|>user\nHi\n/no_think<|im_end|>"
            })),
            RequestThinking::Disabled
        );

        // ChatCompletions structured reasoning_content (non-stream & stream) + role delta
        state.push_scripted(ScriptedResponse {
            content: "ans".into(),
            reasoning: Some("think".into()),
            tool_calls: vec![],
            finish_reason: Some("stop".into()),
        });
        let chat_json = response_json(
            handle_chat_completions(
                State(state.clone()),
                HeaderMap::new(),
                Json(json!({
                    "reasoning_format": "deepseek",
                    "messages": [{"role": "user", "content": "hi"}]
                })),
            )
            .await,
        )
        .await;
        assert_eq!(chat_json["choices"][0]["message"]["content"], "ans");
        assert_eq!(
            chat_json["choices"][0]["message"]["reasoning_content"],
            "think"
        );

        state.push_scripted(ScriptedResponse {
            content: "plain".into(),
            reasoning: None,
            tool_calls: vec![],
            finish_reason: Some("stop".into()),
        });
        let chat_sse = response_text(
            handle_chat_completions(
                State(state.clone()),
                HeaderMap::new(),
                Json(json!({
                    "stream": true,
                    "messages": [{"role": "user", "content": "hi"}]
                })),
            )
            .await,
        )
        .await;
        assert!(chat_sse.contains(r#""role":"assistant""#));

        // Responses structured reasoning item + incomplete finish_reason
        state.push_scripted(ScriptedResponse {
            content: "cut".into(),
            reasoning: Some("reason".into()),
            tool_calls: vec![],
            finish_reason: Some("incomplete".into()),
        });
        let resp_sse = response_text(
            handle_responses(
                State(state.clone()),
                HeaderMap::new(),
                Json(json!({
                    "stream": true,
                    "reasoning_format": "deepseek",
                    "input": "hi"
                })),
            )
            .await,
        )
        .await;
        assert!(resp_sse.contains("event: response.reasoning.delta"));
        assert!(resp_sse.contains("event: response.incomplete"));

        // Anthropic structured thinking + input_json_delta + max_tokens stop_reason
        state.push_scripted(ScriptedResponse {
            content: "doing".into(),
            reasoning: Some("plan".into()),
            tool_calls: vec![ScriptedToolCall {
                id: "tc_1".into(),
                name: "ls".into(),
                arguments: json!({"path": "."}),
            }],
            finish_reason: Some("length".into()),
        });
        let anth_sse = response_text(
            handle_anthropic_messages(
                State(state.clone()),
                HeaderMap::new(),
                Json(json!({
                    "stream": true,
                    "thinking": {"type": "enabled", "budget_tokens": 4096},
                    "messages": [{"role": "user", "content": "hi"}]
                })),
            )
            .await,
        )
        .await;
        assert!(anth_sse.contains(r#""type":"thinking_delta""#));
        assert!(anth_sse.contains(r#""type":"input_json_delta""#));
        assert!(anth_sse.contains("event: content_block_stop"));
        assert!(anth_sse.contains(r#""stop_reason":"max_tokens""#));

        // Completions honors body.model
        let cmpl_json = response_json(
            handle_completions(
                State(state),
                HeaderMap::new(),
                Json(json!({"model": "custom-cmpl-model", "prompt": "hi"})),
            )
            .await,
        )
        .await;
        assert_eq!(cmpl_json["model"], "custom-cmpl-model");
    }

    #[tokio::test]
    async fn props_and_slots_expose_kv_sampling_defaults_and_overrides() {
        let state = MockState::new();
        let props = response_json(handle_props(State(state.clone())).await).await;
        assert_eq!(props["default_generation_settings"]["n_ctx"], 16384);
        assert_eq!(props["cache_type_k"], "q8_0");
        assert_eq!(props["cache_type_v"], "q8_0");
        assert_eq!(props["flash_attn"], true);
        assert_eq!(props["kv_cache"]["cache_type_k"], "q8_0");
        let params = &props["default_generation_settings"]["params"];
        assert_eq!(params["n_keep"], 0);
        assert_eq!(params["top_k"], 40);
        assert_eq!(params["repeat_last_n"], 256);
        assert_eq!(params["dry_penalty_last_n"], 4096);
        assert_eq!(
            params["samplers"],
            json!([
                "penalties",
                "dry",
                "top_k",
                "typ_p",
                "top_p",
                "min_p",
                "xtc",
                "temperature"
            ])
        );
        assert!((params["temperature"].as_f64().unwrap() - 0.7).abs() < 1e-4);
        assert!((params["dry_multiplier"].as_f64().unwrap() - 0.8).abs() < 1e-4);

        let slots = response_json(handle_slots(State(state.clone())).await).await;
        assert_eq!(slots[0]["cache_type_k"], "q8_0");
        assert_eq!(slots[0]["flash_attn"], true);
        assert_eq!(slots[0]["params"]["repeat_last_n"], 256);

        let custom = MockKvSampling {
            cache_type_k: Some("f16".into()),
            cache_type_v: Some("q4_0".into()),
            flash_attn: Some(false),
            n_keep: 128,
            temperature: 0.35,
            top_k: 64,
            top_p: 0.9,
            min_p: 0.1,
            repeat_penalty: 1.12,
            repeat_last_n: 512,
            presence_penalty: 0.25,
            frequency_penalty: 0.15,
            dry_multiplier: 1.1,
            dry_base: 1.85,
            dry_allowed_length: 3,
            dry_penalty_last_n: 2048,
            xtc_probability: 0.2,
            xtc_threshold: 0.3,
            samplers: vec!["top_k".into(), "temperature".into()],
        };
        state.set_mock_kv_sampling(custom.clone());
        assert_eq!(state.mock_kv_sampling(), custom);

        let props2 = response_json(handle_props(State(state.clone())).await).await;
        assert_eq!(props2["cache_type_k"], "f16");
        assert_eq!(props2["cache_type_v"], "q4_0");
        assert_eq!(props2["flash_attn"], false);
        assert_eq!(props2["kv_cache"]["cache_type_v"], "q4_0");
        let params2 = &props2["default_generation_settings"]["params"];
        assert_eq!(params2["n_keep"], 128);
        assert_eq!(params2["top_k"], 64);
        assert_eq!(params2["repeat_last_n"], 512);
        assert_eq!(params2["dry_allowed_length"], 3);
        assert_eq!(params2["dry_penalty_last_n"], 2048);
        assert_eq!(params2["samplers"], json!(["top_k", "temperature"]));
        assert!((params2["xtc_probability"].as_f64().unwrap() - 0.2).abs() < 1e-4);

        let slots2 = response_json(handle_slots(State(state.clone())).await).await;
        assert_eq!(slots2[0]["cache_type_k"], "f16");
        assert_eq!(slots2[0]["params"]["n_keep"], 128);

        state.set_kv_cache(None, None, None);
        let props3 = response_json(handle_props(State(state)).await).await;
        assert!(props3.get("cache_type_k").is_none());
        assert!(props3.get("kv_cache").is_none());
    }

    #[tokio::test]
    async fn chat_completions_emit_timings_and_detect_shared_prefix_cache_n() {
        let state = MockState::new();
        let sys = json!({"role": "system", "content": "Static system prompt for prefix reuse testing across turns."});
        let user1 = json!({"role": "user", "content": "Turn one greeting"});

        // Turn 1 (cold): cache_n == 0, default tok/s rates, usage preserved
        let turn1 = response_json(
            handle_chat_completions(
                State(state.clone()),
                HeaderMap::new(),
                Json(json!({
                    "id_slot": 1,
                    "messages": [sys.clone(), user1.clone()]
                })),
            )
            .await,
        )
        .await;
        assert_eq!(turn1["usage"]["prompt_tokens"], 16);
        assert_eq!(turn1["usage"]["completion_tokens"], 12);
        assert_eq!(turn1["usage"]["total_tokens"], 28);
        assert_eq!(turn1["timings"]["predicted_per_second"], 118.4);
        assert_eq!(turn1["timings"]["prompt_per_second"], 420.0);
        assert_eq!(turn1["timings"]["predicted_ms"], 101.4);
        assert_eq!(turn1["timings"]["prompt_ms"], 38.1);
        assert_eq!(turn1["timings"]["predicted_n"], 12);
        assert_eq!(turn1["timings"]["prompt_n"], 16);
        assert_eq!(turn1["timings"]["cache_n"], 0);

        // Turn 2 on same slot with identical prefix (streaming SSE final chunk) -> cache_n > 0
        let sse = response_text(
            handle_chat_completions(
                State(state.clone()),
                HeaderMap::new(),
                Json(json!({
                    "id_slot": 1,
                    "stream": true,
                    "stream_options": {"include_usage": true},
                    "messages": [
                        sys.clone(),
                        user1.clone(),
                        {"role": "assistant", "content": "Hi"},
                        {"role": "user", "content": "Turn two"}
                    ]
                })),
            )
            .await,
        )
        .await;
        let data_lines: Vec<Value> = sse
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .filter(|payload| *payload != "[DONE]")
            .map(|payload| serde_json::from_str::<Value>(payload).expect("valid sse chunk json"))
            .collect();
        let final_chunk = data_lines.last().expect("final sse chunk");
        assert_eq!(final_chunk["usage"]["total_tokens"], 28);
        assert_eq!(final_chunk["timings"]["predicted_per_second"], 118.4);
        assert_eq!(final_chunk["timings"]["prompt_per_second"], 420.0);
        assert!(final_chunk["timings"]["cache_n"].as_u64().unwrap() > 0);

        // Different slot with no prior turn on slot 2 -> cache_n == 0
        let other_slot = response_json(
            handle_chat_completions(
                State(state.clone()),
                HeaderMap::new(),
                Json(json!({
                    "id_slot": 2,
                    "messages": [sys.clone(), user1.clone()]
                })),
            )
            .await,
        )
        .await;
        assert_eq!(other_slot["timings"]["cache_n"], 0);

        // Explicit MockTimings override + prefill response timings
        let custom_timings = MockTimings {
            predicted_per_second: 142.5,
            prompt_per_second: 510.2,
            predicted_ms: 210.0,
            prompt_ms: 45.0,
            predicted_n: 30,
            prompt_n: 23,
            cache_n: Some(333),
        };
        state.set_mock_timings(custom_timings.clone());
        assert_eq!(state.mock_timings(), custom_timings);

        let override_resp = response_json(
            handle_chat_completions(
                State(state.clone()),
                HeaderMap::new(),
                Json(json!({"messages": [sys.clone(), user1.clone()]})),
            )
            .await,
        )
        .await;
        assert_eq!(override_resp["timings"]["predicted_per_second"], 142.5);
        assert_eq!(override_resp["timings"]["prompt_per_second"], 510.2);
        assert_eq!(override_resp["timings"]["cache_n"], 333);

        let prefill_resp = response_json(
            handle_chat_completions(
                State(state),
                HeaderMap::new(),
                Json(json!({
                    "n_predict": 0,
                    "messages": [sys, user1]
                })),
            )
            .await,
        )
        .await;
        assert_eq!(prefill_resp["timings"]["predicted_n"], 0);
        assert_eq!(prefill_resp["timings"]["prompt_per_second"], 510.2);
        assert_eq!(prefill_resp["timings"]["cache_n"], 333);
    }

    #[tokio::test]
    async fn recorded_request_exposes_id_slot_stream_options_and_sampling_fields() {
        let state = MockState::new();
        let _ = handle_chat_completions(
            State(state.clone()),
            HeaderMap::new(),
            Json(json!({
                "model": "qwen-loaded",
                "stream": true,
                "id_slot": 3,
                "stream_options": {"include_usage": true},
                "temperature": 0.6,
                "repeat_last_n": 256,
                "dry_multiplier": 0.8,
                "messages": [{"role": "user", "content": "hi"}]
            })),
        )
        .await;
        let reqs = state.recorded_requests();
        assert_eq!(reqs.len(), 1);
        let rec = &reqs[0];
        assert_eq!(rec.id_slot(), Some(3));
        assert_eq!(
            rec.stream_options(),
            Some(&json!({"include_usage": true}))
        );
        assert!(rec.has_field("dry_multiplier"));
        assert!(!rec.has_field("presence_penalty"));
        let sampling = rec.sampling_fields();
        assert_eq!(sampling.get("temperature"), Some(&json!(0.6)));
        assert_eq!(sampling.get("repeat_last_n"), Some(&json!(256)));
        assert_eq!(sampling.get("dry_multiplier"), Some(&json!(0.8)));
        assert!(!sampling.contains_key("presence_penalty"));
    }
}
