use crate::auth::AuthContext;
use crate::backend::{
    CancelConversationTasksRequest, CreateAgentRequest, CreateTaskRequest, CreateWebhookRequest,
    CreateWebhookResponse, ExecuteToolRequest, ExtensionsOverviewResponse, ForkConversationRequest,
    HttpCreateSchedule, HttpScheduleAiRequest, HttpScheduleAiResponse, HttpToggleScheduleRequest,
    HttpUpdateSchedule, ImportMcpRequest, ImportMemoriesRequest, ImportPluginRequest,
    ImportSkillRequest, McpServerDto, MessageActionRequest, MkdirRequest, PluginInfoDto,
    ReorderConversationsRequest, SandboxStateDto, SendAgentMessageRequest, SendMessageRequest,
    SendMessageResponse, SkillDetailResponse, SkillInfoDto, SlashCommandRequest,
    ToggleExtensionRequest, UpdateAgentRequest, UpdateSandboxRequest, UpdateToolPolicyRequest,
    UploadWorkspaceFileRequest, WebhookTriggerResponse, WorkspaceFileEntry,
    WorkspaceFilesResponse, WriteBrainFileRequest,
};
use crate::error::{HttpError, HttpResult};
use crate::HttpState;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use futures_util::stream::Stream;
use ps5_agent_core::{
    new_id, now_iso, AgentCardView, AgentDefinition, AgentEnvelope, AgentError, ApprovalRecord,
    AuditLogEntry, BrainFileDetail, BrainFileSummary, ChatMessage, Conversation, ConversationMode,
    DoctorFixReport, DoctorReport, MemoryItem, MemoryScope, MetricsSnapshot, Ps5LmCapabilities,
    ScheduleRecord, SlashCommandResponse, StreamEvent, SystemOneRequest, SystemOneResponse,
    TaskEvent, TaskRecord, TaskStatus, ToolExecutionRecord, ToolSpec, WebhookView,
};
use ps5_agent_logging::{LogEntry, LogLevel};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast::error::RecvError;

const MAX_SSE_CONNECTIONS: usize = 32;

fn validate_id(id: &str, label: &str) -> HttpResult<()> {
    let trimmed = id.trim();
    if id != trimmed
        || trimmed.is_empty()
        || trimmed == "."
        || trimmed == ".."
        || trimmed.len() > 128
        || trimmed
            .chars()
            .any(|c| c.is_control() || c == '/' || c == '\\')
    {
        return Err(HttpError::BadRequest(format!("Invalid {label} identifier")));
    }
    Ok(())
}

fn normalize_optional_id<'a>(id: Option<&'a str>, label: &str) -> HttpResult<Option<&'a str>> {
    let Some(raw) = id else {
        return Ok(None);
    };
    if raw.trim().is_empty() {
        return Ok(None);
    }
    validate_id(raw, label)?;
    Ok(Some(raw))
}

fn normalize_optional_owned_id(id: &mut Option<String>, label: &str) -> HttpResult<()> {
    if let Some(raw) = id.as_ref() {
        if raw.is_empty() {
            *id = None;
        } else {
            validate_id(raw, label)?;
        }
    }
    Ok(())
}

fn validate_extension_kind(kind: &str) -> HttpResult<&str> {
    let trimmed = kind.trim();
    match trimmed {
        "skill" | "plugin" | "mcp" => Ok(trimmed),
        _ => Err(HttpError::BadRequest(
            "Extension kind must be 'skill', 'plugin', or 'mcp'".to_string(),
        )),
    }
}

fn clamp_limit(limit: Option<usize>, default: usize) -> usize {
    match limit {
        Some(0) => 10_000,
        Some(n) => n.clamp(1, 10_000),
        None => default.clamp(1, 10_000),
    }
}

fn parse_optional_json<T: serde::de::DeserializeOwned + Default>(body: &[u8]) -> HttpResult<T> {
    if body.iter().all(|b| b.is_ascii_whitespace()) {
        return Ok(T::default());
    }
    serde_json::from_slice::<T>(body)
        .map_err(|e| HttpError::BadRequest(format!("Invalid JSON body: {e}")))
}

fn is_sensitive_setting_key(key: &str) -> bool {
    let k = key.trim().to_ascii_lowercase();
    if k.ends_with("_tokens")
        || k == "tokens"
        || k == "timings_per_token"
        || k.ends_with(".timings_per_token")
    {
        return false;
    }
    k.starts_with("auth.")
        || k.contains("password")
        || k.contains("secret")
        || k.contains("token")
        || k.contains("api_key")
        || k.contains("private_key")
}

// ---------- Static Dashboard Routes ----------

pub async fn index_html(State(state): State<Arc<HttpState>>) -> Response {
    let mut resp = (StatusCode::OK, state.assets.index_html).into_response();
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
    );
    resp
}

pub async fn styles_css(State(state): State<Arc<HttpState>>) -> Response {
    let mut resp = (StatusCode::OK, state.assets.styles_css).into_response();
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/css; charset=utf-8"),
    );
    resp
}

pub async fn app_js(State(state): State<Arc<HttpState>>) -> Response {
    let mut resp = (StatusCode::OK, state.assets.app_js).into_response();
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/javascript; charset=utf-8"),
    );
    resp
}

// ---------- Health, Metrics, SSE ----------

pub async fn health_handler(
    State(state): State<Arc<HttpState>>,
    auth: Option<Extension<AuthContext>>,
) -> Json<serde_json::Value> {
    let degraded = state.is_network_degraded();
    let status = if degraded { "degraded" } else { "ok" };
    let version = env!("CARGO_PKG_VERSION");

    if auth.is_some() {
        let metrics = state.backend.metrics().await;
        Json(serde_json::json!({
            "status": status,
            "version": version,
            "uptime_secs": metrics.uptime_secs,
            "storage_driver": state.store.driver_name(),
            "ps5lm_reachable": metrics.ps5lm_reachable,
            "ps5lm_loaded_model": metrics.ps5lm_loaded_model,
            "network_degraded": degraded,
        }))
    } else {
        Json(serde_json::json!({
            "status": status,
            "version": version,
        }))
    }
}

pub async fn metrics_handler(State(state): State<Arc<HttpState>>) -> Json<MetricsSnapshot> {
    let mut snap = state.backend.metrics().await;
    snap.storage_driver = state.store.driver_name().to_string();
    snap.network_degraded = snap.network_degraded || state.is_network_degraded();
    let (http_total, http_avg_ms) = state.http_metrics();
    if snap.http_requests_total == 0 {
        snap.http_requests_total = http_total;
        snap.http_avg_ms = http_avg_ms;
    }
    Json(snap)
}

fn stream_event_name(ev: &StreamEvent) -> &'static str {
    ev.event_name()
}

struct SseConnectionGuard {
    state: Arc<HttpState>,
}

impl SseConnectionGuard {
    fn acquire(state: Arc<HttpState>) -> HttpResult<Self> {
        let mut current = state.active_sse_connections.load(Ordering::Acquire);
        loop {
            if current >= MAX_SSE_CONNECTIONS {
                return Err(HttpError::TooManyRequests {
                    retry_after_secs: 5,
                    message: "Too many concurrent SSE connections",
                });
            }
            match state.active_sse_connections.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(Self { state }),
                Err(observed) => current = observed,
            }
        }
    }
}

impl Drop for SseConnectionGuard {
    fn drop(&mut self) {
        self.state
            .active_sse_connections
            .fetch_sub(1, Ordering::AcqRel);
    }
}

pub async fn events_sse_handler(
    State(state): State<Arc<HttpState>>,
) -> HttpResult<Sse<impl Stream<Item = std::result::Result<Event, Infallible>>>> {
    let guard = SseConnectionGuard::acquire(state.clone())?;
    let rx = state.events_tx.subscribe();
    let shutdown = state.shutdown.clone();

    let stream = async_stream::stream(rx, shutdown, guard);
    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keepalive"),
    ))
}

const SSE_LAG_ALERT_COOLDOWN: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub(crate) struct LagCoalescer {
    cooldown: Duration,
    last_alert_at: Option<std::time::Instant>,
    pending_skipped: u64,
}

impl LagCoalescer {
    pub(crate) fn new(cooldown: Duration) -> Self {
        Self {
            cooldown,
            last_alert_at: None,
            pending_skipped: 0,
        }
    }

    pub(crate) fn record_lag(&mut self, skipped: u64, now: std::time::Instant) -> Option<u64> {
        let total = self.pending_skipped.saturating_add(skipped);
        let ready = match self.last_alert_at {
            None => true,
            Some(last) => now.saturating_duration_since(last) >= self.cooldown,
        };
        if ready && total > 0 {
            self.pending_skipped = 0;
            self.last_alert_at = Some(now);
            Some(total)
        } else {
            self.pending_skipped = total;
            None
        }
    }

    pub(crate) fn take_due(&mut self, now: std::time::Instant) -> Option<u64> {
        if self.pending_skipped == 0 {
            return None;
        }
        let ready = match self.last_alert_at {
            None => true,
            Some(last) => now.saturating_duration_since(last) >= self.cooldown,
        };
        if ready {
            let total = self.pending_skipped;
            self.pending_skipped = 0;
            self.last_alert_at = Some(now);
            Some(total)
        } else {
            None
        }
    }
}

mod async_stream {
    use super::*;
    use futures_util::stream;
    use tokio::sync::broadcast::Receiver;
    use tokio_util::sync::CancellationToken;

    struct SseUnfoldState<G, F> {
        rx: Receiver<StreamEvent>,
        shutdown: CancellationToken,
        guard: G,
        on_lag: F,
        coalescer: LagCoalescer,
        pending_event: Option<Event>,
        closed: bool,
    }

    fn build_lag_alert_event(skipped: u64) -> Option<Event> {
        let resync_payload = serde_json::json!({
            "type": "system_alert",
            "level": "warn",
            "resync": true,
            "resync_required": true,
            "skipped": skipped,
            "message": format!("SSE stream lagged; skipped {skipped} events; resync required"),
        });
        Event::default()
            .event("system_alert")
            .json_data(&resync_payload)
            .ok()
    }

    pub fn stream(
        rx: Receiver<StreamEvent>,
        shutdown: CancellationToken,
        guard: SseConnectionGuard,
    ) -> impl Stream<Item = std::result::Result<Event, Infallible>> {
        stream_with_cooldown(
            rx,
            shutdown,
            guard,
            SSE_LAG_ALERT_COOLDOWN,
            |guard, skipped| {
                guard.state.logger.warn(
                    "http.sse",
                    format!("SSE subscriber lagged; skipped {skipped} events"),
                );
            },
        )
    }

    pub(crate) fn stream_with_cooldown<G, F>(
        rx: Receiver<StreamEvent>,
        shutdown: CancellationToken,
        guard: G,
        cooldown: Duration,
        on_lag: F,
    ) -> impl Stream<Item = std::result::Result<Event, Infallible>>
    where
        F: FnMut(&G, u64),
    {
        let initial = SseUnfoldState {
            rx,
            shutdown,
            guard,
            on_lag,
            coalescer: LagCoalescer::new(cooldown),
            pending_event: None,
            closed: false,
        };
        stream::unfold(initial, |mut state| async move {
            if let Some(event) = state.pending_event.take() {
                if state.shutdown.is_cancelled() {
                    return None;
                }
                return Some((Ok(event), state));
            }
            if state.closed {
                return None;
            }
            loop {
                tokio::select! {
                    biased;
                    _ = state.shutdown.cancelled() => {
                        return None;
                    }
                    res = state.rx.recv() => {
                        match res {
                            Ok(ev) => {
                                let name = stream_event_name(&ev);
                                if let Ok(event) = Event::default().event(name).json_data(&ev) {
                                    if let Some(skipped) = state.coalescer.take_due(std::time::Instant::now()) {
                                        (state.on_lag)(&state.guard, skipped);
                                        if let Some(alert) = build_lag_alert_event(skipped) {
                                            state.pending_event = Some(event);
                                            return Some((Ok(alert), state));
                                        }
                                    }
                                    return Some((Ok(event), state));
                                }
                            }
                            Err(RecvError::Lagged(skipped)) => {
                                if let Some(total_skipped) = state.coalescer.record_lag(skipped, std::time::Instant::now()) {
                                    (state.on_lag)(&state.guard, total_skipped);
                                    if let Some(event) = build_lag_alert_event(total_skipped) {
                                        return Some((Ok(event), state));
                                    }
                                }
                            }
                            Err(RecvError::Closed) => {
                                state.closed = true;
                                if let Some(skipped) = state.coalescer.take_due(std::time::Instant::now()) {
                                    (state.on_lag)(&state.guard, skipped);
                                    if let Some(event) = build_lag_alert_event(skipped) {
                                        return Some((Ok(event), state));
                                    }
                                }
                                return None;
                            }
                        }
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod sse_lag_coalesce_tests {
    use super::*;
    use axum::body::to_bytes;
    use tokio::sync::broadcast;
    use tokio_util::sync::CancellationToken;

    fn sample_stream_event(seq: usize) -> StreamEvent {
        StreamEvent::AgentMessageSent {
            envelope_id: format!("env-{seq}"),
            sender_agent_id: "commander".to_string(),
            recipient_agent_id: "worker".to_string(),
            message_type: "AGENT_MESSAGE".to_string(),
            status: "DELIVERED".to_string(),
        }
    }

    #[test]
    fn lag_coalescer_batches_bursts_within_cooldown() {
        let cooldown = Duration::from_secs(5);
        let t0 = std::time::Instant::now();
        let mut c = LagCoalescer::new(cooldown);

        assert_eq!(c.record_lag(3, t0), Some(3));
        assert_eq!(c.record_lag(4, t0 + Duration::from_millis(500)), None);
        assert_eq!(c.record_lag(5, t0 + Duration::from_secs(4)), None);
        assert_eq!(c.take_due(t0 + Duration::from_secs(4)), None);

        assert_eq!(c.take_due(t0 + Duration::from_secs(5)), Some(9));
        assert_eq!(c.take_due(t0 + Duration::from_secs(5)), None);
        assert_eq!(c.record_lag(2, t0 + Duration::from_secs(10)), Some(2));
    }

    #[tokio::test]
    async fn sse_stream_coalesces_repeated_lag_and_flushes_after_cooldown() {
        let (tx, rx) = broadcast::channel::<StreamEvent>(2);
        let shutdown = CancellationToken::new();
        let observed_skips = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
        let observed_ref = Arc::clone(&observed_skips);

        let stream = async_stream::stream_with_cooldown(
            rx,
            shutdown,
            (),
            Duration::from_millis(60),
            move |_, skipped| {
                observed_ref.lock().unwrap().push(skipped);
            },
        );
        let sse_resp = Sse::new(stream).into_response();
        let body_task =
            tokio::spawn(async move { to_bytes(sse_resp.into_body(), 1024 * 1024).await.unwrap() });

        // First burst overflows capacity=2 (lags 4 events).
        for i in 0..6 {
            let _ = tx.send(sample_stream_event(i));
        }
        tokio::time::sleep(Duration::from_millis(15)).await;

        // Second burst inside 60ms cooldown overflows again without immediately emitting a 2nd alert.
        for i in 6..12 {
            let _ = tx.send(sample_stream_event(i));
        }
        tokio::time::sleep(Duration::from_millis(75)).await;

        // Closing the channel after cooldown flushes pending skipped count cleanly.
        drop(tx);
        let bytes = body_task.await.unwrap();
        let text = String::from_utf8_lossy(&bytes);
        let alert_count = text.matches("event: system_alert").count();
        assert_eq!(alert_count, 2, "expected 1 initial + 1 flushed alert, got:\n{text}");
        assert!(text.contains("env-11"), "expected latest live event preserved:\n{text}");
        let skips = observed_skips.lock().unwrap();
        assert_eq!(skips.len(), 2);
        assert!(skips.iter().sum::<u64>() >= 8);
    }
}

// ---------- Conversations ----------

#[derive(Debug, Deserialize)]
pub struct ListConversationsQuery {
    #[serde(default)]
    pub include_archived: bool,
}

pub async fn list_conversations(
    State(state): State<Arc<HttpState>>,
    Query(q): Query<ListConversationsQuery>,
) -> HttpResult<Json<Vec<Conversation>>> {
    Ok(Json(state.store.list_conversations(q.include_archived)?))
}

#[derive(Debug, Deserialize)]
pub struct CreateConversationRequest {
    pub title: String,
    pub mode: ConversationMode,
}

pub async fn create_conversation(
    State(state): State<Arc<HttpState>>,
    Json(req): Json<CreateConversationRequest>,
) -> HttpResult<(StatusCode, Json<Conversation>)> {
    let title = req.title.trim();
    if title.is_empty() || title.len() > 200 {
        return Err(HttpError::BadRequest(
            "Conversation title must be 1..=200 characters".to_string(),
        ));
    }
    let existing = state.store.list_conversations(true)?;
    let sort_order = existing
        .iter()
        .map(|c| c.sort_order)
        .min()
        .map(|m| m.saturating_sub(10))
        .unwrap_or(0);
    let now = now_iso();
    let conv = Conversation {
        id: new_id(),
        title: title.to_string(),
        mode: req.mode,
        created_at: now.clone(),
        updated_at: now,
        archived: false,
        sort_order,
    };
    state.store.create_conversation(&conv)?;
    Ok((StatusCode::CREATED, Json(conv)))
}

#[derive(Debug, Deserialize)]
pub struct GetConversationQuery {
    #[serde(default)]
    pub include_archived: bool,
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct ConversationDetailResponse {
    pub conversation: Conversation,
    pub messages: Vec<ChatMessage>,
}

pub async fn get_conversation(
    State(state): State<Arc<HttpState>>,
    Path(id): Path<String>,
    Query(q): Query<GetConversationQuery>,
) -> HttpResult<Json<ConversationDetailResponse>> {
    validate_id(&id, "conversation")?;
    let conv = state
        .store
        .get_conversation(&id)?
        .ok_or_else(|| HttpError::NotFound(format!("Conversation {id} not found")))?;
    let limit = clamp_limit(q.limit, 100);
    let messages = state
        .store
        .list_chat_messages(&id, q.include_archived, limit)?;
    Ok(Json(ConversationDetailResponse {
        conversation: conv,
        messages,
    }))
}

pub async fn delete_conversation(
    State(state): State<Arc<HttpState>>,
    Path(id): Path<String>,
) -> HttpResult<Json<serde_json::Value>> {
    validate_id(&id, "conversation")?;
    let deleted = state.store.delete_conversation(&id)?;
    if !deleted {
        return Err(HttpError::NotFound(format!("Conversation {id} not found")));
    }
    Ok(Json(serde_json::json!({ "deleted": true, "id": id })))
}

#[derive(Debug, Deserialize)]
pub struct SearchConversationsQuery {
    pub q: String,
    pub limit: Option<usize>,
}

pub async fn search_conversations(
    State(state): State<Arc<HttpState>>,
    Query(q): Query<SearchConversationsQuery>,
) -> HttpResult<Json<Vec<ChatMessage>>> {
    let query = q.q.trim();
    if query.is_empty() || query.len() > 256 {
        return Err(HttpError::BadRequest(
            "Search query must be 1..=256 characters".to_string(),
        ));
    }
    let limit = clamp_limit(q.limit, 50);
    Ok(Json(state.store.search_chat_messages(query, limit)?))
}

pub async fn list_conversation_messages(
    State(state): State<Arc<HttpState>>,
    Path(id): Path<String>,
    Query(q): Query<GetConversationQuery>,
) -> HttpResult<Json<Vec<ChatMessage>>> {
    validate_id(&id, "conversation")?;
    state
        .store
        .get_conversation(&id)?
        .ok_or_else(|| HttpError::NotFound(format!("Conversation {id} not found")))?;
    let limit = clamp_limit(q.limit, 500);
    let messages = state
        .store
        .list_chat_messages(&id, q.include_archived, limit)?;
    Ok(Json(messages))
}

pub async fn post_conversation_message(
    State(state): State<Arc<HttpState>>,
    Path(id): Path<String>,
    Json(req): Json<SendMessageRequest>,
) -> HttpResult<(StatusCode, Json<SendMessageResponse>)> {
    validate_id(&id, "conversation")?;
    if let Some(ref agent_id) = req.agent_id {
        validate_id(agent_id, "agent")?;
    }
    let trimmed = req.content.trim();
    if (trimmed.is_empty() && req.attachments.is_empty()) || req.content.len() > 64_000 {
        return Err(HttpError::BadRequest(
            "Message content must be 1..=64000 characters".to_string(),
        ));
    }
    if req.attachments.len() > 16 {
        return Err(HttpError::BadRequest(
            "Too many attachments (maximum 16)".to_string(),
        ));
    }
    for att in &req.attachments {
        let name = att.name.trim();
        if name.is_empty() || name.len() > 255 || name.contains('\0') {
            return Err(HttpError::BadRequest(
                "Attachment name must be 1..=255 valid characters".to_string(),
            ));
        }
    }
    let resp = state.backend.send_conversation_message(&id, req).await?;
    Ok((StatusCode::CREATED, Json(resp)))
}

// ---------- Agents ----------

pub async fn list_agents(State(state): State<Arc<HttpState>>) -> HttpResult<Json<Vec<AgentCardView>>> {
    Ok(Json(state.backend.list_agent_cards().await?))
}

pub async fn create_agent(
    State(state): State<Arc<HttpState>>,
    Extension(auth): Extension<AuthContext>,
    Json(req): Json<CreateAgentRequest>,
) -> HttpResult<(StatusCode, Json<AgentDefinition>)> {
    if req.name.trim().is_empty() || req.name.len() > 100 {
        return Err(HttpError::BadRequest(
            "Agent name must be 1..=100 characters".to_string(),
        ));
    }
    let created = state.backend.create_agent(req).await?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "agent.create".to_string(),
        target: created.id.clone(),
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: serde_json::json!({ "name": created.name }).to_string(),
    });
    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn get_agent(
    State(state): State<Arc<HttpState>>,
    Path(id): Path<String>,
) -> HttpResult<Json<AgentCardView>> {
    validate_id(&id, "agent")?;
    Ok(Json(state.backend.get_agent_card(&id).await?))
}

pub async fn update_agent(
    State(state): State<Arc<HttpState>>,
    Extension(auth): Extension<AuthContext>,
    Path(id): Path<String>,
    Json(req): Json<UpdateAgentRequest>,
) -> HttpResult<Json<AgentDefinition>> {
    validate_id(&id, "agent")?;
    if let Some(ref name) = req.name {
        if name.trim().is_empty() || name.len() > 100 {
            return Err(HttpError::BadRequest(
                "Agent name must be 1..=100 characters".to_string(),
            ));
        }
    }
    let updated = state.backend.update_agent(&id, req).await?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "agent.update".to_string(),
        target: id,
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: "{}".to_string(),
    });
    Ok(Json(updated))
}

pub async fn delete_agent(
    State(state): State<Arc<HttpState>>,
    Extension(auth): Extension<AuthContext>,
    Path(id): Path<String>,
) -> HttpResult<Json<serde_json::Value>> {
    validate_id(&id, "agent")?;
    let deleted = state.backend.delete_agent(&id).await?;
    if !deleted {
        return Err(HttpError::NotFound(format!("Agent {id} not found")));
    }
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "agent.delete".to_string(),
        target: id.clone(),
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: "{}".to_string(),
    });
    Ok(Json(serde_json::json!({ "deleted": true, "id": id })))
}

pub async fn enable_agent(
    State(state): State<Arc<HttpState>>,
    Path(id): Path<String>,
) -> HttpResult<Json<AgentDefinition>> {
    validate_id(&id, "agent")?;
    Ok(Json(state.backend.set_agent_enabled(&id, true).await?))
}

pub async fn disable_agent(
    State(state): State<Arc<HttpState>>,
    Path(id): Path<String>,
) -> HttpResult<Json<AgentDefinition>> {
    validate_id(&id, "agent")?;
    Ok(Json(state.backend.set_agent_enabled(&id, false).await?))
}

#[derive(Debug, Deserialize)]
pub struct AgentMemoryQuery {
    #[serde(default)]
    pub include_archived: bool,
    pub limit: Option<usize>,
}

pub async fn get_agent_memory(
    State(state): State<Arc<HttpState>>,
    Path(id): Path<String>,
    Query(q): Query<AgentMemoryQuery>,
) -> HttpResult<Json<Vec<MemoryItem>>> {
    validate_id(&id, "agent")?;
    if state.store.get_agent(&id)?.is_none() {
        return Err(HttpError::NotFound(format!("Agent {id} not found")));
    }
    let limit = clamp_limit(q.limit, 100);
    let items = state
        .store
        .query_memories(None, None, Some(&id), q.include_archived, limit)?;
    Ok(Json(items))
}

// ---------- Agent Messages ----------

#[derive(Debug, Deserialize)]
pub struct ListAgentMessagesQuery {
    pub task_id: Option<String>,
    pub agent_id: Option<String>,
    pub limit: Option<usize>,
}

pub async fn list_agent_messages(
    State(state): State<Arc<HttpState>>,
    Query(q): Query<ListAgentMessagesQuery>,
) -> HttpResult<Json<Vec<AgentEnvelope>>> {
    let task_id = normalize_optional_id(q.task_id.as_deref(), "task")?;
    let agent_id = normalize_optional_id(q.agent_id.as_deref(), "agent")?;
    let limit = clamp_limit(q.limit, 100);
    Ok(Json(state.store.list_agent_messages(
        task_id,
        agent_id,
        limit,
    )?))
}

pub async fn send_agent_message(
    State(state): State<Arc<HttpState>>,
    Json(mut req): Json<SendAgentMessageRequest>,
) -> HttpResult<(StatusCode, Json<AgentEnvelope>)> {
    validate_id(&req.sender_agent_id, "sender_agent_id")?;
    validate_id(&req.recipient_agent_id, "recipient_agent_id")?;
    normalize_optional_owned_id(&mut req.task_id, "task")?;
    if req.content.trim().is_empty() || req.content.len() > 32_000 {
        return Err(HttpError::BadRequest(
            "Agent message content must be 1..=32000 characters".to_string(),
        ));
    }
    let env = state.backend.send_agent_message(req).await?;
    Ok((StatusCode::CREATED, Json(env)))
}

// ---------- Tasks ----------

#[derive(Debug, Deserialize)]
pub struct ListTasksQuery {
    pub status: Option<String>,
    pub agent_id: Option<String>,
    pub conversation_id: Option<String>,
    pub limit: Option<usize>,
}

enum TaskStatusFilter {
    Active,
    Exact(TaskStatus),
}

fn parse_task_status_filter(raw: Option<&str>) -> HttpResult<Option<TaskStatusFilter>> {
    let Some(s) = raw.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(None);
    };
    match s.to_ascii_lowercase().as_str() {
        "active" => Ok(Some(TaskStatusFilter::Active)),
        "pending" => Ok(Some(TaskStatusFilter::Exact(TaskStatus::Pending))),
        "assigned" => Ok(Some(TaskStatusFilter::Exact(TaskStatus::Assigned))),
        "running" => Ok(Some(TaskStatusFilter::Exact(TaskStatus::Running))),
        "waiting" => Ok(Some(TaskStatusFilter::Exact(TaskStatus::Waiting))),
        "completed" => Ok(Some(TaskStatusFilter::Exact(TaskStatus::Completed))),
        "failed" => Ok(Some(TaskStatusFilter::Exact(TaskStatus::Failed))),
        "cancelled" | "canceled" => Ok(Some(TaskStatusFilter::Exact(TaskStatus::Cancelled))),
        _ => Err(HttpError::BadRequest(format!(
            "Invalid task status filter: {s}"
        ))),
    }
}

pub async fn list_tasks(
    State(state): State<Arc<HttpState>>,
    Query(q): Query<ListTasksQuery>,
) -> HttpResult<Json<Vec<TaskRecord>>> {
    let status_filter = parse_task_status_filter(q.status.as_deref())?;
    let agent_filter = normalize_optional_id(q.agent_id.as_deref(), "agent")?;
    let conv_filter = normalize_optional_id(q.conversation_id.as_deref(), "conversation")?;
    let limit = clamp_limit(q.limit, 500);

    if status_filter.is_none() && agent_filter.is_none() && conv_filter.is_none() {
        return Ok(Json(state.store.list_tasks(limit)?));
    }

    let mut tasks = state.store.list_tasks(10_000)?;
    tasks.retain(|task| {
        if let Some(ref sf) = status_filter {
            let matches_status = match sf {
                TaskStatusFilter::Active => !task.status.is_terminal(),
                TaskStatusFilter::Exact(expected) => task.status == *expected,
            };
            if !matches_status {
                return false;
            }
        }
        if let Some(aid) = agent_filter {
            if task.assigned_agent_id != aid {
                return false;
            }
        }
        if let Some(cid) = conv_filter {
            if task.conversation_id.as_deref() != Some(cid) {
                return false;
            }
        }
        true
    });
    tasks.truncate(limit);
    Ok(Json(tasks))
}

pub async fn create_task(
    State(state): State<Arc<HttpState>>,
    Json(mut req): Json<CreateTaskRequest>,
) -> HttpResult<(StatusCode, Json<TaskRecord>)> {
    normalize_optional_owned_id(&mut req.conversation_id, "conversation")?;
    normalize_optional_owned_id(&mut req.assigned_agent_id, "agent")?;
    if req.user_request.trim().is_empty() || req.user_request.len() > 64_000 {
        return Err(HttpError::BadRequest(
            "Task user_request must be 1..=64000 characters".to_string(),
        ));
    }
    let task = state.backend.create_task(req).await?;
    Ok((StatusCode::CREATED, Json(task)))
}

pub async fn get_task(
    State(state): State<Arc<HttpState>>,
    Path(id): Path<String>,
) -> HttpResult<Json<TaskRecord>> {
    validate_id(&id, "task")?;
    let task = state
        .store
        .get_task(&id)?
        .ok_or_else(|| HttpError::NotFound(format!("Task {id} not found")))?;
    Ok(Json(task))
}

pub async fn cancel_task(
    State(state): State<Arc<HttpState>>,
    Path(id): Path<String>,
) -> HttpResult<Json<TaskRecord>> {
    validate_id(&id, "task")?;
    Ok(Json(state.backend.cancel_task(&id).await?))
}

pub async fn get_task_events(
    State(state): State<Arc<HttpState>>,
    Path(id): Path<String>,
) -> HttpResult<Json<Vec<TaskEvent>>> {
    validate_id(&id, "task")?;
    if state.store.get_task(&id)?.is_none() {
        return Err(HttpError::NotFound(format!("Task {id} not found")));
    }
    Ok(Json(state.store.list_task_events(&id)?))
}

// ---------- Approvals ----------

#[derive(Debug, Deserialize)]
pub struct ListApprovalsQuery {
    #[serde(default)]
    pub pending_only: bool,
    pub limit: Option<usize>,
}

pub async fn list_approvals(
    State(state): State<Arc<HttpState>>,
    Query(q): Query<ListApprovalsQuery>,
) -> HttpResult<Json<Vec<ApprovalRecord>>> {
    let limit = clamp_limit(q.limit, 100);
    Ok(Json(state.store.list_approvals(q.pending_only, limit)?))
}

pub async fn approve_action(
    State(state): State<Arc<HttpState>>,
    Extension(auth): Extension<AuthContext>,
    Path(id): Path<String>,
) -> HttpResult<Json<ApprovalRecord>> {
    validate_id(&id, "approval")?;
    let rec = state
        .backend
        .resolve_approval(&id, true, &auth.username)
        .await?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "approval.approve".to_string(),
        target: id,
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: "{}".to_string(),
    });
    Ok(Json(rec))
}

pub async fn deny_action(
    State(state): State<Arc<HttpState>>,
    Extension(auth): Extension<AuthContext>,
    Path(id): Path<String>,
) -> HttpResult<Json<ApprovalRecord>> {
    validate_id(&id, "approval")?;
    let rec = state
        .backend
        .resolve_approval(&id, false, &auth.username)
        .await?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "approval.deny".to_string(),
        target: id,
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: "{}".to_string(),
    });
    Ok(Json(rec))
}

// ---------- Memories ----------

#[derive(Debug, Deserialize)]
pub struct ListMemoriesQuery {
    pub scope: Option<MemoryScope>,
    pub namespace: Option<String>,
    pub owner_agent_id: Option<String>,
    #[serde(default)]
    pub include_archived: bool,
    pub limit: Option<usize>,
}

pub async fn list_memories(
    State(state): State<Arc<HttpState>>,
    Query(q): Query<ListMemoriesQuery>,
) -> HttpResult<Json<Vec<MemoryItem>>> {
    let namespace = q.namespace.as_deref().map(str::trim).filter(|s| !s.is_empty());
    let owner_agent_id = normalize_optional_id(q.owner_agent_id.as_deref(), "agent")?;
    let limit = clamp_limit(q.limit, 500);
    Ok(Json(state.store.query_memories(
        q.scope,
        namespace,
        owner_agent_id,
        q.include_archived,
        limit,
    )?))
}

#[derive(Debug, Deserialize)]
pub struct CreateMemoryRequest {
    pub scope: MemoryScope,
    pub namespace: String,
    #[serde(default)]
    pub owner_agent_id: Option<String>,
    #[serde(default)]
    pub conversation_id: Option<String>,
    #[serde(default)]
    pub task_id: Option<String>,
    pub key: String,
    pub content: String,
    #[serde(default)]
    pub summary: Option<String>,
    #[serde(default = "default_importance")]
    pub importance: u8,
    #[serde(default)]
    pub tags: Vec<String>,
}

fn default_importance() -> u8 {
    5
}

pub async fn create_memory(
    State(state): State<Arc<HttpState>>,
    Json(mut req): Json<CreateMemoryRequest>,
) -> HttpResult<(StatusCode, Json<MemoryItem>)> {
    normalize_optional_owned_id(&mut req.owner_agent_id, "agent")?;
    normalize_optional_owned_id(&mut req.conversation_id, "conversation")?;
    normalize_optional_owned_id(&mut req.task_id, "task")?;
    if req.namespace.trim().is_empty() || req.namespace.len() > 128 {
        return Err(HttpError::BadRequest(
            "Memory namespace must be 1..=128 characters".to_string(),
        ));
    }
    if req.key.trim().is_empty() || req.key.len() > 200 {
        return Err(HttpError::BadRequest(
            "Memory key must be 1..=200 characters".to_string(),
        ));
    }
    if req.content.trim().is_empty() || req.content.len() > 64_000 {
        return Err(HttpError::BadRequest(
            "Memory content must be 1..=64000 characters".to_string(),
        ));
    }

    let now = now_iso();
    let item = MemoryItem {
        id: new_id(),
        scope: req.scope,
        namespace: req.namespace.trim().to_string(),
        owner_agent_id: req.owner_agent_id,
        conversation_id: req.conversation_id,
        task_id: req.task_id,
        key: req.key.trim().to_string(),
        content: req.content,
        summary: req.summary,
        importance: req.importance.clamp(1, 10),
        tags: req.tags,
        archived: false,
        created_at: now.clone(),
        updated_at: now,
    };
    state.store.upsert_memory(&item)?;
    Ok((StatusCode::CREATED, Json(item)))
}

pub async fn delete_memory(
    State(state): State<Arc<HttpState>>,
    Path(id): Path<String>,
) -> HttpResult<Json<serde_json::Value>> {
    validate_id(&id, "memory")?;
    let deleted = state.store.delete_memory(&id)?;
    if !deleted {
        return Err(HttpError::NotFound(format!("Memory {id} not found")));
    }
    Ok(Json(serde_json::json!({ "deleted": true, "id": id })))
}

// ---------- Tools ----------

pub async fn list_tools(State(state): State<Arc<HttpState>>) -> HttpResult<Json<Vec<ToolSpec>>> {
    Ok(Json(state.backend.list_tools().await?))
}

#[derive(Debug, Default, Deserialize)]
pub struct ToggleToolRequest {
    pub enabled: Option<bool>,
}

pub async fn toggle_tool(
    State(state): State<Arc<HttpState>>,
    Extension(auth): Extension<AuthContext>,
    Path(id): Path<String>,
    body: Option<Json<ToggleToolRequest>>,
) -> HttpResult<Json<ToolSpec>> {
    validate_id(&id, "tool")?;
    let enabled = body.and_then(|Json(b)| b.enabled);
    let spec = state.backend.toggle_tool(&id, enabled).await?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "tool.toggle".to_string(),
        target: id,
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: serde_json::json!({ "enabled": spec.enabled }).to_string(),
    });
    Ok(Json(spec))
}

pub async fn execute_tool(
    State(state): State<Arc<HttpState>>,
    Extension(auth): Extension<AuthContext>,
    Path(id): Path<String>,
    Json(mut req): Json<ExecuteToolRequest>,
) -> HttpResult<Json<ToolExecutionRecord>> {
    validate_id(&id, "tool")?;
    normalize_optional_owned_id(&mut req.agent_id, "agent")?;
    normalize_optional_owned_id(&mut req.task_id, "task")?;
    let rec = state.backend.execute_tool(&id, req).await?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "tool.execute".to_string(),
        target: id,
        severity: "info".to_string(),
        outcome: if rec.success { "success" } else { "failure" }.to_string(),
        details_json: serde_json::json!({ "execution_id": rec.id }).to_string(),
    });
    Ok(Json(rec))
}

#[derive(Debug, Deserialize)]
pub struct ListToolExecutionsQuery {
    pub agent_id: Option<String>,
    pub limit: Option<usize>,
}

pub async fn list_tool_executions(
    State(state): State<Arc<HttpState>>,
    Query(q): Query<ListToolExecutionsQuery>,
) -> HttpResult<Json<Vec<ToolExecutionRecord>>> {
    let agent_id = normalize_optional_id(q.agent_id.as_deref(), "agent")?;
    let limit = clamp_limit(q.limit, 500);
    Ok(Json(
        state
            .store
            .list_tool_executions(agent_id, limit)?,
    ))
}

// ---------- Logs & Audit Logs ----------

#[derive(Debug, Deserialize)]
pub struct ListLogsQuery {
    pub level: Option<String>,
    pub limit: Option<usize>,
}

pub async fn list_logs(
    State(state): State<Arc<HttpState>>,
    Query(q): Query<ListLogsQuery>,
) -> HttpResult<Json<Vec<LogEntry>>> {
    let level = q.level.as_deref().map(str::trim).filter(|s| !s.is_empty());
    if let Some(lvl) = level {
        if LogLevel::parse_strict(lvl).is_none() {
            return Err(HttpError::BadRequest(format!("Invalid log level: {lvl}")));
        }
    }
    let limit = clamp_limit(q.limit, 100);
    Ok(Json(state.logger.recent(level, limit)))
}

#[derive(Debug, Deserialize)]
pub struct ListAuditLogsQuery {
    pub limit: Option<usize>,
}

pub async fn list_audit_logs(
    State(state): State<Arc<HttpState>>,
    Query(q): Query<ListAuditLogsQuery>,
) -> HttpResult<Json<Vec<AuditLogEntry>>> {
    let limit = clamp_limit(q.limit, 100);
    Ok(Json(state.store.list_audit_logs(limit)?))
}

// ---------- Settings & System ----------

fn build_sanitized_settings(state: &HttpState) -> HttpResult<serde_json::Value> {
    let raw = state.store.list_settings()?;
    let mut stored = BTreeMap::new();
    for (k, v) in raw {
        if !is_sensitive_setting_key(&k) {
            stored.insert(k, v);
        }
    }
    let cfg = state
        .backend
        .current_config()
        .unwrap_or_else(|| state.config.clone());
    let mut inference_json = serde_json::to_value(&cfg.inference)
        .map_err(|e| HttpError::Internal(format!("serializing inference settings: {e}")))?;
    if let Some(obj) = inference_json.as_object_mut() {
        obj.remove("api_key");
    }
    Ok(serde_json::json!({
        "settings": stored,
        "inference": inference_json,
        "server": {
            "bind_host": cfg.server.bind_host,
            "port": cfg.server.port,
            "max_body_bytes": cfg.server.max_body_bytes,
            "upload_max_body_bytes": cfg.server.upload_max_body_bytes,
            "allow_lan_only": cfg.server.allow_lan_only,
        },
        "database": {
            "driver": cfg.database.driver,
            "path": cfg.database.path.to_string_lossy(),
            "busy_timeout_ms": cfg.database.busy_timeout_ms,
            "cache_size_kb": cfg.database.cache_size_kb,
            "wal_mode": cfg.database.wal_mode,
            "retention_days": cfg.database.retention_days,
            "max_rows_per_table": cfg.database.max_rows_per_table,
        },
        "agents": cfg.agents,
        "memory": cfg.memory,
        "tools": cfg.tools,
        "logging": {
            "level": cfg.logging.level,
            "max_file_bytes": cfg.logging.max_file_bytes,
            "ring_buffer_capacity": cfg.logging.ring_buffer_capacity,
            "enable_klog": cfg.logging.enable_klog,
            "enable_toast": cfg.logging.enable_toast,
        },
        "security": {
            "auth_enabled": cfg.security.auth_enabled,
            "admin_username": cfg.security.admin_username,
            "require_approval_for_mutating": cfg.security.require_approval_for_mutating,
            "session_ttl_hours": cfg.security.session_ttl_hours,
            "rate_limit_per_minute": cfg.security.rate_limit_per_minute,
            "login_rate_limit_per_minute": cfg.security.login_rate_limit_per_minute,
        },
        "runtime": {
            "bind_host": cfg.server.bind_host,
            "port": cfg.server.port,
            "allow_lan_only": cfg.server.allow_lan_only,
            "max_body_bytes": cfg.server.max_body_bytes,
            "upload_max_body_bytes": cfg.server.upload_max_body_bytes,
            "auth_enabled": cfg.security.auth_enabled,
            "rate_limit_per_minute": cfg.security.rate_limit_per_minute,
            "login_rate_limit_per_minute": cfg.security.login_rate_limit_per_minute,
            "require_approval_for_mutating": cfg.security.require_approval_for_mutating,
            "storage_driver": state.store.driver_name(),
            "inference_model": cfg.inference.model,
            "log_level": cfg.logging.level,
        }
    }))
}

pub async fn get_settings(State(state): State<Arc<HttpState>>) -> HttpResult<Json<serde_json::Value>> {
    Ok(Json(build_sanitized_settings(&state)?))
}

#[derive(Debug, Deserialize)]
#[serde(transparent)]
pub struct UpdateSettingsRequest(pub serde_json::Map<String, serde_json::Value>);

fn extract_kv_updates(
    raw: &serde_json::Map<String, serde_json::Value>,
) -> HttpResult<BTreeMap<String, String>> {
    let mut kv = BTreeMap::new();
    let mut inspect_map = |map: &serde_json::Map<String, serde_json::Value>| -> HttpResult<()> {
        if map.len() > 256 {
            return Err(HttpError::BadRequest(
                "Too many settings keys in single update".to_string(),
            ));
        }
        for (k, v) in map {
            if k.trim().is_empty() || k.len() > 128 {
                return Err(HttpError::BadRequest("Invalid setting key".to_string()));
            }
            if is_sensitive_setting_key(k) {
                return Err(HttpError::Forbidden(
                    "Sensitive credential settings cannot be modified via /settings",
                ));
            }
            let encoded = match v {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            if encoded.len() > 8192 {
                return Err(HttpError::BadRequest(
                    "Setting value exceeds maximum length".to_string(),
                ));
            }
            kv.insert(k.clone(), encoded);
        }
        Ok(())
    };

    if let Some(serde_json::Value::Object(settings_obj)) = raw.get("settings") {
        inspect_map(settings_obj)?;
    }

    let mut flat = serde_json::Map::new();
    for (k, v) in raw {
        if matches!(
            k.as_str(),
            "settings"
                | "inference"
                | "server"
                | "database"
                | "agents"
                | "memory"
                | "tools"
                | "logging"
                | "runtime"
                | "sandbox"
                | "security"
        ) {
            continue;
        }
        flat.insert(k.clone(), v.clone());
    }
    if !flat.is_empty() {
        inspect_map(&flat)?;
    }

    // Also guard against sensitive keys inside structured sub-objects (e.g. inference.api_key)
    for section in [
        "inference",
        "server",
        "database",
        "agents",
        "memory",
        "tools",
        "logging",
        "runtime",
        "sandbox",
        "security",
    ] {
        if let Some(serde_json::Value::Object(obj)) = raw.get(section) {
            for k in obj.keys() {
                if is_sensitive_setting_key(k) {
                    return Err(HttpError::Forbidden(
                        "Sensitive credential settings cannot be modified via /settings",
                    ));
                }
            }
        }
    }

    Ok(kv)
}

pub async fn update_settings(
    State(state): State<Arc<HttpState>>,
    Extension(auth): Extension<AuthContext>,
    Json(req): Json<UpdateSettingsRequest>,
) -> HttpResult<Json<serde_json::Value>> {
    let kv = extract_kv_updates(&req.0)?;

    state.backend.apply_settings_update(&req.0).await?;

    for (k, v) in &kv {
        state.store.set_setting(k, v)?;
    }

    let keys: Vec<String> = req.0.keys().cloned().collect();
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "settings.update".to_string(),
        target: "settings".to_string(),
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: serde_json::json!({ "keys": keys }).to_string(),
    });

    Ok(Json(build_sanitized_settings(&state)?))
}

pub async fn list_inference_models(
    State(state): State<Arc<HttpState>>,
) -> HttpResult<Json<serde_json::Value>> {
    let models = state.backend.list_inference_models().await?;
    Ok(Json(serde_json::json!({ "models": models })))
}

pub async fn evaluate_systemone(
    State(state): State<Arc<HttpState>>,
    Extension(auth): Extension<AuthContext>,
    Json(req): Json<SystemOneRequest>,
) -> HttpResult<Json<SystemOneResponse>> {
    if req.state.trim().is_empty() {
        return Err(HttpError::BadRequest(
            "SystemOne state cannot be empty".to_string(),
        ));
    }
    if req.questions.is_empty() {
        return Err(HttpError::BadRequest(
            "SystemOne requires at least one question".to_string(),
        ));
    }
    let resp = state.backend.evaluate_systemone(req).await?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "inference.systemone".to_string(),
        target: resp.model.clone(),
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: serde_json::json!({ "latency_ms": resp.latency_ms }).to_string(),
    });
    Ok(Json(resp))
}

pub async fn system_checkpoint(
    State(state): State<Arc<HttpState>>,
    Extension(auth): Extension<AuthContext>,
) -> HttpResult<Json<serde_json::Value>> {
    state.store.checkpoint()?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "system.checkpoint".to_string(),
        target: "database".to_string(),
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: "{}".to_string(),
    });
    Ok(Json(serde_json::json!({ "checkpointed": true })))
}

pub async fn system_shutdown(
    State(state): State<Arc<HttpState>>,
    Extension(auth): Extension<AuthContext>,
) -> HttpResult<(StatusCode, Json<serde_json::Value>)> {
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "system.shutdown".to_string(),
        target: "daemon".to_string(),
        severity: "warn".to_string(),
        outcome: "initiated".to_string(),
        details_json: "{}".to_string(),
    });
    state.logger.warn("system", "Shutdown requested via HTTP API");
    state.shutdown.cancel();
    Ok((
        StatusCode::ACCEPTED,
        Json(serde_json::json!({ "shutting_down": true })),
    ))
}

// ---------- Workspace Files API ----------

#[derive(Debug, Deserialize)]
pub struct WorkspacePathQuery {
    #[serde(default)]
    pub path: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct RequiredWorkspacePathQuery {
    pub path: String,
}

fn has_parent_traversal(path: &str) -> bool {
    path.split(['/', '\\']).any(|seg| seg == "..")
}

fn validate_workspace_path_param(path: &str) -> HttpResult<()> {
    let trimmed = path.trim();
    if path != trimmed
        || trimmed.is_empty()
        || trimmed.len() > 1024
        || trimmed.contains('\0')
        || trimmed.chars().any(char::is_control)
        || has_parent_traversal(trimmed)
    {
        return Err(HttpError::BadRequest("Invalid workspace path".to_string()));
    }
    Ok(())
}

fn map_file_backend_err(err: AgentError) -> HttpError {
    match err {
        AgentError::LimitExceeded(_) => HttpError::PayloadTooLarge,
        other => HttpError::from_agent(other),
    }
}

fn file_extension_lower(name: &str) -> String {
    name.rsplit_once('.')
        .map(|(_, ext)| ext.trim().to_ascii_lowercase())
        .unwrap_or_default()
}

fn detect_language(name: &str) -> &'static str {
    match file_extension_lower(name).as_str() {
        "html" | "htm" => "html",
        "css" => "css",
        "js" | "mjs" | "cjs" => "javascript",
        "ts" | "tsx" => "typescript",
        "json" => "json",
        "rs" => "rust",
        "py" => "python",
        "md" | "markdown" => "markdown",
        "toml" => "toml",
        "yaml" | "yml" => "yaml",
        "xml" | "svg" => "xml",
        "sh" | "bash" => "bash",
        "ps1" => "powershell",
        "sql" => "sql",
        "c" | "h" => "c",
        "cpp" | "cc" | "hpp" => "cpp",
        "txt" | "log" | "csv" => "text",
        _ => "plaintext",
    }
}

fn sanitize_download_filename(raw: &str) -> String {
    let base = raw.rsplit(['/', '\\']).next().unwrap_or(raw).trim();
    let sanitized: String = base
        .chars()
        .map(|c| {
            if c.is_control() || matches!(c, '"' | '\\' | '/' | ';') {
                '_'
            } else {
                c
            }
        })
        .collect();
    let trimmed = sanitized.trim();
    if trimmed.is_empty() || trimmed == "." || trimmed == ".." {
        "download.bin".to_string()
    } else {
        trimmed.chars().take(180).collect()
    }
}

const SANDBOX_STORAGE_SHIM: &str = r#"<script>(function(){try{var s=window.localStorage;s.getItem("_");}catch(e){var m={};var a={getItem:function(k){return Object.prototype.hasOwnProperty.call(m,k)?m[k]:null;},setItem:function(k,v){m[String(k)]=String(v);},removeItem:function(k){delete m[String(k)];},clear:function(){m={};},key:function(i){return Object.keys(m)[i]||null;},get length(){return Object.keys(m).length;}};try{Object.defineProperty(window,"localStorage",{value:api_wrap(a),configurable:true});}catch(_){}try{Object.defineProperty(window,"sessionStorage",{value:api_wrap(a),configurable:true});}catch(_){}function api_wrap(x){return x;}}})();</script>"#;

fn inject_sandbox_storage_shim(html: &str) -> String {
    let lower = html.to_ascii_lowercase();
    if let Some(head_idx) = lower.find("<head") {
        if let Some(rel_end) = html[head_idx..].find('>') {
            let insert_pos = head_idx + rel_end + 1;
            let mut out = String::with_capacity(html.len() + SANDBOX_STORAGE_SHIM.len());
            out.push_str(&html[..insert_pos]);
            out.push_str(SANDBOX_STORAGE_SHIM);
            out.push_str(&html[insert_pos..]);
            return out;
        }
    }
    format!("{SANDBOX_STORAGE_SHIM}{html}")
}

fn decode_base64(input: &str) -> HttpResult<Vec<u8>> {
    let trimmed = input.trim();
    let payload = if trimmed.starts_with("data:") {
        if let Some(comma_idx) = trimmed.find(',') {
            let header_part = &trimmed[..comma_idx];
            if !header_part.to_ascii_lowercase().contains(";base64") {
                return Err(HttpError::BadRequest(
                    "Data URL payload must use base64 encoding".to_string(),
                ));
            }
            &trimmed[comma_idx + 1..]
        } else {
            return Err(HttpError::BadRequest("Malformed data URL".to_string()));
        }
    } else {
        trimmed
    };

    let clean: Vec<u8> = payload
        .bytes()
        .filter(|b| !b.is_ascii_whitespace())
        .collect();
    if clean.is_empty() {
        return Ok(Vec::new());
    }
    if clean.len() % 4 == 1 {
        return Err(HttpError::BadRequest(
            "Invalid base64 payload length".to_string(),
        ));
    }

    let decode_char = |b: u8| -> HttpResult<u8> {
        match b {
            b'A'..=b'Z' => Ok(b - b'A'),
            b'a'..=b'z' => Ok(b - b'a' + 26),
            b'0'..=b'9' => Ok(b - b'0' + 52),
            b'+' | b'-' => Ok(62),
            b'/' | b'_' => Ok(63),
            _ => Err(HttpError::BadRequest("Invalid base64 character".to_string())),
        }
    };

    let mut out = Vec::with_capacity((clean.len() / 4) * 3);
    let mut idx = 0;
    while idx < clean.len() {
        let remaining = clean.len() - idx;
        let is_last_chunk = remaining <= 4;
        let b0 = clean[idx];
        let b1 = clean[idx + 1];
        if b0 == b'=' || b1 == b'=' {
            return Err(HttpError::BadRequest("Invalid base64 padding".to_string()));
        }
        let v0 = decode_char(b0)?;
        let v1 = decode_char(b1)?;

        let b2 = if remaining > 2 { clean[idx + 2] } else { b'=' };
        let b3 = if remaining > 3 { clean[idx + 3] } else { b'=' };

        if !is_last_chunk && (b2 == b'=' || b3 == b'=') {
            return Err(HttpError::BadRequest("Unexpected base64 padding".to_string()));
        }
        if b2 == b'=' && b3 != b'=' {
            return Err(HttpError::BadRequest("Invalid base64 padding".to_string()));
        }

        out.push((v0 << 2) | (v1 >> 4));
        if b2 != b'=' {
            let v2 = decode_char(b2)?;
            out.push((v1 << 4) | (v2 >> 2));
            if b3 != b'=' {
                let v3 = decode_char(b3)?;
                out.push((v2 << 6) | v3);
            }
        }
        idx += 4;
    }

    Ok(out)
}

pub async fn list_workspace_files(
    State(state): State<Arc<HttpState>>,
    Query(q): Query<WorkspacePathQuery>,
) -> HttpResult<Json<WorkspaceFilesResponse>> {
    let normalized = q.path.as_deref().map(str::trim).filter(|s| !s.is_empty());
    if let Some(p) = normalized {
        if p.contains('\0')
            || p.len() > 1024
            || p.chars().any(char::is_control)
            || has_parent_traversal(p)
        {
            return Err(HttpError::BadRequest("Invalid workspace path".to_string()));
        }
    }
    Ok(Json(
        state
            .backend
            .list_workspace_files(normalized)
            .await
            .map_err(map_file_backend_err)?,
    ))
}

pub async fn get_workspace_file_content(
    State(state): State<Arc<HttpState>>,
    Query(q): Query<RequiredWorkspacePathQuery>,
) -> HttpResult<Json<serde_json::Value>> {
    validate_workspace_path_param(&q.path)?;
    let (name, bytes) = state
        .backend
        .read_workspace_file(&q.path)
        .await
        .map_err(map_file_backend_err)?;
    let size_bytes = bytes.len();
    let content = String::from_utf8(bytes)
        .map_err(|_| HttpError::BadRequest("File is binary and not valid UTF-8 text".to_string()))?;
    let language = detect_language(&name);
    Ok(Json(serde_json::json!({
        "path": q.path,
        "name": name,
        "content": content,
        "size_bytes": size_bytes,
        "language": language,
    })))
}

pub async fn download_workspace_file(
    State(state): State<Arc<HttpState>>,
    Query(q): Query<RequiredWorkspacePathQuery>,
) -> HttpResult<Response> {
    validate_workspace_path_param(&q.path)?;
    let (name, bytes) = state
        .backend
        .read_workspace_file(&q.path)
        .await
        .map_err(map_file_backend_err)?;
    let safe_name = sanitize_download_filename(&name);
    let ascii_name: String = safe_name
        .chars()
        .filter(|c| c.is_ascii() && !c.is_control() && !matches!(c, '"' | '\\' | '/' | ';'))
        .collect();
    let disp_name = if ascii_name.trim().is_empty() {
        "download.bin".to_string()
    } else {
        ascii_name
    };
    let disposition = HeaderValue::from_str(&format!("attachment; filename=\"{disp_name}\""))
        .unwrap_or_else(|_| HeaderValue::from_static("attachment; filename=\"download.bin\""));

    let mut resp = (StatusCode::OK, bytes).into_response();
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    resp.headers_mut()
        .insert(header::CONTENT_DISPOSITION, disposition);
    Ok(resp)
}

pub async fn preview_workspace_file(
    State(state): State<Arc<HttpState>>,
    Query(q): Query<RequiredWorkspacePathQuery>,
) -> HttpResult<Response> {
    validate_workspace_path_param(&q.path)?;
    let (name, bytes) = state
        .backend
        .read_workspace_file(&q.path)
        .await
        .map_err(map_file_backend_err)?;
    let ext = file_extension_lower(&name);
    match ext.as_str() {
        "html" | "htm" => {
            let html = String::from_utf8_lossy(&bytes);
            let with_shim = inject_sandbox_storage_shim(&html);
            let mut resp = (StatusCode::OK, with_shim).into_response();
            resp.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("text/html; charset=utf-8"),
            );
            Ok(resp)
        }
        "png" | "jpg" | "jpeg" | "webp" | "gif" => {
            let ct = match ext.as_str() {
                "png" => "image/png",
                "jpg" | "jpeg" => "image/jpeg",
                "webp" => "image/webp",
                "gif" => "image/gif",
                _ => unreachable!(),
            };
            let mut resp = (StatusCode::OK, bytes).into_response();
            resp.headers_mut()
                .insert(header::CONTENT_TYPE, HeaderValue::from_static(ct));
            Ok(resp)
        }
        _ => Err(HttpError::BadRequest(
            "Preview only supports .html, .htm, .png, .jpg, .jpeg, .webp, and .gif files"
                .to_string(),
        )),
    }
}

pub async fn upload_workspace_file(
    State(state): State<Arc<HttpState>>,
    Extension(auth): Extension<AuthContext>,
    Json(req): Json<UploadWorkspaceFileRequest>,
) -> HttpResult<(StatusCode, Json<WorkspaceFileEntry>)> {
    validate_workspace_path_param(&req.path)?;
    let bytes = match (req.content, req.content_base64) {
        (Some(_), Some(_)) => {
            return Err(HttpError::BadRequest(
                "Provide either 'content' or 'content_base64', not both".to_string(),
            ));
        }
        (None, Some(ref b64)) => decode_base64(b64)?,
        (Some(ref text), None) => text.as_bytes().to_vec(),
        (None, None) => {
            return Err(HttpError::BadRequest(
                "Either 'content' or 'content_base64' must be provided".to_string(),
            ));
        }
    };

    let entry = state
        .backend
        .write_workspace_file(&req.path, &bytes)
        .await
        .map_err(map_file_backend_err)?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "files.upload".to_string(),
        target: entry.path.clone(),
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: serde_json::json!({ "size_bytes": entry.size_bytes }).to_string(),
    });
    Ok((StatusCode::CREATED, Json(entry)))
}

pub async fn create_workspace_folder(
    State(state): State<Arc<HttpState>>,
    Extension(auth): Extension<AuthContext>,
    Json(req): Json<MkdirRequest>,
) -> HttpResult<(StatusCode, Json<serde_json::Value>)> {
    validate_workspace_path_param(&req.path)?;
    state.backend.create_workspace_folder(&req.path).await?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "files.mkdir".to_string(),
        target: req.path.clone(),
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: "{}".to_string(),
    });
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({ "created": true, "path": req.path })),
    ))
}

pub async fn delete_workspace_file(
    State(state): State<Arc<HttpState>>,
    Extension(auth): Extension<AuthContext>,
    Query(q): Query<RequiredWorkspacePathQuery>,
) -> HttpResult<Json<serde_json::Value>> {
    validate_workspace_path_param(&q.path)?;
    state.backend.delete_workspace_file(&q.path).await?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "files.delete".to_string(),
        target: q.path.clone(),
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: "{}".to_string(),
    });
    Ok(Json(serde_json::json!({ "deleted": true, "path": q.path })))
}

// ---------- Extensions API ----------

pub async fn list_extensions(
    State(state): State<Arc<HttpState>>,
) -> HttpResult<Json<ExtensionsOverviewResponse>> {
    Ok(Json(state.backend.list_extensions().await?))
}

pub async fn get_skill_detail(
    State(state): State<Arc<HttpState>>,
    Path(id): Path<String>,
) -> HttpResult<Json<SkillDetailResponse>> {
    validate_id(&id, "skill")?;
    Ok(Json(state.backend.get_skill_detail(&id).await?))
}

pub async fn import_skill(
    State(state): State<Arc<HttpState>>,
    Extension(auth): Extension<AuthContext>,
    Json(req): Json<ImportSkillRequest>,
) -> HttpResult<(StatusCode, Json<SkillInfoDto>)> {
    if let Some(ref slug) = req.slug {
        validate_id(slug, "skill")?;
    }
    if req.markdown.trim().is_empty() {
        return Err(HttpError::BadRequest(
            "Skill markdown cannot be empty".to_string(),
        ));
    }
    let skill = state.backend.import_skill(req).await?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "extensions.skill.import".to_string(),
        target: skill.id.clone(),
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: serde_json::json!({ "name": skill.name }).to_string(),
    });
    Ok((StatusCode::CREATED, Json(skill)))
}

pub async fn import_plugin(
    State(state): State<Arc<HttpState>>,
    Extension(auth): Extension<AuthContext>,
    Json(req): Json<ImportPluginRequest>,
) -> HttpResult<(StatusCode, Json<PluginInfoDto>)> {
    if req.manifest_json.trim().is_empty() {
        return Err(HttpError::BadRequest(
            "Plugin manifest JSON cannot be empty".to_string(),
        ));
    }
    let plugin = state.backend.import_plugin(req).await?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "extensions.plugin.import".to_string(),
        target: plugin.id.clone(),
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: serde_json::json!({ "name": plugin.name, "version": plugin.version })
            .to_string(),
    });
    Ok((StatusCode::CREATED, Json(plugin)))
}

pub async fn import_mcp_servers(
    State(state): State<Arc<HttpState>>,
    Extension(auth): Extension<AuthContext>,
    Json(req): Json<ImportMcpRequest>,
) -> HttpResult<(StatusCode, Json<Vec<McpServerDto>>)> {
    if req.raw_json.trim().is_empty() {
        return Err(HttpError::BadRequest(
            "MCP server configuration JSON cannot be empty".to_string(),
        ));
    }
    let servers = state.backend.import_mcp_servers(req).await?;
    let server_ids: Vec<&str> = servers.iter().map(|s| s.id.as_str()).collect();
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "extensions.mcp.import".to_string(),
        target: "mcp".to_string(),
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: serde_json::json!({ "server_ids": server_ids }).to_string(),
    });
    Ok((StatusCode::CREATED, Json(servers)))
}

pub async fn sync_mcp_server(
    State(state): State<Arc<HttpState>>,
    Extension(auth): Extension<AuthContext>,
    Path(id): Path<String>,
) -> HttpResult<Json<McpServerDto>> {
    validate_id(&id, "mcp server")?;
    let server = state.backend.sync_mcp_server(&id).await?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "extensions.mcp.sync".to_string(),
        target: id,
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: serde_json::json!({
            "status": server.status,
            "tools_count": server.tools.len()
        })
        .to_string(),
    });
    Ok(Json(server))
}

pub async fn toggle_extension(
    State(state): State<Arc<HttpState>>,
    Extension(auth): Extension<AuthContext>,
    Path((kind, id)): Path<(String, String)>,
    Json(req): Json<ToggleExtensionRequest>,
) -> HttpResult<Json<serde_json::Value>> {
    let kind = validate_extension_kind(&kind)?;
    validate_id(&id, "extension")?;
    state
        .backend
        .set_extension_enabled(kind, &id, req.enabled)
        .await?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "extensions.toggle".to_string(),
        target: format!("{kind}:{id}"),
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: serde_json::json!({ "enabled": req.enabled }).to_string(),
    });
    Ok(Json(serde_json::json!({
        "kind": kind,
        "id": id,
        "enabled": req.enabled
    })))
}

pub async fn delete_extension(
    State(state): State<Arc<HttpState>>,
    Extension(auth): Extension<AuthContext>,
    Path((kind, id)): Path<(String, String)>,
) -> HttpResult<Json<serde_json::Value>> {
    let kind = validate_extension_kind(&kind)?;
    validate_id(&id, "extension")?;
    state.backend.delete_extension(kind, &id).await?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "extensions.delete".to_string(),
        target: format!("{kind}:{id}"),
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: "{}".to_string(),
    });
    Ok(Json(serde_json::json!({
        "deleted": true,
        "kind": kind,
        "id": id
    })))
}

// ---------- PS5LM Capabilities & Sync ----------

pub async fn get_inference_capabilities(
    State(state): State<Arc<HttpState>>,
) -> HttpResult<Json<Ps5LmCapabilities>> {
    Ok(Json(state.backend.get_inference_capabilities().await?))
}

pub async fn sync_inference_capabilities(
    State(state): State<Arc<HttpState>>,
    Extension(auth): Extension<AuthContext>,
) -> HttpResult<Json<Ps5LmCapabilities>> {
    let caps = state.backend.sync_inference_capabilities().await?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "inference.sync".to_string(),
        target: caps.base_url.clone(),
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: serde_json::json!({
            "healthy": caps.healthy,
            "loaded_model": caps.loaded_model,
            "context_length": caps.context_length,
            "total_slots": caps.total_slots,
        })
        .to_string(),
    });
    Ok(Json(caps))
}

fn strip_sensitive_kv_fields(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            map.retain(|k, _| {
                let lower = k.to_ascii_lowercase();
                lower != "api_key"
                    && lower != "apikey"
                    && lower != "ps5lm_api_key"
                    && lower != "authorization"
                    && !lower.ends_with("_api_key")
                    && !lower.ends_with("_secret")
                    && !lower.ends_with("_password")
            });
            for v in map.values_mut() {
                strip_sensitive_kv_fields(v);
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr.iter_mut() {
                strip_sensitive_kv_fields(v);
            }
        }
        _ => {}
    }
}

pub async fn import_inference_kv(
    State(state): State<Arc<HttpState>>,
    Extension(auth): Extension<AuthContext>,
) -> HttpResult<Json<serde_json::Value>> {
    let mut payload = state.backend.import_ps5lm_kv_settings(&auth.username).await?;
    strip_sensitive_kv_fields(&mut payload);

    let endpoint = payload
        .get("endpoint")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let imported_keys: Vec<&str> = payload
        .get("imported")
        .and_then(|v| v.as_object())
        .map(|m| m.keys().map(|k| k.as_str()).collect())
        .unwrap_or_default();
    let applied_keys: Vec<&str> = payload
        .get("applied")
        .and_then(|v| v.as_object())
        .map(|m| m.keys().map(|k| k.as_str()).collect())
        .unwrap_or_default();

    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "inference.import_kv".to_string(),
        target: endpoint.clone().unwrap_or_else(|| "ps5lm".to_string()),
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: serde_json::json!({
            "endpoint": endpoint,
            "imported_keys": imported_keys,
            "applied_keys": applied_keys,
        })
        .to_string(),
    });
    Ok(Json(payload))
}

#[derive(Debug, Deserialize)]
pub struct BrowserPreviewQuery {
    #[serde(default)]
    pub session_id: Option<String>,
}

fn sanitize_header_token(raw: &str, max_len: usize) -> String {
    raw.chars()
        .filter(|c| !c.is_control() && *c != '\r' && *c != '\n')
        .take(max_len)
        .collect()
}

pub async fn preview_browser_session(
    State(state): State<Arc<HttpState>>,
    Query(query): Query<BrowserPreviewQuery>,
) -> HttpResult<Response> {
    let requested_session_id = match query.session_id.as_deref().map(str::trim) {
        Some("") => None,
        Some(id) => {
            if id.len() > 128
                || !id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':'))
            {
                return Err(HttpError::from_agent(AgentError::InvalidInput(
                    "Invalid session_id".into(),
                )));
            }
            Some(id)
        }
        None => None,
    };

    let (session_id, sanitized_url, sanitized_html) = state
        .backend
        .preview_browser_session(requested_session_id)
        .await?;

    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
    );

    let safe_sid = sanitize_header_token(&session_id, 128);
    if !safe_sid.is_empty() {
        if let Ok(hv) = HeaderValue::from_str(&safe_sid) {
            headers.insert(HeaderName::from_static("x-browser-session-id"), hv);
        }
    }

    let safe_url = sanitize_header_token(&sanitized_url, 1024);
    if !safe_url.is_empty() {
        if let Ok(hv) = HeaderValue::from_str(&safe_url) {
            headers.insert(HeaderName::from_static("x-browser-url"), hv);
        }
    }

    Ok((StatusCode::OK, headers, sanitized_html).into_response())
}

pub async fn reorder_conversations(
    State(state): State<Arc<HttpState>>,
    Extension(auth): Extension<AuthContext>,
    Json(req): Json<ReorderConversationsRequest>,
) -> HttpResult<Json<Vec<Conversation>>> {
    for oid in &req.ordered_ids {
        validate_id(oid, "conversation")?;
    }
    if let Some(ref cid) = req.conversation_id {
        validate_id(cid, "conversation")?;
    }
    let updated = state
        .backend
        .reorder_conversations(req.ordered_ids, req.conversation_id, req.direction)
        .await?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "conversation.reorder".to_string(),
        target: "conversations".to_string(),
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: serde_json::json!({ "count": updated.len() }).to_string(),
    });
    Ok(Json(updated))
}

pub async fn fork_conversation(
    State(state): State<Arc<HttpState>>,
    Path(id): Path<String>,
    Extension(auth): Extension<AuthContext>,
    body: axum::body::Bytes,
) -> HttpResult<(StatusCode, Json<Conversation>)> {
    validate_id(&id, "conversation")?;
    let req: ForkConversationRequest = parse_optional_json(&body)?;
    if let Some(ref mid) = req.up_to_message_id {
        validate_id(mid, "message")?;
    }
    let forked = state
        .backend
        .fork_conversation(&id, req.up_to_message_id, req.title)
        .await?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "conversation.fork".to_string(),
        target: forked.id.clone(),
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: serde_json::json!({ "source_conversation_id": id }).to_string(),
    });
    Ok((StatusCode::CREATED, Json(forked)))
}

pub async fn delete_chat_message(
    State(state): State<Arc<HttpState>>,
    Path((id, msg_id)): Path<(String, String)>,
    Extension(auth): Extension<AuthContext>,
) -> HttpResult<Json<serde_json::Value>> {
    validate_id(&id, "conversation")?;
    validate_id(&msg_id, "message")?;
    let deleted = state.backend.delete_chat_message(&id, &msg_id).await?;
    if !deleted {
        return Err(HttpError::NotFound(format!(
            "Message {msg_id} not found in conversation {id}"
        )));
    }
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "message.delete".to_string(),
        target: format!("{id}/{msg_id}"),
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: "{}".to_string(),
    });
    Ok(Json(serde_json::json!({
        "deleted": true,
        "conversation_id": id,
        "message_id": msg_id
    })))
}

pub async fn retry_chat_message(
    State(state): State<Arc<HttpState>>,
    Path((id, msg_id)): Path<(String, String)>,
    Extension(auth): Extension<AuthContext>,
    body: axum::body::Bytes,
) -> HttpResult<Json<SendMessageResponse>> {
    validate_id(&id, "conversation")?;
    validate_id(&msg_id, "message")?;
    let req: MessageActionRequest = parse_optional_json(&body)?;
    if let Some(ref agent_id) = req.agent_id {
        validate_id(agent_id, "agent")?;
    }
    let resp = state
        .backend
        .retry_chat_message(
            &id,
            &msg_id,
            req.thinking_level,
            req.agent_id,
            req.async_dispatch,
        )
        .await?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "message.retry".to_string(),
        target: format!("{id}/{msg_id}"),
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: serde_json::json!({ "task_id": resp.task.as_ref().map(|t| t.id.as_str()) }).to_string(),
    });
    Ok(Json(resp))
}

pub async fn regenerate_chat_message(
    State(state): State<Arc<HttpState>>,
    Path((id, msg_id)): Path<(String, String)>,
    Extension(auth): Extension<AuthContext>,
    body: axum::body::Bytes,
) -> HttpResult<Json<SendMessageResponse>> {
    validate_id(&id, "conversation")?;
    validate_id(&msg_id, "message")?;
    let mut req: MessageActionRequest = parse_optional_json(&body)?;
    normalize_optional_owned_id(&mut req.agent_id, "agent")?;
    let resp = state
        .backend
        .regenerate_chat_message(&id, &msg_id, req.thinking_level, req.async_dispatch)
        .await?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "message.regenerate".to_string(),
        target: format!("{id}/{msg_id}"),
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: serde_json::json!({ "task_id": resp.task.as_ref().map(|t| t.id.as_str()) }).to_string(),
    });
    Ok(Json(resp))
}

pub async fn continue_chat_message(
    State(state): State<Arc<HttpState>>,
    Path((id, msg_id)): Path<(String, String)>,
    Extension(auth): Extension<AuthContext>,
    body: axum::body::Bytes,
) -> HttpResult<Json<SendMessageResponse>> {
    validate_id(&id, "conversation")?;
    validate_id(&msg_id, "message")?;
    let req: MessageActionRequest = parse_optional_json(&body)?;
    let resp = state
        .backend
        .continue_chat_message(&id, &msg_id, req.thinking_level)
        .await?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "message.continue".to_string(),
        target: format!("{id}/{msg_id}"),
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: serde_json::json!({ "task_id": resp.task.as_ref().map(|t| t.id.as_str()) }).to_string(),
    });
    Ok(Json(resp))
}

pub async fn enable_all_agents(
    State(state): State<Arc<HttpState>>,
    Extension(auth): Extension<AuthContext>,
) -> HttpResult<Json<serde_json::Value>> {
    let enabled_count = state.backend.enable_all_agents().await?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "agent.enable_all".to_string(),
        target: "agents".to_string(),
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: serde_json::json!({ "enabled_count": enabled_count }).to_string(),
    });
    Ok(Json(serde_json::json!({ "enabled_count": enabled_count })))
}

pub async fn create_or_import_tools(
    State(state): State<Arc<HttpState>>,
    Extension(auth): Extension<AuthContext>,
    Json(payload): Json<serde_json::Value>,
) -> HttpResult<(StatusCode, Json<Vec<ToolSpec>>)> {
    let tools = state.backend.create_or_import_tools(payload).await?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "tool.import".to_string(),
        target: "tools".to_string(),
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: serde_json::json!({ "count": tools.len() }).to_string(),
    });
    Ok((StatusCode::CREATED, Json(tools)))
}

pub async fn update_tool_policy(
    State(state): State<Arc<HttpState>>,
    Path(id): Path<String>,
    Extension(auth): Extension<AuthContext>,
    Json(req): Json<UpdateToolPolicyRequest>,
) -> HttpResult<Json<ToolSpec>> {
    validate_id(&id, "tool")?;
    let updated = state
        .backend
        .update_tool_policy(&id, req.enabled, req.requires_approval, req.timeout_secs)
        .await?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "tool.update_policy".to_string(),
        target: id,
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: serde_json::json!({
            "enabled": updated.enabled,
            "requires_approval": updated.requires_approval,
            "timeout_secs": updated.timeout_secs,
        })
        .to_string(),
    });
    Ok(Json(updated))
}

pub async fn delete_custom_tool(
    State(state): State<Arc<HttpState>>,
    Path(id): Path<String>,
    Extension(auth): Extension<AuthContext>,
) -> HttpResult<Json<serde_json::Value>> {
    validate_id(&id, "tool")?;
    let deleted = state.backend.delete_custom_tool(&id).await?;
    if !deleted {
        return Err(HttpError::NotFound(format!("Custom tool {id} not found")));
    }
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "tool.delete".to_string(),
        target: id.clone(),
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: "{}".to_string(),
    });
    Ok(Json(serde_json::json!({ "deleted": true, "id": id })))
}

pub async fn import_memories(
    State(state): State<Arc<HttpState>>,
    Extension(auth): Extension<AuthContext>,
    Json(ImportMemoriesRequest(payload)): Json<ImportMemoriesRequest>,
) -> HttpResult<Json<serde_json::Value>> {
    let imported = state.backend.import_memories(payload).await?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "memory.import".to_string(),
        target: "memories".to_string(),
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: serde_json::json!({ "imported": imported }).to_string(),
    });
    Ok(Json(serde_json::json!({ "imported": imported })))
}

pub async fn export_memories(
    State(state): State<Arc<HttpState>>,
) -> HttpResult<Json<Vec<MemoryItem>>> {
    let items = state.backend.export_memories().await?;
    Ok(Json(items))
}

pub async fn cancel_conversation_tasks(
    State(state): State<Arc<HttpState>>,
    Path(id): Path<String>,
    Extension(auth): Extension<AuthContext>,
    body: axum::body::Bytes,
) -> HttpResult<Json<serde_json::Value>> {
    validate_id(&id, "conversation")?;
    let req: CancelConversationTasksRequest = parse_optional_json(&body)?;
    if let Some(ref agent_id) = req.agent_id {
        validate_id(agent_id, "agent")?;
    }
    let agent_filter = req.agent_id.clone();
    let count = state
        .backend
        .cancel_conversation_tasks(&id, req.agent_id)
        .await?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "conversation.cancel_tasks".to_string(),
        target: id,
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: serde_json::json!({
            "cancelled_count": count,
            "agent_id": agent_filter,
        })
        .to_string(),
    });
    Ok(Json(serde_json::json!({ "cancelled_count": count })))
}

pub async fn expedite_task(
    State(state): State<Arc<HttpState>>,
    Path(id): Path<String>,
    Extension(auth): Extension<AuthContext>,
) -> HttpResult<Json<serde_json::Value>> {
    validate_id(&id, "task")?;
    let task = state.backend.expedite_task(&id).await?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "task.expedite".to_string(),
        target: id,
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: serde_json::json!({ "status": task.status }).to_string(),
    });
    Ok(Json(serde_json::json!({ "task": task })))
}

// ---------- Schedules ----------

pub async fn list_schedules(
    State(state): State<Arc<HttpState>>,
) -> HttpResult<Json<Vec<ScheduleRecord>>> {
    let items = state.backend.list_schedules().await?;
    Ok(Json(items))
}

pub async fn get_schedule(
    State(state): State<Arc<HttpState>>,
    Path(id): Path<String>,
) -> HttpResult<Json<ScheduleRecord>> {
    validate_id(&id, "schedule")?;
    let sched = state
        .backend
        .get_schedule(&id)
        .await?
        .ok_or_else(|| HttpError::NotFound(format!("Schedule {id} not found")))?;
    Ok(Json(sched))
}

pub async fn create_schedule(
    State(state): State<Arc<HttpState>>,
    Extension(auth): Extension<AuthContext>,
    Json(req): Json<HttpCreateSchedule>,
) -> HttpResult<(StatusCode, Json<ScheduleRecord>)> {
    if req.title.trim().is_empty() || req.title.len() > 200 {
        return Err(HttpError::BadRequest(
            "Schedule title must be 1..=200 characters".to_string(),
        ));
    }
    if req.prompt.trim().is_empty() || req.prompt.len() > 64_000 {
        return Err(HttpError::BadRequest(
            "Schedule prompt must be 1..=64000 characters".to_string(),
        ));
    }
    if let Some(ref aid) = req.target_agent_id {
        if !aid.trim().is_empty() {
            validate_id(aid, "agent")?;
        }
    }
    if let Some(ref cid) = req.conversation_id {
        if !cid.trim().is_empty() {
            validate_id(cid, "conversation")?;
        }
    }
    let created = state.backend.create_schedule(req).await?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "schedule.create".to_string(),
        target: created.id.clone(),
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: serde_json::json!({
            "title": created.title,
            "kind": created.kind,
            "enabled": created.enabled,
        })
        .to_string(),
    });
    Ok((StatusCode::CREATED, Json(created)))
}

pub async fn update_schedule(
    State(state): State<Arc<HttpState>>,
    Path(id): Path<String>,
    Extension(auth): Extension<AuthContext>,
    Json(req): Json<HttpUpdateSchedule>,
) -> HttpResult<Json<ScheduleRecord>> {
    validate_id(&id, "schedule")?;
    if let Some(ref title) = req.title {
        if title.trim().is_empty() || title.len() > 200 {
            return Err(HttpError::BadRequest(
                "Schedule title must be 1..=200 characters".to_string(),
            ));
        }
    }
    if let Some(ref prompt) = req.prompt {
        if prompt.trim().is_empty() || prompt.len() > 64_000 {
            return Err(HttpError::BadRequest(
                "Schedule prompt must be 1..=64000 characters".to_string(),
            ));
        }
    }
    if let Some(ref aid) = req.target_agent_id {
        if !aid.trim().is_empty() {
            validate_id(aid, "agent")?;
        }
    }
    let updated = state.backend.update_schedule(&id, req).await?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "schedule.update".to_string(),
        target: id,
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: serde_json::json!({
            "enabled": updated.enabled,
            "kind": updated.kind,
        })
        .to_string(),
    });
    Ok(Json(updated))
}

pub async fn delete_schedule(
    State(state): State<Arc<HttpState>>,
    Path(id): Path<String>,
    Extension(auth): Extension<AuthContext>,
) -> HttpResult<Json<serde_json::Value>> {
    validate_id(&id, "schedule")?;
    state.backend.delete_schedule(&id).await?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "schedule.delete".to_string(),
        target: id.clone(),
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: "{}".to_string(),
    });
    Ok(Json(serde_json::json!({ "deleted": true, "id": id })))
}

pub async fn toggle_schedule(
    State(state): State<Arc<HttpState>>,
    Path(id): Path<String>,
    Extension(auth): Extension<AuthContext>,
    body: axum::body::Bytes,
) -> HttpResult<Json<ScheduleRecord>> {
    validate_id(&id, "schedule")?;
    let req: HttpToggleScheduleRequest = parse_optional_json(&body)?;
    let updated = state.backend.toggle_schedule(&id, req.enabled).await?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "schedule.toggle".to_string(),
        target: id,
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: serde_json::json!({ "enabled": updated.enabled }).to_string(),
    });
    Ok(Json(updated))
}

pub async fn run_schedule_now(
    State(state): State<Arc<HttpState>>,
    Path(id): Path<String>,
    Extension(auth): Extension<AuthContext>,
) -> HttpResult<Json<ScheduleRecord>> {
    validate_id(&id, "schedule")?;
    let updated = state.backend.run_schedule_now(&id).await?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "schedule.run_now".to_string(),
        target: id,
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: serde_json::json!({
            "last_status": updated.last_status,
            "run_count": updated.run_count,
        })
        .to_string(),
    });
    Ok(Json(updated))
}

pub async fn parse_schedule_ai(
    State(state): State<Arc<HttpState>>,
    Extension(auth): Extension<AuthContext>,
    Json(req): Json<HttpScheduleAiRequest>,
) -> HttpResult<Json<HttpScheduleAiResponse>> {
    if req.prompt.trim().is_empty() || req.prompt.len() > 16_000 {
        return Err(HttpError::BadRequest(
            "Schedule prompt must be 1..=16000 characters".to_string(),
        ));
    }
    if let Some(ref aid) = req.target_agent_id {
        if !aid.trim().is_empty() {
            validate_id(aid, "agent")?;
        }
    }
    let auto_create = req.auto_create;
    let resp = state.backend.parse_schedule_ai(req).await?;
    let target = resp
        .created
        .as_ref()
        .map(|s| s.id.clone())
        .unwrap_or_else(|| "draft".to_string());
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "schedule.parse_ai".to_string(),
        target,
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: serde_json::json!({
            "auto_create": auto_create,
            "source": resp.draft.source,
            "kind": resp.draft.kind,
        })
        .to_string(),
    });
    Ok(Json(resp))
}

// ---------- Doctor Diagnostics ----------

pub async fn run_doctor(State(state): State<Arc<HttpState>>) -> HttpResult<Json<DoctorReport>> {
    Ok(Json(state.backend.run_doctor().await?))
}

pub async fn fix_doctor(
    State(state): State<Arc<HttpState>>,
    Extension(auth): Extension<AuthContext>,
) -> HttpResult<Json<DoctorFixReport>> {
    let report = state.backend.fix_doctor().await?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "doctor.fix".to_string(),
        target: "system".to_string(),
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: serde_json::json!({
            "applied_count": report.applied_fixes.len(),
        })
        .to_string(),
    });
    Ok(Json(report))
}

// ---------- Sandbox & Remote Browser ----------

pub async fn get_sandbox_state(
    State(state): State<Arc<HttpState>>,
) -> HttpResult<Json<SandboxStateDto>> {
    Ok(Json(state.backend.get_sandbox_state().await?))
}

pub async fn update_sandbox_policy(
    State(state): State<Arc<HttpState>>,
    Extension(auth): Extension<AuthContext>,
    Json(req): Json<UpdateSandboxRequest>,
) -> HttpResult<Json<SandboxStateDto>> {
    let mode_for_audit = req.mode.clone();
    let updated = state.backend.update_sandbox_policy(req).await?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "sandbox.update".to_string(),
        target: "sandbox".to_string(),
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: serde_json::json!({
            "mode": mode_for_audit.as_deref().unwrap_or(&updated.mode),
            "approval_policy": updated.approval_policy,
        })
        .to_string(),
    });
    Ok(Json(updated))
}

pub async fn list_browser_sessions(
    State(state): State<Arc<HttpState>>,
) -> HttpResult<Json<serde_json::Value>> {
    Ok(Json(state.backend.list_browser_sessions().await?))
}

pub async fn execute_browser_action(
    State(state): State<Arc<HttpState>>,
    Extension(auth): Extension<AuthContext>,
    Json(payload): Json<serde_json::Value>,
) -> HttpResult<Json<serde_json::Value>> {
    if !payload.is_object() {
        return Err(HttpError::BadRequest(
            "Browser action payload must be a JSON object".to_string(),
        ));
    }
    let action_name = payload
        .get("action")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();
    let res = state.backend.execute_browser_action(payload).await?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "sandbox.browser.action".to_string(),
        target: action_name,
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: "{}".to_string(),
    });
    Ok(Json(res))
}

pub async fn close_browser_session(
    State(state): State<Arc<HttpState>>,
    Extension(auth): Extension<AuthContext>,
    Path(id): Path<String>,
) -> HttpResult<Json<serde_json::Value>> {
    validate_id(&id, "browser session")?;
    let closed = state.backend.close_browser_session(&id).await?;
    if !closed {
        return Err(HttpError::NotFound(format!(
            "Browser session {id} not found"
        )));
    }
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "sandbox.browser.close".to_string(),
        target: id.clone(),
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: "{}".to_string(),
    });
    Ok(Json(serde_json::json!({
        "closed": true,
        "deleted": true,
        "id": id
    })))
}

// ---------- Brain Markdown Files ----------

#[derive(Debug, Deserialize)]
pub struct BrainQuery {
    pub name: Option<String>,
}

fn validate_brain_name(name: &str) -> HttpResult<()> {
    let trimmed = name.trim();
    if name != trimmed
        || trimmed.is_empty()
        || trimmed.len() > 256
        || trimmed.contains('\0')
        || trimmed.starts_with('/')
        || trimmed.starts_with('\\')
        || trimmed.contains('\\')
        || trimmed.chars().any(char::is_control)
        || trimmed
            .split('/')
            .any(|seg| seg.is_empty() || seg == "." || seg == "..")
    {
        return Err(HttpError::BadRequest("Invalid brain file name".to_string()));
    }
    Ok(())
}

pub async fn list_or_read_brain(
    State(state): State<Arc<HttpState>>,
    Query(q): Query<BrainQuery>,
) -> HttpResult<Json<serde_json::Value>> {
    if let Some(ref raw_name) = q.name {
        let name = raw_name.trim();
        if !name.is_empty() {
            validate_brain_name(raw_name)?;
            let detail = state.backend.read_brain_file(name).await?;
            let val = serde_json::to_value(detail)
                .map_err(|e| HttpError::Internal(format!("serializing brain file: {e}")))?;
            return Ok(Json(val));
        }
    }
    let files: Vec<BrainFileSummary> = state.backend.list_brain_files().await?;
    let val = serde_json::to_value(files)
        .map_err(|e| HttpError::Internal(format!("serializing brain files: {e}")))?;
    Ok(Json(val))
}

pub async fn write_brain(
    State(state): State<Arc<HttpState>>,
    Extension(auth): Extension<AuthContext>,
    Json(req): Json<WriteBrainFileRequest>,
) -> HttpResult<Json<BrainFileDetail>> {
    let name = req
        .name
        .as_deref()
        .ok_or_else(|| HttpError::BadRequest("Brain file 'name' is required".to_string()))?;
    validate_brain_name(name)?;
    let detail = state.backend.write_brain_file(name, &req.content).await?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "brain.write".to_string(),
        target: detail.name.clone(),
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: serde_json::json!({ "size_bytes": detail.size_bytes }).to_string(),
    });
    Ok(Json(detail))
}

pub async fn get_brain_file(
    State(state): State<Arc<HttpState>>,
    Path(name): Path<String>,
) -> HttpResult<Json<BrainFileDetail>> {
    validate_brain_name(&name)?;
    Ok(Json(state.backend.read_brain_file(&name).await?))
}

pub async fn put_brain_file(
    State(state): State<Arc<HttpState>>,
    Extension(auth): Extension<AuthContext>,
    Path(name): Path<String>,
    Json(req): Json<WriteBrainFileRequest>,
) -> HttpResult<Json<BrainFileDetail>> {
    validate_brain_name(&name)?;
    if let Some(ref body_name) = req.name {
        if body_name != &name {
            return Err(HttpError::BadRequest(
                "Brain file 'name' in body does not match URL path".to_string(),
            ));
        }
    }
    let detail = state.backend.write_brain_file(&name, &req.content).await?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "brain.write".to_string(),
        target: detail.name.clone(),
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: serde_json::json!({ "size_bytes": detail.size_bytes }).to_string(),
    });
    Ok(Json(detail))
}

// ---------- Conversation Slash Commands ----------

pub async fn execute_conversation_command(
    State(state): State<Arc<HttpState>>,
    Extension(auth): Extension<AuthContext>,
    Path(id): Path<String>,
    Json(mut req): Json<SlashCommandRequest>,
) -> HttpResult<Json<SlashCommandResponse>> {
    validate_id(&id, "conversation")?;
    if req.command.trim().is_empty() || req.command.len() > 8000 {
        return Err(HttpError::BadRequest(
            "Slash command must be 1..=8000 characters".to_string(),
        ));
    }
    normalize_optional_owned_id(&mut req.agent_id, "agent")?;
    let cmd_preview: String = req.command.trim().chars().take(64).collect();
    let resp = state
        .backend
        .execute_conversation_command(&id, req)
        .await?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "conversation.command".to_string(),
        target: id,
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: serde_json::json!({
            "command": cmd_preview,
            "ephemeral": resp.ephemeral,
        })
        .to_string(),
    });
    Ok(Json(resp))
}

// ---------- Webhooks ----------

pub async fn list_webhooks(
    State(state): State<Arc<HttpState>>,
) -> HttpResult<Json<Vec<WebhookView>>> {
    Ok(Json(state.backend.list_webhooks().await?))
}

pub async fn create_webhook(
    State(state): State<Arc<HttpState>>,
    Extension(auth): Extension<AuthContext>,
    Json(mut req): Json<CreateWebhookRequest>,
) -> HttpResult<(StatusCode, Json<CreateWebhookResponse>)> {
    let trimmed_name = req.name.trim();
    if trimmed_name.is_empty() || trimmed_name.len() > 120 {
        return Err(HttpError::BadRequest(
            "Webhook name must be 1..=120 characters".to_string(),
        ));
    }
    validate_id(&req.target_agent_id, "agent")?;
    normalize_optional_owned_id(&mut req.conversation_id, "conversation")?;
    if req.prompt_template.len() > 16_000 {
        return Err(HttpError::BadRequest(
            "Webhook prompt_template must be <= 16000 characters".to_string(),
        ));
    }
    let resp = state.backend.create_webhook(req).await?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "webhook.create".to_string(),
        target: resp.webhook.id.clone(),
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: serde_json::json!({
            "name": resp.webhook.name,
            "target_agent_id": resp.webhook.target_agent_id,
        })
        .to_string(),
    });
    Ok((StatusCode::CREATED, Json(resp)))
}

pub async fn delete_webhook(
    State(state): State<Arc<HttpState>>,
    Extension(auth): Extension<AuthContext>,
    Path(id): Path<String>,
) -> HttpResult<Json<serde_json::Value>> {
    validate_id(&id, "webhook")?;
    let deleted = state.backend.delete_webhook(&id).await?;
    if !deleted {
        return Err(HttpError::NotFound(format!("Webhook {id} not found")));
    }
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "webhook.delete".to_string(),
        target: id.clone(),
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: "{}".to_string(),
    });
    Ok(Json(serde_json::json!({ "deleted": true, "id": id })))
}

fn extract_webhook_token(headers: &HeaderMap) -> HttpResult<String> {
    let custom_count = headers.get_all("x-ps5-webhook-token").iter().count();
    let auth_count = headers.get_all(header::AUTHORIZATION).iter().count();
    if custom_count > 1 || auth_count > 1 {
        return Err(HttpError::Unauthorized("Duplicate webhook token headers"));
    }

    let custom_token = match headers.get("x-ps5-webhook-token") {
        Some(v) => {
            let raw = v
                .to_str()
                .map_err(|_| HttpError::Unauthorized("Invalid webhook token header"))?;
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        }
        None => None,
    };

    let bearer_token = match headers.get(header::AUTHORIZATION) {
        Some(v) => {
            let raw = v
                .to_str()
                .map_err(|_| HttpError::Unauthorized("Invalid Authorization header"))?;
            let trimmed = raw.trim();
            if let Some(rest) = trimmed
                .strip_prefix("Bearer ")
                .or_else(|| trimmed.strip_prefix("bearer "))
            {
                let tok = rest.trim();
                if tok.is_empty() {
                    None
                } else {
                    Some(tok.to_string())
                }
            } else {
                return Err(HttpError::Unauthorized(
                    "Authorization header must use Bearer scheme",
                ));
            }
        }
        None => None,
    };

    match (custom_token, bearer_token) {
        (Some(a), Some(b)) => {
            if a != b {
                Err(HttpError::Unauthorized("Conflicting webhook tokens"))
            } else {
                Ok(a)
            }
        }
        (Some(a), None) => Ok(a),
        (None, Some(b)) => Ok(b),
        (None, None) => Err(HttpError::Unauthorized("Missing webhook authentication token")),
    }
}

pub async fn trigger_inbound_webhook(
    State(state): State<Arc<HttpState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> HttpResult<Json<WebhookTriggerResponse>> {
    validate_id(&id, "webhook")?;
    let token = extract_webhook_token(&headers)?;
    let payload = if body.iter().all(|b| b.is_ascii_whitespace()) {
        serde_json::json!({})
    } else {
        serde_json::from_slice::<serde_json::Value>(&body)
            .map_err(|e| HttpError::BadRequest(format!("Invalid JSON payload: {e}")))?
    };

    let resp = state
        .backend
        .trigger_inbound_webhook(&id, &token, payload)
        .await?;
    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: "webhook".to_string(),
        action: "webhook.inbound".to_string(),
        target: id,
        severity: "info".to_string(),
        outcome: if resp.ok { "success" } else { "failure" }.to_string(),
        details_json: serde_json::json!({ "task_id": resp.task_id }).to_string(),
    });
    Ok(Json(resp))
}
