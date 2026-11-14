use crate::parser::{
    normalize_conversation_messages, parse_model_output, truncate_verbatim_prose_loops,
    StreamPiece, StreamingLoopDecision, StreamingRepetitionGuard, ThinkStreamSplitter,
};
use async_trait::async_trait;
use futures_util::StreamExt;
use ps5_agent_config::InferenceConfig;
use ps5_agent_core::{
    now_iso, AgentError, ApiMode, InferenceMessage, InferenceProvider, InferenceRequest,
    InferenceResponse, InferenceStreamChunk, InferenceTimings, ProviderHealth, Ps5LmCapabilities,
    Ps5LmKvSamplingImport, Result, SystemOneRequest, SystemOneResponse, ThinkingLevel, TokenUsage,
    ToolCallRequest,
};
use ps5_agent_logging::{log_debug, log_error, log_info, log_warn};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex, Once, RwLock},
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

pub const MAX_SSE_LINE_BYTES: usize = 8 * 1024 * 1024;
pub const UNLIMITED_CONCURRENCY_PERMITS: usize = 4096;
const ANTHROPIC_DEFAULT_MAX_TOKENS: u32 = 8192;

static CRYPTO_INIT: Once = Once::new();

fn effective_concurrency_permits(max_concurrent_inference: usize) -> usize {
    if max_concurrent_inference == 0 {
        UNLIMITED_CONCURRENCY_PERMITS
    } else {
        max_concurrent_inference
    }
}

fn ensure_rustls_ring_provider() {
    CRYPTO_INIT.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InferenceMetrics {
    pub requests_total: u64,
    pub errors_total: u64,
    pub p50_ms: u64,
    pub p95_ms: u64,
    pub last_ms: u64,
    pub prompt_tokens_total: u64,
    pub completion_tokens_total: u64,
}

#[derive(Default)]
struct MetricsTracker {
    requests_total: u64,
    errors_total: u64,
    last_ms: u64,
    prompt_tokens_total: u64,
    completion_tokens_total: u64,
    latencies_ms: VecDeque<u64>,
}

impl MetricsTracker {
    const WINDOW: usize = 256;

    fn record_success(&mut self, latency_ms: u64, usage: &TokenUsage) {
        self.requests_total += 1;
        self.last_ms = latency_ms;
        self.prompt_tokens_total += u64::from(usage.prompt_tokens);
        self.completion_tokens_total += u64::from(usage.completion_tokens);
        if self.latencies_ms.len() >= Self::WINDOW {
            self.latencies_ms.pop_front();
        }
        self.latencies_ms.push_back(latency_ms);
    }

    fn record_error(&mut self, latency_ms: u64) {
        self.requests_total += 1;
        self.errors_total += 1;
        self.last_ms = latency_ms;
        if self.latencies_ms.len() >= Self::WINDOW {
            self.latencies_ms.pop_front();
        }
        self.latencies_ms.push_back(latency_ms);
    }

    fn snapshot(&self) -> InferenceMetrics {
        let (p50_ms, p95_ms) = if self.latencies_ms.is_empty() {
            (0, 0)
        } else {
            let mut sorted: Vec<u64> = self.latencies_ms.iter().copied().collect();
            sorted.sort_unstable();
            let p50_idx = ((sorted.len() - 1) * 50) / 100;
            let p95_idx = ((sorted.len() - 1) * 95) / 100;
            (sorted[p50_idx], sorted[p95_idx])
        };
        InferenceMetrics {
            requests_total: self.requests_total,
            errors_total: self.errors_total,
            p50_ms,
            p95_ms,
            last_ms: self.last_ms,
            prompt_tokens_total: self.prompt_tokens_total,
            completion_tokens_total: self.completion_tokens_total,
        }
    }
}

struct ProviderInner {
    config: InferenceConfig,
    client: reqwest::Client,
    root_url: String,
    v1_url: String,
}

#[derive(Debug)]
struct GateState {
    in_flight: usize,
    limit: usize,
    rev: u64,
}

#[derive(Debug)]
struct ConcurrencyGate {
    state: Mutex<GateState>,
    changed_tx: watch::Sender<u64>,
}

#[derive(Debug)]
struct ConcurrencyPermit {
    gate: Arc<ConcurrencyGate>,
}

impl Drop for ConcurrencyPermit {
    fn drop(&mut self) {
        let rev = {
            let mut g = self.gate.state.lock().unwrap_or_else(|e| e.into_inner());
            g.in_flight = g.in_flight.saturating_sub(1);
            g.rev = g.rev.wrapping_add(1);
            g.rev
        };
        let _ = self.gate.changed_tx.send(rev);
    }
}

impl ConcurrencyGate {
    fn new(limit: usize) -> Arc<Self> {
        let (changed_tx, _rx) = watch::channel(0u64);
        Arc::new(Self {
            state: Mutex::new(GateState {
                in_flight: 0,
                limit: limit.max(1),
                rev: 0,
            }),
            changed_tx,
        })
    }

    fn limit(&self) -> usize {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).limit
    }

    fn set_limit(&self, limit: usize) {
        let limit = limit.max(1);
        let rev = {
            let mut g = self.state.lock().unwrap_or_else(|e| e.into_inner());
            g.limit = limit;
            g.rev = g.rev.wrapping_add(1);
            g.rev
        };
        let _ = self.changed_tx.send(rev);
    }

    fn try_acquire(self: &Arc<Self>) -> Option<ConcurrencyPermit> {
        let mut g = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if g.in_flight < g.limit {
            g.in_flight += 1;
            Some(ConcurrencyPermit {
                gate: Arc::clone(self),
            })
        } else {
            None
        }
    }

    async fn acquire(self: &Arc<Self>, cancel: &CancellationToken) -> Result<ConcurrencyPermit> {
        let mut rx = self.changed_tx.subscribe();
        loop {
            if cancel.is_cancelled() {
                return Err(AgentError::Cancelled(
                    "inference cancelled while waiting for concurrency slot".into(),
                ));
            }
            // Snapshot watch version BEFORE reading state so no wakeups can be lost.
            let _ = rx.borrow_and_update();
            {
                let mut g = self.state.lock().unwrap_or_else(|e| e.into_inner());
                if g.in_flight < g.limit {
                    g.in_flight += 1;
                    return Ok(ConcurrencyPermit {
                        gate: Arc::clone(self),
                    });
                }
            }
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    return Err(AgentError::Cancelled(
                        "inference cancelled while waiting for concurrency slot".into(),
                    ));
                }
                res = rx.changed() => {
                    if res.is_err() {
                        return Err(AgentError::Internal(
                            "inference concurrency gate closed".into(),
                        ));
                    }
                }
            }
        }
    }
}

pub struct Ps5LmProvider {
    inner: RwLock<ProviderInner>,
    gate: Arc<ConcurrencyGate>,
    metrics: Mutex<MetricsTracker>,
}

impl Ps5LmProvider {
    pub fn new(config: InferenceConfig) -> Result<Self> {
        Self::with_concurrency(config, 1)
    }

    pub fn with_concurrency(config: InferenceConfig, max_concurrent_inference: usize) -> Result<Self> {
        ensure_rustls_ring_provider();
        let max_c = effective_concurrency_permits(max_concurrent_inference);
        let client = build_http_client(&config)?;
        let (root_url, v1_url) = normalize_base_urls(&config.base_url);

        Ok(Self {
            inner: RwLock::new(ProviderInner {
                config,
                client,
                root_url,
                v1_url,
            }),
            gate: ConcurrencyGate::new(max_c),
            metrics: Mutex::new(MetricsTracker::default()),
        })
    }

    pub fn update_config(&self, config: InferenceConfig) -> Result<()> {
        let client = build_http_client(&config)?;
        let (root_url, v1_url) = normalize_base_urls(&config.base_url);
        let mut guard = self
            .inner
            .write()
            .map_err(|_| AgentError::Internal("inference provider lock poisoned".into()))?;
        guard.config = config;
        guard.client = client;
        guard.root_url = root_url;
        guard.v1_url = v1_url;
        log_info("inference", "updated Ps5LmProvider configuration");
        Ok(())
    }

    pub fn set_default_model(&self, model: impl Into<String>) -> Result<()> {
        let model = model.into();
        let trimmed = model.trim();
        if trimmed.is_empty() {
            return Ok(());
        }
        let mut guard = self
            .inner
            .write()
            .map_err(|_| AgentError::Internal("inference provider lock poisoned".into()))?;
        guard.config.model = trimmed.to_string();
        log_info("inference", format!("updated default model={trimmed}"));
        Ok(())
    }

    pub fn apply_discovered_context(&self, caps: &Ps5LmCapabilities) -> Result<()> {
        let mut guard = self
            .inner
            .write()
            .map_err(|_| AgentError::Internal("inference provider lock poisoned".into()))?;
        if let Some(ref loaded) = caps.loaded_model {
            let trimmed = loaded.trim();
            if !trimmed.is_empty() {
                guard.config.model = trimmed.to_string();
            }
        }
        if let Some(ctx) = caps.context_length {
            if ctx > 0 {
                guard.config.context_limit = ctx;
            }
        }
        Ok(())
    }

    pub fn set_max_concurrent(&self, max_concurrent_inference: usize) {
        let max_c = effective_concurrency_permits(max_concurrent_inference);
        self.gate.set_limit(max_c);
        log_info(
            "inference",
            format!("updated max_concurrent_inference={max_c}"),
        );
    }

    pub fn max_concurrent(&self) -> usize {
        self.gate.limit()
    }

    pub fn config(&self) -> InferenceConfig {
        self.inner
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .config
            .clone()
    }

    pub fn metrics(&self) -> InferenceMetrics {
        self.metrics
            .lock()
            .map(|g| g.snapshot())
            .unwrap_or_default()
    }

    fn snapshot_client(&self) -> Result<(InferenceConfig, reqwest::Client, String, String)> {
        let g = self
            .inner
            .read()
            .map_err(|_| AgentError::Internal("inference provider lock poisoned".into()))?;
        Ok((
            g.config.clone(),
            g.client.clone(),
            g.root_url.clone(),
            g.v1_url.clone(),
        ))
    }

    async fn acquire_permit(&self, cancel: &CancellationToken) -> Result<ConcurrencyPermit> {
        self.gate.acquire(cancel).await
    }

    async fn send_with_connect_retries(
        &self,
        client: &reqwest::Client,
        url: &str,
        headers: HeaderMap,
        body: Option<&Value>,
        max_retries: u32,
        request_timeout: Option<Duration>,
        cancel: &CancellationToken,
    ) -> Result<reqwest::Response> {
        let mut attempt: u32 = 0;
        loop {
            if cancel.is_cancelled() {
                return Err(AgentError::Cancelled("inference cancelled".into()));
            }

            let req_builder = match body {
                Some(b) => client.post(url).headers(headers.clone()).json(b),
                None => client.get(url).headers(headers.clone()),
            };

            let send_res = match request_timeout {
                Some(dur) => {
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => {
                            return Err(AgentError::Cancelled("inference cancelled during request".into()));
                        }
                        res = tokio::time::timeout(dur, req_builder.send()) => match res {
                            Ok(inner) => inner,
                            Err(_) => {
                                return Err(AgentError::Timeout(format!(
                                    "inference request timed out after {}s",
                                    dur.as_secs()
                                )));
                            }
                        }
                    }
                }
                None => {
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => {
                            return Err(AgentError::Cancelled("inference cancelled during request".into()));
                        }
                        res = req_builder.send() => res,
                    }
                }
            };

            match send_res {
                Ok(resp) => return Ok(resp),
                Err(err) => {
                    // Bounded exponential retry ONLY on pre-response transient connection failures.
                    if err.is_connect() {
                        if attempt < max_retries {
                            let backoff_ms =
                                (100u64.saturating_mul(1u64 << attempt.min(6))).min(2000);
                            log_warn(
                                "inference",
                                format!(
                                    "pre-response connect error (attempt {}/{}), retrying in {}ms: {err}",
                                    attempt + 1,
                                    max_retries,
                                    backoff_ms
                                ),
                            );
                            attempt += 1;
                            tokio::select! {
                                biased;
                                _ = cancel.cancelled() => {
                                    return Err(AgentError::Cancelled("inference cancelled during retry backoff".into()));
                                }
                                _ = tokio::time::sleep(Duration::from_millis(backoff_ms)) => {}
                            }
                            continue;
                        }
                        return Err(AgentError::InferenceUnavailable(format!(
                            "connection to inference server failed: {err}"
                        )));
                    }
                    if err.is_timeout() {
                        return Err(AgentError::Timeout(format!(
                            "inference request timed out: {err}"
                        )));
                    }
                    return Err(AgentError::InferenceUnavailable(format!(
                        "connection to inference server failed: {err}"
                    )));
                }
            }
        }
    }

    async fn execute_non_streaming(
        &self,
        req: &InferenceRequest,
        cancel: &CancellationToken,
    ) -> Result<InferenceResponse> {
        let started = Instant::now();
        let (cfg, client, _root_url, v1_url) = self.snapshot_client()?;
        let model = resolve_model(&req.model, &cfg.model);
        let url = endpoint_url(&v1_url, req.api_mode);
        let headers = build_headers(req.api_mode, cfg.api_key.as_deref());
        let payload = build_request_payload(req, &cfg, &model, false);

        log_debug(
            "inference",
            format!("POST {url} api_mode={:?} model={model} stream=false", req.api_mode),
        );

        let req_timeout = effective_request_timeout(cfg.timeout_secs);
        let resp = self
            .send_with_connect_retries(
                &client,
                &url,
                headers,
                Some(&payload),
                cfg.max_retries,
                req_timeout,
                cancel,
            )
            .await?;

        let status = resp.status();
        let text_body =
            read_text_with_timeout(resp, req_timeout, cancel, "reading response body").await?;

        if status == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            return Err(AgentError::InferenceUnavailable(format!(
                "PS5LM returned 503 unavailable: {}",
                truncate_for_err(&text_body, 256)
            )));
        }
        if !status.is_success() {
            return Err(AgentError::Inference(format!(
                "HTTP {} from inference endpoint: {}",
                status.as_u16(),
                truncate_for_err(&text_body, 256)
            )));
        }

        let val: Value = serde_json::from_str(&text_body)
            .map_err(|e| AgentError::Inference(format!("malformed JSON response: {e}")))?;

        let latency_ms = started.elapsed().as_millis() as u64;
        parse_non_streaming_response(req.api_mode, &model, &val, latency_ms)
    }

    async fn execute_streaming(
        &self,
        req: &InferenceRequest,
        tx: &mpsc::Sender<InferenceStreamChunk>,
        cancel: &CancellationToken,
    ) -> Result<InferenceResponse> {
        if req.api_mode == ApiMode::SystemOne {
            let resp = self.execute_non_streaming(req, cancel).await?;
            if !resp.content.is_empty() {
                let _ = tx
                    .send(InferenceStreamChunk::TextDelta(resp.content.clone()))
                    .await;
            }
            let _ = tx
                .send(InferenceStreamChunk::Completed(resp.clone()))
                .await;
            return Ok(resp);
        }

        let started = Instant::now();
        let (cfg, client, _root_url, v1_url) = self.snapshot_client()?;
        let model = resolve_model(&req.model, &cfg.model);
        let url = endpoint_url(&v1_url, req.api_mode);
        let headers = build_headers(req.api_mode, cfg.api_key.as_deref());
        let payload = build_request_payload(req, &cfg, &model, true);

        log_debug(
            "inference",
            format!("POST {url} api_mode={:?} model={model} stream=true", req.api_mode),
        );

        let req_timeout = effective_request_timeout(cfg.timeout_secs);
        let resp = self
            .send_with_connect_retries(
                &client,
                &url,
                headers,
                Some(&payload),
                cfg.max_retries,
                req_timeout,
                cancel,
            )
            .await?;

        let status = resp.status();
        if status == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            let body = match read_text_with_timeout(
                resp,
                req_timeout,
                cancel,
                "reading 503 response body",
            )
            .await
            {
                Ok(b) => b,
                Err(err @ AgentError::Cancelled(_)) => return Err(err),
                Err(_) => String::new(),
            };
            return Err(AgentError::InferenceUnavailable(format!(
                "PS5LM returned 503 unavailable: {}",
                truncate_for_err(&body, 256)
            )));
        }
        if !status.is_success() {
            let body = match read_text_with_timeout(
                resp,
                req_timeout,
                cancel,
                "reading error response body",
            )
            .await
            {
                Ok(b) => b,
                Err(err @ AgentError::Cancelled(_)) => return Err(err),
                Err(_) => String::new(),
            };
            return Err(AgentError::Inference(format!(
                "HTTP {} from inference endpoint: {}",
                status.as_u16(),
                truncate_for_err(&body, 256)
            )));
        }

        let mut sse_state = SseAccumulator::new(req.api_mode, model.clone());
        let initial_in_think = completions_prompt_starts_in_think(req.api_mode, &payload);
        let mut splitter = ThinkStreamSplitter::with_initial_in_think(initial_in_think);
        let mut byte_stream = resp.bytes_stream();
        let mut raw_buf: Vec<u8> = Vec::new();
        let mut current_event: Option<String> = None;

        loop {
            let next_chunk = match req_timeout {
                Some(idle_dur) => {
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => {
                            return Err(AgentError::Cancelled("inference stream cancelled".into()));
                        }
                        res = tokio::time::timeout(idle_dur, byte_stream.next()) => match res {
                            Ok(chunk) => chunk,
                            Err(_) => {
                                return Err(AgentError::Timeout(format!(
                                    "SSE stream idle timed out after {}s waiting for next chunk",
                                    idle_dur.as_secs()
                                )));
                            }
                        }
                    }
                }
                None => {
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => {
                            return Err(AgentError::Cancelled("inference stream cancelled".into()));
                        }
                        chunk = byte_stream.next() => chunk,
                    }
                }
            };

            match next_chunk {
                Some(Ok(bytes)) => {
                    check_sse_line_limit_bytes(raw_buf.len(), &bytes)?;
                    raw_buf.extend_from_slice(&bytes);

                    let mut consumed = 0usize;
                    while let Some(rel_nl) = raw_buf[consumed..].iter().position(|&b| b == b'\n') {
                        let line_end = consumed + rel_nl;
                        let mut line_bytes = &raw_buf[consumed..line_end];
                        if line_bytes.ends_with(b"\r") {
                            line_bytes = &line_bytes[..line_bytes.len() - 1];
                        }
                        let line = std::str::from_utf8(line_bytes).map_err(|e| {
                            AgentError::Inference(format!("invalid UTF-8 in SSE stream line: {e}"))
                        })?;
                        process_sse_line(
                            line,
                            &mut current_event,
                            &mut sse_state,
                            &mut splitter,
                            tx,
                        )
                        .await?;
                        consumed = line_end + 1;
                        if sse_state.aborted_loop_clean.is_some() {
                            break;
                        }
                    }
                    if consumed > 0 {
                        raw_buf.drain(..consumed);
                    }
                    if sse_state.aborted_loop_clean.is_some() {
                        break;
                    }
                }
                Some(Err(err)) => {
                    return Err(map_reqwest_body_err("SSE stream interrupted", err));
                }
                None => break,
            }
        }

        if !raw_buf.is_empty() && sse_state.aborted_loop_clean.is_none() {
            let mut tail = raw_buf.as_slice();
            if tail.ends_with(b"\r") {
                tail = &tail[..tail.len() - 1];
            }
            let line = std::str::from_utf8(tail).map_err(|e| {
                AgentError::Inference(format!("invalid UTF-8 in SSE stream trailing line: {e}"))
            })?;
            if !line.trim().is_empty() {
                process_sse_line(line, &mut current_event, &mut sse_state, &mut splitter, tx)
                    .await?;
            }
        }

        let was_aborted = sse_state.aborted_loop_clean.is_some();
        for piece in splitter.finish() {
            match &piece {
                StreamPiece::Reasoning(r) => {
                    sse_state.inline_reasoning.push_str(r);
                    emit_stream_piece(piece, tx).await;
                }
                StreamPiece::Text(t) if !was_aborted => {
                    if !t.is_empty() {
                        sse_state.emitted_text = true;
                    }
                    emit_stream_piece(piece, tx).await;
                }
                StreamPiece::Text(_) => {}
            }
        }

        sse_state.finalize_loop_guard_eof();

        if !sse_state.saw_done && sse_state.aborted_loop_clean.is_none() {
            return Err(AgentError::Inference(
                "incomplete SSE stream: stream ended before completion marker".into(),
            ));
        }

        let latency_ms = started.elapsed().as_millis() as u64;
        let final_resp = sse_state.into_response(latency_ms)?;
        let _ = tx.send(InferenceStreamChunk::Completed(final_resp.clone())).await;
        Ok(final_resp)
    }
}

#[async_trait]
impl InferenceProvider for Ps5LmProvider {
    async fn generate(
        &self,
        req: InferenceRequest,
        cancel: CancellationToken,
    ) -> Result<InferenceResponse> {
        let _permit = self.acquire_permit(&cancel).await?;
        let started = Instant::now();
        let res = self.execute_non_streaming(&req, &cancel).await;
        let latency_ms = started.elapsed().as_millis() as u64;
        match &res {
            Ok(resp) => {
                if let Ok(mut m) = self.metrics.lock() {
                    m.record_success(latency_ms, &resp.usage);
                }
            }
            Err(err) => {
                log_error("inference", format!("generate failed: {err}"));
                if let Ok(mut m) = self.metrics.lock() {
                    m.record_error(latency_ms);
                }
            }
        }
        res
    }

    async fn generate_stream(
        &self,
        req: InferenceRequest,
        tx: mpsc::Sender<InferenceStreamChunk>,
        cancel: CancellationToken,
    ) -> Result<InferenceResponse> {
        let _permit = self.acquire_permit(&cancel).await?;
        let started = Instant::now();
        let res = self.execute_streaming(&req, &tx, &cancel).await;
        let latency_ms = started.elapsed().as_millis() as u64;
        match &res {
            Ok(resp) => {
                if let Ok(mut m) = self.metrics.lock() {
                    m.record_success(latency_ms, &resp.usage);
                }
            }
            Err(err) => {
                log_error("inference", format!("generate_stream failed: {err}"));
                let _ = tx.send(InferenceStreamChunk::Error(err.to_string())).await;
                if let Ok(mut m) = self.metrics.lock() {
                    m.record_error(latency_ms);
                }
            }
        }
        res
    }

    async fn system_one(
        &self,
        req: SystemOneRequest,
        cancel: CancellationToken,
    ) -> Result<SystemOneResponse> {
        let _permit = self.acquire_permit(&cancel).await?;
        let started = Instant::now();
        let (cfg, client, _root_url, v1_url) = self.snapshot_client()?;
        let model = resolve_model(&req.model, &cfg.model);
        let url = endpoint_url(&v1_url, ApiMode::SystemOne);
        let headers = build_headers(ApiMode::SystemOne, cfg.api_key.as_deref());
        let payload = json!({
            "model": model,
            "state": req.state,
            "questions": req.questions,
        });

        log_debug("inference", format!("POST {url} model={model} (systemone)"));

        let req_timeout = effective_request_timeout(cfg.timeout_secs);
        let res = async {
            let resp = self
                .send_with_connect_retries(
                    &client,
                    &url,
                    headers,
                    Some(&payload),
                    cfg.max_retries,
                    req_timeout,
                    &cancel,
                )
                .await?;
            let status = resp.status();
            let text_body = read_text_with_timeout(
                resp,
                req_timeout,
                &cancel,
                "reading systemone response",
            )
            .await?;
            if status == reqwest::StatusCode::SERVICE_UNAVAILABLE {
                return Err(AgentError::InferenceUnavailable(format!(
                    "PS5LM returned 503 unavailable: {}",
                    truncate_for_err(&text_body, 256)
                )));
            }
            if !status.is_success() {
                return Err(AgentError::Inference(format!(
                    "HTTP {} from /v1/systemone: {}",
                    status.as_u16(),
                    truncate_for_err(&text_body, 256)
                )));
            }
            let val: Value = serde_json::from_str(&text_body)
                .map_err(|e| AgentError::Inference(format!("malformed /v1/systemone JSON: {e}")))?;
            let parsed_model = val
                .get("model")
                .and_then(|v| v.as_str())
                .unwrap_or(&model)
                .to_string();
            let answers = val
                .get("answers")
                .cloned()
                .ok_or_else(|| AgentError::Inference("missing answers in /v1/systemone response".into()))?;
            let usage = parse_usage(val.get("usage"));
            let latency_ms = started.elapsed().as_millis() as u64;
            Ok(SystemOneResponse {
                model: parsed_model,
                answers,
                usage,
                latency_ms,
            })
        }
        .await;

        let latency_ms = started.elapsed().as_millis() as u64;
        match &res {
            Ok(r) => {
                if let Ok(mut m) = self.metrics.lock() {
                    m.record_success(latency_ms, &r.usage);
                }
            }
            Err(err) => {
                log_error("inference", format!("system_one failed: {err}"));
                if let Ok(mut m) = self.metrics.lock() {
                    m.record_error(latency_ms);
                }
            }
        }
        res
    }

    async fn health_check(&self) -> ProviderHealth {
        let Ok((cfg, client, root_url, v1_url)) = self.snapshot_client() else {
            return ProviderHealth {
                reachable: false,
                loaded_model: None,
                available_models: Vec::new(),
                error: Some("provider state lock poisoned".into()),
            };
        };

        let health_url = format!("{root_url}/health");
        let headers = build_headers(ApiMode::ChatCompletions, cfg.api_key.as_deref());
        let cancel = CancellationToken::new();
        let probe_t = probe_timeout(&cfg);

        let health_res = self
            .send_with_connect_retries(
                &client,
                &health_url,
                headers,
                None,
                0,
                Some(probe_t),
                &cancel,
            )
            .await;

        let mut loaded_model: Option<String> = None;
        let health_err: Option<String>;
        let mut health_ok = false;

        match health_res {
            Ok(resp) => {
                let status = resp.status();
                match read_text_with_timeout(resp, Some(probe_t), &cancel, "reading /health").await {
                    Ok(body) => {
                        let (ok, lm, err) = parse_health_body(status, &body);
                        health_ok = ok;
                        loaded_model = lm;
                        health_err = err;
                    }
                    Err(e) => {
                        health_err = Some(e.to_string());
                    }
                }
            }
            Err(e) => {
                health_err = Some(e.to_string());
            }
        }

        let models_res = self
            .list_models_inner(&cfg, &client, &v1_url, Some(probe_t))
            .await;
        match models_res {
            Ok(models) => {
                if loaded_model.is_none() {
                    loaded_model = models.first().cloned();
                }
                ProviderHealth {
                    reachable: health_ok || health_err.is_none(),
                    loaded_model,
                    available_models: models,
                    error: health_err,
                }
            }
            Err(e) => ProviderHealth {
                reachable: health_ok,
                loaded_model,
                available_models: Vec::new(),
                error: Some(health_err.unwrap_or_else(|| e.to_string())),
            },
        }
    }

    async fn list_models(&self) -> Result<Vec<String>> {
        let (cfg, client, _root_url, v1_url) = self.snapshot_client()?;
        let req_timeout = effective_request_timeout(cfg.timeout_secs);
        self.list_models_inner(&cfg, &client, &v1_url, req_timeout).await
    }

    async fn discover_capabilities(&self) -> Result<Option<Ps5LmCapabilities>> {
        let (cfg, client, root_url, v1_url) = self.snapshot_client()?;
        let headers = build_headers(ApiMode::ChatCompletions, cfg.api_key.as_deref());
        let cancel = CancellationToken::new();
        let probe_t = probe_timeout(&cfg);

        let health_url = format!("{root_url}/health");
        let models_url = format!("{v1_url}/models");
        let props_url = format!("{root_url}/props");
        let slots_url = format!("{root_url}/slots");

        let (health_probe, models_probe, props_probe, slots_probe) = tokio::join!(
            self.probe_get_json(&client, &health_url, headers.clone(), probe_t, &cancel),
            self.probe_get_json(&client, &models_url, headers.clone(), probe_t, &cancel),
            self.probe_get_json(&client, &props_url, headers.clone(), probe_t, &cancel),
            self.probe_get_json(&client, &slots_url, headers, probe_t, &cancel),
        );

        let mut loaded_model: Option<String> = None;
        let mut available_models: Vec<String> = Vec::new();
        let mut context_length: Option<u32> = None;
        let mut models_context_fallback: Option<u32> = None;
        let mut total_slots: Option<u32> = None;
        let mut supports_vision = false;
        let mut supports_thinking = false;

        let mut health_ok = false;
        let mut health_503 = false;
        match health_probe {
            ProbeResult::Json(val) => {
                let bad_status = val
                    .get("status")
                    .and_then(|v| v.as_str())
                    .map(|s| {
                        let t = s.trim();
                        !(t.eq_ignore_ascii_case("ok")
                            || t.eq_ignore_ascii_case("healthy")
                            || t.eq_ignore_ascii_case("ready"))
                    })
                    .unwrap_or(false);
                let explicitly_not_ok = val.get("ok").and_then(|v| v.as_bool()) == Some(false);
                if !bad_status && !explicitly_not_ok {
                    health_ok = true;
                } else {
                    health_503 = true;
                }
                loaded_model = val
                    .get("loaded_model")
                    .or_else(|| val.get("model"))
                    .and_then(|v| v.as_str())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string);
            }
            ProbeResult::PlainOk => {
                health_ok = true;
            }
            ProbeResult::Unavailable503 => {
                health_503 = true;
            }
            ProbeResult::Skipped => {}
        }

        let mut models_ok = false;
        let mut models_503 = false;
        if let ProbeResult::Json(val) = models_probe {
            models_ok = true;
            available_models = parse_models_list(&val);
            models_context_fallback = extract_models_context_length(&val);
            if loaded_model.is_none() {
                loaded_model = available_models.first().cloned();
            }
        } else if matches!(models_probe, ProbeResult::Unavailable503) {
            models_503 = true;
        }

        let mut props_ok = false;
        if let ProbeResult::Json(val) = props_probe {
            props_ok = true;
            apply_props_json(
                &val,
                &mut loaded_model,
                &mut context_length,
                &mut total_slots,
                &mut supports_vision,
                &mut supports_thinking,
            );
        }

        let mut slots_ok = false;
        if let ProbeResult::Json(val) = slots_probe {
            slots_ok = true;
            apply_slots_json(&val, &mut context_length, &mut total_slots);
        }

        context_length = context_length.or(models_context_fallback);

        for name in loaded_model.iter().chain(available_models.iter()) {
            let (vision_hint, thinking_hint) = infer_model_hints(name);
            supports_vision |= vision_hint;
            supports_thinking |= thinking_hint;
        }

        let healthy = (health_ok || models_ok || props_ok || slots_ok) && !health_503 && !models_503;

        Ok(Some(Ps5LmCapabilities {
            base_url: cfg.base_url,
            healthy,
            loaded_model,
            available_models,
            context_length,
            total_slots,
            supports_vision,
            supports_thinking,
            synced_at: Some(now_iso()),
        }))
    }

    async fn prefill_kv_cache(
        &self,
        messages: Vec<InferenceMessage>,
        model_override: Option<String>,
        cancel: CancellationToken,
    ) -> Result<()> {
        if messages.is_empty() || cancel.is_cancelled() {
            return Ok(());
        }
        let Ok((cfg, _, _, _)) = self.snapshot_client() else {
            return Ok(());
        };
        let thinking_level = cfg
            .thinking_level
            .parse::<ThinkingLevel>()
            .unwrap_or(ThinkingLevel::Off);
        let req = InferenceRequest {
            model: model_override,
            api_mode: ApiMode::ChatCompletions,
            messages,
            tools: Vec::new(),
            temperature: cfg.temperature,
            max_output_tokens: 0,
            context_limit: cfg.context_limit,
            stream: false,
            thinking_level,
            slot_id: None,
        };
        self.prefill_kv_cache_request(req, cancel).await
    }

    async fn prefill_kv_cache_request(
        &self,
        req: InferenceRequest,
        cancel: CancellationToken,
    ) -> Result<()> {
        if req.messages.is_empty() || cancel.is_cancelled() {
            return Ok(());
        }
        if req.api_mode == ApiMode::SystemOne {
            return Ok(());
        }
        let Ok((cfg, client, _root_url, v1_url)) = self.snapshot_client() else {
            return Ok(());
        };
        if !cfg.prefill_kv_cache {
            return Ok(());
        }
        let Some(_permit) = self.gate.try_acquire() else {
            log_debug("inference", "KV cache prefill skipped (inference slot busy)");
            return Ok(());
        };

        let normalized = normalize_conversation_messages(&req.messages, false);
        if normalized.is_empty() {
            return Ok(());
        }

        let model = resolve_model(&req.model, &cfg.model);
        let prefill_mode = match req.api_mode {
            ApiMode::Completions => ApiMode::Completions,
            _ => ApiMode::ChatCompletions,
        };
        let url = endpoint_url(&v1_url, prefill_mode);
        let headers = build_headers(prefill_mode, cfg.api_key.as_deref());
        let mut payload = match prefill_mode {
            ApiMode::Completions => {
                build_completions_payload(&req, &normalized, &cfg, &model, false)
            }
            _ => build_chat_completions_payload(&req, &normalized, &cfg, &model, false),
        };
        payload["stream"] = json!(false);
        payload["cache_prompt"] = json!(true);
        payload["max_tokens"] = json!(0);
        payload["n_predict"] = json!(0);
        if let Some(obj) = payload.as_object_mut() {
            obj.remove("stream_options");
        }

        let send_fut = client
            .post(&url)
            .headers(headers)
            .json(&payload)
            .timeout(Duration::from_secs(30))
            .send();

        let res = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(()),
            r = send_fut => r,
        };

        match res {
            Ok(resp) if resp.status().is_success() => {
                log_debug("inference", format!("KV cache prefill succeeded for model={model}"));
            }
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                log_warn(
                    "inference",
                    format!(
                        "KV cache prefill returned HTTP {}: {}",
                        status.as_u16(),
                        truncate_for_err(&body, 192)
                    ),
                );
            }
            Err(err) => {
                log_warn("inference", format!("KV cache prefill request failed: {err}"));
            }
        }
        Ok(())
    }

    async fn import_kv_and_sampling(&self) -> Result<Ps5LmKvSamplingImport> {
        let (cfg, client, root_url, _v1_url) = self.snapshot_client()?;
        let headers = build_headers(ApiMode::ChatCompletions, cfg.api_key.as_deref());
        let cancel = CancellationToken::new();
        let probe_t = probe_timeout(&cfg);

        let props_url = format!("{root_url}/props");
        let slots_url = format!("{root_url}/slots");
        let health_url = format!("{root_url}/health");

        let (props_probe, slots_probe, health_probe) = tokio::join!(
            self.probe_get_json(&client, &props_url, headers.clone(), probe_t, &cancel),
            self.probe_get_json(&client, &slots_url, headers.clone(), probe_t, &cancel),
            self.probe_get_json(&client, &health_url, headers, probe_t, &cancel),
        );

        let mut sources: Vec<&'static str> = Vec::new();
        let props_val = match props_probe {
            ProbeResult::Json(v) => {
                sources.push("props");
                Some(v)
            }
            _ => None,
        };
        let slots_val = match slots_probe {
            ProbeResult::Json(v) => {
                sources.push("slots");
                Some(v)
            }
            _ => None,
        };
        let health_val = match health_probe {
            ProbeResult::Json(v) => {
                sources.push("health");
                Some(v)
            }
            ProbeResult::PlainOk => {
                sources.push("health");
                None
            }
            _ => None,
        };

        if sources.is_empty() {
            return Err(AgentError::InferenceUnavailable(
                "PS5LM /props, /slots, and /health endpoints were unreachable or returned unavailable status"
                    .to_string(),
            ));
        }

        Ok(parse_kv_and_sampling_from_probes(
            props_val.as_ref(),
            slots_val.as_ref(),
            health_val.as_ref(),
            sources,
        ))
    }
}

enum ProbeResult {
    Json(Value),
    PlainOk,
    Unavailable503,
    Skipped,
}

impl Ps5LmProvider {
    async fn probe_get_json(
        &self,
        client: &reqwest::Client,
        url: &str,
        headers: HeaderMap,
        probe_timeout: Duration,
        cancel: &CancellationToken,
    ) -> ProbeResult {
        let Ok(resp) = self
            .send_with_connect_retries(
                client,
                url,
                headers,
                None,
                0,
                Some(probe_timeout),
                cancel,
            )
            .await
        else {
            return ProbeResult::Skipped;
        };
        let status = resp.status();
        if status == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            return ProbeResult::Unavailable503;
        }
        if !status.is_success() {
            return ProbeResult::Skipped;
        }
        let Ok(text) =
            read_text_with_timeout(resp, Some(probe_timeout), cancel, "reading probe response")
                .await
        else {
            return ProbeResult::Skipped;
        };
        if text.trim().eq_ignore_ascii_case("ok") {
            return ProbeResult::PlainOk;
        }
        match serde_json::from_str::<Value>(&text) {
            Ok(val) => ProbeResult::Json(val),
            Err(_) => ProbeResult::Skipped,
        }
    }

    async fn list_models_inner(
        &self,
        cfg: &InferenceConfig,
        client: &reqwest::Client,
        v1_url: &str,
        request_timeout: Option<Duration>,
    ) -> Result<Vec<String>> {
        let url = format!("{v1_url}/models");
        let headers = build_headers(ApiMode::ChatCompletions, cfg.api_key.as_deref());
        let cancel = CancellationToken::new();
        let resp = self
            .send_with_connect_retries(
                client,
                &url,
                headers,
                None,
                cfg.max_retries,
                request_timeout,
                &cancel,
            )
            .await?;

        let status = resp.status();
        let text_body =
            read_text_with_timeout(resp, request_timeout, &cancel, "reading /v1/models").await?;

        if status == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            return Err(AgentError::InferenceUnavailable(format!(
                "PS5LM returned 503 on /v1/models: {}",
                truncate_for_err(&text_body, 256)
            )));
        }
        if !status.is_success() {
            return Err(AgentError::Inference(format!(
                "HTTP {} from /v1/models: {}",
                status.as_u16(),
                truncate_for_err(&text_body, 256)
            )));
        }

        let val: Value = serde_json::from_str(&text_body)
            .map_err(|e| AgentError::Inference(format!("malformed /v1/models JSON: {e}")))?;

        Ok(parse_models_list(&val))
    }
}

fn check_sse_line_limit_bytes(pending_len: usize, chunk: &[u8]) -> Result<()> {
    let mut rem_start = 0usize;
    let mut first = true;
    while let Some(rel_nl) = chunk[rem_start..].iter().position(|&b| b == b'\n') {
        let seg_len = if first {
            first = false;
            pending_len.saturating_add(rel_nl)
        } else {
            rel_nl
        };
        if seg_len > MAX_SSE_LINE_BYTES {
            return Err(AgentError::Inference(format!(
                "SSE line exceeded maximum size of {MAX_SSE_LINE_BYTES} bytes"
            )));
        }
        rem_start += rel_nl + 1;
    }
    let tail_len = if first {
        pending_len.saturating_add(chunk.len())
    } else {
        chunk.len().saturating_sub(rem_start)
    };
    if tail_len > MAX_SSE_LINE_BYTES {
        return Err(AgentError::Inference(format!(
            "SSE line exceeded maximum size of {MAX_SSE_LINE_BYTES} bytes"
        )));
    }
    Ok(())
}

#[cfg(test)]
fn check_sse_line_limit(pending_len: usize, chunk: &str) -> Result<()> {
    check_sse_line_limit_bytes(pending_len, chunk.as_bytes())
}

fn parse_models_list(val: &Value) -> Vec<String> {
    let mut models = Vec::new();
    if let Some(arr) = val.get("data").and_then(|d| d.as_array()) {
        for item in arr {
            if let Some(id) = item.get("id").and_then(|v| v.as_str()) {
                models.push(id.to_string());
            }
        }
    } else if let Some(arr) = val.get("models").and_then(|d| d.as_array()) {
        for item in arr {
            if let Some(id) = item.as_str().or_else(|| item.get("id").and_then(|v| v.as_str())) {
                models.push(id.to_string());
            }
        }
    }
    models
}

fn extract_models_context_length(val: &Value) -> Option<u32> {
    let arr = val.get("data").and_then(|d| d.as_array())?;
    for item in arr {
        let n = item
            .get("n_ctx")
            .and_then(|v| v.as_u64())
            .or_else(|| {
                item.get("meta")
                    .and_then(|m| m.get("n_ctx"))
                    .and_then(|v| v.as_u64())
            })
            .or_else(|| item.get("context_length").and_then(|v| v.as_u64()))
            .or_else(|| item.get("max_model_len").and_then(|v| v.as_u64()));
        if let Some(ctx) = n.and_then(|x| u32::try_from(x).ok()).filter(|&x| x > 0) {
            return Some(ctx);
        }
    }
    None
}

fn apply_props_json(
    val: &Value,
    loaded_model: &mut Option<String>,
    context_length: &mut Option<u32>,
    total_slots: &mut Option<u32>,
    supports_vision: &mut bool,
    supports_thinking: &mut bool,
) {
    if context_length.is_none() {
        *context_length = val
            .get("default_generation_settings")
            .and_then(|d| d.get("n_ctx"))
            .and_then(|v| v.as_u64())
            .or_else(|| val.get("n_ctx").and_then(|v| v.as_u64()))
            .and_then(|n| u32::try_from(n).ok())
            .filter(|&n| n > 0);
    }

    if total_slots.is_none() {
        *total_slots = val
            .get("total_slots")
            .and_then(|v| v.as_u64())
            .and_then(|n| u32::try_from(n).ok())
            .filter(|&n| n > 0);
    }

    if loaded_model.is_none() {
        *loaded_model = extract_model_from_props_json(val);
    }

    if let Some(modalities) = val.get("modalities") {
        if modalities.get("vision").and_then(|v| v.as_bool()) == Some(true)
            || modalities.get("mmproj").and_then(|v| v.as_bool()) == Some(true)
            || modalities
                .get("mmproj")
                .and_then(|v| v.as_str())
                .is_some_and(|s| !s.trim().is_empty())
        {
            *supports_vision = true;
        }
    }

    if val
        .get("chat_template_caps")
        .and_then(|c| c.get("supports_thinking"))
        .and_then(|v| v.as_bool())
        == Some(true)
    {
        *supports_thinking = true;
    }

    for key in ["chat_template", "chat_template_tool_use"] {
        if let Some(tmpl) = val.get(key).and_then(|v| v.as_str()) {
            if tmpl.contains("<think>")
                || tmpl.contains("<seed:think>")
                || tmpl.contains("enable_thinking")
                || tmpl.contains("reasoning_content")
            {
                *supports_thinking = true;
            }
        }
    }
}

fn extract_model_from_props_json(val: &Value) -> Option<String> {
    let alias = val
        .get("model_alias")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let from_path = val
        .get("model_path")
        .and_then(|v| v.as_str())
        .and_then(|p| std::path::Path::new(p).file_name())
        .and_then(|f| f.to_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let direct = val
        .get("model")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    alias.or(from_path).or(direct)
}

fn extract_slots_array(val: &Value) -> Option<&Vec<Value>> {
    val.as_array()
        .or_else(|| val.get("slots").and_then(Value::as_array))
}

fn apply_slots_json(
    val: &Value,
    context_length: &mut Option<u32>,
    total_slots: &mut Option<u32>,
) {
    let Some(arr) = extract_slots_array(val) else {
        return;
    };
    if total_slots.is_none() && !arr.is_empty() {
        *total_slots = u32::try_from(arr.len()).ok().filter(|&n| n > 0);
    }
    if context_length.is_none() {
        *context_length = arr
            .first()
            .and_then(|slot| slot.get("n_ctx"))
            .and_then(|v| v.as_u64())
            .and_then(|n| u32::try_from(n).ok())
            .filter(|&n| n > 0);
    }
}

fn normalize_sampler_token(raw: &str) -> Option<String> {
    let t = raw.trim();
    if t.is_empty() {
        return None;
    }
    let mapped = match t.to_ascii_lowercase().as_str() {
        "e" | "cp" | "penalties" => "penalties",
        "d" | "dry" => "dry",
        "k" | "top_k" | "top-k" => "top_k",
        "y" | "typ_p" | "typical_p" | "typical" => "typ_p",
        "p" | "top_p" | "top-p" => "top_p",
        "m" | "min_p" | "min-p" => "min_p",
        "x" | "xtc" => "xtc",
        "t" | "temp" | "temperature" => "temperature",
        "s" | "top_n_sigma" => "top_n_sigma",
        "i" | "infill" => "infill",
        _ => return Some(t.to_string()),
    };
    Some(mapped.to_string())
}

fn parse_samplers_str(raw: &str) -> Option<Vec<String>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.starts_with('[') {
        if let Ok(arr) = serde_json::from_str::<Vec<String>>(trimmed) {
            let normalized: Vec<String> = arr
                .iter()
                .filter_map(|s| normalize_sampler_token(s))
                .collect();
            if !normalized.is_empty() {
                return Some(normalized);
            }
        }
    }
    if trimmed.contains(',') || trimmed.contains(';') || trimmed.chars().any(char::is_whitespace) {
        let normalized: Vec<String> = trimmed
            .split(|c: char| c == ',' || c == ';' || c.is_whitespace())
            .filter_map(normalize_sampler_token)
            .collect();
        return (!normalized.is_empty()).then_some(normalized);
    }
    let lower = trimmed.to_ascii_lowercase();
    let is_named = matches!(
        lower.as_str(),
        "penalties"
            | "dry"
            | "top_k"
            | "top-k"
            | "typ_p"
            | "typical_p"
            | "typical"
            | "top_p"
            | "top-p"
            | "min_p"
            | "min-p"
            | "xtc"
            | "temperature"
            | "temp"
            | "mirostat"
            | "infill"
            | "top_n_sigma"
    );
    if !is_named
        && trimmed.len() >= 2
        && trimmed.chars().all(|c| {
            matches!(
                c.to_ascii_lowercase(),
                'e' | 'd' | 'k' | 'y' | 'p' | 'm' | 'x' | 't' | 's' | 'i'
            )
        })
    {
        let expanded: Vec<String> = trimmed
            .chars()
            .filter_map(|c| {
                let mut buf = [0u8; 4];
                normalize_sampler_token(c.encode_utf8(&mut buf))
            })
            .collect();
        if !expanded.is_empty() {
            return Some(expanded);
        }
    }
    normalize_sampler_token(trimmed).map(|s| vec![s])
}

fn parse_samplers_value(val: &Value) -> Option<Vec<String>> {
    match val {
        Value::Array(arr) => {
            let normalized: Vec<String> = arr
                .iter()
                .filter_map(|v| v.as_str())
                .filter_map(normalize_sampler_token)
                .collect();
            (!normalized.is_empty()).then_some(normalized)
        }
        Value::String(s) => parse_samplers_str(s),
        _ => None,
    }
}

fn pick_non_null_value<'a>(containers: &[Option<&'a Value>], keys: &[&str]) -> Option<&'a Value> {
    for container in containers.iter().flatten() {
        for &key in keys {
            if let Some(v) = container.get(key) {
                if !v.is_null() {
                    return Some(v);
                }
            }
        }
    }
    None
}

fn pick_f32_finite(containers: &[Option<&Value>], keys: &[&str], min_zero: bool) -> Option<f32> {
    let raw = pick_non_null_value(containers, keys)?.as_f64()?;
    if !raw.is_finite() || (min_zero && raw < 0.0) {
        return None;
    }
    let val = raw as f32;
    val.is_finite().then_some(val)
}

fn pick_f32_unit_interval(containers: &[Option<&Value>], keys: &[&str]) -> Option<f32> {
    pick_f32_finite(containers, keys, true).map(|v| v.clamp(0.0, 1.0))
}

fn pick_i32_field(containers: &[Option<&Value>], keys: &[&str]) -> Option<i32> {
    pick_non_null_value(containers, keys)?
        .as_i64()
        .and_then(|n| i32::try_from(n).ok())
}

fn pick_u32_field(containers: &[Option<&Value>], keys: &[&str]) -> Option<u32> {
    pick_non_null_value(containers, keys)?
        .as_u64()
        .and_then(|n| u32::try_from(n).ok())
}

fn pick_kv_cache_type(containers: &[Option<&Value>], keys: &[&str]) -> Option<String> {
    let s = pick_non_null_value(containers, keys)?.as_str()?.trim();
    if s.is_empty() {
        return None;
    }
    let lower = s.to_ascii_lowercase();
    lower
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
        .then_some(lower)
}

fn pick_flash_attn(containers: &[Option<&Value>], keys: &[&str]) -> Option<bool> {
    match pick_non_null_value(containers, keys)? {
        Value::Bool(b) => Some(*b),
        Value::Number(n) => match n.as_i64() {
            Some(1) => Some(true),
            Some(0) => Some(false),
            _ => None,
        },
        Value::String(s) => match s.trim().to_ascii_lowercase().as_str() {
            "true" | "on" | "enabled" | "enable" | "1" | "yes" => Some(true),
            "false" | "off" | "disabled" | "disable" | "0" | "no" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

fn parse_kv_and_sampling_from_probes(
    props: Option<&Value>,
    slots: Option<&Value>,
    health: Option<&Value>,
    sources: Vec<&'static str>,
) -> Ps5LmKvSamplingImport {
    let dgs = props.and_then(|p| p.get("default_generation_settings"));
    let dgs_params = dgs.and_then(|d| d.get("params"));
    let props_params = props.and_then(|p| p.get("params"));
    let props_kv = props.and_then(|p| p.get("kv_cache"));
    let props_cache = props.and_then(|p| p.get("cache"));
    let props_server = props.and_then(|p| p.get("server_params"));

    let slots_arr = slots.and_then(extract_slots_array);
    let slot0 = slots_arr.and_then(|arr| arr.first());
    let slot0_params = slot0.and_then(|s| s.get("params"));
    let slot0_kv = slot0.and_then(|s| s.get("kv_cache"));

    let sampling_sources = [dgs_params, dgs, props_params, props, slot0_params, slot0];
    let kv_sources = [
        props_kv,
        props_cache,
        props_server,
        dgs_params,
        dgs,
        props_params,
        props,
        slot0_params,
        slot0,
        slot0_kv,
    ];

    let model = health
        .and_then(|h| {
            h.get("loaded_model")
                .or_else(|| h.get("model"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        })
        .or_else(|| props.and_then(extract_model_from_props_json));

    let n_ctx = dgs
        .and_then(|d| d.get("n_ctx"))
        .and_then(|v| v.as_u64())
        .or_else(|| props.and_then(|p| p.get("n_ctx")).and_then(|v| v.as_u64()))
        .or_else(|| slot0.and_then(|s| s.get("n_ctx")).and_then(|v| v.as_u64()))
        .and_then(|n| u32::try_from(n).ok())
        .filter(|&n| n > 0);

    let total_slots = props
        .and_then(|p| p.get("total_slots"))
        .and_then(|v| v.as_u64())
        .and_then(|n| u32::try_from(n).ok())
        .filter(|&n| n > 0)
        .or_else(|| {
            slots_arr
                .and_then(|arr| u32::try_from(arr.len()).ok())
                .filter(|&n| n > 0)
        });

    Ps5LmKvSamplingImport {
        model,
        n_ctx,
        total_slots,
        cache_type_k: pick_kv_cache_type(&kv_sources, &["cache_type_k", "type_k", "kv_cache_type_k"]),
        cache_type_v: pick_kv_cache_type(&kv_sources, &["cache_type_v", "type_v", "kv_cache_type_v"]),
        flash_attn: pick_flash_attn(&kv_sources, &["flash_attn", "flash_attention"]),
        n_keep: pick_u32_field(&sampling_sources, &["n_keep"]),
        temperature: pick_f32_finite(&sampling_sources, &["temperature", "temp"], true),
        top_k: pick_i32_field(&sampling_sources, &["top_k"]),
        top_p: pick_f32_unit_interval(&sampling_sources, &["top_p"]),
        min_p: pick_f32_unit_interval(&sampling_sources, &["min_p"]),
        repeat_penalty: pick_f32_finite(&sampling_sources, &["repeat_penalty"], true),
        repeat_last_n: pick_i32_field(&sampling_sources, &["repeat_last_n"]),
        presence_penalty: pick_f32_finite(&sampling_sources, &["presence_penalty"], false),
        frequency_penalty: pick_f32_finite(&sampling_sources, &["frequency_penalty"], false),
        dry_multiplier: pick_f32_finite(&sampling_sources, &["dry_multiplier"], true),
        dry_base: pick_f32_finite(&sampling_sources, &["dry_base"], true),
        dry_allowed_length: pick_u32_field(&sampling_sources, &["dry_allowed_length"]),
        dry_penalty_last_n: pick_i32_field(&sampling_sources, &["dry_penalty_last_n"]),
        xtc_probability: pick_f32_unit_interval(&sampling_sources, &["xtc_probability"]),
        xtc_threshold: pick_f32_finite(&sampling_sources, &["xtc_threshold"], true),
        samplers: pick_non_null_value(&sampling_sources, &["samplers"]).and_then(parse_samplers_value),
        raw_source: sources.join("+"),
    }
}

fn infer_model_hints(model_name: &str) -> (bool, bool) {
    let lower = model_name.to_ascii_lowercase();
    let tokens: Vec<&str> = lower
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|t| !t.is_empty())
        .collect();
    let has_tok = |needle: &str| tokens.iter().any(|&t| t == needle);
    let has_pair = |a: &str, b: &str| tokens.windows(2).any(|w| w[0] == a && w[1] == b);

    let vision = has_tok("vl") || has_tok("vision") || has_tok("llava");
    let thinking = tokens.iter().any(|t| t.starts_with("qwen3"))
        || has_tok("qwq")
        || has_tok("deepseek")
        || has_tok("r1")
        || has_tok("thinking")
        || has_tok("seed")
        || has_pair("gpt", "oss")
        || has_tok("gptoss");
    (vision, thinking)
}

fn effective_request_timeout(timeout_secs: u64) -> Option<Duration> {
    if timeout_secs == 0 {
        None
    } else {
        Some(Duration::from_secs(timeout_secs))
    }
}

fn probe_timeout(cfg: &InferenceConfig) -> Duration {
    let mut secs = if cfg.connect_timeout_secs == 0 {
        5
    } else {
        cfg.connect_timeout_secs.clamp(2, 5)
    };
    if (1..=5).contains(&cfg.timeout_secs) {
        secs = secs.min(cfg.timeout_secs);
    }
    Duration::from_secs(secs)
}

async fn read_text_with_timeout(
    resp: reqwest::Response,
    timeout: Option<Duration>,
    cancel: &CancellationToken,
    context: &str,
) -> Result<String> {
    let cancel_msg = format!("inference cancelled {context}");
    match timeout {
        Some(dur) => {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => Err(AgentError::Cancelled(cancel_msg)),
                res = tokio::time::timeout(dur, resp.text()) => match res {
                    Ok(Ok(s)) => Ok(s),
                    Ok(Err(e)) => Err(map_reqwest_body_err(context, e)),
                    Err(_) => Err(AgentError::Timeout(format!(
                        "{context} timed out after {}s",
                        dur.as_secs()
                    ))),
                }
            }
        }
        None => {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => Err(AgentError::Cancelled(cancel_msg)),
                res = resp.text() => res.map_err(|e| map_reqwest_body_err(context, e)),
            }
        }
    }
}

fn completions_prompt_starts_in_think(mode: ApiMode, payload: &Value) -> bool {
    if mode != ApiMode::Completions {
        return false;
    }
    let Some(prompt) = payload.get("prompt").and_then(|v| v.as_str()) else {
        return false;
    };
    let tail = prompt
        .rfind("<|im_start|>assistant")
        .map(|pos| &prompt[pos..])
        .unwrap_or(prompt);
    let lower = tail.to_ascii_lowercase();
    let has_open_std = match (lower.rfind("<think>"), lower.rfind("</think>")) {
        (Some(open), Some(close)) => open > close,
        (Some(_), None) => true,
        _ => false,
    };
    let has_open_seed = match (lower.rfind("<seed:think>"), lower.rfind("</seed:think>")) {
        (Some(open), Some(close)) => open > close,
        (Some(_), None) => true,
        _ => false,
    };
    has_open_std || has_open_seed
}

fn parse_health_body(
    status: reqwest::StatusCode,
    body: &str,
) -> (bool, Option<String>, Option<String>) {
    if !status.is_success() {
        let err = if status == reqwest::StatusCode::NOT_FOUND {
            None
        } else {
            Some(format!(
                "HTTP {} from /health: {}",
                status.as_u16(),
                truncate_for_err(body, 128)
            ))
        };
        return (false, None, err);
    }
    let trimmed = body.trim();
    if trimmed.eq_ignore_ascii_case("ok") {
        return (true, None, None);
    }
    match serde_json::from_str::<Value>(trimmed) {
        Ok(val) => {
            let loaded = val
                .get("loaded_model")
                .or_else(|| val.get("model"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string);
            if let Some(st) = val.get("status").and_then(|v| v.as_str()) {
                let st_trim = st.trim();
                if st_trim.eq_ignore_ascii_case("ok")
                    || st_trim.eq_ignore_ascii_case("healthy")
                    || st_trim.eq_ignore_ascii_case("ready")
                {
                    return (true, loaded, None);
                }
                return (
                    false,
                    loaded,
                    Some(format!("unhealthy /health status: {st_trim}")),
                );
            }
            if val.get("ok").and_then(|v| v.as_bool()) == Some(false) {
                return (
                    false,
                    loaded,
                    Some("health check reported ok=false".into()),
                );
            }
            (true, loaded, None)
        }
        Err(e) => (false, None, Some(format!("malformed /health JSON: {e}"))),
    }
}

fn build_http_client(cfg: &InferenceConfig) -> Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .tcp_nodelay(true)
        .pool_idle_timeout(Duration::from_secs(45));
    if cfg.connect_timeout_secs > 0 {
        builder = builder.connect_timeout(Duration::from_secs(cfg.connect_timeout_secs));
    }
    builder
        .build()
        .map_err(|e| AgentError::Internal(format!("failed to build HTTP client: {e}")))
}

fn normalize_base_urls(base_url: &str) -> (String, String) {
    let trimmed = base_url.trim().trim_end_matches('/');
    if let Some(root) = trimmed.strip_suffix("/v1") {
        (root.to_string(), trimmed.to_string())
    } else {
        (trimmed.to_string(), format!("{trimmed}/v1"))
    }
}

fn resolve_model(req_model: &Option<String>, cfg_model: &str) -> String {
    match req_model {
        Some(m) if !m.trim().is_empty() => m.trim().to_string(),
        _ => cfg_model.to_string(),
    }
}

fn endpoint_url(v1_url: &str, mode: ApiMode) -> String {
    match mode {
        ApiMode::ChatCompletions => format!("{v1_url}/chat/completions"),
        ApiMode::Responses => format!("{v1_url}/responses"),
        ApiMode::AnthropicMessages => format!("{v1_url}/messages"),
        ApiMode::Completions => format!("{v1_url}/completions"),
        ApiMode::SystemOne => format!("{v1_url}/systemone"),
    }
}

fn build_headers(mode: ApiMode, api_key: Option<&str>) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    if mode == ApiMode::AnthropicMessages {
        headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
    }
    if let Some(key) = api_key.map(str::trim).filter(|k| !k.is_empty()) {
        match mode {
            ApiMode::AnthropicMessages => {
                if let Ok(val) = HeaderValue::from_str(key) {
                    headers.insert("x-api-key", val);
                }
            }
            ApiMode::ChatCompletions
            | ApiMode::Responses
            | ApiMode::Completions
            | ApiMode::SystemOne => {
                if let Ok(val) = HeaderValue::from_str(&format!("Bearer {key}")) {
                    headers.insert(AUTHORIZATION, val);
                }
            }
        }
    }
    headers
}

fn build_request_payload(
    req: &InferenceRequest,
    cfg: &InferenceConfig,
    model: &str,
    stream: bool,
) -> Value {
    let normalized = normalize_conversation_messages(&req.messages, false);
    match req.api_mode {
        ApiMode::ChatCompletions => {
            build_chat_completions_payload(req, &normalized, cfg, model, stream)
        }
        ApiMode::Responses => build_responses_payload(req, &normalized, cfg, model, stream),
        ApiMode::AnthropicMessages => build_anthropic_payload(req, &normalized, model, stream),
        ApiMode::Completions => build_completions_payload(req, &normalized, cfg, model, stream),
        ApiMode::SystemOne => build_systemone_payload(req, &normalized, model),
    }
}

pub fn apply_llamacpp_sampling_fields(
    payload: &mut Value,
    cfg: &InferenceConfig,
    req: &InferenceRequest,
) {
    let Some(obj) = payload.as_object_mut() else {
        return;
    };

    obj.insert("cache_prompt".into(), json!(cfg.cache_prompt));
    obj.insert("top_k".into(), json!(cfg.top_k));
    obj.insert("top_p".into(), json!(cfg.top_p));
    obj.insert("min_p".into(), json!(cfg.min_p));
    if cfg.repeat_penalty.is_finite() && (cfg.repeat_penalty - 1.0).abs() >= 1e-5 {
        obj.insert("repeat_penalty".into(), json!(cfg.repeat_penalty));
    }
    obj.insert("repeat_last_n".into(), json!(cfg.repeat_last_n));
    if cfg.presence_penalty.is_finite() && cfg.presence_penalty.abs() >= 1e-5 {
        obj.insert("presence_penalty".into(), json!(cfg.presence_penalty));
    }
    if cfg.frequency_penalty.is_finite() && cfg.frequency_penalty.abs() >= 1e-5 {
        obj.insert("frequency_penalty".into(), json!(cfg.frequency_penalty));
    }

    if cfg.dynatemp_range > 0.0 {
        obj.insert("dynatemp_range".into(), json!(cfg.dynatemp_range));
        obj.insert("dynatemp_exponent".into(), json!(cfg.dynatemp_exponent));
    }
    if cfg.typical_p < 1.0 {
        obj.insert("typical_p".into(), json!(cfg.typical_p));
    }
    if cfg.xtc_probability > 0.0 {
        obj.insert("xtc_probability".into(), json!(cfg.xtc_probability));
        obj.insert("xtc_threshold".into(), json!(cfg.xtc_threshold));
    }
    if cfg.top_n_sigma > 0.0 {
        obj.insert("top_n_sigma".into(), json!(cfg.top_n_sigma));
    }
    if cfg.dry_multiplier > 0.0 {
        obj.insert("dry_multiplier".into(), json!(cfg.dry_multiplier));
        obj.insert("dry_base".into(), json!(cfg.dry_base));
        obj.insert("dry_allowed_length".into(), json!(cfg.dry_allowed_length));
        obj.insert("dry_penalty_last_n".into(), json!(cfg.dry_penalty_last_n));
    }
    if cfg.mirostat > 0 {
        obj.insert("mirostat".into(), json!(cfg.mirostat));
        obj.insert("mirostat_tau".into(), json!(cfg.mirostat_tau));
        obj.insert("mirostat_eta".into(), json!(cfg.mirostat_eta));
    }
    if cfg.seed != -1 {
        obj.insert("seed".into(), json!(cfg.seed));
    }
    if cfg.ignore_eos {
        obj.insert("ignore_eos".into(), json!(true));
    }
    if cfg.n_keep != 0 {
        obj.insert("n_keep".into(), json!(cfg.n_keep));
    }
    let effective_slot = req.slot_id.unwrap_or(cfg.id_slot);
    if effective_slot >= 0 {
        obj.insert("id_slot".into(), json!(effective_slot));
    }

    if let Some(list) = parse_samplers_str(&cfg.samplers) {
        obj.insert("samplers".into(), json!(list));
    }

    if !cfg.grammar.trim().is_empty() {
        obj.insert("grammar".into(), Value::String(cfg.grammar.clone()));
    }
    if cfg.timings_per_token {
        obj.insert("timings_per_token".into(), json!(true));
    }

    if !cfg.stop_sequences.is_empty() {
        let mut stops: Vec<String> = obj
            .get("stop")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        for s in &cfg.stop_sequences {
            if !s.is_empty() && !stops.contains(s) {
                stops.push(s.clone());
            }
        }
        if !stops.is_empty() {
            obj.insert("stop".into(), json!(stops));
        }
    }

    if req.thinking_level != ThinkingLevel::Off {
        let rf = cfg.reasoning_format.trim();
        if !rf.is_empty() && !rf.eq_ignore_ascii_case("none") {
            obj.insert("reasoning_format".into(), Value::String(rf.to_string()));
        }
    }

    let extra = cfg.custom_extra_body_json.trim();
    if !extra.is_empty() {
        match serde_json::from_str::<Value>(extra) {
            Ok(Value::Object(map)) => {
                for (k, v) in map {
                    if !matches!(k.as_str(), "messages" | "input" | "prompt" | "stream") {
                        obj.insert(k, v);
                    }
                }
            }
            Ok(_) => {
                log_warn(
                    "inference",
                    "ignoring custom_extra_body_json: expected a JSON object",
                );
            }
            Err(err) => {
                log_warn(
                    "inference",
                    format!("ignoring invalid custom_extra_body_json: {err}"),
                );
            }
        }
    }
}

fn anthropic_image_block(url: &str) -> Value {
    let trimmed = url.trim();
    if let Some(rest) = trimmed.strip_prefix("data:") {
        if let Some((meta, data)) = rest.split_once(";base64,") {
            let media_type = meta.split(';').next().unwrap_or("").trim();
            let media_type = if media_type.is_empty() {
                "image/png"
            } else {
                media_type
            };
            return json!({
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": media_type,
                    "data": data,
                }
            });
        }
    }
    json!({
        "type": "image",
        "source": {
            "type": "url",
            "url": trimmed,
        }
    })
}

fn build_chat_completions_payload(
    req: &InferenceRequest,
    messages: &[InferenceMessage],
    cfg: &InferenceConfig,
    model: &str,
    stream: bool,
) -> Value {
    let msgs_json: Vec<Value> = messages
        .iter()
        .map(|m| {
            let content_val = if m.image_urls.is_empty() {
                Value::String(m.content.clone())
            } else {
                let mut parts = Vec::with_capacity(1 + m.image_urls.len());
                parts.push(json!({"type": "text", "text": m.content}));
                for url in &m.image_urls {
                    parts.push(json!({
                        "type": "image_url",
                        "image_url": {"url": url},
                    }));
                }
                Value::Array(parts)
            };
            let mut obj = json!({
                "role": m.role,
                "content": content_val,
            });
            if let Some(ref tc_id) = m.tool_call_id {
                obj["tool_call_id"] = Value::String(tc_id.clone());
            }
            if m.role.eq_ignore_ascii_case("tool") {
                if let Some(ref name) = m.name {
                    if !name.trim().is_empty() {
                        obj["name"] = Value::String(name.clone());
                    }
                }
            }
            if let Some(ref tcs) = m.tool_calls {
                if !tcs.is_empty() {
                    let arr: Vec<Value> = tcs
                        .iter()
                        .map(|tc| {
                            json!({
                                "id": tc.id,
                                "type": "function",
                                "function": {
                                    "name": tc.name,
                                    "arguments": serde_json::to_string(&tc.arguments).unwrap_or_else(|_| "{}".into()),
                                }
                            })
                        })
                        .collect();
                    obj["tool_calls"] = Value::Array(arr);
                }
            }
            obj
        })
        .collect();

    let mut payload = json!({
        "model": model,
        "messages": msgs_json,
        "temperature": req.temperature,
        "stream": stream,
    });
    if stream {
        payload["stream_options"] = json!({"include_usage": true});
    }
    if req.max_output_tokens > 0 {
        payload["max_tokens"] = json!(req.max_output_tokens);
    }

    match req.thinking_level {
        ThinkingLevel::Off => {
            payload["chat_template_kwargs"] = json!({"enable_thinking": false});
            payload["reasoning_budget"] = json!(0);
            payload["reasoning_format"] = Value::String("none".to_string());
        }
        ThinkingLevel::Low => {
            payload["chat_template_kwargs"] = json!({"enable_thinking": true});
            payload["reasoning_effort"] = Value::String("low".to_string());
            payload["reasoning_budget"] = json!(512);
        }
        ThinkingLevel::Medium => {
            payload["chat_template_kwargs"] = json!({"enable_thinking": true});
            payload["reasoning_effort"] = Value::String("medium".to_string());
            payload["reasoning_budget"] = json!(2048);
        }
        ThinkingLevel::High => {
            payload["chat_template_kwargs"] = json!({"enable_thinking": true});
            payload["reasoning_effort"] = Value::String("high".to_string());
            payload["reasoning_budget"] = json!(8192);
        }
        ThinkingLevel::Max => {
            payload["chat_template_kwargs"] = json!({"enable_thinking": true});
            payload["reasoning_effort"] = Value::String("high".to_string());
            payload["reasoning_budget"] = json!(-1);
        }
    }

    if !req.tools.is_empty() {
        let tools_json: Vec<Value> = req
            .tools
            .iter()
            .map(|t| {
                json!({
                    "type": "function",
                    "function": {
                        "name": t.name,
                        "description": t.description,
                        "parameters": t.parameters,
                    }
                })
            })
            .collect();
        payload["tools"] = Value::Array(tools_json);
    }

    apply_llamacpp_sampling_fields(&mut payload, cfg, req);
    if !stream {
        if let Some(obj) = payload.as_object_mut() {
            obj.remove("stream_options");
        }
    }
    if !cfg.stop_sequences.iter().any(|s| !s.is_empty()) && payload.get("stop").is_none() {
        payload["stop"] = json!(["<|im_end|>", "<|endoftext|>"]);
    }

    payload
}

fn build_responses_payload(
    req: &InferenceRequest,
    messages: &[InferenceMessage],
    cfg: &InferenceConfig,
    model: &str,
    stream: bool,
) -> Value {
    let (instructions, rest) = if messages
        .first()
        .is_some_and(|m| m.role.eq_ignore_ascii_case("system"))
    {
        (Some(messages[0].content.clone()), &messages[1..])
    } else {
        (None, messages)
    };

    let mut input_items = Vec::new();
    for m in rest {
        if m.role.eq_ignore_ascii_case("tool") {
            input_items.push(json!({
                "type": "function_call_output",
                "call_id": m.tool_call_id.clone().unwrap_or_default(),
                "output": m.content,
            }));
        } else {
            if !m.image_urls.is_empty() {
                let mut parts = Vec::with_capacity(1 + m.image_urls.len());
                parts.push(json!({"type": "input_text", "text": m.content}));
                for url in &m.image_urls {
                    parts.push(json!({
                        "type": "input_image",
                        "image_url": url,
                    }));
                }
                input_items.push(json!({
                    "role": m.role,
                    "content": parts,
                }));
            } else if !m.content.is_empty() || m.tool_calls.as_ref().is_none_or(|t| t.is_empty()) {
                input_items.push(json!({
                    "role": m.role,
                    "content": m.content,
                }));
            }
            if let Some(ref tcs) = m.tool_calls {
                for tc in tcs {
                    input_items.push(json!({
                        "type": "function_call",
                        "call_id": tc.id,
                        "name": tc.name,
                        "arguments": serde_json::to_string(&tc.arguments).unwrap_or_else(|_| "{}".into()),
                    }));
                }
            }
        }
    }

    let mut payload = json!({
        "model": model,
        "input": input_items,
        "temperature": req.temperature,
        "stream": stream,
    });
    if req.max_output_tokens > 0 {
        payload["max_output_tokens"] = json!(req.max_output_tokens);
    }
    match req.thinking_level {
        ThinkingLevel::Off => {}
        ThinkingLevel::Low => {
            payload["reasoning"] = json!({"effort": "low"});
        }
        ThinkingLevel::Medium => {
            payload["reasoning"] = json!({"effort": "medium"});
        }
        ThinkingLevel::High | ThinkingLevel::Max => {
            payload["reasoning"] = json!({"effort": "high"});
        }
    }
    if let Some(inst) = instructions {
        payload["instructions"] = Value::String(inst);
    }
    if !req.tools.is_empty() {
        let tools_json: Vec<Value> = req
            .tools
            .iter()
            .map(|t| {
                json!({
                    "type": "function",
                    "name": t.name,
                    "description": t.description,
                    "parameters": t.parameters,
                })
            })
            .collect();
        payload["tools"] = Value::Array(tools_json);
    }

    apply_llamacpp_sampling_fields(&mut payload, cfg, req);
    if !stream {
        if let Some(obj) = payload.as_object_mut() {
            obj.remove("stream_options");
        }
    }

    payload
}

fn push_anthropic_user_blocks(msgs_json: &mut Vec<Value>, mut new_blocks: Vec<Value>) {
    if new_blocks.is_empty() {
        return;
    }
    if let Some(last) = msgs_json.last_mut() {
        let is_user = last
            .get("role")
            .and_then(|r| r.as_str())
            .is_some_and(|r| r.eq_ignore_ascii_case("user"));
        if is_user {
            match last.get_mut("content") {
                Some(Value::Array(arr)) => {
                    arr.append(&mut new_blocks);
                    return;
                }
                Some(Value::String(prev)) => {
                    let prev_str = std::mem::take(prev);
                    let mut merged = Vec::with_capacity(1 + new_blocks.len());
                    if !prev_str.is_empty() {
                        merged.push(json!({"type": "text", "text": prev_str}));
                    }
                    merged.append(&mut new_blocks);
                    if let Some(obj) = last.as_object_mut() {
                        obj.insert("content".into(), Value::Array(merged));
                    }
                    return;
                }
                _ => {}
            }
        }
    }
    msgs_json.push(json!({
        "role": "user",
        "content": Value::Array(new_blocks),
    }));
}

fn build_anthropic_payload(
    req: &InferenceRequest,
    messages: &[InferenceMessage],
    model: &str,
    stream: bool,
) -> Value {
    let (system_prompt, rest) = if messages
        .first()
        .is_some_and(|m| m.role.eq_ignore_ascii_case("system"))
    {
        (Some(messages[0].content.clone()), &messages[1..])
    } else {
        (None, messages)
    };

    let mut msgs_json = Vec::new();
    for m in rest {
        if m.role.eq_ignore_ascii_case("tool") {
            push_anthropic_user_blocks(
                &mut msgs_json,
                vec![json!({
                    "type": "tool_result",
                    "tool_use_id": m.tool_call_id.clone().unwrap_or_default(),
                    "content": m.content,
                })],
            );
        } else if m.role.eq_ignore_ascii_case("assistant")
            && m.tool_calls.as_ref().is_some_and(|t| !t.is_empty())
        {
            let mut blocks = Vec::new();
            if !m.content.trim().is_empty() {
                blocks.push(json!({"type": "text", "text": m.content}));
            }
            for url in &m.image_urls {
                blocks.push(anthropic_image_block(url));
            }
            if let Some(ref tcs) = m.tool_calls {
                for tc in tcs {
                    blocks.push(json!({
                        "type": "tool_use",
                        "id": tc.id,
                        "name": tc.name,
                        "input": tc.arguments,
                    }));
                }
            }
            msgs_json.push(json!({
                "role": "assistant",
                "content": blocks,
            }));
        } else if m.role.eq_ignore_ascii_case("user") {
            let prev_is_user = msgs_json
                .last()
                .and_then(|msg| msg.get("role"))
                .and_then(|r| r.as_str())
                .is_some_and(|r| r.eq_ignore_ascii_case("user"));
            if prev_is_user || !m.image_urls.is_empty() {
                let mut blocks = Vec::with_capacity(1 + m.image_urls.len());
                if !m.content.is_empty() {
                    blocks.push(json!({"type": "text", "text": m.content}));
                }
                for url in &m.image_urls {
                    blocks.push(anthropic_image_block(url));
                }
                if !blocks.is_empty() {
                    push_anthropic_user_blocks(&mut msgs_json, blocks);
                } else if !prev_is_user {
                    msgs_json.push(json!({
                        "role": "user",
                        "content": m.content,
                    }));
                }
            } else {
                msgs_json.push(json!({
                    "role": "user",
                    "content": m.content,
                }));
            }
        } else if m.image_urls.is_empty() {
            msgs_json.push(json!({
                "role": m.role,
                "content": m.content,
            }));
        } else {
            let mut blocks = Vec::with_capacity(1 + m.image_urls.len());
            if !m.content.is_empty() {
                blocks.push(json!({"type": "text", "text": m.content}));
            }
            for url in &m.image_urls {
                blocks.push(anthropic_image_block(url));
            }
            msgs_json.push(json!({
                "role": m.role,
                "content": blocks,
            }));
        }
    }

    let effective_max_tokens = if req.max_output_tokens == 0 {
        ANTHROPIC_DEFAULT_MAX_TOKENS
    } else {
        req.max_output_tokens
    };

    let mut payload = json!({
        "model": model,
        "messages": msgs_json,
        "temperature": req.temperature,
        "max_tokens": effective_max_tokens,
        "stream": stream,
    });
    let raw_budget = match req.thinking_level {
        ThinkingLevel::Off => None,
        ThinkingLevel::Low => Some(1024u32),
        ThinkingLevel::Medium => Some(4096u32),
        ThinkingLevel::High => Some(8192u32),
        ThinkingLevel::Max => Some(16384u32),
    };
    let ends_with_assistant = payload["messages"]
        .as_array()
        .and_then(|arr| arr.last())
        .and_then(|m| m.get("role"))
        .and_then(|r| r.as_str())
        .is_some_and(|r| r.eq_ignore_ascii_case("assistant"));
    if !ends_with_assistant {
        if let Some(raw) = raw_budget {
            let budget = raw.min(effective_max_tokens.saturating_sub(1));
            if budget > 0 {
                payload["thinking"] = json!({
                    "type": "enabled",
                    "budget_tokens": budget,
                });
            }
        }
    }
    if let Some(sys) = system_prompt {
        payload["system"] = Value::String(sys);
    }
    if !req.tools.is_empty() {
        let tools_json: Vec<Value> = req
            .tools
            .iter()
            .map(|t| {
                json!({
                    "name": t.name,
                    "description": t.description,
                    "input_schema": t.parameters,
                })
            })
            .collect();
        payload["tools"] = Value::Array(tools_json);
    }

    payload
}

fn build_completions_payload(
    req: &InferenceRequest,
    messages: &[InferenceMessage],
    cfg: &InferenceConfig,
    model: &str,
    stream: bool,
) -> Value {
    let mut prompt = String::new();
    let has_system_msg = messages
        .first()
        .is_some_and(|m| m.role.trim().eq_ignore_ascii_case("system"));
    let ends_with_plain_assistant = messages.last().is_some_and(|m| {
        m.role.trim().eq_ignore_ascii_case("assistant")
            && m.tool_calls.as_ref().map_or(true, |t| t.is_empty())
    });
    let last_user_idx = if req.thinking_level == ThinkingLevel::Off && !has_system_msg {
        messages
            .iter()
            .rposition(|m| m.role.trim().eq_ignore_ascii_case("user"))
    } else {
        None
    };

    for (idx, m) in messages.iter().enumerate() {
        let role = m.role.trim();
        prompt.push_str("<|im_start|>");
        prompt.push_str(role);
        prompt.push('\n');
        prompt.push_str(&m.content);
        if idx == 0 && role.eq_ignore_ascii_case("system") && !req.tools.is_empty() {
            if let Ok(tools_json) = serde_json::to_string_pretty(&req.tools) {
                prompt.push_str(
                    "\n\nAvailable tools (invoke using <tool_call>{\"name\": \"...\", \"arguments\": {...}}</tool_call>):\n",
                );
                prompt.push_str(&tools_json);
            }
        }
        if req.thinking_level == ThinkingLevel::Off
            && !m.content.contains("/no_think")
            && !m.content.contains("/think")
            && ((has_system_msg && idx == 0) || last_user_idx == Some(idx))
        {
            prompt.push_str("\n/no_think");
        }
        if let Some(ref tcs) = m.tool_calls {
            for tc in tcs {
                if let Ok(call_json) = serde_json::to_string(&json!({
                    "name": tc.name,
                    "arguments": tc.arguments,
                })) {
                    prompt.push_str("\n<tool_call>");
                    prompt.push_str(&call_json);
                    prompt.push_str("</tool_call>");
                }
            }
        }
        let is_trailing_continuation = ends_with_plain_assistant && idx + 1 == messages.len();
        if !is_trailing_continuation {
            prompt.push_str("<|im_end|>\n");
        }
    }
    if !ends_with_plain_assistant {
        prompt.push_str("<|im_start|>assistant\n");
    }

    let mut payload = json!({
        "model": model,
        "prompt": prompt,
        "temperature": req.temperature,
        "stream": stream,
        "stop": ["<|im_end|>"],
    });
    if stream {
        payload["stream_options"] = json!({"include_usage": true});
    }
    if req.max_output_tokens > 0 {
        payload["max_tokens"] = json!(req.max_output_tokens);
    }
    apply_llamacpp_sampling_fields(&mut payload, cfg, req);
    if !stream {
        if let Some(obj) = payload.as_object_mut() {
            obj.remove("stream_options");
        }
    }
    payload
}

fn build_systemone_payload(
    req: &InferenceRequest,
    messages: &[InferenceMessage],
    model: &str,
) -> Value {
    if let Some(last_user) = messages.iter().rev().find(|m| m.role.eq_ignore_ascii_case("user")) {
        if let Ok(mut parsed) = serde_json::from_str::<Value>(&last_user.content) {
            if parsed.get("state").is_some() && parsed.get("questions").is_some() {
                if let Some(obj) = parsed.as_object_mut() {
                    obj.insert("model".into(), Value::String(model.to_string()));
                }
                return parsed;
            }
        }
    }

    let state = messages
        .iter()
        .map(|m| format!("{}: {}", m.role, m.content))
        .collect::<Vec<_>>()
        .join("\n");

    let questions = if !req.tools.is_empty() {
        let mut criteria = serde_json::Map::new();
        for t in &req.tools {
            criteria.insert(t.name.clone(), Value::String(t.description.clone()));
        }
        json!({
            "selected_tool": {
                "type": "choice",
                "instructions": "Select the best matching tool for the current state.",
                "criteria": Value::Object(criteria),
            },
            "invoke_tool": {
                "type": "noul",
                "instructions": "Does the current state require executing a tool?",
            }
        })
    } else {
        json!({
            "assessment": {
                "type": "noul",
                "instructions": "Evaluate whether the statement or request should be approved or affirmed.",
            },
            "priority": {
                "type": "score",
                "instructions": "Score the urgency or priority level.",
                "criteria": ["Low", "Normal", "High"],
            }
        })
    };

    json!({
        "model": model,
        "state": state,
        "questions": questions,
    })
}

fn parse_non_streaming_response(
    mode: ApiMode,
    default_model: &str,
    val: &Value,
    latency_ms: u64,
) -> Result<InferenceResponse> {
    match mode {
        ApiMode::ChatCompletions => parse_chat_completions_json(default_model, val, latency_ms),
        ApiMode::Responses => parse_responses_json(default_model, val, latency_ms),
        ApiMode::AnthropicMessages => parse_anthropic_json(default_model, val, latency_ms),
        ApiMode::Completions => parse_completions_json(default_model, val, latency_ms),
        ApiMode::SystemOne => parse_systemone_json(default_model, val, latency_ms),
    }
}

fn parse_completions_json(
    default_model: &str,
    val: &Value,
    latency_ms: u64,
) -> Result<InferenceResponse> {
    let model = val
        .get("model")
        .and_then(|v| v.as_str())
        .unwrap_or(default_model)
        .to_string();

    let choice = val
        .get("choices")
        .and_then(|c| c.as_array())
        .and_then(|arr| arr.first())
        .ok_or_else(|| AgentError::Inference("missing choices[0] in /v1/completions response".into()))?;

    let raw_content = choice
        .get("text")
        .and_then(|v| v.as_str())
        .or_else(|| {
            choice
                .get("message")
                .and_then(|m| m.get("content"))
                .and_then(|v| v.as_str())
        })
        .unwrap_or("")
        .to_string();

    let finish_reason = choice
        .get("finish_reason")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let usage = parse_usage(val.get("usage"));
    let timings = parse_timings(val.get("timings"));
    let parsed = parse_model_output(&raw_content, None, Vec::new());

    Ok(InferenceResponse {
        content: parsed.content,
        reasoning: parsed.reasoning,
        tool_calls: parsed.tool_calls,
        model,
        finish_reason,
        usage,
        latency_ms,
        timings,
    })
}

fn parse_systemone_json(
    default_model: &str,
    val: &Value,
    latency_ms: u64,
) -> Result<InferenceResponse> {
    let model = val
        .get("model")
        .and_then(|v| v.as_str())
        .unwrap_or(default_model)
        .to_string();
    let answers = val
        .get("answers")
        .ok_or_else(|| AgentError::Inference("missing answers in /v1/systemone response".into()))?;
    let content = serde_json::to_string_pretty(answers)
        .unwrap_or_else(|_| answers.to_string());
    let usage = parse_usage(val.get("usage"));

    Ok(InferenceResponse {
        content,
        reasoning: None,
        tool_calls: Vec::new(),
        model,
        finish_reason: Some("stop".to_string()),
        usage,
        latency_ms,
        timings: None,
    })
}

fn parse_chat_completions_json(
    default_model: &str,
    val: &Value,
    latency_ms: u64,
) -> Result<InferenceResponse> {
    let model = val
        .get("model")
        .and_then(|v| v.as_str())
        .unwrap_or(default_model)
        .to_string();

    let choice = val
        .get("choices")
        .and_then(|c| c.as_array())
        .and_then(|arr| arr.first())
        .ok_or_else(|| AgentError::Inference("missing choices[0] in chat completions response".into()))?;

    let msg = choice
        .get("message")
        .ok_or_else(|| AgentError::Inference("missing choices[0].message".into()))?;

    let raw_content = msg
        .get("content")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let api_reasoning = msg
        .get("reasoning_content")
        .or_else(|| msg.get("reasoning"))
        .and_then(|v| v.as_str())
        .map(str::to_string);

    let mut api_tool_calls = Vec::new();
    if let Some(tcs) = msg.get("tool_calls").and_then(|v| v.as_array()) {
        for tc in tcs {
            if let Some(parsed) = parse_openai_tool_call(tc) {
                api_tool_calls.push(parsed);
            }
        }
    }

    let finish_reason = choice
        .get("finish_reason")
        .and_then(|v| v.as_str())
        .map(str::to_string);

    let usage = parse_usage(val.get("usage"));
    let timings = parse_timings(val.get("timings"));
    let parsed = parse_model_output(&raw_content, api_reasoning, api_tool_calls);

    Ok(InferenceResponse {
        content: parsed.content,
        reasoning: parsed.reasoning,
        tool_calls: parsed.tool_calls,
        model,
        finish_reason,
        usage,
        latency_ms,
        timings,
    })
}

fn parse_responses_json(
    default_model: &str,
    val: &Value,
    latency_ms: u64,
) -> Result<InferenceResponse> {
    let model = val
        .get("model")
        .and_then(|v| v.as_str())
        .unwrap_or(default_model)
        .to_string();

    let mut text_parts = Vec::new();
    let mut reasoning_parts = Vec::new();
    let mut api_tool_calls = Vec::new();

    if let Some(items) = val.get("output").and_then(|v| v.as_array()) {
        for item in items {
            let kind = item.get("type").and_then(|t| t.as_str()).unwrap_or("");
            match kind {
                "message" => {
                    if let Some(content_arr) = item.get("content").and_then(|c| c.as_array()) {
                        for part in content_arr {
                            if let Some(txt) = part.get("text").and_then(|t| t.as_str()) {
                                text_parts.push(txt.to_string());
                            }
                        }
                    } else if let Some(txt) = item.get("content").and_then(|c| c.as_str()) {
                        text_parts.push(txt.to_string());
                    }
                }
                "reasoning" => {
                    reasoning_parts.extend(extract_responses_reasoning_parts(item));
                }
                "function_call" => {
                    let id = item
                        .get("call_id")
                        .or_else(|| item.get("id"))
                        .and_then(|v| v.as_str())
                        .map(str::to_string)
                        .unwrap_or_else(|| format!("call_{}", uuid::Uuid::new_v4()));
                    if let Some(name) = item.get("name").and_then(|v| v.as_str()) {
                        let arguments = parse_arguments_value(item.get("arguments"));
                        api_tool_calls.push(ToolCallRequest {
                            id,
                            name: name.to_string(),
                            arguments,
                        });
                    }
                }
                _ => {}
            }
        }
    }

    if text_parts.is_empty() {
        if let Some(out_txt) = val.get("output_text").and_then(|v| v.as_str()) {
            text_parts.push(out_txt.to_string());
        }
    }

    if text_parts.is_empty() && api_tool_calls.is_empty() && val.get("output").is_none() {
        return Err(AgentError::Inference(
            "missing output in /v1/responses response".into(),
        ));
    }

    let raw_content = text_parts.join("");
    let api_reasoning = if reasoning_parts.is_empty() {
        None
    } else {
        Some(reasoning_parts.join("\n"))
    };
    let finish_reason = val
        .get("status")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let usage = parse_usage(val.get("usage"));
    let timings = parse_timings(val.get("timings"));
    let parsed = parse_model_output(&raw_content, api_reasoning, api_tool_calls);

    Ok(InferenceResponse {
        content: parsed.content,
        reasoning: parsed.reasoning,
        tool_calls: parsed.tool_calls,
        model,
        finish_reason,
        usage,
        latency_ms,
        timings,
    })
}

fn parse_anthropic_json(
    default_model: &str,
    val: &Value,
    latency_ms: u64,
) -> Result<InferenceResponse> {
    let model = val
        .get("model")
        .and_then(|v| v.as_str())
        .unwrap_or(default_model)
        .to_string();

    let blocks = val
        .get("content")
        .and_then(|v| v.as_array())
        .ok_or_else(|| AgentError::Inference("missing content array in /v1/messages response".into()))?;

    let mut text_parts = Vec::new();
    let mut reasoning_parts = Vec::new();
    let mut api_tool_calls = Vec::new();

    for b in blocks {
        match b.get("type").and_then(|t| t.as_str()).unwrap_or("") {
            "text" => {
                if let Some(txt) = b.get("text").and_then(|t| t.as_str()) {
                    text_parts.push(txt.to_string());
                }
            }
            "thinking" => {
                if let Some(txt) = b.get("thinking").and_then(|t| t.as_str()) {
                    reasoning_parts.push(txt.to_string());
                }
            }
            "tool_use" => {
                let id = b
                    .get("id")
                    .and_then(|v| v.as_str())
                    .map(str::to_string)
                    .unwrap_or_else(|| format!("call_{}", uuid::Uuid::new_v4()));
                if let Some(name) = b.get("name").and_then(|v| v.as_str()) {
                    let arguments = match b.get("input") {
                        Some(Value::Null) | None => json!({}),
                        Some(other) => other.clone(),
                    };
                    api_tool_calls.push(ToolCallRequest {
                        id,
                        name: name.to_string(),
                        arguments,
                    });
                }
            }
            _ => {}
        }
    }

    let raw_content = text_parts.join("");
    let api_reasoning = if reasoning_parts.is_empty() {
        None
    } else {
        Some(reasoning_parts.join("\n"))
    };
    let finish_reason = val
        .get("stop_reason")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let usage = parse_usage(val.get("usage"));
    let timings = parse_timings(val.get("timings"));
    let parsed = parse_model_output(&raw_content, api_reasoning, api_tool_calls);

    Ok(InferenceResponse {
        content: parsed.content,
        reasoning: parsed.reasoning,
        tool_calls: parsed.tool_calls,
        model,
        finish_reason,
        usage,
        latency_ms,
        timings,
    })
}

fn parse_openai_tool_call(tc: &Value) -> Option<ToolCallRequest> {
    let func = tc.get("function")?;
    let name = func.get("name").and_then(|v| v.as_str())?.trim();
    if name.is_empty() {
        return None;
    }
    let id = tc
        .get("id")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| format!("call_{}", uuid::Uuid::new_v4()));
    let arguments = parse_arguments_value(func.get("arguments"));
    Some(ToolCallRequest {
        id,
        name: name.to_string(),
        arguments,
    })
}

fn parse_arguments_value(val: Option<&Value>) -> Value {
    match val {
        None | Some(Value::Null) => json!({}),
        Some(Value::String(s)) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                json!({})
            } else {
                serde_json::from_str(trimmed).unwrap_or_else(|_| Value::String(s.clone()))
            }
        }
        Some(other) => other.clone(),
    }
}

fn parse_usage(usage_val: Option<&Value>) -> TokenUsage {
    let Some(u) = usage_val else {
        return TokenUsage::default();
    };
    let prompt_tokens = u
        .get("prompt_tokens")
        .or_else(|| u.get("input_tokens"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as u32;
    let completion_tokens = u
        .get("completion_tokens")
        .or_else(|| u.get("output_tokens"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as u32;
    let total_tokens = u
        .get("total_tokens")
        .and_then(|v| v.as_u64())
        .map(|t| t as u32)
        .unwrap_or(prompt_tokens.saturating_add(completion_tokens));

    TokenUsage {
        prompt_tokens,
        completion_tokens,
        total_tokens,
    }
}

fn finite_non_neg_f64(val: Option<&Value>) -> Option<f64> {
    val.and_then(|v| v.as_f64())
        .filter(|&x| x.is_finite() && x >= 0.0)
}

fn opt_u32(val: Option<&Value>) -> Option<u32> {
    val.and_then(|v| v.as_u64())
        .and_then(|n| u32::try_from(n).ok())
}

fn parse_timings(timings_val: Option<&Value>) -> Option<InferenceTimings> {
    let obj = timings_val?.as_object()?;
    let t = InferenceTimings {
        predicted_per_second: finite_non_neg_f64(obj.get("predicted_per_second")),
        prompt_per_second: finite_non_neg_f64(obj.get("prompt_per_second")),
        predicted_ms: finite_non_neg_f64(obj.get("predicted_ms")),
        prompt_ms: finite_non_neg_f64(obj.get("prompt_ms")),
        predicted_n: opt_u32(obj.get("predicted_n")),
        prompt_n: opt_u32(obj.get("prompt_n")),
        cache_n: opt_u32(obj.get("cache_n")),
    };
    if t.predicted_per_second.is_none()
        && t.prompt_per_second.is_none()
        && t.predicted_ms.is_none()
        && t.prompt_ms.is_none()
        && t.predicted_n.is_none()
        && t.prompt_n.is_none()
        && t.cache_n.is_none()
    {
        None
    } else {
        Some(t)
    }
}

fn merge_timings(dst: &mut Option<InferenceTimings>, incoming: Option<InferenceTimings>) {
    let Some(inc) = incoming else {
        return;
    };
    match dst {
        None => *dst = Some(inc),
        Some(cur) => {
            if inc.predicted_per_second.is_some() {
                cur.predicted_per_second = inc.predicted_per_second;
            }
            if inc.prompt_per_second.is_some() {
                cur.prompt_per_second = inc.prompt_per_second;
            }
            if inc.predicted_ms.is_some() {
                cur.predicted_ms = inc.predicted_ms;
            }
            if inc.prompt_ms.is_some() {
                cur.prompt_ms = inc.prompt_ms;
            }
            if inc.predicted_n.is_some() {
                cur.predicted_n = inc.predicted_n;
            }
            if inc.prompt_n.is_some() {
                cur.prompt_n = inc.prompt_n;
            }
            if inc.cache_n.is_some() {
                cur.cache_n = inc.cache_n;
            }
        }
    }
}

#[derive(Default)]
struct ToolCallPartial {
    id: String,
    name: String,
    arguments_buf: String,
    arguments_val: Option<Value>,
}

struct SseAccumulator {
    mode: ApiMode,
    model: String,
    raw_content: String,
    api_reasoning: String,
    inline_reasoning: String,
    tool_calls: BTreeMap<usize, ToolCallPartial>,
    response_item_to_index: BTreeMap<String, usize>,
    finish_reason: Option<String>,
    usage: TokenUsage,
    timings: Option<InferenceTimings>,
    saw_done: bool,
    final_response_override: Option<Value>,
    loop_guard: StreamingRepetitionGuard,
    loop_guard_finished: bool,
    aborted_loop_clean: Option<String>,
    emitted_text: bool,
}

impl SseAccumulator {
    fn new(mode: ApiMode, model: String) -> Self {
        Self {
            mode,
            model,
            raw_content: String::new(),
            api_reasoning: String::new(),
            inline_reasoning: String::new(),
            tool_calls: BTreeMap::new(),
            response_item_to_index: BTreeMap::new(),
            finish_reason: None,
            usage: TokenUsage::default(),
            timings: None,
            saw_done: false,
            final_response_override: None,
            loop_guard: StreamingRepetitionGuard::new(),
            loop_guard_finished: false,
            aborted_loop_clean: None,
            emitted_text: false,
        }
    }

    async fn push_content_delta(
        &mut self,
        delta: &str,
        splitter: &mut ThinkStreamSplitter,
        tx: &mpsc::Sender<InferenceStreamChunk>,
    ) {
        if self.aborted_loop_clean.is_some() || delta.is_empty() {
            return;
        }
        match self.loop_guard.push_delta(delta) {
            StreamingLoopDecision::Continue => {
                self.raw_content.push_str(delta);
                for piece in splitter.push(delta) {
                    match &piece {
                        StreamPiece::Reasoning(r) => {
                            self.inline_reasoning.push_str(r);
                        }
                        StreamPiece::Text(t) => {
                            if !t.is_empty() {
                                self.emitted_text = true;
                            }
                        }
                    }
                    emit_stream_piece(piece, tx).await;
                }
            }
            StreamingLoopDecision::AbortRepeatedLoop {
                truncated_clean_visible,
            } => {
                for piece in splitter.push(delta) {
                    if let StreamPiece::Reasoning(r) = &piece {
                        self.inline_reasoning.push_str(r);
                        emit_stream_piece(piece, tx).await;
                    }
                }
                if !self.emitted_text && !truncated_clean_visible.is_empty() {
                    self.emitted_text = true;
                    let _ = tx
                        .send(InferenceStreamChunk::TextDelta(
                            truncated_clean_visible.clone(),
                        ))
                        .await;
                }
                self.aborted_loop_clean = Some(truncated_clean_visible);
                self.finish_reason = Some("repetition_abort".to_string());
            }
        }
    }

    fn finalize_loop_guard_eof(&mut self) {
        if self.loop_guard_finished {
            return;
        }
        self.loop_guard_finished = true;
        if self.aborted_loop_clean.is_some() {
            return;
        }
        if let StreamingLoopDecision::AbortRepeatedLoop {
            truncated_clean_visible,
        } = self.loop_guard.finish()
        {
            self.aborted_loop_clean = Some(truncated_clean_visible);
            self.finish_reason = Some("repetition_abort".to_string());
        }
    }

    fn resolve_response_tc_index(&self, val: &Value) -> usize {
        val.get("item_id")
            .and_then(|v| v.as_str())
            .and_then(|id| self.response_item_to_index.get(id).copied())
            .or_else(|| val.get("output_index").and_then(|v| v.as_u64()).map(|n| n as usize))
            .unwrap_or_else(|| self.tool_calls.keys().next_back().copied().unwrap_or(0))
    }

    fn finalize_tool_calls(&self) -> Vec<ToolCallRequest> {
        self.tool_calls
            .values()
            .filter(|tc| !tc.name.trim().is_empty())
            .map(|tc| {
                let id = if tc.id.is_empty() {
                    format!("call_{}", uuid::Uuid::new_v4())
                } else {
                    tc.id.clone()
                };
                let arguments = if let Some(ref v) = tc.arguments_val {
                    v.clone()
                } else if tc.arguments_buf.trim().is_empty() {
                    json!({})
                } else {
                    serde_json::from_str(tc.arguments_buf.trim())
                        .unwrap_or_else(|_| Value::String(tc.arguments_buf.clone()))
                };
                ToolCallRequest {
                    id,
                    name: tc.name.clone(),
                    arguments,
                }
            })
            .collect()
    }

    fn into_response(mut self, latency_ms: u64) -> Result<InferenceResponse> {
        if let Some(clean_visible) = self.aborted_loop_clean.take() {
            let api_tcs = self.finalize_tool_calls();
            let reasoning = match (
                non_empty_opt(&self.api_reasoning),
                non_empty_opt(&self.inline_reasoning),
            ) {
                (Some(api), Some(inline)) if api != inline => Some(format!("{api}\n{inline}")),
                (Some(api), _) => Some(api),
                (None, Some(inline)) => Some(inline),
                (None, None) => None,
            };
            return Ok(InferenceResponse {
                content: truncate_verbatim_prose_loops(&clean_visible),
                reasoning,
                tool_calls: api_tcs,
                model: self.model,
                finish_reason: self
                    .finish_reason
                    .or_else(|| Some("repetition_abort".to_string())),
                usage: self.usage,
                latency_ms,
                timings: self.timings,
            });
        }

        if self.mode == ApiMode::Responses {
            if let Some(ref resp_obj) = self.final_response_override {
                if let Ok(mut parsed) = parse_responses_json(&self.model, resp_obj, latency_ms) {
                    let streamed_reasoning = non_empty_opt(&self.api_reasoning);
                    if parsed.content.is_empty() && !self.raw_content.is_empty() {
                        let reparsed = parse_model_output(
                            &self.raw_content,
                            streamed_reasoning.clone(),
                            parsed.tool_calls,
                        );
                        parsed.content = reparsed.content;
                        if parsed.reasoning.is_none() {
                            parsed.reasoning = reparsed.reasoning;
                        }
                        parsed.tool_calls = reparsed.tool_calls;
                    }
                    if parsed.reasoning.is_none() {
                        parsed.reasoning = streamed_reasoning;
                    }
                    if parsed.tool_calls.is_empty() {
                        let streamed_tcs = self.finalize_tool_calls();
                        if !streamed_tcs.is_empty() {
                            parsed.tool_calls = streamed_tcs;
                        }
                    }
                    if parsed.finish_reason.is_none() {
                        parsed.finish_reason = self.finish_reason;
                    }
                    let mut combined_timings = self.timings;
                    merge_timings(&mut combined_timings, parsed.timings.take());
                    parsed.timings = combined_timings;
                    return Ok(parsed);
                }
            }
        }

        let api_tcs = self.finalize_tool_calls();
        let parsed = parse_model_output(
            &self.raw_content,
            non_empty_opt(&self.api_reasoning),
            api_tcs,
        );
        Ok(InferenceResponse {
            content: parsed.content,
            reasoning: parsed.reasoning,
            tool_calls: parsed.tool_calls,
            model: self.model,
            finish_reason: self.finish_reason,
            usage: self.usage,
            latency_ms,
            timings: self.timings,
        })
    }
}

fn extract_sse_error(val: &Value, current_event: Option<&str>) -> Option<String> {
    let is_error_event = current_event == Some("error")
        || val.get("type").and_then(|v| v.as_str()) == Some("error");
    if let Some(err) = val
        .get("error")
        .filter(|e| !matches!(e, Value::Null | Value::Bool(false)))
    {
        if let Some(s) = err.as_str() {
            return Some(s.to_string());
        }
        if let Some(msg) = err.get("message").and_then(|v| v.as_str()) {
            return Some(msg.to_string());
        }
        return Some(err.to_string());
    }
    if is_error_event {
        let msg = val
            .get("message")
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .unwrap_or_else(|| val.to_string());
        return Some(msg);
    }
    None
}

fn push_unique_reasoning(parts: &mut Vec<String>, txt: &str) {
    if !txt.is_empty() && !parts.iter().any(|existing| existing == txt) {
        parts.push(txt.to_string());
    }
}

fn extract_responses_reasoning_parts(item: &Value) -> Vec<String> {
    let mut parts: Vec<String> = Vec::new();
    for field in ["summary", "content"] {
        if let Some(val) = item.get(field) {
            if let Some(arr) = val.as_array() {
                for part in arr {
                    if let Some(txt) = part
                        .as_str()
                        .or_else(|| part.get("text").and_then(|t| t.as_str()))
                    {
                        push_unique_reasoning(&mut parts, txt);
                    }
                }
            } else if let Some(txt) = val.as_str() {
                push_unique_reasoning(&mut parts, txt);
            }
        }
    }
    if parts.is_empty() {
        if let Some(txt) = item.get("text").and_then(|t| t.as_str()) {
            push_unique_reasoning(&mut parts, txt);
        }
    }
    parts
}

fn non_empty_opt(s: &str) -> Option<String> {
    let t = s.trim();
    if t.is_empty() {
        None
    } else {
        Some(t.to_string())
    }
}

async fn emit_stream_piece(piece: StreamPiece, tx: &mpsc::Sender<InferenceStreamChunk>) {
    let chunk = match piece {
        StreamPiece::Text(t) => InferenceStreamChunk::TextDelta(t),
        StreamPiece::Reasoning(r) => InferenceStreamChunk::ReasoningDelta(r),
    };
    let _ = tx.send(chunk).await;
}

async fn process_sse_line(
    line: &str,
    current_event: &mut Option<String>,
    acc: &mut SseAccumulator,
    splitter: &mut ThinkStreamSplitter,
    tx: &mpsc::Sender<InferenceStreamChunk>,
) -> Result<()> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        *current_event = None;
        return Ok(());
    }
    if trimmed.starts_with(':') {
        return Ok(());
    }
    if let Some(ev) = trimmed.strip_prefix("event:") {
        *current_event = Some(ev.trim().to_string());
        return Ok(());
    }
    let Some(data) = trimmed.strip_prefix("data:") else {
        return Ok(());
    };
    let payload_str = data.trim();
    if payload_str == "[DONE]" {
        acc.saw_done = true;
        acc.finalize_loop_guard_eof();
        return Ok(());
    }

    let val: Value = serde_json::from_str(payload_str)
        .map_err(|e| AgentError::Inference(format!("malformed SSE JSON chunk: {e}")))?;

    if let Some(err_msg) = extract_sse_error(&val, current_event.as_deref()) {
        return Err(AgentError::Inference(format!("PS5LM SSE error: {err_msg}")));
    }

    if val.get("timings").is_some() {
        merge_timings(&mut acc.timings, parse_timings(val.get("timings")));
    }

    match acc.mode {
        ApiMode::ChatCompletions => {
            if let Some(m) = val.get("model").and_then(|v| v.as_str()) {
                acc.model = m.to_string();
            }
            if val.get("usage").is_some() {
                acc.usage = parse_usage(val.get("usage"));
            }
            if let Some(choice) = val
                .get("choices")
                .and_then(|c| c.as_array())
                .and_then(|a| a.first())
            {
                if let Some(fr) = choice.get("finish_reason").and_then(|v| v.as_str()) {
                    acc.finish_reason = Some(fr.to_string());
                }
                if let Some(delta) = choice.get("delta") {
                    if let Some(r) = delta
                        .get("reasoning_content")
                        .or_else(|| delta.get("reasoning"))
                        .and_then(|v| v.as_str())
                    {
                        acc.api_reasoning.push_str(r);
                        let _ = tx.send(InferenceStreamChunk::ReasoningDelta(r.to_string())).await;
                    }
                    if let Some(c) = delta.get("content").and_then(|v| v.as_str()) {
                        acc.push_content_delta(c, splitter, tx).await;
                    }
                    if let Some(tcs) = delta.get("tool_calls").and_then(|v| v.as_array()) {
                        for tc in tcs {
                            let idx = tc.get("index").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
                            let entry = acc.tool_calls.entry(idx).or_default();
                            if let Some(id) = tc.get("id").and_then(|v| v.as_str()) {
                                if !id.is_empty() {
                                    entry.id = id.to_string();
                                }
                            }
                            if let Some(func) = tc.get("function") {
                                if let Some(name) = func.get("name").and_then(|v| v.as_str()) {
                                    entry.name.push_str(name);
                                }
                                if let Some(args) = func.get("arguments") {
                                    if let Some(s) = args.as_str() {
                                        entry.arguments_buf.push_str(s);
                                    } else if args.is_object() {
                                        entry.arguments_val = Some(args.clone());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        ApiMode::Responses => {
            let ev_type = val
                .get("type")
                .and_then(|v| v.as_str())
                .or(current_event.as_deref())
                .unwrap_or("");
            match ev_type {
                "response.output_text.delta" => {
                    if let Some(delta) = val.get("delta").and_then(|v| v.as_str()) {
                        acc.push_content_delta(delta, splitter, tx).await;
                    }
                }
                "response.reasoning.delta" | "response.reasoning_summary_text.delta" => {
                    if let Some(delta) = val.get("delta").and_then(|v| v.as_str()) {
                        acc.api_reasoning.push_str(delta);
                        let _ = tx.send(InferenceStreamChunk::ReasoningDelta(delta.to_string())).await;
                    }
                }
                "response.output_item.added" => {
                    if let Some(item) = val.get("item") {
                        if item.get("type").and_then(|v| v.as_str()) == Some("function_call") {
                            let idx = val
                                .get("output_index")
                                .and_then(|v| v.as_u64())
                                .map(|n| n as usize)
                                .unwrap_or_else(|| acc.tool_calls.len());
                            if let Some(item_id) =
                                item.get("id").and_then(|v| v.as_str()).filter(|s| !s.is_empty())
                            {
                                acc.response_item_to_index.insert(item_id.to_string(), idx);
                            }
                            let entry = acc.tool_calls.entry(idx).or_default();
                            if let Some(call_id) = item
                                .get("call_id")
                                .or_else(|| item.get("id"))
                                .and_then(|v| v.as_str())
                                .filter(|s| !s.is_empty())
                            {
                                entry.id = call_id.to_string();
                            }
                            if let Some(name) =
                                item.get("name").and_then(|v| v.as_str()).filter(|s| !s.is_empty())
                            {
                                entry.name = name.to_string();
                            }
                            if let Some(args) = item.get("arguments") {
                                if args.is_object() {
                                    entry.arguments_val = Some(args.clone());
                                } else if let Some(s) = args.as_str().filter(|s| !s.is_empty()) {
                                    if entry.arguments_buf.is_empty() {
                                        entry.arguments_buf.push_str(s);
                                    }
                                }
                            }
                        }
                    }
                }
                "response.function_call_arguments.delta" => {
                    let idx = acc.resolve_response_tc_index(&val);
                    let entry = acc.tool_calls.entry(idx).or_default();
                    if let Some(delta) = val.get("delta") {
                        if let Some(s) = delta.as_str() {
                            entry.arguments_buf.push_str(s);
                        } else if delta.is_object() {
                            entry.arguments_val = Some(delta.clone());
                        }
                    }
                }
                "response.function_call_arguments.done" => {
                    let idx = acc.resolve_response_tc_index(&val);
                    let entry = acc.tool_calls.entry(idx).or_default();
                    if let Some(name) =
                        val.get("name").and_then(|v| v.as_str()).filter(|s| !s.is_empty())
                    {
                        if entry.name.is_empty() {
                            entry.name = name.to_string();
                        }
                    }
                    if let Some(args) = val.get("arguments") {
                        if args.is_object() {
                            entry.arguments_val = Some(args.clone());
                        } else if let Some(s) = args.as_str() {
                            entry.arguments_val = None;
                            entry.arguments_buf = s.to_string();
                        }
                    }
                }
                "response.output_item.done" => {
                    if let Some(item) = val.get("item") {
                        match item.get("type").and_then(|v| v.as_str()).unwrap_or("") {
                            "function_call" => {
                                let idx = item
                                    .get("id")
                                    .and_then(|v| v.as_str())
                                    .and_then(|id| acc.response_item_to_index.get(id).copied())
                                    .or_else(|| {
                                        val.get("output_index")
                                            .and_then(|v| v.as_u64())
                                            .map(|n| n as usize)
                                    })
                                    .unwrap_or_else(|| acc.tool_calls.len());
                                if let Some(item_id) =
                                    item.get("id").and_then(|v| v.as_str()).filter(|s| !s.is_empty())
                                {
                                    acc.response_item_to_index.insert(item_id.to_string(), idx);
                                }
                                let entry = acc.tool_calls.entry(idx).or_default();
                                if let Some(call_id) = item
                                    .get("call_id")
                                    .or_else(|| item.get("id"))
                                    .and_then(|v| v.as_str())
                                    .filter(|s| !s.is_empty())
                                {
                                    entry.id = call_id.to_string();
                                }
                                if let Some(name) =
                                    item.get("name").and_then(|v| v.as_str()).filter(|s| !s.is_empty())
                                {
                                    entry.name = name.to_string();
                                }
                                if let Some(args) = item.get("arguments") {
                                    if args.is_object() {
                                        entry.arguments_val = Some(args.clone());
                                    } else if let Some(s) =
                                        args.as_str().filter(|s| !s.trim().is_empty())
                                    {
                                        entry.arguments_val = None;
                                        entry.arguments_buf = s.to_string();
                                    }
                                }
                            }
                            "reasoning" if acc.api_reasoning.is_empty() => {
                                for txt in extract_responses_reasoning_parts(item) {
                                    acc.api_reasoning.push_str(&txt);
                                }
                            }
                            _ => {}
                        }
                    }
                }
                "response.completed" | "response.incomplete" => {
                    acc.saw_done = true;
                    acc.finalize_loop_guard_eof();
                    let default_status = if ev_type == "response.incomplete" {
                        "incomplete"
                    } else {
                        "completed"
                    };
                    let status_fr = val
                        .get("response")
                        .and_then(|r| r.get("status"))
                        .and_then(|v| v.as_str())
                        .unwrap_or(default_status)
                        .to_string();
                    acc.finish_reason = Some(status_fr);
                    if let Some(resp) = val.get("response") {
                        acc.final_response_override = Some(resp.clone());
                        if resp.get("usage").is_some() {
                            acc.usage = parse_usage(resp.get("usage"));
                        }
                        if resp.get("timings").is_some() {
                            merge_timings(&mut acc.timings, parse_timings(resp.get("timings")));
                        }
                    }
                }
                "response.failed" => {
                    let resp = val.get("response").unwrap_or(&val);
                    let err_msg = resp
                        .get("error")
                        .and_then(|e| {
                            e.get("message")
                                .and_then(|m| m.as_str())
                                .map(str::to_string)
                                .or_else(|| e.as_str().map(str::to_string))
                                .or_else(|| Some(e.to_string()))
                        })
                        .unwrap_or_else(|| "response.failed".to_string());
                    return Err(AgentError::Inference(format!(
                        "PS5LM Responses stream failed: {err_msg}"
                    )));
                }
                _ => {}
            }
        }
        ApiMode::AnthropicMessages => {
            let ev_type = val
                .get("type")
                .and_then(|v| v.as_str())
                .or(current_event.as_deref())
                .unwrap_or("");
            match ev_type {
                "message_start" => {
                    if let Some(msg) = val.get("message") {
                        if let Some(m) = msg.get("model").and_then(|v| v.as_str()) {
                            acc.model = m.to_string();
                        }
                        if let Some(u) = msg.get("usage") {
                            let u_parsed = parse_usage(Some(u));
                            acc.usage.prompt_tokens = u_parsed.prompt_tokens;
                            acc.usage.total_tokens =
                                acc.usage.prompt_tokens + acc.usage.completion_tokens;
                        }
                    }
                }
                "content_block_start" => {
                    let idx = val.get("index").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
                    if let Some(cb) = val.get("content_block") {
                        if cb.get("type").and_then(|v| v.as_str()) == Some("tool_use") {
                            let entry = acc.tool_calls.entry(idx).or_default();
                            if let Some(id) = cb.get("id").and_then(|v| v.as_str()) {
                                entry.id = id.to_string();
                            }
                            if let Some(name) = cb.get("name").and_then(|v| v.as_str()) {
                                entry.name = name.to_string();
                            }
                            if let Some(inp) = cb.get("input") {
                                if !inp.is_null() && inp.as_object().is_some_and(|o| !o.is_empty()) {
                                    entry.arguments_val = Some(inp.clone());
                                }
                            }
                        }
                    }
                }
                "content_block_delta" => {
                    let idx = val.get("index").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
                    if let Some(delta) = val.get("delta") {
                        match delta.get("type").and_then(|v| v.as_str()).unwrap_or("") {
                            "text_delta" => {
                                if let Some(txt) = delta.get("text").and_then(|v| v.as_str()) {
                                    acc.push_content_delta(txt, splitter, tx).await;
                                }
                            }
                            "thinking_delta" => {
                                if let Some(txt) = delta.get("thinking").and_then(|v| v.as_str()) {
                                    acc.api_reasoning.push_str(txt);
                                    let _ = tx
                                        .send(InferenceStreamChunk::ReasoningDelta(txt.to_string()))
                                        .await;
                                }
                            }
                            "input_json_delta" => {
                                if let Some(pj) = delta.get("partial_json").and_then(|v| v.as_str()) {
                                    let entry = acc.tool_calls.entry(idx).or_default();
                                    entry.arguments_buf.push_str(pj);
                                }
                            }
                            _ => {}
                        }
                    }
                }
                "message_delta" => {
                    if let Some(sr) = val
                        .get("delta")
                        .and_then(|d| d.get("stop_reason"))
                        .and_then(|v| v.as_str())
                    {
                        acc.finish_reason = Some(sr.to_string());
                    }
                    if let Some(u) = val.get("usage") {
                        let out_tok = u
                            .get("output_tokens")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0) as u32;
                        acc.usage.completion_tokens = out_tok;
                        acc.usage.total_tokens =
                            acc.usage.prompt_tokens + acc.usage.completion_tokens;
                    }
                }
                "message_stop" => {
                    acc.saw_done = true;
                    acc.finalize_loop_guard_eof();
                }
                _ => {}
            }
        }
        ApiMode::Completions => {
            if let Some(m) = val.get("model").and_then(|v| v.as_str()) {
                acc.model = m.to_string();
            }
            if val.get("usage").is_some() {
                acc.usage = parse_usage(val.get("usage"));
            }
            if let Some(choice) = val
                .get("choices")
                .and_then(|c| c.as_array())
                .and_then(|a| a.first())
            {
                if let Some(fr) = choice.get("finish_reason").and_then(|v| v.as_str()) {
                    acc.finish_reason = Some(fr.to_string());
                }
                if let Some(txt) = choice.get("text").and_then(|v| v.as_str()) {
                    acc.push_content_delta(txt, splitter, tx).await;
                }
            }
        }
        ApiMode::SystemOne => {
            // /v1/systemone is non-streaming and handled directly in execute_streaming.
        }
    }

    Ok(())
}

fn map_reqwest_body_err(context: &str, err: reqwest::Error) -> AgentError {
    if err.is_timeout() {
        AgentError::Timeout(format!("{context}: {err}"))
    } else {
        AgentError::Inference(format!("{context}: {err}"))
    }
}

fn truncate_for_err(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        let mut end = max;
        while end > 0 && !s.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}...", &s[..end])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req_with(
        mode: ApiMode,
        thinking_level: ThinkingLevel,
        max_output_tokens: u32,
        image_urls: Vec<String>,
    ) -> InferenceRequest {
        InferenceRequest {
            model: None,
            api_mode: mode,
            messages: vec![
                InferenceMessage {
                    role: "system".into(),
                    content: "Sys".into(),
                    ..Default::default()
                },
                InferenceMessage {
                    role: "user".into(),
                    content: "Hello".into(),
                    image_urls,
                    ..Default::default()
                },
            ],
            tools: Vec::new(),
            temperature: 0.3,
            max_output_tokens,
            context_limit: 8192,
            stream: false,
            thinking_level,
            slot_id: None,
        }
    }

    #[test]
    fn thinking_level_and_zero_max_tokens_chat_completions() {
        let cfg = InferenceConfig::default();
        let off = build_request_payload(
            &req_with(ApiMode::ChatCompletions, ThinkingLevel::Off, 0, vec![]),
            &cfg,
            "qwen3",
            false,
        );
        assert!(off.get("max_tokens").is_none());
        assert_eq!(off["chat_template_kwargs"]["enable_thinking"], false);
        assert_eq!(off["reasoning_budget"], 0);
        assert_eq!(off["reasoning_format"], "none");
        assert!(off.get("reasoning_effort").is_none());
        assert_eq!(off["messages"][1]["content"], "Hello");

        let low = build_request_payload(
            &req_with(ApiMode::ChatCompletions, ThinkingLevel::Low, 256, vec![]),
            &cfg,
            "qwen3",
            false,
        );
        assert_eq!(low["chat_template_kwargs"]["enable_thinking"], true);
        assert_eq!(low["reasoning_effort"], "low");
        assert_eq!(low["reasoning_budget"], 512);
        assert_eq!(low["reasoning_format"], "deepseek");

        let med = build_request_payload(
            &req_with(ApiMode::ChatCompletions, ThinkingLevel::Medium, 256, vec![]),
            &cfg,
            "qwen3",
            false,
        );
        assert_eq!(med["reasoning_effort"], "medium");
        assert_eq!(med["reasoning_budget"], 2048);

        let high = build_request_payload(
            &req_with(ApiMode::ChatCompletions, ThinkingLevel::High, 256, vec![]),
            &cfg,
            "qwen3",
            false,
        );
        assert_eq!(high["reasoning_effort"], "high");
        assert_eq!(high["reasoning_budget"], 8192);

        let max = build_request_payload(
            &req_with(ApiMode::ChatCompletions, ThinkingLevel::Max, 512, vec![]),
            &cfg,
            "qwen3",
            false,
        );
        assert_eq!(max["max_tokens"], 512);
        assert_eq!(max["reasoning_effort"], "high");
        assert_eq!(max["chat_template_kwargs"]["enable_thinking"], true);
        assert_eq!(max["reasoning_budget"], -1);
        assert_eq!(max["reasoning_format"], "deepseek");
    }

    #[test]
    fn llamacpp_sampling_fields_and_custom_extra_body() {
        let mut cfg = InferenceConfig::default();
        cfg.dynatemp_range = 0.25;
        cfg.dynatemp_exponent = 1.1;
        cfg.typical_p = 0.9;
        cfg.xtc_probability = 0.1;
        cfg.xtc_threshold = 0.2;
        cfg.top_n_sigma = 1.5;
        cfg.dry_multiplier = 0.8;
        cfg.mirostat = 2;
        cfg.seed = 42;
        cfg.ignore_eos = true;
        cfg.n_keep = 64;
        cfg.id_slot = 1;
        cfg.samplers = "top_k, top_p, min_p".into();
        cfg.grammar = "root ::= \"ok\"".into();
        cfg.timings_per_token = true;
        cfg.stop_sequences = vec!["END".into()];
        cfg.custom_extra_body_json =
            r#"{"messages":["hijack"],"stream":true,"custom_flag":123}"#.into();

        let payload = build_request_payload(
            &req_with(ApiMode::ChatCompletions, ThinkingLevel::Medium, 128, vec![]),
            &cfg,
            "qwen3",
            false,
        );
        assert_eq!(payload["cache_prompt"], true);
        assert_eq!(payload["top_k"], 40);
        assert!((payload["top_p"].as_f64().unwrap() - 0.95).abs() < 1e-5);
        assert!((payload["min_p"].as_f64().unwrap() - 0.05).abs() < 1e-5);
        assert_eq!(payload["repeat_last_n"], 256);
        assert_eq!(payload["mirostat"], 2);
        assert_eq!(payload["seed"], 42);
        assert_eq!(payload["ignore_eos"], true);
        assert_eq!(payload["n_keep"], 64);
        assert_eq!(payload["id_slot"], 1);
        assert_eq!(payload["samplers"], json!(["top_k", "top_p", "min_p"]));
        assert_eq!(payload["grammar"], "root ::= \"ok\"");
        assert_eq!(payload["timings_per_token"], true);
        assert_eq!(payload["stop"], json!(["END"]));
        assert_eq!(payload["custom_flag"], 123);
        assert_eq!(payload["stream"], false);
        assert!(payload["messages"].as_array().unwrap().len() == 2);
    }

    #[test]
    fn thinking_level_and_zero_max_tokens_responses_and_anthropic() {
        let cfg = InferenceConfig::default();
        let resp_off = build_request_payload(
            &req_with(ApiMode::Responses, ThinkingLevel::Off, 0, vec![]),
            &cfg,
            "m",
            false,
        );
        assert!(resp_off.get("max_output_tokens").is_none());
        assert!(resp_off.get("reasoning").is_none());

        let resp_high = build_request_payload(
            &req_with(ApiMode::Responses, ThinkingLevel::High, 256, vec![]),
            &cfg,
            "m",
            false,
        );
        assert_eq!(resp_high["max_output_tokens"], 256);
        assert_eq!(resp_high["reasoning"]["effort"], "high");

        let ant_zero = build_request_payload(
            &req_with(ApiMode::AnthropicMessages, ThinkingLevel::Max, 0, vec![]),
            &cfg,
            "claude",
            false,
        );
        assert_eq!(ant_zero["max_tokens"], ANTHROPIC_DEFAULT_MAX_TOKENS);
        assert_eq!(ant_zero["thinking"]["type"], "enabled");
        assert_eq!(ant_zero["thinking"]["budget_tokens"], 8191);

        let ant_small = build_request_payload(
            &req_with(ApiMode::AnthropicMessages, ThinkingLevel::High, 500, vec![]),
            &cfg,
            "claude",
            false,
        );
        assert_eq!(ant_small["max_tokens"], 500);
        assert_eq!(ant_small["thinking"]["budget_tokens"], 499);
    }

    #[test]
    fn multimodal_image_urls_payloads_across_modes() {
        let cfg = InferenceConfig::default();
        let imgs = vec![
            "data:image/png;base64,AAAA".to_string(),
            "https://example.com/pic.jpg".to_string(),
        ];

        let chat = build_request_payload(
            &req_with(
                ApiMode::ChatCompletions,
                ThinkingLevel::Medium,
                128,
                imgs.clone(),
            ),
            &cfg,
            "vl",
            false,
        );
        let chat_parts = chat["messages"][1]["content"].as_array().unwrap();
        assert_eq!(chat_parts[0], json!({"type": "text", "text": "Hello"}));
        assert_eq!(
            chat_parts[1],
            json!({"type": "image_url", "image_url": {"url": "data:image/png;base64,AAAA"}})
        );

        let resp = build_request_payload(
            &req_with(ApiMode::Responses, ThinkingLevel::Medium, 128, imgs.clone()),
            &cfg,
            "vl",
            false,
        );
        let resp_parts = resp["input"][0]["content"].as_array().unwrap();
        assert_eq!(resp_parts[0], json!({"type": "input_text", "text": "Hello"}));
        assert_eq!(
            resp_parts[2],
            json!({"type": "input_image", "image_url": "https://example.com/pic.jpg"})
        );

        let ant = build_request_payload(
            &req_with(
                ApiMode::AnthropicMessages,
                ThinkingLevel::Off,
                128,
                imgs,
            ),
            &cfg,
            "vl",
            false,
        );
        let ant_parts = ant["messages"][0]["content"].as_array().unwrap();
        assert_eq!(ant_parts[0], json!({"type": "text", "text": "Hello"}));
        assert_eq!(
            ant_parts[1],
            json!({"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "AAAA"}})
        );
        assert_eq!(
            ant_parts[2],
            json!({"type": "image", "source": {"type": "url", "url": "https://example.com/pic.jpg"}})
        );
    }

    #[test]
    fn props_slots_and_sse_line_limit_helpers() {
        let mut model = None;
        let mut n_ctx = None;
        let mut slots = None;
        let mut vision = false;
        let mut thinking = false;
        apply_props_json(
            &json!({
                "default_generation_settings": {"n_ctx": 32768},
                "total_slots": 4,
                "modalities": {"vision": true},
                "chat_template_caps": {"supports_thinking": true}
            }),
            &mut model,
            &mut n_ctx,
            &mut slots,
            &mut vision,
            &mut thinking,
        );
        assert_eq!(n_ctx, Some(32768));
        assert_eq!(slots, Some(4));
        assert!(vision);
        assert!(thinking);

        assert!(check_sse_line_limit(0, "data: ok\n").is_ok());
        assert!(check_sse_line_limit(MAX_SSE_LINE_BYTES, "x").is_err());
    }

    #[test]
    fn zero_concurrency_maps_to_unlimited_permits_and_applies_context() {
        let provider = Ps5LmProvider::with_concurrency(InferenceConfig::default(), 0).unwrap();
        assert_eq!(provider.max_concurrent(), UNLIMITED_CONCURRENCY_PERMITS);

        provider.set_max_concurrent(2);
        assert_eq!(provider.max_concurrent(), 2);
        provider.set_max_concurrent(0);
        assert_eq!(provider.max_concurrent(), UNLIMITED_CONCURRENCY_PERMITS);

        provider.set_default_model("qwen3-vl-8b").unwrap();
        assert_eq!(provider.config().model, "qwen3-vl-8b");

        let caps = Ps5LmCapabilities {
            loaded_model: Some("deepseek-r1".into()),
            context_length: Some(65536),
            ..Default::default()
        };
        provider.apply_discovered_context(&caps).unwrap();
        assert_eq!(provider.config().model, "deepseek-r1");
        assert_eq!(provider.config().context_limit, 65536);
    }

    #[tokio::test]
    async fn prefill_kv_cache_is_best_effort_on_unreachable_endpoint() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let mut cfg = InferenceConfig::default();
        cfg.base_url = format!("http://{addr}/v1");
        cfg.connect_timeout_secs = 1;
        let provider = Ps5LmProvider::new(cfg).unwrap();

        let msgs = vec![InferenceMessage {
            role: "user".into(),
            content: "hello".into(),
            ..Default::default()
        }];
        let res = provider
            .prefill_kv_cache(msgs, None, CancellationToken::new())
            .await;
        assert!(res.is_ok());
    }

    #[test]
    fn outbound_payloads_coalesce_consecutive_assistants_for_chat_and_responses() {
        let cfg = InferenceConfig::default();
        let req = InferenceRequest {
            model: None,
            api_mode: ApiMode::ChatCompletions,
            messages: vec![
                InferenceMessage {
                    role: "system".into(),
                    content: "Sys".into(),
                    ..Default::default()
                },
                InferenceMessage {
                    role: "assistant".into(),
                    content: "stale".into(),
                    ..Default::default()
                },
                InferenceMessage {
                    role: "user".into(),
                    content: "u1".into(),
                    ..Default::default()
                },
                InferenceMessage {
                    role: "user".into(),
                    content: "u2".into(),
                    ..Default::default()
                },
                InferenceMessage {
                    role: "assistant".into(),
                    content: "a1".into(),
                    ..Default::default()
                },
                InferenceMessage {
                    role: "assistant".into(),
                    content: "a2".into(),
                    ..Default::default()
                },
            ],
            tools: Vec::new(),
            temperature: 0.3,
            max_output_tokens: 0,
            context_limit: 8192,
            stream: false,
            thinking_level: ThinkingLevel::Off,
            slot_id: None,
        };

        let chat = build_request_payload(&req, &cfg, "qwen3", false);
        let chat_msgs = chat["messages"].as_array().unwrap();
        assert_eq!(chat_msgs.len(), 3);
        assert_eq!(chat_msgs[0]["role"], "system");
        assert_eq!(chat_msgs[1]["role"], "user");
        assert_eq!(chat_msgs[1]["content"], "u1\n\nu2");
        assert_eq!(chat_msgs[2]["role"], "assistant");
        assert_eq!(chat_msgs[2]["content"], "a1\n\na2");

        let mut resp_req = req;
        resp_req.api_mode = ApiMode::Responses;
        let resp = build_request_payload(&resp_req, &cfg, "qwen3", false);
        assert_eq!(resp["instructions"], "Sys");
        let resp_input = resp["input"].as_array().unwrap();
        assert_eq!(resp_input.len(), 2);
        assert_eq!(resp_input[0]["role"], "user");
        assert_eq!(resp_input[0]["content"], "u1\n\nu2");
        assert_eq!(resp_input[1]["role"], "assistant");
        assert_eq!(resp_input[1]["content"], "a1\n\na2");
    }

    #[tokio::test]
    async fn prefill_kv_cache_normalizes_trailing_assistants_and_skips_when_semaphore_busy() {
        let mock = mock_ps5lm::start_ephemeral().await.unwrap();
        let mut cfg = InferenceConfig::default();
        cfg.base_url = mock.base_url();
        cfg.prefill_kv_cache = true;
        let provider = Ps5LmProvider::with_concurrency(cfg, 1).unwrap();

        let msgs = vec![
            InferenceMessage {
                role: "system".into(),
                content: "Sys".into(),
                ..Default::default()
            },
            InferenceMessage {
                role: "user".into(),
                content: "hello".into(),
                ..Default::default()
            },
            InferenceMessage {
                role: "assistant".into(),
                content: "part1".into(),
                ..Default::default()
            },
            InferenceMessage {
                role: "assistant".into(),
                content: "part2".into(),
                ..Default::default()
            },
        ];

        provider
            .prefill_kv_cache(msgs.clone(), None, CancellationToken::new())
            .await
            .unwrap();
        let prefills = mock.recorded_prefill_requests();
        assert_eq!(prefills.len(), 1);
        let sent_msgs = prefills[0].body["messages"].as_array().unwrap();
        assert_eq!(sent_msgs.len(), 3);
        assert_eq!(sent_msgs[0]["role"], "system");
        assert_eq!(sent_msgs[1]["role"], "user");
        assert_eq!(sent_msgs[2]["role"], "assistant");
        assert_eq!(sent_msgs[2]["content"], "part1\n\npart2");
        assert_eq!(prefills[0].body["n_predict"], 0);

        // Hold the sole concurrency permit and verify prefill skips immediately without queueing
        let _busy = provider.gate.try_acquire().unwrap();
        provider
            .prefill_kv_cache(msgs, None, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(mock.recorded_prefill_requests().len(), 1);
    }

    #[test]
    fn anthropic_payload_coalesces_tool_and_user_omits_empty_text_and_guards_trailing_assistant() {
        let cfg = InferenceConfig::default();
        let req = InferenceRequest {
            model: None,
            api_mode: ApiMode::AnthropicMessages,
            messages: vec![
                InferenceMessage {
                    role: "user".into(),
                    content: "".into(),
                    image_urls: vec!["https://example.com/a.png".into()],
                    ..Default::default()
                },
                InferenceMessage {
                    role: "assistant".into(),
                    content: "".into(),
                    tool_calls: Some(vec![ToolCallRequest {
                        id: "tc_1".into(),
                        name: "read".into(),
                        arguments: json!({"path": "a.txt"}),
                    }]),
                    ..Default::default()
                },
                InferenceMessage {
                    role: "tool".into(),
                    content: "file body".into(),
                    tool_call_id: Some("tc_1".into()),
                    ..Default::default()
                },
                InferenceMessage {
                    role: "user".into(),
                    content: "now summarize".into(),
                    ..Default::default()
                },
            ],
            tools: Vec::new(),
            temperature: 0.2,
            max_output_tokens: 4096,
            context_limit: 8192,
            stream: false,
            thinking_level: ThinkingLevel::Medium,
            slot_id: None,
        };
        let payload = build_request_payload(&req, &cfg, "claude", false);
        assert!(payload.get("thinking").is_some());
        let msgs = payload["messages"].as_array().unwrap();
        assert_eq!(msgs.len(), 3);
        // Empty text omitted when image_urls is present
        let first_blocks = msgs[0]["content"].as_array().unwrap();
        assert_eq!(first_blocks.len(), 1);
        assert_eq!(first_blocks[0]["type"], "image");
        // Adjacent tool + user coalesced into single user message
        assert_eq!(msgs[2]["role"], "user");
        let merged_blocks = msgs[2]["content"].as_array().unwrap();
        assert_eq!(merged_blocks.len(), 2);
        assert_eq!(merged_blocks[0]["type"], "tool_result");
        assert_eq!(merged_blocks[1]["type"], "text");
        assert_eq!(merged_blocks[1]["text"], "now summarize");

        // Trailing assistant prefill disables Anthropic thinking block
        let prefill_req = InferenceRequest {
            messages: vec![
                InferenceMessage {
                    role: "user".into(),
                    content: "hello".into(),
                    ..Default::default()
                },
                InferenceMessage {
                    role: "assistant".into(),
                    content: "{".into(),
                    ..Default::default()
                },
            ],
            ..req
        };
        let prefill_payload = build_request_payload(&prefill_req, &cfg, "claude", false);
        assert!(prefill_payload.get("thinking").is_none());
    }

    #[test]
    fn completions_payload_continues_trailing_plain_assistant_without_duplicate_header() {
        let cfg = InferenceConfig::default();
        let req = InferenceRequest {
            model: None,
            api_mode: ApiMode::Completions,
            messages: vec![
                InferenceMessage {
                    role: "system".into(),
                    content: "Sys".into(),
                    ..Default::default()
                },
                InferenceMessage {
                    role: "user".into(),
                    content: "Hi".into(),
                    ..Default::default()
                },
                InferenceMessage {
                    role: "assistant".into(),
                    content: "<think>\n".into(),
                    ..Default::default()
                },
            ],
            tools: Vec::new(),
            temperature: 0.2,
            max_output_tokens: 0,
            context_limit: 8192,
            stream: true,
            thinking_level: ThinkingLevel::Medium,
            slot_id: None,
        };
        let payload = build_request_payload(&req, &cfg, "qwen3", true);
        let prompt = payload["prompt"].as_str().unwrap();
        assert!(prompt.ends_with("<|im_start|>assistant\n<think>\n"));
        assert_eq!(prompt.matches("<|im_start|>assistant").count(), 1);
        assert!(completions_prompt_starts_in_think(
            ApiMode::Completions,
            &payload
        ));
    }

    #[tokio::test]
    async fn responses_sse_and_json_preserve_reasoning_tool_calls_and_status_events() {
        let (tx, _rx) = mpsc::channel(32);
        let mut acc = SseAccumulator::new(ApiMode::Responses, "qwen3".into());
        let mut splitter = ThinkStreamSplitter::new();
        let mut ev = None;

        for line in [
            r#"data: {"type":"response.reasoning.delta","delta":"step 1"}"#,
            r#"data: {"type":"response.output_text.delta","delta":"partial"}"#,
            r#"data: {"type":"response.output_item.added","output_index":1,"item":{"id":"fc_1","type":"function_call","call_id":"call_1","name":"bash"}}"#,
            r#"data: {"type":"response.function_call_arguments.delta","item_id":"fc_1","delta":"{\"cmd\":"}"#,
            r#"data: {"type":"response.function_call_arguments.done","item_id":"fc_1","arguments":"{\"cmd\":\"ls\"}"}"#,
            r#"data: {"type":"response.incomplete","response":{"id":"r1","status":"incomplete","output":[{"type":"message","content":[{"text":"partial"}]}]}}"#,
        ] {
            process_sse_line(line, &mut ev, &mut acc, &mut splitter, &tx)
                .await
                .unwrap();
        }
        assert!(acc.saw_done);
        let resp = acc.into_response(5).unwrap();
        assert_eq!(resp.content, "partial");
        assert_eq!(resp.reasoning.as_deref(), Some("step 1"));
        assert_eq!(resp.finish_reason.as_deref(), Some("incomplete"));
        assert_eq!(resp.tool_calls.len(), 1);
        assert_eq!(resp.tool_calls[0].id, "call_1");
        assert_eq!(resp.tool_calls[0].arguments, json!({"cmd": "ls"}));

        let mut acc_fail = SseAccumulator::new(ApiMode::Responses, "qwen3".into());
        let mut splitter_fail = ThinkStreamSplitter::new();
        let err = process_sse_line(
            r#"data: {"type":"response.failed","response":{"error":{"message":"boom"}}}"#,
            &mut None,
            &mut acc_fail,
            &mut splitter_fail,
            &tx,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, AgentError::Inference(_)));

        let parsed_resp = parse_responses_json(
            "gpt",
            &json!({
                "status": "completed",
                "output": [
                    {"type": "reasoning", "summary": [{"type": "summary_text", "text": "summary step"}]},
                    {"type": "message", "content": [{"text": "done"}]}
                ]
            }),
            5,
        )
        .unwrap();
        assert_eq!(parsed_resp.reasoning.as_deref(), Some("summary step"));

        let parsed_anth = parse_anthropic_json(
            "claude",
            &json!({
                "content": [{"type": "tool_use", "id": "tu_1", "name": "ping", "input": null}]
            }),
            5,
        )
        .unwrap();
        assert_eq!(parsed_anth.tool_calls[0].arguments, json!({}));
    }

    #[tokio::test]
    async fn chat_completions_sse_handles_empty_id_object_args_and_error_payloads() {
        let (tx, _rx) = mpsc::channel(32);
        let mut acc = SseAccumulator::new(ApiMode::ChatCompletions, "qwen3".into());
        let mut splitter = ThinkStreamSplitter::new();
        let mut ev = None;

        for line in [
            r#"data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_keep","function":{"name":"read","arguments":"{\"p\":"}}]}}]}"#,
            r#"data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"","function":{"arguments":"\"a\"}"}}]}}]}"#,
            r#"data: {"choices":[{"delta":{"tool_calls":[{"index":1,"id":"call_obj","function":{"name":"ls","arguments":{"dir":"."}}}]},"finish_reason":"tool_calls"}]}"#,
            "data: [DONE]",
        ] {
            process_sse_line(line, &mut ev, &mut acc, &mut splitter, &tx)
                .await
                .unwrap();
        }
        let resp = acc.into_response(1).unwrap();
        assert_eq!(resp.tool_calls.len(), 2);
        assert_eq!(resp.tool_calls[0].id, "call_keep");
        assert_eq!(resp.tool_calls[0].arguments, json!({"p": "a"}));
        assert_eq!(resp.tool_calls[1].id, "call_obj");
        assert_eq!(resp.tool_calls[1].arguments, json!({"dir": "."}));

        let mut acc_err = SseAccumulator::new(ApiMode::ChatCompletions, "qwen3".into());
        let mut splitter_err = ThinkStreamSplitter::new();
        let err = process_sse_line(
            r#"data: {"error":{"message":"context overflow"}}"#,
            &mut None,
            &mut acc_err,
            &mut splitter_err,
            &tx,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, AgentError::Inference(_)));
    }

    #[test]
    fn discovery_ignores_n_ctx_train_handles_zero_slots_and_uses_token_boundaries_for_hints() {
        assert_eq!(
            extract_models_context_length(&json!({"data": [{"id": "m", "meta": {"n_ctx_train": 262144}}]})),
            None
        );
        assert_eq!(
            extract_models_context_length(&json!({"data": [{"id": "m", "meta": {"n_ctx": 32768, "n_ctx_train": 262144}}]})),
            Some(32768)
        );

        let mut model = None;
        let mut n_ctx = None;
        let mut slots = None;
        let mut vision = false;
        let mut thinking = false;
        apply_props_json(
            &json!({"total_slots": 0, "chat_template": "<seed:think>"}),
            &mut model,
            &mut n_ctx,
            &mut slots,
            &mut vision,
            &mut thinking,
        );
        assert_eq!(slots, None);
        assert!(thinking);
        apply_slots_json(&json!([{"n_ctx": 16384}, {"n_ctx": 16384}]), &mut n_ctx, &mut slots);
        assert_eq!(slots, Some(2));
        assert_eq!(n_ctx, Some(16384));

        assert_eq!(infer_model_hints("coder1-eval"), (false, false));
        assert_eq!(infer_model_hints("qwen3-vl-8b"), (true, true));
        assert_eq!(infer_model_hints("qwq-32b"), (false, true));
        assert_eq!(infer_model_hints("seed-oss-36b"), (false, true));
        assert_eq!(infer_model_hints("gpt-oss-20b"), (false, true));

        assert_eq!(
            parse_health_body(reqwest::StatusCode::OK, "ok"),
            (true, None, None)
        );
        assert!(
            !parse_health_body(reqwest::StatusCode::OK, r#"{"status":"loading"}"#).0
        );
        assert!(check_sse_line_limit_bytes(0, b"data: \xF0\x9F\x98\x80\n").is_ok());
        assert!(effective_request_timeout(0).is_none());
        let mut cfg = InferenceConfig::default();
        cfg.timeout_secs = 0;
        assert!(build_http_client(&cfg).is_ok());
    }

    #[tokio::test]
    async fn concurrency_gate_resize_down_prevents_oversubscription() {
        let cfg = InferenceConfig::default();
        let provider = Ps5LmProvider::with_concurrency(cfg, 3).unwrap();
        let p1 = provider.gate.try_acquire().unwrap();
        let p2 = provider.gate.try_acquire().unwrap();
        provider.set_max_concurrent(1);
        assert_eq!(provider.max_concurrent(), 1);
        assert!(provider.gate.try_acquire().is_none());
        drop(p1);
        assert!(provider.gate.try_acquire().is_none());
        drop(p2);
        assert!(provider.gate.try_acquire().is_some());
    }

    #[test]
    fn chat_completions_payload_omits_neutral_penalties_restricts_name_default_stop_slot_id_and_stream_options() {
        let mut cfg = InferenceConfig::default();
        let mut req = req_with(ApiMode::ChatCompletions, ThinkingLevel::Off, 0, vec![]);
        req.messages = vec![
            InferenceMessage {
                role: "system".into(),
                content: "Sys".into(),
                name: Some("sys_name".into()),
                ..Default::default()
            },
            InferenceMessage {
                role: "user".into(),
                content: "Hi".into(),
                name: Some("alice".into()),
                ..Default::default()
            },
            InferenceMessage {
                role: "assistant".into(),
                content: "".into(),
                name: Some("bot".into()),
                tool_calls: Some(vec![ToolCallRequest {
                    id: "call_1".into(),
                    name: "read".into(),
                    arguments: json!({"path": "a.txt"}),
                }]),
                ..Default::default()
            },
            InferenceMessage {
                role: "tool".into(),
                content: "ok".into(),
                tool_call_id: Some("call_1".into()),
                name: Some("read".into()),
                ..Default::default()
            },
            InferenceMessage {
                role: "tool".into(),
                content: "ok2".into(),
                tool_call_id: Some("call_2".into()),
                name: Some("   ".into()),
                ..Default::default()
            },
        ];

        let default_payload = build_request_payload(&req, &cfg, "qwen3", false);
        // InferenceConfig::default() has repeat_penalty=1.05 (non-neutral) and presence/frequency=0.0 (neutral).
        assert!((default_payload["repeat_penalty"].as_f64().unwrap() - 1.05).abs() < 1e-4);
        assert!(default_payload.get("presence_penalty").is_none());
        assert!(default_payload.get("frequency_penalty").is_none());
        assert_eq!(default_payload["repeat_last_n"], 256);
        assert!(default_payload.get("id_slot").is_none());
        assert!(default_payload.get("stream_options").is_none());
        assert_eq!(
            default_payload["stop"],
            json!(["<|im_end|>", "<|endoftext|>"])
        );

        let mut neutral_cfg = cfg.clone();
        neutral_cfg.repeat_penalty = 1.0;
        let neutral_payload = build_request_payload(&req, &neutral_cfg, "qwen3", false);
        assert!(neutral_payload.get("repeat_penalty").is_none());
        assert!(neutral_payload.get("presence_penalty").is_none());
        assert!(neutral_payload.get("frequency_penalty").is_none());
        assert_eq!(neutral_payload["repeat_last_n"], 256);

        let msgs = default_payload["messages"].as_array().unwrap();
        assert!(msgs[0].get("name").is_none());
        assert!(msgs[1].get("name").is_none());
        assert!(msgs[2].get("name").is_none());
        assert_eq!(msgs[3]["name"], "read");
        assert!(msgs[4].get("name").is_none());

        let resp_default = build_request_payload(
            &req_with(ApiMode::Responses, ThinkingLevel::Off, 0, vec![]),
            &cfg,
            "qwen3",
            false,
        );
        assert!(resp_default.get("stop").is_none());

        cfg.repeat_penalty = 1.15;
        cfg.presence_penalty = 0.25;
        cfg.frequency_penalty = -0.2;
        cfg.id_slot = 1;
        cfg.custom_extra_body_json = r#"{"stream_options":{"include_usage":true}}"#.into();

        let non_stream = build_request_payload(&req, &cfg, "qwen3", false);
        assert!((non_stream["repeat_penalty"].as_f64().unwrap() - 1.15).abs() < 1e-5);
        assert!((non_stream["presence_penalty"].as_f64().unwrap() - 0.25).abs() < 1e-5);
        assert!((non_stream["frequency_penalty"].as_f64().unwrap() + 0.2).abs() < 1e-5);
        assert_eq!(non_stream["id_slot"], 1);
        assert!(non_stream.get("stream_options").is_none());

        req.slot_id = Some(3);
        let stream_payload = build_request_payload(&req, &cfg, "qwen3", true);
        assert_eq!(stream_payload["id_slot"], 3);
        assert_eq!(stream_payload["stream_options"], json!({"include_usage": true}));

        req.slot_id = Some(-1);
        let omitted_slot = build_request_payload(&req, &cfg, "qwen3", true);
        assert!(omitted_slot.get("id_slot").is_none());
    }

    #[tokio::test]
    async fn parse_timings_clamps_invalid_values_and_merges_sse_chunks() {
        assert_eq!(parse_timings(None), None);
        assert_eq!(parse_timings(Some(&json!({}))), None);
        assert_eq!(
            parse_timings(Some(&json!({"predicted_per_second": -5.0, "cache_n": -1}))),
            None
        );

        let partial = parse_timings(Some(&json!({
            "predicted_per_second": 122.5,
            "predicted_ms": -10.0,
            "cache_n": 256,
            "prompt_n": -3
        })))
        .expect("partially valid timings should still parse");
        assert_eq!(partial.predicted_per_second, Some(122.5));
        assert_eq!(partial.predicted_ms, None);
        assert_eq!(partial.cache_n, Some(256));
        assert_eq!(partial.prompt_n, None);

        let non_stream = parse_chat_completions_json(
            "qwen3",
            &json!({
                "model": "qwen3",
                "choices": [{"message": {"content": "ok"}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
                "timings": {
                    "predicted_per_second": 119.2,
                    "prompt_per_second": 520.0,
                    "predicted_ms": 42.0,
                    "prompt_ms": 19.2,
                    "predicted_n": 5,
                    "prompt_n": 10,
                    "cache_n": 128
                }
            }),
            50,
        )
        .unwrap();
        let t = non_stream.timings.expect("non-stream timings expected");
        assert_eq!(t.predicted_per_second, Some(119.2));
        assert_eq!(t.prompt_per_second, Some(520.0));
        assert_eq!(t.cache_n, Some(128));

        let (tx, _rx) = mpsc::channel(32);
        let mut acc = SseAccumulator::new(ApiMode::ChatCompletions, "qwen3".into());
        let mut splitter = ThinkStreamSplitter::new();
        let mut ev = None;
        for line in [
            r#"data: {"choices":[{"delta":{"content":"hello "}}],"timings":{"prompt_n":16,"cache_n":512,"prompt_per_second":480.0}}"#,
            r#"data: {"choices":[{"delta":{"content":"world"},"finish_reason":"stop"}]}"#,
            r#"data: {"choices":[],"usage":{"prompt_tokens":16,"completion_tokens":2,"total_tokens":18},"timings":{"predicted_n":2,"predicted_ms":16.0,"predicted_per_second":125.0}}"#,
            "data: [DONE]",
        ] {
            process_sse_line(line, &mut ev, &mut acc, &mut splitter, &tx)
                .await
                .unwrap();
        }
        let resp = acc.into_response(20).unwrap();
        assert_eq!(resp.content, "hello world");
        assert_eq!(resp.usage.completion_tokens, 2);
        let sse_t = resp.timings.expect("SSE timings should merge across chunks");
        assert_eq!(sse_t.cache_n, Some(512));
        assert_eq!(sse_t.prompt_n, Some(16));
        assert_eq!(sse_t.predicted_n, Some(2));
        assert_eq!(sse_t.predicted_per_second, Some(125.0));
    }

    #[tokio::test]
    async fn sse_repetition_guard_aborts_early_without_done_and_preserves_reasoning() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut req_buf = [0u8; 4096];
            let _ = socket.read(&mut req_buf).await;

            let mut sse = String::new();
            sse.push_str(
                "data: {\"choices\":[{\"delta\":{\"content\":\"<think>analyze carefully</think>\\nFirst valid summary line.\\n\"}}]}\n\n",
            );
            for _ in 0..8 {
                sse.push_str(
                    "data: {\"choices\":[{\"delta\":{\"content\":\"Repeating the exact same status sentence over and over again.\\n\"}}]}\n\n",
                );
            }
            // Deliberately close stream without sending `data: [DONE]`.
            let hdr = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                sse.len()
            );
            let _ = socket.write_all(hdr.as_bytes()).await;
            let _ = socket.write_all(sse.as_bytes()).await;
            let _ = socket.shutdown().await;
        });

        let mut cfg = InferenceConfig::default();
        cfg.base_url = format!("http://{addr}");
        cfg.max_retries = 0;
        let provider = Ps5LmProvider::new(cfg).unwrap();

        let (tx, mut rx) = mpsc::channel(64);
        let resp = provider
            .generate_stream(
                req_with(ApiMode::ChatCompletions, ThinkingLevel::Medium, 256, vec![]),
                tx,
                CancellationToken::new(),
            )
            .await
            .unwrap();

        let mut streamed_reasoning = String::new();
        while let Some(chunk) = rx.recv().await {
            if let InferenceStreamChunk::ReasoningDelta(r) = chunk {
                streamed_reasoning.push_str(&r);
            }
        }
        let _ = server.await;

        assert_eq!(resp.finish_reason.as_deref(), Some("repetition_abort"));
        assert_eq!(resp.reasoning.as_deref(), Some("analyze carefully"));
        assert_eq!(streamed_reasoning, "analyze carefully");
        assert!(resp.content.contains("First valid summary line."));
        assert!(
            resp.content.matches("Repeating the exact same status sentence over and over again.").count() <= 2
        );

        let (acc_tx, _acc_rx) = mpsc::channel(32);
        let mut acc = SseAccumulator::new(ApiMode::ChatCompletions, "qwen3".into());
        let mut splitter = ThinkStreamSplitter::new();
        let mut ev = None;
        process_sse_line(
            r#"data: {"choices":[{"delta":{"reasoning_content":"API think step"}}]}"#,
            &mut ev,
            &mut acc,
            &mut splitter,
            &acc_tx,
        )
        .await
        .unwrap();
        for _ in 0..8 {
            process_sse_line(
                r#"data: {"choices":[{"delta":{"content":"Hearing you loud and clear, Commander! Let me fix this!\n"}}]}"#,
                &mut ev,
                &mut acc,
                &mut splitter,
                &acc_tx,
            )
            .await
            .unwrap();
        }
        let api_resp = acc.into_response(10).unwrap();
        assert_eq!(api_resp.reasoning.as_deref(), Some("API think step"));
        assert_eq!(api_resp.finish_reason.as_deref(), Some("repetition_abort"));
    }

    #[tokio::test]
    async fn import_kv_and_sampling_probes_endpoints_normalizes_samplers_and_hides_api_key() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..3 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut req_buf = [0u8; 4096];
                let n = socket.read(&mut req_buf).await.unwrap_or(0);
                let req_str = String::from_utf8_lossy(&req_buf[..n]);
                let first_line = req_str.lines().next().unwrap_or("");
                let body = if first_line.contains(" /props ") {
                    json!({
                        "model_path": "/models/Qwen3-32B-Q8_0.gguf",
                        "total_slots": 2,
                        "default_generation_settings": {
                            "n_ctx": 32768,
                            "params": {
                                "kv_cache_type_k": "q8_0",
                                "kv_cache_type_v": "f16",
                                "flash_attention": true,
                                "n_keep": 128,
                                "temperature": 0.6,
                                "top_k": 30,
                                "top_p": 0.9,
                                "min_p": 0.04,
                                "repeat_penalty": 1.08,
                                "repeat_last_n": 256,
                                "presence_penalty": 0.1,
                                "frequency_penalty": 0.05,
                                "dry_multiplier": 0.8,
                                "dry_base": 1.75,
                                "dry_allowed_length": 2,
                                "dry_penalty_last_n": 4096,
                                "xtc_probability": 0.1,
                                "xtc_threshold": 0.2,
                                "samplers": "edkypmxt"
                            }
                        }
                    })
                    .to_string()
                } else if first_line.contains(" /slots ") {
                    json!([{"id": 0, "n_ctx": 32768}, {"id": 1, "n_ctx": 32768}]).to_string()
                } else {
                    json!({"status": "ok"}).to_string()
                };
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = socket.write_all(resp.as_bytes()).await;
                let _ = socket.shutdown().await;
            }
        });

        let secret_key = "super-secret-kv-token-999";
        let mut cfg = InferenceConfig::default();
        cfg.base_url = format!("http://{addr}");
        cfg.api_key = Some(secret_key.into());
        let provider = Ps5LmProvider::new(cfg).unwrap();

        let imported = provider.import_kv_and_sampling().await.unwrap();
        let _ = server.await;

        assert_eq!(imported.model.as_deref(), Some("Qwen3-32B-Q8_0.gguf"));
        assert_eq!(imported.n_ctx, Some(32768));
        assert_eq!(imported.total_slots, Some(2));
        assert_eq!(imported.cache_type_k.as_deref(), Some("q8_0"));
        assert_eq!(imported.cache_type_v.as_deref(), Some("f16"));
        assert_eq!(imported.flash_attn, Some(true));
        assert_eq!(imported.n_keep, Some(128));
        assert_eq!(imported.top_k, Some(30));
        assert_eq!(
            imported.samplers.as_deref(),
            Some(
                [
                    "penalties",
                    "dry",
                    "top_k",
                    "typ_p",
                    "top_p",
                    "min_p",
                    "xtc",
                    "temperature",
                ]
                .map(str::to_string)
                .as_slice()
            )
        );
        let serialized = serde_json::to_string(&imported).unwrap();
        assert!(!serialized.contains(secret_key));

        let unreachable_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let closed_addr = unreachable_listener.local_addr().unwrap();
        drop(unreachable_listener);
        let mut bad_cfg = InferenceConfig::default();
        bad_cfg.base_url = format!("http://{closed_addr}");
        let bad_provider = Ps5LmProvider::new(bad_cfg).unwrap();
        let err = bad_provider.import_kv_and_sampling().await.unwrap_err();
        assert!(matches!(err, AgentError::InferenceUnavailable(_)));
    }
}
