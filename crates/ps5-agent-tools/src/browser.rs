use ps5_agent_config::ToolsConfig;
use ps5_agent_core::{new_id, now_iso, AgentError, Result, StreamEvent};
use regex::{Regex, RegexBuilder};
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE, COOKIE, SET_COOKIE};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::sync::{Mutex, MutexGuard, OnceLock, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use url::Url;

const MAX_SESSIONS: usize = 16;
const SESSION_IDLE_TTL: Duration = Duration::from_secs(30 * 60);
const MAX_COOKIES_PER_SESSION: usize = 64;
const MAX_HISTORY_PER_SESSION: usize = 10;
const MAX_STORED_HTML_BYTES: usize = 512 * 1024;
const MAX_PREVIEW_HTML_BYTES: usize = 128 * 1024;
const MAX_SNAPSHOT_ELEMENTS: usize = 128;
const MAX_PAGE_TEXT_CHARS: usize = 16_000;

#[derive(Debug, Clone, Default)]
pub struct BrowserActivityContext {
    pub conversation_id: Option<String>,
    pub task_id: Option<String>,
    pub agent_id: Option<String>,
}

#[derive(Debug, Default)]
pub struct SandboxBrowserManager {
    inner: Mutex<BrowserStore>,
    events_tx: RwLock<Option<broadcast::Sender<StreamEvent>>>,
}

#[derive(Debug, Default)]
struct BrowserStore {
    sessions: HashMap<String, BrowserSession>,
    access_seq: u64,
}

#[derive(Debug, Clone)]
struct BrowserSession {
    id: String,
    current_url: String,
    title: Option<String>,
    status: u16,
    html: String,
    history: Vec<String>,
    cookies: Vec<StoredCookie>,
    elements: Vec<SnapshotElement>,
    forms: Vec<ParsedForm>,
    input_values: HashMap<String, String>,
    cursor_x_pct: f32,
    cursor_y_pct: f32,
    scroll_y: f32,
    last_action: Option<String>,
    last_selector: Option<String>,
    agent_id: Option<String>,
    last_active: Instant,
    last_active_seq: u64,
    last_active_iso: String,
}

#[derive(Debug, Clone)]
struct StoredCookie {
    name: String,
    value: String,
    domain: String,
    path: String,
    secure: bool,
}

#[derive(Debug, Clone)]
struct SnapshotElement {
    ref_id: String,
    tag: String,
    role: String,
    text: String,
    href: Option<String>,
    input_type: Option<String>,
    name: Option<String>,
    value: Option<String>,
    placeholder: Option<String>,
    checked: bool,
    disabled: bool,
    formaction: Option<String>,
    formmethod: Option<String>,
    form_idx: Option<usize>,
    form_ref: Option<String>,
    pub x_pct: f32,
    pub y_pct: f32,
    pub w_pct: f32,
    pub h_pct: f32,
}

#[derive(Debug, Clone)]
struct ParsedForm {
    form_ref: String,
    action: String,
    method: String,
}

struct PageUpdate {
    final_url: String,
    status: u16,
    title: Option<String>,
    html: String,
    elements: Vec<SnapshotElement>,
    forms: Vec<ParsedForm>,
    truncated: bool,
}

struct SubmitPlan {
    target_url: String,
    method: reqwest::Method,
    body: Option<Vec<u8>>,
    content_type: Option<&'static str>,
    cookies: Vec<StoredCookie>,
}

struct BrowserActivityFields {
    session_id: String,
    action: String,
    url: Option<String>,
    title: Option<String>,
    selector: Option<String>,
    x_pct: Option<f32>,
    y_pct: Option<f32>,
    w_pct: Option<f32>,
    h_pct: Option<f32>,
    scroll_y: Option<f32>,
    status: String,
    session_agent_id: Option<String>,
}

impl SandboxBrowserManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn attach_events_tx(&self, tx: broadcast::Sender<StreamEvent>) {
        let mut guard = self.events_tx.write().unwrap_or_else(|p| p.into_inner());
        *guard = Some(tx);
    }

    fn emit_activity(&self, meta: Option<&BrowserActivityContext>, fields: BrowserActivityFields) {
        let tx_opt = {
            let guard = self.events_tx.read().unwrap_or_else(|p| p.into_inner());
            guard.clone()
        };
        let Some(tx) = tx_opt else {
            return;
        };
        let conversation_id = meta.and_then(|m| non_empty_trimmed(m.conversation_id.as_deref()));
        let task_id = meta.and_then(|m| non_empty_trimmed(m.task_id.as_deref()));
        let agent_id = meta
            .and_then(|m| non_empty_trimmed(m.agent_id.as_deref()))
            .or_else(|| non_empty_trimmed(fields.session_agent_id.as_deref()));
        let clean_url = fields
            .url
            .as_deref()
            .map(sanitize_url_for_event)
            .filter(|s| !s.is_empty());
        let ev = StreamEvent::BrowserActivity {
            conversation_id,
            task_id,
            agent_id,
            session_id: fields.session_id,
            action: fields.action,
            url: clean_url,
            title: fields.title,
            selector: fields.selector,
            x_pct: fields.x_pct.map(clamp_pct),
            y_pct: fields.y_pct.map(clamp_pct),
            w_pct: fields.w_pct.map(clamp_pct),
            h_pct: fields.h_pct.map(clamp_pct),
            scroll_y: fields.scroll_y.map(clamp_pct),
            status: fields.status,
            timestamp: now_iso(),
        };
        let _ = tx.send(ev);
    }

    fn lock_store(&self) -> MutexGuard<'_, BrowserStore> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub fn list_sessions(&self) -> Value {
        let mut store = self.lock_store();
        evict_idle_sessions(&mut store);
        let mut items: Vec<&BrowserSession> = store.sessions.values().collect();
        items.sort_by(|a, b| b.last_active_seq.cmp(&a.last_active_seq));
        let sessions_json: Vec<Value> = items
            .into_iter()
            .map(|s| {
                json!({
                    "session_id": s.id,
                    "url": s.current_url,
                    "title": s.title,
                    "status": s.status,
                    "history_len": s.history.len(),
                    "cookie_count": s.cookies.len(),
                    "elements_count": s.elements.len(),
                    "last_active_at": s.last_active_iso,
                    "cursor": cursor_to_json(s),
                })
            })
            .collect();
        json!({
            "ok": true,
            "count": sessions_json.len(),
            "max_sessions": MAX_SESSIONS,
            "sessions": sessions_json,
        })
    }

    pub fn close_session(&self, session_id: &str) -> bool {
        self.close_session_internal(session_id, None)
    }

    fn close_session_internal(
        &self,
        session_id: &str,
        meta: Option<&BrowserActivityContext>,
    ) -> bool {
        let sid = session_id.trim();
        let removed = {
            let mut store = self.lock_store();
            evict_idle_sessions(&mut store);
            store.sessions.remove(sid)
        };
        if let Some(sess) = removed {
            self.emit_activity(
                meta,
                BrowserActivityFields {
                    session_id: sess.id,
                    action: "close".into(),
                    url: Some(sess.current_url),
                    title: sess.title,
                    selector: None,
                    x_pct: Some(sess.cursor_x_pct),
                    y_pct: Some(sess.cursor_y_pct),
                    w_pct: None,
                    h_pct: None,
                    scroll_y: Some(sess.scroll_y),
                    status: "closed".into(),
                    session_agent_id: sess.agent_id,
                },
            );
            true
        } else {
            false
        }
    }

    pub fn sanitized_session_preview_html(
        &self,
        session_id: Option<&str>,
    ) -> Result<(String, String, String)> {
        let (sid, url, raw_html, active_selector) = {
            let mut store = self.lock_store();
            evict_idle_sessions(&mut store);
            let sess = match session_id.map(str::trim).filter(|s| !s.is_empty()) {
                Some(id) => store.sessions.get(id).ok_or_else(|| {
                    AgentError::NotFound(format!("browser session '{id}' not found"))
                })?,
                None => store
                    .sessions
                    .values()
                    .max_by_key(|s| s.last_active_seq)
                    .ok_or_else(|| AgentError::NotFound("no active browser session".into()))?,
            };
            (
                sess.id.clone(),
                sanitize_url_for_event(&sess.current_url),
                sess.html.clone(),
                sess.last_selector.clone(),
            )
        };
        let sanitized = sanitize_html_for_preview(&raw_html, active_selector.as_deref());
        Ok((sid, url, sanitized))
    }

    pub async fn execute(
        &self,
        args: &Value,
        cfg: &ToolsConfig,
        client: &reqwest::Client,
    ) -> Result<Value> {
        self.execute_with_meta(args, cfg, client, None).await
    }

    pub async fn execute_with_meta(
        &self,
        args: &Value,
        cfg: &ToolsConfig,
        _client: &reqwest::Client,
        meta: Option<BrowserActivityContext>,
    ) -> Result<Value> {
        let action = args
            .get("action")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| AgentError::InvalidInput("missing required string 'action'".into()))?
            .to_ascii_lowercase();

        let res = match action.as_str() {
            "sessions" => Ok(self.list_sessions()),
            "close" => {
                let sid = require_str(args, "session_id")?;
                let closed = self.close_session_internal(sid, meta.as_ref());
                Ok(json!({
                    "ok": true,
                    "session_id": sid,
                    "closed": closed,
                }))
            }
            "snapshot" => self.handle_snapshot(args, meta.as_ref()),
            "text" => self.handle_text(args, meta.as_ref()),
            "hover" => self.handle_hover(args, meta.as_ref()),
            "scroll" => self.handle_scroll(args, meta.as_ref()),
            "fill" => self.handle_fill(args, meta.as_ref()),
            "links" => self.handle_links(args, meta.as_ref()),
            "save_snapshot" => self.handle_save_snapshot(args, cfg, meta.as_ref()),
            "open" | "navigate" => {
                self.handle_open_or_navigate(args, cfg, &action, meta.as_ref())
                    .await
            }
            "click" => self.handle_click(args, cfg, meta.as_ref()).await,
            "submit" => self.handle_submit(args, cfg, meta.as_ref()).await,
            "back" => self.handle_back(args, cfg, meta.as_ref()).await,
            "download" => self.handle_download(args, cfg, meta.as_ref()).await,
            other => Err(AgentError::InvalidInput(format!(
                "unsupported browser action '{other}'"
            ))),
        };

        if let Err(ref err) = res {
            let _ = err;
            self.emit_error_for_args(&action, args, meta.as_ref());
        }
        res
    }

    fn emit_error_for_args(
        &self,
        action: &str,
        args: &Value,
        meta: Option<&BrowserActivityContext>,
    ) {
        let req_sid = optional_trimmed_str(args, "session_id")
            .unwrap_or("")
            .to_string();
        let req_selector = optional_ref(args).map(|r| normalize_ref(&r));
        let req_url = optional_trimmed_str(args, "url").map(str::to_string);
        let (sess_url, sess_title, sess_x, sess_y, sess_scroll, sess_agent) = if !req_sid.is_empty()
        {
            let store = self.lock_store();
            if let Some(s) = store.sessions.get(&req_sid) {
                (
                    Some(s.current_url.clone()),
                    s.title.clone(),
                    Some(s.cursor_x_pct),
                    Some(s.cursor_y_pct),
                    Some(s.scroll_y),
                    s.agent_id.clone(),
                )
            } else {
                (None, None, None, None, None, None)
            }
        } else {
            (None, None, None, None, None, None)
        };
        self.emit_activity(
            meta,
            BrowserActivityFields {
                session_id: req_sid,
                action: action.to_string(),
                url: req_url.or(sess_url),
                title: sess_title,
                selector: req_selector,
                x_pct: sess_x,
                y_pct: sess_y,
                w_pct: None,
                h_pct: None,
                scroll_y: sess_scroll,
                status: "error".into(),
                session_agent_id: sess_agent,
            },
        );
    }

    async fn handle_open_or_navigate(
        &self,
        args: &Value,
        cfg: &ToolsConfig,
        action: &str,
        meta: Option<&BrowserActivityContext>,
    ) -> Result<Value> {
        ensure_network_allowed(cfg)?;
        let raw_url = require_str(args, "url")?;
        if let Some(remote_resp) = try_remote_browser_bridge(args, cfg).await {
            return Ok(remote_resp);
        }

        let requested_sid = optional_trimmed_str(args, "session_id").map(str::to_string);
        let (target_url, existing_cookies, existing_agent) = {
            let mut store = self.lock_store();
            evict_idle_sessions(&mut store);
            if let Some(ref sid) = requested_sid {
                if let Some(sess) = store.sessions.get(sid) {
                    let resolved = resolve_url_against(Some(&sess.current_url), raw_url)?;
                    (resolved, sess.cookies.clone(), sess.agent_id.clone())
                } else {
                    (resolve_url_against(None, raw_url)?, Vec::new(), None)
                }
            } else {
                (resolve_url_against(None, raw_url)?, Vec::new(), None)
            }
        };

        self.emit_activity(
            meta,
            BrowserActivityFields {
                session_id: requested_sid.clone().unwrap_or_default(),
                action: action.to_string(),
                url: Some(target_url.clone()),
                title: None,
                selector: None,
                x_pct: Some(0.0),
                y_pct: Some(0.0),
                w_pct: None,
                h_pct: None,
                scroll_y: Some(0.0),
                status: "navigating".into(),
                session_agent_id: existing_agent.clone(),
            },
        );

        let resp = fetch_page_with_cookies(
            reqwest::Method::GET,
            &target_url,
            None,
            None,
            &existing_cookies,
            cfg,
        )
        .await?;

        if let Some(challenge) = check_html_challenge(&resp) {
            self.emit_activity(
                meta,
                BrowserActivityFields {
                    session_id: requested_sid.unwrap_or_default(),
                    action: action.to_string(),
                    url: Some(resp.final_url.clone()),
                    title: challenge["challenge_kind"].as_str().map(str::to_string),
                    selector: None,
                    x_pct: None,
                    y_pct: None,
                    w_pct: None,
                    h_pct: None,
                    scroll_y: None,
                    status: "error".into(),
                    session_agent_id: existing_agent,
                },
            );
            return Ok(challenge);
        }

        let page = build_page_update(&resp);
        let (
            sid,
            history_len,
            cookie_count,
            elements_count,
            links_count,
            title,
            final_url,
            status,
            cursor_val,
            cursor_x,
            cursor_y,
            scroll_y,
            sess_agent_id,
        ) = {
            let mut store = self.lock_store();
            evict_idle_sessions(&mut store);
            store.access_seq = store.access_seq.saturating_add(1);
            let seq = store.access_seq;

            let sid = requested_sid.unwrap_or_else(|| generate_session_id(&store.sessions));
            if let Some(sess) = store.sessions.get_mut(&sid) {
                apply_meta_agent(sess, meta);
                apply_page_update(sess, page, &resp.headers, true, seq, true);
                sess.last_action = Some(action.to_string());
                (
                    sid,
                    sess.history.len(),
                    sess.cookies.len(),
                    sess.elements.len(),
                    sess.elements.iter().filter(|e| e.tag == "a").count(),
                    sess.title.clone(),
                    sess.current_url.clone(),
                    sess.status,
                    cursor_to_json(sess),
                    sess.cursor_x_pct,
                    sess.cursor_y_pct,
                    sess.scroll_y,
                    sess.agent_id.clone(),
                )
            } else {
                ensure_session_capacity(&mut store);
                let mut sess = BrowserSession {
                    id: sid.clone(),
                    current_url: String::new(),
                    title: None,
                    status: 0,
                    html: String::new(),
                    history: Vec::new(),
                    cookies: Vec::new(),
                    elements: Vec::new(),
                    forms: Vec::new(),
                    input_values: HashMap::new(),
                    cursor_x_pct: 0.0,
                    cursor_y_pct: 0.0,
                    scroll_y: 0.0,
                    last_action: Some(action.to_string()),
                    last_selector: None,
                    agent_id: meta.and_then(|m| non_empty_trimmed(m.agent_id.as_deref())),
                    last_active: Instant::now(),
                    last_active_seq: seq,
                    last_active_iso: now_iso(),
                };
                apply_page_update(&mut sess, page, &resp.headers, true, seq, true);
                sess.last_action = Some(action.to_string());
                let out = (
                    sid.clone(),
                    sess.history.len(),
                    sess.cookies.len(),
                    sess.elements.len(),
                    sess.elements.iter().filter(|e| e.tag == "a").count(),
                    sess.title.clone(),
                    sess.current_url.clone(),
                    sess.status,
                    cursor_to_json(&sess),
                    sess.cursor_x_pct,
                    sess.cursor_y_pct,
                    sess.scroll_y,
                    sess.agent_id.clone(),
                );
                store.sessions.insert(sid, sess);
                out
            }
        };

        self.emit_activity(
            meta,
            BrowserActivityFields {
                session_id: sid.clone(),
                action: action.to_string(),
                url: Some(final_url.clone()),
                title: title.clone(),
                selector: None,
                x_pct: Some(cursor_x),
                y_pct: Some(cursor_y),
                w_pct: None,
                h_pct: None,
                scroll_y: Some(scroll_y),
                status: "opened".into(),
                session_agent_id: sess_agent_id,
            },
        );

        Ok(json!({
            "ok": true,
            "session_id": sid,
            "url": final_url,
            "status": status,
            "title": title,
            "elements_count": elements_count,
            "links_count": links_count,
            "history_len": history_len,
            "cookie_count": cookie_count,
            "truncated": resp.truncated,
            "cursor": cursor_val,
        }))
    }

    fn handle_snapshot(
        &self,
        args: &Value,
        meta: Option<&BrowserActivityContext>,
    ) -> Result<Value> {
        let sid = require_str(args, "session_id")?;
        let mut store = self.lock_store();
        evict_idle_sessions(&mut store);
        store.access_seq = store.access_seq.saturating_add(1);
        let seq = store.access_seq;
        let sess = store
            .sessions
            .get_mut(sid)
            .ok_or_else(|| AgentError::NotFound(format!("browser session '{sid}' not found")))?;
        touch_session(sess, seq);
        apply_meta_agent(sess, meta);

        let elements_json: Vec<Value> = sess
            .elements
            .iter()
            .map(|e| element_to_json(e, sess.input_values.get(&e.ref_id)))
            .collect();
        let links_count = sess.elements.iter().filter(|e| e.tag == "a").count();
        let table = format_elements_table(&sess.elements, &sess.input_values);
        let page_text = extract_readable_text(&sess.html);

        let mut combined = String::new();
        if let Some(ref t) = sess.title {
            combined.push_str("Title: ");
            combined.push_str(t);
            combined.push('\n');
        }
        combined.push_str("URL: ");
        combined.push_str(&sess.current_url);
        combined.push('\n');
        if !table.is_empty() {
            combined.push_str("\nInteractive Elements:\n");
            combined.push_str(&table);
        }
        if !page_text.is_empty() {
            combined.push_str("\nPage Text:\n");
            combined.push_str(&page_text);
        }

        let wrapped_snapshot = crate::web::wrap_untrusted_web_content(&sess.current_url, &combined);
        let wrapped_text = crate::web::wrap_untrusted_web_content(&sess.current_url, &page_text);

        Ok(json!({
            "ok": true,
            "session_id": sess.id,
            "url": sess.current_url,
            "status": sess.status,
            "title": sess.title,
            "elements_count": elements_json.len(),
            "links_count": links_count,
            "elements": elements_json,
            "snapshot": wrapped_snapshot,
            "text": wrapped_text,
            "history_len": sess.history.len(),
            "cookie_count": sess.cookies.len(),
            "cursor": cursor_to_json(sess),
        }))
    }

    fn handle_text(&self, args: &Value, meta: Option<&BrowserActivityContext>) -> Result<Value> {
        let sid = require_str(args, "session_id")?;
        let mut store = self.lock_store();
        evict_idle_sessions(&mut store);
        store.access_seq = store.access_seq.saturating_add(1);
        let seq = store.access_seq;
        let sess = store
            .sessions
            .get_mut(sid)
            .ok_or_else(|| AgentError::NotFound(format!("browser session '{sid}' not found")))?;
        touch_session(sess, seq);
        apply_meta_agent(sess, meta);

        let page_text = extract_readable_text(&sess.html);
        let wrapped_text = crate::web::wrap_untrusted_web_content(&sess.current_url, &page_text);
        let links_count = sess.elements.iter().filter(|e| e.tag == "a").count();

        Ok(json!({
            "ok": true,
            "session_id": sess.id,
            "url": sess.current_url,
            "status": sess.status,
            "title": sess.title,
            "text": wrapped_text,
            "elements_count": sess.elements.len(),
            "links_count": links_count,
            "history_len": sess.history.len(),
            "cookie_count": sess.cookies.len(),
            "cursor": cursor_to_json(sess),
        }))
    }

    fn handle_hover(&self, args: &Value, meta: Option<&BrowserActivityContext>) -> Result<Value> {
        let sid = require_str(args, "session_id")?;
        let ref_raw = require_ref(args)?;
        let norm = normalize_ref(&ref_raw);

        let (
            out_json,
            url,
            title,
            cx,
            cy,
            ew,
            eh,
            scroll_y,
            sess_agent,
            resolved_sid,
            resolved_ref,
        ) = {
            let mut store = self.lock_store();
            evict_idle_sessions(&mut store);
            store.access_seq = store.access_seq.saturating_add(1);
            let seq = store.access_seq;
            let sess = store
                .sessions
                .get_mut(sid)
                .ok_or_else(|| AgentError::NotFound(format!("browser session '{sid}' not found")))?;
            touch_session(sess, seq);
            apply_meta_agent(sess, meta);

            let elem = sess
                .elements
                .iter()
                .find(|e| e.ref_id == norm)
                .ok_or_else(|| AgentError::NotFound(format!("element ref '{norm}' not found")))?
                .clone();

            let (cx, cy) = elem_center(&elem);
            sess.cursor_x_pct = cx;
            sess.cursor_y_pct = cy;
            sess.last_action = Some("hover".into());
            sess.last_selector = Some(elem.ref_id.clone());

            let filled = sess.input_values.get(&elem.ref_id);
            let cursor_val = cursor_to_json(sess);
            let out = json!({
                "ok": true,
                "session_id": sess.id,
                "url": sess.current_url,
                "status": sess.status,
                "title": sess.title,
                "ref": elem.ref_id,
                "tag": elem.tag,
                "role": elem.role,
                "text": elem.text,
                "x_pct": cx,
                "y_pct": cy,
                "w_pct": clamp_pct(elem.w_pct),
                "h_pct": clamp_pct(elem.h_pct),
                "element": element_to_json(&elem, filled),
                "elements_count": sess.elements.len(),
                "links_count": sess.elements.iter().filter(|e| e.tag == "a").count(),
                "history_len": sess.history.len(),
                "cookie_count": sess.cookies.len(),
                "cursor": cursor_val,
            });
            (
                out,
                sess.current_url.clone(),
                sess.title.clone(),
                cx,
                cy,
                elem.w_pct,
                elem.h_pct,
                sess.scroll_y,
                sess.agent_id.clone(),
                sess.id.clone(),
                elem.ref_id,
            )
        };

        self.emit_activity(
            meta,
            BrowserActivityFields {
                session_id: resolved_sid,
                action: "hover".into(),
                url: Some(url),
                title,
                selector: Some(resolved_ref),
                x_pct: Some(cx),
                y_pct: Some(cy),
                w_pct: Some(ew),
                h_pct: Some(eh),
                scroll_y: Some(scroll_y),
                status: "hovered".into(),
                session_agent_id: sess_agent,
            },
        );

        Ok(out_json)
    }

    fn handle_scroll(&self, args: &Value, meta: Option<&BrowserActivityContext>) -> Result<Value> {
        let sid = require_str(args, "session_id")?;
        let target_ref = optional_ref(args).map(|r| normalize_ref(&r));
        let explicit_y = match args.get("y_pct") {
            None | Some(Value::Null) => None,
            Some(v) => {
                let num = v.as_f64().ok_or_else(|| {
                    AgentError::InvalidInput("scroll 'y_pct' must be a finite number".into())
                })?;
                if !num.is_finite() {
                    return Err(AgentError::InvalidInput(
                        "scroll 'y_pct' must be a finite number".into(),
                    ));
                }
                Some(clamp_pct(num as f32))
            }
        };
        let amount = match args.get("amount") {
            None | Some(Value::Null) => 25.0_f32,
            Some(v) => {
                let num = v.as_f64().ok_or_else(|| {
                    AgentError::InvalidInput(
                        "scroll 'amount' must be a finite non-negative number".into(),
                    )
                })?;
                if !num.is_finite() || num < 0.0 {
                    return Err(AgentError::InvalidInput(
                        "scroll 'amount' must be a finite non-negative number".into(),
                    ));
                }
                clamp_pct(num as f32)
            }
        };
        let direction = optional_trimmed_str(args, "direction").map(|d| d.to_ascii_lowercase());
        if let Some(ref dir) = direction {
            if !matches!(dir.as_str(), "up" | "down" | "top" | "bottom") {
                return Err(AgentError::InvalidInput(format!(
                    "unsupported scroll direction '{dir}'; expected up, down, top, or bottom"
                )));
            }
        }

        let (
            out_json,
            url,
            title,
            resolved_selector,
            cursor_x,
            cursor_y,
            ew,
            eh,
            new_scroll_y,
            sess_agent,
            resolved_sid,
        ) = {
            let mut store = self.lock_store();
            evict_idle_sessions(&mut store);
            store.access_seq = store.access_seq.saturating_add(1);
            let seq = store.access_seq;
            let sess = store
                .sessions
                .get_mut(sid)
                .ok_or_else(|| AgentError::NotFound(format!("browser session '{sid}' not found")))?;
            touch_session(sess, seq);
            apply_meta_agent(sess, meta);

            let mut elem_dims: (Option<String>, Option<f32>, Option<f32>) = (None, None, None);
            let next_scroll = if let Some(ref norm) = target_ref {
                let elem = sess
                    .elements
                    .iter()
                    .find(|e| &e.ref_id == norm)
                    .ok_or_else(|| {
                        AgentError::NotFound(format!("element ref '{norm}' not found"))
                    })?;
                elem_dims = (
                    Some(elem.ref_id.clone()),
                    Some(elem.w_pct),
                    Some(elem.h_pct),
                );
                clamp_pct(elem.y_pct)
            } else if let Some(y) = explicit_y {
                y
            } else {
                match direction.as_deref().unwrap_or("down") {
                    "top" => 0.0,
                    "bottom" => 100.0,
                    "up" => clamp_pct(sess.scroll_y - amount),
                    _ => clamp_pct(sess.scroll_y + amount),
                }
            };

            sess.scroll_y = next_scroll;
            sess.last_action = Some("scroll".into());
            sess.last_selector = elem_dims.0.clone();

            let cursor_val = cursor_to_json(sess);
            let out = json!({
                "ok": true,
                "session_id": sess.id,
                "url": sess.current_url,
                "status": sess.status,
                "title": sess.title,
                "scroll_y": next_scroll,
                "ref": elem_dims.0,
                "elements_count": sess.elements.len(),
                "links_count": sess.elements.iter().filter(|e| e.tag == "a").count(),
                "history_len": sess.history.len(),
                "cookie_count": sess.cookies.len(),
                "cursor": cursor_val,
            });
            (
                out,
                sess.current_url.clone(),
                sess.title.clone(),
                elem_dims.0,
                sess.cursor_x_pct,
                sess.cursor_y_pct,
                elem_dims.1,
                elem_dims.2,
                next_scroll,
                sess.agent_id.clone(),
                sess.id.clone(),
            )
        };

        self.emit_activity(
            meta,
            BrowserActivityFields {
                session_id: resolved_sid,
                action: "scroll".into(),
                url: Some(url),
                title,
                selector: resolved_selector,
                x_pct: Some(cursor_x),
                y_pct: Some(cursor_y),
                w_pct: ew,
                h_pct: eh,
                scroll_y: Some(new_scroll_y),
                status: "scrolled".into(),
                session_agent_id: sess_agent,
            },
        );

        Ok(out_json)
    }

    fn handle_fill(&self, args: &Value, meta: Option<&BrowserActivityContext>) -> Result<Value> {
        let sid = require_str(args, "session_id")?;
        let ref_raw = require_ref(args)?;
        let value = match args.get("value") {
            Some(Value::String(s)) => s.clone(),
            Some(Value::Number(n)) => n.to_string(),
            Some(Value::Bool(b)) => b.to_string(),
            Some(Value::Null) | None => {
                return Err(AgentError::InvalidInput(
                    "missing required string 'value'".into(),
                ))
            }
            Some(other) => other.to_string(),
        };

        let (
            out_json,
            url,
            title,
            ref_id,
            cx,
            cy,
            ew,
            eh,
            scroll_y,
            sess_agent,
            resolved_sid,
        ) = {
            let mut store = self.lock_store();
            evict_idle_sessions(&mut store);
            store.access_seq = store.access_seq.saturating_add(1);
            let seq = store.access_seq;
            let sess = store
                .sessions
                .get_mut(sid)
                .ok_or_else(|| AgentError::NotFound(format!("browser session '{sid}' not found")))?;
            touch_session(sess, seq);
            apply_meta_agent(sess, meta);

            let norm = normalize_ref(&ref_raw);
            let elem = sess
                .elements
                .iter()
                .find(|e| e.ref_id == norm)
                .ok_or_else(|| AgentError::NotFound(format!("element ref '{norm}' not found")))?
                .clone();

            if elem.disabled {
                return Err(AgentError::InvalidInput(format!(
                    "element '{}' is disabled",
                    elem.ref_id
                )));
            }

            let fillable = match elem.tag.as_str() {
                "textarea" | "select" => true,
                "input" => !matches!(
                    elem.input_type.as_deref(),
                    Some("submit" | "button" | "reset" | "image")
                ),
                _ => false,
            };
            if !fillable {
                return Err(AgentError::InvalidInput(format!(
                    "element '{}' ({}) cannot be filled",
                    elem.ref_id, elem.tag
                )));
            }

            let ref_id = elem.ref_id.clone();
            let tag = elem.tag.clone();
            let name = elem.name.clone();
            let (cx, cy) = elem_center(&elem);
            sess.input_values.insert(ref_id.clone(), value.clone());
            sess.cursor_x_pct = cx;
            sess.cursor_y_pct = cy;
            sess.last_action = Some("fill".into());
            sess.last_selector = Some(ref_id.clone());

            let cursor_val = cursor_to_json(sess);
            let out = json!({
                "ok": true,
                "session_id": sess.id,
                "ref": ref_id,
                "tag": tag,
                "name": name,
                "value": value,
                "cursor": cursor_val,
            });
            (
                out,
                sess.current_url.clone(),
                sess.title.clone(),
                ref_id,
                cx,
                cy,
                elem.w_pct,
                elem.h_pct,
                sess.scroll_y,
                sess.agent_id.clone(),
                sess.id.clone(),
            )
        };

        self.emit_activity(
            meta,
            BrowserActivityFields {
                session_id: resolved_sid,
                action: "fill".into(),
                url: Some(url),
                title,
                selector: Some(ref_id),
                x_pct: Some(cx),
                y_pct: Some(cy),
                w_pct: Some(ew),
                h_pct: Some(eh),
                scroll_y: Some(scroll_y),
                status: "filled".into(),
                session_agent_id: sess_agent,
            },
        );

        Ok(out_json)
    }

    async fn handle_click(
        &self,
        args: &Value,
        cfg: &ToolsConfig,
        meta: Option<&BrowserActivityContext>,
    ) -> Result<Value> {
        ensure_network_allowed(cfg)?;
        let sid = require_str(args, "session_id")?;
        let ref_raw = require_ref(args)?;
        if let Some(remote_resp) = try_remote_browser_bridge(args, cfg).await {
            return Ok(remote_resp);
        }

        let norm = normalize_ref(&ref_raw);
        enum LocalControlOp {
            ToggleCheckbox,
            SelectRadio,
            ResetForm,
            Noop,
        }
        enum ClickAction {
            Navigate {
                url: String,
                cookies: Vec<StoredCookie>,
            },
            Submit(SubmitPlan),
            LocalControl(LocalControlOp),
        }
        enum ResolvedClick {
            Remote {
                action: ClickAction,
                clicked_ref: String,
                cx: f32,
                cy: f32,
                ew: f32,
                eh: f32,
                prev_url: String,
                prev_title: Option<String>,
                prev_scroll: f32,
                sess_agent: Option<String>,
            },
            Local {
                out_sid: String,
                out_url: String,
                out_status: u16,
                out_title: Option<String>,
                elements_count: usize,
                links_count: usize,
                history_len: usize,
                cookie_count: usize,
                cursor_val: Value,
                scroll_y: f32,
                final_agent: Option<String>,
                clicked_ref: String,
                cx: f32,
                cy: f32,
                ew: f32,
                eh: f32,
            },
        }

        let resolved = {
            let mut store = self.lock_store();
            evict_idle_sessions(&mut store);
            store.access_seq = store.access_seq.saturating_add(1);
            let seq = store.access_seq;
            let sess = store
                .sessions
                .get_mut(sid)
                .ok_or_else(|| AgentError::NotFound(format!("browser session '{sid}' not found")))?;
            touch_session(sess, seq);
            apply_meta_agent(sess, meta);

            let elem = sess
                .elements
                .iter()
                .find(|e| e.ref_id == norm)
                .ok_or_else(|| AgentError::NotFound(format!("element ref '{norm}' not found")))?
                .clone();

            if elem.disabled {
                return Err(AgentError::InvalidInput(format!(
                    "element '{}' is disabled",
                    elem.ref_id
                )));
            }

            let (cx, cy) = elem_center(&elem);
            sess.cursor_x_pct = cx;
            sess.cursor_y_pct = cy;
            sess.last_selector = Some(elem.ref_id.clone());

            let action_plan = if elem.tag == "a" {
                let href = elem.href.as_deref().map(str::trim).unwrap_or("");
                if href.is_empty() {
                    return Err(AgentError::InvalidInput(format!(
                        "link '{}' has no href",
                        elem.ref_id
                    )));
                }
                if href.to_ascii_lowercase().starts_with("javascript:") {
                    return Err(AgentError::PermissionDenied(
                        "javascript: URLs are forbidden in sandbox browser".into(),
                    ));
                }
                let target = resolve_url_against(Some(&sess.current_url), href)?;
                ClickAction::Navigate {
                    url: target,
                    cookies: sess.cookies.clone(),
                }
            } else if elem.tag == "form"
                || (elem.tag == "button"
                    && matches!(
                        elem.input_type.as_deref(),
                        None | Some("submit") | Some("")
                    ))
                || (elem.tag == "input"
                    && matches!(elem.input_type.as_deref(), Some("submit" | "image")))
            {
                ClickAction::Submit(build_submit_plan(sess, &elem)?)
            } else if elem.tag == "button"
                && matches!(elem.input_type.as_deref(), Some("button" | "reset"))
            {
                let op = if elem.input_type.as_deref() == Some("reset") {
                    LocalControlOp::ResetForm
                } else {
                    LocalControlOp::Noop
                };
                ClickAction::LocalControl(op)
            } else if elem.tag == "input"
                && matches!(
                    elem.input_type.as_deref(),
                    Some("button" | "reset" | "checkbox" | "radio")
                )
            {
                let op = match elem.input_type.as_deref() {
                    Some("checkbox") => LocalControlOp::ToggleCheckbox,
                    Some("radio") => LocalControlOp::SelectRadio,
                    Some("reset") => LocalControlOp::ResetForm,
                    _ => LocalControlOp::Noop,
                };
                ClickAction::LocalControl(op)
            } else if elem.role == "button" {
                ClickAction::LocalControl(LocalControlOp::Noop)
            } else {
                return Err(AgentError::InvalidInput(format!(
                    "element '{}' ({}) is not clickable",
                    elem.ref_id, elem.tag
                )));
            };

            if let ClickAction::LocalControl(op) = action_plan {
                match op {
                    LocalControlOp::ToggleCheckbox => {
                        let currently_checked = match sess.input_values.get(&elem.ref_id) {
                            Some(v) => !matches!(
                                v.trim().to_ascii_lowercase().as_str(),
                                "" | "false" | "0" | "off"
                            ),
                            None => elem.checked,
                        };
                        sess.input_values.insert(
                            elem.ref_id.clone(),
                            if currently_checked { "false" } else { "true" }.to_string(),
                        );
                    }
                    LocalControlOp::SelectRadio => {
                        if let Some(ref group_name) = elem.name {
                            if !group_name.is_empty() {
                                let sibling_refs: Vec<String> = sess
                                    .elements
                                    .iter()
                                    .filter(|e| {
                                        e.tag == "input"
                                            && e.input_type.as_deref() == Some("radio")
                                            && e.form_idx == elem.form_idx
                                            && e.name.as_deref() == Some(group_name.as_str())
                                    })
                                    .map(|e| e.ref_id.clone())
                                    .collect();
                                for sib in sibling_refs {
                                    if sib != elem.ref_id {
                                        sess.input_values.insert(sib, "false".to_string());
                                    }
                                }
                            }
                        }
                        sess.input_values
                            .insert(elem.ref_id.clone(), "true".to_string());
                    }
                    LocalControlOp::ResetForm => {
                        if let Some(fidx) = elem.form_idx {
                            let form_refs: Vec<String> = sess
                                .elements
                                .iter()
                                .filter(|e| e.form_idx == Some(fidx))
                                .map(|e| e.ref_id.clone())
                                .collect();
                            for r in form_refs {
                                sess.input_values.remove(&r);
                            }
                        }
                    }
                    LocalControlOp::Noop => {}
                }
                sess.last_action = Some("click".into());
                sess.last_selector = Some(elem.ref_id.clone());
                sess.cursor_x_pct = cx;
                sess.cursor_y_pct = cy;
                ResolvedClick::Local {
                    out_sid: sess.id.clone(),
                    out_url: sess.current_url.clone(),
                    out_status: sess.status,
                    out_title: sess.title.clone(),
                    elements_count: sess.elements.len(),
                    links_count: sess.elements.iter().filter(|e| e.tag == "a").count(),
                    history_len: sess.history.len(),
                    cookie_count: sess.cookies.len(),
                    cursor_val: cursor_to_json(sess),
                    scroll_y: sess.scroll_y,
                    final_agent: sess.agent_id.clone(),
                    clicked_ref: elem.ref_id,
                    cx,
                    cy,
                    ew: elem.w_pct,
                    eh: elem.h_pct,
                }
            } else {
                sess.last_action = Some("click".into());
                ResolvedClick::Remote {
                    action: action_plan,
                    clicked_ref: elem.ref_id,
                    cx,
                    cy,
                    ew: elem.w_pct,
                    eh: elem.h_pct,
                    prev_url: sess.current_url.clone(),
                    prev_title: sess.title.clone(),
                    prev_scroll: sess.scroll_y,
                    sess_agent: sess.agent_id.clone(),
                }
            }
        };

        let (plan, clicked_ref, cx, cy, ew, eh, prev_url, prev_title, prev_scroll, sess_agent) =
            match resolved {
                ResolvedClick::Local {
                    out_sid,
                    out_url,
                    out_status,
                    out_title,
                    elements_count,
                    links_count,
                    history_len,
                    cookie_count,
                    cursor_val,
                    scroll_y,
                    final_agent,
                    clicked_ref,
                    cx,
                    cy,
                    ew,
                    eh,
                } => {
                    self.emit_activity(
                        meta,
                        BrowserActivityFields {
                            session_id: out_sid.clone(),
                            action: "click".into(),
                            url: Some(out_url.clone()),
                            title: out_title.clone(),
                            selector: Some(clicked_ref),
                            x_pct: Some(cx),
                            y_pct: Some(cy),
                            w_pct: Some(ew),
                            h_pct: Some(eh),
                            scroll_y: Some(scroll_y),
                            status: "clicked".into(),
                            session_agent_id: final_agent,
                        },
                    );
                    return Ok(json!({
                        "ok": true,
                        "session_id": out_sid,
                        "method": "LOCAL",
                        "url": out_url,
                        "status": out_status,
                        "title": out_title,
                        "elements_count": elements_count,
                        "links_count": links_count,
                        "history_len": history_len,
                        "cookie_count": cookie_count,
                        "truncated": false,
                        "cursor": cursor_val,
                    }));
                }
                ResolvedClick::Remote {
                    action,
                    clicked_ref,
                    cx,
                    cy,
                    ew,
                    eh,
                    prev_url,
                    prev_title,
                    prev_scroll,
                    sess_agent,
                } => (
                    action,
                    clicked_ref,
                    cx,
                    cy,
                    ew,
                    eh,
                    prev_url,
                    prev_title,
                    prev_scroll,
                    sess_agent,
                ),
            };

        self.emit_activity(
            meta,
            BrowserActivityFields {
                session_id: sid.to_string(),
                action: "click".into(),
                url: Some(prev_url),
                title: prev_title,
                selector: Some(clicked_ref.clone()),
                x_pct: Some(cx),
                y_pct: Some(cy),
                w_pct: Some(ew),
                h_pct: Some(eh),
                scroll_y: Some(prev_scroll),
                status: "navigating".into(),
                session_agent_id: sess_agent.clone(),
            },
        );

        let (resp, used_method) = match plan {
            ClickAction::Navigate { url, cookies } => (
                fetch_page_with_cookies(reqwest::Method::GET, &url, None, None, &cookies, cfg)
                    .await?,
                "GET".to_string(),
            ),
            ClickAction::Submit(sub) => {
                let m_str = sub.method.as_str().to_string();
                let resp = fetch_page_with_cookies(
                    sub.method,
                    &sub.target_url,
                    sub.body,
                    sub.content_type,
                    &sub.cookies,
                    cfg,
                )
                .await?;
                (resp, m_str)
            }
            ClickAction::LocalControl(_) => unreachable!("local control returned above"),
        };

        if let Some(challenge) = check_html_challenge(&resp) {
            self.emit_activity(
                meta,
                BrowserActivityFields {
                    session_id: sid.to_string(),
                    action: "click".into(),
                    url: Some(resp.final_url.clone()),
                    title: challenge["challenge_kind"].as_str().map(str::to_string),
                    selector: Some(clicked_ref),
                    x_pct: Some(cx),
                    y_pct: Some(cy),
                    w_pct: Some(ew),
                    h_pct: Some(eh),
                    scroll_y: Some(prev_scroll),
                    status: "error".into(),
                    session_agent_id: sess_agent,
                },
            );
            return Ok(challenge);
        }

        let page = build_page_update(&resp);
        let (
            out_sid,
            out_url,
            out_status,
            out_title,
            elements_count,
            links_count,
            history_len,
            cookie_count,
            cursor_val,
            scroll_y,
            final_agent,
        ) = {
            let mut store = self.lock_store();
            evict_idle_sessions(&mut store);
            store.access_seq = store.access_seq.saturating_add(1);
            let seq = store.access_seq;
            let sess = store
                .sessions
                .get_mut(sid)
                .ok_or_else(|| AgentError::NotFound(format!("browser session '{sid}' not found")))?;
            apply_meta_agent(sess, meta);
            apply_page_update(sess, page, &resp.headers, true, seq, false);
            sess.cursor_x_pct = cx;
            sess.cursor_y_pct = cy;
            sess.last_action = Some("click".into());
            sess.last_selector = Some(clicked_ref.clone());
            (
                sess.id.clone(),
                sess.current_url.clone(),
                sess.status,
                sess.title.clone(),
                sess.elements.len(),
                sess.elements.iter().filter(|e| e.tag == "a").count(),
                sess.history.len(),
                sess.cookies.len(),
                cursor_to_json(sess),
                sess.scroll_y,
                sess.agent_id.clone(),
            )
        };

        self.emit_activity(
            meta,
            BrowserActivityFields {
                session_id: out_sid.clone(),
                action: "click".into(),
                url: Some(out_url.clone()),
                title: out_title.clone(),
                selector: Some(clicked_ref),
                x_pct: Some(cx),
                y_pct: Some(cy),
                w_pct: Some(ew),
                h_pct: Some(eh),
                scroll_y: Some(scroll_y),
                status: "clicked".into(),
                session_agent_id: final_agent,
            },
        );

        Ok(json!({
            "ok": true,
            "session_id": out_sid,
            "method": used_method,
            "url": out_url,
            "status": out_status,
            "title": out_title,
            "elements_count": elements_count,
            "links_count": links_count,
            "history_len": history_len,
            "cookie_count": cookie_count,
            "truncated": resp.truncated,
            "cursor": cursor_val,
        }))
    }

    async fn handle_submit(
        &self,
        args: &Value,
        cfg: &ToolsConfig,
        meta: Option<&BrowserActivityContext>,
    ) -> Result<Value> {
        ensure_network_allowed(cfg)?;
        let sid = require_str(args, "session_id")?;
        let ref_raw = require_ref(args)?;
        if let Some(remote_resp) = try_remote_browser_bridge(args, cfg).await {
            return Ok(remote_resp);
        }

        let norm = normalize_ref(&ref_raw);
        let (sub, submit_ref, cx, cy, ew, eh, prev_url, prev_title, prev_scroll, sess_agent) = {
            let mut store = self.lock_store();
            evict_idle_sessions(&mut store);
            store.access_seq = store.access_seq.saturating_add(1);
            let seq = store.access_seq;
            let sess = store
                .sessions
                .get_mut(sid)
                .ok_or_else(|| AgentError::NotFound(format!("browser session '{sid}' not found")))?;
            touch_session(sess, seq);
            apply_meta_agent(sess, meta);

            let elem = sess
                .elements
                .iter()
                .find(|e| e.ref_id == norm)
                .ok_or_else(|| AgentError::NotFound(format!("element ref '{norm}' not found")))?
                .clone();
            let sub_plan = build_submit_plan(sess, &elem)?;
            let (cx, cy) = elem_center(&elem);
            sess.cursor_x_pct = cx;
            sess.cursor_y_pct = cy;
            sess.last_action = Some("submit".into());
            sess.last_selector = Some(elem.ref_id.clone());
            (
                sub_plan,
                elem.ref_id,
                cx,
                cy,
                elem.w_pct,
                elem.h_pct,
                sess.current_url.clone(),
                sess.title.clone(),
                sess.scroll_y,
                sess.agent_id.clone(),
            )
        };

        self.emit_activity(
            meta,
            BrowserActivityFields {
                session_id: sid.to_string(),
                action: "submit".into(),
                url: Some(prev_url),
                title: prev_title,
                selector: Some(submit_ref.clone()),
                x_pct: Some(cx),
                y_pct: Some(cy),
                w_pct: Some(ew),
                h_pct: Some(eh),
                scroll_y: Some(prev_scroll),
                status: "navigating".into(),
                session_agent_id: sess_agent.clone(),
            },
        );

        let method_str = sub.method.as_str().to_string();
        let resp = fetch_page_with_cookies(
            sub.method,
            &sub.target_url,
            sub.body,
            sub.content_type,
            &sub.cookies,
            cfg,
        )
        .await?;

        if let Some(challenge) = check_html_challenge(&resp) {
            self.emit_activity(
                meta,
                BrowserActivityFields {
                    session_id: sid.to_string(),
                    action: "submit".into(),
                    url: Some(resp.final_url.clone()),
                    title: challenge["challenge_kind"].as_str().map(str::to_string),
                    selector: Some(submit_ref),
                    x_pct: Some(cx),
                    y_pct: Some(cy),
                    w_pct: Some(ew),
                    h_pct: Some(eh),
                    scroll_y: Some(prev_scroll),
                    status: "error".into(),
                    session_agent_id: sess_agent,
                },
            );
            return Ok(challenge);
        }

        let page = build_page_update(&resp);
        let (
            out_sid,
            out_url,
            out_status,
            out_title,
            elements_count,
            links_count,
            history_len,
            cookie_count,
            cursor_val,
            scroll_y,
            final_agent,
        ) = {
            let mut store = self.lock_store();
            evict_idle_sessions(&mut store);
            store.access_seq = store.access_seq.saturating_add(1);
            let seq = store.access_seq;
            let sess = store
                .sessions
                .get_mut(sid)
                .ok_or_else(|| AgentError::NotFound(format!("browser session '{sid}' not found")))?;
            apply_meta_agent(sess, meta);
            apply_page_update(sess, page, &resp.headers, true, seq, false);
            sess.cursor_x_pct = cx;
            sess.cursor_y_pct = cy;
            sess.last_action = Some("submit".into());
            sess.last_selector = Some(submit_ref.clone());
            (
                sess.id.clone(),
                sess.current_url.clone(),
                sess.status,
                sess.title.clone(),
                sess.elements.len(),
                sess.elements.iter().filter(|e| e.tag == "a").count(),
                sess.history.len(),
                sess.cookies.len(),
                cursor_to_json(sess),
                sess.scroll_y,
                sess.agent_id.clone(),
            )
        };

        self.emit_activity(
            meta,
            BrowserActivityFields {
                session_id: out_sid.clone(),
                action: "submit".into(),
                url: Some(out_url.clone()),
                title: out_title.clone(),
                selector: Some(submit_ref),
                x_pct: Some(cx),
                y_pct: Some(cy),
                w_pct: Some(ew),
                h_pct: Some(eh),
                scroll_y: Some(scroll_y),
                status: "opened".into(),
                session_agent_id: final_agent,
            },
        );

        Ok(json!({
            "ok": true,
            "session_id": out_sid,
            "method": method_str,
            "url": out_url,
            "status": out_status,
            "title": out_title,
            "elements_count": elements_count,
            "links_count": links_count,
            "history_len": history_len,
            "cookie_count": cookie_count,
            "truncated": resp.truncated,
            "cursor": cursor_val,
        }))
    }

    async fn handle_back(
        &self,
        args: &Value,
        cfg: &ToolsConfig,
        meta: Option<&BrowserActivityContext>,
    ) -> Result<Value> {
        ensure_network_allowed(cfg)?;
        let sid = require_str(args, "session_id")?;
        if let Some(remote_resp) = try_remote_browser_bridge(args, cfg).await {
            return Ok(remote_resp);
        }

        let (prev_url, cookies, sess_agent) = {
            let mut store = self.lock_store();
            evict_idle_sessions(&mut store);
            let sess = store
                .sessions
                .get(sid)
                .ok_or_else(|| AgentError::NotFound(format!("browser session '{sid}' not found")))?;
            if sess.history.len() < 2 {
                return Err(AgentError::InvalidInput(
                    "no previous page in session history".into(),
                ));
            }
            let target = sess.history[sess.history.len() - 2].clone();
            (target, sess.cookies.clone(), sess.agent_id.clone())
        };

        self.emit_activity(
            meta,
            BrowserActivityFields {
                session_id: sid.to_string(),
                action: "back".into(),
                url: Some(prev_url.clone()),
                title: None,
                selector: None,
                x_pct: Some(0.0),
                y_pct: Some(0.0),
                w_pct: None,
                h_pct: None,
                scroll_y: Some(0.0),
                status: "navigating".into(),
                session_agent_id: sess_agent.clone(),
            },
        );

        let resp =
            fetch_page_with_cookies(reqwest::Method::GET, &prev_url, None, None, &cookies, cfg)
                .await?;

        if let Some(challenge) = check_html_challenge(&resp) {
            self.emit_activity(
                meta,
                BrowserActivityFields {
                    session_id: sid.to_string(),
                    action: "back".into(),
                    url: Some(resp.final_url.clone()),
                    title: challenge["challenge_kind"].as_str().map(str::to_string),
                    selector: None,
                    x_pct: None,
                    y_pct: None,
                    w_pct: None,
                    h_pct: None,
                    scroll_y: None,
                    status: "error".into(),
                    session_agent_id: sess_agent,
                },
            );
            return Ok(challenge);
        }

        let page = build_page_update(&resp);
        let (
            out_sid,
            out_url,
            out_status,
            out_title,
            elements_count,
            links_count,
            history_len,
            cookie_count,
            cursor_val,
            cursor_x,
            cursor_y,
            scroll_y,
            final_agent,
        ) = {
            let mut store = self.lock_store();
            evict_idle_sessions(&mut store);
            store.access_seq = store.access_seq.saturating_add(1);
            let seq = store.access_seq;
            let sess = store
                .sessions
                .get_mut(sid)
                .ok_or_else(|| AgentError::NotFound(format!("browser session '{sid}' not found")))?;
            if sess.history.len() >= 2 {
                sess.history.pop();
            }
            apply_meta_agent(sess, meta);
            apply_page_update(sess, page, &resp.headers, false, seq, true);
            sess.last_action = Some("back".into());
            (
                sess.id.clone(),
                sess.current_url.clone(),
                sess.status,
                sess.title.clone(),
                sess.elements.len(),
                sess.elements.iter().filter(|e| e.tag == "a").count(),
                sess.history.len(),
                sess.cookies.len(),
                cursor_to_json(sess),
                sess.cursor_x_pct,
                sess.cursor_y_pct,
                sess.scroll_y,
                sess.agent_id.clone(),
            )
        };

        self.emit_activity(
            meta,
            BrowserActivityFields {
                session_id: out_sid.clone(),
                action: "back".into(),
                url: Some(out_url.clone()),
                title: out_title.clone(),
                selector: None,
                x_pct: Some(cursor_x),
                y_pct: Some(cursor_y),
                w_pct: None,
                h_pct: None,
                scroll_y: Some(scroll_y),
                status: "opened".into(),
                session_agent_id: final_agent,
            },
        );

        Ok(json!({
            "ok": true,
            "session_id": out_sid,
            "url": out_url,
            "status": out_status,
            "title": out_title,
            "elements_count": elements_count,
            "links_count": links_count,
            "history_len": history_len,
            "cookie_count": cookie_count,
            "truncated": resp.truncated,
            "cursor": cursor_val,
        }))
    }

    fn handle_links(&self, args: &Value, meta: Option<&BrowserActivityContext>) -> Result<Value> {
        let sid = require_str(args, "session_id")?;
        let mut store = self.lock_store();
        evict_idle_sessions(&mut store);
        store.access_seq = store.access_seq.saturating_add(1);
        let seq = store.access_seq;
        let sess = store
            .sessions
            .get_mut(sid)
            .ok_or_else(|| AgentError::NotFound(format!("browser session '{sid}' not found")))?;
        touch_session(sess, seq);
        apply_meta_agent(sess, meta);

        let base = Url::parse(&sess.current_url).ok();
        let links: Vec<Value> = sess
            .elements
            .iter()
            .filter(|e| e.tag == "a")
            .map(|e| {
                let raw_href = e.href.as_deref().unwrap_or("");
                let resolved = base
                    .as_ref()
                    .and_then(|b| b.join(raw_href).ok())
                    .map(|u| u.to_string());
                json!({
                    "ref": e.ref_id,
                    "text": e.text,
                    "href": e.href,
                    "resolved_url": resolved,
                    "x_pct": clamp_pct(e.x_pct),
                    "y_pct": clamp_pct(e.y_pct),
                    "w_pct": clamp_pct(e.w_pct),
                    "h_pct": clamp_pct(e.h_pct),
                })
            })
            .collect();

        Ok(json!({
            "ok": true,
            "session_id": sess.id,
            "url": sess.current_url,
            "status": sess.status,
            "title": sess.title,
            "count": links.len(),
            "links": links,
            "elements_count": sess.elements.len(),
            "links_count": links.len(),
            "cookie_count": sess.cookies.len(),
            "cursor": cursor_to_json(sess),
        }))
    }

    async fn handle_download(
        &self,
        args: &Value,
        cfg: &ToolsConfig,
        meta: Option<&BrowserActivityContext>,
    ) -> Result<Value> {
        ensure_network_allowed(cfg)?;
        let sid = require_str(args, "session_id")?;
        let explicit_url = optional_trimmed_str(args, "url");
        let explicit_ref = optional_ref(args);
        if explicit_url.is_none() && explicit_ref.is_none() {
            return Err(AgentError::InvalidInput(
                "download requires either 'url' or 'ref'".into(),
            ));
        }

        let (target_url, cookies, sess_agent) = {
            let mut store = self.lock_store();
            evict_idle_sessions(&mut store);
            let sess = store
                .sessions
                .get(sid)
                .ok_or_else(|| AgentError::NotFound(format!("browser session '{sid}' not found")))?;

            let raw_target = if let Some(u) = explicit_url {
                u.to_string()
            } else {
                let norm = normalize_ref(explicit_ref.as_deref().unwrap_or_default());
                let elem = sess
                    .elements
                    .iter()
                    .find(|e| e.ref_id == norm)
                    .ok_or_else(|| {
                        AgentError::NotFound(format!("element ref '{norm}' not found"))
                    })?;
                if let Some(ref href) = elem.href {
                    href.clone()
                } else if elem.tag == "form" {
                    let act = sess
                        .forms
                        .iter()
                        .find(|f| f.form_ref == elem.ref_id)
                        .map(|f| f.action.as_str())
                        .unwrap_or("");
                    if act.is_empty() {
                        sess.current_url.clone()
                    } else {
                        act.to_string()
                    }
                } else {
                    return Err(AgentError::InvalidInput(format!(
                        "element '{}' has no downloadable URL",
                        elem.ref_id
                    )));
                }
            };

            if raw_target.to_ascii_lowercase().starts_with("javascript:") {
                return Err(AgentError::PermissionDenied(
                    "javascript: URLs are forbidden".into(),
                ));
            }
            let resolved = resolve_url_against(Some(&sess.current_url), &raw_target)?;
            (resolved, sess.cookies.clone(), sess.agent_id.clone())
        };

        let resp =
            fetch_page_with_cookies(reqwest::Method::GET, &target_url, None, None, &cookies, cfg)
                .await?;

        if should_check_download_challenge(&resp) {
            if let Some(challenge) = check_html_challenge(&resp) {
                self.emit_activity(
                    meta,
                    BrowserActivityFields {
                        session_id: sid.to_string(),
                        action: "download".into(),
                        url: Some(resp.final_url.clone()),
                        title: challenge["challenge_kind"].as_str().map(str::to_string),
                        selector: explicit_ref.map(|r| normalize_ref(&r)),
                        x_pct: None,
                        y_pct: None,
                        w_pct: None,
                        h_pct: None,
                        scroll_y: None,
                        status: "error".into(),
                        session_agent_id: sess_agent,
                    },
                );
                return Ok(challenge);
            }
        }

        if cfg.max_file_bytes > 0 && resp.body.len() > cfg.max_file_bytes {
            return Err(AgentError::LimitExceeded(format!(
                "downloaded payload ({} bytes) exceeds max_file_bytes ({})",
                resp.body.len(),
                cfg.max_file_bytes
            )));
        }

        let safe_name = match optional_trimmed_str(args, "filename") {
            Some(raw_name) => sanitize_explicit_filename(raw_name, "download.bin")?,
            None => derive_filename_from_url(&resp.final_url),
        };

        let rel_path = format!("sandbox/downloads/{safe_name}");
        let (canonical_root, resolved_target) =
            crate::files::resolve_workspace_path_checked(&rel_path, cfg)?;
        write_checked_bytes(&resolved_target, &resp.body)?;

        let (cursor_val, cx, cy, scroll_y, final_agent, sess_title) = {
            let mut store = self.lock_store();
            evict_idle_sessions(&mut store);
            store.access_seq = store.access_seq.saturating_add(1);
            let seq = store.access_seq;
            if let Some(sess) = store.sessions.get_mut(sid) {
                touch_session(sess, seq);
                apply_meta_agent(sess, meta);
                merge_response_cookies(sess, &resp.final_url, &resp.headers);
                sess.last_action = Some("download".into());
                (
                    cursor_to_json(sess),
                    Some(sess.cursor_x_pct),
                    Some(sess.cursor_y_pct),
                    Some(sess.scroll_y),
                    sess.agent_id.clone(),
                    sess.title.clone(),
                )
            } else {
                (Value::Null, None, None, None, sess_agent, None)
            }
        };

        self.emit_activity(
            meta,
            BrowserActivityFields {
                session_id: sid.to_string(),
                action: "download".into(),
                url: Some(resp.final_url.clone()),
                title: sess_title,
                selector: explicit_ref.map(|r| normalize_ref(&r)),
                x_pct: cx,
                y_pct: cy,
                w_pct: None,
                h_pct: None,
                scroll_y,
                status: "opened".into(),
                session_agent_id: final_agent,
            },
        );

        Ok(json!({
            "ok": true,
            "session_id": sid,
            "url": resp.final_url,
            "status": resp.status,
            "path": relative_display_path(&canonical_root, &resolved_target, &rel_path),
            "bytes": resp.body.len(),
            "truncated": resp.truncated,
            "cursor": cursor_val,
        }))
    }

    fn handle_save_snapshot(
        &self,
        args: &Value,
        cfg: &ToolsConfig,
        meta: Option<&BrowserActivityContext>,
    ) -> Result<Value> {
        let sid = require_str(args, "session_id")?;
        let (url, html, markdown, cursor_val, cx, cy, scroll_y, final_agent, sess_title) = {
            let mut store = self.lock_store();
            evict_idle_sessions(&mut store);
            store.access_seq = store.access_seq.saturating_add(1);
            let seq = store.access_seq;
            let sess = store
                .sessions
                .get_mut(sid)
                .ok_or_else(|| {
                    AgentError::NotFound(format!("browser session '{sid}' not found"))
                })?;
            touch_session(sess, seq);
            apply_meta_agent(sess, meta);
            sess.last_action = Some("save_snapshot".into());
            let md = render_snapshot_markdown(sess);
            (
                sess.current_url.clone(),
                sess.html.clone(),
                md,
                cursor_to_json(sess),
                sess.cursor_x_pct,
                sess.cursor_y_pct,
                sess.scroll_y,
                sess.agent_id.clone(),
                sess.title.clone(),
            )
        };

        let base_name = match optional_trimmed_str(args, "name") {
            Some(raw_name) => {
                let sanitized = sanitize_explicit_filename(raw_name, "snapshot")?;
                sanitized
                    .strip_suffix(".html")
                    .or_else(|| sanitized.strip_suffix(".md"))
                    .unwrap_or(&sanitized)
                    .to_string()
            }
            None => derive_snapshot_name(&url, sid),
        };
        let safe_stem = if base_name.is_empty() {
            "snapshot".to_string()
        } else {
            base_name
        };

        let html_rel = format!("sandbox/snapshots/{safe_stem}.html");
        let md_rel = format!("sandbox/snapshots/{safe_stem}.md");

        let html_bytes = html.as_bytes();
        let md_bytes = markdown.as_bytes();
        if cfg.max_file_bytes > 0
            && (html_bytes.len() > cfg.max_file_bytes || md_bytes.len() > cfg.max_file_bytes)
        {
            return Err(AgentError::LimitExceeded(format!(
                "snapshot exceeds max_file_bytes ({})",
                cfg.max_file_bytes
            )));
        }

        let (root_html, html_target) =
            crate::files::resolve_workspace_path_checked(&html_rel, cfg)?;
        let (_root_md, md_target) = crate::files::resolve_workspace_path_checked(&md_rel, cfg)?;
        write_checked_bytes(&html_target, html_bytes)?;
        write_checked_bytes(&md_target, md_bytes)?;

        self.emit_activity(
            meta,
            BrowserActivityFields {
                session_id: sid.to_string(),
                action: "save_snapshot".into(),
                url: Some(url.clone()),
                title: sess_title,
                selector: None,
                x_pct: Some(cx),
                y_pct: Some(cy),
                w_pct: None,
                h_pct: None,
                scroll_y: Some(scroll_y),
                status: "opened".into(),
                session_agent_id: final_agent,
            },
        );

        Ok(json!({
            "ok": true,
            "session_id": sid,
            "url": url,
            "html_path": relative_display_path(&root_html, &html_target, &html_rel),
            "markdown_path": relative_display_path(&root_html, &md_target, &md_rel),
            "bytes_html": html_bytes.len(),
            "bytes_markdown": md_bytes.len(),
            "cursor": cursor_val,
        }))
    }
}

fn require_str<'a>(args: &'a Value, key: &str) -> Result<&'a str> {
    args.get(key)
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AgentError::InvalidInput(format!("missing required string '{key}'")))
}

fn optional_trimmed_str<'a>(args: &'a Value, key: &str) -> Option<&'a str> {
    args.get(key)
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

fn optional_ref(args: &Value) -> Option<String> {
    optional_trimmed_str(args, "ref")
        .or_else(|| optional_trimmed_str(args, "element_ref"))
        .map(str::to_string)
}

fn require_ref(args: &Value) -> Result<String> {
    optional_ref(args)
        .ok_or_else(|| AgentError::InvalidInput("missing required string 'ref'".into()))
}

fn normalize_ref(raw: &str) -> String {
    let trimmed = raw.trim();
    let inner = trimmed
        .strip_prefix("[ref=")
        .and_then(|s| s.strip_suffix(']'))
        .unwrap_or(trimmed);
    inner.trim().to_ascii_lowercase()
}

fn ensure_network_allowed(cfg: &ToolsConfig) -> Result<()> {
    if !cfg.network_access {
        return Err(AgentError::PermissionDenied(
            "network_access is disabled by sandbox policy".into(),
        ));
    }
    Ok(())
}

fn evict_idle_sessions(store: &mut BrowserStore) {
    let now = Instant::now();
    store
        .sessions
        .retain(|_, s| now.saturating_duration_since(s.last_active) <= SESSION_IDLE_TTL);
}

fn ensure_session_capacity(store: &mut BrowserStore) {
    while store.sessions.len() >= MAX_SESSIONS {
        let oldest_id = store
            .sessions
            .iter()
            .min_by_key(|(_, s)| (s.last_active, s.last_active_seq))
            .map(|(id, _)| id.clone());
        match oldest_id {
            Some(id) => {
                store.sessions.remove(&id);
            }
            None => break,
        }
    }
}

fn generate_session_id(existing: &HashMap<String, BrowserSession>) -> String {
    for _ in 0..8 {
        let raw = new_id();
        let short = &raw[..8.min(raw.len())];
        let cand = format!("bs_{short}");
        if !existing.contains_key(&cand) {
            return cand;
        }
    }
    format!("bs_{}", new_id())
}

fn touch_session(sess: &mut BrowserSession, seq: u64) {
    sess.last_active = Instant::now();
    sess.last_active_seq = seq;
    sess.last_active_iso = now_iso();
}

fn resolve_url_against(base: Option<&str>, raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(AgentError::InvalidInput("URL cannot be empty".into()));
    }
    if let Ok(abs) = Url::parse(trimmed) {
        return Ok(abs.to_string());
    }
    if let Some(base_str) = base {
        let base_url = Url::parse(base_str)
            .map_err(|e| AgentError::InvalidInput(format!("invalid base URL: {e}")))?;
        let joined = base_url
            .join(trimmed)
            .map_err(|e| AgentError::InvalidInput(format!("invalid relative URL: {e}")))?;
        return Ok(joined.to_string());
    }
    Err(AgentError::InvalidInput(format!(
        "invalid URL '{trimmed}'"
    )))
}

async fn try_remote_browser_bridge(args: &Value, cfg: &ToolsConfig) -> Option<Value> {
    let remote = cfg.remote_browser_url.trim();
    if remote.is_empty() {
        return None;
    }
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    let body = serde_json::to_vec(args).ok()?;
    match crate::web::fetch_validated_url(reqwest::Method::POST, remote, headers, Some(body), cfg)
        .await
    {
        Ok(resp) => {
            if let Some(challenge) = check_html_challenge(&resp) {
                return Some(challenge);
            }
            if (200..300).contains(&resp.status) {
                if let Ok(Value::Object(map)) = serde_json::from_slice::<Value>(&resp.body) {
                    return Some(Value::Object(map));
                }
            }
            ps5_agent_logging::log_warn(
                "browser",
                format!(
                    "remote browser bridge returned HTTP {} or non-JSON; falling back to pure-Rust mode",
                    resp.status
                ),
            );
            None
        }
        Err(err) => {
            ps5_agent_logging::log_warn(
                "browser",
                format!("remote browser bridge failed ({err}); falling back to pure-Rust mode"),
            );
            None
        }
    }
}

async fn fetch_page_with_cookies(
    method: reqwest::Method,
    target_url: &str,
    body: Option<Vec<u8>>,
    content_type: Option<&'static str>,
    cookies: &[StoredCookie],
    cfg: &ToolsConfig,
) -> Result<crate::web::ValidatedHttpResponse> {
    let mut headers = HeaderMap::new();
    if let Some(ct) = content_type {
        headers.insert(CONTENT_TYPE, HeaderValue::from_static(ct));
    }
    if let Ok(parsed) = Url::parse(target_url) {
        if let Some(cookie_hdr) = build_cookie_header(&parsed, cookies) {
            if let Ok(hv) = HeaderValue::from_str(&cookie_hdr) {
                headers.insert(COOKIE, hv);
            }
        }
    }
    crate::web::fetch_validated_url(method, target_url, headers, body, cfg).await
}

fn check_html_challenge(resp: &crate::web::ValidatedHttpResponse) -> Option<Value> {
    let preview_len = resp.body.len().min(128 * 1024);
    let html = String::from_utf8_lossy(&resp.body[..preview_len]);
    crate::web::detect_challenge_gate(resp.status, &resp.headers, &html).map(|kind| {
        json!({
            "ok": false,
            "blocked_by_challenge": true,
            "challenge_kind": kind,
            "url": resp.final_url,
            "status": resp.status,
        })
    })
}

fn should_check_download_challenge(resp: &crate::web::ValidatedHttpResponse) -> bool {
    if matches!(resp.status, 403 | 429 | 503) {
        return true;
    }
    let ct_is_html = resp
        .headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map_or(false, |s| s.to_ascii_lowercase().contains("html"));
    if ct_is_html {
        return true;
    }
    let prefix = String::from_utf8_lossy(&resp.body[..resp.body.len().min(64)]);
    let trimmed = prefix.trim_start().to_ascii_lowercase();
    trimmed.starts_with("<!doctype html") || trimmed.starts_with("<html")
}

fn build_page_update(resp: &crate::web::ValidatedHttpResponse) -> PageUpdate {
    let slice_len = floor_char_boundary_lossy(&resp.body, MAX_STORED_HTML_BYTES);
    let html = String::from_utf8_lossy(&resp.body[..slice_len]).into_owned();
    let (title, elements, forms) = parse_html_snapshot(&html);
    PageUpdate {
        final_url: resp.final_url.clone(),
        status: resp.status,
        title,
        html,
        elements,
        forms,
        truncated: resp.truncated || resp.body.len() > slice_len,
    }
}

fn floor_char_boundary_lossy(bytes: &[u8], max_bytes: usize) -> usize {
    if bytes.len() <= max_bytes {
        return bytes.len();
    }
    let mut end = max_bytes;
    while end > 0 && (bytes[end] & 0xC0) == 0x80 {
        end -= 1;
    }
    end
}

fn clamp_pct(v: f32) -> f32 {
    if !v.is_finite() {
        return 0.0;
    }
    let clamped = v.clamp(0.0, 100.0);
    (clamped * 100.0).round() / 100.0
}

fn elem_center(elem: &SnapshotElement) -> (f32, f32) {
    (
        clamp_pct(elem.x_pct + elem.w_pct / 2.0),
        clamp_pct(elem.y_pct + elem.h_pct / 2.0),
    )
}

fn cursor_to_json(sess: &BrowserSession) -> Value {
    json!({
        "x_pct": clamp_pct(sess.cursor_x_pct),
        "y_pct": clamp_pct(sess.cursor_y_pct),
        "scroll_y": clamp_pct(sess.scroll_y),
        "last_action": sess.last_action,
        "last_selector": sess.last_selector,
        "agent_id": sess.agent_id,
    })
}

fn non_empty_trimmed(val: Option<&str>) -> Option<String> {
    val.map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

fn apply_meta_agent(sess: &mut BrowserSession, meta: Option<&BrowserActivityContext>) {
    if let Some(aid) = meta.and_then(|m| non_empty_trimmed(m.agent_id.as_deref())) {
        sess.agent_id = Some(aid);
    }
}

fn sanitize_url_for_event(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    if let Ok(mut parsed) = Url::parse(trimmed) {
        if !parsed.username().is_empty() || parsed.password().is_some() {
            let _ = parsed.set_username("");
            let _ = parsed.set_password(None);
        }
        parsed.set_query(None);
        parsed.set_fragment(None);
        return parsed.to_string();
    }
    let without_qf = trimmed
        .split(['?', '#'])
        .next()
        .unwrap_or(trimmed);
    if let Some(scheme_pos) = without_qf.find("://") {
        let after_scheme = &without_qf[scheme_pos + 3..];
        let auth_end = after_scheme.find('/').unwrap_or(after_scheme.len());
        if let Some(at_pos) = after_scheme[..auth_end].rfind('@') {
            return format!(
                "{}{}",
                &without_qf[..scheme_pos + 3],
                &after_scheme[at_pos + 1..]
            );
        }
    }
    without_qf.to_string()
}

fn apply_page_update(
    sess: &mut BrowserSession,
    page: PageUpdate,
    headers: &HeaderMap,
    push_history: bool,
    seq: u64,
    reset_cursor: bool,
) {
    let _ = page.truncated;
    merge_response_cookies(sess, &page.final_url, headers);
    if push_history {
        let should_push = sess
            .history
            .last()
            .map_or(true, |last| last != &page.final_url);
        if should_push {
            sess.history.push(page.final_url.clone());
            while sess.history.len() > MAX_HISTORY_PER_SESSION {
                sess.history.remove(0);
            }
        }
    }
    sess.current_url = page.final_url;
    sess.status = page.status;
    sess.title = page.title;
    sess.html = page.html;
    sess.elements = page.elements;
    sess.forms = page.forms;
    sess.input_values.clear();
    sess.scroll_y = 0.0;
    if reset_cursor {
        sess.cursor_x_pct = 0.0;
        sess.cursor_y_pct = 0.0;
        sess.last_selector = None;
    }
    touch_session(sess, seq);
}

fn build_cookie_header(url: &Url, cookies: &[StoredCookie]) -> Option<String> {
    let host = url
        .host_str()
        .map(|h| h.trim_end_matches('.').to_ascii_lowercase())?;
    let is_https = url.scheme() == "https";
    let req_path = url.path();
    let mut pairs = Vec::new();
    for c in cookies {
        if c.secure && !is_https {
            continue;
        }
        if !domain_matches_cookie(&host, &c.domain) {
            continue;
        }
        if !path_matches_cookie(req_path, &c.path) {
            continue;
        }
        pairs.push(format!("{}={}", c.name, c.value));
    }
    if pairs.is_empty() {
        None
    } else {
        Some(pairs.join("; "))
    }
}

fn domain_matches_cookie(host: &str, cookie_domain: &str) -> bool {
    if host == cookie_domain {
        return true;
    }
    host.ends_with(&format!(".{cookie_domain}"))
}

fn path_matches_cookie(req_path: &str, cookie_path: &str) -> bool {
    if cookie_path == "/" || req_path == cookie_path {
        return true;
    }
    if !req_path.starts_with(cookie_path) {
        return false;
    }
    cookie_path.ends_with('/')
        || req_path.as_bytes().get(cookie_path.len()) == Some(&b'/')
}

fn merge_response_cookies(sess: &mut BrowserSession, resp_url: &str, headers: &HeaderMap) {
    let Ok(parsed_url) = Url::parse(resp_url) else {
        return;
    };
    let Some(req_host) = parsed_url
        .host_str()
        .map(|h| h.trim_end_matches('.').to_ascii_lowercase())
    else {
        return;
    };

    for hv in headers.get_all(SET_COOKIE) {
        let Ok(raw) = hv.to_str() else { continue };
        let mut parts = raw.split(';');
        let Some(nv) = parts.next() else { continue };
        let Some((raw_name, raw_val)) = nv.split_once('=') else {
            continue;
        };
        let name = raw_name.trim().to_string();
        if name.is_empty() {
            continue;
        }
        let value = raw_val.trim().to_string();

        let mut domain = req_host.clone();
        let mut path = "/".to_string();
        let mut secure = false;
        let mut delete_cookie = false;

        for attr in parts {
            let trimmed = attr.trim();
            if trimmed.is_empty() {
                continue;
            }
            if let Some((ak, av)) = trimmed.split_once('=') {
                let k = ak.trim().to_ascii_lowercase();
                let v = av.trim();
                match k.as_str() {
                    "domain" => {
                        let d = v.trim_start_matches('.').trim_end_matches('.').to_ascii_lowercase();
                        if !d.is_empty() && domain_matches_cookie(&req_host, &d) {
                            domain = d;
                        } else if !d.is_empty() {
                            delete_cookie = true;
                        }
                    }
                    "path" => {
                        if v.starts_with('/') {
                            path = v.to_string();
                        }
                    }
                    "max-age" => {
                        if let Ok(secs) = v.parse::<i64>() {
                            if secs <= 0 {
                                delete_cookie = true;
                            }
                        }
                    }
                    _ => {}
                }
            } else if trimmed.eq_ignore_ascii_case("secure") {
                secure = true;
            }
        }

        sess.cookies
            .retain(|c| !(c.name == name && c.domain == domain && c.path == path));

        if !delete_cookie {
            while sess.cookies.len() >= MAX_COOKIES_PER_SESSION {
                sess.cookies.remove(0);
            }
            sess.cookies.push(StoredCookie {
                name,
                value,
                domain,
                path,
                secure,
            });
        }
    }
}

fn build_submit_plan(sess: &BrowserSession, clicked: &SnapshotElement) -> Result<SubmitPlan> {
    let (form_idx, form) = if clicked.tag == "form" {
        let idx = sess
            .forms
            .iter()
            .position(|f| f.form_ref == clicked.ref_id)
            .ok_or_else(|| {
                AgentError::NotFound(format!("form '{}' not found", clicked.ref_id))
            })?;
        (idx, &sess.forms[idx])
    } else {
        let idx = clicked.form_idx.ok_or_else(|| {
            AgentError::InvalidInput(format!(
                "element '{}' is not associated with a <form>",
                clicked.ref_id
            ))
        })?;
        (idx, &sess.forms[idx])
    };

    let raw_action = clicked
        .formaction
        .as_deref()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or(form.action.as_str());
    let target_base = if raw_action.trim().is_empty() {
        sess.current_url.clone()
    } else {
        resolve_url_against(Some(&sess.current_url), raw_action)?
    };

    let method_str = clicked
        .formmethod
        .as_deref()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or(form.method.as_str())
        .trim()
        .to_ascii_uppercase();
    let is_post = method_str == "POST";

    let mut pairs: Vec<(String, String)> = Vec::new();
    for elem in &sess.elements {
        if elem.form_idx != Some(form_idx) || elem.disabled {
            continue;
        }
        let Some(ref field_name) = elem.name else {
            continue;
        };
        if field_name.is_empty() {
            continue;
        }
        match elem.tag.as_str() {
            "button" => {
                if elem.ref_id == clicked.ref_id
                    && matches!(
                        elem.input_type.as_deref(),
                        None | Some("submit") | Some("")
                    )
                {
                    pairs.push((
                        field_name.clone(),
                        elem.value.clone().unwrap_or_default(),
                    ));
                }
            }
            "input" => match elem.input_type.as_deref().unwrap_or("text") {
                "submit" | "image" => {
                    if elem.ref_id == clicked.ref_id {
                        pairs.push((
                            field_name.clone(),
                            elem.value.clone().unwrap_or_default(),
                        ));
                    }
                }
                "button" | "reset" => {}
                "checkbox" | "radio" => {
                    let is_checked = match sess.input_values.get(&elem.ref_id) {
                        Some(v) => !matches!(
                            v.trim().to_ascii_lowercase().as_str(),
                            "" | "false" | "0" | "off"
                        ),
                        None => elem.checked,
                    };
                    if is_checked {
                        let val = sess
                            .input_values
                            .get(&elem.ref_id)
                            .filter(|v| {
                                !matches!(
                                    v.trim().to_ascii_lowercase().as_str(),
                                    "true" | "false"
                                )
                            })
                            .cloned()
                            .or_else(|| elem.value.clone())
                            .unwrap_or_else(|| "on".to_string());
                        pairs.push((field_name.clone(), val));
                    }
                }
                _ => {
                    let val = sess
                        .input_values
                        .get(&elem.ref_id)
                        .cloned()
                        .or_else(|| elem.value.clone())
                        .unwrap_or_default();
                    pairs.push((field_name.clone(), val));
                }
            },
            "textarea" | "select" => {
                let val = sess
                    .input_values
                    .get(&elem.ref_id)
                    .cloned()
                    .or_else(|| elem.value.clone())
                    .unwrap_or_default();
                pairs.push((field_name.clone(), val));
            }
            _ => {}
        }
    }

    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for (k, v) in &pairs {
        serializer.append_pair(k, v);
    }
    let encoded = serializer.finish();

    if is_post {
        Ok(SubmitPlan {
            target_url: target_base,
            method: reqwest::Method::POST,
            body: Some(encoded.into_bytes()),
            content_type: Some("application/x-www-form-urlencoded"),
            cookies: sess.cookies.clone(),
        })
    } else {
        let mut parsed = Url::parse(&target_base)
            .map_err(|e| AgentError::InvalidInput(format!("invalid form action URL: {e}")))?;
        if !encoded.is_empty() {
            parsed.set_query(Some(&encoded));
        }
        Ok(SubmitPlan {
            target_url: parsed.to_string(),
            method: reqwest::Method::GET,
            body: None,
            content_type: None,
            cookies: sess.cookies.clone(),
        })
    }
}

fn strip_non_visible_html(html: &str) -> String {
    static COMMENTS_RE: OnceLock<Regex> = OnceLock::new();
    static SCRIPT_RE: OnceLock<Regex> = OnceLock::new();
    static STYLE_RE: OnceLock<Regex> = OnceLock::new();
    static NOSCRIPT_RE: OnceLock<Regex> = OnceLock::new();

    let comments_re = COMMENTS_RE.get_or_init(|| {
        RegexBuilder::new(r"(?s)<!--.*?-->")
            .build()
            .expect("valid regex")
    });
    let script_re = SCRIPT_RE.get_or_init(|| {
        RegexBuilder::new(r"(?is)<script\b[^>]*>.*?</script\s*>")
            .build()
            .expect("valid regex")
    });
    let style_re = STYLE_RE.get_or_init(|| {
        RegexBuilder::new(r"(?is)<style\b[^>]*>.*?</style\s*>")
            .build()
            .expect("valid regex")
    });
    let noscript_re = NOSCRIPT_RE.get_or_init(|| {
        RegexBuilder::new(r"(?is)<noscript\b[^>]*>.*?</noscript\s*>")
            .build()
            .expect("valid regex")
    });

    let s1 = comments_re.replace_all(html, " ");
    let s2 = script_re.replace_all(&s1, " ");
    let s3 = style_re.replace_all(&s2, " ");
    noscript_re.replace_all(&s3, " ").into_owned()
}

fn extract_html_title(cleaned_html: &str) -> Option<String> {
    static TITLE_RE: OnceLock<Regex> = OnceLock::new();
    let title_re = TITLE_RE.get_or_init(|| {
        RegexBuilder::new(r"(?is)<title\b[^>]*>(.*?)</title\s*>")
            .build()
            .expect("valid regex")
    });
    title_re
        .captures(cleaned_html)
        .and_then(|c| c.get(1))
        .map(|m| collapse_ws(&decode_html_entities(&strip_all_tags(m.as_str()))))
        .filter(|s| !s.is_empty())
}

struct FlowLayoutEstimator {
    doc_len: usize,
    inline_x: f32,
    last_row_y: f32,
    same_row_y: Option<f32>,
    in_form: bool,
    row_dirty: bool,
}

impl FlowLayoutEstimator {
    fn new(doc_len: usize) -> Self {
        Self {
            doc_len: doc_len.max(1),
            inline_x: 6.0,
            last_row_y: 0.0,
            same_row_y: None,
            in_form: false,
            row_dirty: false,
        }
    }

    fn base_x(&self) -> f32 {
        if self.in_form {
            12.0
        } else {
            6.0
        }
    }

    fn doc_y(&self, tag_start: usize) -> f32 {
        let ratio = (tag_start as f32 / self.doc_len as f32).clamp(0.0, 1.0);
        4.0 + ratio * 84.0
    }

    fn newline(&mut self) {
        if self.row_dirty {
            self.inline_x = self.base_x();
            self.same_row_y = None;
            self.row_dirty = false;
        }
    }

    fn exit_form(&mut self) {
        self.in_form = false;
        self.newline();
    }

    fn place_form(&mut self, tag_start: usize) -> (f32, f32, f32, f32) {
        self.newline();
        self.in_form = true;
        self.inline_x = self.base_x();
        self.same_row_y = None;
        self.row_dirty = false;
        let w = 82.0_f32;
        let h = 24.0_f32;
        let x = 6.0_f32;
        let y = clamp_pct(
            self.doc_y(tag_start)
                .max(if self.last_row_y > 0.0 {
                    self.last_row_y + 0.6
                } else {
                    4.0
                })
                .min(100.0 - h),
        );
        self.last_row_y = y;
        (x, y, w, h)
    }

    fn place_stacked(&mut self, tag_start: usize, w: f32, h: f32) -> (f32, f32, f32, f32) {
        self.newline();
        let w_c = clamp_pct(w);
        let h_c = clamp_pct(h);
        let x = clamp_pct(self.base_x());
        let min_y = if self.last_row_y > 0.0 {
            self.last_row_y + 0.6
        } else {
            4.0
        };
        let y = clamp_pct(self.doc_y(tag_start).max(min_y).min((100.0 - h_c).max(0.0)));
        self.last_row_y = y;
        self.inline_x = self.base_x();
        self.same_row_y = None;
        self.row_dirty = false;
        (x, y, w_c, h_c)
    }

    fn place_inline(&mut self, tag_start: usize, w: f32, h: f32) -> (f32, f32, f32, f32) {
        let w_c = clamp_pct(w);
        let h_c = clamp_pct(h);
        let base_x = self.base_x();
        if let Some(row_y) = self.same_row_y {
            if self.inline_x + w_c <= 94.0 {
                let x = clamp_pct(self.inline_x);
                self.inline_x = x + w_c + 2.5;
                self.row_dirty = true;
                return (x, row_y, w_c, h_c);
            }
        }
        let min_y = if self.last_row_y > 0.0 {
            self.last_row_y + 0.6
        } else {
            4.0
        };
        let y = clamp_pct(self.doc_y(tag_start).max(min_y).min((100.0 - h_c).max(0.0)));
        let x = clamp_pct(base_x);
        self.last_row_y = y;
        self.same_row_y = Some(y);
        self.inline_x = x + w_c + 2.5;
        self.row_dirty = true;
        (x, y, w_c, h_c)
    }

    fn place_hidden(&self, tag_start: usize) -> (f32, f32, f32, f32) {
        (0.0, clamp_pct(self.doc_y(tag_start)), 0.0, 0.0)
    }
}

fn is_block_layout_tag(tag: &str) -> bool {
    matches!(
        tag,
        "p" | "div"
            | "li"
            | "tr"
            | "h1"
            | "h2"
            | "h3"
            | "h4"
            | "h5"
            | "h6"
            | "section"
            | "article"
            | "header"
            | "nav"
            | "footer"
            | "main"
            | "aside"
            | "fieldset"
            | "table"
            | "ul"
            | "ol"
            | "br"
            | "hr"
            | "label"
    )
}

fn parse_html_snapshot(html: &str) -> (Option<String>, Vec<SnapshotElement>, Vec<ParsedForm>) {
    let cleaned = strip_non_visible_html(html);
    let title = extract_html_title(&cleaned);
    let bytes = cleaned.as_bytes();
    let len = bytes.len();

    let mut elements: Vec<SnapshotElement> = Vec::new();
    let mut forms: Vec<ParsedForm> = Vec::new();
    let mut form_id_map: HashMap<String, usize> = HashMap::new();
    let mut current_form_idx: Option<usize> = None;
    let mut layout = FlowLayoutEstimator::new(len);
    let mut pos = 0usize;
    let mut ref_counter = 0usize;

    while pos < len {
        let Some(rel_lt) = cleaned[pos..].find('<') else {
            break;
        };
        let tag_start = pos + rel_lt;
        if tag_start + 1 >= len {
            break;
        }
        let after_lt = &cleaned[tag_start + 1..];
        if after_lt.starts_with('!') || after_lt.starts_with('?') {
            if let Some(rel_gt) = after_lt.find('>') {
                pos = tag_start + 1 + rel_gt + 1;
            } else {
                break;
            }
            continue;
        }

        let Some(tag_end) = find_tag_close(cleaned.as_str(), tag_start + 1) else {
            break;
        };
        let raw_inside = cleaned[tag_start + 1..tag_end].trim();
        pos = tag_end + 1;

        if raw_inside.is_empty() {
            continue;
        }
        if let Some(closing) = raw_inside.strip_prefix('/') {
            let close_name = closing
                .split(|c: char| c.is_ascii_whitespace() || c == '/')
                .next()
                .unwrap_or("")
                .to_ascii_lowercase();
            if close_name == "form" {
                current_form_idx = None;
                layout.exit_form();
            } else if is_block_layout_tag(&close_name) {
                layout.newline();
            }
            continue;
        }

        let self_closing = raw_inside.ends_with('/');
        let without_slash = if self_closing {
            raw_inside[..raw_inside.len() - 1].trim_end()
        } else {
            raw_inside
        };
        let (tag_name_raw, attr_str) = split_tag_name_and_attrs(without_slash);
        let tag_name = tag_name_raw.to_ascii_lowercase();
        if !matches!(
            tag_name.as_str(),
            "form" | "a" | "button" | "input" | "textarea" | "select"
        ) {
            if is_block_layout_tag(&tag_name) {
                layout.newline();
            }
            continue;
        }

        let attrs = parse_html_attributes(attr_str);

        if tag_name == "form" {
            let ref_id = if elements.len() < MAX_SNAPSHOT_ELEMENTS {
                ref_counter += 1;
                format!("e{ref_counter}")
            } else {
                format!("form_{}", forms.len() + 1)
            };
            let id_attr = attrs.get("id").cloned().filter(|s| !s.is_empty());
            let action = attrs.get("action").cloned().unwrap_or_default();
            let method = attrs
                .get("method")
                .map(|m| {
                    if m.trim().eq_ignore_ascii_case("post") {
                        "POST".to_string()
                    } else {
                        "GET".to_string()
                    }
                })
                .unwrap_or_else(|| "GET".to_string());
            let idx = forms.len();
            if let Some(ref id_val) = id_attr {
                form_id_map.insert(id_val.clone(), idx);
            }
            forms.push(ParsedForm {
                form_ref: ref_id.clone(),
                action: action.clone(),
                method: method.clone(),
            });
            current_form_idx = Some(idx);

            if elements.len() < MAX_SNAPSHOT_ELEMENTS {
                let (x_pct, y_pct, w_pct, h_pct) = layout.place_form(tag_start);
                elements.push(SnapshotElement {
                    ref_id: ref_id.clone(),
                    tag: "form".into(),
                    role: "form".into(),
                    text: format!("{method} {}", if action.is_empty() { "." } else { &action }),
                    href: if action.is_empty() {
                        None
                    } else {
                        Some(action)
                    },
                    input_type: Some(method),
                    name: attrs.get("name").cloned().filter(|s| !s.is_empty()),
                    value: None,
                    placeholder: None,
                    checked: false,
                    disabled: false,
                    formaction: None,
                    formmethod: None,
                    form_idx: Some(idx),
                    form_ref: Some(ref_id),
                    x_pct,
                    y_pct,
                    w_pct,
                    h_pct,
                });
            }
            continue;
        }

        if elements.len() >= MAX_SNAPSHOT_ELEMENTS {
            continue;
        }

        let target_form_idx = attrs
            .get("form")
            .and_then(|fid| form_id_map.get(fid).copied())
            .or(current_form_idx);
        let form_ref = target_form_idx.and_then(|i| forms.get(i).map(|f| f.form_ref.clone()));

        match tag_name.as_str() {
            "a" => {
                let inner_html = if self_closing {
                    String::new()
                } else {
                    let (inner, next_pos) = read_until_closing_tag(&cleaned, pos, "a");
                    pos = next_pos;
                    inner
                };
                let label = non_empty_label(
                    &inner_html,
                    &attrs,
                    attrs.get("href").map(String::as_str).unwrap_or(""),
                );
                let est_w = (label.chars().count() as f32 * 1.1 + 6.0).clamp(8.0, 48.0);
                let (x_pct, y_pct, w_pct, h_pct) = layout.place_inline(tag_start, est_w, 4.2);
                ref_counter += 1;
                elements.push(SnapshotElement {
                    ref_id: format!("e{ref_counter}"),
                    tag: "a".into(),
                    role: "link".into(),
                    text: label,
                    href: attrs.get("href").cloned(),
                    input_type: None,
                    name: attrs.get("name").cloned().filter(|s| !s.is_empty()),
                    value: None,
                    placeholder: None,
                    checked: false,
                    disabled: false,
                    formaction: None,
                    formmethod: None,
                    form_idx: target_form_idx,
                    form_ref,
                    x_pct,
                    y_pct,
                    w_pct,
                    h_pct,
                });
            }
            "button" => {
                let inner_html = if self_closing {
                    String::new()
                } else {
                    let (inner, next_pos) = read_until_closing_tag(&cleaned, pos, "button");
                    pos = next_pos;
                    inner
                };
                let btn_type = attrs
                    .get("type")
                    .map(|s| s.trim().to_ascii_lowercase())
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| "submit".to_string());
                let label = non_empty_label(
                    &inner_html,
                    &attrs,
                    attrs.get("value").map(String::as_str).unwrap_or("button"),
                );
                let est_w = (label.chars().count() as f32 * 1.2 + 10.0).clamp(14.0, 40.0);
                let (x_pct, y_pct, w_pct, h_pct) = layout.place_inline(tag_start, est_w, 5.5);
                ref_counter += 1;
                elements.push(SnapshotElement {
                    ref_id: format!("e{ref_counter}"),
                    tag: "button".into(),
                    role: "button".into(),
                    text: label,
                    href: None,
                    input_type: Some(btn_type),
                    name: attrs.get("name").cloned().filter(|s| !s.is_empty()),
                    value: attrs.get("value").cloned(),
                    placeholder: None,
                    checked: false,
                    disabled: attrs.contains_key("disabled"),
                    formaction: attrs.get("formaction").cloned(),
                    formmethod: attrs.get("formmethod").cloned(),
                    form_idx: target_form_idx,
                    form_ref,
                    x_pct,
                    y_pct,
                    w_pct,
                    h_pct,
                });
            }
            "input" => {
                let input_type = attrs
                    .get("type")
                    .map(|s| s.trim().to_ascii_lowercase())
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| "text".to_string());
                let role = match input_type.as_str() {
                    "submit" | "button" | "reset" | "image" => "button",
                    "checkbox" => "checkbox",
                    "radio" => "radio",
                    _ => "textbox",
                };
                let label = attrs
                    .get("aria-label")
                    .or_else(|| attrs.get("placeholder"))
                    .or_else(|| attrs.get("name"))
                    .or_else(|| attrs.get("value"))
                    .cloned()
                    .unwrap_or_default();
                let collapsed = collapse_ws(&label);
                let (x_pct, y_pct, w_pct, h_pct) = match input_type.as_str() {
                    "hidden" => layout.place_hidden(tag_start),
                    "checkbox" | "radio" => layout.place_inline(tag_start, 3.5, 3.5),
                    "submit" | "button" | "reset" | "image" => {
                        let est_w =
                            (collapsed.chars().count() as f32 * 1.2 + 10.0).clamp(14.0, 38.0);
                        layout.place_inline(tag_start, est_w, 5.5)
                    }
                    _ => {
                        let est_w =
                            (collapsed.chars().count() as f32 * 0.9 + 36.0).clamp(36.0, 72.0);
                        layout.place_stacked(tag_start, est_w, 5.5)
                    }
                };
                ref_counter += 1;
                elements.push(SnapshotElement {
                    ref_id: format!("e{ref_counter}"),
                    tag: "input".into(),
                    role: role.into(),
                    text: collapsed,
                    href: None,
                    input_type: Some(input_type),
                    name: attrs.get("name").cloned().filter(|s| !s.is_empty()),
                    value: attrs.get("value").cloned(),
                    placeholder: attrs.get("placeholder").cloned(),
                    checked: attrs.contains_key("checked"),
                    disabled: attrs.contains_key("disabled"),
                    formaction: attrs.get("formaction").cloned(),
                    formmethod: attrs.get("formmethod").cloned(),
                    form_idx: target_form_idx,
                    form_ref,
                    x_pct,
                    y_pct,
                    w_pct,
                    h_pct,
                });
            }
            "textarea" => {
                let inner_raw = if self_closing {
                    String::new()
                } else {
                    let (inner, next_pos) = read_until_closing_tag(&cleaned, pos, "textarea");
                    pos = next_pos;
                    inner
                };
                let initial_val = decode_html_entities(&inner_raw);
                let label = attrs
                    .get("aria-label")
                    .or_else(|| attrs.get("placeholder"))
                    .or_else(|| attrs.get("name"))
                    .cloned()
                    .unwrap_or_default();
                let (x_pct, y_pct, w_pct, h_pct) = layout.place_stacked(tag_start, 72.0, 14.0);
                ref_counter += 1;
                elements.push(SnapshotElement {
                    ref_id: format!("e{ref_counter}"),
                    tag: "textarea".into(),
                    role: "textbox".into(),
                    text: collapse_ws(&label),
                    href: None,
                    input_type: Some("textarea".into()),
                    name: attrs.get("name").cloned().filter(|s| !s.is_empty()),
                    value: if initial_val.is_empty() {
                        None
                    } else {
                        Some(initial_val)
                    },
                    placeholder: attrs.get("placeholder").cloned(),
                    checked: false,
                    disabled: attrs.contains_key("disabled"),
                    formaction: None,
                    formmethod: None,
                    form_idx: target_form_idx,
                    form_ref,
                    x_pct,
                    y_pct,
                    w_pct,
                    h_pct,
                });
            }
            "select" => {
                let inner_html = if self_closing {
                    String::new()
                } else {
                    let (inner, next_pos) = read_until_closing_tag(&cleaned, pos, "select");
                    pos = next_pos;
                    inner
                };
                let selected_val = extract_select_default(&inner_html);
                let label = attrs
                    .get("aria-label")
                    .or_else(|| attrs.get("name"))
                    .cloned()
                    .unwrap_or_default();
                let collapsed = collapse_ws(&label);
                let est_w = (collapsed.chars().count() as f32 * 1.1 + 16.0).clamp(22.0, 56.0);
                let (x_pct, y_pct, w_pct, h_pct) = layout.place_stacked(tag_start, est_w, 5.5);
                ref_counter += 1;
                elements.push(SnapshotElement {
                    ref_id: format!("e{ref_counter}"),
                    tag: "select".into(),
                    role: "combobox".into(),
                    text: collapsed,
                    href: None,
                    input_type: Some("select".into()),
                    name: attrs.get("name").cloned().filter(|s| !s.is_empty()),
                    value: selected_val,
                    placeholder: None,
                    checked: false,
                    disabled: attrs.contains_key("disabled"),
                    formaction: None,
                    formmethod: None,
                    form_idx: target_form_idx,
                    form_ref,
                    x_pct,
                    y_pct,
                    w_pct,
                    h_pct,
                });
            }
            _ => {}
        }
    }

    (title, elements, forms)
}

fn find_tag_close(s: &str, start: usize) -> Option<usize> {
    let bytes = s.as_bytes();
    let mut i = start;
    let mut quote: Option<u8> = None;
    while i < bytes.len() {
        let b = bytes[i];
        match quote {
            Some(q) => {
                if b == q {
                    quote = None;
                }
            }
            None => {
                if b == b'"' || b == b'\'' {
                    quote = Some(b);
                } else if b == b'>' {
                    return Some(i);
                }
            }
        }
        i += 1;
    }
    None
}

fn split_tag_name_and_attrs(raw: &str) -> (&str, &str) {
    let trimmed = raw.trim_start();
    match trimmed.find(|c: char| c.is_ascii_whitespace()) {
        Some(idx) => (&trimmed[..idx], trimmed[idx..].trim()),
        None => (trimmed, ""),
    }
}

fn parse_html_attributes(attr_str: &str) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    let bytes = attr_str.as_bytes();
    let len = bytes.len();
    let mut i = 0usize;

    while i < len {
        while i < len && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= len {
            break;
        }
        let name_start = i;
        while i < len && !bytes[i].is_ascii_whitespace() && bytes[i] != b'=' {
            i += 1;
        }
        let name = attr_str[name_start..i].trim().to_ascii_lowercase();
        while i < len && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        let val = if i < len && bytes[i] == b'=' {
            i += 1;
            while i < len && bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            if i < len && (bytes[i] == b'"' || bytes[i] == b'\'') {
                let q = bytes[i];
                i += 1;
                let v_start = i;
                while i < len && bytes[i] != q {
                    i += 1;
                }
                let raw_val = &attr_str[v_start..i];
                if i < len {
                    i += 1;
                }
                decode_html_entities(raw_val)
            } else {
                let v_start = i;
                while i < len && !bytes[i].is_ascii_whitespace() {
                    i += 1;
                }
                decode_html_entities(&attr_str[v_start..i])
            }
        } else {
            String::new()
        };
        if !name.is_empty() {
            map.insert(name, val);
        }
    }

    map
}

fn read_until_closing_tag(html: &str, start: usize, tag_name: &str) -> (String, usize) {
    let needle = format!("</{tag_name}");
    let lower = html[start..].to_ascii_lowercase();
    let mut search_from = 0usize;
    while let Some(rel) = lower[search_from..].find(&needle) {
        let after = search_from + rel + needle.len();
        let next_ch = lower.as_bytes().get(after).copied();
        if matches!(next_ch, Some(b'>' | b' ' | b'\t' | b'\r' | b'\n')) {
            let abs_end = start + search_from + rel;
            let close_gt = html[abs_end..].find('>').map(|g| abs_end + g + 1).unwrap_or(html.len());
            return (html[start..abs_end].to_string(), close_gt);
        }
        search_from = after;
    }
    (html[start..].to_string(), html.len())
}

fn extract_select_default(inner_html: &str) -> Option<String> {
    static OPT_RE: OnceLock<Regex> = OnceLock::new();
    let opt_re = OPT_RE.get_or_init(|| {
        RegexBuilder::new(r"(?is)<option\b([^>]*)>(.*?)</option\s*>")
            .build()
            .expect("valid regex")
    });
    let mut first_val: Option<String> = None;
    for cap in opt_re.captures_iter(inner_html) {
        let attr_s = cap.get(1).map(|m| m.as_str()).unwrap_or("");
        let label = cap
            .get(2)
            .map(|m| collapse_ws(&decode_html_entities(&strip_all_tags(m.as_str()))))
            .unwrap_or_default();
        let attrs = parse_html_attributes(attr_s);
        let val = attrs.get("value").cloned().unwrap_or(label);
        if first_val.is_none() {
            first_val = Some(val.clone());
        }
        if attrs.contains_key("selected") {
            return Some(val);
        }
    }
    first_val
}

fn non_empty_label(inner_html: &str, attrs: &BTreeMap<String, String>, fallback: &str) -> String {
    let text = collapse_ws(&decode_html_entities(&strip_all_tags(inner_html)));
    if !text.is_empty() {
        return text;
    }
    for key in ["aria-label", "title", "alt"] {
        if let Some(v) = attrs.get(key) {
            let cleaned = collapse_ws(v);
            if !cleaned.is_empty() {
                return cleaned;
            }
        }
    }
    collapse_ws(fallback)
}

fn strip_all_tags(s: &str) -> String {
    static TAG_RE: OnceLock<Regex> = OnceLock::new();
    let tag_re = TAG_RE.get_or_init(|| {
        RegexBuilder::new(r"(?s)<[^>]+>")
            .build()
            .expect("valid regex")
    });
    tag_re.replace_all(s, " ").into_owned()
}

fn extract_readable_text(html: &str) -> String {
    static BLOCK_RE: OnceLock<Regex> = OnceLock::new();
    let no_vis = strip_non_visible_html(html);
    let block_re = BLOCK_RE.get_or_init(|| {
        RegexBuilder::new(
            r"(?i)</?(p|div|br|li|tr|h[1-6]|section|article|header|footer|nav|main|aside|blockquote|pre|form|table|thead|tbody|ul|ol)\b[^>]*>",
        )
        .build()
        .expect("valid regex")
    });
    let with_newlines = block_re.replace_all(&no_vis, "\n");
    let stripped = strip_all_tags(&with_newlines);
    let decoded = decode_html_entities(&stripped);
    let normalized = decoded
        .lines()
        .map(collapse_ws)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n");

    if normalized.chars().count() <= MAX_PAGE_TEXT_CHARS {
        normalized
    } else {
        let mut out: String = normalized.chars().take(MAX_PAGE_TEXT_CHARS).collect();
        out.push_str("\n...[truncated]");
        out
    }
}

fn collapse_ws(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn decode_html_entities(s: &str) -> String {
    s.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&apos;", "'")
        .replace("&nbsp;", " ")
}

fn element_to_json(elem: &SnapshotElement, filled_val: Option<&String>) -> Value {
    let mut obj = serde_json::Map::new();
    obj.insert("ref".into(), Value::String(elem.ref_id.clone()));
    obj.insert("tag".into(), Value::String(elem.tag.clone()));
    obj.insert("role".into(), Value::String(elem.role.clone()));
    if !elem.text.is_empty() {
        obj.insert("text".into(), Value::String(elem.text.clone()));
    }
    if let Some(ref href) = elem.href {
        obj.insert("href".into(), Value::String(href.clone()));
    }
    if let Some(ref it) = elem.input_type {
        obj.insert("type".into(), Value::String(it.clone()));
    }
    if let Some(ref name) = elem.name {
        obj.insert("name".into(), Value::String(name.clone()));
    }
    let is_toggle_control = elem.tag == "input"
        && matches!(elem.input_type.as_deref(), Some("checkbox" | "radio"));
    let is_checked = if is_toggle_control {
        match filled_val {
            Some(v) => !matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "" | "false" | "0" | "off"
            ),
            None => elem.checked,
        }
    } else {
        elem.checked
    };
    let effective_val = if is_toggle_control
        && filled_val.is_some_and(|v| {
            matches!(v.trim().to_ascii_lowercase().as_str(), "true" | "false")
        })
    {
        elem.value.as_ref()
    } else {
        filled_val.or(elem.value.as_ref())
    };
    if let Some(val) = effective_val {
        let masked = if elem.input_type.as_deref() == Some("password") {
            "***".to_string()
        } else {
            val.clone()
        };
        obj.insert("value".into(), Value::String(masked));
    }
    if let Some(ref ph) = elem.placeholder {
        obj.insert("placeholder".into(), Value::String(ph.clone()));
    }
    if is_checked {
        obj.insert("checked".into(), Value::Bool(true));
    }
    if elem.disabled {
        obj.insert("disabled".into(), Value::Bool(true));
    }
    if let Some(ref fref) = elem.form_ref {
        obj.insert("form_ref".into(), Value::String(fref.clone()));
    }
    obj.insert("x_pct".into(), json!(clamp_pct(elem.x_pct)));
    obj.insert("y_pct".into(), json!(clamp_pct(elem.y_pct)));
    obj.insert("w_pct".into(), json!(clamp_pct(elem.w_pct)));
    obj.insert("h_pct".into(), json!(clamp_pct(elem.h_pct)));
    Value::Object(obj)
}

const PREVIEW_REF_HIGHLIGHT_STYLE: &str = "<style data-ps5-preview=\"1\">[data-ps5-ref]{outline:1.5px dashed rgba(56,189,248,0.65);outline-offset:2px;border-radius:3px;}[data-ps5-ref][data-ps5-active=\"true\"]{outline:2px solid rgba(34,211,238,0.95);background-color:rgba(34,211,238,0.10);}</style>";

fn sanitize_html_for_preview(raw_html: &str, active_selector: Option<&str>) -> String {
    let slice_len = floor_char_boundary_lossy(raw_html.as_bytes(), MAX_STORED_HTML_BYTES);
    let src = &raw_html[..slice_len];
    let body_budget = MAX_PREVIEW_HTML_BYTES.saturating_sub(PREVIEW_REF_HIGHLIGHT_STYLE.len() + 64);

    let mut out = String::with_capacity(src.len().min(body_budget) + PREVIEW_REF_HIGHLIGHT_STYLE.len());
    let mut style_injected = false;
    let mut noscript_depth = 0usize;
    let mut interactive_count = 0usize;
    let mut pos = 0usize;
    let len = src.len();

    while pos < len && out.len() < body_budget {
        let Some(rel_lt) = src[pos..].find('<') else {
            push_capped(&mut out, &src[pos..], body_budget);
            break;
        };
        let tag_start = pos + rel_lt;
        if tag_start > pos {
            push_capped(&mut out, &src[pos..tag_start], body_budget);
            if out.len() >= body_budget {
                break;
            }
        }
        if tag_start + 1 >= len {
            push_capped(&mut out, "&lt;", body_budget);
            break;
        }

        let after_lt = &src[tag_start + 1..];
        if after_lt.starts_with("!--") {
            if let Some(rel_end) = after_lt.find("-->") {
                pos = tag_start + 1 + rel_end + 3;
            } else {
                break;
            }
            continue;
        }
        if after_lt.starts_with('!') || after_lt.starts_with('?') {
            if let Some(rel_gt) = after_lt.find('>') {
                let decl = &after_lt[..rel_gt];
                if decl.to_ascii_lowercase().starts_with("!doctype") {
                    push_capped(&mut out, "<!DOCTYPE html>", body_budget);
                }
                pos = tag_start + 1 + rel_gt + 1;
            } else {
                break;
            }
            continue;
        }

        let Some(tag_end) = find_tag_close(src, tag_start + 1) else {
            break;
        };
        let raw_inside = src[tag_start + 1..tag_end].trim();
        pos = tag_end + 1;

        if raw_inside.is_empty() {
            continue;
        }

        if let Some(closing) = raw_inside.strip_prefix('/') {
            let close_name = extract_tag_ident(closing);
            if close_name.is_empty() {
                continue;
            }
            if close_name == "noscript" {
                noscript_depth = noscript_depth.saturating_sub(1);
            }
            if is_preview_banned_subtree_tag(&close_name) || is_preview_banned_void_tag(&close_name) || close_name == "meta" {
                continue;
            }
            if close_name == "head" && !style_injected {
                out.push_str(PREVIEW_REF_HIGHLIGHT_STYLE);
                style_injected = true;
            }
            if close_name == "textarea" {
                continue;
            }
            push_capped(&mut out, &format!("</{close_name}>"), body_budget);
            continue;
        }

        let self_closing = has_unquoted_self_close(raw_inside);
        let without_slash = if self_closing {
            raw_inside[..raw_inside.len() - 1].trim_end()
        } else {
            raw_inside
        };
        let (tag_name, attr_str) = split_tag_name_and_attrs_lenient(without_slash);
        if tag_name.is_empty() {
            continue;
        }

        if tag_name == "script" {
            let (_inner, next_pos) = read_until_closing_tag(src, pos, "script");
            pos = next_pos;
            continue;
        }

        if is_preview_banned_subtree_tag(&tag_name) {
            if !self_closing {
                pos = skip_tag_subtree_raw(src, pos, &tag_name);
            }
            continue;
        }

        if is_preview_banned_void_tag(&tag_name) {
            continue;
        }

        let ordered_attrs = parse_ordered_html_attributes(attr_str);

        if tag_name == "meta" {
            let has_http_equiv = ordered_attrs.iter().any(|(k, _)| k == "http-equiv");
            if has_http_equiv {
                continue;
            }
            let safe_meta_attrs: Vec<(String, Option<String>)> = ordered_attrs
                .into_iter()
                .filter(|(k, _)| matches!(k.as_str(), "charset" | "name" | "content"))
                .collect();
            let has_charset = safe_meta_attrs.iter().any(|(k, _)| k == "charset");
            let is_safe_named = safe_meta_attrs.iter().any(|(k, v)| {
                k == "name"
                    && matches!(
                        v.as_deref().unwrap_or("").trim().to_ascii_lowercase().as_str(),
                        "viewport" | "description" | "color-scheme" | "theme-color"
                    )
            });
            if has_charset || is_safe_named {
                emit_sanitized_tag(&mut out, "meta", &safe_meta_attrs, self_closing, body_budget);
            }
            continue;
        }

        if tag_name == "style" {
            if self_closing {
                continue;
            }
            let (inner_css, next_pos) = read_until_closing_tag(src, pos, "style");
            pos = next_pos;
            let safe_css = sanitize_css_text(&inner_css);
            if !safe_css.is_empty() {
                let mut block = String::with_capacity(safe_css.len() + 16);
                block.push_str("<style>");
                block.push_str(&safe_css);
                block.push_str("</style>");
                push_capped(&mut out, &block, body_budget);
            }
            continue;
        }

        let mut safe_attrs = sanitize_preview_attributes(&tag_name, &ordered_attrs);

        if noscript_depth == 0
            && interactive_count < MAX_SNAPSHOT_ELEMENTS
            && matches!(
                tag_name.as_str(),
                "form" | "a" | "button" | "input" | "textarea" | "select"
            )
        {
            interactive_count += 1;
            let ref_id = format!("e{interactive_count}");
            let is_active = active_selector == Some(ref_id.as_str());
            safe_attrs.push(("data-ps5-ref".into(), Some(ref_id)));
            if is_active {
                safe_attrs.push(("data-ps5-active".into(), Some("true".into())));
            }
        }

        if tag_name == "noscript" && !self_closing {
            noscript_depth += 1;
        }

        if tag_name == "textarea" && !self_closing {
            let (inner_raw, next_pos) = read_until_closing_tag(src, pos, "textarea");
            pos = next_pos;
            emit_sanitized_tag(&mut out, "textarea", &safe_attrs, false, body_budget);
            push_capped(&mut out, &escape_html_text(&inner_raw), body_budget);
            push_capped(&mut out, "</textarea>", body_budget);
            continue;
        }

        emit_sanitized_tag(&mut out, &tag_name, &safe_attrs, self_closing, body_budget);
    }

    if !style_injected {
        let mut combined = String::with_capacity(PREVIEW_REF_HIGHLIGHT_STYLE.len() + out.len());
        combined.push_str(PREVIEW_REF_HIGHLIGHT_STYLE);
        combined.push_str(&out);
        if combined.len() > MAX_PREVIEW_HTML_BYTES {
            let cut = floor_char_boundary_lossy(combined.as_bytes(), MAX_PREVIEW_HTML_BYTES);
            combined.truncate(cut);
        }
        return combined;
    }

    if out.len() > MAX_PREVIEW_HTML_BYTES {
        let cut = floor_char_boundary_lossy(out.as_bytes(), MAX_PREVIEW_HTML_BYTES);
        out.truncate(cut);
    }
    out
}

fn push_capped(dst: &mut String, piece: &str, max_bytes: usize) {
    if dst.len() >= max_bytes || piece.is_empty() {
        return;
    }
    let avail = max_bytes - dst.len();
    if piece.len() <= avail {
        dst.push_str(piece);
    } else {
        let cut = floor_char_boundary_lossy(piece.as_bytes(), avail);
        dst.push_str(&piece[..cut]);
    }
}

fn extract_tag_ident(s: &str) -> String {
    let trimmed = s.trim_start();
    let end = trimmed
        .find(|c: char| !c.is_ascii_alphanumeric() && c != '-' && c != ':')
        .unwrap_or(trimmed.len());
    trimmed[..end]
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-')
        .collect::<String>()
        .to_ascii_lowercase()
}

fn split_tag_name_and_attrs_lenient(raw: &str) -> (String, &str) {
    let trimmed = raw.trim_start();
    let end = trimmed
        .find(|c: char| !c.is_ascii_alphanumeric() && c != '-' && c != ':')
        .unwrap_or(trimmed.len());
    let name = trimmed[..end]
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-')
        .collect::<String>()
        .to_ascii_lowercase();
    let rest = trimmed[end..].trim_start_matches(['/', ' ', '\t', '\r', '\n']);
    (name, rest)
}

fn has_unquoted_self_close(raw_inside: &str) -> bool {
    let trimmed = raw_inside.trim_end();
    if !trimmed.ends_with('/') {
        return false;
    }
    let bytes = trimmed.as_bytes();
    let mut quote: Option<u8> = None;
    for (i, &b) in bytes.iter().enumerate() {
        match quote {
            Some(q) => {
                if b == q {
                    quote = None;
                }
            }
            None => {
                if b == b'"' || b == b'\'' {
                    quote = Some(b);
                } else if i + 1 == bytes.len() && b == b'/' {
                    return true;
                }
            }
        }
    }
    false
}

fn is_preview_banned_subtree_tag(tag: &str) -> bool {
    matches!(
        tag,
        "script"
            | "iframe"
            | "frame"
            | "frameset"
            | "object"
            | "applet"
            | "svg"
            | "math"
            | "foreignobject"
            | "portal"
            | "template"
    )
}

fn is_preview_banned_void_tag(tag: &str) -> bool {
    matches!(
        tag,
        "base" | "embed" | "link" | "param" | "source" | "track"
    )
}

fn skip_tag_subtree_raw(html: &str, start: usize, tag_name: &str) -> usize {
    let close_needle = format!("</{tag_name}");
    let open_needle = format!("<{tag_name}");
    let lower = html[start..].to_ascii_lowercase();
    let bytes = lower.as_bytes();
    let mut depth = 1usize;
    let mut i = 0usize;

    while i < bytes.len() {
        let Some(rel_lt) = lower[i..].find('<') else {
            return html.len();
        };
        let pos = i + rel_lt;
        if lower[pos..].starts_with(&close_needle) {
            let after = pos + close_needle.len();
            if matches!(
                bytes.get(after).copied(),
                Some(b'>' | b' ' | b'\t' | b'\r' | b'\n' | b'/')
            ) {
                let close_gt = html[start + pos..]
                    .find('>')
                    .map(|g| start + pos + g + 1)
                    .unwrap_or(html.len());
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return close_gt;
                }
                i = close_gt.saturating_sub(start);
                continue;
            }
        } else if lower[pos..].starts_with(&open_needle) {
            let after = pos + open_needle.len();
            if matches!(
                bytes.get(after).copied(),
                Some(b'>' | b' ' | b'\t' | b'\r' | b'\n' | b'/')
            ) {
                if let Some(gt) = find_tag_close(html, start + pos + 1) {
                    let inside = html[start + pos + 1..gt].trim();
                    if !has_unquoted_self_close(inside) {
                        depth += 1;
                    }
                    i = (gt + 1).saturating_sub(start);
                    continue;
                } else {
                    return html.len();
                }
            }
        }
        i = pos + 1;
    }

    html.len()
}

fn parse_ordered_html_attributes(attr_str: &str) -> Vec<(String, Option<String>)> {
    let mut out = Vec::new();
    let bytes = attr_str.as_bytes();
    let len = bytes.len();
    let mut i = 0usize;

    while i < len {
        while i < len && (bytes[i].is_ascii_whitespace() || bytes[i] == b'/') {
            i += 1;
        }
        if i >= len {
            break;
        }
        let name_start = i;
        while i < len
            && !bytes[i].is_ascii_whitespace()
            && bytes[i] != b'='
            && bytes[i] != b'/'
        {
            i += 1;
        }
        let raw_name = attr_str[name_start..i]
            .trim()
            .chars()
            .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
            .collect::<String>()
            .to_ascii_lowercase();
        while i < len && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        let val = if i < len && bytes[i] == b'=' {
            i += 1;
            while i < len && bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            if i < len && (bytes[i] == b'"' || bytes[i] == b'\'') {
                let q = bytes[i];
                i += 1;
                let v_start = i;
                while i < len && bytes[i] != q {
                    i += 1;
                }
                let raw_val = &attr_str[v_start..i];
                if i < len {
                    i += 1;
                }
                Some(decode_html_entities_extended(raw_val))
            } else {
                let v_start = i;
                while i < len && !bytes[i].is_ascii_whitespace() {
                    i += 1;
                }
                Some(decode_html_entities_extended(&attr_str[v_start..i]))
            }
        } else {
            None
        };
        if !raw_name.is_empty() {
            out.push((raw_name, val));
        }
    }

    out
}

fn sanitize_preview_attributes(
    tag_name: &str,
    attrs: &[(String, Option<String>)],
) -> Vec<(String, Option<String>)> {
    let mut out = Vec::with_capacity(attrs.len());
    let mut seen: HashSet<String> = HashSet::new();

    for (name, val_opt) in attrs {
        if name.is_empty() || !seen.insert(name.clone()) {
            continue;
        }
        if name.starts_with("on") {
            continue;
        }
        if matches!(
            name.as_str(),
            "srcdoc"
                | "formaction"
                | "formmethod"
                | "ping"
                | "http-equiv"
                | "autofocus"
                | "action"
        ) {
            continue;
        }
        if name.starts_with("data-ps5-") {
            continue;
        }
        if tag_name == "form" && name == "target" {
            continue;
        }

        if name == "style" {
            if let Some(raw_css) = val_opt {
                let safe_css = sanitize_css_text(raw_css);
                if !safe_css.is_empty() {
                    out.push((name.clone(), Some(safe_css)));
                }
            }
            continue;
        }

        if name == "srcset" {
            if let Some(raw_set) = val_opt {
                let mut safe_items = Vec::new();
                for item in raw_set.split(',') {
                    let trimmed = item.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    let url_part = trimmed.split_whitespace().next().unwrap_or("");
                    if is_safe_preview_uri("src", url_part) {
                        safe_items.push(trimmed.to_string());
                    }
                }
                if !safe_items.is_empty() {
                    out.push((name.clone(), Some(safe_items.join(", "))));
                }
            }
            continue;
        }

        if matches!(
            name.as_str(),
            "href" | "src" | "data" | "poster" | "background" | "cite"
        ) {
            if let Some(uri) = val_opt {
                if is_safe_preview_uri(name, uri) {
                    out.push((name.clone(), Some(uri.clone())));
                }
            }
            continue;
        }

        out.push((name.clone(), val_opt.clone()));
    }

    out
}

fn is_safe_preview_uri(attr_name: &str, raw_uri: &str) -> bool {
    let normalized: String = raw_uri
        .chars()
        .filter(|c| !c.is_ascii_control() && !c.is_ascii_whitespace())
        .collect::<String>()
        .to_ascii_lowercase();
    if normalized.is_empty() {
        return attr_name == "href";
    }
    if normalized.starts_with("javascript:")
        || normalized.starts_with("vbscript:")
        || normalized.starts_with("file:")
        || normalized.starts_with("data:text/html")
        || normalized.starts_with("data:image/svg+xml")
        || normalized.starts_with("data:application/")
    {
        return false;
    }
    if normalized.starts_with("data:") {
        if attr_name == "href" {
            return false;
        }
        return normalized.starts_with("data:image/png;")
            || normalized.starts_with("data:image/jpeg;")
            || normalized.starts_with("data:image/jpg;")
            || normalized.starts_with("data:image/gif;")
            || normalized.starts_with("data:image/webp;")
            || normalized.starts_with("data:image/bmp;")
            || normalized.starts_with("data:image/x-icon;");
    }
    if let Some(colon_idx) = normalized.find(':') {
        let before_colon = &normalized[..colon_idx];
        let is_scheme = !before_colon.is_empty()
            && before_colon
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
            && !normalized[..colon_idx].contains('/')
            && !normalized[..colon_idx].contains('?')
            && !normalized[..colon_idx].contains('#');
        if is_scheme {
            return matches!(before_colon, "http" | "https" | "blob");
        }
    }
    true
}

fn sanitize_css_text(raw_css: &str) -> String {
    static CSS_AT_IMPORT_RE: OnceLock<Regex> = OnceLock::new();
    static CSS_EXPR_RE: OnceLock<Regex> = OnceLock::new();
    static CSS_BINDING_RE: OnceLock<Regex> = OnceLock::new();
    static CSS_UNSAFE_URL_RE: OnceLock<Regex> = OnceLock::new();

    let import_re = CSS_AT_IMPORT_RE.get_or_init(|| {
        RegexBuilder::new(r"(?is)@import\b[^;]*;?")
            .build()
            .expect("valid regex")
    });
    let expr_re = CSS_EXPR_RE.get_or_init(|| {
        RegexBuilder::new(r"(?is)expression\s*\([^)]*\)")
            .build()
            .expect("valid regex")
    });
    let binding_re = CSS_BINDING_RE.get_or_init(|| {
        RegexBuilder::new(r"(?is)(-moz-binding|behavior)\s*:[^;]*;?")
            .build()
            .expect("valid regex")
    });
    let url_re = CSS_UNSAFE_URL_RE.get_or_init(|| {
        RegexBuilder::new(r"(?is)url\s*\(([^)]*)\)")
            .build()
            .expect("valid regex")
    });

    let no_lt = raw_css.replace('<', "");
    let s1 = import_re.replace_all(&no_lt, "");
    let s2 = expr_re.replace_all(&s1, "none");
    let s3 = binding_re.replace_all(&s2, "");
    let s4 = url_re.replace_all(&s3, |caps: &regex::Captures<'_>| {
        let inner = caps
            .get(1)
            .map(|m| m.as_str())
            .unwrap_or("")
            .trim()
            .trim_matches(|c| c == '\'' || c == '"')
            .trim();
        let norm: String = inner
            .chars()
            .filter(|c| !c.is_ascii_control() && !c.is_ascii_whitespace())
            .collect::<String>()
            .to_ascii_lowercase();
        let safe_data = norm.starts_with("data:image/png;")
            || norm.starts_with("data:image/jpeg;")
            || norm.starts_with("data:image/jpg;")
            || norm.starts_with("data:image/gif;")
            || norm.starts_with("data:image/webp;");
        let dangerous = norm.starts_with("javascript:")
            || norm.starts_with("vbscript:")
            || norm.starts_with("http:")
            || norm.starts_with("https:")
            || norm.starts_with("file:")
            || (norm.starts_with("data:") && !safe_data);
        if dangerous {
            "none".to_string()
        } else {
            caps.get(0)
                .map(|m| m.as_str().to_string())
                .unwrap_or_else(|| "none".to_string())
        }
    });
    s4.trim().to_string()
}

fn emit_sanitized_tag(
    out: &mut String,
    tag_name: &str,
    attrs: &[(String, Option<String>)],
    self_closing: bool,
    budget: usize,
) {
    let mut buf = String::with_capacity(tag_name.len() + 32);
    buf.push('<');
    buf.push_str(tag_name);
    for (k, v_opt) in attrs {
        buf.push(' ');
        buf.push_str(k);
        if let Some(v) = v_opt {
            buf.push_str("=\"");
            buf.push_str(&escape_html_attr(v));
            buf.push('"');
        }
    }
    if self_closing {
        buf.push_str(" />");
    } else {
        buf.push('>');
    }
    push_capped(out, &buf, budget);
}

fn escape_html_attr(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '"' => out.push_str("&quot;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            _ => out.push(ch),
        }
    }
    out
}

fn escape_html_text(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            _ => out.push(ch),
        }
    }
    out
}

fn decode_html_entities_extended(s: &str) -> String {
    let base = decode_html_entities(s)
        .replace("&colon;", ":")
        .replace("&tab;", "\t")
        .replace("&newline;", "\n")
        .replace("&sol;", "/");
    if !base.contains("&#") {
        return base;
    }
    let mut out = String::with_capacity(base.len());
    let bytes = base.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        if base[i..].starts_with("&#") {
            let after_prefix = &base[i + 2..];
            if let Some(semi_rel) = after_prefix.find(';') {
                if semi_rel > 0 && semi_rel <= 10 {
                    let body = &after_prefix[..semi_rel];
                    let parsed = if let Some(hex) = body.strip_prefix('x').or_else(|| body.strip_prefix('X')) {
                        u32::from_str_radix(hex, 16).ok()
                    } else {
                        body.parse::<u32>().ok()
                    };
                    if let Some(ch) = parsed.and_then(char::from_u32) {
                        out.push(ch);
                        i += 2 + semi_rel + 1;
                        continue;
                    }
                }
            }
        }
        let ch = base[i..].chars().next().unwrap();
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

fn format_elements_table(
    elements: &[SnapshotElement],
    input_values: &HashMap<String, String>,
) -> String {
    let mut lines = Vec::with_capacity(elements.len());
    for e in elements {
        let mut parts = vec![format!("[ref={}]", e.ref_id), format!("<{}>", e.tag)];
        if let Some(ref t) = e.input_type {
            parts.push(format!("type={t}"));
        }
        if let Some(ref n) = e.name {
            parts.push(format!("name={n}"));
        }
        if !e.text.is_empty() {
            parts.push(format!("\"{}\"", e.text));
        }
        if let Some(ref h) = e.href {
            parts.push(format!("-> {h}"));
        }
        let val = input_values.get(&e.ref_id).or(e.value.as_ref());
        if let Some(v) = val {
            if !v.is_empty() {
                let shown = if e.input_type.as_deref() == Some("password") {
                    "***"
                } else {
                    v.as_str()
                };
                parts.push(format!("value=\"{shown}\""));
            }
        }
        lines.push(parts.join(" "));
    }
    lines.join("\n")
}

fn render_snapshot_markdown(sess: &BrowserSession) -> String {
    let title = sess.title.as_deref().unwrap_or("Untitled Page");
    let table = format_elements_table(&sess.elements, &sess.input_values);
    let body_text = extract_readable_text(&sess.html);
    let mut out = format!("# {title}\n\nURL: {}\nStatus: {}\n", sess.current_url, sess.status);
    if !table.is_empty() {
        out.push_str("\n## Interactive Elements\n\n");
        out.push_str(&table);
        out.push('\n');
    }
    if !body_text.is_empty() {
        out.push_str("\n## Page Text\n\n");
        out.push_str(&body_text);
        out.push('\n');
    }
    out
}

fn sanitize_explicit_filename(raw: &str, fallback: &str) -> Result<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(fallback.to_string());
    }
    if trimmed.contains('\0') {
        return Err(AgentError::InvalidInput(
            "filename contains illegal NUL byte".into(),
        ));
    }
    if trimmed.contains("..") || trimmed.contains('/') || trimmed.contains('\\') || trimmed.contains(':') {
        return Err(AgentError::PermissionDenied(
            "filename must be a single safe basename without path separators or traversal".into(),
        ));
    }
    let safe: String = trimmed
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect();
    let trimmed_dots = safe.trim_matches('.');
    if trimmed_dots.is_empty() {
        Ok(fallback.to_string())
    } else {
        Ok(safe)
    }
}

fn derive_filename_from_url(raw_url: &str) -> String {
    let candidate = Url::parse(raw_url)
        .ok()
        .and_then(|u| {
            u.path_segments()
                .and_then(|segs| segs.last().map(str::to_string))
        })
        .unwrap_or_default();
    let cleaned: String = candidate
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect();
    let stripped = cleaned.trim_matches('.');
    if stripped.is_empty() {
        "download.bin".to_string()
    } else {
        cleaned
    }
}

fn derive_snapshot_name(raw_url: &str, session_id: &str) -> String {
    let host_part = Url::parse(raw_url)
        .ok()
        .and_then(|u| u.host_str().map(str::to_string))
        .unwrap_or_else(|| "page".to_string());
    let combined = format!("{host_part}_{session_id}");
    let cleaned: String = combined
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect();
    let trimmed = cleaned.trim_matches('_');
    if trimmed.is_empty() {
        "snapshot".to_string()
    } else {
        trimmed.to_string()
    }
}

fn write_checked_bytes(target: &Path, bytes: &[u8]) -> Result<()> {
    if let Ok(meta) = fs::symlink_metadata(target) {
        if meta.file_type().is_symlink() {
            return Err(AgentError::PermissionDenied(
                "symlinks are forbidden inside sandbox".into(),
            ));
        }
        if meta.is_dir() {
            return Err(AgentError::InvalidInput(
                "cannot overwrite directory as file".into(),
            ));
        }
    }
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent).map_err(|e| AgentError::Tool {
            tool_id: "browser".into(),
            message: format!("failed to create parent directory: {e}"),
        })?;
        if let Ok(pmeta) = fs::symlink_metadata(parent) {
            if pmeta.file_type().is_symlink() {
                return Err(AgentError::PermissionDenied(
                    "symlinks are forbidden inside sandbox".into(),
                ));
            }
        }
    }
    fs::write(target, bytes).map_err(|e| AgentError::Tool {
        tool_id: "browser".into(),
        message: format!("failed to write file '{}': {e}", target.display()),
    })
}

fn relative_display_path(root: &Path, target: &Path, fallback: &str) -> String {
    target
        .strip_prefix(root)
        .ok()
        .map(|rel| {
            rel.components()
                .filter_map(|c| match c {
                    std::path::Component::Normal(s) => Some(s.to_string_lossy().into_owned()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("/")
        })
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| fallback.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        extract::Query,
        http::HeaderMap as AxumHeaders,
        response::{Html, IntoResponse},
        routing::{get, post},
        Router,
    };
    use std::collections::HashMap as StdMap;
    use tokio::net::TcpListener;

    fn test_cfg(workspace: &Path) -> ToolsConfig {
        let mut cfg = ToolsConfig::default();
        cfg.allowed_paths = vec![workspace.to_string_lossy().into_owned()];
        cfg.allowed_domains = vec!["*".into()];
        cfg.network_access = true;
        cfg.allow_local_network = true;
        cfg.block_private_lan_web_requests = false;
        cfg.remote_browser_url = String::new();
        cfg
    }

    async fn spawn_test_site() -> (String, tokio::task::JoinHandle<()>) {
        let app = Router::new()
            .route(
                "/",
                get(|| async {
                    Html(
                        r#"<!doctype html>
<html>
<head><title>Home Page</title></head>
<body>
  <h1>Welcome</h1>
  <p>Explore the sandbox browser.</p>
  <a href="/login">Sign In</a>
  <a href="/files/report.txt">Download Report</a>
</body>
</html>"#,
                    )
                }),
            )
            .route(
                "/login",
                get(|| async {
                    Html(
                        r#"<!doctype html>
<html>
<head><title>Login</title></head>
<body>
  <form action="/submit-login" method="POST">
    <input type="text" name="username" placeholder="Username" />
    <input type="password" name="password" placeholder="Password" />
    <select name="role">
      <option value="viewer">Viewer</option>
      <option value="admin" selected>Admin</option>
    </select>
    <textarea name="notes">hello</textarea>
    <button type="submit">Log In</button>
  </form>
</body>
</html>"#,
                    )
                }),
            )
            .route(
                "/submit-login",
                post(|body: String| async move {
                    let params: StdMap<String, String> =
                        url::form_urlencoded::parse(body.as_bytes())
                            .into_owned()
                            .collect();
                    let user = params.get("username").cloned().unwrap_or_default();
                    let role = params.get("role").cloned().unwrap_or_default();
                    let notes = params.get("notes").cloned().unwrap_or_default();
                    (
                        [(
                            axum::http::header::SET_COOKIE,
                            format!("session_tok=ok_{user}; Path=/"),
                        )],
                        Html(format!(
                            "<html><head><title>Dashboard</title></head><body><p>User: {user} Role: {role} Notes: {notes}</p><a href=\"/profile\">Profile</a></body></html>"
                        )),
                    )
                }),
            )
            .route(
                "/profile",
                get(|headers: AxumHeaders| async move {
                    let cookie = headers
                        .get(axum::http::header::COOKIE)
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("");
                    Html(format!(
                        "<html><head><title>Profile</title></head><body><p>Cookie: {cookie}</p></body></html>"
                    ))
                }),
            )
            .route(
                "/search",
                get(|Query(q): Query<StdMap<String, String>>| async move {
                    let term = q.get("q").cloned().unwrap_or_default();
                    Html(format!(
                        "<html><head><title>Search</title></head><body><p>Result for {term}</p></body></html>"
                    ))
                }),
            )
            .route(
                "/files/report.txt",
                get(|| async {
                    (
                        [(axum::http::header::CONTENT_TYPE, "text/plain")],
                        "PS5 Sandbox Report Contents",
                    )
                        .into_response()
                }),
            )
            .route(
                "/challenge",
                get(|| async {
                    (
                        axum::http::StatusCode::FORBIDDEN,
                        [(axum::http::header::HeaderName::from_static("cf-mitigated"), "challenge")],
                        Html("<html><body>Just a moment...</body></html>"),
                    )
                        .into_response()
                }),
            );

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });
        (format!("http://{addr}"), handle)
    }

    #[tokio::test]
    async fn browser_navigation_snapshot_fill_submit_cookies_back_download_and_save() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = test_cfg(dir.path());
        crate::web::ensure_rustls_ring_provider();
        let client = reqwest::Client::new();
        let mgr = SandboxBrowserManager::new();
        let (base_url, server) = spawn_test_site().await;

        // 1. Open home page & inspect snapshot refs
        let opened = mgr
            .execute(&json!({"action": "open", "url": format!("{base_url}/")}), &cfg, &client)
            .await
            .expect("open home");
        assert_eq!(opened["ok"], true);
        let sid = opened["session_id"].as_str().unwrap().to_string();

        let snap = mgr
            .execute(&json!({"action": "snapshot", "session_id": sid}), &cfg, &client)
            .await
            .expect("snapshot home");
        let elems = snap["elements"].as_array().unwrap();
        assert_eq!(elems[0]["ref"], "e1");
        assert_eq!(elems[0]["tag"], "a");
        assert_eq!(elems[0]["href"], "/login");
        assert!(snap["snapshot"].as_str().unwrap().contains("[ref=e1]"));

        // 2. Links listing
        let links = mgr
            .execute(&json!({"action": "links", "session_id": sid}), &cfg, &client)
            .await
            .expect("links");
        assert_eq!(links["count"], 2);

        // 3. Click link to /login
        let clicked = mgr
            .execute(&json!({"action": "click", "session_id": sid, "ref": "e1"}), &cfg, &client)
            .await
            .expect("click login");
        assert_eq!(clicked["title"], "Login");
        assert_eq!(clicked["history_len"], 2);

        // 4. Snapshot login form, fill username/password/notes, submit form
        let login_snap = mgr
            .execute(&json!({"action": "snapshot", "session_id": sid}), &cfg, &client)
            .await
            .expect("snapshot login");
        let login_elems = login_snap["elements"].as_array().unwrap();
        let form_ref = login_elems
            .iter()
            .find(|e| e["tag"] == "form")
            .unwrap()["ref"]
            .as_str()
            .unwrap()
            .to_string();
        let user_ref = login_elems
            .iter()
            .find(|e| e["name"] == "username")
            .unwrap()["ref"]
            .as_str()
            .unwrap()
            .to_string();
        let pass_ref = login_elems
            .iter()
            .find(|e| e["name"] == "password")
            .unwrap()["ref"]
            .as_str()
            .unwrap()
            .to_string();
        let notes_ref = login_elems
            .iter()
            .find(|e| e["name"] == "notes")
            .unwrap()["ref"]
            .as_str()
            .unwrap()
            .to_string();

        mgr.execute(
            &json!({"action": "fill", "session_id": sid, "ref": user_ref, "value": "alice"}),
            &cfg,
            &client,
        )
        .await
        .expect("fill user");
        mgr.execute(
            &json!({"action": "fill", "session_id": sid, "ref": pass_ref, "value": "secret"}),
            &cfg,
            &client,
        )
        .await
        .expect("fill pass");
        mgr.execute(
            &json!({"action": "fill", "session_id": sid, "ref": notes_ref, "value": "updated"}),
            &cfg,
            &client,
        )
        .await
        .expect("fill notes");

        let submitted = mgr
            .execute(
                &json!({"action": "submit", "session_id": sid, "ref": form_ref}),
                &cfg,
                &client,
            )
            .await
            .expect("submit");
        assert_eq!(submitted["title"], "Dashboard");
        assert_eq!(submitted["cookie_count"], 1);

        // 5. Verify cookie persists on next navigation (/profile via click)
        let dash_snap = mgr
            .execute(&json!({"action": "snapshot", "session_id": sid}), &cfg, &client)
            .await
            .expect("dash snap");
        assert!(dash_snap["text"]
            .as_str()
            .unwrap()
            .contains("User: alice Role: admin Notes: updated"));

        let prof_click = mgr
            .execute(&json!({"action": "click", "session_id": sid, "ref": "e1"}), &cfg, &client)
            .await
            .expect("click profile");
        assert_eq!(prof_click["title"], "Profile");

        let prof_snap = mgr
            .execute(&json!({"action": "snapshot", "session_id": sid}), &cfg, &client)
            .await
            .expect("profile snap");
        assert!(prof_snap["text"]
            .as_str()
            .unwrap()
            .contains("session_tok=ok_alice"));

        // 6. Back navigates to previous URL (/submit-login)
        let backed = mgr
            .execute(&json!({"action": "back", "session_id": sid}), &cfg, &client)
            .await
            .expect("back");
        assert!(backed["url"].as_str().unwrap().ends_with("/submit-login"));

        // 7. Save snapshot (.html & .md) into sandbox/snapshots/
        let saved = mgr
            .execute(
                &json!({"action": "save_snapshot", "session_id": sid, "name": "dash_snap"}),
                &cfg,
                &client,
            )
            .await
            .expect("save_snapshot");
        assert_eq!(saved["html_path"], "sandbox/snapshots/dash_snap.html");
        assert_eq!(saved["markdown_path"], "sandbox/snapshots/dash_snap.md");
        assert!(dir.path().join("sandbox/snapshots/dash_snap.html").is_file());
        assert!(dir.path().join("sandbox/snapshots/dash_snap.md").is_file());

        // 8. Download into sandbox/downloads/ and block traversal filenames
        let dl = mgr
            .execute(
                &json!({
                    "action": "download",
                    "session_id": sid,
                    "url": format!("{base_url}/files/report.txt"),
                }),
                &cfg,
                &client,
            )
            .await
            .expect("download");
        assert_eq!(dl["path"], "sandbox/downloads/report.txt");
        let dl_content =
            fs::read_to_string(dir.path().join("sandbox/downloads/report.txt")).unwrap();
        assert_eq!(dl_content, "PS5 Sandbox Report Contents");

        let bad_dl = mgr
            .execute(
                &json!({
                    "action": "download",
                    "session_id": sid,
                    "url": format!("{base_url}/files/report.txt"),
                    "filename": "../escape.txt"
                }),
                &cfg,
                &client,
            )
            .await
            .expect_err("traversal filename must be denied");
        assert!(matches!(bad_dl, AgentError::PermissionDenied(_)));

        // 9. Challenge gate returns blocked_by_challenge without bypass
        let gated = mgr
            .execute(
                &json!({"action": "open", "url": format!("{base_url}/challenge")}),
                &cfg,
                &client,
            )
            .await
            .expect("challenge returns structured block");
        assert_eq!(gated["ok"], false);
        assert_eq!(gated["blocked_by_challenge"], true);

        server.abort();
    }

    #[tokio::test]
    async fn browser_enforces_16_session_cap_network_toggle_and_close() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut cfg = test_cfg(dir.path());
        crate::web::ensure_rustls_ring_provider();
        let client = reqwest::Client::new();
        let mgr = SandboxBrowserManager::new();
        let (base_url, server) = spawn_test_site().await;

        let mut first_sid = String::new();
        for i in 0..18 {
            let res = mgr
                .execute(
                    &json!({"action": "open", "url": format!("{base_url}/search?q={i}")}),
                    &cfg,
                    &client,
                )
                .await
                .expect("open session");
            if i == 0 {
                first_sid = res["session_id"].as_str().unwrap().to_string();
            }
        }

        let listed = mgr.list_sessions();
        assert_eq!(listed["count"], MAX_SESSIONS);
        let active_ids: Vec<&str> = listed["sessions"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|s| s["session_id"].as_str())
            .collect();
        assert!(!active_ids.contains(&first_sid.as_str()));

        // Close last active session
        let last_sid = active_ids[0].to_string();
        let closed = mgr
            .execute(&json!({"action": "close", "session_id": last_sid}), &cfg, &client)
            .await
            .expect("close");
        assert_eq!(closed["closed"], true);
        assert_eq!(mgr.list_sessions()["count"], MAX_SESSIONS - 1);

        // network_access == false blocks network actions
        cfg.network_access = false;
        let denied = mgr
            .execute(&json!({"action": "open", "url": format!("{base_url}/")}), &cfg, &client)
            .await
            .expect_err("network_access=false must deny");
        assert!(matches!(denied, AgentError::PermissionDenied(_)));

        server.abort();
    }

    #[tokio::test]
    async fn browser_viewport_coords_cursor_hover_scroll_text_and_reset() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = test_cfg(dir.path());
        crate::web::ensure_rustls_ring_provider();
        let client = reqwest::Client::new();
        let mgr = SandboxBrowserManager::new();
        let (base_url, server) = spawn_test_site().await;

        let opened = mgr
            .execute(
                &json!({"action": "open", "url": format!("{base_url}/login")}),
                &cfg,
                &client,
            )
            .await
            .expect("open login");
        let sid = opened["session_id"].as_str().unwrap().to_string();
        assert_eq!(opened["cursor"]["scroll_y"], 0.0);
        assert_eq!(opened["cursor"]["x_pct"], 0.0);
        assert_eq!(opened["cursor"]["y_pct"], 0.0);
        assert_eq!(opened["cursor"]["last_action"], "open");

        let snap = mgr
            .execute(&json!({"action": "snapshot", "session_id": sid}), &cfg, &client)
            .await
            .expect("snapshot");
        let elems = snap["elements"].as_array().unwrap();
        assert!(elems.len() >= 4);
        for el in elems {
            let x = el["x_pct"].as_f64().unwrap();
            let y = el["y_pct"].as_f64().unwrap();
            let w = el["w_pct"].as_f64().unwrap();
            let h = el["h_pct"].as_f64().unwrap();
            assert!((0.0..=100.0).contains(&x), "x out of range: {x}");
            assert!((0.0..=100.0).contains(&y), "y out of range: {y}");
            assert!((0.0..=100.0).contains(&w), "w out of range: {w}");
            assert!((0.0..=100.0).contains(&h), "h out of range: {h}");
        }
        assert!(snap["snapshot"].as_str().unwrap().contains("[ref=e1]"));

        // Hover over username input moves cursor to its center
        let user_el = elems.iter().find(|e| e["name"] == "username").unwrap();
        let user_ref = user_el["ref"].as_str().unwrap().to_string();
        let expected_cx =
            ((user_el["x_pct"].as_f64().unwrap() + user_el["w_pct"].as_f64().unwrap() / 2.0) * 100.0)
                .round()
                / 100.0;
        let expected_cy =
            ((user_el["y_pct"].as_f64().unwrap() + user_el["h_pct"].as_f64().unwrap() / 2.0) * 100.0)
                .round()
                / 100.0;

        let hovered = mgr
            .execute(
                &json!({"action": "hover", "session_id": sid, "ref": user_ref}),
                &cfg,
                &client,
            )
            .await
            .expect("hover");
        assert_eq!(hovered["cursor"]["last_action"], "hover");
        assert_eq!(hovered["cursor"]["last_selector"], user_ref);
        assert!((hovered["cursor"]["x_pct"].as_f64().unwrap() - expected_cx).abs() < 0.05);
        assert!((hovered["cursor"]["y_pct"].as_f64().unwrap() - expected_cy).abs() < 0.05);

        // Text action returns readable wrapped content without wiping last_action
        let text_res = mgr
            .execute(&json!({"action": "text", "session_id": sid}), &cfg, &client)
            .await
            .expect("text");
        assert!(text_res["text"].as_str().unwrap().contains("Log In"));
        assert_eq!(text_res["cursor"]["last_action"], "hover");
        assert_eq!(text_res["cursor"]["last_selector"], user_ref);

        // Scroll actions clamp cleanly in 0..=100 and persist in list_sessions
        let scrolled_down = mgr
            .execute(
                &json!({"action": "scroll", "session_id": sid, "direction": "down", "amount": 40}),
                &cfg,
                &client,
            )
            .await
            .expect("scroll down");
        assert!((scrolled_down["scroll_y"].as_f64().unwrap() - 40.0).abs() < 0.05);
        assert_eq!(scrolled_down["cursor"]["scroll_y"], scrolled_down["scroll_y"]);

        let scrolled_y = mgr
            .execute(
                &json!({"action": "scroll", "session_id": sid, "y_pct": 140.0}),
                &cfg,
                &client,
            )
            .await
            .expect("scroll clamp");
        assert_eq!(scrolled_y["scroll_y"], 100.0);

        let scrolled_up = mgr
            .execute(
                &json!({"action": "scroll", "session_id": sid, "direction": "up", "amount": 25}),
                &cfg,
                &client,
            )
            .await
            .expect("scroll up");
        assert!((scrolled_up["scroll_y"].as_f64().unwrap() - 75.0).abs() < 0.05);

        let listed = mgr.list_sessions();
        let sess_cursor = &listed["sessions"][0]["cursor"];
        assert!((sess_cursor["scroll_y"].as_f64().unwrap() - 75.0).abs() < 0.05);
        assert_eq!(sess_cursor["last_action"], "scroll");

        // Navigating resets scroll and cursor to top
        let nav = mgr
            .execute(
                &json!({"action": "navigate", "session_id": sid, "url": format!("{base_url}/search?q=ps5")}),
                &cfg,
                &client,
            )
            .await
            .expect("navigate");
        assert_eq!(nav["cursor"]["scroll_y"], 0.0);
        assert_eq!(nav["cursor"]["x_pct"], 0.0);
        assert_eq!(nav["cursor"]["y_pct"], 0.0);
        assert_eq!(nav["cursor"]["last_action"], "navigate");

        // Scroll before going back, then back resets scroll and cursor
        mgr.execute(
            &json!({"action": "scroll", "session_id": sid, "direction": "bottom"}),
            &cfg,
            &client,
        )
        .await
        .expect("scroll bottom");
        let backed = mgr
            .execute(&json!({"action": "back", "session_id": sid}), &cfg, &client)
            .await
            .expect("back");
        assert_eq!(backed["cursor"]["scroll_y"], 0.0);
        assert_eq!(backed["cursor"]["x_pct"], 0.0);
        assert_eq!(backed["cursor"]["y_pct"], 0.0);
        assert_eq!(backed["cursor"]["last_action"], "back");

        server.abort();
    }

    #[test]
    fn browser_preview_html_sanitization_strips_dangerous_markup_and_caps_size() {
        let dirty = r#"<!DOCTYPE html>
<html>
<head>
  <title>Dirty Preview</title>
  <base href="https://evil.example/">
  <meta http-equiv="refresh" content="0;url=https://evil.example/">
  <meta charset="utf-8">
  <script>window.alert('xss1');</script>
  <style>
    body { color: #111; }
    @import url("https://evil.example/track.css");
    .x { width: expression(alert(1)); background: url("javascript:alert(2)"); }
  </style>
</head>
<body onload="alert('xss2')">
  <svg onload="alert('xss3')"><foreignObject><script>alert('xss4')</script></foreignObject></svg>
  <iframe src="https://evil.example/frame" srcdoc="<script>alert(5)</script>"></iframe>
  <object data="https://evil.example/obj"></object>
  <embed src="https://evil.example/emb">
  <form action="https://evil.example/steal" method="post">
    <input name="token" onfocus="alert('xss6')" value="ok">
    <a href="JaVaScRiPt:alert(7)">Bad Link 1</a>
    <a href="java&#x09;script:alert(8)">Bad Link 2</a>
    <a href="/dashboard">Safe Dashboard</a>
    <button type="submit" formaction="https://evil.example/btn" onclick="alert(9)">Submit</button>
  </form>
  <img src="data:text/html,<script>alert(10)</script>">
  <img src="data:image/png;base64,iVBORw0KGgo=" alt="okimg">
</body>
</html>"#;

        let sanitized = sanitize_html_for_preview(dirty, Some("e3"));
        let lower = sanitized.to_ascii_lowercase();
        assert!(!lower.contains("<script"));
        assert!(!lower.contains("<svg"));
        assert!(!lower.contains("<foreignobject"));
        assert!(!lower.contains("<iframe"));
        assert!(!lower.contains("<object"));
        assert!(!lower.contains("<embed"));
        assert!(!lower.contains("<base"));
        assert!(!lower.contains("http-equiv"));
        assert!(!lower.contains("onload="));
        assert!(!lower.contains("onfocus="));
        assert!(!lower.contains("onclick="));
        assert!(!lower.contains("javascript:"));
        assert!(!lower.contains("@import"));
        assert!(!lower.contains("expression("));
        assert!(!lower.contains("https://evil.example/steal"));
        assert!(!lower.contains("formaction="));
        assert!(!lower.contains("srcdoc="));
        assert!(!lower.contains("data:text/html"));

        assert!(sanitized.contains("body { color: #111; }"));
        assert!(sanitized.contains("href=\"/dashboard\""));
        assert!(sanitized.contains("data-ps5-ref=\"e1\""));
        assert!(sanitized.contains("data-ps5-ref=\"e3\""));
        assert!(sanitized.contains("data-ps5-active=\"true\""));
        assert!(sanitized.contains("[data-ps5-ref]"));
        assert!(sanitized.contains("data:image/png;base64,iVBORw0KGgo="));

        // Large payload gets capped cleanly <= MAX_PREVIEW_HTML_BYTES (128 KiB)
        let huge = format!("<p>{}</p>", "A".repeat(250 * 1024));
        let capped = sanitize_html_for_preview(&huge, None);
        assert!(capped.len() <= MAX_PREVIEW_HTML_BYTES);
    }

    #[tokio::test]
    async fn browser_emits_live_browser_activity_events_with_sanitized_urls() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = test_cfg(dir.path());
        crate::web::ensure_rustls_ring_provider();
        let client = reqwest::Client::new();
        let mgr = SandboxBrowserManager::new();
        let (tx, mut rx) = broadcast::channel(64);
        mgr.attach_events_tx(tx);

        let (base_url, server) = spawn_test_site().await;
        assert_eq!(
            sanitize_url_for_event("http://alice:topsecret@127.0.0.1:8080/path?token=secret#frag"),
            "http://127.0.0.1:8080/path"
        );
        let cred_url = base_url.replacen("http://", "http://alice:topsecret@", 1);
        let ctx = BrowserActivityContext {
            conversation_id: Some("conv-1".into()),
            task_id: Some("task-1".into()),
            agent_id: Some("programmer".into()),
        };

        // Opening URL with embedded userinfo fails SSRF check AND emits sanitized error URL
        let cred_err = mgr
            .execute_with_meta(
                &json!({"action": "open", "url": format!("{cred_url}/secret?token=topsecret#frag")}),
                &cfg,
                &client,
                Some(ctx.clone()),
            )
            .await
            .expect_err("embedded credentials must be denied");
        assert!(matches!(cred_err, AgentError::PermissionDenied(_)));

        let opened = mgr
            .execute_with_meta(
                &json!({"action": "open", "url": format!("{base_url}/?token=topsecret#frag")}),
                &cfg,
                &client,
                Some(ctx.clone()),
            )
            .await
            .expect("open");
        let sid = opened["session_id"].as_str().unwrap().to_string();

        mgr.execute_with_meta(
            &json!({"action": "hover", "session_id": sid, "ref": "e1"}),
            &cfg,
            &client,
            Some(ctx.clone()),
        )
        .await
        .expect("hover");

        mgr.execute_with_meta(
            &json!({"action": "scroll", "session_id": sid, "direction": "down"}),
            &cfg,
            &client,
            Some(ctx.clone()),
        )
        .await
        .expect("scroll");

        mgr.execute_with_meta(
            &json!({"action": "click", "session_id": sid, "ref": "e1"}),
            &cfg,
            &client,
            Some(ctx.clone()),
        )
        .await
        .expect("click");

        let _err = mgr
            .execute_with_meta(
                &json!({"action": "hover", "session_id": sid, "ref": "e999"}),
                &cfg,
                &client,
                Some(ctx.clone()),
            )
            .await
            .expect_err("missing ref");

        // preview helper returns sanitized HTML and supports None (most recent session)
        let (prev_sid, prev_url, prev_html) = mgr
            .sanitized_session_preview_html(None)
            .expect("preview html");
        assert_eq!(prev_sid, sid);
        assert!(prev_url.starts_with(&base_url));
        assert!(prev_html.contains("data-ps5-ref=\"e1\""));
        assert!(!prev_html.contains("action="));

        assert!(mgr.close_session(&sid));

        let mut captured = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            if let StreamEvent::BrowserActivity {
                conversation_id,
                task_id,
                agent_id,
                session_id,
                action,
                url,
                selector,
                x_pct,
                y_pct,
                scroll_y,
                status,
                ..
            } = ev
            {
                if let Some(ref u) = url {
                    assert!(
                        !u.contains("topsecret")
                            && !u.contains("alice:")
                            && !u.contains("token=")
                            && !u.contains("#frag"),
                        "credentials/query/fragment must never leak in BrowserActivity url: {u}"
                    );
                }
                captured.push((
                    conversation_id,
                    task_id,
                    agent_id,
                    session_id,
                    action,
                    selector,
                    x_pct,
                    y_pct,
                    scroll_y,
                    status,
                ));
            }
        }

        let statuses: Vec<&str> = captured.iter().map(|c| c.9.as_str()).collect();
        assert!(statuses.contains(&"navigating"), "statuses={statuses:?}");
        assert!(statuses.contains(&"opened"), "statuses={statuses:?}");
        assert!(statuses.contains(&"hovered"), "statuses={statuses:?}");
        assert!(statuses.contains(&"scrolled"), "statuses={statuses:?}");
        assert!(statuses.contains(&"clicked"), "statuses={statuses:?}");
        assert!(statuses.contains(&"error"), "statuses={statuses:?}");
        assert!(statuses.contains(&"closed"), "statuses={statuses:?}");

        let hovered_ev = captured.iter().find(|c| c.9 == "hovered").unwrap();
        assert_eq!(hovered_ev.0.as_deref(), Some("conv-1"));
        assert_eq!(hovered_ev.1.as_deref(), Some("task-1"));
        assert_eq!(hovered_ev.2.as_deref(), Some("programmer"));
        assert_eq!(hovered_ev.5.as_deref(), Some("e1"));
        assert!(hovered_ev.6.is_some() && hovered_ev.7.is_some());

        server.abort();
    }

    #[tokio::test]
    async fn browser_click_local_controls_and_error_cursor_position() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = test_cfg(dir.path());
        crate::web::ensure_rustls_ring_provider();
        let client = reqwest::Client::new();
        let mgr = SandboxBrowserManager::new();
        let (tx, mut rx) = broadcast::channel(64);
        mgr.attach_events_tx(tx);

        let hits = Arc::new(AtomicUsize::new(0));
        let hits_clone = hits.clone();
        let app = Router::new().route(
            "/controls",
            get(move || {
                let hits_inner = hits_clone.clone();
                async move {
                    hits_inner.fetch_add(1, Ordering::SeqCst);
                    Html(
                        r#"<!doctype html>
<html>
<head><title>Controls</title></head>
<body>
  <button id="act-btn" type="button">Launch Diagnostics</button>
  <form action="/unused" method="post">
    <input type="text" name="query" placeholder="Query">
    <input type="checkbox" name="agree" value="yes">
    <input type="radio" name="tier" value="a" checked>
    <input type="radio" name="tier" value="b">
    <button type="reset">Reset Form</button>
  </form>
</body>
</html>"#,
                    )
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });
        let url = format!("http://{addr}/controls");

        let opened = mgr
            .execute(&json!({"action": "open", "url": url}), &cfg, &client)
            .await
            .expect("open controls");
        let sid = opened["session_id"].as_str().unwrap().to_string();
        assert_eq!(hits.load(Ordering::SeqCst), 1);

        // 1. Click non-submitting <button type="button"> (method == "LOCAL", zero network calls)
        let clicked = mgr
            .execute(
                &json!({"action": "click", "session_id": sid, "ref": "e1"}),
                &cfg,
                &client,
            )
            .await
            .expect("click button type=button");
        assert_eq!(clicked["ok"], true);
        assert_eq!(clicked["method"], "LOCAL");
        assert_eq!(clicked["truncated"], false);
        assert!(clicked["cursor"]["x_pct"].as_f64().unwrap() > 0.0);
        assert!(clicked["cursor"]["y_pct"].as_f64().unwrap() > 0.0);
        assert_eq!(clicked["cursor"]["last_action"], "click");
        assert_eq!(clicked["cursor"]["last_selector"], "e1");
        assert_eq!(hits.load(Ordering::SeqCst), 1);

        // Drain events and verify local button emitted status == "clicked"
        let mut clicked_events = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            if let StreamEvent::BrowserActivity {
                selector,
                status,
                x_pct,
                y_pct,
                ..
            } = ev
            {
                if status == "clicked" {
                    clicked_events.push((selector, x_pct, y_pct));
                }
            }
        }
        assert_eq!(clicked_events.len(), 1);
        assert_eq!(clicked_events[0].0.as_deref(), Some("e1"));
        assert!(clicked_events[0].1.unwrap() > 0.0 && clicked_events[0].2.unwrap() > 0.0);

        // 2. Toggle checkbox, switch radio group, fill text, and reset form locally
        let snap = mgr
            .execute(&json!({"action": "snapshot", "session_id": sid}), &cfg, &client)
            .await
            .expect("snapshot");
        let elems = snap["elements"].as_array().unwrap();
        let text_ref = elems.iter().find(|e| e["name"] == "query").unwrap()["ref"]
            .as_str()
            .unwrap()
            .to_string();
        let check_ref = elems.iter().find(|e| e["type"] == "checkbox").unwrap()["ref"]
            .as_str()
            .unwrap()
            .to_string();
        let radio_a_ref = elems
            .iter()
            .find(|e| e["type"] == "radio" && e["value"] == "a")
            .unwrap()["ref"]
            .as_str()
            .unwrap()
            .to_string();
        let radio_b_ref = elems
            .iter()
            .find(|e| e["type"] == "radio" && e["value"] == "b")
            .unwrap()["ref"]
            .as_str()
            .unwrap()
            .to_string();
        let reset_ref = elems.iter().find(|e| e["type"] == "reset").unwrap()["ref"]
            .as_str()
            .unwrap()
            .to_string();

        mgr.execute(
            &json!({"action": "fill", "session_id": sid, "ref": text_ref, "value": "ps5"}),
            &cfg,
            &client,
        )
        .await
        .expect("fill query");

        let chk_click = mgr
            .execute(
                &json!({"action": "click", "session_id": sid, "ref": check_ref}),
                &cfg,
                &client,
            )
            .await
            .expect("toggle checkbox");
        assert_eq!(chk_click["method"], "LOCAL");

        let rb_click = mgr
            .execute(
                &json!({"action": "click", "session_id": sid, "ref": radio_b_ref}),
                &cfg,
                &client,
            )
            .await
            .expect("select radio b");
        assert_eq!(rb_click["method"], "LOCAL");

        let mid_snap = mgr
            .execute(&json!({"action": "snapshot", "session_id": sid}), &cfg, &client)
            .await
            .expect("mid snapshot");
        let mid_elems = mid_snap["elements"].as_array().unwrap();
        assert_eq!(
            mid_elems.iter().find(|e| e["ref"] == check_ref).unwrap()["checked"],
            true
        );
        assert!(mid_elems
            .iter()
            .find(|e| e["ref"] == radio_a_ref)
            .unwrap()
            .get("checked")
            .is_none());
        assert_eq!(
            mid_elems.iter().find(|e| e["ref"] == radio_b_ref).unwrap()["checked"],
            true
        );

        let rst_click = mgr
            .execute(
                &json!({"action": "click", "session_id": sid, "ref": reset_ref}),
                &cfg,
                &client,
            )
            .await
            .expect("reset form");
        assert_eq!(rst_click["method"], "LOCAL");
        assert_eq!(hits.load(Ordering::SeqCst), 1);

        let post_snap = mgr
            .execute(&json!({"action": "snapshot", "session_id": sid}), &cfg, &client)
            .await
            .expect("post reset snapshot");
        let post_elems = post_snap["elements"].as_array().unwrap();
        assert!(post_elems
            .iter()
            .find(|e| e["ref"] == text_ref)
            .unwrap()
            .get("value")
            .is_none());
        assert!(post_elems
            .iter()
            .find(|e| e["ref"] == check_ref)
            .unwrap()
            .get("checked")
            .is_none());
        assert_eq!(
            post_elems.iter().find(|e| e["ref"] == radio_a_ref).unwrap()["checked"],
            true
        );

        // 3. Failing click on non-clickable <input type="text"> still moves cursor & error event to element center
        while rx.try_recv().is_ok() {}
        let _bad = mgr
            .execute(
                &json!({"action": "click", "session_id": sid, "ref": text_ref}),
                &cfg,
                &client,
            )
            .await
            .expect_err("textbox is not clickable");

        let err_coords = loop {
            match rx.try_recv() {
                Ok(StreamEvent::BrowserActivity {
                    status,
                    selector,
                    x_pct,
                    y_pct,
                    ..
                }) if status == "error" => break (selector, x_pct, y_pct),
                Ok(_) => continue,
                Err(_) => panic!("expected error BrowserActivity"),
            }
        };
        assert_eq!(err_coords.0.as_deref(), Some(text_ref.as_str()));
        assert!(err_coords.1.unwrap() > 0.0 && err_coords.2.unwrap() > 0.0);

        server.abort();
    }
}
