use ps5_agent_config::ToolsConfig;
use ps5_agent_core::{new_id, now_iso, AgentError, Result};
use regex::{Regex, RegexBuilder};
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE, COOKIE, SET_COOKIE};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::Path;
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};
use url::Url;

const MAX_SESSIONS: usize = 16;
const SESSION_IDLE_TTL: Duration = Duration::from_secs(30 * 60);
const MAX_COOKIES_PER_SESSION: usize = 64;
const MAX_HISTORY_PER_SESSION: usize = 10;
const MAX_STORED_HTML_BYTES: usize = 512 * 1024;
const MAX_SNAPSHOT_ELEMENTS: usize = 128;
const MAX_PAGE_TEXT_CHARS: usize = 16_000;

#[derive(Debug, Default)]
pub struct SandboxBrowserManager {
    inner: Mutex<BrowserStore>,
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

impl SandboxBrowserManager {
    pub fn new() -> Self {
        Self::default()
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
        let mut store = self.lock_store();
        evict_idle_sessions(&mut store);
        store.sessions.remove(session_id.trim()).is_some()
    }

    pub async fn execute(
        &self,
        args: &Value,
        cfg: &ToolsConfig,
        _client: &reqwest::Client,
    ) -> Result<Value> {
        let action = args
            .get("action")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| AgentError::InvalidInput("missing required string 'action'".into()))?
            .to_ascii_lowercase();

        match action.as_str() {
            "sessions" => Ok(self.list_sessions()),
            "close" => {
                let sid = require_str(args, "session_id")?;
                let closed = self.close_session(sid);
                Ok(json!({
                    "ok": true,
                    "session_id": sid,
                    "closed": closed,
                }))
            }
            "snapshot" => self.handle_snapshot(args),
            "fill" => self.handle_fill(args),
            "links" => self.handle_links(args),
            "save_snapshot" => self.handle_save_snapshot(args, cfg),
            "open" | "navigate" => self.handle_open_or_navigate(args, cfg).await,
            "click" => self.handle_click(args, cfg).await,
            "submit" => self.handle_submit(args, cfg).await,
            "back" => self.handle_back(args, cfg).await,
            "download" => self.handle_download(args, cfg).await,
            other => Err(AgentError::InvalidInput(format!(
                "unsupported browser action '{other}'"
            ))),
        }
    }

    async fn handle_open_or_navigate(&self, args: &Value, cfg: &ToolsConfig) -> Result<Value> {
        ensure_network_allowed(cfg)?;
        let raw_url = require_str(args, "url")?;
        if let Some(remote_resp) = try_remote_browser_bridge(args, cfg).await {
            return Ok(remote_resp);
        }

        let requested_sid = optional_trimmed_str(args, "session_id").map(str::to_string);
        let (target_url, existing_cookies) = {
            let mut store = self.lock_store();
            evict_idle_sessions(&mut store);
            if let Some(ref sid) = requested_sid {
                if let Some(sess) = store.sessions.get(sid) {
                    let resolved = resolve_url_against(Some(&sess.current_url), raw_url)?;
                    (resolved, sess.cookies.clone())
                } else {
                    (resolve_url_against(None, raw_url)?, Vec::new())
                }
            } else {
                (resolve_url_against(None, raw_url)?, Vec::new())
            }
        };

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
            return Ok(challenge);
        }

        let page = build_page_update(&resp);
        let mut store = self.lock_store();
        evict_idle_sessions(&mut store);
        store.access_seq = store.access_seq.saturating_add(1);
        let seq = store.access_seq;

        let sid = requested_sid.unwrap_or_else(|| generate_session_id(&store.sessions));
        let (history_len, cookie_count, elements_count, links_count, title, final_url, status) =
            if let Some(sess) = store.sessions.get_mut(&sid) {
                apply_page_update(sess, page, &resp.headers, true, seq);
                (
                    sess.history.len(),
                    sess.cookies.len(),
                    sess.elements.len(),
                    sess.elements.iter().filter(|e| e.tag == "a").count(),
                    sess.title.clone(),
                    sess.current_url.clone(),
                    sess.status,
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
                    last_active: Instant::now(),
                    last_active_seq: seq,
                    last_active_iso: now_iso(),
                };
                apply_page_update(&mut sess, page, &resp.headers, true, seq);
                let out = (
                    sess.history.len(),
                    sess.cookies.len(),
                    sess.elements.len(),
                    sess.elements.iter().filter(|e| e.tag == "a").count(),
                    sess.title.clone(),
                    sess.current_url.clone(),
                    sess.status,
                );
                store.sessions.insert(sid.clone(), sess);
                out
            };

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
        }))
    }

    fn handle_snapshot(&self, args: &Value) -> Result<Value> {
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

        let elements_json: Vec<Value> = sess
            .elements
            .iter()
            .map(|e| element_to_json(e, sess.input_values.get(&e.ref_id)))
            .collect();
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
            "elements": elements_json,
            "snapshot": wrapped_snapshot,
            "text": wrapped_text,
            "history_len": sess.history.len(),
            "cookie_count": sess.cookies.len(),
        }))
    }

    fn handle_fill(&self, args: &Value) -> Result<Value> {
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

        let mut store = self.lock_store();
        evict_idle_sessions(&mut store);
        store.access_seq = store.access_seq.saturating_add(1);
        let seq = store.access_seq;
        let sess = store
            .sessions
            .get_mut(sid)
            .ok_or_else(|| AgentError::NotFound(format!("browser session '{sid}' not found")))?;
        touch_session(sess, seq);

        let norm = normalize_ref(&ref_raw);
        let elem = sess
            .elements
            .iter()
            .find(|e| e.ref_id == norm)
            .ok_or_else(|| AgentError::NotFound(format!("element ref '{norm}' not found")))?;

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
        sess.input_values.insert(ref_id.clone(), value.clone());

        Ok(json!({
            "ok": true,
            "session_id": sess.id,
            "ref": ref_id,
            "tag": tag,
            "name": name,
            "value": value,
        }))
    }

    async fn handle_click(&self, args: &Value, cfg: &ToolsConfig) -> Result<Value> {
        ensure_network_allowed(cfg)?;
        let sid = require_str(args, "session_id")?;
        let ref_raw = require_ref(args)?;
        if let Some(remote_resp) = try_remote_browser_bridge(args, cfg).await {
            return Ok(remote_resp);
        }

        let norm = normalize_ref(&ref_raw);
        enum ClickAction {
            Navigate {
                url: String,
                cookies: Vec<StoredCookie>,
            },
            Submit(SubmitPlan),
        }

        let plan = {
            let mut store = self.lock_store();
            evict_idle_sessions(&mut store);
            let sess = store
                .sessions
                .get(sid)
                .ok_or_else(|| AgentError::NotFound(format!("browser session '{sid}' not found")))?;
            let elem = sess
                .elements
                .iter()
                .find(|e| e.ref_id == norm)
                .ok_or_else(|| AgentError::NotFound(format!("element ref '{norm}' not found")))?;

            if elem.disabled {
                return Err(AgentError::InvalidInput(format!(
                    "element '{}' is disabled",
                    elem.ref_id
                )));
            }

            if elem.tag == "a" {
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
                ClickAction::Submit(build_submit_plan(sess, elem)?)
            } else {
                return Err(AgentError::InvalidInput(format!(
                    "element '{}' ({}) is not clickable",
                    elem.ref_id, elem.tag
                )));
            }
        };

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
        };

        if let Some(challenge) = check_html_challenge(&resp) {
            return Ok(challenge);
        }

        let page = build_page_update(&resp);
        let mut store = self.lock_store();
        evict_idle_sessions(&mut store);
        store.access_seq = store.access_seq.saturating_add(1);
        let seq = store.access_seq;
        let sess = store
            .sessions
            .get_mut(sid)
            .ok_or_else(|| AgentError::NotFound(format!("browser session '{sid}' not found")))?;
        apply_page_update(sess, page, &resp.headers, true, seq);

        Ok(json!({
            "ok": true,
            "session_id": sess.id,
            "method": used_method,
            "url": sess.current_url,
            "status": sess.status,
            "title": sess.title,
            "elements_count": sess.elements.len(),
            "history_len": sess.history.len(),
            "cookie_count": sess.cookies.len(),
            "truncated": resp.truncated,
        }))
    }

    async fn handle_submit(&self, args: &Value, cfg: &ToolsConfig) -> Result<Value> {
        ensure_network_allowed(cfg)?;
        let sid = require_str(args, "session_id")?;
        let ref_raw = require_ref(args)?;
        if let Some(remote_resp) = try_remote_browser_bridge(args, cfg).await {
            return Ok(remote_resp);
        }

        let norm = normalize_ref(&ref_raw);
        let sub = {
            let mut store = self.lock_store();
            evict_idle_sessions(&mut store);
            let sess = store
                .sessions
                .get(sid)
                .ok_or_else(|| AgentError::NotFound(format!("browser session '{sid}' not found")))?;
            let elem = sess
                .elements
                .iter()
                .find(|e| e.ref_id == norm)
                .ok_or_else(|| AgentError::NotFound(format!("element ref '{norm}' not found")))?;
            build_submit_plan(sess, elem)?
        };

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
            return Ok(challenge);
        }

        let page = build_page_update(&resp);
        let mut store = self.lock_store();
        evict_idle_sessions(&mut store);
        store.access_seq = store.access_seq.saturating_add(1);
        let seq = store.access_seq;
        let sess = store
            .sessions
            .get_mut(sid)
            .ok_or_else(|| AgentError::NotFound(format!("browser session '{sid}' not found")))?;
        apply_page_update(sess, page, &resp.headers, true, seq);

        Ok(json!({
            "ok": true,
            "session_id": sess.id,
            "method": method_str,
            "url": sess.current_url,
            "status": sess.status,
            "title": sess.title,
            "elements_count": sess.elements.len(),
            "history_len": sess.history.len(),
            "cookie_count": sess.cookies.len(),
            "truncated": resp.truncated,
        }))
    }

    async fn handle_back(&self, args: &Value, cfg: &ToolsConfig) -> Result<Value> {
        ensure_network_allowed(cfg)?;
        let sid = require_str(args, "session_id")?;
        if let Some(remote_resp) = try_remote_browser_bridge(args, cfg).await {
            return Ok(remote_resp);
        }

        let (prev_url, cookies) = {
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
            (target, sess.cookies.clone())
        };

        let resp =
            fetch_page_with_cookies(reqwest::Method::GET, &prev_url, None, None, &cookies, cfg)
                .await?;

        if let Some(challenge) = check_html_challenge(&resp) {
            return Ok(challenge);
        }

        let page = build_page_update(&resp);
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
        apply_page_update(sess, page, &resp.headers, false, seq);

        Ok(json!({
            "ok": true,
            "session_id": sess.id,
            "url": sess.current_url,
            "status": sess.status,
            "title": sess.title,
            "elements_count": sess.elements.len(),
            "history_len": sess.history.len(),
            "cookie_count": sess.cookies.len(),
            "truncated": resp.truncated,
        }))
    }

    fn handle_links(&self, args: &Value) -> Result<Value> {
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
                })
            })
            .collect();

        Ok(json!({
            "ok": true,
            "session_id": sess.id,
            "url": sess.current_url,
            "count": links.len(),
            "links": links,
        }))
    }

    async fn handle_download(&self, args: &Value, cfg: &ToolsConfig) -> Result<Value> {
        ensure_network_allowed(cfg)?;
        let sid = require_str(args, "session_id")?;
        let explicit_url = optional_trimmed_str(args, "url");
        let explicit_ref = optional_ref(args);
        if explicit_url.is_none() && explicit_ref.is_none() {
            return Err(AgentError::InvalidInput(
                "download requires either 'url' or 'ref'".into(),
            ));
        }

        let (target_url, cookies) = {
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
            (resolved, sess.cookies.clone())
        };

        let resp =
            fetch_page_with_cookies(reqwest::Method::GET, &target_url, None, None, &cookies, cfg)
                .await?;

        if should_check_download_challenge(&resp) {
            if let Some(challenge) = check_html_challenge(&resp) {
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

        let mut store = self.lock_store();
        evict_idle_sessions(&mut store);
        store.access_seq = store.access_seq.saturating_add(1);
        let seq = store.access_seq;
        if let Some(sess) = store.sessions.get_mut(sid) {
            touch_session(sess, seq);
            merge_response_cookies(sess, &resp.final_url, &resp.headers);
        }

        Ok(json!({
            "ok": true,
            "session_id": sid,
            "url": resp.final_url,
            "status": resp.status,
            "path": relative_display_path(&canonical_root, &resolved_target, &rel_path),
            "bytes": resp.body.len(),
            "truncated": resp.truncated,
        }))
    }

    fn handle_save_snapshot(&self, args: &Value, cfg: &ToolsConfig) -> Result<Value> {
        let sid = require_str(args, "session_id")?;
        let (url, html, markdown) = {
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
            let md = render_snapshot_markdown(sess);
            (sess.current_url.clone(), sess.html.clone(), md)
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

        Ok(json!({
            "ok": true,
            "session_id": sid,
            "url": url,
            "html_path": relative_display_path(&root_html, &html_target, &html_rel),
            "markdown_path": relative_display_path(&root_html, &md_target, &md_rel),
            "bytes_html": html_bytes.len(),
            "bytes_markdown": md_bytes.len(),
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

fn apply_page_update(
    sess: &mut BrowserSession,
    page: PageUpdate,
    headers: &HeaderMap,
    push_history: bool,
    seq: u64,
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

fn parse_html_snapshot(html: &str) -> (Option<String>, Vec<SnapshotElement>, Vec<ParsedForm>) {
    let cleaned = strip_non_visible_html(html);
    let title = extract_html_title(&cleaned);
    let bytes = cleaned.as_bytes();
    let len = bytes.len();

    let mut elements: Vec<SnapshotElement> = Vec::new();
    let mut forms: Vec<ParsedForm> = Vec::new();
    let mut form_id_map: HashMap<String, usize> = HashMap::new();
    let mut current_form_idx: Option<usize> = None;
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
                ref_counter += 1;
                elements.push(SnapshotElement {
                    ref_id: format!("e{ref_counter}"),
                    tag: "input".into(),
                    role: role.into(),
                    text: collapse_ws(&label),
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
                ref_counter += 1;
                elements.push(SnapshotElement {
                    ref_id: format!("e{ref_counter}"),
                    tag: "select".into(),
                    role: "combobox".into(),
                    text: collapse_ws(&label),
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
    let effective_val = filled_val.or(elem.value.as_ref());
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
    if elem.checked {
        obj.insert("checked".into(), Value::Bool(true));
    }
    if elem.disabled {
        obj.insert("disabled".into(), Value::Bool(true));
    }
    if let Some(ref fref) = elem.form_ref {
        obj.insert("form_ref".into(), Value::String(fref.clone()));
    }
    Value::Object(obj)
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
}
