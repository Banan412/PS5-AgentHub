use crate::auth::{authenticate_request, constant_time_eq, AuthContext, AuthMethod};
use crate::error::HttpError;
use crate::HttpState;
use axum::body::Body;
use axum::extract::{ConnectInfo, Request, State};
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use std::collections::{HashMap, VecDeque};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub const HEADER_X_CSRF_TOKEN: &str = "x-csrf-token";

pub const CSP_VALUE: &str = "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; font-src 'self'; frame-src 'self' blob:; object-src 'none'; base-uri 'self'; frame-ancestors 'none'; form-action 'self'";
pub const PREVIEW_CSP_VALUE: &str = "sandbox allow-scripts; default-src 'none'; img-src data: blob:; style-src 'unsafe-inline'; script-src 'unsafe-inline'; base-uri 'none'; form-action 'none';";
/// Strict script-free CSP applied to sandbox browser HTML previews.
pub const BROWSER_PREVIEW_CSP_VALUE: &str = "default-src 'none'; img-src data: blob:; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'; frame-ancestors 'self';";

const RATE_LIMIT_WINDOW: Duration = Duration::from_secs(60);
const MAX_TRACKED_IPS: usize = 4096;
const SWEEP_INTERVAL: u64 = 64;
const SWEEP_MIN_BUCKETS: usize = 64;

#[derive(Default)]
pub struct SlidingWindowRateLimiter {
    buckets: Mutex<HashMap<IpAddr, VecDeque<Instant>>>,
    checks: AtomicU64,
}

impl SlidingWindowRateLimiter {
    pub fn new() -> Self {
        Self::default()
    }

    fn sweep_expired(buckets: &mut HashMap<IpAddr, VecDeque<Instant>>, cutoff: Instant) {
        buckets.retain(|_, q| q.back().is_some_and(|&t| t > cutoff));
        if buckets.capacity() > 16 && buckets.len() < buckets.capacity() / 4 {
            buckets.shrink_to_fit();
        }
    }

    /// Checks whether `ip` is within `max_per_minute` over a 60-second sliding window.
    /// Returns `Ok(())` and records the request, or `Err(retry_after_secs)` if exceeded.
    pub fn check_and_record(&self, ip: IpAddr, max_per_minute: u32) -> std::result::Result<(), u64> {
        if max_per_minute == 0 {
            return Err(60);
        }

        let now = Instant::now();
        let cutoff = now.checked_sub(RATE_LIMIT_WINDOW);

        let Ok(mut guard) = self.buckets.lock() else {
            return Err(5);
        };

        let check_num = self.checks.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
        let periodic_due = check_num % SWEEP_INTERVAL == 0
            || (guard.len() > SWEEP_MIN_BUCKETS && check_num % 16 == 0);
        if periodic_due {
            if let Some(c) = cutoff {
                Self::sweep_expired(&mut guard, c);
            }
        }

        if guard.len() >= MAX_TRACKED_IPS && !guard.contains_key(&ip) {
            if let Some(c) = cutoff {
                Self::sweep_expired(&mut guard, c);
            }
            if guard.len() >= MAX_TRACKED_IPS {
                if let Some(oldest_key) = guard
                    .iter()
                    .min_by_key(|(_, q)| q.back().copied().unwrap_or(now))
                    .map(|(k, _)| *k)
                {
                    guard.remove(&oldest_key);
                }
            }
        }

        let queue = guard.entry(ip).or_default();
        if let Some(c) = cutoff {
            while queue.front().is_some_and(|&ts| ts <= c) {
                queue.pop_front();
            }
        }

        if queue.len() >= max_per_minute as usize {
            let retry_after = if let Some(&oldest) = queue.front() {
                let reset_at = oldest + RATE_LIMIT_WINDOW;
                let remaining = reset_at.saturating_duration_since(now);
                let secs = remaining.as_secs() + u64::from(remaining.subsec_nanos() > 0);
                secs.clamp(1, 60)
            } else {
                60
            };
            return Err(retry_after);
        }

        queue.push_back(now);
        Ok(())
    }
}

/// Returns true iff `ip` is loopback, RFC1918 private IPv4, or IPv6 ULA (`fc00::/7`).
/// IPv4-mapped IPv6 addresses (`::ffff:x.x.x.x`) are unwrapped and checked as IPv4.
pub fn is_allowed_lan_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_allowed_lan_v4(v4),
        IpAddr::V6(v6) => {
            if let Some(mapped) = v6.to_ipv4_mapped() {
                return is_allowed_lan_v4(mapped);
            }
            if v6.is_loopback() {
                return true;
            }
            (v6.segments()[0] & 0xfe00) == 0xfc00
        }
    }
}

fn is_allowed_lan_v4(v4: Ipv4Addr) -> bool {
    v4.is_loopback() || v4.is_private()
}

pub fn extract_peer_ip(req: &Request<Body>) -> Option<IpAddr> {
    if let Some(ConnectInfo(addr)) = req.extensions().get::<ConnectInfo<SocketAddr>>() {
        return Some(addr.ip());
    }
    if let Some(addr) = req.extensions().get::<SocketAddr>() {
        return Some(addr.ip());
    }
    None
}

pub fn is_mutating_method(method: &Method) -> bool {
    matches!(
        *method,
        Method::POST | Method::PUT | Method::PATCH | Method::DELETE
    )
}

/// Validates `Origin` (or `Referer` if `Origin` is absent) against the `Host` header.
/// Returns `true` when neither header is present, or when the origin/referer matches `Host`
/// in scheme, host, and port.
pub fn validate_origin_or_referer(headers: &HeaderMap) -> bool {
    if headers.get_all(header::ORIGIN).iter().count() > 1
        || headers.get_all(header::REFERER).iter().count() > 1
    {
        return false;
    }

    let candidate = if let Some(origin) = headers.get(header::ORIGIN) {
        let Ok(s) = origin.to_str() else {
            return false;
        };
        let s = s.trim();
        if s.is_empty() || s.eq_ignore_ascii_case("null") {
            return false;
        }
        Some(s)
    } else if let Some(referer) = headers.get(header::REFERER) {
        let Ok(s) = referer.to_str() else {
            return false;
        };
        let s = s.trim();
        if s.is_empty() {
            return false;
        }
        Some(s)
    } else {
        None
    };

    let Some(raw_url) = candidate else {
        return true;
    };

    let Ok(parsed) = url::Url::parse(raw_url) else {
        return false;
    };
    if !matches!(parsed.scheme(), "http" | "https")
        || !parsed.username().is_empty()
        || parsed.password().is_some()
    {
        return false;
    }
    let Some(origin_host) = parsed.host_str() else {
        return false;
    };

    if headers.get_all(header::HOST).iter().count() != 1 {
        return false;
    }
    let Some(host_hdr) = headers.get(header::HOST).and_then(|v| v.to_str().ok()) else {
        return false;
    };
    let host_hdr = host_hdr.trim();
    if host_hdr.is_empty()
        || host_hdr.contains('@')
        || host_hdr.contains('/')
        || host_hdr.contains('\\')
        || host_hdr.contains('?')
        || host_hdr.contains('#')
        || host_hdr.chars().any(char::is_whitespace)
    {
        return false;
    }

    let Ok(host_url) = url::Url::parse(&format!("http://{host_hdr}")) else {
        return false;
    };
    let Some(expected_host) = host_url.host_str() else {
        return false;
    };

    if parsed.scheme() != host_url.scheme() {
        return false;
    }
    if !origin_host.eq_ignore_ascii_case(expected_host) {
        return false;
    }

    parsed.port_or_known_default() == host_url.port_or_known_default()
}

/// Outermost middleware: attaches strict security headers to EVERY response,
/// enforces `allow_lan_only`, per-IP sliding-window rate limits, and records basic HTTP metrics.
pub async fn security_network_middleware(
    State(state): State<Arc<HttpState>>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let start = Instant::now();
    let path = req.uri().path().to_string();
    let is_login = path == "/api/v1/auth/login" && req.method() == Method::POST;

    let mut response = 'handle: {
        let cfg = state.effective_config();
        let peer_ip = extract_peer_ip(&req);

        if cfg.server.allow_lan_only {
            match peer_ip {
                Some(ip) if is_allowed_lan_ip(ip) => {}
                Some(ip) => {
                    state.logger.warn(
                        "http.security",
                        format!("Rejected non-LAN client IP {ip} on {path}"),
                    );
                    break 'handle HttpError::Forbidden("Access restricted to LAN clients")
                        .into_response();
                }
                None => {
                    break 'handle HttpError::Forbidden("Client address unavailable")
                        .into_response();
                }
            }
        }

        if let Some(ip) = peer_ip {
            if is_login && cfg.security.login_rate_limit_per_minute > 0 {
                if let Err(retry_after) = state
                    .login_limiter
                    .check_and_record(ip, cfg.security.login_rate_limit_per_minute)
                {
                    break 'handle HttpError::TooManyRequests {
                        retry_after_secs: retry_after,
                        message: "Too many login attempts; please try again later",
                    }
                    .into_response();
                }
            }

            if cfg.security.rate_limit_per_minute > 0 {
                if let Err(retry_after) = state
                    .general_limiter
                    .check_and_record(ip, cfg.security.rate_limit_per_minute)
                {
                    break 'handle HttpError::TooManyRequests {
                        retry_after_secs: retry_after,
                        message: "Rate limit exceeded; please slow down",
                    }
                    .into_response();
                }
            }
        }

        next.run(req).await
    };

    let status = response.status();
    apply_security_headers(response.headers_mut(), &path, status);
    let elapsed_ms = start.elapsed().as_millis() as u64;
    state.record_http_request(elapsed_ms);
    response
}

fn is_files_preview_path(path: &str) -> bool {
    path == "/api/v1/files/preview" || path == "/api/v1/files/preview/"
}

fn is_browser_preview_path(path: &str) -> bool {
    path == "/api/v1/sandbox/browser/preview" || path == "/api/v1/sandbox/browser/preview/"
}

pub fn apply_security_headers(headers: &mut HeaderMap, path: &str, status: StatusCode) {
    let is_api = path.starts_with("/api/");
    let is_files_preview = is_files_preview_path(path) && status.is_success();
    let is_browser_preview = is_browser_preview_path(path) && status.is_success();
    let (csp, frame_options) = if is_browser_preview {
        (BROWSER_PREVIEW_CSP_VALUE, "SAMEORIGIN")
    } else if is_files_preview {
        (PREVIEW_CSP_VALUE, "SAMEORIGIN")
    } else {
        (CSP_VALUE, "DENY")
    };

    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(csp),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::X_FRAME_OPTIONS,
        HeaderValue::from_static(frame_options),
    );
    headers.insert(
        HeaderName::from_static("referrer-policy"),
        HeaderValue::from_static("no-referrer"),
    );
    if is_api {
        headers.insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static("no-store"),
        );
    } else {
        headers.insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static("no-cache"),
        );
    }
}

fn is_inbound_webhook_path(path: &str) -> bool {
    let rest = path
        .strip_prefix("/api/v1/webhooks/inbound/")
        .or_else(|| path.strip_prefix("/webhooks/inbound/"));
    matches!(rest, Some(id) if !id.is_empty() && !id.contains('/'))
}

/// Auth & CSRF middleware for `/api/v1/*` routes.
pub async fn auth_csrf_middleware(
    State(state): State<Arc<HttpState>>,
    mut req: Request<Body>,
    next: Next,
) -> Response {
    let path = req.uri().path().to_string();
    let method = req.method().clone();

    if matches!(path.as_str(), "/auth/login" | "/api/v1/auth/login") && method == Method::POST {
        if !validate_origin_or_referer(req.headers()) {
            return HttpError::Forbidden("Invalid Origin or Referer").into_response();
        }
        return next.run(req).await;
    }

    if is_inbound_webhook_path(&path) && method == Method::POST {
        return next.run(req).await;
    }

    if matches!(path.as_str(), "/health" | "/api/v1/health")
        && matches!(method, Method::GET | Method::HEAD)
    {
        if let Ok(auth) = authenticate_request(&state, req.headers()) {
            req.extensions_mut().insert(auth);
        }
        return next.run(req).await;
    }

    let auth = match authenticate_request(&state, req.headers()) {
        Ok(ctx) => ctx,
        Err(err) => return err.into_response(),
    };

    if is_mutating_method(&method) {
        if !validate_origin_or_referer(req.headers()) {
            return HttpError::Forbidden("Cross-origin request rejected").into_response();
        }

        if let AuthMethod::Cookie(ref session) = auth.method {
            let provided_csrf = req
                .headers()
                .get(HEADER_X_CSRF_TOKEN)
                .and_then(|v| v.to_str().ok())
                .map(str::trim);

            match provided_csrf {
                Some(tok) if constant_time_eq(tok, &session.csrf_token) => {}
                _ => {
                    return HttpError::Forbidden("Missing or invalid X-CSRF-Token")
                        .into_response();
                }
            }
        }
    }

    req.extensions_mut().insert(auth);
    next.run(req).await
}

/// Helper to insert `AuthContext` only when `auth_enabled == false` is tested cleanly.
#[allow(dead_code)]
pub fn is_authenticated_cookie(auth: &AuthContext) -> bool {
    matches!(auth.method, AuthMethod::Cookie(_))
}
