use ps5_agent_config::ToolsConfig;
use ps5_agent_core::{AgentError, Result};
use regex::{Regex, RegexBuilder};
use reqwest::header::{
    HeaderMap, HeaderName, HeaderValue, ACCEPT, CONTENT_LENGTH, CONTENT_TYPE, HOST, LOCATION,
};
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Mutex, Once, OnceLock};
use std::time::{Duration, Instant};
use url::Url;

const MAX_REDIRECTS: usize = 5;
const DEFAULT_MAX_WEB_BYTES: usize = 16 * 1024 * 1024;
const MAX_EXTRACT_HTML_BYTES: usize = 16 * 1024 * 1024;
const SEARCH_CACHE_MAX_ENTRIES: usize = 128;
const SEARCH_CACHE_TTL: Duration = Duration::from_secs(15 * 60);

static CRYPTO_INIT: Once = Once::new();

#[derive(Debug, Clone)]
pub(crate) struct ValidatedHttpResponse {
    pub status: u16,
    pub final_url: String,
    pub headers: HeaderMap,
    pub body: Vec<u8>,
    pub truncated: bool,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct SearchCacheKey {
    provider: String,
    endpoint_url: String,
    query: String,
    count: usize,
    freshness: Option<String>,
    domain: Option<String>,
}

#[derive(Debug, Clone)]
struct SearchCacheEntry {
    response: Value,
    inserted_at: Instant,
}

#[derive(Debug, Default)]
struct SearchCache {
    entries: HashMap<SearchCacheKey, SearchCacheEntry>,
    order: VecDeque<SearchCacheKey>,
}

impl SearchCache {
    fn evict_expired(&mut self, now: Instant) {
        let expired: Vec<SearchCacheKey> = self
            .entries
            .iter()
            .filter_map(|(k, v)| {
                if now.saturating_duration_since(v.inserted_at) >= SEARCH_CACHE_TTL {
                    Some(k.clone())
                } else {
                    None
                }
            })
            .collect();
        if !expired.is_empty() {
            for k in &expired {
                self.entries.remove(k);
            }
            self.order.retain(|k| !expired.contains(k));
        }
    }

    fn get(&mut self, key: &SearchCacheKey) -> Option<Value> {
        let now = Instant::now();
        self.evict_expired(now);
        let resp = self.entries.get(key).map(|e| e.response.clone())?;
        self.order.retain(|k| k != key);
        self.order.push_back(key.clone());
        Some(resp)
    }

    fn insert(&mut self, key: SearchCacheKey, response: Value) {
        let now = Instant::now();
        self.evict_expired(now);
        if self.entries.contains_key(&key) {
            self.order.retain(|k| k != &key);
        } else {
            while self.entries.len() >= SEARCH_CACHE_MAX_ENTRIES {
                if let Some(oldest) = self.order.pop_front() {
                    self.entries.remove(&oldest);
                } else {
                    break;
                }
            }
        }
        self.entries.insert(
            key.clone(),
            SearchCacheEntry {
                response,
                inserted_at: now,
            },
        );
        self.order.push_back(key);
    }
}

static SEARCH_CACHE: OnceLock<Mutex<SearchCache>> = OnceLock::new();

fn search_cache() -> &'static Mutex<SearchCache> {
    SEARCH_CACHE.get_or_init(|| Mutex::new(SearchCache::default()))
}

pub fn ensure_rustls_ring_provider() {
    CRYPTO_INIT.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

pub fn build_web_client(timeout_secs: u64) -> Result<reqwest::Client> {
    build_pinned_web_client(timeout_secs, None, &[])
}

/// Modern desktop browser profile that rotates realistic User-Agents
/// and attaches standard Client Hint and Fetch Metadata headers to reduce
/// bot detection and rate limiting.
#[derive(Debug, Clone)]
pub struct BrowserHeadersProfile {
    pub user_agent: &'static str,
    pub sec_ch_ua: Option<&'static str>,
    pub sec_ch_ua_mobile: &'static str,
    pub sec_ch_ua_platform: Option<&'static str>,
    pub sec_fetch_dest: &'static str,
    pub sec_fetch_mode: &'static str,
    pub sec_fetch_site: &'static str,
    pub sec_fetch_user: &'static str,
    pub accept: &'static str,
    pub accept_language: &'static str,
    pub upgrade_insecure_requests: &'static str,
}

impl BrowserHeadersProfile {
    /// Rotates across realistic modern desktop browser profiles (Chrome 124 on Windows/macOS,
    /// Edge 124 on Windows, Firefox 125 on Windows).
    pub fn rotate() -> &'static BrowserHeadersProfile {
        static PROFILES: OnceLock<Vec<BrowserHeadersProfile>> = OnceLock::new();
        static ROTATION_INDEX: std::sync::atomic::AtomicUsize =
            std::sync::atomic::AtomicUsize::new(0);
        let profiles = PROFILES.get_or_init(|| {
            vec![
                BrowserHeadersProfile {
                    user_agent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                    sec_ch_ua: Some("\"Chromium\";v=\"124\", \"Google Chrome\";v=\"124\", \"Not-A.Brand\";v=\"99\""),
                    sec_ch_ua_mobile: "?0",
                    sec_ch_ua_platform: Some("\"Windows\""),
                    sec_fetch_dest: "document",
                    sec_fetch_mode: "navigate",
                    sec_fetch_site: "none",
                    sec_fetch_user: "?1",
                    accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    accept_language: "en-US,en;q=0.9",
                    upgrade_insecure_requests: "1",
                },
                BrowserHeadersProfile {
                    user_agent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                    sec_ch_ua: Some("\"Chromium\";v=\"124\", \"Google Chrome\";v=\"124\", \"Not-A.Brand\";v=\"99\""),
                    sec_ch_ua_mobile: "?0",
                    sec_ch_ua_platform: Some("\"macOS\""),
                    sec_fetch_dest: "document",
                    sec_fetch_mode: "navigate",
                    sec_fetch_site: "none",
                    sec_fetch_user: "?1",
                    accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    accept_language: "en-US,en;q=0.9",
                    upgrade_insecure_requests: "1",
                },
                BrowserHeadersProfile {
                    user_agent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
                    sec_ch_ua: Some("\"Chromium\";v=\"124\", \"Microsoft Edge\";v=\"124\", \"Not-A.Brand\";v=\"99\""),
                    sec_ch_ua_mobile: "?0",
                    sec_ch_ua_platform: Some("\"Windows\""),
                    sec_fetch_dest: "document",
                    sec_fetch_mode: "navigate",
                    sec_fetch_site: "none",
                    sec_fetch_user: "?1",
                    accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    accept_language: "en-US,en;q=0.9",
                    upgrade_insecure_requests: "1",
                },
                BrowserHeadersProfile {
                    user_agent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
                    sec_ch_ua: None,
                    sec_ch_ua_mobile: "?0",
                    sec_ch_ua_platform: Some("\"Windows\""),
                    sec_fetch_dest: "document",
                    sec_fetch_mode: "navigate",
                    sec_fetch_site: "none",
                    sec_fetch_user: "?1",
                    accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    accept_language: "en-US,en;q=0.9",
                    upgrade_insecure_requests: "1",
                },
            ]
        });
        let idx = ROTATION_INDEX.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % profiles.len();
        &profiles[idx]
    }

    /// Appends standard modern browser headers if not already present in `headers`.
    pub fn apply_to_headers(&self, headers: &mut HeaderMap) {
        if let Some(ua) = self.sec_ch_ua {
            headers
                .entry(HeaderName::from_static("sec-ch-ua"))
                .or_insert_with(|| HeaderValue::from_static(ua));
        }
        headers
            .entry(HeaderName::from_static("sec-ch-ua-mobile"))
            .or_insert_with(|| HeaderValue::from_static(self.sec_ch_ua_mobile));
        if let Some(plat) = self.sec_ch_ua_platform {
            headers
                .entry(HeaderName::from_static("sec-ch-ua-platform"))
                .or_insert_with(|| HeaderValue::from_static(plat));
        }
        headers
            .entry(HeaderName::from_static("sec-fetch-dest"))
            .or_insert_with(|| HeaderValue::from_static(self.sec_fetch_dest));
        headers
            .entry(HeaderName::from_static("sec-fetch-mode"))
            .or_insert_with(|| HeaderValue::from_static(self.sec_fetch_mode));
        headers
            .entry(HeaderName::from_static("sec-fetch-site"))
            .or_insert_with(|| HeaderValue::from_static(self.sec_fetch_site));
        headers
            .entry(HeaderName::from_static("sec-fetch-user"))
            .or_insert_with(|| HeaderValue::from_static(self.sec_fetch_user));
        headers
            .entry(ACCEPT)
            .or_insert_with(|| HeaderValue::from_static(self.accept));
        headers
            .entry(HeaderName::from_static("accept-language"))
            .or_insert_with(|| HeaderValue::from_static(self.accept_language));
        headers
            .entry(HeaderName::from_static("upgrade-insecure-requests"))
            .or_insert_with(|| HeaderValue::from_static(self.upgrade_insecure_requests));
    }
}

fn build_pinned_web_client(
    timeout_secs: u64,
    pinned_host: Option<&str>,
    pinned_addrs: &[SocketAddr],
) -> Result<reqwest::Client> {
    ensure_rustls_ring_provider();
    let connect_secs = if timeout_secs == 0 {
        30
    } else {
        timeout_secs.clamp(1, 30)
    };
    let profile = BrowserHeadersProfile::rotate();
    let mut default_headers = HeaderMap::new();
    profile.apply_to_headers(&mut default_headers);

    let mut builder = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(connect_secs))
        .user_agent(profile.user_agent)
        .default_headers(default_headers);
    if timeout_secs > 0 {
        builder = builder.timeout(Duration::from_secs(timeout_secs));
    }
    if let Some(raw_host) = pinned_host {
        let trimmed = raw_host.trim_start_matches('[').trim_end_matches(']');
        if !trimmed.is_empty() && trimmed.parse::<IpAddr>().is_err() && !pinned_addrs.is_empty() {
            builder = builder.resolve_to_addrs(trimmed, pinned_addrs);
            let stripped = trimmed.trim_end_matches('.');
            if !stripped.is_empty() && stripped != trimmed {
                builder = builder.resolve_to_addrs(stripped, pinned_addrs);
            }
        }
    }
    builder
        .build()
        .map_err(|e| AgentError::Internal(format!("failed to build web HTTP client: {e}")))
}

pub fn wrap_untrusted_web_content(source: &str, body: &str) -> String {
    let safe_source = source
        .replace('"', "&quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;");
    let safe_body = sanitize_closing_untrusted_tag(body);
    format!("<untrusted_web_content source=\"{safe_source}\">\n{safe_body}\n</untrusted_web_content>")
}

fn sanitize_closing_untrusted_tag(s: &str) -> String {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| {
        RegexBuilder::new(r"(?i)</\s*untrusted_web_content\s*>")
            .build()
            .expect("valid static regex")
    });
    re.replace_all(s, "&lt;/untrusted_web_content&gt;")
        .into_owned()
}

fn normalize_host(raw: &str) -> String {
    raw.trim()
        .trim_start_matches('[')
        .trim_end_matches(']')
        .trim_end_matches('.')
        .to_ascii_lowercase()
}

fn is_cloud_metadata_v4(ip: Ipv4Addr) -> bool {
    let o = ip.octets();
    // AWS/GCP/Azure/DO link-local metadata 169.254.169.0/24, ECS 169.254.170.2, Alibaba 100.100.100.200
    (o[0] == 169 && o[1] == 254 && (o[2] == 169 || o[2] == 170))
        || ip == Ipv4Addr::new(100, 100, 100, 200)
}

fn embedded_v4_from_segments(hi: u16, lo: u16) -> Ipv4Addr {
    let h = hi.to_be_bytes();
    let l = lo.to_be_bytes();
    Ipv4Addr::new(h[0], h[1], l[0], l[1])
}

fn is_cloud_metadata_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_cloud_metadata_v4(v4),
        IpAddr::V6(v6) => {
            if v6 == Ipv6Addr::new(0xfd00, 0x0ec2, 0, 0, 0, 0, 0, 0x0254) {
                return true;
            }
            if let Some(mapped) = v6.to_ipv4_mapped() {
                if is_cloud_metadata_v4(mapped) {
                    return true;
                }
            }
            let s = v6.segments();
            // ::/96 IPv4-compatible
            if s[0] == 0 && s[1] == 0 && s[2] == 0 && s[3] == 0 && s[4] == 0 && s[5] == 0
                && is_cloud_metadata_v4(embedded_v4_from_segments(s[6], s[7]))
            {
                return true;
            }
            // ::ffff:0:0:0/96 SIIT
            if s[0] == 0 && s[1] == 0 && s[2] == 0 && s[3] == 0 && s[4] == 0xffff && s[5] == 0
                && is_cloud_metadata_v4(embedded_v4_from_segments(s[6], s[7]))
            {
                return true;
            }
            // 64:ff9b::/96 NAT64 well-known
            if s[0] == 0x0064 && s[1] == 0xff9b && s[2] == 0 && s[3] == 0 && s[4] == 0 && s[5] == 0
                && is_cloud_metadata_v4(embedded_v4_from_segments(s[6], s[7]))
            {
                return true;
            }
            // 64:ff9b:1::/48 NAT64 local-use
            if s[0] == 0x0064 && s[1] == 0xff9b && s[2] == 1
                && is_cloud_metadata_v4(embedded_v4_from_segments(s[6], s[7]))
            {
                return true;
            }
            // 2002::/16 6to4
            if s[0] == 0x2002 && is_cloud_metadata_v4(embedded_v4_from_segments(s[1], s[2])) {
                return true;
            }
            // 2001::/32 Teredo (server in segs[2..4], inverted client in !segs[6..8])
            if s[0] == 0x2001
                && s[1] == 0
                && (is_cloud_metadata_v4(embedded_v4_from_segments(s[2], s[3]))
                    || is_cloud_metadata_v4(embedded_v4_from_segments(!s[6], !s[7])))
            {
                return true;
            }
            false
        }
    }
}

fn is_cloud_metadata_host(host_l: &str) -> bool {
    let h = normalize_host(host_l);
    h == "metadata.google.internal"
        || h.ends_with(".metadata.google.internal")
        || h == "metadata.goog"
        || h == "instance-data"
}

pub fn is_forbidden_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_forbidden_v4(v4),
        IpAddr::V6(v6) => is_forbidden_v6(v6),
    }
}

fn is_forbidden_v4(ip: Ipv4Addr) -> bool {
    let octets = ip.octets();
    ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_unspecified()
        || ip.is_broadcast()
        || ip.is_multicast()
        || ip.is_documentation()
        || is_cloud_metadata_v4(ip)
        // 0.0.0.0/8
        || octets[0] == 0
        // 100.64.0.0/10 (CGNAT)
        || (octets[0] == 100 && (64..=127).contains(&octets[1]))
        // 192.0.0.0/24
        || (octets[0] == 192 && octets[1] == 0 && octets[2] == 0)
        // 198.18.0.0/15 (benchmarking)
        || (octets[0] == 198 && (octets[1] == 18 || octets[1] == 19))
        // 240.0.0.0/4 (reserved)
        || octets[0] >= 240
}

fn is_forbidden_v6(ip: Ipv6Addr) -> bool {
    if let Some(mapped_v4) = ip.to_ipv4_mapped() {
        return is_forbidden_v4(mapped_v4);
    }
    let segs = ip.segments();
    ip.is_loopback()
        || ip.is_unspecified()
        || ip.is_multicast()
        // AWS EC2 IPv6 metadata endpoint fd00:ec2::254 (also covered by fc00::/7)
        || ip == Ipv6Addr::new(0xfd00, 0x0ec2, 0, 0, 0, 0, 0, 0x0254)
        // ::/96 (deprecated IPv4-compatible IPv6)
        || segs[..6] == [0, 0, 0, 0, 0, 0]
        // ::ffff:0:0:0/96 (SIIT IPv4-translated)
        || segs[..6] == [0, 0, 0, 0, 0xffff, 0]
        // 64:ff9b::/96 (NAT64 well-known prefix)
        || segs[..6] == [0x0064, 0xff9b, 0, 0, 0, 0]
        // 64:ff9b:1::/48 (NAT64 local-use prefix)
        || (segs[0] == 0x0064 && segs[1] == 0xff9b && segs[2] == 1)
        // 2002::/16 (6to4)
        || segs[0] == 0x2002
        // 2001::/32 (Teredo)
        || (segs[0] == 0x2001 && segs[1] == 0)
        // 100::/64 (Discard-Only)
        || (segs[0] == 0x0100 && segs[1] == 0 && segs[2] == 0 && segs[3] == 0)
        // 2001:2::/48 (Benchmarking)
        || (segs[0] == 0x2001 && segs[1] == 0x0002 && segs[2] == 0)
        // 2001:db8::/32 (Documentation)
        || (segs[0] == 0x2001 && segs[1] == 0x0db8)
        // fc00::/7 (Unique Local Address)
        || (segs[0] & 0xfe00) == 0xfc00
        // fe80::/10 (Link-local)
        || (segs[0] & 0xffc0) == 0xfe80
        // fec0::/10 (Site-local deprecated)
        || (segs[0] & 0xffc0) == 0xfec0
}

fn wildcard_match(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    let (mut pi, mut ti) = (0usize, 0usize);
    let mut star: Option<(usize, usize)> = None;
    while ti < t.len() {
        if pi < p.len() && p[pi] == t[ti] {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some((pi, ti));
            pi += 1;
        } else if let Some((sp, st)) = star {
            pi = sp + 1;
            ti = st + 1;
            star = Some((sp, ti));
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

fn domain_matches(host: &str, domains: &[String]) -> bool {
    if domains.is_empty() {
        return false;
    }
    let host_l = normalize_host(host);
    if host_l.is_empty() {
        return false;
    }
    for entry in domains {
        let raw = entry.trim().to_ascii_lowercase();
        if raw.is_empty() {
            continue;
        }
        if raw == "*" {
            return true;
        }
        // Reject degenerate wildcards/dots before stripping trailing `.` so `"*."` never becomes `"*"`.
        if raw == "*." || raw.chars().all(|c| c == '.') {
            continue;
        }
        let pat = raw.trim_end_matches('.');
        if pat.is_empty() || pat == "*" {
            continue;
        }
        if let Some(suffix) = pat.strip_prefix("*.") {
            if suffix.is_empty() || suffix.contains('*') {
                continue;
            }
            if host_l == suffix || host_l.ends_with(&format!(".{suffix}")) {
                return true;
            }
            continue;
        }
        if pat.contains('*') {
            if wildcard_match(pat, &host_l) {
                return true;
            }
            continue;
        }
        if host_l == pat || host_l.ends_with(&format!(".{pat}")) {
            return true;
        }
    }
    false
}

pub async fn validate_web_url(raw_url: &str, cfg: &ToolsConfig) -> Result<Url> {
    let (url, _addrs) = validate_and_resolve_web_url(raw_url, cfg).await?;
    Ok(url)
}

async fn validate_and_resolve_web_url(
    raw_url: &str,
    cfg: &ToolsConfig,
) -> Result<(Url, Vec<SocketAddr>)> {
    if !cfg.network_access {
        return Err(AgentError::PermissionDenied(
            "outbound network access is disabled by policy (tools.network_access = false)".into(),
        ));
    }

    let parsed = Url::parse(raw_url.trim())
        .map_err(|e| AgentError::InvalidInput(format!("invalid URL: {e}")))?;

    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(AgentError::PermissionDenied(format!(
            "unsupported URL scheme '{}'; only http and https are allowed",
            parsed.scheme()
        )));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(AgentError::PermissionDenied(
            "URLs with embedded credentials are forbidden".into(),
        ));
    }

    let raw_host = parsed
        .host_str()
        .map(|h| h.trim_start_matches('[').trim_end_matches(']'))
        .filter(|h| !h.is_empty())
        .ok_or_else(|| AgentError::InvalidInput("URL has no host".into()))?;
    let host_norm = normalize_host(raw_host);
    if host_norm.is_empty() {
        return Err(AgentError::InvalidInput("URL has no host".into()));
    }

    // Explicit blocked_domains always beats allowed_domains (even `["*"]`).
    if domain_matches(&host_norm, &cfg.blocked_domains) {
        return Err(AgentError::PermissionDenied(format!(
            "domain '{host_norm}' is blocked by blocked_domains policy"
        )));
    }
    if !domain_matches(&host_norm, &cfg.allowed_domains) {
        return Err(AgentError::PermissionDenied(format!(
            "domain '{host_norm}' is not in allowed_domains"
        )));
    }

    // Cloud metadata hosts and IPs are unconditionally blocked even if allow_lan is enabled.
    if is_cloud_metadata_host(&host_norm) {
        return Err(AgentError::PermissionDenied(format!(
            "SSRF guard blocked cloud metadata host '{host_norm}'"
        )));
    }

    let allow_lan = cfg.allow_local_network || !cfg.block_private_lan_web_requests;
    if !allow_lan
        && (host_norm == "localhost"
            || host_norm.ends_with(".localhost")
            || host_norm.ends_with(".local")
            || host_norm.ends_with(".internal")
            || host_norm == "ip6-localhost")
    {
        return Err(AgentError::PermissionDenied(format!(
            "SSRF guard blocked local hostname '{host_norm}'"
        )));
    }

    let port = parsed.port_or_known_default().unwrap_or(80);
    if let Ok(ip) = host_norm.parse::<IpAddr>() {
        if is_cloud_metadata_ip(ip) {
            return Err(AgentError::PermissionDenied(format!(
                "SSRF guard blocked cloud metadata IP '{ip}'"
            )));
        }
        if !allow_lan && is_forbidden_ip(ip) {
            return Err(AgentError::PermissionDenied(format!(
                "SSRF guard blocked private/loopback/link-local IP '{ip}'"
            )));
        }
        return Ok((parsed, vec![SocketAddr::new(ip, port)]));
    }

    let addrs_iter = tokio::net::lookup_host((raw_host, port))
        .await
        .map_err(|e| AgentError::Tool {
            tool_id: "web".into(),
            message: format!("DNS resolution failed for '{host_norm}': {e}"),
        })?;

    let mut pinned_addrs = Vec::new();
    for addr in addrs_iter {
        if is_cloud_metadata_ip(addr.ip()) {
            return Err(AgentError::PermissionDenied(format!(
                "SSRF guard blocked resolved cloud metadata IP '{}' for '{host_norm}'",
                addr.ip()
            )));
        }
        if !allow_lan && is_forbidden_ip(addr.ip()) {
            return Err(AgentError::PermissionDenied(format!(
                "SSRF guard blocked resolved private/loopback/link-local IP '{}' for '{host_norm}'",
                addr.ip()
            )));
        }
        pinned_addrs.push(addr);
    }
    if pinned_addrs.is_empty() {
        return Err(AgentError::Tool {
            tool_id: "web".into(),
            message: format!("DNS resolution returned no addresses for '{host_norm}'"),
        });
    }

    Ok((parsed, pinned_addrs))
}

fn same_origin(a: &Url, b: &Url) -> bool {
    let ha = a.host_str().map(normalize_host).unwrap_or_default();
    let hb = b.host_str().map(normalize_host).unwrap_or_default();
    a.scheme().eq_ignore_ascii_case(b.scheme())
        && ha == hb
        && a.port_or_known_default() == b.port_or_known_default()
}

fn effective_max_web_bytes(cfg: &ToolsConfig) -> usize {
    if cfg.max_web_bytes == 0 {
        DEFAULT_MAX_WEB_BYTES
    } else {
        cfg.max_web_bytes
    }
}

fn parse_headers(val: Option<&Value>) -> Result<HeaderMap> {
    let mut map = HeaderMap::new();
    let Some(obj) = val.and_then(|v| v.as_object()) else {
        return Ok(map);
    };
    for (k, v) in obj {
        let Some(vs) = v.as_str() else { continue };
        if k.contains('\r') || k.contains('\n') || vs.contains('\r') || vs.contains('\n') {
            return Err(AgentError::InvalidInput("header contains CR/LF".into()));
        }
        let name = HeaderName::from_bytes(k.as_bytes())
            .map_err(|e| AgentError::InvalidInput(format!("invalid header name '{k}': {e}")))?;
        let value = HeaderValue::from_str(vs)
            .map_err(|e| AgentError::InvalidInput(format!("invalid header value for '{k}': {e}")))?;
        map.insert(name, value);
    }
    Ok(map)
}

pub(crate) async fn fetch_validated_url(
    method: reqwest::Method,
    initial_url: &str,
    headers: HeaderMap,
    body: Option<Vec<u8>>,
    cfg: &ToolsConfig,
) -> Result<ValidatedHttpResponse> {
    let (mut current_url, mut current_addrs) = validate_and_resolve_web_url(initial_url, cfg).await?;
    let mut current_method = method;
    let mut current_headers = headers;
    let mut current_body = body;
    let max_bytes = effective_max_web_bytes(cfg);

    // Reuse HTTP client connection pool across redirect hops to prevent socket churn and TCP port exhaustion.
    let mut client_cache: Vec<(Option<String>, Vec<SocketAddr>, reqwest::Client)> =
        Vec::with_capacity(MAX_REDIRECTS + 1);

    for _hop in 0..=MAX_REDIRECTS {
        let hop_host = current_url.host_str().map(|h| h.to_string());
        let hop_client = if let Some((_, _, cached)) = client_cache
            .iter()
            .find(|(h, a, _)| *h == hop_host && a.as_slice() == current_addrs.as_slice())
        {
            cached.clone()
        } else {
            let fresh = build_pinned_web_client(
                cfg.default_timeout_secs,
                current_url.host_str(),
                &current_addrs,
            )?;
            client_cache.push((hop_host, current_addrs.clone(), fresh.clone()));
            fresh
        };
        let mut req = hop_client
            .request(current_method.clone(), current_url.clone())
            .headers(current_headers.clone());
        if let Some(ref b) = current_body {
            req = req.body(b.clone());
        }

        let mut resp = req.send().await.map_err(|e| {
            if e.is_timeout() {
                AgentError::Timeout(format!("web request timed out: {e}"))
            } else {
                AgentError::Tool {
                    tool_id: "web".into(),
                    message: format!("HTTP request failed: {e}"),
                }
            }
        })?;

        let status = resp.status();
        if status.is_redirection() {
            if let Some(loc_val) = resp.headers().get(LOCATION) {
                let loc = loc_val.to_str().map_err(|_| AgentError::Tool {
                    tool_id: "web".into(),
                    message: "redirect Location header contains invalid UTF-8".into(),
                })?;
                let next_url = current_url.join(loc).map_err(|e| AgentError::Tool {
                    tool_id: "web".into(),
                    message: format!("invalid redirect URL: {e}"),
                })?;
                let (validated_next, next_addrs) =
                    validate_and_resolve_web_url(next_url.as_str(), cfg).await?;

                let is_same_origin = same_origin(&current_url, &validated_next);
                let is_scheme_downgrade =
                    current_url.scheme() == "https" && validated_next.scheme() == "http";
                if !is_same_origin || is_scheme_downgrade {
                    crate::custom::strip_sensitive_redirect_headers(&mut current_headers);
                }
                current_headers.remove(HOST);

                let code = status.as_u16();
                if matches!(code, 301..=303) {
                    if current_method == reqwest::Method::POST
                        || current_method == reqwest::Method::PUT
                        || (code == 303 && current_method != reqwest::Method::HEAD)
                    {
                        current_method = reqwest::Method::GET;
                    }
                    current_body = None;
                    current_headers.remove(CONTENT_TYPE);
                    current_headers.remove(CONTENT_LENGTH);
                } else if matches!(code, 307 | 308) && !is_same_origin && current_body.is_some() {
                    return Err(AgentError::PermissionDenied(
                        "cross-origin 307/308 redirect with request body is forbidden".into(),
                    ));
                }

                current_url = validated_next;
                current_addrs = next_addrs;
                continue;
            }
        }

        let resp_headers = resp.headers().clone();
        let mut buf = Vec::new();
        let mut truncated = false;

        while let Some(chunk) = resp.chunk().await.map_err(|e| AgentError::Tool {
            tool_id: "web".into(),
            message: format!("reading HTTP response stream failed: {e}"),
        })? {
            let remaining = max_bytes.saturating_sub(buf.len());
            if remaining == 0 {
                truncated = true;
                break;
            }
            if chunk.len() > remaining {
                buf.extend_from_slice(&chunk[..remaining]);
                truncated = true;
                break;
            }
            buf.extend_from_slice(&chunk);
        }

        return Ok(ValidatedHttpResponse {
            status: status.as_u16(),
            final_url: current_url.to_string(),
            headers: resp_headers,
            body: buf,
            truncated,
        });
    }

    Err(AgentError::Tool {
        tool_id: "web".into(),
        message: "too many HTTP redirects".into(),
    })
}

/// Detects Cloudflare Turnstile, reCAPTCHA, hCaptcha, Altcha, and anti-bot
/// challenge pages. Never attempts to solve or bypass any gate.
pub(crate) fn detect_challenge_gate(status: u16, headers: &HeaderMap, html: &str) -> Option<String> {
    if let Some(cf) = headers.get("cf-mitigated").and_then(|v| v.to_str().ok()) {
        if cf.to_ascii_lowercase().contains("challenge") {
            return Some("cloudflare_challenge".to_string());
        }
    }
    if headers.contains_key("x-captcha-required") || headers.contains_key("x-ddg-bot") {
        return Some("anti_bot_challenge".to_string());
    }

    let lower = html.to_ascii_lowercase();
    if lower.contains("class=\"cf-turnstile\"")
        || lower.contains("class='cf-turnstile'")
        || lower.contains("challenges.cloudflare.com/turnstile/")
    {
        return Some("cloudflare_turnstile".to_string());
    }
    if lower.contains("class=\"g-recaptcha\"")
        || lower.contains("class='g-recaptcha'")
        || lower.contains("google.com/recaptcha/api.js")
        || lower.contains("recaptcha/api2")
    {
        return Some("google_recaptcha".to_string());
    }
    if lower.contains("class=\"h-captcha\"")
        || lower.contains("class='h-captcha'")
        || lower.contains("hcaptcha.com/1/api.js")
    {
        return Some("hcaptcha".to_string());
    }
    if lower.contains("<altcha-widget") || lower.contains("altcha.min.js") {
        return Some("altcha_pow".to_string());
    }
    if lower.contains("anomaly-modal")
        || lower.contains("bots use duckduckgo")
        || lower.contains("please complete the following challenge")
    {
        return Some("duckduckgo_challenge".to_string());
    }

    if matches!(status, 403 | 429 | 503) {
        if lower.contains("cf-browser-verification")
            || lower.contains("cf_chl_opt")
            || lower.contains("/cdn-cgi/challenge-platform/")
            || (lower.contains("just a moment...") && lower.contains("cloudflare"))
        {
            return Some("cloudflare_challenge".to_string());
        }
        if lower.contains("verify you are human")
            || lower.contains("checking your browser before accessing")
            || lower.contains("enable javascript and cookies to continue")
            || lower.contains("attention required! | cloudflare")
            || lower.contains("px-captcha")
        {
            return Some("anti_bot_challenge".to_string());
        }
    }

    None
}

pub async fn exec_http_get(
    args: &Value,
    cfg: &ToolsConfig,
    _client: &reqwest::Client,
) -> Result<Value> {
    let url = args
        .get("url")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AgentError::InvalidInput("missing required string 'url'".into()))?;
    let headers = parse_headers(args.get("headers"))?;

    let resp = fetch_validated_url(reqwest::Method::GET, url, headers, None, cfg).await?;
    let body_text = String::from_utf8_lossy(&resp.body);
    let wrapped = wrap_untrusted_web_content(&resp.final_url, &body_text);
    let headers_map: serde_json::Map<String, Value> = resp
        .headers
        .iter()
        .filter_map(|(k, v)| {
            v.to_str()
                .ok()
                .map(|vs| (k.to_string(), Value::String(vs.to_string())))
        })
        .collect();

    Ok(json!({
        "status": resp.status,
        "url": resp.final_url,
        "headers": headers_map,
        "body": wrapped,
        "truncated": resp.truncated,
    }))
}

pub async fn exec_http_post(
    args: &Value,
    cfg: &ToolsConfig,
    _client: &reqwest::Client,
) -> Result<Value> {
    let url = args
        .get("url")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AgentError::InvalidInput("missing required string 'url'".into()))?;
    let mut headers = parse_headers(args.get("headers"))?;
    let content_type = args
        .get("content_type")
        .and_then(|v| v.as_str())
        .unwrap_or("application/json");
    if !headers.contains_key(CONTENT_TYPE) {
        if let Ok(hv) = HeaderValue::from_str(content_type) {
            headers.insert(CONTENT_TYPE, hv);
        }
    }

    let body_bytes = match args.get("body") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::String(s)) => s.as_bytes().to_vec(),
        Some(other) => serde_json::to_vec(other).unwrap_or_default(),
    };

    let resp =
        fetch_validated_url(reqwest::Method::POST, url, headers, Some(body_bytes), cfg).await?;
    let body_text = String::from_utf8_lossy(&resp.body);
    let wrapped = wrap_untrusted_web_content(&resp.final_url, &body_text);

    Ok(json!({
        "status": resp.status,
        "url": resp.final_url,
        "body": wrapped,
        "truncated": resp.truncated,
    }))
}

fn looks_like_html(headers: &HeaderMap, raw: &str) -> bool {
    let ct_html = headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map_or(false, |ct| {
            let l = ct.to_ascii_lowercase();
            l.contains("html") && !l.contains("markdown")
        });
    if ct_html {
        return true;
    }
    let trimmed = raw.trim_start().to_ascii_lowercase();
    trimmed.starts_with("<!doctype html") || trimmed.starts_with("<html")
}

pub async fn exec_fetch_website(
    args: &Value,
    cfg: &ToolsConfig,
    _client: &reqwest::Client,
) -> Result<Value> {
    let url = args
        .get("url")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AgentError::InvalidInput("missing required string 'url'".into()))?;

    let resp = fetch_validated_url(reqwest::Method::GET, url, HeaderMap::new(), None, cfg).await?;
    let raw = String::from_utf8_lossy(&resp.body).into_owned();
    let (title, extracted) = if looks_like_html(&resp.headers, &raw) {
        extract_title_and_text(&raw)
    } else {
        (None, raw)
    };

    let wrapped = wrap_untrusted_web_content(&resp.final_url, &extracted);
    Ok(json!({
        "status": resp.status,
        "url": resp.final_url,
        "title": title,
        "content": wrapped,
        "truncated": resp.truncated,
    }))
}

pub fn exec_extract_html_text(args: &Value) -> Result<Value> {
    let html = args
        .get("html")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AgentError::InvalidInput("missing required string 'html'".into()))?;
    if html.len() > MAX_EXTRACT_HTML_BYTES {
        return Err(AgentError::LimitExceeded(format!(
            "html input exceeds maximum size ({} > {MAX_EXTRACT_HTML_BYTES} bytes)",
            html.len()
        )));
    }
    let source = args
        .get("source")
        .and_then(|v| v.as_str())
        .unwrap_or("inline-html");

    let (title, text) = extract_title_and_text(html);
    let wrapped = wrap_untrusted_web_content(source, &text);

    Ok(json!({
        "title": title,
        "text": wrapped,
    }))
}

pub async fn exec_json_api_request(
    args: &Value,
    cfg: &ToolsConfig,
    _client: &reqwest::Client,
    approval_granted: bool,
) -> Result<Value> {
    let url = args
        .get("url")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AgentError::InvalidInput("missing required string 'url'".into()))?;
    let method_str = args
        .get("method")
        .and_then(|v| v.as_str())
        .unwrap_or("GET")
        .trim()
        .to_ascii_uppercase();

    let method = match method_str.as_str() {
        "GET" => reqwest::Method::GET,
        "HEAD" => reqwest::Method::HEAD,
        "POST" | "PUT" | "PATCH" | "DELETE" => {
            if !approval_granted {
                return Err(AgentError::PermissionDenied(format!(
                    "mutating json_api_request method '{method_str}' requires approval_granted"
                )));
            }
            method_str.parse::<reqwest::Method>().map_err(|_| {
                AgentError::InvalidInput(format!("invalid HTTP method '{method_str}'"))
            })?
        }
        other => {
            return Err(AgentError::InvalidInput(format!(
                "unsupported HTTP method '{other}'"
            )));
        }
    };

    let mut headers = parse_headers(args.get("headers"))?;
    if !headers.contains_key(CONTENT_TYPE) {
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    }

    let body = args
        .get("body")
        .filter(|v| !v.is_null())
        .map(|v| serde_json::to_vec(v).unwrap_or_default());

    let resp = fetch_validated_url(method, url, headers, body, cfg).await?;
    let body_text = String::from_utf8_lossy(&resp.body);
    let parsed_json: Option<Value> = serde_json::from_str(&body_text).ok();
    let wrapped_raw = wrap_untrusted_web_content(&resp.final_url, &body_text);

    Ok(json!({
        "status": resp.status,
        "url": resp.final_url,
        "json": parsed_json,
        "raw_body": wrapped_raw,
        "truncated": resp.truncated,
    }))
}

pub async fn exec_web_fetch(
    args: &Value,
    cfg: &ToolsConfig,
    _client: &reqwest::Client,
) -> Result<Value> {
    let url = args
        .get("url")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AgentError::InvalidInput("missing required string 'url'".into()))?;

    let extract_mode = args
        .get("extract_mode")
        .and_then(|v| v.as_str())
        .unwrap_or("markdown")
        .trim()
        .to_ascii_lowercase();
    if !matches!(extract_mode.as_str(), "markdown" | "text") {
        return Err(AgentError::InvalidInput(format!(
            "unsupported extract_mode '{extract_mode}'; expected 'markdown' or 'text'"
        )));
    }

    let max_chars = match args.get("max_chars") {
        None | Some(Value::Null) => None,
        Some(v) => {
            let n = v.as_u64().ok_or_else(|| {
                AgentError::InvalidInput("'max_chars' must be a positive integer".into())
            })?;
            if n == 0 {
                return Err(AgentError::InvalidInput("'max_chars' must be >= 1".into()));
            }
            Some(n as usize)
        }
    };

    let is_yt = is_youtube_url(url);
    if is_yt {
        let encoded_url: String = url::form_urlencoded::byte_serialize(url.as_bytes()).collect();
        let oembed_url = format!("https://www.youtube.com/oembed?url={encoded_url}&format=json");
        let mut oembed_headers = HeaderMap::new();
        oembed_headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
        if let Ok(oembed_resp) =
            fetch_validated_url(reqwest::Method::GET, &oembed_url, oembed_headers, None, cfg).await
        {
            if oembed_resp.status == 200 {
                if let Ok(val) = serde_json::from_slice::<Value>(&oembed_resp.body) {
                    let title = val
                        .get("title")
                        .and_then(|v| v.as_str())
                        .unwrap_or("YouTube Video")
                        .trim();
                    let author_name = val
                        .get("author_name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .trim();
                    let author_url = val
                        .get("author_url")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .trim();
                    let media_type = val
                        .get("type")
                        .and_then(|v| v.as_str())
                        .unwrap_or("video")
                        .trim();

                    let formatted = if extract_mode == "text" {
                        if !author_name.is_empty() {
                            format!("{title}\n\nChannel: {author_name} ({author_url})\nType: {media_type}\nURL: {url}")
                        } else {
                            format!("{title}\nType: {media_type}\nURL: {url}")
                        }
                    } else if !author_name.is_empty() {
                        format!("# {title}\n\n- **Platform**: YouTube ({media_type})\n- **Channel**: [{author_name}]({author_url})\n- **URL**: {url}\n\n[Video content available on YouTube]")
                    } else {
                        format!("# {title}\n\n- **Platform**: YouTube ({media_type})\n- **URL**: {url}\n\n[Video content available on YouTube]")
                    };

                    let (bounded, chars_truncated) = match max_chars {
                        Some(limit) => truncate_to_chars(&formatted, limit),
                        None => (formatted, false),
                    };
                    let wrapped = wrap_untrusted_web_content(url, &bounded);

                    return Ok(json!({
                        "ok": true,
                        "blocked_by_challenge": false,
                        "status": 200,
                        "url": url,
                        "title": Some(title.to_string()),
                        "extract_mode": extract_mode,
                        "content": wrapped,
                        "truncated": chars_truncated,
                    }));
                }
            }
        }
    }

    let mut headers = HeaderMap::new();
    headers.insert(
        ACCEPT,
        HeaderValue::from_static(
            "text/markdown, text/html;q=0.9, application/xhtml+xml;q=0.8, text/plain;q=0.7, */*;q=0.5",
        ),
    );
    let resp = fetch_validated_url(reqwest::Method::GET, url, headers, None, cfg).await?;
    let raw = String::from_utf8_lossy(&resp.body).into_owned();

    if let Some(kind) = detect_challenge_gate(resp.status, &resp.headers, &raw) {
        return Ok(json!({
            "ok": false,
            "blocked_by_challenge": true,
            "challenge_kind": kind,
            "status": resp.status,
            "url": resp.final_url,
        }));
    }

    let (og_title, og_desc) = extract_opengraph_metadata(&raw);

    let (mut title, extracted) = if looks_like_html(&resp.headers, &raw) {
        if extract_mode == "text" {
            extract_title_and_text(&raw)
        } else {
            extract_title_and_markdown(&raw, &resp.final_url)
        }
    } else {
        (None, raw)
    };

    if title.is_none() {
        title = og_title.clone();
    }

    let effective_content = if (is_yt || extracted.trim().is_empty())
        && (og_title.is_some() || og_desc.is_some())
    {
        let t = og_title.as_deref().or(title.as_deref()).unwrap_or("Media");
        let d = og_desc.as_deref().unwrap_or("");
        if extract_mode == "text" {
            if !d.is_empty() {
                format!("{t}\n\n{d}")
            } else {
                t.to_string()
            }
        } else if !d.is_empty() {
            format!("# {t}\n\n{d}")
        } else {
            format!("# {t}")
        }
    } else {
        extracted
    };

    let (bounded, chars_truncated) = match max_chars {
        Some(limit) => truncate_to_chars(&effective_content, limit),
        None => (effective_content, false),
    };
    let wrapped = wrap_untrusted_web_content(&resp.final_url, &bounded);

    Ok(json!({
        "ok": resp.status < 400,
        "blocked_by_challenge": false,
        "status": resp.status,
        "url": resp.final_url,
        "title": title,
        "extract_mode": extract_mode,
        "content": wrapped,
        "truncated": resp.truncated || chars_truncated,
    }))
}

fn is_youtube_url(url: &str) -> bool {
    static YT_RE: OnceLock<Regex> = OnceLock::new();
    let re = YT_RE.get_or_init(|| {
        RegexBuilder::new(
            r"(?i)(?:youtube\.com/(?:watch\?(?:.*&)?v=|shorts/)|youtu\.be/)([\w-]{11})",
        )
        .build()
        .expect("valid static regex")
    });
    re.is_match(url)
}

fn extract_opengraph_metadata(html: &str) -> (Option<String>, Option<String>) {
    static OG_TITLE_RE: OnceLock<Regex> = OnceLock::new();
    static OG_DESC_RE: OnceLock<Regex> = OnceLock::new();
    static OG_TITLE_REV: OnceLock<Regex> = OnceLock::new();
    static OG_DESC_REV: OnceLock<Regex> = OnceLock::new();

    let title_re = OG_TITLE_RE.get_or_init(|| {
        RegexBuilder::new(
            r#"(?is)<meta\b[^>]*?(?:property|name)\s*=\s*["'](?:og:title|twitter:title)["'][^>]*?content\s*=\s*["']([^"']*)["'][^>]*>"#,
        )
        .build()
        .expect("valid static regex")
    });
    let desc_re = OG_DESC_RE.get_or_init(|| {
        RegexBuilder::new(
            r#"(?is)<meta\b[^>]*?(?:property|name)\s*=\s*["'](?:og:description|twitter:description|description)["'][^>]*?content\s*=\s*["']([^"']*)["'][^>]*>"#,
        )
        .build()
        .expect("valid static regex")
    });

    let title_rev = OG_TITLE_REV.get_or_init(|| {
        RegexBuilder::new(
            r#"(?is)<meta\b[^>]*?content\s*=\s*["']([^"']*)["'][^>]*?(?:property|name)\s*=\s*["'](?:og:title|twitter:title)["'][^>]*>"#,
        )
        .build()
        .expect("valid static regex")
    });
    let desc_rev = OG_DESC_REV.get_or_init(|| {
        RegexBuilder::new(
            r#"(?is)<meta\b[^>]*?content\s*=\s*["']([^"']*)["'][^>]*?(?:property|name)\s*=\s*["'](?:og:description|twitter:description|description)["'][^>]*>"#,
        )
        .build()
        .expect("valid static regex")
    });

    let title = title_re
        .captures(html)
        .or_else(|| title_rev.captures(html))
        .map(|c| decode_html_entities(&c[1]))
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    let desc = desc_re
        .captures(html)
        .or_else(|| desc_rev.captures(html))
        .map(|c| decode_html_entities(&c[1]))
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    (title, desc)
}

#[derive(Debug, Clone)]
struct SearchResultItem {
    title: String,
    url: String,
    snippet: String,
}

fn resolve_search_provider(requested: Option<&str>, cfg: &ToolsConfig) -> Result<String> {
    let raw = requested
        .map(str::trim)
        .filter(|s| !s.is_empty() && !s.eq_ignore_ascii_case("auto"))
        .or_else(|| {
            let configured = cfg.web_search_provider.trim();
            if configured.is_empty() || configured.eq_ignore_ascii_case("auto") {
                None
            } else {
                Some(configured)
            }
        });

    let selected = match raw {
        Some(p) => p.to_ascii_lowercase(),
        None => {
            if !cfg.searxng_url.trim().is_empty() {
                "searxng".to_string()
            } else if !cfg.tavily_api_key.trim().is_empty() {
                "tavily".to_string()
            } else if !cfg.brave_api_key.trim().is_empty() {
                "brave".to_string()
            } else {
                "duckduckgo".to_string()
            }
        }
    };

    match selected.as_str() {
        "duckduckgo" | "duckduckgo_lite" | "searxng" | "brave" | "tavily" => Ok(selected),
        other => Err(AgentError::InvalidInput(format!(
            "unsupported web_search provider '{other}'; expected duckduckgo, duckduckgo_lite, searxng, brave, or tavily"
        ))),
    }
}

fn build_search_fallback_chain(starting_provider: &str, cfg: &ToolsConfig) -> Vec<String> {
    const CANONICAL_CHAIN: [&str; 5] = [
        "duckduckgo",
        "duckduckgo_lite",
        "searxng",
        "brave",
        "tavily",
    ];

    let start_idx = CANONICAL_CHAIN
        .iter()
        .position(|p| p.eq_ignore_ascii_case(starting_provider))
        .unwrap_or(0);

    let mut chain = Vec::new();
    chain.push(CANONICAL_CHAIN[start_idx].to_string());

    for p in &CANONICAL_CHAIN[start_idx + 1..] {
        match *p {
            "duckduckgo" | "duckduckgo_lite" => chain.push(p.to_string()),
            "searxng" => {
                if !cfg.searxng_url.trim().is_empty() {
                    chain.push(p.to_string());
                }
            }
            "brave" => {
                if !cfg.brave_api_key.trim().is_empty() {
                    chain.push(p.to_string());
                }
            }
            "tavily" => {
                if !cfg.tavily_api_key.trim().is_empty() {
                    chain.push(p.to_string());
                }
            }
            _ => {}
        }
    }
    chain
}

fn prepare_provider_request(
    provider: &str,
    effective_query: &str,
    freshness: Option<&str>,
    count: usize,
    cfg: &ToolsConfig,
) -> Result<(reqwest::Method, Url, HeaderMap, Option<Vec<u8>>)> {
    match provider {
        "duckduckgo" => {
            let mut u = Url::parse("https://html.duckduckgo.com/html/")
                .expect("valid static duckduckgo URL");
            {
                let mut pairs = u.query_pairs_mut();
                pairs.append_pair("q", effective_query);
                if let Some(f) = freshness {
                    let df = match f {
                        "day" | "pd" | "d" => Some("d"),
                        "week" | "pw" | "w" => Some("w"),
                        "month" | "pm" | "m" => Some("m"),
                        "year" | "py" | "y" => Some("y"),
                        _ => None,
                    };
                    if let Some(df_val) = df {
                        pairs.append_pair("df", df_val);
                    }
                }
            }
            let mut hdrs = HeaderMap::new();
            hdrs.insert(ACCEPT, HeaderValue::from_static("text/html,application/xhtml+xml"));
            Ok((reqwest::Method::GET, u, hdrs, None))
        }
        "duckduckgo_lite" => {
            let mut u = Url::parse("https://lite.duckduckgo.com/lite/")
                .expect("valid static duckduckgo lite URL");
            {
                let mut pairs = u.query_pairs_mut();
                pairs.append_pair("q", effective_query);
                if let Some(f) = freshness {
                    let df = match f {
                        "day" | "pd" | "d" => Some("d"),
                        "week" | "pw" | "w" => Some("w"),
                        "month" | "pm" | "m" => Some("m"),
                        "year" | "py" | "y" => Some("y"),
                        _ => None,
                    };
                    if let Some(df_val) = df {
                        pairs.append_pair("df", df_val);
                    }
                }
            }
            let mut hdrs = HeaderMap::new();
            hdrs.insert(ACCEPT, HeaderValue::from_static("text/html,application/xhtml+xml"));
            Ok((reqwest::Method::GET, u, hdrs, None))
        }
        "searxng" => {
            if cfg.searxng_url.trim().is_empty() {
                return Err(AgentError::InvalidInput(
                    "searxng search requires tools.searxng_url to be configured".into(),
                ));
            }
            let mut u = searxng_search_url(&cfg.searxng_url)?;
            {
                let mut pairs = u.query_pairs_mut();
                pairs.append_pair("q", effective_query);
                pairs.append_pair("format", "json");
                if let Some(f) = freshness {
                    let tr = match f {
                        "pd" | "d" => "day",
                        "pw" | "w" => "week",
                        "pm" | "m" => "month",
                        "py" | "y" => "year",
                        other => other,
                    };
                    pairs.append_pair("time_range", tr);
                }
            }
            let mut hdrs = HeaderMap::new();
            hdrs.insert(ACCEPT, HeaderValue::from_static("application/json"));
            Ok((reqwest::Method::GET, u, hdrs, None))
        }
        "brave" => {
            if cfg.brave_api_key.trim().is_empty() {
                return Err(AgentError::InvalidInput(
                    "brave search requires tools.brave_api_key to be configured".into(),
                ));
            }
            let mut u = Url::parse("https://api.search.brave.com/res/v1/web/search")
                .expect("valid static brave URL");
            {
                let mut pairs = u.query_pairs_mut();
                pairs.append_pair("q", effective_query);
                pairs.append_pair("count", &count.to_string());
                if let Some(f) = freshness {
                    let bf = match f {
                        "day" | "d" => "pd",
                        "week" | "w" => "pw",
                        "month" | "m" => "pm",
                        "year" | "y" => "py",
                        other => other,
                    };
                    pairs.append_pair("freshness", bf);
                }
            }
            let mut hdrs = HeaderMap::new();
            hdrs.insert(ACCEPT, HeaderValue::from_static("application/json"));
            let token = HeaderValue::from_str(cfg.brave_api_key.trim()).map_err(|e| {
                AgentError::InvalidInput(format!("invalid tools.brave_api_key header value: {e}"))
            })?;
            hdrs.insert(HeaderName::from_static("x-subscription-token"), token);
            Ok((reqwest::Method::GET, u, hdrs, None))
        }
        "tavily" => {
            if cfg.tavily_api_key.trim().is_empty() {
                return Err(AgentError::InvalidInput(
                    "tavily search requires tools.tavily_api_key to be configured".into(),
                ));
            }
            let u = Url::parse("https://api.tavily.com/search")
                .expect("valid static tavily URL");
            let mut payload = json!({
                "api_key": cfg.tavily_api_key.trim(),
                "query": effective_query,
                "max_results": count,
                "search_depth": "basic",
            });
            if let Some(f) = freshness {
                let tr = match f {
                    "pd" | "d" => "day",
                    "pw" | "w" => "week",
                    "pm" | "m" => "month",
                    "py" | "y" => "year",
                    other => other,
                };
                payload["time_range"] = Value::String(tr.to_string());
            }
            let mut hdrs = HeaderMap::new();
            hdrs.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
            hdrs.insert(ACCEPT, HeaderValue::from_static("application/json"));
            Ok((
                reqwest::Method::POST,
                u,
                hdrs,
                Some(serde_json::to_vec(&payload).unwrap_or_default()),
            ))
        }
        other => Err(AgentError::InvalidInput(format!(
            "unsupported web_search provider '{other}'"
        ))),
    }
}

fn normalize_freshness(raw: Option<&str>) -> Option<String> {
    raw.map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_ascii_lowercase())
}

fn searxng_search_url(base: &str) -> Result<Url> {
    let mut url = Url::parse(base.trim())
        .map_err(|e| AgentError::InvalidInput(format!("invalid tools.searxng_url: {e}")))?;
    let path = url.path().trim_end_matches('/');
    if !path.ends_with("/search") {
        let new_path = if path.is_empty() {
            "/search".to_string()
        } else {
            format!("{path}/search")
        };
        url.set_path(&new_path);
    }
    Ok(url)
}

fn format_search_response(
    provider: &str,
    query: &str,
    cached: bool,
    items: &[SearchResultItem],
) -> Value {
    let results: Vec<Value> = items
        .iter()
        .map(|item| {
            json!({
                "title": sanitize_closing_untrusted_tag(&item.title),
                "url": item.url,
                "snippet": wrap_untrusted_web_content(&item.url, &item.snippet),
            })
        })
        .collect();
    json!({
        "ok": true,
        "blocked_by_challenge": false,
        "provider": provider,
        "query": query,
        "count": results.len(),
        "cached": cached,
        "results": results,
    })
}

pub async fn exec_web_search(
    args: &Value,
    cfg: &ToolsConfig,
    _client: &reqwest::Client,
) -> Result<Value> {
    if !cfg.network_access {
        return Err(AgentError::PermissionDenied(
            "outbound network access is disabled by policy (tools.network_access = false)".into(),
        ));
    }

    let raw_query = args
        .get("query")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AgentError::InvalidInput("missing required string 'query'".into()))?;

    let count = args
        .get("count")
        .and_then(|v| v.as_u64())
        .map(|n| n.clamp(1, 20) as usize)
        .unwrap_or(5);

    let domain = args
        .get("domain")
        .and_then(|v| v.as_str())
        .map(|s| normalize_host(s))
        .filter(|s| !s.is_empty());

    let freshness = normalize_freshness(args.get("freshness").and_then(|v| v.as_str()));
    let initial_provider =
        resolve_search_provider(args.get("provider").and_then(|v| v.as_str()), cfg)?;

    let effective_query = match domain {
        Some(ref dom) if !raw_query.to_ascii_lowercase().contains("site:") => {
            format!("site:{dom} {raw_query}")
        }
        _ => raw_query.to_string(),
    };

    let chain = build_search_fallback_chain(&initial_provider, cfg);
    let mut last_challenge: Option<(String, String, u16, String)> = None;
    let mut last_error: Option<String> = None;

    for current_provider in chain {
        let (method, request_url, mut headers, body) = match prepare_provider_request(
            &current_provider,
            &effective_query,
            freshness.as_deref(),
            count,
            cfg,
        ) {
            Ok(req) => req,
            Err(e) => {
                last_error = Some(e.to_string());
                continue;
            }
        };

        if let Err(e) = validate_web_url(request_url.as_str(), cfg).await {
            last_error = Some(e.to_string());
            continue;
        }

        let cache_key = SearchCacheKey {
            provider: current_provider.clone(),
            endpoint_url: request_url.to_string(),
            query: effective_query.clone(),
            count,
            freshness: freshness.clone(),
            domain: domain.clone(),
        };

        if let Ok(mut cache) = search_cache().lock() {
            if let Some(mut cached_resp) = cache.get(&cache_key) {
                if let Some(obj) = cached_resp.as_object_mut() {
                    obj.insert("cached".to_string(), Value::Bool(true));
                }
                return Ok(cached_resp);
            }
        }

        headers.entry(ACCEPT).or_insert_with(|| HeaderValue::from_static("*/*"));

        let mut resp_result = fetch_validated_url(
            method.clone(),
            request_url.as_str(),
            headers.clone(),
            body.clone(),
            cfg,
        )
        .await;

        let needs_retry = match &resp_result {
            Ok(resp) => {
                resp.status == 429
                    || resp.status == 503
                    || detect_challenge_gate(
                        resp.status,
                        &resp.headers,
                        &String::from_utf8_lossy(&resp.body),
                    )
                    .is_some()
            }
            Err(err) => matches!(err, AgentError::Timeout(_)),
        };

        if needs_retry {
            let jitter_bytes = uuid::Uuid::new_v4();
            let jitter_ms =
                u64::from_le_bytes(jitter_bytes.as_bytes()[..8].try_into().unwrap()) % 250;
            tokio::time::sleep(Duration::from_millis(500 + jitter_ms)).await;

            resp_result = fetch_validated_url(
                method,
                request_url.as_str(),
                headers,
                body,
                cfg,
            )
            .await;
        }

        let resp = match resp_result {
            Ok(r) => r,
            Err(err) => {
                last_error = Some(err.to_string());
                continue;
            }
        };

        let body_text = String::from_utf8_lossy(&resp.body);
        if let Some(kind) = detect_challenge_gate(resp.status, &resp.headers, &body_text) {
            last_challenge = Some((current_provider.clone(), kind, resp.status, resp.final_url));
            continue;
        }

        if resp.status >= 400 {
            last_error = Some(format!(
                "search provider '{current_provider}' returned HTTP {}",
                resp.status
            ));
            continue;
        }

        let items = match current_provider.as_str() {
            "duckduckgo" => parse_duckduckgo_html(&body_text, count),
            "duckduckgo_lite" => parse_duckduckgo_lite_html(&body_text, count),
            "searxng" => match parse_searxng_json(&body_text, count) {
                Ok(items) => items,
                Err(e) => {
                    last_error = Some(e.to_string());
                    continue;
                }
            },
            "brave" => match parse_brave_json(&body_text, count) {
                Ok(items) => items,
                Err(e) => {
                    last_error = Some(e.to_string());
                    continue;
                }
            },
            "tavily" => match parse_tavily_json(&body_text, count) {
                Ok(items) => items,
                Err(e) => {
                    last_error = Some(e.to_string());
                    continue;
                }
            },
            _ => Vec::new(),
        };

        let response = format_search_response(&current_provider, &effective_query, false, &items);
        if let Ok(mut cache) = search_cache().lock() {
            cache.insert(cache_key, response.clone());
        }
        return Ok(response);
    }

    if let Some((provider, kind, status, final_url)) = last_challenge {
        return Ok(json!({
            "ok": false,
            "blocked_by_challenge": true,
            "challenge_kind": kind,
            "provider": provider,
            "status": status,
            "url": final_url,
        }));
    }

    Err(AgentError::Tool {
        tool_id: "web_search".into(),
        message: last_error
            .unwrap_or_else(|| "all search providers in fallback chain failed".into()),
    })
}

fn parse_json_search_items(
    arr: &[Value],
    limit: usize,
    snippet_keys: &[&str],
) -> Vec<SearchResultItem> {
    let mut out = Vec::new();
    for item in arr.iter().take(limit) {
        let Some(url) = item.get("url").and_then(|v| v.as_str()).map(str::trim) else {
            continue;
        };
        if url.is_empty() {
            continue;
        }
        let title = item
            .get("title")
            .and_then(|v| v.as_str())
            .unwrap_or(url)
            .trim()
            .to_string();
        let snippet = snippet_keys
            .iter()
            .find_map(|key| item.get(*key).and_then(|v| v.as_str()))
            .unwrap_or("")
            .trim()
            .to_string();
        out.push(SearchResultItem {
            title,
            url: url.to_string(),
            snippet,
        });
    }
    out
}

fn parse_searxng_json(raw: &str, limit: usize) -> Result<Vec<SearchResultItem>> {
    let val: Value = serde_json::from_str(raw).map_err(|e| AgentError::Tool {
        tool_id: "web_search".into(),
        message: format!("invalid JSON from searxng: {e}"),
    })?;
    let Some(arr) = val.get("results").and_then(|v| v.as_array()) else {
        return Ok(Vec::new());
    };
    Ok(parse_json_search_items(arr, limit, &["content", "snippet"]))
}

fn parse_brave_json(raw: &str, limit: usize) -> Result<Vec<SearchResultItem>> {
    let val: Value = serde_json::from_str(raw).map_err(|e| AgentError::Tool {
        tool_id: "web_search".into(),
        message: format!("invalid JSON from brave search: {e}"),
    })?;
    let Some(arr) = val
        .get("web")
        .and_then(|w| w.get("results"))
        .and_then(|v| v.as_array())
    else {
        return Ok(Vec::new());
    };
    Ok(parse_json_search_items(arr, limit, &["description", "snippet"]))
}

fn parse_tavily_json(raw: &str, limit: usize) -> Result<Vec<SearchResultItem>> {
    let val: Value = serde_json::from_str(raw).map_err(|e| AgentError::Tool {
        tool_id: "web_search".into(),
        message: format!("invalid JSON from tavily search: {e}"),
    })?;
    let Some(arr) = val.get("results").and_then(|v| v.as_array()) else {
        return Ok(Vec::new());
    };
    Ok(parse_json_search_items(arr, limit, &["content", "snippet"]))
}

fn extract_attr(attrs: &str, attr_name: &str) -> Option<String> {
    static ATTR_DOUBLE: OnceLock<Regex> = OnceLock::new();
    static ATTR_SINGLE: OnceLock<Regex> = OnceLock::new();
    let dbl = ATTR_DOUBLE.get_or_init(|| {
        RegexBuilder::new(r#"([a-zA-Z_:][a-zA-Z0-9_.:-]*)\s*=\s*"([^"]*)""#)
            .build()
            .expect("valid static regex")
    });
    let sgl = ATTR_SINGLE.get_or_init(|| {
        RegexBuilder::new(r#"([a-zA-Z_:][a-zA-Z0-9_.:-]*)\s*=\s*'([^']*)'"#)
            .build()
            .expect("valid static regex")
    });
    for cap in dbl.captures_iter(attrs) {
        if cap[1].eq_ignore_ascii_case(attr_name) {
            return Some(decode_html_entities(&cap[2]));
        }
    }
    for cap in sgl.captures_iter(attrs) {
        if cap[1].eq_ignore_ascii_case(attr_name) {
            return Some(decode_html_entities(&cap[2]));
        }
    }
    None
}

fn has_css_class(attrs: &str, targets: &[&str]) -> bool {
    let Some(cls) = extract_attr(attrs, "class") else {
        return false;
    };
    cls.split_whitespace().any(|tok| targets.contains(&tok))
}

fn unwrap_duckduckgo_href(raw_href: &str) -> Option<String> {
    let trimmed = raw_href.trim();
    if trimmed.is_empty() {
        return None;
    }
    let normalized = if trimmed.starts_with("//") {
        format!("https:{trimmed}")
    } else if trimmed.starts_with('/') {
        format!("https://duckduckgo.com{trimmed}")
    } else {
        trimmed.to_string()
    };
    let parsed = Url::parse(&normalized).ok()?;
    if parsed
        .host_str()
        .map_or(false, |h| h.eq_ignore_ascii_case("duckduckgo.com") || h.ends_with(".duckduckgo.com"))
    {
        if parsed.path().starts_with("/y.js") {
            return None;
        }
        if parsed.path().starts_with("/l/") {
            for (k, v) in parsed.query_pairs() {
                if k == "uddg" {
                    let target = v.trim();
                    if !target.is_empty() {
                        if let Ok(u) = Url::parse(target) {
                            if matches!(u.scheme(), "http" | "https") {
                                return Some(u.to_string());
                            }
                        }
                    }
                }
            }
            return None;
        }
    }
    if matches!(parsed.scheme(), "http" | "https") {
        Some(parsed.to_string())
    } else {
        None
    }
}

fn strip_tags_and_collapse(html_fragment: &str) -> String {
    let stripped = any_tag_regex().replace_all(html_fragment, " ");
    let decoded = decode_html_entities(&stripped);
    decoded.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn extract_ddg_snippet(segment: &str) -> Option<String> {
    static SNIPPET_A: OnceLock<Regex> = OnceLock::new();
    static SNIPPET_TD: OnceLock<Regex> = OnceLock::new();
    static SNIPPET_DIV: OnceLock<Regex> = OnceLock::new();
    static SNIPPET_SPAN: OnceLock<Regex> = OnceLock::new();

    let patterns = [
        SNIPPET_A.get_or_init(|| {
            RegexBuilder::new(r"(?is)<a\b([^>]*)>(.*?)</a>")
                .build()
                .expect("valid static regex")
        }),
        SNIPPET_TD.get_or_init(|| {
            RegexBuilder::new(r"(?is)<td\b([^>]*)>(.*?)</td>")
                .build()
                .expect("valid static regex")
        }),
        SNIPPET_DIV.get_or_init(|| {
            RegexBuilder::new(r"(?is)<div\b([^>]*)>(.*?)</div>")
                .build()
                .expect("valid static regex")
        }),
        SNIPPET_SPAN.get_or_init(|| {
            RegexBuilder::new(r"(?is)<span\b([^>]*)>(.*?)</span>")
                .build()
                .expect("valid static regex")
        }),
    ];

    for re in patterns {
        for cap in re.captures_iter(segment) {
            if has_css_class(&cap[1], &["result__snippet", "result-snippet"]) {
                let text = strip_tags_and_collapse(&cap[2]);
                if !text.is_empty() {
                    return Some(text);
                }
            }
        }
    }
    None
}

fn parse_duckduckgo_html(html: &str, limit: usize) -> Vec<SearchResultItem> {
    static ANCHOR_RE: OnceLock<Regex> = OnceLock::new();
    let anchor_re = ANCHOR_RE.get_or_init(|| {
        RegexBuilder::new(r"(?is)<a\b([^>]*)>(.*?)</a>")
            .build()
            .expect("valid static regex")
    });

    struct TitleHit {
        end: usize,
        start: usize,
        title: String,
        url: String,
    }

    let mut hits = Vec::new();
    for cap in anchor_re.captures_iter(html) {
        let whole = match cap.get(0) {
            Some(m) => m,
            None => continue,
        };
        let attrs = &cap[1];
        if !has_css_class(attrs, &["result__a", "result-link"]) {
            continue;
        }
        let Some(href) = extract_attr(attrs, "href") else {
            continue;
        };
        let Some(url) = unwrap_duckduckgo_href(&href) else {
            continue;
        };
        let title = strip_tags_and_collapse(&cap[2]);
        hits.push(TitleHit {
            start: whole.start(),
            end: whole.end(),
            title: if title.is_empty() { url.clone() } else { title },
            url,
        });
    }

    let mut results = Vec::new();
    for (idx, hit) in hits.iter().enumerate().take(limit) {
        let next_start = hits.get(idx + 1).map_or(html.len(), |n| n.start);
        let segment = &html[hit.end..next_start];
        let snippet = extract_ddg_snippet(segment).unwrap_or_default();
        results.push(SearchResultItem {
            title: hit.title.clone(),
            url: hit.url.clone(),
            snippet,
        });
    }
    results
}

fn parse_duckduckgo_lite_html(html: &str, limit: usize) -> Vec<SearchResultItem> {
    let standard = parse_duckduckgo_html(html, limit);
    if !standard.is_empty() {
        return standard;
    }

    static TABLE_ROW_RE: OnceLock<Regex> = OnceLock::new();
    let row_re = TABLE_ROW_RE.get_or_init(|| {
        RegexBuilder::new(r"(?is)<tr\b[^>]*>(.*?)</tr>")
            .build()
            .expect("valid static regex")
    });
    static ANCHOR_RE: OnceLock<Regex> = OnceLock::new();
    let anchor_re = ANCHOR_RE.get_or_init(|| {
        RegexBuilder::new(r"(?is)<a\b([^>]*)>(.*?)</a>")
            .build()
            .expect("valid static regex")
    });

    let mut results = Vec::new();
    let mut pending_title_url: Option<(String, String)> = None;

    for row_cap in row_re.captures_iter(html) {
        if results.len() >= limit {
            break;
        }
        let row_content = &row_cap[1];

        let mut found_anchor = None;
        for cap in anchor_re.captures_iter(row_content) {
            let attrs = &cap[1];
            if let Some(href) = extract_attr(attrs, "href") {
                if let Some(unwrapped) = unwrap_duckduckgo_href(&href) {
                    let title = strip_tags_and_collapse(&cap[2]);
                    if !title.is_empty() && !is_ddg_nav_title(&title) {
                        found_anchor = Some((title, unwrapped));
                        break;
                    }
                }
            }
        }

        if let Some((t, u)) = found_anchor {
            if let Some((prev_t, prev_u)) = pending_title_url.take() {
                results.push(SearchResultItem {
                    title: prev_t,
                    url: prev_u,
                    snippet: String::new(),
                });
            }
            pending_title_url = Some((t, u));
        } else if let Some((prev_t, prev_u)) = pending_title_url.take() {
            let snippet = extract_ddg_snippet(row_content)
                .unwrap_or_else(|| strip_tags_and_collapse(row_content));
            results.push(SearchResultItem {
                title: prev_t,
                url: prev_u,
                snippet,
            });
        }
    }

    if let Some((prev_t, prev_u)) = pending_title_url {
        if results.len() < limit {
            results.push(SearchResultItem {
                title: prev_t,
                url: prev_u,
                snippet: String::new(),
            });
        }
    }

    results
}

fn is_ddg_nav_title(title: &str) -> bool {
    let lower = title.to_ascii_lowercase();
    matches!(
        lower.as_str(),
        "next" | "previous" | "next page" | "prev page" | "duckduckgo" | "feedback"
    )
}

fn truncate_to_chars(s: &str, max_chars: usize) -> (String, bool) {
    match s.char_indices().nth(max_chars) {
        Some((idx, _)) => (s[..idx].to_string(), true),
        None => (s.to_string(), false),
    }
}

fn any_tag_regex() -> &'static Regex {
    static ANY_TAG_RE: OnceLock<Regex> = OnceLock::new();
    ANY_TAG_RE.get_or_init(|| {
        RegexBuilder::new(r"(?s)<[^>]+>")
            .build()
            .expect("valid static regex")
    })
}

fn strip_non_content_html(html: &str) -> String {
    static SCRIPT_RE: OnceLock<Regex> = OnceLock::new();
    static STYLE_RE: OnceLock<Regex> = OnceLock::new();
    static NOSCRIPT_RE: OnceLock<Regex> = OnceLock::new();
    static TEMPLATE_RE: OnceLock<Regex> = OnceLock::new();
    static COMMENTS_RE: OnceLock<Regex> = OnceLock::new();

    let script_re = SCRIPT_RE.get_or_init(|| {
        RegexBuilder::new(r"(?is)<script\b[^>]*>.*?</script\s*>")
            .build()
            .expect("valid static regex")
    });
    let style_re = STYLE_RE.get_or_init(|| {
        RegexBuilder::new(r"(?is)<style\b[^>]*>.*?</style\s*>")
            .build()
            .expect("valid static regex")
    });
    let noscript_re = NOSCRIPT_RE.get_or_init(|| {
        RegexBuilder::new(r"(?is)<noscript\b[^>]*>.*?</noscript\s*>")
            .build()
            .expect("valid static regex")
    });
    let template_re = TEMPLATE_RE.get_or_init(|| {
        RegexBuilder::new(r"(?is)<template\b[^>]*>.*?</template\s*>")
            .build()
            .expect("valid static regex")
    });
    let comments_re = COMMENTS_RE.get_or_init(|| {
        RegexBuilder::new(r"(?s)<!--.*?-->")
            .build()
            .expect("valid static regex")
    });

    let s1 = comments_re.replace_all(html, " ");
    let s2 = script_re.replace_all(&s1, " ");
    let s3 = style_re.replace_all(&s2, " ");
    let s4 = noscript_re.replace_all(&s3, " ");
    template_re.replace_all(&s4, " ").into_owned()
}

fn extract_html_title(html: &str) -> Option<String> {
    static TITLE_RE: OnceLock<Regex> = OnceLock::new();
    let title_re = TITLE_RE.get_or_init(|| {
        RegexBuilder::new(r"(?is)<title[^>]*>(.*?)</title>")
            .build()
            .expect("valid static regex")
    });
    title_re
        .captures(html)
        .and_then(|cap| cap.get(1))
        .map(|m| decode_html_entities(m.as_str()).trim().to_string())
        .filter(|s| !s.is_empty())
}

fn extract_title_and_text(html: &str) -> (Option<String>, String) {
    static BLOCK_TAGS_RE: OnceLock<Regex> = OnceLock::new();

    let title = extract_html_title(html);
    let cleaned = strip_non_content_html(html);

    let block_tags = BLOCK_TAGS_RE.get_or_init(|| {
        RegexBuilder::new(r"(?i)</?(p|div|br|li|tr|h[1-6]|section|article|header|footer)[^>]*>")
            .build()
            .expect("valid static regex")
    });
    let with_newlines = block_tags.replace_all(&cleaned, "\n");
    let stripped = any_tag_regex().replace_all(&with_newlines, " ");
    let decoded = decode_html_entities(&stripped);

    let normalized = decoded
        .lines()
        .map(|line| line.split_whitespace().collect::<Vec<_>>().join(" "))
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n");

    (title, normalized)
}

fn extract_title_and_markdown(html: &str, base_url: &str) -> (Option<String>, String) {
    static PRE_RE: OnceLock<Regex> = OnceLock::new();
    static INLINE_CODE_RE: OnceLock<Regex> = OnceLock::new();
    static ANCHOR_RE: OnceLock<Regex> = OnceLock::new();
    static TITLE_TAG_RE: OnceLock<Regex> = OnceLock::new();
    static LI_RE: OnceLock<Regex> = OnceLock::new();
    static BR_RE: OnceLock<Regex> = OnceLock::new();
    static HR_RE: OnceLock<Regex> = OnceLock::new();
    static BLOCK_RE: OnceLock<Regex> = OnceLock::new();
    static CELL_RE: OnceLock<Regex> = OnceLock::new();
    static HEADING_RES: OnceLock<[Regex; 6]> = OnceLock::new();
    static CODE_LANG_RE: OnceLock<Regex> = OnceLock::new();

    let title = extract_html_title(html);
    let safe_input = html.replace('\0', "");
    let title_removed = TITLE_TAG_RE
        .get_or_init(|| {
            RegexBuilder::new(r"(?is)<title\b[^>]*>.*?</title\s*>")
                .build()
                .expect("valid static regex")
        })
        .replace_all(&safe_input, "");
    let mut work = strip_non_content_html(&title_removed);

    // 1. Protect <pre>...</pre> blocks before whitespace normalization.
    let pre_re = PRE_RE.get_or_init(|| {
        RegexBuilder::new(r"(?is)<pre\b([^>]*)>(.*?)</pre\s*>")
            .build()
            .expect("valid static regex")
    });
    let lang_re = CODE_LANG_RE.get_or_init(|| {
        RegexBuilder::new(r"(?is)<code\b[^>]*class\s*=\s*[^>]*language-([a-zA-Z0-9_+-]+)")
            .build()
            .expect("valid static regex")
    });
    let mut pre_blocks: Vec<String> = Vec::new();
    work = pre_re
        .replace_all(&work, |caps: &regex::Captures<'_>| {
            let lang = lang_re
                .captures(&caps[1])
                .or_else(|| lang_re.captures(&caps[2]))
                .and_then(|c| c.get(1))
                .map(|m| m.as_str().to_string())
                .unwrap_or_default();
            let raw_inner = any_tag_regex().replace_all(&caps[2], "");
            let decoded_inner = decode_html_entities(&raw_inner);
            let trimmed_inner = decoded_inner.trim_matches(|c| c == '\r' || c == '\n');
            let idx = pre_blocks.len();
            pre_blocks.push(format!("\n```{lang}\n{trimmed_inner}\n```\n"));
            format!("\n\x00PS5_PRE_{idx}\x00\n")
        })
        .into_owned();

    // 2. Inline <code>...</code>
    let inline_code_re = INLINE_CODE_RE.get_or_init(|| {
        RegexBuilder::new(r"(?is)<code\b[^>]*>(.*?)</code\s*>")
            .build()
            .expect("valid static regex")
    });
    work = inline_code_re
        .replace_all(&work, |caps: &regex::Captures<'_>| {
            let text = strip_tags_and_collapse(&caps[1]);
            if text.is_empty() {
                String::new()
            } else {
                format!("`{}`", text.replace('`', "'"))
            }
        })
        .into_owned();

    // 3. Anchor links <a href="...">text</a>
    let base = Url::parse(base_url).ok();
    let anchor_re = ANCHOR_RE.get_or_init(|| {
        RegexBuilder::new(r"(?is)<a\b([^>]*)>(.*?)</a\s*>")
            .build()
            .expect("valid static regex")
    });
    work = anchor_re
        .replace_all(&work, |caps: &regex::Captures<'_>| {
            let text = strip_tags_and_collapse(&caps[2]);
            let Some(href) = extract_attr(&caps[1], "href") else {
                return text;
            };
            let href_trimmed = href.trim();
            if href_trimmed.is_empty()
                || href_trimmed.starts_with('#')
                || href_trimmed.to_ascii_lowercase().starts_with("javascript:")
                || href_trimmed.to_ascii_lowercase().starts_with("data:")
            {
                return text;
            }
            let resolved = match base.as_ref().and_then(|b| b.join(href_trimmed).ok()) {
                Some(u) if matches!(u.scheme(), "http" | "https" | "mailto") => u.to_string(),
                _ => match Url::parse(href_trimmed) {
                    Ok(u) if matches!(u.scheme(), "http" | "https" | "mailto") => u.to_string(),
                    _ => return text,
                },
            };
            if text.is_empty() {
                format!("[{resolved}]({resolved})")
            } else {
                format!("[{text}]({resolved})")
            }
        })
        .into_owned();

    // 4. Headings <h1>..<h6> (individual non-backreference regexes).
    let heading_res = HEADING_RES.get_or_init(|| {
        [
            RegexBuilder::new(r"(?is)<h1\b[^>]*>(.*?)</h1\s*>").build().expect("valid static regex"),
            RegexBuilder::new(r"(?is)<h2\b[^>]*>(.*?)</h2\s*>").build().expect("valid static regex"),
            RegexBuilder::new(r"(?is)<h3\b[^>]*>(.*?)</h3\s*>").build().expect("valid static regex"),
            RegexBuilder::new(r"(?is)<h4\b[^>]*>(.*?)</h4\s*>").build().expect("valid static regex"),
            RegexBuilder::new(r"(?is)<h5\b[^>]*>(.*?)</h5\s*>").build().expect("valid static regex"),
            RegexBuilder::new(r"(?is)<h6\b[^>]*>(.*?)</h6\s*>").build().expect("valid static regex"),
        ]
    });
    for (idx, re) in heading_res.iter().enumerate() {
        let hashes = "#".repeat(idx + 1);
        work = re
            .replace_all(&work, |caps: &regex::Captures<'_>| {
                let text = strip_tags_and_collapse(&caps[1]);
                if text.is_empty() {
                    String::new()
                } else {
                    format!("\n\n{hashes} {text}\n\n")
                }
            })
            .into_owned();
    }

    // 5. Lists, breaks, horizontal rules, table cells, and block boundaries.
    let li_re = LI_RE.get_or_init(|| {
        RegexBuilder::new(r"(?is)<li\b[^>]*>")
            .build()
            .expect("valid static regex")
    });
    work = li_re.replace_all(&work, "\n- ").into_owned();

    let br_re = BR_RE.get_or_init(|| {
        RegexBuilder::new(r"(?is)<br\s*/?>")
            .build()
            .expect("valid static regex")
    });
    work = br_re.replace_all(&work, "\n").into_owned();

    let hr_re = HR_RE.get_or_init(|| {
        RegexBuilder::new(r"(?is)<hr\s*/?>")
            .build()
            .expect("valid static regex")
    });
    work = hr_re.replace_all(&work, "\n---\n").into_owned();

    let cell_re = CELL_RE.get_or_init(|| {
        RegexBuilder::new(r"(?is)</t[dh]\s*>")
            .build()
            .expect("valid static regex")
    });
    work = cell_re.replace_all(&work, " | ").into_owned();

    let block_re = BLOCK_RE.get_or_init(|| {
        RegexBuilder::new(
            r"(?i)</?(p|div|section|article|main|header|footer|aside|nav|blockquote|tr|table|ul|ol|li|dd|dt|dl)[^>]*>",
        )
        .build()
        .expect("valid static regex")
    });
    work = block_re.replace_all(&work, "\n").into_owned();

    let stripped = any_tag_regex().replace_all(&work, " ");
    let decoded = decode_html_entities(&stripped);

    let mut lines = Vec::new();
    let mut blank_run = 0usize;
    for raw_line in decoded.lines() {
        let collapsed = raw_line.split_whitespace().collect::<Vec<_>>().join(" ");
        if collapsed.is_empty() {
            blank_run += 1;
            if blank_run <= 1 && !lines.is_empty() {
                lines.push(String::new());
            }
        } else {
            blank_run = 0;
            lines.push(collapsed);
        }
    }
    while lines.last().map_or(false, |l| l.is_empty()) {
        lines.pop();
    }

    let mut md = lines.join("\n");
    for (idx, block) in pre_blocks.iter().enumerate() {
        let token = format!("\x00PS5_PRE_{idx}\x00");
        md = md.replace(&token, block.trim_matches('\n'));
    }

    if let Some(ref t) = title {
        let h1 = format!("# {t}");
        if !md.starts_with(&h1) {
            if md.is_empty() {
                md = h1;
            } else {
                md = format!("{h1}\n\n{md}");
            }
        }
    }

    (title, md.trim().to_string())
}

fn decode_html_entities(s: &str) -> String {
    static NUM_ENTITY_RE: OnceLock<Regex> = OnceLock::new();
    let base = s
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&apos;", "'")
        .replace("&nbsp;", " ")
        .replace("&ndash;", "-")
        .replace("&mdash;", "--")
        .replace("&hellip;", "...")
        .replace("&rsquo;", "'")
        .replace("&lsquo;", "'")
        .replace("&rdquo;", "\"")
        .replace("&ldquo;", "\"");

    if !base.contains("&#") {
        return base;
    }

    let num_re = NUM_ENTITY_RE.get_or_init(|| {
        RegexBuilder::new(r"&#(?:x([0-9a-fA-F]{1,6})|([0-9]{1,7}));")
            .build()
            .expect("valid static regex")
    });
    num_re
        .replace_all(&base, |caps: &regex::Captures<'_>| {
            let code = if let Some(hex) = caps.get(1) {
                u32::from_str_radix(hex.as_str(), 16).ok()
            } else if let Some(dec) = caps.get(2) {
                dec.as_str().parse::<u32>().ok()
            } else {
                None
            };
            code.filter(|&cp| cp != 0)
                .and_then(char::from_u32)
                .map(|ch| ch.to_string())
                .unwrap_or_else(|| caps[0].to_string())
        })
        .into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use axum::routing::{get, post};
    use axum::Router;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn local_cfg(port_allowed: bool) -> ToolsConfig {
        ToolsConfig {
            allowed_domains: vec!["*".into()],
            allow_local_network: port_allowed,
            block_private_lan_web_requests: !port_allowed,
            ..ToolsConfig::default()
        }
    }

    #[test]
    fn forbidden_ipv6_embedded_and_special_ranges_blocked() {
        for addr_str in [
            "::1",
            "::",
            "::127.0.0.1",
            "::8.8.8.8",
            "::ffff:127.0.0.1",
            "::ffff:169.254.169.254",
            "::ffff:0:127.0.0.1",
            "64:ff9b::127.0.0.1",
            "64:ff9b:1::10.0.0.1",
            "2002:7f00:1::1",
            "2001::1",
            "100::1",
            "2001:2::1",
            "2001:db8::1",
            "fd00:ec2::254",
        ] {
            let ip: IpAddr = addr_str.parse().unwrap();
            assert!(is_forbidden_ip(ip), "expected {addr_str} to be forbidden");
        }

        for public_str in ["2606:4700:4700::1111", "2001:4860:4860::8888", "93.184.216.34"] {
            let ip: IpAddr = public_str.parse().unwrap();
            assert!(!is_forbidden_ip(ip), "expected {public_str} to be public");
        }
    }

    #[test]
    fn domain_matching_handles_trailing_dots_and_bad_wildcards() {
        assert!(!domain_matches("example.com", &["*.".into(), ".".into()]));
        assert!(domain_matches("api.example.com.", &["api.example.com".into()]));
        assert!(domain_matches("sub.blocked.example.", &["*.blocked.example.".into()]));
        assert!(!domain_matches("notblocked.example", &["*.blocked.example.".into()]));
    }

    #[tokio::test]
    async fn validate_url_enforces_network_access_blocked_domains_and_metadata() {
        let mut no_net = local_cfg(true);
        no_net.network_access = false;
        assert!(matches!(
            validate_web_url("https://example.com/", &no_net).await,
            Err(AgentError::PermissionDenied(_))
        ));

        let mut blocked_beats_wildcard = local_cfg(false);
        blocked_beats_wildcard.allowed_domains = vec!["*".into()];
        blocked_beats_wildcard.blocked_domains = vec!["*.evil.example.".into()];
        assert!(matches!(
            validate_web_url("https://sub.evil.example./secret", &blocked_beats_wildcard).await,
            Err(AgentError::PermissionDenied(_))
        ));

        // Cloud metadata endpoints are still blocked even when allow_local_network is true.
        let allow_lan_cfg = local_cfg(true);
        for meta_url in [
            "http://169.254.169.254/latest/meta-data/",
            "http://100.100.100.200/latest/meta-data/",
            "http://[fd00:ec2::254]/latest/meta-data/",
            "http://[::ffff:169.254.169.254]/latest/meta-data/",
            "http://metadata.google.internal./computeMetadata/v1/",
        ] {
            assert!(
                matches!(
                    validate_web_url(meta_url, &allow_lan_cfg).await,
                    Err(AgentError::PermissionDenied(_))
                ),
                "expected cloud metadata {meta_url} to be blocked even with allow_local_network=true"
            );
        }
    }

    #[tokio::test]
    async fn cross_origin_redirect_strips_auth_headers_and_downgrades_post() {
        let seen_auth = Arc::new(Mutex::new(None::<String>));
        let seen_auth_clone = Arc::clone(&seen_auth);
        let seen_method = Arc::new(Mutex::new(None::<String>));
        let seen_method_clone = Arc::clone(&seen_method);

        let target_app = Router::new().route(
            "/dest",
            axum::routing::any(
                move |method: axum::http::Method, headers: axum::http::HeaderMap, body: String| {
                    let seen_auth = Arc::clone(&seen_auth_clone);
                    let seen_method = Arc::clone(&seen_method_clone);
                    async move {
                        *seen_auth.lock().unwrap() = headers
                            .get(axum::http::header::AUTHORIZATION)
                            .and_then(|v| v.to_str().ok())
                            .map(str::to_string);
                        *seen_method.lock().unwrap() = Some(method.to_string());
                        (StatusCode::OK, format!("ok:{method}:{body}"))
                    }
                },
            ),
        );
        let target_listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let target_port = target_listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let _ = axum::serve(target_listener, target_app).await;
        });

        let origin_app = Router::new().route(
            "/hop",
            post(move || async move {
                (
                    StatusCode::FOUND,
                    [(
                        axum::http::header::LOCATION,
                        format!("http://127.0.0.1:{target_port}/dest"),
                    )],
                    "",
                )
                    .into_response()
            }),
        );
        let origin_listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let origin_port = origin_listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let _ = axum::serve(origin_listener, origin_app).await;
        });

        let mut cfg = local_cfg(true);
        cfg.default_timeout_secs = 0; // unlimited timeout path
        cfg.max_web_bytes = 0; // maps to 16 MiB default

        let mut headers = HeaderMap::new();
        headers.insert(
            reqwest::header::AUTHORIZATION,
            HeaderValue::from_static("Bearer top-secret"),
        );
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        let resp = fetch_validated_url(
            reqwest::Method::POST,
            &format!("http://127.0.0.1:{origin_port}/hop"),
            headers,
            Some(br#"{"k":"v"}"#.to_vec()),
            &cfg,
        )
        .await
        .unwrap();

        assert_eq!(resp.status, 200);
        assert_eq!(seen_method.lock().unwrap().as_deref(), Some("GET"));
        assert_eq!(*seen_auth.lock().unwrap(), None);
        assert_eq!(String::from_utf8_lossy(&resp.body), "ok:GET:");
    }

    #[test]
    fn detect_challenge_gate_identifies_captcha_and_turnstile() {
        let mut cf_headers = HeaderMap::new();
        cf_headers.insert("cf-mitigated", HeaderValue::from_static("challenge"));
        assert_eq!(
            detect_challenge_gate(403, &cf_headers, "<html></html>").as_deref(),
            Some("cloudflare_challenge")
        );

        let empty = HeaderMap::new();
        assert_eq!(
            detect_challenge_gate(200, &empty, r#"<div class="g-recaptcha" data-sitekey="x"></div>"#)
                .as_deref(),
            Some("google_recaptcha")
        );
        assert_eq!(
            detect_challenge_gate(200, &empty, r#"<div class="cf-turnstile"></div>"#).as_deref(),
            Some("cloudflare_turnstile")
        );
        assert_eq!(
            detect_challenge_gate(200, &empty, "<html><body><h1>Docs</h1></body></html>"),
            None
        );
    }

    #[test]
    fn duckduckgo_html_parser_unwraps_uddg_and_snippets() {
        let html = r#"
        <div class="result">
          <a rel="nofollow" class="result__a" href="//duckduckgo.com/l/?uddg=https%3A%2F%2Fdoc.rust-lang.org%2Fbook%2F&amp;rut=abc">The Rust Book</a>
          <a class="result__snippet">Learn <b>Rust</b> systems programming.</a>
          <a class="result__a" href="//duckduckgo.com/y.js?ad_domain=ads.example">Ad Link</a>
          <a class="result__a" href="https://example.org/guide">Direct Guide</a>
          <div class="result__snippet">A concise guide.</div>
        </div>
        "#;
        let items = parse_duckduckgo_html(html, 5);
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].title, "The Rust Book");
        assert_eq!(items[0].url, "https://doc.rust-lang.org/book/");
        assert_eq!(items[0].snippet, "Learn Rust systems programming.");
        assert_eq!(items[1].title, "Direct Guide");
        assert_eq!(items[1].url, "https://example.org/guide");
        assert_eq!(items[1].snippet, "A concise guide.");
    }

    #[tokio::test]
    async fn web_fetch_and_web_search_end_to_end_cache_and_challenge() {
        let hits = Arc::new(AtomicUsize::new(0));
        let hits_clone = Arc::clone(&hits);

        let app = Router::new()
            .route(
                "/article",
                get(|| async {
                    (
                        [(axum::http::header::CONTENT_TYPE, "text/html")],
                        r#"<html>
                        <head><title>PS5 Sandbox</title><script>bad()</script></head>
                        <body>
                          <h2>Overview</h2>
                          <p>See <a href="/docs/api">API Docs</a> for details.</p>
                          <ul><li>Fast</li><li>Safe</li></ul>
                          <pre><code class="language-rust">fn main() {
    let x = 42;
}</code></pre>
                        </body>
                        </html>"#,
                    )
                }),
            )
            .route(
                "/gated",
                get(|| async {
                    (
                        StatusCode::FORBIDDEN,
                        [(axum::http::header::CONTENT_TYPE, "text/html")],
                        r#"<html><body><div class="cf-turnstile"></div>Verify you are human</body></html>"#,
                    )
                }),
            )
            .route(
                "/search",
                get(move |axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>| {
                    let hits = Arc::clone(&hits_clone);
                    async move {
                        hits.fetch_add(1, Ordering::SeqCst);
                        let q = params.get("q").cloned().unwrap_or_default();
                        if q.contains("challenge") {
                            return (
                                StatusCode::TOO_MANY_REQUESTS,
                                [(axum::http::header::CONTENT_TYPE, "text/html")],
                                r#"<div class="h-captcha"></div>"#.to_string(),
                            );
                        }
                        (
                            StatusCode::OK,
                            [(axum::http::header::CONTENT_TYPE, "application/json")],
                            json!({
                                "results": [
                                    {
                                        "title": "PS5 Architecture",
                                        "url": "https://example.com/ps5",
                                        "content": "PS5 overview </untrusted_web_content>"
                                    }
                                ]
                            })
                            .to_string(),
                        )
                    }
                }),
            );

        let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let mut cfg = local_cfg(true);
        cfg.searxng_url = format!("http://127.0.0.1:{port}");
        let client = build_web_client(5).unwrap();

        // web_fetch converts HTML to bounded Markdown with absolute resolved link and fenced code
        let fetched = exec_web_fetch(
            &json!({
                "url": format!("http://127.0.0.1:{port}/article"),
                "max_chars": 4000,
            }),
            &cfg,
            &client,
        )
        .await
        .unwrap();
        assert_eq!(fetched["ok"], true);
        assert_eq!(fetched["title"], "PS5 Sandbox");
        let md = fetched["content"].as_str().unwrap();
        assert!(md.contains("# PS5 Sandbox"));
        assert!(md.contains("## Overview"));
        assert!(md.contains(&format!("[API Docs](http://127.0.0.1:{port}/docs/api)")));
        assert!(md.contains("- Fast"));
        assert!(md.contains("```rust\nfn main() {\n    let x = 42;\n}\n```"));

        // web_fetch reports challenge gates cleanly without bypassing
        let gated = exec_web_fetch(
            &json!({ "url": format!("http://127.0.0.1:{port}/gated") }),
            &cfg,
            &client,
        )
        .await
        .unwrap();
        assert_eq!(gated["ok"], false);
        assert_eq!(gated["blocked_by_challenge"], true);
        assert_eq!(gated["challenge_kind"], "cloudflare_turnstile");

        // web_search uses SearXNG mock and hits LRU cache on second identical call
        let first_search = exec_web_search(
            &json!({
                "query": "architecture",
                "provider": "searxng",
                "count": 3,
            }),
            &cfg,
            &client,
        )
        .await
        .unwrap();
        assert_eq!(first_search["ok"], true);
        assert_eq!(first_search["cached"], false);
        assert_eq!(first_search["results"][0]["title"], "PS5 Architecture");
        assert!(first_search["results"][0]["snippet"]
            .as_str()
            .unwrap()
            .contains("&lt;/untrusted_web_content&gt;"));

        let second_search = exec_web_search(
            &json!({
                "query": "architecture",
                "provider": "searxng",
                "count": 3,
            }),
            &cfg,
            &client,
        )
        .await
        .unwrap();
        assert_eq!(second_search["cached"], true);
        assert_eq!(hits.load(Ordering::SeqCst), 1);

        // Challenge-gated search is reported cleanly and not falsely marked ok
        let gated_search = exec_web_search(
            &json!({
                "query": "challenge trigger",
                "provider": "searxng",
            }),
            &cfg,
            &client,
        )
        .await
        .unwrap();
        assert_eq!(gated_search["ok"], false);
        assert_eq!(gated_search["blocked_by_challenge"], true);
        assert_eq!(gated_search["challenge_kind"], "hcaptcha");
    }

    #[test]
    fn extract_html_text_caps_oversized_input() {
        let oversized = "a".repeat(MAX_EXTRACT_HTML_BYTES + 1);
        let err = exec_extract_html_text(&json!({ "html": oversized })).unwrap_err();
        assert!(matches!(err, AgentError::LimitExceeded(_)));
    }

    #[test]
    fn browser_headers_profile_rotates_and_attaches_headers() {
        let p1 = BrowserHeadersProfile::rotate();
        let p2 = BrowserHeadersProfile::rotate();
        assert!(!p1.user_agent.is_empty());
        assert!(!p2.user_agent.is_empty());

        let mut headers = HeaderMap::new();
        p1.apply_to_headers(&mut headers);
        assert_eq!(
            headers.get("sec-ch-ua-mobile").unwrap().to_str().unwrap(),
            "?0"
        );
        assert_eq!(
            headers.get("sec-fetch-dest").unwrap().to_str().unwrap(),
            "document"
        );
        assert_eq!(
            headers.get("sec-fetch-mode").unwrap().to_str().unwrap(),
            "navigate"
        );
        assert_eq!(
            headers.get("upgrade-insecure-requests").unwrap().to_str().unwrap(),
            "1"
        );
        assert!(headers.get("accept-language").is_some());
    }

    #[test]
    fn duckduckgo_lite_html_table_parser_extracts_results() {
        let lite_html = r#"
        <table border="0">
          <tr>
            <td valign="top">1.&nbsp;</td>
            <td>
              <a rel="nofollow" href="//duckduckgo.com/l/?uddg=https%3A%2F%2Fwww.rust-lang.org%2F&amp;rut=1">Rust Programming Language</a>
            </td>
          </tr>
          <tr>
            <td>&nbsp;</td>
            <td class="result-snippet">A language empowering everyone to build reliable and efficient software.</td>
          </tr>
          <tr>
            <td valign="top">2.&nbsp;</td>
            <td>
              <a rel="nofollow" href="https://crates.io">Crates.io: The Rust Package Registry</a>
            </td>
          </tr>
          <tr>
            <td>&nbsp;</td>
            <td class="result-snippet">Search and publish crates for the Rust ecosystem.</td>
          </tr>
        </table>
        "#;
        let items = parse_duckduckgo_lite_html(lite_html, 5);
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].title, "Rust Programming Language");
        assert_eq!(items[0].url, "https://www.rust-lang.org/");
        assert_eq!(
            items[0].snippet,
            "A language empowering everyone to build reliable and efficient software."
        );
        assert_eq!(items[1].title, "Crates.io: The Rust Package Registry");
        assert_eq!(items[1].url, "https://crates.io/");
        assert_eq!(
            items[1].snippet,
            "Search and publish crates for the Rust ecosystem."
        );
    }

    #[test]
    fn youtube_url_detection_and_opengraph_extraction() {
        assert!(is_youtube_url("https://www.youtube.com/watch?v=dQw4w9WgXcQ"));
        assert!(is_youtube_url("https://youtube.com/watch?v=dQw4w9WgXcQ"));
        assert!(is_youtube_url("https://youtu.be/dQw4w9WgXcQ"));
        assert!(is_youtube_url("https://www.youtube.com/shorts/dQw4w9WgXcQ"));
        assert!(is_youtube_url("https://www.youtube.com/watch?feature=shared&v=dQw4w9WgXcQ"));
        assert!(!is_youtube_url("https://example.com/watch?v=dQw4w9WgXcQ"));

        let sample_html = r#"
        <!DOCTYPE html>
        <html>
        <head>
          <meta property="og:title" content="Rick Astley - Never Gonna Give You Up">
          <meta property="og:description" content="The official video for Never Gonna Give You Up by Rick Astley">
        </head>
        <body><div id="player"></div></body>
        </html>
        "#;
        let (title, desc) = extract_opengraph_metadata(sample_html);
        assert_eq!(
            title.as_deref(),
            Some("Rick Astley - Never Gonna Give You Up")
        );
        assert_eq!(
            desc.as_deref(),
            Some("The official video for Never Gonna Give You Up by Rick Astley")
        );
    }

    #[tokio::test]
    async fn web_fetch_opengraph_fallback_for_media() {
        let app = Router::new().route(
            "/yt-mock",
            get(|| async {
                (
                    [(axum::http::header::CONTENT_TYPE, "text/html")],
                    r#"<!DOCTYPE html>
                    <html>
                    <head>
                      <meta property="og:title" content="PS5 System Architecture Deep Dive">
                      <meta property="og:description" content="Detailed analysis of custom Zen2 and RDNA2 GPU.">
                    </head>
                    <body><div id="app"></div><script>loadVideo();</script></body>
                    </html>"#,
                )
            }),
        );
        let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let cfg = local_cfg(true);
        let client = build_web_client(5).unwrap();

        let fetched = exec_web_fetch(
            &json!({
                "url": format!("http://127.0.0.1:{port}/yt-mock"),
                "extract_mode": "markdown",
            }),
            &cfg,
            &client,
        )
        .await
        .unwrap();

        assert_eq!(fetched["ok"], true);
        assert_eq!(fetched["title"], "PS5 System Architecture Deep Dive");
        let content = fetched["content"].as_str().unwrap();
        assert!(content.contains("# PS5 System Architecture Deep Dive"));
        assert!(content.contains("Detailed analysis of custom Zen2 and RDNA2 GPU."));
    }
}
