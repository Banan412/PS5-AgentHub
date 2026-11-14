use ps5_agent_core::{
    AgentError, ApiMode, Result, RuntimeLimits, SandboxApprovalPolicy, SandboxMode,
};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AppConfig {
    pub data_dir: PathBuf,
    pub inference: InferenceConfig,
    pub server: ServerConfig,
    pub database: DatabaseConfig,
    pub security: SecurityConfig,
    pub agents: AgentsConfig,
    pub memory: MemoryConfig,
    pub tools: ToolsConfig,
    pub logging: LoggingConfig,
    pub runtime: RuntimeConfig,
    #[serde(default)]
    pub sandbox: SandboxConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct InferenceConfig {
    #[serde(alias = "url")]
    pub base_url: String,
    pub model: String,
    pub api_mode: ApiMode,
    pub api_key: Option<String>,
    pub timeout_secs: u64,
    pub connect_timeout_secs: u64,
    pub context_limit: u32,
    pub max_output_tokens: u32,
    pub stream_enabled: bool,
    pub max_retries: u32,
    #[serde(default = "default_thinking_level")]
    pub thinking_level: String,
    #[serde(default = "default_true")]
    pub auto_sync_from_ps5lm: bool,
    #[serde(default = "default_sync_interval_secs")]
    pub auto_sync_interval_secs: u64,
    #[serde(default = "default_true")]
    pub prefill_kv_cache: bool,
    #[serde(default = "default_true")]
    pub cache_prompt: bool,
    #[serde(default = "default_temperature")]
    pub temperature: f32,
    #[serde(default)]
    pub dynatemp_range: f32,
    #[serde(default = "default_one_f32")]
    pub dynatemp_exponent: f32,
    #[serde(default = "default_top_k")]
    pub top_k: i32,
    #[serde(default = "default_top_p")]
    pub top_p: f32,
    #[serde(default = "default_min_p")]
    pub min_p: f32,
    #[serde(default = "default_one_f32")]
    pub typical_p: f32,
    #[serde(default)]
    pub xtc_probability: f32,
    #[serde(default = "default_xtc_threshold")]
    pub xtc_threshold: f32,
    #[serde(default = "default_neg_one_f32")]
    pub top_n_sigma: f32,
    #[serde(default = "default_repeat_penalty")]
    pub repeat_penalty: f32,
    #[serde(default = "default_repeat_last_n")]
    pub repeat_last_n: i32,
    #[serde(default)]
    pub presence_penalty: f32,
    #[serde(default)]
    pub frequency_penalty: f32,
    #[serde(default = "default_dry_multiplier")]
    pub dry_multiplier: f32,
    #[serde(default = "default_dry_base")]
    pub dry_base: f32,
    #[serde(default = "default_dry_allowed_length")]
    pub dry_allowed_length: i32,
    #[serde(default = "default_neg_one_i32")]
    pub dry_penalty_last_n: i32,
    #[serde(default)]
    pub mirostat: u8,
    #[serde(default = "default_mirostat_tau")]
    pub mirostat_tau: f32,
    #[serde(default = "default_mirostat_eta")]
    pub mirostat_eta: f32,
    #[serde(default = "default_neg_one_i64")]
    pub seed: i64,
    #[serde(default)]
    pub ignore_eos: bool,
    #[serde(default)]
    pub stop_sequences: Vec<String>,
    #[serde(default)]
    pub n_keep: i32,
    #[serde(default = "default_neg_one_i32")]
    pub id_slot: i32,
    #[serde(default)]
    pub samplers: String,
    #[serde(default)]
    pub grammar: String,
    #[serde(default = "default_reasoning_format")]
    pub reasoning_format: String,
    #[serde(default)]
    pub timings_per_token: bool,
    #[serde(default, alias = "extra_request_body_json")]
    pub custom_extra_body_json: String,
    #[serde(default = "default_true")]
    pub smart_tool_routing: bool,
    #[serde(default)]
    pub cache_type_k: String,
    #[serde(default)]
    pub cache_type_v: String,
    #[serde(default)]
    pub flash_attn: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    #[serde(alias = "host")]
    pub bind_host: String,
    pub port: u16,
    pub max_body_bytes: usize,
    #[serde(default = "default_upload_max_body_bytes")]
    pub upload_max_body_bytes: usize,
    pub allow_lan_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DatabaseConfig {
    /// "sqlite", "jsonl", or "auto"
    pub driver: String,
    pub path: PathBuf,
    pub busy_timeout_ms: u32,
    pub cache_size_kb: i32,
    pub wal_mode: bool,
    /// Retention window in days for pruning old records (`0 = unlimited`).
    #[serde(default = "default_retention_days")]
    pub retention_days: u32,
    /// Maximum rows retained per prunable table (`0 = unlimited`).
    #[serde(default = "default_max_rows_per_table")]
    pub max_rows_per_table: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SecurityConfig {
    #[serde(alias = "auth")]
    pub auth_enabled: bool,
    #[serde(alias = "username")]
    pub admin_username: String,
    #[serde(default, alias = "password", skip_serializing_if = "Option::is_none")]
    pub admin_password: Option<String>,
    pub password_hash: Option<String>,
    pub api_token_sha256: Option<String>,
    pub session_ttl_hours: u64,
    pub rate_limit_per_minute: u32,
    pub login_rate_limit_per_minute: u32,
    pub require_approval_for_mutating: bool,
    pub argon2_memory_kib: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AgentsConfig {
    pub max_agents: usize,
    pub max_active_tasks: usize,
    pub max_queued_tasks: usize,
    pub max_concurrent_inference: usize,
    pub max_messages_per_task: u32,
    pub max_reasoning_iterations: u32,
    /// Default task execution timeout in seconds (`0 = unlimited`).
    #[serde(default)]
    pub default_task_timeout_secs: u64,
    pub allow_temporary_specialists: bool,
    /// "cancel" or "finish_step"
    pub on_disable_policy: String,
}

impl From<&AgentsConfig> for RuntimeLimits {
    fn from(c: &AgentsConfig) -> Self {
        Self {
            max_agents: c.max_agents,
            max_active_tasks: c.max_active_tasks,
            max_queued_tasks: c.max_queued_tasks,
            max_concurrent_inference: c.max_concurrent_inference,
            max_messages_per_task: c.max_messages_per_task,
            max_reasoning_iterations: c.max_reasoning_iterations,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MemoryConfig {
    pub max_memories_per_agent: usize,
    pub archive_after_messages: usize,
    pub keep_recent_messages: usize,
    pub max_context_memories: usize,
    pub max_memory_context_chars: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ToolsConfig {
    pub allowed_paths: Vec<String>,
    pub allowed_domains: Vec<String>,
    #[serde(default)]
    pub blocked_domains: Vec<String>,
    pub block_private_lan_web_requests: bool,
    pub default_timeout_secs: u64,
    pub max_file_bytes: usize,
    pub max_web_bytes: usize,
    #[serde(default = "default_true")]
    pub network_access: bool,
    #[serde(default)]
    pub allow_local_network: bool,
    #[serde(default = "default_search_provider")]
    pub web_search_provider: String,
    #[serde(default)]
    pub searxng_url: String,
    #[serde(default)]
    pub brave_api_key: String,
    #[serde(default)]
    pub tavily_api_key: String,
    #[serde(default)]
    pub remote_browser_url: String,
    #[serde(default)]
    pub allow_host_shell: bool,
    #[serde(default = "default_max_exec_output_bytes")]
    pub max_exec_output_bytes: usize,
    #[serde(default = "default_exec_timeout_secs")]
    pub exec_timeout_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LoggingConfig {
    /// "error", "warn", "info", "debug", "trace"
    pub level: String,
    pub log_dir: PathBuf,
    pub max_file_bytes: usize,
    pub ring_buffer_capacity: usize,
    pub enable_klog: bool,
    pub enable_toast: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RuntimeConfig {
    pub tokio_worker_threads: usize,
    pub health_poll_interval_secs: u64,
    pub rest_mode_recovery: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SandboxConfig {
    #[serde(default = "default_sandbox_mode")]
    pub mode: String,
    #[serde(default = "default_approval_policy")]
    pub approval_policy: String,
    #[serde(default = "default_true")]
    pub network_access: bool,
    #[serde(default)]
    pub allow_local_network: bool,
    #[serde(default = "default_search_provider")]
    pub web_search_provider: String,
    #[serde(default)]
    pub searxng_url: String,
    #[serde(default)]
    pub brave_api_key: String,
    #[serde(default)]
    pub tavily_api_key: String,
    #[serde(default)]
    pub remote_browser_url: String,
    #[serde(default)]
    pub allow_host_shell: bool,
    #[serde(default = "default_max_exec_output_bytes")]
    pub max_exec_output_bytes: usize,
    #[serde(default = "default_exec_timeout_secs")]
    pub exec_timeout_secs: u64,
    #[serde(default = "default_true")]
    pub inject_brain_files: bool,
    #[serde(default = "default_max_brain_context_chars")]
    pub max_brain_context_chars: usize,
    #[serde(default)]
    pub heartbeat_interval_secs: u64,
}

impl Default for AppConfig {
    fn default() -> Self {
        let data_dir = default_data_dir();
        Self::with_data_dir(data_dir)
    }
}

impl Default for InferenceConfig {
    fn default() -> Self {
        AppConfig::default().inference
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        AppConfig::default().server
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        AppConfig::default().database
    }
}

impl Default for SecurityConfig {
    fn default() -> Self {
        AppConfig::default().security
    }
}

impl Default for AgentsConfig {
    fn default() -> Self {
        AppConfig::default().agents
    }
}

impl Default for MemoryConfig {
    fn default() -> Self {
        AppConfig::default().memory
    }
}

impl Default for ToolsConfig {
    fn default() -> Self {
        AppConfig::default().tools
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        AppConfig::default().logging
    }
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        AppConfig::default().runtime
    }
}

impl Default for SandboxConfig {
    fn default() -> Self {
        Self {
            mode: default_sandbox_mode(),
            approval_policy: default_approval_policy(),
            network_access: true,
            allow_local_network: false,
            web_search_provider: default_search_provider(),
            searxng_url: String::new(),
            brave_api_key: String::new(),
            tavily_api_key: String::new(),
            remote_browser_url: String::new(),
            allow_host_shell: false,
            max_exec_output_bytes: default_max_exec_output_bytes(),
            exec_timeout_secs: default_exec_timeout_secs(),
            inject_brain_files: true,
            max_brain_context_chars: default_max_brain_context_chars(),
            heartbeat_interval_secs: 0,
        }
    }
}

pub fn default_data_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("PS5_AGENT_DATA_DIR") {
        if !dir.trim().is_empty() {
            return PathBuf::from(dir);
        }
    }
    #[cfg(target_vendor = "sie")]
    {
        PathBuf::from("/data/ps5-agent")
    }
    #[cfg(not(target_vendor = "sie"))]
    {
        PathBuf::from("./data/ps5-agent")
    }
}

#[cfg(any(target_vendor = "sie", test))]
const DATA_MOUNT_RETRY_ATTEMPTS: u32 = 12;

#[cfg(any(target_vendor = "sie", test))]
fn retry_writable_probe<F>(attempts: u32, delay: std::time::Duration, mut probe: F) -> Option<u32>
where
    F: FnMut() -> bool,
{
    for attempt in 2..=attempts {
        if !delay.is_zero() {
            std::thread::sleep(delay);
        }
        if probe() {
            return Some(attempt);
        }
    }
    None
}

/// Checks whether a candidate data directory can be written; on PS5 retries
/// `/data/ps5-agent` for cold-boot `pldmgr` mount readiness before falling back
/// from `/data/ps5-agent` to `/download0/ps5-agent` if `/data` is unavailable.
pub fn resolve_writable_data_dir(preferred: &Path) -> PathBuf {
    if try_ensure_writable(preferred).is_ok() {
        return preferred.to_path_buf();
    }
    #[cfg(target_vendor = "sie")]
    {
        use std::io::Write as _;
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::time::Duration;

        static DATA_RETRY_EXHAUSTED: AtomicBool = AtomicBool::new(false);
        const DATA_MOUNT_RETRY_DELAY: Duration = Duration::from_millis(250);

        if preferred == Path::new("/data/ps5-agent")
            && !DATA_RETRY_EXHAUSTED.load(Ordering::Relaxed)
        {
            if let Some(attempt) = retry_writable_probe(
                DATA_MOUNT_RETRY_ATTEMPTS,
                DATA_MOUNT_RETRY_DELAY,
                || try_ensure_writable(preferred).is_ok(),
            ) {
                let _ = writeln!(
                    std::io::stderr(),
                    "[ps5-agent-config] /data/ps5-agent became writable on retry attempt {attempt}/{DATA_MOUNT_RETRY_ATTEMPTS}"
                );
                return preferred.to_path_buf();
            }
            DATA_RETRY_EXHAUSTED.store(true, Ordering::Relaxed);
            let _ = writeln!(
                std::io::stderr(),
                "[ps5-agent-config] /data/ps5-agent remained unwritable after {DATA_MOUNT_RETRY_ATTEMPTS} attempts; falling back to /download0/ps5-agent"
            );
        }
        let fallback = PathBuf::from("/download0/ps5-agent");
        if try_ensure_writable(&fallback).is_ok() {
            return fallback;
        }
    }
    preferred.to_path_buf()
}

fn try_ensure_writable(dir: &Path) -> std::io::Result<()> {
    fs::create_dir_all(dir)?;
    let probe = dir.join(".write_probe");
    fs::write(&probe, b"ok")?;
    let _ = fs::remove_file(&probe);
    Ok(())
}

fn default_true() -> bool {
    true
}

fn default_thinking_level() -> String {
    "medium".to_string()
}

fn default_sync_interval_secs() -> u64 {
    30
}

fn default_temperature() -> f32 {
    0.4
}

fn default_one_f32() -> f32 {
    1.0
}

fn default_top_k() -> i32 {
    40
}

fn default_top_p() -> f32 {
    0.95
}

fn default_min_p() -> f32 {
    0.05
}

fn default_xtc_threshold() -> f32 {
    0.1
}

fn default_neg_one_f32() -> f32 {
    -1.0
}

fn default_repeat_penalty() -> f32 {
    1.05
}

fn default_repeat_last_n() -> i32 {
    256
}

fn default_dry_multiplier() -> f32 {
    0.8
}

fn default_dry_base() -> f32 {
    1.75
}

fn default_dry_allowed_length() -> i32 {
    2
}

fn default_neg_one_i32() -> i32 {
    -1
}

fn default_neg_one_i64() -> i64 {
    -1
}

fn default_mirostat_tau() -> f32 {
    5.0
}

fn default_mirostat_eta() -> f32 {
    0.1
}

fn default_reasoning_format() -> String {
    "deepseek".to_string()
}

fn default_upload_max_body_bytes() -> usize {
    64 * 1024 * 1024
}

fn default_retention_days() -> u32 {
    30
}

fn default_max_rows_per_table() -> usize {
    10_000
}

fn default_sandbox_mode() -> String {
    "full_sandbox".to_string()
}

fn default_approval_policy() -> String {
    "full_auto".to_string()
}

fn default_search_provider() -> String {
    "duckduckgo".to_string()
}

fn default_max_exec_output_bytes() -> usize {
    262_144
}

fn default_exec_timeout_secs() -> u64 {
    20
}

fn default_max_brain_context_chars() -> usize {
    4_000
}

fn is_valid_ggml_kv_cache_type(value: &str) -> bool {
    matches!(
        value,
        "" | "f32" | "f16" | "bf16" | "q8_0" | "q4_0" | "q4_1" | "iq4_nl" | "q5_0" | "q5_1"
    )
}

const KNOWN_BOOL_KEYS: &[&str] = &[
    "auth_enabled",
    "auth",
    "allow_lan_only",
    "wal_mode",
    "stream_enabled",
    "auto_sync_from_ps5lm",
    "prefill_kv_cache",
    "cache_prompt",
    "ignore_eos",
    "timings_per_token",
    "smart_tool_routing",
    "flash_attn",
    "require_approval_for_mutating",
    "allow_temporary_specialists",
    "block_private_lan_web_requests",
    "network_access",
    "allow_local_network",
    "allow_host_shell",
    "inject_brain_files",
    "enable_klog",
    "enable_toast",
    "rest_mode_recovery",
];

#[derive(Copy, Clone, PartialEq, Eq)]
enum MultilineQuote {
    None,
    Double,
    Single,
}

/// Sanitizes user-edited TOML (e.g., from Notepad, mobile editors, or FTP transfers):
/// - Strips leading UTF-8 BOM (`\u{feff}`) and all NUL bytes (`\0`).
/// - Normalizes unquoted boolean variants (`True`/`TRUE`/`yes`/`on`, `False`/`FALSE`/`no`/`off`)
///   and quoted `"true"`/`"false"` on known boolean keys, while preserving comments and string
///   values on non-boolean keys.
pub fn sanitize_user_toml(raw: &str) -> String {
    let cleaned = raw.trim_start_matches('\u{feff}').replace('\0', "");
    let mut out = String::with_capacity(cleaned.len());
    let mut multiline = MultilineQuote::None;

    for segment in cleaned.split_inclusive('\n') {
        let (body, eol) = if let Some(s) = segment.strip_suffix("\r\n") {
            (s, "\r\n")
        } else if let Some(s) = segment.strip_suffix('\n') {
            (s, "\n")
        } else {
            (segment, "")
        };

        if multiline != MultilineQuote::None {
            multiline = advance_multiline_state(body, multiline);
            out.push_str(body);
            out.push_str(eol);
            continue;
        }

        let trimmed_line = body.trim();
        if trimmed_line.is_empty() || trimmed_line.starts_with('#') || trimmed_line.starts_with('[') {
            out.push_str(body);
            out.push_str(eol);
            continue;
        }

        if let Some((lhs, rhs)) = split_key_value_line(body) {
            let next_multiline = detect_unclosed_multiline_rhs(rhs);
            if next_multiline != MultilineQuote::None {
                multiline = next_multiline;
                out.push_str(body);
                out.push_str(eol);
                continue;
            }

            let key = lhs.trim().trim_matches('"').trim_matches('\'');
            let (val_raw, comment) = split_toml_value_and_comment(rhs);
            let trimmed_val = val_raw.trim();
            if !trimmed_val.is_empty() {
                if let Some(normalized) = normalize_bool_token(key, trimmed_val) {
                    let leading_ws = &val_raw[..val_raw.len() - val_raw.trim_start().len()];
                    let trailing_ws = &val_raw[val_raw.trim_end().len()..];
                    out.push_str(lhs);
                    out.push('=');
                    out.push_str(leading_ws);
                    out.push_str(normalized);
                    out.push_str(trailing_ws);
                    out.push_str(comment);
                    out.push_str(eol);
                    continue;
                }
            }
        }

        out.push_str(body);
        out.push_str(eol);
    }

    out
}

fn split_key_value_line(line: &str) -> Option<(&str, &str)> {
    let mut in_double = false;
    let mut in_single = false;
    let mut escaped = false;

    for (idx, ch) in line.char_indices() {
        if in_double {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_double = false;
            }
        } else if in_single {
            if ch == '\'' {
                in_single = false;
            }
        } else {
            match ch {
                '"' => in_double = true,
                '\'' => in_single = true,
                '#' => return None,
                '=' => return Some((&line[..idx], &line[idx + 1..])),
                _ => {}
            }
        }
    }
    None
}

fn split_toml_value_and_comment(rhs: &str) -> (&str, &str) {
    let mut in_double = false;
    let mut in_single = false;
    let mut escaped = false;

    for (idx, ch) in rhs.char_indices() {
        if in_double {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_double = false;
            }
        } else if in_single {
            if ch == '\'' {
                in_single = false;
            }
        } else {
            match ch {
                '"' => in_double = true,
                '\'' => in_single = true,
                '#' => return (&rhs[..idx], &rhs[idx..]),
                _ => {}
            }
        }
    }
    (rhs, "")
}

fn detect_unclosed_multiline_rhs(rhs: &str) -> MultilineQuote {
    advance_multiline_state(rhs, MultilineQuote::None)
}

fn advance_multiline_state(s: &str, mut state: MultilineQuote) -> MultilineQuote {
    let bytes = s.as_bytes();
    let mut i = 0;
    let mut in_double = false;
    let mut in_single = false;
    let mut escaped = false;

    while i < bytes.len() {
        match state {
            MultilineQuote::Double => {
                if escaped {
                    escaped = false;
                    i += 1;
                } else if bytes[i] == b'\\' {
                    escaped = true;
                    i += 1;
                } else if bytes[i..].starts_with(b"\"\"\"") {
                    state = MultilineQuote::None;
                    i += 3;
                } else {
                    i += 1;
                }
            }
            MultilineQuote::Single => {
                if bytes[i..].starts_with(b"'''") {
                    state = MultilineQuote::None;
                    i += 3;
                } else {
                    i += 1;
                }
            }
            MultilineQuote::None => {
                if in_double {
                    if escaped {
                        escaped = false;
                        i += 1;
                    } else if bytes[i] == b'\\' {
                        escaped = true;
                        i += 1;
                    } else if bytes[i] == b'"' {
                        in_double = false;
                        i += 1;
                    } else {
                        i += 1;
                    }
                } else if in_single {
                    if bytes[i] == b'\'' {
                        in_single = false;
                    }
                    i += 1;
                } else if bytes[i..].starts_with(b"\"\"\"") {
                    state = MultilineQuote::Double;
                    i += 3;
                } else if bytes[i..].starts_with(b"'''") {
                    state = MultilineQuote::Single;
                    i += 3;
                } else if bytes[i] == b'"' {
                    in_double = true;
                    i += 1;
                } else if bytes[i] == b'\'' {
                    in_single = true;
                    i += 1;
                } else if bytes[i] == b'#' {
                    break;
                } else {
                    i += 1;
                }
            }
        }
    }
    state
}

fn normalize_bool_token(key: &str, trimmed_val: &str) -> Option<&'static str> {
    if !trimmed_val.starts_with('"') && !trimmed_val.starts_with('\'') {
        if trimmed_val.eq_ignore_ascii_case("true")
            || trimmed_val.eq_ignore_ascii_case("yes")
            || trimmed_val.eq_ignore_ascii_case("on")
        {
            return Some("true");
        }
        if trimmed_val.eq_ignore_ascii_case("false")
            || trimmed_val.eq_ignore_ascii_case("no")
            || trimmed_val.eq_ignore_ascii_case("off")
        {
            return Some("false");
        }
        return None;
    }

    if KNOWN_BOOL_KEYS.contains(&key) {
        let inner = if (trimmed_val.starts_with('"')
            && trimmed_val.ends_with('"')
            && trimmed_val.len() >= 2)
            || (trimmed_val.starts_with('\'')
                && trimmed_val.ends_with('\'')
                && trimmed_val.len() >= 2)
        {
            &trimmed_val[1..trimmed_val.len() - 1]
        } else {
            return None;
        };

        if inner.eq_ignore_ascii_case("true") {
            return Some("true");
        }
        if inner.eq_ignore_ascii_case("false") {
            return Some("false");
        }
    }

    None
}

fn preferred_data_dir_for_path(path: &Path) -> PathBuf {
    let preferred = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .map(|p| p.to_path_buf())
        .unwrap_or_else(default_data_dir);
    resolve_writable_data_dir(&preferred)
}

fn env_override<F>(get_env: &mut F, key: &str) -> Option<String>
where
    F: FnMut(&str) -> Option<String>,
{
    get_env(key)
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn reconcile_val<T: Copy + PartialEq>(sandbox_val: T, tools_val: T, default: T) -> T {
    if sandbox_val != default {
        sandbox_val
    } else {
        tools_val
    }
}

fn reconcile_string(sandbox_val: &str, tools_val: &str, default: &str) -> String {
    let s = sandbox_val.trim();
    let t = tools_val.trim();
    if !s.eq_ignore_ascii_case(default) && !s.is_empty() {
        s.to_string()
    } else if !t.eq_ignore_ascii_case(default) && !t.is_empty() {
        t.to_string()
    } else {
        default.to_string()
    }
}

fn toml_table_has_key(root: &toml::map::Map<String, toml::Value>, section: &str, key: &str) -> bool {
    root.get(section)
        .and_then(|v| v.as_table())
        .map_or(false, |t| t.contains_key(key))
}

/// Parses TOML config and, when `data_dir` was explicitly overridden while sub-paths
/// were omitted, derives `database.path`, `logging.log_dir`, and `tools.allowed_paths`
/// from the overridden `data_dir` instead of the compile-time/env default dir.
fn parse_app_config_with_subpaths(
    sanitized: &str,
) -> std::result::Result<AppConfig, toml::de::Error> {
    let mut cfg: AppConfig = toml::from_str(sanitized)?;
    if let Ok(toml::Value::Table(root)) = toml::from_str::<toml::Value>(sanitized) {
        if root.contains_key("data_dir") {
            if !toml_table_has_key(&root, "database", "path") {
                cfg.database.path = cfg.data_dir.join("database").join("agent.db");
            }
            if !toml_table_has_key(&root, "logging", "log_dir") {
                cfg.logging.log_dir = cfg.data_dir.join("logs");
            }
            if !toml_table_has_key(&root, "tools", "allowed_paths") {
                cfg.tools.allowed_paths =
                    vec![cfg.data_dir.join("workspace").to_string_lossy().into_owned()];
            }
        }
    }
    Ok(cfg)
}

struct JsonValidator<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> JsonValidator<'a> {
    fn next_is(&self, pred: fn(&u8) -> bool) -> bool {
        self.bytes.get(self.pos).map_or(false, pred)
    }

    fn skip_ws(&mut self) {
        while matches!(self.bytes.get(self.pos), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.pos += 1;
        }
    }

    fn expect_byte(
        &mut self,
        expected: u8,
        err: &'static str,
    ) -> std::result::Result<(), &'static str> {
        if self.bytes.get(self.pos) == Some(&expected) {
            self.pos += 1;
            Ok(())
        } else {
            Err(err)
        }
    }

    fn expect_literal(&mut self, literal: &[u8]) -> std::result::Result<(), &'static str> {
        if self.bytes[self.pos..].starts_with(literal) {
            self.pos += literal.len();
            Ok(())
        } else {
            Err("invalid JSON literal")
        }
    }

    fn parse_object(&mut self, depth: usize) -> std::result::Result<(), &'static str> {
        if depth > 64 {
            return Err("JSON nesting depth exceeds 64");
        }
        self.expect_byte(b'{', "JSON value must be an object")?;
        self.skip_ws();
        if self.bytes.get(self.pos) == Some(&b'}') {
            self.pos += 1;
            return Ok(());
        }
        loop {
            self.skip_ws();
            self.parse_string()?;
            self.skip_ws();
            self.expect_byte(b':', "expected ':' after JSON object key")?;
            self.parse_value(depth + 1)?;
            self.skip_ws();
            match self.bytes.get(self.pos) {
                Some(b',') => self.pos += 1,
                Some(b'}') => {
                    self.pos += 1;
                    return Ok(());
                }
                _ => return Err("expected ',' or '}' in JSON object"),
            }
        }
    }

    fn parse_array(&mut self, depth: usize) -> std::result::Result<(), &'static str> {
        if depth > 64 {
            return Err("JSON nesting depth exceeds 64");
        }
        self.expect_byte(b'[', "expected '['")?;
        self.skip_ws();
        if self.bytes.get(self.pos) == Some(&b']') {
            self.pos += 1;
            return Ok(());
        }
        loop {
            self.parse_value(depth + 1)?;
            self.skip_ws();
            match self.bytes.get(self.pos) {
                Some(b',') => self.pos += 1,
                Some(b']') => {
                    self.pos += 1;
                    return Ok(());
                }
                _ => return Err("expected ',' or ']' in JSON array"),
            }
        }
    }

    fn parse_value(&mut self, depth: usize) -> std::result::Result<(), &'static str> {
        if depth > 64 {
            return Err("JSON nesting depth exceeds 64");
        }
        self.skip_ws();
        match self.bytes.get(self.pos) {
            Some(b'{') => self.parse_object(depth),
            Some(b'[') => self.parse_array(depth),
            Some(b'"') => self.parse_string(),
            Some(b't') => self.expect_literal(b"true"),
            Some(b'f') => self.expect_literal(b"false"),
            Some(b'n') => self.expect_literal(b"null"),
            Some(b'-' | b'0'..=b'9') => self.parse_number(),
            _ => Err("unexpected token in JSON value"),
        }
    }

    fn parse_string(&mut self) -> std::result::Result<(), &'static str> {
        self.expect_byte(b'"', "expected JSON string")?;
        while let Some(&b) = self.bytes.get(self.pos) {
            self.pos += 1;
            if b == b'"' {
                return Ok(());
            }
            if b == b'\\' {
                let &esc = self.bytes.get(self.pos).ok_or("unterminated JSON escape")?;
                self.pos += 1;
                match esc {
                    b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => {}
                    b'u' => {
                        for _ in 0..4 {
                            if !self.next_is(u8::is_ascii_hexdigit) {
                                return Err("invalid unicode escape in JSON string");
                            }
                            self.pos += 1;
                        }
                    }
                    _ => return Err("invalid escape sequence in JSON string"),
                }
            } else if b < 0x20 {
                return Err("unescaped control character in JSON string");
            }
        }
        Err("unterminated JSON string")
    }

    fn parse_number(&mut self) -> std::result::Result<(), &'static str> {
        if self.bytes.get(self.pos) == Some(&b'-') {
            self.pos += 1;
        }
        match self.bytes.get(self.pos) {
            Some(b'0') => self.pos += 1,
            Some(b'1'..=b'9') => {
                while self.next_is(u8::is_ascii_digit) {
                    self.pos += 1;
                }
            }
            _ => return Err("invalid JSON number"),
        }
        if self.bytes.get(self.pos) == Some(&b'.') {
            self.pos += 1;
            if !self.next_is(u8::is_ascii_digit) {
                return Err("invalid fraction in JSON number");
            }
            while self.next_is(u8::is_ascii_digit) {
                self.pos += 1;
            }
        }
        if matches!(self.bytes.get(self.pos), Some(b'e' | b'E')) {
            self.pos += 1;
            if matches!(self.bytes.get(self.pos), Some(b'+' | b'-')) {
                self.pos += 1;
            }
            if !self.next_is(u8::is_ascii_digit) {
                return Err("invalid exponent in JSON number");
            }
            while self.next_is(u8::is_ascii_digit) {
                self.pos += 1;
            }
        }
        Ok(())
    }
}

fn validate_json_object(raw: &str) -> std::result::Result<(), &'static str> {
    let mut parser = JsonValidator {
        bytes: raw.as_bytes(),
        pos: 0,
    };
    parser.skip_ws();
    parser.parse_object(0)?;
    parser.skip_ws();
    if parser.pos != parser.bytes.len() {
        return Err("unexpected trailing characters after JSON object");
    }
    Ok(())
}

impl AppConfig {
    pub fn with_data_dir(data_dir: PathBuf) -> Self {
        let db_path = data_dir.join("database").join("agent.db");
        let log_dir = data_dir.join("logs");
        let workspace_dir = data_dir.join("workspace");

        #[cfg(target_vendor = "sie")]
        let (bind_host, argon2_mem, workers, enable_klog, enable_toast, wal_mode) =
            ("0.0.0.0".to_string(), 8192, 2, true, true, false);
        #[cfg(not(target_vendor = "sie"))]
        let (bind_host, argon2_mem, workers, enable_klog, enable_toast, wal_mode) =
            ("127.0.0.1".to_string(), 19456, 2, false, false, true);

        let mut cfg = Self {
            data_dir: data_dir.clone(),
            inference: InferenceConfig {
                base_url: "http://127.0.0.1:8081/v1".to_string(),
                model: "qwen-loaded".to_string(),
                api_mode: ApiMode::ChatCompletions,
                api_key: None,
                timeout_secs: 120,
                connect_timeout_secs: 5,
                context_limit: 8192,
                max_output_tokens: 1024,
                stream_enabled: true,
                max_retries: 2,
                thinking_level: default_thinking_level(),
                auto_sync_from_ps5lm: true,
                auto_sync_interval_secs: default_sync_interval_secs(),
                prefill_kv_cache: true,
                cache_prompt: true,
                temperature: default_temperature(),
                dynatemp_range: 0.0,
                dynatemp_exponent: default_one_f32(),
                top_k: default_top_k(),
                top_p: default_top_p(),
                min_p: default_min_p(),
                typical_p: default_one_f32(),
                xtc_probability: 0.0,
                xtc_threshold: default_xtc_threshold(),
                top_n_sigma: default_neg_one_f32(),
                repeat_penalty: default_repeat_penalty(),
                repeat_last_n: default_repeat_last_n(),
                presence_penalty: 0.0,
                frequency_penalty: 0.0,
                dry_multiplier: default_dry_multiplier(),
                dry_base: default_dry_base(),
                dry_allowed_length: default_dry_allowed_length(),
                dry_penalty_last_n: default_neg_one_i32(),
                mirostat: 0,
                mirostat_tau: default_mirostat_tau(),
                mirostat_eta: default_mirostat_eta(),
                seed: default_neg_one_i64(),
                ignore_eos: false,
                stop_sequences: Vec::new(),
                n_keep: 0,
                id_slot: default_neg_one_i32(),
                samplers: String::new(),
                grammar: String::new(),
                reasoning_format: default_reasoning_format(),
                timings_per_token: false,
                custom_extra_body_json: String::new(),
                smart_tool_routing: true,
                cache_type_k: String::new(),
                cache_type_v: String::new(),
                flash_attn: false,
            },
            server: ServerConfig {
                bind_host,
                port: 8080,
                max_body_bytes: 1024 * 1024,
                upload_max_body_bytes: default_upload_max_body_bytes(),
                allow_lan_only: true,
            },
            database: DatabaseConfig {
                driver: "auto".to_string(),
                path: db_path,
                busy_timeout_ms: 5000,
                cache_size_kb: 4000,
                wal_mode,
                retention_days: default_retention_days(),
                max_rows_per_table: default_max_rows_per_table(),
            },
            security: SecurityConfig {
                auth_enabled: true,
                admin_username: "admin".to_string(),
                admin_password: None,
                password_hash: None,
                api_token_sha256: None,
                session_ttl_hours: 24,
                rate_limit_per_minute: 240,
                login_rate_limit_per_minute: 10,
                require_approval_for_mutating: false,
                argon2_memory_kib: argon2_mem,
            },
            agents: AgentsConfig {
                max_agents: 0,
                max_active_tasks: 0,
                max_queued_tasks: 0,
                max_concurrent_inference: 4,
                max_messages_per_task: 0,
                max_reasoning_iterations: 0,
                default_task_timeout_secs: 0,
                allow_temporary_specialists: true,
                on_disable_policy: "cancel".to_string(),
            },
            memory: MemoryConfig {
                max_memories_per_agent: 0,
                archive_after_messages: 40,
                keep_recent_messages: 16,
                max_context_memories: 8,
                max_memory_context_chars: 3200,
            },
            tools: ToolsConfig {
                allowed_paths: vec![workspace_dir.to_string_lossy().into_owned()],
                allowed_domains: vec!["*".to_string()],
                blocked_domains: Vec::new(),
                block_private_lan_web_requests: true,
                default_timeout_secs: 20,
                max_file_bytes: 2 * 1024 * 1024,
                max_web_bytes: 512 * 1024,
                network_access: true,
                allow_local_network: false,
                web_search_provider: default_search_provider(),
                searxng_url: String::new(),
                brave_api_key: String::new(),
                tavily_api_key: String::new(),
                remote_browser_url: String::new(),
                allow_host_shell: false,
                max_exec_output_bytes: default_max_exec_output_bytes(),
                exec_timeout_secs: default_exec_timeout_secs(),
            },
            logging: LoggingConfig {
                level: "info".to_string(),
                log_dir,
                max_file_bytes: 2 * 1024 * 1024,
                ring_buffer_capacity: 500,
                enable_klog,
                enable_toast,
            },
            runtime: RuntimeConfig {
                tokio_worker_threads: workers,
                health_poll_interval_secs: 15,
                rest_mode_recovery: true,
            },
            sandbox: SandboxConfig::default(),
        };
        cfg.apply_env_overrides();
        cfg.sync_sandbox_and_tools();
        cfg
    }

    /// Applies supported `PS5_AGENT_*` environment variable overrides for sandbox and tool settings.
    pub fn apply_env_overrides(&mut self) {
        self.apply_env_overrides_with(|key| std::env::var(key).ok());
    }

    fn apply_env_overrides_with<F>(&mut self, mut get_env: F)
    where
        F: FnMut(&str) -> Option<String>,
    {
        if let Some(val) = env_override(&mut get_env, "PS5_AGENT_SANDBOX_MODE") {
            self.sandbox.mode = val;
        }
        if let Some(val) = env_override(&mut get_env, "PS5_AGENT_APPROVAL_POLICY") {
            self.sandbox.approval_policy = val;
        }
        if let Some(val) = env_override(&mut get_env, "PS5_AGENT_SEARCH_PROVIDER") {
            let lower = val.to_ascii_lowercase();
            self.sandbox.web_search_provider = lower.clone();
            self.tools.web_search_provider = lower;
        }
        for (var, s_slot, t_slot) in [
            (
                "PS5_AGENT_SEARXNG_URL",
                &mut self.sandbox.searxng_url,
                &mut self.tools.searxng_url,
            ),
            (
                "PS5_AGENT_BRAVE_API_KEY",
                &mut self.sandbox.brave_api_key,
                &mut self.tools.brave_api_key,
            ),
            (
                "PS5_AGENT_TAVILY_API_KEY",
                &mut self.sandbox.tavily_api_key,
                &mut self.tools.tavily_api_key,
            ),
            (
                "PS5_AGENT_REMOTE_BROWSER_URL",
                &mut self.sandbox.remote_browser_url,
                &mut self.tools.remote_browser_url,
            ),
        ] {
            if let Some(val) = env_override(&mut get_env, var) {
                *s_slot = val.clone();
                *t_slot = val;
            }
        }
    }

    /// Synchronizes overlapping sandbox and tools configuration fields so settings
    /// provided under either `[sandbox]` or `[tools]` are honored consistently.
    pub fn sync_sandbox_and_tools(&mut self) {
        let network_access = reconcile_val(
            self.sandbox.network_access,
            self.tools.network_access,
            true,
        );
        self.sandbox.network_access = network_access;
        self.tools.network_access = network_access;

        let allow_local = self.sandbox.allow_local_network
            || self.tools.allow_local_network
            || !self.tools.block_private_lan_web_requests;
        self.sandbox.allow_local_network = allow_local;
        self.tools.allow_local_network = allow_local;
        self.tools.block_private_lan_web_requests = !allow_local;

        let provider = reconcile_string(
            &self.sandbox.web_search_provider,
            &self.tools.web_search_provider,
            "duckduckgo",
        )
        .to_ascii_lowercase();
        self.sandbox.web_search_provider = provider.clone();
        self.tools.web_search_provider = provider;

        for (s_slot, t_slot) in [
            (&mut self.sandbox.searxng_url, &mut self.tools.searxng_url),
            (&mut self.sandbox.brave_api_key, &mut self.tools.brave_api_key),
            (&mut self.sandbox.tavily_api_key, &mut self.tools.tavily_api_key),
            (
                &mut self.sandbox.remote_browser_url,
                &mut self.tools.remote_browser_url,
            ),
        ] {
            let synced = reconcile_string(s_slot, t_slot, "");
            *s_slot = synced.clone();
            *t_slot = synced;
        }

        let allow_shell = reconcile_val(
            self.sandbox.allow_host_shell,
            self.tools.allow_host_shell,
            false,
        );
        self.sandbox.allow_host_shell = allow_shell;
        self.tools.allow_host_shell = allow_shell;

        let max_exec = reconcile_val(
            self.sandbox.max_exec_output_bytes,
            self.tools.max_exec_output_bytes,
            default_max_exec_output_bytes(),
        );
        self.sandbox.max_exec_output_bytes = max_exec;
        self.tools.max_exec_output_bytes = max_exec;

        let exec_timeout = reconcile_val(
            self.sandbox.exec_timeout_secs,
            self.tools.exec_timeout_secs,
            default_exec_timeout_secs(),
        );
        self.sandbox.exec_timeout_secs = exec_timeout;
        self.tools.exec_timeout_secs = exec_timeout;
    }

    /// Clamps numeric tuning fields and normalizes enum-like strings rather than
    /// fatally rejecting slightly out-of-range user edits, returning a warning for each change.
    pub fn normalize_and_clamp(&mut self) -> Vec<String> {
        let mut warnings = Vec::new();
        self.sync_sandbox_and_tools();

        if self.inference.context_limit > 0 {
            let clamped_ctx = self.inference.context_limit.clamp(512, 1_048_576);
            if clamped_ctx != self.inference.context_limit {
                warnings.push(format!(
                    "clamped inference.context_limit from {} to {}",
                    self.inference.context_limit, clamped_ctx
                ));
                self.inference.context_limit = clamped_ctx;
            }
        }

        if self.inference.max_output_tokens > 0 {
            let clamped_out = if self.inference.context_limit > 0 {
                let max_out_upper = self.inference.context_limit.saturating_sub(64).max(64);
                self.inference.max_output_tokens.clamp(64, max_out_upper)
            } else {
                self.inference.max_output_tokens.max(64)
            };
            if clamped_out != self.inference.max_output_tokens {
                warnings.push(format!(
                    "clamped inference.max_output_tokens from {} to {}",
                    self.inference.max_output_tokens, clamped_out
                ));
                self.inference.max_output_tokens = clamped_out;
            }
        }

        for (name, slot, fallback) in [
            ("inference.temperature", &mut self.inference.temperature, default_temperature()),
            ("inference.dynatemp_range", &mut self.inference.dynatemp_range, 0.0),
            ("inference.dynatemp_exponent", &mut self.inference.dynatemp_exponent, default_one_f32()),
            ("inference.top_p", &mut self.inference.top_p, default_top_p()),
            ("inference.min_p", &mut self.inference.min_p, default_min_p()),
            ("inference.typical_p", &mut self.inference.typical_p, default_one_f32()),
            ("inference.xtc_probability", &mut self.inference.xtc_probability, 0.0),
            ("inference.xtc_threshold", &mut self.inference.xtc_threshold, default_xtc_threshold()),
            ("inference.top_n_sigma", &mut self.inference.top_n_sigma, default_neg_one_f32()),
            ("inference.repeat_penalty", &mut self.inference.repeat_penalty, default_repeat_penalty()),
            ("inference.presence_penalty", &mut self.inference.presence_penalty, 0.0),
            ("inference.frequency_penalty", &mut self.inference.frequency_penalty, 0.0),
            ("inference.dry_multiplier", &mut self.inference.dry_multiplier, default_dry_multiplier()),
            ("inference.dry_base", &mut self.inference.dry_base, default_dry_base()),
            ("inference.mirostat_tau", &mut self.inference.mirostat_tau, default_mirostat_tau()),
            ("inference.mirostat_eta", &mut self.inference.mirostat_eta, default_mirostat_eta()),
        ] {
            if !slot.is_finite() {
                warnings.push(format!("reset non-finite {name} to {fallback}"));
                *slot = fallback;
            }
        }

        if self.inference.temperature < 0.0 {
            warnings.push(format!(
                "clamped inference.temperature from {} to 0.0",
                self.inference.temperature
            ));
            self.inference.temperature = 0.0;
        }

        if self.inference.top_p < 0.0 || self.inference.top_p > 1.0 {
            let clamped = self.inference.top_p.clamp(0.0, 1.0);
            warnings.push(format!(
                "clamped inference.top_p from {} to {clamped}",
                self.inference.top_p
            ));
            self.inference.top_p = clamped;
        }

        if self.inference.min_p < 0.0 || self.inference.min_p > 1.0 {
            let clamped = self.inference.min_p.clamp(0.0, 1.0);
            warnings.push(format!(
                "clamped inference.min_p from {} to {clamped}",
                self.inference.min_p
            ));
            self.inference.min_p = clamped;
        }

        if self.inference.dry_multiplier < 0.0 {
            warnings.push(format!(
                "clamped inference.dry_multiplier from {} to 0.0",
                self.inference.dry_multiplier
            ));
            self.inference.dry_multiplier = 0.0;
        }

        if self.inference.mirostat > 2 {
            warnings.push(format!(
                "clamped inference.mirostat from {} to 2",
                self.inference.mirostat
            ));
            self.inference.mirostat = 2;
        }

        for (name, slot) in [
            ("inference.cache_type_k", &mut self.inference.cache_type_k),
            ("inference.cache_type_v", &mut self.inference.cache_type_v),
        ] {
            let normalized = slot.trim().to_ascii_lowercase();
            if is_valid_ggml_kv_cache_type(&normalized) {
                *slot = normalized;
            } else {
                warnings.push(format!("reset invalid {name} {slot:?} to \"\""));
                *slot = String::new();
            }
        }

        let thinking_lower = self.inference.thinking_level.trim().to_ascii_lowercase();
        if matches!(
            thinking_lower.as_str(),
            "off" | "low" | "medium" | "high" | "max"
        ) {
            self.inference.thinking_level = thinking_lower;
        } else {
            warnings.push(format!(
                "reset invalid inference.thinking_level {:?} to \"medium\"",
                self.inference.thinking_level
            ));
            self.inference.thinking_level = "medium".to_string();
        }

        if self.server.max_body_bytes > 0 {
            let clamped_body = self.server.max_body_bytes.clamp(1024, 256 * 1024 * 1024);
            if clamped_body != self.server.max_body_bytes {
                warnings.push(format!(
                    "clamped server.max_body_bytes from {} to {}",
                    self.server.max_body_bytes, clamped_body
                ));
                self.server.max_body_bytes = clamped_body;
            }
        }

        if self.server.upload_max_body_bytes > 0 {
            let clamped_upload = self
                .server
                .upload_max_body_bytes
                .clamp(1024, 256 * 1024 * 1024);
            if clamped_upload != self.server.upload_max_body_bytes {
                warnings.push(format!(
                    "clamped server.upload_max_body_bytes from {} to {}",
                    self.server.upload_max_body_bytes, clamped_upload
                ));
                self.server.upload_max_body_bytes = clamped_upload;
            }
        }

        if self.security.session_ttl_hours > 87_600 {
            warnings.push(format!(
                "clamped security.session_ttl_hours from {} to 87600",
                self.security.session_ttl_hours
            ));
            self.security.session_ttl_hours = 87_600;
        }

        let clamped_threads = self.runtime.tokio_worker_threads.clamp(1, 8);
        if clamped_threads != self.runtime.tokio_worker_threads {
            warnings.push(format!(
                "clamped runtime.tokio_worker_threads from {} to {}",
                self.runtime.tokio_worker_threads, clamped_threads
            ));
            self.runtime.tokio_worker_threads = clamped_threads;
        }

        if self.tools.allowed_paths.is_empty() {
            let default_ws = self.data_dir.join("workspace").to_string_lossy().into_owned();
            warnings.push(format!(
                "tools.allowed_paths was empty; defaulted to [\"{}\"]",
                default_ws
            ));
            self.tools.allowed_paths = vec![default_ws];
        }

        if self.tools.allowed_domains.is_empty() && self.sandbox.network_access {
            warnings.push(
                "tools.allowed_domains was empty while network_access is enabled; defaulted to [\"*\"]"
                    .to_string(),
            );
            self.tools.allowed_domains = vec!["*".to_string()];
        }

        let driver_lower = self.database.driver.trim().to_ascii_lowercase();
        if matches!(driver_lower.as_str(), "auto" | "sqlite" | "jsonl") {
            self.database.driver = driver_lower;
        } else {
            warnings.push(format!(
                "reset invalid database.driver {:?} to \"auto\"",
                self.database.driver
            ));
            self.database.driver = "auto".to_string();
        }

        let policy_lower = self.agents.on_disable_policy.trim().to_ascii_lowercase();
        if matches!(policy_lower.as_str(), "cancel" | "finish_step") {
            self.agents.on_disable_policy = policy_lower;
        } else {
            warnings.push(format!(
                "reset invalid agents.on_disable_policy {:?} to \"cancel\"",
                self.agents.on_disable_policy
            ));
            self.agents.on_disable_policy = "cancel".to_string();
        }

        if let Some(mode) = SandboxMode::parse(&self.sandbox.mode) {
            self.sandbox.mode = mode.as_str().to_string();
        } else {
            warnings.push(format!(
                "reset invalid sandbox.mode {:?} to \"full_sandbox\"",
                self.sandbox.mode
            ));
            self.sandbox.mode = SandboxMode::FullSandbox.as_str().to_string();
        }

        if let Some(policy) = SandboxApprovalPolicy::parse(&self.sandbox.approval_policy) {
            self.sandbox.approval_policy = policy.as_str().to_string();
        } else {
            warnings.push(format!(
                "reset invalid sandbox.approval_policy {:?} to \"full_auto\"",
                self.sandbox.approval_policy
            ));
            self.sandbox.approval_policy = SandboxApprovalPolicy::FullAuto.as_str().to_string();
        }

        warnings
    }

    pub fn load_or_create_resilient(path: &Path) -> Result<(Self, Vec<String>)> {
        if path.exists() {
            let bytes = fs::read(path)
                .map_err(|e| AgentError::Storage(format!("reading {}: {e}", path.display())))?;
            let raw = String::from_utf8_lossy(&bytes);
            let sanitized = sanitize_user_toml(&raw);
            let parse_res: std::result::Result<(AppConfig, Vec<String>), String> =
                match parse_app_config_with_subpaths(&sanitized) {
                    Ok(mut cfg) => {
                        cfg.apply_env_overrides();
                        cfg.sync_sandbox_and_tools();
                        let warnings = cfg.normalize_and_clamp();
                        match cfg.validate() {
                            Ok(()) => Ok((cfg, warnings)),
                            Err(e) => Err(format!("validation failed: {e}")),
                        }
                    }
                    Err(e) => Err(format!("TOML parse failed: {e}")),
                };

            match parse_res {
                Ok((cfg, warnings)) => Ok((cfg, warnings)),
                Err(reason) => {
                    let bak = path.with_extension("toml.broken.bak");
                    let _ = fs::copy(path, &bak);
                    let fallback_cfg = Self::with_data_dir(preferred_data_dir_for_path(path));
                    fallback_cfg.save(path)?;
                    Ok((
                        fallback_cfg,
                        vec![format!(
                            "Recovered invalid config at {} (backed up to {}): {reason}",
                            path.display(),
                            bak.display()
                        )],
                    ))
                }
            }
        } else {
            let cfg = Self::with_data_dir(preferred_data_dir_for_path(path));
            cfg.save(path)?;
            Ok((cfg, Vec::new()))
        }
    }

    pub fn load_or_create(path: &Path) -> Result<Self> {
        if path.exists() {
            let raw = fs::read_to_string(path)
                .map_err(|e| AgentError::Storage(format!("reading {}: {e}", path.display())))?;
            let sanitized = sanitize_user_toml(&raw);
            let mut cfg = parse_app_config_with_subpaths(&sanitized).map_err(|e| {
                AgentError::InvalidInput(format!("invalid TOML {}: {e}", path.display()))
            })?;
            cfg.apply_env_overrides();
            cfg.sync_sandbox_and_tools();

            let thinking_lower = cfg.inference.thinking_level.trim().to_ascii_lowercase();
            if matches!(
                thinking_lower.as_str(),
                "off" | "low" | "medium" | "high" | "max"
            ) {
                cfg.inference.thinking_level = thinking_lower;
            }
            for slot in [
                &mut cfg.inference.cache_type_k,
                &mut cfg.inference.cache_type_v,
            ] {
                let normalized = slot.trim().to_ascii_lowercase();
                if is_valid_ggml_kv_cache_type(&normalized) {
                    *slot = normalized;
                }
            }
            let driver_lower = cfg.database.driver.trim().to_ascii_lowercase();
            if matches!(driver_lower.as_str(), "auto" | "sqlite" | "jsonl") {
                cfg.database.driver = driver_lower;
            }
            let policy_lower = cfg.agents.on_disable_policy.trim().to_ascii_lowercase();
            if matches!(policy_lower.as_str(), "cancel" | "finish_step") {
                cfg.agents.on_disable_policy = policy_lower;
            }
            if let Some(mode) = SandboxMode::parse(&cfg.sandbox.mode) {
                cfg.sandbox.mode = mode.as_str().to_string();
            }
            if let Some(policy) = SandboxApprovalPolicy::parse(&cfg.sandbox.approval_policy) {
                cfg.sandbox.approval_policy = policy.as_str().to_string();
            }

            cfg.validate()?;
            Ok(cfg)
        } else {
            let cfg = Self::with_data_dir(preferred_data_dir_for_path(path));
            cfg.save(path)?;
            Ok(cfg)
        }
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        let mut synced = self.clone();
        synced.sync_sandbox_and_tools();
        synced.validate()?;
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            fs::create_dir_all(parent)
                .map_err(|e| AgentError::Storage(format!("creating {}: {e}", parent.display())))?;
        }
        let encoded = toml::to_string_pretty(&synced)
            .map_err(|e| AgentError::Internal(format!("serializing config: {e}")))?;
        let tmp = path.with_extension("toml.tmp");
        fs::write(&tmp, &encoded)
            .map_err(|e| AgentError::Storage(format!("writing {}: {e}", tmp.display())))?;
        if let Err(rename_err) = fs::rename(&tmp, path) {
            fs::write(path, &encoded).map_err(|write_err| {
                let _ = fs::remove_file(&tmp);
                AgentError::Storage(format!(
                    "renaming to {} ({rename_err}) and fallback write failed: {write_err}",
                    path.display()
                ))
            })?;
            let _ = fs::remove_file(&tmp);
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        let parsed = url::Url::parse(&self.inference.base_url)
            .map_err(|e| AgentError::InvalidInput(format!("inference.base_url invalid: {e}")))?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(AgentError::InvalidInput(
                "inference.base_url must be http or https".into(),
            ));
        }
        if self.inference.model.trim().is_empty() {
            return Err(AgentError::InvalidInput("inference.model cannot be empty".into()));
        }
        if self.inference.context_limit != 0
            && (self.inference.context_limit < 512 || self.inference.context_limit > 1_048_576)
        {
            return Err(AgentError::InvalidInput(
                "inference.context_limit must be 0 (server default) or 512..=1048576".into(),
            ));
        }
        if self.inference.max_output_tokens != 0
            && (self.inference.max_output_tokens < 64
                || (self.inference.context_limit > 0
                    && self.inference.max_output_tokens >= self.inference.context_limit))
        {
            return Err(AgentError::InvalidInput(
                "inference.max_output_tokens must be 0 (unlimited) or >= 64 and < context_limit"
                    .into(),
            ));
        }
        for (name, val) in [
            ("inference.temperature", self.inference.temperature),
            ("inference.dynatemp_range", self.inference.dynatemp_range),
            ("inference.dynatemp_exponent", self.inference.dynatemp_exponent),
            ("inference.top_p", self.inference.top_p),
            ("inference.min_p", self.inference.min_p),
            ("inference.typical_p", self.inference.typical_p),
            ("inference.xtc_probability", self.inference.xtc_probability),
            ("inference.xtc_threshold", self.inference.xtc_threshold),
            ("inference.top_n_sigma", self.inference.top_n_sigma),
            ("inference.repeat_penalty", self.inference.repeat_penalty),
            ("inference.presence_penalty", self.inference.presence_penalty),
            ("inference.frequency_penalty", self.inference.frequency_penalty),
            ("inference.dry_multiplier", self.inference.dry_multiplier),
            ("inference.dry_base", self.inference.dry_base),
            ("inference.mirostat_tau", self.inference.mirostat_tau),
            ("inference.mirostat_eta", self.inference.mirostat_eta),
        ] {
            if !val.is_finite() {
                return Err(AgentError::InvalidInput(format!("{name} must be finite")));
            }
        }
        if self.inference.temperature < 0.0 {
            return Err(AgentError::InvalidInput(
                "inference.temperature must be >= 0.0".into(),
            ));
        }
        if !(0.0..=1.0).contains(&self.inference.top_p) {
            return Err(AgentError::InvalidInput(
                "inference.top_p must be within 0.0..=1.0".into(),
            ));
        }
        if !(0.0..=1.0).contains(&self.inference.min_p) {
            return Err(AgentError::InvalidInput(
                "inference.min_p must be within 0.0..=1.0".into(),
            ));
        }
        if self.inference.dry_multiplier < 0.0 {
            return Err(AgentError::InvalidInput(
                "inference.dry_multiplier must be >= 0.0".into(),
            ));
        }
        if self.inference.mirostat > 2 {
            return Err(AgentError::InvalidInput(
                "inference.mirostat must be 0, 1, or 2".into(),
            ));
        }
        for (name, raw) in [
            ("inference.cache_type_k", &self.inference.cache_type_k),
            ("inference.cache_type_v", &self.inference.cache_type_v),
        ] {
            let normalized = raw.trim().to_ascii_lowercase();
            if !is_valid_ggml_kv_cache_type(&normalized) {
                return Err(AgentError::InvalidInput(format!(
                    "{name} must be empty or a valid ggml KV cache type (f32, f16, bf16, q8_0, q4_0, q4_1, iq4_nl, q5_0, q5_1)"
                )));
            }
        }
        let extra_json = self.inference.custom_extra_body_json.trim();
        if !extra_json.is_empty() {
            validate_json_object(extra_json).map_err(|reason| {
                AgentError::InvalidInput(format!(
                    "inference.custom_extra_body_json must be a valid JSON object: {reason}"
                ))
            })?;
        }
        if !matches!(
            self.inference
                .thinking_level
                .trim()
                .to_ascii_lowercase()
                .as_str(),
            "off" | "low" | "medium" | "high" | "max"
        ) {
            return Err(AgentError::InvalidInput(
                "inference.thinking_level must be off, low, medium, high, or max".into(),
            ));
        }
        if self.server.port == 0 {
            return Err(AgentError::InvalidInput("server.port must be 1..=65535".into()));
        }
        if self.server.max_body_bytes != 0
            && (self.server.max_body_bytes < 1024
                || self.server.max_body_bytes > 256 * 1024 * 1024)
        {
            return Err(AgentError::InvalidInput(
                "server.max_body_bytes must be 0 (unlimited) or between 1 KiB and 256 MiB".into(),
            ));
        }
        if self.server.upload_max_body_bytes != 0
            && (self.server.upload_max_body_bytes < 1024
                || self.server.upload_max_body_bytes > 256 * 1024 * 1024)
        {
            return Err(AgentError::InvalidInput(
                "server.upload_max_body_bytes must be 0 (unlimited) or between 1 KiB and 256 MiB"
                    .into(),
            ));
        }
        if self.security.session_ttl_hours > 87_600 {
            return Err(AgentError::InvalidInput(
                "security.session_ttl_hours must be 0 (unlimited) or <= 87600".into(),
            ));
        }
        if !matches!(
            self.database.driver.trim().to_ascii_lowercase().as_str(),
            "auto" | "sqlite" | "jsonl"
        ) {
            return Err(AgentError::InvalidInput(
                "database.driver must be auto, sqlite, or jsonl".into(),
            ));
        }
        if !matches!(
            self.agents
                .on_disable_policy
                .trim()
                .to_ascii_lowercase()
                .as_str(),
            "cancel" | "finish_step"
        ) {
            return Err(AgentError::InvalidInput(
                "agents.on_disable_policy must be cancel or finish_step".into(),
            ));
        }
        if SandboxMode::parse(&self.sandbox.mode).is_none() {
            return Err(AgentError::InvalidInput(
                "sandbox.mode must be read_only, workspace_write, or full_sandbox".into(),
            ));
        }
        if SandboxApprovalPolicy::parse(&self.sandbox.approval_policy).is_none() {
            return Err(AgentError::InvalidInput(
                "sandbox.approval_policy must be full_auto, on_request, always_ask, or never"
                    .into(),
            ));
        }
        if self.tools.allowed_paths.is_empty() {
            return Err(AgentError::InvalidInput(
                "tools.allowed_paths must include at least one directory".into(),
            ));
        }
        if self.runtime.tokio_worker_threads == 0 || self.runtime.tokio_worker_threads > 8 {
            return Err(AgentError::InvalidInput(
                "runtime.tokio_worker_threads must be 1..=8".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn lock_env() -> std::sync::MutexGuard<'static, ()> {
        ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    struct EnvGuard(Vec<(&'static str, Option<String>)>);

    impl EnvGuard {
        fn set(pairs: &[(&'static str, &str)]) -> Self {
            let saved = pairs
                .iter()
                .map(|(k, _)| (*k, std::env::var(k).ok()))
                .collect();
            for (k, v) in pairs {
                std::env::set_var(k, v);
            }
            Self(saved)
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (k, prev) in &self.0 {
                match prev {
                    Some(v) => std::env::set_var(k, v),
                    None => std::env::remove_var(k),
                }
            }
        }
    }

    fn test_dir(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!("ps5-cfg-test-{}-{label}", std::process::id()))
    }

    #[test]
    fn default_config_validates_and_roundtrips() {
        let _env = lock_env();
        let dir = test_dir("roundtrip");
        let _ = fs::remove_dir_all(&dir);
        let path = dir.join("config.toml");
        let cfg = AppConfig::load_or_create(&path).expect("create config");
        assert_eq!(cfg.agents.max_agents, 0);
        assert_eq!(cfg.agents.max_active_tasks, 0);
        assert_eq!(cfg.agents.max_queued_tasks, 0);
        assert_eq!(cfg.agents.max_concurrent_inference, 4);
        assert_eq!(cfg.agents.max_messages_per_task, 0);
        assert_eq!(cfg.agents.max_reasoning_iterations, 0);
        assert_eq!(cfg.agents.default_task_timeout_secs, 0);
        assert_eq!(cfg.memory.max_memories_per_agent, 0);
        assert_eq!(cfg.server.max_body_bytes, 1024 * 1024);
        assert_eq!(cfg.server.upload_max_body_bytes, 64 * 1024 * 1024);
        assert_eq!(cfg.inference.base_url, "http://127.0.0.1:8081/v1");
        assert_eq!(cfg.inference.thinking_level, "medium");
        assert!(cfg.inference.auto_sync_from_ps5lm);
        assert_eq!(cfg.inference.auto_sync_interval_secs, 30);
        assert_eq!(cfg.inference.repeat_last_n, 256);
        assert!((cfg.inference.dry_multiplier - 0.8).abs() < f32::EPSILON);
        assert!(cfg.inference.smart_tool_routing);
        assert!(cfg.inference.cache_type_k.is_empty());
        assert!(cfg.inference.cache_type_v.is_empty());
        assert!(!cfg.inference.flash_attn);
        assert_eq!(cfg.database.retention_days, 30);
        assert_eq!(cfg.database.max_rows_per_table, 10_000);
        assert!(cfg.security.admin_password.is_none());
        assert!(!cfg.security.require_approval_for_mutating);
        assert_eq!(cfg.sandbox.mode, "full_sandbox");
        assert_eq!(cfg.sandbox.approval_policy, "full_auto");
        assert!(cfg.sandbox.network_access);
        assert!(!cfg.sandbox.allow_local_network);
        assert_eq!(cfg.sandbox.web_search_provider, "duckduckgo");
        assert!(!cfg.sandbox.allow_host_shell);
        assert_eq!(cfg.sandbox.max_exec_output_bytes, 262_144);
        assert_eq!(cfg.sandbox.exec_timeout_secs, 20);
        assert!(cfg.sandbox.inject_brain_files);
        assert_eq!(cfg.sandbox.max_brain_context_chars, 4_000);
        assert_eq!(cfg.sandbox.heartbeat_interval_secs, 0);
        assert!(cfg.tools.blocked_domains.is_empty());
        assert!(cfg.tools.network_access);
        assert!(!cfg.tools.allow_local_network);
        assert_eq!(cfg.tools.web_search_provider, "duckduckgo");
        assert!(!cfg.tools.allow_host_shell);
        assert_eq!(cfg.tools.max_exec_output_bytes, 262_144);
        assert_eq!(cfg.tools.exec_timeout_secs, 20);

        let mut modified = cfg.clone();
        modified.inference.prefill_kv_cache = false;
        modified.inference.cache_prompt = false;
        modified.inference.temperature = 0.25;
        modified.inference.mirostat = 2;
        modified.inference.custom_extra_body_json = "{\"mirostat_tau\": 3.0}".to_string();
        modified.inference.smart_tool_routing = false;
        modified.inference.cache_type_k = "q8_0".to_string();
        modified.inference.cache_type_v = "q4_0".to_string();
        modified.inference.flash_attn = true;
        modified.save(&path).expect("save modified config");

        let reloaded = AppConfig::load_or_create(&path).expect("reload config");
        assert_eq!(reloaded.server.port, cfg.server.port);
        assert_eq!(reloaded.inference.thinking_level, "medium");
        assert!(!reloaded.inference.prefill_kv_cache);
        assert!(!reloaded.inference.cache_prompt);
        assert!((reloaded.inference.temperature - 0.25).abs() < f32::EPSILON);
        assert_eq!(reloaded.inference.mirostat, 2);
        assert_eq!(
            reloaded.inference.custom_extra_body_json,
            "{\"mirostat_tau\": 3.0}"
        );
        assert!(!reloaded.inference.smart_tool_routing);
        assert_eq!(reloaded.inference.cache_type_k, "q8_0");
        assert_eq!(reloaded.inference.cache_type_v, "q4_0");
        assert!(reloaded.inference.flash_attn);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn invalid_config_rejected() {
        let _env = lock_env();
        let mut cfg = AppConfig::default();
        cfg.inference.thinking_level = "ultra".to_string();
        assert!(cfg.validate().is_err());

        let mut cfg2 = AppConfig::default();
        cfg2.runtime.tokio_worker_threads = 99;
        assert!(cfg2.validate().is_err());

        let mut cfg3 = AppConfig::default();
        cfg3.inference.max_output_tokens = 32;
        assert!(cfg3.validate().is_err());
    }

    #[test]
    fn sanitize_strips_bom_and_nul_and_normalizes_booleans_safely() {
        let _env = lock_env();
        let raw = "\u{feff}[security]\r\nauth_enabled = \"false\" # disable on trusted LAN\r\nadmin_password = \"false\"\r\nrequire_approval_for_mutating = False\r\n[database]\r\nwal_mode = no\0\r\n[server]\r\nallow_lan_only = YES\r\n[inference]\r\nbase_url = \"http://127.0.0.1:8081/v1#keep\"\r\nstream_enabled = on\r\nauto_sync_from_ps5lm = \"false\"\r\nsmart_tool_routing = \"false\"\r\nflash_attn = YES\r\n[sandbox]\r\nnetwork_access = \"false\"\r\nallow_local_network = YES\r\nallow_host_shell = \"true\"\r\ninject_brain_files = Off\r\n[logging]\r\nenable_klog = TRUE\r\nenable_toast = off\r\n";
        let sanitized = sanitize_user_toml(raw);
        assert!(!sanitized.starts_with('\u{feff}'));
        assert!(!sanitized.contains('\0'));
        assert!(sanitized.contains("auth_enabled = false # disable on trusted LAN"));
        assert!(sanitized.contains("admin_password = \"false\""));
        assert!(sanitized.contains("require_approval_for_mutating = false"));
        assert!(sanitized.contains("wal_mode = false"));
        assert!(sanitized.contains("allow_lan_only = true"));
        assert!(sanitized.contains("base_url = \"http://127.0.0.1:8081/v1#keep\""));
        assert!(sanitized.contains("stream_enabled = true"));
        assert!(sanitized.contains("auto_sync_from_ps5lm = false"));
        assert!(sanitized.contains("smart_tool_routing = false"));
        assert!(sanitized.contains("flash_attn = true"));
        assert!(sanitized.contains("network_access = false"));
        assert!(sanitized.contains("allow_local_network = true"));
        assert!(sanitized.contains("allow_host_shell = true"));
        assert!(sanitized.contains("inject_brain_files = false"));
        assert!(sanitized.contains("enable_klog = true"));
        assert!(sanitized.contains("enable_toast = false"));

        let cfg: AppConfig = toml::from_str(&sanitized).expect("parse sanitized partial TOML");
        assert!(!cfg.security.auth_enabled);
        assert_eq!(cfg.security.admin_password.as_deref(), Some("false"));
        assert!(!cfg.security.require_approval_for_mutating);
        assert!(!cfg.database.wal_mode);
        assert!(cfg.server.allow_lan_only);
        assert_eq!(cfg.inference.base_url, "http://127.0.0.1:8081/v1#keep");
        assert!(cfg.inference.stream_enabled);
        assert!(!cfg.inference.auto_sync_from_ps5lm);
        assert!(!cfg.inference.smart_tool_routing);
        assert!(cfg.inference.flash_attn);
        assert!(!cfg.sandbox.network_access);
        assert!(cfg.sandbox.allow_local_network);
        assert!(cfg.sandbox.allow_host_shell);
        assert!(!cfg.sandbox.inject_brain_files);
        assert!(cfg.logging.enable_klog);
        assert!(!cfg.logging.enable_toast);
    }

    #[test]
    fn partial_toml_and_aliases_and_admin_password_parse() {
        let _env = lock_env();
        let dir = test_dir("partial-aliases");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.toml");
        fs::write(
            &path,
            "\u{feff}[inference]\nurl = \"http://10.0.0.5:8081/v1\"\n[server]\nhost = \"0.0.0.0\"\nport = 9090\n[security]\nauth = False\nusername = \"ps5admin\"\npassword = \"MySecretPass123\"\n",
        )
        .unwrap();

        let (cfg, warnings) = AppConfig::load_or_create_resilient(&path).expect("load partial");
        assert!(warnings.is_empty());
        assert_eq!(cfg.inference.base_url, "http://10.0.0.5:8081/v1");
        assert_eq!(cfg.inference.thinking_level, "medium");
        assert!(cfg.inference.auto_sync_from_ps5lm);
        assert_eq!(cfg.inference.auto_sync_interval_secs, 30);
        assert_eq!(cfg.inference.repeat_last_n, 256);
        assert!((cfg.inference.dry_multiplier - 0.8).abs() < f32::EPSILON);
        assert!(cfg.inference.smart_tool_routing);
        assert!(cfg.inference.cache_type_k.is_empty());
        assert!(cfg.inference.cache_type_v.is_empty());
        assert!(!cfg.inference.flash_attn);
        assert_eq!(cfg.server.bind_host, "0.0.0.0");
        assert_eq!(cfg.server.port, 9090);
        assert!(!cfg.security.auth_enabled);
        assert_eq!(cfg.security.admin_username, "ps5admin");
        assert_eq!(cfg.security.admin_password.as_deref(), Some("MySecretPass123"));
        assert_eq!(cfg.agents.max_agents, 0);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn normalize_and_clamp_adjusts_out_of_range_values_with_warnings() {
        let _env = lock_env();
        let mut cfg = AppConfig::default();
        cfg.inference.context_limit = 128;
        cfg.inference.max_output_tokens = 9999;
        cfg.inference.thinking_level = "EXTREME".to_string();
        cfg.server.max_body_bytes = 100;
        cfg.server.upload_max_body_bytes = 512;
        cfg.agents.max_agents = 200;
        cfg.agents.max_active_tasks = 0;
        cfg.agents.max_concurrent_inference = 99;
        cfg.agents.max_messages_per_task = 2000;
        cfg.agents.max_reasoning_iterations = 0;
        cfg.runtime.tokio_worker_threads = 64;
        cfg.tools.allowed_paths.clear();
        cfg.database.driver = "POSTGRES".to_string();
        cfg.agents.on_disable_policy = "EXPLODE".to_string();

        let warnings = cfg.normalize_and_clamp();
        assert_eq!(warnings.len(), 9);
        assert_eq!(cfg.inference.context_limit, 512);
        assert_eq!(cfg.inference.max_output_tokens, 448);
        assert_eq!(cfg.inference.thinking_level, "medium");
        assert_eq!(cfg.server.max_body_bytes, 1024);
        assert_eq!(cfg.server.upload_max_body_bytes, 1024);
        assert_eq!(cfg.agents.max_agents, 200);
        assert_eq!(cfg.agents.max_active_tasks, 0);
        assert_eq!(cfg.agents.max_concurrent_inference, 99);
        assert_eq!(cfg.agents.max_messages_per_task, 2000);
        assert_eq!(cfg.agents.max_reasoning_iterations, 0);
        assert_eq!(cfg.runtime.tokio_worker_threads, 8);
        assert_eq!(cfg.tools.allowed_paths.len(), 1);
        assert_eq!(cfg.database.driver, "auto");
        assert_eq!(cfg.agents.on_disable_policy, "cancel");
        cfg.validate().expect("clamped config should validate");
    }

    #[test]
    fn zero_unlimited_and_high_caps_validate_without_warnings() {
        let _env = lock_env();
        let mut cfg = AppConfig::default();
        cfg.inference.context_limit = 0;
        cfg.inference.max_output_tokens = 0;
        cfg.inference.thinking_level = "HIGH".to_string();
        cfg.inference.auto_sync_interval_secs = 0;
        cfg.server.max_body_bytes = 0;
        cfg.server.upload_max_body_bytes = 0;
        cfg.security.session_ttl_hours = 0;
        cfg.security.rate_limit_per_minute = 0;
        cfg.agents.max_agents = 0;
        cfg.agents.max_active_tasks = 0;
        cfg.agents.max_queued_tasks = 0;
        cfg.agents.max_concurrent_inference = 0;
        cfg.agents.max_messages_per_task = 0;
        cfg.agents.max_reasoning_iterations = 0;
        cfg.agents.default_task_timeout_secs = 0;
        cfg.memory.max_memories_per_agent = 0;
        cfg.database.retention_days = 0;
        cfg.database.max_rows_per_table = 0;

        let warnings = cfg.normalize_and_clamp();
        assert!(warnings.is_empty(), "expected no warnings, got: {warnings:?}");
        assert_eq!(cfg.inference.context_limit, 0);
        assert_eq!(cfg.inference.max_output_tokens, 0);
        assert_eq!(cfg.inference.thinking_level, "high");
        assert_eq!(cfg.server.max_body_bytes, 0);
        assert_eq!(cfg.server.upload_max_body_bytes, 0);
        assert_eq!(cfg.security.session_ttl_hours, 0);
        assert_eq!(cfg.security.rate_limit_per_minute, 0);
        assert_eq!(cfg.agents.max_agents, 0);
        assert_eq!(cfg.agents.max_active_tasks, 0);
        assert_eq!(cfg.agents.max_reasoning_iterations, 0);
        assert_eq!(cfg.memory.max_memories_per_agent, 0);
        cfg.validate().expect("all-zero unlimited config validates");

        let mut high = AppConfig::default();
        high.inference.context_limit = 1_048_576;
        high.inference.max_output_tokens = 65_536;
        high.server.max_body_bytes = 128 * 1024 * 1024;
        high.security.session_ttl_hours = 87_600;
        high.agents.max_agents = 200;
        high.agents.max_active_tasks = 128;
        high.agents.max_concurrent_inference = 16;
        high.agents.max_messages_per_task = 2000;
        high.agents.max_reasoning_iterations = 100;
        let warnings_high = high.normalize_and_clamp();
        assert!(warnings_high.is_empty(), "expected no warnings, got: {warnings_high:?}");
        high.validate().expect("high-cap config validates");
    }

    #[test]
    fn resilient_loader_recovers_broken_toml_and_writes_backup() {
        let _env = lock_env();
        let dir = test_dir("broken-bak");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.toml");
        let broken_contents = "[inference\nthis is not valid toml = = =";
        fs::write(&path, broken_contents).unwrap();

        let (cfg, warnings) = AppConfig::load_or_create_resilient(&path).expect("recover syntax");
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("toml.broken.bak"));
        let bak_path = path.with_extension("toml.broken.bak");
        assert!(bak_path.exists());
        assert_eq!(fs::read_to_string(&bak_path).unwrap(), broken_contents);
        cfg.validate().expect("recovered config validates");

        // Also recover when syntax is valid TOML but semantic validation fails (e.g. bad URL).
        let invalid_url_toml = "[inference]\nbase_url = \"ftp://bad-scheme\"\n";
        fs::write(&path, invalid_url_toml).unwrap();
        let (cfg2, warnings2) =
            AppConfig::load_or_create_resilient(&path).expect("recover validation failure");
        assert_eq!(warnings2.len(), 1);
        assert_eq!(fs::read_to_string(&bak_path).unwrap(), invalid_url_toml);
        assert_eq!(cfg2.inference.base_url, "http://127.0.0.1:8081/v1");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn sync_sandbox_and_tools_and_empty_domains_repair() {
        let _env = lock_env();
        let dir = test_dir("sync-tools-sandbox");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        // Tools-only overrides sync to sandbox; empty allowed_domains with network disabled stays empty.
        let path_net_off = dir.join("net_off.toml");
        fs::write(
            &path_net_off,
            "[tools]\nallowed_domains = []\nnetwork_access = false\nblock_private_lan_web_requests = false\nweb_search_provider = \"Brave\"\nsearxng_url = \"http://10.0.0.2:8888\"\nbrave_api_key = \"b-key\"\ntavily_api_key = \"t-key\"\nremote_browser_url = \"http://10.0.0.2:9222\"\nallow_host_shell = true\nmax_exec_output_bytes = 65536\nexec_timeout_secs = 45\n",
        )
        .unwrap();

        let (cfg_off, warnings_off) =
            AppConfig::load_or_create_resilient(&path_net_off).expect("load net_off");
        assert!(warnings_off.is_empty(), "got warnings: {warnings_off:?}");
        assert!(!cfg_off.sandbox.network_access);
        assert!(!cfg_off.tools.network_access);
        assert!(cfg_off.tools.allowed_domains.is_empty());
        assert!(cfg_off.sandbox.allow_local_network);
        assert!(cfg_off.tools.allow_local_network);
        assert!(!cfg_off.tools.block_private_lan_web_requests);
        assert_eq!(cfg_off.sandbox.web_search_provider, "brave");
        assert_eq!(cfg_off.tools.web_search_provider, "brave");
        assert_eq!(cfg_off.sandbox.searxng_url, "http://10.0.0.2:8888");
        assert_eq!(cfg_off.sandbox.brave_api_key, "b-key");
        assert_eq!(cfg_off.sandbox.tavily_api_key, "t-key");
        assert_eq!(cfg_off.sandbox.remote_browser_url, "http://10.0.0.2:9222");
        assert!(cfg_off.sandbox.allow_host_shell);
        assert_eq!(cfg_off.sandbox.max_exec_output_bytes, 65_536);
        assert_eq!(cfg_off.sandbox.exec_timeout_secs, 45);

        // Bug #6: empty allowed_domains with network_access enabled repairs to ["*"] with warning.
        let path_bug6 = dir.join("bug6.toml");
        fs::write(&path_bug6, "[tools]\nallowed_domains = []\n").unwrap();
        let (mut cfg_bug6, warnings_bug6) =
            AppConfig::load_or_create_resilient(&path_bug6).expect("load bug6");
        assert_eq!(warnings_bug6.len(), 1);
        assert!(warnings_bug6[0].contains("allowed_domains"));
        assert_eq!(cfg_bug6.tools.allowed_domains, vec!["*".to_string()]);

        // Saving syncs sandbox changes into tools too.
        cfg_bug6.sandbox.web_search_provider = "tavily".to_string();
        cfg_bug6.sandbox.allow_host_shell = true;
        cfg_bug6.save(&path_bug6).expect("save synced");
        let reloaded = AppConfig::load_or_create(&path_bug6).expect("reload synced");
        assert_eq!(reloaded.tools.web_search_provider, "tavily");
        assert!(reloaded.tools.allow_host_shell);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn partial_toml_derives_subpaths_from_overridden_data_dir() {
        let _env = lock_env();
        let dir = test_dir("data-dir-subpaths");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let custom_root = dir.join("custom-root");
        let escaped_root = custom_root.to_string_lossy().replace('\\', "\\\\");

        let partial_path = dir.join("partial.toml");
        fs::write(&partial_path, format!("data_dir = \"{escaped_root}\"\n")).unwrap();
        let (cfg, warnings) =
            AppConfig::load_or_create_resilient(&partial_path).expect("resilient subpath derive");
        assert!(warnings.is_empty());
        assert_eq!(cfg.data_dir, custom_root);
        assert_eq!(cfg.database.path, custom_root.join("database").join("agent.db"));
        assert_eq!(cfg.logging.log_dir, custom_root.join("logs"));
        assert_eq!(
            cfg.tools.allowed_paths,
            vec![custom_root.join("workspace").to_string_lossy().into_owned()]
        );

        let strict_cfg = AppConfig::load_or_create(&partial_path).expect("strict subpath derive");
        assert_eq!(
            strict_cfg.database.path,
            custom_root.join("database").join("agent.db")
        );
        assert_eq!(strict_cfg.logging.log_dir, custom_root.join("logs"));

        // Explicit sub-path stays intact while omitted sub-paths still derive from data_dir.
        let explicit_db = dir.join("explicit.db");
        let escaped_db = explicit_db.to_string_lossy().replace('\\', "\\\\");
        let mixed_path = dir.join("mixed.toml");
        fs::write(
            &mixed_path,
            format!("data_dir = \"{escaped_root}\"\n[database]\npath = \"{escaped_db}\"\n"),
        )
        .unwrap();
        let mixed = AppConfig::load_or_create(&mixed_path).expect("mixed subpaths");
        assert_eq!(mixed.database.path, explicit_db);
        assert_eq!(mixed.logging.log_dir, custom_root.join("logs"));
        assert_eq!(
            mixed.tools.allowed_paths,
            vec![custom_root.join("workspace").to_string_lossy().into_owned()]
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn validation_and_normalization_for_floats_mirostat_ttl_json_and_enums() {
        let _env = lock_env();
        let mut cfg = AppConfig::default();
        cfg.inference.top_p = f32::NAN;
        assert!(cfg.validate().is_err());

        let mut cfg_inf = AppConfig::default();
        cfg_inf.inference.mirostat_tau = f32::INFINITY;
        assert!(cfg_inf.validate().is_err());

        let mut cfg_neg_temp = AppConfig::default();
        cfg_neg_temp.inference.temperature = -0.1;
        assert!(cfg_neg_temp.validate().is_err());

        let mut cfg_miro = AppConfig::default();
        cfg_miro.inference.mirostat = 3;
        assert!(cfg_miro.validate().is_err());

        let mut cfg_ttl = AppConfig::default();
        cfg_ttl.security.session_ttl_hours = 87_601;
        assert!(cfg_ttl.validate().is_err());

        for bad_json in [
            "[1, 2]",
            "\"hello\"",
            "{unquoted: 1}",
            "{\"a\": 1,}",
            "{\"a\": 1} trailing",
        ] {
            let mut bad_cfg = AppConfig::default();
            bad_cfg.inference.custom_extra_body_json = bad_json.to_string();
            assert!(
                bad_cfg.validate().is_err(),
                "expected invalid JSON object error for {bad_json}"
            );
        }

        let mut bad_mode = AppConfig::default();
        bad_mode.sandbox.mode = "host".to_string();
        assert!(bad_mode.validate().is_err());

        let mut bad_policy = AppConfig::default();
        bad_policy.sandbox.approval_policy = "yolo".to_string();
        assert!(bad_policy.validate().is_err());

        // Strict loader handles case normalization, extra_request_body_json alias, and valid edge bounds.
        let dir = test_dir("validate-edges");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.toml");
        fs::write(
            &path,
            "[inference]\ntemperature = 0.0\ntop_n_sigma = -1.0\nmirostat = 2\nextra_request_body_json = '{\"nested\":{\"arr\":[1,true,null,\"ok\"]}}'\n[security]\nsession_ttl_hours = 87600\n[database]\ndriver = \"SQLite\"\n[agents]\non_disable_policy = \"Finish_Step\"\n[sandbox]\nmode = \"Workspace-Write\"\napproval_policy = \"on-request\"\n",
        )
        .unwrap();
        let strict = AppConfig::load_or_create(&path).expect("valid strict config");
        assert_eq!(strict.database.driver, "sqlite");
        assert_eq!(strict.agents.on_disable_policy, "finish_step");
        assert_eq!(strict.sandbox.mode, "workspace_write");
        assert_eq!(strict.sandbox.approval_policy, "on_request");
        assert_eq!(strict.inference.mirostat, 2);
        assert_eq!(strict.security.session_ttl_hours, 87_600);

        // Resilient normalization resets non-finite floats and clamps out-of-range fields with warnings.
        let mut norm = AppConfig::default();
        norm.inference.temperature = f32::NAN;
        norm.inference.top_p = f32::INFINITY;
        norm.inference.mirostat = 7;
        norm.security.session_ttl_hours = 100_000;
        norm.sandbox.mode = "invalid-mode".to_string();
        norm.sandbox.approval_policy = "invalid-policy".to_string();
        let warnings = norm.normalize_and_clamp();
        assert_eq!(warnings.len(), 6);
        assert!((norm.inference.temperature - 0.4).abs() < f32::EPSILON);
        assert!((norm.inference.top_p - 0.95).abs() < f32::EPSILON);
        assert_eq!(norm.inference.mirostat, 2);
        assert_eq!(norm.security.session_ttl_hours, 87_600);
        assert_eq!(norm.sandbox.mode, "full_sandbox");
        assert_eq!(norm.sandbox.approval_policy, "full_auto");
        norm.validate().expect("normalized config validates");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn env_overrides_populate_sandbox_and_tools() {
        let _env = lock_env();
        let _vars = EnvGuard::set(&[
            ("PS5_AGENT_SANDBOX_MODE", "workspace-write"),
            ("PS5_AGENT_APPROVAL_POLICY", "on-request"),
            ("PS5_AGENT_SEARCH_PROVIDER", "SearXNG"),
            ("PS5_AGENT_SEARXNG_URL", "http://127.0.0.1:8888"),
            ("PS5_AGENT_BRAVE_API_KEY", "brave-env-key"),
            ("PS5_AGENT_TAVILY_API_KEY", "tavily-env-key"),
            ("PS5_AGENT_REMOTE_BROWSER_URL", "http://127.0.0.1:9222"),
        ]);

        let dir = test_dir("env-overrides");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.toml");
        fs::write(
            &path,
            "[sandbox]\nmode = \"full_sandbox\"\napproval_policy = \"full_auto\"\nweb_search_provider = \"duckduckgo\"\n",
        )
        .unwrap();

        let (cfg, warnings) =
            AppConfig::load_or_create_resilient(&path).expect("load with env overrides");
        assert!(warnings.is_empty(), "got warnings: {warnings:?}");
        assert_eq!(cfg.sandbox.mode, "workspace_write");
        assert_eq!(cfg.sandbox.approval_policy, "on_request");
        assert_eq!(cfg.sandbox.web_search_provider, "searxng");
        assert_eq!(cfg.tools.web_search_provider, "searxng");
        assert_eq!(cfg.sandbox.searxng_url, "http://127.0.0.1:8888");
        assert_eq!(cfg.tools.searxng_url, "http://127.0.0.1:8888");
        assert_eq!(cfg.sandbox.brave_api_key, "brave-env-key");
        assert_eq!(cfg.tools.brave_api_key, "brave-env-key");
        assert_eq!(cfg.sandbox.tavily_api_key, "tavily-env-key");
        assert_eq!(cfg.tools.tavily_api_key, "tavily-env-key");
        assert_eq!(cfg.sandbox.remote_browser_url, "http://127.0.0.1:9222");
        assert_eq!(cfg.tools.remote_browser_url, "http://127.0.0.1:9222");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn sampling_kv_clamping_validation_retry_and_repo_templates() {
        let _env = lock_env();

        // Retry helper handles cold-boot recovery within 12 attempts and bounded failure.
        let mut tries = 0u32;
        let recovered = retry_writable_probe(
            DATA_MOUNT_RETRY_ATTEMPTS,
            std::time::Duration::ZERO,
            || {
                tries += 1;
                tries >= 3
            },
        );
        assert_eq!(recovered, Some(4));

        let mut never_tries = 0u32;
        let exhausted = retry_writable_probe(
            DATA_MOUNT_RETRY_ATTEMPTS,
            std::time::Duration::ZERO,
            || {
                never_tries += 1;
                false
            },
        );
        assert_eq!(exhausted, None);
        assert_eq!(never_tries, DATA_MOUNT_RETRY_ATTEMPTS - 1);

        // Strict validation enforces sampling & KV cache type bounds.
        let mut bad_top_p = AppConfig::default();
        bad_top_p.inference.top_p = 1.2;
        assert!(bad_top_p.validate().is_err());
        bad_top_p.inference.top_p = -0.1;
        assert!(bad_top_p.validate().is_err());

        let mut bad_min_p = AppConfig::default();
        bad_min_p.inference.min_p = -0.01;
        assert!(bad_min_p.validate().is_err());
        bad_min_p.inference.min_p = 1.05;
        assert!(bad_min_p.validate().is_err());

        let mut bad_dry = AppConfig::default();
        bad_dry.inference.dry_multiplier = -0.5;
        assert!(bad_dry.validate().is_err());

        let mut bad_kv_k = AppConfig::default();
        bad_kv_k.inference.cache_type_k = "fp8".to_string();
        assert!(bad_kv_k.validate().is_err());

        let mut bad_kv_v = AppConfig::default();
        bad_kv_v.inference.cache_type_v = "int8".to_string();
        assert!(bad_kv_v.validate().is_err());

        for valid_kv in [
            "", "f32", "f16", "bf16", "q8_0", "q4_0", "q4_1", "iq4_nl", "q5_0", "q5_1",
        ] {
            let mut valid_cfg = AppConfig::default();
            valid_cfg.inference.cache_type_k = valid_kv.to_string();
            valid_cfg.inference.cache_type_v = valid_kv.to_string();
            valid_cfg
                .validate()
                .unwrap_or_else(|e| panic!("kv type {valid_kv:?} should validate: {e}"));
        }

        // Strict loader trims & lowercases valid KV cache types.
        let dir = test_dir("kv-cache-strict");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let strict_path = dir.join("config.toml");
        fs::write(
            &strict_path,
            "[inference]\ncache_type_k = \" Q8_0 \"\ncache_type_v = \" Q5_1 \"\n",
        )
        .unwrap();
        let strict_cfg = AppConfig::load_or_create(&strict_path).expect("strict kv normalize");
        assert_eq!(strict_cfg.inference.cache_type_k, "q8_0");
        assert_eq!(strict_cfg.inference.cache_type_v, "q5_1");
        let _ = fs::remove_dir_all(&dir);

        // Resilient clamping resets NaN dry_multiplier to 0.8 and repairs out-of-range sampling/KV fields.
        let mut clamp_cfg = AppConfig::default();
        clamp_cfg.inference.dry_multiplier = f32::NAN;
        clamp_cfg.inference.top_p = 1.5;
        clamp_cfg.inference.min_p = -0.2;
        clamp_cfg.inference.cache_type_k = " Q4_0 ".to_string();
        clamp_cfg.inference.cache_type_v = "invalid_kv".to_string();
        let warnings = clamp_cfg.normalize_and_clamp();
        assert_eq!(warnings.len(), 4);
        assert!((clamp_cfg.inference.dry_multiplier - 0.8).abs() < f32::EPSILON);
        assert!((clamp_cfg.inference.top_p - 1.0).abs() < f32::EPSILON);
        assert!((clamp_cfg.inference.min_p - 0.0).abs() < f32::EPSILON);
        assert_eq!(clamp_cfg.inference.cache_type_k, "q4_0");
        assert_eq!(clamp_cfg.inference.cache_type_v, "");
        clamp_cfg.validate().expect("clamped sampling/KV config validates");

        clamp_cfg.inference.dry_multiplier = -1.0;
        let dry_warnings = clamp_cfg.normalize_and_clamp();
        assert_eq!(dry_warnings.len(), 1);
        assert!((clamp_cfg.inference.dry_multiplier - 0.0).abs() < f32::EPSILON);

        // Checked-in TOML templates parse, normalize cleanly, and stay in sync on inference defaults.
        for (label, raw_toml) in [
            (
                "config.ps5-slim-1340.toml",
                include_str!("../../../config/config.ps5-slim-1340.toml"),
            ),
            (
                "config.example.toml",
                include_str!("../../../config/config.example.toml"),
            ),
        ] {
            let mut tpl =
                parse_app_config_with_subpaths(&sanitize_user_toml(raw_toml)).unwrap_or_else(|e| {
                    panic!("{label} should parse: {e}");
                });
            tpl.sync_sandbox_and_tools();
            let tpl_warnings = tpl.normalize_and_clamp();
            assert!(
                tpl_warnings.is_empty(),
                "{label} emitted unexpected warnings: {tpl_warnings:?}"
            );
            tpl.validate()
                .unwrap_or_else(|e| panic!("{label} failed validation: {e}"));
            assert_eq!(tpl.inference.repeat_last_n, 256, "{label} repeat_last_n");
            assert!(
                (tpl.inference.dry_multiplier - 0.8).abs() < f32::EPSILON,
                "{label} dry_multiplier"
            );
            assert!(tpl.inference.smart_tool_routing, "{label} smart_tool_routing");
            assert!(tpl.inference.cache_type_k.is_empty(), "{label} cache_type_k");
            assert!(tpl.inference.cache_type_v.is_empty(), "{label} cache_type_v");
            assert!(!tpl.inference.flash_attn, "{label} flash_attn");
        }
    }
}
