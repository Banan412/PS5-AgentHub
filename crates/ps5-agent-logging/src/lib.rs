use chrono::Utc;
use ps5_agent_config::LoggingConfig;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock, TryLockError};
use std::time::{Duration, Instant};

pub const MAX_LOG_ENTRY_BYTES: usize = 64 * 1024;
const MAX_LOG_TARGET_BYTES: usize = 256;
const MIN_RING_CAPACITY: usize = 64;
const MAX_RING_CAPACITY: usize = 16_384;
const MIN_LOG_FILE_BYTES: u64 = 64 * 1024;
const MAX_LOG_FILE_BYTES: u64 = 128 * 1024 * 1024;
const IO_DIAG_THROTTLE: Duration = Duration::from_secs(30);
const REOPEN_RETRY_INTERVAL: Duration = Duration::from_secs(5);
const TRUNCATED_SUFFIX: &str = "...[truncated]";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum LogLevel {
    Error = 1,
    Warn = 2,
    Info = 3,
    Debug = 4,
    Trace = 5,
}

impl LogLevel {
    pub fn parse_strict(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "error" => Some(Self::Error),
            "warn" | "warning" => Some(Self::Warn),
            "info" => Some(Self::Info),
            "debug" => Some(Self::Debug),
            "trace" => Some(Self::Trace),
            _ => None,
        }
    }

    pub fn parse(s: &str) -> Self {
        Self::parse_strict(s).unwrap_or(Self::Info)
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Error => "ERROR",
            Self::Warn => "WARN",
            Self::Info => "INFO",
            Self::Debug => "DEBUG",
            Self::Trace => "TRACE",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub seq: u64,
    pub timestamp: String,
    pub level: LogLevel,
    pub target: String,
    pub message: String,
}

fn truncate_utf8_with_suffix(s: &str, max_bytes: usize, suffix: &str) -> String {
    if s.len() <= max_bytes {
        return s.to_string();
    }
    if max_bytes <= suffix.len() {
        let mut end = max_bytes.min(s.len());
        while end > 0 && !s.is_char_boundary(end) {
            end -= 1;
        }
        return s[..end].to_string();
    }
    let mut end = (max_bytes - suffix.len()).min(s.len());
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{}", &s[..end], suffix)
}

fn format_entry_line(entry: &LogEntry) -> String {
    format!(
        "{} [{}] [{}] {}",
        entry.timestamp,
        entry.level.as_str(),
        entry.target,
        entry.message
    )
}

fn format_entry_line_nl(entry: &LogEntry) -> String {
    format!(
        "{} [{}] [{}] {}\n",
        entry.timestamp,
        entry.level.as_str(),
        entry.target,
        entry.message
    )
}

fn write_stderr_line(line: &str) {
    let _ = writeln!(std::io::stderr(), "{line}");
}

struct RingState {
    level: LogLevel,
    ring: VecDeque<LogEntry>,
    capacity: usize,
    next_seq: u64,
    #[cfg_attr(not(target_vendor = "sie"), allow(dead_code))]
    enable_klog: bool,
    #[cfg_attr(not(target_vendor = "sie"), allow(dead_code))]
    enable_toast: bool,
}

impl RingState {
    fn push_entry(&mut self, level: LogLevel, target: &str, msg: &str) -> LogEntry {
        let seq = self.next_seq;
        self.next_seq = self.next_seq.saturating_add(1);
        let entry = LogEntry {
            seq,
            timestamp: Utc::now().to_rfc3339(),
            level,
            target: truncate_utf8_with_suffix(target, MAX_LOG_TARGET_BYTES, "..."),
            message: truncate_utf8_with_suffix(msg, MAX_LOG_ENTRY_BYTES, TRUNCATED_SUFFIX),
        };
        if self.capacity > 0 {
            while self.ring.len() >= self.capacity {
                self.ring.pop_front();
            }
            self.ring.push_back(entry.clone());
        }
        entry
    }
}

struct FileSinkState {
    log_file_path: PathBuf,
    max_file_bytes: usize,
    file: Option<File>,
    last_io_error_diag: Option<Instant>,
    last_reopen_attempt: Option<Instant>,
    last_rotate_failure: Option<Instant>,
    suppressed_io_errors: u64,
}

impl FileSinkState {
    fn note_io_error(&mut self, context: &str, err: impl std::fmt::Display) -> Option<String> {
        let now = Instant::now();
        let should_emit = match self.last_io_error_diag {
            None => true,
            Some(prev) => now.saturating_duration_since(prev) >= IO_DIAG_THROTTLE,
        };
        if should_emit {
            let suppressed = self.suppressed_io_errors;
            self.suppressed_io_errors = 0;
            self.last_io_error_diag = Some(now);
            if suppressed > 0 {
                Some(format!("{context}: {err} (suppressed {suppressed} earlier error(s))"))
            } else {
                Some(format!("{context}: {err}"))
            }
        } else {
            self.suppressed_io_errors = self.suppressed_io_errors.saturating_add(1);
            None
        }
    }

    fn write_and_rotate(&mut self, line_with_nl: &str, flush_now: bool) -> Option<String> {
        let mut diag: Option<String> = None;

        if self.file.is_none() {
            let should_retry = self
                .last_reopen_attempt
                .map_or(true, |t| t.elapsed() >= REOPEN_RETRY_INTERVAL);
            if should_retry {
                self.last_reopen_attempt = Some(Instant::now());
                if let Some(parent) = self.log_file_path.parent() {
                    let _ = fs::create_dir_all(parent);
                }
                match OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&self.log_file_path)
                {
                    Ok(f) => self.file = Some(f),
                    Err(e) => {
                        diag = self.note_io_error(
                            &format!("failed to open log file {}", self.log_file_path.display()),
                            e,
                        );
                    }
                }
            }
        }

        let mut rotate_now = false;
        if let Some(ref mut f) = self.file {
            if let Err(e) = f.write_all(line_with_nl.as_bytes()) {
                if diag.is_none() {
                    diag = self.note_io_error(
                        &format!("failed to write log file {}", self.log_file_path.display()),
                        e,
                    );
                }
            } else {
                if flush_now {
                    let _ = f.flush();
                }
                if let Ok(meta) = f.metadata() {
                    if meta.len() > self.max_file_bytes as u64 {
                        let cooldown_active = self
                            .last_rotate_failure
                            .is_some_and(|t| t.elapsed() < REOPEN_RETRY_INTERVAL);
                        if !cooldown_active {
                            rotate_now = true;
                        }
                    }
                }
            }
        }

        if rotate_now {
            let mut rotate_failed = false;
            let rotated = self.log_file_path.with_extension("log.1");
            if let Err(e) = fs::remove_file(&rotated) {
                if e.kind() != ErrorKind::NotFound {
                    rotate_failed = true;
                    if diag.is_none() {
                        diag = self.note_io_error(
                            &format!("failed to remove rotated log {}", rotated.display()),
                            e,
                        );
                    }
                }
            }
            drop(self.file.take());
            if !rotate_failed {
                if let Err(e) = fs::rename(&self.log_file_path, &rotated) {
                    rotate_failed = true;
                    if diag.is_none() {
                        diag = self.note_io_error(
                            &format!("failed to rotate log to {}", rotated.display()),
                            e,
                        );
                    }
                }
            }
            match OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.log_file_path)
            {
                Ok(new_file) => self.file = Some(new_file),
                Err(e) => {
                    rotate_failed = true;
                    if diag.is_none() {
                        diag = self.note_io_error(
                            &format!("failed to reopen log file {}", self.log_file_path.display()),
                            e,
                        );
                    }
                }
            }
            self.last_rotate_failure = if rotate_failed {
                Some(Instant::now())
            } else {
                None
            };
        }

        diag
    }
}

#[derive(Clone)]
pub struct AppLogger {
    state: Arc<Mutex<RingState>>,
    sink: Arc<Mutex<FileSinkState>>,
}

static GLOBAL_LOGGER: OnceLock<AppLogger> = OnceLock::new();

impl AppLogger {
    pub fn init(cfg: &LoggingConfig) -> Self {
        let mut init_errors: Vec<String> = Vec::new();
        if let Err(e) = fs::create_dir_all(&cfg.log_dir) {
            init_errors.push(format!(
                "failed to create log directory {}: {e}",
                cfg.log_dir.display()
            ));
        }

        let log_file_path = cfg.log_dir.join("ps5-agent.log");
        let file = match OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_file_path)
        {
            Ok(f) => Some(f),
            Err(e) => {
                init_errors.push(format!(
                    "failed to open log file {}: {e}",
                    log_file_path.display()
                ));
                None
            }
        };

        let capacity = cfg
            .ring_buffer_capacity
            .clamp(MIN_RING_CAPACITY, MAX_RING_CAPACITY);
        let max_file_bytes = usize::try_from(
            (cfg.max_file_bytes as u64).clamp(MIN_LOG_FILE_BYTES, MAX_LOG_FILE_BYTES),
        )
        .unwrap_or(usize::MAX);

        let initial_diag = if init_errors.is_empty() {
            None
        } else {
            Some(Instant::now())
        };

        let mut ring_state = RingState {
            level: LogLevel::parse(&cfg.level),
            ring: VecDeque::with_capacity(capacity),
            capacity,
            next_seq: 1,
            enable_klog: cfg.enable_klog,
            enable_toast: cfg.enable_toast,
        };
        for err in &init_errors {
            let entry = ring_state.push_entry(LogLevel::Warn, "logging", err);
            write_stderr_line(&format_entry_line(&entry));
        }

        let logger = Self {
            state: Arc::new(Mutex::new(ring_state)),
            sink: Arc::new(Mutex::new(FileSinkState {
                log_file_path,
                max_file_bytes,
                file,
                last_io_error_diag: initial_diag,
                last_reopen_attempt: None,
                last_rotate_failure: None,
                suppressed_io_errors: 0,
            })),
        };
        let _ = GLOBAL_LOGGER.set(logger.clone());
        logger
    }

    pub fn global() -> Option<&'static AppLogger> {
        GLOBAL_LOGGER.get()
    }

    pub fn set_level(&self, level: &str) {
        let mut g = self.state.lock().unwrap_or_else(|e| e.into_inner());
        g.level = LogLevel::parse(level);
    }

    pub fn apply_config(
        &self,
        level: LogLevel,
        max_file_bytes: u64,
        ring_capacity: usize,
        enable_klog: bool,
        enable_toast: bool,
    ) {
        let clamped_cap = ring_capacity.clamp(1, MAX_RING_CAPACITY);
        let clamped_max_bytes = usize::try_from(
            max_file_bytes.clamp(MIN_LOG_FILE_BYTES, MAX_LOG_FILE_BYTES),
        )
        .unwrap_or(usize::MAX);

        {
            let mut g = self.state.lock().unwrap_or_else(|e| e.into_inner());
            let prev_capacity = g.capacity;
            g.level = level;
            g.capacity = clamped_cap;
            let mut evicted = false;
            while g.ring.len() > clamped_cap {
                g.ring.pop_front();
                evicted = true;
            }
            if evicted || clamped_cap < prev_capacity {
                g.ring.shrink_to_fit();
            }
            g.enable_klog = enable_klog;
            g.enable_toast = enable_toast;
        }

        {
            let mut s = self.sink.lock().unwrap_or_else(|e| e.into_inner());
            s.max_file_bytes = clamped_max_bytes;
        }
    }

    pub fn log(&self, level: LogLevel, target: &str, message: impl AsRef<str>) {
        let msg = message.as_ref();
        let (entry, enable_klog) = {
            let mut g = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if level > g.level {
                return;
            }
            let entry = g.push_entry(level, target, msg);
            (entry, g.enable_klog)
        };

        let line_with_nl = format_entry_line_nl(&entry);

        let sink_diag = {
            let mut s = self.sink.lock().unwrap_or_else(|e| e.into_inner());
            s.write_and_rotate(&line_with_nl, level == LogLevel::Error)
        };

        if let Some(diag) = sink_diag {
            let diag_entry = {
                let mut g = self.state.lock().unwrap_or_else(|e| e.into_inner());
                g.push_entry(LogLevel::Warn, "logging", &diag)
            };
            write_stderr_line(&format_entry_line(&diag_entry));
        }

        // BrokenPipe-safe stdout write (elfldr binds fd 1 to sender socket)
        let _ = std::io::stdout().write_all(line_with_nl.as_bytes());

        #[cfg(target_vendor = "sie")]
        if enable_klog {
            let kline = format!(
                "[ps5-agent][{}] {}: {}",
                entry.level.as_str(),
                entry.target,
                entry.message
            );
            ps5::klog::log(&kline);
        }
        #[cfg(not(target_vendor = "sie"))]
        let _ = enable_klog;
    }

    pub fn info(&self, target: &str, message: impl AsRef<str>) {
        self.log(LogLevel::Info, target, message);
    }

    pub fn warn(&self, target: &str, message: impl AsRef<str>) {
        self.log(LogLevel::Warn, target, message);
    }

    pub fn error(&self, target: &str, message: impl AsRef<str>) {
        self.log(LogLevel::Error, target, message);
    }

    pub fn debug(&self, target: &str, message: impl AsRef<str>) {
        self.log(LogLevel::Debug, target, message);
    }

    pub fn trace(&self, target: &str, message: impl AsRef<str>) {
        self.log(LogLevel::Trace, target, message);
    }

    pub fn toast(&self, title: &str, body: &str) {
        self.info("toast", format!("{title}: {body}"));
        #[cfg(target_vendor = "sie")]
        {
            let enabled = {
                let g = self.state.lock().unwrap_or_else(|e| e.into_inner());
                g.enable_toast
            };
            if enabled {
                if let Err(err) = ps5::Toast::new(title).sub(body).send() {
                    let msg = format!("ps5 toast send failed: {:?}", err);
                    write_stderr_line(&msg);
                    ps5::klog::log(&format!("[ps5-agent][WARN] toast: {msg}"));
                }
            }
        }
    }

    pub fn flush(&self) {
        {
            let mut s = self.sink.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(ref mut f) = s.file {
                let _ = f.flush();
            }
        }
        let _ = std::io::stdout().flush();
    }

    pub fn recent(&self, level_filter: Option<&str>, limit: usize) -> Vec<LogEntry> {
        if limit == 0 {
            return Vec::new();
        }
        let min_level = level_filter.map(LogLevel::parse);
        let g = self.state.lock().unwrap_or_else(|e| e.into_inner());
        g.ring
            .iter()
            .rev()
            .filter(|e| match min_level {
                Some(l) => e.level <= l,
                None => true,
            })
            .take(limit)
            .cloned()
            .collect()
    }
}

pub fn log_info(target: &str, msg: impl AsRef<str>) {
    if let Some(l) = AppLogger::global() {
        l.info(target, msg);
    }
}

pub fn log_warn(target: &str, msg: impl AsRef<str>) {
    if let Some(l) = AppLogger::global() {
        l.warn(target, msg);
    }
}

pub fn log_error(target: &str, msg: impl AsRef<str>) {
    let msg_ref = msg.as_ref();
    if let Some(l) = AppLogger::global() {
        l.error(target, msg_ref);
    } else {
        let capped = truncate_utf8_with_suffix(msg_ref, MAX_LOG_ENTRY_BYTES, TRUNCATED_SUFFIX);
        let line = format!("{} [ERROR] [{}] {}", Utc::now().to_rfc3339(), target, capped);
        write_stderr_line(&line);
        #[cfg(target_vendor = "sie")]
        ps5::klog::log(&format!("[ps5-agent][ERROR] {target}: {capped}"));
    }
}

pub fn log_debug(target: &str, msg: impl AsRef<str>) {
    if let Some(l) = AppLogger::global() {
        l.debug(target, msg);
    }
}

pub fn log_trace(target: &str, msg: impl AsRef<str>) {
    if let Some(l) = AppLogger::global() {
        l.trace(target, msg);
    }
}

fn handle_panic_log(logger: &AppLogger, full: &str) {
    let capped = truncate_utf8_with_suffix(full, MAX_LOG_ENTRY_BYTES, TRUNCATED_SUFFIX);
    let fallback_line = format!("{} [ERROR] [panic] {}", Utc::now().to_rfc3339(), capped);
    write_stderr_line(&fallback_line);
    #[cfg(target_vendor = "sie")]
    ps5::klog::log(&format!("[ps5-agent][FATAL] {capped}"));

    let (line_for_file, enable_toast) = match logger.state.try_lock() {
        Ok(mut g) => {
            let entry = g.push_entry(LogLevel::Error, "panic", &capped);
            (format_entry_line(&entry), g.enable_toast)
        }
        Err(TryLockError::Poisoned(p)) => {
            let mut g = p.into_inner();
            let entry = g.push_entry(LogLevel::Error, "panic", &capped);
            (format_entry_line(&entry), g.enable_toast)
        }
        Err(TryLockError::WouldBlock) => (fallback_line.clone(), true),
    };

    match logger.sink.try_lock() {
        Ok(mut s) => {
            if let Some(ref mut f) = s.file {
                let _ = writeln!(f, "{line_for_file}");
                let _ = f.flush();
            }
        }
        Err(TryLockError::Poisoned(p)) => {
            let mut s = p.into_inner();
            if let Some(ref mut f) = s.file {
                let _ = writeln!(f, "{line_for_file}");
                let _ = f.flush();
            }
        }
        Err(TryLockError::WouldBlock) => {}
    }

    #[cfg(target_vendor = "sie")]
    if enable_toast {
        let _ = ps5::Toast::new("ps5-agent panic").sub(&capped).send();
    }
    #[cfg(not(target_vendor = "sie"))]
    let _ = enable_toast;

    let _ = std::io::stdout().flush();
    let _ = std::io::stderr().flush();
}

pub fn install_panic_hook(logger: AppLogger) {
    std::panic::set_hook(Box::new(move |info| {
        let msg = info
            .payload()
            .downcast_ref::<&str>()
            .map(|s| (*s).to_string())
            .or_else(|| info.payload().downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "panic".to_string());
        let loc = info
            .location()
            .map(|l| format!("{}:{}", l.file(), l.line()))
            .unwrap_or_else(|| "unknown".to_string());
        let full = format!("PANIC at {loc}: {msg}");
        handle_panic_log(&logger, &full);
    }));
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct TempDirGuard(PathBuf);

    impl TempDirGuard {
        fn new(prefix: &str) -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let dir = std::env::temp_dir().join(format!(
                "ps5_agent_logging_test_{prefix}_{}_{}",
                std::process::id(),
                nanos
            ));
            fs::create_dir_all(&dir).expect("create temp dir");
            Self(dir)
        }
    }

    impl Drop for TempDirGuard {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn test_config(log_dir: PathBuf) -> LoggingConfig {
        LoggingConfig {
            level: "info".to_string(),
            log_dir,
            max_file_bytes: 64 * 1024,
            ring_buffer_capacity: 64,
            enable_klog: false,
            enable_toast: false,
        }
    }

    #[test]
    fn parse_strict_and_parse_fallback() {
        assert_eq!(LogLevel::parse_strict("error"), Some(LogLevel::Error));
        assert_eq!(LogLevel::parse_strict("WARN"), Some(LogLevel::Warn));
        assert_eq!(LogLevel::parse_strict(" warning "), Some(LogLevel::Warn));
        assert_eq!(LogLevel::parse_strict("info"), Some(LogLevel::Info));
        assert_eq!(LogLevel::parse_strict("Debug"), Some(LogLevel::Debug));
        assert_eq!(LogLevel::parse_strict("trace"), Some(LogLevel::Trace));

        assert_eq!(LogLevel::parse_strict(""), None);
        assert_eq!(LogLevel::parse_strict("fatal"), None);
        assert_eq!(LogLevel::parse_strict("off"), None);
        assert_eq!(LogLevel::parse("fatal"), LogLevel::Info);
    }

    #[test]
    fn ring_buffer_evicts_oldest_and_respects_filters_and_64kib_cap() {
        let dir = TempDirGuard::new("ring");
        let logger = AppLogger::init(&test_config(dir.0.clone()));

        for i in 0..80 {
            logger.info("unit", format!("msg-{i:03}"));
        }
        // Filtered debug should be ignored at Info level
        logger.debug("unit", "filtered-debug");

        let all = logger.recent(None, 200);
        assert_eq!(all.len(), 64);
        assert_eq!(all.first().unwrap().message, "msg-079");
        assert_eq!(all.last().unwrap().message, "msg-016");
        assert!(logger.recent(None, 0).is_empty());

        // 64 KiB cap (multi-byte safe)
        let huge = "ą".repeat(MAX_LOG_ENTRY_BYTES); // 128 KiB UTF-8
        logger.warn("cap", huge);
        let latest = logger.recent(Some("warn"), 5);
        assert_eq!(latest.len(), 1);
        assert!(latest[0].message.len() <= MAX_LOG_ENTRY_BYTES);
        assert!(latest[0].message.ends_with(TRUNCATED_SUFFIX));
    }

    #[test]
    fn log_rotation_creates_backup_and_recovers_from_rotation_failure() {
        let dir = TempDirGuard::new("rotate");
        let logger = AppLogger::init(&test_config(dir.0.clone()));

        // Two 40 KiB lines exceed 64 KiB max_file_bytes and trigger rotation
        let chunk = "a".repeat(40 * 1024);
        logger.info("rot", &chunk);
        logger.info("rot", &chunk);
        logger.info("rot", "after-rotation");
        logger.flush();

        let active = dir.0.join("ps5-agent.log");
        let rotated = dir.0.join("ps5-agent.log.1");
        assert!(rotated.exists(), "rotated log file .log.1 must exist");
        let active_text = fs::read_to_string(&active).expect("read active log");
        assert!(active_text.contains("after-rotation"));

        // Block rotation by replacing .log.1 with a non-empty directory
        fs::remove_file(&rotated).expect("remove .log.1");
        fs::create_dir_all(rotated.join("blocker")).expect("create blocking dir");
        fs::write(rotated.join("blocker").join("keep"), b"x").expect("seed blocking file");

        logger.info("rot", &chunk);
        logger.info("rot", &chunk);
        logger.info("rot", "after-failed-rotation");
        logger.info("rot", "still-writable-after-failure");
        logger.flush();

        // Must emit throttled warning (at most once within 30s) and NOT drop file handle
        let warns = logger.recent(Some("warn"), 50);
        assert_eq!(
            warns
                .iter()
                .filter(|e| e.target == "logging")
                .count(),
            1,
            "repeated rotation failures must be throttled"
        );
        let post_fail_text = fs::read_to_string(&active).expect("read active log after failure");
        assert!(
            post_fail_text.contains("after-failed-rotation"),
            "file logging must preserve entry from failed rotation"
        );
        assert!(
            post_fail_text.contains("still-writable-after-failure"),
            "file handle must remain active after rotation failure"
        );
    }

    #[test]
    fn apply_config_updates_level_ring_capacity_flags_and_max_bytes() {
        let dir = TempDirGuard::new("apply_cfg");
        let logger = AppLogger::init(&test_config(dir.0.clone()));

        for i in 0..20 {
            logger.info("cfg", format!("entry-{i:02}"));
        }
        assert_eq!(logger.recent(None, 50).len(), 20);

        // Dynamically update to Debug, shrink ring capacity to 5, update flags & file cap
        logger.apply_config(LogLevel::Debug, 128 * 1024, 5, true, true);
        let shrunk = logger.recent(None, 50);
        assert_eq!(shrunk.len(), 5);
        assert_eq!(shrunk[0].message, "entry-19");
        assert_eq!(shrunk[4].message, "entry-15");

        logger.debug("cfg", "debug-enabled");
        let after_dbg = logger.recent(Some("debug"), 10);
        assert_eq!(after_dbg[0].message, "debug-enabled");

        {
            let g = logger.state.lock().unwrap();
            assert_eq!(g.level, LogLevel::Debug);
            assert_eq!(g.capacity, 5);
            assert!(g.enable_klog);
            assert!(g.enable_toast);
        }
        {
            let s = logger.sink.lock().unwrap();
            assert_eq!(s.max_file_bytes, 128 * 1024);
        }
    }

    #[test]
    fn panic_hook_handler_uses_try_lock_without_deadlocking() {
        let dir = TempDirGuard::new("panic_trylock");
        let logger = AppLogger::init(&test_config(dir.0.clone()));

        // Hold both mutexes on the current thread to simulate panicking while locked
        let _state_guard = logger.state.lock().unwrap();
        let _sink_guard = logger.sink.lock().unwrap();
        handle_panic_log(&logger, "PANIC at test: simulated lock-held panic");
        drop(_sink_guard);
        drop(_state_guard);

        // Unlocked path also persists panic record into ring & active log file
        handle_panic_log(&logger, "PANIC at test: unlocked panic");
        let recent = logger.recent(Some("error"), 5);
        assert_eq!(recent[0].target, "panic");
        assert!(recent[0].message.contains("unlocked panic"));
    }

    #[test]
    fn apply_config_shrinks_ring_capacity_and_preserves_latest() {
        let dir = TempDirGuard::new("shrink");
        let mut cfg = test_config(dir.0.clone());
        cfg.ring_buffer_capacity = 4096;
        let logger = AppLogger::init(&cfg);

        for i in 0..2000 {
            logger.info("shrink", format!("m-{i:04}"));
        }
        let cap_before = {
            let g = logger.state.lock().unwrap();
            assert_eq!(g.ring.len(), 2000);
            g.ring.capacity()
        };
        assert!(cap_before >= 4096);

        logger.apply_config(LogLevel::Info, 64 * 1024, 8, false, false);
        let cap_after = {
            let g = logger.state.lock().unwrap();
            assert_eq!(g.ring.len(), 8);
            g.ring.capacity()
        };
        assert!(
            cap_after <= 64 && cap_after < cap_before,
            "ring capacity must shrink after downsizing: before={cap_before}, after={cap_after}"
        );
        let kept = logger.recent(None, 20);
        assert_eq!(kept.len(), 8);
        assert_eq!(kept[0].message, "m-1999");
        assert_eq!(kept[7].message, "m-1992");

        // Also verify pre-allocated unused capacity shrinks when capacity is reduced without eviction
        let dir_sparse = TempDirGuard::new("shrink_sparse");
        let mut cfg_sparse = test_config(dir_sparse.0.clone());
        cfg_sparse.ring_buffer_capacity = 4096;
        let sparse_logger = AppLogger::init(&cfg_sparse);
        sparse_logger.info("shrink", "only-entry");
        sparse_logger.apply_config(LogLevel::Info, 64 * 1024, 64, false, false);
        let sparse_cap = {
            let g = sparse_logger.state.lock().unwrap();
            assert_eq!(g.ring.len(), 1);
            g.ring.capacity()
        };
        assert!(
            sparse_cap <= 64,
            "sparse ring capacity must shrink when ring_capacity drops: cap={sparse_cap}"
        );
    }

    #[test]
    fn log_error_handles_uninitialized_or_initialized_logger_safely() {
        log_error("unit", "safe error diagnostic");
    }
}
