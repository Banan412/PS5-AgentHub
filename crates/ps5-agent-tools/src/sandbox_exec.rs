use chrono::{DateTime, Utc};
use ps5_agent_config::ToolsConfig;
use ps5_agent_core::{AgentError, Result, SandboxMode};
use regex::{Regex, RegexBuilder};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

const DEFAULT_MAX_FILE_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_MAX_EXEC_OUTPUT_BYTES: usize = 262_144;
const MIN_EXEC_OUTPUT_BYTES: usize = 1024;
const TRUNCATED_SUFFIX: &str = "\n...[truncated]";
const SKIP_WALK_DIRS: [&str; 3] = [".git", "target", "node_modules"];
const VIRTUAL_BUILTINS: [&str; 30] = [
    "pwd",
    "ls",
    "dir",
    "cat",
    "type",
    "head",
    "tail",
    "wc",
    "grep",
    "rg",
    "find",
    "echo",
    "mkdir",
    "touch",
    "cp",
    "mv",
    "rm",
    "sed",
    "sort",
    "uniq",
    "cut",
    "tr",
    "sha256sum",
    "json",
    "date",
    "env",
    "which",
    "true",
    "false",
    "cd",
];

#[cfg(not(target_vendor = "sie"))]
const SAFE_HOST_ENV_KEYS: [&str; 5] = ["PATH", "HOME", "TEMP", "TMP", "SYSTEMROOT"];

// ---------------------------------------------------------------------------
// Pure-Rust FIPS 180-4 SHA-256 helper (no external sha2 dependency required)
// ---------------------------------------------------------------------------

mod sha256 {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];

    const INIT_STATE: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];

    fn compress(state: &mut [u32; 8], block: &[u8]) {
        let mut w = [0u32; 64];
        for i in 0..16 {
            let j = i * 4;
            w[i] = u32::from_be_bytes([block[j], block[j + 1], block[j + 2], block[j + 3]]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }

        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = *state;
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let temp1 = h
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(maj);

            h = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }

        let deltas = [a, b, c, d, e, f, g, h];
        for (dst, delta) in state.iter_mut().zip(deltas) {
            *dst = dst.wrapping_add(delta);
        }
    }

    pub fn hex(data: &[u8]) -> String {
        let mut state = INIT_STATE;
        let full_blocks = data.len() / 64;
        for i in 0..full_blocks {
            compress(&mut state, &data[i * 64..(i + 1) * 64]);
        }

        let rem = &data[full_blocks * 64..];
        let mut tail = [0u8; 128];
        tail[..rem.len()].copy_from_slice(rem);
        tail[rem.len()] = 0x80;

        let total_bits = (data.len() as u64).wrapping_mul(8);
        let pad_len = if rem.len() < 56 { 64 } else { 128 };
        tail[pad_len - 8..pad_len].copy_from_slice(&total_bits.to_be_bytes());

        compress(&mut state, &tail[..64]);
        if pad_len == 128 {
            compress(&mut state, &tail[64..128]);
        }

        let mut out = String::with_capacity(64);
        for word in state {
            use std::fmt::Write as _;
            let _ = write!(out, "{word:08x}");
        }
        out
    }
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Executes a shell command inside the sandboxed workspace using either the
/// pure-Rust `"virtual"` interpreter (default, identical across host and PS5)
/// or the strictly gated `"host"` subprocess engine on non-PS5 platforms.
pub async fn exec_sandbox_exec(
    args: &Value,
    cfg: &ToolsConfig,
    sandbox_mode: SandboxMode,
) -> Result<Value> {
    let command = args
        .get("command")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AgentError::InvalidInput("missing required non-empty string 'command'".into()))?
        .to_string();

    let raw_cwd = args
        .get("cwd")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(".")
        .to_string();

    let engine = args
        .get("engine")
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "virtual".to_string());

    match engine.as_str() {
        "virtual" => run_virtual_async(command, raw_cwd, cfg.clone(), sandbox_mode).await,
        "host" => run_host_engine(&command, &raw_cwd, cfg, sandbox_mode).await,
        other => Err(AgentError::InvalidInput(format!(
            "unsupported sandbox_exec engine '{other}'; expected 'virtual' or 'host'"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Bounds, truncation, and workspace path helpers
// ---------------------------------------------------------------------------

fn effective_max_output_bytes(cfg: &ToolsConfig) -> usize {
    if cfg.max_exec_output_bytes == 0 {
        DEFAULT_MAX_EXEC_OUTPUT_BYTES
    } else {
        cfg.max_exec_output_bytes.max(MIN_EXEC_OUTPUT_BYTES)
    }
}

fn effective_max_file_bytes(cfg: &ToolsConfig) -> usize {
    if cfg.max_file_bytes == 0 {
        DEFAULT_MAX_FILE_BYTES
    } else {
        cfg.max_file_bytes
    }
}

fn truncate_utf8_bytes(s: &str, max_bytes: usize) -> (String, bool) {
    if s.len() <= max_bytes {
        return (s.to_string(), false);
    }
    let suffix_len = TRUNCATED_SUFFIX.len();
    if max_bytes <= suffix_len {
        let mut end = max_bytes;
        while end > 0 && !s.is_char_boundary(end) {
            end -= 1;
        }
        return (s[..end].to_string(), true);
    }
    let mut end = max_bytes - suffix_len;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    let mut out = String::with_capacity(end + suffix_len);
    out.push_str(&s[..end]);
    out.push_str(TRUNCATED_SUFFIX);
    (out, true)
}

fn cap_outputs(stdout: String, stderr: String, cap: usize) -> (String, String, bool) {
    let (stdout, out_trunc) = truncate_utf8_bytes(&stdout, cap);
    let (stderr, err_trunc) = truncate_utf8_bytes(&stderr, cap);
    (stdout, stderr, out_trunc || err_trunc)
}

fn normalize_checked_pair(pair: (PathBuf, PathBuf)) -> (PathBuf, PathBuf) {
    let (a, b) = pair;
    if a != b && a.starts_with(&b) {
        (b, a)
    } else {
        (a, b)
    }
}

fn reject_raw_traversal(raw: &str) -> Result<()> {
    if raw.trim().is_empty() {
        return Err(AgentError::InvalidInput("path cannot be empty".into()));
    }
    if raw.contains('\0') {
        return Err(AgentError::InvalidInput(
            "path contains illegal NUL byte".into(),
        ));
    }
    for seg in raw.split(['/', '\\']) {
        if seg == ".." {
            return Err(AgentError::PermissionDenied(
                "path traversal ('..') is forbidden".into(),
            ));
        }
    }
    let mut comps = Path::new(raw).components().peekable();
    while let Some(comp) = comps.next() {
        match comp {
            Component::ParentDir => {
                return Err(AgentError::PermissionDenied(
                    "path traversal ('..') is forbidden".into(),
                ));
            }
            Component::Prefix(_) => {
                if !matches!(comps.peek(), Some(Component::RootDir)) {
                    return Err(AgentError::PermissionDenied(
                        "drive-relative Windows paths are forbidden".into(),
                    ));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn rel_path_forward(path: &Path, root: &Path) -> String {
    let Ok(rel) = path.strip_prefix(root) else {
        return ".".to_string();
    };
    let parts: Vec<String> = rel
        .components()
        .filter_map(|c| match c {
            Component::Normal(seg) => Some(seg.to_string_lossy().into_owned()),
            _ => None,
        })
        .collect();
    if parts.is_empty() {
        ".".to_string()
    } else {
        parts.join("/")
    }
}

fn resolve_root_and_cwd(raw_cwd: &str, cfg: &ToolsConfig) -> Result<(PathBuf, PathBuf)> {
    let (root, cwd) = normalize_checked_pair(crate::files::resolve_workspace_path_checked(raw_cwd, cfg)?);
    if !cwd.exists() {
        return Err(AgentError::NotFound(format!(
            "cwd '{}' does not exist",
            rel_path_forward(&cwd, &root)
        )));
    }
    let meta = fs::symlink_metadata(&cwd).map_err(|e| AgentError::Tool {
        tool_id: "sandbox_exec".into(),
        message: format!("stat cwd failed: {e}"),
    })?;
    if meta.file_type().is_symlink() {
        return Err(AgentError::PermissionDenied(
            "symlinks are forbidden for cwd".into(),
        ));
    }
    if !meta.is_dir() {
        return Err(AgentError::InvalidInput(
            "cwd must point to a directory".into(),
        ));
    }
    Ok((root, cwd))
}

fn resolve_shell_path(raw: &str, root: &Path, cwd: &Path, cfg: &ToolsConfig) -> Result<PathBuf> {
    reject_raw_traversal(raw)?;
    let p = Path::new(raw);
    let lookup = if p.is_absolute() || p.has_root() || raw.starts_with('/') || raw.starts_with('\\') {
        raw.to_string()
    } else {
        let mut segs: Vec<String> = if let Ok(rel) = cwd.strip_prefix(root) {
            rel.components()
                .filter_map(|c| match c {
                    Component::Normal(s) => Some(s.to_string_lossy().into_owned()),
                    _ => None,
                })
                .collect()
        } else {
            Vec::new()
        };
        for part in raw.split(['/', '\\']) {
            if part.is_empty() || part == "." {
                continue;
            }
            segs.push(part.to_string());
        }
        if segs.is_empty() {
            ".".to_string()
        } else {
            segs.join("/")
        }
    };

    let (_resolved_root, target) =
        normalize_checked_pair(crate::files::resolve_workspace_path_checked(&lookup, cfg)?);
    if let Ok(meta) = fs::symlink_metadata(&target) {
        if meta.file_type().is_symlink() {
            return Err(AgentError::PermissionDenied(format!(
                "symlinks are forbidden inside sandbox: '{}'",
                target.display()
            )));
        }
    }
    Ok(target)
}

fn read_bounded_file(path: &Path, cfg: &ToolsConfig) -> std::result::Result<Vec<u8>, String> {
    let max_bytes = effective_max_file_bytes(cfg);
    let meta = fs::symlink_metadata(path).map_err(|e| format!("{}: {e}", path.display()))?;
    if meta.file_type().is_symlink() {
        return Err(format!("{}: symlinks are forbidden", path.display()));
    }
    if meta.is_dir() {
        return Err(format!("{}: is a directory", path.display()));
    }
    if meta.len() as usize > max_bytes {
        return Err(format!(
            "{}: file size {} exceeds limit {}",
            path.display(),
            meta.len(),
            max_bytes
        ));
    }
    fs::read(path).map_err(|e| format!("{}: {e}", path.display()))
}

fn validate_no_symlinks_tree(path: &Path) -> Result<()> {
    let meta = fs::symlink_metadata(path).map_err(|e| AgentError::Tool {
        tool_id: "sandbox_exec".into(),
        message: format!("stat '{}' failed: {e}", path.display()),
    })?;
    if meta.file_type().is_symlink() {
        return Err(AgentError::PermissionDenied(format!(
            "symlinks are forbidden inside sandbox: '{}'",
            path.display()
        )));
    }
    if meta.is_dir() {
        let rd = fs::read_dir(path).map_err(|e| AgentError::Tool {
            tool_id: "sandbox_exec".into(),
            message: format!("read_dir '{}' failed: {e}", path.display()),
        })?;
        for entry_res in rd {
            let entry = entry_res.map_err(|e| AgentError::Tool {
                tool_id: "sandbox_exec".into(),
                message: format!("read_dir entry failed: {e}"),
            })?;
            validate_no_symlinks_tree(&entry.path())?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Shell Lexer & Parser
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    Word(String),
    Semi,
    AndIf,
    OrIf,
    Pipe,
    RedirIn,
    RedirOut,
    RedirAppend,
}

#[derive(Debug, Clone)]
struct Redirect {
    kind: RedirectKind,
    target: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RedirectKind {
    Input,
    Truncate,
    Append,
}

#[derive(Debug, Clone)]
struct SimpleCommand {
    argv: Vec<String>,
    redirects: Vec<Redirect>,
}

#[derive(Debug, Clone)]
struct Pipeline {
    stages: Vec<SimpleCommand>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChainOp {
    And,
    Or,
}

#[derive(Debug, Clone)]
struct ConditionalList {
    first: Pipeline,
    rest: Vec<(ChainOp, Pipeline)>,
}

fn tokenize_command(input: &str, env: &BTreeMap<String, String>) -> Result<Vec<Token>> {
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_word = false;

    let flush_word = |tokens: &mut Vec<Token>, current: &mut String, in_word: &mut bool| {
        if *in_word {
            tokens.push(Token::Word(std::mem::take(current)));
            *in_word = false;
        }
    };

    while i < chars.len() {
        let ch = chars[i];
        match ch {
            ' ' | '\t' | '\r' => {
                flush_word(&mut tokens, &mut current, &mut in_word);
                i += 1;
            }
            '\n' | ';' => {
                flush_word(&mut tokens, &mut current, &mut in_word);
                if !matches!(tokens.last(), Some(Token::Semi) | None) {
                    tokens.push(Token::Semi);
                }
                i += 1;
            }
            '#' if !in_word => {
                while i < chars.len() && chars[i] != '\n' {
                    i += 1;
                }
            }
            '&' => {
                flush_word(&mut tokens, &mut current, &mut in_word);
                if i + 1 < chars.len() && chars[i + 1] == '&' {
                    tokens.push(Token::AndIf);
                    i += 2;
                } else {
                    return Err(AgentError::InvalidInput(
                        "background '&' operator is not supported in virtual shell; use ';' or '&&'"
                            .into(),
                    ));
                }
            }
            '|' => {
                flush_word(&mut tokens, &mut current, &mut in_word);
                if i + 1 < chars.len() && chars[i + 1] == '|' {
                    tokens.push(Token::OrIf);
                    i += 2;
                } else {
                    tokens.push(Token::Pipe);
                    i += 1;
                }
            }
            '>' => {
                flush_word(&mut tokens, &mut current, &mut in_word);
                if i + 1 < chars.len() && chars[i + 1] == '>' {
                    tokens.push(Token::RedirAppend);
                    i += 2;
                } else {
                    tokens.push(Token::RedirOut);
                    i += 1;
                }
            }
            '<' => {
                flush_word(&mut tokens, &mut current, &mut in_word);
                tokens.push(Token::RedirIn);
                i += 1;
            }
            '\'' => {
                in_word = true;
                i += 1;
                let mut closed = false;
                while i < chars.len() {
                    if chars[i] == '\'' {
                        closed = true;
                        i += 1;
                        break;
                    }
                    current.push(chars[i]);
                    i += 1;
                }
                if !closed {
                    return Err(AgentError::InvalidInput("unclosed single quote".into()));
                }
            }
            '"' => {
                in_word = true;
                i += 1;
                let mut closed = false;
                while i < chars.len() {
                    let c = chars[i];
                    if c == '"' {
                        closed = true;
                        i += 1;
                        break;
                    }
                    if c == '\\' && i + 1 < chars.len() {
                        let next = chars[i + 1];
                        match next {
                            '"' | '\\' | '$' => {
                                current.push(next);
                                i += 2;
                            }
                            _ => {
                                current.push('\\');
                                current.push(next);
                                i += 2;
                            }
                        }
                        continue;
                    }
                    if c == '$' {
                        let (val, consumed) = expand_var(&chars[i..], env);
                        current.push_str(&val);
                        i += consumed;
                        continue;
                    }
                    current.push(c);
                    i += 1;
                }
                if !closed {
                    return Err(AgentError::InvalidInput("unclosed double quote".into()));
                }
            }
            '\\' => {
                in_word = true;
                if i + 1 < chars.len() {
                    current.push(chars[i + 1]);
                    i += 2;
                } else {
                    current.push('\\');
                    i += 1;
                }
            }
            '$' => {
                in_word = true;
                let (val, consumed) = expand_var(&chars[i..], env);
                current.push_str(&val);
                i += consumed;
            }
            other => {
                in_word = true;
                current.push(other);
                i += 1;
            }
        }
    }

    flush_word(&mut tokens, &mut current, &mut in_word);
    if matches!(tokens.last(), Some(Token::Semi)) {
        tokens.pop();
    }
    Ok(tokens)
}

fn expand_var(slice: &[char], env: &BTreeMap<String, String>) -> (String, usize) {
    if slice.len() < 2 {
        return ("$".to_string(), 1);
    }
    if slice[1] == '{' {
        let mut end = 2;
        while end < slice.len() && slice[end] != '}' {
            end += 1;
        }
        if end < slice.len() && end > 2 {
            let name: String = slice[2..end].iter().collect();
            return (env.get(&name).cloned().unwrap_or_default(), end + 1);
        }
        return ("$".to_string(), 1);
    }
    let mut end = 1;
    while end < slice.len() && (slice[end].is_ascii_alphanumeric() || slice[end] == '_') {
        end += 1;
    }
    if end == 1 {
        return ("$".to_string(), 1);
    }
    let name: String = slice[1..end].iter().collect();
    (env.get(&name).cloned().unwrap_or_default(), end)
}

fn parse_commands(tokens: &[Token]) -> Result<Vec<ConditionalList>> {
    if tokens.is_empty() {
        return Ok(Vec::new());
    }
    let mut lists = Vec::new();
    let mut idx = 0;

    while idx < tokens.len() {
        if matches!(tokens[idx], Token::Semi) {
            idx += 1;
            continue;
        }
        let first = parse_pipeline(tokens, &mut idx)?;
        let mut rest = Vec::new();
        while idx < tokens.len() && matches!(tokens[idx], Token::AndIf | Token::OrIf) {
            let op = match tokens[idx] {
                Token::AndIf => ChainOp::And,
                Token::OrIf => ChainOp::Or,
                _ => unreachable!(),
            };
            idx += 1;
            let next_pipe = parse_pipeline(tokens, &mut idx)?;
            rest.push((op, next_pipe));
        }
        lists.push(ConditionalList { first, rest });
        if idx < tokens.len() {
            if matches!(tokens[idx], Token::Semi) {
                idx += 1;
            } else {
                return Err(AgentError::InvalidInput("unexpected shell token".into()));
            }
        }
    }

    Ok(lists)
}

fn parse_pipeline(tokens: &[Token], idx: &mut usize) -> Result<Pipeline> {
    let mut stages = Vec::new();
    stages.push(parse_simple_command(tokens, idx)?);
    while *idx < tokens.len() && matches!(tokens[*idx], Token::Pipe) {
        *idx += 1;
        stages.push(parse_simple_command(tokens, idx)?);
    }
    Ok(Pipeline { stages })
}

fn parse_simple_command(tokens: &[Token], idx: &mut usize) -> Result<SimpleCommand> {
    let mut argv = Vec::new();
    let mut redirects = Vec::new();

    while *idx < tokens.len() {
        match &tokens[*idx] {
            Token::Word(w) => {
                argv.push(w.clone());
                *idx += 1;
            }
            Token::RedirIn | Token::RedirOut | Token::RedirAppend => {
                let kind = match tokens[*idx] {
                    Token::RedirIn => RedirectKind::Input,
                    Token::RedirOut => RedirectKind::Truncate,
                    Token::RedirAppend => RedirectKind::Append,
                    _ => unreachable!(),
                };
                *idx += 1;
                match tokens.get(*idx) {
                    Some(Token::Word(target)) => {
                        redirects.push(Redirect {
                            kind,
                            target: target.clone(),
                        });
                        *idx += 1;
                    }
                    _ => {
                        return Err(AgentError::InvalidInput(
                            "expected target filename after redirection operator".into(),
                        ));
                    }
                }
            }
            Token::Pipe | Token::Semi | Token::AndIf | Token::OrIf => break,
        }
    }

    if argv.is_empty() {
        return Err(AgentError::InvalidInput(
            "expected command name in shell expression".into(),
        ));
    }

    Ok(SimpleCommand { argv, redirects })
}

// ---------------------------------------------------------------------------
// Virtual Engine Execution
// ---------------------------------------------------------------------------

struct ShellState<'a> {
    root: PathBuf,
    cwd: PathBuf,
    cfg: &'a ToolsConfig,
    sandbox_mode: SandboxMode,
    env: BTreeMap<String, String>,
}

struct CmdOutcome {
    exit_code: i32,
    stdout: String,
    stderr: String,
}

impl CmdOutcome {
    fn ok(stdout: String) -> Self {
        Self {
            exit_code: 0,
            stdout,
            stderr: String::new(),
        }
    }

    fn err(code: i32, stderr: String) -> Self {
        Self {
            exit_code: code,
            stdout: String::new(),
            stderr,
        }
    }
}

async fn run_virtual_async(
    command: String,
    raw_cwd: String,
    cfg: ToolsConfig,
    sandbox_mode: SandboxMode,
) -> Result<Value> {
    let timeout_secs = cfg.exec_timeout_secs;
    let join = tokio::task::spawn_blocking(move || run_virtual_blocking(&command, &raw_cwd, &cfg, sandbox_mode));

    let res = if timeout_secs > 0 {
        match tokio::time::timeout(Duration::from_secs(timeout_secs), join).await {
            Ok(joined) => joined.map_err(|e| AgentError::Internal(format!("virtual shell join error: {e}")))?,
            Err(_) => {
                return Err(AgentError::Timeout(format!(
                    "virtual sandbox_exec timed out after {timeout_secs}s"
                )));
            }
        }
    } else {
        join.await
            .map_err(|e| AgentError::Internal(format!("virtual shell join error: {e}")))?
    };

    res
}

fn sandbox_mode_label(mode: SandboxMode) -> &'static str {
    match mode {
        SandboxMode::ReadOnly => "read_only",
        SandboxMode::WorkspaceWrite => "workspace_write",
        SandboxMode::FullSandbox => "full_sandbox",
    }
}

fn build_virtual_env(cwd_rel: &str, mode: SandboxMode) -> BTreeMap<String, String> {
    let mut env = BTreeMap::new();
    env.insert("ENGINE".into(), "virtual".into());
    env.insert("HOME".into(), "/workspace".into());
    env.insert("PATH".into(), "/virtual/bin".into());
    env.insert("PWD".into(), cwd_rel.to_string());
    env.insert("SANDBOX_MODE".into(), sandbox_mode_label(mode).into());
    env.insert("WORKSPACE".into(), "/workspace".into());
    env
}

fn run_virtual_blocking(
    command: &str,
    raw_cwd: &str,
    cfg: &ToolsConfig,
    sandbox_mode: SandboxMode,
) -> Result<Value> {
    let (root, cwd) = resolve_root_and_cwd(raw_cwd, cfg)?;
    let cwd_rel = rel_path_forward(&cwd, &root);
    let env = build_virtual_env(&cwd_rel, sandbox_mode);
    let tokens = tokenize_command(command, &env)?;
    let lists = parse_commands(&tokens)?;
    if lists.is_empty() {
        return Err(AgentError::InvalidInput("command cannot be empty".into()));
    }

    let mut state = ShellState {
        root,
        cwd,
        cfg,
        sandbox_mode,
        env,
    };

    let mut total_stdout = String::new();
    let mut total_stderr = String::new();
    let mut last_code = 0;

    for list in lists {
        let mut outcome = execute_pipeline(&list.first, &mut state)?;
        total_stdout.push_str(&outcome.stdout);
        total_stderr.push_str(&outcome.stderr);
        last_code = outcome.exit_code;

        for (op, pipeline) in list.rest {
            let should_run = match op {
                ChainOp::And => last_code == 0,
                ChainOp::Or => last_code != 0,
            };
            if should_run {
                outcome = execute_pipeline(&pipeline, &mut state)?;
                total_stdout.push_str(&outcome.stdout);
                total_stderr.push_str(&outcome.stderr);
                last_code = outcome.exit_code;
            }
        }
    }

    let cap = effective_max_output_bytes(cfg);
    let (stdout, stderr, truncated) = cap_outputs(total_stdout, total_stderr, cap);
    let final_cwd = rel_path_forward(&state.cwd, &state.root);

    Ok(json!({
        "engine": "virtual",
        "exit_code": last_code,
        "stdout": stdout,
        "stderr": stderr,
        "truncated": truncated,
        "cwd": final_cwd,
    }))
}

fn execute_pipeline(pipeline: &Pipeline, state: &mut ShellState<'_>) -> Result<CmdOutcome> {
    let mut pipe_stdin = String::new();
    let mut collected_stderr = String::new();
    let mut final_stdout = String::new();
    let mut last_exit = 0;

    let stage_count = pipeline.stages.len();
    for (idx, cmd) in pipeline.stages.iter().enumerate() {
        // Pre-validate ReadOnly for output redirects BEFORE running command
        for redir in &cmd.redirects {
            if matches!(redir.kind, RedirectKind::Truncate | RedirectKind::Append)
                && state.sandbox_mode == SandboxMode::ReadOnly
            {
                return Err(AgentError::PermissionDenied(
                    "sandbox_mode is read_only; output redirection ('>', '>>') is disabled"
                        .into(),
                ));
            }
        }

        let mut stage_input = if idx == 0 {
            String::new()
        } else {
            std::mem::take(&mut pipe_stdin)
        };

        // Handle input redirection '<'
        for redir in &cmd.redirects {
            if redir.kind == RedirectKind::Input {
                let target = resolve_shell_path(&redir.target, &state.root, &state.cwd, state.cfg)?;
                match read_bounded_file(&target, state.cfg) {
                    Ok(bytes) => {
                        stage_input = String::from_utf8_lossy(&bytes).into_owned();
                    }
                    Err(msg) => {
                        collected_stderr.push_str(&format!("{}: {msg}\n", cmd.argv[0]));
                        last_exit = 1;
                        return Ok(CmdOutcome {
                            exit_code: last_exit,
                            stdout: String::new(),
                            stderr: collected_stderr,
                        });
                    }
                }
            }
        }

        let outcome = dispatch_builtin(&cmd.argv, &stage_input, state)?;
        collected_stderr.push_str(&outcome.stderr);
        last_exit = outcome.exit_code;

        let mut has_output_redir = false;
        for redir in &cmd.redirects {
            if matches!(redir.kind, RedirectKind::Truncate | RedirectKind::Append) {
                has_output_redir = true;
                let target = resolve_shell_path(&redir.target, &state.root, &state.cwd, state.cfg)?;
                if let Some(parent) = target.parent() {
                    if !parent.exists() {
                        collected_stderr.push_str(&format!(
                            "redirect: parent directory does not exist: '{}'\n",
                            rel_path_forward(parent, &state.root)
                        ));
                        last_exit = 1;
                        continue;
                    }
                }
                if let Err(msg) = write_redirect_target(
                    &target,
                    outcome.stdout.as_bytes(),
                    redir.kind == RedirectKind::Append,
                    state.cfg,
                ) {
                    collected_stderr.push_str(&format!("redirect: {msg}\n"));
                    last_exit = 1;
                }
            }
        }

        let forwarded = if has_output_redir {
            String::new()
        } else {
            outcome.stdout
        };

        if idx + 1 < stage_count {
            pipe_stdin = forwarded;
        } else {
            final_stdout = forwarded;
        }
    }

    Ok(CmdOutcome {
        exit_code: last_exit,
        stdout: final_stdout,
        stderr: collected_stderr,
    })
}

fn write_redirect_target(
    target: &Path,
    data: &[u8],
    append: bool,
    cfg: &ToolsConfig,
) -> std::result::Result<(), String> {
    let max_bytes = effective_max_file_bytes(cfg);
    if target.is_dir() {
        return Err(format!("'{}' is a directory", target.display()));
    }
    let existing_len = if append && target.exists() {
        fs::symlink_metadata(target)
            .map_err(|e| format!("stat failed: {e}"))?
            .len() as usize
    } else {
        0
    };
    if existing_len.saturating_add(data.len()) > max_bytes {
        return Err(format!(
            "redirect output exceeds max_file_bytes ({max_bytes} bytes)"
        ));
    }

    let mut file = if append {
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(target)
            .map_err(|e| format!("open failed: {e}"))?
    } else {
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(target)
            .map_err(|e| format!("open failed: {e}"))?
    };
    file.write_all(data)
        .map_err(|e| format!("write failed: {e}"))
}

fn dispatch_builtin(
    argv: &[String],
    stdin_str: &str,
    state: &mut ShellState<'_>,
) -> Result<CmdOutcome> {
    let name = argv[0].as_str();
    let args = &argv[1..];

    if state.sandbox_mode == SandboxMode::ReadOnly
        && matches!(name, "mkdir" | "touch" | "cp" | "mv" | "rm")
    {
        return Err(AgentError::PermissionDenied(format!(
            "sandbox_mode is read_only; mutating command '{name}' is disabled"
        )));
    }

    match name {
        "pwd" => {
            let mut out = rel_path_forward(&state.cwd, &state.root);
            out.push('\n');
            Ok(CmdOutcome::ok(out))
        }
        "cd" => cmd_cd(args, state),
        "ls" | "dir" => cmd_ls(args, state),
        "cat" | "type" => cmd_cat(args, stdin_str, state),
        "head" => cmd_head_tail(args, stdin_str, state, true),
        "tail" => cmd_head_tail(args, stdin_str, state, false),
        "wc" => cmd_wc(args, stdin_str, state),
        "grep" | "rg" => cmd_grep(name == "rg", args, stdin_str, state),
        "find" => cmd_find(args, state),
        "echo" => cmd_echo(args),
        "mkdir" => cmd_mkdir(args, state),
        "touch" => cmd_touch(args, state),
        "cp" => cmd_cp(args, state),
        "mv" => cmd_mv(args, state),
        "rm" => cmd_rm(args, state),
        "sed" => cmd_sed(args, stdin_str, state),
        "sort" => cmd_sort(args, stdin_str, state),
        "uniq" => cmd_uniq(args, stdin_str, state),
        "cut" => cmd_cut(args, stdin_str, state),
        "tr" => cmd_tr(args, stdin_str),
        "sha256sum" => cmd_sha256sum(args, stdin_str, state),
        "json" => cmd_json(args, stdin_str, state),
        "date" => cmd_date(args),
        "env" => cmd_env(state),
        "which" => cmd_which(args),
        "true" => Ok(CmdOutcome::ok(String::new())),
        "false" => Ok(CmdOutcome::err(1, String::new())),
        other => Ok(CmdOutcome::err(
            127,
            format!("virtual shell: command not found: {other}\n"),
        )),
    }
}

// ---------------------------------------------------------------------------
// Built-in Command Implementations
// ---------------------------------------------------------------------------

fn cmd_cd(args: &[String], state: &mut ShellState<'_>) -> Result<CmdOutcome> {
    if args.len() > 1 {
        return Ok(CmdOutcome::err(1, "cd: too many arguments\n".into()));
    }
    let target = match args.first() {
        None => state.root.clone(),
        Some(raw) => resolve_shell_path(raw, &state.root, &state.cwd, state.cfg)?,
    };
    if !target.exists() {
        return Ok(CmdOutcome::err(
            1,
            format!("cd: '{}': no such directory\n", rel_path_forward(&target, &state.root)),
        ));
    }
    if !target.is_dir() {
        return Ok(CmdOutcome::err(
            1,
            format!("cd: '{}': not a directory\n", rel_path_forward(&target, &state.root)),
        ));
    }
    state.cwd = target;
    let rel = rel_path_forward(&state.cwd, &state.root);
    state.env.insert("PWD".into(), rel);
    Ok(CmdOutcome::ok(String::new()))
}

fn cmd_ls(args: &[String], state: &ShellState<'_>) -> Result<CmdOutcome> {
    let mut long = false;
    let mut all = false;
    let mut paths = Vec::new();
    let mut stop_flags = false;

    for arg in args {
        if !stop_flags && arg == "--" {
            stop_flags = true;
            continue;
        }
        if !stop_flags && arg.starts_with('-') && arg.len() > 1 {
            for c in arg[1..].chars() {
                match c {
                    'l' => long = true,
                    'a' => all = true,
                    other => {
                        return Ok(CmdOutcome::err(
                            2,
                            format!("ls: unsupported option '-{other}'\n"),
                        ));
                    }
                }
            }
        } else {
            paths.push(arg.as_str());
        }
    }
    if paths.is_empty() {
        paths.push(".");
    }

    let mut out = String::new();
    let mut err = String::new();
    let mut code = 0;

    for (idx, raw) in paths.iter().enumerate() {
        let target = resolve_shell_path(raw, &state.root, &state.cwd, state.cfg)?;
        let Ok(meta) = fs::symlink_metadata(&target) else {
            err.push_str(&format!("ls: '{raw}': no such file or directory\n"));
            code = 1;
            continue;
        };
        if meta.file_type().is_symlink() {
            return Err(AgentError::PermissionDenied(
                "symlinks are forbidden inside sandbox".into(),
            ));
        }
        if paths.len() > 1 && meta.is_dir() {
            if idx > 0 {
                out.push('\n');
            }
            let _ = writeln!(out, "{raw}:");
        }
        if meta.is_file() {
            format_ls_entry(&mut out, raw, &meta, long);
            continue;
        }
        let Ok(rd) = fs::read_dir(&target) else {
            err.push_str(&format!("ls: '{raw}': cannot read directory\n"));
            code = 1;
            continue;
        };
        let mut entries: Vec<(String, fs::Metadata)> = Vec::new();
        for item in rd.flatten() {
            let name = item.file_name().to_string_lossy().into_owned();
            if !all && name.starts_with('.') {
                continue;
            }
            if let Ok(m) = fs::symlink_metadata(item.path()) {
                if !m.file_type().is_symlink() {
                    entries.push((name, m));
                }
            }
        }
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        for (name, m) in entries {
            format_ls_entry(&mut out, &name, &m, long);
        }
    }

    Ok(CmdOutcome {
        exit_code: code,
        stdout: out,
        stderr: err,
    })
}

fn format_ls_entry(out: &mut String, name: &str, meta: &fs::Metadata, long: bool) {
    if !long {
        let _ = writeln!(out, "{name}");
        return;
    }
    let kind = if meta.is_dir() { 'd' } else { '-' };
    let size = if meta.is_dir() { 0 } else { meta.len() };
    let ts = meta
        .modified()
        .ok()
        .map(|st| DateTime::<Utc>::from(st).format("%Y-%m-%dT%H:%M:%SZ").to_string())
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_string());
    let _ = writeln!(out, "{kind} {size:>10} {ts} {name}");
}

fn collect_text_inputs(
    files: &[String],
    stdin_str: &str,
    state: &ShellState<'_>,
) -> Result<(Vec<(String, String)>, String, i32)> {
    if files.is_empty() {
        return Ok((vec![("-".to_string(), stdin_str.to_string())], String::new(), 0));
    }
    let mut out = Vec::new();
    let mut err = String::new();
    let mut code = 0;

    for raw in files {
        if raw == "-" {
            out.push(("-".to_string(), stdin_str.to_string()));
            continue;
        }
        let target = resolve_shell_path(raw, &state.root, &state.cwd, state.cfg)?;
        match read_bounded_file(&target, state.cfg) {
            Ok(bytes) => {
                out.push((raw.clone(), String::from_utf8_lossy(&bytes).into_owned()));
            }
            Err(msg) => {
                err.push_str(&format!("{msg}\n"));
                code = 1;
            }
        }
    }
    Ok((out, err, code))
}

fn cmd_cat(args: &[String], stdin_str: &str, state: &ShellState<'_>) -> Result<CmdOutcome> {
    let files: Vec<String> = args
        .iter()
        .filter(|a| a.as_str() != "--")
        .cloned()
        .collect();
    let (inputs, err, code) = collect_text_inputs(&files, stdin_str, state)?;
    let mut out = String::new();
    for (_, text) in inputs {
        out.push_str(&text);
    }
    Ok(CmdOutcome {
        exit_code: code,
        stdout: out,
        stderr: err,
    })
}

fn cmd_head_tail(
    args: &[String],
    stdin_str: &str,
    state: &ShellState<'_>,
    is_head: bool,
) -> Result<CmdOutcome> {
    let cmd_name = if is_head { "head" } else { "tail" };
    let mut count: usize = 10;
    let mut files = Vec::new();
    let mut i = 0;
    let mut stop_flags = false;

    while i < args.len() {
        let arg = &args[i];
        if !stop_flags && arg == "--" {
            stop_flags = true;
            i += 1;
            continue;
        }
        if !stop_flags && arg == "-n" {
            let Some(val) = args.get(i + 1) else {
                return Ok(CmdOutcome::err(
                    2,
                    format!("{cmd_name}: option '-n' requires an integer argument\n"),
                ));
            };
            let Ok(n) = val.parse::<usize>() else {
                return Ok(CmdOutcome::err(
                    2,
                    format!("{cmd_name}: invalid line count '{val}'\n"),
                ));
            };
            count = n;
            i += 2;
            continue;
        }
        if !stop_flags && arg.starts_with("-n") && arg.len() > 2 {
            let Ok(n) = arg[2..].parse::<usize>() else {
                return Ok(CmdOutcome::err(
                    2,
                    format!("{cmd_name}: invalid line count '{}'\n", &arg[2..]),
                ));
            };
            count = n;
            i += 1;
            continue;
        }
        if !stop_flags && arg.starts_with('-') && arg.len() > 1 && arg[1..].chars().all(|c| c.is_ascii_digit()) {
            let Ok(n) = arg[1..].parse::<usize>() else {
                return Ok(CmdOutcome::err(
                    2,
                    format!("{cmd_name}: invalid line count '{arg}'\n"),
                ));
            };
            count = n;
            i += 1;
            continue;
        }
        if !stop_flags && arg.starts_with('-') && arg.len() > 1 {
            return Ok(CmdOutcome::err(
                2,
                format!("{cmd_name}: unsupported option '{arg}'\n"),
            ));
        }
        files.push(arg.clone());
        i += 1;
    }

    let (inputs, err, code) = collect_text_inputs(&files, stdin_str, state)?;
    let multiple = inputs.len() > 1;
    let mut out = String::new();

    for (idx, (label, text)) in inputs.iter().enumerate() {
        if multiple {
            if idx > 0 {
                out.push('\n');
            }
            let _ = writeln!(out, "==> {label} <==");
        }
        let lines: Vec<&str> = text.lines().collect();
        let selected: &[&str] = if count == 0 {
            &[]
        } else if is_head {
            let end = count.min(lines.len());
            &lines[..end]
        } else {
            let start = lines.len().saturating_sub(count);
            &lines[start..]
        };
        for line in selected {
            out.push_str(line);
            out.push('\n');
        }
    }

    Ok(CmdOutcome {
        exit_code: code,
        stdout: out,
        stderr: err,
    })
}

fn cmd_wc(args: &[String], stdin_str: &str, state: &ShellState<'_>) -> Result<CmdOutcome> {
    let mut show_l = false;
    let mut show_w = false;
    let mut show_c = false;
    let mut files = Vec::new();
    let mut stop_flags = false;

    for arg in args {
        if !stop_flags && arg == "--" {
            stop_flags = true;
            continue;
        }
        if !stop_flags && arg.starts_with('-') && arg.len() > 1 {
            for c in arg[1..].chars() {
                match c {
                    'l' => show_l = true,
                    'w' => show_w = true,
                    'c' => show_c = true,
                    other => {
                        return Ok(CmdOutcome::err(
                            2,
                            format!("wc: unsupported option '-{other}'\n"),
                        ));
                    }
                }
            }
        } else {
            files.push(arg.clone());
        }
    }

    if !show_l && !show_w && !show_c {
        show_l = true;
        show_w = true;
        show_c = true;
    }

    let has_file_args = !files.is_empty();
    let (inputs, err, code) = collect_text_inputs(&files, stdin_str, state)?;
    let mut out = String::new();
    let (mut tot_l, mut tot_w, mut tot_c) = (0usize, 0usize, 0usize);

    let format_wc_row = |out: &mut String, l: usize, w: usize, c: usize, label: Option<&str>| {
        let mut cols = Vec::new();
        if show_l {
            cols.push(l.to_string());
        }
        if show_w {
            cols.push(w.to_string());
        }
        if show_c {
            cols.push(c.to_string());
        }
        out.push_str(&cols.join(" "));
        if let Some(name) = label {
            let _ = write!(out, " {name}");
        }
        out.push('\n');
    };

    for (label, text) in &inputs {
        let l = text.as_bytes().iter().filter(|&&b| b == b'\n').count();
        let w = text.split_whitespace().count();
        let c = text.len();
        tot_l += l;
        tot_w += w;
        tot_c += c;
        let display_label = if has_file_args && label != "-" {
            Some(label.as_str())
        } else {
            None
        };
        format_wc_row(&mut out, l, w, c, display_label);
    }

    if inputs.len() > 1 {
        format_wc_row(&mut out, tot_l, tot_w, tot_c, Some("total"));
    }

    Ok(CmdOutcome {
        exit_code: code,
        stdout: out,
        stderr: err,
    })
}

fn cmd_grep(
    is_rg: bool,
    args: &[String],
    stdin_str: &str,
    state: &ShellState<'_>,
) -> Result<CmdOutcome> {
    let mut ignore_case = false;
    let mut line_numbers = false;
    let mut invert = false;
    let mut fixed = false;
    let mut recursive = false;
    let mut count_only = false;
    let mut positionals = Vec::new();
    let mut stop_flags = false;

    for arg in args {
        if !stop_flags && arg == "--" {
            stop_flags = true;
            continue;
        }
        if !stop_flags && arg.starts_with('-') && arg.len() > 1 {
            for c in arg[1..].chars() {
                match c {
                    'i' => ignore_case = true,
                    'n' => line_numbers = true,
                    'v' => invert = true,
                    'F' => fixed = true,
                    'E' => fixed = false,
                    'r' | 'R' => recursive = true,
                    'c' => count_only = true,
                    other => {
                        return Ok(CmdOutcome::err(
                            2,
                            format!("grep: unsupported option '-{other}'\n"),
                        ));
                    }
                }
            }
        } else {
            positionals.push(arg.clone());
        }
    }

    if positionals.is_empty() {
        return Ok(CmdOutcome::err(2, "grep: missing search pattern\n".into()));
    }
    let pattern = &positionals[0];
    let regex_src = if fixed {
        regex::escape(pattern)
    } else {
        pattern.clone()
    };
    let regex = match RegexBuilder::new(&regex_src).case_insensitive(ignore_case).build() {
        Ok(r) => r,
        Err(e) => return Ok(CmdOutcome::err(2, format!("grep: invalid pattern: {e}\n"))),
    };

    let targets: Vec<String> = if positionals.len() > 1 {
        positionals[1..].to_vec()
    } else if recursive || (is_rg && stdin_str.is_empty()) {
        recursive = true;
        vec![".".to_string()]
    } else {
        Vec::new()
    };

    if targets.is_empty() {
        let mut matched_any = false;
        let mut count = 0usize;
        let mut out = String::new();
        for (idx, line) in stdin_str.lines().enumerate() {
            let is_match = regex.is_match(line) ^ invert;
            if is_match {
                matched_any = true;
                count += 1;
                if !count_only {
                    if line_numbers {
                        let _ = writeln!(out, "{}:{line}", idx + 1);
                    } else {
                        let _ = writeln!(out, "{line}");
                    }
                }
            }
        }
        if count_only {
            let _ = writeln!(out, "{count}");
        }
        return Ok(CmdOutcome {
            exit_code: if matched_any { 0 } else { 1 },
            stdout: out,
            stderr: String::new(),
        });
    }

    let mut file_entries: Vec<(String, PathBuf)> = Vec::new();
    let mut err = String::new();
    let mut had_error = false;
    let mut multi_source = targets.len() > 1 || recursive;

    for raw in &targets {
        let target = resolve_shell_path(raw, &state.root, &state.cwd, state.cfg)?;
        let Ok(meta) = fs::symlink_metadata(&target) else {
            err.push_str(&format!("grep: '{raw}': no such file or directory\n"));
            had_error = true;
            continue;
        };
        if meta.file_type().is_symlink() {
            return Err(AgentError::PermissionDenied(
                "symlinks are forbidden inside sandbox".into(),
            ));
        }
        if meta.is_dir() {
            if recursive || is_rg {
                multi_source = true;
                walk_files_sorted(&target, &state.root, &mut file_entries)?;
            } else {
                err.push_str(&format!("grep: '{raw}': is a directory\n"));
                had_error = true;
            }
        } else {
            let label = rel_path_forward(&target, &state.root);
            file_entries.push((label, target));
        }
    }

    if file_entries.len() > 1 {
        multi_source = true;
    }

    let mut out = String::new();
    let mut matched_any = false;
    for (label, path) in file_entries {
        let bytes = match read_bounded_file(&path, state.cfg) {
            Ok(b) => b,
            Err(_) => continue,
        };
        if bytes.contains(&0) {
            continue;
        }
        let text = String::from_utf8_lossy(&bytes);
        let mut count = 0usize;
        for (idx, line) in text.lines().enumerate() {
            let is_match = regex.is_match(line) ^ invert;
            if is_match {
                matched_any = true;
                count += 1;
                if !count_only {
                    if multi_source {
                        let _ = write!(out, "{label}:");
                    }
                    if line_numbers {
                        let _ = write!(out, "{}:", idx + 1);
                    }
                    let _ = writeln!(out, "{line}");
                }
            }
        }
        if count_only && (count > 0 || !multi_source) {
            if multi_source {
                let _ = writeln!(out, "{label}:{count}");
            } else {
                let _ = writeln!(out, "{count}");
            }
        }
    }

    let exit_code = if had_error && !matched_any {
        2
    } else if matched_any {
        0
    } else {
        1
    };

    Ok(CmdOutcome {
        exit_code,
        stdout: out,
        stderr: err,
    })
}

fn walk_files_sorted(
    dir: &Path,
    root: &Path,
    out: &mut Vec<(String, PathBuf)>,
) -> Result<()> {
    let Ok(rd) = fs::read_dir(dir) else {
        return Ok(());
    };
    let mut children: Vec<PathBuf> = rd.flatten().map(|e| e.path()).collect();
    children.sort();
    for path in children {
        let Ok(meta) = fs::symlink_metadata(&path) else {
            continue;
        };
        if meta.file_type().is_symlink() {
            continue;
        }
        if meta.is_dir() {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if SKIP_WALK_DIRS.contains(&name) {
                    continue;
                }
            }
            walk_files_sorted(&path, root, out)?;
        } else if meta.is_file() {
            out.push((rel_path_forward(&path, root), path));
        }
    }
    Ok(())
}

fn glob_to_regex(pattern: &str, case_insensitive: bool) -> std::result::Result<Regex, String> {
    let mut re = String::from("^");
    for ch in pattern.chars() {
        match ch {
            '*' => re.push_str(".*"),
            '?' => re.push('.'),
            other => re.push_str(&regex::escape(&other.to_string())),
        }
    }
    re.push('$');
    RegexBuilder::new(&re)
        .case_insensitive(case_insensitive)
        .build()
        .map_err(|e| e.to_string())
}

fn cmd_find(args: &[String], state: &ShellState<'_>) -> Result<CmdOutcome> {
    let mut start_raw: Option<String> = None;
    let mut name_pat: Option<(String, bool)> = None;
    let mut type_filter: Option<char> = None;
    let mut max_depth: Option<usize> = None;

    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        match arg.as_str() {
            "-name" | "-iname" => {
                let Some(val) = args.get(i + 1) else {
                    return Ok(CmdOutcome::err(2, format!("find: missing argument for '{arg}'\n")));
                };
                name_pat = Some((val.clone(), arg == "-iname"));
                i += 2;
            }
            "-type" => {
                let Some(val) = args.get(i + 1) else {
                    return Ok(CmdOutcome::err(2, "find: missing argument for '-type'\n".into()));
                };
                match val.as_str() {
                    "f" => type_filter = Some('f'),
                    "d" => type_filter = Some('d'),
                    other => {
                        return Ok(CmdOutcome::err(
                            2,
                            format!("find: unsupported -type '{other}' (expected 'f' or 'd')\n"),
                        ));
                    }
                }
                i += 2;
            }
            "-maxdepth" => {
                let Some(val) = args.get(i + 1) else {
                    return Ok(CmdOutcome::err(
                        2,
                        "find: missing argument for '-maxdepth'\n".into(),
                    ));
                };
                let Ok(d) = val.parse::<usize>() else {
                    return Ok(CmdOutcome::err(
                        2,
                        format!("find: invalid maxdepth '{val}'\n"),
                    ));
                };
                max_depth = Some(d);
                i += 2;
            }
            other if !other.starts_with('-') && start_raw.is_none() => {
                start_raw = Some(other.to_string());
                i += 1;
            }
            other => {
                return Ok(CmdOutcome::err(
                    2,
                    format!("find: unsupported argument '{other}'\n"),
                ));
            }
        }
    }

    let name_re = match name_pat {
        Some((pat, ci)) => match glob_to_regex(&pat, ci) {
            Ok(r) => Some(r),
            Err(e) => return Ok(CmdOutcome::err(2, format!("find: invalid glob: {e}\n"))),
        },
        None => None,
    };

    let start_label = start_raw.as_deref().unwrap_or(".");
    let start_path = resolve_shell_path(start_label, &state.root, &state.cwd, state.cfg)?;
    let Ok(meta) = fs::symlink_metadata(&start_path) else {
        return Ok(CmdOutcome::err(
            1,
            format!("find: '{start_label}': no such file or directory\n"),
        ));
    };
    if meta.file_type().is_symlink() {
        return Err(AgentError::PermissionDenied(
            "symlinks are forbidden inside sandbox".into(),
        ));
    }

    let mut results = Vec::new();
    collect_find_entries(
        &start_path,
        &state.root,
        0,
        max_depth,
        type_filter,
        name_re.as_ref(),
        &mut results,
    );

    let mut out = String::new();
    for entry in results {
        let _ = writeln!(out, "{entry}");
    }
    Ok(CmdOutcome::ok(out))
}

fn collect_find_entries(
    path: &Path,
    root: &Path,
    depth: usize,
    max_depth: Option<usize>,
    type_filter: Option<char>,
    name_re: Option<&Regex>,
    out: &mut Vec<String>,
) {
    let Ok(meta) = fs::symlink_metadata(path) else {
        return;
    };
    if meta.file_type().is_symlink() {
        return;
    }

    let is_dir = meta.is_dir();
    let type_ok = match type_filter {
        Some('f') => meta.is_file(),
        Some('d') => is_dir,
        _ => true,
    };
    let name_ok = match name_re {
        Some(re) => {
            let base = if path == root {
                "."
            } else {
                path.file_name().and_then(|n| n.to_str()).unwrap_or("")
            };
            re.is_match(base)
        }
        None => true,
    };

    if type_ok && name_ok {
        out.push(rel_path_forward(path, root));
    }

    if is_dir && max_depth.map(|m| depth < m).unwrap_or(true) {
        if let Ok(rd) = fs::read_dir(path) {
            let mut children: Vec<PathBuf> = rd.flatten().map(|e| e.path()).collect();
            children.sort();
            for child in children {
                if let Some(name) = child.file_name().and_then(|n| n.to_str()) {
                    if SKIP_WALK_DIRS.contains(&name) {
                        continue;
                    }
                }
                collect_find_entries(
                    &child,
                    root,
                    depth + 1,
                    max_depth,
                    type_filter,
                    name_re,
                    out,
                );
            }
        }
    }
}

fn unescape_c_escapes(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            match chars.next() {
                Some('n') => out.push('\n'),
                Some('t') => out.push('\t'),
                Some('r') => out.push('\r'),
                Some('\\') => out.push('\\'),
                Some(other) => {
                    out.push('\\');
                    out.push(other);
                }
                None => out.push('\\'),
            }
        } else {
            out.push(ch);
        }
    }
    out
}

fn cmd_echo(args: &[String]) -> Result<CmdOutcome> {
    let mut no_newline = false;
    let mut interpret_escapes = false;
    let mut idx = 0;

    while idx < args.len() {
        match args[idx].as_str() {
            "-n" => {
                no_newline = true;
                idx += 1;
            }
            "-e" => {
                interpret_escapes = true;
                idx += 1;
            }
            "-ne" | "-en" => {
                no_newline = true;
                interpret_escapes = true;
                idx += 1;
            }
            _ => break,
        }
    }

    let joined = args[idx..].join(" ");
    let mut out = if interpret_escapes {
        unescape_c_escapes(&joined)
    } else {
        joined
    };
    if !no_newline {
        out.push('\n');
    }
    Ok(CmdOutcome::ok(out))
}

fn cmd_mkdir(args: &[String], state: &ShellState<'_>) -> Result<CmdOutcome> {
    let mut parents = false;
    let mut dirs = Vec::new();
    let mut stop_flags = false;

    for arg in args {
        if !stop_flags && arg == "--" {
            stop_flags = true;
            continue;
        }
        if !stop_flags && arg.starts_with('-') && arg.len() > 1 {
            for c in arg[1..].chars() {
                match c {
                    'p' => parents = true,
                    other => {
                        return Ok(CmdOutcome::err(
                            2,
                            format!("mkdir: unsupported option '-{other}'\n"),
                        ));
                    }
                }
            }
        } else {
            dirs.push(arg.as_str());
        }
    }
    if dirs.is_empty() {
        return Ok(CmdOutcome::err(1, "mkdir: missing operand\n".into()));
    }

    let mut err = String::new();
    let mut code = 0;
    for raw in dirs {
        let target = resolve_shell_path(raw, &state.root, &state.cwd, state.cfg)?;
        if target.exists() {
            if parents && target.is_dir() {
                continue;
            }
            err.push_str(&format!("mkdir: cannot create directory '{raw}': file exists\n"));
            code = 1;
            continue;
        }
        let res = if parents {
            fs::create_dir_all(&target)
        } else {
            fs::create_dir(&target)
        };
        if let Err(e) = res {
            err.push_str(&format!("mkdir: cannot create '{raw}': {e}\n"));
            code = 1;
        }
    }
    Ok(CmdOutcome {
        exit_code: code,
        stdout: String::new(),
        stderr: err,
    })
}

fn cmd_touch(args: &[String], state: &ShellState<'_>) -> Result<CmdOutcome> {
    let files: Vec<&str> = args
        .iter()
        .map(String::as_str)
        .filter(|a| *a != "--")
        .collect();
    if files.is_empty() {
        return Ok(CmdOutcome::err(1, "touch: missing file operand\n".into()));
    }

    let mut err = String::new();
    let mut code = 0;
    for raw in files {
        let target = resolve_shell_path(raw, &state.root, &state.cwd, state.cfg)?;
        if target.is_dir() {
            err.push_str(&format!("touch: '{raw}': is a directory\n"));
            code = 1;
            continue;
        }
        if let Some(parent) = target.parent() {
            if !parent.exists() {
                err.push_str(&format!("touch: '{raw}': parent directory does not exist\n"));
                code = 1;
                continue;
            }
        }
        if let Err(e) = OpenOptions::new().create(true).append(true).open(&target) {
            err.push_str(&format!("touch: '{raw}': {e}\n"));
            code = 1;
        }
    }
    Ok(CmdOutcome {
        exit_code: code,
        stdout: String::new(),
        stderr: err,
    })
}

fn copy_entry_recursive(src: &Path, dst: &Path, recursive: bool, cfg: &ToolsConfig) -> Result<()> {
    let meta = fs::symlink_metadata(src).map_err(|e| AgentError::Tool {
        tool_id: "sandbox_exec".into(),
        message: format!("stat '{}' failed: {e}", src.display()),
    })?;
    if meta.file_type().is_symlink() {
        return Err(AgentError::PermissionDenied(
            "symlinks are forbidden inside sandbox".into(),
        ));
    }
    if meta.is_dir() {
        if !recursive {
            return Err(AgentError::InvalidInput(format!(
                "cp: '{}' is a directory (pass -r)",
                src.display()
            )));
        }
        if dst.starts_with(src) {
            return Err(AgentError::InvalidInput(
                "cp: cannot copy a directory into itself".into(),
            ));
        }
        fs::create_dir_all(dst).map_err(|e| AgentError::Tool {
            tool_id: "sandbox_exec".into(),
            message: format!("mkdir '{}' failed: {e}", dst.display()),
        })?;
        let rd = fs::read_dir(src).map_err(|e| AgentError::Tool {
            tool_id: "sandbox_exec".into(),
            message: format!("read_dir '{}' failed: {e}", src.display()),
        })?;
        for entry_res in rd {
            let entry = entry_res.map_err(|e| AgentError::Tool {
                tool_id: "sandbox_exec".into(),
                message: format!("read_dir entry failed: {e}"),
            })?;
            let child_src = entry.path();
            let child_dst = dst.join(entry.file_name());
            copy_entry_recursive(&child_src, &child_dst, true, cfg)?;
        }
        return Ok(());
    }

    let bytes = read_bounded_file(src, cfg).map_err(|msg| AgentError::Tool {
        tool_id: "sandbox_exec".into(),
        message: msg,
    })?;
    if let Ok(dst_meta) = fs::symlink_metadata(dst) {
        if dst_meta.file_type().is_symlink() {
            return Err(AgentError::PermissionDenied(
                "symlinks are forbidden inside sandbox".into(),
            ));
        }
    }
    fs::write(dst, &bytes).map_err(|e| AgentError::Tool {
        tool_id: "sandbox_exec".into(),
        message: format!("write '{}' failed: {e}", dst.display()),
    })
}

fn cmd_cp(args: &[String], state: &ShellState<'_>) -> Result<CmdOutcome> {
    let mut recursive = false;
    let mut paths = Vec::new();
    let mut stop_flags = false;

    for arg in args {
        if !stop_flags && arg == "--" {
            stop_flags = true;
            continue;
        }
        if !stop_flags && arg.starts_with('-') && arg.len() > 1 {
            for c in arg[1..].chars() {
                match c {
                    'r' | 'R' => recursive = true,
                    other => {
                        return Ok(CmdOutcome::err(
                            2,
                            format!("cp: unsupported option '-{other}'\n"),
                        ));
                    }
                }
            }
        } else {
            paths.push(arg.as_str());
        }
    }

    if paths.len() < 2 {
        return Ok(CmdOutcome::err(1, "cp: expected source and destination\n".into()));
    }

    let dst_raw = *paths.last().unwrap();
    let sources = &paths[..paths.len() - 1];
    let dst_base = resolve_shell_path(dst_raw, &state.root, &state.cwd, state.cfg)?;

    if sources.len() > 1 && !dst_base.is_dir() {
        return Ok(CmdOutcome::err(
            1,
            format!("cp: target '{dst_raw}' is not a directory\n"),
        ));
    }

    let mut err = String::new();
    let mut code = 0;

    for src_raw in sources {
        let src = resolve_shell_path(src_raw, &state.root, &state.cwd, state.cfg)?;
        if src == state.root {
            return Err(AgentError::PermissionDenied(
                "cannot copy workspace root directory".into(),
            ));
        }
        if !src.exists() {
            err.push_str(&format!("cp: '{src_raw}': no such file or directory\n"));
            code = 1;
            continue;
        }
        let target_dst = if dst_base.is_dir() {
            match src.file_name() {
                Some(name) => dst_base.join(name),
                None => {
                    err.push_str(&format!("cp: invalid source '{src_raw}'\n"));
                    code = 1;
                    continue;
                }
            }
        } else {
            if let Some(parent) = dst_base.parent() {
                if !parent.exists() {
                    err.push_str(&format!(
                        "cp: destination directory for '{dst_raw}' does not exist\n"
                    ));
                    code = 1;
                    continue;
                }
            }
            dst_base.clone()
        };

        if let Err(e) = copy_entry_recursive(&src, &target_dst, recursive, state.cfg) {
            match e {
                AgentError::PermissionDenied(_) => return Err(e),
                other => {
                    err.push_str(&format!("{other}\n"));
                    code = 1;
                }
            }
        }
    }

    Ok(CmdOutcome {
        exit_code: code,
        stdout: String::new(),
        stderr: err,
    })
}

fn cmd_mv(args: &[String], state: &ShellState<'_>) -> Result<CmdOutcome> {
    let mut paths = Vec::new();
    let mut stop_flags = false;

    for arg in args {
        if !stop_flags && arg == "--" {
            stop_flags = true;
            continue;
        }
        if !stop_flags && arg == "-f" {
            continue;
        }
        if !stop_flags && arg.starts_with('-') && arg.len() > 1 {
            return Ok(CmdOutcome::err(
                2,
                format!("mv: unsupported option '{arg}'\n"),
            ));
        }
        paths.push(arg.as_str());
    }

    if paths.len() < 2 {
        return Ok(CmdOutcome::err(1, "mv: expected source and destination\n".into()));
    }

    let dst_raw = *paths.last().unwrap();
    let sources = &paths[..paths.len() - 1];
    let dst_base = resolve_shell_path(dst_raw, &state.root, &state.cwd, state.cfg)?;

    if sources.len() > 1 && !dst_base.is_dir() {
        return Ok(CmdOutcome::err(
            1,
            format!("mv: target '{dst_raw}' is not a directory\n"),
        ));
    }

    let mut err = String::new();
    let mut code = 0;

    for src_raw in sources {
        let src = resolve_shell_path(src_raw, &state.root, &state.cwd, state.cfg)?;
        if src == state.root {
            return Err(AgentError::PermissionDenied(
                "cannot move workspace root directory".into(),
            ));
        }
        if !src.exists() {
            err.push_str(&format!("mv: '{src_raw}': no such file or directory\n"));
            code = 1;
            continue;
        }
        validate_no_symlinks_tree(&src)?;
        let target_dst = if dst_base.is_dir() {
            match src.file_name() {
                Some(name) => dst_base.join(name),
                None => {
                    err.push_str(&format!("mv: invalid source '{src_raw}'\n"));
                    code = 1;
                    continue;
                }
            }
        } else {
            if let Some(parent) = dst_base.parent() {
                if !parent.exists() {
                    err.push_str(&format!(
                        "mv: destination directory for '{dst_raw}' does not exist\n"
                    ));
                    code = 1;
                    continue;
                }
            }
            dst_base.clone()
        };

        if target_dst.starts_with(&src) && src.is_dir() {
            err.push_str("mv: cannot move a directory into itself\n");
            code = 1;
            continue;
        }
        if let Err(e) = fs::rename(&src, &target_dst) {
            err.push_str(&format!("mv: failed to move '{src_raw}' -> '{dst_raw}': {e}\n"));
            code = 1;
        }
    }

    Ok(CmdOutcome {
        exit_code: code,
        stdout: String::new(),
        stderr: err,
    })
}

fn cmd_rm(args: &[String], state: &ShellState<'_>) -> Result<CmdOutcome> {
    let mut recursive = false;
    let mut force = false;
    let mut targets = Vec::new();
    let mut stop_flags = false;

    for arg in args {
        if !stop_flags && arg == "--" {
            stop_flags = true;
            continue;
        }
        if !stop_flags && arg.starts_with('-') && arg.len() > 1 {
            for c in arg[1..].chars() {
                match c {
                    'r' | 'R' => recursive = true,
                    'f' => force = true,
                    other => {
                        return Ok(CmdOutcome::err(
                            2,
                            format!("rm: unsupported option '-{other}'\n"),
                        ));
                    }
                }
            }
        } else {
            targets.push(arg.as_str());
        }
    }

    if targets.is_empty() {
        if force {
            return Ok(CmdOutcome::ok(String::new()));
        }
        return Ok(CmdOutcome::err(1, "rm: missing operand\n".into()));
    }

    let mut err = String::new();
    let mut code = 0;

    for raw in targets {
        let target = resolve_shell_path(raw, &state.root, &state.cwd, state.cfg)?;
        if target == state.root {
            return Err(AgentError::PermissionDenied(
                "cannot remove workspace root directory".into(),
            ));
        }
        let Ok(meta) = fs::symlink_metadata(&target) else {
            if !force {
                err.push_str(&format!("rm: '{raw}': no such file or directory\n"));
                code = 1;
            }
            continue;
        };
        if meta.file_type().is_symlink() {
            return Err(AgentError::PermissionDenied(
                "symlinks are forbidden inside sandbox".into(),
            ));
        }
        if meta.is_dir() {
            if !recursive {
                err.push_str(&format!("rm: cannot remove '{raw}': is a directory\n"));
                code = 1;
                continue;
            }
            validate_no_symlinks_tree(&target)?;
            if let Err(e) = fs::remove_dir_all(&target) {
                err.push_str(&format!("rm: failed to remove '{raw}': {e}\n"));
                code = 1;
            }
        } else if let Err(e) = fs::remove_file(&target) {
            err.push_str(&format!("rm: failed to remove '{raw}': {e}\n"));
            code = 1;
        }
    }

    Ok(CmdOutcome {
        exit_code: code,
        stdout: String::new(),
        stderr: err,
    })
}

struct SedSub {
    regex: Regex,
    replacement: String,
    global: bool,
}

fn parse_sed_expr(expr: &str) -> std::result::Result<SedSub, String> {
    let mut chars = expr.chars();
    if chars.next() != Some('s') {
        return Err(format!("unsupported sed command '{expr}'; expected s/pat/rep/[g][i]"));
    }
    let Some(delim) = chars.next() else {
        return Err("sed 's' command missing delimiter".into());
    };
    let rest: Vec<char> = chars.collect();

    let parse_segment = |start: usize| -> std::result::Result<(String, usize), String> {
        let mut out = String::new();
        let mut i = start;
        while i < rest.len() {
            let c = rest[i];
            if c == '\\' && i + 1 < rest.len() && rest[i + 1] == delim {
                out.push(delim);
                i += 2;
                continue;
            }
            if c == delim {
                return Ok((out, i + 1));
            }
            out.push(c);
            i += 1;
        }
        Err("unterminated sed pattern/replacement".into())
    };

    let (pat, next_idx) = parse_segment(0)?;
    let (raw_rep, flags_idx) = parse_segment(next_idx)?;
    let mut global = false;
    let mut ignore_case = false;
    for flag in &rest[flags_idx..] {
        match flag {
            'g' => global = true,
            'i' | 'I' => ignore_case = true,
            other => return Err(format!("unsupported sed flag '{other}'")),
        }
    }

    let regex = RegexBuilder::new(&pat)
        .case_insensitive(ignore_case)
        .build()
        .map_err(|e| format!("invalid sed regex '{pat}': {e}"))?;

    let replacement = convert_sed_replacement(&raw_rep);
    Ok(SedSub {
        regex,
        replacement,
        global,
    })
}

fn convert_sed_replacement(raw: &str) -> String {
    let chars: Vec<char> = raw.chars().collect();
    let mut out = String::with_capacity(raw.len());
    let mut i = 0;
    while i < chars.len() {
        match chars[i] {
            '\\' if i + 1 < chars.len() => {
                let next = chars[i + 1];
                match next {
                    'n' => out.push('\n'),
                    't' => out.push('\t'),
                    'r' => out.push('\r'),
                    '&' => out.push('&'),
                    '\\' => out.push('\\'),
                    '1'..='9' => {
                        out.push_str("${");
                        out.push(next);
                        out.push('}');
                    }
                    other => out.push(other),
                }
                i += 2;
            }
            '&' => {
                out.push_str("${0}");
                i += 1;
            }
            '$' => {
                out.push_str("$$");
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    out
}

fn cmd_sed(args: &[String], stdin_str: &str, state: &ShellState<'_>) -> Result<CmdOutcome> {
    let mut in_place = false;
    let mut exprs: Vec<String> = Vec::new();
    let mut files: Vec<String> = Vec::new();
    let mut i = 0;
    let mut stop_flags = false;

    while i < args.len() {
        let arg = &args[i];
        if !stop_flags && arg == "--" {
            stop_flags = true;
            i += 1;
            continue;
        }
        if !stop_flags && arg == "-i" {
            in_place = true;
            i += 1;
            continue;
        }
        if !stop_flags && arg == "-e" {
            let Some(val) = args.get(i + 1) else {
                return Ok(CmdOutcome::err(2, "sed: '-e' requires an expression\n".into()));
            };
            exprs.push(val.clone());
            i += 2;
            continue;
        }
        if !stop_flags && arg.starts_with('-') && arg.len() > 1 {
            return Ok(CmdOutcome::err(2, format!("sed: unsupported option '{arg}'\n")));
        }
        if exprs.is_empty() {
            exprs.push(arg.clone());
        } else {
            files.push(arg.clone());
        }
        i += 1;
    }

    if in_place && state.sandbox_mode == SandboxMode::ReadOnly {
        return Err(AgentError::PermissionDenied(
            "sandbox_mode is read_only; 'sed -i' is disabled".into(),
        ));
    }
    if exprs.is_empty() {
        return Ok(CmdOutcome::err(2, "sed: missing script expression\n".into()));
    }

    let mut subs = Vec::with_capacity(exprs.len());
    for e in &exprs {
        match parse_sed_expr(e) {
            Ok(s) => subs.push(s),
            Err(msg) => return Ok(CmdOutcome::err(2, format!("sed: {msg}\n"))),
        }
    }

    let apply_subs = |text: &str| -> String {
        let mut out = String::with_capacity(text.len());
        for line in text.split_inclusive('\n') {
            let (body, nl) = match line.strip_suffix('\n') {
                Some(b) => (b, "\n"),
                None => (line, ""),
            };
            let mut cur = body.to_string();
            for sub in &subs {
                cur = if sub.global {
                    sub.regex.replace_all(&cur, sub.replacement.as_str()).into_owned()
                } else {
                    sub.regex.replace(&cur, sub.replacement.as_str()).into_owned()
                };
            }
            out.push_str(&cur);
            out.push_str(nl);
        }
        out
    };

    if in_place {
        if files.is_empty() {
            return Ok(CmdOutcome::err(2, "sed: '-i' requires at least one file\n".into()));
        }
        let mut err = String::new();
        let mut code = 0;
        for raw in &files {
            let target = resolve_shell_path(raw, &state.root, &state.cwd, state.cfg)?;
            match read_bounded_file(&target, state.cfg) {
                Ok(bytes) => {
                    let updated = apply_subs(&String::from_utf8_lossy(&bytes));
                    if let Err(msg) = write_redirect_target(&target, updated.as_bytes(), false, state.cfg) {
                        err.push_str(&format!("sed: {msg}\n"));
                        code = 1;
                    }
                }
                Err(msg) => {
                    err.push_str(&format!("sed: {msg}\n"));
                    code = 1;
                }
            }
        }
        return Ok(CmdOutcome {
            exit_code: code,
            stdout: String::new(),
            stderr: err,
        });
    }

    let (inputs, err, code) = collect_text_inputs(&files, stdin_str, state)?;
    let mut out = String::new();
    for (_, text) in inputs {
        out.push_str(&apply_subs(&text));
    }
    Ok(CmdOutcome {
        exit_code: code,
        stdout: out,
        stderr: err,
    })
}

fn cmd_sort(args: &[String], stdin_str: &str, state: &ShellState<'_>) -> Result<CmdOutcome> {
    let mut reverse = false;
    let mut unique = false;
    let mut numeric = false;
    let mut files = Vec::new();
    let mut stop_flags = false;

    for arg in args {
        if !stop_flags && arg == "--" {
            stop_flags = true;
            continue;
        }
        if !stop_flags && arg.starts_with('-') && arg.len() > 1 {
            for c in arg[1..].chars() {
                match c {
                    'r' => reverse = true,
                    'u' => unique = true,
                    'n' => numeric = true,
                    other => {
                        return Ok(CmdOutcome::err(
                            2,
                            format!("sort: unsupported option '-{other}'\n"),
                        ));
                    }
                }
            }
        } else {
            files.push(arg.clone());
        }
    }

    let (inputs, err, code) = collect_text_inputs(&files, stdin_str, state)?;
    let mut lines: Vec<String> = Vec::new();
    for (_, text) in inputs {
        for line in text.lines() {
            lines.push(line.to_string());
        }
    }

    lines.sort_by(|a, b| {
        let ord = if numeric {
            let na = a.trim().parse::<f64>().unwrap_or(0.0);
            let nb = b.trim().parse::<f64>().unwrap_or(0.0);
            na.total_cmp(&nb).then_with(|| a.cmp(b))
        } else {
            a.cmp(b)
        };
        if reverse {
            ord.reverse()
        } else {
            ord
        }
    });

    if unique {
        lines.dedup();
    }

    let mut out = String::new();
    for line in lines {
        let _ = writeln!(out, "{line}");
    }
    Ok(CmdOutcome {
        exit_code: code,
        stdout: out,
        stderr: err,
    })
}

fn cmd_uniq(args: &[String], stdin_str: &str, state: &ShellState<'_>) -> Result<CmdOutcome> {
    let mut count = false;
    let mut files = Vec::new();
    let mut stop_flags = false;

    for arg in args {
        if !stop_flags && arg == "--" {
            stop_flags = true;
            continue;
        }
        if !stop_flags && arg.starts_with('-') && arg.len() > 1 {
            for c in arg[1..].chars() {
                match c {
                    'c' => count = true,
                    other => {
                        return Ok(CmdOutcome::err(
                            2,
                            format!("uniq: unsupported option '-{other}'\n"),
                        ));
                    }
                }
            }
        } else {
            files.push(arg.clone());
        }
    }

    let (inputs, err, code) = collect_text_inputs(&files, stdin_str, state)?;
    let mut combined = String::new();
    for (_, text) in inputs {
        combined.push_str(&text);
    }

    let mut groups: Vec<(usize, &str)> = Vec::new();
    for line in combined.lines() {
        if let Some(last) = groups.last_mut() {
            if last.1 == line {
                last.0 += 1;
                continue;
            }
        }
        groups.push((1, line));
    }

    let mut out = String::new();
    for (n, line) in groups {
        if count {
            let _ = writeln!(out, "{n:>4} {line}");
        } else {
            let _ = writeln!(out, "{line}");
        }
    }
    Ok(CmdOutcome {
        exit_code: code,
        stdout: out,
        stderr: err,
    })
}

#[derive(Debug, Clone)]
enum CutRange {
    Single(usize),
    From(usize),
    UpTo(usize),
    Span(usize, usize),
}

impl CutRange {
    fn contains(&self, one_based: usize) -> bool {
        match *self {
            Self::Single(n) => one_based == n,
            Self::From(start) => one_based >= start,
            Self::UpTo(end) => one_based <= end,
            Self::Span(s, e) => one_based >= s && one_based <= e,
        }
    }
}

fn parse_cut_ranges(spec: &str) -> std::result::Result<Vec<CutRange>, String> {
    let mut ranges = Vec::new();
    for part in spec.split(',') {
        let part = part.trim();
        if part.is_empty() {
            return Err("empty field specification".into());
        }
        if let Some((left, right)) = part.split_once('-') {
            if left.is_empty() && right.is_empty() {
                return Err("invalid range '-'".into());
            } else if left.is_empty() {
                let end = right.parse::<usize>().map_err(|_| format!("invalid field '{part}'"))?;
                if end == 0 {
                    return Err("cut fields are 1-based".into());
                }
                ranges.push(CutRange::UpTo(end));
            } else if right.is_empty() {
                let start = left.parse::<usize>().map_err(|_| format!("invalid field '{part}'"))?;
                if start == 0 {
                    return Err("cut fields are 1-based".into());
                }
                ranges.push(CutRange::From(start));
            } else {
                let start = left.parse::<usize>().map_err(|_| format!("invalid field '{part}'"))?;
                let end = right.parse::<usize>().map_err(|_| format!("invalid field '{part}'"))?;
                if start == 0 || end < start {
                    return Err(format!("invalid range '{part}'"));
                }
                ranges.push(CutRange::Span(start, end));
            }
        } else {
            let n = part.parse::<usize>().map_err(|_| format!("invalid field '{part}'"))?;
            if n == 0 {
                return Err("cut fields are 1-based".into());
            }
            ranges.push(CutRange::Single(n));
        }
    }
    Ok(ranges)
}

fn cmd_cut(args: &[String], stdin_str: &str, state: &ShellState<'_>) -> Result<CmdOutcome> {
    let mut delim = "\t".to_string();
    let mut field_spec: Option<String> = None;
    let mut files = Vec::new();
    let mut i = 0;

    while i < args.len() {
        let arg = &args[i];
        if arg == "-d" {
            let Some(val) = args.get(i + 1) else {
                return Ok(CmdOutcome::err(2, "cut: '-d' requires a delimiter\n".into()));
            };
            delim = val.clone();
            i += 2;
        } else if let Some(rest) = arg.strip_prefix("-d") {
            delim = rest.to_string();
            i += 1;
        } else if arg == "-f" {
            let Some(val) = args.get(i + 1) else {
                return Ok(CmdOutcome::err(2, "cut: '-f' requires field list\n".into()));
            };
            field_spec = Some(val.clone());
            i += 2;
        } else if let Some(rest) = arg.strip_prefix("-f") {
            field_spec = Some(rest.to_string());
            i += 1;
        } else if arg.starts_with('-') && arg.len() > 1 {
            return Ok(CmdOutcome::err(2, format!("cut: unsupported option '{arg}'\n")));
        } else {
            files.push(arg.clone());
            i += 1;
        }
    }

    let Some(spec) = field_spec else {
        return Ok(CmdOutcome::err(2, "cut: must specify '-f'\n".into()));
    };
    let ranges = match parse_cut_ranges(&spec) {
        Ok(r) => r,
        Err(msg) => return Ok(CmdOutcome::err(2, format!("cut: {msg}\n"))),
    };

    let (inputs, err, code) = collect_text_inputs(&files, stdin_str, state)?;
    let mut out = String::new();
    for (_, text) in inputs {
        for line in text.lines() {
            if !delim.is_empty() && !line.contains(&delim) {
                let _ = writeln!(out, "{line}");
                continue;
            }
            let parts: Vec<&str> = line.split(&delim).collect();
            let chosen: Vec<&str> = parts
                .iter()
                .enumerate()
                .filter_map(|(idx, val)| {
                    let pos = idx + 1;
                    if ranges.iter().any(|r| r.contains(pos)) {
                        Some(*val)
                    } else {
                        None
                    }
                })
                .collect();
            let _ = writeln!(out, "{}", chosen.join(&delim));
        }
    }
    Ok(CmdOutcome {
        exit_code: code,
        stdout: out,
        stderr: err,
    })
}

fn expand_tr_set(spec: &str) -> Vec<char> {
    let chars: Vec<char> = spec.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;

    while i < chars.len() {
        if chars[i] == '[' {
            let slice: String = chars[i..].iter().collect();
            let class_map: [(&str, &str); 5] = [
                ("[:lower:]", "abcdefghijklmnopqrstuvwxyz"),
                ("[:upper:]", "ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
                ("[:digit:]", "0123456789"),
                ("[:alpha:]", "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"),
                ("[:space:]", " \t\n\r"),
            ];
            let mut matched = false;
            for (pat, expanded) in class_map {
                if slice.starts_with(pat) {
                    out.extend(expanded.chars());
                    i += pat.chars().count();
                    matched = true;
                    break;
                }
            }
            if matched {
                continue;
            }
        }

        let current = if chars[i] == '\\' && i + 1 < chars.len() {
            let mapped = match chars[i + 1] {
                'n' => '\n',
                't' => '\t',
                'r' => '\r',
                '\\' => '\\',
                other => other,
            };
            i += 2;
            mapped
        } else {
            let c = chars[i];
            i += 1;
            c
        };

        if i + 1 < chars.len() && chars[i] == '-' {
            let end_ch = if chars[i + 1] == '\\' && i + 2 < chars.len() {
                let c = chars[i + 2];
                i += 3;
                c
            } else {
                let c = chars[i + 1];
                i += 2;
                c
            };
            if (current as u32) <= (end_ch as u32) {
                for code in (current as u32)..=(end_ch as u32) {
                    if let Some(ch) = char::from_u32(code) {
                        out.push(ch);
                    }
                }
            }
        } else {
            out.push(current);
        }
    }
    out
}

fn cmd_tr(args: &[String], stdin_str: &str) -> Result<CmdOutcome> {
    let mut delete = false;
    let mut squeeze = false;
    let mut sets = Vec::new();

    for arg in args {
        if arg.starts_with('-') && arg.len() > 1 {
            for c in arg[1..].chars() {
                match c {
                    'd' => delete = true,
                    's' => squeeze = true,
                    other => {
                        return Ok(CmdOutcome::err(
                            2,
                            format!("tr: unsupported option '-{other}'\n"),
                        ));
                    }
                }
            }
        } else {
            sets.push(arg.as_str());
        }
    }

    if sets.is_empty() {
        return Ok(CmdOutcome::err(2, "tr: missing operand\n".into()));
    }
    let set1 = expand_tr_set(sets[0]);

    if delete {
        let delete_set: BTreeSet<char> = set1.into_iter().collect();
        let out: String = stdin_str
            .chars()
            .filter(|c| !delete_set.contains(c))
            .collect();
        return Ok(CmdOutcome::ok(out));
    }

    if sets.len() == 1 && squeeze {
        let sq_set: BTreeSet<char> = set1.into_iter().collect();
        let mut out = String::with_capacity(stdin_str.len());
        let mut prev: Option<char> = None;
        for c in stdin_str.chars() {
            if sq_set.contains(&c) && prev == Some(c) {
                continue;
            }
            out.push(c);
            prev = Some(c);
        }
        return Ok(CmdOutcome::ok(out));
    }

    if sets.len() < 2 {
        return Ok(CmdOutcome::err(2, "tr: missing second character set\n".into()));
    }
    let set2 = expand_tr_set(sets[1]);
    if set2.is_empty() {
        return Ok(CmdOutcome::err(2, "tr: replacement set cannot be empty\n".into()));
    }
    let sq_set: BTreeSet<char> = if squeeze {
        set2.iter().copied().collect()
    } else {
        BTreeSet::new()
    };

    let mut out = String::with_capacity(stdin_str.len());
    let mut prev: Option<char> = None;
    for ch in stdin_str.chars() {
        let mapped = match set1.iter().position(|&c| c == ch) {
            Some(pos) => set2[pos.min(set2.len() - 1)],
            None => ch,
        };
        if squeeze && sq_set.contains(&mapped) && prev == Some(mapped) {
            continue;
        }
        out.push(mapped);
        prev = Some(mapped);
    }

    Ok(CmdOutcome::ok(out))
}

fn cmd_sha256sum(args: &[String], stdin_str: &str, state: &ShellState<'_>) -> Result<CmdOutcome> {
    let files: Vec<String> = args
        .iter()
        .filter(|a| a.as_str() != "--")
        .cloned()
        .collect();
    if files.is_empty() {
        let digest = sha256::hex(stdin_str.as_bytes());
        return Ok(CmdOutcome::ok(format!("{digest}  -\n")));
    }

    let mut out = String::new();
    let mut err = String::new();
    let mut code = 0;

    for raw in files {
        if raw == "-" {
            let digest = sha256::hex(stdin_str.as_bytes());
            let _ = writeln!(out, "{digest}  -");
            continue;
        }
        let target = resolve_shell_path(&raw, &state.root, &state.cwd, state.cfg)?;
        match read_bounded_file(&target, state.cfg) {
            Ok(bytes) => {
                let digest = sha256::hex(&bytes);
                let _ = writeln!(out, "{digest}  {raw}");
            }
            Err(msg) => {
                err.push_str(&format!("sha256sum: {msg}\n"));
                code = 1;
            }
        }
    }

    Ok(CmdOutcome {
        exit_code: code,
        stdout: out,
        stderr: err,
    })
}

fn cmd_json(args: &[String], stdin_str: &str, state: &ShellState<'_>) -> Result<CmdOutcome> {
    let mut compact = false;
    let mut files = Vec::new();

    for arg in args {
        match arg.as_str() {
            "-c" | "--compact" => compact = true,
            other if other.starts_with('-') && other != "-" => {
                return Ok(CmdOutcome::err(2, format!("json: unsupported option '{other}'\n")));
            }
            other => files.push(other.to_string()),
        }
    }

    let (inputs, mut err, mut code) = collect_text_inputs(&files, stdin_str, state)?;
    let mut out = String::new();
    for (label, text) in inputs {
        match serde_json::from_str::<Value>(&text) {
            Ok(val) => {
                let rendered = if compact {
                    serde_json::to_string(&val)
                } else {
                    serde_json::to_string_pretty(&val)
                };
                match rendered {
                    Ok(s) => {
                        out.push_str(&s);
                        out.push('\n');
                    }
                    Err(e) => {
                        err.push_str(&format!("json: serialize '{label}' failed: {e}\n"));
                        code = 1;
                    }
                }
            }
            Err(e) => {
                err.push_str(&format!("json: invalid JSON ({label}): {e}\n"));
                code = 1;
            }
        }
    }

    Ok(CmdOutcome {
        exit_code: code,
        stdout: out,
        stderr: err,
    })
}

fn cmd_date(args: &[String]) -> Result<CmdOutcome> {
    let now = Utc::now();
    let mut fmt_spec: Option<&str> = None;

    for arg in args {
        match arg.as_str() {
            "-u" | "--utc" => {}
            "-I" | "--iso-8601" => fmt_spec = Some("%Y-%m-%d"),
            s if s.starts_with('+') => fmt_spec = Some(&s[1..]),
            other => {
                return Ok(CmdOutcome::err(2, format!("date: unsupported option '{other}'\n")));
            }
        }
    }

    if let Some(spec) = fmt_spec {
        let mut formatted = String::new();
        if write!(formatted, "{}", now.format(spec)).is_err() {
            return Ok(CmdOutcome::err(
                2,
                format!("date: invalid format string '+{spec}'\n"),
            ));
        }
        formatted.push('\n');
        return Ok(CmdOutcome::ok(formatted));
    }

    Ok(CmdOutcome::ok(format!("{}\n", now.to_rfc3339())))
}

fn cmd_env(state: &ShellState<'_>) -> Result<CmdOutcome> {
    let mut out = String::new();
    for (k, v) in &state.env {
        let _ = writeln!(out, "{k}={v}");
    }
    Ok(CmdOutcome::ok(out))
}

fn cmd_which(args: &[String]) -> Result<CmdOutcome> {
    if args.is_empty() {
        return Ok(CmdOutcome::err(1, String::new()));
    }
    let mut out = String::new();
    let mut err = String::new();
    let mut code = 0;

    for name in args {
        if VIRTUAL_BUILTINS.contains(&name.as_str()) {
            let _ = writeln!(out, "/virtual/bin/{name}");
        } else {
            let _ = writeln!(err, "which: no '{name}' in virtual shell built-ins");
            code = 1;
        }
    }
    Ok(CmdOutcome {
        exit_code: code,
        stdout: out,
        stderr: err,
    })
}

// ---------------------------------------------------------------------------
// Host Engine Execution (compiled ONLY on non-PS5 hosts)
// ---------------------------------------------------------------------------

#[cfg(target_vendor = "sie")]
async fn run_host_engine(
    _command: &str,
    _raw_cwd: &str,
    _cfg: &ToolsConfig,
    _sandbox_mode: SandboxMode,
) -> Result<Value> {
    Err(AgentError::PermissionDenied(
        "host shell execution is disabled on PS5; use engine='virtual'".into(),
    ))
}

#[cfg(not(target_vendor = "sie"))]
struct CancelOnDrop {
    flag: std::sync::Arc<std::sync::atomic::AtomicBool>,
    active: bool,
}

#[cfg(not(target_vendor = "sie"))]
impl CancelOnDrop {
    fn new(flag: std::sync::Arc<std::sync::atomic::AtomicBool>) -> Self {
        Self { flag, active: true }
    }

    fn disarm(&mut self) {
        self.active = false;
    }
}

#[cfg(not(target_vendor = "sie"))]
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if self.active {
            self.flag.store(true, std::sync::atomic::Ordering::Release);
        }
    }
}

#[cfg(not(target_vendor = "sie"))]
fn spawn_bounded_pipe_reader<R: std::io::Read + Send + 'static>(
    pipe_opt: Option<R>,
    read_limit: u64,
) -> std::thread::JoinHandle<Vec<u8>> {
    use std::io::Read as _;

    std::thread::spawn(move || {
        let mut buf = Vec::new();
        if let Some(pipe) = pipe_opt {
            let mut limited = pipe.take(read_limit);
            let _ = limited.read_to_end(&mut buf);
        }
        buf
    })
}

#[cfg(not(target_vendor = "sie"))]
fn cleanup_host_child(
    child: &mut std::process::Child,
    out_handle: std::thread::JoinHandle<Vec<u8>>,
    err_handle: std::thread::JoinHandle<Vec<u8>>,
) {
    let _ = child.kill();
    let _ = child.wait();
    let _ = out_handle.join();
    let _ = err_handle.join();
}

#[cfg(not(target_vendor = "sie"))]
fn run_host_engine_blocking(
    command: &str,
    cwd_path: &Path,
    cwd_rel: &str,
    max_out: usize,
    timeout_secs: u64,
    cancel_flag: &std::sync::atomic::AtomicBool,
) -> Result<Value> {
    use std::process::{Command, Stdio};
    use std::sync::atomic::Ordering;
    use std::time::Instant;

    let read_limit = (max_out as u64).saturating_add(1);
    let mut cmd = {
        #[cfg(windows)]
        {
            let mut c = Command::new("cmd");
            c.args(["/C", command]);
            c
        }
        #[cfg(not(windows))]
        {
            let mut c = Command::new("sh");
            c.args(["-c", command]);
            c
        }
    };

    cmd.current_dir(cwd_path)
        .env_clear()
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    for key in SAFE_HOST_ENV_KEYS {
        if let Some(val) = std::env::var_os(key) {
            cmd.env(key, val);
        }
    }

    let mut child = cmd.spawn().map_err(|e| AgentError::Tool {
        tool_id: "sandbox_exec".into(),
        message: format!("failed to spawn host shell: {e}"),
    })?;

    let out_handle = spawn_bounded_pipe_reader(child.stdout.take(), read_limit);
    let err_handle = spawn_bounded_pipe_reader(child.stderr.take(), read_limit);
    let deadline = if timeout_secs > 0 {
        Some(Instant::now() + Duration::from_secs(timeout_secs))
    } else {
        None
    };

    let status = loop {
        if cancel_flag.load(Ordering::Acquire) {
            cleanup_host_child(&mut child, out_handle, err_handle);
            return Err(AgentError::Cancelled(
                "host sandbox_exec cancelled".into(),
            ));
        }

        match child.try_wait() {
            Ok(Some(st)) => break st,
            Ok(None) => {
                if let Some(dl) = deadline {
                    if Instant::now() >= dl {
                        cleanup_host_child(&mut child, out_handle, err_handle);
                        return Err(AgentError::Timeout(format!(
                            "host sandbox_exec timed out after {timeout_secs}s"
                        )));
                    }
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(e) => {
                cleanup_host_child(&mut child, out_handle, err_handle);
                return Err(AgentError::Tool {
                    tool_id: "sandbox_exec".into(),
                    message: format!("host shell wait failed: {e}"),
                });
            }
        }
    };

    let out_bytes = out_handle.join().unwrap_or_default();
    let err_bytes = err_handle.join().unwrap_or_default();
    let raw_stdout = String::from_utf8_lossy(&out_bytes).into_owned();
    let raw_stderr = String::from_utf8_lossy(&err_bytes).into_owned();
    let overflowed = out_bytes.len() > max_out || err_bytes.len() > max_out;
    let (stdout, stderr, cap_trunc) = cap_outputs(raw_stdout, raw_stderr, max_out);

    Ok(json!({
        "engine": "host",
        "exit_code": status.code().unwrap_or(-1),
        "stdout": stdout,
        "stderr": stderr,
        "truncated": overflowed || cap_trunc,
        "cwd": cwd_rel,
    }))
}

#[cfg(not(target_vendor = "sie"))]
async fn run_host_engine(
    command: &str,
    raw_cwd: &str,
    cfg: &ToolsConfig,
    sandbox_mode: SandboxMode,
) -> Result<Value> {
    if !cfg.allow_host_shell || sandbox_mode != SandboxMode::FullSandbox {
        return Err(AgentError::PermissionDenied(
            "host shell execution requires allow_host_shell=true and sandbox_mode='full_sandbox'; use engine='virtual'"
                .into(),
        ));
    }

    let (root, cwd_path) = resolve_root_and_cwd(raw_cwd, cfg)?;
    let cwd_rel = rel_path_forward(&cwd_path, &root);
    let max_out = effective_max_output_bytes(cfg);
    let timeout_secs = cfg.exec_timeout_secs;
    let command_owned = command.to_string();

    let cancel_flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut guard = CancelOnDrop::new(std::sync::Arc::clone(&cancel_flag));
    let worker_flag = std::sync::Arc::clone(&cancel_flag);

    let res = tokio::task::spawn_blocking(move || {
        run_host_engine_blocking(
            &command_owned,
            &cwd_path,
            &cwd_rel,
            max_out,
            timeout_secs,
            &worker_flag,
        )
    })
    .await
    .map_err(|e| AgentError::Internal(format!("host shell join error: {e}")))?;

    guard.disarm();
    res
}

// ---------------------------------------------------------------------------
// Unit Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_cfg(root: &Path) -> ToolsConfig {
        ToolsConfig {
            allowed_paths: vec![root.to_string_lossy().into_owned()],
            ..Default::default()
        }
    }

    #[test]
    fn fips180_4_sha256_vectors_match() {
        assert_eq!(
            sha256::hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            sha256::hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[tokio::test]
    async fn virtual_pipelines_redirects_and_text_builtins() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = test_cfg(dir.path());

        // Write via redirect & append, then transform through pipeline
        let r1 = exec_sandbox_exec(
            &json!({ "command": "echo 'banana\napple\nbanana\ncherry' > list.txt && echo 'apple' >> list.txt" }),
            &cfg,
            SandboxMode::FullSandbox,
        )
        .await
        .unwrap();
        assert_eq!(r1["engine"], "virtual");
        assert_eq!(r1["exit_code"], 0);

        let r2 = exec_sandbox_exec(
            &json!({ "command": "cat list.txt | sort | uniq -c | wc -l" }),
            &cfg,
            SandboxMode::ReadOnly,
        )
        .await
        .unwrap();
        assert_eq!(r2["exit_code"], 0);
        assert_eq!(r2["stdout"].as_str().unwrap().trim(), "3");

        let r3 = exec_sandbox_exec(
            &json!({ "command": "echo 'a,b,c\n1,2,3' | cut -d ',' -f 1,3 | tr 'a-z' 'A-Z' | tail -n 1" }),
            &cfg,
            SandboxMode::ReadOnly,
        )
        .await
        .unwrap();
        assert_eq!(r3["stdout"].as_str().unwrap().trim(), "1,3");

        let r4 = exec_sandbox_exec(
            &json!({ "command": "echo '{\"k\":1}' | json" }),
            &cfg,
            SandboxMode::ReadOnly,
        )
        .await
        .unwrap();
        assert_eq!(r4["exit_code"], 0);
        assert!(r4["stdout"].as_str().unwrap().contains("\"k\": 1"));
    }

    #[tokio::test]
    async fn virtual_sha256sum_grep_sed_find_and_fs_ops() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = test_cfg(dir.path());

        exec_sandbox_exec(
            &json!({ "command": "mkdir -p sub && echo 'alpha beta alpha' > sub/data.txt && touch sub/empty.txt" }),
            &cfg,
            SandboxMode::WorkspaceWrite,
        )
        .await
        .unwrap();

        let sum = exec_sandbox_exec(
            &json!({ "command": "printf_missing_nope || echo -n 'abc' | sha256sum" }),
            &cfg,
            SandboxMode::ReadOnly,
        )
        .await
        .unwrap();
        assert_eq!(sum["exit_code"], 0);
        assert!(sum["stdout"]
            .as_str()
            .unwrap()
            .starts_with("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"));

        let grepped = exec_sandbox_exec(
            &json!({ "command": "grep -in 'BETA' sub/data.txt" }),
            &cfg,
            SandboxMode::ReadOnly,
        )
        .await
        .unwrap();
        assert_eq!(grepped["exit_code"], 0);
        assert_eq!(grepped["stdout"].as_str().unwrap().trim(), "1:alpha beta alpha");

        let seded = exec_sandbox_exec(
            &json!({ "command": "sed 's/alpha/OMEGA/g' sub/data.txt" }),
            &cfg,
            SandboxMode::ReadOnly,
        )
        .await
        .unwrap();
        assert_eq!(seded["stdout"].as_str().unwrap().trim(), "OMEGA beta OMEGA");

        let found = exec_sandbox_exec(
            &json!({ "command": "cp sub/data.txt sub/copy.txt && mv sub/copy.txt sub/moved.txt && find sub -type f -name '*.txt'" }),
            &cfg,
            SandboxMode::WorkspaceWrite,
        )
        .await
        .unwrap();
        let found_out = found["stdout"].as_str().unwrap();
        assert!(found_out.contains("sub/data.txt"));
        assert!(found_out.contains("sub/moved.txt"));
        assert!(!found_out.contains("sub/copy.txt"));

        let rmed = exec_sandbox_exec(
            &json!({ "command": "rm -rf sub && ls" }),
            &cfg,
            SandboxMode::WorkspaceWrite,
        )
        .await
        .unwrap();
        assert_eq!(rmed["exit_code"], 0);
        assert_eq!(rmed["stdout"].as_str().unwrap().trim(), "");
    }

    #[tokio::test]
    async fn readonly_rejects_all_mutating_builtins_and_redirects() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("keep.txt"), "hello\n").unwrap();
        let cfg = test_cfg(dir.path());

        for cmd in [
            "mkdir newdir",
            "touch new.txt",
            "cp keep.txt copy.txt",
            "mv keep.txt moved.txt",
            "rm keep.txt",
            "echo hi > out.txt",
            "echo hi >> keep.txt",
            "sed -i 's/hello/bye/' keep.txt",
        ] {
            let err = exec_sandbox_exec(&json!({ "command": cmd }), &cfg, SandboxMode::ReadOnly)
                .await
                .unwrap_err();
            assert!(
                matches!(err, AgentError::PermissionDenied(_)),
                "expected PermissionDenied for '{cmd}' in ReadOnly, got {err:?}"
            );
        }
    }

    #[tokio::test]
    async fn workspace_escape_root_delete_and_env_isolation() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = test_cfg(dir.path());

        // Set a host secret env var and confirm virtual `env` never leaks it
        std::env::set_var("PS5_AGENT_TEST_SECRET_TOKEN", "super_secret_do_not_leak");
        let env_out = exec_sandbox_exec(&json!({ "command": "env" }), &cfg, SandboxMode::ReadOnly)
            .await
            .unwrap();
        let stdout = env_out["stdout"].as_str().unwrap();
        assert!(!stdout.contains("super_secret_do_not_leak"));
        assert!(!stdout.contains("PS5_AGENT_TEST_SECRET_TOKEN"));
        assert!(stdout.contains("ENGINE=virtual"));

        for escape_cmd in [
            "cat ../outside.txt",
            "cat /etc/passwd",
            "rm -rf .",
        ] {
            let err =
                exec_sandbox_exec(&json!({ "command": escape_cmd }), &cfg, SandboxMode::FullSandbox)
                    .await
                    .unwrap_err();
            assert!(
                matches!(err, AgentError::PermissionDenied(_)),
                "expected PermissionDenied for '{escape_cmd}', got {err:?}"
            );
        }
    }

    #[tokio::test]
    async fn host_engine_rejects_when_disabled_or_non_full_sandbox() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = test_cfg(dir.path());
        cfg.allow_host_shell = false;

        let err1 = exec_sandbox_exec(
            &json!({ "command": "echo hi", "engine": "host" }),
            &cfg,
            SandboxMode::FullSandbox,
        )
        .await
        .unwrap_err();
        assert!(matches!(err1, AgentError::PermissionDenied(_)));

        cfg.allow_host_shell = true;
        for mode in [SandboxMode::ReadOnly, SandboxMode::WorkspaceWrite] {
            let err = exec_sandbox_exec(
                &json!({ "command": "echo hi", "engine": "host" }),
                &cfg,
                mode,
            )
            .await
            .unwrap_err();
            assert!(matches!(err, AgentError::PermissionDenied(_)));
        }
    }

    #[tokio::test]
    async fn output_truncation_caps_at_configured_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = test_cfg(dir.path());
        cfg.max_exec_output_bytes = 1024;

        let big = "A".repeat(2500);
        let cmd = format!("echo '{big}'");
        let res = exec_sandbox_exec(&json!({ "command": cmd }), &cfg, SandboxMode::ReadOnly)
            .await
            .unwrap();
        assert_eq!(res["truncated"], true);
        let stdout = res["stdout"].as_str().unwrap();
        assert!(stdout.len() <= 1024);
        assert!(stdout.ends_with("...[truncated]"));
    }

    #[tokio::test]
    async fn virtual_cd_head_date_which_rg_and_symlink_rejection() {
        let dir = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        fs::create_dir_all(dir.path().join("nest/deep")).unwrap();
        fs::write(dir.path().join("nest/deep/notes.md"), "line1\nneedle line\nline3\n").unwrap();
        fs::write(outside.path().join("secret.txt"), "topsecret").unwrap();
        let cfg = test_cfg(dir.path());

        let r = exec_sandbox_exec(
            &json!({ "command": "cd nest/deep && pwd && head -n 2 notes.md | rg -n needle && which sha256sum date && date -I" }),
            &cfg,
            SandboxMode::ReadOnly,
        )
        .await
        .unwrap();
        assert_eq!(r["exit_code"], 0);
        assert_eq!(r["cwd"], "nest/deep");
        let out = r["stdout"].as_str().unwrap();
        assert!(out.contains("nest/deep"));
        assert!(out.contains("2:needle line"));
        assert!(out.contains("/virtual/bin/sha256sum"));
        assert!(out.contains("/virtual/bin/date"));

        // Symlink inside sandbox pointing outside must be rejected
        let link_path = dir.path().join("nest/escape_link.txt");
        let target_path = outside.path().join("secret.txt");
        let symlink_res = {
            #[cfg(unix)]
            {
                std::os::unix::fs::symlink(&target_path, &link_path)
            }
            #[cfg(windows)]
            {
                std::os::windows::fs::symlink_file(&target_path, &link_path)
            }
        };
        if symlink_res.is_ok() {
            let err = exec_sandbox_exec(
                &json!({ "command": "cat nest/escape_link.txt" }),
                &cfg,
                SandboxMode::FullSandbox,
            )
            .await
            .unwrap_err();
            assert!(matches!(err, AgentError::PermissionDenied(_)));
        }
    }

    #[cfg(not(target_vendor = "sie"))]
    #[tokio::test]
    async fn host_engine_runs_when_explicitly_enabled_and_strips_secrets() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = test_cfg(dir.path());
        cfg.allow_host_shell = true;

        std::env::set_var("PS5_AGENT_HOST_SECRET_ENV", "leaked_secret_value");
        let cmd = if cfg!(windows) {
            "set & echo HOST_OK"
        } else {
            "env && echo HOST_OK"
        };

        let res = exec_sandbox_exec(
            &json!({ "command": cmd, "engine": "host" }),
            &cfg,
            SandboxMode::FullSandbox,
        )
        .await
        .unwrap();
        assert_eq!(res["engine"], "host");
        assert_eq!(res["exit_code"], 0);
        let stdout = res["stdout"].as_str().unwrap();
        assert!(stdout.contains("HOST_OK"));
        assert!(!stdout.contains("PS5_AGENT_HOST_SECRET_ENV"));
        assert!(!stdout.contains("leaked_secret_value"));
    }
}
