use chrono::{DateTime, NaiveDate, Utc};
use ps5_agent_core::{AgentError, BrainFileDetail, BrainFileSummary, Result};
use serde_json::{json, Value};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// Maximum size of a workspace brain / markdown memory file (512 KiB).
pub const MAX_BRAIN_FILE_BYTES: usize = 512 * 1024;

/// Maximum per-file body budget when assembling `<brain>` prompt context on large limits.
const MAX_PER_FILE_PROMPT_CHARS: usize = 4096;

/// Workspace subdirectories ensured during brain seeding.
const WORKSPACE_SUBDIRS: &[&str] = &[
    "sandbox",
    "sandbox/downloads",
    "sandbox/snapshots",
    "sandbox/tmp",
    "artifacts",
    "memory",
    ".agents",
    ".agents/rules",
    ".agents/workflows",
];

/// Canonical root brain files recognized in `<workspace_root>`.
const ROOT_BRAIN_FILES: &[(&str, &str)] = &[
    (
        "AGENTS.md",
        "Workspace operating instructions, role boundaries, and coding conventions for agents.",
    ),
    (
        "SOUL.md",
        "Agent persona, communication style, tone, and behavioral principles.",
    ),
    (
        "IDENTITY.md",
        "Assistant identity, environment context, and PS5 runtime profile.",
    ),
    (
        "USER.md",
        "User preferences, hardware notes, and workflow context.",
    ),
    (
        "TOOLS.md",
        "Guidance on workspace tools, sandbox rules, and PS5LM usage.",
    ),
    (
        "MEMORY.md",
        "Curated long-term markdown notes persisted across sessions.",
    ),
    (
        "HEARTBEAT.md",
        "Actionable periodic tasks (comment-only lines starting with '#' are ignored).",
    ),
];

static TEMP_FILE_SEQ: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, PartialEq, Eq)]
enum BrainTarget {
    Root(&'static str),
    DailyMemory(String),
    Rule(String),
    Workflow(String),
}

impl BrainTarget {
    fn canonical_rel_path(&self) -> String {
        match self {
            Self::Root(name) => (*name).to_string(),
            Self::DailyMemory(file) => format!("memory/{file}"),
            Self::Rule(file) => format!(".agents/rules/{file}"),
            Self::Workflow(file) => format!(".agents/workflows/{file}"),
        }
    }

    fn relative_segments(&self) -> Vec<&str> {
        match self {
            Self::Root(name) => vec![*name],
            Self::DailyMemory(file) => vec!["memory", file.as_str()],
            Self::Rule(file) => vec![".agents", "rules", file.as_str()],
            Self::Workflow(file) => vec![".agents", "workflows", file.as_str()],
        }
    }
}

fn default_template_for(rel_path: &str) -> Option<&'static str> {
    match rel_path {
        "AGENTS.md" => Some(
            "# Workspace Agent Instructions\n\n\
             - Operate strictly inside the configured workspace sandbox and never mutate host or PS5 system directories.\n\
             - Prefer small, reversible edits, read files before modifying them, and verify work before reporting completion.\n\
             - Keep responses concise, factual, and explicit about files created or modified.\n",
        ),
        "SOUL.md" => Some(
            "# Agent Soul & Style\n\n\
             - Direct, technical, and transparent.\n\
             - Report only directly observed outputs, file paths, and tool results.\n\
             - Ask for clarification instead of guessing when requirements conflict.\n",
        ),
        "IDENTITY.md" => Some(
            "# Agent Identity\n\n\
             - **Runtime:** PS5 Agent (Jailbroken PS5 FW 13.40 + PS5LM local inference)\n\
             - **Workspace Role:** Autonomous coding, research, and automation assistant\n",
        ),
        "USER.md" => Some(
            "# User Context & Preferences\n\n\
             - Record preferred coding languages, project conventions, and PS5 deployment targets here.\n",
        ),
        "TOOLS.md" => Some(
            "# Workspace Tools Guide\n\n\
             - Use file and patch tools for workspace code inside `artifacts/` and project subdirectories.\n\
             - Store temporary scratch files in `sandbox/tmp/`, fetched assets in `sandbox/downloads/`, and web snapshots in `sandbox/snapshots/`.\n\
             - Use `memory/YYYY-MM-DD.md` and `MEMORY.md` for persistent project facts.\n",
        ),
        "MEMORY.md" => Some(
            "# Long-Term Workspace Memory\n\n\
             - Keep durable architecture decisions, key ports/URLs, and verified findings concise.\n",
        ),
        // Strictly comments/blank lines by default so fresh workspaces do not trigger unsolicited heartbeats.
        "HEARTBEAT.md" => Some(
            "# Workspace Heartbeat Checklist\n\
             #\n\
             # Lines starting with '#' and blank lines are ignored.\n\
             # Add plain-text instructions below when you want periodic heartbeat checks to execute a prompt.\n\
             # Example:\n\
             # - Check server logs and summarize any recent warnings.\n",
        ),
        ".agents/rules/ps5-sandbox.md" => Some(
            "# PS5 Sandbox Safety Rule\n\n\
             - Never access `/system`, `/user`, `/dev`, or symlinks escaping the workspace.\n\
             - Keep generated deliverables inside the workspace and respect approval policies for mutating actions.\n",
        ),
        ".agents/workflows/daily-brief.md" => Some(
            "# Daily Workspace Brief Workflow\n\n\
             Summarize active tasks, recent `memory/` entries, open approvals, and scheduled job status concisely.\n",
        ),
        _ => None,
    }
}

fn is_valid_rule_or_workflow_filename(filename: &str) -> bool {
    let Some(stem) = filename.strip_suffix(".md") else {
        return false;
    };
    if stem.is_empty() || stem.len() > 96 {
        return false;
    }
    let mut chars = stem.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_alphanumeric() {
        return false;
    }
    stem.chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

fn is_valid_daily_memory_filename(filename: &str) -> bool {
    let Some(date_part) = filename.strip_suffix(".md") else {
        return false;
    };
    let bytes = date_part.as_bytes();
    if bytes.len() != 10 {
        return false;
    }
    if bytes[4] != b'-' || bytes[7] != b'-' {
        return false;
    }
    if !bytes
        .iter()
        .enumerate()
        .all(|(idx, &b)| idx == 4 || idx == 7 || b.is_ascii_digit())
    {
        return false;
    }
    NaiveDate::parse_from_str(date_part, "%Y-%m-%d").is_ok()
}

fn reject_common_unsafe_path(raw: &str) -> Result<&str> {
    if raw.contains('\0') {
        return Err(AgentError::InvalidInput(
            "brain path cannot contain NUL byte".into(),
        ));
    }
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(AgentError::InvalidInput(
            "brain path cannot be empty".into(),
        ));
    }
    if trimmed.contains('\\') {
        return Err(AgentError::PermissionDenied(
            "backslashes are forbidden in brain paths".into(),
        ));
    }
    if trimmed.starts_with('/') {
        return Err(AgentError::PermissionDenied(
            "absolute brain paths are forbidden".into(),
        ));
    }
    let b = trimmed.as_bytes();
    if b.len() >= 2 && b[0].is_ascii_alphabetic() && b[1] == b':' {
        return Err(AgentError::PermissionDenied(
            "drive-qualified brain paths are forbidden".into(),
        ));
    }
    if Path::new(trimmed).is_absolute() {
        return Err(AgentError::PermissionDenied(
            "absolute brain paths are forbidden".into(),
        ));
    }
    for comp in Path::new(trimmed).components() {
        match comp {
            Component::Prefix(_) | Component::RootDir | Component::ParentDir => {
                return Err(AgentError::PermissionDenied(
                    "path traversal or root escape is forbidden in brain paths".into(),
                ));
            }
            Component::CurDir | Component::Normal(_) => {}
        }
    }
    for seg in trimmed.split('/') {
        if seg.is_empty() || seg == "." || seg == ".." {
            return Err(AgentError::PermissionDenied(
                "invalid or traversal path segment in brain path".into(),
            ));
        }
    }
    Ok(trimmed)
}

fn parse_brain_target(raw: &str) -> Result<BrainTarget> {
    let clean = reject_common_unsafe_path(raw)?;
    let segs: Vec<&str> = clean.split('/').collect();
    match segs.as_slice() {
        [root_name] => {
            if let Some(&(canonical, _)) = ROOT_BRAIN_FILES
                .iter()
                .find(|&&(name, _)| name == *root_name)
            {
                Ok(BrainTarget::Root(canonical))
            } else {
                Err(AgentError::InvalidInput(format!(
                    "unsupported brain file '{}'; must be a standard root brain file, memory/YYYY-MM-DD.md, .agents/rules/<name>.md, or .agents/workflows/<name>.md",
                    clean
                )))
            }
        }
        ["memory", file] => {
            if is_valid_daily_memory_filename(file) {
                Ok(BrainTarget::DailyMemory((*file).to_string()))
            } else {
                Err(AgentError::InvalidInput(format!(
                    "daily memory file '{}' must match memory/YYYY-MM-DD.md",
                    clean
                )))
            }
        }
        [".agents", "rules", file] => {
            if is_valid_rule_or_workflow_filename(file) {
                Ok(BrainTarget::Rule((*file).to_string()))
            } else {
                Err(AgentError::InvalidInput(format!(
                    "rule file '{}' must match .agents/rules/<name>.md with [A-Za-z0-9_-]",
                    clean
                )))
            }
        }
        [".agents", "workflows", file] => {
            if is_valid_rule_or_workflow_filename(file) {
                Ok(BrainTarget::Workflow((*file).to_string()))
            } else {
                Err(AgentError::InvalidInput(format!(
                    "workflow file '{}' must match .agents/workflows/<name>.md with [A-Za-z0-9_-]",
                    clean
                )))
            }
        }
        _ => Err(AgentError::InvalidInput(format!(
            "brain path '{}' is outside the allowed brain file paths",
            clean
        ))),
    }
}

fn ensure_not_symlink(path: &Path) -> Result<Option<fs::Metadata>> {
    match fs::symlink_metadata(path) {
        Ok(meta) => {
            if meta.file_type().is_symlink() {
                return Err(AgentError::PermissionDenied(format!(
                    "symlinks are forbidden in workspace brain paths: {}",
                    path.display()
                )));
            }
            Ok(Some(meta))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(AgentError::Storage(format!(
            "failed to inspect '{}': {err}",
            path.display()
        ))),
    }
}

fn ensure_safe_workspace_root(workspace_root: &Path, create_if_missing: bool) -> Result<()> {
    match ensure_not_symlink(workspace_root)? {
        Some(meta) => {
            if !meta.is_dir() {
                return Err(AgentError::InvalidInput(format!(
                    "workspace_root '{}' is not a directory",
                    workspace_root.display()
                )));
            }
            Ok(())
        }
        None if create_if_missing => fs::create_dir_all(workspace_root).map_err(|err| {
            AgentError::Storage(format!(
                "failed to create workspace directory '{}': {err}",
                workspace_root.display()
            ))
        }),
        None => Ok(()),
    }
}

fn ensure_safe_subdir(workspace_root: &Path, rel_dir: &str) -> Result<PathBuf> {
    let mut cur = workspace_root.to_path_buf();
    for seg in rel_dir.split('/') {
        if seg.is_empty() {
            continue;
        }
        cur.push(seg);
        match ensure_not_symlink(&cur)? {
            Some(meta) => {
                if !meta.is_dir() {
                    return Err(AgentError::InvalidInput(format!(
                        "expected directory at '{}' but found non-directory",
                        cur.display()
                    )));
                }
            }
            None => {
                fs::create_dir(&cur).map_err(|err| {
                    AgentError::Storage(format!(
                        "failed to create workspace directory '{}': {err}",
                        cur.display()
                    ))
                })?;
            }
        }
    }
    verify_within_canonical_root(workspace_root, &cur)?;
    Ok(cur)
}

fn verify_safe_existing_chain(workspace_root: &Path, segments: &[&str]) -> Result<PathBuf> {
    ensure_safe_workspace_root(workspace_root, false)?;
    let mut cur = workspace_root.to_path_buf();
    for seg in segments {
        cur.push(seg);
        let _ = ensure_not_symlink(&cur)?;
    }
    if cur.exists() {
        verify_within_canonical_root(workspace_root, &cur)?;
    } else if let Some(parent) = cur.parent() {
        if parent.exists() {
            verify_within_canonical_root(workspace_root, parent)?;
        }
    }
    Ok(cur)
}

fn verify_within_canonical_root(workspace_root: &Path, candidate: &Path) -> Result<()> {
    if !workspace_root.exists() || !candidate.exists() {
        return Ok(());
    }
    let canon_root = fs::canonicalize(workspace_root).map_err(|err| {
        AgentError::Storage(format!(
            "failed to canonicalize workspace_root '{}': {err}",
            workspace_root.display()
        ))
    })?;
    let canon_cand = fs::canonicalize(candidate).map_err(|err| {
        AgentError::Storage(format!(
            "failed to canonicalize '{}': {err}",
            candidate.display()
        ))
    })?;
    if !canon_cand.starts_with(&canon_root) {
        return Err(AgentError::PermissionDenied(format!(
            "resolved path '{}' escapes workspace root",
            candidate.display()
        )));
    }
    Ok(())
}

fn atomic_write_file(target: &Path, bytes: &[u8]) -> Result<()> {
    let parent = target.parent().ok_or_else(|| {
        AgentError::InvalidInput(format!("target '{}' has no parent directory", target.display()))
    })?;
    let pid = std::process::id();
    let seq = TEMP_FILE_SEQ.fetch_add(1, Ordering::Relaxed);
    let tmp_name = format!(".brain-tmp-{pid}-{seq}");
    let tmp_path = parent.join(tmp_name);

    {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp_path)
            .map_err(|err| {
                AgentError::Storage(format!(
                    "failed to create temp brain file '{}': {err}",
                    tmp_path.display()
                ))
            })?;
        if let Err(err) = file.write_all(bytes).and_then(|()| file.sync_all()) {
            let _ = fs::remove_file(&tmp_path);
            return Err(AgentError::Storage(format!(
                "failed to write temp brain file '{}': {err}",
                tmp_path.display()
            )));
        }
    }

    if let Err(err) = fs::rename(&tmp_path, target) {
        let _ = fs::remove_file(&tmp_path);
        return Err(AgentError::Storage(format!(
            "failed to commit brain file '{}': {err}",
            target.display()
        )));
    }
    Ok(())
}

fn format_modified_at(meta: &fs::Metadata) -> Option<String> {
    meta.modified()
        .ok()
        .map(|ts| DateTime::<Utc>::from(ts).to_rfc3339())
}

fn read_regular_text_bounded(path: &Path) -> Result<(String, fs::Metadata)> {
    let meta = ensure_not_symlink(path)?.ok_or_else(|| {
        AgentError::NotFound(format!("brain file not found: {}", path.display()))
    })?;
    if !meta.is_file() {
        return Err(AgentError::InvalidInput(format!(
            "brain path '{}' is not a regular file",
            path.display()
        )));
    }
    if meta.len() > MAX_BRAIN_FILE_BYTES as u64 {
        return Err(AgentError::LimitExceeded(format!(
            "brain file '{}' exceeds {} bytes",
            path.display(),
            MAX_BRAIN_FILE_BYTES
        )));
    }
    let content = fs::read_to_string(path).map_err(|err| {
        AgentError::Storage(format!("failed to read '{}': {err}", path.display()))
    })?;
    Ok((content, meta))
}

fn truncate_chars_utf8(input: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }
    match input.char_indices().nth(max_chars) {
        Some((byte_idx, _)) => input[..byte_idx].to_string(),
        None => input.to_string(),
    }
}

/// Ensures workspace runtime directories and non-destructively creates missing default brain files.
///
/// Existing files are never overwritten. Returns the relative paths of files newly created.
pub fn seed_workspace_brain_files(workspace_root: &Path) -> Result<Vec<String>> {
    ensure_safe_workspace_root(workspace_root, true)?;
    for rel_dir in WORKSPACE_SUBDIRS {
        let _ = ensure_safe_subdir(workspace_root, rel_dir)?;
    }

    let seed_targets: &[&str] = &[
        "AGENTS.md",
        "SOUL.md",
        "IDENTITY.md",
        "USER.md",
        "TOOLS.md",
        "MEMORY.md",
        "HEARTBEAT.md",
        ".agents/rules/ps5-sandbox.md",
        ".agents/workflows/daily-brief.md",
    ];

    let mut created = Vec::new();
    for rel_path in seed_targets {
        let target = parse_brain_target(rel_path)?;
        let segs = target.relative_segments();
        let full_path = verify_safe_existing_chain(workspace_root, &segs)?;
        match ensure_not_symlink(&full_path)? {
            Some(meta) => {
                if !meta.is_file() {
                    return Err(AgentError::InvalidInput(format!(
                        "expected regular file at '{}' while seeding brain files",
                        full_path.display()
                    )));
                }
            }
            None => {
                let template = default_template_for(rel_path).ok_or_else(|| {
                    AgentError::Internal(format!("missing default brain template for '{rel_path}'"))
                })?;
                atomic_write_file(&full_path, template.as_bytes())?;
                created.push((*rel_path).to_string());
            }
        }
    }

    Ok(created)
}

fn collect_dir_markdown_entries(
    dir: &Path,
    rel_prefix: &str,
    validator: fn(&str) -> bool,
) -> Result<Vec<(String, fs::Metadata)>> {
    match ensure_not_symlink(dir)? {
        Some(meta) if meta.is_dir() => {}
        _ => return Ok(Vec::new()),
    }

    let rd = fs::read_dir(dir).map_err(|err| {
        AgentError::Storage(format!("failed to read directory '{}': {err}", dir.display()))
    })?;

    let mut entries = Vec::new();
    for entry_res in rd {
        let Ok(entry) = entry_res else {
            continue;
        };
        let file_name = entry.file_name();
        let Some(name_str) = file_name.to_str() else {
            continue;
        };
        if !validator(name_str) {
            continue;
        }
        let path = entry.path();
        if let Ok(Some(meta)) = ensure_not_symlink(&path) {
            if meta.is_file() {
                entries.push((format!("{rel_prefix}/{name_str}"), meta));
            }
        }
    }
    Ok(entries)
}

/// Lists all core root brain files (reporting `exists = false` when absent) plus existing
/// `memory/YYYY-MM-DD.md`, `.agents/rules/*.md`, and `.agents/workflows/*.md` files.
pub fn list_brain_files(workspace_root: &Path) -> Result<Vec<BrainFileSummary>> {
    ensure_safe_workspace_root(workspace_root, false)?;

    let mut out = Vec::new();
    for &(name, description) in ROOT_BRAIN_FILES {
        let path = workspace_root.join(name);
        let summary = match ensure_not_symlink(&path)? {
            Some(meta) if meta.is_file() => {
                verify_within_canonical_root(workspace_root, &path)?;
                BrainFileSummary {
                    name: name.to_string(),
                    path: name.to_string(),
                    exists: true,
                    size_bytes: meta.len(),
                    updated_at: format_modified_at(&meta),
                    description: description.to_string(),
                }
            }
            _ => BrainFileSummary {
                name: name.to_string(),
                path: name.to_string(),
                exists: false,
                size_bytes: 0,
                updated_at: None,
                description: description.to_string(),
            },
        };
        out.push(summary);
    }

    if workspace_root.exists() {
        let mut memory_entries = collect_dir_markdown_entries(
            &workspace_root.join("memory"),
            "memory",
            is_valid_daily_memory_filename,
        )?;
        memory_entries.sort_by(|a, b| b.0.cmp(&a.0));
        for (rel, meta) in memory_entries {
            out.push(BrainFileSummary {
                name: rel.clone(),
                path: rel,
                exists: true,
                size_bytes: meta.len(),
                updated_at: format_modified_at(&meta),
                description: "Daily markdown memory log (YYYY-MM-DD).".to_string(),
            });
        }

        if let Some(meta) = ensure_not_symlink(&workspace_root.join(".agents"))? {
            if meta.is_dir() {
                let mut rule_entries = collect_dir_markdown_entries(
                    &workspace_root.join(".agents").join("rules"),
                    ".agents/rules",
                    is_valid_rule_or_workflow_filename,
                )?;
                rule_entries.sort_by(|a, b| a.0.cmp(&b.0));
                for (rel, fmeta) in rule_entries {
                    out.push(BrainFileSummary {
                        name: rel.clone(),
                        path: rel,
                        exists: true,
                        size_bytes: fmeta.len(),
                        updated_at: format_modified_at(&fmeta),
                        description: "Workspace agent rule injected into prompts.".to_string(),
                    });
                }

                let mut workflow_entries = collect_dir_markdown_entries(
                    &workspace_root.join(".agents").join("workflows"),
                    ".agents/workflows",
                    is_valid_rule_or_workflow_filename,
                )?;
                workflow_entries.sort_by(|a, b| a.0.cmp(&b.0));
                for (rel, fmeta) in workflow_entries {
                    out.push(BrainFileSummary {
                        name: rel.clone(),
                        path: rel,
                        exists: true,
                        size_bytes: fmeta.len(),
                        updated_at: format_modified_at(&fmeta),
                        description: "Workspace markdown slash workflow.".to_string(),
                    });
                }
            }
        }
    }

    Ok(out)
}

/// Reads an allowlisted workspace brain file after strict traversal and symlink checks.
pub fn read_brain_file(workspace_root: &Path, name: &str) -> Result<BrainFileDetail> {
    let target = parse_brain_target(name)?;
    let canonical = target.canonical_rel_path();
    let segs = target.relative_segments();
    let full_path = verify_safe_existing_chain(workspace_root, &segs)?;
    let (content, meta) = read_regular_text_bounded(&full_path)?;
    Ok(BrainFileDetail {
        name: canonical.clone(),
        path: canonical,
        size_bytes: content.len() as u64,
        content,
        updated_at: format_modified_at(&meta),
    })
}

/// Atomically writes an allowlisted workspace brain file (`<= 512 KiB`) via temp file + rename.
pub fn write_brain_file(
    workspace_root: &Path,
    name: &str,
    content: &str,
) -> Result<BrainFileDetail> {
    if content.len() > MAX_BRAIN_FILE_BYTES {
        return Err(AgentError::LimitExceeded(format!(
            "brain file content ({} bytes) exceeds {} byte limit",
            content.len(),
            MAX_BRAIN_FILE_BYTES
        )));
    }
    let target = parse_brain_target(name)?;
    let canonical = target.canonical_rel_path();

    ensure_safe_workspace_root(workspace_root, true)?;
    match &target {
        BrainTarget::Root(_) => {}
        BrainTarget::DailyMemory(_) => {
            let _ = ensure_safe_subdir(workspace_root, "memory")?;
        }
        BrainTarget::Rule(_) => {
            let _ = ensure_safe_subdir(workspace_root, ".agents/rules")?;
        }
        BrainTarget::Workflow(_) => {
            let _ = ensure_safe_subdir(workspace_root, ".agents/workflows")?;
        }
    }

    let segs = target.relative_segments();
    let full_path = verify_safe_existing_chain(workspace_root, &segs)?;
    if let Some(meta) = ensure_not_symlink(&full_path)? {
        if meta.is_dir() {
            return Err(AgentError::InvalidInput(format!(
                "brain path '{}' points to a directory",
                canonical
            )));
        }
    }

    atomic_write_file(&full_path, content.as_bytes())?;
    verify_within_canonical_root(workspace_root, &full_path)?;
    let meta = ensure_not_symlink(&full_path)?.ok_or_else(|| {
        AgentError::Storage(format!(
            "brain file missing immediately after write: {}",
            full_path.display()
        ))
    })?;

    Ok(BrainFileDetail {
        name: canonical.clone(),
        path: canonical,
        size_bytes: content.len() as u64,
        content: content.to_string(),
        updated_at: format_modified_at(&meta),
    })
}

/// Builds a compact, boundary-safe UTF-8 prompt section from existing brain files and `.agents/rules/*.md`.
pub fn build_brain_prompt_section(
    workspace_root: &Path,
    max_total_chars: usize,
    include_user: bool,
) -> String {
    if max_total_chars == 0 || !workspace_root.exists() {
        return String::new();
    }
    if !ensure_safe_workspace_root(workspace_root, false)
        .map(|()| workspace_root.is_dir())
        .unwrap_or(false)
    {
        return String::new();
    }

    let mut candidate_paths: Vec<String> = vec![
        "AGENTS.md".to_string(),
        "SOUL.md".to_string(),
        "IDENTITY.md".to_string(),
        "TOOLS.md".to_string(),
        "MEMORY.md".to_string(),
    ];
    if include_user {
        candidate_paths.push("USER.md".to_string());
    }

    if let Ok(Some(agents_meta)) = ensure_not_symlink(&workspace_root.join(".agents")) {
        if agents_meta.is_dir() {
            if let Ok(mut rules) = collect_dir_markdown_entries(
                &workspace_root.join(".agents").join("rules"),
                ".agents/rules",
                is_valid_rule_or_workflow_filename,
            ) {
                rules.sort_by(|a, b| a.0.cmp(&b.0));
                for (rel, _) in rules {
                    candidate_paths.push(rel);
                }
            }
        }
    }

    let mut sections: Vec<(String, String)> = Vec::new();
    for rel in candidate_paths {
        if let Ok(detail) = read_brain_file(workspace_root, &rel) {
            let trimmed = detail.content.trim();
            if !trimmed.is_empty() {
                sections.push((rel, trimmed.to_string()));
            }
        }
    }

    if sections.is_empty() {
        return String::new();
    }

    let mut out = String::new();
    let mut used_chars = 0usize;
    let total_files = sections.len();

    for (idx, (rel, body)) in sections.into_iter().enumerate() {
        let remaining_total = max_total_chars.saturating_sub(used_chars);
        if remaining_total == 0 {
            break;
        }
        let remaining_files = total_files - idx;
        let fair_share = if remaining_files <= 1 {
            remaining_total
        } else {
            (remaining_total / remaining_files)
                .max(32)
                .min(remaining_total)
        };
        let per_section_cap = fair_share.min(remaining_total).min(MAX_PER_FILE_PROMPT_CHARS);

        let prefix = if out.is_empty() {
            format!("### {rel}\n")
        } else {
            format!("\n\n### {rel}\n")
        };
        let prefix_chars = prefix.chars().count();
        if prefix_chars >= per_section_cap {
            if out.is_empty() {
                return truncate_chars_utf8(&format!("### {rel}\n{body}"), max_total_chars);
            }
            break;
        }

        let body_budget = per_section_cap - prefix_chars;
        let body_chars = body.chars().count();
        let snippet = if body_chars > body_budget {
            let marker = "\n...[truncated]";
            let marker_chars = marker.chars().count();
            if body_budget > marker_chars {
                let mut s = truncate_chars_utf8(&body, body_budget - marker_chars);
                s.push_str(marker);
                s
            } else {
                truncate_chars_utf8(&body, body_budget)
            }
        } else {
            body
        };

        out.push_str(&prefix);
        out.push_str(&snippet);
        used_chars = out.chars().count();
    }

    if out.chars().count() > max_total_chars {
        truncate_chars_utf8(&out, max_total_chars)
    } else {
        out
    }
}

fn is_safe_markdown_memory_filename(filename: &str) -> bool {
    is_valid_daily_memory_filename(filename) || is_valid_rule_or_workflow_filename(filename)
}

fn resolve_markdown_memory_target(workspace_root: &Path, rel_path: &str) -> Result<(String, PathBuf)> {
    let clean = reject_common_unsafe_path(rel_path)?;
    let segs: Vec<&str> = clean.split('/').collect();
    match segs.as_slice() {
        ["MEMORY.md"] => {
            let full = verify_safe_existing_chain(workspace_root, &["MEMORY.md"])?;
            Ok(("MEMORY.md".to_string(), full))
        }
        ["memory", file] if is_safe_markdown_memory_filename(file) => {
            let full = verify_safe_existing_chain(workspace_root, &["memory", file])?;
            Ok((format!("memory/{file}"), full))
        }
        _ => Err(AgentError::InvalidInput(format!(
            "memory path '{}' must be 'MEMORY.md' or 'memory/<name>.md'",
            clean
        ))),
    }
}

/// Searches `MEMORY.md` and `memory/*.md` for query terms, returning ranked JSON snippets
/// shaped as `{ path, line_start, line_end, score, snippet }`.
pub fn search_markdown_memories(
    workspace_root: &Path,
    query: &str,
    max_results: usize,
) -> Result<Vec<Value>> {
    ensure_safe_workspace_root(workspace_root, false)?;
    let trimmed_query = query.trim();
    if trimmed_query.is_empty() || max_results == 0 || !workspace_root.exists() {
        return Ok(Vec::new());
    }

    let capped_results = max_results.min(200);
    let query_lower = trimmed_query.to_lowercase();
    let mut terms: Vec<String> = Vec::new();
    for word in query_lower.split_whitespace() {
        let clean = word.trim_matches(|c: char| !c.is_alphanumeric() && c != '_' && c != '-');
        if !clean.is_empty() && !terms.iter().any(|existing| existing == clean) {
            terms.push(clean.to_string());
        }
    }
    if terms.is_empty() {
        terms.push(query_lower.clone());
    }

    let mut files: Vec<(String, PathBuf)> = Vec::new();
    let root_memory = workspace_root.join("MEMORY.md");
    if let Ok(Some(meta)) = ensure_not_symlink(&root_memory) {
        if meta.is_file() && verify_within_canonical_root(workspace_root, &root_memory).is_ok() {
            files.push(("MEMORY.md".to_string(), root_memory));
        }
    }

    if let Ok(mut daily_or_named) = collect_dir_markdown_entries(
        &workspace_root.join("memory"),
        "memory",
        is_safe_markdown_memory_filename,
    ) {
        daily_or_named.sort_by(|a, b| b.0.cmp(&a.0));
        for (rel, _) in daily_or_named {
            let p = workspace_root.join(&rel);
            files.push((rel, p));
        }
    }

    let mut candidates: Vec<(f64, String, usize, usize, String)> = Vec::new();

    for (rel_path, full_path) in files {
        let Ok((content, _)) = read_regular_text_bounded(&full_path) else {
            continue;
        };
        let lines: Vec<&str> = content.lines().collect();
        if lines.is_empty() {
            continue;
        }

        let mut last_window_end: usize = 0;
        for (idx, line) in lines.iter().enumerate() {
            let lower = line.to_lowercase();
            let phrase_hit = lower.contains(&query_lower);
            let mut matched_terms = 0usize;
            let mut term_occurrences = 0usize;
            for term in &terms {
                let count = lower.matches(term.as_str()).count();
                if count > 0 {
                    matched_terms += 1;
                    term_occurrences += count;
                }
            }

            if !phrase_hit && matched_terms == 0 {
                continue;
            }

            let start_idx = idx.saturating_sub(1);
            let end_idx = (idx + 1).min(lines.len() - 1);
            let line_start = start_idx + 1;
            let line_end = end_idx + 1;

            let score = (if phrase_hit { 10.0 } else { 0.0 })
                + (matched_terms as f64) * 2.0
                + (term_occurrences as f64) * 0.5;

            if line_start <= last_window_end {
                if let Some(prev) = candidates.last_mut() {
                    if prev.1 == rel_path && score > prev.0 {
                        prev.0 = score;
                    }
                    if prev.1 == rel_path && line_end > prev.3 {
                        let existing_start = prev.2;
                        let merged_end = line_end;
                        let snippet_raw = lines[(existing_start - 1)..merged_end].join("\n");
                        prev.3 = merged_end;
                        prev.4 = truncate_chars_utf8(&snippet_raw, 600);
                        last_window_end = merged_end;
                    }
                }
                continue;
            }

            let snippet_raw = lines[start_idx..=end_idx].join("\n");
            let snippet = truncate_chars_utf8(&snippet_raw, 600);
            candidates.push((score, rel_path.clone(), line_start, line_end, snippet));
            last_window_end = line_end;
        }
    }

    candidates.sort_by(|a, b| {
        b.0.partial_cmp(&a.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.1.cmp(&b.1))
            .then_with(|| a.2.cmp(&b.2))
    });
    candidates.truncate(capped_results);

    Ok(candidates
        .into_iter()
        .map(|(score, path, line_start, line_end, snippet)| {
            json!({
                "path": path,
                "line_start": line_start,
                "line_end": line_end,
                "score": score,
                "snippet": snippet,
            })
        })
        .collect())
}

/// Reads a bounded line slice from `MEMORY.md` or `memory/*.md` (1-based `from_line`).
pub fn read_markdown_memory_slice(
    workspace_root: &Path,
    rel_path: &str,
    from_line: Option<usize>,
    lines: Option<usize>,
) -> Result<Value> {
    let (canonical, full_path) = resolve_markdown_memory_target(workspace_root, rel_path)?;
    let (content, _) = read_regular_text_bounded(&full_path)?;
    let all_lines: Vec<&str> = content.lines().collect();
    let total_lines = all_lines.len();

    let start_line = from_line.unwrap_or(1).max(1);
    let max_lines = lines.unwrap_or(50).clamp(1, 500);

    if total_lines == 0 || start_line > total_lines {
        return Ok(json!({
            "path": canonical,
            "line_start": start_line,
            "line_end": total_lines,
            "total_lines": total_lines,
            "content": "",
        }));
    }

    let start_idx = start_line - 1;
    let end_idx_excl = (start_idx + max_lines).min(total_lines);
    let slice = all_lines[start_idx..end_idx_excl].join("\n");

    Ok(json!({
        "path": canonical,
        "line_start": start_line,
        "line_end": end_idx_excl,
        "total_lines": total_lines,
        "content": slice,
    }))
}

/// Returns `Some(prompt)` only when `HEARTBEAT.md` exists cleanly (non-symlink) and contains at
/// least one non-empty line that does not start with `'#'`.
pub fn read_actionable_heartbeat(workspace_root: &Path) -> Option<String> {
    if !workspace_root.exists() {
        return None;
    }
    if ensure_safe_workspace_root(workspace_root, false).is_err() {
        return None;
    }
    let hb_path = workspace_root.join("HEARTBEAT.md");
    if verify_within_canonical_root(workspace_root, &hb_path).is_err() {
        return None;
    }
    let Ok((content, _)) = read_regular_text_bounded(&hb_path) else {
        return None;
    };
    let actionable: Vec<&str> = content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .collect();
    if actionable.is_empty() {
        None
    } else {
        Some(actionable.join("\n"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TempWorkspace {
        path: PathBuf,
    }

    impl TempWorkspace {
        fn new(label: &str) -> Self {
            static TEST_SEQ: AtomicU64 = AtomicU64::new(1);
            let seq = TEST_SEQ.fetch_add(1, Ordering::Relaxed);
            let dir = std::env::temp_dir().join(format!(
                "ps5-agent-brain-{}-{}-{}",
                label,
                std::process::id(),
                seq
            ));
            let _ = fs::remove_dir_all(&dir);
            fs::create_dir_all(&dir).expect("create temp workspace");
            Self { path: dir }
        }
    }

    impl Drop for TempWorkspace {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn seed_creates_dirs_and_preserves_user_edits() {
        let ws = TempWorkspace::new("seed");
        let created_first = seed_workspace_brain_files(&ws.path).expect("initial seed");
        assert_eq!(created_first.len(), 9);
        assert!(created_first.contains(&"AGENTS.md".to_string()));
        assert!(created_first.contains(&".agents/rules/ps5-sandbox.md".to_string()));
        assert!(created_first.contains(&".agents/workflows/daily-brief.md".to_string()));

        for subdir in WORKSPACE_SUBDIRS {
            assert!(
                ws.path.join(subdir).is_dir(),
                "expected directory '{}' to exist",
                subdir
            );
        }

        // Default seeded HEARTBEAT.md only contains comments and blanks.
        assert_eq!(read_actionable_heartbeat(&ws.path), None);

        // User modifies AGENTS.md and writes a daily memory; re-seeding must not overwrite.
        let custom_agents = "# Custom User Rules\nKeep my edits intact!\n";
        write_brain_file(&ws.path, "AGENTS.md", custom_agents).expect("write custom AGENTS.md");
        write_brain_file(
            &ws.path,
            "memory/2026-11-13.md",
            "Deployed ps5-agent ELF to PS5 13.40.",
        )
        .expect("write daily memory");

        let created_second = seed_workspace_brain_files(&ws.path).expect("idempotent re-seed");
        assert!(created_second.is_empty());

        let after_agents = read_brain_file(&ws.path, "AGENTS.md").expect("read AGENTS.md");
        assert_eq!(after_agents.content, custom_agents);

        let listed = list_brain_files(&ws.path).expect("list brain files");
        assert!(listed.iter().any(|f| f.path == "AGENTS.md" && f.exists));
        assert!(listed
            .iter()
            .any(|f| f.path == "memory/2026-11-13.md" && f.exists));
        assert!(listed
            .iter()
            .any(|f| f.path == ".agents/rules/ps5-sandbox.md" && f.exists));
        assert!(listed
            .iter()
            .any(|f| f.path == ".agents/workflows/daily-brief.md" && f.exists));
    }

    #[test]
    fn list_reports_missing_core_files_on_unseeded_workspace() {
        let ws = TempWorkspace::new("unseeded");
        let files = list_brain_files(&ws.path).expect("list empty workspace");
        assert_eq!(files.len(), ROOT_BRAIN_FILES.len());
        assert!(files.iter().all(|f| !f.exists && f.size_bytes == 0));
    }

    #[test]
    fn allowlist_rejects_traversal_backslashes_absolute_and_non_allowlisted_paths() {
        let ws = TempWorkspace::new("allowlist");
        seed_workspace_brain_files(&ws.path).expect("seed");

        let bad_paths = [
            "",
            "   ",
            "../secret.md",
            "memory/../../etc/passwd",
            "memory\\2026-11-13.md",
            "AGENTS.md\\..\\secret.md",
            "/etc/passwd",
            "C:\\Windows\\system.ini",
            "C:AGENTS.md",
            "./AGENTS.md",
            "AGENTS.md/",
            "sandbox/tmp/note.md",
            "memory/not-a-date.md",
            "memory/2026-13-45.md",
            "memory/2026-11-13.txt",
            ".agents/rules/.hidden.md",
            ".agents/rules/bad..name.md",
            ".agents/rules/nested/foo.md",
            ".agents/workflows/run.sh",
            "random.md",
            "a\0b.md",
        ];

        for bad in bad_paths {
            assert!(
                read_brain_file(&ws.path, bad).is_err(),
                "read_brain_file should reject '{bad}'"
            );
            assert!(
                write_brain_file(&ws.path, bad, "x").is_err(),
                "write_brain_file should reject '{bad}'"
            );
        }

        // Valid rule & workflow writes succeed.
        let rule = write_brain_file(
            &ws.path,
            ".agents/rules/custom_rule-1.md",
            "# Rule\nStay in workspace.\n",
        )
        .expect("write rule");
        assert_eq!(rule.path, ".agents/rules/custom_rule-1.md");

        let workflow = write_brain_file(
            &ws.path,
            ".agents/workflows/check-ps5.md",
            "# Workflow\nInspect logs.\n",
        )
        .expect("write workflow");
        assert_eq!(workflow.path, ".agents/workflows/check-ps5.md");

        // Enforce 512 KiB size limit cleanly.
        let oversized = "a".repeat(MAX_BRAIN_FILE_BYTES + 1);
        let err = write_brain_file(&ws.path, "MEMORY.md", &oversized).unwrap_err();
        assert!(matches!(err, AgentError::LimitExceeded(_)));
    }

    #[test]
    fn symlink_rejection_for_brain_files() {
        let ws = TempWorkspace::new("symlink");
        seed_workspace_brain_files(&ws.path).expect("seed");
        let outside = ws.path.join("sandbox").join("outside.txt");
        fs::write(&outside, "outside secret").expect("write outside");

        let target = ws.path.join("AGENTS.md");
        fs::remove_file(&target).expect("remove seeded AGENTS.md");

        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(&outside, &target).expect("create symlink");
            assert!(matches!(
                read_brain_file(&ws.path, "AGENTS.md"),
                Err(AgentError::PermissionDenied(_))
            ));
            assert!(matches!(
                write_brain_file(&ws.path, "AGENTS.md", "new"),
                Err(AgentError::PermissionDenied(_))
            ));
            assert!(seed_workspace_brain_files(&ws.path).is_err());
        }

        #[cfg(windows)]
        {
            match std::os::windows::fs::symlink_file(&outside, &target) {
                Ok(()) => {
                    assert!(matches!(
                        read_brain_file(&ws.path, "AGENTS.md"),
                        Err(AgentError::PermissionDenied(_))
                    ));
                    assert!(matches!(
                        write_brain_file(&ws.path, "AGENTS.md", "new"),
                        Err(AgentError::PermissionDenied(_))
                    ));
                }
                Err(err) if err.raw_os_error() == Some(1314) => {
                    // Unprivileged Windows without Developer Mode cannot create symlinks; skip safely.
                }
                Err(err) => panic!("unexpected symlink_file error: {err}"),
            }
        }
    }

    #[test]
    fn bounded_utf8_prompt_assembly_and_user_inclusion() {
        let ws = TempWorkspace::new("prompt");
        let unicode_body = "🦀 ".repeat(400) + "PS5_MARKER_END";
        write_brain_file(&ws.path, "AGENTS.md", &unicode_body).expect("write AGENTS");
        write_brain_file(&ws.path, "USER.md", "USER_PREFERENCE_XYZ").expect("write USER");
        write_brain_file(&ws.path, ".agents/rules/alpha.md", "ALPHA_RULE_TOKEN")
            .expect("write rule");

        let without_user = build_brain_prompt_section(&ws.path, 350, false);
        assert!(without_user.chars().count() <= 350);
        assert!(without_user.contains("### AGENTS.md"));
        assert!(without_user.contains("ALPHA_RULE_TOKEN"));
        assert!(!without_user.contains("USER_PREFERENCE_XYZ"));

        let with_user = build_brain_prompt_section(&ws.path, 2000, true);
        assert!(with_user.chars().count() <= 2000);
        assert!(with_user.contains("USER_PREFERENCE_XYZ"));
        assert!(with_user.contains("ALPHA_RULE_TOKEN"));

        assert_eq!(build_brain_prompt_section(&ws.path, 0, true), "");
    }

    #[test]
    fn markdown_memory_search_and_slice() {
        let ws = TempWorkspace::new("memory-search");
        write_brain_file(
            &ws.path,
            "MEMORY.md",
            "Line 1: setup notes\nLine 2: PS5LM endpoint is http://127.0.0.1:8081/v1\nLine 3: trailing\n",
        )
        .expect("write MEMORY.md");
        write_brain_file(
            &ws.path,
            "memory/2026-11-13.md",
            "# Daily Log\nVerified Vulkan KV cache warmup on PS5LM.\nCompleted build.\n",
        )
        .expect("write daily memory");

        let results = search_markdown_memories(&ws.path, "PS5LM endpoint", 5).expect("search");
        assert!(!results.is_empty());
        assert_eq!(results[0]["path"], "MEMORY.md");
        assert!(results[0]["score"].as_f64().unwrap() > 10.0);
        assert!(results[0]["line_start"].as_u64().unwrap() >= 1);
        assert!(results[0]["line_end"].as_u64().unwrap() >= results[0]["line_start"].as_u64().unwrap());
        assert!(results[0]["snippet"]
            .as_str()
            .unwrap()
            .contains("http://127.0.0.1:8081/v1"));

        let slice = read_markdown_memory_slice(&ws.path, "memory/2026-11-13.md", Some(2), Some(2))
            .expect("slice");
        assert_eq!(slice["path"], "memory/2026-11-13.md");
        assert_eq!(slice["line_start"], 2);
        assert_eq!(slice["line_end"], 3);
        assert_eq!(
            slice["content"],
            "Verified Vulkan KV cache warmup on PS5LM.\nCompleted build."
        );

        assert!(read_markdown_memory_slice(&ws.path, "AGENTS.md", Some(1), Some(5)).is_err());
        assert!(read_markdown_memory_slice(&ws.path, "../MEMORY.md", Some(1), Some(5)).is_err());
    }

    #[test]
    fn heartbeat_ignores_comments_and_blanks() {
        let ws = TempWorkspace::new("heartbeat");
        assert_eq!(read_actionable_heartbeat(&ws.path), None);

        write_brain_file(
            &ws.path,
            "HEARTBEAT.md",
            "# Heading\n\n   # Indented comment\n   \n",
        )
        .expect("write comment-only heartbeat");
        assert_eq!(read_actionable_heartbeat(&ws.path), None);

        write_brain_file(
            &ws.path,
            "HEARTBEAT.md",
            "# Checklist\n- Verify /health on PS5LM\n\n# Footer\n- Summarize pending tasks\n",
        )
        .expect("write actionable heartbeat");
        assert_eq!(
            read_actionable_heartbeat(&ws.path).as_deref(),
            Some("- Verify /health on PS5LM\n- Summarize pending tasks")
        );
    }
}
