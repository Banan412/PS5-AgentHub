use chrono::{DateTime, Utc};
use ps5_agent_config::ToolsConfig;
use ps5_agent_core::{AgentError, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::fs;
use std::path::{Component, Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkspaceEntryInfo {
    pub name: String,
    pub path: String,
    pub is_dir: bool,
    pub size_bytes: u64,
    pub modified_at: Option<String>,
}

fn reject_unsafe_raw_path(raw: &str) -> Result<()> {
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

/// Normalizes Windows verbatim `\\?\` prefixes so `starts_with` comparisons remain consistent.
fn strip_verbatim_prefix(p: PathBuf) -> PathBuf {
    #[cfg(windows)]
    {
        let s = p.to_string_lossy();
        if let Some(rest) = s.strip_prefix(r"\\?\UNC\") {
            return PathBuf::from(format!(r"\\{rest}"));
        }
        if let Some(rest) = s.strip_prefix(r"\\?\") {
            return PathBuf::from(rest);
        }
    }
    p
}

fn normalize_lexical(p: &Path) -> Result<PathBuf> {
    let mut out = PathBuf::new();
    for comp in p.components() {
        match comp {
            Component::Prefix(pref) => out.push(pref.as_os_str()),
            Component::RootDir => out.push(comp.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                return Err(AgentError::PermissionDenied(
                    "path traversal ('..') is forbidden".into(),
                ));
            }
            Component::Normal(part) => out.push(part),
        }
    }
    Ok(strip_verbatim_prefix(out))
}

#[derive(Debug, Clone)]
struct AllowedRoot {
    canon: PathBuf,
    lexical: PathBuf,
}

fn absolutize_path(raw: &Path) -> Result<PathBuf> {
    if raw.is_absolute() {
        Ok(raw.to_path_buf())
    } else {
        let cwd = std::env::current_dir().map_err(|e| AgentError::Tool {
            tool_id: "file".into(),
            message: format!("failed to resolve current_dir: {e}"),
        })?;
        Ok(cwd.join(raw))
    }
}

fn resolve_deepest_existing_ancestor(abs_lexical: &Path) -> Result<PathBuf> {
    let mut existing = abs_lexical.to_path_buf();
    let mut missing_suffix = Vec::new();
    while !existing.exists() {
        let Some(name) = existing.file_name().map(|n| n.to_os_string()) else {
            return Ok(abs_lexical.to_path_buf());
        };
        missing_suffix.push(name);
        if !existing.pop() {
            return Ok(abs_lexical.to_path_buf());
        }
    }

    let mut base = strip_verbatim_prefix(fs::canonicalize(&existing).map_err(|e| {
        AgentError::Tool {
            tool_id: "file".into(),
            message: format!("failed to canonicalize '{}': {e}", existing.display()),
        }
    })?);
    for seg in missing_suffix.into_iter().rev() {
        base.push(seg);
    }
    Ok(base)
}

fn canonicalize_root_pair(raw_root: &str) -> Result<AllowedRoot> {
    let lexical = normalize_lexical(&absolutize_path(Path::new(raw_root))?)?;
    let canon = resolve_deepest_existing_ancestor(&lexical)?;
    Ok(AllowedRoot { canon, lexical })
}

fn canonicalize_target_and_check_symlinks(
    candidate: &Path,
    allowed_roots: &[AllowedRoot],
) -> Result<(PathBuf, PathBuf)> {
    // First lexically normalize candidate
    let abs_candidate = if candidate.is_absolute() {
        candidate.to_path_buf()
    } else if let Some(first_root) = allowed_roots.first() {
        first_root.canon.join(candidate)
    } else {
        return Err(AgentError::PermissionDenied(
            "no allowed_paths configured".into(),
        ));
    };

    let lexical = normalize_lexical(&abs_candidate)?;

    // Canonicalize deepest existing ancestor so Windows 8.3 short paths (e.g. RUNNER~1)
    // and OS path aliases resolve cleanly before comparing against canonical allowed_roots.
    let resolved = resolve_deepest_existing_ancestor(&lexical)?;

    let matched_root = allowed_roots
        .iter()
        .find(|root| resolved.starts_with(&root.canon))
        .cloned()
        .ok_or_else(|| {
            AgentError::PermissionDenied(format!(
                "path '{}' is outside configured allowed_paths",
                resolved.display()
            ))
        })?;

    // Walk every existing component strictly inside matched_root (both on `lexical`
    // and on `resolved`) and reject any symlink, even one pointing inside the sandbox.
    // Determine skip depth against `matched_root.lexical` when the caller supplied an
    // uncanonicalized alias of the configured root so ancestor symlinks (e.g. `/tmp` ->
    // `/private/tmp`) neither cause false positives nor shift depth and skip inner links.
    let lex_skip_components = if lexical.starts_with(&matched_root.lexical) {
        Some(matched_root.lexical.components().count())
    } else if lexical.starts_with(&matched_root.canon) {
        Some(matched_root.canon.components().count())
    } else {
        None
    };

    let mut lex_cur = PathBuf::new();
    let mut entered_root = false;
    for (idx, comp) in lexical.components().enumerate() {
        lex_cur.push(comp);
        let inside_root = match lex_skip_components {
            Some(skip) => idx >= skip,
            None => entered_root,
        };
        if inside_root {
            if let Ok(meta) = fs::symlink_metadata(&lex_cur) {
                if meta.file_type().is_symlink() {
                    return Err(AgentError::PermissionDenied(format!(
                        "symlinks are forbidden inside sandbox: '{}'",
                        lex_cur.display()
                    )));
                }
            }
        } else if lex_skip_components.is_none() {
            if let Ok(canon_cur) = fs::canonicalize(&lex_cur).map(strip_verbatim_prefix) {
                if canon_cur == matched_root.canon {
                    entered_root = true;
                } else if canon_cur.starts_with(&matched_root.canon) {
                    if let Ok(meta) = fs::symlink_metadata(&lex_cur) {
                        if meta.file_type().is_symlink() {
                            return Err(AgentError::PermissionDenied(format!(
                                "symlinks are forbidden inside sandbox: '{}'",
                                lex_cur.display()
                            )));
                        }
                    }
                    entered_root = true;
                }
            }
        }
    }

    let rel = resolved
        .strip_prefix(&matched_root.canon)
        .map_err(|_| AgentError::PermissionDenied("path escapes allowed root".into()))?;

    let mut cur = matched_root.canon.clone();
    for comp in rel.components() {
        cur.push(comp);
        if let Ok(meta) = fs::symlink_metadata(&cur) {
            if meta.file_type().is_symlink() {
                return Err(AgentError::PermissionDenied(format!(
                    "symlinks are forbidden inside sandbox: '{}'",
                    cur.display()
                )));
            }
        }
    }

    Ok((resolved, matched_root.canon))
}

pub fn resolve_sandboxed_path(
    raw_path: &str,
    cfg: &ToolsConfig,
    must_exist: bool,
) -> Result<(PathBuf, PathBuf)> {
    reject_unsafe_raw_path(raw_path)?;
    if cfg.allowed_paths.is_empty() {
        return Err(AgentError::PermissionDenied(
            "no allowed_paths configured".into(),
        ));
    }

    let mut roots = Vec::with_capacity(cfg.allowed_paths.len());
    for r in &cfg.allowed_paths {
        roots.push(canonicalize_root_pair(r)?);
    }

    let candidate = Path::new(raw_path);
    let (resolved, root) = canonicalize_target_and_check_symlinks(candidate, &roots)?;

    if must_exist && !resolved.exists() {
        return Err(AgentError::NotFound(format!(
            "path not found: {}",
            resolved.display()
        )));
    }

    if let Ok(meta) = fs::symlink_metadata(&resolved) {
        if meta.file_type().is_symlink() {
            return Err(AgentError::PermissionDenied(
                "symlinks are forbidden".into(),
            ));
        }
    }

    Ok((resolved, root))
}

/// Resolves `raw` against `cfg.allowed_paths` without requiring the target path
/// to exist yet, returning `(canonical_root, resolved_target)`.
pub fn resolve_workspace_path_checked(
    raw: &str,
    cfg: &ToolsConfig,
) -> Result<(PathBuf, PathBuf)> {
    let (resolved, root) = resolve_sandboxed_path(raw, cfg, false)?;
    Ok((root, resolved))
}

pub fn exec_list_files(args: &Value, cfg: &ToolsConfig) -> Result<Value> {
    let raw_path = args
        .get("path")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .or_else(|| cfg.allowed_paths.first().map(|s| s.as_str()))
        .ok_or_else(|| AgentError::PermissionDenied("no allowed_paths configured".into()))?;

    let recursive = args
        .get("recursive")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let max_entries = args
        .get("max_entries")
        .and_then(|v| v.as_u64())
        .unwrap_or(200)
        .clamp(1, 2000) as usize;

    let (dir_path, _root) = resolve_sandboxed_path(raw_path, cfg, true)?;
    let meta = fs::symlink_metadata(&dir_path).map_err(|e| AgentError::Tool {
        tool_id: "list_files".into(),
        message: format!("stat failed: {e}"),
    })?;
    if !meta.is_dir() {
        return Err(AgentError::InvalidInput(format!(
            "'{}' is not a directory",
            dir_path.display()
        )));
    }

    let mut entries = Vec::new();
    collect_dir_entries(&dir_path, recursive, max_entries, &mut entries)?;
    Ok(json!({
        "path": dir_path.to_string_lossy(),
        "count": entries.len(),
        "entries": entries,
    }))
}

fn collect_dir_entries(
    dir: &Path,
    recursive: bool,
    max_entries: usize,
    out: &mut Vec<Value>,
) -> Result<()> {
    let rd = fs::read_dir(dir).map_err(|e| AgentError::Tool {
        tool_id: "list_files".into(),
        message: format!("read_dir failed: {e}"),
    })?;

    let mut subdirs = Vec::new();
    for entry_res in rd {
        if out.len() >= max_entries {
            break;
        }
        let Ok(entry) = entry_res else { continue };
        let path = entry.path();
        let Ok(meta) = fs::symlink_metadata(&path) else {
            continue;
        };
        if meta.file_type().is_symlink() {
            continue;
        }
        let is_dir = meta.is_dir();
        out.push(json!({
            "name": entry.file_name().to_string_lossy(),
            "path": path.to_string_lossy(),
            "kind": if is_dir { "dir" } else { "file" },
            "bytes": if is_dir { 0 } else { meta.len() },
        }));
        if recursive && is_dir {
            subdirs.push(path);
        }
    }

    for sd in subdirs {
        if out.len() >= max_entries {
            break;
        }
        collect_dir_entries(&sd, true, max_entries, out)?;
    }

    Ok(())
}

pub fn exec_read_file(args: &Value, cfg: &ToolsConfig) -> Result<Value> {
    let raw_path = args
        .get("path")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AgentError::InvalidInput("missing required string 'path'".into()))?;

    let (file_path, _) = resolve_sandboxed_path(raw_path, cfg, true)?;
    let meta = fs::symlink_metadata(&file_path).map_err(|e| AgentError::Tool {
        tool_id: "read_file".into(),
        message: format!("stat failed: {e}"),
    })?;
    if meta.file_type().is_symlink() {
        return Err(AgentError::PermissionDenied("symlinks are forbidden".into()));
    }
    if !meta.is_file() {
        return Err(AgentError::InvalidInput(format!(
            "'{}' is not a regular file",
            file_path.display()
        )));
    }
    if cfg.max_file_bytes > 0 && meta.len() as usize > cfg.max_file_bytes {
        return Err(AgentError::LimitExceeded(format!(
            "file size ({} bytes) exceeds max_file_bytes ({})",
            meta.len(),
            cfg.max_file_bytes
        )));
    }

    let bytes = fs::read(&file_path).map_err(|e| AgentError::Tool {
        tool_id: "read_file".into(),
        message: format!("failed to read file: {e}"),
    })?;
    if cfg.max_file_bytes > 0 && bytes.len() > cfg.max_file_bytes {
        return Err(AgentError::LimitExceeded(format!(
            "file size ({} bytes) exceeds max_file_bytes ({})",
            bytes.len(),
            cfg.max_file_bytes
        )));
    }

    let text = String::from_utf8(bytes).map_err(|_| {
        AgentError::InvalidInput("file is not valid UTF-8 text".into())
    })?;

    let offset = args.get("offset").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
    let limit = args.get("limit").and_then(|v| v.as_u64()).map(|v| v as usize);

    let sliced = if offset > 0 || limit.is_some() {
        let lines: Vec<&str> = text.lines().collect();
        let start = offset.min(lines.len());
        let end = match limit {
            Some(l) => start.saturating_add(l).min(lines.len()),
            None => lines.len(),
        };
        lines[start..end].join("\n")
    } else {
        text
    };

    Ok(json!({
        "path": file_path.to_string_lossy(),
        "bytes": meta.len(),
        "content": sliced,
    }))
}

pub fn exec_write_file(args: &Value, cfg: &ToolsConfig) -> Result<Value> {
    let raw_path = args
        .get("path")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AgentError::InvalidInput("missing required string 'path'".into()))?;
    let content = args
        .get("content")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AgentError::InvalidInput("missing required string 'content'".into()))?;
    let append = args.get("append").and_then(|v| v.as_bool()).unwrap_or(false);

    if cfg.max_file_bytes > 0 && content.len() > cfg.max_file_bytes {
        return Err(AgentError::LimitExceeded(format!(
            "content length ({} bytes) exceeds max_file_bytes ({})",
            content.len(),
            cfg.max_file_bytes
        )));
    }

    let (file_path, _root) = resolve_sandboxed_path(raw_path, cfg, false)?;
    if let Ok(meta) = fs::symlink_metadata(&file_path) {
        if meta.file_type().is_symlink() {
            return Err(AgentError::PermissionDenied("symlinks are forbidden".into()));
        }
        if meta.is_dir() {
            return Err(AgentError::InvalidInput("cannot overwrite directory as file".into()));
        }
        if append
            && cfg.max_file_bytes > 0
            && (meta.len() as usize).saturating_add(content.len()) > cfg.max_file_bytes
        {
            return Err(AgentError::LimitExceeded(format!(
                "appended file size exceeds max_file_bytes ({})",
                cfg.max_file_bytes
            )));
        }
    }

    if let Some(parent) = file_path.parent() {
        fs::create_dir_all(parent).map_err(|e| AgentError::Tool {
            tool_id: "write_file".into(),
            message: format!("failed to create parent directory: {e}"),
        })?;
    }

    if append {
        use std::io::Write;
        let mut f = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)
            .map_err(|e| AgentError::Tool {
                tool_id: "write_file".into(),
                message: format!("failed to open file for append: {e}"),
            })?;
        f.write_all(content.as_bytes()).map_err(|e| AgentError::Tool {
            tool_id: "write_file".into(),
            message: format!("failed to append file: {e}"),
        })?;
    } else {
        fs::write(&file_path, content.as_bytes()).map_err(|e| AgentError::Tool {
            tool_id: "write_file".into(),
            message: format!("failed to write file: {e}"),
        })?;
    }

    let size_bytes = fs::metadata(&file_path)
        .map(|m| m.len())
        .unwrap_or(content.len() as u64);

    Ok(json!({
        "path": file_path.to_string_lossy(),
        "bytes_written": content.len(),
        "size_bytes": size_bytes,
        "created": true,
        "append": append,
    }))
}

pub fn exec_create_directory(args: &Value, cfg: &ToolsConfig) -> Result<Value> {
    let raw_path = args
        .get("path")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AgentError::InvalidInput("missing required string 'path'".into()))?;

    let (dir_path, _) = resolve_sandboxed_path(raw_path, cfg, false)?;
    fs::create_dir_all(&dir_path).map_err(|e| AgentError::Tool {
        tool_id: "create_directory".into(),
        message: format!("failed to create directory: {e}"),
    })?;

    Ok(json!({
        "path": dir_path.to_string_lossy(),
        "created": true,
    }))
}

pub fn exec_delete_file(args: &Value, cfg: &ToolsConfig) -> Result<Value> {
    let raw_path = args
        .get("path")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AgentError::InvalidInput("missing required string 'path'".into()))?;

    let (file_path, root) = resolve_sandboxed_path(raw_path, cfg, true)?;
    if file_path == root {
        return Err(AgentError::PermissionDenied(
            "cannot delete sandbox root directory".into(),
        ));
    }

    let meta = fs::symlink_metadata(&file_path).map_err(|e| AgentError::Tool {
        tool_id: "delete_file".into(),
        message: format!("stat failed: {e}"),
    })?;
    if meta.file_type().is_symlink() {
        return Err(AgentError::PermissionDenied("symlinks are forbidden".into()));
    }
    if !meta.is_file() {
        return Err(AgentError::InvalidInput(
            "delete_file only removes regular files".into(),
        ));
    }

    fs::remove_file(&file_path).map_err(|e| AgentError::Tool {
        tool_id: "delete_file".into(),
        message: format!("failed to delete file: {e}"),
    })?;

    Ok(json!({
        "path": file_path.to_string_lossy(),
        "deleted": true,
    }))
}

pub fn workspace_root_dir(cfg: &ToolsConfig) -> Result<PathBuf> {
    let first = cfg
        .allowed_paths
        .first()
        .ok_or_else(|| AgentError::PermissionDenied("no allowed_paths configured".into()))?;
    Ok(canonicalize_root_pair(first)?.canon)
}

fn normalize_rel_path(path: &Path, root: &Path) -> String {
    path.strip_prefix(root)
        .ok()
        .map(|rel| {
            rel.components()
                .filter_map(|c| match c {
                    Component::Normal(s) => Some(s.to_string_lossy().into_owned()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("/")
        })
        .unwrap_or_else(|| path.to_string_lossy().into_owned())
}

fn format_modified_at(meta: &fs::Metadata) -> Option<String> {
    meta.modified()
        .ok()
        .map(|st| DateTime::<Utc>::from(st).to_rfc3339())
}

fn ensure_no_symlinks_in_dir(dir: &Path) -> Result<()> {
    let rd = fs::read_dir(dir).map_err(|e| AgentError::Tool {
        tool_id: "file".into(),
        message: format!("read_dir failed: {e}"),
    })?;
    for entry_res in rd {
        let entry = entry_res.map_err(|e| AgentError::Tool {
            tool_id: "file".into(),
            message: format!("read_dir entry failed: {e}"),
        })?;
        let path = entry.path();
        let meta = fs::symlink_metadata(&path).map_err(|e| AgentError::Tool {
            tool_id: "file".into(),
            message: format!("stat failed: {e}"),
        })?;
        if meta.file_type().is_symlink() {
            return Err(AgentError::PermissionDenied(format!(
                "symlinks are forbidden inside sandbox: '{}'",
                path.display()
            )));
        }
        if meta.is_dir() {
            ensure_no_symlinks_in_dir(&path)?;
        }
    }
    Ok(())
}

pub fn list_workspace_entries(
    rel_dir: Option<&str>,
    cfg: &ToolsConfig,
) -> Result<(String, Vec<WorkspaceEntryInfo>)> {
    let trimmed = rel_dir.map(str::trim).filter(|s| !s.is_empty());
    let (dir_path, root) = match trimmed {
        None | Some(".") => {
            let root = workspace_root_dir(cfg)?;
            if !root.exists() {
                return Ok((String::new(), Vec::new()));
            }
            resolve_sandboxed_path(&root.to_string_lossy(), cfg, true)?
        }
        Some(raw) => resolve_sandboxed_path(raw, cfg, true)?,
    };

    let meta = fs::symlink_metadata(&dir_path).map_err(|e| AgentError::Tool {
        tool_id: "file".into(),
        message: format!("stat failed: {e}"),
    })?;
    if meta.file_type().is_symlink() {
        return Err(AgentError::PermissionDenied("symlinks are forbidden".into()));
    }
    if !meta.is_dir() {
        return Err(AgentError::InvalidInput(format!(
            "'{}' is not a directory",
            dir_path.display()
        )));
    }

    let rd = fs::read_dir(&dir_path).map_err(|e| AgentError::Tool {
        tool_id: "file".into(),
        message: format!("read_dir failed: {e}"),
    })?;

    let mut entries = Vec::new();
    for entry_res in rd {
        let Ok(entry) = entry_res else { continue };
        let path = entry.path();
        let Ok(entry_meta) = fs::symlink_metadata(&path) else {
            continue;
        };
        if entry_meta.file_type().is_symlink() {
            continue;
        }
        let is_dir = entry_meta.is_dir();
        entries.push(WorkspaceEntryInfo {
            name: entry.file_name().to_string_lossy().into_owned(),
            path: normalize_rel_path(&path, &root),
            is_dir,
            size_bytes: if is_dir { 0 } else { entry_meta.len() },
            modified_at: format_modified_at(&entry_meta),
        });
    }

    entries.sort_by(|a, b| {
        b.is_dir
            .cmp(&a.is_dir)
            .then_with(|| a.name.to_ascii_lowercase().cmp(&b.name.to_ascii_lowercase()))
            .then_with(|| a.name.cmp(&b.name))
    });

    Ok((normalize_rel_path(&dir_path, &root), entries))
}

pub fn read_workspace_bytes(raw_path: &str, cfg: &ToolsConfig) -> Result<(PathBuf, Vec<u8>)> {
    let (file_path, _) = resolve_sandboxed_path(raw_path, cfg, true)?;
    let meta = fs::symlink_metadata(&file_path).map_err(|e| AgentError::Tool {
        tool_id: "file".into(),
        message: format!("stat failed: {e}"),
    })?;
    if meta.file_type().is_symlink() {
        return Err(AgentError::PermissionDenied("symlinks are forbidden".into()));
    }
    if !meta.is_file() {
        return Err(AgentError::InvalidInput(format!(
            "'{}' is not a regular file",
            file_path.display()
        )));
    }
    if cfg.max_file_bytes > 0 && meta.len() as usize > cfg.max_file_bytes {
        return Err(AgentError::LimitExceeded(format!(
            "file size ({} bytes) exceeds max_file_bytes ({})",
            meta.len(),
            cfg.max_file_bytes
        )));
    }

    let bytes = fs::read(&file_path).map_err(|e| AgentError::Tool {
        tool_id: "file".into(),
        message: format!("failed to read file: {e}"),
    })?;
    if cfg.max_file_bytes > 0 && bytes.len() > cfg.max_file_bytes {
        return Err(AgentError::LimitExceeded(format!(
            "file size ({} bytes) exceeds max_file_bytes ({})",
            bytes.len(),
            cfg.max_file_bytes
        )));
    }

    Ok((file_path, bytes))
}

pub fn write_workspace_bytes(
    raw_path: &str,
    bytes: &[u8],
    cfg: &ToolsConfig,
) -> Result<(PathBuf, usize)> {
    if cfg.max_file_bytes > 0 && bytes.len() > cfg.max_file_bytes {
        return Err(AgentError::LimitExceeded(format!(
            "content length ({} bytes) exceeds max_file_bytes ({})",
            bytes.len(),
            cfg.max_file_bytes
        )));
    }

    let (file_path, _) = resolve_sandboxed_path(raw_path, cfg, false)?;
    if let Ok(meta) = fs::symlink_metadata(&file_path) {
        if meta.file_type().is_symlink() {
            return Err(AgentError::PermissionDenied("symlinks are forbidden".into()));
        }
        if meta.is_dir() {
            return Err(AgentError::InvalidInput(
                "cannot overwrite directory as file".into(),
            ));
        }
    }

    if let Some(parent) = file_path.parent() {
        fs::create_dir_all(parent).map_err(|e| AgentError::Tool {
            tool_id: "file".into(),
            message: format!("failed to create parent directory: {e}"),
        })?;
    }

    fs::write(&file_path, bytes).map_err(|e| AgentError::Tool {
        tool_id: "file".into(),
        message: format!("failed to write file: {e}"),
    })?;

    Ok((file_path, bytes.len()))
}

pub fn create_workspace_dir(raw_path: &str, cfg: &ToolsConfig) -> Result<PathBuf> {
    let (dir_path, _) = resolve_sandboxed_path(raw_path, cfg, false)?;
    if let Ok(meta) = fs::symlink_metadata(&dir_path) {
        if meta.file_type().is_symlink() {
            return Err(AgentError::PermissionDenied("symlinks are forbidden".into()));
        }
        if !meta.is_dir() {
            return Err(AgentError::InvalidInput(format!(
                "'{}' exists and is not a directory",
                dir_path.display()
            )));
        }
    }

    fs::create_dir_all(&dir_path).map_err(|e| AgentError::Tool {
        tool_id: "file".into(),
        message: format!("failed to create directory: {e}"),
    })?;

    Ok(dir_path)
}

pub fn delete_workspace_entry(raw_path: &str, cfg: &ToolsConfig) -> Result<PathBuf> {
    let (target_path, root) = resolve_sandboxed_path(raw_path, cfg, true)?;
    if target_path == root {
        return Err(AgentError::PermissionDenied(
            "cannot delete sandbox root directory".into(),
        ));
    }

    let meta = fs::symlink_metadata(&target_path).map_err(|e| AgentError::Tool {
        tool_id: "file".into(),
        message: format!("stat failed: {e}"),
    })?;
    if meta.file_type().is_symlink() {
        return Err(AgentError::PermissionDenied("symlinks are forbidden".into()));
    }

    if meta.is_dir() {
        ensure_no_symlinks_in_dir(&target_path)?;
        fs::remove_dir_all(&target_path).map_err(|e| AgentError::Tool {
            tool_id: "file".into(),
            message: format!("failed to delete directory: {e}"),
        })?;
    } else if meta.is_file() {
        fs::remove_file(&target_path).map_err(|e| AgentError::Tool {
            tool_id: "file".into(),
            message: format!("failed to delete file: {e}"),
        })?;
    } else {
        return Err(AgentError::InvalidInput(format!(
            "'{}' is neither a regular file nor a directory",
            target_path.display()
        )));
    }

    Ok(target_path)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg_for(root: &Path, max_file_bytes: usize) -> ToolsConfig {
        ToolsConfig {
            allowed_paths: vec![root.to_string_lossy().into_owned()],
            allowed_domains: vec!["*".into()],
            block_private_lan_web_requests: true,
            default_timeout_secs: 10,
            max_file_bytes,
            max_web_bytes: 2048,
            ..ToolsConfig::default()
        }
    }

    #[test]
    fn binary_workspace_helpers_roundtrip_and_guards() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = cfg_for(dir.path(), 64);

        assert_eq!(
            workspace_root_dir(&cfg).unwrap(),
            strip_verbatim_prefix(fs::canonicalize(dir.path()).unwrap())
        );

        let sub = create_workspace_dir("assets/bin", &cfg).unwrap();
        assert!(sub.is_dir());

        let payload = vec![0x00, 0xFF, 0x89, 0x50, 0x4E, 0x47];
        let (_written_path, n) = write_workspace_bytes("assets/bin/logo.png", &payload, &cfg).unwrap();
        assert_eq!(n, payload.len());

        let (_read_path, read_back) = read_workspace_bytes("assets/bin/logo.png", &cfg).unwrap();
        assert_eq!(read_back, payload);

        let (rel, entries) = list_workspace_entries(Some("assets/bin"), &cfg).unwrap();
        assert_eq!(rel, "assets/bin");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "logo.png");
        assert_eq!(entries[0].path, "assets/bin/logo.png");
        assert!(!entries[0].is_dir);
        assert_eq!(entries[0].size_bytes, payload.len() as u64);

        // Enforce max_file_bytes when > 0
        let too_big = vec![1u8; 65];
        assert!(matches!(
            write_workspace_bytes("big.bin", &too_big, &cfg),
            Err(AgentError::LimitExceeded(_))
        ));

        // 0 = unlimited
        let unlimited_cfg = cfg_for(dir.path(), 0);
        let (_p, big_n) = write_workspace_bytes("big.bin", &too_big, &unlimited_cfg).unwrap();
        assert_eq!(big_n, 65);

        // Root is not deletable
        assert!(matches!(
            delete_workspace_entry(".", &cfg),
            Err(AgentError::PermissionDenied(_))
        ));

        // Delete file & directory
        delete_workspace_entry("assets/bin/logo.png", &cfg).unwrap();
        delete_workspace_entry("assets", &cfg).unwrap();
        let (_root_rel, root_entries) = list_workspace_entries(None, &cfg).unwrap();
        assert_eq!(root_entries.len(), 1);
        assert_eq!(root_entries[0].name, "big.bin");
    }

    #[cfg(windows)]
    #[test]
    fn rejects_windows_drive_relative_paths() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = cfg_for(dir.path(), 1024);
        assert!(matches!(
            resolve_sandboxed_path("C:foo.txt", &cfg, false),
            Err(AgentError::PermissionDenied(_))
        ));
        assert!(matches!(
            resolve_sandboxed_path("D:", &cfg, false),
            Err(AgentError::PermissionDenied(_))
        ));
    }

    #[test]
    fn resolve_workspace_path_checked_returns_root_then_target_and_handles_missing_root() {
        let dir = tempfile::tempdir().unwrap();
        let missing_root = dir.path().join("future_workspace").join("nested");
        let cfg = ToolsConfig {
            allowed_paths: vec![missing_root.to_string_lossy().into_owned()],
            ..ToolsConfig::default()
        };

        let expected_root = strip_verbatim_prefix(fs::canonicalize(dir.path()).unwrap())
            .join("future_workspace")
            .join("nested");

        let (root, target) = resolve_workspace_path_checked("artifacts/out.md", &cfg).unwrap();
        assert_eq!(root, expected_root);
        assert_eq!(target, expected_root.join("artifacts").join("out.md"));

        let (dot_root, dot_target) = resolve_workspace_path_checked(".", &cfg).unwrap();
        assert_eq!(dot_root, expected_root);
        assert_eq!(dot_target, expected_root);

        assert!(matches!(
            resolve_workspace_path_checked("../escape.txt", &cfg),
            Err(AgentError::PermissionDenied(_))
        ));
    }

    #[cfg(unix)]
    #[test]
    fn lexical_symlinked_root_alias_still_blocks_inner_symlinks() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let deep = dir.path().join("a").join("b").join("c");
        let real_ws = deep.join("workspace");
        let inner_real = real_ws.join("real.txt");
        fs::create_dir_all(&real_ws).unwrap();
        fs::write(&inner_real, b"ok").unwrap();

        let alias_ws = dir.path().join("ws_alias");
        symlink(&real_ws, &alias_ws).unwrap();

        let inner_link = real_ws.join("evil_link.txt");
        symlink(&inner_real, &inner_link).unwrap();

        let cfg = cfg_for(&alias_ws, 1024);
        let abs_via_alias = alias_ws.join("evil_link.txt");
        assert!(matches!(
            resolve_workspace_path_checked(&abs_via_alias.to_string_lossy(), &cfg),
            Err(AgentError::PermissionDenied(_))
        ));
    }
}
