use crate::files::resolve_workspace_path_checked;
use ps5_agent_config::ToolsConfig;
use ps5_agent_core::{AgentError, Result};
use regex::RegexBuilder;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use uuid::Uuid;

const DEFAULT_MAX_FILE_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_GREP_MAX_RESULTS: u64 = 50;
const MAX_GREP_RESULTS: u64 = 200;
const DEFAULT_GLOB_MAX_RESULTS: u64 = 100;
const MAX_GLOB_RESULTS: u64 = 500;
const MAX_GREP_LINE_CHARS: usize = 4096;

fn effective_max_file_bytes(cfg: &ToolsConfig) -> usize {
    if cfg.max_file_bytes == 0 {
        DEFAULT_MAX_FILE_BYTES
    } else {
        cfg.max_file_bytes
    }
}

fn normalize_rel_path(path: &Path, root: &Path) -> String {
    let rel = path.strip_prefix(root).unwrap_or(path);
    let parts: Vec<String> = rel
        .components()
        .filter_map(|c| match c {
            Component::Normal(s) => Some(s.to_string_lossy().into_owned()),
            _ => None,
        })
        .collect();
    if parts.is_empty() {
        ".".to_string()
    } else {
        parts.join("/")
    }
}

fn ensure_no_symlink_components_under_root(root: &Path, target: &Path) -> Result<()> {
    let rel = target.strip_prefix(root).map_err(|_| {
        AgentError::PermissionDenied(format!(
            "path '{}' escapes workspace root '{}'",
            target.display(),
            root.display()
        ))
    })?;

    let mut cur = root.to_path_buf();
    for comp in rel.components() {
        cur.push(comp);
        if let Ok(meta) = fs::symlink_metadata(&cur) {
            if meta.file_type().is_symlink() {
                return Err(AgentError::PermissionDenied(format!(
                    "symlinks are forbidden inside workspace: '{}'",
                    cur.display()
                )));
            }
        }
    }
    Ok(())
}

fn read_existing_utf8_file(
    target: &Path,
    root: &Path,
    tool_id: &str,
    max_bytes: usize,
) -> Result<String> {
    if target == root {
        return Err(AgentError::PermissionDenied(
            "cannot operate on workspace root directory as a file".into(),
        ));
    }
    ensure_no_symlink_components_under_root(root, target)?;

    let meta = fs::symlink_metadata(target).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            AgentError::NotFound(format!("file not found: {}", normalize_rel_path(target, root)))
        } else {
            AgentError::Tool {
                tool_id: tool_id.into(),
                message: format!("stat failed for '{}': {e}", target.display()),
            }
        }
    })?;

    if meta.file_type().is_symlink() {
        return Err(AgentError::PermissionDenied("symlinks are forbidden".into()));
    }
    if !meta.is_file() {
        return Err(AgentError::InvalidInput(format!(
            "'{}' is not a regular file",
            normalize_rel_path(target, root)
        )));
    }
    if meta.len() > max_bytes as u64 {
        return Err(AgentError::LimitExceeded(format!(
            "file '{}' ({} bytes) exceeds max_file_bytes ({})",
            normalize_rel_path(target, root),
            meta.len(),
            max_bytes
        )));
    }

    let bytes = fs::read(target).map_err(|e| AgentError::Tool {
        tool_id: tool_id.into(),
        message: format!("failed to read '{}': {e}", target.display()),
    })?;
    if bytes.len() > max_bytes {
        return Err(AgentError::LimitExceeded(format!(
            "file '{}' ({} bytes) exceeds max_file_bytes ({})",
            normalize_rel_path(target, root),
            bytes.len(),
            max_bytes
        )));
    }

    String::from_utf8(bytes).map_err(|_| {
        AgentError::InvalidInput(format!(
            "file '{}' is not valid UTF-8 text",
            normalize_rel_path(target, root)
        ))
    })
}

fn ensure_dir_tree_no_symlink(root: &Path, dir: &Path, tool_id: &str) -> Result<()> {
    let rel = dir.strip_prefix(root).map_err(|_| {
        AgentError::PermissionDenied(format!(
            "directory '{}' escapes workspace root",
            dir.display()
        ))
    })?;

    if !root.exists() {
        fs::create_dir_all(root).map_err(|e| AgentError::Tool {
            tool_id: tool_id.into(),
            message: format!("failed to create workspace root: {e}"),
        })?;
    }

    let mut cur = root.to_path_buf();
    for comp in rel.components() {
        if let Component::Normal(part) = comp {
            cur.push(part);
            match fs::symlink_metadata(&cur) {
                Ok(meta) => {
                    if meta.file_type().is_symlink() {
                        return Err(AgentError::PermissionDenied(format!(
                            "symlinks are forbidden inside workspace: '{}'",
                            cur.display()
                        )));
                    }
                    if !meta.is_dir() {
                        return Err(AgentError::InvalidInput(format!(
                            "parent path '{}' exists and is not a directory",
                            normalize_rel_path(&cur, root)
                        )));
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    fs::create_dir(&cur).map_err(|err| AgentError::Tool {
                        tool_id: tool_id.into(),
                        message: format!("failed to create directory '{}': {err}", cur.display()),
                    })?;
                }
                Err(e) => {
                    return Err(AgentError::Tool {
                        tool_id: tool_id.into(),
                        message: format!("stat failed for '{}': {e}", cur.display()),
                    });
                }
            }
        }
    }
    Ok(())
}

fn atomic_write_file(
    target: &Path,
    root: &Path,
    bytes: &[u8],
    max_bytes: usize,
    tool_id: &str,
) -> Result<()> {
    if bytes.len() > max_bytes {
        return Err(AgentError::LimitExceeded(format!(
            "content size ({} bytes) exceeds max_file_bytes ({})",
            bytes.len(),
            max_bytes
        )));
    }
    if target == root {
        return Err(AgentError::PermissionDenied(
            "cannot overwrite workspace root directory".into(),
        ));
    }
    ensure_no_symlink_components_under_root(root, target)?;

    #[cfg(unix)]
    let existing_perms = match fs::symlink_metadata(target) {
        Ok(meta) => {
            if meta.file_type().is_symlink() {
                return Err(AgentError::PermissionDenied("symlinks are forbidden".into()));
            }
            if meta.is_dir() {
                return Err(AgentError::InvalidInput(format!(
                    "'{}' is a directory, not a regular file",
                    normalize_rel_path(target, root)
                )));
            }
            Some(meta.permissions())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => {
            return Err(AgentError::Tool {
                tool_id: tool_id.into(),
                message: format!("stat failed for '{}': {e}", target.display()),
            });
        }
    };
    #[cfg(not(unix))]
    match fs::symlink_metadata(target) {
        Ok(meta) => {
            if meta.file_type().is_symlink() {
                return Err(AgentError::PermissionDenied("symlinks are forbidden".into()));
            }
            if meta.is_dir() {
                return Err(AgentError::InvalidInput(format!(
                    "'{}' is a directory, not a regular file",
                    normalize_rel_path(target, root)
                )));
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(AgentError::Tool {
                tool_id: tool_id.into(),
                message: format!("stat failed for '{}': {e}", target.display()),
            });
        }
    }

    let parent = target.parent().ok_or_else(|| {
        AgentError::InvalidInput(format!(
            "target '{}' has no parent directory",
            target.display()
        ))
    })?;
    ensure_dir_tree_no_symlink(root, parent, tool_id)?;

    let tmp_path = parent.join(format!(".ps5patch.{}.tmp", Uuid::new_v4().simple()));
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp_path)
        .map_err(|e| AgentError::Tool {
            tool_id: tool_id.into(),
            message: format!("failed to create temp file '{}': {e}", tmp_path.display()),
        })?;

    if let Err(e) = file.write_all(bytes).and_then(|()| file.sync_all()) {
        drop(file);
        let _ = fs::remove_file(&tmp_path);
        return Err(AgentError::Tool {
            tool_id: tool_id.into(),
            message: format!("failed to write temp file '{}': {e}", tmp_path.display()),
        });
    }
    drop(file);

    #[cfg(unix)]
    if let Some(perms) = existing_perms {
        let _ = fs::set_permissions(&tmp_path, perms);
    }

    // Re-check target right before rename so a concurrent symlink swap cannot be overwritten.
    if let Ok(meta) = fs::symlink_metadata(target) {
        if meta.file_type().is_symlink() {
            let _ = fs::remove_file(&tmp_path);
            return Err(AgentError::PermissionDenied("symlinks are forbidden".into()));
        }
        if meta.is_dir() {
            let _ = fs::remove_file(&tmp_path);
            return Err(AgentError::InvalidInput("cannot overwrite directory as file".into()));
        }
    }

    if let Err(e) = fs::rename(&tmp_path, target) {
        let _ = fs::remove_file(&tmp_path);
        return Err(AgentError::Tool {
            tool_id: tool_id.into(),
            message: format!(
                "failed to atomically replace '{}': {e}",
                normalize_rel_path(target, root)
            ),
        });
    }

    Ok(())
}

fn has_bare_lf(s: &str) -> bool {
    let bytes = s.as_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        if b == b'\n' && (i == 0 || bytes[i - 1] != b'\r') {
            return true;
        }
    }
    false
}

/// Replace occurrences of `old_text` with `new_text` in an existing workspace file.
///
/// Enforces workspace confinement, `!is_symlink()`, `max_file_bytes` (`0` = 16 MiB),
/// unique match when `replace_all` is `false`, and atomic temp-file + rename persistence.
pub fn exec_edit_file(args: &Value, cfg: &ToolsConfig) -> Result<Value> {
    let raw_path = args
        .get("path")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AgentError::InvalidInput("missing required string 'path'".into()))?;
    let old_text = args
        .get("old_text")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AgentError::InvalidInput("missing required string 'old_text'".into()))?;
    let new_text = args
        .get("new_text")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AgentError::InvalidInput("missing required string 'new_text'".into()))?;
    let replace_all = args
        .get("replace_all")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    if old_text.is_empty() {
        return Err(AgentError::InvalidInput("old_text cannot be empty".into()));
    }

    let max_bytes = effective_max_file_bytes(cfg);
    let (root, target) = resolve_workspace_path_checked(raw_path, cfg)?;
    let rel_path = normalize_rel_path(&target, &root);
    let original = read_existing_utf8_file(&target, &root, "edit_file", max_bytes)?;

    let exact_count = original.match_indices(old_text).count();
    let (updated, replacements) = if exact_count > 0 {
        if !replace_all && exact_count != 1 {
            return Err(AgentError::InvalidInput(format!(
                "old_text matched {exact_count} times in '{rel_path}'; set replace_all=true or provide more context for a unique match"
            )));
        }
        (original.replace(old_text, new_text), exact_count)
    } else if original.contains("\r\n") || old_text.contains("\r\n") {
        let norm_orig = original.replace("\r\n", "\n");
        let norm_old = old_text.replace("\r\n", "\n");
        let norm_new = new_text.replace("\r\n", "\n");
        let norm_count = norm_orig.match_indices(&norm_old).count();
        if norm_count == 0 {
            return Err(AgentError::InvalidInput(format!(
                "old_text not found in '{rel_path}'"
            )));
        }
        if !replace_all && norm_count != 1 {
            return Err(AgentError::InvalidInput(format!(
                "old_text matched {norm_count} times in '{rel_path}'; set replace_all=true or provide more context for a unique match"
            )));
        }
        let replaced_lf = norm_orig.replace(&norm_old, &norm_new);
        let preserved = if original.contains("\r\n") && !has_bare_lf(&original) {
            replaced_lf.replace('\n', "\r\n")
        } else {
            replaced_lf
        };
        (preserved, norm_count)
    } else {
        return Err(AgentError::InvalidInput(format!(
            "old_text not found in '{rel_path}'"
        )));
    };

    if updated.len() > max_bytes {
        return Err(AgentError::LimitExceeded(format!(
            "edited file size ({} bytes) exceeds max_file_bytes ({})",
            updated.len(),
            max_bytes
        )));
    }

    atomic_write_file(&target, &root, updated.as_bytes(), max_bytes, "edit_file")?;

    Ok(json!({
        "path": rel_path,
        "replacements": replacements,
        "bytes_written": updated.len(),
        "edited": true,
    }))
}

#[derive(Debug, Clone)]
struct PatchHunk {
    anchor: Option<String>,
    /// 0-based hint only when `old_count == 0`, else 1-based line hint from `@@ -L,C`.
    old_start_hint: Option<usize>,
    old_count_zero: bool,
    old_chunk: Vec<String>,
    new_chunk: Vec<String>,
    end_of_file: bool,
    new_no_trailing_nl: Option<bool>,
}

#[derive(Debug, Clone)]
enum ParsedPatchOp {
    Add {
        raw_path: String,
        content: String,
    },
    Delete {
        raw_path: String,
        expected_lines: Option<Vec<String>>,
    },
    Update {
        raw_path: String,
        move_to: Option<String>,
        hunks: Vec<PatchHunk>,
    },
}

fn parse_unified_range(token: &str, prefix: char) -> Option<(usize, usize)> {
    let rest = token.strip_prefix(prefix)?;
    if let Some((start_s, count_s)) = rest.split_once(',') {
        let start = start_s.parse::<usize>().ok()?;
        let count = count_s.parse::<usize>().ok()?;
        Some((start, count))
    } else {
        let start = rest.parse::<usize>().ok()?;
        Some((start, 1))
    }
}

fn parse_hunk_header_line(line: &str) -> Result<(Option<usize>, bool, Option<(usize, usize)>, Option<String>)> {
    let after_open = line
        .strip_prefix("@@")
        .ok_or_else(|| AgentError::InvalidInput(format!("expected '@@' hunk header, got '{line}'")))?;
    let trimmed = after_open.trim();
    if trimmed.is_empty() {
        return Ok((None, false, None, None));
    }

    // Attempt standard unified hunk header: -L(,C)? +L(,C)? @@( section)?
    if let Some(end_idx) = after_open.find("@@") {
        let range_part = after_open[..end_idx].trim();
        let section_part = after_open[end_idx + 2..].trim();
        let mut parts = range_part.split_whitespace();
        if let (Some(old_tok), Some(new_tok), None) = (parts.next(), parts.next(), parts.next()) {
            if let (Some((old_start, old_count)), Some((_new_start, new_count))) = (
                parse_unified_range(old_tok, '-'),
                parse_unified_range(new_tok, '+'),
            ) {
                let anchor = if section_part.is_empty() {
                    None
                } else {
                    Some(section_part.to_string())
                };
                return Ok((
                    Some(old_start),
                    old_count == 0,
                    Some((old_count, new_count)),
                    anchor,
                ));
            }
        }
    }

    Ok((None, false, None, Some(trimmed.to_string())))
}

fn parse_codex_patch(lines: &[&str]) -> Result<Vec<ParsedPatchOp>> {
    let mut i = 0usize;
    while i < lines.len() && lines[i].trim().is_empty() {
        i += 1;
    }
    if i >= lines.len() || lines[i].trim() != "*** Begin Patch" {
        return Err(AgentError::InvalidInput(
            "Codex patch must begin with '*** Begin Patch'".into(),
        ));
    }
    i += 1;

    let mut ops = Vec::new();
    let mut found_end = false;

    while i < lines.len() {
        let line = lines[i];
        let trimmed = line.trim();
        if trimmed == "*** End Patch" {
            found_end = true;
            i += 1;
            break;
        }
        if trimmed.is_empty() {
            i += 1;
            continue;
        }

        if let Some(path) = line.strip_prefix("*** Add File:") {
            let raw_path = path.trim();
            if raw_path.is_empty() {
                return Err(AgentError::InvalidInput("empty path in '*** Add File:'".into()));
            }
            i += 1;
            let mut added_lines = Vec::new();
            let mut no_trailing_nl = false;
            while i < lines.len() {
                let cur = lines[i];
                if cur.starts_with("*** ") {
                    break;
                }
                if cur == "\\ No newline at end of file" {
                    no_trailing_nl = true;
                    i += 1;
                    continue;
                }
                let Some(body) = cur.strip_prefix('+') else {
                    return Err(AgentError::InvalidInput(format!(
                        "invalid line inside '*** Add File: {raw_path}': expected '+' prefix, got '{cur}'"
                    )));
                };
                added_lines.push(body.to_string());
                i += 1;
            }
            let mut content = added_lines.join("\n");
            if !added_lines.is_empty() && !no_trailing_nl {
                content.push('\n');
            }
            ops.push(ParsedPatchOp::Add {
                raw_path: raw_path.to_string(),
                content,
            });
            continue;
        }

        if let Some(path) = line.strip_prefix("*** Delete File:") {
            let raw_path = path.trim();
            if raw_path.is_empty() {
                return Err(AgentError::InvalidInput("empty path in '*** Delete File:'".into()));
            }
            i += 1;
            while i < lines.len() && lines[i].trim().is_empty() {
                i += 1;
            }
            if i < lines.len() && !lines[i].starts_with("*** ") {
                return Err(AgentError::InvalidInput(format!(
                    "unexpected content after '*** Delete File: {raw_path}': '{}'",
                    lines[i]
                )));
            }
            ops.push(ParsedPatchOp::Delete {
                raw_path: raw_path.to_string(),
                expected_lines: None,
            });
            continue;
        }

        if let Some(path) = line.strip_prefix("*** Update File:") {
            let raw_path = path.trim();
            if raw_path.is_empty() {
                return Err(AgentError::InvalidInput("empty path in '*** Update File:'".into()));
            }
            i += 1;

            let move_to = if i < lines.len() {
                if let Some(dst) = lines[i].strip_prefix("*** Move to:") {
                    let dst_trimmed = dst.trim();
                    if dst_trimmed.is_empty() {
                        return Err(AgentError::InvalidInput("empty path in '*** Move to:'".into()));
                    }
                    i += 1;
                    Some(dst_trimmed.to_string())
                } else {
                    None
                }
            } else {
                None
            };

            let mut hunks = Vec::new();
            while i < lines.len() {
                let cur = lines[i];
                if cur.starts_with("*** ") {
                    if cur.starts_with("*** Move to:") {
                        return Err(AgentError::InvalidInput(
                            "'*** Move to:' must appear immediately after '*** Update File:'".into(),
                        ));
                    }
                    break;
                }
                if cur.trim().is_empty() {
                    i += 1;
                    continue;
                }
                if !cur.starts_with("@@") {
                    return Err(AgentError::InvalidInput(format!(
                        "expected '@@' hunk header inside '*** Update File: {raw_path}', got '{cur}'"
                    )));
                }
                let (old_start_hint, old_count_zero, _counts, anchor) = parse_hunk_header_line(cur)?;
                i += 1;

                let mut old_chunk = Vec::new();
                let mut new_chunk = Vec::new();
                let mut end_of_file = false;
                let mut new_no_trailing_nl = None;
                let mut last_tag: Option<char> = None;

                while i < lines.len() {
                    let hl = lines[i];
                    if hl.trim() == "*** End of File" {
                        end_of_file = true;
                        i += 1;
                        break;
                    }
                    if hl.starts_with("*** ") || hl.starts_with("@@") {
                        break;
                    }
                    if hl == "\\ No newline at end of file" {
                        if matches!(last_tag, Some('+' | ' ')) {
                            new_no_trailing_nl = Some(true);
                        }
                        i += 1;
                        continue;
                    }
                    if hl.is_empty() {
                        // Tolerate editors trimming the leading space from a blank context line.
                        old_chunk.push(String::new());
                        new_chunk.push(String::new());
                        last_tag = Some(' ');
                    } else if let Some(rest) = hl.strip_prefix(' ') {
                        old_chunk.push(rest.to_string());
                        new_chunk.push(rest.to_string());
                        last_tag = Some(' ');
                    } else if let Some(rest) = hl.strip_prefix('-') {
                        old_chunk.push(rest.to_string());
                        last_tag = Some('-');
                    } else if let Some(rest) = hl.strip_prefix('+') {
                        new_chunk.push(rest.to_string());
                        last_tag = Some('+');
                    } else {
                        return Err(AgentError::InvalidInput(format!(
                            "invalid hunk line in '*** Update File: {raw_path}': '{hl}'"
                        )));
                    }
                    i += 1;
                }

                if old_chunk.is_empty() && new_chunk.is_empty() {
                    return Err(AgentError::InvalidInput(format!(
                        "empty hunk in '*** Update File: {raw_path}'"
                    )));
                }
                hunks.push(PatchHunk {
                    anchor,
                    old_start_hint,
                    old_count_zero,
                    old_chunk,
                    new_chunk,
                    end_of_file,
                    new_no_trailing_nl,
                });
            }

            if hunks.is_empty() && move_to.is_none() {
                return Err(AgentError::InvalidInput(format!(
                    "'*** Update File: {raw_path}' contains no hunks or move target"
                )));
            }

            ops.push(ParsedPatchOp::Update {
                raw_path: raw_path.to_string(),
                move_to,
                hunks,
            });
            continue;
        }

        return Err(AgentError::InvalidInput(format!(
            "unexpected directive in Codex patch: '{line}'"
        )));
    }

    if !found_end {
        return Err(AgentError::InvalidInput(
            "Codex patch missing '*** End Patch' marker".into(),
        ));
    }
    while i < lines.len() {
        if !lines[i].trim().is_empty() {
            return Err(AgentError::InvalidInput(format!(
                "unexpected trailing content after '*** End Patch': '{}'",
                lines[i]
            )));
        }
        i += 1;
    }
    if ops.is_empty() {
        return Err(AgentError::InvalidInput(
            "patch contains no file operations".into(),
        ));
    }
    Ok(ops)
}

fn clean_diff_header_path(raw: &str) -> String {
    let without_ts = raw.split('\t').next().unwrap_or(raw).trim();
    if without_ts.len() >= 2 && without_ts.starts_with('"') && without_ts.ends_with('"') {
        without_ts[1..without_ts.len() - 1].to_string()
    } else {
        without_ts.to_string()
    }
}

fn strip_git_prefix(raw: &str, expected_prefix: &str, allow_strip: bool) -> String {
    if allow_strip && raw != "/dev/null" {
        if let Some(rest) = raw.strip_prefix(expected_prefix) {
            return rest.to_string();
        }
    }
    raw.to_string()
}

fn parse_unified_diff(lines: &[&str]) -> Result<Vec<ParsedPatchOp>> {
    let mut i = 0usize;
    let mut ops = Vec::new();

    while i < lines.len() {
        let mut in_git_block = false;
        let mut git_rename_from: Option<String> = None;
        let mut git_rename_to: Option<String> = None;

        while i < lines.len() && lines[i].trim().is_empty() {
            i += 1;
        }
        if i >= lines.len() {
            break;
        }

        if lines[i].starts_with("diff --git ") {
            in_git_block = true;
            i += 1;
            while i < lines.len() {
                let l = lines[i];
                if l.starts_with("--- ") || l.starts_with("diff --git ") {
                    break;
                }
                if let Some(rf) = l.strip_prefix("rename from ") {
                    git_rename_from = Some(clean_diff_header_path(rf));
                    i += 1;
                } else if let Some(rt) = l.strip_prefix("rename to ") {
                    git_rename_to = Some(clean_diff_header_path(rt));
                    i += 1;
                } else if l.starts_with("index ")
                    || l.starts_with("old mode ")
                    || l.starts_with("new mode ")
                    || l.starts_with("new file mode ")
                    || l.starts_with("deleted file mode ")
                    || l.starts_with("similarity index ")
                    || l.starts_with("dissimilarity index ")
                    || l.starts_with("copy from ")
                    || l.starts_with("copy to ")
                {
                    i += 1;
                } else if l.trim().is_empty() {
                    i += 1;
                    break;
                } else {
                    return Err(AgentError::InvalidInput(format!(
                        "unexpected git diff metadata line: '{l}'"
                    )));
                }
            }
            if (i >= lines.len() || lines[i].starts_with("diff --git ") || lines[i].trim().is_empty())
                && git_rename_from.is_some()
                && git_rename_to.is_some()
            {
                ops.push(ParsedPatchOp::Update {
                    raw_path: git_rename_from.take().unwrap_or_default(),
                    move_to: git_rename_to.take(),
                    hunks: Vec::new(),
                });
                continue;
            }
        }

        if i >= lines.len() {
            break;
        }
        let Some(old_header) = lines[i].strip_prefix("--- ") else {
            return Err(AgentError::InvalidInput(format!(
                "expected unified diff '--- ' header, got '{}'",
                lines[i]
            )));
        };
        i += 1;
        if i >= lines.len() {
            return Err(AgentError::InvalidInput(
                "unified diff truncated after '--- ' header".into(),
            ));
        }
        let Some(new_header) = lines[i].strip_prefix("+++ ") else {
            return Err(AgentError::InvalidInput(format!(
                "expected unified diff '+++ ' header after '--- ', got '{}'",
                lines[i]
            )));
        };
        i += 1;

        let raw_old = clean_diff_header_path(old_header);
        let raw_new = clean_diff_header_path(new_header);
        let strip_prefixes = in_git_block
            || (raw_old.starts_with("a/")
                && (raw_new.starts_with("b/") || raw_new == "/dev/null"))
            || (raw_old == "/dev/null" && raw_new.starts_with("b/"));

        let old_path = strip_git_prefix(&raw_old, "a/", strip_prefixes);
        let new_path = strip_git_prefix(&raw_new, "b/", strip_prefixes);

        if old_path.is_empty() || new_path.is_empty() {
            return Err(AgentError::InvalidInput(
                "unified diff header contains empty file path".into(),
            ));
        }
        if old_path == "/dev/null" && new_path == "/dev/null" {
            return Err(AgentError::InvalidInput(
                "unified diff cannot have both paths as /dev/null".into(),
            ));
        }

        let mut hunks = Vec::new();
        while i < lines.len() {
            if lines[i].starts_with("diff --git ") || lines[i].starts_with("--- ") {
                break;
            }
            if lines[i].trim().is_empty() {
                i += 1;
                continue;
            }
            if !lines[i].starts_with("@@") {
                return Err(AgentError::InvalidInput(format!(
                    "expected unified diff '@@' hunk header, got '{}'",
                    lines[i]
                )));
            }
            let (old_start_hint, old_count_zero, counts, anchor) =
                parse_hunk_header_line(lines[i])?;
            let (mut old_rem, mut new_rem) = counts.ok_or_else(|| {
                AgentError::InvalidInput(format!(
                    "malformed unified diff hunk header: '{}'",
                    lines[i]
                ))
            })?;
            i += 1;

            let mut old_chunk = Vec::new();
            let mut new_chunk = Vec::new();
            let mut new_no_trailing_nl = None;
            let mut last_tag: Option<char> = None;

            while old_rem > 0 || new_rem > 0 {
                if i >= lines.len() {
                    return Err(AgentError::InvalidInput(
                        "unexpected EOF inside unified diff hunk".into(),
                    ));
                }
                let hl = lines[i];
                if hl == "\\ No newline at end of file" {
                    if matches!(last_tag, Some('+' | ' ')) {
                        new_no_trailing_nl = Some(true);
                    }
                    i += 1;
                    continue;
                }
                if hl.is_empty() && old_rem > 0 && new_rem > 0 {
                    old_rem -= 1;
                    new_rem -= 1;
                    old_chunk.push(String::new());
                    new_chunk.push(String::new());
                    last_tag = Some(' ');
                } else if let Some(rest) = hl.strip_prefix(' ') {
                    if old_rem == 0 || new_rem == 0 {
                        return Err(AgentError::InvalidInput(format!(
                            "unexpected context line past unified hunk counts: '{hl}'"
                        )));
                    }
                    old_rem -= 1;
                    new_rem -= 1;
                    old_chunk.push(rest.to_string());
                    new_chunk.push(rest.to_string());
                    last_tag = Some(' ');
                } else if let Some(rest) = hl.strip_prefix('-') {
                    if old_rem == 0 {
                        return Err(AgentError::InvalidInput(format!(
                            "unexpected '-' line past unified hunk old count: '{hl}'"
                        )));
                    }
                    old_rem -= 1;
                    old_chunk.push(rest.to_string());
                    last_tag = Some('-');
                } else if let Some(rest) = hl.strip_prefix('+') {
                    if new_rem == 0 {
                        return Err(AgentError::InvalidInput(format!(
                            "unexpected '+' line past unified hunk new count: '{hl}'"
                        )));
                    }
                    new_rem -= 1;
                    new_chunk.push(rest.to_string());
                    last_tag = Some('+');
                } else {
                    return Err(AgentError::InvalidInput(format!(
                        "invalid line inside unified diff hunk: '{hl}'"
                    )));
                }
                i += 1;
            }

            if i < lines.len() && lines[i] == "\\ No newline at end of file" {
                if matches!(last_tag, Some('+' | ' ')) {
                    new_no_trailing_nl = Some(true);
                }
                i += 1;
            } else if last_tag == Some('+') && new_no_trailing_nl.is_none() {
                new_no_trailing_nl = Some(false);
            }

            hunks.push(PatchHunk {
                anchor,
                old_start_hint,
                old_count_zero,
                old_chunk,
                new_chunk,
                end_of_file: false,
                new_no_trailing_nl,
            });
        }

        if old_path == "/dev/null" {
            if hunks.iter().any(|h| !h.old_chunk.is_empty()) {
                return Err(AgentError::InvalidInput(format!(
                    "added file '{new_path}' cannot remove or match old lines"
                )));
            }
            let mut added_lines = Vec::new();
            let mut no_nl = false;
            for h in &hunks {
                added_lines.extend(h.new_chunk.iter().cloned());
                if h.new_no_trailing_nl == Some(true) {
                    no_nl = true;
                }
            }
            let mut content = added_lines.join("\n");
            if !added_lines.is_empty() && !no_nl {
                content.push('\n');
            }
            ops.push(ParsedPatchOp::Add {
                raw_path: new_path,
                content,
            });
        } else if new_path == "/dev/null" {
            if hunks.iter().any(|h| !h.new_chunk.is_empty()) {
                return Err(AgentError::InvalidInput(format!(
                    "deleted file '{old_path}' cannot add lines"
                )));
            }
            let mut expected = Vec::new();
            for h in &hunks {
                expected.extend(h.old_chunk.iter().cloned());
            }
            ops.push(ParsedPatchOp::Delete {
                raw_path: old_path,
                expected_lines: Some(expected),
            });
        } else {
            if hunks.is_empty() && old_path == new_path {
                return Err(AgentError::InvalidInput(format!(
                    "unified diff for '{old_path}' has no hunks"
                )));
            }
            let move_to = if old_path != new_path {
                Some(new_path)
            } else {
                None
            };
            ops.push(ParsedPatchOp::Update {
                raw_path: old_path,
                move_to,
                hunks,
            });
        }
    }

    if ops.is_empty() {
        return Err(AgentError::InvalidInput(
            "patch contains no file operations".into(),
        ));
    }
    Ok(ops)
}

fn parse_patch_document(patch: &str) -> Result<Vec<ParsedPatchOp>> {
    let lines: Vec<&str> = patch.lines().collect();
    let first_non_empty = lines.iter().copied().find(|l| !l.trim().is_empty());
    match first_non_empty {
        None => Err(AgentError::InvalidInput("patch cannot be empty".into())),
        Some(first) if first.trim() == "*** Begin Patch" => parse_codex_patch(&lines),
        Some(first)
            if first.starts_with("diff --git ") || first.starts_with("--- ") =>
        {
            parse_unified_diff(&lines)
        }
        Some(other) => Err(AgentError::InvalidInput(format!(
            "unrecognized patch format starting with '{other}'; expected '*** Begin Patch' or unified diff ('--- '/'diff --git ')"
        ))),
    }
}

fn split_lines_preserving_style(text: &str) -> (Vec<String>, bool, bool) {
    if text.is_empty() {
        return (Vec::new(), false, false);
    }
    let uses_crlf = text.contains("\r\n") && !has_bare_lf(text);
    let trailing_nl = text.ends_with('\n');
    let lines = text.lines().map(str::to_string).collect();
    (lines, trailing_nl, uses_crlf)
}

fn find_anchor_index(lines: &[String], anchor: &str, start_from: usize) -> Option<usize> {
    let trimmed_anchor = anchor.trim();
    lines
        .iter()
        .enumerate()
        .skip(start_from)
        .find(|(_, l)| l.trim() == trimmed_anchor)
        .map(|(idx, _)| idx)
        .or_else(|| {
            lines
                .iter()
                .enumerate()
                .skip(start_from)
                .find(|(_, l)| l.contains(trimmed_anchor))
                .map(|(idx, _)| idx)
        })
}

fn select_hunk_position(
    lines: &[String],
    hunk: &PatchHunk,
    cursor: usize,
    rel_path: &str,
) -> Result<usize> {
    if hunk.old_chunk.is_empty() {
        if hunk.end_of_file {
            return Ok(lines.len());
        }
        if let Some(start_hint) = hunk.old_start_hint {
            let pos = if hunk.old_count_zero {
                start_hint.min(lines.len())
            } else {
                start_hint.saturating_sub(1).min(lines.len())
            };
            return Ok(pos.max(cursor).min(lines.len()));
        }
        if let Some(anchor) = hunk.anchor.as_deref() {
            if let Some(idx) = find_anchor_index(lines, anchor, cursor) {
                return Ok((idx + 1).min(lines.len()));
            }
        }
        return Ok(lines.len());
    }

    let old_len = hunk.old_chunk.len();
    if lines.len() < old_len || cursor > lines.len() - old_len {
        return Err(AgentError::InvalidInput(format!(
            "patch hunk does not fit in '{rel_path}' (file has {} lines remaining from line {})",
            lines.len().saturating_sub(cursor),
            cursor + 1
        )));
    }

    let max_start = lines.len() - old_len;
    if hunk.end_of_file {
        let eof_idx = max_start;
        if eof_idx >= cursor && lines[eof_idx..eof_idx + old_len] == hunk.old_chunk[..] {
            return Ok(eof_idx);
        }
        return Err(AgentError::InvalidInput(format!(
            "EOF patch hunk did not match end of '{rel_path}'"
        )));
    }

    let search_start = if hunk.old_start_hint.is_none() {
        if let Some(anchor) = hunk.anchor.as_deref() {
            if let Some(anchor_idx) = find_anchor_index(lines, anchor, cursor) {
                let mut anchored_matches = Vec::new();
                for idx in anchor_idx..=max_start {
                    if lines[idx..idx + old_len] == hunk.old_chunk[..] {
                        anchored_matches.push(idx);
                    }
                }
                if !anchored_matches.is_empty() {
                    return Ok(anchored_matches[0]);
                }
            }
        }
        cursor
    } else {
        cursor
    };

    let mut matches = Vec::new();
    for idx in search_start..=max_start {
        if lines[idx..idx + old_len] == hunk.old_chunk[..] {
            matches.push(idx);
        }
    }

    if matches.is_empty() {
        return Err(AgentError::InvalidInput(format!(
            "patch hunk context did not match '{}' near line {}",
            rel_path,
            hunk.old_start_hint.unwrap_or(cursor + 1)
        )));
    }

    if matches.len() == 1 {
        return Ok(matches[0]);
    }

    if let Some(hint) = hunk.old_start_hint {
        let target_idx = hint.saturating_sub(1);
        let best = matches
            .iter()
            .copied()
            .min_by_key(|&idx| (idx.abs_diff(target_idx), idx))
            .unwrap_or(matches[0]);
        return Ok(best);
    }

    Ok(matches[0])
}

fn apply_hunks_in_memory(
    original: &str,
    hunks: &[PatchHunk],
    rel_path: &str,
    max_bytes: usize,
) -> Result<String> {
    let (mut lines, mut trailing_nl, uses_crlf) = split_lines_preserving_style(original);
    let orig_was_empty = lines.is_empty();
    let mut cursor = 0usize;

    for hunk in hunks {
        let pos = select_hunk_position(&lines, hunk, cursor, rel_path)?;
        let old_len = hunk.old_chunk.len();
        let new_len = hunk.new_chunk.len();
        lines.splice(pos..pos + old_len, hunk.new_chunk.iter().cloned());
        cursor = pos + new_len;

        if let Some(no_nl) = hunk.new_no_trailing_nl {
            if pos + new_len == lines.len() {
                trailing_nl = !no_nl;
            }
        } else if orig_was_empty && !lines.is_empty() && new_len > 0 {
            trailing_nl = true;
        }
    }

    let joined = if lines.is_empty() {
        String::new()
    } else {
        let sep = if uses_crlf { "\r\n" } else { "\n" };
        let mut out = lines.join(sep);
        if trailing_nl {
            out.push_str(sep);
        }
        out
    };

    if joined.len() > max_bytes {
        return Err(AgentError::LimitExceeded(format!(
            "patched file '{}' ({} bytes) exceeds max_file_bytes ({})",
            rel_path,
            joined.len(),
            max_bytes
        )));
    }
    Ok(joined)
}

struct VirtualPatchWorkspace<'a> {
    cfg: &'a ToolsConfig,
    root: Option<PathBuf>,
    max_bytes: usize,
    files: HashMap<PathBuf, Option<String>>,
}

impl<'a> VirtualPatchWorkspace<'a> {
    fn new(cfg: &'a ToolsConfig, max_bytes: usize) -> Self {
        Self {
            cfg,
            root: None,
            max_bytes,
            files: HashMap::new(),
        }
    }

    fn resolve_file(&mut self, raw_path: &str) -> Result<(PathBuf, PathBuf)> {
        let (root, target) = resolve_workspace_path_checked(raw_path, self.cfg)?;
        if target == root {
            return Err(AgentError::PermissionDenied(
                "cannot patch workspace root directory".into(),
            ));
        }
        ensure_no_symlink_components_under_root(&root, &target)?;
        if let Some(existing_root) = &self.root {
            if existing_root != &root {
                return Err(AgentError::PermissionDenied(
                    "patch cannot span multiple distinct workspace roots".into(),
                ));
            }
        } else {
            self.root = Some(root.clone());
        }
        Ok((root, target))
    }

    fn ensure_absent(&self, root: &Path, target: &Path) -> Result<()> {
        if let Some(state) = self.files.get(target) {
            return match state {
                None => Ok(()),
                Some(_) => Err(AgentError::InvalidInput(format!(
                    "target file already exists: '{}'",
                    normalize_rel_path(target, root)
                ))),
            };
        }
        match fs::symlink_metadata(target) {
            Ok(meta) => {
                if meta.file_type().is_symlink() {
                    Err(AgentError::PermissionDenied("symlinks are forbidden".into()))
                } else {
                    Err(AgentError::InvalidInput(format!(
                        "target path already exists: '{}'",
                        normalize_rel_path(target, root)
                    )))
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(AgentError::Tool {
                tool_id: "apply_patch".into(),
                message: format!("stat failed for '{}': {e}", target.display()),
            }),
        }
    }

    fn load_file(&mut self, root: &Path, target: &Path) -> Result<String> {
        if let Some(state) = self.files.get(target) {
            return state.clone().ok_or_else(|| {
                AgentError::NotFound(format!(
                    "file was already deleted earlier in patch: '{}'",
                    normalize_rel_path(target, root)
                ))
            });
        }
        let content = read_existing_utf8_file(target, root, "apply_patch", self.max_bytes)?;
        self.files.insert(target.to_path_buf(), Some(content.clone()));
        Ok(content)
    }
}

/// Atomically pre-validate and apply a multi-file Codex patch or unified diff inside the workspace.
///
/// Pre-validates every file path, symlink guard, `max_file_bytes` (`0` = 16 MiB), and hunk context
/// in memory before writing, renaming, or deleting any file on disk.
pub fn exec_apply_patch(args: &Value, cfg: &ToolsConfig) -> Result<Value> {
    let patch_text = args
        .get("patch")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AgentError::InvalidInput("missing required string 'patch'".into()))?;
    if patch_text.trim().is_empty() {
        return Err(AgentError::InvalidInput("patch cannot be empty".into()));
    }

    let ops = parse_patch_document(patch_text)?;
    let max_bytes = effective_max_file_bytes(cfg);
    let mut vws = VirtualPatchWorkspace::new(cfg, max_bytes);

    let mut added = Vec::new();
    let mut updated = Vec::new();
    let mut deleted = Vec::new();
    let mut moved = Vec::new();
    let mut write_order: Vec<PathBuf> = Vec::new();

    for op in ops {
        match op {
            ParsedPatchOp::Add { raw_path, content } => {
                if content.len() > max_bytes {
                    return Err(AgentError::LimitExceeded(format!(
                        "added file '{raw_path}' ({} bytes) exceeds max_file_bytes ({})",
                        content.len(),
                        max_bytes
                    )));
                }
                let (root, target) = vws.resolve_file(&raw_path)?;
                vws.ensure_absent(&root, &target)?;
                let rel = normalize_rel_path(&target, &root);
                vws.files.insert(target.clone(), Some(content));
                write_order.push(target);
                added.push(rel);
            }
            ParsedPatchOp::Delete {
                raw_path,
                expected_lines,
            } => {
                let (root, target) = vws.resolve_file(&raw_path)?;
                let current = vws.load_file(&root, &target)?;
                let rel = normalize_rel_path(&target, &root);
                if let Some(exp) = expected_lines {
                    let (disk_lines, _, _) = split_lines_preserving_style(&current);
                    if !exp.is_empty() && disk_lines != exp {
                        return Err(AgentError::InvalidInput(format!(
                            "delete diff for '{rel}' does not match current file content"
                        )));
                    }
                }
                vws.files.insert(target, None);
                deleted.push(rel);
            }
            ParsedPatchOp::Update {
                raw_path,
                move_to,
                hunks,
            } => {
                let (root, src_target) = vws.resolve_file(&raw_path)?;
                let current = vws.load_file(&root, &src_target)?;
                let src_rel = normalize_rel_path(&src_target, &root);
                let patched = apply_hunks_in_memory(&current, &hunks, &src_rel, max_bytes)?;

                if let Some(dst_raw) = move_to {
                    let (_dst_root, dst_target) = vws.resolve_file(&dst_raw)?;
                    if dst_target != src_target {
                        vws.ensure_absent(&root, &dst_target)?;
                        let dst_rel = normalize_rel_path(&dst_target, &root);
                        vws.files.insert(src_target, None);
                        vws.files.insert(dst_target.clone(), Some(patched));
                        write_order.push(dst_target);
                        moved.push(json!({ "from": src_rel, "to": dst_rel }));
                    } else {
                        vws.files.insert(src_target.clone(), Some(patched));
                        write_order.push(src_target);
                        updated.push(src_rel);
                    }
                } else {
                    vws.files.insert(src_target.clone(), Some(patched));
                    write_order.push(src_target);
                    updated.push(src_rel);
                }
            }
        }
    }

    let root = vws.root.ok_or_else(|| {
        AgentError::InvalidInput("patch did not resolve any target paths".into())
    })?;

    // Commit phase: write final contents atomically first, then remove deleted/moved-from paths.
    let mut written_paths = Vec::new();
    for path in write_order {
        if let Some(Some(content)) = vws.files.get(&path) {
            if !written_paths.contains(&path) {
                atomic_write_file(&path, &root, content.as_bytes(), max_bytes, "apply_patch")?;
                written_paths.push(path);
            }
        }
    }

    for (path, state) in &vws.files {
        if state.is_none() {
            match fs::symlink_metadata(path) {
                Ok(meta) => {
                    if meta.file_type().is_symlink() {
                        return Err(AgentError::PermissionDenied("symlinks are forbidden".into()));
                    }
                    if !meta.is_file() {
                        return Err(AgentError::InvalidInput(format!(
                            "'{}' is not a regular file",
                            normalize_rel_path(path, &root)
                        )));
                    }
                    fs::remove_file(path).map_err(|e| AgentError::Tool {
                        tool_id: "apply_patch".into(),
                        message: format!(
                            "failed to remove '{}': {e}",
                            normalize_rel_path(path, &root)
                        ),
                    })?;
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    return Err(AgentError::Tool {
                        tool_id: "apply_patch".into(),
                        message: format!("stat failed for '{}': {e}", path.display()),
                    });
                }
            }
        }
    }

    let mut files_changed: Vec<String> = Vec::new();
    for p in added
        .iter()
        .chain(updated.iter())
        .chain(deleted.iter())
    {
        if !files_changed.contains(p) {
            files_changed.push(p.clone());
        }
    }
    for m in &moved {
        if let Some(to) = m.get("to").and_then(|v| v.as_str()) {
            let s = to.to_string();
            if !files_changed.contains(&s) {
                files_changed.push(s);
            }
        }
    }

    Ok(json!({
        "applied": true,
        "files_changed": files_changed,
        "added": added,
        "updated": updated,
        "deleted": deleted,
        "moved": moved,
    }))
}

fn should_skip_walk_dir_name(name: &std::ffi::OsStr) -> bool {
    matches!(name.to_str(), Some(".git" | "target" | "node_modules"))
}

fn sorted_dir_entries(dir: &Path, tool_id: &str) -> Result<Vec<(PathBuf, fs::Metadata)>> {
    let rd = fs::read_dir(dir).map_err(|e| AgentError::Tool {
        tool_id: tool_id.into(),
        message: format!("read_dir failed for '{}': {e}", dir.display()),
    })?;
    let mut map: BTreeMap<std::ffi::OsString, (PathBuf, fs::Metadata)> = BTreeMap::new();
    for entry_res in rd {
        let Ok(entry) = entry_res else { continue };
        let path = entry.path();
        let Ok(meta) = fs::symlink_metadata(&path) else {
            continue;
        };
        if meta.file_type().is_symlink() {
            continue;
        }
        map.insert(entry.file_name(), (path, meta));
    }
    Ok(map.into_values().collect())
}

fn truncate_line_chars(line: &str, max_chars: usize) -> String {
    let mut out = String::new();
    for (idx, ch) in line.chars().enumerate() {
        if idx >= max_chars {
            out.push_str("...");
            break;
        }
        out.push(ch);
    }
    out
}

struct GrepWalk<'a> {
    root: &'a Path,
    max_bytes: usize,
    re: &'a regex::Regex,
    max_results: usize,
    matches: Vec<Value>,
    truncated: bool,
}

impl<'a> GrepWalk<'a> {
    fn search_file(&mut self, file_path: &Path, meta: &fs::Metadata) {
        if self.truncated || meta.len() > self.max_bytes as u64 {
            return;
        }
        let Ok(bytes) = fs::read(file_path) else {
            return;
        };
        if bytes.len() > self.max_bytes || bytes.contains(&0) {
            return;
        }
        let Ok(text) = std::str::from_utf8(&bytes) else {
            return;
        };

        let rel_path = normalize_rel_path(file_path, self.root);
        for (idx, line) in text.lines().enumerate() {
            if self.re.is_match(line) {
                if self.matches.len() >= self.max_results {
                    self.truncated = true;
                    return;
                }
                self.matches.push(json!({
                    "path": rel_path,
                    "line": idx + 1,
                    "text": truncate_line_chars(line, MAX_GREP_LINE_CHARS),
                }));
            }
        }
    }

    fn walk_dir(&mut self, dir: &Path) -> Result<()> {
        if self.truncated {
            return Ok(());
        }
        let entries = sorted_dir_entries(dir, "grep_search")?;
        let mut subdirs = Vec::new();
        for (path, meta) in entries {
            if self.truncated {
                break;
            }
            if meta.is_dir() {
                if let Some(fname) = path.file_name() {
                    if should_skip_walk_dir_name(fname) {
                        continue;
                    }
                }
                subdirs.push(path);
            } else if meta.is_file() {
                self.search_file(&path, &meta);
            }
        }
        for sd in subdirs {
            if self.truncated {
                break;
            }
            self.walk_dir(&sd)?;
        }
        Ok(())
    }
}

/// Recursively search text files under a workspace path for matching lines.
///
/// Skips symlinks, `.git`, `target`, `node_modules`, binary files with NUL bytes,
/// non-UTF-8 files, and files larger than `max_file_bytes` (`0` = 16 MiB).
pub fn exec_grep_search(args: &Value, cfg: &ToolsConfig) -> Result<Value> {
    let query = args
        .get("query")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AgentError::InvalidInput("missing required string 'query'".into()))?;
    if query.is_empty() {
        return Err(AgentError::InvalidInput("query cannot be empty".into()));
    }

    let raw_path = args
        .get("path")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(".");
    let is_regex = args
        .get("is_regex")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let case_insensitive = args
        .get("case_insensitive")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let max_results = args
        .get("max_results")
        .and_then(|v| v.as_u64())
        .unwrap_or(DEFAULT_GREP_MAX_RESULTS)
        .clamp(1, MAX_GREP_RESULTS) as usize;

    let pattern = if is_regex {
        query.to_string()
    } else {
        regex::escape(query)
    };
    let re = RegexBuilder::new(&pattern)
        .case_insensitive(case_insensitive)
        .build()
        .map_err(|e| AgentError::InvalidInput(format!("invalid regex query: {e}")))?;

    let (root, target) = resolve_workspace_path_checked(raw_path, cfg)?;
    ensure_no_symlink_components_under_root(&root, &target)?;

    let meta = fs::symlink_metadata(&target).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            AgentError::NotFound(format!("path not found: {}", normalize_rel_path(&target, &root)))
        } else {
            AgentError::Tool {
                tool_id: "grep_search".into(),
                message: format!("stat failed for '{}': {e}", target.display()),
            }
        }
    })?;
    if meta.file_type().is_symlink() {
        return Err(AgentError::PermissionDenied("symlinks are forbidden".into()));
    }

    let mut walk = GrepWalk {
        root: &root,
        max_bytes: effective_max_file_bytes(cfg),
        re: &re,
        max_results,
        matches: Vec::new(),
        truncated: false,
    };

    if meta.is_file() {
        walk.search_file(&target, &meta);
    } else if meta.is_dir() {
        walk.walk_dir(&target)?;
    } else {
        return Err(AgentError::InvalidInput(format!(
            "'{}' is neither a file nor a directory",
            normalize_rel_path(&target, &root)
        )));
    }

    Ok(json!({
        "matches": walk.matches,
        "truncated": walk.truncated,
    }))
}

fn glob_match_segment(pat: &[char], text: &[char]) -> bool {
    let mut pi = 0usize;
    let mut ti = 0usize;
    let mut star_pi: Option<usize> = None;
    let mut star_ti = 0usize;

    while ti < text.len() {
        if pi < pat.len() && (pat[pi] == '?' || pat[pi] == text[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < pat.len() && pat[pi] == '*' {
            star_pi = Some(pi);
            star_ti = ti;
            pi += 1;
        } else if let Some(sp) = star_pi {
            pi = sp + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }

    while pi < pat.len() && pat[pi] == '*' {
        pi += 1;
    }
    pi == pat.len()
}

#[derive(Clone, Debug)]
enum GlobSegment {
    AnyDepth,
    Pattern(Vec<char>),
}

fn compile_glob_segments(raw_pattern: &str) -> Result<Vec<GlobSegment>> {
    let normalized = raw_pattern.trim().replace('\\', "/");
    if normalized.is_empty() {
        return Err(AgentError::InvalidInput("pattern cannot be empty".into()));
    }
    if normalized.starts_with('/') {
        return Err(AgentError::PermissionDenied(
            "absolute glob patterns are forbidden".into(),
        ));
    }

    let trimmed = normalized.strip_prefix("./").unwrap_or(&normalized);
    let mut compiled = Vec::new();
    for seg in trimmed.split('/') {
        if seg.is_empty() || seg == "." {
            continue;
        }
        if seg == ".." {
            return Err(AgentError::PermissionDenied(
                "path traversal ('..') in glob pattern is forbidden".into(),
            ));
        }
        if seg == "**" {
            if !matches!(compiled.last(), Some(GlobSegment::AnyDepth)) {
                compiled.push(GlobSegment::AnyDepth);
            }
        } else {
            compiled.push(GlobSegment::Pattern(seg.chars().collect()));
        }
    }

    if compiled.is_empty() || normalized.ends_with('/') {
        return Err(AgentError::InvalidInput(format!(
            "invalid file glob pattern: '{raw_pattern}'"
        )));
    }
    Ok(compiled)
}

fn glob_matches_segments(pat: &[GlobSegment], path_segs: &[&str]) -> bool {
    let m = pat.len();
    let n = path_segs.len();
    let seg_chars: Vec<Vec<char>> = path_segs.iter().map(|s| s.chars().collect()).collect();

    let mut dp = vec![vec![false; n + 1]; m + 1];
    dp[m][n] = true;

    for pi in (0..m).rev() {
        if matches!(pat[pi], GlobSegment::AnyDepth) {
            dp[pi][n] = dp[pi + 1][n];
            for ti in (0..n).rev() {
                dp[pi][ti] = dp[pi + 1][ti] || dp[pi][ti + 1];
            }
        } else if let GlobSegment::Pattern(ref pchars) = pat[pi] {
            for ti in (0..n).rev() {
                if glob_match_segment(pchars, &seg_chars[ti]) {
                    dp[pi][ti] = dp[pi + 1][ti + 1];
                }
            }
        }
    }

    dp[0][0]
}

struct GlobWalk<'a> {
    search_base: &'a Path,
    workspace_root: &'a Path,
    pat: &'a [GlobSegment],
    max_results: usize,
    files: Vec<Value>,
    truncated: bool,
}

impl<'a> GlobWalk<'a> {
    fn walk_dir(&mut self, dir: &Path) -> Result<()> {
        if self.truncated {
            return Ok(());
        }
        let entries = sorted_dir_entries(dir, "glob_files")?;
        let mut subdirs = Vec::new();
        for (path, meta) in entries {
            if self.truncated {
                break;
            }
            if meta.is_dir() {
                if let Some(fname) = path.file_name() {
                    if should_skip_walk_dir_name(fname) {
                        continue;
                    }
                }
                subdirs.push(path);
            } else if meta.is_file() {
                let rel_to_base = normalize_rel_path(&path, self.search_base);
                let segs: Vec<&str> = rel_to_base.split('/').collect();
                if glob_matches_segments(self.pat, &segs) {
                    if self.files.len() >= self.max_results {
                        self.truncated = true;
                        return Ok(());
                    }
                    self.files.push(json!({
                        "path": normalize_rel_path(&path, self.workspace_root),
                        "size_bytes": meta.len(),
                    }));
                }
            }
        }
        for sd in subdirs {
            if self.truncated {
                break;
            }
            self.walk_dir(&sd)?;
        }
        Ok(())
    }
}

/// Find regular files in the workspace matching a glob pattern (`*`, `?`, `**`).
///
/// Skips symlinks, `.git`, `target`, and `node_modules` directories.
pub fn exec_glob_files(args: &Value, cfg: &ToolsConfig) -> Result<Value> {
    let raw_pattern = args
        .get("pattern")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AgentError::InvalidInput("missing required string 'pattern'".into()))?;
    let compiled = compile_glob_segments(raw_pattern)?;

    let raw_path = args
        .get("path")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(".");
    let max_results = args
        .get("max_results")
        .and_then(|v| v.as_u64())
        .unwrap_or(DEFAULT_GLOB_MAX_RESULTS)
        .clamp(1, MAX_GLOB_RESULTS) as usize;

    let (root, target) = resolve_workspace_path_checked(raw_path, cfg)?;
    ensure_no_symlink_components_under_root(&root, &target)?;

    let meta = fs::symlink_metadata(&target).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            AgentError::NotFound(format!("path not found: {}", normalize_rel_path(&target, &root)))
        } else {
            AgentError::Tool {
                tool_id: "glob_files".into(),
                message: format!("stat failed for '{}': {e}", target.display()),
            }
        }
    })?;
    if meta.file_type().is_symlink() {
        return Err(AgentError::PermissionDenied("symlinks are forbidden".into()));
    }
    if !meta.is_dir() {
        return Err(AgentError::InvalidInput(format!(
            "'{}' is not a directory",
            normalize_rel_path(&target, &root)
        )));
    }

    let mut walk = GlobWalk {
        search_base: &target,
        workspace_root: &root,
        pat: &compiled,
        max_results,
        files: Vec::new(),
        truncated: false,
    };
    walk.walk_dir(&target)?;

    Ok(json!({
        "files": walk.files,
        "truncated": walk.truncated,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg_for(root: &Path, max_file_bytes: usize) -> ToolsConfig {
        ToolsConfig {
            allowed_paths: vec![root.to_string_lossy().into_owned()],
            max_file_bytes,
            ..Default::default()
        }
    }

    #[test]
    fn edit_file_unique_replace_all_errors_and_limits() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = cfg_for(dir.path(), 0); // 0 => 16 MiB
        let target = dir.path().join("lib.rs");
        fs::write(&target, "let x = foo();\nlet y = foo();\nlet z = bar();\n").unwrap();

        // Ambiguous match fails when replace_all=false
        let err = exec_edit_file(
            &json!({
                "path": "lib.rs",
                "old_text": "foo()",
                "new_text": "baz()",
            }),
            &cfg,
        )
        .unwrap_err();
        assert!(matches!(err, AgentError::InvalidInput(_)));
        assert!(err.to_string().contains("2 times"));

        // Unique match succeeds
        let res = exec_edit_file(
            &json!({
                "path": "lib.rs",
                "old_text": "bar()",
                "new_text": "qux()",
            }),
            &cfg,
        )
        .unwrap();
        assert_eq!(res["replacements"], 1);
        assert_eq!(
            fs::read_to_string(&target).unwrap(),
            "let x = foo();\nlet y = foo();\nlet z = qux();\n"
        );

        // replace_all updates all matches
        let res_all = exec_edit_file(
            &json!({
                "path": "lib.rs",
                "old_text": "foo()",
                "new_text": "baz()",
                "replace_all": true,
            }),
            &cfg,
        )
        .unwrap();
        assert_eq!(res_all["replacements"], 2);
        assert_eq!(
            fs::read_to_string(&target).unwrap(),
            "let x = baz();\nlet y = baz();\nlet z = qux();\n"
        );

        // Missing match returns InvalidInput
        assert!(matches!(
            exec_edit_file(
                &json!({
                    "path": "lib.rs",
                    "old_text": "does_not_exist",
                    "new_text": "x",
                }),
                &cfg,
            ),
            Err(AgentError::InvalidInput(_))
        ));

        // max_file_bytes enforced on edit output
        let small_cfg = cfg_for(dir.path(), 64);
        assert!(matches!(
            exec_edit_file(
                &json!({
                    "path": "lib.rs",
                    "old_text": "qux()",
                    "new_text": "a_very_long_replacement_string_exceeding_sixty_four_bytes_total_size",
                }),
                &small_cfg,
            ),
            Err(AgentError::LimitExceeded(_))
        ));
    }

    #[test]
    fn apply_patch_codex_envelope_add_update_move_delete_and_atomic_rollback() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = cfg_for(dir.path(), 0);
        fs::create_dir_all(dir.path().join("src")).unwrap();
        fs::write(dir.path().join("src/a.txt"), "one\ntwo\nthree\n").unwrap();
        fs::write(dir.path().join("old.txt"), "obsolete\n").unwrap();

        // Partial-failure pre-validation test: a.txt hunk is valid, b.txt is missing -> a.txt untouched!
        let bad_patch = "\
*** Begin Patch
*** Update File: src/a.txt
@@
 one
-two
+TWO
 three
*** Delete File: missing.txt
*** End Patch
";
        assert!(matches!(
            exec_apply_patch(&json!({ "patch": bad_patch }), &cfg),
            Err(AgentError::NotFound(_))
        ));
        assert_eq!(
            fs::read_to_string(dir.path().join("src/a.txt")).unwrap(),
            "one\ntwo\nthree\n"
        );

        // Full Codex patch with Add, Update + Move to, and Delete
        let good_patch = "\
*** Begin Patch
*** Add File: docs/hello.txt
+hello
+world
*** Update File: src/a.txt
*** Move to: src/b.txt
@@
 one
-two
+TWO
 three
*** Delete File: old.txt
*** End Patch
";
        let out = exec_apply_patch(&json!({ "patch": good_patch }), &cfg).unwrap();
        assert_eq!(out["applied"], true);
        assert_eq!(
            fs::read_to_string(dir.path().join("docs/hello.txt")).unwrap(),
            "hello\nworld\n"
        );
        assert!(!dir.path().join("src/a.txt").exists());
        assert_eq!(
            fs::read_to_string(dir.path().join("src/b.txt")).unwrap(),
            "one\nTWO\nthree\n"
        );
        assert!(!dir.path().join("old.txt").exists());
    }

    #[test]
    fn apply_patch_unified_diff_multi_file_and_dev_null() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = cfg_for(dir.path(), 0);
        fs::write(dir.path().join("main.rs"), "fn a() {\n    1\n}\n").unwrap();
        fs::write(dir.path().join("drop.txt"), "bye\n").unwrap();

        let unified = "\
--- a/main.rs
+++ b/main.rs
@@ -1,3 +1,4 @@ fn a()
 fn a() {
-    1
+    2
+    // extra
 }
--- /dev/null
+++ b/created.txt
@@ -0,0 +1,2 @@
+alpha
+beta
--- a/drop.txt
+++ /dev/null
@@ -1,1 +0,0 @@
-bye
";
        let out = exec_apply_patch(&json!({ "patch": unified }), &cfg).unwrap();
        assert_eq!(out["applied"], true);
        assert_eq!(
            fs::read_to_string(dir.path().join("main.rs")).unwrap(),
            "fn a() {\n    2\n    // extra\n}\n"
        );
        assert_eq!(
            fs::read_to_string(dir.path().join("created.txt")).unwrap(),
            "alpha\nbeta\n"
        );
        assert!(!dir.path().join("drop.txt").exists());

        // Traversal attempt in unified diff fails cleanly without touching disk
        let traversal = "\
--- a/main.rs
+++ b/main.rs
@@ -1,1 +1,1 @@
-fn a() {
+fn hacked() {
--- /dev/null
+++ b/../escape.txt
@@ -0,0 +1,1 @@
+oops
";
        assert!(matches!(
            exec_apply_patch(&json!({ "patch": traversal }), &cfg),
            Err(AgentError::PermissionDenied(_))
        ));
        assert_eq!(
            fs::read_to_string(dir.path().join("main.rs")).unwrap(),
            "fn a() {\n    2\n    // extra\n}\n"
        );
    }

    #[test]
    fn grep_search_skips_ignored_dirs_binaries_and_caps_results() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = cfg_for(dir.path(), 128);
        fs::create_dir_all(dir.path().join("src")).unwrap();
        fs::create_dir_all(dir.path().join(".git")).unwrap();
        fs::create_dir_all(dir.path().join("target")).unwrap();
        fs::create_dir_all(dir.path().join("node_modules")).unwrap();

        fs::write(
            dir.path().join("src/app.rs"),
            "fn Alpha() {}\nfn beta() {}\nfn alpha_two() {}\n",
        )
        .unwrap();
        fs::write(dir.path().join(".git/HEAD"), "alpha inside git\n").unwrap();
        fs::write(dir.path().join("target/out.txt"), "alpha inside target\n").unwrap();
        fs::write(dir.path().join("node_modules/pkg.js"), "alpha inside nm\n").unwrap();
        fs::write(dir.path().join("src/bin.dat"), b"alpha\x00binary").unwrap();
        fs::write(dir.path().join("src/huge.txt"), "alpha\n".repeat(40)).unwrap(); // > 128 bytes

        // Case-insensitive regex with max_results=1 tests truncation
        let res = exec_grep_search(
            &json!({
                "query": "alpha(_two)?\\(\\)",
                "is_regex": true,
                "case_insensitive": true,
                "max_results": 1,
            }),
            &cfg,
        )
        .unwrap();
        assert_eq!(res["truncated"], true);
        let matches = res["matches"].as_array().unwrap();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0]["path"], "src/app.rs");
        assert_eq!(matches[0]["line"], 1);

        // Full search shows exact 2 matches and truncated=false
        let res_all = exec_grep_search(
            &json!({
                "query": "alpha",
                "case_insensitive": true,
            }),
            &cfg,
        )
        .unwrap();
        assert_eq!(res_all["truncated"], false);
        assert_eq!(res_all["matches"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn glob_files_supports_wildcards_double_star_and_caps() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = cfg_for(dir.path(), 0);
        fs::create_dir_all(dir.path().join("src/nested")).unwrap();
        fs::create_dir_all(dir.path().join("target/debug")).unwrap();
        fs::write(dir.path().join("root.rs"), "r").unwrap();
        fs::write(dir.path().join("src/a1.rs"), "a1").unwrap();
        fs::write(dir.path().join("src/a12.rs"), "a12").unwrap();
        fs::write(dir.path().join("src/nested/b.rs"), "b").unwrap();
        fs::write(dir.path().join("target/debug/skip.rs"), "skip").unwrap();

        let all_rs = exec_glob_files(&json!({ "pattern": "**/*.rs" }), &cfg).unwrap();
        assert_eq!(all_rs["truncated"], false);
        let paths: Vec<&str> = all_rs["files"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v["path"].as_str().unwrap())
            .collect();
        assert_eq!(
            paths,
            vec!["root.rs", "src/a1.rs", "src/a12.rs", "src/nested/b.rs"]
        );

        let q_single = exec_glob_files(
            &json!({
                "pattern": "a?.rs",
                "path": "src",
            }),
            &cfg,
        )
        .unwrap();
        let q_paths: Vec<&str> = q_single["files"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v["path"].as_str().unwrap())
            .collect();
        assert_eq!(q_paths, vec!["src/a1.rs"]);

        let capped = exec_glob_files(
            &json!({
                "pattern": "**/*.rs",
                "max_results": 2,
            }),
            &cfg,
        )
        .unwrap();
        assert_eq!(capped["truncated"], true);
        assert_eq!(capped["files"].as_array().unwrap().len(), 2);
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn symlink_rejection_for_edit_grep_and_glob() {
        let dir = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let outside_file = outside.path().join("secret.txt");
        fs::write(&outside_file, "secret line\n").unwrap();
        fs::write(dir.path().join("real.txt"), "real line\n").unwrap();

        let link_path = dir.path().join("link.txt");
        let link_created = {
            #[cfg(unix)]
            {
                std::os::unix::fs::symlink(&outside_file, &link_path).is_ok()
            }
            #[cfg(windows)]
            {
                std::os::windows::fs::symlink_file(&outside_file, &link_path).is_ok()
            }
        };

        if link_created {
            let cfg = cfg_for(dir.path(), 0);
            assert!(matches!(
                exec_edit_file(
                    &json!({
                        "path": "link.txt",
                        "old_text": "secret",
                        "new_text": "hacked",
                    }),
                    &cfg,
                ),
                Err(AgentError::PermissionDenied(_))
            ));

            let grep = exec_grep_search(&json!({ "query": "line" }), &cfg).unwrap();
            let grep_paths: Vec<&str> = grep["matches"]
                .as_array()
                .unwrap()
                .iter()
                .map(|m| m["path"].as_str().unwrap())
                .collect();
            assert_eq!(grep_paths, vec!["real.txt"]);

            let glob = exec_glob_files(&json!({ "pattern": "*.txt" }), &cfg).unwrap();
            let glob_paths: Vec<&str> = glob["files"]
                .as_array()
                .unwrap()
                .iter()
                .map(|f| f["path"].as_str().unwrap())
                .collect();
            assert_eq!(glob_paths, vec!["real.txt"]);
        }
    }

    #[test]
    fn crlf_preservation_eof_codex_hunk_and_limit_rollback() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = cfg_for(dir.path(), 0);
        let crlf_path = dir.path().join("win.txt");
        fs::write(&crlf_path, "alpha\r\nbeta\r\ngamma\r\n").unwrap();

        // LF old_text matches CRLF file while preserving CRLF on disk
        exec_edit_file(
            &json!({
                "path": "win.txt",
                "old_text": "alpha\nbeta",
                "new_text": "ALPHA\nBETA",
            }),
            &cfg,
        )
        .unwrap();
        assert_eq!(
            fs::read_to_string(&crlf_path).unwrap(),
            "ALPHA\r\nBETA\r\ngamma\r\n"
        );

        // Traversal rejection on edit_file
        assert!(matches!(
            exec_edit_file(
                &json!({
                    "path": "../outside.txt",
                    "old_text": "a",
                    "new_text": "b",
                }),
                &cfg,
            ),
            Err(AgentError::PermissionDenied(_))
        ));

        // Codex *** End of File hunk
        let eof_patch = "\
*** Begin Patch
*** Update File: win.txt
@@
 gamma
+delta
*** End of File
*** End Patch
";
        exec_apply_patch(&json!({ "patch": eof_patch }), &cfg).unwrap();
        assert_eq!(
            fs::read_to_string(&crlf_path).unwrap(),
            "ALPHA\r\nBETA\r\ngamma\r\ndelta\r\n"
        );

        // Multi-file patch exceeding max_file_bytes on second op leaves first file untouched
        let bounded_cfg = cfg_for(dir.path(), 48);
        let oversize_patch = "\
*** Begin Patch
*** Update File: win.txt
@@
-ALPHA
+alpha_changed
*** Add File: too_big.txt
+012345678901234567890123456789012345678901234567890
*** End Patch
";
        assert!(matches!(
            exec_apply_patch(&json!({ "patch": oversize_patch }), &bounded_cfg),
            Err(AgentError::LimitExceeded(_))
        ));
        assert!(fs::read_to_string(&crlf_path).unwrap().starts_with("ALPHA\r\n"));
        assert!(!dir.path().join("too_big.txt").exists());

        // Atomic writes leave no .tmp files behind
        let leftover_tmp = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .any(|e| e.file_name().to_string_lossy().contains(".tmp"));
        assert!(!leftover_tmp);
    }
}
