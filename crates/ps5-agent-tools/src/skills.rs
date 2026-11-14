use ps5_agent_core::{AgentError, Result};
use ps5_agent_logging::log_warn;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::SystemTime;

const MAX_SKILL_SLUG_LEN: usize = 64;
const MAX_SKILL_MARKDOWN_BYTES: usize = 512 * 1024;
const MAX_RESOURCE_SCAN_DEPTH: usize = 4;
const MAX_RESOURCE_FILES: usize = 256;
const RESOURCE_SUBDIRS: [&str; 4] = ["scripts", "references", "examples", "resources"];

static TMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SkillSummary {
    pub id: String,
    pub name: String,
    pub description: String,
    pub enabled: bool,
    pub source: String,
    pub dir_path: String,
    pub has_scripts: bool,
    pub has_references: bool,
    pub resources: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SkillDocument {
    pub summary: SkillSummary,
    pub instructions: String,
    pub raw_markdown: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParsedSkillMarkdown {
    pub name: String,
    pub description: String,
    pub body: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct SkillsStateFile {
    #[serde(default)]
    disabled_ids: Vec<String>,
}

#[derive(Debug, Clone)]
struct LoadedSkillEntry {
    summary: SkillSummary,
    instructions: String,
    raw_markdown: String,
}

impl LoadedSkillEntry {
    fn summary_with_enabled(&self, enabled: bool) -> SkillSummary {
        let mut summary = self.summary.clone();
        summary.enabled = enabled;
        summary
    }

    fn document_with_enabled(&self, enabled: bool) -> SkillDocument {
        SkillDocument {
            summary: self.summary_with_enabled(enabled),
            instructions: self.instructions.clone(),
            raw_markdown: self.raw_markdown.clone(),
        }
    }
}

#[derive(Debug, Clone)]
struct SkillScanCache {
    generation: u64,
    fingerprint: ScanFingerprint,
    entries: BTreeMap<String, LoadedSkillEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ScanFingerprint {
    workspace_dirs: Vec<PathBuf>,
    roots: Vec<RootFingerprint>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RootFingerprint {
    exists: bool,
    dir_modified: Option<SystemTime>,
    raw_entry_count: usize,
    skills: Vec<SkillDirFingerprint>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SkillDirFingerprint {
    slug: String,
    dir_modified: Option<SystemTime>,
    skill_md_modified: Option<SystemTime>,
    skill_md_len: Option<u64>,
    resource_dirs: [ResourceDirFingerprint; 4],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ResourceDirFingerprint {
    exists: bool,
    dir_modified: Option<SystemTime>,
    entry_count: usize,
    max_dir_modified: Option<SystemTime>,
}

#[derive(Debug)]
pub struct SkillManager {
    data_dir: PathBuf,
    data_skills_dir: PathBuf,
    workspace_skills_dirs: Vec<PathBuf>,
    disabled_ids: RwLock<HashSet<String>>,
    cache_generation: AtomicU64,
    scan_cache: RwLock<Option<SkillScanCache>>,
    #[cfg(test)]
    disk_scan_count: AtomicU64,
}

impl SkillManager {
    pub fn new(data_dir: &Path, workspace_dir: Option<&Path>) -> Self {
        let data_skills_dir = data_dir.join("skills");
        if let Err(err) = fs::create_dir_all(&data_skills_dir) {
            log_warn(
                "skills",
                format!(
                    "failed to create skills dir {}: {err}",
                    data_skills_dir.display()
                ),
            );
        }
        let workspace_skills_dirs = workspace_dir
            .map(|ws| vec![ws.join(".agents").join("skills")])
            .unwrap_or_default();
        let disabled = load_disabled_ids_from_disk(data_dir);

        Self {
            data_dir: data_dir.to_path_buf(),
            data_skills_dir,
            workspace_skills_dirs,
            disabled_ids: RwLock::new(disabled),
            cache_generation: AtomicU64::new(0),
            scan_cache: RwLock::new(None),
            #[cfg(test)]
            disk_scan_count: AtomicU64::new(0),
        }
    }

    pub fn data_skills_dir(&self) -> &Path {
        &self.data_skills_dir
    }

    pub fn workspace_skills_dirs(&self) -> &[PathBuf] {
        &self.workspace_skills_dirs
    }

    pub fn add_workspace_skills_dir(&mut self, dir: impl Into<PathBuf>) {
        self.workspace_skills_dirs.push(dir.into());
        self.invalidate_cache();
    }

    pub fn set_disabled_ids(&self, ids: HashSet<String>) {
        let mut guard = self
            .disabled_ids
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *guard = ids;
    }

    pub fn disabled_ids(&self) -> HashSet<String> {
        self.disabled_ids
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn list_skills(&self) -> Vec<SkillSummary> {
        self.with_cached_entries(|entries, disabled| {
            entries
                .iter()
                .map(|(id, entry)| entry.summary_with_enabled(!disabled.contains(id)))
                .collect()
        })
    }

    pub fn list_enabled_summaries(&self) -> Vec<SkillSummary> {
        self.with_cached_entries(|entries, disabled| {
            entries
                .iter()
                .filter(|(id, _)| !disabled.contains(*id))
                .map(|(_, entry)| entry.summary_with_enabled(true))
                .collect()
        })
    }

    pub fn load_skill(&self, id_or_name: &str) -> Result<SkillDocument> {
        let query = id_or_name.trim();
        if query.is_empty() {
            return Err(AgentError::InvalidInput(
                "skill id or name cannot be empty".into(),
            ));
        }

        self.with_cached_entries(|entries, disabled| {
            let found = entries
                .get(query)
                .or_else(|| {
                    entries
                        .values()
                        .find(|e| e.summary.id.eq_ignore_ascii_case(query))
                })
                .or_else(|| {
                    entries
                        .values()
                        .find(|e| e.summary.name.eq_ignore_ascii_case(query))
                });

            found
                .map(|entry| entry.document_with_enabled(!disabled.contains(&entry.summary.id)))
                .ok_or_else(|| AgentError::NotFound(format!("skill '{query}' not found")))
        })
    }

    pub fn import_skill(&self, slug: Option<&str>, markdown_content: &str) -> Result<SkillSummary> {
        if markdown_content.trim().is_empty() {
            return Err(AgentError::InvalidInput(
                "skill markdown content cannot be empty".into(),
            ));
        }
        if markdown_content.len() > MAX_SKILL_MARKDOWN_BYTES {
            return Err(AgentError::LimitExceeded(format!(
                "skill markdown exceeds {MAX_SKILL_MARKDOWN_BYTES} bytes"
            )));
        }

        let parsed = parse_skill_markdown(markdown_content, "");
        let validated_slug = match slug {
            Some(explicit) => validate_skill_slug(explicit)?,
            None => {
                let derived = slugify_skill_name(&parsed.name);
                if derived.is_empty() {
                    return Err(AgentError::InvalidInput(
                        "could not derive valid skill slug from frontmatter 'name'".into(),
                    ));
                }
                validate_skill_slug(&derived)?
            }
        };

        ensure_real_dir_if_exists(&self.data_skills_dir)?;
        let skill_dir = self.data_skills_dir.join(&validated_slug);
        ensure_real_dir_if_exists(&skill_dir)?;
        fs::create_dir_all(&skill_dir).map_err(|e| {
            AgentError::Storage(format!(
                "failed to create skill directory '{}': {e}",
                skill_dir.display()
            ))
        })?;

        let skill_md_path = skill_dir.join("SKILL.md");
        ensure_real_file_if_exists(&skill_md_path)?;
        atomic_write_bytes(&skill_md_path, markdown_content.as_bytes())?;
        self.invalidate_cache();

        let disabled = self.disabled_ids();
        let entry = load_skill_from_dir(
            &validated_slug,
            &skill_dir,
            "user",
            !disabled.contains(&validated_slug),
        )?
        .ok_or_else(|| {
            AgentError::Storage(format!(
                "imported skill '{validated_slug}' could not be read back"
            ))
        })?;

        Ok(entry.summary)
    }

    pub fn set_enabled(&self, id: &str, enabled: bool) -> Result<SkillSummary> {
        let valid_id = validate_skill_slug(id)?;
        let exists = self.with_cached_entries(|entries, _| entries.contains_key(&valid_id));
        if !exists {
            return Err(AgentError::NotFound(format!("skill '{valid_id}' not found")));
        }

        let snapshot = {
            let mut guard = self
                .disabled_ids
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if enabled {
                guard.remove(&valid_id);
            } else {
                guard.insert(valid_id.clone());
            }
            guard.clone()
        };

        self.persist_disabled_ids(&snapshot)?;

        self.with_cached_entries(|entries, disabled| {
            entries
                .get(&valid_id)
                .map(|entry| entry.summary_with_enabled(!disabled.contains(&valid_id)))
                .ok_or_else(|| AgentError::NotFound(format!("skill '{valid_id}' not found")))
        })
    }

    pub fn delete_skill(&self, id: &str) -> Result<()> {
        let valid_id = validate_skill_slug(id)?;
        let skill_dir = self.data_skills_dir.join(&valid_id);
        match fs::symlink_metadata(&skill_dir) {
            Ok(meta) => {
                if meta.file_type().is_symlink() {
                    return Err(AgentError::PermissionDenied(format!(
                        "refusing to delete symlinked skill path '{}'",
                        skill_dir.display()
                    )));
                }
                if !meta.is_dir() {
                    return Err(AgentError::Storage(format!(
                        "skill path '{}' is not a directory",
                        skill_dir.display()
                    )));
                }
                fs::remove_dir_all(&skill_dir).map_err(|e| {
                    AgentError::Storage(format!(
                        "failed to delete skill directory '{}': {e}",
                        skill_dir.display()
                    ))
                })?;
                self.invalidate_cache();
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(AgentError::NotFound(format!(
                    "skill '{valid_id}' not found"
                )));
            }
            Err(e) => {
                return Err(AgentError::Storage(format!(
                    "failed to stat skill directory '{}': {e}",
                    skill_dir.display()
                )));
            }
        }

        let changed = {
            let mut guard = self
                .disabled_ids
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            guard.remove(&valid_id)
        };
        if changed {
            let snapshot = self.disabled_ids();
            self.persist_disabled_ids(&snapshot)?;
        }

        Ok(())
    }

    fn persist_disabled_ids(&self, disabled: &HashSet<String>) -> Result<()> {
        let mut sorted: Vec<String> = disabled.iter().cloned().collect();
        sorted.sort();
        let state = SkillsStateFile {
            disabled_ids: sorted,
        };
        let json = serde_json::to_vec_pretty(&state)
            .map_err(|e| AgentError::Storage(format!("failed to serialize skills_state.json: {e}")))?;
        let state_path = self.data_dir.join("skills_state.json");
        atomic_write_bytes(&state_path, &json)
    }

    fn invalidate_cache(&self) {
        self.cache_generation.fetch_add(1, Ordering::AcqRel);
        let mut guard = self
            .scan_cache
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *guard = None;
    }

    fn with_cached_entries<R>(
        &self,
        f: impl FnOnce(&BTreeMap<String, LoadedSkillEntry>, &HashSet<String>) -> R,
    ) -> R {
        let gen_before = self.cache_generation.load(Ordering::Acquire);
        let fingerprint =
            compute_scan_fingerprint(&self.workspace_skills_dirs, &self.data_skills_dir);
        let disabled = self.disabled_ids();

        {
            let guard = self
                .scan_cache
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(cached) = guard.as_ref() {
                if cached.generation == gen_before && cached.fingerprint == fingerprint {
                    return f(&cached.entries, &disabled);
                }
            }
        }

        let fresh_entries = self.scan_raw_entries_uncached();
        #[cfg(test)]
        self.disk_scan_count.fetch_add(1, Ordering::Relaxed);
        let disabled = self.disabled_ids();

        let mut guard = self
            .scan_cache
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.cache_generation.load(Ordering::Acquire) == gen_before {
            *guard = Some(SkillScanCache {
                generation: gen_before,
                fingerprint,
                entries: fresh_entries,
            });
            return f(&guard.as_ref().expect("just inserted").entries, &disabled);
        }

        drop(guard);
        f(&fresh_entries, &disabled)
    }

    fn scan_raw_entries_uncached(&self) -> BTreeMap<String, LoadedSkillEntry> {
        let empty_disabled = HashSet::new();
        let mut entries = BTreeMap::new();

        for ws_dir in &self.workspace_skills_dirs {
            scan_skills_root(ws_dir, "workspace", &empty_disabled, &mut entries);
        }
        // User/data skills override workspace skills when IDs collide.
        scan_skills_root(&self.data_skills_dir, "user", &empty_disabled, &mut entries);

        entries
    }

    #[cfg(test)]
    fn disk_scan_count(&self) -> u64 {
        self.disk_scan_count.load(Ordering::Relaxed)
    }
}

fn compute_scan_fingerprint(workspace_dirs: &[PathBuf], data_skills_dir: &Path) -> ScanFingerprint {
    let mut roots = Vec::with_capacity(workspace_dirs.len() + 1);
    for ws_dir in workspace_dirs {
        roots.push(fingerprint_skills_root(ws_dir));
    }
    roots.push(fingerprint_skills_root(data_skills_dir));
    ScanFingerprint {
        workspace_dirs: workspace_dirs.to_vec(),
        roots,
    }
}

fn fingerprint_skills_root(root: &Path) -> RootFingerprint {
    let root_meta = match fs::symlink_metadata(root) {
        Ok(m) if !m.file_type().is_symlink() && m.is_dir() => m,
        _ => {
            return RootFingerprint {
                exists: false,
                dir_modified: None,
                raw_entry_count: 0,
                skills: Vec::new(),
            };
        }
    };
    let dir_modified = root_meta.modified().ok();
    let entries = match fs::read_dir(root) {
        Ok(rd) => rd,
        Err(_) => {
            return RootFingerprint {
                exists: true,
                dir_modified,
                raw_entry_count: 0,
                skills: Vec::new(),
            };
        }
    };

    let mut raw_entry_count = 0usize;
    let mut skills = Vec::new();
    for item in entries.flatten() {
        raw_entry_count += 1;
        let file_type = match item.file_type() {
            Ok(ft) => ft,
            Err(_) => continue,
        };
        if file_type.is_symlink() || !file_type.is_dir() {
            continue;
        }
        let dir_name = item.file_name();
        let Some(slug) = dir_name.to_str() else {
            continue;
        };
        if validate_skill_slug(slug).is_err() {
            continue;
        }
        skills.push(fingerprint_skill_dir(slug, &item.path()));
    }
    skills.sort_by(|a, b| a.slug.cmp(&b.slug));

    RootFingerprint {
        exists: true,
        dir_modified,
        raw_entry_count,
        skills,
    }
}

fn fingerprint_skill_dir(slug: &str, skill_dir: &Path) -> SkillDirFingerprint {
    let dir_modified = fs::symlink_metadata(skill_dir)
        .ok()
        .and_then(|m| m.modified().ok());

    let (skill_md_modified, skill_md_len) = match fs::symlink_metadata(skill_dir.join("SKILL.md")) {
        Ok(m) if !m.file_type().is_symlink() && m.is_file() => {
            (m.modified().ok(), Some(m.len()))
        }
        _ => (None, None),
    };

    let resource_dirs = RESOURCE_SUBDIRS.map(|sub| fingerprint_resource_dir(&skill_dir.join(sub)));

    SkillDirFingerprint {
        slug: slug.to_string(),
        dir_modified,
        skill_md_modified,
        skill_md_len,
        resource_dirs,
    }
}

fn fingerprint_resource_dir(base: &Path) -> ResourceDirFingerprint {
    let meta = match fs::symlink_metadata(base) {
        Ok(m) if !m.file_type().is_symlink() && m.is_dir() => m,
        _ => {
            return ResourceDirFingerprint {
                exists: false,
                dir_modified: None,
                entry_count: 0,
                max_dir_modified: None,
            };
        }
    };

    let dir_modified = meta.modified().ok();
    let mut entry_count = 0usize;
    let mut max_dir_modified = dir_modified;
    accumulate_resource_dir_stats(base, 0, &mut entry_count, &mut max_dir_modified);

    ResourceDirFingerprint {
        exists: true,
        dir_modified,
        entry_count,
        max_dir_modified,
    }
}

fn accumulate_resource_dir_stats(
    dir: &Path,
    depth: usize,
    entry_count: &mut usize,
    max_dir_modified: &mut Option<SystemTime>,
) {
    let max_entries = MAX_RESOURCE_FILES * 2;
    if depth >= MAX_RESOURCE_SCAN_DEPTH || *entry_count >= max_entries {
        return;
    }

    let entries = match fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(_) => return,
    };

    for entry in entries.flatten() {
        if *entry_count >= max_entries {
            break;
        }
        let ft = match entry.file_type() {
            Ok(ft) => ft,
            Err(_) => continue,
        };
        if ft.is_symlink() {
            continue;
        }
        let name = entry.file_name();
        if name.to_str().is_none_or(|s| s.starts_with('.')) {
            continue;
        }
        *entry_count += 1;
        if ft.is_dir() {
            let path = entry.path();
            if let Some(mtime) = fs::symlink_metadata(&path)
                .ok()
                .and_then(|m| m.modified().ok())
            {
                update_max_time(max_dir_modified, mtime);
            }
            accumulate_resource_dir_stats(&path, depth + 1, entry_count, max_dir_modified);
        }
    }
}

fn update_max_time(max: &mut Option<SystemTime>, candidate: SystemTime) {
    if max.is_none_or(|cur| candidate > cur) {
        *max = Some(candidate);
    }
}

pub fn validate_skill_slug(id: &str) -> Result<String> {
    if id.is_empty() {
        return Err(AgentError::InvalidInput(
            "skill slug cannot be empty".into(),
        ));
    }
    if id.len() > MAX_SKILL_SLUG_LEN {
        return Err(AgentError::InvalidInput(format!(
            "skill slug exceeds {MAX_SKILL_SLUG_LEN} characters"
        )));
    }
    if id.contains('\0') {
        return Err(AgentError::InvalidInput(
            "skill slug cannot contain NUL bytes".into(),
        ));
    }
    if id == "." || id == ".." {
        return Err(AgentError::InvalidInput(
            "skill slug cannot be '.' or '..'".into(),
        ));
    }
    if id.contains('/') || id.contains('\\') {
        return Err(AgentError::InvalidInput(
            "skill slug cannot contain path separators".into(),
        ));
    }
    if !id
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(AgentError::InvalidInput(
            "skill slug may only contain ASCII letters, digits, '_' and '-'".into(),
        ));
    }
    Ok(id.to_string())
}

pub fn parse_skill_markdown(raw: &str, fallback_id: &str) -> ParsedSkillMarkdown {
    let without_bom = raw.strip_prefix('\u{feff}').unwrap_or(raw);
    let normalized = without_bom.replace("\r\n", "\n");
    let lines: Vec<&str> = normalized.split('\n').collect();

    let mut name = String::new();
    let mut description = String::new();
    let body;

    if lines.first().map(|l| l.trim()) == Some("---") {
        if let Some(close_idx) = lines
            .iter()
            .enumerate()
            .skip(1)
            .find(|(_, l)| l.trim() == "---")
            .map(|(idx, _)| idx)
        {
            let fm_lines = &lines[1..close_idx];
            parse_frontmatter_lines(fm_lines, &mut name, &mut description);
            let raw_body = lines[(close_idx + 1)..].join("\n");
            body = raw_body
                .trim_start_matches('\n')
                .trim_end()
                .to_string();
        } else {
            body = normalized.trim_start_matches('\n').trim_end().to_string();
        }
    } else {
        body = normalized.trim_start_matches('\n').trim_end().to_string();
    }

    if name.trim().is_empty() {
        name = fallback_id.to_string();
    }

    ParsedSkillMarkdown {
        name,
        description,
        body,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlockStyle {
    Folded,
    Literal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChompMode {
    Clip,
    Strip,
    Keep,
}

fn parse_frontmatter_lines(lines: &[&str], name: &mut String, description: &mut String) {
    let mut i = 0;
    while i < lines.len() {
        let line = lines[i];
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            i += 1;
            continue;
        }
        // Only top-level keys (column 0) are parsed; nested/indented lines are skipped.
        if line.starts_with(' ') || line.starts_with('\t') {
            i += 1;
            continue;
        }

        let Some((raw_key, raw_rest)) = line.split_once(':') else {
            i += 1;
            continue;
        };
        let key = raw_key.trim();
        let rest = raw_rest.trim();

        let (value, next_idx) = if let Some((style, chomp)) = parse_block_indicator(rest) {
            read_block_scalar(lines, i + 1, style, chomp)
        } else {
            read_inline_or_plain_scalar(lines, i + 1, rest)
        };

        match key {
            "name" => *name = value,
            "description" => *description = value,
            _ => {}
        }

        i = next_idx;
    }
}

fn parse_block_indicator(rest: &str) -> Option<(BlockStyle, ChompMode)> {
    let mut parts = rest.split_whitespace();
    let head = parts.next()?;
    if let Some(second) = parts.next() {
        if !second.starts_with('#') {
            return None;
        }
    }

    let mut chars = head.chars();
    let style = match chars.next()? {
        '>' => BlockStyle::Folded,
        '|' => BlockStyle::Literal,
        _ => return None,
    };

    let mut chomp = ChompMode::Clip;
    let mut seen_chomp = false;
    let mut seen_indent = false;
    for ch in chars {
        match ch {
            '-' if !seen_chomp => {
                chomp = ChompMode::Strip;
                seen_chomp = true;
            }
            '+' if !seen_chomp => {
                chomp = ChompMode::Keep;
                seen_chomp = true;
            }
            '1'..='9' if !seen_indent => {
                seen_indent = true;
            }
            _ => return None,
        }
    }

    Some((style, chomp))
}

fn read_block_scalar(
    lines: &[&str],
    start_idx: usize,
    style: BlockStyle,
    chomp: ChompMode,
) -> (String, usize) {
    let mut end_idx = start_idx;
    while end_idx < lines.len() {
        let l = lines[end_idx];
        if l.trim().is_empty() || l.starts_with(' ') || l.starts_with('\t') {
            end_idx += 1;
        } else {
            break;
        }
    }

    let block = &lines[start_idx..end_idx];
    let min_indent = block
        .iter()
        .filter(|l| !l.trim().is_empty())
        .map(|l| count_leading_indent(l))
        .min()
        .unwrap_or(0);

    let stripped_lines: Vec<&str> = block
        .iter()
        .map(|l| {
            if l.trim().is_empty() {
                ""
            } else {
                strip_leading_indent(l, min_indent)
            }
        })
        .collect();

    let mut out = match style {
        BlockStyle::Literal => stripped_lines.join("\n"),
        BlockStyle::Folded => fold_block_lines(&stripped_lines),
    };

    match chomp {
        ChompMode::Strip | ChompMode::Clip => {
            while out.ends_with('\n') {
                out.pop();
            }
        }
        ChompMode::Keep => {}
    }

    (out, end_idx)
}

fn fold_block_lines(lines: &[&str]) -> String {
    let mut out = String::new();
    let mut pending_empty = 0usize;
    let mut prev_indented = false;

    for line in lines {
        if line.is_empty() {
            pending_empty += 1;
            continue;
        }

        let is_indented = line.starts_with(' ') || line.starts_with('\t');
        if !out.is_empty() {
            if pending_empty > 0 {
                for _ in 0..pending_empty {
                    out.push('\n');
                }
            } else if is_indented || prev_indented {
                out.push('\n');
            } else {
                out.push(' ');
            }
        }

        out.push_str(line);
        pending_empty = 0;
        prev_indented = is_indented;
    }

    if pending_empty > 0 && !out.is_empty() {
        for _ in 0..pending_empty {
            out.push('\n');
        }
    }

    out
}

fn read_inline_or_plain_scalar(
    lines: &[&str],
    next_idx: usize,
    rest: &str,
) -> (String, usize) {
    if let Some(quoted) = parse_quoted_yaml_scalar(rest) {
        return (quoted, next_idx);
    }

    let first = strip_inline_yaml_comment(rest).trim();
    if first.is_empty() {
        return (String::new(), next_idx);
    }

    let mut buf = String::from(first);
    let mut idx = next_idx;
    while idx < lines.len() {
        let l = lines[idx];
        if l.trim().is_empty() {
            break;
        }
        if !(l.starts_with(' ') || l.starts_with('\t')) {
            break;
        }
        let cont = strip_inline_yaml_comment(l).trim();
        if cont.is_empty() || cont.starts_with('#') {
            break;
        }
        buf.push(' ');
        buf.push_str(cont);
        idx += 1;
    }

    (buf, idx)
}

fn parse_quoted_yaml_scalar(rest: &str) -> Option<String> {
    if let Some(after_open) = rest.strip_prefix('"') {
        let mut escaped = false;
        let mut end_idx = None;
        for (idx, ch) in after_open.char_indices() {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                end_idx = Some(idx);
                break;
            }
        }
        if let Some(end) = end_idx {
            let trailing = &after_open[end + 1..];
            let trailing_trimmed = trailing.trim_start();
            if trailing_trimmed.is_empty()
                || (trailing.as_bytes().first().is_some_and(u8::is_ascii_whitespace)
                    && trailing_trimmed.starts_with('#'))
            {
                return Some(unescape_double_quoted(&after_open[..end]));
            }
        }
    } else if let Some(after_open) = rest.strip_prefix('\'') {
        let bytes = after_open.as_bytes();
        let mut i = 0;
        let mut end_idx = None;
        while i < bytes.len() {
            if bytes[i] == b'\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                end_idx = Some(i);
                break;
            }
            i += 1;
        }
        if let Some(end) = end_idx {
            let trailing = &after_open[end + 1..];
            let trailing_trimmed = trailing.trim_start();
            if trailing_trimmed.is_empty()
                || (trailing.as_bytes().first().is_some_and(u8::is_ascii_whitespace)
                    && trailing_trimmed.starts_with('#'))
            {
                return Some(after_open[..end].replace("''", "'"));
            }
        }
    }
    None
}

fn strip_inline_yaml_comment(s: &str) -> &str {
    let bytes = s.as_bytes();
    for (idx, &b) in bytes.iter().enumerate() {
        if b == b'#' && (idx == 0 || bytes[idx - 1].is_ascii_whitespace()) {
            return &s[..idx];
        }
    }
    s
}

fn unescape_double_quoted(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            match chars.next() {
                Some('n') => out.push('\n'),
                Some('r') => out.push('\r'),
                Some('t') => out.push('\t'),
                Some('\\') => out.push('\\'),
                Some('"') => out.push('"'),
                Some('/') => out.push('/'),
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

fn count_leading_indent(s: &str) -> usize {
    s.bytes().take_while(|b| *b == b' ' || *b == b'\t').count()
}

fn strip_leading_indent(s: &str, count: usize) -> &str {
    let mut skipped = 0usize;
    let mut byte_idx = 0usize;
    for b in s.bytes() {
        if skipped >= count || (b != b' ' && b != b'\t') {
            break;
        }
        skipped += 1;
        byte_idx += 1;
    }
    &s[byte_idx..]
}

fn slugify_skill_name(name: &str) -> String {
    let mut slug = String::new();
    let mut last_was_dash = false;
    for ch in name.trim().chars() {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch.to_ascii_lowercase());
            last_was_dash = false;
        } else if ch == '_' {
            slug.push('_');
            last_was_dash = false;
        } else if !slug.is_empty() && !last_was_dash {
            slug.push('-');
            last_was_dash = true;
        }
    }
    while slug.ends_with('-') {
        slug.pop();
    }
    if slug.len() > MAX_SKILL_SLUG_LEN {
        slug.truncate(MAX_SKILL_SLUG_LEN);
        while slug.ends_with('-') {
            slug.pop();
        }
    }
    slug
}

fn load_disabled_ids_from_disk(data_dir: &Path) -> HashSet<String> {
    let state_path = data_dir.join("skills_state.json");
    let raw = match fs::read_to_string(&state_path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return HashSet::new(),
        Err(e) => {
            log_warn(
                "skills",
                format!("failed to read '{}': {e}", state_path.display()),
            );
            return HashSet::new();
        }
    };

    if raw.trim().is_empty() {
        return HashSet::new();
    }

    if let Ok(state) = serde_json::from_str::<SkillsStateFile>(&raw) {
        return state
            .disabled_ids
            .into_iter()
            .filter(|id| validate_skill_slug(id).is_ok())
            .collect();
    }

    if let Ok(list) = serde_json::from_str::<Vec<String>>(&raw) {
        return list
            .into_iter()
            .filter(|id| validate_skill_slug(id).is_ok())
            .collect();
    }

    log_warn(
        "skills",
        format!(
            "ignoring malformed skills_state.json at '{}'",
            state_path.display()
        ),
    );
    HashSet::new()
}

fn scan_skills_root(
    root: &Path,
    source: &str,
    disabled_ids: &HashSet<String>,
    out: &mut BTreeMap<String, LoadedSkillEntry>,
) {
    let entries = match fs::read_dir(root) {
        Ok(rd) => rd,
        Err(_) => return,
    };

    for item in entries.flatten() {
        let file_type = match item.file_type() {
            Ok(ft) => ft,
            Err(_) => continue,
        };
        if file_type.is_symlink() || !file_type.is_dir() {
            continue;
        }
        let dir_name = item.file_name();
        let Some(slug) = dir_name.to_str() else {
            continue;
        };
        if validate_skill_slug(slug).is_err() {
            continue;
        }

        let skill_dir = item.path();
        let enabled = !disabled_ids.contains(slug);
        match load_skill_from_dir(slug, &skill_dir, source, enabled) {
            Ok(Some(entry)) => {
                out.insert(slug.to_string(), entry);
            }
            Ok(None) => {}
            Err(e) => {
                log_warn(
                    "skills",
                    format!("skipping skill '{}': {e}", skill_dir.display()),
                );
            }
        }
    }
}

fn is_real_dir(path: &Path) -> bool {
    fs::symlink_metadata(path)
        .map(|m| !m.file_type().is_symlink() && m.is_dir())
        .unwrap_or(false)
}

fn ensure_real_dir_if_exists(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(meta) => {
            if meta.file_type().is_symlink() || !meta.is_dir() {
                return Err(AgentError::PermissionDenied(format!(
                    "refusing to use symlink or non-directory '{}'",
                    path.display()
                )));
            }
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(AgentError::Storage(format!(
            "failed to stat directory '{}': {e}",
            path.display()
        ))),
    }
}

fn ensure_real_file_if_exists(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(meta) => {
            if meta.file_type().is_symlink() || !meta.is_file() {
                return Err(AgentError::PermissionDenied(format!(
                    "refusing to use symlink or non-file '{}'",
                    path.display()
                )));
            }
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(AgentError::Storage(format!(
            "failed to stat file '{}': {e}",
            path.display()
        ))),
    }
}

fn load_skill_from_dir(
    slug: &str,
    skill_dir: &Path,
    source: &str,
    enabled: bool,
) -> Result<Option<LoadedSkillEntry>> {
    if !is_real_dir(skill_dir) {
        return Ok(None);
    }
    let skill_md_path = skill_dir.join("SKILL.md");
    let meta = match fs::symlink_metadata(&skill_md_path) {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(AgentError::Storage(format!(
                "failed to stat '{}': {e}",
                skill_md_path.display()
            )));
        }
    };
    if meta.file_type().is_symlink() || !meta.is_file() {
        return Ok(None);
    }
    if meta.len() as usize > MAX_SKILL_MARKDOWN_BYTES {
        return Err(AgentError::LimitExceeded(format!(
            "'{}' exceeds {MAX_SKILL_MARKDOWN_BYTES} bytes",
            skill_md_path.display()
        )));
    }

    let raw_markdown = fs::read_to_string(&skill_md_path).map_err(|e| {
        AgentError::Storage(format!(
            "failed to read '{}': {e}",
            skill_md_path.display()
        ))
    })?;

    let parsed = parse_skill_markdown(&raw_markdown, slug);
    let has_scripts = is_real_dir(&skill_dir.join("scripts"));
    let has_references = is_real_dir(&skill_dir.join("references"));
    let resources = collect_skill_resources(skill_dir);

    let summary = SkillSummary {
        id: slug.to_string(),
        name: parsed.name,
        description: parsed.description,
        enabled,
        source: source.to_string(),
        dir_path: skill_dir.to_string_lossy().into_owned(),
        has_scripts,
        has_references,
        resources,
    };

    Ok(Some(LoadedSkillEntry {
        summary,
        instructions: parsed.body,
        raw_markdown,
    }))
}

fn collect_skill_resources(skill_dir: &Path) -> Vec<String> {
    let mut resources = Vec::new();
    for subdir in RESOURCE_SUBDIRS {
        let base = skill_dir.join(subdir);
        if let Ok(meta) = fs::symlink_metadata(&base) {
            if !meta.file_type().is_symlink() && meta.is_dir() {
                collect_resources_recursive(&base, subdir, 0, &mut resources);
            }
        }
        if resources.len() >= MAX_RESOURCE_FILES {
            break;
        }
    }
    resources.sort();
    resources
}

fn collect_resources_recursive(
    dir: &Path,
    rel_prefix: &str,
    depth: usize,
    out: &mut Vec<String>,
) {
    if depth >= MAX_RESOURCE_SCAN_DEPTH || out.len() >= MAX_RESOURCE_FILES {
        return;
    }

    let entries = match fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(_) => return,
    };

    for entry in entries.flatten() {
        if out.len() >= MAX_RESOURCE_FILES {
            break;
        }
        let ft = match entry.file_type() {
            Ok(ft) => ft,
            Err(_) => continue,
        };
        if ft.is_symlink() {
            continue;
        }
        let name = entry.file_name();
        let Some(name_str) = name.to_str() else {
            continue;
        };
        if name_str.starts_with('.') {
            continue;
        }
        let rel = format!("{rel_prefix}/{name_str}");
        if ft.is_file() {
            out.push(rel);
        } else if ft.is_dir() {
            collect_resources_recursive(&entry.path(), &rel, depth + 1, out);
        }
    }
}

fn atomic_write_bytes(path: &Path, data: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| {
            AgentError::Storage(format!(
                "failed to create parent directory '{}': {e}",
                parent.display()
            ))
        })?;
    }

    let file_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| AgentError::Storage("invalid target file path".into()))?;
    let seq = TMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let tmp_path = path.with_file_name(format!(".{file_name}.{seq}.tmp"));

    let write_res = (|| -> std::io::Result<()> {
        let mut f = File::create(&tmp_path)?;
        f.write_all(data)?;
        f.sync_all()?;
        Ok(())
    })();

    if let Err(e) = write_res {
        let _ = fs::remove_file(&tmp_path);
        return Err(AgentError::Storage(format!(
            "failed to write temporary file '{}': {e}",
            tmp_path.display()
        )));
    }

    if let Err(e) = fs::rename(&tmp_path, path) {
        let _ = fs::remove_file(&tmp_path);
        return Err(AgentError::Storage(format!(
            "failed to rename '{}' to '{}': {e}",
            tmp_path.display(),
            path.display()
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn validate_skill_slug_strict_rules() {
        assert_eq!(validate_skill_slug("pdf-reader").unwrap(), "pdf-reader");
        assert_eq!(validate_skill_slug("Skill_01").unwrap(), "Skill_01");
        assert_eq!(validate_skill_slug("a").unwrap(), "a");
        assert!(validate_skill_slug(&"a".repeat(64)).is_ok());

        assert!(validate_skill_slug("").is_err());
        assert!(validate_skill_slug(&"a".repeat(65)).is_err());
        assert!(validate_skill_slug(".").is_err());
        assert!(validate_skill_slug("..").is_err());
        assert!(validate_skill_slug("../secret").is_err());
        assert!(validate_skill_slug("foo/bar").is_err());
        assert!(validate_skill_slug("foo\\bar").is_err());
        assert!(validate_skill_slug("foo\0bar").is_err());
        assert!(validate_skill_slug("bad slug").is_err());
        assert!(validate_skill_slug("bad.slug").is_err());
    }

    #[test]
    fn parse_skill_markdown_multiline_folded_and_literal() {
        let md = concat!(
            "\u{feff}---\r\n",
            "name: \"ps5-deploy\"\r\n",
            "description: >-\r\n",
            "  Builds and deploys the PS5 ELF payload\r\n",
            "  over elfldr TCP port 9021.\r\n",
            "\r\n",
            "  Includes health check steps.\r\n",
            "extra:\r\n",
            "  nested: ignored\r\n",
            "---\r\n",
            "\r\n",
            "# Instructions\r\n",
            "Run the payload.\r\n",
        );

        let parsed = parse_skill_markdown(md, "fallback");
        assert_eq!(parsed.name, "ps5-deploy");
        assert_eq!(
            parsed.description,
            "Builds and deploys the PS5 ELF payload over elfldr TCP port 9021.\nIncludes health check steps."
        );
        assert_eq!(parsed.body, "# Instructions\nRun the payload.");

        let literal_md = "---\nname: 'literal-skill'\ndescription: |\n  Line one\n  Line two\n---\nBody text\n";
        let literal = parse_skill_markdown(literal_md, "fallback");
        assert_eq!(literal.name, "literal-skill");
        assert_eq!(literal.description, "Line one\nLine two");
        assert_eq!(literal.body, "Body text");

        let plain_md = "# Plain Skill\nNo frontmatter here.";
        let plain = parse_skill_markdown(plain_md, "plain-id");
        assert_eq!(plain.name, "plain-id");
        assert_eq!(plain.description, "");
        assert_eq!(plain.body, "# Plain Skill\nNo frontmatter here.");
    }

    #[test]
    fn import_toggle_load_and_delete_lifecycle() {
        let data_tmp = TempDir::new().unwrap();
        let ws_tmp = TempDir::new().unwrap();

        // Seed a workspace skill with scripts and references.
        let ws_skill_dir = ws_tmp.path().join(".agents").join("skills").join("alpha-ws");
        fs::create_dir_all(ws_skill_dir.join("scripts")).unwrap();
        fs::create_dir_all(ws_skill_dir.join("references")).unwrap();
        fs::write(
            ws_skill_dir.join("SKILL.md"),
            "---\nname: Alpha Workspace\ndescription: Workspace helper\n---\nUse script.",
        )
        .unwrap();
        fs::write(ws_skill_dir.join("scripts").join("run.sh"), "#!/bin/sh\necho ok\n").unwrap();
        fs::write(ws_skill_dir.join("references").join("guide.md"), "# Guide\n").unwrap();

        let mgr = SkillManager::new(data_tmp.path(), Some(ws_tmp.path()));

        // Import deriving slug from frontmatter name.
        let imported = mgr
            .import_skill(
                None,
                "---\nname: \"Beta Custom!\"\ndescription: >-\n  Multi-line\n  description\n---\n# Beta\nDo thing.\n",
            )
            .unwrap();
        assert_eq!(imported.id, "beta-custom");
        assert_eq!(imported.name, "Beta Custom!");
        assert_eq!(imported.description, "Multi-line description");
        assert!(imported.enabled);
        assert_eq!(imported.source, "user");

        // Reject traversal slug on import.
        assert!(mgr
            .import_skill(Some("../evil"), "---\nname: evil\n---\nno")
            .is_err());

        // Deterministic listing sorted by id.
        let all = mgr.list_skills();
        assert_eq!(
            all.iter().map(|s| s.id.as_str()).collect::<Vec<_>>(),
            vec!["alpha-ws", "beta-custom"]
        );
        assert!(all[0].has_scripts);
        assert!(all[0].has_references);
        assert_eq!(
            all[0].resources,
            vec!["references/guide.md", "scripts/run.sh"]
        );

        // User skill overrides workspace skill of same id.
        mgr.import_skill(
            Some("alpha-ws"),
            "---\nname: Alpha Override\ndescription: User override\n---\nUser instructions.",
        )
        .unwrap();
        let overridden = mgr.load_skill("alpha-ws").unwrap();
        assert_eq!(overridden.summary.source, "user");
        assert_eq!(overridden.summary.name, "Alpha Override");

        // Case-insensitive load by name.
        let loaded_by_name = mgr.load_skill("beta custom!").unwrap();
        assert_eq!(loaded_by_name.summary.id, "beta-custom");
        assert_eq!(loaded_by_name.instructions, "# Beta\nDo thing.");

        // Toggle disable & verify persistence across restart.
        let disabled = mgr.set_enabled("beta-custom", false).unwrap();
        assert!(!disabled.enabled);
        let enabled_only = mgr.list_enabled_summaries();
        assert_eq!(enabled_only.len(), 1);
        assert_eq!(enabled_only[0].id, "alpha-ws");

        let mgr_reloaded = SkillManager::new(data_tmp.path(), Some(ws_tmp.path()));
        let reloaded_beta = mgr_reloaded.load_skill("beta-custom").unwrap();
        assert!(!reloaded_beta.summary.enabled);

        // Toggle back on.
        let reenabled = mgr_reloaded.set_enabled("beta-custom", true).unwrap();
        assert!(reenabled.enabled);

        // set_disabled_ids in-memory override.
        mgr_reloaded.set_disabled_ids(HashSet::from(["alpha-ws".to_string()]));
        assert_eq!(
            mgr_reloaded
                .list_enabled_summaries()
                .iter()
                .map(|s| s.id.as_str())
                .collect::<Vec<_>>(),
            vec!["beta-custom"]
        );

        // Delete removes user skill; deleting missing returns NotFound; traversal rejected.
        mgr_reloaded.delete_skill("beta-custom").unwrap();
        assert!(mgr_reloaded.load_skill("beta-custom").is_err());
        assert!(matches!(
            mgr_reloaded.delete_skill("never-existed"),
            Err(AgentError::NotFound(_))
        ));
        assert!(mgr_reloaded.delete_skill("../nope").is_err());
    }

    #[test]
    fn parse_skill_markdown_quoted_scalars_with_inline_comments() {
        let md = concat!(
            "---\n",
            "name: \"Skill #1\" # trailing double-quote comment\n",
            "description: 'It''s #2 # inside single quotes'   # trailing comment\n",
            "---\n",
            "Body\n",
        );
        let parsed = parse_skill_markdown(md, "fallback");
        assert_eq!(parsed.name, "Skill #1");
        assert_eq!(parsed.description, "It's #2 # inside single quotes");
        assert_eq!(parsed.body, "Body");
    }

    #[cfg(unix)]
    #[test]
    fn symlink_checks_block_scripts_references_import_and_delete() {
        use std::os::unix::fs::{symlink, PermissionsExt};
        let _ = PermissionsExt::from_mode(0o755);
        let data_tmp = TempDir::new().unwrap();
        let target_tmp = TempDir::new().unwrap();
        let real_dir = target_tmp.path().join("outside");
        fs::create_dir_all(&real_dir).unwrap();
        fs::write(real_dir.join("secret.txt"), "secret").unwrap();

        let mgr = SkillManager::new(data_tmp.path(), None);

        // Symlinked scripts/references directories must not report has_scripts / has_references.
        let s_dir = mgr.data_skills_dir().join("sym-res");
        fs::create_dir_all(&s_dir).unwrap();
        fs::write(s_dir.join("SKILL.md"), "---\nname: sym-res\n---\nbody").unwrap();
        symlink(&real_dir, s_dir.join("scripts")).unwrap();
        symlink(&real_dir, s_dir.join("references")).unwrap();
        let doc = mgr.load_skill("sym-res").unwrap();
        assert!(!doc.summary.has_scripts);
        assert!(!doc.summary.has_references);

        // Symlinked skill directory must be rejected on import and delete.
        let sym_skill = mgr.data_skills_dir().join("sym-skill");
        symlink(&real_dir, &sym_skill).unwrap();
        assert!(matches!(
            mgr.import_skill(Some("sym-skill"), "---\nname: sym-skill\n---\nx"),
            Err(AgentError::PermissionDenied(_))
        ));
        assert!(matches!(
            mgr.delete_skill("sym-skill"),
            Err(AgentError::PermissionDenied(_))
        ));
    }

    #[test]
    fn scan_cache_hits_and_invalidates_on_mutations_mtime_and_workspace_dirs() {
        use std::fs::OpenOptions;
        use std::time::Duration;

        let data_tmp = TempDir::new().unwrap();
        let ws1_tmp = TempDir::new().unwrap();
        let ws2_tmp = TempDir::new().unwrap();

        let ws1_skills = ws1_tmp.path().join(".agents").join("skills");
        let alpha_dir = ws1_skills.join("alpha");
        fs::create_dir_all(&alpha_dir).unwrap();
        let alpha_md = alpha_dir.join("SKILL.md");
        fs::write(&alpha_md, "---\nname: Alpha\ndescription: v1\n---\nBody v1").unwrap();

        let mut mgr = SkillManager::new(data_tmp.path(), Some(ws1_tmp.path()));
        assert_eq!(mgr.disk_scan_count(), 0);

        // First read populates cache (1 disk scan).
        let first = mgr.list_skills();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].description, "v1");
        assert_eq!(mgr.disk_scan_count(), 1);

        // Repeated queries reuse cached entries without re-reading SKILL.md.
        assert_eq!(mgr.list_skills().len(), 1);
        assert_eq!(mgr.list_enabled_summaries().len(), 1);
        assert_eq!(mgr.load_skill("alpha").unwrap().instructions, "Body v1");
        assert_eq!(mgr.disk_scan_count(), 1);

        // Toggle via set_enabled and set_disabled_ids reflects immediately without stale cache.
        let toggled = mgr.set_enabled("alpha", false).unwrap();
        assert!(!toggled.enabled);
        assert!(mgr.list_enabled_summaries().is_empty());
        assert!(!mgr.load_skill("alpha").unwrap().summary.enabled);
        assert_eq!(mgr.disk_scan_count(), 1);

        mgr.set_disabled_ids(HashSet::new());
        assert_eq!(mgr.list_enabled_summaries().len(), 1);
        assert!(mgr.load_skill("alpha").unwrap().summary.enabled);
        assert_eq!(mgr.disk_scan_count(), 1);

        // Out-of-band SKILL.md edit with identical byte length + advanced mtime invalidates cache.
        fs::write(&alpha_md, "---\nname: Alpha\ndescription: v2\n---\nBody v2").unwrap();
        let future = SystemTime::now() + Duration::from_secs(10);
        OpenOptions::new()
            .write(true)
            .open(&alpha_md)
            .unwrap()
            .set_modified(future)
            .unwrap();
        let after_mtime = mgr.list_skills();
        assert_eq!(after_mtime[0].description, "v2");
        assert_eq!(mgr.load_skill("alpha").unwrap().instructions, "Body v2");
        assert_eq!(mgr.disk_scan_count(), 2);

        // Repeated reads hit cache again after mtime rescan.
        assert_eq!(mgr.list_enabled_summaries().len(), 1);
        assert_eq!(mgr.disk_scan_count(), 2);

        // Out-of-band resource directory/file creation invalidates cache cleanly.
        let scripts_dir = alpha_dir.join("scripts");
        fs::create_dir_all(&scripts_dir).unwrap();
        fs::write(scripts_dir.join("helper.sh"), "#!/bin/sh\n").unwrap();
        let with_script = mgr.list_skills();
        assert!(with_script[0].has_scripts);
        assert_eq!(with_script[0].resources, vec!["scripts/helper.sh"]);
        assert_eq!(mgr.disk_scan_count(), 3);

        // import_skill invalidates cache immediately even with same-tick writes.
        let imported = mgr
            .import_skill(
                Some("beta"),
                "---\nname: Beta\ndescription: imp1\n---\nBeta body",
            )
            .unwrap();
        assert_eq!(imported.id, "beta");
        assert_eq!(mgr.list_skills().len(), 2);
        assert_eq!(mgr.disk_scan_count(), 4);
        assert_eq!(mgr.load_skill("beta").unwrap().instructions, "Beta body");
        assert_eq!(mgr.disk_scan_count(), 4);

        // Re-importing same slug with same byte length invalidates immediately.
        mgr.import_skill(
            Some("beta"),
            "---\nname: Beta\ndescription: imp2\n---\nBeta body",
        )
        .unwrap();
        assert_eq!(mgr.load_skill("beta").unwrap().summary.description, "imp2");
        assert_eq!(mgr.disk_scan_count(), 5);

        // delete_skill invalidates cache immediately.
        mgr.delete_skill("beta").unwrap();
        assert_eq!(mgr.list_skills().len(), 1);
        assert!(mgr.load_skill("beta").is_err());
        assert_eq!(mgr.disk_scan_count(), 6);

        // Adding another workspace skills dir invalidates cache and respects the new root.
        let ws2_skills = ws2_tmp.path().join(".agents").join("skills");
        let gamma_dir = ws2_skills.join("gamma");
        fs::create_dir_all(&gamma_dir).unwrap();
        fs::write(
            gamma_dir.join("SKILL.md"),
            "---\nname: Gamma\ndescription: ws2\n---\nGamma body",
        )
        .unwrap();
        mgr.add_workspace_skills_dir(&ws2_skills);
        let after_ws_add = mgr.list_skills();
        assert_eq!(
            after_ws_add.iter().map(|s| s.id.as_str()).collect::<Vec<_>>(),
            vec!["alpha", "gamma"]
        );
        assert_eq!(mgr.disk_scan_count(), 7);
        assert_eq!(mgr.load_skill("gamma").unwrap().instructions, "Gamma body");
        assert_eq!(mgr.disk_scan_count(), 7);
    }
}
