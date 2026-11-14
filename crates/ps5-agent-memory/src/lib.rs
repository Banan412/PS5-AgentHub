use chrono::{DateTime, Utc};
use ps5_agent_config::MemoryConfig;
use ps5_agent_core::{
    now_iso, now_utc, AgentError, ChatAttachment, ChatMessage, ChatRole, InferenceMessage,
    MemoryItem, MemoryPermissions, MemoryScope, Result, StorageBackend,
};
use std::collections::HashSet;
use std::fmt::Write as _;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct MemoryQuery<'a> {
    pub reader_agent_id: &'a str,
    pub permissions: &'a MemoryPermissions,
    pub query_text: &'a str,
    pub conversation_id: Option<&'a str>,
    pub task_id: Option<&'a str>,
    pub namespace: Option<&'a str>,
    pub scopes: Option<&'a [MemoryScope]>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct ScoredMemory {
    pub item: MemoryItem,
    pub score: f64,
}

#[derive(Debug, Clone)]
pub struct ConversationContext {
    pub messages: Vec<InferenceMessage>,
    pub archived_count: usize,
    pub rolling_summary: Option<String>,
    pub estimated_tokens: usize,
    pub truncated_messages: usize,
}

/// Approximate fixed token cost charged per attached image instead of raw base64 character count.
const IMAGE_ATTACHMENT_TOKEN_ESTIMATE: usize = 85;
/// Approximate fixed token cost charged for non-image inline base64 data URIs.
const DATA_URI_PLACEHOLDER_TOKENS: usize = 16;

/// Fast BPE-approximate token count (~4 characters per token).
pub fn estimate_tokens(text: &str) -> usize {
    if text.is_empty() {
        0
    } else {
        ((text.chars().count() + 3) / 4).max(1)
    }
}

fn find_ascii_ci(haystack: &str, from: usize, needle: &[u8]) -> Option<usize> {
    let bytes = haystack.as_bytes();
    if from >= bytes.len() || needle.is_empty() || bytes.len() - from < needle.len() {
        return None;
    }
    bytes[from..]
        .windows(needle.len())
        .position(|w| w.eq_ignore_ascii_case(needle))
        .map(|rel| from + rel)
}

fn contains_ascii_ci(haystack: &str, needle: &str) -> bool {
    if needle.is_empty() {
        return true;
    }
    find_ascii_ci(haystack, 0, needle.as_bytes()).is_some()
}

fn starts_with_ascii_ci(text: &str, prefix: &[u8]) -> bool {
    text.as_bytes()
        .get(..prefix.len())
        .is_some_and(|head| head.eq_ignore_ascii_case(prefix))
}

/// Finds the next inline `data:<mime>;base64,<payload>` span starting at or after `from`.
/// Returns `(start_byte, end_byte, mime, payload_len)`.
fn find_next_base64_data_url(text: &str, mut from: usize) -> Option<(usize, usize, &str, usize)> {
    while let Some(uri_start) = find_ascii_ci(text, from, b"data:") {
        let rest = &text[uri_start..];
        let uri_end_rel = rest
            .find(|c: char| c.is_whitespace() || matches!(c, ')' | '"' | '\'' | '<' | '>'))
            .unwrap_or(rest.len());
        let candidate = &rest[..uri_end_rel];
        if let Some(comma) = candidate.find(',') {
            let header = &candidate["data:".len()..comma];
            if comma <= 128 && contains_ascii_ci(header, ";base64") {
                let mime = header.split(';').next().unwrap_or("").trim();
                let payload_len = candidate.len().saturating_sub(comma + 1);
                return Some((uri_start, uri_start + uri_end_rel, mime, payload_len));
            }
        }
        from = uri_start + "data:".len();
    }
    None
}

/// Replaces inline base64 `data:...;base64,...` strings with compact summaries so
/// embedded payloads in historical turns do not blow up model context windows.
fn sanitize_inline_data_urls(text: &str) -> String {
    let Some(first) = find_next_base64_data_url(text, 0) else {
        return text.to_string();
    };
    let mut out = String::with_capacity(text.len().min(4096));
    let mut cursor = 0usize;
    let mut next = Some(first);
    while let Some((start, end, mime, payload_len)) = next {
        if start > cursor {
            out.push_str(&text[cursor..start]);
        }
        let label = if mime.is_empty() { "binary" } else { mime };
        let _ = write!(out, "[inline {label} data omitted ({payload_len} chars)]");
        cursor = end;
        next = find_next_base64_data_url(text, cursor);
    }
    if cursor < text.len() {
        out.push_str(&text[cursor..]);
    }
    out
}

/// Estimates text tokens while replacing inline base64 `data:` URIs with a bounded
/// per-attachment token estimate so raw base64 payloads do not truncate text context.
fn estimate_text_tokens_without_data_urls(text: &str) -> usize {
    if text.is_empty() {
        return 0;
    }
    let Some(first) = find_next_base64_data_url(text, 0) else {
        return estimate_tokens(text);
    };

    let mut total_tokens = 0usize;
    let mut cursor = 0usize;
    let mut next = Some(first);
    while let Some((start, end, mime, _)) = next {
        if start > cursor {
            total_tokens += estimate_tokens(&text[cursor..start]);
        }
        let cost = if starts_with_ascii_ci(mime, b"image/") {
            IMAGE_ATTACHMENT_TOKEN_ESTIMATE
        } else {
            DATA_URI_PLACEHOLDER_TOKENS
        };
        total_tokens += cost;
        cursor = end;
        next = find_next_base64_data_url(text, cursor);
    }

    if cursor < text.len() {
        total_tokens += estimate_tokens(&text[cursor..]);
    }
    total_tokens.max(1)
}

fn image_attachment_url(att: &ChatAttachment) -> Option<&str> {
    let url = att.data_url.as_deref()?.trim();
    if url.is_empty() {
        return None;
    }
    let is_image = att.kind.eq_ignore_ascii_case("image")
        || starts_with_ascii_ci(&att.mime_type, b"image/")
        || url.starts_with("data:image/");
    if is_image {
        Some(url)
    } else {
        None
    }
}

fn estimate_message_tokens(msg: &InferenceMessage) -> usize {
    4 + estimate_tokens(&msg.role)
        + estimate_text_tokens_without_data_urls(&msg.content)
        + msg.image_urls
            .len()
            .saturating_mul(IMAGE_ATTACHMENT_TOKEN_ESTIMATE)
}

fn scope_label(scope: MemoryScope) -> &'static str {
    match scope {
        MemoryScope::Conversation => "conversation",
        MemoryScope::Agent => "agent",
        MemoryScope::Project => "project",
        MemoryScope::Task => "task",
        MemoryScope::UserPreference => "user_preference",
    }
}

fn chat_role_str(role: ChatRole) -> &'static str {
    match role {
        ChatRole::User => "user",
        ChatRole::Assistant => "assistant",
        ChatRole::System => "system",
        ChatRole::Tool => "tool",
    }
}

fn can_read_memory(reader_agent_id: &str, perms: &MemoryPermissions, item: &MemoryItem) -> bool {
    match item.scope {
        MemoryScope::Agent => {
            if item.owner_agent_id.as_deref() == Some(reader_agent_id) {
                perms.read_own
            } else {
                perms.cross_agent_read
            }
        }
        MemoryScope::Conversation | MemoryScope::Project | MemoryScope::Task => perms.read_shared,
        MemoryScope::UserPreference => perms.read_user_prefs,
    }
}

fn tokenize_query(text: &str) -> Vec<String> {
    let mut tokens: Vec<String> = text
        .split(|c: char| !c.is_alphanumeric() && c != '_')
        .filter(|t| !t.is_empty())
        .map(|t| t.to_ascii_lowercase())
        .collect();
    if tokens.iter().any(|t| t.len() >= 2) {
        tokens.retain(|t| t.len() >= 2);
    }
    let mut seen = HashSet::with_capacity(tokens.len());
    tokens.retain(|t| seen.insert(t.clone()));
    tokens
}

fn truncate_chars(text: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }
    let count = text.chars().count();
    if count <= max_chars {
        return text.to_string();
    }
    if max_chars <= 3 {
        return text.chars().take(max_chars).collect();
    }
    let mut out: String = text.chars().take(max_chars - 3).collect();
    out.push_str("...");
    out
}

fn format_archived_turn(msg: &ChatMessage) -> String {
    let speaker = match msg.role {
        ChatRole::User => "User",
        ChatRole::Assistant => "Assistant",
        ChatRole::System => "System",
        ChatRole::Tool => "Tool",
    };
    let sanitized = sanitize_inline_data_urls(&msg.content);
    let trimmed = sanitized.trim();
    let snippet = if trimmed.is_empty() && !msg.attachments.is_empty() {
        let names: Vec<&str> = msg
            .attachments
            .iter()
            .map(|a| a.name.as_str())
            .filter(|n| !n.is_empty())
            .take(3)
            .collect();
        if names.is_empty() {
            format!("[{} attachment(s)]", msg.attachments.len())
        } else {
            truncate_chars(&format!("[attachments: {}]", names.join(", ")), 160)
        }
    } else {
        truncate_chars(trimmed, 160)
    };
    format!("- {speaker}: {snippet}")
}

fn omission_marker(omitted: usize) -> String {
    let noun = if omitted == 1 { "turn" } else { "turns" };
    format!("- ... ({omitted} earlier {noun} omitted)")
}

fn synthesize_rolling_summary(archived: &[&ChatMessage], max_chars: usize) -> String {
    if archived.is_empty() || max_chars == 0 {
        return String::new();
    }
    if archived.len() == 1 {
        return truncate_chars(&format_archived_turn(archived[0]), max_chars);
    }

    // Reserve initial context from the front (~1/3 of budget) and spend the remaining
    // budget on the newest archived turns so recent history is preserved alongside opening turns.
    let mut head_lines: Vec<String> = Vec::new();
    let mut head_chars = 0usize;
    let head_budget = (max_chars / 3).max(1);

    for (idx, msg) in archived.iter().enumerate() {
        // Leave the newest archived turn for the tail pass.
        if idx == archived.len() - 1 {
            break;
        }
        let mut line = format_archived_turn(msg);
        if idx == 0 {
            let max_first = if max_chars >= 80 {
                head_budget.max(40)
            } else {
                max_chars
            };
            if line.chars().count() > max_first {
                line = truncate_chars(&line, max_first);
            }
            head_chars = line.chars().count();
            head_lines.push(line);
            continue;
        }
        let line_chars = line.chars().count();
        let next_chars = head_chars + 1 + line_chars;
        if next_chars > head_budget {
            break;
        }
        head_chars = next_chars;
        head_lines.push(line);
    }

    let head_count = head_lines.len();
    let mut tail_lines_rev: Vec<String> = Vec::new();
    let mut tail_chars = 0usize;

    for msg in archived[head_count..].iter().rev() {
        let line = format_archived_turn(msg);
        let line_chars = line.chars().count();
        let new_tail_chars = if tail_lines_rev.is_empty() {
            line_chars
        } else {
            tail_chars + 1 + line_chars
        };

        let omitted = archived.len() - head_count - (tail_lines_rev.len() + 1);
        let sep_cost = if omitted > 0 {
            omission_marker(omitted).chars().count() + 1
        } else {
            0
        };
        let total = if head_chars == 0 {
            new_tail_chars + sep_cost
        } else {
            head_chars + 1 + sep_cost + new_tail_chars
        };
        if total > max_chars {
            break;
        }
        tail_chars = new_tail_chars;
        tail_lines_rev.push(line);
    }

    if tail_lines_rev.is_empty() && head_count < archived.len() {
        let newest = format_archived_turn(archived[archived.len() - 1]);
        let omitted = archived.len() - head_count - 1;
        let overhead = if omitted > 0 {
            omission_marker(omitted).chars().count() + 1
        } else {
            0
        };
        let avail_for_newest =
            max_chars.saturating_sub(head_chars + usize::from(head_chars > 0) + overhead);
        if avail_for_newest >= 24 {
            tail_lines_rev.push(truncate_chars(&newest, avail_for_newest));
        } else if head_chars > max_chars {
            return truncate_chars(&newest, max_chars);
        }
    }

    tail_lines_rev.reverse();
    let omitted = archived
        .len()
        .saturating_sub(head_count + tail_lines_rev.len());

    let mut combined = Vec::with_capacity(head_lines.len() + 1 + tail_lines_rev.len());
    combined.extend(head_lines);
    if omitted > 0 && !tail_lines_rev.is_empty() {
        combined.push(omission_marker(omitted));
    }
    combined.extend(tail_lines_rev);

    let joined = combined.join("\n");
    if joined.chars().count() <= max_chars {
        joined
    } else {
        truncate_chars(&joined, max_chars)
    }
}

pub struct MemoryManager {
    storage: Arc<dyn StorageBackend>,
    config: MemoryConfig,
    write_lock: std::sync::Mutex<()>,
}

impl MemoryManager {
    pub fn new(storage: Arc<dyn StorageBackend>, config: MemoryConfig) -> Self {
        Self {
            storage,
            config,
            write_lock: std::sync::Mutex::new(()),
        }
    }

    pub fn config(&self) -> &MemoryConfig {
        &self.config
    }

    /// Persists a memory item, enforcing `max_memories_per_agent` by archiving
    /// the lowest-importance oldest active memories for `owner_agent_id` if needed.
    pub fn store_memory(&self, item: MemoryItem) -> Result<MemoryItem> {
        let _guard = self
            .write_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !item.archived && self.config.max_memories_per_agent > 0 {
            if let Some(ref owner) = item.owner_agent_id {
                self.enforce_agent_memory_cap(owner, &item.id)?;
            }
        }
        self.storage.upsert_memory(&item)?;
        Ok(item)
    }

    fn enforce_agent_memory_cap(&self, owner_agent_id: &str, incoming_id: &str) -> Result<()> {
        let cap = self.config.max_memories_per_agent;
        if cap == 0 {
            return Ok(());
        }

        let (total_owner_count, _) = self.storage.memory_stats_for_agent(owner_agent_id)?;
        let max_query_limit = (i64::MAX as usize) / 2;
        let owner_count_hint = usize::try_from(total_owner_count).unwrap_or(max_query_limit);
        let mut fetch_limit = owner_count_hint
            .max(cap.saturating_add(64))
            .max(1024)
            .min(max_query_limit);

        let mut active = loop {
            let batch = self.storage.query_memories(
                None,
                None,
                Some(owner_agent_id),
                false,
                fetch_limit,
            )?;
            if batch.len() >= fetch_limit && fetch_limit < max_query_limit {
                fetch_limit = fetch_limit.saturating_mul(2).min(max_query_limit);
                continue;
            }
            break batch;
        };

        let incoming_exists = active.iter().any(|m| m.id == incoming_id);
        let max_allowed_before_upsert = if incoming_exists {
            cap
        } else {
            cap.saturating_sub(1)
        };
        if active.len() <= max_allowed_before_upsert {
            return Ok(());
        }

        // Archive lowest-importance, oldest memories first without evicting `incoming_id`.
        active.retain(|m| m.id != incoming_id);
        active.sort_by(|a, b| {
            a.importance
                .cmp(&b.importance)
                .then_with(|| a.updated_at.cmp(&b.updated_at))
                .then_with(|| a.id.cmp(&b.id))
        });

        let keep_others = cap.saturating_sub(1);
        let to_archive = active.len().saturating_sub(keep_others);
        for mut old in active.into_iter().take(to_archive) {
            old.archived = true;
            old.updated_at = now_iso();
            self.storage.upsert_memory(&old)?;
        }
        if to_archive > 0 {
            ps5_agent_logging::log_debug(
                "memory",
                format!(
                    "Archived {to_archive} memory entries for agent '{owner_agent_id}' to enforce cap"
                ),
            );
        }
        Ok(())
    }

    /// Permission-checked write for an agent across all 5 `MemoryScope`s.
    pub fn write_agent_memory(
        &self,
        caller_agent_id: &str,
        perms: &MemoryPermissions,
        mut item: MemoryItem,
    ) -> Result<MemoryItem> {
        match item.scope {
            MemoryScope::Agent => {
                if !perms.write_own {
                    return Err(AgentError::PermissionDenied(format!(
                        "agent '{caller_agent_id}' lacks write_own memory permission"
                    )));
                }
                if let Some(ref owner) = item.owner_agent_id {
                    if owner != caller_agent_id {
                        return Err(AgentError::PermissionDenied(format!(
                            "agent '{caller_agent_id}' cannot write private memory owned by '{owner}'"
                        )));
                    }
                } else {
                    item.owner_agent_id = Some(caller_agent_id.to_string());
                }
            }
            MemoryScope::Conversation | MemoryScope::Project | MemoryScope::Task => {
                if !perms.write_shared {
                    return Err(AgentError::PermissionDenied(format!(
                        "agent '{caller_agent_id}' lacks write_shared memory permission"
                    )));
                }
            }
            MemoryScope::UserPreference => {
                if !perms.write_user_prefs {
                    return Err(AgentError::PermissionDenied(format!(
                        "agent '{caller_agent_id}' lacks write_user_prefs memory permission"
                    )));
                }
            }
        }
        self.store_memory(item)
    }

    /// Permission-aware read across all 5 `MemoryScope`s.
    pub fn read_agent_memory(
        &self,
        reader_agent_id: &str,
        perms: &MemoryPermissions,
        target_agent_id: Option<&str>,
        scope: Option<MemoryScope>,
        namespace: Option<&str>,
        include_archived: bool,
        limit: usize,
    ) -> Result<Vec<MemoryItem>> {
        if let Some(target) = target_agent_id {
            if target != reader_agent_id && !perms.cross_agent_read {
                return Err(AgentError::PermissionDenied(format!(
                    "agent '{reader_agent_id}' lacks cross_agent_read permission for '{target}'"
                )));
            }
        }

        if let Some(explicit_scope) = scope {
            let allowed = match explicit_scope {
                MemoryScope::Agent => {
                    if target_agent_id.is_some_and(|t| t != reader_agent_id) {
                        perms.cross_agent_read
                    } else {
                        perms.read_own
                    }
                }
                MemoryScope::Conversation | MemoryScope::Project | MemoryScope::Task => {
                    perms.read_shared
                }
                MemoryScope::UserPreference => perms.read_user_prefs,
            };
            if !allowed {
                return Err(AgentError::PermissionDenied(format!(
                    "agent '{reader_agent_id}' lacks read permission for scope '{}'",
                    scope_label(explicit_scope)
                )));
            }
        }

        if limit == 0 {
            return Ok(Vec::new());
        }

        let fetch_limit = limit.saturating_mul(4).max(limit).min(2000);
        let raw = self.storage.query_memories(
            scope,
            namespace,
            target_agent_id,
            include_archived,
            fetch_limit,
        )?;

        let mut filtered: Vec<MemoryItem> = raw
            .into_iter()
            .filter(|m| can_read_memory(reader_agent_id, perms, m))
            .collect();
        filtered.truncate(limit);
        Ok(filtered)
    }

    /// Retrieves and scores relevant memories using keyword match, recency decay,
    /// importance weight, and conversation/task affinity.
    pub fn retrieve_context_scored(&self, query: &MemoryQuery<'_>) -> Result<Vec<ScoredMemory>> {
        let limit = query.limit.unwrap_or(self.config.max_context_memories);
        if limit == 0 {
            return Ok(Vec::new());
        }
        if query.scopes.is_some_and(|s| s.is_empty()) {
            return Ok(Vec::new());
        }

        let scope_hint = query
            .scopes
            .and_then(|s| if s.len() == 1 { Some(s[0]) } else { None });
        let scan_limit = limit.saturating_mul(8).clamp(32, 256).max(limit.min(500));
        let candidates = self
            .storage
            .query_memories(scope_hint, query.namespace, None, false, scan_limit)?;
        let tokens = tokenize_query(query.query_text);
        let now = now_utc();

        let mut scored: Vec<ScoredMemory> = candidates
            .into_iter()
            .filter(|item| can_read_memory(query.reader_agent_id, query.permissions, item))
            .filter(|item| query.scopes.map_or(true, |s| s.contains(&item.scope)))
            .filter(|item| {
                if item.scope == MemoryScope::Conversation {
                    match (query.conversation_id, item.conversation_id.as_deref()) {
                        (Some(qc), Some(ic)) => qc == ic,
                        _ => true,
                    }
                } else {
                    true
                }
            })
            .filter(|item| {
                if item.scope == MemoryScope::Task {
                    match (query.task_id, item.task_id.as_deref()) {
                        (Some(qt), Some(it)) => qt == it,
                        _ => true,
                    }
                } else {
                    true
                }
            })
            .map(|item| {
                let score = score_memory(&item, &tokens, query.conversation_id, query.task_id, now);
                ScoredMemory { item, score }
            })
            .collect();

        scored.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| b.item.importance.cmp(&a.item.importance))
                .then_with(|| b.item.updated_at.cmp(&a.item.updated_at))
                .then_with(|| a.item.id.cmp(&b.item.id))
        });
        scored.truncate(limit);
        Ok(scored)
    }

    pub fn retrieve_context(&self, query: &MemoryQuery<'_>) -> Result<Vec<MemoryItem>> {
        Ok(self
            .retrieve_context_scored(query)?
            .into_iter()
            .map(|s| s.item)
            .collect())
    }

    /// Formats retrieved memories into a prompt block strictly bounded by `max_chars`.
    pub fn format_memory_block(&self, memories: &[MemoryItem], max_chars: usize) -> String {
        if memories.is_empty() || max_chars == 0 {
            return String::new();
        }

        let header = "[Memory Context]\n";
        let header_chars = header.chars().count();
        if max_chars <= header_chars {
            return String::new();
        }

        let mut out = String::from(header);
        let mut used_chars = header_chars;

        for mem in memories {
            let body = mem
                .summary
                .as_deref()
                .filter(|s| !s.trim().is_empty() && mem.content.chars().count() > 240)
                .unwrap_or(&mem.content);
            let line = format!("- [{}:{}] {}\n", scope_label(mem.scope), mem.key, body.trim());
            let line_chars = line.chars().count();

            if used_chars + line_chars <= max_chars {
                out.push_str(&line);
                used_chars += line_chars;
            } else {
                if used_chars == header_chars {
                    let remaining = max_chars - used_chars;
                    let clipped = truncate_chars(line.trim_end_matches('\n'), remaining);
                    out.push_str(&clipped);
                }
                break;
            }
        }

        if out.chars().count() == header_chars {
            return String::new();
        }
        out.trim_end_matches('\n').to_string()
    }

    pub fn format_memory_block_default(&self, memories: &[MemoryItem]) -> String {
        self.format_memory_block(memories, self.config.max_memory_context_chars)
    }

    /// Builds an inference-ready conversation context:
    /// 1. Automatically archives old chat messages if active history > `archive_after_messages`.
    /// 2. Maintains a rolling summary of archived turns in `MemoryScope::Conversation`.
    /// 3. Folds system prompt, rolling summary, and memory block into `messages[0]`.
    /// 4. Truncates oldest turns as needed so total estimated tokens <= `token_budget`.
    pub fn build_conversation_context(
        &self,
        conversation_id: &str,
        system_prompt: &str,
        memory_block: Option<&str>,
        token_budget: usize,
    ) -> Result<ConversationContext> {
        let fetch_cap = if self.config.archive_after_messages == 0 {
            1024
        } else {
            self.config.archive_after_messages.saturating_add(1024)
        };
        let active_before = self
            .storage
            .list_chat_messages(conversation_id, false, fetch_cap)?;

        let archived_count = if self.config.archive_after_messages > 0
            && active_before.len() > self.config.archive_after_messages
        {
            self.storage
                .archive_old_chat_messages(conversation_id, self.config.keep_recent_messages)?
        } else {
            0
        };

        let (active_turns, rolling_summary) = if archived_count == 0 {
            let summary_id = format!("rolling-summary-{conversation_id}");
            let existing = self
                .storage
                .query_memories(
                    Some(MemoryScope::Conversation),
                    Some("conversation_summary"),
                    None,
                    false,
                    500,
                )?
                .into_iter()
                .find(|m| m.id == summary_id)
                .map(|m| m.content);
            (active_before, existing)
        } else {
            drop(active_before);
            let all_messages = self.storage.list_chat_messages(
                conversation_id,
                true,
                fetch_cap.saturating_add(1024),
            )?;
            let mut archived_turns = Vec::new();
            let mut active_turns = Vec::new();
            for msg in all_messages {
                if msg.archived {
                    archived_turns.push(msg);
                } else {
                    active_turns.push(msg);
                }
            }

            let rolling_summary = if archived_turns.is_empty() {
                None
            } else {
                let archived_refs: Vec<&ChatMessage> = archived_turns.iter().collect();
                let generated = synthesize_rolling_summary(&archived_refs, 1200);
                let summary_id = format!("rolling-summary-{conversation_id}");
                let now = now_iso();
                let mem = MemoryItem {
                    id: summary_id,
                    scope: MemoryScope::Conversation,
                    namespace: "conversation_summary".to_string(),
                    owner_agent_id: None,
                    conversation_id: Some(conversation_id.to_string()),
                    task_id: None,
                    key: format!("rolling_summary:{conversation_id}"),
                    content: generated.clone(),
                    summary: None,
                    importance: 8,
                    tags: vec!["rolling_summary".to_string()],
                    archived: false,
                    created_at: now.clone(),
                    updated_at: now,
                };
                self.storage.upsert_memory(&mem)?;
                Some(generated)
            };
            (active_turns, rolling_summary)
        };

        let mut system_sections = Vec::new();
        if !system_prompt.trim().is_empty() {
            system_sections.push(system_prompt.trim().to_string());
        }
        if let Some(ref summary) = rolling_summary {
            if !summary.trim().is_empty() {
                system_sections.push(format!("[Earlier Conversation Summary]\n{summary}"));
            }
        }
        if let Some(mem_blk) = memory_block {
            if !mem_blk.trim().is_empty() {
                system_sections.push(mem_blk.trim().to_string());
            }
        }

        let mut messages = Vec::new();
        let mut used_tokens = 0usize;

        if !system_sections.is_empty() && token_budget > 0 {
            let combined_sys = system_sections.join("\n\n");
            let mut sys_msg = InferenceMessage {
                role: "system".to_string(),
                content: combined_sys,
                tool_call_id: None,
                name: None,
                tool_calls: None,
                image_urls: Vec::new(),
            };
            let sys_tokens = estimate_message_tokens(&sys_msg);
            if sys_tokens > token_budget {
                // Overhead is ~6 tokens (`role: "system"` + framing). Cap content chars to fit budget.
                let overhead = 4 + estimate_tokens("system");
                let allowed_content_tokens = token_budget.saturating_sub(overhead);
                let max_content_chars = allowed_content_tokens.saturating_mul(4);
                sys_msg.content = truncate_chars(&sys_msg.content, max_content_chars);
            }
            used_tokens = estimate_message_tokens(&sys_msg);
            messages.push(sys_msg);
        }

        let mut kept_turns_rev = Vec::new();
        let mut truncated_messages = 0usize;

        for (idx, turn) in active_turns.iter().rev().enumerate() {
            let role_str = chat_role_str(turn.role);
            let sanitized_content = sanitize_inline_data_urls(&turn.content);
            let image_count = turn
                .attachments
                .iter()
                .filter(|att| image_attachment_url(att).is_some())
                .count();
            let t_tokens = 4
                + estimate_tokens(role_str)
                + estimate_text_tokens_without_data_urls(&sanitized_content)
                + image_count.saturating_mul(IMAGE_ATTACHMENT_TOKEN_ESTIMATE);
            if used_tokens + t_tokens <= token_budget {
                used_tokens += t_tokens;
                let image_urls = if image_count == 0 {
                    Vec::new()
                } else {
                    turn.attachments
                        .iter()
                        .filter_map(|att| image_attachment_url(att).map(str::to_string))
                        .collect()
                };
                kept_turns_rev.push(InferenceMessage {
                    role: role_str.to_string(),
                    content: sanitized_content,
                    tool_call_id: None,
                    name: turn.agent_id.clone(),
                    tool_calls: None,
                    image_urls,
                });
            } else {
                // Stop immediately so older smaller turns cannot leapfrog a newer truncated turn.
                truncated_messages = active_turns.len() - idx;
                break;
            }
        }

        kept_turns_rev.reverse();
        messages.extend(kept_turns_rev);

        Ok(ConversationContext {
            messages,
            archived_count,
            rolling_summary,
            estimated_tokens: used_tokens,
            truncated_messages,
        })
    }
}

fn score_memory(
    item: &MemoryItem,
    tokens: &[String],
    conversation_id: Option<&str>,
    task_id: Option<&str>,
    now: DateTime<Utc>,
) -> f64 {
    let importance_score = (item.importance.min(10) as f64) / 10.0;

    let recency_score = DateTime::parse_from_rfc3339(&item.updated_at)
        .or_else(|_| DateTime::parse_from_rfc3339(&item.created_at))
        .map(|dt| {
            let age_secs = now.signed_duration_since(dt.with_timezone(&Utc)).num_seconds().max(0);
            let age_hours = (age_secs as f64) / 3600.0;
            1.0 / (1.0 + age_hours / 24.0)
        })
        .unwrap_or(0.5);

    let mut affinity_bonus = 0.0;
    if let (Some(qt), Some(it)) = (task_id, item.task_id.as_deref()) {
        if qt == it {
            affinity_bonus += 0.05;
        }
    }
    if let (Some(qc), Some(ic)) = (conversation_id, item.conversation_id.as_deref()) {
        if qc == ic {
            affinity_bonus += 0.05;
        }
    }

    if tokens.is_empty() {
        return 0.60 * importance_score + 0.40 * recency_score + affinity_bonus;
    }

    let mut hits = 0.0;
    for tok in tokens {
        let in_key = contains_ascii_ci(&item.key, tok);
        let exact_tag = item.tags.iter().any(|t| t.eq_ignore_ascii_case(tok));
        let in_haystack = in_key
            || exact_tag
            || contains_ascii_ci(&item.content, tok)
            || contains_ascii_ci(item.summary.as_deref().unwrap_or(""), tok)
            || item.tags.iter().any(|t| contains_ascii_ci(t, tok));
        if in_haystack {
            hits += 1.0;
            if in_key || exact_tag {
                hits += 0.15;
            }
        }
    }
    let keyword_score = (hits / tokens.len() as f64).min(1.0);

    0.50 * keyword_score + 0.30 * importance_score + 0.20 * recency_score + affinity_bonus
}

#[cfg(test)]
mod tests {
    use super::*;
    use ps5_agent_core::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct MockMemoryStorage {
        memories: Mutex<Vec<MemoryItem>>,
        messages: Mutex<Vec<ChatMessage>>,
        last_query_limit: Mutex<Option<usize>>,
        chat_list_calls: Mutex<Vec<(bool, usize)>>,
    }

    impl StorageBackend for MockMemoryStorage {
        fn driver_name(&self) -> &'static str { "mock" }
        fn run_migrations(&self) -> Result<()> { Ok(()) }
        fn integrity_check(&self) -> Result<String> { Ok("ok".into()) }
        fn checkpoint(&self) -> Result<()> { Ok(()) }
        fn upsert_agent(&self, _: &AgentDefinition) -> Result<()> { Ok(()) }
        fn get_agent(&self, _: &str) -> Result<Option<AgentDefinition>> { Ok(None) }
        fn list_agents(&self) -> Result<Vec<AgentDefinition>> { Ok(vec![]) }
        fn delete_agent(&self, _: &str) -> Result<bool> { Ok(false) }
        fn create_conversation(&self, _: &Conversation) -> Result<()> { Ok(()) }
        fn get_conversation(&self, _: &str) -> Result<Option<Conversation>> { Ok(None) }
        fn list_conversations(&self, _: bool) -> Result<Vec<Conversation>> { Ok(vec![]) }
        fn delete_conversation(&self, _: &str) -> Result<bool> { Ok(false) }
        fn append_chat_message(&self, msg: &ChatMessage) -> Result<()> {
            self.messages.lock().unwrap().push(msg.clone());
            Ok(())
        }
        fn list_chat_messages(&self, conv_id: &str, include_archived: bool, limit: usize) -> Result<Vec<ChatMessage>> {
            self.chat_list_calls
                .lock()
                .unwrap()
                .push((include_archived, limit));
            Ok(self
                .messages
                .lock()
                .unwrap()
                .iter()
                .filter(|m| m.conversation_id == conv_id && (include_archived || !m.archived))
                .take(limit)
                .cloned()
                .collect())
        }
        fn search_chat_messages(&self, _: &str, _: usize) -> Result<Vec<ChatMessage>> { Ok(vec![]) }
        fn archive_old_chat_messages(&self, conv_id: &str, keep_recent: usize) -> Result<usize> {
            let mut msgs = self.messages.lock().unwrap();
            let active_indices: Vec<usize> = msgs
                .iter()
                .enumerate()
                .filter(|(_, m)| m.conversation_id == conv_id && !m.archived)
                .map(|(i, _)| i)
                .collect();
            let to_archive = active_indices.len().saturating_sub(keep_recent);
            for &idx in active_indices.iter().take(to_archive) {
                msgs[idx].archived = true;
            }
            Ok(to_archive)
        }
        fn upsert_task(&self, _: &TaskRecord) -> Result<()> { Ok(()) }
        fn get_task(&self, _: &str) -> Result<Option<TaskRecord>> { Ok(None) }
        fn list_tasks(&self, _: usize) -> Result<Vec<TaskRecord>> { Ok(vec![]) }
        fn task_counts_summary(&self) -> Result<TaskCountsSummary> { Ok(TaskCountsSummary::default()) }
        fn append_task_event(&self, _: &TaskEvent) -> Result<()> { Ok(()) }
        fn list_task_events(&self, _: &str) -> Result<Vec<TaskEvent>> { Ok(vec![]) }
        fn append_agent_message(&self, _: &AgentEnvelope) -> Result<()> { Ok(()) }
        fn update_agent_message_status(&self, _: &str, _: DeliveryStatus) -> Result<()> { Ok(()) }
        fn list_agent_messages(&self, _: Option<&str>, _: Option<&str>, _: usize) -> Result<Vec<AgentEnvelope>> { Ok(vec![]) }
        fn count_task_agent_messages(&self, _: &str) -> Result<u32> { Ok(0) }
        fn upsert_memory(&self, item: &MemoryItem) -> Result<()> {
            let mut guard = self.memories.lock().unwrap();
            if let Some(pos) = guard.iter().position(|m| m.id == item.id) {
                guard[pos] = item.clone();
            } else {
                guard.push(item.clone());
            }
            Ok(())
        }
        fn query_memories(
            &self,
            scope: Option<MemoryScope>,
            namespace: Option<&str>,
            owner_agent_id: Option<&str>,
            include_archived: bool,
            limit: usize,
        ) -> Result<Vec<MemoryItem>> {
            *self.last_query_limit.lock().unwrap() = Some(limit);
            Ok(self
                .memories
                .lock()
                .unwrap()
                .iter()
                .filter(|m| scope.map_or(true, |s| m.scope == s))
                .filter(|m| namespace.map_or(true, |n| m.namespace == n))
                .filter(|m| owner_agent_id.map_or(true, |o| m.owner_agent_id.as_deref() == Some(o)))
                .filter(|m| include_archived || !m.archived)
                .take(limit)
                .cloned()
                .collect())
        }
        fn delete_memory(&self, _: &str) -> Result<bool> { Ok(false) }
        fn delete_agent_memories(&self, _: &str) -> Result<usize> { Ok(0) }
        fn memory_stats_for_agent(&self, _: &str) -> Result<(u64, u64)> { Ok((0, 0)) }
        fn upsert_tool_spec(&self, _: &ToolSpec) -> Result<()> { Ok(()) }
        fn list_tool_specs(&self) -> Result<Vec<ToolSpec>> { Ok(vec![]) }
        fn record_tool_execution(&self, _: &ToolExecutionRecord) -> Result<()> { Ok(()) }
        fn list_tool_executions(&self, _: Option<&str>, _: usize) -> Result<Vec<ToolExecutionRecord>> { Ok(vec![]) }
        fn get_setting(&self, _: &str) -> Result<Option<String>> { Ok(None) }
        fn set_setting(&self, _: &str, _: &str) -> Result<()> { Ok(()) }
        fn list_settings(&self) -> Result<Vec<(String, String)>> { Ok(vec![]) }
        fn create_session(&self, _: &SessionRecord) -> Result<()> { Ok(()) }
        fn get_session_by_hash(&self, _: &str) -> Result<Option<SessionRecord>> { Ok(None) }
        fn revoke_session(&self, _: &str) -> Result<()> { Ok(()) }
        fn purge_expired_sessions(&self, _: &str) -> Result<usize> { Ok(0) }
        fn upsert_approval(&self, _: &ApprovalRecord) -> Result<()> { Ok(()) }
        fn get_approval(&self, _: &str) -> Result<Option<ApprovalRecord>> { Ok(None) }
        fn list_approvals(&self, _: bool, _: usize) -> Result<Vec<ApprovalRecord>> { Ok(vec![]) }
        fn append_audit_log(&self, _: &AuditLogEntry) -> Result<()> { Ok(()) }
        fn list_audit_logs(&self, _: usize) -> Result<Vec<AuditLogEntry>> { Ok(vec![]) }
    }

    fn make_item(id: &str, owner: &str) -> MemoryItem {
        MemoryItem {
            id: id.to_string(),
            scope: MemoryScope::Agent,
            namespace: "default".to_string(),
            owner_agent_id: Some(owner.to_string()),
            conversation_id: None,
            task_id: None,
            key: id.to_string(),
            content: "sample".to_string(),
            summary: None,
            importance: 5,
            tags: vec![],
            archived: false,
            created_at: now_iso(),
            updated_at: now_iso(),
        }
    }

    #[test]
    fn zero_max_memories_and_zero_archive_cap_are_unlimited() {
        let storage = Arc::new(MockMemoryStorage::default());
        let mgr = MemoryManager::new(
            storage.clone(),
            MemoryConfig {
                max_memories_per_agent: 0,
                archive_after_messages: 0,
                keep_recent_messages: 2,
                max_context_memories: 4,
                max_memory_context_chars: 1000,
            },
        );

        for i in 0..10 {
            mgr.store_memory(make_item(&format!("m-{i}"), "agent-a")).unwrap();
        }
        mgr.enforce_agent_memory_cap("agent-a", "m-extra").unwrap();
        let active = storage
            .query_memories(None, None, Some("agent-a"), false, 50)
            .unwrap();
        assert_eq!(active.len(), 10);

        for i in 0..6 {
            storage
                .append_chat_message(&ChatMessage {
                    id: format!("c-{i}"),
                    conversation_id: "conv-1".to_string(),
                    role: ChatRole::User,
                    agent_id: None,
                    content: format!("hello {i}"),
                    reasoning: None,
                    tool_calls_json: None,
                    task_id: None,
                    token_count: 5,
                    latency_ms: None,
                    archived: false,
                    created_at: now_iso(),
                    attachments: vec![],
                    timings: None,
                })
                .unwrap();
        }
        let ctx = mgr
            .build_conversation_context("conv-1", "sys", None, 1024)
            .unwrap();
        assert_eq!(ctx.archived_count, 0);
        assert_eq!(ctx.messages.len(), 7);
    }

    #[test]
    fn base64_image_attachments_do_not_exhaust_text_token_budget() {
        let storage = Arc::new(MockMemoryStorage::default());
        let mgr = MemoryManager::new(storage.clone(), MemoryConfig::default());
        let big_b64 = format!("data:image/png;base64,{}", "A".repeat(40_000));

        storage
            .append_chat_message(&ChatMessage {
                id: "img-msg".to_string(),
                conversation_id: "conv-img".to_string(),
                role: ChatRole::User,
                agent_id: None,
                content: "Describe this image".to_string(),
                reasoning: None,
                tool_calls_json: None,
                task_id: None,
                token_count: 10,
                latency_ms: None,
                archived: false,
                created_at: now_iso(),
                attachments: vec![ChatAttachment {
                    kind: "image".to_string(),
                    name: "shot.png".to_string(),
                    mime_type: "image/png".to_string(),
                    data_url: Some(big_b64.clone()),
                    workspace_path: None,
                    size_bytes: 30_000,
                }],
                timings: None,
            })
            .unwrap();

        let ctx = mgr
            .build_conversation_context("conv-img", "System.", None, 512)
            .unwrap();
        assert_eq!(ctx.truncated_messages, 0);
        assert_eq!(ctx.messages.len(), 2);
        assert_eq!(ctx.messages[1].image_urls, vec![big_b64]);
        assert!(ctx.estimated_tokens <= 512);
    }

    #[test]
    fn retrieve_context_scored_bounds_scan_limit() {
        let storage = Arc::new(MockMemoryStorage::default());
        let mgr = MemoryManager::new(storage.clone(), MemoryConfig::default());
        let perms = MemoryPermissions::default();

        mgr.retrieve_context_scored(&MemoryQuery {
            reader_agent_id: "a",
            permissions: &perms,
            query_text: "test",
            conversation_id: None,
            task_id: None,
            namespace: None,
            scopes: None,
            limit: Some(3),
        })
        .unwrap();
        assert_eq!(*storage.last_query_limit.lock().unwrap(), Some(32));

        mgr.retrieve_context_scored(&MemoryQuery {
            reader_agent_id: "a",
            permissions: &perms,
            query_text: "test",
            conversation_id: None,
            task_id: None,
            namespace: None,
            scopes: None,
            limit: Some(10_000),
        })
        .unwrap();
        assert_eq!(*storage.last_query_limit.lock().unwrap(), Some(500));
    }

    #[test]
    fn enforce_agent_memory_cap_evicts_low_importance_beyond_top_window() {
        let storage = Arc::new(MockMemoryStorage::default());
        let mgr = MemoryManager::new(
            storage.clone(),
            MemoryConfig {
                max_memories_per_agent: 3,
                ..MemoryConfig::default()
            },
        );

        // Seed 70 high-importance + 10 low-importance memories directly (> cap + 64).
        for i in 0..70 {
            let mut m = make_item(&format!("high-{i:02}"), "agent-a");
            m.importance = 9;
            storage.upsert_memory(&m).unwrap();
        }
        for i in 0..10 {
            let mut m = make_item(&format!("low-{i:02}"), "agent-a");
            m.importance = 1;
            storage.upsert_memory(&m).unwrap();
        }

        let mut incoming = make_item("incoming-mid", "agent-a");
        incoming.importance = 6;
        mgr.store_memory(incoming).unwrap();

        let active = storage
            .query_memories(None, None, Some("agent-a"), false, 200)
            .unwrap();
        assert_eq!(active.len(), 3);
        assert!(active.iter().any(|m| m.id == "incoming-mid"));
        assert!(!active.iter().any(|m| m.id.starts_with("low-")));
    }

    #[test]
    fn synthesize_rolling_summary_includes_initial_and_newest_turns() {
        let msgs: Vec<ChatMessage> = (1..=30)
            .map(|i| ChatMessage {
                id: format!("msg-{i:02}"),
                conversation_id: "conv-roll".to_string(),
                role: if i % 2 == 1 {
                    ChatRole::User
                } else {
                    ChatRole::Assistant
                },
                agent_id: None,
                content: format!(
                    "Turn {i:02}: verifying PS5 13.40 subsystem diagnostics and telemetry metrics."
                ),
                reasoning: None,
                tool_calls_json: None,
                task_id: None,
                token_count: 20,
                latency_ms: None,
                archived: true,
                created_at: now_iso(),
                attachments: Vec::new(),
                timings: None,
            })
            .collect();
        let refs: Vec<&ChatMessage> = msgs.iter().collect();

        let summary = synthesize_rolling_summary(&refs, 1200);
        assert!(summary.chars().count() <= 1200);
        assert!(summary.contains("Turn 01"));
        assert!(summary.contains("Turn 30"));
        assert!(summary.contains("earlier turns omitted"));
    }

    #[test]
    fn build_conversation_context_breaks_cleanly_and_strips_inline_data_urls() {
        let storage = Arc::new(MockMemoryStorage::default());
        let mgr = MemoryManager::new(
            storage.clone(),
            MemoryConfig {
                archive_after_messages: 0,
                ..MemoryConfig::default()
            },
        );

        let big_msg = "X".repeat(800);
        let msgs = [
            ("m1", "oldest tiny"),
            ("m2", big_msg.as_str()),
            ("m3", "newest tiny"),
        ];
        for (id, content) in msgs {
            storage
                .append_chat_message(&ChatMessage {
                    id: id.to_string(),
                    conversation_id: "conv-gap".to_string(),
                    role: ChatRole::User,
                    agent_id: None,
                    content: content.to_string(),
                    reasoning: None,
                    tool_calls_json: None,
                    task_id: None,
                    token_count: 10,
                    latency_ms: None,
                    archived: false,
                    created_at: now_iso(),
                    attachments: Vec::new(),
                    timings: None,
                })
                .unwrap();
        }

        // Fits sys + m3, overflows on m2: must break without keeping m1.
        let gap_ctx = mgr
            .build_conversation_context("conv-gap", "Sys.", None, 40)
            .unwrap();
        assert_eq!(gap_ctx.truncated_messages, 2);
        assert_eq!(gap_ctx.messages.len(), 2);
        assert_eq!(gap_ctx.messages[1].content, "newest tiny");

        // Inline data:...;base64,... in historical turns is replaced with compact summary.
        let inline_img = format!(
            "Look at ![shot](data:image/png;base64,{}) and data:text/plain;base64,{}",
            "A".repeat(20_000),
            "B".repeat(8_000)
        );
        storage
            .append_chat_message(&ChatMessage {
                id: "m-inline".to_string(),
                conversation_id: "conv-inline".to_string(),
                role: ChatRole::User,
                agent_id: None,
                content: inline_img,
                reasoning: None,
                tool_calls_json: None,
                task_id: None,
                token_count: 20,
                latency_ms: None,
                archived: false,
                created_at: now_iso(),
                attachments: Vec::new(),
                timings: None,
            })
            .unwrap();

        let inline_ctx = mgr
            .build_conversation_context("conv-inline", "Sys.", None, 256)
            .unwrap();
        assert_eq!(inline_ctx.truncated_messages, 0);
        assert_eq!(inline_ctx.messages.len(), 2);
        assert!(inline_ctx.estimated_tokens <= 256);
        let msg_text = &inline_ctx.messages[1].content;
        assert!(!msg_text.contains("AAAA"));
        assert!(!msg_text.contains("BBBB"));
        assert!(msg_text.contains("[inline image/png data omitted (20000 chars)]"));
        assert!(msg_text.contains("[inline text/plain data omitted (8000 chars)]"));
    }

    #[test]
    fn build_conversation_context_skips_second_chat_list_when_archived_count_is_zero() {
        let storage = Arc::new(MockMemoryStorage::default());
        let mgr = MemoryManager::new(
            storage.clone(),
            MemoryConfig {
                archive_after_messages: 4,
                keep_recent_messages: 2,
                ..MemoryConfig::default()
            },
        );

        for i in 1..=3 {
            storage
                .append_chat_message(&ChatMessage {
                    id: format!("m-{i}"),
                    conversation_id: "conv-skip".to_string(),
                    role: ChatRole::User,
                    agent_id: None,
                    content: format!("Turn {i} uncompacted"),
                    reasoning: None,
                    tool_calls_json: None,
                    task_id: None,
                    token_count: 10,
                    latency_ms: None,
                    archived: false,
                    created_at: now_iso(),
                    attachments: Vec::new(),
                    timings: None,
                })
                .unwrap();
        }

        // Below threshold: archived_count == 0, only include_archived=false query runs.
        let ctx1 = mgr
            .build_conversation_context("conv-skip", "Sys.", None, 1024)
            .unwrap();
        assert_eq!(ctx1.archived_count, 0);
        assert!(ctx1.rolling_summary.is_none());
        assert_eq!(ctx1.messages.len(), 4);
        assert_eq!(*storage.chat_list_calls.lock().unwrap(), vec![(false, 1028)]);

        // Cross threshold (> 4): triggers archive + include_archived=true reload.
        for i in 4..=5 {
            storage
                .append_chat_message(&ChatMessage {
                    id: format!("m-{i}"),
                    conversation_id: "conv-skip".to_string(),
                    role: ChatRole::Assistant,
                    agent_id: Some("commander".to_string()),
                    content: format!("Turn {i} overflow"),
                    reasoning: None,
                    tool_calls_json: None,
                    task_id: None,
                    token_count: 10,
                    latency_ms: None,
                    archived: false,
                    created_at: now_iso(),
                    attachments: Vec::new(),
                    timings: None,
                })
                .unwrap();
        }
        storage.chat_list_calls.lock().unwrap().clear();

        let ctx2 = mgr
            .build_conversation_context("conv-skip", "Sys.", None, 1024)
            .unwrap();
        assert_eq!(ctx2.archived_count, 3);
        assert!(ctx2.rolling_summary.is_some());
        assert_eq!(ctx2.messages.len(), 3);
        assert_eq!(
            *storage.chat_list_calls.lock().unwrap(),
            vec![(false, 1028), (true, 2052)]
        );

        // Next turn below threshold again: archived_count == 0, rolling_summary preserved,
        // and the second include_archived=true list call is skipped.
        storage
            .append_chat_message(&ChatMessage {
                id: "m-6".to_string(),
                conversation_id: "conv-skip".to_string(),
                role: ChatRole::User,
                agent_id: None,
                content: "Turn 6 follow-up".to_string(),
                reasoning: None,
                tool_calls_json: None,
                task_id: None,
                token_count: 10,
                latency_ms: None,
                archived: false,
                created_at: now_iso(),
                attachments: Vec::new(),
                timings: None,
            })
            .unwrap();
        storage.chat_list_calls.lock().unwrap().clear();

        let ctx3 = mgr
            .build_conversation_context("conv-skip", "Sys.", None, 1024)
            .unwrap();
        assert_eq!(ctx3.archived_count, 0);
        assert_eq!(ctx3.rolling_summary, ctx2.rolling_summary);
        assert_eq!(ctx3.messages.len(), 4);
        assert_eq!(*storage.chat_list_calls.lock().unwrap(), vec![(false, 1028)]);
    }

    #[test]
    fn score_memory_matches_legacy_lowercase_semantics() {
        fn baseline_score(
            item: &MemoryItem,
            tokens: &[String],
            conversation_id: Option<&str>,
            task_id: Option<&str>,
            now: DateTime<Utc>,
        ) -> f64 {
            let importance_score = (item.importance.min(10) as f64) / 10.0;
            let recency_score = DateTime::parse_from_rfc3339(&item.updated_at)
                .or_else(|_| DateTime::parse_from_rfc3339(&item.created_at))
                .map(|dt| {
                    let age_secs = now
                        .signed_duration_since(dt.with_timezone(&Utc))
                        .num_seconds()
                        .max(0);
                    let age_hours = (age_secs as f64) / 3600.0;
                    1.0 / (1.0 + age_hours / 24.0)
                })
                .unwrap_or(0.5);
            let mut affinity_bonus = 0.0;
            if let (Some(qt), Some(it)) = (task_id, item.task_id.as_deref()) {
                if qt == it {
                    affinity_bonus += 0.05;
                }
            }
            if let (Some(qc), Some(ic)) = (conversation_id, item.conversation_id.as_deref()) {
                if qc == ic {
                    affinity_bonus += 0.05;
                }
            }
            if tokens.is_empty() {
                return 0.60 * importance_score + 0.40 * recency_score + affinity_bonus;
            }
            let haystack = format!(
                "{} {} {} {}",
                item.key,
                item.content,
                item.summary.as_deref().unwrap_or(""),
                item.tags.join(" ")
            )
            .to_ascii_lowercase();
            let key_lower = item.key.to_ascii_lowercase();
            let tags_lower: Vec<String> = item.tags.iter().map(|t| t.to_ascii_lowercase()).collect();
            let mut hits = 0.0;
            for tok in tokens {
                if haystack.contains(tok) {
                    hits += 1.0;
                    if key_lower.contains(tok) || tags_lower.iter().any(|t| t == tok) {
                        hits += 0.15;
                    }
                }
            }
            let keyword_score = (hits / tokens.len() as f64).min(1.0);
            0.50 * keyword_score + 0.30 * importance_score + 0.20 * recency_score + affinity_bonus
        }

        let now = now_utc();
        let mut item_key = make_item("SQLite_WAL_Mode", "agent-a");
        item_key.content = "Fallback to TRUNCATE in Łódź".to_string();
        item_key.summary = Some("PS5 /data Summary Żółć".to_string());
        item_key.tags = vec!["SQLite".to_string(), "wal_tuning".to_string()];
        item_key.conversation_id = Some("conv-1".to_string());
        item_key.task_id = Some("task-1".to_string());

        let mut item_partial_tag = make_item("other_key", "agent-a");
        item_partial_tag.content = "Check TRUNCATE behavior".to_string();
        item_partial_tag.tags = vec!["wal_tuning".to_string()];

        for q in [
            "",
            "sqlite wal truncate",
            "Łódź Żółć",
            "WAL ps5 summary",
            "wal_tuning",
        ] {
            let tokens = tokenize_query(q);
            for item in [&item_key, &item_partial_tag] {
                let actual = score_memory(item, &tokens, Some("conv-1"), Some("task-1"), now);
                let expected = baseline_score(item, &tokens, Some("conv-1"), Some("task-1"), now);
                assert!(
                    (actual - expected).abs() < 1e-12,
                    "score mismatch for query={q:?}, item={}: actual={actual}, expected={expected}",
                    item.id
                );
            }
        }
    }
}
