use std::collections::HashSet;

use ps5_agent_core::{InferenceMessage, ToolCallRequest};
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct ParsedModelOutput {
    pub content: String,
    pub reasoning: Option<String>,
    pub tool_calls: Vec<ToolCallRequest>,
}

/// Qwen compatibility: merges all `system` and `developer` messages into a single
/// leading `messages[0]` with `role = "system"`, preserving relative order of all
/// other messages.
pub fn fold_system_messages(messages: &[InferenceMessage]) -> Vec<InferenceMessage> {
    let mut system_parts: Vec<&str> = Vec::new();
    let mut system_image_urls: Vec<String> = Vec::new();
    let mut rest: Vec<InferenceMessage> = Vec::with_capacity(messages.len());

    for msg in messages {
        let role_lower = msg.role.trim().to_ascii_lowercase();
        if role_lower == "system" || role_lower == "developer" {
            let trimmed = msg.content.trim();
            if !trimmed.is_empty() {
                system_parts.push(trimmed);
            }
            if !msg.image_urls.is_empty() {
                system_image_urls.extend(msg.image_urls.iter().cloned());
            }
        } else {
            rest.push(msg.clone());
        }
    }

    if system_parts.is_empty() && system_image_urls.is_empty() {
        return rest;
    }

    let mut folded = Vec::with_capacity(rest.len() + 1);
    folded.push(InferenceMessage {
        role: "system".to_string(),
        content: system_parts.join("\n\n"),
        tool_call_id: None,
        name: None,
        tool_calls: None,
        image_urls: system_image_urls,
    });
    folded.extend(rest);
    folded
}

fn is_plain_assistant(msg: &InferenceMessage) -> bool {
    msg.role.trim().eq_ignore_ascii_case("assistant")
        && msg.tool_calls.as_ref().is_none_or(|t| t.is_empty())
}

fn has_tool_calls(msg: &InferenceMessage) -> bool {
    msg.tool_calls.as_ref().is_some_and(|t| !t.is_empty())
}

fn is_empty_plain_assistant(msg: &InferenceMessage) -> bool {
    is_plain_assistant(msg) && msg.content.trim().is_empty() && msg.image_urls.is_empty()
}

fn append_message_content(dst: &mut InferenceMessage, src: InferenceMessage) {
    if dst.content.trim().is_empty() {
        dst.content = src.content;
    } else if !src.content.trim().is_empty() {
        dst.content.push_str("\n\n");
        dst.content.push_str(&src.content);
    }
    if !src.image_urls.is_empty() {
        dst.image_urls.extend(src.image_urls);
    }
    if dst.role.trim().eq_ignore_ascii_case("tool") {
        if dst.name.is_none() {
            dst.name = src.name;
        }
    } else {
        dst.name = None;
    }
}

/// Normalizes conversation history before outbound serialization to PS5LM / llama.cpp:
/// 1. Folds all `system`/`developer` messages into at most one leading `system` message.
/// 2. Drops completely empty plain `assistant` messages (no text, images, or `tool_calls`).
/// 3. Drops leading `assistant` (with or without `tool_calls`) and orphan `tool` messages before
///    the first `user` anchor.
/// 4. Coalesces consecutive `user` messages and consecutive plain `assistant` messages (joining
///    non-empty text with `"\n\n"` and preserving `image_urls`), while never merging across
///    `assistant` messages that carry `tool_calls` or breaking `assistant(tool_calls) -> tool`.
/// 5. When `ensure_trailing_user` is true, appends a minimal trailing `user` turn if the
///    non-empty normalized history does not already end with a `user` message.
pub fn normalize_conversation_messages(
    messages: &[InferenceMessage],
    ensure_trailing_user: bool,
) -> Vec<InferenceMessage> {
    let folded = fold_system_messages(messages);
    let mut folded_iter = folded.into_iter().peekable();

    let system_msg = folded_iter.next_if(|m| m.role.trim().eq_ignore_ascii_case("system"));

    let mut non_system: Vec<InferenceMessage> = Vec::new();
    let mut seen_non_system_anchor = false;

    for mut msg in folded_iter {
        if is_empty_plain_assistant(&msg) {
            continue;
        }

        let role_lower = msg.role.trim().to_ascii_lowercase();
        if msg.role != role_lower {
            msg.role = role_lower.clone();
        }
        if role_lower != "tool" {
            msg.name = None;
        }

        if !seen_non_system_anchor {
            if role_lower != "user" {
                continue;
            }
            seen_non_system_anchor = true;
        }

        if let Some(prev) = non_system.last_mut() {
            if prev.role == "user"
                && role_lower == "user"
                && !has_tool_calls(prev)
                && !has_tool_calls(&msg)
            {
                append_message_content(prev, msg);
                continue;
            }
            if is_plain_assistant(prev) && is_plain_assistant(&msg) {
                append_message_content(prev, msg);
                continue;
            }
        }

        non_system.push(msg);
    }

    let mut out = Vec::with_capacity(
        non_system.len() + usize::from(system_msg.is_some()) + usize::from(ensure_trailing_user),
    );
    if let Some(sys) = system_msg {
        out.push(sys);
    }
    out.extend(non_system);

    if ensure_trailing_user
        && !out.is_empty()
        && out
            .last()
            .is_some_and(|m| !m.role.eq_ignore_ascii_case("user"))
    {
        out.push(InferenceMessage {
            role: "user".to_string(),
            content: " ".to_string(),
            tool_call_id: None,
            name: None,
            tool_calls: None,
            image_urls: Vec::new(),
        });
    }

    out
}

/// Extracts `<think>...</think>` blocks and applies 3-tier tool-call extraction:
/// 1. Standard API `tool_calls` array
/// 2. Qwen `<tool_call>{"name": ..., "arguments": ...}</tool_call>` XML tags
/// 3. Safe structured JSON fallback `{"thought": "...", "tool_calls": [...], "final_answer": "..."}`
pub fn parse_model_output(
    raw_content: &str,
    api_reasoning: Option<String>,
    api_tool_calls: Vec<ToolCallRequest>,
) -> ParsedModelOutput {
    let (without_think, mut think_parts) = extract_think_blocks(raw_content);
    let mut reasoning_parts: Vec<String> = Vec::new();
    if let Some(r) = api_reasoning {
        let trimmed = r.trim();
        if !trimmed.is_empty() {
            reasoning_parts.push(trimmed.to_string());
        }
    }
    reasoning_parts.append(&mut think_parts);

    // Tier 1: structured API tool calls
    if !api_tool_calls.is_empty() {
        let cleaned = truncate_verbatim_prose_loops(&without_think);
        return ParsedModelOutput {
            content: cleaned.trim().to_string(),
            reasoning: join_non_empty(reasoning_parts),
            tool_calls: api_tool_calls,
        };
    }

    // Tier 2: Qwen <tool_call>...</tool_call> XML blocks
    let (without_xml_tools, xml_calls) = extract_qwen_tool_calls(&without_think);
    if !xml_calls.is_empty() {
        let cleaned = truncate_verbatim_prose_loops(&without_xml_tools);
        return ParsedModelOutput {
            content: cleaned.trim().to_string(),
            reasoning: join_non_empty(reasoning_parts),
            tool_calls: xml_calls,
        };
    }

    // Tier 3: Safe structured JSON fallback
    if let Some((thought, json_calls, final_answer)) =
        try_parse_structured_json_fallback(&without_xml_tools)
    {
        if let Some(t) = thought {
            if !t.is_empty() {
                reasoning_parts.push(t);
            }
        }
        let cleaned = truncate_verbatim_prose_loops(&final_answer);
        return ParsedModelOutput {
            content: cleaned.trim().to_string(),
            reasoning: join_non_empty(reasoning_parts),
            tool_calls: json_calls,
        };
    }

    let cleaned = truncate_verbatim_prose_loops(&without_xml_tools);
    ParsedModelOutput {
        content: cleaned.trim().to_string(),
        reasoning: join_non_empty(reasoning_parts),
        tool_calls: Vec::new(),
    }
}

fn join_non_empty(parts: Vec<String>) -> Option<String> {
    let filtered: Vec<String> = parts
        .into_iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if filtered.is_empty() {
        None
    } else {
        Some(filtered.join("\n"))
    }
}

const THINK_CLOSE_TAGS: [&str; 2] = ["</think>", "</seed:think>"];
const THINK_OPEN_TAGS: [&str; 2] = ["<think>", "<seed:think>"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ThinkVariant {
    Standard,
    Seed,
}

impl ThinkVariant {
    fn open_tag(self) -> &'static str {
        match self {
            Self::Standard => "<think>",
            Self::Seed => "<seed:think>",
        }
    }

    fn close_tag(self) -> &'static str {
        match self {
            Self::Standard => "</think>",
            Self::Seed => "</seed:think>",
        }
    }

    fn close_tags(self) -> &'static [&'static str] {
        match self {
            Self::Standard => &["</think>"],
            Self::Seed => &["</seed:think>"],
        }
    }
}

fn find_ci(haystack: &str, needle: &str, from: usize) -> Option<usize> {
    let sub = haystack.get(from..)?;
    if needle.is_empty() {
        return Some(from);
    }
    let hb = sub.as_bytes();
    let nb = needle.as_bytes();
    if nb.len() > hb.len() {
        return None;
    }
    for i in 0..=hb.len() - nb.len() {
        if hb[i..i + nb.len()].eq_ignore_ascii_case(nb) && haystack.is_char_boundary(from + i) {
            return Some(from + i);
        }
    }
    None
}

fn find_first_tag_variant_ci(
    haystack: &str,
    from: usize,
    is_close: bool,
) -> Option<(usize, usize, ThinkVariant)> {
    [ThinkVariant::Standard, ThinkVariant::Seed]
        .into_iter()
        .filter_map(|variant| {
            let tag = if is_close {
                variant.close_tag()
            } else {
                variant.open_tag()
            };
            find_ci(haystack, tag, from).map(|idx| (idx, tag.len(), variant))
        })
        .min_by_key(|&(idx, _, _)| idx)
}

pub fn extract_think_blocks(input: &str) -> (String, Vec<String>) {
    let mut cleaned = String::with_capacity(input.len());
    let mut thoughts = Vec::new();
    let mut cursor = 0usize;

    while cursor < input.len() {
        let open_opt = find_first_tag_variant_ci(input, cursor, false);
        let close_opt = find_first_tag_variant_ci(input, cursor, true);

        if let Some((end, close_len, _)) = close_opt {
            if open_opt.is_none_or(|(open, _, _)| end < open) {
                let slice = &input[cursor..end];
                if cursor == 0 && cleaned.is_empty() {
                    let block = slice.trim();
                    if !block.is_empty() {
                        thoughts.push(block.to_string());
                    }
                } else {
                    cleaned.push_str(slice);
                }
                cursor = end + close_len;
                continue;
            }
        }

        if let Some((start, open_len, variant)) = open_opt {
            cleaned.push_str(&input[cursor..start]);
            let body_start = start + open_len;
            let close_tag = variant.close_tag();
            if let Some(end) = find_ci(input, close_tag, body_start) {
                let block = input[body_start..end].trim();
                if !block.is_empty() {
                    thoughts.push(block.to_string());
                }
                cursor = end + close_tag.len();
            } else {
                // Unclosed <think>/<seed:think> (e.g. max_tokens truncation): align with
                // ThinkStreamSplitter by extracting the truncated reasoning body into thoughts.
                let block = input[body_start..].trim();
                if !block.is_empty() {
                    thoughts.push(block.to_string());
                }
                break;
            }
        } else {
            cleaned.push_str(&input[cursor..]);
            break;
        }
    }

    (cleaned, thoughts)
}

fn strip_code_fence(s: &str) -> &str {
    let trimmed = s.trim();
    if let Some(rest) = trimmed.strip_prefix("```") {
        let after_lang = if let Some(nl) = rest.find('\n') {
            &rest[nl + 1..]
        } else {
            rest
        };
        if let Some(inner) = after_lang.strip_suffix("```") {
            return inner.trim();
        }
    }
    trimmed
}

fn parse_tool_call_value(v: &Value) -> Option<ToolCallRequest> {
    let obj = v.as_object()?;
    let name = obj
        .get("name")
        .and_then(|n| n.as_str())
        .map(str::trim)
        .filter(|n| !n.is_empty())?
        .to_string();

    let id = obj
        .get("id")
        .and_then(|i| i.as_str())
        .map(str::trim)
        .filter(|i| !i.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| format!("call_{}", uuid::Uuid::new_v4()));

    let arguments = match obj.get("arguments") {
        None | Some(Value::Null) => Value::Object(serde_json::Map::new()),
        Some(Value::String(s)) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                Value::Object(serde_json::Map::new())
            } else {
                serde_json::from_str(trimmed).unwrap_or_else(|_| Value::String(s.clone()))
            }
        }
        Some(other) => other.clone(),
    };

    Some(ToolCallRequest {
        id,
        name,
        arguments,
    })
}

fn extract_qwen_tool_calls(input: &str) -> (String, Vec<ToolCallRequest>) {
    let mut cleaned = String::with_capacity(input.len());
    let mut calls = Vec::new();
    let mut cursor = 0usize;

    while let Some(start) = find_ci(input, "<tool_call>", cursor) {
        let body_start = start + "<tool_call>".len();
        if let Some(end) = find_ci(input, "</tool_call>", body_start) {
            let inner = strip_code_fence(&input[body_start..end]);
            if let Ok(val) = serde_json::from_str::<Value>(inner) {
                if let Some(tc) = parse_tool_call_value(&val) {
                    cleaned.push_str(&input[cursor..start]);
                    calls.push(tc);
                    cursor = end + "</tool_call>".len();
                    continue;
                }
            }
            // Malformed tool_call payload: preserve literal slice and advance
            cleaned.push_str(&input[cursor..end + "</tool_call>".len()]);
            cursor = end + "</tool_call>".len();
        } else {
            cleaned.push_str(&input[cursor..]);
            cursor = input.len();
            break;
        }
    }

    if cursor < input.len() {
        cleaned.push_str(&input[cursor..]);
    }

    (cleaned, calls)
}

fn try_parse_structured_json_fallback(
    input: &str,
) -> Option<(Option<String>, Vec<ToolCallRequest>, String)> {
    let candidate = strip_code_fence(input);
    if !candidate.starts_with('{') || !candidate.ends_with('}') {
        return None;
    }

    let val: Value = serde_json::from_str(candidate).ok()?;
    let obj = val.as_object()?;

    let has_tool_calls_array = obj.get("tool_calls").and_then(|v| v.as_array()).is_some();
    let has_final_answer = obj.contains_key("final_answer");

    if !has_tool_calls_array && !has_final_answer {
        return None;
    }

    let thought = obj
        .get("thought")
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    let mut calls = Vec::new();
    if let Some(arr) = obj.get("tool_calls").and_then(|v| v.as_array()) {
        for item in arr {
            if let Some(tc) = parse_tool_call_value(item) {
                calls.push(tc);
            }
        }
    }

    let final_answer = match obj.get("final_answer") {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Null) | None => String::new(),
        Some(other) => other.to_string(),
    };

    Some((thought, calls, final_answer))
}

/// Incremental SSE tag splitter that separates `<think>...</think>` (and
/// `<seed:think>...</seed:think>` aliases) into reasoning deltas vs text deltas
/// even when tags span multiple SSE chunks.
#[derive(Debug, Default)]
pub struct ThinkStreamSplitter {
    in_think: bool,
    active_variant: Option<ThinkVariant>,
    emitted_any_text: bool,
    closed_any_think: bool,
    pending: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamPiece {
    Text(String),
    Reasoning(String),
}

impl ThinkStreamSplitter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_initial_in_think(initial_in_think: bool) -> Self {
        Self {
            in_think: initial_in_think,
            active_variant: None,
            emitted_any_text: false,
            closed_any_think: false,
            pending: String::new(),
        }
    }

    pub fn push(&mut self, delta: &str) -> Vec<StreamPiece> {
        self.pending.push_str(delta);
        let mut out = Vec::new();

        loop {
            if self.in_think {
                let close_match = match self.active_variant {
                    Some(variant) => {
                        let tag = variant.close_tag();
                        find_ci(&self.pending, tag, 0).map(|idx| (idx, tag.len()))
                    }
                    None => find_first_tag_variant_ci(&self.pending, 0, true)
                        .map(|(idx, len, _)| (idx, len)),
                };
                if let Some((idx, close_len)) = close_match {
                    if idx > 0 {
                        let prefix = self.pending[..idx].to_string();
                        out.push(StreamPiece::Reasoning(prefix));
                    }
                    self.pending.drain(..idx + close_len);
                    self.in_think = false;
                    self.active_variant = None;
                    self.closed_any_think = true;
                    continue;
                }

                let close_tags = self
                    .active_variant
                    .map_or(&THINK_CLOSE_TAGS[..], ThinkVariant::close_tags);
                let hold_len = max_partial_tag_suffix_len(&self.pending, close_tags);
                let emit_len = self.pending.len() - hold_len;
                if emit_len > 0 {
                    let emit = self.pending[..emit_len].to_string();
                    self.pending.drain(..emit_len);
                    out.push(StreamPiece::Reasoning(emit));
                }
                break;
            }

            let open_opt = find_first_tag_variant_ci(&self.pending, 0, false);
            let close_opt = find_first_tag_variant_ci(&self.pending, 0, true);

            if let Some((end, close_len, _)) = close_opt {
                if open_opt.is_none_or(|(open, _, _)| end < open) {
                    if end > 0 {
                        let prefix = self.pending[..end].to_string();
                        if !self.emitted_any_text && !self.closed_any_think {
                            let clean_prefix = prefix.trim_start();
                            if !clean_prefix.is_empty() {
                                out.push(StreamPiece::Reasoning(clean_prefix.to_string()));
                            }
                        } else {
                            if !prefix.trim().is_empty() {
                                self.emitted_any_text = true;
                            }
                            out.push(StreamPiece::Text(prefix));
                        }
                    }
                    self.pending.drain(..end + close_len);
                    self.closed_any_think = true;
                    continue;
                }
            }

            if let Some((idx, open_len, variant)) = open_opt {
                if idx > 0 {
                    let prefix = self.pending[..idx].to_string();
                    if !self.emitted_any_text
                        && !self.closed_any_think
                        && prefix.trim().is_empty()
                    {
                        // Skip pure leading whitespace before an opening think tag.
                    } else {
                        if !prefix.trim().is_empty() {
                            self.emitted_any_text = true;
                        }
                        out.push(StreamPiece::Text(prefix));
                    }
                }
                self.pending.drain(..idx + open_len);
                self.in_think = true;
                self.active_variant = Some(variant);
                continue;
            }

            // Keep any trailing suffix that might be a split `<think>`, `</think>`,
            // `<seed:think>`, or `</seed:think>` tag across chunk boundaries, and hold
            // initial whitespace or draft text ending in a partial closing tag until a
            // closing tag resolves whether it is orphan reasoning.
            let open_hold = max_partial_tag_suffix_len(&self.pending, &THINK_OPEN_TAGS);
            let close_hold = max_partial_tag_suffix_len(&self.pending, &THINK_CLOSE_TAGS);
            if !self.emitted_any_text
                && !self.closed_any_think
                && (self.pending.trim().is_empty() || close_hold > 0)
            {
                break;
            }
            let hold_len = open_hold.max(close_hold);
            let emit_len = self.pending.len() - hold_len;
            if emit_len > 0 {
                let emit = self.pending[..emit_len].to_string();
                self.pending.drain(..emit_len);
                if !emit.trim().is_empty() {
                    self.emitted_any_text = true;
                }
                out.push(StreamPiece::Text(emit));
            }
            break;
        }

        out
    }

    pub fn finish(&mut self) -> Vec<StreamPiece> {
        if self.pending.is_empty() {
            return Vec::new();
        }
        let rem = std::mem::take(&mut self.pending);
        vec![if self.in_think {
            StreamPiece::Reasoning(rem)
        } else {
            if !rem.trim().is_empty() {
                self.emitted_any_text = true;
            }
            StreamPiece::Text(rem)
        }]
    }
}

fn partial_tag_suffix_len(s: &str, tag: &str) -> usize {
    let max_check = tag.len().saturating_sub(1).min(s.len());
    for k in (1..=max_check).rev() {
        let start = s.len() - k;
        if s.is_char_boundary(start) && s[start..].eq_ignore_ascii_case(&tag[..k]) {
            return k;
        }
    }
    0
}

fn max_partial_tag_suffix_len(s: &str, tags: &[&str]) -> usize {
    tags.iter()
        .map(|tag| partial_tag_suffix_len(s, tag))
        .max()
        .unwrap_or(0)
}

const STREAM_GUARD_TAG_OPENS: [&str; 3] = ["<seed:think>", "<think>", "<tool_call>"];
const MAX_REPETITION_WINDOW_BYTES: usize = 4096;
const PROSE_BOUNDARY_MARKER: char = '\x1e';

const KNOWN_AGENT_ROLES: [&str; 12] = [
    "commander",
    "researcher",
    "programmer",
    "analyst",
    "executor",
    "assistant",
    "coordinator",
    "reviewer",
    "verifier",
    "planner",
    "architect",
    "operator",
];

const KNOWN_LOOP_FILLERS: [&str; 12] = [
    "doing it again:",
    "hearing you:",
    "running it again:",
    "trying again:",
    "here we go again:",
    "here it is again:",
    "repeating:",
    "again:",
    "on it:",
    "copy that:",
    "acknowledged:",
    "got it:",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GuardTagKind {
    Think(ThinkVariant),
    ToolCall,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputSegment<'a> {
    Protected(&'a str),
    Prose(&'a str),
}

fn parse_fence_open(line: &str) -> Option<(u8, usize)> {
    let line_no_eol = line.trim_end_matches(['\r', '\n']);
    let trimmed_start = line_no_eol.trim_start_matches([' ', '\t']);
    let first = trimmed_start.as_bytes().first().copied()?;
    if first != b'`' && first != b'~' {
        return None;
    }
    let len = trimmed_start.bytes().take_while(|&b| b == first).count();
    if len < 3 {
        return None;
    }
    let rest = &trimmed_start[len..];
    if first == b'`' && rest.as_bytes().contains(&b'`') {
        return None;
    }
    if first == b'~' && rest.trim_end().ends_with("~~~") {
        return None;
    }
    Some((first, len))
}

fn is_fence_close(line: &str, fence_char: u8, open_len: usize) -> bool {
    let trimmed = line.trim();
    let len = trimmed.bytes().take_while(|&b| b == fence_char).count();
    len >= open_len && len == trimmed.len()
}

fn could_be_fence_open_prefix(line_no_eol: &str) -> bool {
    let ts = line_no_eol.trim_start_matches([' ', '\t']);
    if ts.is_empty() {
        return true;
    }
    if ts.starts_with("```") {
        return !ts[3..].as_bytes().contains(&b'`');
    }
    if ts.starts_with("~~~") {
        let all_tildes = ts.bytes().take_while(|&b| b == b'~').count() == ts.len();
        return all_tildes || !ts.trim_end().ends_with("~~~");
    }
    let first = ts.as_bytes()[0];
    (first == b'`' || first == b'~') && ts.bytes().all(|b| b == first)
}

fn is_backtick_wrapped_tag(s: &str, tag_start: usize, tag_len: usize) -> bool {
    tag_start > 0
        && s.as_bytes()[tag_start - 1] == b'`'
        && s.get(tag_start + tag_len..)
            .is_some_and(|after| after.starts_with('`'))
}

fn find_earliest_protected_open_tag(
    s: &str,
    from: usize,
    limit: usize,
) -> Option<(usize, usize, GuardTagKind, &'static str)> {
    let slice = &s[..limit];
    let candidates: [(&str, GuardTagKind, &str); 3] = [
        ("<seed:think>", GuardTagKind::Think(ThinkVariant::Seed), "</seed:think>"),
        ("<think>", GuardTagKind::Think(ThinkVariant::Standard), "</think>"),
        ("<tool_call>", GuardTagKind::ToolCall, "</tool_call>"),
    ];
    let mut best: Option<(usize, usize, GuardTagKind, &'static str)> = None;
    for (open, kind, close) in candidates {
        let mut search = from;
        while let Some(pos) = find_ci(slice, open, search) {
            if is_backtick_wrapped_tag(slice, pos, open.len()) {
                search = pos + 1;
                continue;
            }
            if best.is_none_or(|(b_pos, _, _, _)| pos < b_pos) {
                best = Some((pos, open.len(), kind, close));
            }
            break;
        }
    }
    best
}

fn flush_prose_segment<'a>(
    input: &'a str,
    end: usize,
    ps: &mut Option<usize>,
    out: &mut Vec<OutputSegment<'a>>,
) {
    if let Some(start) = ps.take() {
        if start < end {
            out.push(OutputSegment::Prose(&input[start..end]));
        }
    }
}

fn split_protected_segments(input: &str) -> Vec<OutputSegment<'_>> {
    let mut segments = Vec::new();
    let mut cursor = 0usize;
    let mut prose_start: Option<usize> = None;

    while cursor < input.len() {
        let at_line_start = cursor == 0 || input.as_bytes()[cursor - 1] == b'\n';
        let line_end = input[cursor..]
            .find('\n')
            .map_or(input.len(), |i| cursor + i + 1);
        let line = &input[cursor..line_end];

        if at_line_start {
            if let Some((fence_char, open_len)) = parse_fence_open(line) {
                flush_prose_segment(input, cursor, &mut prose_start, &mut segments);
                let mut scan = line_end;
                while scan < input.len() {
                    let next_end = input[scan..]
                        .find('\n')
                        .map_or(input.len(), |i| scan + i + 1);
                    let next_line = &input[scan..next_end];
                    scan = next_end;
                    if is_fence_close(next_line, fence_char, open_len) {
                        break;
                    }
                }
                segments.push(OutputSegment::Protected(&input[cursor..scan]));
                cursor = scan;
                continue;
            }
        }

        if let Some((tag_pos, open_len, _, close_tag)) =
            find_earliest_protected_open_tag(input, cursor, line_end)
        {
            if tag_pos > cursor {
                prose_start.get_or_insert(cursor);
            }
            flush_prose_segment(input, tag_pos, &mut prose_start, &mut segments);
            let body_start = tag_pos + open_len;
            let block_end = find_ci(input, close_tag, body_start)
                .map_or(input.len(), |end| end + close_tag.len());
            segments.push(OutputSegment::Protected(&input[tag_pos..block_end]));
            cursor = block_end;
            continue;
        }

        prose_start.get_or_insert(cursor);
        cursor = line_end;
    }

    flush_prose_segment(input, input.len(), &mut prose_start, &mut segments);
    segments
}

fn starts_with_ascii_ci(s: &str, prefix: &str) -> bool {
    s.get(..prefix.len())
        .is_some_and(|head| head.eq_ignore_ascii_case(prefix))
}

/// Matches a leaked self-addressed agent greeting prefix such as
/// `"Got it, Commander! Doing it again: "` or `"Got it, Commander! Hearing you:"`
/// at the start of `s`, returning the prefix byte length to strip.
fn match_spurious_greeting_prefix(s: &str) -> Option<usize> {
    let intro_len = if starts_with_ascii_ci(s, "got it, ") {
        8
    } else if starts_with_ascii_ci(s, "understood, ") {
        12
    } else {
        return None;
    };

    let after_intro = &s[intro_len..];
    let (sep_byte, sep_ch) = after_intro
        .char_indices()
        .take_while(|&(idx, ch)| idx <= 32 && ch != '\n' && ch != '\r')
        .find(|&(_, ch)| matches!(ch, '!' | '.' | ',' | ':'))?;

    let agent_raw = after_intro[..sep_byte].trim();
    if agent_raw.is_empty()
        || agent_raw.chars().count() > 24
        || agent_raw.split_whitespace().count() > 2
        || !agent_raw
            .chars()
            .all(|c| c.is_alphanumeric() || matches!(c, ' ' | '-' | '_'))
    {
        return None;
    }

    let is_known_role = KNOWN_AGENT_ROLES
        .iter()
        .any(|role| agent_raw.eq_ignore_ascii_case(role));
    let after_sep = &after_intro[sep_byte + sep_ch.len_utf8()..];
    let trimmed_tail = after_sep.trim_start_matches([' ', '\t']);
    let ws_after_sep = after_sep.len() - trimmed_tail.len();
    let base_strip = intro_len + sep_byte + sep_ch.len_utf8() + ws_after_sep;

    for filler in KNOWN_LOOP_FILLERS {
        if starts_with_ascii_ci(trimmed_tail, filler) {
            let after_filler = &trimmed_tail[filler.len()..];
            let ws2 = after_filler.len() - after_filler.trim_start_matches([' ', '\t']).len();
            return Some(base_strip + filler.len() + ws2);
        }
    }

    if !is_known_role {
        return None;
    }

    if trimmed_tail.is_empty() {
        return Some(s.len());
    }

    // Standalone greeting + short loop intro ending with `:` at EOL, e.g. "Got it, Commander! Here it goes:"
    if trimmed_tail.ends_with(':') && trimmed_tail.len() <= 40 && !trimmed_tail.contains('/') {
        return Some(s.len());
    }

    if matches!(sep_ch, '!' | ':') {
        return Some(base_strip);
    }

    None
}

fn strip_spurious_greeting_segments_from_line(line: &str) -> (Vec<(String, bool)>, bool) {
    let mut cur = line.to_string();
    let mut prefix_stripped = false;

    loop {
        let trimmed = cur.trim_start_matches([' ', '\t']);
        let leading_ws = cur.len() - trimmed.len();
        if let Some(strip_len) = match_spurious_greeting_prefix(trimmed) {
            let remainder = trimmed[strip_len..].to_string();
            cur = if leading_ws > 0 && !remainder.is_empty() {
                format!("{}{}", &cur[..leading_ws], remainder)
            } else {
                remainder
            };
            prefix_stripped = true;
            continue;
        }
        break;
    }

    // Also split & strip leaked greeting headers appearing inline right after sentence punctuation.
    let mut segments: Vec<(String, bool)> = Vec::new();
    let mut seg_start = 0usize;
    let mut seg_had_header = prefix_stripped;
    let mut search_from = 0usize;
    let mut any_inline_stripped = false;

    while search_from < cur.len() {
        let mut found: Option<(usize, usize, usize)> = None;
        for (idx, ch) in cur[search_from..].char_indices() {
            let abs = search_from + idx;
            if matches!(ch, '.' | '!' | '?') {
                let after_punct = abs + ch.len_utf8();
                if let Some(rest) = cur.get(after_punct..) {
                    let ws_len = rest.len() - rest.trim_start_matches([' ', '\t']).len();
                    if ws_len > 0 {
                        let cand_start = after_punct + ws_len;
                        if let Some(strip_len) = match_spurious_greeting_prefix(&cur[cand_start..]) {
                            found = Some((after_punct, cand_start, cand_start + strip_len));
                            break;
                        }
                    }
                }
            }
        }
        match found {
            Some((after_punct, _rm_start, rm_end)) => {
                let head = cur[seg_start..after_punct]
                    .trim_end_matches([' ', '\t'])
                    .to_string();
                if !head.trim().is_empty() {
                    segments.push((head, seg_had_header));
                }
                any_inline_stripped = true;
                seg_had_header = true;
                seg_start = rm_end;
                search_from = rm_end;
            }
            None => break,
        }
    }

    let tail = cur[seg_start..].trim_end_matches([' ', '\t']).to_string();
    if !tail.trim().is_empty() {
        segments.push((tail, seg_had_header));
    }

    (segments, prefix_stripped || any_inline_stripped)
}

fn strip_spurious_headers_in_paragraph(para: &str) -> (String, bool) {
    let mut lines_out = Vec::new();
    let mut any_stripped = false;
    for raw_line in para.lines() {
        let clean_raw = raw_line.trim_end_matches('\r');
        let (segments, stripped) = strip_spurious_greeting_segments_from_line(clean_raw);
        if stripped {
            any_stripped = true;
        }
        let line_joined = segments
            .into_iter()
            .map(|(s, _)| s)
            .filter(|s| !s.trim().is_empty())
            .collect::<Vec<_>>()
            .join(" ");
        if !line_joined.trim().is_empty() {
            lines_out.push(line_joined);
        }
    }
    (lines_out.join("\n"), any_stripped)
}

fn normalize_prose_key(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut last_ws = true;
    for ch in s.chars() {
        if ch.is_whitespace() {
            if !last_ws {
                out.push(' ');
                last_ws = true;
            }
        } else {
            let norm_ch = match ch {
                '\u{2018}' | '\u{2019}' => '\'',
                '\u{201C}' | '\u{201D}' => '"',
                '\u{2013}' | '\u{2014}' => '-',
                other => other,
            };
            for lc in norm_ch.to_lowercase() {
                out.push(lc);
            }
            last_ws = false;
        }
    }
    if out.ends_with(' ') {
        out.pop();
    }
    out
}

fn is_pure_number_or_divider(s: &str) -> bool {
    let t = s.trim();
    !t.is_empty()
        && t.chars()
            .all(|c| c.is_ascii_digit() || c.is_ascii_punctuation() || c.is_whitespace())
}

fn starts_with_list_marker(s: &str) -> bool {
    let t = s.trim_start();
    if t.starts_with("- ") || t.starts_with("* ") || t.starts_with("+ ") {
        return true;
    }
    let digits = t.bytes().take_while(|b| b.is_ascii_digit()).count();
    digits > 0
        && digits <= 4
        && t.as_bytes()
            .get(digits..)
            .is_some_and(|rest| rest.starts_with(b". ") || rest.starts_with(b") "))
}

fn is_markdown_table_row(s: &str) -> bool {
    let t = s.trim();
    t.starts_with('|') && t.ends_with('|') && t.len() >= 3
}

fn is_protected_boilerplate_short_item(s: &str, norm_chars: usize) -> bool {
    is_pure_number_or_divider(s)
        || is_markdown_table_row(s)
        || (starts_with_list_marker(s) && norm_chars < 35)
}

fn split_blank_line_blocks(prose: &str) -> Vec<&str> {
    let mut blocks = Vec::new();
    let mut offset = 0usize;
    let mut block_start: Option<usize> = None;
    let mut block_end = 0usize;

    for raw_line in prose.split_inclusive('\n') {
        let line_len = raw_line.len();
        let line_trimmed = raw_line.trim();
        if line_trimmed.is_empty() {
            if let Some(st) = block_start.take() {
                let slice = prose[st..block_end].trim_end_matches(['\r', '\n']);
                if !slice.trim().is_empty() {
                    blocks.push(slice);
                }
            }
        } else {
            block_start.get_or_insert(offset);
            block_end = offset + line_len;
        }
        offset += line_len;
    }

    if let Some(st) = block_start {
        let slice = prose[st..block_end].trim_end_matches(['\r', '\n']);
        if !slice.trim().is_empty() {
            blocks.push(slice);
        }
    }

    blocks
}

fn extract_digit_runs(s: &str) -> Vec<&str> {
    let mut nums = Vec::new();
    let mut start: Option<usize> = None;
    for (idx, ch) in s.char_indices() {
        if ch.is_ascii_digit() {
            start.get_or_insert(idx);
        } else if let Some(st) = start.take() {
            nums.push(&s[st..idx]);
        }
    }
    if let Some(st) = start {
        nums.push(&s[st..]);
    }
    nums
}

fn are_near_identical_paragraphs(prev_norm: &str, curr_norm: &str) -> bool {
    if prev_norm == curr_norm {
        return true;
    }
    let min_l = prev_norm.len().min(curr_norm.len());
    let max_l = prev_norm.len().max(curr_norm.len());
    if min_l < 80 || min_l * 10 < max_l * 8 {
        return false;
    }
    if extract_digit_runs(prev_norm) != extract_digit_runs(curr_norm) {
        return false;
    }
    let words_a: Vec<&str> = prev_norm.split_whitespace().collect();
    let words_b: Vec<&str> = curr_norm.split_whitespace().collect();
    if words_a.len() < 12 || words_b.len() < 12 || words_a[..3] != words_b[..3] {
        return false;
    }
    let mut used = vec![false; words_b.len()];
    let mut matched = 0usize;
    for wa in &words_a {
        if let Some(pos) = words_b
            .iter()
            .enumerate()
            .position(|(idx, wb)| !used[idx] && wb == wa)
        {
            used[pos] = true;
            matched += 1;
        }
    }
    let total = words_a.len().max(words_b.len());
    matched * 100 >= total * 85
}

fn is_abbreviation_before_dot(s: &str, dot_idx: usize) -> bool {
    let prefix = &s[..dot_idx];
    let token = prefix
        .rsplit(|c: char| c.is_whitespace() || matches!(c, '(' | '[' | '"' | '\''))
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    if token.is_empty() {
        return false;
    }
    if token.len() <= 2
        && (token.chars().all(|c| c.is_ascii_digit()) || token == "e.g" || token == "i.e")
    {
        return true;
    }
    matches!(
        token.as_str(),
        "e.g" | "i.e" | "etc" | "vs" | "mr" | "mrs" | "ms" | "dr" | "prof" | "fig" | "no" | "vol"
            | "approx"
    )
}

fn split_sentence_spans(line: &str) -> Vec<(usize, usize)> {
    let mut spans = Vec::new();
    let mut start = 0usize;
    let bytes = line.as_bytes();
    for (idx, ch) in line.char_indices() {
        if matches!(ch, '.' | '!' | '?') {
            let end = idx + ch.len_utf8();
            let next_is_ws = bytes.get(end).is_some_and(u8::is_ascii_whitespace);
            let at_end = end == line.len();
            if (next_is_ws || at_end)
                && (ch != '.' || !is_abbreviation_before_dot(line, idx))
            {
                let span = &line[start..end];
                if !span.trim().is_empty() {
                    let trim_start = start + (span.len() - span.trim_start().len());
                    spans.push((trim_start, end));
                }
                start = end;
                while start < line.len() && bytes[start].is_ascii_whitespace() {
                    start += 1;
                }
            }
        }
    }
    if start < line.len() && !line[start..].trim().is_empty() {
        let span = &line[start..];
        let trim_start = start + (span.len() - span.trim_start().len());
        spans.push((trim_start, line.len()));
    }
    spans
}

fn find_contiguous_suffix_repeat(text: &str, min_unit_chars: usize) -> Option<usize> {
    let trimmed = text.trim_end();
    if trimmed.chars().count() < min_unit_chars * 2 {
        return None;
    }

    let mut win_start = trimmed.len().saturating_sub(MAX_REPETITION_WINDOW_BYTES);
    while !trimmed.is_char_boundary(win_start) {
        win_start += 1;
    }
    let win = &trimmed[win_start..];
    let anchor_chars = 24.min(min_unit_chars).max(8);
    let anchor_byte_start = win
        .char_indices()
        .rev()
        .nth(anchor_chars - 1)
        .map(|(idx, _)| idx)?;
    let anchor = &win[anchor_byte_start..];

    let mut search_end = anchor_byte_start;
    let mut hits = 0usize;
    while hits < 16 {
        let Some(pos) = win[..search_end].rfind(anchor) else {
            break;
        };
        hits += 1;
        let first_end = pos + anchor.len();
        let after_first = &win[first_end..];
        let ws_len = after_first.len() - after_first.trim_start().len();
        let second_x = &after_first[ws_len..];
        if !second_x.is_empty()
            && first_end >= second_x.len()
            && win.is_char_boundary(first_end - second_x.len())
            && &win[first_end - second_x.len()..first_end] == second_x
        {
            let first_start = first_end - second_x.len();
            let clean_boundary = first_start == 0
                || !win[..first_start]
                    .chars()
                    .next_back()
                    .is_some_and(char::is_alphanumeric);
            let unit_chars = second_x.trim().chars().count();
            if clean_boundary
                && unit_chars >= min_unit_chars
                && !is_pure_number_or_divider(second_x)
            {
                return Some(win_start + first_end);
            }
        }
        if pos == 0 {
            break;
        }
        search_end = pos;
    }
    None
}

fn trim_contiguous_phrase_repeats(line: &str) -> (String, bool) {
    let mut cur = line.to_string();
    let mut changed = false;
    for _ in 0..8 {
        if let Some(cut_idx) = find_contiguous_suffix_repeat(&cur, 40) {
            cur.truncate(cut_idx);
            cur = cur.trim_end().to_string();
            changed = true;
        } else {
            break;
        }
    }
    (cur, changed)
}

fn collapse_contiguous_cycles<T>(
    items: &[T],
    keys: &[String],
    min_k1_chars: usize,
    min_cycle_chars: usize,
    allow_k1_short_repeat_at_3: usize,
    skip_fn: impl Fn(&T, usize) -> bool,
) -> (Vec<usize>, bool) {
    let mut keep = Vec::with_capacity(items.len());
    let mut changed = false;
    let mut i = 0usize;

    while i < items.len() {
        let mut matched_cycle: Option<(usize, usize)> = None;
        let max_k = (items.len() - i) / 2;
        for k in 1..=max_k {
            if keys[i..i + k] != keys[i + k..i + 2 * k] {
                continue;
            }
            let valid = if k == 1 {
                let len = keys[i].chars().count();
                if skip_fn(&items[i], len) {
                    false
                } else if len >= min_k1_chars {
                    true
                } else if allow_k1_short_repeat_at_3 > 0 && len >= allow_k1_short_repeat_at_3 {
                    i + 2 < items.len() && keys[i] == keys[i + 2]
                } else {
                    false
                }
            } else {
                let total_chars: usize = keys[i..i + k].iter().map(|s| s.chars().count()).sum();
                total_chars >= min_cycle_chars
            };
            if valid {
                let mut rep_end = i + 2 * k;
                while rep_end + k <= items.len() && keys[i..i + k] == keys[rep_end..rep_end + k] {
                    rep_end += k;
                }
                matched_cycle = Some((k, rep_end));
                break;
            }
        }

        if let Some((k, rep_end)) = matched_cycle {
            for idx in i..i + k {
                keep.push(idx);
            }
            i = rep_end;
            changed = true;
        } else {
            keep.push(i);
            i += 1;
        }
    }

    (keep, changed)
}

fn deduplicate_line_sentences(line: &str) -> (String, bool) {
    let spans = split_sentence_spans(line);
    if spans.len() < 2 {
        return trim_contiguous_phrase_repeats(line);
    }

    let sentences: Vec<&str> = spans.iter().map(|&(s, e)| &line[s..e]).collect();
    let keys: Vec<String> = sentences.iter().map(|s| normalize_prose_key(s)).collect();

    let (cycle_kept_idxs, mut changed) = collapse_contiguous_cycles(
        &sentences,
        &keys,
        40,
        40,
        15,
        |s, c| is_protected_boilerplate_short_item(s, c),
    );

    let mut seen_substantial: HashSet<String> = HashSet::new();
    let mut kept_texts: Vec<String> = Vec::with_capacity(cycle_kept_idxs.len());
    for idx in cycle_kept_idxs {
        let key = &keys[idx];
        let chars = key.chars().count();
        if chars >= 50 && !is_protected_boilerplate_short_item(sentences[idx], chars) {
            if !seen_substantial.insert(key.clone()) {
                changed = true;
                continue;
            }
        }
        kept_texts.push(sentences[idx].to_string());
    }

    // Drop trailing partial sentence repetition if it starts an earlier kept sentence.
    if kept_texts.len() >= 2 {
        let last_key = normalize_prose_key(kept_texts.last().unwrap());
        let last_chars = last_key.chars().count();
        let min_prefix_chars = if changed { 25 } else { 40 };
        if last_chars >= min_prefix_chars
            && kept_texts[..kept_texts.len() - 1]
                .iter()
                .any(|prev| normalize_prose_key(prev).starts_with(&last_key))
        {
            kept_texts.pop();
            changed = true;
        }
    }

    let joined = if changed {
        kept_texts.join(" ")
    } else {
        line.to_string()
    };
    let (final_line, phrase_changed) = trim_contiguous_phrase_repeats(&joined);
    (final_line, changed || phrase_changed)
}

fn deduplicate_paragraph_lines(para: &str) -> (String, bool) {
    let raw_lines: Vec<&str> = para.lines().map(|l| l.trim_end_matches('\r')).collect();
    let mut changed = false;

    let keys: Vec<String> = raw_lines.iter().map(|l| normalize_prose_key(l)).collect();
    let (cycle_idxs, cycle_changed) = collapse_contiguous_cycles(
        &raw_lines,
        &keys,
        40,
        40,
        20,
        |l, c| is_protected_boilerplate_short_item(l, c),
    );
    if cycle_changed {
        changed = true;
    }

    let mut seen_substantial: HashSet<String> = HashSet::new();
    let mut cleaned_lines: Vec<String> = Vec::with_capacity(cycle_idxs.len());
    for idx in cycle_idxs {
        let line = raw_lines[idx];
        let key = &keys[idx];
        let chars = key.chars().count();
        let is_indented_code_like = line.starts_with("    ") || line.starts_with('\t');
        if chars >= 50
            && !is_indented_code_like
            && !is_protected_boilerplate_short_item(line, chars)
        {
            if !seen_substantial.insert(key.clone()) {
                changed = true;
                continue;
            }
        }
        if !is_indented_code_like && !is_markdown_table_row(line) {
            let (deduped_line, line_changed) = deduplicate_line_sentences(line);
            if line_changed {
                changed = true;
            }
            if !deduped_line.trim().is_empty() || line.trim().is_empty() {
                cleaned_lines.push(deduped_line);
            }
        } else {
            cleaned_lines.push(line.to_string());
        }
    }

    if cleaned_lines.len() >= 2 {
        let last_key = normalize_prose_key(cleaned_lines.last().unwrap());
        let last_chars = last_key.chars().count();
        let min_prefix_chars = if changed { 25 } else { 40 };
        if last_chars >= min_prefix_chars
            && cleaned_lines[..cleaned_lines.len() - 1]
                .iter()
                .any(|prev| normalize_prose_key(prev).starts_with(&last_key))
        {
            cleaned_lines.pop();
            changed = true;
        }
    }

    if changed {
        (cleaned_lines.join("\n"), true)
    } else {
        (para.to_string(), false)
    }
}

#[derive(Debug, Clone)]
struct ProseBlock {
    text: String,
    preceded_by_spurious_header: bool,
}

fn split_paragraph_on_spurious_headers(para: &str) -> (Vec<(String, bool)>, bool) {
    let mut subblocks: Vec<(Vec<String>, bool)> = Vec::new();
    let mut cur_lines: Vec<String> = Vec::new();
    let mut cur_header = false;
    let mut any_stripped = false;

    for raw_line in para.lines() {
        let clean_raw = raw_line.trim_end_matches('\r');
        let (segments, stripped) = strip_spurious_greeting_segments_from_line(clean_raw);
        if stripped {
            any_stripped = true;
        }
        if segments.is_empty() && stripped {
            if !cur_lines.is_empty() {
                subblocks.push((std::mem::take(&mut cur_lines), cur_header));
            }
            cur_header = true;
            continue;
        }
        for (seg_text, seg_had_header) in segments {
            if seg_had_header {
                if !cur_lines.is_empty() {
                    subblocks.push((std::mem::take(&mut cur_lines), cur_header));
                }
                cur_header = true;
            }
            if !seg_text.trim().is_empty() {
                cur_lines.push(seg_text);
            }
        }
    }
    if !cur_lines.is_empty() {
        subblocks.push((cur_lines, cur_header));
    }

    let out = subblocks
        .into_iter()
        .map(|(lines, header)| (lines.join("\n"), header))
        .collect();
    (out, any_stripped)
}

fn collect_prose_blocks(prose: &str) -> (Vec<ProseBlock>, bool) {
    let raw_blocks = split_blank_line_blocks(prose);
    let mut blocks = Vec::with_capacity(raw_blocks.len());
    let mut pending_header = false;
    let mut any_changed = false;

    for raw in raw_blocks {
        let (subblocks, had_any_header) = split_paragraph_on_spurious_headers(raw);
        if had_any_header {
            any_changed = true;
        }
        if subblocks.is_empty() {
            if had_any_header {
                pending_header = true;
            }
            continue;
        }
        for (idx, (stripped, sub_header)) in subblocks.into_iter().enumerate() {
            let (cleaned, line_changed) = deduplicate_paragraph_lines(&stripped);
            if line_changed {
                any_changed = true;
            }
            if !cleaned.trim().is_empty() {
                let header_flag = sub_header || (idx == 0 && pending_header);
                blocks.push(ProseBlock {
                    text: cleaned,
                    preceded_by_spurious_header: header_flag,
                });
                pending_header = false;
            } else if sub_header {
                pending_header = true;
            }
        }
    }

    (blocks, any_changed)
}

fn deduplicate_prose_segment(
    prose: &str,
    seen_counts: &mut HashSet<(String, bool)>,
    seen_all_keys: &mut Vec<String>,
    seen_substantial_keys: &mut Vec<String>,
    had_global_dup: &mut bool,
    has_prev_segment: bool,
    has_next_segment: bool,
) -> (String, bool) {
    let (blocks, mut any_changed) = collect_prose_blocks(prose);
    if blocks.is_empty() {
        if any_changed {
            return (String::new(), true);
        }
        return (prose.to_string(), false);
    }

    let keys: Vec<String> = blocks.iter().map(|b| normalize_prose_key(&b.text)).collect();
    let (cycle_idxs, cycle_changed) = collapse_contiguous_cycles(
        &blocks,
        &keys,
        15,
        40,
        8,
        |b, c| !b.preceded_by_spurious_header && is_protected_boilerplate_short_item(&b.text, c),
    );
    if cycle_changed {
        any_changed = true;
        *had_global_dup = true;
    }

    let mut kept_blocks: Vec<ProseBlock> = Vec::with_capacity(cycle_idxs.len());
    for idx in cycle_idxs {
        let b = &blocks[idx];
        let key = &keys[idx];
        let chars = key.chars().count();
        let is_short_boilerplate =
            !b.preceded_by_spurious_header && is_protected_boilerplate_short_item(&b.text, chars);

        let dup_due_to_header = b.preceded_by_spurious_header && seen_all_keys.contains(key);
        let dup_first_repeat = !is_short_boilerplate && chars >= 40 && seen_all_keys.contains(key);
        let dup_third_repeat =
            !is_short_boilerplate && chars >= 20 && seen_counts.contains(&(key.clone(), true));
        let dup_near = !is_short_boilerplate
            && seen_substantial_keys
                .iter()
                .any(|prev| are_near_identical_paragraphs(prev, key));

        if dup_due_to_header || dup_first_repeat || dup_third_repeat || dup_near {
            any_changed = true;
            *had_global_dup = true;
            continue;
        }

        if !seen_counts.insert((key.clone(), false)) {
            seen_counts.insert((key.clone(), true));
        }
        seen_all_keys.push(key.clone());
        if chars >= 80 && !is_short_boilerplate {
            seen_substantial_keys.push(key.clone());
        }
        kept_blocks.push(b.clone());
    }

    if !kept_blocks.is_empty() && !has_next_segment {
        let last_key = normalize_prose_key(&kept_blocks.last().unwrap().text);
        let last_chars = last_key.chars().count();
        let last_spurious = kept_blocks.last().unwrap().preceded_by_spurious_header;
        let min_prefix_chars = if *had_global_dup || last_spurious {
            25
        } else {
            40
        };
        let is_bullet = is_protected_boilerplate_short_item(
            &kept_blocks.last().unwrap().text,
            last_chars,
        );
        if !is_bullet && last_chars >= min_prefix_chars {
            let earlier_match = seen_all_keys[..seen_all_keys.len().saturating_sub(1)]
                .iter()
                .any(|prev| prev.starts_with(&last_key));
            if earlier_match {
                kept_blocks.pop();
                any_changed = true;
                *had_global_dup = true;
            }
        }
    }

    if !any_changed {
        return (prose.to_string(), false);
    }

    let joined: String = kept_blocks
        .iter()
        .map(|b| b.text.as_str())
        .collect::<Vec<_>>()
        .join("\n\n");

    if joined.is_empty() {
        return (String::new(), true);
    }

    let mut out = String::new();
    if has_prev_segment && prose.starts_with('\n') {
        if prose.starts_with("\n\n") || prose.starts_with("\r\n\r\n") {
            out.push_str("\n\n");
        } else {
            out.push('\n');
        }
    }
    out.push_str(&joined);
    if has_next_segment && prose.ends_with('\n') {
        if prose.ends_with("\n\n") || prose.ends_with("\r\n\r\n") {
            out.push_str("\n\n");
        } else {
            out.push('\n');
        }
    }
    (out, true)
}

/// Removes verbatim repeated prose loops and leaked self-addressed agent greeting
/// headers (such as `"Got it, Commander! Doing it again:"`) while preserving
/// `<think>...</think>`, `<seed:think>...</seed:think>`, `<tool_call>...</tool_call>`,
/// and fenced code blocks (` ``` ` / `~~~`) verbatim untouched.
pub fn truncate_verbatim_prose_loops(input: &str) -> String {
    if input.is_empty() {
        return String::new();
    }

    let segments = split_protected_segments(input);
    let has_protected = segments
        .iter()
        .any(|seg| matches!(seg, OutputSegment::Protected(_)));

    let mut seen_counts: HashSet<(String, bool)> = HashSet::new();
    let mut seen_all_keys: Vec<String> = Vec::new();
    let mut seen_substantial_keys: Vec<String> = Vec::new();
    let mut had_global_dup = false;

    let mut rendered: Vec<(bool, String)> = Vec::with_capacity(segments.len());
    let mut any_changed = false;

    for (idx, seg) in segments.iter().enumerate() {
        match seg {
            OutputSegment::Protected(raw) => {
                rendered.push((true, (*raw).to_string()));
            }
            OutputSegment::Prose(raw) => {
                let has_prev = idx > 0;
                let has_next = idx + 1 < segments.len();
                let (cleaned, changed) = deduplicate_prose_segment(
                    raw,
                    &mut seen_counts,
                    &mut seen_all_keys,
                    &mut seen_substantial_keys,
                    &mut had_global_dup,
                    has_prev,
                    has_next,
                );
                if changed {
                    any_changed = true;
                }
                rendered.push((false, cleaned));
            }
        }
    }

    if !any_changed {
        return input.to_string();
    }

    let mut out = String::with_capacity(input.len());
    let mut prev_emitted_non_empty = false;
    for (idx, (is_protected, text)) in rendered.iter().enumerate() {
        if text.is_empty() {
            // If all prose between two protected segments was stripped, keep a blank line separator.
            if prev_emitted_non_empty
                && idx + 1 < rendered.len()
                && rendered[idx + 1].0
                && !out.ends_with("\n\n")
            {
                if out.ends_with('\n') {
                    out.push('\n');
                } else {
                    out.push_str("\n\n");
                }
            }
            continue;
        }
        if *is_protected && prev_emitted_non_empty && !out.ends_with('\n') && !text.starts_with('\n')
        {
            out.push_str("\n\n");
        }
        out.push_str(text);
        prev_emitted_non_empty = true;
    }

    if rendered
        .last()
        .is_some_and(|(is_prot, text)| !is_prot && text.is_empty())
    {
        while out.ends_with(['\r', '\n']) {
            out.pop();
        }
    }

    if !has_protected && out.trim().is_empty() {
        return input.to_string();
    }

    out
}

/// Alias for [`truncate_verbatim_prose_loops`].
pub fn deduplicate_repeated_prose_blocks(input: &str) -> String {
    truncate_verbatim_prose_loops(input)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamingLoopDecision {
    Continue,
    AbortRepeatedLoop { truncated_clean_visible: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum GuardTagMode {
    #[default]
    Normal,
    InThink(ThinkVariant),
    InToolCall,
}

/// Incremental SSE prose loop detector that monitors visible non-thinking,
/// non-`<tool_call>`, outside-fenced-code-block text across streamed deltas
/// and aborts verbatim repetition loops early with clean pre-loop visible text.
#[derive(Debug, Clone, Default)]
pub struct StreamingRepetitionGuard {
    visible_acc: String,
    prose_acc: String,
    tag_pending: String,
    tag_mode: GuardTagMode,
    line_buf: String,
    line_flushed_len: usize,
    in_fence: Option<(u8, usize)>,
    aborted_clean: Option<String>,
}

impl StreamingRepetitionGuard {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn visible_so_far(&self) -> &str {
        &self.visible_acc
    }

    pub fn push_delta(&mut self, delta: &str) -> StreamingLoopDecision {
        if let Some(ref clean) = self.aborted_clean {
            return StreamingLoopDecision::AbortRepeatedLoop {
                truncated_clean_visible: clean.clone(),
            };
        }
        if delta.is_empty() {
            return StreamingLoopDecision::Continue;
        }

        self.tag_pending.push_str(delta);
        self.drain_tag_pending(false);

        self.evaluate_repetition()
    }

    pub fn finish(&mut self) -> StreamingLoopDecision {
        if let Some(ref clean) = self.aborted_clean {
            return StreamingLoopDecision::AbortRepeatedLoop {
                truncated_clean_visible: clean.clone(),
            };
        }
        self.drain_tag_pending(true);
        self.flush_line_buf_eof();
        self.evaluate_repetition()
    }

    fn evaluate_repetition(&mut self) -> StreamingLoopDecision {
        if let Some(ref clean) = self.aborted_clean {
            return StreamingLoopDecision::AbortRepeatedLoop {
                truncated_clean_visible: clean.clone(),
            };
        }
        if self.detect_prose_repetition() {
            let truncated = truncate_verbatim_prose_loops(&self.visible_acc)
                .trim()
                .to_string();
            self.aborted_clean = Some(truncated.clone());
            return StreamingLoopDecision::AbortRepeatedLoop {
                truncated_clean_visible: truncated,
            };
        }
        StreamingLoopDecision::Continue
    }

    fn drain_tag_pending(&mut self, flush_all: bool) {
        loop {
            match self.tag_mode {
                GuardTagMode::InThink(variant) => {
                    let close_tag = variant.close_tag();
                    if let Some(idx) = find_ci(&self.tag_pending, close_tag, 0) {
                        self.tag_pending.drain(..idx + close_tag.len());
                        self.tag_mode = GuardTagMode::Normal;
                        continue;
                    }
                    if flush_all {
                        self.tag_pending.clear();
                        self.tag_mode = GuardTagMode::Normal;
                    } else {
                        let hold = partial_tag_suffix_len(&self.tag_pending, close_tag);
                        let drop_len = self.tag_pending.len() - hold;
                        if drop_len > 0 {
                            self.tag_pending.drain(..drop_len);
                        }
                    }
                    break;
                }
                GuardTagMode::InToolCall => {
                    let close_tag = "</tool_call>";
                    if let Some(idx) = find_ci(&self.tag_pending, close_tag, 0) {
                        let end = idx + close_tag.len();
                        self.visible_acc.push_str(&self.tag_pending[..end]);
                        self.tag_pending.drain(..end);
                        self.tag_mode = GuardTagMode::Normal;
                        self.push_prose_boundary();
                        continue;
                    }
                    if flush_all {
                        self.visible_acc.push_str(&self.tag_pending);
                        self.tag_pending.clear();
                        self.tag_mode = GuardTagMode::Normal;
                        self.push_prose_boundary();
                    } else {
                        let hold = partial_tag_suffix_len(&self.tag_pending, close_tag);
                        let emit_len = self.tag_pending.len() - hold;
                        if emit_len > 0 {
                            self.visible_acc.push_str(&self.tag_pending[..emit_len]);
                            self.tag_pending.drain(..emit_len);
                        }
                    }
                    break;
                }
                GuardTagMode::Normal => {
                    if let Some((idx, open_len, kind, _)) = find_earliest_protected_open_tag(
                        &self.tag_pending,
                        0,
                        self.tag_pending.len(),
                    ) {
                        if idx > 0 {
                            let vis = self.tag_pending[..idx].to_string();
                            self.visible_acc.push_str(&vis);
                            self.push_visible_chunk(&vis);
                        }
                        match kind {
                            GuardTagKind::Think(variant) => {
                                self.tag_pending.drain(..idx + open_len);
                                self.tag_mode = GuardTagMode::InThink(variant);
                            }
                            GuardTagKind::ToolCall => {
                                self.visible_acc
                                    .push_str(&self.tag_pending[idx..idx + open_len]);
                                self.tag_pending.drain(..idx + open_len);
                                self.tag_mode = GuardTagMode::InToolCall;
                                self.push_prose_boundary();
                            }
                        }
                        continue;
                    }

                    let hold = if flush_all {
                        0
                    } else {
                        max_partial_tag_suffix_len(&self.tag_pending, &STREAM_GUARD_TAG_OPENS)
                    };
                    let emit_len = self.tag_pending.len() - hold;
                    if emit_len > 0 {
                        let vis = self.tag_pending[..emit_len].to_string();
                        self.tag_pending.drain(..emit_len);
                        self.visible_acc.push_str(&vis);
                        self.push_visible_chunk(&vis);
                    }
                    break;
                }
            }
        }
    }

    fn push_prose_boundary(&mut self) {
        if self.in_fence.is_none() && self.line_flushed_len < self.line_buf.len() {
            if !could_be_fence_open_prefix(&self.line_buf) {
                self.prose_acc
                    .push_str(&self.line_buf[self.line_flushed_len..]);
            }
            self.line_buf.clear();
            self.line_flushed_len = 0;
        }
        if !self.prose_acc.is_empty() && !self.prose_acc.ends_with(PROSE_BOUNDARY_MARKER) {
            self.prose_acc.push(PROSE_BOUNDARY_MARKER);
        }
    }

    fn flush_line_buf_eof(&mut self) {
        if self.in_fence.is_none() && self.line_flushed_len < self.line_buf.len() {
            if !could_be_fence_open_prefix(&self.line_buf) {
                self.prose_acc
                    .push_str(&self.line_buf[self.line_flushed_len..]);
            }
        }
        self.line_buf.clear();
        self.line_flushed_len = 0;
    }

    fn push_visible_chunk(&mut self, chunk: &str) {
        for piece in chunk.split_inclusive('\n') {
            let ends_nl = piece.ends_with('\n');
            let body = if ends_nl {
                &piece[..piece.len() - 1]
            } else {
                piece
            };
            self.line_buf.push_str(body);

            if ends_nl {
                if let Some((fchar, open_len)) = self.in_fence {
                    if is_fence_close(&self.line_buf, fchar, open_len) {
                        self.in_fence = None;
                        self.push_prose_boundary();
                    }
                } else if let Some((fchar, open_len)) = parse_fence_open(&self.line_buf) {
                    self.in_fence = Some((fchar, open_len));
                    self.push_prose_boundary();
                } else {
                    self.prose_acc
                        .push_str(&self.line_buf[self.line_flushed_len..]);
                    self.prose_acc.push('\n');
                }
                self.line_buf.clear();
                self.line_flushed_len = 0;
            } else if self.in_fence.is_none() && !could_be_fence_open_prefix(&self.line_buf) {
                self.prose_acc
                    .push_str(&self.line_buf[self.line_flushed_len..]);
                self.line_flushed_len = self.line_buf.len();
            }
        }
    }

    fn detect_prose_repetition(&self) -> bool {
        let runs: Vec<&str> = self
            .prose_acc
            .split(PROSE_BOUNDARY_MARKER)
            .filter(|r| !r.trim().is_empty())
            .collect();
        if runs.is_empty() {
            return false;
        }

        // 1. Consecutive repeated non-empty lines (>= 3 consecutive repeats of >= 20 chars)
        let mut non_empty_lines: Vec<String> = Vec::new();
        for run in &runs {
            for line in run.lines() {
                let key = normalize_prose_key(line);
                if !key.is_empty() {
                    non_empty_lines.push(key);
                }
            }
        }
        if has_consecutive_repeated_keys(&non_empty_lines, 3, 20) {
            return true;
        }

        // 2. Substantial repeated paragraph / multi-paragraph cycle (>= 2 repeats of >= 40 chars)
        let mut raw_paras: Vec<(String, bool)> = Vec::new();
        let mut cleaned_paras: Vec<(String, bool)> = Vec::new();
        for (r_idx, run) in runs.iter().enumerate() {
            let is_last_run = r_idx + 1 == runs.len();
            let ends_blank = run.ends_with("\n\n") || run.ends_with("\r\n\r\n");
            let blocks = split_blank_line_blocks(run);
            for (b_idx, block) in blocks.iter().enumerate() {
                let is_complete = !(is_last_run && b_idx + 1 == blocks.len() && !ends_blank);
                let raw_key = normalize_prose_key(block);
                if !raw_key.is_empty() && !is_protected_boilerplate_short_item(block, raw_key.chars().count()) {
                    raw_paras.push((raw_key, is_complete));
                }
                let (stripped, _) = strip_spurious_headers_in_paragraph(block);
                let clean_key = normalize_prose_key(&stripped);
                if !clean_key.is_empty()
                    && !is_protected_boilerplate_short_item(&stripped, clean_key.chars().count())
                {
                    cleaned_paras.push((clean_key, is_complete));
                }
            }
        }
        if has_repeated_substantial_units(&raw_paras, 40)
            || has_repeated_substantial_units(&cleaned_paras, 40)
        {
            return true;
        }

        // 3. Repeated complete sentences (>= 2 repeats of >= 40 chars, or >= 3 consecutive >= 20)
        let mut all_sentences: Vec<(String, bool)> = Vec::new();
        for run in &runs {
            for raw_line in run.lines() {
                let line = raw_line.trim_end_matches('\r');
                if is_markdown_table_row(line) {
                    continue;
                }
                let spans = split_sentence_spans(line);
                for (s_idx, &(st, en)) in spans.iter().enumerate() {
                    let s = &line[st..en];
                    let terminated = s.ends_with(['.', '!', '?']) || s_idx + 1 < spans.len();
                    let key = normalize_prose_key(s);
                    if !key.is_empty() && !is_protected_boilerplate_short_item(s, key.chars().count()) {
                        all_sentences.push((key, terminated));
                    }
                }
            }
        }
        let sentence_keys: Vec<String> = all_sentences.iter().map(|(k, _)| k.clone()).collect();
        if has_consecutive_repeated_keys(&sentence_keys, 3, 20)
            || has_repeated_substantial_units(&all_sentences, 40)
        {
            return true;
        }

        // 4. Contiguous verbatim suffix repeat (catches run-on/same-line loops >= 40 chars immediately)
        if let Some(last_run) = runs.last() {
            if find_contiguous_suffix_repeat(last_run, 40).is_some() {
                return true;
            }
        }

        false
    }
}

fn has_consecutive_repeated_keys(keys: &[String], required_repeats: usize, min_chars: usize) -> bool {
    if keys.len() < required_repeats {
        return false;
    }
    let mut streak = 1usize;
    for i in 1..keys.len() {
        if !keys[i].is_empty() && keys[i] == keys[i - 1] {
            streak += 1;
            if streak >= required_repeats
                && keys[i].chars().count() >= min_chars
                && !is_pure_number_or_divider(&keys[i])
            {
                return true;
            }
        } else {
            streak = 1;
        }
    }
    false
}

fn has_repeated_substantial_units(units: &[(String, bool)], min_chars: usize) -> bool {
    if units.len() < 2 {
        return false;
    }
    let mut seen: HashSet<&str> = HashSet::new();
    for (key, is_complete) in units {
        let chars = key.chars().count();
        if chars >= min_chars && !is_pure_number_or_divider(key) {
            if seen.contains(key.as_str()) {
                return true;
            }
            if *is_complete {
                seen.insert(key.as_str());
            }
        }
    }

    let keys: Vec<&str> = units.iter().map(|(k, _)| k.as_str()).collect();
    for i in 0..keys.len() {
        let max_k = (keys.len() - i) / 2;
        for k in 2..=max_k {
            if keys[i..i + k] == keys[i + k..i + 2 * k] {
                let total_chars: usize = keys[i..i + k].iter().map(|s| s.chars().count()).sum();
                if total_chars >= min_chars {
                    return true;
                }
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn fold_system_messages_merges_system_and_developer_at_zero() {
        let msgs = vec![
            InferenceMessage {
                role: "user".into(),
                content: "u1".into(),
                image_urls: vec!["https://example.com/a.png".into()],
                ..Default::default()
            },
            InferenceMessage {
                role: "system".into(),
                content: "sys1".into(),
                ..Default::default()
            },
            InferenceMessage {
                role: "developer".into(),
                content: "dev1".into(),
                ..Default::default()
            },
            InferenceMessage {
                role: "assistant".into(),
                content: "a1".into(),
                ..Default::default()
            },
        ];

        let folded = fold_system_messages(&msgs);
        assert_eq!(folded.len(), 3);
        assert_eq!(folded[0].role, "system");
        assert_eq!(folded[0].content, "sys1\n\ndev1");
        assert!(folded[0].image_urls.is_empty());
        assert_eq!(folded[1].role, "user");
        assert_eq!(
            folded[1].image_urls,
            vec!["https://example.com/a.png".to_string()]
        );
        assert_eq!(folded[2].role, "assistant");
    }

    #[test]
    fn parse_tier1_tier2_tier3_and_think() {
        // Tier 1
        let t1 = parse_model_output(
            "<think>plan</think>calling tool",
            None,
            vec![ToolCallRequest {
                id: "c1".into(),
                name: "calculator".into(),
                arguments: json!({"expression": "1+1"}),
            }],
        );
        assert_eq!(t1.reasoning.as_deref(), Some("plan"));
        assert_eq!(t1.content, "calling tool");
        assert_eq!(t1.tool_calls.len(), 1);
        assert_eq!(t1.tool_calls[0].name, "calculator");

        // Tier 2 (Qwen XML)
        let t2 = parse_model_output(
            "<think>need file</think>\n<tool_call>\n{\"name\":\"read_file\",\"arguments\":{\"path\":\"a.txt\"}}\n</tool_call>",
            None,
            vec![],
        );
        assert_eq!(t2.reasoning.as_deref(), Some("need file"));
        assert_eq!(t2.content, "");
        assert_eq!(t2.tool_calls.len(), 1);
        assert_eq!(t2.tool_calls[0].name, "read_file");
        assert_eq!(t2.tool_calls[0].arguments["path"], "a.txt");

        // Tier 3 (Safe structured JSON fallback)
        let t3 = parse_model_output(
            r#"```json
{"thought":"checking","tool_calls":[{"name":"datetime","arguments":{"action":"now"}}],"final_answer":""}
```"#,
            None,
            vec![],
        );
        assert_eq!(t3.reasoning.as_deref(), Some("checking"));
        assert_eq!(t3.content, "");
        assert_eq!(t3.tool_calls.len(), 1);
        assert_eq!(t3.tool_calls[0].name, "datetime");

        // Arbitrary JSON without tool_calls/final_answer is NOT hijacked
        let plain = parse_model_output(r#"{"status":"ok","count":3}"#, None, vec![]);
        assert!(plain.tool_calls.is_empty());
        assert_eq!(plain.content, r#"{"status":"ok","count":3}"#);
    }

    #[test]
    fn think_stream_splitter_handles_split_tags() {
        let mut s = ThinkStreamSplitter::new();
        let mut pieces = Vec::new();
        for chunk in ["<thi", "nk>step 1", "</th", "ink>Hello ", "world"] {
            pieces.extend(s.push(chunk));
        }
        pieces.extend(s.finish());
        assert_eq!(
            pieces,
            vec![
                StreamPiece::Reasoning("step 1".into()),
                StreamPiece::Text("Hello ".into()),
                StreamPiece::Text("world".into()),
            ]
        );
    }

    #[test]
    fn think_stream_splitter_and_parser_handle_seed_think_aliases() {
        let mut s = ThinkStreamSplitter::new();
        let mut pieces = Vec::new();
        for chunk in ["<seed:th", "ink>plan step", "</seed:th", "ink>Visible reply"] {
            pieces.extend(s.push(chunk));
        }
        pieces.extend(s.finish());
        assert_eq!(
            pieces,
            vec![
                StreamPiece::Reasoning("plan step".into()),
                StreamPiece::Text("Visible reply".into()),
            ]
        );

        let parsed = parse_model_output(
            "<seed:think>seed draft</seed:think>\nVisible reply",
            None,
            vec![],
        );
        assert_eq!(parsed.reasoning.as_deref(), Some("seed draft"));
        assert_eq!(parsed.content, "Visible reply");
    }

    #[test]
    fn orphan_close_think_puts_draft_html_in_reasoning_and_final_html_in_visible() {
        let draft_html = "```html\n<div>draft prototype</div>\n```";
        let final_html = "```html\n<div>final release</div>\n```";
        let raw = format!("{draft_html}\n</think>\n{final_html}");

        // Non-streaming / final output extraction (e.g. PS5LM prompt prefill "<think>")
        let parsed = parse_model_output(&raw, None, vec![]);
        assert_eq!(parsed.reasoning.as_deref(), Some(draft_html));
        assert_eq!(parsed.content, final_html);

        // Streaming splitter with leading whitespace before orphan </think>
        let mut s = ThinkStreamSplitter::new();
        let mut reasoning = String::new();
        let mut visible = String::new();
        for chunk in ["\n  ", &raw] {
            for piece in s.push(chunk) {
                match piece {
                    StreamPiece::Reasoning(r) => reasoning.push_str(&r),
                    StreamPiece::Text(t) => visible.push_str(&t),
                }
            }
        }
        for piece in s.finish() {
            match piece {
                StreamPiece::Reasoning(r) => reasoning.push_str(&r),
                StreamPiece::Text(t) => visible.push_str(&t),
            }
        }
        assert_eq!(reasoning.trim(), draft_html);
        assert_eq!(visible.trim(), final_html);

        // Orphan </seed:think> alias
        let raw_seed = format!("{draft_html}\n</seed:think>\n{final_html}");
        let parsed_seed = parse_model_output(&raw_seed, None, vec![]);
        assert_eq!(parsed_seed.reasoning.as_deref(), Some(draft_html));
        assert_eq!(parsed_seed.content, final_html);
    }

    #[test]
    fn leading_whitespace_before_think_keeps_reasoning_isolated_in_splitter() {
        // Leading whitespace before `<think>` in separate chunks and before `<seed:think>`
        let mut s = ThinkStreamSplitter::new();
        let mut reasoning = String::new();
        let mut visible = String::new();
        for chunk in [
            "\n\n",
            "   <thi",
            "nk>hidden reasoning</th",
            "ink>\nFinal visible answer",
        ] {
            for piece in s.push(chunk) {
                match piece {
                    StreamPiece::Reasoning(r) => reasoning.push_str(&r),
                    StreamPiece::Text(t) => visible.push_str(&t),
                }
            }
        }
        for piece in s.finish() {
            match piece {
                StreamPiece::Reasoning(r) => reasoning.push_str(&r),
                StreamPiece::Text(t) => visible.push_str(&t),
            }
        }
        assert_eq!(reasoning, "hidden reasoning");
        assert_eq!(visible.trim(), "Final visible answer");

        // Leading whitespace followed by split orphan `</seed:think>` across chunks
        let mut s2 = ThinkStreamSplitter::new();
        let mut r2 = String::new();
        let mut v2 = String::new();
        for chunk in [
            "\n  ",
            "```html\n<p>draft</p>\n```</seed:thi",
            "nk>\n```html\n<p>final</p>\n```",
        ] {
            for piece in s2.push(chunk) {
                match piece {
                    StreamPiece::Reasoning(r) => r2.push_str(&r),
                    StreamPiece::Text(t) => v2.push_str(&t),
                }
            }
        }
        for piece in s2.finish() {
            match piece {
                StreamPiece::Reasoning(r) => r2.push_str(&r),
                StreamPiece::Text(t) => v2.push_str(&t),
            }
        }
        assert_eq!(r2, "```html\n<p>draft</p>\n```");
        assert_eq!(v2.trim(), "```html\n<p>final</p>\n```");

        // Literal `</think>` appearing later after `<think>...</think>` must not pull visible text into reasoning
        let mut s3 = ThinkStreamSplitter::new();
        let mut r3 = String::new();
        let mut v3 = String::new();
        for piece in s3
            .push("<think>inner</think>visible before </think> after")
            .into_iter()
            .chain(s3.finish())
        {
            match piece {
                StreamPiece::Reasoning(r) => r3.push_str(&r),
                StreamPiece::Text(t) => v3.push_str(&t),
            }
        }
        assert_eq!(r3, "inner");
        assert_eq!(v3, "visible before  after");
    }

    #[test]
    fn normalize_conversation_messages_coalesces_consecutive_assistants_and_drops_empty() {
        let msgs = vec![
            InferenceMessage {
                role: "system".into(),
                content: "sys".into(),
                ..Default::default()
            },
            InferenceMessage {
                role: "user".into(),
                content: "u1".into(),
                ..Default::default()
            },
            InferenceMessage {
                role: "assistant".into(),
                content: "a1".into(),
                ..Default::default()
            },
            InferenceMessage {
                role: "assistant".into(),
                content: "   \n  ".into(),
                ..Default::default()
            },
            InferenceMessage {
                role: "assistant".into(),
                content: "a2".into(),
                ..Default::default()
            },
        ];

        let norm = normalize_conversation_messages(&msgs, false);
        assert_eq!(norm.len(), 3);
        assert_eq!(norm[0].role, "system");
        assert_eq!(norm[1].role, "user");
        assert_eq!(norm[1].content, "u1");
        assert_eq!(norm[2].role, "assistant");
        assert_eq!(norm[2].content, "a1\n\na2");
    }

    #[test]
    fn normalize_conversation_messages_coalesces_consecutive_users_with_images_and_drops_leading() {
        let msgs = vec![
            InferenceMessage {
                role: "system".into(),
                content: "sys".into(),
                ..Default::default()
            },
            InferenceMessage {
                role: "assistant".into(),
                content: "stale greeting".into(),
                ..Default::default()
            },
            InferenceMessage {
                role: "tool".into(),
                content: "orphan tool".into(),
                ..Default::default()
            },
            InferenceMessage {
                role: "user".into(),
                content: "u1".into(),
                image_urls: vec!["data:image/png;base64,AAA".into()],
                ..Default::default()
            },
            InferenceMessage {
                role: "assistant".into(),
                content: "".into(),
                ..Default::default()
            },
            InferenceMessage {
                role: "user".into(),
                content: "u2".into(),
                image_urls: vec!["https://example.com/b.png".into()],
                ..Default::default()
            },
            InferenceMessage {
                role: "assistant".into(),
                content: "a1".into(),
                ..Default::default()
            },
        ];

        let norm = normalize_conversation_messages(&msgs, false);
        assert_eq!(norm.len(), 3);
        assert_eq!(norm[0].role, "system");
        assert_eq!(norm[1].role, "user");
        assert_eq!(norm[1].content, "u1\n\nu2");
        assert_eq!(
            norm[1].image_urls,
            vec![
                "data:image/png;base64,AAA".to_string(),
                "https://example.com/b.png".to_string()
            ]
        );
        assert_eq!(norm[2].role, "assistant");
        assert_eq!(norm[2].content, "a1");
    }

    #[test]
    fn normalize_conversation_messages_preserves_tool_call_chains() {
        let tc1 = ToolCallRequest {
            id: "call_1".into(),
            name: "calculator".into(),
            arguments: json!({"expression": "1+1"}),
        };
        let tc2 = ToolCallRequest {
            id: "call_2".into(),
            name: "datetime".into(),
            arguments: json!({"action": "now"}),
        };
        let msgs = vec![
            InferenceMessage {
                role: "user".into(),
                content: "compute".into(),
                ..Default::default()
            },
            InferenceMessage {
                role: "assistant".into(),
                content: "preamble".into(),
                ..Default::default()
            },
            InferenceMessage {
                role: "assistant".into(),
                content: "".into(),
                tool_calls: Some(vec![tc1, tc2]),
                ..Default::default()
            },
            InferenceMessage {
                role: "tool".into(),
                content: "2".into(),
                tool_call_id: Some("call_1".into()),
                ..Default::default()
            },
            InferenceMessage {
                role: "tool".into(),
                content: "2026-11-13".into(),
                tool_call_id: Some("call_2".into()),
                ..Default::default()
            },
            InferenceMessage {
                role: "assistant".into(),
                content: "answer part 1".into(),
                ..Default::default()
            },
            InferenceMessage {
                role: "assistant".into(),
                content: "answer part 2".into(),
                ..Default::default()
            },
        ];

        let norm = normalize_conversation_messages(&msgs, false);
        assert_eq!(norm.len(), 6);
        assert_eq!(norm[0].role, "user");
        assert_eq!(norm[1].role, "assistant");
        assert_eq!(norm[1].content, "preamble");
        assert!(norm[1].tool_calls.is_none());
        assert_eq!(norm[2].role, "assistant");
        assert_eq!(norm[2].tool_calls.as_ref().map(|v| v.len()), Some(2));
        assert_eq!(norm[3].role, "tool");
        assert_eq!(norm[3].tool_call_id.as_deref(), Some("call_1"));
        assert_eq!(norm[4].role, "tool");
        assert_eq!(norm[4].tool_call_id.as_deref(), Some("call_2"));
        assert_eq!(norm[5].role, "assistant");
        assert_eq!(norm[5].content, "answer part 1\n\nanswer part 2");
    }

    #[test]
    fn normalize_conversation_messages_handles_ensure_trailing_user() {
        let msgs = vec![
            InferenceMessage {
                role: "system".into(),
                content: "sys".into(),
                ..Default::default()
            },
            InferenceMessage {
                role: "user".into(),
                content: "u1".into(),
                ..Default::default()
            },
            InferenceMessage {
                role: "assistant".into(),
                content: "a1".into(),
                ..Default::default()
            },
            InferenceMessage {
                role: "assistant".into(),
                content: "a2".into(),
                ..Default::default()
            },
        ];

        let without_trailing = normalize_conversation_messages(&msgs, false);
        assert_eq!(without_trailing.len(), 3);
        assert_eq!(without_trailing.last().unwrap().role, "assistant");
        assert_eq!(without_trailing.last().unwrap().content, "a1\n\na2");

        let with_trailing = normalize_conversation_messages(&msgs, true);
        assert_eq!(with_trailing.len(), 4);
        assert_eq!(with_trailing[2].role, "assistant");
        assert_eq!(with_trailing[2].content, "a1\n\na2");
        assert_eq!(with_trailing[3].role, "user");

        // Already ending in user does not append a duplicate trailing user
        let already_user = normalize_conversation_messages(&with_trailing, true);
        assert_eq!(already_user.len(), 4);
        assert_eq!(already_user[3].role, "user");
        assert!(normalize_conversation_messages(&[], true).is_empty());
    }

    #[test]
    fn think_stream_splitter_with_initial_in_think_streams_prefilled_reasoning() {
        let mut s = ThinkStreamSplitter::with_initial_in_think(true);
        let mut reasoning = String::new();
        let mut visible = String::new();
        for chunk in ["Let", " me think", " łódź", "</", "think>Answer"] {
            for piece in s.push(chunk) {
                match piece {
                    StreamPiece::Reasoning(r) => reasoning.push_str(&r),
                    StreamPiece::Text(t) => visible.push_str(&t),
                }
            }
        }
        for piece in s.finish() {
            match piece {
                StreamPiece::Reasoning(r) => reasoning.push_str(&r),
                StreamPiece::Text(t) => visible.push_str(&t),
            }
        }
        assert_eq!(reasoning, "Let me think łódź");
        assert_eq!(visible, "Answer");
    }

    #[test]
    fn extract_and_split_require_matching_think_and_seed_think_tags() {
        let input = "<seed:think>alpha </think> beta</seed:think>done";
        let parsed = parse_model_output(input, None, vec![]);
        assert_eq!(parsed.reasoning.as_deref(), Some("alpha </think> beta"));
        assert_eq!(parsed.content, "done");

        let mut s = ThinkStreamSplitter::new();
        let mut reasoning = String::new();
        let mut visible = String::new();
        for chunk in ["<seed:think>alpha ", "</think> beta</seed:", "think>done"] {
            for piece in s.push(chunk) {
                match piece {
                    StreamPiece::Reasoning(r) => reasoning.push_str(&r),
                    StreamPiece::Text(t) => visible.push_str(&t),
                }
            }
        }
        for piece in s.finish() {
            match piece {
                StreamPiece::Reasoning(r) => reasoning.push_str(&r),
                StreamPiece::Text(t) => visible.push_str(&t),
            }
        }
        assert_eq!(reasoning, "alpha </think> beta");
        assert_eq!(visible, "done");
    }

    #[test]
    fn extract_think_blocks_matches_splitter_on_unclosed_think() {
        let raw = "Hello <think>unfinished reasoning łódź";
        let parsed = parse_model_output(raw, None, vec![]);
        assert_eq!(parsed.content, "Hello");
        assert_eq!(parsed.reasoning.as_deref(), Some("unfinished reasoning łódź"));

        let mut s = ThinkStreamSplitter::new();
        let mut reasoning = String::new();
        let mut visible = String::new();
        for piece in s.push(raw).into_iter().chain(s.finish()) {
            match piece {
                StreamPiece::Reasoning(r) => reasoning.push_str(&r),
                StreamPiece::Text(t) => visible.push_str(&t),
            }
        }
        assert_eq!(visible.trim(), "Hello");
        assert_eq!(reasoning.trim(), "unfinished reasoning łódź");
    }

    #[test]
    fn normalize_conversation_messages_drops_leading_assistant_with_tool_calls() {
        let tc = ToolCallRequest {
            id: "call_0".into(),
            name: "read_file".into(),
            arguments: json!({"path": "x.txt"}),
        };
        let msgs = vec![
            InferenceMessage {
                role: "system".into(),
                content: "sys".into(),
                ..Default::default()
            },
            InferenceMessage {
                role: "assistant".into(),
                content: "".into(),
                tool_calls: Some(vec![tc]),
                ..Default::default()
            },
            InferenceMessage {
                role: "tool".into(),
                content: "file contents".into(),
                tool_call_id: Some("call_0".into()),
                ..Default::default()
            },
            InferenceMessage {
                role: "user".into(),
                content: "first real user".into(),
                ..Default::default()
            },
        ];

        let norm = normalize_conversation_messages(&msgs, false);
        assert_eq!(norm.len(), 2);
        assert_eq!(norm[0].role, "system");
        assert_eq!(norm[1].role, "user");
        assert_eq!(norm[1].content, "first real user");
    }

    #[test]
    fn normalize_conversation_messages_strips_non_tool_names_and_preserves_tool_name() {
        let tc = ToolCallRequest {
            id: "call_1".into(),
            name: "read_file".into(),
            arguments: json!({"path": "src/lib.rs"}),
        };
        let msgs = vec![
            InferenceMessage {
                role: "user".into(),
                content: "u1".into(),
                name: Some("commander".into()),
                ..Default::default()
            },
            InferenceMessage {
                role: "user".into(),
                content: "u2".into(),
                name: Some("commander".into()),
                ..Default::default()
            },
            InferenceMessage {
                role: "assistant".into(),
                content: "Checking now.".into(),
                name: Some("commander".into()),
                tool_calls: Some(vec![tc]),
                ..Default::default()
            },
            InferenceMessage {
                role: "tool".into(),
                content: "ok".into(),
                tool_call_id: Some("call_1".into()),
                name: Some("read_file".into()),
                ..Default::default()
            },
            InferenceMessage {
                role: "assistant".into(),
                content: "part 1".into(),
                name: Some("commander".into()),
                ..Default::default()
            },
            InferenceMessage {
                role: "assistant".into(),
                content: "part 2".into(),
                name: Some("analyst".into()),
                ..Default::default()
            },
        ];

        let norm = normalize_conversation_messages(&msgs, false);
        assert_eq!(norm.len(), 4);
        assert_eq!(norm[0].role, "user");
        assert!(norm[0].name.is_none());
        assert_eq!(norm[1].role, "assistant");
        assert!(norm[1].name.is_none());
        assert_eq!(norm[2].role, "tool");
        assert_eq!(norm[2].name.as_deref(), Some("read_file"));
        assert_eq!(norm[3].role, "assistant");
        assert!(norm[3].name.is_none());
        assert_eq!(norm[3].content, "part 1\n\npart 2");
    }

    #[test]
    fn truncate_verbatim_prose_loops_collapses_doing_it_again_commander_loop() {
        let body = "I checked crates/ps5-agent-inference/src/parser.rs and fixed the normalization path.";
        let looped_all = format!(
            "Got it, Commander! Doing it again:\n\n{body}\n\n\
             Got it, Commander! Doing it again:\n\n{body}\n\n\
             Got it, Commander! Doing it again:\n\n{body}"
        );
        assert_eq!(truncate_verbatim_prose_loops(&looped_all), body);
        assert_eq!(deduplicate_repeated_prose_blocks(&looped_all), body);

        // Also test via parse_model_output with leading <think>
        let with_think = format!("<think>checking</think>\n{looped_all}");
        let parsed = parse_model_output(&with_think, None, vec![]);
        assert_eq!(parsed.reasoning.as_deref(), Some("checking"));
        assert_eq!(parsed.content, body);

        // First copy clean, subsequent loops prefixed by leaked header
        let looped_tail = format!(
            "{body}\n\nGot it, Commander! Doing it again:\n\n{body}\n\n\
             Got it, Commander! Doing it again:\n\n{body}"
        );
        assert_eq!(truncate_verbatim_prose_loops(&looped_tail), body);
    }

    #[test]
    fn truncate_verbatim_prose_loops_collapses_hearing_you_commander_loop() {
        let spaced = "Got it, Commander! Hearing you:\nHey there! How can I help you today?\n\n\
             Got it, Commander! Hearing you:\nHey there! How can I help you today?\n\n\
             Got it, Commander! Hearing you:\nHey there! How can I help you today?";
        assert_eq!(
            truncate_verbatim_prose_loops(spaced),
            "Hey there! How can I help you today?"
        );

        let tight = "Got it, Commander! Hearing you:\nHey there! How can I help you today?\n\
             Got it, Commander! Hearing you:\nHey there! How can I help you today?\n\
             Got it, Commander! Hearing you:\nHey there! How can I help you today?";
        assert_eq!(
            truncate_verbatim_prose_loops(tight),
            "Hey there! How can I help you today?"
        );
    }

    #[test]
    fn truncate_verbatim_prose_loops_preserves_fenced_code_think_tool_call_and_short_items() {
        let code_and_tags = "<think>step one\nstep one\nstep one</think>\n\n\
Here are the assertions:\n\n\
```rust\n\
assert_eq!(tick(), 1);\n\
assert_eq!(tick(), 1);\n\
assert_eq!(tick(), 1);\n\
assert_eq!(tick(), 1);\n\
```\n\n\
~~~python\n\
print(\"ping\")\n\
print(\"ping\")\n\
print(\"ping\")\n\
~~~\n\n\
- OK\n\
- OK\n\n\
<tool_call>\n{\"name\":\"echo\",\"arguments\":{\"msg\":\"repeat repeat\"}}\n</tool_call>";
        assert_eq!(truncate_verbatim_prose_loops(code_and_tags), code_and_tags);

        // Normal conversational greetings and numeric variants must stay intact
        let normal_conv = "Got it! Here is your script.\n\n\
Got it, thanks!\n\n\
Option A listens on port 8080 with local HTTP proxy enabled and caches requests safely.\n\n\
Option A listens on port 9090 with local HTTP proxy enabled and caches requests safely.";
        assert_eq!(truncate_verbatim_prose_loops(normal_conv), normal_conv);

        // Standalone greeting without body must not erase into emptiness
        assert_eq!(
            truncate_verbatim_prose_loops("Got it, Commander!"),
            "Got it, Commander!"
        );
    }

    #[test]
    fn truncate_verbatim_prose_loops_handles_repeated_lines_and_polish_utf8() {
        let polish_lines = "Sprawdzam konfigurację serwera w mieście łódź!\n\
                            Sprawdzam konfigurację serwera w mieście łódź!\n\
                            Sprawdzam konfigurację serwera w mieście łódź!";
        assert_eq!(
            truncate_verbatim_prose_loops(polish_lines),
            "Sprawdzam konfigurację serwera w mieście łódź!"
        );

        // Two short 22-char lines without spurious header stay intact (< 3 lines and < 40 chars)
        let two_short = "Krótka linia testowa 1\nKrótka linia testowa 1";
        assert_eq!(truncate_verbatim_prose_loops(two_short), two_short);

        // Repeated substantial block wrapped around a fenced code block cleans the duplicate prose
        let mixed = "Got it, Commander! Doing it again:\n\nUse this config:\n\n\
```toml\nline = 1\nline = 1\nline = 1\n```\n\n\
Got it, Commander! Doing it again:\n\nUse this config:";
        let expected = "Use this config:\n\n```toml\nline = 1\nline = 1\nline = 1\n```";
        assert_eq!(truncate_verbatim_prose_loops(mixed), expected);
    }

    #[test]
    fn streaming_repetition_guard_triggers_incrementally_and_skips_code_fences() {
        // 1. Incremental trigger across small chunks with <think> and fenced code block intact
        let stream_payload = concat!(
            "<thi",
            "nk>hidden loop phrase hidden loop phrase\n",
            "hidden loop phrase hidden loop phrase</th",
            "ink>",
            "Here is the working handler:\n\n",
            "```ru",
            "st\n",
            "tick(); // repeated line inside code fence >= 40 chars here\n",
            "tick(); // repeated line inside code fence >= 40 chars here\n",
            "tick(); // repeated line inside code fence >= 40 chars here\n",
            "```\n\n",
            "Got it, Commander! Doing it again:\n\n",
            "The diagnostic summary for PS5LM is ready with Polish łódź verification.\n\n",
            "Got it, Commander! Doing it again:\n\n",
            "The diagnostic summary for PS5LM is ready with Polish łódź verification."
        );

        let mut guard = StreamingRepetitionGuard::new();
        let mut aborted: Option<String> = None;
        for ch in stream_payload.chars() {
            let mut buf = [0u8; 4];
            let s = ch.encode_utf8(&mut buf);
            if let StreamingLoopDecision::AbortRepeatedLoop {
                truncated_clean_visible,
            } = guard.push_delta(s)
            {
                aborted = Some(truncated_clean_visible);
                break;
            }
        }
        let cleaned = aborted.expect("repetition guard should abort on repeated visible block");
        assert!(!cleaned.contains("<think>"));
        assert!(!cleaned.contains("hidden loop phrase"));
        assert!(cleaned.contains("```rust\ntick();"));
        assert_eq!(
            cleaned.matches("The diagnostic summary for PS5LM is ready with Polish łódź verification.").count(),
            1
        );

        // 2. Repeated lines inside a fenced code block ONLY must never abort
        let mut fence_guard = StreamingRepetitionGuard::new();
        let code_only = "Summary:\n\n```rust\n\
            println!(\"This repeated code line is well over forty characters long\");\n\
            println!(\"This repeated code line is well over forty characters long\");\n\
            println!(\"This repeated code line is well over forty characters long\");\n\
            println!(\"This repeated code line is well over forty characters long\");\n\
            ```\nDone.";
        for ch in code_only.chars() {
            let mut buf = [0u8; 4];
            assert_eq!(
                fence_guard.push_delta(ch.encode_utf8(&mut buf)),
                StreamingLoopDecision::Continue
            );
        }
        assert_eq!(fence_guard.finish(), StreamingLoopDecision::Continue);

        // 3. 3 consecutive repeated visible lines >= 20 chars triggers, while 2 does not
        let mut line_guard = StreamingRepetitionGuard::new();
        assert_eq!(
            line_guard.push_delta("Checking status line A\nChecking status line A\n"),
            StreamingLoopDecision::Continue
        );
        match line_guard.push_delta("Checking status line A\n") {
            StreamingLoopDecision::AbortRepeatedLoop {
                truncated_clean_visible,
            } => {
                assert_eq!(truncated_clean_visible, "Checking status line A");
            }
            StreamingLoopDecision::Continue => panic!("expected 3rd repeated line to abort"),
        }

        // 4. <tool_call> and <seed:think> across split chunks stay untouched and do not false-trigger
        let mut tool_guard = StreamingRepetitionGuard::new();
        let tool_stream = [
            "<seed:th",
            "ink>plan repeat plan repeat\nplan repeat plan repeat</seed:th",
            "ink><tool_",
            "call>{\"name\":\"read_file\",\"arguments\":{\"path\":\"a.txt\"}}</tool_call>",
            "Checking now.",
        ];
        for chunk in tool_stream {
            assert_eq!(tool_guard.push_delta(chunk), StreamingLoopDecision::Continue);
        }
        assert_eq!(tool_guard.finish(), StreamingLoopDecision::Continue);
        assert_eq!(
            tool_guard.visible_so_far(),
            "<tool_call>{\"name\":\"read_file\",\"arguments\":{\"path\":\"a.txt\"}}</tool_call>Checking now."
        );
    }

    #[test]
    fn truncate_verbatim_prose_loops_handles_cycles_inline_and_near_identical() {
        // Inline leaked header after sentence punctuation
        let inline = "Got it, Commander! Doing it again: The system check succeeded. \
                      Got it, Commander! Doing it again: The system check succeeded.";
        assert_eq!(
            truncate_verbatim_prose_loops(inline),
            "The system check succeeded."
        );

        // Two-paragraph A-B-A-B cycle
        let abab = "Paragraph Alpha explains the first requirement cleanly.\n\n\
                    Paragraph Beta explains the second requirement cleanly.\n\n\
                    Paragraph Alpha explains the first requirement cleanly.\n\n\
                    Paragraph Beta explains the second requirement cleanly.";
        assert_eq!(
            truncate_verbatim_prose_loops(abab),
            "Paragraph Alpha explains the first requirement cleanly.\n\n\
             Paragraph Beta explains the second requirement cleanly."
        );

        // Near-identical multi-sentence paragraph (>= 12 words, >= 80 chars, same numbers & opening)
        let p1 = "The verification suite inspected all twelve modules across the workspace and confirmed every check passed cleanly today.";
        let p2 = "The verification suite inspected all twelve modules across the workspace and confirmed each check passed cleanly now.";
        let near = format!("{p1}\n\n{p2}");
        assert_eq!(truncate_verbatim_prose_loops(&near), p1);
    }
}
