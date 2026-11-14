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
    if dst.name.is_none() {
        dst.name = src.name;
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
        return ParsedModelOutput {
            content: without_think.trim().to_string(),
            reasoning: join_non_empty(reasoning_parts),
            tool_calls: api_tool_calls,
        };
    }

    // Tier 2: Qwen <tool_call>...</tool_call> XML blocks
    let (without_xml_tools, xml_calls) = extract_qwen_tool_calls(&without_think);
    if !xml_calls.is_empty() {
        return ParsedModelOutput {
            content: without_xml_tools.trim().to_string(),
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
        return ParsedModelOutput {
            content: final_answer.trim().to_string(),
            reasoning: join_non_empty(reasoning_parts),
            tool_calls: json_calls,
        };
    }

    ParsedModelOutput {
        content: without_xml_tools.trim().to_string(),
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
}
