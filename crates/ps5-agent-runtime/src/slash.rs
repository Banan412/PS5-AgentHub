use ps5_agent_config::AppConfig;
use ps5_agent_core::{
    agent_allows_tool_with_policy, canonical_tool_id, expand_tool_group, new_id, now_iso,
    AgentDefinition, AgentError, BrainFileSummary, ChatMessage, ChatRole, Conversation,
    ConversationMode, DoctorFixReport, DoctorReport, DoctorStatus, InferenceMessage,
    InferenceProvider, InferenceRequest, MemoryItem, MemoryScope, Result, SandboxApprovalPolicy,
    SandboxMode, SlashCommandResponse, StorageBackend, ThinkingLevel, ToolSpec,
};
use serde_json::{json, Map, Value};
use std::collections::HashSet;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::RwLock;
use tokio_util::sync::CancellationToken;

use crate::brain::{build_brain_prompt_section, list_brain_files};
use crate::doctor::{run_doctor_checks, run_doctor_fixes};
use crate::traits::RuntimeToolExecutor;

const MAX_WORKFLOW_BYTES: usize = 512 * 1024;
const MAX_WORKFLOW_NAME_LEN: usize = 96;
const MAX_BTW_HISTORY_MESSAGES: usize = 10;
const MAX_BTW_MSG_CHARS: usize = 320;
const MAX_BTW_CONTEXT_CHARS: usize = 3200;

const STANDARD_TOOL_GROUPS: &[&str] = &[
    "group:fs",
    "group:web",
    "group:runtime",
    "group:memory",
    "group:sessions",
    "group:automation",
    "group:openclaw",
];

/// Built-in slash command definitions `(usage, summary)`.
pub const BUILTIN_SLASH_COMMANDS: &[(&str, &str)] = &[
    (
        "/btw <question>",
        "Ask a standalone ephemeral side-question without writing chat, tasks, or memory.",
    ),
    (
        "/status",
        "Inspect active agent, model, PS5LM endpoint, sandbox policy, tasks, and memories.",
    ),
    (
        "/context",
        "Inspect estimated prompt token/char footprint, brain files, and memory items count.",
    ),
    (
        "/tools [agent]",
        "List effective enabled tools, denied tools, and expanded group:* sets.",
    ),
    (
        "/compact [keep_recent]",
        "Summarize older conversation messages into rolling memory and archive old messages.",
    ),
    (
        "/think [off|minimal|low|medium|high|auto]",
        "Report or set the preferred reasoning/thinking level.",
    ),
    (
        "/model [name]",
        "Report the active model or apply a validated model override.",
    ),
    (
        "/sandbox [read_only|workspace_write|full_sandbox|full_auto|on_request|always_ask|never]",
        "Inspect or update sandbox mode and tool approval policy.",
    ),
    (
        "/doctor [fix]",
        "Run 12 diagnostic system checks and optionally apply safe automatic fixes.",
    ),
    (
        "/new [title]",
        "Create a fresh conversation and return new_conversation_id in metadata.",
    ),
    (
        "/help",
        "List built-in slash commands and workspace .agents/workflows/*.md commands.",
    ),
];

/// Parses a user input string into `(lowercase_command, trimmed_args)` if it begins with `/`.
pub fn parse_slash_command(input: &str) -> Option<(String, String)> {
    let trimmed = input.trim();
    let rest = trimmed.strip_prefix('/')?;
    if rest.is_empty() || rest.starts_with('/') {
        return None;
    }

    let (raw_cmd, raw_args) = match rest.split_once(char::is_whitespace) {
        Some((cmd, args)) => (cmd, args),
        None => (rest, ""),
    };

    if raw_cmd.is_empty() {
        return None;
    }

    Some((raw_cmd.to_ascii_lowercase(), raw_args.trim().to_string()))
}

/// Executes a built-in slash command or custom `.agents/workflows/<cmd>.md` workflow.
#[allow(clippy::too_many_arguments)]
pub async fn execute_slash_command(
    input: &str,
    conversation_id: Option<&str>,
    agent_id: Option<&str>,
    config: &RwLock<AppConfig>,
    storage: &dyn StorageBackend,
    inference: &dyn InferenceProvider,
    tool_executor: &dyn RuntimeToolExecutor,
    cancel: CancellationToken,
) -> Result<SlashCommandResponse> {
    let (cmd, args) = parse_slash_command(input).ok_or_else(|| {
        AgentError::InvalidInput("input does not start with a valid slash command".to_string())
    })?;

    let normalized_agent_id = normalize_agent_id(agent_id);

    match cmd.as_str() {
        "btw" => {
            cmd_btw(
                &args,
                conversation_id,
                &normalized_agent_id,
                config,
                storage,
                inference,
                cancel,
            )
            .await
        }
        "status" => cmd_status(&normalized_agent_id, config, storage),
        "context" => cmd_context(
            conversation_id,
            &normalized_agent_id,
            config,
            storage,
            tool_executor,
        ),
        "tools" => {
            cmd_tools(
                &args,
                &normalized_agent_id,
                storage,
                tool_executor,
            )
            .await
        }
        "compact" => {
            cmd_compact(
                &args,
                conversation_id,
                &normalized_agent_id,
                config,
                storage,
                inference,
                cancel,
            )
            .await
        }
        "think" => cmd_think(&args, &normalized_agent_id, config, storage),
        "model" => cmd_model(&args, &normalized_agent_id, config, storage, inference).await,
        "sandbox" => cmd_sandbox(&args, &normalized_agent_id, config),
        "doctor" => {
            cmd_doctor(
                &args,
                &normalized_agent_id,
                config,
                storage,
                inference,
                tool_executor,
            )
            .await
        }
        "new" => cmd_new(&args, &normalized_agent_id, storage),
        "help" => cmd_help(&normalized_agent_id, config),
        _ => {
            execute_custom_workflow(
                &cmd,
                &args,
                conversation_id,
                &normalized_agent_id,
                config,
                storage,
                inference,
                cancel,
            )
            .await
        }
    }
}

fn snapshot_config(config: &RwLock<AppConfig>) -> AppConfig {
    config
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

fn update_config<F, T>(config: &RwLock<AppConfig>, f: F) -> T
where
    F: FnOnce(&mut AppConfig) -> T,
{
    let mut guard = config
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    f(&mut guard)
}

fn normalize_agent_id(agent_id: Option<&str>) -> String {
    match agent_id {
        Some(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                "commander".to_string()
            } else {
                trimmed.to_string()
            }
        }
        None => "commander".to_string(),
    }
}

fn resolve_workspace_root(cfg: &AppConfig) -> PathBuf {
    cfg.tools
        .allowed_paths
        .first()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| cfg.data_dir.join("workspace"))
}

fn truncate_chars(text: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }
    let mut cutoff = None;
    let mut count = 0usize;
    for (idx, _) in text.char_indices() {
        if count == max_chars {
            cutoff = Some(idx);
            break;
        }
        count += 1;
    }
    match cutoff {
        Some(end) => format!("{}...", &text[..end]),
        None => text.to_string(),
    }
}

fn estimate_tokens(chars: usize) -> usize {
    if chars == 0 {
        0
    } else {
        (chars + 3) / 4
    }
}

fn effective_model_name(cfg: &AppConfig, agent: Option<&AgentDefinition>) -> Option<String> {
    if let Some(a) = agent {
        if let Some(override_name) = a.model_config.model_override.as_ref() {
            let trimmed = override_name.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    let global = cfg.inference.model.trim();
    if global.is_empty() {
        None
    } else {
        Some(global.to_string())
    }
}

fn parse_config_thinking_level(raw: &str) -> ThinkingLevel {
    raw.trim().parse::<ThinkingLevel>().unwrap_or_default()
}

fn format_role_label(role: ChatRole) -> &'static str {
    match role {
        ChatRole::User => "user",
        ChatRole::Assistant => "assistant",
        ChatRole::System => "system",
        ChatRole::Tool => "tool",
    }
}

fn build_read_only_context_summary(messages: &[ChatMessage]) -> (String, usize) {
    if messages.is_empty() {
        return (String::new(), 0);
    }
    let start = messages.len().saturating_sub(MAX_BTW_HISTORY_MESSAGES);
    let slice = &messages[start..];
    let mut lines = Vec::new();
    let mut total_chars = 0usize;
    let mut included = 0usize;

    for msg in slice {
        let single_line = msg
            .content
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        if single_line.is_empty() {
            continue;
        }
        let body = truncate_chars(&single_line, MAX_BTW_MSG_CHARS);
        let entry = format!("- [{}] {}", format_role_label(msg.role), body);
        if total_chars + entry.len() > MAX_BTW_CONTEXT_CHARS {
            break;
        }
        total_chars += entry.len() + 1;
        lines.push(entry);
        included += 1;
    }

    (lines.join("\n"), included)
}

async fn cmd_btw(
    question: &str,
    conversation_id: Option<&str>,
    agent_id: &str,
    config: &RwLock<AppConfig>,
    storage: &dyn StorageBackend,
    inference: &dyn InferenceProvider,
    cancel: CancellationToken,
) -> Result<SlashCommandResponse> {
    let trimmed_question = question.trim();
    if trimmed_question.is_empty() {
        return Ok(SlashCommandResponse {
            command: "btw".to_string(),
            ephemeral: true,
            content: "Usage: `/btw <question>`\n\nAsks a standalone side-question with read-only conversation context without saving chat messages, tasks, or memory items.".to_string(),
            reasoning: None,
            agent_id: Some(agent_id.to_string()),
            metadata: json!({
                "ephemeral": true,
                "usage": true,
                "persisted": false,
            }),
        });
    }

    let cfg = snapshot_config(config);
    let agent = storage.get_agent(agent_id)?;
    let (context_summary, context_messages_used) = match conversation_id {
        Some(cid) if !cid.trim().is_empty() => {
            let recent = storage.list_chat_messages(cid.trim(), false, 32)?;
            build_read_only_context_summary(&recent)
        }
        _ => (String::new(), 0),
    };

    let system_prompt = if context_summary.is_empty() {
        "You are answering a standalone ephemeral `/btw` side-question. Answer concisely and directly without claiming to modify workspace files or persistent state.".to_string()
    } else {
        format!(
            "You are answering a standalone ephemeral `/btw` side-question. Use this read-only excerpt of recent conversation turns for context when relevant:\n\n{}\n\nAnswer concisely without triggering side effects.",
            context_summary
        )
    };

    let model = effective_model_name(&cfg, agent.as_ref());
    let api_mode = agent
        .as_ref()
        .map(|a| a.model_config.api_mode)
        .unwrap_or(cfg.inference.api_mode);
    let temperature = agent
        .as_ref()
        .map(|a| a.model_config.temperature)
        .unwrap_or(cfg.inference.temperature);
    let max_output_tokens = agent
        .as_ref()
        .map(|a| a.model_config.max_output_tokens)
        .unwrap_or(cfg.inference.max_output_tokens);
    let context_limit = agent
        .as_ref()
        .map(|a| a.model_config.context_limit)
        .unwrap_or(cfg.inference.context_limit);
    let thinking_level = if cfg.inference.thinking_level.trim().is_empty() {
        agent
            .as_ref()
            .map(|a| a.model_config.thinking_level)
            .unwrap_or_default()
    } else {
        parse_config_thinking_level(&cfg.inference.thinking_level)
    };

    let req = InferenceRequest {
        model,
        api_mode,
        messages: vec![
            InferenceMessage {
                role: "system".to_string(),
                content: system_prompt,
                tool_call_id: None,
                name: None,
                tool_calls: None,
                image_urls: Vec::new(),
            },
            InferenceMessage {
                role: "user".to_string(),
                content: trimmed_question.to_string(),
                tool_call_id: None,
                name: None,
                tool_calls: None,
                image_urls: Vec::new(),
            },
        ],
        tools: Vec::new(),
        temperature,
        max_output_tokens,
        context_limit,
        stream: false,
        thinking_level,
    };

    let resp = inference.generate(req, cancel).await?;

    Ok(SlashCommandResponse {
        command: "btw".to_string(),
        ephemeral: true,
        content: resp.content,
        reasoning: resp.reasoning,
        agent_id: Some(agent_id.to_string()),
        metadata: json!({
            "ephemeral": true,
            "persisted": false,
            "model": resp.model,
            "context_messages_used": context_messages_used,
            "usage": {
                "prompt_tokens": resp.usage.prompt_tokens,
                "completion_tokens": resp.usage.completion_tokens,
                "total_tokens": resp.usage.total_tokens,
            },
            "latency_ms": resp.latency_ms,
        }),
    })
}

fn cmd_status(
    agent_id: &str,
    config: &RwLock<AppConfig>,
    storage: &dyn StorageBackend,
) -> Result<SlashCommandResponse> {
    let cfg = snapshot_config(config);
    let agent = storage.get_agent(agent_id)?;
    let model = effective_model_name(&cfg, agent.as_ref())
        .unwrap_or_else(|| "auto (server default)".to_string());

    let counts = storage.task_counts_summary()?;
    let active_task_count = counts.pending + counts.running + counts.waiting;
    let total_memories = storage.query_memories(None, None, None, false, 10_000)?.len();
    let (agent_memory_count, agent_memory_bytes) = storage.memory_stats_for_agent(agent_id)?;

    let agent_display = agent
        .as_ref()
        .map(|a| format!("{} (`{}`)", a.name, a.id))
        .unwrap_or_else(|| format!("`{agent_id}`"));
    let workspace_root = resolve_workspace_root(&cfg);

    let content = format!(
        "### Runtime Status\n\
         - **Active Agent:** {}\n\
         - **Model:** `{}` (thinking: `{}`)\n\
         - **PS5LM URL:** `{}`\n\
         - **Sandbox:** mode `{}`, approval policy `{}` (network: {}, local network: {})\n\
         - **Workspace:** `{}`\n\
         - **Tasks:** {} active ({} pending, {} running, {} waiting; {} total / {} completed / {} failed / {} cancelled)\n\
         - **Memories:** {} active workspace memories ({} owned by `{}` / {} bytes)",
        agent_display,
        model,
        cfg.inference.thinking_level,
        cfg.inference.base_url,
        cfg.sandbox.mode,
        cfg.sandbox.approval_policy,
        cfg.sandbox.network_access,
        cfg.sandbox.allow_local_network,
        workspace_root.display(),
        active_task_count,
        counts.pending,
        counts.running,
        counts.waiting,
        counts.total,
        counts.completed,
        counts.failed,
        counts.cancelled,
        total_memories,
        agent_memory_count,
        agent_id,
        agent_memory_bytes,
    );

    Ok(SlashCommandResponse {
        command: "status".to_string(),
        ephemeral: true,
        content,
        reasoning: None,
        agent_id: Some(agent_id.to_string()),
        metadata: json!({
            "active_agent_id": agent_id,
            "model": model,
            "thinking_level": cfg.inference.thinking_level,
            "ps5lm_url": cfg.inference.base_url,
            "sandbox": {
                "mode": cfg.sandbox.mode,
                "approval_policy": cfg.sandbox.approval_policy,
                "network_access": cfg.sandbox.network_access,
                "allow_local_network": cfg.sandbox.allow_local_network,
                "allow_host_shell": cfg.sandbox.allow_host_shell,
            },
            "tasks": {
                "active": active_task_count,
                "pending": counts.pending,
                "running": counts.running,
                "waiting": counts.waiting,
                "completed": counts.completed,
                "failed": counts.failed,
                "cancelled": counts.cancelled,
                "total": counts.total,
            },
            "memory_count": total_memories,
            "agent_memory_count": agent_memory_count,
            "agent_memory_bytes": agent_memory_bytes,
        }),
    })
}

fn cmd_context(
    conversation_id: Option<&str>,
    agent_id: &str,
    config: &RwLock<AppConfig>,
    storage: &dyn StorageBackend,
    tool_executor: &dyn RuntimeToolExecutor,
) -> Result<SlashCommandResponse> {
    let cfg = snapshot_config(config);
    let workspace_root = resolve_workspace_root(&cfg);
    let agent = storage.get_agent(agent_id)?;

    let system_prompt_chars = agent.as_ref().map(|a| a.system_prompt.len()).unwrap_or(0);

    let brain_files = list_brain_files(&workspace_root)?;
    let existing_brain_files: Vec<&BrainFileSummary> =
        brain_files.iter().filter(|f| f.exists).collect();
    let brain_prompt_section = if cfg.sandbox.inject_brain_files {
        build_brain_prompt_section(&workspace_root, cfg.sandbox.max_brain_context_chars, true)
    } else {
        String::new()
    };
    let brain_chars = brain_prompt_section.len();

    let (skill_pairs, plugin_pairs) = tool_executor.extension_prompt_context();
    let extension_chars: usize = skill_pairs
        .iter()
        .chain(plugin_pairs.iter())
        .map(|(k, v)| k.len() + v.len() + 6)
        .sum();

    let memories = storage.query_memories(None, None, None, false, 10_000)?;
    let memory_count = memories.len();
    let raw_memory_chars: usize = memories.iter().map(|m| m.key.len() + m.content.len() + 8).sum();
    let effective_memory_chars = if cfg.memory.max_memory_context_chars > 0 {
        raw_memory_chars.min(cfg.memory.max_memory_context_chars)
    } else {
        raw_memory_chars
    };

    let (chat_message_count, chat_chars) = match conversation_id {
        Some(cid) if !cid.trim().is_empty() => {
            let msgs = storage.list_chat_messages(cid.trim(), false, 500)?;
            let chars: usize = msgs.iter().map(|m| m.content.len() + 16).sum();
            (msgs.len(), chars)
        }
        _ => (0, 0),
    };

    let total_chars =
        system_prompt_chars + brain_chars + extension_chars + effective_memory_chars + chat_chars;
    let total_tokens = estimate_tokens(total_chars);
    let context_limit = cfg.inference.context_limit;
    let utilization_pct = if context_limit > 0 {
        (total_tokens as f64 / context_limit as f64) * 100.0
    } else {
        0.0
    };

    let brain_list_md = if existing_brain_files.is_empty() {
        "none present".to_string()
    } else {
        existing_brain_files
            .iter()
            .map(|f| format!("`{}` ({} B)", f.path, f.size_bytes))
            .collect::<Vec<_>>()
            .join(", ")
    };

    let limit_label = if context_limit == 0 {
        "unlimited".to_string()
    } else {
        format!("{context_limit} tokens ({utilization_pct:.1}%)")
    };

    let content = format!(
        "### Context Footprint\n\
         - **Estimated Total:** ~{} tokens ({} chars) / limit {}\n\
         - **System Prompt (`{}`):** {} chars (~{} tokens)\n\
         - **Brain Injection:** {} chars (~{} tokens, enabled: {})\n\
         - **Active Brain Files ({}):** {}\n\
         - **Extensions (Skills/Plugins):** {} chars (~{} tokens)\n\
         - **Memories:** {} items (capped prompt contribution {} chars / ~{} tokens)\n\
         - **Conversation Messages:** {} active messages ({} chars / ~{} tokens)",
        total_tokens,
        total_chars,
        limit_label,
        agent_id,
        system_prompt_chars,
        estimate_tokens(system_prompt_chars),
        brain_chars,
        estimate_tokens(brain_chars),
        cfg.sandbox.inject_brain_files,
        existing_brain_files.len(),
        brain_list_md,
        extension_chars,
        estimate_tokens(extension_chars),
        memory_count,
        effective_memory_chars,
        estimate_tokens(effective_memory_chars),
        chat_message_count,
        chat_chars,
        estimate_tokens(chat_chars),
    );

    let brain_files_json: Vec<Value> = existing_brain_files
        .iter()
        .map(|f| {
            json!({
                "name": f.name,
                "path": f.path,
                "size_bytes": f.size_bytes,
                "updated_at": f.updated_at,
            })
        })
        .collect();

    Ok(SlashCommandResponse {
        command: "context".to_string(),
        ephemeral: true,
        content,
        reasoning: None,
        agent_id: Some(agent_id.to_string()),
        metadata: json!({
            "estimated_chars": total_chars,
            "estimated_tokens": total_tokens,
            "context_limit": context_limit,
            "utilization_pct": utilization_pct,
            "system_prompt_chars": system_prompt_chars,
            "brain_chars": brain_chars,
            "extension_chars": extension_chars,
            "memory_count": memory_count,
            "memory_chars": effective_memory_chars,
            "chat_message_count": chat_message_count,
            "chat_chars": chat_chars,
            "existing_brain_files_count": existing_brain_files.len(),
            "brain_files": brain_files_json,
        }),
    })
}

async fn cmd_tools(
    args: &str,
    default_agent_id: &str,
    storage: &dyn StorageBackend,
    tool_executor: &dyn RuntimeToolExecutor,
) -> Result<SlashCommandResponse> {
    let explicit_target = args.trim();
    let target_agent_id = if explicit_target.is_empty() {
        default_agent_id.to_string()
    } else {
        explicit_target.to_string()
    };

    let agent_opt = storage.get_agent(&target_agent_id)?;
    let agent = match agent_opt {
        Some(a) => Some(a),
        None if !explicit_target.is_empty() => {
            return Err(AgentError::NotFound(format!(
                "agent '{}' not found",
                target_agent_id
            )));
        }
        None => None,
    };

    let enabled_patterns: Vec<String> = agent
        .as_ref()
        .map(|a| a.enabled_tools.clone())
        .unwrap_or_else(|| vec!["*".to_string()]);
    let denied_patterns: Vec<String> = agent
        .as_ref()
        .map(|a| a.denied_tools.clone())
        .unwrap_or_default();

    let specs: Vec<ToolSpec> = tool_executor.list_tools().await;
    let mut effective_enabled = Vec::new();
    let mut effective_denied = Vec::new();

    for spec in &specs {
        if agent_allows_tool_with_policy(&enabled_patterns, &denied_patterns, &spec.id, &spec.category)
        {
            effective_enabled.push(spec.id.clone());
        } else {
            effective_denied.push(spec.id.clone());
        }
    }

    let mut seen_groups = HashSet::new();
    let mut ordered_groups = Vec::new();
    for g in enabled_patterns
        .iter()
        .chain(denied_patterns.iter())
        .map(String::as_str)
        .filter(|s| s.trim().starts_with("group:"))
        .chain(STANDARD_TOOL_GROUPS.iter().copied())
    {
        let trimmed = g.trim();
        if seen_groups.insert(trimmed.to_string()) {
            ordered_groups.push(trimmed.to_string());
        }
    }

    let mut expanded_groups_map = Map::new();
    let mut group_lines = Vec::new();
    for group in &ordered_groups {
        if let Some(members) = expand_tool_group(group) {
            let member_strings: Vec<String> = members.iter().map(|m| (*m).to_string()).collect();
            group_lines.push(format!("- `{group}`: {}", member_strings.join(", ")));
            expanded_groups_map.insert(group.clone(), json!(member_strings));
        }
    }

    let mut explicit_denied_expanded = Vec::new();
    let mut denied_seen = HashSet::new();
    for pat in &denied_patterns {
        let trimmed = pat.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Some(members) = expand_tool_group(trimmed) {
            for &m in members {
                if denied_seen.insert(m.to_string()) {
                    explicit_denied_expanded.push(m.to_string());
                }
            }
        } else {
            let canonical = canonical_tool_id(trimmed).to_string();
            if denied_seen.insert(canonical.clone()) {
                explicit_denied_expanded.push(canonical);
            }
        }
    }

    let format_list = |items: &[String]| -> String {
        if items.is_empty() {
            "none".to_string()
        } else {
            items
                .iter()
                .map(|s| format!("`{s}`"))
                .collect::<Vec<_>>()
                .join(", ")
        }
    };

    let content = format!(
        "### Effective Tools for `{}`\n\
         - **Configured Enabled Patterns:** {}\n\
         - **Configured Denied Patterns:** {}\n\
         - **Effective Enabled Tools ({}):** {}\n\
         - **Effective Denied / Blocked Tools ({}):** {}\n\
         - **Expanded Deny Targets:** {}\n\n\
         #### Tool Groups (`group:*`)\n{}",
        target_agent_id,
        format_list(&enabled_patterns),
        format_list(&denied_patterns),
        effective_enabled.len(),
        format_list(&effective_enabled),
        effective_denied.len(),
        format_list(&effective_denied),
        format_list(&explicit_denied_expanded),
        group_lines.join("\n"),
    );

    Ok(SlashCommandResponse {
        command: "tools".to_string(),
        ephemeral: true,
        content,
        reasoning: None,
        agent_id: Some(target_agent_id.clone()),
        metadata: json!({
            "target_agent_id": target_agent_id,
            "enabled_patterns": enabled_patterns,
            "denied_patterns": denied_patterns,
            "effective_enabled_tools": effective_enabled,
            "effective_denied_tools": effective_denied,
            "expanded_denied_tools": explicit_denied_expanded,
            "expanded_groups": expanded_groups_map,
        }),
    })
}

fn build_extractive_summary(messages: &[ChatMessage]) -> String {
    let mut lines = Vec::new();
    for msg in messages.iter().take(12) {
        let single_line = msg
            .content
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        if single_line.is_empty() {
            continue;
        }
        lines.push(format!(
            "- {}: {}",
            format_role_label(msg.role),
            truncate_chars(&single_line, 180)
        ));
    }
    if lines.is_empty() {
        "Compacted conversation messages (no textual content).".to_string()
    } else {
        format!("Conversation summary:\n{}", lines.join("\n"))
    }
}

async fn cmd_compact(
    args: &str,
    conversation_id: Option<&str>,
    agent_id: &str,
    config: &RwLock<AppConfig>,
    storage: &dyn StorageBackend,
    inference: &dyn InferenceProvider,
    cancel: CancellationToken,
) -> Result<SlashCommandResponse> {
    let cid = conversation_id
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            AgentError::InvalidInput(
                "active conversation_id is required to run /compact".to_string(),
            )
        })?;

    let keep_override = if args.trim().is_empty() {
        None
    } else {
        let parsed = args.trim().parse::<usize>().map_err(|_| {
            AgentError::InvalidInput(format!(
                "invalid /compact argument '{}': expected optional non-negative keep_recent count",
                args.trim()
            ))
        })?;
        Some(parsed)
    };

    let cfg = snapshot_config(config);
    let messages = storage.list_chat_messages(cid, false, 1000)?;
    if messages.is_empty() {
        return Ok(SlashCommandResponse {
            command: "compact".to_string(),
            ephemeral: true,
            content: "Nothing to compact: conversation has no active messages.".to_string(),
            reasoning: None,
            agent_id: Some(agent_id.to_string()),
            metadata: json!({
                "conversation_id": cid,
                "archived_messages": 0,
                "kept_messages": 0,
            }),
        });
    }

    let keep_recent = match keep_override {
        Some(n) => n.min(messages.len()),
        None => {
            if messages.len() <= 1 {
                messages.len()
            } else {
                cfg.memory
                    .keep_recent_messages
                    .min(messages.len() / 2)
            }
        }
    };

    let split_idx = messages.len().saturating_sub(keep_recent);
    let to_summarize = if split_idx > 0 {
        &messages[..split_idx]
    } else {
        &messages[..]
    };

    let transcript = to_summarize
        .iter()
        .map(|m| {
            format!(
                "[{}] {}",
                format_role_label(m.role),
                truncate_chars(&m.content, 500)
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    let agent = storage.get_agent(agent_id)?;
    let req = InferenceRequest {
        model: effective_model_name(&cfg, agent.as_ref()),
        api_mode: cfg.inference.api_mode,
        messages: vec![
            InferenceMessage {
                role: "system".to_string(),
                content: "Summarize the following conversation turns into concise bullet points capturing key decisions, file paths, constraints, and open questions.".to_string(),
                tool_call_id: None,
                name: None,
                tool_calls: None,
                image_urls: Vec::new(),
            },
            InferenceMessage {
                role: "user".to_string(),
                content: transcript,
                tool_call_id: None,
                name: None,
                tool_calls: None,
                image_urls: Vec::new(),
            },
        ],
        tools: Vec::new(),
        temperature: 0.2,
        max_output_tokens: cfg.inference.max_output_tokens.min(1024).max(256),
        context_limit: cfg.inference.context_limit,
        stream: false,
        thinking_level: ThinkingLevel::Off,
    };

    let (summary_text, summarized_via) = match inference.generate(req, cancel).await {
        Ok(resp) if !resp.content.trim().is_empty() => {
            (resp.content.trim().to_string(), "inference")
        }
        _ => (build_extractive_summary(to_summarize), "extractive_fallback"),
    };

    let summary_id = format!("rolling-summary-{cid}");
    let now = now_iso();
    let item = MemoryItem {
        id: summary_id.clone(),
        scope: MemoryScope::Conversation,
        namespace: "conversation_summary".to_string(),
        owner_agent_id: Some(agent_id.to_string()),
        conversation_id: Some(cid.to_string()),
        task_id: None,
        key: "rolling_summary".to_string(),
        content: summary_text.clone(),
        summary: Some(truncate_chars(&summary_text, 240)),
        importance: 8,
        tags: vec!["compaction".to_string(), "summary".to_string()],
        archived: false,
        created_at: now.clone(),
        updated_at: now,
    };
    storage.upsert_memory(&item)?;

    let archived_count = if split_idx > 0 {
        storage.archive_old_chat_messages(cid, keep_recent)?
    } else {
        0
    };

    let content = format!(
        "### Conversation Compacted\n\
         - **Conversation:** `{}`\n\
         - **Summarized:** {} messages (via `{}`)\n\
         - **Archived:** {} older messages\n\
         - **Kept Active:** {} recent messages\n\n\
         #### Summary\n{}",
        cid,
        to_summarize.len(),
        summarized_via,
        archived_count,
        keep_recent,
        summary_text,
    );

    Ok(SlashCommandResponse {
        command: "compact".to_string(),
        ephemeral: false,
        content,
        reasoning: None,
        agent_id: Some(agent_id.to_string()),
        metadata: json!({
            "conversation_id": cid,
            "summarized_messages": to_summarize.len(),
            "archived_messages": archived_count,
            "kept_messages": keep_recent,
            "summary_memory_id": summary_id,
            "summarized_via": summarized_via,
            "summary": summary_text,
        }),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ThinkArg {
    Explicit {
        requested: &'static str,
        level: ThinkingLevel,
    },
    Auto,
}

fn parse_think_arg(raw: &str) -> Option<ThinkArg> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "off" => Some(ThinkArg::Explicit {
            requested: "off",
            level: ThinkingLevel::Off,
        }),
        "minimal" => Some(ThinkArg::Explicit {
            requested: "minimal",
            level: ThinkingLevel::Low,
        }),
        "low" => Some(ThinkArg::Explicit {
            requested: "low",
            level: ThinkingLevel::Low,
        }),
        "medium" => Some(ThinkArg::Explicit {
            requested: "medium",
            level: ThinkingLevel::Medium,
        }),
        "high" => Some(ThinkArg::Explicit {
            requested: "high",
            level: ThinkingLevel::High,
        }),
        "max" => Some(ThinkArg::Explicit {
            requested: "max",
            level: ThinkingLevel::Max,
        }),
        "auto" => Some(ThinkArg::Auto),
        _ => None,
    }
}

fn cmd_think(
    args: &str,
    agent_id: &str,
    config: &RwLock<AppConfig>,
    storage: &dyn StorageBackend,
) -> Result<SlashCommandResponse> {
    let trimmed = args.trim();
    let agent = storage.get_agent(agent_id)?;
    let agent_default_level = agent
        .as_ref()
        .map(|a| a.model_config.thinking_level)
        .unwrap_or_default();

    if trimmed.is_empty() {
        let cfg = snapshot_config(config);
        let effective = parse_config_thinking_level(&cfg.inference.thinking_level);
        return Ok(SlashCommandResponse {
            command: "think".to_string(),
            ephemeral: true,
            content: format!(
                "Current thinking level: `{}` (effective: `{}`, agent default: `{}`).\nAvailable: `off`, `minimal`, `low`, `medium`, `high`, `auto`.",
                cfg.inference.thinking_level,
                effective.as_str(),
                agent_default_level.as_str(),
            ),
            reasoning: None,
            agent_id: Some(agent_id.to_string()),
            metadata: json!({
                "thinking_level": cfg.inference.thinking_level,
                "effective": effective.as_str(),
                "agent_default": agent_default_level.as_str(),
                "updated": false,
            }),
        });
    }

    let parsed = parse_think_arg(trimmed).ok_or_else(|| {
        AgentError::InvalidInput(format!(
            "unsupported thinking level '{}'; expected one of: off, minimal, low, medium, high, max, auto",
            trimmed
        ))
    })?;

    let (requested, effective_level) = match parsed {
        ThinkArg::Explicit { requested, level } => (requested, level),
        ThinkArg::Auto => ("auto", agent_default_level),
    };

    update_config(config, |cfg| {
        cfg.inference.thinking_level = effective_level.as_str().to_string();
    });

    Ok(SlashCommandResponse {
        command: "think".to_string(),
        ephemeral: true,
        content: format!(
            "Preferred thinking level set to `{}` (effective runtime level: `{}`).",
            requested,
            effective_level.as_str(),
        ),
        reasoning: None,
        agent_id: Some(agent_id.to_string()),
        metadata: json!({
            "requested": requested,
            "thinking_level": requested,
            "effective": effective_level.as_str(),
            "updated": true,
        }),
    })
}

async fn cmd_model(
    args: &str,
    agent_id: &str,
    config: &RwLock<AppConfig>,
    storage: &dyn StorageBackend,
    inference: &dyn InferenceProvider,
) -> Result<SlashCommandResponse> {
    let trimmed = args.trim();
    let cfg = snapshot_config(config);
    let agent = storage.get_agent(agent_id)?;
    let available_models = inference.list_models().await.unwrap_or_default();

    if trimmed.is_empty() {
        let active = effective_model_name(&cfg, agent.as_ref())
            .unwrap_or_else(|| "auto (server default)".to_string());
        let agent_override = agent
            .as_ref()
            .and_then(|a| a.model_config.model_override.clone());
        let available_text = if available_models.is_empty() {
            "unavailable / not reported".to_string()
        } else {
            available_models
                .iter()
                .map(|m| format!("`{m}`"))
                .collect::<Vec<_>>()
                .join(", ")
        };

        return Ok(SlashCommandResponse {
            command: "model".to_string(),
            ephemeral: true,
            content: format!(
                "### Active Model\n\
                 - **Effective Model:** `{}`\n\
                 - **Config Model (`inference.model`):** `{}`\n\
                 - **Agent Override (`{}`):** {}\n\
                 - **Available Provider Models:** {}",
                active,
                if cfg.inference.model.trim().is_empty() {
                    "(auto)"
                } else {
                    cfg.inference.model.trim()
                },
                agent_id,
                agent_override
                    .as_deref()
                    .map(|m| format!("`{m}`"))
                    .unwrap_or_else(|| "none".to_string()),
                available_text,
            ),
            reasoning: None,
            agent_id: Some(agent_id.to_string()),
            metadata: json!({
                "model": active,
                "config_model": cfg.inference.model,
                "agent_model_override": agent_override,
                "available_models": available_models,
                "updated": false,
            }),
        });
    }

    if trimmed.len() > 256
        || trimmed.contains("..")
        || trimmed.chars().any(|c| c.is_control() || c.is_whitespace())
    {
        return Err(AgentError::InvalidInput(format!(
            "invalid model identifier '{}'",
            trimmed
        )));
    }

    let lower = trimmed.to_ascii_lowercase();
    let is_auto = lower == "auto" || lower == "default" || lower == "reset";

    let (resolved_model, validated_against_provider) = if is_auto {
        (String::new(), false)
    } else if !available_models.is_empty() {
        let matched = available_models
            .iter()
            .find(|m| m.as_str() == trimmed)
            .or_else(|| {
                available_models
                    .iter()
                    .find(|m| m.eq_ignore_ascii_case(trimmed))
            })
            .cloned();

        match matched {
            Some(exact) => (exact, true),
            None => {
                return Err(AgentError::InvalidInput(format!(
                    "model '{}' is not available on PS5LM (available: {})",
                    trimmed,
                    available_models.join(", ")
                )));
            }
        }
    } else {
        (trimmed.to_string(), false)
    };

    update_config(config, |mutable_cfg| {
        mutable_cfg.inference.model = resolved_model.clone();
    });

    let display_model = if resolved_model.is_empty() {
        "auto".to_string()
    } else {
        resolved_model.clone()
    };

    Ok(SlashCommandResponse {
        command: "model".to_string(),
        ephemeral: true,
        content: format!("Model override updated to `{}`.", display_model),
        reasoning: None,
        agent_id: Some(agent_id.to_string()),
        metadata: json!({
            "model": display_model,
            "model_override": if resolved_model.is_empty() { Value::Null } else { json!(resolved_model) },
            "validated_against_provider": validated_against_provider,
            "available_models": available_models,
            "updated": true,
        }),
    })
}

fn cmd_sandbox(
    args: &str,
    agent_id: &str,
    config: &RwLock<AppConfig>,
) -> Result<SlashCommandResponse> {
    let trimmed = args.trim();
    if trimmed.is_empty() {
        let cfg = snapshot_config(config);
        return Ok(SlashCommandResponse {
            command: "sandbox".to_string(),
            ephemeral: true,
            content: format!(
                "### Sandbox Policy\n\
                 - **Mode:** `{}` (`read_only` | `workspace_write` | `full_sandbox`)\n\
                 - **Approval Policy:** `{}` (`full_auto` | `on_request` | `always_ask` | `never`)\n\
                 - **Network Access:** {} (local network: {})\n\
                 - **Host Shell:** {}",
                cfg.sandbox.mode,
                cfg.sandbox.approval_policy,
                cfg.sandbox.network_access,
                cfg.sandbox.allow_local_network,
                cfg.sandbox.allow_host_shell,
            ),
            reasoning: None,
            agent_id: Some(agent_id.to_string()),
            metadata: json!({
                "mode": cfg.sandbox.mode,
                "approval_policy": cfg.sandbox.approval_policy,
                "network_access": cfg.sandbox.network_access,
                "allow_local_network": cfg.sandbox.allow_local_network,
                "allow_host_shell": cfg.sandbox.allow_host_shell,
                "updated": false,
            }),
        });
    }

    let mut new_mode: Option<SandboxMode> = None;
    let mut new_policy: Option<SandboxApprovalPolicy> = None;

    for token in trimmed.split_whitespace() {
        if let Some(m) = SandboxMode::parse(token) {
            new_mode = Some(m);
        } else if let Some(p) = SandboxApprovalPolicy::parse(token) {
            new_policy = Some(p);
        } else {
            return Err(AgentError::InvalidInput(format!(
                "invalid sandbox setting '{}'; expected mode (read_only|workspace_write|full_sandbox) or approval policy (full_auto|on_request|always_ask|never)",
                token
            )));
        }
    }

    let updated = update_config(config, |cfg| {
        if let Some(m) = new_mode {
            cfg.sandbox.mode = m.as_str().to_string();
        }
        if let Some(p) = new_policy {
            cfg.sandbox.approval_policy = p.as_str().to_string();
        }
        cfg.sync_sandbox_and_tools();
        cfg.sandbox.clone()
    });

    Ok(SlashCommandResponse {
        command: "sandbox".to_string(),
        ephemeral: true,
        content: format!(
            "Updated sandbox policy: mode=`{}`, approval_policy=`{}` (network: {}, local network: {}).",
            updated.mode,
            updated.approval_policy,
            updated.network_access,
            updated.allow_local_network,
        ),
        reasoning: None,
        agent_id: Some(agent_id.to_string()),
        metadata: json!({
            "mode": updated.mode,
            "approval_policy": updated.approval_policy,
            "network_access": updated.network_access,
            "allow_local_network": updated.allow_local_network,
            "allow_host_shell": updated.allow_host_shell,
            "updated": true,
        }),
    })
}

fn doctor_status_badge(status: DoctorStatus) -> &'static str {
    match status {
        DoctorStatus::Pass => "PASS",
        DoctorStatus::Warn => "WARN",
        DoctorStatus::Fail => "FAIL",
    }
}

fn format_doctor_report(report: &DoctorReport, applied_fixes: Option<&[String]>) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "### System Doctor Report (`{}`)\n\
         - **Overall:** `{}` | **Pass:** {} | **Warn:** {} | **Fail:** {}\n",
        report.checked_at,
        doctor_status_badge(report.overall_status),
        report.pass_count,
        report.warn_count,
        report.fail_count,
    ));

    if let Some(fixes) = applied_fixes {
        out.push_str("\n#### Applied Fixes\n");
        if fixes.is_empty() {
            out.push_str("- No fixes were needed.\n");
        } else {
            for fix in fixes {
                out.push_str(&format!("- {}\n", fix));
            }
        }
    }

    out.push_str("\n#### Diagnostic Checks\n");
    for check in &report.checks {
        out.push_str(&format!(
            "- **[{}]** `{}` ({}) — {}: {}",
            doctor_status_badge(check.status),
            check.id,
            check.category,
            check.title,
            check.summary
        ));
        if let Some(rem) = check.remediation.as_deref() {
            if !rem.trim().is_empty() {
                out.push_str(&format!(" _Remediation: {}_", rem.trim()));
            }
        }
        out.push('\n');
    }

    out
}

async fn cmd_doctor(
    args: &str,
    agent_id: &str,
    config: &RwLock<AppConfig>,
    storage: &dyn StorageBackend,
    inference: &dyn InferenceProvider,
    tool_executor: &dyn RuntimeToolExecutor,
) -> Result<SlashCommandResponse> {
    let trimmed = args.trim().to_ascii_lowercase();
    let do_fix = match trimmed.as_str() {
        "" | "check" => false,
        "fix" => true,
        _ => {
            return Err(AgentError::InvalidInput(format!(
                "invalid /doctor argument '{}'; usage: `/doctor [fix]`",
                args.trim()
            )));
        }
    };

    let cfg = snapshot_config(config);
    let builtin_tool_ids: Vec<String> = tool_executor
        .list_tools()
        .await
        .into_iter()
        .map(|spec| spec.id)
        .collect();

    if do_fix {
        let fix_report: DoctorFixReport =
            run_doctor_fixes(&cfg, storage, Some(inference), &builtin_tool_ids).await?;
        let content =
            format_doctor_report(&fix_report.report, Some(&fix_report.applied_fixes));
        let metadata = serde_json::to_value(&fix_report).unwrap_or_else(|_| json!({}));
        Ok(SlashCommandResponse {
            command: "doctor".to_string(),
            ephemeral: true,
            content,
            reasoning: None,
            agent_id: Some(agent_id.to_string()),
            metadata,
        })
    } else {
        let report: DoctorReport =
            run_doctor_checks(&cfg, storage, Some(inference), &builtin_tool_ids).await?;
        let content = format_doctor_report(&report, None);
        let metadata = serde_json::to_value(&report).unwrap_or_else(|_| json!({}));
        Ok(SlashCommandResponse {
            command: "doctor".to_string(),
            ephemeral: true,
            content,
            reasoning: None,
            agent_id: Some(agent_id.to_string()),
            metadata,
        })
    }
}

fn cmd_new(
    args: &str,
    agent_id: &str,
    storage: &dyn StorageBackend,
) -> Result<SlashCommandResponse> {
    let title = if args.trim().is_empty() {
        "New Conversation".to_string()
    } else {
        truncate_chars(args.trim(), 120)
    };
    let conv_id = new_id();
    let now = now_iso();
    let conv = Conversation {
        id: conv_id.clone(),
        title: title.clone(),
        mode: ConversationMode::Direct {
            agent_id: agent_id.to_string(),
        },
        created_at: now.clone(),
        updated_at: now,
        archived: false,
        sort_order: 0,
    };
    storage.create_conversation(&conv)?;

    Ok(SlashCommandResponse {
        command: "new".to_string(),
        ephemeral: false,
        content: format!("Started new conversation `{}` (`{}`).", title, conv_id),
        reasoning: None,
        agent_id: Some(agent_id.to_string()),
        metadata: json!({
            "new_conversation_id": conv_id,
            "title": title,
            "agent_id": agent_id,
            "mode": "direct",
        }),
    })
}

fn discover_workflow_commands(workspace_root: &Path) -> Vec<(String, String)> {
    let mut workflows = Vec::new();
    let mut seen = HashSet::new();

    if let Ok(files) = list_brain_files(workspace_root) {
        for entry in files {
            if let Some(stem) = entry
                .path
                .strip_prefix(".agents/workflows/")
                .and_then(|s| s.strip_suffix(".md"))
            {
                let lower = stem.to_ascii_lowercase();
                if is_valid_workflow_command_name(&lower) && seen.insert(lower.clone()) {
                    workflows.push((lower, entry.path));
                }
            }
        }
    }

    let workflows_dir = workspace_root.join(".agents").join("workflows");
    if is_safe_existing_dir(&workflows_dir) {
        if let Ok(read_dir) = fs::read_dir(&workflows_dir) {
            for item in read_dir.flatten() {
                let path = item.path();
                let is_non_symlink_file = fs::symlink_metadata(&path)
                    .map(|m| !m.file_type().is_symlink() && m.is_file())
                    .unwrap_or(false);
                if !is_non_symlink_file {
                    continue;
                }
                if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                    if let Some(stem) = file_name.strip_suffix(".md") {
                        let lower = stem.to_ascii_lowercase();
                        if stem == lower
                            && is_valid_workflow_command_name(&lower)
                            && seen.insert(lower.clone())
                        {
                            workflows.push((lower, format!(".agents/workflows/{file_name}")));
                        }
                    }
                }
            }
        }
    }

    workflows.sort_by(|a, b| a.0.cmp(&b.0));
    workflows
}

fn cmd_help(agent_id: &str, config: &RwLock<AppConfig>) -> Result<SlashCommandResponse> {
    let cfg = snapshot_config(config);
    let workspace_root = resolve_workspace_root(&cfg);
    let custom_workflows = discover_workflow_commands(&workspace_root);

    let mut out = String::from("### Built-in Slash Commands\n");
    let mut builtin_names = Vec::with_capacity(BUILTIN_SLASH_COMMANDS.len());
    for &(usage, desc) in BUILTIN_SLASH_COMMANDS {
        out.push_str(&format!("- `{usage}` — {desc}\n"));
        if let Some((cmd_tok, _)) = parse_slash_command(usage) {
            builtin_names.push(cmd_tok);
        }
    }

    out.push_str("\n### Custom Workspace Workflows (`.agents/workflows/*.md`)\n");
    if custom_workflows.is_empty() {
        out.push_str("- None discovered in `.agents/workflows/*.md`.\n");
    } else {
        for (cmd, rel_path) in &custom_workflows {
            out.push_str(&format!("- `/{cmd}` — `{rel_path}`\n"));
        }
    }

    let custom_json: Vec<Value> = custom_workflows
        .iter()
        .map(|(cmd, path)| json!({ "command": cmd, "path": path }))
        .collect();

    Ok(SlashCommandResponse {
        command: "help".to_string(),
        ephemeral: true,
        content: out,
        reasoning: None,
        agent_id: Some(agent_id.to_string()),
        metadata: json!({
            "builtin_commands": builtin_names,
            "custom_workflows": custom_json,
        }),
    })
}

fn is_valid_workflow_command_name(cmd: &str) -> bool {
    if cmd.is_empty() || cmd.len() > MAX_WORKFLOW_NAME_LEN || cmd == "." || cmd == ".." {
        return false;
    }
    cmd.bytes()
        .all(|b| matches!(b, b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-'))
}

fn is_safe_existing_dir(path: &Path) -> bool {
    let mut current = PathBuf::new();
    for comp in path.components() {
        current.push(comp);
        if matches!(comp, Component::Normal(_)) {
            if let Ok(meta) = fs::symlink_metadata(&current) {
                if meta.file_type().is_symlink() {
                    return false;
                }
            }
        }
    }
    fs::symlink_metadata(path)
        .map(|m| !m.file_type().is_symlink() && m.is_dir())
        .unwrap_or(false)
}

fn verify_no_symlink_segment(path: &Path, label: &str) -> Result<Option<fs::Metadata>> {
    match fs::symlink_metadata(path) {
        Ok(meta) => {
            if meta.file_type().is_symlink() {
                return Err(AgentError::PermissionDenied(format!(
                    "refusing to follow symlink at workflow path component '{}'",
                    label
                )));
            }
            Ok(Some(meta))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(AgentError::Storage(format!(
            "failed inspecting workflow path '{}': {}",
            path.display(),
            err
        ))),
    }
}

fn read_safe_workflow_markdown(workspace_root: &Path, cmd: &str) -> Result<(String, String)> {
    if !is_valid_workflow_command_name(cmd) {
        return Err(AgentError::InvalidInput(format!(
            "invalid slash command '/{}': workflow names must match [a-z0-9_-]",
            cmd
        )));
    }

    let rel_path = format!(".agents/workflows/{cmd}.md");
    let root_meta = verify_no_symlink_segment(workspace_root, "workspace_root")?.ok_or_else(|| {
        AgentError::NotFound(format!(
            "unknown slash command '/{}' (workspace root does not exist)",
            cmd
        ))
    })?;
    if !root_meta.is_dir() {
        return Err(AgentError::InvalidInput(
            "workspace_root is not a directory".to_string(),
        ));
    }

    let agents_dir = workspace_root.join(".agents");
    let Some(agents_meta) = verify_no_symlink_segment(&agents_dir, ".agents")? else {
        return Err(AgentError::NotFound(format!(
            "unknown slash command '/{}' (no `{}`)",
            cmd, rel_path
        )));
    };
    if !agents_meta.is_dir() {
        return Err(AgentError::NotFound(format!(
            "unknown slash command '/{}' (no `{}`)",
            cmd, rel_path
        )));
    }

    let workflows_dir = agents_dir.join("workflows");
    let Some(wf_dir_meta) = verify_no_symlink_segment(&workflows_dir, ".agents/workflows")? else {
        return Err(AgentError::NotFound(format!(
            "unknown slash command '/{}' (no `{}`)",
            cmd, rel_path
        )));
    };
    if !wf_dir_meta.is_dir() {
        return Err(AgentError::NotFound(format!(
            "unknown slash command '/{}' (no `{}`)",
            cmd, rel_path
        )));
    }

    let file_path = workflows_dir.join(format!("{cmd}.md"));
    let Some(file_meta) = verify_no_symlink_segment(&file_path, &rel_path)? else {
        return Err(AgentError::NotFound(format!(
            "unknown slash command '/{}' (missing `{}`)",
            cmd, rel_path
        )));
    };
    if !file_meta.is_file() {
        return Err(AgentError::InvalidInput(format!(
            "workflow path '{}' is not a regular file",
            rel_path
        )));
    }
    if file_meta.len() > MAX_WORKFLOW_BYTES as u64 {
        return Err(AgentError::LimitExceeded(format!(
            "workflow '{}' exceeds {} byte limit",
            rel_path, MAX_WORKFLOW_BYTES
        )));
    }

    let canon_root = fs::canonicalize(workspace_root).map_err(|e| {
        AgentError::Storage(format!("failed canonicalizing workspace_root: {e}"))
    })?;
    let canon_wf_dir = fs::canonicalize(&workflows_dir).map_err(|e| {
        AgentError::Storage(format!("failed canonicalizing workflows dir: {e}"))
    })?;
    let canon_file = fs::canonicalize(&file_path).map_err(|e| {
        AgentError::Storage(format!("failed canonicalizing workflow file: {e}"))
    })?;

    if !canon_wf_dir.starts_with(&canon_root) || !canon_file.starts_with(&canon_wf_dir) {
        return Err(AgentError::PermissionDenied(format!(
            "workflow path '{}' escaped workspace containment",
            rel_path
        )));
    }

    let bytes = fs::read(&canon_file).map_err(|e| {
        AgentError::Storage(format!("failed reading workflow '{}': {e}", rel_path))
    })?;
    if bytes.len() > MAX_WORKFLOW_BYTES {
        return Err(AgentError::LimitExceeded(format!(
            "workflow '{}' exceeds {} byte limit",
            rel_path, MAX_WORKFLOW_BYTES
        )));
    }
    let text = String::from_utf8(bytes).map_err(|_| {
        AgentError::InvalidInput(format!("workflow '{}' is not valid UTF-8", rel_path))
    })?;

    Ok((rel_path, text))
}

#[derive(Debug)]
struct ParsedWorkflow {
    description: Option<String>,
    ephemeral: bool,
    body: String,
}

fn parse_workflow_markdown(raw: &str) -> ParsedWorkflow {
    let normalized = raw.replace("\r\n", "\n");
    let mut description = None;
    let mut ephemeral = true;
    let mut body = normalized.as_str();

    if let Some(after_open) = normalized.strip_prefix("---\n") {
        if let Some(end_idx) = after_open.find("\n---") {
            let frontmatter = &after_open[..end_idx];
            let mut rest = &after_open[end_idx + 4..];
            if let Some(stripped) = rest.strip_prefix('\n') {
                rest = stripped;
            }
            body = rest;

            for line in frontmatter.lines() {
                if let Some((k, v)) = line.split_once(':') {
                    let key = k.trim().to_ascii_lowercase();
                    let val = v.trim().trim_matches('"').trim_matches('\'');
                    match key.as_str() {
                        "description" if !val.is_empty() => {
                            description = Some(val.to_string());
                        }
                        "ephemeral" => {
                            if val.eq_ignore_ascii_case("false") {
                                ephemeral = false;
                            } else if val.eq_ignore_ascii_case("true") {
                                ephemeral = true;
                            }
                        }
                        "mutating" => {
                            if val.eq_ignore_ascii_case("true") {
                                ephemeral = false;
                            } else if val.eq_ignore_ascii_case("false") {
                                ephemeral = true;
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    ParsedWorkflow {
        description,
        ephemeral,
        body: body.trim().to_string(),
    }
}

fn render_workflow_body(body: &str, args: &str) -> String {
    let clean_args = args.trim();
    if body.contains("{{args}}") || body.contains("$ARGUMENTS") {
        body.replace("{{args}}", clean_args)
            .replace("$ARGUMENTS", clean_args)
    } else if clean_args.is_empty() {
        body.to_string()
    } else {
        format!("{body}\n\nArguments:\n{clean_args}")
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute_custom_workflow(
    cmd: &str,
    args: &str,
    conversation_id: Option<&str>,
    agent_id: &str,
    config: &RwLock<AppConfig>,
    storage: &dyn StorageBackend,
    inference: &dyn InferenceProvider,
    cancel: CancellationToken,
) -> Result<SlashCommandResponse> {
    let cfg = snapshot_config(config);
    let workspace_root = resolve_workspace_root(&cfg);
    let (rel_path, raw_markdown) = read_safe_workflow_markdown(&workspace_root, cmd)?;
    let parsed = parse_workflow_markdown(&raw_markdown);

    let trimmed_args = args.trim();
    let dry_show = trimmed_args == "--show" || trimmed_args == "--raw";
    let rendered = render_workflow_body(&parsed.body, if dry_show { "" } else { trimmed_args });

    if dry_show || !parsed.ephemeral {
        return Ok(SlashCommandResponse {
            command: cmd.to_string(),
            ephemeral: parsed.ephemeral,
            content: rendered.clone(),
            reasoning: None,
            agent_id: Some(agent_id.to_string()),
            metadata: json!({
                "workflow": cmd,
                "workflow_path": rel_path,
                "description": parsed.description,
                "ephemeral": parsed.ephemeral,
                "mutating": !parsed.ephemeral,
                "executed": false,
                "workflow_content": rendered,
            }),
        });
    }

    let agent = storage.get_agent(agent_id)?;
    let (context_summary, _) = match conversation_id {
        Some(cid) if !cid.trim().is_empty() => {
            let recent = storage.list_chat_messages(cid.trim(), false, 16)?;
            build_read_only_context_summary(&recent)
        }
        _ => (String::new(), 0),
    };

    let system_prompt = if context_summary.is_empty() {
        format!(
            "Follow this read-only workspace workflow (`{rel_path}`) concisely without writing files or mutating state:\n\n{rendered}"
        )
    } else {
        format!(
            "Follow this read-only workspace workflow (`{rel_path}`) concisely without writing files or mutating state:\n\n{rendered}\n\nRecent conversation context:\n{context_summary}"
        )
    };
    let user_prompt = if trimmed_args.is_empty() {
        format!("Run workflow /{cmd}.")
    } else {
        trimmed_args.to_string()
    };

    let req = InferenceRequest {
        model: effective_model_name(&cfg, agent.as_ref()),
        api_mode: cfg.inference.api_mode,
        messages: vec![
            InferenceMessage {
                role: "system".to_string(),
                content: system_prompt,
                tool_call_id: None,
                name: None,
                tool_calls: None,
                image_urls: Vec::new(),
            },
            InferenceMessage {
                role: "user".to_string(),
                content: user_prompt,
                tool_call_id: None,
                name: None,
                tool_calls: None,
                image_urls: Vec::new(),
            },
        ],
        tools: Vec::new(),
        temperature: cfg.inference.temperature,
        max_output_tokens: cfg.inference.max_output_tokens,
        context_limit: cfg.inference.context_limit,
        stream: false,
        thinking_level: parse_config_thinking_level(&cfg.inference.thinking_level),
    };

    match inference.generate(req, cancel).await {
        Ok(resp) => Ok(SlashCommandResponse {
            command: cmd.to_string(),
            ephemeral: true,
            content: resp.content,
            reasoning: resp.reasoning,
            agent_id: Some(agent_id.to_string()),
            metadata: json!({
                "workflow": cmd,
                "workflow_path": rel_path,
                "description": parsed.description,
                "ephemeral": true,
                "mutating": false,
                "executed": true,
                "model": resp.model,
                "workflow_content": rendered,
            }),
        }),
        Err(AgentError::InferenceUnavailable(_)) => Ok(SlashCommandResponse {
            command: cmd.to_string(),
            ephemeral: true,
            content: rendered.clone(),
            reasoning: None,
            agent_id: Some(agent_id.to_string()),
            metadata: json!({
                "workflow": cmd,
                "workflow_path": rel_path,
                "description": parsed.description,
                "ephemeral": true,
                "mutating": false,
                "executed": false,
                "workflow_content": rendered,
            }),
        }),
        Err(err) => Err(err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use ps5_agent_core::{
        AgentEnvelope, AgentModelConfig, AgentRole, ApprovalRecord, AuditLogEntry, DeliveryStatus,
        InferenceResponse, InferenceStreamChunk, MemoryPermissions, ProviderHealth, SessionRecord,
        TaskCountsSummary, TaskEvent, TaskRecord, TaskStatus, TokenUsage, ToolExecutionRecord,
        ToolPermission,
    };
    use ps5_agent_manager::default_agent_definitions;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex;
    use tokio::sync::mpsc;

    use crate::traits::ToolExecutionOutput;

    static TEMP_SEQ: AtomicU64 = AtomicU64::new(1);

    struct TempWorkspace {
        path: PathBuf,
    }

    impl TempWorkspace {
        fn new(prefix: &str) -> Self {
            let seq = TEMP_SEQ.fetch_add(1, Ordering::Relaxed);
            let dir = std::env::temp_dir().join(format!(
                "ps5-slash-test-{prefix}-{}-{seq}",
                std::process::id()
            ));
            fs::create_dir_all(&dir).expect("create temp workspace");
            Self { path: dir }
        }
    }

    impl Drop for TempWorkspace {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[derive(Default)]
    struct DbState {
        agents: HashMap<String, AgentDefinition>,
        conversations: HashMap<String, Conversation>,
        chat_messages: Vec<ChatMessage>,
        tasks: Vec<TaskRecord>,
        memories: HashMap<String, MemoryItem>,
    }

    #[derive(Default)]
    struct MockStorage {
        db: Mutex<DbState>,
    }

    impl MockStorage {
        fn with_builtin_agents() -> Self {
            let storage = Self::default();
            for agent in default_agent_definitions() {
                storage.upsert_agent(&agent).expect("seed agent");
            }
            storage
        }

        fn counts(&self) -> (usize, usize, usize) {
            let g = self.db.lock().unwrap();
            (g.chat_messages.len(), g.tasks.len(), g.memories.len())
        }
    }

    impl StorageBackend for MockStorage {
        fn driver_name(&self) -> &'static str {
            "mock-memory"
        }

        fn run_migrations(&self) -> Result<()> {
            Ok(())
        }

        fn integrity_check(&self) -> Result<String> {
            Ok("ok".to_string())
        }

        fn checkpoint(&self) -> Result<()> {
            Ok(())
        }

        fn upsert_agent(&self, agent: &AgentDefinition) -> Result<()> {
            self.db
                .lock()
                .unwrap()
                .agents
                .insert(agent.id.clone(), agent.clone());
            Ok(())
        }

        fn get_agent(&self, agent_id: &str) -> Result<Option<AgentDefinition>> {
            Ok(self.db.lock().unwrap().agents.get(agent_id).cloned())
        }

        fn list_agents(&self) -> Result<Vec<AgentDefinition>> {
            Ok(self.db.lock().unwrap().agents.values().cloned().collect())
        }

        fn delete_agent(&self, agent_id: &str) -> Result<bool> {
            Ok(self.db.lock().unwrap().agents.remove(agent_id).is_some())
        }

        fn create_conversation(&self, conv: &Conversation) -> Result<()> {
            self.db
                .lock()
                .unwrap()
                .conversations
                .insert(conv.id.clone(), conv.clone());
            Ok(())
        }

        fn get_conversation(&self, conv_id: &str) -> Result<Option<Conversation>> {
            Ok(self
                .db
                .lock()
                .unwrap()
                .conversations
                .get(conv_id)
                .cloned())
        }

        fn list_conversations(&self, include_archived: bool) -> Result<Vec<Conversation>> {
            Ok(self
                .db
                .lock()
                .unwrap()
                .conversations
                .values()
                .filter(|c| include_archived || !c.archived)
                .cloned()
                .collect())
        }

        fn delete_conversation(&self, conv_id: &str) -> Result<bool> {
            Ok(self
                .db
                .lock()
                .unwrap()
                .conversations
                .remove(conv_id)
                .is_some())
        }

        fn append_chat_message(&self, msg: &ChatMessage) -> Result<()> {
            self.db.lock().unwrap().chat_messages.push(msg.clone());
            Ok(())
        }

        fn list_chat_messages(
            &self,
            conv_id: &str,
            include_archived: bool,
            limit: usize,
        ) -> Result<Vec<ChatMessage>> {
            let g = self.db.lock().unwrap();
            let mut out: Vec<ChatMessage> = g
                .chat_messages
                .iter()
                .filter(|m| m.conversation_id == conv_id && (include_archived || !m.archived))
                .cloned()
                .collect();
            if limit > 0 && out.len() > limit {
                let start = out.len() - limit;
                out = out.split_off(start);
            }
            Ok(out)
        }

        fn search_chat_messages(&self, query: &str, limit: usize) -> Result<Vec<ChatMessage>> {
            let g = self.db.lock().unwrap();
            let mut out: Vec<ChatMessage> = g
                .chat_messages
                .iter()
                .filter(|m| m.content.contains(query))
                .take(limit)
                .cloned()
                .collect();
            out.truncate(limit);
            Ok(out)
        }

        fn archive_old_chat_messages(&self, conv_id: &str, keep_recent: usize) -> Result<usize> {
            let mut g = self.db.lock().unwrap();
            let mut indices: Vec<usize> = g
                .chat_messages
                .iter()
                .enumerate()
                .filter(|(_, m)| m.conversation_id == conv_id && !m.archived)
                .map(|(i, _)| i)
                .collect();
            if indices.len() <= keep_recent {
                return Ok(0);
            }
            let archive_end = indices.len() - keep_recent;
            indices.truncate(archive_end);
            for idx in &indices {
                g.chat_messages[*idx].archived = true;
            }
            Ok(indices.len())
        }

        fn upsert_task(&self, task: &TaskRecord) -> Result<()> {
            let mut g = self.db.lock().unwrap();
            if let Some(existing) = g.tasks.iter_mut().find(|t| t.id == task.id) {
                *existing = task.clone();
            } else {
                g.tasks.push(task.clone());
            }
            Ok(())
        }

        fn get_task(&self, task_id: &str) -> Result<Option<TaskRecord>> {
            Ok(self
                .db
                .lock()
                .unwrap()
                .tasks
                .iter()
                .find(|t| t.id == task_id)
                .cloned())
        }

        fn list_tasks(&self, limit: usize) -> Result<Vec<TaskRecord>> {
            let g = self.db.lock().unwrap();
            Ok(g.tasks.iter().take(limit).cloned().collect())
        }

        fn task_counts_summary(&self) -> Result<TaskCountsSummary> {
            let g = self.db.lock().unwrap();
            let mut summary = TaskCountsSummary::default();
            summary.total = g.tasks.len();
            for t in &g.tasks {
                match t.status {
                    TaskStatus::Pending | TaskStatus::Assigned => summary.pending += 1,
                    TaskStatus::Running => summary.running += 1,
                    TaskStatus::Waiting => summary.waiting += 1,
                    TaskStatus::Completed => summary.completed += 1,
                    TaskStatus::Failed => summary.failed += 1,
                    TaskStatus::Cancelled => summary.cancelled += 1,
                }
            }
            Ok(summary)
        }

        fn append_task_event(&self, _event: &TaskEvent) -> Result<()> {
            Ok(())
        }

        fn list_task_events(&self, _task_id: &str) -> Result<Vec<TaskEvent>> {
            Ok(Vec::new())
        }

        fn append_agent_message(&self, _msg: &AgentEnvelope) -> Result<()> {
            Ok(())
        }

        fn update_agent_message_status(
            &self,
            _msg_id: &str,
            _status: DeliveryStatus,
        ) -> Result<()> {
            Ok(())
        }

        fn list_agent_messages(
            &self,
            _task_id: Option<&str>,
            _agent_id: Option<&str>,
            _limit: usize,
        ) -> Result<Vec<AgentEnvelope>> {
            Ok(Vec::new())
        }

        fn count_task_agent_messages(&self, _task_id: &str) -> Result<u32> {
            Ok(0)
        }

        fn upsert_memory(&self, item: &MemoryItem) -> Result<()> {
            self.db
                .lock()
                .unwrap()
                .memories
                .insert(item.id.clone(), item.clone());
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
            let g = self.db.lock().unwrap();
            Ok(g.memories
                .values()
                .filter(|m| include_archived || !m.archived)
                .filter(|m| scope.map(|s| m.scope == s).unwrap_or(true))
                .filter(|m| namespace.map(|ns| m.namespace == ns).unwrap_or(true))
                .filter(|m| {
                    owner_agent_id
                        .map(|owner| m.owner_agent_id.as_deref() == Some(owner))
                        .unwrap_or(true)
                })
                .take(limit)
                .cloned()
                .collect())
        }

        fn delete_memory(&self, memory_id: &str) -> Result<bool> {
            Ok(self.db.lock().unwrap().memories.remove(memory_id).is_some())
        }

        fn delete_agent_memories(&self, agent_id: &str) -> Result<usize> {
            let mut g = self.db.lock().unwrap();
            let before = g.memories.len();
            g.memories
                .retain(|_, m| m.owner_agent_id.as_deref() != Some(agent_id));
            Ok(before - g.memories.len())
        }

        fn memory_stats_for_agent(&self, agent_id: &str) -> Result<(u64, u64)> {
            let g = self.db.lock().unwrap();
            let mut count = 0u64;
            let mut bytes = 0u64;
            for m in g.memories.values() {
                if !m.archived && m.owner_agent_id.as_deref() == Some(agent_id) {
                    count += 1;
                    bytes += m.content.len() as u64;
                }
            }
            Ok((count, bytes))
        }

        fn upsert_tool_spec(&self, _spec: &ToolSpec) -> Result<()> {
            Ok(())
        }

        fn list_tool_specs(&self) -> Result<Vec<ToolSpec>> {
            Ok(Vec::new())
        }

        fn record_tool_execution(&self, _rec: &ToolExecutionRecord) -> Result<()> {
            Ok(())
        }

        fn list_tool_executions(
            &self,
            _agent_id: Option<&str>,
            _limit: usize,
        ) -> Result<Vec<ToolExecutionRecord>> {
            Ok(Vec::new())
        }

        fn get_setting(&self, _key: &str) -> Result<Option<String>> {
            Ok(None)
        }

        fn set_setting(&self, _key: &str, _value: &str) -> Result<()> {
            Ok(())
        }

        fn list_settings(&self) -> Result<Vec<(String, String)>> {
            Ok(Vec::new())
        }

        fn create_session(&self, _sess: &SessionRecord) -> Result<()> {
            Ok(())
        }

        fn get_session_by_hash(&self, _token_sha256: &str) -> Result<Option<SessionRecord>> {
            Ok(None)
        }

        fn revoke_session(&self, _session_id: &str) -> Result<()> {
            Ok(())
        }

        fn purge_expired_sessions(&self, _now_iso: &str) -> Result<usize> {
            Ok(0)
        }

        fn upsert_approval(&self, _approval: &ApprovalRecord) -> Result<()> {
            Ok(())
        }

        fn get_approval(&self, _approval_id: &str) -> Result<Option<ApprovalRecord>> {
            Ok(None)
        }

        fn list_approvals(&self, _pending_only: bool, _limit: usize) -> Result<Vec<ApprovalRecord>> {
            Ok(Vec::new())
        }

        fn append_audit_log(&self, _entry: &AuditLogEntry) -> Result<()> {
            Ok(())
        }

        fn list_audit_logs(&self, _limit: usize) -> Result<Vec<AuditLogEntry>> {
            Ok(Vec::new())
        }
    }

    struct MockInference {
        models: Vec<String>,
        reply: String,
        requests: Mutex<Vec<InferenceRequest>>,
    }

    impl MockInference {
        fn new(models: Vec<&str>, reply: &str) -> Self {
            Self {
                models: models.into_iter().map(String::from).collect(),
                reply: reply.to_string(),
                requests: Mutex::new(Vec::new()),
            }
        }

        fn captured_requests(&self) -> Vec<InferenceRequest> {
            self.requests.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl InferenceProvider for MockInference {
        async fn generate(
            &self,
            req: InferenceRequest,
            _cancel: CancellationToken,
        ) -> Result<InferenceResponse> {
            self.requests.lock().unwrap().push(req.clone());
            Ok(InferenceResponse {
                content: self.reply.clone(),
                reasoning: Some("concise btw reasoning".to_string()),
                tool_calls: Vec::new(),
                model: req.model.unwrap_or_else(|| "mock-ps5lm".to_string()),
                finish_reason: Some("stop".to_string()),
                usage: TokenUsage {
                    prompt_tokens: 24,
                    completion_tokens: 12,
                    total_tokens: 36,
                },
                latency_ms: 9,
            })
        }

        async fn generate_stream(
            &self,
            req: InferenceRequest,
            _tx: mpsc::Sender<InferenceStreamChunk>,
            cancel: CancellationToken,
        ) -> Result<InferenceResponse> {
            self.generate(req, cancel).await
        }

        async fn health_check(&self) -> ProviderHealth {
            ProviderHealth {
                reachable: true,
                loaded_model: self.models.first().cloned(),
                available_models: self.models.clone(),
                error: None,
            }
        }

        async fn list_models(&self) -> Result<Vec<String>> {
            Ok(self.models.clone())
        }
    }

    struct MockToolExecutor {
        specs: Vec<ToolSpec>,
    }

    impl MockToolExecutor {
        fn sample() -> Self {
            let mk = |id: &str, category: &str| ToolSpec {
                id: id.to_string(),
                name: id.to_string(),
                category: category.to_string(),
                description: format!("tool {id}"),
                input_schema: json!({ "type": "object" }),
                output_schema: json!({ "type": "object" }),
                required_permissions: vec![ToolPermission::Utility],
                requires_approval: false,
                is_mutating: false,
                enabled: true,
                timeout_secs: 30,
            };
            Self {
                specs: vec![
                    mk("read_file", "fs"),
                    mk("write_file", "fs"),
                    mk("delete_file", "fs"),
                    mk("web_search", "web"),
                    mk("web_fetch", "web"),
                    mk("browser", "web"),
                    mk("sandbox_exec", "runtime"),
                    mk("schedule_create", "automation"),
                ],
            }
        }
    }

    #[async_trait]
    impl RuntimeToolExecutor for MockToolExecutor {
        async fn list_tools(&self) -> Vec<ToolSpec> {
            self.specs.clone()
        }

        async fn get_tool(&self, tool_id: &str) -> Option<ToolSpec> {
            self.specs.iter().find(|t| t.id == tool_id).cloned()
        }

        async fn execute_tool(
            &self,
            _agent: &AgentDefinition,
            _task_id: Option<&str>,
            _tool_id: &str,
            _arguments: Value,
            _approved_bypass: bool,
            _cancel: CancellationToken,
        ) -> Result<ToolExecutionOutput> {
            Ok(ToolExecutionOutput {
                output: json!({ "ok": true }),
                is_mutating: false,
            })
        }

        fn extension_prompt_context(&self) -> (Vec<(String, String)>, Vec<(String, String)>) {
            (
                vec![("rust-skill".to_string(), "Rust best practices".to_string())],
                vec![],
            )
        }
    }

    fn test_config(root: &Path) -> RwLock<AppConfig> {
        let mut cfg = AppConfig::with_data_dir(root.join("data"));
        cfg.tools.allowed_paths = vec![root.to_string_lossy().to_string()];
        cfg.inference.model = "qwen3-32b".to_string();
        cfg.sync_sandbox_and_tools();
        RwLock::new(cfg)
    }

    fn sample_chat_msg(conv_id: &str, role: ChatRole, content: &str) -> ChatMessage {
        ChatMessage {
            id: new_id(),
            conversation_id: conv_id.to_string(),
            role,
            agent_id: Some("commander".to_string()),
            content: content.to_string(),
            reasoning: None,
            tool_calls_json: None,
            task_id: None,
            token_count: 10,
            latency_ms: None,
            archived: false,
            created_at: now_iso(),
            attachments: Vec::new(),
        }
    }

    #[test]
    fn parse_slash_command_handles_whitespace_case_and_args() {
        assert_eq!(parse_slash_command("hello world"), None);
        assert_eq!(parse_slash_command("/"), None);
        assert_eq!(parse_slash_command("/   "), None);
        assert_eq!(parse_slash_command("//comment"), None);
        assert_eq!(
            parse_slash_command("  /BTW   why is Rust fast? "),
            Some(("btw".to_string(), "why is Rust fast?".to_string()))
        );
        assert_eq!(
            parse_slash_command("/Status"),
            Some(("status".to_string(), String::new()))
        );
        assert_eq!(
            parse_slash_command("/SANDBOX  read_only  always_ask "),
            Some(("sandbox".to_string(), "read_only  always_ask".to_string()))
        );
    }

    #[tokio::test]
    async fn btw_is_ephemeral_and_writes_zero_messages_tasks_or_memories() {
        let ws = TempWorkspace::new("btw");
        let cfg = test_config(&ws.path);
        let storage = MockStorage::with_builtin_agents();
        let inference = MockInference::new(vec!["qwen3-32b"], "Side answer: Rust has zero-cost abstractions.");
        let tools = MockToolExecutor::sample();

        let resp = execute_slash_command(
            "/btw What is RAII?",
            None,
            None,
            &cfg,
            &storage,
            &inference,
            &tools,
            CancellationToken::new(),
        )
        .await
        .expect("btw succeeds");

        assert_eq!(resp.command, "btw");
        assert!(resp.ephemeral);
        assert!(resp.content.contains("zero-cost abstractions"));
        assert_eq!(resp.metadata["persisted"], json!(false));
        assert_eq!(storage.counts(), (0, 0, 0));
        assert_eq!(inference.captured_requests().len(), 1);

        let usage_resp = execute_slash_command(
            "/btw",
            None,
            None,
            &cfg,
            &storage,
            &inference,
            &tools,
            CancellationToken::new(),
        )
        .await
        .expect("empty btw returns usage");
        assert!(usage_resp.ephemeral);
        assert!(usage_resp.content.contains("Usage: `/btw <question>`"));
        assert_eq!(storage.counts(), (0, 0, 0));
        assert_eq!(inference.captured_requests().len(), 1);
    }

    #[tokio::test]
    async fn btw_reads_conversation_context_without_mutating_storage() {
        let ws = TempWorkspace::new("btw-ctx");
        let cfg = test_config(&ws.path);
        let storage = MockStorage::with_builtin_agents();
        let inference = MockInference::new(vec!["qwen3-32b"], "You decided on port 8080.");
        let tools = MockToolExecutor::sample();

        storage
            .append_chat_message(&sample_chat_msg(
                "conv-1",
                ChatRole::User,
                "Let's bind HTTP server to port 8080.",
            ))
            .unwrap();
        storage
            .append_chat_message(&sample_chat_msg(
                "conv-1",
                ChatRole::Assistant,
                "Configured port 8080 cleanly.",
            ))
            .unwrap();

        let resp = execute_slash_command(
            "/btw Which port did we pick?",
            Some("conv-1"),
            Some("commander"),
            &cfg,
            &storage,
            &inference,
            &tools,
            CancellationToken::new(),
        )
        .await
        .expect("btw with context");

        assert!(resp.ephemeral);
        assert_eq!(resp.metadata["context_messages_used"], json!(2));
        let reqs = inference.captured_requests();
        assert_eq!(reqs.len(), 1);
        assert!(reqs[0].messages[0].content.contains("port 8080"));
        assert_eq!(storage.counts(), (2, 0, 0));
    }

    #[tokio::test]
    async fn status_and_context_report_runtime_brain_and_memory_metrics() {
        let ws = TempWorkspace::new("status-ctx");
        fs::write(ws.path.join("AGENTS.md"), "# Agent Rules\nStay safe.\n").unwrap();
        let cfg = test_config(&ws.path);
        let storage = MockStorage::with_builtin_agents();
        let inference = MockInference::new(vec!["qwen3-32b"], "ok");
        let tools = MockToolExecutor::sample();

        storage
            .upsert_task(&TaskRecord {
                id: "t1".to_string(),
                conversation_id: Some("c1".to_string()),
                user_request: "Active job".to_string(),
                assigned_agent_id: "commander".to_string(),
                parent_task_id: None,
                priority: 0,
                created_at: now_iso(),
                started_at: None,
                completed_at: None,
                status: TaskStatus::Running,
                result: None,
                error_info: None,
                has_side_effects: false,
                progress_pct: 50,
                progress_note: None,
            })
            .unwrap();
        storage
            .upsert_memory(&MemoryItem {
                id: "m1".to_string(),
                scope: MemoryScope::Project,
                namespace: "notes".to_string(),
                owner_agent_id: Some("commander".to_string()),
                conversation_id: Some("c1".to_string()),
                task_id: None,
                key: "target".to_string(),
                content: "PS5 FW 13.40".to_string(),
                summary: None,
                importance: 7,
                tags: vec![],
                archived: false,
                created_at: now_iso(),
                updated_at: now_iso(),
            })
            .unwrap();

        let status = execute_slash_command(
            "/status",
            Some("c1"),
            Some("commander"),
            &cfg,
            &storage,
            &inference,
            &tools,
            CancellationToken::new(),
        )
        .await
        .expect("status");
        assert!(status.ephemeral);
        assert_eq!(status.metadata["tasks"]["active"], json!(1));
        assert_eq!(status.metadata["memory_count"], json!(1));
        assert_eq!(status.metadata["model"], json!("qwen3-32b"));

        let ctx = execute_slash_command(
            "/context",
            Some("c1"),
            Some("commander"),
            &cfg,
            &storage,
            &inference,
            &tools,
            CancellationToken::new(),
        )
        .await
        .expect("context");
        assert!(ctx.ephemeral);
        assert_eq!(ctx.metadata["memory_count"], json!(1));
        assert!(ctx.metadata["existing_brain_files_count"].as_u64().unwrap() >= 1);
        assert!(ctx.metadata["estimated_tokens"].as_u64().unwrap() > 0);
    }

    #[tokio::test]
    async fn tools_expands_groups_and_reports_denials() {
        let ws = TempWorkspace::new("tools");
        let cfg = test_config(&ws.path);
        let storage = MockStorage::with_builtin_agents();
        storage
            .upsert_agent(&AgentDefinition {
                id: "auditor".to_string(),
                name: "Auditor".to_string(),
                description: "Read-only auditor".to_string(),
                role: AgentRole::Analyst,
                role_label: "Auditor".to_string(),
                system_prompt: "Audit".to_string(),
                model_config: AgentModelConfig::default(),
                enabled_tools: vec!["group:fs".to_string(), "group:web".to_string()],
                denied_tools: vec!["file_delete".to_string(), "group:runtime".to_string()],
                sandbox_mode: Some("read_only".to_string()),
                tool_profile: None,
                memory_permissions: MemoryPermissions::default(),
                max_task_duration_secs: 60,
                max_reasoning_iterations: 4,
                priority: 1,
                can_communicate: true,
                enabled: true,
                is_builtin: false,
                is_protected: false,
                is_temporary: false,
                created_at: now_iso(),
                updated_at: now_iso(),
            })
            .unwrap();
        let inference = MockInference::new(vec!["qwen3-32b"], "ok");
        let tools = MockToolExecutor::sample();

        let resp = execute_slash_command(
            "/tools auditor",
            None,
            None,
            &cfg,
            &storage,
            &inference,
            &tools,
            CancellationToken::new(),
        )
        .await
        .expect("tools");
        assert!(resp.ephemeral);
        let enabled = resp.metadata["effective_enabled_tools"].as_array().unwrap();
        let denied = resp.metadata["effective_denied_tools"].as_array().unwrap();
        assert!(enabled.iter().any(|v| v == "read_file"));
        assert!(denied.iter().any(|v| v == "delete_file"));
        assert!(denied.iter().any(|v| v == "sandbox_exec"));
        assert!(resp.metadata["expanded_groups"]["group:fs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v == "read_file"));
    }

    #[tokio::test]
    async fn compact_think_model_sandbox_new_and_doctor_work_end_to_end() {
        let ws = TempWorkspace::new("all-cmds");
        let cfg = test_config(&ws.path);
        let storage = MockStorage::with_builtin_agents();
        let inference = MockInference::new(
            vec!["qwen3-32b", "llama-3.3-70b"],
            "- Decided to target 127.0.0.1:8080\n- Seeded workspace docs",
        );
        let tools = MockToolExecutor::sample();

        // /new creates a conversation and returns new_conversation_id
        let new_resp = execute_slash_command(
            "/new Sprint Planning",
            None,
            Some("commander"),
            &cfg,
            &storage,
            &inference,
            &tools,
            CancellationToken::new(),
        )
        .await
        .expect("/new");
        let conv_id = new_resp.metadata["new_conversation_id"]
            .as_str()
            .expect("new_conversation_id")
            .to_string();
        assert!(storage.get_conversation(&conv_id).unwrap().is_some());

        // Seed 6 messages then /compact
        for i in 0..6 {
            let role = if i % 2 == 0 {
                ChatRole::User
            } else {
                ChatRole::Assistant
            };
            storage
                .append_chat_message(&sample_chat_msg(&conv_id, role, &format!("Turn {i} details")))
                .unwrap();
        }
        let compact_resp = execute_slash_command(
            "/compact",
            Some(&conv_id),
            Some("commander"),
            &cfg,
            &storage,
            &inference,
            &tools,
            CancellationToken::new(),
        )
        .await
        .expect("/compact");
        assert_eq!(compact_resp.metadata["archived_messages"], json!(3));
        assert_eq!(compact_resp.metadata["kept_messages"], json!(3));
        assert_eq!(storage.list_chat_messages(&conv_id, false, 100).unwrap().len(), 3);
        assert_eq!(
            storage
                .query_memories(Some(MemoryScope::Conversation), Some("conversation_summary"), None, false, 10)
                .unwrap()
                .len(),
            1
        );

        // /think minimal -> maps to low, /think auto -> resolves cleanly
        let think_min = execute_slash_command(
            "/think minimal",
            None,
            None,
            &cfg,
            &storage,
            &inference,
            &tools,
            CancellationToken::new(),
        )
        .await
        .expect("/think minimal");
        assert_eq!(think_min.metadata["requested"], json!("minimal"));
        assert_eq!(think_min.metadata["effective"], json!("low"));
        assert_eq!(snapshot_config(&cfg).inference.thinking_level, "low");

        let think_auto = execute_slash_command(
            "/think auto",
            None,
            Some("commander"),
            &cfg,
            &storage,
            &inference,
            &tools,
            CancellationToken::new(),
        )
        .await
        .expect("/think auto");
        assert_eq!(think_auto.metadata["thinking_level"], json!("auto"));

        // /model validates against available models
        assert!(execute_slash_command(
            "/model bogus-model",
            None,
            None,
            &cfg,
            &storage,
            &inference,
            &tools,
            CancellationToken::new(),
        )
        .await
        .is_err());

        let model_ok = execute_slash_command(
            "/model llama-3.3-70b",
            None,
            None,
            &cfg,
            &storage,
            &inference,
            &tools,
            CancellationToken::new(),
        )
        .await
        .expect("/model valid");
        assert_eq!(model_ok.metadata["model"], json!("llama-3.3-70b"));
        assert_eq!(model_ok.metadata["validated_against_provider"], json!(true));
        assert_eq!(snapshot_config(&cfg).inference.model, "llama-3.3-70b");

        // /sandbox updates mode + approval_policy and calls sync_sandbox_and_tools()
        let sb_resp = execute_slash_command(
            "/sandbox full_sandbox full_auto",
            None,
            None,
            &cfg,
            &storage,
            &inference,
            &tools,
            CancellationToken::new(),
        )
        .await
        .expect("/sandbox");
        assert_eq!(sb_resp.metadata["mode"], json!("full_sandbox"));
        assert_eq!(sb_resp.metadata["approval_policy"], json!("full_auto"));
        assert_eq!(snapshot_config(&cfg).sandbox.mode, "full_sandbox");

        // /doctor and /doctor fix
        let doc_check = execute_slash_command(
            "/doctor",
            None,
            None,
            &cfg,
            &storage,
            &inference,
            &tools,
            CancellationToken::new(),
        )
        .await
        .expect("/doctor");
        assert!(doc_check.content.contains("System Doctor Report"));
        assert_eq!(doc_check.metadata["checks"].as_array().unwrap().len(), 12);

        let doc_fix = execute_slash_command(
            "/doctor fix",
            None,
            None,
            &cfg,
            &storage,
            &inference,
            &tools,
            CancellationToken::new(),
        )
        .await
        .expect("/doctor fix");
        assert!(!doc_fix.metadata["applied_fixes"].as_array().unwrap().is_empty());
        assert!(ws.path.join("AGENTS.md").exists());
    }

    #[tokio::test]
    async fn help_and_custom_workflows_enforce_validation_and_symlink_safety() {
        let ws = TempWorkspace::new("workflows");
        let wf_dir = ws.path.join(".agents").join("workflows");
        fs::create_dir_all(&wf_dir).unwrap();
        fs::write(
            wf_dir.join("daily-brief.md"),
            "---\ndescription: Summarize today's status\nephemeral: true\n---\nSummarize focus items for {{args}}.",
        )
        .unwrap();

        let cfg = test_config(&ws.path);
        let storage = MockStorage::with_builtin_agents();
        let inference = MockInference::new(vec!["qwen3-32b"], "Daily brief: All systems nominal.");
        let tools = MockToolExecutor::sample();

        let help = execute_slash_command(
            "/help",
            None,
            None,
            &cfg,
            &storage,
            &inference,
            &tools,
            CancellationToken::new(),
        )
        .await
        .expect("/help");
        assert!(help.content.contains("/btw"));
        assert!(help.content.contains("/daily-brief"));

        let wf_resp = execute_slash_command(
            "/daily-brief PS5 Runtime",
            None,
            None,
            &cfg,
            &storage,
            &inference,
            &tools,
            CancellationToken::new(),
        )
        .await
        .expect("/daily-brief");
        assert_eq!(wf_resp.command, "daily-brief");
        assert!(wf_resp.ephemeral);
        assert!(wf_resp.content.contains("All systems nominal"));
        assert!(wf_resp.metadata["workflow_content"]
            .as_str()
            .unwrap()
            .contains("PS5 Runtime"));
        assert_eq!(storage.counts(), (0, 0, 0));

        // Reject path traversal / invalid command chars
        assert!(execute_slash_command(
            "/../../etc/passwd",
            None,
            None,
            &cfg,
            &storage,
            &inference,
            &tools,
            CancellationToken::new(),
        )
        .await
        .is_err());

        // Reject symlinked workflow file where OS allows symlink creation
        let outside = TempWorkspace::new("outside-secret");
        let secret_file = outside.path.join("secret.md");
        fs::write(&secret_file, "SECRET").unwrap();
        let symlink_target = wf_dir.join("sym-escape.md");

        #[cfg(unix)]
        let symlink_created = std::os::unix::fs::symlink(&secret_file, &symlink_target).is_ok();
        #[cfg(windows)]
        let symlink_created =
            std::os::windows::fs::symlink_file(&secret_file, &symlink_target).is_ok();

        if symlink_created {
            let err = execute_slash_command(
                "/sym-escape",
                None,
                None,
                &cfg,
                &storage,
                &inference,
                &tools,
                CancellationToken::new(),
            )
            .await
            .expect_err("symlink workflow must be rejected");
            assert!(matches!(err, AgentError::PermissionDenied(_)));
        }
    }
}
