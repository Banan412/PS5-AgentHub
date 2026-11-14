use ps5_agent_core::{ToolPermission, ToolSpec};
use serde_json::json;

#[allow(clippy::too_many_arguments)]
fn spec(
    id: &str,
    category: &str,
    description: &str,
    input_schema: serde_json::Value,
    output_schema: serde_json::Value,
    required_permissions: Vec<ToolPermission>,
    requires_approval: bool,
    is_mutating: bool,
    timeout_secs: u64,
) -> ToolSpec {
    ToolSpec {
        id: id.to_string(),
        name: id.to_string(),
        category: category.to_string(),
        description: description.to_string(),
        input_schema,
        output_schema,
        required_permissions,
        requires_approval,
        is_mutating,
        enabled: true,
        timeout_secs,
    }
}

pub fn builtin_tool_specs(default_timeout_secs: u64) -> Vec<ToolSpec> {
    let t = default_timeout_secs;
    vec![
        // ---- Web (5) ----
        spec(
            "http_get",
            "web",
            "Perform an SSRF-guarded HTTP GET request to an allowed domain.",
            json!({
                "type": "object",
                "properties": {
                    "url": { "type": "string", "description": "HTTP/HTTPS URL to fetch" },
                    "headers": { "type": "object", "additionalProperties": { "type": "string" } }
                },
                "required": ["url"]
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::WebRead],
            false,
            false,
            t,
        ),
        spec(
            "http_post",
            "web",
            "Perform an SSRF-guarded HTTP POST request (requires operator approval).",
            json!({
                "type": "object",
                "properties": {
                    "url": { "type": "string" },
                    "body": {},
                    "content_type": { "type": "string" },
                    "headers": { "type": "object", "additionalProperties": { "type": "string" } }
                },
                "required": ["url"]
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::WebWrite],
            true,
            true,
            t,
        ),
        spec(
            "fetch_website",
            "web",
            "Fetch a webpage with SSRF protection and extract readable text wrapped as untrusted content.",
            json!({
                "type": "object",
                "properties": {
                    "url": { "type": "string" }
                },
                "required": ["url"]
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::WebRead],
            false,
            false,
            t,
        ),
        spec(
            "extract_html_text",
            "web",
            "Strip scripts/styles/tags from HTML and return readable text wrapped as untrusted content.",
            json!({
                "type": "object",
                "properties": {
                    "html": { "type": "string" },
                    "source": { "type": "string" }
                },
                "required": ["html"]
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::WebRead],
            false,
            false,
            t,
        ),
        spec(
            "json_api_request",
            "web",
            "Perform an SSRF-guarded JSON HTTP API request.",
            json!({
                "type": "object",
                "properties": {
                    "url": { "type": "string" },
                    "method": { "type": "string", "enum": ["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE"] },
                    "headers": { "type": "object", "additionalProperties": { "type": "string" } },
                    "body": {}
                },
                "required": ["url"]
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::WebRead],
            false,
            false,
            t,
        ),
        // ---- File (5) ----
        spec(
            "list_files",
            "file",
            "List files and directories inside the configured allowed_paths sandbox.",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "recursive": { "type": "boolean" },
                    "max_entries": { "type": "integer", "minimum": 1, "maximum": 2000 }
                }
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::FileRead],
            false,
            false,
            t,
        ),
        spec(
            "read_file",
            "file",
            "Read a UTF-8 text file strictly inside allowed_paths.",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "offset": { "type": "integer", "minimum": 0 },
                    "limit": { "type": "integer", "minimum": 1 }
                },
                "required": ["path"]
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::FileRead],
            false,
            false,
            t,
        ),
        spec(
            "write_file",
            "file",
            "Write or append UTF-8 text to a file inside allowed_paths.",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "content": { "type": "string" },
                    "append": { "type": "boolean" }
                },
                "required": ["path", "content"]
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::FileWrite],
            false,
            true,
            t,
        ),
        spec(
            "create_directory",
            "file",
            "Create a directory inside allowed_paths.",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" }
                },
                "required": ["path"]
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::FileWrite],
            false,
            true,
            t,
        ),
        spec(
            "delete_file",
            "file",
            "Delete a regular file inside allowed_paths (requires operator approval).",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" }
                },
                "required": ["path"]
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::FileDelete],
            true,
            true,
            t,
        ),
        // ---- Utility (4) ----
        spec(
            "calculator",
            "utility",
            "Evaluate a mathematical arithmetic expression safely without shell execution.",
            json!({
                "type": "object",
                "properties": {
                    "expression": { "type": "string" }
                },
                "required": ["expression"]
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::Utility],
            false,
            false,
            t,
        ),
        spec(
            "datetime",
            "utility",
            "Get current UTC time, parse, format, add seconds, or diff RFC3339 timestamps.",
            json!({
                "type": "object",
                "properties": {
                    "action": { "type": "string", "enum": ["now", "format", "parse", "add_seconds", "diff"] },
                    "timestamp": { "type": "string" },
                    "timestamp2": { "type": "string" },
                    "format": { "type": "string" },
                    "seconds": { "type": "integer" }
                }
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::Utility],
            false,
            false,
            t,
        ),
        spec(
            "json_process",
            "utility",
            "Parse, validate, pretty-format, minify, inspect keys, or query JSON pointers.",
            json!({
                "type": "object",
                "properties": {
                    "action": { "type": "string", "enum": ["parse", "validate", "format", "minify", "keys", "get"] },
                    "input": {},
                    "pointer": { "type": "string" }
                },
                "required": ["input"]
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::Utility],
            false,
            false,
            t,
        ),
        spec(
            "text_process",
            "utility",
            "Inspect or transform text (length, word_count, trim, case, split, replace, truncate, regex_match).",
            json!({
                "type": "object",
                "properties": {
                    "action": { "type": "string", "enum": ["length", "word_count", "trim", "lowercase", "uppercase", "split", "replace", "truncate", "regex_match"] },
                    "text": { "type": "string" },
                    "pattern": { "type": "string" },
                    "replacement": { "type": "string" },
                    "max_chars": { "type": "integer", "minimum": 0 }
                },
                "required": ["action", "text"]
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::Utility],
            false,
            false,
            t,
        ),
        // ---- Agent (5) ----
        spec(
            "send_agent_message",
            "agent",
            "Send an agent-to-agent message through the runtime messaging bus.",
            json!({
                "type": "object",
                "properties": {
                    "recipient_agent_id": { "type": "string" },
                    "content": { "type": "string" }
                },
                "required": ["recipient_agent_id", "content"]
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::AgentComm],
            false,
            false,
            t,
        ),
        spec(
            "assign_agent_task",
            "agent",
            "Delegate a sub-task to another enabled agent.",
            json!({
                "type": "object",
                "properties": {
                    "target_agent_id": { "type": "string" },
                    "user_request": { "type": "string" }
                },
                "required": ["target_agent_id", "user_request"]
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::AgentDelegate],
            false,
            false,
            t,
        ),
        spec(
            "query_agent_status",
            "agent",
            "Query status of an agent or task.",
            json!({
                "type": "object",
                "properties": {
                    "target_agent_id": { "type": "string" },
                    "task_id": { "type": "string" }
                }
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::AgentComm],
            false,
            false,
            t,
        ),
        spec(
            "read_agent_memory",
            "agent",
            "Read memories belonging to another agent if cross-agent read is permitted.",
            json!({
                "type": "object",
                "properties": {
                    "target_agent_id": { "type": "string" },
                    "query": { "type": "string" },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 100 }
                },
                "required": ["target_agent_id"]
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::MemoryCrossRead],
            false,
            false,
            t,
        ),
        spec(
            "request_temporary_specialist",
            "agent",
            "Request creation of a temporary specialist agent for a focused sub-task.",
            json!({
                "type": "object",
                "properties": {
                    "name": { "type": "string" },
                    "role_label": { "type": "string" },
                    "system_prompt": { "type": "string" },
                    "enabled_tools": { "type": "array", "items": { "type": "string" } },
                    "task_request": { "type": "string" }
                },
                "required": ["name", "role_label", "system_prompt"]
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::AgentSpawnTemp],
            false,
            true,
            t,
        ),
        // ---- Skills (1) ----
        spec(
            "load_skill",
            "skill",
            "Load the full SKILL.md instructions and resource list for an available skill by ID or name.",
            json!({
                "type": "object",
                "properties": {
                    "skill": { "type": "string", "description": "Skill ID or name to load" }
                },
                "required": ["skill"]
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::Utility],
            false,
            false,
            t,
        ),
        // ---- Web Extensions (3) ----
        spec(
            "web_search",
            "web",
            "Search the web via DuckDuckGo, SearXNG, Brave, or Tavily with SSRF safeguards and caching.",
            json!({
                "type": "object",
                "properties": {
                    "query": { "type": "string", "description": "Search query" },
                    "count": { "type": "integer", "minimum": 1, "maximum": 20 },
                    "provider": { "type": "string", "enum": ["auto", "duckduckgo", "searxng", "brave", "tavily"] },
                    "freshness": { "type": "string" }
                },
                "required": ["query"]
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::WebRead],
            false,
            false,
            t,
        ),
        spec(
            "web_fetch",
            "web",
            "Fetch a webpage with SSRF safeguards and convert readable HTML/text into clean bounded Markdown.",
            json!({
                "type": "object",
                "properties": {
                    "url": { "type": "string", "description": "HTTP/HTTPS URL to fetch" },
                    "max_chars": { "type": "integer", "minimum": 100 },
                    "extract_mode": { "type": "string", "enum": ["markdown", "text"] }
                },
                "required": ["url"]
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::WebRead],
            false,
            false,
            t,
        ),
        spec(
            "browser",
            "web",
            "Control a stateful pure-Rust sandbox browser session (open, snapshot, click, fill, submit, back, links, download, save_snapshot, sessions, close).",
            json!({
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["open", "navigate", "snapshot", "click", "fill", "submit", "back", "links", "download", "save_snapshot", "sessions", "close"]
                    },
                    "session_id": { "type": "string" },
                    "url": { "type": "string" },
                    "ref": { "type": "string", "description": "Element ref ID such as e1 from snapshot" },
                    "value": { "type": "string" },
                    "filename": { "type": "string" },
                    "name": { "type": "string" }
                },
                "required": ["action"]
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::WebRead],
            false,
            false,
            t,
        ),
        // ---- Code / Workspace Search & Patch (4) ----
        spec(
            "edit_file",
            "file",
            "Edit a workspace file in-place using targeted text replacement.",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "old_text": { "type": "string" },
                    "new_text": { "type": "string" },
                    "replace_all": { "type": "boolean" }
                },
                "required": ["path", "old_text", "new_text"]
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::FileWrite],
            false,
            true,
            t,
        ),
        spec(
            "apply_patch",
            "file",
            "Atomically validate and apply a unified diff or Codex patch envelope inside the workspace.",
            json!({
                "type": "object",
                "properties": {
                    "patch": { "type": "string" }
                },
                "required": ["patch"]
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::FileWrite],
            false,
            true,
            t,
        ),
        spec(
            "grep_search",
            "file",
            "Recursively search text files inside the workspace for a substring or regex pattern.",
            json!({
                "type": "object",
                "properties": {
                    "query": { "type": "string" },
                    "path": { "type": "string" },
                    "is_regex": { "type": "boolean" },
                    "case_insensitive": { "type": "boolean" },
                    "max_results": { "type": "integer", "minimum": 1, "maximum": 200 }
                },
                "required": ["query"]
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::FileRead],
            false,
            false,
            t,
        ),
        spec(
            "glob_files",
            "file",
            "Find files inside the workspace matching a glob pattern (supports *, ?, **).",
            json!({
                "type": "object",
                "properties": {
                    "pattern": { "type": "string" },
                    "path": { "type": "string" },
                    "max_results": { "type": "integer", "minimum": 1, "maximum": 500 }
                },
                "required": ["pattern"]
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::FileRead],
            false,
            false,
            t,
        ),
        // ---- Runtime Sandbox Execution & Processes (2) ----
        spec(
            "sandbox_exec",
            "runtime",
            "Execute commands inside the sandboxed workspace (virtual PS5-safe interpreter by default, optional host engine when explicitly permitted).",
            json!({
                "type": "object",
                "properties": {
                    "command": { "type": "string" },
                    "cwd": { "type": "string" },
                    "engine": { "type": "string", "enum": ["virtual", "host"] }
                },
                "required": ["command"]
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::FileWrite, ToolPermission::Utility],
            false,
            true,
            t,
        ),
        spec(
            "process",
            "runtime",
            "Inspect or manage active runtime tasks and background browser/execution handles.",
            json!({
                "type": "object",
                "properties": {
                    "action": { "type": "string", "enum": ["list", "status", "stop", "cancel"] },
                    "target_id": { "type": "string" }
                },
                "required": ["action"]
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::Utility],
            false,
            false,
            t,
        ),
        // ---- Memory (4) ----
        spec(
            "memory_read",
            "memory",
            "Read or search structured memory entries scoped to the current agent/project.",
            json!({
                "type": "object",
                "properties": {
                    "scope": { "type": "string" },
                    "namespace": { "type": "string" },
                    "query": { "type": "string" },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 100 }
                }
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::Utility],
            false,
            false,
            t,
        ),
        spec(
            "memory_write",
            "memory",
            "Store or update a structured persistent memory item.",
            json!({
                "type": "object",
                "properties": {
                    "scope": { "type": "string" },
                    "namespace": { "type": "string" },
                    "key": { "type": "string" },
                    "content": { "type": "string" },
                    "importance": { "type": "integer", "minimum": 1, "maximum": 10 }
                },
                "required": ["key", "content"]
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::Utility],
            false,
            true,
            t,
        ),
        spec(
            "memory_search",
            "memory",
            "Search persistent workspace MEMORY.md and daily memory/*.md markdown files.",
            json!({
                "type": "object",
                "properties": {
                    "query": { "type": "string" },
                    "max_results": { "type": "integer", "minimum": 1, "maximum": 100 }
                },
                "required": ["query"]
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::FileRead, ToolPermission::Utility],
            false,
            false,
            t,
        ),
        spec(
            "memory_get",
            "memory",
            "Read a line-bounded slice from MEMORY.md or a memory/*.md file in the workspace.",
            json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "from_line": { "type": "integer", "minimum": 1 },
                    "lines": { "type": "integer", "minimum": 1 }
                },
                "required": ["path"]
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::FileRead, ToolPermission::Utility],
            false,
            false,
            t,
        ),
        // ---- Utility / Info / Doctor / Artifacts (4) ----
        spec(
            "system_info",
            "utility",
            "Return host/runtime OS, architecture, runtime limits, and workspace overview.",
            json!({
                "type": "object",
                "properties": {}
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::Utility],
            false,
            false,
            t,
        ),
        spec(
            "ps5_info",
            "utility",
            "Return PS5 platform diagnostics, PS5LM inference endpoint status, and sandbox posture.",
            json!({
                "type": "object",
                "properties": {}
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::Utility],
            false,
            false,
            t,
        ),
        spec(
            "doctor",
            "utility",
            "Run the 12-check system diagnostic doctor and optionally apply safe auto-fixes.",
            json!({
                "type": "object",
                "properties": {
                    "fix": { "type": "boolean" }
                }
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::Utility],
            false,
            false,
            t,
        ),
        spec(
            "write_artifact",
            "utility",
            "Save a formatted report, HTML preview, SVG diagram, JSON output, or text artifact under workspace artifacts/.",
            json!({
                "type": "object",
                "properties": {
                    "filename": { "type": "string" },
                    "name": { "type": "string" },
                    "title": { "type": "string" },
                    "content": { "type": "string" }
                },
                "required": ["content"]
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::FileWrite],
            false,
            true,
            t,
        ),
        // ---- Sessions / A2A (4) ----
        spec(
            "sessions_list",
            "agent",
            "List active conversations and recent sessions.",
            json!({
                "type": "object",
                "properties": {
                    "limit": { "type": "integer", "minimum": 1, "maximum": 200 },
                    "active_only": { "type": "boolean" }
                }
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::AgentComm],
            false,
            false,
            t,
        ),
        spec(
            "sessions_spawn",
            "agent",
            "Spawn a background sub-agent task in a target or new conversation.",
            json!({
                "type": "object",
                "properties": {
                    "target_agent_id": { "type": "string" },
                    "conversation_id": { "type": "string" },
                    "prompt": { "type": "string" }
                },
                "required": ["prompt"]
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::AgentDelegate],
            false,
            true,
            t,
        ),
        spec(
            "sessions_send",
            "agent",
            "Post a message into another conversation session.",
            json!({
                "type": "object",
                "properties": {
                    "conversation_id": { "type": "string" },
                    "message": { "type": "string" }
                },
                "required": ["conversation_id", "message"]
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::AgentComm],
            false,
            true,
            t,
        ),
        spec(
            "session_status",
            "agent",
            "Inspect status and recent activity of a conversation or task.",
            json!({
                "type": "object",
                "properties": {
                    "conversation_id": { "type": "string" },
                    "task_id": { "type": "string" }
                }
            }),
            json!({ "type": "object" }),
            vec![ToolPermission::AgentComm],
            false,
            false,
            t,
        ),
    ]
}
