# PS5 Agent HTTP & SSE API Reference (`/api/v1`)

This document describes the REST and Server-Sent Events (SSE) HTTP API exposed by `ps5-agent-http` (`crates/ps5-agent-http/src/lib.rs`, `routes.rs`, `backend.rs`, and `security.rs`).

## Base URL, Content Type & Body Size Limits

- Default bind address:
  - Host builds: `http://127.0.0.1:8080`
  - PS5 builds (`target_vendor = "sie"`): `http://0.0.0.0:8080`
- Request/response bodies use `application/json` (except SSE `text/event-stream`, static dashboard assets, and workspace file download/preview responses).
- Request body limits (`crates/ps5-agent-http/src/lib.rs`):
  - **Standard API & public routes:** `server.max_body_bytes` (default `1048576` bytes / `1 MiB`; `0` lifts to the `256 MiB` router ceiling).
  - **Upload API routes:** `max(server.upload_max_body_bytes, server.max_body_bytes)` (default `67108864` bytes / `64 MiB`; `0` lifts to `256 MiB`). Applies to `GET`/`POST /api/v1/conversations/{id}/messages`, `GET`/`POST /api/v1/tools`, `POST /api/v1/memories/import`, `POST /api/v1/files/upload`, `POST /api/v1/extensions/skills/import`, `POST /api/v1/extensions/plugins/import`, and `POST /api/v1/extensions/mcp`.
  - Oversized payloads return `413 Payload Too Large`.

## Authentication & CSRF

When `security.auth_enabled = true` (default):

1. **Cookie Session (`POST /api/v1/auth/login`)**:
   - Sets `Set-Cookie: ps5_agent_session=<token>; HttpOnly; SameSite=Strict; Path=/; Max-Age=<ttl_secs>` (where `ttl_hours = session_ttl_hours.clamp(1, 87_600)`, so `0` produces `Max-Age=3600`).
   - Returns `{ "username", "csrf_token", "expires_at" }`.
   - Every mutating request (`POST`, `PUT`, `PATCH`, `DELETE`) authenticated by cookie MUST include `X-CSRF-Token: <csrf_token>` and, when `Origin` or `Referer` is sent by the browser, it must match the `Host` header (`403 Forbidden` otherwise).
2. **Bearer API Token (`Authorization: Bearer <token>`)**:
   - Compared by SHA-256 hex digest against `security.api_token_sha256` (or `settings["auth.api_token_sha256"]`) using constant-time equality (`subtle::ConstantTimeEq`).
3. **Unauthenticated & Custom-Token Exceptions (`auth_csrf_middleware`)**:
   - `GET` / `HEAD /api/v1/health`: minimal `{ "status", "version" }` when unauthenticated; extended status when authenticated.
   - `POST /api/v1/auth/login`: login rate-limited; still enforces same-origin `Origin`/`Referer` validation when present.
   - **`POST /api/v1/webhooks/inbound/{id}`**: exempt from browser cookie session, `Origin`/`Referer`, and `X-CSRF-Token` checks; authenticates via per-webhook secret token in `x-ps5-webhook-token: <secret>` **or** `Authorization: Bearer <secret>` (if both headers are present they must match; duplicate headers, missing/empty headers, or conflicting tokens return `401 Unauthorized`, while hash mismatches return `403 Forbidden`).
   - Static dashboard shell: `GET /`, `GET /styles.css`, `GET /app.js`.

All other `/api/v1/*` routes (including `GET /api/v1/auth/session`) return `401 Unauthorized` when unauthenticated.

## Error Envelope & Pagination

All error responses return JSON:

```json
{
  "error": {
    "code": "unauthorized",
    "message": "Authentication required"
  }
}
```

- When an action requires human approval (`AgentError::ApprovalRequired`), the response returns `409 Conflict` with `"code": "approval_required"` and `"approval_id"`.
- List endpoints accepting `?limit=` pass through `clamp_limit(limit, default)`: omitting `limit` uses the route default (`50`, `100`, or `500`), `?limit=0` expands up to `10000`, and positive values clamp to `1..=10000`.
- Path/query IDs (`validate_id`) must be `1..=128` trimmed non-empty characters with no `/`, `\`, `.`, `..`, or control characters.

---

## Endpoints

### Authentication

| Method | Path | Auth | Description |
|---|---|---|---|
| `POST` | `/api/v1/auth/login` | Public (login rate-limited) | Validate username & Argon2id password, issue session cookie + CSRF token |
| `POST` | `/api/v1/auth/logout` | Required + CSRF | Revoke cookie session and clear cookie (`Max-Age=0`) |
| `GET` | `/api/v1/auth/session` | Required | Return `{ authenticated, username, auth_method, csrf_token, expires_at }` |
| `POST` | `/api/v1/auth/password` | Required + CSRF | Verify `current_password`, hash `new_password` (8–256 chars) with Argon2id, persist to `auth.password_hash` |

### Health, Metrics & SSE Events

| Method | Path | Auth | Description |
|---|---|---|---|
| `GET` | `/api/v1/health` | Optional | Unauthenticated: `{ status, version }`. Authenticated adds `uptime_secs`, `storage_driver`, `ps5lm_reachable`, `ps5lm_loaded_model`, `network_degraded`. |
| `GET` | `/api/v1/metrics` | Required | Returns `MetricsSnapshot` (`uptime_secs`, `storage_driver`, inference/HTTP/DB stats, task counts, hardware temps, `network_degraded`). |
| `GET` | `/api/v1/events` | Required | SSE stream (`text/event-stream`) emitting `StreamEvent` frames with 15s `keepalive` pings. Concurrent connections are capped at `MAX_SSE_CONNECTIONS = 32` (`429 Too Many Requests` when full). |
| `GET` | `/api/v1/events/stream` | Required | Alias for `GET /api/v1/events` (`events_sse_handler`). |

If a subscriber falls behind the broadcast ring (`RecvError::Lagged(skipped)`), the stream emits a `system_alert` frame containing `{ "type": "system_alert", "level": "warn", "resync": true, "resync_required": true, "skipped": <n>, "message": ... }`.

#### SSE `StreamEvent` Types (`crates/ps5-agent-core/src/events.rs`)

| `event:` Name | Payload Fields (`"type"` matches `event:`) |
|---|---|
| `chat_token` | `conversation_id`, `task_id`, `agent_id`, `delta` |
| `chat_reasoning` | `conversation_id`, `task_id`, `agent_id`, `delta` |
| `chat_completed` | `conversation_id`, `task_id`, `agent_id`, `message_id`, `content`, `reasoning?` |
| `tool_call_started` | `task_id`, `agent_id`, `tool_id`, `arguments_json` |
| `tool_call_finished` | `task_id`, `agent_id`, `tool_id`, `success`, `duration_ms` |
| `task_updated` | `conversation_id?`, `task_id`, `agent_id`, `status`, `progress_pct`, `progress_note?` |
| `agent_message_sent` | `envelope_id`, `sender_agent_id`, `recipient_agent_id`, `message_type`, `status` |
| `approval_requested` | `approval_id`, `task_id?`, `agent_id`, `operation`, `reason` |
| `approval_resolved` | `approval_id`, `status` |
| `agent_updated` | `agent_id`, `action` |
| `system_alert` | `level`, `message` *(plus `resync`, `resync_required`, `skipped` on lagged SSE)* |
| `files_changed` | `conversation_id?`, `path`, `action` *(emitted on auto-transported script writes and brain file writes)* |
| `ps5_lm_synced` | `loaded_model?`, `context_length?`, `total_slots?` |
| `continuation_triggered` | `conversation_id`, `task_id`, `agent_id`, `reason` |
| `schedule_updated` | `schedule` (`ScheduleRecord`) |
| `sandbox_updated` | `mode`, `approval_policy`, `network_access` |
| `webhook_triggered` | `webhook_id`, `name`, `task_id`, `conversation_id` |

### Conversations, Chat & Slash Commands

| Method | Path | Query / Body | Description |
|---|---|---|---|
| `GET` | `/api/v1/conversations` | `?include_archived=false` | List `Conversation` items |
| `POST` | `/api/v1/conversations` | `{ title, mode }` | Create a `direct` or `group` conversation (`201 Created`; title `1..=200` chars) |
| `GET` | `/api/v1/conversations/search` | `?q=...&limit=50` | Search chat messages (`1..=256` char query) |
| `POST` | `/api/v1/conversations/reorder` | `ReorderConversationsRequest { ordered_ids?, conversation_id?, direction? }` | Reorder sidebar conversations; returns updated `Conversation[]` (audits `conversation.reorder`) |
| `GET` | `/api/v1/conversations/{id}` | `?include_archived=false&limit=100` | Return `{ conversation, messages }` |
| `DELETE` | `/api/v1/conversations/{id}` | — | Delete conversation (`{ deleted: true, id }`) |
| `POST` | `/api/v1/conversations/{id}/fork` | `ForkConversationRequest { up_to_message_id?, title? }` (optional JSON) | Fork conversation history into a new conversation (`201 Created`; audits `conversation.fork`) |
| `POST` | `/api/v1/conversations/{id}/cancel` | `CancelConversationTasksRequest { agent_id? }` (optional JSON) | Cancel non-terminal tasks for this conversation (optionally filtered to `agent_id`); returns `{ cancelled_count }` |
| `POST` | `/api/v1/conversations/{id}/command` | `SlashCommandRequest { command, agent_id? }` | Execute a slash command (`1..=8000` chars) in the conversation context; returns `SlashCommandResponse` (audits `conversation.command`) |
| `GET` | `/api/v1/conversations/{id}/messages` | `?include_archived=false&limit=500` | List `ChatMessage` entries *(upload tier)* |
| `POST` | `/api/v1/conversations/{id}/messages` | `SendMessageRequest { content, agent_id?, thinking_level?, attachments?, async_dispatch?, immediate? }` | Append user message (content `<= 64000` chars, `<= 16` attachments) and dispatch task (`201 Created` `SendMessageResponse { user_message, task }`; `immediate: true` bypasses `max_active_tasks`; *upload tier*) |
| `DELETE` | `/api/v1/conversations/{id}/messages/{msg_id}` | — | Delete single message (`{ deleted: true, conversation_id, message_id }`; audits `message.delete`) |
| `POST` | `/api/v1/conversations/{id}/messages/{msg_id}/retry` | `MessageActionRequest { thinking_level?, agent_id?, async_dispatch? }` (optional JSON) | Retry from a user message; returns `SendMessageResponse` (audits `message.retry`) |
| `POST` | `/api/v1/conversations/{id}/messages/{msg_id}/regenerate` | `MessageActionRequest { thinking_level?, agent_id?, async_dispatch? }` (optional JSON) | Regenerate an assistant response; returns `SendMessageResponse` (audits `message.regenerate`) |
| `POST` | `/api/v1/conversations/{id}/messages/{msg_id}/continue` | `MessageActionRequest { thinking_level? }` (optional JSON) | Continue a truncated/incomplete assistant message; returns `SendMessageResponse` (audits `message.continue`) |

#### `SlashCommandRequest` / `SlashCommandResponse` (`POST /api/v1/conversations/{id}/command`)

Returns `SlashCommandResponse { command, ephemeral, content, reasoning, agent_id, metadata }`:
- **Built-in commands:** `/btw <question>` (ephemeral side-question with zero DB/chat/task/memory mutation), `/status`, `/context`, `/tools [agent]`, `/compact [keep_recent]`, `/think [off|minimal|low|medium|high|auto]`, `/model [name]`, `/sandbox [read_only|workspace_write|full_sandbox|full_auto|on_request|always_ask|never]`, `/doctor [fix]`, `/new [title]` (`metadata.new_conversation_id`), and `/help`.
- **Workspace workflows:** `/<workflow_name>` matches `.agents/workflows/<workflow_name>.md`.

### Agents

| Method | Path | Query / Body | Description |
|---|---|---|---|
| `GET` | `/api/v1/agents` | — | List `AgentCardView` entries (definition + runtime stats) |
| `POST` | `/api/v1/agents` | `CreateAgentRequest` | Create custom agent (`201 Created`). Supports `denied_tools`, `sandbox_mode`, `tool_profile` alongside `enabled_tools`, `model_config`, `memory_permissions`, `max_task_duration_secs`, `max_reasoning_iterations`, `priority`, `can_communicate`, `enabled`, `is_temporary`. |
| `POST` | `/api/v1/agents/enable-all` | — | Re-enable all disabled agents; returns `{ "enabled_count": <n> }` |
| `GET` | `/api/v1/agents/{id}` | — | Get single `AgentCardView` |
| `PUT` | `/api/v1/agents/{id}` | `UpdateAgentRequest` | Update agent configuration (supports `denied_tools`, `sandbox_mode`, `tool_profile`, etc.) |
| `DELETE` | `/api/v1/agents/{id}` | — | Delete non-protected agent (`{ deleted: true, id }`; `403` if protected) |
| `POST` | `/api/v1/agents/{id}/enable` | — | Enable agent |
| `POST` | `/api/v1/agents/{id}/disable` | — | Disable non-protected agent (`403` if protected) |
| `GET` | `/api/v1/agents/{id}/memory` | `?include_archived=false&limit=100` | List `MemoryItem` records owned by agent |

### Agent Messages

| Method | Path | Query / Body | Description |
|---|---|---|---|
| `GET` | `/api/v1/agent-messages` | `?task_id=&agent_id=&limit=100` | List `AgentEnvelope` records |
| `POST` | `/api/v1/agent-messages` | `SendAgentMessageRequest { sender_agent_id, recipient_agent_id, task_id?, message_type?, content }` | Send inter-agent message (`201 Created`; content `1..=32000` chars) |

### Tasks

| Method | Path | Query / Body | Description |
|---|---|---|---|
| `GET` | `/api/v1/tasks` | `?status=&agent_id=&conversation_id=&limit=500` | List `TaskRecord` items. `status` accepts `active` (any non-terminal state: `pending`, `assigned`, `running`, `waiting`) or exact `pending`, `assigned`, `running`, `waiting`, `completed`, `failed`, `cancelled` (`canceled` alias); invalid `status` returns `400 Bad Request`. Filters cleanly by `agent_id` (`assigned_agent_id`) and `conversation_id`. |
| `POST` | `/api/v1/tasks` | `CreateTaskRequest { conversation_id?, user_request, assigned_agent_id?, priority? }` | Submit new task (`201 Created`; `user_request` `1..=64000` chars) |
| `GET` | `/api/v1/tasks/{id}` | — | Get `TaskRecord` |
| `POST` | `/api/v1/tasks/{id}/cancel` | — | Request task cancellation |
| `POST` | `/api/v1/tasks/{id}/expedite` | — | Expedite a `Pending` task to start immediately bypassing `max_active_tasks`; returns `{ "task": TaskRecord }` (audits `task.expedite`) |
| `GET` | `/api/v1/tasks/{id}/events` | — | List `TaskEvent` audit trail for task |

### Schedules

| Method | Path | Query / Body | Description |
|---|---|---|---|
| `GET` | `/api/v1/schedules` | — | List `ScheduleRecord` items |
| `POST` | `/api/v1/schedules` | `HttpCreateSchedule { title, prompt, target_agent_id?, conversation_id?, kind, run_at?, interval_secs?, daily_time?, cron_expr?, timezone_offset_mins?, enabled? }` | Create schedule (`201 Created`; audits `schedule.create`) |
| `POST` | `/api/v1/schedules/ai` | `HttpScheduleAiRequest { prompt, timezone_offset_mins?, target_agent_id?, auto_create? }` | Parse natural-language schedule (`1..=16000` chars) into `HttpScheduleAiResponse { draft, created? }` (audits `schedule.parse_ai`) |
| `GET` | `/api/v1/schedules/{id}` | — | Get `ScheduleRecord` |
| `PUT` / `PATCH` | `/api/v1/schedules/{id}` | `HttpUpdateSchedule` | Update schedule fields (audits `schedule.update`) |
| `DELETE` | `/api/v1/schedules/{id}` | — | Delete schedule (`{ deleted: true, id }`; audits `schedule.delete`) |
| `POST` | `/api/v1/schedules/{id}/toggle` | `{ enabled?: bool }` (optional JSON) | Toggle or explicitly set schedule enabled state (audits `schedule.toggle`) |
| `POST` | `/api/v1/schedules/{id}/run-now` | — | Trigger schedule immediately (audits `schedule.run_now`) |

### Approvals

| Method | Path | Query / Body | Description |
|---|---|---|---|
| `GET` | `/api/v1/approvals` | `?pending_only=false&limit=100` | List `ApprovalRecord` entries |
| `POST` | `/api/v1/approvals/{id}/approve` | — | Approve pending operation |
| `POST` | `/api/v1/approvals/{id}/deny` | — | Deny pending operation |

### Memories

| Method | Path | Query / Body | Description |
|---|---|---|---|
| `GET` | `/api/v1/memories` | `?scope=&namespace=&owner_agent_id=&include_archived=false&limit=500` | Query scoped `MemoryItem` records |
| `POST` | `/api/v1/memories` | `CreateMemoryRequest { scope, namespace, owner_agent_id?, conversation_id?, task_id?, key, content, summary?, importance?, tags? }` | Create/upsert memory item (`201 Created`) |
| `POST` | `/api/v1/memories/import` | JSON array, `{"memories": [...]}`/`{"items": [...]}`, JSONL, or Markdown/text notes | Bulk import memories *(upload tier)* -> `{ "imported": <n> }` |
| `GET` | `/api/v1/memories/export` | — | Export memories (up to `10000`) as `MemoryItem[]` |
| `DELETE` | `/api/v1/memories/{id}` | — | Delete memory item (`{ deleted: true, id }`) |

### Tools & Executions

| Method | Path | Query / Body | Description |
|---|---|---|---|
| `GET` | `/api/v1/tools` | — | List registered `ToolSpec` definitions (built-in, custom, MCP, plugin) *(upload tier)* |
| `POST` | `/api/v1/tools` | Custom tool JSON object, array, `{"tools": [...]}`, or OpenAI function spec | Create or import custom tools (`201 Created` `Vec<ToolSpec>`; *upload tier*; audits `tool.import`) |
| `PATCH` | `/api/v1/tools/{id}` | `UpdateToolPolicyRequest { enabled?, requires_approval?, timeout_secs? }` | Update tool policy fields; returns `ToolSpec` (audits `tool.update_policy`) |
| `DELETE` | `/api/v1/tools/{id}` | — | Delete custom tool (`{ deleted: true, id }`; built-in tools cannot be deleted; `404` if not found) |
| `POST` | `/api/v1/tools/{id}/toggle` | `{ enabled?: bool }` (optional JSON) | Enable/disable a tool |
| `POST` | `/api/v1/tools/{id}/execute` | `ExecuteToolRequest { agent_id?, task_id?, arguments? }` | Execute tool (`409 approval_required` if gated) |
| `GET` | `/api/v1/tool-executions` | `?agent_id=&limit=500` | List `ToolExecutionRecord` history |

### Workspace Files

All paths are constrained inside the primary workspace (`tools.allowed_paths[0]`) with traversal checks.

| Method | Path | Query / Body | Description |
|---|---|---|---|
| `GET` | `/api/v1/files` | `?path=` (optional) | List directory entries -> `WorkspaceFilesResponse { current_path, root_label, entries }` |
| `DELETE` | `/api/v1/files` | `?path=...` (required) | Delete workspace file or directory (`{ deleted: true, path }`) |
| `GET` | `/api/v1/files/content` | `?path=...` (required) | Read UTF-8 text file -> `{ path, name, content, size_bytes, language }` |
| `GET` | `/api/v1/files/download` | `?path=...` (required) | Download raw file (`application/octet-stream`, `Content-Disposition: attachment`) |
| `GET` | `/api/v1/files/preview` | `?path=...` (required) | Preview `.html`/`.htm` (with storage shim) or `.png`/`.jpg`/`.jpeg`/`.webp`/`.gif` image (`400` on unsupported extensions) |
| `POST` | `/api/v1/files/upload` | `UploadWorkspaceFileRequest { path, content?, content_base64? }` | Upload/overwrite file (`201 Created` `WorkspaceFileEntry`; provide either `content` or `content_base64`, not both; *upload tier*) |
| `POST` | `/api/v1/files/mkdir` | `MkdirRequest { path }` | Create directory (`201 Created` `{ created: true, path }`) |

### Extensions (Skills, Plugins, MCP)

| Method | Path | Query / Body | Description |
|---|---|---|---|
| `GET` | `/api/v1/extensions` | — | Return `ExtensionsOverviewResponse { skills, plugins, mcp_servers }` |
| `GET` | `/api/v1/extensions/skills/{id}` | — | Get `SkillDetailResponse { id, name, description, instructions, raw_markdown, resources }` |
| `POST` | `/api/v1/extensions/skills/import` | `ImportSkillRequest { slug?, markdown }` | Import skill `SKILL.md` (`201 Created` `SkillInfoDto`; *upload tier*) |
| `POST` | `/api/v1/extensions/plugins/import` | `ImportPluginRequest { manifest_json }` | Import plugin manifest JSON (`201 Created` `PluginInfoDto`; *upload tier*) |
| `POST` | `/api/v1/extensions/mcp` | `ImportMcpRequest { raw_json }` | Import/configure MCP server(s) (`201 Created` `Vec<McpServerDto>`; *upload tier*) |
| `POST` | `/api/v1/extensions/mcp/{id}/sync` | — | Discover and sync MCP server tools -> `McpServerDto` |
| `PATCH` | `/api/v1/extensions/{kind}/{id}` | `ToggleExtensionRequest { enabled }` | Toggle extension (`kind`: `skill`, `plugin`, or `mcp`) -> `{ kind, id, enabled }` |
| `DELETE` | `/api/v1/extensions/{kind}/{id}` | — | Delete extension (`{ deleted: true, kind, id }`) |

### Sandbox Policy & Stateful Browser Console

| Method | Path | Query / Body | Description |
|---|---|---|---|
| `GET` | `/api/v1/sandbox` | — | Return `SandboxStateDto`: `mode`, `approval_policy`, `network_access`, `allow_local_network`, `web_search_provider`, `searxng_url`, `has_brave_api_key`, `has_tavily_api_key`, `remote_browser_url`, `allow_host_shell`, `max_exec_output_bytes`, `exec_timeout_secs`, `inject_brain_files`, `max_brain_context_chars`, `heartbeat_interval_secs`, `allowed_domains`, `blocked_domains`. Secret keys (`brave_api_key`, `tavily_api_key`) are redacted behind booleans. |
| `PUT` | `/api/v1/sandbox` | `UpdateSandboxRequest` (all optional fields matching `SandboxStateDto` plus `brave_api_key`, `tavily_api_key`) | Update sandbox policy, sync overlapping `[tools]` config, normalize/validate (`allowed_domains` repaired to `["*"]` if empty while `network_access = true`), persist `config.toml`, hot-reload tools/runtime, emit `sandbox_updated` SSE, audit `sandbox.update`, and return `SandboxStateDto`. |
| `GET` | `/api/v1/sandbox/browser/sessions` | — | List active stateful sandbox browser sessions -> `{ "count": usize, "sessions": [...] }`. |
| `POST` | `/api/v1/sandbox/browser/action` | JSON object `{ "action": "<action>", ... }` | Execute a stateful browser action via `SandboxBrowserManager` (`400` if payload is not a JSON object; audits `sandbox.browser.action`). Supported actions: `open`, `navigate`, `snapshot`, `links`, `fill`, `click`, `submit`, `back`, `download`, `save_snapshot`, `sessions`, `close`. |
| `DELETE` | `/api/v1/sandbox/browser/sessions/{id}` | — | Close a stateful browser session -> `{ "closed": true, "deleted": true, "id": id }` (`404` if not found; audits `sandbox.browser.close`). |

### System Doctor Diagnostics

| Method | Path | Query / Body | Description |
|---|---|---|---|
| `GET` | `/api/v1/doctor` | — | Run all 12 system diagnostics (`ps5lm_endpoint`, `storage_integrity`, `workspace_sandbox`, `brain_files`, `sandbox_network_policy`, `auth_csrf_posture`, `builtin_agent_tools`, `extension_directories`, `scheduler_engine`, `memory_bounds`, `ps5_platform_compat`, `meta_release_sync`) and return `DoctorReport { overall_status, checked_at, pass_count, warn_count, fail_count, checks: DoctorCheckResult[] }`. |
| `POST` | `/api/v1/doctor/fix` | — | Run safe idempotent auto-repairs (ensure workspace/sandbox/artifact/extension directories, seed missing default brain files without overwriting, restore missing built-in agents), re-run checks, audit `doctor.fix`, and return `DoctorFixReport { applied_fixes, report }`. |

### Workspace Brain Markdown Files

Brain files live in the primary workspace root (`tools.allowed_paths[0]`). `validate_brain_name` and `brain.rs` restrict paths strictly to canonical targets:
- Root files: `AGENTS.md`, `SOUL.md`, `IDENTITY.md`, `USER.md`, `TOOLS.md`, `MEMORY.md`, `HEARTBEAT.md`
- Daily memory notes: `memory/YYYY-MM-DD.md`
- Workspace rules/workflows: `.agents/rules/<name>.md`, `.agents/workflows/<name>.md` (`[A-Za-z0-9_-]` stem, `1..=96` chars)

Symlinks, NUL bytes, backslashes, drive prefixes, leading `/`, and `..` segments are rejected (`512 KiB` file cap).

| Method | Path | Query / Body | Description |
|---|---|---|---|
| `GET` | `/api/v1/brain` | `?name=` (optional) | When `name` is omitted/empty, returns `BrainFileSummary[]` (`name`, `path`, `exists`, `size_bytes`, `updated_at`, `description`). When `name` is provided, returns `BrainFileDetail`. |
| `PUT` | `/api/v1/brain` | `WriteBrainFileRequest { name: "<path>", content }` | Create/overwrite the named brain file atomically, emit `files_changed` SSE (`action: "write"`), audit `brain.write`, and return `BrainFileDetail { name, path, content, size_bytes, updated_at }`. |
| `GET` | `/api/v1/brain/{*name}` | — | Read brain file at path `{*name}` -> `BrainFileDetail`. |
| `PUT` | `/api/v1/brain/{*name}` | `WriteBrainFileRequest { name?, content }` | Write brain file at path `{*name}` (`400` if optional `body.name` differs from URL path) -> `BrainFileDetail` (emits `files_changed`, audits `brain.write`). |

### Webhooks & Inbound Triggers

| Method | Path | Auth / Body | Description |
|---|---|---|---|
| `GET` | `/api/v1/webhooks` | Session / Bearer | List configured webhooks as `WebhookView[]` (`id`, `name`, `enabled`, `secret_prefix`, `target_agent_id`, `conversation_id`, `prompt_template`, `created_at`, `updated_at`, `last_triggered_at`, `trigger_count`). Full secrets/hashes are never returned. |
| `POST` | `/api/v1/webhooks` | Session / Bearer + CSRF<br>`CreateWebhookRequest { name, target_agent_id, conversation_id?, prompt_template?, enabled? }` | Create inbound webhook (`name` `1..=120` chars, `prompt_template` `<= 16000` chars; `404` if agent or conversation missing). Generates a 256-bit hex secret returned once in `201 Created` `CreateWebhookResponse { webhook, secret }` (audits `webhook.create`). |
| `DELETE` | `/api/v1/webhooks/{id}` | Session / Bearer + CSRF | Delete webhook -> `{ "deleted": true, "id": id }` (`404` if not found; audits `webhook.delete`). |
| `POST` | `/api/v1/webhooks/inbound/{id}` | **Webhook Token Auth** (`x-ps5-webhook-token: <secret>` or `Authorization: Bearer <secret>`)<br>Optional JSON payload (empty/whitespace defaults to `{}`) | Public inbound trigger exempt from cookie/CSRF/Origin checks. Verifies SHA-256 hash in constant time (`401` on missing/empty/duplicate/conflicting headers; `403` on hash mismatch or disabled webhook/target agent; `404` if missing). Expands `{{payload}}` in `prompt_template` (or appends pretty JSON), dispatches background conversation chat task (when `conversation_id` exists) or agent task, increments `trigger_count`, emits `webhook_triggered` SSE, audits `webhook.inbound`, and returns `WebhookTriggerResponse { ok, webhook_id, task_id, message }`. |

### Inference, PS5LM Pass-Through, Logs, Settings & System

Supported `inference.api_mode` values (in global settings and per-agent `model_config.api_mode`):
- `chat_completions` -> `POST /v1/chat/completions`
- `responses` -> `POST /v1/responses`
- `anthropic_messages` (alias `messages`) -> `POST /v1/messages`
- `completions` -> `POST /v1/completions`
- `system_one` (alias `systemone`) -> `POST /v1/systemone`

| Method | Path | Query / Body | Description |
|---|---|---|---|
| `GET` | `/api/v1/inference/models` | — | Query PS5LM `GET /v1/models` and return `{ "models": [...] }` |
| `GET` | `/api/v1/inference/capabilities` | — | Return `Ps5LmCapabilities` (`base_url`, `healthy`, `loaded_model`, `available_models`, `context_length`, `total_slots`, `supports_vision`, `supports_thinking`, `synced_at`) |
| `POST` | `/api/v1/inference/sync` | — | Probe PS5LM (`/health`, `/v1/models`, `/props`, `/slots`), update runtime config if `auto_sync_from_ps5lm = true`, emit `ps5_lm_synced`, return `Ps5LmCapabilities` (audits `inference.sync`) |
| `POST` | `/api/v1/inference/systemone` | `{ model?, state, questions }` | Forward single-pass `SystemOneRequest` (`choice`, `noul`, `score`) to PS5LM `POST /v1/systemone` (audits `inference.systemone`) |
| `GET` | `/api/v1/logs` | `?level=&limit=100` | Read recent in-memory ring-buffer `LogEntry` items |
| `GET` | `/api/v1/audit-logs` | `?limit=100` | Read persistent `AuditLogEntry` records |
| `GET` | `/api/v1/settings` | — | Return sanitized `settings` + config sections (`inference` with `api_key` stripped, `server`, `database`, `agents`, `memory`, `tools`, `logging`, `security`, `runtime`) |
| `PUT` | `/api/v1/settings` | `{ settings?, inference?, server?, database?, agents?, memory?, tools?, sandbox?, logging?, security?, runtime?, ... }` | Update non-sensitive settings (`403` on `auth.*`, `password`, `secret`, `*api_key*`, or `*token*` keys except `timings_per_token`/`tokens`/`*_tokens`; use `PUT /api/v1/sandbox` for Brave/Tavily search keys). Reconciles `[sandbox]`/`[tools]` and persists `config.toml` (audits `settings.update`). |
| `POST` | `/api/v1/system/checkpoint` | — | Flush/checkpoint storage backend (`{ checkpointed: true }`; audits `system.checkpoint`) |
| `POST` | `/api/v1/system/shutdown` | — | Trigger graceful daemon shutdown (`202 Accepted` `{ shutting_down: true }`; audits `system.shutdown`) |

### Dashboard Static Routes

| Method | Path | Auth | Content-Type |
|---|---|---|---|
| `GET` | `/` | Public | `text/html; charset=utf-8` |
| `GET` | `/styles.css` | Public | `text/css; charset=utf-8` |
| `GET` | `/app.js` | Public | `application/javascript; charset=utf-8` |
