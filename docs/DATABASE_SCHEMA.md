# PS5 Agent Database Schema & Persistence Architecture

This document defines the persistence layer implemented in `crates/ps5-agent-database` (`SqliteStorage`, `JsonlStorage`, `InstrumentedStorage`, `DbMetrics`, `open_storage`) and consumed by `crates/ps5-agent-memory` (`MemoryManager`).

## 1. Storage Driver Selection (`open_storage`)

`open_storage(cfg: &DatabaseConfig, data_dir: &Path)` selects the backend according to `cfg.driver`:

| `cfg.driver` | Behavior |
|---|---|
| `"sqlite"` | Opens `SqliteStorage` at `cfg.path` and runs versioned migrations (`run_migrations`). Fails if SQLite cannot open/migrate. |
| `"jsonl"` | Opens `JsonlStorage` under `<data_dir>/jsonl_db/` and ensures snapshot/metadata state is initialized. |
| `"auto"` | Attempts `SqliteStorage` at `cfg.path` first; if opening or migrating SQLite fails (e.g., restricted filesystem mount), logs a warning and falls back to `JsonlStorage` under `<data_dir>/jsonl_db/`. |

All opened backends are wrapped in `InstrumentedStorage<S>`, which tracks `DbMetrics` (`ops_total`, `total_us`, `errors_total`, and `avg_us()`) via lock-free `AtomicU64` counters.

---

## 2. PS5-Safe SQLite Configuration & PRAGMAs

On the PS5 payload environment (`x86_64-sie-ps5-std`), `/data` writes at ~1–2 MB/s and does not support symlinks (`EOPNOTSUPP`), and `sqlite3.c` is compiled with `-include include/ps5_sqlite_shim.h` (`__ps5_*11` FreeBSD 11 `stat` struct shim).

`SqliteStorage::open` configures:
- Prepared statement cache capacity: `conn.set_prepared_statement_cache_capacity(64)`
- `PRAGMA busy_timeout = <cfg.busy_timeout_ms>;` (default `5000`)
- `PRAGMA foreign_keys = ON;`
- `PRAGMA locking_mode = EXCLUSIVE;` (single-process payload ownership, eliminates multi-process lock overhead)
- `PRAGMA journal_mode = WAL;` when `cfg.wal_mode == true`, **automatically falling back to `PRAGMA journal_mode = TRUNCATE;`** if setting WAL errors or returns a non-`wal` mode (excluding `:memory:` databases). When `cfg.wal_mode == false`, sets `PRAGMA journal_mode = TRUNCATE;`.
- `PRAGMA synchronous = NORMAL;`
- `PRAGMA cache_size = -<abs(cfg.cache_size_kb)>;` (negative KiB count; default `-4000`, ~4 MiB)

### Maintenance Operations
- `checkpoint()`: Executes `PRAGMA wal_checkpoint(TRUNCATE); PRAGMA optimize;` to flush WAL frames back into the main database file before shutdown or rest-mode suspend.
- `integrity_check()`: Executes `PRAGMA integrity_check;` and returns `"ok"` on a healthy database (or diagnostic lines if issues are reported).

---

## 3. Migration Versioning (`schema_migrations`)

Migrations are tracked in the `schema_migrations` table and applied idempotently inside a SQLite transaction by `run_migrations()`:

```sql
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    INTEGER PRIMARY KEY,
    name       TEXT NOT NULL,
    applied_at TEXT NOT NULL
);
```

| Version | Name | Summary |
|---|---|---|
| `1` | `001_initial_schema` | Creates all 13 domain tables and their secondary indexes. |

Upserts across all parent tables use `INSERT INTO ... ON CONFLICT(id) DO UPDATE SET ...` rather than `INSERT OR REPLACE` so updating a parent row (`conversations`, `tasks`, etc.) never triggers unintended `ON DELETE CASCADE` deletions on child rows (`messages`, `task_events`).

---

## 4. Relational Schema (13 Domain Tables)

### 4.1 `agents`
Stores `AgentDefinition` configurations.

| Column | Type | Constraints / Notes |
|---|---|---|
| `id` | `TEXT` | `PRIMARY KEY` |
| `name` | `TEXT` | `NOT NULL` |
| `description` | `TEXT` | `NOT NULL` |
| `role` | `TEXT` | `NOT NULL` (JSON-serialized `AgentRole`) |
| `role_label` | `TEXT` | `NOT NULL` |
| `system_prompt` | `TEXT` | `NOT NULL` |
| `model_config_json` | `TEXT` | `NOT NULL` (`AgentModelConfig` JSON) |
| `enabled_tools_json` | `TEXT` | `NOT NULL` (`Vec<String>` JSON) |
| `memory_permissions_json` | `TEXT` | `NOT NULL` (`MemoryPermissions` JSON) |
| `max_task_duration_secs` | `INTEGER` | `NOT NULL` |
| `max_reasoning_iterations` | `INTEGER` | `NOT NULL` |
| `priority` | `INTEGER` | `NOT NULL` |
| `can_communicate` | `INTEGER` | `NOT NULL` (`0` or `1`) |
| `enabled` | `INTEGER` | `NOT NULL` (`0` or `1`) |
| `is_builtin` | `INTEGER` | `NOT NULL` (`0` or `1`) |
| `is_protected` | `INTEGER` | `NOT NULL` (`0` or `1`) |
| `is_temporary` | `INTEGER` | `NOT NULL` (`0` or `1`) |
| `created_at` | `TEXT` | `NOT NULL` (RFC 3339) |
| `updated_at` | `TEXT` | `NOT NULL` (RFC 3339) |

**Indexes:**
- `idx_agents_enabled_priority` on `agents(enabled, priority DESC, created_at ASC)`

---

### 4.2 `conversations`
Stores `Conversation` headers.

| Column | Type | Constraints / Notes |
|---|---|---|
| `id` | `TEXT` | `PRIMARY KEY` |
| `title` | `TEXT` | `NOT NULL` |
| `mode_json` | `TEXT` | `NOT NULL` (`ConversationMode` JSON) |
| `created_at` | `TEXT` | `NOT NULL` |
| `updated_at` | `TEXT` | `NOT NULL` |
| `archived` | `INTEGER` | `NOT NULL DEFAULT 0` |

**Indexes:**
- `idx_conversations_archived_updated` on `conversations(archived, updated_at DESC)`

---

### 4.3 `messages`
Stores `ChatMessage` turns within conversations.

| Column | Type | Constraints / Notes |
|---|---|---|
| `id` | `TEXT` | `PRIMARY KEY` |
| `conversation_id` | `TEXT` | `NOT NULL REFERENCES conversations(id) ON DELETE CASCADE` |
| `role` | `TEXT` | `NOT NULL` (`user`, `assistant`, `system`, `tool`) |
| `agent_id` | `TEXT` | Nullable |
| `content` | `TEXT` | `NOT NULL` |
| `reasoning` | `TEXT` | Nullable |
| `tool_calls_json` | `TEXT` | Nullable JSON |
| `task_id` | `TEXT` | Nullable |
| `token_count` | `INTEGER` | `NOT NULL` |
| `latency_ms` | `INTEGER` | Nullable |
| `archived` | `INTEGER` | `NOT NULL DEFAULT 0` |
| `created_at` | `TEXT` | `NOT NULL` |

**Indexes:**
- `idx_messages_conv_arch_created` on `messages(conversation_id, archived, created_at ASC)`
- `idx_messages_task_id` on `messages(task_id)`

---

### 4.4 `tasks`
Stores `TaskRecord` execution state.

| Column | Type | Constraints / Notes |
|---|---|---|
| `id` | `TEXT` | `PRIMARY KEY` |
| `conversation_id` | `TEXT` | Nullable |
| `user_request` | `TEXT` | `NOT NULL` |
| `assigned_agent_id` | `TEXT` | `NOT NULL` |
| `parent_task_id` | `TEXT` | Nullable |
| `priority` | `INTEGER` | `NOT NULL` |
| `created_at` | `TEXT` | `NOT NULL` |
| `started_at` | `TEXT` | Nullable |
| `completed_at` | `TEXT` | Nullable |
| `status` | `TEXT` | `NOT NULL` (`TaskStatus`) |
| `result` | `TEXT` | Nullable |
| `error_info` | `TEXT` | Nullable |
| `has_side_effects` | `INTEGER` | `NOT NULL` |
| `progress_pct` | `INTEGER` | `NOT NULL` |
| `progress_note` | `TEXT` | Nullable |

**Indexes:**
- `idx_tasks_status_created` on `tasks(status, created_at DESC)`
- `idx_tasks_agent_status` on `tasks(assigned_agent_id, status)`
- `idx_tasks_conversation` on `tasks(conversation_id)`

---

### 4.5 `task_events`
Stores chronological `TaskEvent` audit trail entries for a task.

| Column | Type | Constraints / Notes |
|---|---|---|
| `id` | `TEXT` | `PRIMARY KEY` |
| `task_id` | `TEXT` | `NOT NULL REFERENCES tasks(id) ON DELETE CASCADE` |
| `timestamp` | `TEXT` | `NOT NULL` |
| `event_type` | `TEXT` | `NOT NULL` |
| `details` | `TEXT` | `NOT NULL` |

**Indexes:**
- `idx_task_events_task_ts` on `task_events(task_id, timestamp ASC)`

---

### 4.6 `agent_messages`
Stores inter-agent `AgentEnvelope` messages.

| Column | Type | Constraints / Notes |
|---|---|---|
| `id` | `TEXT` | `PRIMARY KEY` |
| `sender_agent_id` | `TEXT` | `NOT NULL` |
| `recipient_agent_id` | `TEXT` | `NOT NULL` |
| `task_id` | `TEXT` | Nullable |
| `timestamp` | `TEXT` | `NOT NULL` |
| `message_type` | `TEXT` | `NOT NULL` (`AgentMessageType`) |
| `content` | `TEXT` | `NOT NULL` |
| `status` | `TEXT` | `NOT NULL` (`DeliveryStatus`) |
| `hop_count` | `INTEGER` | `NOT NULL` |

**Indexes:**
- `idx_agent_messages_task_ts` on `agent_messages(task_id, timestamp DESC)`
- `idx_agent_messages_sender` on `agent_messages(sender_agent_id, timestamp DESC)`
- `idx_agent_messages_recipient` on `agent_messages(recipient_agent_id, timestamp DESC)`

---

### 4.7 `memories`
Stores `MemoryItem` records across all 5 scopes (`conversation`, `agent`, `project`, `task`, `user_preference`).

| Column | Type | Constraints / Notes |
|---|---|---|
| `id` | `TEXT` | `PRIMARY KEY` |
| `scope` | `TEXT` | `NOT NULL` (`MemoryScope`) |
| `namespace` | `TEXT` | `NOT NULL` |
| `owner_agent_id` | `TEXT` | Nullable |
| `conversation_id` | `TEXT` | Nullable |
| `task_id` | `TEXT` | Nullable |
| `key` | `TEXT` | `NOT NULL` |
| `content` | `TEXT` | `NOT NULL` |
| `summary` | `TEXT` | Nullable |
| `importance` | `INTEGER` | `NOT NULL` (`0..=10`) |
| `tags_json` | `TEXT` | `NOT NULL` (`Vec<String>` JSON) |
| `archived` | `INTEGER` | `NOT NULL DEFAULT 0` |
| `created_at` | `TEXT` | `NOT NULL` |
| `updated_at` | `TEXT` | `NOT NULL` |

**Indexes:**
- `idx_memories_scope_ns_arch` on `memories(scope, namespace, archived, importance DESC, updated_at DESC)`
- `idx_memories_owner_arch` on `memories(owner_agent_id, archived, importance DESC)`
- `idx_memories_conv` on `memories(conversation_id, archived)`
- `idx_memories_task` on `memories(task_id, archived)`

`memory_stats_for_agent(agent_id)` returns `(COUNT(*), SUM(LENGTH(CAST(content AS BLOB))))` for all rows where `owner_agent_id = agent_id`.

---

### 4.8 `tool_registry`
Stores `ToolSpec` definitions.

| Column | Type | Constraints / Notes |
|---|---|---|
| `id` | `TEXT` | `PRIMARY KEY` |
| `name` | `TEXT` | `NOT NULL` |
| `category` | `TEXT` | `NOT NULL` |
| `description` | `TEXT` | `NOT NULL` |
| `input_schema_json` | `TEXT` | `NOT NULL` |
| `output_schema_json` | `TEXT` | `NOT NULL` |
| `required_permissions_json` | `TEXT` | `NOT NULL` (`Vec<ToolPermission>` JSON) |
| `requires_approval` | `INTEGER` | `NOT NULL` |
| `is_mutating` | `INTEGER` | `NOT NULL` |
| `enabled` | `INTEGER` | `NOT NULL` |
| `timeout_secs` | `INTEGER` | `NOT NULL` |

**Indexes:**
- `idx_tool_registry_cat_name` on `tool_registry(category, name)`

---

### 4.9 `tool_executions`
Stores `ToolExecutionRecord` invocations.

| Column | Type | Constraints / Notes |
|---|---|---|
| `id` | `TEXT` | `PRIMARY KEY` |
| `tool_id` | `TEXT` | `NOT NULL` |
| `agent_id` | `TEXT` | `NOT NULL` |
| `task_id` | `TEXT` | Nullable |
| `input_json` | `TEXT` | `NOT NULL` |
| `output_json` | `TEXT` | Nullable |
| `success` | `INTEGER` | `NOT NULL` |
| `error` | `TEXT` | Nullable |
| `duration_ms` | `INTEGER` | `NOT NULL` |
| `created_at` | `TEXT` | `NOT NULL` |

**Indexes:**
- `idx_tool_exec_agent_created` on `tool_executions(agent_id, created_at DESC)`
- `idx_tool_exec_tool_created` on `tool_executions(tool_id, created_at DESC)`
- `idx_tool_exec_task` on `tool_executions(task_id)`

---

### 4.10 `settings`
Stores runtime key-value settings.

| Column | Type | Constraints / Notes |
|---|---|---|
| `key` | `TEXT` | `PRIMARY KEY` |
| `value` | `TEXT` | `NOT NULL` |
| `updated_at` | `TEXT` | `NOT NULL` |

---

### 4.11 `audit_logs`
Stores `AuditLogEntry` security and lifecycle events.

| Column | Type | Constraints / Notes |
|---|---|---|
| `id` | `TEXT` | `PRIMARY KEY` |
| `timestamp` | `TEXT` | `NOT NULL` |
| `actor` | `TEXT` | `NOT NULL` |
| `action` | `TEXT` | `NOT NULL` |
| `target` | `TEXT` | `NOT NULL` |
| `severity` | `TEXT` | `NOT NULL` |
| `outcome` | `TEXT` | `NOT NULL` |
| `details_json` | `TEXT` | `NOT NULL` |

**Indexes:**
- `idx_audit_logs_ts` on `audit_logs(timestamp DESC)`
- `idx_audit_logs_actor_action` on `audit_logs(actor, action)`

---

### 4.12 `sessions`
Stores `SessionRecord` HTTP authentication sessions.

| Column | Type | Constraints / Notes |
|---|---|---|
| `id` | `TEXT` | `PRIMARY KEY` |
| `token_sha256` | `TEXT` | `NOT NULL UNIQUE` |
| `csrf_token` | `TEXT` | `NOT NULL` |
| `username` | `TEXT` | `NOT NULL` |
| `client_ip` | `TEXT` | `NOT NULL` |
| `created_at` | `TEXT` | `NOT NULL` |
| `expires_at` | `TEXT` | `NOT NULL` |
| `revoked` | `INTEGER` | `NOT NULL DEFAULT 0` |

**Indexes:**
- `idx_sessions_token_hash` on `sessions(token_sha256)`
- `idx_sessions_expires` on `sessions(expires_at)`

---

### 4.13 `approvals`
Stores `ApprovalRecord` human-in-the-loop authorization requests.

| Column | Type | Constraints / Notes |
|---|---|---|
| `id` | `TEXT` | `PRIMARY KEY` |
| `task_id` | `TEXT` | Nullable |
| `agent_id` | `TEXT` | `NOT NULL` |
| `operation` | `TEXT` | `NOT NULL` |
| `tool_id` | `TEXT` | Nullable |
| `arguments_json` | `TEXT` | `NOT NULL` |
| `reason` | `TEXT` | `NOT NULL` |
| `status` | `TEXT` | `NOT NULL` (`ApprovalStatus`) |
| `created_at` | `TEXT` | `NOT NULL` |
| `resolved_at` | `TEXT` | Nullable |
| `resolved_by` | `TEXT` | Nullable |

**Indexes:**
- `idx_approvals_status_created` on `approvals(status, created_at DESC)`
- `idx_approvals_task` on `approvals(task_id)`

---

## 5. JSONL Fallback Storage Format (`<data_dir>/jsonl_db/`)

`JsonlStorage` implements the exact same `StorageBackend` semantics (including foreign-key checks for `messages -> conversations` and `task_events -> tasks`, cascade deletes on `delete_conversation`, duplicate conflict detection on `create_conversation`, and deterministic tie-broken sorting via a monotonic `next_seq` counter).

### Directory Layout
```text
<data_dir>/jsonl_db/
├── state.json            # Atomic temp+rename full state snapshot
├── messages.jsonl        # Append-only log of ChatMessage inserts
├── task_events.jsonl     # Append-only log of TaskEvent inserts
├── agent_messages.jsonl  # Append-only log of AgentEnvelope inserts
├── tool_executions.jsonl # Append-only log of ToolExecutionRecord inserts
└── audit_logs.jsonl      # Append-only log of AuditLogEntry inserts
```

### Crash-Safety & Atomic Writes
- **State Snapshot (`state.json`)**: Written to `state.json.tmp`, flushed (`sync_all`), and atomically moved over `state.json` via `fs::rename`.
- **Append-Only Logs (`*.jsonl`)**: Opened with `OpenOptions::new().create(true).append(true)` and written as one JSON object per line (`\n`), then flushed.
- **Integrity Check (`integrity_check`)**: Parses `state.json` (when present) as `JsonlState` and validates every non-empty line in all `*.jsonl` files as valid JSON, returning `"ok"` if valid or an `AgentError::Storage` describing the corruption.
- **Checkpoint (`checkpoint`)**: Atomically flushes and re-snapshots `state.json`.
