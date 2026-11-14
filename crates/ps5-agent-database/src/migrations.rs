use ps5_agent_core::{now_iso, AgentError, Result};
use rusqlite::{params, Connection};

pub const CURRENT_SCHEMA_VERSION: i64 = 5;

const MIGRATION_001_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS agents (
    id                      TEXT PRIMARY KEY,
    name                    TEXT NOT NULL,
    description             TEXT NOT NULL,
    role                    TEXT NOT NULL,
    role_label              TEXT NOT NULL,
    system_prompt           TEXT NOT NULL,
    model_config_json       TEXT NOT NULL,
    enabled_tools_json      TEXT NOT NULL,
    memory_permissions_json TEXT NOT NULL,
    max_task_duration_secs  INTEGER NOT NULL,
    max_reasoning_iterations INTEGER NOT NULL,
    priority                INTEGER NOT NULL,
    can_communicate         INTEGER NOT NULL,
    enabled                 INTEGER NOT NULL,
    is_builtin              INTEGER NOT NULL,
    is_protected            INTEGER NOT NULL,
    is_temporary            INTEGER NOT NULL,
    created_at              TEXT NOT NULL,
    updated_at              TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_agents_enabled_priority
    ON agents(enabled, priority DESC, created_at ASC);

CREATE TABLE IF NOT EXISTS conversations (
    id          TEXT PRIMARY KEY,
    title       TEXT NOT NULL,
    mode_json   TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    archived    INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_conversations_archived_updated
    ON conversations(archived, updated_at DESC);

CREATE TABLE IF NOT EXISTS messages (
    id              TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    role            TEXT NOT NULL,
    agent_id        TEXT,
    content         TEXT NOT NULL,
    reasoning       TEXT,
    tool_calls_json TEXT,
    task_id         TEXT,
    token_count     INTEGER NOT NULL,
    latency_ms      INTEGER,
    archived        INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_messages_conv_arch_created
    ON messages(conversation_id, archived, created_at ASC);
CREATE INDEX IF NOT EXISTS idx_messages_task_id
    ON messages(task_id);

CREATE TABLE IF NOT EXISTS tasks (
    id                TEXT PRIMARY KEY,
    conversation_id   TEXT,
    user_request      TEXT NOT NULL,
    assigned_agent_id TEXT NOT NULL,
    parent_task_id    TEXT,
    priority          INTEGER NOT NULL,
    created_at        TEXT NOT NULL,
    started_at        TEXT,
    completed_at      TEXT,
    status            TEXT NOT NULL,
    result            TEXT,
    error_info        TEXT,
    has_side_effects  INTEGER NOT NULL,
    progress_pct      INTEGER NOT NULL,
    progress_note     TEXT
);
CREATE INDEX IF NOT EXISTS idx_tasks_status_created
    ON tasks(status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_tasks_agent_status
    ON tasks(assigned_agent_id, status);
CREATE INDEX IF NOT EXISTS idx_tasks_conversation
    ON tasks(conversation_id);

CREATE TABLE IF NOT EXISTS task_events (
    id         TEXT PRIMARY KEY,
    task_id    TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    timestamp  TEXT NOT NULL,
    event_type TEXT NOT NULL,
    details    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_task_events_task_ts
    ON task_events(task_id, timestamp ASC);

CREATE TABLE IF NOT EXISTS agent_messages (
    id                 TEXT PRIMARY KEY,
    sender_agent_id    TEXT NOT NULL,
    recipient_agent_id TEXT NOT NULL,
    task_id            TEXT,
    timestamp          TEXT NOT NULL,
    message_type       TEXT NOT NULL,
    content            TEXT NOT NULL,
    status             TEXT NOT NULL,
    hop_count          INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_agent_messages_task_ts
    ON agent_messages(task_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_agent_messages_sender
    ON agent_messages(sender_agent_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_agent_messages_recipient
    ON agent_messages(recipient_agent_id, timestamp DESC);

CREATE TABLE IF NOT EXISTS memories (
    id              TEXT PRIMARY KEY,
    scope           TEXT NOT NULL,
    namespace       TEXT NOT NULL,
    owner_agent_id  TEXT,
    conversation_id TEXT,
    task_id         TEXT,
    key             TEXT NOT NULL,
    content         TEXT NOT NULL,
    summary         TEXT,
    importance      INTEGER NOT NULL,
    tags_json       TEXT NOT NULL,
    archived        INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_memories_scope_ns_arch
    ON memories(scope, namespace, archived, importance DESC, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_memories_owner_arch
    ON memories(owner_agent_id, archived, importance DESC);
CREATE INDEX IF NOT EXISTS idx_memories_conv
    ON memories(conversation_id, archived);
CREATE INDEX IF NOT EXISTS idx_memories_task
    ON memories(task_id, archived);

CREATE TABLE IF NOT EXISTS tool_registry (
    id                        TEXT PRIMARY KEY,
    name                      TEXT NOT NULL,
    category                  TEXT NOT NULL,
    description               TEXT NOT NULL,
    input_schema_json         TEXT NOT NULL,
    output_schema_json        TEXT NOT NULL,
    required_permissions_json TEXT NOT NULL,
    requires_approval         INTEGER NOT NULL,
    is_mutating               INTEGER NOT NULL,
    enabled                   INTEGER NOT NULL,
    timeout_secs              INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tool_registry_cat_name
    ON tool_registry(category, name);

CREATE TABLE IF NOT EXISTS tool_executions (
    id          TEXT PRIMARY KEY,
    tool_id     TEXT NOT NULL,
    agent_id    TEXT NOT NULL,
    task_id     TEXT,
    input_json  TEXT NOT NULL,
    output_json TEXT,
    success     INTEGER NOT NULL,
    error       TEXT,
    duration_ms INTEGER NOT NULL,
    created_at  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tool_exec_agent_created
    ON tool_executions(agent_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_tool_exec_tool_created
    ON tool_executions(tool_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_tool_exec_task
    ON tool_executions(task_id);

CREATE TABLE IF NOT EXISTS settings (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_logs (
    id           TEXT PRIMARY KEY,
    timestamp    TEXT NOT NULL,
    actor        TEXT NOT NULL,
    action       TEXT NOT NULL,
    target       TEXT NOT NULL,
    severity     TEXT NOT NULL,
    outcome      TEXT NOT NULL,
    details_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_audit_logs_ts
    ON audit_logs(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_audit_logs_actor_action
    ON audit_logs(actor, action);

CREATE TABLE IF NOT EXISTS sessions (
    id           TEXT PRIMARY KEY,
    token_sha256 TEXT NOT NULL UNIQUE,
    csrf_token   TEXT NOT NULL,
    username     TEXT NOT NULL,
    client_ip    TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    expires_at   TEXT NOT NULL,
    revoked      INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_sessions_token_hash
    ON sessions(token_sha256);
CREATE INDEX IF NOT EXISTS idx_sessions_expires
    ON sessions(expires_at);

CREATE TABLE IF NOT EXISTS approvals (
    id             TEXT PRIMARY KEY,
    task_id        TEXT,
    agent_id       TEXT NOT NULL,
    operation      TEXT NOT NULL,
    tool_id        TEXT,
    arguments_json TEXT NOT NULL,
    reason         TEXT NOT NULL,
    status         TEXT NOT NULL,
    created_at     TEXT NOT NULL,
    resolved_at    TEXT,
    resolved_by    TEXT
);
CREATE INDEX IF NOT EXISTS idx_approvals_status_created
    ON approvals(status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_approvals_task
    ON approvals(task_id);
"#;

const MIGRATION_002_SQL: &str = r#"
ALTER TABLE messages ADD COLUMN attachments_json TEXT;
CREATE INDEX IF NOT EXISTS idx_tasks_agent_status_created
    ON tasks(assigned_agent_id, status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_messages_archived_created
    ON messages(archived, created_at);
CREATE INDEX IF NOT EXISTS idx_approvals_status_resolved
    ON approvals(status, resolved_at);
CREATE INDEX IF NOT EXISTS idx_tool_exec_created
    ON tool_executions(created_at);
CREATE INDEX IF NOT EXISTS idx_task_events_ts
    ON task_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_agent_messages_ts
    ON agent_messages(timestamp);
"#;

const MIGRATION_003_SQL: &str = r#"
ALTER TABLE conversations ADD COLUMN sort_order INTEGER NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS idx_conversations_sort_updated
    ON conversations(archived, sort_order ASC, updated_at DESC);
"#;

const MIGRATION_004_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS schedules (
    id                   TEXT PRIMARY KEY,
    title                TEXT NOT NULL,
    prompt               TEXT NOT NULL,
    target_agent_id      TEXT,
    conversation_id      TEXT,
    kind                 TEXT NOT NULL,
    run_at               TEXT,
    interval_secs        INTEGER,
    daily_time           TEXT,
    cron_expr            TEXT,
    timezone_offset_mins INTEGER NOT NULL DEFAULT 0,
    enabled              INTEGER NOT NULL DEFAULT 1,
    next_run_at          TEXT,
    last_run_at          TEXT,
    last_status          TEXT,
    last_error           TEXT,
    last_task_id         TEXT,
    run_count            INTEGER NOT NULL DEFAULT 0,
    created_at           TEXT NOT NULL,
    updated_at           TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_schedules_enabled_next
    ON schedules(enabled, next_run_at);
"#;

const MIGRATION_005_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS webhooks (
    id                TEXT PRIMARY KEY,
    name              TEXT NOT NULL,
    enabled           INTEGER NOT NULL DEFAULT 1,
    secret_hash       TEXT NOT NULL,
    secret_prefix     TEXT NOT NULL,
    target_agent_id   TEXT NOT NULL,
    conversation_id   TEXT,
    prompt_template   TEXT NOT NULL,
    created_at        TEXT NOT NULL,
    updated_at        TEXT NOT NULL,
    last_triggered_at TEXT,
    trigger_count     INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_webhooks_enabled
    ON webhooks(enabled);
"#;

const MIGRATIONS: &[(i64, &str, &str)] = &[
    (1, "001_initial_schema", MIGRATION_001_SQL),
    (2, "002_chat_attachments_and_retention", MIGRATION_002_SQL),
    (3, "003_conversation_sort_order", MIGRATION_003_SQL),
    (4, "004_schedules", MIGRATION_004_SQL),
    (5, "005_sandbox_webhooks_agent_policy", MIGRATION_005_SQL),
];

fn column_exists(conn: &Connection, table: &str, column: &str) -> Result<bool> {
    let sql = format!("PRAGMA table_info({table})");
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|e| AgentError::Storage(format!("checking table_info({table}): {e}")))?;
    let rows = stmt
        .query_map([], |r| r.get::<_, String>(1))
        .map_err(|e| AgentError::Storage(format!("reading table_info({table}): {e}")))?;
    for col_res in rows {
        let name =
            col_res.map_err(|e| AgentError::Storage(format!("iterating table_info({table}): {e}")))?;
        if name.eq_ignore_ascii_case(column) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn ensure_column_exists(
    conn: &Connection,
    table: &str,
    column: &str,
    col_def: &str,
) -> Result<()> {
    if !column_exists(conn, table, column)? {
        conn.execute_batch(&format!("ALTER TABLE {table} ADD COLUMN {column} {col_def};"))
            .map_err(|e| AgentError::Storage(format!("adding column {table}.{column}: {e}")))?;
    }
    Ok(())
}

pub fn run_sqlite_migrations(conn: &Connection) -> Result<()> {
    let page_count: i64 = conn
        .query_row("PRAGMA page_count", [], |r| r.get(0))
        .unwrap_or(1);
    if page_count == 0 {
        let _ = conn.execute_batch("PRAGMA auto_vacuum = INCREMENTAL;");
    }

    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS schema_migrations (
            version    INTEGER PRIMARY KEY,
            name       TEXT NOT NULL,
            applied_at TEXT NOT NULL
        );",
    )
    .map_err(|e| AgentError::Storage(format!("creating schema_migrations: {e}")))?;

    for &(version, name, sql) in MIGRATIONS {
        let already_applied: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = ?1)",
                params![version],
                |r| r.get(0),
            )
            .map_err(|e| AgentError::Storage(format!("checking migration {version}: {e}")))?;

        if already_applied {
            continue;
        }

        let tx = conn
            .unchecked_transaction()
            .map_err(|e| AgentError::Storage(format!("begin migration {version}: {e}")))?;
        if version == 5 {
            ensure_column_exists(&tx, "agents", "denied_tools_json", "TEXT NOT NULL DEFAULT '[]'")?;
            ensure_column_exists(&tx, "agents", "sandbox_mode", "TEXT")?;
            ensure_column_exists(&tx, "agents", "tool_profile", "TEXT")?;
        }
        tx.execute_batch(sql)
            .map_err(|e| AgentError::Storage(format!("applying migration {version} ({name}): {e}")))?;
        tx.execute(
            "INSERT INTO schema_migrations (version, name, applied_at) VALUES (?1, ?2, ?3)",
            params![version, name, now_iso()],
        )
        .map_err(|e| AgentError::Storage(format!("recording migration {version}: {e}")))?;
        tx.commit()
            .map_err(|e| AgentError::Storage(format!("commit migration {version}: {e}")))?;
    }

    Ok(())
}
