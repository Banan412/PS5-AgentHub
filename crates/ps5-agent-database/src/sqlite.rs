use crate::migrations::run_sqlite_migrations;
use ps5_agent_config::DatabaseConfig;
use ps5_agent_core::*;
use rusqlite::{params, Connection, ErrorCode, OptionalExtension, Row};
use serde::Serialize;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

fn sidecar_path(path: &Path, suffix: &str) -> PathBuf {
    let mut os: OsString = path.as_os_str().to_os_string();
    os.push(suffix);
    PathBuf::from(os)
}

pub struct SqliteStorage {
    conn: Mutex<Connection>,
}

impl SqliteStorage {
    pub fn open(path: &Path, cfg: &DatabaseConfig) -> Result<Self> {
        let is_memory = path.as_os_str() == ":memory:";
        if !is_memory {
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    fs::create_dir_all(parent).map_err(|e| {
                        AgentError::Storage(format!("creating db dir {}: {e}", parent.display()))
                    })?;
                }
            }
        }

        let effective_wal = cfg.wal_mode && !cfg!(target_vendor = "sie");
        let shm_path = sidecar_path(path, "-shm");
        let wal_path = sidecar_path(path, "-wal");

        if !is_memory && !effective_wal && shm_path.exists() {
            let wal_missing_or_empty = fs::metadata(&wal_path).map_or(true, |m| m.len() == 0);
            if wal_missing_or_empty {
                ps5_agent_logging::log_warn(
                    "database",
                    format!(
                        "Removing stale SQLite SHM file before open: {}",
                        shm_path.display()
                    ),
                );
                let _ = fs::remove_file(&shm_path);
            }
        }

        const MAX_OPEN_ATTEMPTS: usize = 5;
        const OPEN_BACKOFF: std::time::Duration = std::time::Duration::from_millis(150);

        let mut shm_removed = false;
        let mut attempt = 0usize;
        let conn = loop {
            attempt += 1;
            match open_and_configure(path, cfg, is_memory) {
                Ok(c) => break c,
                Err(failure) if !is_memory && !shm_removed && shm_path.exists() => {
                    ps5_agent_logging::log_warn(
                        "database",
                        format!(
                            "SQLite open/pragma failed ({}) while {} exists; removing stale SHM and retrying",
                            failure.err,
                            shm_path.display()
                        ),
                    );
                    let _ = fs::remove_file(&shm_path);
                    shm_removed = true;
                    if failure.transient && attempt < MAX_OPEN_ATTEMPTS {
                        std::thread::sleep(OPEN_BACKOFF);
                    }
                }
                Err(failure) if failure.transient && attempt < MAX_OPEN_ATTEMPTS => {
                    ps5_agent_logging::log_warn(
                        "database",
                        format!(
                            "SQLite open/pragma transient error on attempt {attempt}/{MAX_OPEN_ATTEMPTS} ({}); retrying in {}ms",
                            failure.err,
                            OPEN_BACKOFF.as_millis()
                        ),
                    );
                    std::thread::sleep(OPEN_BACKOFF);
                }
                Err(failure) => return Err(failure.err),
            }
        };

        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    fn lock_conn(&self) -> Result<MutexGuard<'_, Connection>> {
        self.conn
            .lock()
            .map_err(|_| AgentError::Internal("sqlite mutex poisoned".into()))
    }
}

struct OpenFailure {
    err: AgentError,
    transient: bool,
}

fn is_transient_rusqlite_open_error(err: &rusqlite::Error) -> bool {
    matches!(
        err,
        rusqlite::Error::SqliteFailure(ffi_err, _)
            if matches!(
                ffi_err.code,
                ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked | ErrorCode::CannotOpen
            )
    )
}

pub(crate) fn is_transient_busy_or_locked_error(err: &AgentError) -> bool {
    match err {
        AgentError::Storage(msg) => {
            let lower = msg.to_ascii_lowercase();
            lower.contains("database is locked")
                || lower.contains("database table is locked")
                || lower.contains("database busy")
                || lower.contains("database locked")
                || lower.contains("sqlite_busy")
                || lower.contains("sqlite_locked")
        }
        _ => false,
    }
}

fn open_err(ctx: &str, err: rusqlite::Error) -> OpenFailure {
    OpenFailure {
        transient: is_transient_rusqlite_open_error(&err),
        err: AgentError::Storage(format!("{ctx}: {err}")),
    }
}

fn open_and_configure(
    path: &Path,
    cfg: &DatabaseConfig,
    is_memory: bool,
) -> std::result::Result<Connection, OpenFailure> {
    let conn = Connection::open(path)
        .map_err(|e| open_err(&format!("opening sqlite {}", path.display()), e))?;
    conn.set_prepared_statement_cache_capacity(128);
    configure_pragmas(&conn, cfg, is_memory)?;
    Ok(conn)
}

fn configure_pragmas(
    conn: &Connection,
    cfg: &DatabaseConfig,
    is_memory: bool,
) -> std::result::Result<(), OpenFailure> {
    let cache_kib = -(cfg.cache_size_kb.abs().max(1));
    let effective_wal = cfg.wal_mode && !cfg!(target_vendor = "sie");
    let initial_locking = if !effective_wal && !is_memory {
        "EXCLUSIVE"
    } else {
        "NORMAL"
    };
    conn.execute_batch(&format!(
        "PRAGMA busy_timeout = {};
         PRAGMA foreign_keys = ON;
         PRAGMA locking_mode = {};
         PRAGMA synchronous = NORMAL;
         PRAGMA cache_size = {};",
        cfg.busy_timeout_ms, initial_locking, cache_kib
    ))
    .map_err(|e| open_err("applying sqlite pragmas", e))?;

    let page_count: i64 = conn
        .query_row("PRAGMA page_count", [], |r| r.get(0))
        .unwrap_or(1);
    if page_count == 0 {
        conn.execute_batch("PRAGMA auto_vacuum = INCREMENTAL;")
            .map_err(|e| open_err("setting auto_vacuum=INCREMENTAL", e))?;
    }

    if effective_wal {
        let wal_res: std::result::Result<String, _> =
            conn.query_row("PRAGMA journal_mode = WAL", [], |r| r.get(0));
        match wal_res {
            Ok(mode) if mode.eq_ignore_ascii_case("wal") || is_memory => {}
            Ok(mode) => {
                ps5_agent_logging::log_warn(
                    "database",
                    format!("SQLite journal_mode=WAL returned '{mode}'; falling back to TRUNCATE"),
                );
                conn.execute_batch("PRAGMA journal_mode = TRUNCATE;")
                    .map_err(|e| open_err("fallback TRUNCATE mode", e))?;
            }
            Err(err)
                if matches!(
                    err,
                    rusqlite::Error::SqliteFailure(ffi_err, _)
                        if matches!(
                            ffi_err.code,
                            ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked
                        )
                ) =>
            {
                return Err(open_err("setting WAL mode", err));
            }
            Err(err) => {
                ps5_agent_logging::log_warn(
                    "database",
                    format!("SQLite journal_mode=WAL failed ({err}); falling back to TRUNCATE"),
                );
                conn.execute_batch("PRAGMA journal_mode = TRUNCATE;")
                    .map_err(|e| open_err("fallback TRUNCATE mode", e))?;
            }
        }
    } else {
        conn.execute_batch("PRAGMA journal_mode = TRUNCATE;")
            .map_err(|e| open_err("setting TRUNCATE mode", e))?;
        if !is_memory {
            conn.execute_batch("PRAGMA locking_mode = NORMAL;")
                .map_err(|e| open_err("restoring locking_mode=NORMAL", e))?;
            let _ = conn.query_row("SELECT 1 FROM sqlite_master LIMIT 1", [], |_| Ok(()));
        }
    }

    Ok(())
}

fn exec_cached<P: rusqlite::Params>(
    conn: &Connection,
    sql: &str,
    params: P,
    op: &str,
) -> Result<usize> {
    let mut stmt = conn
        .prepare_cached(sql)
        .map_err(|e| map_sqlite_err(&format!("prepare {op}"), e))?;
    stmt.execute(params).map_err(|e| map_sqlite_err(op, e))
}

fn query_row_cached<T, P, F>(
    conn: &Connection,
    sql: &str,
    params: P,
    map_fn: F,
    op: &str,
) -> Result<T>
where
    P: rusqlite::Params,
    F: FnOnce(&Row<'_>) -> rusqlite::Result<T>,
{
    let mut stmt = conn
        .prepare_cached(sql)
        .map_err(|e| map_sqlite_err(&format!("prepare {op}"), e))?;
    stmt.query_row(params, map_fn)
        .map_err(|e| map_sqlite_err(op, e))
}

fn query_row_optional_cached<T, P, F>(
    conn: &Connection,
    sql: &str,
    params: P,
    map_fn: F,
    op: &str,
) -> Result<Option<T>>
where
    P: rusqlite::Params,
    F: FnOnce(&Row<'_>) -> rusqlite::Result<T>,
{
    let mut stmt = conn
        .prepare_cached(sql)
        .map_err(|e| map_sqlite_err(&format!("prepare {op}"), e))?;
    stmt.query_row(params, map_fn)
        .optional()
        .map_err(|e| map_sqlite_err(op, e))
}

fn map_sqlite_err(ctx: &str, err: rusqlite::Error) -> AgentError {
    if let rusqlite::Error::SqliteFailure(ffi_err, ref msg) = err {
        if ffi_err.code == ErrorCode::ConstraintViolation {
            let detail = msg.as_deref().unwrap_or("");
            if detail.contains("UNIQUE") || detail.contains("PRIMARY KEY") {
                return AgentError::Conflict(format!("{ctx}: {err}"));
            }
        }
    }
    AgentError::Storage(format!("{ctx}: {err}"))
}

fn clamp_limit(v: usize) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

fn clamp_u64(v: i64) -> u64 {
    v.max(0) as u64
}

fn clamp_u32(v: i64) -> u32 {
    v.clamp(0, i64::from(u32::MAX)) as u32
}

fn clamp_u8(v: i64) -> u8 {
    v.clamp(0, i64::from(u8::MAX)) as u8
}

fn to_i64_checked(v: u64, ctx: &str) -> Result<i64> {
    i64::try_from(v).map_err(|_| AgentError::InvalidInput(format!("{ctx} overflow")))
}

fn to_json<T: Serialize + ?Sized>(val: &T, ctx: &str) -> Result<String> {
    serde_json::to_string(val).map_err(|e| AgentError::Internal(format!("serializing {ctx}: {e}")))
}

fn from_json<T: serde::de::DeserializeOwned>(raw: &str, ctx: &str) -> Result<T> {
    serde_json::from_str(raw).map_err(|e| AgentError::Storage(format!("deserializing {ctx}: {e}")))
}

fn encode_enum_str<T: Serialize>(val: &T, ctx: &str) -> Result<String> {
    match serde_json::to_value(val)
        .map_err(|e| AgentError::Internal(format!("serializing {ctx}: {e}")))?
    {
        serde_json::Value::String(s) => Ok(s),
        other => Ok(other.to_string()),
    }
}

fn decode_enum_str<T: serde::de::DeserializeOwned>(raw: &str, ctx: &str) -> Result<T> {
    serde_json::from_value(serde_json::Value::String(raw.to_string()))
        .map_err(|e| AgentError::Storage(format!("deserializing {ctx} '{raw}': {e}")))
}

fn format_dt(dt: &chrono::DateTime<chrono::Utc>) -> String {
    dt.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
}

fn parse_dt(raw: &str, col: usize, ctx: &str) -> rusqlite::Result<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(
                col,
                rusqlite::types::Type::Text,
                Box::new(AgentError::Storage(format!("parsing {ctx} '{raw}': {e}"))),
            )
        })
}

fn parse_opt_dt(
    raw: Option<String>,
    col: usize,
    ctx: &str,
) -> rusqlite::Result<Option<chrono::DateTime<chrono::Utc>>> {
    raw.filter(|s| !s.is_empty())
        .map(|s| parse_dt(&s, col, ctx))
        .transpose()
}

fn escape_like_pattern(query: &str) -> String {
    let mut out = String::with_capacity(query.len() + 4);
    for ch in query.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '%' => out.push_str("\\%"),
            '_' => out.push_str("\\_"),
            c => out.push(c),
        }
    }
    out
}

const AGENT_COLUMNS: &str = "id, name, description, role, role_label, system_prompt,
    model_config_json, enabled_tools_json, memory_permissions_json,
    max_task_duration_secs, max_reasoning_iterations, priority,
    can_communicate, enabled, is_builtin, is_protected, is_temporary,
    created_at, updated_at, denied_tools_json, sandbox_mode, tool_profile";

fn row_to_agent(row: &Row<'_>) -> rusqlite::Result<AgentDefinition> {
    let role_raw: String = row.get(3)?;
    let model_raw: String = row.get(6)?;
    let tools_raw: String = row.get(7)?;
    let perms_raw: String = row.get(8)?;
    let denied_raw: Option<String> = row.get(19)?;

    let role = decode_enum_str(&role_raw, "agent.role")
        .map_err(|e| rusqlite::Error::FromSqlConversionFailure(3, rusqlite::types::Type::Text, Box::new(e)))?;
    let model_config = from_json(&model_raw, "agent.model_config")
        .map_err(|e| rusqlite::Error::FromSqlConversionFailure(6, rusqlite::types::Type::Text, Box::new(e)))?;
    let enabled_tools = from_json(&tools_raw, "agent.enabled_tools")
        .map_err(|e| rusqlite::Error::FromSqlConversionFailure(7, rusqlite::types::Type::Text, Box::new(e)))?;
    let memory_permissions = from_json(&perms_raw, "agent.memory_permissions")
        .map_err(|e| rusqlite::Error::FromSqlConversionFailure(8, rusqlite::types::Type::Text, Box::new(e)))?;
    let denied_tools: Vec<String> = match denied_raw {
        Some(ref raw) if !raw.trim().is_empty() => from_json(raw, "agent.denied_tools")
            .map_err(|e| rusqlite::Error::FromSqlConversionFailure(19, rusqlite::types::Type::Text, Box::new(e)))?,
        _ => Vec::new(),
    };

    Ok(AgentDefinition {
        id: row.get(0)?,
        name: row.get(1)?,
        description: row.get(2)?,
        role,
        role_label: row.get(4)?,
        system_prompt: row.get(5)?,
        model_config,
        enabled_tools,
        denied_tools,
        sandbox_mode: row.get(20)?,
        tool_profile: row.get(21)?,
        memory_permissions,
        max_task_duration_secs: clamp_u64(row.get(9)?),
        max_reasoning_iterations: clamp_u32(row.get(10)?),
        priority: row.get(11)?,
        can_communicate: row.get::<_, i64>(12)? != 0,
        enabled: row.get::<_, i64>(13)? != 0,
        is_builtin: row.get::<_, i64>(14)? != 0,
        is_protected: row.get::<_, i64>(15)? != 0,
        is_temporary: row.get::<_, i64>(16)? != 0,
        created_at: row.get(17)?,
        updated_at: row.get(18)?,
    })
}

fn row_to_conversation(row: &Row<'_>) -> rusqlite::Result<Conversation> {
    let mode_raw: String = row.get(2)?;
    let mode = from_json(&mode_raw, "conversation.mode")
        .map_err(|e| rusqlite::Error::FromSqlConversionFailure(2, rusqlite::types::Type::Text, Box::new(e)))?;
    Ok(Conversation {
        id: row.get(0)?,
        title: row.get(1)?,
        mode,
        created_at: row.get(3)?,
        updated_at: row.get(4)?,
        archived: row.get::<_, i64>(5)? != 0,
        sort_order: row.get(6)?,
    })
}

const MESSAGE_COLUMNS: &str = "id, conversation_id, role, agent_id, content, reasoning,
    tool_calls_json, task_id, token_count, latency_ms, archived, created_at,
    attachments_json, timings_json";

fn encode_message_json_fields(
    msg: &ChatMessage,
) -> Result<(Option<String>, Option<String>, Option<i64>)> {
    let attachments_json = if msg.attachments.is_empty() {
        None
    } else {
        Some(to_json(&msg.attachments, "message.attachments")?)
    };
    let timings_json = msg
        .timings
        .as_ref()
        .map(|t| to_json(t, "message.timings"))
        .transpose()?;
    let latency_ms = msg
        .latency_ms
        .map(|v| to_i64_checked(v, "message.latency_ms"))
        .transpose()?;
    Ok((attachments_json, timings_json, latency_ms))
}

fn row_to_chat_message(row: &Row<'_>) -> rusqlite::Result<ChatMessage> {
    let role_raw: String = row.get(2)?;
    let role = decode_enum_str(&role_raw, "message.role")
        .map_err(|e| rusqlite::Error::FromSqlConversionFailure(2, rusqlite::types::Type::Text, Box::new(e)))?;
    let attachments_raw: Option<String> = row.get(12)?;
    let attachments: Vec<ChatAttachment> = match attachments_raw {
        Some(ref raw) if !raw.trim().is_empty() => from_json(raw, "message.attachments")
            .map_err(|e| rusqlite::Error::FromSqlConversionFailure(12, rusqlite::types::Type::Text, Box::new(e)))?,
        _ => Vec::new(),
    };
    let timings_raw: Option<String> = row.get(13)?;
    let timings: Option<InferenceTimings> = match timings_raw {
        Some(ref raw) if !raw.trim().is_empty() => match serde_json::from_str(raw) {
            Ok(parsed) => Some(parsed),
            Err(err) => {
                ps5_agent_logging::log_warn(
                    "database",
                    format!("Ignoring malformed message.timings JSON: {err}"),
                );
                None
            }
        },
        _ => None,
    };
    Ok(ChatMessage {
        id: row.get(0)?,
        conversation_id: row.get(1)?,
        role,
        agent_id: row.get(3)?,
        content: row.get(4)?,
        reasoning: row.get(5)?,
        tool_calls_json: row.get(6)?,
        task_id: row.get(7)?,
        token_count: clamp_u32(row.get(8)?),
        latency_ms: row.get::<_, Option<i64>>(9)?.map(clamp_u64),
        archived: row.get::<_, i64>(10)? != 0,
        created_at: row.get(11)?,
        attachments,
        timings,
    })
}

fn row_to_task(row: &Row<'_>) -> rusqlite::Result<TaskRecord> {
    let status_raw: String = row.get(9)?;
    let status = decode_enum_str(&status_raw, "task.status")
        .map_err(|e| rusqlite::Error::FromSqlConversionFailure(9, rusqlite::types::Type::Text, Box::new(e)))?;
    Ok(TaskRecord {
        id: row.get(0)?,
        conversation_id: row.get(1)?,
        user_request: row.get(2)?,
        assigned_agent_id: row.get(3)?,
        parent_task_id: row.get(4)?,
        priority: row.get(5)?,
        created_at: row.get(6)?,
        started_at: row.get(7)?,
        completed_at: row.get(8)?,
        status,
        result: row.get(10)?,
        error_info: row.get(11)?,
        has_side_effects: row.get::<_, i64>(12)? != 0,
        progress_pct: clamp_u8(row.get(13)?),
        progress_note: row.get(14)?,
    })
}

fn row_to_agent_envelope(row: &Row<'_>) -> rusqlite::Result<AgentEnvelope> {
    let msg_type_raw: String = row.get(5)?;
    let status_raw: String = row.get(7)?;
    let message_type = decode_enum_str(&msg_type_raw, "agent_message.message_type")
        .map_err(|e| rusqlite::Error::FromSqlConversionFailure(5, rusqlite::types::Type::Text, Box::new(e)))?;
    let status = decode_enum_str(&status_raw, "agent_message.status")
        .map_err(|e| rusqlite::Error::FromSqlConversionFailure(7, rusqlite::types::Type::Text, Box::new(e)))?;
    Ok(AgentEnvelope {
        id: row.get(0)?,
        sender_agent_id: row.get(1)?,
        recipient_agent_id: row.get(2)?,
        task_id: row.get(3)?,
        timestamp: row.get(4)?,
        message_type,
        content: row.get(6)?,
        status,
        hop_count: clamp_u32(row.get(8)?),
    })
}

fn row_to_memory(row: &Row<'_>) -> rusqlite::Result<MemoryItem> {
    let scope_raw: String = row.get(1)?;
    let tags_raw: String = row.get(10)?;
    let scope = decode_enum_str(&scope_raw, "memory.scope")
        .map_err(|e| rusqlite::Error::FromSqlConversionFailure(1, rusqlite::types::Type::Text, Box::new(e)))?;
    let tags = from_json(&tags_raw, "memory.tags")
        .map_err(|e| rusqlite::Error::FromSqlConversionFailure(10, rusqlite::types::Type::Text, Box::new(e)))?;
    Ok(MemoryItem {
        id: row.get(0)?,
        scope,
        namespace: row.get(2)?,
        owner_agent_id: row.get(3)?,
        conversation_id: row.get(4)?,
        task_id: row.get(5)?,
        key: row.get(6)?,
        content: row.get(7)?,
        summary: row.get(8)?,
        importance: clamp_u8(row.get(9)?),
        tags,
        archived: row.get::<_, i64>(11)? != 0,
        created_at: row.get(12)?,
        updated_at: row.get(13)?,
    })
}

fn row_to_tool_spec(row: &Row<'_>) -> rusqlite::Result<ToolSpec> {
    let in_raw: String = row.get(4)?;
    let out_raw: String = row.get(5)?;
    let perms_raw: String = row.get(6)?;
    let input_schema = from_json(&in_raw, "tool.input_schema")
        .map_err(|e| rusqlite::Error::FromSqlConversionFailure(4, rusqlite::types::Type::Text, Box::new(e)))?;
    let output_schema = from_json(&out_raw, "tool.output_schema")
        .map_err(|e| rusqlite::Error::FromSqlConversionFailure(5, rusqlite::types::Type::Text, Box::new(e)))?;
    let required_permissions = from_json(&perms_raw, "tool.required_permissions")
        .map_err(|e| rusqlite::Error::FromSqlConversionFailure(6, rusqlite::types::Type::Text, Box::new(e)))?;
    Ok(ToolSpec {
        id: row.get(0)?,
        name: row.get(1)?,
        category: row.get(2)?,
        description: row.get(3)?,
        input_schema,
        output_schema,
        required_permissions,
        requires_approval: row.get::<_, i64>(7)? != 0,
        is_mutating: row.get::<_, i64>(8)? != 0,
        enabled: row.get::<_, i64>(9)? != 0,
        timeout_secs: clamp_u64(row.get(10)?),
    })
}

fn row_to_tool_execution(row: &Row<'_>) -> rusqlite::Result<ToolExecutionRecord> {
    Ok(ToolExecutionRecord {
        id: row.get(0)?,
        tool_id: row.get(1)?,
        agent_id: row.get(2)?,
        task_id: row.get(3)?,
        input_json: row.get(4)?,
        output_json: row.get(5)?,
        success: row.get::<_, i64>(6)? != 0,
        error: row.get(7)?,
        duration_ms: clamp_u64(row.get(8)?),
        created_at: row.get(9)?,
    })
}

fn row_to_session(row: &Row<'_>) -> rusqlite::Result<SessionRecord> {
    Ok(SessionRecord {
        id: row.get(0)?,
        token_sha256: row.get(1)?,
        csrf_token: row.get(2)?,
        username: row.get(3)?,
        client_ip: row.get(4)?,
        created_at: row.get(5)?,
        expires_at: row.get(6)?,
        revoked: row.get::<_, i64>(7)? != 0,
    })
}

fn row_to_approval(row: &Row<'_>) -> rusqlite::Result<ApprovalRecord> {
    let status_raw: String = row.get(7)?;
    let status = decode_enum_str(&status_raw, "approval.status")
        .map_err(|e| rusqlite::Error::FromSqlConversionFailure(7, rusqlite::types::Type::Text, Box::new(e)))?;
    Ok(ApprovalRecord {
        id: row.get(0)?,
        task_id: row.get(1)?,
        agent_id: row.get(2)?,
        operation: row.get(3)?,
        tool_id: row.get(4)?,
        arguments_json: row.get(5)?,
        reason: row.get(6)?,
        status,
        created_at: row.get(8)?,
        resolved_at: row.get(9)?,
        resolved_by: row.get(10)?,
    })
}

const SCHEDULE_COLUMNS: &str = "id, title, prompt, target_agent_id, conversation_id, kind,
    run_at, interval_secs, daily_time, cron_expr, timezone_offset_mins,
    enabled, next_run_at, last_run_at, last_status, last_error,
    last_task_id, run_count, created_at, updated_at";

fn row_to_schedule(row: &Row<'_>) -> rusqlite::Result<ScheduleRecord> {
    let kind_raw: String = row.get(5)?;
    let kind: ScheduleKind = decode_enum_str(&kind_raw, "schedule.kind")
        .map_err(|e| rusqlite::Error::FromSqlConversionFailure(5, rusqlite::types::Type::Text, Box::new(e)))?;
    let created_raw: String = row.get(18)?;
    let updated_raw: String = row.get(19)?;

    Ok(ScheduleRecord {
        id: row.get(0)?,
        title: row.get(1)?,
        prompt: row.get(2)?,
        target_agent_id: row.get(3)?,
        conversation_id: row.get(4)?,
        kind,
        run_at: parse_opt_dt(row.get(6)?, 6, "schedule.run_at")?,
        interval_secs: row.get::<_, Option<i64>>(7)?.map(|v| v.max(0) as u64),
        daily_time: row.get(8)?,
        cron_expr: row.get(9)?,
        timezone_offset_mins: row.get(10)?,
        enabled: row.get::<_, i64>(11)? != 0,
        next_run_at: parse_opt_dt(row.get(12)?, 12, "schedule.next_run_at")?,
        last_run_at: parse_opt_dt(row.get(13)?, 13, "schedule.last_run_at")?,
        last_status: row.get(14)?,
        last_error: row.get(15)?,
        last_task_id: row.get(16)?,
        run_count: row.get::<_, i64>(17)?.max(0) as u64,
        created_at: parse_dt(&created_raw, 18, "schedule.created_at")?,
        updated_at: parse_dt(&updated_raw, 19, "schedule.updated_at")?,
    })
}

fn collect_schedules<P: rusqlite::Params>(
    conn: &Connection,
    sql: &str,
    params: P,
    op: &str,
) -> Result<Vec<ScheduleRecord>> {
    let mut stmt = conn
        .prepare_cached(sql)
        .map_err(|e| map_sqlite_err(&format!("prepare {op}"), e))?;
    let rows = stmt
        .query_map(params, row_to_schedule)
        .map_err(|e| map_sqlite_err(&format!("query {op}"), e))?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| map_sqlite_err(&format!("collect {op}"), e))
}

const WEBHOOK_COLUMNS: &str = "id, name, enabled, secret_hash, secret_prefix, target_agent_id,
    conversation_id, prompt_template, created_at, updated_at, last_triggered_at, trigger_count";

fn row_to_webhook(row: &Row<'_>) -> rusqlite::Result<WebhookRecord> {
    Ok(WebhookRecord {
        id: row.get(0)?,
        name: row.get(1)?,
        enabled: row.get::<_, i64>(2)? != 0,
        secret_hash: row.get(3)?,
        secret_prefix: row.get(4)?,
        target_agent_id: row.get(5)?,
        conversation_id: row.get(6)?,
        prompt_template: row.get(7)?,
        created_at: row.get(8)?,
        updated_at: row.get(9)?,
        last_triggered_at: row.get(10)?,
        trigger_count: clamp_u64(row.get(11)?),
    })
}

impl StorageBackend for SqliteStorage {
    fn driver_name(&self) -> &'static str {
        "sqlite"
    }

    fn run_migrations(&self) -> Result<()> {
        let conn = self.lock_conn()?;
        run_sqlite_migrations(&conn)
    }

    fn integrity_check(&self) -> Result<String> {
        let conn = self.lock_conn()?;
        let mut stmt = conn
            .prepare("PRAGMA integrity_check;")
            .map_err(|e| map_sqlite_err("prepare integrity_check", e))?;
        let rows = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .map_err(|e| map_sqlite_err("query integrity_check", e))?;
        let mut lines = Vec::new();
        for row in rows {
            lines.push(row.map_err(|e| map_sqlite_err("read integrity_check", e))?);
        }
        Ok(lines.join("\n"))
    }

    fn checkpoint(&self) -> Result<()> {
        let conn = self.lock_conn()?;
        let auto_vacuum: i64 = conn
            .query_row("PRAGMA auto_vacuum", [], |r| r.get(0))
            .unwrap_or(0);
        if auto_vacuum != 2 {
            conn.execute_batch("PRAGMA auto_vacuum = INCREMENTAL; VACUUM;")
                .map_err(|e| map_sqlite_err("checkpoint vacuum", e))?;
        } else {
            conn.execute_batch("PRAGMA incremental_vacuum;")
                .map_err(|e| map_sqlite_err("checkpoint incremental_vacuum", e))?;
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE); PRAGMA optimize; PRAGMA shrink_memory;")
            .map_err(|e| map_sqlite_err("checkpoint", e))?;
        Ok(())
    }

    fn upsert_agent(&self, agent: &AgentDefinition) -> Result<()> {
        let role = encode_enum_str(&agent.role, "agent.role")?;
        let model_cfg = to_json(&agent.model_config, "agent.model_config")?;
        let tools = to_json(&agent.enabled_tools, "agent.enabled_tools")?;
        let perms = to_json(&agent.memory_permissions, "agent.memory_permissions")?;
        let denied_tools = to_json(&agent.denied_tools, "agent.denied_tools")?;
        let max_duration = to_i64_checked(agent.max_task_duration_secs, "agent.max_task_duration_secs")?;
        let conn = self.lock_conn()?;
        exec_cached(
            &conn,
            "INSERT INTO agents (
                id, name, description, role, role_label, system_prompt,
                model_config_json, enabled_tools_json, memory_permissions_json,
                max_task_duration_secs, max_reasoning_iterations, priority,
                can_communicate, enabled, is_builtin, is_protected, is_temporary,
                created_at, updated_at, denied_tools_json, sandbox_mode, tool_profile
             ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11,
                ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22
             )
             ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                description = excluded.description,
                role = excluded.role,
                role_label = excluded.role_label,
                system_prompt = excluded.system_prompt,
                model_config_json = excluded.model_config_json,
                enabled_tools_json = excluded.enabled_tools_json,
                memory_permissions_json = excluded.memory_permissions_json,
                max_task_duration_secs = excluded.max_task_duration_secs,
                max_reasoning_iterations = excluded.max_reasoning_iterations,
                priority = excluded.priority,
                can_communicate = excluded.can_communicate,
                enabled = excluded.enabled,
                is_builtin = excluded.is_builtin,
                is_protected = excluded.is_protected,
                is_temporary = excluded.is_temporary,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at,
                denied_tools_json = excluded.denied_tools_json,
                sandbox_mode = excluded.sandbox_mode,
                tool_profile = excluded.tool_profile",
            params![
                agent.id,
                agent.name,
                agent.description,
                role,
                agent.role_label,
                agent.system_prompt,
                model_cfg,
                tools,
                perms,
                max_duration,
                i64::from(agent.max_reasoning_iterations),
                agent.priority,
                i64::from(agent.can_communicate),
                i64::from(agent.enabled),
                i64::from(agent.is_builtin),
                i64::from(agent.is_protected),
                i64::from(agent.is_temporary),
                agent.created_at,
                agent.updated_at,
                denied_tools,
                agent.sandbox_mode,
                agent.tool_profile,
            ],
            "upsert_agent",
        )?;
        Ok(())
    }

    fn get_agent(&self, agent_id: &str) -> Result<Option<AgentDefinition>> {
        let conn = self.lock_conn()?;
        let sql = format!("SELECT {AGENT_COLUMNS} FROM agents WHERE id = ?1");
        query_row_optional_cached(&conn, &sql, params![agent_id], row_to_agent, "get_agent")
    }

    fn list_agents(&self) -> Result<Vec<AgentDefinition>> {
        let conn = self.lock_conn()?;
        let sql = format!(
            "SELECT {AGENT_COLUMNS} FROM agents ORDER BY priority DESC, created_at ASC, id ASC"
        );
        let mut stmt = conn
            .prepare_cached(&sql)
            .map_err(|e| map_sqlite_err("prepare list_agents", e))?;
        let rows = stmt
            .query_map([], row_to_agent)
            .map_err(|e| map_sqlite_err("query list_agents", e))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| map_sqlite_err("collect list_agents", e))
    }

    fn delete_agent(&self, agent_id: &str) -> Result<bool> {
        let conn = self.lock_conn()?;
        let changed = exec_cached(&conn, "DELETE FROM agents WHERE id = ?1", params![agent_id], "delete_agent")?;
        Ok(changed > 0)
    }

    fn create_conversation(&self, conv: &Conversation) -> Result<()> {
        let mode_json = to_json(&conv.mode, "conversation.mode")?;
        let conn = self.lock_conn()?;
        exec_cached(
            &conn,
            "INSERT INTO conversations (id, title, mode_json, created_at, updated_at, archived, sort_order)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                conv.id,
                conv.title,
                mode_json,
                conv.created_at,
                conv.updated_at,
                i64::from(conv.archived),
                conv.sort_order,
            ],
            "create_conversation",
        )?;
        Ok(())
    }

    fn get_conversation(&self, conv_id: &str) -> Result<Option<Conversation>> {
        let conn = self.lock_conn()?;
        query_row_optional_cached(
            &conn,
            "SELECT id, title, mode_json, created_at, updated_at, archived, sort_order
             FROM conversations WHERE id = ?1",
            params![conv_id],
            row_to_conversation,
            "get_conversation",
        )
    }

    fn list_conversations(&self, include_archived: bool) -> Result<Vec<Conversation>> {
        let conn = self.lock_conn()?;
        let sql = if include_archived {
            "SELECT id, title, mode_json, created_at, updated_at, archived, sort_order
             FROM conversations ORDER BY sort_order ASC, updated_at DESC, created_at DESC, id ASC"
        } else {
            "SELECT id, title, mode_json, created_at, updated_at, archived, sort_order
             FROM conversations WHERE archived = 0 ORDER BY sort_order ASC, updated_at DESC, created_at DESC, id ASC"
        };
        let mut stmt = conn
            .prepare_cached(sql)
            .map_err(|e| map_sqlite_err("prepare list_conversations", e))?;
        let rows = stmt
            .query_map([], row_to_conversation)
            .map_err(|e| map_sqlite_err("query list_conversations", e))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| map_sqlite_err("collect list_conversations", e))
    }

    fn delete_conversation(&self, conv_id: &str) -> Result<bool> {
        let conn = self.lock_conn()?;
        let changed = exec_cached(
            &conn,
            "DELETE FROM conversations WHERE id = ?1",
            params![conv_id],
            "delete_conversation",
        )?;
        Ok(changed > 0)
    }

    fn reorder_conversations(&self, ordered_ids: &[String]) -> Result<()> {
        let conn = self.lock_conn()?;
        let tx = conn
            .unchecked_transaction()
            .map_err(|e| map_sqlite_err("begin reorder_conversations", e))?;
        {
            let mut stmt = tx
                .prepare_cached("UPDATE conversations SET sort_order = ?1 WHERE id = ?2")
                .map_err(|e| map_sqlite_err("prepare reorder_conversations", e))?;
            for (idx, id) in ordered_ids.iter().enumerate() {
                let sort_order = ((idx as i32) + 1) * 10;
                stmt.execute(params![sort_order, id])
                    .map_err(|e| map_sqlite_err("update reorder_conversations", e))?;
            }
        }
        tx.commit()
            .map_err(|e| map_sqlite_err("commit reorder_conversations", e))?;
        Ok(())
    }

    fn append_chat_message(&self, msg: &ChatMessage) -> Result<()> {
        let role = encode_enum_str(&msg.role, "message.role")?;
        let (attachments_json, timings_json, latency_ms) = encode_message_json_fields(msg)?;
        let conn = self.lock_conn()?;
        let sql = format!(
            "INSERT INTO messages ({MESSAGE_COLUMNS})
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)"
        );
        exec_cached(
            &conn,
            &sql,
            params![
                msg.id,
                msg.conversation_id,
                role,
                msg.agent_id,
                msg.content,
                msg.reasoning,
                msg.tool_calls_json,
                msg.task_id,
                i64::from(msg.token_count),
                latency_ms,
                i64::from(msg.archived),
                msg.created_at,
                attachments_json,
                timings_json,
            ],
            "append_chat_message",
        )?;
        Ok(())
    }

    fn list_chat_messages(
        &self,
        conv_id: &str,
        include_archived: bool,
        limit: usize,
    ) -> Result<Vec<ChatMessage>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let conn = self.lock_conn()?;
        let sql = if include_archived {
            format!(
                "SELECT {MESSAGE_COLUMNS}
                 FROM (
                    SELECT {MESSAGE_COLUMNS}, rowid AS _seq
                    FROM messages
                    WHERE conversation_id = ?1
                    ORDER BY created_at DESC, rowid DESC
                    LIMIT ?2
                 )
                 ORDER BY created_at ASC, _seq ASC"
            )
        } else {
            format!(
                "SELECT {MESSAGE_COLUMNS}
                 FROM (
                    SELECT {MESSAGE_COLUMNS}, rowid AS _seq
                    FROM messages
                    WHERE conversation_id = ?1 AND archived = 0
                    ORDER BY created_at DESC, rowid DESC
                    LIMIT ?2
                 )
                 ORDER BY created_at ASC, _seq ASC"
            )
        };
        let mut stmt = conn
            .prepare_cached(&sql)
            .map_err(|e| map_sqlite_err("prepare list_chat_messages", e))?;
        let rows = stmt
            .query_map(params![conv_id, clamp_limit(limit)], row_to_chat_message)
            .map_err(|e| map_sqlite_err("query list_chat_messages", e))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| map_sqlite_err("collect list_chat_messages", e))
    }

    fn search_chat_messages(&self, query: &str, limit: usize) -> Result<Vec<ChatMessage>> {
        let trimmed = query.trim();
        if trimmed.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let pattern = format!("%{}%", escape_like_pattern(&trimmed.to_lowercase()));
        let conn = self.lock_conn()?;
        let sql = format!(
            "SELECT {MESSAGE_COLUMNS}
             FROM messages
             WHERE LOWER(content) LIKE ?1 ESCAPE '\\'
             ORDER BY created_at DESC, rowid DESC
             LIMIT ?2"
        );
        let mut stmt = conn
            .prepare_cached(&sql)
            .map_err(|e| map_sqlite_err("prepare search_chat_messages", e))?;
        let rows = stmt
            .query_map(params![pattern, clamp_limit(limit)], row_to_chat_message)
            .map_err(|e| map_sqlite_err("query search_chat_messages", e))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| map_sqlite_err("collect search_chat_messages", e))
    }

    fn archive_old_chat_messages(&self, conv_id: &str, keep_recent: usize) -> Result<usize> {
        let conn = self.lock_conn()?;
        exec_cached(
            &conn,
            "UPDATE messages
             SET archived = 1
             WHERE conversation_id = ?1
               AND archived = 0
               AND id NOT IN (
                   SELECT id FROM messages
                   WHERE conversation_id = ?1 AND archived = 0
                   ORDER BY created_at DESC, rowid DESC
                   LIMIT ?2
               )",
            params![conv_id, clamp_limit(keep_recent)],
            "archive_old_chat_messages",
        )
    }

    fn get_chat_message(&self, msg_id: &str) -> Result<Option<ChatMessage>> {
        let conn = self.lock_conn()?;
        let sql = format!("SELECT {MESSAGE_COLUMNS} FROM messages WHERE id = ?1");
        query_row_optional_cached(&conn, &sql, params![msg_id], row_to_chat_message, "get_chat_message")
    }

    fn update_chat_message(&self, msg: &ChatMessage) -> Result<bool> {
        let (attachments_json, timings_json, latency_ms) = encode_message_json_fields(msg)?;
        let conn = self.lock_conn()?;
        let changed = exec_cached(
            &conn,
            "UPDATE messages
             SET content = ?2,
                 reasoning = ?3,
                 tool_calls_json = ?4,
                 task_id = ?5,
                 token_count = ?6,
                 latency_ms = ?7,
                 archived = ?8,
                 attachments_json = ?9,
                 timings_json = ?10
             WHERE id = ?1",
            params![
                msg.id,
                msg.content,
                msg.reasoning,
                msg.tool_calls_json,
                msg.task_id,
                i64::from(msg.token_count),
                latency_ms,
                i64::from(msg.archived),
                attachments_json,
                timings_json,
            ],
            "update_chat_message",
        )?;
        Ok(changed > 0)
    }

    fn delete_chat_message(&self, msg_id: &str) -> Result<bool> {
        let conn = self.lock_conn()?;
        let changed = exec_cached(&conn, "DELETE FROM messages WHERE id = ?1", params![msg_id], "delete_chat_message")?;
        Ok(changed > 0)
    }

    fn delete_chat_messages_after(
        &self,
        conversation_id: &str,
        anchor_msg_id: &str,
        inclusive: bool,
    ) -> Result<usize> {
        let conn = self.lock_conn()?;
        let anchor_row: Option<(String, i64)> = query_row_optional_cached(
            &conn,
            "SELECT created_at, rowid FROM messages WHERE id = ?1 AND conversation_id = ?2",
            params![anchor_msg_id, conversation_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
            "find anchor message for delete_chat_messages_after",
        )?;
        let Some((anchor_ts, anchor_rowid)) = anchor_row else {
            return Ok(0);
        };
        let sql = if inclusive {
            "DELETE FROM messages
             WHERE conversation_id = ?1
               AND (created_at > ?2 OR (created_at = ?2 AND rowid >= ?3))"
        } else {
            "DELETE FROM messages
             WHERE conversation_id = ?1
               AND (created_at > ?2 OR (created_at = ?2 AND rowid > ?3))"
        };
        exec_cached(
            &conn,
            sql,
            params![conversation_id, anchor_ts, anchor_rowid],
            "delete_chat_messages_after",
        )
    }

    fn upsert_task(&self, task: &TaskRecord) -> Result<()> {
        let status = encode_enum_str(&task.status, "task.status")?;
        let conn = self.lock_conn()?;
        exec_cached(
            &conn,
            "INSERT INTO tasks (
                id, conversation_id, user_request, assigned_agent_id, parent_task_id,
                priority, created_at, started_at, completed_at, status, result, error_info,
                has_side_effects, progress_pct, progress_note
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
             ON CONFLICT(id) DO UPDATE SET
                conversation_id = excluded.conversation_id,
                user_request = excluded.user_request,
                assigned_agent_id = excluded.assigned_agent_id,
                parent_task_id = excluded.parent_task_id,
                priority = excluded.priority,
                created_at = excluded.created_at,
                started_at = excluded.started_at,
                completed_at = excluded.completed_at,
                status = excluded.status,
                result = excluded.result,
                error_info = excluded.error_info,
                has_side_effects = excluded.has_side_effects,
                progress_pct = excluded.progress_pct,
                progress_note = excluded.progress_note",
            params![
                task.id,
                task.conversation_id,
                task.user_request,
                task.assigned_agent_id,
                task.parent_task_id,
                task.priority,
                task.created_at,
                task.started_at,
                task.completed_at,
                status,
                task.result,
                task.error_info,
                i64::from(task.has_side_effects),
                i64::from(task.progress_pct),
                task.progress_note,
            ],
            "upsert_task",
        )?;
        Ok(())
    }

    fn get_task(&self, task_id: &str) -> Result<Option<TaskRecord>> {
        let conn = self.lock_conn()?;
        query_row_optional_cached(
            &conn,
            "SELECT id, conversation_id, user_request, assigned_agent_id, parent_task_id,
                    priority, created_at, started_at, completed_at, status, result, error_info,
                    has_side_effects, progress_pct, progress_note
             FROM tasks WHERE id = ?1",
            params![task_id],
            row_to_task,
            "get_task",
        )
    }

    fn list_tasks(&self, limit: usize) -> Result<Vec<TaskRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let conn = self.lock_conn()?;
        let mut stmt = conn
            .prepare_cached(
                "SELECT id, conversation_id, user_request, assigned_agent_id, parent_task_id,
                        priority, created_at, started_at, completed_at, status, result, error_info,
                        has_side_effects, progress_pct, progress_note
                 FROM tasks
                 ORDER BY created_at DESC, rowid DESC
                 LIMIT ?1",
            )
            .map_err(|e| map_sqlite_err("prepare list_tasks", e))?;
        let rows = stmt
            .query_map(params![clamp_limit(limit)], row_to_task)
            .map_err(|e| map_sqlite_err("query list_tasks", e))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| map_sqlite_err("collect list_tasks", e))
    }

    fn task_counts_summary(&self) -> Result<TaskCountsSummary> {
        let conn = self.lock_conn()?;
        let mut summary = TaskCountsSummary::default();

        {
            let mut stmt = conn
                .prepare_cached("SELECT status, COUNT(*) FROM tasks GROUP BY status")
                .map_err(|e| map_sqlite_err("prepare task_counts_summary", e))?;
            let rows = stmt
                .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))
                .map_err(|e| map_sqlite_err("query task_counts_summary", e))?;
            for row in rows {
                let (status, count_i64) =
                    row.map_err(|e| map_sqlite_err("read task_counts_summary", e))?;
                let count = count_i64.max(0) as usize;
                summary.total += count;
                match status.as_str() {
                    "PENDING" => summary.pending += count,
                    "ASSIGNED" | "RUNNING" => summary.running += count,
                    "WAITING" => summary.waiting += count,
                    "COMPLETED" => summary.completed += count,
                    "FAILED" => summary.failed += count,
                    "CANCELLED" => summary.cancelled += count,
                    _ => {}
                }
            }
        }

        {
            let mut stmt = conn
                .prepare_cached(
                    "SELECT assigned_agent_id, status, COUNT(*)
                     FROM tasks
                     WHERE assigned_agent_id IS NOT NULL
                     GROUP BY assigned_agent_id, status",
                )
                .map_err(|e| map_sqlite_err("prepare task_counts_summary per_agent", e))?;
            let rows = stmt
                .query_map([], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, i64>(2)?,
                    ))
                })
                .map_err(|e| map_sqlite_err("query task_counts_summary per_agent", e))?;

            for row in rows {
                let (agent_id, status, count_i64) =
                    row.map_err(|e| map_sqlite_err("read task_counts_summary per_agent", e))?;
                let count = count_i64.max(0) as usize;
                let entry = summary.per_agent.entry(agent_id).or_default();
                match status.as_str() {
                    "COMPLETED" => entry.completed += count,
                    "FAILED" => entry.failed += count,
                    "ASSIGNED" | "RUNNING" | "WAITING" => entry.active += count,
                    _ => {}
                }
            }
        }

        {
            let mut stmt = conn
                .prepare_cached(
                    "SELECT assigned_agent_id, ts FROM (
                         SELECT assigned_agent_id,
                                COALESCE(completed_at, started_at, created_at) AS ts,
                                ROW_NUMBER() OVER (
                                    PARTITION BY assigned_agent_id
                                    ORDER BY julianday(COALESCE(completed_at, started_at, created_at)) DESC, rowid DESC
                                ) AS rn
                         FROM tasks
                         WHERE assigned_agent_id IS NOT NULL
                     ) WHERE rn = 1",
                )
                .map_err(|e| map_sqlite_err("prepare task_counts_summary last_activity", e))?;
            let rows = stmt
                .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
                .map_err(|e| map_sqlite_err("query task_counts_summary last_activity", e))?;
            for row in rows {
                let (agent_id, ts) =
                    row.map_err(|e| map_sqlite_err("read task_counts_summary last_activity", e))?;
                let entry = summary.per_agent.entry(agent_id).or_default();
                entry.last_activity = Some(ts);
            }
        }

        {
            let mut stmt = conn
                .prepare_cached(
                    "SELECT assigned_agent_id, id FROM (
                         SELECT assigned_agent_id, id,
                                ROW_NUMBER() OVER (
                                    PARTITION BY assigned_agent_id
                                    ORDER BY julianday(created_at) DESC, rowid DESC
                                ) AS rn
                         FROM tasks
                         WHERE assigned_agent_id IS NOT NULL
                           AND status IN ('ASSIGNED', 'RUNNING', 'WAITING')
                     ) WHERE rn = 1",
                )
                .map_err(|e| map_sqlite_err("prepare task_counts_summary active_task", e))?;
            let rows = stmt
                .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
                .map_err(|e| map_sqlite_err("query task_counts_summary active_task", e))?;
            for row in rows {
                let (agent_id, task_id) =
                    row.map_err(|e| map_sqlite_err("read task_counts_summary active_task", e))?;
                let entry = summary.per_agent.entry(agent_id).or_default();
                entry.latest_active_task_id = Some(task_id);
            }
        }

        Ok(summary)
    }

    fn append_task_event(&self, event: &TaskEvent) -> Result<()> {
        let conn = self.lock_conn()?;
        exec_cached(
            &conn,
            "INSERT INTO task_events (id, task_id, timestamp, event_type, details)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                event.id,
                event.task_id,
                event.timestamp,
                event.event_type,
                event.details,
            ],
            "append_task_event",
        )?;
        Ok(())
    }

    fn list_task_events(&self, task_id: &str) -> Result<Vec<TaskEvent>> {
        let conn = self.lock_conn()?;
        let mut stmt = conn
            .prepare_cached(
                "SELECT id, task_id, timestamp, event_type, details
                 FROM task_events
                 WHERE task_id = ?1
                 ORDER BY timestamp ASC, rowid ASC",
            )
            .map_err(|e| map_sqlite_err("prepare list_task_events", e))?;
        let rows = stmt
            .query_map(params![task_id], |r| {
                Ok(TaskEvent {
                    id: r.get(0)?,
                    task_id: r.get(1)?,
                    timestamp: r.get(2)?,
                    event_type: r.get(3)?,
                    details: r.get(4)?,
                })
            })
            .map_err(|e| map_sqlite_err("query list_task_events", e))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| map_sqlite_err("collect list_task_events", e))
    }

    fn append_agent_message(&self, msg: &AgentEnvelope) -> Result<()> {
        let msg_type = encode_enum_str(&msg.message_type, "agent_message.message_type")?;
        let status = encode_enum_str(&msg.status, "agent_message.status")?;
        let conn = self.lock_conn()?;
        exec_cached(
            &conn,
            "INSERT INTO agent_messages (
                id, sender_agent_id, recipient_agent_id, task_id, timestamp,
                message_type, content, status, hop_count
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                msg.id,
                msg.sender_agent_id,
                msg.recipient_agent_id,
                msg.task_id,
                msg.timestamp,
                msg_type,
                msg.content,
                status,
                i64::from(msg.hop_count),
            ],
            "append_agent_message",
        )?;
        Ok(())
    }

    fn update_agent_message_status(&self, msg_id: &str, status: DeliveryStatus) -> Result<()> {
        let status_str = encode_enum_str(&status, "agent_message.status")?;
        let conn = self.lock_conn()?;
        let changed = exec_cached(
            &conn,
            "UPDATE agent_messages SET status = ?2 WHERE id = ?1",
            params![msg_id, status_str],
            "update_agent_message_status",
        )?;
        if changed == 0 {
            return Err(AgentError::NotFound(format!(
                "agent message '{msg_id}' not found"
            )));
        }
        Ok(())
    }

    fn list_agent_messages(
        &self,
        task_id: Option<&str>,
        agent_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<AgentEnvelope>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let conn = self.lock_conn()?;
        let mut stmt = conn
            .prepare_cached(
                "SELECT id, sender_agent_id, recipient_agent_id, task_id, timestamp,
                        message_type, content, status, hop_count
                 FROM agent_messages
                 WHERE (?1 IS NULL OR task_id = ?1)
                   AND (?2 IS NULL OR sender_agent_id = ?2 OR recipient_agent_id = ?2)
                 ORDER BY timestamp DESC, rowid DESC
                 LIMIT ?3",
            )
            .map_err(|e| map_sqlite_err("prepare list_agent_messages", e))?;
        let rows = stmt
            .query_map(params![task_id, agent_id, clamp_limit(limit)], row_to_agent_envelope)
            .map_err(|e| map_sqlite_err("query list_agent_messages", e))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| map_sqlite_err("collect list_agent_messages", e))
    }

    fn count_task_agent_messages(&self, task_id: &str) -> Result<u32> {
        let conn = self.lock_conn()?;
        let count: i64 = query_row_cached(
            &conn,
            "SELECT COUNT(*) FROM agent_messages WHERE task_id = ?1",
            params![task_id],
            |r| r.get(0),
            "count_task_agent_messages",
        )?;
        Ok(clamp_u32(count))
    }

    fn upsert_memory(&self, item: &MemoryItem) -> Result<()> {
        let scope = encode_enum_str(&item.scope, "memory.scope")?;
        let tags = to_json(&item.tags, "memory.tags")?;
        let conn = self.lock_conn()?;
        exec_cached(
            &conn,
            "INSERT INTO memories (
                id, scope, namespace, owner_agent_id, conversation_id, task_id,
                key, content, summary, importance, tags_json, archived, created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
             ON CONFLICT(id) DO UPDATE SET
                scope = excluded.scope,
                namespace = excluded.namespace,
                owner_agent_id = excluded.owner_agent_id,
                conversation_id = excluded.conversation_id,
                task_id = excluded.task_id,
                key = excluded.key,
                content = excluded.content,
                summary = excluded.summary,
                importance = excluded.importance,
                tags_json = excluded.tags_json,
                archived = excluded.archived,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at",
            params![
                item.id,
                scope,
                item.namespace,
                item.owner_agent_id,
                item.conversation_id,
                item.task_id,
                item.key,
                item.content,
                item.summary,
                i64::from(item.importance),
                tags,
                i64::from(item.archived),
                item.created_at,
                item.updated_at,
            ],
            "upsert_memory",
        )?;
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
        if limit == 0 {
            return Ok(Vec::new());
        }
        let scope_str = scope
            .as_ref()
            .map(|s| encode_enum_str(s, "memory.scope"))
            .transpose()?;
        let conn = self.lock_conn()?;
        let mut stmt = conn
            .prepare_cached(
                "SELECT id, scope, namespace, owner_agent_id, conversation_id, task_id,
                        key, content, summary, importance, tags_json, archived, created_at, updated_at
                 FROM memories
                 WHERE (?1 IS NULL OR scope = ?1)
                   AND (?2 IS NULL OR namespace = ?2)
                   AND (?3 IS NULL OR owner_agent_id = ?3)
                   AND (?4 = 1 OR archived = 0)
                 ORDER BY importance DESC, updated_at DESC, rowid DESC
                 LIMIT ?5",
            )
            .map_err(|e| map_sqlite_err("prepare query_memories", e))?;
        let rows = stmt
            .query_map(
                params![
                    scope_str,
                    namespace,
                    owner_agent_id,
                    i64::from(include_archived),
                    clamp_limit(limit),
                ],
                row_to_memory,
            )
            .map_err(|e| map_sqlite_err("query query_memories", e))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| map_sqlite_err("collect query_memories", e))
    }

    fn delete_memory(&self, memory_id: &str) -> Result<bool> {
        let conn = self.lock_conn()?;
        let changed = exec_cached(&conn, "DELETE FROM memories WHERE id = ?1", params![memory_id], "delete_memory")?;
        Ok(changed > 0)
    }

    fn delete_agent_memories(&self, agent_id: &str) -> Result<usize> {
        let conn = self.lock_conn()?;
        exec_cached(
            &conn,
            "DELETE FROM memories WHERE owner_agent_id = ?1",
            params![agent_id],
            "delete_agent_memories",
        )
    }

    fn memory_stats_for_agent(&self, agent_id: &str) -> Result<(u64, u64)> {
        let conn = self.lock_conn()?;
        let (count, bytes): (i64, i64) = query_row_cached(
            &conn,
            "SELECT COUNT(*), COALESCE(SUM(LENGTH(CAST(content AS BLOB))), 0)
             FROM memories
             WHERE owner_agent_id = ?1 AND archived = 0",
            params![agent_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
            "memory_stats_for_agent",
        )?;
        Ok((clamp_u64(count), clamp_u64(bytes)))
    }

    fn upsert_tool_spec(&self, spec: &ToolSpec) -> Result<()> {
        let in_json = to_json(&spec.input_schema, "tool.input_schema")?;
        let out_json = to_json(&spec.output_schema, "tool.output_schema")?;
        let perms_json = to_json(&spec.required_permissions, "tool.required_permissions")?;
        let timeout_secs = to_i64_checked(spec.timeout_secs, "tool.timeout_secs")?;
        let conn = self.lock_conn()?;
        exec_cached(
            &conn,
            "INSERT INTO tool_registry (
                id, name, category, description, input_schema_json, output_schema_json,
                required_permissions_json, requires_approval, is_mutating, enabled, timeout_secs
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
             ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                category = excluded.category,
                description = excluded.description,
                input_schema_json = excluded.input_schema_json,
                output_schema_json = excluded.output_schema_json,
                required_permissions_json = excluded.required_permissions_json,
                requires_approval = excluded.requires_approval,
                is_mutating = excluded.is_mutating,
                enabled = excluded.enabled,
                timeout_secs = excluded.timeout_secs",
            params![
                spec.id,
                spec.name,
                spec.category,
                spec.description,
                in_json,
                out_json,
                perms_json,
                i64::from(spec.requires_approval),
                i64::from(spec.is_mutating),
                i64::from(spec.enabled),
                timeout_secs,
            ],
            "upsert_tool_spec",
        )?;
        Ok(())
    }

    fn list_tool_specs(&self) -> Result<Vec<ToolSpec>> {
        let conn = self.lock_conn()?;
        let mut stmt = conn
            .prepare_cached(
                "SELECT id, name, category, description, input_schema_json, output_schema_json,
                        required_permissions_json, requires_approval, is_mutating, enabled, timeout_secs
                 FROM tool_registry
                 ORDER BY category ASC, name ASC, id ASC",
            )
            .map_err(|e| map_sqlite_err("prepare list_tool_specs", e))?;
        let rows = stmt
            .query_map([], row_to_tool_spec)
            .map_err(|e| map_sqlite_err("query list_tool_specs", e))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| map_sqlite_err("collect list_tool_specs", e))
    }

    fn delete_tool_spec(&self, tool_id: &str) -> Result<bool> {
        let conn = self.lock_conn()?;
        let changed = exec_cached(&conn, "DELETE FROM tool_registry WHERE id = ?1", params![tool_id], "delete_tool_spec")?;
        Ok(changed > 0)
    }

    fn record_tool_execution(&self, rec: &ToolExecutionRecord) -> Result<()> {
        let duration_ms = to_i64_checked(rec.duration_ms, "tool_execution.duration_ms")?;
        let conn = self.lock_conn()?;
        exec_cached(
            &conn,
            "INSERT INTO tool_executions (
                id, tool_id, agent_id, task_id, input_json, output_json,
                success, error, duration_ms, created_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                rec.id,
                rec.tool_id,
                rec.agent_id,
                rec.task_id,
                rec.input_json,
                rec.output_json,
                i64::from(rec.success),
                rec.error,
                duration_ms,
                rec.created_at,
            ],
            "record_tool_execution",
        )?;
        Ok(())
    }

    fn list_tool_executions(
        &self,
        agent_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ToolExecutionRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let conn = self.lock_conn()?;
        let mut stmt = conn
            .prepare_cached(
                "SELECT id, tool_id, agent_id, task_id, input_json, output_json,
                        success, error, duration_ms, created_at
                 FROM tool_executions
                 WHERE (?1 IS NULL OR agent_id = ?1)
                 ORDER BY created_at DESC, rowid DESC
                 LIMIT ?2",
            )
            .map_err(|e| map_sqlite_err("prepare list_tool_executions", e))?;
        let rows = stmt
            .query_map(params![agent_id, clamp_limit(limit)], row_to_tool_execution)
            .map_err(|e| map_sqlite_err("query list_tool_executions", e))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| map_sqlite_err("collect list_tool_executions", e))
    }

    fn get_setting(&self, key: &str) -> Result<Option<String>> {
        let conn = self.lock_conn()?;
        query_row_optional_cached(
            &conn,
            "SELECT value FROM settings WHERE key = ?1",
            params![key],
            |r| r.get(0),
            "get_setting",
        )
    }

    fn set_setting(&self, key: &str, value: &str) -> Result<()> {
        let conn = self.lock_conn()?;
        exec_cached(
            &conn,
            "INSERT INTO settings (key, value, updated_at)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = excluded.updated_at",
            params![key, value, now_iso()],
            "set_setting",
        )?;
        Ok(())
    }

    fn list_settings(&self) -> Result<Vec<(String, String)>> {
        let conn = self.lock_conn()?;
        let mut stmt = conn
            .prepare_cached("SELECT key, value FROM settings ORDER BY key ASC")
            .map_err(|e| map_sqlite_err("prepare list_settings", e))?;
        let rows = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .map_err(|e| map_sqlite_err("query list_settings", e))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| map_sqlite_err("collect list_settings", e))
    }

    fn create_session(&self, sess: &SessionRecord) -> Result<()> {
        let conn = self.lock_conn()?;
        exec_cached(
            &conn,
            "INSERT INTO sessions (
                id, token_sha256, csrf_token, username, client_ip,
                created_at, expires_at, revoked
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                sess.id,
                sess.token_sha256,
                sess.csrf_token,
                sess.username,
                sess.client_ip,
                sess.created_at,
                sess.expires_at,
                i64::from(sess.revoked),
            ],
            "create_session",
        )?;
        Ok(())
    }

    fn get_session_by_hash(&self, token_sha256: &str) -> Result<Option<SessionRecord>> {
        let conn = self.lock_conn()?;
        query_row_optional_cached(
            &conn,
            "SELECT id, token_sha256, csrf_token, username, client_ip,
                    created_at, expires_at, revoked
             FROM sessions WHERE token_sha256 = ?1",
            params![token_sha256],
            row_to_session,
            "get_session_by_hash",
        )
    }

    fn revoke_session(&self, session_id: &str) -> Result<()> {
        let conn = self.lock_conn()?;
        exec_cached(
            &conn,
            "UPDATE sessions SET revoked = 1 WHERE id = ?1",
            params![session_id],
            "revoke_session",
        )?;
        Ok(())
    }

    fn purge_expired_sessions(&self, now_iso: &str) -> Result<usize> {
        let now_ts = chrono::DateTime::parse_from_rfc3339(now_iso)
            .map(|d| d.with_timezone(&chrono::Utc))
            .unwrap_or_else(|_| chrono::Utc::now());
        let conn = self.lock_conn()?;
        let tx = conn
            .unchecked_transaction()
            .map_err(|e| map_sqlite_err("begin purge_expired_sessions", e))?;
        let expired_ids = {
            let mut stmt = tx
                .prepare_cached("SELECT id, expires_at FROM sessions")
                .map_err(|e| map_sqlite_err("prepare purge_expired_sessions", e))?;
            let rows = stmt
                .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
                .map_err(|e| map_sqlite_err("query purge_expired_sessions", e))?;
            let mut ids = Vec::new();
            for row in rows {
                let (id, expires_at) =
                    row.map_err(|e| map_sqlite_err("read purge_expired_sessions", e))?;
                let is_expired = match chrono::DateTime::parse_from_rfc3339(&expires_at) {
                    Ok(exp_ts) => exp_ts.with_timezone(&chrono::Utc) <= now_ts,
                    Err(_) => true,
                };
                if is_expired {
                    ids.push(id);
                }
            }
            ids
        };

        if expired_ids.is_empty() {
            return Ok(0);
        }

        let mut deleted = 0usize;
        {
            let mut del_stmt = tx
                .prepare_cached("DELETE FROM sessions WHERE id = ?1")
                .map_err(|e| map_sqlite_err("prepare delete expired session", e))?;
            for id in &expired_ids {
                deleted += del_stmt
                    .execute(params![id])
                    .map_err(|e| map_sqlite_err("purge_expired_sessions delete", e))?;
            }
        }
        tx.commit()
            .map_err(|e| map_sqlite_err("commit purge_expired_sessions", e))?;
        Ok(deleted)
    }

    fn upsert_approval(&self, approval: &ApprovalRecord) -> Result<()> {
        let status = encode_enum_str(&approval.status, "approval.status")?;
        let conn = self.lock_conn()?;
        exec_cached(
            &conn,
            "INSERT INTO approvals (
                id, task_id, agent_id, operation, tool_id, arguments_json,
                reason, status, created_at, resolved_at, resolved_by
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
             ON CONFLICT(id) DO UPDATE SET
                task_id = excluded.task_id,
                agent_id = excluded.agent_id,
                operation = excluded.operation,
                tool_id = excluded.tool_id,
                arguments_json = excluded.arguments_json,
                reason = excluded.reason,
                status = excluded.status,
                created_at = excluded.created_at,
                resolved_at = excluded.resolved_at,
                resolved_by = excluded.resolved_by",
            params![
                approval.id,
                approval.task_id,
                approval.agent_id,
                approval.operation,
                approval.tool_id,
                approval.arguments_json,
                approval.reason,
                status,
                approval.created_at,
                approval.resolved_at,
                approval.resolved_by,
            ],
            "upsert_approval",
        )?;
        Ok(())
    }

    fn get_approval(&self, approval_id: &str) -> Result<Option<ApprovalRecord>> {
        let conn = self.lock_conn()?;
        query_row_optional_cached(
            &conn,
            "SELECT id, task_id, agent_id, operation, tool_id, arguments_json,
                    reason, status, created_at, resolved_at, resolved_by
             FROM approvals WHERE id = ?1",
            params![approval_id],
            row_to_approval,
            "get_approval",
        )
    }

    fn list_approvals(&self, pending_only: bool, limit: usize) -> Result<Vec<ApprovalRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let conn = self.lock_conn()?;
        let sql = if pending_only {
            "SELECT id, task_id, agent_id, operation, tool_id, arguments_json,
                    reason, status, created_at, resolved_at, resolved_by
             FROM approvals
             WHERE status = 'PENDING'
             ORDER BY created_at DESC, rowid DESC
             LIMIT ?1"
        } else {
            "SELECT id, task_id, agent_id, operation, tool_id, arguments_json,
                    reason, status, created_at, resolved_at, resolved_by
             FROM approvals
             ORDER BY created_at DESC, rowid DESC
             LIMIT ?1"
        };
        let mut stmt = conn
            .prepare_cached(sql)
            .map_err(|e| map_sqlite_err("prepare list_approvals", e))?;
        let rows = stmt
            .query_map(params![clamp_limit(limit)], row_to_approval)
            .map_err(|e| map_sqlite_err("query list_approvals", e))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| map_sqlite_err("collect list_approvals", e))
    }

    fn append_audit_log(&self, entry: &AuditLogEntry) -> Result<()> {
        let conn = self.lock_conn()?;
        exec_cached(
            &conn,
            "INSERT INTO audit_logs (
                id, timestamp, actor, action, target, severity, outcome, details_json
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                entry.id,
                entry.timestamp,
                entry.actor,
                entry.action,
                entry.target,
                entry.severity,
                entry.outcome,
                entry.details_json,
            ],
            "append_audit_log",
        )?;
        Ok(())
    }

    fn list_audit_logs(&self, limit: usize) -> Result<Vec<AuditLogEntry>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let conn = self.lock_conn()?;
        let mut stmt = conn
            .prepare_cached(
                "SELECT id, timestamp, actor, action, target, severity, outcome, details_json
                 FROM audit_logs
                 ORDER BY timestamp DESC, rowid DESC
                 LIMIT ?1",
            )
            .map_err(|e| map_sqlite_err("prepare list_audit_logs", e))?;
        let rows = stmt
            .query_map(params![clamp_limit(limit)], |r| {
                Ok(AuditLogEntry {
                    id: r.get(0)?,
                    timestamp: r.get(1)?,
                    actor: r.get(2)?,
                    action: r.get(3)?,
                    target: r.get(4)?,
                    severity: r.get(5)?,
                    outcome: r.get(6)?,
                    details_json: r.get(7)?,
                })
            })
            .map_err(|e| map_sqlite_err("query list_audit_logs", e))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| map_sqlite_err("collect list_audit_logs", e))
    }

    fn prune_old_records(&self, retention_days: u32, max_rows_per_table: usize) -> Result<PruneReport> {
        let now = chrono::Utc::now();
        let cutoff_iso = if retention_days > 0 {
            let days = i64::from(retention_days.min(36_500));
            Some((now - chrono::Duration::days(days)).to_rfc3339())
        } else {
            None
        };
        let row_cap = if max_rows_per_table > 0 {
            Some(i64::try_from(max_rows_per_table).unwrap_or(i64::MAX))
        } else {
            None
        };

        let mut report = PruneReport::default();
        let conn = self.lock_conn()?;
        let tx = conn
            .unchecked_transaction()
            .map_err(|e| map_sqlite_err("begin prune_old_records", e))?;

        // 1. Purge expired or revoked sessions (never evict active non-expired sessions by row count).
        {
            let mut dead_session_ids = Vec::new();
            {
                let mut stmt = tx
                    .prepare_cached("SELECT id, expires_at, revoked FROM sessions")
                    .map_err(|e| map_sqlite_err("prepare prune sessions", e))?;
                let rows = stmt
                    .query_map([], |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, i64>(2)? != 0,
                        ))
                    })
                    .map_err(|e| map_sqlite_err("query prune sessions", e))?;
                for row in rows {
                    let (id, expires_at, revoked) =
                        row.map_err(|e| map_sqlite_err("read prune sessions", e))?;
                    let is_expired = match chrono::DateTime::parse_from_rfc3339(&expires_at) {
                        Ok(exp_ts) => exp_ts.with_timezone(&chrono::Utc) <= now,
                        Err(_) => true,
                    };
                    if revoked || is_expired {
                        dead_session_ids.push(id);
                    }
                }
            }
            if !dead_session_ids.is_empty() {
                let mut del_stmt = tx
                    .prepare_cached("DELETE FROM sessions WHERE id = ?1")
                    .map_err(|e| map_sqlite_err("prepare delete dead session", e))?;
                for id in &dead_session_ids {
                    report.purged_sessions += del_stmt
                        .execute(params![id])
                        .map_err(|e| map_sqlite_err("prune dead session", e))?;
                }
            }
        }

        // 2. Purge resolved approvals beyond retention_days / max_rows_per_table (never pending).
        if let Some(ref cutoff) = cutoff_iso {
            tx.execute(
                "DELETE FROM approvals
                 WHERE status != 'PENDING'
                   AND julianday(COALESCE(resolved_at, created_at)) < julianday(?1)",
                params![cutoff],
            )
            .map_err(|e| map_sqlite_err("prune old approvals", e))?;
        }
        if let Some(cap) = row_cap {
            tx.execute(
                "DELETE FROM approvals
                 WHERE id IN (
                     SELECT id FROM approvals
                     WHERE status != 'PENDING'
                     ORDER BY julianday(COALESCE(resolved_at, created_at)) ASC, rowid ASC
                     LIMIT MAX(0, (SELECT COUNT(*) FROM approvals) - ?1)
                 )",
                params![cap],
            )
            .map_err(|e| map_sqlite_err("prune excess approvals", e))?;
        }

        // 3. Purge terminal tasks (and their child task_events / linked agent_messages).
        if let Some(ref cutoff) = cutoff_iso {
            report.deleted_task_events += tx
                .execute(
                    "DELETE FROM task_events
                     WHERE task_id IN (
                         SELECT id FROM tasks
                         WHERE status IN ('COMPLETED', 'FAILED', 'CANCELLED')
                           AND julianday(COALESCE(completed_at, started_at, created_at)) < julianday(?1)
                     )",
                    params![cutoff],
                )
                .map_err(|e| map_sqlite_err("prune old task_events via tasks", e))?;
            report.deleted_agent_messages += tx
                .execute(
                    "DELETE FROM agent_messages
                     WHERE task_id IN (
                         SELECT id FROM tasks
                         WHERE status IN ('COMPLETED', 'FAILED', 'CANCELLED')
                           AND julianday(COALESCE(completed_at, started_at, created_at)) < julianday(?1)
                     )",
                    params![cutoff],
                )
                .map_err(|e| map_sqlite_err("prune old agent_messages via tasks", e))?;
            report.deleted_tasks += tx
                .execute(
                    "DELETE FROM tasks
                     WHERE status IN ('COMPLETED', 'FAILED', 'CANCELLED')
                       AND julianday(COALESCE(completed_at, started_at, created_at)) < julianday(?1)",
                    params![cutoff],
                )
                .map_err(|e| map_sqlite_err("prune old tasks", e))?;
        }
        if let Some(cap) = row_cap {
            report.deleted_task_events += tx
                .execute(
                    "DELETE FROM task_events
                     WHERE task_id IN (
                         SELECT id FROM tasks
                         WHERE status IN ('COMPLETED', 'FAILED', 'CANCELLED')
                         ORDER BY julianday(COALESCE(completed_at, started_at, created_at)) ASC, rowid ASC
                         LIMIT MAX(0, (SELECT COUNT(*) FROM tasks) - ?1)
                     )",
                    params![cap],
                )
                .map_err(|e| map_sqlite_err("prune excess task_events via tasks", e))?;
            report.deleted_agent_messages += tx
                .execute(
                    "DELETE FROM agent_messages
                     WHERE task_id IN (
                         SELECT id FROM tasks
                         WHERE status IN ('COMPLETED', 'FAILED', 'CANCELLED')
                         ORDER BY julianday(COALESCE(completed_at, started_at, created_at)) ASC, rowid ASC
                         LIMIT MAX(0, (SELECT COUNT(*) FROM tasks) - ?1)
                     )",
                    params![cap],
                )
                .map_err(|e| map_sqlite_err("prune excess agent_messages via tasks", e))?;
            report.deleted_tasks += tx
                .execute(
                    "DELETE FROM tasks
                     WHERE id IN (
                         SELECT id FROM tasks
                         WHERE status IN ('COMPLETED', 'FAILED', 'CANCELLED')
                         ORDER BY julianday(COALESCE(completed_at, started_at, created_at)) ASC, rowid ASC
                         LIMIT MAX(0, (SELECT COUNT(*) FROM tasks) - ?1)
                     )",
                    params![cap],
                )
                .map_err(|e| map_sqlite_err("prune excess tasks", e))?;
        }

        // 4. Purge remaining old/excess task_events (prioritize keeping active-task events).
        if let Some(ref cutoff) = cutoff_iso {
            report.deleted_task_events += tx
                .execute(
                    "DELETE FROM task_events
                     WHERE julianday(timestamp) < julianday(?1)
                       AND NOT EXISTS (
                           SELECT 1 FROM tasks t
                           WHERE t.id = task_events.task_id
                             AND t.status IN ('PENDING', 'ASSIGNED', 'RUNNING', 'WAITING')
                       )",
                    params![cutoff],
                )
                .map_err(|e| map_sqlite_err("prune old task_events", e))?;
        }
        if let Some(cap) = row_cap {
            report.deleted_task_events += tx
                .execute(
                    "DELETE FROM task_events
                     WHERE id IN (
                         SELECT te.id FROM task_events te
                         LEFT JOIN tasks t ON t.id = te.task_id
                         ORDER BY
                             CASE WHEN t.status IN ('PENDING', 'ASSIGNED', 'RUNNING', 'WAITING') THEN 1 ELSE 0 END ASC,
                             julianday(te.timestamp) ASC,
                             te.rowid ASC
                         LIMIT MAX(0, (SELECT COUNT(*) FROM task_events) - ?1)
                     )",
                    params![cap],
                )
                .map_err(|e| map_sqlite_err("prune excess task_events", e))?;
        }

        // 5. Purge old/excess agent_messages (prioritize keeping active-task messages).
        if let Some(ref cutoff) = cutoff_iso {
            report.deleted_agent_messages += tx
                .execute(
                    "DELETE FROM agent_messages
                     WHERE julianday(timestamp) < julianday(?1)
                       AND (task_id IS NULL OR NOT EXISTS (
                           SELECT 1 FROM tasks t
                           WHERE t.id = agent_messages.task_id
                             AND t.status IN ('PENDING', 'ASSIGNED', 'RUNNING', 'WAITING')
                       ))",
                    params![cutoff],
                )
                .map_err(|e| map_sqlite_err("prune old agent_messages", e))?;
        }
        if let Some(cap) = row_cap {
            report.deleted_agent_messages += tx
                .execute(
                    "DELETE FROM agent_messages
                     WHERE id IN (
                         SELECT am.id FROM agent_messages am
                         LEFT JOIN tasks t ON t.id = am.task_id
                         ORDER BY
                             CASE WHEN t.status IN ('PENDING', 'ASSIGNED', 'RUNNING', 'WAITING') THEN 1 ELSE 0 END ASC,
                             julianday(am.timestamp) ASC,
                             am.rowid ASC
                         LIMIT MAX(0, (SELECT COUNT(*) FROM agent_messages) - ?1)
                     )",
                    params![cap],
                )
                .map_err(|e| map_sqlite_err("prune excess agent_messages", e))?;
        }

        // 6. Purge old/excess tool_executions.
        if let Some(ref cutoff) = cutoff_iso {
            report.deleted_tool_executions += tx
                .execute(
                    "DELETE FROM tool_executions WHERE julianday(created_at) < julianday(?1)",
                    params![cutoff],
                )
                .map_err(|e| map_sqlite_err("prune old tool_executions", e))?;
        }
        if let Some(cap) = row_cap {
            report.deleted_tool_executions += tx
                .execute(
                    "DELETE FROM tool_executions
                     WHERE id IN (
                         SELECT id FROM tool_executions
                         ORDER BY julianday(created_at) ASC, rowid ASC
                         LIMIT MAX(0, (SELECT COUNT(*) FROM tool_executions) - ?1)
                     )",
                    params![cap],
                )
                .map_err(|e| map_sqlite_err("prune excess tool_executions", e))?;
        }

        // 7. Purge old/excess audit_logs.
        if let Some(ref cutoff) = cutoff_iso {
            report.deleted_audit_logs += tx
                .execute(
                    "DELETE FROM audit_logs WHERE julianday(timestamp) < julianday(?1)",
                    params![cutoff],
                )
                .map_err(|e| map_sqlite_err("prune old audit_logs", e))?;
        }
        if let Some(cap) = row_cap {
            report.deleted_audit_logs += tx
                .execute(
                    "DELETE FROM audit_logs
                     WHERE id IN (
                         SELECT id FROM audit_logs
                         ORDER BY julianday(timestamp) ASC, rowid ASC
                         LIMIT MAX(0, (SELECT COUNT(*) FROM audit_logs) - ?1)
                     )",
                    params![cap],
                )
                .map_err(|e| map_sqlite_err("prune excess audit_logs", e))?;
        }

        // 8. Purge archived chat messages older than cutoff or pushed past row cap (never non-archived).
        if let Some(ref cutoff) = cutoff_iso {
            report.deleted_chat_messages += tx
                .execute(
                    "DELETE FROM messages WHERE archived = 1 AND julianday(created_at) < julianday(?1)",
                    params![cutoff],
                )
                .map_err(|e| map_sqlite_err("prune old archived messages", e))?;
        }
        if let Some(cap) = row_cap {
            report.deleted_chat_messages += tx
                .execute(
                    "DELETE FROM messages
                     WHERE id IN (
                         SELECT id FROM messages
                         WHERE archived = 1
                         ORDER BY julianday(created_at) ASC, rowid ASC
                         LIMIT MAX(0, (SELECT COUNT(*) FROM messages) - ?1)
                     )",
                    params![cap],
                )
                .map_err(|e| map_sqlite_err("prune excess archived messages", e))?;
        }

        tx.commit()
            .map_err(|e| map_sqlite_err("commit prune_old_records", e))?;

        conn.execute_batch("PRAGMA incremental_vacuum(64); PRAGMA shrink_memory;")
            .map_err(|e| map_sqlite_err("prune incremental_vacuum", e))?;

        Ok(report)
    }

    fn upsert_schedule(&self, schedule: &ScheduleRecord) -> Result<()> {
        let kind = encode_enum_str(&schedule.kind, "schedule.kind")?;
        let run_at = schedule.run_at.as_ref().map(format_dt);
        let interval_secs = schedule
            .interval_secs
            .map(|v| {
                i64::try_from(v)
                    .map_err(|_| AgentError::InvalidInput("schedule interval_secs overflow".into()))
            })
            .transpose()?;
        let next_run_at = schedule.next_run_at.as_ref().map(format_dt);
        let last_run_at = schedule.last_run_at.as_ref().map(format_dt);
        let run_count = i64::try_from(schedule.run_count)
            .map_err(|_| AgentError::InvalidInput("schedule run_count overflow".into()))?;
        let created_at = format_dt(&schedule.created_at);
        let updated_at = format_dt(&schedule.updated_at);

        let conn = self.lock_conn()?;
        exec_cached(
            &conn,
            "INSERT INTO schedules (
                id, title, prompt, target_agent_id, conversation_id, kind,
                run_at, interval_secs, daily_time, cron_expr, timezone_offset_mins,
                enabled, next_run_at, last_run_at, last_status, last_error,
                last_task_id, run_count, created_at, updated_at
             ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6,
                ?7, ?8, ?9, ?10, ?11,
                ?12, ?13, ?14, ?15, ?16,
                ?17, ?18, ?19, ?20
             )
             ON CONFLICT(id) DO UPDATE SET
                title = excluded.title,
                prompt = excluded.prompt,
                target_agent_id = excluded.target_agent_id,
                conversation_id = excluded.conversation_id,
                kind = excluded.kind,
                run_at = excluded.run_at,
                interval_secs = excluded.interval_secs,
                daily_time = excluded.daily_time,
                cron_expr = excluded.cron_expr,
                timezone_offset_mins = excluded.timezone_offset_mins,
                enabled = excluded.enabled,
                next_run_at = excluded.next_run_at,
                last_run_at = excluded.last_run_at,
                last_status = excluded.last_status,
                last_error = excluded.last_error,
                last_task_id = excluded.last_task_id,
                run_count = excluded.run_count,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at",
            params![
                schedule.id,
                schedule.title,
                schedule.prompt,
                schedule.target_agent_id,
                schedule.conversation_id,
                kind,
                run_at,
                interval_secs,
                schedule.daily_time,
                schedule.cron_expr,
                schedule.timezone_offset_mins,
                i64::from(schedule.enabled),
                next_run_at,
                last_run_at,
                schedule.last_status,
                schedule.last_error,
                schedule.last_task_id,
                run_count,
                created_at,
                updated_at,
            ],
            "upsert_schedule",
        )?;
        Ok(())
    }

    fn get_schedule(&self, id: &str) -> Result<Option<ScheduleRecord>> {
        let conn = self.lock_conn()?;
        let sql = format!("SELECT {SCHEDULE_COLUMNS} FROM schedules WHERE id = ?1");
        query_row_optional_cached(&conn, &sql, params![id], row_to_schedule, "get_schedule")
    }

    fn list_schedules(&self, limit: usize) -> Result<Vec<ScheduleRecord>> {
        let conn = self.lock_conn()?;
        let cap = (if limit == 0 { 1000 } else { limit.min(1000) }) as i64;
        let sql = format!(
            "SELECT {SCHEDULE_COLUMNS} FROM schedules ORDER BY created_at DESC, id ASC LIMIT ?1"
        );
        collect_schedules(&conn, &sql, params![cap], "list_schedules")
    }

    fn list_due_schedules(
        &self,
        now: chrono::DateTime<chrono::Utc>,
        limit: usize,
    ) -> Result<Vec<ScheduleRecord>> {
        let conn = self.lock_conn()?;
        let now_iso = format_dt(&now);
        let cap = (if limit == 0 { 64 } else { limit.min(512) }) as i64;
        let sql = format!(
            "SELECT {SCHEDULE_COLUMNS}
             FROM schedules
             WHERE enabled = 1 AND next_run_at IS NOT NULL AND next_run_at <= ?1
             ORDER BY next_run_at ASC, id ASC
             LIMIT ?2"
        );
        collect_schedules(&conn, &sql, params![now_iso, cap], "list_due_schedules")
    }

    fn delete_schedule(&self, id: &str) -> Result<()> {
        let conn = self.lock_conn()?;
        exec_cached(&conn, "DELETE FROM schedules WHERE id = ?1", params![id], "delete_schedule")?;
        Ok(())
    }

    fn list_webhooks(&self) -> Result<Vec<WebhookRecord>> {
        let conn = self.lock_conn()?;
        let sql = format!(
            "SELECT {WEBHOOK_COLUMNS} FROM webhooks ORDER BY created_at DESC, id ASC LIMIT 1000"
        );
        let mut stmt = conn
            .prepare_cached(&sql)
            .map_err(|e| map_sqlite_err("prepare list_webhooks", e))?;
        let rows = stmt
            .query_map([], row_to_webhook)
            .map_err(|e| map_sqlite_err("query list_webhooks", e))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| map_sqlite_err("collect list_webhooks", e))
    }

    fn get_webhook(&self, id: &str) -> Result<Option<WebhookRecord>> {
        let conn = self.lock_conn()?;
        let sql = format!("SELECT {WEBHOOK_COLUMNS} FROM webhooks WHERE id = ?1");
        query_row_optional_cached(&conn, &sql, params![id], row_to_webhook, "get_webhook")
    }

    fn upsert_webhook(&self, hook: &WebhookRecord) -> Result<()> {
        let trigger_count = to_i64_checked(hook.trigger_count, "webhook.trigger_count")?;
        let conn = self.lock_conn()?;
        exec_cached(
            &conn,
            "INSERT INTO webhooks (
                id, name, enabled, secret_hash, secret_prefix, target_agent_id,
                conversation_id, prompt_template, created_at, updated_at,
                last_triggered_at, trigger_count
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
             ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                enabled = excluded.enabled,
                secret_hash = excluded.secret_hash,
                secret_prefix = excluded.secret_prefix,
                target_agent_id = excluded.target_agent_id,
                conversation_id = excluded.conversation_id,
                prompt_template = excluded.prompt_template,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at,
                last_triggered_at = excluded.last_triggered_at,
                trigger_count = excluded.trigger_count",
            params![
                hook.id,
                hook.name,
                i64::from(hook.enabled),
                hook.secret_hash,
                hook.secret_prefix,
                hook.target_agent_id,
                hook.conversation_id,
                hook.prompt_template,
                hook.created_at,
                hook.updated_at,
                hook.last_triggered_at,
                trigger_count,
            ],
            "upsert_webhook",
        )?;
        Ok(())
    }

    fn delete_webhook(&self, id: &str) -> Result<bool> {
        let conn = self.lock_conn()?;
        let changed = exec_cached(&conn, "DELETE FROM webhooks WHERE id = ?1", params![id], "delete_webhook")?;
        Ok(changed > 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TempDir(PathBuf);
    impl TempDir {
        fn new(prefix: &str) -> Self {
            let p = std::env::temp_dir().join(format!("{prefix}-{}", new_id()));
            fs::create_dir_all(&p).unwrap();
            Self(p)
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn make_session(id: &str, expires_at: &str) -> SessionRecord {
        SessionRecord {
            id: id.to_string(),
            token_sha256: format!("hash-{id}"),
            csrf_token: format!("csrf-{id}"),
            username: "admin".to_string(),
            client_ip: "127.0.0.1".to_string(),
            created_at: "2026-11-13T08:00:00Z".to_string(),
            expires_at: expires_at.to_string(),
            revoked: false,
        }
    }

    #[test]
    fn pragmas_use_normal_locking_mode_and_respect_effective_wal() {
        let dir = TempDir::new("ps5-db-sqlite-pragmas");
        let db_path = dir.0.join("pragmas.db");

        let cfg_wal = DatabaseConfig {
            driver: "sqlite".to_string(),
            path: db_path.clone(),
            busy_timeout_ms: 5000,
            cache_size_kb: 4000,
            wal_mode: true,
            ..DatabaseConfig::default()
        };
        let storage_wal = SqliteStorage::open(&db_path, &cfg_wal).expect("open wal");
        {
            let conn = storage_wal.lock_conn().unwrap();
            let locking: String = conn.query_row("PRAGMA locking_mode", [], |r| r.get(0)).unwrap();
            let journal: String = conn.query_row("PRAGMA journal_mode", [], |r| r.get(0)).unwrap();
            assert_eq!(locking.to_lowercase(), "normal");
            if cfg!(target_vendor = "sie") {
                assert_eq!(journal.to_lowercase(), "truncate");
            } else {
                assert_eq!(journal.to_lowercase(), "wal");
            }
        }
        drop(storage_wal);

        let mut cfg_trunc = cfg_wal;
        cfg_trunc.wal_mode = false;
        let storage_trunc = SqliteStorage::open(&db_path, &cfg_trunc).expect("open truncate");
        let conn = storage_trunc.lock_conn().unwrap();
        let locking: String = conn.query_row("PRAGMA locking_mode", [], |r| r.get(0)).unwrap();
        let journal: String = conn.query_row("PRAGMA journal_mode", [], |r| r.get(0)).unwrap();
        assert_eq!(locking.to_lowercase(), "normal");
        assert_eq!(journal.to_lowercase(), "truncate");
    }

    #[test]
    fn stale_shm_removed_when_wal_missing_or_empty() {
        let dir = TempDir::new("ps5-db-stale-shm");
        let db_path = dir.0.join("stale.db");
        let shm_path = sidecar_path(&db_path, "-shm");
        let wal_path = sidecar_path(&db_path, "-wal");

        let cfg = DatabaseConfig {
            driver: "sqlite".to_string(),
            path: db_path.clone(),
            busy_timeout_ms: 5000,
            cache_size_kb: 4000,
            wal_mode: false,
            ..DatabaseConfig::default()
        };

        // Case 1: -wal missing, -shm exists
        fs::write(&shm_path, b"stale shm bytes").unwrap();
        let _s1 = SqliteStorage::open(&db_path, &cfg).expect("open removing stale shm");
        assert!(!shm_path.exists());

        // Case 2: -wal is 0 bytes, -shm exists
        fs::write(&wal_path, b"").unwrap();
        fs::write(&shm_path, b"stale shm bytes").unwrap();
        let _s2 = SqliteStorage::open(&db_path, &cfg).expect("open removing shm with empty wal");
        assert!(!shm_path.exists());
    }

    #[test]
    fn purge_expired_sessions_handles_mixed_rfc3339_offsets() {
        let dir = TempDir::new("ps5-db-sqlite-purge");
        let db_path = dir.0.join("sessions.db");
        let cfg = DatabaseConfig {
            driver: "sqlite".to_string(),
            path: db_path.clone(),
            busy_timeout_ms: 5000,
            cache_size_kb: 4000,
            wal_mode: false,
            ..DatabaseConfig::default()
        };
        let storage = SqliteStorage::open(&db_path, &cfg).unwrap();
        storage.run_migrations().unwrap();

        storage.create_session(&make_session("exp-z", "2026-11-13T09:30:00Z")).unwrap();
        storage.create_session(&make_session("exp-eq", "2026-11-13T10:00:00+00:00")).unwrap();
        storage.create_session(&make_session("exp-tz", "2026-11-13T11:30:00+02:00")).unwrap();
        storage.create_session(&make_session("exp-bad", "not-a-timestamp")).unwrap();

        storage.create_session(&make_session("act-z", "2026-11-13T10:01:00Z")).unwrap();
        storage.create_session(&make_session("act-plus", "2026-11-13T10:30:00+00:00")).unwrap();
        storage.create_session(&make_session("act-tz", "2026-11-13T13:00:00+02:00")).unwrap();

        let purged = storage
            .purge_expired_sessions("2026-11-13T10:00:00Z")
            .unwrap();
        assert_eq!(purged, 4);

        assert!(storage.get_session_by_hash("hash-exp-z").unwrap().is_none());
        assert!(storage.get_session_by_hash("hash-exp-eq").unwrap().is_none());
        assert!(storage.get_session_by_hash("hash-exp-tz").unwrap().is_none());
        assert!(storage.get_session_by_hash("hash-exp-bad").unwrap().is_none());

        assert!(storage.get_session_by_hash("hash-act-z").unwrap().is_some());
        assert!(storage.get_session_by_hash("hash-act-plus").unwrap().is_some());
        assert!(storage.get_session_by_hash("hash-act-tz").unwrap().is_some());
    }

    #[test]
    fn migration_v2_attachments_and_incremental_auto_vacuum() {
        let dir = TempDir::new("ps5-db-sqlite-v2");
        let db_path = dir.0.join("v2.db");
        let cfg = DatabaseConfig {
            driver: "sqlite".to_string(),
            path: db_path.clone(),
            busy_timeout_ms: 5000,
            cache_size_kb: 4000,
            wal_mode: false,
            ..DatabaseConfig::default()
        };
        let storage = SqliteStorage::open(&db_path, &cfg).unwrap();
        storage.run_migrations().unwrap();

        {
            let conn = storage.lock_conn().unwrap();
            let auto_vac: i64 = conn.query_row("PRAGMA auto_vacuum", [], |r| r.get(0)).unwrap();
            assert_eq!(auto_vac, 2);
            let ver: i64 = conn
                .query_row("SELECT MAX(version) FROM schema_migrations", [], |r| r.get(0))
                .unwrap();
            assert_eq!(ver, crate::CURRENT_SCHEMA_VERSION);
            assert_eq!(ver, 6);
            let msg_timings_col_exists: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM pragma_table_info('messages') WHERE name = 'timings_json')",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert!(msg_timings_col_exists);
            let msg_conv_idx_exists: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'idx_messages_conv_created')",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert!(msg_conv_idx_exists);
            let schedules_table_exists: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'schedules')",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert!(schedules_table_exists);
            let schedules_index_exists: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'idx_schedules_enabled_next')",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert!(schedules_index_exists);
            let webhooks_table_exists: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'webhooks')",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert!(webhooks_table_exists);
            let webhooks_index_exists: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'idx_webhooks_enabled')",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert!(webhooks_index_exists);
            let mut stmt = conn.prepare("PRAGMA table_info(agents)").unwrap();
            let cols: Vec<String> = stmt
                .query_map([], |r| r.get::<_, String>(1))
                .unwrap()
                .map(|r| r.unwrap())
                .collect();
            for required in ["denied_tools_json", "sandbox_mode", "tool_profile"] {
                assert!(
                    cols.iter().any(|c| c == required),
                    "agents table missing column {required}"
                );
            }
        }

        storage
            .create_conversation(&Conversation {
                id: "c1".to_string(),
                title: "Attachments".to_string(),
                mode: ConversationMode::Direct {
                    agent_id: "commander".to_string(),
                },
                created_at: now_iso(),
                updated_at: now_iso(),
                archived: false,
                sort_order: 0,
            })
            .unwrap();

        let att = ChatAttachment {
            kind: "image".to_string(),
            name: "screen.png".to_string(),
            mime_type: "image/png".to_string(),
            data_url: Some("data:image/png;base64,AAAA".to_string()),
            workspace_path: None,
            size_bytes: 4,
        };

        storage
            .append_chat_message(&ChatMessage {
                id: "m1".to_string(),
                conversation_id: "c1".to_string(),
                role: ChatRole::User,
                agent_id: None,
                content: "check attachment".to_string(),
                reasoning: None,
                tool_calls_json: None,
                task_id: None,
                token_count: 3,
                latency_ms: None,
                archived: false,
                created_at: "2026-11-13T10:00:00Z".to_string(),
                attachments: vec![att.clone()],
                timings: None,
            })
            .unwrap();
        storage
            .append_chat_message(&ChatMessage {
                id: "m2".to_string(),
                conversation_id: "c1".to_string(),
                role: ChatRole::Assistant,
                agent_id: Some("commander".to_string()),
                content: "plain text".to_string(),
                reasoning: None,
                tool_calls_json: None,
                task_id: None,
                token_count: 2,
                latency_ms: Some(10),
                archived: true,
                created_at: "2026-11-13T10:00:01Z".to_string(),
                attachments: Vec::new(),
                timings: None,
            })
            .unwrap();

        let active = storage.list_chat_messages("c1", false, 10).unwrap();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].attachments, vec![att.clone()]);

        let all = storage.list_chat_messages("c1", true, 10).unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].attachments, vec![att.clone()]);
        assert!(all[1].attachments.is_empty());

        let hits = storage.search_chat_messages("attachment", 10).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].attachments, vec![att]);
    }

    #[test]
    fn task_counts_summary_aggregates_without_loading_rows() {
        let dir = TempDir::new("ps5-db-sqlite-summary");
        let db_path = dir.0.join("summary.db");
        let cfg = DatabaseConfig {
            driver: "sqlite".to_string(),
            path: db_path.clone(),
            busy_timeout_ms: 5000,
            cache_size_kb: 4000,
            wal_mode: false,
            ..DatabaseConfig::default()
        };
        let storage = SqliteStorage::open(&db_path, &cfg).unwrap();
        storage.run_migrations().unwrap();

        let mk_task = |id: &str, agent: &str, status: TaskStatus, created: &str, completed: Option<&str>| TaskRecord {
            id: id.to_string(),
            conversation_id: None,
            user_request: "req".to_string(),
            assigned_agent_id: agent.to_string(),
            parent_task_id: None,
            priority: 0,
            created_at: created.to_string(),
            started_at: None,
            completed_at: completed.map(str::to_string),
            status,
            result: None,
            error_info: None,
            has_side_effects: false,
            progress_pct: 0,
            progress_note: None,
        };

        storage.upsert_task(&mk_task("t1", "a1", TaskStatus::Pending, "2026-11-13T10:00:00Z", None)).unwrap();
        storage.upsert_task(&mk_task("t2", "a1", TaskStatus::Running, "2026-11-13T10:01:00Z", None)).unwrap();
        storage.upsert_task(&mk_task("t2a", "a1", TaskStatus::Assigned, "2026-11-13T11:59:00+02:00", None)).unwrap();
        storage.upsert_task(&mk_task("t3", "a1", TaskStatus::Waiting, "2026-11-13T10:02:00Z", None)).unwrap();
        storage.upsert_task(&mk_task("t4", "a1", TaskStatus::Completed, "2026-11-13T10:03:00Z", Some("2026-11-13T10:04:00Z"))).unwrap();
        storage.upsert_task(&mk_task("t5", "a2", TaskStatus::Failed, "2026-11-13T10:00:00Z", Some("2026-11-13T10:01:00Z"))).unwrap();
        storage.upsert_task(&mk_task("t6", "a2", TaskStatus::Cancelled, "2026-11-13T10:02:00Z", Some("2026-11-13T10:02:30Z"))).unwrap();

        let sum = storage.task_counts_summary().unwrap();
        assert_eq!(sum.total, 7);
        assert_eq!(sum.pending, 1);
        assert_eq!(sum.running, 2);
        assert_eq!(sum.waiting, 1);
        assert_eq!(sum.completed, 1);
        assert_eq!(sum.failed, 1);
        assert_eq!(sum.cancelled, 1);

        let a1 = sum.per_agent.get("a1").unwrap();
        assert_eq!(a1.completed, 1);
        assert_eq!(a1.failed, 0);
        assert_eq!(a1.active, 3);
        assert_eq!(a1.latest_active_task_id.as_deref(), Some("t3"));
        assert_eq!(a1.last_activity.as_deref(), Some("2026-11-13T10:04:00Z"));

        let a2 = sum.per_agent.get("a2").unwrap();
        assert_eq!(a2.completed, 0);
        assert_eq!(a2.failed, 1);
        assert_eq!(a2.active, 0);
        assert_eq!(a2.latest_active_task_id, None);
        assert_eq!(a2.last_activity.as_deref(), Some("2026-11-13T10:02:30Z"));
    }

    #[test]
    fn prune_old_records_respects_retention_caps_and_zero_unlimited() {
        let dir = TempDir::new("ps5-db-sqlite-prune");
        let db_path = dir.0.join("prune.db");
        let cfg = DatabaseConfig {
            driver: "sqlite".to_string(),
            path: db_path.clone(),
            busy_timeout_ms: 5000,
            cache_size_kb: 4000,
            wal_mode: false,
            ..DatabaseConfig::default()
        };
        let storage = SqliteStorage::open(&db_path, &cfg).unwrap();
        storage.run_migrations().unwrap();

        let old_ts = "2020-01-01T00:00:00Z";
        let fresh_ts = now_iso();

        // Sessions: 1 expired, 1 revoked, 1 active
        storage.create_session(&make_session("s-exp", old_ts)).unwrap();
        let mut s_rev = make_session("s-rev", "2099-01-01T00:00:00Z");
        s_rev.revoked = true;
        storage.create_session(&s_rev).unwrap();
        storage.create_session(&make_session("s-act", "2099-01-01T00:00:00Z")).unwrap();

        // Tasks: 1 old completed, 1 old running (must be kept), 1 fresh completed
        for (id, status, ts) in [
            ("t-old-done", TaskStatus::Completed, old_ts),
            ("t-old-run", TaskStatus::Running, old_ts),
            ("t-new-done", TaskStatus::Completed, fresh_ts.as_str()),
        ] {
            storage
                .upsert_task(&TaskRecord {
                    id: id.to_string(),
                    conversation_id: None,
                    user_request: "u".to_string(),
                    assigned_agent_id: "commander".to_string(),
                    parent_task_id: None,
                    priority: 0,
                    created_at: ts.to_string(),
                    started_at: None,
                    completed_at: if status.is_terminal() { Some(ts.to_string()) } else { None },
                    status,
                    result: None,
                    error_info: None,
                    has_side_effects: false,
                    progress_pct: 0,
                    progress_note: None,
                })
                .unwrap();
        }
        storage
            .append_task_event(&TaskEvent {
                id: "ev-old".to_string(),
                task_id: "t-old-done".to_string(),
                timestamp: old_ts.to_string(),
                event_type: "done".to_string(),
                details: "ok".to_string(),
            })
            .unwrap();

        // Messages: 1 old archived (deleted), 1 old non-archived (kept)
        storage
            .create_conversation(&Conversation {
                id: "conv-p".to_string(),
                title: "Prune".to_string(),
                mode: ConversationMode::Direct {
                    agent_id: "commander".to_string(),
                },
                created_at: old_ts.to_string(),
                updated_at: old_ts.to_string(),
                archived: false,
                sort_order: 0,
            })
            .unwrap();
        storage
            .append_chat_message(&ChatMessage {
                id: "m-arch-old".to_string(),
                conversation_id: "conv-p".to_string(),
                role: ChatRole::User,
                agent_id: None,
                content: "old archived".to_string(),
                reasoning: None,
                tool_calls_json: None,
                task_id: None,
                token_count: 1,
                latency_ms: None,
                archived: true,
                created_at: old_ts.to_string(),
                attachments: Vec::new(),
                timings: None,
            })
            .unwrap();
        storage
            .append_chat_message(&ChatMessage {
                id: "m-live-old".to_string(),
                conversation_id: "conv-p".to_string(),
                role: ChatRole::User,
                agent_id: None,
                content: "old active".to_string(),
                reasoning: None,
                tool_calls_json: None,
                task_id: None,
                token_count: 1,
                latency_ms: None,
                archived: false,
                created_at: old_ts.to_string(),
                attachments: Vec::new(),
                timings: None,
            })
            .unwrap();

        // Audit logs: 3 fresh entries to test max_rows_per_table = 2
        for i in 1..=3 {
            storage
                .append_audit_log(&AuditLogEntry {
                    id: format!("aud-{i}"),
                    timestamp: fresh_ts.clone(),
                    actor: "admin".to_string(),
                    action: "test".to_string(),
                    target: "t".to_string(),
                    severity: "info".to_string(),
                    outcome: "ok".to_string(),
                    details_json: "{}".to_string(),
                })
                .unwrap();
        }

        // First call with 0, 0 only purges expired/revoked sessions
        let r0 = storage.prune_old_records(0, 0).unwrap();
        assert_eq!(r0.purged_sessions, 2);
        assert_eq!(r0.deleted_tasks, 0);
        assert_eq!(r0.deleted_audit_logs, 0);
        assert_eq!(r0.deleted_chat_messages, 0);

        // Now prune with 30 days retention and max 2 rows per table
        let r1 = storage.prune_old_records(30, 2).unwrap();
        assert_eq!(r1.deleted_tasks, 1);
        assert_eq!(r1.deleted_task_events, 1);
        assert_eq!(r1.deleted_chat_messages, 1);
        assert_eq!(r1.deleted_audit_logs, 1);

        assert!(storage.get_task("t-old-done").unwrap().is_none());
        assert!(storage.get_task("t-old-run").unwrap().is_some());
        assert!(storage.get_task("t-new-done").unwrap().is_some());
        assert_eq!(storage.list_chat_messages("conv-p", true, 10).unwrap().len(), 1);
        assert_eq!(storage.list_audit_logs(10).unwrap().len(), 2);

        storage.checkpoint().unwrap();
        assert_eq!(storage.integrity_check().unwrap(), "ok");
    }

    #[test]
    fn reorder_conversations_updates_sort_order_and_list_ordering() {
        let dir = TempDir::new("ps5-db-sqlite-reorder");
        let db_path = dir.0.join("reorder.db");
        let cfg = DatabaseConfig {
            driver: "sqlite".to_string(),
            path: db_path.clone(),
            busy_timeout_ms: 5000,
            cache_size_kb: 4000,
            wal_mode: false,
            ..DatabaseConfig::default()
        };
        let storage = SqliteStorage::open(&db_path, &cfg).unwrap();
        storage.run_migrations().unwrap();

        for (id, updated_at) in [
            ("c1", "2026-11-13T10:00:00Z"),
            ("c2", "2026-11-13T10:01:00Z"),
            ("c3", "2026-11-13T10:02:00Z"),
        ] {
            storage
                .create_conversation(&Conversation {
                    id: id.to_string(),
                    title: id.to_string(),
                    mode: ConversationMode::Direct {
                        agent_id: "commander".to_string(),
                    },
                    created_at: updated_at.to_string(),
                    updated_at: updated_at.to_string(),
                    archived: false,
                    sort_order: 0,
                })
                .unwrap();
        }

        // Default sort_order=0 orders by updated_at DESC
        let initial_ids: Vec<String> = storage
            .list_conversations(false)
            .unwrap()
            .into_iter()
            .map(|c| c.id)
            .collect();
        assert_eq!(initial_ids, vec!["c3", "c2", "c1"]);

        storage
            .reorder_conversations(&["c1".to_string(), "c3".to_string(), "c2".to_string()])
            .unwrap();

        let reordered = storage.list_conversations(false).unwrap();
        let reordered_ids: Vec<String> = reordered.iter().map(|c| c.id.clone()).collect();
        assert_eq!(reordered_ids, vec!["c1", "c3", "c2"]);
        assert_eq!(reordered[0].sort_order, 10);
        assert_eq!(reordered[1].sort_order, 20);
        assert_eq!(reordered[2].sort_order, 30);
        assert_eq!(storage.get_conversation("c3").unwrap().unwrap().sort_order, 20);
    }

    #[test]
    fn schedules_crud_ordering_limit_and_kinds() {
        let dir = TempDir::new("ps5-db-sqlite-schedules");
        let db_path = dir.0.join("schedules.db");
        let cfg = DatabaseConfig {
            driver: "sqlite".to_string(),
            path: db_path.clone(),
            busy_timeout_ms: 5000,
            cache_size_kb: 4000,
            wal_mode: false,
            ..DatabaseConfig::default()
        };
        let storage = SqliteStorage::open(&db_path, &cfg).unwrap();
        storage.run_migrations().unwrap();

        let dt = |s: &str| {
            chrono::DateTime::parse_from_rfc3339(s)
                .unwrap()
                .with_timezone(&chrono::Utc)
        };

        assert!(storage.get_schedule("missing").unwrap().is_none());
        assert!(storage.list_schedules(0).unwrap().is_empty());

        let s_once = ScheduleRecord {
            id: "s-once".to_string(),
            title: "Once".to_string(),
            prompt: "Run once".to_string(),
            target_agent_id: Some("commander".to_string()),
            conversation_id: Some("conv-1".to_string()),
            kind: ScheduleKind::Once,
            run_at: Some(dt("2026-11-14T10:00:00Z")),
            interval_secs: None,
            daily_time: None,
            cron_expr: None,
            timezone_offset_mins: 60,
            enabled: true,
            next_run_at: Some(dt("2026-11-14T09:00:00Z")),
            last_run_at: None,
            last_status: None,
            last_error: None,
            last_task_id: None,
            run_count: 0,
            created_at: dt("2026-11-13T10:00:00Z"),
            updated_at: dt("2026-11-13T10:00:00Z"),
        };
        let s_interval = ScheduleRecord {
            id: "s-a-interval".to_string(),
            title: "Interval".to_string(),
            prompt: "Check logs".to_string(),
            target_agent_id: Some("analyst".to_string()),
            conversation_id: None,
            kind: ScheduleKind::Interval,
            run_at: None,
            interval_secs: Some(1800),
            daily_time: None,
            cron_expr: None,
            timezone_offset_mins: 0,
            enabled: true,
            next_run_at: Some(dt("2026-11-13T12:30:00Z")),
            last_run_at: Some(dt("2026-11-13T12:00:00Z")),
            last_status: Some("completed".to_string()),
            last_error: None,
            last_task_id: Some("task-1".to_string()),
            run_count: 4,
            created_at: dt("2026-11-13T12:00:00Z"),
            updated_at: dt("2026-11-13T12:00:00Z"),
        };
        let s_daily = ScheduleRecord {
            id: "s-b-daily".to_string(),
            title: "Daily".to_string(),
            prompt: "Morning summary".to_string(),
            target_agent_id: None,
            conversation_id: None,
            kind: ScheduleKind::Daily,
            run_at: None,
            interval_secs: None,
            daily_time: Some("08:00".to_string()),
            cron_expr: None,
            timezone_offset_mins: -300,
            enabled: false,
            next_run_at: None,
            last_run_at: Some(dt("2026-11-13T11:00:00Z")),
            last_status: Some("failed".to_string()),
            last_error: Some("timeout".to_string()),
            last_task_id: Some("task-2".to_string()),
            run_count: 1,
            created_at: dt("2026-11-13T12:00:00Z"),
            updated_at: dt("2026-11-13T12:05:00Z"),
        };
        let s_cron = ScheduleRecord {
            id: "s-cron".to_string(),
            title: "Cron".to_string(),
            prompt: "Weekday check".to_string(),
            target_agent_id: Some("executor".to_string()),
            conversation_id: None,
            kind: ScheduleKind::Cron,
            run_at: None,
            interval_secs: None,
            daily_time: None,
            cron_expr: Some("0 9 * * 1-5".to_string()),
            timezone_offset_mins: 60,
            enabled: true,
            next_run_at: Some(dt("2026-11-14T08:00:00Z")),
            last_run_at: None,
            last_status: None,
            last_error: None,
            last_task_id: None,
            run_count: 0,
            created_at: dt("2026-11-13T14:00:00.250Z"),
            updated_at: dt("2026-11-13T14:00:00.250Z"),
        };

        for sched in [&s_once, &s_daily, &s_cron, &s_interval] {
            storage.upsert_schedule(sched).unwrap();
        }

        // Verify stored TEXT kind encodings match "once" | "interval" | "daily" | "cron".
        {
            let conn = storage.lock_conn().unwrap();
            for (id, expected_kind) in [
                ("s-once", "once"),
                ("s-a-interval", "interval"),
                ("s-b-daily", "daily"),
                ("s-cron", "cron"),
            ] {
                let raw_kind: String = conn
                    .query_row(
                        "SELECT kind FROM schedules WHERE id = ?1",
                        params![id],
                        |r| r.get(0),
                    )
                    .unwrap();
                assert_eq!(raw_kind, expected_kind);
            }
        }

        assert_eq!(storage.get_schedule("s-once").unwrap().unwrap(), s_once);
        assert_eq!(
            storage.get_schedule("s-a-interval").unwrap().unwrap(),
            s_interval
        );
        assert_eq!(storage.get_schedule("s-b-daily").unwrap().unwrap(), s_daily);
        assert_eq!(storage.get_schedule("s-cron").unwrap().unwrap(), s_cron);

        let all = storage.list_schedules(0).unwrap();
        let all_ids: Vec<&str> = all.iter().map(|s| s.id.as_str()).collect();
        assert_eq!(
            all_ids,
            vec!["s-cron", "s-a-interval", "s-b-daily", "s-once"]
        );

        let top_two = storage.list_schedules(2).unwrap();
        let top_two_ids: Vec<&str> = top_two.iter().map(|s| s.id.as_str()).collect();
        assert_eq!(top_two_ids, vec!["s-cron", "s-a-interval"]);

        // Upsert existing record updates all fields cleanly.
        let updated = ScheduleRecord {
            title: "Once (Updated)".to_string(),
            enabled: false,
            next_run_at: None,
            last_run_at: Some(dt("2026-11-14T10:00:05Z")),
            last_status: Some("completed".to_string()),
            last_task_id: Some("task-once".to_string()),
            run_count: 1,
            updated_at: dt("2026-11-14T10:00:05Z"),
            ..s_once.clone()
        };
        storage.upsert_schedule(&updated).unwrap();
        assert_eq!(storage.get_schedule("s-once").unwrap().unwrap(), updated);

        storage.delete_schedule("s-once").unwrap();
        assert!(storage.get_schedule("s-once").unwrap().is_none());
        storage.delete_schedule("s-once").unwrap();
        assert_eq!(storage.list_schedules(0).unwrap().len(), 3);
    }

    #[test]
    fn list_due_schedules_filters_sorts_and_respects_limit() {
        let dir = TempDir::new("ps5-db-sqlite-due-schedules");
        let db_path = dir.0.join("due_schedules.db");
        let cfg = DatabaseConfig {
            driver: "sqlite".to_string(),
            path: db_path.clone(),
            busy_timeout_ms: 5000,
            cache_size_kb: 4000,
            wal_mode: false,
            ..DatabaseConfig::default()
        };
        let storage = SqliteStorage::open(&db_path, &cfg).unwrap();
        storage.run_migrations().unwrap();

        let dt = |s: &str| {
            chrono::DateTime::parse_from_rfc3339(s)
                .unwrap()
                .with_timezone(&chrono::Utc)
        };
        let now = dt("2026-11-13T12:00:00Z");

        let make = |id: &str, enabled: bool, next_run_at: Option<&str>| ScheduleRecord {
            id: id.to_string(),
            title: id.to_string(),
            prompt: format!("prompt-{id}"),
            target_agent_id: None,
            conversation_id: None,
            kind: ScheduleKind::Interval,
            run_at: None,
            interval_secs: Some(60),
            daily_time: None,
            cron_expr: None,
            timezone_offset_mins: 0,
            enabled,
            next_run_at: next_run_at.map(dt),
            last_run_at: None,
            last_status: None,
            last_error: None,
            last_task_id: None,
            run_count: 0,
            created_at: dt("2026-11-13T10:00:00Z"),
            updated_at: dt("2026-11-13T10:00:00Z"),
        };

        for s in [
            make("d-disabled", false, Some("2026-11-13T11:00:00Z")),
            make("d-null-next", true, None),
            make("d-future", true, Some("2026-11-13T12:00:01Z")),
            make("d-tie-b", true, Some("2026-11-13T11:45:00Z")),
            make("d-exact-now", true, Some("2026-11-13T12:00:00Z")),
            make("d-earliest", true, Some("2026-11-13T11:30:00Z")),
            make("d-tie-a", true, Some("2026-11-13T11:45:00Z")),
        ] {
            storage.upsert_schedule(&s).unwrap();
        }

        let due_all = storage.list_due_schedules(now, 0).unwrap();
        let due_ids: Vec<&str> = due_all.iter().map(|s| s.id.as_str()).collect();
        assert_eq!(
            due_ids,
            vec!["d-earliest", "d-tie-a", "d-tie-b", "d-exact-now"]
        );

        let due_two = storage.list_due_schedules(now, 2).unwrap();
        let due_two_ids: Vec<&str> = due_two.iter().map(|s| s.id.as_str()).collect();
        assert_eq!(due_two_ids, vec!["d-earliest", "d-tie-a"]);
    }

    #[test]
    fn agents_policy_fields_and_idempotent_v5_migration() {
        let dir = TempDir::new("ps5-db-sqlite-agent-policy");
        let db_path = dir.0.join("agents.db");
        let cfg = DatabaseConfig {
            driver: "sqlite".to_string(),
            path: db_path.clone(),
            busy_timeout_ms: 5000,
            cache_size_kb: 4000,
            wal_mode: false,
            ..DatabaseConfig::default()
        };
        let storage = SqliteStorage::open(&db_path, &cfg).unwrap();
        storage.run_migrations().unwrap();

        let agent = AgentDefinition {
            id: "agent-policy".to_string(),
            name: "Policy Agent".to_string(),
            description: "Tests sandbox & tool policy columns".to_string(),
            role: AgentRole::Programmer,
            role_label: "Programmer".to_string(),
            system_prompt: "be safe".to_string(),
            model_config: AgentModelConfig::default(),
            enabled_tools: vec!["read_file".to_string()],
            denied_tools: vec!["shell".to_string(), "write_file".to_string()],
            sandbox_mode: Some("workspace_write".to_string()),
            tool_profile: Some("minimal".to_string()),
            memory_permissions: MemoryPermissions::default(),
            max_task_duration_secs: 120,
            max_reasoning_iterations: 12,
            priority: 5,
            can_communicate: true,
            enabled: true,
            is_builtin: false,
            is_protected: false,
            is_temporary: false,
            created_at: "2026-11-13T10:00:00Z".to_string(),
            updated_at: "2026-11-13T10:00:00Z".to_string(),
        };
        storage.upsert_agent(&agent).unwrap();

        let got = storage.get_agent("agent-policy").unwrap().unwrap();
        assert_eq!(
            got.denied_tools,
            vec!["shell".to_string(), "write_file".to_string()]
        );
        assert_eq!(got.sandbox_mode.as_deref(), Some("workspace_write"));
        assert_eq!(got.tool_profile.as_deref(), Some("minimal"));

        let listed = storage.list_agents().unwrap();
        let listed_agent = listed.iter().find(|a| a.id == "agent-policy").unwrap();
        assert_eq!(listed_agent.denied_tools, got.denied_tools);
        assert_eq!(listed_agent.sandbox_mode, got.sandbox_mode);
        assert_eq!(listed_agent.tool_profile, got.tool_profile);

        let cleared = AgentDefinition {
            denied_tools: Vec::new(),
            sandbox_mode: None,
            tool_profile: None,
            updated_at: "2026-11-13T10:05:00Z".to_string(),
            ..agent.clone()
        };
        storage.upsert_agent(&cleared).unwrap();
        let got2 = storage.get_agent("agent-policy").unwrap().unwrap();
        assert!(got2.denied_tools.is_empty());
        assert!(got2.sandbox_mode.is_none());
        assert!(got2.tool_profile.is_none());

        // Simulate a DB whose agents/messages tables already have v5/v6 columns before schema_migrations records them.
        {
            let conn = storage.lock_conn().unwrap();
            conn.execute_batch(
                "DELETE FROM schema_migrations WHERE version >= 5;
                 DROP TABLE IF EXISTS webhooks;",
            )
            .unwrap();
        }
        storage.upsert_agent(&agent).unwrap();
        storage.run_migrations().unwrap();

        let ver: i64 = {
            let conn = storage.lock_conn().unwrap();
            conn.query_row("SELECT MAX(version) FROM schema_migrations", [], |r| r.get(0))
                .unwrap()
        };
        assert_eq!(ver, 6);
        let got_after = storage.get_agent("agent-policy").unwrap().unwrap();
        assert_eq!(got_after.denied_tools, agent.denied_tools);
        assert_eq!(got_after.sandbox_mode, agent.sandbox_mode);
        assert_eq!(got_after.tool_profile, agent.tool_profile);
        assert!(storage.list_webhooks().unwrap().is_empty());
    }

    #[test]
    fn webhooks_crud_lifecycle() {
        let dir = TempDir::new("ps5-db-sqlite-webhooks");
        let db_path = dir.0.join("webhooks.db");
        let cfg = DatabaseConfig {
            driver: "sqlite".to_string(),
            path: db_path.clone(),
            busy_timeout_ms: 5000,
            cache_size_kb: 4000,
            wal_mode: false,
            ..DatabaseConfig::default()
        };
        let storage = SqliteStorage::open(&db_path, &cfg).unwrap();
        storage.run_migrations().unwrap();

        assert!(storage.list_webhooks().unwrap().is_empty());
        assert!(storage.get_webhook("missing").unwrap().is_none());

        let w1 = WebhookRecord {
            id: "wh-1".to_string(),
            name: "Deploy Hook".to_string(),
            enabled: true,
            secret_hash: "sha256-1".to_string(),
            secret_prefix: "whsec_11".to_string(),
            target_agent_id: "commander".to_string(),
            conversation_id: Some("conv-1".to_string()),
            prompt_template: "Deploy {{ref}}".to_string(),
            created_at: "2026-11-13T10:00:00Z".to_string(),
            updated_at: "2026-11-13T10:00:00Z".to_string(),
            last_triggered_at: None,
            trigger_count: 0,
        };
        let w2 = WebhookRecord {
            id: "wh-2".to_string(),
            name: "Alert Hook".to_string(),
            enabled: false,
            secret_hash: "sha256-2".to_string(),
            secret_prefix: "whsec_22".to_string(),
            target_agent_id: "analyst".to_string(),
            conversation_id: None,
            prompt_template: "Alert {{body}}".to_string(),
            created_at: "2026-11-13T10:05:00Z".to_string(),
            updated_at: "2026-11-13T10:05:00Z".to_string(),
            last_triggered_at: Some("2026-11-13T10:06:00Z".to_string()),
            trigger_count: 4,
        };

        storage.upsert_webhook(&w1).unwrap();
        storage.upsert_webhook(&w2).unwrap();

        let listed = storage.list_webhooks().unwrap();
        let ids: Vec<&str> = listed.iter().map(|w| w.id.as_str()).collect();
        assert_eq!(ids, vec!["wh-2", "wh-1"]);

        let got2 = storage.get_webhook("wh-2").unwrap().unwrap();
        assert!(!got2.enabled);
        assert!(got2.conversation_id.is_none());
        assert_eq!(
            got2.last_triggered_at.as_deref(),
            Some("2026-11-13T10:06:00Z")
        );
        assert_eq!(got2.trigger_count, 4);

        let w1_updated = WebhookRecord {
            enabled: false,
            prompt_template: "Updated {{ref}}".to_string(),
            updated_at: "2026-11-13T11:00:00Z".to_string(),
            last_triggered_at: Some("2026-11-13T11:00:00Z".to_string()),
            trigger_count: 1,
            ..w1
        };
        storage.upsert_webhook(&w1_updated).unwrap();
        let got1 = storage.get_webhook("wh-1").unwrap().unwrap();
        assert!(!got1.enabled);
        assert_eq!(got1.prompt_template, "Updated {{ref}}");
        assert_eq!(
            got1.last_triggered_at.as_deref(),
            Some("2026-11-13T11:00:00Z")
        );
        assert_eq!(got1.trigger_count, 1);

        assert!(storage.delete_webhook("wh-1").unwrap());
        assert!(!storage.delete_webhook("wh-1").unwrap());
        assert!(storage.get_webhook("wh-1").unwrap().is_none());
        assert_eq!(storage.list_webhooks().unwrap().len(), 1);
    }

    #[test]
    fn prune_old_records_parity_tiers_active_sessions_and_eligible_caps() {
        let dir = TempDir::new("ps5-db-sqlite-prune-parity");
        let db_path = dir.0.join("prune-parity.db");
        let cfg = DatabaseConfig {
            driver: "sqlite".to_string(),
            path: db_path.clone(),
            busy_timeout_ms: 5000,
            cache_size_kb: 4000,
            wal_mode: false,
            ..DatabaseConfig::default()
        };
        let storage = SqliteStorage::open(&db_path, &cfg).unwrap();
        storage.run_migrations().unwrap();

        let old_ts = "2020-01-01T00:00:00Z";
        let mid_ts = "2020-01-02T00:00:00Z";
        let fresh_ts = now_iso();

        // 3 active non-expired non-revoked sessions must never be evicted by max_rows_per_table.
        for s in ["s-1", "s-2", "s-3"] {
            storage
                .create_session(&make_session(s, "2099-01-01T00:00:00Z"))
                .unwrap();
        }

        // Tasks: 1 older active task + 1 newer terminal task.
        storage
            .upsert_task(&TaskRecord {
                id: "t-active".to_string(),
                conversation_id: None,
                user_request: "active".to_string(),
                assigned_agent_id: "commander".to_string(),
                parent_task_id: None,
                priority: 0,
                created_at: old_ts.to_string(),
                started_at: None,
                completed_at: None,
                status: TaskStatus::Running,
                result: None,
                error_info: None,
                has_side_effects: false,
                progress_pct: 0,
                progress_note: None,
            })
            .unwrap();
        storage
            .upsert_task(&TaskRecord {
                id: "t-term".to_string(),
                conversation_id: None,
                user_request: "term".to_string(),
                assigned_agent_id: "commander".to_string(),
                parent_task_id: None,
                priority: 0,
                created_at: fresh_ts.clone(),
                started_at: None,
                completed_at: Some(fresh_ts.clone()),
                status: TaskStatus::Completed,
                result: None,
                error_info: None,
                has_side_effects: false,
                progress_pct: 100,
                progress_note: None,
            })
            .unwrap();

        // Task events: oldest belongs to active task (tier 1), mid & new belong to terminal task (tier 0).
        for (id, task_id, ts) in [
            ("ev-act-old", "t-active", old_ts),
            ("ev-term-mid", "t-term", mid_ts),
            ("ev-term-new", "t-term", fresh_ts.as_str()),
        ] {
            storage
                .append_task_event(&TaskEvent {
                    id: id.to_string(),
                    task_id: task_id.to_string(),
                    timestamp: ts.to_string(),
                    event_type: "step".to_string(),
                    details: "ok".to_string(),
                })
                .unwrap();
        }

        // Agent messages: oldest belongs to active task (tier 1), mid is unlinked (tier 0), new belongs to terminal task.
        for (id, task_id, ts) in [
            ("am-act-old", Some("t-active"), old_ts),
            ("am-orphan-mid", None, mid_ts),
            ("am-term-new", Some("t-term"), fresh_ts.as_str()),
        ] {
            storage
                .append_agent_message(&AgentEnvelope {
                    id: id.to_string(),
                    sender_agent_id: "commander".to_string(),
                    recipient_agent_id: "programmer".to_string(),
                    task_id: task_id.map(str::to_string),
                    timestamp: ts.to_string(),
                    message_type: AgentMessageType::TaskUpdate,
                    content: id.to_string(),
                    status: DeliveryStatus::Delivered,
                    hop_count: 1,
                })
                .unwrap();
        }

        // Approvals: 1 older pending (ineligible) + 1 newer resolved (eligible).
        storage
            .upsert_approval(&ApprovalRecord {
                id: "app-pending".to_string(),
                task_id: Some("t-active".to_string()),
                agent_id: "commander".to_string(),
                operation: "op".to_string(),
                tool_id: Some("shell".to_string()),
                arguments_json: "{}".to_string(),
                reason: "wait".to_string(),
                status: ApprovalStatus::Pending,
                created_at: old_ts.to_string(),
                resolved_at: None,
                resolved_by: None,
            })
            .unwrap();
        storage
            .upsert_approval(&ApprovalRecord {
                id: "app-resolved".to_string(),
                task_id: Some("t-active".to_string()),
                agent_id: "commander".to_string(),
                operation: "op".to_string(),
                tool_id: Some("shell".to_string()),
                arguments_json: "{}".to_string(),
                reason: "done".to_string(),
                status: ApprovalStatus::Approved,
                created_at: fresh_ts.clone(),
                resolved_at: Some(fresh_ts.clone()),
                resolved_by: Some("admin".to_string()),
            })
            .unwrap();

        // Messages: 1 older non-archived (ineligible) + 1 newer archived (eligible).
        storage
            .create_conversation(&Conversation {
                id: "conv-cap".to_string(),
                title: "Cap".to_string(),
                mode: ConversationMode::Direct {
                    agent_id: "commander".to_string(),
                },
                created_at: old_ts.to_string(),
                updated_at: old_ts.to_string(),
                archived: false,
                sort_order: 0,
            })
            .unwrap();
        storage
            .append_chat_message(&ChatMessage {
                id: "m-keep".to_string(),
                conversation_id: "conv-cap".to_string(),
                role: ChatRole::User,
                agent_id: None,
                content: "keep".to_string(),
                reasoning: None,
                tool_calls_json: None,
                task_id: None,
                token_count: 1,
                latency_ms: None,
                archived: false,
                created_at: old_ts.to_string(),
                attachments: Vec::new(),
                timings: None,
            })
            .unwrap();
        storage
            .append_chat_message(&ChatMessage {
                id: "m-arch-new".to_string(),
                conversation_id: "conv-cap".to_string(),
                role: ChatRole::User,
                agent_id: None,
                content: "archive".to_string(),
                reasoning: None,
                tool_calls_json: None,
                task_id: None,
                token_count: 1,
                latency_ms: None,
                archived: true,
                created_at: fresh_ts.clone(),
                attachments: Vec::new(),
                timings: None,
            })
            .unwrap();

        // Pass A: cap=2 removes tier-0 non-active event/message before older active-task rows, keeping all 3 sessions.
        let r_a = storage.prune_old_records(0, 2).unwrap();
        assert_eq!(r_a.purged_sessions, 0);
        assert_eq!(r_a.deleted_tasks, 0);
        assert_eq!(r_a.deleted_task_events, 1);
        assert_eq!(r_a.deleted_agent_messages, 1);
        assert_eq!(storage.list_task_events("t-active").unwrap().len(), 1);
        assert_eq!(storage.list_task_events("t-term").unwrap().len(), 1);
        assert_eq!(
            storage
                .list_agent_messages(Some("t-active"), None, 10)
                .unwrap()
                .len(),
            1
        );
        assert!(storage
            .list_agent_messages(None, None, 10)
            .unwrap()
            .iter()
            .all(|m| m.id != "am-orphan-mid"));

        // Pass B: retention_days=30 deletes an ancient orphan agent_message while keeping ancient active-task rows.
        storage
            .append_agent_message(&AgentEnvelope {
                id: "am-orphan-ancient".to_string(),
                sender_agent_id: "commander".to_string(),
                recipient_agent_id: "programmer".to_string(),
                task_id: None,
                timestamp: old_ts.to_string(),
                message_type: AgentMessageType::TaskUpdate,
                content: "ancient".to_string(),
                status: DeliveryStatus::Delivered,
                hop_count: 1,
            })
            .unwrap();
        let r_b = storage.prune_old_records(30, 0).unwrap();
        assert_eq!(r_b.deleted_agent_messages, 1);
        assert_eq!(storage.list_task_events("t-active").unwrap().len(), 1);
        assert_eq!(
            storage
                .list_agent_messages(Some("t-active"), None, 10)
                .unwrap()
                .len(),
            1
        );

        // Pass C: cap=1 evicts newer eligible rows even when an older ineligible row occupies the table.
        let r_c = storage.prune_old_records(0, 1).unwrap();
        assert_eq!(r_c.purged_sessions, 0);
        assert_eq!(r_c.deleted_tasks, 1);
        assert_eq!(r_c.deleted_chat_messages, 1);
        assert!(storage.get_task("t-active").unwrap().is_some());
        assert!(storage.get_task("t-term").unwrap().is_none());
        let remaining_apps = storage.list_approvals(false, 10).unwrap();
        assert_eq!(remaining_apps.len(), 1);
        assert_eq!(remaining_apps[0].id, "app-pending");
        let remaining_msgs = storage.list_chat_messages("conv-cap", true, 10).unwrap();
        assert_eq!(remaining_msgs.len(), 1);
        assert_eq!(remaining_msgs[0].id, "m-keep");
        for s in ["s-1", "s-2", "s-3"] {
            assert!(storage
                .get_session_by_hash(&format!("hash-{s}"))
                .unwrap()
                .is_some());
        }
    }

    #[test]
    fn wal_to_truncate_recovery_and_search_trim() {
        let dir = TempDir::new("ps5-db-sqlite-wal-truncate");
        let db_path = dir.0.join("wal-to-truncate.db");
        let wal_cfg = DatabaseConfig {
            driver: "sqlite".to_string(),
            path: db_path.clone(),
            busy_timeout_ms: 5000,
            cache_size_kb: 4000,
            wal_mode: true,
            ..DatabaseConfig::default()
        };

        {
            let storage = SqliteStorage::open(&db_path, &wal_cfg).unwrap();
            storage.run_migrations().unwrap();
            storage
                .create_conversation(&Conversation {
                    id: "c-wal".to_string(),
                    title: "WAL".to_string(),
                    mode: ConversationMode::Direct {
                        agent_id: "commander".to_string(),
                    },
                    created_at: "2026-11-13T10:00:00Z".to_string(),
                    updated_at: "2026-11-13T10:00:00Z".to_string(),
                    archived: false,
                    sort_order: 0,
                })
                .unwrap();
            storage
                .append_chat_message(&ChatMessage {
                    id: "m-wal".to_string(),
                    conversation_id: "c-wal".to_string(),
                    role: ChatRole::User,
                    agent_id: None,
                    content: "Needle inside WAL body".to_string(),
                    reasoning: None,
                    tool_calls_json: None,
                    task_id: None,
                    token_count: 4,
                    latency_ms: None,
                    archived: false,
                    created_at: "2026-11-13T10:00:01Z".to_string(),
                    attachments: Vec::new(),
                    timings: None,
                })
                .unwrap();

            assert!(storage.search_chat_messages("   \t\n ", 10).unwrap().is_empty());
            let trimmed_hits = storage.search_chat_messages("  needle  ", 10).unwrap();
            assert_eq!(trimmed_hits.len(), 1);
            assert_eq!(trimmed_hits[0].id, "m-wal");
        }

        let trunc_cfg = DatabaseConfig {
            wal_mode: false,
            ..wal_cfg
        };
        let recovered = SqliteStorage::open(&db_path, &trunc_cfg).unwrap();
        {
            let conn = recovered.lock_conn().unwrap();
            let journal: String = conn
                .query_row("PRAGMA journal_mode", [], |r| r.get(0))
                .unwrap();
            let locking: String = conn
                .query_row("PRAGMA locking_mode", [], |r| r.get(0))
                .unwrap();
            assert!(
                journal.eq_ignore_ascii_case("truncate"),
                "expected truncate journal_mode, got {journal}"
            );
            assert!(
                locking.eq_ignore_ascii_case("normal"),
                "expected normal locking_mode, got {locking}"
            );
        }
        let msgs = recovered.list_chat_messages("c-wal", false, 10).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].id, "m-wal");
        let post_hits = recovered.search_chat_messages("  NEEDLE ", 10).unwrap();
        assert_eq!(post_hits.len(), 1);
        assert_eq!(recovered.integrity_check().unwrap(), "ok");
    }

    #[test]
    fn message_timings_roundtrip_update_and_malformed_json_fallback() {
        let storage = SqliteStorage::open(Path::new(":memory:"), &DatabaseConfig::default()).unwrap();
        storage.run_migrations().unwrap();
        storage
            .create_conversation(&Conversation {
                id: "c-timings".to_string(),
                title: "Timings".to_string(),
                mode: ConversationMode::Direct {
                    agent_id: "commander".to_string(),
                },
                created_at: "2026-11-14T10:00:00Z".to_string(),
                updated_at: "2026-11-14T10:00:00Z".to_string(),
                archived: false,
                sort_order: 0,
            })
            .unwrap();

        let t1 = InferenceTimings {
            predicted_per_second: Some(33.3),
            prompt_per_second: Some(150.5),
            predicted_ms: Some(900.0),
            prompt_ms: Some(120.0),
            predicted_n: Some(30),
            prompt_n: Some(18),
            cache_n: Some(6),
        };
        let mut msg = ChatMessage {
            id: "m-timings".to_string(),
            conversation_id: "c-timings".to_string(),
            role: ChatRole::Assistant,
            agent_id: Some("commander".to_string()),
            content: "measured response".to_string(),
            reasoning: None,
            tool_calls_json: None,
            task_id: None,
            token_count: 30,
            latency_ms: Some(1020),
            archived: false,
            created_at: "2026-11-14T10:00:01Z".to_string(),
            attachments: Vec::new(),
            timings: Some(t1.clone()),
        };
        storage.append_chat_message(&msg).unwrap();

        assert_eq!(
            storage.get_chat_message("m-timings").unwrap().unwrap().timings,
            Some(t1.clone())
        );
        assert_eq!(
            storage.list_chat_messages("c-timings", false, 10).unwrap()[0].timings,
            Some(t1.clone())
        );
        assert_eq!(
            storage.search_chat_messages("measured", 10).unwrap()[0].timings,
            Some(t1)
        );

        let t2 = InferenceTimings {
            predicted_per_second: Some(45.0),
            prompt_per_second: None,
            predicted_ms: Some(444.0),
            prompt_ms: None,
            predicted_n: Some(20),
            prompt_n: None,
            cache_n: None,
        };
        msg.timings = Some(t2.clone());
        assert!(storage.update_chat_message(&msg).unwrap());
        assert_eq!(
            storage.get_chat_message("m-timings").unwrap().unwrap().timings,
            Some(t2)
        );

        msg.timings = None;
        assert!(storage.update_chat_message(&msg).unwrap());
        assert!(storage
            .get_chat_message("m-timings")
            .unwrap()
            .unwrap()
            .timings
            .is_none());

        {
            let conn = storage.lock_conn().unwrap();
            conn.execute(
                "UPDATE messages SET timings_json = '{not valid json' WHERE id = 'm-timings'",
                [],
            )
            .unwrap();
        }
        assert!(storage
            .get_chat_message("m-timings")
            .unwrap()
            .unwrap()
            .timings
            .is_none());

        {
            let conn = storage.lock_conn().unwrap();
            conn.execute(
                "UPDATE messages SET timings_json = '   ' WHERE id = 'm-timings'",
                [],
            )
            .unwrap();
        }
        assert!(storage
            .get_chat_message("m-timings")
            .unwrap()
            .unwrap()
            .timings
            .is_none());
    }

    #[test]
    fn memory_stats_excludes_archived_memories() {
        let storage = SqliteStorage::open(Path::new(":memory:"), &DatabaseConfig::default()).unwrap();
        storage.run_migrations().unwrap();

        storage
            .upsert_memory(&MemoryItem {
                id: "mem-active".to_string(),
                scope: MemoryScope::Agent,
                namespace: "default".to_string(),
                owner_agent_id: Some("agent-stats".to_string()),
                conversation_id: None,
                task_id: None,
                key: "active".to_string(),
                content: "hello".to_string(),
                summary: None,
                importance: 5,
                tags: Vec::new(),
                archived: false,
                created_at: "2026-11-14T10:00:00Z".to_string(),
                updated_at: "2026-11-14T10:00:00Z".to_string(),
            })
            .unwrap();
        storage
            .upsert_memory(&MemoryItem {
                id: "mem-arch".to_string(),
                scope: MemoryScope::Agent,
                namespace: "default".to_string(),
                owner_agent_id: Some("agent-stats".to_string()),
                conversation_id: None,
                task_id: None,
                key: "archived".to_string(),
                content: "archived payload ignored".to_string(),
                summary: None,
                importance: 5,
                tags: Vec::new(),
                archived: true,
                created_at: "2026-11-14T10:00:01Z".to_string(),
                updated_at: "2026-11-14T10:00:01Z".to_string(),
            })
            .unwrap();

        assert_eq!(storage.memory_stats_for_agent("agent-stats").unwrap(), (1, 5));
    }

    #[test]
    fn sqlite_open_retries_transient_busy_then_succeeds() {
        let dir = TempDir::new("ps5-db-sqlite-busy-retry");
        let db_path = dir.0.join("retry.db");
        let cfg = DatabaseConfig {
            driver: "sqlite".to_string(),
            path: db_path.clone(),
            busy_timeout_ms: 0,
            cache_size_kb: 4000,
            wal_mode: false,
            ..DatabaseConfig::default()
        };

        {
            let init = SqliteStorage::open(&db_path, &cfg).unwrap();
            init.run_migrations().unwrap();
            init.set_setting("retry_key", "ok").unwrap();
        }

        let lock_conn = rusqlite::Connection::open(&db_path).unwrap();
        lock_conn
            .execute_batch(
                "PRAGMA locking_mode = EXCLUSIVE;
                 BEGIN EXCLUSIVE;
                 INSERT INTO settings (key, value, updated_at) VALUES ('_tmp', '1', '2026-11-14T00:00:00Z');",
            )
            .unwrap();

        let holder = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(220));
            let _ = lock_conn.execute_batch("ROLLBACK;");
            drop(lock_conn);
        });

        let reopened = SqliteStorage::open(&db_path, &cfg).expect("open should succeed after brief lock releases");
        holder.join().unwrap();
        assert_eq!(reopened.get_setting("retry_key").unwrap().as_deref(), Some("ok"));
    }
}
