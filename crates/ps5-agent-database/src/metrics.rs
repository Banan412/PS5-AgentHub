use crate::{JsonlStorage, SqliteStorage};
use chrono::{DateTime, Utc};
use ps5_agent_config::DatabaseConfig;
use ps5_agent_core::*;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug, Default)]
pub struct DbMetrics {
    ops_total: AtomicU64,
    total_us: AtomicU64,
    errors_total: AtomicU64,
}

impl DbMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&self, elapsed_us: u64, ok: bool) {
        self.ops_total.fetch_add(1, Ordering::Relaxed);
        self.total_us.fetch_add(elapsed_us, Ordering::Relaxed);
        if !ok {
            self.errors_total.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn ops_total(&self) -> u64 {
        self.ops_total.load(Ordering::Relaxed)
    }

    pub fn total_us(&self) -> u64 {
        self.total_us.load(Ordering::Relaxed)
    }

    pub fn avg_us(&self) -> u64 {
        let ops = self.ops_total();
        if ops == 0 {
            0
        } else {
            self.total_us() / ops
        }
    }

    pub fn errors_total(&self) -> u64 {
        self.errors_total.load(Ordering::Relaxed)
    }
}

pub struct InstrumentedStorage<S> {
    inner: S,
    metrics: Arc<DbMetrics>,
}

impl<S> InstrumentedStorage<S> {
    pub fn new(inner: S, metrics: Arc<DbMetrics>) -> Self {
        Self { inner, metrics }
    }

    pub fn inner(&self) -> &S {
        &self.inner
    }

    pub fn metrics(&self) -> &Arc<DbMetrics> {
        &self.metrics
    }

    #[inline]
    fn timed<T, F: FnOnce(&S) -> Result<T>>(&self, f: F) -> Result<T> {
        let start = Instant::now();
        let res = f(&self.inner);
        let us = start.elapsed().as_micros() as u64;
        self.metrics.record(us, res.is_ok());
        res
    }
}

impl<S: StorageBackend> StorageBackend for InstrumentedStorage<S> {
    fn driver_name(&self) -> &'static str {
        self.inner.driver_name()
    }
    fn run_migrations(&self) -> Result<()> {
        self.timed(|s| s.run_migrations())
    }
    fn integrity_check(&self) -> Result<String> {
        self.timed(|s| s.integrity_check())
    }
    fn checkpoint(&self) -> Result<()> {
        self.timed(|s| s.checkpoint())
    }
    fn upsert_agent(&self, agent: &AgentDefinition) -> Result<()> {
        self.timed(|s| s.upsert_agent(agent))
    }
    fn get_agent(&self, agent_id: &str) -> Result<Option<AgentDefinition>> {
        self.timed(|s| s.get_agent(agent_id))
    }
    fn list_agents(&self) -> Result<Vec<AgentDefinition>> {
        self.timed(|s| s.list_agents())
    }
    fn delete_agent(&self, agent_id: &str) -> Result<bool> {
        self.timed(|s| s.delete_agent(agent_id))
    }
    fn create_conversation(&self, conv: &Conversation) -> Result<()> {
        self.timed(|s| s.create_conversation(conv))
    }
    fn get_conversation(&self, conv_id: &str) -> Result<Option<Conversation>> {
        self.timed(|s| s.get_conversation(conv_id))
    }
    fn list_conversations(&self, include_archived: bool) -> Result<Vec<Conversation>> {
        self.timed(|s| s.list_conversations(include_archived))
    }
    fn delete_conversation(&self, conv_id: &str) -> Result<bool> {
        self.timed(|s| s.delete_conversation(conv_id))
    }
    fn reorder_conversations(&self, ordered_ids: &[String]) -> Result<()> {
        self.timed(|s| s.reorder_conversations(ordered_ids))
    }
    fn append_chat_message(&self, msg: &ChatMessage) -> Result<()> {
        self.timed(|s| s.append_chat_message(msg))
    }
    fn list_chat_messages(&self, conv_id: &str, include_archived: bool, limit: usize) -> Result<Vec<ChatMessage>> {
        self.timed(|s| s.list_chat_messages(conv_id, include_archived, limit))
    }
    fn search_chat_messages(&self, query: &str, limit: usize) -> Result<Vec<ChatMessage>> {
        self.timed(|s| s.search_chat_messages(query, limit))
    }
    fn archive_old_chat_messages(&self, conv_id: &str, keep_recent: usize) -> Result<usize> {
        self.timed(|s| s.archive_old_chat_messages(conv_id, keep_recent))
    }
    fn get_chat_message(&self, msg_id: &str) -> Result<Option<ChatMessage>> {
        self.timed(|s| s.get_chat_message(msg_id))
    }
    fn update_chat_message(&self, msg: &ChatMessage) -> Result<bool> {
        self.timed(|s| s.update_chat_message(msg))
    }
    fn delete_chat_message(&self, msg_id: &str) -> Result<bool> {
        self.timed(|s| s.delete_chat_message(msg_id))
    }
    fn delete_chat_messages_after(
        &self,
        conversation_id: &str,
        anchor_msg_id: &str,
        inclusive: bool,
    ) -> Result<usize> {
        self.timed(|s| s.delete_chat_messages_after(conversation_id, anchor_msg_id, inclusive))
    }
    fn upsert_task(&self, task: &TaskRecord) -> Result<()> {
        self.timed(|s| s.upsert_task(task))
    }
    fn get_task(&self, task_id: &str) -> Result<Option<TaskRecord>> {
        self.timed(|s| s.get_task(task_id))
    }
    fn list_tasks(&self, limit: usize) -> Result<Vec<TaskRecord>> {
        self.timed(|s| s.list_tasks(limit))
    }
    fn task_counts_summary(&self) -> Result<TaskCountsSummary> {
        self.timed(|s| s.task_counts_summary())
    }
    fn append_task_event(&self, event: &TaskEvent) -> Result<()> {
        self.timed(|s| s.append_task_event(event))
    }
    fn list_task_events(&self, task_id: &str) -> Result<Vec<TaskEvent>> {
        self.timed(|s| s.list_task_events(task_id))
    }
    fn append_agent_message(&self, msg: &AgentEnvelope) -> Result<()> {
        self.timed(|s| s.append_agent_message(msg))
    }
    fn update_agent_message_status(&self, msg_id: &str, status: DeliveryStatus) -> Result<()> {
        self.timed(|s| s.update_agent_message_status(msg_id, status))
    }
    fn list_agent_messages(&self, task_id: Option<&str>, agent_id: Option<&str>, limit: usize) -> Result<Vec<AgentEnvelope>> {
        self.timed(|s| s.list_agent_messages(task_id, agent_id, limit))
    }
    fn count_task_agent_messages(&self, task_id: &str) -> Result<u32> {
        self.timed(|s| s.count_task_agent_messages(task_id))
    }
    fn upsert_memory(&self, item: &MemoryItem) -> Result<()> {
        self.timed(|s| s.upsert_memory(item))
    }
    fn query_memories(
        &self,
        scope: Option<MemoryScope>,
        namespace: Option<&str>,
        owner_agent_id: Option<&str>,
        include_archived: bool,
        limit: usize,
    ) -> Result<Vec<MemoryItem>> {
        self.timed(|s| s.query_memories(scope, namespace, owner_agent_id, include_archived, limit))
    }
    fn delete_memory(&self, memory_id: &str) -> Result<bool> {
        self.timed(|s| s.delete_memory(memory_id))
    }
    fn delete_agent_memories(&self, agent_id: &str) -> Result<usize> {
        self.timed(|s| s.delete_agent_memories(agent_id))
    }
    fn memory_stats_for_agent(&self, agent_id: &str) -> Result<(u64, u64)> {
        self.timed(|s| s.memory_stats_for_agent(agent_id))
    }
    fn upsert_tool_spec(&self, spec: &ToolSpec) -> Result<()> {
        self.timed(|s| s.upsert_tool_spec(spec))
    }
    fn list_tool_specs(&self) -> Result<Vec<ToolSpec>> {
        self.timed(|s| s.list_tool_specs())
    }
    fn delete_tool_spec(&self, tool_id: &str) -> Result<bool> {
        self.timed(|s| s.delete_tool_spec(tool_id))
    }
    fn record_tool_execution(&self, rec: &ToolExecutionRecord) -> Result<()> {
        self.timed(|s| s.record_tool_execution(rec))
    }
    fn list_tool_executions(&self, agent_id: Option<&str>, limit: usize) -> Result<Vec<ToolExecutionRecord>> {
        self.timed(|s| s.list_tool_executions(agent_id, limit))
    }
    fn get_setting(&self, key: &str) -> Result<Option<String>> {
        self.timed(|s| s.get_setting(key))
    }
    fn set_setting(&self, key: &str, value: &str) -> Result<()> {
        self.timed(|s| s.set_setting(key, value))
    }
    fn list_settings(&self) -> Result<Vec<(String, String)>> {
        self.timed(|s| s.list_settings())
    }
    fn create_session(&self, sess: &SessionRecord) -> Result<()> {
        self.timed(|s| s.create_session(sess))
    }
    fn get_session_by_hash(&self, token_sha256: &str) -> Result<Option<SessionRecord>> {
        self.timed(|s| s.get_session_by_hash(token_sha256))
    }
    fn revoke_session(&self, session_id: &str) -> Result<()> {
        self.timed(|s| s.revoke_session(session_id))
    }
    fn purge_expired_sessions(&self, now_iso: &str) -> Result<usize> {
        self.timed(|s| s.purge_expired_sessions(now_iso))
    }
    fn upsert_approval(&self, approval: &ApprovalRecord) -> Result<()> {
        self.timed(|s| s.upsert_approval(approval))
    }
    fn get_approval(&self, approval_id: &str) -> Result<Option<ApprovalRecord>> {
        self.timed(|s| s.get_approval(approval_id))
    }
    fn list_approvals(&self, pending_only: bool, limit: usize) -> Result<Vec<ApprovalRecord>> {
        self.timed(|s| s.list_approvals(pending_only, limit))
    }
    fn append_audit_log(&self, entry: &AuditLogEntry) -> Result<()> {
        self.timed(|s| s.append_audit_log(entry))
    }
    fn list_audit_logs(&self, limit: usize) -> Result<Vec<AuditLogEntry>> {
        self.timed(|s| s.list_audit_logs(limit))
    }
    fn upsert_schedule(&self, schedule: &ScheduleRecord) -> Result<()> {
        self.timed(|s| s.upsert_schedule(schedule))
    }
    fn get_schedule(&self, id: &str) -> Result<Option<ScheduleRecord>> {
        self.timed(|s| s.get_schedule(id))
    }
    fn list_schedules(&self, limit: usize) -> Result<Vec<ScheduleRecord>> {
        self.timed(|s| s.list_schedules(limit))
    }
    fn list_due_schedules(&self, now: DateTime<Utc>, limit: usize) -> Result<Vec<ScheduleRecord>> {
        self.timed(|s| s.list_due_schedules(now, limit))
    }
    fn delete_schedule(&self, id: &str) -> Result<()> {
        self.timed(|s| s.delete_schedule(id))
    }
    fn list_webhooks(&self) -> Result<Vec<WebhookRecord>> {
        self.timed(|s| s.list_webhooks())
    }
    fn get_webhook(&self, id: &str) -> Result<Option<WebhookRecord>> {
        self.timed(|s| s.get_webhook(id))
    }
    fn upsert_webhook(&self, hook: &WebhookRecord) -> Result<()> {
        self.timed(|s| s.upsert_webhook(hook))
    }
    fn delete_webhook(&self, id: &str) -> Result<bool> {
        self.timed(|s| s.delete_webhook(id))
    }
    fn prune_old_records(&self, retention_days: u32, max_rows_per_table: usize) -> Result<PruneReport> {
        self.timed(|s| s.prune_old_records(retention_days, max_rows_per_table))
    }
}

fn sidecar_path(path: &Path, suffix: &str) -> PathBuf {
    let mut os: OsString = path.as_os_str().to_os_string();
    os.push(suffix);
    PathBuf::from(os)
}

fn backup_corrupt_sqlite_file(db_path: &Path, bak_path: &Path) -> std::io::Result<()> {
    if !bak_path.exists() {
        return fs::rename(db_path, bak_path);
    }
    let tmp_bak = sidecar_path(db_path, ".corrupt.bak.tmp");
    if tmp_bak.exists() {
        let _ = fs::remove_file(&tmp_bak);
    }
    // Move the newly corrupt DB out of the way first so a prior .corrupt.bak survives rename failures.
    fs::rename(db_path, &tmp_bak)?;
    if let Err(err) = fs::rename(&tmp_bak, bak_path) {
        let _ = fs::remove_file(bak_path);
        if let Err(retry_err) = fs::rename(&tmp_bak, bak_path) {
            ps5_agent_logging::log_warn(
                "database",
                format!(
                    "Moved corrupt DB to {} but failed replacing {} ({err}; retry: {retry_err})",
                    tmp_bak.display(),
                    bak_path.display()
                ),
            );
        }
    }
    Ok(())
}

fn try_open_sqlite_checked(
    cfg: &DatabaseConfig,
    metrics: &Arc<DbMetrics>,
) -> Result<Arc<InstrumentedStorage<SqliteStorage>>> {
    let s = SqliteStorage::open(&cfg.path, cfg)?;
    let wrapped = Arc::new(InstrumentedStorage::new(s, Arc::clone(metrics)));
    wrapped.run_migrations()?;
    let check = wrapped.integrity_check()?;
    if !check.trim().eq_ignore_ascii_case("ok") {
        return Err(AgentError::Storage(format!(
            "SQLite integrity_check returned '{check}'"
        )));
    }
    Ok(wrapped)
}

pub fn open_storage(cfg: &DatabaseConfig, data_dir: &Path) -> Result<(Arc<dyn StorageBackend>, Arc<DbMetrics>)> {
    let metrics = Arc::new(DbMetrics::new());
    match cfg.driver.as_str() {
        "sqlite" => {
            let wrapped = try_open_sqlite_checked(cfg, &metrics)?;
            Ok((wrapped, metrics))
        }
        "jsonl" => {
            let j = JsonlStorage::open(&data_dir.join("jsonl_db"))?;
            let wrapped = Arc::new(InstrumentedStorage::new(j, Arc::clone(&metrics)));
            wrapped.run_migrations()?;
            Ok((wrapped, metrics))
        }
        "auto" => match try_open_sqlite_checked(cfg, &metrics) {
            Ok(wrapped) => Ok((wrapped, metrics)),
            Err(err) => {
                if cfg.path.as_os_str() != ":memory:" && cfg.path.is_file() {
                    let bak_path = sidecar_path(&cfg.path, ".corrupt.bak");
                    let wal_path = sidecar_path(&cfg.path, "-wal");
                    let shm_path = sidecar_path(&cfg.path, "-shm");
                    let journal_path = sidecar_path(&cfg.path, "-journal");
                    ps5_agent_logging::log_warn(
                        "database",
                        format!(
                            "SQLite initialization failed ({err}); backing up {} to {} and retrying fresh DB",
                            cfg.path.display(),
                            bak_path.display()
                        ),
                    );
                    let rename_res = backup_corrupt_sqlite_file(&cfg.path, &bak_path);

                    match rename_res {
                        Ok(()) => {
                            let _ = fs::remove_file(&wal_path);
                            let _ = fs::remove_file(&shm_path);
                            let _ = fs::remove_file(&journal_path);
                            match try_open_sqlite_checked(cfg, &metrics) {
                                Ok(fresh_wrapped) => return Ok((fresh_wrapped, metrics)),
                                Err(retry_err) => {
                                    ps5_agent_logging::log_warn(
                                        "database",
                                        format!(
                                            "Fresh SQLite retry failed ({retry_err}); falling back to JsonlStorage"
                                        ),
                                    );
                                }
                            }
                        }
                        Err(rename_err) => {
                            ps5_agent_logging::log_warn(
                                "database",
                                format!(
                                    "Failed to back up corrupt SQLite DB {} ({rename_err}); falling back to JsonlStorage",
                                    cfg.path.display()
                                ),
                            );
                        }
                    }
                } else {
                    ps5_agent_logging::log_warn(
                        "database",
                        format!("SQLite initialization failed ({err}); falling back to JsonlStorage"),
                    );
                }

                let j = JsonlStorage::open(&data_dir.join("jsonl_db"))?;
                let wrapped_j = Arc::new(InstrumentedStorage::new(j, Arc::clone(&metrics)));
                wrapped_j.run_migrations()?;
                Ok((wrapped_j, metrics))
            }
        },
        other => Err(AgentError::InvalidInput(format!(
            "unsupported database driver '{other}'; expected auto, sqlite, or jsonl"
        ))),
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

    #[test]
    fn auto_mode_backs_up_corrupt_db_and_keeps_sqlite_primary() {
        let dir = TempDir::new("ps5-db-corrupt-recover");
        let db_path = dir.0.join("agent.db");
        let wal_path = sidecar_path(&db_path, "-wal");
        let shm_path = sidecar_path(&db_path, "-shm");
        let bak_path = sidecar_path(&db_path, ".corrupt.bak");
        let journal_path = sidecar_path(&db_path, "-journal");

        fs::write(&db_path, b"not a valid sqlite database").unwrap();
        fs::write(&wal_path, b"stale wal").unwrap();
        fs::write(&shm_path, b"stale shm").unwrap();
        fs::write(&journal_path, b"stale journal").unwrap();
        fs::write(&bak_path, b"prior backup").unwrap();

        let cfg = DatabaseConfig {
            driver: "auto".to_string(),
            path: db_path.clone(),
            busy_timeout_ms: 5000,
            cache_size_kb: 4000,
            wal_mode: false,
            ..DatabaseConfig::default()
        };

        let (store, _metrics) = open_storage(&cfg, &dir.0).expect("recover with fresh sqlite");
        assert_eq!(store.driver_name(), "sqlite");
        assert_eq!(store.integrity_check().unwrap().to_lowercase(), "ok");
        assert!(bak_path.is_file());
        assert_eq!(fs::read(&bak_path).unwrap(), b"not a valid sqlite database");
        assert!(!wal_path.exists());
        assert!(!shm_path.exists());
        assert!(
            fs::read(&journal_path).map(|b| b.is_empty()).unwrap_or(true),
            "stale non-empty rollback journal must be purged during recovery (TRUNCATE leaves 0-byte journal)"
        );
    }

    #[test]
    fn explicit_sqlite_mode_rejects_corrupt_db() {
        let dir = TempDir::new("ps5-db-sqlite-integrity");
        let db_path = dir.0.join("corrupt.db");
        fs::write(&db_path, b"not a valid sqlite database").unwrap();

        let cfg = DatabaseConfig {
            driver: "sqlite".to_string(),
            path: db_path,
            busy_timeout_ms: 5000,
            cache_size_kb: 4000,
            wal_mode: false,
            ..DatabaseConfig::default()
        };

        assert!(open_storage(&cfg, &dir.0).is_err());
    }
}
