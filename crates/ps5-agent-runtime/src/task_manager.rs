use ps5_agent_core::{
    new_id, now_iso, AuditLogEntry, Result, RuntimeLimits, StorageBackend, StreamEvent, TaskEvent,
    TaskRecord, TaskStatus,
};
use ps5_agent_logging::{log_info, log_warn};
use ps5_agent_manager::AgentTaskController;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::{broadcast, Notify};
use tokio_util::sync::CancellationToken;

const DEFAULT_TASK_SCAN_LIMIT: usize = 10_000;

#[derive(Debug, Clone, Default)]
pub struct RecoveryReport {
    pub resumed_tasks: Vec<TaskRecord>,
    pub paused_side_effect_tasks: Vec<TaskRecord>,
    pub already_waiting_tasks: Vec<TaskRecord>,
}

pub fn task_status_str(status: TaskStatus) -> &'static str {
    match status {
        TaskStatus::Pending => "PENDING",
        TaskStatus::Assigned => "ASSIGNED",
        TaskStatus::Running => "RUNNING",
        TaskStatus::Waiting => "WAITING",
        TaskStatus::Completed => "COMPLETED",
        TaskStatus::Failed => "FAILED",
        TaskStatus::Cancelled => "CANCELLED",
    }
}

pub struct TaskManager {
    storage: Arc<dyn StorageBackend>,
    limits: RwLock<RuntimeLimits>,
    events_tx: broadcast::Sender<StreamEvent>,
    tokens: RwLock<HashMap<String, CancellationToken>>,
    expedite_notifiers: RwLock<HashMap<String, Arc<Notify>>>,
}

impl TaskManager {
    pub fn new(
        storage: Arc<dyn StorageBackend>,
        limits: RuntimeLimits,
        events_tx: broadcast::Sender<StreamEvent>,
    ) -> Self {
        Self {
            storage,
            limits: RwLock::new(limits),
            events_tx,
            tokens: RwLock::new(HashMap::new()),
            expedite_notifiers: RwLock::new(HashMap::new()),
        }
    }

    pub fn update_limits(&self, limits: RuntimeLimits) {
        if let Ok(mut guard) = self.limits.write() {
            *guard = limits;
        }
    }

    pub fn limits(&self) -> RuntimeLimits {
        self.limits.read().map(|g| g.clone()).unwrap_or_default()
    }

    pub fn create_task(
        &self,
        user_request: String,
        assigned_agent_id: String,
        conversation_id: Option<String>,
        parent_task_id: Option<String>,
        priority: i32,
        initial_status: TaskStatus,
    ) -> Result<TaskRecord> {
        let limits = self.limits();
        if parent_task_id.is_none() && limits.max_queued_tasks > 0 {
            let summary = self.storage.task_counts_summary()?;
            let active_or_queued = summary.pending + summary.running + summary.waiting;
            if active_or_queued >= limits.max_queued_tasks {
                return Err(ps5_agent_core::AgentError::LimitExceeded(format!(
                    "max_queued_tasks limit ({}) reached",
                    limits.max_queued_tasks
                )));
            }
        }

        let now = now_iso();
        let task = TaskRecord {
            id: new_id(),
            conversation_id,
            user_request,
            assigned_agent_id,
            parent_task_id,
            priority,
            created_at: now.clone(),
            started_at: if initial_status == TaskStatus::Running {
                Some(now)
            } else {
                None
            },
            completed_at: None,
            status: initial_status,
            result: None,
            error_info: None,
            has_side_effects: false,
            progress_pct: 0,
            progress_note: None,
        };

        self.storage.upsert_task(&task)?;
        self.append_event(&task.id, "task_created", &format!("assigned to {}", task.assigned_agent_id))?;
        self.emit_task_updated(&task);
        Ok(task)
    }

    pub fn get_task(&self, task_id: &str) -> Result<Option<TaskRecord>> {
        self.storage.get_task(task_id)
    }

    pub fn list_tasks(&self, limit: usize) -> Result<Vec<TaskRecord>> {
        self.storage.list_tasks(limit)
    }

    pub fn list_subtasks(&self, parent_task_id: &str) -> Result<Vec<TaskRecord>> {
        let summary = self.storage.task_counts_summary()?;
        let limit = if summary.total > 0 {
            summary.total.min(DEFAULT_TASK_SCAN_LIMIT)
        } else {
            DEFAULT_TASK_SCAN_LIMIT
        };
        let all = self.storage.list_tasks(limit)?;
        Ok(all
            .into_iter()
            .filter(|t| t.parent_task_id.as_deref() == Some(parent_task_id))
            .collect())
    }

    pub fn list_task_events(&self, task_id: &str) -> Result<Vec<TaskEvent>> {
        self.storage.list_task_events(task_id)
    }

    pub fn register_token(&self, task_id: &str, parent_cancel: Option<&CancellationToken>) -> CancellationToken {
        let token = match parent_cancel {
            Some(parent) => parent.child_token(),
            None => CancellationToken::new(),
        };
        if let Ok(mut guard) = self.tokens.write() {
            guard.insert(task_id.to_string(), token.clone());
        }
        token
    }

    pub fn release_token(&self, task_id: &str) {
        if let Ok(mut guard) = self.tokens.write() {
            guard.remove(task_id);
            Self::shrink_map_if_sparse(&mut guard);
        }
        if let Ok(mut guard) = self.expedite_notifiers.write() {
            guard.remove(task_id);
            Self::shrink_map_if_sparse(&mut guard);
        }
    }

    pub fn register_expedite(&self, task_id: &str) -> Arc<Notify> {
        if let Ok(mut guard) = self.expedite_notifiers.write() {
            if let Some(existing) = guard.get(task_id) {
                return existing.clone();
            }
            let notify = Arc::new(Notify::new());
            guard.insert(task_id.to_string(), notify.clone());
            return notify;
        }
        Arc::new(Notify::new())
    }

    pub fn expedite_task(&self, task_id: &str) -> Result<TaskRecord> {
        let mut task = self.require_task(task_id)?;
        if task.status.is_terminal() {
            return Ok(task);
        }
        if task.status == TaskStatus::Pending {
            task.progress_note = Some("Expedited — starting immediately".to_string());
            self.storage.upsert_task(&task)?;
            self.append_event(task_id, "task_expedited", "expedited to start immediately")?;
            self.emit_task_updated(&task);
        }
        if let Ok(guard) = self.expedite_notifiers.read() {
            if let Some(notify) = guard.get(task_id) {
                notify.notify_one();
                notify.notify_waiters();
            }
        }
        Ok(task)
    }

    pub async fn active_token_count(&self) -> usize {
        self.tokens.read().map(|guard| guard.len()).unwrap_or(0)
    }

    fn shrink_map_if_sparse<V>(map: &mut HashMap<String, V>) {
        let capacity = map.capacity();
        if capacity > 16 && map.len() < capacity / 4 {
            map.shrink_to_fit();
        }
    }

    pub fn mark_running(&self, task_id: &str, note: Option<String>) -> Result<TaskRecord> {
        let mut task = self.require_task(task_id)?;
        if task.status == TaskStatus::Cancelled {
            self.release_token(task_id);
            return Err(ps5_agent_core::AgentError::Cancelled(format!(
                "task '{task_id}' already cancelled"
            )));
        }
        if task.status.is_terminal() {
            self.release_token(task_id);
            return Err(ps5_agent_core::AgentError::InvalidInput(format!(
                "task '{task_id}' is already terminal ({})",
                task_status_str(task.status)
            )));
        }
        task.status = TaskStatus::Running;
        if task.started_at.is_none() {
            task.started_at = Some(now_iso());
        }
        if let Some(n) = note {
            task.progress_note = Some(n);
        }
        self.storage.upsert_task(&task)?;
        self.append_event(&task.id, "task_running", "started execution")?;
        self.emit_task_updated(&task);
        Ok(task)
    }

    pub fn mark_waiting(&self, task_id: &str, reason: &str) -> Result<TaskRecord> {
        let mut task = self.require_task(task_id)?;
        if task.status == TaskStatus::Cancelled {
            self.release_token(task_id);
            return Err(ps5_agent_core::AgentError::Cancelled(format!(
                "task '{task_id}' already cancelled"
            )));
        }
        if task.status.is_terminal() {
            self.release_token(task_id);
            return Err(ps5_agent_core::AgentError::InvalidInput(format!(
                "task '{task_id}' is already terminal ({})",
                task_status_str(task.status)
            )));
        }
        task.status = TaskStatus::Waiting;
        task.progress_note = Some(reason.to_string());
        self.storage.upsert_task(&task)?;
        self.append_event(&task.id, "task_waiting", reason)?;
        self.emit_task_updated(&task);
        Ok(task)
    }

    pub fn mark_side_effects(&self, task_id: &str) -> Result<()> {
        let mut task = self.require_task(task_id)?;
        if !task.has_side_effects {
            task.has_side_effects = true;
            self.storage.upsert_task(&task)?;
            self.append_event(task_id, "side_effects_marked", "task performed or queued mutating action")?;
        }
        Ok(())
    }

    pub fn update_progress(
        &self,
        task_id: &str,
        progress_pct: u8,
        progress_note: Option<String>,
    ) -> Result<TaskRecord> {
        let mut task = self.require_task(task_id)?;
        if task.status == TaskStatus::Cancelled {
            self.release_token(task_id);
            return Err(ps5_agent_core::AgentError::Cancelled(format!(
                "task '{task_id}' already cancelled"
            )));
        }
        if task.status.is_terminal() {
            self.release_token(task_id);
            return Ok(task);
        }
        task.progress_pct = progress_pct.min(100);
        task.progress_note = progress_note.clone();
        self.storage.upsert_task(&task)?;
        if let Some(note) = progress_note {
            self.append_event(task_id, "task_progress", &format!("{progress_pct}%: {note}"))?;
        }
        self.emit_task_updated(&task);
        Ok(task)
    }

    pub fn complete_task(&self, task_id: &str, result: String) -> Result<TaskRecord> {
        let mut task = self.require_task(task_id)?;
        if task.status == TaskStatus::Cancelled {
            self.release_token(task_id);
            return Err(ps5_agent_core::AgentError::Cancelled(format!(
                "task '{task_id}' already cancelled"
            )));
        }
        if task.status.is_terminal() {
            self.release_token(task_id);
            return Ok(task);
        }
        task.status = TaskStatus::Completed;
        task.result = Some(result);
        task.progress_pct = 100;
        task.completed_at = Some(now_iso());
        self.storage.upsert_task(&task)?;
        self.append_event(task_id, "task_completed", "completed successfully")?;
        self.emit_task_updated(&task);
        self.release_token(task_id);
        Ok(task)
    }

    pub fn fail_task(&self, task_id: &str, error_info: String) -> Result<TaskRecord> {
        let mut task = self.require_task(task_id)?;
        if task.status.is_terminal() {
            self.release_token(task_id);
            return Ok(task);
        }
        task.status = TaskStatus::Failed;
        task.error_info = Some(error_info.clone());
        task.progress_note = Some(error_info.clone());
        task.completed_at = Some(now_iso());
        self.storage.upsert_task(&task)?;
        self.append_event(task_id, "task_failed", &error_info)?;
        self.emit_task_updated(&task);
        self.release_token(task_id);
        Ok(task)
    }

    pub fn mark_cancelled(&self, task_id: &str, reason: &str) -> Result<TaskRecord> {
        self.cancel_task(task_id, reason)
    }

    pub fn cancel_task(&self, task_id: &str, reason: &str) -> Result<TaskRecord> {
        let mut task = self.require_task(task_id)?;
        let removed_tok = if let Ok(mut guard) = self.tokens.write() {
            let tok = guard.remove(task_id);
            Self::shrink_map_if_sparse(&mut guard);
            tok
        } else {
            None
        };
        if let Some(tok) = removed_tok {
            tok.cancel();
        }
        if !task.status.is_terminal() {
            task.status = TaskStatus::Cancelled;
            task.error_info = Some(reason.to_string());
            task.progress_note = Some(format!("Cancelled: {reason}"));
            task.completed_at = Some(now_iso());
            self.storage.upsert_task(&task)?;
            self.append_event(task_id, "task_cancelled", reason)?;
            self.emit_task_updated(&task);
        }
        let removed_expedite = if let Ok(mut guard) = self.expedite_notifiers.write() {
            let notify = guard.remove(task_id);
            Self::shrink_map_if_sparse(&mut guard);
            notify
        } else {
            None
        };
        if let Some(notify) = removed_expedite {
            notify.notify_waiters();
        }

        // Cascade cancellation to active/pending subtasks.
        let summary = self.storage.task_counts_summary()?;
        if summary.total == 0 || summary.pending + summary.running + summary.waiting > 0 {
            for sub in self.list_subtasks(task_id)? {
                if !sub.status.is_terminal() {
                    let _ = self.cancel_task(&sub.id, &format!("parent task {task_id} cancelled: {reason}"));
                }
            }
        }
        Ok(task)
    }

    pub fn cancel_conversation_tasks(
        &self,
        conv_id: &str,
        agent_id: Option<&str>,
        reason: &str,
    ) -> Result<usize> {
        let summary = self.storage.task_counts_summary()?;
        let non_terminal = summary.pending + summary.running + summary.waiting;
        if summary.total > 0
            && (non_terminal == 0
                || agent_id.is_some_and(|aid| {
                    summary.pending == 0 && summary.per_agent.get(aid).map_or(0, |s| s.active) == 0
                }))
        {
            return Ok(0);
        }

        let fetch_limit = if summary.total > 0 {
            summary
                .total
                .min(DEFAULT_TASK_SCAN_LIMIT)
                .max(non_terminal)
        } else {
            DEFAULT_TASK_SCAN_LIMIT
        };
        let tasks = self.storage.list_tasks(fetch_limit)?;
        let mut count = 0;
        for t in tasks {
            if t.conversation_id.as_deref() == Some(conv_id)
                && agent_id.map_or(true, |aid| t.assigned_agent_id == aid)
                && !t.status.is_terminal()
            {
                self.cancel_task(&t.id, reason)?;
                count += 1;
            }
        }
        Ok(count)
    }

    /// Startup crash/restart recovery:
    /// - Read-only interrupted tasks (`RUNNING`/`ASSIGNED` with `has_side_effects == false`)
    ///   and previously `PENDING` tasks are queued for automatic resumption.
    /// - Side-effecting interrupted tasks (`has_side_effects == true`) are moved to `WAITING`
    ///   without auto-replaying potentially dangerous actions.
    pub fn recover_on_startup(&self) -> Result<RecoveryReport> {
        let summary = self.storage.task_counts_summary()?;
        let non_terminal = summary.pending + summary.running + summary.waiting;
        if let Ok(mut guard) = self.expedite_notifiers.write() {
            guard.clear();
            Self::shrink_map_if_sparse(&mut guard);
        }
        if summary.total > 0 && non_terminal == 0 {
            if let Ok(mut guard) = self.tokens.write() {
                guard.clear();
                Self::shrink_map_if_sparse(&mut guard);
            }
            return Ok(RecoveryReport::default());
        }

        let fetch_limit = if summary.total > 0 {
            summary
                .total
                .min(DEFAULT_TASK_SCAN_LIMIT)
                .max(non_terminal)
        } else {
            DEFAULT_TASK_SCAN_LIMIT
        };
        let tasks = self.storage.list_tasks(fetch_limit)?;
        let mut report = RecoveryReport::default();

        for mut task in tasks {
            match task.status {
                TaskStatus::Running | TaskStatus::Assigned => {
                    self.release_token(&task.id);
                    if task.has_side_effects {
                        task.status = TaskStatus::Waiting;
                        task.progress_note = Some(
                            "Paused on startup recovery: task has side effects and requires manual review"
                                .to_string(),
                        );
                        self.storage.upsert_task(&task)?;
                        self.append_event(
                            &task.id,
                            "recovery_paused_side_effects",
                            "moved side-effecting interrupted task to WAITING",
                        )?;
                        self.emit_task_updated(&task);
                        let _ = self.storage.append_audit_log(&AuditLogEntry {
                            id: new_id(),
                            timestamp: now_iso(),
                            actor: "system".to_string(),
                            action: "task.recovery_paused".to_string(),
                            target: task.id.clone(),
                            severity: "warn".to_string(),
                            outcome: "waiting".to_string(),
                            details_json: serde_json::json!({
                                "assigned_agent_id": task.assigned_agent_id,
                                "has_side_effects": true,
                            })
                            .to_string(),
                        });
                        log_warn(
                            "task_manager",
                            format!("recovery paused side-effecting task '{}'", task.id),
                        );
                        report.paused_side_effect_tasks.push(task);
                    } else {
                        task.status = TaskStatus::Pending;
                        task.progress_note = Some(
                            "Queued for automatic recovery after restart (read-only)".to_string(),
                        );
                        self.storage.upsert_task(&task)?;
                        self.append_event(
                            &task.id,
                            "recovery_resumed_readonly",
                            "re-queued read-only interrupted task",
                        )?;
                        self.emit_task_updated(&task);
                        log_info(
                            "task_manager",
                            format!("recovery queued read-only task '{}'", task.id),
                        );
                        report.resumed_tasks.push(task);
                    }
                }
                TaskStatus::Pending => {
                    report.resumed_tasks.push(task);
                }
                TaskStatus::Waiting => {
                    report.already_waiting_tasks.push(task);
                }
                _ => {
                    self.release_token(&task.id);
                }
            }
        }
        Ok(report)
    }

    fn require_task(&self, task_id: &str) -> Result<TaskRecord> {
        self.storage
            .get_task(task_id)?
            .ok_or_else(|| ps5_agent_core::AgentError::NotFound(format!("task '{task_id}' not found")))
    }

    fn append_event(&self, task_id: &str, event_type: &str, details: &str) -> Result<()> {
        self.storage.append_task_event(&TaskEvent {
            id: new_id(),
            task_id: task_id.to_string(),
            timestamp: now_iso(),
            event_type: event_type.to_string(),
            details: details.to_string(),
        })
    }

    fn emit_task_updated(&self, task: &TaskRecord) {
        let progress_note = if task.status == TaskStatus::Failed {
            task.error_info
                .clone()
                .or_else(|| task.progress_note.clone())
        } else {
            task.progress_note.clone()
        };
        let _ = self.events_tx.send(StreamEvent::TaskUpdated {
            conversation_id: task.conversation_id.clone(),
            task_id: task.id.clone(),
            agent_id: task.assigned_agent_id.clone(),
            status: task_status_str(task.status).to_string(),
            progress_pct: task.progress_pct,
            progress_note,
        });
    }
}

impl AgentTaskController for TaskManager {
    fn cancel_agent_tasks(&self, agent_id: &str, reason: &str) -> Result<usize> {
        let summary = self.storage.task_counts_summary()?;
        let non_terminal = summary.pending + summary.running + summary.waiting;
        let agent_active = summary.per_agent.get(agent_id).map_or(0, |s| s.active);

        if summary.total == 0 || non_terminal == 0 || (summary.pending == 0 && agent_active == 0) {
            return Ok(0);
        }
        if summary.pending == 0 && agent_active == 1 {
            if let Some(task_id) = summary
                .per_agent
                .get(agent_id)
                .and_then(|s| s.latest_active_task_id.as_deref())
            {
                self.cancel_task(task_id, reason)?;
                return Ok(1);
            }
        }

        let fetch_limit = if summary.total > 0 {
            summary.total.min(DEFAULT_TASK_SCAN_LIMIT)
        } else {
            DEFAULT_TASK_SCAN_LIMIT
        };
        let tasks = self.storage.list_tasks(fetch_limit)?;
        let mut count = 0;
        for t in tasks {
            if t.assigned_agent_id == agent_id && !t.status.is_terminal() {
                self.cancel_task(&t.id, reason)?;
                count += 1;
            }
        }
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ps5_agent_core::{
        AgentDefinition, AgentEnvelope, AgentError, AgentTaskStats, ApprovalRecord, ChatMessage,
        Conversation, DeliveryStatus, MemoryItem, MemoryScope, SessionRecord, TaskCountsSummary,
        ToolExecutionRecord, ToolSpec,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    #[derive(Default)]
    struct MockTaskStorage {
        tasks: Mutex<HashMap<String, TaskRecord>>,
        events: Mutex<Vec<TaskEvent>>,
        list_tasks_calls: AtomicUsize,
        summary_calls: AtomicUsize,
    }

    impl MockTaskStorage {
        fn insert_raw(&self, task: TaskRecord) {
            self.tasks.lock().unwrap().insert(task.id.clone(), task);
        }

        fn list_calls(&self) -> usize {
            self.list_tasks_calls.load(Ordering::SeqCst)
        }

        fn summary_calls(&self) -> usize {
            self.summary_calls.load(Ordering::SeqCst)
        }
    }

    impl StorageBackend for MockTaskStorage {
        fn driver_name(&self) -> &'static str {
            "mock-task-storage"
        }
        fn run_migrations(&self) -> Result<()> {
            Ok(())
        }
        fn integrity_check(&self) -> Result<String> {
            Ok("ok".into())
        }
        fn checkpoint(&self) -> Result<()> {
            Ok(())
        }
        fn upsert_agent(&self, _: &AgentDefinition) -> Result<()> {
            Ok(())
        }
        fn get_agent(&self, _: &str) -> Result<Option<AgentDefinition>> {
            Ok(None)
        }
        fn list_agents(&self) -> Result<Vec<AgentDefinition>> {
            Ok(Vec::new())
        }
        fn delete_agent(&self, _: &str) -> Result<bool> {
            Ok(false)
        }
        fn create_conversation(&self, _: &Conversation) -> Result<()> {
            Ok(())
        }
        fn get_conversation(&self, _: &str) -> Result<Option<Conversation>> {
            Ok(None)
        }
        fn list_conversations(&self, _: bool) -> Result<Vec<Conversation>> {
            Ok(Vec::new())
        }
        fn delete_conversation(&self, _: &str) -> Result<bool> {
            Ok(false)
        }
        fn append_chat_message(&self, _: &ChatMessage) -> Result<()> {
            Ok(())
        }
        fn list_chat_messages(&self, _: &str, _: bool, _: usize) -> Result<Vec<ChatMessage>> {
            Ok(Vec::new())
        }
        fn search_chat_messages(&self, _: &str, _: usize) -> Result<Vec<ChatMessage>> {
            Ok(Vec::new())
        }
        fn archive_old_chat_messages(&self, _: &str, _: usize) -> Result<usize> {
            Ok(0)
        }
        fn upsert_task(&self, task: &TaskRecord) -> Result<()> {
            self.tasks
                .lock()
                .unwrap()
                .insert(task.id.clone(), task.clone());
            Ok(())
        }
        fn get_task(&self, task_id: &str) -> Result<Option<TaskRecord>> {
            Ok(self.tasks.lock().unwrap().get(task_id).cloned())
        }
        fn list_tasks(&self, limit: usize) -> Result<Vec<TaskRecord>> {
            self.list_tasks_calls.fetch_add(1, Ordering::SeqCst);
            Ok(self
                .tasks
                .lock()
                .unwrap()
                .values()
                .take(limit)
                .cloned()
                .collect())
        }
        fn task_counts_summary(&self) -> Result<TaskCountsSummary> {
            self.summary_calls.fetch_add(1, Ordering::SeqCst);
            let tasks = self.tasks.lock().unwrap();
            let mut summary = TaskCountsSummary::default();
            for task in tasks.values() {
                summary.total += 1;
                match task.status {
                    TaskStatus::Pending => summary.pending += 1,
                    TaskStatus::Assigned | TaskStatus::Running => summary.running += 1,
                    TaskStatus::Waiting => summary.waiting += 1,
                    TaskStatus::Completed => summary.completed += 1,
                    TaskStatus::Failed => summary.failed += 1,
                    TaskStatus::Cancelled => summary.cancelled += 1,
                }
                let entry: &mut AgentTaskStats = summary
                    .per_agent
                    .entry(task.assigned_agent_id.clone())
                    .or_default();
                match task.status {
                    TaskStatus::Completed => entry.completed += 1,
                    TaskStatus::Failed => entry.failed += 1,
                    TaskStatus::Assigned | TaskStatus::Running | TaskStatus::Waiting => {
                        entry.active += 1;
                        entry.latest_active_task_id = Some(task.id.clone());
                    }
                    TaskStatus::Pending | TaskStatus::Cancelled => {}
                }
            }
            Ok(summary)
        }
        fn append_task_event(&self, event: &TaskEvent) -> Result<()> {
            self.events.lock().unwrap().push(event.clone());
            Ok(())
        }
        fn list_task_events(&self, task_id: &str) -> Result<Vec<TaskEvent>> {
            Ok(self
                .events
                .lock()
                .unwrap()
                .iter()
                .filter(|e| e.task_id == task_id)
                .cloned()
                .collect())
        }
        fn append_agent_message(&self, _: &AgentEnvelope) -> Result<()> {
            Ok(())
        }
        fn update_agent_message_status(&self, _: &str, _: DeliveryStatus) -> Result<()> {
            Ok(())
        }
        fn list_agent_messages(&self, _: Option<&str>, _: Option<&str>, _: usize) -> Result<Vec<AgentEnvelope>> {
            Ok(Vec::new())
        }
        fn count_task_agent_messages(&self, _: &str) -> Result<u32> {
            Ok(0)
        }
        fn upsert_memory(&self, _: &MemoryItem) -> Result<()> {
            Ok(())
        }
        fn query_memories(
            &self,
            _: Option<MemoryScope>,
            _: Option<&str>,
            _: Option<&str>,
            _: bool,
            _: usize,
        ) -> Result<Vec<MemoryItem>> {
            Ok(Vec::new())
        }
        fn delete_memory(&self, _: &str) -> Result<bool> {
            Ok(false)
        }
        fn delete_agent_memories(&self, _: &str) -> Result<usize> {
            Ok(0)
        }
        fn memory_stats_for_agent(&self, _: &str) -> Result<(u64, u64)> {
            Ok((0, 0))
        }
        fn upsert_tool_spec(&self, _: &ToolSpec) -> Result<()> {
            Ok(())
        }
        fn list_tool_specs(&self) -> Result<Vec<ToolSpec>> {
            Ok(Vec::new())
        }
        fn record_tool_execution(&self, _: &ToolExecutionRecord) -> Result<()> {
            Ok(())
        }
        fn list_tool_executions(&self, _: Option<&str>, _: usize) -> Result<Vec<ToolExecutionRecord>> {
            Ok(Vec::new())
        }
        fn get_setting(&self, _: &str) -> Result<Option<String>> {
            Ok(None)
        }
        fn set_setting(&self, _: &str, _: &str) -> Result<()> {
            Ok(())
        }
        fn list_settings(&self) -> Result<Vec<(String, String)>> {
            Ok(Vec::new())
        }
        fn create_session(&self, _: &SessionRecord) -> Result<()> {
            Ok(())
        }
        fn get_session_by_hash(&self, _: &str) -> Result<Option<SessionRecord>> {
            Ok(None)
        }
        fn revoke_session(&self, _: &str) -> Result<()> {
            Ok(())
        }
        fn purge_expired_sessions(&self, _: &str) -> Result<usize> {
            Ok(0)
        }
        fn upsert_approval(&self, _: &ApprovalRecord) -> Result<()> {
            Ok(())
        }
        fn get_approval(&self, _: &str) -> Result<Option<ApprovalRecord>> {
            Ok(None)
        }
        fn list_approvals(&self, _: bool, _: usize) -> Result<Vec<ApprovalRecord>> {
            Ok(Vec::new())
        }
        fn append_audit_log(&self, _: &AuditLogEntry) -> Result<()> {
            Ok(())
        }
        fn list_audit_logs(&self, _: usize) -> Result<Vec<AuditLogEntry>> {
            Ok(Vec::new())
        }
    }

    fn make_manager(max_queued_tasks: usize) -> (Arc<MockTaskStorage>, TaskManager) {
        let storage = Arc::new(MockTaskStorage::default());
        let (tx, _rx) = broadcast::channel(32);
        let limits = RuntimeLimits {
            max_queued_tasks,
            ..RuntimeLimits::default()
        };
        let mgr = TaskManager::new(storage.clone(), limits, tx);
        (storage, mgr)
    }

    #[test]
    fn create_task_uses_summary_and_honors_zero_as_unlimited() {
        let (storage, mgr) = make_manager(2);

        let t1 = mgr
            .create_task("req 1".into(), "commander".into(), None, None, 10, TaskStatus::Pending)
            .unwrap();
        let _t2 = mgr
            .create_task("req 2".into(), "commander".into(), None, None, 10, TaskStatus::Running)
            .unwrap();

        let err = mgr
            .create_task("req 3".into(), "commander".into(), None, None, 10, TaskStatus::Pending)
            .unwrap_err();
        assert!(matches!(err, AgentError::LimitExceeded(_)));

        // Child tasks are exempt from root queue cap.
        let child = mgr
            .create_task(
                "child".into(),
                "researcher".into(),
                None,
                Some(t1.id.clone()),
                5,
                TaskStatus::Pending,
            )
            .unwrap();
        assert_eq!(child.parent_task_id.as_deref(), Some(t1.id.as_str()));

        // Zero means unlimited.
        mgr.update_limits(RuntimeLimits {
            max_queued_tasks: 0,
            ..RuntimeLimits::default()
        });
        let _t4 = mgr
            .create_task("req 4".into(), "commander".into(), None, None, 10, TaskStatus::Pending)
            .unwrap();

        assert_eq!(storage.list_calls(), 0);
        assert!(storage.summary_calls() >= 3);
    }

    #[tokio::test]
    async fn tokens_never_leak_across_terminal_transitions_and_shrink_when_sparse() {
        let (_storage, mgr) = make_manager(0);
        let mut ids = Vec::new();
        for i in 0..40 {
            let task = mgr
                .create_task(
                    format!("job {i}"),
                    "programmer".into(),
                    None,
                    None,
                    10,
                    TaskStatus::Running,
                )
                .unwrap();
            let _tok = mgr.register_token(&task.id, None);
            ids.push(task.id);
        }
        assert_eq!(mgr.active_token_count().await, 40);
        assert!(mgr.tokens.read().unwrap().capacity() > 16);

        for (i, id) in ids.iter().enumerate() {
            match i % 4 {
                0 => {
                    mgr.complete_task(id, "ok".into()).unwrap();
                }
                1 => {
                    mgr.fail_task(id, "boom".into()).unwrap();
                }
                2 => {
                    mgr.cancel_task(id, "abort").unwrap();
                    // Re-failing an already-cancelled task must also keep tokens empty.
                    mgr.fail_task(id, "late fail".into()).unwrap();
                }
                _ => {
                    mgr.mark_cancelled(id, "marked").unwrap();
                }
            }
        }

        assert_eq!(mgr.active_token_count().await, 0);
        assert!(mgr.tokens.read().unwrap().capacity() <= 16);
    }

    #[tokio::test]
    async fn recover_on_startup_releases_tokens_and_skips_list_when_all_terminal() {
        let (storage, mgr) = make_manager(0);
        let done = mgr
            .create_task("done".into(), "analyst".into(), None, None, 10, TaskStatus::Completed)
            .unwrap();
        let _stale_tok = mgr.register_token(&done.id, None);
        assert_eq!(mgr.active_token_count().await, 1);

        let report = mgr.recover_on_startup().unwrap();
        assert!(report.resumed_tasks.is_empty());
        assert_eq!(mgr.active_token_count().await, 0);
        assert_eq!(storage.list_calls(), 0);

        // Now insert a Running task directly in storage with a registered token.
        let running = TaskRecord {
            id: "running-1".into(),
            conversation_id: None,
            user_request: "interrupted".into(),
            assigned_agent_id: "researcher".into(),
            parent_task_id: None,
            priority: 10,
            created_at: now_iso(),
            started_at: Some(now_iso()),
            completed_at: None,
            status: TaskStatus::Running,
            result: None,
            error_info: None,
            has_side_effects: false,
            progress_pct: 50,
            progress_note: None,
        };
        storage.insert_raw(running);
        let _run_tok = mgr.register_token("running-1", None);

        let report2 = mgr.recover_on_startup().unwrap();
        assert_eq!(report2.resumed_tasks.len(), 1);
        assert_eq!(mgr.active_token_count().await, 0);
    }

    #[tokio::test]
    async fn cancel_agent_tasks_uses_summary_fast_paths() {
        let (storage, mgr) = make_manager(0);
        assert_eq!(mgr.cancel_agent_tasks("nobody", "cleanup").unwrap(), 0);

        let t = mgr
            .create_task("work".into(), "executor".into(), None, None, 10, TaskStatus::Running)
            .unwrap();
        let tok = mgr.register_token(&t.id, None);

        let cancelled = mgr.cancel_agent_tasks("executor", "disabled").unwrap();
        assert_eq!(cancelled, 1);
        assert!(tok.is_cancelled());
        assert_eq!(mgr.active_token_count().await, 0);
        assert_eq!(storage.list_calls(), 0);
    }

    #[test]
    fn cancelled_task_never_resurrects_on_mark_running_waiting_or_progress() {
        let (_storage, mgr) = make_manager(0);
        let t = mgr
            .create_task("queued".into(), "programmer".into(), Some("conv-1".into()), None, 10, TaskStatus::Pending)
            .unwrap();
        mgr.cancel_task(&t.id, "cancelled while pending").unwrap();

        assert!(matches!(mgr.mark_running(&t.id, None), Err(AgentError::Cancelled(_))));
        assert!(matches!(mgr.mark_waiting(&t.id, "wait"), Err(AgentError::Cancelled(_))));
        assert!(matches!(
            mgr.update_progress(&t.id, 42, Some("step".into())),
            Err(AgentError::Cancelled(_))
        ));
        assert_eq!(mgr.get_task(&t.id).unwrap().unwrap().status, TaskStatus::Cancelled);
    }

    #[tokio::test]
    async fn expedite_task_wakes_waiter_updates_note_and_cleans_up() {
        let storage = Arc::new(MockTaskStorage::default());
        let (tx, mut rx) = broadcast::channel(32);
        let mgr = TaskManager::new(storage, RuntimeLimits::default(), tx);

        let t = mgr
            .create_task(
                "queued".into(),
                "analyst".into(),
                Some("conv-exp".into()),
                None,
                10,
                TaskStatus::Pending,
            )
            .unwrap();
        let _initial_ev = rx.try_recv().unwrap();

        let notify = mgr.register_expedite(&t.id);
        let fut = notify.notified();
        tokio::pin!(fut);
        assert!(!fut.as_mut().enable());

        let updated = mgr.expedite_task(&t.id).unwrap();
        assert_eq!(updated.status, TaskStatus::Pending);
        assert_eq!(
            updated.progress_note.as_deref(),
            Some("Expedited — starting immediately")
        );

        tokio::time::timeout(std::time::Duration::from_secs(1), fut)
            .await
            .expect("expedite waiter should wake");

        match rx.try_recv().unwrap() {
            StreamEvent::TaskUpdated {
                conversation_id,
                task_id,
                status,
                progress_note,
                ..
            } => {
                assert_eq!(conversation_id.as_deref(), Some("conv-exp"));
                assert_eq!(task_id, t.id);
                assert_eq!(status, "PENDING");
                assert_eq!(
                    progress_note.as_deref(),
                    Some("Expedited — starting immediately")
                );
            }
            other => panic!("unexpected event: {other:?}"),
        }

        mgr.complete_task(&t.id, "done".into()).unwrap();
        assert!(mgr.expedite_notifiers.read().unwrap().is_empty());
    }

    #[tokio::test]
    async fn cancel_task_wakes_and_clears_expedite_notifier() {
        let (_storage, mgr) = make_manager(0);
        let parent = mgr
            .create_task("parent".into(), "commander".into(), Some("conv-c".into()), None, 10, TaskStatus::Pending)
            .unwrap();
        let child = mgr
            .create_task(
                "child".into(),
                "researcher".into(),
                Some("conv-c".into()),
                Some(parent.id.clone()),
                5,
                TaskStatus::Pending,
            )
            .unwrap();

        let p_tok = mgr.register_token(&parent.id, None);
        let c_tok = mgr.register_token(&child.id, Some(&p_tok));
        let p_exp = mgr.register_expedite(&parent.id);
        let _c_exp = mgr.register_expedite(&child.id);

        let fut = p_exp.notified();
        tokio::pin!(fut);
        assert!(!fut.as_mut().enable());

        mgr.cancel_task(&parent.id, "stop").unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), fut)
            .await
            .expect("cancel_task should wake expedite waiter");

        assert!(p_tok.is_cancelled());
        assert!(c_tok.is_cancelled());
        assert_eq!(mgr.active_token_count().await, 0);
        assert!(mgr.expedite_notifiers.read().unwrap().is_empty());
    }

    #[tokio::test]
    async fn cancel_conversation_tasks_filters_by_conv_and_agent() {
        let (_storage, mgr) = make_manager(0);
        let t_prog = mgr
            .create_task("a".into(), "programmer".into(), Some("conv-1".into()), None, 10, TaskStatus::Pending)
            .unwrap();
        let t_anal = mgr
            .create_task("b".into(), "analyst".into(), Some("conv-1".into()), None, 10, TaskStatus::Running)
            .unwrap();
        let _t_done = mgr
            .create_task("done".into(), "programmer".into(), Some("conv-1".into()), None, 10, TaskStatus::Completed)
            .unwrap();
        let t_other = mgr
            .create_task("other".into(), "programmer".into(), Some("conv-2".into()), None, 10, TaskStatus::Running)
            .unwrap();

        let tok_prog = mgr.register_token(&t_prog.id, None);
        let tok_anal = mgr.register_token(&t_anal.id, None);
        let tok_other = mgr.register_token(&t_other.id, None);
        let _exp_prog = mgr.register_expedite(&t_prog.id);

        assert_eq!(
            mgr.cancel_conversation_tasks("conv-1", Some("programmer"), "stop prog")
                .unwrap(),
            1
        );
        assert!(tok_prog.is_cancelled());
        assert!(!tok_anal.is_cancelled());
        assert!(!tok_other.is_cancelled());
        assert_eq!(mgr.get_task(&t_prog.id).unwrap().unwrap().status, TaskStatus::Cancelled);
        assert_eq!(mgr.get_task(&t_anal.id).unwrap().unwrap().status, TaskStatus::Running);
        assert!(mgr.expedite_notifiers.read().unwrap().is_empty());

        assert_eq!(
            mgr.cancel_conversation_tasks("conv-1", None, "stop conv")
                .unwrap(),
            1
        );
        assert!(tok_anal.is_cancelled());
        assert!(!tok_other.is_cancelled());
        assert_eq!(mgr.active_token_count().await, 1);
    }

    #[test]
    fn complete_task_and_transitions_do_not_overwrite_terminal_states() {
        let storage = Arc::new(MockTaskStorage::default());
        let (tx, mut rx) = broadcast::channel(32);
        let mgr = TaskManager::new(storage, RuntimeLimits::default(), tx);

        // Cancelled task must NOT be overwritten to Completed or emit TaskUpdated(COMPLETED).
        let cancelled = mgr
            .create_task("req".into(), "programmer".into(), Some("conv-t".into()), None, 10, TaskStatus::Running)
            .unwrap();
        mgr.cancel_task(&cancelled.id, "user stop").unwrap();
        while rx.try_recv().is_ok() {}

        let err = mgr.complete_task(&cancelled.id, "late finish".into()).unwrap_err();
        assert!(matches!(err, AgentError::Cancelled(_)));
        assert_eq!(mgr.get_task(&cancelled.id).unwrap().unwrap().status, TaskStatus::Cancelled);
        assert!(rx.try_recv().is_err(), "no TaskUpdated should emit for cancelled complete_task");

        // Failed task must NOT be overwritten back to Completed, Running, or Waiting.
        let failed = mgr
            .create_task("req2".into(), "analyst".into(), Some("conv-t".into()), None, 10, TaskStatus::Running)
            .unwrap();
        mgr.fail_task(&failed.id, "timed out".into()).unwrap();
        while rx.try_recv().is_ok() {}

        let kept_failed = mgr.complete_task(&failed.id, "late output".into()).unwrap();
        assert_eq!(kept_failed.status, TaskStatus::Failed);
        assert_eq!(kept_failed.error_info.as_deref(), Some("timed out"));
        assert!(rx.try_recv().is_err(), "no TaskUpdated should emit for failed complete_task");
        assert!(matches!(mgr.mark_running(&failed.id, None), Err(AgentError::InvalidInput(_))));
        assert!(matches!(mgr.mark_waiting(&failed.id, "pause"), Err(AgentError::InvalidInput(_))));

        // Completed task must NOT be overwritten to Failed.
        let done = mgr
            .create_task("req3".into(), "executor".into(), None, None, 10, TaskStatus::Running)
            .unwrap();
        mgr.complete_task(&done.id, "ok".into()).unwrap();
        let still_done = mgr.fail_task(&done.id, "late err".into()).unwrap();
        assert_eq!(still_done.status, TaskStatus::Completed);
        assert_eq!(still_done.result.as_deref(), Some("ok"));
    }

    #[tokio::test]
    async fn scan_limits_above_256_preserve_recovery_and_child_cancellation() {
        let (storage, mgr) = make_manager(0);

        for i in 0..320 {
            mgr.create_task(
                format!("hist {i}"),
                "analyst".into(),
                Some("conv-hist".into()),
                None,
                1,
                TaskStatus::Completed,
            )
            .unwrap();
        }

        let parent = mgr
            .create_task(
                "parent".into(),
                "commander".into(),
                Some("conv-big".into()),
                None,
                10,
                TaskStatus::Running,
            )
            .unwrap();
        let child = mgr
            .create_task(
                "child".into(),
                "researcher".into(),
                Some("conv-big".into()),
                Some(parent.id.clone()),
                5,
                TaskStatus::Pending,
            )
            .unwrap();
        let c_tok = mgr.register_token(&child.id, None);

        assert_eq!(mgr.list_subtasks(&parent.id).unwrap().len(), 1);
        let report = mgr.recover_on_startup().unwrap();
        assert_eq!(report.resumed_tasks.len(), 2);

        mgr.cancel_task(&parent.id, "abort big tree").unwrap();
        assert!(c_tok.is_cancelled());
        assert_eq!(mgr.get_task(&child.id).unwrap().unwrap().status, TaskStatus::Cancelled);
        assert_eq!(mgr.active_token_count().await, 0);
        assert!(storage.list_calls() >= 2);
    }
}
