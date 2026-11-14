use crate::migrations::CURRENT_SCHEMA_VERSION;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use ps5_agent_core::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

const SNAPSHOT_DEBOUNCE_MS: u64 = 250;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredRow<T> {
    seq: u64,
    item: T,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
struct JsonlState {
    schema_version: i64,
    next_seq: u64,
    agents: HashMap<String, StoredRow<AgentDefinition>>,
    conversations: HashMap<String, StoredRow<Conversation>>,
    messages: HashMap<String, StoredRow<ChatMessage>>,
    tasks: HashMap<String, StoredRow<TaskRecord>>,
    task_events: HashMap<String, StoredRow<TaskEvent>>,
    agent_messages: HashMap<String, StoredRow<AgentEnvelope>>,
    memories: HashMap<String, StoredRow<MemoryItem>>,
    tool_registry: HashMap<String, StoredRow<ToolSpec>>,
    tool_executions: HashMap<String, StoredRow<ToolExecutionRecord>>,
    settings: HashMap<String, String>,
    audit_logs: HashMap<String, StoredRow<AuditLogEntry>>,
    sessions: HashMap<String, StoredRow<SessionRecord>>,
    approvals: HashMap<String, StoredRow<ApprovalRecord>>,
    #[serde(default)]
    schedules: HashMap<String, StoredRow<ScheduleRecord>>,
    #[serde(default)]
    webhooks: HashMap<String, StoredRow<WebhookRecord>>,
}

impl JsonlState {
    fn next_seq(&mut self) -> u64 {
        self.next_seq = self.next_seq.saturating_add(1);
        self.next_seq
    }
}

struct JsonlInner {
    base_dir: PathBuf,
    state: JsonlState,
    dirty: bool,
    last_snapshot_at: Instant,
}

impl JsonlInner {
    fn flush_snapshot(&mut self) -> Result<()> {
        write_snapshot_atomic(&self.base_dir, &self.state)?;
        self.dirty = false;
        self.last_snapshot_at = Instant::now();
        Ok(())
    }

    fn flush_snapshot_if_dirty(&mut self) -> Result<()> {
        if self.dirty {
            self.flush_snapshot()?;
        }
        Ok(())
    }
}

pub struct JsonlStorage {
    inner: Mutex<JsonlInner>,
}

impl JsonlStorage {
    pub fn open(dir: &Path) -> Result<Self> {
        fs::create_dir_all(dir).map_err(|e| {
            AgentError::Storage(format!("creating jsonl_db dir {}: {e}", dir.display()))
        })?;

        let state_path = dir.join("state.json");
        let mut state = if state_path.exists() {
            let raw = fs::read_to_string(&state_path).map_err(|e| {
                AgentError::Storage(format!("reading {}: {e}", state_path.display()))
            })?;
            serde_json::from_str::<JsonlState>(&raw).map_err(|e| {
                AgentError::Storage(format!("parsing {}: {e}", state_path.display()))
            })?
        } else {
            JsonlState::default()
        };

        let mut dirty = false;
        let schedules_path = dir.join("schedules.jsonl");
        if schedules_path.exists() {
            let (replayed_changed, needs_compact) = replay_jsonl_table(
                &schedules_path,
                &mut state.schedules,
                &mut state.next_seq,
                |r| &r.id,
                |new, old| new.updated_at >= old.updated_at,
            )?;
            dirty |= replayed_changed;
            if needs_compact {
                compact_jsonl_file(dir, "schedules.jsonl", &state.schedules)?;
            }
        }

        let webhooks_path = dir.join("webhooks.jsonl");
        if webhooks_path.exists() {
            let (replayed_changed, needs_compact) = replay_jsonl_table(
                &webhooks_path,
                &mut state.webhooks,
                &mut state.next_seq,
                |w| &w.id,
                |new, old| cmp_rfc3339_asc(&old.updated_at, &new.updated_at) != std::cmp::Ordering::Greater,
            )?;
            dirty |= replayed_changed;
            if needs_compact {
                compact_jsonl_file(dir, "webhooks.jsonl", &state.webhooks)?;
            }
        }

        Ok(Self {
            inner: Mutex::new(JsonlInner {
                base_dir: dir.to_path_buf(),
                state,
                dirty,
                last_snapshot_at: Instant::now(),
            }),
        })
    }

    fn lock_inner(&self) -> Result<MutexGuard<'_, JsonlInner>> {
        self.inner
            .lock()
            .map_err(|_| AgentError::Internal("jsonl storage mutex poisoned".into()))
    }
}

impl Drop for JsonlStorage {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.inner.lock() {
            if guard.dirty {
                let _ = guard.flush_snapshot();
            }
        }
    }
}

fn write_snapshot_atomic(base_dir: &Path, state: &JsonlState) -> Result<()> {
    let target = base_dir.join("state.json");
    let tmp = base_dir.join("state.json.tmp");
    let encoded = serde_json::to_vec(state)
        .map_err(|e| AgentError::Internal(format!("serializing jsonl state: {e}")))?;

    {
        let mut f = File::create(&tmp)
            .map_err(|e| AgentError::Storage(format!("creating {}: {e}", tmp.display())))?;
        f.write_all(&encoded)
            .map_err(|e| AgentError::Storage(format!("writing {}: {e}", tmp.display())))?;
        f.sync_all()
            .map_err(|e| AgentError::Storage(format!("syncing {}: {e}", tmp.display())))?;
    }

    fs::rename(&tmp, &target).map_err(|e| {
        AgentError::Storage(format!(
            "renaming {} -> {}: {e}",
            tmp.display(),
            target.display()
        ))
    })?;
    Ok(())
}

fn append_jsonl_record<T: Serialize>(base_dir: &Path, file_name: &str, item: &T) -> Result<()> {
    let path = base_dir.join(file_name);
    let mut line = serde_json::to_string(item)
        .map_err(|e| AgentError::Internal(format!("serializing {file_name} record: {e}")))?;
    line.push('\n');
    let mut f = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(|e| AgentError::Storage(format!("opening {}: {e}", path.display())))?;
    f.write_all(line.as_bytes())
        .map_err(|e| AgentError::Storage(format!("appending {}: {e}", path.display())))?;
    f.flush()
        .map_err(|e| AgentError::Storage(format!("flushing {}: {e}", path.display())))?;
    Ok(())
}

fn upsert_row<T: Clone>(map: &mut HashMap<String, StoredRow<T>>, id: String, seq: u64, item: T) {
    if let Some(existing) = map.get_mut(&id) {
        existing.item = item;
    } else {
        map.insert(id, StoredRow { seq, item });
    }
}

fn shrink_if_sparse<K: Eq + std::hash::Hash, V>(map: &mut HashMap<K, V>) {
    if map.capacity() > map.len().saturating_mul(4).max(64) {
        map.shrink_to_fit();
    }
}

fn compact_jsonl_file<T: Serialize>(
    base_dir: &Path,
    file_name: &str,
    map: &HashMap<String, StoredRow<T>>,
) -> Result<()> {
    let target = base_dir.join(file_name);
    if !target.exists() && map.is_empty() {
        return Ok(());
    }
    let tmp = base_dir.join(format!("{file_name}.tmp"));
    let mut rows: Vec<&StoredRow<T>> = map.values().collect();
    rows.sort_by_key(|r| r.seq);

    {
        let file = File::create(&tmp)
            .map_err(|e| AgentError::Storage(format!("creating {}: {e}", tmp.display())))?;
        let mut writer = BufWriter::new(file);
        for row in rows {
            serde_json::to_writer(&mut writer, &row.item).map_err(|e| {
                AgentError::Internal(format!("serializing {file_name} record: {e}"))
            })?;
            writer
                .write_all(b"\n")
                .map_err(|e| AgentError::Storage(format!("writing {}: {e}", tmp.display())))?;
        }
        writer
            .flush()
            .map_err(|e| AgentError::Storage(format!("flushing {}: {e}", tmp.display())))?;
        writer
            .get_ref()
            .sync_all()
            .map_err(|e| AgentError::Storage(format!("syncing {}: {e}", tmp.display())))?;
    }

    fs::rename(&tmp, &target).map_err(|e| {
        AgentError::Storage(format!(
            "renaming {} -> {}: {e}",
            tmp.display(),
            target.display()
        ))
    })?;
    Ok(())
}

fn replay_jsonl_table<T, IdFn, ReplaceFn>(
    path: &Path,
    map: &mut HashMap<String, StoredRow<T>>,
    next_seq: &mut u64,
    id_of: IdFn,
    should_replace: ReplaceFn,
) -> Result<(bool, bool)>
where
    T: Clone + PartialEq + for<'de> Deserialize<'de>,
    IdFn: Fn(&T) -> &str,
    ReplaceFn: Fn(&T, &T) -> bool,
{
    let file = File::open(path)
        .map_err(|e| AgentError::Storage(format!("opening {}: {e}", path.display())))?;
    let reader = BufReader::new(file);
    let mut line_count = 0usize;
    let mut latest: HashMap<String, (usize, T)> = HashMap::new();
    let mut pending_bad: Option<(usize, serde_json::Error)> = None;

    for (idx, line) in reader.lines().enumerate() {
        let line = line.map_err(|e| {
            AgentError::Storage(format!("reading {} line {}: {e}", path.display(), idx + 1))
        })?;
        if line.trim().is_empty() {
            continue;
        }
        if let Some((bad_line, bad_err)) = pending_bad.take() {
            return Err(AgentError::Storage(format!(
                "parsing {} line {}: {bad_err}",
                path.display(),
                bad_line
            )));
        }
        match serde_json::from_str::<T>(&line) {
            Ok(rec) => {
                line_count += 1;
                let id = id_of(&rec).to_string();
                if let Some((_, existing)) = latest.get_mut(&id) {
                    if should_replace(&rec, existing) {
                        *existing = rec;
                    }
                } else {
                    latest.insert(id, (idx, rec));
                }
            }
            Err(err) => {
                pending_bad = Some((idx + 1, err));
            }
        }
    }

    let truncated_tail = if let Some((bad_line, bad_err)) = pending_bad {
        ps5_agent_logging::log_warn(
            "database",
            format!(
                "ignoring truncated trailing line {} in {}: {}",
                bad_line,
                path.display(),
                bad_err
            ),
        );
        true
    } else {
        false
    };

    let unique_count = latest.len();
    let mut ordered: Vec<(usize, T)> = latest.into_values().collect();
    ordered.sort_by_key(|(idx, _)| *idx);

    let mut changed = false;
    for (_, rec) in ordered {
        let id = id_of(&rec).to_string();
        let apply = match map.get(&id) {
            None => true,
            Some(existing) => rec != existing.item && should_replace(&rec, &existing.item),
        };
        if apply {
            *next_seq = next_seq.saturating_add(1);
            upsert_row(map, id, *next_seq, rec);
            changed = true;
        }
    }

    Ok((changed, line_count > unique_count || truncated_tail))
}

fn persist_table_upsert<T: Clone + Serialize>(
    inner: &mut JsonlInner,
    select_map: fn(&mut JsonlState) -> &mut HashMap<String, StoredRow<T>>,
    file_name: &str,
    id: &str,
    item: &T,
) -> Result<()> {
    let seq = inner.state.next_seq();
    let file_missing = !inner.base_dir.join(file_name).exists();
    let prev = {
        let map = select_map(&mut inner.state);
        let prev = map.get(id).cloned();
        upsert_row(map, id.to_string(), seq, item.clone());
        prev
    };
    let disk_res = {
        let map = select_map(&mut inner.state);
        if prev.is_some() || file_missing {
            compact_jsonl_file(&inner.base_dir, file_name, map)
        } else {
            append_jsonl_record(&inner.base_dir, file_name, item)
        }
    }
    .and_then(|()| inner.flush_snapshot());
    if let Err(err) = disk_res {
        let map = select_map(&mut inner.state);
        if let Some(old) = prev {
            map.insert(id.to_string(), old);
        } else {
            map.remove(id);
        }
        return Err(err);
    }
    Ok(())
}

fn persist_table_delete<T: Serialize>(
    inner: &mut JsonlInner,
    select_map: fn(&mut JsonlState) -> &mut HashMap<String, StoredRow<T>>,
    file_name: &str,
    id: &str,
) -> Result<bool> {
    let Some(prev) = select_map(&mut inner.state).remove(id) else {
        return Ok(false);
    };
    if let Err(err) = inner.flush_snapshot() {
        select_map(&mut inner.state).insert(id.to_string(), prev);
        return Err(err);
    }
    let map = select_map(&mut inner.state);
    shrink_if_sparse(map);
    compact_jsonl_file(&inner.base_dir, file_name, map)?;
    Ok(true)
}

fn parse_rfc3339_utc(ts: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(ts)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

fn cmp_rfc3339_asc(a: &str, b: &str) -> std::cmp::Ordering {
    match (parse_rfc3339_utc(a), parse_rfc3339_utc(b)) {
        (Some(da), Some(db)) => da.cmp(&db),
        _ => a.cmp(b),
    }
}

fn is_older_than_cutoff(ts: &str, cutoff: DateTime<Utc>) -> bool {
    parse_rfc3339_utc(ts).map_or(false, |dt| dt < cutoff)
}

fn prune_excess_eligible<T, F>(
    map: &mut HashMap<String, StoredRow<T>>,
    max_rows: usize,
    eligible_key: F,
) -> usize
where
    F: Fn(&T) -> Option<(u8, &str)>,
{
    if max_rows == 0 || map.len() <= max_rows {
        return 0;
    }
    let excess = map.len() - max_rows;
    let mut candidates: Vec<(String, u8, String, u64)> = map
        .iter()
        .filter_map(|(id, row)| {
            eligible_key(&row.item).map(|(tier, ts)| (id.clone(), tier, ts.to_string(), row.seq))
        })
        .collect();
    if candidates.is_empty() {
        return 0;
    }
    candidates.sort_by(|a, b| {
        a.1.cmp(&b.1)
            .then_with(|| cmp_rfc3339_asc(&a.2, &b.2))
            .then_with(|| a.3.cmp(&b.3))
    });
    let mut removed = 0;
    for (id, _, _, _) in candidates.into_iter().take(excess) {
        if map.remove(&id).is_some() {
            removed += 1;
        }
    }
    removed
}

fn tasks_equal(a: &TaskRecord, b: &TaskRecord) -> bool {
    a.id == b.id
        && a.conversation_id == b.conversation_id
        && a.user_request == b.user_request
        && a.assigned_agent_id == b.assigned_agent_id
        && a.parent_task_id == b.parent_task_id
        && a.priority == b.priority
        && a.created_at == b.created_at
        && a.started_at == b.started_at
        && a.completed_at == b.completed_at
        && a.status == b.status
        && a.result == b.result
        && a.error_info == b.error_info
        && a.has_side_effects == b.has_side_effects
        && a.progress_pct == b.progress_pct
        && a.progress_note == b.progress_note
}

impl StorageBackend for JsonlStorage {
    fn driver_name(&self) -> &'static str {
        "jsonl"
    }

    fn run_migrations(&self) -> Result<()> {
        let mut guard = self.lock_inner()?;
        if guard.state.schema_version < CURRENT_SCHEMA_VERSION {
            guard.state.schema_version = CURRENT_SCHEMA_VERSION;
            guard.flush_snapshot()?;
        }
        Ok(())
    }

    fn integrity_check(&self) -> Result<String> {
        let guard = self.lock_inner()?;
        let state_path = guard.base_dir.join("state.json");
        if state_path.exists() {
            let raw = fs::read_to_string(&state_path).map_err(|e| {
                AgentError::Storage(format!("reading {}: {e}", state_path.display()))
            })?;
            let _: JsonlState = serde_json::from_str(&raw).map_err(|e| {
                AgentError::Storage(format!("corrupted {}: {e}", state_path.display()))
            })?;
        }

        let entries = fs::read_dir(&guard.base_dir).map_err(|e| {
            AgentError::Storage(format!("scanning {}: {e}", guard.base_dir.display()))
        })?;
        for entry in entries {
            let entry = entry.map_err(|e| AgentError::Storage(format!("dir entry: {e}")))?;
            let p = entry.path();
            if p.extension().and_then(|s| s.to_str()) == Some("jsonl") {
                let file = File::open(&p)
                    .map_err(|e| AgentError::Storage(format!("opening {}: {e}", p.display())))?;
                let reader = BufReader::new(file);
                for (idx, line) in reader.lines().enumerate() {
                    let line = line.map_err(|e| {
                        AgentError::Storage(format!("reading {} line {}: {e}", p.display(), idx + 1))
                    })?;
                    if line.trim().is_empty() {
                        continue;
                    }
                    let _: serde_json::Value = serde_json::from_str(&line).map_err(|e| {
                        AgentError::Storage(format!(
                            "corrupted JSONL {} line {}: {e}",
                            p.display(),
                            idx + 1
                        ))
                    })?;
                }
            }
        }
        Ok("ok".to_string())
    }

    fn checkpoint(&self) -> Result<()> {
        let mut guard = self.lock_inner()?;
        guard.flush_snapshot_if_dirty()
    }

    fn upsert_agent(&self, agent: &AgentDefinition) -> Result<()> {
        let mut guard = self.lock_inner()?;
        let seq = guard.state.next_seq();
        upsert_row(&mut guard.state.agents, agent.id.clone(), seq, agent.clone());
        guard.flush_snapshot()
    }

    fn get_agent(&self, agent_id: &str) -> Result<Option<AgentDefinition>> {
        let guard = self.lock_inner()?;
        Ok(guard.state.agents.get(agent_id).map(|r| r.item.clone()))
    }

    fn list_agents(&self) -> Result<Vec<AgentDefinition>> {
        let guard = self.lock_inner()?;
        let mut items: Vec<&AgentDefinition> = guard.state.agents.values().map(|r| &r.item).collect();
        items.sort_by(|a, b| {
            b.priority
                .cmp(&a.priority)
                .then_with(|| a.created_at.cmp(&b.created_at))
                .then_with(|| a.id.cmp(&b.id))
        });
        Ok(items.into_iter().cloned().collect())
    }

    fn delete_agent(&self, agent_id: &str) -> Result<bool> {
        let mut guard = self.lock_inner()?;
        let removed = guard.state.agents.remove(agent_id).is_some();
        if removed {
            shrink_if_sparse(&mut guard.state.agents);
            guard.flush_snapshot()?;
        }
        Ok(removed)
    }

    fn create_conversation(&self, conv: &Conversation) -> Result<()> {
        let mut guard = self.lock_inner()?;
        if guard.state.conversations.contains_key(&conv.id) {
            return Err(AgentError::Conflict(format!(
                "conversation '{}' already exists",
                conv.id
            )));
        }
        let seq = guard.state.next_seq();
        guard.state.conversations.insert(
            conv.id.clone(),
            StoredRow {
                seq,
                item: conv.clone(),
            },
        );
        guard.flush_snapshot()
    }

    fn get_conversation(&self, conv_id: &str) -> Result<Option<Conversation>> {
        let guard = self.lock_inner()?;
        Ok(guard
            .state
            .conversations
            .get(conv_id)
            .map(|r| r.item.clone()))
    }

    fn list_conversations(&self, include_archived: bool) -> Result<Vec<Conversation>> {
        let guard = self.lock_inner()?;
        let mut items: Vec<&Conversation> = guard
            .state
            .conversations
            .values()
            .map(|r| &r.item)
            .filter(|c| include_archived || !c.archived)
            .collect();
        items.sort_by(|a, b| {
            a.sort_order
                .cmp(&b.sort_order)
                .then_with(|| b.updated_at.cmp(&a.updated_at))
                .then_with(|| b.created_at.cmp(&a.created_at))
                .then_with(|| a.id.cmp(&b.id))
        });
        Ok(items.into_iter().cloned().collect())
    }

    fn reorder_conversations(&self, ordered_ids: &[String]) -> Result<()> {
        let mut guard = self.lock_inner()?;
        for (idx, id) in ordered_ids.iter().enumerate() {
            if let Some(row) = guard.state.conversations.get_mut(id) {
                row.item.sort_order = ((idx as i32) + 1) * 10;
            }
        }
        guard.flush_snapshot()
    }

    fn delete_conversation(&self, conv_id: &str) -> Result<bool> {
        let mut guard = self.lock_inner()?;
        let removed = guard.state.conversations.remove(conv_id).is_some();
        if removed {
            shrink_if_sparse(&mut guard.state.conversations);
            let before_msgs = guard.state.messages.len();
            guard
                .state
                .messages
                .retain(|_, row| row.item.conversation_id != conv_id);
            if guard.state.messages.len() < before_msgs {
                shrink_if_sparse(&mut guard.state.messages);
                compact_jsonl_file(&guard.base_dir, "messages.jsonl", &guard.state.messages)?;
            }
            guard.flush_snapshot()?;
        }
        Ok(removed)
    }

    fn append_chat_message(&self, msg: &ChatMessage) -> Result<()> {
        let mut guard = self.lock_inner()?;
        if !guard.state.conversations.contains_key(&msg.conversation_id) {
            return Err(AgentError::Storage(format!(
                "FOREIGN KEY constraint failed: conversation '{}' not found",
                msg.conversation_id
            )));
        }
        if guard.state.messages.contains_key(&msg.id) {
            return Err(AgentError::Conflict(format!(
                "chat message '{}' already exists",
                msg.id
            )));
        }
        let seq = guard.state.next_seq();
        guard.state.messages.insert(
            msg.id.clone(),
            StoredRow {
                seq,
                item: msg.clone(),
            },
        );
        append_jsonl_record(&guard.base_dir, "messages.jsonl", msg)?;
        guard.flush_snapshot()
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
        let guard = self.lock_inner()?;
        let mut rows: Vec<&StoredRow<ChatMessage>> = guard
            .state
            .messages
            .values()
            .filter(|r| r.item.conversation_id == conv_id && (include_archived || !r.item.archived))
            .collect();
        // Take the most recent `limit` messages, then sort chronologically
        rows.sort_by(|a, b| {
            b.item
                .created_at
                .cmp(&a.item.created_at)
                .then_with(|| b.seq.cmp(&a.seq))
        });
        rows.truncate(limit);
        rows.sort_by(|a, b| {
            a.item
                .created_at
                .cmp(&b.item.created_at)
                .then_with(|| a.seq.cmp(&b.seq))
        });
        Ok(rows.into_iter().map(|r| r.item.clone()).collect())
    }

    fn search_chat_messages(&self, query: &str, limit: usize) -> Result<Vec<ChatMessage>> {
        let q = query.trim();
        if q.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let q_lower = q.to_lowercase();
        let guard = self.lock_inner()?;
        let mut rows: Vec<&StoredRow<ChatMessage>> = guard
            .state
            .messages
            .values()
            .filter(|r| r.item.content.to_lowercase().contains(&q_lower))
            .collect();
        rows.sort_by(|a, b| {
            b.item
                .created_at
                .cmp(&a.item.created_at)
                .then_with(|| b.seq.cmp(&a.seq))
        });
        rows.truncate(limit);
        Ok(rows.into_iter().map(|r| r.item.clone()).collect())
    }

    fn archive_old_chat_messages(&self, conv_id: &str, keep_recent: usize) -> Result<usize> {
        let mut guard = self.lock_inner()?;
        let mut active: Vec<(String, String, u64)> = guard
            .state
            .messages
            .values()
            .filter(|r| r.item.conversation_id == conv_id && !r.item.archived)
            .map(|r| (r.item.id.clone(), r.item.created_at.clone(), r.seq))
            .collect();

        if active.len() <= keep_recent {
            return Ok(0);
        }

        // Sort newest-first so indices >= keep_recent are archived
        active.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| b.2.cmp(&a.2)));
        let mut archived_count = 0;
        for (id, _, _) in active.into_iter().skip(keep_recent) {
            if let Some(row) = guard.state.messages.get_mut(&id) {
                row.item.archived = true;
                archived_count += 1;
            }
        }

        if archived_count > 0 {
            guard.flush_snapshot()?;
        }
        Ok(archived_count)
    }

    fn get_chat_message(&self, msg_id: &str) -> Result<Option<ChatMessage>> {
        let guard = self.lock_inner()?;
        Ok(guard.state.messages.get(msg_id).map(|r| r.item.clone()))
    }

    fn update_chat_message(&self, msg: &ChatMessage) -> Result<bool> {
        let mut guard = self.lock_inner()?;
        let Some(row) = guard.state.messages.get_mut(&msg.id) else {
            return Ok(false);
        };
        row.item = msg.clone();
        compact_jsonl_file(&guard.base_dir, "messages.jsonl", &guard.state.messages)?;
        guard.flush_snapshot()?;
        Ok(true)
    }

    fn delete_chat_message(&self, msg_id: &str) -> Result<bool> {
        let mut guard = self.lock_inner()?;
        let removed = guard.state.messages.remove(msg_id).is_some();
        if removed {
            shrink_if_sparse(&mut guard.state.messages);
            compact_jsonl_file(&guard.base_dir, "messages.jsonl", &guard.state.messages)?;
            guard.flush_snapshot()?;
        }
        Ok(removed)
    }

    fn delete_chat_messages_after(
        &self,
        conversation_id: &str,
        anchor_msg_id: &str,
        inclusive: bool,
    ) -> Result<usize> {
        let mut guard = self.lock_inner()?;
        let mut ordered: Vec<(String, String, u64)> = guard
            .state
            .messages
            .values()
            .filter(|r| r.item.conversation_id == conversation_id)
            .map(|r| (r.item.id.clone(), r.item.created_at.clone(), r.seq))
            .collect();
        ordered.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.2.cmp(&b.2)));
        let Some(pos) = ordered.iter().position(|(id, _, _)| id == anchor_msg_id) else {
            return Ok(0);
        };
        let start = if inclusive { pos } else { pos + 1 };
        let to_remove: Vec<String> = ordered.into_iter().skip(start).map(|(id, _, _)| id).collect();
        let mut deleted = 0usize;
        for id in to_remove {
            if guard.state.messages.remove(&id).is_some() {
                deleted += 1;
            }
        }
        if deleted > 0 {
            shrink_if_sparse(&mut guard.state.messages);
            compact_jsonl_file(&guard.base_dir, "messages.jsonl", &guard.state.messages)?;
            guard.flush_snapshot()?;
        }
        Ok(deleted)
    }

    fn upsert_task(&self, task: &TaskRecord) -> Result<()> {
        let mut guard = self.lock_inner()?;
        let (unchanged, status_changed, is_new) = match guard.state.tasks.get(&task.id) {
            Some(existing) => (
                tasks_equal(&existing.item, task),
                existing.item.status != task.status,
                false,
            ),
            None => (false, true, true),
        };
        if unchanged {
            return Ok(());
        }

        let seq = guard.state.next_seq();
        upsert_row(&mut guard.state.tasks, task.id.clone(), seq, task.clone());
        guard.dirty = true;
        if is_new
            || status_changed
            || guard.last_snapshot_at.elapsed() >= Duration::from_millis(SNAPSHOT_DEBOUNCE_MS)
        {
            guard.flush_snapshot()?;
        }
        Ok(())
    }

    fn get_task(&self, task_id: &str) -> Result<Option<TaskRecord>> {
        let guard = self.lock_inner()?;
        Ok(guard.state.tasks.get(task_id).map(|r| r.item.clone()))
    }

    fn list_tasks(&self, limit: usize) -> Result<Vec<TaskRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let guard = self.lock_inner()?;
        let mut rows: Vec<&StoredRow<TaskRecord>> = guard.state.tasks.values().collect();
        rows.sort_by(|a, b| {
            b.item
                .created_at
                .cmp(&a.item.created_at)
                .then_with(|| b.seq.cmp(&a.seq))
        });
        rows.truncate(limit);
        Ok(rows.into_iter().map(|r| r.item.clone()).collect())
    }

    fn task_counts_summary(&self) -> Result<TaskCountsSummary> {
        let guard = self.lock_inner()?;
        let mut summary = TaskCountsSummary::default();
        let mut best_active: HashMap<&str, (&str, u64, &str)> = HashMap::new();
        let mut best_activity: HashMap<&str, &str> = HashMap::new();

        for row in guard.state.tasks.values() {
            let task = &row.item;
            summary.total += 1;
            match task.status {
                TaskStatus::Pending => summary.pending += 1,
                TaskStatus::Assigned | TaskStatus::Running => summary.running += 1,
                TaskStatus::Waiting => summary.waiting += 1,
                TaskStatus::Completed => summary.completed += 1,
                TaskStatus::Failed => summary.failed += 1,
                TaskStatus::Cancelled => summary.cancelled += 1,
            }

            let agent_id = task.assigned_agent_id.as_str();
            let stats = if let Some(existing) = summary.per_agent.get_mut(agent_id) {
                existing
            } else {
                summary
                    .per_agent
                    .entry(task.assigned_agent_id.clone())
                    .or_default()
            };

            match task.status {
                TaskStatus::Completed => stats.completed += 1,
                TaskStatus::Failed => stats.failed += 1,
                TaskStatus::Assigned | TaskStatus::Running | TaskStatus::Waiting => {
                    stats.active += 1;
                    let candidate = (task.created_at.as_str(), row.seq, task.id.as_str());
                    let replace = best_active.get(agent_id).map_or(true, |cur| {
                        cmp_rfc3339_asc(candidate.0, cur.0)
                            .then_with(|| candidate.1.cmp(&cur.1))
                            .is_gt()
                    });
                    if replace {
                        best_active.insert(agent_id, candidate);
                    }
                }
                TaskStatus::Pending | TaskStatus::Cancelled => {}
            }

            let activity_ts = task
                .completed_at
                .as_deref()
                .or(task.started_at.as_deref())
                .unwrap_or(task.created_at.as_str());
            if best_activity
                .get(agent_id)
                .map_or(true, |cur| cmp_rfc3339_asc(activity_ts, cur).is_gt())
            {
                best_activity.insert(agent_id, activity_ts);
            }
        }

        for (agent_id, (_, _, task_id)) in best_active {
            if let Some(stats) = summary.per_agent.get_mut(agent_id) {
                stats.latest_active_task_id = Some(task_id.to_string());
            }
        }
        for (agent_id, ts) in best_activity {
            if let Some(stats) = summary.per_agent.get_mut(agent_id) {
                stats.last_activity = Some(ts.to_string());
            }
        }

        Ok(summary)
    }

    fn append_task_event(&self, event: &TaskEvent) -> Result<()> {
        let mut guard = self.lock_inner()?;
        if !guard.state.tasks.contains_key(&event.task_id) {
            return Err(AgentError::Storage(format!(
                "FOREIGN KEY constraint failed: task '{}' not found",
                event.task_id
            )));
        }
        if guard.state.task_events.contains_key(&event.id) {
            return Err(AgentError::Conflict(format!(
                "task event '{}' already exists",
                event.id
            )));
        }
        let seq = guard.state.next_seq();
        guard.state.task_events.insert(
            event.id.clone(),
            StoredRow {
                seq,
                item: event.clone(),
            },
        );
        append_jsonl_record(&guard.base_dir, "task_events.jsonl", event)?;
        guard.flush_snapshot()
    }

    fn list_task_events(&self, task_id: &str) -> Result<Vec<TaskEvent>> {
        let guard = self.lock_inner()?;
        let mut rows: Vec<&StoredRow<TaskEvent>> = guard
            .state
            .task_events
            .values()
            .filter(|r| r.item.task_id == task_id)
            .collect();
        rows.sort_by(|a, b| {
            a.item
                .timestamp
                .cmp(&b.item.timestamp)
                .then_with(|| a.seq.cmp(&b.seq))
        });
        Ok(rows.into_iter().map(|r| r.item.clone()).collect())
    }

    fn append_agent_message(&self, msg: &AgentEnvelope) -> Result<()> {
        let mut guard = self.lock_inner()?;
        if guard.state.agent_messages.contains_key(&msg.id) {
            return Err(AgentError::Conflict(format!(
                "agent message '{}' already exists",
                msg.id
            )));
        }
        let seq = guard.state.next_seq();
        guard.state.agent_messages.insert(
            msg.id.clone(),
            StoredRow {
                seq,
                item: msg.clone(),
            },
        );
        append_jsonl_record(&guard.base_dir, "agent_messages.jsonl", msg)?;
        guard.flush_snapshot()
    }

    fn update_agent_message_status(&self, msg_id: &str, status: DeliveryStatus) -> Result<()> {
        let mut guard = self.lock_inner()?;
        let Some(row) = guard.state.agent_messages.get_mut(msg_id) else {
            return Err(AgentError::NotFound(format!(
                "agent message '{msg_id}' not found"
            )));
        };
        if row.item.status == status {
            return Ok(());
        }
        row.item.status = status;
        guard.flush_snapshot()
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
        let guard = self.lock_inner()?;
        let mut rows: Vec<&StoredRow<AgentEnvelope>> = guard
            .state
            .agent_messages
            .values()
            .filter(|r| {
                task_id.map_or(true, |tid| r.item.task_id.as_deref() == Some(tid))
                    && agent_id.map_or(true, |aid| {
                        r.item.sender_agent_id == aid || r.item.recipient_agent_id == aid
                    })
            })
            .collect();
        rows.sort_by(|a, b| {
            b.item
                .timestamp
                .cmp(&a.item.timestamp)
                .then_with(|| b.seq.cmp(&a.seq))
        });
        rows.truncate(limit);
        Ok(rows.into_iter().map(|r| r.item.clone()).collect())
    }

    fn count_task_agent_messages(&self, task_id: &str) -> Result<u32> {
        let guard = self.lock_inner()?;
        let count = guard
            .state
            .agent_messages
            .values()
            .filter(|r| r.item.task_id.as_deref() == Some(task_id))
            .count();
        Ok(count as u32)
    }

    fn upsert_memory(&self, item: &MemoryItem) -> Result<()> {
        let mut guard = self.lock_inner()?;
        let seq = guard.state.next_seq();
        upsert_row(&mut guard.state.memories, item.id.clone(), seq, item.clone());
        guard.flush_snapshot()
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
        let guard = self.lock_inner()?;
        let mut rows: Vec<&StoredRow<MemoryItem>> = guard
            .state
            .memories
            .values()
            .filter(|r| {
                scope.map_or(true, |s| r.item.scope == s)
                    && namespace.map_or(true, |ns| r.item.namespace == ns)
                    && owner_agent_id.map_or(true, |aid| r.item.owner_agent_id.as_deref() == Some(aid))
                    && (include_archived || !r.item.archived)
            })
            .collect();
        rows.sort_by(|a, b| {
            b.item
                .importance
                .cmp(&a.item.importance)
                .then_with(|| b.item.updated_at.cmp(&a.item.updated_at))
                .then_with(|| b.seq.cmp(&a.seq))
        });
        rows.truncate(limit);
        Ok(rows.into_iter().map(|r| r.item.clone()).collect())
    }

    fn delete_memory(&self, memory_id: &str) -> Result<bool> {
        let mut guard = self.lock_inner()?;
        let removed = guard.state.memories.remove(memory_id).is_some();
        if removed {
            shrink_if_sparse(&mut guard.state.memories);
            guard.flush_snapshot()?;
        }
        Ok(removed)
    }

    fn delete_agent_memories(&self, agent_id: &str) -> Result<usize> {
        let mut guard = self.lock_inner()?;
        let before = guard.state.memories.len();
        guard
            .state
            .memories
            .retain(|_, r| r.item.owner_agent_id.as_deref() != Some(agent_id));
        let deleted = before - guard.state.memories.len();
        if deleted > 0 {
            shrink_if_sparse(&mut guard.state.memories);
            guard.flush_snapshot()?;
        }
        Ok(deleted)
    }

    fn memory_stats_for_agent(&self, agent_id: &str) -> Result<(u64, u64)> {
        let guard = self.lock_inner()?;
        let mut count = 0u64;
        let mut bytes = 0u64;
        for row in guard.state.memories.values() {
            if !row.item.archived && row.item.owner_agent_id.as_deref() == Some(agent_id) {
                count += 1;
                bytes += row.item.content.len() as u64;
            }
        }
        Ok((count, bytes))
    }

    fn upsert_tool_spec(&self, spec: &ToolSpec) -> Result<()> {
        let mut guard = self.lock_inner()?;
        let seq = guard.state.next_seq();
        upsert_row(
            &mut guard.state.tool_registry,
            spec.id.clone(),
            seq,
            spec.clone(),
        );
        guard.flush_snapshot()
    }

    fn list_tool_specs(&self) -> Result<Vec<ToolSpec>> {
        let guard = self.lock_inner()?;
        let mut items: Vec<&ToolSpec> = guard
            .state
            .tool_registry
            .values()
            .map(|r| &r.item)
            .collect();
        items.sort_by(|a, b| {
            a.category
                .cmp(&b.category)
                .then_with(|| a.name.cmp(&b.name))
                .then_with(|| a.id.cmp(&b.id))
        });
        Ok(items.into_iter().cloned().collect())
    }

    fn delete_tool_spec(&self, tool_id: &str) -> Result<bool> {
        let mut guard = self.lock_inner()?;
        let removed = guard.state.tool_registry.remove(tool_id).is_some();
        if removed {
            shrink_if_sparse(&mut guard.state.tool_registry);
            guard.flush_snapshot()?;
        }
        Ok(removed)
    }

    fn record_tool_execution(&self, rec: &ToolExecutionRecord) -> Result<()> {
        let mut guard = self.lock_inner()?;
        if guard.state.tool_executions.contains_key(&rec.id) {
            return Err(AgentError::Conflict(format!(
                "tool execution '{}' already exists",
                rec.id
            )));
        }
        let seq = guard.state.next_seq();
        guard.state.tool_executions.insert(
            rec.id.clone(),
            StoredRow {
                seq,
                item: rec.clone(),
            },
        );
        append_jsonl_record(&guard.base_dir, "tool_executions.jsonl", rec)?;
        guard.flush_snapshot()
    }

    fn list_tool_executions(
        &self,
        agent_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ToolExecutionRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let guard = self.lock_inner()?;
        let mut rows: Vec<&StoredRow<ToolExecutionRecord>> = guard
            .state
            .tool_executions
            .values()
            .filter(|r| agent_id.map_or(true, |aid| r.item.agent_id == aid))
            .collect();
        rows.sort_by(|a, b| {
            b.item
                .created_at
                .cmp(&a.item.created_at)
                .then_with(|| b.seq.cmp(&a.seq))
        });
        rows.truncate(limit);
        Ok(rows.into_iter().map(|r| r.item.clone()).collect())
    }

    fn get_setting(&self, key: &str) -> Result<Option<String>> {
        let guard = self.lock_inner()?;
        Ok(guard.state.settings.get(key).cloned())
    }

    fn set_setting(&self, key: &str, value: &str) -> Result<()> {
        let mut guard = self.lock_inner()?;
        if guard.state.settings.get(key).map(String::as_str) == Some(value) {
            return Ok(());
        }
        guard
            .state
            .settings
            .insert(key.to_string(), value.to_string());
        guard.flush_snapshot()
    }

    fn list_settings(&self) -> Result<Vec<(String, String)>> {
        let guard = self.lock_inner()?;
        let mut pairs: Vec<(String, String)> = guard
            .state
            .settings
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        pairs.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(pairs)
    }

    fn create_session(&self, sess: &SessionRecord) -> Result<()> {
        let mut guard = self.lock_inner()?;
        if guard.state.sessions.contains_key(&sess.id)
            || guard
                .state
                .sessions
                .values()
                .any(|r| r.item.token_sha256 == sess.token_sha256)
        {
            return Err(AgentError::Conflict(format!(
                "session '{}' or token hash already exists",
                sess.id
            )));
        }
        let seq = guard.state.next_seq();
        guard.state.sessions.insert(
            sess.id.clone(),
            StoredRow {
                seq,
                item: sess.clone(),
            },
        );
        guard.flush_snapshot()
    }

    fn get_session_by_hash(&self, token_sha256: &str) -> Result<Option<SessionRecord>> {
        let guard = self.lock_inner()?;
        Ok(guard
            .state
            .sessions
            .values()
            .find(|r| r.item.token_sha256 == token_sha256)
            .map(|r| r.item.clone()))
    }

    fn revoke_session(&self, session_id: &str) -> Result<()> {
        let mut guard = self.lock_inner()?;
        if let Some(row) = guard.state.sessions.get_mut(session_id) {
            if row.item.revoked {
                return Ok(());
            }
            row.item.revoked = true;
            guard.flush_snapshot()?;
        }
        Ok(())
    }

    fn purge_expired_sessions(&self, now_iso: &str) -> Result<usize> {
        let now_ts = chrono::DateTime::parse_from_rfc3339(now_iso)
            .map(|d| d.with_timezone(&chrono::Utc))
            .unwrap_or_else(|_| chrono::Utc::now());
        let mut guard = self.lock_inner()?;
        let before = guard.state.sessions.len();
        guard.state.sessions.retain(|_, r| {
            match chrono::DateTime::parse_from_rfc3339(&r.item.expires_at) {
                Ok(exp_ts) => exp_ts.with_timezone(&chrono::Utc) > now_ts,
                Err(_) => false,
            }
        });
        let purged = before - guard.state.sessions.len();
        if purged > 0 {
            shrink_if_sparse(&mut guard.state.sessions);
            guard.flush_snapshot()?;
        }
        Ok(purged)
    }

    fn upsert_approval(&self, approval: &ApprovalRecord) -> Result<()> {
        let mut guard = self.lock_inner()?;
        let seq = guard.state.next_seq();
        upsert_row(
            &mut guard.state.approvals,
            approval.id.clone(),
            seq,
            approval.clone(),
        );
        guard.flush_snapshot()
    }

    fn get_approval(&self, approval_id: &str) -> Result<Option<ApprovalRecord>> {
        let guard = self.lock_inner()?;
        Ok(guard
            .state
            .approvals
            .get(approval_id)
            .map(|r| r.item.clone()))
    }

    fn list_approvals(&self, pending_only: bool, limit: usize) -> Result<Vec<ApprovalRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let guard = self.lock_inner()?;
        let mut rows: Vec<&StoredRow<ApprovalRecord>> = guard
            .state
            .approvals
            .values()
            .filter(|r| !pending_only || r.item.status == ApprovalStatus::Pending)
            .collect();
        rows.sort_by(|a, b| {
            b.item
                .created_at
                .cmp(&a.item.created_at)
                .then_with(|| b.seq.cmp(&a.seq))
        });
        rows.truncate(limit);
        Ok(rows.into_iter().map(|r| r.item.clone()).collect())
    }

    fn append_audit_log(&self, entry: &AuditLogEntry) -> Result<()> {
        let mut guard = self.lock_inner()?;
        if guard.state.audit_logs.contains_key(&entry.id) {
            return Err(AgentError::Conflict(format!(
                "audit log '{}' already exists",
                entry.id
            )));
        }
        let seq = guard.state.next_seq();
        guard.state.audit_logs.insert(
            entry.id.clone(),
            StoredRow {
                seq,
                item: entry.clone(),
            },
        );
        append_jsonl_record(&guard.base_dir, "audit_logs.jsonl", entry)?;
        guard.flush_snapshot()
    }

    fn list_audit_logs(&self, limit: usize) -> Result<Vec<AuditLogEntry>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let guard = self.lock_inner()?;
        let mut rows: Vec<&StoredRow<AuditLogEntry>> = guard.state.audit_logs.values().collect();
        rows.sort_by(|a, b| {
            b.item
                .timestamp
                .cmp(&a.item.timestamp)
                .then_with(|| b.seq.cmp(&a.seq))
        });
        rows.truncate(limit);
        Ok(rows.into_iter().map(|r| r.item.clone()).collect())
    }

    fn upsert_schedule(&self, schedule: &ScheduleRecord) -> Result<()> {
        let mut guard = self.lock_inner()?;
        persist_table_upsert(
            &mut guard,
            |s| &mut s.schedules,
            "schedules.jsonl",
            &schedule.id,
            schedule,
        )
    }

    fn get_schedule(&self, id: &str) -> Result<Option<ScheduleRecord>> {
        let guard = self.lock_inner()?;
        Ok(guard.state.schedules.get(id).map(|r| r.item.clone()))
    }

    fn list_schedules(&self, limit: usize) -> Result<Vec<ScheduleRecord>> {
        let cap = if limit == 0 { 1000 } else { limit.min(1000) };
        let guard = self.lock_inner()?;
        let mut rows: Vec<&StoredRow<ScheduleRecord>> = guard.state.schedules.values().collect();
        rows.sort_by(|a, b| {
            b.item
                .created_at
                .cmp(&a.item.created_at)
                .then_with(|| a.item.id.cmp(&b.item.id))
        });
        rows.truncate(cap);
        Ok(rows.into_iter().map(|r| r.item.clone()).collect())
    }

    fn list_due_schedules(&self, now: DateTime<Utc>, limit: usize) -> Result<Vec<ScheduleRecord>> {
        let cap = if limit == 0 { 64 } else { limit.min(512) };
        let guard = self.lock_inner()?;
        let mut rows: Vec<&StoredRow<ScheduleRecord>> = guard
            .state
            .schedules
            .values()
            .filter(|r| r.item.enabled && r.item.next_run_at.map_or(false, |t| t <= now))
            .collect();
        rows.sort_by(|a, b| {
            a.item
                .next_run_at
                .cmp(&b.item.next_run_at)
                .then_with(|| a.item.id.cmp(&b.item.id))
        });
        rows.truncate(cap);
        Ok(rows.into_iter().map(|r| r.item.clone()).collect())
    }

    fn delete_schedule(&self, id: &str) -> Result<()> {
        let mut guard = self.lock_inner()?;
        let _ = persist_table_delete(&mut guard, |s| &mut s.schedules, "schedules.jsonl", id)?;
        Ok(())
    }

    fn list_webhooks(&self) -> Result<Vec<WebhookRecord>> {
        let guard = self.lock_inner()?;
        let mut rows: Vec<&StoredRow<WebhookRecord>> = guard.state.webhooks.values().collect();
        rows.sort_by(|a, b| {
            cmp_rfc3339_asc(&b.item.created_at, &a.item.created_at)
                .then_with(|| a.item.id.cmp(&b.item.id))
        });
        Ok(rows.into_iter().map(|r| r.item.clone()).collect())
    }

    fn get_webhook(&self, id: &str) -> Result<Option<WebhookRecord>> {
        let guard = self.lock_inner()?;
        Ok(guard.state.webhooks.get(id).map(|r| r.item.clone()))
    }

    fn upsert_webhook(&self, hook: &WebhookRecord) -> Result<()> {
        let mut guard = self.lock_inner()?;
        persist_table_upsert(
            &mut guard,
            |s| &mut s.webhooks,
            "webhooks.jsonl",
            &hook.id,
            hook,
        )
    }

    fn delete_webhook(&self, id: &str) -> Result<bool> {
        let mut guard = self.lock_inner()?;
        persist_table_delete(&mut guard, |s| &mut s.webhooks, "webhooks.jsonl", id)
    }

    fn prune_old_records(&self, retention_days: u32, max_rows_per_table: usize) -> Result<PruneReport> {
        let now_ts = Utc::now();
        let cutoff = if retention_days > 0 {
            let days = i64::from(retention_days.min(36_500));
            Some(now_ts - ChronoDuration::days(days))
        } else {
            None
        };

        let mut guard = self.lock_inner()?;
        let mut report = PruneReport::default();

        // 1. Purge expired or revoked sessions.
        let before_sessions = guard.state.sessions.len();
        guard.state.sessions.retain(|_, r| {
            if r.item.revoked {
                return false;
            }
            parse_rfc3339_utc(&r.item.expires_at).map_or(false, |exp_ts| exp_ts > now_ts)
        });
        report.purged_sessions = before_sessions - guard.state.sessions.len();
        if report.purged_sessions > 0 {
            shrink_if_sparse(&mut guard.state.sessions);
        }

        // 2. Prune oldest terminal tasks beyond retention_days / max_rows_per_table.
        let before_tasks = guard.state.tasks.len();
        if let Some(c) = cutoff {
            guard.state.tasks.retain(|_, r| {
                if !r.item.status.is_terminal() {
                    return true;
                }
                let ts = r
                    .item
                    .completed_at
                    .as_deref()
                    .or(r.item.started_at.as_deref())
                    .unwrap_or(r.item.created_at.as_str());
                !is_older_than_cutoff(ts, c)
            });
        }
        prune_excess_eligible(&mut guard.state.tasks, max_rows_per_table, |t| {
            t.status.is_terminal().then(|| {
                let ts = t
                    .completed_at
                    .as_deref()
                    .or(t.started_at.as_deref())
                    .unwrap_or(t.created_at.as_str());
                (0, ts)
            })
        });
        report.deleted_tasks = before_tasks - guard.state.tasks.len();
        if report.deleted_tasks > 0 {
            shrink_if_sparse(&mut guard.state.tasks);
        }

        let active_task_ids: HashSet<String> = guard
            .state
            .tasks
            .iter()
            .filter(|(_, r)| !r.item.status.is_terminal())
            .map(|(id, _)| id.clone())
            .collect();

        let state = &mut guard.state;

        // 3. Prune orphaned, aged, or excess task_events.
        let before_events = state.task_events.len();
        state.task_events.retain(|_, r| {
            if !state.tasks.contains_key(&r.item.task_id) {
                return false;
            }
            if let Some(c) = cutoff {
                if !active_task_ids.contains(&r.item.task_id)
                    && is_older_than_cutoff(&r.item.timestamp, c)
                {
                    return false;
                }
            }
            true
        });
        prune_excess_eligible(&mut state.task_events, max_rows_per_table, |e| {
            let tier = u8::from(active_task_ids.contains(&e.task_id));
            Some((tier, e.timestamp.as_str()))
        });
        report.deleted_task_events = before_events - state.task_events.len();
        if report.deleted_task_events > 0 {
            shrink_if_sparse(&mut state.task_events);
        }

        // 4. Prune orphaned, aged, or excess agent_messages.
        let before_agent_msgs = state.agent_messages.len();
        state.agent_messages.retain(|_, r| {
            if let Some(tid) = r.item.task_id.as_deref() {
                if !state.tasks.contains_key(tid) {
                    return false;
                }
            }
            if let Some(c) = cutoff {
                let is_active_task = r
                    .item
                    .task_id
                    .as_deref()
                    .map_or(false, |tid| active_task_ids.contains(tid));
                if !is_active_task && is_older_than_cutoff(&r.item.timestamp, c) {
                    return false;
                }
            }
            true
        });
        prune_excess_eligible(&mut state.agent_messages, max_rows_per_table, |m| {
            let is_active_task = m
                .task_id
                .as_deref()
                .map_or(false, |tid| active_task_ids.contains(tid));
            Some((u8::from(is_active_task), m.timestamp.as_str()))
        });
        report.deleted_agent_messages = before_agent_msgs - state.agent_messages.len();
        if report.deleted_agent_messages > 0 {
            shrink_if_sparse(&mut state.agent_messages);
        }

        // 5. Prune aged or excess tool_executions.
        let before_execs = guard.state.tool_executions.len();
        if let Some(c) = cutoff {
            guard
                .state
                .tool_executions
                .retain(|_, r| !is_older_than_cutoff(&r.item.created_at, c));
        }
        prune_excess_eligible(&mut guard.state.tool_executions, max_rows_per_table, |rec| {
            Some((0, rec.created_at.as_str()))
        });
        report.deleted_tool_executions = before_execs - guard.state.tool_executions.len();
        if report.deleted_tool_executions > 0 {
            shrink_if_sparse(&mut guard.state.tool_executions);
        }

        // 6. Prune aged or excess audit_logs.
        let before_audits = guard.state.audit_logs.len();
        if let Some(c) = cutoff {
            guard
                .state
                .audit_logs
                .retain(|_, r| !is_older_than_cutoff(&r.item.timestamp, c));
        }
        prune_excess_eligible(&mut guard.state.audit_logs, max_rows_per_table, |entry| {
            Some((0, entry.timestamp.as_str()))
        });
        report.deleted_audit_logs = before_audits - guard.state.audit_logs.len();
        if report.deleted_audit_logs > 0 {
            shrink_if_sparse(&mut guard.state.audit_logs);
        }

        // 7. Prune resolved approvals (keep pending approvals).
        let before_approvals = guard.state.approvals.len();
        if let Some(c) = cutoff {
            guard.state.approvals.retain(|_, r| {
                if r.item.status == ApprovalStatus::Pending {
                    return true;
                }
                let ts = r
                    .item
                    .resolved_at
                    .as_deref()
                    .unwrap_or(r.item.created_at.as_str());
                !is_older_than_cutoff(ts, c)
            });
        }
        prune_excess_eligible(&mut guard.state.approvals, max_rows_per_table, |a| {
            (a.status != ApprovalStatus::Pending).then(|| {
                let ts = a.resolved_at.as_deref().unwrap_or(a.created_at.as_str());
                (0, ts)
            })
        });
        let deleted_approvals = before_approvals - guard.state.approvals.len();
        if deleted_approvals > 0 {
            shrink_if_sparse(&mut guard.state.approvals);
        }

        // 8. Prune archived chat_messages (preserve active/unarchived messages).
        let before_msgs = guard.state.messages.len();
        if let Some(c) = cutoff {
            guard.state.messages.retain(|_, r| {
                !(r.item.archived && is_older_than_cutoff(&r.item.created_at, c))
            });
        }
        prune_excess_eligible(&mut guard.state.messages, max_rows_per_table, |m| {
            m.archived.then_some((0, m.created_at.as_str()))
        });
        report.deleted_chat_messages = before_msgs - guard.state.messages.len();
        if report.deleted_chat_messages > 0 {
            shrink_if_sparse(&mut guard.state.messages);
        }

        // 9. Atomically compact affected append-only .jsonl files & snapshot state.json.
        if report.deleted_chat_messages > 0 {
            compact_jsonl_file(&guard.base_dir, "messages.jsonl", &guard.state.messages)?;
        }
        if report.deleted_task_events > 0 {
            compact_jsonl_file(&guard.base_dir, "task_events.jsonl", &guard.state.task_events)?;
        }
        if report.deleted_agent_messages > 0 {
            compact_jsonl_file(
                &guard.base_dir,
                "agent_messages.jsonl",
                &guard.state.agent_messages,
            )?;
        }
        if report.deleted_tool_executions > 0 {
            compact_jsonl_file(
                &guard.base_dir,
                "tool_executions.jsonl",
                &guard.state.tool_executions,
            )?;
        }
        if report.deleted_audit_logs > 0 {
            compact_jsonl_file(&guard.base_dir, "audit_logs.jsonl", &guard.state.audit_logs)?;
        }

        let any_removed = report.purged_sessions > 0
            || report.deleted_tasks > 0
            || report.deleted_task_events > 0
            || report.deleted_agent_messages > 0
            || report.deleted_tool_executions > 0
            || report.deleted_audit_logs > 0
            || report.deleted_chat_messages > 0
            || deleted_approvals > 0;
        if any_removed {
            guard.flush_snapshot()?;
        }

        Ok(report)
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

    fn make_task(id: &str, agent_id: &str, status: TaskStatus, created_at: &str) -> TaskRecord {
        TaskRecord {
            id: id.to_string(),
            conversation_id: Some("conv-1".to_string()),
            user_request: format!("req {id}"),
            assigned_agent_id: agent_id.to_string(),
            parent_task_id: None,
            priority: 1,
            created_at: created_at.to_string(),
            started_at: Some(created_at.to_string()),
            completed_at: if status.is_terminal() {
                Some(created_at.to_string())
            } else {
                None
            },
            status,
            result: None,
            error_info: None,
            has_side_effects: false,
            progress_pct: if status == TaskStatus::Completed { 100 } else { 0 },
            progress_note: None,
        }
    }

    #[test]
    fn purge_expired_sessions_handles_mixed_rfc3339_offsets() {
        let dir = TempDir::new("ps5-db-jsonl-purge");
        let storage = JsonlStorage::open(&dir.0).unwrap();
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
    fn v1_state_deserializes_with_defaults_and_attachments_roundtrip() {
        let dir = TempDir::new("ps5-db-jsonl-v1-compat");
        let v1_state = serde_json::json!({
            "schema_version": 1,
            "next_seq": 3,
            "agents": {
                "a1": {
                    "seq": 1,
                    "item": {
                        "id": "a1",
                        "name": "Agent 1",
                        "description": "Legacy agent",
                        "role": "RESEARCHER",
                        "role_label": "Researcher",
                        "system_prompt": "Prompt",
                        "model_config": {
                            "provider": "ps5lm",
                            "model_override": null,
                            "api_mode": "chat_completions",
                            "temperature": 0.4,
                            "max_output_tokens": 1024,
                            "context_limit": 8192
                        },
                        "enabled_tools": [],
                        "memory_permissions": {
                            "read_own": true,
                            "write_own": true,
                            "read_shared": true,
                            "write_shared": true,
                            "read_user_prefs": true,
                            "write_user_prefs": false,
                            "cross_agent_read": false
                        },
                        "max_task_duration_secs": 60,
                        "max_reasoning_iterations": 8,
                        "priority": 1,
                        "can_communicate": true,
                        "enabled": true,
                        "is_builtin": false,
                        "is_protected": false,
                        "is_temporary": false,
                        "created_at": "2026-11-13T08:00:00Z",
                        "updated_at": "2026-11-13T08:00:00Z"
                    }
                }
            },
            "conversations": {
                "conv-1": {
                    "seq": 2,
                    "item": {
                        "id": "conv-1",
                        "title": "Legacy",
                        "mode": { "kind": "direct", "agent_id": "a1" },
                        "created_at": "2026-11-13T08:00:00Z",
                        "updated_at": "2026-11-13T08:00:00Z",
                        "archived": false
                    }
                }
            },
            "messages": {
                "m1": {
                    "seq": 3,
                    "item": {
                        "id": "m1",
                        "conversation_id": "conv-1",
                        "role": "user",
                        "agent_id": null,
                        "content": "hello legacy",
                        "reasoning": null,
                        "tool_calls_json": null,
                        "task_id": null,
                        "token_count": 2,
                        "latency_ms": null,
                        "archived": false,
                        "created_at": "2026-11-13T08:01:00Z"
                    }
                }
            }
        });
        fs::write(dir.0.join("state.json"), v1_state.to_string()).unwrap();

        let storage = JsonlStorage::open(&dir.0).unwrap();
        storage.run_migrations().unwrap();
        assert_eq!(
            storage.lock_inner().unwrap().state.schema_version,
            crate::CURRENT_SCHEMA_VERSION
        );

        let agent = storage.get_agent("a1").unwrap().unwrap();
        assert_eq!(agent.model_config.thinking_level, ThinkingLevel::Medium);

        let msgs = storage.list_chat_messages("conv-1", false, 10).unwrap();
        assert_eq!(msgs.len(), 1);
        assert!(msgs[0].attachments.is_empty());
        assert!(msgs[0].timings.is_none());

        let sample_timings = InferenceTimings {
            predicted_per_second: Some(38.5),
            prompt_per_second: Some(110.0),
            predicted_ms: Some(410.0),
            prompt_ms: Some(90.0),
            predicted_n: Some(16),
            prompt_n: Some(10),
            cache_n: Some(4),
        };
        let m2 = ChatMessage {
            id: "m2".to_string(),
            conversation_id: "conv-1".to_string(),
            role: ChatRole::User,
            agent_id: None,
            content: "with image".to_string(),
            reasoning: None,
            tool_calls_json: None,
            task_id: None,
            token_count: 3,
            latency_ms: None,
            archived: false,
            created_at: "2026-11-13T08:02:00Z".to_string(),
            attachments: vec![ChatAttachment {
                kind: "image".to_string(),
                name: "shot.png".to_string(),
                mime_type: "image/png".to_string(),
                data_url: Some("data:image/png;base64,AA==".to_string()),
                workspace_path: None,
                size_bytes: 2,
            }],
            timings: Some(sample_timings.clone()),
        };
        storage.append_chat_message(&m2).unwrap();
        let msgs2 = storage.list_chat_messages("conv-1", false, 10).unwrap();
        assert_eq!(msgs2.len(), 2);
        assert_eq!(msgs2[1].attachments.len(), 1);
        assert_eq!(msgs2[1].attachments[0].name, "shot.png");
        assert_eq!(msgs2[1].timings, Some(sample_timings));
    }

    #[test]
    fn task_counts_summary_aggregates_statuses_and_per_agent_stats() {
        let dir = TempDir::new("ps5-db-jsonl-summary");
        let storage = JsonlStorage::open(&dir.0).unwrap();
        storage.run_migrations().unwrap();

        storage
            .upsert_task(&make_task("t-pend", "commander", TaskStatus::Pending, "2026-11-13T10:00:00Z"))
            .unwrap();
        storage
            .upsert_task(&make_task("t-asgn", "programmer", TaskStatus::Assigned, "2026-11-13T10:01:00Z"))
            .unwrap();
        storage
            .upsert_task(&make_task("t-run", "programmer", TaskStatus::Running, "2026-11-13T10:03:00Z"))
            .unwrap();
        storage
            .upsert_task(&make_task("t-wait", "researcher", TaskStatus::Waiting, "2026-11-13T10:02:00Z"))
            .unwrap();
        storage
            .upsert_task(&make_task("t-done", "programmer", TaskStatus::Completed, "2026-11-13T10:04:00Z"))
            .unwrap();
        storage
            .upsert_task(&make_task("t-fail", "researcher", TaskStatus::Failed, "2026-11-13T10:05:00Z"))
            .unwrap();
        storage
            .upsert_task(&make_task("t-canc", "commander", TaskStatus::Cancelled, "2026-11-13T10:06:00Z"))
            .unwrap();

        let summary = storage.task_counts_summary().unwrap();
        assert_eq!(summary.total, 7);
        assert_eq!(summary.pending, 1);
        assert_eq!(summary.running, 2); // Assigned + Running
        assert_eq!(summary.waiting, 1);
        assert_eq!(summary.completed, 1);
        assert_eq!(summary.failed, 1);
        assert_eq!(summary.cancelled, 1);

        let prog = summary.per_agent.get("programmer").unwrap();
        assert_eq!(prog.completed, 1);
        assert_eq!(prog.failed, 0);
        assert_eq!(prog.active, 2);
        assert_eq!(prog.latest_active_task_id.as_deref(), Some("t-run"));
        assert_eq!(prog.last_activity.as_deref(), Some("2026-11-13T10:04:00Z"));

        let res = summary.per_agent.get("researcher").unwrap();
        assert_eq!(res.completed, 0);
        assert_eq!(res.failed, 1);
        assert_eq!(res.active, 1);
        assert_eq!(res.latest_active_task_id.as_deref(), Some("t-wait"));
        assert_eq!(res.last_activity.as_deref(), Some("2026-11-13T10:05:00Z"));
    }

    #[test]
    fn prune_old_records_compacts_jsonl_files_and_respects_zero_bounds() {
        let dir = TempDir::new("ps5-db-jsonl-prune");
        let storage = JsonlStorage::open(&dir.0).unwrap();
        storage.run_migrations().unwrap();

        storage
            .create_conversation(&Conversation {
                id: "conv-1".to_string(),
                title: "Chat".to_string(),
                mode: ConversationMode::Direct {
                    agent_id: "commander".to_string(),
                },
                created_at: "2026-11-13T08:00:00Z".to_string(),
                updated_at: "2026-11-13T08:00:00Z".to_string(),
                archived: false,
                sort_order: 0,
            })
            .unwrap();

        // Sessions: 1 expired, 1 revoked, 1 active
        let far_future = (Utc::now() + ChronoDuration::days(7)).to_rfc3339();
        let past = (Utc::now() - ChronoDuration::days(2)).to_rfc3339();
        storage.create_session(&make_session("s-exp", &past)).unwrap();
        storage.create_session(&make_session("s-rev", &far_future)).unwrap();
        storage.revoke_session("s-rev").unwrap();
        storage.create_session(&make_session("s-ok", &far_future)).unwrap();

        // Tasks: 2 old terminal, 1 recent terminal, 1 active running
        let old_ts = (Utc::now() - ChronoDuration::days(45)).to_rfc3339();
        let new_ts = Utc::now().to_rfc3339();
        storage
            .upsert_task(&make_task("t-old-1", "commander", TaskStatus::Completed, &old_ts))
            .unwrap();
        storage
            .upsert_task(&make_task("t-old-2", "commander", TaskStatus::Failed, &old_ts))
            .unwrap();
        storage
            .upsert_task(&make_task("t-new-done", "commander", TaskStatus::Completed, &new_ts))
            .unwrap();
        storage
            .upsert_task(&make_task("t-active", "commander", TaskStatus::Running, &old_ts))
            .unwrap();

        // Task events: 2 for old tasks, 1 for active task
        for (eid, tid, ts) in [
            ("e-old-1", "t-old-1", &old_ts),
            ("e-old-2", "t-old-2", &old_ts),
            ("e-act", "t-active", &new_ts),
        ] {
            storage
                .append_task_event(&TaskEvent {
                    id: eid.to_string(),
                    task_id: tid.to_string(),
                    timestamp: ts.clone(),
                    event_type: "ev".to_string(),
                    details: "d".to_string(),
                })
                .unwrap();
        }

        // Agent messages: 2 old, 1 recent
        for (mid, tid, ts) in [
            ("am-old-1", "t-old-1", &old_ts),
            ("am-old-2", "t-old-2", &old_ts),
            ("am-new", "t-new-done", &new_ts),
        ] {
            storage
                .append_agent_message(&AgentEnvelope {
                    id: mid.to_string(),
                    sender_agent_id: "commander".to_string(),
                    recipient_agent_id: "programmer".to_string(),
                    task_id: Some(tid.to_string()),
                    timestamp: ts.clone(),
                    message_type: AgentMessageType::TaskAssignment,
                    content: "hi".to_string(),
                    status: DeliveryStatus::Delivered,
                    hop_count: 1,
                })
                .unwrap();
        }

        // Tool executions & audit logs: 3 each (2 old, 1 new)
        for (ts, idx) in [(&old_ts, 0), (&old_ts, 1), (&new_ts, 2)] {
            storage
                .record_tool_execution(&ToolExecutionRecord {
                    id: format!("te-{idx}"),
                    tool_id: "file_read".to_string(),
                    agent_id: "programmer".to_string(),
                    task_id: None,
                    input_json: "{}".to_string(),
                    output_json: None,
                    success: true,
                    error: None,
                    duration_ms: 1,
                    created_at: ts.clone(),
                })
                .unwrap();
            storage
                .append_audit_log(&AuditLogEntry {
                    id: format!("au-{idx}"),
                    timestamp: ts.clone(),
                    actor: "admin".to_string(),
                    action: "test".to_string(),
                    target: "t".to_string(),
                    severity: "info".to_string(),
                    outcome: "ok".to_string(),
                    details_json: "{}".to_string(),
                })
                .unwrap();
        }

        // Chat messages: 2 archived (1 old, 1 new) + 1 unarchived old
        for (id, archived, ts) in [
            ("m-arch-old", true, &old_ts),
            ("m-arch-new", true, &new_ts),
            ("m-live-old", false, &old_ts),
        ] {
            storage
                .append_chat_message(&ChatMessage {
                    id: id.to_string(),
                    conversation_id: "conv-1".to_string(),
                    role: ChatRole::User,
                    agent_id: None,
                    content: id.to_string(),
                    reasoning: None,
                    tool_calls_json: None,
                    task_id: None,
                    token_count: 1,
                    latency_ms: None,
                    archived: archived,
                    created_at: ts.clone(),
                    attachments: Vec::new(),
                    timings: None,
                })
                .unwrap();
        }

        // Approvals: 1 old resolved, 1 old pending
        storage
            .upsert_approval(&ApprovalRecord {
                id: "ap-old-res".to_string(),
                task_id: None,
                agent_id: "executor".to_string(),
                operation: "delete_file".to_string(),
                tool_id: None,
                arguments_json: "{}".to_string(),
                reason: "r".to_string(),
                status: ApprovalStatus::Approved,
                created_at: old_ts.clone(),
                resolved_at: Some(old_ts.clone()),
                resolved_by: Some("admin".to_string()),
            })
            .unwrap();
        storage
            .upsert_approval(&ApprovalRecord {
                id: "ap-pend".to_string(),
                task_id: None,
                agent_id: "executor".to_string(),
                operation: "delete_file".to_string(),
                tool_id: None,
                arguments_json: "{}".to_string(),
                reason: "r".to_string(),
                status: ApprovalStatus::Pending,
                created_at: old_ts.clone(),
                resolved_at: None,
                resolved_by: None,
            })
            .unwrap();

        // First prune by retention_days = 30, max_rows_per_table = 0 (skip row cap)
        let report = storage.prune_old_records(30, 0).unwrap();
        assert_eq!(report.purged_sessions, 2);
        assert_eq!(report.deleted_tasks, 2);
        assert_eq!(report.deleted_task_events, 2);
        assert_eq!(report.deleted_agent_messages, 2);
        assert_eq!(report.deleted_tool_executions, 2);
        assert_eq!(report.deleted_audit_logs, 2);
        assert_eq!(report.deleted_chat_messages, 1);

        // Pending approval and active task and unarchived chat message remain!
        assert!(storage.get_task("t-active").unwrap().is_some());
        assert!(storage.get_task("t-new-done").unwrap().is_some());
        assert!(storage.get_approval("ap-pend").unwrap().is_some());
        assert!(storage.get_approval("ap-old-res").unwrap().is_none());
        assert_eq!(storage.list_chat_messages("conv-1", true, 10).unwrap().len(), 2);

        // Verify .jsonl files were compacted on disk
        let count_lines = |name: &str| -> usize {
            fs::read_to_string(dir.0.join(name))
                .unwrap()
                .lines()
                .filter(|l| !l.trim().is_empty())
                .count()
        };
        assert_eq!(count_lines("task_events.jsonl"), 1);
        assert_eq!(count_lines("agent_messages.jsonl"), 1);
        assert_eq!(count_lines("tool_executions.jsonl"), 1);
        assert_eq!(count_lines("audit_logs.jsonl"), 1);
        assert_eq!(count_lines("messages.jsonl"), 2);
        assert_eq!(storage.integrity_check().unwrap(), "ok");

        // Now prune with retention_days = 0, max_rows_per_table = 1 (drops remaining terminal task + archived message)
        let report2 = storage.prune_old_records(0, 1).unwrap();
        assert_eq!(report2.deleted_tasks, 1); // t-new-done removed, t-active preserved
        assert_eq!(report2.deleted_chat_messages, 1); // m-arch-new removed, m-live-old preserved
        assert!(storage.get_task("t-active").unwrap().is_some());
        assert_eq!(count_lines("messages.jsonl"), 1);
        assert_eq!(storage.integrity_check().unwrap(), "ok");

        // Reopen from disk to confirm state.json matches compacted RAM state
        drop(storage);
        let reopened = JsonlStorage::open(&dir.0).unwrap();
        assert_eq!(reopened.list_tasks(10).unwrap().len(), 1);
        assert_eq!(reopened.list_audit_logs(10).unwrap().len(), 1);
        assert_eq!(reopened.list_chat_messages("conv-1", true, 10).unwrap().len(), 1);
    }

    #[test]
    fn debounced_task_updates_flush_on_status_change_and_checkpoint() {
        let dir = TempDir::new("ps5-db-jsonl-debounce");
        let storage = JsonlStorage::open(&dir.0).unwrap();
        storage.run_migrations().unwrap();

        let mut task = make_task("t-1", "programmer", TaskStatus::Running, "2026-11-13T10:00:00Z");
        storage.upsert_task(&task).unwrap();
        // Identical upsert is a no-op
        storage.upsert_task(&task).unwrap();

        // Rapid progress update within debounce window updates RAM without failing
        task.progress_pct = 50;
        task.progress_note = Some("halfway".to_string());
        storage.upsert_task(&task).unwrap();
        assert_eq!(storage.get_task("t-1").unwrap().unwrap().progress_pct, 50);

        // Checkpoint flushes dirty state to disk; second checkpoint is a no-op
        storage.checkpoint().unwrap();
        storage.checkpoint().unwrap();

        // Reopen verifies flushed progress
        drop(storage);
        let reopened = JsonlStorage::open(&dir.0).unwrap();
        let loaded = reopened.get_task("t-1").unwrap().unwrap();
        assert_eq!(loaded.progress_pct, 50);
        assert_eq!(loaded.progress_note.as_deref(), Some("halfway"));
    }

    #[test]
    fn reorder_conversations_updates_sort_order_and_persists() {
        let dir = TempDir::new("ps5-db-jsonl-reorder");
        let storage = JsonlStorage::open(&dir.0).unwrap();
        storage.run_migrations().unwrap();

        for (id, updated_at) in [
            ("conv-a", "2026-11-13T08:00:00Z"),
            ("conv-b", "2026-11-13T09:00:00Z"),
            ("conv-c", "2026-11-13T10:00:00Z"),
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

        // Before reorder, all have sort_order=0 so newest updated_at comes first
        let initial_ids: Vec<String> = storage
            .list_conversations(false)
            .unwrap()
            .into_iter()
            .map(|c| c.id)
            .collect();
        assert_eq!(initial_ids, vec!["conv-c", "conv-b", "conv-a"]);

        storage
            .reorder_conversations(&["conv-a".into(), "conv-c".into(), "conv-b".into()])
            .unwrap();

        let reordered_ids: Vec<String> = storage
            .list_conversations(false)
            .unwrap()
            .into_iter()
            .map(|c| c.id)
            .collect();
        assert_eq!(reordered_ids, vec!["conv-a", "conv-c", "conv-b"]);
        assert_eq!(storage.get_conversation("conv-a").unwrap().unwrap().sort_order, 10);
        assert_eq!(storage.get_conversation("conv-c").unwrap().unwrap().sort_order, 20);
        assert_eq!(storage.get_conversation("conv-b").unwrap().unwrap().sort_order, 30);

        drop(storage);
        let reopened = JsonlStorage::open(&dir.0).unwrap();
        let persisted_ids: Vec<String> = reopened
            .list_conversations(false)
            .unwrap()
            .into_iter()
            .map(|c| c.id)
            .collect();
        assert_eq!(persisted_ids, vec!["conv-a", "conv-c", "conv-b"]);
    }

    fn make_schedule(
        id: &str,
        title: &str,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
    ) -> ScheduleRecord {
        ScheduleRecord {
            id: id.to_string(),
            title: title.to_string(),
            prompt: "run check".to_string(),
            target_agent_id: Some("commander".to_string()),
            conversation_id: None,
            kind: ScheduleKind::Interval,
            run_at: None,
            interval_secs: Some(300),
            daily_time: None,
            cron_expr: None,
            timezone_offset_mins: 0,
            enabled: true,
            next_run_at: Some(updated_at + ChronoDuration::seconds(300)),
            last_run_at: None,
            last_status: None,
            last_error: None,
            last_task_id: None,
            run_count: 0,
            created_at,
            updated_at,
        }
    }

    #[test]
    fn schedule_crud_ordering_compaction_and_jsonl_replay() {
        let dir = TempDir::new("ps5-db-jsonl-schedules");
        let storage = JsonlStorage::open(&dir.0).unwrap();
        storage.run_migrations().unwrap();

        let t1 = parse_rfc3339_utc("2026-11-13T08:00:00Z").unwrap();
        let t2 = parse_rfc3339_utc("2026-11-13T09:00:00Z").unwrap();
        let t3 = parse_rfc3339_utc("2026-11-13T10:00:00Z").unwrap();

        let s_b = make_schedule("sched-b", "B", t1, t1);
        let s_a = make_schedule("sched-a", "A", t1, t1);
        let s_c = make_schedule("sched-c", "C", t2, t2);

        storage.upsert_schedule(&s_b).unwrap();
        storage.upsert_schedule(&s_a).unwrap();
        storage.upsert_schedule(&s_c).unwrap();

        assert_eq!(storage.get_schedule("sched-a").unwrap().as_ref(), Some(&s_a));
        assert!(storage.get_schedule("missing").unwrap().is_none());

        // Ordering: created_at DESC, id ASC; limit == 0 returns all rows
        let all_ids: Vec<String> = storage
            .list_schedules(0)
            .unwrap()
            .into_iter()
            .map(|s| s.id)
            .collect();
        assert_eq!(all_ids, vec!["sched-c", "sched-a", "sched-b"]);

        let limited_ids: Vec<String> = storage
            .list_schedules(2)
            .unwrap()
            .into_iter()
            .map(|s| s.id)
            .collect();
        assert_eq!(limited_ids, vec!["sched-c", "sched-a"]);

        // Update existing schedule and confirm schedules.jsonl compacts instead of growing
        let mut s_a_updated = s_a.clone();
        s_a_updated.title = "Updated A".to_string();
        s_a_updated.run_count = 3;
        s_a_updated.last_status = Some("ok".to_string());
        s_a_updated.updated_at = t2;
        storage.upsert_schedule(&s_a_updated).unwrap();

        let loaded_a = storage.get_schedule("sched-a").unwrap().unwrap();
        assert_eq!(loaded_a.title, "Updated A");
        assert_eq!(loaded_a.run_count, 3);

        let schedules_jsonl = fs::read_to_string(dir.0.join("schedules.jsonl")).unwrap();
        assert_eq!(
            schedules_jsonl
                .lines()
                .filter(|l| !l.trim().is_empty())
                .count(),
            3
        );

        // Delete existing and non-existent schedule
        storage.delete_schedule("missing").unwrap();
        storage.delete_schedule("sched-b").unwrap();
        assert!(storage.get_schedule("sched-b").unwrap().is_none());

        let remaining_ids: Vec<String> = storage
            .list_schedules(0)
            .unwrap()
            .into_iter()
            .map(|s| s.id)
            .collect();
        assert_eq!(remaining_ids, vec!["sched-c", "sched-a"]);

        let schedules_jsonl = fs::read_to_string(dir.0.join("schedules.jsonl")).unwrap();
        assert_eq!(
            schedules_jsonl
                .lines()
                .filter(|l| !l.trim().is_empty())
                .count(),
            2
        );

        // Reopen verifies state.json + schedules.jsonl persistence
        drop(storage);
        let reopened = JsonlStorage::open(&dir.0).unwrap();
        assert_eq!(reopened.get_schedule("sched-a").unwrap().unwrap().title, "Updated A");
        assert_eq!(
            reopened
                .list_schedules(0)
                .unwrap()
                .into_iter()
                .map(|s| s.id)
                .collect::<Vec<_>>(),
            vec!["sched-c", "sched-a"]
        );

        // Replay recovery from schedules.jsonl when state.json is absent + duplicate lines compact
        drop(reopened);
        fs::remove_file(dir.0.join("state.json")).unwrap();
        let mut s_a_replayed = s_a_updated.clone();
        s_a_replayed.title = "Replayed A".to_string();
        s_a_replayed.run_count = 4;
        s_a_replayed.updated_at = t3;
        append_jsonl_record(&dir.0, "schedules.jsonl", &s_a_replayed).unwrap();

        let recovered = JsonlStorage::open(&dir.0).unwrap();
        assert_eq!(recovered.integrity_check().unwrap(), "ok");
        let recovered_a = recovered.get_schedule("sched-a").unwrap().unwrap();
        assert_eq!(recovered_a.title, "Replayed A");
        assert_eq!(recovered_a.run_count, 4);
        assert!(recovered.get_schedule("sched-c").unwrap().is_some());
        assert!(recovered.get_schedule("sched-b").unwrap().is_none());

        let compacted_jsonl = fs::read_to_string(dir.0.join("schedules.jsonl")).unwrap();
        assert_eq!(
            compacted_jsonl
                .lines()
                .filter(|l| !l.trim().is_empty())
                .count(),
            2
        );
    }

    #[test]
    fn list_due_schedules_filters_sorts_and_caps_results() {
        let dir = TempDir::new("ps5-db-jsonl-due-schedules");
        let storage = JsonlStorage::open(&dir.0).unwrap();
        storage.run_migrations().unwrap();

        let t0 = parse_rfc3339_utc("2026-11-13T08:00:00Z").unwrap();
        let t1 = parse_rfc3339_utc("2026-11-13T09:00:00Z").unwrap();
        let now = parse_rfc3339_utc("2026-11-13T10:00:00Z").unwrap();
        let future = parse_rfc3339_utc("2026-11-13T11:00:00Z").unwrap();

        let mut s_due_later_b = make_schedule("due-b", "due-b", t0, t0);
        s_due_later_b.next_run_at = Some(t1);

        let mut s_due_later_a = make_schedule("due-a", "due-a", t0, t0);
        s_due_later_a.next_run_at = Some(t1);

        let mut s_due_earliest = make_schedule("due-0", "due-0", t0, t0);
        s_due_earliest.next_run_at = Some(t0);

        let mut s_due_exact_now = make_schedule("due-now", "due-now", t0, t0);
        s_due_exact_now.next_run_at = Some(now);

        let mut s_disabled = make_schedule("disabled", "disabled", t0, t0);
        s_disabled.enabled = false;
        s_disabled.next_run_at = Some(t0);

        let mut s_none_next = make_schedule("none-next", "none-next", t0, t0);
        s_none_next.next_run_at = None;

        let mut s_future = make_schedule("future", "future", t0, t0);
        s_future.next_run_at = Some(future);

        for s in [
            &s_due_later_b,
            &s_due_later_a,
            &s_due_earliest,
            &s_due_exact_now,
            &s_disabled,
            &s_none_next,
            &s_future,
        ] {
            storage.upsert_schedule(s).unwrap();
        }

        let due_all: Vec<String> = storage
            .list_due_schedules(now, 0)
            .unwrap()
            .into_iter()
            .map(|s| s.id)
            .collect();
        assert_eq!(due_all, vec!["due-0", "due-a", "due-b", "due-now"]);

        let due_limited: Vec<String> = storage
            .list_due_schedules(now, 2)
            .unwrap()
            .into_iter()
            .map(|s| s.id)
            .collect();
        assert_eq!(due_limited, vec!["due-0", "due-a"]);
    }

    fn make_webhook(
        id: &str,
        name: &str,
        created_at: &str,
        updated_at: &str,
    ) -> WebhookRecord {
        WebhookRecord {
            id: id.to_string(),
            name: name.to_string(),
            enabled: true,
            secret_hash: format!("sha256-{id}"),
            secret_prefix: "whsec_".to_string(),
            target_agent_id: "commander".to_string(),
            conversation_id: None,
            prompt_template: "Handle {{event}}".to_string(),
            created_at: created_at.to_string(),
            updated_at: updated_at.to_string(),
            last_triggered_at: None,
            trigger_count: 0,
        }
    }

    #[test]
    fn webhook_crud_compaction_and_jsonl_replay() {
        let dir = TempDir::new("ps5-db-jsonl-webhooks");
        let storage = JsonlStorage::open(&dir.0).unwrap();
        storage.run_migrations().unwrap();

        let t1 = "2026-11-13T08:00:00Z";
        let t2 = "2026-11-13T09:00:00Z";
        let t3 = "2026-11-13T10:00:00Z";

        let w_b = make_webhook("wh-b", "B", t1, t1);
        let w_a = make_webhook("wh-a", "A", t1, t1);
        let w_c = make_webhook("wh-c", "C", t2, t2);

        storage.upsert_webhook(&w_b).unwrap();
        storage.upsert_webhook(&w_a).unwrap();
        storage.upsert_webhook(&w_c).unwrap();

        assert_eq!(storage.get_webhook("wh-a").unwrap().as_ref(), Some(&w_a));
        assert!(storage.get_webhook("missing").unwrap().is_none());

        let ids: Vec<String> = storage
            .list_webhooks()
            .unwrap()
            .into_iter()
            .map(|w| w.id)
            .collect();
        assert_eq!(ids, vec!["wh-c", "wh-a", "wh-b"]);

        let mut w_a_updated = w_a.clone();
        w_a_updated.name = "Updated A".to_string();
        w_a_updated.trigger_count = 5;
        w_a_updated.updated_at = t2.to_string();
        storage.upsert_webhook(&w_a_updated).unwrap();

        let webhooks_jsonl = fs::read_to_string(dir.0.join("webhooks.jsonl")).unwrap();
        assert_eq!(
            webhooks_jsonl
                .lines()
                .filter(|l| !l.trim().is_empty())
                .count(),
            3
        );

        assert!(!storage.delete_webhook("missing").unwrap());
        assert!(storage.delete_webhook("wh-b").unwrap());
        assert!(storage.get_webhook("wh-b").unwrap().is_none());

        drop(storage);
        let reopened = JsonlStorage::open(&dir.0).unwrap();
        assert_eq!(reopened.get_webhook("wh-a").unwrap().unwrap().name, "Updated A");

        drop(reopened);
        fs::remove_file(dir.0.join("state.json")).unwrap();
        let mut w_a_replayed = w_a_updated.clone();
        w_a_replayed.name = "Replayed A".to_string();
        w_a_replayed.trigger_count = 6;
        w_a_replayed.updated_at = t3.to_string();
        append_jsonl_record(&dir.0, "webhooks.jsonl", &w_a_replayed).unwrap();

        let recovered = JsonlStorage::open(&dir.0).unwrap();
        assert_eq!(recovered.integrity_check().unwrap(), "ok");
        let loaded = recovered.get_webhook("wh-a").unwrap().unwrap();
        assert_eq!(loaded.name, "Replayed A");
        assert_eq!(loaded.trigger_count, 6);
        assert!(recovered.get_webhook("wh-c").unwrap().is_some());
        assert!(recovered.get_webhook("wh-b").unwrap().is_none());

        let compacted_jsonl = fs::read_to_string(dir.0.join("webhooks.jsonl")).unwrap();
        assert_eq!(
            compacted_jsonl
                .lines()
                .filter(|l| !l.trim().is_empty())
                .count(),
            2
        );
    }

    #[test]
    fn jsonl_resilience_fixes_for_audit_391() {
        let dir = TempDir::new("ps5-db-jsonl-audit391");
        let storage = JsonlStorage::open(&dir.0).unwrap();
        storage.run_migrations().unwrap();

        storage
            .create_conversation(&Conversation {
                id: "conv-1".to_string(),
                title: "Chat".to_string(),
                mode: ConversationMode::Direct {
                    agent_id: "commander".to_string(),
                },
                created_at: "2026-11-13T08:00:00Z".to_string(),
                updated_at: "2026-11-13T08:00:00Z".to_string(),
                archived: false,
                sort_order: 0,
            })
            .unwrap();

        storage
            .append_chat_message(&ChatMessage {
                id: "m-trim".to_string(),
                conversation_id: "conv-1".to_string(),
                role: ChatRole::User,
                agent_id: None,
                content: "Find Needle here".to_string(),
                reasoning: None,
                tool_calls_json: None,
                task_id: None,
                token_count: 3,
                latency_ms: None,
                archived: false,
                created_at: "2026-11-13T10:00:00Z".to_string(),
                attachments: Vec::new(),
                timings: None,
            })
            .unwrap();
        let hits = storage.search_chat_messages("  needle  ", 10).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, "m-trim");

        let old_started = (Utc::now() - ChronoDuration::days(45)).to_rfc3339();
        let recent_created = (Utc::now() - ChronoDuration::days(1)).to_rfc3339();
        let mut t_fallback = make_task("t-started-fallback", "commander", TaskStatus::Completed, &recent_created);
        t_fallback.started_at = Some(old_started);
        t_fallback.completed_at = None;
        storage.upsert_task(&t_fallback).unwrap();

        let noop_prune = storage.prune_old_records(u32::MAX, 0).unwrap();
        assert_eq!(noop_prune.deleted_tasks, 0);

        let aged_prune = storage.prune_old_records(30, 0).unwrap();
        assert_eq!(aged_prune.deleted_tasks, 1);
        assert!(storage.get_task("t-started-fallback").unwrap().is_none());

        // Truncated trailing partial line in schedules.jsonl is tolerated and compacted cleanly
        let t0 = parse_rfc3339_utc("2026-11-13T08:00:00Z").unwrap();
        let s_ok = make_schedule("sched-ok", "OK", t0, t0);
        storage.upsert_schedule(&s_ok).unwrap();
        drop(storage);

        {
            let mut f = OpenOptions::new()
                .append(true)
                .open(dir.0.join("schedules.jsonl"))
                .unwrap();
            f.write_all(b"{\"id\":\"partial-tail\",\"title\":").unwrap();
        }
        let recovered = JsonlStorage::open(&dir.0).unwrap();
        assert_eq!(recovered.integrity_check().unwrap(), "ok");
        assert_eq!(recovered.get_schedule("sched-ok").unwrap().unwrap().title, "OK");
    }
}
