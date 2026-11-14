use ps5_agent_core::{
    new_id, now_iso, AgentEnvelope, AgentError, AgentMessageType, AuditLogEntry, DeliveryStatus,
    Result, RuntimeLimits, StorageBackend, StreamEvent,
};
use ps5_agent_logging::{log_info, log_warn};
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc};

/// Maximum A2A delegation/forwarding hops allowed before loop protection triggers.
pub const MAX_A2A_HOP_COUNT: u32 = 6;

/// Maximum identical (sender, recipient, message_type) occurrences allowed within a single task.
pub const MAX_IDENTICAL_TASK_MESSAGES: usize = 3;

/// Bounded per-agent inbox channel capacity to prevent unbounded RAM growth when a receiver stalls.
pub const AGENT_INBOX_CAPACITY: usize = 256;

/// Sliding window (seconds) enforced for unscoped (`task_id: None`) sender rate and loop checks.
pub const UNSCOPED_RATE_WINDOW_SECS: u64 = 60;

/// Default per-sender rate limit per window when `task_id` is `None` and `max_messages_per_task == 0`.
pub const DEFAULT_UNSCOPED_MAX_PER_SENDER: usize = 20;

/// Upper cap for unscoped per-sender rate limits derived from `RuntimeLimits::max_messages_per_task`.
const MAX_UNSCOPED_SENDER_CAP: usize = 128;

/// Maximum `Rejected` / `BlockedLoop` envelopes persisted per `task_id`.
pub const MAX_STORED_DENIED_PER_TASK: usize = 8;

/// Maximum `Rejected` / `BlockedLoop` envelopes persisted per `(sender, recipient)` pair when `task_id` is `None`.
pub const MAX_STORED_DENIED_UNSCOPED_PAIR: usize = 8;

/// Global maximum `Rejected` / `BlockedLoop` envelopes persisted when `task_id` is `None`.
pub const MAX_STORED_DENIED_UNSCOPED_TOTAL: usize = 32;

/// Upper bound on rows scanned per task for active-message accounting and loop checks.
const MAX_TASK_SCAN_LIMIT: usize = 65_536;

/// Upper bound on rows scanned when seeding unscoped denial counters from storage.
const MAX_UNSCOPED_SEED_SCAN: usize = 1024;

/// Upper bound on tracked scope/rate keys retained in memory.
const MAX_TRACKED_GUARD_ENTRIES: usize = 4096;

pub fn message_type_str(t: AgentMessageType) -> &'static str {
    match t {
        AgentMessageType::TaskAssignment => "TASK_ASSIGNMENT",
        AgentMessageType::TaskUpdate => "TASK_UPDATE",
        AgentMessageType::TaskCompleted => "TASK_COMPLETED",
        AgentMessageType::InformationRequest => "INFORMATION_REQUEST",
        AgentMessageType::InformationResponse => "INFORMATION_RESPONSE",
        AgentMessageType::AgentMessage => "AGENT_MESSAGE",
        AgentMessageType::ErrorReport => "ERROR_REPORT",
        AgentMessageType::ApprovalRequest => "APPROVAL_REQUEST",
        AgentMessageType::ApprovalResponse => "APPROVAL_RESPONSE",
    }
}

pub fn delivery_status_str(s: DeliveryStatus) -> &'static str {
    match s {
        DeliveryStatus::Queued => "QUEUED",
        DeliveryStatus::Delivered => "DELIVERED",
        DeliveryStatus::Processed => "PROCESSED",
        DeliveryStatus::Rejected => "REJECTED",
        DeliveryStatus::BlockedLoop => "BLOCKED_LOOP",
    }
}

pub fn parse_message_type(raw: &str) -> Result<AgentMessageType> {
    match raw.trim().to_ascii_uppercase().as_str() {
        "TASK_ASSIGNMENT" => Ok(AgentMessageType::TaskAssignment),
        "TASK_UPDATE" => Ok(AgentMessageType::TaskUpdate),
        "TASK_COMPLETED" => Ok(AgentMessageType::TaskCompleted),
        "INFORMATION_REQUEST" => Ok(AgentMessageType::InformationRequest),
        "INFORMATION_RESPONSE" => Ok(AgentMessageType::InformationResponse),
        "AGENT_MESSAGE" => Ok(AgentMessageType::AgentMessage),
        "ERROR_REPORT" => Ok(AgentMessageType::ErrorReport),
        "APPROVAL_REQUEST" => Ok(AgentMessageType::ApprovalRequest),
        "APPROVAL_RESPONSE" => Ok(AgentMessageType::ApprovalResponse),
        other => Err(AgentError::InvalidInput(format!(
            "unknown agent message type: '{other}'"
        ))),
    }
}

#[derive(Debug, Clone)]
pub struct SendMessageRequest {
    pub sender_agent_id: String,
    pub recipient_agent_id: String,
    pub task_id: Option<String>,
    pub message_type: AgentMessageType,
    pub content: String,
    pub hop_count: u32,
}

#[derive(Debug, Default, Clone, Copy)]
struct DeniedCounter {
    persisted: usize,
    suppression_logged: bool,
    last_seq: u64,
}

#[derive(Debug, Default)]
struct WindowEntry {
    timestamps: VecDeque<Instant>,
    last_seq: u64,
}

#[derive(Debug, Default)]
struct BusGuardState {
    seq: u64,
    per_task_denied: HashMap<String, DeniedCounter>,
    unscoped_seeded: bool,
    unscoped_total_denied: usize,
    unscoped_total_suppression_logged: bool,
    unscoped_pair_denied: HashMap<(String, String), DeniedCounter>,
    unscoped_sender_history: HashMap<String, WindowEntry>,
    unscoped_tuple_history: HashMap<(String, String, &'static str), WindowEntry>,
}

enum DeniedPersistence {
    Persist,
    Suppress { emit_cap_notice: bool },
}

fn lock_mutex<'a, T>(mutex: &'a Mutex<T>, label: &str) -> MutexGuard<'a, T> {
    mutex.lock().unwrap_or_else(|poisoned| {
        log_warn("messaging", format!("recovering poisoned mutex '{label}'"));
        poisoned.into_inner()
    })
}

fn read_rw<'a, T>(rw: &'a RwLock<T>, label: &str) -> RwLockReadGuard<'a, T> {
    rw.read().unwrap_or_else(|poisoned| {
        log_warn(
            "messaging",
            format!("recovering poisoned rwlock read '{label}'"),
        );
        poisoned.into_inner()
    })
}

fn write_rw<'a, T>(rw: &'a RwLock<T>, label: &str) -> RwLockWriteGuard<'a, T> {
    rw.write().unwrap_or_else(|poisoned| {
        log_warn(
            "messaging",
            format!("recovering poisoned rwlock write '{label}'"),
        );
        poisoned.into_inner()
    })
}

fn is_non_counting_status(status: DeliveryStatus) -> bool {
    matches!(status, DeliveryStatus::Rejected | DeliveryStatus::BlockedLoop)
}

fn shrink_map_if_sparse<K: Eq + Hash, V>(map: &mut HashMap<K, V>) {
    if map.capacity() > 16 && map.len() * 4 < map.capacity() {
        map.shrink_to_fit();
    }
}

fn evict_oldest_if_full<K, V, F>(map: &mut HashMap<K, V>, max_entries: usize, seq_of: F)
where
    K: Clone + Eq + Hash,
    F: Fn(&V) -> u64,
{
    if map.len() < max_entries {
        return;
    }
    if let Some(oldest_key) = map
        .iter()
        .min_by_key(|(_, value)| seq_of(value))
        .map(|(key, _)| key.clone())
    {
        map.remove(&oldest_key);
    }
}

fn prune_window(timestamps: &mut VecDeque<Instant>, now: Instant, window: Duration) {
    while let Some(&front) = timestamps.front() {
        if now.saturating_duration_since(front) >= window {
            timestamps.pop_front();
        } else {
            break;
        }
    }
}

fn sweep_inboxes_guard(guard: &mut HashMap<String, mpsc::Sender<AgentEnvelope>>) -> usize {
    let before = guard.len();
    guard.retain(|_, tx| !tx.is_closed());
    shrink_map_if_sparse(guard);
    before.saturating_sub(guard.len())
}

fn unscoped_sender_limit(limits: &RuntimeLimits) -> usize {
    if limits.max_messages_per_task == 0 {
        DEFAULT_UNSCOPED_MAX_PER_SENDER
    } else {
        (limits.max_messages_per_task as usize).clamp(1, MAX_UNSCOPED_SENDER_CAP)
    }
}

impl BusGuardState {
    fn next_seq(&mut self) -> u64 {
        self.seq = self.seq.saturating_add(1);
        self.seq
    }

    fn check_unscoped_guard(
        &mut self,
        req: &SendMessageRequest,
        limits: &RuntimeLimits,
        now: Instant,
    ) -> Option<String> {
        let window = Duration::from_secs(UNSCOPED_RATE_WINDOW_SECS);
        let type_name = message_type_str(req.message_type);
        let tuple_key = (
            req.sender_agent_id.clone(),
            req.recipient_agent_id.clone(),
            type_name,
        );

        let tuple_len = if let Some(entry) = self.unscoped_tuple_history.get_mut(&tuple_key) {
            prune_window(&mut entry.timestamps, now, window);
            entry.timestamps.len()
        } else {
            0
        };
        if tuple_len == 0 {
            self.unscoped_tuple_history.remove(&tuple_key);
        }
        if tuple_len >= MAX_IDENTICAL_TASK_MESSAGES {
            return Some(format!(
                "ping-pong cycle detected without task_id: {} -> {} ({type_name}) repeated > {MAX_IDENTICAL_TASK_MESSAGES} times within {UNSCOPED_RATE_WINDOW_SECS}s",
                req.sender_agent_id, req.recipient_agent_id
            ));
        }

        let sender_cap = unscoped_sender_limit(limits);
        let sender_len = if let Some(entry) = self.unscoped_sender_history.get_mut(&req.sender_agent_id) {
            prune_window(&mut entry.timestamps, now, window);
            entry.timestamps.len()
        } else {
            0
        };
        if sender_len == 0 {
            self.unscoped_sender_history.remove(&req.sender_agent_id);
        }
        if sender_len >= sender_cap {
            return Some(format!(
                "unscoped sender rate limit exceeded: sender '{}' reached {sender_cap} messages within {UNSCOPED_RATE_WINDOW_SECS}s",
                req.sender_agent_id
            ));
        }

        None
    }

    fn record_unscoped_send(&mut self, req: &SendMessageRequest, now: Instant) {
        let seq = self.next_seq();
        let window = Duration::from_secs(UNSCOPED_RATE_WINDOW_SECS);

        if self.unscoped_sender_history.len() > 128 {
            self.unscoped_sender_history.retain(|_, entry| {
                prune_window(&mut entry.timestamps, now, window);
                !entry.timestamps.is_empty()
            });
        }
        if self.unscoped_tuple_history.len() > 128 {
            self.unscoped_tuple_history.retain(|_, entry| {
                prune_window(&mut entry.timestamps, now, window);
                !entry.timestamps.is_empty()
            });
        }

        if !self
            .unscoped_sender_history
            .contains_key(&req.sender_agent_id)
        {
            evict_oldest_if_full(
                &mut self.unscoped_sender_history,
                MAX_TRACKED_GUARD_ENTRIES,
                |entry| entry.last_seq,
            );
        }
        let sender_entry = self
            .unscoped_sender_history
            .entry(req.sender_agent_id.clone())
            .or_default();
        prune_window(&mut sender_entry.timestamps, now, window);
        sender_entry.timestamps.push_back(now);
        sender_entry.last_seq = seq;

        let tuple_key = (
            req.sender_agent_id.clone(),
            req.recipient_agent_id.clone(),
            message_type_str(req.message_type),
        );
        if !self.unscoped_tuple_history.contains_key(&tuple_key) {
            evict_oldest_if_full(
                &mut self.unscoped_tuple_history,
                MAX_TRACKED_GUARD_ENTRIES,
                |entry| entry.last_seq,
            );
        }
        let tuple_entry = self.unscoped_tuple_history.entry(tuple_key).or_default();
        prune_window(&mut tuple_entry.timestamps, now, window);
        tuple_entry.timestamps.push_back(now);
        tuple_entry.last_seq = seq;
    }

    fn reserve_task_denied_slot(&mut self, task_id: &str) -> DeniedPersistence {
        let seq = self.next_seq();
        if !self.per_task_denied.contains_key(task_id) {
            evict_oldest_if_full(
                &mut self.per_task_denied,
                MAX_TRACKED_GUARD_ENTRIES,
                |counter| counter.last_seq,
            );
        }
        let entry = self.per_task_denied.entry(task_id.to_string()).or_default();
        entry.last_seq = seq;
        if entry.persisted < MAX_STORED_DENIED_PER_TASK {
            entry.persisted += 1;
            DeniedPersistence::Persist
        } else {
            let emit_cap_notice = !entry.suppression_logged;
            entry.suppression_logged = true;
            DeniedPersistence::Suppress { emit_cap_notice }
        }
    }

    fn reserve_unscoped_denied_slot(
        &mut self,
        sender_agent_id: &str,
        recipient_agent_id: &str,
    ) -> DeniedPersistence {
        let seq = self.next_seq();
        if self.unscoped_total_denied >= MAX_STORED_DENIED_UNSCOPED_TOTAL {
            let emit_cap_notice = !self.unscoped_total_suppression_logged;
            self.unscoped_total_suppression_logged = true;
            return DeniedPersistence::Suppress { emit_cap_notice };
        }

        let pair_key = (
            sender_agent_id.to_string(),
            recipient_agent_id.to_string(),
        );
        if !self.unscoped_pair_denied.contains_key(&pair_key) {
            evict_oldest_if_full(
                &mut self.unscoped_pair_denied,
                MAX_TRACKED_GUARD_ENTRIES,
                |counter| counter.last_seq,
            );
        }
        let pair_entry = self.unscoped_pair_denied.entry(pair_key).or_default();
        pair_entry.last_seq = seq;
        if pair_entry.persisted < MAX_STORED_DENIED_UNSCOPED_PAIR {
            pair_entry.persisted += 1;
            self.unscoped_total_denied += 1;
            DeniedPersistence::Persist
        } else {
            let emit_cap_notice = !pair_entry.suppression_logged;
            pair_entry.suppression_logged = true;
            DeniedPersistence::Suppress { emit_cap_notice }
        }
    }
}

pub struct MessageBus {
    storage: Arc<dyn StorageBackend>,
    limits: RwLock<RuntimeLimits>,
    events_tx: broadcast::Sender<StreamEvent>,
    inboxes: RwLock<HashMap<String, mpsc::Sender<AgentEnvelope>>>,
    send_gate: Mutex<()>,
    guard_state: Mutex<BusGuardState>,
}

impl MessageBus {
    pub fn new(
        storage: Arc<dyn StorageBackend>,
        limits: RuntimeLimits,
        events_tx: broadcast::Sender<StreamEvent>,
    ) -> Self {
        Self {
            storage,
            limits: RwLock::new(limits),
            events_tx,
            inboxes: RwLock::new(HashMap::new()),
            send_gate: Mutex::new(()),
            guard_state: Mutex::new(BusGuardState::default()),
        }
    }

    pub fn update_limits(&self, limits: RuntimeLimits) {
        let mut guard = write_rw(&self.limits, "limits");
        *guard = limits;
    }

    pub fn limits(&self) -> RuntimeLimits {
        read_rw(&self.limits, "limits").clone()
    }

    pub fn subscribe_agent(&self, agent_id: &str) -> mpsc::Receiver<AgentEnvelope> {
        let (tx, rx) = mpsc::channel(AGENT_INBOX_CAPACITY);
        let mut guard = write_rw(&self.inboxes, "inboxes");
        sweep_inboxes_guard(&mut guard);
        guard.insert(agent_id.to_string(), tx);
        rx
    }

    pub async fn unsubscribe_agent(&self, agent_id: &str) {
        let mut guard = write_rw(&self.inboxes, "inboxes");
        guard.remove(agent_id);
        sweep_inboxes_guard(&mut guard);
    }

    /// Removes closed/dropped agent inbox channels and shrinks the map if sparse.
    pub fn sweep_closed_inboxes(&self) -> usize {
        let mut guard = write_rw(&self.inboxes, "inboxes");
        sweep_inboxes_guard(&mut guard)
    }

    pub fn send(&self, req: SendMessageRequest) -> Result<AgentEnvelope> {
        let send_gate = lock_mutex(&self.send_gate, "send_gate");

        let sender = self
            .storage
            .get_agent(&req.sender_agent_id)?
            .ok_or_else(|| {
                AgentError::NotFound(format!("sender agent '{}' not found", req.sender_agent_id))
            })?;

        if !sender.enabled {
            return self.record_rejected(
                &req,
                AgentError::PermissionDenied(format!(
                    "sender agent '{}' is disabled",
                    req.sender_agent_id
                )),
            );
        }

        if !sender.can_communicate {
            return self.record_rejected(
                &req,
                AgentError::PermissionDenied(format!(
                    "sender agent '{}' has can_communicate=false",
                    req.sender_agent_id
                )),
            );
        }

        let recipient = self
            .storage
            .get_agent(&req.recipient_agent_id)?
            .ok_or_else(|| {
                AgentError::NotFound(format!(
                    "recipient agent '{}' not found",
                    req.recipient_agent_id
                ))
            })?;

        if !recipient.enabled {
            return self.record_rejected(
                &req,
                AgentError::PermissionDenied(format!(
                    "recipient agent '{}' is disabled",
                    req.recipient_agent_id
                )),
            );
        }

        if req.hop_count > MAX_A2A_HOP_COUNT {
            return self.record_blocked_loop(
                &req,
                format!(
                    "hop_count {} exceeds maximum allowed ({MAX_A2A_HOP_COUNT})",
                    req.hop_count
                ),
            );
        }

        let limits = self.limits();
        let now = Instant::now();
        if let Some(ref task_id) = req.task_id {
            let (active_for_task, existing) = self.load_task_messages_for_accounting(task_id)?;
            if limits.max_messages_per_task > 0 && active_for_task >= limits.max_messages_per_task
            {
                return self.record_blocked_loop(
                    &req,
                    format!(
                        "task '{task_id}' reached max_messages_per_task ({})",
                        limits.max_messages_per_task
                    ),
                );
            }

            let identical_count = existing
                .iter()
                .filter(|m| {
                    m.sender_agent_id == req.sender_agent_id
                        && m.recipient_agent_id == req.recipient_agent_id
                        && m.message_type == req.message_type
                        && !is_non_counting_status(m.status)
                })
                .count();
            if identical_count >= MAX_IDENTICAL_TASK_MESSAGES {
                return self.record_blocked_loop(
                    &req,
                    format!(
                        "ping-pong cycle detected: {} -> {} ({}) repeated > {MAX_IDENTICAL_TASK_MESSAGES} times in task '{task_id}'",
                        req.sender_agent_id,
                        req.recipient_agent_id,
                        message_type_str(req.message_type)
                    ),
                );
            }
        } else {
            let blocked_reason = {
                let mut guard = lock_mutex(&self.guard_state, "guard_state");
                guard.check_unscoped_guard(&req, &limits, now)
            };
            if let Some(reason) = blocked_reason {
                return self.record_blocked_loop(&req, reason);
            }
        }

        let envelope = self.build_envelope(&req, DeliveryStatus::Delivered);
        self.storage.append_agent_message(&envelope)?;
        if req.task_id.is_none() {
            let mut guard = lock_mutex(&self.guard_state, "guard_state");
            guard.record_unscoped_send(&req, now);
        }
        drop(send_gate);

        self.emit_sent(&envelope);
        self.deliver_to_inbox(&envelope);
        log_info(
            "messaging",
            format!(
                "{} -> {} [{}] ({})",
                envelope.sender_agent_id,
                envelope.recipient_agent_id,
                message_type_str(envelope.message_type),
                envelope.id
            ),
        );
        Ok(envelope)
    }

    pub fn mark_processed(&self, msg_id: &str) -> Result<()> {
        self.storage
            .update_agent_message_status(msg_id, DeliveryStatus::Processed)
    }

    pub fn list_messages(
        &self,
        task_id: Option<&str>,
        agent_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<AgentEnvelope>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        self.storage
            .list_agent_messages(task_id, agent_id, limit.min(MAX_TASK_SCAN_LIMIT))
    }

    /// Counts active (non-`Rejected`, non-`BlockedLoop`) envelopes for `task_id`.
    /// Even when `StorageBackend::count_task_agent_messages` counts all rows,
    /// `Rejected` and `BlockedLoop` envelopes are excluded here and from `max_messages_per_task`.
    pub fn count_task_messages(&self, task_id: &str) -> Result<u32> {
        let (active_count, _) = self.load_task_messages_for_accounting(task_id)?;
        Ok(active_count)
    }

    fn load_task_messages_for_accounting(
        &self,
        task_id: &str,
    ) -> Result<(u32, Vec<AgentEnvelope>)> {
        let count_hint = self.storage.count_task_agent_messages(task_id)? as usize;
        let scan_limit = count_hint
            .saturating_add(MAX_STORED_DENIED_PER_TASK.saturating_add(64))
            .clamp(128, MAX_TASK_SCAN_LIMIT);
        let rows = self
            .storage
            .list_agent_messages(Some(task_id), None, scan_limit)?;
        let active_count = rows
            .iter()
            .filter(|m| !is_non_counting_status(m.status))
            .count() as u32;
        Ok((active_count, rows))
    }

    fn reserve_denied_slot(&self, req: &SendMessageRequest) -> DeniedPersistence {
        if let Some(ref task_id) = req.task_id {
            let needs_seed = {
                let guard = lock_mutex(&self.guard_state, "guard_state");
                !guard.per_task_denied.contains_key(task_id)
            };
            if needs_seed {
                let existing = self
                    .load_task_messages_for_accounting(task_id)
                    .map(|(_, rows)| {
                        rows.into_iter()
                            .filter(|m| is_non_counting_status(m.status))
                            .count()
                    })
                    .unwrap_or(0);
                let mut guard = lock_mutex(&self.guard_state, "guard_state");
                let seq = guard.next_seq();
                if !guard.per_task_denied.contains_key(task_id) {
                    evict_oldest_if_full(
                        &mut guard.per_task_denied,
                        MAX_TRACKED_GUARD_ENTRIES,
                        |counter| counter.last_seq,
                    );
                }
                guard
                    .per_task_denied
                    .entry(task_id.clone())
                    .or_insert(DeniedCounter {
                        persisted: existing,
                        suppression_logged: existing >= MAX_STORED_DENIED_PER_TASK,
                        last_seq: seq,
                    });
            }
            let mut guard = lock_mutex(&self.guard_state, "guard_state");
            guard.reserve_task_denied_slot(task_id)
        } else {
            let needs_seed = {
                let guard = lock_mutex(&self.guard_state, "guard_state");
                !guard.unscoped_seeded
            };
            if needs_seed {
                let recent = self
                    .storage
                    .list_agent_messages(None, None, MAX_UNSCOPED_SEED_SCAN)
                    .unwrap_or_default();
                let mut guard = lock_mutex(&self.guard_state, "guard_state");
                if !guard.unscoped_seeded {
                    let mut total = 0usize;
                    let mut per_pair: HashMap<(String, String), usize> = HashMap::new();
                    for msg in recent {
                        if msg.task_id.is_none() && is_non_counting_status(msg.status) {
                            total = total.saturating_add(1);
                            *per_pair
                                .entry((msg.sender_agent_id, msg.recipient_agent_id))
                                .or_insert(0) += 1;
                        }
                    }
                    guard.unscoped_total_denied = total;
                    guard.unscoped_total_suppression_logged =
                        total >= MAX_STORED_DENIED_UNSCOPED_TOTAL;
                    for (pair, persisted) in per_pair {
                        let seq = guard.next_seq();
                        evict_oldest_if_full(
                            &mut guard.unscoped_pair_denied,
                            MAX_TRACKED_GUARD_ENTRIES,
                            |counter| counter.last_seq,
                        );
                        guard.unscoped_pair_denied.insert(
                            pair,
                            DeniedCounter {
                                persisted,
                                suppression_logged: persisted >= MAX_STORED_DENIED_UNSCOPED_PAIR,
                                last_seq: seq,
                            },
                        );
                    }
                    guard.unscoped_seeded = true;
                }
            }
            let mut guard = lock_mutex(&self.guard_state, "guard_state");
            guard.reserve_unscoped_denied_slot(&req.sender_agent_id, &req.recipient_agent_id)
        }
    }

    fn build_envelope(&self, req: &SendMessageRequest, status: DeliveryStatus) -> AgentEnvelope {
        AgentEnvelope {
            id: new_id(),
            sender_agent_id: req.sender_agent_id.clone(),
            recipient_agent_id: req.recipient_agent_id.clone(),
            task_id: req.task_id.clone(),
            timestamp: now_iso(),
            message_type: req.message_type,
            content: req.content.clone(),
            status,
            hop_count: req.hop_count,
        }
    }

    fn emit_sent(&self, envelope: &AgentEnvelope) {
        let _ = self.events_tx.send(StreamEvent::AgentMessageSent {
            envelope_id: envelope.id.clone(),
            sender_agent_id: envelope.sender_agent_id.clone(),
            recipient_agent_id: envelope.recipient_agent_id.clone(),
            message_type: message_type_str(envelope.message_type).to_string(),
            status: delivery_status_str(envelope.status).to_string(),
        });
    }

    fn deliver_to_inbox(&self, envelope: &AgentEnvelope) {
        let (maybe_tx, has_closed) = {
            let guard = read_rw(&self.inboxes, "inboxes");
            let tx = guard.get(&envelope.recipient_agent_id).cloned();
            let has_closed = guard.values().any(|sender| sender.is_closed());
            (tx, has_closed)
        };
        if let Some(tx) = maybe_tx {
            match tx.try_send(envelope.clone()) {
                Ok(()) => {
                    if has_closed {
                        self.sweep_closed_inboxes();
                    }
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    log_warn(
                        "messaging",
                        format!(
                            "inbox full ({AGENT_INBOX_CAPACITY}) for agent '{}'; skipping live push for envelope {}",
                            envelope.recipient_agent_id, envelope.id
                        ),
                    );
                    if has_closed {
                        self.sweep_closed_inboxes();
                    }
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    let mut guard = write_rw(&self.inboxes, "inboxes");
                    if guard
                        .get(&envelope.recipient_agent_id)
                        .map_or(false, |current| current.same_channel(&tx))
                    {
                        guard.remove(&envelope.recipient_agent_id);
                    }
                    sweep_inboxes_guard(&mut guard);
                }
            }
        } else if has_closed {
            self.sweep_closed_inboxes();
        }
    }

    fn record_rejected(
        &self,
        req: &SendMessageRequest,
        err: AgentError,
    ) -> Result<AgentEnvelope> {
        match self.reserve_denied_slot(req) {
            DeniedPersistence::Persist => {
                let envelope = self.build_envelope(req, DeliveryStatus::Rejected);
                if let Err(store_err) = self.storage.append_agent_message(&envelope) {
                    log_warn(
                        "messaging",
                        format!("failed to persist REJECTED A2A envelope: {store_err}"),
                    );
                }
                self.emit_sent(&envelope);
                log_warn(
                    "messaging",
                    format!(
                        "rejected A2A {} -> {}: {err}",
                        req.sender_agent_id, req.recipient_agent_id
                    ),
                );
            }
            DeniedPersistence::Suppress { emit_cap_notice } => {
                if emit_cap_notice {
                    log_warn(
                        "messaging",
                        format!(
                            "suppressing further persisted REJECTED envelopes for {} -> {} (task={:?}): {err}",
                            req.sender_agent_id, req.recipient_agent_id, req.task_id
                        ),
                    );
                }
            }
        }
        Err(err)
    }

    fn record_blocked_loop(
        &self,
        req: &SendMessageRequest,
        reason: String,
    ) -> Result<AgentEnvelope> {
        match self.reserve_denied_slot(req) {
            DeniedPersistence::Persist => {
                let envelope = self.build_envelope(req, DeliveryStatus::BlockedLoop);
                if let Err(store_err) = self.storage.append_agent_message(&envelope) {
                    log_warn(
                        "messaging",
                        format!("failed to persist BLOCKED_LOOP envelope: {store_err}"),
                    );
                }
                self.emit_sent(&envelope);
                let _ = self.storage.append_audit_log(&AuditLogEntry {
                    id: new_id(),
                    timestamp: now_iso(),
                    actor: req.sender_agent_id.clone(),
                    action: "a2a.blocked_loop".to_string(),
                    target: req.recipient_agent_id.clone(),
                    severity: "warn".to_string(),
                    outcome: "blocked".to_string(),
                    details_json: serde_json::json!({
                        "envelope_id": envelope.id,
                        "task_id": req.task_id,
                        "message_type": message_type_str(req.message_type),
                        "hop_count": req.hop_count,
                        "reason": reason,
                    })
                    .to_string(),
                });
                log_warn("messaging", format!("BLOCKED_LOOP: {reason}"));
            }
            DeniedPersistence::Suppress { emit_cap_notice } => {
                if emit_cap_notice {
                    log_warn(
                        "messaging",
                        format!(
                            "suppressing further persisted BLOCKED_LOOP records for {} -> {} (task={:?}): {reason}",
                            req.sender_agent_id, req.recipient_agent_id, req.task_id
                        ),
                    );
                }
            }
        }
        Err(AgentError::LimitExceeded(reason))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ps5_agent_core::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct MemStorage {
        agents: Mutex<HashMap<String, AgentDefinition>>,
        envelopes: Mutex<Vec<AgentEnvelope>>,
        audits: Mutex<Vec<AuditLogEntry>>,
    }

    impl MemStorage {
        fn with_agents(agents: Vec<AgentDefinition>) -> Arc<Self> {
            let mut map = HashMap::new();
            for a in agents {
                map.insert(a.id.clone(), a);
            }
            Arc::new(Self {
                agents: Mutex::new(map),
                ..Default::default()
            })
        }
    }

    impl StorageBackend for MemStorage {
        fn driver_name(&self) -> &'static str { "mem" }
        fn run_migrations(&self) -> Result<()> { Ok(()) }
        fn integrity_check(&self) -> Result<String> { Ok("ok".into()) }
        fn checkpoint(&self) -> Result<()> { Ok(()) }
        fn upsert_agent(&self, agent: &AgentDefinition) -> Result<()> {
            self.agents.lock().unwrap().insert(agent.id.clone(), agent.clone());
            Ok(())
        }
        fn get_agent(&self, agent_id: &str) -> Result<Option<AgentDefinition>> {
            Ok(self.agents.lock().unwrap().get(agent_id).cloned())
        }
        fn list_agents(&self) -> Result<Vec<AgentDefinition>> {
            Ok(self.agents.lock().unwrap().values().cloned().collect())
        }
        fn delete_agent(&self, agent_id: &str) -> Result<bool> {
            Ok(self.agents.lock().unwrap().remove(agent_id).is_some())
        }
        fn create_conversation(&self, _: &Conversation) -> Result<()> { Ok(()) }
        fn get_conversation(&self, _: &str) -> Result<Option<Conversation>> { Ok(None) }
        fn list_conversations(&self, _: bool) -> Result<Vec<Conversation>> { Ok(vec![]) }
        fn delete_conversation(&self, _: &str) -> Result<bool> { Ok(false) }
        fn append_chat_message(&self, _: &ChatMessage) -> Result<()> { Ok(()) }
        fn list_chat_messages(&self, _: &str, _: bool, _: usize) -> Result<Vec<ChatMessage>> { Ok(vec![]) }
        fn search_chat_messages(&self, _: &str, _: usize) -> Result<Vec<ChatMessage>> { Ok(vec![]) }
        fn archive_old_chat_messages(&self, _: &str, _: usize) -> Result<usize> { Ok(0) }
        fn upsert_task(&self, _: &TaskRecord) -> Result<()> { Ok(()) }
        fn get_task(&self, _: &str) -> Result<Option<TaskRecord>> { Ok(None) }
        fn list_tasks(&self, _: usize) -> Result<Vec<TaskRecord>> { Ok(vec![]) }
        fn task_counts_summary(&self) -> Result<TaskCountsSummary> { Ok(TaskCountsSummary::default()) }
        fn append_task_event(&self, _: &TaskEvent) -> Result<()> { Ok(()) }
        fn list_task_events(&self, _: &str) -> Result<Vec<TaskEvent>> { Ok(vec![]) }
        fn append_agent_message(&self, msg: &AgentEnvelope) -> Result<()> {
            self.envelopes.lock().unwrap().push(msg.clone());
            Ok(())
        }
        fn update_agent_message_status(&self, msg_id: &str, status: DeliveryStatus) -> Result<()> {
            if let Some(m) = self.envelopes.lock().unwrap().iter_mut().find(|m| m.id == msg_id) {
                m.status = status;
            }
            Ok(())
        }
        fn list_agent_messages(&self, task_id: Option<&str>, agent_id: Option<&str>, limit: usize) -> Result<Vec<AgentEnvelope>> {
            if limit == 0 {
                return Ok(Vec::new());
            }
            let list = self.envelopes.lock().unwrap();
            Ok(list
                .iter()
                .rev()
                .filter(|m| task_id.map_or(true, |t| m.task_id.as_deref() == Some(t)))
                .filter(|m| agent_id.map_or(true, |a| m.sender_agent_id == a || m.recipient_agent_id == a))
                .take(limit)
                .cloned()
                .collect())
        }
        fn count_task_agent_messages(&self, task_id: &str) -> Result<u32> {
            // Mirrors SQLite/JSONL raw row counts so tests verify MessageBus excludes Rejected/BlockedLoop.
            Ok(self
                .envelopes
                .lock()
                .unwrap()
                .iter()
                .filter(|m| m.task_id.as_deref() == Some(task_id))
                .count() as u32)
        }
        fn upsert_memory(&self, _: &MemoryItem) -> Result<()> { Ok(()) }
        fn query_memories(&self, _: Option<MemoryScope>, _: Option<&str>, _: Option<&str>, _: bool, _: usize) -> Result<Vec<MemoryItem>> { Ok(vec![]) }
        fn delete_memory(&self, _: &str) -> Result<bool> { Ok(false) }
        fn delete_agent_memories(&self, _: &str) -> Result<usize> { Ok(0) }
        fn memory_stats_for_agent(&self, _: &str) -> Result<(u64, u64)> { Ok((0, 0)) }
        fn upsert_tool_spec(&self, _: &ToolSpec) -> Result<()> { Ok(()) }
        fn list_tool_specs(&self) -> Result<Vec<ToolSpec>> { Ok(vec![]) }
        fn record_tool_execution(&self, _: &ToolExecutionRecord) -> Result<()> { Ok(()) }
        fn list_tool_executions(&self, _: Option<&str>, _: usize) -> Result<Vec<ToolExecutionRecord>> { Ok(vec![]) }
        fn get_setting(&self, _: &str) -> Result<Option<String>> { Ok(None) }
        fn set_setting(&self, _: &str, _: &str) -> Result<()> { Ok(()) }
        fn list_settings(&self) -> Result<Vec<(String, String)>> { Ok(vec![]) }
        fn create_session(&self, _: &SessionRecord) -> Result<()> { Ok(()) }
        fn get_session_by_hash(&self, _: &str) -> Result<Option<SessionRecord>> { Ok(None) }
        fn revoke_session(&self, _: &str) -> Result<()> { Ok(()) }
        fn purge_expired_sessions(&self, _: &str) -> Result<usize> { Ok(0) }
        fn upsert_approval(&self, _: &ApprovalRecord) -> Result<()> { Ok(()) }
        fn get_approval(&self, _: &str) -> Result<Option<ApprovalRecord>> { Ok(None) }
        fn list_approvals(&self, _: bool, _: usize) -> Result<Vec<ApprovalRecord>> { Ok(vec![]) }
        fn append_audit_log(&self, entry: &AuditLogEntry) -> Result<()> {
            self.audits.lock().unwrap().push(entry.clone());
            Ok(())
        }
        fn list_audit_logs(&self, limit: usize) -> Result<Vec<AuditLogEntry>> {
            Ok(self.audits.lock().unwrap().iter().take(limit).cloned().collect())
        }
    }

    fn make_agent(id: &str, enabled: bool, can_communicate: bool) -> AgentDefinition {
        AgentDefinition {
            id: id.into(),
            name: id.into(),
            description: "test".into(),
            role: AgentRole::Custom,
            role_label: "Custom".into(),
            system_prompt: "test".into(),
            model_config: AgentModelConfig::default(),
            enabled_tools: vec![],
            denied_tools: vec![],
            sandbox_mode: None,
            tool_profile: None,
            memory_permissions: MemoryPermissions::default(),
            max_task_duration_secs: 60,
            max_reasoning_iterations: 4,
            priority: 50,
            can_communicate,
            enabled,
            is_builtin: false,
            is_protected: false,
            is_temporary: false,
            created_at: now_iso(),
            updated_at: now_iso(),
        }
    }

    #[test]
    fn sends_all_9_message_types_and_emits_stream_event() {
        let storage = MemStorage::with_agents(vec![
            make_agent("commander", true, true),
            make_agent("researcher", true, true),
        ]);
        let (tx, mut rx) = broadcast::channel(32);
        let bus = MessageBus::new(storage.clone(), RuntimeLimits::default(), tx);

        let all_types = [
            AgentMessageType::TaskAssignment,
            AgentMessageType::TaskUpdate,
            AgentMessageType::TaskCompleted,
            AgentMessageType::InformationRequest,
            AgentMessageType::InformationResponse,
            AgentMessageType::AgentMessage,
            AgentMessageType::ErrorReport,
            AgentMessageType::ApprovalRequest,
            AgentMessageType::ApprovalResponse,
        ];

        for (i, msg_type) in all_types.into_iter().enumerate() {
            let task_id = format!("task-all-{i}");
            let env = bus
                .send(SendMessageRequest {
                    sender_agent_id: "commander".into(),
                    recipient_agent_id: "researcher".into(),
                    task_id: Some(task_id),
                    message_type: msg_type,
                    content: format!("payload-{i}"),
                    hop_count: 1,
                })
                .expect("send message");
            assert_eq!(env.status, DeliveryStatus::Delivered);
            assert_eq!(env.message_type, msg_type);

            let ev = rx.try_recv().expect("stream event");
            match ev {
                StreamEvent::AgentMessageSent {
                    envelope_id,
                    sender_agent_id,
                    recipient_agent_id,
                    message_type,
                    status,
                } => {
                    assert_eq!(envelope_id, env.id);
                    assert_eq!(sender_agent_id, "commander");
                    assert_eq!(recipient_agent_id, "researcher");
                    assert_eq!(message_type, message_type_str(msg_type));
                    assert_eq!(status, "DELIVERED");
                }
                other => panic!("unexpected event: {other:?}"),
            }
        }

        assert_eq!(bus.list_messages(None, None, 50).unwrap().len(), 9);
    }

    #[test]
    fn blocks_when_sender_cannot_communicate_or_recipient_disabled() {
        let storage = MemStorage::with_agents(vec![
            make_agent("silent", true, false),
            make_agent("active", true, true),
            make_agent("disabled", false, true),
        ]);
        let (tx, _rx) = broadcast::channel(16);
        let bus = MessageBus::new(storage.clone(), RuntimeLimits::default(), tx);

        let err1 = bus
            .send(SendMessageRequest {
                sender_agent_id: "silent".into(),
                recipient_agent_id: "active".into(),
                task_id: Some("t1".into()),
                message_type: AgentMessageType::AgentMessage,
                content: "hi".into(),
                hop_count: 0,
            })
            .unwrap_err();
        assert!(matches!(err1, AgentError::PermissionDenied(_)));

        let err2 = bus
            .send(SendMessageRequest {
                sender_agent_id: "active".into(),
                recipient_agent_id: "disabled".into(),
                task_id: Some("t1".into()),
                message_type: AgentMessageType::AgentMessage,
                content: "hi".into(),
                hop_count: 0,
            })
            .unwrap_err();
        assert!(matches!(err2, AgentError::PermissionDenied(_)));
    }

    #[test]
    fn loop_protection_blocks_hop_limit_task_message_limit_and_ping_pong() {
        let storage = MemStorage::with_agents(vec![
            make_agent("a", true, true),
            make_agent("b", true, true),
        ]);
        let (tx, mut rx) = broadcast::channel(64);
        let mut limits = RuntimeLimits::default();
        limits.max_messages_per_task = 5;
        let bus = MessageBus::new(storage.clone(), limits, tx);

        // 1) Hop count > 6 blocked
        let err = bus
            .send(SendMessageRequest {
                sender_agent_id: "a".into(),
                recipient_agent_id: "b".into(),
                task_id: Some("task-hop".into()),
                message_type: AgentMessageType::TaskAssignment,
                content: "deep".into(),
                hop_count: 7,
            })
            .unwrap_err();
        assert!(matches!(err, AgentError::LimitExceeded(_)));
        let envs = bus.list_messages(Some("task-hop"), None, 10).unwrap();
        assert_eq!(envs[0].status, DeliveryStatus::BlockedLoop);

        // Drain event
        if let StreamEvent::AgentMessageSent { status, .. } = rx.try_recv().unwrap() {
            assert_eq!(status, "BLOCKED_LOOP");
        } else {
            panic!("expected AgentMessageSent");
        }

        // 2) Same (sender, recipient, type) > 3 times in one task blocked on 4th
        for i in 0..3 {
            bus.send(SendMessageRequest {
                sender_agent_id: "a".into(),
                recipient_agent_id: "b".into(),
                task_id: Some("task-ping".into()),
                message_type: AgentMessageType::InformationRequest,
                content: format!("req {i}"),
                hop_count: 1,
            })
            .expect("within 3");
        }
        let err_ping = bus
            .send(SendMessageRequest {
                sender_agent_id: "a".into(),
                recipient_agent_id: "b".into(),
                task_id: Some("task-ping".into()),
                message_type: AgentMessageType::InformationRequest,
                content: "req 4".into(),
                hop_count: 1,
            })
            .unwrap_err();
        assert!(matches!(err_ping, AgentError::LimitExceeded(_)));

        // 3) Task message total limit (max_messages_per_task = 5)
        for i in 0..5 {
            let msg_type = match i % 2 {
                0 => AgentMessageType::TaskUpdate,
                _ => AgentMessageType::AgentMessage,
            };
            bus.send(SendMessageRequest {
                sender_agent_id: "a".into(),
                recipient_agent_id: "b".into(),
                task_id: Some("task-max".into()),
                message_type: msg_type,
                content: format!("m {i}"),
                hop_count: 1,
            })
            .expect("under limit");
        }
        let err_max = bus
            .send(SendMessageRequest {
                sender_agent_id: "b".into(),
                recipient_agent_id: "a".into(),
                task_id: Some("task-max".into()),
                message_type: AgentMessageType::InformationResponse,
                content: "over".into(),
                hop_count: 1,
            })
            .unwrap_err();
        assert!(matches!(err_max, AgentError::LimitExceeded(_)));
    }

    #[test]
    fn subscribes_inbox_marks_processed_and_parses_types() {
        let storage = MemStorage::with_agents(vec![
            make_agent("a", true, true),
            make_agent("b", true, true),
        ]);
        let (tx, _rx) = broadcast::channel(16);
        let bus = MessageBus::new(storage.clone(), RuntimeLimits::default(), tx);
        let mut inbox = bus.subscribe_agent("b");

        let env = bus
            .send(SendMessageRequest {
                sender_agent_id: "a".into(),
                recipient_agent_id: "b".into(),
                task_id: Some("t-inbox".into()),
                message_type: parse_message_type("task_assignment").unwrap(),
                content: "hello".into(),
                hop_count: 0,
            })
            .unwrap();

        let received = inbox.try_recv().expect("inbox received");
        assert_eq!(received.id, env.id);
        assert_eq!(bus.count_task_messages("t-inbox").unwrap(), 1);

        bus.mark_processed(&env.id).unwrap();
        let stored = bus.list_messages(Some("t-inbox"), Some("b"), 10).unwrap();
        assert_eq!(stored[0].status, DeliveryStatus::Processed);
        assert!(parse_message_type("INVALID_TYPE").is_err());
    }

    #[tokio::test]
    async fn zero_max_messages_per_task_is_unlimited_and_dead_inboxes_are_cleaned() {
        let storage = MemStorage::with_agents(vec![
            make_agent("a", true, true),
            make_agent("b", true, true),
            make_agent("c", true, true),
        ]);
        let (tx, _rx) = broadcast::channel(64);
        let mut limits = RuntimeLimits::default();
        limits.max_messages_per_task = 0;
        let bus = MessageBus::new(storage.clone(), limits, tx);

        let rx_b = bus.subscribe_agent("b");
        drop(rx_b);
        assert!(bus.inboxes.read().unwrap().contains_key("b"));

        // Sending to dropped receiver evicts dead inbox entry while still persisting message
        bus.send(SendMessageRequest {
            sender_agent_id: "a".into(),
            recipient_agent_id: "b".into(),
            task_id: Some("t-unlimited".into()),
            message_type: AgentMessageType::TaskUpdate,
            content: "msg-0".into(),
            hop_count: 1,
        })
        .unwrap();
        assert!(!bus.inboxes.read().unwrap().contains_key("b"));

        // Unsubscribe removes live inbox cleanly
        let _rx_c = bus.subscribe_agent("c");
        assert!(bus.inboxes.read().unwrap().contains_key("c"));
        bus.unsubscribe_agent("c").await;
        assert!(!bus.inboxes.read().unwrap().contains_key("c"));

        // Sending > 0 (and > 3 across distinct types) succeeds when max_messages_per_task == 0
        let msg_types = [
            AgentMessageType::TaskAssignment,
            AgentMessageType::TaskUpdate,
            AgentMessageType::TaskCompleted,
            AgentMessageType::InformationRequest,
            AgentMessageType::InformationResponse,
            AgentMessageType::AgentMessage,
        ];
        for (idx, msg_type) in msg_types.iter().enumerate() {
            bus.send(SendMessageRequest {
                sender_agent_id: "a".into(),
                recipient_agent_id: "c".into(),
                task_id: Some("t-unlimited".into()),
                message_type: *msg_type,
                content: format!("unlimited-{idx}"),
                hop_count: 1,
            })
            .unwrap();
        }
        assert_eq!(bus.count_task_messages("t-unlimited").unwrap(), 7);
    }

    #[test]
    fn bounded_inbox_handles_full_channel_without_evicting_or_failing() {
        let storage = MemStorage::with_agents(vec![
            make_agent("a", true, true),
            make_agent("b", true, true),
        ]);
        let (tx, _rx) = broadcast::channel(64);
        let mut limits = RuntimeLimits::default();
        limits.max_messages_per_task = 0;
        let bus = MessageBus::new(storage, limits, tx);

        let mut rx_b = bus.subscribe_agent("b");
        for i in 0..(AGENT_INBOX_CAPACITY + 10) {
            // Alternate task IDs so ping-pong protection doesn't trigger
            bus.send(SendMessageRequest {
                sender_agent_id: "a".into(),
                recipient_agent_id: "b".into(),
                task_id: Some(format!("task-{i}")),
                message_type: AgentMessageType::AgentMessage,
                content: format!("m-{i}"),
                hop_count: 1,
            })
            .unwrap();
        }

        // Channel stayed registered and buffered up to AGENT_INBOX_CAPACITY
        assert!(bus.inboxes.read().unwrap().contains_key("b"));
        let mut drained = 0usize;
        while rx_b.try_recv().is_ok() {
            drained += 1;
        }
        assert_eq!(drained, AGENT_INBOX_CAPACITY);
    }

    #[test]
    fn rejected_and_blocked_envelopes_excluded_from_counts_and_quotas() {
        let storage = MemStorage::with_agents(vec![
            make_agent("a", true, true),
            make_agent("b", false, true),
        ]);
        let (tx, _rx) = broadcast::channel(64);
        let mut limits = RuntimeLimits::default();
        limits.max_messages_per_task = 4;
        let bus = MessageBus::new(storage.clone(), limits, tx);

        // Repeatedly rejected messages while 'b' is disabled must not exhaust task quota or count as active.
        for i in 0..5 {
            let err = bus
                .send(SendMessageRequest {
                    sender_agent_id: "a".into(),
                    recipient_agent_id: "b".into(),
                    task_id: Some("t-quota".into()),
                    message_type: AgentMessageType::TaskUpdate,
                    content: format!("rej-{i}"),
                    hop_count: 1,
                })
                .unwrap_err();
            assert!(matches!(err, AgentError::PermissionDenied(_)));
        }
        assert!(storage.count_task_agent_messages("t-quota").unwrap() >= 5);
        assert_eq!(bus.count_task_messages("t-quota").unwrap(), 0);

        // Enable 'b', hit identical-message BlockedLoop, and verify BlockedLoop retries also do not burn quota.
        storage.upsert_agent(&make_agent("b", true, true)).unwrap();
        for i in 0..3 {
            bus.send(SendMessageRequest {
                sender_agent_id: "a".into(),
                recipient_agent_id: "b".into(),
                task_id: Some("t-quota".into()),
                message_type: AgentMessageType::InformationRequest,
                content: format!("ok-ir-{i}"),
                hop_count: 1,
            })
            .unwrap();
        }
        for i in 0..4 {
            let err = bus
                .send(SendMessageRequest {
                    sender_agent_id: "a".into(),
                    recipient_agent_id: "b".into(),
                    task_id: Some("t-quota".into()),
                    message_type: AgentMessageType::InformationRequest,
                    content: format!("blocked-ir-{i}"),
                    hop_count: 1,
                })
                .unwrap_err();
            assert!(matches!(err, AgentError::LimitExceeded(_)));
        }
        assert!(storage.count_task_agent_messages("t-quota").unwrap() >= 11);
        assert_eq!(bus.count_task_messages("t-quota").unwrap(), 3);

        // 4th distinct active message must still succeed (active count was 3 < max 4).
        bus.send(SendMessageRequest {
            sender_agent_id: "a".into(),
            recipient_agent_id: "b".into(),
            task_id: Some("t-quota".into()),
            message_type: AgentMessageType::TaskUpdate,
            content: "ok-4".into(),
            hop_count: 1,
        })
        .unwrap();
        assert_eq!(bus.count_task_messages("t-quota").unwrap(), 4);

        // 5th active message must hit max_messages_per_task = 4.
        let over = bus
            .send(SendMessageRequest {
                sender_agent_id: "a".into(),
                recipient_agent_id: "b".into(),
                task_id: Some("t-quota".into()),
                message_type: AgentMessageType::TaskCompleted,
                content: "over-5".into(),
                hop_count: 1,
            })
            .unwrap_err();
        assert!(matches!(over, AgentError::LimitExceeded(_)));
        assert_eq!(bus.count_task_messages("t-quota").unwrap(), 4);
    }

    #[test]
    fn stored_rejected_and_blocked_envelopes_are_bounded_per_task_and_unscoped() {
        let mut agents = vec![make_agent("a", true, true)];
        for idx in 0..6 {
            agents.push(make_agent(&format!("dis-{idx}"), false, true));
        }
        let storage = MemStorage::with_agents(agents);
        let (tx, _rx) = broadcast::channel(64);
        let bus = MessageBus::new(storage.clone(), RuntimeLimits::default(), tx.clone());

        // 1) Task-scoped Rejected rows are capped at MAX_STORED_DENIED_PER_TASK.
        for i in 0..50 {
            let err = bus
                .send(SendMessageRequest {
                    sender_agent_id: "a".into(),
                    recipient_agent_id: "dis-0".into(),
                    task_id: Some("t-rej-cap".into()),
                    message_type: AgentMessageType::AgentMessage,
                    content: format!("rej-{i}"),
                    hop_count: 1,
                })
                .unwrap_err();
            assert!(matches!(err, AgentError::PermissionDenied(_)));
        }
        assert_eq!(
            bus.list_messages(Some("t-rej-cap"), None, 100).unwrap().len(),
            MAX_STORED_DENIED_PER_TASK
        );
        assert_eq!(bus.count_task_messages("t-rej-cap").unwrap(), 0);

        // 2) Task-scoped BlockedLoop rows + AuditLog entries are capped at MAX_STORED_DENIED_PER_TASK.
        let enabled_b = make_agent("b", true, true);
        storage.upsert_agent(&enabled_b).unwrap();
        for i in 0..50 {
            let err = bus
                .send(SendMessageRequest {
                    sender_agent_id: "a".into(),
                    recipient_agent_id: "b".into(),
                    task_id: Some("t-loop-cap".into()),
                    message_type: AgentMessageType::TaskAssignment,
                    content: format!("hop-{i}"),
                    hop_count: MAX_A2A_HOP_COUNT + 1,
                })
                .unwrap_err();
            assert!(matches!(err, AgentError::LimitExceeded(_)));
        }
        assert_eq!(
            bus.list_messages(Some("t-loop-cap"), None, 100).unwrap().len(),
            MAX_STORED_DENIED_PER_TASK
        );
        assert_eq!(storage.audits.lock().unwrap().len(), MAX_STORED_DENIED_PER_TASK);

        // 3) Unscoped (task_id: None) Rejected rows are capped per pair AND globally across pairs.
        for i in 0..25 {
            let _ = bus.send(SendMessageRequest {
                sender_agent_id: "a".into(),
                recipient_agent_id: "dis-0".into(),
                task_id: None,
                message_type: AgentMessageType::AgentMessage,
                content: format!("unscoped-pair-{i}"),
                hop_count: 1,
            });
        }
        let pair_stored = storage
            .envelopes
            .lock()
            .unwrap()
            .iter()
            .filter(|m| m.task_id.is_none() && m.recipient_agent_id == "dis-0")
            .count();
        assert_eq!(pair_stored, MAX_STORED_DENIED_UNSCOPED_PAIR);

        for target in 1..6 {
            for i in 0..15 {
                let _ = bus.send(SendMessageRequest {
                    sender_agent_id: "a".into(),
                    recipient_agent_id: format!("dis-{target}"),
                    task_id: None,
                    message_type: AgentMessageType::AgentMessage,
                    content: format!("unscoped-multi-{target}-{i}"),
                    hop_count: 1,
                });
            }
        }
        let total_unscoped_denied = storage
            .envelopes
            .lock()
            .unwrap()
            .iter()
            .filter(|m| m.task_id.is_none() && is_non_counting_status(m.status))
            .count();
        assert_eq!(total_unscoped_denied, MAX_STORED_DENIED_UNSCOPED_TOTAL);

        // 4) Restart parity: a fresh MessageBus instance respects existing stored denials for a task.
        let fresh_bus = MessageBus::new(storage.clone(), RuntimeLimits::default(), tx);
        let err = fresh_bus
            .send(SendMessageRequest {
                sender_agent_id: "a".into(),
                recipient_agent_id: "b".into(),
                task_id: Some("t-loop-cap".into()),
                message_type: AgentMessageType::TaskAssignment,
                content: "after-restart".into(),
                hop_count: MAX_A2A_HOP_COUNT + 1,
            })
            .unwrap_err();
        assert!(matches!(err, AgentError::LimitExceeded(_)));
        assert_eq!(
            fresh_bus
                .list_messages(Some("t-loop-cap"), None, 100)
                .unwrap()
                .len(),
            MAX_STORED_DENIED_PER_TASK
        );
    }

    #[test]
    fn unscoped_sends_without_task_id_enforce_identical_and_sender_rate_guards() {
        let storage = MemStorage::with_agents(vec![
            make_agent("a", true, true),
            make_agent("b", true, true),
            make_agent("c", true, true),
            make_agent("d", true, true),
        ]);
        let (tx, _rx) = broadcast::channel(64);
        let mut limits = RuntimeLimits::default();
        limits.max_messages_per_task = 5;
        let bus = MessageBus::new(storage.clone(), limits, tx.clone());

        // 1) Identical (sender, recipient, message_type) without task_id blocks on 4th attempt.
        for i in 0..MAX_IDENTICAL_TASK_MESSAGES {
            bus.send(SendMessageRequest {
                sender_agent_id: "a".into(),
                recipient_agent_id: "b".into(),
                task_id: None,
                message_type: AgentMessageType::InformationRequest,
                content: format!("unscoped-ir-{i}"),
                hop_count: 1,
            })
            .expect("first 3 identical unscoped sends allowed");
        }
        let err_dup = bus
            .send(SendMessageRequest {
                sender_agent_id: "a".into(),
                recipient_agent_id: "b".into(),
                task_id: None,
                message_type: AgentMessageType::InformationRequest,
                content: "unscoped-ir-dup".into(),
                hop_count: 1,
            })
            .unwrap_err();
        assert!(matches!(err_dup, AgentError::LimitExceeded(_)));

        // 2) Rotating types/recipients still hits per-sender rate cap (= 5) when task_id is None.
        let types = [
            AgentMessageType::TaskAssignment,
            AgentMessageType::TaskUpdate,
            AgentMessageType::TaskCompleted,
            AgentMessageType::InformationResponse,
            AgentMessageType::AgentMessage,
            AgentMessageType::ApprovalRequest,
        ];
        for (idx, msg_type) in types[..5].iter().enumerate() {
            bus.send(SendMessageRequest {
                sender_agent_id: "c".into(),
                recipient_agent_id: if idx % 2 == 0 { "b" } else { "d" }.into(),
                task_id: None,
                message_type: *msg_type,
                content: format!("c-unscoped-{idx}"),
                hop_count: 1,
            })
            .expect("within per-sender unscoped rate limit");
        }
        let err_rate = bus
            .send(SendMessageRequest {
                sender_agent_id: "c".into(),
                recipient_agent_id: "b".into(),
                task_id: None,
                message_type: types[5],
                content: "c-over-rate".into(),
                hop_count: 1,
            })
            .unwrap_err();
        assert!(matches!(err_rate, AgentError::LimitExceeded(_)));

        // Other senders remain unaffected.
        bus.send(SendMessageRequest {
            sender_agent_id: "d".into(),
            recipient_agent_id: "c".into(),
            task_id: None,
            message_type: AgentMessageType::AgentMessage,
            content: "d-ok".into(),
            hop_count: 1,
        })
        .unwrap();

        // 3) Even when max_messages_per_task == 0, unscoped sends still enforce DEFAULT_UNSCOPED_MAX_PER_SENDER.
        let mut unlimited_task_limits = RuntimeLimits::default();
        unlimited_task_limits.max_messages_per_task = 0;
        let bus2 = MessageBus::new(storage, unlimited_task_limits, tx);
        let recipients = ["b", "c", "d"];
        let all_types = [
            AgentMessageType::TaskAssignment,
            AgentMessageType::TaskUpdate,
            AgentMessageType::TaskCompleted,
            AgentMessageType::InformationRequest,
            AgentMessageType::InformationResponse,
            AgentMessageType::AgentMessage,
            AgentMessageType::ErrorReport,
            AgentMessageType::ApprovalRequest,
            AgentMessageType::ApprovalResponse,
        ];
        for i in 0..DEFAULT_UNSCOPED_MAX_PER_SENDER {
            let recipient = recipients[i % recipients.len()];
            let msg_type = all_types[(i / recipients.len()) % all_types.len()];
            bus2.send(SendMessageRequest {
                sender_agent_id: "a".into(),
                recipient_agent_id: recipient.into(),
                task_id: None,
                message_type: msg_type,
                content: format!("burst-{i}"),
                hop_count: 1,
            })
            .expect("within default unscoped per-sender rate cap");
        }
        let err_default_cap = bus2
            .send(SendMessageRequest {
                sender_agent_id: "a".into(),
                recipient_agent_id: "d".into(),
                task_id: None,
                message_type: AgentMessageType::ApprovalResponse,
                content: "over-default-cap".into(),
                hop_count: 1,
            })
            .unwrap_err();
        assert!(matches!(err_default_cap, AgentError::LimitExceeded(_)));
    }

    #[test]
    fn sweeps_closed_and_dropped_agent_inboxes_cleanly() {
        let storage = MemStorage::with_agents(vec![
            make_agent("a", true, true),
            make_agent("z", true, true),
        ]);
        let (tx, _rx) = broadcast::channel(64);
        let bus = MessageBus::new(storage, RuntimeLimits::default(), tx);

        let rx_x = bus.subscribe_agent("x");
        let rx_y = bus.subscribe_agent("y");
        let mut rx_z = bus.subscribe_agent("z");
        assert_eq!(bus.inboxes.read().unwrap().len(), 3);

        // Drop x and y, then deliver only to z: idle closed channels must be swept proactively.
        drop(rx_x);
        drop(rx_y);
        bus.send(SendMessageRequest {
            sender_agent_id: "a".into(),
            recipient_agent_id: "z".into(),
            task_id: Some("t-sweep".into()),
            message_type: AgentMessageType::AgentMessage,
            content: "hello-z".into(),
            hop_count: 1,
        })
        .unwrap();
        assert!(rx_z.try_recv().is_ok());
        {
            let inboxes = bus.inboxes.read().unwrap();
            assert_eq!(inboxes.len(), 1);
            assert!(inboxes.contains_key("z"));
        }

        // Subscribing a new agent also sweeps closed entries.
        let rx_w = bus.subscribe_agent("w");
        drop(rx_w);
        let _rx_v = bus.subscribe_agent("v");
        {
            let inboxes = bus.inboxes.read().unwrap();
            assert!(!inboxes.contains_key("w"));
            assert!(inboxes.contains_key("z"));
            assert!(inboxes.contains_key("v"));
        }

        // Explicit sweep removes remaining closed receivers and reports the exact swept count.
        let rx_u = bus.subscribe_agent("u");
        drop(rx_u);
        assert_eq!(bus.sweep_closed_inboxes(), 1);
        assert!(!bus.inboxes.read().unwrap().contains_key("u"));
        assert_eq!(bus.sweep_closed_inboxes(), 0);
    }
}
