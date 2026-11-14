pub mod brain;
pub mod doctor;
pub mod runtime;
pub mod scheduler;
pub mod slash;
pub mod task_manager;
pub mod traits;

pub use brain::{
    list_brain_files, read_brain_file, read_markdown_memory_slice, search_markdown_memories,
    seed_workspace_brain_files, write_brain_file,
};
pub use doctor::{run_doctor_checks, run_doctor_fixes};
pub use runtime::{
    tool_pattern_matches, AgentRuntime, ApprovalResolutionOutcome, ChatDispatchOutcome,
    ChatTurnResult, TaskExecutionOutcome,
};
pub use scheduler::{AiScheduleDraft, CreateScheduleInput, UpdateScheduleInput};
pub use slash::{execute_slash_command, parse_slash_command};
pub use task_manager::{task_status_str, RecoveryReport, TaskManager};
pub use traits::{
    NoopToolExecutor, RuntimeMemoryProvider, RuntimeToolExecutor, StorageMemoryProvider,
    ToolExecutionOutput,
};

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use ps5_agent_config::AppConfig;
    use ps5_agent_core::*;
    use ps5_agent_manager::{ActiveTaskPolicy, AgentManager, DeleteAgentOptions};
    use ps5_agent_messaging::{MessageBus, SendMessageRequest};
    use std::collections::{HashMap, VecDeque};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::sync::{broadcast, mpsc};
    use tokio_util::sync::CancellationToken;

    #[derive(Default)]
    struct InMemoryStorage {
        agents: Mutex<HashMap<String, AgentDefinition>>,
        conversations: Mutex<HashMap<String, Conversation>>,
        chat_messages: Mutex<Vec<ChatMessage>>,
        tasks: Mutex<HashMap<String, TaskRecord>>,
        task_events: Mutex<Vec<TaskEvent>>,
        envelopes: Mutex<Vec<AgentEnvelope>>,
        memories: Mutex<HashMap<String, MemoryItem>>,
        tools: Mutex<HashMap<String, ToolSpec>>,
        tool_execs: Mutex<Vec<ToolExecutionRecord>>,
        settings: Mutex<HashMap<String, String>>,
        approvals: Mutex<HashMap<String, ApprovalRecord>>,
        audits: Mutex<Vec<AuditLogEntry>>,
        schedules: Mutex<HashMap<String, ScheduleRecord>>,
    }

    impl StorageBackend for InMemoryStorage {
        fn driver_name(&self) -> &'static str {
            "in_memory"
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
        fn upsert_agent(&self, agent: &AgentDefinition) -> Result<()> {
            self.agents
                .lock()
                .unwrap()
                .insert(agent.id.clone(), agent.clone());
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
        fn create_conversation(&self, conv: &Conversation) -> Result<()> {
            self.conversations
                .lock()
                .unwrap()
                .insert(conv.id.clone(), conv.clone());
            Ok(())
        }
        fn get_conversation(&self, conv_id: &str) -> Result<Option<Conversation>> {
            Ok(self.conversations.lock().unwrap().get(conv_id).cloned())
        }
        fn list_conversations(&self, include_archived: bool) -> Result<Vec<Conversation>> {
            Ok(self
                .conversations
                .lock()
                .unwrap()
                .values()
                .filter(|c| include_archived || !c.archived)
                .cloned()
                .collect())
        }
        fn delete_conversation(&self, conv_id: &str) -> Result<bool> {
            Ok(self.conversations.lock().unwrap().remove(conv_id).is_some())
        }
        fn append_chat_message(&self, msg: &ChatMessage) -> Result<()> {
            self.chat_messages.lock().unwrap().push(msg.clone());
            Ok(())
        }
        fn list_chat_messages(
            &self,
            conv_id: &str,
            include_archived: bool,
            limit: usize,
        ) -> Result<Vec<ChatMessage>> {
            let msgs: Vec<ChatMessage> = self
                .chat_messages
                .lock()
                .unwrap()
                .iter()
                .filter(|m| m.conversation_id == conv_id && (include_archived || !m.archived))
                .cloned()
                .collect();
            let start = msgs.len().saturating_sub(limit);
            Ok(msgs[start..].to_vec())
        }
        fn search_chat_messages(&self, query: &str, limit: usize) -> Result<Vec<ChatMessage>> {
            let q = query.to_ascii_lowercase();
            Ok(self
                .chat_messages
                .lock()
                .unwrap()
                .iter()
                .filter(|m| m.content.to_ascii_lowercase().contains(&q))
                .take(limit)
                .cloned()
                .collect())
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
            Ok(TaskCountsSummary::default())
        }
        fn append_task_event(&self, event: &TaskEvent) -> Result<()> {
            self.task_events.lock().unwrap().push(event.clone());
            Ok(())
        }
        fn list_task_events(&self, task_id: &str) -> Result<Vec<TaskEvent>> {
            Ok(self
                .task_events
                .lock()
                .unwrap()
                .iter()
                .filter(|e| e.task_id == task_id)
                .cloned()
                .collect())
        }
        fn append_agent_message(&self, msg: &AgentEnvelope) -> Result<()> {
            self.envelopes.lock().unwrap().push(msg.clone());
            Ok(())
        }
        fn update_agent_message_status(&self, msg_id: &str, status: DeliveryStatus) -> Result<()> {
            if let Some(m) = self
                .envelopes
                .lock()
                .unwrap()
                .iter_mut()
                .find(|m| m.id == msg_id)
            {
                m.status = status;
            }
            Ok(())
        }
        fn list_agent_messages(
            &self,
            task_id: Option<&str>,
            agent_id: Option<&str>,
            limit: usize,
        ) -> Result<Vec<AgentEnvelope>> {
            Ok(self
                .envelopes
                .lock()
                .unwrap()
                .iter()
                .filter(|m| task_id.map_or(true, |t| m.task_id.as_deref() == Some(t)))
                .filter(|m| {
                    agent_id.map_or(true, |a| {
                        m.sender_agent_id == a || m.recipient_agent_id == a
                    })
                })
                .take(limit)
                .cloned()
                .collect())
        }
        fn count_task_agent_messages(&self, task_id: &str) -> Result<u32> {
            Ok(self
                .envelopes
                .lock()
                .unwrap()
                .iter()
                .filter(|m| m.task_id.as_deref() == Some(task_id))
                .count() as u32)
        }
        fn upsert_memory(&self, item: &MemoryItem) -> Result<()> {
            self.memories
                .lock()
                .unwrap()
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
            Ok(self
                .memories
                .lock()
                .unwrap()
                .values()
                .filter(|m| include_archived || !m.archived)
                .filter(|m| scope.map_or(true, |s| m.scope == s))
                .filter(|m| namespace.map_or(true, |n| m.namespace == n))
                .filter(|m| owner_agent_id.map_or(true, |o| m.owner_agent_id.as_deref() == Some(o)))
                .take(limit)
                .cloned()
                .collect())
        }
        fn delete_memory(&self, memory_id: &str) -> Result<bool> {
            Ok(self.memories.lock().unwrap().remove(memory_id).is_some())
        }
        fn delete_agent_memories(&self, agent_id: &str) -> Result<usize> {
            let mut mems = self.memories.lock().unwrap();
            let before = mems.len();
            mems.retain(|_, m| m.owner_agent_id.as_deref() != Some(agent_id));
            Ok(before - mems.len())
        }
        fn memory_stats_for_agent(&self, agent_id: &str) -> Result<(u64, u64)> {
            let mems = self.memories.lock().unwrap();
            let owned: Vec<_> = mems
                .values()
                .filter(|m| m.owner_agent_id.as_deref() == Some(agent_id))
                .collect();
            let bytes: u64 = owned.iter().map(|m| m.content.len() as u64).sum();
            Ok((owned.len() as u64, bytes))
        }
        fn upsert_tool_spec(&self, spec: &ToolSpec) -> Result<()> {
            self.tools
                .lock()
                .unwrap()
                .insert(spec.id.clone(), spec.clone());
            Ok(())
        }
        fn list_tool_specs(&self) -> Result<Vec<ToolSpec>> {
            Ok(self.tools.lock().unwrap().values().cloned().collect())
        }
        fn record_tool_execution(&self, rec: &ToolExecutionRecord) -> Result<()> {
            self.tool_execs.lock().unwrap().push(rec.clone());
            Ok(())
        }
        fn list_tool_executions(
            &self,
            agent_id: Option<&str>,
            limit: usize,
        ) -> Result<Vec<ToolExecutionRecord>> {
            Ok(self
                .tool_execs
                .lock()
                .unwrap()
                .iter()
                .filter(|r| agent_id.map_or(true, |a| r.agent_id == a))
                .take(limit)
                .cloned()
                .collect())
        }
        fn get_setting(&self, key: &str) -> Result<Option<String>> {
            Ok(self.settings.lock().unwrap().get(key).cloned())
        }
        fn set_setting(&self, key: &str, value: &str) -> Result<()> {
            self.settings
                .lock()
                .unwrap()
                .insert(key.to_string(), value.to_string());
            Ok(())
        }
        fn list_settings(&self) -> Result<Vec<(String, String)>> {
            Ok(self
                .settings
                .lock()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect())
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
        fn upsert_approval(&self, approval: &ApprovalRecord) -> Result<()> {
            self.approvals
                .lock()
                .unwrap()
                .insert(approval.id.clone(), approval.clone());
            Ok(())
        }
        fn get_approval(&self, approval_id: &str) -> Result<Option<ApprovalRecord>> {
            Ok(self.approvals.lock().unwrap().get(approval_id).cloned())
        }
        fn list_approvals(&self, pending_only: bool, limit: usize) -> Result<Vec<ApprovalRecord>> {
            Ok(self
                .approvals
                .lock()
                .unwrap()
                .values()
                .filter(|a| !pending_only || a.status == ApprovalStatus::Pending)
                .take(limit)
                .cloned()
                .collect())
        }
        fn append_audit_log(&self, entry: &AuditLogEntry) -> Result<()> {
            self.audits.lock().unwrap().push(entry.clone());
            Ok(())
        }
        fn list_audit_logs(&self, limit: usize) -> Result<Vec<AuditLogEntry>> {
            Ok(self.audits.lock().unwrap().iter().take(limit).cloned().collect())
        }
        fn upsert_schedule(&self, schedule: &ScheduleRecord) -> Result<()> {
            self.schedules
                .lock()
                .unwrap()
                .insert(schedule.id.clone(), schedule.clone());
            Ok(())
        }
        fn get_schedule(&self, id: &str) -> Result<Option<ScheduleRecord>> {
            Ok(self.schedules.lock().unwrap().get(id).cloned())
        }
        fn list_schedules(&self, limit: usize) -> Result<Vec<ScheduleRecord>> {
            let mut items: Vec<ScheduleRecord> =
                self.schedules.lock().unwrap().values().cloned().collect();
            items.sort_by(|a, b| {
                b.created_at
                    .cmp(&a.created_at)
                    .then_with(|| a.id.cmp(&b.id))
            });
            let cap = if limit == 0 { 1000 } else { limit.min(1000) };
            items.truncate(cap);
            Ok(items)
        }
        fn list_due_schedules(
            &self,
            now: chrono::DateTime<chrono::Utc>,
            limit: usize,
        ) -> Result<Vec<ScheduleRecord>> {
            let mut due: Vec<ScheduleRecord> = self
                .schedules
                .lock()
                .unwrap()
                .values()
                .filter(|s| s.enabled && s.next_run_at.is_some_and(|t| t <= now))
                .cloned()
                .collect();
            due.sort_by(|a, b| {
                a.next_run_at
                    .cmp(&b.next_run_at)
                    .then_with(|| a.id.cmp(&b.id))
            });
            let cap = if limit == 0 { 64 } else { limit.min(512) };
            due.truncate(cap);
            Ok(due)
        }
        fn delete_schedule(&self, id: &str) -> Result<()> {
            self.schedules.lock().unwrap().remove(id);
            Ok(())
        }
    }

    type ScriptedFn = Box<dyn Fn(&InferenceRequest) -> InferenceResponse + Send + Sync>;

    struct ScriptedInferenceProvider {
        responses: Mutex<VecDeque<ScriptedFn>>,
        recorded_agents: Mutex<Vec<String>>,
        recorded_requests: Mutex<Vec<InferenceRequest>>,
        prefill_calls: AtomicUsize,
    }

    impl ScriptedInferenceProvider {
        fn new() -> Self {
            Self {
                responses: Mutex::new(VecDeque::new()),
                recorded_agents: Mutex::new(Vec::new()),
                recorded_requests: Mutex::new(Vec::new()),
                prefill_calls: AtomicUsize::new(0),
            }
        }

        fn push_fn<F>(&self, f: F)
        where
            F: Fn(&InferenceRequest) -> InferenceResponse + Send + Sync + 'static,
        {
            self.responses.lock().unwrap().push_back(Box::new(f));
        }

        fn push_text(&self, content: &str) {
            let text = content.to_string();
            self.push_fn(move |_| InferenceResponse {
                content: text.clone(),
                reasoning: Some("thinking".into()),
                tool_calls: Vec::new(),
                model: "mock-ps5lm".into(),
                finish_reason: Some("stop".into()),
                usage: TokenUsage::default(),
                latency_ms: 5,
            });
        }

        fn push_tool_calls(&self, calls: Vec<ToolCallRequest>) {
            self.push_fn(move |_| InferenceResponse {
                content: String::new(),
                reasoning: Some("need tool".into()),
                tool_calls: calls.clone(),
                model: "mock-ps5lm".into(),
                finish_reason: Some("tool_calls".into()),
                usage: TokenUsage::default(),
                latency_ms: 5,
            });
        }

        fn called_system_prompts(&self) -> Vec<String> {
            self.recorded_agents.lock().unwrap().clone()
        }

        fn recorded_requests(&self) -> Vec<InferenceRequest> {
            self.recorded_requests.lock().unwrap().clone()
        }

        fn prefill_count(&self) -> usize {
            self.prefill_calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl InferenceProvider for ScriptedInferenceProvider {
        async fn generate(
            &self,
            req: InferenceRequest,
            _cancel: CancellationToken,
        ) -> Result<InferenceResponse> {
            if let Some(sys) = req.messages.first() {
                self.recorded_agents.lock().unwrap().push(sys.content.clone());
            }
            self.recorded_requests.lock().unwrap().push(req.clone());
            let f = self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .ok_or_else(|| AgentError::Inference("no scripted response remaining".into()))?;
            Ok(f(&req))
        }

        async fn generate_stream(
            &self,
            req: InferenceRequest,
            tx: mpsc::Sender<InferenceStreamChunk>,
            cancel: CancellationToken,
        ) -> Result<InferenceResponse> {
            let resp = self.generate(req, cancel).await?;
            if let Some(ref r) = resp.reasoning {
                let _ = tx.send(InferenceStreamChunk::ReasoningDelta(r.clone())).await;
            }
            if !resp.content.is_empty() {
                let _ = tx
                    .send(InferenceStreamChunk::TextDelta(resp.content.clone()))
                    .await;
            }
            let _ = tx.send(InferenceStreamChunk::Completed(resp.clone())).await;
            Ok(resp)
        }

        async fn prefill_kv_cache(
            &self,
            _messages: Vec<InferenceMessage>,
            _model_override: Option<String>,
            _cancel: CancellationToken,
        ) -> Result<()> {
            self.prefill_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn health_check(&self) -> ProviderHealth {
            ProviderHealth {
                reachable: true,
                loaded_model: Some("mock-ps5lm".into()),
                available_models: vec!["mock-ps5lm".into()],
                error: None,
            }
        }

        async fn list_models(&self) -> Result<Vec<String>> {
            Ok(vec!["mock-ps5lm".into()])
        }
    }

    struct MockToolExecutor {
        specs: Vec<ToolSpec>,
        executed_count: AtomicUsize,
        deleted_files: Mutex<Vec<String>>,
    }

    impl MockToolExecutor {
        fn default_suite() -> Self {
            let make_spec = |id: &str, mutating: bool, approval: bool| ToolSpec {
                id: id.into(),
                name: id.into(),
                category: "test".into(),
                description: format!("mock {id}"),
                input_schema: serde_json::json!({ "type": "object" }),
                output_schema: serde_json::json!({ "type": "object" }),
                required_permissions: vec![],
                requires_approval: approval,
                is_mutating: mutating,
                enabled: true,
                timeout_secs: 10,
            };
            Self {
                specs: vec![
                    make_spec("datetime", false, false),
                    make_spec("system_info", false, false),
                    make_spec("ps5_info", false, false),
                    make_spec("web_search", false, false),
                    make_spec("file_read", false, false),
                    make_spec("calculator", false, false),
                    make_spec("file_write", true, false),
                    make_spec("file_delete", true, true),
                ],
                executed_count: AtomicUsize::new(0),
                deleted_files: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl RuntimeToolExecutor for MockToolExecutor {
        async fn list_tools(&self) -> Vec<ToolSpec> {
            self.specs.clone()
        }

        async fn get_tool(&self, tool_id: &str) -> Option<ToolSpec> {
            self.specs.iter().find(|s| s.id == tool_id).cloned()
        }

        async fn execute_tool(
            &self,
            _agent: &AgentDefinition,
            _task_id: Option<&str>,
            tool_id: &str,
            arguments: serde_json::Value,
            approved_bypass: bool,
            _cancel: CancellationToken,
        ) -> Result<ToolExecutionOutput> {
            let spec = self
                .get_tool(tool_id)
                .await
                .ok_or_else(|| AgentError::NotFound(format!("tool '{tool_id}' not found")))?;
            if spec.requires_approval && !approved_bypass {
                return Err(AgentError::ApprovalRequired {
                    approval_id: new_id(),
                    reason: format!("tool '{tool_id}' requires approval"),
                });
            }
            self.executed_count.fetch_add(1, Ordering::SeqCst);
            if tool_id == "file_delete" {
                let path = arguments
                    .get("path")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                self.deleted_files.lock().unwrap().push(path.clone());
                return Ok(ToolExecutionOutput {
                    output: serde_json::json!({ "deleted": path }),
                    is_mutating: true,
                });
            }
            Ok(ToolExecutionOutput {
                output: serde_json::json!({ "tool": tool_id, "ok": true, "args": arguments }),
                is_mutating: spec.is_mutating,
            })
        }
    }

    fn setup_runtime(
        inference: Arc<ScriptedInferenceProvider>,
        tools: Arc<MockToolExecutor>,
        cfg_override: Option<AppConfig>,
    ) -> (Arc<InMemoryStorage>, Arc<AgentRuntime>) {
        let storage = Arc::new(InMemoryStorage::default());
        let cfg = cfg_override.unwrap_or_default();
        let (events_tx, _) = broadcast::channel(256);
        let agent_mgr = Arc::new(AgentManager::new(
            storage.clone(),
            cfg.agents.clone(),
            events_tx.clone(),
        ));
        agent_mgr.seed_defaults().unwrap();
        let bus = Arc::new(MessageBus::new(
            storage.clone(),
            (&cfg.agents).into(),
            events_tx.clone(),
        ));
        let rt = AgentRuntime::new(
            storage.clone(),
            cfg,
            inference,
            tools,
            None,
            agent_mgr,
            bus,
            events_tx,
        );
        (storage, rt)
    }

    fn create_conv(storage: &InMemoryStorage, id: &str, mode: ConversationMode) {
        storage
            .create_conversation(&Conversation {
                id: id.into(),
                title: "test".into(),
                mode,
                created_at: now_iso(),
                updated_at: now_iso(),
                archived: false,
                sort_order: 0,
            })
            .unwrap();
    }

    #[tokio::test]
    async fn single_commander_turn_with_tool_call_and_persistence() {
        let inf = Arc::new(ScriptedInferenceProvider::new());
        inf.push_tool_calls(vec![ToolCallRequest {
            id: "call-1".into(),
            name: "ps5_info".into(),
            arguments: serde_json::json!({}),
        }]);
        inf.push_text("PS5 status checked and ready.");

        let tools = Arc::new(MockToolExecutor::default_suite());
        let (storage, rt) = setup_runtime(inf, tools.clone(), None);
        create_conv(
            &storage,
            "conv-1",
            ConversationMode::Direct {
                agent_id: "commander".into(),
            },
        );
        let mut rx = rt.events();

        let res = rt
            .chat_commander("conv-1", "Check PS5 info", CancellationToken::new())
            .await
            .expect("commander turn");

        assert_eq!(res.status, TaskStatus::Completed);
        assert_eq!(res.agent_id, "commander");
        assert_eq!(res.content, "PS5 status checked and ready.");
        assert_eq!(tools.executed_count.load(Ordering::SeqCst), 1);

        let msgs = storage.list_chat_messages("conv-1", false, 20).unwrap();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].role, ChatRole::User);
        assert_eq!(msgs[1].role, ChatRole::Assistant);
        assert_eq!(msgs[1].agent_id.as_deref(), Some("commander"));

        let execs = storage.list_tool_executions(Some("commander"), 10).unwrap();
        assert_eq!(execs.len(), 1);
        assert!(execs[0].success);

        let mut saw_token = false;
        let mut saw_tool_started = false;
        let mut saw_tool_finished = false;
        let mut saw_completed = false;
        while let Ok(ev) = rx.try_recv() {
            match ev {
                StreamEvent::ChatToken { .. } => saw_token = true,
                StreamEvent::ToolCallStarted { tool_id, .. } if tool_id == "ps5_info" => {
                    saw_tool_started = true
                }
                StreamEvent::ToolCallFinished { tool_id, success, .. }
                    if tool_id == "ps5_info" && success =>
                {
                    saw_tool_finished = true
                }
                StreamEvent::ChatCompleted { .. } => saw_completed = true,
                _ => {}
            }
        }
        assert!(saw_token && saw_tool_started && saw_tool_finished && saw_completed);
    }

    #[tokio::test]
    async fn five_agent_delegation_and_a2a_loop_protection() {
        let inf = Arc::new(ScriptedInferenceProvider::new());
        // Commander delegates to all 4 specialists in parallel/batch
        inf.push_tool_calls(vec![
            ToolCallRequest {
                id: "d1".into(),
                name: "delegate_task".into(),
                arguments: serde_json::json!({ "agent_id": "researcher", "task": "research" }),
            },
            ToolCallRequest {
                id: "d2".into(),
                name: "delegate_task".into(),
                arguments: serde_json::json!({ "agent_id": "programmer", "task": "code" }),
            },
            ToolCallRequest {
                id: "d3".into(),
                name: "delegate_task".into(),
                arguments: serde_json::json!({ "agent_id": "analyst", "task": "analyze" }),
            },
            ToolCallRequest {
                id: "d4".into(),
                name: "delegate_task".into(),
                arguments: serde_json::json!({ "agent_id": "executor", "task": "verify" }),
            },
        ]);
        inf.push_text("Research complete");
        inf.push_text("Code ready");
        inf.push_text("Analysis done");
        inf.push_text("Execution verified");
        inf.push_text("All 4 specialists finished.");

        let tools = Arc::new(MockToolExecutor::default_suite());
        let (storage, rt) = setup_runtime(inf, tools, None);
        create_conv(
            &storage,
            "conv-5",
            ConversationMode::Direct {
                agent_id: "commander".into(),
            },
        );

        let res = rt
            .handle_chat_message(
                "conv-5",
                "Run full 5-agent workflow",
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(res.status, TaskStatus::Completed);
        assert_eq!(res.content, "All 4 specialists finished.");

        let subtasks = rt.task_manager().list_subtasks(&res.task_id).unwrap();
        assert_eq!(subtasks.len(), 4);
        let envelopes = rt
            .message_bus()
            .list_messages(Some(&res.task_id), None, 50)
            .unwrap();
        // 4 TaskAssignment + 4 TaskCompleted = 8 envelopes
        assert_eq!(envelopes.len(), 8);

        // Verify A2A loop protection during runtime: > 3 identical messages in same task blocked
        for i in 0..3 {
            rt.message_bus()
                .send(SendMessageRequest {
                    sender_agent_id: "researcher".into(),
                    recipient_agent_id: "analyst".into(),
                    task_id: Some("loop-task".into()),
                    message_type: AgentMessageType::InformationRequest,
                    content: format!("ping {i}"),
                    hop_count: 1,
                })
                .unwrap();
        }
        let blocked = rt
            .message_bus()
            .send(SendMessageRequest {
                sender_agent_id: "researcher".into(),
                recipient_agent_id: "analyst".into(),
                task_id: Some("loop-task".into()),
                message_type: AgentMessageType::InformationRequest,
                content: "ping 4".into(),
                hop_count: 1,
            })
            .unwrap_err();
        assert!(matches!(blocked, AgentError::LimitExceeded(_)));
    }

    #[tokio::test]
    async fn group_chat_coordinator_skips_irrelevant_participants() {
        let inf = Arc::new(ScriptedInferenceProvider::new());
        // 1) Commander coordinator chooses only programmer
        inf.push_text(r#"{"selected_agents":["programmer"],"direct_answer":null}"#);
        // 2) Single-specialist fast path: Programmer runs directly without A2A or synthesis
        inf.push_text("Here is the Rust patch.");

        let tools = Arc::new(MockToolExecutor::default_suite());
        let (storage, rt) = setup_runtime(inf.clone(), tools, None);
        create_conv(
            &storage,
            "conv-group",
            ConversationMode::Group {
                participant_agent_ids: vec![
                    "researcher".into(),
                    "programmer".into(),
                    "analyst".into(),
                    "executor".into(),
                ],
            },
        );

        let res = rt
            .handle_chat_message(
                "conv-group",
                "Write a Rust helper function",
                CancellationToken::new(),
            )
            .await
            .unwrap();

        assert_eq!(res.status, TaskStatus::Completed);
        assert_eq!(res.agent_id, "programmer");
        assert_eq!(res.content, "Here is the Rust patch.");
        assert_eq!(res.selected_participants, vec!["programmer".to_string()]);
        // Exactly 2 inference calls total: Coordinator + Programmer (no Commander synthesis).
        // Researcher, Analyst, Executor triggered 0 model calls and 0 A2A envelopes!
        let prompts = inf.called_system_prompts();
        assert_eq!(prompts.len(), 2);
        assert!(prompts[1].contains("Programmer"));
        assert!(storage.envelopes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn approval_pause_resume_and_deny_flow() {
        // Part A: Approve -> resumes and executes tool
        let inf = Arc::new(ScriptedInferenceProvider::new());
        inf.push_tool_calls(vec![ToolCallRequest {
            id: "del-1".into(),
            name: "file_delete".into(),
            arguments: serde_json::json!({ "path": "workspace/old.txt" }),
        }]);
        inf.push_text("Deleted workspace/old.txt after approval.");

        let tools = Arc::new(MockToolExecutor::default_suite());
        let (storage, rt) = setup_runtime(inf, tools.clone(), None);
        create_conv(
            &storage,
            "conv-app",
            ConversationMode::Direct {
                agent_id: "executor".into(),
            },
        );

        let paused = rt
            .handle_chat_message("conv-app", "Delete old.txt", CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(paused.status, TaskStatus::Waiting);
        let approval_id = paused.pending_approval_id.expect("approval id");
        assert!(tools.deleted_files.lock().unwrap().is_empty());

        let resolved = rt
            .resolve_approval(&approval_id, true, "admin")
            .await
            .unwrap();
        assert_eq!(resolved.task_status, Some(TaskStatus::Completed));
        assert_eq!(
            resolved.result.as_deref(),
            Some("Deleted workspace/old.txt after approval.")
        );
        assert_eq!(
            tools.deleted_files.lock().unwrap().as_slice(),
            &["workspace/old.txt".to_string()]
        );

        // Part B: Deny -> tool never executes, task fails cleanly
        let inf2 = Arc::new(ScriptedInferenceProvider::new());
        inf2.push_tool_calls(vec![ToolCallRequest {
            id: "del-2".into(),
            name: "file_delete".into(),
            arguments: serde_json::json!({ "path": "workspace/keep.txt" }),
        }]);
        let tools2 = Arc::new(MockToolExecutor::default_suite());
        let (storage2, rt2) = setup_runtime(inf2, tools2.clone(), None);
        create_conv(
            &storage2,
            "conv-deny",
            ConversationMode::Direct {
                agent_id: "executor".into(),
            },
        );

        let paused2 = rt2
            .handle_chat_message("conv-deny", "Delete keep.txt", CancellationToken::new())
            .await
            .unwrap();
        let aid2 = paused2.pending_approval_id.unwrap();
        let denied = rt2.resolve_approval(&aid2, false, "admin").await.unwrap();
        assert!(!denied.approved);
        assert_eq!(denied.task_status, Some(TaskStatus::Failed));
        assert!(tools2.deleted_files.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn restart_recovery_resumes_readonly_and_pauses_side_effecting_tasks() {
        let inf = Arc::new(ScriptedInferenceProvider::new());
        inf.push_text("Read-only task resumed cleanly.");

        let tools = Arc::new(MockToolExecutor::default_suite());
        let (storage, rt) = setup_runtime(inf, tools, None);

        // Seed interrupted tasks directly in storage as if crash happened mid-run
        storage
            .upsert_task(&TaskRecord {
                id: "task-ro".into(),
                conversation_id: None,
                user_request: "Check status".into(),
                assigned_agent_id: "researcher".into(),
                parent_task_id: None,
                priority: 70,
                created_at: now_iso(),
                started_at: Some(now_iso()),
                completed_at: None,
                status: TaskStatus::Running,
                result: None,
                error_info: None,
                has_side_effects: false,
                progress_pct: 30,
                progress_note: None,
            })
            .unwrap();

        storage
            .upsert_task(&TaskRecord {
                id: "task-mut".into(),
                conversation_id: None,
                user_request: "Modify files".into(),
                assigned_agent_id: "executor".into(),
                parent_task_id: None,
                priority: 60,
                created_at: now_iso(),
                started_at: Some(now_iso()),
                completed_at: None,
                status: TaskStatus::Running,
                result: None,
                error_info: None,
                has_side_effects: true,
                progress_pct: 60,
                progress_note: None,
            })
            .unwrap();

        let report = rt.recover_on_startup().await.unwrap();
        assert_eq!(report.resumed_tasks.len(), 1);
        assert_eq!(report.resumed_tasks[0].id, "task-ro");
        assert_eq!(report.paused_side_effect_tasks.len(), 1);
        assert_eq!(report.paused_side_effect_tasks[0].id, "task-mut");

        // Side-effecting task must stay WAITING and not auto-replay
        let mut_task = storage.get_task("task-mut").unwrap().unwrap();
        assert_eq!(mut_task.status, TaskStatus::Waiting);

        // Wait briefly for spawned read-only task to finish
        for _ in 0..40 {
            if storage.get_task("task-ro").unwrap().unwrap().status == TaskStatus::Completed {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(15)).await;
        }
        let ro_task = storage.get_task("task-ro").unwrap().unwrap();
        assert_eq!(ro_task.status, TaskStatus::Completed);
        assert_eq!(
            ro_task.result.as_deref(),
            Some("Read-only task resumed cleanly.")
        );
    }

    #[tokio::test]
    async fn dynamic_agent_crud_and_commander_delete_rejection_integrated() {
        let inf = Arc::new(ScriptedInferenceProvider::new());
        let tools = Arc::new(MockToolExecutor::default_suite());
        let mut cfg = AppConfig::default();
        cfg.agents.max_agents = 6;
        let (_storage, rt) = setup_runtime(inf, tools, Some(cfg));

        // Create -> Update -> Disable -> Enable -> Delete
        let created = rt
            .agent_manager()
            .create_agent(
                ps5_agent_manager::CreateAgentInput {
                    id: Some("qa-bot".into()),
                    name: "QA Bot".into(),
                    description: "Verifies outputs".into(),
                    role: AgentRole::Custom,
                    role_label: Some("QA".into()),
                    system_prompt: "Check quality.".into(),
                    model_config: None,
                    enabled_tools: vec!["file_read".into()],
                    denied_tools: None,
                    sandbox_mode: None,
                    tool_profile: None,
                    memory_permissions: None,
                    max_task_duration_secs: None,
                    max_reasoning_iterations: None,
                    priority: Some(50),
                    can_communicate: Some(true),
                    enabled: Some(true),
                    is_temporary: false,
                    spawned_by: None,
                },
                "admin",
            )
            .unwrap();
        assert_eq!(created.id, "qa-bot");

        let updated = rt
            .agent_manager()
            .update_agent(
                "qa-bot",
                ps5_agent_manager::UpdateAgentInput {
                    name: Some("QA Bot Pro".into()),
                    ..Default::default()
                },
                "admin",
            )
            .unwrap();
        assert_eq!(updated.name, "QA Bot Pro");

        assert!(!rt.agent_manager().set_enabled("qa-bot", false, "admin").unwrap().enabled);
        assert!(rt.agent_manager().set_enabled("qa-bot", true, "admin").unwrap().enabled);

        assert!(matches!(
            rt.agent_manager()
                .delete_agent("commander", DeleteAgentOptions::default())
                .await,
            Err(AgentError::Protected(_))
        ));

        assert!(rt
            .agent_manager()
            .delete_agent(
                "qa-bot",
                DeleteAgentOptions {
                    active_task_policy: ActiveTaskPolicy::CancelActive,
                    delete_memory: true,
                    actor: "admin".into(),
                }
            )
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn enforces_max_reasoning_iterations_and_disallowed_tool_validation() {
        let inf = Arc::new(ScriptedInferenceProvider::new());
        // Always emit a disallowed tool (file_delete is NOT in researcher's allowlist)
        for i in 0..3 {
            inf.push_tool_calls(vec![ToolCallRequest {
                id: format!("bad-{i}"),
                name: "file_delete".into(),
                arguments: serde_json::json!({ "path": "x" }),
            }]);
        }

        let tools = Arc::new(MockToolExecutor::default_suite());
        let mut cfg = AppConfig::default();
        cfg.agents.max_reasoning_iterations = 2;
        let (storage, rt) = setup_runtime(inf, tools.clone(), Some(cfg));
        create_conv(
            &storage,
            "conv-iter",
            ConversationMode::Direct {
                agent_id: "researcher".into(),
            },
        );

        let err = rt
            .handle_chat_message("conv-iter", "Try deleting", CancellationToken::new())
            .await
            .unwrap_err();
        assert!(matches!(err, AgentError::LimitExceeded(_)));
        assert!(tools.deleted_files.lock().unwrap().is_empty());

        let execs = storage.list_tool_executions(Some("researcher"), 10).unwrap();
        assert_eq!(execs.len(), 2);
        assert!(execs.iter().all(|e| !e.success));
    }

    #[tokio::test]
    async fn task_manager_cancellation_cascades_to_subtasks() {
        let inf = Arc::new(ScriptedInferenceProvider::new());
        let tools = Arc::new(MockToolExecutor::default_suite());
        let (_storage, rt) = setup_runtime(inf, tools, None);

        let parent = rt
            .task_manager()
            .create_task(
                "parent".into(),
                "commander".into(),
                None,
                None,
                100,
                TaskStatus::Running,
            )
            .unwrap();
        let child = rt
            .task_manager()
            .create_task(
                "child".into(),
                "researcher".into(),
                None,
                Some(parent.id.clone()),
                70,
                TaskStatus::Running,
            )
            .unwrap();

        let p_tok = rt.task_manager().register_token(&parent.id, None);
        let c_tok = rt.task_manager().register_token(&child.id, Some(&p_tok));

        rt.task_manager()
            .cancel_task(&parent.id, "user abort")
            .unwrap();
        assert!(p_tok.is_cancelled());
        assert!(c_tok.is_cancelled());
        assert_eq!(
            rt.task_manager().get_task(&parent.id).unwrap().unwrap().status,
            TaskStatus::Cancelled
        );
        assert_eq!(
            rt.task_manager().get_task(&child.id).unwrap().unwrap().status,
            TaskStatus::Cancelled
        );
    }

    #[tokio::test]
    async fn wildcard_enabled_tools_matches_mcp_plugin_and_star() {
        assert!(tool_pattern_matches(&["*".into()], "anything:here"));
        assert!(tool_pattern_matches(&["mcp:*".into()], "mcp:github:list_issues"));
        assert!(!tool_pattern_matches(&["mcp:*".into()], "plugin:foo:bar"));
        assert!(tool_pattern_matches(&["plugin:alpha:*".into()], "plugin:alpha:run"));

        let inf = Arc::new(ScriptedInferenceProvider::new());
        inf.push_tool_calls(vec![ToolCallRequest {
            id: "mcp-1".into(),
            name: "mcp:srv:ping".into(),
            arguments: serde_json::json!({ "ok": true }),
        }]);
        inf.push_text("MCP ping succeeded.");

        let mut tools = MockToolExecutor::default_suite();
        tools.specs.push(ToolSpec {
            id: "mcp:srv:ping".into(),
            name: "ping".into(),
            category: "mcp".into(),
            description: "MCP ping".into(),
            input_schema: serde_json::json!({ "type": "object" }),
            output_schema: serde_json::json!({ "type": "object" }),
            required_permissions: vec![],
            requires_approval: false,
            is_mutating: false,
            enabled: true,
            timeout_secs: 10,
        });
        let tools = Arc::new(tools);
        let (storage, rt) = setup_runtime(inf, tools.clone(), None);
        create_conv(
            &storage,
            "conv-mcp",
            ConversationMode::Direct {
                agent_id: "programmer".into(),
            },
        );

        let res = rt
            .handle_chat_message("conv-mcp", "Ping MCP server", CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(res.status, TaskStatus::Completed);
        assert_eq!(res.content, "MCP ping succeeded.");
        assert_eq!(tools.executed_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn zero_limits_hot_swap_and_no_child_semaphore_deadlock() {
        let inf = Arc::new(ScriptedInferenceProvider::new());
        inf.push_tool_calls(vec![ToolCallRequest {
            id: "del-child".into(),
            name: "delegate_task".into(),
            arguments: serde_json::json!({ "agent_id": "researcher", "task": "check" }),
        }]);
        inf.push_text("Child finished without deadlock.");
        inf.push_text("Parent finished.");

        let tools = Arc::new(MockToolExecutor::default_suite());
        let mut cfg = AppConfig::default();
        cfg.agents.max_active_tasks = 1;
        cfg.agents.max_concurrent_inference = 0;
        cfg.agents.max_reasoning_iterations = 0;
        cfg.agents.default_task_timeout_secs = 0;
        let (storage, rt) = setup_runtime(inf, tools, Some(cfg.clone()));

        // Hot-swap to 0 = unlimited task semaphore as well
        cfg.agents.max_active_tasks = 0;
        rt.update_config(cfg);

        // Also explicitly test child task via run_task_by_id when max_active_tasks = 1
        let mut tight_cfg = rt.config();
        tight_cfg.agents.max_active_tasks = 1;
        rt.update_config(tight_cfg);

        create_conv(
            &storage,
            "conv-child",
            ConversationMode::Direct {
                agent_id: "commander".into(),
            },
        );

        let res = rt
            .handle_chat_message("conv-child", "Delegate work", CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(res.status, TaskStatus::Completed);
        assert_eq!(res.content, "Parent finished.");
    }

    #[tokio::test]
    async fn slash_mention_natural_routing_and_background_dispatch() {
        let inf = Arc::new(ScriptedInferenceProvider::new());
        let observed_user_msgs = Arc::new(Mutex::new(Vec::<String>::new()));
        let obs_clone = observed_user_msgs.clone();
        inf.push_fn(move |req| {
            if let Some(last_user) = req.messages.iter().rev().find(|m| m.role == "user") {
                obs_clone.lock().unwrap().push(last_user.content.clone());
            }
            InferenceResponse {
                content: "I built it.".into(),
                reasoning: None,
                tool_calls: Vec::new(),
                model: "mock-ps5lm".into(),
                finish_reason: Some("stop".into()),
                usage: TokenUsage::default(),
                latency_ms: 4,
            }
        });

        let tools = Arc::new(MockToolExecutor::default_suite());
        let (storage, rt) = setup_runtime(inf.clone(), tools, None);
        create_conv(
            &storage,
            "conv-route",
            ConversationMode::Group {
                participant_agent_ids: vec!["researcher".into(), "programmer".into()],
            },
        );

        let res = rt
            .handle_chat_message_with_options(
                "conv-route",
                "/agent programmer build flappy-bird.html",
                None,
                Some(ThinkingLevel::High),
                Vec::new(),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(res.agent_id, "programmer");
        assert_eq!(inf.called_system_prompts().len(), 1);
        let stored = storage.list_chat_messages("conv-route", false, 10).unwrap();
        assert_eq!(stored[0].content, "/agent programmer build flappy-bird.html");
        let sent_user = &observed_user_msgs.lock().unwrap()[0];
        assert!(sent_user.contains("The user asks you directly: build flappy-bird.html"));

        // Background dispatch resolves @researcher synchronously on TaskRecord
        inf.push_text("Researcher background reply.");
        let (user_msg, task) = rt
            .dispatch_chat_background(
                "conv-route".into(),
                "@researcher inspect logs".into(),
                None,
                None,
                Vec::new(),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(user_msg.content, "@researcher inspect logs");
        assert_eq!(task.assigned_agent_id, "researcher");

        for _ in 0..50 {
            if storage
                .get_task(&task.id)
                .unwrap()
                .unwrap()
                .status
                .is_terminal()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let done_task = storage.get_task(&task.id).unwrap().unwrap();
        assert_eq!(done_task.status, TaskStatus::Completed);
        assert_eq!(done_task.result.as_deref(), Some("Researcher background reply."));
    }

    #[tokio::test]
    async fn auto_continuation_attachments_and_file_first_safeguard() {
        let inf = Arc::new(ScriptedInferenceProvider::new());
        let captured_req = Arc::new(Mutex::new(Vec::<InferenceRequest>::new()));
        let cap1 = captured_req.clone();
        inf.push_fn(move |req| {
            cap1.lock().unwrap().push(req.clone());
            InferenceResponse {
                content: "Part 1 ".into(),
                reasoning: None,
                tool_calls: Vec::new(),
                model: "mock-ps5lm".into(),
                finish_reason: Some("length".into()),
                usage: TokenUsage::default(),
                latency_ms: 4,
            }
        });
        let long_html = (0..22)
            .map(|i| format!("  <div>line {i}</div>"))
            .collect::<Vec<_>>()
            .join("\n");
        let part2 = format!("Part 2\n```html\n<!doctype html>\n<html>\n{long_html}\n</html>\n```");
        inf.push_text(&part2);

        let mut tools = MockToolExecutor::default_suite();
        tools.specs.push(ToolSpec {
            id: "write_file".into(),
            name: "write_file".into(),
            category: "files".into(),
            description: "Write workspace file".into(),
            input_schema: serde_json::json!({ "type": "object" }),
            output_schema: serde_json::json!({ "type": "object" }),
            required_permissions: vec![],
            requires_approval: false,
            is_mutating: true,
            enabled: true,
            timeout_secs: 10,
        });
        let tools = Arc::new(tools);
        let mut cfg = AppConfig::default();
        cfg.security.require_approval_for_mutating = false;
        let (storage, rt) = setup_runtime(inf, tools.clone(), Some(cfg));
        let mut rx = rt.events();

        create_conv(
            &storage,
            "conv-cont",
            ConversationMode::Direct {
                agent_id: "programmer".into(),
            },
        );

        let att = ChatAttachment {
            kind: "image".into(),
            name: "sketch.png".into(),
            mime_type: "image/png".into(),
            data_url: Some("data:image/png;base64,AAAA".into()),
            workspace_path: None,
            size_bytes: 4,
        };
        let res = rt
            .handle_chat_message_with_options(
                "conv-cont",
                "Create a flappy bird html game",
                None,
                Some(ThinkingLevel::Max),
                vec![att],
                CancellationToken::new(),
            )
            .await
            .unwrap();

        assert_eq!(res.status, TaskStatus::Completed);
        assert!(res.content.contains("Part 1 Part 2"));
        assert!(res.content.contains("flappy-bird.html"));
        assert_eq!(tools.executed_count.load(Ordering::SeqCst), 1);

        let reqs = captured_req.lock().unwrap();
        assert_eq!(reqs[0].thinking_level, ThinkingLevel::Max);
        assert_eq!(
            reqs[0].messages.last().unwrap().image_urls,
            vec!["data:image/png;base64,AAAA".to_string()]
        );

        let mut saw_cont = false;
        let mut saw_files_changed = false;
        while let Ok(ev) = rx.try_recv() {
            match ev {
                StreamEvent::ContinuationTriggered { .. } => saw_cont = true,
                StreamEvent::FilesChanged { path, action, .. }
                    if path == "flappy-bird.html" && action == "write" =>
                {
                    saw_files_changed = true
                }
                _ => {}
            }
        }
        assert!(saw_cont && saw_files_changed);
    }

    #[tokio::test]
    async fn pending_eviction_and_temp_agent_auto_cleanup() {
        let inf = Arc::new(ScriptedInferenceProvider::new());
        inf.push_tool_calls(vec![ToolCallRequest {
            id: "del-evict".into(),
            name: "file_delete".into(),
            arguments: serde_json::json!({ "path": "tmp.txt" }),
        }]);

        let tools = Arc::new(MockToolExecutor::default_suite());
        let (storage, rt) = setup_runtime(inf.clone(), tools, None);
        create_conv(
            &storage,
            "conv-evict",
            ConversationMode::Direct {
                agent_id: "executor".into(),
            },
        );

        let paused = rt
            .handle_chat_message("conv-evict", "Delete tmp", CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(paused.status, TaskStatus::Waiting);
        assert_eq!(rt.pending_approval_count(), 1);
        assert_eq!(rt.evict_pending_for_task(&paused.task_id), 1);
        assert_eq!(rt.pending_approval_count(), 0);

        // Temp specialist auto-cleaned after task completes
        let temp = rt
            .agent_manager()
            .create_agent(
                ps5_agent_manager::CreateAgentInput {
                    id: Some("temp-helper".into()),
                    name: "Temp Helper".into(),
                    description: "Ephemeral".into(),
                    role: AgentRole::Custom,
                    role_label: None,
                    system_prompt: "Help briefly.".into(),
                    model_config: None,
                    enabled_tools: vec!["file_read".into()],
                    denied_tools: None,
                    sandbox_mode: None,
                    tool_profile: None,
                    memory_permissions: None,
                    max_task_duration_secs: None,
                    max_reasoning_iterations: None,
                    priority: None,
                    can_communicate: None,
                    enabled: Some(true),
                    is_temporary: true,
                    spawned_by: Some("commander".into()),
                },
                "commander",
            )
            .unwrap();
        assert!(rt.agent_manager().get_agent(&temp.id).unwrap().is_some());

        inf.push_text("Temp task done.");
        let task = rt
            .submit_task("Do quick job".into(), temp.id.clone(), None, None)
            .await
            .unwrap();
        for _ in 0..50 {
            if storage
                .get_task(&task.id)
                .unwrap()
                .unwrap()
                .status
                .is_terminal()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(rt.agent_manager().get_agent(&temp.id).unwrap().is_none());
    }

    #[tokio::test]
    async fn chat_completed_reasoning_and_thinking_progress_labels() {
        let inf = Arc::new(ScriptedInferenceProvider::new());
        inf.push_tool_calls(vec![ToolCallRequest {
            id: "c-info".into(),
            name: "ps5_info".into(),
            arguments: serde_json::json!({}),
        }]);
        inf.push_fn(|_| InferenceResponse {
            content: "Tool-assisted answer.".into(),
            reasoning: Some("deep trace".into()),
            tool_calls: Vec::new(),
            model: "mock-ps5lm".into(),
            finish_reason: Some("stop".into()),
            usage: TokenUsage::default(),
            latency_ms: 5,
        });
        inf.push_text("Off level answer.");

        let tools = Arc::new(MockToolExecutor::default_suite());
        let (storage, rt) = setup_runtime(inf, tools, None);
        let mut rx = rt.events();

        create_conv(
            &storage,
            "conv-think-labels",
            ConversationMode::Direct {
                agent_id: "commander".into(),
            },
        );

        let res_high = rt
            .handle_chat_message_with_options(
                "conv-think-labels",
                "Check status",
                None,
                Some(ThinkingLevel::High),
                Vec::new(),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(res_high.status, TaskStatus::Completed);

        let res_off = rt
            .handle_chat_message_with_options(
                "conv-think-labels",
                "Quick reply",
                None,
                Some(ThinkingLevel::Off),
                Vec::new(),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(res_off.status, TaskStatus::Completed);

        let mut completed_reasonings = Vec::new();
        let mut progress_notes = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            match ev {
                StreamEvent::ChatCompleted {
                    conversation_id,
                    reasoning,
                    ..
                } if conversation_id == "conv-think-labels" => {
                    completed_reasonings.push(reasoning);
                }
                StreamEvent::TaskUpdated {
                    conversation_id: Some(cid),
                    progress_note: Some(note),
                    ..
                } if cid == "conv-think-labels" => {
                    progress_notes.push(note);
                }
                _ => {}
            }
        }

        assert!(completed_reasonings
            .iter()
            .any(|r| r.as_deref() == Some("deep trace")));
        assert!(progress_notes.iter().any(|n| n == "Thinking (high)…"));
        assert!(progress_notes.iter().any(|n| n.starts_with("Tool call step 1")));
        assert!(progress_notes.iter().any(|n| n == "Generating response…"));
    }

    #[tokio::test]
    async fn script_transport_unclosed_fence_and_duplicate_cleanup() {
        // Part A: unclosed >= 10-line code block triggers continuation even on finish_reason=stop,
        // and auto-transports using SandboxApprovalPolicy::OnRequest when write_file runs without approval gates.
        let inf = Arc::new(ScriptedInferenceProvider::new());
        let first_half = (0..6)
            .map(|i| format!("    step_{i} = {i}"))
            .collect::<Vec<_>>()
            .join("\n");
        let second_half = (6..12)
            .map(|i| format!("    step_{i} = {i}"))
            .collect::<Vec<_>>()
            .join("\n");
        inf.push_text(&format!("Drafting helper:\n```python\ndef compute():\n{first_half}"));
        inf.push_text(&format!("\n{second_half}\n    return step_11\n```\nReady to run."));

        let mut tools = MockToolExecutor::default_suite();
        tools.specs.push(ToolSpec {
            id: "write_file".into(),
            name: "write_file".into(),
            category: "files".into(),
            description: "Write workspace file".into(),
            input_schema: serde_json::json!({ "type": "object" }),
            output_schema: serde_json::json!({ "type": "object" }),
            required_permissions: vec![],
            requires_approval: false,
            is_mutating: true,
            enabled: true,
            timeout_secs: 10,
        });
        let tools_a = Arc::new(tools);
        let mut cfg_a = AppConfig::default();
        cfg_a.security.require_approval_for_mutating = true;
        let (storage_a, rt_a) = setup_runtime(inf, tools_a.clone(), Some(cfg_a));
        let mut rx_a = rt_a.events();

        create_conv(
            &storage_a,
            "conv-unclosed",
            ConversationMode::Direct {
                agent_id: "programmer".into(),
            },
        );
        let res_a = rt_a
            .handle_chat_message(
                "conv-unclosed",
                "Create helper.py with a python helper",
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(res_a.status, TaskStatus::Completed);
        assert_eq!(tools_a.executed_count.load(Ordering::SeqCst), 1);
        assert!(res_a.content.contains("[FILE_CREATED:"));
        assert!(res_a.content.contains(".py]"));
        assert!(!res_a.content.contains("step_11 = 11"));

        let mut saw_unclosed_cont = false;
        let mut saw_py_write = false;
        while let Ok(ev) = rx_a.try_recv() {
            match ev {
                StreamEvent::ContinuationTriggered { reason, .. }
                    if reason == "unclosed_code_fence" =>
                {
                    saw_unclosed_cont = true;
                }
                StreamEvent::FilesChanged { path, action, .. }
                    if path.ends_with(".py") && action == "write" =>
                {
                    saw_py_write = true;
                }
                _ => {}
            }
        }
        assert!(saw_unclosed_cont && saw_py_write);

        // Part B: when write_file was already called via tool_calls, a trailing >= 15-line fenced
        // block in the final response is replaced by a concise Files-tab confirmation instead of duplicating.
        let inf_b = Arc::new(ScriptedInferenceProvider::new());
        inf_b.push_tool_calls(vec![ToolCallRequest {
            id: "w-1".into(),
            name: "write_file".into(),
            arguments: serde_json::json!({
                "path": "app.js",
                "content": "console.log('ready');\n"
            }),
        }]);
        let long_js = (0..17)
            .map(|i| format!("const item_{i} = {i};"))
            .collect::<Vec<_>>()
            .join("\n");
        inf_b.push_text(&format!("Summary below:\n```js\n{long_js}\n```\nAll done."));

        let mut tools_b = MockToolExecutor::default_suite();
        tools_b.specs.push(ToolSpec {
            id: "write_file".into(),
            name: "write_file".into(),
            category: "files".into(),
            description: "Write workspace file".into(),
            input_schema: serde_json::json!({ "type": "object" }),
            output_schema: serde_json::json!({ "type": "object" }),
            required_permissions: vec![],
            requires_approval: false,
            is_mutating: true,
            enabled: true,
            timeout_secs: 10,
        });
        let tools_b = Arc::new(tools_b);
        let mut cfg_b = AppConfig::default();
        cfg_b.security.require_approval_for_mutating = false;
        let (storage_b, rt_b) = setup_runtime(inf_b, tools_b.clone(), Some(cfg_b));
        create_conv(
            &storage_b,
            "conv-dup",
            ConversationMode::Direct {
                agent_id: "programmer".into(),
            },
        );
        let res_b = rt_b
            .handle_chat_message("conv-dup", "Build app.js", CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(res_b.status, TaskStatus::Completed);
        assert_eq!(tools_b.executed_count.load(Ordering::SeqCst), 1);
        assert!(res_b.content.contains("Created `app.js` and transported it to the Files tab."));
        assert!(res_b.content.contains("[FILE_CREATED: app.js]"));
        assert!(!res_b.content.contains("const item_12 = 12;"));
    }

    #[tokio::test]
    async fn immediate_bypass_expedite_and_cancel_conversation_tasks() {
        struct GateInferenceProvider {
            calls: AtomicUsize,
            first_entered: Arc<tokio::sync::Notify>,
            release_first: Arc<tokio::sync::Notify>,
        }

        #[async_trait]
        impl InferenceProvider for GateInferenceProvider {
            async fn generate(
                &self,
                _req: InferenceRequest,
                _cancel: CancellationToken,
            ) -> Result<InferenceResponse> {
                let idx = self.calls.fetch_add(1, Ordering::SeqCst);
                if idx == 0 {
                    self.first_entered.notify_one();
                    self.release_first.notified().await;
                    return Ok(InferenceResponse {
                        content: "first completed".into(),
                        reasoning: None,
                        tool_calls: Vec::new(),
                        model: "mock-ps5lm".into(),
                        finish_reason: Some("stop".into()),
                        usage: TokenUsage::default(),
                        latency_ms: 5,
                    });
                }
                Ok(InferenceResponse {
                    content: format!("fast completed {idx}"),
                    reasoning: None,
                    tool_calls: Vec::new(),
                    model: "mock-ps5lm".into(),
                    finish_reason: Some("stop".into()),
                    usage: TokenUsage::default(),
                    latency_ms: 2,
                })
            }

            async fn generate_stream(
                &self,
                req: InferenceRequest,
                tx: mpsc::Sender<InferenceStreamChunk>,
                cancel: CancellationToken,
            ) -> Result<InferenceResponse> {
                let resp = self.generate(req, cancel).await?;
                if !resp.content.is_empty() {
                    let _ = tx
                        .send(InferenceStreamChunk::TextDelta(resp.content.clone()))
                        .await;
                }
                let _ = tx.send(InferenceStreamChunk::Completed(resp.clone())).await;
                Ok(resp)
            }

            async fn health_check(&self) -> ProviderHealth {
                ProviderHealth {
                    reachable: true,
                    loaded_model: Some("mock-ps5lm".into()),
                    available_models: vec!["mock-ps5lm".into()],
                    error: None,
                }
            }

            async fn list_models(&self) -> Result<Vec<String>> {
                Ok(vec!["mock-ps5lm".into()])
            }
        }

        let storage = Arc::new(InMemoryStorage::default());
        let mut cfg = AppConfig::default();
        cfg.agents.max_active_tasks = 1;
        cfg.agents.max_concurrent_inference = 4;
        let (events_tx, _) = broadcast::channel(256);
        let agent_mgr = Arc::new(AgentManager::new(
            storage.clone(),
            cfg.agents.clone(),
            events_tx.clone(),
        ));
        agent_mgr.seed_defaults().unwrap();
        let bus = Arc::new(MessageBus::new(
            storage.clone(),
            (&cfg.agents).into(),
            events_tx.clone(),
        ));
        let first_entered = Arc::new(tokio::sync::Notify::new());
        let release_first = Arc::new(tokio::sync::Notify::new());
        let gate_inf = Arc::new(GateInferenceProvider {
            calls: AtomicUsize::new(0),
            first_entered: first_entered.clone(),
            release_first: release_first.clone(),
        });
        let tools = Arc::new(MockToolExecutor::default_suite());
        let rt = AgentRuntime::new(
            storage.clone(),
            cfg,
            gate_inf,
            tools,
            None,
            agent_mgr,
            bus,
            events_tx,
        );

        for cid in ["conv-gate-1", "conv-gate-2", "conv-gate-3", "conv-gate-4"] {
            create_conv(
                &storage,
                cid,
                ConversationMode::Direct {
                    agent_id: "commander".into(),
                },
            );
        }

        let (_, task1) = rt
            .dispatch_chat_background(
                "conv-gate-1".into(),
                "Hold sole slot".into(),
                None,
                None,
                Vec::new(),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        first_entered.notified().await;

        let (_, task2) = rt
            .dispatch_chat_background_with_options(
                "conv-gate-2".into(),
                "Queued then expedited".into(),
                None,
                None,
                Vec::new(),
                false,
                CancellationToken::new(),
            )
            .await
            .unwrap();

        let (_, task3) = rt
            .dispatch_chat_background_with_options(
                "conv-gate-3".into(),
                "Immediate bypass".into(),
                None,
                None,
                Vec::new(),
                true,
                CancellationToken::new(),
            )
            .await
            .unwrap();

        for _ in 0..50 {
            if storage
                .get_task(&task3.id)
                .unwrap()
                .unwrap()
                .status
                == TaskStatus::Completed
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(
            storage.get_task(&task3.id).unwrap().unwrap().status,
            TaskStatus::Completed
        );
        assert_eq!(
            storage.get_task(&task2.id).unwrap().unwrap().status,
            TaskStatus::Pending
        );

        let exp = rt.expedite_task(&task2.id).unwrap();
        assert_eq!(
            exp.progress_note.as_deref(),
            Some("Expedited — starting immediately")
        );
        for _ in 0..50 {
            if storage
                .get_task(&task2.id)
                .unwrap()
                .unwrap()
                .status
                == TaskStatus::Completed
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(
            storage.get_task(&task2.id).unwrap().unwrap().status,
            TaskStatus::Completed
        );

        let (_, task4) = rt
            .dispatch_chat_background(
                "conv-gate-4".into(),
                "Queued to cancel".into(),
                None,
                None,
                Vec::new(),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        tokio::task::yield_now().await;

        let cancelled = rt
            .cancel_conversation_tasks("conv-gate-4", None, "user cancelled queue")
            .unwrap();
        assert_eq!(cancelled, 1);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(
            storage.get_task(&task4.id).unwrap().unwrap().status,
            TaskStatus::Cancelled
        );

        release_first.notify_waiters();
        for _ in 0..50 {
            if storage
                .get_task(&task1.id)
                .unwrap()
                .unwrap()
                .status
                == TaskStatus::Completed
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(
            storage.get_task(&task1.id).unwrap().unwrap().status,
            TaskStatus::Completed
        );
    }

    #[tokio::test]
    async fn polish_and_english_direct_and_vocative_routing() {
        let inf = Arc::new(ScriptedInferenceProvider::new());
        inf.push_text("Cześć!");
        inf.push_text("Tak, słyszę Cię!");
        inf.push_text("Dzień dobry! W czym pomóc?");
        inf.push_text("Cześć, gotowy do działania.");
        // For preposition-preceded trailing noun ("Summarize options for programmer"):
        // goes to Group Coordinator (returns [] -> Commander ReAct)
        inf.push_text(r#"{"selected_agents":[]}"#);
        inf.push_text("Options summary by Commander.");

        let tools = Arc::new(MockToolExecutor::default_suite());
        let (storage, rt) = setup_runtime(inf.clone(), tools, None);
        create_conv(
            &storage,
            "conv-pl-routing",
            ConversationMode::Group {
                participant_agent_ids: vec!["programmer".into(), "executor".into()],
            },
        );

        // 1. Polish delegation + case suffix + connector ("powiedz executorowi że ma odpisać cześć")
        let r1 = rt
            .handle_chat_message(
                "conv-pl-routing",
                "powiedz executorowi że ma odpisać cześć",
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(r1.agent_id, "executor");
        assert_eq!(r1.content, "Cześć!");

        // 2. Leading vocative ("executor slyszusz mnie")
        let r2 = rt
            .handle_chat_message(
                "conv-pl-routing",
                "executor slyszusz mnie",
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(r2.agent_id, "executor");

        // 3. Trailing vocative ("Dzień dobry Commander")
        let r3 = rt
            .handle_chat_message(
                "conv-pl-routing",
                "Dzień dobry Commander",
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(r3.agent_id, "commander");

        // 4. Trailing vocative with exclamation ("cześć executor!")
        let r4 = rt
            .handle_chat_message(
                "conv-pl-routing",
                "cześć executor!",
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(r4.agent_id, "executor");

        // 5. Preposition guard ("for programmer") does NOT hijack trailing vocative -> hits coordinator
        let r5 = rt
            .handle_chat_message(
                "conv-pl-routing",
                "Summarize options for programmer",
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(r5.agent_id, "commander");
        assert_eq!(r5.content, "Options summary by Commander.");

        let reqs = inf.recorded_requests();
        // 1 + 1 + 1 + 1 + 2 = 6 total inference calls (first 4 bypassed coordinator & Commander!)
        assert_eq!(reqs.len(), 6);
        let u0 = &reqs[0].messages.last().unwrap().content;
        assert!(u0.contains("The user asks you directly: odpisać cześć"));
        assert!(!u0.contains("executorowi"));
        let u1 = &reqs[1].messages.last().unwrap().content;
        assert!(u1.contains("The user asks you directly: slyszusz mnie"));
        assert_eq!(reqs[2].messages.last().unwrap().content, "Dzień dobry");
        let u3 = &reqs[3].messages.last().unwrap().content;
        assert!(u3.contains("The user asks you directly: cześć"));
    }

    #[tokio::test]
    async fn group_coordinator_greeting_short_circuit_and_seed_think_cleanup() {
        let inf = Arc::new(ScriptedInferenceProvider::new());
        // Casual greetings bypass coordinator: 1 Commander ReAct call each
        inf.push_text("Dzień dobry! Jak mogę pomóc?");
        inf.push_text("Pong!");
        // Coordinator emits <seed:think>...</seed:think> + malformed output -> falls back to Commander
        inf.push_text("<seed:think>internal trace with programmer word</seed:think> not valid json");
        inf.push_text("Handled directly after malformed coordinator JSON.");
        // Multi-word request with >6 words & domain term hits coordinator cleanly
        inf.push_text(r#"{"selected_agents":[]}"#);
        inf.push_text("Project layout summary.");

        let tools = Arc::new(MockToolExecutor::default_suite());
        let (storage, rt) = setup_runtime(inf.clone(), tools, None);
        create_conv(
            &storage,
            "conv-greetings",
            ConversationMode::Group {
                participant_agent_ids: vec!["programmer".into(), "researcher".into()],
            },
        );

        let g1 = rt
            .handle_chat_message("conv-greetings", "Dzień dobry!", CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(g1.agent_id, "commander");

        let g2 = rt
            .handle_chat_message("conv-greetings", "ping", CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(g2.agent_id, "commander");

        // Malformed coordinator output + <seed:think> falls back to [] (Commander)
        let f1 = rt
            .handle_chat_message(
                "conv-greetings",
                "Could you help organize my weekly roadmap notes?",
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(f1.agent_id, "commander");
        assert_eq!(f1.content, "Handled directly after malformed coordinator JSON.");

        let l1 = rt
            .handle_chat_message(
                "conv-greetings",
                "Summarize the project layout without specialist tools.",
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(l1.content, "Project layout summary.");

        let prompts = inf.called_system_prompts();
        // g1(1) + g2(1) + f1(2: coord + cmdr) + l1(2: coord + cmdr) = 6
        assert_eq!(prompts.len(), 6);
        // Coordinator prompt puts {"selected_agents": []} first and omits commander persona prompt
        assert!(prompts[2].starts_with("You are routing a group chat turn. Default to {\"selected_agents\": []}"));
    }

    #[tokio::test]
    async fn kv_prefill_hop_guard_and_background_failure_system_alert() {
        let inf = Arc::new(ScriptedInferenceProvider::new());
        // Parent calls delegate_task to researcher -> child runs with hop_count = 1
        inf.push_tool_calls(vec![ToolCallRequest {
            id: "call_del_1".into(),
            name: "delegate_task".into(),
            arguments: serde_json::json!({
                "agent_id": "researcher",
                "task": "Return concise fact"
            }),
        }]);
        // Child inference (hop_count = 1)
        inf.push_text("child fact");
        // Parent final inference: assert inside push_fn that child did NOT trigger prefill_kv_cache!
        let inf_ref = inf.clone();
        inf.push_fn(move |_| {
            assert_eq!(
                inf_ref.prefill_count(),
                0,
                "child task with hop_count > 0 must not trigger KV cache prefill"
            );
            InferenceResponse {
                content: "parent combined".into(),
                reasoning: Some("done".into()),
                tool_calls: Vec::new(),
                model: "mock-ps5lm".into(),
                finish_reason: Some("stop".into()),
                usage: TokenUsage::default(),
                latency_ms: 5,
            }
        });

        let mut cfg = AppConfig::default();
        cfg.inference.prefill_kv_cache = true;
        let tools = Arc::new(MockToolExecutor::default_suite());
        let (storage, rt) = setup_runtime(inf.clone(), tools, Some(cfg));
        create_conv(
            &storage,
            "conv-kv-bg",
            ConversationMode::Direct {
                agent_id: "commander".into(),
            },
        );

        let res = rt
            .handle_chat_message("conv-kv-bg", "Delegate something", CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(res.content, "parent combined");

        // Parent root turn (hop_count == 0) triggers async background KV prefill once
        for _ in 0..40 {
            if inf.prefill_count() >= 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(inf.prefill_count(), 1);

        // Background dispatch failure (empty inference queue -> Inference error) must emit SystemAlert
        let mut rx = rt.events();
        let (_u_msg, _task) = rt
            .dispatch_chat_background(
                "conv-kv-bg".to_string(),
                "This background turn will fail".to_string(),
                None,
                None,
                Vec::new(),
                CancellationToken::new(),
            )
            .await
            .unwrap();

        let mut got_error_alert = false;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv()).await {
                Ok(Ok(StreamEvent::SystemAlert { level, message })) => {
                    if level == "error" && message.contains("conv-kv-bg") {
                        got_error_alert = true;
                        break;
                    }
                }
                Ok(Ok(_)) => continue,
                Ok(Err(_)) | Err(_) => continue,
            }
        }
        assert!(got_error_alert, "expected StreamEvent::SystemAlert on background chat failure");
    }

    #[tokio::test]
    async fn autonomous_scheduler_tick_and_builtin_schedule_tools() {
        let inf = Arc::new(ScriptedInferenceProvider::new());
        // Turn 1: Commander uses built-in schedule_create + schedule_list tools
        inf.push_tool_calls(vec![ToolCallRequest {
            id: "call_sched_create".into(),
            name: "schedule_create".into(),
            arguments: serde_json::json!({
                "title": "Due One-Shot via Tool",
                "prompt": "Check runtime health once",
                "kind": "once",
                "delay_secs": 0,
                "target_agent_id": "commander"
            }),
        }]);
        inf.push_tool_calls(vec![ToolCallRequest {
            id: "call_sched_list".into(),
            name: "schedule_list".into(),
            arguments: serde_json::json!({ "limit": 10 }),
        }]);
        inf.push_text("Schedule created and listed.");
        // Scheduled run 1 (Once schedule) & Scheduled run 2 (Interval schedule)
        inf.push_text("Scheduled check #1 ok.");
        inf.push_text("Scheduled interval check ok.");

        let tools = Arc::new(MockToolExecutor::default_suite());
        let (storage, rt) = setup_runtime(inf.clone(), tools, None);
        create_conv(
            &storage,
            "conv-sched-admin",
            ConversationMode::Direct {
                agent_id: "commander".into(),
            },
        );

        let tool_turn = rt
            .handle_chat_message(
                "conv-sched-admin",
                "Create a one-shot schedule and list schedules",
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(tool_turn.content, "Schedule created and listed.");

        let mut schedules = rt.list_schedules(0).unwrap();
        assert_eq!(schedules.len(), 1);
        let once_id = schedules[0].id.clone();

        // Also create a due Interval schedule pointing into the past via create + direct upsert
        let mut interval_sched = rt
            .create_schedule(CreateScheduleInput {
                title: "Heartbeat".into(),
                prompt: "Report status".into(),
                target_agent_id: Some("executor".into()),
                conversation_id: None,
                kind: ScheduleKind::Interval,
                run_at: None,
                interval_secs: Some(60),
                daily_time: None,
                cron_expr: None,
                timezone_offset_mins: 0,
                enabled: Some(true),
            })
            .unwrap();
        interval_sched.next_run_at = Some(chrono::Utc::now() - chrono::Duration::seconds(5));
        storage.upsert_schedule(&interval_sched).unwrap();

        let mut rx = rt.events();
        let before_tick = chrono::Utc::now();
        let executed = rt.tick_due_schedules(CancellationToken::new()).await.unwrap();
        assert_eq!(executed, 2);

        // Second tick immediately afterwards finds nothing due
        let executed_again = rt.tick_due_schedules(CancellationToken::new()).await.unwrap();
        assert_eq!(executed_again, 0);

        let once_after = rt.get_schedule(&once_id).unwrap().unwrap();
        assert!(!once_after.enabled);
        assert!(once_after.next_run_at.is_none());
        assert_eq!(once_after.run_count, 1);
        assert_eq!(once_after.last_status.as_deref(), Some("completed"));
        assert!(once_after.last_task_id.is_some());
        let once_conv_id = once_after.conversation_id.expect("lazy schedule conversation");
        assert!(storage.get_conversation(&once_conv_id).unwrap().is_some());
        let msgs = storage.list_chat_messages(&once_conv_id, false, 10).unwrap();
        assert!(msgs.iter().any(|m| m.role == ChatRole::User));
        assert!(msgs.iter().any(|m| m.role == ChatRole::Assistant));

        let interval_after = rt.get_schedule(&interval_sched.id).unwrap().unwrap();
        assert!(interval_after.enabled);
        assert!(interval_after.next_run_at.unwrap() > before_tick);
        assert_eq!(interval_after.run_count, 1);

        schedules = rt.list_schedules(0).unwrap();
        assert_eq!(schedules.len(), 2);

        // Verify ScheduleUpdated events were emitted
        let mut update_events = 0usize;
        while let Ok(ev) = rx.try_recv() {
            if matches!(ev, StreamEvent::ScheduleUpdated { .. }) {
                update_events += 1;
            }
        }
        assert!(update_events >= 2);

        // Update + delete check
        let updated = rt
            .update_schedule(
                &once_id,
                UpdateScheduleInput {
                    title: Some("Renamed One-Shot".into()),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(updated.title, "Renamed One-Shot");
        rt.delete_schedule(&once_id).unwrap();
        assert!(rt.get_schedule(&once_id).unwrap().is_none());
    }

    #[tokio::test]
    async fn fail_task_surfaces_error_in_task_updated_and_once_reschedule_resets_run_count() {
        let inf = Arc::new(ScriptedInferenceProvider::new());
        inf.push_text("Once run 1 completed.");
        inf.push_text("Once run 2 completed.");

        let tools = Arc::new(MockToolExecutor::default_suite());
        let (_storage, rt) = setup_runtime(inf.clone(), tools, None);
        let mut rx = rt.events();

        // 1. TaskManager::fail_task emits StreamEvent::TaskUpdated with progress_note == Some(error_info)
        let task = rt
            .task_manager()
            .create_task(
                "Broken request".into(),
                "commander".into(),
                Some("conv-err".into()),
                None,
                5,
                TaskStatus::Pending,
            )
            .unwrap();
        rt.task_manager()
            .mark_running(&task.id, Some("Generating response...".into()))
            .unwrap();
        let failed = rt
            .task_manager()
            .fail_task(&task.id, "PS5LM returned HTTP 500".into())
            .unwrap();
        assert_eq!(
            failed.progress_note.as_deref(),
            Some("PS5LM returned HTTP 500")
        );

        let mut saw_failed_event_with_error = false;
        while let Ok(ev) = rx.try_recv() {
            if let StreamEvent::TaskUpdated {
                task_id,
                status,
                progress_note,
                ..
            } = ev
            {
                if task_id == task.id && status == "FAILED" {
                    assert_eq!(progress_note.as_deref(), Some("PS5LM returned HTTP 500"));
                    saw_failed_event_with_error = true;
                }
            }
        }
        assert!(
            saw_failed_event_with_error,
            "TaskManager::fail_task must emit FAILED TaskUpdated with real error in progress_note"
        );

        // 2. Re-enabling / rescheduling a Once schedule with run_count > 0 recomputes non-None next_run_at
        let past_due = chrono::Utc::now() - chrono::Duration::seconds(5);
        let once_sched = rt
            .create_schedule(CreateScheduleInput {
                title: "Reusable One-Shot".into(),
                prompt: "Run once then re-enable".into(),
                target_agent_id: Some("commander".into()),
                conversation_id: None,
                kind: ScheduleKind::Once,
                run_at: Some(past_due),
                interval_secs: None,
                daily_time: None,
                cron_expr: None,
                timezone_offset_mins: 0,
                enabled: Some(true),
            })
            .unwrap();
        assert_eq!(rt.tick_due_schedules(CancellationToken::new()).await.unwrap(), 1);
        let after_first = rt.get_schedule(&once_sched.id).unwrap().unwrap();
        assert!(!after_first.enabled);
        assert_eq!(after_first.run_count, 1);
        assert!(after_first.next_run_at.is_none());

        // 2a. Re-enabling via toggle_schedule resets run_count and sets next_run_at
        let toggled = rt.toggle_schedule(&once_sched.id, Some(true)).unwrap();
        assert!(toggled.enabled);
        assert_eq!(toggled.run_count, 0);
        assert!(toggled.next_run_at.is_some());

        // 2b. Rescheduling via update_schedule with a new run_at (even after execution) resets run_count and sets next_run_at
        assert_eq!(rt.tick_due_schedules(CancellationToken::new()).await.unwrap(), 1);
        let new_future = chrono::Utc::now() + chrono::Duration::seconds(120);
        let rescheduled = rt
            .update_schedule(
                &once_sched.id,
                UpdateScheduleInput {
                    run_at: Some(new_future),
                    enabled: Some(true),
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(rescheduled.enabled);
        assert_eq!(rescheduled.run_count, 0);
        assert_eq!(rescheduled.next_run_at, Some(new_future));
    }

    struct TempTestWorkspace {
        path: std::path::PathBuf,
    }

    impl TempTestWorkspace {
        fn new(prefix: &str) -> Self {
            let path = std::env::temp_dir().join(format!("ps5-runtime-{prefix}-{}", new_id()));
            std::fs::create_dir_all(&path).unwrap();
            Self { path }
        }
    }

    impl Drop for TempTestWorkspace {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    fn setup_runtime_with_tools(
        inference: Arc<ScriptedInferenceProvider>,
        tools: Arc<dyn RuntimeToolExecutor>,
        cfg_override: Option<AppConfig>,
    ) -> (Arc<InMemoryStorage>, Arc<AgentRuntime>) {
        let storage = Arc::new(InMemoryStorage::default());
        let cfg = cfg_override.unwrap_or_default();
        let (events_tx, _) = broadcast::channel(256);
        let agent_mgr = Arc::new(AgentManager::new(
            storage.clone(),
            cfg.agents.clone(),
            events_tx.clone(),
        ));
        agent_mgr.seed_defaults().unwrap();
        let bus = Arc::new(MessageBus::new(
            storage.clone(),
            (&cfg.agents).into(),
            events_tx.clone(),
        ));
        let rt = AgentRuntime::new(
            storage.clone(),
            cfg,
            inference,
            tools,
            None,
            agent_mgr,
            bus,
            events_tx,
        );
        (storage, rt)
    }

    struct RecordingToolExecutor {
        specs: Vec<ToolSpec>,
        calls: Mutex<Vec<(Option<String>, String, SandboxMode, SandboxApprovalPolicy, bool)>>,
    }

    impl RecordingToolExecutor {
        fn new(specs: Vec<ToolSpec>) -> Self {
            Self {
                specs,
                calls: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl RuntimeToolExecutor for RecordingToolExecutor {
        async fn list_tools(&self) -> Vec<ToolSpec> {
            self.specs.clone()
        }

        async fn get_tool(&self, tool_id: &str) -> Option<ToolSpec> {
            self.specs.iter().find(|s| s.id == tool_id).cloned()
        }

        async fn execute_tool(
            &self,
            agent: &AgentDefinition,
            task_id: Option<&str>,
            tool_id: &str,
            arguments: serde_json::Value,
            approved_bypass: bool,
            cancel: CancellationToken,
        ) -> Result<ToolExecutionOutput> {
            self.execute_tool_with_context(
                agent,
                task_id,
                None,
                tool_id,
                arguments,
                approved_bypass,
                SandboxMode::FullSandbox,
                SandboxApprovalPolicy::FullAuto,
                cancel,
            )
            .await
        }

        async fn execute_tool_with_context(
            &self,
            _agent: &AgentDefinition,
            _task_id: Option<&str>,
            conversation_id: Option<&str>,
            tool_id: &str,
            arguments: serde_json::Value,
            approved_bypass: bool,
            sandbox_mode: SandboxMode,
            approval_policy: SandboxApprovalPolicy,
            _cancel: CancellationToken,
        ) -> Result<ToolExecutionOutput> {
            let spec = self
                .get_tool(tool_id)
                .await
                .ok_or_else(|| AgentError::NotFound(format!("tool '{tool_id}' not found")))?;
            if spec.requires_approval
                && !approved_bypass
                && approval_policy != SandboxApprovalPolicy::FullAuto
            {
                return Err(AgentError::ApprovalRequired {
                    approval_id: new_id(),
                    reason: format!("tool '{tool_id}' requires approval"),
                });
            }
            self.calls.lock().unwrap().push((
                conversation_id.map(str::to_string),
                tool_id.to_string(),
                sandbox_mode,
                approval_policy,
                approved_bypass,
            ));
            Ok(ToolExecutionOutput {
                output: serde_json::json!({ "tool": tool_id, "ok": true, "args": arguments }),
                is_mutating: spec.is_mutating,
            })
        }
    }

    #[tokio::test]
    async fn tool_policy_denied_tools_and_groups_enforced_in_schemas_and_execution() {
        let mut tools = MockToolExecutor::default_suite();
        let make_spec = |id: &str, category: &str, mutating: bool| ToolSpec {
            id: id.into(),
            name: id.into(),
            category: category.into(),
            description: format!("mock {id}"),
            input_schema: serde_json::json!({ "type": "object" }),
            output_schema: serde_json::json!({ "type": "object" }),
            required_permissions: vec![],
            requires_approval: false,
            is_mutating: mutating,
            enabled: true,
            timeout_secs: 10,
        };
        tools.specs.push(make_spec("read_file", "files", false));
        tools.specs.push(make_spec("write_file", "files", true));
        tools.specs.push(make_spec("memory_read", "memory", false));

        let tools = Arc::new(tools);
        let inf = Arc::new(ScriptedInferenceProvider::new());
        let (storage, rt) = setup_runtime(inf.clone(), tools.clone(), None);

        rt.agent_manager()
            .create_agent(
                ps5_agent_manager::CreateAgentInput {
                    id: Some("group-bot".into()),
                    name: "Group Bot".into(),
                    description: "Uses tool groups with explicit denial".into(),
                    role: AgentRole::Custom,
                    role_label: None,
                    system_prompt: "Follow tool policies.".into(),
                    model_config: None,
                    enabled_tools: vec!["group:fs".into(), "group:memory".into()],
                    denied_tools: Some(vec!["write_file".into()]),
                    sandbox_mode: None,
                    tool_profile: None,
                    memory_permissions: None,
                    max_task_duration_secs: None,
                    max_reasoning_iterations: None,
                    priority: None,
                    can_communicate: None,
                    enabled: Some(true),
                    is_temporary: false,
                    spawned_by: None,
                },
                "admin",
            )
            .unwrap();

        create_conv(
            &storage,
            "conv-policy",
            ConversationMode::Direct {
                agent_id: "group-bot".into(),
            },
        );
        inf.push_tool_calls(vec![
            ToolCallRequest {
                id: "c1".into(),
                name: "file_read".into(),
                arguments: serde_json::json!({ "path": "a.txt" }),
            },
            ToolCallRequest {
                id: "c2".into(),
                name: "write_file".into(),
                arguments: serde_json::json!({ "path": "b.txt", "content": "x" }),
            },
        ]);
        inf.push_text("Policy checked.");

        let res = rt
            .chat_with_agent_with_options(
                "conv-policy",
                "group-bot",
                "Read a.txt then write b.txt",
                None,
                vec![],
                CancellationToken::new(),
            )
            .await
            .unwrap();

        assert_eq!(res.status, TaskStatus::Completed);
        assert_eq!(tools.executed_count.load(Ordering::SeqCst), 1);

        let reqs = inf.recorded_requests();
        assert_eq!(reqs.len(), 2);
        let schema_names: Vec<&str> = reqs[0].tools.iter().map(|t| t.name.as_str()).collect();
        assert!(schema_names.contains(&"file_read"));
        assert!(schema_names.contains(&"read_file"));
        assert!(schema_names.contains(&"memory_read"));
        assert!(!schema_names.contains(&"write_file"));
        assert!(!schema_names.contains(&"file_write"));

        assert!(reqs[1].messages.iter().any(|m| {
            m.role == "tool"
                && m.tool_call_id.as_deref() == Some("c2")
                && m.content.contains("error")
        }));
    }

    #[tokio::test]
    async fn sandbox_mode_approval_policy_and_context_forwarding_for_tools_and_scripts() {
        let make_spec = |id: &str, requires_approval: bool, perm: ToolPermission| ToolSpec {
            id: id.into(),
            name: id.into(),
            category: "files".into(),
            description: format!("mock {id}"),
            input_schema: serde_json::json!({ "type": "object" }),
            output_schema: serde_json::json!({ "type": "object" }),
            required_permissions: vec![perm],
            requires_approval,
            is_mutating: true,
            enabled: true,
            timeout_secs: 10,
        };
        let specs = vec![
            make_spec("write_file", false, ToolPermission::FileWrite),
            make_spec("file_delete", true, ToolPermission::FileDelete),
        ];

        // Part A: Per-agent SandboxMode::ReadOnly blocks mutating native tools AND fenced script transport
        let mut cfg_a = AppConfig::default();
        cfg_a.sandbox.mode = "workspace_write".into();
        cfg_a.sandbox.approval_policy = "full_auto".into();
        let inf_a = Arc::new(ScriptedInferenceProvider::new());
        let exec_a = Arc::new(RecordingToolExecutor::new(specs.clone()));
        let (storage_a, rt_a) = setup_runtime_with_tools(inf_a.clone(), exec_a.clone(), Some(cfg_a));
        rt_a.agent_manager()
            .update_agent(
                "programmer",
                ps5_agent_manager::UpdateAgentInput {
                    enabled_tools: Some(vec!["*".into()]),
                    sandbox_mode: Some("read_only".into()),
                    ..Default::default()
                },
                "admin",
            )
            .unwrap();
        create_conv(
            &storage_a,
            "conv-ro",
            ConversationMode::Direct {
                agent_id: "programmer".into(),
            },
        );
        inf_a.push_tool_calls(vec![ToolCallRequest {
            id: "w-ro".into(),
            name: "write_file".into(),
            arguments: serde_json::json!({ "path": "a.py", "content": "x = 1\n" }),
        }]);
        let script_body = (0..12)
            .map(|i| format!("    v_{i} = {i}"))
            .collect::<Vec<_>>()
            .join("\n");
        inf_a.push_text(&format!("```python\ndef run():\n{script_body}\n```"));
        let res_a = rt_a
            .handle_chat_message("conv-ro", "Create helper.py", CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(res_a.status, TaskStatus::Completed);
        assert!(exec_a.calls.lock().unwrap().is_empty());
        assert!(inf_a.recorded_requests()[1]
            .messages
            .iter()
            .any(|m| m.role == "tool" && m.content.contains("read_only")));

        // Part B: SandboxApprovalPolicy::AlwaysAsk pauses fenced script transport and forwards
        // conversation_id, SandboxMode, SandboxApprovalPolicy on approval resolution.
        let mut cfg_b = AppConfig::default();
        cfg_b.sandbox.mode = "workspace_write".into();
        cfg_b.sandbox.approval_policy = "always_ask".into();
        cfg_b.security.require_approval_for_mutating = false;
        let inf_b = Arc::new(ScriptedInferenceProvider::new());
        let exec_b = Arc::new(RecordingToolExecutor::new(specs.clone()));
        let (storage_b, rt_b) = setup_runtime_with_tools(inf_b.clone(), exec_b.clone(), Some(cfg_b));
        rt_b.agent_manager()
            .update_agent(
                "programmer",
                ps5_agent_manager::UpdateAgentInput {
                    enabled_tools: Some(vec!["*".into()]),
                    ..Default::default()
                },
                "admin",
            )
            .unwrap();
        create_conv(
            &storage_b,
            "conv-ask",
            ConversationMode::Direct {
                agent_id: "programmer".into(),
            },
        );
        inf_b.push_text(&format!("```python\ndef run():\n{script_body}\n```"));
        inf_b.push_text("Saved after approval.");
        let paused = rt_b
            .handle_chat_message("conv-ask", "Create helper.py", CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(paused.status, TaskStatus::Waiting);
        let aid = paused.pending_approval_id.expect("script approval id");
        assert!(exec_b.calls.lock().unwrap().is_empty());

        let approved = rt_b.resolve_approval(&aid, true, "admin").await.unwrap();
        assert_eq!(approved.task_status, Some(TaskStatus::Completed));
        let calls_b = exec_b.calls.lock().unwrap().clone();
        assert_eq!(calls_b.len(), 1);
        assert_eq!(
            calls_b[0],
            (
                Some("conv-ask".into()),
                "write_file".into(),
                SandboxMode::WorkspaceWrite,
                SandboxApprovalPolicy::AlwaysAsk,
                true,
            )
        );

        // Part C: OnRequest in non-interactive submit_task upgrades builtin execution to FullAuto
        // without hanging, while Never rejects requires_approval tools cleanly.
        let mut cfg_c = AppConfig::default();
        cfg_c.sandbox.mode = "full_sandbox".into();
        cfg_c.sandbox.approval_policy = "on_request".into();
        cfg_c.security.require_approval_for_mutating = false;
        let inf_c = Arc::new(ScriptedInferenceProvider::new());
        let exec_c = Arc::new(RecordingToolExecutor::new(specs.clone()));
        let (storage_c, rt_c) = setup_runtime_with_tools(inf_c.clone(), exec_c.clone(), Some(cfg_c));
        inf_c.push_tool_calls(vec![ToolCallRequest {
            id: "del-bg".into(),
            name: "file_delete".into(),
            arguments: serde_json::json!({ "path": "old.tmp" }),
        }]);
        inf_c.push_text("Deleted in background.");
        let bg_task = rt_c
            .submit_task("Clean temp".into(), "commander".into(), None, None)
            .await
            .unwrap();
        for _ in 0..100 {
            if storage_c
                .get_task(&bg_task.id)
                .unwrap()
                .unwrap()
                .status
                .is_terminal()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(
            storage_c.get_task(&bg_task.id).unwrap().unwrap().status,
            TaskStatus::Completed
        );
        let calls_c = exec_c.calls.lock().unwrap().clone();
        assert_eq!(
            calls_c,
            vec![(
                None,
                "file_delete".into(),
                SandboxMode::FullSandbox,
                SandboxApprovalPolicy::FullAuto,
                false,
            )]
        );

        let mut cfg_d = AppConfig::default();
        cfg_d.sandbox.mode = "full_sandbox".into();
        cfg_d.sandbox.approval_policy = "never".into();
        let inf_d = Arc::new(ScriptedInferenceProvider::new());
        let exec_d = Arc::new(RecordingToolExecutor::new(specs));
        let (storage_d, rt_d) = setup_runtime_with_tools(inf_d.clone(), exec_d.clone(), Some(cfg_d));
        create_conv(
            &storage_d,
            "conv-never",
            ConversationMode::Direct {
                agent_id: "commander".into(),
            },
        );
        inf_d.push_tool_calls(vec![ToolCallRequest {
            id: "del-never".into(),
            name: "file_delete".into(),
            arguments: serde_json::json!({ "path": "old.tmp" }),
        }]);
        inf_d.push_text("Blocked cleanly.");
        let res_d = rt_d
            .handle_chat_message("conv-never", "Delete old.tmp", CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(res_d.status, TaskStatus::Completed);
        assert!(exec_d.calls.lock().unwrap().is_empty());
        assert!(inf_d.recorded_requests()[1]
            .messages
            .iter()
            .any(|m| m.role == "tool" && m.content.contains("never")));
    }

    #[tokio::test]
    async fn resolve_approval_concurrent_calls_conflict_without_double_execution() {
        struct BlockingApproveToolExecutor {
            specs: Vec<ToolSpec>,
            started: Arc<tokio::sync::Notify>,
            release: Arc<tokio::sync::Notify>,
            executed: AtomicUsize,
        }

        #[async_trait]
        impl RuntimeToolExecutor for BlockingApproveToolExecutor {
            async fn list_tools(&self) -> Vec<ToolSpec> {
                self.specs.clone()
            }

            async fn get_tool(&self, tool_id: &str) -> Option<ToolSpec> {
                self.specs.iter().find(|s| s.id == tool_id).cloned()
            }

            async fn execute_tool(
                &self,
                _agent: &AgentDefinition,
                _task_id: Option<&str>,
                tool_id: &str,
                arguments: serde_json::Value,
                approved_bypass: bool,
                _cancel: CancellationToken,
            ) -> Result<ToolExecutionOutput> {
                let spec = self
                    .get_tool(tool_id)
                    .await
                    .ok_or_else(|| AgentError::NotFound(format!("tool '{tool_id}' not found")))?;
                if spec.requires_approval && !approved_bypass {
                    return Err(AgentError::ApprovalRequired {
                        approval_id: new_id(),
                        reason: format!("tool '{tool_id}' requires approval"),
                    });
                }
                self.started.notify_one();
                self.release.notified().await;
                self.executed.fetch_add(1, Ordering::SeqCst);
                Ok(ToolExecutionOutput {
                    output: serde_json::json!({ "deleted": arguments["path"] }),
                    is_mutating: true,
                })
            }
        }

        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let tools = Arc::new(BlockingApproveToolExecutor {
            specs: MockToolExecutor::default_suite().specs,
            started: started.clone(),
            release: release.clone(),
            executed: AtomicUsize::new(0),
        });
        let inf = Arc::new(ScriptedInferenceProvider::new());
        let mut cfg = AppConfig::default();
        cfg.security.require_approval_for_mutating = true;
        let (storage, rt) = setup_runtime_with_tools(inf.clone(), tools.clone(), Some(cfg));
        create_conv(
            &storage,
            "conv-race",
            ConversationMode::Direct {
                agent_id: "commander".into(),
            },
        );
        inf.push_tool_calls(vec![ToolCallRequest {
            id: "del-1".into(),
            name: "file_delete".into(),
            arguments: serde_json::json!({ "path": "old.tmp" }),
        }]);
        inf.push_text("Deleted cleanly.");

        let paused = rt
            .handle_chat_message("conv-race", "Delete old.tmp", CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(paused.status, TaskStatus::Waiting);
        let aid = paused.pending_approval_id.expect("approval id");

        let rt_bg = rt.clone();
        let aid_bg = aid.clone();
        let handle = tokio::spawn(async move { rt_bg.resolve_approval(&aid_bg, true, "admin").await });

        started.notified().await;
        let race = rt.resolve_approval(&aid, true, "admin").await;
        assert!(matches!(race, Err(AgentError::Conflict(_))));

        release.notify_one();
        let first = handle.await.unwrap().unwrap();
        assert_eq!(first.task_status, Some(TaskStatus::Completed));
        assert_eq!(tools.executed.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn routing_direct_mode_skips_trailing_vocative_multi_agent_list_and_unclosed_think() {
        let inf = Arc::new(ScriptedInferenceProvider::new());
        let tools = Arc::new(MockToolExecutor::default_suite());
        let (storage, rt) = setup_runtime(inf.clone(), tools, None);

        // 1. Direct mode pinned to commander must not hijack on trailing ", researcher"
        create_conv(
            &storage,
            "conv-direct-no-hijack",
            ConversationMode::Direct {
                agent_id: "commander".into(),
            },
        );
        inf.push_text("Commander answered directly.");
        let direct_res = rt
            .handle_chat_message(
                "conv-direct-no-hijack",
                "Talk about this, researcher",
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(direct_res.agent_id, "commander");
        assert_eq!(direct_res.content, "Commander answered directly.");

        // 2. Comma-separated multi-agent list routes through Group coordinator instead of 1 specialist
        create_conv(
            &storage,
            "conv-comma-group",
            ConversationMode::Group {
                participant_agent_ids: vec![
                    "commander".into(),
                    "programmer".into(),
                    "researcher".into(),
                ],
            },
        );
        inf.push_text(r#"{"selected_agents":["programmer","researcher"],"direct_answer":null}"#);
        inf.push_text("Programmer inspected.");
        inf.push_text("Researcher inspected.");
        inf.push_text("Combined inspection.");
        let list_res = rt
            .handle_chat_message(
                "conv-comma-group",
                "programmer, researcher inspect the crash dump",
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(list_res.selected_participants.len(), 2);
        assert!(list_res.selected_participants.iter().any(|s| s == "programmer"));
        assert!(list_res.selected_participants.iter().any(|s| s == "researcher"));

        // 3. Coordinator returning unclosed <think> tail falls back cleanly to Commander without leaking <think>
        create_conv(
            &storage,
            "conv-think-fallback",
            ConversationMode::Group {
                participant_agent_ids: vec!["commander".into(), "researcher".into()],
            },
        );
        inf.push_text("<think>internal unclosed reasoning without closing tag");
        inf.push_text("Commander fallback reply.");
        let think_res = rt
            .handle_chat_message(
                "conv-think-fallback",
                "Summarize system status",
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(think_res.agent_id, "commander");
        assert!(think_res.selected_participants.is_empty());
        assert_eq!(think_res.content, "Commander fallback reply.");
    }

    #[tokio::test]
    async fn schedule_concurrent_edit_preserved_and_heartbeat_and_helpers_work() {
        let ws = TempTestWorkspace::new("hb-brain");
        seed_workspace_brain_files(&ws.path).unwrap();
        write_brain_file(
            &ws.path,
            "AGENTS.md",
            "# Workspace Rules\nAlways verify workspace invariants.\n",
        )
        .unwrap();
        write_brain_file(
            &ws.path,
            "HEARTBEAT.md",
            "# Periodic Checks\n- Verify PS5 telemetry\n",
        )
        .unwrap();

        let mut cfg = AppConfig::default();
        cfg.data_dir = ws.path.clone();
        cfg.tools.allowed_paths = vec![ws.path.to_string_lossy().into_owned()];
        cfg.sandbox.inject_brain_files = true;
        cfg.sandbox.heartbeat_interval_secs = 60;

        let inf = Arc::new(ScriptedInferenceProvider::new());
        let tools = Arc::new(MockToolExecutor::default_suite());
        let (storage, rt) = setup_runtime(inf.clone(), tools, Some(cfg));

        // 1. Concurrent edit during schedule execution is preserved across completion save
        let mut sched = rt
            .create_schedule(CreateScheduleInput {
                title: "Original Title".into(),
                prompt: "Run maintenance".into(),
                target_agent_id: Some("commander".into()),
                conversation_id: None,
                kind: ScheduleKind::Interval,
                run_at: None,
                interval_secs: Some(60),
                daily_time: None,
                cron_expr: None,
                timezone_offset_mins: 0,
                enabled: Some(true),
            })
            .unwrap();
        sched.next_run_at = Some(chrono::Utc::now() - chrono::Duration::seconds(5));
        storage.upsert_schedule(&sched).unwrap();

        let sched_id = sched.id.clone();
        let storage_for_inf = storage.clone();
        inf.push_fn(move |_| {
            let mut live = storage_for_inf.get_schedule(&sched_id).unwrap().unwrap();
            live.title = "Edited Live Title".into();
            storage_for_inf.upsert_schedule(&live).unwrap();
            InferenceResponse {
                content: "Maintenance done.".into(),
                reasoning: None,
                tool_calls: Vec::new(),
                model: "mock-ps5lm".into(),
                finish_reason: Some("stop".into()),
                usage: TokenUsage::default(),
                latency_ms: 5,
            }
        });

        let ran = rt.tick_due_schedules(CancellationToken::new()).await.unwrap();
        assert_eq!(ran, 1);
        let after_sched = rt.get_schedule(&sched.id).unwrap().unwrap();
        assert_eq!(after_sched.title, "Edited Live Title");
        assert_eq!(after_sched.run_count, 1);
        assert_eq!(after_sched.last_status.as_deref(), Some("completed"));

        // 2. Heartbeat triggers once within interval and injects AGENTS.md brain files
        inf.push_text("Heartbeat telemetry OK.");
        let hb_task_id = rt
            .tick_heartbeat_if_due()
            .await
            .unwrap()
            .expect("heartbeat task submitted");
        assert_eq!(rt.tick_heartbeat_if_due().await.unwrap(), None);
        for _ in 0..100 {
            if rt
                .storage()
                .get_task(&hb_task_id)
                .unwrap()
                .unwrap()
                .status
                .is_terminal()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(
            rt.storage().get_task(&hb_task_id).unwrap().unwrap().status,
            TaskStatus::Completed
        );
        let prompts = inf.called_system_prompts();
        assert!(prompts
            .last()
            .is_some_and(|p| p.contains("Always verify workspace invariants.")));

        // 3. Slash & Doctor runtime wrappers succeed cleanly
        let slash = rt
            .execute_slash_command(None, Some("commander"), "/status", CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(slash.command, "status");
        assert!(slash.ephemeral);

        let doctor = rt.run_doctor_checks(&[]).await.unwrap();
        assert_eq!(doctor.checks.len(), 12);
    }
}
