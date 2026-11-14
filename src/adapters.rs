use async_trait::async_trait;
use ps5_agent_config::{AppConfig, MemoryConfig};
use ps5_agent_core::{
    now_iso, AgentCardView, AgentDefinition, AgentEnvelope, AgentError, AgentMessageType,
    ApiMode, ApprovalRecord, BrainFileDetail, BrainFileSummary, ChatAttachment, ChatMessage,
    ChatRole, Conversation, ConversationMode, DoctorFixReport, DoctorReport, InferenceMessage,
    InferenceProvider, MemoryItem, MemoryScope, MetricsSnapshot, Ps5LmCapabilities, Result,
    SandboxApprovalPolicy, SandboxMode, ScheduleRecord, SlashCommandResponse, StorageBackend,
    StreamEvent, SystemOneRequest, SystemOneResponse, TaskStatus, ThinkingLevel, ToolSpec,
    WebhookRecord, WebhookView,
};
use ps5_agent_database::DbMetrics;
use ps5_agent_http::{
    CreateAgentRequest, CreateTaskRequest, CreateWebhookRequest, CreateWebhookResponse,
    ExecuteToolRequest, ExtensionsOverviewResponse, HttpBackend, HttpCreateSchedule,
    HttpScheduleAiDraft, HttpScheduleAiRequest, HttpScheduleAiResponse, HttpUpdateSchedule,
    ImportMcpRequest, ImportPluginRequest, ImportSkillRequest, McpServerDto, McpToolDto,
    PluginInfoDto, SandboxStateDto, SendAgentMessageRequest, SendMessageRequest,
    SendMessageResponse, SkillDetailResponse, SkillInfoDto, SlashCommandRequest,
    UpdateAgentRequest, UpdateSandboxRequest, WebhookTriggerResponse, WorkspaceFileEntry,
    WorkspaceFilesResponse,
};
use ps5_agent_inference::Ps5LmProvider;
use ps5_agent_logging::AppLogger;
use ps5_agent_manager::{
    ActiveTaskPolicy, AgentManager, CreateAgentInput, DeleteAgentOptions, UpdateAgentInput,
};
use ps5_agent_memory::{MemoryManager, MemoryQuery};
use ps5_agent_messaging::{MessageBus, SendMessageRequest as BusSendRequest};
use ps5_agent_runtime::{
    AgentRuntime, CreateScheduleInput, RecoveryReport, RuntimeMemoryProvider, RuntimeToolExecutor,
    TaskManager, ToolExecutionOutput, UpdateScheduleInput,
};
use ps5_agent_tools::{
    canonical_tool_id, files as tool_files, mcp::McpServerView, plugins::PluginView,
    skills::SkillSummary, specs::builtin_tool_specs, AgentToolBridge, SandboxBrowserManager,
    ToolExecContext, ToolRegistry,
};
use serde_json::{json, Value};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, RwLock, Weak};
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

/// Breaks the initialization cycle between `ToolRegistry`/`ToolExecContext`
/// and `AgentRuntime` using a `Weak<AgentRuntime>` populated right after runtime construction.
fn primary_workspace_root(cfg: &AppConfig) -> std::path::PathBuf {
    cfg.tools
        .allowed_paths
        .first()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| cfg.data_dir.join("workspace"))
}

fn sandbox_state_dto(cfg: &AppConfig) -> SandboxStateDto {
    SandboxStateDto {
        mode: cfg.sandbox.mode.clone(),
        approval_policy: cfg.sandbox.approval_policy.clone(),
        network_access: cfg.sandbox.network_access,
        allow_local_network: cfg.sandbox.allow_local_network,
        web_search_provider: cfg.sandbox.web_search_provider.clone(),
        searxng_url: cfg.sandbox.searxng_url.clone(),
        has_brave_api_key: !cfg.sandbox.brave_api_key.trim().is_empty()
            || !cfg.tools.brave_api_key.trim().is_empty(),
        has_tavily_api_key: !cfg.sandbox.tavily_api_key.trim().is_empty()
            || !cfg.tools.tavily_api_key.trim().is_empty(),
        remote_browser_url: cfg.sandbox.remote_browser_url.clone(),
        allow_host_shell: cfg.sandbox.allow_host_shell,
        max_exec_output_bytes: cfg.sandbox.max_exec_output_bytes,
        exec_timeout_secs: cfg.sandbox.exec_timeout_secs,
        inject_brain_files: cfg.sandbox.inject_brain_files,
        max_brain_context_chars: cfg.sandbox.max_brain_context_chars,
        heartbeat_interval_secs: cfg.sandbox.heartbeat_interval_secs,
        allowed_domains: cfg.tools.allowed_domains.clone(),
        blocked_domains: cfg.tools.blocked_domains.clone(),
    }
}

pub struct AppToolBridge {
    agent_manager: Arc<AgentManager>,
    message_bus: Arc<MessageBus>,
    task_manager: OnceLock<Arc<TaskManager>>,
    memory_manager: Arc<MemoryManager>,
    runtime: OnceLock<Weak<AgentRuntime>>,
    events_tx: OnceLock<broadcast::Sender<StreamEvent>>,
    browser_manager: OnceLock<Arc<SandboxBrowserManager>>,
}

impl AppToolBridge {
    pub fn new(
        agent_manager: Arc<AgentManager>,
        message_bus: Arc<MessageBus>,
        memory_manager: Arc<MemoryManager>,
    ) -> Self {
        Self {
            agent_manager,
            message_bus,
            task_manager: OnceLock::new(),
            memory_manager,
            runtime: OnceLock::new(),
            events_tx: OnceLock::new(),
            browser_manager: OnceLock::new(),
        }
    }

    pub fn attach_runtime(&self, runtime: &Arc<AgentRuntime>) {
        let _ = self.task_manager.set(runtime.task_manager().clone());
        let _ = self.runtime.set(Arc::downgrade(runtime));
    }

    pub fn attach_events_tx(&self, events_tx: broadcast::Sender<StreamEvent>) {
        let _ = self.events_tx.set(events_tx);
    }

    pub fn attach_browser_manager(&self, browser_manager: Arc<SandboxBrowserManager>) {
        let _ = self.browser_manager.set(browser_manager);
    }

    fn require_runtime(&self) -> Result<Arc<AgentRuntime>> {
        self.runtime
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(|| AgentError::Internal("runtime is not initialized on tool bridge".into()))
    }

    fn workspace_root(&self) -> Result<std::path::PathBuf> {
        let rt = self.require_runtime()?;
        let cfg = rt.config();
        Ok(primary_workspace_root(&cfg))
    }

    fn effective_sandbox(&self) -> (SandboxMode, SandboxApprovalPolicy) {
        if let Some(rt) = self.runtime.get().and_then(Weak::upgrade) {
            let cfg = rt.config();
            let mode = SandboxMode::parse(&cfg.sandbox.mode).unwrap_or_default();
            let policy =
                SandboxApprovalPolicy::parse(&cfg.sandbox.approval_policy).unwrap_or_default();
            return (mode, policy);
        }
        (SandboxMode::default(), SandboxApprovalPolicy::default())
    }
}

#[async_trait]
impl AgentToolBridge for AppToolBridge {
    async fn send_agent_message(
        &self,
        from_agent_id: &str,
        to_agent_id: &str,
        task_id: Option<&str>,
        content: &str,
    ) -> Result<Value> {
        let env = self.message_bus.send(BusSendRequest {
            sender_agent_id: from_agent_id.to_string(),
            recipient_agent_id: to_agent_id.to_string(),
            task_id: task_id.map(str::to_string),
            message_type: AgentMessageType::AgentMessage,
            content: content.to_string(),
            hop_count: 1,
        })?;
        Ok(json!({
            "message_id": env.id,
            "status": env.status,
            "recipient": env.recipient_agent_id,
        }))
    }

    async fn assign_agent_task(
        &self,
        from_agent_id: &str,
        target_agent_id: &str,
        user_request: &str,
        parent_task_id: Option<&str>,
        conversation_id: Option<&str>,
    ) -> Result<Value> {
        let runtime = self.require_runtime()?;
        let target = self
            .agent_manager
            .get_agent(target_agent_id)?
            .ok_or_else(|| AgentError::NotFound(format!("agent '{target_agent_id}' not found")))?;
        if !target.enabled {
            return Err(AgentError::PermissionDenied(format!(
                "agent '{target_agent_id}' is disabled"
            )));
        }

        let subtask = runtime.task_manager().create_task(
            user_request.to_string(),
            target.id.clone(),
            conversation_id.map(str::to_string),
            parent_task_id.map(str::to_string),
            target.priority,
            TaskStatus::Assigned,
        )?;

        let _ = self.message_bus.send(BusSendRequest {
            sender_agent_id: from_agent_id.to_string(),
            recipient_agent_id: target.id.clone(),
            task_id: Some(subtask.id.clone()),
            message_type: AgentMessageType::TaskAssignment,
            content: user_request.to_string(),
            hop_count: 1,
        });

        match runtime.run_task_by_id(&subtask.id).await {
            Ok(outcome) => {
                let _ = self.message_bus.send(BusSendRequest {
                    sender_agent_id: target.id.clone(),
                    recipient_agent_id: from_agent_id.to_string(),
                    task_id: Some(subtask.id.clone()),
                    message_type: AgentMessageType::TaskCompleted,
                    content: outcome
                        .result
                        .clone()
                        .unwrap_or_else(|| format!("Task status: {:?}", outcome.status)),
                    hop_count: 1,
                });
                Ok(json!({
                    "task_id": outcome.task_id,
                    "status": outcome.status,
                    "result": outcome.result,
                    "pending_approval_id": outcome.pending_approval_id,
                }))
            }
            Err(err) => {
                let _ = self.message_bus.send(BusSendRequest {
                    sender_agent_id: target.id.clone(),
                    recipient_agent_id: from_agent_id.to_string(),
                    task_id: Some(subtask.id.clone()),
                    message_type: AgentMessageType::ErrorReport,
                    content: err.to_string(),
                    hop_count: 1,
                });
                Err(err)
            }
        }
    }

    async fn query_agent_status(
        &self,
        _requester_agent_id: &str,
        target_agent_id: Option<&str>,
        task_id: Option<&str>,
    ) -> Result<Value> {
        let task_info = if let Some(tid) = task_id {
            self.task_manager
                .get()
                .and_then(|tm| tm.get_task(tid).ok().flatten())
        } else {
            None
        };

        if let Some(aid) = target_agent_id {
            let cards = self.agent_manager.list_cards()?;
            let card = cards.into_iter().find(|c| c.definition.id == aid);
            return Ok(json!({
                "agent": card,
                "task": task_info,
            }));
        }

        let cards = self.agent_manager.list_cards()?;
        Ok(json!({
            "agents": cards,
            "task": task_info,
        }))
    }

    async fn read_agent_memory(
        &self,
        requester_agent_id: &str,
        target_agent_id: &str,
        query: Option<&str>,
        limit: usize,
    ) -> Result<Value> {
        let requester = self
            .agent_manager
            .get_agent(requester_agent_id)?
            .ok_or_else(|| {
                AgentError::NotFound(format!("agent '{requester_agent_id}' not found"))
            })?;
        let mut items = self.memory_manager.read_agent_memory(
            requester_agent_id,
            &requester.memory_permissions,
            Some(target_agent_id),
            None,
            None,
            false,
            limit.saturating_mul(2).max(10),
        )?;
        if let Some(q) = query.map(str::trim).filter(|s| !s.is_empty()) {
            let q_lower = q.to_ascii_lowercase();
            items.retain(|m| {
                m.key.to_ascii_lowercase().contains(&q_lower)
                    || m.content.to_ascii_lowercase().contains(&q_lower)
                    || m.namespace.to_ascii_lowercase().contains(&q_lower)
            });
        }
        items.truncate(limit);
        Ok(json!({ "memories": items }))
    }

    async fn request_temporary_specialist(
        &self,
        requester_agent_id: &str,
        name: &str,
        role_label: &str,
        system_prompt: &str,
        enabled_tools: Vec<String>,
        task_request: Option<&str>,
        parent_task_id: Option<&str>,
    ) -> Result<Value> {
        let specialist = self.agent_manager.create_agent(
            CreateAgentInput {
                id: None,
                name: name.to_string(),
                description: format!("Temporary specialist requested by {requester_agent_id}"),
                role: ps5_agent_core::AgentRole::Custom,
                role_label: Some(role_label.to_string()),
                system_prompt: system_prompt.to_string(),
                model_config: None,
                enabled_tools,
                denied_tools: None,
                memory_permissions: None,
                max_task_duration_secs: Some(0),
                max_reasoning_iterations: Some(0),
                priority: Some(0),
                can_communicate: Some(true),
                enabled: Some(true),
                sandbox_mode: None,
                tool_profile: None,
                is_temporary: true,
                spawned_by: Some(requester_agent_id.to_string()),
            },
            requester_agent_id,
        )?;

        if let Some(req_text) = task_request.filter(|s| !s.trim().is_empty()) {
            match self
                .assign_agent_task(
                    requester_agent_id,
                    &specialist.id,
                    req_text,
                    parent_task_id,
                    None,
                )
                .await
            {
                Ok(outcome) => {
                    return Ok(json!({
                        "specialist": specialist,
                        "task_outcome": outcome,
                    }));
                }
                Err(err) => {
                    let _ = self
                        .agent_manager
                        .delete_agent(
                            &specialist.id,
                            DeleteAgentOptions {
                                active_task_policy: ActiveTaskPolicy::CancelActive,
                                delete_memory: true,
                                actor: requester_agent_id.to_string(),
                            },
                        )
                        .await;
                    return Err(err);
                }
            }
        }

        Ok(json!({ "specialist": specialist }))
    }

    async fn memory_read(
        &self,
        agent_id: &str,
        scope: Option<&str>,
        namespace: Option<&str>,
        query: Option<&str>,
        limit: usize,
    ) -> Result<Value> {
        let agent = self
            .agent_manager
            .get_agent(agent_id)?
            .ok_or_else(|| AgentError::NotFound(format!("agent '{agent_id}' not found")))?;
        let parsed_scope = match scope.map(str::trim).filter(|s| !s.is_empty()) {
            Some(raw) => Some(parse_memory_scope_str(raw).ok_or_else(|| {
                AgentError::InvalidInput(format!("invalid memory scope '{raw}'"))
            })?),
            None => None,
        };
        let ns_clean = namespace.map(str::trim).filter(|s| !s.is_empty());
        let q_clean = query.map(str::trim).filter(|s| !s.is_empty());
        let capped_limit = limit.clamp(1, 100);
        let fetch_limit = if q_clean.is_some() {
            (capped_limit * 8).clamp(capped_limit, 500)
        } else {
            capped_limit
        };

        let mut items = self.memory_manager.read_agent_memory(
            agent_id,
            &agent.memory_permissions,
            None,
            parsed_scope,
            ns_clean,
            false,
            fetch_limit,
        )?;
        if let Some(q) = q_clean {
            let q_lower = q.to_ascii_lowercase();
            items.retain(|item| {
                item.key.to_ascii_lowercase().contains(&q_lower)
                    || item.content.to_ascii_lowercase().contains(&q_lower)
                    || item.namespace.to_ascii_lowercase().contains(&q_lower)
                    || item
                        .summary
                        .as_deref()
                        .map(|s| s.to_ascii_lowercase().contains(&q_lower))
                        .unwrap_or(false)
            });
        }
        items.truncate(capped_limit);

        Ok(json!({
            "count": items.len(),
            "memories": items,
        }))
    }

    async fn memory_write(
        &self,
        agent_id: &str,
        scope: &str,
        namespace: &str,
        key: &str,
        content: &str,
        importance: u8,
        conversation_id: Option<&str>,
        task_id: Option<&str>,
    ) -> Result<Value> {
        let agent = self
            .agent_manager
            .get_agent(agent_id)?
            .ok_or_else(|| AgentError::NotFound(format!("agent '{agent_id}' not found")))?;
        let parsed_scope = parse_memory_scope_str(scope.trim()).ok_or_else(|| {
            AgentError::InvalidInput(format!("invalid memory scope '{scope}'"))
        })?;
        let ns = {
            let t = namespace.trim();
            if t.is_empty() {
                "general"
            } else {
                t
            }
        };
        let key_clean = key.trim();
        if key_clean.is_empty() {
            return Err(AgentError::InvalidInput(
                "memory key must not be empty".into(),
            ));
        }
        let now = now_iso();
        let item = MemoryItem {
            id: ps5_agent_core::new_id(),
            scope: parsed_scope,
            namespace: ns.to_string(),
            owner_agent_id: if parsed_scope == MemoryScope::Agent {
                Some(agent.id.clone())
            } else {
                None
            },
            conversation_id: conversation_id.map(str::to_string),
            task_id: task_id.map(str::to_string),
            key: key_clean.to_string(),
            content: content.to_string(),
            summary: None,
            importance: importance.clamp(1, 10),
            tags: Vec::new(),
            archived: false,
            created_at: now.clone(),
            updated_at: now,
        };
        let saved = self
            .memory_manager
            .write_agent_memory(&agent.id, &agent.memory_permissions, item)?;
        Ok(json!({
            "ok": true,
            "memory": saved,
        }))
    }

    async fn process_action(&self, action: &str, target_id: Option<&str>) -> Result<Value> {
        let tm = self
            .task_manager
            .get()
            .ok_or_else(|| AgentError::Internal("task manager is not initialized".into()))?;
        let action_norm = action.trim().to_ascii_lowercase();
        match action_norm.as_str() {
            "list" => {
                let tasks = tm.list_tasks(100)?;
                let active_count = tasks.iter().filter(|t| !t.status.is_terminal()).count();
                Ok(json!({
                    "action": "list",
                    "active_count": active_count,
                    "total": tasks.len(),
                    "tasks": tasks,
                }))
            }
            "status" => {
                let id = target_id
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| {
                        AgentError::InvalidInput(
                            "process action 'status' requires 'target_id'".into(),
                        )
                    })?;
                let task = tm
                    .get_task(id)?
                    .ok_or_else(|| AgentError::NotFound(format!("task '{id}' not found")))?;
                Ok(json!({
                    "action": "status",
                    "task": task,
                }))
            }
            "stop" | "cancel" => {
                let id = target_id
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| {
                        AgentError::InvalidInput(format!(
                            "process action '{action_norm}' requires 'target_id'"
                        ))
                    })?;
                let task = tm.cancel_task(id, "stopped via process tool")?;
                Ok(json!({
                    "action": action_norm,
                    "stopped": true,
                    "task": task,
                }))
            }
            other => Err(AgentError::InvalidInput(format!(
                "unsupported process action '{other}'"
            ))),
        }
    }

    async fn memory_search(
        &self,
        _agent_id: &str,
        query: &str,
        max_results: usize,
    ) -> Result<Value> {
        let ws = self.workspace_root()?;
        let q = query.to_string();
        let results = tokio::task::spawn_blocking(move || {
            ps5_agent_runtime::brain::search_markdown_memories(&ws, &q, max_results)
        })
        .await
        .map_err(|e| AgentError::Internal(format!("memory_search blocking task failed: {e}")))??;
        Ok(json!({
            "query": query,
            "max_results": max_results,
            "count": results.len(),
            "results": results,
        }))
    }

    async fn memory_get(
        &self,
        _agent_id: &str,
        path: &str,
        from_line: Option<usize>,
        lines: Option<usize>,
    ) -> Result<Value> {
        let ws = self.workspace_root()?;
        let p = path.to_string();
        tokio::task::spawn_blocking(move || {
            ps5_agent_runtime::brain::read_markdown_memory_slice(&ws, &p, from_line, lines)
        })
        .await
        .map_err(|e| AgentError::Internal(format!("memory_get blocking task failed: {e}")))?
    }

    async fn system_info(&self) -> Result<Value> {
        let cfg_opt = self.runtime.get().and_then(Weak::upgrade).map(|rt| rt.config());
        let agents_count = self
            .agent_manager
            .list_agents()
            .map(|v| v.len())
            .unwrap_or(0);
        let active_tasks_count = self
            .task_manager
            .get()
            .and_then(|tm| tm.list_tasks(200).ok())
            .map(|tasks| tasks.into_iter().filter(|t| !t.status.is_terminal()).count())
            .unwrap_or(0);
        let browser_sessions = self
            .browser_manager
            .get()
            .map(|bm| bm.list_sessions())
            .unwrap_or_else(|| json!({ "count": 0, "sessions": [] }));
        let (sandbox_mode, approval_policy, network_access, allow_host_shell, workspace_root) =
            match &cfg_opt {
                Some(cfg) => (
                    Some(cfg.sandbox.mode.clone()),
                    Some(cfg.sandbox.approval_policy.clone()),
                    Some(cfg.sandbox.network_access),
                    Some(cfg.sandbox.allow_host_shell),
                    Some(primary_workspace_root(cfg).to_string_lossy().into_owned()),
                ),
                None => (None, None, None, None, None),
            };
        Ok(json!({
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
            "family": std::env::consts::FAMILY,
            "agents_count": agents_count,
            "active_tasks_count": active_tasks_count,
            "browser_sessions": browser_sessions,
            "sandbox_mode": sandbox_mode,
            "approval_policy": approval_policy,
            "network_access": network_access,
            "allow_host_shell": allow_host_shell,
            "workspace_root": workspace_root,
        }))
    }

    async fn ps5_info(&self) -> Result<Value> {
        let (fw_version, model, cpu_temp_c, soc_temp_c) = read_ps5_telemetry();
        let cfg_opt = self.runtime.get().and_then(Weak::upgrade).map(|rt| rt.config());
        let allow_host_shell = cfg_opt
            .as_ref()
            .map(|c| c.sandbox.allow_host_shell)
            .unwrap_or(false);
        Ok(json!({
            "target_vendor": if cfg!(target_vendor = "sie") { "sie" } else { std::env::consts::OS },
            "is_ps5": cfg!(target_vendor = "sie"),
            "fw_version": fw_version,
            "model": model,
            "cpu_temp_c": cpu_temp_c,
            "soc_temp_c": soc_temp_c,
            "virtual_shell_default": true,
            "host_shell_available": !cfg!(target_vendor = "sie") && allow_host_shell,
            "sandbox_mode": cfg_opt.as_ref().map(|c| c.sandbox.mode.clone()),
            "approval_policy": cfg_opt.as_ref().map(|c| c.sandbox.approval_policy.clone()),
            "network_access": cfg_opt.as_ref().map(|c| c.sandbox.network_access),
        }))
    }

    async fn sessions_list(&self, limit: usize, active_only: bool) -> Result<Value> {
        let rt = self.require_runtime()?;
        let mut convs = rt.storage().list_conversations(false)?;
        let tasks = self
            .task_manager
            .get()
            .map(|tm| tm.list_tasks(500))
            .transpose()?
            .unwrap_or_default();
        let mut active_conv_ids = std::collections::HashSet::new();
        for t in &tasks {
            if !t.status.is_terminal() {
                if let Some(ref cid) = t.conversation_id {
                    active_conv_ids.insert(cid.clone());
                }
            }
        }
        if active_only {
            convs.retain(|c| active_conv_ids.contains(&c.id));
        }
        convs.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
        convs.truncate(limit.clamp(1, 200));
        let sessions: Vec<Value> = convs
            .into_iter()
            .map(|c| {
                let is_active = active_conv_ids.contains(&c.id);
                json!({
                    "id": c.id,
                    "title": c.title,
                    "mode": c.mode,
                    "active": is_active,
                    "created_at": c.created_at,
                    "updated_at": c.updated_at,
                })
            })
            .collect();
        Ok(json!({
            "count": sessions.len(),
            "active_only": active_only,
            "sessions": sessions,
        }))
    }

    async fn sessions_spawn(
        &self,
        sender_agent_id: &str,
        target_agent_id: Option<&str>,
        conversation_id: Option<&str>,
        prompt: &str,
    ) -> Result<Value> {
        let rt = self.require_runtime()?;
        let prompt_trimmed = prompt.trim();
        if prompt_trimmed.is_empty() {
            return Err(AgentError::InvalidInput(
                "sessions_spawn prompt must not be empty".into(),
            ));
        }
        let target_id = target_agent_id
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("commander");
        let target = self
            .agent_manager
            .get_agent(target_id)?
            .ok_or_else(|| AgentError::NotFound(format!("agent '{target_id}' not found")))?;
        if !target.enabled {
            return Err(AgentError::PermissionDenied(format!(
                "agent '{target_id}' is disabled"
            )));
        }
        if let Some(caller) = self.agent_manager.get_agent(sender_agent_id)? {
            if caller.id != target.id && !caller.can_communicate {
                return Err(AgentError::PermissionDenied(format!(
                    "agent '{}' is not allowed to spawn sessions for other agents",
                    caller.id
                )));
            }
        }

        let conv_id = match conversation_id.map(str::trim).filter(|s| !s.is_empty()) {
            Some(cid) => {
                if rt.storage().get_conversation(cid)?.is_none() {
                    return Err(AgentError::NotFound(format!(
                        "conversation '{cid}' not found"
                    )));
                }
                cid.to_string()
            }
            None => {
                let cid = ps5_agent_core::new_id();
                let now = now_iso();
                let conv = Conversation {
                    id: cid.clone(),
                    title: format!("Session: {}", truncate_utf8_bytes(prompt_trimmed, 48)),
                    mode: ConversationMode::Direct {
                        agent_id: target.id.clone(),
                    },
                    created_at: now.clone(),
                    updated_at: now,
                    archived: false,
                    sort_order: 0,
                };
                rt.storage().create_conversation(&conv)?;
                cid
            }
        };

        let (msg, task) = rt
            .dispatch_chat_background(
                conv_id.clone(),
                prompt_trimmed.to_string(),
                Some(target.id.clone()),
                None,
                Vec::new(),
                CancellationToken::new(),
            )
            .await?;

        Ok(json!({
            "ok": true,
            "conversation_id": conv_id,
            "task_id": task.id,
            "message_id": msg.id,
            "target": target.id,
            "spawned_by": sender_agent_id,
        }))
    }

    async fn sessions_send(
        &self,
        sender_agent_id: &str,
        conversation_id: &str,
        message: &str,
    ) -> Result<Value> {
        let rt = self.require_runtime()?;
        let conv_id = conversation_id.trim();
        let msg_trimmed = message.trim();
        if conv_id.is_empty() || msg_trimmed.is_empty() {
            return Err(AgentError::InvalidInput(
                "conversation_id and message must not be empty".into(),
            ));
        }
        if rt.storage().get_conversation(conv_id)?.is_none() {
            return Err(AgentError::NotFound(format!(
                "conversation '{conv_id}' not found"
            )));
        }
        let (user_msg, task) = rt
            .dispatch_chat_background(
                conv_id.to_string(),
                msg_trimmed.to_string(),
                None,
                None,
                Vec::new(),
                CancellationToken::new(),
            )
            .await?;
        Ok(json!({
            "ok": true,
            "conversation_id": conv_id,
            "task_id": task.id,
            "message_id": user_msg.id,
            "sender_agent_id": sender_agent_id,
        }))
    }

    async fn session_status(
        &self,
        conversation_id: Option<&str>,
        task_id: Option<&str>,
    ) -> Result<Value> {
        let rt = self.require_runtime()?;
        let conv_clean = conversation_id.map(str::trim).filter(|s| !s.is_empty());
        let task_clean = task_id.map(str::trim).filter(|s| !s.is_empty());
        let conv_opt = match conv_clean {
            Some(cid) => rt.storage().get_conversation(cid)?,
            None => None,
        };
        let task_opt = match task_clean {
            Some(tid) => self
                .task_manager
                .get()
                .map(|tm| tm.get_task(tid))
                .transpose()?
                .flatten(),
            None => None,
        };
        let conv_tasks: Vec<_> = match conv_clean {
            Some(cid) => self
                .task_manager
                .get()
                .map(|tm| tm.list_tasks(200))
                .transpose()?
                .unwrap_or_default()
                .into_iter()
                .filter(|t| t.conversation_id.as_deref() == Some(cid))
                .collect(),
            None => Vec::new(),
        };
        let recent_message_count = match conv_clean {
            Some(cid) => rt
                .storage()
                .list_chat_messages(cid, false, 200)
                .map(|m| m.len())
                .unwrap_or(0),
            None => 0,
        };
        Ok(json!({
            "conversation_id": conv_clean,
            "task_id": task_clean,
            "conversation": conv_opt,
            "task": task_opt,
            "conversation_tasks": conv_tasks,
            "recent_message_count": recent_message_count,
        }))
    }

    async fn run_doctor(&self, fix: bool) -> Result<Value> {
        let rt = self.require_runtime()?;
        let cfg = rt.config();
        let builtin_ids: Vec<String> = builtin_tool_specs(cfg.tools.exec_timeout_secs)
            .into_iter()
            .map(|s| s.id)
            .collect();
        if fix {
            let report = rt.run_doctor_fixes(&builtin_ids).await?;
            serde_json::to_value(report).map_err(|e| {
                AgentError::Internal(format!("failed serializing doctor fix report: {e}"))
            })
        } else {
            let report = rt.run_doctor_checks(&builtin_ids).await?;
            serde_json::to_value(report).map_err(|e| {
                AgentError::Internal(format!("failed serializing doctor report: {e}"))
            })
        }
    }

    async fn notify_artifact_created(
        &self,
        conversation_id: Option<&str>,
        _task_id: Option<&str>,
        rel_path: &str,
        _title: &str,
    ) -> Result<()> {
        if let Some(tx) = self.events_tx.get() {
            let _ = tx.send(StreamEvent::FilesChanged {
                conversation_id: conversation_id.map(str::to_string),
                path: rel_path.to_string(),
                action: "created".to_string(),
            });
        }
        Ok(())
    }
}

pub struct RuntimeToolExecutorImpl {
    tools: Arc<ToolRegistry>,
    bridge: Arc<AppToolBridge>,
}

impl RuntimeToolExecutorImpl {
    pub fn new(tools: Arc<ToolRegistry>, bridge: Arc<AppToolBridge>) -> Self {
        Self { tools, bridge }
    }

    fn is_mutating_tool(&self, tool_id: &str) -> bool {
        let canonical = canonical_tool_id(tool_id);
        if let Ok(Some(spec)) = self.tools.get_spec(canonical) {
            return spec.is_mutating;
        }
        matches!(
            canonical,
            "write_file" | "create_directory" | "delete_file" | "http_post" | "json_api_request"
        )
    }
}

#[async_trait]
impl RuntimeToolExecutor for RuntimeToolExecutorImpl {
    async fn list_tools(&self) -> Vec<ToolSpec> {
        self.tools.list_specs().unwrap_or_default()
    }

    async fn get_tool(&self, tool_id: &str) -> Option<ToolSpec> {
        self.tools.get_spec(tool_id).ok().flatten()
    }

    fn extension_prompt_context(&self) -> (Vec<(String, String)>, Vec<(String, String)>) {
        let skills = self
            .tools
            .list_enabled_skills()
            .into_iter()
            .map(|s| (s.name, s.description))
            .collect();
        let plugin_rules = self.tools.plugins().enabled_rules();
        (skills, plugin_rules)
    }

    async fn execute_tool(
        &self,
        agent: &AgentDefinition,
        task_id: Option<&str>,
        tool_id: &str,
        arguments: Value,
        approved_bypass: bool,
        cancel: CancellationToken,
    ) -> Result<ToolExecutionOutput> {
        let (sandbox_mode, approval_policy) = self.bridge.effective_sandbox();
        self.execute_tool_with_context(
            agent,
            task_id,
            None,
            tool_id,
            arguments,
            approved_bypass,
            sandbox_mode,
            approval_policy,
            cancel,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_tool_with_context(
        &self,
        agent: &AgentDefinition,
        task_id: Option<&str>,
        conversation_id: Option<&str>,
        tool_id: &str,
        arguments: Value,
        approved_bypass: bool,
        sandbox_mode: SandboxMode,
        approval_policy: SandboxApprovalPolicy,
        cancel: CancellationToken,
    ) -> Result<ToolExecutionOutput> {
        let ctx = ToolExecContext {
            agent_id: agent.id.clone(),
            task_id: task_id.map(str::to_string),
            conversation_id: conversation_id.map(str::to_string),
            approval_granted: approved_bypass,
            is_operator: false,
            sandbox_mode,
            approval_policy,
            a2a_bridge: Some(self.bridge.clone()),
        };

        let is_mutating = self.is_mutating_tool(tool_id);
        let output = tokio::select! {
            _ = cancel.cancelled() => {
                return Err(AgentError::Cancelled(format!("tool '{tool_id}' cancelled")));
            }
            res = self.tools.invoke(tool_id, arguments, &ctx) => res?,
        };

        Ok(ToolExecutionOutput {
            output,
            is_mutating,
        })
    }
}

pub struct RuntimeMemoryProviderImpl {
    memory: Arc<MemoryManager>,
    storage: Arc<dyn StorageBackend>,
    config: RwLock<MemoryConfig>,
}

impl RuntimeMemoryProviderImpl {
    pub fn new(
        memory: Arc<MemoryManager>,
        storage: Arc<dyn StorageBackend>,
        config: MemoryConfig,
    ) -> Self {
        Self {
            memory,
            storage,
            config: RwLock::new(config),
        }
    }

    pub fn update_config(&self, cfg: MemoryConfig) {
        if let Ok(mut g) = self.config.write() {
            *g = cfg;
        }
    }
}

#[async_trait]
impl RuntimeMemoryProvider for RuntimeMemoryProviderImpl {
    async fn build_context_for_agent(
        &self,
        agent: &AgentDefinition,
        conversation_id: Option<&str>,
        task_id: Option<&str>,
        query_hint: &str,
    ) -> Result<String> {
        let mem_cfg = self
            .config
            .read()
            .map(|g| g.clone())
            .unwrap_or_else(|_| self.memory.config().clone());

        if mem_cfg.archive_after_messages > 0 {
            if let Some(cid) = conversation_id {
                let active = self
                    .storage
                    .list_chat_messages(cid, false, mem_cfg.archive_after_messages.saturating_add(8))
                    .unwrap_or_default();
                if active.len() > mem_cfg.archive_after_messages {
                    let _ = self
                        .memory
                        .build_conversation_context(cid, "", None, 1024);
                }
            }
        }

        let items = self.memory.retrieve_context(&MemoryQuery {
            reader_agent_id: &agent.id,
            permissions: &agent.memory_permissions,
            query_text: query_hint,
            conversation_id,
            task_id,
            namespace: None,
            scopes: None,
            limit: Some(mem_cfg.max_context_memories.max(1)),
        })?;

        Ok(self.memory.format_memory_block_default(&items))
    }

    async fn store_memory(
        &self,
        agent: &AgentDefinition,
        scope: MemoryScope,
        namespace: &str,
        key: &str,
        content: &str,
        importance: u8,
        conversation_id: Option<&str>,
        task_id: Option<&str>,
    ) -> Result<MemoryItem> {
        let now = now_iso();
        let item = MemoryItem {
            id: ps5_agent_core::new_id(),
            scope,
            namespace: namespace.to_string(),
            owner_agent_id: if scope == MemoryScope::Agent {
                Some(agent.id.clone())
            } else {
                None
            },
            conversation_id: conversation_id.map(str::to_string),
            task_id: task_id.map(str::to_string),
            key: key.to_string(),
            content: content.to_string(),
            summary: None,
            importance,
            tags: Vec::new(),
            archived: false,
            created_at: now.clone(),
            updated_at: now,
        };
        self.memory
            .write_agent_memory(&agent.id, &agent.memory_permissions, item)
    }
}

pub struct AppSystem {
    pub config: RwLock<AppConfig>,
    pub store: Arc<dyn StorageBackend>,
    pub db_metrics: Arc<DbMetrics>,
    pub inference: Arc<Ps5LmProvider>,
    pub tools: Arc<ToolRegistry>,
    pub memory: Arc<MemoryManager>,
    pub memory_provider: Arc<RuntimeMemoryProviderImpl>,
    pub tool_bridge: Arc<AppToolBridge>,
    pub agent_manager: Arc<AgentManager>,
    pub message_bus: Arc<MessageBus>,
    pub runtime: Arc<AgentRuntime>,
    pub events_tx: broadcast::Sender<StreamEvent>,
    pub logger: AppLogger,
    pub shutdown: CancellationToken,
    pub recovery_report: RecoveryReport,
    pub http_backend: Arc<AppHttpBackend>,
    pub http_state: Arc<ps5_agent_http::HttpState>,
}

pub struct AppHttpBackend {
    pub started_at: Instant,
    pub store: Arc<dyn StorageBackend>,
    pub db_metrics: Arc<DbMetrics>,
    pub inference: Arc<Ps5LmProvider>,
    pub tools: Arc<ToolRegistry>,
    pub tool_bridge: Arc<AppToolBridge>,
    pub agent_manager: Arc<AgentManager>,
    pub message_bus: Arc<MessageBus>,
    pub memory_provider: Arc<RuntimeMemoryProviderImpl>,
    pub runtime: Arc<AgentRuntime>,
    pub events_tx: broadcast::Sender<StreamEvent>,
    pub logger: AppLogger,
    pub ps5lm_health: RwLock<(bool, Option<String>)>,
    pub ps5lm_capabilities: RwLock<Option<Ps5LmCapabilities>>,
    pub auto_synced_slots: AtomicUsize,
}

impl AppHttpBackend {
    pub async fn refresh_ps5lm_health(&self) -> (bool, Option<String>) {
        let probe = tokio::time::timeout(Duration::from_secs(2), self.inference.health_check()).await;
        let state = match probe {
            Ok(h) => (h.reachable, h.loaded_model),
            Err(_) => (false, None),
        };
        if let Ok(mut guard) = self.ps5lm_health.write() {
            *guard = state.clone();
        }
        if let Ok(mut caps_guard) = self.ps5lm_capabilities.write() {
            if let Some(ref mut caps) = *caps_guard {
                caps.healthy = state.0;
                if state.1.is_some() {
                    caps.loaded_model = state.1.clone();
                }
            }
        }
        state
    }

    pub async fn sync_ps5lm_capabilities(&self) -> Result<Ps5LmCapabilities> {
        self.sync_ps5lm_capabilities_with_notify(false).await
    }

    async fn sync_ps5lm_capabilities_with_notify(
        &self,
        always_notify_on_healthy: bool,
    ) -> Result<Ps5LmCapabilities> {
        let mut cfg = self.runtime.config();
        let timeout_secs = cfg.inference.connect_timeout_secs.max(3);
        let probe = tokio::time::timeout(
            Duration::from_secs(timeout_secs),
            self.inference.discover_capabilities(),
        )
        .await;

        let caps = match probe {
            Ok(Ok(Some(c))) => c,
            Ok(Ok(None)) | Err(_) => {
                let h = self.inference.health_check().await;
                Ps5LmCapabilities {
                    base_url: cfg.inference.base_url.clone(),
                    healthy: h.reachable,
                    loaded_model: h.loaded_model,
                    available_models: Vec::new(),
                    context_length: None,
                    total_slots: None,
                    supports_vision: false,
                    supports_thinking: false,
                    synced_at: Some(now_iso()),
                }
            }
            Ok(Err(e)) => return Err(e),
        };

        if let Ok(mut guard) = self.ps5lm_health.write() {
            *guard = (caps.healthy, caps.loaded_model.clone());
        }
        if let Ok(mut guard) = self.ps5lm_capabilities.write() {
            *guard = Some(caps.clone());
        }

        let mut notified = false;
        if caps.healthy && cfg.inference.auto_sync_from_ps5lm {
            let mut config_changed = false;
            let mut max_conc_changed = false;

            if let Some(ref loaded) = caps.loaded_model {
                let trimmed = loaded.trim();
                if !trimmed.is_empty() && cfg.inference.model != trimmed {
                    cfg.inference.model = trimmed.to_string();
                    config_changed = true;
                }
            }
            if let Some(ctx) = caps.context_length {
                if ctx > 0 && cfg.inference.context_limit != ctx {
                    cfg.inference.context_limit = ctx;
                    config_changed = true;
                }
            }
            if let Some(slots) = caps.total_slots {
                if slots > 0 {
                    let slots_usize = slots as usize;
                    let prev_auto = self.auto_synced_slots.load(Ordering::Relaxed);
                    let cur = cfg.agents.max_concurrent_inference;
                    if cur == 0 || cur == 4 || (prev_auto > 0 && cur == prev_auto) {
                        if cur != slots_usize {
                            cfg.agents.max_concurrent_inference = slots_usize;
                            config_changed = true;
                            max_conc_changed = true;
                        }
                        self.auto_synced_slots.store(slots_usize, Ordering::Relaxed);
                    }
                }
            }

            let _ = self.inference.apply_discovered_context(&caps);
            if max_conc_changed {
                self.inference
                    .set_max_concurrent(cfg.agents.max_concurrent_inference);
            }
            if config_changed {
                self.runtime.update_config(cfg);
                let _ = self.events_tx.send(StreamEvent::Ps5LmSynced {
                    loaded_model: caps.loaded_model.clone(),
                    context_length: caps.context_length,
                    total_slots: caps.total_slots,
                });
                notified = true;
            }
        }

        if always_notify_on_healthy && caps.healthy && !notified {
            let _ = self.events_tx.send(StreamEvent::Ps5LmSynced {
                loaded_model: caps.loaded_model.clone(),
                context_length: caps.context_length,
                total_slots: caps.total_slots,
            });
        }

        Ok(caps)
    }
}

pub(crate) fn sync_plugin_mcp_configs(tools: &ToolRegistry, logger: &AppLogger) {
    let plugin_mcps = tools.plugins().enabled_mcp_configs();
    for (plugin_id, raw_val) in plugin_mcps {
        let normalized_json = match raw_val {
            Value::Object(ref obj)
                if obj.contains_key("mcpServers") || obj.contains_key("mcp_servers") =>
            {
                serde_json::to_string(&raw_val)
            }
            Value::Object(mut obj) if obj.contains_key("url") => {
                if !obj.contains_key("id") {
                    obj.insert("id".to_string(), Value::String(plugin_id.clone()));
                }
                serde_json::to_string(&Value::Object(obj))
            }
            Value::Object(_) => serde_json::to_string(&json!({ "mcpServers": raw_val })),
            _ => continue,
        };
        let Ok(raw_json) = normalized_json else {
            continue;
        };
        if let Err(e) = tools.mcp().add_or_import(&raw_json) {
            logger.warn(
                "extensions",
                format!("failed to import MCP config from plugin '{plugin_id}': {e}"),
            );
        }
    }
    if let Err(e) = tools.sync_specs() {
        logger.warn("extensions", format!("failed to sync tool specs: {e}"));
    }
}

fn file_extension_lower(name: &str) -> String {
    Path::new(name)
        .extension()
        .and_then(|s| s.to_str())
        .map(|s| s.to_ascii_lowercase())
        .unwrap_or_default()
}

fn classify_workspace_entry(name: &str, is_dir: bool) -> (String, bool) {
    if is_dir {
        return ("dir".to_string(), false);
    }
    let ext = file_extension_lower(name);
    let previewable = matches!(
        ext.as_str(),
        "html" | "htm" | "png" | "jpg" | "jpeg" | "webp" | "gif"
    );
    let kind = match ext.as_str() {
        "html" | "htm" => "html",
        "png" | "jpg" | "jpeg" | "webp" | "gif" | "svg" => "image",
        "txt" | "md" | "json" | "toml" | "yaml" | "yml" | "rs" | "js" | "ts" | "css" | "py"
        | "c" | "cpp" | "h" | "hpp" | "sh" | "csv" | "xml" | "log" | "sql" => "text",
        _ => "binary",
    };
    (kind.to_string(), previewable)
}

fn truncate_utf8_bytes(s: &str, max_bytes: usize) -> String {
    if s.len() <= max_bytes {
        return s.to_string();
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    s[..end].to_string()
}

fn clean_rel_path(raw: &str, allowed_roots: &[String]) -> String {
    let trimmed = raw.trim();
    let norm = trimmed.replace('\\', "/");
    for root in allowed_roots {
        let root_norm = root.trim().replace('\\', "/");
        let root_trimmed = root_norm.trim_end_matches('/');
        if root_trimmed.is_empty() {
            continue;
        }
        if norm == root_trimmed {
            return String::new();
        }
        let prefix = format!("{root_trimmed}/");
        if let Some(rest) = norm.strip_prefix(&prefix) {
            let mut rel = rest.trim_start_matches('/');
            while let Some(next) = rel.strip_prefix("./") {
                rel = next;
            }
            return rel.to_string();
        }
        if let (Ok(canon_root), Ok(canon_raw)) =
            (std::fs::canonicalize(root_trimmed), std::fs::canonicalize(trimmed))
        {
            if let Ok(rel) = canon_raw.strip_prefix(&canon_root) {
                return rel.to_string_lossy().replace('\\', "/");
            }
        }
    }
    let mut rel = norm.trim_start_matches('/');
    while let Some(next) = rel.strip_prefix("./") {
        rel = next;
    }
    rel.to_string()
}

fn sanitize_upload_filename(raw: &str) -> String {
    let leaf = Path::new(raw.trim())
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("attachment.bin");
    let cleaned: String = leaf
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect();
    let trimmed = cleaned.trim_matches('.');
    if trimmed.is_empty() || trimmed == "." || trimmed == ".." {
        "attachment.bin".to_string()
    } else {
        trimmed.to_string()
    }
}

fn decode_base64_str(input: &str) -> Result<Vec<u8>> {
    fn val(b: u8) -> Option<u8> {
        match b {
            b'A'..=b'Z' => Some(b - b'A'),
            b'a'..=b'z' => Some(b - b'a' + 26),
            b'0'..=b'9' => Some(b - b'0' + 52),
            b'+' | b'-' => Some(62),
            b'/' | b'_' => Some(63),
            _ => None,
        }
    }

    let cleaned: Vec<u8> = input
        .bytes()
        .filter(|b| !b.is_ascii_whitespace())
        .collect();
    if cleaned.is_empty() {
        return Ok(Vec::new());
    }
    if cleaned.len() % 4 == 1 {
        return Err(AgentError::InvalidInput("invalid base64 payload".into()));
    }

    let mut out = Vec::with_capacity((cleaned.len() / 4) * 3);
    for chunk in cleaned.chunks(4) {
        let b0 = val(chunk[0]).ok_or_else(|| AgentError::InvalidInput("invalid base64".into()))?;
        let b1 = val(chunk[1]).ok_or_else(|| AgentError::InvalidInput("invalid base64".into()))?;
        out.push((b0 << 2) | (b1 >> 4));

        if chunk.len() > 2 && chunk[2] != b'=' {
            let b2 =
                val(chunk[2]).ok_or_else(|| AgentError::InvalidInput("invalid base64".into()))?;
            out.push((b1 << 4) | (b2 >> 2));
            if chunk.len() > 3 && chunk[3] != b'=' {
                let b3 = val(chunk[3])
                    .ok_or_else(|| AgentError::InvalidInput("invalid base64".into()))?;
                out.push((b2 << 6) | b3);
            }
        }
    }
    Ok(out)
}

fn decode_data_url_bytes(raw: &str) -> Result<Vec<u8>> {
    let trimmed = raw.trim();
    if let Some(rest) = trimmed.strip_prefix("data:") {
        let (meta, payload) = rest
            .split_once(',')
            .ok_or_else(|| AgentError::InvalidInput("malformed data URL".into()))?;
        if meta
            .split(';')
            .any(|part| part.trim().eq_ignore_ascii_case("base64"))
        {
            return decode_base64_str(payload);
        }
        return Ok(payload.as_bytes().to_vec());
    }
    Ok(trimmed.as_bytes().to_vec())
}

fn is_image_attachment(att: &ChatAttachment) -> bool {
    att.kind.eq_ignore_ascii_case("image")
        || att.mime_type.to_ascii_lowercase().starts_with("image/")
}

fn skill_to_dto(s: SkillSummary) -> SkillInfoDto {
    SkillInfoDto {
        id: s.id,
        name: s.name,
        description: s.description,
        enabled: s.enabled,
        source: s.source,
        dir_path: s.dir_path,
        has_scripts: s.has_scripts,
        has_references: s.has_references,
        resources: s.resources,
    }
}

fn plugin_to_dto(p: PluginView) -> PluginInfoDto {
    PluginInfoDto {
        id: p.id,
        name: p.name,
        display_name: p.display_name,
        description: p.description,
        version: p.version,
        enabled: p.enabled,
        rules_count: p.rules_count,
        skills_count: p.skills_count,
        tools: p.tools,
        has_mcp: p.has_mcp,
    }
}

fn mcp_to_dto(m: McpServerView) -> McpServerDto {
    McpServerDto {
        id: m.id,
        url: m.url,
        enabled: m.enabled,
        trust_local: m.trust_local,
        has_auth: m.has_auth,
        masked_headers: m.masked_headers,
        status: m.status,
        last_error: m.last_error,
        last_synced_at: m.last_synced_at,
        tools: m
            .tools
            .into_iter()
            .map(|t| McpToolDto {
                namespaced_id: t.namespaced_id,
                server_id: t.server_id,
                tool_name: t.tool_name,
                description: t.description,
                input_schema: t.input_schema,
            })
            .collect(),
    }
}

fn parse_thinking_level_str(raw: &str) -> Result<&'static str> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "off" => Ok("off"),
        "low" => Ok("low"),
        "medium" => Ok("medium"),
        "high" => Ok("high"),
        "max" => Ok("max"),
        other => Err(AgentError::InvalidInput(format!(
            "unknown inference.thinking_level '{other}'"
        ))),
    }
}

fn read_ps5_telemetry() -> (Option<String>, Option<String>, Option<i32>, Option<i32>) {
    #[cfg(target_vendor = "sie")]
    {
        let fw = Some(format!("0x{:08x}", ps5::fw_version()));
        let model = ps5::hw::model_name().ok().map(|s| s.as_str().to_string());
        let cpu = ps5::hw::cpu_temperature().ok();
        let soc = ps5::hw::soc_temperature(0).ok();
        (fw, model, cpu, soc)
    }
    #[cfg(not(target_vendor = "sie"))]
    {
        (
            Some("host-sim".to_string()),
            Some("Host Dev Environment".to_string()),
            None,
            None,
        )
    }
}

fn parse_api_mode_str(raw: &str) -> Result<ApiMode> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "chat_completions" => Ok(ApiMode::ChatCompletions),
        "responses" => Ok(ApiMode::Responses),
        "anthropic_messages" | "messages" => Ok(ApiMode::AnthropicMessages),
        "completions" => Ok(ApiMode::Completions),
        "system_one" | "systemone" => Ok(ApiMode::SystemOne),
        other => Err(AgentError::InvalidInput(format!(
            "unknown inference.api_mode '{other}'"
        ))),
    }
}

#[async_trait]
impl HttpBackend for AppHttpBackend {
    fn current_config(&self) -> Option<AppConfig> {
        Some(self.runtime.config())
    }

    async fn metrics(&self) -> MetricsSnapshot {
        let inf_metrics = self.inference.metrics();
        let (reachable, loaded_model) = self
            .ps5lm_health
            .read()
            .map(|g| g.clone())
            .unwrap_or((false, None));
        let summary = self.store.task_counts_summary().unwrap_or_default();

        let active_tasks = (summary.running + summary.waiting) as u64;
        let queued_tasks = summary.pending as u64;
        let completed_tasks = summary.completed as u64;
        let failed_tasks = (summary.failed + summary.cancelled) as u64;

        let (ps5_fw_version, ps5_model, ps5_cpu_temp_c, ps5_soc_temp_c) = read_ps5_telemetry();

        MetricsSnapshot {
            uptime_secs: self.started_at.elapsed().as_secs(),
            storage_driver: self.store.driver_name().to_string(),
            inference_requests_total: inf_metrics.requests_total,
            inference_errors_total: inf_metrics.errors_total,
            inference_p50_ms: inf_metrics.p50_ms,
            inference_p95_ms: inf_metrics.p95_ms,
            last_inference_ms: inf_metrics.last_ms,
            tokens_prompt_total: inf_metrics.prompt_tokens_total,
            tokens_completion_total: inf_metrics.completion_tokens_total,
            http_requests_total: 0,
            http_avg_ms: 0,
            db_ops_total: self.db_metrics.ops_total(),
            db_avg_us: self.db_metrics.avg_us(),
            active_tasks,
            queued_tasks,
            completed_tasks,
            failed_tasks,
            memory_rss_bytes: None,
            ps5_fw_version,
            ps5_model,
            ps5_cpu_temp_c,
            ps5_soc_temp_c,
            ps5lm_reachable: reachable,
            ps5lm_loaded_model: loaded_model,
            network_degraded: false,
        }
    }

    async fn send_conversation_message(
        &self,
        conversation_id: &str,
        mut req: SendMessageRequest,
    ) -> Result<SendMessageResponse> {
        let cfg = self.runtime.config();
        let mut upload_cfg = self.tools.config();
        if cfg.server.upload_max_body_bytes == 0 {
            upload_cfg.max_file_bytes = 0;
        } else {
            upload_cfg.max_file_bytes = upload_cfg
                .max_file_bytes
                .max(cfg.server.upload_max_body_bytes);
        }

        for att in &mut req.attachments {
            if is_image_attachment(att) || att.workspace_path.is_some() {
                continue;
            }
            let Some(ref raw_data_url) = att.data_url else {
                continue;
            };
            let bytes = decode_data_url_bytes(raw_data_url)?;
            let safe_name = sanitize_upload_filename(&att.name);
            let short_id = &ps5_agent_core::new_id()[..8];
            let rel_path = format!("uploads/{short_id}_{safe_name}");
            let write_path = rel_path.clone();
            let write_cfg = upload_cfg.clone();

            let (_, written) = tokio::task::spawn_blocking(move || {
                tool_files::write_workspace_bytes(&write_path, &bytes, &write_cfg)
            })
            .await
            .map_err(|e| AgentError::Internal(format!("attachment write task failed: {e}")))??;

            att.workspace_path = Some(rel_path.clone());
            att.size_bytes = written as u64;
            att.data_url = None;
            let _ = self.events_tx.send(StreamEvent::FilesChanged {
                conversation_id: Some(conversation_id.to_string()),
                path: rel_path,
                action: "upload".to_string(),
            });
        }

        let cancel = CancellationToken::new();
        if req.async_dispatch || req.immediate {
            let (user_message, task) = self
                .runtime
                .dispatch_chat_background_with_options(
                    conversation_id.to_string(),
                    req.content,
                    req.agent_id,
                    req.thinking_level,
                    req.attachments,
                    req.immediate,
                    cancel,
                )
                .await?;
            return Ok(SendMessageResponse {
                user_message,
                task: Some(task),
            });
        }

        let turn = self
            .runtime
            .handle_chat_message_with_options(
                conversation_id,
                &req.content,
                req.agent_id.as_deref(),
                req.thinking_level,
                req.attachments,
                cancel,
            )
            .await?;

        let messages = self.store.list_chat_messages(conversation_id, false, 100)?;
        let user_message = messages
            .into_iter()
            .rev()
            .find(|m| m.role == ChatRole::User && m.content == req.content)
            .unwrap_or_else(|| ChatMessage {
                id: ps5_agent_core::new_id(),
                conversation_id: conversation_id.to_string(),
                role: ChatRole::User,
                agent_id: None,
                task_id: Some(turn.task_id.clone()),
                content: req.content.clone(),
                reasoning: None,
                tool_calls_json: None,
                token_count: 0,
                latency_ms: None,
                created_at: now_iso(),
                archived: false,
                attachments: Vec::new(),
            });
        let task = self.store.get_task(&turn.task_id)?;

        Ok(SendMessageResponse { user_message, task })
    }

    async fn list_agent_cards(&self) -> Result<Vec<AgentCardView>> {
        self.agent_manager.list_cards()
    }

    async fn get_agent_card(&self, agent_id: &str) -> Result<AgentCardView> {
        self.agent_manager
            .list_cards()?
            .into_iter()
            .find(|c| c.definition.id == agent_id)
            .ok_or_else(|| AgentError::NotFound(format!("agent '{agent_id}' not found")))
    }

    async fn create_agent(&self, req: CreateAgentRequest) -> Result<AgentDefinition> {
        self.agent_manager.create_agent(
            CreateAgentInput {
                id: None,
                name: req.name,
                description: req.description,
                role: req.role,
                role_label: req.role_label,
                system_prompt: req.system_prompt,
                model_config: Some(req.model_config),
                enabled_tools: req.enabled_tools,
                denied_tools: Some(req.denied_tools),
                sandbox_mode: req.sandbox_mode,
                tool_profile: req.tool_profile,
                memory_permissions: Some(req.memory_permissions),
                max_task_duration_secs: Some(req.max_task_duration_secs),
                max_reasoning_iterations: Some(req.max_reasoning_iterations),
                priority: Some(req.priority),
                can_communicate: Some(req.can_communicate),
                enabled: Some(req.enabled),
                is_temporary: req.is_temporary,
                spawned_by: Some("admin".to_string()),
            },
            "admin",
        )
    }

    async fn update_agent(&self, agent_id: &str, req: UpdateAgentRequest) -> Result<AgentDefinition> {
        let desired_enabled = req.enabled;
        let updated = self.agent_manager.update_agent(
            agent_id,
            UpdateAgentInput {
                name: req.name,
                description: req.description,
                role_label: req.role_label,
                system_prompt: req.system_prompt,
                model_config: req.model_config,
                enabled_tools: req.enabled_tools,
                denied_tools: req.denied_tools,
                sandbox_mode: req.sandbox_mode,
                tool_profile: req.tool_profile,
                memory_permissions: req.memory_permissions,
                max_task_duration_secs: req.max_task_duration_secs,
                max_reasoning_iterations: req.max_reasoning_iterations,
                priority: req.priority,
                can_communicate: req.can_communicate,
            },
            "admin",
        )?;
        if let Some(enabled) = desired_enabled {
            return self.agent_manager.set_enabled(agent_id, enabled, "admin");
        }
        Ok(updated)
    }

    async fn delete_agent(&self, agent_id: &str) -> Result<bool> {
        self.runtime.evict_pending_for_agent(agent_id);
        self.agent_manager
            .delete_agent(
                agent_id,
                DeleteAgentOptions {
                    active_task_policy: ActiveTaskPolicy::CancelActive,
                    delete_memory: false,
                    actor: "admin".to_string(),
                },
            )
            .await
    }

    async fn set_agent_enabled(&self, agent_id: &str, enabled: bool) -> Result<AgentDefinition> {
        if !enabled {
            self.runtime.evict_pending_for_agent(agent_id);
        }
        self.agent_manager.set_enabled(agent_id, enabled, "admin")
    }

    async fn send_agent_message(&self, req: SendAgentMessageRequest) -> Result<AgentEnvelope> {
        self.message_bus.send(BusSendRequest {
            sender_agent_id: req.sender_agent_id,
            recipient_agent_id: req.recipient_agent_id,
            task_id: req.task_id,
            message_type: req.message_type,
            content: req.content,
            hop_count: 1,
        })
    }

    async fn create_task(&self, req: CreateTaskRequest) -> Result<ps5_agent_core::TaskRecord> {
        let assigned = req
            .assigned_agent_id
            .unwrap_or_else(|| "commander".to_string());
        self.runtime
            .submit_task(req.user_request, assigned, req.conversation_id, req.priority)
            .await
    }

    async fn cancel_task(&self, task_id: &str) -> Result<ps5_agent_core::TaskRecord> {
        self.runtime.evict_pending_for_task(task_id);
        self.runtime
            .task_manager()
            .cancel_task(task_id, "Cancelled by operator")
    }

    async fn resolve_approval(
        &self,
        approval_id: &str,
        approved: bool,
        resolved_by: &str,
    ) -> Result<ApprovalRecord> {
        let _outcome = self
            .runtime
            .resolve_approval(approval_id, approved, resolved_by)
            .await?;
        self.store
            .get_approval(approval_id)?
            .ok_or_else(|| AgentError::NotFound(format!("approval '{approval_id}' not found")))
    }

    async fn list_tools(&self) -> Result<Vec<ToolSpec>> {
        self.tools.list_specs()
    }

    async fn toggle_tool(&self, tool_id: &str, enabled: Option<bool>) -> Result<ToolSpec> {
        self.tools.toggle_tool(tool_id, enabled)
    }

    async fn execute_tool(
        &self,
        tool_id: &str,
        req: ExecuteToolRequest,
    ) -> Result<ps5_agent_core::ToolExecutionRecord> {
        let cfg = self.runtime.config();
        let sandbox_mode =
            SandboxMode::parse(&cfg.sandbox.mode).unwrap_or(SandboxMode::FullSandbox);
        let approval_policy = SandboxApprovalPolicy::parse(&cfg.sandbox.approval_policy)
            .unwrap_or(SandboxApprovalPolicy::FullAuto);
        let agent_id = req.agent_id.unwrap_or_else(|| "__operator__".to_string());
        let before_ids: std::collections::HashSet<String> = self
            .store
            .list_tool_executions(Some(&agent_id), 25)?
            .into_iter()
            .map(|r| r.id)
            .collect();
        let input_json = truncate_utf8_bytes(
            &serde_json::to_string(&req.arguments).unwrap_or_else(|_| "{}".to_string()),
            16 * 1024,
        );
        let ctx = ToolExecContext {
            agent_id: agent_id.clone(),
            task_id: req.task_id.clone(),
            conversation_id: None,
            // Manual execution in the authenticated dashboard Tool Tester explicitly authorizes the run.
            approval_granted: true,
            is_operator: true,
            sandbox_mode,
            approval_policy,
            a2a_bridge: Some(self.tool_bridge.clone()),
        };

        let started = Instant::now();
        let invoke_res = self.tools.invoke(tool_id, req.arguments, &ctx).await;
        let duration_ms = started.elapsed().as_millis() as u64;

        let new_record = self
            .store
            .list_tool_executions(Some(&agent_id), 25)?
            .into_iter()
            .find(|r| r.tool_id == tool_id && !before_ids.contains(&r.id));

        match invoke_res {
            Ok(val) => {
                if let Some(latest) = new_record {
                    return Ok(latest);
                }
                let rec = ps5_agent_core::ToolExecutionRecord {
                    id: ps5_agent_core::new_id(),
                    tool_id: tool_id.to_string(),
                    agent_id,
                    task_id: req.task_id,
                    input_json,
                    output_json: Some(truncate_utf8_bytes(&val.to_string(), 64 * 1024)),
                    success: true,
                    error: None,
                    duration_ms,
                    created_at: now_iso(),
                };
                let _ = self.store.record_tool_execution(&rec);
                Ok(rec)
            }
            Err(err) => {
                if new_record.is_none() {
                    let rec = ps5_agent_core::ToolExecutionRecord {
                        id: ps5_agent_core::new_id(),
                        tool_id: tool_id.to_string(),
                        agent_id,
                        task_id: req.task_id,
                        input_json,
                        output_json: None,
                        success: false,
                        error: Some(err.to_string()),
                        duration_ms,
                        created_at: now_iso(),
                    };
                    let _ = self.store.record_tool_execution(&rec);
                }
                Err(err)
            }
        }
    }

    async fn apply_settings_update(
        &self,
        updates: &serde_json::Map<String, Value>,
    ) -> Result<()> {
        let mut cfg = self.runtime.config();
        let mut inference_changed = false;
        let mut max_conc_changed = false;
        let mut tools_changed = false;
        let mut memory_changed = false;
        let mut logging_changed = false;
        let mut sandbox_changed = false;

        if let Some(Value::Object(inf)) = updates.get("inference") {
            if let Some(v) = inf.get("base_url").and_then(Value::as_str) {
                cfg.inference.base_url = v.trim().to_string();
                inference_changed = true;
            }
            if let Some(v) = inf.get("model").and_then(Value::as_str) {
                cfg.inference.model = v.trim().to_string();
                inference_changed = true;
            }
            if let Some(v) = inf.get("api_mode").and_then(Value::as_str) {
                cfg.inference.api_mode = parse_api_mode_str(v)?;
                inference_changed = true;
            }
            if let Some(v) = inf.get("timeout_secs").and_then(Value::as_u64) {
                cfg.inference.timeout_secs = v;
                inference_changed = true;
            }
            if let Some(v) = inf.get("connect_timeout_secs").and_then(Value::as_u64) {
                cfg.inference.connect_timeout_secs = v;
                inference_changed = true;
            }
            if let Some(v) = inf.get("context_limit").and_then(Value::as_u64) {
                cfg.inference.context_limit = v as u32;
                inference_changed = true;
            }
            if let Some(v) = inf.get("max_output_tokens").and_then(Value::as_u64) {
                cfg.inference.max_output_tokens = v as u32;
                inference_changed = true;
            }
            if let Some(v) = inf.get("stream_enabled").and_then(Value::as_bool) {
                cfg.inference.stream_enabled = v;
                inference_changed = true;
            }
            if let Some(v) = inf.get("max_retries").and_then(Value::as_u64) {
                cfg.inference.max_retries = v as u32;
                inference_changed = true;
            }
            if let Some(v) = inf.get("thinking_level").and_then(Value::as_str) {
                cfg.inference.thinking_level = parse_thinking_level_str(v)?.to_string();
                inference_changed = true;
            }
            if let Some(v) = inf.get("auto_sync_from_ps5lm").and_then(Value::as_bool) {
                cfg.inference.auto_sync_from_ps5lm = v;
                inference_changed = true;
            }
            if let Some(v) = inf.get("auto_sync_interval_secs").and_then(Value::as_u64) {
                cfg.inference.auto_sync_interval_secs = v;
                inference_changed = true;
            }
            if let Some(v) = inf.get("prefill_kv_cache").and_then(Value::as_bool) {
                cfg.inference.prefill_kv_cache = v;
                inference_changed = true;
            }
            if let Some(v) = inf.get("cache_prompt").and_then(Value::as_bool) {
                cfg.inference.cache_prompt = v;
                inference_changed = true;
            }
            if let Some(v) = inf.get("temperature").and_then(Value::as_f64) {
                cfg.inference.temperature = v as f32;
                inference_changed = true;
            }
            if let Some(v) = inf.get("dynatemp_range").and_then(Value::as_f64) {
                cfg.inference.dynatemp_range = v as f32;
                inference_changed = true;
            }
            if let Some(v) = inf.get("dynatemp_exponent").and_then(Value::as_f64) {
                cfg.inference.dynatemp_exponent = v as f32;
                inference_changed = true;
            }
            if let Some(v) = inf.get("top_k").and_then(Value::as_i64) {
                cfg.inference.top_k = v as i32;
                inference_changed = true;
            }
            if let Some(v) = inf.get("top_p").and_then(Value::as_f64) {
                cfg.inference.top_p = v as f32;
                inference_changed = true;
            }
            if let Some(v) = inf.get("min_p").and_then(Value::as_f64) {
                cfg.inference.min_p = v as f32;
                inference_changed = true;
            }
            if let Some(v) = inf.get("typical_p").and_then(Value::as_f64) {
                cfg.inference.typical_p = v as f32;
                inference_changed = true;
            }
            if let Some(v) = inf.get("xtc_probability").and_then(Value::as_f64) {
                cfg.inference.xtc_probability = v as f32;
                inference_changed = true;
            }
            if let Some(v) = inf.get("xtc_threshold").and_then(Value::as_f64) {
                cfg.inference.xtc_threshold = v as f32;
                inference_changed = true;
            }
            if let Some(v) = inf.get("top_n_sigma").and_then(Value::as_f64) {
                cfg.inference.top_n_sigma = v as f32;
                inference_changed = true;
            }
            if let Some(v) = inf.get("repeat_penalty").and_then(Value::as_f64) {
                cfg.inference.repeat_penalty = v as f32;
                inference_changed = true;
            }
            if let Some(v) = inf.get("repeat_last_n").and_then(Value::as_i64) {
                cfg.inference.repeat_last_n = v as i32;
                inference_changed = true;
            }
            if let Some(v) = inf.get("presence_penalty").and_then(Value::as_f64) {
                cfg.inference.presence_penalty = v as f32;
                inference_changed = true;
            }
            if let Some(v) = inf.get("frequency_penalty").and_then(Value::as_f64) {
                cfg.inference.frequency_penalty = v as f32;
                inference_changed = true;
            }
            if let Some(v) = inf.get("dry_multiplier").and_then(Value::as_f64) {
                cfg.inference.dry_multiplier = v as f32;
                inference_changed = true;
            }
            if let Some(v) = inf.get("dry_base").and_then(Value::as_f64) {
                cfg.inference.dry_base = v as f32;
                inference_changed = true;
            }
            if let Some(v) = inf.get("dry_allowed_length").and_then(Value::as_i64) {
                cfg.inference.dry_allowed_length = v as i32;
                inference_changed = true;
            }
            if let Some(v) = inf.get("dry_penalty_last_n").and_then(Value::as_i64) {
                cfg.inference.dry_penalty_last_n = v as i32;
                inference_changed = true;
            }
            if let Some(v) = inf.get("mirostat").and_then(Value::as_u64) {
                cfg.inference.mirostat = v.min(u8::MAX as u64) as u8;
                inference_changed = true;
            }
            if let Some(v) = inf.get("mirostat_tau").and_then(Value::as_f64) {
                cfg.inference.mirostat_tau = v as f32;
                inference_changed = true;
            }
            if let Some(v) = inf.get("mirostat_eta").and_then(Value::as_f64) {
                cfg.inference.mirostat_eta = v as f32;
                inference_changed = true;
            }
            if let Some(v) = inf.get("seed").and_then(Value::as_i64) {
                cfg.inference.seed = v;
                inference_changed = true;
            }
            if let Some(v) = inf.get("ignore_eos").and_then(Value::as_bool) {
                cfg.inference.ignore_eos = v;
                inference_changed = true;
            }
            if let Some(raw_stops) = inf.get("stop_sequences") {
                match raw_stops {
                    Value::Array(arr) => {
                        cfg.inference.stop_sequences = arr
                            .iter()
                            .filter_map(Value::as_str)
                            .map(|s| s.to_string())
                            .filter(|s| !s.is_empty())
                            .collect();
                        inference_changed = true;
                    }
                    Value::String(s) => {
                        cfg.inference.stop_sequences = s
                            .lines()
                            .map(str::trim)
                            .filter(|line| !line.is_empty())
                            .map(str::to_string)
                            .collect();
                        inference_changed = true;
                    }
                    _ => {}
                }
            }
            if let Some(v) = inf.get("n_keep").and_then(Value::as_i64) {
                cfg.inference.n_keep = v as i32;
                inference_changed = true;
            }
            if let Some(v) = inf.get("id_slot").and_then(Value::as_i64) {
                cfg.inference.id_slot = v as i32;
                inference_changed = true;
            }
            if let Some(raw_samplers) = inf.get("samplers") {
                match raw_samplers {
                    Value::String(s) => {
                        cfg.inference.samplers = s.trim().to_string();
                        inference_changed = true;
                    }
                    Value::Array(arr) => {
                        let list: Vec<&str> = arr
                            .iter()
                            .filter_map(Value::as_str)
                            .map(str::trim)
                            .filter(|s| !s.is_empty())
                            .collect();
                        cfg.inference.samplers = list.join(",");
                        inference_changed = true;
                    }
                    _ => {}
                }
            }
            if let Some(v) = inf.get("grammar").and_then(Value::as_str) {
                cfg.inference.grammar = v.trim().to_string();
                inference_changed = true;
            }
            if let Some(v) = inf.get("reasoning_format").and_then(Value::as_str) {
                cfg.inference.reasoning_format = v.trim().to_string();
                inference_changed = true;
            }
            if let Some(v) = inf.get("timings_per_token").and_then(Value::as_bool) {
                cfg.inference.timings_per_token = v;
                inference_changed = true;
            }
            if let Some(raw_extra) = inf.get("custom_extra_body_json") {
                match raw_extra {
                    Value::String(s) => {
                        cfg.inference.custom_extra_body_json = s.trim().to_string();
                        inference_changed = true;
                    }
                    Value::Object(_) => {
                        if let Ok(s) = serde_json::to_string(raw_extra) {
                            cfg.inference.custom_extra_body_json = s;
                            inference_changed = true;
                        }
                    }
                    Value::Null => {
                        cfg.inference.custom_extra_body_json.clear();
                        inference_changed = true;
                    }
                    _ => {}
                }
            }
        }

        if let Some(Value::Object(srv)) = updates.get("server") {
            if let Some(v) = srv.get("bind_host").and_then(Value::as_str) {
                cfg.server.bind_host = v.trim().to_string();
            }
            if let Some(v) = srv.get("port").and_then(Value::as_u64) {
                if v == 0 || v > u16::MAX as u64 {
                    return Err(AgentError::InvalidInput(
                        "server.port must be between 1 and 65535".into(),
                    ));
                }
                cfg.server.port = v as u16;
            }
            if let Some(v) = srv.get("max_body_bytes").and_then(Value::as_u64) {
                cfg.server.max_body_bytes = v as usize;
            }
            if let Some(v) = srv.get("upload_max_body_bytes").and_then(Value::as_u64) {
                cfg.server.upload_max_body_bytes = v as usize;
            }
            if let Some(v) = srv.get("allow_lan_only").and_then(Value::as_bool) {
                cfg.server.allow_lan_only = v;
            }
        }

        if let Some(Value::Object(db)) = updates.get("database") {
            if let Some(v) = db.get("driver").and_then(Value::as_str) {
                cfg.database.driver = v.trim().to_ascii_lowercase();
            }
            if let Some(v) = db.get("busy_timeout_ms").and_then(Value::as_u64) {
                cfg.database.busy_timeout_ms = v as u32;
            }
            if let Some(v) = db.get("cache_size_kb").and_then(Value::as_i64) {
                cfg.database.cache_size_kb = v as i32;
            }
            if let Some(v) = db.get("wal_mode").and_then(Value::as_bool) {
                cfg.database.wal_mode = v;
            }
            if let Some(v) = db.get("retention_days").and_then(Value::as_u64) {
                cfg.database.retention_days = v as u32;
            }
            if let Some(v) = db.get("max_rows_per_table").and_then(Value::as_u64) {
                cfg.database.max_rows_per_table = v as usize;
            }
        }

        if let Some(Value::Object(ag)) = updates.get("agents") {
            if let Some(v) = ag.get("max_agents").and_then(Value::as_u64) {
                cfg.agents.max_agents = v as usize;
            }
            if let Some(v) = ag.get("max_active_tasks").and_then(Value::as_u64) {
                cfg.agents.max_active_tasks = v as usize;
            }
            if let Some(v) = ag.get("max_queued_tasks").and_then(Value::as_u64) {
                cfg.agents.max_queued_tasks = v as usize;
            }
            if let Some(v) = ag.get("max_concurrent_inference").and_then(Value::as_u64) {
                cfg.agents.max_concurrent_inference = v as usize;
                max_conc_changed = true;
            }
            if let Some(v) = ag.get("max_messages_per_task").and_then(Value::as_u64) {
                cfg.agents.max_messages_per_task = v as u32;
            }
            if let Some(v) = ag.get("max_reasoning_iterations").and_then(Value::as_u64) {
                cfg.agents.max_reasoning_iterations = v as u32;
            }
            if let Some(v) = ag.get("default_task_timeout_secs").and_then(Value::as_u64) {
                cfg.agents.default_task_timeout_secs = v;
            }
            if let Some(v) = ag
                .get("allow_temporary_specialists")
                .and_then(Value::as_bool)
            {
                cfg.agents.allow_temporary_specialists = v;
            }
            if let Some(v) = ag.get("on_disable_policy").and_then(Value::as_str) {
                let norm = v.trim().to_ascii_lowercase();
                if !matches!(norm.as_str(), "cancel" | "finish_step") {
                    return Err(AgentError::InvalidInput(format!(
                        "unknown agents.on_disable_policy '{norm}'"
                    )));
                }
                cfg.agents.on_disable_policy = norm;
            }
        }

        if let Some(Value::Object(mem)) = updates.get("memory") {
            if let Some(v) = mem.get("max_memories_per_agent").and_then(Value::as_u64) {
                cfg.memory.max_memories_per_agent = v as usize;
                memory_changed = true;
            }
            if let Some(v) = mem.get("archive_after_messages").and_then(Value::as_u64) {
                cfg.memory.archive_after_messages = v as usize;
                memory_changed = true;
            }
            if let Some(v) = mem.get("keep_recent_messages").and_then(Value::as_u64) {
                cfg.memory.keep_recent_messages = v as usize;
                memory_changed = true;
            }
            if let Some(v) = mem.get("max_context_memories").and_then(Value::as_u64) {
                cfg.memory.max_context_memories = v as usize;
                memory_changed = true;
            }
            if let Some(v) = mem.get("max_memory_context_chars").and_then(Value::as_u64) {
                cfg.memory.max_memory_context_chars = v as usize;
                memory_changed = true;
            }
        }

        if let Some(Value::Object(tl)) = updates.get("tools") {
            if let Some(arr) = tl.get("allowed_paths").and_then(Value::as_array) {
                cfg.tools.allowed_paths = arr
                    .iter()
                    .filter_map(Value::as_str)
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                tools_changed = true;
            }
            if let Some(arr) = tl.get("allowed_domains").and_then(Value::as_array) {
                cfg.tools.allowed_domains = arr
                    .iter()
                    .filter_map(Value::as_str)
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                tools_changed = true;
            }
            if let Some(arr) = tl.get("blocked_domains").and_then(Value::as_array) {
                cfg.tools.blocked_domains = arr
                    .iter()
                    .filter_map(Value::as_str)
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                tools_changed = true;
            }
            if let Some(v) = tl
                .get("block_private_lan_web_requests")
                .and_then(Value::as_bool)
            {
                cfg.tools.block_private_lan_web_requests = v;
                cfg.tools.allow_local_network = !v;
                cfg.sandbox.allow_local_network = !v;
                tools_changed = true;
                sandbox_changed = true;
            }
            if let Some(v) = tl.get("allow_local_network").and_then(Value::as_bool) {
                cfg.tools.allow_local_network = v;
                cfg.tools.block_private_lan_web_requests = !v;
                cfg.sandbox.allow_local_network = v;
                tools_changed = true;
                sandbox_changed = true;
            }
            if let Some(v) = tl.get("network_access").and_then(Value::as_bool) {
                cfg.tools.network_access = v;
                cfg.sandbox.network_access = v;
                tools_changed = true;
                sandbox_changed = true;
            }
            if let Some(v) = tl.get("web_search_provider").and_then(Value::as_str) {
                let norm = v.trim().to_ascii_lowercase();
                if !matches!(norm.as_str(), "duckduckgo" | "searxng" | "brave" | "tavily") {
                    return Err(AgentError::InvalidInput(format!(
                        "unsupported tools.web_search_provider '{norm}'"
                    )));
                }
                cfg.tools.web_search_provider = norm.clone();
                cfg.sandbox.web_search_provider = norm;
                tools_changed = true;
                sandbox_changed = true;
            }
            if let Some(v) = tl.get("searxng_url").and_then(Value::as_str) {
                let s = v.trim().to_string();
                cfg.tools.searxng_url = s.clone();
                cfg.sandbox.searxng_url = s;
                tools_changed = true;
                sandbox_changed = true;
            }
            if let Some(v) = tl.get("brave_api_key").and_then(Value::as_str) {
                let s = v.trim().to_string();
                cfg.tools.brave_api_key = s.clone();
                cfg.sandbox.brave_api_key = s;
                tools_changed = true;
                sandbox_changed = true;
            }
            if let Some(v) = tl.get("tavily_api_key").and_then(Value::as_str) {
                let s = v.trim().to_string();
                cfg.tools.tavily_api_key = s.clone();
                cfg.sandbox.tavily_api_key = s;
                tools_changed = true;
                sandbox_changed = true;
            }
            if let Some(v) = tl.get("remote_browser_url").and_then(Value::as_str) {
                let s = v.trim().to_string();
                cfg.tools.remote_browser_url = s.clone();
                cfg.sandbox.remote_browser_url = s;
                tools_changed = true;
                sandbox_changed = true;
            }
            if let Some(v) = tl.get("allow_host_shell").and_then(Value::as_bool) {
                cfg.tools.allow_host_shell = v;
                cfg.sandbox.allow_host_shell = v;
                tools_changed = true;
                sandbox_changed = true;
            }
            if let Some(v) = tl.get("max_exec_output_bytes").and_then(Value::as_u64) {
                cfg.tools.max_exec_output_bytes = v as usize;
                cfg.sandbox.max_exec_output_bytes = v as usize;
                tools_changed = true;
                sandbox_changed = true;
            }
            if let Some(v) = tl.get("exec_timeout_secs").and_then(Value::as_u64) {
                cfg.tools.exec_timeout_secs = v;
                cfg.sandbox.exec_timeout_secs = v;
                tools_changed = true;
                sandbox_changed = true;
            }
            if let Some(v) = tl.get("default_timeout_secs").and_then(Value::as_u64) {
                cfg.tools.default_timeout_secs = v;
                tools_changed = true;
            }
            if let Some(v) = tl.get("max_file_bytes").and_then(Value::as_u64) {
                cfg.tools.max_file_bytes = v as usize;
                tools_changed = true;
            }
            if let Some(v) = tl.get("max_web_bytes").and_then(Value::as_u64) {
                cfg.tools.max_web_bytes = v as usize;
                tools_changed = true;
            }
        }

        if let Some(Value::Object(sb)) = updates.get("sandbox") {
            if let Some(v) = sb.get("mode").and_then(Value::as_str) {
                let mode = SandboxMode::parse(v).ok_or_else(|| {
                    AgentError::InvalidInput(format!("unknown sandbox.mode '{v}'"))
                })?;
                cfg.sandbox.mode = mode.as_str().to_string();
                sandbox_changed = true;
            }
            if let Some(v) = sb.get("approval_policy").and_then(Value::as_str) {
                let policy = SandboxApprovalPolicy::parse(v).ok_or_else(|| {
                    AgentError::InvalidInput(format!("unknown sandbox.approval_policy '{v}'"))
                })?;
                cfg.sandbox.approval_policy = policy.as_str().to_string();
                sandbox_changed = true;
            }
            if let Some(v) = sb.get("network_access").and_then(Value::as_bool) {
                cfg.sandbox.network_access = v;
                cfg.tools.network_access = v;
                tools_changed = true;
                sandbox_changed = true;
            }
            if let Some(v) = sb.get("allow_local_network").and_then(Value::as_bool) {
                cfg.sandbox.allow_local_network = v;
                cfg.tools.allow_local_network = v;
                cfg.tools.block_private_lan_web_requests = !v;
                tools_changed = true;
                sandbox_changed = true;
            }
            if let Some(v) = sb.get("web_search_provider").and_then(Value::as_str) {
                let norm = v.trim().to_ascii_lowercase();
                if !matches!(norm.as_str(), "duckduckgo" | "searxng" | "brave" | "tavily") {
                    return Err(AgentError::InvalidInput(format!(
                        "unsupported sandbox.web_search_provider '{norm}'"
                    )));
                }
                cfg.sandbox.web_search_provider = norm.clone();
                cfg.tools.web_search_provider = norm;
                tools_changed = true;
                sandbox_changed = true;
            }
            if let Some(v) = sb.get("searxng_url").and_then(Value::as_str) {
                let s = v.trim().to_string();
                cfg.sandbox.searxng_url = s.clone();
                cfg.tools.searxng_url = s;
                tools_changed = true;
                sandbox_changed = true;
            }
            if let Some(v) = sb.get("brave_api_key").and_then(Value::as_str) {
                let s = v.trim().to_string();
                cfg.sandbox.brave_api_key = s.clone();
                cfg.tools.brave_api_key = s;
                tools_changed = true;
                sandbox_changed = true;
            }
            if let Some(v) = sb.get("tavily_api_key").and_then(Value::as_str) {
                let s = v.trim().to_string();
                cfg.sandbox.tavily_api_key = s.clone();
                cfg.tools.tavily_api_key = s;
                tools_changed = true;
                sandbox_changed = true;
            }
            if let Some(v) = sb.get("remote_browser_url").and_then(Value::as_str) {
                let s = v.trim().to_string();
                cfg.sandbox.remote_browser_url = s.clone();
                cfg.tools.remote_browser_url = s;
                tools_changed = true;
                sandbox_changed = true;
            }
            if let Some(v) = sb.get("allow_host_shell").and_then(Value::as_bool) {
                cfg.sandbox.allow_host_shell = v;
                cfg.tools.allow_host_shell = v;
                tools_changed = true;
                sandbox_changed = true;
            }
            if let Some(v) = sb.get("max_exec_output_bytes").and_then(Value::as_u64) {
                cfg.sandbox.max_exec_output_bytes = v as usize;
                cfg.tools.max_exec_output_bytes = v as usize;
                tools_changed = true;
                sandbox_changed = true;
            }
            if let Some(v) = sb.get("exec_timeout_secs").and_then(Value::as_u64) {
                cfg.sandbox.exec_timeout_secs = v;
                cfg.tools.exec_timeout_secs = v;
                tools_changed = true;
                sandbox_changed = true;
            }
            if let Some(v) = sb.get("inject_brain_files").and_then(Value::as_bool) {
                cfg.sandbox.inject_brain_files = v;
                sandbox_changed = true;
            }
            if let Some(v) = sb.get("max_brain_context_chars").and_then(Value::as_u64) {
                cfg.sandbox.max_brain_context_chars = v as usize;
                sandbox_changed = true;
            }
            if let Some(v) = sb.get("heartbeat_interval_secs").and_then(Value::as_u64) {
                cfg.sandbox.heartbeat_interval_secs = v;
                sandbox_changed = true;
            }
        }

        if let Some(Value::Object(log)) = updates.get("logging") {
            if let Some(v) = log.get("level").and_then(Value::as_str) {
                let norm = v.trim().to_ascii_lowercase();
                if ps5_agent_logging::LogLevel::parse_strict(&norm).is_none() {
                    return Err(AgentError::InvalidInput(format!(
                        "unknown logging.level '{norm}'"
                    )));
                }
                cfg.logging.level = norm;
                logging_changed = true;
            }
            if let Some(v) = log.get("max_file_bytes").and_then(Value::as_u64) {
                cfg.logging.max_file_bytes = v as usize;
                logging_changed = true;
            }
            if let Some(v) = log.get("ring_buffer_capacity").and_then(Value::as_u64) {
                cfg.logging.ring_buffer_capacity = v as usize;
                logging_changed = true;
            }
            if let Some(v) = log.get("enable_klog").and_then(Value::as_bool) {
                cfg.logging.enable_klog = v;
                logging_changed = true;
            }
            if let Some(v) = log.get("enable_toast").and_then(Value::as_bool) {
                cfg.logging.enable_toast = v;
                logging_changed = true;
            }
        }

        if let Some(Value::Object(sec)) = updates.get("security") {
            if let Some(v) = sec.get("auth_enabled").and_then(Value::as_bool) {
                cfg.security.auth_enabled = v;
            }
            if let Some(v) = sec.get("admin_username").and_then(Value::as_str) {
                let trimmed = v.trim();
                if trimmed.is_empty() {
                    return Err(AgentError::InvalidInput(
                        "security.admin_username cannot be empty".into(),
                    ));
                }
                cfg.security.admin_username = trimmed.to_string();
            }
            if let Some(v) = sec
                .get("require_approval_for_mutating")
                .and_then(Value::as_bool)
            {
                cfg.security.require_approval_for_mutating = v;
            }
            if let Some(v) = sec.get("session_ttl_hours").and_then(Value::as_u64) {
                if v > 87_600 {
                    return Err(AgentError::InvalidInput(
                        "security.session_ttl_hours must be 0 (unlimited) or <= 87600".into(),
                    ));
                }
                cfg.security.session_ttl_hours = v;
            }
            if let Some(v) = sec.get("rate_limit_per_minute").and_then(Value::as_u64) {
                cfg.security.rate_limit_per_minute = v as u32;
            }
            if let Some(v) = sec
                .get("login_rate_limit_per_minute")
                .and_then(Value::as_u64)
            {
                cfg.security.login_rate_limit_per_minute = v as u32;
            }
        }

        if let Some(Value::Object(rt)) = updates.get("runtime") {
            if let Some(v) = rt.get("health_poll_interval_secs").and_then(Value::as_u64) {
                cfg.runtime.health_poll_interval_secs = v;
            }
            if let Some(v) = rt.get("rest_mode_recovery").and_then(Value::as_bool) {
                cfg.runtime.rest_mode_recovery = v;
            }
            if let Some(v) = rt.get("tokio_worker_threads").and_then(Value::as_u64) {
                cfg.runtime.tokio_worker_threads = v as usize;
            }
        }

        cfg.sync_sandbox_and_tools();
        for w in cfg.normalize_and_clamp() {
            self.logger.warn("settings", w);
        }
        cfg.validate()?;

        if logging_changed {
            self.logger.apply_config(
                ps5_agent_logging::LogLevel::parse(&cfg.logging.level),
                cfg.logging.max_file_bytes as u64,
                cfg.logging.ring_buffer_capacity,
                cfg.logging.enable_klog,
                cfg.logging.enable_toast,
            );
        }
        if inference_changed {
            self.inference.update_config(cfg.inference.clone())?;
        }
        if max_conc_changed {
            self.inference
                .set_max_concurrent(cfg.agents.max_concurrent_inference);
        }
        if tools_changed {
            self.tools.update_config(cfg.tools.clone())?;
        }
        if memory_changed {
            self.memory_provider.update_config(cfg.memory.clone());
        }
        self.runtime.update_config(cfg.clone());
        let config_path = cfg.data_dir.join("config.toml");
        cfg.save(&config_path)?;
        if sandbox_changed {
            let _ = self.events_tx.send(StreamEvent::SandboxUpdated {
                mode: cfg.sandbox.mode.clone(),
                approval_policy: cfg.sandbox.approval_policy.clone(),
                network_access: cfg.sandbox.network_access,
            });
        }
        self.logger.info(
            "settings",
            format!("updated runtime settings and saved {}", config_path.display()),
        );

        Ok(())
    }

    async fn list_inference_models(&self) -> Result<Vec<String>> {
        self.inference.list_models().await
    }

    async fn evaluate_systemone(&self, req: SystemOneRequest) -> Result<SystemOneResponse> {
        self.inference
            .system_one(req, CancellationToken::new())
            .await
    }

    async fn list_workspace_files(&self, path: Option<&str>) -> Result<WorkspaceFilesResponse> {
        let tools_cfg = self.tools.config();
        let root_label = tools_cfg
            .allowed_paths
            .first()
            .cloned()
            .unwrap_or_else(|| "workspace".to_string());
        let rel = path.map(str::to_string);

        let (current_path, raw_entries) = tokio::task::spawn_blocking(move || {
            tool_files::list_workspace_entries(rel.as_deref(), &tools_cfg)
        })
        .await
        .map_err(|e| AgentError::Internal(format!("workspace list task failed: {e}")))??;

        let entries = raw_entries
            .into_iter()
            .map(|e| {
                let (kind, previewable) = classify_workspace_entry(&e.name, e.is_dir);
                WorkspaceFileEntry {
                    name: e.name,
                    path: e.path,
                    is_dir: e.is_dir,
                    size_bytes: e.size_bytes,
                    modified_at: e.modified_at,
                    kind,
                    previewable,
                }
            })
            .collect();

        Ok(WorkspaceFilesResponse {
            current_path,
            root_label,
            entries,
        })
    }

    async fn read_workspace_file(&self, path: &str) -> Result<(String, Vec<u8>)> {
        let app_cfg = self.runtime.config();
        let mut read_cfg = self.tools.config();
        if app_cfg.server.upload_max_body_bytes == 0 {
            read_cfg.max_file_bytes = 0;
        } else {
            read_cfg.max_file_bytes = read_cfg
                .max_file_bytes
                .max(app_cfg.server.upload_max_body_bytes);
        }
        let target = path.to_string();

        let (file_path, bytes) = tokio::task::spawn_blocking(move || {
            tool_files::read_workspace_bytes(&target, &read_cfg)
        })
        .await
        .map_err(|e| AgentError::Internal(format!("workspace read task failed: {e}")))??;

        let name = file_path
            .file_name()
            .and_then(|n| n.to_str())
            .filter(|n| !n.is_empty())
            .unwrap_or("download.bin")
            .to_string();
        Ok((name, bytes))
    }

    async fn write_workspace_file(&self, path: &str, bytes: &[u8]) -> Result<WorkspaceFileEntry> {
        let app_cfg = self.runtime.config();
        let mut upload_cfg = self.tools.config();
        if app_cfg.server.upload_max_body_bytes == 0 {
            upload_cfg.max_file_bytes = 0;
        } else {
            upload_cfg.max_file_bytes = upload_cfg
                .max_file_bytes
                .max(app_cfg.server.upload_max_body_bytes);
        }
        let write_target = path.to_string();
        let data = bytes.to_vec();

        let (abs_path, written) = tokio::task::spawn_blocking(move || {
            tool_files::write_workspace_bytes(&write_target, &data, &upload_cfg)
        })
        .await
        .map_err(|e| AgentError::Internal(format!("workspace write task failed: {e}")))??;

        let rel_path = clean_rel_path(
            &abs_path.to_string_lossy(),
            &app_cfg.tools.allowed_paths,
        );
        let rel_path = if rel_path.is_empty() {
            clean_rel_path(path, &app_cfg.tools.allowed_paths)
        } else {
            rel_path
        };

        let name = abs_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("file")
            .to_string();
        let (kind, previewable) = classify_workspace_entry(&name, false);
        let _ = self.events_tx.send(StreamEvent::FilesChanged {
            conversation_id: None,
            path: rel_path.clone(),
            action: "write".to_string(),
        });

        Ok(WorkspaceFileEntry {
            name,
            path: rel_path,
            is_dir: false,
            size_bytes: written as u64,
            modified_at: Some(now_iso()),
            kind,
            previewable,
        })
    }

    async fn create_workspace_folder(&self, path: &str) -> Result<()> {
        let tools_cfg = self.tools.config();
        let rel_path = clean_rel_path(path, &tools_cfg.allowed_paths);
        let target = path.to_string();
        tokio::task::spawn_blocking(move || tool_files::create_workspace_dir(&target, &tools_cfg))
            .await
            .map_err(|e| AgentError::Internal(format!("workspace mkdir task failed: {e}")))??;

        let _ = self.events_tx.send(StreamEvent::FilesChanged {
            conversation_id: None,
            path: rel_path,
            action: "mkdir".to_string(),
        });
        Ok(())
    }

    async fn delete_workspace_file(&self, path: &str) -> Result<()> {
        let tools_cfg = self.tools.config();
        let rel_path = clean_rel_path(path, &tools_cfg.allowed_paths);
        let target = path.to_string();
        tokio::task::spawn_blocking(move || tool_files::delete_workspace_entry(&target, &tools_cfg))
            .await
            .map_err(|e| AgentError::Internal(format!("workspace delete task failed: {e}")))??;

        let _ = self.events_tx.send(StreamEvent::FilesChanged {
            conversation_id: None,
            path: rel_path,
            action: "delete".to_string(),
        });
        Ok(())
    }

    async fn list_extensions(&self) -> Result<ExtensionsOverviewResponse> {
        let _ = self.tools.mcp().discover_all();
        let _ = self.tools.sync_specs();
        let skills = self
            .tools
            .skills()
            .list_skills()
            .into_iter()
            .map(skill_to_dto)
            .collect();
        let plugins = self
            .tools
            .plugins()
            .list_plugins()
            .into_iter()
            .map(plugin_to_dto)
            .collect();
        let mcp_servers = self
            .tools
            .mcp()
            .list_views()
            .into_iter()
            .map(mcp_to_dto)
            .collect();
        Ok(ExtensionsOverviewResponse {
            skills,
            plugins,
            mcp_servers,
        })
    }

    async fn get_skill_detail(&self, id: &str) -> Result<SkillDetailResponse> {
        let doc = self.tools.skills().load_skill(id)?;
        Ok(SkillDetailResponse {
            id: doc.summary.id,
            name: doc.summary.name,
            description: doc.summary.description,
            instructions: doc.instructions,
            raw_markdown: doc.raw_markdown,
            resources: doc.summary.resources,
        })
    }

    async fn import_skill(&self, req: ImportSkillRequest) -> Result<SkillInfoDto> {
        let summary = self
            .tools
            .skills()
            .import_skill(req.slug.as_deref(), &req.markdown)?;
        let _ = self.tools.sync_specs();
        Ok(skill_to_dto(summary))
    }

    async fn import_plugin(&self, req: ImportPluginRequest) -> Result<PluginInfoDto> {
        let view = self
            .tools
            .plugins()
            .import_plugin_json(&req.manifest_json)?;
        sync_plugin_mcp_configs(&self.tools, &self.logger);
        self.tools.sync_specs()?;
        Ok(plugin_to_dto(view))
    }

    async fn import_mcp_servers(&self, req: ImportMcpRequest) -> Result<Vec<McpServerDto>> {
        let views = self.tools.mcp().add_or_import(&req.raw_json)?;
        self.tools.sync_specs()?;
        Ok(views.into_iter().map(mcp_to_dto).collect())
    }

    async fn sync_mcp_server(&self, id: &str) -> Result<McpServerDto> {
        let client = self.tools.http_client()?;
        let cfg = self.tools.config();
        let view = self.tools.mcp().sync_server(&client, id, &cfg).await?;
        self.tools.sync_specs()?;
        Ok(mcp_to_dto(view))
    }

    async fn set_extension_enabled(&self, kind: &str, id: &str, enabled: bool) -> Result<()> {
        match kind.trim().to_ascii_lowercase().as_str() {
            "skill" => {
                self.tools.skills().set_enabled(id, enabled)?;
            }
            "plugin" => {
                self.tools.plugins().set_enabled(id, enabled)?;
                if enabled {
                    sync_plugin_mcp_configs(&self.tools, &self.logger);
                }
            }
            "mcp" => {
                self.tools.mcp().set_enabled(id, enabled)?;
            }
            other => {
                return Err(AgentError::InvalidInput(format!(
                    "unknown extension kind '{other}'"
                )));
            }
        }
        self.tools.sync_specs()?;
        Ok(())
    }

    async fn delete_extension(&self, kind: &str, id: &str) -> Result<()> {
        match kind.trim().to_ascii_lowercase().as_str() {
            "skill" => {
                self.tools.skills().delete_skill(id)?;
            }
            "plugin" => {
                self.tools.plugins().delete_plugin(id)?;
            }
            "mcp" => {
                self.tools.mcp().remove_server(id)?;
            }
            other => {
                return Err(AgentError::InvalidInput(format!(
                    "unknown extension kind '{other}'"
                )));
            }
        }
        self.tools.sync_specs()?;
        Ok(())
    }

    async fn get_inference_capabilities(&self) -> Result<Ps5LmCapabilities> {
        if let Ok(guard) = self.ps5lm_capabilities.read() {
            if let Some(ref caps) = *guard {
                return Ok(caps.clone());
            }
        }
        self.sync_ps5lm_capabilities().await
    }

    async fn sync_inference_capabilities(&self) -> Result<Ps5LmCapabilities> {
        self.sync_ps5lm_capabilities_with_notify(true).await
    }

    async fn reorder_conversations(
        &self,
        ordered_ids: Vec<String>,
        conversation_id: Option<String>,
        direction: Option<String>,
    ) -> Result<Vec<Conversation>> {
        let move_item = match (conversation_id, direction) {
            (Some(id), Some(dir)) => Some((id, dir)),
            _ => None,
        };
        self.runtime.reorder_conversations(ordered_ids, move_item)
    }

    async fn fork_conversation(
        &self,
        conv_id: &str,
        up_to_message_id: Option<String>,
        title: Option<String>,
    ) -> Result<Conversation> {
        self.runtime
            .fork_conversation(conv_id, up_to_message_id.as_deref(), title)
    }

    async fn delete_chat_message(&self, conv_id: &str, msg_id: &str) -> Result<bool> {
        self.runtime.delete_conversation_message(conv_id, msg_id)
    }

    async fn retry_chat_message(
        &self,
        conv_id: &str,
        msg_id: &str,
        thinking_level: Option<ThinkingLevel>,
        agent_id: Option<String>,
        async_dispatch: bool,
    ) -> Result<SendMessageResponse> {
        let (user_message, task) = self
            .runtime
            .retry_user_message(conv_id, msg_id, thinking_level, agent_id, async_dispatch)
            .await?;
        Ok(SendMessageResponse { user_message, task })
    }

    async fn regenerate_chat_message(
        &self,
        conv_id: &str,
        msg_id: &str,
        thinking_level: Option<ThinkingLevel>,
        async_dispatch: bool,
    ) -> Result<SendMessageResponse> {
        let (user_message, task) = self
            .runtime
            .regenerate_assistant_message(conv_id, msg_id, thinking_level, async_dispatch)
            .await?;
        Ok(SendMessageResponse { user_message, task })
    }

    async fn continue_chat_message(
        &self,
        conv_id: &str,
        msg_id: &str,
        thinking_level: Option<ThinkingLevel>,
    ) -> Result<SendMessageResponse> {
        let (user_message, task) = self
            .runtime
            .continue_assistant_message(conv_id, msg_id, thinking_level)
            .await?;
        Ok(SendMessageResponse {
            user_message,
            task: Some(task),
        })
    }

    async fn enable_all_agents(&self) -> Result<usize> {
        self.agent_manager.enable_all_agents()
    }

    async fn create_or_import_tools(&self, payload: Value) -> Result<Vec<ToolSpec>> {
        let specs = self.tools.register_or_import_custom_tools(&payload)?;
        let grant_to_agents = payload
            .get("grant_to_agents")
            .and_then(Value::as_bool)
            .unwrap_or(true);
        if grant_to_agents {
            for agent_id in ["commander", "programmer"] {
                if let Ok(Some(agent)) = self.agent_manager.get_agent(agent_id) {
                    if !agent.enabled_tools.iter().any(|t| t == "custom:*" || t == "*") {
                        let mut next_tools = agent.enabled_tools;
                        next_tools.push("custom:*".to_string());
                        if let Err(e) = self.agent_manager.update_agent(
                            agent_id,
                            UpdateAgentInput {
                                name: None,
                                description: None,
                                role_label: None,
                                system_prompt: None,
                                model_config: None,
                                enabled_tools: Some(next_tools),
                                denied_tools: None,
                                sandbox_mode: None,
                                tool_profile: None,
                                memory_permissions: None,
                                max_task_duration_secs: None,
                                max_reasoning_iterations: None,
                                priority: None,
                                can_communicate: None,
                            },
                            "admin",
                        ) {
                            self.logger.warn(
                                "tools",
                                format!("failed to grant custom:* to agent '{agent_id}': {e}"),
                            );
                        }
                    }
                }
            }
        }
        Ok(specs)
    }

    async fn update_tool_policy(
        &self,
        tool_id: &str,
        enabled: Option<bool>,
        requires_approval: Option<bool>,
        timeout_secs: Option<u64>,
    ) -> Result<ToolSpec> {
        self.tools
            .update_tool_policy(tool_id, enabled, requires_approval, timeout_secs)
    }

    async fn delete_custom_tool(&self, tool_id: &str) -> Result<bool> {
        self.tools.delete_custom_tool(tool_id)
    }

    async fn import_memories(&self, payload: Value) -> Result<usize> {
        let items = parse_memories_import_value(&payload, MemoryScope::Project, "imported", None)?;
        if items.is_empty() {
            return Err(AgentError::InvalidInput(
                "no memory items found in import payload".into(),
            ));
        }
        let mut imported = 0usize;
        for item in &items {
            self.store.upsert_memory(item)?;
            imported += 1;
        }
        Ok(imported)
    }

    async fn export_memories(&self) -> Result<Vec<MemoryItem>> {
        self.store.query_memories(None, None, None, false, 10_000)
    }

    async fn cancel_conversation_tasks(
        &self,
        conv_id: &str,
        agent_id: Option<String>,
    ) -> Result<usize> {
        self.runtime.cancel_conversation_tasks(
            conv_id,
            agent_id.as_deref(),
            "Cancelled by operator",
        )
    }

    async fn expedite_task(&self, task_id: &str) -> Result<ps5_agent_core::TaskRecord> {
        self.runtime.expedite_task(task_id)
    }

    async fn list_schedules(&self) -> Result<Vec<ScheduleRecord>> {
        self.runtime.list_schedules(500)
    }

    async fn get_schedule(&self, id: &str) -> Result<Option<ScheduleRecord>> {
        self.runtime.get_schedule(id)
    }

    async fn create_schedule(&self, req: HttpCreateSchedule) -> Result<ScheduleRecord> {
        self.runtime.create_schedule(CreateScheduleInput {
            title: req.title,
            prompt: req.prompt,
            target_agent_id: req.target_agent_id,
            conversation_id: req.conversation_id,
            kind: req.kind,
            run_at: req.run_at,
            interval_secs: req.interval_secs,
            daily_time: req.daily_time,
            cron_expr: req.cron_expr,
            timezone_offset_mins: req.timezone_offset_mins,
            enabled: req.enabled,
        })
    }

    async fn update_schedule(&self, id: &str, req: HttpUpdateSchedule) -> Result<ScheduleRecord> {
        self.runtime.update_schedule(
            id,
            UpdateScheduleInput {
                title: req.title,
                prompt: req.prompt,
                target_agent_id: req.target_agent_id,
                kind: req.kind,
                run_at: req.run_at,
                interval_secs: req.interval_secs,
                daily_time: req.daily_time,
                cron_expr: req.cron_expr,
                timezone_offset_mins: req.timezone_offset_mins,
                enabled: req.enabled,
            },
        )
    }

    async fn delete_schedule(&self, id: &str) -> Result<()> {
        self.runtime.delete_schedule(id)
    }

    async fn toggle_schedule(&self, id: &str, enabled: Option<bool>) -> Result<ScheduleRecord> {
        self.runtime.toggle_schedule(id, enabled)
    }

    async fn run_schedule_now(&self, id: &str) -> Result<ScheduleRecord> {
        self.runtime
            .trigger_schedule_now(id, CancellationToken::new())
            .await
    }

    async fn parse_schedule_ai(&self, req: HttpScheduleAiRequest) -> Result<HttpScheduleAiResponse> {
        let draft = self
            .runtime
            .parse_schedule_ai(&req.prompt, req.timezone_offset_mins, req.target_agent_id)
            .await;
        let http_draft = HttpScheduleAiDraft {
            title: draft.title,
            prompt: draft.prompt,
            kind: draft.kind,
            run_at: draft.run_at,
            interval_secs: draft.interval_secs,
            daily_time: draft.daily_time,
            cron_expr: draft.cron_expr,
            target_agent_id: draft.target_agent_id,
            timezone_offset_mins: draft.timezone_offset_mins,
            explanation: draft.explanation,
            source: draft.source,
        };
        let created = if req.auto_create {
            Some(self.runtime.create_schedule(CreateScheduleInput {
                title: http_draft.title.clone(),
                prompt: http_draft.prompt.clone(),
                target_agent_id: http_draft.target_agent_id.clone(),
                conversation_id: None,
                kind: http_draft.kind,
                run_at: http_draft.run_at,
                interval_secs: http_draft.interval_secs,
                daily_time: http_draft.daily_time.clone(),
                cron_expr: http_draft.cron_expr.clone(),
                timezone_offset_mins: http_draft.timezone_offset_mins,
                enabled: Some(true),
            })?)
        } else {
            None
        };
        Ok(HttpScheduleAiResponse {
            draft: http_draft,
            created,
        })
    }

    async fn run_doctor(&self) -> Result<DoctorReport> {
        let cfg = self.runtime.config();
        let builtin_ids: Vec<String> = builtin_tool_specs(cfg.tools.exec_timeout_secs)
            .into_iter()
            .map(|s| s.id)
            .collect();
        self.runtime.run_doctor_checks(&builtin_ids).await
    }

    async fn fix_doctor(&self) -> Result<DoctorFixReport> {
        let cfg = self.runtime.config();
        let builtin_ids: Vec<String> = builtin_tool_specs(cfg.tools.exec_timeout_secs)
            .into_iter()
            .map(|s| s.id)
            .collect();
        self.runtime.run_doctor_fixes(&builtin_ids).await
    }

    async fn get_sandbox_state(&self) -> Result<SandboxStateDto> {
        let cfg = self.runtime.config();
        Ok(sandbox_state_dto(&cfg))
    }

    async fn update_sandbox_policy(&self, req: UpdateSandboxRequest) -> Result<SandboxStateDto> {
        let mut cfg = self.runtime.config();
        if let Some(ref mode_raw) = req.mode {
            let mode = SandboxMode::parse(mode_raw).ok_or_else(|| {
                AgentError::InvalidInput(format!("unknown sandbox.mode '{mode_raw}'"))
            })?;
            cfg.sandbox.mode = mode.as_str().to_string();
        }
        if let Some(ref policy_raw) = req.approval_policy {
            let policy = SandboxApprovalPolicy::parse(policy_raw).ok_or_else(|| {
                AgentError::InvalidInput(format!(
                    "unknown sandbox.approval_policy '{policy_raw}'"
                ))
            })?;
            cfg.sandbox.approval_policy = policy.as_str().to_string();
        }
        if let Some(v) = req.network_access {
            cfg.sandbox.network_access = v;
            cfg.tools.network_access = v;
        }
        if let Some(v) = req.allow_local_network {
            cfg.sandbox.allow_local_network = v;
            cfg.tools.allow_local_network = v;
            cfg.tools.block_private_lan_web_requests = !v;
        }
        if let Some(ref v) = req.web_search_provider {
            let norm = v.trim().to_ascii_lowercase();
            if !matches!(norm.as_str(), "duckduckgo" | "searxng" | "brave" | "tavily") {
                return Err(AgentError::InvalidInput(format!(
                    "unsupported sandbox.web_search_provider '{norm}'"
                )));
            }
            cfg.sandbox.web_search_provider = norm.clone();
            cfg.tools.web_search_provider = norm;
        }
        if let Some(ref v) = req.searxng_url {
            let s = v.trim().to_string();
            cfg.sandbox.searxng_url = s.clone();
            cfg.tools.searxng_url = s;
        }
        if let Some(ref v) = req.brave_api_key {
            let s = v.trim().to_string();
            cfg.sandbox.brave_api_key = s.clone();
            cfg.tools.brave_api_key = s;
        }
        if let Some(ref v) = req.tavily_api_key {
            let s = v.trim().to_string();
            cfg.sandbox.tavily_api_key = s.clone();
            cfg.tools.tavily_api_key = s;
        }
        if let Some(ref v) = req.remote_browser_url {
            let s = v.trim().to_string();
            cfg.sandbox.remote_browser_url = s.clone();
            cfg.tools.remote_browser_url = s;
        }
        if let Some(v) = req.allow_host_shell {
            cfg.sandbox.allow_host_shell = v;
            cfg.tools.allow_host_shell = v;
        }
        if let Some(v) = req.max_exec_output_bytes {
            cfg.sandbox.max_exec_output_bytes = v;
            cfg.tools.max_exec_output_bytes = v;
        }
        if let Some(v) = req.exec_timeout_secs {
            cfg.sandbox.exec_timeout_secs = v;
            cfg.tools.exec_timeout_secs = v;
        }
        if let Some(v) = req.inject_brain_files {
            cfg.sandbox.inject_brain_files = v;
        }
        if let Some(v) = req.max_brain_context_chars {
            cfg.sandbox.max_brain_context_chars = v;
        }
        if let Some(v) = req.heartbeat_interval_secs {
            cfg.sandbox.heartbeat_interval_secs = v;
        }
        if let Some(v) = req.allowed_domains {
            cfg.tools.allowed_domains = v;
        }
        if let Some(v) = req.blocked_domains {
            cfg.tools.blocked_domains = v;
        }

        cfg.sync_sandbox_and_tools();
        cfg.normalize_and_clamp();
        cfg.validate()?;

        self.tools.update_config(cfg.tools.clone())?;
        self.runtime.update_config(cfg.clone());
        let config_path = cfg.data_dir.join("config.toml");
        cfg.save(&config_path)?;

        let _ = self.events_tx.send(StreamEvent::SandboxUpdated {
            mode: cfg.sandbox.mode.clone(),
            approval_policy: cfg.sandbox.approval_policy.clone(),
            network_access: cfg.sandbox.network_access,
        });
        Ok(sandbox_state_dto(&cfg))
    }

    async fn list_browser_sessions(&self) -> Result<Value> {
        Ok(self.tools.browser_manager().list_sessions())
    }

    async fn execute_browser_action(&self, payload: Value) -> Result<Value> {
        let cfg = self.tools.config();
        let client = self.tools.http_client()?;
        self.tools
            .browser_manager()
            .execute(&payload, &cfg, &client)
            .await
    }

    async fn close_browser_session(&self, session_id: &str) -> Result<bool> {
        Ok(self.tools.browser_manager().close_session(session_id))
    }

    async fn list_brain_files(&self) -> Result<Vec<BrainFileSummary>> {
        let cfg = self.runtime.config();
        let ws = primary_workspace_root(&cfg);
        tokio::task::spawn_blocking(move || ps5_agent_runtime::brain::list_brain_files(&ws))
            .await
            .map_err(|e| AgentError::Internal(format!("list_brain_files task failed: {e}")))?
    }

    async fn read_brain_file(&self, name: &str) -> Result<BrainFileDetail> {
        let cfg = self.runtime.config();
        let ws = primary_workspace_root(&cfg);
        let target = name.to_string();
        tokio::task::spawn_blocking(move || ps5_agent_runtime::brain::read_brain_file(&ws, &target))
            .await
            .map_err(|e| AgentError::Internal(format!("read_brain_file task failed: {e}")))?
    }

    async fn write_brain_file(&self, name: &str, content: &str) -> Result<BrainFileDetail> {
        let cfg = self.runtime.config();
        let ws = primary_workspace_root(&cfg);
        let target = name.to_string();
        let body = content.to_string();
        let detail = tokio::task::spawn_blocking(move || {
            ps5_agent_runtime::brain::write_brain_file(&ws, &target, &body)
        })
        .await
        .map_err(|e| AgentError::Internal(format!("write_brain_file task failed: {e}")))??;

        let _ = self.events_tx.send(StreamEvent::FilesChanged {
            conversation_id: None,
            path: detail.path.clone(),
            action: "write".to_string(),
        });
        Ok(detail)
    }

    async fn execute_conversation_command(
        &self,
        conversation_id: &str,
        req: SlashCommandRequest,
    ) -> Result<SlashCommandResponse> {
        if self.store.get_conversation(conversation_id)?.is_none() {
            return Err(AgentError::NotFound(format!(
                "conversation '{conversation_id}' not found"
            )));
        }
        self.runtime
            .execute_slash_command(
                Some(conversation_id),
                req.agent_id.as_deref(),
                &req.command,
                CancellationToken::new(),
            )
            .await
    }

    async fn list_webhooks(&self) -> Result<Vec<WebhookView>> {
        Ok(self
            .store
            .list_webhooks()?
            .into_iter()
            .map(WebhookView::from)
            .collect())
    }

    async fn create_webhook(&self, req: CreateWebhookRequest) -> Result<CreateWebhookResponse> {
        let name = req.name.trim().to_string();
        if name.is_empty() || name.len() > 120 {
            return Err(AgentError::InvalidInput(
                "webhook name must be 1..=120 characters".into(),
            ));
        }
        let target_agent_id = req.target_agent_id.trim().to_string();
        if target_agent_id.is_empty() {
            return Err(AgentError::InvalidInput(
                "webhook target_agent_id must not be empty".into(),
            ));
        }
        if self.agent_manager.get_agent(&target_agent_id)?.is_none() {
            return Err(AgentError::NotFound(format!(
                "agent '{target_agent_id}' not found"
            )));
        }
        let conversation_id = req
            .conversation_id
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        if let Some(ref cid) = conversation_id {
            if self.store.get_conversation(cid)?.is_none() {
                return Err(AgentError::NotFound(format!(
                    "conversation '{cid}' not found"
                )));
            }
        }

        let secret = ps5_agent_http::auth::random_hex_256();
        let secret_hash = ps5_agent_http::auth::sha256_hex(secret.as_bytes());
        let secret_prefix: String = secret.chars().take(8).collect();
        let now = now_iso();
        let record = WebhookRecord {
            id: ps5_agent_core::new_id(),
            name,
            enabled: req.enabled.unwrap_or(true),
            secret_hash,
            secret_prefix,
            target_agent_id,
            conversation_id,
            prompt_template: req.prompt_template,
            created_at: now.clone(),
            updated_at: now,
            last_triggered_at: None,
            trigger_count: 0,
        };
        self.store.upsert_webhook(&record)?;
        Ok(CreateWebhookResponse {
            webhook: WebhookView::from(record),
            secret,
        })
    }

    async fn delete_webhook(&self, id: &str) -> Result<bool> {
        self.store.delete_webhook(id)
    }

    async fn trigger_inbound_webhook(
        &self,
        id: &str,
        token: &str,
        payload: Value,
    ) -> Result<WebhookTriggerResponse> {
        let mut hook = self
            .store
            .get_webhook(id)?
            .ok_or_else(|| AgentError::NotFound(format!("webhook '{id}' not found")))?;
        let presented_hash = ps5_agent_http::auth::sha256_hex(token.as_bytes());
        if !ps5_agent_http::auth::constant_time_eq(&hook.secret_hash, &presented_hash) {
            return Err(AgentError::PermissionDenied(
                "Invalid webhook token".into(),
            ));
        }
        if !hook.enabled {
            return Err(AgentError::PermissionDenied(format!(
                "webhook '{id}' is disabled"
            )));
        }
        let agent = self
            .agent_manager
            .get_agent(&hook.target_agent_id)?
            .ok_or_else(|| {
                AgentError::NotFound(format!("target agent '{}' not found", hook.target_agent_id))
            })?;
        if !agent.enabled {
            return Err(AgentError::PermissionDenied(format!(
                "target agent '{}' is disabled",
                hook.target_agent_id
            )));
        }

        let payload_str =
            serde_json::to_string_pretty(&payload).unwrap_or_else(|_| payload.to_string());
        let tpl = hook.prompt_template.trim();
        let prompt = if tpl.is_empty() {
            format!(
                "Inbound webhook '{}' ({}) triggered:\n```json\n{}\n```",
                hook.name, hook.id, payload_str
            )
        } else if tpl.contains("{{payload}}") {
            tpl.replace("{{payload}}", &payload_str)
        } else {
            format!("{tpl}\n\nWebhook payload:\n```json\n{payload_str}\n```")
        };

        let conv_exists = match &hook.conversation_id {
            Some(cid) => self.store.get_conversation(cid)?.is_some(),
            None => false,
        };
        let task = if conv_exists {
            let cid = hook.conversation_id.clone().unwrap_or_default();
            let (_, t) = self
                .runtime
                .dispatch_chat_background(
                    cid,
                    prompt,
                    Some(hook.target_agent_id.clone()),
                    None,
                    Vec::new(),
                    CancellationToken::new(),
                )
                .await?;
            t
        } else {
            self.runtime
                .submit_task(prompt, hook.target_agent_id.clone(), None, None)
                .await?
        };

        let now = now_iso();
        hook.trigger_count = hook.trigger_count.saturating_add(1);
        hook.last_triggered_at = Some(now.clone());
        hook.updated_at = now;
        self.store.upsert_webhook(&hook)?;

        let _ = self.events_tx.send(StreamEvent::WebhookTriggered {
            webhook_id: hook.id.clone(),
            name: hook.name.clone(),
            task_id: task.id.clone(),
            conversation_id: hook.conversation_id.clone().unwrap_or_default(),
        });

        Ok(WebhookTriggerResponse {
            ok: true,
            webhook_id: hook.id,
            task_id: Some(task.id),
            message: format!("Webhook '{}' triggered", hook.name),
        })
    }
}

fn parse_memory_scope_str(raw: &str) -> Option<MemoryScope> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "conversation" => Some(MemoryScope::Conversation),
        "agent" => Some(MemoryScope::Agent),
        "project" => Some(MemoryScope::Project),
        "task" => Some(MemoryScope::Task),
        "user_preference" | "userpreference" | "preference" => Some(MemoryScope::UserPreference),
        _ => None,
    }
}

fn derive_memory_key(hint: Option<&str>, content: &str, idx: usize) -> String {
    if let Some(raw) = hint.map(str::trim).filter(|s| !s.is_empty()) {
        return raw.chars().take(64).collect();
    }
    let slug: String = content
        .lines()
        .next()
        .unwrap_or("")
        .trim()
        .trim_start_matches(['#', '-', '*', ' '])
        .chars()
        .take(40)
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '-') {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();
    let compact = slug.trim_matches('-');
    if compact.is_empty() {
        format!("imported-{}", idx + 1)
    } else {
        compact.to_string()
    }
}

fn parse_imported_memory_item(
    val: &Value,
    default_scope: MemoryScope,
    default_ns: &str,
    default_owner: Option<&str>,
    idx: usize,
) -> Result<MemoryItem> {
    if let Ok(mut full) = serde_json::from_value::<MemoryItem>(val.clone()) {
        if !full.content.trim().is_empty() {
            let now = now_iso();
            if full.id.trim().is_empty() {
                full.id = ps5_agent_core::new_id();
            }
            if full.namespace.trim().is_empty() {
                full.namespace = default_ns.to_string();
            }
            if full.key.trim().is_empty() {
                full.key = derive_memory_key(None, &full.content, idx);
            }
            if full.created_at.trim().is_empty() {
                full.created_at = now.clone();
            }
            if full.updated_at.trim().is_empty() {
                full.updated_at = now;
            }
            return Ok(full);
        }
    }

    let obj = val
        .as_object()
        .ok_or_else(|| AgentError::InvalidInput("memory entry must be a JSON object".into()))?;
    let content = obj
        .get("content")
        .or_else(|| obj.get("text"))
        .or_else(|| obj.get("value"))
        .or_else(|| obj.get("body"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| AgentError::InvalidInput("memory entry is missing non-empty 'content'".into()))?
        .to_string();

    let scope = obj
        .get("scope")
        .and_then(Value::as_str)
        .and_then(parse_memory_scope_str)
        .unwrap_or(default_scope);
    let namespace = obj
        .get("namespace")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(default_ns)
        .to_string();
    let owner_agent_id = obj
        .get("owner_agent_id")
        .or_else(|| obj.get("agent_id"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| default_owner.map(str::to_string));
    let key_hint = obj
        .get("key")
        .or_else(|| obj.get("title"))
        .or_else(|| obj.get("name"))
        .and_then(Value::as_str);
    let key = derive_memory_key(key_hint, &content, idx);
    let summary = obj
        .get("summary")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let importance = obj
        .get("importance")
        .and_then(Value::as_u64)
        .map(|v| v.clamp(1, 10) as u8)
        .unwrap_or(6);
    let tags = obj
        .get("tags")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    let now = now_iso();

    Ok(MemoryItem {
        id: obj
            .get("id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .unwrap_or_else(ps5_agent_core::new_id),
        scope,
        namespace,
        owner_agent_id,
        conversation_id: obj
            .get("conversation_id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string),
        task_id: obj
            .get("task_id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string),
        key,
        content,
        summary,
        importance,
        tags,
        archived: obj
            .get("archived")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        created_at: obj
            .get("created_at")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| now.clone()),
        updated_at: obj
            .get("updated_at")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .unwrap_or(now),
    })
}

fn parse_memories_import_text(
    raw: &str,
    default_scope: MemoryScope,
    default_ns: &str,
    default_owner: Option<&str>,
) -> Result<Vec<MemoryItem>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(AgentError::InvalidInput(
            "memory import text cannot be empty".into(),
        ));
    }

    if (trimmed.starts_with('[') || trimmed.starts_with('{'))
        && serde_json::from_str::<Value>(trimmed).is_ok()
    {
        if let Ok(parsed) = serde_json::from_str::<Value>(trimmed) {
            return parse_memories_import_value(&parsed, default_scope, default_ns, default_owner);
        }
    }

    let non_empty_lines: Vec<&str> = trimmed
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .collect();
    if !non_empty_lines.is_empty()
        && non_empty_lines
            .iter()
            .all(|l| l.starts_with('{') && l.ends_with('}'))
    {
        let mut out = Vec::with_capacity(non_empty_lines.len());
        for (idx, line) in non_empty_lines.iter().enumerate() {
            let val: Value = serde_json::from_str(line).map_err(|e| {
                AgentError::InvalidInput(format!("invalid JSONL memory on line {}: {e}", idx + 1))
            })?;
            out.push(parse_imported_memory_item(
                &val,
                default_scope,
                default_ns,
                default_owner,
                idx,
            )?);
        }
        return Ok(out);
    }

    let mut sections: Vec<(Option<String>, String)> = Vec::new();
    let has_headings = trimmed
        .lines()
        .any(|l| l.trim_start().starts_with("# ") || l.trim_start().starts_with("## "));
    if has_headings {
        let mut cur_heading: Option<String> = None;
        let mut cur_body: Vec<&str> = Vec::new();
        for line in trimmed.lines() {
            let t = line.trim();
            if t.starts_with("# ") || t.starts_with("## ") {
                let body_str = cur_body.join("\n").trim().to_string();
                if !body_str.is_empty() {
                    sections.push((cur_heading.take(), body_str));
                }
                cur_body.clear();
                cur_heading = Some(t.trim_start_matches('#').trim().to_string());
            } else {
                cur_body.push(line);
            }
        }
        let tail = cur_body.join("\n").trim().to_string();
        if !tail.is_empty() {
            sections.push((cur_heading, tail));
        } else if let Some(h) = cur_heading {
            sections.push((Some(h.clone()), h));
        }
    } else {
        for block in trimmed.split("\n\n") {
            let clean = block.trim();
            if !clean.is_empty() {
                sections.push((None, clean.to_string()));
            }
        }
        if sections.len() == 1 {
            let only = &sections[0].1;
            let bullets: Vec<String> = only
                .lines()
                .map(str::trim)
                .filter(|l| !l.is_empty())
                .map(|l| l.trim_start_matches(['-', '*', '•']).trim().to_string())
                .filter(|l| !l.is_empty())
                .collect();
            if bullets.len() > 1
                && only
                    .lines()
                    .map(str::trim)
                    .filter(|l| !l.is_empty())
                    .all(|l| l.starts_with("- ") || l.starts_with("* ") || l.starts_with("• "))
            {
                sections = bullets.into_iter().map(|b| (None, b)).collect();
            }
        }
    }

    let now = now_iso();
    Ok(sections
        .into_iter()
        .enumerate()
        .map(|(idx, (heading, body))| MemoryItem {
            id: ps5_agent_core::new_id(),
            scope: default_scope,
            namespace: default_ns.to_string(),
            owner_agent_id: default_owner.map(str::to_string),
            conversation_id: None,
            task_id: None,
            key: derive_memory_key(heading.as_deref(), &body, idx),
            content: body,
            summary: None,
            importance: 6,
            tags: vec!["imported".to_string()],
            archived: false,
            created_at: now.clone(),
            updated_at: now.clone(),
        })
        .collect())
}

fn parse_memories_import_value(
    payload: &Value,
    default_scope: MemoryScope,
    default_ns: &str,
    default_owner: Option<&str>,
) -> Result<Vec<MemoryItem>> {
    match payload {
        Value::Array(arr) => arr
            .iter()
            .enumerate()
            .map(|(idx, v)| {
                parse_imported_memory_item(v, default_scope, default_ns, default_owner, idx)
            })
            .collect(),
        Value::String(s) => {
            parse_memories_import_text(s, default_scope, default_ns, default_owner)
        }
        Value::Object(obj) => {
            let scope = obj
                .get("scope")
                .and_then(Value::as_str)
                .and_then(parse_memory_scope_str)
                .unwrap_or(default_scope);
            let ns = obj
                .get("namespace")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or(default_ns);
            let owner = obj
                .get("owner_agent_id")
                .or_else(|| obj.get("agent_id"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .or(default_owner);

            if let Some(arr) = obj
                .get("memories")
                .or_else(|| obj.get("items"))
                .and_then(Value::as_array)
            {
                return arr
                    .iter()
                    .enumerate()
                    .map(|(idx, v)| parse_imported_memory_item(v, scope, ns, owner, idx))
                    .collect();
            }

            if let Some(raw_text) = obj
                .get("raw")
                .or_else(|| obj.get("markdown"))
                .or_else(|| obj.get("notes"))
                .or_else(|| obj.get("text"))
                .and_then(Value::as_str)
            {
                return parse_memories_import_text(raw_text, scope, ns, owner);
            }

            if obj.get("content").and_then(Value::as_str).is_some() {
                if obj.len() <= 4 && !obj.contains_key("key") {
                    if let Some(s) = obj.get("content").and_then(Value::as_str) {
                        return parse_memories_import_text(s, scope, ns, owner);
                    }
                }
                return Ok(vec![parse_imported_memory_item(
                    payload, scope, ns, owner, 0,
                )?]);
            }

            Err(AgentError::InvalidInput(
                "unsupported memory import JSON object; expected 'memories', 'raw', 'text', or 'content'"
                    .into(),
            ))
        }
        _ => Err(AgentError::InvalidInput(
            "memory import payload must be a JSON array, object, or text string".into(),
        )),
    }
}

/// Ensures unused core imports are avoided while keeping helper signatures clean.
#[allow(dead_code)]
fn _unused_placeholder(_: InferenceMessage) {}
