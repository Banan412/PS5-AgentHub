use async_trait::async_trait;
use ps5_agent_config::MemoryConfig;
use ps5_agent_core::{
    new_id, now_iso, AgentDefinition, AgentError, MemoryItem, MemoryScope, Result,
    StorageBackend, ToolSpec,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolExecutionOutput {
    pub output: serde_json::Value,
    pub is_mutating: bool,
}

/// Clean runtime abstraction over `ps5-agent-tools` (`ToolRegistry`).
#[async_trait]
pub trait RuntimeToolExecutor: Send + Sync {
    async fn list_tools(&self) -> Vec<ToolSpec>;

    async fn get_tool(&self, tool_id: &str) -> Option<ToolSpec>;

    async fn execute_tool(
        &self,
        agent: &AgentDefinition,
        task_id: Option<&str>,
        tool_id: &str,
        arguments: serde_json::Value,
        approved_bypass: bool,
        cancel: CancellationToken,
    ) -> Result<ToolExecutionOutput>;

    /// Executes a tool with explicit conversation and sandbox policy context.
    ///
    /// Existing test mocks only need [`RuntimeToolExecutor::execute_tool`]; this default
    /// delegates directly to `execute_tool` so callers can pass sandbox context uniformly.
    #[allow(clippy::too_many_arguments)]
    async fn execute_tool_with_context(
        &self,
        agent: &AgentDefinition,
        task_id: Option<&str>,
        _conversation_id: Option<&str>,
        tool_id: &str,
        arguments: serde_json::Value,
        approved_bypass: bool,
        _sandbox_mode: ps5_agent_core::SandboxMode,
        _approval_policy: ps5_agent_core::SandboxApprovalPolicy,
        cancel: CancellationToken,
    ) -> Result<ToolExecutionOutput> {
        self.execute_tool(agent, task_id, tool_id, arguments, approved_bypass, cancel)
            .await
    }

    /// Returns `(skill_summaries, plugin_rules)` as `(name, description_or_rule)` pairs
    /// for injection into agent system prompts.
    fn extension_prompt_context(&self) -> (Vec<(String, String)>, Vec<(String, String)>) {
        (Vec::new(), Vec::new())
    }
}

/// Clean runtime abstraction over `ps5-agent-memory` (`MemoryManager`).
#[async_trait]
pub trait RuntimeMemoryProvider: Send + Sync {
    async fn build_context_for_agent(
        &self,
        agent: &AgentDefinition,
        conversation_id: Option<&str>,
        task_id: Option<&str>,
        query_hint: &str,
    ) -> Result<String>;

    #[allow(clippy::too_many_arguments)]
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
    ) -> Result<MemoryItem>;
}

/// Default empty tool executor when no external tools are registered.
pub struct NoopToolExecutor;

#[async_trait]
impl RuntimeToolExecutor for NoopToolExecutor {
    async fn list_tools(&self) -> Vec<ToolSpec> {
        Vec::new()
    }

    async fn get_tool(&self, _tool_id: &str) -> Option<ToolSpec> {
        None
    }

    async fn execute_tool(
        &self,
        _agent: &AgentDefinition,
        _task_id: Option<&str>,
        tool_id: &str,
        _arguments: serde_json::Value,
        _approved_bypass: bool,
        _cancel: CancellationToken,
    ) -> Result<ToolExecutionOutput> {
        Err(AgentError::NotFound(format!("tool '{tool_id}' not found")))
    }
}

/// Default StorageBackend-backed memory provider respecting AgentDefinition::memory_permissions.
pub struct StorageMemoryProvider {
    storage: Arc<dyn StorageBackend>,
    config: MemoryConfig,
}

impl StorageMemoryProvider {
    pub fn new(storage: Arc<dyn StorageBackend>, config: MemoryConfig) -> Self {
        Self { storage, config }
    }

    fn can_read_scope(agent: &AgentDefinition, scope: MemoryScope) -> bool {
        match scope {
            MemoryScope::Agent => agent.memory_permissions.read_own,
            MemoryScope::Project | MemoryScope::Task | MemoryScope::Conversation => {
                agent.memory_permissions.read_shared
            }
            MemoryScope::UserPreference => agent.memory_permissions.read_user_prefs,
        }
    }

    fn can_write_scope(agent: &AgentDefinition, scope: MemoryScope) -> bool {
        match scope {
            MemoryScope::Agent => agent.memory_permissions.write_own,
            MemoryScope::Project | MemoryScope::Task | MemoryScope::Conversation => {
                agent.memory_permissions.write_shared
            }
            MemoryScope::UserPreference => agent.memory_permissions.write_user_prefs,
        }
    }
}

#[async_trait]
impl RuntimeMemoryProvider for StorageMemoryProvider {
    async fn build_context_for_agent(
        &self,
        agent: &AgentDefinition,
        _conversation_id: Option<&str>,
        _task_id: Option<&str>,
        query_hint: &str,
    ) -> Result<String> {
        let mut collected = Vec::new();
        let limit = self.config.max_context_memories.max(1);

        if Self::can_read_scope(agent, MemoryScope::Agent) {
            let own = self.storage.query_memories(
                Some(MemoryScope::Agent),
                None,
                Some(&agent.id),
                false,
                limit,
            )?;
            collected.extend(own);
        }

        if Self::can_read_scope(agent, MemoryScope::Project) {
            let shared = self.storage.query_memories(
                Some(MemoryScope::Project),
                Some("shared"),
                None,
                false,
                limit,
            )?;
            collected.extend(shared);
        }

        if Self::can_read_scope(agent, MemoryScope::UserPreference) {
            let prefs = self.storage.query_memories(
                Some(MemoryScope::UserPreference),
                None,
                None,
                false,
                limit,
            )?;
            collected.extend(prefs);
        }

        if collected.is_empty() {
            return Ok(String::new());
        }

        let terms: Vec<String> = query_hint
            .split_whitespace()
            .filter(|w| w.len() >= 3)
            .map(|w| w.to_ascii_lowercase())
            .collect();

        collected.sort_by(|a, b| {
            let score = |item: &MemoryItem| -> i32 {
                let hay = format!("{} {} {}", item.key, item.content, item.tags.join(" "))
                    .to_ascii_lowercase();
                let matches = terms.iter().filter(|t| hay.contains(t.as_str())).count() as i32;
                matches * 10 + i32::from(item.importance)
            };
            score(b).cmp(&score(a))
        });

        let mut out = String::from("Relevant memory context:\n");
        let max_chars = self.config.max_memory_context_chars.max(256);
        for item in collected.into_iter().take(limit) {
            let line = format!("- [{}] {}: {}\n", item.namespace, item.key, item.content);
            if out.len() + line.len() > max_chars {
                break;
            }
            out.push_str(&line);
        }
        Ok(out)
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
        if !Self::can_write_scope(agent, scope) {
            return Err(AgentError::PermissionDenied(format!(
                "agent '{}' lacks write permission for scope {:?}",
                agent.id, scope
            )));
        }

        let now = now_iso();
        let item = MemoryItem {
            id: new_id(),
            scope,
            namespace: namespace.to_string(),
            owner_agent_id: Some(agent.id.clone()),
            conversation_id: conversation_id.map(str::to_string),
            task_id: task_id.map(str::to_string),
            key: key.to_string(),
            content: content.to_string(),
            summary: None,
            importance: importance.clamp(1, 10),
            tags: Vec::new(),
            archived: false,
            created_at: now.clone(),
            updated_at: now,
        };
        self.storage.upsert_memory(&item)?;
        Ok(item)
    }
}
