use ps5_agent_config::DatabaseConfig;
use ps5_agent_core::*;
use ps5_agent_database::{open_storage, DbMetrics, InstrumentedStorage, JsonlStorage, SqliteStorage};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(prefix: &str) -> Self {
        let path = std::env::temp_dir().join(format!("{prefix}-{}", new_id()));
        fs::create_dir_all(&path).expect("create temp dir");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn test_db_cfg(path: PathBuf, driver: &str) -> DatabaseConfig {
    DatabaseConfig {
        driver: driver.to_string(),
        path,
        busy_timeout_ms: 5000,
        cache_size_kb: 4000,
        wal_mode: true,
        ..Default::default()
    }
}

fn sample_agent(id: &str, priority: i32) -> AgentDefinition {
    AgentDefinition {
        id: id.to_string(),
        name: format!("Agent {id}"),
        description: "Test agent".to_string(),
        role: AgentRole::Researcher,
        role_label: "Researcher".to_string(),
        system_prompt: "You are a researcher.".to_string(),
        model_config: AgentModelConfig::default(),
        enabled_tools: vec!["web_fetch".to_string(), "file_read".to_string()],
        denied_tools: Vec::new(),
        sandbox_mode: None,
        tool_profile: None,
        memory_permissions: MemoryPermissions::default(),
        max_task_duration_secs: 60,
        max_reasoning_iterations: 8,
        priority,
        can_communicate: true,
        enabled: true,
        is_builtin: false,
        is_protected: false,
        is_temporary: false,
        created_at: "2026-11-13T10:00:00+00:00".to_string(),
        updated_at: "2026-11-13T10:00:00+00:00".to_string(),
    }
}

fn assert_all_13_tables_crud_and_parity(backend: &dyn StorageBackend) {
    // 1. Migrations idempotency
    backend.run_migrations().expect("migration 1");
    backend.run_migrations().expect("migration 2 idempotent");
    assert_eq!(backend.integrity_check().expect("integrity"), "ok");

    // 2. Agents CRUD
    let a1 = sample_agent("agent-1", 10);
    let mut a2 = sample_agent("agent-2", 20);
    a2.role = AgentRole::Commander;
    a2.is_protected = true;
    a2.denied_tools = vec!["sandbox_exec".to_string()];
    a2.sandbox_mode = Some("workspace_write".to_string());
    a2.tool_profile = Some("research".to_string());
    backend.upsert_agent(&a1).expect("upsert a1");
    backend.upsert_agent(&a2).expect("upsert a2");

    let fetched_a1 = backend.get_agent("agent-1").expect("get a1").expect("a1 exists");
    assert_eq!(fetched_a1.name, "Agent agent-1");
    assert_eq!(fetched_a1.enabled_tools, vec!["web_fetch", "file_read"]);
    assert!(fetched_a1.denied_tools.is_empty());
    assert_eq!(fetched_a1.sandbox_mode, None);
    assert_eq!(fetched_a1.tool_profile, None);

    let fetched_a2 = backend.get_agent("agent-2").expect("get a2").expect("a2 exists");
    assert_eq!(fetched_a2.denied_tools, vec!["sandbox_exec"]);
    assert_eq!(fetched_a2.sandbox_mode.as_deref(), Some("workspace_write"));
    assert_eq!(fetched_a2.tool_profile.as_deref(), Some("research"));

    let agents = backend.list_agents().expect("list agents");
    assert_eq!(agents.len(), 2);
    assert_eq!(agents[0].id, "agent-2"); // higher priority first

    let mut a1_updated = a1.clone();
    a1_updated.name = "Updated Agent 1".to_string();
    backend.upsert_agent(&a1_updated).expect("update a1");
    assert_eq!(
        backend.get_agent("agent-1").unwrap().unwrap().name,
        "Updated Agent 1"
    );
    assert!(backend.delete_agent("agent-2").expect("delete a2"));
    assert!(!backend.delete_agent("agent-2").expect("delete a2 again"));

    // 3. Conversations & Messages CRUD
    let conv = Conversation {
        id: "conv-1".to_string(),
        title: "Main Chat".to_string(),
        mode: ConversationMode::Direct {
            agent_id: "agent-1".to_string(),
        },
        created_at: "2026-11-13T10:01:00+00:00".to_string(),
        updated_at: "2026-11-13T10:01:00+00:00".to_string(),
        archived: false,
        sort_order: 0,
    };
    backend.create_conversation(&conv).expect("create conv");
    assert!(backend.create_conversation(&conv).is_err());

    let conv_arch = Conversation {
        id: "conv-arch".to_string(),
        title: "Old Chat".to_string(),
        mode: ConversationMode::Group {
            participant_agent_ids: vec!["agent-1".to_string()],
        },
        created_at: "2026-11-13T09:00:00+00:00".to_string(),
        updated_at: "2026-11-13T09:00:00+00:00".to_string(),
        archived: true,
        sort_order: 0,
    };
    backend.create_conversation(&conv_arch).expect("create conv_arch");

    assert_eq!(backend.list_conversations(false).unwrap().len(), 1);
    assert_eq!(backend.list_conversations(true).unwrap().len(), 2);
    assert_eq!(
        backend.get_conversation("conv-1").unwrap().unwrap().title,
        "Main Chat"
    );

    for i in 1..=5 {
        let msg = ChatMessage {
            id: format!("msg-{i}"),
            conversation_id: "conv-1".to_string(),
            role: if i % 2 == 1 {
                ChatRole::User
            } else {
                ChatRole::Assistant
            },
            agent_id: if i % 2 == 0 {
                Some("agent-1".to_string())
            } else {
                None
            },
            content: if i == 3 {
                "100% PS5_SDK hardware note".to_string()
            } else {
                format!("Message number {i}")
            },
            reasoning: None,
            tool_calls_json: None,
            task_id: None,
            token_count: 10,
            latency_ms: Some(120),
            archived: false,
            attachments: Vec::new(),
            created_at: format!("2026-11-13T10:01:0{i}+00:00"),
        };
        backend.append_chat_message(&msg).expect("append msg");
    }

    let recent2 = backend.list_chat_messages("conv-1", false, 2).unwrap();
    assert_eq!(recent2.len(), 2);
    assert_eq!(recent2[0].id, "msg-4");
    assert_eq!(recent2[1].id, "msg-5");

    // Search with SQL wildcard characters escaped properly
    let hits = backend.search_chat_messages("100% PS5_", 10).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id, "msg-3");
    assert_eq!(backend.search_chat_messages("   ", 10).unwrap().len(), 0);

    // Archive old messages keeping 2 recent
    let archived_n = backend.archive_old_chat_messages("conv-1", 2).unwrap();
    assert_eq!(archived_n, 3);
    assert_eq!(backend.list_chat_messages("conv-1", false, 10).unwrap().len(), 2);
    assert_eq!(backend.list_chat_messages("conv-1", true, 10).unwrap().len(), 5);

    // 4. Tasks & Task Events CRUD (including upsert preserving task_events)
    let mut task = TaskRecord {
        id: "task-1".to_string(),
        conversation_id: Some("conv-1".to_string()),
        user_request: "Analyze logs".to_string(),
        assigned_agent_id: "agent-1".to_string(),
        parent_task_id: None,
        priority: 5,
        created_at: "2026-11-13T10:02:00+00:00".to_string(),
        started_at: None,
        completed_at: None,
        status: TaskStatus::Pending,
        result: None,
        error_info: None,
        has_side_effects: false,
        progress_pct: 0,
        progress_note: None,
    };
    backend.upsert_task(&task).expect("upsert task");
    backend
        .append_task_event(&TaskEvent {
            id: "evt-1".to_string(),
            task_id: "task-1".to_string(),
            timestamp: "2026-11-13T10:02:01+00:00".to_string(),
            event_type: "started".to_string(),
            details: "started".to_string(),
        })
        .expect("append task event");

    task.status = TaskStatus::Completed;
    task.progress_pct = 100;
    task.result = Some("done".to_string());
    backend.upsert_task(&task).expect("update task");

    let fetched_task = backend.get_task("task-1").unwrap().unwrap();
    assert_eq!(fetched_task.status, TaskStatus::Completed);
    assert_eq!(fetched_task.progress_pct, 100);
    assert_eq!(backend.list_tasks(10).unwrap().len(), 1);
    // Crucial: upsert_task must NOT cascade delete task_events!
    assert_eq!(backend.list_task_events("task-1").unwrap().len(), 1);

    // Delete conversation cascades messages
    assert!(backend.delete_conversation("conv-1").unwrap());
    assert!(backend.get_conversation("conv-1").unwrap().is_none());
    assert_eq!(backend.list_chat_messages("conv-1", true, 10).unwrap().len(), 0);

    // 5. Agent-to-Agent Messages CRUD
    let env = AgentEnvelope {
        id: "env-1".to_string(),
        sender_agent_id: "agent-1".to_string(),
        recipient_agent_id: "agent-2".to_string(),
        task_id: Some("task-1".to_string()),
        timestamp: "2026-11-13T10:03:00+00:00".to_string(),
        message_type: AgentMessageType::TaskAssignment,
        content: "Please check".to_string(),
        status: DeliveryStatus::Queued,
        hop_count: 1,
    };
    backend.append_agent_message(&env).expect("append agent msg");
    backend
        .update_agent_message_status("env-1", DeliveryStatus::Delivered)
        .expect("update status");
    assert!(backend
        .update_agent_message_status("missing-env", DeliveryStatus::Delivered)
        .is_err());
    let envs = backend
        .list_agent_messages(Some("task-1"), Some("agent-2"), 10)
        .unwrap();
    assert_eq!(envs.len(), 1);
    assert_eq!(envs[0].status, DeliveryStatus::Delivered);
    assert_eq!(backend.count_task_agent_messages("task-1").unwrap(), 1);

    // 6. Memories CRUD
    let mem = MemoryItem {
        id: "mem-1".to_string(),
        scope: MemoryScope::Agent,
        namespace: "default".to_string(),
        owner_agent_id: Some("agent-1".to_string()),
        conversation_id: None,
        task_id: None,
        key: "ps5_fw".to_string(),
        content: "Firmware is 13.40".to_string(),
        summary: Some("FW 13.40".to_string()),
        importance: 9,
        tags: vec!["ps5".to_string(), "firmware".to_string()],
        archived: false,
        created_at: "2026-11-13T10:04:00+00:00".to_string(),
        updated_at: "2026-11-13T10:04:00+00:00".to_string(),
    };
    backend.upsert_memory(&mem).expect("upsert mem");
    let mems = backend
        .query_memories(Some(MemoryScope::Agent), Some("default"), Some("agent-1"), false, 10)
        .unwrap();
    assert_eq!(mems.len(), 1);
    assert_eq!(mems[0].tags, vec!["ps5", "firmware"]);
    let (cnt, bytes) = backend.memory_stats_for_agent("agent-1").unwrap();
    assert_eq!(cnt, 1);
    assert_eq!(bytes, mem.content.len() as u64);

    let mem2 = MemoryItem {
        id: "mem-2".to_string(),
        key: "extra".to_string(),
        content: "abc".to_string(),
        ..mem.clone()
    };
    backend.upsert_memory(&mem2).expect("upsert mem2");
    assert!(backend.delete_memory("mem-1").unwrap());
    assert!(!backend.delete_memory("mem-1").unwrap());
    assert_eq!(backend.delete_agent_memories("agent-1").unwrap(), 1);

    // 7. Tools & Executions CRUD
    let tool = ToolSpec {
        id: "file_read".to_string(),
        name: "file_read".to_string(),
        category: "filesystem".to_string(),
        description: "Reads a file".to_string(),
        input_schema: serde_json::json!({"type": "object"}),
        output_schema: serde_json::json!({"type": "string"}),
        required_permissions: vec![ToolPermission::FileRead],
        requires_approval: false,
        is_mutating: false,
        enabled: true,
        timeout_secs: 20,
    };
    backend.upsert_tool_spec(&tool).expect("upsert tool");
    assert_eq!(backend.list_tool_specs().unwrap().len(), 1);

    let exec = ToolExecutionRecord {
        id: "exec-1".to_string(),
        tool_id: "file_read".to_string(),
        agent_id: "agent-1".to_string(),
        task_id: Some("task-1".to_string()),
        input_json: r#"{"path":"a.txt"}"#.to_string(),
        output_json: Some(r#"{"content":"hello"}"#.to_string()),
        success: true,
        error: None,
        duration_ms: 4,
        created_at: "2026-11-13T10:05:00+00:00".to_string(),
    };
    backend.record_tool_execution(&exec).expect("record exec");
    let execs = backend.list_tool_executions(Some("agent-1"), 10).unwrap();
    assert_eq!(execs.len(), 1);
    assert!(execs[0].success);

    // 8. Settings CRUD
    backend.set_setting("theme", "dark").expect("set theme");
    backend.set_setting("theme", "midnight").expect("overwrite theme");
    backend.set_setting("locale", "en-US").expect("set locale");
    assert_eq!(backend.get_setting("theme").unwrap().as_deref(), Some("midnight"));
    assert_eq!(backend.get_setting("missing").unwrap(), None);
    let all_settings = backend.list_settings().unwrap();
    assert_eq!(
        all_settings,
        vec![
            ("locale".to_string(), "en-US".to_string()),
            ("theme".to_string(), "midnight".to_string())
        ]
    );

    // 9. Sessions CRUD
    let sess_old = SessionRecord {
        id: "sess-old".to_string(),
        token_sha256: "hash-old".to_string(),
        csrf_token: "csrf-old".to_string(),
        username: "admin".to_string(),
        client_ip: "127.0.0.1".to_string(),
        created_at: "2026-11-13T08:00:00+00:00".to_string(),
        expires_at: "2026-11-13T09:00:00+00:00".to_string(),
        revoked: false,
    };
    let sess_active = SessionRecord {
        id: "sess-active".to_string(),
        token_sha256: "hash-active".to_string(),
        csrf_token: "csrf-active".to_string(),
        username: "admin".to_string(),
        client_ip: "127.0.0.1".to_string(),
        created_at: "2026-11-13T10:00:00+00:00".to_string(),
        expires_at: "2026-11-14T10:00:00+00:00".to_string(),
        revoked: false,
    };
    backend.create_session(&sess_old).expect("create old sess");
    backend.create_session(&sess_active).expect("create active sess");
    backend.revoke_session("sess-active").expect("revoke active sess");
    let fetched_sess = backend.get_session_by_hash("hash-active").unwrap().unwrap();
    assert!(fetched_sess.revoked);

    let purged = backend
        .purge_expired_sessions("2026-11-13T10:05:00+00:00")
        .unwrap();
    assert_eq!(purged, 1);
    assert!(backend.get_session_by_hash("hash-old").unwrap().is_none());
    assert!(backend.get_session_by_hash("hash-active").unwrap().is_some());

    // 10. Approvals CRUD
    let mut appr = ApprovalRecord {
        id: "appr-1".to_string(),
        task_id: Some("task-1".to_string()),
        agent_id: "agent-1".to_string(),
        operation: "file_delete".to_string(),
        tool_id: Some("file_delete".to_string()),
        arguments_json: r#"{"path":"foo"}"#.to_string(),
        reason: "Cleanup".to_string(),
        status: ApprovalStatus::Pending,
        created_at: "2026-11-13T10:06:00+00:00".to_string(),
        resolved_at: None,
        resolved_by: None,
    };
    backend.upsert_approval(&appr).expect("upsert appr");
    assert_eq!(backend.list_approvals(true, 10).unwrap().len(), 1);
    appr.status = ApprovalStatus::Approved;
    appr.resolved_by = Some("admin".to_string());
    appr.resolved_at = Some("2026-11-13T10:06:30+00:00".to_string());
    backend.upsert_approval(&appr).expect("resolve appr");
    assert_eq!(backend.list_approvals(true, 10).unwrap().len(), 0);
    assert_eq!(backend.list_approvals(false, 10).unwrap().len(), 1);
    assert_eq!(
        backend.get_approval("appr-1").unwrap().unwrap().status,
        ApprovalStatus::Approved
    );

    // 11. Audit Logs CRUD
    backend
        .append_audit_log(&AuditLogEntry {
            id: "audit-1".to_string(),
            timestamp: "2026-11-13T10:07:00+00:00".to_string(),
            actor: "admin".to_string(),
            action: "approval.resolve".to_string(),
            target: "appr-1".to_string(),
            severity: "info".to_string(),
            outcome: "success".to_string(),
            details_json: "{}".to_string(),
        })
        .expect("append audit");
    assert_eq!(backend.list_audit_logs(10).unwrap().len(), 1);

    // 12. Webhooks CRUD
    let mut hook1 = WebhookRecord {
        id: "hook-1".to_string(),
        name: "GitHub Push".to_string(),
        enabled: true,
        secret_hash: "sha256-hash-1".to_string(),
        secret_prefix: "whsec_11".to_string(),
        target_agent_id: "agent-1".to_string(),
        conversation_id: Some("conv-hook".to_string()),
        prompt_template: "Handle payload: {{payload}}".to_string(),
        created_at: "2026-11-13T10:08:00+00:00".to_string(),
        updated_at: "2026-11-13T10:08:00+00:00".to_string(),
        last_triggered_at: None,
        trigger_count: 0,
    };
    let hook2 = WebhookRecord {
        id: "hook-2".to_string(),
        name: "Alert Hook".to_string(),
        enabled: false,
        secret_hash: "sha256-hash-2".to_string(),
        secret_prefix: "whsec_22".to_string(),
        target_agent_id: "agent-1".to_string(),
        conversation_id: None,
        prompt_template: "Triage alert".to_string(),
        created_at: "2026-11-13T10:09:00+00:00".to_string(),
        updated_at: "2026-11-13T10:09:00+00:00".to_string(),
        last_triggered_at: Some("2026-11-13T10:09:30+00:00".to_string()),
        trigger_count: 3,
    };
    backend.upsert_webhook(&hook1).expect("upsert hook1");
    backend.upsert_webhook(&hook2).expect("upsert hook2");
    assert!(backend.get_webhook("missing-hook").unwrap().is_none());

    let fetched_h1 = backend.get_webhook("hook-1").unwrap().expect("hook1 exists");
    assert_eq!(fetched_h1, hook1);

    let hooks = backend.list_webhooks().expect("list webhooks");
    assert_eq!(hooks.len(), 2);
    assert_eq!(hooks[0].id, "hook-2");
    assert_eq!(hooks[1].id, "hook-1");

    hook1.enabled = false;
    hook1.trigger_count = 1;
    hook1.last_triggered_at = Some("2026-11-13T10:10:00+00:00".to_string());
    hook1.updated_at = "2026-11-13T10:10:00+00:00".to_string();
    backend.upsert_webhook(&hook1).expect("update hook1");
    assert_eq!(backend.get_webhook("hook-1").unwrap().unwrap(), hook1);

    assert!(backend.delete_webhook("hook-2").expect("delete hook2"));
    assert!(!backend.delete_webhook("hook-2").expect("delete hook2 again"));
    assert!(backend.get_webhook("hook-2").unwrap().is_none());
    assert_eq!(backend.list_webhooks().unwrap().len(), 1);

    // Checkpoint & integrity check
    backend.checkpoint().expect("checkpoint");
    assert_eq!(backend.integrity_check().expect("integrity after writes"), "ok");
}

#[test]
fn sqlite_crud_parity_checkpoint_and_migrations_idempotency() {
    let dir = TempDir::new("ps5-db-sqlite-crud");
    let db_path = dir.path().join("database").join("agent.db");
    let cfg = test_db_cfg(db_path.clone(), "sqlite");
    let storage = SqliteStorage::open(&db_path, &cfg).expect("open sqlite");

    assert_eq!(storage.driver_name(), "sqlite");
    assert_all_13_tables_crud_and_parity(&storage);
}

#[test]
fn jsonl_crud_parity_and_checkpoint() {
    let dir = TempDir::new("ps5-db-jsonl-crud");
    let jsonl_dir = dir.path().join("jsonl_db");
    let storage = JsonlStorage::open(&jsonl_dir).expect("open jsonl");

    assert_eq!(storage.driver_name(), "jsonl");
    assert_all_13_tables_crud_and_parity(&storage);
    assert!(jsonl_dir.join("state.json").exists());
    assert!(jsonl_dir.join("audit_logs.jsonl").exists());
}

#[test]
fn sqlite_and_jsonl_restart_persistence() {
    let dir = TempDir::new("ps5-db-restart");
    let db_path = dir.path().join("agent.db");
    let cfg = test_db_cfg(db_path.clone(), "sqlite");

    {
        let s = SqliteStorage::open(&db_path, &cfg).expect("open 1");
        s.run_migrations().expect("migrate");
        s.set_setting("persist_key", "persist_val").expect("set");
        s.upsert_agent(&sample_agent("a-persist", 1)).expect("agent");
        s.checkpoint().expect("checkpoint");
    }
    {
        let s = SqliteStorage::open(&db_path, &cfg).expect("reopen");
        s.run_migrations().expect("re-migrate");
        assert_eq!(
            s.get_setting("persist_key").unwrap().as_deref(),
            Some("persist_val")
        );
        assert!(s.get_agent("a-persist").unwrap().is_some());
    }

    let jdir = dir.path().join("jsonl_db");
    {
        let j = JsonlStorage::open(&jdir).expect("open jsonl 1");
        j.run_migrations().expect("migrate");
        j.set_setting("persist_key", "persist_val").expect("set");
        j.upsert_agent(&sample_agent("a-persist", 1)).expect("agent");
        j.checkpoint().expect("checkpoint");
    }
    {
        let j = JsonlStorage::open(&jdir).expect("reopen jsonl");
        j.run_migrations().expect("re-migrate");
        assert_eq!(
            j.get_setting("persist_key").unwrap().as_deref(),
            Some("persist_val")
        );
        assert!(j.get_agent("a-persist").unwrap().is_some());
    }
}

#[test]
fn concurrent_multithread_access_sqlite_and_jsonl() {
    let dir = TempDir::new("ps5-db-concurrent");
    let db_path = dir.path().join("concurrent.db");
    let cfg = test_db_cfg(db_path.clone(), "sqlite");
    let sqlite = Arc::new(SqliteStorage::open(&db_path, &cfg).expect("open sqlite"));
    sqlite.run_migrations().expect("migrate sqlite");

    let jsonl = Arc::new(JsonlStorage::open(&dir.path().join("jsonl_db")).expect("open jsonl"));
    jsonl.run_migrations().expect("migrate jsonl");

    for backend in [sqlite as Arc<dyn StorageBackend>, jsonl as Arc<dyn StorageBackend>] {
        let mut handles = Vec::new();
        for t in 0..6 {
            let b = Arc::clone(&backend);
            handles.push(thread::spawn(move || {
                for i in 0..10 {
                    let key = format!("k-{t}-{i}");
                    b.set_setting(&key, &format!("v-{t}-{i}")).expect("set_setting");
                    let _ = b.get_setting(&key).expect("get_setting");
                    b.append_audit_log(&AuditLogEntry {
                        id: new_id(),
                        timestamp: now_iso(),
                        actor: format!("thread-{t}"),
                        action: "ping".to_string(),
                        target: key,
                        severity: "info".to_string(),
                        outcome: "ok".to_string(),
                        details_json: "{}".to_string(),
                    })
                    .expect("audit");
                }
            }));
        }
        for h in handles {
            h.join().expect("thread join");
        }
        assert_eq!(backend.list_settings().unwrap().len(), 60);
        assert_eq!(backend.list_audit_logs(100).unwrap().len(), 60);
    }
}

#[test]
fn open_storage_and_instrumented_metrics() {
    let dir = TempDir::new("ps5-db-open-storage");
    let cfg_auto = test_db_cfg(dir.path().join("database").join("agent.db"), "auto");
    let (store, metrics) = open_storage(&cfg_auto, dir.path()).expect("open auto");
    assert_eq!(store.driver_name(), "sqlite");
    assert!(metrics.ops_total() >= 1); // run_migrations recorded

    let before = metrics.ops_total();
    store.set_setting("k", "v").expect("set");
    assert_eq!(store.get_setting("k").unwrap().as_deref(), Some("v"));
    assert_eq!(metrics.ops_total(), before + 2);

    let cfg_jsonl = test_db_cfg(dir.path().join("unused.db"), "jsonl");
    let (jstore, jmetrics) = open_storage(&cfg_jsonl, dir.path()).expect("open jsonl");
    assert_eq!(jstore.driver_name(), "jsonl");
    jstore.set_setting("jk", "jv").expect("set jsonl");
    assert!(jmetrics.ops_total() >= 2);

    // Direct InstrumentedStorage wrapper check
    let direct_metrics = Arc::new(DbMetrics::new());
    let direct = JsonlStorage::open(&dir.path().join("direct_jsonl")).unwrap();
    let wrapped = InstrumentedStorage::new(direct, Arc::clone(&direct_metrics));
    wrapped.run_migrations().unwrap();
    assert_eq!(direct_metrics.ops_total(), 1);
}

#[test]
fn sqlite_pragmas_truncate_fallback_and_auto_driver_fallback() {
    let dir = TempDir::new("ps5-db-pragmas-fallback");

    // 1. wal_mode = false uses TRUNCATE cleanly
    let trunc_path = dir.path().join("trunc.db");
    let mut cfg_trunc = test_db_cfg(trunc_path.clone(), "sqlite");
    cfg_trunc.wal_mode = false;
    let s_trunc = SqliteStorage::open(&trunc_path, &cfg_trunc).expect("open truncate sqlite");
    s_trunc.run_migrations().expect("migrate truncate");
    assert_eq!(s_trunc.integrity_check().unwrap(), "ok");
    s_trunc.checkpoint().expect("checkpoint truncate");

    // 2. Corrupted JSONL detected by integrity_check
    let bad_jdir = dir.path().join("bad_jsonl");
    let j = JsonlStorage::open(&bad_jdir).expect("open jsonl");
    j.run_migrations().expect("migrate jsonl");
    assert_eq!(j.integrity_check().unwrap(), "ok");
    fs::write(bad_jdir.join("audit_logs.jsonl"), "{not valid json\n").unwrap();
    assert!(j.integrity_check().is_err());

    // 3. "auto" falls back to JsonlStorage when SQLite path is a directory (cannot open as file)
    let blocker_dir = dir.path().join("sqlite_blocker_dir");
    fs::create_dir_all(&blocker_dir).unwrap();
    let cfg_auto_fallback = test_db_cfg(blocker_dir, "auto");
    let (fallback_store, _) = open_storage(&cfg_auto_fallback, dir.path()).expect("auto fallback");
    assert_eq!(fallback_store.driver_name(), "jsonl");
    fallback_store.set_setting("fb", "ok").unwrap();
    assert_eq!(fallback_store.get_setting("fb").unwrap().as_deref(), Some("ok"));
}
