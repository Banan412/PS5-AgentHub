use ps5_agent_config::{DatabaseConfig, MemoryConfig};
use ps5_agent_core::*;
use ps5_agent_database::{open_storage, JsonlStorage};
use ps5_agent_memory::{MemoryManager, MemoryQuery};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

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

fn setup_memory_manager(dir: &Path, cfg: MemoryConfig) -> (Arc<dyn StorageBackend>, MemoryManager) {
    let db_cfg = DatabaseConfig {
        driver: "sqlite".to_string(),
        path: dir.join("database").join("agent.db"),
        busy_timeout_ms: 5000,
        cache_size_kb: 4000,
        wal_mode: true,
        ..Default::default()
    };
    let (storage, _) = open_storage(&db_cfg, dir).expect("open sqlite storage");
    let mgr = MemoryManager::new(Arc::clone(&storage), cfg);
    (storage, mgr)
}

fn make_mem(
    id: &str,
    scope: MemoryScope,
    owner: Option<&str>,
    key: &str,
    content: &str,
    importance: u8,
    updated_at: &str,
) -> MemoryItem {
    MemoryItem {
        id: id.to_string(),
        scope,
        namespace: "default".to_string(),
        owner_agent_id: owner.map(str::to_string),
        conversation_id: None,
        task_id: None,
        key: key.to_string(),
        content: content.to_string(),
        summary: None,
        importance,
        tags: vec![],
        archived: false,
        created_at: updated_at.to_string(),
        updated_at: updated_at.to_string(),
    }
}

#[test]
fn supports_all_five_scopes_and_permission_enforcement() {
    let dir = TempDir::new("ps5-mem-perms");
    let (_, mgr) = setup_memory_manager(dir.path(), MemoryConfig {
        max_memories_per_agent: 50,
        archive_after_messages: 40,
        keep_recent_messages: 16,
        max_context_memories: 8,
        max_memory_context_chars: 3200,
    });

    // Store memories in all 5 scopes
    let m_agent_a = make_mem(
        "m-agent-a",
        MemoryScope::Agent,
        Some("agent-a"),
        "secret_a",
        "Agent A private note",
        7,
        "2026-11-13T10:00:00+00:00",
    );
    let m_agent_b = make_mem(
        "m-agent-b",
        MemoryScope::Agent,
        Some("agent-b"),
        "secret_b",
        "Agent B private note",
        7,
        "2026-11-13T10:00:00+00:00",
    );
    let mut m_conv = make_mem(
        "m-conv",
        MemoryScope::Conversation,
        Some("agent-a"),
        "topic",
        "Discussing PS5 kernel memory",
        6,
        "2026-11-13T10:00:00+00:00",
    );
    m_conv.conversation_id = Some("conv-1".to_string());
    let m_proj = make_mem(
        "m-proj",
        MemoryScope::Project,
        None,
        "arch",
        "Rust workspace with 13 crates",
        8,
        "2026-11-13T10:00:00+00:00",
    );
    let mut m_task = make_mem(
        "m-task",
        MemoryScope::Task,
        Some("agent-a"),
        "step1",
        "Completed static scan",
        5,
        "2026-11-13T10:00:00+00:00",
    );
    m_task.task_id = Some("task-1".to_string());
    let m_pref = make_mem(
        "m-pref",
        MemoryScope::UserPreference,
        None,
        "style",
        "Prefer concise technical answers",
        9,
        "2026-11-13T10:00:00+00:00",
    );

    for m in [m_agent_a, m_agent_b, m_conv, m_proj, m_task, m_pref] {
        mgr.store_memory(m).expect("store memory");
    }

    // Default permissions: read_own=true, read_shared=true, read_user_prefs=true, cross_agent_read=false, write_user_prefs=false
    let default_perms = MemoryPermissions::default();

    // Reading agent-b's memories from agent-a without cross_agent_read -> PermissionDenied
    let cross_err = mgr.read_agent_memory(
        "agent-a",
        &default_perms,
        Some("agent-b"),
        None,
        None,
        false,
        20,
    );
    assert!(matches!(cross_err, Err(AgentError::PermissionDenied(_))));

    // With cross_agent_read = true -> allowed
    let cross_perms = MemoryPermissions {
        cross_agent_read: true,
        ..default_perms.clone()
    };
    let b_mems = mgr
        .read_agent_memory("agent-a", &cross_perms, Some("agent-b"), None, None, false, 20)
        .expect("cross read allowed");
    assert_eq!(b_mems.len(), 1);
    assert_eq!(b_mems[0].id, "m-agent-b");

    // Writing UserPreference with write_user_prefs = false -> PermissionDenied
    let pref_write = mgr.write_agent_memory(
        "agent-a",
        &default_perms,
        make_mem(
            "m-pref-new",
            MemoryScope::UserPreference,
            None,
            "lang",
            "Rust",
            5,
            "2026-11-13T10:01:00+00:00",
        ),
    );
    assert!(matches!(pref_write, Err(AgentError::PermissionDenied(_))));

    // Reading explicit UserPreference with read_user_prefs = false -> PermissionDenied
    let no_prefs = MemoryPermissions {
        read_user_prefs: false,
        ..default_perms.clone()
    };
    assert!(matches!(
        mgr.read_agent_memory(
            "agent-a",
            &no_prefs,
            None,
            Some(MemoryScope::UserPreference),
            None,
            false,
            20,
        ),
        Err(AgentError::PermissionDenied(_))
    ));

    // Unscoped read_agent_memory filters out agent-b private and user_prefs when disabled
    let filtered = mgr
        .read_agent_memory("agent-a", &no_prefs, None, None, None, false, 50)
        .expect("filtered read");
    let ids: Vec<_> = filtered.iter().map(|m| m.id.as_str()).collect();
    assert!(ids.contains(&"m-agent-a"));
    assert!(ids.contains(&"m-conv"));
    assert!(ids.contains(&"m-proj"));
    assert!(ids.contains(&"m-task"));
    assert!(!ids.contains(&"m-agent-b"));
    assert!(!ids.contains(&"m-pref"));
}

#[test]
fn retrieval_scoring_keyword_recency_importance_and_bounded_formatting() {
    let dir = TempDir::new("ps5-mem-retrieval");
    let (_, mgr) = setup_memory_manager(dir.path(), MemoryConfig {
        max_memories_per_agent: 50,
        archive_after_messages: 40,
        keep_recent_messages: 16,
        max_context_memories: 3,
        max_memory_context_chars: 140,
    });

    let perms = MemoryPermissions::default();
    let now = now_utc();
    let recent = now.to_rfc3339();
    let old = (now - chrono::Duration::days(30)).to_rfc3339();

    let mut m_match_recent = make_mem(
        "m-match-recent",
        MemoryScope::Project,
        Some("agent-a"),
        "sqlite_wal",
        "SQLite WAL mode falls back to TRUNCATE on PS5",
        8,
        &recent,
    );
    m_match_recent.tags = vec!["sqlite".to_string(), "wal".to_string()];

    let m_match_old_low = make_mem(
        "m-match-old-low",
        MemoryScope::Project,
        Some("agent-a"),
        "sqlite_note",
        "Old SQLite note",
        2,
        &old,
    );

    let m_unrelated_high = make_mem(
        "m-unrelated",
        MemoryScope::Project,
        Some("agent-a"),
        "ui_theme",
        "Dashboard uses dark slate colors",
        10,
        &recent,
    );

    for m in [m_match_old_low, m_unrelated_high, m_match_recent] {
        mgr.store_memory(m).unwrap();
    }

    let retrieved = mgr
        .retrieve_context(&MemoryQuery {
            reader_agent_id: "agent-a",
            permissions: &perms,
            query_text: "sqlite wal fallback",
            conversation_id: None,
            task_id: None,
            namespace: None,
            scopes: None,
            limit: None,
        })
        .expect("retrieve");

    assert_eq!(retrieved.len(), 3);
    assert_eq!(retrieved[0].id, "m-match-recent");

    // Bounded formatting check
    let block = mgr.format_memory_block_default(&retrieved);
    assert!(!block.is_empty());
    assert!(block.chars().count() <= 140);
    assert!(block.contains("sqlite_wal"));
}

#[test]
fn build_conversation_context_archives_summarizes_and_truncates_to_budget() {
    let dir = TempDir::new("ps5-mem-context");
    let cfg = MemoryConfig {
        max_memories_per_agent: 50,
        archive_after_messages: 6,
        keep_recent_messages: 3,
        max_context_memories: 4,
        max_memory_context_chars: 1000,
    };
    let (storage, mgr) = setup_memory_manager(dir.path(), cfg);

    let conv = Conversation {
        id: "conv-ctx".to_string(),
        title: "Long Chat".to_string(),
        mode: ConversationMode::Direct {
            agent_id: "agent-a".to_string(),
        },
        created_at: "2026-11-13T09:00:00+00:00".to_string(),
        updated_at: "2026-11-13T09:00:00+00:00".to_string(),
        archived: false,
        sort_order: 0,
    };
    storage.create_conversation(&conv).unwrap();

    // Append 8 messages (> archive_after_messages = 6)
    for i in 1..=8 {
        storage
            .append_chat_message(&ChatMessage {
                id: format!("m-{i}"),
                conversation_id: "conv-ctx".to_string(),
                role: if i % 2 == 1 {
                    ChatRole::User
                } else {
                    ChatRole::Assistant
                },
                agent_id: if i % 2 == 0 {
                    Some("agent-a".to_string())
                } else {
                    None
                },
                content: format!(
                    "Turn {i}: detailed diagnostic payload information regarding PS5 firmware 13.40 subsystem check #{i}"
                ),
                reasoning: None,
                tool_calls_json: None,
                task_id: None,
                token_count: 25,
                latency_ms: None,
                archived: false,
                attachments: Vec::new(),
                created_at: format!("2026-11-13T09:00:0{i}+00:00"),
            })
            .unwrap();
    }

    // Large budget: triggers auto-archive of 5 messages (8 - 3 = 5), keeps 3 recent, generates rolling summary
    let ctx = mgr
        .build_conversation_context(
            "conv-ctx",
            "You are Commander.",
            Some("- [project:fw] Target is 13.40"),
            2000,
        )
        .expect("build context");

    assert_eq!(ctx.archived_count, 5);
    assert!(ctx.rolling_summary.is_some());
    assert_eq!(ctx.truncated_messages, 0);
    // 1 folded system message + 3 recent turns
    assert_eq!(ctx.messages.len(), 4);
    assert_eq!(ctx.messages[0].role, "system");
    assert!(ctx.messages[0].content.contains("You are Commander."));
    assert!(ctx.messages[0].content.contains("Target is 13.40"));
    assert!(ctx.messages[0].content.contains("Turn 1"));
    assert!(ctx.messages[1].content.contains("Turn 6"));
    assert!(ctx.messages[2].content.contains("Turn 7"));
    assert!(ctx.messages[3].content.contains("Turn 8"));
    assert!(ctx.estimated_tokens <= 2000);

    // Verify DB really archived those 5 messages
    assert_eq!(storage.list_chat_messages("conv-ctx", false, 50).unwrap().len(), 3);
    assert_eq!(storage.list_chat_messages("conv-ctx", true, 50).unwrap().len(), 8);

    // Tight token budget: forces truncation of older recent turns while keeping newest & staying <= budget
    let tight = mgr
        .build_conversation_context("conv-ctx", "Sys.", None, 65)
        .expect("build tight context");
    assert!(tight.estimated_tokens <= 65);
    assert!(tight.truncated_messages >= 1);
    assert!(tight.messages.len() < 4);
    if tight.messages.len() > 1 {
        assert!(tight.messages.last().unwrap().content.contains("Turn 8"));
    }

    // Also works with JsonlStorage backend
    let jdir = dir.path().join("jsonl_db");
    let jstore = Arc::new(JsonlStorage::open(&jdir).unwrap());
    jstore.run_migrations().unwrap();
    jstore.create_conversation(&conv).unwrap();
    let jmgr = MemoryManager::new(
        jstore,
        MemoryConfig {
            max_memories_per_agent: 50,
            archive_after_messages: 6,
            keep_recent_messages: 3,
            max_context_memories: 4,
            max_memory_context_chars: 1000,
        },
    );
    let jctx = jmgr
        .build_conversation_context("conv-ctx", "Sys.", None, 500)
        .expect("jsonl context");
    assert_eq!(jctx.messages.len(), 1);
}

#[test]
fn enforces_max_memories_per_agent_and_write_permissions() {
    let dir = TempDir::new("ps5-mem-cap");
    let (_, mgr) = setup_memory_manager(
        dir.path(),
        MemoryConfig {
            max_memories_per_agent: 2,
            archive_after_messages: 40,
            keep_recent_messages: 16,
            max_context_memories: 8,
            max_memory_context_chars: 1000,
        },
    );

    let perms = MemoryPermissions::default();
    let m1 = make_mem(
        "cap-1",
        MemoryScope::Agent,
        Some("agent-x"),
        "low",
        "Low importance",
        2,
        "2026-11-13T10:00:00+00:00",
    );
    let m2 = make_mem(
        "cap-2",
        MemoryScope::Agent,
        Some("agent-x"),
        "high",
        "High importance",
        9,
        "2026-11-13T10:01:00+00:00",
    );
    let m3 = make_mem(
        "cap-3",
        MemoryScope::Agent,
        Some("agent-x"),
        "mid",
        "Medium importance",
        6,
        "2026-11-13T10:02:00+00:00",
    );

    mgr.write_agent_memory("agent-x", &perms, m1).unwrap();
    mgr.write_agent_memory("agent-x", &perms, m2).unwrap();
    mgr.write_agent_memory("agent-x", &perms, m3).unwrap();

    let active = mgr
        .read_agent_memory("agent-x", &perms, Some("agent-x"), None, None, false, 10)
        .unwrap();
    assert_eq!(active.len(), 2);
    let active_ids: Vec<_> = active.iter().map(|m| m.id.as_str()).collect();
    assert!(active_ids.contains(&"cap-2"));
    assert!(active_ids.contains(&"cap-3"));
    assert!(!active_ids.contains(&"cap-1"));

    // Denied write_own and write_shared
    let readonly = MemoryPermissions {
        write_own: false,
        write_shared: false,
        read_own: false,
        read_shared: false,
        ..perms
    };
    assert!(matches!(
        mgr.write_agent_memory(
            "agent-x",
            &readonly,
            make_mem("w1", MemoryScope::Agent, Some("agent-x"), "k", "v", 5, "2026-11-13T10:03:00+00:00")
        ),
        Err(AgentError::PermissionDenied(_))
    ));
    assert!(matches!(
        mgr.write_agent_memory(
            "agent-x",
            &readonly,
            make_mem("w2", MemoryScope::Project, None, "k", "v", 5, "2026-11-13T10:03:00+00:00")
        ),
        Err(AgentError::PermissionDenied(_))
    ));
    assert!(matches!(
        mgr.read_agent_memory("agent-x", &readonly, None, Some(MemoryScope::Agent), None, false, 10),
        Err(AgentError::PermissionDenied(_))
    ));
    assert!(matches!(
        mgr.read_agent_memory("agent-x", &readonly, None, Some(MemoryScope::Project), None, false, 10),
        Err(AgentError::PermissionDenied(_))
    ));
}
