use ps5_agent::shutdown::finalize_shutdown;
use ps5_agent::startup::{
    initialize_with_warnings, load_effective_config_with_warnings, CliOptions,
};
use ps5_agent_config::AppConfig;
use ps5_agent_http::server::bind_reusable_listener;
use ps5_agent_http::{serve_with_listener, verify_password};
use std::fs;
use tempfile::TempDir;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_boot_edit_stop_restart_lifecycle_with_bom_and_partial_toml() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().to_path_buf();
    let config_path = data_dir.join("config.toml");

    // Reserve a free loopback port so Boot #1 and Boot #2 bind the exact same TCP port.
    let probe = bind_reusable_listener("127.0.0.1:0").await.unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);
    let bind_addr = format!("127.0.0.1:{port}");

    // --- Boot #1: fresh directory, creates config.toml, DB, 5 agents, bootstrap creds ---
    let opts1 = CliOptions {
        config_path: None,
        data_dir: Some(data_dir.clone()),
        bind: Some(bind_addr.clone()),
        ps5lm_url: None,
    };
    let (cfg1, warnings1) = load_effective_config_with_warnings(&opts1).unwrap();
    assert!(
        warnings1.is_empty(),
        "fresh boot should produce no config warnings, got: {warnings1:?}"
    );
    assert!(config_path.exists(), "config.toml must be created on Boot #1");

    let system1 = initialize_with_warnings(cfg1.clone(), warnings1)
        .await
        .unwrap();
    assert_eq!(system1.store.list_agents().unwrap().len(), 5);
    assert!(data_dir.join("bootstrap_credentials.txt").exists());
    assert!(data_dir.join("ps5-agent.pid").exists());
    assert_eq!(system1.store.driver_name(), "sqlite");

    let listener1 = bind_reusable_listener(&bind_addr).await.unwrap();
    let state1 = system1.http_state.clone();
    let addr1 = bind_addr.clone();
    let server1 = tokio::spawn(async move { serve_with_listener(addr1, listener1, state1).await });

    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(0)
        .build()
        .unwrap();
    let resp1 = client
        .get(format!("http://{bind_addr}/api/v1/agents"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp1.status(),
        reqwest::StatusCode::UNAUTHORIZED,
        "auth_enabled defaults to true on Boot #1"
    );

    system1.shutdown.cancel();
    server1.await.unwrap().unwrap();
    finalize_shutdown(&system1);
    drop(system1);
    assert!(
        !data_dir.join("ps5-agent.pid").exists(),
        "finalize_shutdown must remove ps5-agent.pid"
    );

    // --- Edit config.toml like a Windows/FTP user: UTF-8 BOM, partial sections, `False`, uncapped max_agents = 200 ---
    let edited_toml = "\u{feff}[security]\nauth_enabled = False\nadmin_password = \"NewUserPass99\"\n[agents]\nmax_agents = 200\n";
    fs::write(&config_path, edited_toml).unwrap();

    // --- Boot #2: same data_dir and same TCP port ---
    let opts2 = CliOptions {
        config_path: None,
        data_dir: Some(data_dir.clone()),
        bind: Some(bind_addr.clone()),
        ps5lm_url: None,
    };
    let (cfg2, warnings2) = load_effective_config_with_warnings(&opts2).unwrap();
    assert!(
        !warnings2.iter().any(|w| w.contains("max_agents")),
        "max_agents = 200 must no longer be clamped or warn, got: {warnings2:?}"
    );
    assert!(
        warnings2.is_empty(),
        "valid partial config must produce no warnings, got: {warnings2:?}"
    );
    assert_eq!(cfg2.agents.max_agents, 200);
    assert!(!cfg2.security.auth_enabled);
    assert_eq!(
        cfg2.security.admin_password.as_deref(),
        Some("NewUserPass99")
    );

    let system2 = initialize_with_warnings(cfg2.clone(), warnings2)
        .await
        .unwrap();
    assert_eq!(
        system2.store.list_agents().unwrap().len(),
        5,
        "5 seeded agents must remain intact across restart"
    );

    let stored_hash = system2
        .store
        .get_setting("auth.password_hash")
        .unwrap()
        .expect("admin_password from config must be hashed into DB");
    assert!(stored_hash.starts_with("$argon2id$"));
    assert!(
        verify_password("NewUserPass99", &stored_hash),
        "DB hash must verify against NewUserPass99"
    );

    let listener2 = bind_reusable_listener(&bind_addr)
        .await
        .expect("Boot #2 must immediately rebind the same TCP port");
    let state2 = system2.http_state.clone();
    let addr2 = bind_addr.clone();
    let server2 = tokio::spawn(async move { serve_with_listener(addr2, listener2, state2).await });

    let resp2 = client
        .get(format!("http://{bind_addr}/api/v1/agents"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp2.status(),
        reqwest::StatusCode::OK,
        "auth_enabled = False in partial BOM config.toml must disable auth on Boot #2"
    );
    drop(client);

    system2.shutdown.cancel();
    server2.await.unwrap().unwrap();
    finalize_shutdown(&system2);
    assert!(!data_dir.join("ps5-agent.pid").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_corrupt_config_recovery_creates_bak_and_still_boots() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().to_path_buf();
    let config_path = data_dir.join("config.toml");

    let broken_toml = "[[broken syntax\nfoo = \"unterminated\n";
    fs::write(&config_path, broken_toml).unwrap();

    let opts = CliOptions {
        config_path: None,
        data_dir: Some(data_dir.clone()),
        bind: None,
        ps5lm_url: None,
    };
    let (cfg, warnings) = load_effective_config_with_warnings(&opts).unwrap();
    assert!(
        warnings.iter().any(|w| w.contains(".broken.bak")),
        "expected .broken.bak recovery warning, got: {warnings:?}"
    );

    let bak_path = data_dir.join("config.toml.broken.bak");
    assert!(
        bak_path.exists(),
        "corrupt config backup file must exist at {}",
        bak_path.display()
    );
    assert_eq!(fs::read_to_string(&bak_path).unwrap(), broken_toml);
    assert!(
        AppConfig::load_or_create(&config_path).is_ok(),
        "recovered config.toml must parse cleanly"
    );

    let system = initialize_with_warnings(cfg, warnings).await.unwrap();
    assert_eq!(system.store.list_agents().unwrap().len(), 5);
    assert!(
        system
            .logger
            .recent(Some("warn"), 50)
            .iter()
            .any(|e| e.target == "config" && e.message.contains(".broken.bak")),
        "recovery warning must be logged to AppLogger"
    );

    finalize_shutdown(&system);
    assert!(!data_dir.join("ps5-agent.pid").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_unlimited_zero_limits_and_new_fields_roundtrip_across_restart() {
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().to_path_buf();
    let config_path = data_dir.join("config.toml");

    // --- Boot #1: verify defaults before editing ---
    let opts1 = CliOptions {
        config_path: None,
        data_dir: Some(data_dir.clone()),
        bind: None,
        ps5lm_url: None,
    };
    let (cfg1, warnings1) = load_effective_config_with_warnings(&opts1).unwrap();
    assert!(warnings1.is_empty(), "unexpected warnings: {warnings1:?}");
    assert_eq!(cfg1.agents.max_agents, 0);
    assert_eq!(cfg1.agents.default_task_timeout_secs, 0);
    assert_eq!(cfg1.inference.thinking_level, "medium");
    assert!(cfg1.inference.auto_sync_from_ps5lm);
    assert_eq!(cfg1.server.upload_max_body_bytes, 64 * 1024 * 1024);
    assert_eq!(cfg1.database.retention_days, 30);
    assert_eq!(cfg1.database.max_rows_per_table, 10_000);

    let system1 = initialize_with_warnings(cfg1, warnings1).await.unwrap();
    finalize_shutdown(&system1);
    drop(system1);

    // --- Edit config.toml with 0 = unlimited limits & new fields ---
    let edited_toml = "\u{feff}\
[inference]
thinking_level = \"high\"
auto_sync_from_ps5lm = False
auto_sync_interval_secs = 45

[server]
upload_max_body_bytes = 33554432

[database]
retention_days = 0
max_rows_per_table = 0

[security]
rate_limit_per_minute = 0

[agents]
max_agents = 0
default_task_timeout_secs = 0
";
    fs::write(&config_path, edited_toml).unwrap();

    // --- Boot #2: verify all 0 = unlimited and new fields roundtrip cleanly ---
    let opts2 = CliOptions {
        config_path: None,
        data_dir: Some(data_dir.clone()),
        bind: None,
        ps5lm_url: None,
    };
    let (cfg2, warnings2) = load_effective_config_with_warnings(&opts2).unwrap();
    assert!(
        warnings2.is_empty(),
        "valid unlimited/new config fields must produce no warnings, got: {warnings2:?}"
    );
    assert_eq!(cfg2.agents.max_agents, 0);
    assert_eq!(cfg2.security.rate_limit_per_minute, 0);
    assert_eq!(cfg2.agents.default_task_timeout_secs, 0);
    assert_eq!(cfg2.inference.thinking_level, "high");
    assert!(!cfg2.inference.auto_sync_from_ps5lm);
    assert_eq!(cfg2.inference.auto_sync_interval_secs, 45);
    assert_eq!(cfg2.server.upload_max_body_bytes, 33_554_432);
    assert_eq!(cfg2.database.retention_days, 0);
    assert_eq!(cfg2.database.max_rows_per_table, 0);

    let system2 = initialize_with_warnings(cfg2, warnings2).await.unwrap();
    let eff = system2.http_state.effective_config();
    assert_eq!(eff.agents.max_agents, 0);
    assert_eq!(eff.security.rate_limit_per_minute, 0);
    assert_eq!(eff.agents.default_task_timeout_secs, 0);
    assert_eq!(eff.inference.thinking_level, "high");
    assert!(!eff.inference.auto_sync_from_ps5lm);
    assert_eq!(eff.inference.auto_sync_interval_secs, 45);
    assert_eq!(eff.server.upload_max_body_bytes, 33_554_432);
    assert_eq!(eff.database.retention_days, 0);
    assert_eq!(eff.database.max_rows_per_table, 0);

    finalize_shutdown(&system2);
    assert!(!data_dir.join("ps5-agent.pid").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_elf_upgrade_auto_migrates_v1_agents_config_and_meta_preserving_user_data() {
    use ps5_agent_core::{
        AgentDefinition, AgentModelConfig, AgentRole, ChatMessage, ChatRole, Conversation,
        ConversationMode, MemoryPermissions,
    };
    use ps5_agent_database::open_storage;
    use ps5_agent_http::hash_password;
    use ps5_agent_manager::default_agent_definitions;

    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().to_path_buf();
    let config_path = data_dir.join("config.toml");

    // 1. Stage a legacy v1 config.toml (no thinking_level, no auto_sync_from_ps5lm, old v1 caps)
    let legacy_v1_toml = r#"
[security]
auth_enabled = true
admin_username = "customadmin"

[agents]
max_agents = 5
max_active_tasks = 4
max_concurrent_inference = 1

[memory]
max_memories_per_agent = 500
"#;
    fs::write(&config_path, legacy_v1_toml).unwrap();

    // Stage a stale meta.json in data_dir to verify detection logging without errors.
    let stale_meta = serde_json::json!({
        "schema": 2,
        "kind": "repo",
        "filename": "ps5-agent.elf",
        "id": "ps5-agent",
        "version": "v0.0.1",
        "checksum": "0000"
    });
    fs::write(
        data_dir.join("meta.json"),
        serde_json::to_string_pretty(&stale_meta).unwrap(),
    )
    .unwrap();

    // 2. Stage a pre-existing v1 agent.db state
    let mut bootstrap_cfg = AppConfig::with_data_dir(data_dir.clone());
    bootstrap_cfg.normalize_and_clamp();
    let (v1_store, _) = open_storage(&bootstrap_cfg.database, &data_dir).unwrap();
    v1_store.run_migrations().unwrap();
    v1_store.set_setting("agents_seeded", "true").unwrap();
    let pw_hash = hash_password("KeepMyPass77", 8192).unwrap();
    v1_store.set_setting("auth.password_hash", &pw_hash).unwrap();

    for mut def in default_agent_definitions() {
        match def.id.as_str() {
            "commander" | "programmer" | "researcher" => {
                def.system_prompt = format!("You are {} in legacy v1.", def.name);
                def.model_config.max_output_tokens = 1024;
                def.enabled_tools = vec!["file_read".into(), "file_write".into()];
            }
            "executor" => {
                def.system_prompt = "You are Executor in legacy v1.".to_string();
                def.enabled = false;
                def.enabled_tools = vec!["file_read".into()];
            }
            "analyst" => {
                def.system_prompt = "Custom analyst instructions from user".to_string();
                def.created_at = "2026-10-01T00:00:00Z".to_string();
                def.updated_at = "2026-10-15T12:00:00Z".to_string();
                def.enabled_tools = vec!["file_read".into()];
            }
            _ => {}
        }
        v1_store.upsert_agent(&def).unwrap();
    }

    let custom_agent = AgentDefinition {
        id: "my-custom-bot".to_string(),
        name: "My Custom Bot".to_string(),
        description: "User-created custom specialist".to_string(),
        role: AgentRole::Custom,
        role_label: "Custom".to_string(),
        system_prompt: "Keep my custom prompt untouched.".to_string(),
        model_config: AgentModelConfig::default(),
        enabled_tools: vec!["read_file".into()],
        denied_tools: Vec::new(),
        memory_permissions: MemoryPermissions::default(),
        max_task_duration_secs: 120,
        max_reasoning_iterations: 4,
        priority: 40,
        can_communicate: true,
        enabled: true,
        sandbox_mode: None,
        tool_profile: None,
        is_builtin: false,
        is_protected: false,
        is_temporary: false,
        created_at: "2026-10-02T00:00:00Z".to_string(),
        updated_at: "2026-10-02T00:00:00Z".to_string(),
    };
    v1_store.upsert_agent(&custom_agent).unwrap();

    let conv = Conversation {
        id: "conv-legacy-1".to_string(),
        title: "Legacy Session".to_string(),
        mode: ConversationMode::Direct {
            agent_id: "commander".to_string(),
        },
        created_at: "2026-10-02T00:00:00Z".to_string(),
        updated_at: "2026-10-02T00:00:00Z".to_string(),
        archived: false,
        sort_order: 0,
    };
    v1_store.create_conversation(&conv).unwrap();
    let msg = ChatMessage {
        id: "msg-legacy-1".to_string(),
        conversation_id: "conv-legacy-1".to_string(),
        role: ChatRole::User,
        agent_id: None,
        content: "Keep my history intact".to_string(),
        reasoning: None,
        tool_calls_json: None,
        task_id: None,
        token_count: 5,
        latency_ms: None,
        archived: false,
        created_at: "2026-10-02T00:00:01Z".to_string(),
        attachments: Vec::new(),
        timings: None,
    };
    v1_store.append_chat_message(&msg).unwrap();
    drop(v1_store);

    // 3. Boot #1 with the new binary over legacy /data state
    let opts1 = CliOptions {
        config_path: None,
        data_dir: Some(data_dir.clone()),
        bind: None,
        ps5lm_url: None,
    };
    let (cfg1, warnings1) = load_effective_config_with_warnings(&opts1).unwrap();
    assert!(warnings1.is_empty(), "unexpected warnings: {warnings1:?}");

    let system1 = initialize_with_warnings(cfg1, warnings1).await.unwrap();

    // Built-in prompts, token budgets, and missing tools must have upgraded
    let programmer = system1.store.get_agent("programmer").unwrap().unwrap();
    assert!(programmer.system_prompt.starts_with("I am Programmer"));
    assert_eq!(programmer.model_config.max_output_tokens, 0);
    for expected_tool in [
        "read_file",
        "write_file",
        "edit_file",
        "apply_patch",
        "load_skill",
        "mcp:*",
        "plugin:*",
        "custom:*",
    ] {
        assert!(
            programmer.enabled_tools.iter().any(|t| t == expected_tool),
            "programmer missing expected tool '{expected_tool}': {:?}",
            programmer.enabled_tools
        );
    }
    assert!(
        ps5_agent_core::agent_allows_tool_with_policy(
            &programmer.enabled_tools,
            &programmer.denied_tools,
            "file_read",
            "file",
        ) && ps5_agent_core::agent_allows_tool_with_policy(
            &programmer.enabled_tools,
            &programmer.denied_tools,
            "file_write",
            "file",
        ),
        "programmer canonical tools must match legacy file_read/file_write aliases"
    );

    let commander = system1.store.get_agent("commander").unwrap().unwrap();
    assert!(commander.system_prompt.starts_with("I am Commander"));
    assert_eq!(commander.model_config.max_output_tokens, 0);
    assert!(commander.enabled_tools.iter().any(|t| t == "mcp:*"));

    // executor kept disabled status while upgrading prompt & tools
    let executor = system1.store.get_agent("executor").unwrap().unwrap();
    assert!(!executor.enabled);
    assert!(executor.system_prompt.starts_with("I am Executor"));
    assert!(executor.enabled_tools.iter().any(|t| t == "write_file"));

    // analyst preserved its user-customized prompt while gaining new default tools
    let analyst = system1.store.get_agent("analyst").unwrap().unwrap();
    assert_eq!(analyst.system_prompt, "Custom analyst instructions from user");
    assert!(analyst.enabled_tools.iter().any(|t| t == "ps5_info"));

    // Custom agent, conversations, chat messages, and password hash are preserved
    let kept_custom = system1.store.get_agent("my-custom-bot").unwrap().unwrap();
    assert_eq!(kept_custom.system_prompt, "Keep my custom prompt untouched.");
    assert!(system1.store.get_conversation("conv-legacy-1").unwrap().is_some());
    let msgs = system1
        .store
        .list_chat_messages("conv-legacy-1", false, 50)
        .unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].content, "Keep my history intact");
    let kept_hash = system1.store.get_setting("auth.password_hash").unwrap().unwrap();
    assert!(verify_password("KeepMyPass77", &kept_hash));

    // Legacy config.toml backed up & upgraded to v2 defaults while keeping user security fields
    let bak_path = data_dir.join("config.toml.v1.bak");
    assert!(bak_path.exists(), "expected config.toml.v1.bak backup");
    let eff = system1.runtime.config();
    assert_eq!(eff.agents.max_agents, 0);
    assert_eq!(eff.agents.max_active_tasks, 0);
    assert_eq!(eff.agents.max_concurrent_inference, 4);
    assert_eq!(eff.memory.max_memories_per_agent, 0);
    assert_eq!(eff.security.admin_username, "customadmin");
    assert_eq!(eff.inference.thinking_level, "medium");
    assert!(eff.inference.auto_sync_from_ps5lm);

    let reloaded = AppConfig::load_or_create(&config_path).unwrap();
    assert_eq!(reloaded.agents.max_agents, 0);
    assert_eq!(reloaded.agents.max_concurrent_inference, 4);
    assert_eq!(reloaded.security.admin_username, "customadmin");

    // Upgrade stamps and audit log entries are recorded
    assert!(system1
        .store
        .get_setting("installed_release_stamp")
        .unwrap()
        .is_some());
    assert_eq!(
        system1
            .store
            .get_setting("installed_config_schema_version")
            .unwrap()
            .as_deref(),
        Some(ps5_agent::startup::CONFIG_SCHEMA_VERSION)
    );
    assert_eq!(
        system1
            .store
            .get_setting("builtin_agents_schema_version")
            .unwrap()
            .as_deref(),
        Some(ps5_agent_manager::BUILTIN_AGENTS_SCHEMA_VERSION)
    );
    assert!(system1.store.get_setting("last_upgraded_at").unwrap().is_some());

    let audits = system1.store.list_audit_logs(50).unwrap();
    assert!(audits.iter().any(|a| a.action == "system.elf_upgrade"));
    assert!(audits
        .iter()
        .any(|a| a.action == "system.builtin_agents_upgrade"));

    finalize_shutdown(&system1);
    drop(system1);

    // 4. Boot #2: verify idempotence
    let opts2 = CliOptions {
        config_path: None,
        data_dir: Some(data_dir.clone()),
        bind: None,
        ps5lm_url: None,
    };
    let (cfg2, warnings2) = load_effective_config_with_warnings(&opts2).unwrap();
    let system2 = initialize_with_warnings(cfg2, warnings2).await.unwrap();
    assert_eq!(system2.agent_manager.upgrade_builtin_agents().unwrap(), 0);
    assert_eq!(
        system2
            .store
            .get_agent("analyst")
            .unwrap()
            .unwrap()
            .system_prompt,
        "Custom analyst instructions from user"
    );
    assert!(bak_path.exists());
    finalize_shutdown(&system2);
    assert!(!data_dir.join("ps5-agent.pid").exists());
}

fn assert_f32_near(actual: f32, expected: f32, field: &str) {
    assert!(
        (actual - expected).abs() < 1e-4,
        "expected {field} ~= {expected}, got {actual}"
    );
}

fn assert_custom_llamacpp_and_prefill_config(inf: &ps5_agent_config::InferenceConfig) {
    assert!(!inf.prefill_kv_cache);
    assert!(!inf.cache_prompt);
    assert_f32_near(inf.temperature, 0.75, "temperature");
    assert_f32_near(inf.dynatemp_range, 0.25, "dynatemp_range");
    assert_f32_near(inf.dynatemp_exponent, 1.5, "dynatemp_exponent");
    assert_eq!(inf.top_k, 32);
    assert_f32_near(inf.top_p, 0.88, "top_p");
    assert_f32_near(inf.min_p, 0.08, "min_p");
    assert_f32_near(inf.typical_p, 0.92, "typical_p");
    assert_f32_near(inf.xtc_probability, 0.15, "xtc_probability");
    assert_f32_near(inf.xtc_threshold, 0.2, "xtc_threshold");
    assert_f32_near(inf.top_n_sigma, 1.8, "top_n_sigma");
    assert_f32_near(inf.repeat_penalty, 1.15, "repeat_penalty");
    assert_eq!(inf.repeat_last_n, 128);
    assert_f32_near(inf.presence_penalty, 0.35, "presence_penalty");
    assert_f32_near(inf.frequency_penalty, 0.45, "frequency_penalty");
    assert_f32_near(inf.dry_multiplier, 0.8, "dry_multiplier");
    assert_f32_near(inf.dry_base, 1.9, "dry_base");
    assert_eq!(inf.dry_allowed_length, 3);
    assert_eq!(inf.dry_penalty_last_n, 256);
    assert_eq!(inf.mirostat, 2);
    assert_f32_near(inf.mirostat_tau, 4.5, "mirostat_tau");
    assert_f32_near(inf.mirostat_eta, 0.2, "mirostat_eta");
    assert_eq!(inf.seed, 42);
    assert!(inf.ignore_eos);
    assert_eq!(
        inf.stop_sequences,
        vec!["<|end|>".to_string(), "STOP".to_string()]
    );
    assert_eq!(inf.n_keep, 64);
    assert_eq!(inf.id_slot, 1);
    assert_eq!(inf.samplers, "top_k;min_p;temperature");
    assert_eq!(inf.grammar, r#"root ::= "ok""#);
    assert_eq!(inf.reasoning_format, "none");
    assert!(inf.timings_per_token);
    assert_eq!(inf.custom_extra_body_json, r#"{"custom_flag":true}"#);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_llamacpp_fields_prefill_and_sqlite_sort_order_survive_restart() {
    use ps5_agent_core::{Conversation, ConversationMode};

    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().to_path_buf();
    let config_path = data_dir.join("config.toml");

    // --- Boot #1: verify defaults, SQLite backend, and persist custom conversation sort_order ---
    let opts1 = CliOptions {
        config_path: None,
        data_dir: Some(data_dir.clone()),
        bind: None,
        ps5lm_url: None,
    };
    let (cfg1, warnings1) = load_effective_config_with_warnings(&opts1).unwrap();
    assert!(warnings1.is_empty(), "unexpected warnings: {warnings1:?}");
    assert!(cfg1.inference.prefill_kv_cache);
    assert!(cfg1.inference.cache_prompt);
    assert_eq!(cfg1.inference.top_k, 40);
    assert_eq!(cfg1.inference.repeat_last_n, 256);
    assert_f32_near(cfg1.inference.dry_multiplier, 0.8, "dry_multiplier");
    assert_eq!(cfg1.inference.dry_allowed_length, 2);
    assert_eq!(cfg1.inference.dry_penalty_last_n, -1);
    assert_eq!(cfg1.inference.mirostat, 0);
    assert_eq!(cfg1.inference.seed, -1);
    assert!(!cfg1.inference.ignore_eos);
    assert!(cfg1.inference.stop_sequences.is_empty());
    assert_eq!(cfg1.inference.n_keep, 0);
    assert_eq!(cfg1.inference.id_slot, -1);
    assert_eq!(cfg1.inference.reasoning_format, "deepseek");
    assert!(!cfg1.inference.timings_per_token);
    assert!(cfg1.inference.smart_tool_routing);
    assert!(cfg1.inference.cache_type_k.is_empty());
    assert!(cfg1.inference.cache_type_v.is_empty());
    assert!(!cfg1.inference.flash_attn);

    let system1 = initialize_with_warnings(cfg1, warnings1).await.unwrap();
    assert_eq!(system1.store.driver_name(), "sqlite");

    let existing_ids: Vec<String> = system1
        .store
        .list_conversations(false)
        .unwrap()
        .into_iter()
        .map(|c| c.id)
        .collect();

    for (id, title, ts) in [
        ("conv-alpha", "Alpha", "2026-01-01T00:00:01Z"),
        ("conv-beta", "Beta", "2026-01-01T00:00:02Z"),
        ("conv-gamma", "Gamma", "2026-01-01T00:00:03Z"),
    ] {
        system1
            .store
            .create_conversation(&Conversation {
                id: id.to_string(),
                title: title.to_string(),
                mode: ConversationMode::Direct {
                    agent_id: "commander".to_string(),
                },
                created_at: ts.to_string(),
                updated_at: ts.to_string(),
                archived: false,
                sort_order: 0,
            })
            .unwrap();
    }

    // Reorder newest ("conv-gamma") behind "conv-beta" and "conv-alpha" so sort_order
    // overrides updated_at DESC across SQLite restart.
    let mut expected_order = vec![
        "conv-beta".to_string(),
        "conv-alpha".to_string(),
        "conv-gamma".to_string(),
    ];
    expected_order.extend(existing_ids);
    system1.store.reorder_conversations(&expected_order).unwrap();

    let convs1 = system1.store.list_conversations(false).unwrap();
    let ids1: Vec<String> = convs1.iter().map(|c| c.id.clone()).collect();
    assert_eq!(ids1, expected_order);
    assert_eq!(convs1[0].sort_order, 10);
    assert_eq!(convs1[1].sort_order, 20);
    assert_eq!(convs1[2].sort_order, 30);

    finalize_shutdown(&system1);
    drop(system1);

    // --- Edit config.toml: set non-default values for prefill_kv_cache + all 32 llama.cpp fields ---
    let edited_body = r#"[inference]
prefill_kv_cache = false
cache_prompt = false
temperature = 0.75
dynatemp_range = 0.25
dynatemp_exponent = 1.5
top_k = 32
top_p = 0.88
min_p = 0.08
typical_p = 0.92
xtc_probability = 0.15
xtc_threshold = 0.2
top_n_sigma = 1.8
repeat_penalty = 1.15
repeat_last_n = 128
presence_penalty = 0.35
frequency_penalty = 0.45
dry_multiplier = 0.8
dry_base = 1.9
dry_allowed_length = 3
dry_penalty_last_n = 256
mirostat = 2
mirostat_tau = 4.5
mirostat_eta = 0.2
seed = 42
ignore_eos = true
stop_sequences = ["<|end|>", "STOP"]
n_keep = 64
id_slot = 1
samplers = "top_k;min_p;temperature"
grammar = 'root ::= "ok"'
reasoning_format = "none"
timings_per_token = true
custom_extra_body_json = '{"custom_flag":true}'
"#;
    let edited_toml = format!("\u{feff}{edited_body}");
    fs::write(&config_path, edited_toml).unwrap();

    // --- Boot #2: verify all 33 inference fields and SQLite schema v3 sort_order survive restart ---
    let opts2 = CliOptions {
        config_path: None,
        data_dir: Some(data_dir.clone()),
        bind: None,
        ps5lm_url: None,
    };
    let (cfg2, warnings2) = load_effective_config_with_warnings(&opts2).unwrap();
    assert!(
        warnings2.is_empty(),
        "valid llama.cpp + prefill_kv_cache config must produce no warnings, got: {warnings2:?}"
    );
    assert_custom_llamacpp_and_prefill_config(&cfg2.inference);

    let system2 = initialize_with_warnings(cfg2, warnings2).await.unwrap();
    assert_eq!(system2.store.driver_name(), "sqlite");
    assert_custom_llamacpp_and_prefill_config(&system2.http_state.effective_config().inference);
    assert_custom_llamacpp_and_prefill_config(&system2.runtime.config().inference);

    let convs2 = system2.store.list_conversations(false).unwrap();
    let ids2: Vec<String> = convs2.iter().map(|c| c.id.clone()).collect();
    assert_eq!(ids2, expected_order);
    assert_eq!(convs2[0].sort_order, 10);
    assert_eq!(convs2[1].sort_order, 20);
    assert_eq!(convs2[2].sort_order, 30);
    assert_eq!(
        system2
            .store
            .get_conversation("conv-gamma")
            .unwrap()
            .unwrap()
            .sort_order,
        30
    );

    finalize_shutdown(&system2);
    assert!(!data_dir.join("ps5-agent.pid").exists());
}

