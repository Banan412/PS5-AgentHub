use crate::adapters::{
    sync_plugin_mcp_configs, AppHttpBackend, AppSystem, AppToolBridge,
    RuntimeMemoryProviderImpl, RuntimeToolExecutorImpl,
};
use ps5_agent_config::{default_data_dir, resolve_writable_data_dir, AppConfig};
use ps5_agent_core::{AuditLogEntry, Conversation, ConversationMode, Result, StreamEvent, new_id, now_iso};
use ps5_agent_database::{CURRENT_SCHEMA_VERSION, open_storage};
use ps5_agent_http::{ensure_bootstrap_credentials_with_report, DashboardAssets, HttpState};
use ps5_agent_inference::Ps5LmProvider;
use ps5_agent_logging::{install_panic_hook, AppLogger};
use ps5_agent_manager::{builtin_agents_fingerprint, AgentManager, BUILTIN_AGENTS_SCHEMA_VERSION};
use ps5_agent_memory::MemoryManager;
use ps5_agent_messaging::MessageBus;
use ps5_agent_runtime::AgentRuntime;
use ps5_agent_tools::ToolRegistry;
use sha2::{Digest, Sha256};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

pub const CONFIG_SCHEMA_VERSION: &str = "3";

fn compute_release_stamp() -> (String, Option<(PathBuf, String)>) {
    let mut hasher = Sha256::new();
    hasher.update(env!("CARGO_PKG_VERSION").as_bytes());
    hasher.update(format!("|db:{CURRENT_SCHEMA_VERSION}|agents:{BUILTIN_AGENTS_SCHEMA_VERSION}|").as_bytes());
    hasher.update(builtin_agents_fingerprint().as_bytes());
    hasher.update(ps5_agent_dashboard::INDEX_HTML.as_bytes());
    hasher.update(ps5_agent_dashboard::STYLES_CSS.as_bytes());
    hasher.update(ps5_agent_dashboard::APP_JS.as_bytes());

    let mut exe_info = None;
    if let Ok(exe_path) = std::env::current_exe() {
        if let Ok(meta) = fs::metadata(&exe_path) {
            if meta.is_file() && meta.len() <= 64 * 1024 * 1024 {
                if let Ok(mut file) = fs::File::open(&exe_path) {
                    let mut exe_hasher = Sha256::new();
                    let mut buf = [0u8; 64 * 1024];
                    let mut read_ok = true;
                    loop {
                        match file.read(&mut buf) {
                            Ok(0) => break,
                            Ok(n) => exe_hasher.update(&buf[..n]),
                            Err(_) => {
                                read_ok = false;
                                break;
                            }
                        }
                    }
                    if read_ok {
                        let exe_sha256 = hex::encode(exe_hasher.finalize());
                        hasher.update(b"|exe:");
                        hasher.update(exe_sha256.as_bytes());
                        exe_info = Some((exe_path, exe_sha256));
                    }
                }
            }
        }
    }

    (hex::encode(hasher.finalize()), exe_info)
}

fn verify_or_sync_meta_json(
    exe_info: Option<&(PathBuf, String)>,
    data_dir: &Path,
    logger: &AppLogger,
) {
    let mut candidates: Vec<PathBuf> = Vec::new();
    if let Some((exe_path, _)) = exe_info {
        if let Some(parent) = exe_path.parent() {
            candidates.push(parent.join("meta.json"));
        }
    }
    let data_meta = data_dir.join("meta.json");
    if !candidates.iter().any(|p| p == &data_meta) {
        candidates.push(data_meta);
    }

    let expected_version = format!("v{}", env!("CARGO_PKG_VERSION"));
    let is_ps5_agent_exe = exe_info.is_some_and(|(p, _)| {
        p.file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n == "ps5-agent.elf")
    });

    for meta_path in candidates {
        if !meta_path.is_file() {
            continue;
        }
        let Ok(raw) = fs::read_to_string(&meta_path) else {
            continue;
        };
        let Ok(mut obj) = serde_json::from_str::<serde_json::Value>(&raw) else {
            logger.warn(
                "startup",
                format!("Skipping invalid JSON in {}", meta_path.display()),
            );
            continue;
        };
        let is_ps5_agent_meta = obj.get("id").and_then(|v| v.as_str()) == Some("ps5-agent")
            || obj.get("filename").and_then(|v| v.as_str()) == Some("ps5-agent.elf");
        if !is_ps5_agent_meta {
            continue;
        }

        let meta_ver = obj
            .get("version")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        let meta_cksum = obj
            .get("checksum")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        logger.info(
            "startup",
            format!(
                "Detected meta.json at {} (version={}, checksum={})",
                meta_path.display(),
                meta_ver,
                meta_cksum
            ),
        );

        if is_ps5_agent_exe {
            if let Some((_, exe_sha256)) = exe_info {
                if meta_cksum != *exe_sha256 || meta_ver != expected_version {
                    if let Some(map) = obj.as_object_mut() {
                        map.insert(
                            "checksum".to_string(),
                            serde_json::Value::String(exe_sha256.clone()),
                        );
                        map.insert(
                            "version".to_string(),
                            serde_json::Value::String(expected_version.clone()),
                        );
                        let date_prefix = now_iso();
                        let last_update = date_prefix.get(..10).unwrap_or(&date_prefix);
                        map.insert(
                            "last_update".to_string(),
                            serde_json::Value::String(last_update.to_string()),
                        );
                    }
                    match serde_json::to_string_pretty(&obj) {
                        Ok(updated) => {
                            let tmp_path = meta_path.with_extension("json.tmp");
                            let write_res = fs::OpenOptions::new()
                                .write(true)
                                .create(true)
                                .truncate(true)
                                .open(&tmp_path)
                                .and_then(|mut f| {
                                    use std::io::Write;
                                    f.write_all(format!("{updated}\n").as_bytes())?;
                                    f.sync_all()
                                })
                                .and_then(|()| fs::rename(&tmp_path, &meta_path));
                            if let Err(e) = write_res {
                                let _ = fs::remove_file(&tmp_path);
                                logger.warn(
                                    "startup",
                                    format!(
                                        "Unable to update {} checksum: {e}",
                                        meta_path.display()
                                    ),
                                );
                            } else {
                                logger.info(
                                    "startup",
                                    "Updated meta.json checksum/version to match running ps5-agent.elf",
                                );
                            }
                        }
                        Err(e) => {
                            logger.warn(
                                "startup",
                                format!("Failed serializing meta.json at {}: {e}", meta_path.display()),
                            );
                        }
                    }
                }
            }
        }
    }
}

fn apply_v1_cap_upgrades(cfg: &mut AppConfig) -> bool {
    let mut changed = false;
    if matches!(cfg.agents.max_agents, 5 | 12) {
        cfg.agents.max_agents = 0;
        changed = true;
    }
    if matches!(cfg.agents.max_active_tasks, 4 | 8) {
        cfg.agents.max_active_tasks = 0;
        changed = true;
    }
    if matches!(cfg.agents.max_queued_tasks, 32 | 64) {
        cfg.agents.max_queued_tasks = 0;
        changed = true;
    }
    if matches!(cfg.agents.max_concurrent_inference, 1 | 2) {
        cfg.agents.max_concurrent_inference = 4;
        changed = true;
    }
    if matches!(cfg.agents.max_messages_per_task, 20 | 24) {
        cfg.agents.max_messages_per_task = 0;
        changed = true;
    }
    if cfg.agents.max_reasoning_iterations == 8 {
        cfg.agents.max_reasoning_iterations = 0;
        changed = true;
    }
    if matches!(cfg.memory.max_memories_per_agent, 200 | 500) {
        cfg.memory.max_memories_per_agent = 0;
        changed = true;
    }
    changed
}

fn maybe_upgrade_legacy_v1_config(
    cfg: &mut AppConfig,
    store: &dyn ps5_agent_core::StorageBackend,
    was_preexisting_install: bool,
    logger: &AppLogger,
) -> Result<bool> {
    if store
        .get_setting("installed_config_schema_version")?
        .as_deref()
        == Some(CONFIG_SCHEMA_VERSION)
    {
        return Ok(false);
    }

    if !was_preexisting_install {
        store.set_setting("installed_config_schema_version", CONFIG_SCHEMA_VERSION)?;
        return Ok(false);
    }

    let config_path = cfg.data_dir.join("config.toml");
    let mut upgraded_disk = false;
    if config_path.is_file() {
        if let Ok(raw) = fs::read_to_string(&config_path) {
            if !raw.contains("thinking_level") && !raw.contains("auto_sync_from_ps5lm") {
                let bak_path = config_path.with_extension("toml.v1.bak");
                if !bak_path.exists() {
                    let _ = fs::copy(&config_path, &bak_path);
                }
                let (mut disk_cfg, _) = AppConfig::load_or_create_resilient(&config_path)?;
                let _ = apply_v1_cap_upgrades(&mut disk_cfg);
                disk_cfg.save(&config_path)?;
                let _ = apply_v1_cap_upgrades(cfg);
                upgraded_disk = true;
                logger.info(
                    "startup",
                    "Upgraded legacy v1 config.toml to schema v2 (backup at config.toml.v1.bak)",
                );
            }
        }
    }

    store.set_setting("installed_config_schema_version", CONFIG_SCHEMA_VERSION)?;
    Ok(upgraded_disk)
}

#[derive(Debug, Clone, Default)]
pub struct CliOptions {
    pub config_path: Option<PathBuf>,
    pub data_dir: Option<PathBuf>,
    pub bind: Option<String>,
    pub ps5lm_url: Option<String>,
}

pub fn parse_cli_args() -> CliOptions {
    let mut opts = CliOptions::default();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => opts.config_path = args.next().map(PathBuf::from),
            "--data-dir" => opts.data_dir = args.next().map(PathBuf::from),
            "--bind" => opts.bind = args.next(),
            "--ps5lm-url" => opts.ps5lm_url = args.next(),
            other => {
                if let Some(v) = other.strip_prefix("--config=") {
                    opts.config_path = Some(PathBuf::from(v));
                } else if let Some(v) = other.strip_prefix("--data-dir=") {
                    opts.data_dir = Some(PathBuf::from(v));
                } else if let Some(v) = other.strip_prefix("--bind=") {
                    opts.bind = Some(v.to_string());
                } else if let Some(v) = other.strip_prefix("--ps5lm-url=") {
                    opts.ps5lm_url = Some(v.to_string());
                }
            }
        }
    }
    opts
}

pub fn load_effective_config(opts: &CliOptions) -> Result<AppConfig> {
    load_effective_config_with_warnings(opts).map(|(c, _)| c)
}

pub fn load_effective_config_with_warnings(opts: &CliOptions) -> Result<(AppConfig, Vec<String>)> {
    let data_override = opts.data_dir.as_ref().map(|d| resolve_writable_data_dir(d));
    let config_path = opts.config_path.clone().unwrap_or_else(|| {
        data_override
            .as_ref()
            .map(|d| d.join("config.toml"))
            .unwrap_or_else(|| resolve_writable_data_dir(&default_data_dir()).join("config.toml"))
    });

    let (mut cfg, mut warnings) = AppConfig::load_or_create_resilient(&config_path)?;
    if let Some(dir) = data_override {
        cfg.data_dir = dir.clone();
        cfg.database.path = dir.join("database").join("agent.db");
        cfg.logging.log_dir = dir.join("logs");
        cfg.tools.allowed_paths = vec![dir.join("workspace").to_string_lossy().into_owned()];
    } else {
        let resolved = resolve_writable_data_dir(&cfg.data_dir);
        if resolved != cfg.data_dir {
            cfg.data_dir = resolved.clone();
            cfg.database.path = resolved.join("database").join("agent.db");
            cfg.logging.log_dir = resolved.join("logs");
            cfg.tools.allowed_paths = vec![resolved.join("workspace").to_string_lossy().into_owned()];
        }
    }

    let mut cli_overrode = false;
    if let Some(ref bind) = opts.bind {
        if let Some((host, port_str)) = bind.rsplit_once(':') {
            cfg.server.bind_host = host.trim_matches(['[', ']']).to_string();
            if let Ok(p) = port_str.parse::<u16>() {
                cfg.server.port = p;
            }
            cli_overrode = true;
        }
    }

    if let Some(ref url) = opts.ps5lm_url {
        cfg.inference.base_url = url.clone();
        cli_overrode = true;
    }

    if cli_overrode {
        warnings.extend(cfg.normalize_and_clamp());
    }

    cfg.validate()?;
    Ok((cfg, warnings))
}

#[cfg(target_os = "freebsd")]
fn process_name_matches_ps5_agent(pid: libc::pid_t) -> bool {
    unsafe {
        let mib: [libc::c_int; 4] = [
            libc::CTL_KERN,
            libc::KERN_PROC,
            libc::KERN_PROC_PID,
            pid as libc::c_int,
        ];
        let mut len: libc::size_t = 0;
        if libc::sysctl(
            mib.as_ptr(),
            mib.len() as libc::c_uint,
            std::ptr::null_mut(),
            &mut len,
            std::ptr::null(),
            0,
        ) == 0
            && len > 0
        {
            let mut buf = vec![0u8; len];
            if libc::sysctl(
                mib.as_ptr(),
                mib.len() as libc::c_uint,
                buf.as_mut_ptr() as *mut libc::c_void,
                &mut len,
                std::ptr::null(),
                0,
            ) == 0
                && len > 0
            {
                let data = &buf[..len.min(buf.len())];
                return data
                    .windows(b"ps5-agent.elf".len())
                    .any(|w| w == b"ps5-agent.elf")
                    || data
                        .windows(b"payload.elf".len())
                        .any(|w| w == b"payload.elf");
            }
        }
        false
    }
}

#[cfg(all(unix, not(target_os = "freebsd")))]
fn process_name_matches_ps5_agent(pid: libc::pid_t) -> bool {
    fs::read_to_string(format!("/proc/{pid}/comm"))
        .map(|s| s.contains("ps5-agent.elf") || s.contains("payload.elf"))
        .unwrap_or(false)
}

pub fn manage_pid_file(data_dir: &Path, logger: &AppLogger) {
    let pid_path = data_dir.join("ps5-agent.pid");
    let my_pid = std::process::id();

    if let Ok(raw) = fs::read_to_string(&pid_path) {
        if let Ok(old_pid) = raw.trim().parse::<u32>() {
            if old_pid > 1 && old_pid != my_pid {
                #[cfg(unix)]
                {
                    let pid = old_pid as libc::pid_t;
                    let alive = unsafe { libc::kill(pid, 0) == 0 };
                    if alive {
                        if process_name_matches_ps5_agent(pid) {
                            logger.warn(
                                "startup",
                                format!("Found previous instance (pid={pid}); sending SIGTERM"),
                            );
                            unsafe {
                                libc::kill(pid, libc::SIGTERM);
                            }
                            let mut exited = false;
                            for _ in 0..12 {
                                std::thread::sleep(Duration::from_millis(50));
                                if unsafe { libc::kill(pid, 0) != 0 } {
                                    exited = true;
                                    break;
                                }
                            }
                            if exited {
                                logger.info(
                                    "startup",
                                    format!("Previous instance (pid={pid}) exited after SIGTERM"),
                                );
                            } else {
                                logger.warn(
                                    "startup",
                                    format!(
                                        "Previous instance (pid={pid}) still alive after 600ms; sending SIGKILL"
                                    ),
                                );
                                unsafe {
                                    libc::kill(pid, libc::SIGKILL);
                                }
                            }
                        } else {
                            logger.warn(
                                "startup",
                                format!(
                                    "PID file references unrelated live process pid={pid}; replacing PID file"
                                ),
                            );
                        }
                    }
                }
                #[cfg(not(unix))]
                {
                    logger.info(
                        "startup",
                        format!("Replacing previous PID file entry (old_pid={old_pid})"),
                    );
                }
            }
        }
    }

    if let Err(e) = fs::write(&pid_path, format!("{my_pid}\n")) {
        logger.warn(
            "startup",
            format!("Failed to write PID file {}: {e}", pid_path.display()),
        );
    }
}

fn ensure_directories(cfg: &AppConfig) -> Result<()> {
    let skills_dir = cfg.data_dir.join("skills");
    let plugins_dir = cfg.data_dir.join("plugins");
    let mcp_dir = cfg.data_dir.join("mcp");
    let tools_dir = cfg.data_dir.join("tools");
    let backups_dir = cfg.data_dir.join("backups");
    let dirs = [
        cfg.data_dir.as_path(),
        Path::new(&cfg.logging.log_dir),
        cfg.database.path.parent().unwrap_or(&cfg.data_dir),
        backups_dir.as_path(),
        skills_dir.as_path(),
        plugins_dir.as_path(),
        mcp_dir.as_path(),
        tools_dir.as_path(),
    ];
    for d in dirs {
        fs::create_dir_all(d).map_err(|e| {
            ps5_agent_core::AgentError::Storage(format!(
                "creating directory '{}': {e}",
                d.display()
            ))
        })?;
    }
    for allowed in &cfg.tools.allowed_paths {
        let _ = fs::create_dir_all(allowed);
    }
    Ok(())
}

pub async fn initialize(cfg: AppConfig) -> Result<Arc<AppSystem>> {
    initialize_with_warnings(cfg, Vec::new()).await
}

pub async fn initialize_with_warnings(
    mut cfg: AppConfig,
    warnings: Vec<String>,
) -> Result<Arc<AppSystem>> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    ensure_directories(&cfg)?;

    let logger = AppLogger::init(&cfg.logging);
    install_panic_hook(logger.clone());
    for w in &warnings {
        logger.warn("config", w);
    }
    #[cfg(target_vendor = "sie")]
    if cfg.logging.enable_toast && warnings.iter().any(|w| w.contains(".broken.bak")) {
        let _ = ps5::Toast::new("PS5 Agent Config Recovered")
            .sub("Invalid config.toml backed up to .broken.bak; defaults restored")
            .send();
    }

    manage_pid_file(&cfg.data_dir, &logger);

    let brain_workspace_root = cfg
        .tools
        .allowed_paths
        .first()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| cfg.data_dir.join("workspace"));
    match ps5_agent_runtime::brain::seed_workspace_brain_files(&brain_workspace_root) {
        Ok(created) if !created.is_empty() => {
            logger.info(
                "brain",
                format!(
                    "seeded {} workspace brain file(s) in {}",
                    created.len(),
                    brain_workspace_root.display()
                ),
            );
        }
        Ok(_) => {}
        Err(e) => {
            logger.warn(
                "brain",
                format!(
                    "failed seeding workspace brain files in {}: {e}",
                    brain_workspace_root.display()
                ),
            );
        }
    }

    logger.info(
        "startup",
        format!(
            "Starting PS5 Agent v{} (data_dir={}, inference={})",
            env!("CARGO_PKG_VERSION"),
            cfg.data_dir.display(),
            cfg.inference.base_url
        ),
    );

    let (store, db_metrics) = open_storage(&cfg.database, &cfg.data_dir)?;
    store.run_migrations()?;
    let integrity = store.integrity_check()?;
    logger.info(
        "startup",
        format!(
            "Storage initialized (driver={}, integrity={})",
            store.driver_name(),
            integrity
        ),
    );

    let was_preexisting_install = store
        .get_setting("agents_seeded")?
        .as_deref()
        == Some("true")
        || !store.list_agents()?.is_empty();
    let prev_release_stamp = store.get_setting("installed_release_stamp")?;
    let (current_release_stamp, exe_info) = compute_release_stamp();
    verify_or_sync_meta_json(exe_info.as_ref(), &cfg.data_dir, &logger);
    let config_upgraded = maybe_upgrade_legacy_v1_config(
        &mut cfg,
        store.as_ref(),
        was_preexisting_install,
        &logger,
    )?;
    let is_elf_upgrade = (was_preexisting_install && prev_release_stamp.is_none())
        || matches!(
            prev_release_stamp.as_deref(),
            Some(prev) if prev != current_release_stamp.as_str()
        );

    let (events_tx, _) = broadcast::channel::<StreamEvent>(512);
    let message_bus = Arc::new(MessageBus::new(
        store.clone(),
        (&cfg.agents).into(),
        events_tx.clone(),
    ));
    let agent_manager = Arc::new(AgentManager::new(
        store.clone(),
        cfg.agents.clone(),
        events_tx.clone(),
    ));
    let seeded_agents = agent_manager.seed_defaults()?;
    logger.info(
        "startup",
        format!("Agents ready (seeded_defaults={seeded_agents})"),
    );

    let memory = Arc::new(MemoryManager::new(store.clone(), cfg.memory.clone()));
    let memory_provider = Arc::new(RuntimeMemoryProviderImpl::new(
        memory.clone(),
        store.clone(),
        cfg.memory.clone(),
    ));

    let tools = Arc::new(ToolRegistry::with_data_dir(
        store.clone(),
        cfg.tools.clone(),
        &cfg.data_dir,
    )?);
    sync_plugin_mcp_configs(&tools, &logger);
    logger.info(
        "startup",
        format!(
            "Extensions loaded (skills={}, plugins={}, mcp_servers={})",
            tools.skills().list_skills().len(),
            tools.plugins().list_plugins().len(),
            tools.mcp().list_views().len()
        ),
    );

    store.set_setting("installed_release_stamp", &current_release_stamp)?;
    store.set_setting("installed_version", env!("CARGO_PKG_VERSION"))?;
    if is_elf_upgrade {
        store.set_setting("last_upgraded_at", &now_iso())?;
        let _ = store.append_audit_log(&AuditLogEntry {
            id: new_id(),
            timestamp: now_iso(),
            actor: "system".to_string(),
            action: "system.elf_upgrade".to_string(),
            target: "ps5-agent.elf".to_string(),
            severity: "info".to_string(),
            outcome: "ok".to_string(),
            details_json: serde_json::json!({
                "version": env!("CARGO_PKG_VERSION"),
                "release_stamp": current_release_stamp,
                "previous_stamp": prev_release_stamp,
                "config_upgraded": config_upgraded,
            })
            .to_string(),
        });
        logger.info(
            "startup",
            "Detected updated ELF/release -> verified and updated DB schema, built-in agents, tool specs, extensions, and config",
        );
    }
    let tool_bridge = Arc::new(AppToolBridge::new(
        agent_manager.clone(),
        message_bus.clone(),
        memory.clone(),
    ));
    let tool_executor = Arc::new(RuntimeToolExecutorImpl::new(
        tools.clone(),
        tool_bridge.clone(),
    ));

    let inference = Arc::new(Ps5LmProvider::with_concurrency(
        cfg.inference.clone(),
        cfg.agents.max_concurrent_inference,
    )?);

    let generated_pw = ensure_bootstrap_credentials_with_report(&cfg, store.as_ref(), &logger)?;

    if store.list_conversations(true)?.is_empty() {
        let now = now_iso();
        let default_conv = Conversation {
            id: new_id(),
            title: "Commander Console".to_string(),
            mode: ConversationMode::Direct {
                agent_id: "commander".to_string(),
            },
            created_at: now.clone(),
            updated_at: now,
            archived: false,
            sort_order: 0,
        };
        store.create_conversation(&default_conv)?;
        logger.info("startup", "Seeded default 'Commander Console' conversation");
    }

    match store.prune_old_records(
        cfg.database.retention_days,
        cfg.database.max_rows_per_table,
    ) {
        Ok(report) => {
            let total_removed = report.deleted_tasks
                + report.deleted_task_events
                + report.deleted_agent_messages
                + report.deleted_tool_executions
                + report.deleted_audit_logs
                + report.deleted_chat_messages
                + report.purged_sessions;
            if total_removed > 0 {
                logger.info(
                    "maintenance",
                    format!(
                        "Startup retention prune removed {} records (tasks={}, events={}, agent_msgs={}, tools={}, audits={}, chat={}, sessions={})",
                        total_removed,
                        report.deleted_tasks,
                        report.deleted_task_events,
                        report.deleted_agent_messages,
                        report.deleted_tool_executions,
                        report.deleted_audit_logs,
                        report.deleted_chat_messages,
                        report.purged_sessions,
                    ),
                );
            }
        }
        Err(e) => {
            logger.warn("maintenance", format!("Startup retention prune skipped: {e}"));
        }
    }

    let runtime = AgentRuntime::new(
        store.clone(),
        cfg.clone(),
        inference.clone(),
        tool_executor,
        Some(memory_provider.clone()),
        agent_manager.clone(),
        message_bus.clone(),
        events_tx.clone(),
    );
    tool_bridge.attach_runtime(&runtime);
    tool_bridge.attach_events_tx(events_tx.clone());
    tool_bridge.attach_browser_manager(tools.browser_manager().clone());

    let recovery_report = runtime.recover_on_startup().await?;
    logger.info(
        "startup",
        format!(
            "Task recovery: resumed={}, paused_side_effects={}, already_waiting={}",
            recovery_report.resumed_tasks.len(),
            recovery_report.paused_side_effect_tasks.len(),
            recovery_report.already_waiting_tasks.len()
        ),
    );

    let shutdown = CancellationToken::new();

    let http_backend = Arc::new(AppHttpBackend {
        started_at: Instant::now(),
        store: store.clone(),
        db_metrics: db_metrics.clone(),
        inference: inference.clone(),
        tools: tools.clone(),
        tool_bridge: tool_bridge.clone(),
        agent_manager: agent_manager.clone(),
        message_bus: message_bus.clone(),
        memory_provider: memory_provider.clone(),
        runtime: runtime.clone(),
        events_tx: events_tx.clone(),
        logger: logger.clone(),
        ps5lm_health: RwLock::new((false, None)),
        ps5lm_capabilities: RwLock::new(None),
        auto_synced_slots: AtomicUsize::new(0),
    });
    if cfg.inference.auto_sync_from_ps5lm {
        let _ = http_backend.sync_ps5lm_capabilities().await;
    } else {
        let _ = http_backend.refresh_ps5lm_health().await;
    }

    // Background PS5LM health & capability auto-sync polling loop
    {
        let poll_backend = http_backend.clone();
        let poll_shutdown = shutdown.clone();
        let interval_secs = cfg.runtime.health_poll_interval_secs.max(5);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            ticker.tick().await;
            let mut last_caps_sync = Instant::now();
            loop {
                tokio::select! {
                    _ = poll_shutdown.cancelled() => break,
                    _ = ticker.tick() => {
                        let cur_cfg = poll_backend.runtime.config();
                        let sync_interval = cur_cfg.inference.auto_sync_interval_secs;
                        if cur_cfg.inference.auto_sync_from_ps5lm
                            && sync_interval > 0
                            && last_caps_sync.elapsed() + Duration::from_millis(250)
                                >= Duration::from_secs(sync_interval)
                        {
                            let _ = poll_backend.sync_ps5lm_capabilities().await;
                            last_caps_sync = Instant::now();
                        } else {
                            let _ = poll_backend.refresh_ps5lm_health().await;
                        }
                    }
                }
            }
        });
    }

    // Periodic database retention, session purge & WAL checkpoint loop (every 600s)
    {
        let maint_store = store.clone();
        let maint_runtime = runtime.clone();
        let maint_logger = logger.clone();
        let maint_shutdown = shutdown.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(600));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = maint_shutdown.cancelled() => break,
                    _ = ticker.tick() => {
                        let cur_cfg = maint_runtime.config();
                        let store_for_maint = maint_store.clone();
                        let retention_days = cur_cfg.database.retention_days;
                        let max_rows = cur_cfg.database.max_rows_per_table;
                        let now = now_iso();
                        match tokio::task::spawn_blocking(move || {
                            let prune_res = store_for_maint.prune_old_records(
                                retention_days,
                                max_rows,
                            );
                            let purge_res = store_for_maint.purge_expired_sessions(&now);
                            let ckpt_res = store_for_maint.checkpoint();
                            (prune_res, purge_res, ckpt_res)
                        })
                        .await
                        {
                            Ok((prune_res, purge_res, ckpt_res)) => {
                                if let Err(e) = prune_res {
                                    maint_logger.warn("maintenance", format!("periodic prune failed: {e}"));
                                }
                                if let Err(e) = purge_res {
                                    maint_logger.warn("maintenance", format!("periodic session purge failed: {e}"));
                                }
                                if let Err(e) = ckpt_res {
                                    maint_logger.warn("maintenance", format!("periodic checkpoint failed: {e}"));
                                }
                            }
                            Err(e) => {
                                maint_logger.warn(
                                    "maintenance",
                                    format!("periodic maintenance task panicked: {e}"),
                                );
                            }
                        }
                    }
                }
            }
        });
    }

    // Autonomous background scheduler loop (every 5s; runs independently of UI/SSE clients)
    {
        let sched_runtime = Arc::clone(&runtime);
        let sched_logger = logger.clone();
        let sched_shutdown = shutdown.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    biased;
                    _ = sched_shutdown.cancelled() => break,
                    _ = interval.tick() => {
                        if let Err(e) = sched_runtime.tick_due_schedules(sched_shutdown.clone()).await {
                            sched_logger.warn("scheduler", format!("scheduled task tick failed: {e}"));
                        }
                        if let Err(e) = sched_runtime.tick_heartbeat_if_due().await {
                            sched_logger.warn("scheduler", format!("heartbeat tick failed: {e}"));
                        }
                    }
                }
            }
        });
    }

    let assets = DashboardAssets {
        index_html: ps5_agent_dashboard::INDEX_HTML,
        styles_css: ps5_agent_dashboard::STYLES_CSS,
        app_js: ps5_agent_dashboard::APP_JS,
    };

    let http_state = HttpState::new(
        cfg.clone(),
        store.clone(),
        http_backend.clone(),
        logger.clone(),
        events_tx.clone(),
        shutdown.clone(),
        assets,
    );

    #[cfg(target_vendor = "sie")]
    if cfg.logging.enable_toast {
        if let Some(ref pw) = generated_pw {
            let _ = ps5::notify::simple(&format!(
                "PS5 Agent :{}\nUser: {} | Pass: {}",
                cfg.server.port, cfg.security.admin_username, pw
            ));
        } else if is_elf_upgrade {
            let _ = ps5::notify::simple(&format!(
                "PS5 Agent updated & ready on {}:{}",
                cfg.server.bind_host, cfg.server.port
            ));
        } else {
            let _ = ps5::notify::simple(&format!(
                "PS5 Agent ready on {}:{}",
                cfg.server.bind_host, cfg.server.port
            ));
        }
    }
    #[cfg(not(target_vendor = "sie"))]
    {
        let _ = &generated_pw;
        let _ = is_elf_upgrade;
    }

    Ok(Arc::new(AppSystem {
        config: RwLock::new(cfg),
        store,
        db_metrics,
        inference,
        tools,
        memory,
        memory_provider,
        tool_bridge,
        agent_manager,
        message_bus,
        runtime,
        events_tx,
        logger,
        shutdown,
        recovery_report,
        http_backend,
        http_state,
    }))
}
