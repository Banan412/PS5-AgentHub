use chrono::{Duration, Utc};
use ps5_agent_config::AppConfig;
use ps5_agent_core::{
    agent_allows_tool_with_policy, now_iso, AgentError, DoctorCheckResult, DoctorFixReport,
    DoctorReport, DoctorStatus, InferenceProvider, Result, SandboxApprovalPolicy, SandboxMode,
    StorageBackend,
};
use ps5_agent_manager::default_agent_definitions;
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

const CORE_BUILTIN_AGENT_IDS: [&str; 5] = [
    "commander",
    "researcher",
    "programmer",
    "analyst",
    "executor",
];

const SCHEDULE_TOOL_IDS: [&str; 4] = [
    "schedule_create",
    "schedule_list",
    "schedule_update",
    "schedule_delete",
];

const REQUIRED_SANDBOX_TOOLS: [&str; 10] = [
    "web_search",
    "web_fetch",
    "browser",
    "edit_file",
    "apply_patch",
    "grep_search",
    "glob_files",
    "sandbox_exec",
    "doctor",
    "write_artifact",
];

const REQUIRED_BRAIN_FILES: [&str; 8] = [
    "AGENTS.md",
    "SOUL.md",
    "IDENTITY.md",
    "USER.md",
    "TOOLS.md",
    "MEMORY.md",
    "HEARTBEAT.md",
    ".agents/rules/ps5-sandbox.md",
];

const OVERDUE_GRACE_SECS: i64 = 60;

/// Runs all 12 diagnostic checks for the PS5 Agent runtime, workspace, security,
/// agents, scheduler, memory, platform compatibility, and release artifacts.
pub async fn run_doctor_checks(
    cfg: &AppConfig,
    storage: &dyn StorageBackend,
    provider: Option<&dyn InferenceProvider>,
    builtin_tool_ids: &[String],
) -> Result<DoctorReport> {
    let checks = vec![
        check_ps5lm_endpoint(cfg, provider).await,
        check_storage_integrity(cfg, storage),
        check_workspace_sandbox(cfg),
        check_brain_files(cfg),
        check_sandbox_network_policy(cfg),
        check_auth_csrf_posture(cfg),
        check_builtin_agent_tools(storage, builtin_tool_ids),
        check_extension_directories(cfg),
        check_scheduler_engine(storage),
        check_memory_bounds(cfg),
        check_ps5_platform_compat(cfg),
        check_meta_release_sync_at(Path::new(".")),
    ];

    let mut pass_count = 0usize;
    let mut warn_count = 0usize;
    let mut fail_count = 0usize;
    for check in &checks {
        match check.status {
            DoctorStatus::Pass => pass_count += 1,
            DoctorStatus::Warn => warn_count += 1,
            DoctorStatus::Fail => fail_count += 1,
        }
    }

    let overall_status = if fail_count > 0 {
        DoctorStatus::Fail
    } else if warn_count > 0 {
        DoctorStatus::Warn
    } else {
        DoctorStatus::Pass
    };

    Ok(DoctorReport {
        overall_status,
        checked_at: now_iso(),
        pass_count,
        warn_count,
        fail_count,
        checks,
    })
}

/// Applies safe, idempotent fixes for fixable diagnostics: creates workspace/sandbox/
/// artifact/extension directories, seeds missing brain files non-destructively,
/// restores missing built-in agents, and re-runs `run_doctor_checks`.
pub async fn run_doctor_fixes(
    cfg: &AppConfig,
    storage: &dyn StorageBackend,
    provider: Option<&dyn InferenceProvider>,
    builtin_tool_ids: &[String],
) -> Result<DoctorFixReport> {
    let mut applied_fixes = Vec::new();
    let workspace_root = primary_workspace_root(cfg);

    let dirs_to_ensure = [
        workspace_root.clone(),
        workspace_root.join("sandbox").join("downloads"),
        workspace_root.join("sandbox").join("snapshots"),
        workspace_root.join("sandbox").join("tmp"),
        workspace_root.join("artifacts"),
        cfg.data_dir.join("skills"),
        cfg.data_dir.join("plugins"),
        cfg.data_dir.join("mcp"),
    ];

    for dir in &dirs_to_ensure {
        if let Some(created) = ensure_real_dir(dir)? {
            applied_fixes.push(format!("created directory: {created}"));
        }
    }

    let seeded = crate::brain::seed_workspace_brain_files(&workspace_root)?;
    for rel in seeded {
        applied_fixes.push(format!("seeded brain file: {rel}"));
    }

    let existing_agents: HashSet<String> = storage
        .list_agents()?
        .into_iter()
        .map(|a| a.id)
        .collect();
    for def in default_agent_definitions() {
        if CORE_BUILTIN_AGENT_IDS.contains(&def.id.as_str()) && !existing_agents.contains(&def.id) {
            storage.upsert_agent(&def)?;
            applied_fixes.push(format!("restored built-in agent: {}", def.id));
        }
    }

    let report = run_doctor_checks(cfg, storage, provider, builtin_tool_ids).await?;
    Ok(DoctorFixReport {
        applied_fixes,
        report,
    })
}

fn primary_workspace_root(cfg: &AppConfig) -> PathBuf {
    cfg.tools
        .allowed_paths
        .first()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| cfg.data_dir.join("workspace"))
}

fn ensure_real_dir(path: &Path) -> Result<Option<String>> {
    match fs::symlink_metadata(path) {
        Ok(meta) => {
            if meta.file_type().is_symlink() {
                return Err(AgentError::PermissionDenied(format!(
                    "refusing to traverse symlink directory: {}",
                    path.display()
                )));
            }
            if !meta.is_dir() {
                return Err(AgentError::InvalidInput(format!(
                    "expected directory, found file at: {}",
                    path.display()
                )));
            }
            Ok(None)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir_all(path).map_err(|err| {
                AgentError::Storage(format!("failed creating {}: {err}", path.display()))
            })?;
            Ok(Some(path.display().to_string()))
        }
        Err(e) => Err(AgentError::Storage(format!(
            "failed inspecting {}: {e}",
            path.display()
        ))),
    }
}

fn inspect_dir_state(path: &Path) -> DirState {
    match fs::symlink_metadata(path) {
        Ok(meta) => {
            if meta.file_type().is_symlink() {
                DirState::Symlink
            } else if meta.is_dir() {
                DirState::Ok
            } else {
                DirState::NotDir
            }
        }
        Err(_) => DirState::Missing,
    }
}

enum DirState {
    Ok,
    Missing,
    Symlink,
    NotDir,
}

fn has_valid_http_url(raw: &str) -> bool {
    let trimmed = raw.trim();
    let rest = if let Some(r) = trimmed.strip_prefix("http://") {
        r
    } else if let Some(r) = trimmed.strip_prefix("https://") {
        r
    } else {
        return false;
    };
    !rest.is_empty() && !rest.starts_with('/') && !rest.contains(char::is_whitespace)
}

async fn check_ps5lm_endpoint(
    cfg: &AppConfig,
    provider: Option<&dyn InferenceProvider>,
) -> DoctorCheckResult {
    let base_url = cfg.inference.base_url.trim();
    if !has_valid_http_url(base_url) {
        return DoctorCheckResult {
            id: "ps5lm_endpoint".into(),
            category: "inference".into(),
            title: "PS5LM Inference Endpoint".into(),
            status: DoctorStatus::Fail,
            summary: format!("Invalid inference.base_url: '{base_url}'."),
            remediation: Some(
                "Set [inference] base_url to a valid http:// or https:// PS5LM endpoint (e.g. http://127.0.0.1:8081/v1)."
                    .into(),
            ),
            fixable: false,
        };
    }

    let Some(p) = provider else {
        return DoctorCheckResult {
            id: "ps5lm_endpoint".into(),
            category: "inference".into(),
            title: "PS5LM Inference Endpoint".into(),
            status: DoctorStatus::Pass,
            summary: format!(
                "PS5LM endpoint URL format valid ({base_url}); live health probe skipped (no provider supplied)."
            ),
            remediation: None,
            fixable: false,
        };
    };

    match tokio::time::timeout(std::time::Duration::from_secs(3), p.health_check()).await {
        Ok(health) if health.reachable => {
            let model_note = health
                .loaded_model
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|m| format!("loaded model '{m}'"))
                .unwrap_or_else(|| {
                    format!("{} available model(s)", health.available_models.len())
                });
            DoctorCheckResult {
                id: "ps5lm_endpoint".into(),
                category: "inference".into(),
                title: "PS5LM Inference Endpoint".into(),
                status: DoctorStatus::Pass,
                summary: format!("PS5LM reachable at {base_url} ({model_note})."),
                remediation: None,
                fixable: false,
            }
        }
        Ok(health) => DoctorCheckResult {
            id: "ps5lm_endpoint".into(),
            category: "inference".into(),
            title: "PS5LM Inference Endpoint".into(),
            status: DoctorStatus::Warn,
            summary: format!(
                "PS5LM unreachable at {base_url}: {}",
                health.error.unwrap_or_else(|| "endpoint offline".into())
            ),
            remediation: Some(
                "Start PS5LM on the configured host/port and verify inference.base_url.".into(),
            ),
            fixable: false,
        },
        Err(_) => DoctorCheckResult {
            id: "ps5lm_endpoint".into(),
            category: "inference".into(),
            title: "PS5LM Inference Endpoint".into(),
            status: DoctorStatus::Warn,
            summary: format!("PS5LM health probe timed out after 3s for {base_url}."),
            remediation: Some(
                "Verify PS5LM server responsiveness and network connectivity.".into(),
            ),
            fixable: false,
        },
    }
}

fn check_storage_integrity(cfg: &AppConfig, storage: &dyn StorageBackend) -> DoctorCheckResult {
    let driver = storage.driver_name();
    let integrity = match storage.integrity_check() {
        Ok(s) => s,
        Err(e) => {
            return DoctorCheckResult {
                id: "storage_integrity".into(),
                category: "storage".into(),
                title: "Storage Integrity".into(),
                status: DoctorStatus::Fail,
                summary: format!("Storage '{driver}' integrity check failed: {e}"),
                remediation: Some(
                    "Inspect database file permissions/corruption or switch database.driver to 'auto'/'jsonl'."
                        .into(),
                ),
                fixable: false,
            };
        }
    };
    if integrity != "ok" {
        return DoctorCheckResult {
            id: "storage_integrity".into(),
            category: "storage".into(),
            title: "Storage Integrity".into(),
            status: DoctorStatus::Fail,
            summary: format!("Storage '{driver}' reported non-ok integrity: '{integrity}'."),
            remediation: Some("Restore database backup or run recovery.".into()),
            fixable: false,
        };
    }

    let agent_count = match storage.list_agents() {
        Ok(v) => v.len(),
        Err(e) => {
            return DoctorCheckResult {
                id: "storage_integrity".into(),
                category: "storage".into(),
                title: "Storage Integrity".into(),
                status: DoctorStatus::Fail,
                summary: format!("Storage '{driver}' failed listing agents: {e}"),
                remediation: Some("Run database migrations cleanly before startup.".into()),
                fixable: false,
            };
        }
    };
    let conv_count = match storage.list_conversations(true) {
        Ok(v) => v.len(),
        Err(e) => {
            return DoctorCheckResult {
                id: "storage_integrity".into(),
                category: "storage".into(),
                title: "Storage Integrity".into(),
                status: DoctorStatus::Fail,
                summary: format!("Storage '{driver}' failed listing conversations: {e}"),
                remediation: Some("Run database migrations cleanly before startup.".into()),
                fixable: false,
            };
        }
    };

    if driver == "sqlite" && cfg.database.wal_mode && cfg!(target_vendor = "sie") {
        return DoctorCheckResult {
            id: "storage_integrity".into(),
            category: "storage".into(),
            title: "Storage Integrity".into(),
            status: DoctorStatus::Warn,
            summary: format!(
                "SQLite integrity ok ({agent_count} agents, {conv_count} conversations), but database.wal_mode = true on PS5."
            ),
            remediation: Some(
                "Set database.wal_mode = false on PS5 so TRUNCATE journal mode avoids mmap(-shm)."
                    .into(),
            ),
            fixable: false,
        };
    }

    DoctorCheckResult {
        id: "storage_integrity".into(),
        category: "storage".into(),
        title: "Storage Integrity".into(),
        status: DoctorStatus::Pass,
        summary: format!(
            "Storage '{driver}' integrity ok ({agent_count} agents, {conv_count} conversations, wal_mode={}).",
            cfg.database.wal_mode
        ),
        remediation: None,
        fixable: false,
    }
}

fn check_workspace_sandbox(cfg: &AppConfig) -> DoctorCheckResult {
    if cfg.tools.allowed_paths.is_empty() {
        return DoctorCheckResult {
            id: "workspace_sandbox".into(),
            category: "sandbox".into(),
            title: "Workspace & Sandbox Directories".into(),
            status: DoctorStatus::Fail,
            summary: "tools.allowed_paths is empty; no primary workspace root is configured.".into(),
            remediation: Some("Add at least one workspace directory to tools.allowed_paths.".into()),
            fixable: false,
        };
    }

    let root = primary_workspace_root(cfg);
    let required_dirs = [
        ("workspace", root.clone()),
        ("sandbox/downloads", root.join("sandbox").join("downloads")),
        ("sandbox/snapshots", root.join("sandbox").join("snapshots")),
        ("sandbox/tmp", root.join("sandbox").join("tmp")),
        ("artifacts", root.join("artifacts")),
    ];

    let mut missing = Vec::new();
    for (label, path) in &required_dirs {
        match inspect_dir_state(path) {
            DirState::Ok => {}
            DirState::Missing => missing.push(*label),
            DirState::Symlink => {
                return DoctorCheckResult {
                    id: "workspace_sandbox".into(),
                    category: "sandbox".into(),
                    title: "Workspace & Sandbox Directories".into(),
                    status: DoctorStatus::Fail,
                    summary: format!(
                        "Refusing symlink directory for '{label}' at {}.",
                        path.display()
                    ),
                    remediation: Some("Replace the symlink with a regular directory.".into()),
                    fixable: false,
                };
            }
            DirState::NotDir => {
                return DoctorCheckResult {
                    id: "workspace_sandbox".into(),
                    category: "sandbox".into(),
                    title: "Workspace & Sandbox Directories".into(),
                    status: DoctorStatus::Fail,
                    summary: format!(
                        "Expected directory for '{label}' at {}, found regular file.",
                        path.display()
                    ),
                    remediation: Some("Remove the file conflict and re-run /doctor fix.".into()),
                    fixable: false,
                };
            }
        }
    }

    if !missing.is_empty() {
        let root_missing = missing.contains(&"workspace");
        return DoctorCheckResult {
            id: "workspace_sandbox".into(),
            category: "sandbox".into(),
            title: "Workspace & Sandbox Directories".into(),
            status: if root_missing {
                DoctorStatus::Fail
            } else {
                DoctorStatus::Warn
            },
            summary: format!("Missing workspace/sandbox directory(ies): {}.", missing.join(", ")),
            remediation: Some("Run /doctor fix to create missing sandbox directories.".into()),
            fixable: true,
        };
    }

    let probe_path = root.join(format!(".ps5_doctor_probe_{}", std::process::id()));
    match fs::write(&probe_path, b"ok") {
        Ok(()) => {
            let _ = fs::remove_file(&probe_path);
        }
        Err(e) => {
            return DoctorCheckResult {
                id: "workspace_sandbox".into(),
                category: "sandbox".into(),
                title: "Workspace & Sandbox Directories".into(),
                status: DoctorStatus::Fail,
                summary: format!("Workspace {} is not writable: {e}", root.display()),
                remediation: Some("Grant write permissions on the primary workspace directory.".into()),
                fixable: false,
            };
        }
    }

    DoctorCheckResult {
        id: "workspace_sandbox".into(),
        category: "sandbox".into(),
        title: "Workspace & Sandbox Directories".into(),
        status: DoctorStatus::Pass,
        summary: format!(
            "Primary workspace and sandbox subdirs present and writable at {}.",
            root.display()
        ),
        remediation: None,
        fixable: false,
    }
}

fn check_brain_files(cfg: &AppConfig) -> DoctorCheckResult {
    let root = primary_workspace_root(cfg);
    let mut missing = Vec::new();
    for rel in REQUIRED_BRAIN_FILES {
        let path = root.join(rel);
        match fs::symlink_metadata(&path) {
            Ok(meta) => {
                if meta.file_type().is_symlink() || !meta.is_file() {
                    return DoctorCheckResult {
                        id: "brain_files".into(),
                        category: "workspace".into(),
                        title: "Workspace Brain Files".into(),
                        status: DoctorStatus::Fail,
                        summary: format!(
                            "Brain file '{rel}' at {} is a symlink or non-regular file.",
                            path.display()
                        ),
                        remediation: Some(
                            "Replace with a regular markdown file and re-run /doctor fix.".into(),
                        ),
                        fixable: false,
                    };
                }
            }
            Err(_) => missing.push(rel),
        }
    }

    if !missing.is_empty() {
        return DoctorCheckResult {
            id: "brain_files".into(),
            category: "workspace".into(),
            title: "Workspace Brain Files".into(),
            status: DoctorStatus::Warn,
            summary: format!("Missing brain file(s): {}.", missing.join(", ")),
            remediation: Some(
                "Run /doctor fix to seed default brain files non-destructively.".into(),
            ),
            fixable: true,
        };
    }

    DoctorCheckResult {
        id: "brain_files".into(),
        category: "workspace".into(),
        title: "Workspace Brain Files".into(),
        status: DoctorStatus::Pass,
        summary: format!(
            "All {} required brain/rule files present in {}.",
            REQUIRED_BRAIN_FILES.len(),
            root.display()
        ),
        remediation: None,
        fixable: false,
    }
}

fn is_cloud_metadata_host(entry: &str) -> bool {
    let norm = entry.trim().trim_end_matches('.').to_ascii_lowercase();
    matches!(
        norm.as_str(),
        "169.254.169.254" | "metadata.google.internal" | "100.100.100.200" | "fd00:ec2::254"
    )
}

fn check_sandbox_network_policy(cfg: &AppConfig) -> DoctorCheckResult {
    let Some(mode) = SandboxMode::parse(&cfg.sandbox.mode) else {
        return DoctorCheckResult {
            id: "sandbox_network_policy".into(),
            category: "security".into(),
            title: "Sandbox & Network SSRF Policy".into(),
            status: DoctorStatus::Fail,
            summary: format!("Invalid sandbox.mode '{}'.", cfg.sandbox.mode),
            remediation: Some(
                "Set sandbox.mode to 'read_only', 'workspace_write', or 'full_sandbox'.".into(),
            ),
            fixable: false,
        };
    };

    let Some(policy) = SandboxApprovalPolicy::parse(&cfg.sandbox.approval_policy) else {
        return DoctorCheckResult {
            id: "sandbox_network_policy".into(),
            category: "security".into(),
            title: "Sandbox & Network SSRF Policy".into(),
            status: DoctorStatus::Fail,
            summary: format!(
                "Invalid sandbox.approval_policy '{}'.",
                cfg.sandbox.approval_policy
            ),
            remediation: Some(
                "Set sandbox.approval_policy to 'full_auto', 'on_request', 'always_ask', or 'never'."
                    .into(),
            ),
            fixable: false,
        };
    };

    if cfg.tools.allowed_domains.iter().any(|d| is_cloud_metadata_host(d)) {
        return DoctorCheckResult {
            id: "sandbox_network_policy".into(),
            category: "security".into(),
            title: "Sandbox & Network SSRF Policy".into(),
            status: DoctorStatus::Fail,
            summary: "tools.allowed_domains explicitly lists a cloud metadata endpoint (e.g. 169.254.169.254).".into(),
            remediation: Some(
                "Remove all cloud metadata endpoints from tools.allowed_domains immediately.".into(),
            ),
            fixable: false,
        };
    }

    if cfg.sandbox.network_access && cfg.tools.allowed_domains.is_empty() {
        return DoctorCheckResult {
            id: "sandbox_network_policy".into(),
            category: "security".into(),
            title: "Sandbox & Network SSRF Policy".into(),
            status: DoctorStatus::Warn,
            summary: "tools.allowed_domains is empty while sandbox.network_access is true.".into(),
            remediation: Some(
                "Configure tools.allowed_domains (e.g. [\"*\"]) or disable sandbox.network_access."
                    .into(),
            ),
            fixable: false,
        };
    }

    if cfg.sandbox.allow_local_network || !cfg.tools.block_private_lan_web_requests {
        return DoctorCheckResult {
            id: "sandbox_network_policy".into(),
            category: "security".into(),
            title: "Sandbox & Network SSRF Policy".into(),
            status: DoctorStatus::Warn,
            summary: format!(
                "Sandbox mode={}, approval={}; local LAN requests enabled (cloud metadata 169.254.169.254 remains unconditionally blocked).",
                mode.as_str(),
                policy.as_str()
            ),
            remediation: Some(
                "Keep sandbox.allow_local_network = false unless local network tools require LAN access."
                    .into(),
            ),
            fixable: false,
        };
    }

    DoctorCheckResult {
        id: "sandbox_network_policy".into(),
        category: "security".into(),
        title: "Sandbox & Network SSRF Policy".into(),
        status: DoctorStatus::Pass,
        summary: format!(
            "Sandbox mode={}, approval={}, network_access={}, private LAN & metadata SSRF protection active (169.254.169.254 blocked).",
            mode.as_str(),
            policy.as_str(),
            cfg.sandbox.network_access
        ),
        remediation: None,
        fixable: false,
    }
}

fn is_weak_secret(raw: &str) -> bool {
    let lower = raw.trim().to_ascii_lowercase();
    matches!(
        lower.as_str(),
        "" | "admin" | "password" | "changeme" | "ps5-agent" | "123456" | "12345678" | "root"
    )
}

fn check_auth_csrf_posture(cfg: &AppConfig) -> DoctorCheckResult {
    let bind = cfg.server.bind_host.trim();
    let is_loopback = matches!(bind, "127.0.0.1" | "::1" | "localhost" | "[::1]");

    if !cfg.security.auth_enabled {
        if !is_loopback {
            return DoctorCheckResult {
                id: "auth_csrf_posture".into(),
                category: "security".into(),
                title: "Authentication & CSRF Posture".into(),
                status: DoctorStatus::Fail,
                summary: format!(
                    "Authentication is disabled while bound to non-loopback host '{bind}'."
                ),
                remediation: Some(
                    "Set security.auth_enabled = true or bind server.bind_host to 127.0.0.1."
                        .into(),
                ),
                fixable: false,
            };
        }
        return DoctorCheckResult {
            id: "auth_csrf_posture".into(),
            category: "security".into(),
            title: "Authentication & CSRF Posture".into(),
            status: DoctorStatus::Warn,
            summary: format!("Authentication is disabled (loopback host '{bind}')."),
            remediation: Some("Enable security.auth_enabled for multi-user or LAN deployments.".into()),
            fixable: false,
        };
    }

    if cfg
        .security
        .admin_password
        .as_deref()
        .map_or(false, is_weak_secret)
    {
        return DoctorCheckResult {
            id: "auth_csrf_posture".into(),
            category: "security".into(),
            title: "Authentication & CSRF Posture".into(),
            status: DoctorStatus::Warn,
            summary: "Configured security.admin_password uses a weak or default secret.".into(),
            remediation: Some(
                "Rotate the admin password to a strong unique secret and store its Argon2 hash."
                    .into(),
            ),
            fixable: false,
        };
    }

    if !is_loopback && !cfg.server.allow_lan_only {
        return DoctorCheckResult {
            id: "auth_csrf_posture".into(),
            category: "security".into(),
            title: "Authentication & CSRF Posture".into(),
            status: DoctorStatus::Warn,
            summary: format!(
                "Auth enabled on non-loopback host '{bind}' with allow_lan_only = false; ensure HTTPS/TLS proxy terminates external traffic."
            ),
            remediation: Some(
                "Enable server.allow_lan_only = true or front PS5 Agent with HTTPS/TLS.".into(),
            ),
            fixable: false,
        };
    }

    DoctorCheckResult {
        id: "auth_csrf_posture".into(),
        category: "security".into(),
        title: "Authentication & CSRF Posture".into(),
        status: DoctorStatus::Pass,
        summary: format!(
            "Auth enabled (bind_host='{bind}', allow_lan_only={}).",
            cfg.server.allow_lan_only
        ),
        remediation: None,
        fixable: false,
    }
}

fn check_builtin_agent_tools(
    storage: &dyn StorageBackend,
    builtin_tool_ids: &[String],
) -> DoctorCheckResult {
    let agents = match storage.list_agents() {
        Ok(list) => list,
        Err(e) => {
            return DoctorCheckResult {
                id: "builtin_agent_tools".into(),
                category: "agents".into(),
                title: "Built-in Agents & Tool Wiring".into(),
                status: DoctorStatus::Fail,
                summary: format!("Failed reading agents from storage: {e}"),
                remediation: Some("Verify storage integrity first.".into()),
                fixable: false,
            };
        }
    };

    let present_ids: HashSet<&str> = agents.iter().map(|a| a.id.as_str()).collect();
    let missing_agents: Vec<&str> = CORE_BUILTIN_AGENT_IDS
        .iter()
        .copied()
        .filter(|id| !present_ids.contains(id))
        .collect();

    if !missing_agents.is_empty() {
        return DoctorCheckResult {
            id: "builtin_agent_tools".into(),
            category: "agents".into(),
            title: "Built-in Agents & Tool Wiring".into(),
            status: DoctorStatus::Warn,
            summary: format!("Missing core built-in agent(s): {}.", missing_agents.join(", ")),
            remediation: Some(
                "Run /doctor fix to restore missing built-in agents from defaults.".into(),
            ),
            fixable: true,
        };
    }

    let mut schedule_leaks = Vec::new();
    for agent in &agents {
        if agent.id != "commander" && agent.id != "executor" {
            let allows_schedule = SCHEDULE_TOOL_IDS.iter().any(|tool_id| {
                agent_allows_tool_with_policy(
                    &agent.enabled_tools,
                    &agent.denied_tools,
                    tool_id,
                    "automation",
                )
            });
            if allows_schedule {
                schedule_leaks.push(agent.id.as_str());
            }
        }
    }
    if !schedule_leaks.is_empty() {
        return DoctorCheckResult {
            id: "builtin_agent_tools".into(),
            category: "agents".into(),
            title: "Built-in Agents & Tool Wiring".into(),
            status: DoctorStatus::Warn,
            summary: format!(
                "Schedule tools should stay restricted to commander/executor; also permitted on: {}.",
                schedule_leaks.join(", ")
            ),
            remediation: Some(
                "Remove schedule_* / wildcard tool grants from non-commander/non-executor agents."
                    .into(),
            ),
            fixable: false,
        };
    }

    if !builtin_tool_ids.is_empty() {
        let tool_set: HashSet<&str> = builtin_tool_ids.iter().map(|s| s.trim()).collect();
        let missing_tools: Vec<&str> = REQUIRED_SANDBOX_TOOLS
            .iter()
            .copied()
            .filter(|id| !tool_set.contains(id))
            .collect();
        if !missing_tools.is_empty() {
            return DoctorCheckResult {
                id: "builtin_agent_tools".into(),
                category: "agents".into(),
                title: "Built-in Agents & Tool Wiring".into(),
                status: DoctorStatus::Warn,
                summary: format!(
                    "Tool registry missing expected sandbox/web/browser tool(s): {}.",
                    missing_tools.join(", ")
                ),
                remediation: Some("Verify ToolRegistry built-in tool registration.".into()),
                fixable: false,
            };
        }
    }

    DoctorCheckResult {
        id: "builtin_agent_tools".into(),
        category: "agents".into(),
        title: "Built-in Agents & Tool Wiring".into(),
        status: DoctorStatus::Pass,
        summary: format!(
            "All {} core built-in agents present, schedule tools properly scoped, {} built-in tool IDs checked.",
            CORE_BUILTIN_AGENT_IDS.len(),
            builtin_tool_ids.len()
        ),
        remediation: None,
        fixable: false,
    }
}

fn check_extension_directories(cfg: &AppConfig) -> DoctorCheckResult {
    let dirs = [
        ("skills", cfg.data_dir.join("skills")),
        ("plugins", cfg.data_dir.join("plugins")),
        ("mcp", cfg.data_dir.join("mcp")),
    ];

    let mut missing = Vec::new();
    for (name, path) in &dirs {
        match inspect_dir_state(path) {
            DirState::Ok => {}
            DirState::Missing => missing.push(*name),
            DirState::Symlink => {
                return DoctorCheckResult {
                    id: "extension_directories".into(),
                    category: "extensions".into(),
                    title: "Extension Directories".into(),
                    status: DoctorStatus::Fail,
                    summary: format!(
                        "Extension directory '{name}' at {} is a symlink.",
                        path.display()
                    ),
                    remediation: Some("Replace the symlink with a regular directory.".into()),
                    fixable: false,
                };
            }
            DirState::NotDir => {
                return DoctorCheckResult {
                    id: "extension_directories".into(),
                    category: "extensions".into(),
                    title: "Extension Directories".into(),
                    status: DoctorStatus::Fail,
                    summary: format!(
                        "Extension path '{name}' at {} is a file, not a directory.",
                        path.display()
                    ),
                    remediation: Some("Remove the file conflict and re-run /doctor fix.".into()),
                    fixable: false,
                };
            }
        }
    }

    if !missing.is_empty() {
        return DoctorCheckResult {
            id: "extension_directories".into(),
            category: "extensions".into(),
            title: "Extension Directories".into(),
            status: DoctorStatus::Warn,
            summary: format!("Missing extension directory(ies): {}.", missing.join(", ")),
            remediation: Some("Run /doctor fix to create skills/plugins/mcp directories.".into()),
            fixable: true,
        };
    }

    DoctorCheckResult {
        id: "extension_directories".into(),
        category: "extensions".into(),
        title: "Extension Directories".into(),
        status: DoctorStatus::Pass,
        summary: format!(
            "skills, plugins, and mcp directories present under {}.",
            cfg.data_dir.display()
        ),
        remediation: None,
        fixable: false,
    }
}

fn check_scheduler_engine(storage: &dyn StorageBackend) -> DoctorCheckResult {
    let schedules = match storage.list_schedules(0) {
        Ok(list) => list,
        Err(e) => {
            return DoctorCheckResult {
                id: "scheduler_engine".into(),
                category: "scheduler".into(),
                title: "Autonomous Scheduler Engine".into(),
                status: DoctorStatus::Fail,
                summary: format!("Failed listing schedules: {e}"),
                remediation: Some("Check schedules storage table or JSONL state.".into()),
                fixable: false,
            };
        }
    };

    let now = Utc::now();
    let overdue_cutoff = now - Duration::seconds(OVERDUE_GRACE_SECS);
    let mut enabled_count = 0usize;
    let mut error_count = 0usize;
    let mut overdue_count = 0usize;

    for sched in &schedules {
        if !sched.enabled {
            continue;
        }
        enabled_count += 1;
        if sched.last_status.as_deref().map_or(false, |s| {
            s.eq_ignore_ascii_case("error") || s.eq_ignore_ascii_case("failed")
        }) {
            error_count += 1;
        }
        if sched
            .next_run_at
            .map_or(false, |next| next <= overdue_cutoff)
        {
            overdue_count += 1;
        }
    }

    if error_count > 0 || overdue_count > 0 {
        return DoctorCheckResult {
            id: "scheduler_engine".into(),
            category: "scheduler".into(),
            title: "Autonomous Scheduler Engine".into(),
            status: DoctorStatus::Warn,
            summary: format!(
                "{} schedule(s) ({enabled_count} enabled): {error_count} error(s), {overdue_count} overdue.",
                schedules.len()
            ),
            remediation: Some(
                "Inspect failing or overdue schedules in the Scheduler panel or scheduler logs."
                    .into(),
            ),
            fixable: false,
        };
    }

    DoctorCheckResult {
        id: "scheduler_engine".into(),
        category: "scheduler".into(),
        title: "Autonomous Scheduler Engine".into(),
        status: DoctorStatus::Pass,
        summary: format!(
            "{} schedule(s) configured ({enabled_count} enabled); no errors or overdue jobs.",
            schedules.len()
        ),
        remediation: None,
        fixable: false,
    }
}

fn check_memory_bounds(cfg: &AppConfig) -> DoctorCheckResult {
    if cfg.memory.archive_after_messages <= cfg.memory.keep_recent_messages {
        return DoctorCheckResult {
            id: "memory_bounds".into(),
            category: "memory".into(),
            title: "Memory & Compaction Bounds".into(),
            status: DoctorStatus::Fail,
            summary: format!(
                "memory.archive_after_messages ({}) must exceed memory.keep_recent_messages ({}).",
                cfg.memory.archive_after_messages, cfg.memory.keep_recent_messages
            ),
            remediation: Some(
                "Set memory.archive_after_messages greater than memory.keep_recent_messages."
                    .into(),
            ),
            fixable: false,
        };
    }

    if cfg.memory.max_memories_per_agent == 0 {
        return DoctorCheckResult {
            id: "memory_bounds".into(),
            category: "memory".into(),
            title: "Memory & Compaction Bounds".into(),
            status: DoctorStatus::Warn,
            summary: format!(
                "memory.max_memories_per_agent is 0 (unlimited); archive_after_messages={}, keep_recent_messages={}.",
                cfg.memory.archive_after_messages, cfg.memory.keep_recent_messages
            ),
            remediation: Some(
                "Consider setting memory.max_memories_per_agent > 0 on PS5 to cap RAM/storage growth."
                    .into(),
            ),
            fixable: false,
        };
    }

    DoctorCheckResult {
        id: "memory_bounds".into(),
        category: "memory".into(),
        title: "Memory & Compaction Bounds".into(),
        status: DoctorStatus::Pass,
        summary: format!(
            "Memory bounds valid (max_memories_per_agent={}, archive_after_messages={}, keep_recent_messages={}).",
            cfg.memory.max_memories_per_agent,
            cfg.memory.archive_after_messages,
            cfg.memory.keep_recent_messages
        ),
        remediation: None,
        fixable: false,
    }
}

fn check_ps5_platform_compat(cfg: &AppConfig) -> DoctorCheckResult {
    let workers = cfg.runtime.tokio_worker_threads;
    if workers == 0 || workers > 8 {
        return DoctorCheckResult {
            id: "ps5_platform_compat".into(),
            category: "platform".into(),
            title: "PS5 Platform Compatibility".into(),
            status: DoctorStatus::Warn,
            summary: format!(
                "runtime.tokio_worker_threads = {workers} (PS5 budget recommends 1..=8 threads)."
            ),
            remediation: Some("Set runtime.tokio_worker_threads to 2..=8.".into()),
            fixable: false,
        };
    }

    if cfg.sandbox.allow_host_shell {
        return DoctorCheckResult {
            id: "ps5_platform_compat".into(),
            category: "platform".into(),
            title: "PS5 Platform Compatibility".into(),
            status: DoctorStatus::Warn,
            summary: if cfg!(target_vendor = "sie") {
                "sandbox.allow_host_shell = true, but OS subprocesses are disabled on PS5 (virtual sandbox shell will be used).".into()
            } else {
                "sandbox.allow_host_shell = true; host shell execution is enabled outside virtual sandbox.".into()
            },
            remediation: Some(
                "Keep sandbox.allow_host_shell = false to use the PS5-identical pure-Rust virtual shell."
                    .into(),
            ),
            fixable: false,
        };
    }

    if cfg!(target_vendor = "sie") && cfg.database.wal_mode {
        return DoctorCheckResult {
            id: "ps5_platform_compat".into(),
            category: "platform".into(),
            title: "PS5 Platform Compatibility".into(),
            status: DoctorStatus::Warn,
            summary: "database.wal_mode = true on PS5; set wal_mode = false to avoid mmap(-shm)."
                .into(),
            remediation: Some("Set database.wal_mode = false in config.toml.".into()),
            fixable: false,
        };
    }

    let wal_note = if cfg.database.wal_mode {
        "wal_mode=true (recommended false on PS5)"
    } else {
        "wal_mode=false"
    };

    DoctorCheckResult {
        id: "ps5_platform_compat".into(),
        category: "platform".into(),
        title: "PS5 Platform Compatibility".into(),
        status: DoctorStatus::Pass,
        summary: format!(
            "PS5 compatibility OK (tokio_worker_threads={workers} <= 8, {wal_note}, allow_host_shell=false)."
        ),
        remediation: None,
        fixable: false,
    }
}

fn check_meta_release_sync_at(base_dir: &Path) -> DoctorCheckResult {
    let meta_path = base_dir.join("build").join("meta.json");
    let elf_path = base_dir.join("build").join("ps5-agent.elf");
    let meta_exists = meta_path.is_file();
    let elf_exists = elf_path.is_file();

    if !meta_exists && !elf_exists {
        return DoctorCheckResult {
            id: "meta_release_sync".into(),
            category: "release".into(),
            title: "PS5 Release & meta.json Sync".into(),
            status: DoctorStatus::Pass,
            summary: "Host dev mode (no ELF present)".into(),
            remediation: None,
            fixable: false,
        };
    }

    if !meta_exists || !elf_exists {
        return DoctorCheckResult {
            id: "meta_release_sync".into(),
            category: "release".into(),
            title: "PS5 Release & meta.json Sync".into(),
            status: DoctorStatus::Warn,
            summary: format!(
                "Incomplete build/ artifacts (meta.json present={meta_exists}, ps5-agent.elf present={elf_exists})."
            ),
            remediation: Some(
                "Run build/package release script so build/meta.json matches build/ps5-agent.elf."
                    .into(),
            ),
            fixable: false,
        };
    }

    let raw = match fs::read_to_string(&meta_path) {
        Ok(s) => s,
        Err(e) => {
            return DoctorCheckResult {
                id: "meta_release_sync".into(),
                category: "release".into(),
                title: "PS5 Release & meta.json Sync".into(),
                status: DoctorStatus::Warn,
                summary: format!("Failed reading build/meta.json: {e}"),
                remediation: Some("Regenerate build/meta.json alongside build/ps5-agent.elf.".into()),
                fixable: false,
            };
        }
    };

    let Ok(val) = serde_json::from_str::<serde_json::Value>(&raw) else {
        return DoctorCheckResult {
            id: "meta_release_sync".into(),
            category: "release".into(),
            title: "PS5 Release & meta.json Sync".into(),
            status: DoctorStatus::Warn,
            summary: "build/meta.json contains invalid JSON.".into(),
            remediation: Some("Regenerate valid build/meta.json.".into()),
            fixable: false,
        };
    };

    let version = val
        .get("version")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    DoctorCheckResult {
        id: "meta_release_sync".into(),
        category: "release".into(),
        title: "PS5 Release & meta.json Sync".into(),
        status: DoctorStatus::Pass,
        summary: format!("build/ps5-agent.elf and build/meta.json present (version={version})."),
        remediation: None,
        fixable: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use ps5_agent_core::{
        AgentDefinition, AgentEnvelope, ApprovalRecord, AuditLogEntry, ChatMessage, Conversation,
        DeliveryStatus, InferenceMessage, InferenceRequest, InferenceResponse,
        InferenceStreamChunk, MemoryItem, MemoryScope, ProviderHealth, ScheduleKind,
        ScheduleRecord, SessionRecord, TaskCountsSummary, TaskEvent, TaskRecord,
        ToolExecutionRecord, ToolSpec,
    };
    use std::collections::HashMap;
    use std::sync::Mutex;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(prefix: &str) -> Self {
            let dir = std::env::temp_dir().join(format!("{prefix}-{}", ps5_agent_core::new_id()));
            fs::create_dir_all(&dir).expect("create temp dir");
            Self(dir)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[derive(Default)]
    struct MockDoctorStorage {
        agents: Mutex<HashMap<String, AgentDefinition>>,
        schedules: Mutex<Vec<ScheduleRecord>>,
    }

    impl StorageBackend for MockDoctorStorage {
        fn driver_name(&self) -> &'static str {
            "sqlite"
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
        fn upsert_task(&self, _: &TaskRecord) -> Result<()> {
            Ok(())
        }
        fn get_task(&self, _: &str) -> Result<Option<TaskRecord>> {
            Ok(None)
        }
        fn list_tasks(&self, _: usize) -> Result<Vec<TaskRecord>> {
            Ok(Vec::new())
        }
        fn task_counts_summary(&self) -> Result<TaskCountsSummary> {
            Ok(TaskCountsSummary::default())
        }
        fn append_task_event(&self, _: &TaskEvent) -> Result<()> {
            Ok(())
        }
        fn list_task_events(&self, _: &str) -> Result<Vec<TaskEvent>> {
            Ok(Vec::new())
        }
        fn append_agent_message(&self, _: &AgentEnvelope) -> Result<()> {
            Ok(())
        }
        fn update_agent_message_status(&self, _: &str, _: DeliveryStatus) -> Result<()> {
            Ok(())
        }
        fn list_agent_messages(
            &self,
            _: Option<&str>,
            _: Option<&str>,
            _: usize,
        ) -> Result<Vec<AgentEnvelope>> {
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
        fn list_schedules(&self, _: usize) -> Result<Vec<ScheduleRecord>> {
            Ok(self.schedules.lock().unwrap().clone())
        }
    }

    struct MockDoctorInference {
        reachable: bool,
    }

    #[async_trait]
    impl InferenceProvider for MockDoctorInference {
        async fn generate(
            &self,
            _req: InferenceRequest,
            _cancel: CancellationToken,
        ) -> Result<InferenceResponse> {
            unreachable!()
        }

        async fn generate_stream(
            &self,
            _req: InferenceRequest,
            _tx: mpsc::Sender<InferenceStreamChunk>,
            _cancel: CancellationToken,
        ) -> Result<InferenceResponse> {
            unreachable!()
        }

        async fn prefill_kv_cache(
            &self,
            _messages: Vec<InferenceMessage>,
            _model_override: Option<String>,
            _cancel: CancellationToken,
        ) -> Result<()> {
            Ok(())
        }

        async fn health_check(&self) -> ProviderHealth {
            ProviderHealth {
                reachable: self.reachable,
                loaded_model: if self.reachable {
                    Some("qwen3-14b".into())
                } else {
                    None
                },
                available_models: if self.reachable {
                    vec!["qwen3-14b".into()]
                } else {
                    Vec::new()
                },
                error: if self.reachable {
                    None
                } else {
                    Some("connection refused".into())
                },
            }
        }

        async fn list_models(&self) -> Result<Vec<String>> {
            Ok(vec!["qwen3-14b".into()])
        }
    }

    fn all_required_tool_ids() -> Vec<String> {
        REQUIRED_SANDBOX_TOOLS
            .iter()
            .map(|s| (*s).to_string())
            .collect()
    }

    #[tokio::test]
    async fn doctor_checks_and_fixes_repair_workspace_brain_extensions_and_agents() {
        let dir = TempDir::new("ps5-doctor-fix");
        let mut cfg = AppConfig::with_data_dir(dir.path().to_path_buf());
        cfg.memory.max_memories_per_agent = 500;
        let storage = MockDoctorStorage::default();
        let provider = MockDoctorInference { reachable: true };
        let tool_ids = all_required_tool_ids();

        let before = run_doctor_checks(&cfg, &storage, Some(&provider), &tool_ids)
            .await
            .expect("doctor checks before");
        assert_eq!(before.checks.len(), 12);
        assert_eq!(before.overall_status, DoctorStatus::Fail);

        let fixed = run_doctor_fixes(&cfg, &storage, Some(&provider), &tool_ids)
            .await
            .expect("doctor fixes");
        assert!(!fixed.applied_fixes.is_empty());
        assert_eq!(fixed.report.checks.len(), 12);
        assert_eq!(fixed.report.fail_count, 0);
        assert_eq!(fixed.report.warn_count, 0);
        assert_eq!(fixed.report.overall_status, DoctorStatus::Pass);
        assert_eq!(storage.list_agents().unwrap().len(), 5);

        let ws = dir.path().join("workspace");
        assert!(ws.join("sandbox").join("downloads").is_dir());
        assert!(ws.join("AGENTS.md").is_file());
        assert!(ws.join(".agents").join("rules").join("ps5-sandbox.md").is_file());
        assert!(dir.path().join("skills").is_dir());
        assert!(dir.path().join("plugins").is_dir());
        assert!(dir.path().join("mcp").is_dir());
    }

    #[tokio::test]
    async fn doctor_detects_security_scheduler_platform_and_release_warnings() {
        let dir = TempDir::new("ps5-doctor-warns");
        let mut cfg = AppConfig::with_data_dir(dir.path().to_path_buf());
        cfg.server.bind_host = "0.0.0.0".into();
        cfg.security.auth_enabled = false;
        cfg.tools.allowed_domains = vec!["169.254.169.254".into()];
        cfg.runtime.tokio_worker_threads = 16;
        cfg.memory.archive_after_messages = 10;
        cfg.memory.keep_recent_messages = 16;

        let storage = MockDoctorStorage::default();
        for mut def in default_agent_definitions() {
            if def.id == "researcher" {
                def.enabled_tools.push("schedule_create".into());
            }
            storage.upsert_agent(&def).unwrap();
        }
        storage.schedules.lock().unwrap().push(ScheduleRecord {
            id: "s1".into(),
            title: "Overdue".into(),
            prompt: "check".into(),
            target_agent_id: None,
            conversation_id: None,
            kind: ScheduleKind::Interval,
            run_at: None,
            interval_secs: Some(60),
            daily_time: None,
            cron_expr: None,
            timezone_offset_mins: 0,
            enabled: true,
            next_run_at: Some(Utc::now() - Duration::minutes(10)),
            last_run_at: None,
            last_status: Some("error".into()),
            last_error: Some("failed".into()),
            last_task_id: None,
            run_count: 1,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        });

        let provider = MockDoctorInference { reachable: false };
        let report = run_doctor_checks(&cfg, &storage, Some(&provider), &all_required_tool_ids())
            .await
            .expect("doctor report");
        let by_id: HashMap<&str, &DoctorCheckResult> =
            report.checks.iter().map(|c| (c.id.as_str(), c)).collect();

        assert_eq!(by_id["ps5lm_endpoint"].status, DoctorStatus::Warn);
        assert_eq!(by_id["auth_csrf_posture"].status, DoctorStatus::Fail);
        assert_eq!(by_id["sandbox_network_policy"].status, DoctorStatus::Fail);
        assert_eq!(by_id["builtin_agent_tools"].status, DoctorStatus::Warn);
        assert_eq!(by_id["scheduler_engine"].status, DoctorStatus::Warn);
        assert_eq!(by_id["memory_bounds"].status, DoctorStatus::Fail);
        assert_eq!(by_id["ps5_platform_compat"].status, DoctorStatus::Warn);

        let build_dir = dir.path().join("build");
        fs::create_dir_all(&build_dir).unwrap();
        fs::write(build_dir.join("ps5-agent.elf"), b"elf").unwrap();
        assert_eq!(
            check_meta_release_sync_at(dir.path()).status,
            DoctorStatus::Warn
        );
        fs::write(
            build_dir.join("meta.json"),
            r#"{"id":"ps5-agent","filename":"ps5-agent.elf","version":"v0.1.0"}"#,
        )
        .unwrap();
        assert_eq!(
            check_meta_release_sync_at(dir.path()).status,
            DoctorStatus::Pass
        );
    }
}
