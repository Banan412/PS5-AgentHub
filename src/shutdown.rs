use crate::adapters::AppSystem;
use ps5_agent_core::TaskStatus;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

#[allow(dead_code)]
static TERM_SIGNAL_RECEIVED: AtomicBool = AtomicBool::new(false);

#[allow(dead_code)]
extern "C" fn on_term_signal(_sig: libc::c_int) {
    TERM_SIGNAL_RECEIVED.store(true, Ordering::SeqCst);
}

pub fn spawn_signal_handler(system: &AppSystem) {
    #[cfg(target_vendor = "sie")]
    {
        TERM_SIGNAL_RECEIVED.store(false, Ordering::SeqCst);
        unsafe {
            libc::signal(libc::SIGTERM, on_term_signal as usize);
            libc::signal(libc::SIGINT, on_term_signal as usize);
        }
        let shutdown = system.shutdown.clone();
        let logger = system.logger.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_millis(250));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = ticker.tick() => {
                        if TERM_SIGNAL_RECEIVED.load(Ordering::SeqCst) {
                            logger.warn(
                                "shutdown",
                                "SIGTERM/SIGINT received; initiating graceful shutdown",
                            );
                            shutdown.cancel();
                            break;
                        }
                    }
                }
            }
        });
    }

    #[cfg(all(unix, not(target_vendor = "sie")))]
    {
        let shutdown = system.shutdown.clone();
        let logger = system.logger.clone();
        tokio::spawn(async move {
            let mut sigterm = match tokio::signal::unix::signal(
                tokio::signal::unix::SignalKind::terminate(),
            ) {
                Ok(s) => s,
                Err(_) => {
                    tokio::select! {
                        _ = shutdown.cancelled() => {}
                        res = tokio::signal::ctrl_c() => {
                            if res.is_ok() {
                                logger.warn("shutdown", "Ctrl+C received; initiating graceful shutdown");
                                shutdown.cancel();
                            }
                        }
                    }
                    return;
                }
            };
            tokio::select! {
                _ = shutdown.cancelled() => {}
                res = tokio::signal::ctrl_c() => {
                    if res.is_ok() {
                        logger.warn("shutdown", "Ctrl+C received; initiating graceful shutdown");
                        shutdown.cancel();
                    }
                }
                _ = sigterm.recv() => {
                    logger.warn("shutdown", "SIGTERM received; initiating graceful shutdown");
                    shutdown.cancel();
                }
            }
        });
    }

    #[cfg(not(unix))]
    {
        let shutdown = system.shutdown.clone();
        let logger = system.logger.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = shutdown.cancelled() => {}
                res = tokio::signal::ctrl_c() => {
                    if res.is_ok() {
                        logger.warn("shutdown", "Ctrl+C received; initiating graceful shutdown");
                        shutdown.cancel();
                    }
                }
            }
        });
    }
}

pub fn cleanup_pid_file(data_dir: &Path) {
    let pid_path = data_dir.join("ps5-agent.pid");
    if pid_path.exists() {
        let _ = std::fs::remove_file(&pid_path);
    }
}

pub fn finalize_shutdown(system: &AppSystem) {
    system.shutdown.cancel();

    // Cancel any active/pending tasks cleanly
    if let Ok(tasks) = system.store.list_tasks(500) {
        for task in tasks {
            if !task.status.is_terminal() && task.status != TaskStatus::Waiting {
                let _ = system
                    .runtime
                    .task_manager()
                    .cancel_task(&task.id, "Daemon shutting down");
            }
        }
    }

    if let Err(e) = system.store.checkpoint() {
        system
            .logger
            .warn("shutdown", format!("SQLite checkpoint error on shutdown: {e}"));
    } else {
        system
            .logger
            .info("shutdown", "Storage checkpoint completed");
    }

    if let Ok(cfg) = system.config.read() {
        cleanup_pid_file(&cfg.data_dir);
    }

    system.logger.info("shutdown", "PS5 Agent stopped cleanly");
    system.logger.flush();
}
