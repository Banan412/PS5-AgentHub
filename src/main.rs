#[cfg(target_vendor = "sie")]
use ps5_std_rt as _;

use ps5_agent::shutdown::{cleanup_pid_file, finalize_shutdown, spawn_signal_handler};
use ps5_agent::startup::{
    initialize_with_warnings, load_effective_config_with_warnings, parse_cli_args,
};
use ps5_agent_core::now_iso;
use ps5_agent_logging::log_error;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::process::ExitCode;

fn worker_thread_count(configured: usize) -> usize {
    #[cfg(target_vendor = "sie")]
    {
        configured.clamp(1, 3)
    }
    #[cfg(not(target_vendor = "sie"))]
    {
        configured.clamp(1, 8)
    }
}

fn report_fatal_startup_error(stage: &str, err: &dyn std::fmt::Display) {
    let dir = ps5_agent_config::resolve_writable_data_dir(&ps5_agent_config::default_data_dir());
    let _ = fs::create_dir_all(&dir);
    let log_path = dir.join("startup_error.log");
    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(&log_path) {
        let _ = writeln!(f, "{} [FATAL] [{stage}] {err}", now_iso());
    }

    eprintln!("{stage}: {err}");
    log_error("startup", format!("Fatal startup error ({stage}): {err}"));

    #[cfg(target_vendor = "sie")]
    {
        ps5::klog::log(&format!("[ps5-agent][FATAL] {stage}: {err}"));
        let _ = ps5::Toast::new("PS5 Agent Startup Error")
            .sub(&format!("{stage}: {err}"))
            .send();
    }
}

fn main() -> ExitCode {
    #[cfg(target_vendor = "sie")]
    {
        let _ = ps5::payload::set_process_name("ps5-agent.elf");
        unsafe {
            libc::signal(libc::SIGPIPE, libc::SIG_IGN);
            libc::signal(libc::SIGHUP, libc::SIG_IGN);
        }
    }

    let cli = parse_cli_args();
    let (cfg, config_warnings) = match load_effective_config_with_warnings(&cli) {
        Ok(res) => res,
        Err(err) => {
            report_fatal_startup_error("config", &err);
            return ExitCode::from(2);
        }
    };

    let workers = worker_thread_count(cfg.runtime.tokio_worker_threads);
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
    {
        Ok(r) => r,
        Err(err) => {
            report_fatal_startup_error("tokio_runtime", &err);
            return ExitCode::from(3);
        }
    };

    let exit_code = rt.block_on(async move {
        let system = match initialize_with_warnings(cfg, config_warnings).await {
            Ok(sys) => sys,
            Err(err) => {
                report_fatal_startup_error("initialize", &err);
                return 1u8;
            }
        };

        spawn_signal_handler(&system);

        let serve_res = ps5_agent_http::serve(system.http_state.clone()).await;
        if let Err(err) = serve_res {
            system
                .logger
                .error("http.server", format!("HTTP server terminated with error: {err}"));
        }

        finalize_shutdown(&system);
        if let Ok(cfg) = system.config.read() {
            cleanup_pid_file(&cfg.data_dir);
        }
        0u8
    });

    drop(rt);

    #[cfg(target_vendor = "sie")]
    {
        ps5::payload::exit(i32::from(exit_code));
    }

    #[cfg(not(target_vendor = "sie"))]
    ExitCode::from(exit_code)
}
