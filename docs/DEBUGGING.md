# PS5 Agent Debugging & Diagnostics Guide

This guide covers console telemetry channels, Milestone 1 hardware probe verification, HTTP/dashboard observability, and troubleshooting on a jailbroken **PlayStation 5 Slim (Firmware `13.40`)** and host development builds.

---

## 1. Console Telemetry Channels

`ps5-agent` uses `AppLogger` (`crates/ps5-agent-logging/src/lib.rs`) to fan out structured log events simultaneously to an in-memory ring buffer, rotating file storage, broken-pipe-safe `stdout`/`stderr`, `/dev/klog` (PS5), and on-screen PS5 toast notifications.

### Telemetry Sinks at a Glance

| Channel | Configuration / Target | Destination | Notes |
|---|---|---|---|
| **Kernel Log (`/dev/klog`)** | `logging.enable_klog = true` (`target_vendor = "sie"`) | PS5 kernel log stream (typically TCP `:9081` via klog/etaHEN, or `:3232` via `klogsrv`) | Emits `[ps5-agent][<LEVEL>] <target>: <message>` via `ps5::klog::log` -> `ps5_sys::klog_puts`. |
| **`elfldr` Deploy Socket** | `scripts/deploy-ps5.ps1 -Follow` (TCP `:9021`) | Attached to fds `0..2` (`stdout`/`stderr`) of the payload process | Streams stdout/stderr until the sender disconnects or `-StreamSeconds` elapses (`-Follow` keeps reading). |
| **PS5 On-Screen Toasts** | `logging.enable_toast = true` (`libSceNotification.sprx`) | PS5 system notification overlay (`sceNotificationSend` / `ps5::notify::simple`) | Sends startup readiness, first-boot credentials (when generated), `PS5 Agent Startup Error` alerts, panic hook alerts (`ps5-agent panic`), and probe status. |
| **Rotating File Logs** | `logging.log_dir` + `logging.max_file_bytes` | `/data/ps5-agent/logs/ps5-agent.log` (fallback: `/download0/ps5-agent/logs/ps5-agent.log`) | Rotates to `ps5-agent.log.1` when size exceeds `max_file_bytes` (default `2097152` bytes / 2 MiB; floor `65536` bytes). |
| **In-Memory Ring Buffer** | `logging.ring_buffer_capacity` (default `500`, floor `64`) | `GET /api/v1/logs` & Dashboard **Logs -> Runtime Logs** | Bounded `VecDeque<LogEntry>` retaining the newest `N` entries in RAM. |

### Why `println!` and `eprintln!` Are Banned

When `ps5-payload-dev/elfldr` launches an ELF sent to port `9021`, it attaches file descriptors `0..2` directly to the deployer's TCP socket. As soon as the deployer disconnects (for example, after `scripts/deploy-ps5.ps1` finishes its default 5-second `-StreamSeconds` window or `nc` closes), writes to `stdout`/`stderr` fail with `EPIPE` (`BrokenPipe`).

In Rust `std`, `println!` and `eprintln!` **panic** on write errors such as `BrokenPipe`—and with `panic = "abort"` in `Cargo.toml`, an unhandled `BrokenPipe` panic immediately aborts the daemon.

To prevent this (both on closed `elfldr` deploy sockets and when `pldmgr` cold-boot autostart launches with detached/closed standard descriptors):
- `AppLogger::log()` (`crates/ps5-agent-logging/src/lib.rs`) writes log lines to `stdout` via `let _ = std::io::stdout().write_all(line_with_nl.as_bytes());` (`elfldr` binds fd `1` to the sender socket), while sink/init/fallback diagnostics use `write_stderr_line()` (`let _ = writeln!(std::io::stderr(), "{line}");`), discarding `BrokenPipe` and other I/O errors.
- `report_fatal_startup_error()` (`src/main.rs`) and `resolve_writable_data_dir()` (`crates/ps5-agent-config/src/lib.rs`) likewise use `let _ = std::io::Write::write_fmt(&mut std::io::stderr(), ...);` / `let _ = writeln!(std::io::stderr(), ...);` so early startup/mount-retry messages never panic when `stderr` is detached.
- `bins/m1-ps5-probe/src/main.rs` uses `safe_stdout()`, which locks `std::io::stdout()` and ignores `write_all`/`flush` errors.
- `install_panic_hook()` (`crates/ps5-agent-logging/src/lib.rs`) records any panic to the ring buffer, file log, `/dev/klog`, and a PS5 toast (`ps5-agent panic`), then flushes before aborting.

### Streaming Live Console Output

1. **Follow `elfldr` stdout during deployment (Windows):**
   ```powershell
   powershell -ExecutionPolicy Bypass -File scripts/deploy-ps5.ps1 -TargetHost <PS5-IP> -Port 9021 -Follow
   ```
2. **Connect to the PS5 `/dev/klog` stream:**
   ```bash
   # Common etaHEN / klog TCP stream port
   nc <PS5-IP> 9081
   # Or port 3232 when running ps5-payload-dev/klogsrv
   nc <PS5-IP> 3232
   ```
3. **Inspect persistent logs over FTP:**
   Download `/data/ps5-agent/logs/ps5-agent.log` (and `.log.1` if rotated), or `/download0/ps5-agent/logs/ps5-agent.log` if HEN blocked `/data`.

---

## 2. Milestone 1 Hardware Probe (`m1-ps5-probe`)

Before running the full multi-crate agent daemon on physical PS5 hardware, use the standalone Milestone 1 probe (`bins/m1-ps5-probe/src/main.rs`, built to `build/m1-ps5-probe.elf`) to isolate FreeBSD 11/12 ABI translation (`ps5-std-rt`), `/data` filesystem permissions, Sony hardware/toast syscalls, and Tokio `kevent` networking.

### What `build/m1-ps5-probe.elf` Verifies

On startup, `m1-ps5-probe` executes the following checks in order:
1. Installs a panic hook that writes to `/data/ps5-agent/m1_probe.log`, `/dev/klog`, `safe_stdout`, and `ps5::Toast`.
2. Queries Sony system info: `ps5::fw_version()`, `ps5::pid()`, `ps5::hw::model_name()`, and `ps5::hw::cpu_temperature()`.
3. Runs `verify_filesystem()` against `/data/ps5-agent-m1-test`: `create_dir_all`, writes `probe.txt`, reads back and compares bytes, then removes `probe.txt` and the test directory.
4. Sends an on-screen toast (`"m1-ps5-probe ready"` with firmware version, model, and CPU temperature).
5. Starts a multi-threaded Tokio runtime (`TcpListener::bind(("0.0.0.0", 9957))`) and serves HTTP/1.1 JSON responses:
   - `GET /`: returns probe status, firmware version, model, CPU temp, `fs_test` result, hit counter, uptime, and peer address.
   - `GET /quit`: responds with `{"status":"quitting"}`, sends an exit toast, and terminates cleanly via `ps5::sys::_Exit(0)`.

### Building and Running the Probe

Build `build/m1-ps5-probe.elf`:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/build-ps5.ps1 -Package m1-ps5-probe
```

Deploy and verify automatically with `scripts/verify-on-ps5.py`:

```bash
# Deploy build/m1-ps5-probe.elf to elfldr (:9021), poll http://<PS5-IP>:9957/, check status & fs_test, then call /quit
python3 scripts/verify-on-ps5.py --host <PS5-IP> --mode m1 --quit
```

Useful `scripts/verify-on-ps5.py` options:
- `--elfldr-port 9021` — override `elfldr` port.
- `--probe-port 9957` — override M1 probe HTTP port.
- `--skip-deploy` — query an already-running probe without re-sending the ELF.
- `--mode agent --agent-port 8080` — verify the full `build/ps5-agent.elf` deployment via `GET /api/v1/health` (or `/health` fallback).

Example successful JSON response from `http://<PS5-IP>:9957/`:

```json
{
  "status": "ok",
  "probe": "m1-ps5-probe",
  "pid": 42,
  "fw_version": "0x13400000",
  "fw_version_raw": 322961408,
  "model": "CFI-2000",
  "cpu_temp_c": 52,
  "cpu_temp_err": null,
  "fs_test": {
    "ok": true,
    "detail": "mkdir + write + read + cleanup OK"
  },
  "hits": 1,
  "uptime_ms": 514,
  "peer": "192.168.1.50:54321"
}
```

---

## 3. Dashboard & HTTP API Observability

All `/api/v1/*` endpoints below (except unauthenticated `/api/v1/health`) require an authenticated session cookie or `Authorization: Bearer <token>`, and mutating `POST`/`PUT`/`DELETE` cookie requests require `X-CSRF-Token`.

### Runtime Logs Ring Buffer

- **UI:** **Logs -> Runtime Logs** (`#logs-tab-runtime`), with level filter (`#logs-level-filter`) and live text search across `target`, `message`, and `level`.
- **API:** `GET /api/v1/logs?level=<error|warn|info|debug|trace>&limit=<1..1000>` (default `limit = 100`).
- **Dynamic verbosity:** Adjust `logging.level` at runtime without restarting via **Settings** or `PUT /api/v1/settings` (`{"logging": {"level": "debug"}}`), which calls `AppLogger::set_level` and persists `config.toml`.

### Persistent Security Audit Logs

- **UI:** **Logs -> Security Audit Logs** (`#logs-tab-audit`), searchable by `actor`, `action`, `target`, and `outcome`, with formatted `details_json`.
- **API:** `GET /api/v1/audit-logs?limit=<1..1000>` (backed by SQLite `audit_logs` or `jsonl_db/audit_logs.jsonl`).
- **Recorded actions:** `auth.login`, `auth.logout`, `auth.password_change`, `agent.create`, `agent.update`, `agent.delete`, `approval.approve`, `approval.deny`, `conversation.cancel_tasks`, `task.expedite`, `tool.toggle`, `tool.execute`, `settings.update`, `inference.systemone`, `system.elf_upgrade`, `system.checkpoint`, and `system.shutdown`.

### Task Events Inspection Modal

- **UI:** In the **Tasks** section, click **Inspect** on any task row to open the **Task Inspection** modal.
- **API:** `GET /api/v1/tasks/{id}/events` returns the chronological `Vec<TaskEvent>` (`timestamp`, `event_type`, `details`).
- **What it shows:** Task ID, Assigned Agent, Status and `progress_pct`, creation timestamp, full `user_request`, `result`, `error_info`, and the ordered `Task Events` log—making it straightforward to trace multi-agent handoffs, approval pauses, and inference failures.

### Metrics & Health Endpoints (`GET /api/v1/metrics` & `GET /api/v1/health`)

- **Overview polling:** The dashboard polls `GET /api/v1/metrics` to populate header/overview cards and status pills (`LAN OK` / `Network Degraded`, `PS5LM Reachable` / `Unreachable`, and active `SQLITE` / `JSONL` storage badge).
- **`MetricsSnapshot` fields (`crates/ps5-agent-core/src/models.rs`):**
  - Runtime & Network: `uptime_secs`, `network_degraded`, `memory_rss_bytes`, `http_requests_total`, `http_avg_ms`.
  - PS5 Hardware (`target_vendor = "sie"`): `ps5_fw_version`, `ps5_model`, `ps5_cpu_temp_c`, `ps5_soc_temp_c`.
  - Storage: `storage_driver` (`"sqlite"` or `"jsonl"`), `db_ops_total`, `db_avg_us`.
  - Inference (`PS5LM`): `ps5lm_reachable`, `ps5lm_loaded_model`, `inference_requests_total`, `inference_errors_total`, `inference_p50_ms`, `inference_p95_ms`, `last_inference_ms`, `tokens_prompt_total`, `tokens_completion_total`.
  - Tasks: `active_tasks`, `queued_tasks`, `completed_tasks`, `failed_tasks`.
- **Health endpoint:** `GET /api/v1/health` always returns `status` (`"ok"` or `"degraded"` when `network_degraded` is true) and `version`; when authenticated, it also includes `uptime_secs`, `storage_driver`, `ps5lm_reachable`, `ps5lm_loaded_model`, and `network_degraded`.

### Chat Raw Output Toggle (`Show raw output`) & `</think>` Thinking Inspection

- **Raw Output Mode (`#chat-raw-output-toggle`):** Toggle **Show raw output** in the chat header (`crates/ps5-agent-dashboard/assets/index.html`, persisted in `localStorage` via `RAW_OUTPUT_KEY` in `app.js`).
  - When **off** (default), `msg.reasoning` renders in a collapsible `<details class="thinking-drawer">` (`Reasoning (live · N chars)` / `Reasoning (N chars)`), while `msg.content` renders formatted Markdown and workspace script artifact cards.
  - When **on**, the separate reasoning drawer is hidden and `formatRawMessageText(reasoning, content)` renders `<pre class="msg-raw-output">` showing `<think>\n{reasoning}\n</think>\n\n{content}` verbatim (without Markdown rendering; **Copy** also copies the raw text). Use this to inspect raw code fences, `<tool_call>` tags, or malformed Markdown.
- **`</think>` & `<seed:think>` Isolation (`crates/ps5-agent-inference/src/parser.rs`):**
  - `ThinkStreamSplitter` splits `<think>...</think>` and `<seed:think>...</seed:think>` blocks (even across partial SSE chunk boundaries) into `StreamPiece::Reasoning` (`ChatReasoning`) vs `StreamPiece::Text` (`ChatToken`).
  - **Orphan closing tag recovery:** If a model begins emitting reasoning immediately without an opening `<think>` tag and later emits `</think>` or `</seed:think>` before any visible text (`!emitted_any_text && !closed_any_think` in `ThinkStreamSplitter`, `cursor == 0 && cleaned.is_empty()` in `extract_think_blocks`, and `sweepOrphanClosingThinkTag()` in `app.js`), everything before the closing tag is routed to `msg.reasoning` / `ChatCompleted.reasoning` and kept out of `msg.content`.
- **Thinking Mode Wire Payload (`crates/ps5-agent-inference/src/provider.rs`):**
  - `off`: `chat_template_kwargs: {"enable_thinking": false}`, `reasoning_budget: 0`, `reasoning_format: "none"` (no `reasoning_effort`); `TaskUpdated` note shows `"Generating response…"`.
  - `low` / `medium` / `high` / `max`: sets `enable_thinking: true`, `reasoning_effort` (`"low"` / `"medium"` / `"high"` / `"high"`), `reasoning_budget` (`512` / `2048` / `8192` / `-1`), and `reasoning_format` (from `inference.reasoning_format`, default `"deepseek"`), emitting `"Thinking (<level>)…"`.
  - Group/Auto coordinator routing (`coordinate_group_participants`) always runs with `ThinkingLevel::Off` so coordinator JSON never emits `</think>` reasoning blocks.

### Per-Token Inference Timings (`inference.timings_per_token`)

- **Enablement:** Set `timings_per_token = true` in `[inference]` (`config.toml`), via `PUT /api/v1/settings`, or in **Settings -> KV Cache & llama.cpp / PS5LM Sampling -> Timings Per Token (`#cfg-timings-per-token`)** (default `false` to avoid extra payload overhead).
- **Security Guard Exemption:** `is_sensitive_setting_key()` (`crates/ps5-agent-http/src/routes.rs`) explicitly exempts `"timings_per_token"` and `"*.timings_per_token"` from the `"token"` redaction/`403` filter so `GET`/`PUT /api/v1/settings` read and hot-reload it cleanly.
- **Wire Effect:** `apply_llamacpp_sampling_fields()` (`crates/ps5-agent-inference/src/provider.rs`) includes `"timings_per_token": true` on outgoing `llama.cpp` / `PS5LM` requests so `PS5LM` returns per-token prompt-eval and generation timing statistics in responses. Pair this with `logging.level = "debug"` and `/api/v1/metrics` (`inference_p50_ms`, `inference_p95_ms`, `last_inference_ms`, `tokens_prompt_total`, `tokens_completion_total`) to separate slow KV prefill from decode throughput bottlenecks.

### Manual Database Checkpoint (`POST /api/v1/system/checkpoint`)

- **UI:** Click **Checkpoint DB** (Overview quick actions) or **Checkpoint Database** (Settings -> System Controls).
- **API:** `POST /api/v1/system/checkpoint` calls `StorageBackend::checkpoint()`, appends a `system.checkpoint` audit log entry, and returns `{"checkpointed": true}`.
  - On **SQLite**: checks `PRAGMA auto_vacuum` (running `PRAGMA incremental_vacuum;` when incremental, or `PRAGMA auto_vacuum = INCREMENTAL; VACUUM;` otherwise) followed by `PRAGMA wal_checkpoint(TRUNCATE); PRAGMA optimize; PRAGMA shrink_memory;` to reclaim free pages, shrink SQLite heap memory, and flush any journal/WAL state into `agent.db` before FTP backup or console power-off.
  - On **JSONL**: flushes `state.json` via atomic write-temp-rename (`write_snapshot_atomic`).

---

## 4. Troubleshooting Common Issues

### Early Startup Failures (`startup_error.log`, `.broken.bak`, `.corrupt.bak`, Startup Toasts)

If `ps5-agent.elf` exits before the HTTP dashboard comes up, inspect the writable data directory (`/data/ps5-agent/` or `/download0/ps5-agent/` fallback) over FTP and `/dev/klog`:

- **`/data/ps5-agent` vs `/download0/ps5-agent` Cold-Boot Mount Retry (`resolve_writable_data_dir`):** During early `pldmgr` cold-boot autostart, `/data` may still be mounting when `ps5-agent.elf` launches. On `target_vendor = "sie"`, `resolve_writable_data_dir()` (`crates/ps5-agent-config/src/lib.rs`) runs `try_ensure_writable()` (directory creation plus `.write_probe` write/delete) and retries `/data/ps5-agent` with `DATA_MOUNT_RETRY_ATTEMPTS = 12` (`1` initial attempt + `11 × 250ms` sleeps = up to `2.75s`). It logs `[ps5-agent-config] /data/ps5-agent became writable on retry attempt <N>/12` or `... remained unwritable after 12 attempts; falling back to /download0/ps5-agent` using broken-pipe-safe `writeln!(std::io::stderr(), ...)`, and marks `DATA_RETRY_EXHAUSTED` so subsequent lookups fail over immediately without repeating the wait. Check `/download0/ps5-agent/` if logs or `startup_error.log` do not appear under `/data/ps5-agent/`.
- **`<data_dir>/startup_error.log`, Detached `stderr`, `ps5::payload::exit`, & Startup Toasts:** On PS5 (`target_vendor = "sie"`), fatal errors during config loading (exit code `2`), Tokio runtime creation (exit code `3`), or `initialize_with_warnings()` (exit code `1`) exit cleanly via `ps5::payload::exit(...)` (`ps5_sys::payload_exit`), as does normal shutdown (`exit_code`). Before exiting on error, `report_fatal_startup_error()` (`src/main.rs`) resolves the writable data dir, appends `"{timestamp} [FATAL] [{stage}] {err}"` to `<data_dir>/startup_error.log`, emits broken-pipe-safe detached `stderr` output via `std::io::Write::write_fmt(&mut std::io::stderr(), ...)`, records `[ps5-agent][FATAL] {stage}: {err}` to `/dev/klog`, and displays a **`PS5 Agent Startup Error`** on-screen toast.
- **Malformed `config.toml` (`config.toml.broken.bak`):** Resilient loading (`load_or_create_resilient`) strips UTF-8 BOMs (`\u{feff}`) and NUL bytes, normalizes common boolean typos (`True`/`False`/`yes`/`no`/`on`/`off`/`"true"`/`"false"`), applies `#[serde(default)]` for partial files, and clamps out-of-range numeric settings (`normalize_and_clamp`). If `config.toml` still fails TOML parsing or `validate()`, the broken file is backed up to **`config.toml.broken.bak`**, a clean default `config.toml` is written, a warning is logged (`[WARN] [config] ...`) with a PS5 toast notice, and startup continues.
- **Corrupted SQLite Database (`agent.db.corrupt.bak`):** In `database.driver = "auto"` mode, `open_storage()` (`crates/ps5-agent-database/src/metrics.rs`) verifies migrations and `PRAGMA integrity_check` (`"ok"`). Transient `Busy`/`Locked` errors are retried first (`2` retries × `150ms`) and return the transient error **without** triggering `.corrupt.bak` quarantine or fallback to an empty `jsonl_db/`. Only non-lock/genuine corruption failures back up `agent.db` (and `-wal` if present) to **`agent.db.corrupt.bak`**, remove stale sidecars, and initialize a fresh SQLite database (falling back to `JsonlStorage` only if fresh SQLite initialization also fails).

---

### Verifying Process Identity, `pldmgr` Cold-Boot Autostart, & Clean Restarts (`ps5-agent.pid`)

- **`pldmgr` Processes Tab (`SYS_thr_set_name`):** On startup (`target_vendor = "sie"`), `ps5-agent.elf` calls `ps5::payload::set_process_name("ps5-agent.elf")` (FreeBSD syscall `SYS_thr_set_name = 464`, up to 19-byte `MAXCOMLEN`), and `m1-ps5-probe.elf` does likewise for `"m1-ps5-probe.elf"`, so `pldmgr` displays **`ps5-agent.elf`** instead of generic `payload.elf` (alongside `SIGPIPE` and `SIGHUP` ignored).
- **`pldmgr` Cold-Boot Autostart Checks:** Confirm `meta.json` alongside `ps5-agent.elf` (and/or in `<data_dir>`) uses `"schema": 2`, `"daemon": true`, and `"autostart": true`. Note that initial `PS5LM` probing is deferred to the background task on PS5 (`target_vendor = "sie"`), so `:8080` and `pldmgr` readiness do not block if `PS5LM` boots later. If cold-boot autostart fails, inspect `<data_dir>/startup_error.log` (check both `/data/ps5-agent/` and `/download0/ps5-agent/`), `<data_dir>/ps5-agent.pid`, `/data/ps5-agent/logs/ps5-agent.log`, and `/dev/klog`.
- **`<data_dir>/ps5-agent.pid` Boot-Scoped Singleton Lock (`<pid>:<boot_time_secs>`):**
  - `manage_pid_file()` / `manage_pid_file_with_boot_time()` (`src/startup.rs`) writes `<pid>:<boot_time_secs>` using `current_kernel_boot_time_secs()` -> `ps5::payload::kernel_boot_time_secs()` (`sysctl([CTL_KERN, KERN_BOOTTIME])` on PS5; legacy bare `<pid>` records also remain parseable by `parse_pid_file_record()`).
  - **Prior-boot PID protection:** `is_pid_record_from_prior_boot()` compares the recorded `boot_time_secs` against the current `KERN_BOOTTIME` with `BOOT_TIME_JITTER_TOLERANCE_SECS = 5` (`5s` tolerance). If the timestamps differ by more than `5s`, `ps5-agent` logs `"PID file ... belongs to a prior boot ...; treating as stale"` and deletes the stale file—preventing a recycled OS PID after power-off/reboot from blocking `pldmgr` cold-boot autostart or targeting an unrelated system process.
  - **Strict process-name ownership check:** For same-boot live PIDs (`kill(pid, 0) == 0`), `process_name_matches_ps5_agent(pid)` inspects `sysctl([CTL_KERN, KERN_PROC, KERN_PROC_PID, pid])` on FreeBSD (`/proc/<pid>/comm` on Linux) and strictly matches `ps5-agent` or `ps5_agent` (ASCII case-insensitive), explicitly **excluding** generic loader names such as `payload.elf` or `pldmgr.elf`. Only a confirmed `ps5-agent` instance receives `SIGTERM` (up to `12 × 50ms = 600ms` wait before `SIGKILL`); otherwise `ps5-agent` logs `"PID file references unrelated live process pid=...; replacing PID file"` and overwrites `ps5-agent.pid` without signalling it. `cleanup_pid_file()` removes `ps5-agent.pid` during graceful shutdown.
- **Rapid Re-Deploy / Port Reuse:** The HTTP server uses `bind_reusable_listener()` (`SO_REUSEADDR` and `SO_REUSEPORT` on Unix) so immediate restarts on `:8080` do not fail with `EADDRINUSE`. If `pldmgr` does not show `ps5-agent.elf`, check `startup_error.log`, `ps5-agent.pid`, and `/dev/klog`.

---

### Troubleshooting `401` Login & Recovering Admin Credentials

`login_handler` matches `admin_username` case-insensitively (so virtual-keyboard auto-capitalization like `Admin` still signs in as `admin`), trims accidental trailing whitespace on pasted passwords, and surfaces the server's `401` error message in the login form. Generated bootstrap passwords use an unambiguous 20-character alphanumeric alphabet (no punctuation) and are shown on first boot in a PS5 toast (`PS5 Agent :<port>\nUser: <user> | Pass: <pw>`).

If you still hit `401 Unauthorized`:
1. **Check `<data_dir>/bootstrap_credentials.txt`:** Read `/data/ps5-agent/bootstrap_credentials.txt` over FTP. While the password has not yet been rotated (`auth.password_rotated != "true"`), `ps5-agent` also accepts and self-heals `settings["auth.password_hash"]` from `bootstrap_credentials.txt` if the DB hash was missing.
2. **Set `admin_password` in `config.toml`:** Add or uncomment `admin_password = "change-me-at-least-8-chars"` under `[security]` in `config.toml` and restart `ps5-agent`. On boot (and login), `ps5-agent` hashes `admin_password` into `settings["auth.password_hash"]` using Argon2id and syncs `bootstrap_credentials.txt`.
3. **Disable Auth on a Trusted LAN (`auth_enabled = false`):**
   - **Via `config.toml`:** Set `auth_enabled = false` under `[security]` and restart `ps5-agent`.
   - **Via Dashboard Settings:** When signed in, open **Settings**, change **Authentication** (`#cfg-auth-enabled`) to **Disabled (Open LAN Access)** (or update **Admin Username** `#cfg-admin-username`), and save (`PUT /api/v1/settings` updates `HttpState::effective_config()` live without restarting).

---

### PS5 Rest-Mode Wake OS Error `163` (`network_degraded`)

**Symptom:** After resuming the PS5 from Rest Mode on Firmware `13.x`, existing browser sessions drop, `/api/v1/health` reports `"status": "degraded"` (`network_degraded: true`), the Overview pill shows **Network Degraded**, or logs contain:

```text
[WARN] [http.server] Detected PS5 rest-mode socket error 163 on accept(); rebinding listener
```

**How `ps5-agent` handles it (`crates/ps5-agent-http/src/server.rs`):**
- `ps5-agent` uses non-blocking Tokio `kevent` (`RecoveringTcpListener`) rather than blocking `accept()`, avoiding the console power-off crash during suspend.
- On wake, `is_sony_rest_mode_error()` detects `raw_os_error() == Some(163)` (as well as socket-invalidation errors like `NotConnected`, `AddrNotAvailable`, `InvalidInput`, `BrokenPipe`, or repeated transient errors).
- `RecoveringTcpListener` sets `HttpState::network_degraded = true`, emits a `StreamEvent::SystemAlert`, drops the stale `TcpListener`, and loops in `bind_with_recovery()` with exponential backoff (`250ms` initial up to `5s` max) until `TcpListener::bind` succeeds or shutdown is requested.
- Once bound and accepting connections again, `network_degraded` resets to `false` and an info `SystemAlert` is broadcast.

**Recovery steps if `network_degraded` persists:**
1. Inspect `/dev/klog` or `ps5-agent.log` for repeated `Retry bind 0.0.0.0:8080 failed: ...` messages.
2. If the PS5 kernel did not restore the network interface after wake, toggle **Connect to the Internet** off and back on in the PS5 system network settings so `bind_with_recovery()` can re-bind `0.0.0.0:8080`.
3. If the interface remains wedged or `elfldr` suspended networking, re-send `build/ps5-agent.elf` via `scripts/deploy-ps5.ps1` (persistent SQLite/JSONL state is preserved across restarts).

---

### `PS5LM` `503` and Inference Timeout Diagnostics

**How `Ps5LmProvider` (`crates/ps5-agent-inference/src/provider.rs`) reports failures:**

| Failure Mode | Internal Error / Log Signature | Common Cause | Resolution |
|---|---|---|---|
| **HTTP `503 Service Unavailable`** | `PS5LM returned 503 unavailable: ...` or `PS5LM returned 503 on /v1/models: ...` | `PS5LM` (`PPSA99575`) is running but has **no GGUF model loaded**, is still loading weights, or rejected concurrent load. | Load a Qwen GGUF model in `PS5LM`, confirm `GET /api/v1/inference/models` returns the model, and check `ps5lm_loaded_model` in `/api/v1/metrics`. |
| **Pre-response connect failure** | `pre-response connect error (attempt X/2), retrying in Yms` -> `connection to inference server failed` | `PS5LM` is not running or `inference.base_url` does not match `http://127.0.0.1:8081/v1`. | Launch `PS5LM` on the PS5; verify `inference.base_url` in `config.toml` or Dashboard **Settings**. |
| **Request timeout** | `inference request timed out` | Generation exceeded `inference.timeout_secs` (default `120s`) or `connect_timeout_secs` (default `5s`). | Reduce `inference.max_output_tokens` / `context_limit`, check `inference_p95_ms` in `/api/v1/metrics`, or increase `inference.timeout_secs` in Settings. |
| **Background health probe false** | `ps5lm_reachable: false` in `/api/v1/metrics` | `AppHttpBackend::refresh_ps5lm_health()` wraps `health_check()` in a `2s` timeout (`src/adapters.rs`). | If `PS5LM` is busy under heavy prompt eval, wait for the active turn to finish and re-check after `runtime.health_poll_interval_secs` (`15s`). |

**Concurrency & cold-boot probing behavior:**
- **Deferred SIE initial `PS5LM` probe (`src/startup.rs`):** On PS5 (`target_vendor = "sie"`), unlike host builds (which wait up to `1500ms` at boot), the initial `sync_ps5lm_capabilities()` / `refresh_ps5lm_health()` probe is deferred completely into the spawned background health-polling task (`runtime.health_poll_interval_secs`, default `15s`, minimum `5s`). As a result, HTTP `:8080` and `pldmgr` cold-boot autostart readiness never stall if `PS5LM` finishes launching after `ps5-agent.elf`; if `ps5lm_reachable` starts `false` right after cold boot, wait one polling interval (`15s`) and re-check `GET /api/v1/metrics`.
- **Inference semaphore serialization:** `agents.max_concurrent_inference` defaults to `4` (and automatically syncs to `PS5LM` `total_slots` when `auto_sync_from_ps5lm = true`); pin it to `1` on single-slot PS5 setups when strict serialization is desired. `Ps5LmProvider` enforces this limit with an async `Semaphore` so multi-agent turns serialize cleanly against PS5's single loaded model instead of racing into `503`s or shared-memory pressure.

---

### Assistant Repetition & `"Got it, Commander!"` Loop Diagnostics

**Symptom:** A Qwen model starts repeating paragraphs verbatim or echoing leaked multi-agent greetings such as `"Got it, Commander! Doing it again:"` or `"Got it, Commander! Hearing you:"` over SSE.

**Defense-in-depth & what to inspect (`crates/ps5-agent-inference/src/parser.rs`, `provider.rs`, `crates/ps5-agent-runtime/src/runtime.rs`):**

1. **Omitting `name` on single-agent `user`/`assistant` messages:** In single-agent chats, attaching a `"name"` field (such as `"commander"`) to `user`/`assistant` messages induces chat templates to role-play self-replies (`"Got it, Commander!"`). `message_peer_name()` (`runtime.rs`) only populates `name` for `ChatRole::Tool` outputs or peer `ChatRole::Assistant` turns in `ConversationMode::Group` chats (`None` on single-agent `user`/`assistant` turns), while `normalize_conversation_messages()` (`parser.rs`) strips `name` on all non-`"tool"` roles and `build_chat_completions_payload()` (`provider.rs`) serializes `"name"` strictly on `"tool"` role messages.
2. **Sampler anti-repetition defaults (`dry_multiplier = 0.8`, `repeat_last_n = 256`):** Both `AppConfig` (`crates/ps5-agent-config/src/lib.rs`) and `config/config.ps5-slim-1340.toml` default to `inference.repeat_last_n = 256` and `inference.dry_multiplier = 0.8` (with `dry_base = 1.75`, `dry_allowed_length = 2`, `dry_penalty_last_n = -1`). `apply_llamacpp_sampling_fields()` (`provider.rs`) sends `repeat_last_n` and (whenever `dry_multiplier > 0.0`) the DRY sampler parameters to `PS5LM` / `llama.cpp`. Verify both values in **Settings -> KV Cache & llama.cpp / PS5LM Sampling** or `GET /api/v1/settings` if an older `config.toml` has `dry_multiplier = 0.0`.
3. **Incremental SSE early-abort (`StreamingRepetitionGuard` in `SseAccumulator`):** Every streamed `delta` passes through `SseAccumulator::push_content_delta()` -> `StreamingRepetitionGuard::push_delta()` (and `finalize_loop_guard_eof()` -> `finish()`). It monitors **visible prose outside** `<think>...</think>`, `<seed:think>...</seed:think>`, `<tool_call>...</tool_call>`, and fenced code blocks (` ``` ` / `~~~`) via `detect_prose_repetition()` for:
   - `>= 3` consecutive repeated non-empty normalized lines (`>= 20` chars),
   - `>= 2` repeated paragraphs or multi-paragraph cycles (`>= 40` chars, both raw and after stripping spurious greeting prefixes),
   - Repeated complete sentences (`>= 2` repeats of `>= 40` chars or `>= 3` consecutive repeats of `>= 20` chars, skipping Markdown table rows), or
   - Contiguous verbatim suffix repeats on the active run (`>= 40` chars).
   When triggered, it returns `StreamingLoopDecision::AbortRepeatedLoop { truncated_clean_visible }`; `SseAccumulator` halts consumption of remaining SSE loop chunks (`aborted_loop_clean`), emits clean pre-loop text if nothing was streamed yet, and finalizes the response with `finish_reason = "repetition_abort"`.
4. **Post-stream & non-streaming deduplication (`truncate_verbatim_prose_loops`):** `parse_model_output()` (and `SseAccumulator::into_response()` on aborted streams) invokes `truncate_verbatim_prose_loops()` (public alias: `deduplicate_repeated_prose_blocks()`), using `strip_spurious_headers_in_paragraph()` / `match_spurious_greeting_prefix()` to strip leaked loop headers (`"Got it, Commander! Doing it again:"`, `"Got it, Commander! Hearing you:"`, `"Understood, <role>!"`) and deduplicate repeated prose while preserving `<think>`, `<tool_call>`, and fenced code blocks verbatim.

**Verification:** Toggle **Show raw output** in chat to confirm no `<think>` / fenced blocks were clipped, inspect debug logs (`logging.level = "debug"`) or `InferenceResponse.finish_reason` (`"repetition_abort"`), and confirm `dry_multiplier = 0.8` / `repeat_last_n = 256` under `[inference]`.

---

### SQLite `Busy`/`Locked` Resilience, Rollback Mode (`wal_mode = false`), & Switching to `database.driver = "jsonl"`

On PS5 (`target_vendor = "sie"`), `config/config.ps5-slim-1340.toml` sets `wal_mode = false` (and `busy_timeout_ms = 5000`, `cache_size_kb = 4000`), and `SqliteStorage` (`crates/ps5-agent-database/src/sqlite.rs`) enforces `PRAGMA journal_mode = TRUNCATE;` followed by `PRAGMA locking_mode = NORMAL;` even if an older `config.toml` still has `wal_mode = true`. This avoids `.db-shm` shared-memory `mmap(MAP_SHARED)` mappings on PS5 `/data`. Note that on `SqliteStorage` in `TRUNCATE` mode, `POST /api/v1/system/checkpoint` remains safe to call (running `PRAGMA incremental_vacuum;` — or switching to incremental auto-vacuum via `VACUUM` if needed — plus `PRAGMA wal_checkpoint(TRUNCATE); PRAGMA optimize; PRAGMA shrink_memory;`).

**Transient `Busy` / `Locked` startup resilience:**
- **`SqliteStorage::open()` (`crates/ps5-agent-database/src/sqlite.rs`):** Before opening in rollback mode (`!effective_wal`), if a stale `agent.db-shm` exists while `-wal` is missing or empty, it logs `"Removing stale SQLite SHM file before open"` and removes `-shm`. `open()` then attempts `open_and_configure()` (`PRAGMA busy_timeout = 5000;` default) up to `MAX_OPEN_ATTEMPTS = 5` times with `OPEN_BACKOFF = 150ms`. If opening hits a `-shm` failure mid-loop, it removes the stale `-shm` and retries; on transient errors (`ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked | ErrorCode::CannotOpen` via `is_transient_rusqlite_open_error()`), it logs `"SQLite open/pragma transient error on attempt <N>/5 (...); retrying in 150ms"` and backs off `150ms`.
- **`open_storage()` no-false-quarantine protection (`crates/ps5-agent-database/src/metrics.rs`):** In `database.driver = "auto"` mode, if `try_open_sqlite_checked()` fails and `is_transient_busy_or_locked_error()` matches (`"database is locked"`, `"database table is locked"`, `"database busy"`, `"database locked"`, `"sqlite_busy"`, `"sqlite_locked"`), `open_storage()` performs `2` additional retries spaced `150ms` apart (`"SQLite initialization hit transient busy/locked error (...); retry <1|2>/2 without quarantining DB"`). If all attempts remain locked/busy, `open_storage()` returns that transient error directly **without** calling `open_auto_corrupt_or_fallback()`—preventing a temporarily locked healthy `agent.db` from being falsely renamed to `agent.db.corrupt.bak` or replaced by an empty `jsonl_db/`.
- **What to check on a busy/locked failure:** Look for a lingering process holding `database/agent.db` (inspect `<data_dir>/ps5-agent.pid` and `pldmgr`'s **Processes** tab) before re-deploying.

By default, `database.driver = "auto"` opens `SqliteStorage` and falls back to `.corrupt.bak` quarantine + fresh SQLite + `JsonlStorage` only on non-transient/corruption failures. If you suspect `/data` filesystem locking or latency issues, force JSONL mode explicitly to isolate storage behavior:

1. Edit `/data/ps5-agent/config.toml` (or `/download0/ps5-agent/config.toml`) and set:
   ```toml
   [database]
   driver = "jsonl"
   ```
2. Restart `ps5-agent` (`POST /api/v1/system/shutdown` or `SIGTERM`, then re-deploy `build/ps5-agent.elf`). Note that `database.driver` is read at startup and is **not** hot-reloaded by `PUT /api/v1/settings`.
3. Verify the active driver in the startup log (`Storage initialized (driver=jsonl, integrity=ok)`) or via `GET /api/v1/metrics` (`"storage_driver": "jsonl"`).
4. Inspect `<data_dir>/jsonl_db/` (`state.json` plus append-only `messages.jsonl`, `task_events.jsonl`, `agent_messages.jsonl`, `tool_executions.jsonl`, and `audit_logs.jsonl`). On startup, `integrity_check()` validates `state.json` and every non-empty line in each `*.jsonl` file.

> [!NOTE]
> Switching between `"sqlite"` and `"jsonl"` changes which backend directory/file is read (`database/agent.db` vs `jsonl_db/`); records created in one backend are not automatically migrated to the other.

---

### Per-Chat & Per-Agent Cancellation Diagnostics

`ps5-agent` scopes task cancellation so stopping work in one chat room or for one specialist does not interrupt unrelated conversations or sibling agents:

- **Conversation-wide cancellation (`POST /api/v1/conversations/{id}/cancel`):** Clicking **Cancel Task** / **Cancel All** (`#chat-cancel-task-btn` in `#chat-active-task-bar`) calls `POST /api/v1/conversations/{id}/cancel` with `{}`. `TaskManager::cancel_conversation_tasks()` cancels only non-terminal tasks (`PENDING`, `ASSIGNED`, `RUNNING`, `WAITING`) matching `conversation_id`, clears UI stream buckets for that conversation, and appends a `conversation.cancel_tasks` audit log entry (`cancelled_count`, `agent_id: null`).
- **Per-agent cancellation inside a conversation:** Supplying `{"agent_id": "<agent-id>"}` to `POST /api/v1/conversations/{id}/cancel` cancels only that agent's tasks in the conversation while allowing other agents in the same group chat to finish. In the UI, clicking **`Stop <Agent> ×`** (`.btn-stop-agent`) on an individual chip in `#chat-task-agents` calls `POST /api/v1/tasks/{task_id}/cancel` for that specific task.
- **What `TaskManager::cancel_task()` (`crates/ps5-agent-runtime/src/task_manager.rs`) checks & cleans up:**
  1. Cancels the task's `CancellationToken` and marks `status = CANCELLED` with `task_cancelled` task event and `TaskUpdated` SSE event.
  2. Removes and wakes any waiter in `expedite_notifiers` so a queued task cannot transition from `PENDING` to `RUNNING` (`await_task_slot_or_expedite()` re-checks `cancel.is_cancelled()` after waking).
  3. Cascades cancellation to active/pending child subtasks.
- **Verification:** Check **Tasks -> Inspect** (`GET /api/v1/tasks/{id}/events`) for `task_cancelled`, and check **Logs -> Security Audit Logs** (`GET /api/v1/audit-logs`) for `conversation.cancel_tasks`.

---

### Queue Expedite Debugging (`immediate: true` & `POST /api/v1/tasks/{id}/expedite`)

When `agents.max_active_tasks` (`> 0`; default `0` = unlimited in both `AppConfig` and `config/config.ps5-slim-1340.toml`) queues additional tasks in `PENDING` (`Pending (Queued)`), two paths bypass the queue:

- **At dispatch (`Send Now` / `immediate: true`):** Clicking **Send Now** (`#chat-send-immediate-btn`) sends `POST /api/v1/conversations/{id}/messages` with `"immediate": true`. Inside `await_task_slot_or_expedite()` (`crates/ps5-agent-runtime/src/runtime.rs`), `immediate: true` skips acquiring `task_semaphore` and starts the task immediately (unless already cancelled).
- **While queued (`Run Immediately` / `POST /api/v1/tasks/{id}/expedite`):** Clicking **Run Immediately** (`.btn-run-now`) on a `PENDING` agent chip in `#chat-task-agents` invokes `POST /api/v1/tasks/{id}/expedite`:
  - `TaskManager::expedite_task()` updates `progress_note` to `"Expedited — starting immediately"`, appends a `task_expedited` task event, emits `TaskUpdated`, and signals the task's `expedite_notifiers` `Notify` handle to wake `await_task_slot_or_expedite()`.
  - `expedite_task` (`routes.rs`) records a `task.expedite` entry in Security Audit Logs.
- **Why an expedited task may still wait for tokens on PS5:** Expediting bypasses the outer `task_semaphore` (`max_active_tasks`) so the task transitions immediately to `RUNNING`, **but** `Ps5LmProvider::acquire_permit()` still enforces `agents.max_concurrent_inference` (`4` default, auto-synced to `PS5LM` `total_slots`, or `1` when explicitly pinned) so multiple running tasks do not overload `PS5LM`'s single loaded model.
- **Debugging checklist:**
  1. Open **Tasks -> Inspect** (`GET /api/v1/tasks/{id}/events`): confirm the `task_expedited` event and `"Expedited — starting immediately"` progress note were recorded.
  2. If `POST /api/v1/tasks/{id}/expedite` returns a terminal task (`COMPLETED`, `FAILED`, or `CANCELLED`) unchanged, the task had already finished or been cancelled before the expedite call arrived (`expedite_task()` is a no-op on terminal tasks).
  3. Confirm `TaskManager::release_token()` cleans up both `tokens` and `expedite_notifiers` after completion (`recover_on_startup()` also clears stale entries at boot).
