# PS5 Agent Installation & Deployment Guide

This guide covers deploying and configuring `ps5-agent` (`build/ps5-agent.elf`) on a jailbroken **PlayStation 5 Slim** running **Firmware `13.40`** (`0x13400000`).

---

## 1. Prerequisites (PS5 Slim / Firmware 13.40)

Before sending `build/ps5-agent.elf`, ensure your console meets all of the following requirements:

1. **Active PS5 Firmware `13.40` Jailbreak**
   - The console must be on your local area network (LAN) with an active userland jailbreak.
   - Note that PS5 jailbreaks are non-persistent across full cold reboots; after a power-off, re-run the exploit before restarting `elfldr`, `PS5LM`, and `ps5-agent.elf`.
2. **ELF Loader (`ps5-payload-dev/elfldr` `v0.27`)**
   - `elfldr` must be running on the PS5 and listening on TCP port `9021` (default payload port).
3. **Local Inference Server (`PS5LM` `v0.11.0+`)**
   - `PS5LM` (running as native app `PPSA99575` on Vulkan GPU, or compatible local server) must be running on the console and listening on `http://127.0.0.1:8081` (`base_url = "http://127.0.0.1:8081/v1"`).
   - A **Qwen GGUF** model must already be loaded in `PS5LM` before starting agent tasks.
   - Default `config/config.ps5-slim-1340.toml` connects with `api_mode = "chat_completions"` and `model = "qwen-loaded"` (also supported: `responses`, `anthropic_messages`, `completions`, `system_one`).
4. **Built Payload ELF**
   - Cross-compile `build/ps5-agent.elf` (see [BUILDING.md](BUILDING.md) for toolchain and build commands).

---

## 2. On-Console Directory Layout & Automatic Fallback

On first boot, `ps5-agent` automatically creates its runtime directory tree (`ensure_directories` in `src/startup.rs`, `AppConfig::load_or_create_resilient` in `crates/ps5-agent-config/src/lib.rs`, and `seed_workspace_brain_files` in `crates/ps5-agent-runtime/src/brain.rs`), including `database/`, `logs/`, `skills/`, `plugins/`, `mcp/`, `tools/`, `backups/`, and the full `workspace/` Brain & sandbox hierarchy—you do not need to pre-create folders via FTP unless you are pre-staging a custom `config.toml` or extensions.

### Default Layout (`/data/ps5-agent/`)

```text
/data/ps5-agent/
├── config.toml                       # Auto-created with PS5 defaults if missing
├── config.toml.broken.bak            # Created automatically if a broken config.toml is recovered
├── config.toml.v1.bak                # Backup created automatically when migrating legacy v1 config
├── bootstrap_credentials.txt         # Generated once on first boot (mode 0600 on Unix)
├── ps5-agent.pid                     # Active payload PID file for clean singleton restarts
├── startup_error.log                 # Fatal pre-init startup diagnostics (if boot fails)
├── meta.json                         # Optional release metadata (verified/synced if present)
├── database/
│   └── agent.db                      # SQLite database (schema v3; TRUNCATE mode on PS5 or JSONL)
├── logs/
│   └── ps5-agent.log                 # Rotating runtime log (default max 2 MiB)
├── mcp/                              # Streamable-HTTP MCP servers (`<id>.json` or `<id>/mcp.json`)
├── skills/                           # Installed `SKILL.md` skills (`<data_dir>/skills/<slug>/SKILL.md`)
├── plugins/                          # Installed plugins (`<data_dir>/plugins/<slug>/plugin.json`)
├── tools/                            # Custom declarative tools (`<data_dir>/tools/<id>.json`)
├── workspace/                        # Default sandboxed root in tools.allowed_paths
│   ├── AGENTS.md                     # Operating instructions, role boundaries, conventions
│   ├── SOUL.md                       # Agent persona, communication style, behavioral rules
│   ├── IDENTITY.md                   # Runtime identity & PS5 hardware/PS5LM profile
│   ├── USER.md                       # User preferences & workflow notes
│   ├── TOOLS.md                      # Workspace tool, sandbox & PS5LM usage notes
│   ├── MEMORY.md                     # Curated long-term markdown memory
│   ├── HEARTBEAT.md                  # Periodic task checklist (`#` comment lines are ignored)
│   ├── .agents/
│   │   ├── rules/
│   │   │   └── ps5-sandbox.md        # Default PS5 workspace safety rule
│   │   └── workflows/
│   │       └── daily-brief.md        # Default markdown slash workflow (`/daily-brief`)
│   ├── memory/                       # Daily/named markdown notes (`YYYY-MM-DD.md`)
│   ├── sandbox/
│   │   ├── downloads/                # Downloaded assets inside sandbox
│   │   ├── snapshots/                # Stateful browser text/DOM snapshots
│   │   └── tmp/                      # Temporary workspace scratch files
│   ├── artifacts/                    # Agent-written artifacts (`write_artifact`)
│   └── uploads/                      # Non-image chat file attachments (`uploads/<id>_<name>`)
└── backups/                          # Reserved directory for database backups/exports
```

- **Non-destructive Brain seeding (`seed_workspace_brain_files`):** Runs on every startup (`src/startup.rs`) against the primary workspace root (`tools.allowed_paths[0]`, defaulting to `<data_dir>/workspace/`). Existing files are **never overwritten**, symlinks/path traversal (`..`, backslashes, NUL bytes) are rejected, and Brain files are capped at `MAX_BRAIN_FILE_BYTES = 524288` (`512 KiB`).
- **Prompt Injection & Heartbeat:** When `[sandbox].inject_brain_files = true` (default), non-empty Brain files and `.agents/rules/*.md` are injected into agent system prompts within `max_brain_context_chars` (`4000` default). When `[sandbox].heartbeat_interval_secs > 0` (default `0` = off), the background scheduler checks `HEARTBEAT.md` for non-comment (`#`) instructions.

### Automatic `/download0/ps5-agent` Fallback

Some HEN/jailbreak environments restrict write access to `/data`. During startup (`load_effective_config`), `resolve_writable_data_dir()` probes the preferred `data_dir` by creating the directory and writing/removing a `.write_probe` file:

- If `/data/ps5-agent` is writable, it is used as `data_dir`.
- If `/data` is restricted on PS5 (`target_vendor = "sie"`), `resolve_writable_data_dir()` automatically falls back to `/download0/ps5-agent` and updates `database.path`, `logging.log_dir`, and `tools.allowed_paths` (`workspace/`) to point under `/download0/ps5-agent`.
- You can also override the base directory at launch via `PS5_AGENT_DATA_DIR` or `--data-dir <PATH>` (and config location via `--config <PATH>`).

---

## 3. Deploying `build/ps5-agent.elf` to the PS5

Send `build/ps5-agent.elf` directly to `elfldr` on TCP port `9021`. Half-closing the write side of the TCP connection signals `elfldr` to start executing the ELF.

### Windows (PowerShell)

```powershell
powershell -ExecutionPolicy Bypass -File scripts/deploy-ps5.ps1 -TargetHost <PS5-IP> -Port 9021
```

Useful optional flags in `scripts/deploy-ps5.ps1`:
- `-ElfPath <path>` (defaults to `build\ps5-agent.elf`)
- `-Follow` — keep streaming `elfldr` stdout/stderr indefinitely instead of disconnecting after `-StreamSeconds` (default `5` seconds).
- Environment variables `PS5_HOST` and `PS5_PORT` are honored if `-TargetHost` / `-Port` are omitted.

### Linux / macOS

```bash
PS5_HOST=<PS5-IP> PS5_PORT=9021 ./scripts/deploy-ps5.sh build/ps5-agent.elf
```

`scripts/deploy-ps5.sh` automatically selects `$PS5_PAYLOAD_SDK/bin/prospero-deploy` if installed, or falls back to `python3` (with `SHUT_WR`) or `nc -w 3`. You can also run `make deploy PS5_HOST=<PS5-IP> PS5_PORT=9021`.

### Raw `nc` / `socat`

If deploying without the helper scripts, send the raw ELF bytes and shut down the write half of the socket:

```bash
# OpenBSD / Nmap netcat (-N closes after EOF on stdin)
nc -N <PS5-IP> 9021 < build/ps5-agent.elf

# GNU netcat (with short idle timeout)
nc -w 3 <PS5-IP> 9021 < build/ps5-agent.elf

# socat
socat -u FILE:build/ps5-agent.elf TCP:<PS5-IP>:9021
```

When startup succeeds on the PS5 (with `logging.enable_toast = true`):
- On first-boot credential generation, the PS5 shows an on-screen toast with the port, username, and generated bootstrap password (`PS5 Agent :<port>\nUser: <username> | Pass: <password>`).
- On subsequent boots, it shows `PS5 Agent ready on 0.0.0.0:8080`.
- In `pldmgr` (Processes tab), the payload identifies as **`ps5-agent.elf`** (via `SYS_thr_set_name`, rather than `payload.elf`).
- Sending `SIGTERM` (from `pldmgr` or a new `ps5-agent.elf` launch reading `ps5-agent.pid`) triggers a graceful database checkpoint and clean shutdown. If startup hits a fatal configuration or initialization error, check `/data/ps5-agent/startup_error.log` (or the PS5 Startup Error toast).

---

## 4. First Login, Password Rotation & Configuration

### Retrieve Initial Bootstrap Credentials

On first launch, when neither `security.password_hash` / `security.admin_password` (in `config.toml`) nor `settings["auth.password_hash"]` (in the database) is set, `ensure_bootstrap_credentials_with_report()` (`crates/ps5-agent-http/src/auth.rs`):

1. Generates a random 20-character **alphanumeric** password (no punctuation, avoiding ambiguous characters) using `OsRng`.
2. Stores only its **Argon2id** PHC hash (`argon2_memory_kib = 8192` on PS5) in `settings["auth.password_hash"]` inside the database.
3. Writes the plaintext credentials **once** to `/data/ps5-agent/bootstrap_credentials.txt` (or `/download0/ps5-agent/bootstrap_credentials.txt` if fallback triggered) with `0600` permissions and displays them in the first-boot PS5 toast:

```ini
# PS5 Agent Initial Bootstrap Credentials
# Delete this file after logging in and rotating the admin password.
username=admin
password=<20-char-alphanumeric-password>
```

You can read the password from the first-boot PS5 toast or over FTP from `bootstrap_credentials.txt`. Note that passwords and hashes are never printed to `ps5-agent.log` or `/dev/klog`.

### Sign In from Desktop or Mobile Browsers on the LAN

Open the web dashboard from any device on the same LAN:

```text
http://<PS5-IP>:8080
```

- Enter `username` (`admin` by default; matched case-insensitively so mobile auto-capitalization like `Admin` still works) and the bootstrap `password` (or your configured `admin_password`).
- To disable authentication on a trusted home LAN, set `auth_enabled = false` in `[security]` of `config.toml` or toggle **Auth Enabled** in **Settings** in the Dashboard.
- By default, `server.allow_lan_only = true` restricts access to loopback (`127.0.0.1`, `::1`), RFC1918 private IPv4 ranges (`10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`), and IPv6 ULA (`fc00::/7`). Non-LAN clients receive `403 Forbidden`.
- Login establishes an `HttpOnly; SameSite=Strict` cookie (`ps5_agent_session`, default TTL `24` hours) and pairs it with an `X-CSRF-Token` for mutating requests.
- Login attempts are rate-limited (`login_rate_limit_per_minute = 10` per IP).

### Rotate the Admin Password

Rotate the generated bootstrap password immediately after your first sign-in:

- **Via Web Dashboard:** Open **Settings** -> **Change Admin Password**, enter your current bootstrap password and a new password (`8` to `256` characters), and click **Update Password**.
- **Via API (`POST /api/v1/auth/password`):**

```bash
# 1. Authenticate and save session cookie + extract csrf_token
CSRF=$(curl -s -c cookies.txt -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"<BOOTSTRAP_PASSWORD>"}' \
  http://<PS5-IP>:8080/api/v1/auth/login | python3 -c "import sys,json; print(json.load(sys.stdin)['csrf_token'])")

# 2. Rotate password (include Cookie + X-CSRF-Token + matching Origin)
curl -s -b cookies.txt \
  -H "Content-Type: application/json" \
  -H "X-CSRF-Token: ${CSRF}" \
  -H "Origin: http://<PS5-IP>:8080" \
  -d '{"current_password":"<BOOTSTRAP_PASSWORD>","new_password":"<NEW_STRONG_PASSWORD>"}' \
  http://<PS5-IP>:8080/api/v1/auth/password
```

After rotating the password, delete `/data/ps5-agent/bootstrap_credentials.txt` (or `/download0/ps5-agent/bootstrap_credentials.txt`) from the console.

### Customizing `config.toml`

You can copy [config/config.ps5-slim-1340.toml](../config/config.ps5-slim-1340.toml) to `/data/ps5-agent/config.toml`, edit `/data/ps5-agent/config.toml` over FTP and restart `ps5-agent.elf` (`SIGTERM` via `pldmgr` or re-deploying the ELF), or update settings live from **Settings** in the Web Dashboard.

The config loader (`load_or_create_resilient`) is designed for safe manual editing:
- **UTF-8 BOM & partial configs:** Leading UTF-8 BOMs (common from Windows editors), NUL bytes, and partial/minimal TOML files are accepted cleanly with defaults filled in via `#[serde(default)]`, and out-of-range numeric tuning values are clamped with log warnings rather than crashing startup.
- **Automatic syntax recovery:** If an edited `config.toml` contains invalid TOML syntax or an invalid URL, `ps5-agent` backs up the broken file to `config.toml.broken.bak`, restores a clean default `config.toml`, emits a PS5 toast warning, and continues booting (also check `startup_error.log` if another pre-init check fails).
- **Optional plaintext password in config:** You may set `admin_password = "..."` (at least 8 characters) under `[security]` in `config.toml` instead of using `bootstrap_credentials.txt`, or set `auth_enabled = false` on a trusted LAN.

Key sections in `config/config.ps5-slim-1340.toml`:

| Section / Key | Default on PS5 Slim `13.40` | Purpose |
|---|---|---|
| `data_dir` | `"/data/ps5-agent"` | Base directory (`resolve_writable_data_dir` falls back to `/download0/ps5-agent` if restricted). |
| `[inference].base_url` | `"http://127.0.0.1:8081/v1"` | Local `PS5LM` endpoint (override at launch with `--ps5lm-url <URL>`). |
| `[inference].model` | `"qwen-loaded"` | Model identifier sent to `PS5LM` (auto-updated when `auto_sync_from_ps5lm = true`). |
| `[inference].api_mode` | `"chat_completions"` | One of `chat_completions`, `responses`, `anthropic_messages`, `completions`, `system_one`. |
| `[inference].thinking_level` | `"medium"` (code default) | Default reasoning depth (`off`, `low`, `medium`, `high`, `max`). |
| `[inference].auto_sync_from_ps5lm` / `auto_sync_interval_secs` | `true` / `30` | Probes `PS5LM` on boot and every `30s` (`0` disables background sync) to align model, `context_limit`, and slots. |
| `[inference].prefill_kv_cache` / `cache_prompt` | `true` / `true` | Enables background KV prefill (`n_predict: 0`) and prompt prefix KV reuse on `PS5LM` / `llama.cpp` (see [SETTINGS_REFERENCE.md](SETTINGS_REFERENCE.md) for all 33 KV/sampling keys). |
| `[inference].context_limit` / `max_output_tokens` | `8192` / `1024` | Token budgets (`context_limit >= 512` or `0` for auto, `64 <= max_output_tokens < context_limit`). |
| `[server].bind_host` / `port` | `"0.0.0.0"` / `8080` | HTTP bind address and port (override at launch with `--bind <HOST:PORT>`). |
| `[server].upload_max_body_bytes` | `67108864` (64 MiB default) | Max payload size for file/image uploads (`0 = unlimited`). |
| `[server].allow_lan_only` | `true` | Rejects non-LAN peer IPs at the HTTP boundary (`403 Forbidden`). |
| `[database].driver` / `path` | `"auto"` / `"/data/ps5-agent/database/agent.db"` | `"auto"`, `"sqlite"`, or `"jsonl"` (see [DEBUGGING.md](DEBUGGING.md) for JSONL isolation). |
| `[database].wal_mode` | `false` | Uses SQLite rollback `TRUNCATE` mode on PS5 to avoid `.db-shm` `mmap(MAP_SHARED)` crashes on `/data` across restarts. |
| `[database].retention_days` / `max_rows_per_table` | `30` / `10000` (code defaults) | Startup and periodic (`600s`) DB retention pruning (`0 = unlimited`). |
| `[security].auth_enabled` / `admin_username` | `true` / `"admin"` | Enables/disables login (`auth_enabled = false` skips login; `admin_username` matches case-insensitively). |
| `[security].admin_password` | unset (commented) | Optional explicit admin password (`>= 8` chars) in `config.toml`; synced to Argon2id storage on startup. |
| `[security].require_approval_for_mutating` | `true` (`config.ps5-slim-1340.toml` preset) / `false` (`AppConfig` default) | When `true` with `sandbox.approval_policy = "full_auto"`, treats interactive mutating tool calls as `on_request` approval-gated. |
| `[security].argon2_memory_kib` | `8192` | Argon2id memory cost (8 MiB on PS5 to keep RSS lean). |
| `[agents].max_concurrent_inference` | `4` (`config.ps5-slim-1340.toml` and `AppConfig` default) | Bounds concurrent LLM calls (`0 = unlimited`); `PS5LM` slot sync updates this automatically when `auto_sync_from_ps5lm = true` and value is `0`/`4`. |
| `[tools].allowed_paths` | `["/data/ps5-agent/workspace"]` | Sandboxed directories accessible to file tools (must contain at least one path). |
| `[tools].block_private_lan_web_requests` | `true` | Blocks web tools from SSRF against localhost/LAN hosts (cloud metadata IPs are always blocked). |
| `[sandbox].mode` / `approval_policy` | `"full_sandbox"` / `"full_auto"` (code defaults) | Execution isolation (`read_only`, `workspace_write`, `full_sandbox`) and approval policy (`full_auto`, `on_request`, `always_ask`, `never`); `allow_host_shell = false` uses the PS5-safe virtual shell, and `inject_brain_files = true` (`max_brain_context_chars = 4000`, `heartbeat_interval_secs = 0`) controls Brain prompts/heartbeat. |
| `[logging].enable_klog` / `enable_toast` | `true` / `true` | Streams logs to `/dev/klog` and shows PS5 on-screen startup toasts. |
| `[runtime].tokio_worker_threads` / `rest_mode_recovery` | `2` / `true` | Caps Tokio threads (clamped `1..=3` on PS5) and enables rest-mode wake listener recovery. |

For the full list of `0 = unlimited` limits, `thinking_level` options, `[sandbox]` / `PS5_AGENT_*` overrides, PS5LM sync settings, `llama.cpp` sampling/KV-cache parameters, and database retention pruning parameters, see [SETTINGS_REFERENCE.md](SETTINGS_REFERENCE.md).

### Updating `ps5-agent.elf` & `meta.json` (In-Place Auto-Upgrade)

You do **not** need to wipe `/data/ps5-agent/` (or `/download0/ps5-agent/`) when upgrading `ps5-agent.elf` and `meta.json`. Replacing the old `.elf` + `meta.json` with a newer release triggers automatic detection and migration on startup (`src/startup.rs`):

- **Live Process Takeover (`manage_pid_file`):** Detects any older running `ps5-agent` process via `ps5-agent.pid`, sends `SIGTERM` (and `SIGKILL` after `600ms` if needed), and rebinds the port cleanly via `SO_REUSEADDR`/`SO_REUSEPORT`.
- **Automatic Release Stamp Detection (`compute_release_stamp` & `installed_release_stamp`):** On every boot, `compute_release_stamp()` computes a SHA-256 stamp over `CARGO_PKG_VERSION`, `CURRENT_SCHEMA_VERSION`, `BUILTIN_AGENTS_SCHEMA_VERSION`, `builtin_agents_fingerprint()`, embedded dashboard assets (`INDEX_HTML`, `STYLES_CSS`, `APP_JS`), and—when `std::env::current_exe()` resolves to a regular file `<= 64 MiB`—the running executable's SHA-256 (raw TCP `elfldr` payloads with no disk path still compute a valid stamp from version/schema/agents/UI assets). Comparing against `settings["installed_release_stamp"]` detects in-place `.elf` upgrades (including pre-stamp legacy installs).
- **Sibling `meta.json` Verification & Sync (`verify_or_sync_meta_json`):** Checks deduplicated `meta.json` candidates next to `ps5-agent.elf` (`<exe_dir>/meta.json`) and in `<data_dir>/meta.json` where `"id": "ps5-agent"` or `"filename": "ps5-agent.elf"`. Logs the detected `version` and `checksum`, and—when running `ps5-agent.elf` and either `checksum` or `version` (`v<CARGO_PKG_VERSION>`) differs—atomically rewrites `checksum`, `version`, and `last_update` (`YYYY-MM-DD`) via `meta.json.tmp` + `sync_all` + `rename` (logging a non-fatal warning if the path is read-only).
- **SQLite Schema Migration:** Runs `run_migrations()` (`CURRENT_SCHEMA_VERSION = 3`, adding `sort_order` on `conversations` for persistent sidebar ordering) and integrity checks automatically while keeping all existing conversations, credentials, memories, and tasks intact.
- **Built-in Agent Upgrades (`upgrade_builtin_agents`):** Uses `BUILTIN_AGENTS_SCHEMA_VERSION` (`"5"`) and `builtin_agents_hash` to upgrade unmodified built-in prompts, migrate legacy limits to `0`/`0` unlimited defaults, and merge new wildcards/tool groups while preserving user-edited prompts, enabled/disabled state, and custom agents (`POST /api/v1/agents/enable-all` / **Boot / Enable All Agents** in **Agents** can re-enable all agents at once).
- **Tool Specs & Extensions Refresh:** Upserts all `41` built-in tool specs (preserving runtime toggles), seeds any missing workspace Brain files via `seed_workspace_brain_files`, and loads extensions and custom tools from `<data_dir>/{mcp,skills,plugins,tools}` (auto-migrating any legacy `mcp_servers.json` entries into `<data_dir>/mcp/<id>.json`).
- **Legacy `config.toml` Migration:** Detects legacy v1 `config.toml` files without schema v2+ keys (`thinking_level` / `auto_sync_from_ps5lm`), backs up the original to `config.toml.v1.bak`, backfills new keys and unlimited (`0`) defaults while preserving passwords/custom settings, and records `installed_config_schema_version = "3"` (`CONFIG_SCHEMA_VERSION`).
- **Audit Record & Upgrade Toast:** Persists `installed_release_stamp`, `installed_version`, and `last_upgraded_at`, appends a `system.elf_upgrade` audit entry, and shows a PS5 toast (`PS5 Agent updated & ready on <host>:<port>`) when an upgrade is detected.
- **PS5LM Capability Sync:** Immediately probes `PS5LM` (when `auto_sync_from_ps5lm = true`) to update loaded model, `context_limit`, and slot concurrency.

---

## 5. PS5LM Auto-Sync

When `inference.auto_sync_from_ps5lm = true` (default), `ps5-agent` probes your `PS5LM` server (`inference.base_url` or `--ps5lm-url <URL>`):

- **On Startup (`src/startup.rs`):** Calls `sync_ps5lm_capabilities()` immediately after runtime initialization, probing `/props`, `/slots`, `/v1/models`, and `/health`.
- **Periodic Background Sync:** Runs every `inference.auto_sync_interval_secs` (default `30` seconds; set `0` to disable background polling while keeping startup sync).
- **On-Demand Sync in the Dashboard:** Open **Settings** and click **Sync Settings from PS5LM Now** to probe `PS5LM` immediately and inspect the live capabilities summary (loaded model, `n_ctx`, total slots, vision support, thinking support, and last sync timestamp).
- **Synced Fields:** When `PS5LM` reports capabilities, `ps5-agent` aligns `inference.model`, `inference.context_limit`, and `agents.max_concurrent_inference` (slot count) in memory so prompts and concurrency match the currently loaded GGUF model.

---

## 6. Extensions, Custom Tools & Workspace Files

### Installing Skills, MCP Servers, Plugins & Custom Tools

`<data_dir>/{mcp,skills,plugins,tools}` directories are created automatically on startup under `<data_dir>` (typically `/data/ps5-agent/` or `/download0/ps5-agent/`). You can drop files into them over FTP or manage them from the Dashboard **Extensions** and **Tools** tabs:

- **Skills (`SKILL.md`):**
  - On disk: Place `<data_dir>/skills/<slug>/SKILL.md` (with optional `scripts/`, `references/`, `examples/`, and `resources/` subdirectories). Workspace-scoped skills under `<workspace>/.agents/skills/<slug>/SKILL.md` are also discovered.
  - Via Dashboard: Open **Extensions** -> **+ Import Skill** to paste or upload a `SKILL.md` directly.
- **MCP Servers (`<data_dir>/mcp/`):**
  - On disk: Place per-server JSON definitions at `<data_dir>/mcp/<id>.json` or `<data_dir>/mcp/<id>/mcp.json` (workspace `<workspace>/.agents/mcp/*.json` is also scanned). Any legacy `<data_dir>/mcp_servers.json` entries are automatically migrated into `<data_dir>/mcp/<id>.json` on startup (`McpManager::discover_all`). Set `trust_local = true` if connecting to a loopback/LAN MCP server.
  - Via Dashboard: Open **Extensions** -> **+ Add MCP Server** to add, toggle, sync, or delete Streamable-HTTP JSON-RPC 2.0 servers.
- **Plugins (`plugin.json`):**
  - On disk: Place `<data_dir>/plugins/<slug>/plugin.json` (plus optional bundled `skills/*/SKILL.md`, declarative `tools`, `rules`, and `mcpServers` entries; workspace `<workspace>/.agents/plugins/<slug>/` is also supported). Enabled plugin `mcpServers` entries are automatically synced into the MCP manager at startup.
  - Via Dashboard: Open **Extensions** -> **+ Import Plugin** to import a plugin manifest.
- **Custom Tools (`<data_dir>/tools/`):**
  - On disk: Place declarative `template` or `http` tool JSON files at `<data_dir>/tools/<id>.json` (or `<workspace>/.agents/tools/*.json`) with no 20-tool cap (`CustomToolManager`).
  - Via Dashboard: Open **Tools** -> **+ Add Tool** or **Import Tools** (accepts single tool JSON, arrays, `{"tools":[...]}`, or OpenAI `{"type":"function","function":{...}}` definitions).

### Workspace Files, Brain Markdown, Chat Uploads & HTML/Script Preview

- **Agent Workspace (`<data_dir>/workspace/`):** All files created or modified by agents live inside `<data_dir>/workspace/` (the default root in `tools.allowed_paths`), alongside `sandbox/{downloads,snapshots,tmp}`, `artifacts/`, `memory/`, and `.agents/{rules,workflows}`.
- **Workspace Brain & `.agents/` Files:** Edit `AGENTS.md`, `SOUL.md`, `IDENTITY.md`, `USER.md`, `TOOLS.md`, `MEMORY.md`, `HEARTBEAT.md`, daily notes (`memory/YYYY-MM-DD.md`), `.agents/rules/*.md`, and `.agents/workflows/*.md` either over FTP or directly inside **Sandbox & Doctor** -> **Workspace Brain Files & Workflows** (`GET/PUT /api/v1/brain` and `GET/PUT /api/v1/brain/{*name}`, enforcing atomic writes `<= 512 KiB` and rejecting symlinks or `..` traversal).
- **Chat File Attachments (`<data_dir>/workspace/uploads/`):** Non-image files attached in Chat are stored under `<data_dir>/workspace/uploads/<short_id>_<filename>` (governed by `server.upload_max_body_bytes`, default 64 MiB), while image attachments are passed inline for vision-capable models.
- **Dashboard Files Tab:** Open **Files** in the left navigation bar to browse directories, create folders, upload files, inspect code/text/images, use **View Script** / **Edit Script** / **Save** to edit workspace files inline, download artifacts, delete files, or click **Run / Preview HTML** to render `.html`/`.htm` files inside a sandboxed `<iframe sandbox="allow-scripts">`.

---

## 7. System Doctor & Sandbox Diagnostics (PS5 FW 13.40)

On PS5 Firmware `13.40`, verify workspace permissions, `PS5LM` connectivity, security posture, and release metadata synchronization right after deployment using **System Doctor (`12` Checks)**:

- **Via Web Dashboard:** Click **Sandbox & Doctor** (`#nav-sandbox` in the left navigation rail; `#nav-badge-doctor` shows active warning/failure counts) and use **Run Doctor** or **Auto-Fix Issues** in the **System Doctor (12 Checks)** card.
- **Via Chat Slash Command:** Run `/doctor` to execute all 12 checks inline or `/doctor fix` to apply safe repairs (`POST /api/v1/conversations/{id}/command`).
- **Via HTTP API:**
  - `GET /api/v1/doctor` — returns `DoctorReport` (`overall_status`, `pass_count`, `warn_count`, `fail_count`, `checks[]`).
  - `POST /api/v1/doctor/fix` — runs safe, idempotent auto-fixes (`doctor.fix` audit event) and returns `DoctorFixReport` (`applied_fixes[]` + updated `report`).

| Check ID | Title | What It Verifies on PS5 `13.40` | Auto-Fixable |
|---|---|---|---|
| `ps5lm_endpoint` | PS5LM Inference Endpoint | Validates `[inference].base_url` format and probes live `PS5LM` health (`3s` timeout) | No |
| `storage_integrity` | Storage Integrity | Runs DB `integrity_check()`, reads agents/conversations, and warns if `database.wal_mode = true` on PS5 | No |
| `workspace_sandbox` | Workspace & Sandbox Directories | Confirms `workspace/`, `sandbox/{downloads,snapshots,tmp}`, and `artifacts/` are non-symlink writable dirs | Yes |
| `brain_files` | Workspace Brain Files | Confirms the 7 root Brain markdown files plus `.agents/rules/ps5-sandbox.md` exist as regular files | Yes |
| `sandbox_network_policy` | Sandbox & Network SSRF Policy | Validates `sandbox.mode`/`approval_policy`, warns on `allow_local_network`, and blocks cloud metadata IPs | No |
| `auth_csrf_posture` | Authentication & CSRF Posture | Flags disabled auth on non-loopback `bind_host`, weak `admin_password`, and open `allow_lan_only = false` | No |
| `builtin_agent_tools` | Built-in Agents & Tool Wiring | Checks the 5 core built-in agents exist, `schedule_*` stays scoped to `commander`/`executor`, and all 10 required sandbox/web/browser tools are registered | Yes (missing core agents) |
| `extension_directories` | Extension Directories | Verifies `<data_dir>/{skills,plugins,mcp}` exist as regular non-symlink directories | Yes |
| `scheduler_engine` | Autonomous Scheduler Engine | Flags enabled schedules with `error`/`failed` status or runs overdue by `> 60s` | No |
| `memory_bounds` | Memory & Compaction Bounds | Ensures `archive_after_messages > keep_recent_messages` and notes `max_memories_per_agent = 0` RSS growth risk | No |
| `ps5_platform_compat` | PS5 Platform Compatibility | Recommends `tokio_worker_threads` `1..=8` (clamped `1..=3` on PS5), `wal_mode = false`, and `allow_host_shell = false` (virtual shell on PS5) | No |
| `meta_release_sync` | PS5 Release & `meta.json` Sync | Checks `build/ps5-agent.elf` and `build/meta.json` pairing and valid JSON (`"Host dev mode (no ELF present)"` when absent) | No |

When triggered, **Auto-Fix Issues** (`POST /api/v1/doctor/fix`) creates any missing `workspace/`, `sandbox/{downloads,snapshots,tmp}`, `artifacts/`, and `<data_dir>/{skills,plugins,mcp}` directories, calls `seed_workspace_brain_files` non-destructively (never overwriting edited files), restores any missing core built-in agents, and immediately re-runs all 12 checks. For HTTP endpoint details and security notes, see [API.md](API.md) and [SECURITY.md](SECURITY.md).
