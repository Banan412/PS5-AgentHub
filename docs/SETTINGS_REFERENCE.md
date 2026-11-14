# PS5 Agent Settings Reference

Complete reference for every configuration key in `config.toml`, environment variable overrides (`PS5_AGENT_*`), dashboard **Settings** and **Sandbox Policy** form fields, **Agent** modal options, HTTP settings/sandbox endpoints (`GET`/`PUT /api/v1/settings`, `GET`/`PUT /api/v1/sandbox`), thinking mode level, KV-cache prefill & `llama.cpp` sampling parameters, unlimited `0` limits, PS5LM automatic capability sync, extension/custom-tool/memory workflows, and database/memory pruning rules.

> **Target Platform:** PlayStation 5 Slim running Firmware `13.40` (`0x13400000`) via `ps5-payload-dev/elfldr` alongside `PS5LM` (`http://127.0.0.1:8081/v1`).

---

## Table of Contents

1. [Configuration Loading, CLI/Env Overrides & Resilient TOML Parsing](#1-configuration-loading-clienv-overrides--resilient-toml-parsing)
2. [Runtime Defaults vs. `config.ps5-slim-1340.toml` Preset](#2-runtime-defaults-vs-configps5-slim-1340toml-preset)
3. [Complete `config.toml` & Dashboard Settings Reference](#3-complete-configtoml--dashboard-settings-reference)
   - [Top-Level (`data_dir`)](#top-level-data_dir)
   - [`[inference]` — PS5LM Client, Thinking, KV Cache & Sampling](#inference--ps5lm-client-thinking-kv-cache--sampling)
   - [`[server]` — HTTP Listener, LAN Guard & Body Limits](#server--http-listener-lan-guard--body-limits)
   - [`[database]` — SQLite / JSONL Engine & Retention](#database--sqlite--jsonl-engine--retention)
   - [`[security]` — Auth, Rate Limiting, Approvals & Argon2id](#security--auth-rate-limiting-approvals--argon2id)
   - [`[agents]` — Concurrency, Task Caps & Specialist Lifecycle](#agents--concurrency-task-caps--specialist-lifecycle)
   - [`[memory]` — Long-Term Memory & Chat Compaction](#memory--long-term-memory--chat-compaction)
   - [`[sandbox]` — Execution Modes, Approval Policy, Web Search & Brain Injection](#sandbox--execution-modes-approval-policy-web-search--brain-injection)
   - [`[tools]` — Filesystem Sandbox & Web SSRF Guard](#tools--filesystem-sandbox--web-ssrf-guard)
   - [`[logging]` — Rotating File, Ring Buffer, `klog` & Toast](#logging--rotating-file-ring-buffer-klog--toast)
   - [`[runtime]` — Tokio Threads, Health Polling & Rest Mode](#runtime--tokio-threads-health-polling--rest-mode)
4. [Per-Agent Settings & Built-In Agents (`AgentDefinition` & `AgentModelConfig`)](#4-per-agent-settings--built-in-agents-agentdefinition--agentmodelconfig)
5. [Thinking Levels (`off` / `low` / `medium` / `high` / `max`), Reasoning Format & Coordinator Selection](#5-thinking-levels-off--low--medium--high--max-reasoning-format--coordinator-selection)
6. [Unlimited `0` Limits](#6-unlimited-0-limits)
7. [Automatic PS5LM Settings Sync (`/health`, `/v1/models`, `/props`, `/slots`) & KV Cache Prefill](#7-automatic-ps5lm-settings-sync-health-v1models-props-slots--kv-cache-prefill)
8. [Extensions, Custom Tools, Uploads & Memory Import/Export](#8-extensions-custom-tools-uploads--memory-importexport)
9. [Retention & Pruning](#9-retention--pruning)
10. [Hot-Reload vs. Restart Matrix (`PUT /api/v1/settings`, `PUT /api/v1/sandbox` & `POST /api/v1/inference/import-kv`)](#10-hot-reload-vs-restart-matrix-put-apiv1settings-put-apiv1sandbox--post-apiv1inferenceimport-kv)

---

## 1. Configuration Loading, CLI/Env Overrides & Resilient TOML Parsing

At startup (`src/startup.rs` -> `load_effective_config_with_warnings`), configuration is resolved in the following order:

1. **Data directory (`data_dir`):**
   - `--data-dir <path>` CLI flag (if provided), otherwise
   - `PS5_AGENT_DATA_DIR` environment variable (if set and non-empty), otherwise
   - `/data/ps5-agent` on PS5 (`target_vendor = "sie"`) or `./data/ps5-agent` on host builds.
   - `resolve_writable_data_dir()` probes write access (`.write_probe`); on PS5 (`target_vendor = "sie"`), if `/data/ps5-agent` is not writable (e.g., HEN directory restrictions), it automatically falls back to `/download0/ps5-agent`.
   - `ensure_directories()` (`src/startup.rs`) automatically creates `<data_dir>` plus `logs/`, `database/`, `backups/`, `skills/`, `plugins/`, `mcp/`, `tools/`, and configured `tools.allowed_paths` (default `<data_dir>/workspace`).
2. **Config file path:** `--config <path>` CLI flag, or `<resolved_data_dir>/config.toml`.
3. **Resilient TOML loading (`AppConfig::load_or_create_resilient` in `crates/ps5-agent-config/src/lib.rs`):**
   - **Sanitization (`sanitize_user_toml`):** Strips leading UTF-8 BOM (`\u{feff}`), removes NUL bytes (`\0`), and normalizes unquoted (`True`/`yes`/`on`, `False`/`no`/`off`) and quoted (`"true"`/`"false"`) boolean tokens on `KNOWN_BOOL_KEYS` (`auth_enabled`/`auth`, `allow_lan_only`, `wal_mode`, `stream_enabled`, `auto_sync_from_ps5lm`, `prefill_kv_cache`, `cache_prompt`, `ignore_eos`, `timings_per_token`, `smart_tool_routing`, `flash_attn`, `require_approval_for_mutating`, `allow_temporary_specialists`, `block_private_lan_web_requests`, `network_access`, `allow_local_network`, `allow_host_shell`, `inject_brain_files`, `enable_klog`, `enable_toast`, `rest_mode_recovery`) without altering string fields like `admin_password = "false"` or comments.
   - **Partial files, derived subpaths & TOML key aliases (`parse_app_config_with_subpaths`):** Missing keys fall back to `#[serde(default)]` defaults. When a partial TOML sets top-level `data_dir` without setting `database.path`, `logging.log_dir`, or `tools.allowed_paths`, `parse_app_config_with_subpaths` derives those omitted subpaths automatically from the overridden `data_dir`. Supported legacy aliases:
     - `[inference] url` -> `base_url`
     - `[server] host` -> `bind_host`
     - `[security] auth` -> `auth_enabled`
     - `[security] username` -> `admin_username`
     - `[security] password` -> `admin_password`
   - **Environment variable overrides (`AppConfig::apply_env_overrides`):** Applied after loading/creating `config.toml` (and after `with_data_dir`), overriding both `[sandbox]` and mirrored `[tools]` keys when non-empty:
     - `PS5_AGENT_DATA_DIR` -> root data directory (`src/startup.rs`)
     - `PS5_AGENT_SANDBOX_MODE` -> `sandbox.mode`
     - `PS5_AGENT_APPROVAL_POLICY` -> `sandbox.approval_policy`
     - `PS5_AGENT_SEARCH_PROVIDER` -> `sandbox.web_search_provider` & `tools.web_search_provider` (lowercased)
     - `PS5_AGENT_SEARXNG_URL` -> `sandbox.searxng_url` & `tools.searxng_url`
     - `PS5_AGENT_BRAVE_API_KEY` -> `sandbox.brave_api_key` & `tools.brave_api_key`
     - `PS5_AGENT_TAVILY_API_KEY` -> `sandbox.tavily_api_key` & `tools.tavily_api_key`
     - `PS5_AGENT_REMOTE_BROWSER_URL` -> `sandbox.remote_browser_url` & `tools.remote_browser_url`
   - **Sandbox/Tools reconciliation (`sync_sandbox_and_tools`):** Called automatically on config construction, load, normalization, and save. Overlapping keys (`network_access`, `web_search_provider`, `searxng_url`, `brave_api_key`, `tavily_api_key`, `remote_browser_url`, `allow_host_shell`, `max_exec_output_bytes`, `exec_timeout_secs`) reconcile favoring non-default `[sandbox]` over `[tools]`; `allow_local_network` is unified as `sandbox.allow_local_network || tools.allow_local_network || !tools.block_private_lan_web_requests` and keeps `tools.block_private_lan_web_requests = !allow_local_network`.
   - **Clamping, domain repair & validation (`normalize_and_clamp` / `AppConfig::validate`):** Non-finite floats reset to defaults (`temperature < 0.0` clamps to `0.0`, `mirostat > 2` clamps to `2`, `NaN` `dry_multiplier` resets to `0.8` while negative `dry_multiplier` clamps to `0.0`), `inference.cache_type_k` / `cache_type_v` normalize to lowercase and invalid values warn and reset to `""`, empty `tools.allowed_paths` defaults to `[<data_dir>/workspace]`, **empty `tools.allowed_domains` while `sandbox.network_access = true` repairs to `["*"]` with a warning**, `security.session_ttl_hours` clamps to `0..=87600`, and `database.driver` / `agents.on_disable_policy` / `sandbox.mode` / `sandbox.approval_policy` normalize or fall back with warnings. `validate()` verifies finite sampling floats, `temperature >= 0.0`, `dry_multiplier >= 0.0`, `mirostat <= 2`, allowlisted `cache_type_k`/`cache_type_v` (`""`, `f32`, `f16`, `bf16`, `q8_0`, `q4_0`, `q4_1`, `iq4_nl`, `q5_0`, `q5_1`), valid JSON object syntax (`validate_json_object`) on non-empty `inference.custom_extra_body_json`, valid `sandbox.mode`/`sandbox.approval_policy`, and `session_ttl_hours <= 87600`. Unparseable syntax or hard validation failures copy the broken file to `config.toml.broken.bak` and regenerate safe defaults.
4. **CLI flag overrides:** `--bind <host:port>` overrides `server.bind_host` and `server.port`; `--ps5lm-url <url>` overrides `inference.base_url`.
5. **Release upgrade & `meta.json` verification (`compute_release_stamp`, `verify_or_sync_meta_json` in `src/startup.rs`):** On every startup, `initialize_with_warnings` hashes `CARGO_PKG_VERSION`, `CURRENT_SCHEMA_VERSION`, `BUILTIN_AGENTS_SCHEMA_VERSION`, `builtin_agents_fingerprint()`, the embedded dashboard bundle (`INDEX_HTML`/`STYLES_CSS`/`APP_JS`), and the current executable bytes (`<= 64 MiB`) into `current_release_stamp`. It checks `<exe_dir>/meta.json` and `<data_dir>/meta.json` (entries matching `id == "ps5-agent"` or `filename == "ps5-agent.elf"`); when running from `ps5-agent.elf`, stale `checksum`, `version` (`v<CARGO_PKG_VERSION>`), and `last_update` (`YYYY-MM-DD`) fields are synced automatically. When `installed_release_stamp` differs from `current_release_stamp`, SQLite settings persist `installed_release_stamp`, `installed_version`, and `last_upgraded_at`, and record a `system.elf_upgrade` audit entry (`target: "ps5-agent.elf"`).

---

## 2. Runtime Defaults vs. `config.ps5-slim-1340.toml` Preset

`ps5-agent` has two distinct sets of defaults depending on how `config.toml` is created:

- **Code-generated defaults (`AppConfig::with_data_dir`):** Used when no `config.toml` exists or when fields are omitted from a partial `config.toml`. Uses `0` (unlimited) for agent/task/reasoning/memory caps, `max_concurrent_inference = 4`, `thinking_level = "medium"`, `auto_sync_from_ps5lm = true`, `prefill_kv_cache = true`, `cache_prompt = true`, `smart_tool_routing = true`, `cache_type_k = ""`, `cache_type_v = ""`, `flash_attn = false`, `security.require_approval_for_mutating = false`, `sandbox.mode = "full_sandbox"`, `sandbox.approval_policy = "full_auto"`, `llama.cpp` sampling defaults (`temperature = 0.4`, `top_k = 40`, `top_p = 0.95`, `min_p = 0.05`, `repeat_penalty = 1.05`, `repeat_last_n = 256`, `dry_multiplier = 0.8`, `dry_base = 1.75`, `dry_allowed_length = 2`, `dry_penalty_last_n = -1`, `id_slot = -1`, `reasoning_format = "deepseek"`), `upload_max_body_bytes = 67108864` (64 MiB), `retention_days = 30`, and `max_rows_per_table = 10000`. On PS5 (`target_vendor = "sie"`), `bind_host = "0.0.0.0"`, `argon2_memory_kib = 8192`, `wal_mode = false`, `enable_klog = true`, and `enable_toast = true`.
- **Shipped PS5 preset ([`config/config.ps5-slim-1340.toml`](../config/config.ps5-slim-1340.toml)):** Configures `max_agents = 0`, `max_active_tasks = 0`, `max_queued_tasks = 0`, `max_concurrent_inference = 4` (auto-synced to PS5LM `total_slots` when `auto_sync_from_ps5lm = true`), `max_messages_per_task = 0`, `max_reasoning_iterations = 0`, `default_task_timeout_secs = 0`, `max_memories_per_agent = 0`, `context_limit = 8192`, `repeat_last_n = 256`, `dry_multiplier = 0.8`, `smart_tool_routing = true`, `cache_type_k = ""`, `cache_type_v = ""`, `flash_attn = false`, `prefill_kv_cache = true`, and `id_slot = -1` on Firmware `13.40`. Note that `config/config.ps5-slim-1340.toml` explicitly preserves `security.require_approval_for_mutating = true` (more conservative than `SecurityConfig::default().require_approval_for_mutating = false`) and omits `[sandbox]` (so `SandboxConfig` defaults apply, with `require_approval_for_mutating = true` promoting effective `full_auto` -> `on_request`).

---

## 3. Complete `config.toml` & Dashboard Settings Reference

### Top-Level (`data_dir`)

| TOML Key | UI Label | Type | Default | Valid Range | Runtime Effect | Restart Needed | PS5 13.40 RAM/CPU Impact |
|---|---|---|---|---|---|---|---|
| `data_dir` | *(TOML / CLI `--data-dir` / env `PS5_AGENT_DATA_DIR`)* | `PathBuf` | PS5: `"/data/ps5-agent"` (fallback `"/download0/ps5-agent"`), Host: `"./data/ps5-agent"` | Writable directory path | Root directory for `config.toml`, `database/`, `logs/`, `backups/`, `workspace/`, `skills/`, `plugins/`, `mcp/`, `tools/`, legacy `mcp_servers.json`, `ps5-agent.pid`, and `bootstrap_credentials.txt`. | **Yes** | Minimal; `/data` and `/download0` reside on internal SSD. |

---

### `[inference]` — PS5LM Client, Thinking, KV Cache & Sampling

#### Core Connection, Model, Context & Thinking

| TOML Key | UI Label | Type | Default | Valid Range | Runtime Effect | Restart Needed | PS5 13.40 RAM/CPU Impact |
|---|---|---|---|---|---|---|---|
| `inference.base_url` *(alias `url`)* | `PS5LM Base URL` (`#cfg-base-url`) | `String` | `"http://127.0.0.1:8081/v1"` | Valid `http://` or `https://` URL | Base URL for all PS5LM inference calls and capability probes (`/health`, `/v1/models`, `/props`, `/slots`). Trailing `/v1` is normalized automatically. | No (hot-swapped) | Loopback (`127.0.0.1`) avoids LAN round-trip latency and socket overhead. |
| `inference.model` | `Model` (`#cfg-model`) | `String` | `"qwen-loaded"` | Non-empty string | Default model ID sent to PS5LM when an agent has `model_override = null`. Automatically overwritten when `auto_sync_from_ps5lm = true` and PS5LM reports a loaded model. | No (hot-swapped) | None in `ps5-agent`; model weights live in PS5LM's GDDR6 budget. |
| `inference.api_mode` | `API Mode` (`#cfg-api-mode`) | `ApiMode` (`String`) | `"chat_completions"` | `"chat_completions"`, `"responses"`, `"anthropic_messages"` (alias `"messages"`), `"completions"`, `"system_one"` (alias `"systemone"`) | Wire protocol for PS5LM requests (`POST /v1/chat/completions`, `/v1/responses`, `/v1/messages`, `/v1/completions`, `/v1/systemone`). `chat_completions` and `responses` support tools, vision, and SSE streaming. | No (hot-swapped) | Negligible CPU difference; `chat_completions` SSE avoids buffering large responses. |
| `inference.api_key` | *(TOML only; blocked on `/settings`)* | `Option<String>` | `None` | Any non-empty bearer/API key string | Sent as `Authorization: Bearer <key>` (or `x-api-key` for `anthropic_messages`). Rejected with `403 Forbidden` via `PUT /api/v1/settings`. | **Yes** | None. |
| `inference.thinking_level` | `Default Thinking Level` (`#cfg-thinking-level`) | `String` | `"medium"` | `"off"`, `"low"`, `"medium"`, `"high"`, `"max"` (invalid values clamp to `"medium"` with warning) | Global default reasoning depth (`ThinkingLevel`). Overridden per agent via `AgentModelConfig.thinking_level` and per message via the chat Thinking selector. | No (hot-swapped) | Higher levels (`high`/`max`) generate more reasoning tokens, increasing PS5LM GPU time, inference latency, and KV-cache occupancy. |
| `inference.auto_sync_from_ps5lm` | `Auto-Sync from PS5LM` (`#cfg-auto-sync-ps5lm`) | `bool` | `true` | `true` / `false` | When `true`, startup and periodic/manual sync apply PS5LM's `loaded_model`, `context_length` (`n_ctx`), and `total_slots` (when `max_concurrent_inference` is `0`, `4`, or previously auto-synced). | No (hot-swapped) | Low; prevents OOM or context overflow mismatches between `ps5-agent` and `PS5LM`. |
| `inference.auto_sync_interval_secs` | `Auto-Sync Interval (secs)` (`#cfg-auto-sync-interval-secs`) | `u64` | `30` | `0..=86400` (`0` = manual sync only; UI max `86400`) | Minimum interval in seconds between background `/health` + `/v1/models` + `/props` + `/slots` capability syncs (evaluated on each `health_poll_interval_secs` tick). | No (hot-swapped) | Very small intervals (`< 5s`) increase localhost HTTP polling; `30s` is negligible on 5-CPU budget. |
| `inference.stream_enabled` | `Streaming Responses` (`#cfg-stream-enabled`) | `bool` | `true` | `true` / `false` | Enables SSE token and `<think>` reasoning streaming (`StreamEvent::ChatToken`, `ChatReasoning`; `InferenceStreamChunk::TextDelta`, `ReasoningDelta`) via `generate_stream`. When `false`, uses blocking `generate` (note: `system_one` always executes non-streaming). | No (hot-swapped) | Streaming keeps response memory small and improves perceived latency; SSE line buffer is capped at `8 MiB` (`MAX_SSE_LINE_BYTES`). |
| `inference.timeout_secs` | `Inference Timeout (secs)` (`#cfg-timeout-secs`) | `u64` | `120` | `0..=86400` in UI (`0` = unlimited / no per-request deadline via `effective_request_timeout`) | Per-request timeout applied to PS5LM inference calls (`Some(Duration::secs)` when `> 0`, `None` when `0`; `build_http_client` also enables `tcp_nodelay(true)` and `pool_idle_timeout(45s)`). Also caps capability probe timeouts when `1..=5s`. | No (rebuilds HTTP client) | High timeouts keep one TCP socket open longer during generation; negligible RAM (`< 16 KiB`). |
| `inference.connect_timeout_secs` | `Connect Timeout (secs)` (`#cfg-connect-timeout-secs`) | `u64` | `5` | `1..=300` (applied when `> 0`; `probe_timeout` uses `5s` if `0`, else `clamp(2, 5)`) | TCP connect timeout when opening connections to PS5LM. Also drives `probe_timeout` (`2`–`5s`, capped by `timeout_secs` if `1..=5s`) for `/health`, `/v1/models`, `/props`, and `/slots` probes. | No (rebuilds HTTP client) | Prevents hanging worker tasks if PS5LM is loading/restarting. |
| `inference.max_retries` | `Max Inference Retries` (`#cfg-max-retries`) | `u32` | `2` | `0..=20` in UI (`0` = no retries) | Bounded exponential backoff (`100ms` * `2^attempt`, capped at `2000ms`) strictly on **pre-response** TCP connect errors (`err.is_connect()`). Does not retry HTTP `503` or mid-stream drops. | No (hot-swapped) | Minimal; brief sleep on Tokio task without extra RAM. |
| `inference.context_limit` | `Context Limit (tokens)` (`#cfg-context-limit`) | `u32` | `8192` | `0` (server/PS5LM default) or `512..=1048576` | Default prompt + completion token budget per turn. Auto-updated when `auto_sync_from_ps5lm = true`. | No (hot-swapped) | **High PS5LM impact:** larger `n_ctx` in PS5LM increases GDDR6 KV-cache RAM substantially on 16 GB unified memory. |
| `inference.max_output_tokens` | `Max Output Tokens` (`#cfg-max-output-tokens`) | `u32` | `1024` | `0` (unlimited up to context window) or `64..=(context_limit - 1)` when `context_limit > 0` | Maximum completion tokens requested per model reply (`max_tokens` / `max_output_tokens`). In `anthropic_messages`, `0` maps to `8192` (`ANTHROPIC_DEFAULT_MAX_TOKENS`). | No (hot-swapped) | Longer replies take more PS5LM GPU time and slightly more string heap during SSE accumulation. |

#### KV Cache & `llama.cpp` / PS5LM Sampling Parameters (`KV Cache & llama.cpp / PS5LM Sampling`)

All 37 fields below live in `[inference]`, update immediately on `PUT /api/v1/settings` without restarting (`self.inference.update_config`), and are grouped in the dashboard **Settings -> KV Cache & llama.cpp / PS5LM Sampling** fieldset—alongside the **Import KV & Sampling from llama.cpp / PS5LM** (`#btn-import-llamacpp-kv`) action (`POST /api/v1/inference/import-kv` with `#import-kv-status` status pill). Except for `prefill_kv_cache` (background post-response KV warming), `smart_tool_routing` (runtime tool-schema slimming), and metadata/planning flags (`cache_type_k`, `cache_type_v`, `flash_attn`), `apply_llamacpp_sampling_fields` (`crates/ps5-agent-inference/src/provider.rs`) injects these fields onto `chat_completions`, `responses`, and `completions` JSON request payloads (`anthropic_messages` and `system_one` ignore `llama.cpp`-specific sampling keys).

| TOML Key | UI Label & ID | Type | Default | Valid Range (UI) & Wire Condition | Runtime Effect | Restart Needed | PS5 13.40 RAM/CPU Impact |
|---|---|---|---|---|---|---|---|
| `inference.prefill_kv_cache` | `Pre-fill KV cache after response` (`#cfg-prefill-kv-cache`) | `bool` | `true` | `true` / `false` | After a response finishes, asynchronously re-submits the updated conversation (`ThinkingLevel::Off`, `cache_prompt: true`, `max_tokens: 0`, `n_predict: 0`, 30s timeout) if an inference permit is free (`try_acquire_owned`) so PS5LM caches the prompt prefix while the user reads. | No | Short background PS5LM prompt-eval burst after reply; never blocks interactive turns; cuts next-turn prompt latency without extra `ps5-agent` RSS. |
| `inference.cache_prompt` | `Server Prefix Cache (cache_prompt)` (`#cfg-cache-prompt`) | `bool` | `true` | `true` / `false` *(always emitted)* | Instructs PS5LM / `llama.cpp` to retain and reuse matching KV-cache tokens across shared system prompts and prior conversation turns. | No | **High PS5LM benefit:** skips redundant prefill compute and reduces CPU/GPU time per turn; uses existing slot KV memory. |
| `inference.cache_type_k` | `KV Cache Key Quantization (cache_type_k)` (`#set-inf-cache-type-k`) | `String` | `""` *(server default)* | `""`, `"f16"`, `"q8_0"`, `"q4_0"`, `"q5_0"`, `"q5_1"`, `"q4_1"`, `"bf16"`, `"f32"`, `"iq4_nl"` *(normalized lowercase; invalid resets to `""` on normalize / errors on validate; not emitted on chat wire)* | Target `llama.cpp` K-cache quantization format imported via `POST /api/v1/inference/import-kv` (`/props`, `/slots`) or configured explicitly for PS5LM capacity alignment. | No (hot-swapped) | Reference/tuning metadata in `ps5-agent`; quantized K-cache (`q8_0`, `q4_0`) lowers PS5LM unified GDDR6 footprint. |
| `inference.cache_type_v` | `KV Cache Value Quantization (cache_type_v)` (`#set-inf-cache-type-v`) | `String` | `""` *(server default)* | `""`, `"f16"`, `"q8_0"`, `"q4_0"`, `"q5_0"`, `"q5_1"`, `"q4_1"`, `"bf16"`, `"f32"`, `"iq4_nl"` *(normalized lowercase; invalid resets to `""` on normalize / errors on validate; not emitted on chat wire)* | Target `llama.cpp` V-cache quantization format imported via `POST /api/v1/inference/import-kv` (`/props`, `/slots`) or configured explicitly for PS5LM capacity alignment. | No (hot-swapped) | Reference/tuning metadata in `ps5-agent`; quantized V-cache trims PS5LM unified GDDR6 memory use. |
| `inference.flash_attn` | `Flash Attention (flash_attn)` (`#set-inf-flash-attn`) | `bool` | `false` | `false` / `true` *(not emitted on chat wire)* | Records whether PS5LM / `llama.cpp` runs with Flash Attention acceleration enabled (auto-updated from `flash_attn` / `flash_attention` during `POST /api/v1/inference/import-kv`). | No (hot-swapped) | Reference flag in `ps5-agent`; enables KV-cache quantization and faster long-context attention on PS5LM. |
| `inference.smart_tool_routing` | `Smart Tool Routing (smart_tool_routing)` (`#set-inf-smart-tool-routing`) | `bool` | `true` | `true` / `false` | On turn iteration `0` (`iter == 0`) when `!has_attachments`, no `"tool"` history exists, and `allowed_tools.len() > 12`, `select_tools_for_casual_turn` omits all tools (`[]`) for casual greetings/pings or restricts to conversational/extension tools (`is_extension_or_conversational_tool`) on short (`<= 140`-char, `<= 20`-word, single-line) messages lacking file/code/browser/action cues. | No (hot-swapped) | **High PS5LM tok/s benefit:** shrinks prompt token count and prefill latency on conversational turns while sending full schemas on tool/coding prompts. |
| `inference.temperature` | `Temperature` (`#cfg-temperature`) | `f32` | `0.4` | `0.0–2.0` (`step 0.05`) | Base sampling randomness (`0.0` = greedy deterministic). Per-agent turns use `AgentModelConfig.temperature`; global value applies to fallback/system calls (e.g., KV prefill). | No | Zero daemon RAM/CPU impact. |
| `inference.dynatemp_range` | `Dynamic Temp Range (dynatemp_range)` (`#cfg-dynatemp-range`) | `f32` | `0.0` | `0.0–2.0` (`step 0.05`; emitted only if `> 0.0`) | Entropy-based dynamic temperature spread around base temperature (`0.0` = disabled). | No | Negligible PS5LM entropy calculation when `> 0.0`. |
| `inference.dynatemp_exponent` | `Dynamic Temp Exponent (dynatemp_exponent)` (`#cfg-dynatemp-exponent`) | `f32` | `1.0` | `0.0–10.0` (`step 0.05`; emitted when `dynatemp_range > 0.0`) | Exponent shaping dynamic temperature scaling across token entropy. | No | Negligible. |
| `inference.top_k` | `Top-K (top_k)` (`#cfg-top-k`) | `i32` | `40` | `-1–10000` (`0` = disabled; always emitted) | Restricts candidate pool to the top `K` highest-probability tokens. | No | Negligible (smaller `K` slightly trims logit sorting). |
| `inference.top_p` | `Top-P Nucleus (top_p)` (`#cfg-top-p`) | `f32` | `0.95` | `0.0–1.0` (`step 0.01`; `1.0` disables; always emitted) | Retains smallest token subset whose cumulative probability reaches `top_p`. | No | Negligible. |
| `inference.min_p` | `Min-P (min_p)` (`#cfg-min-p`) | `f32` | `0.05` | `0.0–1.0` (`step 0.01`; `0.0` disables; always emitted) | Discards tokens below `min_p * max_prob`. | No | Negligible. |
| `inference.typical_p` | `Locally Typical P (typical_p)` (`#cfg-typical-p`) | `f32` | `1.0` | `0.0–1.0` (`step 0.01`; emitted only if `< 1.0`) | Filters tokens by closeness to expected conditional entropy (`1.0` = disabled). | No | Negligible PS5LM sampler CPU when `< 1.0`. |
| `inference.xtc_probability` | `XTC Probability (xtc_probability)` (`#cfg-xtc-probability`) | `f32` | `0.0` | `0.0–1.0` (`step 0.01`; emitted only if `> 0.0`) | Probability of applying Exclude Top Choices (XTC) to suppress repetitive/cliché phrasing (`0.0` = disabled). | No | Negligible. |
| `inference.xtc_threshold` | `XTC Threshold (xtc_threshold)` (`#cfg-xtc-threshold`) | `f32` | `0.1` | `0.0–0.5` (`step 0.01`; emitted when `xtc_probability > 0.0`) | Minimum token probability required for XTC exclusion when active. | No | Negligible. |
| `inference.top_n_sigma` | `Top-N Sigma (top_n_sigma)` (`#cfg-top-n-sigma`) | `f32` | `-1.0` | `-1.0–100.0` (`step 0.1`; `<= 0` disabled; emitted only if `> 0.0`) | Prunes logits more than `N` standard deviations below the maximum logit. | No | Negligible. |
| `inference.repeat_penalty` | `Repeat Penalty (repeat_penalty)` (`#cfg-repeat-penalty`) | `f32` | `1.05` | `0.0–3.0` (`step 0.01`; omitted when neutral `1.0` within `1e-5`) | Multiplicative penalty on recently seen tokens to reduce looping. | No | Negligible. |
| `inference.repeat_last_n` | `Repeat Last N (repeat_last_n)` (`#cfg-repeat-last-n`) | `i32` | `256` | `-1–131072` (`0` disables, `-1` = full context; always emitted) | Number of trailing tokens scanned by `repeat_penalty` (`256` reduces longer-horizon repetition loops). | No | Setting `-1` on very large contexts adds slight per-token scan CPU in PS5LM. |
| `inference.presence_penalty` | `Presence Penalty (presence_penalty)` (`#cfg-presence-penalty`) | `f32` | `0.0` | `-2.0–2.0` (`step 0.05`; omitted when neutral `0.0` within `1e-5`) | Flat penalty applied once to any token present in the context window. | No | Omitting neutral `0.0` avoids redundant sampler work on `llama.cpp`. |
| `inference.frequency_penalty` | `Frequency Penalty (frequency_penalty)` (`#cfg-frequency-penalty`) | `f32` | `0.0` | `-2.0–2.0` (`step 0.05`; omitted when neutral `0.0` within `1e-5`) | Penalty scaled proportionally by token count in context. | No | Omitting neutral `0.0` avoids redundant sampler work on `llama.cpp`. |
| `inference.dry_multiplier` | `DRY Multiplier (dry_multiplier)` (`#cfg-dry-multiplier`) | `f32` | `0.8` | `0.0–10.0` (`step 0.05`; `0.0` disables; emits all 4 `dry_*` keys when `> 0.0`) | Don't Repeat Yourself (DRY) n-gram repetition penalty multiplier (`0.8` active by default to prevent verbatim prose/phrase looping). | No | Minor PS5LM CPU n-gram lookup overhead per generated token when enabled; stops multi-turn repetition stalls. |
| `inference.dry_base` | `DRY Base (dry_base)` (`#cfg-dry-base`) | `f32` | `1.75` | `1.0–10.0` (`step 0.05`; emitted when `dry_multiplier > 0.0`) | Exponential base controlling DRY penalty scaling by match length. | No | None beyond `dry_multiplier`. |
| `inference.dry_allowed_length` | `DRY Allowed Length (dry_allowed_length)` (`#cfg-dry-allowed-length`) | `i32` | `2` | `0–100` (emitted when `dry_multiplier > 0.0`) | Maximum repeated sequence length (tokens) allowed before DRY penalty applies. | No | None beyond `dry_multiplier`. |
| `inference.dry_penalty_last_n` | `DRY Penalty Last N (dry_penalty_last_n)` (`#cfg-dry-penalty-last-n`) | `i32` | `-1` | `-1–131072` (`0` = off, `-1` = full context; emitted when `dry_multiplier > 0.0`) | Lookback tokens scanned for DRY n-gram matches. | No | Large lookbacks (`-1` at huge `n_ctx`) add minor PS5LM CPU scan cost when DRY is active. |
| `inference.mirostat` | `Mirostat Mode (mirostat)` (`#cfg-mirostat`) | `u8` | `0` | `0` (Disabled), `1` (Mirostat v1), `2` (Mirostat v2); emitted only if `> 0` (`u8::MAX` clamped on write) | Adaptive perplexity-targeting sampler mode (`1`/`2` override `top_k`/`top_p` in `llama.cpp`). | No | Negligible. |
| `inference.mirostat_tau` | `Mirostat Tau (mirostat_tau)` (`#cfg-mirostat-tau`) | `f32` | `5.0` | `0.0–20.0` (`step 0.1`; emitted when `mirostat > 0`) | Target cross-entropy (surprise) when Mirostat 1 or 2 is enabled. | No | Negligible. |
| `inference.mirostat_eta` | `Mirostat Eta (mirostat_eta)` (`#cfg-mirostat-eta`) | `f32` | `0.1` | `0.0–1.0` (`step 0.01`; emitted when `mirostat > 0`) | Learning rate for Mirostat step adjustment when active. | No | Negligible. |
| `inference.seed` | `RNG Seed (seed)` (`#cfg-seed`) | `i64` | `-1` | `-1` (random) or `>= 0` (emitted only if `!= -1`) | Deterministic random seed sent for reproducible outputs. | No | Zero. |
| `inference.ignore_eos` | `Ignore EOS (ignore_eos)` (`#cfg-ignore-eos`) | `bool` | `false` | `false` / `true` (emitted only if `true`) | Ignores End-of-Sequence token and continues generation until a stop string or token budget is hit. | No | Can generate all the way to `max_output_tokens`/`n_ctx`, increasing GPU time if left enabled. |
| `inference.stop_sequences` | `Stop Sequences (stop)` (`#cfg-stop-sequences`) | `Vec<String>` | `[]` | Array of strings (newline-separated string in UI / `PUT /settings`; empty omitted) | Non-empty custom stop strings merged (deduplicated) into the payload `"stop"` array alongside built-in stops (such as `<\|im_end\|>` in `completions`). | No | Zero (`< 1 KiB` payload). |
| `inference.n_keep` | `Prompt Tokens to Keep (n_keep)` (`#cfg-n-keep`) | `i32` | `0` | `-1–131072` (`0` = none, `-1` = keep entire initial prompt; emitted if `!= 0`) | Initial prompt tokens preserved in KV cache when `llama.cpp` performs context shifts. | No | Zero daemon RAM; keeps system prompt warm across KV shifts in PS5LM. |
| `inference.id_slot` | `Pinned Server Slot (id_slot)` (`#cfg-id-slot`) | `i32` | `-1` | `-1–64` (`-1` = auto FNV-1a conversation affinity; `>= 0` = fixed pin) | When `>= 0`, pins all requests to that PS5LM KV slot (`id_slot` emitted). When `-1` and `total_slots > 1`, `AgentRuntime` (`resolve_conversation_slot_id`) computes `fnv1a_64(conversation_id) % total_slots` to route each conversation deterministically to the same KV slot (shared by both live turns and `prefill_kv_cache`); omitted when `id_slot == -1` and `total_slots <= 1` or `conversation_id` is empty. | No | **High PS5LM KV hit benefit:** auto FNV-1a affinity prevents multi-chat slot thrashing without manual slot management; explicit `0+` pins single-stream runs. |
| `inference.samplers` | `Sampler Chain (samplers)` (`#cfg-samplers`) | `String` | `""` | Comma/semicolon string or JSON array (`dry,top_k,typ_p,top_p,min_p,xtc,temperature`; empty omitted) | Overrides `llama.cpp` sampler execution order; normalized to a JSON `Vec<String>` on the wire (arrays sent via `PUT /settings` are comma-joined). | No | Negligible. |
| `inference.grammar` | `GBNF Grammar (grammar)` (`#cfg-grammar`) | `String` | `""` | Valid llama.cpp GBNF text (empty omitted) | Constrains generated output tokens to match a GBNF grammar. | No | Complex grammars increase per-token PS5LM CPU masking work; zero daemon RAM. |
| `inference.reasoning_format` | `Reasoning Format (reasoning_format)` (`#cfg-reasoning-format`) | `String` | `"deepseek"` | Suggested: `"deepseek"`, `"deepseek-legacy"`, `"auto"`, `"none"` (or custom) | Sent whenever `thinking_level != off` and non-empty/`!= "none"`. Note `chat_completions` with `thinking_level == off` always sends `"reasoning_format": "none"`. | No | Zero. |
| `inference.timings_per_token` | `Timings Per Token (timings_per_token)` (`#cfg-timings-per-token`) | `bool` | `false` | `false` / `true` (emitted only if `true`) | Requests per-token prompt/generation timing metadata from `llama.cpp`. Explicitly exempted in `is_sensitive_setting_key` so `_token` suffix filtering does not hide or reject it. | No | Slightly larger HTTP/SSE payloads when `true`; negligible RAM. |
| `inference.custom_extra_body_json` | `Custom Extra Body JSON` (`#cfg-custom-extra-body-json`) | `String` | `""` | Valid JSON object (or JSON object on `PUT /settings`; `<= 8192` chars for flat `/settings`) | Parsed and shallow-merged into top-level request JSON (protected keys `"messages"`, `"input"`, `"prompt"`, `"stream"` cannot be overwritten; malformed/non-object JSON logs a warning and is ignored). | No | Depends on custom keys injected; negligible daemon RAM. |

---

### `[server]` — HTTP Listener, LAN Guard & Body Limits

| TOML Key | UI Label | Type | Default | Valid Range | Runtime Effect | Restart Needed | PS5 13.40 RAM/CPU Impact |
|---|---|---|---|---|---|---|---|
| `server.bind_host` *(alias `host`)* | *(TOML / CLI `--bind`)* | `String` | PS5: `"0.0.0.0"`, Host: `"127.0.0.1"` | Valid IP/hostname string | IP address the HTTP server binds to (via `SO_REUSEADDR` + `SO_REUSEPORT`). | **Yes** | None (`0.0.0.0` exposes dashboard to LAN devices). |
| `server.port` | *(TOML / CLI `--bind`)* | `u16` | `8080` | `1..=65535` | TCP listening port for the Web UI and `/api/v1/*` endpoints. | **Yes** | None. |
| `server.max_body_bytes` | *(TOML; also mirrored in `/settings`)* | `usize` | `1048576` (1 MiB) | `0` (256 MiB router cap) or `1024..=268435456` (1 KiB–256 MiB) | Request body limit (`DefaultBodyLimit` + `RequestBodyLimitLayer`) applied to `standard_api_routes` and `public_routes`. | **Yes** *(router built at boot)* | Limits worst-case per-request JSON heap allocation inside `< 64 MiB` RSS budget. |
| `server.upload_max_body_bytes` | `Max Upload / Attachment Body (bytes)` | `usize` | `67108864` (64 MiB) | `0` (256 MiB router cap) or `1024..=268435456` (1 KiB–256 MiB) | Body limit applied to `upload_api_routes` (`POST /conversations/{id}/messages`, `/files/upload`, `/tools`, `/memories/import`, `/extensions/skills/import`, `/extensions/plugins/import`, `/extensions/mcp`) and raises `read_workspace_file`/`write_workspace_file` byte caps. | **Partial** *(file helper cap updates live; Axum route layer needs restart)* | Uploaded payloads and base64 image data URLs buffer transiently in RAM during JSON decoding; keep `<= 64 MiB` on PS5. |
| `server.allow_lan_only` | *(TOML / API `server.allow_lan_only`)* | `bool` | `true` | `true` / `false` | Middleware guard (`security_network_middleware`) rejecting (`403 Forbidden`) any peer outside loopback (`127.0.0.0/8`, `::1`), RFC1918 (`10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`), or IPv6 ULA (`fc00::/7`). | No (live via `effective_config`) | Negligible CPU check per HTTP request; critical security boundary for plain-HTTP LAN mode. |

---

### `[database]` — SQLite / JSONL Engine & Retention

| TOML Key | UI Label | Type | Default | Valid Range | Runtime Effect | Restart Needed | PS5 13.40 RAM/CPU Impact |
|---|---|---|---|---|---|---|---|
| `database.driver` | *(TOML / API `database.driver`)* | `String` | `"auto"` | `"auto"`, `"sqlite"`, `"jsonl"` | Storage backend selection. `"auto"` opens SQLite (`agent.db`), runs `PRAGMA integrity_check`, and falls back to `JsonlStorage` only if SQLite fails completely. | **Yes** | SQLite uses bounded cache + disk; JSONL keeps state in memory (`BTreeMap`) and appends WAL files. |
| `database.path` | *(TOML only)* | `PathBuf` | `<data_dir>/database/agent.db` | Valid file path | Path to SQLite database file (JSONL mode uses sibling directory `<data_dir>/database/jsonl`). | **Yes** | None. |
| `database.busy_timeout_ms` | *(TOML / API `database.busy_timeout_ms`)* | `u32` | `5000` | Any `u32` (ms) | Applied as `PRAGMA busy_timeout = <ms>` when opening SQLite connections. | **Yes** | Prevents transient lock errors during concurrent DB writes. |
| `database.cache_size_kb` | *(TOML / API `database.cache_size_kb`)* | `i32` | `4000` (~4 MiB) | Non-zero `i32` (stored via `-abs().max(1)` KiB) | Applied as `PRAGMA cache_size = -<abs_kb>` (negative = KiB budget in SQLite). | **Yes** | Directly bounds SQLite page cache RAM (`4000` = ~4 MiB out of `< 64 MiB` RSS budget). |
| `database.wal_mode` | *(TOML / API `database.wal_mode`)* | `bool` | PS5: `false`, Host: `true` | `true` / `false` | Enables `PRAGMA journal_mode = WAL` on host targets. **PS5 override:** on `target_vendor = "sie"`, `SqliteStorage::configure_pragmas` always forces `PRAGMA journal_mode = TRUNCATE` regardless of `wal_mode` to avoid `.db-shm` `mmap(MAP_SHARED)` hazards on `/data`. | **Yes** | `TRUNCATE` eliminates `.db-shm` shared-memory `mmap` and keeps disk/RAM footprint predictable on PS5 13.40. |
| `database.retention_days` | `Database Retention (days)` | `u32` | `30` | `0..=36500` (`0` = keep forever) | Age threshold in days for pruning terminal tasks, task events, A2A messages, tool executions, audit logs, resolved approvals, and archived chat messages. | No (live on next 600s tick) | Smaller values shrink DB size, `PRAGMA incremental_vacuum`, and query scan time on `/data`. |
| `database.max_rows_per_table` | `Max Rows Per Table` | `usize` | `10000` | `0..=10000000` (`0` = unlimited) | Maximum rows kept per prunable table during startup and 600s periodic maintenance (never deletes active tasks, pending approvals, or unarchived chat messages). | No (live on next 600s tick) | Caps long-running daemon SSD and memory growth during multi-day runs. |

---

### `[security]` — Auth, Rate Limiting, Approvals & Argon2id

| TOML Key | UI Label | Type | Default | Valid Range | Runtime Effect | Restart Needed | PS5 13.40 RAM/CPU Impact |
|---|---|---|---|---|---|---|---|
| `security.auth_enabled` *(alias `auth`)* | `Authentication` | `bool` | `true` | `true` / `false` | Enforces session cookie + `X-CSRF-Token` (or `Authorization: Bearer`) on all `/api/v1/*` routes except `GET`/`HEAD /health`, `POST /auth/login`, and `POST /webhooks/inbound/{id}` (which validates per-webhook secret tokens). | No (live) | Negligible; session lookup is an indexed DB query. |
| `security.admin_username` *(alias `username`)* | `Admin Username` | `String` | `"admin"` | Non-empty string | Dashboard login username checked by `/api/v1/auth/login` and bootstrap credential creation. | No (live) | Zero RAM/CPU impact. |
| `security.admin_password` *(alias `password`)* | *(TOML only; cleared after hashing)* | `Option<String>` | `None` | `>= 8` chars if set; `PUT /settings` forbidden (`403`) | Optional plaintext bootstrap password in `config.toml`; hashed into `password_hash` at startup (`ensure_bootstrap_credentials_with_report`). | **Yes** | Hashing runs once at startup using `argon2_memory_kib`. |
| `security.password_hash` | *(TOML only; auto-managed)* | `Option<String>` | `None` | PHC Argon2id string (`PUT /settings` forbidden) | Stored Argon2id hash of the admin password; preferred over `bootstrap_credentials.txt` or DB `auth.password_hash`. | **Yes** *(use `POST /api/v1/auth/password` for live change)* | Verified on login/password-change using `argon2_memory_kib`. |
| `security.api_token_sha256` | *(TOML only)* | `Option<String>` | `None` | 64-char hex SHA-256 (`PUT /settings` forbidden) | Optional pre-hashed API Bearer token for headless/scripted clients (`Authorization: Bearer <raw_token>`). | **Yes** | Fast SHA-256 check per Bearer request; zero extra RAM. |
| `security.session_ttl_hours` | `Session TTL (hours)` | `u64` | `24` | `0..=87600` (`validate()` accepts `0`, while `auth.rs` clamps issued login lifetimes to `1..=87_600` hours so `0` emits `Max-Age=3600`; `> 87600` clamped/rejected) | Lifetime of newly issued dashboard login sessions in hours (`expires_at`). | No (live for new logins) | Longer TTLs keep session rows in DB longer (purged after expiry). |
| `security.rate_limit_per_minute` | `API Rate Limit (req/min)` | `u32` | `240` | `0..=100000` (`0` = unlimited) | Per-IP sliding-window (60s) cap for general HTTP requests (`SlidingWindowRateLimiter`, bounded by `MAX_TRACKED_IPS = 4096` and swept for expired 60s buckets every `64` checks, or `16` when `len > 64`). | No (live) | Protects PS5's 5-CPU budget against runaway polling loops; `0` disables limiter overhead. |
| `security.login_rate_limit_per_minute` | *(TOML / API `security.login_rate_limit_per_minute`)* | `u32` | `10` | Any `u32` (`0` = unlimited) | Stricter per-IP sliding-window (60s) cap on `POST /api/v1/auth/login` brute-force attempts. | No (live) | Prevents Argon2id CPU/RAM exhaustion from repeated login floods on PS5. |
| `security.require_approval_for_mutating` | `Mutating Tool Approval` | `bool` | `false` *(code default; `true` in `config.ps5-slim-1340.toml`)* | `true` / `false` | Legacy/override gate interacting with `sandbox.approval_policy` (`resolve_effective_approval_policy`): when `true` and `sandbox.approval_policy == "full_auto"`, effective policy promotes to `on_request` so tools with `requires_approval = true` pause in `TaskStatus::Waiting` (`pending_approvals` capped at `256`). When `false`, `sandbox.approval_policy` (`full_auto` default) governs prompts. | No (live) | Paused tasks keep minimal state in `pending_approvals`; `false` removes human gate overhead on autonomous sandbox workflows. |
| `security.argon2_memory_kib` | *(TOML only)* | `u32` | PS5: `8192` (8 MiB), Host: `19456` (~19 MiB) | `>= 1024` KiB (clamped `.max(1024)` in `hash_password`) | Argon2id memory parameter (`m_cost` in KiB; `t_cost = 2`, `p_cost = 1`) for password hashing. | **Yes** *(edit `config.toml`)* | Tuned down to `8192` KiB (8 MiB) on PS5 13.40 so logins do not spike process RSS near the `< 64 MiB` target. |

---

### `[agents]` — Concurrency, Task Caps & Specialist Lifecycle

| TOML Key | UI Label | Type | Default | Valid Range | Runtime Effect | Restart Needed | PS5 13.40 RAM/CPU Impact |
|---|---|---|---|---|---|---|---|
| `agents.max_agents` | `Max Agents` | `usize` | `0` *(PS5 preset: `0`)* | `0..=1024` in UI (`0` = unlimited) | Maximum total registered agents (built-in + custom + temporary) allowed in `AgentManager::create_agent`. | No (live) | Each agent is small SQLite/JSON metadata (`< 4 KiB` heap); `0` imposes no artificial cap. |
| `agents.max_active_tasks` | `Max Active Tasks` | `usize` | `0` *(PS5 preset: `0`)* | `0..=1024` in UI (`0` = unlimited -> `4096` permits) | Root-task execution semaphore (`task_semaphore`). Acquired **only by root tasks** (`parent_task_id.is_none()` / `task_id == root_task_id`), preventing deadlocks when root tasks delegate to child specialists. Queued pending tasks normally wait for a permit via `await_task_slot_or_expedite`, unless sent with `immediate: true` (**Send Now**) or woken via `POST /api/v1/tasks/{id}/expedite` (**Run Immediately**). | No (hot-swaps semaphore) | High concurrency increases simultaneous Tokio task stacks and message buffers; `0` lets `max_concurrent_inference` throttle GPU work. |
| `agents.max_queued_tasks` | `Max Queued Tasks` | `usize` | `0` *(PS5 preset: `0`)* | `0..=10000` in UI (`0` = unlimited) | Maximum sum of `pending + running + waiting` tasks before rejecting new root task creation with `LimitExceeded`. | No (live) | Minimal RAM per queued task row in SQLite; set finite cap only for burst backpressure. |
| `agents.max_concurrent_inference` | `Max Concurrent Inference` | `usize` | `4` *(PS5 preset: `4`)* | `0..=64` in UI (`0` = unlimited -> `4096` permits) | Simultaneous HTTP inference requests permitted across `AgentRuntime::inference_semaphore` and `Ps5LmProvider::semaphore`. Auto-synced to PS5LM `total_slots` when `auto_sync_from_ps5lm = true` and current value is `0`, `4`, or previously auto-synced. | No (hot-swaps both semaphores) | **Critical on PS5 13.40:** match PS5LM's `total_slots` (`1`–`4`). Higher values queue/saturate PS5LM's Vulkan/GDDR6 slots without increasing throughput. |
| `agents.max_messages_per_task` | `Max Messages Per Task` | `u32` | `0` *(PS5 preset: `0`)* | `0..=10000` in UI (`0` = unlimited) | Maximum A2A envelopes (`MessageBus`) per `task_id` before loop protection blocks further delegation. | No (live) | Prevents infinite A2A ping-pong loops consuming tokens and SQLite rows. |
| `agents.max_reasoning_iterations` | `Max Reasoning Iterations` | `u32` | `0` *(PS5 preset: `0`)* | `0..=1000` in UI (`0` = unlimited -> `256` safety cap) | Global ceiling on ReAct tool-use iterations per task (`resolve_max_iterations` takes `min(agent, global)` when both `> 0`; `0` defers to the other, or `UNLIMITED_REASONING_SAFETY_CAP = 256` when both are `0`). | No (live) | More iterations extend message history in memory and prompt token usage per task. |
| `agents.default_task_timeout_secs` | `Default Task Timeout (secs)` | `u64` | `0` | `0..=86400` in UI (`0` = unlimited) | Global default task wall-clock timeout persisted in `config.toml` (`AgentsConfig`). *(Note: `execute_task_with_timeout` currently enforces each agent's `AgentDefinition.max_task_duration_secs` directly, where `0` = no timeout and `> 0` has a `5s` floor.)* | No (live config update) | Prevents runaway unattended tasks when finite timeouts are configured on agents. |
| `agents.allow_temporary_specialists` | `Temporary Specialists` | `bool` | `true` | `true` / `false` | Allows Commander/agents to spawn one-off `is_temporary = true` specialist agents via `request_temporary_specialist` (passing `None` for both `max_task_duration_secs` and `max_reasoning_iterations` so `AgentManager::create_agent` inherits global `cfg.default_task_timeout_secs` / `cfg.max_reasoning_iterations`, matching `CreateAgentRequest` defaults of `0`/`0`). Temp agents are automatically deleted (`cleanup_temporary_agent`) and evicted from `pending_approvals` on task finish/error/timeout. | No (live) | Auto-cleanup prevents temp-agent memory/DB leaks on long-running PS5 sessions. |
| `agents.on_disable_policy` | `On Agent Disable Policy` | `String` | `"cancel"` | `"cancel"`, `"finish_step"` | Behavior when an agent is disabled mid-task via `POST /api/v1/agents/{id}/disable`: `"cancel"` cancels active tasks immediately; `"finish_step"` lets the active step finish. | No (live) | `"finish_step"` avoids half-written workspace files at the cost of one more step's latency. |

- **Task Queue Bypass, Expedite & Scoped Cancellation:**
  - **`immediate: true` / `Send Now` (`#chat-send-immediate-btn` on `SendMessageRequest`):** Root chat requests sent with `immediate: true` skip waiting on `agents.max_active_tasks` (`task_semaphore`) inside `await_task_slot_or_expedite` and execute immediately alongside active tasks.
  - **`POST /api/v1/tasks/{id}/expedite` (`Run Immediately` -> `{"task": task}`):** On a `Pending` task, `TaskManager::expedite_task` sets `progress_note = Some("Expedited — starting immediately")`, records `"task_expedited"`, emits `TaskUpdated`, and wakes `expedite_notifiers` so the task bypasses the `task_semaphore` wait immediately.
  - **`POST /api/v1/conversations/{id}/cancel` (`CancelConversationTasksRequest { agent_id: Option<String> }` -> `{"cancelled_count": count}`):** Cancels all non-terminal tasks for a conversation (optionally filtered to a single `agent_id`). While waiting in `await_task_slot_or_expedite`, cancellation immediately aborts pending tasks (`AgentError::Cancelled`), and `TaskManager::mark_running`, `mark_waiting`, and `update_progress` reject `TaskStatus::Cancelled` with `AgentError::Cancelled` so late runtime updates never resurrect a cancelled task.

---

### `[memory]` — Long-Term Memory & Chat Compaction

| TOML Key | UI Label | Type | Default | Valid Range | Runtime Effect | Restart Needed | PS5 13.40 RAM/CPU Impact |
|---|---|---|---|---|---|---|---|
| `memory.max_memories_per_agent` | `Max Memories Per Agent` | `usize` | `0` *(PS5 preset: `0`)* | `0..=1000000` in UI (`0` = unlimited) | Maximum active (unarchived) memory items per agent (`MemoryScope::Agent`); excess oldest/lowest-importance entries are automatically archived when writing new memories. | No (live) | High counts slightly increase SQLite scan time during context retrieval. |
| `memory.archive_after_messages` | `Archive After Messages` | `usize` | `40` | `0..=100000` in UI (`0` = disabled) | When active unarchived chat messages in a conversation exceed this threshold, older messages are compacted into a rolling memory summary leaving `keep_recent_messages` active. | No (live) | **High PS5LM benefit:** keeps prompt token count compact, reducing KV-cache pressure and prefill time on PS5LM. |
| `memory.keep_recent_messages` | *(TOML / API `memory.keep_recent_messages`)* | `usize` | `16` | Any `usize` | Number of most recent chat messages preserved unarchived when compaction runs. | No (live) | Lower values shrink prompt tokens; `16` preserves immediate turn context. |
| `memory.max_context_memories` | *(TOML / API `memory.max_context_memories`)* | `usize` | `8` | Any `usize` (`.max(1)` in `build_context_for_agent`) | Maximum memory items injected into an agent's system context per turn. | No (live) | Bounds prompt overhead from long-term memory retrieval. |
| `memory.max_memory_context_chars` | *(TOML / API `memory.max_memory_context_chars`)* | `usize` | `3200` | Any `usize` | Hard character cap on the formatted `<memory>` block injected into prompts. | No (live) | Keeps system prompt token footprint predictable (~800 tokens). |

---

### `[sandbox]` — Execution Modes, Approval Policy, Web Search & Brain Injection

All `[sandbox]` keys can be configured in `config.toml`, via `PS5_AGENT_*` environment variables, or at runtime via `GET`/`PUT /api/v1/sandbox` (and the dashboard **Sandbox** panel). When updated via `PUT /api/v1/settings` or `PUT /api/v1/sandbox`, `sync_sandbox_and_tools()` keeps overlapping `[sandbox]` and `[tools]` keys synchronized (note that `PUT /api/v1/settings` rejects `sandbox.brave_api_key` and `sandbox.tavily_api_key` with `403 Forbidden`; update those via `PUT /api/v1/sandbox`, env vars, or `config.toml`).

| TOML Key | UI Label | Type | Default | Valid Range | Runtime Effect | Restart Needed | PS5 13.40 RAM/CPU Impact |
|---|---|---|---|---|---|---|---|
| `sandbox.mode` | `Sandbox Mode` (`#sandbox-mode-select`) | `String` (`SandboxMode`) | `"full_sandbox"` | `"read_only"`, `"workspace_write"`, `"full_sandbox"` (`SandboxMode::parse` also normalizes hyphens/no-separator/`"full"` -> `"full_sandbox"`) | `read_only` blocks mutating tools, `doctor fix=true`, mutating `json_api_request`, browser `download`/`save_snapshot`/`submit`, and `sandbox_exec` writes (`>`/`>>`/`sed -i`) while allowing read-only `virtual` `sandbox_exec`. `workspace_write` allows workspace file mutations and read-only web calls while blocking `http_post`, mutating `json_api_request`, custom/extension `WebWrite` mutating tools, and host shell. `full_sandbox` permits all sandboxed operations. | No (live) | Limits filesystem/network/shell blast radius without spawning OS containers. |
| `sandbox.approval_policy` | `Approval Policy` (`#sandbox-approval-select`) | `String` (`SandboxApprovalPolicy`) | `"full_auto"` | `"full_auto"` (alias `"auto"`), `"on_request"`, `"always_ask"`, `"never"` | `full_auto` skips approval for all `41` built-in tools (non-built-in `requires_approval = true` tools prompt in interactive chat, or run autonomously in background/scheduled turns if `security.require_approval_for_mutating = false`). `on_request` prompts when `requires_approval = true` (auto-promoted from `full_auto` when `security.require_approval_for_mutating = true`). `always_ask` prompts whenever `is_mutating \|\| requires_approval`. `never` rejects `requires_approval = true` tools with `PermissionDenied`. | No (live) | Zero runtime overhead; interactive approvals pause task turns until resolved. |
| `sandbox.network_access` | `Allow Network Access` (`#sandbox-network-access`) | `bool` | `true` | `true` / `false` | Master outbound network toggle for web tools, remote browser, MCP HTTP calls, and HTTP custom tools (synced with `tools.network_access`). | No (live) | Disabling avoids all outbound sockets/TLS buffers. |
| `sandbox.allow_local_network` | `Allow Local Network (LAN)` (`#sandbox-allow-local-net`) | `bool` | `false` | `true` / `false` | When `true`, allows outbound tool HTTP calls to loopback/RFC1918 LAN/link-local targets (synced with `tools.allow_local_network` and `!tools.block_private_lan_web_requests`). | No (live) | Protect PS5 local ports (`127.0.0.1:8081`, `:9021`) by leaving `false` unless needed. |
| `sandbox.web_search_provider` | `Web Search Provider` (`#sandbox-search-provider`) | `String` | `"duckduckgo"` | `"duckduckgo"`, `"searxng"`, `"brave"`, `"tavily"` | Selects the backend used by the `web_search` tool (synced with `tools.web_search_provider`). | No (live) | Negligible; external HTTP search request per search call. |
| `sandbox.searxng_url` | `SearXNG Base URL` (`#sandbox-searxng-url`) | `String` | `""` | HTTP/HTTPS base URL | SearXNG instance URL used when `web_search_provider = "searxng"` (synced with `tools.searxng_url`). | No (live) | Negligible. |
| `sandbox.brave_api_key` | *(API `PUT /api/v1/sandbox`, env `PS5_AGENT_BRAVE_API_KEY`, or TOML; `403` on `/settings`)* | `String` | `""` | Secret API key | API key for Brave Search when `web_search_provider = "brave"` (redacted in `GET /api/v1/sandbox` via `has_brave_api_key`; synced with `tools.brave_api_key`). | No (live via `/sandbox`) | Negligible. |
| `sandbox.tavily_api_key` | *(API `PUT /api/v1/sandbox`, env `PS5_AGENT_TAVILY_API_KEY`, or TOML; `403` on `/settings`)* | `String` | `""` | Secret API key | API key for Tavily Search when `web_search_provider = "tavily"` (redacted in `GET /api/v1/sandbox` via `has_tavily_api_key`; synced with `tools.tavily_api_key`). | No (live via `/sandbox`) | Negligible. |
| `sandbox.remote_browser_url` | `Remote Browser URL` (`#sandbox-remote-browser-url`) | `String` | `""` | WebDriver / CDP HTTP endpoint URL | Remote headless browser control endpoint used by the `browser` tool and `/api/v1/sandbox/browser/*` routes when configured (synced with `tools.remote_browser_url`). | No (live) | Offloads headless browser RAM/CPU off the PS5 console. |
| `sandbox.allow_host_shell` | `Allow Host Shell` (`#sandbox-allow-host-shell`) | `bool` | `false` | `true` / `false` | Enables `"host"` execution mode in `sandbox_exec` **only when both** `allow_host_shell = true` **and** effective `sandbox_mode == full_sandbox` (synced with `tools.allow_host_shell`). | No (live) | Keep `false` on PS5 unless explicit host-level execution is intended. |
| `sandbox.max_exec_output_bytes` | `Max Exec Output Bytes` (`#sandbox-max-exec-bytes`) | `usize` | `262144` (256 KiB) | Any `usize` (`sandbox_exec` treats `0` as default `262144`, otherwise clamps `.max(MIN_EXEC_OUTPUT_BYTES)`) | Caps combined stdout/stderr captured by `sandbox_exec` (synced with `tools.max_exec_output_bytes`). | No (live) | Prevents runaway command output from spiking `< 64 MiB` RSS. |
| `sandbox.exec_timeout_secs` | `Exec Timeout (secs)` (`#sandbox-exec-timeout-secs`) | `u64` | `20` | `0` (unlimited) or seconds | Execution timeout applied to both `"virtual"` and `"host"` `sandbox_exec` invocations (synced with `tools.exec_timeout_secs`). | No (live) | Bounds stuck subprocess/virtual execution time. |
| `sandbox.inject_brain_files` | `Inject Brain Prompt Files` (`#sandbox-inject-brain`) | `bool` | `true` | `true` / `false` | Controls whether workspace/data-dir brain markdown files (`SOUL.md`, `IDENTITY.md`, `USER.md`, `AGENTS.md`, `TOOLS.md`, `MEMORY.md`, `HEARTBEAT.md`) are injected into agent system prompts. | No (live) | Small prompt token overhead bounded by `max_brain_context_chars`. |
| `sandbox.max_brain_context_chars` | `Max Brain Prompt Chars` (`#sandbox-max-brain-chars`) | `usize` | `4000` | Any `usize` (`build_brain_prompt_section` returns empty when `0`, disabling brain prompt injection) | Character budget across injected brain markdown files (`0` disables injection even though the HTML label notes `0 = unlimited`). | No (live) | Controls prompt context size against KV-cache limits. |
| `sandbox.heartbeat_interval_secs` | `Heartbeat Interval (secs)` (`#sandbox-heartbeat-secs`) | `u64` | `0` | `0` (disabled) or interval in seconds | Interval for autonomous `tick_heartbeat_if_due` checks driven by `HEARTBEAT.md` (ignoring `#` comment and blank lines); read dynamically from runtime config on each tick. | No (live) | Keep `0` unless periodic background checks are desired; each firing runs an inference turn when non-empty. |

---

### `[tools]` — Filesystem Sandbox & Web SSRF Guard

| TOML Key | UI Label | Type | Default | Valid Range | Runtime Effect | Restart Needed | PS5 13.40 RAM/CPU Impact |
|---|---|---|---|---|---|---|---|
| `tools.allowed_paths` | *(TOML / API `tools.allowed_paths`)* | `Vec<String>` | `[<data_dir>/workspace]` | Non-empty list of directory paths (empty resets to `[<data_dir>/workspace]` with warning) | Canonicalized directory whitelist enforced for all file tools (`read_file`, `write_file`, `list_files`, `create_directory`, `delete_file`) and workspace file endpoints (`/api/v1/files*`). | No (live) | None; protects PS5 OS/HEN paths (`/data`, `/system`, `/user`) from accidental writes. |
| `tools.allowed_domains` | *(TOML / API `tools.allowed_domains`)* | `Vec<String>` | `["*"]` | Domain patterns (`"*"` = all public domains; exact or suffix match; empty repairs to `["*"]` when `network_access = true`) | Hostname allowlist checked by `web_search`, `web_fetch`, `http_get`, `http_post`, `json_api_request`, `fetch_website`, `extract_html_text`, and remote MCP/plugin HTTP requests. | No (live) | Negligible string check per outbound HTTP call. |
| `tools.blocked_domains` | `Blocked Domains` (`#sandbox-blocked-domains`) | `Vec<String>` | `[]` | Comma/newline-separated domain patterns (exact or suffix match) | Hostname denylist evaluated before `allowed_domains` for web/browser/MCP/custom HTTP requests. | No (live) | Negligible string check per outbound HTTP call. |
| `tools.block_private_lan_web_requests` | *(TOML / API `tools.block_private_lan_web_requests`)* | `bool` | `true` | `true` / `false` | SSRF protection blocking tools from fetching loopback, RFC1918 LAN, link-local (`169.254.0.0/16`), or IPv6 ULA/loopback targets (unless an MCP server explicitly sets `trust_local = true`; synced with `!sandbox.allow_local_network`). | No (live) | Negligible; protects LAN services and PS5 loopback (`127.0.0.1:8081` PS5LM, `:9021` elfldr) from agent SSRF. |
| `tools.default_timeout_secs` | *(TOML / API `tools.default_timeout_secs`)* | `u64` | `20` | Any `u64` (`.max(1)` for `reqwest` web client & built-in tool specs) | Default execution timeout in seconds for built-in tools and the shared outbound `reqwest::Client`. | No (rebuilds tool HTTP client) | Prevents hung external HTTP sockets from tying up Tokio worker threads. |
| `tools.max_file_bytes` | *(TOML / API `tools.max_file_bytes`)* | `usize` | `2097152` (2 MiB) | `0` (unlimited) or byte cap | Maximum file size read/written/appended by agent file tools (`read_file`, `write_file`, etc.). Workspace HTTP upload/download uses `max(max_file_bytes, upload_max_body_bytes)` (or unlimited if `upload_max_body_bytes == 0`). | No (live) | Prevents large files from exhausting `ps5-agent`'s `< 64 MiB` RSS budget or overflowing LLM context. |
| `tools.max_web_bytes` | *(TOML / API `tools.max_web_bytes`)* | `usize` | `524288` (512 KiB) | Any `usize` (`.max(1024)` enforced on HTTP fetches) | Maximum response body read by web tools, MCP HTTP calls, and plugin template HTTP requests. | No (live) | Caps transient response buffer allocations on PS5. |

> **Mirrored `[tools]` / `[sandbox]` Keys (`sync_sandbox_and_tools`):** `tools.network_access`, `tools.allow_local_network`, `tools.web_search_provider`, `tools.searxng_url`, `tools.brave_api_key`, `tools.tavily_api_key`, `tools.remote_browser_url`, `tools.allow_host_shell`, `tools.max_exec_output_bytes`, and `tools.exec_timeout_secs` are also present on `ToolsConfig` and kept synchronized with their matching `sandbox.*` keys.

---

### `[logging]` — Rotating File, Ring Buffer, `klog` & Toast

| TOML Key | UI Label | Type | Default | Valid Range | Runtime Effect | Restart Needed | PS5 13.40 RAM/CPU Impact |
|---|---|---|---|---|---|---|---|
| `logging.level` | `Logging Level` | `String` | `"info"` | `"error"`, `"warn"`, `"info"`, `"debug"`, `"trace"` | Minimum severity written to the in-memory ring buffer, rotating log file, and PS5 `klog`. | No (live via `logger.apply_config` / `set_level`) | `debug`/`trace` increase SSD I/O on `/data` and `klog` chatter; keep `"info"` normally. |
| `logging.log_dir` | *(TOML only)* | `PathBuf` | `<data_dir>/logs` | Writable directory path | Directory storing rotating `ps5-agent.log` files. | **Yes** | Disk usage only. |
| `logging.max_file_bytes` | *(TOML / API `logging.max_file_bytes`)* | `usize` | `2097152` (2 MiB) | Any `usize` | Maximum size per rotating log file before rotation. | No (live via `logger.apply_config`) | Bounds `/data` SSD space consumed by logs. |
| `logging.ring_buffer_capacity` | *(TOML / API `logging.ring_buffer_capacity`)* | `usize` | `500` | Any `usize` | Maximum recent log entries retained in RAM for the dashboard **Logs** tab (`GET /api/v1/logs`). | No (live via `logger.apply_config`) | `500` entries uses `< 256 KiB` RAM; keep small on PS5. |
| `logging.enable_klog` | *(TOML / API `logging.enable_klog`)* | `bool` | PS5: `true`, Host: `false` | `true` / `false` | Mirrors log lines to PS5 kernel log (`ps5::klog::log` -> port `9081`/UART) on `target_vendor = "sie"`. | No (live via `logger.apply_config`) | Tiny syscall cost per log line; invaluable for debugging PS5 13.40 crashes. |
| `logging.enable_toast` | *(TOML / API `logging.enable_toast`)* | `bool` | PS5: `true`, Host: `false` | `true` / `false` | Shows PS5 system toast notifications (`ps5::Toast` / `ps5::notify::simple`) on startup (including first-boot password or `.broken.bak` recovery warning). | No (live via `logger.apply_config`; startup alerts trigger at boot) | Zero steady-state overhead (startup-only). |

---

### `[runtime]` — Tokio Threads, Health Polling & Rest Mode

| TOML Key | UI Label | Type | Default | Valid Range | Runtime Effect | Restart Needed | PS5 13.40 RAM/CPU Impact |
|---|---|---|---|---|---|---|---|
| `runtime.tokio_worker_threads` | *(TOML / API `runtime.tokio_worker_threads`)* | `usize` | `2` | `1..=8` validated; **clamped `1..=3` on PS5** (`target_vendor = "sie"`) in `src/main.rs` | Number of Tokio multi-threaded runtime worker threads. | **Yes** | **Critical on PS5 13.40:** payloads are restricted to 5 CPUs (`0xea00`); `2` workers (`<= 3` cap) avoid starving Sony OS/PS5LM threads. |
| `runtime.health_poll_interval_secs` | *(TOML / API `runtime.health_poll_interval_secs`)* | `u64` | `15` | `>= 5` enforced at spawn (`interval_secs.max(5)`) | Interval for the background PS5LM health polling loop (which also triggers capability auto-sync when `auto_sync_interval_secs` has elapsed). | **Yes** | Minimal localhost poll every `15s`. |
| `runtime.rest_mode_recovery` | *(TOML / API `runtime.rest_mode_recovery`)* | `bool` | `true` | `true` / `false` | Configuration flag documenting/enabling Rest-Mode wake recovery awareness; `RecoveringTcpListener` (`crates/ps5-agent-http/src/server.rs`) automatically detects Sony OS error `163`, sets `network_degraded`, emits `StreamEvent::SystemAlert`, and rebinds with `250ms`–`5s` backoff. | No (`config` persisted) | Negligible; avoids fatal listener termination on PS5 Firmware `13.x` sleep/wake. |

---

## 4. Per-Agent Settings & Built-In Agents (`AgentDefinition` & `AgentModelConfig`)

Configured via **Agents -> Create Agent / Edit** (`openAgentModal` in [`app.js`](../crates/ps5-agent-dashboard/assets/app.js)) or `POST /api/v1/agents` / `PUT /api/v1/agents/{id}`. All changes apply immediately without a restart.

### `AgentModelConfig` (`model_config`)

| Field | UI Label | Type | Default | Valid Range | Runtime Effect & PS5 13.40 Impact |
|---|---|---|---|---|---|
| `provider` | *(API only)* | `String` | `"ps5lm"` | `"ps5lm"` | Inference provider identifier. |
| `model_override` | `Model Override` | `Option<String>` | `None` *(empty = inherit)* | Optional model ID string | Overrides global `inference.model` for this agent. |
| `api_mode` | `API Mode` | `ApiMode` | `"chat_completions"` | `chat_completions`, `responses`, `anthropic_messages`, `completions`, `system_one` | Per-agent PS5LM wire protocol override. |
| `thinking_level` | `Thinking Level` (`#agent-model-thinking`) | `ThinkingLevel` | `"medium"` | `"off"`, `"low"`, `"medium"`, `"high"`, `"max"` | Default reasoning depth for this agent when a chat message does not explicitly specify `thinking_level`. |
| `temperature` | `Temperature` | `f32` | `0.4` *(built-ins differ; see table below)* | `0.0..=2.0` (step `0.05` in UI) | Sampling temperature (`0.0` = deterministic, higher = more creative). Zero PS5 RAM impact. |
| `context_limit` | `Context Limit (0 = Default)` | `u32` | `8192` | `0` (inherit/PS5LM default) or `> 0` up to `1048576` | Per-agent token context budget (`InferenceRequest.context_limit`). |
| `max_output_tokens` | `Max Output Tokens (0 = Unlimited)` | `u32` | `1024` *(`AgentModelConfig::default()`; built-in agents seed `0`)* | `0` (unlimited) or `< context_limit` when both `> 0` | Per-agent reply token cap. Setting `0` prevents mid-file truncation during large code generation. |

### `AgentDefinition` Operational Fields

| Field | UI Label | Type | Default (Modal / `#agent-modal`) | Valid Range | Runtime Effect & PS5 13.40 Impact |
|---|---|---|---|---|---|
| `name` | `Name` | `String` | `""` | Non-empty string | Display name and `/agent <name>` / `@name` routing target. |
| `role` | `Role` | `AgentRole` | `"CUSTOM"` | `COMMANDER`, `RESEARCHER`, `PROGRAMMER`, `ANALYST`, `EXECUTOR`, `CUSTOM` | Semantic role tag for orchestration and UI badges. |
| `role_label` | `Role Label` | `String` | `"Specialist"` | String | Human-readable subtitle shown on agent cards. |
| `description` | `Description` | `String` | `""` | String | Used by the Group Coordinator when selecting specialists for a user prompt. |
| `system_prompt` | `System Prompt` | `String` | `"You are a helpful specialist agent."` | String | Base instructions prepended to every turn; skills & plugin rules are appended at runtime. |
| `max_task_duration_secs` | `Max Task Duration (secs, 0 = unlimited)` (`#ag-max-dur` in `openAgentModal`, `#agent-max-duration` static DOM) | `u64` | `0` | `0..=86400` (`0` = unlimited; `> 0` enforces `.max(5)` floor in runtime/manager) | Wall-clock timeout for a single task executed by this agent. |
| `max_reasoning_iterations` | `Max Reasoning Iterations (0 = unlimited)` (`#ag-max-reasoning-iters` in `openAgentModal`, `#agent-max-reasoning-iters` static DOM) | `u32` | `0` | `0..=1000` (`0` = unlimited / inherit global; `256` safety cap if both are `0`) | Maximum ReAct tool-loop steps for this agent (`resolve_max_iterations`). |
| `priority` | `Priority` | `i32` | `5` | `-10..=100` in UI | Task priority assigned to tasks created for this agent. |
| `can_communicate` | `Can Communicate with Other Agents (A2A)` | `bool` | `true` | `true` / `false` | Enables or blocks sending/receiving A2A envelopes on `MessageBus`. |
| `enabled_tools` | `Enabled Tools` | `Vec<String>` | `[]` | Built-in IDs, `"mcp:<srv>:<tool>"`, `"plugin:<id>:<tool>"`, custom IDs, `group:*` groups, or wildcards `"*"`, `"mcp:*"`, `"plugin:*"`, `"custom:*"`, prefix `foo*` | Allowlist matched by `agent_allows_tool` / `agent_allows_tool_with_policy` / `expand_tool_group`. Supported groups in `expand_tool_group`: `group:fs`, `group:web`, `group:runtime`, `group:memory`, `group:sessions`, `group:automation`, `group:utility`, and `group:openclaw` (`"group:*"` expands to `"group:openclaw"`). `"custom:*"` matches tools with `category == "custom"`, `id` prefix `"custom:"`, or definitions managed by `CustomToolManager`. Built-in `"load_skill"` is always allowed when enabled skills exist. |
| `denied_tools` | `Denied Tools / Groups` (`#agent-denied-tools`) | `Vec<String>` | `[]` | Exact IDs, aliases, `"*"`, `"custom:*"`, `"mcp:*"`, `"plugin:*"`, prefix wildcards, or `group:*` | Explicit denylist evaluated by `agent_allows_tool_with_policy`; **always overrides `enabled_tools`** (deny-wins). |
| `sandbox_mode` | `Sandbox Mode Override` (`#agent-sandbox-mode`) | `Option<String>` | `None` *(inherit)* | `None` (inherit global `sandbox.mode`), `"read_only"`, `"workspace_write"`, or `"full_sandbox"` | Per-agent sandbox mode override resolved by `resolve_effective_sandbox_mode` (`normalize_optional_field` converts blank/whitespace to `None`). |
| `tool_profile` | *(API / DB `agents.tool_profile`)* | `Option<String>` | `None` | Optional non-empty profile identifier (`normalize_optional_field` converts blank to `None`) | Optional profile label persisted on `AgentDefinition`, SQLite `agents`, and HTTP DTOs (runtime tool authorization evaluates `enabled_tools` and `denied_tools`). |
| `memory_permissions` | `Memory Permissions` | `MemoryPermissions` | `read_own/write_own/read_shared/write_shared/read_user_prefs: true`, `write_user_prefs/cross_agent_read: false` | 7 booleans | Fine-grained access control over `MemoryScope` reads/writes (`cross_agent_read` gates `read_agent_memory`). |

### Built-In Agents, Schema Upgrade (`BUILTIN_AGENTS_SCHEMA_VERSION = "6"`) & Bulk Enable

`default_agent_definitions()` (`crates/ps5-agent-manager/src/lib.rs`) seeds five built-in agents—all configured with `max_output_tokens = 0`, `max_task_duration_secs = 0`, `max_reasoning_iterations = 0`, `denied_tools = []`, `sandbox_mode = None`, `tool_profile = None`, and `enabled = true` so multi-step reasoning and file generation are never prematurely cut off:

| Built-In Agent ID | Role | Temperature | Default Thinking | Priority | Default Extension Wildcards |
|---|---|---|---|---|---|
| `commander` *(protected)* | `Commander` | `0.35` | `medium` | `100` | `mcp:*`, `plugin:*`, `custom:*`, `load_skill` |
| `researcher` | `Researcher` | `0.25` | `medium` | `70` | *(read/web/file tools; no `custom:*` by default)* |
| `programmer` | `Programmer` | `0.25` | `high` | `70` | `mcp:*`, `plugin:*`, `custom:*`, `load_skill` |
| `analyst` | `Analyst` | `0.2` | `medium` | `65` | *(analysis/file/metrics tools)* |
| `executor` | `Executor` | `0.15` | `medium` | `60` | *(workspace ops & HTTP tools)* |

- **Automatic Upgrade (`AgentManager::upgrade_builtin_agents`):** Runs on startup and migrates earlier schema versions to `"6"`: finite legacy `max_task_duration_secs` / `max_reasoning_iterations` limits are upgraded to `0`, newly added default tool patterns are merged without removing user additions, and unedited built-in prompts/model settings update automatically while leaving `denied_tools: []`, `sandbox_mode: None`, and `tool_profile: None` initialized cleanly.
- **Boot / Enable All Agents (`POST /api/v1/agents/enable-all`, UI `#enable-all-agents-btn`):** Calls `AgentManager::enable_all_agents()` to re-enable every disabled agent in one action, emits `agent.enabled` events + audit log (`system.enable_all_agents`), and returns `{"enabled_count": <n>}`.

---

## 5. Thinking Levels (`off` / `low` / `medium` / `high` / `max`), Reasoning Format & Coordinator Selection

`ThinkingLevel` (`crates/ps5-agent-core/src/models.rs`) controls reasoning depth across three priority layers:

1. **Per-Message Override** (highest priority): Dashboard chat toolbar selector `#thinking-level-select` (`Thinking: Off/Low/Medium/High/Max`) or `thinking_level` in `POST /api/v1/conversations/{id}/messages`.
2. **Per-Agent Default**: `AgentModelConfig.thinking_level` (`#agent-model-thinking` in Agent modal).
3. **Global Config Default**: `inference.thinking_level` (`Default Thinking Level` in Settings, default `"medium"`).

### Wire Protocol Mapping (`crates/ps5-agent-inference/src/provider.rs`)

For `chat_completions`, `responses`, and `completions`, `apply_llamacpp_sampling_fields` also attaches `"reasoning_format": cfg.reasoning_format` (default `"deepseek"`) whenever `thinking_level != off` and `cfg.reasoning_format` is non-empty and not `"none"`.

| `ThinkingLevel` | `chat_completions` (`/v1/chat/completions`) | `responses` (`/v1/responses`) | `anthropic_messages` (`/v1/messages`) | `completions` (`/v1/completions`) | `system_one` |
|---|---|---|---|---|---|
| `off` | `"chat_template_kwargs": {"enable_thinking": false}`, `"reasoning_budget": 0`, `"reasoning_format": "none"` *(no `reasoning_effort`)* | *(omits `"reasoning"` & `"reasoning_format"`)* | *(omits `"thinking"`)* | Appends `\n/no_think` to system or last user prompt | N/A |
| `low` | `"chat_template_kwargs": {"enable_thinking": true}`, `"reasoning_effort": "low"`, `"reasoning_budget": 512`, plus `"reasoning_format"` if active | `"reasoning": {"effort": "low"}` + `"reasoning_format"` | `"thinking": {"type": "enabled", "budget_tokens": min(1024, max_tokens - 1)}` | Standard prompt + `"reasoning_format"` | N/A |
| `medium` *(default)* | `"chat_template_kwargs": {"enable_thinking": true}`, `"reasoning_effort": "medium"`, `"reasoning_budget": 2048`, plus `"reasoning_format"` if active | `"reasoning": {"effort": "medium"}` + `"reasoning_format"` | `"thinking": {"type": "enabled", "budget_tokens": min(4096, max_tokens - 1)}` | Standard prompt + `"reasoning_format"` | N/A |
| `high` | `"chat_template_kwargs": {"enable_thinking": true}`, `"reasoning_effort": "high"`, `"reasoning_budget": 8192`, plus `"reasoning_format"` if active | `"reasoning": {"effort": "high"}` + `"reasoning_format"` | `"thinking": {"type": "enabled", "budget_tokens": min(8192, max_tokens - 1)}` | Standard prompt + `"reasoning_format"` | N/A |
| `max` | `"chat_template_kwargs": {"enable_thinking": true}`, `"reasoning_effort": "high"`, `"reasoning_budget": -1`, plus `"reasoning_format"` if active | `"reasoning": {"effort": "high"}` + `"reasoning_format"` | `"thinking": {"type": "enabled", "budget_tokens": min(16384, max_tokens - 1)}` | Standard prompt + `"reasoning_format"` | N/A |

- **Group Chat Coordinator Selection (`coordinate_group_participants`, `crates/ps5-agent-runtime/src/runtime.rs`):** Group routing calls `inference.generate(...)` with `thinking_level: ThinkingLevel::Off`, `temperature: 0.0`, `max_output_tokens: 128`, `stream: false`, and `tools: []`. Because `ThinkingLevel::Off` explicitly sets `enable_thinking: false`, `reasoning_budget: 0`, and `reasoning_format: "none"`, reasoning-capable models output compact `{"selected_agents": [...]}` JSON immediately instead of burning the 128-token budget inside `<think>` tags. As a second layer of defense, the parser strips `<think>` and `<seed:think>` blocks (`extract_think_blocks_text`) and Markdown fences, falling back to `@agent_id` mentions or Commander direct response.
- **UI Display, Think-Tag Isolation & Raw Output Mode (`ThinkStreamSplitter`, `parser.rs`):** Streamed `<think>...</think>`, `<seed:think>...</seed:think>` (`THINK_OPEN_TAGS` / `THINK_CLOSE_TAGS`), or `reasoning_content` tokens are separated by `ThinkStreamSplitter` into `StreamEvent::ChatReasoning` and rendered inside a collapsible `<details class="thinking-drawer">` drawer in Main Chat, while final reasoning is carried cleanly on `StreamEvent::ChatCompleted.reasoning` (`Option<String>`) and task state events carry `StreamEvent::TaskUpdated.conversation_id` (`Option<String>`). In `ThinkStreamSplitter::push`, leading whitespace-only chunks do not lock `emitted_any_text = true`, so when a model template pre-fills an opening tag and streams an orphan closing `</think>` / `</seed:think>` (`!emitted_any_text && !closed_any_think`), pre-close reasoning/draft text is routed cleanly into `StreamPiece::Reasoning` instead of leaking into the final chat bubble. Enabling **Show raw output** (`#chat-raw-output-toggle`, persisted in `localStorage` under `ps5_agent_chat_raw_output`) switches message rendering and live stream updates to raw `<think>` + verbatim output inside `<pre class="msg-raw-output">`.
- **PS5 13.40 Guidance:** Use `off` or `low` for quick Q&A or simple file reads on 7B–14B quantized GGUFs; reserve `high` / `max` for complex multi-file coding or debugging, and pair `max` with `max_output_tokens = 0` (or `>= 4096`) so reasoning tokens do not starve final output.

---

## 6. Unlimited `0` Limits

To prevent silent truncation on long builds or multi-agent workflows, `0` consistently means **unlimited** (or **defer to server/safety ceiling / feature disabled**, except `prefill_kv_cache`'s `max_tokens: 0` / `n_predict: 0` prefill probe) across config, agent, HTTP query, and runtime layers:

| Setting | Internal Behavior When Set to `0` |
|---|---|
| `inference.context_limit` | Skips `512..=1048576` clamp; defers to PS5LM's configured `n_ctx`. |
| `inference.max_output_tokens` *(global & agent)* | Omits `max_tokens` / `max_output_tokens` on `chat_completions`, `responses`, and `completions` so PS5LM generates up to `n_ctx`. (`anthropic_messages` sends `ANTHROPIC_DEFAULT_MAX_TOKENS = 8192` because Anthropic's API requires `max_tokens`.) Combined with `MAX_AUTO_CONTINUATIONS = 4`, long or truncated outputs continue automatically when `finish_reason == "length" || has_unclosed_code_fence` (and once complete, `>= 10`-line fenced script blocks extracted via `extract_all_fenced_script_blocks` are auto-transported to the workspace via the enabled write tool with `approved_bypass = true`, emitting `StreamEvent::FilesChanged` and replacing the code block with ``Created `{filename}` ({line_count} lines) and transported it to the Files tab.\n[FILE_CREATED: {filename}]``, while duplicate `>= 15`-line inline echoes are cleaned up when `write_file` already wrote them). *(Note: background `prefill_kv_cache` probes force `max_tokens: 0` and `n_predict: 0` so PS5LM warms the prompt KV prefix while generating zero completion tokens.)* |
| `inference.timeout_secs` | Disables the per-request HTTP deadline (`effective_request_timeout(0) -> None` in `provider.rs`) so long PS5LM generations wait without expiring on request duration. |
| `inference.connect_timeout_secs` | Skips `reqwest::ClientBuilder::connect_timeout` on the client builder (`probe_timeout` falls back to `5s` for capability probes). |
| `inference.auto_sync_interval_secs` | Disables periodic background capability discovery (manual **Sync Settings from PS5LM Now** and `/health` polling remain active). |
| `inference.max_retries` | Disables pre-response TCP connect retries (`0` retries). |
| `inference.top_k`, `min_p`, `dynatemp_range`, `xtc_probability`, `dry_multiplier`, `repeat_last_n`, `dry_penalty_last_n`, `mirostat`, `n_keep` | Disables the corresponding `llama.cpp` sampler or penalty lookback (`repeat_penalty = 1.0` and `typical_p = 1.0` disable those samplers; `-1` uses full context on lookbacks / random seed on `seed` / auto-slot on `id_slot`). |
| `server.max_body_bytes` & `server.upload_max_body_bytes` | Router body limit layers lift to the maximum `256 MiB` (`256 * 1024 * 1024`) ceiling (effective at startup); workspace HTTP file read/write/upload helpers skip the `max_file_bytes` cap when `upload_max_body_bytes == 0`. |
| `agents.max_agents` | Disables agent count limit in `AgentManager::create_agent`. |
| `agents.max_active_tasks` | Maps `task_semaphore` to `UNLIMITED_SEMAPHORE_PERMITS = 4096` (hot-swapped live). |
| `agents.max_queued_tasks` | Skips queue-depth rejection in `TaskManager::create_task`. |
| `agents.max_concurrent_inference` | Maps `inference_semaphore` and `Ps5LmProvider` semaphore to `UNLIMITED_CONCURRENCY_PERMITS = 4096` (hot-swapped live). |
| `agents.max_messages_per_task` | Disables per-task A2A envelope count checks in `MessageBus::send`. |
| `agents.max_reasoning_iterations` *(global & agent)* | If one is `0`, uses the other. If **both** are `0` (as on all 5 built-in agents and `config.toml` defaults), `opts.max_iters` is `None` (`execute_react_loop` uses `UNLIMITED_REASONING_SAFETY_CAP = 256`). Task progress notes reflect this cleanly: step `0` (`format_thinking_start_note`) displays `"Generating response…"` (`Off`), `"Thinking (low)…"`, `"Thinking (medium)…"`, `"Thinking (high)…"`, or `"Thinking (max)…"`; tool steps (`iter >= 1`) display `"Tool call step {iter}"` instead of a misleading `1/8` denominator (`"Tool call step {iter}/{limit}"` appears only when a positive cap is set). |
| `agents.default_task_timeout_secs` & `agent.max_task_duration_secs` | `execute_task_with_timeout` awaits the task future without a `tokio::time::timeout` wrapper. |
| `security.rate_limit_per_minute` & `login_rate_limit_per_minute` | Bypasses the corresponding `SlidingWindowRateLimiter` check. |
| `security.session_ttl_hours` | Passes `validate()` (`0..=87600`, `"0 (unlimited)"`), though `auth.rs` clamps issued session lifetimes to `1..=87_600` hours (`0` yields a `1`-hour session at login). |
| `memory.max_memories_per_agent` | Disables auto-archiving of older `MemoryScope::Agent` memories on write. |
| `memory.archive_after_messages` | Disables automatic conversation-message archiving/compaction. |
| `tools.max_file_bytes` | Disables size check in workspace/tool file reads and writes (`files.rs`). |
| `sandbox.exec_timeout_secs` | Disables per-command execution timeouts in both virtual and host `sandbox_exec`. |
| `sandbox.heartbeat_interval_secs` | Disables periodic `tick_heartbeat_if_due` (`HEARTBEAT.md`) execution. |
| `sandbox.max_exec_output_bytes` | **Fallback, not unlimited:** `sandbox_exec` falls back to `DEFAULT_MAX_EXEC_OUTPUT_BYTES` (`262144` bytes / 256 KiB). |
| `sandbox.max_brain_context_chars` | **Disables injection:** `build_brain_prompt_section` returns an empty prompt section when `0` (even though the HTML label reads `0 = unlimited`). |
| `database.retention_days` | Disables age-based pruning (records are never expired by age). |
| `database.max_rows_per_table` | Disables row-count capping during maintenance pruning. |
| HTTP query `?limit=0` (`clamp_limit`) | Expands list endpoints (conversations, messages, tasks, logs, memories, audit logs) up to `10000` records (`None` uses route default `50`, `100`, or `500`). |

> **Note on `0` exceptions:** Unlike `inference.timeout_secs = 0` (which genuinely disables per-request deadlines via `effective_request_timeout(0) -> None`), `sandbox.max_exec_output_bytes = 0` falls back to `262144` bytes (256 KiB) and `sandbox.max_brain_context_chars = 0` disables workspace brain prompt injection.

---

## 7. Automatic PS5LM Settings Sync (`/health`, `/v1/models`, `/props`, `/slots`) & KV Cache Prefill

`Ps5LmProvider::discover_capabilities()` (`crates/ps5-agent-inference/src/provider.rs`) probes four PS5LM endpoints concurrently using `tokio::join!`:

1. **`GET {root_url}/health`** — Extracts `loaded_model` (or `model`) and detects `200 OK` vs `503 Service Unavailable`.
2. **`GET {v1_url}/models`** — Extracts `available_models` (`data[].id` or `models[]`) and fallback `context_length` (`meta.n_ctx_train`, `context_length`, `max_model_len`).
3. **`GET {root_url}/props`** — Extracts `default_generation_settings.n_ctx` (or `n_ctx`), `total_slots`, `model_alias` / `model_path` basename, `modalities.vision` / `modalities.mmproj`, `chat_template_caps.supports_thinking`, and `<think>` / `enable_thinking` markers in `chat_template` / `chat_template_tool_use`.
4. **`GET {root_url}/slots`** — Extracts `total_slots` from slot array length and fallback `n_ctx` from `slots[0].n_ctx`.

Additional heuristic hints (`infer_model_hints`) detect vision (`vl`, `vision`, `llava`) and thinking (`qwen3`, `deepseek`, `r1`, `thinking`) from model names. The aggregated result populates `Ps5LmCapabilities`:

```text
Ps5LmCapabilities {
  base_url, healthy, loaded_model, available_models,
  context_length, total_slots, supports_vision, supports_thinking, synced_at
}
```

### How Sync Updates Runtime Settings (`AppHttpBackend::sync_ps5lm_capabilities_with_notify`)

When `caps.healthy == true` and `inference.auto_sync_from_ps5lm == true`:
- `inference.model` is updated to `caps.loaded_model` (if non-empty).
- `inference.context_limit` is updated to `caps.context_length` (if `> 0`).
- `agents.max_concurrent_inference` is updated to `caps.total_slots` **only if** the current setting is `0`, the default `4`, or matches the previously auto-synced slot count (`auto_synced_slots`), preserving explicit manual overrides (like `1`).
- Updates `Ps5LmProvider` (`apply_discovered_context`, `set_max_concurrent`), updates `AgentRuntime` (`update_config`), and broadcasts `StreamEvent::Ps5LmSynced` to live dashboard clients.

**Triggers & Endpoints:**
- **Startup (Host) / Deferred Background Probe (PS5):** Runs automatically on host startup when `auto_sync_from_ps5lm = true` (or immediately on the background poller on PS5 `sie` so HTTP/`:8080` binds without waiting on `PS5LM`).
- **Background loop:** Fires every `inference.auto_sync_interval_secs` (default `30s`; `0` = disabled) inside the `runtime.health_poll_interval_secs` (`15s`) poller.
- **On-demand API / UI:** `GET /api/v1/inference/capabilities` (cached or live probe) and `POST /api/v1/inference/sync` (**Sync Settings from PS5LM Now** button in Settings).

### Importing KV Cache & Sampling Settings (`POST /api/v1/inference/import-kv`)

Alongside capability discovery, **Settings -> KV Cache & llama.cpp / PS5LM Sampling** provides a one-click **Import KV & Sampling from llama.cpp / PS5LM** button (`#btn-import-llamacpp-kv`) with live feedback in `#import-kv-status`:

1. **Auth & CSRF:** `POST /api/v1/inference/import-kv` is mounted under `standard_api_routes` (`crates/ps5-agent-http/src/lib.rs`) and requires standard `auth_csrf_middleware` authentication + `X-CSRF-Token` (on cookie sessions).
2. **PS5LM / `llama.cpp` Probe (`Ps5LmProvider::import_kv_and_sampling`):** Concurrently probes `GET {root_url}/props`, `GET {root_url}/slots`, and `GET {root_url}/health` via `tokio::join!` (returning `InferenceUnavailable` if all three fail). `parse_kv_and_sampling_from_probes` extracts `model`, `n_ctx`, `total_slots`, `cache_type_k` (`cache_type_k` / `type_k` / `kv_cache_type_k`), `cache_type_v` (`cache_type_v` / `type_v` / `kv_cache_type_v`), `flash_attn` (`flash_attn` / `flash_attention`), `n_keep`, `temperature`, `top_k`, `top_p`, `min_p`, `repeat_penalty`, `repeat_last_n`, `presence_penalty`, `frequency_penalty`, `dry_multiplier`, `dry_base`, `dry_allowed_length`, `dry_penalty_last_n`, `xtc_probability`, `xtc_threshold`, and `samplers`. (Model list & capabilities are discovered via `/v1/models` alongside `/props`/`/slots`/`/health` in `discover_capabilities`.)
3. **Validation, Hot-Reload & Persistence (`AppHttpBackend::import_ps5lm_kv_settings`):** Allowlists `cache_type_k` / `cache_type_v` (`f32`, `f16`, `bf16`, `q8_0`, `q4_0`, `q4_1`, `iq4_nl`, `q5_0`, `q5_1`), syncs `total_slots` into `agents.max_concurrent_inference` when eligible (`try_sync_slots_concurrency`), normalizes and validates `AppConfig`, hot-reloads `InferenceProvider` and `AgentRuntime`, saves `config.toml`, and persists `inference.<key>` values in storage.
4. **Sanitized Response & Audit (`routes::import_inference_kv`):** `strip_sensitive_kv_fields` recursively removes any `api_key`, `apikey`, `ps5lm_api_key`, `authorization`, `*_api_key`, `*_secret`, or `*_password` keys from the returned JSON before responding `{ ok, imported, applied, endpoint, total_slots, source }`, and logs an `inference.import_kv` audit entry. `#import-kv-status` renders a summary pill (`K:<k> · V:<v> · FA:<on|off> · ctx <n_ctx> · <n> slots`) and updates the Settings form fields live (`applyImportedKvFields`).

### Post-Response KV Cache Prefill (`prefill_kv_cache`) & Server Prefix Reuse (`cache_prompt`)

- **`cache_prompt = true` (default):** Included on every `chat_completions`, `responses`, and `completions` call (`apply_llamacpp_sampling_fields`) so `PS5LM` / `llama.cpp` retains and reuses matching KV-cache tokens for shared system prompts and prior conversation turns.
- **`prefill_kv_cache = true` (default) & FNV-1a Slot Affinity (`id_slot = -1`):** After a reply is saved (`spawn_kv_cache_prefill` in `crates/ps5-agent-runtime/src/runtime.rs`), `ps5-agent` spawns a best-effort background task that non-blockingly acquires an inference permit (`try_acquire_owned`)—skipping silently if PS5LM slots are busy—and calls `prefill_kv_cache` (`stream: false`, `cache_prompt: true`, `max_tokens: 0`, `n_predict: 0`, `ThinkingLevel::Off`, 30s timeout) with the updated conversation history. When `id_slot = -1` (default) and PS5LM reports `total_slots > 1`, `resolve_conversation_slot_id` computes `fnv1a_64(conversation_id) % total_slots` so both live turns and background KV prefill hit the exact same PS5LM slot without manual pinning; setting `id_slot >= 0` pins every request to that slot explicitly.

---

## 8. Extensions, Custom Tools, Uploads & Memory Import/Export

### Upload & Body Size Boundaries (`crates/ps5-agent-http/src/lib.rs`)

- **`standard_api_routes` & `public_routes`:** Bounded by `server.max_body_bytes` (default `1 MiB`).
- **`upload_api_routes`:** Bounded by `max(server.upload_max_body_bytes, server.max_body_bytes)` (default `64 MiB`, or `256 MiB` if either is `0`). Covers:
  - `POST /api/v1/conversations/{id}/messages` (chat prompts + inline base64 images / workspace attachments)
  - `POST /api/v1/files/upload` (Files tab uploads)
  - `GET` / `POST /api/v1/tools` (listing and creating/importing large custom tool JSON packs)
  - `POST /api/v1/memories/import` (bulk memory JSON/JSONL/Markdown import)
  - `POST /api/v1/extensions/skills/import`
  - `POST /api/v1/extensions/plugins/import`
  - `POST /api/v1/extensions/mcp`
- **Workspace File Ops (`adapters.rs`):** `read_workspace_file` and `write_workspace_file` use `max(tools.max_file_bytes, server.upload_max_body_bytes)` (or unlimited if `upload_max_body_bytes == 0`), constrained strictly within `tools.allowed_paths[0]` (path-traversal protected).
- **HTTP List Query Pagination (`clamp_limit`):** List endpoints default to `50`, `100`, or `500` items when `limit` is omitted; `limit=0` returns up to `10000` (`1..=10000`).

### Extension & Custom Tool Storage, Auto-Creation & Matching

On startup (`src/startup.rs:ensure_directories`) and manager initialization/rescan, `<data_dir>/skills`, `<data_dir>/plugins`, `<data_dir>/mcp`, `<data_dir>/tools`, and `<data_dir>/backups` are created automatically if absent.

| Extension Type | Storage Locations (`data_dir` & Workspace) | Key Config / Format | Runtime & Security Controls |
|---|---|---|---|
| **Skills** (`SkillManager`) | `<data_dir>/skills/<slug>/SKILL.md` and `<workspace>/.agents/skills/<slug>/SKILL.md`; disabled state in `<data_dir>/skills_state.json` | YAML frontmatter (`name`, `description`) + Markdown instructions + optional `scripts/`, `references/`, `examples/`, `resources/` (`MAX_SKILL_MARKDOWN_BYTES = 512 KiB`) | Enabled skill summaries inject into agent system prompts; agents load full instructions on demand via built-in `load_skill` tool. |
| **MCP Servers** (`McpManager`) | Auto-created `<data_dir>/mcp/` (`*.json` and `*/mcp.json`) + `<workspace>/.agents/mcp/*.json`; legacy `<data_dir>/mcp_servers.json` auto-migrated | Single object, `[...]`, `{"mcpServers": {...}}` / `{"mcp_servers": {...}}`, or `{"servers": ...}` (`McpServerConfig`: `url`/`serverUrl`, `headers`, `api_key`/`bearerToken`, `enabled`, `trust_local`, `timeout_secs` default `20`) | Precedence (lowest->highest): `<workspace>/.agents/mcp/*.json` -> legacy `mcp_servers.json` -> `<data_dir>/mcp/*/mcp.json` -> `<data_dir>/mcp/*.json`. Legacy servers auto-migrate into `<data_dir>/mcp/<id>.json`; add/import/toggle/remove keep per-server files and `mcp_servers.json` in sync. Honors `tools.allowed_domains`, `max_web_bytes`, and `block_private_lan_web_requests` (`trust_local = true` bypass). Registers tools as `mcp:<server_id>:<tool_name>`. |
| **Plugins** (`PluginManager`) | Auto-created `<data_dir>/plugins/`; scans `<data_dir>/plugins/<slug>/` and `<workspace>/.agents/plugins/<slug>/` (checking `plugin.json` or `.claude-plugin/plugin.json` inside `<slug>/`, plus sibling `skills/`, `rules/`); state in `<data_dir>/plugins_state.json` | `PluginManifest`: `name`, `displayName`, `description`, `version`, `disabled`, `rules[]`, `mcpServers`, `tools[]` (`PluginCustomToolSpec`) | Enabled `rules[]` inject into agent system prompts; `mcpServers` auto-import into `McpManager` (`sync_plugin_mcp_configs`); plugin tools register as `plugin:<id>:<tool_name>` with `tools.max_web_bytes` and `allowed_domains` enforcement. |
| **Custom Tools** (`CustomToolManager`) | Auto-created `<data_dir>/tools/*.json` (`"data"`, overrides workspace) and `<workspace>/.agents/tools/*.json` (`"workspace"`); policy/API writes persist to `<data_dir>/tools/<sanitized_id>.json` | JSON `CustomToolDefinition` (`id`, `name`, `category` default `"custom"`, `kind`: `"template"` or `"http"`, `url`, `method`, `headers`, `body_template`, `template`, `input_schema`/`output_schema`, `enabled`, `requires_approval`, `is_mutating`, `allow_private_lan`, `timeout_secs` default `20`, `max_http_response_bytes`); imports single object, `[...]`, `{"tools": [...]}`, or OpenAI `{"type":"function","function":{...}}` | **No tool-count cap:** all `41` built-in tools plus arbitrary custom tools (`2 MiB` file cap, `id` `1..=96` chars) load cleanly; built-in IDs cannot be overwritten or deleted. `"http"` tools render `{{placeholders}}` and enforce `tools.allowed_domains`, `tools.blocked_domains`, SSRF protection (unless `allow_private_lan: true`), max `5` redirects, and `tools.max_web_bytes`. Managed via UI **Tools** (`+ Add Tool`, `Import Tools`, `Enable`/`Approval`, `Delete`) and HTTP (`/api/v1/tools`, `PATCH/DELETE /api/v1/tools/{id}`); matched via `"custom:*"` (enabled on `commander` & `programmer`), `"*"`, or exact ID. |

Agents authorize built-in, extension, and custom tools via `AgentDefinition.enabled_tools` patterns (`"*"`, `"mcp:*"`, `"plugin:*"`, `"custom:*"`, `group:*` groups from `expand_tool_group`, prefix wildcards, or exact IDs/aliases), while `AgentDefinition.denied_tools` overrides `enabled_tools` under `agent_allows_tool_with_policy` (deny-wins).

### Memory Import & Export

- **`POST /api/v1/memories/import` (Dashboard: `Memory -> Import JSON` / `#import-memories-btn`):** Mounted under `upload_api_routes` (`64 MiB` default upload limit). Accepts JSON arrays (`[...]`), `{"memories": [...]}` or `{"items": [...]}`, single `MemoryItem` objects, JSONL lines, or Markdown/text notes (`#`/`##` headings, blank-line paragraphs, or bullet lists under `raw`/`markdown`/`notes`/`text`/`content`) and upserts parsed records into long-term memory storage.
- **`GET /api/v1/memories/export` (Dashboard: `Memory -> Export JSON` / `#export-memories-btn`):** Exports up to `10000` memories as a JSON array (saved by the UI as `ps5-agent-memories.json`).

---

## 9. Retention & Pruning

Long-running daemon memory and disk growth are bounded across three layers:

1. **Database Retention & Row Caps (`database.retention_days` & `database.max_rows_per_table`):**
   - Executed by `StorageBackend::prune_old_records(retention_days, max_rows_per_table)` at **startup** (`src/startup.rs`) and **every 600 seconds** (10 minutes) in the background maintenance loop (alongside `purge_expired_sessions` and `checkpoint`).
   - **SQLite (`crates/ps5-agent-database/src/sqlite.rs`):** Runs inside a single transaction across 8 steps, then calls `PRAGMA incremental_vacuum(64)`:
     1. `sessions`: removes expired or revoked sessions + trims to `max_rows_per_table`.
     2. `approvals`: removes resolved (`status != 'PENDING'`) approvals past `retention_days` / `max_rows_per_table` (never `PENDING`).
     3. `tasks`: removes terminal tasks (`COMPLETED`, `FAILED`, `CANCELLED`) and their linked `task_events` and `agent_messages`.
     4. `task_events`: removes old/excess rows (never events belonging to active `PENDING`/`ASSIGNED`/`RUNNING`/`WAITING` tasks).
     5. `agent_messages`: removes old/excess A2A envelopes.
     6. `tool_executions`: removes old/excess tool audit rows.
     7. `audit_logs`: removes old/excess security audit rows.
     8. `messages`: removes `archived = 1` chat messages past `retention_days` / `max_rows_per_table` (**never deletes unarchived active chat messages**).
   - **JSONL (`crates/ps5-agent-database/src/jsonl.rs`):** Compacts in-memory maps and appends/rewrites logs using the same retention and row-cap rules, shrinking sparse maps (`shrink_if_sparse`).
2. **Chat Compaction & Agent Memory Caps (`[memory]`):**
   - `memory.archive_after_messages` (default `40`) + `memory.keep_recent_messages` (`16`) mark older conversation messages `archived = 1` and distill rolling summaries so prompt size stays bounded and old messages become eligible for DB pruning.
   - `memory.max_memories_per_agent` (default `0` in both `AppConfig` and PS5 preset) archives oldest/lowest-importance `MemoryScope::Agent` rows when exceeded.
3. **In-Memory Leak Guards (automatic, non-configurable):**
   - `TaskManager::tokens`: releases cancellation tokens on task completion/waiting and shrinks sparse maps.
   - `AgentRuntime::pending_approvals`: capped at `MAX_PENDING_APPROVALS = 256` (evicting oldest entries) and cleaned up on task completion, failure, cancellation, timeout, or agent deletion/temp cleanup.
   - `SlidingWindowRateLimiter`: caps at `MAX_TRACKED_IPS = 4096` and sweeps expired 60s buckets every `64` checks (`16` when `len > 64`).

---

## 10. Hot-Reload vs. Restart Matrix (`PUT /api/v1/settings`, `PUT /api/v1/sandbox` & `POST /api/v1/inference/import-kv`)

`PUT /api/v1/settings` validates inputs (`extract_kv_updates` accepts flat/nested keys including `"sandbox"` objects, up to `256` entries per update, `<= 128`-char keys, and `<= 8192`-byte values), rejects sensitive credential keys (`auth.*`, `*password*`, `*secret*`, `*token*`, `*api_key*`, `*private_key*`—explicitly exempting `timings_per_token`, `*.timings_per_token`, `tokens`, and `*_tokens`) with `403 Forbidden`, runs `apply_settings_update` (`src/adapters.rs`) and `sync_sandbox_and_tools()`, writes `config.toml` atomically, and records an `audit_logs` entry (`settings.update`). Similarly, `POST /api/v1/inference/import-kv` hot-reloads imported PS5LM KV/sampling settings into `InferenceProvider` and `AgentRuntime`, saves `config.toml`, strips any sensitive keys via `strip_sensitive_kv_fields`, and records an `inference.import_kv` audit entry. Because `sandbox.brave_api_key` and `sandbox.tavily_api_key` match `*api_key*`, update them via `PUT /api/v1/sandbox` (or `PS5_AGENT_*` env vars / `config.toml`) rather than `PUT /api/v1/settings`.

| Setting Group | Applies Immediately (Hot-Swapped) | Requires Daemon Restart |
|---|---|---|
| **`[inference]`** | `base_url`, `model`, `api_mode`, `thinking_level`, `auto_sync_from_ps5lm`, `auto_sync_interval_secs`, `prefill_kv_cache`, `cache_prompt`, `cache_type_k`, `cache_type_v`, `flash_attn`, `smart_tool_routing`, `temperature`, `dynatemp_range`, `dynatemp_exponent`, `top_k`, `top_p`, `min_p`, `typical_p`, `xtc_probability`, `xtc_threshold`, `top_n_sigma`, `repeat_penalty`, `repeat_last_n`, `presence_penalty`, `frequency_penalty`, `dry_multiplier`, `dry_base`, `dry_allowed_length`, `dry_penalty_last_n`, `mirostat`, `mirostat_tau`, `mirostat_eta`, `seed`, `ignore_eos`, `stop_sequences`, `n_keep`, `id_slot`, `samplers`, `grammar`, `reasoning_format`, `timings_per_token`, `custom_extra_body_json`, `stream_enabled`, `timeout_secs`, `connect_timeout_secs`, `max_retries`, `context_limit`, `max_output_tokens`, plus `POST /api/v1/inference/import-kv` | `api_key` *(edit `config.toml`)* |
| **`[server]`** | `allow_lan_only`, plus workspace upload/download file helper cap for `upload_max_body_bytes` | `bind_host`, `port`, `max_body_bytes` (router layer), `upload_max_body_bytes` (Axum `RequestBodyLimitLayer`) |
| **`[database]`** | `retention_days`, `max_rows_per_table` | `driver`, `path`, `busy_timeout_ms`, `cache_size_kb`, `wal_mode` |
| **`[security]`** | `auth_enabled`, `admin_username`, `require_approval_for_mutating`, `session_ttl_hours`, `rate_limit_per_minute`, `login_rate_limit_per_minute` *(use `POST /api/v1/auth/password` for live password rotation)* | `admin_password`, `password_hash`, `api_token_sha256`, `argon2_memory_kib` *(edit `config.toml`)* |
| **`[agents]`** | `max_agents`, `max_active_tasks`, `max_queued_tasks`, `max_concurrent_inference`, `max_messages_per_task`, `max_reasoning_iterations`, `default_task_timeout_secs` *(stored)*, `allow_temporary_specialists`, `on_disable_policy` | *(None)* |
| **`[memory]`** | `max_memories_per_agent`, `archive_after_messages`, `keep_recent_messages`, `max_context_memories`, `max_memory_context_chars` | *(None)* |
| **`[sandbox]`** | `mode`, `approval_policy`, `network_access`, `allow_local_network`, `web_search_provider`, `searxng_url`, `remote_browser_url`, `allow_host_shell`, `max_exec_output_bytes`, `exec_timeout_secs`, `inject_brain_files`, `max_brain_context_chars`, `heartbeat_interval_secs` *(via `/settings` or `/sandbox`)*; `brave_api_key`, `tavily_api_key` *(via `PUT /api/v1/sandbox`)* | *(None)* |
| **`[tools]`** | `allowed_paths`, `allowed_domains`, `blocked_domains`, `block_private_lan_web_requests`, `default_timeout_secs`, `max_file_bytes`, `max_web_bytes`, plus `[sandbox]`-mirrored keys via `sync_sandbox_and_tools()` | *(None)* |
| **`[logging]`** | `level`, `max_file_bytes`, `ring_buffer_capacity`, `enable_klog`, `enable_toast` *(live via `logger.apply_config`; startup alerts only trigger at boot)* | `log_dir` |
| **`[runtime]`** | `rest_mode_recovery` *(persisted)* | `tokio_worker_threads`, `health_poll_interval_secs` |
| **Per-Agent (`agents/{id}` & `/agents/enable-all`)** | All `AgentDefinition` & `AgentModelConfig` fields (`denied_tools`, `sandbox_mode`, `tool_profile`, etc.), plus bulk `Boot / Enable All Agents` | *(None)* |
| **Extensions, Custom Tools & Memories** | `Skills`, `Plugins`, `MCP Servers` (`<data_dir>/mcp/`), `Custom Tools` (`<data_dir>/tools/`), and `/api/v1/memories/import` | *(None)* |
