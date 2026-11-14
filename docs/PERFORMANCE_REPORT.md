# PS5 Agent Performance, Tuning, Resource Budget & Verification Report

This report documents the configured resource budgets and tuning knobs for PlayStation 5 Slim (Firmware `13.40`, `0x13400000`), KV-cache prefill (`prefill_kv_cache`, `cache_prompt`, `id_slot`), the Wave 6 throughput optimization path (`90 -> 115–120 tok/s` design target vs. raw `llama.cpp` CLI) and `llama.cpp` / `PS5LM` sampler tuning, bounded vs. `0 = unlimited` RSS/queue trade-offs, RAM/CPU/storage leak mitigations, release build optimization settings, verified Wave 6 PS5 ELF and workspace test measurements, and the explicit boundary between **Host & PS5-ELF Verified** checks and **Physical PS5 Hardware-Pending** validation.

> **Verification Honesty Notice:** No physical PS5 console benchmarks (on-console RSS, `/data` SQLite write throughput, live PS5LM tokens/sec validating the `90 -> 115–120 tok/s` optimization path, cold-boot `pldmgr` timings, or Rest-Mode wake latency) were measured in this environment. Those items are explicitly marked **Physical PS5 Hardware-Pending** below.

---

## 1. PS5 Slim (FW `13.40`) Resource Budget & Unbounded vs. Bounded Trade-Offs

Under `ps5-payload-dev/elfldr`, `ps5-agent.elf` executes as a userland payload with `sys_budget_set(0)` (0 Direct Memory, ~8 GiB shared Flexible Memory) alongside `PS5LM` (up to 11.4 GiB on Vulkan GPU as native app `PPSA99575`, or `<= 1.6 GiB` in legacy payload mode) on a shared 16 GB GDDR6 pool.

### Current Config Defaults (`config/config.ps5-slim-1340.toml` & `AppConfig::with_data_dir`)

| Parameter | Shipped PS5 Default | Code / PS5 Rationale |
|---|---|---|
| `runtime.tokio_worker_threads` | `2` (clamped `1..=8`) | `available_parallelism()` reports 16 logical CPUs, but payloads are restricted to 5 CPUs (`0xea00`); capping Tokio at `2` threads (`<= 3`) prevents starving Sony OS threads. |
| `agents.max_concurrent_inference` | `4` (`0` = `4096` permits) | Matches `PS5LM`'s default `total_slots` when `auto_sync_from_ps5lm = true` (auto-updates only if current value is `0`, `4`, or previously auto-synced; explicit overrides like `1` are preserved). |
| `agents.max_agents` / `max_active_tasks` / `max_queued_tasks` | `0` / `0` / `0` (`0` = unlimited) | Removes artificial 5/4/64 caps out of the box (`max_active_tasks = 0` maps `task_semaphore` to `UNLIMITED_SEMAPHORE_PERMITS = 4096`). |
| `agents.max_messages_per_task` / `max_reasoning_iterations` | `0` / `0` (`0` = unlimited) | Removes truncation on long multi-step tasks (`max_reasoning_iterations = 0` enforces an internal safety ceiling `UNLIMITED_REASONING_SAFETY_CAP = 256`). |
| `agents.default_task_timeout_secs` | `0` (`0` = unlimited) | Prevents long-running builds/analyses from timing out unless bounded per agent or globally. |
| `memory.max_memories_per_agent` | `0` (`0` = unlimited) | Allows unbounded long-term memory accumulation unless capped explicitly. |
| `server.max_body_bytes` / `upload_max_body_bytes` | `1048576` (`1 MiB`) / `67108864` (`64 MiB`) | Caps standard JSON/API payloads at `1 MiB` while giving multipart/attachment uploads and extension imports a separate `64 MiB` tier (`0` = unlimited; clamped `1024..=256 MiB` when `> 0`). |
| `security.argon2_memory_kib` | `8192` (`8 MiB`) on PS5 (`19456` host) | Bounds peak Argon2id password-hashing allocation during login/password rotation (`rate_limit_per_minute = 240`, `login_rate_limit_per_minute = 10`; `0` bypasses rate limiting). |
| `database.cache_size_kb` | `4000` (`PRAGMA cache_size = -4000`, ~`4 MiB`) | Bounds SQLite page cache in Flexible Memory (`busy_timeout_ms = 5000`, `PRAGMA locking_mode = NORMAL`, `PRAGMA synchronous = NORMAL`, `PRAGMA auto_vacuum = INCREMENTAL`). On PS5 (`target_vendor = "sie"` and `wal_mode = false`), SQLite uses `PRAGMA journal_mode = TRUNCATE` (vs. host `WAL`) to avoid `.db-shm` `mmap(MAP_SHARED)` issues. |
| `database.retention_days` / `max_rows_per_table` | `30` / `10000` (`0` = unlimited) | Prunes old terminal tasks, audit logs, and tool executions at startup and every `600s` (`prune_old_records`). |
| `logging.max_file_bytes` / `ring_buffer_capacity` | `2097152` (`2 MiB`) / `500` entries | Bounds rotating disk logs in `/data/ps5-agent/logs` and in-memory log ring buffer. |
| `tools.max_file_bytes` / `max_web_bytes` | `2097152` (`2 MiB`) / `524288` (`512 KiB`) | Prevents large file reads or web fetches from spiking payload RSS (`0` falls back to `DEFAULT_MAX_FILE_BYTES = 16 MiB` / `DEFAULT_MAX_WEB_BYTES = 16 MiB` in tools). |
| `sandbox.max_exec_output_bytes` / `exec_timeout_secs` | `262144` (`256 KiB`) / `20`s (`SandboxConfig::default()`) | Bounds per-stream (`stdout`/`stderr`) virtual/host shell capture (`MIN_EXEC_OUTPUT_BYTES = 1024` B floor when `> 0`) and command runtime (`0` output cap falls back to `256 KiB`; `0` timeout disables timer). |
| `sandbox.inject_brain_files` / `max_brain_context_chars` | `true` / `4000` chars (`SandboxConfig::default()`) | Bounds system-prompt Workspace Brain injection across root Brain files and `.agents/rules/*.md` (`MAX_PER_FILE_PROMPT_CHARS = 4096`, `MAX_BRAIN_FILE_BYTES = 512 KiB`; `0` disables injection). |
| `memory.archive_after_messages` / `keep_recent_messages` | `40` / `16` | Triggers rolling-summary compaction (`rolling-summary-{conv_id}`) so prompt context stays bounded (`max_context_memories = 8`, `max_memory_context_chars = 3200`, `IMAGE_ATTACHMENT_TOKEN_ESTIMATE = 85`). |

### Unbounded vs. Bounded RSS / Queue Trade-Offs

| Profile / Workload | Recommended Settings | RSS, Queue & Throughput Trade-Off |
|---|---|---|
| **Unbounded Throughput (Default)** | `max_agents = 0`, `max_active_tasks = 0` (`4096` permits), `max_queued_tasks = 0`, `max_concurrent_inference = 4` (auto-synced to `total_slots`), `max_reasoning_iterations = 0` (`256` cap) | Maximizes responsiveness across concurrent chat rooms, group fan-out (`JoinSet`), and tool pipelines without premature task rejections. Requires `PS5LM` to have enough VRAM/Flexible Memory for `total_slots` concurrent KV contexts. Higher concurrent task counts increase `ps5-agent` Tokio heap usage (bounded by `TaskManager` token eviction and `MAX_PENDING_APPROVALS = 256`). |
| **Strict Low-RSS PS5 (`< 64 MiB` target)** | `max_concurrent_inference = 1`, `max_active_tasks = 4`, `max_agents = 5`, `max_queued_tasks = 64`, `retention_days = 30`, `max_rows_per_table = 10000`, `cache_size_kb = 4000`, `tokio_worker_threads = 2` | Serializes `PS5LM` inference to 1 active GPU request (best when running larger GGUF quantizations or tight 16 GB GDDR6 budgets), caps concurrent in-flight task state in RAM, and keeps SQLite page cache (~4 MiB) + `/data` file growth strictly bounded. |
| **Interactive Queue Bypass (`immediate: true` / Expedite)** | `SendMessageRequest.immediate = true` (**Send Now**) or `POST /api/v1/tasks/{id}/expedite` (**Run Immediately**) | Bypasses `task_semaphore` waiting in `await_task_slot_or_expedite` (`crates/ps5-agent-runtime/src/runtime.rs`) without increasing `max_active_tasks`. Note: actual model HTTP calls still honor `Ps5LmProvider::semaphore` (`max_concurrent_inference`), and only root tasks acquire `task_semaphore` so child delegations never self-deadlock. |
| **HTTP List Queries (`limit=0`)** | `clamp_limit(Some(0), default)` (`crates/ps5-agent-http/src/routes.rs`) | Query parameter `?limit=0` expands up to `10_000` rows instead of returning unbounded result sets, protecting both PS5 RSS and browser JSON parsing. |

### Release Build Optimization Profile (`Cargo.toml` & `scripts/build-ps5.ps1`)

| Setting | Value | Effect |
|---|---|---|
| `[profile.release] opt-level` | `"z"` | Optimizes for minimal binary size (`.text` footprint in Flexible Memory). |
| `[profile.release] lto` | `"fat"` | Enables full cross-crate link-time dead-code elimination and inlining. |
| `[profile.release] codegen-units` | `1` | Maximizes whole-crate optimization at the cost of longer release link times. |
| `[profile.release] panic` | `"abort"` | Matches `x86_64-sie-ps5-std.json` (`panic-strategy: abort`, `-Z build-std=std,panic_abort`) and omits unwinding tables. |
| `[profile.release] strip` | `false` | Keeps symbols in the raw binary for `--unstripped` `elf-verify.exe` inspection; `scripts/build-ps5.ps1` runs `prospero-strip.exe` to emit stripped `build/*.elf` artifacts and `build/meta.json`. |

---

## 2. PS5LM KV Cache, Throughput Optimization Path & `llama.cpp` Sampling Tuning

KV-cache, tool-routing, and `llama.cpp` sampling fields under `[inference]` hot-reload on `PUT /api/v1/settings` (`Ps5LmProvider::update_config`) without restarting `ps5-agent`, or can be imported directly from a live `llama.cpp` / `PS5LM` server (`/props`, `/slots`, `/v1/models`) via `POST /api/v1/inference/import-kv` (Settings UI `#btn-import-llamacpp-kv`). Except for `prefill_kv_cache` (runtime orchestration), `smart_tool_routing` (turn tool-schema filtering), and metadata fields (`cache_type_k`, `cache_type_v`, `flash_attn`), `apply_llamacpp_sampling_fields` (`crates/ps5-agent-inference/src/provider.rs`) injects these parameters on `chat_completions`, `responses`, and `completions` payloads (`anthropic_messages` and `system_one` do not carry `llama.cpp`-specific sampling keys).

### KV-Cache & Tool-Routing Controls (`[inference]`)

| Parameter | Default | Mechanism & Tuning Trade-Off |
|---|---|---|
| `inference.prefill_kv_cache` | `true` | After an assistant reply completes (`spawn_kv_cache_prefill` in `crates/ps5-agent-runtime/src/runtime.rs`), spawns a background task that tries `inf_sem.try_acquire_owned()`, builds the conversation messages (`build_prefill_messages`), attaches the exact `allowed_tool_schemas(&agent)` list and deterministic `slot_id`, and delegates to `prefill_kv_cache_request` (`crates/ps5-agent-inference/src/provider.rs`). That method skips if `self.gate.try_acquire()` is busy, builds the matching chat/completions payload, overrides `stream: false`, `cache_prompt: true`, `max_tokens: 0`, `n_predict: 0` (removing `stream_options`), and sends a `30s`-timeout probe. **Trade-off:** Trades a short background prompt-eval burst immediately after a reply for warm KV reuse on the user's next turn, with zero extra `ps5-agent` RSS. Disable (`false`) if PS5LM runs under tight thermal/power headroom. |
| `inference.cache_prompt` | `true` | Always emitted on `chat_completions`, `responses`, and `completions`. Instructs `llama.cpp` / `PS5LM` to retain and match KV-cache prefixes across turns instead of re-evaluating the system prompt and history from token 0. |
| `inference.id_slot` | `-1` | When `< 0` (auto mode), `resolve_conversation_slot_id` (`crates/ps5-agent-runtime/src/runtime.rs`) deterministically maps non-empty `conversation_id`s onto slots via `fnv1a_64(conv_id.as_bytes()) % total_slots` whenever `total_slots > 1`, keeping each conversation pinned to a consistent KV slot instead of colliding on random/shared slots. Only omits `id_slot` when `total_slots <= 1` or no `conversation_id` is set. Setting `id_slot >= 0` explicitly pins all traffic to that single slot. |
| `inference.smart_tool_routing` | `true` | On initial interactive turns (`iter == 0`, `!opts.has_attachments`, no `"tool"` history, `allowed_tools.len() > 12`), calls `select_tools_for_casual_turn` (`crates/ps5-agent-runtime/src/runtime.rs`): pure greetings/pings (`is_casual_greeting_or_ping`) strip all tool schemas (`Vec::new()`), while short (`<= 140` chars, `<= 20` words, single-line) prompts without dotted filenames/URLs or `NON_CASUAL_TOOL_CUE_WORDS` slim the list to extension/conversational tools only (`mcp:*`, `plugin:*`, `custom:*`, `load_skill`, `memory_read`, `memory_search`, `datetime`). Eliminates unnecessary prompt tokens on conversational turns while preserving extensions and multi-step tool calls on coding/search/file turns. |
| `inference.cache_type_k` / `cache_type_v` / `flash_attn` | `""` / `""` / `false` | KV-cache quantization and FlashAttention settings imported from `/props` & `/slots` (`POST /api/v1/inference/import-kv`). Quant types are normalized lowercase and validated against the GGML allowlist (`""`, `f32`, `f16`, `bf16`, `q8_0`, `q4_0`, `q4_1`, `iq4_nl`, `q5_0`, `q5_1`) in `crates/ps5-agent-config/src/lib.rs`. |

### Throughput & KV-Cache Optimization Path (`90 tok/s -> 115–120 tok/s` Design Target)

To close the prompt-eval and per-turn overhead gap between `ps5-agent` and raw `llama.cpp` CLI (target uplift from ~`90 tok/s` toward `115–120 tok/s`; live on-console measurements remain **Physical PS5 Hardware-Pending**), Wave 6 aligns the full request pipeline:

1. **Static KV-Cache Prefix Byte-Identity (`build_system_prompt`, `crates/ps5-agent-runtime/src/runtime.rs`):** Removed per-second wall-clock drift (`Current UTC time:` / `Utc::now()`) from `build_system_prompt`, sorted peer agent descriptions and skill/plugin entries deterministically, and placed dynamic per-turn context (`mem_ctx` and `Language Requirement: <directive>` from `crate::scheduler::language_response_directive`) strictly at the **tail** of the system prompt so the entire static prefix remains byte-identical across turns and hits PS5LM's KV prefix cache.
2. **Aligned Prefill + Deterministic `id_slot` Affinity:** `resolve_conversation_slot_id` (FNV-1a hash mod `total_slots`) and `spawn_kv_cache_prefill` / `prefill_kv_cache_request` both attach identical `tools` schemas and the same `slot_id`, ensuring speculative prefill populates the exact slot and prompt prefix reused by the next user turn.
3. **Tool Schema Caching & Casual-Turn Slimming (`allowed_tool_schemas`, `Runtime::tool_schema_cache`):** Built tool schemas are cached in `tool_schema_cache` keyed by `(agent_id, policy_hash, revision)` (`fnv1a_hash_str`, invalidated cleanly on agent/MCP/tool changes), and `smart_tool_routing` trims >12-tool JSON schemas on greetings and simple chat turns.
4. **TCP `TCP_NODELAY` & Minimal Wire JSON (`ps5-agent-inference`, `ps5-agent-http`):**
   - `build_http_client` (`crates/ps5-agent-inference/src/provider.rs`) enables `.tcp_nodelay(true)` with `.pool_idle_timeout(Duration::from_secs(45))`, and `configure_accepted_stream` (`crates/ps5-agent-http/src/server.rs`) sets `stream.set_nodelay(true)` on accepted Axum sockets so small request frames and SSE token deltas do not wait on Nagle / delayed-ACK timers.
   - `apply_llamacpp_sampling_fields` omits neutral `repeat_penalty` (~`1.0`), `presence_penalty` (~`0.0`), and `frequency_penalty` (~`0.0`) unless finite and `>= 1e-5` from zero (or `1.0`), and `build_chat_completions_payload` emits message `"name"` only when `role == "tool"` (omitting `"name"` on single-agent `user`/`assistant` turns to match clean chat-template formatting and prevent role confusion).
5. **End-to-End `InferenceTimings` Observability:** `parse_timings` and `merge_timings` (`crates/ps5-agent-inference/src/provider.rs`) extract `predicted_per_second`, `prompt_per_second`, and `cache_n` (filtering non-finite/negative values), `emit_chat_completed` forwards `timings` on `StreamEvent::ChatCompleted` (`crates/ps5-agent-core/src/events.rs`), SQLite schema `v6` (`006_message_timings_and_conv_index`) persists `messages.timings_json`, and the dashboard renders `.msg-timings-pill` (`is-kv-hit` when `cache_n > 0`).

### `llama.cpp` Samplers, Repetition Controls & Thinking Wire Format

- **Always emitted (`apply_llamacpp_sampling_fields`):** `cache_prompt` (`true`), `top_k` (`40`), `top_p` (`0.95`), `min_p` (`0.05`), `repeat_last_n` (**Wave-6 default `256`**, raised from `64`).
- **Conditionally emitted (omitted when neutral/inactive — shrinking JSON wire payloads):**
  - **Repetition & Presence/Frequency Penalties:** `repeat_penalty` (`1.05` default; omitted when `|repeat_penalty - 1.0| < 1e-5`), `presence_penalty` (`0.0` default; omitted when `|v| < 1e-5`), `frequency_penalty` (`0.0` default; omitted when `|v| < 1e-5`).
  - **DRY Repetition Penalty & Stream Loop Breaker:** `dry_multiplier > 0.0` (**Wave-6 default `0.8`**, previously `0.0`) emits `dry_multiplier`, `dry_base` (`1.75`), `dry_allowed_length` (`2`), and `dry_penalty_last_n` (`-1` = full context). Paired on the client side with `StreamingRepetitionGuard` (`crates/ps5-agent-inference/src/parser.rs`) inside `SseAccumulator` to abort degenerate verbatim prose loops mid-stream.
  - **Dynamic Temperature:** `dynatemp_range > 0.0` emits `dynatemp_range` + `dynatemp_exponent` (`1.0`).
  - **Typical / XTC / Top-n-sigma:** `typical_p < 1.0` emits `typical_p`; `xtc_probability > 0.0` emits `xtc_probability` + `xtc_threshold` (`0.1`); `top_n_sigma > 0.0` (default `-1.0`) emits `top_n_sigma`.
  - **Mirostat (v1/v2):** `mirostat > 0` (`1` or `2`; default `0` off) emits `mirostat`, `mirostat_tau` (`5.0`), and `mirostat_eta` (`0.1`) for perplexity-targeted sampling.
- **Deterministic / Slot / Diagnostics / Extra Body Controls:**
  - `seed != -1` emits fixed RNG `seed`; `ignore_eos = true` emits `ignore_eos`; `n_keep != 0` emits `n_keep`; non-empty `samplers` (comma/semicolon-separated or JSON array) emits `samplers`; non-empty `grammar` emits GBNF `grammar`; `timings_per_token = true` emits `timings_per_token` (exempt from settings redaction so it can be toggled via `PUT /api/v1/settings` for live token/s profiling); non-empty `stop_sequences` merges without duplicates into `stop`; `POST /api/v1/inference/import-kv` probes `/props`, `/slots`, and `/v1/models` (stripping API keys/sensitive headers via `strip_sensitive_kv_fields`); and `custom_extra_body_json` merges top-level JSON object keys (excluding protected `"messages"`, `"input"`, `"prompt"`, `"stream"`).
- **Thinking Mode (`build_chat_completions_payload`):**
  - `ThinkingLevel::Off` -> `chat_template_kwargs: {"enable_thinking": false}`, `reasoning_budget: 0`, `reasoning_format: "none"`.
  - `Low` -> `enable_thinking: true`, `reasoning_effort: "low"`, `reasoning_budget: 512`.
  - `Medium` -> `enable_thinking: true`, `reasoning_effort: "medium"`, `reasoning_budget: 2048`.
  - `High` -> `enable_thinking: true`, `reasoning_effort: "high"`, `reasoning_budget: 8192`.
  - `Max` -> `enable_thinking: true`, `reasoning_effort: "high"`, `reasoning_budget: -1` (unbounded model budget).
  - When `ThinkingLevel != Off` and `reasoning_format` is non-empty and not `"none"` (default `"deepseek"`), `apply_llamacpp_sampling_fields` emits `reasoning_format`.

---

## 3. RAM, CPU & Storage Leak Mitigations Implemented & Tested

To prevent long-running memory growth, file/database bloat, and idle CPU wakeups on PS5 Flexible Memory and `/data` storage, the following mitigations are implemented:

- **PS5 Signal Polling Reduction (`src/shutdown.rs`):** Raised the PS5 `TERM_SIGNAL_RECEIVED` poll interval from `50ms` to `250ms` (~80% fewer idle wakeups per second).
- **Logging Ring Shrink & Rotate Backoff (`crates/ps5-agent-logging/src/lib.rs`):**
  - `AppLogger::apply_config` evicts entries above clamped `ring_capacity` and invokes `g.ring.shrink_to_fit()` whenever entries are evicted or capacity shrinks.
  - `format_entry_line_nl` allocates a single formatted line buffer (`format!(... "\n")`) written in one `write_all` call.
  - `FileSinkState::write_and_rotate` enforces a `5s` backoff (`REOPEN_RETRY_INTERVAL`) via `last_rotate_failure` and `last_reopen_attempt` (paired with `IO_DIAG_THROTTLE = 30s`) so failed log rotations/opens do not retry I/O syscalls on every log message.
- **HTTP Bounds, Socket NoDelay & SSE Lag Coalescing (`crates/ps5-agent-http`):**
  - `SlidingWindowRateLimiter` capped at `4096` tracked IPs with periodic stale-entry sweep + `shrink_to_fit()` (and bypassed completely when `rate_limit_per_minute = 0`).
  - `SseConnectionGuard` capped at `32` concurrent SSE streams (`Drop` decrements `active_sse_connections`).
  - `LagCoalescer` (`SSE_LAG_ALERT_COOLDOWN = 5s`, `crates/ps5-agent-http/src/routes.rs`) coalesces consecutive broadcast lag events (`pending_skipped`) instead of flooding slow SSE clients, while `configure_accepted_stream` (`server.rs`) enables `stream.set_nodelay(true)`.
- **Runtime & Orchestration Bounds (`crates/ps5-agent-runtime`):**
  - `TaskManager::release_token` removes terminal entries from both `tokens` and `expedite_notifiers` and shrinks sparse maps (`shrink_map_if_sparse`, `active_token_count()` drops back to `0` after 300+ tasks).
  - `AgentRuntime::pending_approvals` bounded at `256` entries (`MAX_PENDING_APPROVALS`) with `evict_pending_for_agent` / `evict_pending_for_task` cleanup on cancellation or disablement.
  - Root-only `task_semaphore` avoids child-task delegation deadlocks, and group orchestration uses parallel `JoinSet` fan-out.
- **Messaging & Zero-Alloc Memory Bounds (`crates/ps5-agent-messaging`, `crates/ps5-agent-memory`):**
  - `MessageBus` uses bounded `mpsc::channel(256)` per subscriber with `unsubscribe_agent()` and automatic closed-receiver eviction.
  - `IMAGE_ATTACHMENT_TOKEN_ESTIMATE = 85` estimates image attachment token cost at a fixed constant instead of counting raw base64 payload characters.
  - `score_memory` (`crates/ps5-agent-memory/src/lib.rs`) uses zero-allocation ASCII case-insensitive scanning (`contains_ascii_ci` + `eq_ignore_ascii_case`) instead of heap `.to_lowercase()` allocations per memory/token, and `build_conversation_context` skips the redundant full-history `list_chat_messages(..., true, ...)` query whenever `archived_count == 0` (reusing `active_before`).
- **SQLite Statement Cache, Memory Shrinking, Busy Retry & Debounced Snapshots (`crates/ps5-agent-database`):**
  - `open_and_configure` sets `conn.set_prepared_statement_cache_capacity(128)` and routes hot queries/writes through `prepare_cached` (`exec_cached`, `query_row_cached`), while schema `v6` (`006_message_timings_and_conv_index`) adds `idx_messages_conv_created` on `(conversation_id, created_at DESC)`.
  - SQLite configures `PRAGMA auto_vacuum = INCREMENTAL` (on fresh DBs); both `checkpoint()` (`PRAGMA wal_checkpoint(TRUNCATE); PRAGMA optimize; PRAGMA shrink_memory;`) and `prune_old_records()` (`PRAGMA incremental_vacuum(64); PRAGMA shrink_memory;`) explicitly reclaim SQLite heap caches.
  - `SqliteStorage::open` retries transient open errors (`DatabaseBusy`, `DatabaseLocked`, `CannotOpen`) up to `MAX_OPEN_ATTEMPTS = 5` (`150ms`) with stale `-shm` cleanup, and `open_storage` in `"auto"` mode retries `is_transient_busy_or_locked_error` (`2` retries × `150ms`) before escalating without false `.corrupt.bak` quarantine.
  - Both SQLite and JSONL implement `prune_old_records(retention_days, max_rows_per_table)` invoked on startup and on a `600s` periodic maintenance timer; `task_counts_summary()` aggregates counts directly in SQL; `JsonlStorage` debounces snapshots at `250ms`.
- **Tools, SkillScanCache, Scheduler & Bounded Dashboard State (`crates/ps5-agent-tools`, `crates/ps5-agent-runtime`, `crates/ps5-agent-dashboard`):**
  - `SkillManager` (`crates/ps5-agent-tools/src/skills.rs`) maintains `scan_cache: RwLock<Option<SkillScanCache>>` keyed by `cache_generation` + `ScanFingerprint` (root/skill/resource directory mtimes, entry counts, `SKILL.md` mtime & byte length) to eliminate repeated recursive disk scans and markdown parses when files are unchanged.
  - HTML extraction regexes are compiled once via `OnceLock`; built-in `ToolSpec` definitions are cached in memory; SQLite/JSONL schema `v4` (`004_schedules`) indexes due schedules via `idx_schedules_enabled_next` (`MAX_SCHEDULES = 1000`, `MAX_CRON_SEARCH_MINUTES = 527_040`, background tick batch `list_due_schedules(..., 64)`, HTTP listing bound `500`).
  - Dashboard client (`crates/ps5-agent-dashboard/assets/app.js`) bounds long-running browser memory via `MAX_CHAT_STREAM_BUCKETS = 32` (`pruneChatStreamBuckets()`), `MAX_LIVE_BROWSER_TABS = 12` (`trimLiveBrowserTabs()`), `MAX_LIVE_BROWSER_FEED = 100`, `MAX_CACHED_TIMINGS = 256` (`setBoundedMapEntry` FIFO/LRU eviction on `chatTimingsByMsgId` and `chatTimingsByTaskId`), `MAX_SVG_HOTSPOTS = 40`, `MAX_SVG_RIPPLES = 4`, `completedChatTaskIds` (`<= 512`), `cleanupConversationClientState(convId)` on conversation removal, and `resetSessionUiState()` on logout/401 transitions.
- **Stateful Browser Tool Bounds (`crates/ps5-agent-tools/src/browser.rs`):**
  - `SandboxBrowserManager` limits concurrent in-memory sessions to `MAX_SESSIONS = 16`, evicts idle sessions after `SESSION_IDLE_TTL = 30m` (`1800s`, `evict_idle_sessions`), and applies LRU eviction (`ensure_session_capacity` by `(last_active, last_active_seq)`) when at capacity.
  - Each session bounds cookies to `MAX_COOKIES_PER_SESSION = 64` (FIFO eviction), navigation history to `MAX_HISTORY_PER_SESSION = 10` (FIFO eviction), retained HTML to `MAX_STORED_HTML_BYTES = 512 KiB` (`524_288` B sliced at UTF-8 char boundaries via `floor_char_boundary_lossy`), interactive snapshot elements to `MAX_SNAPSHOT_ELEMENTS = 128` (`[ref=eN]`), extracted page text to `MAX_PAGE_TEXT_CHARS = 16_000` chars, and challenge detection scans to a `128 KiB` HTML prefix (`check_html_challenge`).
- **Web Search Cache & HTTP Redirect Bounds (`crates/ps5-agent-tools/src/web.rs`):**
  - `SEARCH_CACHE` (`OnceLock<Mutex<SearchCache>>`) is strictly bounded at `SEARCH_CACHE_MAX_ENTRIES = 128` entries with `SEARCH_CACHE_TTL = 15m` (`900s`) expiration and `VecDeque` LRU eviction on `get`/`insert`.
  - `fetch_validated_url` limits HTTP redirects to `MAX_REDIRECTS = 5` (re-validating DNS/SSRF policy per hop with IP pinning), clamps `connect_timeout` to `1..=30s`, enforces `tools.max_web_bytes` (default `512 KiB` on PS5, `DEFAULT_MAX_WEB_BYTES = 16 MiB` if `0`; HTML extraction cap `MAX_EXTRACT_HTML_BYTES = 16 MiB`), and returns `blocked_by_challenge: true` immediately on anti-bot gates (`detect_challenge_gate`) without solver loops.
- **Virtual Shell & Host Exec Caps (`crates/ps5-agent-tools/src/sandbox_exec.rs`, `crates/ps5-agent-config/src/lib.rs`):**
  - On PS5 (`target_vendor = "sie"`), `sandbox_exec` runs only the in-process pure-Rust `"virtual"` shell (30 built-ins, `0` spawned child processes), while `engine = "host"` is compiled out (`#[cfg(target_vendor = "sie")]` returns `PermissionDenied`).
  - `stdout` and `stderr` are each capped via `cap_outputs` at `max_exec_output_bytes` (default `262_144` B / `256 KiB` in `SandboxConfig` and `ToolsConfig`; `0` uses `DEFAULT_MAX_EXEC_OUTPUT_BYTES = 262_144`, non-zero enforces `MIN_EXEC_OUTPUT_BYTES = 1024` B) with UTF-8 boundary truncation (`"\n...[truncated]"`).
  - Virtual file ops honor `max_file_bytes` (default `2 MiB`, `DEFAULT_MAX_FILE_BYTES = 16 MiB` if `0`) and skip `.git`, `target`, and `node_modules` (`SKIP_WALK_DIRS`); execution respects `exec_timeout_secs` (default `20s`; on host builds `#[cfg(not(target_vendor = "sie"))]`, `spawn_bounded_pipe_reader` also caps pipes at `max_out + 1` bytes with `10ms` cancellation/timeout polling).
- **Workspace Brain Prompt & File Caps (`crates/ps5-agent-runtime/src/brain.rs`, `runtime.rs`, `crates/ps5-agent-config/src/lib.rs`):**
  - Every Brain/memory/rule/workflow markdown file (`AGENTS.md`, `SOUL.md`, `IDENTITY.md`, `USER.md`, `TOOLS.md`, `MEMORY.md`, `HEARTBEAT.md`, `memory/*.md`, `.agents/{rules,workflows}/*.md`) is capped at `MAX_BRAIN_FILE_BYTES = 512 KiB` (`524_288` B) on reads and atomic writes.
  - `build_brain_prompt_section` (enabled when `sandbox.inject_brain_files = true`) enforces `sandbox.max_brain_context_chars` (default `4_000` chars; `0` returns empty string) and `MAX_PER_FILE_PROMPT_CHARS = 4096` per file using fair-share slicing (floored at `32` chars/section) and `truncate_chars_utf8`, while `Runtime` bounds injected skill summaries and active plugin guidelines to `.take(24)` entries each.
- **Memory Retention, Query Scan & Token-Budget Bounds (`crates/ps5-agent-memory/src/lib.rs`, `crates/ps5-agent-config/src/lib.rs`):**
  - Default `MemoryConfig` sets `max_memories_per_agent = 0` (`0 = unlimited`; positive values invoke `enforce_agent_memory_cap` under `write_lock`, fetching in batches starting at `owner_count_hint.max(cap + 64).max(1024)` and archiving lowest-`importance`, oldest entries), `archive_after_messages = 40`, `keep_recent_messages = 16`, `max_context_memories = 8`, and `max_memory_context_chars = 3200`.
  - Read/retrieval paths stay bounded in RAM: `read_agent_memory` caps backend fetch at `fetch_limit = (limit * 4).max(limit).min(2000)`, `retrieve_context_scored` bounds candidate scoring scans at `scan_limit = (limit * 8).clamp(32, 256).max(limit.min(500))`, and `format_memory_block` stops at `max_memory_context_chars` (`3200`) while preferring short summaries when `content > 240` chars.
  - `build_conversation_context` bounds message loading to `archive_after_messages + 1024` (`1024` if `0`), synthesizes archived rolling summaries (`synthesize_rolling_summary`) capped at `1200` chars (scanning up to `50` summary records), scrubs inline `data:<mime>;base64,...` URIs (`sanitize_inline_data_urls`, MIME header `<= 128` B), charges fixed token estimates (`IMAGE_ATTACHMENT_TOKEN_ESTIMATE = 85`, `DATA_URI_PLACEHOLDER_TOKENS = 16`), and trims oldest active turns plus system text (`allowed_content_tokens * 4` chars) to respect `token_budget`.

---

## 4. PS5 Cross-Compiled ELF Verification & Footprint

> **Latest Release Build (`scripts/build-ps5.ps1`):** Verified after all Wave 6 Step A/B Rust and UI changes (`BUILD_EXIT_CODE=0`, `1m 57s`, `3` non-blocking FreeBSD target warnings: `crates/ps5/src/buf.rs:15` unused `from_str`, and `src/shutdown.rs:19-20` `on_term_signal as usize`). Both `elf-verify.exe build\ps5-agent.elf` and `elf-verify.exe build\ps5-agent.elf --require-sqlite-shim --unstripped target\x86_64-sie-ps5-std\release\ps5-agent` passed (`EXIT=0`) and confirmed `build/meta.json` SHA-256 parity with `build/ps5-agent.elf`.

| Artifact | Byte Size | Human Size | Build Time | SHA-256 | ELF Header & `elf-verify.exe` Status |
|---|---|---|---|---|---|
| `build/ps5-agent.elf` (Wave 6 Release) | `9621568` bytes | `9.18 MiB` | `1m 57s` | `a321a25744fbf8c44a0fd2fd0ebe9b43afeb7d19d3b7d4061063977a193a1de7` | `ET_DYN`, `EM_X86_64`, entry `0x5b3654`; `elf_verify: PASS` (`--require-sqlite-shim --unstripped`) |
| `build/m1-ps5-probe.elf` (Wave 2C Baseline — not rebuilt in Wave 6) | `479232` bytes | `468.00 KiB` | `12.19s` | `72c62fef7c9796d463d45ebea995a44136a9518c62e578ad3cb73a2daae36fb7` | `ET_DYN`, `EM_X86_64`, entry `0x3f57c`; `elf_verify: PASS` |

### `build/ps5-agent.elf` Page Layout, Dependencies & Package Manifest

- **Page Layout** (`elf-verify.exe build\ps5-agent.elf --require-sqlite-shim --unstripped target\x86_64-sie-ps5-std\release\ps5-agent`):
  - `exec pages`: `[(0, 5bc000)]` (strictly page-disjoint from writable data at `0x4000` alignment)
  - `write pages`: `[(5bc000, 924000), (924000, 92c000)]`
- **Dynamic Dependencies (`DT_NEEDED`, 4 `.sprx` libraries):**
  1. `libkernel_sys.sprx`
  2. `libSceNotification.sprx`
  3. `libSceLibcInternal.sprx`
  4. `libSceNet.sprx`
- **TLS Dependency Hygiene (Pre-Wave 5 `cargo tree -e no-dev` audit):** `0` matches for `aws-lc` (`rustls` uses `ring` only).
- **Package Manifest (`build/meta.json`):** Valid UTF-8 without BOM, `schema: 2`, `kind: "repo"`, `id: "ps5-agent"`, `name: "PS5 Agent"`, `version: "v0.1.0"`, `filename: "ps5-agent.elf"`, `daemon: true`, `autostart: true`, `installed_at: "2026-11-14T13:27:41+0100"`, `last_update: "2026-11-14"`, `category: "Networking & Servers"`, and `checksum` (`a321a25744fbf8c44a0fd2fd0ebe9b43afeb7d19d3b7d4061063977a193a1de7`) verified equal to `build/ps5-agent.elf` SHA-256.

---

## 5. Host Workspace Verification & Stress / Regression Snapshot

- **Workspace Type & Lint Compile (`cargo check --workspace --all-targets`):** Exited `0` (`CHECK_EXIT=0`, `5.89s`; **0 errors, 0 compiler warnings**). Host binary build (`cargo build --bin ps5-agent`) also exited `0` (`BUILD_EXIT=0`, `0.49s`, **0 warnings**).
- **JS Syntax Check (`node --check crates/ps5-agent-dashboard/assets/app.js`):** Exited `0` (`node_check_exit=0`, verified post-Step-A UI edit; Pre-Wave 5 Playwright live run passed earlier on `18090`/`18091` and was not re-run separately after Wave 6).
- **Full Workspace Test Run (`cargo test --workspace --no-fail-fast`):** Exited `0` (`TEST_EXIT=0`, `1m 00s` overall; **0 errors, 0 compiler warnings**) with **405 passed / 0 failed / 0 ignored** across **30** non-empty test binaries (including `83` tests in `ps5-agent-tools`: `79` unit + `4` `tests/milestone2_tools.rs`, plus `0` failed across `5` empty unit/bin suites and `17` doc-test suites; summed binary runtime `50.17s`).

| Crate / Test Binary | Suite Scope | Passed | Failed | Ignored | Measured Duration |
|---|---|---:|---:|---:|---:|
| `mock-ps5lm` (`src/lib.rs`) | Unit | `9` | `0` | `0` | `0.02s` |
| `ps5-agent` (`src/lib.rs`) | Unit (`startup.rs` PID `<pid>:<boot_time_secs>`, `/data` retry, process matching) | `5` | `0` | `0` | `0.04s` |
| `ps5-agent` (`tests/chat_routing_and_files.rs`) | Integration (chat routing, Thinking: Off, fast-completion SSE/task ordering, attachments, files/HTML preview) | `14` | `0` | `0` | `3.46s` |
| `ps5-agent` (`tests/config_restart_lifecycle.rs`) | Integration (BOM/partial TOML, `0 = unlimited`, `.broken.bak`, restart) | `5` | `0` | `0` | `6.23s` |
| `ps5-agent` (`tests/extensions_mcp_skills.rs`) | Integration (`SKILL.md`, Streamable-HTTP MCP, `plugin.json`, wildcards) | `6` | `0` | `0` | `4.15s` |
| `ps5-agent` (`tests/milestone8_stress.rs`) | Integration / Stress & Chaos | `6` | `0` | `0` | `2.98s` |
| `ps5-agent` (`tests/openclaw_sandbox_browser_doctor.rs`) | Integration (browser viewport preview, CSP/sandbox, agentic controls, doctor) | `9` | `0` | `0` | `2.56s` |
| `ps5-agent` (`tests/schedules_and_background.rs`) | Integration (headless autonomous scheduler, SQLite/JSONL schedules parity, HTTP `/schedules*` + AI builder, `schedule_create` tool) | `4` | `0` | `0` | `0.90s` |
| `ps5-agent` (`tests/thinking_and_ps5lm_sync.rs`) | Integration (thinking levels, Group routing, KV prefill + PS5LM capability sync) | `6` | `0` | `0` | `1.98s` |
| `ps5-agent` (`tests/unlimited_and_leak_bounds.rs`) | Integration (`0 = unlimited` + RAM/CPU/storage leak bounds) | `9` | `0` | `0` | `7.36s` |
| `ps5-agent` (`tests/wave1_bug_regressions.rs`) | Integration (Wave 1 regressions) | `6` | `0` | `0` | `2.40s` |
| `ps5-agent` (`tests/wave6_features_and_perf.rs`) | Integration (Wave 6 static KV prefix, slot affinity, `smart_tool_routing`, `import-kv`, `timings_json`, browser viewport, leak bounds) | `7` | `0` | `0` | `2.74s` |
| `ps5-agent-config` (`src/lib.rs`) | Unit (TOML sanitization, `0 = unlimited`, `.broken.bak`, GGML `cache_type_k/v` allowlist, `smart_tool_routing`, `dry_multiplier = 0.8`, `repeat_last_n = 256`) | `12` | `0` | `0` | `0.26s` |
| `ps5-agent-core` (`src/lib.rs`) | Unit (models, `InferenceTimings`, `StreamEvent::ChatCompleted`, `BrowserActivity`) | `5` | `0` | `0` | `0.00s` |
| `ps5-agent-dashboard` (`src/lib.rs`) | Unit (embedded SPA assets, HTML controls) | `5` | `0` | `0` | `0.01s` |
| `ps5-agent-database` (`src/lib.rs`) | Unit (`NORMAL` locking, `-shm` cleanup, `.corrupt.bak`, `Busy`/`Locked` retry, schema v6 `timings_json`, `shrink_memory`, pruning) | `30` | `0` | `0` | `2.94s` |
| `ps5-agent-database` (`tests/storage_parity.rs`) | Integration (SQLite + JSONL parity) | `6` | `0` | `0` | `0.84s` |
| `ps5-agent-http` (`src/lib.rs`) | Unit (`LagCoalescer`, rate limiter, headers, CSP) | `5` | `0` | `0` | `0.11s` |
| `ps5-agent-http` (`tests/auth_and_restart_regression.rs`) | Integration (login normalization, bootstrap self-heal, live auth toggle, reusable bind) | `6` | `0` | `0` | `1.12s` |
| `ps5-agent-http` (`tests/http_security_tests.rs`) | Integration (Auth, CSRF, LAN/CORS, `/inference/import-kv`, preview CSP) | `16` | `0` | `0` | `1.07s` |
| `ps5-agent-inference` (`src/lib.rs`) | Unit (`ThinkStreamSplitter`, `StreamingRepetitionGuard`, `parse_timings`, penalty omission, `import_kv_and_sampling`) | `40` | `0` | `0` | `2.05s` |
| `ps5-agent-inference` (`tests/milestone2_inference.rs`) | Integration (all 7 PS5LM modes + tool parsing) | `8` | `0` | `0` | `5.34s` |
| `ps5-agent-logging` (`src/lib.rs`) | Unit (ring `shrink_to_fit`, `5s` rotate backoff, I/O throttle, rotation) | `7` | `0` | `0` | `0.08s` |
| `ps5-agent-manager` (`src/lib.rs`) | Unit (built-in upgrade, agent registry) | `9` | `0` | `0` | `0.07s` |
| `ps5-agent-memory` (`src/lib.rs`) | Unit (zero-alloc ASCII CI `score_memory`, compaction fast-path, token budgeting) | `8` | `0` | `0` | `0.00s` |
| `ps5-agent-memory` (`tests/memory_tests.rs`) | Integration (5-scope memory + compaction) | `4` | `0` | `0` | `0.07s` |
| `ps5-agent-messaging` (`src/lib.rs`) | Unit | `10` | `0` | `0` | `0.01s` |
| `ps5-agent-runtime` (`src/lib.rs`) | Unit (routing, dynamic language directive, FNV-1a slot affinity, `select_tools_for_casual_turn`, scheduler, cancellation) | `65` | `0` | `0` | `0.31s` |
| `ps5-agent-tools` (`src/lib.rs`) | Unit (`SkillScanCache`, browser local controls & cursor coords, custom tools >20, `rustls` `ring` provider, sandbox, registry) | `79` | `0` | `0` | `0.98s` |
| `ps5-agent-tools` (`tests/milestone2_tools.rs`) | Integration (sandbox + SSRF + tool specs) | `4` | `0` | `0` | `0.09s` |
| **Total (`cargo test --workspace --no-fail-fast`)** | **30 non-empty binaries (`83` in `ps5-agent-tools` + 5 empty binaries & 17 doc-test suites)** | **`405`** | **`0`** | **`0`** | **`1m 00s` overall (`50.17s` summed)** |

### Milestone 8 Stress, Chaos, Recovery & Integrity Coverage (`tests/milestone8_stress.rs`, `2.98s`)

1. `stress_20_concurrent_tasks_bounded_to_single_inference`: Dispatches 20 concurrent tasks across all 5 default agents (`commander`, `researcher`, `programmer`, `analyst`, `executor`) and verifies `mock-ps5lm` `max_concurrent() == 1`, `errors_total == 0`, and `requests_total >= 20`.
2. `stress_120_message_conversation_compaction_and_rolling_summary`: Inserts 120 messages and verifies compaction retains 16 active messages, archives 104 messages, persists a non-empty rolling summary (`rolling-summary-{conv_id}` / `rolling_summary:{conv_id}`), and keeps prompt `estimated_tokens <= 512`.
3. `chaos_five_agent_workflow_and_all_ps5lm_modes_under_503_recovery`: Exercises multi-agent A2A delegation (`TaskAssignment` + `TaskCompleted` verified with `a2a.len() >= 2`), transient HTTP `503` retry/recovery, `list_inference_models()`, and all PS5LM inference endpoints including `/v1/completions` and `/v1/systemone`.
4. `restart_recovery_pauses_side_effecting_and_resumes_readonly`: Simulates restart recovery and confirms interrupted read-only tasks auto-resume to `TaskStatus::Completed` while interrupted side-effecting tasks pause safely in `TaskStatus::Waiting`.
5. `tool_approval_gate_pause_and_approve_to_completion`: Verifies `delete_file` pauses for human approval with target file preserved, then completes deletion upon `resolve_approval(..., true, "admin")` with zero pending approvals remaining.
6. `post_stress_sqlite_integrity_and_wal_checkpoint`: Verifies `PRAGMA integrity_check == "ok"` and clean SQLite checkpointing after stress execution.

---

## 6. Verification Matrix: Host & PS5-ELF Verified vs. Physical PS5 Hardware-Pending

| Area / Metric | Status | Basis & Notes |
|---|---|---|
| FW `13.40` (`0x13400000`) kernel data offsets | **Host & PS5-ELF Verified** | Verified in `ps5-payload-dev/sdk v0.45` `crt/kernel.c`. |
| `x86_64-sie-ps5-std` Wave 6 release build, ELF layout & `meta.json` | **Host & PS5-ELF Verified** | `scripts/build-ps5.ps1` (`EXIT=0`, `1m 57s`), `build/ps5-agent.elf` (`9621568` B / `9.18 MiB`, SHA-256 `a321a25744fbf8c44a0fd2fd0ebe9b43afeb7d19d3b7d4061063977a193a1de7`, `ET_DYN`, `entry: 0x5b3654`, `exec: [(0, 5bc000)]`, `write: [(5bc000, 924000), (924000, 92c000)]`, `DT_NEEDED: 4 .sprx`) passed `elf-verify.exe` & `--require-sqlite-shim` and matches `build/meta.json` (`schema: 2`, `daemon: true`, `autostart: true`, `installed_at: "2026-11-14T13:27:41+0100"`). |
| SQLite `Stat11` ABI shim (`__ps5_stat11`/`fstat11`/`lstat11`), schema `v6` & `Busy`/`Locked` retry | **Host & PS5-ELF Verified** | Verified via `elf-verify.exe --require-sqlite-shim` on `build/ps5-agent.elf` and host SQLite unit/integration tests (`30` unit + `storage_parity.rs`, `milestone8_stress.rs`, `unlimited_and_leak_bounds.rs`, `schedules_and_background.rs`, `wave6_features_and_perf.rs`). |
| Workspace compilation, JS syntax & unit/integration/stress/perf suites | **Host Verified (Wave 6)** | `cargo check --workspace --all-targets` (`EXIT=0`, `5.89s`, `0` host warnings), `node --check crates/ps5-agent-dashboard/assets/app.js` (`EXIT=0`), `cargo build --bin ps5-agent` (`EXIT=0`, `0.49s`), and `cargo test --workspace --no-fail-fast` (`EXIT=0`, `1m 00s`, **405/405** passed across `30` non-empty binaries including `83` in `ps5-agent-tools`, `0` failed, `0` ignored), including `tests/wave6_features_and_perf.rs` (`7/7`), `tests/milestone8_stress.rs` (`6/6`), and `tests/unlimited_and_leak_bounds.rs` (`9/9`). |
| Live `PS5LM` throughput uplift (`90 -> 115–120 tok/s`), `prefill_kv_cache`, KV slots, samplers & memory coexistence | **Physical PS5 Hardware-Pending** | Static KV prefix, FNV-1a slot affinity, `smart_tool_routing`, `TCP_NODELAY`, `InferenceTimings`, `import-kv`, and `StreamingRepetitionGuard` are host-verified against `mock-ps5lm`; actual tokens/sec uplift, thermal headroom, and real GDDR6 coexistence require co-locating `ps5-agent.elf` with live `PS5LM` (`127.0.0.1:8081`) on physical PS5. |
| Live PS5 Slim FW `13.40` execution, `pldmgr` cold-boot autostart (`<pid>:<boot_time_secs>` & `/data` retry), `SYS_thr_set_name` & on-console RSS | **Physical PS5 Hardware-Pending** | Requires deploying `build/ps5-agent.elf` + `build/meta.json` (`daemon: true`, `autostart: true`) to a jailbroken PS5 Slim (`13.40`) via `elfldr` (`:9021`) or `pldmgr` cold boot. |
| Real `/data` filesystem, `/download0` fallback, log rotation backoff & SQLite `TRUNCATE` throughput on PS5 | **Physical PS5 Hardware-Pending** | Requires running on physical console storage (`scripts/verify-on-ps5.py`), including Brain markdown writes (`workspace/`) and virtual shell `/data` file I/O. |
| FW `13.x` Rest-Mode suspend & post-wake error `163` recovery | **Physical PS5 Hardware-Pending** | `RecoveringTcpListener` logic verified on host; real Sony kernel socket invalidation across suspend/resume requires physical console testing. |
