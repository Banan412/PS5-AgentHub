# PS5 Firmware 13.40 Feasibility & Compatibility Report (`ps5-agent`)

**Target Console:** PlayStation 5 Slim (AMD Zen 2 CPU, AMD RDNA 2 GPU, 16 GB shared GDDR6)  
**Target Firmware:** PS5 Firmware `13.40` (`0x13400000`)  
**Upstream Repositories Audited:**
- [`ps5-payload-dev/sdk`](https://github.com/ps5-payload-dev/sdk) (`v0.45`, 2026-10-21; `master` 2026-11-08)
- [`ps5-payload-dev/elfldr`](https://github.com/ps5-payload-dev/elfldr) (`v0.27`, 2026-10-21)
- [`neokuze/ps5-rust`](https://github.com/neokuze/ps5-rust) (`main`, 2026-10-20)
- [`cobanov/PS5LM`](https://github.com/cobanov/PS5LM) (`v0.11.0`, 2026-11-02; `main` 2026-11-09)

---

## 1. Executive Summary

Building an autonomous multi-agent AI runtime (**PS5 Agent**) natively in Rust (`std` + Tokio + Axum + Reqwest/Rustls + SQLite) as a PS5 userland ELF payload targeting firmware **13.40** is **feasible**, provided the runtime respects strict PS5 kernel, memory, CPU, ABI, and lifecycle constraints discovered in the upstream repositories.

Key findings:
1. **Firmware 13.40 is officially supported** by `ps5-payload-dev/sdk` (`v0.41` through `v0.45`) with verified kernel data offsets in `crt/kernel.c`.
2. **Rust `std` + Tokio + Reqwest (`rustls` + `ring`) work** via `neokuze/ps5-rust`'s `x86_64-sie-ps5-std.json` target and `ps5-std-rt` FreeBSD 11 -> FreeBSD 12 ABI bridge.
3. **Critical SQLite C ABI Hazard Identified & Solved:** `ps5-payload-dev/sdk`'s `<sys/stat.h>` defines FreeBSD 11 `struct stat` (120 bytes), while `ps5-std-rt` exports `stat`/`fstat`/`lstat` expecting FreeBSD 12 `Stat12` (224 bytes). Compiling `sqlite3.c` without symbol redirection would corrupt 104 bytes of stack. We solve this cleanly via `include/ps5_sqlite_shim.h` redirecting C `stat`/`fstat`/`lstat` calls to `__ps5_stat11`/`__ps5_fstat11`/`__ps5_lstat11`, plus a `StorageBackend` trait offering both SQLite (`rusqlite` bundled) and a persistent `JsonlStorage` fallback.
4. **Shared Inference Architecture is Mandatory:** Payload processes receive `sys_budget_set(0)` (0 Direct Memory, ~8 GiB shared Flexible Memory). `PS5LM` (running as native app `PPSA99575` on Vulkan GPU with up to 11.4 GiB memory, or legacy payload <= 1.6 GiB) serves a single loaded GGUF model on `http://127.0.0.1:8081/v1`. `ps5-agent.elf` must stay ultra-lean (< 64 MiB RSS) and serialize or strictly bound inference requests.

---

## 2. Firmware 13.40 & SDK Compatibility (`ps5-payload-dev/sdk v0.45`)

Verified in `crt/kernel.c` of `ps5-payload-dev/sdk` (`v0.45`):

```c
case 0x13400000:
case 0x13420000:
  KERNEL_ADDRESS_TEXT_BASE        = KERNEL_ADDRESS_DATA_BASE - 0x0CB0000;
  KERNEL_ADDRESS_ALLPROC          = KERNEL_ADDRESS_DATA_BASE + 0x28C9E80;
  KERNEL_ADDRESS_SECURITY_FLAGS   = KERNEL_ADDRESS_DATA_BASE + 0x0D99064;
  KERNEL_ADDRESS_ROOTVNODE        = KERNEL_ADDRESS_DATA_BASE + 0x3137510;
  KERNEL_ADDRESS_BUS_DATA_DEVICES = KERNEL_ADDRESS_DATA_BASE + 0x20981E8;
  KERNEL_OFFSET_VMSPACE_P_ROOT    = 0x1d0;
  KERNEL_OFFSET_VMSPACE_VM_PMAP   = 0x2e8;
  break;
```

- **Toolchain Triple & Linker:** `clang -target x86_64-sie-ps5 -fno-stack-protector -fno-plt -femulated-tls` linked via `ld.lld -m elf_x86_64 -pie --eh-frame-hdr -z max-page-size=0x4000 -T ldscripts/elf_x86_64.x`.
- **Clang >= 20 CRT Handling:** Clang 20+ auto-appends `crt1.o` when `SCE_PROSPERO_SDK_DIR` is set; our host `prospero-clang` wrapper explicitly passes `-nostartfiles` and appends `crt1.o` once on executable links for deterministic builds across LLVM 18–23.
- **Dynamic Sony `.sprx` Imports:** `libSceLibcInternal.sprx`, `libSceNet.sprx`, `libkernel_web.sprx` (always linked), plus optional `libSceNotification.sprx` (toasts) and `libkernel_sys.sprx` (CPU/SoC temperature, model name).
- **License Implication:** Because `crt1.o`, `crt/kernel.c`, and `ps5-rust` bindings are GPL-3.0-or-later, `ps5-agent` is licensed under `GPL-3.0-or-later`.

---

## 3. Rust `std`, Target Spec & Crate Compatibility (`neokuze/ps5-rust`)

| Component | Target / Crate Behavior on PS5 | Mitigation / Verification |
|---|---|---|
| Rust Target | `targets/x86_64-sie-ps5-std.json` (`os: freebsd`, `cpu: znver2`, `has-thread-local: false`, `panic-strategy: abort`, `default-uwtable: true`, `relro-level: full`, `plt-by-default: false`) | Uses nightly `-Z build-std=std,panic_abort` and `-Z json-target-spec`. Page-aligned `.eh_frame_hdr` prevents code/data page sharing faults. |
| `bindgen` Header Parsing | `libclang` silently drops declarations under `--target=x86_64-sie-ps5` | `ps5-sys/build.rs` passes `--target=x86_64-unknown-freebsd` to bindgen with OS-portable regexes (`[\\/]`). |
| FreeBSD 11 vs 12 Syscalls | PS5 kernel uses 32B `kevent`, 120B `stat`, old `dirent`, lacks `accept4`, `F_DUPFD_CLOEXEC`, `getrandom`, `faccessat`, `readlinkat`, `clock_nanosleep`, and SDK `syscalls.o` omits carry-flag errno checks | `ps5-std-rt` translates `Kevent12<->Kevent11`, `Stat12<->Stat11`, `Dirent12<->Dirent11`, emulates missing syscalls, patches `libc.a` -> `libps5c.a`, and regenerates ~230 error-checking syscall stubs. |
| Tokio (`1.x`) | Works with `--cfg mio_unsupported_force_waker_pipe`; `available_parallelism()` reports 16 even though payloads can only use 5 CPUs (`0xea00`) | Explicitly configure `Builder::new_multi_thread().worker_threads(2)` (capped `<= 3` on PS5) to avoid starving Sony system services. |
| TLS / HTTPS (`reqwest 0.12`) | Sony's `libSceHttp2` fails (`sceHttp2SendRequest` error) from payloads; `aws-lc-rs` / OpenSSL require CMake/NASM or unsupported platform assumptions | Use `reqwest 0.12` with `rustls` + `ring` + `webpki-roots` (`default-features = false`, `rustls-tls-webpki-roots-no-provider`), compiled with `prospero-clang`. |
| HTTP Server (`axum 0.8` / `hyper 1.x`) | Pure Rust async over Tokio `TcpListener` (`kevent` non-blocking) | Safe across rest mode (`kevent` does not trigger the blocking `accept()` console power-off bug), with reconnect loop for post-wake OS error `163`. |
| `println!` / `eprintln!` | `elfldr` attaches fds `0..2` to the sender socket; disconnecting closes them and causes `BrokenPipe` panic in `std::io::println!` | **Banned everywhere.** `ps5-agent-logging` uses `let _ = writeln!(std::io::stdout(), ...)` plus rotating file logs and `/dev/klog`. |
| Normal `main()` Return | Returning from `main()` invokes Sony `__cxa_atexit` destructors while threads hold heap allocations (`[SceLibc] A heap error is detected`) | Clean shutdown checkpoints DB, flushes logs, and exits via `libc::_Exit(0)` on PS5. |

---

## 4. SQLite Compatibility & Storage Strategy

### The `Stat11` vs `Stat12` Hazard
- `ps5-payload-dev/sdk/include/freebsd/sys/stat.h` defines `struct stat` as **120 bytes** (FreeBSD 11).
- `ps5-std-rt` defines `#[no_mangle] pub extern "C" fn stat/fstat/lstat` taking `*mut Stat12` (**224 bytes**).
- `libsqlite3-sys` (`rusqlite` with `bundled`) compiles `sqlite3.c` using `CC_x86_64_sie_ps5_std=prospero-clang`, which reads `<sys/stat.h>` (120 bytes) and emits calls to `stat`/`fstat`/`lstat`. Without redirection, `ps5-std-rt`'s 224-byte write overflows stack buffers by 104 bytes.

### Implemented Solution
1. Added `#[no_mangle] pub unsafe extern "C" fn __ps5_fstat11(fd: c_int, buf: *mut c_void) -> c_int` in `crates/ps5-std-rt/src/lib.rs` (alongside `__ps5_stat11` and `__ps5_lstat11`).
2. Created `include/ps5_sqlite_shim.h` (guarded by `#ifndef __ASSEMBLER__` so `ring`'s `.S` assembly files are unaffected) redirecting C `stat`/`fstat`/`lstat` via `__asm__("__ps5_*11")` to the 120-byte syscall wrappers.
3. Passed `-include include/ps5_sqlite_shim.h -DSQLITE_OS_UNIX=1 -DSQLITE_THREADSAFE=1 -DSQLITE_TEMP_STORE=2 -DSQLITE_OMIT_LOAD_EXTENSION=1 -DSQLITE_DEFAULT_MEMSTATUS=0 -DHAVE_POSIX_FALLOCATE=0` in `CFLAGS_x86_64_sie_ps5_std`.
4. **Filesystem & Locking Settings:** `/data` does not support symlinks (`EOPNOTSUPP`) and writes at ~1–2 MB/s. `SqliteStorage` uses a single `Mutex<rusqlite::Connection>` with `PRAGMA locking_mode = EXCLUSIVE`, `PRAGMA journal_mode = WAL` (falling back automatically to `TRUNCATE` if WAL is rejected by the mount), `PRAGMA synchronous = NORMAL`, `PRAGMA cache_size = -4000` (~4 MiB), and `PRAGMA wal_checkpoint(TRUNCATE)` on shutdown.
5. **Fallback Storage:** `StorageBackend` trait also includes `JsonlStorage` (atomic write-temp-rename + append-only `.jsonl`), selectable via `database.driver = "auto" | "sqlite" | "jsonl"`.

---

## 5. Process Lifecycle & Rest Mode (`ps5-payload-dev/elfldr v0.27`)

- **Startup:** Sent as an ELF payload to `elfldr` on port `9021` (e.g. `prospero-deploy` or `nc` or loaded via `file:/data/ps5-agent/ps5-agent.elf`). Runs as an isolated process under `SceSpZeroConf` with jailbreak root/uid0/caps raised by `elfldr`.
- **Rest Mode:** Payloads survive console suspend/resume if no thread is blocked inside raw `accept()`/`recv()`. `ps5-std-rt` + Tokio use `kevent`. However, after wake on firmware 13.x, sockets fail with Sony OS error `163` until network/console restart. `ps5-agent-http` detects clock jumps and error `163`, attempts listener re-bind with backoff, and surfaces a degraded-network alert.
- **Cold Reboot:** Full power-off/reboot clears the jailbreak and payload processes. The user must re-run the jailbreak entry point (e.g., WebKit/BD-J + `elfldr` + `PS5LM`) and re-send `ps5-agent.elf` (automated via `scripts/deploy-ps5.ps1` or `elfldr` `file:/data/ps5-agent/ps5-agent.elf`).

---

## 6. PS5LM Integration (`cobanov/PS5LM v0.11.0`)

- **Endpoints:** `POST /v1/chat/completions`, `POST /v1/responses`, `POST /v1/messages`, `POST /v1/completions`, `POST /v1/systemone`, `GET /v1/models`, and `GET /health` on `http://127.0.0.1:8081/v1` (with `/health` probed at root `http://127.0.0.1:8081/health` and `/v1/health` fallback). `/v1/completions` renders ChatML prompts with `<tool_call>` schema injection, and `/v1/systemone` evaluates structured single-pass probability queries (`choice`, `noul`, `score`).
- **Single Shared Model:** `max_concurrent_inference = 1` default semaphore ensures all 5+ agents share one loaded Qwen GGUF without saturating PS5LM or triggering OOM.
- **Qwen Template Rule:** `PS5LM` `patches/0004-fold-late-system-messages.patch` documents that Qwen templates reject non-leading system messages. `Ps5LmProvider` folds all system/memory/tool prompts into `messages[0]`.
- **Tool Calling:** 3-tier extraction: (1) OpenAI `tool_calls` array, (2) Qwen `<tool_call>{...}</tool_call>` XML tags, (3) strict structured JSON fallback (`{"thought", "tool_calls", "final_answer"}`).

---

## 7. Verification Matrix Summary

| Capability | Verification Basis |
|---|---|
| Firmware `13.40` kernel offsets | Verified directly in `ps5-payload-dev/sdk v0.45` `crt/kernel.c` |
| `x86_64-sie-ps5-std` compilation & ELF validity | Built locally with `clang`/`ld.lld` + `elf-verify.exe` + `llvm-readelf` |
| SQLite `Stat11` shim symbols (`__ps5_*11`) | Verified in compiled ELF symbol table + host SQLite integration tests |
| Multi-agent, HTTP security, Dashboard UI, Stress tests | Verified on host via `cargo test` + live `mock-ps5lm` + headless browser check |
| Live execution on physical PS5 Slim FW 13.40 | **Hardware-Pending** (requires user's jailbroken PS5 console on LAN) |
