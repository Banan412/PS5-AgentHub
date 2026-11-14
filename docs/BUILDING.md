# Building & Testing `ps5-agent`

This guide covers toolchain setup, target and C/linker configuration, PS5 cross-compilation (`x86_64-sie-ps5-std`), ELF verification, and host workspace testing.

---

## 1. Toolchain Requirements

### Rust Nightly (`rust-toolchain.toml`)

`rust-toolchain.toml` pins the workspace to Rust `nightly` with the required components:

```toml
[toolchain]
channel = "nightly"
components = ["rust-src", "rustfmt", "clippy"]
```

Cross-compiling for PS5 uses two nightly flags:
- `-Z json-target-spec` — enables loading the custom target specification `targets/x86_64-sie-ps5-std.json`.
- `-Z build-std=std,panic_abort` — rebuilds `core`, `std`, and `panic_abort` from `rust-src` against the PS5 target.

### LLVM / Clang, `ld.lld`, and `libclang`

- **Clang + `ld.lld`**: Required to compile C dependencies (`rusqlite` bundled `sqlite3.c`, `ring` C/assembly primitives) and link PIE ELF payloads for `x86_64-sie-ps5`.
- **`libclang`**: Required by `bindgen` in `crates/ps5-sys/build.rs` to parse SDK headers under `--target=x86_64-unknown-freebsd`.
  - On Windows, set `LIBCLANG_PATH` to the directory containing `libclang.dll` (automatically set by `scripts/build-ps5.ps1` to `PS5_MINGW_BIN`, defaulting to `..\.ps5-toolchain\mingw64\bin`).
  - On Windows, `..\.ps5-toolchain\mingw64\bin` provides the standalone WinLibs GCC + LLVM/Clang/LLD toolchain (`gcc.exe`, `clang.exe`, `ld.lld.exe`, `libclang.dll`).

### PS5 Payload SDK (`ps5-payload-dev/sdk v0.45`)

- Requires `ps5-payload-dev/sdk` (`v0.45` or compatible) for PS5 Firmware `13.40` headers, libraries (`libc.a`, `crt1.o`, Sony `.sprx` stubs), linker script (`ldscripts/elf_x86_64.x`), and wrapper binaries (`prospero-clang`, `prospero-ar`/`llvm-ar`, `prospero-strip`, `prospero-lld`, `elf-verify`).
- Default SDK locations used by the build scripts:
  - **Windows (`scripts/build-ps5.ps1`)**: `$env:PS5_PAYLOAD_SDK`, or `..\.ps5-toolchain\ps5-payload-sdk` relative to the repo root.
  - **Linux/macOS (`scripts/build-ps5.sh`, `Makefile`)**: `${PS5_PAYLOAD_SDK:-/opt/ps5-payload-sdk}`.

---

## 2. Target & C/Linker Configuration

### Custom Target Spec (`targets/x86_64-sie-ps5-std.json`)

Key target fields configured for PS5 payloads with Rust `std`:

| Field | Value | Purpose |
|---|---|---|
| `cpu` | `"znver2"` | Optimizes codegen for the PS5 AMD Zen 2 CPU |
| `llvm-target` / `os` / `vendor` | `x86_64-unknown-freebsd` / `freebsd` / `sie` | Uses FreeBSD `std` ABI with `cfg(target_vendor = "sie")` gates |
| `panic-strategy` | `"abort"` | No unwinding runtime; panic hook runs before aborting |
| `has-thread-local` | `false` | Forces emulated TLS (`-femulated-tls`) instead of unsupported ELF TLS |
| `default-uwtable` / `stack-probes` | `true` / `inline` | Stack unwinding tables for diagnostics and inline stack probing |
| `linker-flavor` / `linker` | `"gnu-cc"` / `"prospero-clang"` | Links through the SDK C compiler driver |
| `no-default-libraries` / `crt-objects-fallback` | `true` / `"false"` | Prevents Rust from injecting host CRT objects or host system libs |
| `position-independent-executables` / `relocation-model` | `true` / `"pic"` | Produces `ET_DYN` PIE payloads required by `elfldr` |
| `plt-by-default` / `relro-level` | `false` / `"full"` | Matches `-fno-plt` and full RELRO hardening |
| `late-link-args` | `["-lSceLibcInternal", "-lSceNet", "-lkernel_web"]` | Links required Sony `.sprx` dynamic libraries |

Linker alignment (`prospero-lld` -> `ld.lld`):
- Passes `-m elf_x86_64 -pie --eh-frame-hdr -z max-page-size=0x4000 -T <sdk>/ldscripts/elf_x86_64.x`.
- `max-page-size=0x4000` (16 KiB) ensures `PF_X` (`.text`) and `PF_W` (writable data) `PT_LOAD` segments never share a 16 KiB PS5 memory page.

Cargo target config (`.cargo/config.toml`):
- Sets `rustflags = ["-Zunstable-options", "--cfg", "mio_unsupported_force_waker_pipe"]` for `x86_64-sie-ps5-std` so Tokio's `mio` uses pipe-based wakers.
- Intentionally does **not** set a global `[build] target` so `cargo test --workspace` runs natively on the host.

### `prospero-clang` Wrapper

Both `CC_x86_64_sie_ps5_std` and `CARGO_TARGET_X86_64_SIE_PS5_STD_LINKER` point to `prospero-clang` (`prospero-clang.exe` on Windows):
- Injects `-target x86_64-sie-ps5 -fvisibility-nodllstorageclass=default -isysroot <sdk> -isystem <sdk>/target/include -L <sdk>/target/lib -L <sdk>/target/user/homebrew/lib`.
- Enforces codegen flags: `-fno-stack-protector -fno-plt -femulated-tls -nostartfiles`.
- On executable links, links `-lc`, appends a **single** `<sdk>/target/lib/crt1.o` (avoiding duplicate CRT startup objects across LLVM/Clang versions, including Clang >= 20), and appends `-lkernel_web -lSceLibcInternal -lSceNet`.
- Note on archiver: `build-ps5.ps1` sets `AR_x86_64_sie_ps5_std` to `llvm-ar.exe`, and `build-ps5.sh` sets it to `prospero-ar`.

### `CFLAGS_x86_64_sie_ps5_std` & the SQLite `Stat11` ABI Shim

The build scripts export:

```text
CFLAGS_x86_64_sie_ps5_std="-include <repo>/include/ps5_sqlite_shim.h -DSQLITE_OS_UNIX=1 -DSQLITE_THREADSAFE=1 -DSQLITE_TEMP_STORE=2 -DSQLITE_OMIT_LOAD_EXTENSION=1 -DSQLITE_DEFAULT_MEMSTATUS=0 -DHAVE_POSIX_FALLOCATE=0"
```

Why this is required:
- `ps5-payload-dev/sdk`'s `<sys/stat.h>` defines FreeBSD 11 `struct stat` (120 bytes), while `crates/ps5-std-rt` exports FreeBSD 12 `stat`/`fstat`/`lstat` expecting Rust `std`'s 224-byte `Stat12` layout.
- Compiling `rusqlite`'s bundled `sqlite3.c` without redirection would corrupt 104 bytes of stack per `stat`/`fstat`/`lstat` call.
- `include/ps5_sqlite_shim.h` (guarded by `#ifndef __ASSEMBLER__` so `ring`'s `.S` assembly files are unaffected) redirects C `stat`, `fstat`, and `lstat` calls via `__asm__` to `ps5-std-rt`'s 120-byte wrappers: `__ps5_stat11`, `__ps5_fstat11`, and `__ps5_lstat11`.
- `-DHAVE_POSIX_FALLOCATE=0` disables `posix_fallocate` (unavailable on PS5), and `-DSQLITE_TEMP_STORE=2` stores temporary tables in memory.
- At runtime on PS5 (`target_vendor = "sie"`), `config/config.ps5-slim-1340.toml` sets `wal_mode = false` (matching the `#[cfg(target_vendor = "sie")]` config default), and `SqliteStorage` enforces `PRAGMA journal_mode = TRUNCATE` with `PRAGMA locking_mode = NORMAL` so SQLite never maps `.db-shm` via `mmap(MAP_SHARED)` on `/data` (host builds retain `WAL` mode when `wal_mode = true`).

### PS5 Process Naming (`SYS_thr_set_name`)

- `crates/ps5/src/payload.rs` exposes `ps5::payload::set_process_name(name: &str) -> i32`, which invokes FreeBSD `SYS_thr_set_name` (`464`) via inline `syscall` assembly (truncating to FreeBSD's 19-byte `MAXCOMLEN`).
- Both `ps5-agent` (`"ps5-agent.elf"`) and `bins/m1-ps5-probe/src/main.rs` (`"m1-ps5-probe.elf"`) call `set_process_name` at startup so `pldmgr` displays the real payload name instead of `payload.elf`.

### Pure-Rust/Clang `ring` TLS (Instead of `aws-lc-rs`)

`Cargo.toml` configures `rustls` and `reqwest` without `aws-lc-rs` or OpenSSL:

```toml
reqwest = { version = "0.12", default-features = false, features = ["rustls-tls-webpki-roots-no-provider", "json", "stream"] }
rustls = { version = "0.23", default-features = false, features = ["ring", "std", "tls12"] }
```

- Avoids `aws-lc-rs` / OpenSSL CMake and NASM build-system assumptions that fail on `x86_64-sie-ps5-std` (`cargo tree -e no-dev` has zero `aws-lc` dependencies), and avoids Sony's `libSceHttp2` (`sceHttp2SendRequest` fails from userland payloads).
- `ring`'s C and `.S` sources compile cleanly with `prospero-clang` thanks to `#ifndef __ASSEMBLER__` in `include/ps5_sqlite_shim.h`.

### Subprocess Guard & Pure-Rust Fallbacks (`crates/ps5-agent-tools/src/sandbox_exec.rs`)

- **Compile-time subprocess exclusion (`#[cfg(not(target_vendor = "sie"))]`)**: In `crates/ps5-agent-tools/src/sandbox_exec.rs`, `std::process::Command` (`sh -c` on Unix, `cmd /C` on Windows) and its blocking process helpers (`run_host_engine`, `run_host_engine_blocking`, `spawn_bounded_pipe_reader`, `cleanup_host_child`, `CancelOnDrop`, and `SAFE_HOST_ENV_KEYS = ["PATH", "HOME", "TEMP", "TMP", "SYSTEMROOT"]`) are gated behind `#[cfg(not(target_vendor = "sie"))]`, with zero `tokio::process` dependency.
- **PS5 (`#[cfg(target_vendor = "sie")]`) virtual shell**: `run_host_engine` unconditionally returns `AgentError::PermissionDenied("host shell execution is disabled on PS5; use engine='virtual'")`. By default on both host and PS5, `sandbox_exec` runs the in-process pure-Rust `"virtual"` shell (`run_virtual_async` / `run_virtual_blocking`) supporting 30 built-in commands (`VIRTUAL_BUILTINS`, including `pwd`, `ls`, `cat`, `grep`, `rg`, `find`, `sed`, `sha256sum`, `json`, etc.) inside the workspace sandbox.
- **Pure-Rust SHA-256**: `sandbox_exec.rs` embeds a self-contained FIPS 180-4 `mod sha256` implementation for virtual `sha256sum` without requiring an external `sha2` dependency in `crates/ps5-agent-tools/Cargo.toml` (while the root `ps5-agent` crate uses `sha2` + `hex` for `compute_release_stamp()` and auth/API hashing).
- **Workspace lint config (`Cargo.toml`)**: `[workspace.lints.rust]` declares `unexpected_cfgs = { level = "warn", check-cfg = ['cfg(target_vendor, values("sie"))', 'cfg(mio_unsupported_force_waker_pipe)'] }` so host checks do not warn on PS5 `cfg` gates.

---

## 3. Build, Verify & Test Commands

### Windows (PowerShell)

`scripts/build-ps5.ps1` auto-configures `PS5_PAYLOAD_SDK`, `PS5_MINGW_BIN`, `LIBCLANG_PATH`, `PATH`, and all `*_x86_64_sie_ps5_std` environment variables, compiles with `cargo +nightly`, strips with `prospero-strip.exe` into `build\`, runs `elf-verify.exe`, and (for `ps5-agent`) writes `build\meta.json`:

```powershell
# Build and verify the main ps5-agent payload -> build\ps5-agent.elf + build\meta.json
.\scripts\build-ps5.ps1 -Package ps5-agent

# Build and verify the Milestone 1 hardware probe -> build\m1-ps5-probe.elf
.\scripts\build-ps5.ps1 -Package m1-ps5-probe

# Optional debug build
.\scripts\build-ps5.ps1 -Package ps5-agent -Release $false
```

### Linux / macOS (`build-ps5.sh` and `Makefile`)

```bash
# Using the shell script directly (defaults: PACKAGE=ps5-agent, RELEASE=1)
./scripts/build-ps5.sh
./scripts/build-ps5.sh m1-ps5-probe

# Debug build via shell script
RELEASE=0 ./scripts/build-ps5.sh ps5-agent

# Using Makefile shortcuts
make build    # builds build/ps5-agent.elf + build/meta.json
make m1       # builds build/m1-ps5-probe.elf
```

When building `ps5-agent`, both `scripts/build-ps5.ps1` and `scripts/build-ps5.sh` automatically generate `build/meta.json` (`schema: 2`, `filename: "ps5-agent.elf"`, `id: "ps5-agent"`, `version` from `Cargo.toml`, `installed_at`, `last_update`, and lowercase SHA-256 `checksum` of the stripped ELF).

Optional `Makefile` targets: `make deploy`, `make deploy-m1`, `make verify-m1` (accepting `PS5_HOST` and `PS5_PORT`, defaulting to `ps5` and `9021`), and `make clean`.

### `elf-verify` Validation

Both build scripts invoke `elf-verify` automatically after stripping:

```bash
# Main payload (checks ELF64 ET_DYN, 16 KiB PF_X/PF_W separation, DT_NEEDED, and SQLite Stat11 shim symbols)
elf-verify build/ps5-agent.elf --require-sqlite-shim --unstripped target/x86_64-sie-ps5-std/release/ps5-agent

# Milestone 1 probe (does not link SQLite)
elf-verify build/m1-ps5-probe.elf
```

On Windows PowerShell, invoke `& "$env:PS5_PAYLOAD_SDK\bin\elf-verify.exe"` with the same arguments (using `\` paths).

Why `--unstripped` is passed with `--require-sqlite-shim`: `prospero-strip` strips symbol tables from `build/ps5-agent.elf`, so `elf-verify` reads `--unstripped target/x86_64-sie-ps5-std/release/ps5-agent` to confirm `__ps5_stat11` and `__ps5_fstat11` are linked into the binary.

### `ps5-agent.elf` + `meta.json` Checksum Verification & `installed_release_stamp` Upgrades

#### Build-Time & Runtime `meta.json` Checksum Sync

1. **Build-time (`scripts/build-ps5.ps1` / `scripts/build-ps5.sh`)**: When building `ps5-agent`, both scripts hash the stripped `build/ps5-agent.elf` (SHA-256) and emit `build/meta.json` (`schema: 2`, `kind: "repo"`, `filename: "ps5-agent.elf"`, `id: "ps5-agent"`, `version: "v<Cargo.toml version>"`, `installed_at`, `last_update`, `checksum`). You can verify parity manually after a build:
   - **PowerShell**: `(Get-FileHash -Algorithm SHA256 build\ps5-agent.elf).Hash.ToLower()` matches `checksum` in `build\meta.json`.
   - **Linux/macOS**: `sha256sum build/ps5-agent.elf` (or `shasum -a 256 build/ps5-agent.elf`) matches `checksum` in `build/meta.json`.
2. **Startup-time (`src/startup.rs`, `verify_or_sync_meta_json`)**: At boot, `compute_release_stamp()` hashes the running binary (up to 64 MiB), and `verify_or_sync_meta_json()` inspects `<exe_dir>/meta.json` and `<data_dir>/meta.json` when `id == "ps5-agent"` or `filename == "ps5-agent.elf"`. It logs the detected `version` and `checksum`, and when running as `ps5-agent.elf`, automatically updates `checksum`, `version` (`v<CARGO_PKG_VERSION>`), and `last_update` if an in-place `.elf` replacement left stale `meta.json` metadata.

#### `installed_release_stamp` In-Place Release Upgrade Detection

`compute_release_stamp()` in `src/startup.rs` computes a deterministic SHA-256 stamp across:
- `CARGO_PKG_VERSION`
- Database schema version (`CURRENT_SCHEMA_VERSION = 3`)
- Built-in agent schema (`BUILTIN_AGENTS_SCHEMA_VERSION = "3"`) and `builtin_agents_fingerprint()`
- Embedded dashboard assets (`INDEX_HTML`, `STYLES_CSS`, `APP_JS`)
- Running executable SHA-256 (`exe_sha256`)

During `initialize_with_warnings()`, `ps5-agent` compares the stored DB setting `installed_release_stamp` against `current_release_stamp` (and also treats a pre-existing install lacking `installed_release_stamp` as an upgrade). When a new build is detected (`is_elf_upgrade`):
- Persists the updated `installed_release_stamp`, `installed_version`, and `last_upgraded_at` settings and records a `system.elf_upgrade` entry in the audit log (`AuditLogEntry`).
- Reconciles persisted state cleanly in place: runs SQLite migrations (`store.run_migrations()`, e.g., `003_conversation_sort_order` adding `conversations.sort_order`), migrates legacy v1 `config.toml` files (`maybe_upgrade_legacy_v1_config` writes `config.toml.v1.bak`, bumps `installed_config_schema_version = "2"`, and applies `apply_v1_cap_upgrades`), refreshes untouched built-in agent definitions (`agent_manager.seed_defaults()`), ensures `<data_dir>/{skills,plugins,mcp,tools,backups}` exist (`ensure_directories`), and syncs plugin MCP definitions (`sync_plugin_mcp_configs`).
- Before binding sockets, `manage_pid_file()` checks `<data_dir>/ps5-agent.pid` and signals any older live `ps5-agent.elf` / `payload.elf` process (`SIGTERM`, then `SIGKILL` after 600 ms) so re-deploying a freshly built `build/ps5-agent.elf` replaces the running daemon cleanly.

### Host Workspace Build & Tests (`cargo check` & `cargo test --workspace`)

`Cargo.toml` excludes `crates/ps5-sys`, `crates/ps5`, `crates/ps5-std-rt`, and `bins/m1-ps5-probe` from `[workspace]` so all 14 host-compatible crates (`ps5-agent-*`, `mock-ps5lm`, and root `ps5-agent`) compile and test natively.

**Linux / macOS:**

```bash
cargo check --workspace --all-targets
cargo test --workspace
# or
make test
```

**Windows (PowerShell):**

Add `mingw64\bin` (and `%USERPROFILE%\.cargo\bin` if not already global) to `PATH` before running `cargo check` or `cargo test --workspace` so the GNU toolchain (`gcc.exe`, `ar.exe`, `dlltool.exe`) is available when compiling `ring` and `rusqlite` (`sqlite3.c`) for the host:

```powershell
$ToolchainBin = Resolve-Path "..\.ps5-toolchain\mingw64\bin"
$env:PATH = "$ToolchainBin;$env:USERPROFILE\.cargo\bin;$env:PATH"
cargo check --workspace --all-targets
cargo test --workspace
```
