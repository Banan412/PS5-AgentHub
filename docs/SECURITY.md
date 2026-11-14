# PS5 Agent HTTP & Runtime Security Model

This document describes the security controls implemented in `crates/ps5-agent-http` for hosting the PS5 Agent API and web dashboard on a LAN-accessible jailbroken PlayStation 5 or local workstation.

## 1. Threat Model & Assumptions

- **Deployment environment**: `ps5-agent` runs on a local PlayStation 5 (`0.0.0.0:8080`) or host (`127.0.0.1:8080`) inside a trusted home/lab LAN.
- **No direct Internet exposure**: Because the PS5 ELF does not terminate TLS natively, direct exposure to the public Internet is unsupported. `server.allow_lan_only = true` is enabled by default to block non-LAN peers at the application boundary.
- **Sensitive capabilities**: Agents can read/write allowed workspace files, invoke tools, and query local PS5LM inference. All mutating HTTP operations require authenticated identity and CSRF protection.

## 2. Authentication & Credential Storage

### First-Boot Bootstrap (`ensure_bootstrap_credentials`)
- If neither `security.password_hash` in TOML config nor `settings["auth.password_hash"]` in storage is set on startup:
  - Generates a 20-character password using `rand::rngs::OsRng` with unbiased rejection sampling across a 66-character ASCII alphabet.
  - Hashes the password using **Argon2id** (`Algorithm::Argon2id`, `Version::V0x13`, `m_cost = cfg.security.argon2_memory_kib` [8192 KiB on PS5, 19456 KiB on host], `t_cost = 2`, `p_cost = 1`) with a 16-byte CSPRNG salt.
  - Stores only the PHC-formatted Argon2id hash in `settings["auth.password_hash"]`.
  - Writes the plaintext credential file **once** to `<data_dir>/bootstrap_credentials.txt` (created with `0600` file permissions on Unix).
- Passwords, Argon2 hashes, session tokens, and CSRF tokens are **never** written to logs (`AppLogger`).

### Session Cookies & Bearer Tokens
- **Cookie login (`POST /api/v1/auth/login`)**:
  - Compares username and verifies Argon2id password hash (running Argon2id verification even if username mismatches to mitigate timing-based user enumeration).
  - Generates a 256-bit CSPRNG session token (`OsRng`) and stores only its **SHA-256 hex digest** in `StorageBackend::create_session`, alongside a separate 256-bit CSPRNG `csrf_token`, `client_ip`, and `expires_at`.
  - Sets `Set-Cookie: ps5_agent_session=<token>; HttpOnly; SameSite=Strict; Path=/; Max-Age=<ttl>`.
  - Session lookup validates SHA-256 digest via `subtle::ConstantTimeEq`, checks `!session.revoked`, and enforces `expires_at > now` (automatically revoking expired sessions).
- **Bearer API tokens**:
  - `Authorization: Bearer <token>` is hashed with SHA-256 and compared against `security.api_token_sha256` (or `settings["auth.api_token_sha256"]`) via `subtle::ConstantTimeEq`.
- **Password rotation (`POST /api/v1/auth/password`)**:
  - Requires active authentication + CSRF verification, verifies `current_password`, validates `new_password` length (`8..=256` characters without truncation or arbitrary composition rules), and persists a fresh Argon2id hash.

## 3. Cross-Site Request Forgery (CSRF) & Origin Validation

- Every mutating request (`POST`, `PUT`, `PATCH`, `DELETE`) authenticated via session cookie must provide `X-CSRF-Token` matching the session's `csrf_token` via `subtle::ConstantTimeEq` (`403 Forbidden` otherwise).
- Whenever `Origin` (or `Referer` if `Origin` is absent) is present on a mutating request or `/api/v1/auth/login`, the server parses the URL, rejects `"null"` or non-HTTP(S) schemes, and verifies host/port equality against the request `Host` header (`403 Forbidden` on mismatch).

## 4. Network, Abuse & Payload Controls

- **LAN-Only Filtering (`server.allow_lan_only`)**:
  - Extracts peer `SocketAddr` via `ConnectInfo<SocketAddr>` (fail-closed `403 Forbidden` if peer IP is unavailable).
  - Allows only IPv4 loopback (`127.0.0.0/8`), IPv4 RFC1918 (`10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`), IPv6 loopback (`::1`), and IPv6 ULA (`fc00::/7`). IPv4-mapped IPv6 addresses (`::ffff:x.x.x.x`) are unwrapped and checked against the IPv4 rules.
- **Per-IP Sliding-Window Rate Limiter**:
  - Tracks request timestamps over a 60-second sliding window per client IP with bounded map capacity (`4096` IPs max, stale eviction) to prevent unbounded memory growth on PS5.
  - Enforces `security.login_rate_limit_per_minute` (default `10`) on `POST /api/v1/auth/login` and `security.rate_limit_per_minute` (default `240`) across endpoints.
  - Exceeded quotas return `429 Too Many Requests` with an accurate `Retry-After` header (`1..=60` seconds) computed from the oldest active timestamp in the window.
- **Body Size Enforcement**:
  - Both `tower_http::limit::RequestBodyLimitLayer` and `axum::extract::DefaultBodyLimit` enforce `server.max_body_bytes` (default `1048576` bytes / 1 MiB), returning `413 Payload Too Large`.

## 5. HTTP Security Headers

Applied via outermost middleware to **all** responses (including `401`, `403`, `413`, `429`, and `5xx` errors):

- `Content-Security-Policy`: `default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; font-src 'self'; object-src 'none'; base-uri 'self'; frame-ancestors 'none'; form-action 'self'`
- `X-Content-Type-Options: nosniff`
- `X-Frame-Options: DENY`
- `Referrer-Policy: no-referrer`
- `Cache-Control: no-store` on all `/api/*` routes (`Cache-Control: no-cache` on static dashboard shell)

## 6. Settings Protection & Audit Logging

- `GET /api/v1/settings` redacts any stored key starting with `auth.` or containing `password`, `secret`, `token`, `api_key`, or `private_key`.
- `PUT /api/v1/settings` rejects writes to any sensitive key with `403 Forbidden`.
- Security-relevant events (login success/failure, logout, password change, agent lifecycle mutations, approval decisions, tool toggles/executions, settings updates, DB checkpoint, shutdown) append structured records to `StorageBackend::append_audit_log`.

## 7. PS5 Rest-Mode Listener Resilience

- `serve()` and `RecoveringTcpListener` detect bind failures and `accept()` errors including Sony OS error `163` (socket invalidation after PS5 rest-mode resume), set the `network_degraded` flag (surfaced in `/api/v1/health` and `/api/v1/metrics`), emit `StreamEvent::SystemAlert`, and rebind with exponential backoff (`250ms` to `5s`) until recovered or `CancellationToken` shutdown.
