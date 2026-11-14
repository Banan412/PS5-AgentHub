use crate::error::{HttpError, HttpResult};
use crate::HttpState;
use argon2::{
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Algorithm, Argon2, Params, Version,
};
use axum::extract::{ConnectInfo, State};
use axum::http::{header, HeaderMap, HeaderValue};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use chrono::{DateTime, Duration, Utc};
use ps5_agent_config::AppConfig;
use ps5_agent_core::{new_id, now_iso, AgentError, AuditLogEntry, Result, SessionRecord, StorageBackend};
use ps5_agent_logging::AppLogger;
use rand::rngs::OsRng;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::net::SocketAddr;
use std::sync::Arc;
use subtle::ConstantTimeEq;

pub const SESSION_COOKIE_NAME: &str = "ps5_agent_session";

const PASSWORD_ALPHABET: &[u8] =
    b"ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789";

#[derive(Debug, Clone)]
pub enum AuthMethod {
    Cookie(SessionRecord),
    Bearer,
    Disabled,
}

#[derive(Debug, Clone)]
pub struct AuthContext {
    pub username: String,
    pub method: AuthMethod,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct LoginResponse {
    pub username: String,
    pub csrf_token: String,
    pub expires_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SessionInfoResponse {
    pub authenticated: bool,
    pub username: String,
    pub auth_method: &'static str,
    pub csrf_token: Option<String>,
    pub expires_at: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChangePasswordRequest {
    pub current_password: String,
    pub new_password: String,
}

pub fn sha256_hex(input: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input);
    hex::encode(hasher.finalize())
}

pub fn random_hex_256() -> String {
    let mut bytes = [0u8; 32];
    OsRng.fill_bytes(&mut bytes);
    hex::encode(bytes)
}

pub fn generate_random_password(len: usize) -> String {
    let mut out = String::with_capacity(len);
    let n = PASSWORD_ALPHABET.len();
    // Rejection sampling avoids modulo bias across the alphabet.
    let limit = (256 / n) * n;
    let mut buf = [0u8; 1];
    while out.len() < len {
        OsRng.fill_bytes(&mut buf);
        let b = buf[0] as usize;
        if b < limit {
            out.push(PASSWORD_ALPHABET[b % n] as char);
        }
    }
    out
}

pub fn constant_time_eq(a: &str, b: &str) -> bool {
    let a_bytes = a.as_bytes();
    let b_bytes = b.as_bytes();
    if a_bytes.len() != b_bytes.len() {
        return false;
    }
    bool::from(a_bytes.ct_eq(b_bytes))
}

pub fn hash_password(password: &str, argon2_memory_kib: u32) -> Result<String> {
    let params = Params::new(argon2_memory_kib.max(1024), 2, 1, None)
        .map_err(|e| AgentError::Internal(format!("argon2 params error: {e}")))?;
    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
    let mut salt_bytes = [0u8; 16];
    OsRng.fill_bytes(&mut salt_bytes);
    let salt = SaltString::encode_b64(&salt_bytes)
        .map_err(|e| AgentError::Internal(format!("argon2 salt error: {e}")))?;
    argon2
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| AgentError::Internal(format!("argon2 hash error: {e}")))
}

pub fn verify_password(password: &str, phc_hash: &str) -> bool {
    let Ok(parsed) = PasswordHash::new(phc_hash) else {
        return false;
    };
    Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok()
}

pub fn resolve_password_hash(cfg: &AppConfig, store: &dyn StorageBackend) -> Result<Option<String>> {
    if let Some(db_hash) = store.get_setting("auth.password_hash")? {
        let trimmed = db_hash.trim();
        if !trimmed.is_empty() && trimmed.starts_with("$argon2") {
            return Ok(Some(trimmed.to_string()));
        }
    }
    Ok(cfg
        .security
        .password_hash
        .as_ref()
        .map(|h| h.trim())
        .filter(|h| !h.is_empty() && h.starts_with("$argon2"))
        .map(str::to_string))
}

pub fn read_bootstrap_password_file(data_dir: &std::path::Path) -> Option<String> {
    let path = data_dir.join("bootstrap_credentials.txt");
    let raw = fs::read_to_string(path).ok()?;
    for line in raw.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("password=") {
            let pw = rest.trim();
            if !pw.is_empty() {
                return Some(pw.to_string());
            }
        }
    }
    None
}

pub fn write_bootstrap_credentials_file(
    cfg: &AppConfig,
    password: &str,
) -> Result<std::path::PathBuf> {
    fs::create_dir_all(&cfg.data_dir)
        .map_err(|e| AgentError::Storage(format!("creating data_dir: {e}")))?;
    let creds_path = cfg.data_dir.join("bootstrap_credentials.txt");

    let mut opts = OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }

    let mut file = opts
        .open(&creds_path)
        .map_err(|e| AgentError::Storage(format!("opening {}: {e}", creds_path.display())))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(&creds_path, fs::Permissions::from_mode(0o600));
    }

    let content = format!(
        "# PS5 Agent Initial Bootstrap Credentials\n\
         # Delete this file after logging in and rotating the admin password.\n\
         username={}\n\
         password={}\n",
        cfg.security.admin_username.trim(),
        password
    );
    file.write_all(content.as_bytes())
        .map_err(|e| AgentError::Storage(format!("writing {}: {e}", creds_path.display())))?;
    let _ = file.sync_all();
    Ok(creds_path)
}

pub fn resolve_api_token_sha256(
    cfg: &AppConfig,
    store: &dyn StorageBackend,
) -> Result<Option<String>> {
    if let Some(cfg_tok) = cfg
        .security
        .api_token_sha256
        .as_ref()
        .filter(|t| !t.trim().is_empty())
    {
        return Ok(Some(cfg_tok.trim().to_ascii_lowercase()));
    }
    if let Some(db_tok) = store.get_setting("auth.api_token_sha256")? {
        if !db_tok.trim().is_empty() {
            return Ok(Some(db_tok.trim().to_ascii_lowercase()));
        }
    }
    Ok(None)
}

/// Ensures an Argon2id admin password hash exists in config or storage on first boot.
/// Returns `Some(password)` only when a brand-new random bootstrap password was generated.
pub fn ensure_bootstrap_credentials_with_report(
    cfg: &AppConfig,
    store: &dyn StorageBackend,
    logger: &AppLogger,
) -> Result<Option<String>> {
    if let Some(plain) = cfg
        .security
        .admin_password
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        let hash = hash_password(plain, cfg.security.argon2_memory_kib)?;
        store.set_setting("auth.password_hash", &hash)?;
        let creds_path = write_bootstrap_credentials_file(cfg, plain)?;
        let _ = store.checkpoint();
        logger.info(
            "auth",
            format!(
                "Synchronized admin password from config (bootstrap file: {})",
                creds_path.display()
            ),
        );
        return Ok(None);
    }

    if let Some(plain) = cfg
        .security
        .password_hash
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty() && !s.starts_with("$argon2"))
    {
        let hash = hash_password(plain, cfg.security.argon2_memory_kib)?;
        store.set_setting("auth.password_hash", &hash)?;
        let _ = store.checkpoint();
        logger.warn(
            "auth",
            "Detected plaintext password in security.password_hash; hashed and stored in database settings (prefer security.admin_password for plaintext config)",
        );
        return Ok(None);
    }

    if resolve_password_hash(cfg, store)?.is_some() {
        return Ok(None);
    }

    if let Some(existing_pw) = read_bootstrap_password_file(&cfg.data_dir) {
        let hash = hash_password(&existing_pw, cfg.security.argon2_memory_kib)?;
        store.set_setting("auth.password_hash", &hash)?;
        let _ = store.checkpoint();
        logger.info(
            "auth",
            "Restored admin password hash from existing bootstrap_credentials.txt",
        );
        return Ok(None);
    }

    let password = generate_random_password(20);
    let hash = hash_password(&password, cfg.security.argon2_memory_kib)?;
    store.set_setting("auth.password_hash", &hash)?;
    let creds_path = write_bootstrap_credentials_file(cfg, &password)?;
    let _ = store.checkpoint();

    logger.info(
        "auth",
        format!(
            "Generated initial bootstrap credentials at {}",
            creds_path.display()
        ),
    );
    Ok(Some(password))
}

/// Ensures an Argon2id admin password hash exists in config or storage on first boot.
/// When absent, generates a 20-char random password, stores only its Argon2id hash in
/// `settings["auth.password_hash"]`, and writes the plaintext password once to
/// `<data_dir>/bootstrap_credentials.txt` (mode 0600 on Unix).
pub fn ensure_bootstrap_credentials(
    cfg: &AppConfig,
    store: &dyn StorageBackend,
    logger: &AppLogger,
) -> Result<()> {
    let _ = ensure_bootstrap_credentials_with_report(cfg, store, logger)?;
    Ok(())
}

pub fn extract_cookie_value<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    for raw in headers.get_all(header::COOKIE) {
        let Ok(cookie_str) = raw.to_str() else {
            continue;
        };
        for pair in cookie_str.split(';') {
            let trimmed = pair.trim();
            if let Some((k, v)) = trimmed.split_once('=') {
                if k.trim() == name {
                    let val = v.trim().trim_matches('"');
                    if !val.is_empty() && val.len() <= 512 {
                        return Some(val);
                    }
                }
            }
        }
    }
    None
}

pub fn extract_bearer_token(headers: &HeaderMap) -> Option<&str> {
    let raw = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    if raw.len() > 4096 {
        return None;
    }
    let (scheme, token) = raw.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = token.trim();
    if token.is_empty() {
        None
    } else {
        Some(token)
    }
}

pub fn authenticate_request(state: &HttpState, headers: &HeaderMap) -> HttpResult<AuthContext> {
    let cfg = state.effective_config();

    if !cfg.security.auth_enabled {
        return Ok(AuthContext {
            username: cfg.security.admin_username.clone(),
            method: AuthMethod::Disabled,
        });
    }

    if let Some(bearer) = extract_bearer_token(headers) {
        let expected_hash = resolve_api_token_sha256(&cfg, state.store.as_ref())?
            .ok_or(HttpError::Unauthorized("Invalid credentials"))?;
        let candidate_hash = sha256_hex(bearer.as_bytes());
        if constant_time_eq(&candidate_hash, &expected_hash) {
            return Ok(AuthContext {
                username: cfg.security.admin_username.clone(),
                method: AuthMethod::Bearer,
            });
        }
        return Err(HttpError::Unauthorized("Invalid bearer token"));
    }

    if let Some(cookie_token) = extract_cookie_value(headers, SESSION_COOKIE_NAME) {
        let token_hash = sha256_hex(cookie_token.as_bytes());
        let Some(session) = state.store.get_session_by_hash(&token_hash)? else {
            return Err(HttpError::Unauthorized("Invalid or expired session"));
        };
        if session.revoked || !constant_time_eq(&session.token_sha256, &token_hash) {
            return Err(HttpError::Unauthorized("Invalid or expired session"));
        }
        let Ok(expires_at) = DateTime::parse_from_rfc3339(&session.expires_at) else {
            return Err(HttpError::Unauthorized("Invalid or expired session"));
        };
        if expires_at.with_timezone(&Utc) <= Utc::now() {
            let _ = state.store.revoke_session(&session.id);
            return Err(HttpError::Unauthorized("Session expired"));
        }
        if let Some(revoked_before) = state.store.get_setting("auth.sessions_revoked_before")? {
            if let Ok(revoked_dt) = DateTime::parse_from_rfc3339(&revoked_before) {
                let keep_id = state
                    .store
                    .get_setting("auth.sessions_revoked_keep_id")?
                    .unwrap_or_default();
                let is_kept = !keep_id.is_empty() && keep_id == session.id;
                if !is_kept {
                    let Ok(created_at) = DateTime::parse_from_rfc3339(&session.created_at) else {
                        return Err(HttpError::Unauthorized("Invalid or expired session"));
                    };
                    if created_at.with_timezone(&Utc) <= revoked_dt.with_timezone(&Utc) {
                        let _ = state.store.revoke_session(&session.id);
                        return Err(HttpError::Unauthorized("Session revoked"));
                    }
                }
            }
        }
        return Ok(AuthContext {
            username: session.username.clone(),
            method: AuthMethod::Cookie(session),
        });
    }

    Err(HttpError::Unauthorized("Authentication required"))
}

pub async fn login_handler(
    State(state): State<Arc<HttpState>>,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
    Json(req): Json<LoginRequest>,
) -> HttpResult<Response> {
    let username = req.username.trim();
    if username.is_empty()
        || username.len() > 128
        || req.password.is_empty()
        || req.password.len() > 256
    {
        return Err(HttpError::BadRequest(
            "Invalid username or password format".to_string(),
        ));
    }

    let client_ip = peer
        .map(|Extension(ConnectInfo(addr))| addr.ip().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    let cfg = state.effective_config();
    let stored_hash = resolve_password_hash(&cfg, state.store.as_ref())?;
    let expected_user = cfg.security.admin_username.trim();
    let user_matches = !expected_user.is_empty() && username.eq_ignore_ascii_case(expected_user);

    let trimmed_password = req.password.trim();
    let mut password_verified = match stored_hash {
        Some(ref hash) => {
            // Always run Argon2id verification even when username mismatches to reduce timing leaks.
            verify_password(&req.password, hash)
                || (trimmed_password != req.password && verify_password(trimmed_password, hash))
        }
        None => false,
    };

    let password_rotated =
        state.store.get_setting("auth.password_rotated")?.as_deref() == Some("true");

    if !password_verified && user_matches && !trimmed_password.is_empty() && !password_rotated {
        let config_plain = cfg
            .security
            .admin_password
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .or_else(|| {
                cfg.security
                    .password_hash
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty() && !s.starts_with("$argon2"))
            });
        if let Some(plain) = config_plain {
            if constant_time_eq(trimmed_password, plain) {
                let new_hash = hash_password(plain, cfg.security.argon2_memory_kib)?;
                state.store.set_setting("auth.password_hash", &new_hash)?;
                let _ = state.store.checkpoint();
                password_verified = true;
            }
        }
    }

    if !password_verified && user_matches && !trimmed_password.is_empty() && !password_rotated {
        if let Some(file_pw) = read_bootstrap_password_file(&cfg.data_dir) {
            if constant_time_eq(trimmed_password, &file_pw) {
                let new_hash = hash_password(&file_pw, cfg.security.argon2_memory_kib)?;
                state.store.set_setting("auth.password_hash", &new_hash)?;
                let _ = state.store.checkpoint();
                state.logger.info(
                    "auth",
                    "Synchronized admin password hash from bootstrap_credentials.txt",
                );
                password_verified = true;
            }
        }
    }

    let password_ok = user_matches && password_verified;

    if !password_ok {
        let _ = state.store.append_audit_log(&AuditLogEntry {
            id: new_id(),
            timestamp: now_iso(),
            actor: username.to_string(),
            action: "auth.login".to_string(),
            target: "session".to_string(),
            severity: "warn".to_string(),
            outcome: "failure".to_string(),
            details_json: serde_json::json!({ "client_ip": client_ip }).to_string(),
        });
        state.logger.warn(
            "auth",
            format!("Failed login attempt from {client_ip}"),
        );
        return Err(HttpError::Unauthorized("Invalid username or password"));
    }

    let session_token = random_hex_256();
    let token_sha256 = sha256_hex(session_token.as_bytes());
    let csrf_token = random_hex_256();
    let ttl_hours = cfg.security.session_ttl_hours.clamp(1, 87_600);
    let ttl_secs = ttl_hours.checked_mul(3600).unwrap_or(87_600 * 3600);
    let mut now = Utc::now();
    if let Some(revoked_str) = state.store.get_setting("auth.sessions_revoked_before")? {
        if let Ok(revoked_dt) = DateTime::parse_from_rfc3339(&revoked_str) {
            let revoked_utc = revoked_dt.with_timezone(&Utc);
            if now <= revoked_utc {
                now = revoked_utc + Duration::microseconds(1);
            }
        }
    }
    let ttl_delta =
        Duration::try_hours(ttl_hours as i64).unwrap_or_else(|| Duration::days(3650));
    let expires_at = now
        .checked_add_signed(ttl_delta)
        .unwrap_or_else(|| now + Duration::days(3650))
        .to_rfc3339();

    let session = SessionRecord {
        id: new_id(),
        token_sha256,
        csrf_token: csrf_token.clone(),
        username: expected_user.to_string(),
        client_ip: client_ip.clone(),
        created_at: now.to_rfc3339(),
        expires_at: expires_at.clone(),
        revoked: false,
    };

    state.store.create_session(&session)?;
    let _ = state.store.purge_expired_sessions(&now_iso());

    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: session.username.clone(),
        action: "auth.login".to_string(),
        target: session.id.clone(),
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: serde_json::json!({ "client_ip": client_ip }).to_string(),
    });
    state
        .logger
        .info("auth", format!("Session created for {} from {client_ip}", session.username));

    let cookie_val = format!(
        "{SESSION_COOKIE_NAME}={session_token}; HttpOnly; SameSite=Strict; Path=/; Max-Age={ttl_secs}"
    );
    let mut resp = Json(LoginResponse {
        username: session.username,
        csrf_token,
        expires_at,
    })
    .into_response();
    if let Ok(hv) = HeaderValue::from_str(&cookie_val) {
        resp.headers_mut().insert(header::SET_COOKIE, hv);
    }
    Ok(resp)
}

pub async fn logout_handler(
    State(state): State<Arc<HttpState>>,
    Extension(auth): Extension<AuthContext>,
) -> HttpResult<Response> {
    if let AuthMethod::Cookie(ref sess) = auth.method {
        state.store.revoke_session(&sess.id)?;
        let _ = state.store.append_audit_log(&AuditLogEntry {
            id: new_id(),
            timestamp: now_iso(),
            actor: auth.username,
            action: "auth.logout".to_string(),
            target: sess.id.clone(),
            severity: "info".to_string(),
            outcome: "success".to_string(),
            details_json: "{}".to_string(),
        });
    }

    let clear_cookie =
        format!("{SESSION_COOKIE_NAME}=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0");
    let mut resp = Json(serde_json::json!({ "logged_out": true })).into_response();
    if let Ok(hv) = HeaderValue::from_str(&clear_cookie) {
        resp.headers_mut().insert(header::SET_COOKIE, hv);
    }
    Ok(resp)
}

pub async fn session_handler(
    Extension(auth): Extension<AuthContext>,
) -> HttpResult<Json<SessionInfoResponse>> {
    let (auth_method, csrf_token, expires_at) = match auth.method {
        AuthMethod::Cookie(sess) => ("cookie", Some(sess.csrf_token), Some(sess.expires_at)),
        AuthMethod::Bearer => ("bearer", None, None),
        AuthMethod::Disabled => ("disabled", None, None),
    };
    Ok(Json(SessionInfoResponse {
        authenticated: true,
        username: auth.username,
        auth_method,
        csrf_token,
        expires_at,
    }))
}

pub async fn change_password_handler(
    State(state): State<Arc<HttpState>>,
    Extension(auth): Extension<AuthContext>,
    Json(req): Json<ChangePasswordRequest>,
) -> HttpResult<Json<serde_json::Value>> {
    if req.new_password.len() < 8 || req.new_password.len() > 256 {
        return Err(HttpError::BadRequest(
            "New password must be between 8 and 256 characters".to_string(),
        ));
    }

    let cfg = state.effective_config();
    let current_hash = resolve_password_hash(&cfg, state.store.as_ref())?
        .ok_or(HttpError::Unauthorized("No password configured"))?;

    let trimmed_current = req.current_password.trim();
    let current_ok = verify_password(&req.current_password, &current_hash)
        || (trimmed_current != req.current_password
            && verify_password(trimmed_current, &current_hash));

    if !current_ok {
        let _ = state.store.append_audit_log(&AuditLogEntry {
            id: new_id(),
            timestamp: now_iso(),
            actor: auth.username,
            action: "auth.password_change".to_string(),
            target: "admin_password".to_string(),
            severity: "warn".to_string(),
            outcome: "failure".to_string(),
            details_json: "{}".to_string(),
        });
        return Err(HttpError::Unauthorized("Current password is incorrect"));
    }

    let new_hash = hash_password(&req.new_password, cfg.security.argon2_memory_kib)?;
    state.store.set_setting("auth.password_hash", &new_hash)?;
    state.store.set_setting("auth.password_rotated", "true")?;

    let mut revoked_cutoff = Utc::now();
    if let Some(prev) = state.store.get_setting("auth.sessions_revoked_before")? {
        if let Ok(prev_dt) = DateTime::parse_from_rfc3339(&prev) {
            let prev_utc = prev_dt.with_timezone(&Utc);
            if revoked_cutoff <= prev_utc {
                revoked_cutoff = prev_utc + Duration::microseconds(1);
            }
        }
    }
    let keep_id = match &auth.method {
        AuthMethod::Cookie(session) => session.id.as_str(),
        AuthMethod::Bearer | AuthMethod::Disabled => "",
    };
    state
        .store
        .set_setting("auth.sessions_revoked_before", &revoked_cutoff.to_rfc3339())?;
    state
        .store
        .set_setting("auth.sessions_revoked_keep_id", keep_id)?;
    let _ = state.store.checkpoint();

    let bootstrap_file = cfg.data_dir.join("bootstrap_credentials.txt");
    if bootstrap_file.exists() {
        let _ = fs::remove_file(&bootstrap_file);
    }

    let _ = state.store.append_audit_log(&AuditLogEntry {
        id: new_id(),
        timestamp: now_iso(),
        actor: auth.username,
        action: "auth.password_change".to_string(),
        target: "admin_password".to_string(),
        severity: "info".to_string(),
        outcome: "success".to_string(),
        details_json: "{}".to_string(),
    });
    state.logger.info("auth", "Admin password updated");

    Ok(Json(serde_json::json!({ "updated": true })))
}
