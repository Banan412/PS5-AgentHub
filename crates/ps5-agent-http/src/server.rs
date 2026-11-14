use crate::{build_router, HttpState};
use axum::serve::{Listener, ListenerExt};
use ps5_agent_core::StreamEvent;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::time::sleep;

const DEFAULT_BACKOFF_INITIAL: Duration = Duration::from_millis(250);
const DEFAULT_BACKOFF_MAX: Duration = Duration::from_secs(5);

pub fn is_sony_rest_mode_error(err: &io::Error) -> bool {
    err.raw_os_error() == Some(163)
}

/// Returns `true` for bind errors caused by invalid config or missing OS privileges
/// where retrying with backoff will never succeed.
pub fn is_permanent_bind_error(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::PermissionDenied
            | io::ErrorKind::InvalidInput
            | io::ErrorKind::Unsupported
    )
}

fn configure_accepted_stream(stream: &TcpStream) {
    // Disable Nagle's algorithm so small HTTP responses and SSE token chunks flush immediately.
    let _ = stream.set_nodelay(true);
}

pub fn should_rebind_on_accept_error(err: &io::Error, consecutive_errors: u32) -> bool {
    if is_sony_rest_mode_error(err) {
        return true;
    }
    if matches!(
        err.kind(),
        io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
    ) {
        return consecutive_errors >= 8;
    }
    // Rebind immediately on network/socket invalidation or after 2 consecutive errors.
    matches!(
        err.kind(),
        io::ErrorKind::NotConnected
            | io::ErrorKind::AddrNotAvailable
            | io::ErrorKind::InvalidInput
            | io::ErrorKind::BrokenPipe
    ) || consecutive_errors >= 2
}

/// Format `host:port` cleanly, bracketing unbracketed IPv6 literals such as `::1` or `::`.
pub fn format_bind_addr(host: &str, port: u16) -> String {
    let trimmed = host.trim();
    if trimmed.contains(':') && !trimmed.starts_with('[') && !trimmed.ends_with(']') {
        format!("[{trimmed}]:{port}")
    } else {
        format!("{trimmed}:{port}")
    }
}

/// Bind a `TcpListener` with `SO_REUSEADDR` (and `SO_REUSEPORT` on Unix) so rapid
/// restarts can immediately rebind the port even when prior connections are in `TIME_WAIT`.
pub async fn bind_reusable_listener(bind_addr: &str) -> io::Result<TcpListener> {
    let addrs: Vec<SocketAddr> = match bind_addr.parse::<SocketAddr>() {
        Ok(addr) => vec![addr],
        Err(_) => tokio::net::lookup_host(bind_addr).await?.collect(),
    };
    if addrs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            format!("Could not resolve bind address: {bind_addr}"),
        ));
    }

    let mut last_err: Option<io::Error> = None;
    let mut record_err = |err: io::Error| {
        // Never overwrite a transient boot/port error (e.g. AddrNotAvailable) with a sibling
        // permanent address-family error (e.g. IPv6 Unsupported) when binding multi-addr hosts.
        let replace = match &last_err {
            None => true,
            Some(prev) => is_permanent_bind_error(prev) && !is_permanent_bind_error(&err),
        };
        if replace {
            last_err = Some(err);
        }
    };
    for addr in addrs {
        let socket = match addr {
            SocketAddr::V4(_) => TcpSocket::new_v4(),
            SocketAddr::V6(_) => TcpSocket::new_v6(),
        };
        let socket = match socket {
            Ok(s) => s,
            Err(e) => {
                record_err(e);
                continue;
            }
        };
        let _ = socket.set_reuseaddr(true);
        #[cfg(unix)]
        {
            let _ = socket.set_reuseport(true);
        }
        if let Err(e) = socket.bind(addr) {
            record_err(e);
            continue;
        }
        match socket.listen(1024) {
            Ok(listener) => return Ok(listener),
            Err(e) => record_err(e),
        }
    }

    Err(last_err.unwrap_or_else(|| {
        io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            format!("Could not bind resolved address: {bind_addr}"),
        )
    }))
}

async fn bind_with_recovery_result(
    bind_addr: &str,
    state: &Arc<HttpState>,
    initial_backoff: Duration,
    max_backoff: Duration,
) -> io::Result<Option<TcpListener>> {
    let mut backoff = initial_backoff.max(Duration::from_millis(1));
    let max_backoff = max_backoff.max(backoff);
    let mut attempt = 0u64;
    loop {
        if state.shutdown.is_cancelled() {
            return Ok(None);
        }
        match bind_reusable_listener(bind_addr).await {
            Ok(listener) => {
                if state.is_network_degraded() {
                    state.set_network_degraded(false);
                    state.logger.info(
                        "http.server",
                        format!("HTTP listener bound/recovered on {bind_addr}"),
                    );
                    let _ = state.events_tx.send(StreamEvent::SystemAlert {
                        level: "info".to_string(),
                        message: format!("HTTP listener recovered on {bind_addr}"),
                    });
                }
                return Ok(Some(listener));
            }
            Err(err) => {
                if is_permanent_bind_error(&err) {
                    state.set_network_degraded(true);
                    state.logger.error(
                        "http.server",
                        format!(
                            "Permanent bind error on {bind_addr} (kind={}): {err}; aborting listener startup",
                            err.kind()
                        ),
                    );
                    let _ = state.events_tx.send(StreamEvent::SystemAlert {
                        level: "error".to_string(),
                        message: format!("HTTP listener failed permanently ({bind_addr}): {err}"),
                    });
                    return Err(err);
                }

                attempt = attempt.saturating_add(1);
                let was_degraded = state.set_network_degraded(true);
                if !was_degraded {
                    state.logger.warn(
                        "http.server",
                        format!(
                            "Transient bind error on {bind_addr} (kind={}): {err}; retrying with backoff",
                            err.kind()
                        ),
                    );
                    let _ = state.events_tx.send(StreamEvent::SystemAlert {
                        level: "warn".to_string(),
                        message: format!("HTTP listener waiting to bind ({bind_addr}): {err}"),
                    });
                } else if attempt <= 5 || attempt % 12 == 0 {
                    state.logger.warn(
                        "http.server",
                        format!(
                            "Retry #{attempt} bind {bind_addr} failed (kind={}): {err}; next retry in {}ms",
                            err.kind(),
                            backoff.as_millis()
                        ),
                    );
                }

                tokio::select! {
                    _ = state.shutdown.cancelled() => return Ok(None),
                    _ = sleep(backoff) => {}
                }
                backoff = (backoff.saturating_mul(2)).min(max_backoff);
            }
        }
    }
}

/// Bind `TcpListener` with exponential backoff until bound, `shutdown.is_cancelled()`,
/// or a permanent configuration/permission error occurs.
pub async fn bind_with_recovery(
    bind_addr: &str,
    state: &Arc<HttpState>,
    initial_backoff: Duration,
    max_backoff: Duration,
) -> Option<TcpListener> {
    bind_with_recovery_result(bind_addr, state, initial_backoff, max_backoff)
        .await
        .ok()
        .flatten()
}

/// `axum::serve::Listener` wrapper that survives PS5 rest-mode socket invalidation
/// (such as Sony OS error `163`), sets `network_degraded`, and rebinds with backoff.
pub struct RecoveringTcpListener {
    bind_addr: String,
    listener: Option<TcpListener>,
    local_addr: SocketAddr,
    state: Arc<HttpState>,
    initial_backoff: Duration,
    max_backoff: Duration,
}

impl RecoveringTcpListener {
    pub fn new(bind_addr: String, listener: TcpListener, state: Arc<HttpState>) -> io::Result<Self> {
        let local_addr = listener.local_addr()?;
        Ok(Self {
            bind_addr,
            listener: Some(listener),
            local_addr,
            state,
            initial_backoff: DEFAULT_BACKOFF_INITIAL,
            max_backoff: DEFAULT_BACKOFF_MAX,
        })
    }

    pub fn with_backoff(mut self, initial: Duration, max: Duration) -> Self {
        self.initial_backoff = initial;
        self.max_backoff = max;
        self
    }

    async fn try_rebind(&mut self) {
        match bind_with_recovery_result(
            &self.bind_addr,
            &self.state,
            self.initial_backoff,
            self.max_backoff,
        )
        .await
        {
            Ok(Some(new_listener)) => {
                if let Ok(addr) = new_listener.local_addr() {
                    self.local_addr = addr;
                }
                self.listener = Some(new_listener);
            }
            Ok(None) => {}
            Err(_) => {
                // Permanent bind errors cannot recover; cancel shutdown so axum::serve exits cleanly.
                self.state.shutdown.cancel();
            }
        }
    }

    pub async fn handle_accept_result(
        &mut self,
        res: io::Result<(TcpStream, SocketAddr)>,
        consecutive_errors: &mut u32,
    ) -> Option<(TcpStream, SocketAddr)> {
        match res {
            Ok((stream, addr)) => {
                configure_accepted_stream(&stream);
                *consecutive_errors = 0;
                if self.state.is_network_degraded() {
                    self.state.set_network_degraded(false);
                    self.state.logger.info(
                        "http.server",
                        "HTTP accept loop recovered from degraded network state",
                    );
                    let _ = self.state.events_tx.send(StreamEvent::SystemAlert {
                        level: "info".to_string(),
                        message: "HTTP listener network recovered".to_string(),
                    });
                }
                Some((stream, addr))
            }
            Err(err) => {
                *consecutive_errors += 1;
                if is_sony_rest_mode_error(&err) {
                    self.state.logger.warn(
                        "http.server",
                        "Detected PS5 rest-mode socket error 163 on accept(); rebinding listener",
                    );
                } else {
                    self.state.logger.warn(
                        "http.server",
                        format!("HTTP accept() error (count={}): {err}", *consecutive_errors),
                    );
                }

                if should_rebind_on_accept_error(&err, *consecutive_errors) {
                    let was_degraded = self.state.set_network_degraded(true);
                    if !was_degraded {
                        let _ = self.state.events_tx.send(StreamEvent::SystemAlert {
                            level: "warn".to_string(),
                            message: format!("HTTP listener degraded ({err}); rebinding"),
                        });
                    }
                    self.listener = None;
                    *consecutive_errors = 0;
                    self.try_rebind().await;
                } else {
                    tokio::select! {
                        _ = self.state.shutdown.cancelled() => {}
                        _ = sleep(Duration::from_millis(100)) => {}
                    }
                }
                None
            }
        }
    }
}

impl Listener for RecoveringTcpListener {
    type Io = TcpStream;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        let mut consecutive_errors = 0u32;
        loop {
            if self.listener.is_none() {
                self.try_rebind().await;
                if self.listener.is_none() {
                    // Shutdown was cancelled (or permanent bind error triggered shutdown); park until axum exits.
                    self.state.shutdown.cancelled().await;
                    std::future::pending::<()>().await;
                }
            }

            let listener = self.listener.as_ref().expect("listener bound above");
            let res = tokio::select! {
                _ = self.state.shutdown.cancelled() => {
                    std::future::pending::<()>().await;
                    unreachable!()
                }
                accepted = listener.accept() => accepted,
            };

            if let Some(pair) = self
                .handle_accept_result(res, &mut consecutive_errors)
                .await
            {
                return pair;
            }
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        Ok(self.local_addr)
    }
}

/// Serves the PS5 Agent HTTP server on `config.server.bind_host:config.server.port`
/// with automatic rest-mode listener recovery until `state.shutdown` is cancelled
/// or a permanent bind error occurs.
pub async fn serve(state: Arc<HttpState>) -> io::Result<()> {
    let cfg = state.effective_config();
    let bind_addr = format_bind_addr(&cfg.server.bind_host, cfg.server.port);
    let Some(listener) = bind_with_recovery_result(
        &bind_addr,
        &state,
        DEFAULT_BACKOFF_INITIAL,
        DEFAULT_BACKOFF_MAX,
    )
    .await?
    else {
        return Ok(());
    };

    serve_with_listener(bind_addr, listener, state).await
}

/// Serves on a pre-bound `TcpListener` (useful for tests binding port `0`) while retaining
/// rest-mode recovery and graceful shutdown.
pub async fn serve_with_listener(
    bind_addr: String,
    listener: TcpListener,
    state: Arc<HttpState>,
) -> io::Result<()> {
    let local = listener.local_addr()?;
    state
        .logger
        .info("http.server", format!("HTTP server listening on http://{local}"));

    let recovering = RecoveringTcpListener::new(bind_addr, listener, state.clone())?
        .tap_io(|stream| {
            configure_accepted_stream(stream);
        });
    let router = build_router(state.clone());
    let shutdown = state.shutdown.clone();

    axum::serve(
        recovering,
        router.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        shutdown.cancelled().await;
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permanent_vs_transient_bind_error_classification() {
        for kind in [
            io::ErrorKind::PermissionDenied,
            io::ErrorKind::InvalidInput,
            io::ErrorKind::Unsupported,
        ] {
            let err = io::Error::new(kind, "permanent");
            assert!(is_permanent_bind_error(&err), "expected permanent for {kind:?}");
        }

        for kind in [
            io::ErrorKind::AddrNotAvailable,
            io::ErrorKind::AddrInUse,
            io::ErrorKind::WouldBlock,
            io::ErrorKind::TimedOut,
            io::ErrorKind::Interrupted,
        ] {
            let err = io::Error::new(kind, "transient");
            assert!(!is_permanent_bind_error(&err), "expected transient for {kind:?}");
        }

        let sony_163 = io::Error::from_raw_os_error(163);
        assert!(!is_permanent_bind_error(&sony_163));
    }

    #[tokio::test]
    async fn bind_reusable_listener_distinguishes_invalid_and_unassigned_addrs() {
        let invalid = bind_reusable_listener("invalid-without-port")
            .await
            .expect_err("malformed address should fail");
        assert_eq!(invalid.kind(), io::ErrorKind::InvalidInput);
        assert!(is_permanent_bind_error(&invalid));

        // 192.0.2.1 (TEST-NET-1) simulates early PS5 boot before LAN interface IP assignment.
        let unavailable = bind_reusable_listener("192.0.2.1:0")
            .await
            .expect_err("unassigned local IP should fail transiently");
        assert!(!is_permanent_bind_error(&unavailable));

        let ok = bind_reusable_listener("127.0.0.1:0")
            .await
            .expect("loopback ephemeral bind should succeed");
        assert!(ok.local_addr().unwrap().port() > 0);
    }

    #[tokio::test]
    async fn configure_accepted_stream_enables_tcp_nodelay() {
        let listener = bind_reusable_listener("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let connect_fut = TcpStream::connect(addr);
        let accept_fut = listener.accept();
        let (client_res, accept_res) = tokio::join!(connect_fut, accept_fut);
        let _client = client_res.unwrap();
        let (server_stream, _) = accept_res.unwrap();

        configure_accepted_stream(&server_stream);
        assert!(server_stream.nodelay().unwrap());
    }
}
