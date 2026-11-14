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

    let mut last_err = None;
    for addr in addrs {
        let socket = match addr {
            SocketAddr::V4(_) => TcpSocket::new_v4(),
            SocketAddr::V6(_) => TcpSocket::new_v6(),
        };
        let socket = match socket {
            Ok(s) => s,
            Err(e) => {
                last_err = Some(e);
                continue;
            }
        };
        let _ = socket.set_reuseaddr(true);
        #[cfg(unix)]
        {
            let _ = socket.set_reuseport(true);
        }
        if let Err(e) = socket.bind(addr) {
            last_err = Some(e);
            continue;
        }
        match socket.listen(1024) {
            Ok(listener) => return Ok(listener),
            Err(e) => last_err = Some(e),
        }
    }

    Err(last_err.unwrap_or_else(|| {
        io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            format!("Could not bind resolved address: {bind_addr}"),
        )
    }))
}

/// Bind `TcpListener` with exponential backoff until bound or `shutdown.is_cancelled()`.
pub async fn bind_with_recovery(
    bind_addr: &str,
    state: &Arc<HttpState>,
    initial_backoff: Duration,
    max_backoff: Duration,
) -> Option<TcpListener> {
    let mut backoff = initial_backoff;
    loop {
        if state.shutdown.is_cancelled() {
            return None;
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
                return Some(listener);
            }
            Err(err) => {
                let was_degraded = state.set_network_degraded(true);
                if !was_degraded {
                    state.logger.error(
                        "http.server",
                        format!("Failed to bind {bind_addr}: {err}; entering degraded retry loop"),
                    );
                    let _ = state.events_tx.send(StreamEvent::SystemAlert {
                        level: "error".to_string(),
                        message: format!("HTTP listener degraded ({bind_addr}): {err}"),
                    });
                } else {
                    state
                        .logger
                        .warn("http.server", format!("Retry bind {bind_addr} failed: {err}"));
                }

                tokio::select! {
                    _ = state.shutdown.cancelled() => return None,
                    _ = sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(max_backoff);
            }
        }
    }
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

    pub async fn handle_accept_result(
        &mut self,
        res: io::Result<(TcpStream, SocketAddr)>,
        consecutive_errors: &mut u32,
    ) -> Option<(TcpStream, SocketAddr)> {
        match res {
            Ok((stream, addr)) => {
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
                    if let Some(new_listener) = bind_with_recovery(
                        &self.bind_addr,
                        &self.state,
                        self.initial_backoff,
                        self.max_backoff,
                    )
                    .await
                    {
                        if let Ok(addr) = new_listener.local_addr() {
                            self.local_addr = addr;
                        }
                        self.listener = Some(new_listener);
                    }
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
                if let Some(new_listener) = bind_with_recovery(
                    &self.bind_addr,
                    &self.state,
                    self.initial_backoff,
                    self.max_backoff,
                )
                .await
                {
                    if let Ok(addr) = new_listener.local_addr() {
                        self.local_addr = addr;
                    }
                    self.listener = Some(new_listener);
                } else {
                    // Shutdown was cancelled; park until axum's graceful shutdown exits.
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
/// with automatic rest-mode listener recovery until `state.shutdown` is cancelled.
pub async fn serve(state: Arc<HttpState>) -> io::Result<()> {
    let cfg = state.effective_config();
    let bind_addr = format_bind_addr(&cfg.server.bind_host, cfg.server.port);
    let Some(listener) = bind_with_recovery(
        &bind_addr,
        &state,
        DEFAULT_BACKOFF_INITIAL,
        DEFAULT_BACKOFF_MAX,
    )
    .await
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

    let recovering = RecoveringTcpListener::new(bind_addr, listener, state.clone())?.tap_io(|_| {});
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
