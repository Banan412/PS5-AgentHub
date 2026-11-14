use std::net::SocketAddr;

#[tokio::main]
async fn main() {
    let addr: SocketAddr = std::env::var("MOCK_PS5LM_ADDR")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 8081)));

    let handle = match mock_ps5lm::start_on(addr).await {
        Ok(h) => h,
        Err(err) => {
            eprintln!("mock-ps5lm failed to bind {addr}: {err}");
            std::process::exit(1);
        }
    };

    let _ = tokio::signal::ctrl_c().await;
    handle.shutdown();
}
