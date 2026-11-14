//! Milestone 1 PS5 hardware/runtime verification probe.
//!
//! - Installs a panic hook writing to `/data/ps5-agent/m1_probe.log` and `ps5::Toast`
//! - Never panics on stdout `BrokenPipe`
//! - Logs firmware version, model name, and CPU temperature
//! - Verifies directory creation and file read/write under `/data/ps5-agent-m1-test` (and cleans up)
//! - Sends a `ps5::Toast`
//! - Serves a JSON probe report on TCP port 9957 (`/` and `/quit`)

use ps5::{klog, Toast};
use ps5_std_rt as _;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const PORT: u16 = 9957;
const LOG_DIR: &str = "/data/ps5-agent";
const LOG_FILE: &str = "/data/ps5-agent/m1_probe.log";
const TEST_DIR: &str = "/data/ps5-agent-m1-test";

fn safe_stdout(line: &str) {
    let mut out = std::io::stdout().lock();
    let _ = out.write_all(line.as_bytes());
    let _ = out.write_all(b"\n");
    let _ = out.flush();
}

fn append_log_file(line: &str) {
    let _ = fs::create_dir_all(LOG_DIR);
    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(LOG_FILE) {
        let _ = writeln!(f, "{line}");
        let _ = f.flush();
    }
}

fn log_line(msg: &str) {
    let line = format!("[m1-ps5-probe] {msg}");
    append_log_file(&line);
    klog::log(&line);
    safe_stdout(&line);
}

fn send_toast(title: &str, body: &str) {
    let _ = Toast::new(title).sub(body).send();
}

fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out
}

#[derive(Clone)]
struct ProbeReport {
    fw_version: u32,
    model_name: String,
    cpu_temp_c: Result<i32, i32>,
    fs_ok: bool,
    fs_detail: String,
    pid: i32,
}

impl ProbeReport {
    fn to_json(&self, hits: u64, uptime_ms: u128, peer: &str) -> String {
        let temp_json = match self.cpu_temp_c {
            Ok(t) => format!("{t}"),
            Err(_) => "null".to_owned(),
        };
        let temp_err_json = match self.cpu_temp_c {
            Ok(_) => "null".to_owned(),
            Err(e) => format!("{e}"),
        };
        format!(
            "{{\"status\":\"ok\",\"probe\":\"m1-ps5-probe\",\"pid\":{},\"fw_version\":\"0x{:08x}\",\"fw_version_raw\":{},\"model\":\"{}\",\"cpu_temp_c\":{},\"cpu_temp_err\":{},\"fs_test\":{{\"ok\":{},\"detail\":\"{}\"}},\"hits\":{},\"uptime_ms\":{},\"peer\":\"{}\"}}\n",
            self.pid,
            self.fw_version,
            self.fw_version,
            json_escape(&self.model_name),
            temp_json,
            temp_err_json,
            self.fs_ok,
            json_escape(&self.fs_detail),
            hits,
            uptime_ms,
            json_escape(peer),
        )
    }
}

fn verify_filesystem() -> (bool, String) {
    let dir = Path::new(TEST_DIR);
    let file = dir.join("probe.txt");
    let payload = b"ps5-agent m1 fs check\n";

    let _ = fs::remove_dir_all(dir);
    if let Err(e) = fs::create_dir_all(dir) {
        return (false, format!("create_dir_all failed: {e}"));
    }
    if let Err(e) = fs::write(&file, payload) {
        let _ = fs::remove_dir_all(dir);
        return (false, format!("write failed: {e}"));
    }
    let read_back = match fs::read(&file) {
        Ok(bytes) => bytes,
        Err(e) => {
            let _ = fs::remove_dir_all(dir);
            return (false, format!("read failed: {e}"));
        }
    };
    if read_back != payload {
        let _ = fs::remove_dir_all(dir);
        return (
            false,
            format!("readback mismatch: got {} bytes", read_back.len()),
        );
    }
    if let Err(e) = fs::remove_file(&file) {
        let _ = fs::remove_dir_all(dir);
        return (false, format!("remove_file failed: {e}"));
    }
    if let Err(e) = fs::remove_dir(dir) {
        let _ = fs::remove_dir_all(dir);
        return (false, format!("remove_dir failed: {e}"));
    }
    (true, "mkdir + write + read + cleanup OK".to_owned())
}

fn install_panic_hook() {
    std::panic::set_hook(Box::new(|info| {
        let msg = info
            .payload()
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| info.payload().downcast_ref::<String>().map(String::as_str))
            .unwrap_or("unknown panic");
        let loc = info
            .location()
            .map(|l| format!("{}:{}", l.file(), l.line()))
            .unwrap_or_else(|| "unknown".to_owned());
        let line = format!("[m1-ps5-probe] PANIC: {msg} @ {loc}");
        append_log_file(&line);
        klog::log(&line);
        safe_stdout(&line);
        send_toast("m1-ps5-probe PANIC", &format!("{msg} @ {loc}"));
    }));
}

fn main() {
    let _ = ps5::payload::set_process_name("m1-ps5-probe.elf");
    install_panic_hook();
    log_line("starting milestone 1 probe");

    let fw_version = ps5::fw_version();
    let pid = ps5::pid();
    let model_name = match ps5::hw::model_name() {
        Ok(m) => m.as_str().to_owned(),
        Err(e) => format!("error({})", e.0),
    };
    let cpu_temp_c = ps5::hw::cpu_temperature().map_err(|e| e.0);

    log_line(&format!(
        "pid={pid} fw_version=0x{fw_version:08x} model={model_name} cpu_temp_c={cpu_temp_c:?}"
    ));

    let (fs_ok, fs_detail) = verify_filesystem();
    log_line(&format!("fs_test ok={fs_ok} detail={fs_detail}"));

    let temp_str = match cpu_temp_c {
        Ok(t) => format!("{t}C"),
        Err(e) => format!("err({e})"),
    };
    send_toast(
        "m1-ps5-probe ready",
        &format!("FW 0x{fw_version:08x} | {model_name} | {temp_str}"),
    );

    let report = ProbeReport {
        fw_version,
        model_name,
        cpu_temp_c,
        fs_ok,
        fs_detail,
        pid,
    };

    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            log_line(&format!("tokio runtime build failed: {e}"));
            send_toast("m1-ps5-probe error", &format!("tokio init: {e}"));
            unsafe { ps5::sys::_Exit(1) };
        }
    };

    rt.block_on(serve(report));
}

async fn serve(report: ProbeReport) {
    let listener = match TcpListener::bind(("0.0.0.0", PORT)).await {
        Ok(l) => l,
        Err(e) => {
            log_line(&format!("bind 0.0.0.0:{PORT} failed: {e}"));
            send_toast("m1-ps5-probe bind failed", &e.to_string());
            unsafe { ps5::sys::_Exit(1) };
        }
    };
    log_line(&format!("listening on 0.0.0.0:{PORT}"));

    let hits = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    loop {
        let (mut stream, peer) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                log_line(&format!("accept error: {e}"));
                continue;
            }
        };

        let report = report.clone();
        let hits = hits.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            let n = stream.read(&mut buf).await.unwrap_or(0);
            let path = std::str::from_utf8(&buf[..n])
                .ok()
                .and_then(|req| req.split_whitespace().nth(1))
                .unwrap_or("/")
                .to_owned();

            if path == "/quit" || path.starts_with("/quit?") {
                let body = "{\"status\":\"quitting\"}\n";
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(resp.as_bytes()).await;
                let _ = stream.flush().await;
                drop(stream);
                log_line("/quit requested, exiting cleanly");
                send_toast("m1-ps5-probe", "exiting via /quit");
                tokio::time::sleep(Duration::from_millis(50)).await;
                unsafe { ps5::sys::_Exit(0) };
            }

            let hit = hits.fetch_add(1, Ordering::Relaxed) + 1;
            let body = report.to_json(hit, start.elapsed().as_millis(), &peer.to_string());
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(resp.as_bytes()).await;
            let _ = stream.flush().await;
        });
    }
}
