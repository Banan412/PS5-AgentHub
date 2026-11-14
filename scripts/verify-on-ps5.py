#!/usr/bin/env python3
"""Deploy and verify PS5 payloads over the network."""

import argparse
import json
import socket
import sys
import time
import urllib.error
import urllib.request


def deploy_elf(host: str, port: int, elf_path: str, stream_secs: float = 2.0) -> None:
    with open(elf_path, "rb") as f:
        payload = f.read()
    print(f"[verify] Deploying {elf_path} ({len(payload)} bytes) -> {host}:{port}")
    with socket.create_connection((host, port), timeout=5.0) as sock:
        sock.settimeout(stream_secs)
        sock.sendall(payload)
        sock.shutdown(socket.SHUT_WR)
        deadline = time.monotonic() + stream_secs
        while time.monotonic() < deadline:
            try:
                chunk = sock.recv(4096)
            except socket.timeout:
                break
            if not chunk:
                break
            sys.stdout.write(chunk.decode("utf-8", errors="replace"))
            sys.stdout.flush()


def http_get_json(url: str, timeout: float = 5.0) -> dict:
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8")
        return json.loads(raw)


def verify_m1(host: str, port: int, quit_after: bool) -> int:
    url = f"http://{host}:{port}/"
    print(f"[verify] Polling {url} ...")
    last_err = None
    data = None
    for _ in range(10):
        try:
            data = http_get_json(url, timeout=3.0)
            break
        except (urllib.error.URLError, ConnectionError, TimeoutError, json.JSONDecodeError) as e:
            last_err = e
            time.sleep(0.5)

    if data is None:
        print(f"[verify] FAIL: could not query {url}: {last_err}", file=sys.stderr)
        return 1

    print("[verify] Probe response:", json.dumps(data, indent=2))
    if data.get("status") != "ok":
        print("[verify] FAIL: status != ok", file=sys.stderr)
        return 1
    if not data.get("fs_test", {}).get("ok"):
        print("[verify] FAIL: fs_test failed", file=sys.stderr)
        return 1

    if quit_after:
        quit_url = f"http://{host}:{port}/quit"
        print(f"[verify] Requesting clean shutdown: {quit_url}")
        try:
            q = http_get_json(quit_url, timeout=3.0)
            print("[verify] Quit response:", q)
        except Exception as e:
            print(f"[verify] Warning during /quit: {e}")

    print("[verify] PASS: Milestone 1 PS5 probe verified.")
    return 0


def verify_agent(host: str, port: int) -> int:
    for path in ("/api/v1/health", "/health"):
        url = f"http://{host}:{port}{path}"
        try:
            data = http_get_json(url, timeout=3.0)
            print(f"[verify] {url} ->", json.dumps(data, indent=2))
            print("[verify] PASS: ps5-agent responded.")
            return 0
        except Exception as e:
            last_err = e
    print(f"[verify] FAIL: ps5-agent health check failed: {last_err}", file=sys.stderr)
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify PS5 payload on console")
    parser.add_argument("--host", default="ps5", help="PS5 hostname or IP")
    parser.add_argument("--elfldr-port", type=int, default=9021, help="PS5 elfldr port")
    parser.add_argument("--mode", choices=("m1", "agent"), default="m1")
    parser.add_argument("--elf", default=None, help="ELF file to deploy first")
    parser.add_argument("--skip-deploy", action="store_true")
    parser.add_argument("--probe-port", type=int, default=9957)
    parser.add_argument("--agent-port", type=int, default=8080)
    parser.add_argument("--quit", action="store_true", help="Call /quit after M1 probe verification")
    args = parser.parse_args()

    if not args.skip_deploy:
        elf = args.elf or ("build/m1-ps5-probe.elf" if args.mode == "m1" else "build/ps5-agent.elf")
        deploy_elf(args.host, args.elfldr_port, elf)
        time.sleep(0.5)

    if args.mode == "m1":
        return verify_m1(args.host, args.probe_port, args.quit)
    return verify_agent(args.host, args.agent_port)


if __name__ == "__main__":
    sys.exit(main())
