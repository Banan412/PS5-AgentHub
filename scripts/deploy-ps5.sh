#!/usr/bin/env bash
set -euo pipefail

ELF_PATH="${1:-build/ps5-agent.elf}"
PS5_HOST="${PS5_HOST:-ps5}"
PS5_PORT="${PS5_PORT:-9021}"
PS5_PAYLOAD_SDK="${PS5_PAYLOAD_SDK:-/opt/ps5-payload-sdk}"

if [[ ! -f "${ELF_PATH}" ]]; then
    echo "ELF not found: ${ELF_PATH}" >&2
    exit 1
fi

echo "[deploy-ps5] Sending ${ELF_PATH} ($(wc -c < "${ELF_PATH}") bytes) to ${PS5_HOST}:${PS5_PORT}..."

if [[ -x "${PS5_PAYLOAD_SDK}/bin/prospero-deploy" ]]; then
    exec "${PS5_PAYLOAD_SDK}/bin/prospero-deploy" -h "${PS5_HOST}" -p "${PS5_PORT}" "${ELF_PATH}"
elif command -v python3 >/dev/null 2>&1; then
    exec python3 - "${PS5_HOST}" "${PS5_PORT}" "${ELF_PATH}" <<'PY'
import socket, sys
host, port, path = sys.argv[1], int(sys.argv[2]), sys.argv[3]
with open(path, "rb") as f:
    data = f.read()
with socket.create_connection((host, port), timeout=5) as s:
    s.settimeout(3)
    s.sendall(data)
    s.shutdown(socket.SHUT_WR)
    while True:
        try:
            chunk = s.recv(4096)
        except socket.timeout:
            break
        if not chunk:
            break
        sys.stdout.buffer.write(chunk)
        sys.stdout.buffer.flush()
PY
elif command -v nc >/dev/null 2>&1; then
    nc -w 3 "${PS5_HOST}" "${PS5_PORT}" < "${ELF_PATH}"
else
    echo "No prospero-deploy, python3, or nc found to send ELF." >&2
    exit 1
fi
