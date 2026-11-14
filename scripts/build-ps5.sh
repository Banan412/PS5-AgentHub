#!/usr/bin/env bash
set -euo pipefail

PACKAGE="${1:-ps5-agent}"
RELEASE="${RELEASE:-1}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

export PS5_PAYLOAD_SDK="${PS5_PAYLOAD_SDK:-/opt/ps5-payload-sdk}"
export PATH="${PS5_PAYLOAD_SDK}/bin:${PATH}"

export CC_x86_64_sie_ps5_std="${PS5_PAYLOAD_SDK}/bin/prospero-clang"
export AR_x86_64_sie_ps5_std="${PS5_PAYLOAD_SDK}/bin/prospero-ar"
export CFLAGS_x86_64_sie_ps5_std="-include ${REPO_ROOT}/include/ps5_sqlite_shim.h -DSQLITE_OS_UNIX=1 -DSQLITE_THREADSAFE=1 -DSQLITE_TEMP_STORE=2 -DSQLITE_OMIT_LOAD_EXTENSION=1 -DSQLITE_DEFAULT_MEMSTATUS=0 -DHAVE_POSIX_FALLOCATE=0"
export CARGO_TARGET_X86_64_SIE_PS5_STD_LINKER="${PS5_PAYLOAD_SDK}/bin/prospero-clang"

PROFILE_DIR="release"
CARGO_FLAGS=(
    "+nightly" "build"
    "--target" "${REPO_ROOT}/targets/x86_64-sie-ps5-std.json"
    "--target-dir" "${REPO_ROOT}/target"
    "-Z" "json-target-spec"
    "-Z" "build-std=std,panic_abort"
)
if [[ "${RELEASE}" == "1" ]]; then
    CARGO_FLAGS+=("--release")
else
    PROFILE_DIR="debug"
fi

if [[ "${PACKAGE}" == "m1-ps5-probe" ]]; then
    CARGO_FLAGS+=("--manifest-path" "${REPO_ROOT}/bins/m1-ps5-probe/Cargo.toml")
else
    CARGO_FLAGS+=("--manifest-path" "${REPO_ROOT}/Cargo.toml" "-p" "${PACKAGE}")
fi

echo "[build-ps5] Building ${PACKAGE} (${PROFILE_DIR}) for x86_64-sie-ps5-std..."
(cd "${REPO_ROOT}" && cargo "${CARGO_FLAGS[@]}")

RAW_BIN="${REPO_ROOT}/target/x86_64-sie-ps5-std/${PROFILE_DIR}/${PACKAGE}"
OUT_DIR="${REPO_ROOT}/build"
OUT_ELF="${OUT_DIR}/${PACKAGE}.elf"
mkdir -p "${OUT_DIR}"

# Strip via temp file to avoid chmod issues on WSL /mnt/c checkouts
TMP_ELF="$(mktemp)"
trap 'rm -f "${TMP_ELF}"' EXIT
"${PS5_PAYLOAD_SDK}/bin/prospero-strip" -o "${TMP_ELF}" "${RAW_BIN}"
cat "${TMP_ELF}" > "${OUT_ELF}"

if command -v elf-verify >/dev/null 2>&1; then
    if [[ "${PACKAGE}" == "ps5-agent" ]]; then
        elf-verify "${OUT_ELF}" --require-sqlite-shim --unstripped "${RAW_BIN}"
    else
        elf-verify "${OUT_ELF}"
    fi
elif command -v llvm-readelf >/dev/null 2>&1; then
    llvm-readelf -h -l -d "${OUT_ELF}" >/dev/null
fi

if [[ "${PACKAGE}" == "ps5-agent" ]]; then
    CARGO_VERSION="$(grep -m1 -E '^\s*version\s*=\s*"' "${REPO_ROOT}/Cargo.toml" | sed -E 's/.*"([^"]+)".*/\1/' || true)"
    CARGO_VERSION="${CARGO_VERSION:-0.1.0}"
    if command -v sha256sum >/dev/null 2>&1; then
        CHECKSUM="$(sha256sum "${OUT_ELF}" | awk '{print tolower($1)}')"
    else
        CHECKSUM="$(shasum -a 256 "${OUT_ELF}" | awk '{print tolower($1)}')"
    fi
    INSTALLED_AT="$(date +"%Y-%m-%dT%H:%M:%S%z")"
    LAST_UPDATE="$(date +"%Y-%m-%d")"
    META_PATH="${OUT_DIR}/meta.json"
    cat > "${META_PATH}" <<EOF
{
  "schema": 2,
  "kind": "repo",
  "filename": "ps5-agent.elf",
  "daemon": true,
  "autostart": true,
  "installed_at": "${INSTALLED_AT}",
  "id": "ps5-agent",
  "source_url": "",
  "source_name": "Custom Build",
  "name": "PS5 Agent",
  "version": "v${CARGO_VERSION}",
  "checksum": "${CHECKSUM}",
  "last_update": "${LAST_UPDATE}",
  "url": "",
  "source": "",
  "source_direct": "",
  "category": "Networking & Servers",
  "description": "Autonomous multi-agent AI runtime and web dashboard for jailbroken PS5 (FW 13.40) powered by PS5LM",
  "author": "",
  "details_url": ""
}
EOF
    echo "[build-ps5] Wrote ${META_PATH} (checksum: ${CHECKSUM})"
fi

echo "[build-ps5] SUCCESS: ${OUT_ELF} ($(wc -c < "${OUT_ELF}") bytes)"
