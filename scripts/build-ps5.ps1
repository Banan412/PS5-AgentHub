param(
    [ValidateSet("ps5-agent", "m1-ps5-probe")]
    [string]$Package = "ps5-agent",
    [bool]$Release = $true
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
$DefaultToolchain = Join-Path (Split-Path -Parent $RepoRoot) ".ps5-toolchain"

if (-not $env:PS5_PAYLOAD_SDK) {
    $CandidateSdk = Join-Path $DefaultToolchain "ps5-payload-sdk"
    if (Test-Path $CandidateSdk) {
        $env:PS5_PAYLOAD_SDK = $CandidateSdk
    } else {
        throw "PS5_PAYLOAD_SDK is not set and default toolchain was not found at $CandidateSdk"
    }
}

$MingwBin = if ($env:PS5_MINGW_BIN) {
    $env:PS5_MINGW_BIN
} else {
    Join-Path $DefaultToolchain "mingw64\bin"
}
if (-not (Test-Path $MingwBin)) {
    $FallbackMingw = Join-Path (Split-Path -Parent $env:PS5_PAYLOAD_SDK) "mingw64\bin"
    if (Test-Path $FallbackMingw) {
        $MingwBin = $FallbackMingw
    } else {
        throw "MinGW/Clang bin directory not found at $MingwBin"
    }
}
$env:PS5_MINGW_BIN = $MingwBin

if (-not $env:LIBCLANG_PATH) {
    $env:LIBCLANG_PATH = $MingwBin
}

$CargoBin = Join-Path $env:USERPROFILE ".cargo\bin"
$SdkBin = Join-Path $env:PS5_PAYLOAD_SDK "bin"
$BuildDir = Join-Path $RepoRoot "build"
New-Item -ItemType Directory -Force -Path $BuildDir | Out-Null

# Put cargo/rustc, SDK bin wrappers, and MinGW/Clang DLLs at the front of PATH
$PathParts = @()
if (Test-Path $CargoBin) { $PathParts += $CargoBin }
$PathParts += $SdkBin
$PathParts += $MingwBin
$ExistingParts = ($env:PATH -split ';' | Where-Object { $_ })
$env:PATH = (($PathParts + $ExistingParts) | Select-Object -Unique) -join ';'

$ClangExe = Join-Path $SdkBin "prospero-clang.exe"
$ArExe = Join-Path $SdkBin "llvm-ar.exe"
$StripExe = Join-Path $SdkBin "prospero-strip.exe"
$VerifyExe = Join-Path $SdkBin "elf-verify.exe"
$ShimHeader = (Join-Path $RepoRoot "include\ps5_sqlite_shim.h") -replace '\\', '/'

$env:CC_x86_64_sie_ps5_std = $ClangExe
$env:AR_x86_64_sie_ps5_std = $ArExe
$env:CFLAGS_x86_64_sie_ps5_std = "-include $ShimHeader -DSQLITE_OS_UNIX=1 -DSQLITE_THREADSAFE=1 -DSQLITE_TEMP_STORE=2 -DSQLITE_OMIT_LOAD_EXTENSION=1 -DSQLITE_DEFAULT_MEMSTATUS=0 -DHAVE_POSIX_FALLOCATE=0"
$env:CARGO_TARGET_X86_64_SIE_PS5_STD_LINKER = $ClangExe

$TargetJson = Join-Path $RepoRoot "targets\x86_64-sie-ps5-std.json"
$ProfileDir = if ($Release) { "release" } else { "debug" }

$CargoArgs = @(
    "+nightly", "build",
    "--target", $TargetJson,
    "--target-dir", (Join-Path $RepoRoot "target"),
    "-Z", "json-target-spec",
    "-Z", "build-std=std,panic_abort"
)
if ($Release) {
    $CargoArgs += "--release"
}

if ($Package -eq "m1-ps5-probe") {
    $CargoArgs += @("--manifest-path", (Join-Path $RepoRoot "bins\m1-ps5-probe\Cargo.toml"))
} else {
    $CargoArgs += @("--manifest-path", (Join-Path $RepoRoot "Cargo.toml"), "-p", $Package)
}

Write-Host "[build-ps5] Building $Package ($ProfileDir) for x86_64-sie-ps5-std..."
Write-Host "[build-ps5] PS5_PAYLOAD_SDK=$($env:PS5_PAYLOAD_SDK)"
Write-Host "[build-ps5] LIBCLANG_PATH=$($env:LIBCLANG_PATH)"

Push-Location $RepoRoot
try {
    & cargo @CargoArgs
    if ($LASTEXITCODE -ne 0) {
        throw "cargo build failed with exit code $LASTEXITCODE"
    }
} finally {
    Pop-Location
}

$RawBin = Join-Path $RepoRoot "target\x86_64-sie-ps5-std\$ProfileDir\$Package"
if (-not (Test-Path $RawBin)) {
    throw "Expected compiled binary not found at $RawBin"
}

$OutElf = Join-Path $BuildDir "$Package.elf"
Write-Host "[build-ps5] Stripping -> $OutElf"
& $StripExe -o $OutElf $RawBin
if ($LASTEXITCODE -ne 0) {
    throw "prospero-strip.exe failed with exit code $LASTEXITCODE"
}

Write-Host "[build-ps5] Verifying -> $OutElf"
if ($Package -eq "ps5-agent") {
    & $VerifyExe $OutElf --require-sqlite-shim --unstripped $RawBin
} else {
    & $VerifyExe $OutElf
}
if ($LASTEXITCODE -ne 0) {
    throw "elf-verify.exe failed with exit code $LASTEXITCODE"
}

$ElfSize = (Get-Item $OutElf).Length

if ($Package -eq "ps5-agent") {
    $CargoTomlPath = Join-Path $RepoRoot "Cargo.toml"
    $CargoVersion = "0.1.0"
    if (Test-Path $CargoTomlPath) {
        $VersionMatch = Select-String -Path $CargoTomlPath -Pattern '^\s*version\s*=\s*"([^"]+)"' | Select-Object -First 1
        if ($VersionMatch) {
            $CargoVersion = $VersionMatch.Matches.Groups[1].Value
        }
    }
    $Checksum = (Get-FileHash -Algorithm SHA256 $OutElf).Hash.ToLower()
    $InstalledAt = (Get-Date).ToString("yyyy-MM-ddTHH:mm:sszzz") -replace '([+-]\d{2}):(\d{2})$', '$1$2'
    $LastUpdate = (Get-Date).ToString("yyyy-MM-dd")
    $MetaPath = Join-Path $BuildDir "meta.json"
    $MetaJson = @"
{
  "schema": 2,
  "kind": "repo",
  "filename": "ps5-agent.elf",
  "daemon": true,
  "autostart": true,
  "installed_at": "$InstalledAt",
  "id": "ps5-agent",
  "source_url": "",
  "source_name": "Custom Build",
  "name": "PS5 Agent",
  "version": "v$CargoVersion",
  "checksum": "$Checksum",
  "last_update": "$LastUpdate",
  "url": "",
  "source": "",
  "source_direct": "",
  "category": "Networking & Servers",
  "description": "Autonomous multi-agent AI runtime and web dashboard for jailbroken PS5 (FW 13.40) powered by PS5LM",
  "author": "",
  "details_url": ""
}

"@ -replace "`r`n", "`n"
    [System.IO.File]::WriteAllText($MetaPath, $MetaJson, (New-Object System.Text.UTF8Encoding($false)))
    Write-Host "[build-ps5] Wrote $MetaPath (checksum: $Checksum)"
}

Write-Host "[build-ps5] SUCCESS: $OutElf ($ElfSize bytes)"
