param(
    [string]$ElfPath = "build\ps5-agent.elf",
    [Alias("Ps5Host")]
    [string]$TargetHost = $(if ($env:PS5_HOST) { $env:PS5_HOST } else { "ps5" }),
    [int]$Port = $(if ($env:PS5_PORT) { [int]$env:PS5_PORT } else { 9021 }),
    [int]$ConnectTimeoutMs = 5000,
    [int]$ReadTimeoutMs = 3000,
    [int]$StreamSeconds = 5,
    [switch]$Follow
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
$ResolvedElf = if ([System.IO.Path]::IsPathRooted($ElfPath)) {
    $ElfPath
} else {
    Join-Path $RepoRoot $ElfPath
}

if (-not (Test-Path $ResolvedElf)) {
    throw "ELF not found at $ResolvedElf"
}

$Payload = [System.IO.File]::ReadAllBytes($ResolvedElf)
Write-Host "[deploy-ps5] Sending $ResolvedElf ($($Payload.Length) bytes) to ${TargetHost}:${Port}..."

$Client = New-Object System.Net.Sockets.TcpClient
try {
    $ConnectTask = $Client.ConnectAsync($TargetHost, $Port)
    if (-not $ConnectTask.Wait($ConnectTimeoutMs)) {
        throw "Timed out connecting to ${TargetHost}:${Port} after ${ConnectTimeoutMs}ms"
    }
    $ConnectTask.GetAwaiter().GetResult()

    $Client.ReceiveTimeout = $ReadTimeoutMs
    $Client.SendTimeout = 10000
    $Stream = $Client.GetStream()

    # Send raw ELF bytes and half-close write side so elfldr starts execution
    $Stream.Write($Payload, 0, $Payload.Length)
    $Stream.Flush()
    $Client.Client.Shutdown([System.Net.Sockets.SocketShutdown]::Send)
    Write-Host "[deploy-ps5] Payload sent. Streaming elfldr output..."

    $Buffer = New-Object byte[] 4096
    $Deadline = if ($Follow) { [DateTime]::MaxValue } else { (Get-Date).AddSeconds($StreamSeconds) }

    while ((Get-Date) -lt $Deadline) {
        try {
            $Read = $Stream.Read($Buffer, 0, $Buffer.Length)
            if ($Read -le 0) {
                break
            }
            $Text = [System.Text.Encoding]::UTF8.GetString($Buffer, 0, $Read)
            [Console]::Write($Text)
        } catch [System.IO.IOException] {
            $SockEx = $_.Exception.InnerException
            if ($SockEx -is [System.Net.Sockets.SocketException] -and $SockEx.SocketErrorCode -eq [System.Net.Sockets.SocketError]::TimedOut) {
                if (-not $Follow) {
                    break
                }
                continue
            }
            throw
        }
    }
    Write-Host ""
    Write-Host "[deploy-ps5] Done."
} finally {
    $Client.Close()
}
