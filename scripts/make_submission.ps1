# Builds the submission zip: code, README, docs, proto files, scripts, tests.
#
#   .\scripts\make_submission.ps1                  # -> dist\Distributed-Auction-System-<time>.zip
#   .\scripts\make_submission.ps1 -OutDir C:\tmp   # somewhere else
#
# Prints the zip path ("zip: <path>") and then every file in it, one per
# line, indented two spaces. Exit code 0 on success.
#
# What goes in is an INCLUDE list (below), not "everything minus some
# things": a new folder in the repo stays out until someone adds it here on
# purpose. On top of that, every candidate file is checked against the
# EXCLUDE rules, so a cache or data directory that turns up inside an
# included folder is still left out:
#   .venv, data, __pycache__, *.pyc/*.pyo, .git, papers, CLAUDE.md,
#   CLAUDE.old.md, run output (runs, logs), dist, .pytest_cache.
# MyNotes\, Learning\ and Documents\ are personal study notes: not included.
#
# Entries are written with forward slashes under one top folder
# (Distributed-Auction-System/...). Compress-Archive is not used because on
# Windows PowerShell 5.1 it writes backslashes into entry names, which other
# unzip tools then treat as part of the file name.

param(
    [string]$OutDir = ""
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
if (-not $OutDir) { $OutDir = Join-Path $root "dist" }
$top = "Distributed-Auction-System"

$includeDirs = @(
    "auction", "checker", "client", "common", "generated", "harness",
    "llm_server", "raft", "replication", "server", "sessions",
    "proto", "scripts", "docs", "tests"
)
$includeFiles = @("README.md", "requirements.txt", "pytest.ini", "LICENSE")

$excludeSegments = @(".venv", "data", "__pycache__", ".git", "papers",
                     "runs", "logs", "dist", ".pytest_cache")
$excludeNames = @("CLAUDE.md", "CLAUDE.old.md")
$excludeExtensions = @(".pyc", ".pyo")

function Test-Excluded([string]$rel) {
    # $rel uses forward slashes, relative to the repo root.
    $parts = $rel.Split("/")
    $name = $parts[-1]
    if ($excludeNames -contains $name) { return $true }
    if ($excludeExtensions -contains [System.IO.Path]::GetExtension($name).ToLower()) { return $true }
    for ($i = 0; $i -lt $parts.Length - 1; $i++) {
        if ($excludeSegments -contains $parts[$i]) { return $true }
    }
    return $false
}

# --- collect the files ------------------------------------------------------
$files = New-Object System.Collections.Generic.List[string]
foreach ($f in $includeFiles) {
    $p = Join-Path $root $f
    if (-not (Test-Path $p -PathType Leaf)) { throw "missing top-level file: $f" }
    $files.Add($f)
}
foreach ($d in $includeDirs) {
    $p = Join-Path $root $d
    if (-not (Test-Path $p -PathType Container)) { throw "missing folder: $d" }
    Get-ChildItem -Path $p -Recurse -File -Force | ForEach-Object {
        $rel = $_.FullName.Substring($root.Length + 1).Replace("\", "/")
        if (-not (Test-Excluded $rel)) { $files.Add($rel) }
    }
}
$files = $files | Sort-Object

# --- write the zip -------------------------------------------------------------
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
$OutDir = (Resolve-Path $OutDir).Path
$zipPath = Join-Path $OutDir ("$top-" + (Get-Date -Format "yyyyMMdd-HHmmss") + ".zip")
if (Test-Path $zipPath) { throw "already exists: $zipPath" }

Add-Type -AssemblyName System.IO.Compression
Add-Type -AssemblyName System.IO.Compression.FileSystem
$zip = [System.IO.Compression.ZipFile]::Open($zipPath, [System.IO.Compression.ZipArchiveMode]::Create)
try {
    foreach ($rel in $files) {
        $src = Join-Path $root ($rel.Replace("/", "\"))
        [System.IO.Compression.ZipFileExtensions]::CreateEntryFromFile(
            $zip, $src, "$top/$rel", [System.IO.Compression.CompressionLevel]::Optimal) | Out-Null
    }
} finally {
    $zip.Dispose()
}

# --- report ----------------------------------------------------------------------
$size = (Get-Item $zipPath).Length
Write-Output "zip: $zipPath"
Write-Output ("files: {0}, {1} bytes" -f $files.Count, $size)
foreach ($rel in $files) { Write-Output "  $top/$rel" }
exit 0
