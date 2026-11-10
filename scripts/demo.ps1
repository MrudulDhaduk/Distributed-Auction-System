# One-command demo: opens the dashboard in a second window, then runs the
# scripted auction in this one (scripts/demo.py): LLM server + 3 nodes,
# login, bids, an LLM answer about the live auction, kill -9 the leader
# mid-auction, restart it, the close, the LLM-written result summary, the
# winner paying into escrow, a SECOND leader kill with the same escrow read
# back, price trends, then the property checker prints PASS/FAIL per property.
#
#   .\scripts\demo.ps1                 # 2 s between steps
#   .\scripts\demo.ps1 -Pause 4        # slower, for presenting
#   .\scripts\demo.ps1 -NoLLM          # without the LLM server (LLM steps show the clear error)
#
# Uses the run_cluster.ps1 ports (peer 50061-50063, client 50071-50073, LLM
# 50052): do not run both at once. Output under runs\demo-<time>\.

param(
    [double]$Pause = 2,
    [int]$BasePeer = 50061,
    [int]$BaseClient = 50071,
    [int]$LlmPort = 50052,
    [switch]$NoLLM,
    [switch]$Keep
)

$root = Split-Path -Parent $PSScriptRoot
$python = Join-Path $root ".venv\Scripts\python.exe"
if (-not (Test-Path $python)) {
    throw "venv python not found at $python (see README: Environment)"
}

$dash = "`$Host.UI.RawUI.WindowTitle = 'dashboard'; Set-Location '$root'; " +
        "& '$python' scripts\dashboard.py --base-peer $BasePeer"
Start-Process powershell -ArgumentList "-NoExit", "-Command", $dash

$demoArgs = @("scripts\demo.py", "--pause", $Pause, "--base-peer", $BasePeer,
              "--base-client", $BaseClient, "--llm-port", $LlmPort)
if ($NoLLM) { $demoArgs += "--no-llm" }
if ($Keep) { $demoArgs += "--keep" }

Push-Location $root
try {
    & $python @demoArgs
    $code = $LASTEXITCODE
} finally {
    Pop-Location
}
if ($code -eq 0) { Write-Host "`nDEMO: ALL PROPERTIES PASS" -ForegroundColor Green }
else { Write-Host "`nDEMO: CHECKER FAILED (exit $code)" -ForegroundColor Red }
exit $code
