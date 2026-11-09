# Starts a 3-node Raft cluster, one console window per node, all on
# 127.0.0.1. Kill the leader by closing its window; watch the other two
# elect a new one. To bring the dead node back, run its command line from
# -DryRun in a new window; it rejoins as a follower.
#
#   .\scripts\run_cluster.ps1                # ports 50061-50063
#   .\scripts\run_cluster.ps1 -BasePort 6000
#   .\scripts\run_cluster.ps1 -DryRun        # print the commands only

param(
    [int]$BasePort = 50061,
    [switch]$DryRun
)

$root = Split-Path -Parent $PSScriptRoot
$python = Join-Path $root ".venv\Scripts\python.exe"
if (-not (Test-Path $python)) {
    throw "venv python not found at $python (see README: Environment)"
}

$ids = @("n1", "n2", "n3")
$addr = @{}
for ($i = 0; $i -lt $ids.Count; $i++) {
    $addr[$ids[$i]] = "127.0.0.1:$($BasePort + $i)"
}

foreach ($id in $ids) {
    $port = $addr[$id].Split(":")[1]
    $peers = ($ids | Where-Object { $_ -ne $id } | ForEach-Object { "$_=$($addr[$_])" }) -join ","
    $cmd = "`$Host.UI.RawUI.WindowTitle = 'raft $id (port $port)'; " +
           "Set-Location '$root'; " +
           "& '$python' -m raft.run_node --id $id --port $port --peers $peers"
    if ($DryRun) {
        Write-Host "powershell -NoExit -Command $cmd"
    } else {
        Start-Process powershell -ArgumentList "-NoExit", "-Command", $cmd
    }
}
