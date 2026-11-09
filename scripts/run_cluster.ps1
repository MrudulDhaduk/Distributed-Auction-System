# Starts the auction cluster on 127.0.0.1, one console window per process:
#   - the LLM server              port 50052 (llm_server/config.py)
#   - auction nodes n1, n2, n3    peer ports 50061-50063 (RaftService)
#                                 client ports 50071-50073 (Auth + Auction)
# Each node keeps term, vote and log in data\<id> (gitignored), so closing
# a window and re-running its command line from -DryRun restarts that node
# with its log, and it rebuilds its state by replay.
#
# Kill the leader by closing its window; watch the other two elect a new
# one; clients (client/auction_client.py) follow the redirect.
#
#   .\scripts\run_cluster.ps1                 # the auction cluster + LLM server
#   .\scripts\run_cluster.ps1 -NoLLM          # without the LLM server (descriptions degrade to "")
#   .\scripts\run_cluster.ps1 -RaftOnly       # bare Raft nodes (raft.run_node), ports 50061-50063
#   .\scripts\run_cluster.ps1 -DryRun         # print the commands only
#
# Ports: Windows reserves 50155-50254 on this machine (netsh interface ipv4
# show excludedportrange protocol=tcp), so -BasePort must keep both ranges
# clear of that.

param(
    [int]$BasePort = 50061,         # peer ports BasePort .. BasePort+2
    [int]$ClientBasePort = 50071,   # client ports ClientBasePort .. +2
    [switch]$RaftOnly,
    [switch]$NoLLM,
    [switch]$DryRun
)

$root = Split-Path -Parent $PSScriptRoot
$python = Join-Path $root ".venv\Scripts\python.exe"
if (-not (Test-Path $python)) {
    throw "venv python not found at $python (see README: Environment)"
}

function Launch($title, $argsLine) {
    $cmd = "`$Host.UI.RawUI.WindowTitle = '$title'; " +
           "Set-Location '$root'; " +
           "& '$python' $argsLine"
    if ($DryRun) {
        Write-Host "powershell -NoExit -Command $cmd"
    } else {
        Start-Process powershell -ArgumentList "-NoExit", "-Command", $cmd
    }
}

$ids = @("n1", "n2", "n3")
$peerPort = @{}
$clientPort = @{}
for ($i = 0; $i -lt $ids.Count; $i++) {
    $peerPort[$ids[$i]] = $BasePort + $i
    $clientPort[$ids[$i]] = $ClientBasePort + $i
}

if ($RaftOnly) {
    foreach ($id in $ids) {
        $peers = ($ids | Where-Object { $_ -ne $id } | ForEach-Object { "$_=127.0.0.1:$($peerPort[$_])" }) -join ","
        Launch "raft $id (port $($peerPort[$id]))" "-m raft.run_node --id $id --port $($peerPort[$id]) --peers $peers"
    }
    return
}

if (-not $NoLLM) {
    Launch "llm server (port 50052)" "-m llm_server.serve"
}

foreach ($id in $ids) {
    $peers = ($ids | Where-Object { $_ -ne $id } | ForEach-Object {
        "$_=127.0.0.1:$($peerPort[$_]):$($clientPort[$_])"
    }) -join ","
    $dataDir = Join-Path $root "data\$id"
    Launch "auction $id (client $($clientPort[$id]), peer $($peerPort[$id]))" (
        "-m server.serve --node-id $id --client-port $($clientPort[$id]) --peer-port $($peerPort[$id]) " +
        "--peers $peers --data-dir '$dataDir' --llm-addr 127.0.0.1:50052")
}
