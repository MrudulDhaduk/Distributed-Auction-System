"""Terminal dashboard: every node's Raft and state-machine status, live.

    .venv\\Scripts\\python.exe scripts\\dashboard.py                 # run_cluster.ps1 ports 50061-50063
    .venv\\Scripts\\python.exe scripts\\dashboard.py --base-peer 50081  # a chaos / demo cluster

Polls StatusService.GetNodeStatus on each node's PEER port about 5 times a
second (harness/status.py) and shows, per node: up/down, role, term, who it
thinks leads, commitIndex, lastApplied, last log index/term, and the state
digest with the index it covers. Underneath, one verdict and a scrolling
event line:

  agree        every reachable node: same digest_index, same digest
  catching up  indexes differ (normal for a moment after each write)
  DIVERGED     same index, different digest: a real bug (apply() is
               deterministic, so the same log prefix must give the same state)

Digest colours: nodes sharing a digest share a colour.

Read-only: it never writes, and the numbers are each node's OWN view, so
two LEADER rows can appear for a moment (a deposed leader that has not yet
heard the new term). The term column tells them apart.
"""

import argparse
import collections
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rich.console import Group  # noqa: E402
from rich.live import Live  # noqa: E402
from rich.panel import Panel  # noqa: E402
from rich.table import Table  # noqa: E402
from rich.text import Text  # noqa: E402

from harness.status import StatusPoller, agreement, current_leader, events  # noqa: E402

ROLE_STYLE = {"LEADER": "bold green", "CANDIDATE": "bold yellow", "FOLLOWER": "cyan"}
DIGEST_STYLES = ["magenta", "blue", "bright_red", "bright_cyan"]
VERDICT_STYLE = {"agree": "bold green", "catching up": "yellow", "DIVERGED": "bold white on red", "no data": "red"}


def render(view, event_lines, polls):
    table = Table(expand=True, title="Raft auction cluster", title_style="bold")
    for col in ("node", "up", "role", "term", "leader", "commit", "applied",
                "last log (idx/term)", "digest @ index"):
        table.add_column(col, justify="right" if col in ("term", "commit", "applied") else "left")
    colours = {}
    leader = current_leader(view)
    for node in sorted(view):
        s = view[node]
        if s is None:
            table.add_row(node, Text("DOWN", style="bold red"), "-", "-", "-", "-", "-", "-", "-")
            continue
        style = colours.setdefault(s.state_digest, DIGEST_STYLES[len(colours) % len(DIGEST_STYLES)])
        role = Text(s.role, style=ROLE_STYLE.get(s.role, ""))
        if s.role == "LEADER" and node != leader:
            role.append(" (stale)", style="dim")
        table.add_row(
            node, Text("up", style="green"), role, str(s.term), s.leader_id or "?",
            str(s.commit_index), str(s.last_applied), f"{s.last_log_index}/{s.last_log_term}",
            Text(f"{s.state_digest[:12]} @ {s.digest_index}", style=style),
        )
    verdict = agreement(view)
    head = Text.assemble(("replicas: ", "bold"), (verdict, VERDICT_STYLE[verdict]),
                         (f"    leader: {leader or 'none'}    polls: {polls}", "dim"))
    log = Panel("\n".join(event_lines) or "(no events yet)", title="events", height=14)
    return Group(table, head, log)


def parse_nodes(text):
    out = {}
    for item in filter(None, (x.strip() for x in text.split(","))):
        nid, _, addr = item.partition("=")
        out[nid] = addr
    return out


def main(argv=None):
    p = argparse.ArgumentParser(description="Live status of every node (StatusService on the peer ports).")
    p.add_argument("--nodes", help="n1=127.0.0.1:50061,... (overrides --base-peer)")
    p.add_argument("--base-peer", type=int, default=50061, help="peer ports base..base+2 for n1..n3")
    p.add_argument("--interval", type=float, default=0.2, help="seconds between polls (default 0.2 = 5/s)")
    args = p.parse_args(argv)
    addrs = parse_nodes(args.nodes) if args.nodes else {
        f"n{i + 1}": f"127.0.0.1:{args.base_peer + i}" for i in range(3)}

    poller = StatusPoller(addrs)
    lines = collections.deque(maxlen=12)
    prev, polls = {}, 0
    try:
        with Live(render({n: None for n in addrs}, lines, 0), refresh_per_second=10, screen=False) as live:
            while True:
                t0 = time.monotonic()
                view = poller.poll()
                polls += 1
                stamp = time.strftime("%H:%M:%S")
                for e in events(prev, view):
                    lines.append(f"{stamp}  {e}")
                prev = view
                live.update(render(view, lines, polls))
                time.sleep(max(0.0, args.interval - (time.monotonic() - t0)))
    except KeyboardInterrupt:
        pass
    finally:
        poller.close()


if __name__ == "__main__":
    main()
