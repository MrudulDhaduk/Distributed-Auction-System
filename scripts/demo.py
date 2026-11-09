"""The scripted demo (run by scripts/demo.ps1, which also opens the dashboard).

  1. start the LLM server and three node processes (fresh data dirs)
  2. log in a seller and three bidders (one ClusterClient each)
  3. create an auction (with attributes: the LLM writes its description)
  4. bid, including two EQUAL bids at once (exactly one may win)
  5. kill -9 the leader mid-auction; keep bidding (clients find the new leader)
  6. restart the dead node; it catches up by replaying its log + AppendEntries
  7. bid again, wait for the close, read the winner
  8. run the property checker on everything the clients saw: PASS/FAIL each

    .venv\\Scripts\\python.exe scripts\\demo.py [--pause 2] [--keep]

--pause: seconds between steps, so a human can watch the dashboard.
--keep:  leave the cluster running at the end (Ctrl+C to stop it).
"""

import argparse
import datetime
import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from client.auction_client import ClusterClient  # noqa: E402
from common import time_conv  # noqa: E402
from harness import run as R  # noqa: E402
from harness.procs import ROOT, LocalCluster  # noqa: E402
from harness.status import StatusMonitor, StatusPoller, current_leader  # noqa: E402
from harness.workload import PASSWORD, History, record_write  # noqa: E402

AUCTION = 1
CLOSE_SECONDS = 60


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Scripted auction with a leader kill, then the checker.")
    p.add_argument("--pause", type=float, default=2.0)
    p.add_argument("--keep", action="store_true")
    p.add_argument("--base-peer", type=int, default=50061)
    p.add_argument("--base-client", type=int, default=50071)
    p.add_argument("--llm-port", type=int, default=50052)
    p.add_argument("--no-llm", action="store_true")
    args = p.parse_args(argv)

    run_dir = os.path.join(ROOT, "runs", f"demo-{time.strftime('%Y%m%d-%H%M%S')}")
    cluster = LocalCluster(run_dir, base_peer=args.base_peer, base_client=args.base_client,
                           llm_port=None if args.no_llm else args.llm_port)
    poller = StatusPoller(cluster.peer_addrs())
    monitor = StatusMonitor(poller, interval=0.05)
    history = History()
    users = {}

    def step(text):
        print(f"\n=== {text}", flush=True)
        time.sleep(args.pause)

    def write(name, op, call, **fields):
        c = users[name]
        r = record_write(history, name, name, c, op, call, **fields)
        extra = f" [high {r['high_bid']} by {r['high_bidder']}]" if op == "bid" else ""
        print(f"  {name:7s} {op:6s} {fields.get('amount', '')!s:>4}  -> {r['outcome']:8s} {r['reason']}{extra}",
              flush=True)
        return r

    def bid(name, amount):
        return write(name, "bid", lambda: users[name].place_bid(AUCTION, amount), auction_id=AUCTION, amount=amount)

    status = 1
    try:
        step(f"starting LLM + 3 nodes (logs and data under {run_dir})")
        cluster.start()
        monitor.start()
        leader = R.wait_for_leader(poller, 60)
        print(f"  leader: {leader}")

        step("log in: nisarg (seller), karan, mrudul, tanmay -- one client each")
        for name in ("nisarg", "karan", "mrudul", "tanmay"):
            users[name] = ClusterClient(cluster.client_addrs())
            write(name, "login", lambda n=name: users[n].login(n, PASSWORD))

        step(f"nisarg creates auction {AUCTION}, closing in {CLOSE_SECONDS} s (LLM writes the description)")
        close = time_conv.dt_to_micros(time_conv.now_utc() + datetime.timedelta(seconds=CLOSE_SECONDS))
        write("nisarg", "create", lambda: users["nisarg"].create_auction(
            AUCTION, "vintage clock", close, {"brand": "Seiko", "era": "1970s", "condition": "running"}),
            auction_id=AUCTION, item="vintage clock", close_time=close)
        a = users["nisarg"].get_auction(AUCTION)
        print(f"  description: {a.description!r}" if a else "  (no auction?)")

        step("bids: karan 100, mrudul 150")
        bid("karan", 100)
        bid("mrudul", 150)

        step("karan and tanmay both bid 200 at the same moment (exactly one can win)")
        threads = [threading.Thread(target=bid, args=(n, 200)) for n in ("karan", "tanmay")]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        view = poller.poll()
        leader = current_leader(view)
        step(f"KILL -9 the leader {leader} (term {view[leader].term}) mid-auction")
        cluster.nodes[leader].kill()
        print(f"  {leader} killed")

        step("keep bidding: the clients' next call hits the dead node, backs off, finds the new leader")
        t0 = time.monotonic()
        bid("mrudul", 250)
        print(f"  (that bid took {time.monotonic() - t0:.2f} s including failover)")
        print(f"  new leader: {current_leader(poller.poll())}")
        bid("tanmay", 300)
        bid("karan", 300)

        step(f"restart {leader}: it replays its log from disk and catches up from the new leader")
        R.ensure_started(cluster.nodes[leader])
        caught_up = R.quiet(poller, timeout=30)
        print(f"  {leader} back; replicas agree: {caught_up}")

        step("more bids after the restart")
        bid("karan", 350)
        bid("mrudul", 400)

        wait = max(0.0, close / 1e6 - time.time()) + 2.0
        step(f"waiting {wait:.0f} s for the close (the leader's closer proposes CloseAuction)")
        time.sleep(wait)
        bid("tanmay", 500)  # too late: must be refused
        a = users["nisarg"].get_auction(AUCTION)
        print(f"  closed={a.closed} winner={a.winner!r} bids={[(b.bidder, b.amount) for b in a.bids]}")

        step("checker: every property, on what the clients saw + every node's final state")
        R.quiet(poller, timeout=30)
        dumps = R.collect_dumps(poller)
        monitor.stop()
        report, text = R.check_and_save(run_dir, history.snapshot(), dumps, R.leader_records(cluster, monitor))
        print(text)
        status = 0 if all(r.passed for r in report) else 1
        if args.keep:
            print("\n--keep: cluster still running; Ctrl+C to stop")
            while True:
                time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        monitor.stop()
        for c in users.values():
            c.close()
        cluster.stop()
        poller.close()
    return status


if __name__ == "__main__":
    sys.exit(main())
