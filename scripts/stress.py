"""Stress numbers for docs/stress-results.md, against REAL node processes.

    .venv\\Scripts\\python.exe scripts\\stress.py throughput --clients 1,2,4,8,16 --seconds 15
    .venv\\Scripts\\python.exe scripts\\stress.py failover --kills 20 --configs 0.5-1.0,0.15-0.3,0.15-0.15

throughput
    One fresh cluster (no LLM: bids never call it). For each client count N:
    N ClusterClients, each with its own auction, each placing bids one
    after another as fast as replies come back (one write in flight per
    client, the workload rule). Every bid is a real write: propose, fsync
    on the leader, AppendEntries, fsync on a follower, commit, apply.
    Reports accepted writes/s and latency p50/p95/p99/max, measured in the
    client with time.monotonic(), the first second of each step discarded.

failover (like the paper's Figure 16)
    For each election-timeout config, a fresh cluster and one client
    writing continuously. Then, `kills` times: wait until the cluster is
    settled (all up, all agree on one leader, every node within 50 entries of
    the leader's commitIndex, for 1 s; harness/run.py settled()), kill
    -9 the leader at t0, and measure
        t_elect   t0 -> some other node reports LEADER in a higher term
                  (status polled every 10 ms)
        t_write   t0 -> the client's first write acknowledged after t0 by a
                  node other than the killed one
    then restart the dead node and let it catch up. t_write includes the
    client's own backoff (0.05 s doubling to 1 s), which is what a user
    would actually wait.

Writes results to runs/stress-<time>/results.json (+ failover.csv) and
prints a summary. Numbers are whatever this machine gives.
"""

import argparse
import csv
import json
import datetime
import os
import statistics
import sys
import threading
import time

import grpc

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from client.auction_client import ClusterClient, ClusterUnavailable  # noqa: E402
from common import time_conv  # noqa: E402
from harness import run as R  # noqa: E402
from harness.procs import ROOT, LocalCluster  # noqa: E402
from harness.status import StatusPoller, current_leader  # noqa: E402
from harness.workload import PASSWORD, USERS  # noqa: E402

FAR = datetime.timedelta(days=1)
BIDS_PER_AUCTION = 1000
ROTATE_STRIDE = 1_000  # writer i uses ids i, i+1000, i+2000, ... (fewer than 1000 writers)


def ms(seconds):
    return "none" if seconds is None else f"{seconds * 1e3:.0f} ms"


def pct(values, q):
    if not values:
        return float("nan")
    s = sorted(values)
    return s[min(len(s) - 1, int(round(q / 100 * (len(s) - 1))))]


class Writer(threading.Thread):
    """One client, one auction, bids back to back. Records (start, end,
    ok, addr) in monotonic seconds; addr is the node that gave the final
    answer (the last attempt in ClusterClient.sent)."""

    def __init__(self, addrs, index, auction_id, stop, give_up_after=30.0):
        super().__init__(daemon=True)
        self.client = ClusterClient(addrs, give_up_after=give_up_after)
        self.user = USERS[index % len(USERS)]
        self.auction_id = auction_id
        self.stop = stop
        self.samples = []
        self.ready = threading.Event()

    def new_auction(self):
        close = time_conv.dt_to_micros(time_conv.now_utc() + FAR)
        r = self.client.create_auction(self.auction_id, f"stress {self.auction_id}", close)
        assert r.result.success, r.result.reason

    def setup(self):
        assert self.client.login(self.user, PASSWORD).result.success
        self.new_auction()

    def run(self):
        amount = 0
        try:
            self.setup()
            self.ready.set()
            while not self.stop.is_set():
                if amount == BIDS_PER_AUCTION:
                    # A fresh auction every 1000 bids: M1's apply() scans an
                    # auction's whole bid list on every PlaceBid (2.85 ms at
                    # 25 000 bids, measured), so one ever-growing auction would
                    # make a long run measure that scan instead of Raft.
                    self.auction_id += ROTATE_STRIDE
                    self.new_auction()
                    amount = 0
                amount += 1
                t0 = time.monotonic()
                try:
                    ok = self.client.place_bid(self.auction_id, amount).result.success
                except (ClusterUnavailable, grpc.RpcError):  # recorded as a failed write, never fatal
                    ok = False
                addr = self.client.sent[-1][0] if self.client.sent else None
                self.samples.append((t0, time.monotonic(), ok, addr))
                self.client.sent.clear()
        finally:
            self.ready.set()
            self.client.close()


def throughput(args, out):
    cluster = LocalCluster(os.path.join(out, "throughput"), llm_port=None,
                           base_peer=args.base_peer, base_client=args.base_client)
    poller = StatusPoller(cluster.peer_addrs())
    results = []
    try:
        cluster.start()
        assert R.wait_for_leader(poller, 60), "no leader"
        next_id = 1
        for n in args.clients:
            stop = threading.Event()
            writers = []
            for i in range(n):
                writers.append(Writer(cluster.client_addrs(), i, next_id, stop))
                next_id += 1
            for w in writers:
                w.start()
            for w in writers:
                w.ready.wait(60)
            begin = time.monotonic()
            time.sleep(args.seconds + 1)
            stop.set()
            for w in writers:
                w.join(timeout=40)
            lo, hi = begin + 1.0, begin + 1.0 + args.seconds
            lat = [e - s for w in writers for s, e, ok, _ in w.samples if ok and lo <= s and e <= hi]
            failed = sum(1 for w in writers for s, e, ok, _ in w.samples if not ok and lo <= s <= hi)
            row = {"clients": n, "seconds": args.seconds, "writes": len(lat), "writes_per_s": len(lat) / args.seconds,
                   "failed": failed, "p50_ms": pct(lat, 50) * 1e3, "p95_ms": pct(lat, 95) * 1e3,
                   "p99_ms": pct(lat, 99) * 1e3, "max_ms": max(lat, default=float("nan")) * 1e3}
            results.append(row)
            print(f"clients {n:3d}: {row['writes_per_s']:7.1f} writes/s  p50 {row['p50_ms']:6.1f} ms  "
                  f"p95 {row['p95_ms']:6.1f} ms  p99 {row['p99_ms']:6.1f} ms  max {row['max_ms']:6.1f} ms  "
                  f"(failed {failed})", flush=True)
    finally:
        cluster.stop()
        poller.close()
    return results


def failover_config(args, out, election):
    lo, hi = election
    heartbeat = min(0.1, lo / 3)
    cluster = LocalCluster(os.path.join(out, f"failover-{lo}-{hi}"), llm_port=None, election=election,
                           heartbeat=heartbeat, base_peer=args.base_peer, base_client=args.base_client)
    poller = StatusPoller(cluster.peer_addrs(), timeout=0.2, include_digest=False)  # T16
    rows = []
    stop = threading.Event()
    writer = None
    try:
        cluster.start()
        assert R.wait_for_leader(poller, 60), "no leader"
        writer = Writer(cluster.client_addrs(), 0, 1, stop)
        writer.start()
        writer.ready.wait(60)
        k = skipped = 0
        while k < args.kills and skipped < 10:
            if not R.settled(poller, timeout=60, stable_for=1.0):
                skipped += 1
                print(f"  kill {k + 1}: cluster not settled within 60 s; retrying", flush=True)
                continue
            view = poller.poll()
            leader = current_leader(view)
            if leader is None:  # an election began between settled() and this poll
                skipped += 1
                print(f"  kill {k + 1}: no leader right after settling; retrying", flush=True)
                continue
            old_term = view[leader].term
            dead_addr = cluster.client_addrs()[list(cluster.nodes).index(leader)]
            t0 = time.monotonic()
            cluster.nodes[leader].kill()
            t_kill_done = time.monotonic()
            new = None
            while time.monotonic() - t0 < 30:
                v = poller.poll()
                cand = [s for n, s in v.items() if s and n != leader and s.role == "LEADER" and s.term > old_term]
                if cand:
                    new = cand[0]
                    break
                time.sleep(0.01)
            t1 = time.monotonic()
            t2 = None
            while time.monotonic() - t0 < 30 and t2 is None:
                # Acknowledged after the kill BY ANOTHER NODE: a reply the old
                # leader sent in the instant before TerminateProcess landed
                # is not a failover (the smoke run measured "2 ms" that way).
                t2 = next((e for s, e, ok, addr in list(writer.samples)
                           if ok and e > t0 and addr != dead_addr), None)
                time.sleep(0.01)
            row = {"config": f"{lo}-{hi}", "kill": k + 1, "old_leader": leader, "old_term": old_term,
                   "new_leader": new.node_id if new else None, "new_term": new.term if new else None,
                   "kill_call_s": t_kill_done - t0,
                   "elect_s": (t1 - t0) if new else None, "write_s": (t2 - t0) if t2 else None}
            rows.append(row)
            with open(os.path.join(out, "failover-rows.jsonl"), "a", encoding="utf-8") as f:
                print(json.dumps(row), file=f)
            k += 1
            print(f"  kill {k:2d}: {leader} (term {old_term}) -> {row['new_leader']} (term {row['new_term']}): "
                  f"elect {ms(row['elect_s'])}, first write {ms(row['write_s'])}", flush=True)
            R.ensure_started(cluster.nodes[leader])
    finally:
        stop.set()
        # Join BEFORE stopping the cluster. A writer still inside a retry
        # when the cluster goes away would otherwise carry its token into
        # the NEXT config's fresh cluster on the same ports, and be refused
        # UNAUTHENTICATED there (correctly: that cluster never saw it).
        if writer is not None:
            writer.join(timeout=40)
        cluster.stop()
        poller.close()
    return rows


def summarize(rows, key):
    v = [r[key] * 1e3 for r in rows if r[key] is not None]
    if not v:
        return {}
    return {"n": len(v), "min_ms": min(v), "p50_ms": statistics.median(v), "p95_ms": pct(v, 95), "max_ms": max(v),
            "mean_ms": statistics.mean(v)}


def main(argv=None):
    p = argparse.ArgumentParser(description="Throughput/latency and failover numbers on real node processes.")
    p.add_argument("mode", choices=["throughput", "failover", "all"])
    p.add_argument("--clients", default="1,2,4,8,16")
    p.add_argument("--seconds", type=float, default=15.0)
    p.add_argument("--kills", type=int, default=20)
    p.add_argument("--configs", default="0.5-1.0,0.15-0.3,0.15-0.15",
                   help="election timeout min-max, seconds, comma-separated")
    p.add_argument("--base-peer", type=int, default=50081)
    p.add_argument("--base-client", type=int, default=50091)
    p.add_argument("--out", default=os.path.join(ROOT, "runs"))
    args = p.parse_args(argv)
    args.clients = [int(x) for x in args.clients.split(",")]
    out = os.path.join(args.out, f"stress-{time.strftime('%Y%m%d-%H%M%S')}")
    os.makedirs(out)
    results = {"machine": R.machine_info(), "settings": {k: v for k, v in vars(args).items()}}
    print(f"out {out}\nmachine {results['machine']}", flush=True)
    if args.mode in ("throughput", "all"):
        results["throughput"] = throughput(args, out)
    if args.mode in ("failover", "all"):
        results["failover"] = {}
        all_rows = []
        for cfg in args.configs.split(","):
            lo, hi = (float(x) for x in cfg.split("-"))
            print(f"failover, election timeout {lo}-{hi} s:", flush=True)
            rows = failover_config(args, out, (lo, hi))
            all_rows += rows
            results["failover"][cfg] = {"elect": summarize(rows, "elect_s"), "write": summarize(rows, "write_s"),
                                        "rows": rows}
            print(f"  elect {results['failover'][cfg]['elect']}\n  write {results['failover'][cfg]['write']}", flush=True)
        with open(os.path.join(out, "failover.csv"), "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(all_rows[0]) if all_rows else ["config"])
            w.writeheader()
            w.writerows(all_rows)
    R.write_json(os.path.join(out, "results.json"), results)
    print(f"wrote {out}", flush=True)


if __name__ == "__main__":
    main()
