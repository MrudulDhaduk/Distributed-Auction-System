"""Chaos run against a REAL cluster: three `server.serve` processes with data
dirs, plus the LLM server, a concurrent client workload, a seeded fault
schedule, and the property checker at the end.

    .venv\\Scripts\\python.exe scripts\\chaos.py --duration 600
    .venv\\Scripts\\python.exe scripts\\chaos.py --seed 1234 --duration 120 --no-llm

Prints the seed first. The same seed gives the same fault SCHEDULE (times,
actions, durations, follower picks), not the same run: who is leader when a
fault fires, and every thread interleaving, are not replayable
(harness/schedule.py).

Output: runs/chaos-<seed>-<time>/
    history.jsonl dumps.json leaders.json   the checker's inputs
    report.txt                              PASS/FAIL per property
    summary.json                            seed, settings, schedule, counts
    events.log faults.log                   what the monitor saw, what we did
    logs/<node>.log logs/llm.log            every process's own log
    data/<node>/                            the nodes' FileStorage
Re-check later with: python -m checker runs/chaos-...

Exit code 0 only if every property passed.
"""

import argparse
import os
import secrets
import sys
import threading
import time
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from harness import run as R  # noqa: E402
from harness.procs import ROOT, LocalCluster  # noqa: E402
from harness.schedule import describe, make_schedule  # noqa: E402
from harness.status import StatusMonitor, StatusPoller  # noqa: E402
from harness.workload import Workload  # noqa: E402


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--seed", type=int, default=None, help="default: random (printed)")
    p.add_argument("--duration", type=float, default=600.0, help="seconds of faults + workload")
    p.add_argument("--sellers", type=int, default=1)
    p.add_argument("--bidders", type=int, default=4)
    p.add_argument("--racers", type=int, default=3)
    p.add_argument("--close-min", type=float, default=15.0)
    p.add_argument("--close-max", type=float, default=60.0)
    p.add_argument("--give-up", type=float, default=6.0,
                   help="client give_up_after, seconds. Below the longest outage (8 s) on purpose, so "
                        "majority-loss episodes produce real 'unknown' outcomes for the checker")
    p.add_argument("--no-llm", action="store_true", help="do not start the LLM server")
    p.add_argument("--llm-port", type=int, default=50059)
    p.add_argument("--base-peer", type=int, default=50081)
    p.add_argument("--base-client", type=int, default=50091)
    p.add_argument("--out", default=os.path.join(ROOT, "runs"))
    args = p.parse_args(argv)

    seed = args.seed if args.seed is not None else secrets.randbelow(10**6)
    run_dir = os.path.join(args.out, f"chaos-{seed}-{time.strftime('%Y%m%d-%H%M%S')}")
    os.makedirs(run_dir)
    schedule = make_schedule(seed, args.duration)
    print(f"SEED {seed}")
    print(f"run dir {run_dir}")
    print(f"schedule ({len(schedule)} faults):\n{describe(schedule)}", flush=True)

    lock = threading.Lock()
    faults_log = open(os.path.join(run_dir, "faults.log"), "w", encoding="utf-8")
    events_log = open(os.path.join(run_dir, "events.log"), "w", encoding="utf-8")
    t0 = time.monotonic()

    def say(line, f=faults_log):
        line = f"[{time.monotonic() - t0:7.2f}s] {line}"
        with lock:
            print(line, flush=True)
            f.write(line + "\n")
            f.flush()

    cluster = LocalCluster(run_dir, base_peer=args.base_peer, base_client=args.base_client,
                           llm_port=None if args.no_llm else args.llm_port)
    poller = StatusPoller(cluster.peer_addrs())
    monitor = StatusMonitor(poller, interval=0.05, on_event=lambda e: say(e, events_log))
    workload = None
    applied, dumps, is_quiet, history = [], {}, False, []
    try:
        cluster.start()
        monitor.start()
        leader = R.wait_for_leader(poller, 60)
        if not leader:
            say("no leader within 60 s; giving up")
            return 2
        say(f"leader {leader}; starting workload")
        workload = Workload(cluster.client_addrs(), seed, sellers=args.sellers, bidders=args.bidders,
                            racers=args.racers, close_after=(args.close_min, args.close_max),
                            give_up_after=args.give_up).start()
        start = time.monotonic()
        for fault in schedule:
            time.sleep(max(0.0, start + fault.at - time.monotonic()))
            done = R.inject(cluster, poller, fault, say)
            applied.append({"at": round(time.monotonic() - start, 2), "fault": fault.action,
                            "planned_at": round(fault.at, 2), "targets": done})
            time.sleep(fault.down_for)
            R.heal(cluster, done, say)
        time.sleep(max(0.0, start + args.duration - time.monotonic()))

        say("workload stopping")
        stuck = workload.stop()
        if stuck:
            say(f"actors still running after stop: {stuck}")
        cluster.heal()
        last_close = max(workload.board.close_time.values(), default=0) / 1e6
        wait_close = max(0.0, last_close - time.time()) + 2.0
        say(f"waiting {wait_close:.1f}s for the last auction's close, then for quiet")
        time.sleep(wait_close)
        is_quiet = R.quiet(poller, timeout=90)
        say("cluster quiet" if is_quiet else "cluster did NOT go quiet within 90 s")
        dumps = R.collect_dumps(poller)
    finally:
        monitor.stop()
        if workload is not None:
            workload.stopping.set()
            history = workload.history.snapshot()
        cluster.stop()  # always: never leave node processes behind
        poller.close()

    leaders = R.leader_records(cluster, monitor)
    report, text = R.check_and_save(run_dir, history, dumps, leaders)

    counts = Counter((r["op"], r["outcome"]) for r in history)
    summary = {
        "seed": seed, "duration": args.duration, "settings": vars(args), "machine": R.machine_info(),
        "faults_planned": len(schedule), "faults_applied": applied,
        "ops": {f"{op}/{outcome}": n for (op, outcome), n in sorted(counts.items())},
        "races": sum(1 for r in history if r.get("race")),
        "leader_terms_seen": len({rec["term"] for rec in leaders}),
        "workload_errors": workload.errors if workload else [],
        "passed": all(r.passed for r in report),
        "quiet": is_quiet,
    }
    R.write_json(os.path.join(run_dir, "summary.json"), summary)
    faults_log.close()
    events_log.close()
    print("\n" + "\n".join(f"{k:>14}: {v}" for k, v in summary["ops"].items()))
    print(f"races {summary['races']}, leader terms seen {summary['leader_terms_seen']}, "
          f"faults {len(applied)}, workload errors {len(summary['workload_errors'])}")
    print(f"\nSEED {seed}\n{text}")
    return 0 if summary["passed"] and is_quiet and not summary["workload_errors"] else 1


if __name__ == "__main__":
    sys.exit(main())
