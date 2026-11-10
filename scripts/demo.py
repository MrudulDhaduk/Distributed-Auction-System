"""The scripted demo (run by scripts/demo.ps1, which also opens the dashboard).

  1. start the LLM server and three node processes (fresh data dirs); warm the model
  2. log in a seller and three bidders (one ClusterClient each)
  3. create an auction (with attributes: the LLM writes its description)
  4. bid, including two EQUAL bids at once (exactly one may win)
  5. ask the LLM assistant a question; the leader sends it the live auction
  6. kill -9 the leader mid-auction; keep bidding (clients find the new leader)
  7. restart the dead node; it catches up by replaying its log + AppendEntries
  8. bid again, wait for the close; the leader's worker has the LLM write the
     result summary, which is replicated like any other command
  9. the winner pays into ESCROW; kill -9 the leader AGAIN: the new leader shows
     the same escrow, and the seller releases it
 10. price trends over the closed auctions (statistics in code + the LLM's words)
 11. run the property checker on everything the clients saw: PASS/FAIL each

    .venv\\Scripts\\python.exe scripts\\demo.py [--pause 2] [--keep] [--no-llm]

--pause: seconds between steps, so a human can watch the dashboard.
--keep:  leave the cluster running at the end (Ctrl+C to stop it).
--no-llm: do not start the LLM server; every LLM step then shows its clear
          "assistant unavailable" answer and the auction carries on.

Every step prints how long it took, so the video can be planned from a real run.
"""

import argparse
import datetime
import os
import sys
import threading
import time

import grpc

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from client.auction_client import ClusterClient
from common import time_conv
from harness import run as R
from harness.procs import ROOT, LocalCluster
from harness.status import StatusMonitor, StatusPoller, current_leader
from harness.workload import PASSWORD, History, record_write
from server.llm_client import LLMClient

AUCTION = 1
CLOSE_SECONDS = 60
SUMMARY_WAIT_SECONDS = 60


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
    t_start = time.monotonic()
    timings = []   # (step, seconds): what the video planning reads

    def step(text):
        print(f"\n=== [{time.monotonic() - t_start:6.1f}s] {text}", flush=True)
        time.sleep(args.pause)

    def timed(label, fn):
        t0 = time.monotonic()
        out = fn()
        timings.append((label, time.monotonic() - t0))
        print(f"  ({label}: {timings[-1][1]:.2f} s)", flush=True)
        return out

    def write(name, op, call, **fields):
        c = users[name]
        r = record_write(history, name, name, c, op, call, **fields)
        extra = f" [high {r['high_bid']} by {r['high_bidder']}]" if op == "bid" else ""
        if op in ("pay", "release", "refund"):
            extra = f" [reference {r['reference'] or '-'}]"
        print(f"  {name:7s} {op:7s} {fields.get('amount', '')!s:>4}  -> {r['outcome']:8s} {r['reason']}{extra}",
              flush=True)
        return r

    def bid(name, amount):
        return write(name, "bid", lambda: users[name].place_bid(AUCTION, amount), auction_id=AUCTION, amount=amount)

    def show_escrow(name, label):
        e = users[name].get_escrow(AUCTION).escrow
        print(f"  {label}: status {e.status}, amount {e.amount}, payer {e.payer or '-'}, seller {e.seller}, "
              f"payment_ref {e.payment_ref or '-'}, settlement_ref {e.settlement_ref or '-'}", flush=True)
        return e

    def kill_leader(why):
        view = poller.poll()
        leader = current_leader(view)
        step(f"KILL -9 the leader {leader} (term {view[leader].term}) {why}")
        cluster.nodes[leader].kill()
        print(f"  {leader} killed")
        return leader

    def restart(node_id):
        step(f"restart {node_id}: it replays its log from disk and catches up from the new leader")
        R.ensure_started(cluster.nodes[node_id])
        caught_up = R.quiet(poller, timeout=60)
        print(f"  {node_id} back; replicas agree: {caught_up}")

    status = 1
    try:
        step(f"starting LLM + 3 nodes (logs and data under {run_dir})")
        cluster.start()
        monitor.start()
        leader = R.wait_for_leader(poller, 60)
        print(f"  leader: {leader}")

        if not args.no_llm:
            step("warm the model: the first call after Ollama loads the weights is slow")

            def warm():
                client = LLMClient(f"127.0.0.1:{args.llm_port}", deadline=120)
                for _ in range(20):  # the LLM process may still be binding its port
                    try:
                        client.get_llm_answer("demo-warmup", "What is an auction?", {})
                        return
                    except grpc.RpcError as e:
                        if e.code() != grpc.StatusCode.UNAVAILABLE:
                            break
                        time.sleep(0.5)
                print("  warm-up failed; the LLM steps will show the clear error and the auction carries on")

            timed("model warm-up", warm)

        step("log in: nisarg (seller), karan, mrudul, tanmay -- one client each")
        for name in ("nisarg", "karan", "mrudul", "tanmay"):
            users[name] = ClusterClient(cluster.client_addrs())
            write(name, "login", lambda n=name: users[n].login(n, PASSWORD))

        step(f"nisarg creates auction {AUCTION}, closing in {CLOSE_SECONDS} s (the LLM writes the description)")
        close = time_conv.dt_to_micros(time_conv.now_utc() + datetime.timedelta(seconds=CLOSE_SECONDS))
        timed("create auction incl. LLM description", lambda: write(
            "nisarg", "create", lambda: users["nisarg"].create_auction(
                AUCTION, "vintage clock", close, {"brand": "Seiko", "era": "1970s", "condition": "running"}),
            auction_id=AUCTION, item="vintage clock", close_time=close))
        a = users["nisarg"].get_auction(AUCTION)
        print(f"  description (replicated as data): {a.description!r}" if a else "  (no auction?)")

        step("bids: karan 100, mrudul 150")
        bid("karan", 100)
        bid("mrudul", 150)

        step("karan and tanmay both bid 200 at the same moment (exactly one can win)")
        threads = [threading.Thread(target=bid, args=(n, 200)) for n in ("karan", "tanmay")]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        step("tanmay asks the assistant about the live auction (the leader sends the model its current state)")
        q = "Can I bid one more than the current high bid?"
        answer = timed("assistant FAQ", lambda: users["tanmay"].ask_assistant(q, auction_id=AUCTION))
        print(f"  Q: {q}")
        print(f"  A: {answer.answer!r}" if answer.result.success else f"  (no answer: {answer.result.reason})")

        leader = kill_leader("mid-auction")

        step("keep bidding: the clients' next call hits the dead node, backs off, finds the new leader")
        t0 = time.monotonic()
        bid("mrudul", 250)
        print(f"  (that bid took {time.monotonic() - t0:.2f} s including failover)")
        print(f"  new leader: {current_leader(poller.poll())}")
        bid("tanmay", 300)
        bid("karan", 300)

        restart(leader)

        step("more bids after the restart")
        bid("karan", 350)
        bid("mrudul", 400)

        wait = max(0.0, close / 1e6 - time.time()) + 2.0
        step(f"waiting {wait:.0f} s for the close (the leader's closer proposes CloseAuction)")
        time.sleep(wait)
        bid("tanmay", 500)  # too late: must be refused
        a = users["nisarg"].get_auction(AUCTION)
        print(f"  closed={a.closed} winner={a.winner!r} bids={[(b.bidder, b.amount) for b in a.bids]}")

        if not args.no_llm:
            step("the leader's summary worker asks the model for the result summary and replicates the text")
            closed_at = time.monotonic()
            deadline = closed_at + SUMMARY_WAIT_SECONDS
            summary = ""
            while time.monotonic() < deadline and not summary:
                a = users["nisarg"].get_auction(AUCTION)
                summary = a.summary if a else ""
                if not summary:
                    time.sleep(0.5)
            timings.append(("close -> summary visible", time.monotonic() - closed_at))
            print(f"  summary after {timings[-1][1]:.1f} s: {summary!r}" if summary
                  else f"  no summary within {SUMMARY_WAIT_SECONDS} s")

        winner, amount = a.winner, a.current_high_bid
        step(f"{winner} (the winner) pays {amount} into ESCROW; tanmay (not the winner) tries and is refused")
        write("tanmay", "pay", lambda: users["tanmay"].pay(AUCTION, amount), auction_id=AUCTION, amount=amount)
        timed("payment", lambda: write(winner, "pay", lambda: users[winner].pay(AUCTION, amount),
                                       auction_id=AUCTION, amount=amount))
        before = show_escrow("nisarg", "escrow now")

        leader = kill_leader("AFTER the payment")

        step("the new leader shows the SAME escrow; then nisarg (the seller) releases it")
        after = show_escrow("nisarg", "escrow on the new leader")
        print(f"  same record as before the kill: {after == before}")
        write("nisarg", "release", lambda: users["nisarg"].release_escrow(AUCTION), auction_id=AUCTION)
        show_escrow(winner, "escrow (seen by the winner)")

        restart(leader)

        step("price trends over the closed auctions: statistics in code, wording by the model")
        trends = timed("price trends", lambda: users["karan"].price_trends())
        s = trends.stats
        print(f"  auctions {s.auctions}, with bids {s.with_bids}, lowest {s.lowest_winning_bid}, "
              f"highest {s.highest_winning_bid}, total bids {s.total_bids}")
        print(f"  summary: {trends.summary!r}" if trends.result.success else f"  ({trends.result.reason})")

        step("checker: every property, on what the clients saw + every node's final state")
        R.quiet(poller, timeout=60)
        dumps = R.collect_dumps(poller)
        monitor.stop()
        report, text = R.check_and_save(run_dir, history.snapshot(), dumps, R.leader_records(cluster, monitor))
        print(text)
        print("\ntimings of the slow steps:")
        for label, seconds in timings:
            print(f"  {label:42s} {seconds:6.2f} s")
        print(f"  {'whole run (including the pauses)':42s} {time.monotonic() - t_start:6.2f} s")
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
