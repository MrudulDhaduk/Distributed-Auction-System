"""The scripted demo (run by scripts/demo.ps1, which also opens the dashboard).

  1. start the LLM server and three node processes (fresh data dirs); warm the model
  2. log in a seller and three bidders (one ClusterClient each)
  3. create auction 1 (the hero: the LLM writes its description), then auctions 2-4
     (background auctions, each with an LLM description), right after it
  4. start six BACKGROUND bidders (bg1..bg6, one client each) that bid on 2-4
     until they close, about 15-25 bids a second in all
  5. bid, including two EQUAL bids at once (exactly one may win) on the hero
  6. ask the LLM assistant a question; the leader sends it the live auction
  7. kill -9 the leader mid-auction, under background load; keep bidding
  8. restart the dead node; it catches up by replaying its log + AppendEntries
  9. bid again; the hero closes LAST; the leader's worker has the LLM write a
     result summary for every auction
 10. the hero winner pays into ESCROW; kill -9 the leader AGAIN: the new leader
     shows the same escrow, and the seller releases it
 11. the background winners paid with their own clients; the seller releases
     auctions 2 and 4 and REFUNDS auction 3
 12. price trends over the closed auctions (statistics in code + the LLM's words)
 13. background stall times at both kills, then the property checker on
     everything the clients saw: PASS/FAIL each

    .venv\\Scripts\\python.exe scripts\\demo.py [--pause 2] [--keep] [--no-llm]

--pause: seconds between steps, so a human can watch the dashboard.
--keep:  leave the cluster running at the end (Ctrl+C to stop it).
--no-llm: do not start the LLM server; every LLM step then shows its clear
          "assistant unavailable" answer and the auction carries on.

Every step prints how long it took, so the video can be planned from a real run.
The whole run is recorded to <run_dir>\\events.jsonl (harness/events.py).
"""

import argparse
import os
import sys
import threading
import time

import grpc

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from client.auction_client import ClusterClient  # noqa: E402
from common import time_conv  # noqa: E402
from harness import run as R  # noqa: E402
from harness.background import BG_USERS, BackgroundBidder, StatusLine, stall_during  # noqa: E402
from harness.events import EventLog, StatusThread, election_from_text  # noqa: E402
from harness.procs import ROOT, LocalCluster  # noqa: E402
from harness.status import StatusMonitor, StatusPoller, current_leader  # noqa: E402
from harness.workload import Board, PASSWORD, History, record_write  # noqa: E402
from server.llm_client import LLMClient  # noqa: E402

AUCTION = 1                       # the hero
AUCTIONS = (1, 2, 3, 4)
# Auction 1 closes HERO_CLOSE_SECONDS after the hero create call starts; the
# background auctions close at these offsets from the same moment. So the hero
# closes last, after the background ones have closed.
HERO_CLOSE_SECONDS = 75   # later than the background closes (35/45/55 s), with room for the hero's bids
BACKGROUND = {2: ("film camera", 35, {"brand": "Pentax", "era": "1970s", "condition": "working"}),
              3: ("pocket watch", 45, {"brand": "Omega", "era": "1920s", "condition": "restored"}),
              4: ("vinyl record player", 55, {"brand": "Dual", "era": "1980s", "condition": "working"})}
SUMMARY_WAIT_SECONDS = 60
KILL_STALL_WINDOW = 15.0


def mono_of_wall(wall_seconds: float) -> float:
    """The time.monotonic() value that corresponds to a wall-clock time (epoch seconds)."""
    return time.monotonic() - (time.time() - wall_seconds)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Scripted auction with background bidders, leader kills, then the checker.")
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
    history = History()
    users = {}
    t_start = time.monotonic()
    timings = []   # (step, seconds): what the video planning reads
    kill_marks = []  # (node, monotonic time of the kill): for the stall lines
    log = EventLog(os.path.join(run_dir, "events.jsonl"), t0=t_start)

    def on_monitor_event(text):
        found = election_from_text(text)
        if found:
            log.write("election", leader=found[0], term=found[1])

    monitor = StatusMonitor(poller, interval=0.05, on_event=on_monitor_event)
    status_thread = StatusThread(log, monitor, list(cluster.nodes))
    bg_stop = threading.Event()
    bg_threads = []
    status_line = None

    def step(text):
        log.write("step", text=text)
        print(f"\n=== [{time.monotonic() - t_start:6.1f}s] {text}", flush=True)
        time.sleep(args.pause)

    def timed(label, fn):
        t0 = time.monotonic()
        out = fn()
        timings.append((label, time.monotonic() - t0))
        print(f"  ({label}: {timings[-1][1]:.2f} s)", flush=True)
        return out

    def record(name, client, op, call, background, **fields):
        """ONE client write: into the shared history, and into the event log."""
        r = record_write(history, name, name, client, op, call, background=background, **fields)
        log.write("action", user=r["user"], op=op, auction_id=fields.get("auction_id"),
                  amount=fields.get("amount"), outcome=r["outcome"], reason=r["reason"],
                  high_bid=r["high_bid"] if op == "bid" else None,
                  high_bidder=r["high_bidder"] if op == "bid" else None, background=background)
        return r

    def write(name, op, call, **fields):
        """A hero or seller write: recorded, and printed."""
        r = record(name, users[name], op, call, False, **fields)
        extra = f" [high {r['high_bid']} by {r['high_bidder']}]" if op == "bid" else ""
        if op in ("pay", "release", "refund"):
            extra = f" [reference {r['reference'] or '-'}]"
        print(f"  {name:7s} {op:7s} {fields.get('amount', '')!s:>4}  -> {r['outcome']:8s} {r['reason']}{extra}",
              flush=True)
        return r

    def bid(name, amount):
        return write(name, "bid", lambda: users[name].place_bid(AUCTION, amount), auction_id=AUCTION, amount=amount)

    def log_escrow_from(client, auction_id):
        """Read the escrow record through `client` and log it, without printing."""
        e = client.get_escrow(auction_id).escrow
        log.write("escrow", auction_id=auction_id, status=e.status, amount=e.amount, payer=e.payer,
                  seller=e.seller, payment_ref=e.payment_ref, settlement_ref=e.settlement_ref)
        return e

    def log_escrow(name, auction_id):
        return log_escrow_from(users[name], auction_id)

    def show_escrow(name, label, auction_id=AUCTION):
        e = log_escrow(name, auction_id)
        print(f"  {label}: status {e.status}, amount {e.amount}, payer {e.payer or '-'}, seller {e.seller}, "
              f"payment_ref {e.payment_ref or '-'}, settlement_ref {e.settlement_ref or '-'}", flush=True)
        return e

    def kill_leader(why):
        view = poller.poll()
        leader = current_leader(view)
        step(f"KILL -9 the leader {leader} (term {view[leader].term}) {why}")
        log.write("kill", node=leader, term=view[leader].term)
        kill_marks.append((leader, time.monotonic()))
        cluster.nodes[leader].kill()
        print(f"  {leader} killed")
        return leader

    def restart(node_id):
        step(f"restart {node_id}: it replays its log from disk and catches up from the new leader")
        log.write("restart", node=node_id)
        R.ensure_started(cluster.nodes[node_id])
        caught_up = R.quiet(poller, timeout=60)
        print(f"  {node_id} back; replicas agree: {caught_up}")

    def record_auction(a):
        """Read an auction back and log its "auction" event (item, close, description)."""
        auction = users["nisarg"].get_auction(a)
        if auction is not None:
            log.write("auction", auction_id=a, item=auction.item, close_time=auction.close_time,
                      description=auction.description)
        return auction

    status = 1
    try:
        step(f"starting LLM + 3 nodes (logs and data under {run_dir})")
        cluster.start()
        monitor.start()
        status_thread.start()
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

        origin_us = time_conv.dt_to_micros(time_conv.now_utc())   # the hero's close and the background closes count from here
        hero_close = origin_us + int(HERO_CLOSE_SECONDS * 1e6)
        step(f"nisarg creates auction {AUCTION}, closing in {HERO_CLOSE_SECONDS} s (the LLM writes the description)")
        timed("create auction incl. LLM description", lambda: write(
            "nisarg", "create", lambda: users["nisarg"].create_auction(
                AUCTION, "vintage clock", hero_close, {"brand": "Seiko", "era": "1970s", "condition": "running"}),
            auction_id=AUCTION, item="vintage clock", close_time=hero_close))
        a = record_auction(AUCTION)
        print(f"  description (replicated as data): {a.description!r}" if a else "  (no auction?)")

        board = Board()
        close_of = {AUCTION: hero_close}
        step("nisarg creates auctions 2-4 right after the hero: background auctions (the LLM writes each description)")
        for aid, (item, offset, attrs) in sorted(BACKGROUND.items()):
            close = origin_us + int(offset * 1e6)
            close_of[aid] = close
            write("nisarg", "create", lambda aid=aid, item=item, close=close, attrs=attrs:
                  users["nisarg"].create_auction(aid, item, close, attrs),
                  auction_id=aid, item=item, close_time=close)
            board.add(aid, close)
            record_auction(aid)

        step(f"six background bidders bg1-bg6 log in and bid on auctions 2-4 until they close "
             f"(not printed one by one; a status line every 5 s)")
        for name in BG_USERS:
            # Each bidder makes and closes its own ClusterClient (one per thread).
            bg_threads.append(BackgroundBidder(
                name, cluster.client_addrs(), board, sorted(BACKGROUND),
                record=lambda n, client, op, call, **f: record(n, client, op, call, True, **f),
                stop=bg_stop, seed=run_dir, after_pay=log_escrow_from))
        for thread in bg_threads:
            thread.start()
        status_line = StatusLine(lambda: history.snapshot(), monitor.view, every=5.0)
        status_line.start()

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
        log.write("faq", question=q, answer=answer.answer if answer.result.success else None)

        leader = kill_leader("mid-auction, under background load")

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

        wait = max(0.0, hero_close / 1e6 - time.time()) + 2.0
        step(f"waiting {wait:.0f} s for the closes: auctions 2-4, then auction 1 last (the closer proposes CloseAuction)")
        time.sleep(wait)
        bid("tanmay", 500)  # too late: must be refused
        a = record_auction(AUCTION)
        print(f"  closed={a.closed} winner={a.winner!r} bids={[(b.bidder, b.amount) for b in a.bids]}")
        for aid in AUCTIONS:
            auction = users["nisarg"].get_auction(aid)
            if auction is not None and auction.closed:
                # stamped with the scheduled close time, not the time the demo noticed it
                log.write("close", at=mono_of_wall(auction.close_time / 1e6),
                          auction_id=aid, winner=auction.winner, winning_bid=auction.current_high_bid)
        # The background bidders stop by themselves once their auctions are closed and
        # settled. Give them time to do that before the stop signal, so no winner's
        # payment is cut off.
        for thread in bg_threads:
            thread.join(timeout=60)
        bg_stop.set()
        for thread in bg_threads:
            thread.join(timeout=5)
        status_line.stop()
        bg_alive = [t.name for t in bg_threads if t.is_alive()]
        print(f"  background bidders stopped; still running: {bg_alive or 'none'}")

        if not args.no_llm:
            step("the leader's summary workers ask the model for the four result summaries and replicate the texts")
            for aid in AUCTIONS:
                # This is when the summary was first READ back, counted from the auction's close.
                # It is an upper bound on when the text was written: the demo only looks when it
                # asks. A background auction that closed long ago shows a large number for that reason.
                close_wall = users["nisarg"].get_auction(aid).close_time / 1e6
                deadline = time.monotonic() + SUMMARY_WAIT_SECONDS
                summary = ""
                while time.monotonic() < deadline and not summary:
                    auction = users["nisarg"].get_auction(aid)
                    summary = auction.summary if auction else ""
                    if not summary:
                        time.sleep(0.5)
                read_after = max(0.0, time.time() - close_wall)
                timings.append((f"close -> summary first read (auction {aid})", read_after))
                print(f"  auction {aid} summary read back {read_after:.1f} s after its close: {summary!r}" if summary
                      else f"  auction {aid}: no summary within {SUMMARY_WAIT_SECONDS} s")
                if summary:
                    log.write("summary", auction_id=aid, text=summary)

        a = users["nisarg"].get_auction(AUCTION)
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

        step("background winners paid with their own clients; the seller releases auctions 2 and 4 and refunds 3")
        paid = {r["auction_id"]: r for r in history.snapshot() if r.get("background") and r["op"] == "pay"}
        for aid in sorted(BACKGROUND):
            if aid not in paid:
                print(f"  auction {aid}: nobody won it, so there is nothing to settle")
                continue
            r = paid[aid]
            print(f"  {r['user']:5s} pay {r['amount']:>4} for auction {aid} -> {r['outcome']:8s} {r['reason']} "
                  f"[reference {r['reference'] or '-'}]", flush=True)
        for aid, op in ((2, "release"), (3, "refund"), (4, "release")):
            if aid not in paid or paid[aid]["outcome"] != "ok":
                continue
            call = users["nisarg"].release_escrow if op == "release" else users["nisarg"].refund_escrow
            write("nisarg", op, lambda aid=aid, call=call: call(aid), auction_id=aid)
            log_escrow("nisarg", aid)

        restart(leader)

        step("the escrow of every auction, at the end")
        for aid in AUCTIONS:
            show_escrow("nisarg", f"auction {aid}", aid)

        step("price trends over the closed auctions: statistics in code, wording by the model")
        trends = timed("price trends", lambda: users["karan"].price_trends())
        s = trends.stats
        print(f"  closed auctions {s.auctions}, with at least one bid {s.with_bids}, "
              f"lowest winning bid {s.lowest_winning_bid}, highest winning bid {s.highest_winning_bid}, "
              f"total bids {s.total_bids}")
        print(f"  summary: {trends.summary!r}" if trends.result.success else f"  ({trends.result.reason})")
        log.write("trends", stats={f.name: getattr(s, f.name) for f in s.DESCRIPTOR.fields},
                  summary=trends.summary if trends.result.success else None)

        step("background stall at each leader kill: the slowest background bid in flight around the kill")
        records = history.snapshot()
        for node, t_kill in kill_marks:
            stall = stall_during(records, t_kill, KILL_STALL_WINDOW)
            if stall is None:
                print(f"  background stall during failover ({node} killed): n/a -- no background bid was "
                      f"in flight then (every background auction had closed)")
            else:
                print(f"  background stall during failover ({node} killed): {stall:.2f} s")

        step("checker: every property, on what the clients saw + every node's final state")
        R.quiet(poller, timeout=60)
        dumps = R.collect_dumps(poller)
        status_thread.stop()
        monitor.stop()
        report, text = R.check_and_save(run_dir, history.snapshot(), dumps, R.leader_records(cluster, monitor))
        print(text)
        log.write("checker", properties=[{"name": r.name, "passed": r.passed, "checked": r.checked}
                                         for r in report],
                  all_passed=all(r.passed for r in report))
        print("\ntimings of the slow steps:")
        for label, seconds in timings:
            print(f"  {label:42s} {seconds:6.2f} s")
        print(f"  {'whole run (including the pauses)':42s} {time.monotonic() - t_start:6.2f} s")
        status = 0 if all(r.passed for r in report) else 1
        if args.keep:
            print("\n--keep: cluster still running; Ctrl+C to stop it")
            while True:
                time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        bg_stop.set()
        if status_line is not None:
            status_line.stop()
        status_thread.stop()
        monitor.stop()
        log.close()
        for c in users.values():
            c.close()
        cluster.stop()
        poller.close()
    return status


if __name__ == "__main__":
    sys.exit(main())
