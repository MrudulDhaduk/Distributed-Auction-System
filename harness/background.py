"""Background bidders for the four-auction demo (scripts/demo.py, Part A).

Six threads, bg1..bg6, each with ONE ClusterClient of its own. That is the
workload rule (harness/workload.py, docs/decisions.md A5): the dedupe table
keeps one (seq, response) per client_id, so one client is never used from two
threads at once.

Every write goes through a `record` callback that the demo points at
record_write + the event log, with background=True. Nothing here prints bids.

The bid rule, for each pass of the loop:
  - pick one background auction that is still open;
  - bid the current high bid plus a random 5-50, or, on about 1 bid in 10,
    bid EXACTLY the current high bid (those are refused: "not added because
    current highest bid is <amount>");
  - never send an amount this thread already sent on that auction. The
    checker's bid identity is (auction, bidder, amount), so a repeat would be
    flagged as a duplicate even though the server handled it correctly;
  - then wait 0.2-0.5 s.

The settling rule: once a background auction has closed, the thread whose user
WON it pays the winning bid, with its own client (escrow: pay -> HELD).

Stall measure (for the two leader kills): the longest time any background bid
took, among the bids that were in flight during the failover window after a
kill (stall_during).
"""

import random
import threading
import time

import grpc

from client.auction_client import ClusterClient, ClusterUnavailable
from harness.status import current_leader
from harness.workload import PASSWORD, Board

BG_USERS = ("bg1", "bg2", "bg3", "bg4", "bg5", "bg6")  # server/auth_servicer.py; password "pass"
EQUAL_BID_CHANCE = 0.1
STEP_LO, STEP_HI = 5, 50
PAUSE = (0.2, 0.5)
STALL_WINDOW = 15.0   # seconds after a kill in which a slow bid counts as "around the kill"


def choose_bid(rng, high: int, sent: set, equal_chance: float = EQUAL_BID_CHANCE) -> tuple:
    """(amount, equal_to_high). Never an amount already in `sent`."""
    if high > 0 and high not in sent and rng.random() < equal_chance:
        return high, True
    amount = high + rng.randint(STEP_LO, STEP_HI)
    while amount in sent:
        amount += 1
    return amount, False


class BackgroundBidder(threading.Thread):
    def __init__(self, user, addrs, board: Board, auctions, record, stop: threading.Event, seed, after_pay=None):
        super().__init__(name=f"bg-{user}", daemon=True)
        self.user = user
        self.addrs = list(addrs)
        self.board = board
        self.auctions = tuple(auctions)
        self.record = record          # record(name, client, op, call, **fields) -> record dict
        self.after_pay = after_pay    # after_pay(client, auction_id): e.g. read and log the escrow
        self.stop_evt = stop
        self.rng = random.Random(f"bg:{seed}:{user}")
        self.sent = {a: set() for a in self.auctions}
        self.checked = set()          # closed auctions this thread has looked at
        self.error = None

    def run(self):
        client = ClusterClient(self.addrs)
        try:
            self._login(client)
            while not self._finished():
                self._settle(client)
                open_ids = [a for a in self.board.open_ids() if a in self.auctions]
                if open_ids:
                    self._bid(client, self.rng.choice(open_ids))
                self.stop_evt.wait(self.rng.uniform(*PAUSE))
        except Exception as e:  # an actor dying silently would look like "quiet"
            self.error = f"{type(e).__name__}: {e}"
            raise
        finally:
            client.close()

    def _finished(self) -> bool:
        return self.stop_evt.is_set() or set(self.auctions) <= self.checked

    def _login(self, client):
        while not self.stop_evt.is_set():
            r = self.record(self.user, client, "login", lambda: client.login(self.user, PASSWORD))
            if r["outcome"] == "ok":
                return

    def _bid(self, client, a):
        amount, equal = choose_bid(self.rng, self.board.high_of(a), self.sent[a])
        self.sent[a].add(amount)
        r = self.record(self.user, client, "bid", lambda: client.place_bid(a, amount),
                        auction_id=a, amount=amount, equal_to_high=equal)
        self.board.observe(a, r["high_bid"])

    def _settle(self, client):
        for a in self.board.closed_ids():
            if a not in self.auctions or a in self.checked:
                continue
            try:
                auction = client.get_auction(a)
            except (ClusterUnavailable, grpc.RpcError):
                continue          # look again on the next pass
            if auction is None or not auction.closed:
                continue          # the closer has not closed it yet
            self.checked.add(a)
            if auction.winner == self.user and auction.current_high_bid > 0:
                amount = auction.current_high_bid
                self.record(self.user, client, "pay", lambda: client.pay(a, amount),
                            auction_id=a, amount=amount)
                if self.after_pay is not None:
                    try:
                        self.after_pay(client, a)
                    except (ClusterUnavailable, grpc.RpcError):
                        pass          # the escrow is still HELD; the seller's step reads it later


class StatusLine(threading.Thread):
    """Prints one line every `every` seconds until stopped:
      background: N bids, A accepted, R rejected, E errors | leader X, term T, commit C
    `records()` returns the history records; `view()` returns the monitor's latest view."""

    def __init__(self, records, view, every=5.0, out=print):
        super().__init__(name="bg-status", daemon=True)
        self.records, self.view, self.every, self.out = records, view, every, out
        self._stop_evt = threading.Event()

    def run(self):
        while not self._stop_evt.wait(self.every):
            self.out(status_line(self.records(), self.view()))

    def stop(self):
        self._stop_evt.set()
        if self.is_alive():
            self.join(timeout=5)


def background_counts(records) -> tuple:
    """(bids, accepted, rejected, errors) over the background bids. An "unknown"
    outcome (no answer came back) is an error."""
    bids = [r for r in records if r.get("background") and r.get("op") == "bid"]
    acc = sum(1 for r in bids if r["outcome"] == "ok")
    rej = sum(1 for r in bids if r["outcome"] == "rejected")
    return len(bids), acc, rej, len(bids) - acc - rej


def status_line(records, view) -> str:
    bids, acc, rej, err = background_counts(records)
    leader = current_leader(view)
    if leader is None:
        where = "leader none"
    else:
        s = view[leader]
        where = f"leader {leader}, term {s.term}, commit {s.commit_index}"
    return f"background: {bids} bids, {acc} accepted, {rej} rejected, {err} errors | {where}"


def stall_during(records, t_mono: float, window: float = STALL_WINDOW):
    """The longest latency of a background bid that was in flight at some point
    in [t_mono, t_mono + window] (monotonic seconds). None if no background bid
    was in flight then (for example, every background auction had closed)."""
    worst = None
    for r in records:
        if not r.get("background") or r.get("op") != "bid":
            continue
        end = r["mono_end"]
        start = end - r["latency"]
        if start <= t_mono + window and end >= t_mono:
            worst = r["latency"] if worst is None else max(worst, r["latency"])
    return worst
