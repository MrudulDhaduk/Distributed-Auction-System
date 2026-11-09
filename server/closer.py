"""Background poll loop that closes auctions past their close_time.

Design: one fixed-interval poll thread, not a per-auction timer and not a
next-deadline scheduler. See docs/design-notes.md for the full comparison
-- the short version is that a poll loop is restartable (nothing to
recover after a crash, the next tick just checks the clock again),
inspectable, and cheap enough that saving wakeups isn't worth the extra
moving parts.

M2 (Raft), resolving seam 1. In M1 every node's loop called
state.apply(CloseAuction(...)) itself, each stamped with its OWN clock.
Under Raft that is divergence: the nodes would close at different moments,
with different closed_at values, and a bid could be accepted on one node
and refused on another. Now (docs/decisions.md, A7):
  * the loop runs on EVERY node, but proposes only while this node is the
    leader. A follower's tick does nothing. After a failover the new
    leader's loop simply finds the overdue auctions on its next tick:
    there is no hand-over, nothing to recover.
  * it PROPOSES CloseAuction(auction_id, curr_time=now) through the log,
    with `now` read from the leader's clock, once, before proposing. Every
    node then applies that same command with that same curr_time.
  * it does not propose a close for an auction whose close is already in
    flight (proposed, not yet resolved). Without that, a 250 ms tick and a
    commit that takes longer would put two closes in the log.
  * apply() still rejects a second close ("Auction is already closed").
    That is the backstop for what the in-flight set cannot see: a close
    proposed by an earlier leader, or one whose outcome was unknown.

PlaceBid's own deadline guard (auction/state.py) rejects late bids
independently of this loop, so a slightly-late close cannot let a bid
through -- see docs/design-notes.md.
"""

import logging
import threading

from auction.state import AuctionState, CloseAuction
from common import time_conv
from raft.proposal import Outcome

DEFAULT_INTERVAL_SECONDS = 0.25
STOP_JOIN_SECONDS = 5.0

log = logging.getLogger("server.closer")


def due_auction_ids(state: AuctionState, now) -> list:
    """Ids of auctions whose close_time is past `now` and that aren't
    closed yet, as far as this node has applied.

    The clock is passed in, never read here, so a test can drive this with a
    fixed timestamp and no sleeping. The poll thread below is the only
    caller that ever passes a real clock reading.

    The scan is deliberately lock-free and advisory: `state.lock` belongs to
    AuctionState and is taken only by apply(). An entry can go stale between
    being scanned and being applied -- apply() re-checks both conditions
    under its own lock, so a stale entry just comes back as a rejected
    Result.

    list() around .items() because a dict raises RuntimeError if it is
    resized mid-iteration, and a concurrent CreateAuction inserts a key.
    """
    return [
        auction_id
        for auction_id, auction in list(state.auctions.items())
        if not auction["closed"] and now > auction["close_time"]
    ]


class AuctionCloser:
    """Owns the poll thread. The thread itself stays a thin loop: read the
    clock once per iteration, call tick(), wait. All the actual logic lives
    in tick() so it can be tested without a thread.

    replicator: this node's RaftReplicator (or LocalReplicator in a cluster
    of one). Only is_leader() and the non-blocking propose() are used: the
    loop never waits for a commit, it looks at its handles on the next tick.
    """

    def __init__(self, state: AuctionState, replicator, interval_seconds: float = DEFAULT_INTERVAL_SECONDS):
        self._state = state
        self._replicator = replicator
        self._interval_seconds = interval_seconds
        # auction_id -> Proposal of the CloseAuction this node proposed and
        # has not seen resolved yet. Touched only by the poll thread (or the
        # test that calls tick()).
        self._in_flight: dict = {}
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="auction-closer", daemon=True)

    def tick(self, now) -> list:
        """One pass. Returns the auction ids it proposed a close for."""
        # Forget resolved handles, whatever the outcome. APPLIED: the
        # auction is closed now and no longer due. Anything else: the
        # outcome is unknown, and if the auction is still open on a later
        # tick (on whichever node leads then) it is proposed again.
        self._in_flight = {aid: p for aid, p in self._in_flight.items() if not p.wait(0)}

        if not self._replicator.is_leader():
            return []

        proposed = []
        for auction_id in due_auction_ids(self._state, now):
            if auction_id in self._in_flight:
                continue
            proposal = self._replicator.propose(CloseAuction(auction_id=auction_id, curr_time=now))
            if proposal.outcome is Outcome.NOT_LEADER:
                break  # lost leadership between is_leader() and here
            self._in_flight[auction_id] = proposal
            proposed.append(auction_id)
        if proposed:
            log.info("closer: proposed CloseAuction for %s", proposed)
        return proposed

    def start(self):
        self._thread.start()

    def stop(self):
        """Signal the loop to exit and wait for the thread to join.

        Bounded wait: a join() with no timeout would hang shutdown forever
        if the loop ever wedged. The thread is a daemon, so if it does not
        come back in time the process can still exit.
        """
        self._stop.set()
        self._thread.join(timeout=STOP_JOIN_SECONDS)

    def _run(self):
        while not self._stop.is_set():
            try:
                self.tick(time_conv.now_utc())
            except Exception:
                # One bad tick (e.g. the node stopping under us) must not
                # kill the loop for the life of the process.
                log.exception("closer tick failed")
            self._stop.wait(self._interval_seconds)
