"""Background poll loop that fires CloseAuction for auctions past their
close_time.

Design: one fixed-interval poll thread, not a per-auction timer and not a
next-deadline scheduler. See docs/design-notes.md for the full comparison
-- the short version is that a poll loop is restartable (nothing to
recover after a crash, the next tick just checks the clock again),
inspectable, and cheap enough that saving wakeups isn't worth the extra
moving parts.

PlaceBid's own deadline guard (auction/state.py) rejects late bids
independently of this loop, so a slightly-late close cannot let a bid
through -- see docs/design-notes.md.
"""

import threading

from auction.state import AuctionState, CloseAuction
from common import time_conv

DEFAULT_INTERVAL_SECONDS = 0.25
STOP_JOIN_SECONDS = 5.0


def close_expired(state: AuctionState, now):
    """Close every auction whose close_time is past `now` and that isn't
    already closed. Returns the [(auction_id, Result), ...] pairs for each
    CloseAuction attempted.

    The clock is passed in, never read here, so a test can drive this with a
    fixed timestamp and no sleeping. The poll thread below is the only
    caller that ever passes a real clock reading.

    The scan below is deliberately lock-free and advisory: `state.lock`
    belongs to AuctionState and is taken only by apply(). An entry can go
    stale between being scanned and being applied -- apply() re-checks both
    conditions under its own lock, so a stale entry just comes back as a
    rejected Result. Same argument as the advisory high-bid read in
    AuctionServicer.PlaceBid.

    list() around .items() because a dict raises RuntimeError if it is
    resized mid-iteration, and a concurrent CreateAuction inserts a key.
    """
    due = [
        auction_id
        for auction_id, auction in list(state.auctions.items())
        if not auction["closed"] and now > auction["close_time"]
    ]

    return [
        (auction_id, state.apply(CloseAuction(auction_id=auction_id, curr_time=now)))
        for auction_id in due
    ]


class AuctionCloser:
    """Owns the poll thread. The thread itself stays a thin loop: read the
    clock once per iteration, call close_expired(), wait. All the actual
    logic lives in close_expired() so it can be tested without a thread.
    """

    def __init__(self, state: AuctionState, interval_seconds: float = DEFAULT_INTERVAL_SECONDS):
        self._state = state
        self._interval_seconds = interval_seconds
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="auction-closer", daemon=True)

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
            now = time_conv.now_utc()
            close_expired(self._state, now)
            self._stop.wait(self._interval_seconds)
