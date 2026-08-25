"""
Specifies the concurrency invariant for PlaceBid: under contention, exactly
one bid wins and every loser gets an honest reason.

Written against the real contract in auction/state.py:

- AuctionState() holds the table; state.apply(command) is the single entry
  point and returns Result(success: bool, reason: str).
- CreateAuction(auction_id: int, item: str, close_time: datetime)
- PlaceBid(auction_id: int, bidder: str, amount: int, curr_time: datetime)
  — apply() is deterministic, so the command carries its own timestamp.
- History for an auction is state.auctions[auction_id]["bids"], a list of
  BidEntry(bid_amount, bidder, time_stamp).

apply() returns no snapshot, so the final state is read off the shared
AuctionState after every thread has joined.

This test asserts apply() is thread-safe under 50 simultaneous equal bids.
It is expected to fail: there is no concurrency control around apply() yet.
"""

import datetime
import sys
import threading

import pytest

from auction.state import AuctionState, CreateAuction, PlaceBid

CLOSE_TIME = datetime.datetime(2030, 1, 1, 12, 0, 0)
BID_TIME = datetime.datetime(2030, 1, 1, 11, 0, 0)


@pytest.fixture
def hair_trigger_thread_switching():
    """Make CPython consider switching threads far more often.

    The default switch interval (5ms) is long enough that a thread usually
    runs the whole read-modify-write in `apply()` before the interpreter ever
    offers the GIL to anyone else, so the race hides. Dropping the interval
    lets a single run explore many more interleavings.

    This is global interpreter state, so it is restored afterwards.
    """
    original = sys.getswitchinterval()
    sys.setswitchinterval(1e-9)
    try:
        yield
    finally:
        sys.setswitchinterval(original)


def test_fifty_concurrent_equal_bids_exactly_one_wins(hair_trigger_thread_switching):
    state = AuctionState()
    auction_id = 1
    bid_amount = 100
    n_bidders = 50

    setup = state.apply(
        CreateAuction(
            auction_id=auction_id,
            item="a signed copy of the Raft paper",
            close_time=CLOSE_TIME,
        )
    )
    assert setup.success, f"auction setup failed: {setup.reason}"

    barrier = threading.Barrier(n_bidders)
    results = [None] * n_bidders
    errors = [None] * n_bidders

    def place_bid(i):
        command = PlaceBid(
            auction_id=auction_id,
            bidder=f"bidder-{i}",
            amount=bid_amount,
            curr_time=BID_TIME,
        )
        barrier.wait()  # release all 50 threads at the same moment
        try:
            results[i] = state.apply(command)
        except Exception as exc:  # surface any crash instead of hanging
            errors[i] = exc

    threads = [threading.Thread(target=place_bid, args=(i,)) for i in range(n_bidders)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    for i, err in enumerate(errors):
        assert err is None, f"bidder-{i} raised instead of returning a result: {err!r}"

    accepted = [r for r in results if r.success]
    rejected = [r for r in results if not r.success]

    assert len(accepted) == 1, (
        f"expected exactly one accepted bid under contention, got {len(accepted)}"
    )
    assert len(rejected) == n_bidders - 1

    for r in rejected:
        assert isinstance(r.reason, str) and r.reason.strip(), (
            "a rejected bid must carry an honest, non-empty reason"
        )

    # every thread has joined, so the shared state is now final
    history = state.auctions[auction_id]["bids"]
    assert len(history) == 1
    assert history[0].bid_amount == bid_amount
    assert max(entry.bid_amount for entry in history) == bid_amount
