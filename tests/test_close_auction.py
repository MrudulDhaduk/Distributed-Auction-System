"""
Specifies CloseAuction: when it is allowed to take effect, and what it freezes
in place once it does.

Written against the real contract in auction/state.py:

- AuctionState() holds the table; state.apply(command) is the single entry
  point and returns Result(success: bool, reason: str).
- CreateAuction(auction_id: int, item: str, close_time: datetime)
- PlaceBid(auction_id: int, bidder: str, amount: int, curr_time: datetime)
- CloseAuction(auction_id: int, curr_time: datetime)
- state.auctions[auction_id] has keys: item, close_time, bids, closed,
  winner, closed_at. Bids are BidEntry(bid_amount, bidder, time_stamp).

All timestamps below are fixed datetimes -- no datetime.now() -- so the tests
are deterministic regardless of when they run.
"""

import datetime

from auction.state import AuctionState, CloseAuction, CreateAuction, PlaceBid

CLOSE_TIME = datetime.datetime(2030, 1, 1, 12, 0, 0)
BEFORE_CLOSE = datetime.datetime(2030, 1, 1, 11, 0, 0)
AFTER_CLOSE = datetime.datetime(2030, 1, 1, 13, 0, 0)
LATER_STILL = datetime.datetime(2030, 1, 1, 14, 0, 0)


def make_auction(state, auction_id=1, close_time=CLOSE_TIME):
    setup = state.apply(
        CreateAuction(auction_id=auction_id, item="a signed copy of the Raft paper", close_time=close_time)
    )
    assert setup.success, f"auction setup failed: {setup.reason}"


def test_close_after_close_time_succeeds_and_records_highest_bidder():
    state = AuctionState()
    make_auction(state)

    low = state.apply(PlaceBid(auction_id=1, bidder="alice", amount=50, curr_time=BEFORE_CLOSE))
    high = state.apply(PlaceBid(auction_id=1, bidder="bob", amount=100, curr_time=BEFORE_CLOSE))
    assert low.success and high.success

    result = state.apply(CloseAuction(auction_id=1, curr_time=AFTER_CLOSE))

    assert result.success, result.reason
    auction = state.auctions[1]
    assert auction["closed"] is True
    assert auction["closed_at"] == AFTER_CLOSE
    assert auction["winner"].bidder == "bob"
    assert auction["winner"].bid_amount == 100


def test_winner_is_the_highest_bid_not_the_last_one():
    state = AuctionState()
    make_auction(state)

    state.apply(PlaceBid(auction_id=1, bidder="alice", amount=100, curr_time=BEFORE_CLOSE))
    state.apply(PlaceBid(auction_id=1, bidder="bob", amount=150, curr_time=BEFORE_CLOSE))
    # a later bid that is rejected for being too low -- still the last entry
    # attempted, but must not become the winner
    rejected = state.apply(PlaceBid(auction_id=1, bidder="carol", amount=120, curr_time=BEFORE_CLOSE))
    assert not rejected.success

    state.apply(CloseAuction(auction_id=1, curr_time=AFTER_CLOSE))

    assert state.auctions[1]["winner"].bidder == "bob"
    assert state.auctions[1]["winner"].bid_amount == 150


def test_close_before_close_time_is_rejected():
    state = AuctionState()
    make_auction(state)

    result = state.apply(CloseAuction(auction_id=1, curr_time=BEFORE_CLOSE))

    assert not result.success
    assert state.auctions[1]["closed"] is False
    assert state.auctions[1]["closed_at"] is None


def test_close_nonexistent_auction_is_rejected():
    state = AuctionState()

    result = state.apply(CloseAuction(auction_id=999, curr_time=AFTER_CLOSE))

    assert not result.success


def test_close_already_closed_auction_is_rejected():
    state = AuctionState()
    make_auction(state)

    first = state.apply(CloseAuction(auction_id=1, curr_time=AFTER_CLOSE))
    assert first.success

    second = state.apply(CloseAuction(auction_id=1, curr_time=LATER_STILL))

    assert not second.success
    # the first close's timestamp must not be clobbered by the rejected retry
    assert state.auctions[1]["closed_at"] == AFTER_CLOSE


def test_close_with_zero_bids_succeeds_with_no_winner():
    state = AuctionState()
    make_auction(state)

    result = state.apply(CloseAuction(auction_id=1, curr_time=AFTER_CLOSE))

    assert result.success, result.reason
    assert state.auctions[1]["closed"] is True
    assert state.auctions[1]["winner"] is None


def test_bid_after_close_time_is_rejected_by_the_time_guard():
    state = AuctionState()
    make_auction(state)

    result = state.apply(PlaceBid(auction_id=1, bidder="alice", amount=100, curr_time=AFTER_CLOSE))

    assert not result.success
    assert state.auctions[1]["bids"] == []
    assert state.auctions[1]["closed"] is False


def test_bid_after_auction_closed_is_rejected_by_the_closed_flag():
    state = AuctionState()
    make_auction(state)

    closed = state.apply(CloseAuction(auction_id=1, curr_time=AFTER_CLOSE))
    assert closed.success, closed.reason

    # curr_time is BEFORE close_time, so the time guard alone would let this
    # bid through -- only the "closed" flag can be rejecting it here
    result = state.apply(PlaceBid(auction_id=1, bidder="alice", amount=100, curr_time=BEFORE_CLOSE))

    assert not result.success
    assert state.auctions[1]["bids"] == []
