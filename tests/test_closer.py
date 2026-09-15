"""Specifies close_expired(): the pure function the poll thread in
server/closer.py calls every tick.

Deliberately no real clock and no sleeping -- `now` is always a fixed
datetime passed straight in, matching tests/test_close_auction.py.
"""

import datetime

from auction.state import AuctionState, CreateAuction, PlaceBid
from server.closer import close_expired

CLOSE_TIME = datetime.datetime(2030, 1, 1, 12, 0, 0)
BEFORE_CLOSE = datetime.datetime(2030, 1, 1, 11, 0, 0)
AFTER_CLOSE = datetime.datetime(2030, 1, 1, 13, 0, 0)


def make_auction(state, auction_id=1, close_time=CLOSE_TIME):
    setup = state.apply(
        CreateAuction(auction_id=auction_id, item="a signed copy of the Raft paper", close_time=close_time)
    )
    assert setup.success, f"auction setup failed: {setup.reason}"


def test_closes_auction_past_its_close_time():
    state = AuctionState()
    make_auction(state)

    results = close_expired(state, AFTER_CLOSE)

    assert [auction_id for auction_id, _ in results] == [1]
    assert results[0][1].success
    assert state.auctions[1]["closed"] is True
    assert state.auctions[1]["closed_at"] == AFTER_CLOSE


def test_leaves_auction_before_its_close_time_untouched():
    state = AuctionState()
    make_auction(state)

    results = close_expired(state, BEFORE_CLOSE)

    assert results == []
    assert state.auctions[1]["closed"] is False


def test_skips_auction_already_closed():
    state = AuctionState()
    make_auction(state)
    close_expired(state, AFTER_CLOSE)

    results = close_expired(state, AFTER_CLOSE)

    assert results == []


def test_closes_only_the_auctions_that_are_due():
    state = AuctionState()
    make_auction(state, auction_id=1, close_time=CLOSE_TIME)
    make_auction(state, auction_id=2, close_time=AFTER_CLOSE)

    results = close_expired(state, AFTER_CLOSE)

    assert [auction_id for auction_id, _ in results] == [1]
    assert state.auctions[1]["closed"] is True
    assert state.auctions[2]["closed"] is False


def test_records_the_highest_bid_as_winner_on_close():
    state = AuctionState()
    make_auction(state)
    state.apply(PlaceBid(auction_id=1, bidder="alice", amount=50, curr_time=BEFORE_CLOSE))
    state.apply(PlaceBid(auction_id=1, bidder="bob", amount=100, curr_time=BEFORE_CLOSE))

    close_expired(state, AFTER_CLOSE)

    assert state.auctions[1]["winner"].bidder == "bob"


def test_no_auctions_is_a_no_op():
    state = AuctionState()

    results = close_expired(state, AFTER_CLOSE)

    assert results == []
