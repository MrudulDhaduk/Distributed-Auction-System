"""Specifies the two LLM-text commands in auction/state.py (chunk 7, step 2):

    SetDescription  the seller replaces the listing text (generated on request)
    RecordSummary   the cluster records the result summary of a CLOSED auction

Both carry LLM output as plain data: apply() stores a string and never learns
a model exists. Both are refused by apply() on the same rules the handlers
pre-check (description_rejection / summary_rejection are shared).

RecordSummary is idempotent by rule: an auction takes at most one summary, so
a second command (a retry, or a second leader that also generated one) is
refused and changes nothing. The state keeps the accepted summaries as a list
so that "at most one" is something the checker can see, not something it has
to take on trust.
"""

import datetime

import pytest

from auction.state import (
    AuctionState,
    CloseAuction,
    CreateAuction,
    PlaceBid,
    RecordSummary,
    SetDescription,
    description_rejection,
    summary_of,
    summary_rejection,
)

CLOSE_TIME = datetime.datetime(2030, 1, 1, 12, 0, 0)
BEFORE_CLOSE = datetime.datetime(2030, 1, 1, 11, 0, 0)
AFTER_CLOSE = datetime.datetime(2030, 1, 1, 13, 0, 0)


def make_state(*, close=True, bids=(("bob", 100),)):
    state = AuctionState()
    assert state.apply(CreateAuction(
        auction_id=1, item="clock", close_time=CLOSE_TIME, owner="seller", description="old")).success
    for bidder, amount in bids:
        assert state.apply(PlaceBid(auction_id=1, bidder=bidder, amount=amount, curr_time=BEFORE_CLOSE)).success
    if close:
        assert state.apply(CloseAuction(auction_id=1, curr_time=AFTER_CLOSE)).success
    return state


def describe(state, actor="seller", text="a fine clock", auction_id=1):
    return state.apply(SetDescription(auction_id=auction_id, actor=actor, description=text))


def summarise(state, text="Bob won at 100.", auction_id=1):
    return state.apply(RecordSummary(auction_id=auction_id, summary=text))


# --- SetDescription ----------------------------------------------------------------------


def test_the_seller_replaces_the_description_of_an_open_auction():
    state = make_state(close=False)

    result = describe(state)

    assert result.success, result.reason
    assert state.auctions[1]["description"] == "a fine clock"


def test_only_the_seller_can_change_the_description():
    state = make_state(close=False)

    result = describe(state, actor="bob")

    assert not result.success and "only the seller" in result.reason
    assert state.auctions[1]["description"] == "old"


def test_a_closed_auctions_description_is_frozen():
    state = make_state(close=True)

    result = describe(state)

    assert not result.success and "closed" in result.reason
    assert state.auctions[1]["description"] == "old"


@pytest.mark.parametrize("text", ["", "   ", "\n"])
def test_an_empty_description_is_refused(text):
    state = make_state(close=False)

    result = describe(state, text=text)

    assert not result.success and "empty" in result.reason
    assert state.auctions[1]["description"] == "old"


def test_describing_an_unknown_auction_is_refused():
    result = describe(make_state(), auction_id=99)

    assert not result.success and "does not exist" in result.reason


# --- RecordSummary -----------------------------------------------------------------------


def test_a_closed_auction_takes_a_summary():
    state = make_state()

    result = summarise(state)

    assert result.success, result.reason
    assert summary_of(state.auctions[1]) == "Bob won at 100."
    assert state.auctions[1]["summaries"] == ["Bob won at 100."]


def test_an_auction_with_no_summary_reads_as_empty():
    assert summary_of(make_state().auctions[1]) == ""
    assert "summaries" not in make_state().auctions[1]  # created lazily: CreateAuction is untouched


def test_an_open_auction_takes_no_summary():
    state = make_state(close=False)

    result = summarise(state)

    assert not result.success and "not closed" in result.reason
    assert summary_of(state.auctions[1]) == ""


def test_an_auction_closed_with_no_bids_takes_a_summary():
    """Nobody bidding is a real outcome, and worth recording."""
    state = make_state(bids=())

    assert summarise(state, "Nobody bid.").success


def test_a_second_summary_is_refused_and_the_first_is_kept():
    state = make_state()
    assert summarise(state, "first").success

    again = summarise(state, "second")

    assert not again.success and "already has a summary" in again.reason
    assert state.auctions[1]["summaries"] == ["first"]


@pytest.mark.parametrize("text", ["", "  "])
def test_an_empty_summary_is_refused(text):
    state = make_state()

    result = summarise(state, text)

    assert not result.success and "empty" in result.reason
    assert summary_of(state.auctions[1]) == ""


def test_summarising_an_unknown_auction_is_refused():
    result = summarise(make_state(), auction_id=99)

    assert not result.success and "does not exist" in result.reason


# --- the shared rule functions -------------------------------------------------------------


def test_the_rule_functions_agree_with_apply():
    state = make_state()
    auction = state.auctions[1]

    assert summary_rejection(auction, "text") is None
    assert summary_rejection(None, "text") is not None
    assert summary_rejection(auction, "") is not None
    summarise(state)
    assert summary_rejection(state.auctions[1], "text") is not None

    open_state = make_state(close=False)
    assert description_rejection(open_state.auctions[1], "seller", "x") is None
    assert description_rejection(open_state.auctions[1], "bob", "x") is not None
    assert description_rejection(open_state.auctions[1], "seller", " ") is not None
    assert description_rejection(state.auctions[1], "seller", "x") is not None  # closed
