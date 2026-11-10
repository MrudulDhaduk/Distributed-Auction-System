"""Specifies the escrow commands in auction/state.py (chunk 7, step 1):

    PayAuction      the winner pays the winning bid into escrow
    ReleaseEscrow   the seller confirms; the held money goes to the seller
    RefundEscrow    the seller cancels; the held money goes back to the winner

Escrow per auction: NONE -> HELD -> RELEASED | REFUNDED. RELEASED and
REFUNDED are final. apply() is the only judge: it decides from the command
and the current state, with no clock, no randomness and no I/O. The gateway's
answer (payment_ref / settlement_ref) arrives on the command as plain data.

Fixed datetimes, no datetime.now(), as in test_close_auction.py.
"""

import datetime

import pytest

from auction.state import (
    AuctionState,
    CloseAuction,
    CreateAuction,
    PayAuction,
    PlaceBid,
    RefundEscrow,
    ReleaseEscrow,
    escrow_of,
    payment_rejection,
    settlement_rejection,
)

CLOSE_TIME = datetime.datetime(2030, 1, 1, 12, 0, 0)
BEFORE_CLOSE = datetime.datetime(2030, 1, 1, 11, 0, 0)
AFTER_CLOSE = datetime.datetime(2030, 1, 1, 13, 0, 0)


def make_state(*, bids=(("alice", 50), ("bob", 100)), close=True):
    """Auction 1, seller "seller". Bids, and a close unless close=False."""
    state = AuctionState()
    assert state.apply(CreateAuction(
        auction_id=1, item="clock", close_time=CLOSE_TIME, owner="seller", description="")).success
    for bidder, amount in bids:
        assert state.apply(PlaceBid(auction_id=1, bidder=bidder, amount=amount, curr_time=BEFORE_CLOSE)).success
    if close:
        assert state.apply(CloseAuction(auction_id=1, curr_time=AFTER_CLOSE)).success
    return state


def pay(state, payer="bob", amount=100, ref="PAY-1", auction_id=1):
    return state.apply(PayAuction(auction_id=auction_id, payer=payer, amount=amount, payment_ref=ref))


def release(state, actor="seller", ref="REL-1"):
    return state.apply(ReleaseEscrow(auction_id=1, actor=actor, settlement_ref=ref))


def refund(state, actor="seller", ref="RFD-1"):
    return state.apply(RefundEscrow(auction_id=1, actor=actor, settlement_ref=ref))


def status(state):
    return escrow_of(state.auctions[1])["status"]


# --- PayAuction: the five conditions ---------------------------------------------------


def test_an_auction_that_was_never_paid_has_escrow_none():
    state = make_state()

    assert status(state) == "NONE"
    assert "escrow" not in state.auctions[1]  # created lazily: CreateAuction is untouched


def test_the_winner_paying_the_winning_bid_puts_it_in_escrow():
    state = make_state()

    result = pay(state)

    assert result.success, result.reason
    assert escrow_of(state.auctions[1]) == {
        "status": "HELD", "amount": 100, "payer": "bob", "payment_ref": "PAY-1", "settlement_ref": "",
    }


def test_paying_before_the_auction_is_closed_is_refused():
    state = make_state(close=False)

    result = pay(state)

    assert not result.success and "not closed" in result.reason
    assert status(state) == "NONE"


def test_paying_for_an_auction_that_does_not_exist_is_refused():
    result = pay(make_state(), auction_id=99)

    assert not result.success and "does not exist" in result.reason


def test_paying_for_an_auction_that_closed_with_no_bids_is_refused():
    state = make_state(bids=())

    result = pay(state)

    assert not result.success and "no winner" in result.reason
    assert status(state) == "NONE"


@pytest.mark.parametrize("payer", ["alice", "seller", "mallory"])
def test_only_the_winner_can_pay(payer):
    state = make_state()

    result = pay(state, payer=payer)

    assert not result.success and "only the winner" in result.reason
    assert status(state) == "NONE"


@pytest.mark.parametrize("amount", [99, 101, 0, 50])
def test_the_amount_must_equal_the_winning_bid(amount):
    state = make_state()

    result = pay(state, amount=amount)

    assert not result.success and "100" in result.reason
    assert status(state) == "NONE"


def test_a_payment_without_a_gateway_reference_is_refused():
    """The handler only proposes an approved payment. If a command ever
    arrives without a reference, apply() will not treat it as money."""
    state = make_state()

    result = pay(state, ref="")

    assert not result.success and "reference" in result.reason
    assert status(state) == "NONE"


def test_the_real_reason_wins_over_the_missing_reference():
    """A doomed command is proposed with an empty reference (the handler
    skips the gateway when its pre-check fails). The client must be told
    the real reason."""
    state = make_state()

    result = pay(state, payer="mallory", ref="")

    assert "only the winner" in result.reason


def test_no_double_pay_and_the_first_payment_is_kept():
    state = make_state()
    assert pay(state, ref="PAY-FIRST").success

    again = pay(state, ref="PAY-SECOND")

    assert not again.success and "HELD" in again.reason
    assert escrow_of(state.auctions[1])["payment_ref"] == "PAY-FIRST"


# --- ReleaseEscrow / RefundEscrow ----------------------------------------------------------


def test_the_seller_releasing_held_money_makes_it_released():
    state = make_state()
    pay(state)

    result = release(state)

    assert result.success, result.reason
    held = escrow_of(state.auctions[1])
    assert (held["status"], held["settlement_ref"], held["payment_ref"], held["amount"]) == (
        "RELEASED", "REL-1", "PAY-1", 100)


def test_the_seller_refunding_held_money_makes_it_refunded():
    state = make_state()
    pay(state)

    result = refund(state)

    assert result.success, result.reason
    assert status(state) == "REFUNDED"
    assert escrow_of(state.auctions[1])["settlement_ref"] == "RFD-1"


@pytest.mark.parametrize("settle", [release, refund])
def test_nothing_can_be_settled_before_it_is_paid(settle):
    state = make_state()

    result = settle(state)

    assert not result.success and "NONE" in result.reason
    assert status(state) == "NONE"


@pytest.mark.parametrize("settle", [release, refund])
@pytest.mark.parametrize("actor", ["bob", "alice", "mallory"])
def test_only_the_seller_can_settle(settle, actor):
    state = make_state()
    pay(state)

    result = settle(state, actor=actor)

    assert not result.success and "only the seller" in result.reason
    assert status(state) == "HELD"


@pytest.mark.parametrize("first", [release, refund])
@pytest.mark.parametrize("second", [release, refund])
def test_released_and_refunded_are_final(first, second):
    state = make_state()
    pay(state)
    assert first(state).success
    before = dict(escrow_of(state.auctions[1]))

    result = second(state, ref="LATE")

    assert not result.success
    assert escrow_of(state.auctions[1]) == before


@pytest.mark.parametrize("settle", [release, refund])
def test_after_settlement_the_winner_cannot_pay_again(settle):
    state = make_state()
    pay(state)
    settle(state)
    before = dict(escrow_of(state.auctions[1]))

    result = pay(state, ref="PAY-AGAIN")

    assert not result.success
    assert escrow_of(state.auctions[1]) == before


@pytest.mark.parametrize("settle", [release, refund])
def test_a_settlement_without_a_gateway_reference_is_refused(settle):
    state = make_state()
    pay(state)

    result = settle(state, ref="")

    assert not result.success and "reference" in result.reason
    assert status(state) == "HELD"


# --- the shared rule functions (the handler's pre-check uses the same ones) -----------------


def test_payment_rejection_is_none_exactly_when_apply_would_accept():
    state = make_state()
    auction = state.auctions[1]

    assert payment_rejection(auction, "bob", 100) is None
    assert payment_rejection(auction, "alice", 100) is not None
    assert payment_rejection(auction, "bob", 99) is not None
    assert payment_rejection(None, "bob", 100) is not None
    pay(state)
    assert payment_rejection(state.auctions[1], "bob", 100) is not None  # now HELD


def test_settlement_rejection_is_none_exactly_when_apply_would_accept():
    state = make_state()
    assert settlement_rejection(state.auctions[1], "seller") is not None  # nothing held
    pay(state)
    assert settlement_rejection(state.auctions[1], "seller") is None
    assert settlement_rejection(state.auctions[1], "bob") is not None
    release(state)
    assert settlement_rejection(state.auctions[1], "seller") is not None  # final


# --- determinism ----------------------------------------------------------------------------


def test_the_same_commands_give_the_same_state_every_time():
    """apply() reads nothing but the command and the state."""
    def run():
        state = make_state()
        pay(state)
        release(state)
        return escrow_of(state.auctions[1])

    assert run() == run()
