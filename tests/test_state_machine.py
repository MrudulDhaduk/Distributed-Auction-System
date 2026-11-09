"""Specifies replication/state_machine.py: StateMachine.apply_fn(index,
data), the function every Raft node hands to RaftNode as apply_fn.

  * decode the bytes, dispatch: auction commands -> AuctionState.apply(),
    session commands -> SessionState.apply();
  * the client-request dedupe table of the Raft paper, section 8: "the
    state machine tracks the latest serial number processed for each
    client, along with the associated response. If it receives a command
    whose serial number has already been executed, it responds immediately
    without re-executing the request";
  * PlaceBid's high bid is read inside apply_fn, right after apply();
  * corrupt bytes are a fail-stop (R3): logged at CRITICAL, then raised.

No Raft here: apply_fn is called directly with hand-made indexes, the way
the applier thread calls it.
"""

import datetime
import logging

import pytest

from auction.state import AuctionState, CloseAuction, CreateAuction, PlaceBid
from replication import codec
from replication.state_machine import Applied, CorruptEntryError, StateMachine
from sessions.state import Login, Logout, SessionState

UTC = datetime.timezone.utc
CLOSE = datetime.datetime(2030, 1, 1, 12, 0, tzinfo=UTC)
BEFORE = CLOSE - datetime.timedelta(hours=1)
AFTER = CLOSE + datetime.timedelta(hours=1)


def create(auction_id=1, owner="nisarg"):
    return CreateAuction(auction_id=auction_id, item="clock", close_time=CLOSE, owner=owner, description="")


def bid(amount, bidder="karan", auction_id=1, at=BEFORE):
    return PlaceBid(auction_id=auction_id, bidder=bidder, amount=amount, curr_time=at)


class Feeder:
    """Calls apply_fn the way the applier does: increasing indexes."""

    def __init__(self, sm):
        self.sm = sm
        self.index = 0

    def __call__(self, command, client_id="", seq=0):
        self.index += 1
        return self.sm.apply_fn(self.index, codec.encode(command, client_id, seq))


@pytest.fixture
def sm():
    return StateMachine()


@pytest.fixture
def feed(sm):
    return Feeder(sm)


# --- dispatch ------------------------------------------------------------------


def test_auction_commands_reach_auction_state(sm, feed):
    applied = feed(create())

    assert applied == Applied(result=applied.result)
    assert applied.result.success
    assert sm.auctions.auctions[1]["owner"] == "nisarg"


def test_session_commands_reach_session_state(sm, feed):
    applied = feed(Login(token="t1", username="mrudul"))

    assert applied.result.success and applied.token == "t1"
    assert sm.sessions.validate("t1") == "mrudul"

    assert feed(Logout(token="t1")).result.success
    assert sm.sessions.validate("t1") is None


def test_accepted_bid_returns_the_high_bid_after_it(sm, feed):
    feed(create())
    feed(bid(100, "karan"))

    applied = feed(bid(150, "mrudul"))

    assert applied.result.success
    assert (applied.high_bid, applied.high_bidder) == (150, "mrudul")


def test_rejected_bid_returns_the_high_bid_that_beat_it(sm, feed):
    feed(create())
    feed(bid(200, "mrudul"))

    applied = feed(bid(200, "tanmay"))

    assert not applied.result.success
    assert (applied.high_bid, applied.high_bidder) == (200, "mrudul")


def test_bid_on_a_missing_auction_has_no_high_bid(sm, feed):
    applied = feed(bid(10, auction_id=99))

    assert not applied.result.success
    assert (applied.high_bid, applied.high_bidder) == (0, "")


# --- section 8 dedupe ----------------------------------------------------------


def test_a_repeated_serial_number_returns_the_original_response(sm, feed):
    """The headline case. Without dedupe the retry would execute again and
    say "ID already exists"; with it, the client sees its original
    success."""
    first = feed(create(), client_id="c1", seq=1)
    retry = feed(create(), client_id="c1", seq=1)

    assert first.result.success
    assert retry == first
    assert list(sm.auctions.auctions) == [1]


def test_a_repeated_bid_is_not_placed_twice(sm, feed):
    feed(create())
    first = feed(bid(100), client_id="c1", seq=1)
    # The retry was stamped later on a new leader: a different command,
    # same serial number.
    retry = feed(bid(100, at=BEFORE + datetime.timedelta(seconds=3)), client_id="c1", seq=1)

    assert retry == first
    assert len(sm.auctions.auctions[1]["bids"]) == 1


def test_a_repeated_login_returns_the_original_token(sm, feed):
    """The retry minted a new token on the new leader. The client must get
    the token that is actually in the session table, the first one."""
    first = feed(Login(token="first", username="mrudul"), client_id="c1", seq=1)
    retry = feed(Login(token="second", username="mrudul"), client_id="c1", seq=1)

    assert retry.token == "first"
    assert sm.sessions.validate("first") == "mrudul"
    assert sm.sessions.validate("second") is None


def test_an_older_serial_number_is_refused_without_executing(sm, feed):
    feed(create(1), client_id="c1", seq=5)

    old = feed(create(2), client_id="c1", seq=4)

    assert not old.result.success
    assert "stale" in old.result.reason
    assert 2 not in sm.auctions.auctions


def test_clients_are_deduplicated_independently(sm, feed):
    feed(create(1), client_id="c1", seq=1)

    other = feed(create(2), client_id="c2", seq=1)

    assert other.result.success
    assert sorted(sm.auctions.auctions) == [1, 2]


def test_a_new_serial_number_executes(sm, feed):
    feed(create(1), client_id="c1", seq=1)

    nxt = feed(create(2), client_id="c1", seq=2)

    assert nxt.result.success and 2 in sm.auctions.auctions


def test_no_client_id_is_never_deduplicated(sm, feed):
    """The closer proposes with no client id. A second close of the same
    auction executes, and apply() rejects it (the backstop)."""
    feed(create())
    first = feed(CloseAuction(auction_id=1, curr_time=AFTER))
    second = feed(CloseAuction(auction_id=1, curr_time=AFTER))

    assert first.result.success
    assert not second.result.success and "already closed" in second.result.reason


def test_a_rejected_command_is_remembered_too(sm, feed):
    """The response is what is stored, success or not: a retried
    too-low bid gets the same "no", not a second evaluation."""
    feed(create())
    feed(bid(500, "mrudul"))
    first = feed(bid(100), client_id="c1", seq=1)

    retry = feed(bid(100), client_id="c1", seq=1)

    assert not first.result.success and retry == first


# --- determinism ----------------------------------------------------------------


def test_same_entries_same_state_including_the_dedupe_table():
    entries = [
        codec.encode(Login(token="t", username="karan"), "c1", 1),
        codec.encode(create(), "c2", 1),
        codec.encode(bid(10), "c1", 2),
        codec.encode(bid(10), "c1", 2),
        codec.encode(CloseAuction(auction_id=1, curr_time=AFTER)),
    ]
    a, b = StateMachine(), StateMachine()
    for i, data in enumerate(entries, start=1):
        a.apply_fn(i, data)
    for i, data in enumerate(entries, start=1):
        b.apply_fn(i, data)

    assert a.snapshot() == b.snapshot()
    assert a.snapshot()["dedupe"]["c1"][0] == 2


# --- fail-stop on corrupt bytes ---------------------------------------------------


def test_corrupt_bytes_are_logged_critical_and_raised(sm, caplog):
    with caplog.at_level(logging.CRITICAL):
        with pytest.raises(CorruptEntryError):
            sm.apply_fn(7, b"\xff\xff\xff")

    assert any(r.levelno == logging.CRITICAL and "index 7" in r.getMessage() for r in caplog.records)
    assert sm.snapshot() == StateMachine().snapshot()


# --- SessionState on its own -------------------------------------------------------


def test_session_state_login_twice_with_one_token_is_refused():
    s = SessionState()
    assert s.apply(Login("t", "a")).success

    again = s.apply(Login("t", "b"))

    assert not again.success
    assert s.validate("t") == "a"


def test_session_state_logout_is_idempotent():
    s = SessionState()
    s.apply(Login("t", "a"))

    assert s.apply(Logout("t")).success
    assert s.apply(Logout("t")).success
    assert s.validate("t") is None


def test_session_state_shares_its_constructor_argument():
    """StateMachine can be handed an existing AuctionState/SessionState, so
    the servicers and the state machine look at the same objects."""
    auctions, sessions = AuctionState(), SessionState()
    sm = StateMachine(auctions=auctions, sessions=sessions)
    Feeder(sm)(Login("t", "a"))

    assert sm.auctions is auctions and sessions.validate("t") == "a"
