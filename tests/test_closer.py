"""Specifies server/closer.py on Raft (resolves M1->M2 seam 1: "closer.py
calls state.apply(CloseAuction(...)) directly, on every node. Under Raft
that's divergence. Only the leader may propose closes").

  * due_auction_ids(state, now): the pure scan -- which auctions are past
    close_time and not closed. No clock read inside; `now` is passed in.
  * AuctionCloser.tick(now): on the leader only, propose CloseAuction
    (stamped with `now`, the leader's clock) for each due auction that has
    no close already in flight. A follower proposes nothing.
  * apply() still rejects a second close ("already closed"): the backstop.

ADAPTED from M1: the M1 version called close_expired(state, now), which
applied CloseAuction directly, with naive datetimes. Its cases survive
below as due_auction_ids / tick tests; datetimes are now UTC-aware because
a command is encoded before it is applied (common/time_conv refuses naive).
"""

import datetime

import pytest

from auction.state import AuctionState, CloseAuction, CreateAuction, PlaceBid
from raft.messages import EntryKind
from raft.proposal import Outcome, Proposal
from replication import codec
from replication.replicator import LocalReplicator
from replication.state_machine import StateMachine
from server.closer import AuctionCloser, due_auction_ids
from tests.auction_cluster import AuctionCluster, wait_until

UTC = datetime.timezone.utc
CLOSE_TIME = datetime.datetime(2030, 1, 1, 12, 0, 0, tzinfo=UTC)
BEFORE_CLOSE = datetime.datetime(2030, 1, 1, 11, 0, 0, tzinfo=UTC)
AFTER_CLOSE = datetime.datetime(2030, 1, 1, 13, 0, 0, tzinfo=UTC)


def make_auction(state, auction_id=1, close_time=CLOSE_TIME):
    setup = state.apply(
        CreateAuction(
            auction_id=auction_id,
            item="a signed copy of the Raft paper",
            close_time=close_time,
            owner="seller",
            description="",
        )
    )
    assert setup.success, f"auction setup failed: {setup.reason}"


def local_closer(state):
    return AuctionCloser(state, LocalReplicator(StateMachine(auctions=state)))


class FakeReplicator:
    """Records proposals; each returns a PENDING handle the test resolves."""

    def __init__(self, leader=True):
        self.leader = leader
        self.proposed = []

    def is_leader(self):
        return self.leader

    def propose(self, command, client_id="", seq=0):
        if not self.leader:
            p = Proposal(0, 1)
            p._resolve(Outcome.NOT_LEADER)
            return p
        p = Proposal(len(self.proposed) + 1, 1)
        self.proposed.append((command, p))
        return p


# --- the scan (M1 cases) -------------------------------------------------------


def test_an_auction_past_its_close_time_is_due():
    state = AuctionState()
    make_auction(state)

    assert due_auction_ids(state, AFTER_CLOSE) == [1]


def test_an_auction_before_its_close_time_is_not_due():
    state = AuctionState()
    make_auction(state)

    assert due_auction_ids(state, BEFORE_CLOSE) == []


def test_an_auction_already_closed_is_not_due():
    state = AuctionState()
    make_auction(state)
    state.apply(CloseAuction(auction_id=1, curr_time=AFTER_CLOSE))

    assert due_auction_ids(state, AFTER_CLOSE) == []


def test_only_the_auctions_that_are_due():
    state = AuctionState()
    make_auction(state, auction_id=1, close_time=CLOSE_TIME)
    make_auction(state, auction_id=2, close_time=AFTER_CLOSE)

    assert due_auction_ids(state, AFTER_CLOSE) == [1]


def test_no_auctions_is_a_no_op():
    assert due_auction_ids(AuctionState(), AFTER_CLOSE) == []


# --- tick ------------------------------------------------------------------------


def test_tick_closes_and_records_the_highest_bid_as_winner():
    state = AuctionState()
    make_auction(state)
    state.apply(PlaceBid(auction_id=1, bidder="alice", amount=50, curr_time=BEFORE_CLOSE))
    state.apply(PlaceBid(auction_id=1, bidder="bob", amount=100, curr_time=BEFORE_CLOSE))

    assert local_closer(state).tick(AFTER_CLOSE) == [1]

    assert state.auctions[1]["closed"] is True
    assert state.auctions[1]["closed_at"] == AFTER_CLOSE  # the stamped time
    assert state.auctions[1]["winner"].bidder == "bob"


def test_a_follower_proposes_nothing():
    state = AuctionState()
    make_auction(state)
    replicator = FakeReplicator(leader=False)

    assert AuctionCloser(state, replicator).tick(AFTER_CLOSE) == []
    assert replicator.proposed == []


def test_a_close_in_flight_is_not_proposed_again():
    state = AuctionState()
    make_auction(state)
    replicator = FakeReplicator()
    closer = AuctionCloser(state, replicator)

    assert closer.tick(AFTER_CLOSE) == [1]
    assert closer.tick(AFTER_CLOSE) == []  # still pending
    assert len(replicator.proposed) == 1
    assert replicator.proposed[0][0] == CloseAuction(auction_id=1, curr_time=AFTER_CLOSE)


def test_after_a_failed_close_the_auction_is_proposed_again():
    """LEADERSHIP_LOST is "outcome unknown". If this node leads again and
    the auction is still open, it proposes again; if the first close did
    commit after all, apply() rejects the second one."""
    state = AuctionState()
    make_auction(state)
    replicator = FakeReplicator()
    closer = AuctionCloser(state, replicator)
    closer.tick(AFTER_CLOSE)

    replicator.proposed[0][1]._resolve(Outcome.LEADERSHIP_LOST)

    assert closer.tick(AFTER_CLOSE) == [1]
    assert len(replicator.proposed) == 2


# --- on a Raft cluster --------------------------------------------------------------


@pytest.fixture
def cluster():
    c = AuctionCluster()
    yield c
    c.stop()


def close_commands_in_log(node, auction_id):
    out = []
    for entry in node.raft.log_entries():
        if entry.kind is EntryKind.COMMAND:
            command = codec.decode(entry.data).command
            if isinstance(command, CloseAuction) and command.auction_id == auction_id:
                out.append(command)
    return out


def create_on(leader, auction_id, close_time):
    applied = leader.replicator.submit(CreateAuction(
        auction_id=auction_id, item="x", close_time=close_time, owner="s", description=""))
    assert applied.result.success


def test_only_the_leader_proposes_and_each_auction_closes_exactly_once(cluster):
    leader = cluster.start("n1")
    create_on(leader, 1, CLOSE_TIME)
    create_on(leader, 2, CLOSE_TIME)
    cluster.wait_converged()
    closers = {nid: AuctionCloser(n.sm.auctions, n.replicator) for nid, n in cluster.nodes.items()}

    # Every node ticks, several times, with the deadline long past.
    for _ in range(5):
        for nid in ("n2", "n3", "n1"):
            closers[nid].tick(AFTER_CLOSE)

    cluster.wait_converged()
    for aid in (1, 2):
        assert len(close_commands_in_log(leader, aid)) == 1, aid
    for node in cluster.nodes.values():
        assert all(node.sm.auctions.auctions[aid]["closed"] for aid in (1, 2)), node.id


def test_after_failover_the_new_leader_closes_what_is_overdue(cluster):
    leader = cluster.start("n1")
    create_on(leader, 1, CLOSE_TIME)
    cluster.wait_converged()

    cluster.crash("n1")  # died before its closer got to it
    cluster.make_candidate("n2")
    new_leader = cluster.wait_for_leader(among=["n2"])
    closer = AuctionCloser(new_leader.sm.auctions, new_leader.replicator)

    assert closer.tick(AFTER_CLOSE) == [1]

    assert wait_until(lambda: all(n.sm.auctions.auctions[1]["closed"] for n in cluster.nodes.values()))
    assert cluster.nodes["n3"].sm.auctions.auctions[1]["closed_at"] == AFTER_CLOSE


def test_the_poll_threads_close_once_on_a_live_cluster(cluster):
    """The real loop: a thread per node, a real clock, 20 ms ticks. Bounded
    by wait_until, so a slow machine only makes it slower, not red.
    Exactly-once holds without timing luck: the leader never re-proposes
    while its close is pending, and by the time the handle resolves, apply
    has run and the auction is no longer due."""
    leader = cluster.start("n1")
    soon = datetime.datetime.now(UTC) + datetime.timedelta(milliseconds=200)
    create_on(leader, 1, soon)
    closers = [AuctionCloser(n.sm.auctions, n.replicator, interval_seconds=0.02) for n in cluster.nodes.values()]
    for c in closers:
        c.start()
    try:
        assert wait_until(lambda: all(n.sm.auctions.auctions[1]["closed"] for n in cluster.nodes.values()))
        cluster.wait_converged()
    finally:
        for c in closers:
            c.stop()

    assert len(close_commands_in_log(leader, 1)) == 1
