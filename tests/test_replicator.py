"""Specifies replication/replicator.py: the one path from a handler to the
log.

    submit(command, client_id, seq):
        encode -> raft.propose() -> wait (bounded) -> the Applied response
    and the two ways it can fail, which the handlers map to gRPC codes:
        NotLeaderError       nothing was appended; retry at leader_addr
        OutcomeUnknownError  LEADERSHIP_LOST / STOPPED / OVERWRITTEN / timeout:
                             the command may or may not be committed;
                             retry with the SAME (client_id, seq)

LocalReplicator is the single-process stand-in (M1 unit tests, no Raft):
same interface, applies at once.
"""

import datetime
import threading

import pytest

from auction.state import AuctionState, CreateAuction, PlaceBid
from replication.replicator import LocalReplicator, NotLeaderError, OutcomeUnknownError
from replication.state_machine import Applied, StateMachine
from tests.auction_cluster import CLIENT_ADDRS, AuctionCluster, wait_until

UTC = datetime.timezone.utc
CLOSE = datetime.datetime(2030, 1, 1, tzinfo=UTC)
NOW = datetime.datetime(2029, 1, 1, tzinfo=UTC)


def create(auction_id=1):
    return CreateAuction(auction_id=auction_id, item="clock", close_time=CLOSE, owner="nisarg", description="")


@pytest.fixture
def cluster():
    c = AuctionCluster()
    yield c
    c.stop()


def test_submit_on_the_leader_returns_the_applied_response(cluster):
    leader = cluster.start("n1")

    applied = leader.replicator.submit(create(), client_id="c1", seq=1)

    assert isinstance(applied, Applied) and applied.result.success


def test_after_submit_every_node_has_the_same_state(cluster):
    leader = cluster.start("n1")
    leader.replicator.submit(create())
    leader.replicator.submit(PlaceBid(auction_id=1, bidder="karan", amount=10, curr_time=NOW))

    cluster.wait_converged()

    snaps = cluster.snapshots()
    assert snaps["n1"] == snaps["n2"] == snaps["n3"]
    assert len(snaps["n2"]["auctions"][1]["bids"]) == 1


def test_submit_on_a_follower_names_the_leader_and_its_client_address(cluster):
    cluster.start("n1")
    follower = cluster.nodes["n2"]
    assert wait_until(lambda: follower.raft.status().leader_id == "n1")

    with pytest.raises(NotLeaderError) as e:
        follower.replicator.submit(create())

    assert (e.value.leader_id, e.value.leader_addr) == ("n1", CLIENT_ADDRS["n1"])
    assert follower.raft.status().last_log_index == cluster.nodes["n1"].raft.status().last_log_index


def test_check_leader_is_the_same_test_without_proposing(cluster):
    cluster.start("n1")
    assert wait_until(lambda: cluster.nodes["n3"].raft.status().leader_id == "n1")

    cluster.nodes["n1"].replicator.check_leader()  # no raise
    with pytest.raises(NotLeaderError) as e:
        cluster.nodes["n3"].replicator.check_leader()
    assert e.value.leader_id == "n1"


def test_no_known_leader_means_no_address(cluster):
    node = cluster.boot("n1")  # alone, never elected (30 s timeout)

    with pytest.raises(NotLeaderError) as e:
        node.replicator.check_leader()

    assert (e.value.leader_id, e.value.leader_addr) == (None, None)


def test_a_leader_that_cannot_commit_times_out_with_outcome_unknown(cluster):
    leader = cluster.start("n1")
    cluster.net.partition(["n1"], ["n2", "n3"])

    with pytest.raises(OutcomeUnknownError) as e:
        leader.replicator.submit(create())

    assert "timed out" in str(e.value)


def test_a_leader_stopped_mid_wait_reports_outcome_unknown(cluster):
    leader = cluster.start("n1")
    cluster.lossy.lose.add("n1")  # replicated, but n1 never hears back
    threading.Timer(0.2, leader.raft.stop).start()
    with pytest.raises(OutcomeUnknownError) as e:
        leader.replicator.submit(create())

    assert "STOPPED" in str(e.value)


def test_propose_returns_the_handle_without_waiting(cluster):
    leader = cluster.start("n1")

    handle = leader.replicator.propose(create())

    assert handle.wait(5) and handle.ok and handle.result.result.success


def test_local_replicator_applies_at_once_through_the_codec():
    sm = StateMachine(auctions=AuctionState())
    local = LocalReplicator(sm)

    applied = local.submit(create(), client_id="c", seq=1)
    retry = local.submit(create(), client_id="c", seq=1)

    assert applied.result.success and retry == applied
    local.check_leader()  # always the leader of its cluster of one
    with pytest.raises(ValueError):  # a naive datetime never gets encoded
        local.submit(PlaceBid(auction_id=1, bidder="a", amount=1, curr_time=datetime.datetime(2029, 1, 1)))
