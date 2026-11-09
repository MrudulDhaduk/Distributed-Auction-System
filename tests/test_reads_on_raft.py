"""Specifies reads on Raft (resolves M1->M2 seam 5: "GetAuction /
ListAuctions read local state. Decide leader-only vs. follower reads").

Decision (A15): leader only, through RaftNode.read_index (section 8). A
follower answers NOT_LEADER like a write. The leader confirms it is still
the leader, waits until it has applied everything committed before the
read began, then reads its own state.

Also here: the interceptor uses the same barrier before it refuses an
unknown token on the leader, which closes Part A's gap "a brand-new leader
may not have applied a recent Login yet".

Real interceptor + servicers, in-memory 3-node cluster.
"""

import datetime
import threading

import grpc
import pytest

from common import time_conv
from generated import auction_pb2
from tests.auction_cluster import CLIENT_ADDRS, AuctionCluster, RpcAbort, call, wait_until

CLOSE = time_conv.dt_to_micros(time_conv.now_utc() + datetime.timedelta(hours=1))


@pytest.fixture
def cluster():
    c = AuctionCluster()
    yield c
    c.stop()


def login(node, user):
    r = call(node, "AuthService", "Login", auction_pb2.LoginRequest(username=user, password="pass"))
    assert r.result.success, r
    return r.token


def get(node, token, auction_id=1):
    return call(node, "AuctionService", "GetAuction", auction_pb2.GetAuctionRequest(auction_id=auction_id), token=token)


def listing(node, token):
    return call(node, "AuctionService", "ListAuctions", auction_pb2.ListAuctionsRequest(include_closed=True), token=token)


def setup_auction(leader):
    seller, bidder = login(leader, "nisarg"), login(leader, "karan")
    assert call(leader, "AuctionService", "CreateAuction", auction_pb2.CreateAuctionRequest(
        auction_id=1, item="clock", close_time=CLOSE), token=seller).result.success
    return seller, bidder


def bid(node, token, amount):
    return call(node, "AuctionService", "PlaceBid", auction_pb2.PlaceBidRequest(auction_id=1, amount=amount), token=token)


def test_reads_on_the_leader_see_its_writes(cluster):
    leader = cluster.start("n1")
    seller, bidder = setup_auction(leader)
    assert bid(leader, bidder, 150).result.success

    one = get(leader, bidder)
    many = listing(leader, bidder)

    assert one.result.success and one.auction.current_high_bid == 150
    assert [a.auction_id for a in many.auctions] == [1]


@pytest.mark.parametrize("read", [get, listing], ids=["GetAuction", "ListAuctions"])
def test_a_follower_redirects_reads(cluster, read):
    leader = cluster.start("n1")
    seller, _ = setup_auction(leader)
    cluster.wait_converged()  # the follower knows the token

    with pytest.raises(RpcAbort) as e:
        read(cluster.nodes["n2"], seller)

    assert e.value.code == grpc.StatusCode.FAILED_PRECONDITION
    assert e.value.trailing["leader-addr"] == CLIENT_ADDRS["n1"]


def test_a_deposed_leader_that_has_not_noticed_cannot_serve_a_stale_read(cluster):
    """n1 is cut off and still believes it leads. n2 leads term 2 and
    accepts a bid of 300. n1's own state still says 150. A read on n1 must
    not answer 150: it answers UNAVAILABLE. A read on n2 says 300."""
    n1 = cluster.start("n1")
    seller, bidder = setup_auction(n1)
    assert bid(n1, bidder, 150).result.success
    cluster.wait_converged()

    cluster.net.partition(["n1"], ["n2", "n3"])
    cluster.make_candidate("n2")
    n2 = cluster.wait_for_leader(among=["n2"], above_term=1)
    assert bid(n2, bidder, 300).result.success
    assert n1.raft.status().role.value == "LEADER"  # it has not noticed

    with pytest.raises(RpcAbort) as e:
        get(n1, bidder)
    assert e.value.code == grpc.StatusCode.UNAVAILABLE
    assert n1.snapshot()["auctions"][1]["bids"][-1].bid_amount == 150  # what it would have said

    assert get(n2, bidder).auction.current_high_bid == 300


def test_a_new_leader_accepts_a_token_it_has_not_applied_yet(cluster):
    """n1 commits karan's Login with n3. n2 stores the entry but never
    learns it committed. n1 dies; n2 leads with the Login unapplied. A
    request with that token must not be refused: the interceptor runs a
    read barrier, which applies the Login, then looks again.

    n2's no-op is held back while the request is in flight, so the request
    cannot arrive "late enough" by luck; then it is let through."""
    n1 = cluster.start("n1")
    seller, _ = setup_auction(n1)
    cluster.wait_converged()
    n2 = cluster.nodes["n2"]

    login_index = n1.raft.status().last_log_index + 1
    cluster.lossy.drop = lambda a: a.from_id == "n1" and a.to == "n2" and a.commit_index >= login_index
    karan = login(n1, "karan")
    assert wait_until(lambda: n2.raft.status().last_log_index >= login_index)
    assert n2.sessions.validate(karan) is None

    cluster.crash("n1")
    cluster.lossy.drop = lambda a: a.from_id == "n2" and a.to == "n3" and len(a.entries) > 0
    cluster.make_candidate("n2")
    cluster.wait_for_leader(among=["n2"], above_term=1)
    assert n2.sessions.validate(karan) is None  # the new leader does not know it yet

    out = {}

    def request():
        try:
            out["r"] = get(n2, karan)
        except RpcAbort as e:
            out["r"] = e

    t = threading.Thread(target=request, daemon=True)
    t.start()
    t.join(0.3)
    assert t.is_alive(), "answered before the new leader could know the token"
    cluster.lossy.drop = lambda a: False
    t.join(5)

    assert not isinstance(out["r"], RpcAbort), out["r"]
    assert out["r"].result.success


def test_an_unknown_token_is_still_refused_by_the_leader_after_the_barrier(cluster):
    leader = cluster.start("n1")

    with pytest.raises(RpcAbort) as e:
        get(leader, "never-issued")

    assert e.value.code == grpc.StatusCode.UNAUTHENTICATED
