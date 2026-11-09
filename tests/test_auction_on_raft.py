"""Specifies the auction write path on Raft (resolves M1->M2 seam 2: "Every
handler calls apply() directly. Under Raft: propose -> commit -> apply").

Every write RPC:
    leader check -> (LLM call, CreateAuction only) -> stamp curr_time ->
    encode -> raft.propose() -> wait (bounded) -> reply with apply_fn's
    response
Errors: NOT_LEADER -> FAILED_PRECONDITION + leader-addr; anything unknown
-> UNAVAILABLE.

Real interceptor + servicers of each node, called directly, over an
in-memory 3-node Raft cluster (tests/auction_cluster.py).
"""

import datetime

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
    response = call(node, "AuthService", "Login", auction_pb2.LoginRequest(username=user, password="pass"))
    assert response.result.success, response
    return response.token


def create(node, token, auction_id=1, attributes=None, client_id="", seq=0, close_time=CLOSE):
    return call(node, "AuctionService", "CreateAuction", auction_pb2.CreateAuctionRequest(
        auction_id=auction_id, item="vintage clock", close_time=close_time,
        attributes=attributes or {}, client_id=client_id, seq=seq,
    ), token=token)


def bid(node, token, amount, auction_id=1, client_id="", seq=0):
    return call(node, "AuctionService", "PlaceBid", auction_pb2.PlaceBidRequest(
        auction_id=auction_id, amount=amount, client_id=client_id, seq=seq,
    ), token=token)


def auctions_of(node):
    return node.snapshot()["auctions"]


# --- redirect, then identical state everywhere -----------------------------------


def test_a_bid_sent_to_a_follower_is_redirected_and_then_applied_everywhere(cluster):
    leader = cluster.start("n1")
    seller, bidder = login(leader, "nisarg"), login(leader, "karan")
    assert create(leader, seller).result.success
    cluster.wait_converged()  # the follower knows karan's token
    follower = cluster.nodes["n2"]

    with pytest.raises(RpcAbort) as e:
        bid(follower, bidder, 150)

    assert e.value.code == grpc.StatusCode.FAILED_PRECONDITION
    addr = e.value.trailing["leader-addr"]
    assert addr == CLIENT_ADDRS["n1"]
    assert auctions_of(follower)[1]["bids"] == []  # nothing happened on the follower

    target = next(n for n in cluster.nodes.values() if CLIENT_ADDRS[n.id] == addr)
    assert bid(target, bidder, 150).result.success

    cluster.wait_converged()
    snaps = cluster.snapshots()
    assert snaps["n1"]["auctions"] == snaps["n2"]["auctions"] == snaps["n3"]["auctions"]
    assert [b.bid_amount for b in snaps["n3"]["auctions"][1]["bids"]] == [150]


def test_owner_and_bidder_come_from_the_token_on_every_node(cluster):
    leader = cluster.start("n1")
    create(leader, login(leader, "nisarg"))
    bid(leader, login(leader, "tanmay"), 10)

    cluster.wait_converged()
    for node in cluster.nodes.values():
        a = auctions_of(node)[1]
        assert (a["owner"], a["bids"][0].bidder) == ("nisarg", "tanmay"), node.id


# --- PlaceBid's high bid comes from the apply result ------------------------------


def test_place_bid_reports_the_high_bid_from_the_applied_command(cluster):
    leader = cluster.start("n1")
    create(leader, login(leader, "nisarg"))
    mrudul, tanmay = login(leader, "mrudul"), login(leader, "tanmay")

    accepted = bid(leader, mrudul, 200)
    rejected = bid(leader, tanmay, 200)

    assert accepted.result.success
    assert (accepted.current_high_bid, accepted.current_high_bidder) == (200, "mrudul")
    assert not rejected.result.success
    assert (rejected.current_high_bid, rejected.current_high_bidder) == (200, "mrudul")


# --- dedupe: a retry after an unknown outcome ---------------------------------------


def test_a_retried_create_auction_returns_the_original_success(cluster):
    """The case section 8 is about. n1 replicates CreateAuction to n2 and
    n3, but every reply is lost, so n1's handler cannot know it committed:
    UNAVAILABLE. n1 dies. n2 leads, its no-op commits the entry. The client
    retries on n2 with the SAME (client_id, seq) and gets the ORIGINAL
    success -- without dedupe it would execute again and get "ID already
    exists" for an auction it created itself."""
    leader = cluster.start("n1")
    seller = login(leader, "nisarg")
    cluster.wait_converged()

    cluster.lossy.lose.add("n1")
    with pytest.raises(RpcAbort) as e:
        create(leader, seller, client_id="seller-1", seq=7)
    assert e.value.code == grpc.StatusCode.UNAVAILABLE
    assert wait_until(lambda: len(cluster.nodes["n2"].raft.log_entries()) == len(leader.raft.log_entries()))

    cluster.crash("n1")
    cluster.make_candidate("n2")
    new_leader = cluster.wait_for_leader(among=["n2"])

    retry = create(new_leader, seller, client_id="seller-1", seq=7)

    assert retry.result.success, retry.result.reason
    assert retry.result.reason == "Auction created successfully by nisarg"
    assert list(auctions_of(new_leader)) == [1]


def test_a_second_copy_of_a_committed_bid_is_not_placed_again(cluster):
    leader = cluster.start("n1")
    create(leader, login(leader, "nisarg"))
    karan = login(leader, "karan")

    first = bid(leader, karan, 100, client_id="karan-1", seq=1)
    again = bid(leader, karan, 100, client_id="karan-1", seq=1)

    assert first.result.success and again.result == first.result
    assert len(auctions_of(leader)[1]["bids"]) == 1


def test_an_unknown_outcome_is_unavailable(cluster):
    leader = cluster.start("n1")
    create(leader, login(leader, "nisarg"))
    karan = login(leader, "karan")
    cluster.net.partition(["n1"], ["n2", "n3"])

    with pytest.raises(RpcAbort) as e:
        bid(leader, karan, 100, client_id="k", seq=1)

    assert e.value.code == grpc.StatusCode.UNAVAILABLE
    assert "outcome unknown" in e.value.details


# --- the LLM call ---------------------------------------------------------------------


def test_llm_down_the_auction_is_still_created_with_an_empty_description(cluster):
    leader = cluster.start("n1")
    leader.llm.error = grpc.RpcError()

    response = create(leader, login(leader, "nisarg"), attributes={"condition": "mint"})

    assert response.result.success
    assert len(leader.llm.calls) == 1
    cluster.wait_converged()
    assert all(auctions_of(n)[1]["description"] == "" for n in cluster.nodes.values())


def test_the_llm_text_is_replicated_as_data(cluster):
    leader = cluster.start("n1")
    leader.llm.answer = "a 1970s Seiko, running"

    create(leader, login(leader, "nisarg"), attributes={"brand": "Seiko"})

    cluster.wait_converged()
    assert all(auctions_of(n)[1]["description"] == "a 1970s Seiko, running" for n in cluster.nodes.values())
    assert [len(n.llm.calls) for n in cluster.nodes.values()] == [1, 0, 0]


def test_a_follower_never_calls_the_llm(cluster):
    leader = cluster.start("n1")
    seller = login(leader, "nisarg")
    cluster.wait_converged()
    follower = cluster.nodes["n3"]

    with pytest.raises(RpcAbort) as e:
        create(follower, seller, attributes={"condition": "mint"})

    assert e.value.code == grpc.StatusCode.FAILED_PRECONDITION
    assert follower.llm.calls == []
