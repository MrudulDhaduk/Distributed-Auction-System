"""Specifies payment and escrow on the Raft cluster (chunk 7, step 1).

Write path of Pay / ReleaseEscrow / RefundEscrow (server/payment_servicer.py):

    leader check -> read barrier -> pre-check with the SAME rule functions
    apply() uses -> gateway call (only if the pre-check passed) ->
    propose(command carrying the gateway's reference as data) -> wait ->
    reply with apply()'s verdict

apply() is still the judge: a command proposed with an empty reference (the
pre-check failed) is refused by apply() with the real reason. If apply()
refuses a command whose authorization the gateway already granted, the
handler voids it.

Real interceptor + servicers, called directly, over an in-memory 3-node Raft
cluster (tests/auction_cluster.py).
"""

import datetime

import grpc
import pytest

from auction.state import CloseAuction
from client.auction_client import ClusterClient
from common import time_conv
from generated import auction_pb2, payment_pb2
from server.payment_gateway import MockPaymentGateway
from tests.auction_cluster import (
    CLIENT_ADDRS,
    AuctionCluster,
    RpcAbort,
    call,
    in_memory_invoker,
    wait_until,
)

NOW = time_conv.now_utc()
CLOSE = time_conv.dt_to_micros(NOW + datetime.timedelta(hours=1))
ADDRS = [CLIENT_ADDRS[n] for n in ("n1", "n2", "n3")]


@pytest.fixture
def cluster():
    c = AuctionCluster()
    yield c
    c.stop()


def login(node, user):
    response = call(node, "AuthService", "Login", auction_pb2.LoginRequest(username=user, password="pass"))
    assert response.result.success, response
    return response.token


def auction_with_winner(cluster, bids=(("karan", 100), ("mrudul", 150))):
    """Leader n1; auction 1 by nisarg, bids, then closed. Returns
    (leader, {user: token})."""
    leader = cluster.start("n1")
    tokens = {u: login(leader, u) for u in ("nisarg", "karan", "mrudul", "tanmay")}
    assert call(leader, "AuctionService", "CreateAuction", auction_pb2.CreateAuctionRequest(
        auction_id=1, item="clock", close_time=CLOSE), token=tokens["nisarg"]).result.success
    for user, amount in bids:
        assert call(leader, "AuctionService", "PlaceBid", auction_pb2.PlaceBidRequest(
            auction_id=1, amount=amount), token=tokens[user]).result.success
    closed = leader.replicator.submit(CloseAuction(auction_id=1, curr_time=NOW + datetime.timedelta(hours=2)))
    assert closed.result.success, closed.result.reason
    cluster.wait_converged()
    return leader, tokens


def pay(node, token, amount=150, auction_id=1, client_id="", seq=0):
    return call(node, "PaymentService", "Pay", payment_pb2.PayRequest(
        auction_id=auction_id, amount=amount, client_id=client_id, seq=seq), token=token)


def release(node, token, client_id="", seq=0):
    return call(node, "PaymentService", "ReleaseEscrow", payment_pb2.EscrowActionRequest(
        auction_id=1, client_id=client_id, seq=seq), token=token)


def refund(node, token, client_id="", seq=0):
    return call(node, "PaymentService", "RefundEscrow", payment_pb2.EscrowActionRequest(
        auction_id=1, client_id=client_id, seq=seq), token=token)


def get_escrow(node, token, auction_id=1):
    return call(node, "PaymentService", "GetEscrow", payment_pb2.GetEscrowRequest(auction_id=auction_id), token=token)


def escrows(cluster):
    return {nid: n.snapshot()["auctions"][1].get("escrow") for nid, n in cluster.nodes.items()}


def gateway_ops(node):
    return [c[0] for c in node.gateway.calls]


# --- pay -------------------------------------------------------------------------------


def test_the_winner_pays_and_every_node_holds_the_same_escrow(cluster):
    leader, t = auction_with_winner(cluster)

    response = pay(leader, t["mrudul"])

    assert response.result.success, response.result.reason
    assert response.reference.startswith("PAY-")
    cluster.wait_converged()
    held = escrows(cluster)
    assert held["n1"] == held["n2"] == held["n3"]
    assert held["n1"]["status"] == "HELD" and held["n1"]["amount"] == 150
    assert held["n1"]["payer"] == "mrudul" and held["n1"]["payment_ref"] == response.reference


def test_the_gateway_is_called_by_the_leader_only_and_exactly_once(cluster):
    leader, t = auction_with_winner(cluster)

    pay(leader, t["mrudul"])
    cluster.wait_converged()

    assert gateway_ops(leader) == ["authorize"]
    assert all(gateway_ops(n) == [] for n in cluster.followers())


def test_the_gateway_reference_is_replicated_as_data(cluster):
    """Followers never talk to the gateway, yet hold the leader's reference."""
    leader, t = auction_with_winner(cluster)

    ref = pay(leader, t["mrudul"]).reference
    cluster.wait_converged()

    assert all(n.snapshot()["auctions"][1]["escrow"]["payment_ref"] == ref for n in cluster.followers())


@pytest.mark.parametrize("who, amount, why", [
    ("karan", 150, "only the winner"),     # not the winner (outbid)
    ("tanmay", 150, "only the winner"),    # never bid
    ("nisarg", 150, "only the winner"),    # the seller
    ("mrudul", 149, "150"),                # wrong amount
    ("mrudul", 151, "150"),
])
def test_a_refused_payment_never_reaches_the_gateway(cluster, who, amount, why):
    leader, t = auction_with_winner(cluster)

    response = pay(leader, t[who], amount=amount)

    assert not response.result.success and why in response.result.reason
    assert response.reference == ""
    assert leader.gateway.calls == []
    cluster.wait_converged()
    assert all(e is None for e in escrows(cluster).values())


def test_paying_before_the_close_is_refused_and_the_gateway_is_not_called(cluster):
    leader = cluster.start("n1")
    seller, bidder = login(leader, "nisarg"), login(leader, "karan")
    call(leader, "AuctionService", "CreateAuction",
         auction_pb2.CreateAuctionRequest(auction_id=1, item="x", close_time=CLOSE), token=seller)
    call(leader, "AuctionService", "PlaceBid", auction_pb2.PlaceBidRequest(auction_id=1, amount=10), token=bidder)

    response = pay(leader, bidder, amount=10)

    assert not response.result.success and "not closed" in response.result.reason
    assert leader.gateway.calls == []


def test_a_gateway_decline_is_reported_and_nothing_is_proposed(cluster):
    leader, t = auction_with_winner(cluster)
    leader.gateway.decline_over = 100  # the winning bid is 150
    entries_before = len(leader.raft.log_entries())

    response = pay(leader, t["mrudul"])

    assert not response.result.success and "declined" in response.result.reason
    assert response.reference == ""
    assert len(leader.raft.log_entries()) == entries_before  # nothing went to the log
    assert all(e is None for e in escrows(cluster).values())


def test_a_second_payment_is_refused_and_does_not_reach_the_gateway(cluster):
    leader, t = auction_with_winner(cluster)
    first = pay(leader, t["mrudul"])

    second = pay(leader, t["mrudul"])

    assert first.result.success and not second.result.success and "HELD" in second.result.reason
    assert gateway_ops(leader) == ["authorize"]
    assert get_escrow(leader, t["mrudul"]).escrow.payment_ref == first.reference


def test_a_retried_payment_is_answered_from_the_dedupe_table_not_charged_again(cluster):
    """Same (client_id, seq) twice, as after an unknown outcome. The second
    call must return the first call's success, the same reference, and must
    not reach the gateway a second time."""
    leader, t = auction_with_winner(cluster)

    first = pay(leader, t["mrudul"], client_id="m-1", seq=5)
    again = pay(leader, t["mrudul"], client_id="m-1", seq=5)

    assert first.result.success and again.result.success
    assert again.reference == first.reference  # read back from the replicated escrow record
    assert gateway_ops(leader) == ["authorize"]
    cluster.wait_converged()
    assert all(e["status"] == "HELD" for e in escrows(cluster).values())


def test_apply_refusing_an_authorized_payment_voids_it(cluster, monkeypatch):
    """The pre-check is advisory. Force it to pass for a non-winner: the
    gateway authorizes, apply() refuses, and the handler must void the hold."""
    import server.payment_servicer as module

    leader, t = auction_with_winner(cluster)
    monkeypatch.setattr(module, "payment_rejection", lambda *a, **k: None)

    response = pay(leader, t["karan"], client_id="k-1", seq=1)  # karan is not the winner

    assert not response.result.success and "only the winner" in response.result.reason
    assert gateway_ops(leader) == ["authorize", "void"]
    held_ref = MockPaymentGateway().authorize("karan", 150, 1, idempotency_key="k-1:1").reference
    assert leader.gateway.calls[1] == ("void", held_ref)  # the void is for the very hold that was granted
    cluster.wait_converged()
    assert all(e is None for e in escrows(cluster).values())


def test_a_follower_redirects_and_never_calls_the_gateway(cluster):
    leader, t = auction_with_winner(cluster)
    follower = cluster.nodes["n2"]

    with pytest.raises(RpcAbort) as e:
        pay(follower, t["mrudul"])

    assert e.value.code == grpc.StatusCode.FAILED_PRECONDITION
    assert e.value.trailing["leader-addr"] == CLIENT_ADDRS["n1"]
    assert follower.gateway.calls == []


def test_an_unknown_outcome_is_unavailable(cluster):
    leader, t = auction_with_winner(cluster)
    cluster.net.partition(["n1"], ["n2", "n3"])

    with pytest.raises(RpcAbort) as e:
        pay(leader, t["mrudul"], client_id="m", seq=1)

    assert e.value.code == grpc.StatusCode.UNAVAILABLE


# --- release and refund -------------------------------------------------------------------


def test_the_seller_releases_held_money_on_every_node(cluster):
    leader, t = auction_with_winner(cluster)
    ref = pay(leader, t["mrudul"]).reference

    response = release(leader, t["nisarg"])

    assert response.result.success, response.result.reason
    assert response.reference.startswith("REL-")
    cluster.wait_converged()
    for e in escrows(cluster).values():
        assert (e["status"], e["payment_ref"], e["settlement_ref"]) == ("RELEASED", ref, response.reference)
    assert gateway_ops(leader) == ["authorize", "release"]


def test_the_seller_refunds_held_money_on_every_node(cluster):
    leader, t = auction_with_winner(cluster)
    pay(leader, t["mrudul"])

    response = refund(leader, t["nisarg"])

    assert response.result.success, response.result.reason
    assert response.reference.startswith("RFD-")
    cluster.wait_converged()
    assert all(e["status"] == "REFUNDED" for e in escrows(cluster).values())
    assert gateway_ops(leader) == ["authorize", "refund"]


@pytest.mark.parametrize("settle", [release, refund])
@pytest.mark.parametrize("who", ["mrudul", "karan", "tanmay"])
def test_only_the_seller_can_settle_and_the_gateway_is_not_called(cluster, settle, who):
    leader, t = auction_with_winner(cluster)
    pay(leader, t["mrudul"])

    response = settle(leader, t[who])

    assert not response.result.success and "only the seller" in response.result.reason
    assert gateway_ops(leader) == ["authorize"]
    assert get_escrow(leader, t["nisarg"]).escrow.status == "HELD"


@pytest.mark.parametrize("settle", [release, refund])
def test_nothing_to_settle_before_payment(cluster, settle):
    leader, t = auction_with_winner(cluster)

    response = settle(leader, t["nisarg"])

    assert not response.result.success and "NONE" in response.result.reason
    assert leader.gateway.calls == []


def test_released_is_final_even_through_the_handlers(cluster):
    leader, t = auction_with_winner(cluster)
    pay(leader, t["mrudul"])
    assert release(leader, t["nisarg"]).result.success

    assert not refund(leader, t["nisarg"]).result.success
    assert not release(leader, t["nisarg"]).result.success
    assert not pay(leader, t["mrudul"]).result.success
    assert gateway_ops(leader) == ["authorize", "release"]
    assert get_escrow(leader, t["nisarg"]).escrow.status == "RELEASED"


def test_a_retried_release_is_answered_from_the_dedupe_table(cluster):
    leader, t = auction_with_winner(cluster)
    pay(leader, t["mrudul"])

    first = release(leader, t["nisarg"], client_id="n-1", seq=3)
    again = release(leader, t["nisarg"], client_id="n-1", seq=3)

    assert first.result.success and again.result.success
    assert gateway_ops(leader) == ["authorize", "release"]


# --- reading the escrow ------------------------------------------------------------------


def test_get_escrow_reports_none_for_an_unpaid_auction_and_the_seller_and_winner(cluster):
    leader, t = auction_with_winner(cluster)

    view = get_escrow(leader, t["karan"])  # anyone logged in may look

    assert view.result.success
    assert (view.escrow.status, view.escrow.amount, view.escrow.seller, view.escrow.winner) == ("NONE", 0, "nisarg", "mrudul")


def test_get_escrow_of_an_unknown_auction_is_a_business_no(cluster):
    leader, t = auction_with_winner(cluster)

    view = get_escrow(leader, t["karan"], auction_id=99)

    assert not view.result.success and "no such auction" in view.result.reason


def test_get_escrow_on_a_follower_redirects(cluster):
    leader, t = auction_with_winner(cluster)

    with pytest.raises(RpcAbort) as e:
        get_escrow(cluster.nodes["n3"], t["karan"])

    assert e.value.code == grpc.StatusCode.FAILED_PRECONDITION


# --- the failover test the chunk asks for -------------------------------------------------


def test_pay_kill_the_leader_the_new_leader_shows_the_same_escrow(cluster):
    leader, t = auction_with_winner(cluster)
    paid = pay(leader, t["mrudul"], client_id="m-1", seq=9)
    assert paid.result.success
    cluster.wait_converged()
    before = get_escrow(leader, t["karan"]).escrow

    cluster.crash("n1")
    cluster.make_candidate("n2")
    new_leader = cluster.wait_for_leader(among=["n2"])

    after = get_escrow(new_leader, t["karan"]).escrow  # same token: sessions are replicated
    assert (after.status, after.amount, after.payer, after.payment_ref) == (
        "HELD", 150, "mrudul", paid.reference)
    assert after == before

    # The old leader's client retries its pay on the new leader: answered
    # from the replicated dedupe table. The new gateway is never called.
    retry = pay(new_leader, t["mrudul"], client_id="m-1", seq=9)
    assert retry.result.success
    assert new_leader.gateway.calls == []

    # And the escrow keeps working on the new leader.
    assert release(new_leader, t["nisarg"]).result.success
    cluster.wait_converged()
    assert all(e["status"] == "RELEASED" for e in escrows(cluster).values())


def test_pay_on_the_new_leader_when_the_old_one_died_before_the_payment(cluster):
    """The unknown-outcome case: the old leader dies, the client retries the
    same (client_id, seq) on the new leader, which authorizes with its OWN
    gateway. Same key, same inputs: the same reference."""
    leader, t = auction_with_winner(cluster)
    key_ref = MockPaymentGateway().authorize("mrudul", 150, 1, idempotency_key="m-1:9").reference
    cluster.crash("n1")
    cluster.make_candidate("n2")
    new_leader = cluster.wait_for_leader(among=["n2"])

    retry = pay(new_leader, t["mrudul"], client_id="m-1", seq=9)

    assert retry.result.success, retry.result.reason
    assert retry.reference == key_ref  # deterministic from the idempotency key
    assert gateway_ops(new_leader) == ["authorize"]


# --- through the real ClusterClient (post / get are tested in test_client_post_get.py) ----


def client_for(cluster):
    return ClusterClient(ADDRS, invoker=in_memory_invoker(cluster), backoff=(0.01, 0.1))


def test_the_cluster_client_pays_and_reads_the_escrow_across_a_leader_kill(cluster):
    leader, _ = auction_with_winner(cluster)
    seller, winner = client_for(cluster), client_for(cluster)
    seller.login("nisarg", "pass")
    winner.login("mrudul", "pass")

    paid = winner.pay(1, 150)
    assert paid.result.success, paid.result.reason
    cluster.crash("n1")
    cluster.make_candidate("n2")
    cluster.wait_for_leader(among=["n2"])  # as the sibling failover tests do: the election is not what this tests

    view = seller.get_escrow(1)  # first attempt hits the dead node, then finds the leader

    assert view.escrow.status == "HELD" and view.escrow.payment_ref == paid.reference
    assert seller.release_escrow(1).result.success
    assert winner.get_escrow(1).escrow.status == "RELEASED"
    assert wait_until(lambda: cluster.leader().id == "n2")
