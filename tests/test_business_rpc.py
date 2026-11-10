"""Specifies BusinessService.ProcessBusinessRequest(requestId, payload, context)
(R12): the assignment's server-side entry point, as a real RPC on the app server.

It adds no logic of its own. It parses the payload and DISPATCHES to the
handlers that already exist (AuctionServicer, PaymentServicer, AssistantServicer),
passing them the SAME gRPC context. So, by construction:

  * AUTH is the interceptor's: the session token is read from gRPC metadata,
    like every other RPC. A token inside `context` or `payload` is never used.
  * IDENTITY comes from the validated token, never from the payload.
  * LEADER RULES are the handlers': a follower redirects (FAILED_PRECONDITION +
    leader-addr), an unknown outcome is UNAVAILABLE, reads wait for a read
    barrier, the model is called only on the leader.
  * WRITES are deduplicated by the request's (client_id, seq), exactly as the
    typed RPCs are.

payload is JSON text: {"method": "post"|"get", "type": ..., "data": {...}}.
The response carries the dispatched handler's own typed response in an Any.

Real interceptor + servicers over the in-memory 3-node cluster, plus one test
over real gRPC. The model is FakeLLMClient.
"""

import datetime
import json

import grpc
import pytest

from client.auction_client import GET_TYPES, POST_TYPES, ClusterClient
from common import time_conv
from generated import (
    assistant_pb2,
    auction_pb2,
    business_pb2,
    payment_pb2,
)
from server.business_servicer import GET_DISPATCH, POST_DISPATCH
from tests.auction_cluster import (
    CLIENT_ADDRS,
    AuctionCluster,
    RpcAbort,
    call,
    in_memory_invoker,
)
from tests.test_escrow_on_raft import auction_with_winner, login
from tests.test_serve_grpc import FIRST, grpc_cluster  # noqa: F401 (fixture)

CLOSE = time_conv.dt_to_micros(time_conv.now_utc() + datetime.timedelta(hours=1))


@pytest.fixture
def cluster():
    c = AuctionCluster()
    yield c
    c.stop()


def rpc(node, token, method, type_, data=None, *, request_id="req-1", client_id="", seq=0, raw=None, context=None):
    """One ProcessBusinessRequest through the node's interceptor."""
    payload = raw if raw is not None else json.dumps({"method": method, "type": type_, "data": data or {}})
    request = business_pb2.BusinessRequest(
        request_id=request_id, payload=payload, context=context or {}, client_id=client_id, seq=seq)
    return call(node, "BusinessService", "ProcessBusinessRequest", request, token=token)


def unpack(response, message_type):
    message = message_type()
    assert response.response.Unpack(message), f"response is {response.response.TypeName()}, not {message_type.__name__}"
    return message


def open_auction(cluster, auction_id=1):
    leader = cluster.start("n1")
    seller, karan = login(leader, "nisarg"), login(leader, "karan")
    created = unpack(
        rpc(leader, seller, "post", "auction", {"auction_id": auction_id, "item": "clock", "close_time": CLOSE}),
        auction_pb2.CreateAuctionResponse)
    assert created.result.success
    cluster.wait_converged()
    return leader, seller, karan


# --- it dispatches, and the answer is the handler's own ----------------------------------------


def test_post_bid_dispatches_to_place_bid_and_echoes_the_request_id(cluster):
    leader, seller, karan = open_auction(cluster)

    response = rpc(leader, karan, "post", "bid", {"auction_id": 1, "amount": 150}, request_id="req-77")

    assert response.request_id == "req-77"
    bid = unpack(response, auction_pb2.PlaceBidResponse)
    assert bid.result.success and (bid.current_high_bid, bid.current_high_bidder) == (150, "karan")
    cluster.wait_converged()
    assert [b.bidder for b in leader.snapshot()["auctions"][1]["bids"]] == ["karan"]


def test_get_auction_dispatches_to_get_auction(cluster):
    leader, seller, karan = open_auction(cluster)
    rpc(leader, karan, "post", "bid", {"auction_id": 1, "amount": 150})

    got = unpack(rpc(leader, karan, "get", "auction", {"auction_id": 1}), auction_pb2.GetAuctionResponse)

    assert got.result.success and (got.auction.item, got.auction.current_high_bid) == ("clock", 150)


def test_get_auctions_dispatches_to_list_auctions(cluster):
    leader, seller, karan = open_auction(cluster)

    listed = unpack(rpc(leader, karan, "get", "auctions", {"include_closed": False}), auction_pb2.ListAuctionsResponse)

    assert [a.auction_id for a in listed.auctions] == [1]


def test_payment_types_dispatch_to_pay_release_refund_and_get_escrow(cluster):
    leader, t = auction_with_winner(cluster)

    paid = unpack(rpc(leader, t["mrudul"], "post", "payment", {"auction_id": 1, "action": "pay", "amount": 150}),
                  payment_pb2.PaymentResponse)
    held = unpack(rpc(leader, t["karan"], "get", "escrow", {"auction_id": 1}), payment_pb2.GetEscrowResponse)
    released = unpack(rpc(leader, t["nisarg"], "post", "payment", {"auction_id": 1, "action": "release"}),
                      payment_pb2.PaymentResponse)

    assert paid.result.success and held.escrow.status == "HELD" and held.escrow.payment_ref == paid.reference
    assert released.result.success and released.reference.startswith("REL-")
    assert leader.gateway.calls[0][0] == "authorize" and leader.gateway.calls[-1][0] == "release"


def test_payment_refund_dispatches(cluster):
    leader, t = auction_with_winner(cluster)
    rpc(leader, t["mrudul"], "post", "payment", {"auction_id": 1, "amount": 150})   # "pay" is the default action

    refunded = unpack(rpc(leader, t["nisarg"], "post", "payment", {"auction_id": 1, "action": "refund"}),
                      payment_pb2.PaymentResponse)

    assert refunded.result.success and refunded.reference.startswith("RFD-")


def test_the_llm_types_dispatch_to_the_assistant_handlers(cluster):
    leader, seller, karan = open_auction(cluster)
    leader.llm.answer = "A 1970s Seiko."

    described = unpack(rpc(leader, seller, "post", "description", {"auction_id": 1, "attributes": {"brand": "Seiko"}}),
                       assistant_pb2.DescriptionResponse)
    answered = unpack(rpc(leader, karan, "get", "assistant", {"question": "Can I bid one more?", "auction_id": 1}),
                      assistant_pb2.AnswerResponse)
    trends = unpack(rpc(leader, karan, "get", "price_trends", {}), assistant_pb2.PriceTrendsResponse)

    assert described.result.success and described.description == "A 1970s Seiko."
    assert answered.result.success and answered.answer == "A 1970s Seiko."
    assert trends.result.success and "no closed auctions" in trends.result.reason
    # two model calls (the description, then the FAQ); trends with no closed auction asks the model nothing
    assert [query for _, query, _ in leader.llm.calls] == ["", "Can I bid one more?"]


def test_every_type_the_client_knows_is_a_type_the_server_dispatches():
    assert set(POST_TYPES) == set(POST_DISPATCH)
    assert set(GET_TYPES) == set(GET_DISPATCH)


# --- same auth ------------------------------------------------------------------------------------


def test_no_token_is_unauthenticated(cluster):
    leader, seller, karan = open_auction(cluster)

    with pytest.raises(RpcAbort) as e:
        rpc(leader, None, "get", "auctions", {})

    assert e.value.code == grpc.StatusCode.UNAUTHENTICATED


def test_an_unknown_token_is_unauthenticated_on_the_leader(cluster):
    leader, seller, karan = open_auction(cluster)

    with pytest.raises(RpcAbort) as e:
        rpc(leader, "not-a-token", "get", "auctions", {})

    assert e.value.code == grpc.StatusCode.UNAUTHENTICATED


def test_a_token_in_the_context_or_payload_is_never_used_for_auth(cluster):
    """Auth is the interceptor's, from metadata only. Neither `context` nor
    `payload` can carry a credential."""
    leader, seller, karan = open_auction(cluster)

    with pytest.raises(RpcAbort) as e:
        rpc(leader, None, "get", "auctions", {"token": karan}, context={"token": karan})

    assert e.value.code == grpc.StatusCode.UNAUTHENTICATED


def test_identity_comes_from_the_token_not_the_payload(cluster):
    leader, seller, karan = open_auction(cluster)

    rpc(leader, karan, "post", "bid", {"auction_id": 1, "amount": 150, "bidder": "nisarg", "user": "nisarg"})

    cluster.wait_converged()
    assert [b.bidder for b in leader.snapshot()["auctions"][1]["bids"]] == ["karan"]


# --- same leader rules ----------------------------------------------------------------------------


@pytest.mark.parametrize("method, type_, data", [
    ("post", "bid", {"auction_id": 1, "amount": 150}),
    ("get", "auction", {"auction_id": 1}),
    ("get", "assistant", {"question": "q?", "auction_id": 1}),
])
def test_a_follower_redirects_and_nothing_happens(cluster, method, type_, data):
    leader, seller, karan = open_auction(cluster)
    follower = cluster.nodes["n2"]
    entries = len(leader.raft.log_entries())

    with pytest.raises(RpcAbort) as e:
        rpc(follower, karan, method, type_, data)

    assert e.value.code == grpc.StatusCode.FAILED_PRECONDITION
    assert e.value.trailing["leader-addr"] == CLIENT_ADDRS["n1"]
    assert follower.llm.calls == []
    assert len(leader.raft.log_entries()) == entries


def test_an_unknown_outcome_is_unavailable(cluster):
    leader, seller, karan = open_auction(cluster)
    cluster.net.partition(["n1"], ["n2", "n3"])

    with pytest.raises(RpcAbort) as e:
        rpc(leader, karan, "post", "bid", {"auction_id": 1, "amount": 150}, client_id="k", seq=1)

    assert e.value.code == grpc.StatusCode.UNAVAILABLE


def test_a_business_refusal_is_in_the_response_not_a_grpc_error(cluster):
    leader, seller, karan = open_auction(cluster)
    rpc(leader, karan, "post", "bid", {"auction_id": 1, "amount": 150})

    low = unpack(rpc(leader, karan, "post", "bid", {"auction_id": 1, "amount": 100}), auction_pb2.PlaceBidResponse)

    assert not low.result.success and "highest bid is 150" in low.result.reason


def test_a_retried_write_is_executed_once(cluster):
    leader, seller, karan = open_auction(cluster)

    first = unpack(rpc(leader, karan, "post", "bid", {"auction_id": 1, "amount": 150}, client_id="k-1", seq=4),
                   auction_pb2.PlaceBidResponse)
    again = unpack(rpc(leader, karan, "post", "bid", {"auction_id": 1, "amount": 150}, client_id="k-1", seq=4),
                   auction_pb2.PlaceBidResponse)

    assert first.result.success and again.result == first.result
    assert len(leader.snapshot()["auctions"][1]["bids"]) == 1


# --- bad payloads are INVALID_ARGUMENT and change nothing ---------------------------------------------


@pytest.mark.parametrize("raw, why", [
    ("this is not json", "JSON"),
    ("[]", "object"),
    ('{"type": "bid", "data": {}}', "method"),
    ('{"method": "delete", "type": "bid", "data": {}}', "delete"),
    ('{"method": "post", "data": {}}', "type"),
    ('{"method": "post", "type": "nonsense", "data": {}}', "nonsense"),
    ('{"method": "get", "type": "bid", "data": {}}', "bid"),              # a post type used as a get
    ('{"method": "post", "type": "bid", "data": []}', "data"),
    ('{"method": "post", "type": "bid", "data": {"auction_id": 1}}', "amount"),
    ('{"method": "post", "type": "bid", "data": {"auction_id": "1", "amount": 5}}', "auction_id"),
    ('{"method": "post", "type": "bid", "data": {"auction_id": 1, "amount": 5.5}}', "amount"),
    ('{"method": "post", "type": "bid", "data": {"auction_id": true, "amount": 5}}', "auction_id"),
    ('{"method": "post", "type": "payment", "data": {"auction_id": 1, "action": "dance"}}', "dance"),
    ('{"method": "post", "type": "payment", "data": {"auction_id": 1}}', "amount"),
    ('{"method": "post", "type": "description", "data": {"auction_id": 1, "attributes": "x"}}', "attributes"),
])
def test_a_bad_payload_is_invalid_argument_and_changes_nothing(cluster, raw, why):
    leader, seller, karan = open_auction(cluster)
    entries = len(leader.raft.log_entries())

    with pytest.raises(RpcAbort) as e:
        rpc(leader, karan, None, None, raw=raw)

    assert e.value.code == grpc.StatusCode.INVALID_ARGUMENT
    assert why in e.value.details
    assert len(leader.raft.log_entries()) == entries and leader.llm.calls == []


# --- the client adapter now calls this RPC -----------------------------------------------------------------


def client_for(cluster):
    return ClusterClient(list(CLIENT_ADDRS.values()), invoker=in_memory_invoker(cluster), backoff=(0.01, 0.1))


def test_process_business_request_on_the_client_uses_the_rpc_not_the_typed_ones(cluster):
    cluster.start("n1")
    karan, seller = client_for(cluster), client_for(cluster)
    karan.login("karan", "pass")
    seller.login("nisarg", "pass")
    seller.post(seller.token, "auction", {"auction_id": 5, "item": "lamp", "close_time": CLOSE})
    karan.sent.clear()

    rid, bid = karan.process_business_request(
        "r-9", {"method": "post", "type": "bid", "data": {"auction_id": 5, "amount": 40}}, {"token": karan.token})
    rid2, auction = karan.process_business_request(
        "r-10", {"method": "get", "type": "auction", "data": {"auction_id": 5}}, {"token": karan.token})

    assert (rid, rid2) == ("r-9", "r-10")
    assert bid.result.success and (auction.item, auction.current_high_bidder) == ("lamp", "karan")
    assert [m for _, m, _ in karan.sent] == ["ProcessBusinessRequest", "ProcessBusinessRequest"]
    post, read = karan.sent[0][2], karan.sent[1][2]
    assert post.client_id == karan.client_id and post.seq > 0     # a write takes a serial number
    assert read.seq == 0 and read.client_id == ""                 # a read takes none


def test_the_client_never_puts_its_token_in_the_payload_or_context(cluster):
    cluster.start("n1")
    karan = client_for(cluster)
    karan.login("karan", "pass")
    karan.sent.clear()

    karan.process_business_request("r", {"method": "get", "type": "auctions"}, {"token": karan.token, "trace": "t-1"})

    request = karan.sent[0][2]
    assert karan.token not in request.payload and "token" not in request.context
    assert dict(request.context) == {"trace": "t-1"}


def test_a_get_of_a_missing_auction_is_none_through_the_rpc_as_through_get(cluster):
    cluster.start("n1")
    karan = client_for(cluster)
    karan.login("karan", "pass")

    _, missing = karan.process_business_request("r", {"method": "get", "type": "auction", "data": {"auction_id": 99}},
                                                {"token": karan.token})

    assert missing is None


def test_the_client_retries_a_business_request_across_a_leader_kill_with_the_same_serial_number(cluster):
    leader, seller, karan_token = open_auction(cluster)
    karan = client_for(cluster)
    karan.login("karan", "pass")
    cluster.crash("n1")
    cluster.make_candidate("n2")
    cluster.wait_for_leader(among=["n2"])

    _, bid = karan.process_business_request(
        "r", {"method": "post", "type": "bid", "data": {"auction_id": 1, "amount": 60}}, {"token": karan.token})

    assert bid.result.success
    seqs = {r.seq for _, m, r in karan.sent if m == "ProcessBusinessRequest"}
    assert len(seqs) == 1


# --- over real gRPC ---------------------------------------------------------------------------------------------


def test_over_real_grpc_a_follower_redirects_and_the_leader_dispatches(grpc_cluster):  # noqa: F811
    from tests.auction_cluster import wait_until

    c = grpc_cluster()
    c.boot("n1", **FIRST)
    c.boot("n2")
    c.boot("n3")
    assert wait_until(lambda: c.leader() is not None and c.leader().node_id == "n1", 10)
    addrs = [c.client_addr(n) for n in ("n3", "n2", "n1")]     # start at a follower
    seller, karan = ClusterClient(addrs, backoff=(0.05, 0.5)), ClusterClient(addrs, backoff=(0.05, 0.5))
    try:
        assert seller.login("nisarg", "pass").result.success and karan.login("karan", "pass").result.success
        _, created = seller.process_business_request(
            "r1", {"method": "post", "type": "auction", "data": {"auction_id": 1, "item": "clock", "close_time": CLOSE}},
            {"token": seller.token})
        _, bid = karan.process_business_request(
            "r2", {"method": "post", "type": "bid", "data": {"auction_id": 1, "amount": 150}}, {"token": karan.token})
        _, auction = karan.process_business_request(
            "r3", {"method": "get", "type": "auction", "data": {"auction_id": 1}}, {"token": karan.token})
        assert created.result.success and bid.result.success
        assert (auction.current_high_bid, auction.current_high_bidder) == (150, "karan")
        assert {m for _, m, _ in karan.sent} == {"Login", "ProcessBusinessRequest"}   # no typed auction RPC
        assert karan.sent[0][0] != c.client_addr("n1")    # the very first attempt (the login) went to a follower
    finally:
        seller.close()
        karan.close()
