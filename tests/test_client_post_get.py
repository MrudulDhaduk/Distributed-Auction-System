"""Specifies ClusterClient.post(token, type, data) and .get(token, type,
params): the client functions the assignment prescribes (R11).

The wire stays typed (proto/auction.proto and payment.proto say why); post
and get are real adapters on top, in the same spirit as
server/llm_client.py's get_llm_answer. They dispatch on `type` to the typed
methods, send exactly the token they were GIVEN (not whatever login() stored),
and refuse an unknown type or a missing field with a ValueError that names
what was wrong, before any RPC is made.

    post  "auction"  {auction_id, item, close_time[, attributes]}
          "bid"      {auction_id, amount}
          "payment"  {auction_id, action: "pay" | "release" | "refund"[, amount]}
    get   "auction"  {auction_id}
          "auctions" {[include_closed]}
          "escrow"   {auction_id}
"""

import datetime

import pytest

from client.auction_client import ClusterClient
from common import time_conv
from tests.auction_cluster import CLIENT_ADDRS, AuctionCluster, in_memory_invoker
from tests.test_escrow_on_raft import auction_with_winner

ADDRS = [CLIENT_ADDRS[n] for n in ("n1", "n2", "n3")]
CLOSE = time_conv.dt_to_micros(time_conv.now_utc() + datetime.timedelta(hours=1))


@pytest.fixture
def cluster():
    c = AuctionCluster()
    yield c
    c.stop()


def make_client(cluster):
    return ClusterClient(ADDRS, invoker=in_memory_invoker(cluster), backoff=(0.01, 0.1))


def logged_in(cluster, user):
    client = make_client(cluster)
    assert client.login(user, "pass").result.success
    return client


def test_post_auction_and_bid_and_get_auction(cluster):
    cluster.start("n1")
    seller, bidder = logged_in(cluster, "nisarg"), logged_in(cluster, "karan")

    created = seller.post(seller.token, "auction", {"auction_id": 5, "item": "lamp", "close_time": CLOSE})
    bid = bidder.post(bidder.token, "bid", {"auction_id": 5, "amount": 40})
    auction = bidder.get(bidder.token, "auction", {"auction_id": 5})

    assert created.result.success and bid.result.success
    assert (auction.item, auction.current_high_bid, auction.current_high_bidder) == ("lamp", 40, "karan")


def test_get_auctions_lists_open_auctions(cluster):
    cluster.start("n1")
    seller = logged_in(cluster, "nisarg")
    seller.post(seller.token, "auction", {"auction_id": 5, "item": "lamp", "close_time": CLOSE})

    listed = seller.get(seller.token, "auctions", {})

    assert [a.auction_id for a in listed] == [5]


def test_post_sends_the_token_it_is_given_not_the_logged_in_one(cluster):
    """post(token, ...) means what it says: the bid is placed as the owner of
    THAT token."""
    cluster.start("n1")
    seller, karan = logged_in(cluster, "nisarg"), logged_in(cluster, "karan")
    seller.post(seller.token, "auction", {"auction_id": 5, "item": "lamp", "close_time": CLOSE})

    # seller's client object, karan's token
    seller.post(karan.token, "bid", {"auction_id": 5, "amount": 40})

    assert seller.get(seller.token, "auction", {"auction_id": 5}).current_high_bidder == "karan"


def test_post_payment_pay_release_and_refund_and_get_escrow(cluster):
    leader, _ = auction_with_winner(cluster)
    seller, winner = logged_in(cluster, "nisarg"), logged_in(cluster, "mrudul")

    paid = winner.post(winner.token, "payment", {"auction_id": 1, "action": "pay", "amount": 150})
    held = winner.get(winner.token, "escrow", {"auction_id": 1})
    released = seller.post(seller.token, "payment", {"auction_id": 1, "action": "release"})
    done = winner.get(winner.token, "escrow", {"auction_id": 1})

    assert paid.result.success and held.escrow.status == "HELD"
    assert released.result.success and done.escrow.status == "RELEASED"


def test_post_payment_refund(cluster):
    leader, _ = auction_with_winner(cluster)
    seller, winner = logged_in(cluster, "nisarg"), logged_in(cluster, "mrudul")
    winner.post(winner.token, "payment", {"auction_id": 1, "action": "pay", "amount": 150})

    refunded = seller.post(seller.token, "payment", {"auction_id": 1, "action": "refund"})

    assert refunded.result.success
    assert seller.get(seller.token, "escrow", {"auction_id": 1}).escrow.status == "REFUNDED"


def test_each_post_takes_a_new_serial_number_and_a_get_takes_none(cluster):
    leader, _ = auction_with_winner(cluster)
    winner = logged_in(cluster, "mrudul")

    winner.post(winner.token, "payment", {"auction_id": 1, "action": "pay", "amount": 150})
    winner.get(winner.token, "escrow", {"auction_id": 1})

    pays = [r for _, m, r in winner.sent if m == "Pay"]
    reads = [r for _, m, r in winner.sent if m == "GetEscrow"]
    assert len(pays) == 1 and pays[0].client_id == winner.client_id and pays[0].seq > 0
    assert not hasattr(reads[0], "seq")


@pytest.mark.parametrize("method, args", [
    ("post", ("nonsense", {})),
    ("get", ("nonsense", {})),
    ("post", ("auction", {})),                                    # missing keys
    ("post", ("bid", {"auction_id": 1})),                          # missing amount
    ("post", ("payment", {"auction_id": 1, "action": "dance"})),   # unknown action
    ("post", ("payment", {"auction_id": 1, "action": "pay"})),     # pay needs an amount
    ("get", ("escrow", {})),
])
def test_a_bad_type_or_missing_field_is_a_value_error_before_any_rpc(cluster, method, args):
    cluster.start("n1")
    client = logged_in(cluster, "karan")
    client.sent.clear()

    with pytest.raises(ValueError):
        getattr(client, method)(client.token, *args)

    assert client.sent == []


def test_the_value_error_names_the_problem(cluster):
    cluster.start("n1")
    client = logged_in(cluster, "karan")

    with pytest.raises(ValueError, match="amount"):
        client.post(client.token, "bid", {"auction_id": 1})
    with pytest.raises(ValueError, match="nonsense"):
        client.get(client.token, "nonsense", {})


# --- the LLM assistant types and processBusinessRequest (chunk 7, step 2) ---------------------


def test_post_description_and_get_assistant_and_get_price_trends(cluster):
    leader = cluster.start("n1")
    leader.llm.answer = "A 1970s Seiko."
    seller, bidder = logged_in(cluster, "nisarg"), logged_in(cluster, "karan")
    seller.post(seller.token, "auction", {"auction_id": 5, "item": "clock", "close_time": CLOSE})

    described = seller.post(seller.token, "description", {"auction_id": 5, "attributes": {"brand": "Seiko"}})
    answered = bidder.get(bidder.token, "assistant", {"question": "Can I bid one more?", "auction_id": 5})
    general = bidder.get(bidder.token, "assistant", {"question": "What is a reserve price?"})
    trends = bidder.get(bidder.token, "price_trends", {})

    assert described.result.success and described.description == "A 1970s Seiko."
    assert bidder.get(bidder.token, "auction", {"auction_id": 5}).description == "A 1970s Seiko."
    assert answered.result.success and answered.answer == "A 1970s Seiko."
    assert general.result.success
    assert trends.result.success and "no closed auctions" in trends.result.reason


def test_a_get_assistant_without_a_question_is_a_value_error_before_any_rpc(cluster):
    cluster.start("n1")
    client = logged_in(cluster, "karan")
    client.sent.clear()

    with pytest.raises(ValueError, match="question"):
        client.get(client.token, "assistant", {})
    with pytest.raises(ValueError, match="attributes"):
        client.post(client.token, "description", {"auction_id": 1})

    assert client.sent == []


def test_process_business_request_dispatches_and_echoes_the_request_id(cluster):
    """The handout's processBusinessRequest(requestId, payload, context) as a real
    function: (request_id, response) back, the token from `context`."""
    cluster.start("n1")
    seller, karan = logged_in(cluster, "nisarg"), logged_in(cluster, "karan")

    rid, created = seller.process_business_request(
        "req-1", {"method": "post", "type": "auction", "data": {"auction_id": 5, "item": "lamp", "close_time": CLOSE}},
        {"token": seller.token})
    rid2, bid = karan.process_business_request(
        "req-2", {"method": "post", "type": "bid", "data": {"auction_id": 5, "amount": 40}}, {"token": karan.token})
    rid3, auction = karan.process_business_request(
        "req-3", {"method": "get", "type": "auction", "data": {"auction_id": 5}}, {"token": karan.token})

    assert (rid, rid2, rid3) == ("req-1", "req-2", "req-3")
    assert created.result.success and bid.result.success
    assert (auction.item, auction.current_high_bidder) == ("lamp", "karan")


def test_process_business_request_uses_the_context_token_not_the_clients(cluster):
    cluster.start("n1")
    seller, karan = logged_in(cluster, "nisarg"), logged_in(cluster, "karan")
    seller.post(seller.token, "auction", {"auction_id": 5, "item": "lamp", "close_time": CLOSE})

    seller.process_business_request(
        "r", {"method": "post", "type": "bid", "data": {"auction_id": 5, "amount": 40}}, {"token": karan.token})

    assert seller.get(seller.token, "auction", {"auction_id": 5}).current_high_bidder == "karan"


@pytest.mark.parametrize("payload", [
    {},                                           # no method or type
    {"method": "post"},                           # no type
    {"method": "delete", "type": "auction"},      # unknown method
    {"method": "get", "type": "nonsense"},        # unknown type
])
def test_process_business_request_refuses_a_bad_payload_before_any_rpc(cluster, payload):
    cluster.start("n1")
    client = logged_in(cluster, "karan")
    client.sent.clear()

    with pytest.raises(ValueError):
        client.process_business_request("r", payload, {"token": client.token})

    assert client.sent == []
