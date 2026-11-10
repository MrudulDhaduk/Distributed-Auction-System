"""Specifies server/assistant_servicer.py (chunk 7, step 2; R4, R5, R6, R9):

    AskAssistant         FAQ chatbot, optionally about one auction
    GenerateDescription  the seller asks for a (new) listing text
    GetPriceTrends       a summary of the closed auctions' prices

The rules they all share:

  * LEADER ONLY. A follower redirects (FAILED_PRECONDITION) and never calls
    the model. The leader first confirms it is the leader and has applied
    everything committed (the read barrier), THEN reads the auction state it
    sends to the model as context.
  * The model is called OUTSIDE any lock and OUTSIDE apply(). The FAQ and the
    trends are never written to the log. The description is: the generated
    TEXT goes into a SetDescription command as plain data.
  * An LLM outage (down, slow, failing) is a clear answer in Result, never a
    gRPC error. A gRPC UNAVAILABLE would make ClusterClient retry the same
    call on every node for 30 s. And the auction keeps working.

Real interceptor + servicers, called directly, over an in-memory 3-node
cluster (tests/auction_cluster.py). The model is FakeLLMClient.
"""

import datetime

import grpc
import pytest

from auction.state import CloseAuction
from common import time_conv
from generated import assistant_pb2, auction_pb2
from raft.messages import EntryKind
from replication import codec
from tests.auction_cluster import AuctionCluster, FakeRpcError, RpcAbort, call

NOW = time_conv.now_utc()
CLOSE = time_conv.dt_to_micros(NOW + datetime.timedelta(hours=1))


@pytest.fixture
def cluster():
    c = AuctionCluster()
    yield c
    c.stop()


def login(node, user):
    response = call(node, "AuthService", "Login", auction_pb2.LoginRequest(username=user, password="pass"))
    assert response.result.success, response
    return response.token


def setup(cluster, close=False, bids=(("karan", 100), ("mrudul", 150)), auction_id=1, seller="nisarg"):
    """Leader n1, one auction by `seller`, bids placed, optionally closed.
    Returns (leader, {user: token})."""
    leader = cluster.leader() or cluster.start("n1")
    tokens = {u: login(leader, u) for u in ("nisarg", "karan", "mrudul", "tanmay")}
    assert call(leader, "AuctionService", "CreateAuction", auction_pb2.CreateAuctionRequest(
        auction_id=auction_id, item="vintage clock", close_time=CLOSE), token=tokens[seller]).result.success
    for user, amount in bids:
        assert call(leader, "AuctionService", "PlaceBid", auction_pb2.PlaceBidRequest(
            auction_id=auction_id, amount=amount), token=tokens[user]).result.success
    if close:
        closed = leader.replicator.submit(CloseAuction(auction_id=auction_id, curr_time=NOW + datetime.timedelta(hours=2)))
        assert closed.result.success
    cluster.wait_converged()
    return leader, tokens


def ask(node, token, question="Can I bid one more than the current high bid?", auction_id=None):
    request = assistant_pb2.AskRequest(question=question)
    if auction_id is not None:
        request.auction_id = auction_id
    return call(node, "AssistantService", "AskAssistant", request, token=token)


def describe(node, token, attributes=None, auction_id=1, client_id="", seq=0):
    return call(node, "AssistantService", "GenerateDescription", assistant_pb2.GenerateDescriptionRequest(
        auction_id=auction_id, attributes=attributes if attributes is not None else {"brand": "Seiko"},
        client_id=client_id, seq=seq), token=token)


def trends(node, token, limit=0):
    return call(node, "AssistantService", "GetPriceTrends", assistant_pb2.PriceTrendsRequest(limit=limit), token=token)


def command_kinds(node):
    return [type(codec.decode(e.data).command).__name__
            for e in node.raft.log_entries() if e.kind is EntryKind.COMMAND]


def descriptions(cluster, auction_id=1):
    return {nid: n.snapshot()["auctions"][auction_id]["description"] for nid, n in cluster.nodes.items()}


# --- AskAssistant: the FAQ -------------------------------------------------------------------


def test_the_leader_reads_the_auction_and_sends_it_as_context(cluster):
    leader, t = setup(cluster)
    leader.llm.answer = "Yes. One more than 150 is 151."

    response = ask(leader, t["tanmay"], auction_id=1)

    assert response.result.success, response.result.reason
    assert response.answer == "Yes. One more than 150 is 151."
    request_id, query, context = leader.llm.calls[-1]
    assert response.request_id == request_id  # the id that appears in both logs
    assert query == "Can I bid one more than the current high bid?"
    auction = context["auction"]
    assert (auction["auction_id"], auction["item"], auction["closed"]) == (1, "vintage clock", False)
    assert (auction["current_high_bid"], auction["bid_count"], auction["close_time"]) == (150, 2, CLOSE)


def test_a_closed_flag_comes_from_the_applied_state(cluster):
    leader, t = setup(cluster, close=True)

    ask(leader, t["tanmay"], auction_id=1)

    assert leader.llm.calls[-1][2]["auction"]["closed"] is True


def test_a_general_question_carries_no_auction(cluster):
    leader, t = setup(cluster)

    response = ask(leader, t["tanmay"], question="What is a reserve price?")

    assert response.result.success
    assert leader.llm.calls[-1][2] == {}


def test_an_unknown_auction_is_a_business_no_and_the_model_is_not_called(cluster):
    leader, t = setup(cluster)

    response = ask(leader, t["tanmay"], auction_id=99)

    assert not response.result.success and "no such auction" in response.result.reason
    assert leader.llm.calls == []


@pytest.mark.parametrize("question", ["", "   ", "\n\t"])
def test_an_empty_question_is_a_business_no_and_the_model_is_not_called(cluster, question):
    leader, t = setup(cluster)

    response = ask(leader, t["tanmay"], question=question)

    assert not response.result.success and "empty" in response.result.reason
    assert leader.llm.calls == []


def test_the_faq_never_touches_the_log(cluster):
    leader, t = setup(cluster)
    before = len(leader.raft.log_entries())

    ask(leader, t["tanmay"], auction_id=1)

    assert len(leader.raft.log_entries()) == before


def test_a_follower_redirects_and_never_calls_the_model(cluster):
    leader, t = setup(cluster)
    follower = cluster.nodes["n2"]

    with pytest.raises(RpcAbort) as e:
        ask(follower, t["tanmay"], auction_id=1)

    assert e.value.code == grpc.StatusCode.FAILED_PRECONDITION
    assert "leader-addr" in e.value.trailing
    assert follower.llm.calls == []


def test_a_deposed_leader_does_not_answer_from_stale_state(cluster):
    """The read barrier: a leader cut off from the majority cannot confirm it
    still leads, so it must not read its (possibly stale) state into a prompt."""
    leader, t = setup(cluster)
    cluster.net.partition(["n1"], ["n2", "n3"])

    with pytest.raises(RpcAbort) as e:
        ask(leader, t["tanmay"], auction_id=1)

    assert e.value.code in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.FAILED_PRECONDITION)
    assert leader.llm.calls == []


# --- the LLM is down or slow: a clear answer, and the auction keeps working ----------------------


@pytest.mark.parametrize("code, expected", [
    (grpc.StatusCode.UNAVAILABLE, "unavailable"),
    (grpc.StatusCode.DEADLINE_EXCEEDED, "did not answer in time"),
    (grpc.StatusCode.INTERNAL, "failed"),
])
def test_an_llm_failure_is_a_clear_answer_not_a_grpc_error(cluster, code, expected):
    leader, t = setup(cluster)
    leader.llm.error = FakeRpcError(code, "backend said so")

    response = ask(leader, t["tanmay"], auction_id=1)  # no exception: not an RpcAbort

    assert not response.result.success
    assert expected in response.result.reason
    assert "auction is unaffected" in response.result.reason
    assert response.answer == ""


def test_a_bare_rpc_error_without_a_code_is_still_a_clear_answer(cluster):
    leader, t = setup(cluster)
    leader.llm.error = grpc.RpcError()

    response = ask(leader, t["tanmay"], auction_id=1)

    assert not response.result.success and "assistant" in response.result.reason


def test_bidding_and_closing_keep_working_while_the_llm_is_down(cluster):
    leader, t = setup(cluster)
    leader.llm.error = FakeRpcError(grpc.StatusCode.UNAVAILABLE, "down")
    assert not ask(leader, t["tanmay"], auction_id=1).result.success

    bid = call(leader, "AuctionService", "PlaceBid", auction_pb2.PlaceBidRequest(auction_id=1, amount=200), token=t["tanmay"])
    closed = leader.replicator.submit(CloseAuction(auction_id=1, curr_time=NOW + datetime.timedelta(hours=2)))

    assert bid.result.success and closed.result.success
    assert leader.snapshot()["auctions"][1]["winner"].bidder == "tanmay"


# --- GenerateDescription: on request, replicated as DATA --------------------------------------------


def test_the_generated_text_is_replicated_as_a_set_description_command(cluster):
    leader, t = setup(cluster)
    leader.llm.answer = "A 1970s Seiko wall clock in running order."

    response = describe(leader, t["nisarg"], {"brand": "Seiko", "era": "1970s"})

    assert response.result.success, response.result.reason
    assert response.description == "A 1970s Seiko wall clock in running order."
    cluster.wait_converged()
    assert set(descriptions(cluster).values()) == {"A 1970s Seiko wall clock in running order."}
    # The log holds the TEXT, in a SetDescription. No model call is recorded anywhere in it.
    assert command_kinds(leader).count("SetDescription") == 1
    # The model was asked once, with the item from the replicated state and the seller's attributes.
    assert len(leader.llm.calls) == 1
    _, query, context = leader.llm.calls[0]
    assert query == "" and context == {"item": "vintage clock", "attributes": {"brand": "Seiko", "era": "1970s"}}
    assert [len(n.llm.calls) for n in cluster.followers()] == [0, 0]


def test_only_the_seller_may_ask_and_the_model_is_not_called(cluster):
    leader, t = setup(cluster)

    response = describe(leader, t["karan"])

    assert not response.result.success and "only the seller" in response.result.reason
    assert leader.llm.calls == []
    assert set(descriptions(cluster).values()) == {""}


def test_a_closed_auction_cannot_be_redescribed_and_the_model_is_not_called(cluster):
    leader, t = setup(cluster, close=True)

    response = describe(leader, t["nisarg"])

    assert not response.result.success and "closed" in response.result.reason
    assert leader.llm.calls == []


def test_no_attributes_is_a_business_no_and_the_model_is_not_called(cluster):
    leader, t = setup(cluster)

    response = describe(leader, t["nisarg"], attributes={})

    assert not response.result.success and "attribute" in response.result.reason
    assert leader.llm.calls == []


def test_an_unknown_auction_is_a_business_no(cluster):
    leader, t = setup(cluster)

    response = describe(leader, t["nisarg"], auction_id=99)

    assert not response.result.success and "does not exist" in response.result.reason
    assert leader.llm.calls == []


def test_an_llm_outage_leaves_the_old_description_and_proposes_nothing(cluster):
    leader, t = setup(cluster)
    leader.llm.error = FakeRpcError(grpc.StatusCode.UNAVAILABLE, "down")
    before = len(leader.raft.log_entries())

    response = describe(leader, t["nisarg"])

    assert not response.result.success and "unavailable" in response.result.reason
    assert len(leader.raft.log_entries()) == before
    assert set(descriptions(cluster).values()) == {""}


def test_an_empty_model_answer_is_refused_by_apply_and_changes_nothing(cluster):
    """apply() is the judge of the text too: an empty description is refused."""
    leader, t = setup(cluster)
    leader.llm.answer = "   "

    response = describe(leader, t["nisarg"])

    assert not response.result.success and "empty" in response.result.reason
    assert set(descriptions(cluster).values()) == {""}


def test_a_follower_redirects_a_description_request_and_never_calls_the_model(cluster):
    leader, t = setup(cluster)

    with pytest.raises(RpcAbort) as e:
        describe(cluster.nodes["n3"], t["nisarg"])

    assert e.value.code == grpc.StatusCode.FAILED_PRECONDITION
    assert cluster.nodes["n3"].llm.calls == []


def test_a_retried_description_is_answered_from_the_dedupe_table(cluster):
    leader, t = setup(cluster)
    leader.llm.answer = "first text"
    first = describe(leader, t["nisarg"], client_id="s-1", seq=4)
    leader.llm.answer = "second text"

    again = describe(leader, t["nisarg"], client_id="s-1", seq=4)

    assert first.result.success and again.result.success
    cluster.wait_converged()  # every follower has applied the entry before we compare
    assert set(descriptions(cluster).values()) == {"first text"}  # the retry did not overwrite it


# --- GetPriceTrends -----------------------------------------------------------------------------------


def two_closed_auctions(cluster):
    """Auction 1: bids 100, 150 (winner mrudul 150). Auction 2: bid 40 only. Both closed.
    Auction 3: open. Auction 4: closed with no bids."""
    leader, t = setup(cluster, close=True)
    for auction_id, bids in ((2, [("karan", 40)]), (3, [("karan", 5)]), (4, [])):
        assert call(leader, "AuctionService", "CreateAuction", auction_pb2.CreateAuctionRequest(
            auction_id=auction_id, item=f"item {auction_id}", close_time=CLOSE), token=t["nisarg"]).result.success
        for user, amount in bids:
            call(leader, "AuctionService", "PlaceBid", auction_pb2.PlaceBidRequest(auction_id=auction_id, amount=amount), token=t[user])
    for auction_id in (2, 4):
        leader.replicator.submit(CloseAuction(auction_id=auction_id, curr_time=NOW + datetime.timedelta(hours=2)))
    cluster.wait_converged()
    return leader, t


def test_the_statistics_are_computed_in_code_from_the_closed_auctions_only(cluster):
    leader, t = two_closed_auctions(cluster)
    leader.llm.answer = "Winning bids were 150 and 40."

    response = trends(leader, t["tanmay"])

    assert response.result.success, response.result.reason
    assert response.summary == "Winning bids were 150 and 40."
    s = response.stats
    assert (s.auctions, s.with_bids, s.no_bids) == (3, 2, 1)           # 1, 2 and 4; auction 3 is still open
    assert (s.lowest_winning_bid, s.highest_winning_bid, s.mean_winning_bid, s.total_bids) == (40, 150, 95, 3)
    assert [(p.auction_id, p.opening_bid, p.winning_bid, p.bid_count) for p in response.points] == [
        (1, 100, 150, 2), (2, 40, 40, 1), (4, 0, 0, 0)]


def test_the_model_gets_the_points_as_price_history(cluster):
    leader, t = two_closed_auctions(cluster)

    trends(leader, t["tanmay"])

    _, query, context = leader.llm.calls[-1]
    assert query == ""
    assert [(p["auction_id"], p["item"], p["opening_bid"], p["winning_bid"], p["bid_count"])
            for p in context["price_history"]] == [
        (1, "vintage clock", 100, 150, 2), (2, "item 2", 40, 40, 1), (4, "item 4", 0, 0, 0)]


def test_the_limit_keeps_the_most_recent_closed_auctions(cluster):
    leader, t = two_closed_auctions(cluster)

    response = trends(leader, t["tanmay"], limit=2)

    assert [p.auction_id for p in response.points] == [2, 4]
    assert response.stats.auctions == 2


def test_no_closed_auctions_is_an_answer_and_the_model_is_not_called(cluster):
    leader, t = setup(cluster, close=False)

    response = trends(leader, t["tanmay"])

    assert response.result.success and "no closed auctions" in response.result.reason
    assert response.stats.auctions == 0 and response.summary == ""
    assert leader.llm.calls == []


def test_with_the_llm_down_the_numbers_still_come_back(cluster):
    leader, t = two_closed_auctions(cluster)
    leader.llm.error = FakeRpcError(grpc.StatusCode.DEADLINE_EXCEEDED, "slow")

    response = trends(leader, t["tanmay"])

    assert not response.result.success and "did not answer in time" in response.result.reason
    assert response.summary == ""
    assert (response.stats.auctions, response.stats.highest_winning_bid) == (3, 150)  # computed in code


def test_trends_never_touch_the_log_and_a_follower_redirects(cluster):
    leader, t = two_closed_auctions(cluster)
    before = len(leader.raft.log_entries())

    trends(leader, t["tanmay"])
    with pytest.raises(RpcAbort) as e:
        trends(cluster.nodes["n2"], t["tanmay"])

    assert len(leader.raft.log_entries()) == before
    assert e.value.code == grpc.StatusCode.FAILED_PRECONDITION
    assert cluster.nodes["n2"].llm.calls == []
