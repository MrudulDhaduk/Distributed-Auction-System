"""Specifies LLMClient: the adapter that makes the handout's
getLLMAnswer(requestId, query, context) signature real, and its dispatch
onto the three typed RPCs.

No network anywhere in this file. FakeStub stands in for LLMServiceStub --
it records which RPC was called with what request and returns a canned
LLMAnswerResponse, or raises whatever the test asks it to. That is the
whole reason LLMClient takes its stub by injection (server/llm_client.py).
"""

import grpc
import pytest

from generated import llm_pb2
from server.llm_client import LLMClient


class FakeStub:
    def __init__(self):
        self.calls = []
        self.answer = "canned answer"
        self.error = None

    def _respond(self, method, request):
        self.calls.append((method, request))
        if self.error is not None:
            raise self.error
        return llm_pb2.LLMAnswerResponse(
            request_id=request.request_id, answer=self.answer
        )

    def AnswerBiddingQuestion(self, request):
        return self._respond("AnswerBiddingQuestion", request)

    def GenerateItemDescription(self, request):
        return self._respond("GenerateItemDescription", request)

    def SummariseAuctionResult(self, request):
        return self._respond("SummariseAuctionResult", request)


@pytest.fixture
def stub():
    return FakeStub()


@pytest.fixture
def client(stub):
    return LLMClient(stub=stub)


def test_query_present_dispatches_to_answer_bidding_question(client, stub):
    answer = client.get_llm_answer(
        request_id="r1", query="am I still leading?", context={}
    )

    assert answer == "canned answer"
    assert len(stub.calls) == 1
    method, request = stub.calls[0]
    assert method == "AnswerBiddingQuestion"
    assert request.request_id == "r1"
    assert request.question == "am I still leading?"
    assert not request.HasField("auction")


def test_query_present_with_auction_context_attaches_it(client, stub):
    auction = {
        "auction_id": 7,
        "item": "vintage clock",
        "close_time": 123,
        "closed": False,
        "current_high_bid": 500,
        "bid_count": 3,
    }
    client.get_llm_answer(
        request_id="r2", query="how much competition is there?",
        context={"auction": auction},
    )

    _, request = stub.calls[0]
    assert request.HasField("auction")
    assert request.auction.auction_id == 7
    assert request.auction.bid_count == 3


def test_attributes_dispatches_to_generate_item_description(client, stub):
    client.get_llm_answer(
        request_id="r3",
        query="",
        context={"item": "1987 Fender Stratocaster", "attributes": {"condition": "mint"}},
    )

    method, request = stub.calls[0]
    assert method == "GenerateItemDescription"
    assert request.request_id == "r3"
    assert request.item == "1987 Fender Stratocaster"
    assert list(request.attributes) == [llm_pb2.Attribute(key="condition", value="mint")]


def test_bid_history_dispatches_to_summarise_auction_result(client, stub):
    auction = {
        "auction_id": 7,
        "item": "vintage clock",
        "close_time": 123,
        "closed": True,
        "current_high_bid": 500,
        "bid_count": 2,
    }
    bid_history = [
        {"bidder": "alice", "amount": 400, "time": 100},
        {"bidder": "bob", "amount": 500, "time": 200},
    ]
    client.get_llm_answer(
        request_id="r4", query="", context={"auction": auction, "bid_history": bid_history}
    )

    method, request = stub.calls[0]
    assert method == "SummariseAuctionResult"
    assert request.auction.auction_id == 7
    assert [b.bidder for b in request.bid_history] == ["alice", "bob"]


def test_empty_query_and_unrecognised_context_raises():
    client = LLMClient(stub=FakeStub())

    with pytest.raises(ValueError):
        client.get_llm_answer(request_id="r5", query="", context={})


def test_rpc_error_propagates_uncaught(client, stub):
    """Degrading on an outage is the CALLER's job (auction_servicer.py),
    not this adapter's -- see the module docstring. LLMClient must let
    grpc.RpcError through untouched."""
    stub.error = grpc.RpcError()

    with pytest.raises(grpc.RpcError):
        client.get_llm_answer(
            request_id="r6", query="", context={"item": "clock", "attributes": {"a": "b"}}
        )
