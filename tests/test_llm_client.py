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
        self.timeouts = []  # the `timeout=` each call was made with
        self.answer = "canned answer"
        self.error = None

    def _respond(self, method, request, timeout=None):
        self.calls.append((method, request))
        self.timeouts.append(timeout)
        if self.error is not None:
            raise self.error
        return llm_pb2.LLMAnswerResponse(
            request_id=request.request_id, answer=self.answer
        )

    def AnswerBiddingQuestion(self, request, timeout=None):
        return self._respond("AnswerBiddingQuestion", request, timeout)

    def GenerateItemDescription(self, request, timeout=None):
        return self._respond("GenerateItemDescription", request, timeout)

    def SummariseAuctionResult(self, request, timeout=None):
        return self._respond("SummariseAuctionResult", request, timeout)

    def SummarisePriceTrends(self, request, timeout=None):
        return self._respond("SummarisePriceTrends", request, timeout)


@pytest.fixture
def stub():
    return FakeStub()


@pytest.fixture
def client(stub):
    return LLMClient(stub=stub)


def test_query_present_dispatches_to_answer_bidding_question(client, stub):
    request_id, answer = client.get_llm_answer(
        request_id="r1", query="am I still leading?", context={}
    )

    # The handout's getLLMAnswer returns (requestId, answer).
    assert (request_id, answer) == ("r1", "canned answer")
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


# --- chunk 7: the handout's return value, the fourth job, and the deadline ---------------


def test_the_returned_request_id_is_the_one_the_server_echoed(client, stub):
    """Not just the one we sent: the pair comes from the response, so a
    server that answered a different request would show."""
    request_id, answer = client.get_llm_answer(request_id="r7", query="q?", context={})

    assert request_id == "r7" and answer == "canned answer"


def test_price_history_dispatches_to_summarise_price_trends(client, stub):
    points = [
        {"auction_id": 1, "item": "lamp", "opening_bid": 10, "winning_bid": 40, "bid_count": 3},
        {"auction_id": 2, "item": "chair", "opening_bid": 5, "winning_bid": 80, "bid_count": 6},
    ]

    request_id, answer = client.get_llm_answer(request_id="r8", query="", context={"price_history": points})

    method, request = stub.calls[0]
    assert method == "SummarisePriceTrends"
    assert request.request_id == "r8"
    assert [(a.auction_id, a.item, a.opening_bid, a.winning_bid, a.bid_count) for a in request.auctions] == [
        (1, "lamp", 10, 40, 3), (2, "chair", 5, 80, 6)]
    assert (request_id, answer) == ("r8", "canned answer")


@pytest.mark.parametrize("query, context", [
    ("a question?", {}),
    ("", {"item": "clock", "attributes": {"a": "b"}}),
    ("", {"auction": {"auction_id": 1, "item": "x", "close_time": 1, "closed": True,
                      "current_high_bid": 0, "bid_count": 0}, "bid_history": []}),
    ("", {"price_history": []}),
])
def test_every_job_is_called_with_the_deadline(stub, query, context):
    client = LLMClient(stub=stub, deadline=3.5)

    client.get_llm_answer(request_id="r", query=query, context=context)

    assert stub.timeouts == [3.5]


def test_the_default_deadline_is_the_one_in_the_llm_config(stub):
    from llm_server import config

    LLMClient(stub=stub).get_llm_answer(request_id="r", query="q?", context={})

    assert stub.timeouts == [config.APP_DEADLINE_SECONDS]
    assert config.APP_DEADLINE_SECONDS < config.TIMEOUT_SECONDS  # the app gives up before Ollama's own backstop


def test_a_hung_llm_server_raises_deadline_exceeded_at_the_deadline():
    """The real thing, not a fake: a gRPC LLM server whose handlers sleep.
    The client must give up at its own deadline with DEADLINE_EXCEEDED, long
    before the server answers."""
    import time
    from concurrent import futures

    from generated import llm_pb2_grpc

    release = __import__("threading").Event()

    class Hung(llm_pb2_grpc.LLMServiceServicer):
        def AnswerBiddingQuestion(self, request, context):
            release.wait(10)
            return llm_pb2.LLMAnswerResponse(request_id=request.request_id, answer="too late")

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    llm_pb2_grpc.add_LLMServiceServicer_to_server(Hung(), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    client = LLMClient(f"127.0.0.1:{port}", deadline=0.4)
    try:
        started = time.monotonic()
        with pytest.raises(grpc.RpcError) as e:
            client.get_llm_answer(request_id="r", query="q?", context={})
        elapsed = time.monotonic() - started
    finally:
        release.set()
        server.stop(grace=None).wait()

    assert e.value.code() == grpc.StatusCode.DEADLINE_EXCEEDED
    assert elapsed < 3.0
