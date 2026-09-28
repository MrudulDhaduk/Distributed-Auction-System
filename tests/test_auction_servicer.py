"""Specifies AuctionServicer.CreateAuction's LLM wiring: call first,
replicate second, and degrade on an outage without ever blocking auction
creation (llm.proto's failure note).

Real AuctionState -- it is plain in-memory bookkeeping, not what this file
is testing. FakeLLMClient stands in for server/llm_client.py's LLMClient:
it records what get_llm_answer was called with and returns a canned
string, or raises grpc.RpcError if the test asks it to. No network, no
model, anywhere in this file.
"""

import grpc
import pytest

from auction.state import AuctionState
from generated import auction_pb2
from server.auction_servicer import AuctionServicer


class FakeLLMClient:
    def __init__(self):
        self.calls = []
        self.answer = "a fine specimen"
        self.error = None

    def get_llm_answer(self, request_id, query, context):
        self.calls.append((request_id, query, context))
        if self.error is not None:
            raise self.error
        return self.answer


class FakeContext:
    """Stands in for the gRPC context after the auth interceptor has
    resolved the token to a username."""

    username = "nisarg"


CTX = FakeContext()


@pytest.fixture
def llm():
    return FakeLLMClient()


@pytest.fixture
def servicer(llm):
    return AuctionServicer(AuctionState(), llm)


def test_no_attributes_skips_llm_call_and_creates_with_empty_description(servicer, llm):
    response = servicer.CreateAuction(
        auction_pb2.CreateAuctionRequest(auction_id=1, item="vintage clock", close_time=0),
        context=CTX,
    )

    assert response.result.success
    assert llm.calls == []
    auction = servicer._state.auctions[1]
    assert auction["description"] == ""


def test_owner_comes_from_context_and_is_named_in_the_result(servicer, llm):
    response = servicer.CreateAuction(
        auction_pb2.CreateAuctionRequest(auction_id=1, item="vintage clock", close_time=0),
        context=CTX,
    )

    assert response.result.reason == "Auction created successfully by nisarg"
    assert servicer._state.auctions[1]["owner"] == "nisarg"


def test_attributes_present_calls_llm_and_stores_returned_description(servicer, llm):
    request = auction_pb2.CreateAuctionRequest(
        auction_id=1, item="1987 Fender Stratocaster", close_time=0,
        attributes={"condition": "mint"},
    )
    response = servicer.CreateAuction(request, context=CTX)

    assert response.result.success
    assert len(llm.calls) == 1
    request_id, query, context = llm.calls[0]
    assert query == ""
    assert context == {"item": "1987 Fender Stratocaster", "attributes": {"condition": "mint"}}
    auction = servicer._state.auctions[1]
    assert auction["description"] == "a fine specimen"


def test_llm_outage_degrades_to_empty_description_auction_still_created(servicer, llm):
    llm.error = grpc.RpcError()
    request = auction_pb2.CreateAuctionRequest(
        auction_id=1, item="1987 Fender Stratocaster", close_time=0,
        attributes={"condition": "mint"},
    )

    response = servicer.CreateAuction(request, context=CTX)

    assert response.result.success, "an LLM outage must never block auction creation"
    auction = servicer._state.auctions[1]
    assert auction["description"] == ""


def test_get_auction_returns_stored_description(servicer, llm):
    request = auction_pb2.CreateAuctionRequest(
        auction_id=1, item="1987 Fender Stratocaster", close_time=0,
        attributes={"condition": "mint"},
    )
    servicer.CreateAuction(request, context=CTX)

    response = servicer.GetAuction(
        auction_pb2.GetAuctionRequest(auction_id=1), context=None
    )

    assert response.result.success
    assert response.auction.description == "a fine specimen"
