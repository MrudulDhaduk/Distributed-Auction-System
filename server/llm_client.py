"""The adapter that makes the handout's client-facing signature real.

llm.proto's service comment documents a dispatch:

    getLLMAnswer(requestId, query, context)
        -> getLLMAnswerResponse(requestId, answer)

    getLLMAnswer(id, <bidder's question>, <live auction state>)
                                         -> AnswerBiddingQuestion
    getLLMAnswer(id, -,                   <seller attributes>)
                                         -> GenerateItemDescription
    getLLMAnswer(id, -,                   <closed auction + bid history>)
                                         -> SummariseAuctionResult
    getLLMAnswer(id, -,                   <closed auctions' price history>)
                                         -> SummarisePriceTrends      (chunk 7)

That comment describes the wire-level mapping; it is not itself a callable.
LLMClient.get_llm_answer is the real function with that name and those
three parameters -- see MEMORY on handout-prescribed signatures needing a
real adapter, not just a comment. It holds one LLMServiceStub and picks the
RPC from the *shape* of `query`/`context`, exactly as the comment above
lays out:

    query truthy               -> AnswerBiddingQuestion
    query falsy, "attributes"  -> GenerateItemDescription
      in context
    query falsy, "bid_history" -> SummariseAuctionResult
      in context
    query falsy, "price_history" -> SummarisePriceTrends
      in context

It returns (request_id, answer), as the handout's signature says: the pair
comes from the RESPONSE, so a server that answered a different request would
show. (M1 returned the answer string only; chunk 7 changed it, and the one
caller, AuctionServicer._describe, with it. docs/decisions.md, C12.)

Every call carries a DEADLINE (`deadline`, default llm_config.APP_DEADLINE_SECONDS).
Without one a hung LLM process would hold a handler thread until the client's
own timeout. A call that runs out raises grpc.RpcError with DEADLINE_EXCEEDED
like any other failure; callers treat it as an outage (chunk 7, R9).

`context` is a plain dict here, not an AuctionContext message -- this
module is the one place that knows how to build the right protobuf from
it, so callers (server/auction_servicer.py today) stay untyped-request-in,
plain-string-out, same as the handout's own signature.

Failure handling is NOT this module's job. It raises grpc.RpcError like
any other stub call; llm.proto's failure note says the caller must catch
it and degrade. Swallowing it here would hide that decision from the one
place (auction_servicer.py) that knows what "degrade" means for a
CreateAuction.
"""

import grpc

from generated import llm_pb2, llm_pb2_grpc
from llm_server import config as llm_config


def _to_auction_context(auction: dict) -> llm_pb2.AuctionContext:
    return llm_pb2.AuctionContext(
        auction_id=auction["auction_id"],
        item=auction["item"],
        close_time=auction["close_time"],
        closed=auction["closed"],
        current_high_bid=auction["current_high_bid"],
        bid_count=auction["bid_count"],
    )


class LLMClient:
    """Holds one channel + stub to the LLM server for the lifetime of the
    process, same shape as any other gRPC client in this repo.

    The stub is injected so the test suite can pass a fake and run with no
    network (same pattern as LLMServicer's injected Ollama client). Passing
    None builds a real channel + stub against `address`.
    """

    def __init__(
        self,
        address: str = f"localhost:{llm_config.PORT}",
        stub=None,
        deadline: float = llm_config.APP_DEADLINE_SECONDS,
    ):
        self._deadline = deadline
        if stub is not None:
            self._stub = stub
        else:
            self._channel = grpc.insecure_channel(address)
            self._stub = llm_pb2_grpc.LLMServiceStub(self._channel)

    def get_llm_answer(self, request_id: str, query: str, context: dict) -> tuple:
        """The handout's getLLMAnswer(requestId, query, context) ->
        (requestId, answer).

        `context` is required (never None) so a caller cannot forget it;
        pass {} for the FAQ job's "no live auction" case.
        """
        if query:
            # (a) bidding FAQ. context: {} or {"auction": <auction dict>}.
            request = llm_pb2.BiddingQuestionRequest(
                request_id=request_id, question=query
            )
            if "auction" in context:
                request.auction.CopyFrom(_to_auction_context(context["auction"]))
            response = self._stub.AnswerBiddingQuestion(request, timeout=self._deadline)

        elif "attributes" in context:
            # (b) item description. context: {"item": str, "attributes": dict}.
            response = self._stub.GenerateItemDescription(
                llm_pb2.ItemDescriptionRequest(
                    request_id=request_id,
                    item=context["item"],
                    attributes=[
                        llm_pb2.Attribute(key=key, value=value)
                        for key, value in context["attributes"].items()
                    ],
                ),
                timeout=self._deadline,
            )

        elif "bid_history" in context:
            # (c) result summary. context: {"auction": dict, "bid_history": [...]}.
            response = self._stub.SummariseAuctionResult(
                llm_pb2.AuctionSummaryRequest(
                    request_id=request_id,
                    auction=_to_auction_context(context["auction"]),
                    bid_history=[
                        llm_pb2.BidPoint(
                            bidder=point["bidder"],
                            amount=point["amount"],
                            time=point["time"],
                        )
                        for point in context["bid_history"]
                    ],
                ),
                timeout=self._deadline,
            )

        elif "price_history" in context:
            # (c') price trends. context: {"price_history": [{auction_id, item,
            # opening_bid, winning_bid, bid_count}, ...]}, oldest first.
            response = self._stub.SummarisePriceTrends(
                llm_pb2.PriceTrendsRequest(
                    request_id=request_id,
                    auctions=[
                        llm_pb2.ClosedAuctionPoint(
                            auction_id=point["auction_id"],
                            item=point["item"],
                            opening_bid=point["opening_bid"],
                            winning_bid=point["winning_bid"],
                            bid_count=point["bid_count"],
                        )
                        for point in context["price_history"]
                    ],
                ),
                timeout=self._deadline,
            )

        else:
            raise ValueError(
                "get_llm_answer: query is empty and context matches none of "
                "'attributes', 'bid_history' or 'price_history' -- nothing to dispatch to"
            )

        return response.request_id, response.answer
