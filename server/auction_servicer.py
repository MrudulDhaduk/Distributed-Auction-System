"""gRPC servicer for AuctionService: CreateAuction, PlaceBid, GetAuction,
ListAuctions.

Translates between the wire (auction_pb2 messages) and the domain
(auction/state.py commands): unwrap the request, build a command, hand it
to AuctionState.apply(), wrap the Result back up. No auction logic lives
here -- every decision is apply()'s.

No SessionStore: the interceptor resolves context.username before any
handler runs, so this class only needs AuctionState.

Command dataclasses are imported qualified as `commands.X`, not bare, so
`commands.CreateAuction` (the dataclass) can never be confused with
`CreateAuction` (the RPC method below).
"""

from auction import state as commands
from auction.state import AuctionState
from generated import auction_pb2, auction_pb2_grpc
from server import time_conv


def _to_pb_result(result: commands.Result) -> auction_pb2.Result:
    return auction_pb2.Result(success=result.success, reason=result.reason)


class AuctionServicer(auction_pb2_grpc.AuctionServiceServicer):
    def __init__(self, state: AuctionState):
        self._state = state

    def CreateAuction(
        self, request: auction_pb2.CreateAuctionRequest, context
    ) -> auction_pb2.CreateAuctionResponse:
        """Create an auction. All validation lives in apply()."""
        close_time = time_conv.micros_to_dt(request.close_time)
        command = commands.CreateAuction(
            auction_id=request.auction_id,
            item=request.item,
            close_time=close_time,
        )
        result = self._state.apply(command)
        return auction_pb2.CreateAuctionResponse(
            result=_to_pb_result(result),
            auction_id=request.auction_id,
        )

    def PlaceBid(
        self, request: auction_pb2.PlaceBidRequest, context
    ) -> auction_pb2.PlaceBidResponse:
        """Place a bid on an auction.

        Bidder comes from the validated token (context.username), never
        from the request -- a caller cannot bid as someone else. The
        timestamp is stamped here, once, so apply() stays deterministic.
        All validation (auction exists, closed, past deadline, amount too
        low) lives in apply(). The high-bid fields in the response are an
        advisory hint so a rejected bidder can re-bid without a second
        round trip.
        """
        curr_time = time_conv.now_utc()
        command = commands.PlaceBid(
            auction_id=request.auction_id,
            bidder=context.username,
            amount=request.amount,
            curr_time=curr_time,
        )
        result = self._state.apply(command)
        auction = self._state.auctions.get(request.auction_id)
        high_bid = 0
        high_bidder = ""
        if auction is not None and auction["bids"]:
            top = max(auction["bids"], key=lambda b: b.bid_amount)
            high_bid = top.bid_amount
            high_bidder = top.bidder
        return auction_pb2.PlaceBidResponse(
            result=_to_pb_result(result),
            current_high_bid=high_bid,
            current_high_bidder=high_bidder,
        )

    def GetAuction(
        self, request: auction_pb2.GetAuctionRequest, context
    ) -> auction_pb2.GetAuctionResponse:
        """Read one auction. Snapshot under the lock, build outside it."""
        with self._state.lock:
            auction = self._state.auctions.get(request.auction_id)
            if auction is not None:
                item = auction["item"]
                close_time = auction["close_time"]
                closed = auction["closed"]
                winner = auction["winner"]
                entries = list(auction["bids"])
            if auction is None:
                return auction_pb2.GetAuctionResponse(
                    result=auction_pb2.Result(success=False, reason="no such auction")
                )

        high_bid = 0
        high_bidder = ""
        if entries:
            high = max(entries, key=lambda b: b.bid_amount)
            high_bid = high.bid_amount
            high_bidder = high.bidder
        pb_bids = [
            auction_pb2.Bid(
                bidder=e.bidder,
                amount=e.bid_amount,
                time=time_conv.dt_to_micros(e.time_stamp),
            )
            for e in entries
        ]

        pb_auction = auction_pb2.Auction(
            auction_id=request.auction_id,
            item=item,
            close_time=time_conv.dt_to_micros(close_time),
            closed=closed,
            current_high_bid=high_bid,
            current_high_bidder=high_bidder,
            bids=pb_bids,
            winner=winner.bidder if winner is not None else "",
        )
        return auction_pb2.GetAuctionResponse(
            result=auction_pb2.Result(success=True, reason="ok"),
            auction=pb_auction,
        )

    def ListAuctions(
        self, request: auction_pb2.ListAuctionsRequest, context
    ) -> auction_pb2.ListAuctionsResponse:
        """List auctions, open only unless include_closed is set."""
        summaries = []
        with self._state.lock:
            for auction_id, auction in self._state.auctions.items():
                if (
                    auction["closed"] and not request.include_closed
                ):  # skip closed unless asked
                    continue
                high_bid = 0
                if auction["bids"]:  # any bids?
                    high_bid = max(
                        auction["bids"], key=lambda b: b.bid_amount
                    ).bid_amount

                summaries.append(
                    auction_pb2.AuctionSummary(
                        auction_id=auction_id,
                        item=auction["item"],
                        close_time=time_conv.dt_to_micros(auction["close_time"]),
                        closed=auction["closed"],
                        current_high_bid=high_bid,
                    )
                )

        return auction_pb2.ListAuctionsResponse(auctions=summaries)
