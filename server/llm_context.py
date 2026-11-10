"""Turning replicated auction state into the plain dicts the LLM client takes
(server/llm_client.py's `context`), and an LLM failure into a sentence for a
client. Used by the assistant servicer and by the summary worker, so the two
cannot describe the same auction differently.

Everything here is a pure read of the dicts AuctionState holds. The callers
take the snapshot under `state.lock` and call the model AFTER releasing it:
a model call takes seconds, and nothing may wait on a lock for that long.

Times are epoch microseconds (common/time_conv.py), the form llm.proto uses.
"""

import grpc

from common import time_conv


def auction_context(auction_id: int, auction: dict) -> dict:
    """The AuctionContext projection (llm.proto): what the model needs and
    nothing else. `closed` is the stored flag set by an applied CloseAuction,
    never recomputed from a clock (correctness property 4)."""
    bids = auction["bids"]
    return {
        "auction_id": auction_id,
        "item": auction["item"],
        "close_time": time_conv.dt_to_micros(auction["close_time"]),
        "closed": auction["closed"],
        "current_high_bid": max((b.bid_amount for b in bids), default=0),
        "bid_count": len(bids),
    }


def bid_history(auction: dict) -> list:
    """The bids in the order they were applied."""
    return [
        {"bidder": b.bidder, "amount": b.bid_amount, "time": time_conv.dt_to_micros(b.time_stamp)}
        for b in auction["bids"]
    ]


def closed_points(state, limit: int) -> list:
    """The `limit` most recently CREATED closed auctions, oldest first, as
    price-trend points. dict order is creation order, and every replica
    applied the same CreateAuctions in the same order, so this is the same
    list on every node."""
    with state.lock:
        closed = [(aid, a) for aid, a in state.auctions.items() if a["closed"]]
        points = []
        for aid, a in closed[-limit:]:
            bids = a["bids"]
            points.append({
                "auction_id": aid,
                "item": a["item"],
                "opening_bid": bids[0].bid_amount if bids else 0,
                "winning_bid": a["winner"].bid_amount if a["winner"] is not None else 0,
                "bid_count": len(bids),
            })
    return points


def failure_reason(exc: BaseException) -> str:
    """One sentence for a client when the LLM call failed. Always says the
    auction is unaffected, because that is the contract (llm.proto: an
    outage never blocks a bid). Tolerates an RpcError with no code()."""
    code = None
    code_fn = getattr(exc, "code", None)
    if callable(code_fn):
        try:
            code = code_fn()
        except Exception:  # noqa: BLE001 -- a malformed error must not hide the real one
            code = None
    if code == grpc.StatusCode.DEADLINE_EXCEEDED:
        what = "the assistant did not answer in time"
    elif code == grpc.StatusCode.UNAVAILABLE:
        what = "the assistant is unavailable (the LLM server could not be reached)"
    elif code is not None:
        what = f"the assistant failed ({code.name})"
    else:
        what = "the assistant failed"
    return f"{what}; the auction is unaffected"
