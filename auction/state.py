"""Replicated auction state and the commands that mutate it.

Every state change goes through `AuctionState.apply()`, which must stay
deterministic: no clocks, no randomness, no I/O. Anything time-dependent is
carried on the command itself (see `PlaceBid.curr_time`).
"""

import datetime
import threading
from dataclasses import dataclass

# --- Commands ---


@dataclass(frozen=True)
class CreateAuction:
    auction_id: int
    item: str
    close_time: datetime.datetime


@dataclass(frozen=True)
class PlaceBid:
    auction_id: int
    bidder: str
    amount: int
    curr_time: datetime.datetime


# --- Result ---


@dataclass(frozen=True)
class Result:
    success: bool
    reason: str


@dataclass(frozen=True)
class BidEntry:
    bid_amount: int
    bidder: str
    time_stamp: datetime.datetime


class AuctionState:
    """In-memory auction table, keyed by auction id."""

    auctions: dict

    def __init__(self):
        self.auctions = {}
        self.lock = threading.Lock()

    def apply(self, command):
        """Apply one command and return whether it was accepted.
 
        Serialised by `self._lock`: one command runs start to finish before
        the next begins, so the check-then-act sequences below cannot
        interleave. The lock guarantees safety, not ordering -- which
        command wins under contention is left to the scheduler.
        """
        with self.lock:
            if isinstance(command, CreateAuction):
                if command.auction_id in self.auctions:
                    return Result(
                        success=False, reason="ID already exists, choose a different ID"
                    )

                self.auctions[command.auction_id] = {
                    "item": command.item,
                    "close_time": command.close_time,
                    "bids": [],
                }
                return Result(success=True, reason="Auction created successfully")

            if isinstance(command, PlaceBid):
                auction = self.auctions.get(command.auction_id)
                if auction is None:
                    return Result(
                        success=False,
                        reason="Auction ID does not exist, please check the ID again",
                    )

                if command.curr_time > auction["close_time"]:
                    return Result(success=False, reason="Auction already closed")

                highest_bid = max(
                    (entry.bid_amount for entry in auction["bids"]), default=0
                )
                if command.amount <= highest_bid:
                    return Result(
                        success=False,
                        reason=(
                            f"Bid of {command.amount} is not added because "
                            f"current highest bid is {highest_bid}"
                        ),
                    )
                bid = BidEntry(
                    bid_amount=command.amount,
                    bidder=command.bidder,
                    time_stamp=command.curr_time,
                )

                auction["bids"].append(bid)
                return Result(success=True, reason=(f"Bid of {command.amount} added"))

            return Result(
                success=False,
                reason="Command not recognised, please enter the command as either "
                "CreateAuction or PlaceBid",
            )
