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
    # Seller's username, from the validated token -- never from the request.
    owner: str
    # Generated server-side (LLMService.GenerateItemDescription) BEFORE
    # this command is built, never inside apply() -- see llm.proto's
    # ordering note. "" when no attributes were supplied or the LLM call
    # failed; an outage must never block auction creation.
    description: str


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


@dataclass(frozen=True)
class CloseAuction:
    auction_id: int
    curr_time: datetime.datetime


# --- Escrow (chunk 7) ---
#
# NONE -> HELD -> RELEASED | REFUNDED. RELEASED and REFUNDED are final.
#
# The money itself moves in the mock payment gateway (server/payment_gateway.py),
# OUTSIDE apply(), on the leader, before the command is proposed. What reaches
# apply() is the gateway's answer as plain data: `payment_ref` / `settlement_ref`
# are opaque strings that apply() stores and never interprets. apply() decides
# only from the command and the current state, like every other command.


@dataclass(frozen=True)
class PayAuction:
    auction_id: int
    # The winner's username, from the validated token -- never from the request.
    payer: str
    amount: int
    # The gateway's reference for the approved payment. "" means the handler
    # did not (or could not) get one; apply() then refuses.
    payment_ref: str


@dataclass(frozen=True)
class ReleaseEscrow:
    """The seller confirms: the held money goes to the seller."""

    auction_id: int
    # The seller's username, from the validated token.
    actor: str
    settlement_ref: str


@dataclass(frozen=True)
class RefundEscrow:
    """The seller cancels the sale: the held money goes back to the winner."""

    auction_id: int
    actor: str
    settlement_ref: str


# --- LLM text as data (chunk 7) ---
#
# The model is called by the leader's handler or worker, outside apply(), and
# its text arrives here as an ordinary string. apply() cannot tell that an LLM
# wrote it, and replaying the log never calls a model (llm.proto's rule:
# "call first, replicate second").


@dataclass(frozen=True)
class SetDescription:
    """The seller replaces the listing text (generated on request)."""

    auction_id: int
    # The seller's username, from the validated token.
    actor: str
    description: str


@dataclass(frozen=True)
class RecordSummary:
    """The cluster records the result summary of a closed auction. Proposed
    by the leader's summary worker, so (like CloseAuction) it has no actor and
    no client_id: the cluster is the author."""

    auction_id: int
    summary: str


ESCROW_NONE = "NONE"
ESCROW_HELD = "HELD"
ESCROW_RELEASED = "RELEASED"
ESCROW_REFUNDED = "REFUNDED"


def escrow_of(auction: dict) -> dict:
    """The auction's escrow record. An auction nobody has paid for has none
    stored (the key is created by the first PayAuction, so CreateAuction is
    untouched); this returns the NONE record for it."""
    return auction.get("escrow") or {
        "status": ESCROW_NONE,
        "amount": 0,
        "payer": "",
        "payment_ref": "",
        "settlement_ref": "",
    }


def payment_rejection(auction, payer: str, amount: int):
    """Why `payer` may not pay `amount` into this auction's escrow, or None
    if they may. The one definition of the rule: apply() calls it to decide,
    and the handler calls it as a pre-check, so the two cannot drift."""
    if auction is None:
        return "Auction ID does not exist, please check the ID again"
    if not auction["closed"]:
        return "Auction is not closed yet, nothing to pay"
    winner = auction["winner"]
    if winner is None:
        return "Auction closed with no winner, nothing to pay"
    if payer != winner.bidder:
        return f"{payer} is not the winner: only the winner can pay"
    if amount != winner.bid_amount:
        return f"The amount must equal the winning bid of {winner.bid_amount}, not {amount}"
    status = escrow_of(auction)["status"]
    if status != ESCROW_NONE:
        return f"Escrow is already {status}, no second payment"
    return None


def settlement_rejection(auction, actor: str):
    """Why `actor` may not release or refund this auction's escrow, or None
    if they may. Same arrangement as payment_rejection. Release and refund
    share one rule: held money, and the seller asks."""
    if auction is None:
        return "Auction ID does not exist, please check the ID again"
    status = escrow_of(auction)["status"]
    if status != ESCROW_HELD:
        return f"Escrow is {status}, only HELD money can be settled"
    if actor != auction["owner"]:
        return f"{actor} is not the seller: only the seller can settle the escrow"
    return None


def description_rejection(auction, actor: str, description=None):
    """Why `actor` may not replace this auction's description, or None if
    they may. `description` None = "the text is not known yet": the handler
    pre-checks before it asks the model, and apply() checks again with the
    real text. The one definition of the rule."""
    if auction is None:
        return "Auction ID does not exist, please check the ID again"
    if actor != auction["owner"]:
        return f"{actor} is not the seller: only the seller can change the description"
    if auction["closed"]:
        return "Auction is closed, its listing is frozen"
    if description is not None and not description.strip():
        return "The description must not be empty"
    return None


def summary_rejection(auction, summary=None):
    """Why this auction may not take this summary now, or None if it may.
    An auction takes AT MOST ONE summary, and only once it is closed; that is
    what makes recording a summary idempotent."""
    if auction is None:
        return "Auction ID does not exist, please check the ID again"
    if not auction["closed"]:
        return "Auction is not closed yet, nothing to summarise"
    if auction.get("summaries"):
        return "Auction already has a summary"
    if summary is not None and not summary.strip():
        return "The summary must not be empty"
    return None


def summary_of(auction: dict) -> str:
    """The auction's summary text, or "" if it has none. The state keeps the
    ACCEPTED summaries as a list (created by the first RecordSummary) so a
    broken apply() that accepted two would show up as a list of two, instead
    of silently overwriting one string."""
    summaries = auction.get("summaries")
    return summaries[0] if summaries else ""


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
                    "owner": command.owner,
                    "description": command.description,
                    "bids": [],
                    "closed": False,
                    "winner": None,
                    "closed_at": None,
                }
                return Result(success=True, reason=f"Auction created successfully by {command.owner}")

            if isinstance(command, PlaceBid):
                auction = self.auctions.get(command.auction_id)
                if auction is None:
                    return Result(
                        success=False,
                        reason="Auction ID does not exist, please check the ID again",
                    )

                if auction["closed"]:
                    return Result(success=False, reason=f"Auction is already closed {command.bidder}")
                if command.curr_time > auction["close_time"]:
                    return Result(success=False, reason=f"Time is up {command.bidder}")
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

            if isinstance(command, CloseAuction):
                auction = self.auctions.get(command.auction_id)
                if auction is None:
                    return Result(success=False, reason="Auction dosen't exist")

                if auction["closed"]:
                    return Result(success=False, reason="Auction is already closed")

                if command.curr_time <= auction["close_time"]:
                    return Result(
                        success=False, reason="There is still time left for the auction"
                    )

                auction["closed"] = True
                auction["closed_at"] = command.curr_time

                if auction["bids"]:
                    auction["winner"] = max(auction["bids"], key=lambda p: p.bid_amount)
                    return Result(success=True, reason="The auction is closed")

                return Result(success=True, reason="The auction is closed with no winner")

            if isinstance(command, PayAuction):
                auction = self.auctions.get(command.auction_id)
                # The real rule first, so a doomed command (empty reference)
                # is refused for its real reason, not for the missing one.
                reason = payment_rejection(auction, command.payer, command.amount)
                if reason is None and not command.payment_ref:
                    reason = "No payment reference: the gateway did not approve a payment"
                if reason is not None:
                    return Result(success=False, reason=reason)
                auction["escrow"] = {
                    "status": ESCROW_HELD,
                    "amount": command.amount,
                    "payer": command.payer,
                    "payment_ref": command.payment_ref,
                    "settlement_ref": "",
                }
                return Result(
                    success=True,
                    reason=f"{command.amount} is held in escrow for auction {command.auction_id}",
                )

            if isinstance(command, (ReleaseEscrow, RefundEscrow)):
                auction = self.auctions.get(command.auction_id)
                reason = settlement_rejection(auction, command.actor)
                if reason is None and not command.settlement_ref:
                    reason = "No settlement reference: the gateway did not approve the settlement"
                if reason is not None:
                    return Result(success=False, reason=reason)
                released = isinstance(command, ReleaseEscrow)
                auction["escrow"] = {
                    **auction["escrow"],
                    "status": ESCROW_RELEASED if released else ESCROW_REFUNDED,
                    "settlement_ref": command.settlement_ref,
                }
                return Result(
                    success=True,
                    reason="Escrow released to the seller" if released else "Escrow refunded to the winner",
                )

            if isinstance(command, SetDescription):
                auction = self.auctions.get(command.auction_id)
                reason = description_rejection(auction, command.actor, command.description)
                if reason is not None:
                    return Result(success=False, reason=reason)
                auction["description"] = command.description
                return Result(success=True, reason=f"Description of auction {command.auction_id} updated")

            if isinstance(command, RecordSummary):
                auction = self.auctions.get(command.auction_id)
                reason = summary_rejection(auction, command.summary)
                if reason is not None:
                    return Result(success=False, reason=reason)
                auction["summaries"] = [command.summary]
                return Result(success=True, reason=f"Summary of auction {command.auction_id} recorded")

            return Result(
                success=False,
                reason="Command not recognised, please enter the command as either "
                "CreateAuction or PlaceBid",
            )
