"""A mock payment gateway, in-process. The assignment allows a stub (R3).

It is OUTSIDE the replicated state machine. The leader's handler calls it
before it proposes, and whatever it answers reaches the Raft log only as
plain data on the command (PayAuction.payment_ref, ReleaseEscrow.settlement_ref).
apply() never calls it, so apply() stays deterministic: no clock, no
randomness, no I/O. That is the same arrangement as the LLM text and the
session token (call first, replicate second).

Four operations, named after how a card authorization works:

    authorize(payer, amount, auction_id, idempotency_key) -> Receipt
        places a hold on the payer's money. This is what ends up in escrow.
    release(payment_ref)  the held money goes to the seller
    refund(payment_ref)   the held money goes back to the payer
    void(payment_ref)     cancels a hold that never became escrow: the
                          handler calls it when apply() refused a payment
                          the gateway had already authorized

A decline is a normal answer (Receipt.approved is False), not an exception:
the handler reports it and proposes nothing.

IDEMPOTENT, WITH NO MEMORY. authorize() derives its reference from its
inputs (a hash), so the same (payer, amount, auction, key) always gives the
same reference, from any gateway instance. That matters because each node
has its own in-process gateway: a client that retries a payment after the
old leader died reaches a NEW leader whose gateway has never seen the first
attempt, and must still get the same reference. A real gateway is an external
service shared by every node and would offer the same property as an
idempotency key; this stub gets it from the hash. Without a key (no
client_id, so no retry protection either) every call gets a fresh reference.

What this stub does not do, on purpose: it keeps no ledger of holds, so it
cannot tell a hold that was already released from one that was not. The
once-only rule for the money lives in apply() (the escrow state machine),
which every replica enforces. See docs/decisions.md for what that does and
does not guarantee.
"""

import hashlib
import threading
import uuid
from dataclasses import dataclass


@dataclass(frozen=True)
class Receipt:
    approved: bool
    # The gateway's reference. "" when not approved.
    reference: str
    # Why it was declined. "" when approved.
    reason: str = ""


_PAY = "PAY-"


def _declined(reason: str) -> Receipt:
    return Receipt(approved=False, reference="", reason=reason)


class MockPaymentGateway:
    def __init__(self, decline_over=None):
        """decline_over: decline any amount above this limit (None = no
        limit). A plain attribute, so a test or the demo can change it."""
        self.decline_over = decline_over
        # Every call, in order, as a tuple: ("authorize", payer, amount,
        # auction_id, key) or (op, payment_ref). Tests read it to assert
        # "the gateway was called once", or "never".
        self.calls = []
        self._lock = threading.Lock()

    def _record(self, call: tuple) -> None:
        with self._lock:
            self.calls.append(call)

    def authorize(self, payer: str, amount: int, auction_id: int, idempotency_key: str = "") -> Receipt:
        self._record(("authorize", payer, amount, auction_id, idempotency_key))
        if amount <= 0:
            return _declined(f"declined: the amount must be positive, not {amount}")
        if self.decline_over is not None and amount > self.decline_over:
            return _declined(f"declined: {amount} is over the gateway's limit of {self.decline_over}")
        if idempotency_key:
            digest = hashlib.sha256(f"{auction_id}|{payer}|{amount}|{idempotency_key}".encode()).hexdigest()[:12]
        else:
            digest = uuid.uuid4().hex[:12]
        return Receipt(approved=True, reference=_PAY + digest.upper())

    def _settle(self, op: str, prefix: str, payment_ref: str) -> Receipt:
        self._record((op, payment_ref))
        if not payment_ref.startswith(_PAY) or len(payment_ref) == len(_PAY):
            return _declined(f"declined: {payment_ref!r} is not a payment this gateway issued")
        return Receipt(approved=True, reference=prefix + payment_ref[len(_PAY):])

    def release(self, payment_ref: str) -> Receipt:
        return self._settle("release", "REL-", payment_ref)

    def refund(self, payment_ref: str) -> Receipt:
        return self._settle("refund", "RFD-", payment_ref)

    def void(self, payment_ref: str) -> Receipt:
        return self._settle("void", "VOID-", payment_ref)
