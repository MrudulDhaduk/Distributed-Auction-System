"""gRPC servicer for PaymentService: Pay, ReleaseEscrow, RefundEscrow, GetEscrow.

Translates between the wire (payment_pb2) and the domain (auction/state.py's
PayAuction / ReleaseEscrow / RefundEscrow). The money moves in the mock
gateway (server/payment_gateway.py), and the gateway is called HERE, on the
leader, before the command is proposed -- never inside apply() (see the
gateway's docstring, and docs/decisions.md, chunk 7).

A write goes:

    leader check -> per-auction lock -> read barrier ->
    pre-check (the SAME rule functions apply() uses) ->
    gateway call, only if the pre-check passed ->
    propose(command with the gateway's reference as data) -> wait -> reply

apply() is still the judge. If the pre-check fails, the command is proposed
anyway with an EMPTY reference and no gateway call: apply() refuses it with
the real reason, or -- for a retry of something that already committed --
the replicated dedupe table (replication/state_machine.py) answers with the
stored first response. That is what makes a client retry safe: the pre-check
sees "already HELD" and skips the gateway, and the retry still gets the
original success instead of a refusal.

If apply() refuses a command whose authorization the gateway already granted
(two sessions of the winner paying at once, say), the handler voids the hold.

The per-auction lock serialises payment calls for one auction on this node,
so the pre-check and the proposal cannot be separated by another payment call
on the same leader. It is a lock on the handler layer, not on the Raft node:
nothing here holds the node lock across an RPC. It does not stop two
different leaders' handlers racing across a failover; apply() still does.

Reads: leader only, behind a read barrier, as GetAuction (decisions.md, A15).
"""

import threading

from auction import state as commands
from auction.state import (
    AuctionState,
    escrow_of,
    payment_rejection,
    settlement_rejection,
)
from generated import payment_pb2, payment_pb2_grpc
from replication.replicator import LocalReplicator
from replication.state_machine import StateMachine
from server.errors import raft_errors
from server.payment_gateway import MockPaymentGateway


def _result(r: commands.Result) -> payment_pb2.Result:
    return payment_pb2.Result(success=r.success, reason=r.reason)


def _idempotency_key(request) -> str:
    """The gateway's idempotency key for this request: the client's serial
    number, which is already unique per logical write. "" for a request with
    no client_id (no dedupe, so no retry protection either)."""
    return f"{request.client_id}:{request.seq}" if request.client_id else ""


class PaymentServicer(payment_pb2_grpc.PaymentServiceServicer):
    def __init__(self, state: AuctionState, gateway: MockPaymentGateway, replicator=None):
        """state: this node's AuctionState (only READ here; writes go through
        the replicator). replicator: this node's RaftReplicator. None = a
        cluster of one without Raft, as in AuctionServicer."""
        self._state = state
        self._gateway = gateway
        if replicator is None:
            replicator = LocalReplicator(StateMachine(auctions=state))
        self._replicator = replicator
        self._locks_guard = threading.Lock()
        self._locks = {}

    def _auction_lock(self, auction_id: int) -> threading.Lock:
        with self._locks_guard:
            return self._locks.setdefault(auction_id, threading.Lock())

    def _stored(self, auction_id: int, field: str) -> str:
        """A field of the auction's escrow record as it is right now."""
        with self._state.lock:
            auction = self._state.auctions.get(auction_id)
            return escrow_of(auction)[field] if auction is not None else ""

    # --- writes -------------------------------------------------------------------

    def Pay(self, request: payment_pb2.PayRequest, context) -> payment_pb2.PaymentResponse:
        """The winner pays the winning bid into escrow. payer is the token's
        user, never a request field."""
        payer = context.username
        with raft_errors(context):
            self._replicator.check_leader()
            with self._auction_lock(request.auction_id):
                self._replicator.read_barrier()
                with self._state.lock:
                    refusal = payment_rejection(
                        self._state.auctions.get(request.auction_id), payer, request.amount
                    )

                payment_ref = ""
                if refusal is None:
                    receipt = self._gateway.authorize(
                        payer, request.amount, request.auction_id, _idempotency_key(request)
                    )
                    if not receipt.approved:
                        # A decline is the gateway's answer, not a command:
                        # nothing is proposed, nothing is logged.
                        return payment_pb2.PaymentResponse(
                            result=payment_pb2.Result(success=False, reason=receipt.reason)
                        )
                    payment_ref = receipt.reference

                applied = self._replicator.submit(
                    commands.PayAuction(
                        auction_id=request.auction_id,
                        payer=payer,
                        amount=request.amount,
                        payment_ref=payment_ref,
                    ),
                    request.client_id,
                    request.seq,
                )
                if payment_ref and not applied.result.success:
                    # The gateway granted a hold, apply() refused the payment.
                    self._gateway.void(payment_ref)
                reference = (
                    self._stored(request.auction_id, "payment_ref") if applied.result.success else ""
                )
        return payment_pb2.PaymentResponse(result=_result(applied.result), reference=reference)

    def ReleaseEscrow(self, request: payment_pb2.EscrowActionRequest, context) -> payment_pb2.PaymentResponse:
        """The seller confirms: HELD -> RELEASED."""
        return self._settle(request, context, release=True)

    def RefundEscrow(self, request: payment_pb2.EscrowActionRequest, context) -> payment_pb2.PaymentResponse:
        """The seller cancels the sale: HELD -> REFUNDED."""
        return self._settle(request, context, release=False)

    def _settle(self, request, context, *, release: bool) -> payment_pb2.PaymentResponse:
        actor = context.username
        with raft_errors(context):
            self._replicator.check_leader()
            with self._auction_lock(request.auction_id):
                self._replicator.read_barrier()
                with self._state.lock:
                    auction = self._state.auctions.get(request.auction_id)
                    refusal = settlement_rejection(auction, actor)
                    payment_ref = escrow_of(auction)["payment_ref"] if auction is not None else ""

                settlement_ref = ""
                if refusal is None:
                    receipt = (
                        self._gateway.release(payment_ref) if release else self._gateway.refund(payment_ref)
                    )
                    if not receipt.approved:
                        return payment_pb2.PaymentResponse(
                            result=payment_pb2.Result(success=False, reason=receipt.reason)
                        )
                    settlement_ref = receipt.reference

                command_type = commands.ReleaseEscrow if release else commands.RefundEscrow
                applied = self._replicator.submit(
                    command_type(
                        auction_id=request.auction_id, actor=actor, settlement_ref=settlement_ref
                    ),
                    request.client_id,
                    request.seq,
                )
                reference = (
                    self._stored(request.auction_id, "settlement_ref") if applied.result.success else ""
                )
        return payment_pb2.PaymentResponse(result=_result(applied.result), reference=reference)

    # --- reads --------------------------------------------------------------------

    def GetEscrow(self, request: payment_pb2.GetEscrowRequest, context) -> payment_pb2.GetEscrowResponse:
        """Leader only, after a read barrier. Anyone logged in may look."""
        with raft_errors(context):
            self._replicator.read_barrier()
        with self._state.lock:
            auction = self._state.auctions.get(request.auction_id)
            if auction is None:
                return payment_pb2.GetEscrowResponse(
                    result=payment_pb2.Result(success=False, reason="no such auction")
                )
            record = dict(escrow_of(auction))
            seller = auction["owner"]
            winner = auction["winner"].bidder if auction["winner"] is not None else ""
        return payment_pb2.GetEscrowResponse(
            result=payment_pb2.Result(success=True, reason="ok"),
            escrow=payment_pb2.Escrow(
                auction_id=request.auction_id,
                status=record["status"],
                amount=record["amount"],
                payer=record["payer"],
                seller=seller,
                winner=winner,
                payment_ref=record["payment_ref"],
                settlement_ref=record["settlement_ref"],
            ),
        )
