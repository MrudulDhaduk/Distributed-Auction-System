"""gRPC servicer for AssistantService: AskAssistant, GenerateDescription,
GetPriceTrends (chunk 7, step 2; R4, R5, R6, R9).

The LEADER reads the replicated auction state, sends it to the LLM server as
context (LLMClient.get_llm_answer: the handout's getLLMAnswer(requestId,
query, context)), and answers. Every handler:

    leader check -> read barrier -> snapshot the state under the lock ->
    (release the lock) -> call the model -> reply

  * LEADER ONLY. The pre-check spares a follower the work; the read barrier
    (RaftNode.read_index, section 8) is what makes the state it reads current.
    Without it a deposed leader would put stale state into a prompt.
  * NO LOCK IS HELD DURING THE MODEL CALL. It takes seconds.
  * THE MODEL IS NEVER CALLED FROM apply(), and no model call is ever logged.
    Of the three, only GenerateDescription writes anything: it proposes a
    SetDescription carrying the generated TEXT as plain data (llm.proto: call
    first, replicate second).
  * AN LLM FAILURE IS A CLEAR ANSWER, NOT A gRPC ERROR: Result(success=false,
    reason). A gRPC UNAVAILABLE would send ClusterClient round every node for
    30 s. Failure includes "slow": LLMClient puts a deadline on each call.
    The auction is untouched either way.
"""

import logging
import uuid

import grpc

from auction import state as commands
from auction.state import AuctionState, description_rejection
from generated import assistant_pb2, assistant_pb2_grpc
from replication.replicator import LocalReplicator
from replication.state_machine import StateMachine
from server import llm_context
from server.errors import raft_errors
from server.llm_client import LLMClient

log = logging.getLogger(__name__)

DEFAULT_TREND_WINDOW = 20
MAX_TREND_WINDOW = 50


def _no(reason: str) -> assistant_pb2.Result:
    return assistant_pb2.Result(success=False, reason=reason)


def _ok(reason: str = "ok") -> assistant_pb2.Result:
    return assistant_pb2.Result(success=True, reason=reason)


class AssistantServicer(assistant_pb2_grpc.AssistantServiceServicer):
    def __init__(self, state: AuctionState, llm_client: LLMClient, replicator=None):
        """state: this node's AuctionState (only READ here; the one write,
        SetDescription, goes through the replicator). replicator: None = a
        cluster of one without Raft, as in AuctionServicer."""
        self._state = state
        self._llm = llm_client
        if replicator is None:
            replicator = LocalReplicator(StateMachine(auctions=state))
        self._replicator = replicator

    def _ask_model(self, query: str, context: dict):
        """(request_id, answer, None) or (request_id, "", reason). The id is
        minted HERE, by the gRPC caller, so it can be put in this process's
        log next to the LLM server's (llm.proto, "Correlation")."""
        request_id = str(uuid.uuid4())
        try:
            _, answer = self._llm.get_llm_answer(request_id=request_id, query=query, context=context)
        except grpc.RpcError as e:
            log.warning("LLM call failed (request_id=%s): %s", request_id, e, exc_info=True)
            return request_id, "", llm_context.failure_reason(e)
        return request_id, answer, None

    # --- R4: the FAQ ------------------------------------------------------------

    def AskAssistant(self, request: assistant_pb2.AskRequest, context) -> assistant_pb2.AnswerResponse:
        with raft_errors(context):
            self._replicator.check_leader()
            if not request.question.strip():
                return assistant_pb2.AnswerResponse(result=_no("The question must not be empty"))
            self._replicator.read_barrier()

        llm_ctx = {}
        if request.HasField("auction_id"):
            with self._state.lock:
                auction = self._state.auctions.get(request.auction_id)
                if auction is None:
                    return assistant_pb2.AnswerResponse(result=_no("no such auction"))
                llm_ctx = {"auction": llm_context.auction_context(request.auction_id, auction)}

        request_id, answer, failure = self._ask_model(request.question, llm_ctx)
        if failure is not None:
            return assistant_pb2.AnswerResponse(result=_no(failure), request_id=request_id)
        return assistant_pb2.AnswerResponse(result=_ok(), request_id=request_id, answer=answer)

    # --- R5: a description, on request --------------------------------------------

    def GenerateDescription(
        self, request: assistant_pb2.GenerateDescriptionRequest, context
    ) -> assistant_pb2.DescriptionResponse:
        """Seller only, open auctions only. The same rule function apply()
        uses decides the pre-check, so the model is not asked for text that
        apply() would refuse."""
        actor = context.username
        with raft_errors(context):
            self._replicator.check_leader()
            self._replicator.read_barrier()
            with self._state.lock:
                auction = self._state.auctions.get(request.auction_id)
                refusal = description_rejection(auction, actor)
                item = auction["item"] if auction is not None else ""
            if refusal is not None:
                return assistant_pb2.DescriptionResponse(result=_no(refusal))
            if not request.attributes:
                return assistant_pb2.DescriptionResponse(
                    result=_no("At least one attribute is required: with nothing to work from the model would invent the item")
                )

            request_id, text, failure = self._ask_model(
                "", {"item": item, "attributes": dict(request.attributes)}
            )
            if failure is not None:
                return assistant_pb2.DescriptionResponse(result=_no(failure), request_id=request_id)

            applied = self._replicator.submit(
                commands.SetDescription(auction_id=request.auction_id, actor=actor, description=text),
                request.client_id,
                request.seq,
            )
        result = assistant_pb2.Result(success=applied.result.success, reason=applied.result.reason)
        return assistant_pb2.DescriptionResponse(
            result=result, request_id=request_id, description=text if applied.result.success else ""
        )

    # --- R6: price trends ------------------------------------------------------------

    def GetPriceTrends(
        self, request: assistant_pb2.PriceTrendsRequest, context
    ) -> assistant_pb2.PriceTrendsResponse:
        """Statistics are computed here, in code, from the closed auctions;
        the model only puts them into words. If it cannot, the numbers still
        come back and result says why there is no summary."""
        with raft_errors(context):
            self._replicator.check_leader()
            self._replicator.read_barrier()

        limit = min(request.limit or DEFAULT_TREND_WINDOW, MAX_TREND_WINDOW)
        points = llm_context.closed_points(self._state, limit)
        stats = _stats(points)
        pb_points = [assistant_pb2.TrendPoint(**p) for p in points]
        if not points:
            return assistant_pb2.PriceTrendsResponse(result=_ok("no closed auctions yet"), stats=stats)

        request_id, summary, failure = self._ask_model("", {"price_history": points})
        if failure is not None:
            return assistant_pb2.PriceTrendsResponse(
                result=_no(failure), request_id=request_id, stats=stats, points=pb_points
            )
        return assistant_pb2.PriceTrendsResponse(
            result=_ok(), request_id=request_id, summary=summary, stats=stats, points=pb_points
        )


def _stats(points: list) -> assistant_pb2.TrendStats:
    """Plain integer arithmetic over the points. The mean is floored."""
    winning = [p["winning_bid"] for p in points if p["bid_count"] > 0]
    return assistant_pb2.TrendStats(
        auctions=len(points),
        with_bids=len(winning),
        no_bids=len(points) - len(winning),
        lowest_winning_bid=min(winning, default=0),
        highest_winning_bid=max(winning, default=0),
        mean_winning_bid=sum(winning) // len(winning) if winning else 0,
        total_bids=sum(p["bid_count"] for p in points),
    )
