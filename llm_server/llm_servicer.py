"""gRPC servicer for LLMService: the three jobs required by handout §4.

Translates between the wire (llm_pb2 messages) and Ollama: validate the
request, build a chat prompt, call the model, wrap the text back up. No
auction logic lives here, and -- see proto/llm.proto -- nothing here ever
reaches apply() or the replicated log. The generated text leaves as plain
data; what the caller does with it afterwards is the caller's business.

Two invariants this module exists to hold:

  1. UNTRUSTED CALLER TEXT NEVER ENTERS A SYSTEM TURN. The system turn is
     where the model's instructions live. Text a CALLER supplies as the
     subject of the request -- a bidder's question, a seller's attributes --
     goes in a USER turn, verbatim, never formatted into the system content.
     Concatenating them would let a bidder who types "ignore your
     instructions and ..." have that text arrive in the same place the real
     instructions do.

     The honest limit of this: the system turn is server-ASSEMBLED, not
     server-ORIGINATED. build_summary_messages renders an auction whose
     `item` a seller chose and whose `bidder` names users chose, so
     user-originated values do reach the system turn in that one job. The
     structure is always ours; the strings inside are not always. See the
     note on build_summary_messages.

  2. EVERY FAILURE IS A gRPC STATUS, NEVER A BODY FIELD. LLMAnswerResponse
     has no Result field on purpose (see proto/llm.proto): a dead model is
     not a decision the domain made, it is the absence of an answer. The
     caller is expected to catch grpc.RpcError and degrade -- an LLM outage
     must never block a bid.
"""

import logging
import time

import grpc
import httpx
from ollama import Client, RequestError, ResponseError

from common import time_conv
from generated import llm_pb2, llm_pb2_grpc
from llm_server import config

log = logging.getLogger(__name__)


# --- Prompt construction ---------------------------------------------------
#
# Module-level and pure: given the same request they return the same
# messages, with no client and no network in sight. That is what lets the
# trust-boundary test assert on the exact turns without a model running.

# Every rule in _FAQ_SYSTEM below is one apply() actually enforces
# (auction/state.py): whole numbers from `amount: int`, strictly-higher from
# `amount <= highest_bid` being a rejection, the close check from
# `curr_time > close_time`, the winner from `max(bids, key=bid_amount)`.
# If apply()'s rules change, this block is wrong until it changes too -- it
# is documentation of the state machine that happens to be aimed at a model.
#
# The "complete set" sentence is doing real work. Asked whether a bidder
# could bid one more than the current high, an earlier version of this
# prompt answered "check the site's rules for specific bid increments" --
# inventing a minimum-increment rule this auction does not have, because
# most auctions it has read about do have one. A domain-specific assistant
# that falls back on generic domain knowledge is worse than useless: it is
# confidently wrong about the one system it is supposed to know.
_FAQ_SYSTEM = """\
You are a bidding assistant for an online auction site.

Answer only questions about bidding, auctions, and how this site works. If a
question is about anything else, say in one sentence that it is outside what
you can help with.

These are this site's bidding rules, and they are the complete set:

  - Bids are whole numbers.
  - A bid must be strictly higher than the current high bid. There is no
    minimum increment: one more than the current high is a valid bid.
  - No bids are accepted after the auction's close time.
  - The winner is the highest bid at the moment the auction closes.

Do not state any other rule. This site has no reserve prices, no minimum
increments, no proxy or automatic bidding, no buyer's premium, and no
anti-sniping extension. If a question is not answered by the rules above,
say you do not know rather than answering from how other auction sites
work.

Answer in at most two sentences. Plain prose -- no markdown, no bullet
points, no headings."""

_DESCRIPTION_SYSTEM = """\
You write short listing descriptions for an online auction site.

Use ONLY the attributes the seller supplied. Do not invent condition,
provenance, history, rarity, or value, and do not guess at anything the
attributes do not state. If the attributes are thin, write a shorter
description rather than padding it.

Write one paragraph of at most 60 words. Plain prose -- no markdown, no
bullet points, no headings. Do not address the reader as "you"."""

# The timing rule below is not general caution, it is a correction of an
# observed failure. Given a closed auction whose last bid was at 13:41 and
# whose scheduled close was 14:00, an earlier version of this prompt wrote
# "the auction ended just before its scheduled close time" -- which is not
# merely unsupported but impossible: apply() rejects a CloseAuction unless
# `curr_time > close_time` (auction/state.py), so an auction can only ever
# end AFTER its scheduled close, never before.
#
# The model had no actual close instant to work from -- AuctionContext
# carries close_time (scheduled) and not closed_at (actual) -- so it
# reached for a plausible-sounding one. Stopping it from inferring is the
# fix; giving it closed_at to state instead is a separate question, raised
# in the handover notes.
_SUMMARY_SYSTEM = """\
You summarise auctions for an online auction site's records.

State what happened: the item, the outcome, and the shape of the bidding.

State ONLY facts present in the data below, and state them as they are
given. In particular:

  - You are NOT told when the auction actually ended. The scheduled close
    time below is the time it was DUE to end, which is not the time it
    ended. Never report the scheduled close time as the time the auction
    closed, and never say when the auction closed at all.
  - Do not infer, estimate, or describe WHEN anything happened beyond the
    bid timestamps written below, and do not characterise any bid or the
    close as early, late, last-minute, or near the scheduled time.
  - Do not list the bids one by one. Give the count, the opening amount and
    the winning amount.
  - Do not speculate about why anyone bid, what the item is worth, or what
    it might have sold for elsewhere.
  - Do not describe the bidding as competitive, fierce, slow, or any other
    judgement the numbers do not literally state.

If a fact is not in the data, leave it out. A shorter summary is correct;
an invented one is not.

Write one paragraph of at most 70 words. Plain prose -- no markdown, no
bullet points, no headings."""

# The user turn for the summary job. The auction data is server-ASSEMBLED --
# the server chose the fields, the labels and the layout -- so it goes in the
# system turn, leaving the user turn carrying only this fixed instruction.
#
# Server-assembled is NOT the same as server-originated, and the difference
# matters here. `item` came from a seller's CreateAuction and `bidder` from
# whoever logged in, so user-originated VALUES do reach the system turn in
# this one job. The structure around them is ours; the strings inside are
# not. See the note on build_summary_messages.
_SUMMARY_USER = "Summarise this auction."


def _render_auction(auction: llm_pb2.AuctionContext) -> str:
    """Render AuctionContext as the plain-text block the prompt carries.

    `closed` is read straight off the message -- it is stored state, decided
    once by the cluster's applied CloseAuction. This function must never
    compare close_time to its own clock: correctness property 4 exists
    precisely so that two nodes with skewed clocks cannot disagree about
    whether an auction has ended, and a second opinion formed here would
    reintroduce that.
    """
    close_time = time_conv.micros_to_dt(auction.close_time)
    return "\n".join(
        [
            f"  auction id:       {auction.auction_id}",
            f"  item:             {auction.item}",
            f"  status:           {'closed' if auction.closed else 'open'}",
            f"  scheduled close:  {close_time:%Y-%m-%d %H:%M:%S} UTC",
            f"  current high bid: {auction.current_high_bid}",
            f"  bids so far:      {auction.bid_count}",
        ]
    )


def _render_bid_history(bids) -> str:
    if not bids:
        return "  (no bids were placed)"
    return "\n".join(
        f"  {i}. {bid.bidder} bid {bid.amount} at "
        f"{time_conv.micros_to_dt(bid.time):%Y-%m-%d %H:%M:%S} UTC"
        for i, bid in enumerate(bids, start=1)
    )


def build_faq_messages(request: llm_pb2.BiddingQuestionRequest) -> list[dict]:
    """System turn: role + live auction state. User turn: the question.

    This is what handout §4 means by context-aware -- the auction state is
    injected into the prompt so the model answers from it rather than from
    its own knowledge. The auction field is optional: a general question
    ("how does a reserve price work?") arrives without one.
    """
    system = _FAQ_SYSTEM
    if request.HasField("auction"):
        system += (
            "\n\nThe bidder is asking about this auction. These figures are "
            "authoritative -- prefer them over anything you think you know:\n"
            + _render_auction(request.auction)
        )
    # request.question is untrusted and goes in the user turn VERBATIM.
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": request.question},
    ]


def build_description_messages(request: llm_pb2.ItemDescriptionRequest) -> list[dict]:
    """System turn: role + rules. User turn: the seller's item and attributes.

    The attributes are seller-supplied, so they are untrusted on the same
    grounds as a bidder's question and go in the user turn for the same
    reason.
    """
    attributes = "\n".join(f"  {a.key}: {a.value}" for a in request.attributes)
    user = f"Item: {request.item}\nAttributes:\n{attributes}"
    return [
        {"role": "system", "content": _DESCRIPTION_SYSTEM},
        {"role": "user", "content": user},
    ]


def build_summary_messages(request: llm_pb2.AuctionSummaryRequest) -> list[dict]:
    """System turn: role + the auction and its price history. User turn: a
    fixed instruction.

    Every byte of this prompt is server-ASSEMBLED: the server picked the
    fields, wrote the labels and laid out the block. That is weaker than
    server-originated, and the gap is a real one -- `item` is a seller's
    string and `bidder` is a username, so user-originated values do land in
    the system turn here, unlike in the other two jobs.

    This is the one place the module's trust boundary is softer than it
    looks. It is called out rather than quietly relied on; closing it means
    moving the auction block into a user turn, which is a design change, not
    a comment fix.
    """
    system = (
        _SUMMARY_SYSTEM
        + "\n\nAuction:\n"
        + _render_auction(request.auction)
        + "\n\nBid history, in the order the bids were applied:\n"
        + _render_bid_history(request.bid_history)
    )
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": _SUMMARY_USER},
    ]


# --- Error mapping ---------------------------------------------------------


def map_ollama_error(exc: BaseException) -> tuple[grpc.StatusCode, str]:
    """Map an exception from the Ollama client to the status in llm.proto.

    Kept as a free function so the mapping can be tested without standing up
    a server, and so all three handlers cannot drift apart on it.
    """
    if isinstance(exc, httpx.TimeoutException):
        # Note: httpx.ConnectTimeout lands here rather than under
        # ConnectionError below, because in httpx's hierarchy it is a
        # TimeoutException and not a ConnectError. Arguably "could not even
        # reach the daemon" is UNAVAILABLE, but splitting the two buys the
        # caller nothing -- it degrades identically either way -- so this
        # keeps the single rule "the clock ran out" -> DEADLINE_EXCEEDED.
        return (
            grpc.StatusCode.DEADLINE_EXCEEDED,
            f"LLM did not respond within {config.TIMEOUT_SECONDS}s",
        )

    if isinstance(exc, ConnectionError):
        # The Ollama client converts httpx.ConnectError into a builtin
        # ConnectionError, so this is the "daemon is not running" case.
        return (
            grpc.StatusCode.UNAVAILABLE,
            "LLM backend unreachable: is the Ollama daemon running?",
        )

    if isinstance(exc, ResponseError):
        if exc.status_code == 404:
            # The model is not pulled. Misconfiguration, not a transient
            # fault -- retrying will never fix it, so not UNAVAILABLE.
            return (
                grpc.StatusCode.FAILED_PRECONDITION,
                f"model {config.MODEL!r} is not installed on the LLM backend",
            )
        return (grpc.StatusCode.INTERNAL, f"LLM backend error: {exc}")

    if isinstance(exc, RequestError):
        # The client rejected our request before sending it -- our bug, not
        # the caller's, so INTERNAL rather than INVALID_ARGUMENT.
        return (grpc.StatusCode.INTERNAL, f"malformed LLM request: {exc}")

    return (grpc.StatusCode.INTERNAL, f"unexpected LLM failure: {exc!r}")


# --- Servicer --------------------------------------------------------------


class LLMServicer(llm_pb2_grpc.LLMServiceServicer):
    """Stateless. One Ollama client, reused across calls and across threads.

    The client is injected so the test suite can pass a fake and run with no
    model and no network. Passing None builds the real one.
    """

    def __init__(self, client=None, model: str = config.MODEL):
        self._client = (
            client if client is not None else Client(timeout=config.TIMEOUT_SECONDS)
        )
        self._model = model

    def _generate(self, job: str, request_id: str, messages, max_tokens, context) -> str:
        """Call the model once, timing and logging the attempt either way.

        Never returns on failure: context.abort() raises, which is what
        turns the mapped status into an RpcError on the caller's side.
        """
        started = time.perf_counter()
        try:
            response = self._client.chat(
                model=self._model,
                messages=messages,
                options={"num_predict": max_tokens},
            )
        except Exception as exc:  # noqa: BLE001 -- every failure is mapped below
            elapsed = time.perf_counter() - started
            code, detail = map_ollama_error(exc)
            log.warning(
                "llm %s request_id=%s FAILED in %.2fs: %s: %s",
                job, request_id, elapsed, code.name, detail,
            )
            context.abort(code, detail)

        elapsed = time.perf_counter() - started
        answer = response.message.content.strip()
        log.info(
            "llm %s request_id=%s ok in %.2fs (%d chars)",
            job, request_id, elapsed, len(answer),
        )
        return answer

    def AnswerBiddingQuestion(
        self, request: llm_pb2.BiddingQuestionRequest, context
    ) -> llm_pb2.LLMAnswerResponse:
        """(a) Bidding-assistant FAQ, optionally about a specific auction."""
        if not request.question.strip():
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "question must not be empty")

        answer = self._generate(
            "faq",
            request.request_id,
            build_faq_messages(request),
            config.MAX_TOKENS_FAQ,
            context,
        )
        return llm_pb2.LLMAnswerResponse(request_id=request.request_id, answer=answer)

    def GenerateItemDescription(
        self, request: llm_pb2.ItemDescriptionRequest, context
    ) -> llm_pb2.LLMAnswerResponse:
        """(b) Listing description from seller-supplied attributes.

        Both guards below are the same rule: with nothing to work from, the
        model invents the item's properties, and an invented description is
        a listing a bidder goes on to rely on.
        """
        if not request.item.strip():
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "item must not be empty")
        if not request.attributes:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "at least one attribute is required"
            )

        answer = self._generate(
            "description",
            request.request_id,
            build_description_messages(request),
            config.MAX_TOKENS_DESCRIPTION,
            context,
        )
        return llm_pb2.LLMAnswerResponse(request_id=request.request_id, answer=answer)

    def SummariseAuctionResult(
        self, request: llm_pb2.AuctionSummaryRequest, context
    ) -> llm_pb2.LLMAnswerResponse:
        """(c) Summary of an auction's result and price history.

        An empty bid_history is NOT an error: "nobody bid" is a real
        outcome, and summarising it is exactly the useful thing to do.
        """
        if not request.HasField("auction"):
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "auction context is required")

        answer = self._generate(
            "summary",
            request.request_id,
            build_summary_messages(request),
            config.MAX_TOKENS_SUMMARY,
            context,
        )
        return llm_pb2.LLMAnswerResponse(request_id=request.request_id, answer=answer)
