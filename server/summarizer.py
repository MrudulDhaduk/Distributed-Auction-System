"""Background worker that records an LLM-written result summary for every
closed auction (chunk 7, step 2; R6).

    closed auction with no summary
        -> LEADER only: snapshot the auction under the lock, release it, ask
           the model (seconds), propose RecordSummary(auction_id, text)
        -> apply() stores the text as plain data on every node

Same shape as server/closer.py (a fixed-interval poll thread whose whole
logic is tick(), so tests need no thread), and for the same reasons:

  * It runs on EVERY node and acts only while its node is the leader. A
    follower's tick does nothing and never calls the model.
  * It remembers nothing it cannot see in the replicated state. "Which
    auctions still need a summary?" is answered by scanning the state: closed
    and no accepted summary. So after a failover the NEW leader's worker finds
    exactly the auctions the old leader closed but never summarised (it may
    have died between the close and the summary), and there is no hand-over.
  * It is IDEMPOTENT. An auction takes at most one summary (apply() refuses a
    second, auction/state.py summary_rejection), so a duplicate from a second
    leader or a retry changes nothing; the loser drops its text and is not
    retried.

It is a separate thread from the closer, not a step inside its tick, because
the closer's tick must stay fast: a close is due on the closer's 250 ms
clock, and a model call takes seconds.

THE MODEL IS DOWN OR SLOW: the call raises grpc.RpcError (LLMClient puts a
deadline on every call). Nothing is proposed; the auction is already closed
and stays closed. The auction is tried again after `retry_after_seconds`,
not on the next tick, so a dead LLM server is not hammered. The pause is
measured with time.monotonic(), never the wall clock (CLAUDE.md: Raft timers).

Proposals carry client_id "" (the cluster is the author, like CloseAuction),
so they are not deduplicated by the section 8 table; summary_rejection is
what makes them idempotent.
"""

import logging
import re
import threading
import time
import uuid

import grpc

from auction.state import AuctionState, RecordSummary
from replication.replicator import NotLeaderError, OutcomeUnknownError
from server import llm_context

DEFAULT_INTERVAL_SECONDS = 1.0
DEFAULT_RETRY_AFTER_SECONDS = 10.0
STOP_JOIN_SECONDS = 5.0

log = logging.getLogger("server.summarizer")

# A number: digits, optionally with comma separators ("1,200") and a decimal part.
_NUMBER = re.compile(r"\d[\d,]*(?:\.\d+)?")


def _numbers(text: str) -> set:
    """Every number in `text`, commas stripped, so "1,200" and "1200" are the same."""
    return {m.replace(",", "") for m in _NUMBER.findall(text)}


def summary_numbers_ok(summary: str, facts: dict) -> bool:
    """True only if every number in the model's summary also appears in the
    string form of some value in `facts`. A dropped digit ("40" for 400) fails.
    Known limit: it cannot catch the model swapping two numbers that are both
    in the facts (e.g. opening and closing bid)."""
    allowed = set()
    for value in facts.values():
        allowed |= _numbers(str(value))
    return _numbers(summary) <= allowed


def template_summary(facts: dict) -> str:
    """A plain summary built only from the facts, no model involved."""
    if not facts["bids"]:
        return f"{facts['item']}: no bids were placed."
    return (
        f"{facts['item']}: bids placed {facts['bids']}, from {facts['distinct_bidders']} bidders. "
        f"Opening bid {facts['opening_bid']}, closing bid {facts['closing_bid']}. "
        f"Winner {facts['winner']}, with a winning bid of {facts['winning_bid']}."
    )


def _facts(context: dict) -> dict:
    """The same figures the LLM server puts in its FACTS block
    (llm_servicer._summary_facts), worked out here from the snapshot, so the
    check compares the model's text to the state and not to its own prompt."""
    bids = context["bid_history"]
    facts = {
        "item": context["auction"]["item"],
        "bids": len(bids),
        "distinct_bidders": len({b["bidder"] for b in bids}),
        "opening_bid": None, "closing_bid": None, "winner": None, "winning_bid": None,
    }
    if bids:
        top = max(bids, key=lambda b: b["amount"])
        facts.update(
            opening_bid=bids[0]["amount"], closing_bid=bids[-1]["amount"],
            winner=top["bidder"], winning_bid=top["amount"],
        )
    return facts


class SummaryWorker:
    def __init__(
        self,
        state: AuctionState,
        llm_client,
        replicator,
        interval_seconds: float = DEFAULT_INTERVAL_SECONDS,
        retry_after_seconds: float = DEFAULT_RETRY_AFTER_SECONDS,
    ):
        self._state = state
        self._llm = llm_client
        self._replicator = replicator
        self._interval_seconds = interval_seconds
        self._retry_after = retry_after_seconds
        # auction_id -> monotonic time before which it is not tried again.
        # This node's own pacing, not replicated state, and lost on restart:
        # at worst one extra attempt.
        self._retry_at: dict = {}
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="summary-worker", daemon=True)

    def _pending(self, now: float) -> list:
        """Closed auctions with no accepted summary that are not waiting out a
        pause. A scan of the state, taken under its lock."""
        with self._state.lock:
            return [
                aid
                for aid, a in self._state.auctions.items()
                if a["closed"] and not a.get("summaries") and self._retry_at.get(aid, float("-inf")) <= now
            ]

    def tick(self, now: float | None = None) -> list:
        """One pass. Returns the auction ids whose summary THIS pass got
        recorded. `now` is a time.monotonic() reading; tests pass numbers."""
        if now is None:
            now = time.monotonic()
        if not self._replicator.is_leader():
            return []
        recorded = []
        for auction_id in self._pending(now):
            if not self._replicator.is_leader():
                break  # lost leadership during an earlier (slow) model call
            if self._summarise(auction_id, now):
                recorded.append(auction_id)
        if recorded:
            log.info("summarizer: recorded summaries for %s", recorded)
        return recorded

    def _summarise(self, auction_id: int, now: float) -> bool:
        with self._state.lock:
            auction = self._state.auctions.get(auction_id)
            if auction is None or not auction["closed"] or auction.get("summaries"):
                return False
            context = {
                "auction": llm_context.auction_context(auction_id, auction),
                "bid_history": llm_context.bid_history(auction),
            }

        request_id = str(uuid.uuid4())
        try:
            _, text = self._llm.get_llm_answer(request_id=request_id, query="", context=context)
        except grpc.RpcError as e:
            self._retry_at[auction_id] = now + self._retry_after
            log.warning(
                "summarizer: LLM failed for auction %s (request_id=%s), retrying in %.0fs: %s",
                auction_id, request_id, self._retry_after, llm_context.failure_reason(e),
            )
            return False

        facts = _facts(context)
        if not summary_numbers_ok(text, facts):
            log.warning("summarizer: auction %s summary has a number not in the facts; using the template", auction_id)
            text = template_summary(facts)

        try:
            applied = self._replicator.submit(RecordSummary(auction_id=auction_id, summary=text))
        except NotLeaderError:
            return False  # someone else leads now; their worker will find it
        except OutcomeUnknownError:
            # May or may not have committed. The scan decides next time: if it
            # did, the auction has a summary and drops out of the list.
            self._retry_at[auction_id] = now + self._retry_after
            return False
        if not applied.result.success:
            # Refused: another writer got there first, or the text was empty.
            # Either way the state is authoritative; no retry marker needed
            # (a refusal for emptiness would otherwise loop, so pace it).
            log.info("summarizer: auction %s summary refused: %s", auction_id, applied.result.reason)
            self._retry_at[auction_id] = now + self._retry_after
            return False
        return True

    def start(self):
        self._thread.start()

    def stop(self):
        """Signal the loop to exit and wait, boundedly, for the thread."""
        self._stop.set()
        self._thread.join(timeout=STOP_JOIN_SECONDS)

    def _run(self):
        while not self._stop.is_set():
            try:
                self.tick()
            except Exception:
                log.exception("summarizer tick failed")
            self._stop.wait(self._interval_seconds)
