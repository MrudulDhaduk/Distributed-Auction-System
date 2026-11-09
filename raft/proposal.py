"""The handle that RaftNode.propose() returns.

What "acknowledged" means -- the contract the property checker will rely on
(docs/decisions.md, R4):

  outcome APPLIED       The entry with exactly this (index, term) was
                        committed and this node's apply_fn has returned for
                        it. `result` is what apply_fn returned. By State
                        Machine Safety every other node applies the same
                        entry at the same index (if it applies that index
                        at all). This is the ONLY success.

  outcome NOT_LEADER    Refused at once; nothing was appended anywhere.
                        `leader_hint` is the leader this node believes in,
                        or None. Safe to retry on that node.

  outcome LEADERSHIP_LOST   The node stopped being leader before the entry
  outcome STOPPED           was applied here (or the node was stopped).
                        UNKNOWN outcome: the entry may still be committed
                        by a later leader, or it may be overwritten. The
                        caller must not assume either. Same as a timeout.

  outcome OVERWRITTEN   A different entry (another term) was applied at
                        this index, so this entry can never be committed.

A handle never moves from failure to success or back; it resolves once.
"""

import enum
import threading
from typing import Any, Optional


class Outcome(enum.Enum):
    APPLIED = "APPLIED"
    NOT_LEADER = "NOT_LEADER"
    LEADERSHIP_LOST = "LEADERSHIP_LOST"
    OVERWRITTEN = "OVERWRITTEN"
    STOPPED = "STOPPED"


class Proposal:
    def __init__(self, index: int, term: int, leader_hint: Optional[str] = None):
        self.index = index  # 0 if nothing was appended (NOT_LEADER)
        self.term = term
        self.leader_hint = leader_hint
        self.outcome: Optional[Outcome] = None  # None while pending
        self.result: Any = None
        self._done = threading.Event()

    @property
    def ok(self) -> bool:
        return self.outcome is Outcome.APPLIED

    def wait(self, timeout: Optional[float] = None) -> bool:
        """Block until resolved. True if resolved, False on timeout (still
        pending: the outcome is not known yet)."""
        return self._done.wait(timeout)

    def _resolve(self, outcome: Outcome, result: Any = None) -> None:
        """Called by the node, under its lock. First resolution wins."""
        if self._done.is_set():
            return
        self.outcome = outcome
        self.result = result
        self._done.set()

    def __repr__(self):
        state = self.outcome.value if self.outcome else "PENDING"
        return f"Proposal(index={self.index}, term={self.term}, {state})"
