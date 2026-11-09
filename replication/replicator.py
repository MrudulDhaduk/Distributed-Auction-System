"""The write path from a handler into the Raft log, in one place.

    applied = replicator.submit(command, client_id, seq)

does: encode the command (codec.py) -> raft.propose(bytes) -> wait, at most
`timeout` seconds, for the Proposal to resolve -> return the Applied
response that apply_fn produced for THIS entry (R4).

A handler sees exactly three things:
  Applied               the command was committed and applied; this is
                        its response (success or a business "no").
  NotLeaderError        refused at once, nothing appended anywhere. Carries
                        the leader this node believes in (id and CLIENT
                        address), or None. Safe to retry there.
  OutcomeUnknownError   anything else: LEADERSHIP_LOST, STOPPED, OVERWRITTEN,
                        or the wait timed out. The command may or may not
                        commit. The client must retry with the SAME
                        (client_id, seq); the dedupe table makes that safe.

OVERWRITTEN is in fact a definite "not committed", but it is folded into
"unknown" on purpose: the client's correct action is the same (retry with
the same serial number), and one rule is easier to get right than two.
(docs/decisions.md, A6.)

Two implementations with one interface:
  RaftReplicator   the real one, over a RaftNode.
  LocalReplicator  a cluster of one with no Raft: encodes, then calls
                   apply_fn directly. Used where M1 code is unit-tested
                   on its own (AuctionServicer(state, llm), SessionStore()).
"""

import itertools
import threading
from typing import Optional

from raft.node import RaftNode, ReadOutcome, Role
from raft.proposal import Outcome, Proposal
from replication import codec
from replication.state_machine import Applied, StateMachine

# How long a handler waits for its entry to be applied. A healthy commit is
# one AppendEntries round (~1-2 ms on localhost); a leader change takes an
# election timeout (0.5-1.0 s) plus a round. 5 s is "something is wrong"
# territory, and stays well under the client's own deadline
# (client/auction_client.py) so the client hears UNAVAILABLE, not a
# DEADLINE_EXCEEDED it cannot tell apart from a dead server.
DEFAULT_TIMEOUT = 5.0


class NotLeaderError(Exception):
    def __init__(self, leader_id: Optional[str], leader_addr: Optional[str]):
        self.leader_id = leader_id
        self.leader_addr = leader_addr
        where = f"{leader_id} at {leader_addr}" if leader_id else "unknown"
        super().__init__(f"not the leader; leader is {where}")


class OutcomeUnknownError(Exception):
    pass


class ReadNotConfirmedError(OutcomeUnknownError):
    """read_barrier() could not confirm leadership in time (or lost it).
    Nothing was read; the client retries. A subclass so every caller that
    handles "retry later" handles this too."""


class RaftReplicator:
    def __init__(self, node: RaftNode, client_addrs: dict, timeout: float = DEFAULT_TIMEOUT):
        """client_addrs: node id -> "host:client_port", for EVERY node,
        so a NOT_LEADER reply can tell the client where to go (the Raft
        layer only knows node ids)."""
        self.node = node
        self._client_addrs = dict(client_addrs)
        self._timeout = timeout

    def _not_leader(self, leader_id) -> NotLeaderError:
        return NotLeaderError(leader_id, self._client_addrs.get(leader_id) if leader_id else None)

    def is_leader(self) -> bool:
        return self.node.status().role == Role.LEADER

    def check_leader(self) -> None:
        """Raise NotLeaderError unless this node is the leader right now.

        Only a pre-check, to spare followers expensive work (the LLM call
        before CreateAuction). It is NOT a guarantee: the node can lose
        leadership a moment later. propose() decides for real.
        """
        st = self.node.status()
        if st.role != Role.LEADER:
            raise self._not_leader(st.leader_id)

    def propose(self, command, client_id: str = "", seq: int = 0) -> Proposal:
        """Encode and propose, without waiting (the closer uses this)."""
        return self.node.propose(codec.encode(command, client_id, seq))

    def submit(self, command, client_id: str = "", seq: int = 0) -> Applied:
        proposal = self.propose(command, client_id, seq)
        if proposal.outcome is Outcome.NOT_LEADER:
            raise self._not_leader(proposal.leader_hint)
        if not proposal.wait(self._timeout):
            raise OutcomeUnknownError(
                f"timed out after {self._timeout:.1f}s waiting for index {proposal.index}"
            )
        if proposal.outcome is Outcome.APPLIED:
            return proposal.result
        raise OutcomeUnknownError(f"{proposal.outcome.value} at index {proposal.index}")

    def read_barrier(self) -> int:
        """RaftNode.read_index (section 8): on return, this node is a
        leader that a majority still followed after the call began, and it
        has applied every entry committed before the call began. Reading
        local state now is linearizable. Returns the read index."""
        r = self.node.read_index(self._timeout)
        if r.ok:
            return r.index
        if r.outcome is ReadOutcome.NOT_LEADER:
            raise self._not_leader(r.leader_hint)
        raise ReadNotConfirmedError(f"read not confirmed ({r.outcome.value})")


class LocalReplicator:
    """A cluster of one, without Raft: always the leader, applies at once.
    Still goes through encode/decode, so a unit test of M1 code also
    exercises the real command encoding."""

    def __init__(self, state_machine: StateMachine):
        self.state_machine = state_machine
        self._index = itertools.count(1)
        self._lock = threading.Lock()

    def is_leader(self) -> bool:
        return True

    def check_leader(self) -> None:
        pass

    def propose(self, command, client_id: str = "", seq: int = 0) -> Proposal:
        data = codec.encode(command, client_id, seq)
        with self._lock:
            index = next(self._index)
        proposal = Proposal(index, term=1)
        proposal._resolve(Outcome.APPLIED, self.state_machine.apply_fn(index, data))
        return proposal

    def submit(self, command, client_id: str = "", seq: int = 0) -> Applied:
        return self.propose(command, client_id, seq).result

    def read_barrier(self) -> int:
        """A cluster of one is always up to date with itself."""
        return 0
