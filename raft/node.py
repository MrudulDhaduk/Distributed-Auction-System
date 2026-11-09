"""The Raft node core: leader election only (Raft paper, Ongaro & Ousterhout,
Figure 2 and section 5.2). No log, no replication, no persistence.

Why this file looks the way it does -- the invariants first
-----------------------------------------------------------
Election Safety (Figure 3): at most one leader per term. It holds because of
two facts this file enforces:
  (a) a node grants AT MOST ONE vote per term  (votedFor, cleared only when
      currentTerm increases), and
  (b) a candidate needs votes from a MAJORITY, and any two majorities of the
      same cluster overlap in at least one node.
So two different candidates cannot both collect a majority in one term.

Liveness (a leader eventually emerges) comes from randomised election
timeouts: two nodes rarely time out together, so one usually wins before
anyone else starts.

Threading model
---------------
  * ONE lock (self._lock) guards all mutable node state.
  * The lock is NEVER held across an outgoing RPC. Handlers and reply
    processing take it briefly, decide, mutate, release. Outgoing RPCs are
    prepared under the lock as plain argument objects, then sent after it is
    released, in parallel, on a worker pool.
  * Every reply path re-checks term and role after re-acquiring the lock,
    because the world may have moved on while the RPC was in flight.
  * ONE ticker thread owns time: it wakes every TICK_INTERVAL, and under the
    lock checks the election deadline (follower/candidate) or the next
    heartbeat time (leader). It never sends an RPC itself.
  * The node never imports grpc. It talks to peers through the Transport
    interface in raft/messages.py.

"Caller holds the lock" is the contract for every method whose name starts
with an underscore and that mutates state.
"""

import concurrent.futures
import dataclasses
import enum
import logging
import random
import threading
import time
from typing import Callable, Optional, Sequence

from raft.messages import (
    AppendEntriesArgs,
    AppendEntriesReply,
    RequestVoteArgs,
    RequestVoteReply,
    Transport,
    TransportError,
)

log = logging.getLogger("raft.node")

# --- Timing -----------------------------------------------------------------
# Paper section 5.6: broadcastTime << electionTimeout << MTBF.
#
# Defaults are for Python + gRPC on one machine (docs/decisions.md has the
# reasoning): a localhost RPC takes ~1 ms but a Python process on Windows can
# stall tens of ms (GIL, 15.6 ms timer tick, console I/O), so the paper's
# 150-300 ms is too tight a margin. We use 500-1000 ms election timeouts and
# a 100 ms heartbeat: the shortest timeout still spans five heartbeat
# periods, so several consecutive heartbeats can be late or lost before a
# healthy leader is wrongly deposed; and the 500 ms-wide random window is
# ~100x a vote round trip, so split votes are rare.
DEFAULT_ELECTION_TIMEOUT = (0.5, 1.0)
DEFAULT_HEARTBEAT_INTERVAL = 0.1
# How often the ticker looks at the clock. Adds at most this much jitter to
# a timeout; 5 ms is ~1% of the shortest default timeout.
TICK_INTERVAL = 0.005


class Role(enum.Enum):
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"


@dataclasses.dataclass(frozen=True)
class RoleChange:
    """One change of (currentTerm, role), delivered to the observer."""

    node_id: str
    old_term: int
    new_term: int
    old_role: Role
    new_role: Role
    voted_for: Optional[str]  # votedFor immediately after the change
    reason: str

    def describe(self) -> str:
        line = f"[{self.node_id}] term {self.new_term} {self.old_role.value} -> {self.new_role.value}"
        if self.old_term != self.new_term:
            line += f" (was term {self.old_term})"
        return f"{line}: {self.reason}"


@dataclasses.dataclass(frozen=True)
class NodeStatus:
    """A consistent snapshot, taken under the lock."""

    node_id: str
    term: int
    role: Role
    voted_for: Optional[str]
    leader_id: Optional[str]


def _empty_log() -> tuple:
    """(lastLogIndex, lastLogTerm) of a log that does not exist yet."""
    return (0, 0)


class RaftNode:
    def __init__(
        self,
        node_id: str,
        peer_ids: Sequence[str],
        transport: Transport,
        *,
        election_timeout: tuple = DEFAULT_ELECTION_TIMEOUT,
        heartbeat_interval: float = DEFAULT_HEARTBEAT_INTERVAL,
        rng: Optional[random.Random] = None,
        observer: Optional[Callable[[RoleChange], None]] = None,
        last_log: Optional[Callable[[], tuple]] = None,
    ):
        """
        observer: called with a RoleChange on every change of (term, role),
            WHILE the node lock is held, so events are delivered in exactly
            the order the changes happened. It must therefore be quick,
            must not block, and must not call back into this node.
        last_log: returns (lastLogIndex, lastLogTerm). Defaults to (0, 0),
            the empty log. Step 4 replaces it with the real log; the voting
            rule below already uses it. Called under the lock.
        """
        lo, hi = election_timeout
        if not 0 < lo <= hi:
            raise ValueError("election_timeout must be 0 < min <= max")
        if heartbeat_interval >= lo:
            raise ValueError("heartbeat_interval must be below the minimum election timeout")
        if node_id in peer_ids:
            raise ValueError("a node is not its own peer")

        self.node_id = node_id
        self._peers = list(peer_ids)
        self._transport = transport
        self._election_timeout = (lo, hi)
        self._heartbeat_interval = heartbeat_interval
        self._rng = rng or random.Random()
        self._observer = observer
        self._last_log = last_log or _empty_log

        # Figure 2, "State" -- persistent in the paper, in memory only here
        # (step 6; see docs/raft-election.md, "Known gaps").
        self._current_term = 0
        self._voted_for: Optional[str] = None

        # Volatile.
        self._role = Role.FOLLOWER
        self._leader_id: Optional[str] = None
        self._votes: set = set()  # who voted for us in the current candidacy
        self._election_deadline = 0.0  # time.monotonic() value
        self._next_heartbeat = 0.0
        self._hb_in_flight: set = set()  # peers with an unanswered heartbeat

        # Cluster of N = peers + self; a majority is more than half.
        self._majority = (len(self._peers) + 1) // 2 + 1

        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._ticker: Optional[threading.Thread] = None
        self._pool: Optional[concurrent.futures.ThreadPoolExecutor] = None

    # --- lifecycle -----------------------------------------------------------

    def start(self):
        with self._lock:
            self._reset_election_timer()
        self._pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=max(2, 2 * len(self._peers)),
            thread_name_prefix=f"raft-{self.node_id}-rpc",
        )
        self._ticker = threading.Thread(
            target=self._run_ticker, name=f"raft-{self.node_id}-ticker", daemon=True
        )
        self._ticker.start()

    def stop(self):
        self._stop.set()
        if self._ticker is not None:
            self._ticker.join(timeout=5)
        if self._pool is not None:
            self._pool.shutdown(wait=False, cancel_futures=True)

    def status(self) -> NodeStatus:
        with self._lock:
            return NodeStatus(
                self.node_id, self._current_term, self._role, self._voted_for, self._leader_id
            )

    # --- the clock -----------------------------------------------------------

    def _run_ticker(self):
        while not self._stop.wait(TICK_INTERVAL):
            try:
                self._tick()
            except Exception:
                log.exception("[%s] ticker error", self.node_id)

    def _tick(self):
        now = time.monotonic()
        votes, heartbeats = [], []
        with self._lock:
            if self._role == Role.LEADER:
                # Figure 2, Leaders: "repeat during idle periods to prevent
                # election timeouts (5.2)".
                if now >= self._next_heartbeat:
                    self._next_heartbeat = now + self._heartbeat_interval
                    heartbeats = self._prepare_heartbeats()
            elif now >= self._election_deadline:
                # Figure 2, Followers (and Candidates): "If election timeout
                # elapses without receiving AppendEntries RPC from current
                # leader or granting vote to candidate: convert to candidate"
                # / "If election timeout elapses: start new election".
                votes = self._start_election()
        # Lock released: only now do any RPCs leave the node.
        for peer, args in votes:
            self._submit(self._send_vote_request, peer, args)
        for peer, args in heartbeats:
            self._submit(self._send_heartbeat, peer, args)

    def _reset_election_timer(self):
        lo, hi = self._election_timeout
        self._election_deadline = time.monotonic() + self._rng.uniform(lo, hi)

    # --- state changes (caller holds the lock) -------------------------------

    def _transition(self, term: int, role: Role, reason: str, vote: Optional[str] = None):
        """The ONLY place currentTerm or role change. Keeps the rule "votedFor
        belongs to currentTerm" in one spot: whenever the term increases the
        ballot is blank again, except that a candidate has already voted for
        itself (`vote`).
        """
        old_term, old_role = self._current_term, self._role
        if term < old_term:
            raise AssertionError(f"term went backwards: {old_term} -> {term}")
        if term > old_term:
            self._current_term = term
            self._voted_for = vote
            self._votes = set()
        self._role = role
        if role != Role.LEADER:
            self._hb_in_flight.clear()
        if old_role == Role.LEADER and role != Role.LEADER:
            # A leader never runs the election timer, so its deadline is long
            # stale. Arm a fresh one, or the deposed leader would start an
            # election on the very next tick. (docs/decisions.md)
            self._reset_election_timer()
        if (term, role) != (old_term, old_role):
            self._emit(RoleChange(self.node_id, old_term, term, old_role, role, self._voted_for, reason))

    def _step_down_if_stale(self, term: int, source: str) -> bool:
        """Figure 2, All Servers: "If RPC request or response contains term T
        > currentTerm: set currentTerm = T, convert to follower (5.1)".
        Called from every handler and every reply path. Returns True if the
        node stepped down.
        """
        if term > self._current_term:
            self._transition(term, Role.FOLLOWER, f"higher term {term} seen in {source}")
            # A new term has no known leader yet. (Without this a deposed
            # leader would keep reporting itself as leader_id.)
            self._leader_id = None
            return True
        return False

    def _start_election(self) -> list:
        """Figure 2, Candidates, "On conversion to candidate, start election":
        increment currentTerm; vote for self; reset election timer; send
        RequestVote RPCs to all other servers. Returns the RPCs to send.
        """
        self._transition(
            self._current_term + 1, Role.CANDIDATE, "election timeout", vote=self.node_id
        )
        self._leader_id = None
        self._votes = {self.node_id}
        self._reset_election_timer()
        if len(self._votes) >= self._majority:  # cluster of one
            self._become_leader()
            return []
        last_index, last_term = self._last_log()
        return [
            (
                peer,
                RequestVoteArgs(self.node_id, peer, self._current_term, last_index, last_term),
            )
            for peer in self._peers
        ]

    def _become_leader(self):
        """Figure 2, Candidates: "If votes received from majority of servers:
        become leader". Leaders: "Upon election: send initial empty
        AppendEntries RPCs (heartbeat) to each server" -- done by the next
        tick (<= TICK_INTERVAL away) rather than from this reply thread, so
        heartbeats have exactly one sender.
        """
        self._transition(self._current_term, Role.LEADER, f"won election with {len(self._votes)} votes")
        self._leader_id = self.node_id
        self._next_heartbeat = 0.0

    def _prepare_heartbeats(self) -> list:
        # Election-only chunk: no log, so prevIndex = prevTerm = commitIndex
        # = 0 and entries is empty.
        out = []
        for peer in self._peers:
            if peer in self._hb_in_flight:
                continue  # one unanswered heartbeat per peer is plenty
            self._hb_in_flight.add(peer)
            out.append(
                (peer, AppendEntriesArgs(self.node_id, peer, self._current_term, 0, 0, 0, ()))
            )
        return out

    def _emit(self, event: RoleChange):
        if self._observer is None:
            return
        try:
            self._observer(event)
        except Exception:
            log.exception("[%s] observer failed", self.node_id)

    # --- incoming RPCs (called by the servicer / InMemoryTransport) ----------

    def handle_request_vote(self, args: RequestVoteArgs) -> RequestVoteReply:
        """Figure 2, "RequestVote RPC", receiver implementation."""
        with self._lock:
            # All Servers rule first: it can raise currentTerm and clear
            # votedFor, which the checks below then see.
            self._step_down_if_stale(args.term, f"RequestVote from {args.from_id}")

            granted = False
            if args.term < self._current_term:
                # Receiver impl. 1: "Reply false if term < currentTerm".
                why = f"stale term {args.term}"
            elif self._voted_for not in (None, args.from_id):
                # Receiver impl. 2, first half: "votedFor is null or candidateId".
                why = f"already voted for {self._voted_for} in term {self._current_term}"
            elif not self._candidate_log_ok(args):
                # Receiver impl. 2, second half: "candidate's log is at least
                # as up-to-date as receiver's log" (5.4.1).
                my_index, my_term = self._last_log()
                why = (
                    f"candidate log (term {args.last_log_term}, index {args.last_log_index})"
                    f" is behind ours (term {my_term}, index {my_index})"
                )
            else:
                granted = True
                self._voted_for = args.from_id
                # Followers: the timer is reset on "granting vote to candidate".
                self._reset_election_timer()
                why = "ok"

            log.info(
                "[%s] term %d: %s vote for %s (%s)",
                self.node_id, self._current_term,
                "GRANTED" if granted else "refused", args.from_id, why,
            )
            return RequestVoteReply(self.node_id, args.from_id, self._current_term, granted)

    def _candidate_log_ok(self, args: RequestVoteArgs) -> bool:
        """Section 5.4.1: "If the logs have last entries with different
        terms, then the log with the later term is more up-to-date. If the
        logs end with the same term, then whichever log is longer is more
        up-to-date." Comparing (lastLogTerm, lastLogIndex) as a tuple is
        exactly that rule: term first, index only to break a tie.
        """
        my_index, my_term = self._last_log()
        return (args.last_log_term, args.last_log_index) >= (my_term, my_index)

    def handle_append_entries(self, args: AppendEntriesArgs) -> AppendEntriesReply:
        """Figure 2, "AppendEntries RPC", receiver implementation -- the
        heartbeat subset: steps 1 and 2 only (no entries to append, no
        commitIndex to advance yet).
        """
        with self._lock:
            self._step_down_if_stale(args.term, f"AppendEntries from {args.from_id}")

            if args.term < self._current_term:
                # Receiver impl. 1: "Reply false if term < currentTerm".
                # A stale leader: do NOT reset the timer, do NOT adopt it.
                return AppendEntriesReply(
                    self.node_id, args.from_id, self._current_term, False, 0
                )

            # args.term == currentTerm: args.from_id is the legitimate leader
            # of this term. Election Safety says it cannot be us.
            assert self._role != Role.LEADER, (
                f"two leaders in term {self._current_term}: {self.node_id} and {args.from_id}"
            )
            if self._role == Role.CANDIDATE:
                # Candidates: "If AppendEntries RPC received from new leader:
                # convert to follower".
                self._transition(
                    self._current_term, Role.FOLLOWER, f"AppendEntries from leader {args.from_id}"
                )
            self._leader_id = args.from_id
            # Followers: the timer is reset on "receiving AppendEntries RPC
            # from current leader" -- and only from a current-term leader,
            # which the term check above has established.
            self._reset_election_timer()

            # Receiver impl. 2: "Reply false if log doesn't contain an entry
            # at prevLogIndex whose term matches prevLogTerm". Our log is
            # empty, so the only entry that "exists" is the sentinel at 0.
            ok = args.prev_index == 0 and args.prev_term == 0
            return AppendEntriesReply(self.node_id, args.from_id, self._current_term, ok, 0)

    # --- outgoing RPCs (worker threads; the node lock is NOT held) -----------

    def _submit(self, fn, peer, args):
        pool = self._pool
        if pool is None or self._stop.is_set():
            return
        try:
            pool.submit(fn, peer, args)
        except RuntimeError:  # pool shut down between the check and the submit
            pass

    def _send_vote_request(self, peer: str, args: RequestVoteArgs):
        try:
            reply = self._transport.request_vote(peer, args)
        except TransportError as e:
            log.debug("[%s] RequestVote to %s lost: %s", self.node_id, peer, e)
            return
        except Exception:
            log.exception("[%s] RequestVote to %s crashed", self.node_id, peer)
            return
        self._on_vote_reply(peer, args.term, reply)

    def _on_vote_reply(self, peer: str, sent_term: int, reply: RequestVoteReply):
        with self._lock:
            # All Servers rule, on the reply path too.
            if self._step_down_if_stale(reply.term, f"RequestVote reply from {peer}"):
                return
            # The RPC was in flight with the lock released: by now we may no
            # longer be a candidate, or be a candidate of a LATER term.
            if self._role != Role.CANDIDATE or self._current_term != sent_term:
                return
            if reply.vote_granted:
                self._votes.add(peer)
                if len(self._votes) >= self._majority:
                    self._become_leader()

    def _send_heartbeat(self, peer: str, args: AppendEntriesArgs):
        try:
            reply = self._transport.append_entries(peer, args)
        except TransportError as e:
            log.debug("[%s] AppendEntries to %s lost: %s", self.node_id, peer, e)
            return
        except Exception:
            log.exception("[%s] AppendEntries to %s crashed", self.node_id, peer)
            return
        finally:
            with self._lock:
                self._hb_in_flight.discard(peer)
        with self._lock:
            # The only thing a heartbeat reply can teach an election-only
            # leader is that it has been deposed.
            self._step_down_if_stale(reply.term, f"AppendEntries reply from {peer}")
