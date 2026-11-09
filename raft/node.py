"""The Raft node core (Raft paper, Ongaro & Ousterhout, Figure 2): leader
election (section 5.2), log replication and repair (5.3), the election
restriction and the commit rule (5.4). Persistent state goes through
raft/storage.py; it is in memory only until step 6.

Why this file looks the way it does -- the invariants first
-----------------------------------------------------------
Election Safety (Figure 3): at most one leader per term. It holds because of
two facts this file enforces:
  (a) a node grants AT MOST ONE vote per term  (votedFor, cleared only when
      currentTerm increases), and
  (b) a candidate needs votes from a MAJORITY, and any two majorities of the
      same cluster overlap in at least one node.
So two different candidates cannot both collect a majority in one term.

Log Matching (Figure 3): if two logs hold an entry with the same index and
term, the logs are identical up to that index. Holds because
  (a) a leader creates at most one entry per index in its term and never
      moves or deletes it (propose() only appends), and
  (b) a follower accepts entries only if it already holds the entry just
      before them (AppendEntries receiver step 2) -- an induction on length.

Leader Completeness (Figure 3): a committed entry is in the log of every
later leader. A commit needs copies on a majority, a win needs votes from a
majority, so some voter holds the entry -- and it votes only for a candidate
whose log is at least as up-to-date as its own (_candidate_log_ok). This
argument needs one more rule to be true: commitment is decided only by
counting replicas of CURRENT-term entries (_advance_commit; Figure 8).

State Machine Safety (Figure 3): no two nodes apply different entries at
one index. Only committed entries are applied (_run_applier), strictly in
index order, and a committed entry never changes (the two properties above).
The receiver asserts that it never deletes a committed entry.

Liveness (a leader eventually emerges) comes from randomised election
timeouts: two nodes rarely time out together, so one usually wins before
anyone else starts.

Threading model
---------------
  * ONE lock (self._lock) guards all mutable node state, including the
    storage object.
  * The lock is NEVER held across an outgoing RPC. Handlers and reply
    processing take it briefly, decide, mutate, release. Outgoing RPCs are
    prepared under the lock as plain argument objects, then sent after it is
    released, in parallel, on a worker pool.
  * Every reply path re-checks term and role after re-acquiring the lock,
    because the world may have moved on while the RPC was in flight.
  * ONE ticker thread owns time: it wakes every TICK_INTERVAL, and under the
    lock checks the election deadline (follower/candidate) or the next
    heartbeat time (leader). It never sends an RPC itself.
  * ONE applier thread hands committed entries to apply_fn, in index order,
    with the lock RELEASED (apply_fn is user code with its own lock).
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
from typing import Any, Callable, Optional, Sequence

from raft.messages import (
    AppendEntriesArgs,
    AppendEntriesReply,
    LogEntry,
    RequestVoteArgs,
    RequestVoteReply,
    Transport,
    TransportError,
)
from raft.proposal import Outcome, Proposal
from raft.storage import MemoryStorage

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
# At most this many entries in one AppendEntries (Figure 2: "may send more
# than one for efficiency"). Bounds the RPC size and how long a follower
# holds its lock for one request. (docs/decisions.md, R7)
MAX_ENTRIES_PER_RPC = 64


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
    last_log_index: int = 0
    last_log_term: int = 0
    commit_index: int = 0
    last_applied: int = 0


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
        storage: Optional[MemoryStorage] = None,
        apply_fn: Optional[Callable[[int, bytes], Any]] = None,
    ):
        """
        observer: called with a RoleChange on every change of (term, role),
            WHILE the node lock is held, so events are delivered in exactly
            the order the changes happened. It must therefore be quick,
            must not block, and must not call back into this node.
        storage: Figure 2's persistent state (currentTerm, votedFor, log[]).
            Default: a fresh, empty MemoryStorage. Every read and write of
            that state goes through it (raft/storage.py explains the seam).
        apply_fn: apply_fn(index, data) -> result. Called exactly once per
            committed entry, in index order, from the applier thread, with
            the node lock NOT held, so it may take its own locks and call
            back into this node. Its return value becomes Proposal.result
            on the leader that proposed the entry. It must be deterministic
            and should not raise (an exception halts this node's applier:
            docs/decisions.md, R3). Default: ignore entries.
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
        self._apply_fn = apply_fn or (lambda index, data: None)

        # Figure 2, "Persistent state on all servers": currentTerm, votedFor,
        # log[]. All three live in storage; see the properties below.
        self._storage = storage if storage is not None else MemoryStorage()

        # Figure 2, "Volatile state on all servers".
        self._commit_index = 0
        self._last_applied = 0

        # Figure 2, "Volatile state on leaders (Reinitialized after
        # election)". Filled in by _become_leader.
        self._next_index: dict = {}
        self._match_index: dict = {}

        # Volatile, not in Figure 2.
        self._role = Role.FOLLOWER
        self._leader_id: Optional[str] = None
        self._votes: set = set()  # who voted for us in the current candidacy
        self._election_deadline = 0.0  # time.monotonic() value
        self._next_heartbeat = 0.0
        self._ae_in_flight: set = set()  # peers with an unanswered AppendEntries
        self._pending: dict = {}  # log index -> Proposal; leader only

        # Cluster of N = peers + self; a majority is more than half.
        self._majority = (len(self._peers) + 1) // 2 + 1

        self._lock = threading.Lock()
        self._applier_wake = threading.Condition(self._lock)
        self._stop = threading.Event()
        self._ticker: Optional[threading.Thread] = None
        self._applier: Optional[threading.Thread] = None
        self._pool: Optional[concurrent.futures.ThreadPoolExecutor] = None

    # --- Figure 2 persistent state, read through storage ---------------------
    # Read-only properties: `self._current_term = x` raises AttributeError,
    # so no line in this file can change the term or vote without going
    # through self._storage.set_term_and_vote (the persistence seam).

    @property
    def _current_term(self) -> int:
        return self._storage.term

    @property
    def _voted_for(self) -> Optional[str]:
        return self._storage.voted_for

    def _last_log(self) -> tuple:
        """(lastLogIndex, lastLogTerm). In the election chunk this was an
        injected hook defaulting to (0, 0) (E12); now it reads the real log,
        so the vote check below uses it with no other change."""
        index = self._storage.last_index()
        return (index, self._storage.term_at(index))

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
        self._applier = threading.Thread(
            target=self._run_applier, name=f"raft-{self.node_id}-applier", daemon=True
        )
        self._ticker.start()
        self._applier.start()

    def stop(self):
        self._stop.set()
        with self._lock:
            self._applier_wake.notify_all()
            self._fail_pending(Outcome.STOPPED)
        for thread in (self._ticker, self._applier):
            if thread is not None and thread is not threading.current_thread():
                thread.join(timeout=5)
        if self._pool is not None:
            self._pool.shutdown(wait=False, cancel_futures=True)

    def status(self) -> NodeStatus:
        with self._lock:
            last_index, last_term = self._last_log()
            return NodeStatus(
                self.node_id, self._current_term, self._role, self._voted_for, self._leader_id,
                last_index, last_term, self._commit_index, self._last_applied,
            )

    def log_entries(self) -> tuple:
        """A copy of log[1..lastLogIndex], for tests and debugging."""
        with self._lock:
            return self._storage.entries_from(1, self._storage.last_index())

    # --- the client-facing entry point ---------------------------------------

    def propose(self, data: bytes) -> Proposal:
        """Figure 2, Leaders: "If command received from client: append entry
        to local log, respond after entry applied to state machine (5.3)".

        Returns at once with a handle. On a non-leader the handle is already
        resolved to NOT_LEADER, with leader_hint for redirecting the client.
        On the leader it resolves later; raft/proposal.py defines exactly
        what each outcome promises.
        """
        if not isinstance(data, bytes):
            raise TypeError(f"propose() takes bytes, not {type(data).__name__}")
        with self._lock:
            if self._stop.is_set():
                p = Proposal(0, self._current_term)
                p._resolve(Outcome.STOPPED)
                return p
            if self._role != Role.LEADER:
                p = Proposal(0, self._current_term, leader_hint=self._leader_id)
                p._resolve(Outcome.NOT_LEADER)
                return p
            self._storage.append([LogEntry(self._current_term, data)])
            index = self._storage.last_index()
            p = Proposal(index, self._current_term, leader_hint=self.node_id)
            self._pending[index] = p
            log.info("[%s] term %d: appended index %d", self.node_id, self._current_term, index)
            self._advance_commit()  # only matters in a cluster of one
            sends = self._prepare_appends(self._peers)
        for peer, args in sends:
            self._submit(self._send_append, peer, args)
        return p

    # --- the clock -----------------------------------------------------------

    def _run_ticker(self):
        while not self._stop.wait(TICK_INTERVAL):
            try:
                self._tick()
            except Exception:
                log.exception("[%s] ticker error", self.node_id)

    def _tick(self):
        now = time.monotonic()
        votes, appends = [], []
        with self._lock:
            if self._role == Role.LEADER:
                # Figure 2, Leaders: "repeat during idle periods to prevent
                # election timeouts (5.2)". The same RPC also carries any
                # entries a peer is missing, so this is also the retry loop
                # for peers whose last RPC was lost (5.3: "retries
                # AppendEntries RPCs indefinitely").
                if now >= self._next_heartbeat:
                    self._next_heartbeat = now + self._heartbeat_interval
                    appends = self._prepare_appends(self._peers)
            elif now >= self._election_deadline:
                # Figure 2, Followers (and Candidates): "If election timeout
                # elapses without receiving AppendEntries RPC from current
                # leader or granting vote to candidate: convert to candidate"
                # / "If election timeout elapses: start new election".
                votes = self._start_election()
        # Lock released: only now do any RPCs leave the node.
        for peer, args in votes:
            self._submit(self._send_vote_request, peer, args)
        for peer, args in appends:
            self._submit(self._send_append, peer, args)

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
            self._storage.set_term_and_vote(term, vote)
            self._votes = set()
        self._role = role
        if role != Role.LEADER:
            self._ae_in_flight.clear()
        if old_role == Role.LEADER and role != Role.LEADER:
            # A leader never runs the election timer, so its deadline is long
            # stale. Arm a fresh one, or the deposed leader would start an
            # election on the very next tick. (docs/decisions.md)
            self._reset_election_timer()
            # An ex-leader can no longer see its entries through to apply,
            # so it cannot keep any promise about them (R4).
            self._fail_pending(Outcome.LEADERSHIP_LOST)
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
        # Figure 2, Volatile state on leaders: nextIndex "initialized to
        # leader last log index + 1", matchIndex "initialized to 0". The
        # leader assumes the best (every follower is up to date) and lets
        # the consistency check correct it.
        last_index = self._storage.last_index()
        self._next_index = {peer: last_index + 1 for peer in self._peers}
        self._match_index = {peer: 0 for peer in self._peers}
        self._next_heartbeat = 0.0

    def _prepare_appends(self, peers) -> list:
        """One AppendEntries per peer in `peers` that has none in flight."""
        out = []
        for peer in peers:
            if peer in self._ae_in_flight:
                continue  # its reply will trigger the next send (_on_append_reply)
            self._ae_in_flight.add(peer)
            out.append((peer, self._append_args_for(peer)))
        return out

    def _append_args_for(self, peer: str) -> AppendEntriesArgs:
        """Figure 2, Leaders: "If last log index >= nextIndex for a follower:
        send AppendEntries RPC with log entries starting at nextIndex". If
        nextIndex is past the end of the log, the same RPC has no entries:
        a heartbeat. prevLogIndex/prevLogTerm name the entry just before
        nextIndex, so the follower can run its consistency check.
        """
        next_index = self._next_index[peer]
        prev_index = next_index - 1
        return AppendEntriesArgs(
            self.node_id,
            peer,
            self._current_term,
            prev_index,
            self._storage.term_at(prev_index),
            self._commit_index,
            self._storage.entries_from(next_index, MAX_ENTRIES_PER_RPC),
        )

    def _advance_commit(self):
        """Figure 2, Leaders: "If there exists an N such that N >
        commitIndex, a majority of matchIndex[i] >= N, and log[N].term ==
        currentTerm: set commitIndex = N (5.3, 5.4)".

        Search downwards from the end of the log; the first N that has a
        majority is the largest one. Stop at the first entry from an older
        term: terms never decrease along a log, so every N below it is older
        too, and Raft "never commits log entries from previous terms by
        counting replicas" (5.4.2, Figure 8). Those entries become committed
        indirectly, when a current-term entry after them is.
        """
        for n in range(self._storage.last_index(), self._commit_index, -1):
            if self._storage.term_at(n) != self._current_term:
                break
            copies = 1 + sum(1 for p in self._peers if self._match_index[p] >= n)  # 1: the leader
            if copies >= self._majority:
                self._set_commit_index(n)
                break

    def _set_commit_index(self, n: int):
        """commitIndex "increases monotonically" (Figure 2): never lowered."""
        if n > self._commit_index:
            log.info("[%s] term %d: commitIndex %d -> %d", self.node_id, self._current_term, self._commit_index, n)
            self._commit_index = n
            self._applier_wake.notify()

    def _fail_pending(self, outcome: Outcome):
        for proposal in self._pending.values():
            proposal._resolve(outcome)
        self._pending.clear()

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
                self._storage.set_term_and_vote(self._current_term, args.from_id)
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
        """Figure 2, "AppendEntries RPC", receiver implementation, steps 1-5."""
        with self._lock:
            self._step_down_if_stale(args.term, f"AppendEntries from {args.from_id}")

            if args.term < self._current_term:
                # Step 1: "Reply false if term < currentTerm".
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

            # Step 2: "Reply false if log doesn't contain an entry at
            # prevLogIndex whose term matches prevLogTerm". Index 0 is the
            # sentinel (term 0) that every log has, so prevLogIndex = 0
            # always passes.
            if (
                args.prev_index > self._storage.last_index()
                or self._storage.term_at(args.prev_index) != args.prev_term
            ):
                log.debug(
                    "[%s] refuse AppendEntries from %s: no entry (index %d, term %d)",
                    self.node_id, args.from_id, args.prev_index, args.prev_term,
                )
                return AppendEntriesReply(
                    self.node_id, args.from_id, self._current_term, False, 0
                )

            # Steps 3 and 4.
            self._merge_entries(args.prev_index, args.entries)

            # Our log now matches the leader's up to here -- and not
            # necessarily beyond (we may hold stale entries after it).
            last_new = args.prev_index + len(args.entries)

            # Step 5: "If leaderCommit > commitIndex, set commitIndex =
            # min(leaderCommit, index of last new entry)". For a delayed RPC
            # the min can be BELOW our commitIndex; _set_commit_index never
            # lowers it ("increases monotonically").
            if args.commit_index > self._commit_index:
                self._set_commit_index(min(args.commit_index, last_new))

            return AppendEntriesReply(
                self.node_id, args.from_id, self._current_term, True, last_new
            )

    def _merge_entries(self, prev_index: int, entries: Sequence[LogEntry]):
        """Receiver steps 3 and 4:
          3. "If an existing entry conflicts with a new one (same index but
             different terms), delete the existing entry and all that follow it"
          4. "Append any new entries not already in the log"

        Walk the new entries alongside our log. An entry we already hold
        with the same term is skipped: same index and same term means the
        same entry (Log Matching), so there is nothing to do -- and nothing
        after it is deleted. Only a real conflict truncates. That is what
        makes a duplicated or delayed AppendEntries harmless (5.5: the
        follower "ignores those entries in the new request").
        """
        for offset, entry in enumerate(entries):
            index = prev_index + 1 + offset
            if index <= self._storage.last_index():
                if self._storage.term_at(index) == entry.term:
                    continue  # already have it
                # Step 3: a real conflict.
                assert index > self._commit_index, (
                    f"[{self.node_id}] would delete committed entry {index}"
                    f" (commitIndex {self._commit_index}): State Machine Safety broken"
                )
                log.info(
                    "[%s] conflict at index %d (have term %d, leader has %d): deleting %d..%d",
                    self.node_id, index, self._storage.term_at(index), entry.term,
                    index, self._storage.last_index(),
                )
                self._storage.truncate_from(index)
            # Step 4: everything from this entry on is new to us.
            self._storage.append(entries[offset:])
            log.info(
                "[%s] appended %d..%d from leader", self.node_id, index, prev_index + len(entries)
            )
            return

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

    def _send_append(self, peer: str, args: AppendEntriesArgs):
        try:
            reply = self._transport.append_entries(peer, args)
        except TransportError as e:
            log.debug("[%s] AppendEntries to %s lost: %s", self.node_id, peer, e)
            reply = None
        except Exception:
            log.exception("[%s] AppendEntries to %s crashed", self.node_id, peer)
            reply = None
        with self._lock:
            # Only a reply from THIS term clears the flag: a straggler from an
            # earlier leadership must not clear the flag of a newer RPC.
            if args.term == self._current_term:
                self._ae_in_flight.discard(peer)
            if reply is None:
                return  # the next heartbeat tick retries
            follow_up = self._on_append_reply(peer, args, reply)
        for p, a in follow_up:
            self._submit(self._send_append, p, a)

    def _on_append_reply(self, peer: str, args: AppendEntriesArgs, reply: AppendEntriesReply) -> list:
        """The leader's half of 5.3. Returns the next RPC for this peer, if
        it is still behind."""
        # All Servers rule, on the reply path too.
        if self._step_down_if_stale(reply.term, f"AppendEntries reply from {peer}"):
            return []
        if self._role != Role.LEADER or self._current_term != args.term:
            return []  # we were deposed (or re-elected) while it was in flight

        if reply.entry_appended:
            # Figure 2, Leaders: "If successful: update nextIndex and
            # matchIndex for follower (5.3)". The follower's log matches
            # ours up to the last entry WE sent -- computed from the
            # request, so a delayed success cannot claim more than it proved.
            match = args.prev_index + len(args.entries)
            if match > self._match_index[peer]:  # "increases monotonically"
                self._match_index[peer] = match
                self._advance_commit()
            self._next_index[peer] = self._match_index[peer] + 1
        elif self._next_index[peer] == args.prev_index + 1 and self._next_index[peer] > 1:
            # Figure 2, Leaders: "If AppendEntries fails because of log
            # inconsistency: decrement nextIndex and retry (5.3)". reply.term
            # equals ours (a higher one was handled above), so the refusal
            # was step 2, the consistency check. Only a refusal of the probe
            # we are currently making moves nextIndex: a delayed refusal of
            # an older probe must not decrement it a second time.
            self._next_index[peer] -= 1
            log.info(
                "[%s] %s has no (index %d, term %d): nextIndex -> %d",
                self.node_id, peer, args.prev_index, args.prev_term, self._next_index[peer],
            )

        # "...and retry": keep going while the peer is behind, without
        # waiting for the next heartbeat.
        if self._next_index[peer] <= self._storage.last_index():
            return self._prepare_appends([peer])
        return []

    # --- applying committed entries (the applier thread) ---------------------

    def _run_applier(self):
        """Figure 2, All Servers: "If commitIndex > lastApplied: increment
        lastApplied, apply log[lastApplied] to state machine (5.3)".

        One thread does all applying, so apply_fn sees entries strictly in
        index order and each exactly once: only this thread moves
        lastApplied, by one, after each apply_fn call returns. The entries
        are copied out under the lock and handed over with the lock
        RELEASED. Copying is safe because committed entries never change.
        """
        while True:
            with self._lock:
                while not self._stop.is_set() and self._commit_index <= self._last_applied:
                    self._applier_wake.wait()
                if self._stop.is_set():
                    return
                first = self._last_applied + 1
                batch = self._storage.entries_from(first, self._commit_index - self._last_applied)
            for index, entry in enumerate(batch, start=first):
                try:
                    result = self._apply_fn(index, entry.data)  # no lock held
                except Exception:
                    # Skipping the entry would make this replica differ from
                    # the others; retrying might apply half of it twice.
                    # Fail-stop instead: this node applies nothing more.
                    log.critical(
                        "[%s] apply_fn raised at index %d; applier halted",
                        self.node_id, index, exc_info=True,
                    )
                    return
                with self._lock:
                    self._last_applied = index
                    self._resolve_applied(index, entry.term, result)
                if self._stop.is_set():
                    return

    def _resolve_applied(self, index: int, term: int, result: Any):
        """Resolve the leader's handle for `index`, if it has one. Success
        needs the SAME (index, term) the handle was created for: by Log
        Matching that is the very entry that was proposed."""
        proposal = self._pending.pop(index, None)
        if proposal is None:
            return
        if proposal.term == term:
            proposal._resolve(Outcome.APPLIED, result)
        else:
            proposal._resolve(Outcome.OVERWRITTEN)
