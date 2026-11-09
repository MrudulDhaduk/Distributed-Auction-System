"""StatusService (proto/status.proto): one node's local view, read-only.

    GetNodeStatus  Raft's NodeStatus (term, role, leader, commitIndex,
                   lastApplied, last log index/term) + the state digest.
    DumpState      the canonical JSON of the applied state (debug, checker).

On the PEER port, no interceptor (docs/decisions.md, T2).

The two halves of GetNodeStatus are read one after the other, not under one
lock: raft.status() under the node lock, then the digest. So last_applied
and digest_index come from slightly different moments. digest_index is the
honest label of which state the digest covers. The checker only compares
dumps taken after the cluster has gone quiet.

COST (docs/decisions.md, T16). A digest is a deep copy of the whole state
UNDER THE STATE-MACHINE LOCK, then JSON and sha256. The first version did
the copy on every poll, so the applier waited behind the observer. In the
failover stress run (polled every 10 ms, ~18 000 bids) commits fell to one
every ~0.4 s. So:
  * the cache check reads only state_machine.last_index() (no copy): an
    idle node costs nothing per poll;
  * a busy node recomputes at most once per `min_interval` seconds. In
    between, GetNodeStatus returns the last digest WITH ITS OWN
    digest_index, so it is an older state, correctly labelled, never a
    wrong one;
  * DumpState always computes fresh (rare, and the checker needs it exact)
    and refreshes the cache;
  * and a caller that does not need the digest (NodeStatusRequest
    include_digest = false: the failover stress poller) pays nothing. The
    throttle alone was not enough: at 25 000 bids one snapshot holds the
    lock ~190 ms, so even twice a second the applier stalled and status
    calls timed out.
"""

import threading
import time

from generated import status_pb2, status_pb2_grpc
from replication import digest

DEFAULT_MIN_INTERVAL = 0.5


class StatusServicer(status_pb2_grpc.StatusServiceServicer):
    def __init__(self, raft, state_machine, *, min_interval=DEFAULT_MIN_INTERVAL, clock=time.monotonic):
        self._raft = raft
        self._sm = state_machine
        self._min_interval = min_interval
        self._clock = clock
        self._lock = threading.Lock()
        self._cached = None  # (digest_index, canonical_json, digest)
        self._computed_at = None

    def _compute(self) -> tuple:
        index, snapshot = self._sm.indexed_snapshot()
        text = digest.canonical_json(snapshot)
        entry = (index, text, digest.digest_of_json(text))
        with self._lock:
            self._cached, self._computed_at = entry, self._clock()
        return entry

    def _current(self) -> tuple:
        with self._lock:
            cached, at = self._cached, self._computed_at
        if cached is not None:
            if cached[0] == self._sm.last_index():
                return cached  # nothing applied since: same state
            if self._clock() - at < self._min_interval:
                return cached  # busy: an older state, labelled as such
        return self._compute()

    def GetNodeStatus(self, request, context):
        s = self._raft.status()
        if request is None or request.include_digest:
            index, _, state_digest = self._current()
        else:  # no copy, no JSON: an int read under the state-machine lock
            index, state_digest = self._sm.last_index(), ""
        return status_pb2.NodeStatus(
            node_id=s.node_id,
            role=s.role.value,
            term=s.term,
            leader_id=s.leader_id or "",
            voted_for=s.voted_for or "",
            commit_index=s.commit_index,
            last_applied=s.last_applied,
            last_log_index=s.last_log_index,
            last_log_term=s.last_log_term,
            state_digest=state_digest,
            digest_index=index,
        )

    def DumpState(self, request, context):
        index, text, state_digest = self._compute()
        return status_pb2.StateDump(
            node_id=self._raft.node_id, digest_index=index, state_digest=state_digest, state_json=text,
        )
