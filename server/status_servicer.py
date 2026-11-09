"""StatusService (proto/status.proto): one node's local view, read-only.

    GetNodeStatus  Raft's NodeStatus (term, role, leader, commitIndex,
                   lastApplied, last log index/term) + the state digest.
    DumpState      the canonical JSON of the applied state (debug, checker).

On the PEER port, no interceptor (docs/decisions.md, T2).

The two halves of GetNodeStatus are read one after the other, not under one
lock: raft.status() under the node lock, then the state machine's
indexed_snapshot() under its own. So last_applied and digest_index can come
from two slightly different moments, and digest_index can even be a little
AHEAD of last_applied (the applier calls apply_fn, then takes the node lock
to bump lastApplied). That is fine for a dashboard; digest_index is the
honest label of which state the digest covers. The checker only compares
dumps taken after the cluster has gone quiet.

Cost: a digest is a deep copy + JSON + sha256 of the whole state. It is
cached by digest_index -- the state only changes when apply_fn runs, and
apply_fn moves the index -- so a dashboard polling an idle node five times
a second recomputes nothing.
"""

import threading

from generated import status_pb2, status_pb2_grpc
from replication import digest


class StatusServicer(status_pb2_grpc.StatusServiceServicer):
    def __init__(self, raft, state_machine):
        self._raft = raft
        self._sm = state_machine
        self._lock = threading.Lock()
        self._cached = None  # (digest_index, canonical_json, digest)

    def _current(self) -> tuple:
        index, snapshot = self._sm.indexed_snapshot()
        with self._lock:
            if self._cached is not None and self._cached[0] == index:
                return self._cached
        text = digest.canonical_json(snapshot)
        entry = (index, text, digest.digest_of_json(text))
        with self._lock:
            self._cached = entry
        return entry

    def GetNodeStatus(self, request, context):
        s = self._raft.status()
        index, _, state_digest = self._current()
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
        index, text, state_digest = self._current()
        return status_pb2.StateDump(
            node_id=self._raft.node_id, digest_index=index, state_digest=state_digest, state_json=text,
        )
