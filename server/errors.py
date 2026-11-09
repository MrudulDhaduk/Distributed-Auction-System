"""Raft outcomes -> gRPC status codes, in one place (auction.proto,
design note 3).

    NotLeaderError       FAILED_PRECONDITION, plus trailing metadata
                         leader-id / leader-addr when the leader is known.
                         Nothing was done; the client retries at leader-addr.
    OutcomeUnknownError  UNAVAILABLE "outcome unknown, retry". The command
                         may or may not have committed; the client retries,
                         on any node, with the SAME (client_id, seq).

Why these two codes. FAILED_PRECONDITION is "the system is not in a state
required for the operation" (gRPC's status-code guide): this node is not
the leader, and the client must change something (where it sends) before
retrying. UNAVAILABLE is the code gRPC itself describes as "most likely a
transient condition, which can be corrected by retrying"; it is also what a
client sees when a node is down. So the client handles "node dead" and
"outcome unknown" in one branch: retry elsewhere, same seq.
"""

import contextlib

import grpc

from common.wire import LEADER_ADDR_METADATA_KEY, LEADER_ID_METADATA_KEY
from replication.replicator import NotLeaderError, OutcomeUnknownError


def abort_not_leader(context, e: NotLeaderError):
    if e.leader_id is not None:
        trailing = [(LEADER_ID_METADATA_KEY, e.leader_id)]
        if e.leader_addr is not None:
            trailing.append((LEADER_ADDR_METADATA_KEY, e.leader_addr))
        context.set_trailing_metadata(trailing)
    context.abort(grpc.StatusCode.FAILED_PRECONDITION, str(e))


def abort_outcome_unknown(context, e: OutcomeUnknownError):
    context.abort(
        grpc.StatusCode.UNAVAILABLE,
        f"outcome unknown, retry with the same (client_id, seq): {e}",
    )


@contextlib.contextmanager
def raft_errors(context):
    """`with raft_errors(context): ...` maps the two exceptions above.
    context.abort() raises, so nothing after the with-block runs on error."""
    try:
        yield
    except NotLeaderError as e:
        abort_not_leader(context, e)
    except OutcomeUnknownError as e:
        abort_outcome_unknown(context, e)
