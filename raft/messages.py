"""Plain-Python shapes for the two Raft RPCs, and the Transport interface
the node talks through.

This module is the seam that keeps raft/node.py free of gRPC: the node only
knows these dataclasses and the Transport ABC. raft/wire.py converts them
to and from the protobuf messages in proto/raft.proto; raft/transport.py
carries them (over gRPC, or in memory for tests).

Names follow the Raft paper (Args / Reply) here; the wire messages use the
repo's Request / Response convention. Field names are the handout's, in
snake_case, with `from` -> `from_id` for the reason given in raft.proto.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass(frozen=True)
class RequestVoteArgs:
    from_id: str
    to: str
    term: int
    last_log_index: int
    last_log_term: int


@dataclass(frozen=True)
class RequestVoteReply:
    from_id: str
    to: str
    term: int
    vote_granted: bool


@dataclass(frozen=True)
class AppendEntriesArgs:
    from_id: str
    to: str
    term: int
    prev_index: int
    prev_term: int
    commit_index: int
    # Always () in the election-only chunk (heartbeats). A tuple, not a
    # list, so the frozen dataclass really is immutable.
    entries: tuple = ()


@dataclass(frozen=True)
class AppendEntriesReply:
    from_id: str
    to: str
    term: int
    entry_appended: bool
    match_index: int


class TransportError(Exception):
    """The RPC did not complete: peer down, partitioned, or timed out.

    Raised by every Transport implementation so the node never has to know
    which kind of failure (grpc.RpcError, a simulated drop) it was. The node
    treats it as "no reply" -- Raft tolerates lost messages by design.
    """


class Transport(ABC):
    """How a node sends an RPC to a peer. Blocking; raises TransportError
    on any failure. The node always calls these from worker threads and
    never while holding its lock.
    """

    @abstractmethod
    def request_vote(self, to: str, args: RequestVoteArgs) -> RequestVoteReply:
        ...

    @abstractmethod
    def append_entries(self, to: str, args: AppendEntriesArgs) -> AppendEntriesReply:
        ...
