"""Conversions between raft/messages.py dataclasses and the protobuf
messages in proto/raft.proto. Pure field copying, no decisions.

Shared by both sides of the wire: GrpcTransport (dataclass -> request,
response -> dataclass) and RaftServicer (request -> dataclass, dataclass ->
response). One module, so the two sides cannot drift apart on a field name.
"""

from generated import raft_pb2
from raft.messages import (
    AppendEntriesArgs,
    AppendEntriesReply,
    RequestVoteArgs,
    RequestVoteReply,
)


def vote_args_to_wire(a: RequestVoteArgs) -> raft_pb2.RequestVoteRequest:
    return raft_pb2.RequestVoteRequest(
        from_id=a.from_id,
        to=a.to,
        term=a.term,
        lastLogIndex=a.last_log_index,
        lastLogTerm=a.last_log_term,
    )


def vote_args_from_wire(m: raft_pb2.RequestVoteRequest) -> RequestVoteArgs:
    return RequestVoteArgs(
        from_id=m.from_id,
        to=m.to,
        term=m.term,
        last_log_index=m.lastLogIndex,
        last_log_term=m.lastLogTerm,
    )


def vote_reply_to_wire(r: RequestVoteReply) -> raft_pb2.RequestVoteResponse:
    return raft_pb2.RequestVoteResponse(
        from_id=r.from_id, to=r.to, term=r.term, voteGranted=r.vote_granted
    )


def vote_reply_from_wire(m: raft_pb2.RequestVoteResponse) -> RequestVoteReply:
    return RequestVoteReply(
        from_id=m.from_id, to=m.to, term=m.term, vote_granted=m.voteGranted
    )


def append_args_to_wire(a: AppendEntriesArgs) -> raft_pb2.AppendEntriesRequest:
    # entries stays empty in the election-only chunk; the log-replication
    # step converts LogEntry <-> wire here.
    return raft_pb2.AppendEntriesRequest(
        from_id=a.from_id,
        to=a.to,
        term=a.term,
        prevIndex=a.prev_index,
        prevTerm=a.prev_term,
        commitIndex=a.commit_index,
    )


def append_args_from_wire(m: raft_pb2.AppendEntriesRequest) -> AppendEntriesArgs:
    return AppendEntriesArgs(
        from_id=m.from_id,
        to=m.to,
        term=m.term,
        prev_index=m.prevIndex,
        prev_term=m.prevTerm,
        commit_index=m.commitIndex,
        entries=tuple(m.entries),
    )


def append_reply_to_wire(r: AppendEntriesReply) -> raft_pb2.AppendEntriesResponse:
    return raft_pb2.AppendEntriesResponse(
        from_id=r.from_id,
        to=r.to,
        term=r.term,
        entryAppended=r.entry_appended,
        matchIndex=r.match_index,
    )


def append_reply_from_wire(m: raft_pb2.AppendEntriesResponse) -> AppendEntriesReply:
    return AppendEntriesReply(
        from_id=m.from_id,
        to=m.to,
        term=m.term,
        entry_appended=m.entryAppended,
        match_index=m.matchIndex,
    )
