"""The thin layers around the Raft node: wire conversion, the gRPC servicer
adapter, and the runner's argument parsing. No sockets: the servicer's
methods are called directly with protobuf messages.
"""

import random

import pytest

from generated import raft_pb2
from raft import wire
from raft.messages import (
    AppendEntriesArgs,
    AppendEntriesReply,
    EntryKind,
    LogEntry,
    RequestVoteArgs,
    RequestVoteReply,
)
from raft.node import RaftNode, Role
from raft.run_node import parse_peers
from raft.servicer import RaftServicer
from raft.transport import InMemoryTransport


def test_wire_round_trips_every_message():
    va = RequestVoteArgs("n1", "n2", 7, 3, 2)
    vr = RequestVoteReply("n2", "n1", 7, True)
    aa = AppendEntriesArgs("n1", "n2", 7, 0, 0, 0, ())
    ar = AppendEntriesReply("n2", "n1", 7, True, 0)

    assert wire.vote_args_from_wire(wire.vote_args_to_wire(va)) == va
    assert wire.vote_reply_from_wire(wire.vote_reply_to_wire(vr)) == vr
    assert wire.append_args_from_wire(wire.append_args_to_wire(aa)) == aa
    assert wire.append_reply_from_wire(wire.append_reply_to_wire(ar)) == ar


def test_wire_round_trips_entries_as_opaque_bytes():
    # Arbitrary bytes, including ones that are not valid UTF-8: the Raft
    # layer must carry them without interpreting them.
    es = (LogEntry(1, b"\x00\xff raw"), LogEntry(3, b""), LogEntry(3, "bid".encode()))
    aa = AppendEntriesArgs("n1", "n2", 3, 4, 1, 2, es)

    m = wire.append_args_to_wire(aa)
    assert [(e.term, e.command) for e in m.entries] == [(1, b"\x00\xff raw"), (3, b""), (3, b"bid")]
    back = wire.append_args_from_wire(raft_pb2.AppendEntriesRequest.FromString(m.SerializeToString()))
    assert back == aa
    assert all(isinstance(e, LogEntry) for e in back.entries)


def test_wire_carries_the_entry_kind():
    es = (LogEntry(4, b"", EntryKind.NOOP), LogEntry(4, b"", EntryKind.COMMAND))
    m = wire.append_args_to_wire(AppendEntriesArgs("n1", "n2", 4, 0, 0, 0, es))
    assert [e.kind for e in m.entries] == [raft_pb2.NOOP, raft_pb2.COMMAND]
    back = wire.append_args_from_wire(raft_pb2.AppendEntriesRequest.FromString(m.SerializeToString()))
    # An empty COMMAND stays a command: the kind, not the payload, decides.
    assert [e.kind for e in back.entries] == [EntryKind.NOOP, EntryKind.COMMAND]


def test_wire_uses_the_handouts_field_names():
    m = wire.vote_args_to_wire(RequestVoteArgs("n1", "n2", 7, 3, 2))
    assert (m.from_id, m.to, m.term, m.lastLogIndex, m.lastLogTerm) == ("n1", "n2", 7, 3, 2)
    r = wire.append_reply_to_wire(AppendEntriesReply("n2", "n1", 7, True, 4))
    assert (r.entryAppended, r.matchIndex) == (True, 4)


def test_servicer_delegates_to_the_node_and_reports_its_decision():
    node = RaftNode("n1", ["n2", "n3"], InMemoryTransport(), rng=random.Random(0))
    servicer = RaftServicer(node)

    granted = servicer.RequestVote(
        raft_pb2.RequestVoteRequest(from_id="n2", to="n1", term=4, lastLogIndex=0, lastLogTerm=0),
        None,
    )
    refused = servicer.RequestVote(
        raft_pb2.RequestVoteRequest(from_id="n3", to="n1", term=4, lastLogIndex=0, lastLogTerm=0),
        None,
    )
    beat = servicer.AppendEntries(
        raft_pb2.AppendEntriesRequest(from_id="n2", to="n1", term=4), None
    )
    stale = servicer.AppendEntries(
        raft_pb2.AppendEntriesRequest(from_id="n3", to="n1", term=2), None
    )

    assert granted.voteGranted and granted.term == 4 and granted.from_id == "n1"
    assert not refused.voteGranted
    assert beat.entryAppended and beat.term == 4
    assert not stale.entryAppended and stale.term == 4
    assert node.status().role == Role.FOLLOWER and node.status().leader_id == "n2"


def test_parse_peers():
    assert parse_peers("n2=127.0.0.1:1,n3=h:2") == {"n2": "127.0.0.1:1", "n3": "h:2"}
    assert parse_peers("") == {}


@pytest.mark.parametrize("bad", ["n2", "n2=nohostport", "=h:1", "n2=h:1,n2=h:2"])
def test_parse_peers_rejects_malformed_input(bad):
    with pytest.raises(ValueError):
        parse_peers(bad)
