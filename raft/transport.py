"""The two Transport implementations: GrpcTransport (real sockets) and
InMemoryTransport (tests, and later the chaos harness).

The node (raft/node.py) sees only the Transport ABC from raft/messages.py.
"""

import threading

import grpc

from generated import raft_pb2_grpc
from raft import wire
from raft.messages import (
    AppendEntriesArgs,
    AppendEntriesReply,
    RequestVoteArgs,
    RequestVoteReply,
    Transport,
    TransportError,
)

# Per-RPC deadline. Must stay BELOW the minimum election timeout (0.5 s by
# default) so a vote round's stragglers have all finished or failed before
# the next round can start; and far above a localhost round trip (~1 ms) so
# a busy Python process is not mistaken for a dead one.
DEFAULT_RPC_TIMEOUT = 0.3

# gRPC's default reconnect backoff grows to 120 s. After a peer is killed
# and restarted, the channel to it would sit in TRANSIENT_FAILURE for far
# longer than any election timeout and heartbeats would not resume. Cap it.
_CHANNEL_OPTIONS = [
    ("grpc.initial_reconnect_backoff_ms", 100),
    ("grpc.min_reconnect_backoff_ms", 100),
    ("grpc.max_reconnect_backoff_ms", 500),
]


class GrpcTransport(Transport):
    """peers maps node id -> "host:port"."""

    def __init__(self, peers: dict[str, str], rpc_timeout: float = DEFAULT_RPC_TIMEOUT):
        self._timeout = rpc_timeout
        self._channels = {
            pid: grpc.insecure_channel(addr, options=_CHANNEL_OPTIONS)
            for pid, addr in peers.items()
        }
        self._stubs = {
            pid: raft_pb2_grpc.RaftServiceStub(ch) for pid, ch in self._channels.items()
        }

    def _stub(self, to: str):
        try:
            return self._stubs[to]
        except KeyError:
            raise TransportError(f"unknown peer {to!r}") from None

    def request_vote(self, to: str, args: RequestVoteArgs) -> RequestVoteReply:
        try:
            response = self._stub(to).RequestVote(
                wire.vote_args_to_wire(args), timeout=self._timeout
            )
        except grpc.RpcError as e:
            raise TransportError(f"RequestVote to {to} failed: {e.code()}") from e
        return wire.vote_reply_from_wire(response)

    def append_entries(self, to: str, args: AppendEntriesArgs) -> AppendEntriesReply:
        try:
            response = self._stub(to).AppendEntries(
                wire.append_args_to_wire(args), timeout=self._timeout
            )
        except grpc.RpcError as e:
            raise TransportError(f"AppendEntries to {to} failed: {e.code()}") from e
        return wire.append_reply_from_wire(response)

    def close(self):
        for ch in self._channels.values():
            ch.close()


class InMemoryTransport(Transport):
    """One shared object that all nodes of a test cluster use as their
    Transport. Delivery is a direct, synchronous call into the target
    node's handler on the caller's (worker) thread -- no sockets, no
    serialisation, no delay.

    Failure injection, applied to the sender AND the receiver of every
    call, requests and replies alike:
      drop(id) / restore(id)   node `id` is unreachable in both directions
                               (a crashed machine, as seen by everyone else;
                               the Python object keeps running)
      partition(*groups)       only nodes in the same group can talk. A node
                               not named in any group is isolated.
      heal()                   clears all partitions (not drops)
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._nodes = {}
        self._down = set()
        self._group_of = None  # None = no partition; else {node_id: group_index}

    def register(self, node):
        with self._lock:
            self._nodes[node.node_id] = node

    def drop(self, node_id: str):
        with self._lock:
            self._down.add(node_id)

    def restore(self, node_id: str):
        with self._lock:
            self._down.discard(node_id)

    def partition(self, *groups):
        with self._lock:
            self._group_of = {nid: i for i, g in enumerate(groups) for nid in g}

    def heal(self):
        with self._lock:
            self._group_of = None

    def _route(self, sender: str, to: str):
        with self._lock:
            node = self._nodes.get(to)
            if node is None:
                raise TransportError(f"no node {to!r} on this network")
            if sender in self._down or to in self._down:
                raise TransportError(f"{sender} -> {to}: node down")
            if self._group_of is not None:
                # Unlisted nodes get a unique group id so they reach nobody.
                if self._group_of.get(sender, ("alone", sender)) != self._group_of.get(
                    to, ("alone", to)
                ):
                    raise TransportError(f"{sender} -> {to}: partitioned")
            return node

    def request_vote(self, to: str, args: RequestVoteArgs) -> RequestVoteReply:
        node = self._route(args.from_id, to)
        reply = node.handle_request_vote(args)
        # The reply travels back over the same network: re-check, so a link
        # cut while the request was being handled still loses the reply.
        self._route(args.from_id, to)
        return reply

    def append_entries(self, to: str, args: AppendEntriesArgs) -> AppendEntriesReply:
        node = self._route(args.from_id, to)
        reply = node.handle_append_entries(args)
        self._route(args.from_id, to)
        return reply
