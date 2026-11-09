"""A 3-node auction cluster in one process, for the integration tests.

Each node is what server/serve.py builds, minus the gRPC servers: a
RaftNode on a shared InMemoryTransport, a StateMachine as its apply_fn, and
a RaftReplicator. Nodes can be crashed and rebooted (same idea as
tests/test_raft_persistence.py's Cluster: a reboot is a brand-new object
that sees only what its storage kept).

Timing policy, as in the Raft tests: one node at a time gets a short
election timeout and the others 30 s, so WHO leads is chosen by the test,
not by a race. Every wait is wait_until(condition, BOUND).
"""

import random
import time

from raft.node import RaftNode, Role
from raft.messages import Transport, TransportError
from raft.storage import MemoryStorage
from raft.transport import InMemoryTransport
from replication.replicator import RaftReplicator
from replication.state_machine import StateMachine
from server.auction_servicer import AuctionServicer
from server.auth_interceptor import AuthInterceptor
from server.auth_servicer import AuthServicer
from sessions.store import SessionStore

FAST = dict(election_timeout=(0.15, 0.30), heartbeat_interval=0.03)
LEADER_FIRST = dict(election_timeout=(0.05, 0.10), heartbeat_interval=0.03)
NEVER = dict(election_timeout=(30.0, 31.0), heartbeat_interval=0.03)
BOUND = 5.0
# How long a handler waits for its entry to be applied. Short, so the
# "outcome unknown" tests do not take long; far above a healthy commit
# (a few ms in memory).
WAIT = 1.0

IDS = ("n1", "n2", "n3")
CLIENT_ADDRS = {nid: f"127.0.0.1:5007{i}" for i, nid in enumerate(IDS, start=1)}


def wait_until(predicate, timeout=BOUND, interval=0.002):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


class LoseReplies(Transport):
    """Wraps the network. While `lose` holds a node id, AppendEntries
    REPLIES to that node are lost after the receiver has handled the
    request. The entry gets replicated, but the sender never learns it, so
    its handler cannot know whether its command committed: the textbook
    "unknown outcome"."""

    def __init__(self, net):
        self._net = net
        self.lose = set()

    def request_vote(self, to, args):
        return self._net.request_vote(to, args)

    def append_entries(self, to, args):
        reply = self._net.append_entries(to, args)
        if args.from_id in self.lose:
            raise TransportError(f"reply {to} -> {args.from_id} lost")
        return reply


class FakeLLMClient:
    """Stands in for server/llm_client.py: records calls, returns a canned
    answer, or raises `error` (a grpc.RpcError = the LLM server is down)."""

    def __init__(self):
        self.calls = []
        self.answer = "a fine specimen"
        self.error = None

    def get_llm_answer(self, request_id, query, context):
        self.calls.append((request_id, query, context))
        if self.error is not None:
            raise self.error
        return self.answer


class Node:
    """One cluster member: everything serve.py wires together, minus gRPC."""

    def __init__(self, nid, raft, sm, replicator, storage):
        self.id, self.raft, self.sm, self.replicator, self.storage = nid, raft, sm, replicator, storage
        self.sessions = SessionStore(sm.sessions, replicator)
        self.auth = AuthServicer(self.sessions)
        self.interceptor = AuthInterceptor(self.sessions, replicator)
        self.llm = FakeLLMClient()
        self.auction = AuctionServicer(sm.auctions, self.llm)

    def snapshot(self):
        return self.sm.snapshot()


class AuctionCluster:
    def __init__(self, boot_storage=None, ids=IDS, wait=WAIT):
        self.net = InMemoryTransport()
        self.lossy = LoseReplies(self.net)
        self.ids = list(ids)
        self.boot_storage = boot_storage or (lambda nid: MemoryStorage())
        self.wait = wait
        self.nodes = {}
        self._boots = 0
        self.extra = {}  # per-node objects a test file adds (servicers ...)

    def boot(self, nid, **config):
        self._boots += 1
        storage = self.boot_storage(nid)
        sm = StateMachine()
        raft = RaftNode(
            nid,
            [p for p in self.ids if p != nid],
            self.lossy,
            rng=random.Random(self._boots),
            storage=storage,
            apply_fn=sm.apply_fn,
            **{**NEVER, **config},
        )
        replicator = RaftReplicator(raft, CLIENT_ADDRS, timeout=self.wait)
        node = Node(nid, raft, sm, replicator, storage)
        self.nodes[nid] = node
        self.net.register(raft)
        self.net.restore(nid)
        raft.start()
        return node

    def start(self, leader="n1"):
        """Boot every node; `leader` campaigns first and wins."""
        for nid in self.ids:
            self.boot(nid, **(LEADER_FIRST if nid == leader else NEVER))
        return self.wait_for_leader(among=[leader])

    def crash(self, nid):
        """kill -9 as the rest of the cluster sees it (see
        test_raft_persistence.Cluster.crash for why the storage is fenced
        under the node lock)."""
        self.net.drop(nid)
        node = self.nodes.pop(nid)
        node.raft.stop()
        with node.raft._lock:
            close = getattr(node.storage, "close", None)
            if close:
                close()
        return node

    def stop(self):
        for nid in list(self.nodes):
            self.crash(nid)

    def make_candidate(self, nid):
        """Give nid a short election timeout from now on, so it campaigns
        next (used after the leader is crashed)."""
        raft = self.nodes[nid].raft
        with raft._lock:
            raft._election_timeout = LEADER_FIRST["election_timeout"]
            raft._reset_election_timer()

    def leader(self, among=None, above_term=0):
        for nid, node in self.nodes.items():
            st = node.raft.status()
            if (among is None or nid in among) and st.role == Role.LEADER and st.term > above_term:
                return node
        return None

    def wait_for_leader(self, among=None, above_term=0):
        assert wait_until(lambda: self.leader(among, above_term) is not None), "no leader in time"
        return self.leader(among, above_term)

    def followers(self):
        lead = self.leader()
        return [n for n in self.nodes.values() if n is not lead]

    def wait_converged(self, nids=None):
        """Every node (or nids) has applied everything the leader has
        committed, and that includes the leader's own last entry."""
        nids = list(nids or self.nodes)

        def done():
            lead = self.leader()
            if lead is None:
                return False
            st = lead.raft.status()
            if st.commit_index < st.last_log_index:
                return False
            return all(self.nodes[n].raft.status().last_applied >= st.commit_index for n in nids)

        assert wait_until(done), {n: self.nodes[n].raft.status() for n in nids}

    def snapshots(self):
        return {nid: node.snapshot() for nid, node in self.nodes.items()}


# --- the server-side stack, called directly (no sockets) ------------------------
#
# call() runs a request through exactly what a gRPC server would run it
# through: the node's AuthInterceptor, then the servicer method. A fake
# context stands in for grpc.ServicerContext: abort() raises RpcAbort,
# carrying whatever trailing metadata the handler set first.

from collections import namedtuple  # noqa: E402

import grpc  # noqa: E402

from common.wire import TOKEN_METADATA_KEY  # noqa: E402

HandlerCallDetails = namedtuple("HandlerCallDetails", ["method", "invocation_metadata"])


class RpcAbort(Exception):
    def __init__(self, code, details, trailing):
        super().__init__(f"{code.name}: {details}")
        self.code, self.details, self.trailing = code, details, dict(trailing)


class FakeContext:
    def __init__(self, metadata):
        self._metadata = tuple(metadata)
        self._trailing = ()

    def invocation_metadata(self):
        return self._metadata

    def set_trailing_metadata(self, metadata):
        self._trailing = tuple(metadata)

    def abort(self, code, details):
        raise RpcAbort(code, details, self._trailing)


def call(node, service, method, request, token=None):
    servicer = {"AuthService": node.auth, "AuctionService": node.auction}[service]
    handler = grpc.unary_unary_rpc_method_handler(getattr(servicer, method))
    metadata = [(TOKEN_METADATA_KEY, token)] if token else []
    details = HandlerCallDetails(f"/auction.{service}/{method}", metadata)
    wrapped = node.interceptor.intercept_service(lambda _: handler, details)
    return wrapped.unary_unary(request, FakeContext(metadata))
