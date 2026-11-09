"""Specifies server/serve.py's AuctionNode over REAL gRPC: real servers, real
sockets on 127.0.0.1, real GrpcTransport between the nodes. In-process
(threads, not subprocesses) so the test can look at every node's state.

  * Two ports per node. CLIENT port: AuthService + AuctionService behind
    the AuthInterceptor. PEER port: RaftService and nothing else, with NO
    interceptor -- Raft RPCs carry no session token and must not need one.
  * A 3-node cluster: a follower redirects with FAILED_PRECONDITION and the
    leader's client address in trailing metadata (checks that metadata set
    before context.abort() really reaches a real client), and writes on
    the leader end up identical on all three nodes.

Ports: picked by binding port 0 and closing. Another process could take
the port in between; AuctionNode raises at once if a bind fails, so that
would be a loud error, not a hang. Windows reserves 50155-50254 on this
machine; the OS never hands those out as ephemeral ports.
"""

import datetime
import os
import socket

import grpc
import pytest

from common import time_conv
from common.wire import TOKEN_METADATA_KEY
from generated import auction_pb2, auction_pb2_grpc, raft_pb2, raft_pb2_grpc
from raft.file_storage import FileStorage
from raft.node import Role
from server.serve import AuctionNode, PeerAddr
from tests.auction_cluster import FakeLLMClient, wait_until

HOST = "127.0.0.1"
FAST = dict(election_timeout=(0.3, 0.6), heartbeat_interval=0.03)
FIRST = dict(election_timeout=(0.05, 0.1), heartbeat_interval=0.03)


def free_port():
    with socket.socket() as s:
        s.bind((HOST, 0))
        return s.getsockname()[1]


class GrpcCluster:
    def __init__(self, data_dir, ids=("n1", "n2", "n3")):
        self.data_dir = data_dir
        self.addrs = {nid: PeerAddr(HOST, free_port(), free_port()) for nid in ids}
        self.nodes = {}

    def boot(self, nid, **config):
        peers = {p: a for p, a in self.addrs.items() if p != nid}
        me = self.addrs[nid]
        node = AuctionNode(
            nid, HOST, me.client_port, me.peer_port, peers,
            storage=FileStorage(os.path.join(self.data_dir, nid)),
            llm_client=FakeLLMClient(),
            propose_timeout=2.0,
            **{**FAST, **config},
        )
        node.start()
        self.nodes[nid] = node
        return node

    def kill(self, nid):
        self.nodes.pop(nid).stop()

    def stop(self):
        for nid in list(self.nodes):
            self.kill(nid)

    def leader(self):
        for node in self.nodes.values():
            if node.raft.status().role == Role.LEADER:
                return node
        return None

    def client_addr(self, nid):
        return f"{HOST}:{self.addrs[nid].client_port}"


@pytest.fixture
def grpc_cluster(data_dir):
    made = []

    def make(ids=("n1", "n2", "n3")):
        c = GrpcCluster(data_dir, ids)
        made.append(c)
        return c

    yield make
    for c in made:
        c.stop()


def stubs(addr):
    channel = grpc.insecure_channel(addr)
    return channel, auction_pb2_grpc.AuthServiceStub(channel), auction_pb2_grpc.AuctionServiceStub(channel)


def test_the_peer_port_needs_no_token_and_the_client_port_does(grpc_cluster):
    c = grpc_cluster(ids=("n1",))
    node = c.boot("n1", **FIRST)
    assert wait_until(lambda: node.raft.status().role == Role.LEADER, 10)  # term >= 1
    peer = grpc.insecure_channel(f"{HOST}:{c.addrs['n1'].peer_port}")
    channel, _, auction = stubs(c.client_addr("n1"))
    try:
        # Raft port, no metadata at all: a real Raft reply. Term 0 is stale,
        # so Raft (not auth) refuses it, and says its own term.
        reply = raft_pb2_grpc.RaftServiceStub(peer).AppendEntries(
            raft_pb2.AppendEntriesRequest(from_id="x", to="n1", term=0), timeout=2)
        assert reply.term >= 1 and not reply.entryAppended

        # Client port, no token: the interceptor refuses.
        with pytest.raises(grpc.RpcError) as e:
            auction.ListAuctions(auction_pb2.ListAuctionsRequest(), timeout=2)
        assert e.value.code() == grpc.StatusCode.UNAUTHENTICATED

        # And the services really are on separate ports.
        with pytest.raises(grpc.RpcError) as e:
            raft_pb2_grpc.RaftServiceStub(channel).AppendEntries(raft_pb2.AppendEntriesRequest(), timeout=2)
        assert e.value.code() in (grpc.StatusCode.UNIMPLEMENTED, grpc.StatusCode.UNAUTHENTICATED)
    finally:
        peer.close()
        channel.close()


def test_three_nodes_over_grpc_redirect_then_replicate(grpc_cluster):
    c = grpc_cluster()
    c.boot("n1", **FIRST)
    c.boot("n2")
    c.boot("n3")
    assert wait_until(lambda: c.leader() is not None and c.leader().node_id == "n1", 10)
    assert wait_until(lambda: c.nodes["n2"].raft.status().leader_id == "n1", 10)

    channel, auth, _ = stubs(c.client_addr("n2"))
    try:
        with pytest.raises(grpc.RpcError) as e:
            auth.Login(auction_pb2.LoginRequest(username="nisarg", password="pass"), timeout=5)
        assert e.value.code() == grpc.StatusCode.FAILED_PRECONDITION
        trailing = dict(e.value.trailing_metadata())
        assert trailing["leader-addr"] == c.client_addr("n1")
    finally:
        channel.close()

    channel, auth, auction = stubs(trailing["leader-addr"])
    try:
        token = auth.Login(auction_pb2.LoginRequest(username="nisarg", password="pass"), timeout=5).token
        md = [(TOKEN_METADATA_KEY, token)]
        close = time_conv.dt_to_micros(time_conv.now_utc() + datetime.timedelta(hours=1))
        created = auction.CreateAuction(
            auction_pb2.CreateAuctionRequest(auction_id=1, item="clock", close_time=close),
            metadata=md, timeout=5)
        assert created.result.success
        bid = auction.PlaceBid(auction_pb2.PlaceBidRequest(auction_id=1, amount=150), metadata=md, timeout=5)
        assert bid.result.success and bid.current_high_bid == 150
    finally:
        channel.close()

    def same_everywhere():
        snaps = [n.state_machine.snapshot() for n in c.nodes.values()]
        return all(s == snaps[0] for s in snaps) and 1 in snaps[0]["auctions"]

    assert wait_until(same_everywhere, 10)
    assert c.nodes["n3"].state_machine.sessions.validate(token) == "nisarg"
