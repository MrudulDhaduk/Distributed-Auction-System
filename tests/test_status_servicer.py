"""Specifies server/status_servicer.py: GetNodeStatus and DumpState, over
real gRPC on a real AuctionNode's PEER port, with no token.

Uses the 3-node GrpcCluster from test_serve_grpc (FileStorage, real ports).
Waits are bounded wait_until()s; "all three digests equal" is waited for,
not assumed, because followers learn the commit index one heartbeat late.
"""

import datetime
import hashlib
import json

import grpc

from client.auction_client import ClusterClient
from common import time_conv
from generated import status_pb2, status_pb2_grpc
from tests.auction_cluster import wait_until
from tests.test_serve_grpc import FIRST, HOST, grpc_cluster  # noqa: F401 (fixture)

IDS = ("n1", "n2", "n3")


def status_stub(c, nid):
    channel = grpc.insecure_channel(f"{HOST}:{c.addrs[nid].peer_port}")
    return channel, status_pb2_grpc.StatusServiceStub(channel)


def test_status_and_dump_over_the_peer_port(grpc_cluster):  # noqa: F811
    c = grpc_cluster()
    c.boot("n1", **FIRST)
    c.boot("n2")
    c.boot("n3")
    assert wait_until(lambda: c.leader() is not None, 10)
    seller = ClusterClient([c.client_addr(n) for n in IDS])
    stubs = {nid: status_stub(c, nid) for nid in IDS}
    try:
        assert seller.login("nisarg", "pass").result.success
        close = time_conv.dt_to_micros(time_conv.now_utc() + datetime.timedelta(hours=1))
        assert seller.create_auction(1, "clock", close).result.success
        assert seller.place_bid(1, 100).result.success

        def statuses():
            return {nid: stub.GetNodeStatus(status_pb2.NodeStatusRequest(), timeout=2)
                    for nid, (_, stub) in stubs.items()}

        # Every node catches up: same digest_index, same digest.
        assert wait_until(lambda: len({(s.digest_index, s.state_digest) for s in statuses().values()}) == 1, 10)
        st = statuses()
        leader = c.leader().node_id
        assert st[leader].role == "LEADER"
        assert all(s.leader_id == leader for s in st.values())
        assert all(s.role == "FOLLOWER" for nid, s in st.items() if nid != leader)
        assert len({s.term for s in st.values()}) == 1
        for nid, s in st.items():
            assert s.node_id == nid
            assert s.commit_index >= s.last_applied >= s.digest_index >= 3
            assert s.last_log_index >= s.commit_index and s.last_log_term == s.term

        dump = stubs["n2"][1].DumpState(status_pb2.DumpStateRequest(), timeout=2)
        assert dump.node_id == "n2"
        assert dump.state_digest == hashlib.sha256(dump.state_json.encode("utf-8")).hexdigest()
        assert dump.state_digest == st["n2"].state_digest
        assert dump.digest_index == st["n2"].digest_index
        doc = json.loads(dump.state_json)
        assert [(b["bidder"], b["amount"]) for b in doc["auctions"][0]["bids"]] == [("nisarg", 100)]
        assert seller.token not in dump.state_json
    finally:
        seller.close()
        for channel, _ in stubs.values():
            channel.close()


def test_status_of_a_lone_node_with_no_leader(grpc_cluster):  # noqa: F811
    c = grpc_cluster()
    c.boot("n2")  # its peers never come up: it can never win
    channel, stub = status_stub(c, "n2")
    try:
        s = stub.GetNodeStatus(status_pb2.NodeStatusRequest(), timeout=2)
        assert s.role in ("FOLLOWER", "CANDIDATE")
        assert s.leader_id == ""
        assert (s.commit_index, s.last_applied, s.digest_index) == (0, 0, 0)
        assert len(s.state_digest) == 64  # the empty state has a digest too
    finally:
        channel.close()
