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
            return {nid: stub.GetNodeStatus(status_pb2.NodeStatusRequest(include_digest=True), timeout=2)
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
        s = stub.GetNodeStatus(status_pb2.NodeStatusRequest(include_digest=True), timeout=2)
        assert s.role in ("FOLLOWER", "CANDIDATE")
        assert s.leader_id == ""
        assert (s.commit_index, s.last_applied, s.digest_index) == (0, 0, 0)
        assert len(s.state_digest) == 64  # the empty state has a digest too
    finally:
        channel.close()


# --- cost: the status RPC must not throttle the node it observes ---------------
#
# Found in the failover stress run: every GetNodeStatus deep-copied the whole
# state under the state-machine lock (and recomputed the digest whenever the
# index had moved). Polled every 10 ms against ~18 000 bids, that blocked the
# applier: commits fell to one every ~0.4 s and status calls timed out.


class FakeRaft:
    node_id = "n1"

    def status(self):
        from raft.node import NodeStatus, Role
        return NodeStatus("n1", 1, Role.LEADER, "n1", "n1", 0, 0, 0, 0)


def fed_state_machine(n):
    from auction.state import CreateAuction, PlaceBid
    from replication import codec
    from replication.state_machine import StateMachine
    import datetime as _dt
    t = _dt.datetime(2030, 1, 1, tzinfo=_dt.timezone.utc)
    sm = StateMachine()
    sm.apply_fn(1, codec.encode(CreateAuction(1, "x", t + _dt.timedelta(days=1), "o", "")))
    for i in range(2, n + 2):
        sm.apply_fn(i, codec.encode(PlaceBid(1, "b", i, t)))
    return sm


class CountingSM:
    """Wraps a StateMachine and counts full snapshots."""

    def __init__(self, sm):
        self.sm, self.snapshots = sm, 0

    def indexed_snapshot(self):
        self.snapshots += 1
        return self.sm.indexed_snapshot()

    def last_index(self):
        return self.sm.last_index()


def test_an_idle_node_is_not_copied_on_every_poll():
    from server.status_servicer import StatusServicer
    sm = CountingSM(fed_state_machine(10))
    svc = StatusServicer(FakeRaft(), sm)
    for _ in range(20):
        svc.GetNodeStatus(None, None)
    assert sm.snapshots == 1


def test_a_busy_node_recomputes_the_digest_at_most_once_per_interval():
    from replication import codec
    from auction.state import PlaceBid
    from server.status_servicer import StatusServicer
    import datetime as _dt
    now = [100.0]
    sm = CountingSM(fed_state_machine(10))
    svc = StatusServicer(FakeRaft(), sm, min_interval=0.5, clock=lambda: now[0])
    first = svc.GetNodeStatus(None, None)
    t = _dt.datetime(2030, 1, 1, tzinfo=_dt.timezone.utc)
    for i in range(20, 40):  # the index moves on every poll
        sm.sm.apply_fn(i, codec.encode(PlaceBid(1, "b", 1000 + i, t)))
        now[0] += 0.01
        s = svc.GetNodeStatus(None, None)
        assert s.digest_index == first.digest_index  # honest label of an older state
    assert sm.snapshots == 1
    now[0] += 0.5
    assert svc.GetNodeStatus(None, None).digest_index == 39
    assert sm.snapshots == 2


def test_dump_state_is_always_fresh():
    from server.status_servicer import StatusServicer
    from replication import codec
    from auction.state import PlaceBid
    import datetime as _dt
    now = [0.0]
    sm = fed_state_machine(3)
    svc = StatusServicer(FakeRaft(), sm, min_interval=60, clock=lambda: now[0])
    svc.GetNodeStatus(None, None)
    sm.apply_fn(50, codec.encode(PlaceBid(1, "b", 999, _dt.datetime(2030, 1, 1, tzinfo=_dt.timezone.utc))))
    d = svc.DumpState(None, None)
    assert d.digest_index == 50
    assert svc.GetNodeStatus(None, None).digest_index == 50  # the fresh dump refreshed the cache


def test_last_index_is_cheap_and_matches_indexed_snapshot():
    sm = fed_state_machine(5)
    assert sm.last_index() == sm.indexed_snapshot()[0] == 6


def test_without_include_digest_no_snapshot_is_taken_at_all():
    """Second finding of the failover stress run: at 25 000 bids one
    snapshot holds the state-machine lock ~190 ms. A poller that only needs
    roles and indexes must not pay that, ever."""
    from server.status_servicer import StatusServicer
    sm = CountingSM(fed_state_machine(10))
    svc = StatusServicer(FakeRaft(), sm)
    for _ in range(5):
        s = svc.GetNodeStatus(status_pb2.NodeStatusRequest(include_digest=False), None)
        assert s.state_digest == "" and s.digest_index == 11
    assert sm.snapshots == 0
    assert svc.GetNodeStatus(status_pb2.NodeStatusRequest(include_digest=True), None).state_digest
    assert sm.snapshots == 1


def test_the_poller_can_skip_digests():
    from harness.status import StatusPoller
    assert StatusPoller({}, include_digest=False).include_digest is False
    assert StatusPoller({}).include_digest is True
