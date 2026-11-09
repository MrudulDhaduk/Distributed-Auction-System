"""End to end over REAL gRPC: three AuctionNodes (server/serve.py) with
FileStorage, ClusterClients with the real GrpcInvoker, and the leader
killed mid-auction.

  * the bidders keep going on the new leader with the SAME token;
  * every acknowledged bid is in the new leader's state, in order;
  * after the kill, the client's first attempt goes to the dead node and
    is answered UNAVAILABLE; it moves on by itself.

Timing: all nodes use 0.3-0.6 s election timeouts except n1, which starts
first with 0.05-0.1 s so it is the first leader. After the kill, n2 or n3
wins; the test does not care which. Bounded waits only; the client's own
give_up_after (30 s) is the outer bound.
"""

import datetime

from client.auction_client import ClusterClient
from common import time_conv
from tests.test_serve_grpc import FIRST, GrpcCluster, grpc_cluster  # noqa: F401 (fixture)
from tests.auction_cluster import wait_until


def test_kill_the_leader_mid_auction_over_grpc(grpc_cluster):  # noqa: F811
    c = grpc_cluster()
    c.boot("n1", **FIRST)
    c.boot("n2")
    c.boot("n3")
    assert wait_until(lambda: c.leader() is not None and c.leader().node_id == "n1", 10)
    addrs = [c.client_addr(n) for n in ("n1", "n2", "n3")]

    seller, karan, mrudul = (ClusterClient(addrs, backoff=(0.05, 0.5)) for _ in range(3))
    try:
        assert seller.login("nisarg", "pass").result.success
        assert karan.login("karan", "pass").result.success
        assert mrudul.login("mrudul", "pass").result.success
        close = time_conv.dt_to_micros(time_conv.now_utc() + datetime.timedelta(hours=1))
        assert seller.create_auction(1, "clock", close).result.success
        assert karan.place_bid(1, 100).result.success
        assert mrudul.place_bid(1, 150).result.success
        tokens = (karan.token, mrudul.token)

        c.kill("n1")

        reply = karan.place_bid(1, 200)
        assert reply.result.success, reply.result.reason
        assert (reply.current_high_bid, reply.current_high_bidder) == (200, "karan")
        assert (karan.token, mrudul.token) == tokens  # nobody logged in again

        auction = mrudul.get_auction(1)
        assert [(b.bidder, b.amount) for b in auction.bids] == [("karan", 100), ("mrudul", 150), ("karan", 200)]
        assert c.leader().node_id in ("n2", "n3")
        first_after_kill = next(a for a, m, r in karan.sent if m == "PlaceBid" and r.amount == 200)
        assert first_after_kill == c.client_addr("n1")  # it really did hit the dead node first
    finally:
        for client in (seller, karan, mrudul):
            client.close()
