"""Payment and escrow over REAL gRPC, with the real closer: the chunk-7 step-1
failover test as a client would see it.

Three AuctionNodes (server/serve.py, FileStorage), ClusterClients on the real
GrpcInvoker. The auction closes by itself (the leader's closer thread
proposes CloseAuction); the winner pays through post(token, "payment", ...);
the leader is killed; the new leader shows the same escrow, accepts the
seller's release, and every surviving replica ends with the same record.

Timing: election timeouts 0.3-0.6 s (n1 first, 0.05-0.1 s). The auction
closes ~1.5 s after creation; every wait is a bounded wait_until, so a slow
machine makes the test slower, not red. The client's give_up_after (30 s) is
the outer bound for the calls after the kill.
"""

import datetime

from client.auction_client import ClusterClient
from common import time_conv
from tests.auction_cluster import wait_until
from tests.test_serve_grpc import FIRST, grpc_cluster  # noqa: F401 (fixture)


def test_pay_kill_the_leader_the_new_leader_has_the_same_escrow(grpc_cluster):  # noqa: F811
    c = grpc_cluster()
    c.boot("n1", **FIRST)
    c.boot("n2")
    c.boot("n3")
    assert wait_until(lambda: c.leader() is not None and c.leader().node_id == "n1", 10)
    addrs = [c.client_addr(n) for n in ("n1", "n2", "n3")]

    seller, karan, mrudul = (ClusterClient(addrs, backoff=(0.05, 0.5)) for _ in range(3))
    try:
        for client, user in ((seller, "nisarg"), (karan, "karan"), (mrudul, "mrudul")):
            assert client.login(user, "pass").result.success
        close = time_conv.dt_to_micros(time_conv.now_utc() + datetime.timedelta(seconds=1.5))
        assert seller.post(seller.token, "auction", {"auction_id": 1, "item": "clock", "close_time": close}).result.success
        assert karan.post(karan.token, "bid", {"auction_id": 1, "amount": 100}).result.success
        assert mrudul.post(mrudul.token, "bid", {"auction_id": 1, "amount": 150}).result.success

        # The closer, not the test, closes the auction.
        assert wait_until(lambda: mrudul.get(mrudul.token, "auction", {"auction_id": 1}).closed, 20)

        # Only the winner pays, and only the winning bid.
        wrong = karan.post(karan.token, "payment", {"auction_id": 1, "action": "pay", "amount": 150})
        assert not wrong.result.success and "only the winner" in wrong.result.reason
        paid = mrudul.post(mrudul.token, "payment", {"auction_id": 1, "action": "pay", "amount": 150})
        assert paid.result.success, paid.result.reason
        before = mrudul.get(mrudul.token, "escrow", {"auction_id": 1}).escrow
        assert (before.status, before.amount, before.payer, before.seller) == ("HELD", 150, "mrudul", "nisarg")

        killed = c.leader().node_id
        c.kill(killed)

        # Same tokens, no re-login: the first attempt hits the dead node.
        after = seller.get(seller.token, "escrow", {"auction_id": 1}).escrow
        assert after == before
        assert c.leader().node_id != killed

        released = seller.post(seller.token, "payment", {"auction_id": 1, "action": "release"})
        assert released.result.success, released.result.reason
        assert mrudul.get(mrudul.token, "escrow", {"auction_id": 1}).escrow.status == "RELEASED"

        def same_escrow_everywhere():
            records = [n.state_machine.snapshot()["auctions"][1].get("escrow") for n in c.nodes.values()]
            return len(records) == 2 and records[0] == records[1] and records[0]["status"] == "RELEASED"

        assert wait_until(same_escrow_everywhere, 10)
    finally:
        for client in (seller, karan, mrudul):
            client.close()
