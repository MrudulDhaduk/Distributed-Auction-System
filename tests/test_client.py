"""Specifies client/auction_client.ClusterClient: one logical client of the
whole cluster.

  * It is given every node's client address. It sends to the node it last
    succeeded with.
  * FAILED_PRECONDITION with a leader-addr hint: go there at once.
    No hint (an election is on): wait, then try the next node.
  * UNAVAILABLE / DEADLINE_EXCEEDED: wait (exponential backoff), then try
    the next node.
  * A write keeps the SAME (client_id, seq) through all its retries, so a
    retry after an unknown outcome is never executed twice (section 8).
  * Anything else (UNAUTHENTICATED, ...) is raised, not retried.

The real client logic, over the in-memory cluster: the invoker turns a
call into tests/auction_cluster.call on the right node, and a crashed
node's address answers UNAVAILABLE.
"""

import datetime

import grpc
import pytest

from client.auction_client import ClusterClient, ClusterUnavailable
from common import time_conv
from tests.auction_cluster import CLIENT_ADDRS, AuctionCluster, in_memory_invoker, wait_until

ADDRS = [CLIENT_ADDRS[n] for n in ("n1", "n2", "n3")]
CLOSE = time_conv.dt_to_micros(time_conv.now_utc() + datetime.timedelta(hours=1))


@pytest.fixture
def cluster():
    c = AuctionCluster()
    yield c
    c.stop()


def client_for(cluster, start_at=None, **kw):
    addrs = ADDRS if start_at is None else [start_at] + [a for a in ADDRS if a != start_at]
    return ClusterClient(addrs, invoker=in_memory_invoker(cluster), backoff=(0.01, 0.1), **kw)


def test_a_client_that_starts_at_a_follower_follows_the_hint(cluster):
    cluster.start("n1")
    assert wait_until(lambda: cluster.nodes["n3"].raft.status().leader_id == "n1")
    seller = client_for(cluster, start_at=CLIENT_ADDRS["n3"])

    assert seller.login("nisarg", "pass").result.success
    assert seller.create_auction(1, "clock", CLOSE).result.success

    assert seller.invoker.calls[0] == (CLIENT_ADDRS["n3"], "Login")
    assert seller.invoker.calls[1] == (CLIENT_ADDRS["n1"], "Login")
    assert seller.invoker.calls[-1] == (CLIENT_ADDRS["n1"], "CreateAuction")  # remembered


def test_kill_the_leader_mid_auction_same_token_no_acknowledged_bid_lost(cluster):
    cluster.start("n1")
    seller, karan, mrudul = (client_for(cluster) for _ in range(3))
    seller.login("nisarg", "pass")
    karan.login("karan", "pass")
    mrudul.login("mrudul", "pass")
    seller.create_auction(1, "clock", CLOSE)
    acknowledged = []
    for client, amount in ((karan, 100), (mrudul, 150)):
        assert client.place_bid(1, amount).result.success
        acknowledged.append(amount)
    karan_token = karan.token

    cluster.crash("n1")
    cluster.make_candidate("n2")  # the test picks who campaigns; the client does not know

    reply = karan.place_bid(1, 200)  # n1 refuses the connection; the client moves on

    assert reply.result.success, reply.result.reason
    acknowledged.append(200)
    assert karan.token == karan_token  # no re-login
    auction = mrudul.get_auction(1)
    assert [b.amount for b in auction.bids] == acknowledged
    assert cluster.leader().id == "n2"


def test_a_write_retried_after_an_unknown_outcome_happens_once(cluster):
    """The bid reaches n2 and n3 but n1 never hears their replies: n1
    answers UNAVAILABLE. The client retries with the SAME seq. n1 is then
    killed and n2 elected; the retry is answered from the dedupe table."""
    cluster.start("n1")
    seller, karan = client_for(cluster), client_for(cluster)
    seller.login("nisarg", "pass")
    karan.login("karan", "pass")
    seller.create_auction(1, "clock", CLOSE)
    cluster.wait_converged()

    cluster.lossy.lose.add("n1")
    real = karan.invoker

    def invoke(addr, service, method, request, metadata, timeout):
        try:
            return real(addr, service, method, request, metadata, timeout)
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE and "n1" in cluster.nodes:
                cluster.crash("n1")
                cluster.make_candidate("n2")
            raise

    karan.invoker = invoke
    reply = karan.place_bid(1, 100)

    assert reply.result.success, reply.result.reason
    seqs = [r.seq for a, m, r in karan.sent if m == "PlaceBid"]
    assert len(seqs) >= 2 and len(set(seqs)) == 1  # retried, same serial number
    assert [b.amount for b in karan.get_auction(1).bids] == [100]


def test_unauthenticated_is_raised_not_retried(cluster):
    cluster.start("n1")
    c = client_for(cluster)
    c.token = "not-a-token"

    with pytest.raises(grpc.RpcError) as e:
        c.list_auctions()

    assert e.value.code() == grpc.StatusCode.UNAUTHENTICATED
    assert len(c.invoker.calls) == 1


def test_no_reachable_node_gives_up_with_cluster_unavailable(cluster):
    cluster.start("n1")
    c = client_for(cluster, give_up_after=0.3)
    cluster.stop()

    with pytest.raises(ClusterUnavailable):
        c.login("nisarg", "pass")
    assert {a for a, _ in c.invoker.calls} == set(ADDRS)  # it tried every node


def test_each_write_gets_a_new_serial_number_reads_get_none(cluster):
    cluster.start("n1")
    c = client_for(cluster)
    c.login("nisarg", "pass")
    c.create_auction(1, "clock", CLOSE)
    c.place_bid(1, 10)
    c.get_auction(1)

    writes = [(m, r.seq) for a, m, r in c.sent if m in ("Login", "CreateAuction", "PlaceBid")]
    assert [s for _, s in writes] == [1, 2, 3]
    assert all(r.client_id == c.client_id for a, m, r in c.sent if m in ("Login", "CreateAuction", "PlaceBid"))
