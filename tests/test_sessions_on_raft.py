"""Specifies replicated sessions (resolves M1->M2 seam 3: "SessionStore is
not replicated. After leader failover, every token is unknown to the new
leader").

Login and Logout are commands in the Raft log. The leader checks the
password, mints the token with secrets.token_urlsafe BEFORE proposing, and
the token rides on the Login command as data. Every node applies it, so
every node's SessionStore.validate() knows it.

Runs the real interceptor + AuthServicer of each node, called directly
(tests/auction_cluster.call), over an in-memory 3-node Raft cluster.
"""

import grpc
import pytest

from generated import auction_pb2
from tests.auction_cluster import CLIENT_ADDRS, AuctionCluster, RpcAbort, call, wait_until


@pytest.fixture
def cluster():
    c = AuctionCluster()
    yield c
    c.stop()


def login(node, user="mrudul", client_id="", seq=0, password="pass"):
    return call(node, "AuthService", "Login", auction_pb2.LoginRequest(
        username=user, password=password, client_id=client_id, seq=seq))


def test_a_login_on_the_leader_is_known_to_every_node(cluster):
    leader = cluster.start("n1")

    response = login(leader)

    assert response.result.success and response.token
    cluster.wait_converged()
    for node in cluster.nodes.values():
        assert node.sessions.validate(response.token) == "mrudul", node.id


def test_a_wrong_password_proposes_nothing(cluster):
    leader = cluster.start("n1")
    before = leader.raft.status().last_log_index

    response = login(leader, password="nope")

    assert not response.result.success and response.token == ""
    assert leader.raft.status().last_log_index == before


def test_a_login_on_a_follower_is_redirected_to_the_leader(cluster):
    cluster.start("n1")
    follower = cluster.nodes["n2"]
    assert wait_until(lambda: follower.raft.status().leader_id == "n1")

    with pytest.raises(RpcAbort) as e:
        login(follower)

    assert e.value.code == grpc.StatusCode.FAILED_PRECONDITION
    assert e.value.trailing == {"leader-id": "n1", "leader-addr": CLIENT_ADDRS["n1"]}


def test_a_retried_login_returns_the_first_token(cluster):
    leader = cluster.start("n1")

    first = login(leader, client_id="c1", seq=1)
    retry = login(leader, client_id="c1", seq=1)

    assert retry.result.success and retry.token == first.token


def test_logout_is_replicated(cluster):
    leader = cluster.start("n1")
    token = login(leader).token

    response = call(leader, "AuthService", "Logout", auction_pb2.LogoutRequest(client_id="c", seq=1), token=token)

    assert response.result.success
    cluster.wait_converged()
    assert all(n.sessions.validate(token) is None for n in cluster.nodes.values())


def test_an_unknown_token_on_a_follower_is_redirected_not_refused(cluster):
    """A follower's applied state can lag the leader's: a token minted a
    moment ago may not have reached it yet. It cannot tell "unknown" from
    "not here yet", so it does not say UNAUTHENTICATED; it sends the
    client to the leader, which can."""
    cluster.start("n1")
    follower = cluster.nodes["n3"]
    assert wait_until(lambda: follower.raft.status().leader_id == "n1")

    with pytest.raises(RpcAbort) as e:
        call(follower, "AuctionService", "ListAuctions", auction_pb2.ListAuctionsRequest(), token="nope")

    assert e.value.code == grpc.StatusCode.FAILED_PRECONDITION


def test_an_unknown_token_on_the_leader_is_unauthenticated(cluster):
    leader = cluster.start("n1")

    with pytest.raises(RpcAbort) as e:
        call(leader, "AuctionService", "ListAuctions", auction_pb2.ListAuctionsRequest(), token="nope")

    assert e.value.code == grpc.StatusCode.UNAUTHENTICATED


def test_no_token_at_all_is_unauthenticated_everywhere(cluster):
    cluster.start("n1")
    for node in cluster.nodes.values():
        with pytest.raises(RpcAbort) as e:
            call(node, "AuctionService", "ListAuctions", auction_pb2.ListAuctionsRequest())
        assert e.value.code == grpc.StatusCode.UNAUTHENTICATED, node.id


def test_login_with_an_unknown_outcome_is_unavailable(cluster):
    leader = cluster.start("n1")
    cluster.net.partition(["n1"], ["n2", "n3"])

    with pytest.raises(RpcAbort) as e:
        login(leader, client_id="c1", seq=1)

    assert e.value.code == grpc.StatusCode.UNAVAILABLE
    assert "retry" in e.value.details
