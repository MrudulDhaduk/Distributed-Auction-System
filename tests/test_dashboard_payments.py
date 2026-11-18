"""The dashboard's Pay / Release / Refund endpoint (dashboard/server.py
escrow_action): the friendly pre-checks, and that the cluster's own answer
is passed through. No cluster: the ClusterClient is a fake."""

import collections
import threading

import pytest

from dashboard.server import Dashboard
from dashboard.validate import Invalid


class Result:
    def __init__(self, success, reason):
        self.success, self.reason = success, reason


class Reply:
    def __init__(self, success=True, reason="ok"):
        self.result = Result(success, reason)


class FakeClient:
    def __init__(self, reply=None):
        self.calls = []
        self.reply = reply or Reply()

    def pay(self, auction_id, amount):
        self.calls.append(("pay", auction_id, amount))
        return self.reply

    def release_escrow(self, auction_id):
        self.calls.append(("release", auction_id))
        return self.reply

    def refund_escrow(self, auction_id):
        self.calls.append(("refund", auction_id))
        return self.reply


class FakeSession:
    def __init__(self, username, client):
        self.username, self.client, self.lock = username, client, threading.Lock()


def dash(user, auction, reply=None):
    d = Dashboard.__new__(Dashboard)  # no cluster, no threads
    d._lock = threading.Lock()
    d.events = collections.deque(maxlen=50)
    d._event_seq = 0
    client = FakeClient(reply)
    d.sessions = {"sid": FakeSession(user, client)}
    d.auctions = [auction]
    return d, client


def closed(winner="karan", owner="nisarg", high=150, closed=True):
    return {"auction_id": 7, "owner": owner, "winner": winner, "high_bid": high, "closed": closed}


def test_the_winner_pays_the_winning_amount():
    d, client = dash("karan", closed())
    assert d.escrow_action("sid", "pay", {"auction_id": 7})["ok"]
    assert client.calls == [("pay", 7, 150)]


@pytest.mark.parametrize("user,action,auction,error", [
    ("mrudul", "pay", closed(), "Only the winner can pay"),
    ("karan", "release", closed(), "Only the seller can release or refund"),
    ("karan", "refund", closed(), "Only the seller can release or refund"),
    ("karan", "pay", closed(closed=False), "has not closed yet"),
])
def test_the_wrong_person_or_moment_is_refused_before_the_cluster(user, action, auction, error):
    d, client = dash(user, auction)
    with pytest.raises(Invalid, match=error):
        d.escrow_action("sid", action, {"auction_id": 7})
    assert client.calls == []


@pytest.mark.parametrize("action", ["release", "refund"])
def test_the_seller_settles(action):
    d, client = dash("nisarg", closed())
    assert d.escrow_action("sid", action, {"auction_id": 7})["ok"]
    assert client.calls == [(action, 7)]


def test_the_clusters_refusal_is_shown():
    d, _ = dash("nisarg", closed(), reply=Reply(False, "Escrow is NONE, only HELD money can be settled"))
    with pytest.raises(Invalid, match="only HELD money"):
        d.escrow_action("sid", "release", {"auction_id": 7})


@pytest.mark.parametrize("payload", [{}, {"auction_id": "7"}, {"auction_id": True}, {"auction_id": 0}, []])
def test_a_bad_auction_id_is_refused(payload):
    d, client = dash("karan", closed())
    with pytest.raises(Invalid):
        d.escrow_action("sid", "pay", payload)
    assert client.calls == []


def test_without_a_session_nothing_happens():
    d, client = dash("karan", closed())
    with pytest.raises(PermissionError):
        d.escrow_action("nope", "pay", {"auction_id": 7})
