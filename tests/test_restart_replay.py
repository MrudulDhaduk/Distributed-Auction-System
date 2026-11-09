"""Specifies what a full-cluster restart gives back: ALL THREE nodes crash
(nothing in memory survives anywhere), all three reboot from their data
directories, and each rebuilds exactly the auction state, the session
table and the dedupe table it had -- by replaying the log through
apply_fn. Nothing but currentTerm, votedFor and log[] is on disk (P11);
the state machine is a pure function of the log.

What this checks beyond the Raft persistence tests: that the COMMAND
encoding is replay-safe. Times decode to the same microsecond, the LLM
description is in the log as text (not re-generated), the Login token is
in the log (not re-minted), so a token issued before the restart still
works after it, and a retry of a pre-restart request still dedupes.

Runs twice, like test_raft_persistence.py: with FileStorage, and with
MemoryStorage as a strict xfail (a "restart" there starts from an empty
log, and the state must NOT come back -- proof the test can fail).
"""

import datetime

import pytest

from auction.state import CloseAuction, CreateAuction, PlaceBid
from raft.storage import MemoryStorage
from sessions.state import Login, Logout
from tests.auction_cluster import AuctionCluster

UTC = datetime.timezone.utc
CLOSE = datetime.datetime(2030, 1, 1, 12, 0, 0, 654321, tzinfo=UTC)


@pytest.fixture(
    params=[
        "file",
        pytest.param("memory", marks=pytest.mark.xfail(strict=True, reason="no log, no replay")),
    ]
)
def boot_storage(request, open_storage):
    if request.param == "file":
        return open_storage
    return lambda nid: MemoryStorage()


def test_a_full_restart_rebuilds_identical_auction_and_session_state(boot_storage):
    c = AuctionCluster(boot_storage)
    try:
        leader = c.start("n1")
        submit = leader.replicator.submit
        assert submit(Login("tok-nisarg", "nisarg"), "c-nisarg", 1).result.success
        assert submit(Login("tok-karan", "karan"), "c-karan", 1).result.success
        assert submit(Login("tok-tanmay", "tanmay"), "c-tanmay", 1).result.success
        assert submit(CreateAuction(1, "clock", CLOSE, "nisarg", "a 1970s Seiko"), "c-nisarg", 2).result.success
        assert submit(CreateAuction(2, "lamp", CLOSE, "nisarg", ""), "c-nisarg", 3).result.success
        t = CLOSE - datetime.timedelta(minutes=5, microseconds=7)
        assert submit(PlaceBid(1, "karan", 150, t), "c-karan", 2).result.success
        assert not submit(PlaceBid(1, "tanmay", 150, t), "c-tanmay", 2).result.success  # too low
        assert submit(PlaceBid(1, "tanmay", 200, t), "c-tanmay", 3).result.success
        assert submit(CloseAuction(1, CLOSE + datetime.timedelta(seconds=1))).result.success
        assert submit(Logout("tok-tanmay"), "c-tanmay", 4).result.success
        c.wait_converged()
        before = c.snapshots()
        assert before["n1"] == before["n2"] == before["n3"]

        for nid in list(c.nodes):
            c.crash(nid)
        assert c.nodes == {}

        c.start("n2")  # a different leader this time, for good measure
        c.wait_converged()
        after = c.snapshots()

        for nid in ("n1", "n2", "n3"):
            assert after[nid] == before["n1"], nid
        state = after["n3"]
        assert state["auctions"][1]["winner"].bidder == "tanmay"
        assert state["auctions"][1]["description"] == "a 1970s Seiko"
        assert state["auctions"][1]["bids"][0].time_stamp == t  # to the microsecond
        assert state["sessions"] == {"tok-nisarg": "nisarg", "tok-karan": "karan"}

        # A token from before the restart works after it, and a retry of a
        # pre-restart request is still recognised.
        new_leader = c.leader()
        assert new_leader.sm.sessions.validate("tok-karan") == "karan"
        retry = new_leader.replicator.submit(PlaceBid(1, "karan", 150, t), "c-karan", 2)
        assert retry.result.success and retry.high_bid == 150  # the ORIGINAL response
    finally:
        c.stop()
