"""Specifies server/summarizer.py (chunk 7, step 2; R6): the result summary of
a closed auction, written by the LLM and recorded through the log.

    closed auction without a summary
        -> the LEADER's worker asks the model (outside any lock, outside
           apply()) -> proposes RecordSummary(auction_id, text)
        -> apply() stores the text as plain data on every node

Properties:

  * IDEMPOTENT. Once an auction has a summary the worker never asks the
    model about it again; and if two writers race, apply() refuses the second
    RecordSummary and the first text stays.
  * A NEW LEADER NOTICES. The worker does not remember anything it has not
    seen in the state: after a failover it scans for closed auctions with no
    summary, so a leader that died between the close and the summary loses
    nothing.
  * A FOLLOWER DOES NOTHING and never calls the model.
  * LLM DOWN OR SLOW: nothing is proposed, the auction is already closed and
    stays closed, and the worker tries again after a pause (not in a hot
    loop against a dead server).

tick() is the whole logic, with the clock passed in (the same arrangement as
server/closer.py), so the pause is tested without sleeping.
"""

import datetime

import grpc
import pytest

from auction.state import AuctionState, CloseAuction, CreateAuction, RecordSummary
from generated import auction_pb2
from raft.messages import EntryKind
from replication import codec
from replication.state_machine import Applied
from server.summarizer import SummaryWorker
from tests.auction_cluster import AuctionCluster, FakeRpcError, call, wait_until
from tests.test_assistant_on_raft import CLOSE, NOW, setup


@pytest.fixture
def cluster():
    c = AuctionCluster()
    yield c
    c.stop()


def summaries(cluster, auction_id=1):
    return {nid: n.snapshot()["auctions"][auction_id].get("summaries", []) for nid, n in cluster.nodes.items()}


def record_summary_commands(node):
    return [c.command for c in (codec.decode(e.data) for e in node.raft.log_entries() if e.kind is EntryKind.COMMAND)
            if isinstance(c.command, RecordSummary)]


# --- the happy path ---------------------------------------------------------------------------


def test_a_closed_auction_gets_one_summary_on_every_node(cluster):
    leader, t = setup(cluster, close=True)
    leader.llm.answer = "mrudul won at 150 after 2 bids."

    recorded = leader.summarizer.tick()

    assert recorded == [1]
    cluster.wait_converged()
    assert set(map(tuple, summaries(cluster).values())) == {("mrudul won at 150 after 2 bids.",)}
    assert [c.summary for c in record_summary_commands(leader)] == ["mrudul won at 150 after 2 bids."]


def test_the_model_gets_the_closed_auction_and_its_bid_history_in_order(cluster):
    leader, t = setup(cluster, close=True)

    leader.summarizer.tick()

    request_id, query, context = leader.llm.calls[0]
    assert query == ""
    assert context["auction"] == {
        "auction_id": 1, "item": "vintage clock", "close_time": context["auction"]["close_time"],
        "closed": True, "current_high_bid": 150, "bid_count": 2,
    }
    assert [(b["bidder"], b["amount"]) for b in context["bid_history"]] == [("karan", 100), ("mrudul", 150)]
    assert all(isinstance(b["time"], int) for b in context["bid_history"])  # epoch micros


def test_an_auction_nobody_bid_on_is_summarised_too(cluster):
    leader, t = setup(cluster, close=True, bids=())

    assert leader.summarizer.tick() == [1]
    assert leader.llm.calls[0][2]["bid_history"] == []
    assert leader.llm.calls[0][2]["auction"]["current_high_bid"] == 0


def test_an_open_auction_is_not_summarised(cluster):
    leader, t = setup(cluster, close=False)

    assert leader.summarizer.tick() == []
    assert leader.llm.calls == []


# --- idempotence -------------------------------------------------------------------------------


def test_a_second_tick_asks_the_model_nothing_and_proposes_nothing(cluster):
    leader, t = setup(cluster, close=True)
    leader.summarizer.tick()
    calls, entries = len(leader.llm.calls), len(leader.raft.log_entries())

    assert leader.summarizer.tick() == []
    assert len(leader.llm.calls) == calls and len(leader.raft.log_entries()) == entries


def test_a_fresh_worker_on_the_same_leader_sees_the_summary_and_does_nothing(cluster):
    leader, t = setup(cluster, close=True)
    leader.summarizer.tick()
    other = SummaryWorker(leader.sm.auctions, leader.llm, leader.replicator)
    calls = len(leader.llm.calls)

    assert other.tick() == []
    assert len(leader.llm.calls) == calls


def test_if_another_writer_wins_the_race_its_text_stays_and_this_one_is_dropped(cluster):
    """Two leaders (or two threads) both generate. apply() takes the first
    RecordSummary and refuses the second; the loser neither errors nor retries."""
    leader, t = setup(cluster, close=True)
    leader.llm.answer = "mine"
    leader.llm.on_call = lambda: leader.replicator.submit(RecordSummary(auction_id=1, summary="theirs"))

    recorded = leader.summarizer.tick()

    assert recorded == []  # ours was refused
    cluster.wait_converged()
    assert set(map(tuple, summaries(cluster).values())) == {("theirs",)}
    leader.llm.on_call = None
    assert leader.summarizer.tick() == []  # and it is not retried


# --- who may run it ----------------------------------------------------------------------------


def test_a_follower_does_nothing_and_never_calls_the_model(cluster):
    leader, t = setup(cluster, close=True)

    assert cluster.nodes["n2"].summarizer.tick() == []
    assert cluster.nodes["n3"].summarizer.tick() == []
    assert cluster.nodes["n2"].llm.calls == [] and cluster.nodes["n3"].llm.calls == []
    assert set(map(tuple, summaries(cluster).values())) == {()}


def test_a_new_leader_notices_a_closed_auction_the_old_leader_never_summarised(cluster):
    """The leader closes the auction (CloseAuction is in the log) and dies
    before its worker gets to the summary."""
    leader, t = setup(cluster, close=True)
    cluster.wait_converged()
    cluster.crash("n1")
    cluster.make_candidate("n2")
    new_leader = cluster.wait_for_leader(among=["n2"])
    new_leader.llm.answer = "summarised by the new leader"

    assert new_leader.summarizer.tick() == [1]

    assert wait_until(lambda: all(
        n.snapshot()["auctions"][1].get("summaries") == ["summarised by the new leader"]
        for n in cluster.nodes.values()))
    assert len(record_summary_commands(new_leader)) == 1


def test_the_old_leader_that_came_back_as_a_follower_does_not_summarise_again(cluster):
    leader, t = setup(cluster, close=True)
    leader.summarizer.tick()
    cluster.wait_converged()
    cluster.crash("n1")
    cluster.make_candidate("n2")
    new_leader = cluster.wait_for_leader(among=["n2"])
    calls = len(new_leader.llm.calls)

    assert new_leader.summarizer.tick() == []
    assert len(new_leader.llm.calls) == calls


# --- the model is down or slow -------------------------------------------------------------------


def test_with_the_llm_down_nothing_is_proposed_and_the_auction_stays_closed(cluster):
    leader, t = setup(cluster, close=True)
    leader.llm.error = FakeRpcError(grpc.StatusCode.UNAVAILABLE, "down")
    entries = len(leader.raft.log_entries())

    assert leader.summarizer.tick() == []

    assert len(leader.raft.log_entries()) == entries
    assert leader.snapshot()["auctions"][1]["closed"] is True
    assert set(map(tuple, summaries(cluster).values())) == {()}


def test_after_a_failure_the_worker_waits_then_tries_again():
    """The schedule itself, with no cluster: a real AuctionState and stand-ins
    for the replicator and the model. The pause is a number passed to tick(),
    so nothing sleeps."""
    state = AuctionState()
    when = datetime.datetime(2030, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
    state.apply(CreateAuction(auction_id=1, item="x", close_time=when, owner="s", description=""))
    state.apply(CloseAuction(auction_id=1, curr_time=when + datetime.timedelta(hours=1)))

    class Replicator:
        def __init__(self):
            self.submitted = []

        def is_leader(self):
            return True

        def submit(self, command, client_id="", seq=0):
            self.submitted.append(command)
            return Applied(state.apply(command))

    class Llm:
        def __init__(self):
            self.calls = 0
            self.error = FakeRpcError(grpc.StatusCode.UNAVAILABLE, "down")

        def get_llm_answer(self, request_id, query, context):
            self.calls += 1
            if self.error is not None:
                raise self.error
            return request_id, "a summary"

    llm, replicator = Llm(), Replicator()
    worker = SummaryWorker(state, llm, replicator, retry_after_seconds=10.0)

    assert worker.tick(now=100.0) == [] and llm.calls == 1      # fails, remembers when
    assert worker.tick(now=105.0) == [] and llm.calls == 1      # inside the pause: no call
    assert worker.tick(now=109.9) == [] and llm.calls == 1
    llm.error = None
    assert worker.tick(now=110.0) == [1] and llm.calls == 2     # pause over: tries, succeeds
    assert [type(c).__name__ for c in replicator.submitted] == ["RecordSummary"]
    assert worker.tick(now=500.0) == [] and llm.calls == 2       # done for good


def test_one_failing_auction_does_not_block_the_next(cluster):
    """Auction 1's summary fails; auction 2 is summarised in the same tick."""
    leader, t = setup(cluster, close=True, auction_id=1)
    assert call(leader, "AuctionService", "CreateAuction", auction_pb2.CreateAuctionRequest(
        auction_id=2, item="second", close_time=CLOSE), token=t["nisarg"]).result.success
    assert leader.replicator.submit(CloseAuction(auction_id=2, curr_time=NOW + datetime.timedelta(hours=2))).result.success
    cluster.wait_converged()
    leader.llm.on_call = lambda: setattr(
        leader.llm, "error",
        FakeRpcError(grpc.StatusCode.UNAVAILABLE, "down") if leader.llm.calls[-1][2]["auction"]["auction_id"] == 1 else None)

    assert leader.summarizer.tick(now=0.0) == [2]

    cluster.wait_converged()
    assert summaries(cluster, 1)["n1"] == [] and summaries(cluster, 2)["n1"] == ["a fine specimen"]
    leader.llm.on_call = None
    leader.llm.error = None
    assert leader.summarizer.tick(now=1000.0) == [1]    # and auction 1 is retried after its pause


# --- the real poll thread ---------------------------------------------------------------------------


def test_the_poll_thread_summarises_once_on_a_live_cluster(cluster):
    """The real loop, bounded by wait_until: a thread per node, 20 ms ticks.
    Only the leader's thread does anything; the summary appears exactly once."""
    leader, t = setup(cluster, close=True)
    workers = [SummaryWorker(n.sm.auctions, n.llm, n.replicator, interval_seconds=0.02)
               for n in cluster.nodes.values()]
    for w in workers:
        w.start()
    try:
        assert wait_until(lambda: all(len(s) == 1 for s in summaries(cluster).values()))
        cluster.wait_converged()
    finally:
        for w in workers:
            w.stop()

    assert len(record_summary_commands(leader)) == 1
    assert [len(n.llm.calls) for n in cluster.followers()] == [0, 0]
