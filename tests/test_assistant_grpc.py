"""The LLM wired into the cluster, over REAL gRPC all the way down (chunk 7,
step 2): clients -> three AuctionNodes (server/serve.py, FileStorage) ->
a real LLM gRPC server (llm_server/llm_servicer.py, the real prompts and
error mapping) -> a fake Ollama.

Only the model itself is faked. So these tests exercise the real LLMClient,
the real deadline, the real prompt assembly, and the real leader-only read of
the auction state that ends up in the prompt.

  * the FAQ's system turn contains the LIVE auction (item, high bid, open);
  * the closer closes the auction, the summary worker records a summary,
    and any client can read it as Auction.summary;
  * the summary is asked for once, by the leader, and is on every replica;
  * the LLM server hung or dead: a clear answer within the deadline, and
    bidding, creating and closing carry on (R9).

Timing: election timeouts 0.3-0.6 s; the summary worker's interval is 1 s.
Every wait is a bounded wait_until, so a slow machine is slower, not red.
"""

import datetime
import threading
import time
from concurrent import futures

import grpc
import pytest

from client.auction_client import ClusterClient
from common import time_conv
from generated import llm_pb2, llm_pb2_grpc
from llm_server.llm_servicer import LLMServicer
from server.llm_client import LLMClient
from tests.auction_cluster import wait_until
from tests.test_llm_servicer import FakeOllama
from tests.test_serve_grpc import FIRST, grpc_cluster  # noqa: F401 (fixture)


@pytest.fixture
def llm_server():
    """The real LLMServicer behind a real gRPC server, with a fake Ollama.
    Yields (address, fake_ollama)."""
    fake = FakeOllama(answer="Canned model answer.")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    llm_pb2_grpc.add_LLMServiceServicer_to_server(LLMServicer(client=fake), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    yield f"127.0.0.1:{port}", fake
    server.stop(grace=None).wait()


def three_nodes(grpc_cluster, llm_address, deadline=10.0):  # noqa: F811
    c = grpc_cluster()
    for nid in ("n1", "n2", "n3"):
        c.boot(nid, llm_client=LLMClient(llm_address, deadline=deadline), **(FIRST if nid == "n1" else {}))
    assert wait_until(lambda: c.leader() is not None and c.leader().node_id == "n1", 10)
    return c, [c.client_addr(n) for n in ("n1", "n2", "n3")]


def login(addrs, user):
    client = ClusterClient(addrs, backoff=(0.05, 0.5))
    assert client.login(user, "pass").result.success
    return client


def test_the_faq_prompt_carries_the_live_auction_the_leader_read(grpc_cluster, llm_server):  # noqa: F811
    address, ollama = llm_server
    c, addrs = three_nodes(grpc_cluster, address)
    seller, karan, tanmay = (login(addrs, u) for u in ("nisarg", "karan", "tanmay"))
    try:
        close = time_conv.dt_to_micros(time_conv.now_utc() + datetime.timedelta(hours=1))
        assert seller.create_auction(1, "vintage clock", close).result.success
        assert karan.place_bid(1, 150).result.success

        # tanmay's client starts at n1 (the leader) but we also prove the redirect path:
        # ask through a client whose first node is a follower.
        asker = ClusterClient(list(reversed(addrs)), backoff=(0.05, 0.5))
        assert asker.login("tanmay", "pass").result.success
        response = asker.ask_assistant("Can I bid one more than the current high bid?", auction_id=1)

        assert response.result.success, response.result.reason
        assert response.answer == "Canned model answer."
        system = next(m["content"] for m in ollama.calls[-1]["messages"] if m["role"] == "system")
        user = [m["content"] for m in ollama.calls[-1]["messages"] if m["role"] == "user"]
        assert "vintage clock" in system and "150" in system and "open" in system   # the live state
        assert user == ["Can I bid one more than the current high bid?"]            # verbatim, user turn only
        asker.close()
    finally:
        for client in (seller, karan, tanmay):
            client.close()


def test_the_closer_closes_and_the_summary_worker_summarises_once_on_every_replica(grpc_cluster, llm_server):  # noqa: F811
    address, ollama = llm_server
    ollama.answer = "mrudul won at 150 after 2 bids."
    c, addrs = three_nodes(grpc_cluster, address)
    seller, karan, mrudul = (login(addrs, u) for u in ("nisarg", "karan", "mrudul"))
    try:
        close = time_conv.dt_to_micros(time_conv.now_utc() + datetime.timedelta(seconds=1.5))
        assert seller.create_auction(1, "clock", close).result.success
        assert karan.place_bid(1, 100).result.success
        assert mrudul.place_bid(1, 150).result.success

        assert wait_until(lambda: mrudul.get_auction(1).summary != "", 30), "no summary appeared"

        assert mrudul.get_auction(1).summary == "mrudul won at 150 after 2 bids."
        assert mrudul.get_auction(1).closed
        # One summary prompt, and it carries the bid history of the CLOSED auction.
        summary_prompts = [call for call in ollama.calls if "result summary of an online auction" in call["messages"][0]["content"]]
        assert len(summary_prompts) == 1
        system = summary_prompts[0]["messages"][0]["content"]
        # the FACTS the LLM server's prompt builder worked out from the bid history the leader sent
        assert "status: closed" in system and "bids placed: 2 (from 2 bidders)" in system
        assert "opening bid: 100 (by karan)" in system and "winner: mrudul, with a winning bid of 150" in system

        def on_every_replica():
            return all(n.state_machine.snapshot()["auctions"][1].get("summaries") == ["mrudul won at 150 after 2 bids."]
                       for n in c.nodes.values())

        assert wait_until(on_every_replica, 10)
        # Give a stray second attempt time to show up, then check there was none.
        time.sleep(2.5)
        assert len([call for call in ollama.calls if "result summary of an online auction" in call["messages"][0]["content"]]) == 1
    finally:
        for client in (seller, karan, mrudul):
            client.close()


def test_a_description_on_request_and_price_trends_through_the_real_llm_server(grpc_cluster, llm_server):  # noqa: F811
    address, ollama = llm_server
    c, addrs = three_nodes(grpc_cluster, address)
    seller = login(addrs, "nisarg")
    try:
        close = time_conv.dt_to_micros(time_conv.now_utc() + datetime.timedelta(hours=1))
        assert seller.create_auction(1, "clock", close).result.success
        ollama.answer = "A 1970s Seiko wall clock."

        described = seller.post(seller.token, "description", {"auction_id": 1, "attributes": {"brand": "Seiko"}})

        assert described.result.success, described.result.reason
        assert seller.get(seller.token, "auction", {"auction_id": 1}).description == "A 1970s Seiko wall clock."
        assert wait_until(lambda: all(
            n.state_machine.snapshot()["auctions"][1]["description"] == "A 1970s Seiko wall clock."
            for n in c.nodes.values()), 10)
        user = next(m["content"] for m in ollama.calls[-1]["messages"] if m["role"] == "user")
        assert "Seiko" in user   # the seller's attribute, in the user turn

        trends = seller.get(seller.token, "price_trends", {})
        assert trends.result.success and trends.stats.auctions == 0
    finally:
        seller.close()


# --- R9: the LLM server is slow, hung or dead -------------------------------------------------


class HungLLM(llm_pb2_grpc.LLMServiceServicer):
    """Accepts every call and never answers (until released)."""

    def __init__(self):
        self.release = threading.Event()

    def _hang(self, request, context):
        self.release.wait(30)
        return llm_pb2.LLMAnswerResponse(request_id=request.request_id, answer="far too late")

    AnswerBiddingQuestion = GenerateItemDescription = SummariseAuctionResult = SummarisePriceTrends = _hang


def test_a_hung_llm_server_gives_a_clear_answer_at_the_deadline_and_bidding_carries_on(grpc_cluster):  # noqa: F811
    hung = HungLLM()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    llm_pb2_grpc.add_LLMServiceServicer_to_server(hung, server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    try:
        c, addrs = three_nodes(grpc_cluster, f"127.0.0.1:{port}", deadline=0.6)
        seller, karan = login(addrs, "nisarg"), login(addrs, "karan")
        try:
            close = time_conv.dt_to_micros(time_conv.now_utc() + datetime.timedelta(hours=1))

            started = time.monotonic()
            created = seller.create_auction(1, "clock", close, {"brand": "Seiko"})   # asks the model for a description
            took = time.monotonic() - started
            assert created.result.success                                    # the auction exists regardless
            assert took < 5.0
            assert seller.get_auction(1).description == ""                   # degraded, not blocked

            started = time.monotonic()
            answer = karan.ask_assistant("Can I bid one more?", auction_id=1)
            took = time.monotonic() - started
            assert not answer.result.success
            assert "did not answer in time" in answer.result.reason and "auction is unaffected" in answer.result.reason
            assert answer.answer == ""
            assert 0.5 < took < 5.0                                           # it waited the deadline, not 30 s

            assert karan.place_bid(1, 150).result.success                     # and bidding never noticed
            trends = karan.price_trends()
            assert trends.result.success                                      # no closed auctions: the model is not asked
        finally:
            for client in (seller, karan):
                client.close()
    finally:
        hung.release.set()
        server.stop(grace=None).wait()


def test_a_dead_llm_server_gives_a_clear_answer_at_once_and_the_auction_still_closes(grpc_cluster):  # noqa: F811
    c, addrs = three_nodes(grpc_cluster, "127.0.0.1:1", deadline=5.0)   # nothing listens on port 1
    seller, karan = login(addrs, "nisarg"), login(addrs, "karan")
    try:
        # Windows takes ~2 s to refuse a connect to a dead localhost port
        # (docs/decisions.md T18), and CreateAuction spends that on its
        # description attempt, so the close has to be well after it.
        close = time_conv.dt_to_micros(time_conv.now_utc() + datetime.timedelta(seconds=8))
        assert seller.create_auction(1, "clock", close, {"brand": "Seiko"}).result.success
        assert karan.place_bid(1, 100).result.success

        started = time.monotonic()
        answer = karan.ask_assistant("anything?", auction_id=1)
        assert time.monotonic() - started < 10.0   # a refused connect, not the 25 s deadline
        assert not answer.result.success and "unavailable" in answer.result.reason

        # The auction closes and has a winner although no summary can be written.
        assert wait_until(lambda: karan.get_auction(1).closed, 30)
        closed = karan.get_auction(1)
        assert closed.winner == "karan" and closed.summary == ""
    finally:
        for client in (seller, karan):
            client.close()
