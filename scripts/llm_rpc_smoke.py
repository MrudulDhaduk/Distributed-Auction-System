"""Manual check: call each LLMService RPC once against a running server.

Task 4, step 3. Where scripts/llm_smoke.py asks "can Python reach Ollama at
all", this asks the next question up the stack: does the same call work
through gRPC, with a real prompt, against llm_server/serve.py.

Not a test and deliberately not in tests/ -- it needs a live model and a
live server, and it has no assertions worth the name. It answers "do the
three answers read sensibly, and how slow are they", which is a judgement
you make by reading the output. tests/test_llm_servicer.py is the part that
runs in CI, with a fake client and no network.

Run it in two terminals:

    .venv\\Scripts\\python.exe -m llm_server.serve
    .venv\\Scripts\\python.exe scripts\\llm_rpc_smoke.py

The first call of the process pays a one-time cost to load the weights, so
read its timing as cold-start, not as latency. See docs/design-notes.md for
the warm numbers.
"""

import datetime
import sys
import time
import uuid
from pathlib import Path

import grpc

# Running `python scripts/llm_rpc_smoke.py` puts scripts/ on sys.path, not the
# repo root, so `from generated import ...` would not resolve. llm_smoke.py
# needs no such line because it imports nothing from this repo.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common import time_conv  # noqa: E402 -- must follow the sys.path line
from generated import llm_pb2, llm_pb2_grpc
from llm_server import config

CLOSE = datetime.datetime(2026, 9, 28, 14, 0, 0, tzinfo=datetime.timezone.utc)
BID_AT = datetime.datetime(2026, 9, 28, 13, 0, 0, tzinfo=datetime.timezone.utc)


def auction(closed, high, count):
    return llm_pb2.AuctionContext(
        auction_id=7,
        item="a signed first printing of the Raft paper",
        close_time=time_conv.dt_to_micros(CLOSE),
        closed=closed,
        current_high_bid=high,
        bid_count=count,
    )


def timed(label, fn):
    rid = str(uuid.uuid4())
    start = time.perf_counter()
    try:
        response = fn(rid)
    except grpc.RpcError as e:
        print(f"\n=== {label} ===\nFAILED {e.code().name}: {e.details()}")
        return
    elapsed = time.perf_counter() - start
    ok = "echoed" if response.request_id == rid else "MISMATCH"
    print(f"\n=== {label} ===")
    print(f"request_id : {rid}  ({ok})")
    print(f"elapsed    : {elapsed:.2f}s")
    print(f"answer     : {response.answer}")


def main():
    channel = grpc.insecure_channel(f"localhost:{config.PORT}")
    grpc.channel_ready_future(channel).result(timeout=10)
    stub = llm_pb2_grpc.LLMServiceStub(channel)

    timed(
        "(a) AnswerBiddingQuestion",
        lambda rid: stub.AnswerBiddingQuestion(
            llm_pb2.BiddingQuestionRequest(
                request_id=rid,
                question="I was outbid. Can I just bid one more than the current high?",
                auction=auction(closed=False, high=4500, count=12),
            )
        ),
    )

    timed(
        "(b) GenerateItemDescription",
        lambda rid: stub.GenerateItemDescription(
            llm_pb2.ItemDescriptionRequest(
                request_id=rid,
                item="1987 Fender Stratocaster",
                attributes=[
                    llm_pb2.Attribute(key="condition", value="excellent, light fret wear"),
                    llm_pb2.Attribute(key="colour", value="sunburst"),
                    llm_pb2.Attribute(key="case", value="original hard case included"),
                    llm_pb2.Attribute(key="origin", value="made in USA"),
                ],
            )
        ),
    )

    timed(
        "(c) SummariseAuctionResult",
        lambda rid: stub.SummariseAuctionResult(
            llm_pb2.AuctionSummaryRequest(
                request_id=rid,
                auction=auction(closed=True, high=5200, count=4),
                bid_history=[
                    llm_pb2.BidPoint(
                        bidder=b,
                        amount=a,
                        time=time_conv.dt_to_micros(
                            BID_AT + datetime.timedelta(minutes=m)
                        ),
                    )
                    for b, a, m in [
                        ("mrudul", 3000, 0),
                        ("nisarg", 4000, 12),
                        ("karan", 4500, 25),
                        ("nisarg", 5200, 41),
                    ]
                ],
            )
        ),
    )


if __name__ == "__main__":
    main()
