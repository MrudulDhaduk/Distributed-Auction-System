"""A canned-answer LLM server, for the chaos run (scripts/chaos.py --fake-llm).

The real model is a 4-billion-parameter network on the CPU. A 10-minute chaos
run closes dozens of auctions, so the real thing would spend the run's whole
CPU budget on summaries, and Raft's election timeouts (0.5-1.0 s) share that
CPU: a stall would look like a consensus fault. This server answers instantly
with a fixed sentence built from the request, so that what the chaos run tests
is the distributed behaviour around the LLM -- the leader-only summary worker,
RecordSummary through the log, a summary after a failover -- not the model.

It is the same gRPC service as llm_server/serve.py (same proto, same port
shape), so the nodes cannot tell the difference. It is NOT evidence that the
real model works under chaos; that is shown separately by the demo run.
"""

import logging
from concurrent import futures

import grpc

from generated import llm_pb2, llm_pb2_grpc


class FakeLLMServicer(llm_pb2_grpc.LLMServiceServicer):
    def AnswerBiddingQuestion(self, request, context):
        return llm_pb2.LLMAnswerResponse(request_id=request.request_id, answer="(fake LLM) an answer.")

    def GenerateItemDescription(self, request, context):
        return llm_pb2.LLMAnswerResponse(
            request_id=request.request_id, answer=f"(fake LLM) a description of {request.item}.")

    def SummariseAuctionResult(self, request, context):
        a = request.auction
        return llm_pb2.LLMAnswerResponse(
            request_id=request.request_id,
            answer=f"(fake LLM) auction {a.auction_id}: {a.bid_count} bids, high bid {a.current_high_bid}.")

    def SummarisePriceTrends(self, request, context):
        return llm_pb2.LLMAnswerResponse(
            request_id=request.request_id, answer=f"(fake LLM) {len(request.auctions)} closed auctions.")


def serve(port: int):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    llm_pb2_grpc.add_LLMServiceServicer_to_server(FakeLLMServicer(), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logging.info("FAKE llm server listening on port %d", port)
    server.wait_for_termination()
