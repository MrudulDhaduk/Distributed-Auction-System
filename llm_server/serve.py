"""Stands the LLMServicer up as a gRPC server on its own port.

Same shape as server/serve.py, with two deliberate differences:

  - No AuthInterceptor. The session token identifies a bidder, and the
    bidder is not the caller here -- the application server is. See the
    "Who calls this service" note in proto/llm.proto.
  - No AuctionState, no SessionStore, no closer thread. This process holds
    no state at all; it formats a prompt, calls a model, and returns text.
    That is what lets it be restarted at any moment without consequence.
"""

import logging
from concurrent import futures

import grpc

from generated import llm_pb2_grpc
from llm_server import config
from llm_server.llm_servicer import LLMServicer


def serve(port: int = config.PORT):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    # basicConfig(INFO) turns on every library's INFO logger, and httpx logs
    # a line per request -- so each of our own log lines arrived preceded by
    # a "HTTP Request: POST .../api/chat" duplicate of it. Ours carries the
    # request_id and the duration; httpx's carries neither. Raise its floor
    # to WARNING so a genuine transport problem still surfaces.
    logging.getLogger("httpx").setLevel(logging.WARNING)

    # Generation is a slow blocking call, so each in-flight request holds a
    # worker for seconds, not milliseconds. Four matches server/serve.py;
    # it is a small number on purpose -- the bottleneck is the CPU running
    # the model, and queueing beyond that only makes every caller slower.
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    llm_pb2_grpc.add_LLMServiceServicer_to_server(LLMServicer(), server)

    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logging.info("llm server listening on port %d, model %s", port, config.MODEL)
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
