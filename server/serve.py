"""Wires AuthServicer + AuctionServicer + AuthInterceptor into one gRPC
server. Same shape as ping_server.py, extended to two services and an
interceptor.
"""

from concurrent import futures

import grpc

from auction.state import AuctionState
from generated import auction_pb2_grpc
from sessions.store import SessionStore

from server.auction_servicer import AuctionServicer
from server.auth_interceptor import AuthInterceptor
from server.auth_servicer import AuthServicer
from server.closer import AuctionCloser
from server.llm_client import LLMClient


def serve(port: int = 50051):
    sessions = SessionStore()
    state = AuctionState()
    closer = AuctionCloser(state)
    llm_client = LLMClient()

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=4),
        interceptors=[AuthInterceptor(sessions)],
    )
    auction_pb2_grpc.add_AuthServiceServicer_to_server(AuthServicer(sessions), server)
    auction_pb2_grpc.add_AuctionServiceServicer_to_server(
        AuctionServicer(state, llm_client), server
    )

    server.add_insecure_port(f"[::]:{port}")
    server.start()
    closer.start()
    print(f"auction server listening on port {port}")
    try:
        server.wait_for_termination()
    finally:
        closer.stop()


if __name__ == "__main__":
    serve()
