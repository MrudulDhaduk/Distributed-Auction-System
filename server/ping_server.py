from concurrent import futures

import grpc

from generated import ping_pb2, ping_pb2_grpc


class PingServicer(ping_pb2_grpc.PingServiceServicer):
    def __init__(self, node_id: str):
        self.node_id = node_id

    def Ping(self, request, context):
        return ping_pb2.PingResponse(
            message=f"pong: {request.message}",
            served_by=self.node_id,
        )


def serve(port: int = 50051, node_id: str = "node-1"):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    ping_pb2_grpc.add_PingServiceServicer_to_server(PingServicer(node_id), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"{node_id} listening on port {port}")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
