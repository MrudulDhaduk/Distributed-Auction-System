import grpc

from generated import ping_pb2, ping_pb2_grpc


def main():
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = ping_pb2_grpc.PingServiceStub(channel)
        request = ping_pb2.PingRequest(message="hellp from the client")
        response = stub.Ping(request)
        print(f"server said: {response.message}")
        print(f"served by:   {response.served_by}")


if __name__ == "__main__":
    main()
