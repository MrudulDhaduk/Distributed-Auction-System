"""CLI client driving one end-to-end flow: login, create an auction, place
a bid, then read it back. Same channel/stub shape as ping_client.py, but two
services and a token that has to travel from step to step.

Token placement mirrors server/auth_interceptor.py: every call except Login
carries it as gRPC metadata under TOKEN_METADATA_KEY, never as a message
field.
"""

import datetime

import grpc

from generated import auction_pb2, auction_pb2_grpc
from server import time_conv
from server.auth_interceptor import TOKEN_METADATA_KEY


def _auth_metadata(token):
    # builds the metadata list gRPC expects: [(key, value), ...]
    return [(TOKEN_METADATA_KEY, token)]


def do_login(stub, username, password):
    # calls AuthService.Login; returns the session token (or "" on failure)
    response = stub.Login(
        auction_pb2.LoginRequest(username=username, password=password)
    )
    print(f"login: {response.result.success} - {response.result.reason}")
    return response.token


def do_create_auction(stub, token, auction_id, item, close_time):
    # calls AuctionService.CreateAuction; returns the CreateAuctionResponse
    return stub.CreateAuction(
        auction_pb2.CreateAuctionRequest(
            auction_id=auction_id,
            item=item,
            close_time=close_time,
        ),
        metadata=_auth_metadata(token),
    )


def do_place_bid(stub, token, auction_id, amount):
    # calls AuctionService.PlaceBid; returns the PlaceBidResponse
    return stub.PlaceBid(
        auction_pb2.PlaceBidRequest(
            auction_id=auction_id,
            amount=amount,
        ),
        metadata=_auth_metadata(token),
    )


def do_get_auction(stub, token, auction_id):
    # calls AuctionService.GetAuction; returns the Auction (or None)
    response = stub.GetAuction(
        auction_pb2.GetAuctionRequest(
            auction_id=auction_id,
        ),
        metadata=_auth_metadata(token),
    )
    if not response.result.success:
        print(f"get auction: {response.result.reason}")
        return None
    return response.auction


def main():
    with grpc.insecure_channel("localhost:50051") as channel:
        auth_stub = auction_pb2_grpc.AuthServiceStub(channel)
        auction_stub = auction_pb2_grpc.AuctionServiceStub(channel)

        token = do_login(auth_stub, "mrudul", "pass")
        print(f"login token: {token}")

        close_time = time_conv.dt_to_micros(
            time_conv.now_utc() + datetime.timedelta(hours=1)
        )

        create_response = do_create_auction(
            auction_stub, token, 1, "vintage clock", close_time
        )
        print(f"create auction: {create_response}")

        bid_response = do_place_bid(auction_stub, token, 1, 100)
        print(f"place bid: {bid_response}")

        auction = do_get_auction(auction_stub, token, 1)
        print(f"auction: {auction}")


if __name__ == "__main__":
    main()
