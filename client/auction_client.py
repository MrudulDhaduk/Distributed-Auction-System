"""Client for the auction CLUSTER, plus the M1 end-to-end CLI flow.

ClusterClient is one logical client: one user's session and one section 8
serial-number sequence. It knows every node's CLIENT address and finds the
leader itself (Raft paper, section 8: "If the client's first choice is not
the leader, that server will reject the client's request and supply
information about the most recent leader it has heard from [...] If the
leader crashes, client requests will time out; clients then try again with
randomly-chosen servers").

How one call is retried (_call), by the status the server returned:
  OK                    done; remember this node for the next call.
  FAILED_PRECONDITION   not the leader. With a leader-addr hint: go there at
                        once. Without one (an election is on), or after a
                        run of hints that lead nowhere: back off, next node.
  UNAVAILABLE           node down, or "outcome unknown". Back off, next node.
  DEADLINE_EXCEEDED     same as UNAVAILABLE: we don't know what happened.
  anything else         raised to the caller (UNAUTHENTICATED is a real "no").
Backoff doubles from backoff[0] to backoff[1]. After give_up_after seconds
the call raises ClusterUnavailable.

A WRITE (Login, Logout, CreateAuction, PlaceBid) takes its serial number
ONCE, before the first attempt, and every retry sends the same
(client_id, seq). That is what makes "retry after an unknown outcome" safe:
if the first attempt did commit, the state machine answers the retry with
the stored response instead of executing it again
(replication/state_machine.py). Reads carry no serial number: they change
nothing.

Token placement mirrors server/auth_interceptor.py: every call except Login
carries it as gRPC metadata under TOKEN_METADATA_KEY, never as a message
field.

One write at a time per ClusterClient: the dedupe table keeps only the
latest seq per client_id (docs/decisions.md, A5), so this class is not
meant to be shared by concurrent threads. One per user, as in main().
"""

import argparse
import datetime
import logging
import time
import uuid

import grpc

from common import time_conv
from common.wire import LEADER_ADDR_METADATA_KEY, TOKEN_METADATA_KEY
from generated import auction_pb2, auction_pb2_grpc

log = logging.getLogger("client")

DEFAULT_NODES = ["127.0.0.1:50071", "127.0.0.1:50072", "127.0.0.1:50073"]

# Per-attempt gRPC deadline. Above the server's commit wait (5 s,
# replication/replicator.py) so the server's explicit UNAVAILABLE arrives
# first. CreateAuction gets more: the LLM call before proposing has been
# measured at up to ~7.4 s on a cold model (docs/design-notes.md).
RPC_TIMEOUT = 8.0
CREATE_TIMEOUT = 20.0

_RETRY_ELSEWHERE = {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED}


class ClusterUnavailable(Exception):
    """No node gave a definite answer within give_up_after seconds. For a
    write the outcome is UNKNOWN; calling the same method again does NOT
    reuse the serial number (it is a new request)."""


class GrpcInvoker:
    """invoker(addr, service, method, request, metadata, timeout) over real
    gRPC: one channel per address, kept open."""

    def __init__(self):
        self._stubs = {}

    def __call__(self, addr, service, method, request, metadata, timeout):
        if addr not in self._stubs:
            channel = grpc.insecure_channel(addr)
            self._stubs[addr] = (
                channel,
                {
                    "AuthService": auction_pb2_grpc.AuthServiceStub(channel),
                    "AuctionService": auction_pb2_grpc.AuctionServiceStub(channel),
                },
            )
        stub = self._stubs[addr][1][service]
        return getattr(stub, method)(request, metadata=metadata, timeout=timeout)

    def close(self):
        for channel, _ in self._stubs.values():
            channel.close()
        self._stubs.clear()


class ClusterClient:
    def __init__(
        self,
        addresses,
        *,
        invoker=None,
        client_id=None,
        give_up_after: float = 30.0,
        backoff: tuple = (0.05, 1.0),
        sleep=time.sleep,
    ):
        if not addresses:
            raise ValueError("need at least one node address")
        self.addresses = list(addresses)
        self.invoker = invoker or GrpcInvoker()
        # 128 random bits: unique per client process. It is also what the
        # dedupe table is keyed by, so it is not printed or shared.
        self.client_id = client_id or uuid.uuid4().hex
        self.token = None
        self._seq = 0
        self._target = self.addresses[0]
        self._give_up_after = give_up_after
        self._backoff = backoff
        self._sleep = sleep
        self.sent = []  # (addr, method, request) for every attempt; tests and traces read it

    # --- the retry loop ------------------------------------------------------------

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def _after(self, addr) -> str:
        """The next address in the list after addr (wrapping)."""
        if addr in self.addresses:
            return self.addresses[(self.addresses.index(addr) + 1) % len(self.addresses)]
        return self.addresses[0]

    def _call(self, service, method, request, timeout=RPC_TIMEOUT):
        metadata = [(TOKEN_METADATA_KEY, self.token)] if self.token else []
        deadline = time.monotonic() + self._give_up_after
        delay = self._backoff[0]
        target = self._target
        hops = 0  # redirects followed since the last wait
        while True:
            self.sent.append((target, method, request))
            try:
                response = self.invoker(target, service, method, request, metadata, timeout)
                self._target = target
                return response
            except grpc.RpcError as e:
                code = e.code()
                if code == grpc.StatusCode.FAILED_PRECONDITION:
                    hint = dict(e.trailing_metadata() or ()).get(LEADER_ADDR_METADATA_KEY)
                    if hint and hint != target and hops < len(self.addresses):
                        log.info("%s -> %s: not leader, redirected to %s", method, target, hint)
                        target, hops = hint, hops + 1
                        continue
                    why = "not leader, no usable hint"
                elif code in _RETRY_ELSEWHERE:
                    why = f"{code.name} ({e.details()})"
                else:
                    raise
                if time.monotonic() + delay > deadline:
                    raise ClusterUnavailable(f"{method}: gave up after {self._give_up_after:.0f}s: {why}") from e
                nxt = self._after(target)
                log.info("%s -> %s: %s; trying %s in %.2fs", method, target, why, nxt, delay)
                self._sleep(delay)
                target, hops = nxt, 0
                delay = min(delay * 2, self._backoff[1])

    # --- the API: writes take a serial number once, reads take none ------------------

    def login(self, username, password) -> auction_pb2.LoginResponse:
        response = self._call("AuthService", "Login", auction_pb2.LoginRequest(
            username=username, password=password, client_id=self.client_id, seq=self._next_seq()))
        if response.result.success:
            self.token = response.token
        return response

    def logout(self) -> auction_pb2.LogoutResponse:
        response = self._call("AuthService", "Logout", auction_pb2.LogoutRequest(
            client_id=self.client_id, seq=self._next_seq()))
        self.token = None
        return response

    def create_auction(self, auction_id, item, close_time, attributes=None):
        """close_time: epoch micros UTC. attributes (e.g. {"condition":
        "mint"}) drive the server-side LLM description; omit to skip it."""
        return self._call("AuctionService", "CreateAuction", auction_pb2.CreateAuctionRequest(
            auction_id=auction_id, item=item, close_time=close_time, attributes=attributes or {},
            client_id=self.client_id, seq=self._next_seq()), timeout=CREATE_TIMEOUT)

    def place_bid(self, auction_id, amount) -> auction_pb2.PlaceBidResponse:
        return self._call("AuctionService", "PlaceBid", auction_pb2.PlaceBidRequest(
            auction_id=auction_id, amount=amount, client_id=self.client_id, seq=self._next_seq()))

    def get_auction(self, auction_id):
        """The Auction, or None if there is no such auction.

        GetAuctionResponse nests an Auction submessage, and protobuf has no
        null for submessages -- on failure response.auction is still
        present, just zero-filled, so it would look like a real auction with
        auction_id 0. Returning None makes that failure impossible to
        mistake for data."""
        response = self._call("AuctionService", "GetAuction", auction_pb2.GetAuctionRequest(auction_id=auction_id))
        if not response.result.success:
            log.info("get auction %d: %s", auction_id, response.result.reason)
            return None
        return response.auction

    def list_auctions(self, include_closed=False):
        return self._call("AuctionService", "ListAuctions",
                          auction_pb2.ListAuctionsRequest(include_closed=include_closed)).auctions

    def close(self):
        close = getattr(self.invoker, "close", None)
        if close:
            close()


# --- the M1 end-to-end flow, now against the cluster --------------------------------

# How far out to schedule the demo auction's close, measured from just
# before CreateAuction is sent. Generous on purpose: CreateAuction blocks on
# a synchronous LLM call for the description (attributes are supplied
# below), and docs/design-notes.md measures that call at up to ~7.4s on a
# cold model load. This margin has to clear that call PLUS both bids before
# close_time, or PlaceBid would reject them as "Time is up" -- the same
# deadline guard covers both real usage and this demo.
DEMO_CLOSE_SECONDS = 25


def main(argv=None):
    p = argparse.ArgumentParser(description="M1 demo flow against the auction cluster.")
    p.add_argument("--nodes", default=",".join(DEFAULT_NODES), help="client addresses, comma-separated")
    p.add_argument("--auction-id", type=int, default=1)
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s.%(msecs)03d %(message)s", datefmt="%H:%M:%S")
    nodes = [a.strip() for a in args.nodes.split(",") if a.strip()]

    users = {name: ClusterClient(nodes) for name in ("nisarg", "mrudul", "karan", "tanmay")}
    try:
        for name, client in users.items():
            r = client.login(name, "pass")
            print(f"login {name}: {r.result.success} - {r.result.reason}")

        close_time = time_conv.dt_to_micros(
            time_conv.now_utc() + datetime.timedelta(seconds=DEMO_CLOSE_SECONDS)
        )
        created = users["nisarg"].create_auction(
            args.auction_id, "vintage clock", close_time,
            attributes={"condition": "used, running order", "brand": "Seiko", "era": "1970s"},
        )
        print(f"create auction: {created.result.success} - {created.result.reason}")

        for name, amount in (("karan", 150), ("mrudul", 200), ("tanmay", 200)):
            r = users[name].place_bid(args.auction_id, amount)
            print(f"place bid ({name}, {amount}): {r.result.success} - {r.result.reason}"
                  f" [high {r.current_high_bid} by {r.current_high_bidder}]")

        print(f"waiting for the auction to close ({DEMO_CLOSE_SECONDS + 2}s) ...")
        time.sleep(DEMO_CLOSE_SECONDS + 2)

        auction = users["nisarg"].get_auction(args.auction_id)
        print(f"final result: {auction}")
    finally:
        for client in users.values():
            client.close()


if __name__ == "__main__":
    main()
