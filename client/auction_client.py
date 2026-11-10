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
import json
import logging
import time
import uuid

import grpc

from common import time_conv
from common.wire import LEADER_ADDR_METADATA_KEY, TOKEN_METADATA_KEY
from generated import (
    assistant_pb2,
    assistant_pb2_grpc,
    auction_pb2,
    auction_pb2_grpc,
    business_pb2,
    business_pb2_grpc,
    payment_pb2,
    payment_pb2_grpc,
)

log = logging.getLogger("client")

DEFAULT_NODES = ["127.0.0.1:50071", "127.0.0.1:50072", "127.0.0.1:50073"]

# Per-attempt gRPC deadline. Above the server's commit wait (5 s,
# replication/replicator.py) so the server's explicit UNAVAILABLE arrives
# first. CreateAuction and the assistant calls get more: they wait for the
# LLM server first. The app server gives up on the model after
# llm_server/config.py APP_DEADLINE_SECONDS (25 s), then needs up to 5 s for
# the commit, so 35 s lets the server's own clear answer arrive before this
# deadline (chunk 7; was 20 s when the model call was bounded only by Ollama).
RPC_TIMEOUT = 8.0
CREATE_TIMEOUT = 35.0
ASSIST_TIMEOUT = 35.0

_RETRY_ELSEWHERE = {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED}


POST_TYPES = ("auction", "bid", "payment", "description")
GET_TYPES = ("auction", "auctions", "escrow", "assistant", "price_trends")


def _require(type, fields, *names):
    missing = [n for n in names if n not in fields]
    if missing:
        raise ValueError(f"{type}: missing {', '.join(missing)}")


# (method, type) -> the typed response the server packs into BusinessResponse.response.
_BUSINESS_RESPONSES = {
    ("post", "auction"): auction_pb2.CreateAuctionResponse,
    ("post", "bid"): auction_pb2.PlaceBidResponse,
    ("post", "payment"): payment_pb2.PaymentResponse,
    ("post", "description"): assistant_pb2.DescriptionResponse,
    ("get", "auction"): auction_pb2.GetAuctionResponse,
    ("get", "auctions"): auction_pb2.ListAuctionsResponse,
    ("get", "escrow"): payment_pb2.GetEscrowResponse,
    ("get", "assistant"): assistant_pb2.AnswerResponse,
    ("get", "price_trends"): assistant_pb2.PriceTrendsResponse,
}


def _unpack_business(method, type_, reply):
    """The BusinessResponse's Any, as the object post()/get() return for the same
    type: get 'auction' is the Auction or None, get 'auctions' is the list of rows."""
    message = _BUSINESS_RESPONSES[(method, type_)]()
    if not reply.response.Unpack(message):
        raise ValueError(f"server answered {reply.response.TypeName()}, expected {message.DESCRIPTOR.full_name}")
    if (method, type_) == ("get", "auction"):
        return message.auction if message.result.success else None
    if (method, type_) == ("get", "auctions"):
        return message.auctions
    return message


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
                    "PaymentService": payment_pb2_grpc.PaymentServiceStub(channel),
                    "AssistantService": assistant_pb2_grpc.AssistantServiceStub(channel),
                    "BusinessService": business_pb2_grpc.BusinessServiceStub(channel),
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

    def _call(self, service, method, request, timeout=RPC_TIMEOUT, token=None):
        """token: send THIS token instead of the one login() stored. Only
        post() and get() pass it, because their signature takes a token."""
        token = self.token if token is None else token
        metadata = [(TOKEN_METADATA_KEY, token)] if token else []
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

    # `token` on each method below: send this token instead of login()'s. Only
    # post() and get() use it (their signature carries the token).

    def create_auction(self, auction_id, item, close_time, attributes=None, token=None):
        """close_time: epoch micros UTC. attributes (e.g. {"condition":
        "mint"}) drive the server-side LLM description; omit to skip it."""
        return self._call("AuctionService", "CreateAuction", auction_pb2.CreateAuctionRequest(
            auction_id=auction_id, item=item, close_time=close_time, attributes=attributes or {},
            client_id=self.client_id, seq=self._next_seq()), timeout=CREATE_TIMEOUT, token=token)

    def place_bid(self, auction_id, amount, token=None) -> auction_pb2.PlaceBidResponse:
        return self._call("AuctionService", "PlaceBid", auction_pb2.PlaceBidRequest(
            auction_id=auction_id, amount=amount, client_id=self.client_id, seq=self._next_seq()), token=token)

    def get_auction(self, auction_id, token=None):
        """The Auction, or None if there is no such auction.

        GetAuctionResponse nests an Auction submessage, and protobuf has no
        null for submessages -- on failure response.auction is still
        present, just zero-filled, so it would look like a real auction with
        auction_id 0. Returning None makes that failure impossible to
        mistake for data."""
        response = self._call("AuctionService", "GetAuction",
                              auction_pb2.GetAuctionRequest(auction_id=auction_id), token=token)
        if not response.result.success:
            log.info("get auction %d: %s", auction_id, response.result.reason)
            return None
        return response.auction

    def list_auctions(self, include_closed=False, token=None):
        return self._call("AuctionService", "ListAuctions",
                          auction_pb2.ListAuctionsRequest(include_closed=include_closed), token=token).auctions

    # --- payment and escrow (proto/payment.proto) ----------------------------------

    def pay(self, auction_id, amount, token=None) -> payment_pb2.PaymentResponse:
        """The winner pays the winning bid into escrow."""
        return self._call("PaymentService", "Pay", payment_pb2.PayRequest(
            auction_id=auction_id, amount=amount, client_id=self.client_id, seq=self._next_seq()), token=token)

    def release_escrow(self, auction_id, token=None) -> payment_pb2.PaymentResponse:
        """The seller confirms: the held money goes to the seller."""
        return self._call("PaymentService", "ReleaseEscrow", payment_pb2.EscrowActionRequest(
            auction_id=auction_id, client_id=self.client_id, seq=self._next_seq()), token=token)

    def refund_escrow(self, auction_id, token=None) -> payment_pb2.PaymentResponse:
        """The seller cancels the sale: the held money goes back to the winner."""
        return self._call("PaymentService", "RefundEscrow", payment_pb2.EscrowActionRequest(
            auction_id=auction_id, client_id=self.client_id, seq=self._next_seq()), token=token)

    def get_escrow(self, auction_id, token=None) -> payment_pb2.GetEscrowResponse:
        return self._call("PaymentService", "GetEscrow",
                          payment_pb2.GetEscrowRequest(auction_id=auction_id), token=token)

    # --- the LLM assistant (proto/assistant.proto) -----------------------------------

    def ask_assistant(self, question, auction_id=None, token=None) -> assistant_pb2.AnswerResponse:
        """The bidding FAQ. auction_id given = about that auction (the leader
        sends its live state to the model as context). An LLM outage comes back
        as result.success False with a reason, not as an exception."""
        request = assistant_pb2.AskRequest(question=question)
        if auction_id is not None:
            request.auction_id = auction_id
        return self._call("AssistantService", "AskAssistant", request, timeout=ASSIST_TIMEOUT, token=token)

    def generate_description(self, auction_id, attributes, token=None) -> assistant_pb2.DescriptionResponse:
        """The seller asks for a (new) listing text; it is replicated as data."""
        return self._call("AssistantService", "GenerateDescription", assistant_pb2.GenerateDescriptionRequest(
            auction_id=auction_id, attributes=attributes, client_id=self.client_id, seq=self._next_seq()),
            timeout=ASSIST_TIMEOUT, token=token)

    def price_trends(self, limit=0, token=None) -> assistant_pb2.PriceTrendsResponse:
        return self._call("AssistantService", "GetPriceTrends", assistant_pb2.PriceTrendsRequest(limit=limit),
                          timeout=ASSIST_TIMEOUT, token=token)

    # --- the handout's post(token, type, data) and get(token, type, params) ----------
    #
    # The wire is typed (one RPC per operation; auction.proto and payment.proto
    # say why). These are the prescribed signatures as real functions on top,
    # the same arrangement as server/llm_client.py's get_llm_answer. They
    # validate BEFORE making any RPC, so a typo is a ValueError that names the
    # problem, not a half-built request on the wire.

    def post(self, token, type, data):
        """A write. `type` picks the operation; `data` is its fields.

            "auction"  {auction_id, item, close_time[, attributes]}   -> CreateAuction
            "bid"      {auction_id, amount}                           -> PlaceBid
            "payment"  {auction_id[, action: "pay"|"release"|"refund"][, amount]}
                       "pay" is the default and needs amount          -> Pay / ReleaseEscrow / RefundEscrow
            "description" {auction_id, attributes}                    -> GenerateDescription

        Returns the typed response of the RPC it dispatched to."""
        if type == "auction":
            _require(type, data, "auction_id", "item", "close_time")
            return self.create_auction(data["auction_id"], data["item"], data["close_time"],
                                       data.get("attributes"), token=token)
        if type == "bid":
            _require(type, data, "auction_id", "amount")
            return self.place_bid(data["auction_id"], data["amount"], token=token)
        if type == "payment":
            _require(type, data, "auction_id")
            action = data.get("action", "pay")
            if action == "pay":
                _require("payment (pay)", data, "amount")
                return self.pay(data["auction_id"], data["amount"], token=token)
            if action == "release":
                return self.release_escrow(data["auction_id"], token=token)
            if action == "refund":
                return self.refund_escrow(data["auction_id"], token=token)
            raise ValueError(f"post 'payment': unknown action {action!r}; expected pay, release or refund")
        if type == "description":
            _require(type, data, "auction_id", "attributes")
            return self.generate_description(data["auction_id"], data["attributes"], token=token)
        raise ValueError(f"post: unknown type {type!r}; expected one of {sorted(POST_TYPES)}")

    def get(self, token, type, params):
        """A read. `type` picks what to read; `params` are its fields.

            "auction"   {auction_id}            -> the Auction, or None
            "auctions"  {[include_closed]}      -> the list of AuctionSummary
            "escrow"    {auction_id}            -> GetEscrowResponse
            "assistant" {question[, auction_id]}  -> AnswerResponse   (the FAQ chatbot)
            "price_trends" {[limit]}            -> PriceTrendsResponse
        """
        if type == "auction":
            _require(type, params, "auction_id")
            return self.get_auction(params["auction_id"], token=token)
        if type == "auctions":
            return self.list_auctions(params.get("include_closed", False), token=token)
        if type == "escrow":
            _require(type, params, "auction_id")
            return self.get_escrow(params["auction_id"], token=token)
        if type == "assistant":
            _require(type, params, "question")
            return self.ask_assistant(params["question"], params.get("auction_id"), token=token)
        if type == "price_trends":
            return self.price_trends(params.get("limit", 0), token=token)
        raise ValueError(f"get: unknown type {type!r}; expected one of {sorted(GET_TYPES)}")

    def process_business_request(self, request_id, payload, context):
        """The handout's processBusinessRequest(requestId, payload, context)
        (R12): calls the app server's BusinessService.ProcessBusinessRequest RPC
        (proto/business.proto), which dispatches to the same handlers the typed
        methods above call.

            payload  {"method": "post" | "get", "type": <a post/get type>,
                      "data": {...}}                     (data may be omitted)
            context  {"token": <session token>, ...}     (token omitted = this client's)

        Returns (request_id, response), where response is the same object post()
        or get() returns for that type. request_id is echoed by the server.

        The token goes in gRPC metadata like every other call. It is never put in
        the payload or in the context sent on the wire, and the server would not
        read it there. The other context keys are sent as plain key/values. A post
        takes a serial number once and keeps it across retries, as the typed
        writes do."""
        _require("process_business_request", payload, "method", "type")
        method, type_ = payload["method"], payload["type"]
        if method not in ("post", "get"):
            raise ValueError(f"process_business_request: method must be 'post' or 'get', not {method!r}")
        known = POST_TYPES if method == "post" else GET_TYPES
        if type_ not in known:
            raise ValueError(f"{method}: unknown type {type_!r}; expected one of {sorted(known)}")
        context = dict(context or {})
        token = context.pop("token", self.token)
        request = business_pb2.BusinessRequest(
            request_id=request_id,
            payload=json.dumps({"method": method, "type": type_, "data": payload.get("data", {})}),
            context={k: str(v) for k, v in context.items()},
        )
        if method == "post":
            request.client_id = self.client_id
            request.seq = self._next_seq()
        slow = (method, type_) in {("post", "auction"), ("post", "description"),
                                   ("get", "assistant"), ("get", "price_trends")}   # these wait for the LLM
        reply = self._call("BusinessService", "ProcessBusinessRequest", request,
                           timeout=ASSIST_TIMEOUT if slow else RPC_TIMEOUT, token=token)
        return reply.request_id, _unpack_business(method, type_, reply)

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
