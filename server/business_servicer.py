"""gRPC servicer for BusinessService: ProcessBusinessRequest (R12).

    processBusinessRequest(requestId, payload, context) -> response

A DISPATCHER. It parses the payload, builds the typed request for the
operation that was asked for, and calls the existing handler
(AuctionServicer / PaymentServicer / AssistantServicer) with the SAME gRPC
context it was given. It contains no auction, payment or LLM logic.

That is how "same auth, same leader rules" holds without being re-implemented:

  * the AuthInterceptor wraps this service like every other, so a missing or
    unknown token never reaches this code, and `context.username` is the
    validated user. The payload cannot name a user, and neither the payload nor
    the `context` map is ever read as a credential;
  * the handlers do their own leader check, read barrier, dedupe and error
    mapping (NotLeader -> FAILED_PRECONDITION + leader-addr, unknown outcome ->
    UNAVAILABLE) and abort through the context this function passed them, so
    the gRPC status, and the trailing metadata a client follows to the leader,
    arrive unchanged;
  * a write's (client_id, seq) comes from the REQUEST, not from the payload.

What is its own: the untyped payload. proto/business.proto says why that is
the weakest part of this design, so every field is validated here and a bad one
is INVALID_ARGUMENT, naming the field, before any handler runs.
"""

import json
import logging

import grpc

from generated import (
    assistant_pb2,
    auction_pb2,
    business_pb2,
    business_pb2_grpc,
    payment_pb2,
)

log = logging.getLogger(__name__)


class _Invalid(Exception):
    """The payload is malformed. Becomes INVALID_ARGUMENT."""


# --- field validation -----------------------------------------------------------------------------


def _is_int(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)   # True is an int in Python


def _int(data: dict, name: str, *, required: bool = True, default: int = 0) -> int:
    if name not in data:
        if required:
            raise _Invalid(f"missing {name}")
        return default
    if not _is_int(data[name]):
        raise _Invalid(f"{name} must be a whole number, not {data[name]!r}")
    return data[name]


def _str(data: dict, name: str) -> str:
    if name not in data:
        raise _Invalid(f"missing {name}")
    if not isinstance(data[name], str):
        raise _Invalid(f"{name} must be text, not {data[name]!r}")
    return data[name]


def _attributes(data: dict, *, required: bool) -> dict:
    if "attributes" not in data:
        if required:
            raise _Invalid("missing attributes")
        return {}
    attributes = data["attributes"]
    if not isinstance(attributes, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in attributes.items()):
        raise _Invalid("attributes must be an object of text to text")
    return attributes


# --- the dispatch tables: type -> (handler name on the servicer, request builder) --------------------
#
# A builder gets the validated `data` dict and the BusinessRequest (for the
# write's serial number) and returns the typed request message.


def _create_auction(data, r):
    return auction_pb2.CreateAuctionRequest(
        auction_id=_int(data, "auction_id"), item=_str(data, "item"), close_time=_int(data, "close_time"),
        attributes=_attributes(data, required=False), client_id=r.client_id, seq=r.seq)


def _place_bid(data, r):
    return auction_pb2.PlaceBidRequest(
        auction_id=_int(data, "auction_id"), amount=_int(data, "amount"), client_id=r.client_id, seq=r.seq)


def _generate_description(data, r):
    return assistant_pb2.GenerateDescriptionRequest(
        auction_id=_int(data, "auction_id"), attributes=_attributes(data, required=True),
        client_id=r.client_id, seq=r.seq)


def _get_auction(data, r):
    return auction_pb2.GetAuctionRequest(auction_id=_int(data, "auction_id"))


def _list_auctions(data, r):
    include_closed = data.get("include_closed", False)
    if not isinstance(include_closed, bool):
        raise _Invalid(f"include_closed must be true or false, not {include_closed!r}")
    return auction_pb2.ListAuctionsRequest(include_closed=include_closed)


def _get_escrow(data, r):
    return payment_pb2.GetEscrowRequest(auction_id=_int(data, "auction_id"))


def _ask_assistant(data, r):
    request = assistant_pb2.AskRequest(question=_str(data, "question"))
    if "auction_id" in data:
        request.auction_id = _int(data, "auction_id")
    return request


def _price_trends(data, r):
    return assistant_pb2.PriceTrendsRequest(limit=_int(data, "limit", required=False))


# type -> (servicer attribute, handler method name, builder). "payment" is special-cased below
# because one type fans out to three handlers by `action`.
POST_DISPATCH = {
    "auction": ("auction", "CreateAuction", _create_auction),
    "bid": ("auction", "PlaceBid", _place_bid),
    "payment": None,
    "description": ("assistant", "GenerateDescription", _generate_description),
}
GET_DISPATCH = {
    "auction": ("auction", "GetAuction", _get_auction),
    "auctions": ("auction", "ListAuctions", _list_auctions),
    "escrow": ("payment", "GetEscrow", _get_escrow),
    "assistant": ("assistant", "AskAssistant", _ask_assistant),
    "price_trends": ("assistant", "GetPriceTrends", _price_trends),
}
PAYMENT_ACTIONS = {"pay": "Pay", "release": "ReleaseEscrow", "refund": "RefundEscrow"}


class BusinessServicer(business_pb2_grpc.BusinessServiceServicer):
    def __init__(self, auction_servicer, payment_servicer, assistant_servicer):
        """The three servicers this node already serves, so there is one
        implementation of every operation."""
        self._servicers = {"auction": auction_servicer, "payment": payment_servicer, "assistant": assistant_servicer}

    def _parse(self, payload: str) -> tuple:
        try:
            document = json.loads(payload)
        except ValueError as e:
            raise _Invalid(f"payload is not valid JSON ({e})") from e
        if not isinstance(document, dict):
            raise _Invalid("payload must be a JSON object")
        method = document.get("method")
        if method is None:
            raise _Invalid("missing method")
        if method not in ("post", "get"):
            raise _Invalid(f"method must be 'post' or 'get', not {method!r}")
        if "type" not in document:
            raise _Invalid("missing type")
        data = document.get("data", {})
        if not isinstance(data, dict):
            raise _Invalid("data must be a JSON object")
        return method, document["type"], data

    def _resolve(self, method: str, type_, data: dict, request) -> tuple:
        """(handler, typed request). Raises _Invalid for anything malformed."""
        table = POST_DISPATCH if method == "post" else GET_DISPATCH
        if type_ not in table:
            raise _Invalid(f"unknown {method} type {type_!r}; expected one of {sorted(table)}")
        if method == "post" and type_ == "payment":
            _int(data, "auction_id")
            action = data.get("action", "pay")
            if action not in PAYMENT_ACTIONS:
                raise _Invalid(f"unknown payment action {action!r}; expected pay, release or refund")
            handler = getattr(self._servicers["payment"], PAYMENT_ACTIONS[action])
            if action == "pay":
                message = payment_pb2.PayRequest(
                    auction_id=data["auction_id"], amount=_int(data, "amount"),
                    client_id=request.client_id, seq=request.seq)
            else:
                message = payment_pb2.EscrowActionRequest(
                    auction_id=data["auction_id"], client_id=request.client_id, seq=request.seq)
            return handler, message
        owner, name, build = table[type_]
        return getattr(self._servicers[owner], name), build(data, request)

    def ProcessBusinessRequest(self, request: business_pb2.BusinessRequest, context) -> business_pb2.BusinessResponse:
        try:
            method, type_, data = self._parse(request.payload)
            handler, message = self._resolve(method, type_, data, request)
        except _Invalid as e:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(e))

        log.info("business request_id=%s %s %s context=%s", request.request_id, method, type_, dict(request.context))
        response = handler(message, context)   # the same context: same user, same abort paths
        out = business_pb2.BusinessResponse(request_id=request.request_id)
        out.response.Pack(response)
        return out
