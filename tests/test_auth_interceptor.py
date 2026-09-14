"""
Specifies AuthInterceptor: the one place every RPC's session token gets
checked before a handler runs.

Written against the real contract in server/auth_interceptor.py:

- AuthInterceptor(session_store).intercept_service(continuation, handler_call_details)
  returns a grpc.RpcMethodHandler-like object with .unary_unary(request, context).
- handler_call_details has .method (full method string) and
  .invocation_metadata (a sequence of (key, value) pairs).
- The token metadata key is "token" (server.auth_interceptor.TOKEN_METADATA_KEY).
- AuthService.Login is exempt (server.auth_interceptor.EXEMPT_METHODS).
- On success, the wrapped context exposes `.username`. On rejection, the
  fake context's `.abort(code, details)` is called instead of the real
  handler ever running.

No real gRPC server involved: handler_call_details and the continuation are
both stand-ins, and the "handler" is a stub that records what it was called
with.
"""

from collections import namedtuple

import grpc
import pytest

from server.auth_interceptor import AuthInterceptor, TOKEN_METADATA_KEY
from sessions.store import SessionStore

HandlerCallDetails = namedtuple("HandlerCallDetails", ["method", "invocation_metadata"])

LOGIN_METHOD = "/auction.AuthService/Login"
PLACE_BID_METHOD = "/auction.AuctionService/PlaceBid"


class StubHandler:
    """Stands in for the real grpc.RpcMethodHandler that `continuation`
    would normally return."""

    request_deserializer = object()
    response_serializer = object()

    def __init__(self):
        self.calls = []

    def unary_unary(self, request, context):
        self.calls.append((request, context))
        return "handled"


class AbortRaised(Exception):
    def __init__(self, code, details):
        self.code = code
        self.details = details


class FakeContext:
    """Stands in for grpc.ServicerContext. abort() raises instead of
    terminating the RPC, so tests can assert on it."""

    def abort(self, code, details):
        raise AbortRaised(code, details)


@pytest.fixture
def store():
    return SessionStore()


@pytest.fixture
def interceptor(store):
    return AuthInterceptor(store)


def continuation_returning(handler):
    def continuation(handler_call_details):
        return handler

    return continuation


def test_login_passes_through_with_no_token():
    store = SessionStore()
    interceptor = AuthInterceptor(store)
    handler = StubHandler()

    hcd = HandlerCallDetails(method=LOGIN_METHOD, invocation_metadata=[])
    result_handler = interceptor.intercept_service(continuation_returning(handler), hcd)

    context = FakeContext()
    result_handler.unary_unary("login-request", context)

    assert handler.calls == [("login-request", context)]


def test_valid_token_reaches_handler_with_correct_username():
    store = SessionStore()
    token = store.create("alice")
    interceptor = AuthInterceptor(store)
    handler = StubHandler()

    hcd = HandlerCallDetails(
        method=PLACE_BID_METHOD,
        invocation_metadata=[(TOKEN_METADATA_KEY, token)],
    )
    result_handler = interceptor.intercept_service(continuation_returning(handler), hcd)

    result_handler.unary_unary("bid-request", FakeContext())

    assert len(handler.calls) == 1
    seen_request, seen_context = handler.calls[0]
    assert seen_request == "bid-request"
    assert seen_context.username == "alice"


def test_missing_token_is_rejected_with_unauthenticated():
    store = SessionStore()
    interceptor = AuthInterceptor(store)
    handler = StubHandler()

    hcd = HandlerCallDetails(method=PLACE_BID_METHOD, invocation_metadata=[])
    result_handler = interceptor.intercept_service(continuation_returning(handler), hcd)

    with pytest.raises(AbortRaised) as exc_info:
        result_handler.unary_unary("bid-request", FakeContext())

    assert exc_info.value.code == grpc.StatusCode.UNAUTHENTICATED
    assert handler.calls == []


def test_garbage_token_is_rejected_with_unauthenticated():
    store = SessionStore()
    interceptor = AuthInterceptor(store)
    handler = StubHandler()

    hcd = HandlerCallDetails(
        method=PLACE_BID_METHOD,
        invocation_metadata=[(TOKEN_METADATA_KEY, "not-a-real-token")],
    )
    result_handler = interceptor.intercept_service(continuation_returning(handler), hcd)

    with pytest.raises(AbortRaised) as exc_info:
        result_handler.unary_unary("bid-request", FakeContext())

    assert exc_info.value.code == grpc.StatusCode.UNAUTHENTICATED
    assert handler.calls == []


def test_destroyed_token_is_rejected_with_unauthenticated():
    store = SessionStore()
    token = store.create("alice")
    store.destroy(token)
    interceptor = AuthInterceptor(store)
    handler = StubHandler()

    hcd = HandlerCallDetails(
        method=PLACE_BID_METHOD,
        invocation_metadata=[(TOKEN_METADATA_KEY, token)],
    )
    result_handler = interceptor.intercept_service(continuation_returning(handler), hcd)

    with pytest.raises(AbortRaised) as exc_info:
        result_handler.unary_unary("bid-request", FakeContext())

    assert exc_info.value.code == grpc.StatusCode.UNAUTHENTICATED
    assert handler.calls == []
