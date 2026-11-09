"""Server interceptor that validates the session token on every RPC.

Runs before any servicer method. The token travels in gRPC metadata (never
a message field -- see the design note at the top of proto/auction.proto),
so this is the one place that reads it, instead of every handler doing its
own lookup.

AuthService.Login is exempt: you cannot present a token in order to obtain
one. Every other RPC -- Logout included -- is rejected with UNAUTHENTICATED
if the token is missing or SessionStore.validate() doesn't recognise it.

M2 (Raft): sessions are replicated, and validate() reads THIS node's
applied state, which on a follower can lag the leader. A token minted a
moment ago may simply not have arrived yet. So an unrecognised token is
only refused by the LEADER; a follower cannot tell "unknown" from "not
here yet", and redirects instead (FAILED_PRECONDITION + leader-addr, the
same answer it gives every RPC). A missing token is refused everywhere:
there is nothing to look up. (docs/decisions.md, A4.)

Only unary-unary RPCs are handled. Every RPC in auction.proto is
unary-unary today; a streaming RPC added later would need its own wrapping
here (a different RpcMethodHandler shape), which is a deliberate gap, not
an oversight.
"""

import grpc

from common.wire import TOKEN_METADATA_KEY
from replication.replicator import NotLeaderError
from server.errors import abort_not_leader

EXEMPT_METHODS = frozenset({
    "/auction.AuthService/Login",
})


class _ContextWithUsername:
    """Wraps a ServicerContext to add `.username`, without touching the
    real context object.

    Everything else is forwarded via __getattr__ so the wrapped context is
    interchangeable with the real one from the handler's point of view.
    """

    def __init__(self, context, username):
        self._context = context
        self.username = username

    def __getattr__(self, name):
        return getattr(self._context, name)


def _reject(context, details):
    context.abort(grpc.StatusCode.UNAUTHENTICATED, details)


class AuthInterceptor(grpc.ServerInterceptor):
    def __init__(self, session_store, replicator=None):
        """replicator: this node's RaftReplicator, to ask "am I the
        leader?". None = a cluster of one (M1 unit tests): always leader."""
        self._session_store = session_store
        self._replicator = replicator

    def _not_leader(self):
        """The NotLeaderError to redirect with, or None if we are leader."""
        if self._replicator is None:
            return None
        try:
            self._replicator.check_leader()
        except NotLeaderError as e:
            return e
        return None

    def intercept_service(self, continuation, handler_call_details):
        if handler_call_details.method in EXEMPT_METHODS:
            return continuation(handler_call_details)

        metadata = dict(handler_call_details.invocation_metadata or [])
        token = metadata.get(TOKEN_METADATA_KEY)
        username = self._session_store.validate(token) if token else None

        if username is None:
            not_leader = self._not_leader() if token else None
            if not_leader is not None:
                def redirect(request, context):
                    abort_not_leader(context, not_leader)

                return grpc.unary_unary_rpc_method_handler(redirect)

            def reject(request, context):
                _reject(context, "missing or invalid session token")

            return grpc.unary_unary_rpc_method_handler(reject)

        handler = continuation(handler_call_details)

        def authenticated_behavior(request, context):
            return handler.unary_unary(request, _ContextWithUsername(context, username))

        return grpc.unary_unary_rpc_method_handler(
            authenticated_behavior,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )
