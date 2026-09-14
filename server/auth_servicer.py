"""gRPC servicer for AuthService: Login, Logout.

Method bodies are intentionally left as `pass` -- see CLAUDE.md's protected
list. Login and Logout call into SessionStore (sessions/store.py); neither
touches AuctionState.
"""

from generated import auction_pb2, auction_pb2_grpc
from sessions.store import SessionStore
from server import auth_interceptor

_USERS = {
    "mrudul": "pass",
    "nisarg": "pass",
    "karan": "pass",
}

class AuthServicer(auction_pb2_grpc.AuthServiceServicer):
    def __init__(self, sessions: SessionStore):
        self._sessions = sessions

    def Login(self, request, context) -> auction_pb2.LoginResponse:
        """Authenticate and mint a session token."""
        if _USERS.get(request.username) != request.password:
            return auction_pb2.LoginResponse(
                result=auction_pb2.Result(success=False, reason="invalid username or password"),
                token="",
            )

        token = self._sessions.create(request.username)
        return auction_pb2.LoginResponse(
            result=auction_pb2.Result(success=True, reason="your session is created"),
            token=token,
        )

    def Logout(self, request, context) -> auction_pb2.LogoutResponse:
        """Destroy the caller's session. Idempotent."""
        metadata = dict(context.invocation_metadata())
        token = metadata.get(auth_interceptor.TOKEN_METADATA_KEY)       # the key the interceptor uses
        if token:
            self._sessions.destroy(token)
        return auction_pb2.LogoutResponse(
            result=auction_pb2.Result(success=True, reason="logged out"),
        )
