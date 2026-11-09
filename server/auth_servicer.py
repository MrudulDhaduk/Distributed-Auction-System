"""gRPC servicer for AuthService: Login, Logout.

Both go through SessionStore (sessions/store.py), which replicates them as
Login/Logout commands in the Raft log. Neither touches AuctionState.

Login: the password check is local. _USERS is a constant, identical on
every node, so a follower that refuses a wrong password says exactly what
the leader would say. A correct password then reaches
SessionStore.create(), which proposes; on a follower that raises
NotLeaderError and the client is redirected.
"""

from common.wire import TOKEN_METADATA_KEY
from generated import auction_pb2, auction_pb2_grpc
from server.errors import raft_errors
from sessions.store import SessionRefused, SessionStore

_USERS = {
    "mrudul": "pass",
    "nisarg": "pass",
    "karan": "pass",
    "tanmay": "pass",
}


class AuthServicer(auction_pb2_grpc.AuthServiceServicer):
    def __init__(self, sessions: SessionStore):
        self._sessions = sessions

    def Login(self, request, context) -> auction_pb2.LoginResponse:
        """Authenticate and mint a replicated session token."""
        if _USERS.get(request.username) != request.password:
            return auction_pb2.LoginResponse(
                result=auction_pb2.Result(success=False, reason="invalid username or password"),
                token="",
            )

        with raft_errors(context):
            try:
                token = self._sessions.create(request.username, request.client_id, request.seq)
            except SessionRefused as e:
                return auction_pb2.LoginResponse(
                    result=auction_pb2.Result(success=False, reason=str(e)), token=""
                )
        return auction_pb2.LoginResponse(
            result=auction_pb2.Result(success=True, reason="your session is created"),
            token=token,
        )

    def Logout(self, request, context) -> auction_pb2.LogoutResponse:
        """Destroy the caller's session, through the log. Idempotent."""
        metadata = dict(context.invocation_metadata())
        token = metadata.get(TOKEN_METADATA_KEY)  # the key the interceptor uses
        if token:
            with raft_errors(context):
                self._sessions.destroy(token, request.client_id, request.seq)
        return auction_pb2.LogoutResponse(
            result=auction_pb2.Result(success=True, reason="logged out"),
        )
