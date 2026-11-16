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
from sessions.users import SEED_USERS, password_rejection, username_rejection

# The seeded accounts now live in sessions/users.py (SEED_USERS); accounts
# made with Register are replicated (sessions/state.py RegisterUser).
_USERS = SEED_USERS


class AuthServicer(auction_pb2_grpc.AuthServiceServicer):
    def __init__(self, sessions: SessionStore):
        self._sessions = sessions

    def Login(self, request, context) -> auction_pb2.LoginResponse:
        """Authenticate and mint a replicated session token.

        The password check reads THIS node's applied accounts. A seeded
        account is the same everywhere. A registered one may not have reached
        a lagging follower, or a brand-new leader, yet. So a failed check is
        only final on the leader, after a read barrier; a follower redirects
        (NotLeaderError) instead of saying no."""
        if not self._sessions.check_password(request.username, request.password):
            with raft_errors(context):
                self._sessions.check_leader()
                self._sessions.read_barrier()
            if not self._sessions.check_password(request.username, request.password):
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

    def Register(self, request, context) -> auction_pb2.RegisterResponse:
        """Create an account. Format and password rules first (cheap, and the
        same answer on every node), then leader + read barrier, then the
        uniqueness pre-check, then hash and propose. apply() checks the name
        and uniqueness again: two concurrent Registers for one name are
        ordered by the log, and the second is refused there."""
        def no(reason):
            return auction_pb2.RegisterResponse(result=auction_pb2.Result(success=False, reason=reason))

        reason = username_rejection(request.username) or password_rejection(request.password)
        if reason is not None:
            return no(reason)
        with raft_errors(context):
            self._sessions.check_leader()
            self._sessions.read_barrier()
            if self._sessions.user_exists(request.username):
                return no(f"The username {request.username!r} is already taken")
            result = self._sessions.register(request.username, request.password, request.client_id, request.seq)
        return auction_pb2.RegisterResponse(
            result=auction_pb2.Result(success=result.success, reason=result.reason)
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
