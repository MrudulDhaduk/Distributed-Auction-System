"""Replicated session state and the two commands that change it.

Same shape as auction/state.py: commands are frozen dataclasses, and the
token is generated OUTSIDE (on the leader, sessions/store.py) and carried
on the Login command as plain data. Randomness happens once, before
proposing; every node then applies the same token. Same pattern as the LLM
description on CreateAuction.
"""

import hmac
import threading
from dataclasses import dataclass
from typing import Optional

from auction.state import Result
from sessions.users import SEED_USERS, password_matches, username_rejection


@dataclass(frozen=True)
class Login:
    token: str
    username: str


@dataclass(frozen=True)
class Logout:
    token: str


@dataclass(frozen=True)
class RegisterUser:
    """A new account. salt and password_hash are computed on the leader
    BEFORE proposing (sessions/users.py); the password itself is never in
    the log."""

    username: str
    salt: str
    password_hash: str


class SessionState:
    """token -> username, changed only by apply(). Replicated: every node
    applies the same Login/Logout commands in the same log order, so every
    node holds the same table.

    apply() follows auction/state.py's rules: deterministic (no clock, no
    randomness, no I/O), and an outcome is a returned Result, never an
    exception.

    The lock is for the readers: validate() runs on gRPC worker threads
    while the applier thread may be inside apply().
    """

    def __init__(self):
        self._sessions: dict[str, str] = {}
        # username -> (salt, password_hash), for accounts made by
        # RegisterUser. The seeded accounts (sessions/users.py) are a
        # constant and are not stored here.
        self._users: dict[str, tuple] = {}
        self._lock = threading.Lock()

    def apply(self, command) -> Result:
        with self._lock:
            if isinstance(command, RegisterUser):
                reason = username_rejection(command.username)
                if reason is None and (command.username in SEED_USERS or command.username in self._users):
                    reason = f"The username {command.username!r} is already taken"
                if reason is None and not (command.salt and command.password_hash):
                    reason = "No password hash: the server did not hash a password"
                if reason is not None:
                    return Result(success=False, reason=reason)
                self._users[command.username] = (command.salt, command.password_hash)
                return Result(success=True, reason=f"Account {command.username} created")
            if isinstance(command, Login):
                if command.token in self._sessions:
                    # A 256-bit random token colliding is not going to
                    # happen; refusing keeps apply() total anyway, and the
                    # first owner's session is never handed to someone else.
                    return Result(success=False, reason="token already in use")
                self._sessions[command.token] = command.username
                return Result(success=True, reason="your session is created")
            if isinstance(command, Logout):
                # Idempotent, as in M1: the postcondition "this token is not
                # valid" holds either way.
                self._sessions.pop(command.token, None)
                return Result(success=True, reason="logged out")
            return Result(success=False, reason="not a session command")

    def validate(self, token: str) -> Optional[str]:
        """The username for token, or None. Reads THIS node's applied
        state, which can lag the leader's (see sessions/store.py)."""
        with self._lock:
            return self._sessions.get(token)

    def snapshot(self) -> dict:
        with self._lock:
            return dict(self._sessions)

    def user_exists(self, username: str) -> bool:
        with self._lock:
            return username in SEED_USERS or username in self._users

    def check_password(self, username: str, password: str) -> bool:
        """Seeded accounts: a plain compare against the constant. Registered
        accounts: PBKDF2 against the replicated (salt, hash), as applied on
        THIS node (which can lag the leader; server/auth_servicer.py)."""
        if username in SEED_USERS:
            return hmac.compare_digest(SEED_USERS[username], password)
        with self._lock:
            record = self._users.get(username)
        if record is None:
            return False
        return password_matches(password, *record)

    def users_snapshot(self) -> dict:
        """{username: (salt, hash)} for registered accounts only."""
        with self._lock:
            return dict(self._users)
