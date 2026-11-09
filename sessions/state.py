"""Replicated session state and the two commands that change it.

Same shape as auction/state.py: commands are frozen dataclasses, and the
token is generated OUTSIDE (on the leader, sessions/store.py) and carried
on the Login command as plain data. Randomness happens once, before
proposing; every node then applies the same token. Same pattern as the LLM
description on CreateAuction.
"""

import threading
from dataclasses import dataclass
from typing import Optional

from auction.state import Result


@dataclass(frozen=True)
class Login:
    token: str
    username: str


@dataclass(frozen=True)
class Logout:
    token: str


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
        self._lock = threading.Lock()

    def apply(self, command) -> Result:
        with self._lock:
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
