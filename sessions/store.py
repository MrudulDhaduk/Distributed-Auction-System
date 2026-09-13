"""In-memory session store.

Deliberately NOT part of auction.state.apply() and NOT replicated. Raft-backed
sessions are a stretch goal (problem statement, section 8) and need Raft to
exist first. This class exists so that stretch goal is a swap of this class
for a Raft-backed one, not surgery across every handler that checks a
session: everything else only ever calls create()/validate()/destroy().

No TTL. A session lives until an explicit destroy(). Expiry would need a wall
clock inside validate(), and once sessions are replicated, two nodes with
clock skew would disagree about whether a token is still valid -- the same
class of bug as a wall-clock check inside apply(). Deferring expiry means
deferring that decision too: if it's ever added, it will be as an explicit
command with the time carried on the command, not a clock read in validate().
"""

import secrets
from dataclasses import dataclass

# secrets, not random: `random` is a deterministic PRNG meant to be seeded
# and reproduced -- exactly the property an auth token must NOT have. secrets
# draws from the OS's CSPRNG, so a token can't be predicted or replayed by
# anyone who doesn't already hold it.
TOKEN_BYTES = 32


@dataclass(frozen=True)
class SessionInfo:
    username: str


class SessionStore:
    """Maps opaque tokens to the username that created them."""

    def __init__(self):
        self._sessions: dict[str, SessionInfo] = {}

    def create(self, username: str) -> str:
        """Mint a new token for username and return it."""
        token = secrets.token_urlsafe(TOKEN_BYTES)
        self._sessions[token] = SessionInfo(username=username)
        return token

    def validate(self, token: str) -> str | None:
        """Return the username for token, or None if it names no session."""
        info = self._sessions.get(token)
        return info.username if info is not None else None

    def destroy(self, token: str) -> None:
        """Remove token's session, if any. Destroying an unknown or
        already-destroyed token is not an error."""
        self._sessions.pop(token, None)
