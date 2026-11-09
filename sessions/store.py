"""SessionStore: create / validate / destroy, now over REPLICATED state.

M1 planned this swap. Its docstring said: "This class exists so that
stretch goal is a swap of this class for a Raft-backed one, not surgery
across every handler that checks a session: everything else only ever
calls create()/validate()/destroy()." The shape is kept; what is behind it
changed (docs/decisions.md, A3):

  create(username) -> token
      Mints the token HERE, on the leader, with secrets.token_urlsafe,
      BEFORE proposing, and proposes Login(token, username). Randomness
      happens once, outside apply(); the token is replicated as data. Same
      pattern as the LLM description on CreateAuction. Returns the token
      that apply_fn put in the table. On a dedupe hit (a retried Login with
      the same client_id and seq) that is the FIRST attempt's token, not
      the one just minted, which was never stored.
  validate(token) -> username | None
      Reads THIS node's applied SessionState (sessions/state.py). On a
      follower that can lag the leader; server/auth_interceptor.py deals
      with that. No Raft round.
  destroy(token)
      Proposes Logout(token).

create() and destroy() raise NotLeaderError / OutcomeUnknownError
(replication/replicator.py) like any other write; the servicer maps them.

With no replicator, SessionStore() is a cluster of one with no Raft
(LocalReplicator), so the M1 unit tests run unchanged, through the real
command encoding.

No TTL. A session lives until an explicit destroy(). Expiry would need a wall
clock inside validate(), and once sessions are replicated, two nodes with
clock skew would disagree about whether a token is still valid -- the same
class of bug as a wall-clock check inside apply(). Deferring expiry means
deferring that decision too: if it's ever added, it will be as an explicit
command with the time carried on the command, not a clock read in validate().
"""

import secrets
from typing import Optional

from replication.replicator import LocalReplicator
from replication.state_machine import StateMachine
from sessions.state import Login, Logout, SessionState

# secrets, not random: `random` is a deterministic PRNG meant to be seeded
# and reproduced -- exactly the property an auth token must NOT have. secrets
# draws from the OS's CSPRNG, so a token can't be predicted or replayed by
# anyone who doesn't already hold it.
TOKEN_BYTES = 32


class SessionRefused(Exception):
    """The Login command was applied and said no: a stale serial number,
    or (never in practice) a token collision."""


class SessionStore:
    def __init__(self, state: SessionState = None, replicator=None):
        self._state = state if state is not None else SessionState()
        if replicator is None:
            replicator = LocalReplicator(StateMachine(sessions=self._state))
        self._replicator = replicator

    def create(self, username: str, client_id: str = "", seq: int = 0) -> str:
        """Mint a token, replicate Login, return the token now in the table."""
        token = secrets.token_urlsafe(TOKEN_BYTES)
        applied = self._replicator.submit(Login(token=token, username=username), client_id, seq)
        if not applied.result.success:
            raise SessionRefused(applied.result.reason)
        return applied.token

    def validate(self, token: str) -> Optional[str]:
        """Return the username for token, or None if it names no session
        on this node, as of what this node has applied."""
        return self._state.validate(token)

    def destroy(self, token: str, client_id: str = "", seq: int = 0) -> None:
        """Replicate Logout(token). Destroying an unknown or
        already-destroyed token is not an error."""
        self._replicator.submit(Logout(token=token), client_id, seq)
