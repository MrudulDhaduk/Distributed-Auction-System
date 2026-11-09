"""Replicated session state and the two commands that change it.

Same shape as auction/state.py: commands are frozen dataclasses, and the
token is generated OUTSIDE (on the leader, sessions/store.py) and carried
on the Login command as plain data. Randomness happens once, before
proposing; every node then applies the same token. Same pattern as the LLM
description on CreateAuction.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Login:
    token: str
    username: str


@dataclass(frozen=True)
class Logout:
    token: str
