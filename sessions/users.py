"""Accounts: the seeded ones, the rules for a new one, and password hashing.

Seeded accounts are a constant, identical on every node (they used to live
in server/auth_servicer.py). Accounts made at run time are REPLICATED: a
RegisterUser command in the Raft log (sessions/state.py), so a user created
on one leader can still log in after a failover.

The password never enters the log. The leader's handler draws a random salt
(secrets) and computes PBKDF2-HMAC-SHA256 BEFORE proposing, and the command
carries only (salt, hash) as data -- the same "non-determinism outside
apply()" rule as session tokens and LLM text. apply() checks the username
rule and uniqueness, nothing else.

Pure functions apart from new_salt(); no I/O.
"""

import hashlib
import hmac
import re
import secrets

SEED_USERS = {
    "mrudul": "pass",
    "nisarg": "pass",
    "karan": "pass",
    "tanmay": "pass",
    # Demo background bidders (scripts/demo.py). Same password as the others.
    "bg1": "pass",
    "bg2": "pass",
    "bg3": "pass",
    "bg4": "pass",
    "bg5": "pass",
    "bg6": "pass",
}

USERNAME_RE = re.compile(r"^[a-z][a-z0-9_]{2,19}$")
PASSWORD_MIN = 4
PASSWORD_MAX = 64
PBKDF2_ITERATIONS = 100_000
SALT_BYTES = 16


def username_rejection(username: str):
    """Why `username` cannot be a new account's name (format only), or None.
    Deterministic: apply() calls it too."""
    if not isinstance(username, str) or not username:
        return "The username must not be empty"
    if not USERNAME_RE.match(username):
        return (
            "The username must be 3-20 characters: lowercase letters, digits or _, "
            "starting with a letter"
        )
    return None


def password_rejection(password: str):
    """Why `password` is not acceptable, or None. Checked by the handler
    only: apply() never sees the password."""
    if not isinstance(password, str) or len(password) < PASSWORD_MIN:
        return f"The password must be at least {PASSWORD_MIN} characters"
    if len(password) > PASSWORD_MAX:
        return f"The password must be at most {PASSWORD_MAX} characters"
    if password != password.strip():
        return "The password must not start or end with spaces"
    return None


def new_salt() -> str:
    return secrets.token_hex(SALT_BYTES)


def hash_password(password: str, salt: str) -> str:
    return hashlib.pbkdf2_hmac(
        "sha256", password.encode("utf-8"), bytes.fromhex(salt), PBKDF2_ITERATIONS
    ).hex()


def password_matches(password: str, salt: str, password_hash: str) -> bool:
    return hmac.compare_digest(hash_password(password, salt), password_hash)
