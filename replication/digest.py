"""The canonical form of the applied state, and its digest.

    canonical(snapshot)      -> a JSON-able dict
    canonical_json(snapshot) -> that dict as one exact string
    state_digest(snapshot)   -> sha256 hex of that string's UTF-8 bytes

`snapshot` is StateMachine.snapshot(): {"auctions", "sessions", "dedupe"}.

Why a canonical form: two replicas "agree" when they hold the same state.
Comparing Python objects across processes needs a byte form anyway, and a
digest of one fixed byte form is the cheapest comparison there is: the
dashboard compares three 64-character strings five times a second, and the
checker compares the full JSON as well (docs/decisions.md, T3).

What makes it canonical (the same state always gives the same string):
  * auctions are a LIST sorted by auction_id, not a dict (JSON object keys
    would be strings, and "10" sorts before "2");
  * bids stay in the order applied -- that order is replicated state;
  * every datetime is int epoch microseconds (common/time_conv.py, exact);
  * sessions and dedupe rows are sorted lists;
  * json.dumps(sort_keys=True, separators=(",", ":"), ensure_ascii=True):
    no whitespace choices, no dict-order choices, no encoding choices.

Secrets: a session token is a bearer credential. The dump travels over the
peer port with no auth (proto/status.proto), so tokens appear only as
hash_token(token) -- enough to tell two different tables apart, useless for
logging in.

Pure functions only: no clock, no randomness, no I/O. The digest is part of
nothing replicated; it is computed from the state, never stored in it.
"""

import hashlib
import json

from common import time_conv


def hash_token(token: str) -> str:
    """First 16 hex digits of sha256(token): identifies a token, cannot be
    used as one. "" stays "" (no token)."""
    if not token:
        return ""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:16]


def _micros(dt):
    return None if dt is None else time_conv.dt_to_micros(dt)


def _bid(entry) -> dict:
    return {"bidder": entry.bidder, "amount": entry.bid_amount, "time": _micros(entry.time_stamp)}


def _auction(auction_id, a: dict) -> dict:
    return {
        "auction_id": auction_id,
        "item": a["item"],
        "owner": a["owner"],
        "description": a["description"],
        "close_time": _micros(a["close_time"]),
        "closed": a["closed"],
        "closed_at": _micros(a["closed_at"]),
        "winner": None if a["winner"] is None else _bid(a["winner"]),
        "bids": [_bid(b) for b in a["bids"]],
    }


def _response(applied) -> dict:
    return {
        "success": applied.result.success,
        "reason": applied.result.reason,
        "high_bid": applied.high_bid,
        "high_bidder": applied.high_bidder,
        "token": hash_token(applied.token),
    }


def canonical(snapshot: dict) -> dict:
    return {
        "auctions": [_auction(aid, a) for aid, a in sorted(snapshot["auctions"].items())],
        "sessions": sorted([hash_token(tok), user] for tok, user in snapshot["sessions"].items()),
        "dedupe": [
            [cid, seq, _response(applied)]
            for cid, (seq, applied) in sorted(snapshot["dedupe"].items())
        ],
    }


def canonical_json(snapshot: dict) -> str:
    return json.dumps(canonical(snapshot), sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def digest_of_json(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def state_digest(snapshot: dict) -> str:
    return digest_of_json(canonical_json(snapshot))
