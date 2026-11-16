"""Validation of everything the browser sends, before any of it reaches the
cluster. Pure functions; each returns the cleaned value or raises Invalid
with a sentence the page shows next to the field.

This is the FIRST of three layers, not the only one:
  1. here: types, ranges, sizes (a JSON body can hold anything);
  2. the node's handler: the same rules again, plus the clock checks
     (server/auction_servicer.py close_time_rejection, server/auth_servicer.py);
  3. apply() on every replica: the rules that decide (auction/state.py
     create_rejection / bid_rejection, sessions/state.py RegisterUser).
Layer 3 is what makes "a seller cannot bid on their own auction" true: a
client that skips layers 1 and 2 is still refused there.
"""

from auction.state import ITEM_MAX_CHARS
from server.auction_servicer import (
    ATTRIBUTE_KEY_MAX,
    ATTRIBUTE_VALUE_MAX,
    ATTRIBUTES_MAX,
    MAX_AUCTION_DURATION,
)
from sessions.users import password_rejection, username_rejection

MIN_DURATION_SECONDS = 15
MAX_DURATION_SECONDS = int(MAX_AUCTION_DURATION.total_seconds())
MAX_BID = 10**12
QUESTION_MAX_CHARS = 500


class Invalid(ValueError):
    def __init__(self, message, field=None):
        super().__init__(message)
        self.field = field


def body(obj) -> dict:
    if not isinstance(obj, dict):
        raise Invalid("The request body must be a JSON object")
    return obj


def _int(obj, field, *, label, lo, hi):
    value = obj.get(field)
    # bool is an int in Python; "5" and 5.5 are not whole numbers here.
    if isinstance(value, bool) or not isinstance(value, int):
        if isinstance(value, float) and value.is_integer():
            value = int(value)
        else:
            raise Invalid(f"{label} must be a whole number", field)
    if not lo <= value <= hi:
        raise Invalid(f"{label} must be between {lo:,} and {hi:,}", field)
    return value


def _str(obj, field, *, label, required=True, max_len=None):
    value = obj.get(field, "")
    if not isinstance(value, str):
        raise Invalid(f"{label} must be text", field)
    if required and not value.strip():
        raise Invalid(f"{label} must not be empty", field)
    if max_len is not None and len(value) > max_len:
        raise Invalid(f"{label} is {len(value)} characters; the limit is {max_len}", field)
    return value


def credentials(obj) -> tuple:
    obj = body(obj)
    username = _str(obj, "username", label="Username", max_len=20)
    password = _str(obj, "password", label="Password", max_len=64)
    return username, password


def registration(obj) -> tuple:
    username, password = credentials(obj)
    reason = username_rejection(username)
    if reason:
        raise Invalid(reason, "username")
    reason = password_rejection(password)
    if reason:
        raise Invalid(reason, "password")
    confirm = obj.get("confirm")
    if confirm is not None and confirm != password:
        raise Invalid("The two passwords do not match", "confirm")
    return username, password


def new_auction(obj) -> dict:
    obj = body(obj)
    item = _str(obj, "item", label="Item name", max_len=ITEM_MAX_CHARS).strip()
    duration = _int(obj, "duration_seconds", label="Duration (seconds)",
                    lo=MIN_DURATION_SECONDS, hi=MAX_DURATION_SECONDS)
    raw = obj.get("attributes") or {}
    if not isinstance(raw, dict):
        raise Invalid("Attributes must be name/value pairs", "attributes")
    attributes = {}
    for key, value in raw.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise Invalid("Attribute names and values must be text", "attributes")
        key, value = key.strip(), value.strip()
        if not key and not value:
            continue  # an empty row in the form
        if not key or not value:
            raise Invalid("Every attribute needs both a name and a value", "attributes")
        if len(key) > ATTRIBUTE_KEY_MAX or len(value) > ATTRIBUTE_VALUE_MAX:
            raise Invalid(f"Attribute names are at most {ATTRIBUTE_KEY_MAX} characters, "
                          f"values at most {ATTRIBUTE_VALUE_MAX}", "attributes")
        if key in attributes:
            raise Invalid(f"Attribute {key!r} is given twice", "attributes")
        attributes[key] = value
    if len(attributes) > ATTRIBUTES_MAX:
        raise Invalid(f"At most {ATTRIBUTES_MAX} attributes", "attributes")
    return {"item": item, "duration_seconds": duration, "attributes": attributes}


def bid(obj) -> dict:
    obj = body(obj)
    return {
        "auction_id": _int(obj, "auction_id", label="Auction", lo=1, hi=2**62),
        "amount": _int(obj, "amount", label="Bid amount", lo=1, hi=MAX_BID),
        # True only for the page's "send it anyway" button: skips the
        # dashboard's own seller pre-check so the cluster's refusal shows.
        "skip_precheck": obj.get("skip_precheck") is True,
    }


def question(obj) -> dict:
    obj = body(obj)
    return {
        "auction_id": _int(obj, "auction_id", label="Auction", lo=1, hi=2**62),
        "question": _str(obj, "question", label="Question", max_len=QUESTION_MAX_CHARS).strip(),
    }


def description_request(obj) -> dict:
    obj = body(obj)
    auction_id = _int(obj, "auction_id", label="Auction", lo=1, hi=2**62)
    attrs = new_auction({"item": "x", "duration_seconds": MIN_DURATION_SECONDS,
                         "attributes": obj.get("attributes")})["attributes"]
    if not attrs:
        raise Invalid("Give at least one attribute for the model to work from", "attributes")
    return {"auction_id": auction_id, "attributes": attrs}
