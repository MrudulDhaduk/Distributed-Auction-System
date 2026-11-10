"""Specifies replication/digest.py: the canonical form of the applied state
and its sha256 digest (proto/status.proto, docs/decisions.md T3).

The digest is how the dashboard and the checker decide "these replicas
agree". It must be:
  * a pure function of the replicated state (same state -> same digest,
    on every node, in every process);
  * sensitive to everything that is replicated: every bid, its order, the
    close, the sessions, the dedupe table;
  * free of secrets: session tokens appear only as hashes.
"""

import datetime
import hashlib
import json

from auction.state import (
    CloseAuction,
    CreateAuction,
    PayAuction,
    PlaceBid,
    ReleaseEscrow,
)
from replication import codec, digest
from replication.state_machine import StateMachine
from sessions.state import Login

UTC = datetime.timezone.utc
T0 = datetime.datetime(2030, 1, 1, 12, 0, 0, tzinfo=UTC)


def at(s):
    return T0 + datetime.timedelta(seconds=s)


def build(commands):
    """A StateMachine fed (client_id, seq, command) triples at indexes 1.."""
    sm = StateMachine()
    for i, (cid, seq, cmd) in enumerate(commands, start=1):
        sm.apply_fn(i, codec.encode(cmd, client_id=cid, seq=seq))
    return sm


BASE = [
    ("s", 1, Login(token="tok-secret-1", username="seller")),
    ("s", 2, CreateAuction(auction_id=2, item="lamp", close_time=at(60), owner="seller", description="d")),
    ("s", 3, CreateAuction(auction_id=10, item="clock", close_time=at(30), owner="seller", description="")),
    ("a", 1, PlaceBid(auction_id=10, bidder="karan", amount=100, curr_time=at(1))),
    ("b", 1, PlaceBid(auction_id=10, bidder="mrudul", amount=150, curr_time=at(2))),
    ("", 0, CloseAuction(auction_id=10, curr_time=at(31))),
]


def test_same_commands_same_digest_in_two_state_machines():
    a, b = build(BASE), build(BASE)
    assert digest.state_digest(a.snapshot()) == digest.state_digest(b.snapshot())


def test_digest_is_sha256_of_the_canonical_json():
    snap = build(BASE).snapshot()
    text = digest.canonical_json(snap)
    assert digest.state_digest(snap) == hashlib.sha256(text.encode("utf-8")).hexdigest()


def test_canonical_form_lists_auctions_by_id_and_bids_in_applied_order():
    doc = json.loads(digest.canonical_json(build(BASE).snapshot()))
    assert [a["auction_id"] for a in doc["auctions"]] == [2, 10]
    clock = doc["auctions"][1]
    assert [(b["bidder"], b["amount"]) for b in clock["bids"]] == [("karan", 100), ("mrudul", 150)]
    assert clock["closed"] is True
    assert clock["winner"] == {"bidder": "mrudul", "amount": 150, "time": clock["bids"][1]["time"]}
    assert clock["close_time"] == 1893499230000000  # epoch micros, exact
    assert clock["closed_at"] == 1893499231000000
    lamp = doc["auctions"][0]
    assert lamp["winner"] is None and lamp["closed_at"] is None
    assert (lamp["item"], lamp["owner"], lamp["description"]) == ("lamp", "seller", "d")


def test_tokens_never_appear_in_the_canonical_form():
    text = digest.canonical_json(build(BASE).snapshot())
    assert "tok-secret-1" not in text
    doc = json.loads(text)
    assert doc["sessions"] == [[digest.hash_token("tok-secret-1"), "seller"]]


def test_a_login_response_in_the_dedupe_table_shows_only_the_token_hash():
    sm = build(BASE[:1])
    doc = json.loads(digest.canonical_json(sm.snapshot()))
    assert doc["dedupe"] == [["s", 1, {
        "success": True, "reason": "your session is created",
        "high_bid": 0, "high_bidder": "", "token": digest.hash_token("tok-secret-1"),
    }]]


def test_dedupe_table_is_part_of_the_state():
    doc = json.loads(digest.canonical_json(build(BASE).snapshot()))
    by_client = {row[0]: row for row in doc["dedupe"]}
    assert set(by_client) == {"s", "a", "b"}  # the closer (client_id "") is never deduplicated
    _, seq, response = by_client["s"]
    assert seq == 3 and response["success"] is True and response["token"] == ""


def test_every_replicated_difference_changes_the_digest():
    base = digest.state_digest(build(BASE).snapshot())
    bid = BASE[3][2]
    variants = {
        "one bid fewer": BASE[:4] + BASE[5:],
        "bid amount": BASE[:3] + [("a", 1, PlaceBid(10, "karan", 101, bid.curr_time))] + BASE[4:],
        "bid time": BASE[:3] + [("a", 1, PlaceBid(10, "karan", 100, at(1.000001)))] + BASE[4:],
        "no close": BASE[:5],
        "other session": [("s", 1, Login(token="tok-other", username="seller"))] + BASE[1:],
        "dedupe seq": [("s", 7, BASE[0][2])] + BASE[1:],
        "description": BASE[:1]
        + [("s", 2, CreateAuction(auction_id=2, item="lamp", close_time=at(60), owner="seller", description="D"))]
        + BASE[2:],
    }
    for name, commands in variants.items():
        assert digest.state_digest(build(commands).snapshot()) != base, name


def test_independent_auctions_in_either_order_are_the_same_state():
    """Two bids on DIFFERENT auctions applied in either order leave the same
    per-auction bid lists, so the same digest. That is right: it IS the same
    state. (Under Raft every node applies one order anyway.)"""
    x = ("a", 1, PlaceBid(auction_id=10, bidder="karan", amount=100, curr_time=at(1)))
    y = ("b", 1, PlaceBid(auction_id=2, bidder="mrudul", amount=5, curr_time=at(2)))
    one = digest.state_digest(build(BASE[:3] + [x, y]).snapshot())
    two = digest.state_digest(build(BASE[:3] + [y, x]).snapshot())
    assert one == two


def test_empty_state_has_a_stable_digest():
    assert digest.state_digest(StateMachine().snapshot()) == digest.state_digest(StateMachine().snapshot())


def test_indexed_snapshot_reports_the_last_applied_command_index():
    sm = build(BASE)
    index, snap = sm.indexed_snapshot()
    assert index == len(BASE)
    assert snap == sm.snapshot()
    assert StateMachine().indexed_snapshot()[0] == 0


def test_a_dedupe_hit_still_advances_the_index():
    sm = build(BASE)
    sm.apply_fn(9, codec.encode(BASE[3][2], client_id="a", seq=1))  # a retry
    assert sm.indexed_snapshot()[0] == 9


# --- escrow is replicated state, so it is part of the digest (chunk 7) -------------------

PAID = BASE + [("m", 1, PayAuction(auction_id=10, payer="mrudul", amount=150, payment_ref="PAY-1"))]
RELEASED = PAID + [("s", 4, ReleaseEscrow(auction_id=10, actor="seller", settlement_ref="REL-1"))]


def test_canonical_form_has_an_escrow_record_for_every_auction():
    doc = json.loads(digest.canonical_json(build(PAID).snapshot()))
    lamp, clock = doc["auctions"]
    assert lamp["escrow"] == {"status": "NONE", "amount": 0, "payer": "", "payment_ref": "", "settlement_ref": ""}
    assert clock["escrow"] == {
        "status": "HELD", "amount": 150, "payer": "mrudul", "payment_ref": "PAY-1", "settlement_ref": "",
    }


def test_every_escrow_difference_changes_the_digest():
    digests = {name: digest.state_digest(build(cmds).snapshot())
               for name, cmds in {"unpaid": BASE, "held": PAID, "released": RELEASED}.items()}
    other_ref = BASE + [("m", 1, PayAuction(auction_id=10, payer="mrudul", amount=150, payment_ref="PAY-2"))]
    digests["other reference"] = digest.state_digest(build(other_ref).snapshot())

    assert len(set(digests.values())) == 4, digests


def test_the_same_escrow_commands_give_the_same_digest():
    assert digest.state_digest(build(RELEASED).snapshot()) == digest.state_digest(build(RELEASED).snapshot())
