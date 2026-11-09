"""Specifies checker/: the property checker run after a chaos run.

Inputs (all plain data, as the chaos harness writes them):
  history  one dict per client operation: who, (client_id, seq), what,
           outcome ok / rejected / unknown, wall-clock start and end.
  dumps    {node_id: StateDump as a dict}: each node's canonical JSON.
  leaders  [{"term", "node", "source"}]: every time anyone saw a LEADER.

The good case is built for real: commands go through a real StateMachine
and replication/digest.py, so the dumps have exactly the shape a node
returns. Each broken case changes ONE thing and must make exactly the
property that guards it fail, with evidence that names the culprit.
"""

import copy
import datetime
import json

import pytest

from auction.state import CloseAuction, CreateAuction, PlaceBid
from checker import properties as P
from checker.properties import check_all
from common import time_conv
from replication import codec, digest
from replication.state_machine import StateMachine
from sessions.state import Login

UTC = datetime.timezone.utc
T0 = datetime.datetime(2030, 1, 1, 12, 0, 0, tzinfo=UTC)
CLOSE = T0 + datetime.timedelta(seconds=30)


def us(seconds):
    return time_conv.dt_to_micros(T0 + datetime.timedelta(seconds=seconds))


def rec(client, seq, op, outcome, start, end=None, **kw):
    r = dict(client=client, client_id=f"cid-{client}", user=client, seq=seq, op=op,
             outcome=outcome, start=us(start), end=us(end if end is not None else start + 0.01),
             reason=kw.pop("reason", ""), auction_id=kw.pop("auction_id", 0), amount=kw.pop("amount", 0),
             high_bid=kw.pop("high_bid", 0), high_bidder=kw.pop("high_bidder", ""))
    r.update(kw)
    return r


def scenario():
    """A seller and three bidders on auction 1 (closes at T0+30 s), one
    racing equal bid, one unknown outcome that DID commit, one that did
    not, and the close."""
    applied = [
        ("cid-seller", 1, Login(token="tok-s", username="seller")),
        ("cid-seller", 2, CreateAuction(1, "clock", CLOSE, "seller", "")),
        ("cid-karan", 1, Login(token="tok-k", username="karan")),
        ("cid-mrudul", 1, Login(token="tok-m", username="mrudul")),
        ("cid-tanmay", 1, Login(token="tok-t", username="tanmay")),
        ("cid-karan", 2, PlaceBid(1, "karan", 100, T0 + datetime.timedelta(seconds=1))),
        ("cid-mrudul", 2, PlaceBid(1, "mrudul", 150, T0 + datetime.timedelta(seconds=2))),
        ("cid-tanmay", 2, PlaceBid(1, "tanmay", 150, T0 + datetime.timedelta(seconds=2))),  # loses the race
        ("cid-karan", 3, PlaceBid(1, "karan", 200, T0 + datetime.timedelta(seconds=3))),  # unknown, committed
        ("", 0, CloseAuction(1, T0 + datetime.timedelta(seconds=31))),
        ("cid-mrudul", 4, PlaceBid(1, "mrudul", 300, T0 + datetime.timedelta(seconds=32))),  # after close
    ]
    history = [
        rec("seller", 1, "login", "ok", 0, token_hash=digest.hash_token("tok-s")),
        rec("seller", 2, "create", "ok", 0.1, auction_id=1, item="clock", close_time=time_conv.dt_to_micros(CLOSE)),
        rec("karan", 1, "login", "ok", 0.2, token_hash=digest.hash_token("tok-k")),
        rec("mrudul", 1, "login", "ok", 0.2, token_hash=digest.hash_token("tok-m")),
        rec("tanmay", 1, "login", "ok", 0.2, token_hash=digest.hash_token("tok-t")),
        rec("karan", 2, "bid", "ok", 1, auction_id=1, amount=100, high_bid=100, high_bidder="karan"),
        rec("mrudul", 2, "bid", "ok", 2, auction_id=1, amount=150, high_bid=150, high_bidder="mrudul"),
        rec("tanmay", 2, "bid", "rejected", 2, auction_id=1, amount=150, high_bid=150, high_bidder="mrudul",
            reason="Bid of 150 is not added because current highest bid is 150"),
        rec("karan", 3, "bid", "unknown", 3, 10, auction_id=1, amount=200, reason="ClusterUnavailable"),
        rec("mrudul", 3, "bid", "unknown", 4, 10, auction_id=1, amount=250, reason="ClusterUnavailable"),  # never committed
        rec("mrudul", 4, "bid", "rejected", 32, auction_id=1, amount=300,
            reason="Auction is already closed mrudul"),
    ]
    dumps = dumps_from(applied)
    leaders = [{"term": 1, "node": "n1", "source": "status"}, {"term": 2, "node": "n2", "source": "log:n2"},
               {"term": 2, "node": "n2", "source": "status"}]
    return history, dumps, leaders


def dumps_from(applied, nodes=("n1", "n2", "n3")):
    sm = StateMachine()
    for i, (cid, seq, cmd) in enumerate(applied, start=1):
        sm.apply_fn(i, codec.encode(cmd, client_id=cid, seq=seq))
    index, snap = sm.indexed_snapshot()
    text = digest.canonical_json(snap)
    return {n: {"node_id": n, "digest_index": index, "state_digest": digest.digest_of_json(text), "state_json": text}
            for n in nodes}


def edit_dump(dumps, node, fn, fix_digest=True):
    """Change one node's state document with fn(doc); recompute its digest
    unless fix_digest=False (then the digest no longer matches the JSON)."""
    dumps = copy.deepcopy(dumps)
    doc = json.loads(dumps[node]["state_json"])
    fn(doc)
    text = json.dumps(doc, sort_keys=True, separators=(",", ":"))
    dumps[node]["state_json"] = text
    if fix_digest:
        dumps[node]["state_digest"] = digest.digest_of_json(text)
    return dumps


def failed(report):
    return {r.name for r in report if not r.passed}


def evidence(report, name):
    return "\n".join(next(r for r in report if r.name == name).violations)


# --- the good case -------------------------------------------------------------


def test_a_correct_run_passes_every_property():
    report = check_all(*scenario())
    assert failed(report) == set(), [(r.name, r.violations) for r in report if not r.passed]
    assert {r.name for r in report} == set(P.PROPERTY_NAMES)
    assert all(r.checked > 0 for r in report)


# --- one broken input per property -------------------------------------------


def test_an_acknowledged_bid_missing_on_one_node():
    history, dumps, leaders = scenario()
    dumps = edit_dump(dumps, "n3", lambda d: d["auctions"][0]["bids"].pop(0))
    report = check_all(history, dumps, leaders)
    assert P.ACKED_PRESENT in failed(report)
    assert "n3" in evidence(report, P.ACKED_PRESENT) and "karan" in evidence(report, P.ACKED_PRESENT)


def test_an_acknowledged_bid_applied_twice():
    history, dumps, leaders = scenario()
    dumps = edit_dump(dumps, "n1", lambda d: d["auctions"][0]["bids"].insert(1, dict(d["auctions"][0]["bids"][0])))
    report = check_all(history, dumps, leaders)
    assert P.ACKED_PRESENT in failed(report)
    assert "2 times" in evidence(report, P.ACKED_PRESENT)


def test_an_acknowledged_create_or_login_missing():
    history, dumps, leaders = scenario()
    dumps = edit_dump(dumps, "n2", lambda d: d["sessions"].pop(0))
    report = check_all(history, dumps, leaders)
    assert P.ACKED_PRESENT in failed(report)
    assert "login" in evidence(report, P.ACKED_PRESENT)


def test_replicas_that_differ_are_caught_with_the_differing_field():
    history, dumps, leaders = scenario()
    dumps = edit_dump(dumps, "n2", lambda d: d["auctions"][0].update(description="changed"))
    report = check_all(history, dumps, leaders)
    assert P.IDENTICAL in failed(report)
    assert "description" in evidence(report, P.IDENTICAL) and "n2" in evidence(report, P.IDENTICAL)


def test_a_digest_that_does_not_match_its_json():
    history, dumps, leaders = scenario()
    dumps = edit_dump(dumps, "n1", lambda d: d["auctions"][0].update(item="x"), fix_digest=False)
    report = check_all(history, dumps, leaders)
    assert P.IDENTICAL in failed(report)
    assert "does not match" in evidence(report, P.IDENTICAL)


def test_replicas_at_different_indexes():
    history, dumps, leaders = scenario()
    dumps["n3"]["digest_index"] -= 1
    report = check_all(history, dumps, leaders)
    assert P.IDENTICAL in failed(report)
    assert "digest_index" in evidence(report, P.IDENTICAL)


def test_a_bid_history_that_is_not_strictly_increasing():
    history, dumps, leaders = scenario()

    def lower(d):
        d["auctions"][0]["bids"][1]["amount"] = 100  # 100, 100, 200
    dumps = {n: v for n, v in edit_dump(dumps, "n1", lower).items()}
    for n in ("n2", "n3"):
        dumps = edit_dump(dumps, n, lower)
    report = check_all(history, dumps, leaders)
    assert P.INCREASING in failed(report)
    assert "not above" in evidence(report, P.INCREASING)


def test_a_winner_that_is_not_the_max():
    history, dumps, leaders = scenario()
    for n in ("n1", "n2", "n3"):
        dumps = edit_dump(dumps, n, lambda d: d["auctions"][0].update(winner=d["auctions"][0]["bids"][0]))
    report = check_all(history, dumps, leaders)
    assert P.INCREASING in failed(report)
    assert "winner" in evidence(report, P.INCREASING)


def test_a_bid_stamped_after_the_close():
    history, dumps, leaders = scenario()

    def late(d):
        a = d["auctions"][0]
        a["bids"].append({"bidder": "mrudul", "amount": 300, "time": a["close_time"] + 1})
        a["winner"] = a["bids"][-1]
    for n in ("n1", "n2", "n3"):
        dumps = edit_dump(dumps, n, late)
    report = check_all(history, dumps, leaders)
    assert P.NO_BID_AFTER_CLOSE in failed(report)
    assert "after close_time" in evidence(report, P.NO_BID_AFTER_CLOSE)


def test_an_acknowledged_bid_sent_after_the_close_time():
    """The server stamps curr_time AFTER the client started the call, on
    the same clock, so a bid started after close_time cannot legally win."""
    history, dumps, leaders = scenario()
    history[5] = dict(history[5], start=us(30.5), end=us(30.6))
    report = check_all(history, dumps, leaders)
    assert P.NO_BID_AFTER_CLOSE in failed(report)
    assert "started after" in evidence(report, P.NO_BID_AFTER_CLOSE)


def test_a_rejected_bid_that_is_in_the_state():
    """The signature of a dedupe failure: the first attempt committed, the
    retry ran AGAIN (instead of returning the stored response) and was
    rejected because the first attempt had already raised the high bid."""
    history, dumps, leaders = scenario()
    history[6] = dict(history[6], outcome="rejected", reason="Bid of 150 is not added because current highest bid is 150")
    report = check_all(history, dumps, leaders)
    assert P.REJECTIONS in failed(report)
    assert "mrudul" in evidence(report, P.REJECTIONS) and "present" in evidence(report, P.REJECTIONS)


def test_a_rejection_quoting_a_high_bid_that_never_existed():
    history, dumps, leaders = scenario()
    history[7] = dict(history[7], reason="Bid of 150 is not added because current highest bid is 175")
    report = check_all(history, dumps, leaders)
    assert P.REJECTIONS in failed(report)
    assert "175" in evidence(report, P.REJECTIONS)


def test_already_closed_but_the_auction_is_open():
    history, dumps, leaders = scenario()
    for n in ("n1", "n2", "n3"):
        dumps = edit_dump(dumps, n, lambda d: d["auctions"][0].update(closed=False, closed_at=None, winner=None))
    report = check_all(history, dumps, leaders)
    assert P.REJECTIONS in failed(report)
    assert "not closed" in evidence(report, P.REJECTIONS)


def test_a_stale_seq_rejection_means_the_workload_rule_was_broken():
    history, dumps, leaders = scenario()
    history[10] = dict(history[10], reason="stale request: seq 4 is older than 5")
    report = check_all(history, dumps, leaders)
    assert P.REJECTIONS in failed(report)
    assert "stale" in evidence(report, P.REJECTIONS)


def test_an_unknown_outcome_applied_twice():
    history, dumps, leaders = scenario()

    def twice(d):
        bids = d["auctions"][0]["bids"]
        bids.insert(3, dict(bids[2]))
    for n in ("n1", "n2", "n3"):
        dumps = edit_dump(dumps, n, twice)
    report = check_all(history, dumps, leaders)
    assert P.UNKNOWN_AT_MOST_ONCE in failed(report)
    assert "200" in evidence(report, P.UNKNOWN_AT_MOST_ONCE)


def test_two_leaders_in_one_term():
    history, dumps, leaders = scenario()
    leaders.append({"term": 2, "node": "n3", "source": "status"})
    report = check_all(history, dumps, leaders)
    assert failed(report) == {P.ONE_LEADER_PER_TERM}
    assert "term 2" in evidence(report, P.ONE_LEADER_PER_TERM)
    assert "n2" in evidence(report, P.ONE_LEADER_PER_TERM) and "n3" in evidence(report, P.ONE_LEADER_PER_TERM)


@pytest.mark.parametrize("breakage", ["two writes in flight", "seq reused", "same amount twice"])
def test_a_history_that_breaks_the_workload_rule(breakage):
    history, dumps, leaders = scenario()
    if breakage == "two writes in flight":
        history[6] = dict(history[6], start=us(0.15), end=us(0.25))  # overlaps mrudul's login
    elif breakage == "seq reused":
        history[8] = dict(history[8], seq=2)
    else:
        history.append(rec("tanmay", 3, "bid", "rejected", 40, auction_id=1, amount=150,
                           reason="Auction is already closed tanmay"))
    report = check_all(history, dumps, leaders)
    assert P.WELL_FORMED in failed(report)


def test_no_dumps_is_a_failure_not_a_pass():
    history, _, leaders = scenario()
    report = check_all(history, {}, leaders)
    assert P.IDENTICAL in failed(report)


def test_the_report_prints_pass_fail_per_property():
    history, dumps, leaders = scenario()
    leaders.append({"term": 2, "node": "n3", "source": "status"})
    text = P.format_report(check_all(history, dumps, leaders))
    assert f"FAIL  {P.ONE_LEADER_PER_TERM}" in text
    assert f"PASS  {P.IDENTICAL}" in text


def test_a_run_saved_to_disk_is_checked_by_the_cli(tmp_path, capsys):
    from checker.__main__ import main
    from checker.run_files import load_run, save_run

    history, dumps, leaders = scenario()
    save_run(tmp_path / "good", history, dumps, leaders)
    assert load_run(tmp_path / "good") == (history, dumps, leaders)
    assert main([str(tmp_path / "good")]) == 0
    assert "ALL PROPERTIES PASS" in capsys.readouterr().out

    save_run(tmp_path / "bad", history, dumps, leaders + [{"term": 1, "node": "n3", "source": "x"}])
    assert main([str(tmp_path / "bad")]) == 1
    assert f"FAIL  {P.ONE_LEADER_PER_TERM}" in capsys.readouterr().out


def test_a_node_that_gave_no_dump_is_a_failure_not_ignored():
    history, dumps, leaders = scenario()
    dumps["n3"] = {"node_id": "n3", "error": "StatusCode.UNAVAILABLE"}
    report = check_all(history, dumps, leaders)
    assert P.IDENTICAL in failed(report)
    assert "n3: no dump" in evidence(report, P.IDENTICAL)
    assert P.ACKED_PRESENT not in failed(report)  # the other two are still checked
