"""The replay viewer (scripts/replay.py): the state it computes, and the HTML file it writes."""

import json
import os
import re
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scripts"))

import replay  # noqa: E402


def node(role, term, leader, digest, index, up=True):
    if not up:
        return {"up": False, **{k: None for k in ("role", "term", "leader_id", "commit_index", "last_applied",
                                                  "last_log_index", "last_log_term", "state_digest",
                                                  "digest_index")}}
    return {"up": True, "role": role, "term": term, "leader_id": leader, "commit_index": index,
            "last_applied": index, "last_log_index": index, "last_log_term": term,
            "state_digest": digest, "digest_index": index}


def status(t, n1, n2, n3):
    return {"t": t, "kind": "status", "nodes": {"n1": n1, "n2": n2, "n3": n3}}


def fake_story():
    """A small run: a hero auction with one refused bid and one 'unknown' bid,
    background bids (accepted, refused, no answer), a kill, an election, a
    restart, escrow, a close, a summary and a checker result."""
    live1 = node("FOLLOWER", 1, "n1", "d1", 3)
    return [
        {"t": 0.1, "kind": "step", "text": "starting LLM + 3 nodes"},
        status(0.2, node("LEADER", 1, "n1", "d0", 2), node("FOLLOWER", 1, "n1", "d0", 2),
               node("FOLLOWER", 1, "n1", "d0", 2)),
        {"t": 0.3, "kind": "auction", "auction_id": 1, "item": "vintage clock", "close_time": 123,
         "description": "A running Seiko clock."},
        {"t": 0.4, "kind": "action", "user": "karan", "op": "bid", "auction_id": 1, "amount": 100,
         "outcome": "ok", "reason": "Bid of 100 added", "high_bid": 100, "high_bidder": "karan", "background": False},
        {"t": 0.5, "kind": "action", "user": "mrudul", "op": "bid", "auction_id": 1, "amount": 150,
         "outcome": "ok", "reason": "Bid of 150 added", "high_bid": 150, "high_bidder": "mrudul", "background": False},
        {"t": 0.6, "kind": "action", "user": "tanmay", "op": "bid", "auction_id": 1, "amount": 150,
         "outcome": "rejected", "reason": "not added because current highest bid is 150",
         "high_bid": 150, "high_bidder": "mrudul", "background": False},
        {"t": 0.7, "kind": "action", "user": "bg1", "op": "bid", "auction_id": 2, "amount": 40,
         "outcome": "ok", "reason": "", "high_bid": 40, "high_bidder": "bg1", "background": True},
        {"t": 0.8, "kind": "action", "user": "bg2", "op": "bid", "auction_id": 2, "amount": 40,
         "outcome": "rejected", "reason": "equal", "high_bid": 40, "high_bidder": "bg1", "background": True},
        {"t": 0.85, "kind": "action", "user": "bg3", "op": "bid", "auction_id": 2, "amount": 60,
         "outcome": "unknown", "reason": "ClusterUnavailable", "high_bid": 0, "high_bidder": "", "background": True},
        {"t": 0.9, "kind": "action", "user": "mrudul", "op": "bid", "auction_id": 1, "amount": 160,
         "outcome": "unknown", "reason": "ClusterUnavailable", "high_bid": 0, "high_bidder": "", "background": False},
        {"t": 1.0, "kind": "kill", "node": "n1", "term": 1},
        {"t": 1.01, "kind": "status", "nodes": {"n1": node("", 0, "", "", 0, up=False),
                                                 "n2": node("LEADER", 2, "n2", "d1", 3),
                                                 "n3": node("FOLLOWER", 2, "n2", "d1", 3)}},
        {"t": 1.1, "kind": "election", "leader": "n2", "term": 2},
        {"t": 2.0, "kind": "restart", "node": "n1"},
        {"t": 2.5, "kind": "escrow", "auction_id": 1, "status": "HELD", "amount": 150, "payer": "mrudul",
         "seller": "nisarg", "payment_ref": "PAY-1", "settlement_ref": ""},
        {"t": 3.0, "kind": "close", "auction_id": 1, "winner": "mrudul", "winning_bid": 150},
        {"t": 3.1, "kind": "summary", "auction_id": 1, "text": "mrudul won at 150."},
        {"t": 3.2, "kind": "escrow", "auction_id": 1, "status": "RELEASED", "amount": 150, "payer": "mrudul",
         "seller": "nisarg", "payment_ref": "PAY-1", "settlement_ref": "REL-1"},
        {"t": 3.3, "kind": "trends", "stats": {"auctions": 1, "with_bids": 1, "total_bids": 4},
         "summary": "one auction"},
        {"t": 3.4, "kind": "status", "nodes": {"n1": live1, "n2": node("LEADER", 2, "n2", "d2", 5),
                                                 "n3": node("FOLLOWER", 2, "n2", "d2", 5)}},
        {"t": 3.5, "kind": "checker", "properties": [{"name": "replicas-identical", "passed": True, "checked": 3},
                                                      {"name": "escrow-legal", "passed": False, "checked": 2}],
         "all_passed": False},
    ]


def write_run(tmp_path, events, name="demo-20260101-120000"):
    run = tmp_path / name
    run.mkdir()
    with open(run / "events.jsonl", "w", encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")
    return str(run)


def embedded_data(html_text):
    m = re.search(r'<script id="replay-data" type="application/json">(.*?)</script>', html_text, re.S)
    assert m, "the data block is missing"
    return json.loads(m.group(1))


# --- the state the viewer computes ---------------------------------------------------

def test_the_final_state_matches_the_last_status_escrow_and_checker_in_the_file(tmp_path):
    events = fake_story()
    initial, snapshots = replay.build_snapshots(events)
    final = snapshots[-1]

    last_status = [e for e in events if e["kind"] == "status"][-1]
    assert final["nodes"] == last_status["nodes"]

    last_escrow = [e for e in events if e["kind"] == "escrow"][-1]
    escrow = final["auctions"]["1"]["escrow"]
    assert (escrow["status"], escrow["amount"], escrow["payer"], escrow["seller"], escrow["payment_ref"],
            escrow["settlement_ref"]) == (last_escrow["status"], last_escrow["amount"], last_escrow["payer"],
                                          last_escrow["seller"], last_escrow["payment_ref"],
                                          last_escrow["settlement_ref"])

    checker = [e for e in events if e["kind"] == "checker"][-1]
    assert final["checker"] == {"properties": checker["properties"], "all_passed": checker["all_passed"]}
    assert initial["checker"] is None and initial["nodes"] == {}


def test_leader_replicas_and_the_kill_are_tracked_through_the_run():
    _, snapshots = replay.build_snapshots(fake_story())
    assert snapshots[1]["leader"] == "n1" and snapshots[1]["replicas"] == "identical"
    after_kill = snapshots[11]                 # event 10 is the kill; event 11 is the status after it
    assert after_kill["nodes"]["n1"]["up"] is False and after_kill["leader"] == "n2"
    assert after_kill["replicas"] == "identical"      # live nodes n2 and n3 agree
    assert snapshots[-1]["kills"] == [{"node": "n1", "term": 1, "t": 1.0}]
    assert snapshots[-1]["elections"] == [{"leader": "n2", "term": 2, "t": 1.1}]


def test_the_replicas_indicator_has_four_honest_states():
    up = lambda d, i: {"up": True, "state_digest": d, "digest_index": i}  # noqa: E731
    down = {"up": False}
    assert replay.replicas_state({"n1": up("a", 4), "n2": up("a", 4), "n3": down}) == "identical"
    assert replay.replicas_state({"n1": up("a", 4), "n2": up("b", 4)}) == "diverged"
    assert replay.replicas_state({"n1": up("a", 4), "n2": up("a", 3)}) == "catching up"   # lag, not divergence
    assert replay.replicas_state({"n1": down, "n2": down}) == "none"


def test_background_bids_are_counted_but_never_listed_in_the_hero_auction():
    _, snapshots = replay.build_snapshots(fake_story())
    final = snapshots[-1]
    assert final["bg"] == {"bids": 3, "accepted": 1, "rejected": 1, "errors": 1}
    assert [b["user"] for b in final["bids"]] == ["karan", "mrudul", "tanmay", "mrudul"]
    assert all(b["user"] != "bg1" for b in final["bids"])


def test_a_bid_with_no_answer_does_not_overwrite_the_high_bid():
    # The last hero bid is "unknown" and carries high_bid 0 in the record. The card must keep 150.
    _, snapshots = replay.build_snapshots(fake_story())
    card = snapshots[-1]["auctions"]["1"]
    assert (card["high_bid"], card["high_bidder"]) == (150, "mrudul")
    assert card["closed"] and card["winner"] == "mrudul" and card["winning_bid"] == 150
    assert card["summary"] == "mrudul won at 150."


def test_the_snapshot_at_any_index_equals_a_rebuild_from_the_start():
    events = fake_story()
    _, snapshots = replay.build_snapshots(events)
    for i in (0, 5, 12, len(events) - 1):
        fresh = replay.ReplayState()
        for e in events[: i + 1]:
            fresh.apply(e)
        assert fresh.view() == snapshots[i]


def test_an_unknown_event_kind_is_skipped_not_fatal():
    events = [{"t": 0.0, "kind": "from-a-newer-version", "x": 1}, {"t": 1.0, "kind": "step", "text": "ok"}]
    _, snapshots = replay.build_snapshots(events)
    assert snapshots[-1]["step"] == "ok"


def test_markers_and_stops_come_from_steps_kills_elections_and_escrow():
    data = replay.build_data(fake_story(), "demo-x", "2026-01-01 12:00:00")
    kinds = [m["kind"] for m in data["markers"]]
    assert kinds == ["step", "kill", "election"]
    assert data["stops"] == sorted(data["stops"]) and 2.5 in data["stops"] and 3.2 in data["stops"]
    assert not any(entry["cls"] == "action" and "bg" in entry["text"] for entry in data["log"])


# --- the HTML file --------------------------------------------------------------------

def test_build_writes_one_self_contained_html_file_with_the_banner(tmp_path):
    run = write_run(tmp_path, fake_story())
    out = replay.build_replay(run)
    assert out == os.path.join(run, "replay.html") and os.path.exists(out)
    text = open(out, encoding="utf-8").read()
    assert "REPLAY of a recorded run - demo-20260101-120000 - recorded 2026-01-01 12:00:00" in text
    assert "http://" not in text and "https://" not in text          # no CDN, no fonts, no server
    assert "<link " not in text and "<script src" not in text


def test_the_embedded_data_of_the_built_file_ends_where_the_events_end(tmp_path):
    events = fake_story()
    run = write_run(tmp_path, events)
    data = embedded_data(open(replay.build_replay(run), encoding="utf-8").read())
    assert len(data["snapshots"]) == len(events) == len(data["times"])
    assert data["snapshots"][-1]["checker"]["all_passed"] is False
    assert data["duration"] >= events[-1]["t"]
    assert data["recorded"] == "2026-01-01 12:00:00"


def test_a_run_without_an_event_file_is_a_clear_error(tmp_path):
    run = tmp_path / "demo-20260101-120000"
    run.mkdir()
    assert replay.main([str(run)]) == 1
    assert replay.main([]) == 2


def test_a_broken_line_in_the_event_file_names_the_line(tmp_path):
    run = tmp_path / "demo-20260101-120000"
    run.mkdir()
    (run / "events.jsonl").write_text('{"t": 0.0, "kind": "step", "text": "ok"}\nnot json\n', encoding="utf-8")
    with pytest.raises(ValueError, match="events.jsonl:2"):
        replay.build_replay(str(run))


def test_marker_labels_are_short_and_cut_at_a_word():
    assert replay.marker_label("KILL -9 the leader n1 (term 1) mid-auction") == "KILL -9 the leader n1..."
    assert replay.marker_label("short") == "short"
