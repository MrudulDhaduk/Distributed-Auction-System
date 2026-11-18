"""A dashboard scenario's event log and replay (dashboard/scenarios.py):
every client write, status poll and node action goes into events.jsonl in
the scripted demo's format, the final state adds only THIS run's auctions,
and scripts/replay.py turns the file into replay.html."""

import json
import os

from auction.state import CreateAuction, PlaceBid
from common import time_conv
from dashboard.scenarios import ScenarioRun, _load_replay_module
from replication import codec, digest
from replication.state_machine import StateMachine
from tests.test_validation_rules import LATER, T0


class Dash:
    client_addrs = ["127.0.0.1:1"]
    nodes = {}

    def __init__(self, run_dir):
        self.run_dir = run_dir

    def event(self, *a, **k):
        pass


def dumps_of(sm):
    index, snap = sm.indexed_snapshot()
    text = digest.canonical_json(snap)
    return {"n1": {"node_id": "n1", "digest_index": index, "state_digest": digest.digest_of_json(text),
                   "state_json": text}}


def events_of(run):
    with open(os.path.join(run.run_dir, "events.jsonl"), encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def test_the_run_log_has_writes_status_and_only_its_own_auctions(tmp_path):
    run = ScenarioRun(Dash(str(tmp_path)), "clock", 1)
    run.say("logging in")
    run.history.append(dict(client="karan", client_id="c", user="karan", seq=1, op="bid", outcome="ok",
                            reason="Bid of 100 added", start=0, end=0, auction_id=2, amount=100,
                            high_bid=100, high_bidder="karan"))
    run.on_status({"n1": {"role": "LEADER", "term": 1, "leader_id": "n1", "commit_index": 3, "last_applied": 3,
                          "last_log_index": 3, "last_log_term": 1, "state_digest": "abc", "digest_index": 3},
                   "n2": None})
    sm = StateMachine()
    for i, cmd in enumerate([CreateAuction(1, "old lamp", LATER, "nisarg", ""),       # from before the run
                             CreateAuction(2, "vintage clock", LATER, "nisarg", ""),
                             PlaceBid(2, "karan", 100, T0)], start=1):
        sm.apply_fn(i, codec.encode(cmd))
    run._record_final_state(dumps_of(sm))
    run.events.close()

    events = events_of(run)
    kinds = [e["kind"] for e in events]
    assert {"step", "action", "status", "auction"} <= set(kinds)
    assert [e["auction_id"] for e in events if e["kind"] == "auction"] == [2]  # not the old lamp
    action = next(e for e in events if e["kind"] == "action")
    assert action["user"] == "karan" and action["background"] is False and action["high_bid"] == 100
    status = next(e for e in events if e["kind"] == "status")
    assert status["nodes"]["n1"]["up"] and not status["nodes"]["n2"]["up"]


def test_chaos_writes_are_marked_background(tmp_path):
    run = ScenarioRun(Dash(str(tmp_path)), "chaos", 2, duration=60)
    assert run.is_background("karan") and run.is_background("bg1")
    demo = ScenarioRun(Dash(str(tmp_path)), "demo", 3)
    assert demo.is_background("bg4") and not demo.is_background("karan")
    run.events.close()
    demo.events.close()


def test_the_replay_page_is_built_from_the_log(tmp_path):
    run = ScenarioRun(Dash(str(tmp_path)), "clock", 4)
    run.say("start")
    run.record("auction", auction_id=2, item="vintage clock", close_time=time_conv.dt_to_micros(LATER),
               description="")
    run.record("kill", node="n1", term=1)
    run.record("election", leader="n2", term=2)
    run.record("checker", all_passed=True, properties=[{"name": "replicas-identical", "passed": True, "checked": 3}])
    run.events.close()
    out = _load_replay_module().build_replay(run.run_dir)
    page = open(out, encoding="utf-8").read()
    assert out.endswith("replay.html") and "REPLAY of a recorded run" in page and "scenario-4-clock" in page
