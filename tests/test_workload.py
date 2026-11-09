"""The chaos workload end to end, in one process but over REAL gRPC: three
AuctionNodes (FileStorage), the harness Workload (sellers, bidders, racers,
each its own ClusterClient), a StatusMonitor, a leader kill mid-run, and
then the property checker on the recorded history and the final dumps.

This is a short, small version of scripts/chaos.py, so the suite proves
the harness and the checker agree on a real run, not just on hand-built
inputs.

Timing: the assertions that MUST hold (every checker property) do not
depend on timing at all. The ones that need something to have happened
(some bids accepted, a race decided, an auction closed) use a 9 s run with
auctions closing after 2-4 s; on a machine too slow to commit a handful of
writes in 9 s they would fail. That is a liveness bound, not a race.
"""

import json
import time

from checker import properties as P
from checker.properties import check_all
from harness.status import StatusMonitor, StatusPoller, agreement
from harness.workload import Workload
from tests.auction_cluster import wait_until
from tests.test_serve_grpc import FIRST, HOST, grpc_cluster  # noqa: F401 (fixture)

IDS = ("n1", "n2", "n3")


def test_workload_with_a_leader_kill_passes_the_checker(grpc_cluster):  # noqa: F811
    c = grpc_cluster()
    c.boot("n1", **FIRST)
    c.boot("n2")
    c.boot("n3")
    assert wait_until(lambda: c.leader() is not None, 10)
    poller = StatusPoller({n: f"{HOST}:{c.addrs[n].peer_port}" for n in IDS})
    monitor = StatusMonitor(poller, interval=0.05).start()
    w = Workload([c.client_addr(n) for n in IDS], seed=11, sellers=1, bidders=3, racers=2,
                 close_after=(2.0, 4.0), max_open=3, llm_fraction=0.5, race_every=(0.2, 0.5),
                 think=(0.0, 0.05), give_up_after=10.0)
    try:
        w.start()
        time.sleep(3)
        killed = c.leader().node_id
        c.kill(killed)
        time.sleep(3)
        c.boot(killed)
        time.sleep(3)
        assert w.stop() == []
        assert w.errors == []

        # Quiet: wait until every node reports the same index and digest,
        # and the last auction's close has been applied.
        assert wait_until(lambda: agreement(poller.poll()) == "agree"
                          and all(s and s.commit_index == s.last_applied for s in poller.poll().values()), 15)
        time.sleep(4.5)  # every auction closes <= 4 s after creation; the closer checks every 0.25 s
        assert wait_until(lambda: agreement(poller.poll()) == "agree", 10)
        dumps = {}
        for n in IDS:
            d = poller.dump(n)
            dumps[n] = {"node_id": d.node_id, "digest_index": d.digest_index,
                        "state_digest": d.state_digest, "state_json": d.state_json}
    finally:
        monitor.stop()
        poller.close()

    history = w.history.snapshot()
    report = check_all(history, dumps, monitor.leader_records())
    assert all(r.passed for r in report), P.format_report(report)

    bids = [r for r in history if r["op"] == "bid"]
    assert sum(r["outcome"] == "ok" for r in bids) >= 5
    races = {}
    for r in bids:
        if r.get("race"):
            races.setdefault((r["auction_id"], r["amount"]), []).append(r["outcome"])
    decided = [o for o in races.values() if len(o) >= 2 and "ok" in o]
    assert decided, f"no race was decided: {races}"
    for outcomes in decided:
        assert outcomes.count("ok") == 1  # equal bids: exactly one wins
    doc = json.loads(dumps["n1"]["state_json"])
    assert any(a["closed"] for a in doc["auctions"])
    assert len({rec["term"] for rec in monitor.leader_records()}) >= 2  # there was a failover
