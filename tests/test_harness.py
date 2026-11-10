"""Specifies the pure parts of harness/: the seeded fault schedule, the
leader lines in node logs, the dashboard's view of a status poll, and the
bid amounts the workload picks. Plus one test of process suspend/resume/kill
on a real child process.

The workload against a live cluster is in test_workload.py.
"""

import os
import subprocess
import sys
import time

import pytest

from harness import procs, schedule, status, workload
from harness.leader_log import leader_sightings_from_log
from tests.auction_cluster import wait_until

# --- schedule ------------------------------------------------------------------


def test_same_seed_same_schedule():
    assert schedule.make_schedule(42, 600) == schedule.make_schedule(42, 600)
    assert schedule.make_schedule(42, 600) != schedule.make_schedule(43, 600)


def test_schedule_episodes_do_not_overlap_and_fit_in_the_run():
    s = schedule.make_schedule(7, 300)
    assert len(s) >= 10
    assert s[0].at >= schedule.WARMUP
    for prev, cur in zip(s, s[1:]):
        assert cur.at >= prev.at + prev.down_for + schedule.GAP[0] - 1e-9
    assert s[-1].at + s[-1].down_for <= 300
    assert {f.action for f in s} <= set(schedule.ACTIONS)
    assert all(0 <= f.pick < 1 for f in s)


def test_a_long_schedule_uses_every_kind_of_fault():
    actions = {f.action for f in schedule.make_schedule(1, 600)}
    assert actions == set(schedule.ACTIONS)


def test_schedule_prints_one_line_per_fault():
    s = schedule.make_schedule(3, 60)
    text = schedule.describe(s)
    assert len(text.splitlines()) == len(s)
    assert s[0].action in text.splitlines()[0]


def test_pick_resolves_the_same_way_for_the_same_candidates():
    assert schedule.resolve_pick(0.0, ["n3", "n1"]) == "n1"
    assert schedule.resolve_pick(0.99, ["n3", "n1"]) == "n3"
    assert schedule.resolve_pick(0.5, []) is None


# --- leader lines in node logs -------------------------------------------------


def test_leader_lines_are_found_in_a_node_log():
    text = (
        "12:00:01.000 [n1] storage data/n1: term 0, votedFor None, 0 log entries\n"
        "12:00:02.000 [n1] term 1 FOLLOWER -> CANDIDATE (was term 0): election timeout\n"
        "12:00:02.010 [n1] term 1 CANDIDATE -> LEADER: won election with votes from n1, n2\n"
        "12:00:09.000 [n1] term 3 LEADER -> FOLLOWER (was term 1): saw term 3\n"
        "12:00:19.000 [n1] term 5 CANDIDATE -> LEADER: won\n"
    )
    assert leader_sightings_from_log(text, "log:n1") == [
        {"term": 1, "node": "n1", "source": "log:n1"},
        {"term": 5, "node": "n1", "source": "log:n1"},
    ]


# --- status view ---------------------------------------------------------------


def st(node, role="FOLLOWER", term=1, leader="n1", index=5, d="aa", commit=5):
    return status.Status(node_id=node, role=role, term=term, leader_id=leader, voted_for="",
                         commit_index=commit, last_applied=index, last_log_index=commit,
                         last_log_term=term, state_digest=d, digest_index=index)


def test_agreement_when_every_reachable_node_has_one_index_and_one_digest():
    view = {"n1": st("n1", "LEADER"), "n2": st("n2"), "n3": None}
    assert status.agreement(view) == "agree"
    view["n2"] = st("n2", index=4, d="bb")
    assert status.agreement(view) == "catching up"
    view["n2"] = st("n2", d="bb")
    assert status.agreement(view) == "DIVERGED"
    assert status.agreement({"n1": None}) == "no data"


def test_events_for_down_up_and_leader_change():
    a = {"n1": st("n1", "LEADER"), "n2": st("n2"), "n3": st("n3")}
    b = {"n1": None, "n2": st("n2", "CANDIDATE", term=2, leader=""), "n3": st("n3", term=2, leader="")}
    c = {"n1": None, "n2": st("n2", "LEADER", term=2, leader="n2"), "n3": st("n3", term=2, leader="n2")}
    d = {"n1": st("n1", term=2, leader="n2"), "n2": c["n2"], "n3": c["n3"]}
    assert status.events(a, b) == ["n1 DOWN", "no leader (was n1, term 1)"]
    assert status.events(b, c) == ["leader n2 (term 2)"]
    assert status.events(c, d) == ["n1 UP"]
    assert status.events(d, d) == []


def test_leaders_seen_in_a_poll():
    view = {"n1": st("n1", "LEADER", term=1), "n2": st("n2", "LEADER", term=2), "n3": None}
    assert status.leader_sightings(view) == [(1, "n1"), (2, "n2")]
    assert status.current_leader(view) == "n2"  # highest term wins
    assert status.current_leader({"n1": st("n1"), "n2": None}) is None


# --- bid amounts ---------------------------------------------------------------


def test_bid_amounts_keep_their_residue_and_always_rise():
    rng = __import__("random").Random(0)
    modulus = 7
    for residue in range(1, modulus):
        last = 0
        for high in (0, 3, 3, 50, 50, 51, 1000):
            a = workload.next_amount(rng, high, last, residue, modulus)
            assert a % modulus == residue
            assert a > high and a > last
            last = a


def test_race_amounts_use_residue_zero():
    for high in (0, 1, 13, 14):
        a = workload.race_amount(high, 7)
        assert a % 7 == 0 and a > high


# --- process control ------------------------------------------------------------

COUNTER = """
import sys, time
path = sys.argv[1]
n = 0
while True:
    n += 1
    with open(path, "w") as f:
        f.write(str(n))
    time.sleep(0.01)
"""


def read_count(path):
    try:
        with open(path) as f:
            return int(f.read() or 0)
    except (OSError, ValueError):
        return 0


def test_suspend_stops_a_process_and_resume_restarts_it(tmp_path):
    """Timing: the child counts every 10 ms. While suspended for 0.5 s it
    must not move at all (allowing one write that was in progress at the
    moment of the suspend); after resume it must move again within 5 s.
    Generous bounds; the only way this flakes is a machine so loaded that
    the child does not get scheduled for 5 s."""
    path = str(tmp_path / "count")
    p = subprocess.Popen([sys.executable, "-c", COUNTER, path])
    try:
        assert wait_until(lambda: read_count(path) > 3, 10)
        procs.suspend(p.pid)
        time.sleep(0.1)
        frozen = read_count(path)
        time.sleep(0.5)
        assert read_count(path) - frozen <= 1
        procs.resume(p.pid)
        assert wait_until(lambda: read_count(path) > frozen + 3, 5)
    finally:
        procs.hard_kill(p)
    assert p.poll() is not None


def test_a_suspended_process_can_be_killed(tmp_path):
    p = subprocess.Popen([sys.executable, "-c", COUNTER, str(tmp_path / "c")])
    try:
        time.sleep(0.2)
        procs.suspend(p.pid)
    finally:
        procs.hard_kill(p)
    assert p.poll() is not None


@pytest.mark.skipif(os.name != "nt", reason="Windows exit code of TerminateProcess")
def test_hard_kill_is_terminate_process_on_windows(tmp_path):
    p = subprocess.Popen([sys.executable, "-c", COUNTER, str(tmp_path / "c")])
    procs.hard_kill(p)
    assert p.returncode == 1  # Popen.kill() == TerminateProcess(handle, 1): no cleanup ran


@pytest.mark.skipif(os.name != "nt" or sys.executable == getattr(sys, "_base_executable", sys.executable),
                    reason="only a Windows venv python is a launcher")
def test_the_venv_python_is_a_launcher_and_the_tree_finds_the_interpreter(tmp_path):
    """Why procs acts on process trees: Popen(venv python).pid is the
    launcher; the code runs in its child. Suspending the pid alone froze
    nothing (this test's first version failed: 13 -> 57 while 'suspended')."""
    p = subprocess.Popen([sys.executable, "-c", COUNTER, str(tmp_path / "c")])
    try:
        assert wait_until(lambda: len(procs.process_tree(p.pid)) >= 2, 10)
    finally:
        procs.hard_kill(p)


# --- process trees and PID reuse --------------------------------------------------
#
# Found by the second 10-minute chaos run (seed 79402): the harness killed
# ITSELF at 562 s (exit code 1 = our TerminateProcess, no traceback). A
# process's recorded parent PID is never updated when the parent exits, so a
# NEW process that is handed a recycled PID "adopts" every orphan that names
# that PID as parent. Killing that new node's "tree" then killed unrelated
# processes -- the harness's own launcher among them.


def test_a_recycled_pid_does_not_adopt_older_orphans():
    # pid 100: the node launcher just started (t=50), recycling a PID that a
    # dead process used to have. pid 7 (t=10) names 100 as parent: an orphan
    # of the OLD pid 100, created before the new one existed.
    parent = {100: 1, 101: 100, 7: 100, 8: 7}
    created = {100: 50.0, 101: 51.0, 7: 10.0, 8: 11.0}
    assert procs.descendants(100, parent, created) == [100, 101]


def test_real_descendants_are_all_found():
    parent = {100: 1, 101: 100, 102: 101, 103: 100}
    created = {100: 1.0, 101: 2.0, 102: 3.0, 103: 4.0}
    assert sorted(procs.descendants(100, parent, created)) == [100, 101, 102, 103]


def test_the_tree_never_contains_the_harness_or_its_launcher():
    me, my_launcher = os.getpid(), os.getppid()
    parent = {100: 1, my_launcher: 100, me: my_launcher}
    created = {100: 1.0, my_launcher: 2.0, me: 3.0}  # even if the times say "child"
    assert procs.descendants(100, parent, created, exclude={me, my_launcher}) == [100]
