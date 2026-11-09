"""Specifies harness/run.py's inject() and heal(): which node a fault hits,
given who leads at that moment, and that heal() undoes exactly that.

Fake nodes and a fake poller: no processes, no timing.
"""

from harness import run as R
from harness.schedule import Fault
from tests.test_harness import st


class FakeNode:
    def __init__(self):
        self.state = "up"
        self.alive = True

    def kill(self):
        self.state, self.alive = "dead", False

    def start(self):
        self.state, self.alive = "up", True

    def suspend(self):
        self.state = "suspended"

    def resume(self):
        self.state = "up"


class FakeCluster:
    def __init__(self):
        self.nodes = {n: FakeNode() for n in ("n1", "n2", "n3")}


class FakePoller:
    def __init__(self, leader):
        self.leader = leader

    def poll(self):
        return {n: st(n, "LEADER" if n == self.leader else "FOLLOWER", leader=self.leader)
                for n in ("n1", "n2", "n3")}


def fault(action, pick=0.0):
    return Fault(at=0, action=action, down_for=1, pick=pick)


def run(action, leader="n2", pick=0.0):
    c = FakeCluster()
    done = R.inject(c, FakePoller(leader), fault(action, pick), say=lambda _: None)
    return c, done


def test_kill_leader_kills_whoever_leads_now():
    c, done = run("kill_leader", leader="n2")
    assert done == [("n2", "killed")]
    assert c.nodes["n2"].state == "dead"


def test_kill_follower_never_hits_the_leader_and_pick_chooses_which():
    assert run("kill_follower", leader="n2", pick=0.0)[1] == [("n1", "killed")]
    assert run("kill_follower", leader="n2", pick=0.9)[1] == [("n3", "killed")]


def test_suspend_leader_and_follower():
    assert run("suspend_leader", leader="n3")[1] == [("n3", "suspended")]
    assert run("suspend_follower", leader="n3", pick=0.9)[1] == [("n2", "suspended")]


def test_kill_two_takes_the_leader_and_one_more():
    c, done = run("kill_two", leader="n1", pick=0.9)
    assert done == [("n1", "killed"), ("n3", "killed")]
    assert [n for n, node in c.nodes.items() if node.state == "up"] == ["n2"]


def test_heal_undoes_exactly_what_was_done():
    c, done = run("kill_two", leader="n1")
    R.heal(c, done, say=lambda _: None)
    assert all(node.state == "up" for node in c.nodes.values())
    c, done = run("suspend_leader", leader="n1")
    R.heal(c, done, say=lambda _: None)
    assert all(node.state == "up" for node in c.nodes.values())


def test_a_node_already_down_is_not_a_candidate():
    c = FakeCluster()
    c.nodes["n1"].kill()
    done = R.inject(c, FakePoller("n2"), fault("kill_follower", 0.0), say=lambda _: None)
    assert done == [("n3", "killed")]


class ViewPoller:
    def __init__(self, view):
        self.view = view

    def poll(self):
        return self.view


def test_settled_allows_a_small_apply_lag_while_writes_continue():
    """quiet() needs identical digests for 1 s, which never happens while
    a writer is committing (the failover stress run skipped every kill
    because of that). settled() is the condition for 'ready to kill the
    leader': all up, all agree who leads, everyone within max_lag."""
    view = {"n1": st("n1", "LEADER", commit=100, index=100), "n2": st("n2", commit=99, index=98),
            "n3": st("n3", commit=100, index=97)}
    assert R.settled(ViewPoller(view), max_lag=10, stable_for=0, timeout=1)
    assert not R.quiet(ViewPoller(view), timeout=0.3, stable_for=0)


def test_settled_refuses_a_lagging_down_or_confused_node():
    up = {"n1": st("n1", "LEADER", commit=100, index=100), "n2": st("n2", commit=100, index=100)}
    assert not R.settled(ViewPoller(dict(up, n3=st("n3", commit=40, index=40))), max_lag=10, stable_for=0, timeout=0.3)
    assert not R.settled(ViewPoller(dict(up, n3=None)), max_lag=10, stable_for=0, timeout=0.3)
    assert not R.settled(ViewPoller(dict(up, n3=st("n3", leader="n3", commit=100, index=100))),
                         max_lag=10, stable_for=0, timeout=0.3)
