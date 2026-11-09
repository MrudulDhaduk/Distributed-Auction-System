"""Specifies RaftNode.read_index(timeout): serving a read without writing
anything to the log, and without returning stale data.

Raft paper, section 8: "Read-only operations can be handled without
writing anything into the log. However, with no additional measures, this
would run the risk of returning stale data, since the leader responding to
the request might have been superseded by a newer leader of which it is
unaware. [...] First, a leader must have the latest information on which
entries are committed. [...] Raft handles this by having each leader
commit a blank no-op entry into the log at the start of its term. Second,
a leader must check whether it has been deposed before processing a
read-only request [...] by having the leader exchange heartbeat messages
with a majority of the cluster before responding to read-only requests."

read_index(timeout) does, in order:
  1. refuse if not leader (NOT_LEADER, with a hint);
  2. wait until this term's no-op is committed (precaution one);
  3. record readIndex = commitIndex;
  4. start a new heartbeat round and wait until a majority (self included)
     has answered an AppendEntries SENT AFTER step 3 with our term
     (precaution two);
  5. wait until lastApplied >= readIndex;
then returns OK with readIndex. The caller reads its state machine.

Timing policy as in the other Raft tests: who leads is chosen by giving one
node a short election timeout; the "deposed" and "lagging" situations are
built by a message filter (it only ever LOSES messages, which Raft must
tolerate), not by racing. Every wait is bounded.
"""

import random
import threading

import pytest

from raft.messages import Transport, TransportError
from raft.node import RaftNode, ReadOutcome, Role
from raft.transport import InMemoryTransport
from tests.test_raft_replication import BOUND, LEADER_FIRST, NEVER, wait_until


class Filter(Transport):
    """Loses every AppendEntries for which drop(args) is true."""

    def __init__(self, net):
        self._net = net
        self.drop = lambda args: False

    def request_vote(self, to, args):
        return self._net.request_vote(to, args)

    def append_entries(self, to, args):
        if self.drop(args):
            raise TransportError(f"filtered {args.from_id} -> {to}")
        return self._net.append_entries(to, args)


class Nodes:
    def __init__(self, ids=("n1", "n2", "n3"), first="n1"):
        self.net = InMemoryTransport()
        self.filter = Filter(self.net)
        self.applied = {nid: [] for nid in ids}
        self.nodes = {}
        for i, nid in enumerate(ids):
            node = RaftNode(
                nid, [p for p in ids if p != nid], self.filter,
                rng=random.Random(i),
                apply_fn=lambda index, data, nid=nid: self.applied[nid].append(data),
                **(LEADER_FIRST if nid == first else NEVER),
            )
            self.nodes[nid] = node
            self.net.register(node)
        for node in self.nodes.values():
            node.start()

    def stop(self):
        for node in self.nodes.values():
            node.stop()

    def campaign(self, nid):
        node = self.nodes[nid]
        with node._lock:
            node._election_timeout = LEADER_FIRST["election_timeout"]
            node._reset_election_timer()

    def wait_leader(self, nid, above_term=0):
        node = self.nodes[nid]
        assert wait_until(lambda: node.status().role == Role.LEADER and node.status().term > above_term)
        return node


@pytest.fixture
def nodes():
    made = []

    def make(**kw):
        n = Nodes(**kw)
        made.append(n)
        return n

    yield make
    for n in made:
        n.stop()


def propose_ok(node, data):
    h = node.propose(data)
    assert h.wait(BOUND) and h.ok, h
    return h


def in_thread(fn):
    out = {}
    t = threading.Thread(target=lambda: out.setdefault("r", fn()), daemon=True)
    t.start()
    return t, out


# --- the basics ---------------------------------------------------------------


def test_a_read_after_a_write_covers_the_write(nodes):
    c = nodes()
    leader = c.wait_leader("n1")
    h = propose_ok(leader, b"x")

    r = leader.read_index(BOUND)

    assert r.outcome is ReadOutcome.OK
    assert r.index >= h.index
    assert leader.status().last_applied >= r.index
    assert b"x" in c.applied["n1"]


def test_a_follower_refuses_with_a_hint(nodes):
    c = nodes()
    c.wait_leader("n1")
    follower = c.nodes["n2"]
    assert wait_until(lambda: follower.status().leader_id == "n1")

    r = follower.read_index(BOUND)

    assert (r.outcome, r.leader_hint) == (ReadOutcome.NOT_LEADER, "n1")


def test_a_cluster_of_one_reads_on_its_own(nodes):
    c = nodes(ids=("solo",), first="solo")
    leader = c.wait_leader("solo")
    propose_ok(leader, b"x")

    assert leader.read_index(BOUND).outcome is ReadOutcome.OK


def test_read_index_writes_nothing_to_the_log(nodes):
    c = nodes()
    leader = c.wait_leader("n1")
    propose_ok(leader, b"x")
    before = leader.log_entries()

    for _ in range(5):
        assert leader.read_index(BOUND).outcome is ReadOutcome.OK

    assert leader.log_entries() == before


# --- precaution two: a deposed leader that has not noticed ------------------------


def test_a_deposed_leader_that_has_not_noticed_cannot_serve_a_read(nodes):
    """n1 leads term 1 and is cut off. n2 is elected in term 2 and commits
    "new". n1 has heard nothing, still believes it leads term 1, and its
    state does not have "new". Answering a read from that state would be
    stale. It must not answer."""
    c = nodes()
    n1 = c.wait_leader("n1")
    propose_ok(n1, b"old")
    c.net.partition(["n1"], ["n2", "n3"])
    c.campaign("n2")
    n2 = c.wait_leader("n2", above_term=1)
    propose_ok(n2, b"new")
    assert n1.status().role == Role.LEADER  # it really has not noticed

    r = n1.read_index(0.5)

    assert r.outcome is ReadOutcome.TIMEOUT
    assert b"new" not in c.applied["n1"]
    # Meanwhile the real leader serves the read, and it includes "new".
    assert n2.read_index(BOUND).outcome is ReadOutcome.OK
    assert b"new" in c.applied["n2"]


def test_a_leader_deposed_while_waiting_reports_leadership_lost(nodes):
    c = nodes()
    n1 = c.wait_leader("n1")
    c.net.partition(["n1"], ["n2", "n3"])
    c.campaign("n2")
    c.wait_leader("n2", above_term=1)

    t, out = in_thread(lambda: n1.read_index(BOUND))
    c.net.heal()  # n1 hears term 2 in the next reply and steps down
    t.join(BOUND)

    assert out["r"].outcome is ReadOutcome.LEADERSHIP_LOST
    assert out["r"].leader_hint in (None, "n2")


# --- precaution one: a new leader learns its commitIndex first --------------------


def test_a_new_leader_waits_for_its_no_op_before_choosing_the_read_index(nodes):
    """n1 commits "a" with n3's help. n2 also stores "a" but never hears
    that it committed (every AppendEntries to n2 carrying that commitIndex
    is lost). n1 dies; n2 leads term 2 with commitIndex BELOW "a".
    A read on n2 must include "a" -- a client was told "a" succeeded. If n2
    took commitIndex as the read index right away, it would not.

    n2's no-op is held back (n2 -> n3 lost) while the read starts, so the
    read cannot finish early by luck; then it is let through."""
    c = nodes()
    n1 = c.wait_leader("n1")
    n2 = c.nodes["n2"]
    a_index = n1.status().last_log_index + 1
    c.filter.drop = lambda args: args.from_id == "n1" and args.to == "n2" and args.commit_index >= a_index
    propose_ok(n1, b"a")
    assert wait_until(lambda: n2.status().last_log_index >= a_index)
    assert n2.status().commit_index < a_index

    c.net.drop("n1")
    c.filter.drop = lambda args: args.from_id == "n2" and args.to == "n3" and len(args.entries) > 0
    c.campaign("n2")
    c.wait_leader("n2", above_term=1)
    assert n2.status().commit_index < a_index

    t, out = in_thread(lambda: n2.read_index(BOUND))
    t.join(0.3)
    assert t.is_alive(), "answered before its no-op committed"

    c.filter.drop = lambda args: False
    t.join(BOUND)

    r = out["r"]
    assert r.outcome is ReadOutcome.OK
    assert r.index >= a_index
    assert b"a" in c.applied["n2"]


def test_a_read_times_out_without_a_majority(nodes):
    c = nodes()
    n1 = c.wait_leader("n1")
    c.net.drop("n2")
    c.net.drop("n3")

    assert n1.read_index(0.3).outcome is ReadOutcome.TIMEOUT


def test_a_stopped_node_reports_stopped(nodes):
    c = nodes()
    n1 = c.wait_leader("n1")
    n1.stop()

    assert n1.read_index(0.3).outcome is ReadOutcome.STOPPED
