"""Specifies what persistence buys the Raft node (build order step 6):
crash a node, build a brand-new RaftNode from its data directory, and the
Figure 2 persistent state -- currentTerm, votedFor, log[] -- is back, while
the volatile state (commitIndex, lastApplied) starts from 0 again.

A "crash" here: the node is cut off the network at once, its threads stop,
and the RaftNode object is thrown away. Nothing it held in memory reaches
the next incarnation; only the files do. (FileStorage durability without
close() is tested in test_raft_file_storage.py; here the dead node's
storage is fenced off, see Cluster.crash.)

The two headline tests run twice: with FileStorage, and with MemoryStorage,
where a restart gets a fresh, empty MemoryStorage -- what restarting an
in-memory process really gives you. The MemoryStorage runs are marked
xfail(strict=True): they MUST fail, every run, or the suite goes red. That
is the proof that the test detects the bug persistence fixes.

Timing policy: as in test_raft_replication.py. One node at a time has a
short election timeout, the others 30 s, so who campaigns is decided by the
test, not by a race. Every wait is wait_until(condition, 5 s).
"""

import random
import threading
from collections import defaultdict

import pytest

from raft.messages import RequestVoteArgs
from raft.node import RaftNode, Role
from raft.storage import MemoryStorage
from raft.transport import InMemoryTransport
from tests.test_raft_replication import wait_until  # same helper, same timing policy

FAST = dict(election_timeout=(0.15, 0.30), heartbeat_interval=0.03)
LEADER_FIRST = dict(election_timeout=(0.05, 0.10), heartbeat_interval=0.03)
NEVER = dict(election_timeout=(30.0, 31.0), heartbeat_interval=0.03)
BOUND = 5.0


@pytest.fixture(
    params=[
        "file",
        pytest.param(
            "memory",
            marks=pytest.mark.xfail(
                strict=True, reason="without persistence a restart forgets term, vote and log"
            ),
        ),
    ]
)
def boot_storage(request, open_storage):
    """boot_storage(nid) -> the storage a (re)starting node gets."""
    if request.param == "file":
        return open_storage
    return lambda nid: MemoryStorage()


class Cluster:
    """Nodes on one InMemoryTransport that can be crashed and rebooted.
    `applied[nid]` is what the CURRENT incarnation of nid has applied: a
    reboot starts it empty, like a fresh state machine in a new process."""

    def __init__(self, boot_storage, ids=("n1", "n2", "n3")):
        self.net = InMemoryTransport()
        self.ids = list(ids)
        self.boot_storage = boot_storage
        self.nodes = {}
        self.storages = {}
        self.applied = {}
        self.leaders_by_term = defaultdict(set)
        self._lock = threading.Lock()
        self._boots = 0

    def boot(self, nid, start=True, **config):
        self._boots += 1
        storage = self.boot_storage(nid)
        applied = []
        lock = self._lock

        def apply_fn(index, data):
            with lock:
                applied.append((index, data))

        node = RaftNode(
            nid,
            [p for p in self.ids if p != nid],
            self.net,
            rng=random.Random(self._boots),
            observer=self._observe,
            storage=storage,
            apply_fn=apply_fn,
            **{**FAST, **config},
        )
        with self._lock:
            self.applied[nid] = applied
        self.nodes[nid], self.storages[nid] = node, storage
        self.net.register(node)  # replaces the dead incarnation
        self.net.restore(nid)
        if start:
            node.start()
        return node

    def crash(self, nid):
        """kill -9, as far as the rest of the cluster can tell."""
        self.net.drop(nid)  # unreachable from now on
        node = self.nodes.pop(nid)
        node.stop()
        # Fence the dead incarnation's storage. An RPC that was routed to
        # it just before the drop may still be inside its handler; it must
        # not write to the files after the next incarnation opens them (in
        # a real crash the process is gone, so this cannot happen). Taking
        # the node lock waits for such a handler; close() makes every later
        # write raise. close() does NOT flush anything: FileStorage has
        # nothing unflushed, every write was fsync'd before it returned.
        with node._lock:
            close = getattr(self.storages.pop(nid), "close", None)
            if close:
                close()

    def stop(self):
        for nid in list(self.nodes):
            self.crash(nid)

    def _observe(self, event):
        with self._lock:
            if event.new_role == Role.LEADER:
                self.leaders_by_term[event.new_term].add(event.node_id)

    def applied_data(self, nid):
        with self._lock:
            return [d for _, d in self.applied[nid]]

    def applied_pairs(self, nid):
        with self._lock:
            return list(self.applied[nid])

    def wait_for_leader(self, among=None, above_term=0):
        def find():
            for nid, node in self.nodes.items():
                st = node.status()
                if (among is None or nid in among) and st.role == Role.LEADER and st.term > above_term:
                    return node
            return None

        assert wait_until(lambda: find() is not None, BOUND), "no leader emerged in time"
        return find()

    def wait_applied(self, nids, data):
        assert wait_until(lambda: all(self.applied_data(n) == data for n in nids), BOUND), {
            n: self.applied_data(n) for n in nids
        }

    def assert_one_leader_per_term(self):
        with self._lock:
            bad = {t: ls for t, ls in self.leaders_by_term.items() if len(ls) > 1}
        assert not bad, f"Election Safety broken: {bad}"


@pytest.fixture
def make_cluster():
    made = []

    def make(boot_storage):
        c = Cluster(boot_storage)
        made.append(c)
        return c

    yield make
    for c in made:
        c.stop()


def propose_ok(node, data):
    h = node.propose(data)
    assert h.wait(BOUND) and h.ok, h
    return h


# --- votedFor: one vote per term, across a crash -------------------------------


def test_a_restarted_node_does_not_vote_twice_in_one_term(boot_storage):
    """Figure 2, RequestVote receiver step 2: grant only "If votedFor is
    null or candidateId". Election Safety (at most one leader per term)
    needs that to hold across a crash: if n3 forgets it voted for n1 in
    term 5, it can also vote for n2 in term 5, and with one more vote each
    n1 and n2 are both leaders of term 5."""
    peers = ["n1", "n2"]
    n3 = RaftNode("n3", peers, InMemoryTransport(), storage=boot_storage("n3"))
    assert n3.handle_request_vote(RequestVoteArgs("n1", "n3", 5, 0, 0)).vote_granted

    # Crash: the node object is gone. A new process boots from n3's data.
    n3 = RaftNode("n3", peers, InMemoryTransport(), storage=boot_storage("n3"))
    reply = n3.handle_request_vote(RequestVoteArgs("n2", "n3", 5, 0, 0))

    assert not reply.vote_granted, "voted for n1 AND n2 in term 5"
    st = n3.status()
    assert (st.term, st.voted_for) == (5, "n1")


# --- chunk 2's known gap 1: an acknowledged write lost on restart ------------------


def test_an_acknowledged_write_survives_a_restart_then_the_leaders_crash(boot_storage, make_cluster):
    """docs/raft-replication.md, known gap 1, as a test. n1 commits x on n1
    and n2 (n3 is down), and the client is told APPLIED. n2 restarts. n1
    crashes before it can re-send anything. n3 comes back. Now n2 and n3
    are the only majority there is.

    With persistence, n2 still holds x, so n3's log (empty) is not "at
    least as up-to-date" (5.4.1) and n2 must be the one to win. Its no-op
    commits x, and both apply it. Without persistence n2 is empty too,
    one of them wins, and x -- acknowledged to a client -- is gone."""
    c = make_cluster(boot_storage)
    c.boot("n3", **NEVER)
    c.net.drop("n3")  # down before anyone can campaign
    c.boot("n1", **LEADER_FIRST)
    c.boot("n2", **NEVER)
    leader = c.wait_for_leader()
    assert leader.node_id == "n1"
    propose_ok(leader, b"x")  # APPLIED: committed on n1 + n2

    c.crash("n2")
    c.crash("n1")
    c.boot("n2", **LEADER_FIRST)  # from its data directory
    c.net.restore("n3")

    c.wait_for_leader(among={"n2", "n3"})
    c.wait_applied(["n2", "n3"], [b"x"])
    c.assert_one_leader_per_term()


# --- restart = replay ------------------------------------------------------------


def test_a_restarted_follower_replays_committed_entries_in_order(open_storage, make_cluster):
    """Figure 2: commitIndex and lastApplied are VOLATILE, "initialized to
    0". So a restarted node has its whole log but has applied nothing, and
    its state machine is empty. It re-applies from index 1 once a leader
    tells it the commitIndex -- the same entries, in the same order, at
    the same indexes: same log, same state."""
    c = make_cluster(open_storage)
    c.boot("n1", **LEADER_FIRST)
    c.boot("n2", **NEVER)
    c.boot("n3", **NEVER)
    leader = c.wait_for_leader()
    for d in (b"a", b"b", b"c"):
        propose_ok(leader, d)
    c.wait_applied(["n1", "n2", "n3"], [b"a", b"b", b"c"])
    before = c.applied_pairs("n3")
    old = c.nodes["n3"].status()

    c.crash("n3")
    n3 = c.boot("n3", start=False, **NEVER)
    st = n3.status()
    # Persistent state is back ...
    assert (st.term, st.voted_for, st.last_log_index) == (old.term, old.voted_for, old.last_log_index)
    # ... volatile state is not.
    assert (st.commit_index, st.last_applied) == (0, 0)
    assert c.applied_pairs("n3") == []

    n3.start()
    c.wait_applied(["n3"], [b"a", b"b", b"c"])
    assert c.applied_pairs("n3") == before  # same indexes, same order, once each
    assert n3.status().term == old.term  # n1 is still leader; nothing re-elected


def test_every_committed_entry_survives_restarting_the_whole_cluster(open_storage, make_cluster):
    c = make_cluster(open_storage)
    c.boot("n1", **LEADER_FIRST)
    c.boot("n2", **NEVER)
    c.boot("n3", **NEVER)
    leader = c.wait_for_leader()
    for d in (b"a", b"b", b"c"):
        propose_ok(leader, d)
    c.wait_applied(["n1", "n2", "n3"], [b"a", b"b", b"c"])
    old_term = leader.status().term
    old_logs = {nid: c.nodes[nid].log_entries() for nid in c.ids}

    for nid in c.ids:
        c.crash(nid)
    # A different node campaigns this time: n2. Any of the three can win;
    # all three logs hold every committed entry.
    for nid in c.ids:
        c.boot(nid, **(LEADER_FIRST if nid == "n2" else NEVER))

    new_leader = c.wait_for_leader(above_term=old_term)
    assert new_leader.node_id == "n2"
    # Nothing is applied until the new leader's no-op commits: until then
    # nobody knows what was committed (section 8).
    c.wait_applied(["n1", "n2", "n3"], [b"a", b"b", b"c"])
    propose_ok(new_leader, b"d")
    c.wait_applied(["n1", "n2", "n3"], [b"a", b"b", b"c", b"d"])
    for nid in c.ids:
        # The old log is a prefix of the new one: nothing lost, nothing moved.
        assert c.nodes[nid].log_entries()[: len(old_logs[nid])] == old_logs[nid]
    c.assert_one_leader_per_term()


# --- durable before replying ---------------------------------------------------------


def test_no_vote_is_granted_unless_it_was_made_durable(open_storage, monkeypatch):
    """If the write of votedFor fails, the handler raises instead of
    returning a reply, so the candidate never hears "granted" for a vote
    that a crash could make this node forget."""
    import os

    n3 = RaftNode("n3", ["n1", "n2"], InMemoryTransport(), storage=open_storage("n3"))

    def broken_fsync(fd):
        raise OSError("disk on fire")

    monkeypatch.setattr(os, "fsync", broken_fsync)
    with pytest.raises(OSError):
        n3.handle_request_vote(RequestVoteArgs("n1", "n3", 5, 0, 0))
