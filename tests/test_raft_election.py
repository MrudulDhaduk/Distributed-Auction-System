"""Specifies Raft leader election (build order step 3): Figure 2's State,
RequestVote RPC, and the election-related Rules for Servers, plus Election
Safety (Figure 3). No log, no replication, no sockets: nodes talk through
InMemoryTransport.

Timing policy, because a flaky test is a failing test:
  * Tests that only call the RPC handlers directly never start a node, so
    they involve no clock at all.
  * Tests that start nodes use short timeouts but wait with
    `wait_until(..., timeout=5.0)`: the bound is ~20x the expected time, and
    a pass never depends on a sleep being long enough.
  * The one "nothing happens" test (heartbeats suppress elections) runs for
    several multiples of the longest election timeout, with a heartbeat
    period an order of magnitude below the shortest one.
  * Randomness is seeded (random.Random(seed) per node). Thread
    interleaving is still up to the OS, which is why the safety test
    checks an invariant over many seeds instead of one expected outcome.
"""

import random
import subprocess
import sys
import threading
import time
from collections import defaultdict

import pytest

from raft.messages import (
    AppendEntriesArgs,
    AppendEntriesReply,
    LogEntry,
    RequestVoteArgs,
    RequestVoteReply,
    Transport,
    TransportError,
)
from raft.node import RaftNode, Role
from raft.storage import MemoryStorage
from raft.transport import InMemoryTransport

FAST = dict(election_timeout=(0.15, 0.30), heartbeat_interval=0.03)
# Aggressive on purpose for the safety test: more spurious elections and
# split votes means more chances for a bug to show up.
CHURN = dict(election_timeout=(0.06, 0.12), heartbeat_interval=0.015)
BOUND = 5.0


def wait_until(predicate, timeout=BOUND, interval=0.002):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


class Cluster:
    """N started-on-demand nodes on one InMemoryTransport, with an observer
    recording every role/term change and every leader per term."""

    def __init__(self, n=3, seed=0, **config):
        self.net = InMemoryTransport()
        self.ids = [f"n{i}" for i in range(1, n + 1)]
        self.events = []
        self.leaders_by_term = defaultdict(set)
        self._lock = threading.Lock()
        self.nodes = {}
        for i, nid in enumerate(self.ids):
            node = RaftNode(
                nid,
                [p for p in self.ids if p != nid],
                self.net,
                rng=random.Random(seed * 1000 + i),
                observer=self._observe,
                **{**FAST, **config},
            )
            self.nodes[nid] = node
            self.net.register(node)

    def _observe(self, event):
        with self._lock:
            self.events.append(event)
            if event.new_role == Role.LEADER:
                self.leaders_by_term[event.new_term].add(event.node_id)

    def start(self):
        for node in self.nodes.values():
            node.start()
        return self

    def stop(self):
        for node in self.nodes.values():
            node.stop()

    def leaders(self, among=None, above_term=0):
        out = []
        for nid, node in self.nodes.items():
            if among is not None and nid not in among:
                continue
            st = node.status()
            if st.role == Role.LEADER and st.term > above_term:
                out.append(node)
        return out

    def wait_for_leader(self, among=None, above_term=0, timeout=BOUND):
        found = []

        def ok():
            found[:] = self.leaders(among, above_term)
            return len(found) >= 1

        assert wait_until(ok, timeout), "no leader emerged in time"
        return found[0]

    def wait_for_stable_leader(self, timeout=BOUND):
        """A leader that every node (all reachable) agrees on."""

        def stable():
            ls = self.leaders()
            if len(ls) != 1:
                return False
            lt = ls[0].status().term
            return all(
                n.status().term == lt and n.status().leader_id == ls[0].node_id
                for n in self.nodes.values()
            )

        assert wait_until(stable, timeout), "cluster did not converge on one leader"
        return self.leaders()[0]


@pytest.fixture
def cluster():
    made = []

    def make(**kw):
        c = Cluster(**kw)
        made.append(c)
        return c

    yield make
    for c in made:
        c.stop()


def make_node(node_id="n1", peers=("n2", "n3"), transport=None, **kw):
    """A node that is never started: only its RPC handlers are exercised."""
    return RaftNode(
        node_id,
        list(peers),
        transport or InMemoryTransport(),
        rng=random.Random(1),
        **{**FAST, **kw},
    )


def rv(from_id="n2", term=1, last_log_index=0, last_log_term=0, to="n1"):
    return RequestVoteArgs(from_id, to, term, last_log_index, last_log_term)


def hb(from_id="n2", term=1, to="n1", prev_index=0, prev_term=0):
    return AppendEntriesArgs(from_id, to, term, prev_index, prev_term, 0, ())


# --- Figure 2: Rules for Servers / Followers / Candidates -------------------


def test_lone_follower_whose_timer_expires_becomes_candidate():
    # n1's peers do not exist on the network, so it can never win: it must
    # sit in CANDIDATE state, voted for itself, and keep incrementing term.
    events = []
    lock = threading.Lock()

    def observe(e):
        with lock:
            events.append(e)

    node = make_node(observer=observe)
    try:
        node.start()
        assert wait_until(lambda: len(events) >= 2)
    finally:
        node.stop()

    first, second = events[0], events[1]
    assert (first.old_role, first.new_role) == (Role.FOLLOWER, Role.CANDIDATE)
    assert (first.old_term, first.new_term) == (0, 1)
    assert first.voted_for == "n1"  # votes for itself, in the same step
    # Timer expires again with no winner: new election, term incremented.
    assert (second.old_role, second.new_role) == (Role.CANDIDATE, Role.CANDIDATE)
    assert second.new_term == 2 and second.voted_for == "n1"
    # One vote out of three is not a majority.
    assert not any(e.new_role == Role.LEADER for e in events)


def test_three_node_cluster_elects_exactly_one_leader(cluster):
    c = cluster().start()
    leader = c.wait_for_stable_leader()

    followers = [n for n in c.nodes.values() if n is not leader]
    assert len(followers) == 2
    assert all(f.status().role == Role.FOLLOWER for f in followers)
    assert all(f.status().leader_id == leader.node_id for f in followers)
    assert len(c.leaders()) == 1


@pytest.mark.parametrize("seed", range(15))
def test_election_safety_never_two_leaders_in_one_term(cluster, seed):
    # Figure 3, Election Safety: at most one leader can be elected in a
    # given term. Provoke elections hard (tiny, narrow timeouts) and churn
    # the cluster by isolating whoever leads, then check every leader the
    # observer ever saw -- not just the final state.
    c = cluster(seed=seed, **CHURN).start()
    for _ in range(2):
        old = c.wait_for_leader()
        old_term = old.status().term
        c.net.drop(old.node_id)
        c.wait_for_leader(among=set(c.ids) - {old.node_id}, above_term=old_term)
        c.net.restore(old.node_id)
        # Let the old leader hear the new term and the dust settle a little.
        wait_until(lambda: old.status().role != Role.LEADER)
    c.wait_for_leader()
    c.stop()  # no more events after this

    assert c.leaders_by_term, "no leader was ever elected"
    for term, who in c.leaders_by_term.items():
        assert len(who) == 1, f"term {term} had leaders {sorted(who)}"


def test_follower_receiving_heartbeats_does_not_start_election():
    events = []
    node = make_node(observer=events.append, election_timeout=(0.4, 0.6), heartbeat_interval=0.03)
    try:
        node.start()
        # 1.5 s of heartbeats: >2.5x the longest election timeout.
        end = time.monotonic() + 1.5
        while time.monotonic() < end:
            reply = node.handle_append_entries(hb(term=1))
            assert reply.entry_appended
            time.sleep(0.03)
        st = node.status()
        assert st.role == Role.FOLLOWER and st.leader_id == "n2" and st.term == 1
        assert not any(e.new_role == Role.CANDIDATE for e in events)

        # Control: the silence afterwards DOES trigger an election, so the
        # heartbeats above were what held it off, not a dead timer.
        assert wait_until(lambda: node.status().role == Role.CANDIDATE)
    finally:
        node.stop()


def test_stale_term_heartbeat_is_rejected_and_ignored():
    node = make_node()
    node.handle_request_vote(rv(from_id="n2", term=5))  # adopt term 5

    reply = node.handle_append_entries(hb(from_id="n3", term=3))

    assert reply.entry_appended is False
    assert reply.term == 5  # tells the stale leader the current term
    st = node.status()
    assert st.leader_id is None  # did not accept n3 as leader
    assert st.term == 5


def test_candidate_steps_down_on_heartbeat_from_same_term_leader():
    events = []
    # Peers unreachable: n1 becomes a candidate and keeps campaigning.
    node = make_node(observer=events.append, election_timeout=(0.2, 0.3))
    try:
        node.start()
        # Candidates rule: "If AppendEntries RPC received from new leader:
        # convert to follower". Send a heartbeat for exactly the term the
        # candidate is campaigning in. If a new election slips in between
        # the status() read and the call, the heartbeat is stale and is
        # rejected; just try again in the next candidacy.
        def accepted():
            st = node.status()
            if st.role != Role.CANDIDATE:
                return False
            return node.handle_append_entries(hb(from_id="n3", term=st.term)).entry_appended

        assert wait_until(accepted)
        st = node.status()
        assert st.role == Role.FOLLOWER and st.leader_id == "n3"
    finally:
        node.stop()
    stepped = [e for e in events if e.old_role == Role.CANDIDATE and e.new_role == Role.FOLLOWER]
    assert stepped and stepped[0].old_term == stepped[0].new_term  # same term: not a higher-term step-down


# --- Figure 2: RequestVote RPC receiver implementation ----------------------


def test_request_vote_with_lower_term_is_rejected_and_reply_carries_higher_term():
    node = make_node()
    assert node.handle_request_vote(rv(from_id="n2", term=5)).vote_granted

    reply = node.handle_request_vote(rv(from_id="n3", term=3))

    assert reply.vote_granted is False
    assert reply.term == 5
    assert (reply.from_id, reply.to) == ("n1", "n3")
    assert node.status().term == 5  # unchanged


def test_node_grants_at_most_one_vote_per_term():
    node = make_node()

    first = node.handle_request_vote(rv(from_id="n2", term=1))
    second = node.handle_request_vote(rv(from_id="n3", term=1))
    retry = node.handle_request_vote(rv(from_id="n2", term=1))

    assert first.vote_granted
    assert not second.vote_granted and second.term == 1
    assert retry.vote_granted  # "votedFor is null or candidateId": idempotent
    assert node.status().voted_for == "n2"

    # A new term is a new ballot.
    assert node.handle_request_vote(rv(from_id="n3", term=2)).vote_granted
    assert node.status().voted_for == "n3"


def test_candidate_does_not_vote_for_a_rival_in_its_own_term():
    node = make_node(election_timeout=(0.15, 0.2))
    try:
        node.start()
        assert wait_until(lambda: node.status().role == Role.CANDIDATE)
        # Same-term rival: n1 already voted for itself in this term. If a
        # new election slipped in meanwhile the request is merely stale; it
        # is refused either way.
        st = node.status()
        reply = node.handle_request_vote(rv(from_id="n3", term=st.term))
        assert reply.vote_granted is False
    finally:
        node.stop()


@pytest.mark.parametrize("kind", ["request_vote", "append_entries"])
def test_higher_term_in_a_request_makes_a_node_step_down_and_adopt_it(cluster, kind):
    c = cluster().start()
    leader = c.wait_for_stable_leader()
    term = leader.status().term

    if kind == "request_vote":
        reply = leader.handle_request_vote(rv(from_id="n9", term=term + 4, to=leader.node_id))
        assert reply.term == term + 4
    else:
        reply = leader.handle_append_entries(hb(from_id="n9", term=term + 4, to=leader.node_id))
        assert reply.term == term + 4 and reply.entry_appended

    st = leader.status()
    assert st.role == Role.FOLLOWER
    assert st.term >= term + 4  # may already be higher if it campaigned since
    # votedFor was cleared on the term change (and only re-set by a grant).
    if kind == "append_entries" and st.term == term + 4:
        assert st.voted_for is None
        assert st.leader_id == "n9"
    if kind == "request_vote":
        # A deposed leader must not keep claiming to be the leader it was:
        # in the new term the leader is unknown until someone heartbeats.
        assert st.leader_id != leader.node_id


class Scripted(Transport):
    """A fake network whose peers answer with whatever the test says."""

    def __init__(self, vote, append):
        self._vote, self._append = vote, append

    def request_vote(self, to, args):
        return self._vote(to, args)

    def append_entries(self, to, args):
        return self._append(to, args)


def test_higher_term_in_a_request_vote_reply_makes_a_candidate_step_down():
    events = []
    net = Scripted(
        vote=lambda to, a: RequestVoteReply(to, a.from_id, 9, False),
        append=lambda to, a: (_ for _ in ()).throw(TransportError("unused")),
    )
    node = make_node(transport=net, observer=events.append)
    try:
        node.start()
        assert wait_until(
            lambda: any(e.new_role == Role.FOLLOWER and e.new_term == 9 for e in events)
        )
    finally:
        node.stop()

    step_down = next(e for e in events if e.new_term == 9)
    assert step_down.old_role == Role.CANDIDATE
    assert step_down.voted_for is None  # new term, blank ballot


def test_higher_term_in_an_append_entries_reply_makes_a_leader_step_down():
    events = []
    net = Scripted(
        vote=lambda to, a: RequestVoteReply(to, a.from_id, a.term, True),
        append=lambda to, a: AppendEntriesReply(to, a.from_id, 9, False, 0),
    )
    node = make_node(transport=net, observer=events.append)
    try:
        node.start()
        assert wait_until(
            lambda: any(e.old_role == Role.LEADER and e.new_term == 9 for e in events)
        )
    finally:
        node.stop()

    step_down = next(e for e in events if e.old_role == Role.LEADER)
    assert step_down.new_role == Role.FOLLOWER
    assert step_down.voted_for is None


# --- Failover ---------------------------------------------------------------


def test_dropping_the_leader_produces_a_new_leader_with_a_strictly_higher_term(cluster):
    c = cluster().start()
    old = c.wait_for_stable_leader()
    old_term = old.status().term

    c.net.drop(old.node_id)
    new = c.wait_for_leader(among=set(c.ids) - {old.node_id}, above_term=old_term)
    assert new is not old
    assert new.status().term > old_term

    # The old leader never heard anything, so still believes it leads...
    assert old.status().role == Role.LEADER
    # ...until the partition heals and it sees the higher term.
    c.net.restore(old.node_id)
    assert wait_until(lambda: old.status().role == Role.FOLLOWER)
    assert wait_until(lambda: old.status().term >= new.status().term)
    c.wait_for_stable_leader()


def test_a_minority_partition_cannot_elect_a_leader(cluster):
    c = cluster().start()
    old = c.wait_for_stable_leader()
    others = [i for i in c.ids if i != old.node_id]
    # Isolate a *follower*; it keeps timing out, but can never reach a majority.
    loner = others[0]
    c.net.partition([loner], [old.node_id, others[1]])
    time.sleep(1.0)  # several election timeouts of the isolated node
    assert c.nodes[loner].status().role == Role.CANDIDATE
    assert all(loner not in who for who in c.leaders_by_term.values())


# --- Figure 2 + 5.4.1: "candidate's log is at least as up-to-date" ---------


def voter_with_log():
    """A voter whose real log ends at (index 3, term 2)."""
    log_ = [LogEntry(1, b"a"), LogEntry(2, b"b"), LogEntry(2, b"c")]
    return make_node("n1", storage=MemoryStorage(term=2, entries=log_))


@pytest.mark.parametrize(
    "cand_last_term, cand_last_index, granted",
    [
        (1, 10, False),  # smaller last term loses even with a much longer log
        (2, 2, False),   # same last term, shorter log
        (2, 3, True),    # identical log
        (2, 9, True),    # same last term, longer log
        (3, 1, True),    # larger last term wins even with a shorter log
    ],
)
def test_vote_requires_candidate_log_at_least_as_up_to_date(
    cand_last_term, cand_last_index, granted
):
    voter = voter_with_log()

    reply = voter.handle_request_vote(
        rv(from_id="n2", term=7, last_log_index=cand_last_index, last_log_term=cand_last_term)
    )

    assert reply.vote_granted is granted
    # Either way the voter adopts the candidate's higher term...
    assert reply.term == 7 and voter.status().term == 7
    # ...but only a granted vote is recorded.
    assert voter.status().voted_for == ("n2" if granted else None)


def test_refused_stale_candidate_does_not_burn_the_vote():
    voter = voter_with_log()
    assert not voter.handle_request_vote(rv("n2", term=7, last_log_index=1, last_log_term=1)).vote_granted
    # Same term, an up-to-date candidate can still get the vote.
    assert voter.handle_request_vote(rv("n3", term=7, last_log_index=3, last_log_term=2)).vote_granted


# --- structure --------------------------------------------------------------


def test_single_node_cluster_elects_itself():
    net = InMemoryTransport()
    node = RaftNode("n1", [], net, rng=random.Random(0), **FAST)
    net.register(node)
    try:
        node.start()
        assert wait_until(lambda: node.status().role == Role.LEADER)
        assert node.status().term == 1
    finally:
        node.stop()


def test_node_core_does_not_import_grpc():
    code = (
        "import sys, raft.node;"
        "bad = [m for m in sys.modules if m == 'grpc' or m.startswith('grpc.') "
        "or m.startswith('generated')];"
        "sys.exit(1 if bad else 0)"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
