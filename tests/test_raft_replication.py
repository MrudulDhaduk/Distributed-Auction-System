"""Specifies Raft log replication and log repair (build order steps 4 and 5):
Figure 2's log[], commitIndex, lastApplied, nextIndex[], matchIndex[], the
full AppendEntries receiver, the leader's replication and commit rules, and
the client-facing propose(). Safety properties from Figure 3: Log Matching,
Leader Completeness, State Machine Safety.

Timing policy (same as test_raft_election.py, because a flaky test is a
failing test):
  * Receiver tests call handle_append_entries directly on a node that is
    never started: no clock, no threads, fully deterministic.
  * Scenario tests (Figure 7, Figure 8, catch-up...) make the leader
    deterministic instead of racing for it: exactly one node has a short
    election timeout and every other node has 30 s, so in a test lasting a
    few seconds only that node ever campaigns. Waits are
    `wait_until(..., timeout=5.0)` on a condition, never a bare sleep that
    has to be "long enough".
  * The two "nothing happens" checks (an old-term entry is NOT committed)
    wait for the positive precondition first (the entry has reached a
    majority), then hold for many heartbeat periods. A buggy leader would
    commit within one reply, so the hold is ~10x what a bug needs.
  * Every new leader appends a no-op (section 8), and the no-op commits old
    entries by itself. To set up Figure 8 ("on a majority, NOT committed")
    the tests therefore hold the no-op back with Gate, which loses (never
    alters) the AppendEntries that would take one follower past a given
    index.
  * apply_fn never sees no-ops, so its indexes skip numbers. Tests compare
    applied DATA, or applied indexes against the node's own log.
  * The State Machine Safety test is randomised on purpose: seeded chaos,
    real thread interleavings, an invariant checked over the whole history.
"""

import random
import threading
import time
from collections import defaultdict

import pytest

from raft.messages import (
    AppendEntriesArgs,
    EntryKind,
    LogEntry,
    RequestVoteArgs,
    Transport,
    TransportError,
)
from raft.node import Outcome, RaftNode, Role
from raft.storage import MemoryStorage
from raft.transport import InMemoryTransport

FAST = dict(election_timeout=(0.15, 0.30), heartbeat_interval=0.03)
CHURN = dict(election_timeout=(0.06, 0.12), heartbeat_interval=0.015)
LEADER_FIRST = dict(election_timeout=(0.05, 0.10), heartbeat_interval=0.03)
NEVER = dict(election_timeout=(30.0, 31.0), heartbeat_interval=0.03)
BOUND = 5.0


def wait_until(predicate, timeout=BOUND, interval=0.002):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def entries(terms):
    """Log entries for a list of terms, 1-based. The payload depends only on
    (index, term): two logs holding the same index and term then hold the
    same entry, as the Log Matching Property says real logs do."""
    return [LogEntry(t, f"{i}@{t}".encode()) for i, t in enumerate(terms, start=1)]


class Gate(Transport):
    """Wraps the network. `cap[(sender, receiver)] = k` LOSES every
    AppendEntries from sender to receiver that would SUCCESSFULLY take the
    receiver past index k. Requests the receiver will refuse anyway (it
    lacks the prevIndex entry) still get through, so the leader can back up
    and send older entries.

    It only ever loses whole messages; it never alters one. (Trimming
    entries would make the leader's matchIndex, computed from what it sent,
    R11, lie.) Losing messages, in any pattern, is ordinary network
    behaviour that Raft must be safe under; it retries on the next tick."""

    def __init__(self, net):
        self._net = net
        self.nodes = {}
        self.cap = {}

    def request_vote(self, to, args):
        return self._net.request_vote(to, args)

    def append_entries(self, to, args):
        k = self.cap.get((args.from_id, to))
        if k is not None and args.prev_index + len(args.entries) > k and self._would_accept(to, args):
            raise TransportError(f"gate: {args.from_id} -> {to} held at index {k}")
        return self._net.append_entries(to, args)

    def _would_accept(self, to, args):
        if args.prev_index == 0:
            return True
        log_ = self.nodes[to].log_entries()
        return len(log_) >= args.prev_index and log_[args.prev_index - 1].term == args.prev_term


class Cluster:
    """Nodes on one InMemoryTransport (behind a Gate that is open unless a
    test sets caps). Records every node's applied sequence (via apply_fn)
    and every leader per term (via the observer)."""

    def __init__(self, n=3, seed=0, logs=None, term=0, per_node=None, **config):
        self.net = InMemoryTransport()
        self.gate = Gate(self.net)
        self.ids = [f"n{i}" for i in range(1, n + 1)]
        self.applied = {nid: [] for nid in self.ids}
        self.leaders_by_term = defaultdict(set)
        self._lock = threading.Lock()
        self.nodes = {}
        for i, nid in enumerate(self.ids):
            cfg = {**FAST, **config, **(per_node or {}).get(nid, {})}
            node = RaftNode(
                nid,
                [p for p in self.ids if p != nid],
                self.gate,
                rng=random.Random(seed * 1000 + i),
                observer=self._observe,
                storage=MemoryStorage(term=term, entries=entries((logs or {}).get(nid, ()))),
                apply_fn=self._apply_fn_for(nid),
                **cfg,
            )
            self.nodes[nid] = node
            self.net.register(node)
        self.gate.nodes = self.nodes

    def _apply_fn_for(self, nid):
        def apply_fn(index, data):
            with self._lock:
                self.applied[nid].append((index, data))
            return (nid, index)

        return apply_fn

    def _observe(self, event):
        with self._lock:
            if event.new_role == Role.LEADER:
                self.leaders_by_term[event.new_term].add(event.node_id)

    def start(self):
        for node in self.nodes.values():
            node.start()
        return self

    def stop(self):
        for node in self.nodes.values():
            node.stop()

    def applied_data(self, nid):
        with self._lock:
            return [d for _, d in self.applied[nid]]

    def applied_count(self, nid):
        with self._lock:
            return len(self.applied[nid])

    def leaders(self, among=None, above_term=0):
        return [
            node
            for nid, node in self.nodes.items()
            if (among is None or nid in among)
            and node.status().role == Role.LEADER
            and node.status().term > above_term
        ]

    def wait_for_leader(self, among=None, above_term=0, timeout=BOUND):
        found = []

        def ok():
            found[:] = self.leaders(among, above_term)
            return len(found) >= 1

        assert wait_until(ok, timeout), "no leader emerged in time"
        return found[0]

    def wait_applied(self, nids, count, timeout=BOUND):
        assert wait_until(
            lambda: all(self.applied_count(n) >= count for n in nids), timeout
        ), {n: self.applied_count(n) for n in nids}


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


def follower(terms=(), term=None, **kw):
    """A node that is never started: only its handlers are exercised."""
    es = entries(terms)
    return RaftNode(
        "n1",
        ["n2", "n3"],
        InMemoryTransport(),
        rng=random.Random(1),
        storage=MemoryStorage(term=term if term is not None else max(terms, default=0), entries=es),
        **{**FAST, **kw},
    )


def ae(term, prev_index, prev_term, new_terms=(), commit=0, from_id="n2"):
    """AppendEntries carrying entries for `new_terms`, placed right after
    prev_index (payloads as in entries())."""
    start = prev_index + 1
    es = tuple(LogEntry(t, f"{i}@{t}".encode()) for i, t in enumerate(new_terms, start=start))
    return AppendEntriesArgs(from_id, "n1", term, prev_index, prev_term, commit, es)


def terms_of(node):
    return [e.term for e in node.log_entries()]


# --- AppendEntries RPC, receiver implementation (Figure 2, steps 1-5) -------


def test_receiver_appends_new_entries_to_an_empty_log():
    node = follower()
    reply = node.handle_append_entries(ae(term=1, prev_index=0, prev_term=0, new_terms=[1, 1]))
    assert reply.entry_appended and reply.match_index == 2
    assert terms_of(node) == [1, 1]
    assert node.status().last_log_index == 2


@pytest.mark.parametrize(
    "prev_index, prev_term",
    [
        (5, 1),  # step 2: no entry at prevLogIndex at all (our log is shorter)
        (2, 2),  # step 2: an entry exists there, but with a different term
    ],
)
def test_receiver_rejects_on_failed_consistency_check_and_changes_nothing(prev_index, prev_term):
    node = follower([1, 1, 1])
    reply = node.handle_append_entries(
        ae(term=3, prev_index=prev_index, prev_term=prev_term, new_terms=[3], commit=3)
    )
    assert reply.entry_appended is False
    assert terms_of(node) == [1, 1, 1]  # nothing appended, nothing deleted
    assert node.status().commit_index == 0  # step 5 is not reached either
    # Still a legitimate leader for this term: rejecting the entries is not
    # rejecting the leader.
    assert node.status().leader_id == "n2" and node.status().term == 3


def test_receiver_truncates_from_the_first_real_conflict_only():
    # Figure 7 (f)-style: our tail is from old terms 2 and 3.
    node = follower([1, 1, 1, 2, 2, 3, 3])
    # The leader's log agrees up to index 3 and has term-4 entries after it.
    reply = node.handle_append_entries(ae(term=4, prev_index=3, prev_term=1, new_terms=[4, 4]))
    assert reply.entry_appended and reply.match_index == 5
    # Step 3: index 4 conflicts (2 vs 4), so 4..7 are deleted; step 4 appends.
    assert terms_of(node) == [1, 1, 1, 4, 4]


def test_receiver_keeps_matching_entries_when_the_conflict_is_later():
    node = follower([1, 1, 2, 2])
    # Entries 2 and 3 are carried again (index 2 matches), index 4 conflicts.
    reply = node.handle_append_entries(ae(term=3, prev_index=1, prev_term=1, new_terms=[1, 2, 3]))
    assert reply.entry_appended
    assert terms_of(node) == [1, 1, 2, 3]


def test_duplicated_or_delayed_append_entries_never_truncate_matching_entries():
    node = follower()
    full = ae(term=1, prev_index=0, prev_term=0, new_terms=[1, 1, 1])
    assert node.handle_append_entries(full).entry_appended
    assert terms_of(node) == [1, 1, 1]

    # An exact duplicate of the request just processed (a retried RPC).
    assert node.handle_append_entries(full).entry_appended
    # A delayed, OLDER request that carried only the first entry. A naive
    # "truncate after prevIndex + len(entries)" would delete entries 2-3.
    old = ae(term=1, prev_index=0, prev_term=0, new_terms=[1])
    reply = node.handle_append_entries(old)
    assert reply.entry_appended and reply.match_index == 1
    # Reordered: a middle piece arriving after the whole.
    assert node.handle_append_entries(ae(term=1, prev_index=1, prev_term=1, new_terms=[1])).entry_appended
    # A heartbeat that is behind (prevIndex 1) also deletes nothing.
    assert node.handle_append_entries(ae(term=1, prev_index=1, prev_term=1)).entry_appended

    assert terms_of(node) == [1, 1, 1]  # all three entries survived
    assert [e.data for e in node.log_entries()] == [b"1@1", b"2@1", b"3@1"]


def test_commit_index_is_capped_by_the_last_new_entry_and_never_decreases():
    node = follower([1, 1, 1, 1, 1])  # entries 4-5 are NOT yet verified by this leader
    # Step 5: leaderCommit = 5, but this RPC only proves indexes up to 2 match
    # the leader's log, so commitIndex = min(5, 2) = 2.
    node.handle_append_entries(ae(term=1, prev_index=0, prev_term=0, new_terms=[1, 1], commit=5))
    assert node.status().commit_index == 2

    node.handle_append_entries(ae(term=1, prev_index=4, prev_term=1, new_terms=[1], commit=5))
    assert node.status().commit_index == 5

    # A delayed earlier heartbeat (proves only index 1, leaderCommit 5):
    # min(5, 1) = 1, but commitIndex "increases monotonically" (Figure 2).
    node.handle_append_entries(ae(term=1, prev_index=1, prev_term=1, commit=5))
    assert node.status().commit_index == 5


def test_stale_term_append_entries_changes_nothing():
    node = follower([1, 2], term=3)
    reply = node.handle_append_entries(ae(term=2, prev_index=0, prev_term=0, new_terms=[2, 2, 2], commit=3))
    assert reply.entry_appended is False and reply.term == 3
    assert terms_of(node) == [1, 2]


def test_follower_applies_committed_entries_in_order_once():
    applied = []
    node = follower(apply_fn=lambda i, d: applied.append((i, d)), **NEVER)
    try:
        node.start()  # starts the applier; a 30 s timeout means no campaign
        node.handle_append_entries(ae(term=1, prev_index=0, prev_term=0, new_terms=[1, 1, 1], commit=0))
        time.sleep(0.05)
        assert applied == []  # appended but not committed: not applied
        node.handle_append_entries(ae(term=1, prev_index=3, prev_term=1, commit=2))
        assert wait_until(lambda: len(applied) == 2)
        # Repeating the commit news must not apply anything twice.
        node.handle_append_entries(ae(term=1, prev_index=3, prev_term=1, commit=2))
        node.handle_append_entries(ae(term=1, prev_index=3, prev_term=1, commit=3))
        assert wait_until(lambda: len(applied) == 3)
        time.sleep(0.05)
    finally:
        node.stop()
    assert applied == [(1, b"1@1"), (2, b"2@1"), (3, b"3@1")]
    assert node.status().last_applied == 3


# --- the election restriction now uses the real log (5.4.1) ----------------


def test_vote_check_reads_the_log_that_append_entries_built():
    # No injected last_log any more: the entries a leader replicated are
    # what the up-to-date check compares against.
    voter = follower()
    voter.handle_append_entries(ae(term=2, prev_index=0, prev_term=0, new_terms=[1, 2]))
    behind = voter.handle_request_vote(RequestVoteArgs("n3", "n1", 3, 1, 1))
    level = voter.handle_request_vote(RequestVoteArgs("n3", "n1", 3, 2, 2))
    assert not behind.vote_granted and level.vote_granted


@pytest.mark.parametrize("n3_log, can_win", [([1], False), ([1, 1], True)])
def test_a_node_with_a_stale_log_cannot_win(cluster, n3_log, can_win):
    # Only n3 ever times out (n1 and n2 have 30 s timeouts), so n3 is the
    # only candidate there will ever be. Then the log is the only thing that
    # can stop it: with n3 missing entry 2 it loses every election; the
    # control case (same log as the others) wins at once.
    c = cluster(
        logs={"n1": [1, 1], "n2": [1, 1], "n3": n3_log},
        term=1,
        per_node={
            "n1": NEVER,
            "n2": NEVER,
            "n3": dict(election_timeout=(0.03, 0.05), heartbeat_interval=0.01),
        },
    ).start()
    n3 = c.nodes["n3"]
    if can_win:
        c.wait_for_leader(among={"n3"})
        return
    assert wait_until(lambda: n3.status().term >= 10)  # at least 9 lost elections
    assert not c.leaders_by_term  # nobody, ever, became leader
    assert all(c.nodes[n].status().voted_for is None for n in ("n1", "n2"))


# --- replication, happy path ------------------------------------------------


def test_proposed_entries_are_applied_on_all_nodes_in_the_same_order(cluster):
    c = cluster(per_node={"n1": LEADER_FIRST, "n2": NEVER, "n3": NEVER}).start()
    leader = c.wait_for_leader()
    assert leader.node_id == "n1"

    handles = [leader.propose(f"cmd{k}".encode()) for k in range(5)]
    for k, h in enumerate(handles):
        assert h.wait(BOUND), f"proposal {k} never resolved"
        assert h.outcome is Outcome.APPLIED and h.ok
        # Index 1 is the leader's no-op, so client entries start at 2.
        assert h.index == k + 2 and h.term == leader.status().term
        # The handle carries apply_fn's return value from the LEADER's apply.
        assert h.result == ("n1", k + 2)

    c.wait_applied(c.ids, 5)
    expected = [f"cmd{k}".encode() for k in range(5)]
    for nid in c.ids:
        assert c.applied_data(nid) == expected
        # apply_fn never saw index 1 (the no-op): its indexes skip it.
        assert [i for i, _ in c.applied[nid]] == [2, 3, 4, 5, 6]


def test_single_node_cluster_commits_on_its_own():
    applied = []
    node = RaftNode("n1", [], InMemoryTransport(), rng=random.Random(0),
                    apply_fn=lambda i, d: applied.append(d), **FAST)
    try:
        node.start()
        assert wait_until(lambda: node.status().role == Role.LEADER)
        h = node.propose(b"solo")
        assert h.wait(BOUND) and h.ok
    finally:
        node.stop()
    assert applied == [b"solo"]


def test_apply_fn_runs_outside_the_node_lock():
    # apply_fn calls back into the node. If the applier held the node lock
    # while calling it, status() would deadlock (the lock is not reentrant),
    # nothing would be applied and the proposal would never resolve.
    seen = []
    me = {}

    def apply_fn(index, data):
        seen.append(me["node"].status().last_applied)  # takes the node lock
        return index

    node = RaftNode("n1", [], InMemoryTransport(), rng=random.Random(0), apply_fn=apply_fn, **FAST)
    me["node"] = node
    try:
        node.start()
        assert wait_until(lambda: node.status().role == Role.LEADER)
        h = node.propose(b"x")
        assert h.wait(BOUND) and h.ok
    finally:
        node.stop()
    # Inside apply_fn (index 2) the entry is not yet counted: lastApplied is
    # 1, the no-op, and moves to 2 only after apply_fn returns.
    assert seen == [1]


def test_an_exception_in_apply_fn_halts_this_nodes_applier():
    # Fail-stop (R3): skipping the entry would silently make this replica
    # differ from the others, so after a failure nothing more is applied.
    applied = []

    def apply_fn(index, data):
        if data == b"boom":
            raise RuntimeError("state machine bug")
        applied.append(data)

    node = RaftNode("n1", [], InMemoryTransport(), rng=random.Random(0), apply_fn=apply_fn, **FAST)
    try:
        node.start()
        assert wait_until(lambda: node.status().role == Role.LEADER)
        assert node.propose(b"ok").wait(BOUND)
        boom, after = node.propose(b"boom"), node.propose(b"after")
        # Indexes: 1 no-op, 2 ok, 3 boom, 4 after.
        assert wait_until(lambda: node.status().commit_index == 4)
        time.sleep(0.1)
        assert applied == [b"ok"] and node.status().last_applied == 2
        assert not boom.wait(0) and not after.wait(0)  # never claimed as applied
    finally:
        node.stop()
    assert boom.outcome is Outcome.STOPPED and after.outcome is Outcome.STOPPED


def test_term_and_vote_can_only_be_written_through_storage():
    # The persistence seam (R5): the node keeps no copy of currentTerm or
    # votedFor, it reads them from storage through read-only properties.
    # So an assignment that bypassed storage is an error, not a silent
    # divergence that step 6 would then fail to make durable.
    node = follower()
    with pytest.raises(AttributeError):
        node._current_term = 5
    with pytest.raises(AttributeError):
        node._voted_for = "n2"


# --- catch-up and repair ----------------------------------------------------


def test_a_follower_that_was_down_catches_up_after_restore(cluster):
    c = cluster(per_node={"n1": LEADER_FIRST, "n2": NEVER, "n3": NEVER}).start()
    leader = c.wait_for_leader()
    c.net.drop("n3")
    for k in range(5):
        assert leader.propose(f"e{k}".encode()).wait(BOUND)
    c.wait_applied(["n1", "n2"], 5)
    assert c.applied_count("n3") == 0
    assert c.nodes["n3"].status().last_log_index <= 1  # at most the no-op

    c.net.restore("n3")
    c.wait_applied(["n3"], 5)
    assert c.applied_data("n3") == c.applied_data("n1")
    assert terms_of(c.nodes["n3"]) == terms_of(leader)


def test_figure7_divergent_followers_are_repaired(cluster):
    # Figure 7: the leader for term 8, and followers (a), (b), (e), (f).
    logs = {
        "n1": [1, 1, 1, 4, 4, 5, 5, 6, 6, 6],     # leader
        "n2": [1, 1, 1, 4, 4, 5, 5, 6, 6],        # (a) missing one entry
        "n3": [1, 1, 1, 4],                       # (b) missing many
        "n4": [1, 1, 1, 4, 4, 4, 4],              # (e) extra uncommitted term-4 entries
        "n5": [1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3],  # (f) extra entries from terms 2, 3
    }
    per_node = {nid: NEVER for nid in logs}
    per_node["n1"] = LEADER_FIRST
    c = cluster(n=5, logs=logs, term=7, per_node=per_node).start()
    leader = c.wait_for_leader()
    assert leader.node_id == "n1" and leader.status().term == 8

    # Repair happens before any client proposal: the logs converge on the
    # leader's, including its term-8 no-op at index 11.
    want = logs["n1"] + [8]
    assert wait_until(lambda: all(terms_of(n) == want for n in c.nodes.values())), {
        nid: terms_of(n) for nid, n in c.nodes.items()
    }
    # The no-op is a current-term entry, so it alone commits 1-11 (5.4.2).
    old = [f"{i}@{t}".encode() for i, t in enumerate(logs["n1"], 1)]
    c.wait_applied(c.ids, 10)
    for nid in c.ids:
        assert c.applied_data(nid) == old
    h = leader.propose(b"term8")
    assert h.wait(BOUND) and h.ok and h.index == 12
    c.wait_applied(c.ids, 11)
    for nid in c.ids:
        assert c.applied_data(nid) == old + [b"term8"]
    # (f)'s old entries were overwritten, never applied.
    assert b"4@2" not in c.applied_data("n5")


# --- the Figure 8 rule (5.4.2) ----------------------------------------------


def figure8_cluster(cluster, monkeypatch):
    """3-node Figure 8. n1 led term 2 and wrote index 2; n3 led term 3 and
    wrote a different index 2. n1 wins term 4 (n3 is cut off), and n2 grants
    its vote; n2 only has index 1.
    n2 never times out; n3 times out quickly, but is partitioned away.

    n1 appends its term-4 no-op at index 3 on winning. If that reached n2 it
    would commit everything at once (that is its job), and there would be no
    Figure 8 situation to look at. So: one entry per RPC, and the Gate loses
    every n1 -> n2 AppendEntries that would take n2 past index 2. n2 gets
    the old entry; the no-op stays on n1 alone."""
    monkeypatch.setattr("raft.node.MAX_ENTRIES_PER_RPC", 1)
    c = cluster(
        logs={"n1": [1, 2], "n2": [1], "n3": [1, 3]},
        term=3,
        per_node={"n1": LEADER_FIRST, "n2": NEVER, "n3": LEADER_FIRST},
    )
    c.gate.cap[("n1", "n2")] = 2
    c.net.partition(["n1", "n2"], ["n3"])
    c.start()
    leader = c.wait_for_leader(among={"n1", "n2"})
    assert leader.node_id == "n1" and leader.status().term >= 4
    assert leader.log_entries()[2] == LogEntry(leader.status().term, b"", EntryKind.NOOP)
    # n1 replicates its term-2 entry: it is now on a majority (n1, n2).
    assert wait_until(lambda: terms_of(c.nodes["n2"]) == [1, 2])
    return c


def test_figure8_old_term_entry_on_a_majority_is_not_committed(cluster, monkeypatch):
    c = figure8_cluster(cluster, monkeypatch)
    n1, n2 = c.nodes["n1"], c.nodes["n2"]
    time.sleep(0.3)  # ~10 heartbeat periods: a counting-replicas bug commits in one
    assert n1.status().commit_index == 0 and n2.status().commit_index == 0
    assert c.applied_count("n1") == 0 and c.applied_count("n2") == 0

    # Figure 8(d): this is why. n1 crashes; n3 (last term 3 > 2) gets n2's
    # vote and overwrites the "majority" entry at index 2. n3's own no-op
    # then commits n3's version, and that is what gets applied.
    c.net.drop("n1")
    c.net.heal()
    n3 = c.wait_for_leader(among={"n3"})
    assert wait_until(lambda: terms_of(n2) == terms_of(n3) == [1, 3, n3.status().term])
    c.wait_applied(["n2", "n3"], 2)
    assert c.applied_data("n2") == c.applied_data("n3") == [b"1@1", b"2@3"]
    # The term-2 entry that sat on a majority was never applied anywhere.
    assert all(b"2@2" not in c.applied_data(nid) for nid in c.ids)


def test_figure8_old_term_entry_commits_only_via_a_current_term_entry(cluster, monkeypatch):
    c = figure8_cluster(cluster, monkeypatch)
    n1 = c.nodes["n1"]
    time.sleep(0.1)
    assert n1.status().commit_index == 0
    # Figure 8(e): let n1's current-term entry (its no-op, index 3) through.
    # No client proposes anything.
    c.gate.cap.clear()
    c.wait_applied(["n1", "n2"], 2)
    assert wait_until(lambda: n1.status().commit_index == 3)
    # Committing index 3 committed indexes 1 and 2 indirectly (Log Matching).
    for nid in ("n1", "n2"):
        assert c.applied_data(nid) == [b"1@1", b"2@2"]  # the no-op is not applied
    assert c.applied_count("n3") == 0


# --- the no-op (section 8) --------------------------------------------------


def test_old_term_entries_commit_after_a_leader_change_with_no_new_proposals(cluster):
    # A term-1 leader replicated entries 1-2 to n1 and n2 (a majority) and
    # crashed before committing them: they are on a majority, and
    # commitIndex is 0 everywhere. NO client proposes anything in this test.
    # Without the no-op a new leader could never commit them (the Figure 8
    # rule needs a current-term entry), and this test would time out.
    c = cluster(logs={"n1": [1, 1], "n2": [1, 1], "n3": [1]}, term=1)
    assert all(n.status().commit_index == 0 for n in c.nodes.values())
    c.start()

    first = c.wait_for_leader()  # the leader change: someone leads term >= 2
    assert first.node_id in ("n1", "n2")  # n3's log is stale
    c.wait_applied(c.ids, 2)
    for nid in c.ids:
        assert c.applied_data(nid) == [b"1@1", b"2@1"]

    # The leader changes again, still with no proposals. The next leader's
    # own no-op commits, and every node applies up to the end of its log.
    c.net.drop(first.node_id)
    second = c.wait_for_leader(among=set(c.ids) - {first.node_id}, above_term=first.status().term)
    c.net.restore(first.node_id)
    assert wait_until(
        lambda: all(n.status().last_applied == second.status().last_log_index for n in c.nodes.values())
    ), {nid: n.status() for nid, n in c.nodes.items()}
    kinds = [e.kind for e in second.log_entries()]
    assert kinds[2:] and all(k is EntryKind.NOOP for k in kinds[2:])  # only no-ops were added
    for nid in c.ids:
        assert c.applied_data(nid) == [b"1@1", b"2@1"]  # no-ops never reach apply_fn


# --- Leader Completeness ----------------------------------------------------


def test_a_committed_entry_survives_the_leader_being_dropped(cluster):
    c = cluster(per_node={"n1": LEADER_FIRST, "n2": FAST, "n3": FAST})
    c.start()
    leader = c.wait_for_leader()
    a, b = [nid for nid in c.ids if nid != leader.node_id]
    c.net.drop(b)  # x will exist on exactly two nodes: leader and a
    hx = leader.propose(b"x")
    assert hx.wait(BOUND) and hx.ok  # committed

    c.net.drop(leader.node_id)
    c.net.restore(b)
    # b has an empty log, so only a can win (b votes for a; a refuses b).
    new = c.wait_for_leader(among={a, b}, above_term=leader.status().term)
    assert new.node_id == a
    assert new.log_entries()[hx.index - 1] == LogEntry(hx.term, b"x")

    hy = new.propose(b"y")
    assert hy.wait(BOUND) and hy.ok
    c.wait_applied([a, b], 2)
    assert c.applied_data(a) == c.applied_data(b) == [b"x", b"y"]


# --- propose() --------------------------------------------------------------


def test_propose_on_a_follower_fails_with_a_leader_hint(cluster):
    c = cluster(per_node={"n1": LEADER_FIRST, "n2": NEVER, "n3": NEVER}).start()
    c.wait_for_leader()
    f = c.nodes["n2"]
    assert wait_until(lambda: f.status().leader_id == "n1")
    assert wait_until(lambda: f.status().last_log_index == 1)  # the leader's no-op

    h = f.propose(b"nope")
    assert h.wait(0)  # resolved immediately, no waiting
    assert h.outcome is Outcome.NOT_LEADER and not h.ok
    assert h.leader_hint == "n1"
    assert f.status().last_log_index == 1  # nothing was appended


def test_propose_with_no_known_leader_has_no_hint():
    h = follower().propose(b"nope")
    assert h.outcome is Outcome.NOT_LEADER and h.leader_hint is None


def test_propose_rejects_non_bytes():
    with pytest.raises(TypeError):
        follower().propose("a str is not bytes")


def test_a_proposal_whose_entry_is_overwritten_never_succeeds(cluster):
    c = cluster(per_node={"n1": LEADER_FIRST, "n2": FAST, "n3": FAST}).start()
    old = c.wait_for_leader()
    assert wait_until(lambda: all(n.status().leader_id == "n1" for n in c.nodes.values()))
    old_term = old.status().term

    # Every node holds n1's no-op (index 1) before the cut.
    assert wait_until(lambda: all(n.status().last_log_index == 1 for n in c.nodes.values()))

    # Cut the leader off. It still believes it leads, so it accepts x at
    # index 2, which can never reach a majority.
    c.net.partition(["n1"], ["n2", "n3"])
    hx = old.propose(b"x")
    assert hx.index == 2 and not hx.wait(0.1)  # pending, not resolved

    # The new leader's no-op takes index 2 (x's slot) in a later term.
    new = c.wait_for_leader(among={"n2", "n3"}, above_term=old_term)
    hy = new.propose(b"y")
    assert hy.wait(BOUND) and hy.ok

    c.net.heal()
    assert hx.wait(BOUND), "x's handle never resolved"
    assert not hx.ok and hx.outcome in (Outcome.LEADERSHIP_LOST, Outcome.OVERWRITTEN)
    c.wait_applied(c.ids, 1)
    for nid in c.ids:
        assert c.applied_data(nid) == [b"y"]  # x was never applied anywhere
    # ...and n1's slot 2 now holds a different term: x is gone for good.
    assert wait_until(lambda: terms_of(c.nodes["n1"])[1:2] == [new.status().term])


def test_stopping_a_leader_fails_its_pending_proposals(cluster):
    c = cluster(per_node={"n1": LEADER_FIRST, "n2": NEVER, "n3": NEVER}).start()
    leader = c.wait_for_leader()
    c.net.drop("n2")
    c.net.drop("n3")
    h = leader.propose(b"stuck")
    assert not h.wait(0.1)
    leader.stop()
    assert h.wait(BOUND) and h.outcome is Outcome.STOPPED


# --- State Machine Safety (Figure 3) under chaos ------------------------------


def _random_fault(c, rng):
    r = rng.random()
    if r < 0.25:
        c.net.drop(rng.choice(c.ids))
    elif r < 0.55:
        c.net.restore(rng.choice(c.ids))
    elif r < 0.75:
        ids = c.ids[:]
        rng.shuffle(ids)
        cut = rng.randint(1, len(ids) - 1)
        c.net.partition(ids[:cut], ids[cut:])
    else:
        c.net.heal()


@pytest.mark.parametrize("seed", range(12))
def test_state_machine_safety_under_drops_and_partitions(cluster, seed):
    n = 5 if seed % 3 == 0 else 3
    c = cluster(n=n, seed=seed, **CHURN).start()
    rng = random.Random(seed)
    proposals = []
    count = 0
    end = time.monotonic() + 1.0
    while time.monotonic() < end:
        if rng.random() < 0.3:
            _random_fault(c, rng)
        for node in c.nodes.values():
            if node.status().role == Role.LEADER:
                count += 1
                data = f"s{seed}-{count}".encode()
                proposals.append((data, node.propose(data)))
        time.sleep(rng.uniform(0.005, 0.02))

    # Heal everything; once one more entry is acknowledged, every node must
    # reach it. If leadership moves right after it commits, the new leader
    # may not know that commitIndex yet; only its no-op (R9) lets it commit
    # again without further proposals. So this converges only if the no-op
    # does its job.
    c.net.heal()
    for nid in c.ids:
        c.net.restore(nid)

    def final_applied():
        leader = c.wait_for_leader()
        h = leader.propose(b"final")
        proposals.append((b"final", h))
        return h.wait(1.0) and h.ok

    assert wait_until(final_applied, timeout=10.0, interval=0.0)
    last = proposals[-1][1].index
    assert wait_until(
        lambda: all(n.status().last_applied >= last for n in c.nodes.values()), timeout=10.0
    ), {nid: node.status() for nid, node in c.nodes.items()}
    c.stop()

    seqs = {nid: list(c.applied[nid]) for nid in c.ids}
    # Each node hands apply_fn exactly its committed COMMAND entries, in index
    # order, each once: the applied indexes are the non-no-op indexes of its
    # own log up to lastApplied (no-ops are the only numbers skipped).
    for nid, seq in seqs.items():
        node = c.nodes[nid]
        log_ = node.log_entries()[: node.status().last_applied]
        commands = [i for i, e in enumerate(log_, 1) if e.kind is EntryKind.COMMAND]
        assert [i for i, _ in seq] == commands, nid
        assert [d for _, d in seq] == [log_[i - 1].data for i in commands], nid
    # No index is ever applied with two different values, on any two nodes.
    value_at = {}
    for nid, seq in seqs.items():
        for i, d in seq:
            assert value_at.setdefault(i, d) == d, f"index {i}: {value_at[i]!r} vs {d!r} on {nid}"
    # Hence every applied sequence is a prefix of every other one.
    for a in seqs.values():
        for b in seqs.values():
            short, long_ = sorted((a, b), key=len)
            assert long_[: len(short)] == short
    # An acknowledged proposal is in the history, at its index, with its data.
    for data, h in proposals:
        if h.ok:
            assert value_at.get(h.index) == data, (h.index, data)
    assert any(h.ok for _, h in proposals)
