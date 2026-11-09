"""Specifies the FOLLOWER side of Raft log replication (build order steps 4
and 5): Figure 2's log[] and commitIndex, the full AppendEntries receiver
(steps 1-5), the election restriction against the real log, and the
persistence seam. Every test calls the handlers directly on a node that is
never started: no clock, no threads, fully deterministic.
"""

import random

import pytest

from raft.messages import AppendEntriesArgs, LogEntry, RequestVoteArgs
from raft.node import RaftNode
from raft.storage import MemoryStorage
from raft.transport import InMemoryTransport

FAST = dict(election_timeout=(0.15, 0.30), heartbeat_interval=0.03)


def entries(terms):
    """Log entries for a list of terms, 1-based. The payload depends only on
    (index, term): two logs holding the same index and term then hold the
    same entry, as the Log Matching Property says real logs do."""
    return [LogEntry(t, f"{i}@{t}".encode()) for i, t in enumerate(terms, start=1)]


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


def test_vote_check_reads_the_log_that_append_entries_built():
    # No injected last_log any more: the entries a leader replicated are
    # what the up-to-date check compares against.
    voter = follower()
    voter.handle_append_entries(ae(term=2, prev_index=0, prev_term=0, new_terms=[1, 2]))
    behind = voter.handle_request_vote(RequestVoteArgs("n3", "n1", 3, 1, 1))
    level = voter.handle_request_vote(RequestVoteArgs("n3", "n1", 3, 2, 2))
    assert not behind.vote_granted and level.vote_granted


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

