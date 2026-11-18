"""Simulated partitions (RaftNode.set_blocked_peers, the dashboard's link
switches): a cut link loses Raft messages in both directions.

The classic demonstration: cut the leader off. The majority elects a new
leader in a higher term; the old leader still believes it leads, but nothing
it accepts can commit. When the link comes back it sees the higher term,
steps down, and its uncommitted entry is replaced by the new leader's log.
"""

import time

import pytest

from raft.messages import AppendEntriesArgs, TransportError
from raft.node import Role
from raft.proposal import Outcome
from tests.test_raft_election import cluster, make_node, wait_until  # noqa: F401


def cut(c, a, b):
    c.nodes[a].set_blocked_peers(set(c.nodes[a].status().blocked_peers) | {b})
    c.nodes[b].set_blocked_peers(set(c.nodes[b].status().blocked_peers) | {a})


def heal(c):
    for n in c.nodes.values():
        n.set_blocked_peers(())


def isolate(c, nid):
    for other in c.ids:
        if other != nid:
            cut(c, nid, other)


def test_a_cut_link_refuses_incoming_rpcs():
    n = make_node()
    n.set_blocked_peers({"n3"})
    with pytest.raises(TransportError):
        n.handle_append_entries(AppendEntriesArgs("n3", "n1", 1, 0, 0, 0))
    assert n.status().term == 0  # the message never arrived
    assert n.status().blocked_peers == ("n3",)


def test_only_known_peers_can_be_blocked():
    n = make_node()
    n.set_blocked_peers({"n2", "n9", "n1"})
    assert n.status().blocked_peers == ("n2",)


def test_an_isolated_leader_cannot_commit_and_the_majority_moves_on(cluster):  # noqa: F811
    c = cluster(seed=11).start()
    old = c.wait_for_stable_leader()
    old_term = old.status().term
    isolate(c, old.node_id)

    others = [n for n in c.ids if n != old.node_id]
    new = c.wait_for_leader(among=others, above_term=old_term)
    assert old.status().role == Role.LEADER and old.status().term == old_term  # it does not know

    stuck = old.propose(b"bid from the wrong side")
    assert not stuck.wait(0.8)  # no majority: it can never commit
    ok = new.propose(b"bid on the majority side")
    assert ok.wait(2.0) and ok.outcome == Outcome.APPLIED

    heal(c)
    assert wait_until(lambda: old.status().role == Role.FOLLOWER, 3.0)
    assert wait_until(lambda: len({n.status().last_log_index for n in c.nodes.values()}) == 1, 3.0)
    assert stuck.wait(1.0) and stuck.outcome != Outcome.APPLIED  # its entry was overwritten
    logs = {nid: [e.data for e in n.log_entries()] for nid, n in c.nodes.items()}
    assert b"bid from the wrong side" not in logs[old.node_id]
    assert b"bid on the majority side" in logs[old.node_id]


def test_cutting_one_link_keeps_the_cluster_working(cluster):  # noqa: F811
    """n1-n2 cut, both still reach n3: a majority exists, writes commit."""
    c = cluster(seed=12).start()
    c.wait_for_stable_leader()
    cut(c, "n1", "n2")
    time.sleep(1.0)
    leaders = c.leaders()
    assert leaders
    leader = max(leaders, key=lambda n: n.status().term)
    p = leader.propose(b"x")
    assert p.wait(3.0) and p.outcome == Outcome.APPLIED


def test_an_isolated_follower_raises_its_term_in_classic_mode_but_not_with_prevote(cluster):  # noqa: F811
    for prevote, expect_rise in ((False, True), (True, False)):
        c = cluster(seed=13, prevote=prevote).start()
        leader = c.wait_for_stable_leader()
        term = leader.status().term
        lonely = next(n for n in c.ids if n != leader.node_id)
        isolate(c, lonely)
        time.sleep(1.0)
        assert (c.nodes[lonely].status().term > term) is expect_rise
        heal(c)
        c.stop()
