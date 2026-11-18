"""PreVote (Ongaro's thesis, section 9.6), switchable per node.

Off is the paper's Figure 2: a node that was cut off keeps timing out and
raising its term, and when it comes back that higher term deposes a healthy
leader. On: the cut-off node only ASKS "would you vote for me?", nobody with
a live leader says yes, its term never moves, and the leader stays.
"""

import time

from raft.messages import AppendEntriesArgs, RequestVoteArgs
from raft.node import Role
from tests.test_raft_election import BOUND, cluster, make_node, wait_until  # noqa: F401

CUT_OFF = 1.2  # seconds: several election timeouts at the FAST settings (0.15-0.30 s)


def pick_follower(c, leader):
    return next(nid for nid in c.ids if nid != leader.node_id)


def test_classic_raft_a_returning_node_deposes_the_leader(cluster):  # noqa: F811
    c = cluster(seed=3).start()
    leader = c.wait_for_stable_leader()
    term_before = leader.status().term
    lonely = pick_follower(c, leader)
    c.net.drop(lonely)
    time.sleep(CUT_OFF)
    assert c.nodes[lonely].status().term > term_before  # it kept starting elections alone
    c.net.restore(lonely)
    new = c.wait_for_stable_leader()
    assert new.status().term > term_before  # the old leadership was ended by the higher term


def test_prevote_a_returning_node_does_not_disturb_the_leader(cluster):  # noqa: F811
    c = cluster(seed=3, prevote=True).start()
    leader = c.wait_for_stable_leader()
    term_before = leader.status().term
    lonely = pick_follower(c, leader)
    c.net.drop(lonely)
    time.sleep(CUT_OFF)
    assert c.nodes[lonely].status().term == term_before  # it only asked, it never raised its term
    c.net.restore(lonely)
    time.sleep(CUT_OFF)
    assert leader.status().role == Role.LEADER
    assert leader.status().term == term_before
    assert all(n.status().term == term_before for n in c.nodes.values())


def test_prevote_still_replaces_a_dead_leader(cluster):  # noqa: F811
    c = cluster(seed=5, prevote=True).start()
    leader = c.wait_for_stable_leader()
    term_before = leader.status().term
    c.net.drop(leader.node_id)
    others = [nid for nid in c.ids if nid != leader.node_id]
    new = c.wait_for_leader(among=others, above_term=term_before)
    assert new.node_id != leader.node_id


def test_prevote_can_be_switched_on_while_running(cluster):  # noqa: F811
    c = cluster(seed=7).start()
    leader = c.wait_for_stable_leader()
    for n in c.nodes.values():
        n.set_prevote(True)
    assert all(n.status().prevote for n in c.nodes.values())
    term_before = leader.status().term
    lonely = pick_follower(c, leader)
    c.net.drop(lonely)
    time.sleep(CUT_OFF)
    c.net.restore(lonely)
    time.sleep(CUT_OFF)
    assert leader.status().role == Role.LEADER and leader.status().term == term_before


def prevote(from_id="n2", term=2, last_log_index=0, last_log_term=0):
    return RequestVoteArgs(from_id, "n1", term, last_log_index, last_log_term, pre_vote=True)


def test_answering_a_prevote_changes_no_state():
    n = make_node()
    before = (n.status().term, n.status().voted_for, n._election_deadline)
    r = n.handle_request_vote(prevote(term=5))
    assert r.vote_granted and r.term == 0
    assert (n.status().term, n.status().voted_for, n._election_deadline) == before


def test_a_prevote_is_refused_while_a_leader_is_heard():
    n = make_node()
    n.handle_append_entries(AppendEntriesArgs("n3", "n1", 1, 0, 0, 0))  # n3 leads term 1
    r = n.handle_request_vote(prevote(term=2))
    assert not r.vote_granted
    assert n.status().term == 1 and n.status().leader_id == "n3"


def test_a_prevote_for_a_term_not_ahead_or_a_shorter_log_is_refused():
    n = make_node()
    n.handle_append_entries(AppendEntriesArgs("n3", "n1", 3, 0, 0, 0))
    n._last_leader_contact = None  # pretend the leader went quiet long ago
    assert not n.handle_request_vote(prevote(term=3)).vote_granted       # not ahead of our term 3
    assert n.handle_request_vote(prevote(term=4)).vote_granted
    n._storage.append([__import__("raft.messages", fromlist=["LogEntry"]).LogEntry(3, b"x")])
    assert not n.handle_request_vote(prevote(term=4, last_log_index=0, last_log_term=0)).vote_granted


def test_a_prevote_round_starts_the_real_election_only_on_a_majority():
    """Two-node view: n1 asks n2 and n3; one yes (plus itself) is a majority of 3."""
    n = make_node()
    n.set_prevote(True)
    with n._lock:
        asks = n._start_prevote()
    assert [a.pre_vote for _, a in asks] == [True, True]
    assert n.status().term == 0 and n.status().role == Role.FOLLOWER
    from raft.messages import RequestVoteReply
    n._pool = None  # do not actually send the follow-up RequestVotes
    n._on_prevote_reply("n2", asks[0][1], RequestVoteReply("n2", "n1", 0, True))
    assert n.status().role == Role.CANDIDATE and n.status().term == 1
