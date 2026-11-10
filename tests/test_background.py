"""The background bidders' pure rules (harness/background.py): the bid choice,
the counts and status line, and the stall measure. No cluster is started here."""

import random

from harness.background import background_counts, choose_bid, stall_during, status_line
from harness.status import Status


def bid_record(outcome, mono_end, latency, background=True):
    return {"op": "bid", "outcome": outcome, "background": background, "mono_end": mono_end, "latency": latency}


def test_a_bid_is_above_the_high_bid_and_never_repeats_one_already_sent():
    rng = random.Random(1)
    sent = set()
    high = 100
    for _ in range(500):
        amount, equal = choose_bid(rng, high, sent)
        assert amount not in sent, "the checker's bid identity would see a repeat"
        if not equal:
            assert amount >= high + 5, amount     # the random step is 5-50, so always above the high bid
        sent.add(amount)


def test_about_one_bid_in_ten_is_exactly_the_high_bid():
    rng = random.Random(2)
    equal_count = sum(1 for _ in range(4000) if choose_bid(rng, 500, set())[1])
    assert 250 < equal_count < 550, equal_count      # about 10 %, fixed seed so it never flakes


def test_an_equal_bid_is_never_made_when_that_amount_was_already_sent():
    rng = random.Random(3)
    for _ in range(200):
        amount, equal = choose_bid(rng, 300, {300})
        assert not (equal and amount == 300)


def test_no_equal_bid_when_nobody_has_bid_yet():
    rng = random.Random(4)
    for _ in range(200):
        amount, equal = choose_bid(rng, 0, set())
        assert not equal and amount >= 5


def test_background_counts_split_accepted_rejected_and_errors_and_ignore_the_hero():
    records = [bid_record("ok", 1, 0.01), bid_record("rejected", 2, 0.01), bid_record("unknown", 3, 2.0),
               bid_record("ok", 4, 0.01, background=False),    # a hero bid: not counted
               {"op": "login", "outcome": "ok", "background": True}]
    assert background_counts(records) == (3, 1, 1, 1)


def test_status_line_has_the_agreed_shape():
    view = {"n1": None,
            "n2": Status("n2", "LEADER", 4, "n2", "", 17, 17, 17, 4, "d", 17),
            "n3": Status("n3", "FOLLOWER", 4, "n2", "", 17, 17, 17, 4, "d", 17)}
    records = [bid_record("ok", 1, 0.01), bid_record("rejected", 2, 0.01)]
    line = status_line(records, view)
    assert line == "background: 2 bids, 1 accepted, 1 rejected, 0 errors | leader n2, term 4, commit 17"


def test_status_line_with_no_leader():
    assert status_line([], {"n1": None}).endswith("| leader none")


def test_stall_is_the_longest_bid_in_flight_during_the_failover_window():
    kill = 100.0
    records = [
        bid_record("ok", 99.0, 0.01),            # finished before the kill: not in the window
        bid_record("ok", 101.0, 2.1),            # started at 98.9, in flight at the kill: 2.1 s
        bid_record("ok", 105.0, 0.2),            # later, shorter
        bid_record("ok", 130.0, 9.0),            # long, but after the 15 s window
        bid_record("ok", 102.0, 5.0, background=False),   # hero: not counted
    ]
    assert stall_during(records, kill, window=15.0) == 2.1


def test_stall_is_none_when_no_background_bid_was_in_flight():
    records = [bid_record("ok", 10.0, 0.01)]
    assert stall_during(records, 100.0) is None
