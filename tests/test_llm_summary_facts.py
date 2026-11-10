"""Specifies the result-summary prompt (SummariseAuctionResult): the facts are
worked out IN CODE by the LLM server's prompt builder and handed to the model
as a FACTS block; the model only writes them up. Same approach as the price
trends (decisions.md C18), for the same reason.

Observed on the live model with the previous prompt, which gave it a list of
bids with timestamps and asked it not to list them or discuss timing: it listed
them, repeated the timestamps, and on one run wrote that the auction "did not
end at that time". A rule in prose did not hold, so the material is removed:

  * no timestamp of any kind reaches the prompt (not the bids', not the
    scheduled close), so the model has nothing to invent timing from;
  * the individual bids are not in the prompt, only the opening and the
    closing bid, so there is nothing to enumerate.

No model and no network: tests/test_llm_servicer.py's FakeOllama and FakeContext.
"""

import re

import pytest

from llm_server.llm_servicer import build_summary_messages
from tests.test_llm_servicer import (
    FakeContext,
    make_auction,
    make_servicer,
    summary_request,
    turns,
)


def request(bids, closed=True):
    high = max((a for _, a in bids), default=0)
    return summary_request(auction=make_auction(closed=closed, current_high_bid=high, bid_count=len(bids)),
                           bid_history=bids)


BIDS = (("mrudul", 3000), ("nisarg", 4000), ("karan", 4500), ("nisarg", 5200))


def system_of(bids=BIDS, closed=True):
    return build_summary_messages(request(bids, closed))[0]["content"]


def facts_of(bids=BIDS, closed=True):
    system = system_of(bids, closed)
    return system[system.index("FACTS"):]


def test_the_facts_are_stated_in_code_winner_prices_and_counts():
    facts = facts_of()

    assert "bids placed: 4 (from 3 bidders)" in facts             # nisarg bid twice: 3 distinct bidders
    assert "opening bid: 3000 (by mrudul)" in facts
    assert "closing bid: 5200 (by nisarg)" in facts
    assert "winner: nisarg, with a winning bid of 5200" in facts
    assert "status: closed" in facts


def test_the_winner_is_the_highest_bid_whatever_order_the_list_arrives_in():
    facts = facts_of(bids=(("a", 900), ("b", 100), ("c", 500)))

    assert "winner: a, with a winning bid of 900" in facts
    assert "opening bid: 900 (by a)" in facts       # the first in the list, as given


def test_the_individual_bids_are_not_in_the_prompt_so_there_is_nothing_to_list():
    prompt = " ".join(m["content"] for m in build_summary_messages(request(BIDS)))

    assert "3000" in prompt and "5200" in prompt                      # opening and closing
    assert "4000" not in prompt and "4500" not in prompt              # the ones in between


def test_no_timestamp_of_any_kind_reaches_the_prompt():
    prompt = " ".join(m["content"] for m in build_summary_messages(request(BIDS)))

    assert not re.search(r"\d{4}-\d{2}-\d{2}", prompt)     # no dates
    assert not re.search(r"\d{1,2}:\d{2}", prompt)         # no clock times
    assert "UTC" not in prompt and "2030" not in prompt    # make_auction's close time is in 2030


def test_the_system_turn_forbids_timing_and_telling_the_bids_one_by_one():
    system = system_of()

    assert "FACTS" in system and "do not recompute" in system.lower()
    assert "never state or imply when" in system.lower()


def test_an_auction_nobody_bid_on():
    facts = facts_of(bids=())

    assert "no bids were placed" in facts
    assert "winner: none" in facts
    assert "opening bid" not in facts and "closing bid" not in facts


def test_an_open_auction_has_a_leader_and_no_winner():
    facts = facts_of(closed=False)

    assert "status: open" in facts
    assert "leading bidder: nisarg at 5200" in facts
    assert "winner" not in facts


def test_the_item_is_in_the_facts_and_the_user_turn_stays_fixed():
    messages = build_summary_messages(request(BIDS))

    assert "item: a signed copy of the Raft paper" in messages[0]["content"]
    assert messages[1] == {"role": "user", "content": "Summarise this auction."}


def test_same_request_same_prompt():
    assert build_summary_messages(request(BIDS)) == build_summary_messages(request(BIDS))


@pytest.mark.parametrize("bids", [BIDS, ()])
def test_the_servicer_still_returns_the_models_answer(bids):
    servicer, fake = make_servicer()

    response = servicer.SummariseAuctionResult(request(bids), FakeContext())

    assert response.answer and len(fake.calls) == 1
    assert turns(fake, "system")[0] == system_of(bids)
