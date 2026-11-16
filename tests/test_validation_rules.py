"""The validation rules added with the web dashboard:

  * a seller cannot bid on their own auction (apply(), so on every replica);
  * bids are positive; a create needs a positive id and a sane item name;
  * the handler refuses a close time in the past or too far ahead, before
    the LLM is called;
  * accounts can be registered (RegisterUser, replicated, password hashed
    before proposing) and then log in, also after the log is replayed;
  * the checker understands the two new bid refusals;
  * the dashboard's own first-layer checks.
"""

import datetime

import pytest

from auction.state import AuctionState, CreateAuction, PlaceBid
from checker import properties as P
from checker.properties import check_all
from common import time_conv
from dashboard import validate
from dashboard.validate import Invalid
from generated import auction_pb2
from replication import codec, digest
from replication.state_machine import StateMachine
from server.auction_servicer import AuctionServicer, close_time_rejection
from server.auth_servicer import AuthServicer
from sessions.state import RegisterUser, SessionState
from sessions.store import SessionStore
from sessions.users import hash_password, password_rejection, username_rejection
from tests.test_checker import edit_dump, evidence, failed, rec, scenario

T0 = datetime.datetime(2026, 11, 16, 12, 0, tzinfo=datetime.timezone.utc)
LATER = T0 + datetime.timedelta(hours=1)


def created(owner="nisarg"):
    s = AuctionState()
    assert s.apply(CreateAuction(1, "clock", LATER, owner, "")).success
    return s


# --- apply(): bids ----------------------------------------------------------------


def test_the_seller_cannot_bid_on_their_own_auction():
    s = created("nisarg")
    r = s.apply(PlaceBid(1, "nisarg", 100, T0))
    assert not r.success
    assert r.reason == "nisarg is the seller of this auction: a seller cannot bid on their own auction"
    assert s.auctions[1]["bids"] == []


def test_the_seller_rule_holds_even_for_a_bid_that_would_otherwise_win():
    s = created("nisarg")
    assert s.apply(PlaceBid(1, "karan", 100, T0)).success
    assert not s.apply(PlaceBid(1, "nisarg", 10_000, T0)).success
    assert [b.bidder for b in s.auctions[1]["bids"]] == ["karan"]


def test_another_user_can_bid():
    s = created("nisarg")
    assert s.apply(PlaceBid(1, "karan", 100, T0)).success


@pytest.mark.parametrize("amount", [0, -5])
def test_a_bid_must_be_positive(amount):
    s = created()
    r = s.apply(PlaceBid(1, "karan", amount, T0))
    assert not r.success and r.reason == f"Bid amount must be a positive whole number, not {amount}"


def test_the_seller_rule_is_the_same_on_every_replica_after_replay():
    """Through the real codec and apply_fn, as a follower replays it."""
    log = [
        codec.encode(CreateAuction(1, "clock", LATER, "nisarg", ""), "c1", 1),
        codec.encode(PlaceBid(1, "nisarg", 100, T0), "c1", 2),
        codec.encode(PlaceBid(1, "karan", 50, T0), "c2", 1),
    ]
    replicas = [StateMachine() for _ in range(3)]
    for sm in replicas:
        results = [sm.apply_fn(i, data).result.success for i, data in enumerate(log, start=1)]
        assert results == [True, False, True]
    digests = {digest.state_digest(sm.snapshot()) for sm in replicas}
    assert len(digests) == 1


# --- apply(): creates -------------------------------------------------------------


@pytest.mark.parametrize("auction_id,item,reason", [
    (0, "clock", "Auction ID must be a positive whole number, not 0"),
    (-3, "clock", "Auction ID must be a positive whole number, not -3"),
    (1, "", "The item name must not be empty"),
    (1, "   ", "The item name must not be empty"),
    (1, " clock", "The item name must not start or end with spaces"),
    (1, "x" * 101, "The item name is 101 characters long; the limit is 100"),
])
def test_a_create_with_a_bad_id_or_item_is_refused(auction_id, item, reason):
    r = AuctionState().apply(CreateAuction(auction_id, item, LATER, "nisarg", ""))
    assert not r.success and r.reason == reason


# --- the handler: close time, before the LLM ----------------------------------------


class RecordingLLM:
    def __init__(self):
        self.calls = []

    def get_llm_answer(self, request_id, query, context):
        self.calls.append(context)
        return request_id, "text"


class Ctx:
    username = "nisarg"


@pytest.mark.parametrize("delta,ok", [(-1, False), (0, False), (3600, True), (31 * 86400, False)])
def test_close_time_rule(delta, ok):
    now = time_conv.now_utc()
    assert (close_time_rejection(now + datetime.timedelta(seconds=delta), now) is None) is ok


def test_a_create_in_the_past_is_refused_before_the_llm_is_asked():
    llm = RecordingLLM()
    servicer = AuctionServicer(AuctionState(), llm)
    past = time_conv.dt_to_micros(time_conv.now_utc() - datetime.timedelta(seconds=5))
    r = servicer.CreateAuction(auction_pb2.CreateAuctionRequest(
        auction_id=1, item="clock", close_time=past, attributes={"brand": "Seiko"}), Ctx())
    assert not r.result.success and r.result.reason == "The close time must be in the future"
    assert llm.calls == []


def test_too_many_attributes_are_refused_before_the_llm_is_asked():
    llm = RecordingLLM()
    servicer = AuctionServicer(AuctionState(), llm)
    future = time_conv.dt_to_micros(time_conv.now_utc() + datetime.timedelta(hours=1))
    r = servicer.CreateAuction(auction_pb2.CreateAuctionRequest(
        auction_id=1, item="clock", close_time=future, attributes={f"k{i}": "v" for i in range(9)}), Ctx())
    assert not r.result.success and "At most 8 attributes" in r.result.reason
    assert llm.calls == []


def test_the_owner_comes_back_on_reads():
    servicer = AuctionServicer(AuctionState(), RecordingLLM())
    future = time_conv.dt_to_micros(time_conv.now_utc() + datetime.timedelta(hours=1))
    assert servicer.CreateAuction(auction_pb2.CreateAuctionRequest(
        auction_id=7, item="clock", close_time=future), Ctx()).result.success
    assert servicer.GetAuction(auction_pb2.GetAuctionRequest(auction_id=7), Ctx()).auction.owner == "nisarg"
    listed = servicer.ListAuctions(auction_pb2.ListAuctionsRequest(), Ctx()).auctions
    assert [(a.auction_id, a.owner) for a in listed] == [(7, "nisarg")]


# --- accounts ---------------------------------------------------------------------


@pytest.mark.parametrize("name,ok", [
    ("alice", True), ("a_1", True), ("ab", False), ("Alice", False), ("1abc", False),
    ("al ice", False), ("a" * 21, False), ("", False), ("bob!", False),
])
def test_username_rule(name, ok):
    assert (username_rejection(name) is None) is ok


@pytest.mark.parametrize("pw,ok", [("pass", True), ("abc", False), ("x" * 65, False), (" pass", False)])
def test_password_rule(pw, ok):
    assert (password_rejection(pw) is None) is ok


def register(servicer, username, password, seq=1):
    return servicer.Register(auction_pb2.RegisterRequest(
        username=username, password=password, client_id="c", seq=seq), None)


def login(servicer, username, password, seq=1):
    return servicer.Login(auction_pb2.LoginRequest(
        username=username, password=password, client_id=f"l-{username}-{seq}", seq=seq), None)


def test_register_then_log_in():
    auth = AuthServicer(SessionStore())
    r = register(auth, "alice", "s3cret")
    assert r.result.success, r.result.reason
    assert login(auth, "alice", "s3cret").result.success
    assert not login(auth, "alice", "wrong", seq=2).result.success


@pytest.mark.parametrize("name", ["alice", "nisarg"])
def test_a_taken_username_is_refused(name):
    auth = AuthServicer(SessionStore())
    register(auth, "alice", "s3cret")
    r = register(auth, name, "other1", seq=2)
    assert not r.result.success and "already taken" in r.result.reason


def test_bad_registration_input_is_refused_before_anything_is_proposed():
    store = SessionStore()
    auth = AuthServicer(store)
    assert "3-20 characters" in register(auth, "X", "s3cret").result.reason
    assert "at least 4" in register(auth, "alice", "abc").result.reason
    assert store._state.users_snapshot() == {}


def test_the_log_holds_a_salted_hash_never_the_password():
    sm = StateMachine()
    store = SessionStore(sm.sessions, None)
    # Capture what would be replicated.
    sent = []
    store._replicator = type("R", (), {
        "submit": lambda self, c, cid="", seq=0: (sent.append(c), sm.apply_fn(1, codec.encode(c, cid, seq)))[1],
        "check_leader": lambda self: None, "read_barrier": lambda self: 0,
    })()
    assert store.register("alice", "s3cret").success
    (cmd,) = sent
    assert isinstance(cmd, RegisterUser) and cmd.username == "alice"
    assert "s3cret" not in codec.encode(cmd).decode("latin-1")
    assert cmd.password_hash == hash_password("s3cret", cmd.salt)


def test_a_registered_account_survives_replay():
    salt = "00" * 16
    data = codec.encode(RegisterUser("alice", salt, hash_password("s3cret", salt)), "c", 1)
    sm = StateMachine()
    sm.apply_fn(1, data)
    assert sm.sessions.check_password("alice", "s3cret")
    assert not sm.sessions.check_password("alice", "nope")


def test_apply_refuses_a_duplicate_or_malformed_register():
    s = SessionState()
    assert s.apply(RegisterUser("alice", "aa", "bb")).success
    assert not s.apply(RegisterUser("alice", "cc", "dd")).success
    assert not s.apply(RegisterUser("karan", "cc", "dd")).success  # seeded
    assert not s.apply(RegisterUser("Bad Name", "cc", "dd")).success
    assert not s.apply(RegisterUser("carol", "", "")).success


def test_the_dump_shows_usernames_but_no_salt_or_hash():
    sm = StateMachine()
    salt, h = "ab" * 16, "cd" * 32
    sm.apply_fn(1, codec.encode(RegisterUser("alice", salt, h)))
    text = digest.canonical_json(sm.snapshot())
    assert '"alice"' in text and salt not in text and h not in text


# --- the checker ------------------------------------------------------------------


def test_the_checker_accepts_a_true_seller_refusal():
    history, dumps, leaders = scenario()
    history.append(rec("seller", 9, "bid", "rejected", 5, auction_id=1, amount=999,
                       reason="seller is the seller of this auction: a seller cannot bid on their own auction"))
    assert P.REJECTIONS not in failed(check_all(history, dumps, leaders))


def test_the_checker_catches_a_false_seller_refusal():
    history, dumps, leaders = scenario()
    history.append(rec("karan", 9, "bid", "rejected", 5, auction_id=1, amount=999,
                       reason="karan is the seller of this auction: a seller cannot bid on their own auction"))
    report = check_all(history, dumps, leaders)
    assert P.REJECTIONS in failed(report)
    assert "not the seller" in evidence(report, P.REJECTIONS)


def test_the_checker_understands_a_non_positive_refusal():
    history, dumps, leaders = scenario()
    history.append(rec("karan", 9, "bid", "rejected", 5, auction_id=1, amount=0,
                       reason="Bid amount must be a positive whole number, not 0"))
    assert P.REJECTIONS not in failed(check_all(history, dumps, leaders))


# --- the dashboard's first layer --------------------------------------------------


@pytest.mark.parametrize("payload,field", [
    ({"auction_id": 1, "amount": "100"}, "amount"),
    ({"auction_id": 1, "amount": 10.5}, "amount"),
    ({"auction_id": 1, "amount": True}, "amount"),
    ({"auction_id": 1, "amount": 0}, "amount"),
    ({"auction_id": 1, "amount": 10**13}, "amount"),
    ({"auction_id": "1", "amount": 5}, "auction_id"),
])
def test_dashboard_bid_validation(payload, field):
    with pytest.raises(Invalid) as e:
        validate.bid(payload)
    assert e.value.field == field


def test_dashboard_bid_ok_and_skip_flag_is_strict():
    assert validate.bid({"auction_id": 3, "amount": 7.0}) == {"auction_id": 3, "amount": 7, "skip_precheck": False}
    assert validate.bid({"auction_id": 3, "amount": 7, "skip_precheck": "yes"})["skip_precheck"] is False


@pytest.mark.parametrize("payload,field", [
    ({"item": "", "duration_seconds": 60}, "item"),
    ({"item": "x" * 101, "duration_seconds": 60}, "item"),
    ({"item": "clock", "duration_seconds": 5}, "duration_seconds"),
    ({"item": "clock", "duration_seconds": 60, "attributes": {"a": ""}}, "attributes"),
    ({"item": "clock", "duration_seconds": 60, "attributes": [1]}, "attributes"),
    ({"item": "clock", "duration_seconds": 60, "attributes": {f"k{i}": "v" for i in range(9)}}, "attributes"),
])
def test_dashboard_create_validation(payload, field):
    with pytest.raises(Invalid) as e:
        validate.new_auction(payload)
    assert e.value.field == field


def test_dashboard_create_drops_empty_attribute_rows():
    v = validate.new_auction({"item": " clock ", "duration_seconds": 60, "attributes": {" brand ": " Seiko ", "": ""}})
    assert v == {"item": "clock", "duration_seconds": 60, "attributes": {"brand": "Seiko"}}


def test_dashboard_registration_validation():
    with pytest.raises(Invalid):
        validate.registration({"username": "Bob", "password": "pass"})
    with pytest.raises(Invalid) as e:
        validate.registration({"username": "bob", "password": "pass", "confirm": "pas"})
    assert e.value.field == "confirm"
    assert validate.registration({"username": "bob", "password": "pass", "confirm": "pass"}) == ("bob", "pass")


def test_dashboard_rejects_a_non_object_body():
    for bad in ([], "x", 3, None):
        with pytest.raises(Invalid):
            validate.bid(bad)


# --- the per-auction assistant knows the seller and who is asking -------------------


def faq_system(asker):
    from generated import llm_pb2
    from llm_server.llm_servicer import build_faq_messages

    req = llm_pb2.BiddingQuestionRequest(request_id="r", question="Can I bid?", asker=asker)
    req.auction.CopyFrom(llm_pb2.AuctionContext(auction_id=1, item="clock", close_time=0, seller="nisarg"))
    return build_faq_messages(req)[0]["content"]


def test_the_faq_prompt_states_the_seller_rule_and_the_seller():
    text = faq_system("karan")
    assert "seller of an auction cannot bid on their own auction" in text
    assert "seller:           nisarg" in text
    assert "karan, a bidder (not the seller)" in text


def test_the_faq_prompt_tells_the_model_when_the_seller_asks():
    assert "nisarg, the SELLER of this auction: they cannot bid on it" in faq_system("nisarg")


def test_the_assistant_passes_the_asker_from_the_token():
    from generated import assistant_pb2
    from server.assistant_servicer import AssistantServicer

    state = created("nisarg")
    llm = RecordingLLM()
    llm.get_llm_answer = lambda request_id, query, context: (llm.calls.append(context), (request_id, "no"))[1]
    r = AssistantServicer(state, llm).AskAssistant(assistant_pb2.AskRequest(question="Can I bid?", auction_id=1), Ctx())
    assert r.result.success
    assert llm.calls[0]["asker"] == "nisarg" and llm.calls[0]["auction"]["seller"] == "nisarg"


def test_the_faq_prompt_says_whether_the_asker_holds_the_high_bid():
    from generated import llm_pb2
    from llm_server.llm_servicer import build_faq_messages

    def system(asker):
        req = llm_pb2.BiddingQuestionRequest(request_id="r", question="Am I winning?", asker=asker)
        req.auction.CopyFrom(llm_pb2.AuctionContext(auction_id=1, item="clock", close_time=0, seller="nisarg",
                                                    current_high_bid=150, current_high_bidder="karan"))
        return build_faq_messages(req)[0]["content"]

    assert "high bidder:      karan" in system("karan")
    assert "They hold the current high bid." in system("karan")
    assert "They do not hold the current high bid." in system("mrudul")


def test_the_faq_prompt_says_whether_the_asker_holds_the_high_bid():
    from generated import llm_pb2
    from llm_server.llm_servicer import build_faq_messages

    def system(asker):
        req = llm_pb2.BiddingQuestionRequest(request_id="r", question="Am I winning?", asker=asker)
        req.auction.CopyFrom(llm_pb2.AuctionContext(auction_id=1, item="clock", close_time=0, seller="nisarg",
                                                    current_high_bid=150, current_high_bidder="karan"))
        return build_faq_messages(req)[0]["content"]

    assert "high bidder:      karan" in system("karan")
    assert "They hold the current high bid." in system("karan")
    assert "They do not hold the current high bid." in system("mrudul")
