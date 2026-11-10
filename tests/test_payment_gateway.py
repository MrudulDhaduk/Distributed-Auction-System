"""Specifies server/payment_gateway.py: the mock payment gateway.

It lives outside apply(). The leader's handler calls it BEFORE proposing and
puts the receipt's reference on the command as plain data. Properties the
rest of the design leans on:

  * idempotent by key: the same (payer, amount, auction, key) always gives
    the same reference, on any gateway instance. That is what makes a
    client's retry after an unknown outcome safe, including on a NEW leader
    (whose in-process gateway has never heard of the first attempt);
  * a decline is a normal answer (approved=False), not an exception;
  * every call is recorded, so a test can say "the gateway was called once".
"""

import pytest

from server.payment_gateway import MockPaymentGateway, Receipt


def test_an_authorization_is_approved_and_returns_a_reference():
    receipt = MockPaymentGateway().authorize("bob", 100, 1, idempotency_key="c1:7")

    assert receipt.approved and receipt.reason == ""
    assert receipt.reference.startswith("PAY-") and len(receipt.reference) > len("PAY-")


def test_the_same_key_gives_the_same_reference_on_a_different_gateway():
    first = MockPaymentGateway().authorize("bob", 100, 1, idempotency_key="c1:7")
    second = MockPaymentGateway().authorize("bob", 100, 1, idempotency_key="c1:7")

    assert first == second


@pytest.mark.parametrize("other", [
    dict(payer="alice", amount=100, auction_id=1, idempotency_key="c1:7"),
    dict(payer="bob", amount=101, auction_id=1, idempotency_key="c1:7"),
    dict(payer="bob", amount=100, auction_id=2, idempotency_key="c1:7"),
    dict(payer="bob", amount=100, auction_id=1, idempotency_key="c1:8"),
])
def test_anything_different_gives_a_different_reference(other):
    base = MockPaymentGateway().authorize("bob", 100, 1, idempotency_key="c1:7")

    assert MockPaymentGateway().authorize(**other).reference != base.reference


def test_without_a_key_every_call_gets_a_fresh_reference():
    gw = MockPaymentGateway()

    assert gw.authorize("bob", 100, 1).reference != gw.authorize("bob", 100, 1).reference


@pytest.mark.parametrize("amount", [0, -5])
def test_a_non_positive_amount_is_declined(amount):
    receipt = MockPaymentGateway().authorize("bob", amount, 1, idempotency_key="k")

    assert receipt == Receipt(approved=False, reference="", reason=receipt.reason)
    assert "amount" in receipt.reason


def test_an_amount_over_the_limit_is_declined_and_one_at_the_limit_is_not():
    gw = MockPaymentGateway(decline_over=1000)

    assert gw.authorize("bob", 1000, 1, idempotency_key="k").approved
    declined = gw.authorize("bob", 1001, 1, idempotency_key="k")
    assert not declined.approved and declined.reference == ""
    assert "limit" in declined.reason


def test_release_and_refund_and_void_answer_for_a_payment_reference():
    gw = MockPaymentGateway()
    ref = gw.authorize("bob", 100, 1, idempotency_key="k").reference

    released, refunded, voided = gw.release(ref), gw.refund(ref), gw.void(ref)

    assert released.approved and released.reference.startswith("REL-")
    assert refunded.approved and refunded.reference.startswith("RFD-")
    assert voided.approved and voided.reference.startswith("VOID-")
    assert gw.release(ref) == released  # same input, same receipt


@pytest.mark.parametrize("op", ["release", "refund", "void"])
@pytest.mark.parametrize("ref", ["", "nonsense", "REL-123"])
def test_settling_something_the_gateway_never_issued_is_declined(op, ref):
    receipt = getattr(MockPaymentGateway(), op)(ref)

    assert not receipt.approved and receipt.reference == ""


def test_every_call_is_recorded_in_order():
    gw = MockPaymentGateway()
    ref = gw.authorize("bob", 100, 1, idempotency_key="k").reference
    gw.release(ref)
    gw.authorize("bob", 0, 1, idempotency_key="k2")  # declined, still recorded

    assert [c[0] for c in gw.calls] == ["authorize", "release", "authorize"]
    assert gw.calls[0] == ("authorize", "bob", 100, 1, "k")
