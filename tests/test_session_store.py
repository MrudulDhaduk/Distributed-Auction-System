"""
Specifies SessionStore: create()/validate()/destroy() over an in-memory,
non-replicated token -> username mapping.

Written against the real contract in sessions/store.py:

- SessionStore().create(username) -> token (str)
- SessionStore().validate(token) -> username (str) or None
- SessionStore().destroy(token) -> None, idempotent
"""

from sessions.store import SessionStore


def test_create_returns_token_and_validate_maps_it_back():
    store = SessionStore()

    token = store.create("alice")

    assert isinstance(token, str) and token
    assert store.validate(token) == "alice"


def test_validate_unknown_token_returns_none():
    store = SessionStore()

    assert store.validate("this-token-was-never-issued") is None


def test_destroy_removes_the_session():
    store = SessionStore()
    token = store.create("alice")

    store.destroy(token)

    assert store.validate(token) is None


def test_double_destroy_does_not_raise():
    store = SessionStore()
    token = store.create("alice")

    store.destroy(token)
    store.destroy(token)  # must not raise


def test_two_creates_for_same_username_return_different_tokens_both_valid():
    store = SessionStore()

    first = store.create("alice")
    second = store.create("alice")

    assert first != second
    assert store.validate(first) == "alice"
    assert store.validate(second) == "alice"
