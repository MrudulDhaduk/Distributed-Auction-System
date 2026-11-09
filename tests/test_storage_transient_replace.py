"""FileStorage.set_term_and_vote retries a transient PermissionError from
os.replace (docs/decisions.md T17).

Found by the failover stress run (runs/stress-20261109-222836/
failover-0.15-0.3/logs/n3.log, 22:49:26.435): os.replace(tmp, state.json)
raised PermissionError [WinError 5] "Access is denied" during a step-down.
FileStorage fail-stopped (P10), and n3 could never change term again: dead,
with a healthy disk. On Windows, os.replace onto a file another process has
open at that instant (antivirus, search indexer) is refused, and a moment
later it succeeds. This was a strict xfail until the fix.

The rule now: retry os.replace on PermissionError, up to REPLACE_RETRIES
times, sleeping 10 ms doubling, at most REPLACE_BUDGET seconds in total;
only then fail-stop as before. Any other error still fail-stops at once.
"""

import json
import logging
import os

import pytest

from raft import file_storage


def flaky(monkeypatch, failures):
    """Make os.replace refuse `failures` times, then behave normally.
    Returns the call counter."""
    real_replace = os.replace
    calls = {"n": 0}

    def replace(src, dst):
        calls["n"] += 1
        if calls["n"] <= failures:
            raise PermissionError(13, "Access is denied", dst)
        return real_replace(src, dst)

    monkeypatch.setattr(file_storage.os, "replace", replace)
    return calls


def on_disk(storage):
    with open(os.path.join(storage.path, file_storage.STATE_FILE), encoding="utf-8") as f:
        return json.load(f)


def test_a_transient_access_denied_on_replace_is_retried_not_fatal(open_storage, monkeypatch):
    storage = open_storage("n1")
    calls = flaky(monkeypatch, failures=1)

    storage.set_term_and_vote(5, "n2")

    assert calls["n"] == 2
    assert (storage.term, storage.voted_for) == (5, "n2")
    assert on_disk(storage) == {"term": 5, "voted_for": "n2"}
    storage.set_term_and_vote(6, None)  # still writable afterwards
    assert storage.term == 6


def test_three_refusals_then_success(open_storage, monkeypatch, caplog):
    storage = open_storage("n1")
    calls = flaky(monkeypatch, failures=3)

    with caplog.at_level(logging.WARNING, logger="raft.storage"):
        storage.set_term_and_vote(7, "n3")

    assert calls["n"] == 4
    assert (storage.term, storage.voted_for) == (7, "n3")
    assert on_disk(storage) == {"term": 7, "voted_for": "n3"}
    retries = [r for r in caplog.records if r.levelno == logging.WARNING and "retry" in r.getMessage()]
    assert len(retries) == 3  # one WARNING per retry, so chaos runs show it
    assert "retry 1/" in retries[0].getMessage() and "retry 3/" in retries[2].getMessage()


def test_a_refusal_that_persists_still_fail_stops(open_storage, monkeypatch):
    """Retries are bounded: a file that stays locked is a real failure, and
    P10 applies exactly as before. The old state stays on disk and in
    memory."""
    storage = open_storage("n1")
    storage.set_term_and_vote(2, "n1")
    calls = flaky(monkeypatch, failures=10**6)

    with pytest.raises(PermissionError):
        storage.set_term_and_vote(3, "n2")

    assert 2 <= calls["n"] <= file_storage.REPLACE_RETRIES + 1  # retried, but bounded
    assert (storage.term, storage.voted_for) == (2, "n1")  # memory unchanged
    assert on_disk(storage) == {"term": 2, "voted_for": "n1"}  # disk unchanged
    with pytest.raises(file_storage.StorageFailedError):
        storage.set_term_and_vote(4, None)


def test_retries_stay_within_the_time_budget(open_storage, monkeypatch):
    slept = []
    monkeypatch.setattr(file_storage.time, "sleep", slept.append)
    storage = open_storage("n1")
    flaky(monkeypatch, failures=10**6)

    with pytest.raises(PermissionError):
        storage.set_term_and_vote(3, "n2")

    assert len(slept) <= file_storage.REPLACE_RETRIES
    assert slept[:5] == pytest.approx([0.010, 0.020, 0.040, 0.080, 0.160])  # 10 ms doubling
    assert sum(slept) <= file_storage.REPLACE_BUDGET + 1e-9


def test_any_other_error_is_not_retried(open_storage, monkeypatch):
    storage = open_storage("n1")
    calls = {"n": 0}

    def broken(src, dst):
        calls["n"] += 1
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(file_storage.os, "replace", broken)
    with pytest.raises(OSError):
        storage.set_term_and_vote(3, "n2")
    assert calls["n"] == 1
    with pytest.raises(file_storage.StorageFailedError):
        storage.set_term_and_vote(4, None)
