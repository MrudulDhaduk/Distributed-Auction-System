"""BUG FOUND BY THE FAILOVER STRESS RUN, pinned here, NOT fixed (chunk 5 does
not change raft/; docs/decisions.md T17, testing-and-chaos.md Known gaps).

What happened: runs/stress-20261109-222836/failover-0.15-0.3/logs/n3.log,
22:49:26.435:

    write failed, refusing all further writes: PermissionError(13,
    'Access is denied') ... os.replace(tmp, .../state.json)

raised from FileStorage.set_term_and_vote (raft/file_storage.py:301) while
stepping down for a RequestVote. FileStorage then fail-stops by design
(P10), so every later term change raised StorageFailedError: n3 was dead for
the rest of the run, with a perfectly healthy disk.

Why: on Windows, os.replace() onto a file that another process has open
right now (antivirus and the search indexer both open freshly written
files) fails with ERROR_ACCESS_DENIED / sharing violation. It is TRANSIENT:
a moment later the same call succeeds. With 0.15 s election timeouts the
term changes hundreds of times in a run, so the window is hit. (Which
process held the file is not known: nothing logged it. That it is a
transient lock and not a real permission problem is an inference: the same
directory was written thousands of times before and after.)

The safety side is fine: the write raised, so the caller never acted on an
undurable term or vote. The bug is availability: P10 treats a transient
Windows sharing violation like a dead disk.

What a fix should do (for the step that touches raft/): retry os.replace a
few times with a short sleep on PermissionError on Windows, and fail-stop
only if it persists. This test states that behaviour and fails today.
"""

import os

import pytest

from raft import file_storage


@pytest.mark.xfail(strict=True, reason="BUG (T17): a transient PermissionError from os.replace on Windows "
                   "fail-stops FileStorage for good; it should be retried")
def test_a_transient_access_denied_on_replace_is_retried_not_fatal(open_storage, monkeypatch):
    storage = open_storage("n1")
    real_replace = os.replace
    calls = {"n": 0}

    def flaky_replace(src, dst):
        calls["n"] += 1
        if calls["n"] == 1:
            raise PermissionError(13, "Access is denied", dst)
        return real_replace(src, dst)

    monkeypatch.setattr(file_storage.os, "replace", flaky_replace)
    storage.set_term_and_vote(5, "n2")  # today: raises PermissionError and fail-stops

    assert (storage.term, storage.voted_for) == (5, "n2")
    storage.set_term_and_vote(6, None)  # still writable afterwards
    assert storage.term == 6


def test_today_one_transient_access_denied_kills_the_storage(open_storage, monkeypatch):
    """The current behaviour, pinned so the change is visible when it comes."""
    storage = open_storage("n1")
    real_replace = os.replace
    calls = {"n": 0}

    def flaky_replace(src, dst):
        calls["n"] += 1
        if calls["n"] == 1:
            raise PermissionError(13, "Access is denied", dst)
        return real_replace(src, dst)

    monkeypatch.setattr(file_storage.os, "replace", flaky_replace)
    with pytest.raises(PermissionError):
        storage.set_term_and_vote(5, "n2")
    with pytest.raises(file_storage.StorageFailedError):
        storage.set_term_and_vote(6, None)  # the disk is fine; the storage is not
