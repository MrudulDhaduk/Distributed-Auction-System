"""Specifies FileStorage (build order step 6): Figure 2's persistent state --
currentTerm, votedFor, log[] -- "Updated on stable storage before
responding to RPCs".

These tests talk to the storage alone; no node, no threads, no clock. They
are deterministic. A "crash" is opening a second FileStorage on the same
directory while the first is still open and never closed: nothing the
first instance held in memory is visible to the second, only the files.
"""

import os

import pytest

from raft import file_storage
from raft.file_storage import (
    LOG_FILE,
    STATE_FILE,
    STATE_TMP,
    CorruptStorageError,
    FileStorage,
    StorageFailedError,
)
from raft.messages import EntryKind, LogEntry
from raft.storage import MemoryStorage


def log_path(storage_dir):
    return os.path.join(storage_dir, LOG_FILE)


def size(path):
    return os.path.getsize(path)


def three_entries():
    return [LogEntry(1, b"a"), LogEntry(1, b"", EntryKind.NOOP), LogEntry(2, b"\x00\xffbin")]


def all_entries(s):
    return list(s.entries_from(1, s.last_index()))


# --- what survives a restart ---------------------------------------------------


def test_a_fresh_directory_is_first_boot(open_storage):
    # Figure 2: currentTerm "initialized to 0 on first boot".
    s = open_storage()
    assert (s.term, s.voted_for, s.last_index(), s.term_at(0)) == (0, None, 0, 0)


def test_term_vote_and_log_survive_close_and_reopen(open_storage):
    s = open_storage()
    s.set_term_and_vote(2, "n3")
    s.append(three_entries())
    s.close()

    r = open_storage()
    assert (r.term, r.voted_for) == (2, "n3")
    assert all_entries(r) == three_entries()
    assert [r.term_at(i) for i in range(4)] == [0, 1, 1, 2]


def test_everything_survives_a_crash_without_close(open_storage):
    s = open_storage()
    s.set_term_and_vote(4, "n1")
    s.append([LogEntry(4, b"x")])
    # Crash: s is abandoned, never closed. Durability must not depend on a
    # clean shutdown, because a killed process gets none.
    r = open_storage()
    assert (r.term, r.voted_for, all_entries(r)) == (4, "n1", [LogEntry(4, b"x")])


def test_a_vote_can_be_cleared_and_the_clear_survives(open_storage):
    s = open_storage()
    s.set_term_and_vote(3, "n2")
    s.set_term_and_vote(4, None)  # a higher term: the ballot is blank again
    r = open_storage()
    assert (r.term, r.voted_for) == (4, None)


def test_truncate_from_survives_reopen(open_storage):
    s = open_storage()
    s.set_term_and_vote(3, None)
    s.append(three_entries())
    s.truncate_from(2)
    s.append([LogEntry(3, b"c")])  # the follower's step 3 then step 4

    r = open_storage()
    assert all_entries(r) == [LogEntry(1, b"a"), LogEntry(3, b"c")]


def test_truncating_everything_survives_reopen(open_storage):
    s = open_storage()
    s.set_term_and_vote(1, None)
    s.append(three_entries()[:2])
    s.truncate_from(1)
    assert open_storage().last_index() == 0


def test_file_storage_behaves_exactly_like_memory_storage():
    """The node must not be able to tell the two apart: same operations,
    same answers to every read. (Interface parity, so node.py needs no
    change.)"""
    import tempfile

    with tempfile.TemporaryDirectory(prefix="raft-test-") as d:
        f, m = FileStorage(d), MemoryStorage()
        try:
            ops = [
                ("set_term_and_vote", 1, "n1"),
                ("append", [LogEntry(1, b"a"), LogEntry(1, b"b")]),
                ("set_term_and_vote", 2, None),
                ("set_term_and_vote", 2, "n2"),
                ("append", [LogEntry(2, b"", EntryKind.NOOP)]),
                ("truncate_from", 2),
                ("append", [LogEntry(2, b"c"), LogEntry(2, b"d")]),
            ]
            for name, *args in ops:
                getattr(f, name)(*args)
                getattr(m, name)(*args)
                assert (f.term, f.voted_for, f.last_index()) == (m.term, m.voted_for, m.last_index())
                assert [f.term_at(i) for i in range(f.last_index() + 1)] == [
                    m.term_at(i) for i in range(m.last_index() + 1)
                ]
                assert f.entries_from(1, 99) == m.entries_from(1, 99)
                assert f.entries_from(2, 1) == m.entries_from(2, 1)
            with pytest.raises(IndexError):
                f.entry(0)
            with pytest.raises(IndexError):
                f.truncate_from(0)
        finally:
            f.close()


# --- torn tails and corruption ---------------------------------------------------


def _tear(path, how, good_size):
    """Damage the bytes after `good_size` (the last record) the way a crash
    in the middle of its write can."""
    with open(path, "r+b") as fh:
        data = fh.read()
        last = data[good_size:]
        if how == "cut in header":
            fh.truncate(good_size + 5)
        elif how == "cut in payload":
            fh.truncate(len(data) - 2)
        elif how == "zeros, never written":
            # NTFS after a power cut: the size grew, the data never landed.
            fh.seek(good_size)
            fh.write(b"\x00" * len(last))
        elif how == "bad payload checksum":
            fh.seek(len(data) - 1)
            fh.write(bytes([data[-1] ^ 0xFF]))
        else:
            raise AssertionError(how)


@pytest.mark.parametrize(
    "how", ["cut in header", "cut in payload", "zeros, never written", "bad payload checksum"]
)
def test_a_torn_last_record_is_discarded_on_open(open_storage, data_dir, how):
    s = open_storage()
    s.set_term_and_vote(2, None)
    s.append([LogEntry(1, b"a"), LogEntry(2, b"b")])
    good = size(log_path(s.path))
    s.append([LogEntry(2, b"the torn one")])
    s.close()
    _tear(log_path(s.path), how, good)

    r = open_storage()
    assert all_entries(r) == [LogEntry(1, b"a"), LogEntry(2, b"b")]
    # The torn bytes are cut off the file on open, so the next append
    # lands right after the last good record ...
    assert size(log_path(r.path)) == good
    r.append([LogEntry(2, b"c")])
    r.close()
    # ... and a later reopen does not see the old damage as a bad record
    # in the MIDDLE (which would be a hard error).
    assert all_entries(open_storage()) == [LogEntry(1, b"a"), LogEntry(2, b"b"), LogEntry(2, b"c")]


def test_a_torn_record_from_a_multi_entry_append_keeps_the_complete_ones(open_storage):
    # One append() writes its whole batch in one go; a crash part-way can
    # leave its first records complete. Keeping them is safe: append()
    # never returned, so this is just a follower that took fewer entries.
    s = open_storage()
    s.set_term_and_vote(1, None)
    s.append([LogEntry(1, b"a"), LogEntry(1, b"b"), LogEntry(1, b"c")])
    s.close()
    with open(log_path(s.path), "r+b") as fh:
        fh.truncate(size(log_path(s.path)) - 1)
    assert all_entries(open_storage()) == [LogEntry(1, b"a"), LogEntry(1, b"b")]


@pytest.mark.parametrize("where", ["payload", "header length", "header checksum"])
def test_a_corrupt_record_in_the_middle_is_a_hard_error(open_storage, where):
    s = open_storage()
    s.set_term_and_vote(1, None)
    s.append([LogEntry(1, b"first")])
    s.append([LogEntry(1, b"second"), LogEntry(1, b"third")])
    s.close()
    offset = {"payload": file_storage.HEADER.size + 2, "header length": 1, "header checksum": 5}[where]
    with open(log_path(s.path), "r+b") as fh:
        fh.seek(offset)
        byte = fh.read(1)
        fh.seek(offset)
        fh.write(bytes([byte[0] ^ 0x01]))

    # Records after it are intact, so this cannot be an unfinished last
    # write. Skipping it would silently drop an entry this node may have
    # acknowledged -- refuse to start instead.
    with pytest.raises(CorruptStorageError):
        open_storage()


def test_a_damaged_state_file_is_a_hard_error(open_storage):
    s = open_storage()
    s.set_term_and_vote(5, "n2")
    s.close()
    with open(os.path.join(s.path, STATE_FILE), "wb") as fh:
        fh.write(b'{"term": 5, "voted_fo')
    with pytest.raises(CorruptStorageError):
        open_storage()


def test_a_leftover_temp_state_file_is_ignored(open_storage):
    # A crash between writing the temp file and os.replace: the replace
    # never happened, so set_term_and_vote never returned, so nobody was
    # told about the new term or vote. The old state.json is the truth.
    s = open_storage()
    s.set_term_and_vote(5, "n2")
    s.close()
    with open(os.path.join(s.path, STATE_TMP), "wb") as fh:
        fh.write(b'{"term": 6, "vot')
    r = open_storage()
    assert (r.term, r.voted_for) == (5, "n2")
    assert not os.path.exists(os.path.join(s.path, STATE_TMP))


def test_a_log_without_a_state_file_is_a_hard_error(open_storage):
    # The state file is written before the first entry can exist (an entry
    # carries a term > 0), so a log with no state file means lost state.
    s = open_storage()
    s.set_term_and_vote(1, None)
    s.append([LogEntry(1, b"a")])
    s.close()
    os.remove(os.path.join(s.path, STATE_FILE))
    with pytest.raises(CorruptStorageError):
        open_storage()


def test_a_log_entry_above_current_term_is_a_hard_error(open_storage):
    s = open_storage()
    s.set_term_and_vote(3, None)
    s.append([LogEntry(3, b"a")])
    s.close()
    with open(os.path.join(s.path, STATE_FILE), "wb") as fh:
        fh.write(b'{"term": 1, "voted_for": null}')
    with pytest.raises(CorruptStorageError):
        open_storage()


# --- durable before returning ------------------------------------------------------


@pytest.fixture
def fsyncs(monkeypatch):
    """Records the size of the file behind every os.fsync call, at the
    moment of the call, then really fsyncs."""
    calls = []
    real = os.fsync

    def recording_fsync(fd):
        calls.append(os.fstat(fd).st_size)
        real(fd)

    monkeypatch.setattr(os, "fsync", recording_fsync)
    return calls


def test_every_write_method_fsyncs_before_it_returns(open_storage, fsyncs):
    s = open_storage()
    for name, args in [
        ("set_term_and_vote", (1, "n1")),
        ("append", ([LogEntry(1, b"a"), LogEntry(1, b"b")],)),
        ("truncate_from", (2,)),
    ]:
        before = len(fsyncs)
        getattr(s, name)(*args)
        assert len(fsyncs) > before, f"{name} returned without calling fsync"
    # The append's fsync saw the file with both records in it, and the
    # truncate's fsync saw it with one record: each fsync came AFTER its
    # write, on the bytes that the write produced.
    one_record = file_storage.HEADER.size + file_storage.PAYLOAD_HEAD.size + 1
    assert fsyncs[-2] == 2 * one_record
    assert fsyncs[-1] == one_record


def test_term_and_vote_are_fsynced_before_the_rename(open_storage, monkeypatch):
    s = open_storage()
    s.set_term_and_vote(1, "n1")
    state = os.path.join(s.path, STATE_FILE)
    seen = []
    real = os.fsync

    def checking_fsync(fd):
        with open(state, "rb") as fh:
            seen.append(fh.read())
        real(fd)

    monkeypatch.setattr(os, "fsync", checking_fsync)
    s.set_term_and_vote(2, "n3")
    # While the temp file was being fsynced, state.json still held the OLD
    # term and vote: the new bytes were durable before the rename made
    # them the real file.
    assert seen and b'"term": 1' in seen[0]
    with open(state, "rb") as fh:
        assert b'"term": 2' in fh.read()


def test_empty_append_writes_nothing(open_storage, fsyncs):
    s = open_storage()
    s.append([])
    assert fsyncs == []


def test_a_failed_write_poisons_the_storage(open_storage, monkeypatch):
    s = open_storage()
    s.set_term_and_vote(1, None)

    def broken_fsync(fd):
        raise OSError("disk on fire")

    monkeypatch.setattr(os, "fsync", broken_fsync)
    with pytest.raises(OSError):
        s.append([LogEntry(1, b"a")])
    monkeypatch.undo()
    # The bytes may or may not be on disk now; the in-memory copy and the
    # file can disagree. Fail-stop: no further write is attempted.
    assert s.last_index() == 0
    with pytest.raises(StorageFailedError):
        s.append([LogEntry(1, b"b")])
    with pytest.raises(StorageFailedError):
        s.set_term_and_vote(2, None)


def test_a_closed_storage_refuses_writes(open_storage):
    s = open_storage()
    s.close()
    s.close()  # idempotent
    with pytest.raises(StorageFailedError):
        s.set_term_and_vote(1, None)
