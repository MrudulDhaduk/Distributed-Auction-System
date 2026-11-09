"""FileStorage: Figure 2's persistent state on disk.

    Persistent state on all servers:
    (Updated on stable storage before responding to RPCs)
    currentTerm, votedFor, log[]

Same interface as MemoryStorage (raft/storage.py), so raft/node.py cannot
tell the two apart. The node changes its persistent state only through the
three write methods; this class makes each of them DURABLE (written, then
os.fsync'd) BEFORE IT RETURNS. The node calls them under its lock and only
builds its reply after they return, so nothing is ever said to another
node about state that a crash could take back. (docs/raft-persistence.md,
"durable before responding".)

Files, in one directory per node
--------------------------------
  state.json   {"term": 3, "voted_for": "n2"}
               currentTerm and votedFor, always written TOGETHER, as a whole
               new file: write state.json.tmp, fsync it, os.replace() it
               over state.json. A reader sees the old file or the new one,
               never half of each.
  log.bin      log[1..], append-only, one record per entry. Big-endian:
                 header   length u32          (bytes in the payload)
                          crc32(length) u32
                          crc32(payload) u32
                 payload  term u64, kind u8 (1 COMMAND, 2 NOOP),
                          data (length - 9 bytes, opaque)
               The length has its own checksum so that a damaged length
               field is caught, instead of sending the reader off to a
               wrong "end of record". truncate_from cuts the file at the
               byte offset where entry `index` starts (kept in memory).

Recovery when the directory is opened
-------------------------------------
The log is scanned from the start. A crash in the middle of an append can
leave only these things after the last complete record, and each one is a
"torn tail" that is cut off the file:
  * fewer bytes than a header;
  * nothing but zero bytes (NTFS after a power cut: the file grew, the
    data never landed);
  * a header whose payload runs past the end of the file;
  * a complete LAST record whose payload fails its checksum.
Discarding it is safe because that append never returned: no reply was
built after it, nobody was told this node holds the entry. Anything else
-- a bad header checksum, or a bad payload with more records after it --
is not something an unfinished last write can produce. It is corruption,
and opening fails with CorruptStorageError. Skipping it would silently
drop an entry this node may have acknowledged, which can lose a committed
entry; refusing to start is the only safe answer.

What fsync does and does not promise (this is Windows)
------------------------------------------------------
On Windows os.fsync is FlushFileBuffers: the file's data AND its metadata
(size) are written to the device, and the device is asked to flush its own
write cache, before the call returns. Limits:
  * Directories cannot be fsync'd: os.open() on a directory raises
    PermissionError. A rename (os.replace) and the creation of a file are
    changes to the DIRECTORY. After os.replace returns, every process sees
    the new state.json, so a PROCESS crash (kill, exception, Ctrl+C) is
    covered completely. After a POWER LOSS or OS crash, NTFS's journal
    keeps the directory consistent (old file or new, never neither), but
    the rename may be rolled back to the OLD state.json. We do not use
    MoveFileExW(MOVEFILE_WRITE_THROUGH); docs/decisions.md, P3.
  * On POSIX the same code fsyncs the directory after the rename and
    after creating files (_fsync_dir). That path is not run by the tests
    on this machine.
  * A drive that acknowledges a flush it has not done (a volatile write
    cache with "write-cache buffer flushing" turned off in Windows, or
    lying firmware) defeats any fsync. Nothing in software fixes that.

Failures
--------
If a write raises (disk full, I/O error), the file and the in-memory copy
may disagree, and a later append would land after unknown bytes. So the
storage fails stop: the error propagates (the node sends no reply), and
every later write raises StorageFailedError. Restarting the process
re-reads the files and recovers as above.

Not thread-safe by itself: every call happens under the node's lock.
"""

import json
import logging
import os
import struct
import zlib
from typing import Optional, Sequence

from raft.messages import EntryKind, LogEntry
from raft.storage import MemoryStorage

log = logging.getLogger("raft.storage")

STATE_FILE = "state.json"
STATE_TMP = STATE_FILE + ".tmp"
LOG_FILE = "log.bin"

HEADER = struct.Struct(">III")  # payload length, crc32(length bytes), crc32(payload)
PAYLOAD_HEAD = struct.Struct(">QB")  # term, kind
_LENGTH = struct.Struct(">I")

# 0 is not a kind on purpose: zero-filled bytes never decode as an entry.
_KIND_CODE = {EntryKind.COMMAND: 1, EntryKind.NOOP: 2}
_KIND_FROM_CODE = {code: kind for kind, code in _KIND_CODE.items()}

_O_BINARY = getattr(os, "O_BINARY", 0)  # Windows: no newline translation


class CorruptStorageError(Exception):
    """The files are damaged in a way a crash cannot explain. The node
    must not start on them."""


class StorageFailedError(Exception):
    """A write was refused: the storage is closed, or an earlier write
    failed and the files can no longer be trusted until a restart."""


# --- encoding ---------------------------------------------------------------


def encode_record(entry: LogEntry) -> bytes:
    payload = PAYLOAD_HEAD.pack(entry.term, _KIND_CODE[entry.kind]) + entry.data
    n = len(payload)
    return HEADER.pack(n, zlib.crc32(_LENGTH.pack(n)), zlib.crc32(payload)) + payload


def scan_log(data: bytes):
    """Decode log.bin. Returns (entries, offsets, good_end, torn) where
    offsets[i] is the byte offset of entries[i], good_end is the end of the
    last complete record, and torn is None or why the bytes after good_end
    are an unfinished last write. Raises CorruptStorageError for damage
    that is not a torn tail (module docstring, "Recovery")."""
    entries, offsets = [], []
    p, size = 0, len(data)
    while p < size:
        rest = size - p
        if rest < HEADER.size:
            return entries, offsets, p, f"{rest} bytes, less than a header"
        if data.count(b"\x00", p) == rest:
            return entries, offsets, p, f"{rest} zero bytes, never written"
        n, length_crc, payload_crc = HEADER.unpack_from(data, p)
        if zlib.crc32(_LENGTH.pack(n)) != length_crc:
            raise CorruptStorageError(f"record at byte {p}: header checksum mismatch")
        end = p + HEADER.size + n
        if end > size:
            return entries, offsets, p, f"payload of {n} bytes, only {rest - HEADER.size} present"
        payload = data[p + HEADER.size : end]
        if zlib.crc32(payload) != payload_crc:
            if end == size:
                return entries, offsets, p, "last record fails its checksum"
            raise CorruptStorageError(f"record at byte {p}: payload checksum mismatch, records follow it")
        if n < PAYLOAD_HEAD.size:
            raise CorruptStorageError(f"record at byte {p}: payload too short ({n} bytes)")
        term, code = PAYLOAD_HEAD.unpack_from(payload)
        if code not in _KIND_FROM_CODE:
            raise CorruptStorageError(f"record at byte {p}: unknown entry kind {code}")
        entries.append(LogEntry(term, payload[PAYLOAD_HEAD.size :], _KIND_FROM_CODE[code]))
        offsets.append(p)
        p = end
    return entries, offsets, p, None


# --- durable file operations -------------------------------------------------


def _write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        view = view[os.write(fd, view) :]


def _fsync_dir(path: str) -> None:
    """Make a rename or a newly created file in `path` durable. POSIX
    only: on Windows a directory cannot be opened to fsync it (module
    docstring), so there is nothing to call."""
    if os.name == "nt":
        return
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


# --- the storage -------------------------------------------------------------


class FileStorage:
    def __init__(self, path: str):
        """Open (or create) the data directory `path` and recover from it.
        A fresh directory is Figure 2's "first boot": term 0, no vote,
        empty log."""
        self.path = path
        self._closed = False
        self._failed: Optional[BaseException] = None
        created = not os.path.isdir(path)
        os.makedirs(path, exist_ok=True)

        term, voted_for, have_state = self._load_state()

        log_path = os.path.join(path, LOG_FILE)
        created = created or not os.path.exists(log_path)
        self._fd = os.open(log_path, os.O_RDWR | os.O_CREAT | _O_BINARY, 0o644)
        try:
            self._recover_log(term, voted_for, have_state)
            if created:
                _fsync_dir(path)
                _fsync_dir(os.path.dirname(os.path.abspath(path)))
        except BaseException:
            os.close(self._fd)
            raise

    def _load_state(self):
        tmp = os.path.join(self.path, STATE_TMP)
        if os.path.exists(tmp):
            # A crash before os.replace: that set_term_and_vote never
            # returned, so nobody was told. state.json is the truth.
            log.warning("%s: removing unfinished %s", self.path, STATE_TMP)
            os.remove(tmp)
        try:
            with open(os.path.join(self.path, STATE_FILE), "rb") as fh:
                raw = fh.read()
        except FileNotFoundError:
            return 0, None, False
        try:
            obj = json.loads(raw.decode("utf-8"))
            term, voted_for = obj["term"], obj["voted_for"]
        except (ValueError, KeyError, TypeError) as e:
            raise CorruptStorageError(f"{STATE_FILE} is unreadable: {e}") from e
        if type(term) is not int or term < 0 or not (voted_for is None or isinstance(voted_for, str)):
            raise CorruptStorageError(f"{STATE_FILE} has bad values: {obj!r}")
        return term, voted_for, True

    def _recover_log(self, term, voted_for, have_state):
        with open(os.path.join(self.path, LOG_FILE), "rb") as fh:
            data = fh.read()
        entries, offsets, good_end, torn = scan_log(data)

        # Checks of what a correct node can have written. All of them run
        # BEFORE the torn tail is cut, so a directory we refuse is left
        # exactly as we found it.
        if entries and not have_state:
            # The term is written before any entry of that term can exist
            # (every entry has a term >= 1), so this is lost state.
            raise CorruptStorageError(f"{LOG_FILE} has {len(entries)} entries but {STATE_FILE} is missing")
        if any(a.term > b.term for a, b in zip(entries, entries[1:])):
            raise CorruptStorageError("log terms decrease along the log")
        try:
            # Also checks "no entry has a term above currentTerm".
            self._mem = MemoryStorage(term, voted_for, entries)
        except ValueError as e:
            raise CorruptStorageError(str(e)) from e

        if torn is not None:
            log.warning(
                "%s: discarding torn tail of %s: %d bytes at offset %d (%s)",
                self.path, LOG_FILE, len(data) - good_end, good_end, torn,
            )
            os.ftruncate(self._fd, good_end)
            os.fsync(self._fd)
        # _offsets[i] = byte offset where entry i starts; [0] is the sentinel.
        self._offsets = [0, *offsets]
        self._end = good_end

    # --- reads: the in-memory copy ------------------------------------------
    # Every write reaches disk first and the copy second, so the copy never
    # holds anything that is not durable.

    @property
    def term(self) -> int:
        return self._mem.term

    @property
    def voted_for(self) -> Optional[str]:
        return self._mem.voted_for

    def last_index(self) -> int:
        return self._mem.last_index()

    def term_at(self, index: int) -> int:
        return self._mem.term_at(index)

    def entry(self, index: int) -> LogEntry:
        return self._mem.entry(index)

    def entries_from(self, index: int, limit: int) -> tuple:
        return self._mem.entries_from(index, limit)

    # --- writes: durable before they return ----------------------------------

    def set_term_and_vote(self, term: int, voted_for: Optional[str]) -> None:
        self._check_writable()
        body = json.dumps({"term": term, "voted_for": voted_for}).encode("utf-8")
        tmp = os.path.join(self.path, STATE_TMP)
        try:
            with open(tmp, "wb") as fh:
                fh.write(body)
                fh.flush()  # Python's buffer -> the OS
                os.fsync(fh.fileno())  # the OS -> the device
            os.replace(tmp, os.path.join(self.path, STATE_FILE))
            _fsync_dir(self.path)
        except BaseException as e:
            self._fail(e)
            raise
        self._mem.set_term_and_vote(term, voted_for)

    def append(self, entries: Sequence[LogEntry]) -> None:
        self._check_writable()
        if not entries:
            return
        records = [encode_record(e) for e in entries]
        try:
            os.lseek(self._fd, self._end, os.SEEK_SET)
            _write_all(self._fd, b"".join(records))  # one write for the batch
            os.fsync(self._fd)
        except BaseException as e:
            self._fail(e)
            raise
        for record in records:
            self._offsets.append(self._end)
            self._end += len(record)
        self._mem.append(entries)

    def truncate_from(self, index: int) -> None:
        """Delete the entry at `index` and every entry after it."""
        if index < 1:
            raise IndexError("the sentinel at index 0 is never deleted")
        self._check_writable()
        if index > self._mem.last_index():
            return  # nothing to delete (MemoryStorage: a no-op too)
        cut = self._offsets[index]
        try:
            os.ftruncate(self._fd, cut)
            os.fsync(self._fd)  # the new size is metadata; fsync covers it
        except BaseException as e:
            self._fail(e)
            raise
        del self._offsets[index:]
        self._end = cut
        self._mem.truncate_from(index)

    # --- lifecycle ------------------------------------------------------------

    def close(self) -> None:
        """Release the file handle. Flushes nothing, because nothing is
        ever left unflushed: a crash without close() loses nothing that a
        write method returned for. Idempotent."""
        if not self._closed:
            self._closed = True
            os.close(self._fd)

    def _check_writable(self):
        if self._closed:
            raise StorageFailedError(f"{self.path}: storage is closed")
        if self._failed is not None:
            raise StorageFailedError(f"{self.path}: an earlier write failed ({self._failed!r})")

    def _fail(self, e: BaseException):
        log.critical("%s: write failed, refusing all further writes: %r", self.path, e)
        self._failed = e
