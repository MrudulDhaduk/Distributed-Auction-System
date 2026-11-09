"""Figure 2's persistent state -- currentTerm, votedFor, log[] -- behind one
small interface.

Figure 2 says this state is "Updated on stable storage before responding to
RPCs". The point of this module is the SEAM: the node changes these three
things only through the three write methods of `Storage`, so making them
durable is the storage's job, not the algorithm's. Two implementations:
  MemoryStorage (here)           in memory only: unit tests, and the default
  FileStorage (file_storage.py)  each write is fsync'd before it returns
                                 (build order step 6)
node.py did not change when FileStorage arrived.

The node reads through this object too (it holds no copy of its own), so a
write that bypassed the seam is impossible rather than merely discouraged.

Indexes are 1-based, as in the paper ("first index is 1"). Slot 0 holds a
sentinel entry with term 0. It stands for "the empty prefix every log
shares", so the AppendEntries consistency check at prevLogIndex = 0 always
succeeds and needs no special case.

Not thread-safe by itself: every call happens under the node's lock.
"""

from typing import Iterable, Optional, Protocol, Sequence

from raft.messages import LogEntry

_SENTINEL = LogEntry(0, b"")


class Storage(Protocol):
    """What the node needs. Reads: `term`, `voted_for`, `last_index()`,
    `term_at(i)`, `entry(i)`, `entries_from(i, limit)`. Writes -- the only
    way to change currentTerm, votedFor and log[]:"""

    term: int
    voted_for: Optional[str]

    def last_index(self) -> int: ...
    def term_at(self, index: int) -> int: ...
    def entry(self, index: int) -> LogEntry: ...
    def entries_from(self, index: int, limit: int) -> tuple: ...
    def set_term_and_vote(self, term: int, voted_for: Optional[str]) -> None: ...
    def append(self, entries: Sequence[LogEntry]) -> None: ...
    def truncate_from(self, index: int) -> None: ...


class MemoryStorage:
    def __init__(self, term: int = 0, voted_for: Optional[str] = None, entries: Iterable[LogEntry] = ()):
        """Starting state. A fresh node uses the defaults (term 0, no vote,
        empty log). Tests pass a non-empty log to build the paper's Figure 7
        and Figure 8 situations directly."""
        self._term = term
        self._voted_for = voted_for
        self._log = [_SENTINEL, *entries]
        if any(e.term > term for e in self._log):
            raise ValueError("a log entry cannot have a term above currentTerm")

    # --- reads -----------------------------------------------------------

    @property
    def term(self) -> int:
        return self._term

    @property
    def voted_for(self) -> Optional[str]:
        return self._voted_for

    def last_index(self) -> int:
        return len(self._log) - 1

    def term_at(self, index: int) -> int:
        """Term of the entry at `index`; 0 for the sentinel at index 0."""
        return self._log[index].term

    def entry(self, index: int) -> LogEntry:
        if index < 1:
            raise IndexError("log indexes start at 1")
        return self._log[index]

    def entries_from(self, index: int, limit: int) -> tuple:
        """Up to `limit` entries starting at `index` (1-based)."""
        if index < 1:
            raise IndexError("log indexes start at 1")
        return tuple(self._log[index : index + limit])

    # --- writes: the persistence seam ------------------------------------
    # FileStorage makes each of these durable before it returns.

    def set_term_and_vote(self, term: int, voted_for: Optional[str]) -> None:
        """Written together because votedFor only means something together
        with the term it was cast in."""
        self._term = term
        self._voted_for = voted_for

    def append(self, entries: Sequence[LogEntry]) -> None:
        self._log.extend(entries)

    def truncate_from(self, index: int) -> None:
        """Delete the entry at `index` and every entry after it."""
        if index < 1:
            raise IndexError("the sentinel at index 0 is never deleted")
        del self._log[index:]
