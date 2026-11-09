# Raft persistence: a walkthrough to study from

Scope: build-order step 6. Figure 2's persistent state (currentTerm, votedFor,
log[]) now lives on disk, and each write is durable before the node replies to
anything. A crashed node restarts from its data directory. Spec: Raft paper
(Ongaro & Ousterhout), Figure 2 ("State"), §5.5 (follower and candidate
crashes), §5.6 (persistence costs time), §7 (snapshots, which are out of scope),
and the Figure 3 properties persistence protects. Quotes are from
`papers/raft-extended.pdf`.

New code: [raft/file_storage.py](../raft/file_storage.py) (`FileStorage`), and
`--data-dir` in [raft/run_node.py](../raft/run_node.py). **`raft/node.py`'s
algorithm did not change.** Only its docstring and one type hint changed
(section 2). Line numbers refer to the files as of this chunk. `node.py` grew by
one docstring line, so its numbers are one higher than in
[raft-replication.md](raft-replication.md). Each non-obvious choice has an entry
P1-P15 in [decisions.md](decisions.md). Section 7 lists what this chunk knowingly
doesn't handle.

---

## 1. The invariants first

Figure 2, State:

> **Persistent state on all servers:**
> (Updated on stable storage before responding to RPCs)
> **currentTerm** latest term server has seen (initialized to 0 on first boot,
> increases monotonically)
> **votedFor** candidateId that received vote in current term (or null if none)
> **log[]** log entries; each entry contains command for state machine, and term
> when entry was received by leader (first index is 1)

Note "initialized to 0 **on first boot**". It's set to 0 once, when the
directory is new, not on every boot.

Why these three, and why "before responding"? Each one is a **promise the node
has made to someone else**. If a crash can take the promise back, a Figure 3
property breaks.

| State | The promise | What breaks if a crash forgets it |
| --- | --- | --- |
| `votedFor` | "I voted for A in term T." A counted that vote. | The node votes again for B in term T. A and B can both reach a majority, so there are **two leaders in one term**: Election Safety. |
| `currentTerm` | "I've seen term T." Older leaders were told no. | The node goes back to an older term, takes an old leader's AppendEntries, and can vote twice in a term it has forgotten. That's the same break as above. |
| `log[]` | "I hold entry (i, t)." The leader counted this node toward a majority and maybe **committed** the entry. | The entry was on a majority and now it isn't. A node without it can win (§5.4.1 can't stop it), and a **committed entry is lost**. That breaks Leader Completeness and then State Machine Safety. |

So the rule is: **no reply leaves the node until every promise inside it is on
disk.** That covers a reply, a vote, and also a leader counting itself. Section 4
shows that this code keeps that rule.

What is *not* persistent, on purpose (Figure 2, "Volatile state"):
`commitIndex`, `lastApplied`, `nextIndex[]`, `matchIndex[]`, and the state
machine itself. A restarted node has applied nothing. It rebuilds by
**replaying** the log once a leader tells it the commitIndex. Same log, same
state (section 3, "Restart").

---

## 2. The pieces

| Piece | Where | What it does |
| --- | --- | --- |
| `Storage` protocol | `raft/storage.py` | The interface: 6 reads, 3 writes. Both storages implement it. |
| `MemoryStorage` | `raft/storage.py` | Unchanged. The default, and what the unit tests use. |
| `FileStorage` | `raft/file_storage.py` | Same interface. Every write is fsync'd before it returns. |
| `scan_log` | `file_storage.py:128-161` | Decodes `log.bin` and tells a torn tail apart from corruption. |
| `run_node --data-dir` | `run_node.py:70-72, 106-118` | Picks the storage, logs the recovered state, and refuses to start on corruption. |

The change to `node.py` is two lines: `storage: Optional[Storage]` (it was
`Optional[MemoryStorage]`), and the module docstring no longer says "in memory
only until step 6". This works because of the seam built in step 4 (R5): the
node holds **no copy** of term, vote or log. It reads them through storage and
changes them only through `set_term_and_vote`, `append` and `truncate_from`.
Swapping the storage object is the whole integration.

How FileStorage is built: it **contains a MemoryStorage** (`self._mem`) as its
in-memory copy. Every write goes to disk and is fsync'd first, then the same
call is made on `_mem`. Every read is answered by `_mem`. So reads behave exactly
like MemoryStorage, because they *are* MemoryStorage. And the copy never holds
anything that isn't already durable (P1).

### Files, one directory per node

```
data/n2/
  state.json   {"term": 1, "voted_for": "n1"}
  log.bin      record(noop@1) record(x@1) ...
```

**`state.json`**: currentTerm and votedFor, always written together, as a new
file each time (P2, P5). Here is the write, `set_term_and_vote`,
`file_storage.py:292-306`:

1. Write `state.json.tmp` and `flush()` it. That moves the bytes from Python's
   buffer to the OS.
2. `os.fsync()` it. That moves them from the OS to the device (line 300).
3. `os.replace(tmp, state.json)` (line 301). On one volume this is a rename. A
   reader sees the old file or the new one, never half of each.
4. Only now update the in-memory copy (line 306) and return.

**`log.bin`**: append-only, one record per entry, big-endian (P4):

```
header   length u32          bytes in the payload
         crc32(length) u32   the length has its own checksum
         crc32(payload) u32
payload  term u64, kind u8 (1 = COMMAND, 2 = NOOP), data (length - 9 bytes)
```

Real bytes for `LogEntry(term=1, data=b"x")`, 22 bytes in all:

```
00 00 00 0a  c1 91 36 02  a5 8d ca 0e  00 00 00 00 00 00 00 01  01  78
length=10    crc(length)  crc(payload) term=1                   CMD 'x'
```

A no-op is 21 bytes (no data). That's why every `log.bin` in the gRPC trace
below is 42 bytes: two no-ops.

Why does the length get its own checksum? Suppose a bit flips in the length of
record 1 of 3. Without that checksum, the reader would jump to a wrong "end of
record". If the bad length pointed past the end of the file, record 1 would look
like a torn tail, and records 1-3 would be **silently dropped**. With the
checksum, it's caught as corruption (P4, tested: `header length` case).

---

## 3. Figure 2, rule by rule

### "Updated on stable storage": the three write methods

| Write method | Lines | Disk operation | Then |
| --- | --- | --- | --- |
| `set_term_and_vote(t, v)` | 292-306 | tmp file, fsync, `os.replace` | `_mem.set_term_and_vote` |
| `append(entries)` | 308-323 | **one** `os.write` of all the records at the end of the file, then fsync | offsets, `_mem.append` |
| `truncate_from(i)` | 325-341 | `os.ftruncate` at entry i's byte offset, then fsync | offsets, `_mem.truncate_from` |

`truncate_from` needs the byte offset where entry `i` starts. FileStorage keeps
`_offsets[i]` in memory (line 263). It's built while scanning on open, and
extended on every append. So "delete entries i and after" becomes "cut the file
at `_offsets[i]`". The file's size is metadata, and on Windows `fsync`
(`FlushFileBuffers`) writes data **and** metadata, so the cut is durable
(tested: `test_truncate_from_survives_reopen`).

An `append` of a 64-entry batch from the leader is still one write and one
fsync: one disk wait per AppendEntries, not one per entry.

### Where the node calls them: the seven write sites

Every one runs **under the node lock**, and in every one **the reply (or the
outgoing RPC) is built after the write returns**:

| Site | `node.py` line | Write | What leaves the node only after it |
| --- | --- | --- | --- |
| any term increase (`_transition`) | 376 | `set_term_and_vote(T, vote)` | every reply carries `term` = T |
| candidate votes for itself (`_start_election`) | 376 via 411 | `set_term_and_vote(T+1, self)` | the RequestVote RPCs (sent at 355, after the lock is released) |
| grant a vote | 555 | `set_term_and_vote(T, candidate)` | `RequestVoteReply(granted=True)`, 565 |
| leader appends a client entry | 315 | `append([e])` | the leader counting itself (320) and the AppendEntries (322) |
| new leader's no-op | 453 | `append([noop])` | the leader counting itself (458) |
| follower: conflicting suffix | 668 | `truncate_from(i)` | the AppendEntries reply, 636 |
| follower: new entries | 670 | `append(entries)` | `AppendEntriesReply(success, matchIndex)`, 636 |

### Restart: commitIndex and lastApplied start at 0, and the log is replayed

> **commitIndex** ... (initialized to 0, increases monotonically)
> **lastApplied** ... (initialized to 0, increases monotonically)

These are in the *volatile* box, so a restarted node starts them at 0
(`node.py:204-205`, unchanged). This is the step-2 gap ("R2: save them together,
or replay"). The answer is **replay**:

1. A new `RaftNode` is built on the recovered storage. It has the whole log,
   `commitIndex = 0`, `lastApplied = 0`, and an empty state machine (a new
   process has a fresh one).
2. A leader's AppendEntries passes the consistency check, and receiver step 5
   sets `commitIndex = min(leaderCommit, index of last new entry)`.
3. The applier (unchanged) applies from index 1 up to commitIndex, in order,
   once each. Entries that were applied before the crash are applied again, but
   into an *empty* state machine, so the result is the same state.

The key point: **`lastApplied` and the state machine must be kept or lost
together.** We lose both. Losing only `lastApplied` would double-apply every
entry. Losing only the state machine would skip entries. (P11.)

Test: `test_a_restarted_follower_replays_committed_entries_in_order` checks that
right after reboot the node has its log back but `(commitIndex, lastApplied) ==
(0, 0)` and nothing applied. Then it re-applies `a, b, c` at **the same indexes
as before the crash**.

If the whole cluster restarts, nobody knows the commitIndex. The first leader's
no-op (§8, R9) commits, and that commits everything before it (Figure 8 rule).
Then all three replay
(`test_every_committed_entry_survives_restarting_the_whole_cluster`).

### Opening a directory: torn tail versus corruption (`scan_log`, 128-161)

A crash in the middle of `append` can leave a partial record at the end of
`log.bin`. The scan walks the file record by record. At each position `p`:

| What is at `p` | Line | Verdict | Why |
| --- | --- | --- | --- |
| fewer than 12 bytes | 139 | torn | the header itself is unfinished |
| only zero bytes up to EOF | 141 | torn | NTFS after a power cut: the size grew but the data never landed. We never write an all-zero header (its checksum isn't zero). |
| header checksum wrong | 144 | **corrupt** | an unfinished write leaves *missing* bytes, not *wrong* ones (P7) |
| payload runs past EOF | 147 | torn | the payload is unfinished |
| payload checksum wrong, record ends at EOF | 151 | torn | the last write, damaged |
| payload checksum wrong, more records after it | 152 | **corrupt** | not the last write, so a crash can't explain it |
| payload too short / unknown kind | 154, 157 | **corrupt** | the checksum passed, so this is a format bug |

**Torn: cut it off and carry on** (`_recover_log`, lines 255-261). The scan stops
at the last good record, and the file is **truncated there and fsync'd**. That
truncation matters: without it, the next append would land *after* the garbage,
and the next open would see garbage in the *middle* (a hard error). The test
checks reopen, append, reopen again.

**Why discarding a torn record is safe.** Its `append` never returned, so the
line after it (a reply, or the leader counting itself) never ran. Nobody was told
"I hold this entry". Dropping it makes this node look like a follower that never
received that AppendEntries. The leader retries anyway (§5.5: "Raft handles these
failures by retrying indefinitely"). For a multi-entry append, the complete
records before the torn one are **kept**. That's also safe: it's as if the
follower got a shorter AppendEntries, and the records are a prefix of the
leader's log.

**Corrupt: refuse to start.** `CorruptStorageError`; `run_node` exits with the
message (line 108). Skipping a bad middle record would silently drop an entry
this node *did* acknowledge, and that is exactly how a committed entry gets lost.
A node that won't start is an availability problem. A node that starts on a log
with a hole in it is a safety problem.

Other open-time checks (lines 243-253), all made **before** anything is
truncated, so a directory we refuse is left as we found it:

- log entries but no `state.json`: corrupt. The term is always written before any
  entry of that term exists, so this means lost state.
- terms decrease along the log: corrupt. A correct Raft log never does that
  (`_advance_commit` relies on it).
- an entry's term above currentTerm: corrupt (MemoryStorage's own check, reused).
- unreadable or wrongly typed `state.json`: corrupt (line 230, 232).
- a leftover `state.json.tmp`: **deleted, not an error** (line 217). The crash
  happened before `os.replace`, so that `set_term_and_vote` never returned.

---

## 4. "Before responding to RPCs": the argument

**Claim.** No node ever sends a message that depends on a write that isn't yet
durable.

**Argument, in three steps.**

1. *Each write method is durable when it returns.* `os.fsync` is called after the
   write and before the return, in all three. The in-memory copy changes only
   after that. Tested by patching `os.fsync`:
   `test_every_write_method_fsyncs_before_it_returns` records the file's size at
   each fsync, and checks that the append's fsync saw both new records and the
   truncate's fsync saw the shortened file. So fsync came *after* the write, on
   the written bytes. `test_term_and_vote_are_fsynced_before_the_rename` checks
   that while the temp file was being fsynced, `state.json` still held the old
   term.
2. *The node writes before it builds anything that leaves.* Every row of the
   write-site table above. A handler holds the lock from its first line to its
   `return`, so nothing else can slip in between.
3. *What leaves is built from the state after the write.* Replies are
   constructed at the end of the handler (565, 585/618/636). Outgoing RPCs are
   prepared under the lock after the write and sent after the lock is released
   (322, 355).

If a write *fails*, the exception propagates out of the handler. The node sends
no reply, and the storage refuses all further writes (P10).
`test_no_vote_is_granted_unless_it_was_made_durable` patches `fsync` to fail and
checks that `handle_request_vote` raises instead of returning "granted".

**Why the leader's own append must be durable before it counts itself.** The
commit rule counts the leader as one of the copies: `copies = 1 + ...`
(`_advance_commit`). "Committed" means "on a majority, **durably**". Suppose the
leader counted an entry that was only in its memory:

```
3 nodes. Leader L appends x (memory only), sends to A and B.
A stores x durably and acks. L counts {L, A} = 2 of 3 and commits x.
The client is told APPLIED.
Power fails on L. x was never on L's disk. B never received x.
L and B both restart without x and together form a majority.
B campaigns; L votes for it (B's log is as up-to-date as L's).
B is leader without x. A can't stop it alone.
x, acknowledged to a client, is gone.
```

Only one durable copy existed, A's, and one of three isn't a majority. In our
code `propose()` calls `self._storage.append(...)` (315) and returns from it
durable *before* `_advance_commit()` (320) can count the leader. The same holds
for the no-op (453, then 458). The leader also sends AppendEntries only for
entries it already has durably, but safety doesn't need that. A faster design
writes the leader's disk *in parallel* with sending, and only counts the leader
once its own write has finished. Ongaro's PhD thesis describes this (§10.2.1, as
I remember it; it's not in `papers/raft-extended.pdf`, so check before quoting
it). We do the simple, sequential version (P9).

**Why the candidate's self-vote must be durable before it sends RequestVote.**
`_start_election` writes `(T+1, self)` at 376 under the lock, and the RPCs leave
at 355 after the lock is released. If it didn't: candidate A wins term T
(counting itself), commits nothing yet, crashes, and restarts having forgotten
its self-vote. It then votes for B in term T, and B also wins term T. Two leaders
of term T could each put a *different* entry at the same index with the same
term. That breaks the Log Matching Property itself.

---

## 5. The end-to-end traces

### 5a. The double vote (`test_a_restarted_node_does_not_vote_twice_in_one_term`)

n3 is fresh (term 0, votedFor null). Cluster n1, n2, n3.

| Step | Event | n3 does | Disk after |
| --- | --- | --- | --- |
| 1 | RequestVote(from n1, term 5, last 0/0) | step-down: `set_term_and_vote(5, None)`, fsync #1 | `{"term": 5, "voted_for": null}` |
| 2 | | checks pass; `set_term_and_vote(5, "n1")`, fsync #2 | `{"term": 5, "voted_for": "n1"}` |
| 3 | | replies granted=True (only now) | |
| 4 | **crash**: the RaftNode object is thrown away, nothing closed | | unchanged |
| 5 | restart: a new RaftNode on `data/n3` | recovers term 5, votedFor n1 | |
| 6 | RequestVote(from n2, term 5) | no step-down (5 = 5). votedFor = n1, not in (null, n2): **refuse** | unchanged |

With MemoryStorage, step 5 starts at term 0 with no vote, and step 6 **grants**
n2. n1 and n2 now have n3's vote in the same term, and with one more vote each
they are both leaders of term 5. The test runs both ways. The memory run is
`xfail(strict=True)` and fails with `AssertionError: voted for n1 AND n2 in term
5`. (Two fsyncs in steps 1-2 rather than one: P15.)

### 5b. Chunk 2's known gap 1, now closed (`test_an_acknowledged_write_survives_a_restart_then_the_leaders_crash`)

The scenario from [raft-replication.md](raft-replication.md) §7, gap 1.

| # | Event | n1 | n2 | n3 |
| --- | --- | --- | --- | --- |
| 0 | n3 down; n1 wins term 1 with n2's vote | leader, t1 | `{"term":1,"voted_for":"n1"}` | down, t0, empty |
| 1 | no-op at 1, then `propose(x)` at 2; n2 acks both; commitIndex 2; client gets **APPLIED** | log noop@1 x@1 | log.bin 43 bytes: noop@1 (21) + x@1 (22) | |
| 2 | n2 crashes, n1 crashes | dead | dead | |
| 3 | n2 reboots from `data/n2`; n3 comes back | | t1, voted n1, log [noop@1, x@1], commit 0 | t0, empty |
| 4 | n2 times out: term 2, votes for itself (durable), RequestVote(t2, last 2/1) | | candidate t2 | t0 → t2; its log (0/0) is behind (2/1), so it **grants** |
| 5 | n2 leader t2; no-op@2 at index 3; nextIndex[n3] = 3 | | leader | |
| 6 | prev 2/1: n3 refuses; prev 1/1: refuses; prev 0/0: accepts 1..3 | | matchIndex[n3] = 3 | log [noop@1, x@1, noop@2] |
| 7 | index 3 is term 2 on 2 of 3: commitIndex 3. Figure 8: commits index 2 (x) indirectly | | applies **x** (replay from 1) | applies **x** |

With MemoryStorage, at step 3 n2 is empty at term 0. It wins term **1**
(n3 grants; n1 also led term 1, which is Election Safety broken across the
crash), and `x` is applied nowhere: the xfail run ends with
`{'n2': [], 'n3': []}`.

### 5c. Real processes, real gRPC, hard kills

Three `run_node` processes with `--data-dir`. A kill is `Stop-Process -Force`
(Win32 `TerminateProcess`): no `finally`, no `close()`, no flush. The `listening
on` lines are omitted, and paths are shortened to `data/nX`. Recorded on this
machine:

```
--- phase 1: fresh cluster
[n1] storage data/n1: term 0, votedFor None, 0 log entries (last term 0)
[n1] term 1 FOLLOWER -> CANDIDATE (was term 0): election timeout
[n2] term 1: GRANTED vote for n1 (ok)
[n3] term 1: GRANTED vote for n1 (ok)
[n1] term 1 CANDIDATE -> LEADER: won election with 2 votes
[n1] term 1: appended no-op at index 1
[n1] term 1: commitIndex 0 -> 1

--- phase 2: KILL n1 (the leader)
[n2] term 2 FOLLOWER -> CANDIDATE (was term 1): election timeout
[n3] term 2: GRANTED vote for n2 (ok)
[n2] term 2 CANDIDATE -> LEADER: won election with 2 votes
[n2] term 2: appended no-op at index 2
[n2] term 2: commitIndex 1 -> 2
[n3] appended 2..2 from leader

--- phase 3: restart n1 from data/n1
[n1] storage data/n1: term 1, votedFor n1, 1 log entries (last term 1)
[n1] term 2 FOLLOWER -> FOLLOWER (was term 1): higher term 2 seen in AppendEntries from n2
[n1] appended 2..2 from leader
[n1] term 2: commitIndex 0 -> 2

--- phase 4: KILL all three. On disk:
  n1 state.json: {"term": 2, "voted_for": null}   log.bin: 42 bytes
  n2 state.json: {"term": 2, "voted_for": "n2"}   log.bin: 42 bytes
  n3 state.json: {"term": 2, "voted_for": "n2"}   log.bin: 42 bytes
restart all three:
[n1] storage data/n1: term 2, votedFor None, 2 log entries (last term 2)
[n2] storage data/n2: term 2, votedFor n2, 2 log entries (last term 2)
[n3] storage data/n3: term 2, votedFor n2, 2 log entries (last term 2)
[n3] term 3 FOLLOWER -> CANDIDATE (was term 2): election timeout
[n1] term 3: GRANTED vote for n3 (ok)
[n2] term 3: GRANTED vote for n3 (ok)
[n3] term 3 CANDIDATE -> LEADER: won election with 2 votes
[n3] term 3: appended no-op at index 3
[n3] term 3: commitIndex 0 -> 3
[n1] term 3: commitIndex 0 -> 3
[n2] term 3: commitIndex 0 -> 3
```

What to point at:

- **Phase 3, line 1.** The restarted n1 remembers that it was in term 1 and voted
  for itself, and it has its no-op. It also remembers **nothing volatile**: it
  doesn't think it's leader, and its commitIndex is 0 until n2 says otherwise
  (`commitIndex 0 -> 2`).
- **Phase 4, `state.json`.** n1's vote is `null` because n1 moved to term 2 by
  hearing a higher term, not by voting. n2 and n3 both voted for n2 in term 2.
  If any of them had been asked again in term 2 after the restart, they'd have
  refused.
- **Phase 4, after restart.** The new term is **3**, not 1. Terms never went
  backwards across a full-cluster crash ("increases monotonically"). Every node
  starts at `commitIndex 0` and jumps to 3 with the new no-op. That's the
  replay: indexes 1-3 are applied again. They're all no-ops here, so apply_fn
  isn't called.

`run_node` has no client RPC yet, so this trace has only no-ops. Client entries
surviving restarts are covered by 5b and the two cluster-restart tests.

---

## 6. How to see it yourself

```powershell
.venv\Scripts\python.exe -m pytest tests\test_raft_file_storage.py tests\test_raft_persistence.py -v -rxX
```

`-rxX` lists the two `[memory]` runs as XFAIL with their reason. To see them
**fail** with the real assertion:

```powershell
.venv\Scripts\python.exe -m pytest tests\test_raft_persistence.py --runxfail -k memory
```

Three real nodes, in three windows (ports 50061-50063 are outside this
machine's reserved ranges; `netsh interface ipv4 show excludedportrange
protocol=tcp` lists them. 50155-50254 is reserved here):

```powershell
.venv\Scripts\python.exe -m raft.run_node --id n1 --port 50061 --peers n2=127.0.0.1:50062,n3=127.0.0.1:50063 --data-dir data\n1
.venv\Scripts\python.exe -m raft.run_node --id n2 --port 50062 --peers n1=127.0.0.1:50061,n3=127.0.0.1:50063 --data-dir data\n2
.venv\Scripts\python.exe -m raft.run_node --id n3 --port 50063 --peers n1=127.0.0.1:50061,n2=127.0.0.1:50062 --data-dir data\n3
```

Kill one with `Stop-Process -Id <pid> -Force` (or close its window), start it
again with the same line, and read its first `storage ...` line. Look at
`data\n1\state.json` in an editor. `data/` is in `.gitignore`.
`Remove-Item -Recurse data` gives you first boot again. Never do that to *one*
node of a live cluster (gap 6). `scripts/run_cluster.ps1` doesn't pass
`--data-dir` yet. It wasn't in this chunk's scope.

**Timing and flakiness.** The storage tests use no threads and no clock, so
they're deterministic. The cluster tests use the replication chunk's policy: one
node at a time has a short election timeout and the rest have 30 s, so who
campaigns is decided by the test, and every wait is `wait_until(condition, 5 s)`.
fsync on this machine takes about 0.25 ms (append) and 1.2 ms (temp + fsync +
replace), measured, against 30 ms heartbeats in the tests. Raft tests run 5
times in a row: 117 passed, 2 xfailed, every time. A much slower disk (a USB
stick, a network drive) holds the node lock longer per write. That makes
heartbeats later but not wrong. With the tests' 150 ms election timeouts, a
disk taking tens of ms per fsync *could* cause extra elections, which the
cluster tests tolerate. A >5 s stall would fail a wait.

---

## 7. Known gaps

1. **Power loss can roll back a vote on Windows (P3).** `os.replace` is durable
   against a *process* crash. Against power loss or an OS crash, NTFS keeps the
   directory *consistent* but can lose the *latest* rename, because a directory
   can't be fsync'd from Python on Windows (`os.open` on a directory raises
   `PermissionError`; measured). Then `state.json` comes back one write old, and
   a node could vote twice in one term. Fix: `MoveFileExW(...,
   MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)` via ctypes, or a
   two-slot state file overwritten in place (no rename at all). On POSIX the code
   fsyncs the directory (`_fsync_dir`). That branch is not run by the tests on
   this machine.
2. **No snapshots, no log compaction (§7).** The log grows forever, is held
   entirely in memory, and is replayed from index 1 on every restart: §7's
   "takes more time to replay". The size isn't bounded either. Out of scope for
   this course.
3. **Two processes on one data directory corrupt it.** There is no lock file.
   Two `run_node`s with the same `--data-dir` would interleave appends. That
   needs an exclusive lock on open (`msvcrt.locking` / `fcntl.flock`).
4. **A damaged last record is assumed to be a torn write.** It might instead be
   bit rot in a record that was fsync'd and acknowledged. Dropping it then could
   lose a committed entry. Raft's fault model assumes stable storage really is
   stable, so this sits outside it. A checksum can detect the damage but can't
   tell which of the two it was.
5. **Power loss that persists sectors out of order** could leave garbage
   followed by valid records in the last append. We'd call that corruption and
   refuse to start (safe, but someone has to step in). It can't happen with a
   process crash.
6. **A node whose disk is wiped must not rejoin under its old id.** It forgot its
   votes and its log, which is the MemoryStorage bug again. The paper's answer
   is a membership change (§6): remove the old server and add a new one. There is
   none here, so for now it's an operator rule.
7. **Every write is its own fsync, under the node lock.** No group commit. It's
   fine at this scale (about 0.25 ms). On a slow disk, throughput is one fsync
   per AppendEntries per follower.
8. **A failed write makes the node a zombie until it's restarted (P10).** It
   keeps answering reads, refuses every write, and can't take part. Restarting
   recovers it.
9. **Another program holding `state.json` open** (some antivirus or indexer
   tools do this briefly) makes `os.replace` raise `PermissionError` on Windows.
   That is a failed write, so gap 8 applies. It's an availability problem, not a
   safety one.
10. **Not wired to the auction yet.** Replay calls `apply_fn` from index 1 on
    restart, so the auction state machine must be built empty at boot and must
    be deterministic (it is: CLAUDE.md constraints). `SessionStore` is still not
    replicated, so tokens don't survive a restart (CLAUDE.md seams).
11. **The trace over gRPC has only no-ops** (no client RPC yet; replication gap 7).

---

## 8. Things I am not fully sure about

- **What NTFS does with an un-flushed rename after power loss.** I've written the
  conservative reading: old file or new, and possibly the old one. I haven't
  tested power loss. It can't be done from a test. It would need a VM with its
  disk cut.
- **Whether this machine's fsync reaches stable media.** 0.25 ms per
  `FlushFileBuffers` is fast. Either the drive has power-loss protection, or it
  acknowledges flushes from a volatile cache. Software can't tell which.
- **`MOVEFILE_WRITE_THROUGH`'s documented promise** is worded around moves done
  as copy+delete (across volumes). Whether it also forces a same-volume rename
  to disk is commonly assumed but not clearly stated. That's another reason I
  didn't silently rely on it (P3).
- **The zero-tail rule** (`file_storage.py:141`) covers how NTFS shows unwritten
  extended space (as zeros). Another filesystem could leave other garbage there,
  and we'd call it corruption.
