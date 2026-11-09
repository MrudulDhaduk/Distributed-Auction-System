# Raft log replication and repair: a walkthrough to study from

Scope: build-order steps 4 and 5 together. The log, `commitIndex`,
`lastApplied`, the leader's `nextIndex[]`/`matchIndex[]`, the full
AppendEntries RPC (both sides), the commit rule, the leader's no-op at the start
of its term (§8), the applier, and `propose()`. Still no persistence (step 6),
but every write of persistent state now goes through one seam
(`raft/storage.py`). Spec: Raft paper (Ongaro & Ousterhout), Figure 2, §5.3
(replication), §5.4 (safety: election restriction, Figure 8), §5.5 (idempotent
RPCs), §8 (the no-op), and Figure 3's Log Matching, Leader Completeness and
State Machine Safety. Quotes are from `papers/raft-extended.pdf`.

Line numbers refer to `raft/node.py` as of this chunk. (The line numbers in
[raft-election.md](raft-election.md) are from the previous chunk and have
drifted; its function names are still right.) Each non-obvious choice has an
entry R1-R17 in [decisions.md](decisions.md). Section 7 lists what this chunk
knowingly gets wrong.

---

## 1. The invariants first

Election Safety (one leader per term) was the previous chunk. This chunk adds
three more properties from Figure 3, and each one leans on the one before it.

**Log Matching.** *If two logs contain an entry with the same index and term,
then the logs are identical in all entries up through that index.* There are
two reasons:

1. A leader creates at most one entry per index in its term, and never moves
   or deletes it. Only `propose()` (line 314) and `_become_leader` (the no-op,
   line 452) append on a leader, and both only append.
2. A follower accepts new entries only if it already holds the entry just
   before them, with the same term (receiver step 2, lines 609-619).

So it works by induction. The empty logs match. Every successful
AppendEntries extends a matching prefix by entries the leader holds right
after that prefix.

**Leader Completeness.** *A committed entry is present in the logs of all
later leaders.* An entry is committed when a majority stores it, and a leader
is elected by a majority of votes. Any two majorities share at least one node.
That shared node has the entry, and it only votes for a candidate whose log is
at least as up-to-date as its own (`_candidate_log_ok`, §5.4.1). There is one
catch: this argument is only true if "committed" is decided by counting
replicas of entries **from the leader's current term**. Figure 8 shows how it
breaks otherwise. See `_advance_commit` below.

**State Machine Safety.** *If a server has applied an entry at an index, no
other server ever applies a different entry at that index.* Only committed
entries are applied (`_run_applier`), in index order. A committed entry never
changes, because of Leader Completeness plus Log Matching. The receiver even
asserts it never deletes a committed entry (line 658).

In one sentence: **the log may contain attempts that later get overwritten,
but anything below `commitIndex` is final, and only the COMMAND entries of
that part reach `apply_fn`.**

---

## 2. The pieces

| File | Role in this chunk |
| --- | --- |
| `raft/node.py` | The algorithm: election (unchanged), replication, repair, commit, no-op, applier, `propose()` |
| `raft/storage.py` | **New.** `MemoryStorage`: currentTerm, votedFor, log[]. The node's only way to change them (R5) |
| `raft/proposal.py` | **New.** `Proposal` handle + `Outcome` enum: what `propose()` promises (R4) |
| `raft/messages.py` | **New `LogEntry(term, data: bytes, kind)`** and `EntryKind` (COMMAND / NOOP); `AppendEntriesArgs.entries` carries them |
| `raft/wire.py` | `LogEntry` <-> protobuf `LogEntry` (`data` <-> `command`, kind <-> `EntryKind` enum) |
| `proto/raft.proto` | New `EntryKind` enum and `LogEntry.kind` field (stubs regenerated) |

Threads inside one node, now four kinds:

- **ticker** (unchanged): election timeouts, and the leader's heartbeat
  tick, which now also carries entries and retries after a lost RPC.
- **RPC workers**: send RequestVote / AppendEntries. A reply that shows the
  peer is still behind sends the next AppendEntries at once.
- **handler threads**: gRPC server threads (or test callers) running
  `handle_request_vote` / `handle_append_entries`.
- **applier** (new): one thread that calls `apply_fn(index, data)` for each
  committed COMMAND entry, **with the node lock released** (R2), and steps
  over no-ops without calling it.

All of them share `self._lock`. The applier waits on `self._applier_wake`, a
`Condition` built on that same lock.

---

## 3. Figure 2, rule by rule

### State: `log[]`

> **log[]** log entries; each entry contains command for state machine, and
> term when entry was received by leader (first index is 1)

- `LogEntry(term, data, kind)` in `raft/messages.py`. `data` is **opaque
  bytes**: Raft never looks inside (R1). `kind` is `COMMAND` (a client's
  payload, the default) or `NOOP` (Raft's own entry, §8, R9). It's an explicit
  field rather than "`data == b""` means no-op", because an empty command is a
  legal client payload.
- Stored in `MemoryStorage._log`, a Python list whose slot 0 is a sentinel
  `LogEntry(0, b"")` (R14). So `log[i]` really is entry *i*, 1-based like the
  paper, and `term_at(0) == 0` is the "empty prefix" that `prevLogIndex = 0`
  refers to.
- `_last_log()` (lines 243-248) returns `(lastLogIndex, lastLogTerm)` from
  storage. In the election chunk it was an injected hook that returned
  `(0, 0)` (E12). Now the vote check reads the real log without any other
  change (R13).

### State: persistence seam

> Persistent state on all servers: (Updated on stable storage before
> responding to RPCs) currentTerm, votedFor, log[]

All three live in `MemoryStorage`. The node keeps **no copy**: `_current_term`
and `_voted_for` are read-only properties (lines 235-241), so an assignment that
bypassed storage would raise. There are exactly five write sites in
`node.py`, all calls to storage's three write methods:

| Write | Line | Method |
| --- | --- | --- |
| term increases (step-down, new election) | 375 | `set_term_and_vote(term, vote)` |
| a vote is granted | 554 | `set_term_and_vote(currentTerm, candidate)` |
| leader appends a client entry | 314 | `append([entry])` |
| new leader appends its no-op | 452 | `append([no-op])` |
| follower deletes a conflicting suffix / appends | 667, 669 | `truncate_from(i)`, `append(entries)` |

Step 6 makes those methods durable before they return. Every handler builds its
reply after the calls, so this already has the right shape for "before
responding to RPCs" (R5).

### State: `commitIndex`, `lastApplied`

> **commitIndex** index of highest log entry known to be committed
> (initialized to 0, increases monotonically)
> **lastApplied** index of highest log entry applied to state machine
> (initialized to 0, increases monotonically)

Lines 203-204. `commitIndex` changes in exactly one function,
`_set_commit_index` (508-513), which **refuses to lower it**. That matters in
receiver step 5 (below). `lastApplied` is written only by the applier thread
(line 808). It counts no-ops too: it is "the highest index this node has dealt
with", not "the number of apply_fn calls".

### State: `nextIndex[]`, `matchIndex[]`

> **nextIndex[]** for each server, index of the next log entry to send to that
> server (initialized to leader last log index + 1)
> **matchIndex[]** for each server, index of highest log entry known to be
> replicated on server (initialized to 0, increases monotonically)
> (Reinitialized after election)

`_become_leader`, lines 441-443. A new leader is optimistic: it assumes
everyone already has its whole log (`nextIndex = last + 1`). It is pessimistic
about what it *knows* (`matchIndex = 0`). The consistency check corrects the
optimism. `matchIndex` only grows (line 746). `nextIndex` is set **before** the
no-op is appended (next subsection), so the no-op is exactly the first entry
each follower is offered.

### §8: the leader's no-op at the start of its term

> "each leader [must] commit a blank no-op entry into the log at the start of
> its term" (§8)

`_become_leader`, lines 444-457: right after winning, append
`LogEntry(currentTerm, b"", NOOP)` and replicate it like any other entry. In a
cluster of one, `_advance_commit()` (457) commits it on the spot.

**Why it's needed.** The commit rule (below) only counts replicas of
*current-term* entries. A brand-new leader has none, so it can commit nothing,
not even entries it holds that a previous leader had already committed.
Without a no-op it stays stuck until some client happens to propose. The no-op
is a current-term entry that's there right away. Once it reaches a majority,
everything before it commits too. (R9 has the full reasoning, and the old "no
no-op" decision as the rejected alternative.)

**It never reaches the state machine.** The applier steps over it (lines
791-794). So **the indexes `apply_fn` sees can skip numbers**: in a fresh
cluster the first client entry is index 2, and `apply_fn` never hears about
index 1.

### AppendEntries RPC: receiver implementation

`handle_append_entries` (576-637). First the All Servers term rule (line 579),
then:

> 1. Reply false if term < currentTerm (§5.1)

Lines 581-586. Unchanged from the election chunk: a stale leader is refused,
the timer is not reset, and the leader is not adopted.

Then (unchanged) the sender is this term's legitimate leader. A candidate
steps down, `leader_id` is set, and the election timer is reset (590-603).

> 2. Reply false if log doesn't contain an entry at prevLogIndex whose term
>    matches prevLogTerm (§5.3)

Lines 609-619. Two ways to fail: our log is **too short**
(`prev_index > last_index`) or the entry there has the **wrong term**. Either
way we reply false and change **nothing**. We don't delete anything on a
refusal. (Test: `test_receiver_rejects_on_failed_consistency_check_and_changes_nothing`.)
`prevLogIndex = 0` always passes, because of the sentinel.

> 3. If an existing entry conflicts with a new one (same index but different
>    terms), delete the existing entry and all that follow it (§5.3)
> 4. Append any new entries not already in the log

`_merge_entries` (639-673). Here is the careful part. Walk the new entries
next to ours, one index at a time (loop at 652):

- **We have that index with the same term** (655): same index + same term =
  same entry (Log Matching), so **skip it, and delete nothing**.
- **We have that index with a different term** (657-667): a *real* conflict.
  Assert it isn't committed (658, R16), delete from here to the end (667),
  then append the rest of the new entries (669).
- **We don't have that index** (past our end): append the rest (669).

Why not the simpler "delete everything after `prevLogIndex`, then append"?
Because AppendEntries can arrive **duplicated or late**. Say the follower
holds entries 1-3, all term 1, and a delayed RPC from the same leader arrives
carrying only entry 1 (`prevLogIndex = 0`). The simple version would delete
2 and 3. The leader may already have counted them toward a commit, so
committed data could vanish. With the walk, entry 1 matches and is skipped,
and the loop ends. Entries 2 and 3 are untouched. This is what §5.5 means by
"Raft RPCs are idempotent ... it ignores those entries in the new request".
(Test: `test_duplicated_or_delayed_append_entries_never_truncate_matching_entries`.)

> 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
>    of last new entry)

Lines 626-633. `last_new = prevLogIndex + len(entries)`. That's the last index
this RPC has *proven* matches the leader. We may hold more entries after it,
but they haven't been checked against this leader, so we must not mark them
committed just because the leader's `commitIndex` is higher. For a delayed RPC,
`min(...)` can come out **below** our current `commitIndex`.
`_set_commit_index` ignores that ("increases monotonically", R10). (Test:
`test_commit_index_is_capped_by_the_last_new_entry_and_never_decreases`.)

The success reply carries `matchIndex = last_new` (635-637), the handout's
field.

### Rules for Servers, Leaders

> If command received from client: append entry to local log, respond after
> entry applied to state machine (§5.3)

`propose()` (294-323). On a non-leader it returns an already-failed handle,
`NOT_LEADER` with `leader_hint` (310-313). On the leader: append
`LogEntry(currentTerm, data)` (314), register a pending handle under that index
(317), and send AppendEntries to every idle peer at once (320-322). It
**doesn't** wait. "Respond after applied" is the handle, resolved by the
applier (section 4).

> If last log index ≥ nextIndex for a follower: send AppendEntries RPC with
> log entries starting at nextIndex

`_append_args_for` (469-486): `prevLogIndex = nextIndex - 1`, `prevLogTerm =
log[prevLogIndex].term`, entries = up to 64 entries starting at `nextIndex`
(R7), and `leaderCommit = commitIndex`. When `nextIndex` is past the end, the
same function produces a heartbeat. There are three triggers:

- the heartbeat tick, every `heartbeat_interval`, to every idle peer (`_tick`,
  344-346);
- `propose()`, to idle peers;
- a reply that shows the peer is still behind (765-766).

At most one AppendEntries per peer is in flight (`_ae_in_flight`, 459-467).

> If successful: update nextIndex and matchIndex for follower (§5.3)

`_on_append_reply` lines 740-749. `match = prevLogIndex + len(entries)`,
computed from **our own request**, not the reply (R11). It is raised only if
larger (monotonic), and then `nextIndex = matchIndex + 1`. A rise in
`matchIndex` may commit something, so `_advance_commit()` runs (748).

> If AppendEntries fails because of log inconsistency: decrement nextIndex
> and retry (§5.3)

Lines 750-761, then the retry at 765-766. Two points:

- By here `reply.term == currentTerm` (a higher term already made us step down
  at 735). So a `false` can only mean receiver step 2: a log inconsistency.
- We decrement only if the refusal is about **the probe we're making now**
  (`nextIndex == prevLogIndex + 1`), and never below 1. A delayed refusal of an
  older probe must not decrement twice (R12).

This is Figure 2's plain backoff, one entry per round trip, with no
conflict-term optimisation (R6). The retry is sent at once, not at the next
heartbeat.

> If there exists an N such that N > commitIndex, a majority of matchIndex[i]
> ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).

`_advance_commit` (488-506). Scan N downwards from the last index to
`commitIndex + 1`:

- line 501: if `log[N].term != currentTerm`, **stop**. Terms never decrease
  along a log, so every smaller N is from an older term too. And §5.4.2:
  "Raft never commits log entries from previous terms by counting replicas."
- line 503: count copies = 1 (the leader itself) + peers with
  `matchIndex >= N`.
- line 504: a majority means commit N. Everything below N is committed too, by
  Log Matching (the paper calls this committing "indirectly").

**Why the term check (Figure 8).** Old-term entries can sit on a majority and
still be overwritten. Our 3-node version of it, from
`test_figure8_old_term_entry_on_a_majority_is_not_committed`:

| | n1 | n2 | n3 |
| --- | --- | --- | --- |
| start (all term 3) | [1, **2**] | [1] | [1, **3**] |
| n1 wins term 4 (n3 cut off), appends its no-op, copies index 2 to n2 | [1, 2, 4ₙₒₒₚ] | [1, 2] | [1, 3] |

Index 2 (term 2) is now on n1 and n2, a majority, and n1 knows it
(`matchIndex[n2] = 2`). A leader without the term check would commit it. Now n1
crashes and n3 rejoins. n3's last term is 3, n2's is 2, so n2 votes for n3, and
n3 overwrites n2's index 2 with its own term-3 entry. Index 2 changed after
being "committed". The test makes exactly this happen. It asserts that n1 never
committed index 2, and that the term-2 entry is applied nowhere: n3's own no-op
commits *n3's* index 2, and that's what n2 and n3 apply.

To set this up, the test has to hold n1's no-op back. Its job is to commit
things, so if it reached n2 it would commit index 2 immediately, and there would
be no Figure 8 situation to look at. The test uses one entry per RPC and a
`Gate` that **loses** (never alters) the n1 → n2 AppendEntries that would
successfully take n2 past index 2 (R17). With the gate lifted, the no-op
(index 3, term 4) reaches n2, N = 3 qualifies, and indexes 1-3 commit at once,
with no client proposal (`test_figure8_old_term_entry_commits_only_via_a_current_term_entry`).

### Rules for Servers, All Servers

> If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied]
> to state machine (§5.3)

`_run_applier` (771-811), one thread:

1. Under the lock: wait until `commitIndex > lastApplied` (784-785), then copy
   out entries `lastApplied+1 .. commitIndex` (788-789). Copying is safe:
   committed entries never change.
2. **Lock released**: for a no-op, do nothing (791-794). For a command, call
   `apply_fn(index, data)` (797).
3. Under the lock again: `lastApplied = index` (808) and resolve that index's
   pending handle (809).

One thread doing all of it is what gives "in index order, exactly once". Only
this thread moves `lastApplied`, by one per entry. If `apply_fn` raises, the
applier logs CRITICAL and stops (798-806, R3). It doesn't skip the entry and
doesn't retry it.

### RequestVote: the election restriction, now real

> 2. If votedFor is null or candidateId, and candidate's log is at least as
>    up-to-date as receiver's log, grant vote (§5.2, §5.4)

The code is unchanged (`_candidate_log_ok`, 566-574), but `_last_log()` now
returns the real log. `test_a_node_with_a_stale_log_cannot_win` makes n3 (log
`[1]`, the others `[1, 1]`) the **only** node whose timer ever fires. It
campaigns 9+ times and loses every time. The control case (n3 with `[1, 1]`)
wins at once.

---

## 4. What `propose()` promises (R4)

This is what "acknowledged" will mean for the property checker in step 7.

| Outcome | Meaning | Did it happen? |
| --- | --- | --- |
| `APPLIED` | the entry with exactly this **(index, term)** was committed and the leader's `apply_fn` returned. `result` = its return value | **Yes.** Every node that applies this index applies this entry |
| `NOT_LEADER` | refused at once, nothing appended; `leader_hint` = who this node thinks leads (or `None`) | **No.** Safe to retry on the hinted node |
| `LEADERSHIP_LOST` | the node stepped down before applying it | **Unknown.** A later leader may still commit it |
| `STOPPED` | the node was stopped first | **Unknown** |
| `OVERWRITTEN` | another term's entry was applied at that index | **No** (defence in depth; can't happen today, R4) |

Why **index and term**: after a leader change, a *different* entry can occupy
the same index. `_resolve_applied` (813-823) succeeds only if the applied entry's
term is the handle's term. By Log Matching, same (index, term) means the same
entry. `test_a_proposal_whose_entry_is_overwritten_never_succeeds` checks this.
A cut-off leader accepts x at index 2. The new majority's leader puts its
own no-op in that slot in a later term. x's handle fails, x is applied nowhere,
and the old leader's slot 2 ends up holding the new term.

A pending handle on a leader that is partitioned away stays pending until the
node hears a higher term. Clients must use `wait(timeout)` and treat a
timeout as "unknown" (gap 5).

---

## 5. The end-to-end trace (with one repair and the no-op)

This is real output from three nodes over **real gRPC** on 127.0.0.1, each
with its own server and `GrpcTransport`, in one process (a throwaway script,
not checked in; it only wires `RaftNode`, `RaftServicer` and `GrpcTransport`
together with pre-filled `MemoryStorage`). The starting logs were pre-filled
to set up a Figure 7(f)-style divergence:

| node | log (terms) | data | story |
| --- | --- | --- | --- |
| n1 | [1, 1, 2] | open, bid A 10, **bid B 12** | led term 2, wrote index 3 to n2 |
| n2 | [1, 1, 2] | open, bid A 10, bid B 12 | |
| n3 | [1, 1, 1, 1] | open, bid A 10, **bid C 11, bid C 15** | an old term-1 leader wrote 3-4 only here |

All three are at `currentTerm = 2`, with `commitIndex = 0`. Only n1 has a short
election timeout (R15).

```
17:24:19.036 [n1] term 3 FOLLOWER -> CANDIDATE (was term 2): election timeout
17:24:19.040 [n3] term 3: GRANTED vote for n1 (ok)
17:24:19.040 [n2] term 3: GRANTED vote for n1 (ok)
17:24:19.041 [n1] term 3 CANDIDATE -> LEADER: won election with 2 votes
17:24:19.041 [n1] term 3: appended no-op at index 4
17:24:19.053 [n2] appended 4..4 from leader
17:24:19.054 [n1] term 3: commitIndex 0 -> 4
17:24:19.054 [n1] n3 has no (index 3, term 2): nextIndex -> 3
17:24:19.054 [n1] APPLY index 1: b'open'
17:24:19.054 [n1] APPLY index 2: b'bid A 10'
17:24:19.054 [n1] APPLY index 3: b'bid B 12'
17:24:19.055 [n3] conflict at index 3 (have term 1, leader has 2): deleting 3..4
17:24:19.055 [n3] appended 3..4 from leader
17:24:19.055 [n3] term 3: commitIndex 0 -> 4
17:24:19.055 [n3] APPLY index 1: b'open'
   ... n3 applies 2 and 3 ...
17:24:19.063 >>> client proposes b'bid D 20' on n1
17:24:19.063 [n1] term 3: appended index 5
17:24:19.064 [n2] appended 5..5 from leader
17:24:19.064 [n3] appended 5..5 from leader
17:24:19.064 [n2] term 3: commitIndex 0 -> 4
   ... n2 applies 1, 2, 3 ...
17:24:19.065 [n1] term 3: commitIndex 4 -> 5
17:24:19.065 [n1] APPLY index 5: b'bid D 20'
17:24:19.065 >>> handle: Proposal(index=5, term=3, APPLIED) result=None
17:24:19.154 [n2] term 3: commitIndex 4 -> 5
17:24:19.154 [n3] term 3: commitIndex 4 -> 5
17:24:19.155 [n2] APPLY index 5: b'bid D 20'
17:24:19.155 [n3] APPLY index 5: b'bid D 20'
17:24:19.466 final n1: log terms [1, 1, 2, 3, 3], commitIndex 5, lastApplied 5
17:24:19.466 final n2: log terms [1, 1, 2, 3, 3], commitIndex 5, lastApplied 5
17:24:19.466 final n3: log terms [1, 1, 2, 3, 3], commitIndex 5, lastApplied 5
```

(Two "FOLLOWER -> FOLLOWER (was term 2)" lines for n2 and n3 are trimmed.)

Step by step:

**1. Election (19.036-19.041).** n1 times out and becomes candidate in term 3
with `RequestVote(term=3, lastLogIndex=3, lastLogTerm=2)`.
- n3 compares `(lastLogTerm, lastLogIndex)`: candidate `(2, 3)` against its
  own `(1, 4)`. 2 > 1, so the candidate is more up-to-date **even though n3's
  log is longer**, and n3 grants.
- n2: `(2, 3)` against `(2, 3)` is equal, so it grants.

n1 leads term 3. `_become_leader` sets `nextIndex[n2] = nextIndex[n3] = 4`
(last index 3 + 1) and `matchIndex[*] = 0`, **then** appends the no-op at
index 4 (19.041). n1's log is now `[1, 1, 2, 3ₙₒₒₚ]`.

**2. First AppendEntries to each follower.** On the first tick, each gets
`AppendEntries(term=3, prevLogIndex=3, prevLogTerm=2, entries=[no-op@4],
leaderCommit=0)`. The no-op is the first entry, because `nextIndex` was set
before it was appended.
- **n2** (19.053): has index 3 with term 2, so step 2 passes. It appends 4 and
  replies true. The leader computes `matchIndex[n2] = 3 + 1 = 4` and runs
  `_advance_commit`. For N = 4, `log[4].term = 3 == currentTerm`, and copies =
  n1 + n2 = 2 ≥ 2. **commitIndex 0 -> 4** (19.054), **without any client
  request**. That's the no-op doing its job: it commits index 3, the term-2
  `bid B 12` that couldn't be committed by counting replicas on its own.
- **n3**: index 3 has term **1**, not 2, so step 2 fails and n3 replies false.

**3. Leader applies (19.054).** n1's applier copies 1-4. It calls `apply_fn`
for 1, 2 and 3 and **steps over 4** (the no-op): there is no "APPLY index 4"
line anywhere in the trace.

**4. The repair (19.054-19.055).** The leader gets n3's refusal.
`nextIndex[n3] == 4 == prevLogIndex + 1`, so it is about the current probe:
`nextIndex[n3] -> 3`. Since `3 <= lastLogIndex`, the retry goes out at once:
`AppendEntries(prevLogIndex=2, prevLogTerm=1, entries=[(term 2, "bid B 12"),
no-op@4], leaderCommit=4)`.
- n3: index 2 has term 1 = prevLogTerm, so step 2 passes.
- `_merge_entries`: index 3, n3 has term 1, the leader sends term 2, so that's
  a **conflict**. 3 > commitIndex 0, so it's allowed. Delete 3..4 (`bid C 11`
  and `bid C 15` are gone), then append the leader's 3 and 4. n3's log is now
  `[1, 1, 2, 3]`.
- Step 5: `min(leaderCommit 4, last_new 4) = 4`, so n3's commitIndex goes
  0 -> 4 in the same RPC, and its applier applies 1-3.

Note what was deleted: two entries that **no node had ever applied**. They had
never been committed. A client that sent "bid C" never got APPLIED for it.

**5. A client proposes (19.063).** `propose(b"bid D 20")` on n1 appends index 5,
term 3, registers handle `(5, 3)`, and sends to both peers. That RPC also
carries `leaderCommit = 4`, which is how n2 (19.064) learns that 1-4 are
committed. n2 had appended the no-op but hadn't heard about the commit yet.

**6. Commit and acknowledge (19.065).** The first success reply sets
`matchIndex = 5` for that peer. `_advance_commit`, N = 5: term 3, 2 copies, so
**commitIndex 4 -> 5**. The applier calls `apply_fn(5, b"bid D 20")`, and
`_resolve_applied(5, term 3)` matches the handle, so `APPLIED`. Only now is the
client's request acknowledged. (`result=None` because this script's `apply_fn`
only logs.)

**7. Followers learn the commit (19.154).** On the next heartbeat
(`leaderCommit = 5`), both followers move to 5 and apply it. All three end with
log `[1, 1, 2, 3, 3]` and `commitIndex = lastApplied = 5`. Every node's
`apply_fn` saw indexes 1, 2, 3, 5.

---

## 6. How to see it yourself

```powershell
.venv\Scripts\python.exe -m pytest tests\test_raft_replication.py -v
```

| Test | What it pins down |
| --- | --- |
| `test_receiver_*` (6 tests) | AppendEntries receiver steps 1-5, called directly, no threads |
| `test_duplicated_or_delayed_append_entries_never_truncate_matching_entries` | step 3 vs 4: only a real conflict truncates |
| `test_commit_index_is_capped_by_the_last_new_entry_and_never_decreases` | step 5's `min`, and monotonic commitIndex |
| `test_follower_applies_committed_entries_in_order_once` | applier: only committed entries, in order, once |
| `test_vote_check_reads_the_log_that_append_entries_built`, `test_a_node_with_a_stale_log_cannot_win[...]` | §5.4.1 with the real log |
| `test_proposed_entries_are_applied_on_all_nodes_in_the_same_order` | happy path; `result` from the leader's apply; apply_fn's indexes skip the no-op |
| `test_a_follower_that_was_down_catches_up_after_restore` | catch-up via backoff |
| `test_figure7_divergent_followers_are_repaired` | 5 nodes, Figure 7 (a), (b), (e), (f); the no-op alone commits the repaired logs |
| `test_figure8_*` (2 tests) | the §5.4.2 rule, both halves (no-op held back by `Gate`) |
| `test_old_term_entries_commit_after_a_leader_change_with_no_new_proposals` | **the no-op**: old-term entries on a majority, two leader changes, zero proposals, every node applies them |
| `test_a_committed_entry_survives_the_leader_being_dropped` | Leader Completeness |
| `test_propose_*`, `test_a_proposal_whose_entry_is_overwritten_never_succeeds`, `test_stopping_a_leader_fails_its_pending_proposals` | the handle contract |
| `test_apply_fn_runs_outside_the_node_lock`, `test_an_exception_in_apply_fn_halts_this_nodes_applier` | applier threading (R2, R3) |
| `test_term_and_vote_can_only_be_written_through_storage` | the persistence seam (R5) |
| `test_state_machine_safety_under_drops_and_partitions[0-11]` | Figure 3 over seeded chaos (3 or 5 nodes); converges after healing only if the no-op works |
| `test_wire_carries_the_entry_kind` (adapters) | NOOP / COMMAND survive the wire; an empty COMMAND stays a command |

Without the no-op (the previous version of `node.py`), the no-op test and the
Figure 7/8 tests fail: they were run that way once to check that they depend on
it.

**Election tests adapted** (R13): `test_vote_requires_candidate_log_at_least_as_up_to_date`
(5 cases) and `test_refused_stale_candidate_does_not_burn_the_vote`. Their voter
used to get `last_log=lambda: (3, 2)`. It now gets a real log whose last entry is
(index 3, term 2). The assertions didn't change. The election `Cluster` lost its
unused `last_logs` parameter.

### Timing and flakiness, honestly

- The receiver tests are fully deterministic (no threads, no clock).
- The scenario tests make the leader deterministic (R15) and wait on
  conditions with 5 s bounds. Two of them assert that something does *not*
  happen (Figure 8: no commit; a pending handle stays pending for 0.1 s). They
  first wait for the positive precondition, then hold for many heartbeat
  periods.
- **Two flaky tests were found during this chunk:**
  1. the first draft of the stale-log test let the leader repair n3, after
     which n3 won legitimately. That was a wrong test, rewritten (R15);
  2. one chaos run stalled for 10 s while converging. The likely cause was a new
     leader that could not commit without a client proposal. The first fix was
     a test workaround (keep proposing until everyone caught up). That
     workaround is **reverted**: the leader now appends a no-op (R9), and the
     chaos test is back to one final acknowledged entry followed by "every node
     reaches it", so it converges only if the no-op does its job. The original
     stall was never reproduced (0 in a 150-seed repro), so whether the no-op
     was *its* cause is not proven. The no-op does remove the mechanism, and
     `test_old_term_entries_commit_after_a_leader_change_with_no_new_proposals`
     shows that mechanism deterministically.
- With the no-op in: the full suite once, then 10 repeated runs of the raft
  tests (87 each), all green. That's evidence, not proof.
- The chaos test is randomised on purpose. Its seeds fix the fault schedule
  but not thread interleavings, so each run explores different interleavings.
  The invariant has to hold for all of them.

---

## 7. Known gaps

1. **No persistence (step 6). Now this can lose committed data, not just
   votes.** Concretely, with 3 nodes: leader L commits x with copies on L and
   A, and the client gets `APPLIED`. A restarts with an empty log (memory
   only). L crashes. A and B both have empty logs, one of them wins, and **x is
   gone**: an acknowledged write is lost. Fix: storage's three write methods
   become durable before returning (the seam is ready, R5). Until then the
   Leader Completeness argument in section 1 assumes nodes don't restart.
   **Closed in step 6:** this exact scenario is now a test;
   [raft-persistence.md](raft-persistence.md) §5b.
2. **`lastApplied` and the state machine aren't durable either (R2).** Step 6
   has to decide between saving them together and replaying the log from 1 at
   boot. **Decided in step 6: replay** (P11).
3. **Plain backoff is O(divergence) round trips (R6).** A follower that is far
   behind takes one refused RPC per missing entry just to find the match point.
   It's fine on localhost and slow over a WAN.
4. **No client deduplication (§8).** `LEADERSHIP_LOST` means "unknown"; a
   client that retries can get its command into the log twice. `apply()` has
   to tolerate that, or client ids/sequence numbers are needed.
5. **No CheckQuorum (§9.6 of Ongaro's thesis).** A leader that has been
   partitioned away keeps accepting `propose()` calls. They stay pending until
   it hears a higher term (`test_a_proposal_whose_entry_is_overwritten_never_succeeds`
   relies on exactly that). Clients need a timeout.
6. **No log compaction / snapshots (§7).** The log grows forever, and a
   restarted node replays everything. Each election also adds one no-op.
7. **No client-facing RPC.** `propose()` is a Python call. It goes over gRPC
   when the auction is wired in (next chunks), along with redirecting via
   `leader_hint`.
8. **No read path.** `GetAuction` / `ListAuctions` on a follower would read
   possibly stale state. Linearizable reads (§8) need the no-op (done now) *and*
   a leadership check before answering (not done).
9. **Inputs are trusted.** A malformed AppendEntries (for example a negative
   `prevIndex`, or an unknown entry kind, which raises in `wire.py`) isn't
   validated. Same as election gap 8.
10. **The in-memory network is still polite.** Duplicate and reordered
    AppendEntries are tested by calling the handler directly. The transport
    itself never delays, duplicates or reorders. The test-only `Gate` can lose
    messages selectively, but it's not a general chaos tool. That's the step 7
    chaos harness.
11. **Tests use the real clock** (unchanged from the election chunk).

---

## 8. Things I am not fully sure about

- **The original 10 s stall** (section 6). The no-op removes the mechanism I
  think caused it, but the stall itself was never reproduced.
- **Fail-stop on an `apply_fn` exception (R3)** is a judgement call. If the
  auction's `apply()` can raise on bad input that a client controls, one bad
  command would halt every replica's applier. That has to be checked when the
  auction is wired in: `apply()` must return rejections, not raise them.
- **`reply.match_index` isn't used by the leader (R11).** It's filled in
  because the handout has the field. If the examiner expects it to drive
  `matchIndex`, the answer is that the request gives the same value without
  trusting the follower.
- **The `Gate` in the Figure 8 tests peeks at the receiver's log** to decide
  which messages to lose (R17). It only ever loses messages, which is legal
  network behaviour, but an examiner may ask why a test can see inside a node.
