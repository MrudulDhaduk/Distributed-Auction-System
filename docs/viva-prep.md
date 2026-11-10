# Viva prep

The 15 most likely questions, most likely first, each with an answer you can
say out loud and the code to point at. Function names are the stable handle.
Line numbers in the older docs have drifted (see "Things I can't fully answer
yet", item 8). Figure and section numbers are from `papers/raft-extended.pdf`.

When asked "is this right?", first say which Figure 2 rule it implements.

---

## The 15

**1. Walk me through an election.** (Figure 2, Followers / Candidates; §5.2)
A follower that hears nothing for its random 0.5-1.0 s timeout increments its
term, votes for itself, resets its timer and asks everyone for a vote, in
parallel. Each voter grants at most one vote per term. A majority makes it
leader, and it starts heartbeats within one 5 ms tick. In a split vote nobody
wins, and the randomised timers make one node try term+1 first.
→ `raft/node.py`: `_tick`, `_start_election`, `handle_request_vote`,
`_on_vote_reply`, `_become_leader`. A real split vote:
[auction-on-raft.md](auction-on-raft.md) §11.

**2. Why can't two leaders exist in one term?** (Figure 3, Election Safety)
Two facts. A node votes once per term: `votedFor` belongs to `currentTerm`,
and the only code that changes the term also blanks the vote. And a winner
needs a majority: two majorities of three share a node, and that node voted
only once. The vote is fsync'd before the reply, so a restart can't forget
it.
→ `_transition` (the only term write), `handle_request_vote` (`votedFor in
(None, candidateId)`), `FileStorage.set_term_and_vote`. Tripwire: the
`assert` in `handle_append_entries` (E20). Tests: E19 (15 seeds) and the
checker's `at-most-one-leader-per-term`.

**3. What are the term rules?** (Figure 2, All Servers; RPC receiver step 1)
Any request or reply with a higher term: adopt it and become a follower. Any
request with a lower term: refuse, and reply with my term so the sender
steps down. A reply that comes back after my term or role changed is
ignored. Terms never go backwards (asserted).
→ `_step_down_if_stale` (called on every request and every reply);
`_on_vote_reply` (`role == CANDIDATE and term == sent_term`);
`_on_append_reply` (`args.term == currentTerm`); E4, E10.

**4. A candidate with a shorter log asks for your vote. What do you do?**
(§5.4.1, RequestVote receiver step 2)
I grant only if its log is at least as up-to-date as mine: later last
*term* wins, and if the terms are equal, the longer log wins. That's a tuple
comparison. This is what keeps committed entries in every future leader's
log.
→ `_candidate_log_ok`: `(lastLogTerm, lastLogIndex) >= (myTerm, myIndex)`.
E12. In the real trace, n2 refused n3 because n3 ended at index 7 and n2 at 8.

**5. The commit rule: why only current-term entries?** (Figure 2 Leaders,
last bullet; §5.4.2; Figure 8)
An old-term entry stored on a majority can still be overwritten by a node
whose last term is later (Figure 8(d)). A current-term entry on a majority
can't: any future winner needs a vote from that majority. So the leader
counts replicas only for its own term's entries. Everything before such an
entry commits with it, by Log Matching.
→ `_advance_commit` (scans down, `break` at the first older term).
Test: `test_figure8_old_term_entry_on_a_majority_is_not_committed`; R8, R17.

**6. Why does a new leader write an empty entry?** (§8)
Because of question 5. A fresh leader has no current-term entry, so it can't
commit anything, not even entries the previous leader already committed and
acknowledged. The no-op is that entry, available at once. It's also
precaution one for reads: a leader must know its commitIndex is current.
The applier skips it, so `apply_fn`'s indexes have gaps.
→ `_become_leader` (sets nextIndex, then appends `EntryKind.NOOP`);
`_run_applier` (no-ops never reach `apply_fn`); R9. Test:
`test_old_term_entries_commit_after_a_leader_change_with_no_new_proposals`.

**7. A follower's log diverged. How is it repaired?** (Figure 2
AppendEntries receiver 2-5; Leaders "decrement nextIndex and retry"; §5.3)
The leader sends `prevLogIndex/prevLogTerm` with each batch. If the follower
lacks that entry, it refuses, and the leader steps `nextIndex` back by one
and retries at once. When they match, the follower deletes from the first
real conflict onward and appends the rest. Its commitIndex becomes
`min(leaderCommit, last new entry)`, and it never goes down. matchIndex comes
from what the leader *sent*.
→ `handle_append_entries`, `_merge_entries`, `_on_append_reply`; R6, R10, R11,
R12. Cost: one round trip per missing entry (R6). Trace:
[raft-replication.md](raft-replication.md) §5.

**8. Why not just truncate everything after prevLogIndex?** (§5.5)
The request may be an old, delayed message that carries less than the leader
has sent since. Truncating would delete matching entries the leader may
already have counted toward a commit. So the follower truncates only where
the terms differ. A tripwire asserts it never deletes at or below its
commitIndex.
→ `_merge_entries` (`continue` on same term; `assert index > commitIndex`);
R10, R16.

**9. "Persistent state, updated on stable storage before responding." Show
me.** (Figure 2, State)
Term, vote and log change only through three storage methods. Each one
writes, fsyncs, and only then updates the in-memory copy. The node calls them
while holding its lock and builds the reply afterwards, so a reply never
describes something that isn't on disk. The state file is replaced atomically
(tmp, fsync, `os.replace`). The leader counts itself toward a majority only
after its own append returned.
→ `raft/file_storage.py`: `set_term_and_vote`, `append`, `truncate_from`;
`raft/node.py` `propose` (append, then `_advance_commit`); R5, P1, P3, P8,
P9, P12.

**10. A node is killed with kill -9 and restarted. What happens?** (Figure 2
volatile state; §5.5)
It reads `state.json` and `log.bin`. A torn last record is cut off. Damage
anywhere else is refused, and the node won't start. commitIndex and
lastApplied start at 0 and the state machine starts empty. When the leader
tells it the commitIndex, it re-applies the log from index 1 and gets the
same state, because `apply()` is deterministic. Dropping the torn record is
safe: its append never returned, so it was never acknowledged.
→ `FileStorage.__init__`, `_load_state`, `_recover_log`, `scan_log`; P6,
P7, P11. Tests: [raft-persistence.md](raft-persistence.md) §5;
`test_a_full_restart_rebuilds_identical_auction_and_session_state`.

**11. The leader commits a bid, crashes before replying, and the client
retries. Is the bid placed twice?** (§8)
No. Every write carries `(client_id, seq)`, and a retry reuses the same seq.
`apply_fn` keeps the latest seq and its response per client: an equal seq
returns the stored response without executing, and a lower one is refused
as stale. The table is replicated state, so the new leader has it too.
→ `replication/state_machine.py` `StateMachine.apply_fn`;
`client/auction_client.py` `ClusterClient` (seq taken once per operation);
A5, A17, A18. Real trace: [auction-on-raft.md](auction-on-raft.md) §11
(4 attempts, one seq, one bid). Chaos: 88 unknown bids, 3 committed, each
once.

**12. How are reads linearizable without writing to the log?** (§8, the two
precautions; thesis §6.4)
Only the leader serves reads. First it waits until its own no-op has
committed. Then it records `readIndex = commitIndex` and sends a fresh
heartbeat round. When a majority has answered an AppendEntries built after
that, still in my term, no newer leader can have existed. Then it waits
until it has applied up to readIndex. A leader lease was rejected because it
"would rely on timing for safety".
→ `RaftNode.read_index`, `_round_acks`; `RaftReplicator.read_barrier`;
`AuctionServicer.GetAuction` / `ListAuctions`; the interceptor on an
unknown token (A16); A14, A15, A19.

**13. Why 0.5-1.0 s and 0.1 s, and not the paper's 150-300 ms?** (§5.6:
broadcastTime ≪ electionTimeout ≪ MTBF)
On this machine "broadcast time" includes a Python process stalling on
Windows (GIL, the 15.6 ms timer tick, fsync). Measured: at 0.15-0.3 s the
term went 2 → 68 in 17 kills, from spurious elections that deposed healthy
leaders. At 0.5-1.0 s it rose by exactly one per kill. The cost is a slower
failover: election p50 892.7 ms against 462 ms. Safety never depends on
timing, only availability does.
→ `raft/node.py` `DEFAULT_ELECTION_TIMEOUT`, `DEFAULT_HEARTBEAT_INTERVAL`;
E1; [stress-results.md](stress-results.md) "Finding". The client still sees
~2.2 s because of the Windows connect refusal (T18).

**14. How do you keep `apply()` deterministic?** (CLAUDE.md constraint;
State Machine Safety)
Everything non-deterministic happens once, on the leader, before proposing,
and travels in the command as data. The bid time is stamped in the handler,
the close time by the leader's closer, and the session token is minted
before the Login is proposed. The LLM is called first and only its text is
replicated. One applier thread applies in index order. Times are exact
integer microseconds.
→ `AuctionServicer.PlaceBid` / `CreateAuction`, `AuctionCloser.tick`,
`SessionStore.create`, `RaftNode._run_applier`, `common/time_conv.py`; A1,
A3, A7, A10, R2, T1. Evidence: `replicas-identical` compares full
canonical JSON.

**15. What does your checker prove, and what doesn't it?**
For one recorded run, it proves that the eight properties hold over what the
clients saw and every node's final state. Each property has a test showing it
can fail. It's not a proof over all executions: a seed fixes only the fault
schedule. It isn't a linearizability check. It looks at final states, except
for leader sightings, which can miss a leader that lived under 50 ms. It
covers process faults only, on one machine with one clock (T11 needs that).
And the workload never reads.
→ `checker/properties.py` `check_all`; `tests/test_checker.py`; T9, T10,
T11; [testing-and-chaos.md](testing-and-chaos.md) §4 and Known gaps 2, 4, 5.

### Five more, one line each

- *"Clients wait 2.2 s but the election takes 0.9 s?"* The first retry goes
  to the dead leader's port, and Windows takes 2.0 s to refuse it (T18).
- *"Who closes an auction?"* Every node runs the closer, but only the leader
  proposes `CloseAuction`, and `apply()` rejects a second one
  (`AuctionCloser.tick`, A7).
- *"Does a session survive failover?"* Yes. Login/Logout are log commands,
  and the token is minted before proposing (`SessionStore.create`, A3, A16).
- *"Why is a log entry opaque bytes?"* Raft agrees on order. `apply()` decides
  validity (R1, A1).
- *"UNAVAILABLE: was my bid placed?"* Maybe. Retry with the same seq, and it
  happens exactly once (A6, A11).

---

## Things I can't fully answer yet

1. **Read-index under chaos.** The chaos workload only writes
   (`harness/workload.py` has no `get_auction`/`list_auctions`). So the
   "built after the round" argument ([auction-on-raft.md](auction-on-raft.md)
   §16 asked for it to be checked in step 7) is covered by unit tests and the
   demo's two reads only.
2. **The original 10 s stall (R9).** The no-op removes the mechanism I
   blamed, but the stall itself was never reproduced (0 in a 150-seed repro).
3. **Power loss on Windows.** Whether NTFS can lose the latest `os.replace`.
   Whether this machine's 0.25 ms fsync reaches stable media. What
   `MOVEFILE_WRITE_THROUGH` promises for a same-volume rename.
   [raft-persistence.md](raft-persistence.md) §8. Untested.
4. **Three inferences, not measurements:** the late "not settled" waits blamed
   on replay ([stress-results.md](stress-results.md)), which process held
   `state.json` (T17), and the PID-reuse path that killed the harness (T19).
5. **Can any client input make M1's `apply()` raise?** If one can, every
   replica's applier halts at that index (fail-stop, R3).
   [raft-replication.md](raft-replication.md) §8 said to check this. I haven't
   found a test that feeds `apply()` hostile values.
6. **Problem-statement items that weren't built:** the mock payment/escrow
   service and `ExtendAuction`. The auction server never calls the LLM's FAQ
   or result-summary RPCs (they exist and can be called directly). The
   dashboard shows a digest, not each node's high bid. And §6.3's "re-executed"
   is only true of the schedule (T9).
7. **M1's O(bids) `apply()`.** That's your code. If asked why it isn't O(1):
   keeping the max in the auction would change `apply()`'s logic, which the
   chunks were told not to do ([testing-and-chaos.md](testing-and-chaos.md)
   gap 10). Whether to change it is your call.
8. **Stale line numbers.** For example decisions.md E1 cites `node.py:68-69`,
   but the constants are now at lines 99-100, and testing-and-chaos.md's
   checker line numbers are a few lines off. Answer with function names.

---

## Reading order: the six files to read first

| # | file | read these, in this order | why first | companion doc |
|---|---|---|---|---|
| 1 | `raft/node.py` | `_transition`, `_step_down_if_stale`, `_start_election`, `handle_request_vote`, `_candidate_log_ok`, `_on_vote_reply`, `_become_leader` | Election and term rules: Q1-Q4, the most likely questions | [raft-election.md](raft-election.md) |
| 2 | `raft/node.py` (again) | `propose`, `_append_args_for`, `handle_append_entries`, `_merge_entries`, `_on_append_reply`, `_advance_commit`, `_run_applier`, then `read_index` | Replication, repair, Figure 8, the no-op, reads: Q5-Q8, Q12 | [raft-replication.md](raft-replication.md) |
| 3 | `raft/file_storage.py` | `set_term_and_vote`, `_replace_with_retry`, `append`, `truncate_from`, `scan_log`, `_recover_log` | Durable before reply, crash recovery: Q9-Q10 | [raft-persistence.md](raft-persistence.md) |
| 4 | `replication/state_machine.py` | `apply_fn`, `_dispatch`, `_high_bid` | The §8 dedupe table, and where Raft meets your M1 `apply()`: Q11, Q14 | [auction-on-raft.md](auction-on-raft.md) §4 |
| 5 | `server/auction_servicer.py` + `replication/replicator.py` | `PlaceBid`, `CreateAuction`, `GetAuction`; `submit`, `read_barrier` | The write path end to end, determinism at the edge: Q14 | [auction-on-raft.md](auction-on-raft.md) §2 |
| 6 | `checker/properties.py` | `check_all`, then each `check_*` | What is proven and what isn't: Q15 | [testing-and-chaos.md](testing-and-chaos.md) §4 |

Keep [decisions.md](decisions.md) open beside them. Every non-obvious line
has an entry there with its own viva question.
