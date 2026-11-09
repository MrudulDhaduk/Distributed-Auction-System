# Testing and chaos: status RPC, dashboard, chaos harness, property checker

Build-order step 7. Steps 3-6 built Raft and put the auction on it. This
step tries to break it on purpose, then checks the result against written-down
properties. Decisions T1-T18 are in [decisions.md](decisions.md#testing-and-chaos).
The numbers are in [stress-results.md](stress-results.md).

```
                     +-------------------- scripts/chaos.py ---------------------+
                     |  seeded schedule        workload (one ClusterClient       |
                     |  (harness/schedule.py)  per actor, harness/workload.py)   |
                     |        |                       |  history.jsonl          |
   kill -9 / suspend |        v                       v                         |
   restart / resume  |  harness/procs.py      client ports 50091-3              |
                     +--------|------------------------|------------------------+
                              v                        v
          +---------- n1 ----------+  +---- n2 ----+  +---- n3 ----+   + LLM server
          | server.serve process   |  |            |  |            |
          | client port: Auth+Auction| ...          |  |            |
          | peer port: Raft+Status <---- harness/status.py polls GetNodeStatus (50 ms)
          | data/n1 (FileStorage)  |  |            |  |            |   scripts/dashboard.py (200 ms)
          +------------------------+  +------------+  +------------+
                              |  at the end: DumpState from every node -> dumps.json
                              v
                checker/ (python -m checker <run dir>): PASS/FAIL per property
```

---

## 0. Step 0: the M1 time conversion bug (T1)

Not a Figure 2 rule: a wire-format bug, pinned by a strict xfail in step 4/5
and fixed here on request, in its own commit.

**Rule (CLAUDE.md):** "Time-dependent values are stamped once, before
proposing, and carried on the command." The stamp has to survive the trip to
bytes exactly, because `apply()` compares `curr_time > close_time` to the
microsecond.

**Code:** [common/time_conv.py](../common/time_conv.py), `dt_to_micros` and
`micros_to_dt`.

**In plain words:** the old code did `int(dt.timestamp() * 1_000_000)`.
`timestamp()` is a float, and a float has 53 bits of precision; epoch
microseconds for 2039 need 51 bits *before* the multiplication rounds. So
`2039-12-20 05:40:54.254810` became `2207972454254809.8` and `int()` cut it to
`...809`. The new code never makes a float: `dt - EPOCH` is a `timedelta`
(integers inside), and `timedelta // timedelta(microseconds=1)` is integer
division.

**Why it never broke agreement:** only the leader converts, once, and every
node (the leader included) applies the decoded value. All replicas held the
same wrong microsecond. It was wrong against the user, not between replicas.

**Data dirs written before commit `cd8128a` should be wiped**
(`Remove-Item -Recurse data`). Dev data only.

Tests: `tests/test_time_conv.py` (200 000 seeded random values both ways,
1900-2200, plus the original counterexample); `tests/test_codec.py`'s
`test_round_trip_is_exact_to_the_microsecond` is now a normal test.

---

## 1. Seeing inside a node: `StatusService` (T2-T5)

**Rule (design):** to check "all replicas agree" we need each node's own view
of Figure 2's state (currentTerm, role, commitIndex, lastApplied, the log's
tail) and of its state machine. And we need it *without* going through Raft,
because we want to see the disagreement when it happens.

**Code:**
- [proto/status.proto](../proto/status.proto): `GetNodeStatus`, `DumpState`.
- [server/status_servicer.py:30](../server/status_servicer.py#L30) `StatusServicer`.
- [server/serve.py](../server/serve.py): registered on the **peer** server, next
  to `RaftService`.
- [replication/digest.py:79-98](../replication/digest.py#L79-L98) `canonical`,
  `canonical_json`, `state_digest`.
- [replication/state_machine.py:110](../replication/state_machine.py#L110) and
  `indexed_snapshot` (line 161).

**In plain words:**

1. `GetNodeStatus` reads `raft.status()` (one consistent snapshot under the
   node lock) and the state digest. No auth: it is on the peer port, like
   `RaftService`, for the same reason (T2).
2. The digest is `sha256` of one exact JSON string (the *canonical form*):
   auctions as a list sorted by id, bids in applied order, every time as
   integer epoch micros, sessions and the dedupe table as sorted lists, JSON
   written with sorted keys and no spaces. Same state gives the same bytes on
   every node (T3).
3. Session tokens are bearer credentials, so the canonical form only holds
   `hash_token(token)` (first 16 hex digits of its sha256). The dump can go
   over an unauthenticated port without handing out logins (T3).
4. `digest_index` says *which* state the digest covers: the index of the last
   command `apply_fn` saw. It is recorded inside `apply_fn` under the state
   machine lock, so `(index, snapshot)` come out together (T4). It can be
   below `lastApplied`, because no-ops never reach `apply_fn`.
5. `DumpState` returns that same JSON string, so the checker can confirm
   `state_digest == sha256(state_json)` itself (T5).

**Honest caveat:** `GetNodeStatus` reads Raft's status and the state machine
one after the other, not under one lock. `last_applied` and `digest_index` can
come from two moments a few microseconds apart. `digest_index` can even be one
ahead of `last_applied`, because the applier calls `apply_fn` first and bumps
`lastApplied` afterwards. That's fine for a dashboard. The checker only uses
dumps taken after the cluster is quiet.

---

## 2. The dashboard

**Code:** [scripts/dashboard.py](../scripts/dashboard.py), on top of
[harness/status.py](../harness/status.py) (`StatusPoller`, `agreement`,
`events`, `current_leader`).

It polls all three nodes in parallel every 0.2 s, with a 0.3 s timeout per
call. A killed node refuses at once. A suspended node accepts the TCP connection,
then times out. Both show as `DOWN`. The verdict line:

| verdict | meaning |
|---|---|
| `agree` | every reachable node: same `digest_index`, same digest |
| `catching up` | indexes differ, which is normal for a moment after every write |
| `DIVERGED` | same index, different digest. `apply()` is deterministic, so this is a real bug |

Events: `n1 DOWN`, `n1 UP`, `leader n2 (term 4)`, `no leader (was n2, term 4)`.
A `LEADER` row whose term is below the current leader's is marked `(stale)`.
That's a deposed leader that hasn't heard the new term yet, which is legal
(Figure 3 only forbids two leaders in the **same** term).

```
.\scripts\run_cluster.ps1 -NoLLM          # then, in another window:
.venv\Scripts\python.exe scripts\dashboard.py
.venv\Scripts\python.exe scripts\dashboard.py --base-peer 50081   # a chaos run's cluster
```

---

## 3. The chaos harness (T6-T9, T12, T13)

### 3.1 Real processes, real faults: [harness/procs.py](../harness/procs.py)

Each node is `python -m server.serve` with its own `--data-dir`. Its output is
appended to `logs/<id>.log`, so a restarted node continues the same file.

| fault | how | what the node sees |
|---|---|---|
| kill -9 | `TerminateProcess` on the interpreter, then the launcher ([`hard_kill`, line 146](../harness/procs.py#L146)) | nothing: no handler, no `finally`, no flush. Only what FileStorage fsync'd survives |
| restart | same command line, same data dir (`ensure_started`, [harness/run.py:32](../harness/run.py#L32)) | P11: rebuild state by replaying the log |
| suspend | `NtSuspendProcess` on every process in the tree ([line 111](../harness/procs.py#L111)) | frozen mid-instruction; sockets stay open, so peers' RPCs time out: a *slow* node |
| resume | `NtResumeProcess` | carries on with stale beliefs (a frozen leader still thinks it leads) |

**Found while building it (T6):** on Windows a venv's `python.exe` is a
*launcher* that runs the real interpreter as a child process. My first
`suspend` froze only the launcher. The test caught it: the counter went 13 → 57
while "suspended". So every operation now walks the process tree
(`process_tree`, Toolhelp32 snapshot). Kill already worked through the
launcher's job object, but now it hits the interpreter directly.
`test_the_venv_python_is_a_launcher_and_the_tree_finds_the_interpreter` pins
this.

### 3.2 The seeded schedule: [harness/schedule.py](../harness/schedule.py)

`make_schedule(seed, duration)` returns non-overlapping **episodes**: at
`at`, inject `action`; `down_for` seconds later, heal it; then a 3-8 s gap.

| action | weight | effect |
|---|---|---|
| `kill_leader` | 35 % | the classic failover |
| `kill_follower` | 20 % | leader keeps a majority, must keep committing |
| `suspend_leader` | 15 % | old leader comes back believing it leads (stale leader) |
| `suspend_follower` | 15 % | slow node, and on resume, see §6 "Known gaps" item 1 |
| `kill_two` | 15 % | **majority lost**: nothing can commit; clients get real "unknown" outcomes |

**Same seed, same schedule, not the same run (T9).** The seed fixes times,
actions, durations, and `pick` (which follower: `resolve_pick` indexes the
sorted candidate list). It cannot fix *who leads* when a fault fires, or any
thread interleaving: those come from election timers, the OS scheduler, fsync
latency. Thread timing is not replayable. The run directory keeps
everything needed to *analyse* a run afterwards (history, dumps, every node's
log), not to replay it.

### 3.3 The workload: [harness/workload.py](../harness/workload.py)

**The workload rule (T7):** the dedupe table keeps one `(seq, response)` per
client_id and refuses an older seq as stale (A5). So **concurrency comes from
many client instances, each issuing its writes one after another.** Each actor
is a thread with its own `ClusterClient` (own client_id, own seq counter, own
token). Every write goes through `record_write`
([line 80](../harness/workload.py#L80)) on that actor's own thread, so one
client_id never has two writes in flight. The checker verifies this after the
run (`history-well-formed`).

| actor | does |
|---|---|
| `Seller` | creates auctions closing 15-60 s ahead, ~30 % with attributes (the LLM is called), keeps ≤ 4 open |
| `Bidder` ×4 | random open auction (10 %: any, including closed), bids above the highest it has heard |
| `Racer` ×3 | waits for a race call, bids the race amount at once |
| race coordinator | every 0.5-2 s: picks an auction, sends the **same** `(auction, amount)` to all racers at the same moment: deliberately racing equal bids |

Retries use the real client: same `(client_id, seq)` on every retry, until a
definite answer or `give_up_after` (chaos: 6 s, T12) raises
`ClusterUnavailable`. That's recorded as **unknown**, and the actor moves on with
a new seq.

**Bid identity (T7):** the checker names a bid by `(auction, bidder,
amount)`. M1 has four users, so clients share usernames. Amounts keep them
apart: bidder *i* bids only amounts with `amount % M == i` (M = bidders + 1),
and residue 0 is for races, whose racers all have different usernames. So no
two clients can produce the same triple, and the checker *verifies* that
rather than assuming it.

**History record** (one per write): `client, client_id, user, seq, op,
outcome (ok | rejected | unknown), reason, start, end` (wall clock, epoch micros),
`latency` (monotonic), `auction_id, amount, high_bid, high_bidder` (bids),
`item, close_time` (creates), `token_hash` (logins).

### 3.4 End of a run (T13)

Stop the workload, wait for in-flight retries to finish → `cluster.heal()` →
wait for the last auction's close time → `quiet()` (every node up, one leader,
same commitIndex, everyone applied everything, digests agree, for 1 s) →
`collect_dumps()` (retried until all three are at one `digest_index`) →
leader sightings from the status polls **and** every node log's
`-> LEADER` lines → checker.

---

## 4. The property checker (T10, T11): [checker/properties.py](../checker/properties.py)

Pure functions over three inputs: `history`, `dumps`, `leaders`. Each
property returns PASS/FAIL, how many things it checked (so a vacuous pass is
visible), and the evidence.

| property | rule | code |
|---|---|---|
| `history-well-formed` | workload rule: per client, no overlap, seq strictly increasing and unique; bid triples unique | [line 113](../checker/properties.py#L113) |
| `acked-writes-on-every-node-exactly-once` | every **ok** bid is on every node exactly once (and its reply named itself as the high bid); every ok create and login is there too | [line 140](../checker/properties.py#L140) |
| `replicas-identical` | same `digest_index`; each digest = sha256 of its JSON; same digest; same JSON (full comparison, reports the first differing path) | [line 194](../checker/properties.py#L194) |
| `bids-strictly-increasing-winner-is-max` | per auction each bid above the previous; open → no winner; closed → winner = max bid | [line 226](../checker/properties.py#L226) |
| `no-bid-accepted-after-close` | every bid stamp ≤ close_time ≤ closed_at; no **ok** bid whose client *started* it after close_time | [line 247](../checker/properties.py#L247) |
| `rejections-consistent-with-final-state` | a rejected bid is absent; "highest bid is H" → H ≥ amount and a bid of H exists; "already closed" → closed; "Time is up" → close_time before the reply; "stale" → never | [line 284](../checker/properties.py#L284) |
| `unknown-outcome-at-most-once` | an unknown bid is present 0 or 1 times, never 2 | [line 338](../checker/properties.py#L338) |
| `at-most-one-leader-per-term` | Figure 3, Election Safety, over every sighting | [line 353](../checker/properties.py#L353) |

Figure 3, quoted from the paper: *"Election Safety: at most one leader can be
elected in a given term. §5.2"*. And §8, which the exactly-once checks test:
*"the state machine tracks the latest serial number processed for each client,
along with the associated response. If it receives a command whose serial
number has already been executed, it responds immediately without
re-executing the request."*

Three things worth being able to explain:

1. **How a dedupe bug would show up.** Suppose a retry were executed a second
   time instead of being answered from the table. It would *not* add a second
   bid, because the first attempt already made the high bid equal to its
   amount, so the second execution is refused: "Bid of 150 is not added
   because current highest bid is 150". The client would see **rejected** for
   a bid that is in the state. `rejections-consistent` catches exactly that
   (`test_a_rejected_bid_that_is_in_the_state`).
2. **"No bid after close" seen from the history (T11).** The server stamps
   `curr_time` *after* the client started the call, on the same machine's
   clock. A bid the client started after `close_time` therefore has a stamp
   after `close_time` and must be refused. An **ok** one is a violation. This
   needs one clock for clients and servers, which holds here (one machine)
   and wouldn't across machines.
3. **A bid applied after the `CloseAuction` entry** would also break
   `bids-strictly-increasing-winner-is-max`, because it would be above the frozen
   winner.

Every property has a test that breaks exactly that thing and checks the
evidence names it ([tests/test_checker.py](../tests/test_checker.py), 23 tests).
The good case is built through a real `StateMachine` and the real canonical
form, so the dumps have exactly a node's shape.

---

## 5. End-to-end trace (seed 5, 45 s smoke run, real processes)

Schedule printed at start:

```
t=   5.00s  suspend_follower down  5.74s  pick 0.795
t=  18.45s  kill_two         down  6.44s  pick 0.029
t=  30.22s  suspend_leader   down  7.66s  pick 0.901
```

**A deliberate equal-bid race**, auction 1000001, race amount 20 (residue 0,
M = 5):

| racer | user | seq | outcome | reply |
|---|---|---|---|---|
| racer1 | nisarg | 2 | **ok** | "Bid of 20 added", high 20 by nisarg |
| racer0 | mrudul | 2 | rejected | "Bid of 20 is not added because current highest bid is 20" |
| racer2 | karan | 2 | rejected | same |

The coordinator handed all three the call at the same moment. Each took
5-6 ms end to end. The log decided the order: whichever `PlaceBid` entry came
first in the log won, and `apply()` refused the other two on every node.

**The `kill_two` episode (majority lost, then regained):**

1. `[19.54s] FAULT kill_two: n1 killed, n2 killed (leader was n1)`. n1 led
   term 3. The last entry n3 got from it was index 446.
2. n3 is alone. Its election timer fires every 0.5-1.0 s and every election
   fails (one vote of three):
   `21:52:23.179 [n3] term 4 FOLLOWER -> CANDIDATE` … `21:52:29.070 [n3] term 11 CANDIDATE -> CANDIDATE`.
   Eight failed elections, term 4 → 11.
3. `[26.98s] HEAL n1 restarted`. n1 reads its disk:
   `term 3, votedFor n1, 446 log entries (last term 3)`. Its own timer fires
   first: `term 4 FOLLOWER -> CANDIDATE`. n3 refuses
   (`refused vote for n1 (stale term 4)`), and n1 learns term 11 from the reply
   and steps down (`term 11 CANDIDATE -> FOLLOWER ... higher term 11 seen in RequestVote reply from n3`).
   Figure 2, All Servers: *"If RPC request or response contains term T >
   currentTerm: set currentTerm = T, convert to follower"*.
4. `21:52:29.879 [n3] term 12 CANDIDATE`. n1 grants: n3's log ends at
   (446, term 3), the same as n1's, so it is "at least as up-to-date" (§5.4.1).
   `21:52:29.887 [n3] term 12 CANDIDATE -> LEADER: won election with 2 votes`.
5. `[n3] term 12: appended no-op at index 447` (§8), then
   `closer: proposed CloseAuction for [1000005, 1000007]`. Those auctions
   came due while there was no majority. The new leader's closer picks them up.
6. The slowest writes of the run are this episode: `bidder0 seq 99` on auction
   1000005 waited 8.64 s through retries, then got "Auction is already closed".
   It was *sent* before the close, but it only reached a leader with a
   majority after the `CloseAuction` was in the log. See Known gaps item 3.

**The end:** all three nodes at `digest_index 751`, digest
`c0775ae27f59a7b7…`. Leaders seen: (1, n2), (3, n1), (12, n3), (13, n1), one
per term. 601 accepted bids, 109 rejected, 72 race bids, 0 unknown (the
client then gave up after 30 s, longer than any outage. That is why chaos now
uses 6 s, T12). All eight properties pass.

---

## 6. Results of the 10-minute run

**Seed 737075**, `scripts/chaos.py --duration 600` (defaults: 1 seller, 4
bidders, 3 racers, client give-up 6 s, LLM server running), run dir
`runs/chaos-737075-20261109-215345`. Exit code 0.

| | |
|---|---|
| faults injected | 58: kill_leader 22, kill_two 13, kill_follower 11, suspend_leader 7, suspend_follower 5 |
| leader terms | 47 different terms had a leader; the last term was 184 (the gap is failed elections while a node was alone during `kill_two`) |
| writes | 8 247: bid ok 5 537, bid rejected 2 545, bid unknown 91, create ok 57, create unknown 9, login ok 8 |
| unknown bids | 91, of which **9 committed** (exactly once each) and 82 never did |
| races | 1 050 race bids in 350 equal-amount groups; **no group had two winners** |
| auctions | 57 created, all 57 closed after the run; final `digest_index` 8 298 on all three nodes |
| bid latency under chaos | p50 47 ms, p95 319 ms, max 5.68 s (unknowns excluded) |

```
PASS  history-well-formed  (checked 8247)
PASS  acked-writes-on-every-node-exactly-once  (checked 16806)
PASS  replicas-identical  (checked 3)
PASS  bids-strictly-increasing-winner-is-max  (checked 171)
PASS  no-bid-accepted-after-close  (checked 22175)
PASS  rejections-consistent-with-final-state  (checked 2545)
PASS  unknown-outcome-at-most-once  (checked 273)
PASS  at-most-one-leader-per-term  (checked 94)
ALL PROPERTIES PASS
```

Rejection reasons: 1 230 "not added because current highest bid is N"
(including every losing racer), 484 "already closed", 831 "Auction ID does
not exist". The last one is the workload, not the system: the 9 creates with
unknown outcome were put on the bidders' board in case they had committed.
None had, so every bid on those ids was correctly refused. The run directory
was re-checked afterwards with the final checker
(`python -m checker runs/chaos-737075-...`): same result.

The demo (`scripts/demo.py --pause 0.3`, run `runs/demo-20261109-220454`):
equal bids of 200, karan won and tanmay was refused. Leader n1 was killed and
mrudul's next bid took 2.09 s through the failover to n2. n1 restarted and the
replicas agreed. The post-close bid was refused. All eight properties pass.

---

## 7. Known gaps

1. **A resumed follower deposes a healthy leader.** A follower suspended for
   longer than its election timeout comes back, its timer has expired, and it
   starts an election with term + 1. The leader sees the higher term and steps
   down (seed 5: n3 suspended, then `no leader (was n2, term 1)` the moment it
   resumed, new leader in term 3). That's correct Figure 2 behaviour and
   costs ~0.5-1 s of availability. The paper's §6 has the fix: *"if a server
   receives a RequestVote RPC within the minimum election timeout of hearing
   from a current leader, it does not update its term or grant its vote"*
   (PreVote, in Ongaro's thesis, is the other one). Not implemented: it's not in
   Figure 2, and this chunk doesn't touch `raft/`. If it hit today: an extra
   election after every slow-node episode, never a safety problem.
2. **The schedule is replayable, the run is not.** See 3.2. A failing run is
   analysed from its files, not re-run.
3. **"Sent before the close" can still lose.** `curr_time` is stamped by the
   leader that *receives* the attempt. A bid retried across a failover gets
   the new leader's later stamp, or reaches the log after `CloseAuction`, and
   is refused. That's safe (the checker passes: the bid really did arrive late)
   but a user could call it unfair. The fix would be to stamp at the first
   receipt and carry the stamp on retries, but that would let a client choose
   its own time. Not done.
4. **One machine.** Clients and servers share a clock, which T11 relies on.
   Nothing here tests clock skew, network partitions (all faults are process
   faults), or disk faults (FileStorage's torn-tail handling is unit-tested in
   step 6, not here).
5. **Leader sightings can miss a term.** A leader that lived less than one
   50 ms poll *and* whose log line was lost to a kill -9 would be invisible.
   Election Safety would then be checked on fewer sightings, never falsely
   failed.
6. **The LLM server.** It ran in every chaos run, but Ollama wasn't running on
   this machine, so every description call failed fast and degraded to `""`
   (A10, by design). The *path* "call the LLM before proposing, degrade on
   failure" is exercised. Real generated text is not.
7. **Status reads aren't atomic across Raft and the state machine** (§1
   caveat). That's fine for the dashboard. The checker relies only on quiet-time
   dumps.
8. **Bug found by stress, now fixed (T17): FileStorage died on a transient
   Windows `PermissionError`.** Seen in the failover stress run at 0.15-0.3 s
   timeouts: `os.replace(tmp, state.json)` was refused once ("Access is
   denied"), P10 fail-stopped the storage, and the node could never change
   term again. `set_term_and_vote` now retries the replace (10 ms doubling,
   ≤ 0.5 s), logging a WARNING each time, and fail-stops only if the refusal
   persists. Remaining gap: a lock held longer than 0.5 s still takes the
   node out, by design. Tests: `tests/test_storage_transient_replace.py`.
9. **Three harness bugs found by the stress runs, all fixed** (T14, T16):
   waiting for identical digests before each kill (never happens under
   writes); counting the old leader's last ack as "first write after
   failover"; and a writer thread that outlived its cluster and was
   (correctly) refused UNAUTHENTICATED by the next config's fresh cluster on
   the same ports. Plus the status RPC throttling the node it observed (T16).
10. **M1's `apply()` is O(bids) per PlaceBid** (`max(...)` over the
   auction's bid list, plus `_high_bid` in the state machine): 0.15 ms at
   1 000 bids, 2.85 ms at 25 000. Fine for real auctions. A single auction
   with tens of thousands of bids would cap throughput. That's why the stress
   writer starts a new auction every 1 000 bids (`BIDS_PER_AUCTION`,
   scripts/stress.py): without it, a 25-minute failover run measures this scan,
   not Raft. Not changed:
   `auction/state.py` is M1 code, and keeping the current max in the auction
   dict would change `apply()`'s logic.
11. **Catch-up after a long outage costs one RPC per missing entry (R6).**
   Plain Figure 2 backoff: `nextIndex -= 1` per refused AppendEntries, with no
   conflict-term or log-length hint. A follower that missed N entries is
   probed N times before replication resumes (each ~1 ms round trip on
   loopback, retried at once). Seen in the stress logs as runs of
   `n1 has no (index 22277, term 21): nextIndex -> 22277`, then 22276,
   22275, ... one per millisecond. Fine for seconds-long outages. A node down
   for hours would take as many round trips as entries it missed. §5.3's
   optimisation would fix it. Not done (R6: the handout's reply has no field
   for it, and the paper doubts it is needed).
12. **Client-observed failover is floored at ~2 s by Windows' TCP connect
   (T18).** After a kill, the client's next attempt goes to the remembered
   leader (`self._target`, client/auction_client.py:139). On Windows a
   connect to a localhost port with no listener takes 2.0 s to be refused
   (measured, WSAECONNREFUSED 10061). That dominates `t_write` in every
   config.
13. **Fixed ports inside Windows' ephemeral range.** 50081-50093 lie in
   49152-65535, so any outgoing connection on the machine can briefly hold
   one as its local port while a node is down, and the restart's bind then
   fails. `ensure_started` retries 5 times over ~5 s. In the 10-minute run
   all 71 heals succeeded. Don't run the test suite (which binds port 0)
   during a chaos run.
14. **Restart replays the whole log (P11, no snapshots), so recovery time
   grows with history.** In the failover stress run the restarted nodes'
   logs grew from 1 211 to 39 507 entries. Late in each run the cluster
   needed over 60 s to get every node within 50 entries again (14 such
   waits, stress-results.md). That this is the replay is inferred from the
   timing and log sizes; the replay rate wasn't instrumented. Log compaction
   (§7) is the fix, and it's out of scope for the course.
