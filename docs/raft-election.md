# Raft leader election: a walkthrough to study from

Scope: build-order step 3 only. Leader election and heartbeats. No log, no
replication, no persistence. Spec: Raft paper (Ongaro & Ousterhout), Figure 2
("State", "RequestVote RPC", "Rules for Servers", plus the heartbeat subset of
"AppendEntries RPC"), section 5.2, and Election Safety from Figure 3.

Line numbers refer to `raft/node.py` as of this chunk. If you edit the file
they drift; the function names are the stable handle.

Reasoning behind each non-obvious choice, with the viva question an examiner
would ask, is in [decisions.md](decisions.md) under "Election". Section 6 below
is the list of things this chunk knowingly gets wrong.

---

## 1. The invariant first

**Election Safety (Figure 3): at most one leader can be elected in a given
term.** Everything in the voting code exists to protect this. It follows from
two facts:

1. **A node grants at most one vote per term.** `votedFor` belongs to
   `currentTerm` and is cleared only when `currentTerm` goes up.
2. **A candidate needs a majority of the whole cluster.** Any two majorities of
   the same set share at least one member. That shared node voted once in that
   term, so it cannot have voted for both candidates.

So two candidates cannot both win the same term. (Only if fact 1 breaks, which
is what a restart without persistence can do. See Known gaps.)

**Liveness** (some leader does eventually appear) is a separate matter and is
only probabilistic. Randomised election timeouts make it unlikely that two
nodes time out together, so one usually finishes its election before the
others start. A split vote just means nobody wins that term; they all time out
again at different random moments and try term+1.

---

## 2. The pieces

| File | Role |
| --- | --- |
| `raft/node.py` | The whole algorithm. Imports no grpc. |
| `raft/messages.py` | `RequestVoteArgs/Reply`, `AppendEntriesArgs/Reply`, the `Transport` interface, `TransportError` |
| `raft/transport.py` | `GrpcTransport` (real), `InMemoryTransport` (tests: `drop`, `restore`, `partition`, `heal`) |
| `raft/wire.py` | dataclass <-> protobuf field copying |
| `raft/servicer.py` | gRPC adapter: wire in, `node.handle_*`, wire out. No logic. |
| `raft/run_node.py` | CLI, one process per node, logging |
| `proto/raft.proto` | the two RPCs, handout field names (`from` -> `from_id`) |

Threads inside one node:

- **ticker**: one thread, wakes every 5 ms, looks at the clock under the lock.
  Starts elections, decides when a heartbeat is due. Sends nothing itself.
- **RPC workers**: a small pool that actually calls `transport.request_vote` /
  `append_entries`, so peers are contacted in parallel and the lock is never
  held while waiting for a network reply.
- **handler threads**: whoever calls `handle_request_vote` /
  `handle_append_entries` (gRPC server threads, or the caller in tests).

All of them share one lock, `self._lock` (created at line 171).

---

## 3. Figure 2, rule by rule

### State

> **Persistent state on all servers:** `currentTerm` latest term server has seen
> (initialized to 0 on first boot, increases monotonically); `votedFor`
> candidateId that received vote in current term (or null if none)

`node.py:157-158`: `self._current_term = 0`, `self._voted_for = None`. In
memory only. The paper says these must survive a restart; that is step 6.

The invariant "`votedFor` belongs to `currentTerm`" is kept in exactly one
place, `_transition` (lines 240-262, the `term > old_term` branch at 249-252):
whenever the term increases, `_voted_for` is overwritten (blank, or the node's
own id if it is becoming a candidate). No other line changes `_current_term`.

I also track `leader_id` and `_votes` (who voted for me in this candidacy).
Those are volatile and are not in Figure 2's persistent state.

### Rules for Servers: All Servers

> If RPC request or response contains term T > currentTerm: set currentTerm = T,
> convert to follower (§5.1)

`_step_down_if_stale` (264-276). It calls `_transition(term, FOLLOWER, ...)`,
which sets the new term and clears the vote, and then forgets the leader (the
new term has none yet).

It is called on **every** path where a term arrives:

| Path | Line |
| --- | --- |
| `handle_request_vote` (incoming request) | 340 |
| `handle_append_entries` (incoming request) | 387 |
| `_on_vote_reply` (reply to my RequestVote) | 444 |
| `_send_heartbeat` (reply to my heartbeat) | 470 |

In the two handlers it runs **first**, before any other check, because the
checks that follow (`args.term < currentTerm`, `votedFor`) must see the updated
term and the cleared vote. Example: a node in term 3 that already voted gets a
RequestVote for term 5. It first moves to term 5 with a blank ballot, then
decides on the vote. Without that order it would wrongly answer "already voted".

(The other All Servers rule, `commitIndex > lastApplied`, is about the log: step 4.)

### Rules for Servers: Followers

> If election timeout elapses without receiving AppendEntries RPC from current
> leader or granting vote to candidate: convert to candidate

- *Elapses*: `_tick` (212-232). Line 222, `elif now >= self._election_deadline`,
  then `_start_election()` at 227. The deadline is a `time.monotonic()` value
  set by `_reset_election_timer` (234-236) to now + a random value in
  [min, max].
- *Receiving AppendEntries from the current leader*:
  `handle_append_entries` line 411, `self._reset_election_timer()`. It is only
  reached after the stale-term early return at 389-394, so only a **current-term
  leader** can reset the timer. A stale leader's heartbeat does not.
- *Granting vote to candidate*: `handle_request_vote` line 361, inside the grant
  branch only (357-362). A **refused** vote does not touch the timer. Otherwise a
  candidate with a stale log could keep resetting everyone's timer by spamming
  RequestVotes and no real election could ever start.

Those two lines (361 and 411), plus the one in `_start_election` (288), plus
`start()` and the leader-deposed case (explained in decisions.md), are the
**only** places the timer is reset.

### Rules for Servers: Candidates

> On conversion to candidate, start election: Increment currentTerm; Vote for
> self; Reset election timer; Send RequestVote RPCs to all other servers

`_start_election` (278-299):

- 283-285: `_transition(term + 1, CANDIDATE, vote=self.node_id)`. That is
  *increment currentTerm* and *vote for self* in one atomic step (we hold the
  lock). The event emitted already shows `voted_for == self`.
- 287: `_votes = {self}`.
- 288: reset election timer (a new random timeout, for the case this election
  fails).
- 292-299: build one `RequestVoteArgs` per peer and **return them**. They are
  sent by `_tick` after the lock is released (229-230), through `_submit` (421)
  onto the pool, so all peers are asked in parallel.

> If votes received from majority of servers: become leader

`_on_vote_reply` (441-453): count at 451-452, `len(self._votes) >= self._majority`
then `_become_leader` (301-310). `_majority` is `(peers + 1) // 2 + 1` (line 169),
so 2 of 3. Votes are a **set of peer ids**, so a duplicate reply cannot count twice.

Before counting, line 448 discards stale replies:
`if self._role != CANDIDATE or self._current_term != sent_term: return`. The
RPC was in flight with the lock released; this node may meanwhile have lost
the election, been deposed, or started a *newer* candidacy. A "yes" for an old
term must not count toward a new one.

> If AppendEntries RPC received from new leader: convert to follower

`handle_append_entries` 401-406, guarded by `self._role == CANDIDATE`. This is the
**equal-term** case: someone else won *my* term. (The higher-term case goes
through All Servers instead.)

> If election timeout elapses: start new election

Same `_tick` branch as for followers (222-227): a candidate's deadline was
re-armed in `_start_election`, and if it passes with no majority we run
`_start_election` again, term + 1.

### Rules for Servers: Leaders

> Upon election: send initial empty AppendEntries RPCs (heartbeat) to each
> server; repeat during idle periods to prevent election timeouts (§5.2)

`_become_leader` sets `_next_heartbeat = 0` (line 310) so that the very next
tick (at most 5 ms later) is "due". `_tick` lines 216-221 then calls
`_prepare_heartbeats` (312-323) and re-arms for `now + heartbeat_interval`.
Heartbeats therefore have exactly one origin, the ticker. The heartbeat carries
`prevIndex = prevTerm = commitIndex = 0` and no entries, because there is no
log.

`_hb_in_flight` (set at 319) skips a peer that still has an unanswered
heartbeat. Otherwise a dead peer with a 300 ms RPC timeout would pile up
three outstanding heartbeats at a 100 ms interval.

The heartbeat's **reply** goes through `_send_heartbeat` (455-470). The only
thing an election-only leader can learn from it is "you've been deposed"
(line 470, All Servers rule again).

### RequestVote RPC: receiver implementation

> 1. Reply false if term < currentTerm (§5.1)
> 2. If votedFor is null or candidateId, and candidate's log is at least as
>    up-to-date as receiver's log, grant vote (§5.2, §5.4)

`handle_request_vote` (335-369). After the All Servers step (340):

| Figure 2 clause | Line |
| --- | --- |
| 1. `term < currentTerm` -> false | 343 |
| 2a. `votedFor` is null or `candidateId` | 346 (`not in (None, args.from_id)`) |
| 2b. candidate's log at least as up-to-date | 349 -> `_candidate_log_ok` (371-379) |
| grant: record vote, reset timer | 359, 361 |

Each reply carries `self._current_term`, the term **after** step-down. So a
candidate with a stale term learns the real one from the refusal and steps
down on its own reply path (444).

*Why "or candidateId"*: the same candidate may retry the same request (a lost
reply); granting again is harmless and idempotent. A *different* candidate in
the same term is refused (the "at most one vote per term" test).

*The up-to-date check* (§5.4.1) is `_candidate_log_ok`, line 379:
`(args.last_log_term, args.last_log_index) >= (my_term, my_index)`. Python
compares tuples left to right, which is precisely "later last term wins; if
equal, longer log wins". With no log yet, `last_log` returns `(0, 0)` for
everyone, so the check always passes in a real cluster; the tests inject a
non-empty `last_log` to prove the rule works. Step 4 only has to supply the
real function.

### AppendEntries RPC: receiver implementation (heartbeat subset)

> 1. Reply false if term < currentTerm (§5.1)
> 2. Reply false if log doesn't contain an entry at prevLogIndex whose term
>    matches prevLogTerm (§5.3)

`handle_append_entries` (381-417): step-down 387; rule 1 at 389-394 (reply
false, **no** timer reset, **do not** adopt this sender as leader); then the
sender is the legitimate leader of this term: assertion 398 (Election Safety
says it can't be me), candidate -> follower 401-406, `leader_id` 407, timer
reset 411; rule 2 at 416 with an empty log: the only "entry" that exists is
index 0, so `ok = prev_index == 0 and prev_term == 0`. A sender claiming
`prev_index = 5` is refused, which is exactly what rule 2 says for an empty log.
Rules 3-5 (conflicting entries, append, advance commitIndex) have nothing to do
yet.

### §5.2: randomised election timeouts

`_reset_election_timer` (236) draws `rng.uniform(min, max)` every time.
Defaults are 0.5-1.0 s (lines 68-72); the reasoning and the alternatives are in
decisions.md. The RNG is injected (`rng=random.Random(seed)`) so tests are
repeatable.

---

## 4. The end-to-end trace

Real output: three `raft.run_node` processes over real gRPC on 127.0.0.1, seeds
11/22/33, defaults (election timeout 0.5-1.0 s, heartbeat 0.1 s). I killed the
leader's process, then started it again. Log lines trimmed of the startup banner.

```
01:10:21.055 [n1] term 1 FOLLOWER -> CANDIDATE (was term 0): election timeout
01:10:21.067 [n2] term 1 FOLLOWER -> FOLLOWER (was term 0): higher term 1 seen in RequestVote from n1
01:10:21.067 [n2] term 1: GRANTED vote for n1 (ok)
01:10:21.067 [n3] term 1 FOLLOWER -> FOLLOWER (was term 0): higher term 1 seen in RequestVote from n1
01:10:21.068 [n1] term 1 CANDIDATE -> LEADER: won election with 2 votes
01:10:21.068 [n3] term 1: GRANTED vote for n1 (ok)
   ... n1 is killed here; no log line, a killed process says nothing ...
01:10:24.446 [n3] term 2 FOLLOWER -> CANDIDATE (was term 1): election timeout
01:10:24.455 [n2] term 2 FOLLOWER -> FOLLOWER (was term 1): higher term 2 seen in RequestVote from n3
01:10:24.455 [n2] term 2: GRANTED vote for n3 (ok)
01:10:24.456 [n3] term 2 CANDIDATE -> LEADER: won election with 2 votes
   ... n1 restarted ...
01:10:28.682 [n1] term 2 FOLLOWER -> FOLLOWER (was term 0): higher term 2 seen in AppendEntries from n3
```

Step by step, with the state of each node as `(term, votedFor, role)`:

**0. Boot.** All three: `(0, none, FOLLOWER)`. Each armed a random deadline
0.5-1.0 s away (`start()`, line 180). Nobody is leader; nobody hears heartbeats.

**1. n1's deadline passes first** (21.055; its random draw was the smallest).
`_tick` -> `_start_election` (283-285). n1 = `(1, n1, CANDIDATE)`. It holds 1
vote of the 3 it needs 2 of. It sends `RequestVote(term=1, lastLogIndex=0,
lastLogTerm=0)` to n2 and n3 **in parallel**, with its lock released.

**2. n2 and n3 handle the request** (21.067). Each is `(0, none, FOLLOWER)`,
sees term 1 > 0 and steps down with a blank ballot: `(1, none, FOLLOWER)`
(that is the "FOLLOWER -> FOLLOWER (was term 0)" line: the term changed, the
role didn't). Then: term not stale, `votedFor` is null, log check
`(0,0) >= (0,0)` passes -> **granted**, `votedFor = n1`, timer reset. Now
n2 = n3 = `(1, n1, FOLLOWER)`. Reply: `term=1, voteGranted=true`.

**3. n1 counts.** Whichever grant arrives first (n2's or n3's; the log can't
say which) makes `_votes` two ids, 2 >= 2 -> leader (21.068): `(1, n1, LEADER)`.
The other "yes" arrives at a node that is already leader and is dropped by the
role check at line 448. It would not have mattered.

**4. Heartbeats.** Every ~100 ms n1 sends `AppendEntries(term=1, prev=0, no
entries)`. n2 and n3: term equal, so the sender is the leader; `leader_id = n1`;
timer reset (411). They never time out while this continues.

**5. n1 dies.** No more heartbeats. n2 and n3 still hold `(1, n1, FOLLOWER)` and
their deadlines were last reset by n1's final heartbeat, so each fires
0.5-1.0 s after it.

**6. n3 fires first** (24.446): `(2, n3, CANDIDATE)`. It sends RequestVote(term=2)
to n1 and n2. The one to n1 fails after a connect error (`TransportError`,
logged at DEBUG only, ignored). n3 has 1 vote.

**7. n2 handles it** (24.455). n2 was `(1, n1, FOLLOWER)`: term 2 > 1, so
step down and clear the vote: `(2, none, FOLLOWER)`. Then `votedFor` is null,
logs equal -> granted: `(2, n3, FOLLOWER)`. **Note what made this legal:**
n2 *had* voted in term 1, for n1, and that is now irrelevant, because the
vote belongs to term 1. In term 2 it has a fresh vote.

**8. n3 counts**: `{n3, n2}` = 2 -> `(2, n3, LEADER)` (24.456). Term 2 > term 1,
strictly higher than the dead leader's. Heartbeats resume to n2 (and silently
fail to n1).

**9. n1 comes back** as a brand new process: `(0, none, FOLLOWER)`. It has
**forgotten** that it was leader of term 1. n3's next heartbeat has term 2 > 0:
All Servers rule -> `(2, none, FOLLOWER)`, then the equal-term path sets
`leader_id = n3` and resets the timer (28.682). It is a follower again, not
disturbing anything. That it came back with term 0 is the persistence gap, and
it happened to be harmless here (see Known gaps for when it isn't).

---

## 5. How to see it yourself

Tests (no sockets, a few seconds each):

```powershell
.venv\Scripts\python.exe -m pytest tests\test_raft_election.py -v
```

Live, with real processes, kill the leader by closing its window:

```powershell
.\scripts\run_cluster.ps1            # 3 windows, ports 50061-50063
.\scripts\run_cluster.ps1 -DryRun    # just print the three command lines
```

---

## 6. Known gaps

Things this chunk knowingly does not do. Each is a later build-order step or a
deliberate scope cut. None is fixed here.

1. **`currentTerm` and `votedFor` are in memory only (step 6).**
   Figure 2 marks them *persistent*, "updated on stable storage before
   responding to RPCs". Here a restart resets them to `(0, none)`. What
   goes wrong, concretely (3 nodes n1, n2, n3):
   - n1 becomes a candidate in term 5; n2 grants it (`votedFor = n1`).
   - n2 crashes **before n1's tally finishes** and restarts as `(0, none)`.
     It has forgotten it voted in term 5.
   - n3 also runs in term 5 (it timed out too). n2 sees term 5 > 0, adopts it,
     `votedFor` is null, and grants **again**.
   - n1 has {n1, n2}, n3 has {n3, n2}: two majorities, two leaders in term 5.
     **Election Safety is broken**, because fact 1 in section 1 (one vote per
     term) was destroyed by the restart.
   The same restart also lets a node accept a *lower* term than one it already
   promised to, since its `currentTerm` regressed. We saw a mild form of this
   in the trace: n1 came back at term 0.
   Fix (step 6): write `currentTerm` and `votedFor` to disk and fsync *before*
   sending the reply that depends on them.

2. **No log, so the up-to-date check is untested against real logs.**
   `last_log` is injected and defaults to `(0, 0)`; the rule itself is proven
   by unit tests only. `AppendEntries` rule 2 is the trivial empty-log version,
   `entries` is never converted on the wire, `matchIndex` is always 0 (steps 4-5).

3. **No CheckQuorum / pre-vote.** A leader cut off from the majority keeps
   believing it leads (a test asserts exactly that) and keeps its term; an
   isolated follower keeps incrementing its term, and when it rejoins its
   inflated term deposes a healthy leader (Ongaro's thesis, §9.6, fixes both).
   Safety is unaffected (a deposed leader is never *wrong*, just disruptive);
   availability takes a hit.

4. **Timeouts are reasoned, not measured.** The 0.5-1.0 s / 0.1 s defaults are
   derived in decisions.md, not benchmarked under load. A stall of more than about
   0.5 s in a leader's process (longer than the shortest election timeout)
   would cause a spurious election (harmless to safety, bad for availability).

5. **The in-memory network is too polite.** Synchronous, instant, no delay,
   reordering, duplication or message loss other than whole-node drops and
   partitions. The step-7 chaos harness has to extend it.

6. **Tests use the real clock.** Bounds are generous and the suite was repeated
   (12 clean runs, one under heavy CPU load), but there is no injectable clock
   to make timing tests fully deterministic.

7. **No authentication or TLS** on the Raft port: any local process can send a
   RequestVote. Local-only bind (`127.0.0.1`) is the only mitigation.

8. **Handlers don't validate `args.to`** against the node's own id; routing is
   trusted to the transport.

9. **No membership changes**, and the peer set is fixed at startup (§6 of the paper).
