# Decisions

Non-obvious choices, each with the alternatives, the reason, the paper
reference, and the question an examiner would ask. (Older, pre-Raft decisions
live in [design-notes.md](design-notes.md).)

Format of every entry:
**Decision** / **Alternatives** / **Why this one** / **Reference** /
**Viva question** with a short answer.

---

## Election

Code: `raft/`. Walkthrough: [raft-election.md](raft-election.md). Known gaps:
section 6 of that file.

### E1. Timing values: election timeout 0.5-1.0 s, heartbeat 0.1 s

**Decision.** `DEFAULT_ELECTION_TIMEOUT = (0.5, 1.0)`,
`DEFAULT_HEARTBEAT_INTERVAL = 0.1` (`raft/node.py:68-69`). Ticker resolution
5 ms. gRPC per-RPC deadline 0.3 s (`raft/transport.py`).

**Alternatives.** The paper's example 150-300 ms election timeout with a
~50 ms heartbeat; or 1-2 s for extra safety.

**Why this one.** Section 5.6 requires `broadcastTime << electionTimeout <<
MTBF`. Broadcast time here is ~1 ms on localhost, but a Python process on
Windows can stall for tens of milliseconds (GIL contention, the 15.6 ms
timer tick, console writes), so the paper's numbers leave too thin a margin and
a stall would cause needless elections. At 0.5 s minimum the shortest timeout
spans five heartbeat periods: five heartbeats in a row would have to be
late or lost to depose a healthy leader. The window is 0.5 s wide, hundreds of
times a vote round trip, so two nodes landing within one RTT of each other
(a split vote) is rare. Failover after killing a leader is at most about a
second, which is fine to watch in the demo. The RPC deadline (0.3 s) is
below the minimum election timeout so a vote round's stragglers are finished
or failed before the same node can start the next round.

**Reference.** Paper §5.2 (randomised timeouts), §5.6 (timing requirement).

**Viva: "Why not the paper's 150-300 ms?"** Because that assumes a broadcast time
far below the timeout, and a Python/gRPC process on a desktop OS has multi-ten-ms
scheduling jitter. Safety never depends on timing; only availability does,
and I traded a slightly slower failover for not deposing healthy leaders.

### E2. `time.monotonic()` for every timer, never wall-clock

**Decision.** Deadlines are `time.monotonic()` values (`node.py:213, 236`).

**Alternatives.** `time.time()`; `datetime.now()`.

**Why this one.** The wall clock can jump (NTP step, manual change, DST on
some setups). A backwards jump would extend a timeout indefinitely; a forward
jump would trigger an immediate election. A monotonic clock only measures
elapsed time, which is all a timeout means. Raft requires no clock
synchronisation between nodes, and we don't even compare readings across
nodes.

**Reference.** Paper §5.6: safety must not depend on timing; only availability does.
(Also CLAUDE.md: `apply()` has no clocks. Election uses them, `apply()` doesn't.)

**Viva: "Do the nodes need synchronised clocks?"** No. Each node only measures
its own elapsed time since it last heard from a leader. Terms, not timestamps,
order events across nodes.

### E3. One lock, never held during an outgoing RPC

**Decision.** `self._lock` guards all mutable node state. RPCs are *prepared*
under the lock as plain argument objects (`_start_election` returns them,
`_prepare_heartbeats` returns them), and *sent* after it is released
(`_tick`, lines 228-232). Replies re-take the lock (`_on_vote_reply`,
`_send_heartbeat`).

**Alternatives.** Hold the lock for the whole election, including the RPCs
(simplest). Fine-grained locks per field. An actor/event-loop model (single
thread, no locks).

**Why this one.** Holding a lock across a network call means that while peer
B is slow or dead (a 300 ms timeout), this node cannot answer anyone else's
RequestVote or heartbeat. Two nodes each holding their lock while calling the
other deadlock until the RPC deadlines expire. A single coarse lock is simple
enough to reason about: every method either holds it for the whole decision or
doesn't touch state. The cost is that after the lock is released the world can
change, which is why every reply path re-checks (E4).

**Reference.** Figure 2 specifies a state machine, not a threading model; the
rules are atomic per event, and "take the lock for the event" gives exactly that.

**Viva: "What's the deadlock risk if you hold the lock during a call?"** A calls B
while holding A's lock; B calls A while holding B's lock. Each handler waits
for the other's lock. With a lock-free network phase this cannot happen.

### E4. Re-check term and role after every reply

**Decision.** `_on_vote_reply` (441-453) first applies the higher-term rule,
then discards the reply unless `role == CANDIDATE and currentTerm == sent_term`.
`_send_heartbeat` applies the higher-term rule to its reply.

**Alternatives.** Trust that a reply matches current state (wrong); cancel
in-flight RPCs on state change (complicated, still racy).

**Why this one.** While the lock is released, this node may have granted its
vote elsewhere, been deposed, or started candidacy for term+1. A "yes" for term 3
that arrives during term 4 must not count toward term 4. The check is two
comparisons and is exactly what the paper means by processing each reply as an
event in the *current* state.

**Reference.** Paper §5.2; Figure 2 "All Servers".

**Viva: "A vote arrives late. What happens?"** If the term changed, it's ignored by
the `current_term != sent_term` test. If the node has meanwhile become a
follower/leader, the role test ignores it. If the reply carries a higher term, we
step down first.

### E5. Parallel sends via a worker pool; one heartbeat in flight per peer

**Decision.** RequestVote and heartbeat RPCs are submitted to a
`ThreadPoolExecutor` (`node.py:181`), one task per peer.
`_hb_in_flight` suppresses a new heartbeat to a peer whose previous one has not
returned.

**Alternatives.** Send sequentially in the ticker thread; one new
`threading.Thread` per RPC; asyncio.

**Why this one.** Sequential sends mean one dead peer (a 300 ms timeout) delays
the RPC to the next peer, and the ticker misses its own deadlines. A thread per
RPC grows without bound when a peer is down (heartbeats every 100 ms, each
lasting 300 ms). A fixed pool plus the in-flight guard bounds both threads and
queue length. Only heartbeats are guarded; vote rounds are already spaced by
the election timeout (> RPC deadline), so they cannot pile up. A lost heartbeat
needs no retry: the next one is 100 ms away and AppendEntries is idempotent.

**Reference.** Paper §5.2: "issues RequestVote RPCs in parallel".

**Viva: "Why in parallel?"** The paper says so for a reason: a candidate must collect
a majority before its own timer runs out. If it asks sequentially and the first
peer is dead, the second vote is delayed by the dead peer's timeout.

### E6. Transport split: the node has no gRPC in it

**Decision.** `raft/messages.py` defines plain dataclasses, `TransportError`
and the `Transport` ABC (two methods). `raft/node.py` imports only that.
`GrpcTransport` and `InMemoryTransport` implement it in `raft/transport.py`;
`raft/servicer.py` is the inbound adapter. A test runs `import raft.node` in a
subprocess and fails if `grpc` or `generated` shows up in `sys.modules`.

**Alternatives.** Node calls generated stubs directly; the node takes
protobuf messages in and out.

**Why this one.** Election logic is the part I must defend line by line; it
should be readable without protobuf or channels in view, and testable with no
sockets (no ports, no flaky teardown, the partition/drop of a test is a Python
`set`). The same `InMemoryTransport` is what the step-7 chaos harness will drive.
The price is one conversion module (`raft/wire.py`).

**Reference.** None: engineering decision.

**Viva: "Why abstract the network?"** So the algorithm can be tested for adversarial
network behaviour deterministically, and so a bug in the algorithm can't hide
in transport code (or the reverse).

### E7. Field names: `from` -> `from_id`, everything else as in the handout

**Decision.** `proto/raft.proto` uses the handout's names verbatim (camelCase:
`lastLogIndex`, `voteGranted`, `entryAppended`, `matchIndex`, ...) except
`from`, which is `from_id`. `to` is kept.

**Alternatives.** `from_` ; `sender`/`receiver`; rename both to `from_id`/`to_id`
for symmetry; snake_case throughout to match auction.proto.

**Why this one.** `from` is a Python keyword: the generated message has the
attribute but `RequestVoteRequest(from=...)` is a syntax error and reading it
needs `getattr`. Changing only what is forced keeps the mapping to the
handout one-to-one, so a reviewer comparing handout and proto sees exactly one
difference. I kept the handout's camelCase over this repo's snake_case because
the instruction was "match the handout as closely as proto allows", and proto
allows camelCase. The Python-side dataclasses (`messages.py`) use snake_case;
`wire.py` is the single place that maps between the two.

**Reference.** Course handout signatures.

**Viva: "Why `from_id` but `to`?"** Only `from` is reserved in Python. I changed
the minimum. The cost is mild asymmetry, recorded here.

### E8. Where the timer lives: a deadline checked by one ticker thread

**Decision.** The node owns a single `_election_deadline` (monotonic) and a
single ticker thread that wakes every 5 ms and compares (`_run_ticker`, `_tick`).
The same thread decides when a heartbeat is due.

**Alternatives.** A `threading.Timer` created and cancelled on every reset; a
`Condition.wait(timeout)` that is notified on each reset; timers owned by the
transport/servicer.

**Why this one.** Resetting the timer happens on *every heartbeat*, ~10 times a
second. With `threading.Timer` each reset is cancel + create a thread, and a
cancelled timer that has already fired races with the reset. With a deadline
variable a reset is one assignment under the lock and there is nothing to
cancel, so no race. Polling at 5 ms costs ~200 trivial wakeups/s and adds at most
5 ms (1% of the minimum timeout) to a timeout. The timer lives *inside* the node
(not the servicer) because "timer expired" is a Raft rule, and keeping the
clock next to the state it reads avoids a callback crossing the lock boundary.

**Reference.** Figure 2 Followers / Candidates / Leaders.

**Viva: "What if the timer fires just as a heartbeat arrives?"** Both take the same lock.
Whichever gets it first wins: either the heartbeat resets the deadline and the tick
sees time remaining, or the tick starts an election for term+1 and the heartbeat
(carrying the old term) is then rejected as stale. Both outcomes are legal Raft.

### E9. When the election timer is reset (and the one extra case)

**Decision.** The timer is reset only (a) on granting a vote
(`node.py:361`, Figure 2 Followers: "granting vote to candidate"), (b) on an
AppendEntries from a current-term leader (`:411`, "receiving AppendEntries RPC
from current leader"), (c) on conversion to candidate (`:288`, Candidates:
"Reset election timer"), (d) at `start()`. And one more: (e) when a **leader
steps down** (`_transition`, line 256-260).

**Alternatives for (e).** Reset on every step-down (what etcd does); never,
and handle the stale deadline specially.

**Why this one.** A leader never uses the election timer, so its deadline sits in
the past. If it were deposed and the timer left alone, the node would become a
follower and start an election on the next 5 ms tick, challenging the leader that
just deposed it. (e) arms a fresh timer. I deliberately did *not* reset on other
step-downs: a candidate/follower that sees a higher term in a request it then
*refuses* (stale log) must not have its timer extended, otherwise a candidate
with a stale log could keep every node's timer reset by spamming RequestVotes.
That is why a refused vote and a stale heartbeat reset nothing.

**Reference.** Figure 2, Rules for Servers (Followers, Candidates).

**Viva: "Which events reset the election timer, and why only those?"** Granting a vote and
hearing from the current leader: both are evidence that an election is
making progress, so waiting longer is right. Anything else (a refused vote, a
stale leader, a rival's higher term I didn't vote for) is not evidence of a live
leader.

### E10. Every change of term/role goes through `_transition`

**Decision.** `_transition(term, role, reason, vote)` (`node.py:240`) is the only
place `_current_term` or `_role` are assigned, and it carries the rule "when the
term increases, votedFor is blanked".

**Alternatives.** Assign the fields where the paper's rule mentions them.

**Why this one.** The Election Safety argument depends on "votedFor belongs
to currentTerm". With one write site for the term, that can't be violated by
forgetting to clear the vote in some handler; it also gives one place to log
(`RoleChange` event) and to reject a backwards term (`AssertionError`).

**Reference.** Figure 2 State; Figure 3 Election Safety.

**Viva: "How do you know votedFor is never stale?"** Because the term cannot be
assigned anywhere except the line that overwrites votedFor in the same
statement block, under the lock.

### E11. Observer called under the lock; logging goes through a queue

**Decision.** `RaftNode` calls its `observer(RoleChange)` while holding the
lock; the contract is "quick, non-blocking, never calls back into the node".
`run_node.py` logs through `logging.handlers.QueueHandler`, so the call is just
an enqueue; a `QueueListener` thread writes to the console.

**Alternatives.** Call the observer after releasing the lock (events could be
delivered out of order across threads); log directly to the console; a
dedicated event thread inside the node.

**Why this one.** Delivery under the lock guarantees the observer sees changes
in exactly the order they happened, which the Election Safety test depends on (it
records every leader per term). Logging directly would put a console write inside
the lock, and a Windows console window stops accepting writes while the user is
selecting text in it: one click would freeze the node, which looks exactly like
a crash. The queue removes that.

**Reference.** None.

**Viva: "You call user code under a lock. Isn't that a deadlock risk?"** Only if the
callback re-enters the node. The contract forbids that, and the two observers
(the logger's enqueue, a test's list append) can't.

### E12. The up-to-date check exists now, via an injected `last_log`

**Decision.** `_candidate_log_ok` compares
`(lastLogTerm, lastLogIndex) >= (myTerm, myIndex)` (`node.py:379`). `last_log`
is a constructor argument returning `(index, term)`, default `(0, 0)`.

**Alternatives.** Leave it out until step 4 ("no log yet"); always grant.

**Why this one.** The user asked for step 4 not to have to revisit voting. The
only thing step 4 changes is what `last_log` returns. Lexicographic tuple
comparison is exactly §5.4.1: "different terms: later term wins; same term:
longer log wins". The rule is tested now with an injected log.

**Reference.** §5.4.1; Figure 2 RequestVote receiver impl. 2.

**Viva: "Why compare term before length?"** A longer log from an old term can hold
uncommitted entries that a newer leader overwrote; the latest *term* is the
better evidence of having seen committed entries. Length only breaks ties.

### E13. Heartbeats are sent by the next tick, not by the winning reply thread

**Decision.** `_become_leader` sets `_next_heartbeat = 0`; the ticker sends the
first heartbeats within 5 ms (`node.py:310`, `_tick` 216-221).

**Alternatives.** Send them directly from `_on_vote_reply` on election.

**Why this one.** One sender of heartbeats means one place to rate-limit, to
guard against overlap (`_hb_in_flight`) and to read when debugging. The 5 ms delay
is 1% of the shortest election timeout, so no follower can time out because of it.

**Reference.** Figure 2 Leaders: "Upon election: send initial empty
AppendEntries RPCs".

**Viva: "Is the 'upon election' heartbeat sent immediately?"** Within one tick, 5 ms;
tiny next to a 500 ms timeout. Equivalent from a follower's point of view.

### E14. Replying with the post-update term; `entryAppended` for a heartbeat

**Decision.** Every reply carries `currentTerm` *after* the All Servers step-down.
For AppendEntries, `entryAppended` is true when the sender is a current-term leader
and `prevIndex == prevTerm == 0` (rule 2 evaluated against an empty log);
`matchIndex` is 0.

**Alternatives.** Echo the request's term; always return true to a valid heartbeat.

**Why this one.** A stale candidate or leader learns the real term from the
reply and steps down (E4). For `entryAppended`, evaluating the actual rule on an
empty log means a heartbeat claiming a log we don't have is refused, so the code
is already correct for step 4's first change.

**Reference.** Figure 2 RequestVote/AppendEntries "Results: term, ...".

**Viva: "What does `entryAppended = true` mean for a heartbeat?"** "I accept you
as my leader and your `prev` entry matches mine": nothing was appended
because nothing was sent.

### E15. Re-granting a vote to the same candidate

**Decision.** `votedFor` in `(None, candidateId)` (`node.py:346`).

**Why this one.** Figure 2 says "votedFor is null *or candidateId*". A retried
request after a lost reply must get the same answer; refusing would let a lost
reply cost a candidate the vote. It cannot break Election Safety: we never grant
a *different* candidate.

**Viva: "Doesn't that allow two votes in a term?"** Two *grants*, to the same
candidate; the vote count is a set of voter ids, so it counts once.

### E16. `LogEntry` placeholder in `raft.proto`

**Decision.** `LogEntry{term, command bytes}` is declared so `entries` has a
type; nothing sends one. `wire.append_args_to_wire` ignores `entries`.

**Alternatives.** Omit `entries` until step 4 (breaks the handout's signature);
`repeated bytes`.

**Why this one.** The handout signature includes `entries`. Proto3 lets step 4
add or change fields compatibly. `command` is opaque so the Raft layer stays
ignorant of auctions (CLAUDE.md: LLM text enters the log only as plain data).

**Viva: "Why is `command` bytes?"** Raft replicates opaque commands; `apply()`
interprets them. The consensus layer must not depend on the application.

### E17. `InMemoryTransport`: one shared object, whole-node drops, symmetric partitions

**Decision.** One instance is shared by all test nodes (`register(node)`).
`drop(id)` / `restore(id)` make a node unreachable in both directions without
stopping it; `partition(*groups)` / `heal()` split the network. Reachability is
checked on the request **and again on the reply**. Nodes not named in a
partition are isolated.

**Alternatives.** One transport per node; drop only requests; stop the node object
to simulate a crash.

**Why this one.** `drop` leaves the Python object running, which is what a
crashed machine looks like to everyone else but also lets a test observe
that a cut-off leader still believes it leads (and steps down on heal). Checking
the reply path models "request arrived, reply lost", the case that tests the
"vote counted or not" logic. Shared instance: the sender id is on every message
(`from_id`), so reachability needs no per-node handle.

**Reference.** Paper §5.2, network partitions.

**Viva: "How do you simulate a crashed leader?"** Make it unreachable in both
directions; the others never hear from it again, which is the only thing a crash
looks like from outside.

### E18. gRPC channel options and addressing

**Decision.** Reconnect backoff capped at 100-500 ms
(`transport.py:_CHANNEL_OPTIONS`); processes bind and dial `127.0.0.1`;
per-RPC timeout 0.3 s.

**Alternatives.** gRPC defaults (backoff up to 120 s); `localhost`; `[::]`.

**Why this one.** After a peer is killed, gRPC backs off reconnecting
exponentially. With defaults a restarted node would wait for the backoff before
heartbeats reach it again: far beyond any election timeout. `localhost` can
resolve IPv6 first on Windows and add a fallback delay on every new connection;
the literal address avoids that. Local-only binding matches the repo's "keep it
local-only" stance (there is no auth on this port).

**Viva: "A node restarts. When does the leader reach it again?"** Within the
reconnect backoff (<= 0.5 s) plus a heartbeat; with defaults it could be minutes.

### E19. Election Safety is checked by observing every leader, over many seeds

**Decision.** `test_election_safety_never_two_leaders_in_one_term` is
parametrised over 15 seeds. The test's observer records `{term: {leader ids}}`
for **every** transition into LEADER, with aggressive timeouts (60-120 ms) and
two rounds of leader isolation per seed, and asserts each set has one element.

**Alternatives.** Sample the cluster's state at the end (misses transient double
leaders); one seed.

**Why this one.** The property is about *all* history: two leaders in one term
that later disappear would still be a violation. Seeded RNGs vary timeout
draws; OS thread scheduling still varies, and that is by design: the
invariant must hold for every interleaving. Aggressive timeouts make
split votes and spurious elections frequent, which is where a bug would show up.

**Reference.** Figure 3, Election Safety.

**Viva: "You ran it 15 times and it passed. Is that a proof?"** No. It is
evidence from many random schedules. The proof is the argument in
raft-election.md §1; the test would catch implementation mistakes that
break the premises of that argument (a vote granted twice, a term not
advancing).

### E20. An `assert` guards "two leaders in one term" in the handler

**Decision.** `handle_append_entries` asserts that a current-term AppendEntries
never reaches a node that is itself LEADER in that term (`node.py:398`).

**Why this one.** Election Safety says it is impossible. If a bug ever made it
possible, a loud failure beats silently stepping down and hiding it. The
test-side observer is the real detector; this is the in-process tripwire.

**Viva: "What if it fires in production?"** Then Election Safety is broken
(most plausibly a lost `votedFor` after a restart, the persistence gap); crashing
is the right response.

---

## Replication and repair

Code: `raft/node.py`, `raft/storage.py`, `raft/proposal.py`. Walkthrough:
[raft-replication.md](raft-replication.md). Known gaps: section 7 of that file.
These entries use CLAUDE.md's six-field template.

### R1. Entry payload is opaque bytes
- Chose:        `LogEntry(term: int, data: bytes, kind=COMMAND)` (`raft/messages.py`).
                Raft copies, stores and ships `data` and never parses it.
                `propose()` refuses anything that is not `bytes`. The only thing
                Raft reads on an entry besides its term is `kind`, its own
                marker for no-ops (R9), never anything inside `data`.
- Alternatives: store the auction's `Command` dataclasses directly; store a dict or
                JSON and let Raft look at a "type" field.
- Why:          The log holds *attempts*; `apply()` decides *outcomes*. A bid that
                arrives after the auction closed is still a valid log entry: it is
                committed and replicated like any other, and `apply()` rejects it
                the same way on every node. If Raft understood commands it would be
                tempted to judge them, and judging needs state only the state
                machine has. Bytes also force the encoding question (CLAUDE.md
                seam: `datetime` fields have no wire form) to be settled once, in
                the auction layer. Persistence (step 6) needs that encoding anyway.
- Paper:        Figure 2, State: "each entry contains command for state machine";
                §5.3. The proto field is called `command` (Figure 2's word), and
                the Python field `data`.
- Viva Q:       "How does Raft know a bid is valid before committing it?"
- Answer:       It doesn't, and it shouldn't. Raft only agrees on the *order* of
                commands. The deterministic `apply()` decides validity, and every
                node gets the same verdict because every node sees the same log.

### R2. `apply_fn` is called outside the node lock, by one applier thread
- Chose:        A dedicated applier thread waits on a `Condition` tied to the node
                lock. Under the lock it copies the newly committed entries. With
                the lock released, it calls `apply_fn(index, data)` for each, in
                order. Then, under the lock again, it sets `lastApplied = index`
                and resolves that index's handle (`_run_applier`).
- Alternatives: call `apply_fn` inline, under the lock, in whatever handler or reply
                thread advanced `commitIndex`; do the same but release the lock
                first; one thread per entry.
- Why:          `apply_fn` is user code with its own lock (`AuctionState`). Calling
                it under the node lock (a) hands lock ordering to the user (node
                lock then auction lock; any path taking them the other way
                deadlocks), (b) blocks heartbeats and votes for as long as
                `apply()` runs, and (c) deadlocks outright if `apply_fn` calls back
                into the node, because the lock is not reentrant. Applying from
                whichever thread advanced `commitIndex` (even unlocked) would let
                two threads apply at once, out of order. A single thread is the
                simplest way to get "in index order, exactly once". Copying the
                entries out first is safe because committed entries never change.
                A test makes `apply_fn` call `status()`; it would deadlock if this
                were wrong.
- Paper:        Figure 2, All Servers: "If commitIndex > lastApplied: increment
                lastApplied, apply log[lastApplied] to state machine (§5.3)".
- Viva Q:       "`lastApplied` is updated after `apply_fn` returns. What if the node
                crashes in between?"
- Answer:       Then it has applied an entry that `lastApplied` doesn't count. Today
                that's harmless: a restart loses the state machine too, and the node
                replays from index 1. With persistence, `lastApplied` and the state
                machine have to be saved together, or the state machine rebuilt
                from the log at boot. That's a step 6 decision (gap 2).

### R3. An exception in `apply_fn` halts this node's applier (fail-stop)
- Chose:        Log at CRITICAL and stop the applier thread. `lastApplied` stays
                below the failed index, and pending handles stay pending until the
                node stops (STOPPED). Tested.
- Alternatives: log and skip the entry; retry it; crash the process.
- Why:          Skipping would quietly make this replica differ from the others
                while it keeps serving, which a replicated state machine must never
                do. Retrying might apply half an entry twice. Stopping is loud and
                leaves the other replicas correct. `apply()` is meant to *return*
                rejections (bid too low, auction closed), not raise them, so an
                exception means a bug. The node doesn't crash the whole process so
                the in-memory tests can still observe what happened.
- Paper:        none (the paper assumes a correct state machine).
- Viva Q:       "One node's apply raises. What happens to the cluster?"
- Answer:       That node stops applying. It still votes and replicates, so the
                cluster keeps committing. If the bug is deterministic, every node
                halts at the same index, which leaves the same state everywhere.

### R4. What `propose()`'s success means
- Chose:        `propose(data)` returns a `Proposal` handle at once. The outcomes
                are defined in `raft/proposal.py`:
                - **APPLIED**: the entry with exactly this `(index, term)` was
                  committed and the leader's `apply_fn` returned; `result` is its
                  return value.
                - **NOT_LEADER**: refused at once, nothing appended, `leader_hint`
                  given.
                - **LEADERSHIP_LOST** / **STOPPED**: the outcome is *unknown*.
                - **OVERWRITTEN**: an entry with a different term was applied at
                  that index.
                A handle resolves once and never changes.
- Alternatives: resolve on *commit* instead of on apply; block inside `propose()`;
                when leadership is lost, keep the handle and settle it later when
                the node (now a follower) applies that index.
- Why:          Figure 2 says to "respond after entry applied to state machine", and
                apply is when there is a result to return (the high bid after a
                `PlaceBid`). It is also the strongest claim that is always true.
                APPLIED means a majority stored the entry under the current-term
                rule, so by Leader Completeness and State Machine Safety every node
                that applies that index applies this entry. The **term** matters as
                much as the index: matching on index alone would accept a
                *different* entry that landed in the same slot after a leader
                change. On leadership loss the handle fails instead of waiting,
                because the spec asks for that and because an ex-leader can't
                promise anything any more. The price is that LEADERSHIP_LOST is an
                honest "don't know" (the next leader may still commit the entry),
                so a client that retries can create a duplicate. The OVERWRITTEN
                branch is defence in depth and cannot trigger today: handles only
                exist on a leader, a leader never overwrites its own log, and
                handles are failed when it steps down.
- Paper:        Figure 2, Leaders: "If command received from client: append entry to
                local log, respond after entry applied to state machine (§5.3)";
                §8 (client interaction, duplicate detection).
- Viva Q:       "Your handle failed. Was the bid placed?"
- Answer:       Maybe. Only APPLIED means yes. Only NOT_LEADER and OVERWRITTEN mean
                no. LEADERSHIP_LOST is like a timeout. The property checker will
                only check acknowledged (APPLIED) entries. Exactly-once retries
                would need client ids and sequence numbers (§8), which we don't
                have yet (gap 4).

### R5. The persistence seam: storage holds the state, and the node keeps no copy
- Chose:        `MemoryStorage` (`raft/storage.py`) holds currentTerm, votedFor and
                log[]. Its three write methods (`set_term_and_vote`, `append`,
                `truncate_from`) are the only way to change them. The node reads
                term and vote through read-only properties, so
                `self._current_term = 5` raises `AttributeError` (tested).
- Alternatives: keep the fields in the node and *also* call a `persist()` hook after
                each change; call one `save_all()` after every handler.
- Why:          Two copies can drift apart. If one write path forgets the hook,
                step 6 silently fails to make that write durable. With a single
                copy, the code enforces "every write goes through the seam"; it
                doesn't rely on discipline. Term and vote are written together
                because a vote only means something with the term it was cast in;
                separate writes would leave a crash window holding a new term and
                an old vote. Step 6 makes each method durable before it returns.
                Every handler builds its reply after these calls, which is exactly
                "updated on stable storage before responding to RPCs".
- Paper:        Figure 2, State, "Persistent state on all servers (Updated on stable
                storage before responding to RPCs)".
- Viva Q:       "Where would you add the fsync?"
- Answer:       Inside those three methods, before they return. Nothing in
                `node.py` has to change, because every handler already returns its
                reply only after the write calls.

### R6. Plain nextIndex backoff, no conflict-term optimisation
- Chose:        On a consistency-check refusal, `nextIndex -= 1` and retry at once
                (`_on_append_reply`). That costs one extra RPC per divergent entry.
- Alternatives: §5.3's optimisation (the follower returns the conflicting term and
                the first index it holds for that term, so the leader skips a whole
                term per RPC); a cheaper variant where the follower returns its log
                length and the leader jumps straight to it.
- Why:          The plain rule is the one Figure 2 states. It is three lines and
                obviously safe: the leader only ever steps back one entry at a time
                towards the point where the logs match. The optimisation needs
                fields the handout's reply doesn't have (it only has `matchIndex`),
                and getting the skip slightly wrong is a classic bug (overshooting
                is fine; landing past the match point is not). The paper itself
                says "we doubt this optimization is necessary". Retrying
                *immediately* rather than at the next heartbeat keeps repair at one
                round trip per entry (about 1 ms on localhost) instead of 100 ms.
- Paper:        Figure 2, Leaders: "If AppendEntries fails because of log
                inconsistency: decrement nextIndex and retry (§5.3)"; end of §5.3.
- Viva Q:       "A follower missing 10,000 entries comes back. How long does repair take?"
- Answer:       With plain backoff, finding the match point takes one refused probe
                per missing entry: about 10,000 round trips, even though the
                follower is only *short*. Sending the entries after that takes
                another 10,000 / 64, about 157. On localhost that's a few seconds.
                Over a WAN it would be the reason to add the optimisation (even the
                cheap "reply with my log length" variant fixes the short-follower
                case), and it fits in `_on_append_reply` without touching the
                receiver's safety logic. Gap 3.

### R7. One AppendEntries in flight per peer, immediate follow-ups, at most 64 entries
- Chose:        `_ae_in_flight` (renamed from `_hb_in_flight`) allows one unanswered
                AppendEntries per peer. A reply sends the next RPC at once if the
                peer is still behind. `propose()` sends to idle peers at once. The
                heartbeat tick covers retries after a lost RPC.
                `MAX_ENTRIES_PER_RPC = 64`.
- Alternatives: pipelining (several outstanding RPCs per peer); sending only on the
                heartbeat tick; unbounded batches.
- Why:          With one RPC outstanding per peer, a reply always answers the probe
                in progress, so the leader's bookkeeping stays easy to follow.
                Pipelining would need per-RPC tracking and handling of
                out-of-order replies. Sending only on ticks would add up to 100 ms
                to every commit and every repair step. The cap of 64 keeps one RPC
                small (gRPC's default limit is 4 MB) and limits how long a follower
                holds its lock for a single request.
- Paper:        Figure 2, AppendEntries: entries "may send more than one for
                efficiency"; §5.3: "the leader retries AppendEntries RPCs
                indefinitely".
- Viva Q:       "What if the reply is lost?"
- Answer:       Any failure clears the flag, and the next heartbeat tick resends
                from the same nextIndex. The receiver is idempotent (R10), so if
                the request did arrive the resend is harmless.

### R8. The Figure 8 rule: count replicas only for current-term entries
- Chose:        `_advance_commit` scans N from the last index down to
                `commitIndex + 1`, **stops** at the first entry whose term isn't
                `currentTerm`, and otherwise commits the first N that a majority
                holds (the leader counts itself).
- Alternatives: commit the highest N a majority holds, whatever its term (the bug
                Figure 8 shows); also count old-term entries when they are on
                *every* node (the paper says that would be safe, but it's more code
                for no benefit).
- Why:          A majority holding an old-term entry doesn't stop a node with a
                *later* last term from winning an election and overwriting that
                entry. Figure 8(d) shows this, and
                `test_figure8_old_term_entry_on_a_majority_is_not_committed` makes
                it happen. A current-term entry on a majority does stop it: every
                majority of voters includes a node whose last term is the current
                one, and that node refuses any candidate whose last term is older.
                Once that entry commits, Log Matching makes everything before it
                committed too. Breaking at the first old term is correct because
                terms never decrease along a log, so every lower N is old as well.
- Paper:        Figure 2, Leaders, last bullet ("and log[N].term == currentTerm");
                §5.4.2; Figure 8.
- Viva Q:       "The entry is on a majority. Why isn't it committed?"
- Answer:       Because it's from an older term, and Figure 8 shows that a majority
                copy of an old-term entry can still be overwritten. It becomes
                committed as soon as an entry from my own term above it reaches a
                majority. The trace in raft-replication.md shows commitIndex
                jumping straight from 0 to 4.

### R9. A new leader appends a no-op entry at the start of its term
- Chose:        `_become_leader` appends `LogEntry(currentTerm, b"", EntryKind.NOOP)`
                as its first act and replicates it like any entry. It sets
                nextIndex[] *before* appending, so the first AppendEntries to
                each peer carries the no-op. The kind is an explicit field
                (`EntryKind.COMMAND` / `NOOP`, also on the wire as a proto enum).
                The applier advances lastApplied over a no-op without calling
                `apply_fn`, so **apply_fn's indexes can skip numbers**.
- Alternatives: (a) No no-op (this chunk's first version). Old-term entries a
                new leader holds commit only when a client's `propose()` puts a
                current-term entry on a majority. Not in Figure 2, so it is
                closer to the spec, and `apply_fn` sees every index. Rejected
                because it costs liveness: until the next proposal, a new
                leader (and its followers) can't apply entries that were
                *already committed* under the previous leader. The chaos test
                stalled once in ten runs, most likely for this reason, and the
                first fix was a test workaround (keep proposing until everyone
                caught up). That workaround is reverted now.
                (b) Mark no-ops by `data == b""`: an empty command is a legal
                client payload, so this would silently swallow one.
                (c) Show no-ops to `apply_fn` and let it ignore them: that hands
                a Raft-internal detail to the state machine. A dense index
                sequence isn't worth that.
                (d) Set nextIndex after appending the no-op: also correct, but
                every follower's first reply would then be a refusal, costing
                one wasted round trip per follower per election.
- Why:          The Figure 8 rule (R8) means a leader can only commit by
                counting replicas of a *current-term* entry. A fresh leader has
                none. The no-op is a current-term entry that exists immediately,
                so the cluster's commitIndex catches up after every election
                without depending on clients. It also has to be there before
                leader-served reads (§8): a leader can't answer reads correctly
                until it knows its commitIndex is current.
                `test_old_term_entries_commit_after_a_leader_change_with_no_new_proposals`
                shows it: entries on a majority, two leader changes, zero
                proposals, every node applies them. Against the old `node.py`
                that test times out. The chaos test converges only through it
                too.
- Paper:        §8 ("each leader commit a blank no-op entry into the log at the
                start of its term"); Figure 8 and §5.4.2 for why it's needed.
- Viva Q:       "After failover, why does the new leader write an empty entry?"
- Answer:       Because it may only commit entries from its own term (Figure 8).
                The no-op is that entry. Once it's on a majority, everything
                before it is committed too, including entries the old leader had
                committed but the new one couldn't know about. It never reaches
                the state machine, so apply_fn's indexes skip it.

### R10. Receiver: truncate only on a real conflict; commitIndex never goes down
- Chose:        `_merge_entries` walks the incoming entries alongside the local log:
                - same index, same term: skip;
                - same index, different term: truncate from there and append the
                  rest;
                - past the end of the log: append the rest.
                Step 5 uses `min(leaderCommit, prevIndex + len(entries))`, and
                `_set_commit_index` never lowers `commitIndex`.
- Alternatives: "truncate everything after prevIndex, then append" (the common
                bug); in step 5, use the local last index instead of the last *new*
                entry.
- Why:          A duplicated or delayed AppendEntries carries a *prefix* of what the
                follower already has. Truncating after that prefix would delete
                entries that match. The leader may already have counted those
                entries toward a commit, so losing them can lose committed data.
                The "last new entry" bound exists because entries beyond it have
                not been checked against this leader's log. For a delayed RPC the
                min can be below the current `commitIndex`; Figure 2 says
                commitIndex "increases monotonically", so the higher value is kept.
- Paper:        Figure 2, AppendEntries receiver steps 3-5; §5.5 ("Raft RPCs are
                idempotent ... it ignores those entries in the new request").
- Viva Q:       "Why not just truncate after prevLogIndex? The leader's log is the truth."
- Answer:       The leader's log is the truth, but this request may be an *old*
                message from that leader, carrying fewer entries than it has sent
                since. Deleting on old information can remove an entry the leader
                has already counted as replicated.

### R11. matchIndex comes from the request, not the reply
- Chose:        On success, `match = args.prev_index + len(args.entries)`, raised
                only if larger, and `nextIndex = matchIndex + 1`. The reply's
                `matchIndex` field (from the handout) carries the same number, but
                the leader doesn't rely on it.
- Alternatives: trust `reply.match_index`.
- Why:          The leader knows exactly what it sent and what the follower
                confirmed. Using its own request means a buggy reply can't inflate
                a commit count. Taking the `max` handles a delayed success that
                arrives after a newer one.
- Paper:        Figure 2, Leaders: "If successful: update nextIndex and matchIndex
                for follower"; matchIndex "increases monotonically".
- Viva Q:       "Two success replies arrive out of order. What happens?"
- Answer:       The older reply proves less, the `max` keeps the larger value, and
                nextIndex is recomputed from it.

### R12. Stale-reply guards on the leader
- Chose:        (a) A refusal decrements nextIndex only if
                `nextIndex == args.prev_index + 1` (the refusal is about the probe
                in progress), and never below 1. (b) The in-flight flag is cleared
                only by a reply whose `args.term` is the current term.
- Alternatives: decrement on every refusal; clear the flag on any reply.
- Why:          (a) A delayed refusal of an older probe would decrement a second
                time and skip past the match point. That stays safe (the next probe
                fails or re-sends matching entries), but it wastes round trips and
                is confusing to read. (b) A straggler from an earlier leadership
                would clear the flag of the new term's RPC and allow two
                outstanding RPCs. Both guards are about tidiness and efficiency.
                Safety comes from the receiver's checks.
- Paper:        §5.3.
- Viva Q:       "What if the decrement goes wrong?"
- Answer:       Raft stays safe. The follower's consistency check refuses anything
                that doesn't match, so a wrong nextIndex only costs extra round
                trips.

### R13. The `last_log` hook is gone; the election vote tests use a real log
- Chose:        The constructor parameter `last_log` (E12) is removed, and
                `_last_log()` now reads storage. Two election tests now build their
                voter with `MemoryStorage(term=2, entries=[t1, t2, t2])`, whose last
                entry is (index 3, term 2), the same as the old `lambda: (3, 2)`:
                - `test_vote_requires_candidate_log_at_least_as_up_to_date`
                  (5 cases);
                - `test_refused_stale_candidate_does_not_burn_the_vote`.
                The election `Cluster` lost its unused `last_logs` argument.
- Alternatives: keep `last_log` as an optional override for tests.
- Why:          A test-only hook in production code is one more thing to explain at
                the viva, for no benefit. A pre-filled storage is the real mechanism
                (it is what a restart will look like in step 6), and the assertions
                didn't change.
- Paper:        §5.4.1.
- Viva Q:       "Did you change the tests to make them pass?"
- Answer:       Only how the voter's log is built, not what is asserted. The inputs
                (last entry index 3, term 2) and the expected grants are identical.

### R14. Index-0 sentinel in the log
- Chose:        Slot 0 of the storage list is `LogEntry(0, b"")`; real entries start
                at 1.
- Alternatives: a 0-based list with `-1` arithmetic everywhere; a special case for
                `prevLogIndex == 0`.
- Why:          The paper's indexes are 1-based, and `prevLogIndex = 0,
                prevLogTerm = 0` means "before the first entry". With the sentinel,
                `term_at(0) == 0`, so step 2 needs no special case. The code and the
                paper use the same numbers, so the trace can be read directly
                against Figure 2. The sentinel can't be deleted
                (`truncate_from(0)` raises) and is never returned by
                `entries_from`.
- Paper:        Figure 2, State: "first index is 1".
- Viva Q:       "What does prevLogIndex 0 mean?"
- Answer:       "Append at the very start." The empty prefix always matches, so the
                check passes, and that's how a follower with an empty log receives
                its first entries.

### R15. Scenario tests use a deterministic leader and pre-filled logs
- Chose:        In the Figure 7, Figure 8 and catch-up tests, exactly one node has
                a short election timeout and all the others have 30 s. Starting
                logs come from `MemoryStorage(term, entries)`, and payloads depend
                only on (index, term).
- Alternatives: let all nodes race and accept whichever wins; build the divergent
                logs by actually crashing leaders one after another.
- Why:          In these tests the scenario is the point. If the wrong node can win,
                the test checks a different scenario. That is exactly how the first
                draft of the stale-log test flaked: n3 was repaired by the leader
                and then won legitimately. Building Figure 7 from real crashes would
                take many timing-dependent steps. Making the payload a function of
                (index, term) keeps the pre-filled logs consistent with Log
                Matching. The randomised safety test is the one place where we
                *want* races.
- Paper:        Figures 7 and 8.
- Viva Q:       "Isn't a 30-second timeout cheating?"
- Answer:       It fixes who campaigns, not what the algorithm does. Votes, the
                consistency check, repair and commit all run unmodified. Elections
                under real contention are tested separately (E19, and the chaos
                test in this chunk).

### R16. A tripwire: a follower never deletes a committed entry
- Chose:        `_merge_entries` asserts `index > commitIndex` before truncating.
- Alternatives: no check (trust the proof); log a warning and carry on.
- Why:          By Leader Completeness this can't happen. If it ever does, State
                Machine Safety is already broken, and crashing is better than
                applying a different entry at an index this node has already
                applied. Same idea as E20.
- Paper:        Figure 3, Leader Completeness and State Machine Safety.
- Viva Q:       "Which bug would fire it?"
- Answer:       A broken election restriction, the Figure 8 counting bug, or (after
                step 6) a node that forgot its log on restart but kept its
                commitIndex.

### R17. The Figure 8 tests hold the no-op back with a message-losing `Gate`
- Chose:        A test-only `Transport` wrapper. `gate.cap[("n1", "n2")] = 2` loses
                every n1 -> n2 AppendEntries that would **successfully** take n2
                past index 2. Messages n2 would refuse anyway still get through,
                so n1 can back up. The test also sets one entry per RPC
                (`monkeypatch` on `MAX_ENTRIES_PER_RPC`). The result: n2 gets
                the old term-2 entry, n1 learns `matchIndex[n2] = 2`, and the
                no-op stays on n1 alone.
- Alternatives: trim entries out of messages instead of losing them; a fully
                scripted fake follower; building the situation only from
                pre-filled logs, without n1 ever leading.
- Why:          With the no-op (R9), the old entry and the no-op normally travel
                in the same RPC, so the Figure 8 situation (old-term entry on a
                majority, leader knows it, nothing committed) never occurs on
                its own. Losing messages is legal network behaviour, and Raft
                must be safe under any pattern of loss, so the gate creates a
                legal schedule rather than faking one. Trimming would be
                unsound: the leader computes matchIndex from what it *sent*
                (R11), so a trimmed delivery would make it believe n2 has
                entries it doesn't. Pre-filled logs alone would never run the
                leader's counting code against `matchIndex`, which is the code
                under test.
- Paper:        Figure 8; §5.4.2.
- Viva Q:       "Your test peeks at the follower's log. Isn't that cheating?"
- Answer:       The peek only decides *which* messages get lost. Nothing in the
                node is changed or bypassed, and every message that arrives is
                exactly what the leader sent. It's an adversarial but legal
                network, the kind Raft's safety argument already has to cover.

---

## Persistence

Code: `raft/file_storage.py`, `raft/storage.py` (the `Storage` protocol),
`raft/run_node.py` (`--data-dir`). Walkthrough:
[raft-persistence.md](raft-persistence.md). Known gaps: section 7 of that file.

### P1. FileStorage wraps a MemoryStorage: disk first, then the in-memory copy
- Chose:        `FileStorage` keeps a `MemoryStorage` (`self._mem`). Each write
                method writes and fsyncs the file, and only then makes the same
                call on `_mem`. Every read is answered by `_mem`. The whole log
                stays in memory.
- Alternatives: read entries back from the file on demand (seek by offset); write
                the reads a second time in FileStorage; a shared base class.
- Why:          The node must not be able to tell the two storages apart. Reusing
                MemoryStorage makes the reads identical by construction, not by
                keeping two copies of the code in step. Disk-first ordering means
                the copy never holds anything that isn't durable: if the fsync
                raises, `_mem` is untouched. Holding the log in memory costs RAM
                but makes `term_at` (called on every AppendEntries) free. With no
                snapshots (P11) the log is replayed from memory anyway.
- Paper:        Figure 2, State.
- Viva Q:       "Your reads come from memory. How do you know memory matches disk?"
- Answer:       Memory changes only after the disk write and its fsync return, by
                the same call. On open, memory is built *from* the disk. And
                `test_file_storage_behaves_exactly_like_memory_storage` runs the
                same operations on both and compares every read.

### P2. Two files: `state.json` for term+vote, `log.bin` for entries
- Chose:        currentTerm and votedFor in one small file, replaced whole. The
                log in a separate append-only file.
- Alternatives: one file holding everything; a database (SQLite); a separate file
                for each of term and vote.
- Why:          They change differently. The term and vote change rarely and are
                tiny, so rewriting them whole is cheap and needs no record format.
                The log grows, so it must be appended to, never rewritten. Term
                and vote are in *one* file because a vote only means something
                with the term it was cast in (R5): two files would leave a crash
                window with a new term and an old vote. No SQLite, so the
                durability argument is ours to make and defend line by line.
- Paper:        Figure 2 ("votedFor: candidateId that received vote in current term").
- Viva Q:       "Why not write the term and the vote separately?"
- Answer:       Say the term is written, then we crash before the vote. On reboot
                the old vote would sit next to the new term, and look like a vote
                cast in a term we never voted in. One file, replaced atomically,
                means the pair is always old-and-old or new-and-new.

### P3. Atomic replace with `os.replace`; no directory fsync on Windows
- Chose:        write `state.json.tmp`, `flush()`, `os.fsync()`, `os.replace()`
                over `state.json`. On POSIX also fsync the directory
                (`_fsync_dir`). On Windows, skip the directory fsync, because it
                can't be done: `os.open` on a directory raises `PermissionError`
                (measured on this machine).
- Alternatives: `MoveFileExW(MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)`
                via ctypes; two "slot" files overwritten in place with a sequence
                number and a checksum (no rename at all); overwrite `state.json`
                in place.
- Why:          Overwriting in place can tear: half old, half new. A rename
                can't. After `os.replace` returns, every process on the machine
                sees the new file, so a **process crash** (the failure we test and
                demo) is fully covered. What isn't covered on Windows is a
                **power loss or OS crash** right after the rename: NTFS keeps the
                directory consistent but can come back with the *old* file. That
                is a real gap (walkthrough gap 1). I logged rather than fixed it
                because the brief said `os.replace`, and the ctypes alternative's
                documented guarantee is worded around cross-volume moves (I'm not
                sure it covers a same-volume rename).
- Paper:        Figure 2 ("Updated on stable storage").
- Viva Q:       "Is your vote durable if the power fails right after you reply?"
- Answer:       The *contents* are: they were fsync'd before the rename. The
                *rename* might not be on Windows, because Python can't fsync a
                directory there. After a process crash it's always there. For
                power loss I'd switch to MOVEFILE_WRITE_THROUGH or two in-place
                slots. It's listed as a known gap.

### P4. Log record: length, checksum of the length, checksum of the payload
- Chose:        `[length u32][crc32(length) u32][crc32(payload) u32][term u64,
                kind u8, data]`, big-endian, `zlib.crc32`. Kind 1 = COMMAND,
                2 = NOOP (0 is unused). No file header or version number.
- Alternatives: one checksum over the whole record; length-only framing; newline
                -delimited JSON; a file header with a magic number and version.
- Why:          The length must be trusted before the payload can even be found,
                so it gets its own checksum. Otherwise a flipped bit in a *middle*
                record's length could point past EOF, look like a torn tail, and
                silently drop every record after it. CRC32 catches every burst
                error shorter than 32 bits, which is plenty for detecting torn or
                damaged writes. It's not meant to stop a deliberate forger. JSON
                lines would need escaping for arbitrary bytes (the payload is
                opaque, R1). Kind 0 unused means zero-filled space never decodes
                as an entry. I skipped a version header to keep the
                torn-tail rules about records only; changing the format later
                would need one.
- Paper:        none (an engineering choice under Figure 2).
- Viva Q:       "What if the length field itself is corrupted?"
- Answer:       Its own checksum fails, and we stop with CorruptStorageError. We
                never follow a length we haven't verified. Tested by flipping a
                length byte in the first of three records.

### P5. `state.json` is plain JSON with no checksum
- Chose:        `{"term": 3, "voted_for": "n2"}`. A parse failure or a wrong type
                means corrupt, and the node refuses to start.
- Alternatives: a binary struct with a CRC, like the log.
- Why:          Atomic replace (P3) means the file is never half-written, so the
                main reason for a checksum is gone. JSON can be read in an editor
                during the viva and the demo. A truncated or edited file fails to
                parse (tested). A file that is wrong but still well-formed (say, a
                hand-edited term) wouldn't be caught. I accept that, since it's
                outside the crash model.
- Paper:        —
- Viva Q:       "Why does the log have checksums but the state file doesn't?"
- Answer:       The log is appended to in place, so a crash can leave half a
                record. The state file is only ever swapped whole by a rename, so
                a crash leaves the old one or the new one.

### P6. Torn tail: what a crash in the last append can leave, cut off on open
- Chose:        After the last good record, these count as torn: fewer bytes than
                a header; only zero bytes; a payload that runs past EOF; a
                checksum-failing **last** record. Torn bytes are truncated off the
                file and fsync'd *on open*, before any new append. A partially
                written batch keeps its complete leading records.
- Alternatives: discard the whole last batch (that would need batch markers);
                leave the garbage in place and just skip it; refuse to start on
                any damage.
- Why:          The torn write's `append` never returned, so nothing after it
                ran: no reply, no counting. Nobody was told this node holds that
                entry, which makes dropping it the same as an AppendEntries that
                never arrived (§5.5: the leader retries). Truncating on open is
                essential: otherwise the next append lands after the garbage, and
                the next open sees it in the *middle* and refuses. Keeping the
                complete prefix of a batch is safe for the same reason as
                accepting a shorter AppendEntries: they're the leader's entries,
                in order. The zero rule covers NTFS after a power cut (size grew,
                data didn't land). We never write an all-zero header.
- Paper:        §5.5 ("If a server crashes after completing an RPC but before
                responding, then it will receive the same RPC again after it
                restarts. Raft RPCs are idempotent").
- Viva Q:       "You threw away an entry from the log. Couldn't it be committed?"
- Answer:       Only entries this node acknowledged could count toward a commit.
                An acknowledgement is sent only after `append` returns, which is
                after the fsync. The torn record's append never returned, so it
                was never acknowledged and never counted.

### P7. Damage anywhere except the tail is a hard error
- Chose:        A bad header checksum (anywhere), or a bad payload with more
                records after it, raises `CorruptStorageError`. `run_node` exits.
                The same goes for a log without a state file, decreasing terms,
                or an entry's term above currentTerm. All checks run before the
                tail is truncated, so a refused directory is left untouched.
- Alternatives: skip the bad record and carry on; truncate everything from the bad
                record on; rebuild from the leader by starting empty.
- Why:          A record in the middle was followed by later appends, so its own
                append *returned*, and the entry may have been acknowledged and
                counted toward a commit. Skipping or truncating it silently makes
                this node a copy that's missing a committed entry: Leader
                Completeness can then fail, and it fails without anyone noticing.
                Starting empty is the forgotten-vote bug (walkthrough gap 6). A
                node that won't start costs availability, which a majority
                tolerates. A node that lies about its log costs safety, which
                nothing tolerates.
- Paper:        Figure 3 (Leader Completeness); §5.4.
- Viva Q:       "Why not just skip the bad record?"
- Answer:       Because this node may have acknowledged it. If that ack helped
                commit the entry, a log without it could later win an election and
                overwrite a committed entry. Refusing to start turns a silent
                safety bug into a loud availability problem.

### P8. fsync every write, synchronously, under the node lock
- Chose:        Each write method fsyncs before returning, and the node calls them
                while holding its lock. No batching across RPCs (no group commit),
                no background flusher.
- Alternatives: release the lock while fsyncing; group commit; an async log writer
                that holds back replies until the flush finishes.
- Why:          This makes "durable before responding" true just by the order of
                the code: write, fsync, then build the reply, all in one critical
                section (walkthrough §4). Every alternative needs a second
                mechanism to stop replies until a later flush, and that's where
                bugs live. The cost is measured: about 0.25 ms per append fsync and
                1.2 ms per state replace on this machine, against 100 ms
                heartbeats (30 ms in tests). Heartbeats with no entries and no term
                change write nothing at all.
- Paper:        Figure 2; §5.6 ("Raft's RPCs typically require the recipient to
                persist information to stable storage, so the broadcast time may
                range from 0.5ms to 20ms").
- Viva Q:       "You hold a lock during disk I/O. Isn't that slow?"
- Answer:       It's a quarter of a millisecond here, and the paper already
                expects persistence in the broadcast time. Doing it under the lock
                is what makes the durable-before-reply argument three lines long.
                Group commit would be the next step if throughput mattered.

### P9. The leader's own append is durable before it counts itself
- Chose:        `propose()` and the no-op call `storage.append` (fsync'd) and only
                then `_advance_commit()`, which counts the leader as one copy. This
                is sequential, not parallel to sending.
- Alternatives: send AppendEntries first and write the leader's disk in parallel,
                counting the leader only once its write finishes (the thesis
                optimisation).
- Why:          A commit means a majority of *durable* copies. If the leader counted
                a copy that was only in memory, power loss on the leader could
                leave fewer than a majority of real copies of an entry a client
                was told is APPLIED. The walkthrough §4 has the three-node trace.
                The parallel version is safe too, but it's a later optimisation,
                and the sequential one needed no change to node.py.
- Paper:        §5.3 ("A log entry is committed once the leader that created the
                entry has replicated it on a majority of the servers").
- Viva Q:       "Does the leader have to wait for its own disk?"
- Answer:       It has to wait before it *counts itself*, because otherwise
                "majority" includes a copy that a crash can erase. It doesn't
                strictly have to wait before *sending*, but we keep it simple and
                do both in order.

### P10. A failed write fails the storage stop
- Chose:        If any write raises, the error propagates (so the handler sends no
                reply). The storage records the failure, and every later write
                raises `StorageFailedError` until the process restarts. Reads keep
                working.
- Alternatives: retry the write; ignore the error; crash the process.
- Why:          After a failed write the file's tail is unknown: some, all, or
                none of the bytes may be there. Appending after it could put good
                records behind garbage, and a later open would call that
                corruption (P7). Retrying an fsync after it fails is known to be
                unreliable (the OS may have dropped the dirty pages). Restarting
                re-reads the file and applies the torn-tail rules, which is the
                path we've tested. I didn't kill the whole process because that
                would also take down the auction server sharing it.
- Paper:        —
- Viva Q:       "The disk fills up mid-append. What happens?"
- Answer:       The append raises, so no reply is sent and nobody counts that
                entry. The node then refuses every write: it can't vote or accept
                entries, and the rest of the cluster carries on without it. A
                restart repairs the tail.

### P11. Restart replays the log; nothing volatile is saved; no snapshots
- Chose:        commitIndex, lastApplied and the state machine all start empty on
                boot. The node re-applies from index 1 once a leader tells it the
                commitIndex. No snapshots.
- Alternatives: save lastApplied plus a state-machine snapshot together
                (§7 snapshots); save only commitIndex.
- Why:          Figure 2 puts commitIndex and lastApplied in the *volatile* box,
                and the deterministic `apply()` (CLAUDE.md) means replaying the
                same log rebuilds the same state. The only rule: lastApplied and
                the state machine must be lost or kept *together*. Losing one
                without the other either double-applies or skips entries. Saving
                commitIndex isn't needed: a leader re-establishes it, and the no-op
                gets it moving after a full restart. Snapshots are §7, out of
                scope.
- Paper:        Figure 2 ("Volatile state on all servers"); §7 (log "takes more
                time to replay").
- Viva Q:       "After a restart, won't the node apply everything twice?"
- Answer:       Into an empty state machine, once each, so the result is the same
                state as before the crash. lastApplied is 0 *because* the state
                machine is empty. The test checks the re-applied indexes match the
                ones from before the crash.

### P12. node.py is unchanged in logic; a `Storage` protocol documents the seam
- Chose:        Add `Storage` (a `typing.Protocol`) to `raft/storage.py`. Change
                node.py's type hint to it, and fix one docstring line. Nothing else
                in node.py changed.
- Alternatives: an abstract base class that both inherit from; leave the hint as
                `MemoryStorage`.
- Why:          The seam from R5 already put all seven write sites before their
                replies, so the algorithm needed nothing. A Protocol names the
                interface without making MemoryStorage inherit anything, and the
                old hint was simply wrong.
- Paper:        —
- Viva Q:       "What did you have to change in the Raft algorithm for persistence?"
- Answer:       Nothing. Every write already went through three methods, and every
                reply was built after them. Making those methods fsync was the
                whole change.

### P13. Tests: a crash is a second instance; dead nodes are fenced; memory runs are strict xfails
- Chose:        At the storage level, a crash is opening a second `FileStorage` on
                the same directory while the first is still open and never closed.
                At the cluster level, `crash()` drops the node from the network,
                stops it, then closes its storage while holding the node's lock.
                The two headline tests are parametrised over MemoryStorage with
                `xfail(strict=True)`.
- Alternatives: kill real subprocesses in the unit tests; let the dead node's
                storage stay open.
- Why:          A second instance sees only the files, never the first instance's
                memory, which is exactly what a new process sees. No close means
                durability can't depend on a clean shutdown. In the cluster, an
                RPC routed just before the drop can still be inside the dead node's
                handler. Without the fence it could write through the old handle
                after the new incarnation opened the file, a thing a real crash
                can't do. `close()` flushes nothing, so the fence doesn't hide a
                missing fsync. Strict xfail proves every run that the tests
                *detect* the bug: if the memory runs ever passed, the suite would
                go red. Real subprocess kills are in the walkthrough's gRPC trace.
- Paper:        —
- Viva Q:       "How do you know your test would catch a missing persistence?"
- Answer:       It runs both ways. With MemoryStorage it fails, every time, with
                "voted for n1 AND n2 in term 5" and "x applied nowhere". That's a
                strict xfail, so if it ever stopped failing, the suite would fail.

### P14. `run_node --data-dir`: optional, one directory per node, refuse on corruption
- Chose:        `--data-dir DIR` selects FileStorage on DIR (created if missing).
                Without it the node uses MemoryStorage. On `CorruptStorageError` the
                runner exits with the message. At start it logs the recovered term,
                vote and log length. `data/` is gitignored.
- Alternatives: make a data dir mandatory; derive it from `--id`; one shared
                directory with a subfolder per node chosen automatically.
- Why:          Optional keeps every existing script and test working unchanged.
                Explicit beats derived, because pointing two nodes at one directory
                is the dangerous mistake (walkthrough gap 3), and an explicit path
                makes it visible in the command line. The recovered-state log line
                is what the demo and the trace show.
- Paper:        —
- Viva Q:       "What happens if I start a node on a damaged directory?"
- Answer:       It prints why and exits. It never starts with a log that might be
                missing something it acknowledged.

### P15. Step-down then grant writes the state file twice
- Chose:        A RequestVote with a higher term causes two durable writes:
                `(T, null)` in `_step_down_if_stale`, then `(T, candidate)` when the
                vote is granted. Left as is.
- Alternatives: merge them into one write (that changes node.py's logic).
- Why:          Both writes are correct on their own: each is a consistent
                (term, vote) pair, and a crash between them leaves "term T, no
                vote", which is true. The cost is one extra ~1 ms fsync per
                election per voter. Merging would mean changing the algorithm file
                for speed, which this chunk was told to avoid.
- Paper:        Figure 2, All Servers ("If RPC request or response contains term
                T > currentTerm: set currentTerm = T, convert to follower").
- Viva Q:       "A crash between the two writes: is that safe?"
- Answer:       Yes. On reboot the node is in term T and hasn't voted. It never
                sent "granted", because the reply is built after the second write,
                so the candidate didn't count it.

---

## Auction on Raft

Code: `replication/`, `sessions/`, `server/`, `proto/commands.proto`.
Walkthrough: [auction-on-raft.md](auction-on-raft.md). Known gaps: the end of
each part of that file.

### A1. Command encoding: one `Command` proto with a oneof, times as epoch micros
- Chose:        `proto/commands.proto`. `Command { client_id = 1; seq = 2;
                oneof kind { create_auction = 10; place_bid = 11;
                close_auction = 12; login = 13; logout = 14; } }`. One message
                per command type, mirroring the dataclasses field for field.
                Times are `int64` epoch microseconds through
                `common/time_conv`. `replication/codec.py` is the only code
                that knows the form. Field numbers are permanent: removing a
                field means a `reserved` line, and
                `test_field_numbers_are_pinned` compares against bytes written
                out by hand.
- Alternatives: pickle; JSON; a hand-rolled struct format; one proto message
                with every field optional and a type tag.
- Why:          These bytes sit in `log.bin` forever and are decoded again on
                every restart (P11), possibly by newer code. Pickle is tied to
                Python class paths, so renaming a class breaks old logs, and
                it can run code on load. JSON would work, but nothing would
                check the schema or pin the field numbers. A oneof makes "no
                command" and "two commands" impossible to express, and
                `WhichOneof` gives the dispatch. Epoch micros match
                `auction.proto`, so there is one time representation from the
                wire to the disk. Starting the oneof at 10 leaves 3..9 for
                envelope fields.
- Paper:        Figure 2 ("each entry contains command for state machine");
                R1 (entries are opaque bytes).
- Viva Q:       "You add a field to PlaceBid next month. Can a node replay
                last month's log?"
- Answer:       Yes. A new field number reads as its default from old bytes,
                and old field numbers never change meaning. Renumbering is the
                one thing that would break it, and the pinned-bytes test turns
                red first.

### A2. `apply_fn` is a `StateMachine` method; undecodable bytes fail-stop
- Chose:        `StateMachine.apply_fn(index, data)` decodes, dedupes,
                dispatches (auction commands to the unchanged
                `AuctionState.apply()`, Login/Logout to `SessionState.apply()`),
                and returns `Applied`. Every *outcome* is a returned value. The
                one exception is bytes that don't decode: logged CRITICAL (index
                + 64 bytes of hex), then raised as `CorruptEntryError`, so the
                applier halts (R3).
- Alternatives: log the bad entry and skip it; return a failure `Applied` for
                it.
- Why:          The task said "it must not raise: a decode error is a
                fail-stop, so log it loudly (R3)". I read that as: no outcome
                raises, and a decode error is the one deliberate stop. Skipping
                would actually be "the same on every node", since every node
                holds the same bytes. But a committed entry that doesn't decode
                can only come from a corrupt log or a code version mismatch.
                Carrying on would hide that, and the next entry could depend on
                the skipped one. So stop loudly.
- Paper:        Figure 2, All Servers (apply log[lastApplied]); R3.
- Viva Q:       "One entry in the log is garbage. What does the cluster do?"
- Answer:       Every node reaches the same index, logs CRITICAL and stops
                applying there. They all hold the same state. Raft itself keeps
                running (votes, replication), but nothing past that index is
                applied until someone fixes it.

### A3. Replicated sessions: the leader mints the token, Login/Logout are commands
- Chose:        `SessionStore.create()` mints `secrets.token_urlsafe(32)` on
                the leader, then proposes `Login(token, username)`. `destroy()`
                proposes `Logout(token)`. `validate()` reads the replicated
                `SessionState` on this node. Same `create/validate/destroy`
                shape. `create` returns `Applied.token`, which on a dedupe hit
                is the *first* attempt's token. No TTL.
- Alternatives: accept re-login after a failover (CLAUDE.md's other option);
                derive the token deterministically inside `apply()` (for
                example a hash of index and term); sticky sessions on one node.
- Why:          Re-login on every failover would break the kill-the-leader
                demo, and it hides the problem instead of solving it. A token
                computed inside `apply()` from log data would be guessable by
                anyone who can guess the index. Randomness must happen *once*,
                outside, and be replicated as data: the same rule as the LLM
                text and `curr_time`. Returning `Applied.token` matters. A
                retried Login mints a new token, but dedupe returns the first
                response, and only the first token is in the table.
- Paper:        §8 (dedupe); CLAUDE.md "apply() must be deterministic".
- Viva Q:       "Where is the randomness in your login, and why isn't that a
                determinism bug?"
- Answer:       In `SessionStore.create`, on the leader, before proposing. The
                log holds the resulting token as plain data, so every node
                applies the same string. `apply()` never generates anything.

### A4. The interceptor: an unknown token on a follower redirects instead of refusing
- Chose:        A missing token gives UNAUTHENTICATED everywhere. An
                *unrecognised* token gives UNAUTHENTICATED on the leader, but on
                a follower it gives FAILED_PRECONDITION with the leader's
                address.
- Alternatives: refuse unknown tokens everywhere; forward every request to the
                leader.
- Why:          A follower's session table can lag the leader's. A token the
                leader minted 2 ms ago may simply not have arrived. Refusing it
                would tell a valid user "you're not logged in". A follower
                can't tell "unknown" from "not here yet", and it can't serve
                the request anyway, so it sends the client where the answer is
                authoritative. (Part B adds the same care on a *new* leader.)
- Paper:        —
- Viva Q:       "Can an attacker use this to find the leader without a token?"
- Answer:       Yes, with any made-up token, but the address is not a secret:
                any request without a token at all is refused, and a valid
                user's first redirect reveals it anyway. It gives away nothing
                about sessions.

### A5. The §8 dedupe table lives in `apply_fn`, keyed by client_id, one entry per client
- Chose:        `client_id → (latest seq, Applied)`, in `StateMachine`.
                `seq == last` returns the stored response. `seq < last` is
                refused as stale, without executing. Higher executes and
                stores. `client_id == ""` is never deduplicated. Rejections are
                stored as well as successes. Fields are on the requests in
                `auction.proto` (`client_id`, `seq`, new field numbers) and on
                `Command`.
- Alternatives: in `AuctionState.apply()`; keep *every* (client, seq) ever seen;
                key by username; dedupe in the handler on the leader only.
- Why:          `apply()` is M1 code that the task said not to change, and
                dedupe has to cover Login/Logout too. One entry per client is
                exactly §8 ("the latest serial number processed for each
                client"). It is enough because our client has one write
                outstanding at a time, so a lower seq can only be a stale
                duplicate. It has to be replicated state: a handler-side table
                on the old leader dies with it, and the retry arrives at the
                new leader. Keying by username fails for two sessions of one
                user, and Login has no username until it succeeds.
- Paper:        §8: "the state machine tracks the latest serial number
                processed for each client, along with the associated response."
- Viva Q:       "The leader commits a bid, then crashes before replying. The
                client retries on the new leader. Is the bid placed twice?"
- Answer:       No. The new leader's log already has the first copy, and
                applying it put (client, seq) in the table on every node. The
                retry is a second entry with the same seq, so `apply_fn`
                returns the stored response and doesn't execute it.

### A6. Error mapping: FAILED_PRECONDITION + trailing metadata, and UNAVAILABLE for every unknown
- Chose:        `NotLeaderError` gives `FAILED_PRECONDITION`, with trailing
                metadata `leader-id` and `leader-addr` (the leader's *client*
                address). Everything else (LEADERSHIP_LOST, STOPPED,
                OVERWRITTEN, a timeout) gives `UNAVAILABLE` "outcome unknown,
                retry with the same (client_id, seq)". Done in one place,
                `server/errors.py`.
- Alternatives: put the leader in a message field; NOT_FOUND / ABORTED;
                report OVERWRITTEN as its own "definitely failed" code;
                forward to the leader on the server side.
- Why:          A refused call has no response body, so metadata is the only
                place for a hint. FAILED_PRECONDITION is gRPC's code for "the
                system isn't in the state this needs; fix that before
                retrying". UNAVAILABLE is gRPC's "transient, retry" code, and it
                is also what a dead node looks like. So the client's two retry
                cases (node dead, outcome unknown) are one branch.
                OVERWRITTEN does mean "not committed", but the right client
                action is the same (retry, same seq), and with dedupe a retry
                is always safe. One rule is easier to get right. Server-side
                forwarding would need a leader-to-leader proxy and hides
                topology from the client, and §8 has the client do it.
- Paper:        §8: the server "will reject the client's request and supply
                information about the most recent leader it has heard from".
- Viva Q:       "Your client got UNAVAILABLE for a bid. Was it placed?"
- Answer:       Maybe. That's why the retry carries the same serial number. If
                it was placed, the retry returns the original answer; if not,
                the retry places it. Either way it happens once.

### A7. The closer runs everywhere, proposes only on the leader, one close in flight per auction
- Chose:        `AuctionCloser.tick(now)`: drop resolved handles; return if
                not leader; for each due auction with no close in flight,
                `propose(CloseAuction(id, curr_time=now))` without waiting and
                keep the handle. `now` is the leader's clock, read once per
                tick. `apply()` still rejects a duplicate close.
- Alternatives: start or stop the closer thread on leadership change (an
                observer hook); have `submit()` block per close; no in-flight
                set (rely on apply() rejecting duplicates); a close timer per
                auction on the leader.
- Why:          Running everywhere and checking `is_leader()` each tick needs
                no hand-over. A new leader's next tick finds whatever is
                overdue, and nothing has to be recovered. That keeps M1's
                "restartable poll loop" property. Not waiting means one slow
                commit can't stall the loop. Without the in-flight set, a
                250 ms tick plus a commit slower than that would put two closes
                in the log. That would still be *correct* (apply rejects the
                second) but noisy. With it, exactly-once holds on a stable
                leader without timing luck: the handle resolves only after
                apply, by which time the auction is no longer due.
- Paper:        —; CLAUDE.md "Time-dependent values are stamped once, before
                proposing".
- Viva Q:       "Two nodes both think they are leader for a moment. Can an
                auction be closed twice, at two different times?"
- Answer:       Both may *propose*, but only the real leader's entry commits.
                The stale leader's entry is overwritten, or committed later
                behind the first close and rejected by apply() as "already
                closed". The committed log has exactly one successful close,
                with one curr_time, the same on every node.

### A8. PlaceBid's high bid is read inside `apply_fn`, returned in `Applied`
- Chose:        After `AuctionState.apply(PlaceBid)`, `apply_fn` reads the max
                bid under the state's lock and returns it in `Applied`
                (`high_bid`, `high_bidder`). The handler replies with that.
                Stored in the dedupe table with the rest of the response.
- Alternatives: keep M1's lock-free read in the handler after the wait; add a
                "high bid" to `apply()`'s `Result` (changes M1 code); drop the
                fields.
- Why:          On the leader, by the time `submit()` returns, other bids may
                have been applied after ours, so a read in the handler can be
                for a *later* state than our bid's. Inside `apply_fn` we are on
                the only thread that writes `AuctionState`, under the state
                machine's lock, so it is exactly the post-state of this
                command. That is deterministic, so a retry gets the same
                numbers. `Applied` carries it so `apply()` stays unchanged.
- Paper:        Figure 2, "respond after entry applied to state machine".
- Viva Q:       "What does current_high_bid mean in your reply?"
- Answer:       The highest bid right after *your* bid was applied, in log
                order. Not "now", because by now it may already be higher.

### A9. A separate peer port with no auth interceptor
- Chose:        Each process runs two gRPC servers: the client port (Auth +
                Auction behind `AuthInterceptor`) and the peer port
                (`RaftService` only, no interceptor). `--peers
                id=host:peer_port:client_port`.
- Alternatives: one port, with `RaftService` exempted in the interceptor's
                `EXEMPT_METHODS`; a shared cluster secret in metadata.
- Why:          Raft RPCs come from other nodes and carry no session token.
                Behind the token check, every heartbeat would be refused.
                Exempting them on one port would put the cluster's internal
                API on the same socket as the public one, and one mistake in
                the exempt list exposes it. Two ports keep them apart, and in
                production the peer port would be firewalled to cluster
                members and use mutual TLS (each node proves membership with a
                certificate). That is a transport-level fix, which is why it
                isn't in the interceptor. Here it is plain TCP on 127.0.0.1, a
                known gap.
- Paper:        —
- Viva Q:       "Can a client send AppendEntries to your node?"
- Answer:       On the client port, no: that service isn't there. On the peer
                port, today yes, if it can reach it. In production the peer
                port takes connections only from cluster members, authenticated
                by mTLS.

### A10. Leader check before the LLM call; curr_time is stamped before proposing
- Chose:        `CreateAuction`: `check_leader()`, then the LLM call, then
                `submit()`. `PlaceBid`: stamp `now_utc()`, then `submit()`. A
                follower's stamp is discarded with the NOT_LEADER. A retry on a
                new leader is a new command with a new stamp.
- Alternatives: no pre-check (`submit` refuses anyway); stamp inside
                `apply()`; reuse the first attempt's stamp on a retry.
- Why:          Without the pre-check, a follower would spend seconds of model
                CPU and then throw the text away. The pre-check can be wrong a
                microsecond later, but `propose()` decides for real, so it only
                saves work and never decides an outcome. Stamping in `apply()`
                would make every node read its own clock (seam 1's bug).
                Re-stamping a retry is honest: if the first attempt never
                committed, the bid really did reach the cluster at the retry's
                time. If it did commit, dedupe returns the first answer and the
                new stamp is never used.
- Paper:        CLAUDE.md "LLM output never enters the replicated log as a
                call"; "stamped once, before proposing".
- Viva Q:       "A follower receives a bid. Whose clock stamps it?"
- Answer:       Nobody's. The follower redirects, and the client resends to
                the leader. Only a leader's reading is ever proposed, and the
                followers apply that number from the log.

### A11. Handlers wait 5 s for their entry; the in-memory tests wait 1 s
- Chose:        `RaftReplicator(timeout=5.0)`. After that,
                `OutcomeUnknownError` → UNAVAILABLE. The integration tests use
                1.0 s.
- Alternatives: wait forever; a short value like 0.5 s.
- Why:          A healthy commit is one round trip (ms). A leader change is an
                election timeout (0.5-1.0 s) plus a round. 5 s means "something
                is wrong", and it must stay under the client's own deadline,
                so the client hears an explicit UNAVAILABLE instead of an
                ambiguous DEADLINE_EXCEEDED. Waiting forever ties up a gRPC
                worker per stuck request. The tests use 1 s because they only
                need "longer than a healthy commit", and they hit the timeout
                on purpose.
- Paper:        —
- Viva Q:       "The wait times out but the entry commits a second later.
                What did the client see?"
- Answer:       UNAVAILABLE, outcome unknown. It retries with the same seq,
                the retry finds the committed entry in the dedupe table, and
                it gets the real answer.

### A12. `LocalReplicator`: M1 unit tests run unchanged, through the real codec
- Chose:        `AuctionServicer(state, llm)` and `SessionStore()` with no
                replicator build a `LocalReplicator`, a cluster of one without
                Raft that encodes, then calls `apply_fn` directly.
- Alternatives: make the replicator a required argument and rewrite the M1
                tests; give the servicer an `if raft is None: apply()` branch.
- Why:          The M1 tests specify M1 behaviour that still holds (owner from
                the token, LLM degradation, session semantics). Keeping them
                unchanged shows that. A direct-`apply()` branch would leave a
                second write path in the handler, and that is exactly the
                seam this chunk removes. Through `LocalReplicator`, the only
                write path is `submit()`, everywhere.
- Paper:        —
- Viva Q:       "Is there any code path where a handler still calls apply()?"
- Answer:       No. Handlers only call `submit()`. In production that is Raft;
                in M1's unit tests it is a one-node stand-in that still goes
                through encode and `apply_fn`.

### A13. `serve.py --data-dir` is required
- Chose:        An auction node always uses `FileStorage`. There is no
                in-memory mode for `server.serve` (`raft.run_node` keeps its
                optional one).
- Alternatives: optional, like `run_node` (P14).
- Why:          An auction node acknowledges client writes. In memory, a
                restart forgets acknowledged bids and can double-vote (the
                strict-xfail runs of the persistence tests show both). For a
                bare Raft demo that is a choice; for the auction it is a bug.
- Paper:        Figure 2, "Updated on stable storage before responding to RPCs".
- Viva Q:       "Can I run a node without a disk?"
- Answer:       Not an auction node. Use `raft.run_node` for an in-memory Raft
                demo. Anything that tells a client "your bid is placed" has to
                survive a restart.

### A14. Read-index inside `raft/node.py`: one round counter, one slot per peer, any same-term reply counts
- Chose:        `read_index(timeout)`: wait for this term's no-op to commit,
                record commitIndex, bump `_read_round` and send
                AppendEntries, wait for a majority of `_acked_round >= round`,
                wait for `lastApplied >= readIndex`. `_sent_round[peer]` is
                set when an AppendEntries is built. `_acked_round[peer]` is
                raised by any reply in the same term, success or refusal. A
                peer whose in-flight RPC predates the round gets a follow-up
                right away. Acks reset at the start of each term.
- Alternatives: put the read through the log as a no-op command; a leader
                lease (no round trip, relies on bounded clock drift); a
                separate "ping" RPC; a list of pending reads with their own
                sets of ackers.
- Why:          A log write per read costs an fsync on every node and grows
                the log. §8 rules out the lease for us: it "would rely on
                timing for safety". A separate RPC would be a third Raft
                message that is not in Figure 2. Reusing AppendEntries
                keeps one message type and its existing term checks. One
                counter plus one slot per peer is enough *because* there is at
                most one AppendEntries in flight per peer (R7). Any reply in
                our term proves "the peer had not moved past our term when it
                answered", and that is the only fact the safety argument
                needs.
- Paper:        §8 (two precautions); thesis §6.4 (ReadIndex ordering).
- Viva Q:       "Why is a majority of heartbeat replies enough to rule out a
                newer leader?"
- Answer:       A newer leader needs votes from a majority in a higher term.
                Our acking majority shares at least one node with that one.
                That node answered us in our term after the read began, and
                terms never go back down, so it hadn't voted in a higher term
                yet. No newer leader existed when the read began.

### A15. Reads are served by the leader only
- Chose:        `GetAuction` / `ListAuctions` call `read_barrier()`. A
                follower answers NOT_LEADER like a write.
- Alternatives: follower reads (fast, possibly stale); follower read-index
                (the follower asks the leader for the index and waits until
                it has applied that far); serve reads from any node and
                document them as "eventually consistent".
- Why:          A bidder deciding what to bid needs the real high bid. A
                stale read that shows a lower high bid makes the next bid
                fail and looks like a bug. Follower read-index is correct, but
                it needs a new RPC to fetch the leader's index. Every
                client already finds the leader for writes, so leader-only
                costs nothing extra on the client side.
- Paper:        §8.
- Viva Q:       "Your followers do nothing for reads. Isn't that wasteful?"
- Answer:       For three nodes on one machine, yes, and that's fine.
                Correctness first. Spreading reads would mean the follower
                read-index variant, which keeps the same guarantee and only
                adds an RPC.

### A16. The interceptor runs a read barrier before refusing an unknown token on the leader
- Chose:        Unknown token: on a follower, redirect. On the leader,
                `check_leader()` then `read_barrier()`, then look again. Only
                then UNAUTHENTICATED. A failed barrier gives UNAVAILABLE, never
                UNAUTHENTICATED. The lookup moved into the call (the handler
                body), out of `intercept_service`.
- Alternatives: refuse immediately (Part A's behaviour); a barrier on *every*
                request; put the token on the command and validate it inside
                `apply()`.
- Why:          A newly elected leader may not have applied a Login that an
                earlier leader committed and acknowledged. Refusing would log
                out a valid user exactly at failover, which is the moment
                the demo is about. The barrier only runs on a miss, so the
                normal path costs nothing. Validating inside `apply()` would
                mean changing the M1 commands so they carry a token. The
                barrier needs the call's context to abort, so it can't run in
                `intercept_service`.
- Paper:        §8 (precaution one: a new leader learns its commitIndex by
                committing its no-op).
- Viva Q:       "A user logged in on n1, n1 died, and their first request
                lands on n2 before n2 has applied the Login. What happens?"
- Answer:       n2 doesn't know the token, so it runs the read barrier. That
                waits until n2's no-op commits and n2 has applied everything
                before it, including the Login. The second lookup finds it,
                and the request goes ahead. A test builds exactly this case.

### A17. Client retry policy: follow hints at once, back off on everything else, same seq throughout
- Chose:        A hint is followed immediately (at most `len(addresses)` in a
                row). No hint, UNAVAILABLE or DEADLINE_EXCEEDED: back off
                (0.05 s doubling to 1 s) and try the next node in the list.
                Give up after 30 s with `ClusterUnavailable`. Per-attempt
                deadline 8 s (CreateAuction 20 s). The seq is taken once per
                operation.
- Alternatives: random next node (the paper's "randomly-chosen servers");
                retry the same node; no overall deadline; jittered backoff.
- Why:          Round-robin reaches every node within one pass, and with
                three nodes randomness buys nothing. Backoff stops clients
                from hammering a cluster that is in an election. A hint chain
                is capped because stale hints can point at a dead node or at
                each other (trace step 2). DEADLINE_EXCEEDED means "don't
                know", like UNAVAILABLE, and the same seq makes retrying safe.
                The per-attempt deadline is above the server's 5 s commit
                wait (A11), so the server's explicit answer normally wins.
- Paper:        §8 ("clients then try again with randomly-chosen servers").
- Viva Q:       "What stops your client from placing a bid twice when it
                retries?"
- Answer:       The retry is the same request object, with the same client_id
                and seq. If the first attempt committed, the state machine
                returns its stored response. The trace shows it: four
                attempts, one seq, one bid.

### A18. `ClusterClient`: one per user, one write at a time, pluggable invoker
- Chose:        A `ClusterClient` holds one token and one seq counter, and is
                not shared between threads. The RPC itself goes through an
                `invoker(addr, service, method, request, metadata, timeout)`:
                `GrpcInvoker` in real use, an in-memory one in tests.
- Alternatives: one client for all users (the M1 demo shared one channel);
                thread-safe with a lock; mock gRPC channels in tests.
- Why:          The dedupe table keeps only the latest seq per client_id
                (A5). Two writes in flight on one client_id could make the
                lower one "stale". One client per user, sequential, is
                exactly §8's model. The invoker seam lets the failover tests
                run the real retry logic against a cluster whose nodes the
                test crashes deterministically. The real-gRPC test covers the
                `GrpcInvoker` path.
- Paper:        §8.
- Viva Q:       "Can two threads share one ClusterClient?"
- Answer:       Not for writes. Their serial numbers would interleave and one
                could be refused as stale. Use one client per thread or per
                user.

### A19. A read that can't be confirmed is UNAVAILABLE "read not confirmed; retry"
- Chose:        `ReadNotConfirmedError` (a subclass of `OutcomeUnknownError`),
                mapped to UNAVAILABLE with its own message.
- Alternatives: serve the possibly stale local state with a warning flag;
                reuse the write's "outcome unknown" message.
- Why:          Serving stale data is exactly what read-index exists to
                prevent. A subclass means every "retry later" handler already
                catches it. The message differs because a read has no outcome
                to be unsure about, and telling the client to keep its seq
                would be misleading.
- Paper:        §8 ("Linearizable reads must not return stale data").
- Viva Q:       "A partitioned old leader gets a GetAuction. What does the
                client see?"
- Answer:       UNAVAILABLE after the read timeout, never the old data. The
                client moves on to another node, finds the new leader, and
                reads there.

---

## Testing and chaos

Code: `common/time_conv.py` (T1), `proto/status.proto`, `server/status_servicer.py`,
`checker/`, `scripts/dashboard.py`, `scripts/chaos.py`, `scripts/demo.ps1`.
Walkthrough: [testing-and-chaos.md](testing-and-chaos.md). Known gaps: the
end of that file.

### T1. `time_conv`: exact integer arithmetic, no float (fixes an M1 bug)
- Chose:        `dt_to_micros(dt) = (dt - EPOCH) // timedelta(microseconds=1)`
                and `micros_to_dt(n) = EPOCH + timedelta(microseconds=n)`.
                The strict-xfail codec test became a normal passing test, and
                `tests/test_time_conv.py` round-trips 200 000 seeded random
                values (1900-2200) both ways.
- Alternatives: keep the float and `round()` instead of `int()` (fixes the
                pinned value, but a double still has only 53 bits of mantissa,
                and epoch micros already use 51; it is "probably right", not
                exact); store `google.protobuf.Timestamp` (seconds + nanos,
                exact, but a second time representation next to the int64 the
                whole proto design uses); leave the bug (determinism was never
                broken, see below).
- Why:          `timedelta` stores days, seconds and microseconds as integers,
                and `timedelta // timedelta` is integer division, so no value
                is ever rounded. The old code was not a divergence bug: the
                leader encodes once and every node, the leader included,
                applies the DECODED bytes, so all replicas agreed on the same
                wrong microsecond. But it was a correctness bug at the wire
                boundary: a bid stamped one microsecond AFTER `close_time`
                could be truncated to exactly `close_time` and accepted,
                because `curr_time > close_time` is a microsecond comparison.
- Data:         a `data\<id>` directory written BEFORE this commit holds
                times that were truncated by the old code. They still decode,
                and every node decodes them the same way, but they can be one
                microsecond off what the user meant. It is dev data only:
                wipe `data\` (`Remove-Item -Recurse data`) after pulling this.
- Paper:        none (wire format); CLAUDE.md "apply() must be deterministic".
- Viva Q:       "Your M1 time conversion was wrong. Did that ever make two
                replicas disagree?"
- Answer:       No. Only the leader converts a datetime to micros, once, and
                every node applies the decoded value, so they all agreed on
                the same truncated time. The bug lost a microsecond against
                what the user saw, not between replicas. Now it is exact
                integer arithmetic, tested on 200 000 random values.
