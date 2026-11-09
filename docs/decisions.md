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
