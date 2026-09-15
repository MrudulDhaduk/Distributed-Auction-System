# Where I am — revision note

Updated 2026-09-15. Covers Tasks 1–2 done, Task 3 in progress. If I've been
away again, read this first, then `grpc-notes.md` if the gRPC details have
gone.

---

## The project in four sentences

A distributed online auction. Three servers hold the auction; any one can die
without losing a bid that was already acknowledged. Raft keeps the three
servers holding an identical **log** of commands, and each server replays that
log through `apply()` to build its own copy of the auction state. The auction
is an excuse — the marks and the viva are about the consensus layer.

**The one sentence everything rests on: `same log → same state`.**

Raft supplies the *log*. `apply()` supplies the *→*. Both halves have to be
right or three servers disagree.

---

## Vocabulary

| Plain | Real term |
|---|---|
| the list | the **log** |
| a line in it | a **log entry** |
| line 3 | **index** 3 |
| the auction table | the **state** |
| working the list into the answer | **applying** the log |
| "a majority has it, it's promised" | **committed** |
| the boss | the **leader** |
| a round | a **term** |
| more than half | a **quorum** |

---

## Status

| Task | What | Done? |
|---|---|---|
| 1 | Auction state machine, `apply()`, concurrency | ✅ |
| 2 | gRPC ping round-trip | ✅ |
| 3 | Auth, sessions, real auction RPCs | 🟡 **in progress** |
| 4 | LLM server | ⬜ |
| 5 | Raft: leader election | ⬜ |
| 6 | Raft: log replication | ⬜ |
| 7 | Raft: log repair + persistence | ⬜ |

Plus the three extensions committed in the problem statement: live dashboard,
acknowledged-bid property checker, seeded chaos harness.

**Task 3, what's actually in**, per `git log`:
- `sessions/store.py` — `SessionStore`: `create`/`validate`/`destroy`, opaque
  tokens via `secrets.token_urlsafe`, no TTL (deferred on purpose — see the
  docstring on clock skew across replicated sessions)
- `server/auth_interceptor.py` — the one choke point that reads the token off
  gRPC metadata, validates it, and stamps `context.username` before any
  handler runs. `AuthService.Login` is the one exempt method.
- `server/auth_servicer.py`, `server/auction_servicer.py` — `Login`/`Logout`
  and `CreateAuction`/`PlaceBid`/`GetAuction`/`ListAuctions` all wired to
  `AuctionState.apply()`. Bidder identity comes from `context.username`,
  never from the request.
- `CloseAuction` command (`auction/state.py`) — done and tested at the
  `apply()` level (`tests/test_close_auction.py`, 8 cases: winner-by-highest-
  not-last, reject-before-close-time, reject-double-close, zero-bid close,
  bid-after-close via both the time guard and the `closed` flag).

**Task 3, what's still open:**
- `CloseAuction` has no trigger yet. Not an RPC, no timer in `serve.py`.
  The design note in `proto/auction.proto` says M1 closes over a timer that
  calls `apply(CloseAuction(...))` directly — that piece isn't built.
- Uncommitted: a comment in `auction_servicer.py` on why `PlaceBid`'s
  post-`apply()` read of `auction["bids"]` outside the lock is safe
  (lock-free, possibly stale, never torn — `BidEntry` is frozen and
  `list.append` is atomic under the GIL). Worth deciding: comment now, or
  fold into the CloseAuction-trigger commit.

~20% of the code done. ~35% of the understanding. Raft is half the project.

**Milestone 1 (Sept 28)** = Tasks 3 and 4. No Raft.
**Milestone 2 (Nov 18)** = Tasks 5–7.

---

# TASK 1 — the state machine

`auction/state.py`. No network, no Raft. One file.

## What's in it

- **Commands** — `CreateAuction`, `PlaceBid`. Frozen dataclasses. Dumb data,
  no methods. These are what Raft will eventually replicate.
- **`Result(success, reason)`** — what `apply()` hands back. Local only, never
  replicated.
- **`BidEntry(bid_amount, bidder, time_stamp)`** — one accepted bid. State,
  not command.
- **`AuctionState.apply(command)`** — the single entry point for every state
  change.

## Three properties, and where each comes from

### 1. Determinism — from purity

`apply()` reads **nothing** but its arguments and current state. No clock, no
randomness, no I/O, no LLM calls.

That's why `PlaceBid` carries `curr_time` instead of `apply()` calling
`datetime.now()`. The clock reading gets **frozen into the log** at the moment
it happened, so a replay six months later produces the same answer.

Without this: three nodes with identical logs compute different states, and a
recovering node rebuilds a state that never existed.

### 2. Atomicity — from the lock

```python
def __init__(self):
    self.auctions = {}
    self.lock = threading.Lock()      # instance, not class

def apply(self, command):
    with self.lock:                   # whole body
        ...
```

**Instance attribute**, so three `AuctionState` objects (one per Raft node)
don't contend on one lock. A class-level lock passes the test and is wrong.

**`with`**, because there are six `return` statements and every one must
release. Explicit acquire/release would need `try/finally` and one missed path
hangs everything forever.

**Whole body**, not just `PlaceBid`. Three reasons:
- `CreateAuction` has the same check-then-act shape (two identical ids could
  both pass the `in` check; the second wipes the first, bids and all)
- Future commands (`CloseAuction`, `SettlePayment`) are protected
  automatically instead of each being a chance to forget
- The property I actually need is *"one command in, completes entirely, then
  the next"* — that's what makes state a function of the log at all

### 3. The two are NOT the same thing

**Lock buys atomicity. Purity buys determinism.**

A single-threaded `apply()` with `datetime.now()` inside would be perfectly
thread-safe and completely non-deterministic. Both properties are required;
neither is sufficient.

The lock is the precondition that makes determinism a *meaningful claim* — it
makes state a function of the log, and then purity makes that function the
same on every node.

## The race I found on purpose

Two steps: read the high bid, then append if mine is bigger. Three threads all
read `highest = 0` before any of them appended. Three winners.

```python
highest_bid = max(...)          # ← found out
                                # ← THE GAP. don't unlock here.
auction["bids"].append(bid)     # ← acted on it
```

**Locking the read and the write separately fixes nothing** — another thread
slips into the gap and reads stale state. The lock must span read-decide-write
without letting go.

## The false pass — the most useful thing that happened

The test passed **15 times in a row** with no lock. The bug was there the whole
time.

CPython's default switch interval is **5ms**. `apply()` takes single-digit
**microseconds**. So threads never actually interleaved — they queued at the
GIL and each ran start to finish. **I was getting mutual exclusion by
accident**: the GIL was coarser than my critical section.

Dropping to `sys.setswitchinterval(1e-9)` → **22/40 runs failed**, 2 to 4
winners each.

Two things worth keeping:
- **The list was never corrupted** — always 2, 3, or 4 clean entries, never a
  torn one, because `list.append` is atomic under the GIL. **The GIL protects
  the data structure; it does not protect my invariant.** Different scopes.
- **Nondeterminism arrived with no clock, no randomness, no I/O anywhere in
  sight.** Purely from concurrent invocation. Same commands, four different
  states. That's the argument for why serialisation must be structural.

**The lesson that generalises:** "it passed 15 times" is not evidence of
correctness — it's evidence I sampled 15 orderings out of an enormous space and
got lucky. That gap between *passed* and *correct* is the entire reason the
seeded chaos harness exists in the plan.

After the lock: 40/40. But **the reason to believe it's safe is the argument**
(one lock, held across the whole read-modify-write, no early return escaping
it), not the count. The test's job is to catch the day that argument stops
being true.

## Two design corrections I made

**Two sources of truth is a data model bug.** I first stored
`auction["bids"].append(amount)` plus a separate `auction["bidder"] = name`.
Wrong: the list holds amounts, a scalar holds one name, and the pairing is lost
the moment a second bid lands. Also only ever holds the most recent bidder —
which coincides with the winner today by accident of the code path, not by
construction.

Fix: **one event, one object.** `BidEntry` carries who, how much, when. The
winner is *derived* (max by amount), not maintained.

Why it matters downstream: every replica must produce an identical bid history
*including attribution*, and the property checker has to verify that every
*acknowledged* bid survives a crash — which needs the bidder↔amount pairing
intact.

**Max vs last entry.** My validation rejects anything `<= highest_bid`, so the
history is strictly increasing and max == last, always. But that's a
consequence of *my validation rule*, not a fact about auctions. Allow equal
bids, add retractions, or let a delayed bid land out of order and they diverge
immediately. Viva answer: name the invariant I'm relying on, don't say "the
last one, I think."

---

# TASK 2 — gRPC

Full detail in `grpc-notes.md`. Compressed here.

## The problem

Two processes can't call each other's functions. Only **bytes** cross between
them. gRPC's job: **make a remote call look like a local one.**

```python
response = stub.Ping(request)     # looks local. isn't.
```

## The chain

```
proto/ping.proto              I write the contract
     ↓  python scripts/gen_proto.py
generated/ping_pb2.py         box classes      (from `message` blocks)
generated/ping_pb2_grpc.py    Stub + Servicer  (from `service` block)
     ↓  I subclass the Servicer
server/ping_server.py         the only real work in the system
     ↓  I build a Stub
client/ping_client.py         the call
```

## THE key bit

One `service` block → **two** classes, because a call has two ends.

| | Generated gives me | I do |
|---|---|---|
| **Stub** (client) | a **finished** object | just use it |
| **Servicer** (server) | a class whose methods **raise NotImplementedError** | **inherit, override** |

```python
# generated — the whole class
class PingServiceServicer:
    def Ping(self, request, context):
        raise NotImplementedError('Method not implemented!')

# mine — replaces it
class PingServicer(ping_pb2_grpc.PingServiceServicer):
    def Ping(self, request, context):
        return ping_pb2.PingResponse(...)
```

Exactly `Animal`/`Dog`. **This was the piece I kept missing.** The generated
Servicer gives the *shape*; I supply the *behaviour*.

Why asymmetric: "pack bytes and send to an address" is identical for everyone,
so the generator writes it completely. "What happens when a Ping arrives" is my
decision, so it leaves a hole.

## How the server finds my code

**Push, not pull.** The generated file never calls my code — I hand mine in:

```python
ping_pb2_grpc.add_PingServiceServicer_to_server(PingServicer(node_id), server)
```

Inside, it builds `{'Ping': servicer.Ping}` where `servicer` is **whatever
object I passed**. Without this line, every call returns UNIMPLEMENTED.

## Field numbers

**Names don't travel. Only numbers do.** Rename a field, keep its number:
nothing breaks. Numbers are scoped per message, so two messages can both use 1.

**Never reuse a number.** Old bytes get read as the new field — **no error,
silently wrong**. Use `reserved 2;` so the compiler stops me.

**Why it matters more here:** network bytes live 2ms; **Raft log bytes live on
disk forever**. A log entry written in October gets replayed by December's
code. The incompatible old version is *my own past self*.

*Viva: reusing a field number causes silent misinterpretation of persisted log
entries on replay — not an error.*

## Practical

- `python scripts/gen_proto.py` — regenerates and patches the import. One
  command, whenever a `.proto` changes.
- Generated files are **committed**, not built on the fly — so a grader
  cloning the repo doesn't need matching `grpcio-tools`.
- PowerShell: no `\` line continuations. Single-line commands.
- `max_workers=4` in the server thread pool is **where concurrency enters the
  system** — same shape of problem the `apply()` lock solves.

---

# RAFT — the concepts I need before Task 5

Full version in `how-this-works.md` §4. The essentials:

**The log is the real data structure.** Servers don't store "the current high
bid" — they store the ordered list of what happened and *compute* the high bid
by replaying it. Making servers agree on an ordered list is tractable; making
them agree on a moving target isn't.

**One leader writes.** Followers redirect. This removes the entire class of
"two servers appended conflicting entries at index 3" — by construction, not by
being careful. Raft isn't for speed; it's for not being wrong.

**Committed = a majority has it.** The leader appends → replicates → waits for
more than half → *then* applies and replies to the client. Before that gap
closes, nothing was promised, so losing it is fine. After, it must never be
lost.

*(With 3 nodes, if 2 are down the survivor refuses all writes. Correct
behaviour, not a bug — Raft stops rather than risk disagreeing.)*

**Elections need random timeouts.** Identical timeouts → all candidates at once
→ nobody gets a majority → livelock, busy and permanently leaderless. Random
(e.g. 150–300ms) means one goes first and wins. **Classic first bug.**

**Why nothing committed is ever lost — memorise this:**
- committed ⇒ on a majority
- winning an election ⇒ a majority voted
- any two majorities of the same set **must overlap**
- plus: a server **refuses to vote** for a candidate whose log is behind its own

⇒ a candidate missing a committed entry can't reach a majority. **Anyone who
can win already holds every committed entry.** That's Leader Completeness.

**The log holds attempts; `apply()` decides outcomes.** A bid of 80 against a
high of 100 still gets logged and replicated — all three nodes run the same
`apply()` and reject it identically. If Raft filtered losing bids it would have
to understand auction rules. It stays deliberately dumb about the payload.

**State is never sent over the network.** Only log entries move. Each server
rebuilds its own state by replaying its own log.

---

# What Raft actually changes in my code

**Nothing in `state.py`.** Not one line. Raft wraps around it.

```
NOW:  gRPC handler → state.apply(command) → reply

M2:   gRPC handler → raft.propose(command)
                     → append to log, replicate, wait for majority
                     → committed
                     → state.apply(command)      ← same function, unchanged
                     → reply
```

One line changes, in a handler file I haven't written yet. Then a new applier
loop inside the Raft node calls `apply()` on committed entries in order, on
every node.

| | Owns |
|---|---|
| **Raft** | the log, the ordering, when it's safe to apply |
| **`apply()`** | what a command means, how state changes |
| **the lock** | that a command applies whole, with nothing peeking mid-way |

The lock doesn't disappear in M2 — its job shifts. Raft's applier is sequential
by construction, so writers stop colliding with writers. But the gRPC server
still has many **reader** threads asking "what's the high bid?" while the
applier is mid-append. The lock keeps them from seeing half-applied state.

**Raft takes over the ordering half. The lock keeps the atomicity half.**

---

# How I'm working

**Three chats:** manager (planning, review, what's next), tutor (concepts,
no repo access), Claude Code (repo work, constrained by `CLAUDE.md`).

**Routing:** touches files → Claude Code. "Explain why" → tutor. "What next /
is this design right" → manager.

**Protected list — I write these myself, always:** `apply()`, command types,
concurrency control, Raft node logic, property checker assertions.

**What works for me:** monkey terms first, then the exact code lines mapped to
them. Toy example on an unrelated problem, then apply it myself. Say "that's
too much, slow down" the moment it's too much — that's a skill, not a
weakness. Explain it back in my own words before moving on.

**What doesn't:** copy-pasting between chats without reading. Reading an
explanation feels like understanding and isn't. Understanding is checked by
producing something.

---

# Next

**Finish Task 3** — auth, sessions, and the four auction RPCs are done and
tested (see Status above). What's left: give `CloseAuction` a trigger. Decide
timer-in-handler vs. something else, then wire it into `serve.py`. After
that, Task 3 is closed and Task 4 (LLM server) is next.

Still no Raft. Still one server.

**Also outstanding:** I'm two people on a three-person assignment. Prof. Bera,
chamber hours Wednesday 5–6pm, prior email required. Needs doing.
