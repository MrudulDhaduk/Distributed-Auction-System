# Chunk 4: the auction on Raft

Build-order steps 3 to 6 built a Raft node that replicates opaque bytes. This
chunk puts the auction on top of it. Before this chunk, every handler called
`AuctionState.apply()` directly on whichever node it reached. Now every change
goes **handler → log → every node's apply()**, and every read is served by a
leader that has proved it is still the leader.

- **Part A** (sections 1 to 8): the replicated write path. That covers command
  encoding, `apply_fn`, replicated sessions, §8 dedupe, the handlers, the
  closer and `serve.py`.
- **Part B** (sections 9 onward): reads (read-index), the cluster-aware
  client, and failover.

Paper quotes are from `papers/raft-extended.pdf`. Decision entries A1, A2, ...
are in [decisions.md](decisions.md#auction-on-raft).

---

## 1. The invariants first

The point of the chunk is that the cluster behaves like **one** auction server
that never forgets and never does anything twice. Four facts have to hold.

1. **Same log, same state.** Every node runs the same `apply()` on the same
   commands in the same order. `apply()` is deterministic (no clock, no random
   numbers, no I/O), so every node ends up with the same auctions, sessions
   and dedupe table. Raft gives the "same commands, same order" part (State
   Machine Safety, Figure 3). The rest of this chunk keeps everything that
   *isn't* deterministic (the clock, the token, the LLM) **outside** `apply()`.
   Each of those is decided once, on the leader, and carried on the command as
   plain data.
2. **Only the leader decides what goes into the log.** A follower never
   proposes, never calls the LLM, and never closes an auction. It answers
   "not me, try *there*".
3. **"Success" means applied.** A client hears success only after its entry
   was committed and applied on the leader (R4: `Outcome.APPLIED`). Every other
   failure is either "nothing happened" (NOT_LEADER) or "don't know"
   (UNAVAILABLE).
4. **A retry never runs twice.** "Don't know" is only safe to retry if the
   retry can't execute a second time. The §8 dedupe table makes sure it can't.

---

## 2. The write path, end to end

One `PlaceBid`, from the client's call to the reply:

| step | where | what happens |
|---|---|---|
| 1 | `server/auth_interceptor.py:74` `intercept_service` | The token is checked against **this node's** replicated session table. Unknown on a follower: redirect (A4). Unknown on the leader: UNAUTHENTICATED. |
| 2 | `server/auction_servicer.py:129` `PlaceBid` | Read the clock **once**: `curr_time = now_utc()`. Build `PlaceBid(auction_id, bidder=context.username, amount, curr_time)`. |
| 3 | `replication/replicator.py:92` `submit` | `codec.encode(command, client_id, seq)` gives bytes. Then `raft.propose(bytes)`. |
| 4 | `raft/node.py:295` `propose` | Not leader: resolved NOT_LEADER at once, with `leader_hint`. Leader: append `LogEntry(term, bytes)` and send AppendEntries. |
| 5 | `raft/node.py:489` `_advance_commit` | A majority holds it, and its term is the current term, so commitIndex moves past it. |
| 6 | `raft/node.py:772` `_run_applier`, on **every** node | The applier calls `apply_fn(index, bytes)`. |
| 7 | `replication/state_machine.py:88` `apply_fn` | Decode, check the dedupe table, call `AuctionState.apply()` (unchanged M1 code), read the high bid, store the response. Return `Applied`. |
| 8 | `raft/node.py:814` `_resolve_applied`, leader only | The handle for that `(index, term)` resolves APPLIED, with `result = Applied`. |
| 9 | `replication/replicator.py:92` `submit` | `proposal.wait(timeout)` returns. `submit` gives back `Applied`, or raises `NotLeaderError` / `OutcomeUnknownError`. |
| 10 | `server/errors.py:45` `raft_errors` | Maps NotLeaderError to FAILED_PRECONDITION plus trailing metadata `leader-addr`, and OutcomeUnknownError to UNAVAILABLE. |
| 11 | `server/auction_servicer.py:157` | Reply with `applied.result`, `applied.high_bid`, `applied.high_bidder`. |

`CreateAuction` is the same, with two steps before step 2. First
`check_leader()` (line 113) runs, and then the LLM call `_describe()` (line
115), in that order, so a follower never calls the model. Login and Logout are
the same shape through `SessionStore.create()` / `destroy()`.

---

## 3. Command encoding (`proto/commands.proto`, `replication/codec.py`) — A1

**The rule.** Figure 2 says each log entry "contains command for state
machine". Raft stores and ships *bytes* (R1), and the log is written to
`log.bin` and **replayed** on every restart (P11). So a command needs a byte
form that decodes to the same command on every node, today and after a
restart a year from now.

**The code.**
- `proto/commands.proto`: one `Command` message. It holds `client_id = 1`,
  `seq = 2`, and a `oneof kind` with one member per command type
  (`create_auction = 10` ... `logout = 14`). Every time is `int64` epoch
  microseconds.
- `codec.encode` (`codec.py:44`) and `codec.decode` (`codec.py:71`) are the
  only code that knows this form. Times go through `common/time_conv.py`, so
  there is one conversion in the codebase. A naive datetime raises in
  `encode`, on the leader, before anything is proposed.
- `decode` raises `DecodeError` for bytes that aren't a `Command`, and for a
  `Command` with no `kind` set. `b""` decodes to exactly that, which is why it
  needs checking.

**Why field numbers are permanent.** An old `log.bin` is decoded by new code
on restart. If `place_bid` were renumbered, an old bid entry would decode as
something else (or fail), and a restarted node would build a different state
from the others. `tests/test_codec.py::test_field_numbers_are_pinned` writes
the bytes of one command out by hand, so a renumbering turns that test red.
Removing a field means adding a `reserved` line (the comment at the top of the
proto says so).

**Determinism doesn't depend on byte-identical encoding.** Protobuf doesn't
promise that two encoders give identical bytes, and we don't need that. The
leader encodes once, and every node, **the leader included**, applies the
*decoded bytes*. Nobody re-encodes. This also matters for the M1 bug in
section 8.

**Seam 4 resolved:** "Commands are Python dataclasses with `datetime` fields
and no wire/disk encoding." They now have one, and the Raft log and the disk
both carry it.

---

## 4. The state machine behind `apply_fn` (`replication/state_machine.py`) — A2, A5, A8

**The rule.** Figure 2, All Servers: "If commitIndex > lastApplied: increment
lastApplied, apply log[lastApplied] to state machine (§5.3)". The node calls
`apply_fn(index, data)` once per committed command, in order (R2).
`StateMachine.apply_fn` (line 88) is that function.

**Dispatch** (`_dispatch`, line 120). Auction commands go to
`AuctionState.apply()`, which is unchanged. `Login` / `Logout` go to the new
`SessionState.apply()` (`sessions/state.py:45`), which follows the same rules:
deterministic, and an outcome is a returned `Result`, never an exception.

**The response is `Applied`** (line 55): the `Result`, plus `high_bid` /
`high_bidder` for a PlaceBid, plus `token` for a Login. This value travels back
to the handler as `Proposal.result` (R4), and the dedupe table stores it.

**The high bid** (`_high_bid`, line 132), resolving the `PlaceBid` part of
seam 2: "PlaceBid also reads the high bid right after apply() — decide what
that means on a follower". M1 read it in the handler, after `apply()`,
lock-free, so another bid could land in between. Now it is read inside
`apply_fn`, on the applier thread, which is the **only** thread that changes
`AuctionState`, and under the state machine's lock. Nothing can land in
between. It is exactly the state this bid left behind, and it is a pure
function of the log, so a follower would compute the same value. The
dedupe table stores it with the rest of the response. (A8.)

> Honesty note: `test_place_bid_reports_the_high_bid_from_the_applied_command`
> also passed against the M1 handler, because a single-threaded test can't
> produce the interleaving that made M1's read stale. The argument above is
> what makes the new read right. The test only shows the plumbing works.

**Corrupt bytes are a fail-stop** (lines 89-102). The task said "it must not
raise: a decode error is a fail-stop". I read that as: `apply_fn` never raises
for any *outcome* (a refused bid is a `Result`), and the one thing that does
raise is undecodable bytes. That raise is deliberate. It is logged at CRITICAL
with the index and the first 64 bytes in hex, then re-raised as
`CorruptEntryError`, and the applier halts (R3). Every node holds the same
bytes at that index, so every node halts at the same place with the same
state. (A2.)

### The §8 dedupe table — A5

**The rule**, §8: "clients assign unique serial numbers to every command.
Then, the state machine tracks the latest serial number processed for each
client, along with the associated response. If it receives a command whose
serial number has already been executed, it responds immediately without
re-executing the request."

**The code**, `apply_fn` lines 104-118, with `self._dedupe: client_id → (seq,
Applied)`:

| incoming | what happens |
|---|---|
| `client_id == ""` | never deduplicated (the closer, which is the cluster itself) |
| new client, or `seq > last` | execute, store `(seq, response)` |
| `seq == last` | return the stored response; **nothing executes** |
| `seq < last` | `Result(False, "stale request: ...")`; nothing executes, nothing stored |

**Why it lives in `apply_fn` and not in `AuctionState.apply()`.** The M1
`apply()` stays untouched, and dedupe covers Login and Logout as well.

**Why it is part of the replicated state.** It changes only inside `apply_fn`,
from the log, so every node builds the same table. That is what makes it work
across a failover. The *new* leader recognises a retry of a command the *old*
leader committed (trace in section 7).

**A failed response is remembered too.** A retried too-low bid gets the same
"no". It is not evaluated a second time against a state that has changed since.

---

## 5. Replicated sessions (`sessions/`) — A3, A4

Resolving **seam 3**: "SessionStore is not replicated. After leader failover,
every token is unknown to the new leader."

**The rule** is CLAUDE.md's own: no randomness inside `apply()`. A token is
random, so it is minted **outside**, on the leader, *before* proposing, and the
result is replicated as data. This is the same pattern as the LLM description.

- `SessionStore.create()` (`sessions/store.py:65`): `token =
  secrets.token_urlsafe(32)`, then `submit(Login(token, username), client_id,
  seq)`. It returns `applied.token`, the token that is actually in the table.
  On a dedupe hit that is the **first** attempt's token, not the one just
  minted. The new one was never stored and would never validate.
  (`test_a_repeated_login_returns_the_original_token`.)
- `validate()` (line 73) reads this node's applied `SessionState`. There is no
  Raft round.
- `destroy()` (line 78) proposes `Logout(token)`.

The `create / validate / destroy` shape is unchanged, as M1's docstring
planned. `SessionStore()` with no arguments is a cluster of one with no Raft
(`LocalReplicator`), so `tests/test_session_store.py` and
`tests/test_auth_interceptor.py` run unchanged.

**No TTL.** The reason is unchanged, and it is still in the docstring: expiry
needs a clock, and a clock read inside `validate()` would make nodes with
clock skew disagree.

**The interceptor, and a lagging follower** (`server/auth_interceptor.py:74`,
A4). A follower's table can lag the leader's, so a token minted a moment ago
may not have reached it yet. A follower can't tell "unknown" from "not here
yet", so for an unknown token it does **not** say UNAUTHENTICATED. It
redirects (FAILED_PRECONDITION + `leader-addr`), the same answer it gives
every request. Only the leader refuses an unknown token. A *missing* token is
refused everywhere, because there is nothing to look up.

> Part A gap, closed in Part B: a *newly elected* leader may not have applied
> everything yet either, so for a moment it could refuse a valid token. See
> section 9.

---

## 6. Handlers, errors, closer, ports

### Errors (`server/errors.py`) — A6

| Raft outcome | gRPC | client action |
|---|---|---|
| NOT_LEADER (propose refused, nothing appended) | `FAILED_PRECONDITION`; trailing `leader-id`, `leader-addr` | go to `leader-addr` |
| LEADERSHIP_LOST, STOPPED, OVERWRITTEN, timeout | `UNAVAILABLE` "outcome unknown, retry ..." | retry anywhere, **same (client_id, seq)** |
| business "no" (bid too low ...) | `OK`, `result.success=false` | unchanged from M1 |

Why the leader goes in **trailing metadata** and not in a message field: the
reply to a refused call has no message body. An aborted gRPC call carries only
a status, details and metadata. `test_three_nodes_over_grpc_redirect_then_replicate`
checks over real gRPC that metadata set before `abort()` reaches the client.

### The handlers (`server/auction_servicer.py`) — A10

`CreateAuction` (line 99): `check_leader()` (113), then `_describe()` (115,
the LLM call), then `submit()` (123). `check_leader()` is only a pre-check: the
node could lose leadership a microsecond later, and `propose()` decides for
real. Its job is to save a follower the LLM call
(`test_a_follower_never_calls_the_llm`). An LLM outage still degrades to
`description=""` and the auction is still created
(`test_llm_down_the_auction_is_still_created_with_an_empty_description`).

`PlaceBid` (line 129): `curr_time` is stamped on whichever node received the
request, but only a leader's `propose()` accepts it. A follower's reading is
thrown away with the NOT_LEADER, so **only a leader's clock ever enters the
log**.

### The closer (`server/closer.py`) — A7

Resolving **seam 1**: "`server/closer.py` calls `state.apply(CloseAuction(...))`
directly, on every node. Under Raft that's divergence."

`AuctionCloser.tick(now)` (line 92):
1. Forget the handles that have resolved (line 98).
2. Not the leader: return `[]` (line 100). The loop runs on every node, but a
   follower does nothing.
3. For each id from `due_auction_ids(state, now)` (line 47, the same pure
   lock-free scan as M1) with no close already in flight, call
   `propose(CloseAuction(id, curr_time=now))` **without waiting**, and keep
   the handle.

Exactly-once, and why it doesn't depend on timing. While a close is pending,
`tick` skips that auction. By the time its handle resolves APPLIED, the
leader's `apply()` has already run (resolve happens after apply,
`node.py:808-810`), so the auction is `closed` and no longer due. If the
handle resolves to anything else (unknown outcome), the auction is proposed
again on a later tick, by whichever node leads then. If the first close did
commit after all, `apply()` refuses the second one with "Auction is already
closed". That is the backstop.

After a failover nothing is handed over: the new leader's next tick sees the
overdue auctions (`test_after_failover_the_new_leader_closes_what_is_overdue`).

### Two ports per process (`server/serve.py`) — A9

`AuctionNode` (line 91) builds two gRPC servers:
- **client port**: Auth + Auction services, behind `AuthInterceptor`.
- **peer port**: `RaftService` only, with **no interceptor**. Raft RPCs carry
  no session token. Behind the token check, every heartbeat would be refused
  and no leader could ever hold office. In production this port would be
  reachable only by cluster members, over mutual TLS. Here it is plain TCP on
  127.0.0.1.

`test_the_peer_port_needs_no_token_and_the_client_port_does` checks both
halves over real gRPC.

`--peers id=host:peer_port:client_port` carries both ports, because a
redirect has to name the leader's *client* address, and Raft only knows node
ids. `--data-dir` is required (A13). `scripts/run_cluster.ps1` starts the LLM
server and n1..n3 on peer ports 50061-50063 and client ports 50071-50073, with
data dirs `data\n1..n3`. `-RaftOnly` keeps the bare Raft demo.

---

## 7. A real trace: a retried CreateAuction after an unknown outcome

From `test_a_retried_create_auction_returns_the_original_success`, run with
Raft logging on. These are the real lines, with times since the test started:

```
 384ms [n2] term 1: GRANTED vote for n1 (ok)
 385ms [n1] term 1: appended no-op at index 1
 385ms [n3] term 1: GRANTED vote for n1 (ok)
 385ms [n1] term 1: appended index 2                 <- Login(nisarg)
 386ms [n1] term 1: commitIndex 0 -> 2
 391ms [n2] term 1: commitIndex 0 -> 2
 391ms [n3] term 1: commitIndex 0 -> 2
 394ms [n1] term 1: appended index 3                 <- CreateAuction, (seller-1, 7)
 394ms [n2] appended 3..3 from leader
 394ms [n3] appended 3..3 from leader
                 ... both replies lost; n1 waits 1.0 s; n1 crashes ...
1472ms [n3] term 2: GRANTED vote for n2 (ok)
1472ms [n2] term 2: appended no-op at index 4
1473ms [n2] term 2: appended index 5                 <- the retry, (seller-1, 7)
1474ms [n3] appended 4..5 from leader
1474ms [n2] term 2: commitIndex 2 -> 5
```

Step by step:

1. **Term 1, n1 leads.** No-op at 1, then nisarg's Login at 2. Both commit.
2. **Index 3**: `CreateAuction(1, "vintage clock", owner=nisarg)` with
   `(client_id="seller-1", seq=7)`. n2 and n3 append it and reply "ok", but
   `LoseReplies` drops both replies. n1's matchIndex for both stays at 2, so
   n1 can't commit 3. After `WAIT = 1.0 s` its handler raises
   `OutcomeUnknownError` and the client gets **UNAVAILABLE**. Is the auction
   created? *Nobody knows yet.* It is on a majority (n2, n3) but uncommitted.
3. **n1 crashes. Term 2.** n2 asks for votes with last log (term 1, index 3).
   n3's last log is also (term 1, index 3), so n2 is "at least as
   up-to-date" (§5.4.1) and n3 grants. n2 leads term 2 and appends its no-op at
   **4**.
4. **The client retries on n2** with the same `(seller-1, 7)`, and n2 appends
   it at **5**. One AppendEntries carries 4..5 to n3. With n3's ack, index 5
   (a term-2 entry) is on a majority, so commitIndex jumps **2 → 5**. Index 3,
   the old-term entry, commits *indirectly* through the term-2 entries after
   it (Figure 8 rule, R8).
5. **n2 applies in order.** Index 3: dedupe has no `seller-1`, so it
   executes, the auction is created, and the table stores `seller-1 → (7,
   "Auction created successfully by nisarg")`. Index 4: no-op, skipped.
   Index 5: `seller-1`, seq 7 == 7, so the **stored response** is returned and
   nothing executes.
6. The retry's handle (index 5, term 2) resolves APPLIED with that stored
   response. The client sees **success**, and `auctions == [1]`. Without the
   dedupe table, index 5 would have executed and said "ID already exists,
   choose a different ID" to the seller who created it.

---

## 8. The M1 files this chunk changed, and why

CLAUDE.md marks `auction/state.py`, `server/`, `sessions/` and `llm_server/`
"handle with care".

| file | changed? | what and why |
|---|---|---|
| `auction/state.py` | **no** | `apply()`, the commands and the lock are byte-for-byte unchanged. Dedupe and the high-bid read live in `apply_fn` instead (A5, A8). |
| `sessions/store.py` | yes | Same `create/validate/destroy`, now over replicated state: create mints and proposes `Login`, destroy proposes `Logout`, validate reads `SessionState`. (A3) |
| `sessions/state.py` | new | `Login`, `Logout`, `SessionState.apply()`. |
| `server/auth_servicer.py` | yes | Login/Logout go through `SessionStore` with `(client_id, seq)`, wrapped in `raft_errors`. The password check is unchanged. |
| `server/auth_interceptor.py` | yes | Takes the replicator. An unknown token on a follower redirects instead of refusing. (A4) |
| `server/auction_servicer.py` | yes | Writes go `check_leader → LLM → submit`. The PlaceBid high bid comes from `Applied`. (A8, A10) |
| `server/closer.py` | yes | Leader-only, proposes, in-flight set. `close_expired()` became `due_auction_ids()` + `tick()`. (A7) |
| `server/serve.py` | rewritten | One process = Raft node + two gRPC servers. (A9) |
| `server/errors.py` | new | Raft outcome → gRPC code. (A6) |
| `common/wire.py` | yes | Adds the `leader-id` / `leader-addr` metadata keys. |
| `proto/auction.proto` | yes | `client_id`, `seq` on the four write requests (new field numbers), plus design note 3. |
| `llm_server/` | **no** | |

**M1 tests adapted:** `tests/test_closer.py` only. It called `close_expired()`,
which applied directly, with naive datetimes. Every M1 case survives as a
`due_auction_ids` or `tick` test. Unchanged and still green:
`test_auction_servicer.py`, `test_session_store.py`, `test_auth_interceptor.py`,
`test_close_auction.py`, `test_concurrent_bidding.py`.

**An M1 bug found, not fixed** (CLAUDE.md: "point at the line and the case it
breaks on"). `common/time_conv.py:25`, `return int(dt.timestamp() *
MICROS_PER_SECOND)`, does float arithmetic and then truncates. For
`2039-12-20 05:40:54.254810` the float is `2207972454254809.8`, which
truncates to `...809`, so 1 µs is lost. In a random sample about 2.4% of
microsecond datetimes are off by one. It is pinned by a strict xfail,
`tests/test_codec.py::test_round_trip_is_exact_to_the_microsecond_found_m1_bug`.
- **Determinism is not affected**, because every node, the leader included,
  applies the decoded integer, never the in-memory datetime.
- **What is affected**: a stamped time can be 1 µs early. So a bid stamped
  1 µs after `close_time` could be recorded *at* `close_time` and accepted
  (`curr_time > close_time` is false).
- The fix would be integer arithmetic,
  `(dt - EPOCH) // timedelta(microseconds=1)` and `EPOCH +
  timedelta(microseconds=m)`. That's your call.

---

## Part A: known gaps

| gap | fixed by | what goes wrong today |
|---|---|---|
| Reads (`GetAuction`, `ListAuctions`) still read local state on any node. | Part B, read-index | A follower or a deposed leader can return stale data. |
| A brand-new leader may not have applied a recent Login yet, so it can refuse a valid token for a moment. | Part B: the interceptor runs a read barrier before refusing | The client sees UNAUTHENTICATED right after a failover. |
| The M1 client dials one address and sends no `(client_id, seq)`. | Part B, the client | It can't follow a redirect, and a retry isn't deduplicated. |
| Tokens are stored in plain text in `log.bin`. | not planned | Anyone who can read the data dir can use a live session. A hash would need the dedupe response to carry the token anyway. |
| `client_id` is a bearer value. Knowing someone's `client_id` and `seq` returns their stored response, including a Login token. | not planned | Needs the client_id itself to leak. It is 128 random bits, kept by the client process. Binding it to the session would close it. |
| The dedupe table grows by one entry per client forever. | not planned (no snapshots, P11) | Memory grows with the number of distinct clients, not with requests. |
| `serve.py`'s peer port is plain TCP. | not in scope (A9) | Anyone who can reach it can send Raft RPCs. |
