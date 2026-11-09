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

---

# Part B: reads, the client, failover

Part A's first three gaps are closed below: reads (section 9), a brand-new
leader that hasn't applied a Login yet (section 9c), and the client
(section 10).

## 9. Reads: leader-only, through read-index (`raft/node.py:360`) — A14, A15

Resolving **seam 5**: "`GetAuction` / `ListAuctions` read local state. Decide
leader-only vs. follower reads, and log it." The decision is **leader only**,
with no log entry, using §8's two precautions (A15).

### 9a. Why a plain local read is wrong, even on the leader

A follower's state lags, so it can miss a bid that was already acknowledged.
That one is obvious. The subtle case is a leader. §8: "the leader responding
to the request might have been superseded by a newer leader of which it is
unaware." Partition n1 away. n2 is elected and accepts a bid of 300. n1 has
heard nothing, still thinks it leads, and its state says 150. A client that
already saw "300 accepted" then reads 150. That is a stale read.
`test_a_deposed_leader_that_has_not_noticed_cannot_serve_a_stale_read`
builds exactly that case.

### 9b. The rule and the code

§8: "First, a leader must have the latest information on which entries are
committed. [...] it needs to commit an entry from its term. Raft handles this
by having each leader commit a blank no-op entry into the log at the start of
its term. Second, a leader must check whether it has been deposed before
processing a read-only request [...] by having the leader exchange heartbeat
messages with a majority of the cluster before responding to read-only
requests."

`read_index(timeout)`, step by step:

| step | lines | what |
|---|---|---|
| 1 | 393-396 | Stopped, or not leader: `NOT_LEADER`, with `leader_hint`. |
| 2 | 399 | **Precaution one.** Wait until `commitIndex >= _term_start_index`, the index of this term's no-op (set at line 574). |
| 3 | 402 | `read_index = commitIndex`. |
| 4 | 404-409 | **Precaution two.** `_read_round += 1`, then send AppendEntries to every peer that has none in flight. |
| 4 | 412 | Wait until `_round_acks(round) >= majority`, counting ourselves. |
| 5 | 415 | Wait until `lastApplied >= read_index`. |
| | 418 | `OK(read_index)`. The caller now reads its own state. |

Steps 2 and 4 need us to still be leader of the same term
(`_read_wait`, line 425, returns LEADERSHIP_LOST otherwise). Step 5 doesn't:
entries up to `read_index` are committed, so applying them is correct even if
we are deposed meanwhile.

**Why step 2 is needed.** Leader Completeness says a new leader *has* every
committed entry. It doesn't say the leader *knows* they are committed. In
`test_a_new_leader_waits_for_its_no_op_before_choosing_the_read_index`, n1
commits "a" with n3. n2 stores "a" but never learns it committed. n2 is then
elected with commitIndex *below* "a". Taking commitIndex as the read index
straight away would leave out an acknowledged write. The no-op commit is the
moment the new leader's commitIndex catches up (R9).

**Why step 4 makes the read safe.** A peer's reply in our term means that
when the peer answered, which was *after* the read began, it had not moved to
a later term. For a newer leader to exist, a majority must have voted in a
later term, and any two majorities share a node. So if a majority answered
us, no newer leader existed when the read began, and nothing was committed
that we lack. **Any** reply in our term counts, including a consistency-check
refusal, because "still in your term" is all the read needs (line 873).

**How "an AppendEntries sent after the read began" is tracked.** There is one
counter, `_read_round`, bumped per read. There is at most one AppendEntries in
flight per peer (R7), so a single slot per peer, `_sent_round[peer]`,
remembers which round that RPC was built in (line 590). When the reply comes
back in the same term, `_acked_round[peer]` is raised to that round
(line 873). A peer whose in-flight RPC was built *before* the read gets one
more AppendEntries as soon as that reply arrives (line 906), not at the next
tick. Acks are reset when a term starts (line 563): an answer to a previous
term's leadership proves nothing about this one.

**Nothing is written to the log**
(`test_read_index_writes_nothing_to_the_log`).

**Attribution.** The paper gives the two precautions. The exact ordering
(record commitIndex *first*, then confirm) is the "ReadIndex" algorithm from
Ongaro's PhD thesis, §6.4. I didn't quote that from a file in this repo.

**The tests detect the bugs.** I removed each precaution in turn (step 2 made
`lambda: True`, then step 4 made `lambda: True`) and ran the suite.
- Without step 2: exactly
  `test_a_new_leader_waits_for_its_no_op_before_choosing_the_read_index`
  failed.
- Without step 4: exactly
  `test_a_deposed_leader_that_has_not_noticed_cannot_serve_a_read` failed.

### 9c. Where it is used

- `GetAuction` / `ListAuctions` (`server/auction_servicer.py:176`, `:228`)
  call `replicator.read_barrier()` (`replication/replicator.py:110`) first.
  NOT_LEADER becomes a redirect. Every other failure raises
  `ReadNotConfirmedError` (line 62), which becomes UNAVAILABLE "read not
  confirmed; retry". It never returns stale data.
- **The interceptor** (`server/auth_interceptor.py:64` `_resolve`, A16). An
  unknown token on the leader triggers the same barrier, then a second look.
  A new leader that hasn't applied a recent Login catches up first, so the
  user isn't refused
  (`test_a_new_leader_accepts_a_token_it_has_not_applied_yet`, built with
  the same "hold the commitIndex back" trick). If the barrier itself fails,
  the answer is UNAVAILABLE, never UNAUTHENTICATED: we couldn't find out, so
  we must not say no. The lookup moved from `intercept_service` into the
  call, because the barrier blocks and has to abort through the call's
  context.

---

## 10. The client (`client/auction_client.py`) — A17, A18

§8: "If the client's first choice is not the leader, that server will reject
the client's request and supply information about the most recent leader it
has heard from [...] If the leader crashes, client requests will time out;
clients then try again with randomly-chosen servers."

`ClusterClient` (line 97) holds every node's client address. `_call`
(line 135) works like this:

| reply | action |
|---|---|
| OK | done; remember this node |
| FAILED_PRECONDITION with a `leader-addr` hint (line 149) | go there **at once**. Hints are followed at most `len(addresses)` times in a row, so stale hints that point at each other can't spin. |
| FAILED_PRECONDITION, no hint (an election is on) | back off, next node |
| UNAVAILABLE / DEADLINE_EXCEEDED (line 156) | back off, next node |
| anything else (UNAUTHENTICATED, ...) | raise; don't retry |

Backoff starts at 0.05 s, doubles, and is capped at 1 s. After 30 s the call
raises `ClusterUnavailable`.

**The serial number.** A write takes its `seq` **once**, before the first
attempt (`_next_seq`, line 125). Every retry resends the *same request
object*, so the same `(client_id, seq)`. That is the client half of §8, and
`test_a_write_retried_after_an_unknown_outcome_happens_once` checks it
(several attempts, one distinct seq, one bid). Reads carry no seq.

One `ClusterClient` per user, one write at a time (A18). The M1 CLI flow
(`main()`) now builds four of them, one per user, against
`--nodes 127.0.0.1:50071,...`.

---

## 11. A real trace: a bid while the leader is killed

Three real `server.serve` processes (peer ports 50061-50063, client ports
50071-50073, FileStorage in temp dirs) and three `ClusterClient`s. Login ×3,
create, bid 100, bid 150. Then **`TerminateProcess` on the leader** (n1),
with karan's next bid (200, `seq=3`) already on its way. These are the real
log lines, trimmed only of the repeated startup lines.

```
                                       Indexes 1-7: no-op, 3 Logins, Create,
                                       bid 100, bid 150. All committed.
21:14:24.142 [n1] term 1: appended index 8        <- karan 200, seq 3
21:14:24.143 [n2] appended 8..8 from leader       <- n2 has it; n3 does not
=== 21:14:24 KILL n1 (TerminateProcess) ===
CLIENT PlaceBid -> :50071: UNAVAILABLE (Connection reset ...); trying :50072 in 0.10s
CLIENT PlaceBid -> :50072: not leader, redirected to 127.0.0.1:50071
21:14:25.000 [n3] term 2 FOLLOWER -> CANDIDATE: election timeout
21:14:25.001 [n2] term 2 FOLLOWER -> CANDIDATE: election timeout
21:14:25.004 [n2] term 2: refused vote for n3 (already voted for n2 in term 2)
21:14:25.004 [n3] term 2: refused vote for n2 (already voted for n3 in term 2)
21:14:25.533 [n3] term 3 CANDIDATE -> CANDIDATE (was term 2): election timeout
21:14:25.538 [n2] term 3: refused vote for n3 (candidate log (term 1, index 7)
                                              is behind ours (term 1, index 8))
21:14:25.946 [n2] term 4 CANDIDATE -> LEADER: won election with 2 votes
21:14:25.947 [n2] term 4: appended no-op at index 9
21:14:25.960 [n2] n3 has no (index 8, term 1): nextIndex -> 8
21:14:25.962 [n3] appended 8..9 from leader
21:14:25.963 [n2] term 4: commitIndex 7 -> 9
CLIENT PlaceBid -> :50071: UNAVAILABLE (Connection refused ...); trying :50072 in 0.20s
21:14:26.489 [n2] term 4: appended index 10       <- the retry, same seq 3
21:14:26.492 [n2] term 4: commitIndex 9 -> 10
bid karan 200: True Bid of 200 added high=200 by karan
bids now: [('karan', 100), ('mrudul', 150), ('karan', 200)]
karan's attempts: [(:50071, seq 3), (:50072, seq 3), (:50071, seq 3), (:50072, seq 3)]
```

What happened, step by step:

1. **The kill lands mid-bid.** n1 had appended karan's bid at index 8 and
   sent it to n2, but it died before hearing back from a majority. The client
   sees "connection reset", so UNAVAILABLE: *outcome unknown*. Is the bid
   placed? It is on 2 of 3 logs (n1, now dead, and n2), and it isn't
   committed.
2. **The client moves on.** It backs off 0.10 s and tries n2. n2 still
   believes n1 is leader (term 1), so it redirects to n1. The client follows,
   and the dead n1 refuses the connection. The connect attempt to the dead
   node is what took ~2 s.
3. **Split vote, then the election restriction.** n2 and n3 both time out
   into term 2 and vote for themselves, so nobody wins. In term 3, n3 asks n2
   for a vote. n2 refuses, *because n3's log (term 1, index 7) is behind its
   own (term 1, index 8)*. That is §5.4.1 protecting index 8, an entry that
   might be committed. In term 4, n2 wins.
4. **Repair and indirect commit.** n2 appends its no-op at 9. n3 refuses
   prevIndex 8 ("has no (index 8, term 1)"), so nextIndex backs off to 8
   (R6). n3 then takes 8..9. Index 9 is a term-4 entry on a majority, so
   commitIndex goes **7 → 9**. Index 8, karan's term-1 bid, commits
   *indirectly* (Figure 8 rule). n2 applies it: dedupe has karan's
   client_id at seq 2, and 3 > 2, so it **executes**. "Bid of 200 added" is
   stored under `(karan, 3)`.
5. **The retry is answered, not executed.** The client's fourth attempt
   reaches n2 with the **same seq 3** and lands at index 10. When it is
   applied, seq 3 == 3, so `apply_fn` returns the stored response. The bid is
   placed **once**: `bids == [100, 150, 200]`. The reply's high bid
   (200, karan) comes from the stored `Applied`.
6. **Same token throughout.** karan never logged in again. n2 already had
   karan's Login (index 3) applied.

Without dedupe, step 5 would execute index 10 too. It would be refused ("Bid
of 200 is not added because current highest bid is 200"), so karan would be
told **no** about a bid that was in fact placed.

---

## 12. The five M1 → M2 seams

| # | seam (CLAUDE.md) | resolved by | where |
|---|---|---|---|
| 1 | The closer calls `apply(CloseAuction)` on every node | Runs everywhere, proposes only on the leader, stamped with the leader's clock, one close in flight; `apply()` rejects duplicates | §6, A7, `server/closer.py:92` |
| 2 | Handlers call `apply()` directly; PlaceBid reads the high bid after | `check_leader → (LLM) → stamp → submit → wait`; the high bid comes from `Applied` | §2, §4, A8, A10 |
| 3 | SessionStore not replicated | `Login`/`Logout` are commands; the token is minted on the leader before proposing | §5, A3 |
| 4 | Commands have no wire/disk encoding | `proto/commands.proto` + `replication/codec.py`, with pinned field numbers | §3, A1 |
| 5 | Reads are local | Leader only, through `read_index` (§8's two precautions) | §9, A14, A15 |

**Seam 4, confirmed.** Every entry in `log.bin` is a `Command` proto.
`test_a_full_restart_rebuilds_identical_auction_and_session_state` crashes
all three nodes and replays every command type from disk. It gets
byte-identical state back, with the description, token and microsecond
timestamps intact, and a pre-restart retry still dedupes.

---

## 13. Tests in this chunk

New, all written before their code and shown red first, except
`test_failover_grpc.py` (written after the client existed; `test_client.py`
is its red counterpart):

| file | what it pins |
|---|---|
| `test_codec.py` | round trip of every command type, pinned field numbers, garbage fails; the M1 µs bug (strict xfail) |
| `test_state_machine.py` | dispatch, §8 dedupe (repeat, stale, per client, no-client, rejected remembered), high bid, fail-stop, determinism |
| `test_replicator.py` | Applied / NotLeaderError with client address / OutcomeUnknownError (timeout, STOPPED) |
| `test_sessions_on_raft.py` | Login replicated, redirect, retry returns the first token, Logout replicated, unknown token on a follower vs the leader |
| `test_auction_on_raft.py` | bid to a follower redirected, then identical state on all 3; retried CreateAuction returns the original success; LLM down; follower never calls the LLM |
| `test_closer.py` (adapted) | due scan, leader-only, one in flight, re-propose after failure, exactly once on a cluster, the new leader closes the overdue |
| `test_serve_grpc.py` | real gRPC: peer port needs no token, client port does; redirect metadata over the wire |
| `test_restart_replay.py` | all 3 crash, all 3 replay: identical auctions, sessions and dedupe (MemoryStorage: strict xfail) |
| `test_raft_read_index.py` | read covers writes; follower refuses; deposed leader can't read; new leader waits for its no-op; nothing written to the log |
| `test_reads_on_raft.py` | reads redirect on a follower; stale read refused; a new leader accepts a token it hadn't applied yet |
| `test_client.py` | follows the hint; kill the leader with the same token and no lost bid; retry keeps its seq; UNAUTHENTICATED not retried; give up |
| `test_failover_grpc.py` | the kill-the-leader scenario over real gRPC |

**M1 tests adapted:** only `test_closer.py` (section 8).

**Timing.** As in the Raft tests, the leader is chosen by giving one node a
short election timeout, and "lagging" or "deposed" states are built by
*losing messages*, not by racing. The tests that do run real clocks
(`test_the_poll_threads_close_once_on_a_live_cluster`, the two gRPC files)
only wait with bounded `wait_until`. Their assertions don't depend on how
fast things happen. `test_serve_grpc.py` picks ports by binding port 0 and
closing it, so another process could grab the port in between. That would be
a loud bind error, not a wrong result.

---

## 14. How to see it yourself

```powershell
.\scripts\run_cluster.ps1            # LLM server + n1..n3, data in data\n1..n3
.venv\Scripts\python.exe -m client.auction_client
```

Close the leader's window in the middle of the demo. The client logs its
redirects and retries, and the bids continue. To restart a node, run its line
from `.\scripts\run_cluster.ps1 -DryRun`. It replays its log from `data\` and
catches up.

---

## 15. Known gaps (whole chunk)

| gap | fixed by | what goes wrong today |
|---|---|---|
| Tokens are plain text in `log.bin` | not planned | Read access to a data dir means access to live sessions. |
| `client_id` is a bearer value (A5) | not planned | Someone who learns a client's id and seq can fetch its stored response, including a Login token. |
| The dedupe table and the session table grow forever; no snapshots (P11) | not planned | Memory grows with distinct clients and logins; replay time grows with the log. |
| A garbage token sent to the leader costs one read barrier (a heartbeat round) | not planned | A cheap way to make the leader send extra heartbeats. |
| The peer port is plain TCP (A9) | production: mTLS | Anyone who can reach it can send Raft RPCs. |
| No check-quorum: a partitioned leader keeps believing it leads | not planned | Its *reads* are safe (read-index times out). Its *writes* time out with UNAVAILABLE and the client moves on. It just wastes a few seconds per client. |
| A follower's leader hint can point at a dead leader (trace step 2) | not planned | One wasted connect attempt (~2 s on Windows) per client after a crash. |
| M1 bug: `common/time_conv.py:25` loses 1 µs on ~2% of datetimes | your decision (section 8) | A bid stamped 1 µs after close can be recorded at close and accepted. Determinism is unaffected. |
| The closer stamps with the leader's clock; leaders' clocks can differ | inherent | After a failover, `closed_at` follows the new leader's clock. Each close is still decided once, the same on every node. |
| `ClusterClient` is not thread-safe; one write at a time per client_id | by design (A18) | Two concurrent writes on one client could make the lower seq "stale". |

## 16. Things I am not fully sure about

- **The read-index argument with in-memory acks.** A reply counts for a
  round if its RPC was *built* after the round began (`_sent_round` is set in
  `_prepare_appends`). I believe "built after" is enough, because the peer
  answers after receiving it, which is after it was built. I'd still like
  this checked against the chaos harness in step 7.
- **Stale hints.** The cap of `len(addresses)` consecutive redirects is a
  judgement call, not something the paper prescribes.
- **2 s connect time to a dead node on Windows** (trace step 2). That is
  gRPC's connection behaviour, which I haven't tuned on the client channel.
  `raft/transport.py` caps reconnect backoff for peers, but the client
  channel uses defaults.
