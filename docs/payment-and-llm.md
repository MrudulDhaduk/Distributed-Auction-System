# Chunk 7: payment and escrow, the LLM in the app, checker and chaos

What this chunk adds, and why each piece sits where it does. Decisions with
their viva questions: [decisions.md](decisions.md) section "Payment, LLM
wiring, checker (chunk 7)" (C1 to C22). The requirement-by-requirement status
is [requirements-audit.md](requirements-audit.md).

There are no new Raft rules here. Raft (`raft/`, `replication/replicator.py`)
is untouched. Everything below is about the rule that sits on top of Raft:

> **Same log, same state.** `apply()` must be deterministic: no clock, no
> randomness, no I/O, no LLM call. Anything non-deterministic is taken once,
> by the leader, outside `apply()`, and replicated as plain data.

Figure 2 / §8 rules this chunk leans on:

| paper | rule | where it matters here |
|---|---|---|
| §8 | clients give commands serial numbers; "the state machine tracks the latest serial number processed for each client, along with the associated response. If it receives a command whose serial number has already been executed, it responds immediately without re-executing the request." | a retried `Pay`, `Release`, `GenerateDescription` is answered from the dedupe table (`replication/state_machine.py:94` `apply_fn`) |
| §8 | read-only operations: "a leader must check whether it has been deposed ... by exchanging heartbeat messages with a majority" | `read_barrier()` before every FAQ, trends, escrow read |
| Figure 2, Leaders | "respond after entry applied to state machine" | every write handler returns `apply()`'s verdict after `submit()` |

---

## 1. Escrow (R3)

### The state machine

`NONE -> HELD -> RELEASED | REFUNDED`. The last two are final.

| rule | code | in plain words |
|---|---|---|
| only after the auction is closed | `payment_rejection`, `auction/state.py:147` | no close, no payment |
| only the winner can pay | same function | `payer != winner.bidder` is refused |
| the amount equals the winning bid | same function | any other number is refused, with the real figure in the reason |
| no double pay | same function (`escrow_of(auction)["status"] != "NONE"`) | the first payment is kept, the second is refused |
| only HELD money can be settled, only by the seller | `settlement_rejection`, `auction/state.py:168` | release and refund share one rule |
| `apply()` does the transition | `isinstance(command, PayAuction)` branch, `auction/state.py:312`; `(ReleaseEscrow, RefundEscrow)` branch, `:333` | a missing gateway reference is also refused, after the real rule, so a client sees the real reason |

`payment_rejection` and `settlement_rejection` are functions, not code inside
`apply()`, because the handler pre-checks with the **same** function. The two
cannot drift apart.

Escrow lives in `auction["escrow"]`, created by the first `PayAuction` (C1).
`CreateAuction` is unchanged. `escrow_of()` (`:134`) returns the NONE record
for an auction nobody paid for.

### Where the money moves

Not in `apply()`. In `MockPaymentGateway` (`server/payment_gateway.py:62`),
called by the **leader's handler before it proposes** (C2). `authorize`
(`:77`) returns a `Receipt`; its `reference` rides on the command as plain data
(`PayAuction.payment_ref`). `apply()` stores the string. Replaying the log
never calls the gateway.

The reference is a hash of `(auction, payer, amount, client_id:seq)` (C4), so a
retry reaching a *new* leader's gateway gets the *same* reference.

### The write path, in order (`server/payment_servicer.py:83` `Pay`)

1. `check_leader()`: a follower redirects and touches nothing.
2. per-auction lock (C10).
3. `read_barrier()`: this leader has applied everything committed.
4. pre-check with `payment_rejection` under `state.lock`.
5. **if it passed**: `gateway.authorize(...)`. A decline replies at once and
   proposes nothing.
   **if it failed**: no gateway call; the command is proposed anyway with an
   empty reference (C3).
6. `submit(PayAuction(...), client_id, seq)`: propose, wait, `apply()` decides.
7. if the gateway authorized but `apply()` refused: `gateway.void(ref)` (C5).
8. reply with `apply()`'s result and the reference now stored on the record.

Step 5's "propose anyway" is the part to be able to explain. If the pre-check
simply returned "already HELD", a client retrying a payment that *did* commit
would be told "no" instead of the original "yes". Proposing lets the
replicated dedupe table answer. A refused command is just a log entry, like a
bid that was too low.

### Trace (values from the tests and the real gateway)

Auction 1, seller `nisarg`, bids `karan 100`, `mrudul 150`, closed. Leader n1.

```
client mrudul: Pay(auction 1, amount 150, client_id "m-1", seq 9)
n1 handler:    check_leader ok; read_barrier ok
               payment_rejection(auction 1, "mrudul", 150) -> None   (closed, winner mrudul, 150, NONE)
               gateway.authorize("mrudul", 150, 1, "m-1:9") -> PAY-0DA335CEADCD
               propose PayAuction(1, "mrudul", 150, "PAY-0DA335CEADCD")
n1,n2,n3:      apply() -> escrow {HELD, 150, mrudul, PAY-0DA335CEADCD, ""}
n1 handler:    replies success, reference PAY-0DA335CEADCD

n1 crashes. n2 wins the election.
client mrudul: Pay(auction 1, 150, "m-1", seq 9)   <- the same retry
n2 handler:    pre-check: "Escrow is already HELD"  -> no gateway call
               propose PayAuction(1, "mrudul", 150, "") at a new index
n2,n3:         apply_fn: (m-1, 9) is the latest seq for m-1 -> stored first response
n2 handler:    replies success, reference PAY-0DA335CEADCD (read from the record)

seller nisarg: ReleaseEscrow(auction 1)
n2 handler:    settlement_rejection -> None; gateway.release("PAY-0DA335CEADCD") -> REL-0DA335CEADCD
               propose ReleaseEscrow(1, "nisarg", "REL-0DA335CEADCD")
all nodes:     escrow {RELEASED, 150, mrudul, PAY-0DA335CEADCD, REL-0DA335CEADCD}
```

This is `tests/test_escrow_on_raft.py::test_pay_kill_the_leader_the_new_leader_shows_the_same_escrow`
(in memory) and `tests/test_payment_grpc.py` (real gRPC, real closer, leader
killed: 6.3 s, six repeats all green).

### Client (R11)

`ClusterClient.pay / release_escrow / refund_escrow / get_escrow`, and the
prescribed `post(token, type, data)` (`client/auction_client.py:292`) and
`get(token, type, params)` (`:325`): `post(token, "payment", {"auction_id": 1,
"action": "pay", "amount": 150})`, `get(token, "escrow", {"auction_id": 1})`.
They validate before making any RPC and send the token they are given.

---

## 2. The LLM in the app (R4, R5, R6, R9, R12)

The LLM server (`llm_server/`) stays what M1 made it: stateless, its own
process, port 50052, model `qwen3:4b-instruct-2507-q4_K_M` on Ollama. What is
new is that the application server now uses it.

```
client --gRPC--> leader (AssistantServicer) --gRPC, deadline 25 s--> LLM server --HTTP--> Ollama
                   |  read barrier, then reads the replicated auction state
                   |  (the "context"), calls the model OUTSIDE every lock
```

| job | RPC / worker | what it reads | what reaches the log |
|---|---|---|---|
| FAQ (R4) | `AssistantService.AskAssistant`, `assistant_servicer.py:78` | the auction's live state, as `context={"auction": ...}` | nothing |
| description on request (R5) | `GenerateDescription`, `:100` | item from state + the seller's attributes | `SetDescription(text)` as plain data |
| description at creation (R5, M1) | `AuctionServicer._describe` | the request's attributes | `CreateAuction(description=text)` |
| result summary (R6) | `SummaryWorker.tick`, `summarizer.py:88` | the closed auction and its bid history | `RecordSummary(text)` |
| price trends (R6) | `GetPriceTrends`, `:139` | the closed auctions | nothing |

### The rules, with the code

* **Leader only, after a read barrier** (C14). `check_leader()` then
  `read_barrier()` in every handler. A deposed leader cannot confirm itself and
  never builds a prompt from stale state. Followers redirect and never call
  the model.
* **No lock during the model call.** State is copied under `state.lock`, the
  lock is released, then the model is called. A call takes seconds.
* **A deadline on every call** (C12). `LLMClient` passes
  `timeout=APP_DEADLINE_SECONDS` (25 s, `llm_server/config.py`) to every stub
  call. It is below Ollama's own 30 s timeout, so the app gives up first.
* **A failure is a clear answer** (C13). `llm_context.failure_reason` turns
  UNAVAILABLE, DEADLINE_EXCEEDED or anything else into a sentence ending "the
  auction is unaffected", returned in `Result`. Not a gRPC error, because
  `ClusterClient` retries those on every node for 30 s.
* **`get_llm_answer` returns `(request_id, answer)`** (C12, R12). The pair
  comes from the response.

### The result summary and the failover (C16, C17)

`SummaryWorker` runs on every node, acts only while its node leads, and finds
work by **scanning the state** for "closed, no accepted summary". It remembers
nothing else, so there is no hand-over:

```
t0  n1 leads. Auction 1 closes (CloseAuction applied everywhere).
t1  n1 dies before its worker reaches auction 1.
t2  n2 is elected. n2's worker ticks: scan -> auction 1 closed, summaries == [].
    n2 snapshots the auction + bid history, releases the lock, asks the model
    (request_id minted here), proposes RecordSummary(1, text).
t3  all nodes apply it: auction["summaries"] == [text].
    Any later tick, on any node: summaries is non-empty -> nothing to do.
```
`tests/test_summarizer.py::test_a_new_leader_notices_a_closed_auction_the_old_leader_never_summarised`.

If two leaders both generate: `summary_rejection` (`auction/state.py:198`)
keeps the first `RecordSummary` and refuses the second, which the loser drops
(`test_if_another_writer_wins_the_race_its_text_stays_and_this_one_is_dropped`).

If the model is down: nothing is proposed, the auction stays closed, and that
auction is retried after 10 s (monotonic clock), not on every tick.

### Price trends: the server states the facts (C18, C20)

The statistics (counts, lowest/highest winning bid, mean, total bids) are
computed in code in `AssistantServicer` and returned with the text. The LLM
server's prompt builder (`build_trends_messages`) also works out the facts
itself and gives them to the model as a FACTS block. Found on the live model:
given only a list, it called a no-bid auction "the lowest winning bid is 0".
After the change the same input produced "The lowest winning bid is 40, the
highest is 210. Winning bids rose from 150 in auction 1 to 210 in auction 4."

### processBusinessRequest (R12, C19)

`BusinessService.ProcessBusinessRequest(request_id, payload, context)`
(`proto/business.proto`, `server/business_servicer.py`) is a real RPC on the app
server. It is a dispatcher: it parses the JSON payload, validates each field
(INVALID_ARGUMENT naming the field), and calls the existing handler with the
**same gRPC context**, so auth, identity, leader redirects, read barriers and
dedupe are the handlers' own and nothing is re-implemented. The handler's typed
response comes back in an `Any`. `ClusterClient.process_business_request` calls
this RPC and returns `(request_id, response)`; the token travels in metadata,
never in the payload or context. The typed RPCs remain (C19).

---

## 3. Checker and chaos (step 3)

Two new properties, each with tests that feed it broken input and expect a
failure (`tests/test_checker.py`, 20 new cases).

| property | what it checks |
|---|---|
| `escrow-legal-and-at-most-one-payment` | from every node's state: payer is the winner, amount is the winning bid, a settled record's settlement reference belongs to its own payment and to the right kind of settlement, nothing is in escrow on an open auction. From the clients' history: at most one acknowledged payment per auction and it is the one in the state (same reference, same payer); an acknowledged release/refund is what the state shows; never both a release and a refund; each refusal's stated reason agrees with the final state |
| `summary-at-most-one-on-closed-auction` | each auction has at most one accepted summary, none while open, none empty |

"At most one" is checkable because the state keeps accepted summaries as a
list (C17), and because escrow records carry the references of what they paid
and settled.

Chaos workload: `Payer` actors (one per user) pay the auctions they won, now
and then pay twice or pay an auction they did not win (both must be refused);
the `Seller` releases (80%) or refunds (20%) and now and then tries the other
settlement after (must be refused). `scripts/chaos.py --fake-llm` runs
`harness/fake_llm.py`, a canned-answer LLM server: see the run in
[testing-and-chaos.md](testing-and-chaos.md) and the verdict in the README.

---

## 4. Known gaps

What this chunk does not handle, and what would go wrong today.

1. **The gateway and the Raft commit are two systems.** A release or refund is
   asked of the gateway before it is proposed and cannot be undone. The
   per-auction lock plus the pre-check make a refused settlement impossible on
   one leader. Two *different* leaders' handlers racing across a failover could
   in principle both pay out; `apply()` would accept only one, and the other
   payout would be orphaned. Not tested. A real gateway needs a ledger.
2. **The gateway stub has no memory.** It cannot tell a hold that was already
   released from one that was not; `apply()` is the only once-only guard. It is
   one stub per node, in-process.
3. **No dispute path.** Only the seller can refund. The winner cannot.
4. **The model's text is not tested for truth.** The prompts are tested for
   structure (which turn holds which text) and the numbers are computed in code,
   but a live call can still word things wrongly. The result-summary prompt now
   gets only computed facts and no times (C23). In the one live call run after
   that change it listed no bids and gave no times, but wrote the closing bid as
   "40" when the facts said 400. A check in code that every number in a summary
   appears in the facts would catch that; it is not built.
5. **Model latency.** The first call after Ollama loads the weights took
   15.75 s; later ones 1 to 7 s on this machine. The 25 s deadline fits one
   cold start. A model slower than that degrades like an outage.
6. **One LLM request at a time per auction in the summary worker.** It
   summarises sequentially, so many auctions closing together queue behind the
   model.
7. **`request_id` is not on the typed RPCs.** `ProcessBusinessRequest` carries and
   echoes it; the typed RPCs identify a write by `(client_id, seq)` only. In
   `ProcessBusinessRequest` it is a correlation id, not a dedupe key.
   The payload is untyped JSON, validated by hand (C19).
8. **Tokens and payment references are in plain text in `log.bin`** (existing
   gap, now also payment references).
9. **A payment request that is refused still enters the log** (C3). Cheap, but
   the log grows with attempts.
10. **Reads of escrow, the FAQ and trends are leader-only** (A15), so they add
    load to the leader.
11. **The chaos run used a canned-answer LLM**, not the real model (C22). The
    real model is shown by the demo.
