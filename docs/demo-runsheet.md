# Demo runsheet: 5-10 minute video

The script for the demonstration video (problem statement §9: "multi-node
operation, concurrent bidding, and recovery from leader failure"; plus the
assignment's LLM, payment and fault-tolerance items). Each block gives the
time, what to type, what to say, what should be on screen, and the expected
output. Expected output is copied from a real run of `scripts/demo.py` through
`scripts/demo.ps1` (see below) or from the saved chaos run. Where a value
changes from run to run, the block says so.

**⚠ REHEARSE** marks a step that has not been run exactly as written here.

---

## What has and hasn't been run

| piece | status | evidence |
|---|---|---|
| `scripts/demo.ps1 -Pause 0.5` (dashboard window + `demo.py`), with the real LLM | **run end to end on 2026-11-10**, exit 0, all 10 properties PASS, 90.16 s | `runs/demo-20261110-130952/` (local only; output quoted below) |
| `demo.ps1 -Pause 4` (the presenting speed) | **not run.** Only the pauses differ. Estimate, not measured: about 3.5 s more per step, 24 steps, so roughly 3 minutes in all | — |
| `scripts/chaos.py --duration 600 --fake-llm`, seed 396704 | run on 2026-11-10: all 10 PASS | `runs/chaos-396704-20261110-125757/`; [testing-and-chaos.md](testing-and-chaos.md) §6.1 |
| `python -m checker runs\chaos-634526-…` (the earlier 8-property run) | re-run on 2026-11-10: all 8 PASS | — |
| `python -m checker runs\chaos-396704-…` | **not re-run after the run itself** (the run's own report is the evidence) ⚠ REHEARSE | — |
| `scripts/chaos.py --seed 5 --duration 45` live | run on 2026-11-09 with older settings (before payments); not re-run | `runs/chaos-5-20261109-215202/` |

---

## Before recording (not on video)

1. Close every cluster window from earlier runs. The demo uses the
   `run_cluster.ps1` ports (peer 50061-50063, client 50071-50073, LLM 50052),
   and the chaos run uses 50081-50083 / 50091-50093 / 50059. Check that
   nothing is listening:
   ```powershell
   netstat -ano | findstr ":5006 :5007 :5008 :5009 :50052"
   ```
   This should print nothing in the LISTENING state. A hand-started
   `llm_server.serve` also holds 50052: stop it, the demo starts its own.
2. Don't run the test suite during recording. It binds local ports, and
   Windows can hand one of the fixed ports to it (testing-and-chaos.md Known
   gaps 13).
3. **Ollama must be running and the model pulled** (README, "The LLM"). The
   demo warms the model itself as its first step (6.13 s in the recorded run;
   15.75 s on the first call after a fresh Ollama start). Keep the model loaded:
   Ollama unloads an idle model after a few minutes, and the next call is slow
   again. `demo.py --no-llm` shows the degraded path instead.
4. Window layout: terminal A (left, large) for commands; the dashboard window
   that `demo.ps1` opens (right); terminal B for the chaos re-check. The
   dashboard window stays open after the demo: close it yourself.
5. Font size up. `demo.ps1 -Pause 4` is the slower setting meant for presenting.

---

## 0:00-0:45 Intro

**Screen:** README.md, the architecture diagram.

**Say:** "This is an online auction replicated on three servers with Raft,
written from the paper, no library. Every change, a login, a new auction, a
bid, a payment, is a command in the Raft log, applied by the same
deterministic `apply()` on every node. A separate LLM server, on its own
process and port, answers questions and writes descriptions and summaries, but
only its text ever enters the log, never the call. I'll bid, ask the assistant,
kill the leader, pay the winner into escrow, kill the leader again, and have a
checker verify nothing was lost or duplicated."

---

## 0:45-1:15 Start the cluster

**Type (terminal A):**
```powershell
.\scripts\demo.ps1 -Pause 4
```
It opens the dashboard in a new window, then runs `scripts\demo.py --pause 4`
here. Everything to the checker is `demo.py`. **Fallback:** run the dashboard
and `demo.py` by hand in two windows:
```powershell
.venv\Scripts\python.exe scripts\dashboard.py --base-peer 50061     # window 2
.venv\Scripts\python.exe scripts\demo.py --pause 4                   # window 1
```

**Expected (terminal A), recorded run:**
```
=== [   0.0s] starting LLM + 3 nodes (logs and data under ...\runs\demo-<time>)
  leader: n3

=== [   1.4s] warm the model: the first call after Ollama loads the weights is slow
  (model warm-up: 6.13 s)
```
The leader's name varies. The warm-up time depends on whether Ollama still has
the model loaded.

**Say:** "Three separate processes plus the LLM server. Each node has a client
port and a peer port for Raft, and keeps its term, vote and log on disk,
fsync'd before it replies."

---

## 1:15-1:45 Log in and create an auction (the LLM writes the description)

**Expected:**
```
=== log in: nisarg (seller), karan, mrudul, tanmay -- one client each
  nisarg  login         -> ok       your session is created
  ...
=== nisarg creates auction 1, closing in 60 s (the LLM writes the description)
  nisarg  create        -> ok       Auction created successfully by nisarg
  (create auction incl. LLM description: 1.83 s)
  description (replicated as data): 'A running Seiko clock from the 1970s. ...'
```
The description wording varies.

**Say:** "The leader asked the LLM server for the description, once, then
proposed the auction with that text as an ordinary string. The followers never
call the model, and replaying the log never will."

---

## 1:45-2:15 Bid, including two equal bids at once

**Expected:**
```
  karan   bid      100  -> ok       Bid of 100 added [high 100 by karan]
  mrudul  bid      150  -> ok       Bid of 150 added [high 150 by mrudul]
  karan   bid      200  -> ok       Bid of 200 added [high 200 by karan]
  tanmay  bid      200  -> rejected Bid of 200 is not added because current highest bid is 200 [high 200 by karan]
```
Which of karan and tanmay wins the race varies, and the two lines may print in
either order.

**Say:** "Two threads sent the same amount at once. Raft puts them in one order,
and `apply()` accepts the first and refuses the second, on every node."

---

## 2:15-2:45 The assistant answers about the live auction

**Expected:**
```
=== tanmay asks the assistant about the live auction (the leader sends the model its current state)
  (assistant FAQ: 1.14 s)
  Q: Can I bid one more than the current high bid?
  A: 'Yes, you can bid one more than the current high bid. That would be 201.'
```
The wording varies; the number comes from the live state (the high bid was 200).

**Say:** "The leader first confirms it is still the leader, reads the current
auction from its replicated state, and sends it to the LLM server as context.
The model is told the bidding rules and the live figures, so it answers from
the auction, not from memory. Nothing is written to the log. If the LLM server
were down, the answer would be a clear message and the auction would carry on."

---

## 2:45-3:30 Kill the leader; the client carries on

**Expected:**
```
=== KILL -9 the leader n3 (term 1) mid-auction
  n3 killed
=== keep bidding: ...
  mrudul  bid      250  -> ok       Bid of 250 added [high 250 by mrudul]
  (that bid took 2.09 s including failover)
  new leader: n1
  tanmay  bid      300  -> ok       ...
  karan   bid      300  -> rejected Bid of 300 is not added because current highest bid is 300 [high 300 by tanmay]
```
Node names and terms vary; expect about 2 s.

**Dashboard events:** `n3 DOWN`, `no leader`, `leader n1 (term 2)`.

**Say:** "That was `TerminateProcess`, a real kill -9. The followers stopped
hearing heartbeats, one timed out, asked for votes in a higher term, and won.
The client got an error, retried with the same serial number, and found the new
leader. Raft replaced the leader in under a second; most of the two seconds is
Windows taking 2 seconds to refuse a connection to the dead port."

---

## 3:30-4:00 Restart the dead node

**Expected:**
```
=== restart n3: it replays its log from disk and catches up from the new leader
  n3 back; replicas agree: True
  karan   bid      350  -> ok ...
  mrudul  bid      400  -> ok ...
```
**Say:** "The restarted node read its term, vote and log from disk, rebuilt its
state by replaying the log, and the leader sent what it missed."

---

## 4:00-5:00 The close and the LLM-written summary

**Expected (recorded run: the wait was 47 s; the summary appeared 5.56 s after):**
```
=== waiting 47 s for the close (the leader's closer proposes CloseAuction)
  tanmay  bid      500  -> rejected Auction is already closed tanmay [high 400 by mrudul]
  closed=True winner='mrudul' bids=[('karan', 100), ('mrudul', 150), ('karan', 200), ('mrudul', 250), ('tanmay', 300), ('karan', 350), ('mrudul', 400)]

=== the leader's summary worker asks the model for the result summary and replicates the text
  summary after 5.6 s: 'A vintage clock auction ended with a winning bid of 400. Seven bids were placed: ...'
```
That line is from the recorded demo run, which used the earlier summary prompt (it listed the
bids and times). The prompt was changed afterwards (decisions.md C23). One live call with the
new prompt gave: 'A vintage clock auction ended with 7 bids from 3 bidders. The opening bid
was 100 by karan, and the closing bid was 40. Mrudul won with a bid of 400.' ⚠ REHEARSE: the
wording varies, and note the wrong closing bid in that call.
**Use the wait to point at the dashboard:** the same `digest @ index` on all
three rows, byte-identical state; it would say DIVERGED otherwise.

**Say:** "Every node runs the closer but only the leader proposes the close,
stamped with the leader's clock. Then the leader's summary worker finds a closed
auction with no summary, asks the model for one, and replicates the text. If the
leader had died between the close and the summary, the new leader would find the
same gap by scanning the state and fill it." **Be honest on camera:** the summary
prompt now gets computed facts and no times, but a language model can still get a
number wrong (the one live check wrote 40 for 400). Its text is stored, not trusted.

---

## 5:00-6:00 Pay into escrow, kill the leader again, read the same escrow

**Expected:**
```
=== mrudul (the winner) pays 400 into ESCROW; tanmay (not the winner) tries and is refused
  tanmay  pay      400  -> rejected tanmay is not the winner: only the winner can pay [reference -]
  mrudul  pay      400  -> ok       400 is held in escrow for auction 1 [reference PAY-3A27079225C5]
  escrow now: status HELD, amount 400, payer mrudul, seller nisarg, payment_ref PAY-3A27079225C5, settlement_ref -

=== KILL -9 the leader n1 (term 2) AFTER the payment
=== the new leader shows the SAME escrow; then nisarg (the seller) releases it
  escrow on the new leader: status HELD, amount 400, payer mrudul, seller nisarg, payment_ref PAY-3A27079225C5, settlement_ref -
  same record as before the kill: True
  nisarg  release       -> ok       Escrow released to the seller [reference REL-3A27079225C5]
  escrow (seen by the winner): status RELEASED, ...
```
The reference value varies: it is derived from the client's serial number.

**Say:** "Only the winner can pay, and only the winning bid. The leader called
the mock payment gateway once, before proposing, and the gateway's reference
travels in the command as plain data, so the other two nodes never touch the
gateway. I killed the leader again right after the payment: the new leader
reports the identical escrow record. The seller then releases it. HELD to
RELEASED or REFUNDED, and those two are final."

---

## 6:00-6:20 Price trends

**Expected:**
```
=== price trends over the closed auctions: statistics in code, wording by the model
  (price trends: 2.28 s)
  auctions 1, with bids 1, lowest 400, highest 400, total bids 7
  summary: 'There is 1 closed auction with bids; the lowest and highest winning bid are both 400. ...'
```
**Say:** "The statistics are computed in code and returned with the model's
wording. With one closed auction there is no trend, and it says so."

---

## 6:20-6:50 The checker on the demo run

**Expected (recorded run; counts vary except history-well-formed 18):**
```
PASS  history-well-formed  (checked 18)
PASS  acked-writes-on-every-node-exactly-once  (checked 42)
PASS  replicas-identical  (checked 3)
PASS  bids-strictly-increasing-winner-is-max  (checked 3)
PASS  no-bid-accepted-after-close  (checked 28)
PASS  rejections-consistent-with-final-state  (checked 3)
PASS  unknown-outcome-at-most-once  (checked 0)
PASS  at-most-one-leader-per-term  (checked 6)
PASS  escrow-legal-and-at-most-one-payment  (checked 6)
PASS  summary-at-most-one-on-closed-auction  (checked 3)
ALL PROPERTIES PASS

DEMO: ALL PROPERTIES PASS
```
The last green line comes from `demo.ps1`. **Say:** "`checked 0` for unknown
outcomes means this short demo never produced one, so that property passes
vacuously. The chaos run exists for that case."

---

## 6:50-8:20 The 10-minute chaos run, re-checked live

**Type (terminal B):**
```powershell
Get-Content runs\chaos-396704-20261110-125757\report.txt
.venv\Scripts\python.exe -m checker runs\chaos-396704-20261110-125757       ⚠ REHEARSE (not re-run after the run)
```
**Expected** (the report from the run):
```
PASS  history-well-formed  (checked 11978)
PASS  acked-writes-on-every-node-exactly-once  (checked 29649)
PASS  replicas-identical  (checked 3)
PASS  bids-strictly-increasing-winner-is-max  (checked 207)
PASS  no-bid-accepted-after-close  (checked 38699)
PASS  rejections-consistent-with-final-state  (checked 1982)
PASS  unknown-outcome-at-most-once  (checked 189)
PASS  at-most-one-leader-per-term  (checked 88)
PASS  escrow-legal-and-at-most-one-payment  (checked 385)
PASS  summary-at-most-one-on-closed-auction  (checked 207)
ALL PROPERTIES PASS
```
**Say (numbers from [testing-and-chaos.md](testing-and-chaos.md) §6.1):** "Ten
minutes, 57 real faults: 18 leader kills, 9 times two nodes at once, plus
freezes. 11 978 writes, among them 64 accepted payments and 64 settlements. 63
bids ended 'unknown'. 69 auctions, all closed, each with exactly one summary;
escrow released on 54, refunded on 10. All three nodes at the same index with
identical state. The checker is separate from the run, and each property has a
test that feeds it a broken input to show it can fail. One honest limit: the
LLM in that run was a canned-answer stand-in, so it tests the distributed
behaviour around the LLM, not the model."

**Optional if time (the earlier 8-property run):** `runs\chaos-634526-…`.

---

## 8:20-9:20 Close: what it doesn't do

**Screen:** README "Known limitations".

**Say:** "Honest limits: all faults are process crashes on one machine, so no
network partitions between real processes, no clock skew, no disk faults. The
payment gateway is a mock, and a payout is not atomic with the Raft commit. The
model's text is stored, not verified. `ProcessBusinessRequest` takes an untyped JSON
payload and dispatches to the typed handlers. Catch-up is one RPC per missing
entry, a restart replays the whole log, and the client sees about 2 seconds of
failover on Windows."

---

## If something goes wrong on camera

| symptom | likely cause | do |
|---|---|---|
| `could not bind … port` at start | a port is still held by an earlier run (a hand-started LLM server holds 50052), or Windows lent it to another connection (gap 13) | stop, close old windows, wait a few seconds, re-run |
| `leader:` never prints | nodes didn't start; check `runs\demo-<time>\logs\n*.log` | read the log's last lines |
| the warm-up takes 15 s or more | Ollama had unloaded the model | say so; it is a one-time cost |
| `(no answer: the assistant is unavailable ...)` | Ollama is not running | start it; the auction carried on, which is also worth saying |
| the kill step's bid takes about 2 s | expected (T18) | say so |
| `DIVERGED` on the dashboard | a real bug. It has never been seen in the recorded runs | stop recording, keep the run directory |
| the checker prints `FAIL` | read the evidence lines under the property | keep the run directory |
