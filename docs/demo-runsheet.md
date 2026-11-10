# Demo runsheet: 8-9 minute video

The script for the demonstration video (problem statement §9: "multi-node
operation, concurrent bidding, and recovery from leader failure"; plus the
assignment's LLM, payment and fault-tolerance items).

The video has five parts. Each block gives the time, what to type or show,
what to say, and the expected output. **Every "Expected" block is real output**
from the run named in it. Where a value changes from run to run, the block says
so.

| time | part | source of the footage |
|---|---|---|
| 0:00-0:45 | intro and architecture | README.md, the architecture diagram |
| 0:45-2:45 | the **LIVE** run, `demo.ps1 -Pause 4`, sped up in editing, ending on `DEMO: ALL PROPERTIES PASS` | screen capture of the live run |
| 2:45-6:45 | the **REPLAY** of that same run, slowed to 0.5x, explained marker by marker | `runs\demo-20261110-143237\replay.html` |
| 6:45-8:15 | the 10-minute chaos run and its checker | `runs\chaos-396704-20261110-125757\` |
| 8:15-9:00 | honest limits | README, "Known limitations" |

---

## What has and hasn't been run

| piece | status | evidence |
|---|---|---|
| `scripts/demo.ps1 -Pause 4` (the presenting speed), with the real LLM, after the event log was added | **run on 2026-11-10**, all 10 properties PASS, whole run 125.16 s, `DEMO: ALL PROPERTIES PASS` | `runs/demo-20261110-143237/` (local only; the output is quoted below) |
| `scripts/demo.ps1 -Pause 0.5`, with the real LLM | run on 2026-11-10, exit 0, all 10 PASS, 90.16 s | `runs/demo-20261110-130952/` |
| `scripts/replay.py` on the `-Pause 4` run | built and checked in **Microsoft Edge (headless), driven over the DevTools protocol**. Playwright is **not installed** on this machine, so it could not be used. 17 browser checks passed, no console errors, play/pause, slider drag, arrow keys, marker click and the end checker panel all checked | `runs/demo-20261110-143237/replay.html`, `replay-kill.png`, `replay-end.png` |
| the demo's **four-auction** version (background bidders, refunds) | **not built.** Blocked: the background users `bg1`-`bg6` do not exist on the server, and creating them needs a change to `server/`, which was not allowed | — |
| `scripts/chaos.py --duration 600 --fake-llm`, seed 396704 | run on 2026-11-10: all 10 PASS | `runs/chaos-396704-20261110-125757/`; [testing-and-chaos.md](testing-and-chaos.md) §6.1 |
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
   demo warms the model itself as its first step (4.50 s in the `-Pause 4`
   run). Keep the model loaded: Ollama unloads an idle model after a few
   minutes, and the next call is slow again. `demo.py --no-llm` shows the
   degraded path instead.
4. Window layout: terminal A (left, large) for commands; the dashboard window
   that `demo.ps1` opens (right). The dashboard window stays open after the
   demo: close it yourself.
5. Font size up. The replay is built for a 1920x1080 screen.
6. Make the replay for the run you will show, after that run:
   ```powershell
   .venv\Scripts\python.exe scripts\replay.py runs\demo-<time>
   ```
   It writes `runs\demo-<time>\replay.html`. Open that file in a browser.
   The replay is a file, so nothing needs to be running.

---

## 0:00-0:45 Intro and architecture

**Screen:** README.md, the architecture diagram.

**Say:** "This is an online auction replicated on three servers with Raft,
written from the paper, no library. Every change, a login, a new auction, a
bid, a payment, is a command in the Raft log, applied by the same
deterministic `apply()` on every node. A separate LLM server, on its own
process and port, answers questions and writes descriptions and summaries, but
only its text ever enters the log, never the call. I'll run the demo live, then
replay the same run from its recorded event log, then run a 10-minute chaos test
and its checker."

---

## 0:45-2:45 The LIVE run

**Type (terminal A):**
```powershell
.\scripts\demo.ps1 -Pause 4
```
It opens the dashboard in a new window, then runs `scripts\demo.py --pause 4`
here. The whole run takes about two minutes at this speed; speed it up in
editing if you need to, but keep the pauses in the recording, because the
replay uses the same timings.

**Expected (terminal A), the run recorded as `runs\demo-20261110-143237`:**
```
=== [   0.0s] starting LLM + 3 nodes (logs and data under E:\...\runs\demo-20261110-143237)
  leader: n1

=== [   5.1s] warm the model: the first call after Ollama loads the weights is slow
  (model warm-up: 4.50 s)

=== [  13.6s] log in: nisarg (seller), karan, mrudul, tanmay -- one client each
  nisarg  login         -> ok       your session is created
  karan   login         -> ok       your session is created
  mrudul  login         -> ok       your session is created
  tanmay  login         -> ok       your session is created

=== [  17.6s] nisarg creates auction 1, closing in 60 s (the LLM writes the description)
  nisarg  create        -> ok       Auction created successfully by nisarg
  (create auction incl. LLM description: 1.87 s)
  description (replicated as data): 'A running Seiko clock from the 1970s. Functional and operational as intended. Manufactured during the 1970s era. No additional details provided.'

=== [  23.5s] bids: karan 100, mrudul 150
  karan   bid      100  -> ok       Bid of 100 added [high 100 by karan]
  mrudul  bid      150  -> ok       Bid of 150 added [high 150 by mrudul]

=== [  27.5s] karan and tanmay both bid 200 at the same moment (exactly one can win)
  karan   bid      200  -> ok       Bid of 200 added [high 200 by karan]
  tanmay  bid      200  -> rejected Bid of 200 is not added because current highest bid is 200 [high 200 by karan]

=== [  31.5s] tanmay asks the assistant about the live auction (the leader sends the model its current state)
  (assistant FAQ: 1.17 s)
  Q: Can I bid one more than the current high bid?
  A: 'Yes, you can bid one more than the current high bid. That would be 201.'

=== [  36.6s] KILL -9 the leader n1 (term 1) mid-auction
  n1 killed

=== [  40.6s] keep bidding: the clients' next call hits the dead node, backs off, finds the new leader
  mrudul  bid      250  -> ok       Bid of 250 added [high 250 by mrudul]
  (that bid took 2.08 s including failover)
  new leader: n2
  tanmay  bid      300  -> ok       Bid of 300 added [high 300 by tanmay]
  karan   bid      300  -> rejected Bid of 300 is not added because current highest bid is 300 [high 300 by tanmay]

=== [  50.8s] restart n1: it replays its log from disk and catches up from the new leader
  n1 back; replicas agree: True

=== [  59.3s] more bids after the restart
  karan   bid      350  -> ok       Bid of 350 added [high 350 by karan]
  mrudul  bid      400  -> ok       Bid of 400 added [high 400 by mrudul]

=== [  63.3s] waiting 20 s for the close (the leader's closer proposes CloseAuction)
  tanmay  bid      500  -> rejected Auction is already closed tanmay [high 400 by mrudul]
  closed=True winner='mrudul' bids=[('karan', 100), ('mrudul', 150), ('karan', 200), ('mrudul', 250), ('tanmay', 300), ('karan', 350), ('mrudul', 400)]

=== [  87.6s] the leader's summary worker asks the model for the result summary and replicates the text
  summary after 0.0 s: 'A vintage clock auction ended with 7 bids from 3 bidders. The opening bid was 100 by karan, and the closing bid was 400 by mrudul, who won the item.'

=== [  91.6s] mrudul (the winner) pays 400 into ESCROW; tanmay (not the winner) tries and is refused
  tanmay  pay      400  -> rejected tanmay is not the winner: only the winner can pay [reference -]
  mrudul  pay      400  -> ok       400 is held in escrow for auction 1 [reference PAY-4D3DC12C785E]
  (payment: 0.00 s)
  escrow now: status HELD, amount 400, payer mrudul, seller nisarg, payment_ref PAY-4D3DC12C785E, settlement_ref -

=== [  95.6s] KILL -9 the leader n2 (term 2) AFTER the payment
  n2 killed

=== [  99.6s] the new leader shows the SAME escrow; then nisarg (the seller) releases it
  escrow on the new leader: status HELD, amount 400, payer mrudul, seller nisarg, payment_ref PAY-4D3DC12C785E, settlement_ref -
  same record as before the kill: True
  nisarg  release       -> ok       Escrow released to the seller [reference REL-4D3DC12C785E]
  escrow (seen by the winner): status RELEASED, amount 400, payer mrudul, seller nisarg, payment_ref PAY-4D3DC12C785E, settlement_ref REL-4D3DC12C785E

=== [ 107.7s] restart n2: it replays its log from disk and catches up from the new leader
  n2 back; replicas agree: True

=== [ 113.8s] price trends over the closed auctions: statistics in code, wording by the model
  (price trends: 2.36 s)
  auctions 1, with bids 1, lowest 400, highest 400, total bids 7
  summary: 'There is 1 closed auction with bids; the lowest and highest winning bid are both 400. Due to only one auction having bids, the direction of price change cannot be determined.'

=== [ 120.1s] checker: every property, on what the clients saw + every node's final state
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

timings of the slow steps:
  model warm-up                                4.50 s
  create auction incl. LLM description         1.87 s
  assistant FAQ                                1.17 s
  close -> summary visible                     0.00 s
  payment                                      0.00 s
  price trends                                 2.36 s
  whole run (including the pauses)           125.16 s

DEMO: ALL PROPERTIES PASS
```
The leader's name, the terms, the reference values and the LLM's wording change
from run to run. The lines that matter on camera are the two refusals (tanmay at
27.5 s and at 91.6 s), the second kill's escrow check, and the last line.

**Say at the end of the live run:** "Everything you just saw was printed by the
cluster and by the checker. Now I'll replay the same run from its event log, so
you can see the state of every node at each step."

---

## 2:45-6:45 The REPLAY of that run

**Open:** `runs\demo-20261110-143237\replay.html` in a browser, full screen.
The banner at the top says it is a replay of a recorded run. Press Space to
start, and set the speed to 0.5x. Use the arrow keys to move from one event to
the next, or click a marker on the timeline.

**What is on screen:**
- Three node cards: role, term, commit, applied and the first eight characters
  of the state digest. The leader has a purple border. A node that is killed
  turns grey and says DOWN.
- The big indicator on the right: **replicas identical** (green) when every live
  node has the same digest at the same index. It says DIVERGED (red) only if
  two live nodes at the same index disagree. Amber **catching up** means the
  live nodes are at different indexes, which is normal lag after a write.
- The auction card for the hero auction: item, open or closed, high bid and
  bidder, winner, escrow (NONE, HELD, RELEASED), and the description and summary.
- The hero auction's bid list. Accepted bids are normal text, refused bids are
  greyed out with the reason.
- Background bidders: the counters. In this run there are none, because the
  background bidders are not built yet (see the table at the top).
- The event log, which scrolls: step headings in bold, kills in red, elections
  in amber, escrow changes in green.

**The markers, in order** (the times are the replay clock, `mm:ss`, from the
recorded run):

| replay time | what happens | point at | say |
|---|---|---|---|
| 00:05 | `new leader n1 (term 1)` | the leader card | "Startup. The three nodes elect a leader for term 1: n1." |
| 00:13 | log in, four clients | the event log | "Each client is its own session. Logins are commands too, so every node knows the same sessions." |
| 00:17 | create auction 1 | the auction card, the description | "The LLM wrote this description once, when the auction was created. It is stored as text, and the followers never call the model." |
| 00:23 | bids karan 100, mrudul 150 | the bid list | "Normal bids. Each one is committed on a majority before it is answered." |
| 00:27 | karan and tanmay both bid 200 | the two rows at 00:27 and 00:31 | "The same amount, at the same moment. Raft puts them in one order. karan's is accepted; tanmay's is refused, because the high bid is already 200. The refusal is the same on every node." |
| 00:36 | the assistant answers | the event log, the FAQ line | "The assistant is told the live auction and answers from it: 'Yes... That would be 201.' Nothing is written to the log." |
| 00:36-00:41 | KILL -9 n1, then `new leader n2 (term 2)` | the n1 card turns grey, the n2 card gets the leader border | "A real kill -9, not a pause. The other two nodes stop hearing from n1, one of them times out, asks for votes in a higher term, and wins." |
| 00:50 | restart n1 | the n1 card comes back | "n1 reads its term, vote and log from disk, then catches up from the new leader." |
| 01:03 | the close is awaited | the auction card still OPEN, then CLOSED at 01:27 | "The closer runs on every node, but only the leader proposes the close." |
| 01:27 | CLOSED, mrudul wins at 400 | the auction card | "The close and the winner are part of the replicated state, so every node agrees on them." |
| 01:31 | the summary appears | the auction card, the summary line | "The leader asked the model for a summary and replicated the text. It is stored like any other data. The numbers in it are checked against the facts in code." |
| 01:35 | mrudul pays 400, tanmay refused | the escrow pill HELD | "Only the winner can pay, and only the winning amount. The leader calls the payment gateway once, and the gateway's reference travels in the command as plain data." |
| 01:39 | KILL -9 n2, then `new leader n3 (term 3)` | n2 turns grey, n3 gets the border | "The second kill, after the payment. The new leader is n3." |
| 01:45 | escrow read on the new leader | the escrow pill still HELD, same reference | "The same record, byte for byte: the escrow survived the leader's death." |
| 01:47 | nisarg releases the escrow | the pill turns RELEASED | "The seller releases it. RELEASED is final." |
| 01:51 | restart n2 | n2 comes back | "The last node back. Its state matches the other two." |
| 01:53 | price trends | the event log | "Statistics in code: one closed auction, lowest and highest winning bid 400. The model only words the summary." |
| 02:00 | the checker runs | the event log | "Every property the checker knows, run on what the clients saw and on each node's final state." |
| 02:05 | the checker panel | the property list and the banner | **Stop here on ALL PROPERTIES PASS.** |

**Not in this run, so do not show it:** escrow refunds. The refund path
(auction 3, refunded by the seller) comes with the four-auction demo, which is
not built yet.

**Expected (the replay's end state, as shown on screen):** the checker panel
lists the ten properties, all PASS, and the banner says `ALL PROPERTIES PASS`.
The clock reads `02:06 / 02:06`. The replay PNG at the end of the run is
`runs\demo-20261110-143237\replay-end.png`; the PNG just after the first kill is
`runs\demo-20261110-143237\replay-kill.png`.

**Say at the end of the replay:** "That was the same run, step by step, from
its own event log. Every number on screen came from a recorded event, and the
checker verified it. The replay file is one HTML file with no server behind
it, so anyone can open it and check the same thing."

---

## 6:45-8:15 The 10-minute chaos run, and its checker

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

---

## 8:15-9:00 Honest limits

**Screen:** README "Known limitations".

**Say:** "Honest limits: all faults are process crashes on one machine, so no
network partitions between real processes, no clock skew, no disk faults. The
payment gateway is a mock, and a payout is not atomic with the Raft commit. The
model's text is stored, not verified; numbers in a summary are checked against
the facts, but the check cannot catch two valid numbers swapped, or a number
written as words. The live demo has one auction, so the four-auction version
with refunds is not shown. Catch-up is one RPC per missing entry, a restart
replays the whole log, and the client sees about 2 seconds of failover on
Windows. And chaos runs are seeded but not deterministic: they use real
processes and wall-clock time. A deterministic simulator on a virtual clock is
the natural next step."

---

## If something goes wrong on camera

| symptom | likely cause | do |
|---|---|---|
| `could not bind … port` at start | a port is still held by an earlier run (a hand-started LLM server holds 50052), or Windows lent it to another connection (gap 13) | stop, close old windows, wait a few seconds, re-run |
| `leader:` never prints | nodes didn't start; check `runs\demo-<time>\logs\n*.log` | read the log's last lines |
| the warm-up takes 15 s or more | Ollama had unloaded the model | say so; it is a one-time cost |
| `(no answer: the assistant is unavailable ...)` | Ollama is not running | start it; the auction carried on, which is also worth saying |
| the kill step's bid takes about 2 s | expected (T18) | say so |
| the replay shows amber **catching up** | live nodes at different indexes: normal lag after a write | say so; it is not a divergence |
| the replay shows **DIVERGED** (red) | two live nodes at the same index with different digests: a real bug | stop recording, keep the run directory |
| the checker prints `FAIL` | read the evidence lines under the property | keep the run directory |
| `replay.py` says `no events.jsonl` | the run was made before the event log existed | use `runs\demo-20261110-143237` |
