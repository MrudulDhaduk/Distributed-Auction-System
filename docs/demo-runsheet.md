# Demo runsheet: 8-9 minute video

The script for the demonstration video (problem statement §9: "multi-node
operation, concurrent bidding, and recovery from leader failure"; plus the
assignment's LLM, payment and fault-tolerance items).

The video has five parts. Each block gives the time, what to type or show,
what to say, and the expected output. **Every "Expected" block is real output**
from the run named in it: the official four-auction run `runs\demo-20261110-150720`,
unless a block says otherwise. Where a value changes from run to run, the block says so.

| time | part | source of the footage |
|---|---|---|
| 0:00-0:45 | intro and architecture | README.md, the architecture diagram |
| 0:45-2:45 | the **LIVE** run, `demo.ps1 -Pause 4`, ending on `DEMO: ALL PROPERTIES PASS`. The run takes **2:27**, so speed it up by about 20% in editing | screen capture of the live run |
| 2:45-6:45 | the **REPLAY** of that same run, slowed to about 0.6x (the run is 2:28), explained marker by marker | `runs\demo-20261110-150720\replay.html` |
| 6:45-8:15 | the 10-minute chaos run and its checker | `runs\chaos-396704-20261110-125757\` |
| 8:15-9:00 | honest limits | README, "Known limitations" |

---

## What has and hasn't been run

| piece | status | evidence |
|---|---|---|
| `scripts/demo.ps1 -Pause 4`, the **four-auction** demo, with the real LLM | **run on 2026-11-10, the official run.** Exit 0, all 10 properties PASS, whole run 147.19 s | `runs/demo-20261110-150720/` (local only; the output is quoted below) |
| the same demo, two earlier runs | `demo-20261110-145843` and `demo-20261110-150129`. Both passed, but they found two problems, now fixed: a timing flaw that refused the hero's bids (the hero winner changed), and the replay lacking the background escrow. **Not used for the video** | local only |
| `scripts/replay.py` on the official run | built and checked in **Microsoft Edge (headless), driven over the DevTools protocol**. Playwright is **not installed** on this machine, so it could not be used. 17 browser checks passed on the final page, no console errors | `runs/demo-20261110-150720/replay.html`, `replay-kill.png`, `replay-end.png` |
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
   demo warms the model itself as its first step (1.63 s in the official run).
   Keep the model loaded: Ollama unloads an idle model after a few minutes, and
   the next call is slow again. `demo.py --no-llm` shows the degraded path instead.
4. Window layout: terminal A (left, large) for commands; the dashboard window
   that `demo.ps1` opens (right). The dashboard window stays open after the
   demo: close it yourself.
5. Font size up. The replay is built for a 1920x1080 screen.
6. The replay is made from a run, after the run:
   ```powershell
   .venv\Scripts\python.exe scripts\replay.py runs\demo-<time>
   ```
   It writes `runs\demo-<time>\replay.html`. Open that file in a browser. It is
   one file, so nothing needs to be running.

---

## 0:00-0:45 Intro and architecture

**Screen:** README.md, the architecture diagram.

**Say:** "This is an online auction replicated on three servers with Raft,
written from the paper, no library. Every change, a login, a new auction, a
bid, a payment, is a command in the Raft log, applied by the same
deterministic `apply()` on every node. A separate LLM server, on its own
process and port, answers questions and writes descriptions and summaries, but
only its text ever enters the log, never the call. Four auctions run at once,
with six background bidders on three of them, and I'll kill the leader while
they are bidding. Then I'll replay the same run from its event log, and run a
10-minute chaos test and its checker."

---

## 0:45-2:45 The LIVE run

**Type (terminal A):**
```powershell
.\scripts\demo.ps1 -Pause 4
```
It opens the dashboard in a new window, then runs `scripts\demo.py --pause 4`
here. The run takes 2:27 at this speed. Speed it up in editing by about 20% to
fit the slot, but keep the pauses in the recording, because the replay uses the
same timings.

**Expected (terminal A), the official run `runs\demo-20261110-150720`:**
```
=== [   0.0s] starting LLM + 3 nodes (logs and data under E:\...\runs\demo-20261110-150720)
  leader: n2

=== [   5.1s] warm the model: the first call after Ollama loads the weights is slow
  (model warm-up: 1.63 s)

=== [  10.7s] log in: nisarg (seller), karan, mrudul, tanmay -- one client each
  nisarg  login         -> ok       your session is created
  karan   login         -> ok       your session is created
  mrudul  login         -> ok       your session is created
  tanmay  login         -> ok       your session is created

=== [  14.7s] nisarg creates auction 1, closing in 75 s (the LLM writes the description)
  nisarg  create        -> ok       Auction created successfully by nisarg
  (create auction incl. LLM description: 1.50 s)
  description (replicated as data): 'A running Seiko clock from the 1970s. Functional and operating as intended. No additional details provided about appearance, style, or specific model.'

=== [  20.2s] nisarg creates auctions 2-4 right after the hero: background auctions (the LLM writes each description)
  nisarg  create        -> ok       Auction created successfully by nisarg
  nisarg  create        -> ok       Auction created successfully by nisarg
  nisarg  create        -> ok       Auction created successfully by nisarg

=== [  28.0s] six background bidders bg1-bg6 log in and bid on auctions 2-4 until they close (not printed one by one; a status line every 5 s)

=== [  32.0s] bids: karan 100, mrudul 150
  karan   bid      100  -> ok       Bid of 100 added [high 100 by karan]
  mrudul  bid      150  -> ok       Bid of 150 added [high 150 by mrudul]

=== [  36.0s] karan and tanmay both bid 200 at the same moment (exactly one can win)
background: 88 bids, 78 accepted, 10 rejected, 0 errors | leader n2, term 1, commit 105
  karan   bid      200  -> ok       Bid of 200 added [high 200 by karan]
  tanmay  bid      200  -> rejected Bid of 200 is not added because current highest bid is 200 [high 200 by karan]

=== [  40.0s] tanmay asks the assistant about the live auction (the leader sends the model its current state)
background: 172 bids, 155 accepted, 17 rejected, 0 errors | leader n2, term 1, commit 190
  (assistant FAQ: 1.23 s)
  Q: Can I bid one more than the current high bid?
  A: 'Yes, you can bid one more than the current high bid. That would be 201.'

=== [  45.3s] KILL -9 the leader n2 (term 1) mid-auction, under background load
background: 256 bids, 233 accepted, 23 rejected, 0 errors | leader n2, term 1, commit 275
  n2 killed

=== [  49.3s] keep bidding: the clients' next call hits the dead node, backs off, finds the new leader
background: 305 bids, 269 accepted, 36 rejected, 0 errors | leader n3, term 3, commit 326
  mrudul  bid      250  -> ok       Bid of 250 added [high 250 by mrudul]
  (that bid took 0.18 s including failover)
  new leader: n3
  tanmay  bid      300  -> ok       Bid of 300 added [high 300 by tanmay]
  karan   bid      300  -> rejected Bid of 300 is not added because current highest bid is 300 [high 300 by tanmay]

=== [  55.5s] restart n2: it replays its log from disk and catches up from the new leader
background: 389 bids, 342 accepted, 47 rejected, 0 errors | leader n3, term 3, commit 414
background: 473 bids, 419 accepted, 54 rejected, 0 errors | leader n3, term 3, commit 500
background: 558 bids, 495 accepted, 63 rejected, 0 errors | leader n3, term 3, commit 587
  n2 back; replicas agree: True

=== [  71.3s] more bids after the restart
background: 599 bids, 531 accepted, 68 rejected, 0 errors | leader n3, term 3, commit 630
  karan   bid      350  -> ok       Bid of 350 added [high 350 by karan]
  mrudul  bid      400  -> ok       Bid of 400 added [high 400 by mrudul]

=== [  75.3s] waiting 16 s for the closes: auctions 2-4, then auction 1 last (the closer proposes CloseAuction)
background: 599 bids, 531 accepted, 68 rejected, 0 errors | leader n3, term 3, commit 633
background: 599 bids, 531 accepted, 68 rejected, 0 errors | leader n3, term 3, commit 633
background: 599 bids, 531 accepted, 68 rejected, 0 errors | leader n3, term 3, commit 633
background: 599 bids, 531 accepted, 68 rejected, 0 errors | leader n3, term 3, commit 634
  tanmay  bid      500  -> rejected Auction is already closed tanmay [high 400 by mrudul]
  closed=True winner='mrudul' bids=[('karan', 100), ('mrudul', 150), ('karan', 200), ('mrudul', 250), ('tanmay', 300), ('karan', 350), ('mrudul', 400)]
  background bidders stopped; still running: none

=== [  95.8s] the leader's summary workers ask the model for the four result summaries and replicate the texts
  auction 1 summary read back 10.0 s after its close: 'A vintage clock auction ended with 7 bids from 3 bidders. The opening bid was 100 by karan, and the closing bid was 400 by mrudul, who won the item with that bid.'
  auction 2 summary read back 50.0 s after its close: 'A film camera auction ended with 99 bids from 6 bidders. The opening bid was 29 by bg2, and the closing bid was 2639, also by bg2. bg2 won the auction with a bid of 2639.'
  auction 3 summary read back 40.0 s after its close: 'pocket watch: bids placed 140, from 6 bidders. Opening bid 26, closing bid 3573. Winner bg6, with a winning bid of 3573.'
  auction 4 summary read back 30.0 s after its close: 'A vinyl record player auction ended with 292 bids from 6 bidders. The opening bid was 8, and the closing bid was 8102, won by bg1.'

=== [  99.8s] mrudul (the winner) pays 400 into ESCROW; tanmay (not the winner) tries and is refused
  tanmay  pay      400  -> rejected tanmay is not the winner: only the winner can pay [reference -]
  mrudul  pay      400  -> ok       400 is held in escrow for auction 1 [reference PAY-5F206439A03D]
  (payment: 0.00 s)
  escrow now: status HELD, amount 400, payer mrudul, seller nisarg, payment_ref PAY-5F206439A03D, settlement_ref -

=== [ 103.8s] KILL -9 the leader n3 (term 3) AFTER the payment
  n3 killed

=== [ 107.8s] the new leader shows the SAME escrow; then nisarg (the seller) releases it
  escrow on the new leader: status HELD, amount 400, payer mrudul, seller nisarg, payment_ref PAY-5F206439A03D, settlement_ref -
  same record as before the kill: True
  nisarg  release       -> ok       Escrow released to the seller [reference REL-5F206439A03D]
  escrow (seen by the winner): status RELEASED, amount 400, payer mrudul, seller nisarg, payment_ref PAY-5F206439A03D, settlement_ref REL-5F206439A03D

=== [ 115.9s] background winners paid with their own clients; the seller releases auctions 2 and 4 and refunds 3
  bg2   pay 2639 for auction 2 -> ok       2639 is held in escrow for auction 2 [reference PAY-59E56609FB2F]
  bg6   pay 3573 for auction 3 -> ok       3573 is held in escrow for auction 3 [reference PAY-6531BDBE19E4]
  bg1   pay 8102 for auction 4 -> ok       8102 is held in escrow for auction 4 [reference PAY-A16AE91108D5]
  nisarg  release       -> ok       Escrow released to the seller [reference REL-59E56609FB2F]
  nisarg  refund        -> ok       Escrow refunded to the winner [reference RFD-6531BDBE19E4]
  nisarg  release       -> ok       Escrow released to the seller [reference REL-A16AE91108D5]

=== [ 120.0s] restart n3: it replays its log from disk and catches up from the new leader
  n3 back; replicas agree: True

=== [ 127.6s] the escrow of every auction, at the end
  auction 1: status RELEASED, amount 400, payer mrudul, seller nisarg, payment_ref PAY-5F206439A03D, settlement_ref REL-5F206439A03D
  auction 2: status RELEASED, amount 2639, payer bg2, seller nisarg, payment_ref PAY-59E56609FB2F, settlement_ref REL-59E56609FB2F
  auction 3: status REFUNDED, amount 3573, payer bg6, seller nisarg, payment_ref PAY-6531BDBE19E4, settlement_ref RFD-6531BDBE19E4
  auction 4: status RELEASED, amount 8102, payer bg1, seller nisarg, payment_ref PAY-A16AE91108D5, settlement_ref REL-A16AE91108D5

=== [ 131.6s] price trends over the closed auctions: statistics in code, wording by the model
  (price trends: 2.50 s)
  closed auctions 4, with at least one bid 4, lowest winning bid 400, highest winning bid 8102, total bids 538
  summary: 'There were 4 closed auctions, all with bids. The lowest winning bid was 400, the highest was 8102, and winning bids rose from 400 to 8102 across the auctions.'

=== [ 138.1s] background stall at each leader kill: the slowest background bid in flight around the kill
  background stall during failover (n2 killed): 2.18 s
  background stall during failover (n3 killed): n/a -- no background bid was in flight then (every background auction had closed)

=== [ 142.1s] checker: every property, on what the clients saw + every node's final state
PASS  history-well-formed  (checked 632)
PASS  acked-writes-on-every-node-exactly-once  (checked 1680)
PASS  replicas-identical  (checked 3)
PASS  bids-strictly-increasing-winner-is-max  (checked 12)
PASS  no-bid-accepted-after-close  (checked 2152)
PASS  rejections-consistent-with-final-state  (checked 71)
PASS  unknown-outcome-at-most-once  (checked 0)
PASS  at-most-one-leader-per-term  (checked 6)
PASS  escrow-legal-and-at-most-one-payment  (checked 21)
PASS  summary-at-most-one-on-closed-auction  (checked 12)
ALL PROPERTIES PASS

DEMO: ALL PROPERTIES PASS
```
The leader's name, the terms, the reference values and the LLM's wording change
from run to run. The timings and the number of background bids change too (the
background bidders race each other). The lines that matter on camera: the
background status lines, the equal-bid refusal at 36.0 s, the two stall lines,
the four escrow lines, and the last line.

**Say at the end of the live run:** "Everything you just saw was printed by the
cluster and by the checker. Now I'll replay the same run from its event log, so
you can see the state of every node, and every auction, at each step."

---

## 2:45-6:45 The REPLAY of that run

**Open:** `runs\demo-20261110-150720\replay.html` in a browser, full screen.
The banner at the top says it is a replay of a recorded run. Press Space to
start, and set the speed to 0.5x. Use the arrow keys to move from one event to
the next, or click a marker on the timeline. Hover over a marker to read its
full text.

**What is on screen:**
- Three node cards: role, term, commit, applied and the first eight characters
  of the state digest. The leader has a purple border. A node that is killed
  turns grey and says DOWN.
- The big indicator on the right: **replicas identical** (green) when every live
  node has the same digest at the same index. It says DIVERGED (red) only if
  two live nodes at the same index disagree. Amber **catching up** means the
  live nodes are at different indexes, which is normal lag after a write.
- Four auction cards: item, open or closed, high bid and bidder, winner, escrow
  (NONE, HELD, RELEASED, REFUNDED), and the description and summary.
- The hero auction's bid list. Accepted bids are normal text, refused bids are
  greyed out with the reason.
- Background bidders: the counters (bids, accepted, refused, errors). The
  background bids are counted, not listed one by one.
- The event log, which scrolls: step headings in bold, kills in red, elections
  in amber, escrow changes in green.
- The timeline: short labels on two rows (`kill n2`, `leader n3`, `close #2`,
  `pay #2`, `checker`). Where two labels would overlap, the label is left off
  and the marker's tooltip still has its full text.

**The markers, in order** (the times are the replay clock, `mm:ss`, from the
official run):

| replay time | marker | point at | say |
|---|---|---|---|
| 00:05 | `leader n2` | the leader card | "Startup. The three nodes elect a leader for term 1: n2." |
| 00:14 | `create #1` | the auction card, the description | "The LLM wrote this description once, when the auction was created. It is stored as text, and the followers never call the model." |
| 00:20 | `create #2-4` | the three new auction cards | "Three background auctions, with their own descriptions. They close 35, 45 and 55 seconds after the hero was created, so the hero closes last." |
| 00:28 | `bidders on` | the background counters | "Six bidders, bg1 to bg6, each with its own client. They bid on the three background auctions. Nothing is printed for each bid; the counters show them." |
| 00:32 | `bids` | the bid list | "Normal bids. Each one is committed on a majority before it is answered." |
| 00:36 | `equal bids` | the two rows at 00:36 and 00:40 | "The same amount, at the same moment. Raft puts them in one order. karan's is accepted; tanmay's is refused, because the high bid is already 200. The refusal is the same on every node." |
| 00:40 | `assistant` | the event log, the FAQ line | "The assistant is told the live auction and answers from it: 'Yes... That would be 201.' Nothing is written to the log." |
| 00:49 | `kill n2`, then `failover` | the n2 card turns grey; the background counters keep moving | "A real kill -9, not a pause, while six background bidders are bidding. The other nodes stop hearing from n2, one of them times out, asks for votes in a higher term, and wins." |
| 00:49 | `close #2` | the auction 2 card turns CLOSED | "Auction 2 closes at its scheduled time. Its winner is bg2 at 2639." |
| 00:51 | `leader n3`, `pay #2` | the n3 card gets the border; auction 2 escrow turns HELD | "The new leader is n3. bg2 paid its own winning bid, with its own client, the moment auction 2 closed." |
| 00:55 | `restart n2` | the n2 card comes back | "n2 reads its term, vote and log from disk, then catches up from the new leader." |
| 00:59-01:09 | `close #3`, `pay #3`, `close #4`, `pay #4` | the auction cards | "The same thing for auctions 3 and 4: close at its time, and the winner pays automatically." |
| 01:15 | `close wait` | the hero card still OPEN | "The hero waits for its own close. It closes last." |
| 01:29 | `close #1` | the hero card CLOSED, mrudul at 400 | "The close and the winner are part of the replicated state, so every node agrees on them." |
| 01:35 | `summaries` | the summary lines on the four cards | "The leader asked the model for a summary for each auction, and replicated the texts. They are stored like any other data, and the numbers are checked against the facts in code." |
| 01:39 | `payment`, then `pay #1` | the escrow pill HELD | "Only the winner can pay, and only the winning amount. The leader calls the payment gateway once, and the gateway's reference travels in the command as plain data." |
| 01:47 | `kill n3`, `escrow read` | n3 turns grey; the escrow record is the same | "The second kill, after the payment. The new leader, n1, reports the identical escrow record: the escrow survived the leader's death." |
| 01:55 | `release #1`, `bg escrow` | the escrow pills | "The seller releases the hero's money. Then the seller releases auctions 2 and 4 and REFUNDS auction 3: the refund goes back to the winner." |
| 01:59 | `restart n3` | n3 comes back | "The last node back. Its state matches the other two." |
| 02:11 | `trends` | the event log | "Statistics in code: four closed auctions, lowest winning bid 400, highest 8102. The model only words the summary." |
| 02:18 | `stall` | the event log, the stall lines | "The slowest background bid during the first kill took 2.18 seconds: that is the failover, seen from a bidder. The second kill had no background bid in flight: every background auction had closed by then." |
| 02:27 | `checker` | the property list and the banner | **Stop here on ALL PROPERTIES PASS.** |

**Expected (the replay's end state, as shown on screen):** the checker panel
lists the ten properties, all PASS, and the banner says `ALL PROPERTIES PASS`.
The clock reads `02:28 / 02:28`. The four auction cards read: #1 RELEASED, #2
RELEASED, #3 REFUNDED, #4 RELEASED. The PNG just after the first kill is
`runs\demo-20261110-150720\replay-kill.png`; the PNG at the end of the run is
`runs\demo-20261110-150720\replay-end.png`.

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
written as words. The background bidders are clients of the same cluster, all
on one machine, at about 17 bids a second. The second leader kill is not under
load: the hero closes last, and its payment waits for that close, so every
background auction has already closed. Catch-up is one RPC per missing entry, a
restart replays the whole log, and the client sees about 2 seconds of failover
on Windows. And chaos runs are seeded but not deterministic: they use real
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
| the first kill's bid takes about 2 s | expected (T18) | say so |
| `background stall during failover … n/a` for the first kill | no background bid was in flight at that moment | say so; it is the same as the second kill |
| the replay shows amber **catching up** | live nodes at different indexes: normal lag after a write | say so; it is not a divergence |
| the replay shows **DIVERGED** (red) | two live nodes at the same index with different digests: a real bug | stop recording, keep the run directory |
| the checker prints `FAIL` | read the evidence lines under the property | keep the run directory |
| `replay.py` says `no events.jsonl` | the run was made before the event log existed | use `runs\demo-20261110-150720` |
