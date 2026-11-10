# Demo runsheet: 5-10 minute video

The script for the demonstration video (problem statement §9: "multi-node
operation, concurrent bidding, and recovery from leader failure"). Each block
gives the time, what to type, what to say, what should be on screen, and the
expected output. Expected output is copied from the code's print formats
(`scripts/demo.py`, `scripts/chaos.py`, `checker/properties.py`) or from
recorded runs. Where a value changes from run to run, the block says so.

**⚠ REHEARSE** marks a step that has never been run exactly as written here.
Rehearse those steps before recording.

---

## What has and hasn't been run

| piece | status | evidence |
|---|---|---|
| `scripts/demo.py` (the scripted auction, kill, restart, checker) | **run**, once, with `--pause 0.3`: all 8 properties PASS | `runs/demo-20261109-220454/report.txt`; [testing-and-chaos.md](testing-and-chaos.md) §6 |
| `scripts/demo.ps1` (opens the dashboard window, then runs `demo.py`) | **only syntax-checked** (PowerShell parser: 0 errors on 2026-11-10). Never run end to end | — |
| `scripts/chaos.py --seed 5 --duration 45` | run on 2026-11-09 with the settings then in force. Since then the client give-up default changed from 30 s to 6 s (T12) | `runs/chaos-5-20261109-215202/`; [testing-and-chaos.md](testing-and-chaos.md) §5 |
| `python -m checker runs\chaos-634526-…` | re-run on 2026-11-10: all 8 PASS, 0.6 s | — |
| The LLM with Ollama running | **never** in a recorded run. In the recorded demo the description call failed ("is the Ollama daemon running?") and degraded to `""` | `runs/demo-20261109-220454/logs/llm.log` |

---

## Before recording (not on video)

1. Close every cluster window from earlier runs. The demo uses the
   `run_cluster.ps1` ports (peer 50061-50063, client 50071-50073, LLM 50052),
   and the chaos run uses 50081-50083 / 50091-50093 / 50059. Check that
   nothing is listening:
   ```powershell
   netstat -ano | findstr ":5006 :5007 :5008 :5009 :50052"
   ```
   This should print nothing in the LISTENING state.
2. Don't run the test suite during recording. It binds local ports, and
   Windows can hand one of the fixed ports to it (testing-and-chaos.md Known
   gaps 13).
3. Decide on the LLM. **Without Ollama** (rehearsed path): descriptions are
   `""`, and that is fine to say on camera. **With Ollama ⚠ REHEARSE:** start
   it first and pull the model (README Setup). The first call can take
   3.8-7.4 s ([design-notes.md](design-notes.md)), and the recorded runs never
   had it.
4. Window layout: terminal A (left, large) for commands; the dashboard window
   that `demo.ps1` opens (right); terminal B for the chaos run.
5. Font size up. `demo.ps1 -Pause 4` is the slower setting meant for
   presenting (its header comment).

---

## 0:00-0:45 Intro

**Screen:** README.md, the architecture diagram.

**Say:** "This is an online auction replicated on three servers with Raft,
written from the paper, no library. Every change, a login, a new auction or a
bid, is a command in the Raft log. Each node applies the committed log with
the same deterministic `apply()`, so all three hold the same state. Clients
talk to the leader. A follower redirects them. I'll start a cluster, bid, kill
the leader in the middle of the auction, bring it back, and then have a
checker verify that nothing was lost or duplicated."

---

## 0:45-1:15 Start the cluster ⚠ REHEARSE (`demo.ps1` has never run end to end)

**Type (terminal A):**
```powershell
.\scripts\demo.ps1 -Pause 4
```
This is where `scripts/demo.ps1` fits. It does exactly two things: it opens
the dashboard in a new PowerShell window (`scripts\dashboard.py --base-peer
50061`), then runs `scripts\demo.py --pause 4` in this window. Everything
from here to the checker at 4:30 is `demo.py`.

**Fallback if `demo.ps1` misbehaves:** start the dashboard yourself in a
second window, then run `demo.py` directly. That is the same thing by hand:
```powershell
.venv\Scripts\python.exe scripts\dashboard.py --base-peer 50061     # window 2
.venv\Scripts\python.exe scripts\demo.py --pause 4                   # window 1
```

**Expected (terminal A):**
```
=== starting LLM + 3 nodes (logs and data under ...\runs\demo-<time>)
  leader: n1
```
The leader's name varies (any of n1, n2, n3).

**Expected (dashboard):** three rows go from `DOWN` to `up`. One row shows
`LEADER` in green and two show `FOLLOWER`, all in term 1 (or 2 after a split
vote). Verdict: `replicas: agree`.

**Say:** "Three separate processes. Each has a client port and a peer port
for Raft, and each keeps its term, vote and log in its own data directory,
fsync'd before it replies. The randomised election timeout of 0.5 to 1
second picked one leader."

---

## 1:15-1:45 Log in and create an auction

**Expected (terminal A):**
```
=== log in: nisarg (seller), karan, mrudul, tanmay -- one client each
  nisarg  login        -> ok       your session is created
  karan   login        -> ok       your session is created
  mrudul  login        -> ok       your session is created
  tanmay  login        -> ok       your session is created

=== nisarg creates auction 1, closing in 60 s (LLM writes the description)
  nisarg  create       -> ok       Auction created successfully by nisarg
  description: ''
```
With Ollama running, the description is a sentence instead of `''`.

**Say:** "A login is itself a Raft command. The leader generates the random
token *before* proposing, so the log carries the token as data, and every
node, including a future leader, knows the session. The same rule holds for
the LLM: the model is called once on the leader, and only the text goes into
the log."

**Dashboard:** commit and applied advance together on all three rows. The
verdict may flash `catching up`, then `agree`.

---

## 1:45-2:15 Bid, including two equal bids at once

**Expected:**
```
=== bids: karan 100, mrudul 150
  karan   bid     100  -> ok       Bid of 100 added [high 100 by karan]
  mrudul  bid     150  -> ok       Bid of 150 added [high 150 by mrudul]

=== karan and tanmay both bid 200 at the same moment (exactly one can win)
  karan   bid     200  -> ok       Bid of 200 added [high 200 by karan]
  tanmay  bid     200  -> rejected Bid of 200 is not added because current highest bid is 200 [high 200 by karan]
```
**Which of karan and tanmay wins varies.** In the recorded run, karan won.
The two lines may print in either order.

**Say:** "Two threads sent the same amount at the same moment. Raft puts them
in one order in the log, and `apply()` accepts the first and refuses the
second, on every node. Exactly one winner, and an honest refusal for the
other."

---

## 2:15-3:00 Kill the leader; the client carries on

**Expected:**
```
=== KILL -9 the leader n1 (term 1) mid-auction
  n1 killed

=== keep bidding: the clients' next call hits the dead node, backs off, finds the new leader
  mrudul  bid     250  -> ok       Bid of 250 added [high 250 by mrudul]
  (that bid took 2.09 s including failover)
  new leader: n2
  tanmay  bid     300  -> ok       Bid of 300 added [high 300 by tanmay]
  karan   bid     300  -> rejected Bid of 300 is not added because current highest bid is 300 [high 300 by tanmay]
```
"2.09 s" is the recorded value. Expect about 2 s. Node names and terms vary.

**Dashboard events:** `n1 DOWN`, then `no leader (was n1, term 1)`, then
`leader n2 (term 2)` (the term may be higher after a split vote). The n1 row
is red `DOWN`.

**Say:** "That was `TerminateProcess`, a real kill -9: no cleanup, only
what's on disk survives. The followers stopped hearing heartbeats, one timed
out, asked for votes in a higher term, and won. Its first act as leader is
a no-op entry in its own term. Under the Figure 8 rule it can only commit by
counting copies of a current-term entry, so this is how it commits what the
old leader left behind. The client got an error, retried with the same serial
number, and found the new leader. Raft replaced the leader in under a second
in our measurements. Most of the two seconds is Windows taking 2 seconds to
refuse a connection to the dead port."

**Optional proof on screen (terminal B):**
```powershell
Select-String -Path runs\demo-*\logs\*.log -Pattern "-> LEADER|no-op" | Select-Object -Last 4
```
Expected lines have the form
`[n2] term 2 CANDIDATE -> LEADER: won election with 2 votes` and
`[n2] term 2: appended no-op at index <i>`.

---

## 3:00-3:30 Restart the dead node; it catches up

**Expected:**
```
=== restart n1: it replays its log from disk and catches up from the new leader
  n1 back; replicas agree: True

=== more bids after the restart
  karan   bid     350  -> ok       Bid of 350 added [high 350 by karan]
  mrudul  bid     400  -> ok       Bid of 400 added [high 400 by mrudul]
```

**Dashboard:** `n1 UP`. The n1 row is a `FOLLOWER` in the new term. Its
commit and applied numbers rise to match the others, the three digests share
one colour, and the verdict is `agree`.

**Say:** "The restarted node read its term, its vote and its log from disk.
Nothing volatile was saved. It starts with an empty state machine and
re-applies its log from index 1 once the leader tells it what's committed.
The leader also sends it the entries it missed while it was down."

**Optional (terminal B):** the restart's recovery line in n1's log:
```powershell
Select-String -Path runs\demo-*\logs\n1.log -Pattern "state rebuilt by replay"
```
Form: `[n1] storage …: term T, votedFor X, N log entries (last term T); state rebuilt by replay`.

---

## 3:30-4:30 Wait for the close; the dashboard shows agreement

**Expected:**
```
=== waiting <N> s for the close (the leader's closer proposes CloseAuction)
  tanmay  bid     500  -> rejected Auction is already closed tanmay [high 400 by mrudul]
  closed=True winner='mrudul' bids=[('karan', 100), ('mrudul', 150), ('karan', 200), ('mrudul', 250), ('tanmay', 300), ('karan', 350), ('mrudul', 400)]
```
`<N>` is about half a minute here. The auction closes 60 s after it was
created. The 200 in the bid list belongs to whoever won the race. The
refusal's reason may read `Time is up tanmay` instead, if the close entry
hadn't been applied yet. Both are correct refusals. The `[high 400 by mrudul]` on a refused
bid is the high bid after this command was applied
(`StateMachine._dispatch` reads it after every PlaceBid, accepted or not). Winner = highest bid = mrudul
400.

**Use the wait. Point at the dashboard:** "Three rows, the same
`digest @ index` on each. The digest is a sha256 of each node's whole state:
every auction, every bid in order, the sessions, the dedupe table. Same
colour means byte-identical state. If two nodes ever had different state at
the same index, this line would turn red and say DIVERGED."

**Say about the close:** "Every node runs the closer, but only the leader
proposes the close, stamped with the leader's clock. The close time is
decided once, by the log. The post-close bid is refused on every node."

---

## 4:30-5:00 The checker on the demo run

**Expected:**
```
=== checker: every property, on what the clients saw + every node's final state
PASS  history-well-formed  (checked 15)
PASS  acked-writes-on-every-node-exactly-once  (checked 36)
PASS  replicas-identical  (checked 3)
PASS  bids-strictly-increasing-winner-is-max  (checked 3)
PASS  no-bid-accepted-after-close  (checked 28)
PASS  rejections-consistent-with-final-state  (checked 3)
PASS  unknown-outcome-at-most-once  (checked 0)
PASS  at-most-one-leader-per-term  (checked 4)
ALL PROPERTIES PASS

DEMO: ALL PROPERTIES PASS
```
The counts are from the recorded run. `history-well-formed (checked 15)` is
fixed: 4 logins, 1 create and 10 bids. Others can differ, the leader count in
particular. The last green line comes from `demo.ps1`. With the fallback (bare
`demo.py`) it doesn't appear.

**Say:** "Here `checked 0` for unknown outcomes means this short demo never
produced one, so that property passes vacuously. The chaos run exists for that
case."

---

## 5:00-7:00 A live chaos run ⚠ REHEARSE (time it; the give-up setting changed since the recorded seed-5 run)

**Type (terminal B):**
```powershell
.venv\Scripts\python.exe scripts\chaos.py --seed 5 --duration 45 --close-min 8 --close-max 20
```
**And in the dashboard window** (Ctrl+C the old one first):
```powershell
.venv\Scripts\python.exe scripts\dashboard.py --base-peer 50081
```

**Expected first lines.** The same seed gives the same schedule. Verified on
2026-11-10 with `make_schedule(5, 45)`:
```
SEED 5
run dir ...\runs\chaos-5-<time>
schedule (3 faults):
t=   5.00s  suspend_follower down  5.74s  pick 0.795
t=  18.45s  kill_two         down  6.44s  pick 0.029
t=  30.22s  suspend_leader   down  7.66s  pick 0.901
```
Then `FAULT …` / `HEAL …` lines and the monitor's events. Which node is hit,
and the terms, vary run to run.

**Say:** "A seeded schedule of real faults against real processes: freeze a
follower, kill two of three nodes so there's no majority, freeze the leader.
Meanwhile eight client threads bid, three of them racing equal amounts on
purpose. The seed fixes the schedule, not the run: who leads when a fault
fires depends on timing. During `kill_two` nothing can commit, and the
clients give up after 6 seconds. Those writes are 'unknown': they may or may
not be in the log. That's the case where exactly-once can go wrong."

**Expected end:** the op counts, then the eight `PASS` lines, then
`ALL PROPERTIES PASS`, exit 0. The counts vary. The recorded seed-5 run had
601 accepted bids and 0 unknown, but it used a 30 s give-up, so a run today
should show some unknown bids. **The run's total length was not recorded.**
Time it in rehearsal. If it doesn't fit the video, use the next block alone.

---

## 7:00-8:15 The 10-minute run, re-checked live

**Type:**
```powershell
Get-Content runs\chaos-634526-20261110-104604\report.txt
.venv\Scripts\python.exe -m checker runs\chaos-634526-20261110-104604
```
**Expected** (identical both times; the re-check takes under a second):
```
PASS  history-well-formed  (checked 11256)
PASS  acked-writes-on-every-node-exactly-once  (checked 27081)
PASS  replicas-identical  (checked 3)
PASS  bids-strictly-increasing-winner-is-max  (checked 189)
PASS  no-bid-accepted-after-close  (checked 35837)
PASS  rejections-consistent-with-final-state  (checked 2137)
PASS  unknown-outcome-at-most-once  (checked 264)
PASS  at-most-one-leader-per-term  (checked 77)
ALL PROPERTIES PASS
```

**Say (numbers from [testing-and-chaos.md](testing-and-chaos.md) §6):** "Ten
minutes, 58 faults: 13 leader kills, 14 times two nodes at once, plus freezes.
11 256 writes. 8 957 bids accepted, and every one is on every node exactly
once. 88 bids ended 'unknown'. 3 of those turned out to be committed, each
exactly once. 63 auctions, all closed, all three nodes at the same index with
identical state. One leader per term in every term we saw. The checker is
separate from the run, so I can re-check any saved run, and each property has
a test that feeds it a broken input to show it can fail."

---

## 8:15-9:15 Close: what it doesn't do

**Screen:** README "Known limitations".

**Say:** "Honest limits: all faults are process crashes on one machine, so no
network partitions between real processes, no clock skew, no disk faults.
Catch-up is one RPC per missing entry. A restart replays the whole log,
because there are no snapshots. The client sees about 2 seconds of failover
on Windows. And the chaos workload only writes, so reads are covered by unit
tests, not by chaos."

---

## If something goes wrong on camera

| symptom | likely cause | do |
|---|---|---|
| `could not bind … port` at start | a port is still held by an earlier run, or Windows lent it to another connection (gap 13) | stop, close old windows, wait a few seconds, re-run |
| `leader:` never prints | nodes didn't start; check `runs\demo-<time>\logs\n*.log` | read the log's last lines |
| the kill step's bid takes about 2 s | expected (T18) | say so |
| `DIVERGED` on the dashboard | a real bug. It has never been seen in the recorded runs | stop recording, keep the run directory, don't delete it |
| the checker prints `FAIL` | read the evidence lines it prints under the property | keep the run directory |
