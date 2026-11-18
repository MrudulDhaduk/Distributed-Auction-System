# Individual Reflection: Distributed Online Auction System

**Course:** CS G623 Advanced Operating Systems, First Semester 2026-27
**Project:** Option F, Distributed Online Auction System (gRPC + Raft + LLM), Milestones 1 and 2
**Team:** Mrudul Dhaduk (2026H1030069P) and Nisarg Vagh (2026H1030066P) (two-member team, agreed with Prof. Bera)
**Repository:** github.com/MrudulDhaduk/Distributed-Auction-System

This file has two parts: Part A is Mrudul's reflection, Part B is Nisarg's.

---

# Part A: Mrudul Dhaduk

## 1. What the system is

Three server nodes keep one auction consistent with a Raft implementation written from the paper (no Raft library). All communication is gRPC, in Python. A separate LLM server runs a local model through Ollama.

The system takes concurrent bids, closes auctions on a timer and picks the winner, handles a mock payment into escrow, and uses the LLM for a bidding FAQ, item descriptions, auction result summaries and price trends. If the leader is killed mid-auction, a new one is elected, clients carry on, and all nodes hold the same state.

## 2. My contributions

### Milestone 1 (written and committed by me)

| Area | What I did |
|---|---|
| Foundation | Project skeleton: package layout, dependencies, smoke test |
| Auction core | `AuctionState.apply()` as the single entry point for every state change, the command types, `BidEntry`, `Result`, and a concurrency test. A per-instance lock serialises `apply()` after a race showed up in testing |
| gRPC | A ping round trip first (proto, codegen script, server, client), then `auction.proto` with the real services |
| Sessions and auth | `SessionStore` (mint, validate, destroy tokens), and an auth interceptor so the token is checked at one choke point, with the token in gRPC metadata rather than in request bodies |
| Auction operations | `CloseAuction`, the `AuctionService` and `AuthService` servicers, an end-to-end CLI client, shared wire code in `common/`, and a poll loop that closes auctions when their time is up |
| README | Setup and usage instructions, with `grpcio` pinned |

### Milestone 2 (built in chunks, each planned, reviewed and committed by me)

Milestone 2 was built one chunk at a time. For each chunk I planned the scope, reviewed the code and its explanation notes (a walkthrough against the paper's Figure 2 rules and a decision log), made the design calls described below, and committed it.

| Chunk | What was built | Commits (summary) |
|---|---|---|
| 1. Election | Terms, votes, heartbeats, randomised timeouts; node processes and a 3-node run script | Raft messages and transports, election, runner, docs E1-E20 |
| 2. Replication | Log entries on the wire, follower `AppendEntries`, leader `nextIndex` backoff, the Figure 8 commit rule, the applier, `propose()`, a no-op at the start of each leader's term | docs R1-R17 |
| 3. Persistence | `FileStorage`: term, vote and log fsync'd before a reply; restart tests (no double vote, no lost acknowledged write) | docs P1-P15 |
| 4. Auction on Raft | Command encoding, state machine behind `apply_fn` with exactly-once dedupe by `(client_id, seq)`, replicated sessions, leader-only closer, leader-only reads with read-index, a client that follows leader hints and retries | docs A1-A19 |
| 5. Testing | State digest, status RPC, property checker (8 properties), chaos harness (kill -9, suspend, two nodes at once), stress script, terminal dashboard | docs T1-T19 |
| 6. Packaging | README rewrite, demo runsheet, submission zip script | |
| 7. Assignment features | Mock payment and escrow (`PayAuction`, `ReleaseEscrow`, `RefundEscrow`), LLM wired into the app (FAQ, description, result-summary worker, price trends), `ProcessBusinessRequest`, two more checker properties | docs C1-C23 |
| Demo | Scripted demo with four auctions and six background bidders, two leader kills, and a self-contained HTML replay viewer of the recorded run | |

### Decisions I made along the way

- Set the working mode: one chunk at a time, tests first, with a walkthrough and a decision log for each chunk.
- Approved the change to `raft/` that stress testing called for (the retry in `FileStorage`, T17), and kept my Milestone 1 code unchanged unless a change was explained and logged.
- Required each chaos-test client to have a single write in flight, so the checker's bid identity stays unambiguous.
- Compared the code against the assignment PDF, found payment/escrow and the LLM features missing, and added them.
- Chose the demo design: four concurrent auctions, background bidders, failover under load, and a replay viewer so the recorded run can be paused and explained.

## 3. What I learned

- **Raft election.** Terms are the cluster's logical clock, and a node votes once per term. The §5.4.1 rule refuses a vote to a candidate whose log is behind. Randomised timeouts break split votes: with timeouts of 0.15-0.3 s the term went from 2 to 68 over 17 leader kills (about 4 new terms per kill), and with 0.5-1.0 s it rose by exactly one per kill.
- **Replication and the commit rule.** A leader commits once a majority stores an entry, but only counts replicas for entries of its own term (Figure 8). That is why a new leader appends a no-op first.
- **Persistence.** Term, vote and log must reach disk before a node replies, otherwise a restart can vote twice or lose an acknowledged write. We tested this with restart tests that also fail on in-memory storage.
- **Determinism.** Every node applies the same log through the same `apply()`, so `apply()` can have no clock, no randomness and no I/O. Times are stamped before proposing, and LLM text is generated first and replicated as plain data. The LLM is never called from the log.
- **Exactly-once.** Clients retry after a leader dies. A `(client_id, seq)` dedupe table inside the replicated state stops a retried bid from being applied twice.
- **Reads.** A leader-only read behind read-index confirms the leader is still the leader before answering.
- **Testing a distributed system.** A property checker run over what clients saw plus every node's final state is stronger than unit tests. Each property has a test that feeds it broken input, to show it can fail. One property once passed with `checked 0`, which taught me to read the counts and not only the PASS.
- **LLMs in a system.** A model's output is not trusted. Price-trend facts are computed in code and the model only words them. The summary worker rejects a summary containing a number that is not among the facts and falls back to a template, after a live call wrote 40 for a bid of 400.
- **Windows specifics.** A venv's `python.exe` is a launcher with the real interpreter as its child, a directory can't be fsync'd from Python, and connecting to a dead localhost port takes 2.0 s to be refused.

## 4. Challenges and how they were handled

| Problem | How it showed up | Resolution |
|---|---|---|
| M1 time conversion lost 1 µs on some datetimes (float precision) | A round-trip test in the codec chunk | Integer arithmetic, tested on 200 000 random values |
| The monitoring RPC slowed the node it watched | Each poll copied a large auction under a lock; commits fell to a crawl | Cached check, opt-in digest, throttle |
| One transient `PermissionError` on `os.replace` permanently stopped a node's storage | Found by the failover stress run | Committed first as an expected-to-fail test, then fixed with a bounded retry |
| Client failover took about 2.2 s although elections took under 1 s | Compared election time to client wait | Cause measured: Windows refuses a dead-port connect after 2.0 s. Reported separately, not hidden |
| The chaos harness killed itself | A 10-minute run ended with no verdict | Inferred cause: Windows reused a process ID. Fixed by treating a process as a child only if created after its parent. The cause is an inference, not observed |
| Test log corruption | A gRPC log line ended up inside a node's `log.bin` under pytest capture | `--capture=sys` in `pytest.ini`. The mechanism is inferred, not proven |
| An LLM summary got a number wrong | A live call | Facts computed in code, number check, template fallback |

Two things are still unexplained: the cause of the original stall that led to the no-op fix (never reproduced) and one flaky escrow test (about 2 failures in 78 runs, not reproduced in 150 further runs after hardening).

**Results:** one 10-minute chaos run (seed 396704, 57 faults, 11 978 writes) passed all 10 checker properties. Election after a leader kill took 892.7 ms (median, default timeouts). Throughput was 210.5 writes/s with one client and 688.1 writes/s with 16.

## 5. What I would do differently

- Add CheckQuorum (§6 of the paper and §9.6 of Ongaro's thesis), so a leader cut off from the majority steps down on its own instead of waiting to hear a higher term. PreVote and simulated partitions are in the system now, and the partition tests show the old leader lingering until the network heals.
- Use the §5.3 conflict hint so a lagging follower catches up in one round trip, not one per missing entry, and add log snapshots so a restart does not replay the whole log.
- Fix the M1 `apply()` so it does not scan every bid on each `PlaceBid` (0.15 ms at 1 000 bids, 2.85 ms at 25 000).
- Build a deterministic simulator on a virtual clock. Our chaos runs are seeded but use real processes and real time, so a failing run cannot be replayed exactly.
- Test the real model under chaos, not a canned stand-in, and add partitions to the chaos harness's random fault schedule (today they are cut by hand from the dashboard).

---

# Part B: Nisarg Vagh

I committed under two names, the GitHub alias **Frostbitten-Caracal** and **Nisarg Vagh**. Both are me.

## 1. My contributions

### Milestone 1

| Area | What I did |
|---|---|
| LLM server | Switched to `qwen3:4b-instruct-2507` (non-thinking), `llm.proto` with three typed RPCs behind the handout's `getLLMAnswer`, the LLM server over Ollama, an `LLMClient` adapter, and item descriptions generated at `CreateAuction` that degrade to empty text when the LLM is down |

### Milestone 2

Most of my Milestone 2 work sits on top of the Raft cluster: features in the core system, a web dashboard to drive and show them, and the work to package and deliver the project.

**Core system** (works without the dashboard)

| Feature | What it does |
|---|---|
| Seller cannot bid on their own auction | Enforced in the replicated state machine on every node, with extra checks on bids and on new auctions |
| Replicated user accounts | Sign-up is a Raft log entry, so accounts survive a leader crash. Passwords are stored as salted hashes |
| PreVote | An optional Raft mode, so a node coming back from a freeze cannot knock out a healthy leader |
| Network partitions | Nodes can be cut off from each other (Raft messages dropped), to show "two leaders, only the majority commits" |
| Anti-sniping | A bid in the last 10 s extends the close (up to 5 times), decided identically on every replica |
| Heartbeat telemetry | Nodes report heartbeats sent and received, and their election timer |
| Smarter AI assistant | Each auction's assistant knows the seller, the high bidder and who is asking |

**Web dashboard** (`python -m dashboard`)

| Area | What it shows or does |
|---|---|
| Live cluster view | Heartbeat dots, election timers, and Kill / Suspend / Resume / Restart for each node |
| Raft controls | A Classic Raft / PreVote switch, clickable links to cut, and Isolate / Heal |
| Auction features | Accounts, auctions, bidding, a per-auction AI chat, and Pay / Release / Refund buttons |
| Scenarios | Vintage clock, Four auctions and Chaos workload, each ending with the 10-property checker and a replay |

**Delivery**

| Area | What I did |
|---|---|
| Docker | The whole system runs with one `docker compose up` |
| Fixes | The submission zip was missing the dashboard; a test failed only on Linux; AI chats leaked between accounts (a bug I found); a checker gap turned up in a chaos run |
| Docs | README restructure, the demo video script, and this reflections file |

**Tested by hand:** leader kills; Classic vs PreVote suspend/resume; and a two-cut partition experiment, which produced several leader changes in Classic mode and one in PreVote mode.

## 2. What I learned

- **An LLM is an unreliable dependency.** The auction must keep working when the model is slow or down, so descriptions degrade to empty text instead of failing `CreateAuction`. Putting typed RPCs behind the single `getLLMAnswer` entry point kept each use of the model separate and easy to test, and a non-thinking model keeps answers short enough for a request path.
- **Rules belong in the replicated state machine.** The seller rule only holds if every node applies it to the same log entry in the same way. A check in the dashboard alone could be bypassed by any other client.
- **Anything a replica decides must be deterministic.** Anti-sniping extends the close from data carried on the command, never from a node's own clock, so all three nodes extend the same auction by the same amount.
- **State that must survive a crash goes through the log.** Once sign-up became a Raft entry, accounts survived a leader kill like any other committed write. Storing salted hashes means no node ever holds a plain password.
- **The term number decides who leads.** In Classic Raft, a suspended follower keeps timing out and raising its term, and when it resumes that higher term forces a healthy leader to step down. PreVote makes it ask first, without raising its term, so the cluster ignores it. The two-cut partition experiment showed this clearly: several leader changes in Classic mode and one in PreVote.
- **A partition can have two leaders, but only one can commit.** The leader cut off on the minority side still believes it leads, but it cannot reach a majority, so nothing it accepts is committed. When the partition heals, its term is behind and it steps down.
- **Seeing helps.** Heartbeat dots and election timers on screen made Raft's timing visible, and they made bugs easier to notice than reading logs.

## 3. Challenges and how I handled them

| Problem | How it showed up | Resolution |
|---|---|---|
| AI chats leaked between accounts | While testing in the dashboard, switching accounts still showed the previous user's assistant chats | The dashboard clears the previous user's chats on account switch |
| A gap in the property checker | A chaos run produced a create refusal the checker did not understand | The checker now understands create refusals from the close-time and item rules |
| A test failed only on Linux | Running the suite outside Windows | Fixed the test so it passes on Linux as well |
| The submission zip was missing the dashboard | The packaged zip had no dashboard package | The submission script now includes the dashboard package |

## 4. What I would do differently

- Make PreVote the default from the start. The hand tests showed how much leader churn Classic mode causes after a freeze.
- Cut real network links between containers instead of dropping Raft messages inside the process, so client traffic is partitioned as well.
- Turn the hand tests (leader kills, Classic vs PreVote suspend/resume, the two-cut partition) into automated tests that save their results, so the comparison can be rerun and reported with numbers.
- Test with more than one account from the beginning, which would have caught the chat leak earlier.
- Run the test suite on Linux as well as Windows from the start, for example in CI, instead of finding platform-specific failures at the end.
