# Reflections — GroundHog (CS G623, Option F)

One document for the team, with a section per member: what each of us
contributed, and what each of us learned. The contribution lists come from the
git history (`git log --author=…`); the "What I learned" parts are written by
each member.

| Member | ID | Commits | Main areas |
|---|---|---|---|
| Mrudul Dhaduk | 2026H1030069P | 104 | M1 foundation (state machine, gRPC, auth, LLM server); M2 Raft, persistence, replication, chaos harness, checker, payments, LLM wiring, scripted demo |
| Nisarg Vagh | 2026H1030066P | 23 | M1 fixes; web dashboard, validation rules, replicated accounts, PreVote, partitions, payments UI, anti-sniping, replays, Docker |

## Timeline

| When | What |
|---|---|
| 23 Aug – 28 Sep 2026 | Milestone 1: project skeleton, auction state machine with one `apply()`, gRPC services, sessions and auth interceptor, auction close loop, LLM server (Ollama, `qwen3:4b-instruct`), item descriptions |
| 9 – 10 Nov 2026 | Milestone 2: Raft election, log replication, persistence (fsync before reply), auction on Raft, replicated sessions, read-index reads, chaos harness, property checker, stress tests, escrow, LLM FAQ / summaries / price trends, scripted demo and replay |
| 16 – 18 Nov 2026 | Web dashboard and its scenarios, validation rules, replicated accounts, PreVote, simulated partitions, payment buttons, anti-sniping, scenario replays, Docker setup |

---

## Mrudul Dhaduk

### Contributions

**Milestone 1 (Aug – Sep):**
- Project skeleton and the auction state machine: every change goes through one
  `apply()`, serialised with a lock; the concurrent-bidding test.
- gRPC: the ping round trip, `AuctionService` and `AuthService`, the shared
  wire module, the end-to-end CLI client.
- Sessions (`SessionStore`) and the auth interceptor that checks the token on
  every call; the auction close loop and `CloseAuction`.
- The LLM server: `llm.proto` with three typed RPCs behind `getLLMAnswer`, the
  Ollama-backed server, the `LLMClient` adapter, and LLM-written descriptions
  that degrade cleanly when the model is down. Model choice (`qwen3:4b`).

**Milestone 2 (Nov):**
- Raft from the paper: `raft.proto`, election (terms, votes, randomised
  timeouts), log replication and repair, the commit rule, the leader's no-op,
  durable storage with fsync before reply, restart by log replay.
- The auction on Raft: command encoding, the state machine with request
  dedupe, replicated sessions, read-index reads, client redirects and retries.
- Testing: the seeded chaos harness, the ten-property checker, stress and
  failover measurements, the terminal dashboard.
- Payments and escrow (mock gateway), the LLM FAQ, result summaries and price
  trends, the scripted demo, the replay page, the submission script and docs.

### How it was built

*[In your own words, consistent with the README's "Who wrote what" note: how
the M2 chunks were produced, reviewed and understood.]*

### What I learned

*[Write in your own words. Prompts:]*
- *Which part of Raft was hardest to get right, and what finally made it click?*
- *A bug that taught you something (e.g. the Windows log.bin handle issue, the
  2 s connect-refusal floor on failover, the checker catching a dedupe or
  ordering problem).*
- *What durability ("fsync before reply") cost in throughput, and why it matters.*
- *What you would design differently with more time (snapshots, PreVote from
  the start, a faster catch-up than `nextIndex -= 1`).*

---

## Nisarg Vagh

### Contributions

**Milestone 1 (Sep):**
- `8374041` the auction owner named in `CreateAuction`'s result.
- `172b0bc` README: auction process and user details.

**Milestone 2 additions (16 – 18 Nov):**
- Web dashboard (`python -m dashboard`): live node status, heartbeats and
  election timers, kill / suspend / resume / restart, accounts, auctions,
  bidding and a per-auction assistant.
- Validation rules: a seller cannot bid on their own auction (enforced in
  `apply()` on every replica), positive bids, create-auction checks.
- Replicated user accounts (`RegisterUser` in the Raft log, salted password
  hashes) and lag-aware login.
- Heartbeat telemetry in the status RPC; the assistant told the seller, the
  high bidder and the asker.
- Scenario buttons (vintage clock, four auctions, chaos workload) with the
  checker at the end; the checker learning the new refusal reasons.
- PreVote as a switchable Raft mode; simulated network partitions (cut links);
  Pay / Release / Refund buttons; anti-sniping; scenario replays.
- Docker setup (`docker compose up`), the submission-zip fix, and the README
  restructure.

### How it was built

*[In your own words, consistent with the README's "Who wrote what" note: how
these features were produced, reviewed and tested.]*

### What I learned

*[Write in your own words. Prompts:]*
- *What you saw when you suspended a follower in Classic Raft vs PreVote, and
  why the term number decides who leads.*
- *Why the seller rule had to live in the replicated state machine and not in
  the page.*
- *What the "two leaders at once" partition showed about majorities and commit.*
- *Something that surprised you (e.g. the checker failing on a create retried
  past its own close time, or failover being faster on Linux than on Windows).*

---

## Team learnings

*[Optional, written together: what worked in how you split the work, what you
would change, and what you would build next.]*
