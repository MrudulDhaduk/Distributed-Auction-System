# Reflection fact sheet

Raw material for the individual reflection. **Facts only, each with its
source. No prose and no conclusions: those are yours to write.** "Source" is
a file in this repo, a commit hash, or a saved run under `runs\` (local only,
not in the zip). Where a fact is an inference rather than a measurement, it
says so.

---

## 1. How the work was done

| fact | source |
|---|---|
| M1 (build-order steps 1-2: `apply()`, commands, the lock, gRPC, auth, sessions, auction RPCs, closer, LLM server) was written by you | CLAUDE.md "The rule that overrides everything else" |
| From M2 on, the code was written by Claude one chunk at a time. You studied each chunk, discussed the decisions and made every commit yourself | CLAUDE.md "The chunk loop"; git history: every M2 commit (2026-11-09 onward) is authored by you |
| Two M1 commits are by Nisarg Vagh (GitHub "Frostbitten-Caracal"): `8374041` "Name the auction owner in CreateAuction's result" and `172b0bc` "Revise auction process and user details in README", both 2026-09-28. The problem statement describes a two-member team | `git log --format="%h %an %s"`; `Documents/Problem_Statement.md` §10 |
| Every chunk shipped with a walkthrough (`docs/<chunk>.md`), decision-log entries and a Known-gaps list | CLAUDE.md "Explanation artifacts"; `docs/` |
| Tests were written first and shown failing before each implementation, with the stated exceptions | CLAUDE.md "The chunk loop"; [auction-on-raft.md](auction-on-raft.md) §13 (`test_failover_grpc.py` was written after the client) |
| Raft was written from the paper, with no Raft library | CLAUDE.md "Non-negotiable design constraints"; `requirements.txt` (grpcio, grpcio-tools, pytest, rich, ollama) |

## 2. What was built, in order

| # | what | dates (commit dates) | commits | walkthrough / decisions |
|---|---|---|---|---|
| M1 | Skeleton, `AuctionState.apply()` + concurrency test, lock | 2026-08-25 to 08-26 | `29ec102` … `fad9d99` | [design-notes.md](design-notes.md) |
| M1 | gRPC ping round trip | 2026-08-26 | `df44c47`, `4c5778f` | — |
| M1 | CloseAuction, SessionStore, auth interceptor, servicers, client, close poll loop | 2026-09-14 to 09-15 | `7ec79c7` … `2e536b6` | design-notes.md |
| M1 | LLM: model switch, llm.proto, LLM server, descriptions with degradation, README | 2026-09-21 to 09-28 | `d18929c` … `172b0bc` | design-notes.md |
| Chunk 1 (step 3) | Leader election: terms, votes, heartbeats, randomised timeouts; processes; script | 2026-11-09 | `bb6c179` … `805fe9f` | [raft-election.md](raft-election.md), E1-E20 |
| Chunk 2 (steps 4-5) | Log on the wire; follower AppendEntries 1-5; leader nextIndex backoff, Figure 8 rule, applier, `propose()`; the no-op | 2026-11-09 | `e458a69` … `1d8b60f` | [raft-replication.md](raft-replication.md), R1-R17 |
| Chunk 3 (step 6) | FileStorage (fsync before return), Storage protocol, restart tests, `--data-dir` | 2026-11-09 | `22ac358` … `63c5042` | [raft-persistence.md](raft-persistence.md), P1-P15 |
| Chunk 4 | Auction on Raft: codec, state machine + §8 dedupe, replicator, replicated sessions, handlers through `propose`, leader-only closer, two ports, replay test; then read-index, leader-only reads, ClusterClient | 2026-11-09 | `605a588` … `e7b2084` | [auction-on-raft.md](auction-on-raft.md), A1-A19 |
| Chunk 5 (step 7) | time_conv fix; digest; StatusService; checker; harness (procs, schedule, workload); chaos script; dashboard; stress script; demo; T16, T17, T19 fixes; final chaos run | 2026-11-09 to 11-10 | `cd8128a` … `e7d9f3e` | [testing-and-chaos.md](testing-and-chaos.md), [stress-results.md](stress-results.md), T1-T19 |
| Chunk 6 | README rewrite, demo runsheet, this sheet, viva prep, submission script | 2026-11-10 | (uncommitted) | this file |

## 3. Bugs found, and how they were found

| id | bug | how it was found | fixed? | source |
|---|---|---|---|---|
| T1 | **M1 bug.** `int(dt.timestamp() * 1_000_000)` lost 1 µs on some datetimes (float precision). Example: `2039-12-20 05:40:54.254810` → `…809` | Chunk 4's codec round-trip test. Pinned as a strict xfail (`b3c6f6c`), left for your review, then fixed on request | yes, `cd8128a` (integer `timedelta` arithmetic); 200 000 seeded values tested | decisions.md T1; testing-and-chaos.md §0 |
| T1 note | Replicas never disagreed because of it: only the leader converts, and every node applies the decoded bytes | reasoning, recorded in the docs | — | decisions.md T1 |
| R15 | The first draft of the stale-log test let the leader repair n3, which then won legitimately. The test was wrong, not the code | test flaked | test rewritten (deterministic leader) | decisions.md R15; raft-replication.md §6 |
| R9 | One in-memory chaos test run in ten stalled ~10 s while converging. Likely cause: a new leader can't commit old-term entries without a current-term entry | flaky test | the leader now appends a no-op (`0a9461d`). **The stall itself was never reproduced** (0 in a 150-seed repro), so the cause is not proven | decisions.md R9; raft-replication.md §6, §8 |
| T6 | `suspend` froze only the venv launcher; the real interpreter is its child (a counter went 13 → 57 while "suspended") | a harness test | yes: operations walk the process tree | decisions.md T6; testing-and-chaos.md §3.1 |
| T12 | `unknown-outcome-at-most-once` passed with `checked 0` (vacuous) in the 45 s smoke run, because a 30 s client give-up never gave up | reading the checker's `checked` counts | yes: chaos clients give up after 6 s | decisions.md T12 |
| T14 (1) | Waiting for `quiet()` (identical digests) before each kill skipped every kill under load | the stress output: `kill 1: cluster not quiet within 60 s; skipping` … | yes: `settled()` (within 50 entries) | decisions.md T14; `runs\stress.out` |
| T14 (2) | `t_write` once read "2 ms": the first ack came from the old leader, just before `TerminateProcess` landed | an implausible number | yes: only an ack from another node counts | decisions.md T14 |
| T14 (3) | A writer thread outlived its cluster and was refused UNAUTHENTICATED by the next config's fresh cluster on the same ports | stress run | yes | testing-and-chaos.md gap 9 |
| T16 | The status RPC throttled the node it observed. Each 10 ms poll deep-copied an ~18 000-bid auction under the state-machine lock, and commits fell to about one every 0.4 s. Second round, measured: one snapshot held the lock 7 / 33 / 190 ms at 1 000 / 5 000 / 25 000 bids, and leader commits decayed from ~3 800/min to 28/min | failover stress measurement; `n3.log` | yes (`d48a52d`): cheap cache check, recompute ≤ every 0.5 s, digest opt-in per request | decisions.md T16 |
| T16 side | M1's `apply()` scans an auction's whole bid list per PlaceBid: 0.15 ms at 1 000 bids, 2.85 ms at 25 000 | measured offline while chasing T16 | **not changed** (M1 code); the stress writer starts a new auction every 1 000 bids | testing-and-chaos.md gap 10 |
| T17 | One transient `PermissionError [WinError 5]` on `os.replace(tmp, state.json)` fail-stopped FileStorage for good. n3 never changed term again, on a healthy disk | failover stress at 0.15-0.3 s timeouts, `n3.log` 22:49:26 | yes: pinned strict xfail (`6e8c183`), then bounded retry (`1691f95`, 10 ms doubling, ≤ 0.5 s). **Which process held the file was not logged** (inference: antivirus or indexer) | decisions.md T17 |
| T18 | Client-observed failover ~2.2 s in every config, even when the election took 0.44 s. Cause, measured: Windows refuses a connect to a dead localhost port only after 2.0 s (WSAECONNREFUSED 10061 after 2.02-2.03 s) | comparing `t_elect` with `t_write` | **not tuned**, reported | decisions.md T18; stress-results.md |
| T19 | The second 10-minute chaos run (seed 79402) killed its own harness at 562 s: exit code 1, no traceback, every Python process gone. Inferred mechanism: Windows reused a PID, and a stale parent link made "kill this node's tree" include the harness's launcher. **Inferred, not observed:** the process snapshot wasn't logged | the aborted run | yes (`b3673a7`): a child must be created after its parent; the harness is never in the tree | decisions.md T19 |
| `0beeb68` | Checker: a node that gave no dump must *fail* `replicas-identical`, never be skipped | commit message only; how it was found is not recorded | yes | `git show 0beeb68` |
| A16 | A new leader could refuse a valid token whose Login it hadn't applied yet (the user logged out at exactly the failover moment) | design review (Part A Known gaps) | yes: read barrier before refusing an unknown token | auction-on-raft.md Part A gaps; decisions.md A16 |

## 4. Surprises (measured or observed)

| fact | source |
|---|---|
| With the paper's 150-300 ms timeouts, the term went from 2 to 68 in 17 kills, about 4 terms per kill. In an earlier discarded run it went 1 → 140 by kill 13. With 0.5-1.0 s it rose by exactly one per kill (1 → 22 over 20 kills) | stress-results.md "Finding" |
| With no randomness (0.15-0.15 s) the paper's result was reproduced: median election 11.6 s, half the kills had no leader within 30 s, and 1 866 terms went by in 10 kills | stress-results.md; `runs\stress-20261109-234519\` |
| Raft replaced a crashed leader in ~0.9 s (default) or ~0.46 s (0.15-0.3 s), but the client waited ~2.2 s either way (Windows connect refusal) | stress-results.md; T18 |
| A venv's `python.exe` on Windows is a launcher with the real interpreter as its child | T6 |
| The monitoring RPC was the bottleneck, not Raft | T16 |
| Windows can't fsync a directory from Python: `os.open` on a directory raises `PermissionError` (measured) | decisions.md P3 |
| fsync cost on this machine: about 0.25 ms per log append and 1.2 ms per state-file replace | decisions.md P8 |
| One write at 1 client takes ~4.6 ms, with two fsyncs on the critical path. 16 clients give 3.3× the throughput at 5× the latency | stress-results.md |
| A real failover trace contained a split vote (term 2) and then §5.4.1 refusing a vote to the node missing index 8 (term 3) before n2 won in term 4 | auction-on-raft.md §11 |
| `digest_index` can trail `lastApplied`, because no-ops never reach `apply_fn` | decisions.md T4, R9 |
| Ollama wasn't running during any recorded chaos or demo run. Every description call failed fast and degraded to `""` | testing-and-chaos.md gap 6; `runs\demo-20261109-220454\logs\llm.log` |
| Restarted nodes' logs grew to 39 507 entries in the failover stress run, and 14 kill attempts waited a full 60 s for `settled()`. That replay caused this is **inferred, not measured** | stress-results.md "The 'not settled' waits" |

## 5. What you might do differently (each is a documented alternative or gap; the opinion is yours)

| candidate | where it's documented |
|---|---|
| PreVote or §6's "ignore RequestVote within the minimum election timeout" rule, so a resumed follower doesn't depose a healthy leader | decisions.md T15; testing-and-chaos.md gap 1 |
| §5.3's conflict-term hint (or "reply with my log length") instead of one RPC per missing entry | decisions.md R6; testing-and-chaos.md gap 11 |
| Snapshots / log compaction, to bound replay time and the log size | decisions.md P11; testing-and-chaos.md gap 14 |
| `MOVEFILE_WRITE_THROUGH` or two in-place slots for `state.json`, for power-loss durability on Windows | decisions.md P3; raft-persistence.md gap 1 |
| Keep the current high bid in the auction instead of scanning bids (an M1 `apply()` change) | testing-and-chaos.md gap 10 |
| A client connect timeout, or skip the remembered node after an error, to cut the 2 s Windows floor | decisions.md T18 |
| A Job Object per node instead of walking the process tree | decisions.md T19 (alternatives) |
| Instrument the replay rate; log which process holds `state.json` on a refused replace | stress-results.md; decisions.md T17 |
| Put reads (`GetAuction`, `ListAuctions`) in the chaos workload: today it only writes | `harness/workload.py` (no read calls); viva-prep.md "can't fully answer" |
| Run `demo.ps1` end to end, and run once with Ollama up | demo-runsheet.md "What has and hasn't been run" |
| An injectable clock for the timing tests | raft-election.md gap 6; raft-replication.md gap 11 |
| Network-partition faults between real processes (today only the in-memory transport partitions) | testing-and-chaos.md gap 4 |

## 6. What was hard (evidence in the repo)

| fact | source |
|---|---|
| The Figure 8 situation never arises by itself once the no-op exists. Testing it needed a message-losing `Gate` that only delivers what the leader really sent | decisions.md R17 |
| Two fixes needed a second round: T16 (throttle, then opt-in digest) and the no-op (test workaround first, then reverted) | decisions.md T16, R9 |
| Three things are inferred rather than measured: the cause of the original stall (R9), who held `state.json` (T17), and the PID-reuse mechanism (T19). The replay attribution is inferred too | decisions.md R9, T17, T19; stress-results.md |
| The timing tests use the real clock, so they bound waits generously and were repeated (12 clean runs in the election chunk; 10 repeated runs of the 87 raft tests after the no-op) | raft-election.md gap 6; raft-replication.md §6 |
| "Same seed" fixes only the schedule. A failing run has to be analysed from its files, never replayed | decisions.md T9 |
| Several Windows-specific behaviours shaped the code: launcher processes, the 2 s connect refusal, PID reuse, sharing violations on rename, no directory fsync, the 15.6 ms timer tick, console selection freezing writes | T6, T18, T19, T17, P3, E1, E11 |

## 7. What was verified, and how

| claim | how verified | source |
|---|---|---|
| Election Safety (one leader per term) | 15-seed randomised test with aggressive timeouts, observing every transition into LEADER; `at-most-one-leader-per-term` over 77 sightings in the final chaos run | decisions.md E19; testing-and-chaos.md §6 |
| Figure 8: an old-term entry on a majority is not committed | `test_figure8_old_term_entry_on_a_majority_is_not_committed` | decisions.md R8 |
| The no-op commits old entries with no new proposals | `test_old_term_entries_commit_after_a_leader_change_with_no_new_proposals` (times out against the old `node.py`) | decisions.md R9 |
| Persistence: no double vote and no lost acknowledged write after restart | restart tests, plus strict xfails proving the same tests fail on MemoryStorage | decisions.md P13; raft-persistence.md §5 |
| A full restart rebuilds byte-identical auction, session and dedupe state | `test_a_full_restart_rebuilds_identical_auction_and_session_state` | auction-on-raft.md §12 |
| Exactly-once retries across a leader kill | real gRPC trace (4 attempts, one seq, one bid); `test_client.py`; checker in chaos (3 committed unknowns, each once) | auction-on-raft.md §11; testing-and-chaos.md §6 |
| Each checker property can fail | `tests/test_checker.py`: 23 tests, one broken input per property | testing-and-chaos.md §4 |
| Under 10 minutes of chaos (58 faults), all 8 properties hold | `scripts/chaos.py`, seed 634526; re-checked with `python -m checker` on 2026-11-10 | `runs\chaos-final2.out`; testing-and-chaos.md §6 |
| Throughput and failover numbers | `scripts/stress.py` | stress-results.md; `runs\stress.out`, `runs\failover.out` |
| One node elects itself and serves the client flow | run by hand on 2026-11-10 (no LLM) | README "One node" |
| The submission zip contains what it should and nothing it shouldn't | `tests/test_make_submission.py`; built, unzipped, suite run from the copy | README "Submission zip" |

**Not verified:** power loss (P3, raft-persistence.md §8), whether this
machine's fsync reaches stable media (raft-persistence.md §8), network
partitions or clock skew between real processes, disk faults under chaos,
reads under chaos, real LLM output in any recorded run, `demo.ps1` run end to
end, the POSIX directory-fsync branch (not run on this machine).

## 8. Numbers at a glance (copied; see the sources)

| number | source |
|---|---|
| 210.5 writes/s at 1 client (p50 4.6 ms); 688.1 writes/s at 16 clients (p50 22.8 ms); 0 failed | stress-results.md |
| Election after a leader kill: p50 892.7 ms at 0.5-1.0 s; 462 ms at 0.15-0.3 s | `runs\failover.out` |
| Client's first write after a kill: p50 2 209 ms (default) / 2 220 ms (0.15-0.3 s) | `runs\failover.out` |
| Final chaos: 600 s, 58 faults, 11 256 writes, 8 957 ok bids, 88 unknown bids (3 committed), 63 auctions all closed, `digest_index` 11 293 on all three, 39 leader terms seen (last 159), 8/8 PASS | testing-and-chaos.md §6; `runs\chaos-final2.out` |
| Bid latency under that chaos run: p50 8.1 ms, p95 145 ms, max 5.65 s (it was p50 47 ms before T16) | testing-and-chaos.md §6 |
| Test suite: 377 passed, 3 xfailed (the three strict MemoryStorage xfails) | full run at the end of chunk 6 |
