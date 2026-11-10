# Requirements audit (Option F, R1-R14)

Written at the START of chunk 7 (Step 0), from the code as it was, not from
the docs. The final status of every row is added at the end of the chunk, in
the section "Final status".

Source of the requirement list: the R1-R14 list given at the start of chunk 7.
The professor's PDF itself is **not in the repo** (only `papers/raft-extended.pdf`
and the team's own `Documents/Problem_Statement.md`), so the wording of R11 and
R12 was not checked against the PDF.

## Step 0 findings that decide the plan

| question | answer from the code |
|---|---|
| What does the M1 LLM server offer? | Three RPCs in `llm_server/llm_servicer.py`: `AnswerBiddingQuestion` (FAQ, line 346), `GenerateItemDescription` (line 362), `SummariseAuctionResult` (line 387). Model `qwen3:4b-instruct-2507-q4_K_M` (`llm_server/config.py:13`), 30 s Ollama timeout, port 50052. All three work against a live model. |
| Does the app server call it at all? | **Only for descriptions.** `server/auction_servicer.py:66` `_describe()` calls `LLMClient.get_llm_answer` inside `CreateAuction`. Nothing calls the FAQ or the summary RPC except the manual script `scripts/llm_rpc_smoke.py`. |
| Does `CreateAuction` record a seller? | **Yes.** `auction/state.py:21` `owner`, set from the validated token (`auction_servicer.py:120`), stored at `state.py:86`, replicated via `commands.proto` `CreateAuction.owner`. |
| Do the RPC names match R12? | **No.** `getLLMAnswer` exists as the snake_case adapter `LLMClient.get_llm_answer` (`server/llm_client.py:73`) but returns only the answer string, not `(requestId, answer)`. `processBusinessRequest` does not exist anywhere. The wire has typed RPCs (`CreateAuction`, `PlaceBid`, ...). |
| Is there a `post` / `get` on the client? | **No.** `client/auction_client.py` has `login`, `logout`, `create_auction`, `place_bid`, `get_auction`, `list_auctions`. `proto/auction.proto` only has a comment saying a wrapper dispatches on `type`. `handout-signatures-need-real-functions` (memory) says this needed checking; it is missing. |
| Is there any payment / escrow code? | **None.** `grep` finds no payment or escrow in `auction/`, `server/`, `replication/`, `client/`, `checker/`, `harness/`. README "Known limitations" already says so. |
| Does the LLM call have a deadline on the app side? | **No.** `LLMClient` calls the stub with no `timeout=` (`llm_client.py:86,90,103`). The only bound is Ollama's 30 s inside the LLM server. A dead LLM process gives UNAVAILABLE at once (tested with a fake); a hung LLM *process* would hold a handler thread until the client gives up. Not tested. |
| Was real generated text ever seen through the cluster? | README says every recorded chaos run and the recorded demo run were made **without** Ollama; descriptions degraded to `""`. Real text has only been seen via `scripts/llm_rpc_smoke.py` and, in this session, one FAQ call straight to the LLM server (15.75 s cold, answer correct, `request_id` echoed). |

## Table

Status: DONE / PARTIAL / MISSING. "Shows it" is a test or a recorded run.

| # | requirement | status | implemented in | shows it |
|---|---|---|---|---|
| R1 | Real-time bid placement with concurrency control | **DONE** | `auction/state.py:68` `apply()` holds one lock across read-decide-write (`:76`); `PlaceBid` branch `:95`; write path `replication/replicator.py:98` `submit`; handler `server/auction_servicer.py:129` | `tests/test_concurrent_bidding.py::test_fifty_concurrent_equal_bids_exactly_one_wins`; racers in `harness/workload.py:219`; chaos seed 634526 (README Results) |
| R2 | Automated auction timer and winner determination | **DONE** | `server/closer.py:47` `due_auction_ids`, `:92` `tick` (leader-only, proposes `CloseAuction` with the leader's clock); winner = max bid in `auction/state.py:127-147` | `tests/test_close_auction.py` (8 cases), `tests/test_closer.py` (incl. `test_after_failover_the_new_leader_closes_what_is_overdue`) |
| R3 | Payment and escrow (mock acceptable) | **MISSING** | nothing. `SettlePayment` is named in the problem statement and in `MyNotes` only | none |
| R4 | LLM bidding-assistant FAQ | **PARTIAL** | LLM side done: `llm_server/llm_servicer.py:346`, state injected into the system turn (`build_faq_messages:188`); adapter `LLMClient.get_llm_answer` (`llm_client.py:79`). **App side missing:** no RPC, nothing reads auction state and calls it | `tests/test_llm_servicer.py` (FAQ cases), `tests/test_llm_client.py::test_query_present_with_auction_context_attaches_it`; one live call this session. Not reachable from a client |
| R5 | LLM item descriptions | **DONE** (code + fake-LLM tests); live path through the cluster **unverified** | `server/auction_servicer.py:66` `_describe`, then `CreateAuction` command carries `description` as data (`:116-122`); stored `state.py:87` | `tests/test_auction_on_raft.py::test_the_llm_text_is_replicated_as_data`, `::test_llm_down_the_auction_is_still_created_with_an_empty_description`, `::test_a_follower_never_calls_the_llm`; `tests/test_auction_servicer.py`. No recorded run with real text |
| R6 | LLM summaries of results and price trends | **PARTIAL** | LLM side done: `llm_servicer.py:387`, `build_summary_messages:225`. **App side missing:** no `RecordSummary`, no closer hook, no price-trend read, `AuctionContext` mapper only used for FAQ/summary in tests | `tests/test_llm_servicer.py` (summary cases), `tests/test_llm_client.py::test_bid_history_dispatches_to_summarise_auction_result`. Never called by a node |
| R7 | State and bid history replicated with Raft | **DONE** | `raft/node.py`, `raft/file_storage.py`, `replication/state_machine.py:94` `apply_fn`, `replication/codec.py` | `tests/test_raft_*.py`, `tests/test_auction_on_raft.py`, `tests/test_restart_replay.py`, `tests/test_serve_grpc.py`; chaos seed 634526: all 3 nodes at `digest_index` 11 293, 8/8 properties pass |
| R8 | Fault tolerance on leader/follower failure in an active auction | **DONE** | leader election + log repair in `raft/`; client retry `client/auction_client.py:135` `_call`; closer re-proposes on the new leader (`server/closer.py`) | `tests/test_client.py::test_kill_the_leader_mid_auction_same_token_no_acknowledged_bid_lost`, `tests/test_failover_grpc.py`, `tests/test_workload.py`; 10-minute chaos run seed 634526; `scripts/demo.py` run (without LLM) |
| R9 | LLM on a separate, CPU-friendly, domain-specific, context-aware server reached by the app nodes | **PARTIAL** | separate process `llm_server/serve.py`; 4B q4 model; domain prompts `_FAQ_SYSTEM`, `_DESCRIPTION_SYSTEM`, `_SUMMARY_SYSTEM`; context injected. App nodes use it for descriptions only (R4/R6 gap). **No app-side deadline** on the stub call | `tests/test_llm_servicer.py`; live FAQ call this session; `docs/design-notes.md` (latency measurements). Slow/hung LLM *process* is untested |
| R10 | All inter-service communication over gRPC, Python only | **DONE** | clients -> nodes, node <-> node (Raft), node -> LLM server, dashboard/harness -> status: all gRPC (`proto/*.proto`, `generated/`). One honest note: the LLM server talks to the local Ollama daemon over Ollama's own HTTP API (`ollama.Client`); that is LLM-server-internal, not service-to-service | `tests/test_serve_grpc.py`, `tests/test_llm_client.py` |
| R11 | Client functions: `login`, `logout`, `post(token,type,data)`, `get(token,type,params)` | **PARTIAL** | `login`/`logout`: `client/auction_client.py:170,177`. **`post` and `get` do not exist** | `tests/test_client.py` covers login/logout/typed calls. Nothing covers `post`/`get` |
| R12 | `processBusinessRequest(requestId, payload, context)`, `getLLMAnswer(requestId, query, context) -> (requestId, answer)` | **PARTIAL** | `getLLMAnswer`: real but snake_case and returns `str` only (`llm_client.py:73`). The wire response does carry `request_id` + `answer` (`LLMAnswerResponse`). **`processBusinessRequest`: missing**; the typed RPCs are the only entry points | `tests/test_llm_client.py` for the dispatch |
| R13 | Demo shape: LLM server, leader, 2 followers, clients | **PARTIAL** | `scripts/demo.py` (starts LLM + 3 nodes via `harness/procs.py:290` `LocalCluster`, 4 clients), `scripts/demo.ps1`, `scripts/run_cluster.ps1`, `scripts/dashboard.py` | `demo.py` was run with `--pause 0.3`, 8/8 properties pass, **without Ollama**. `demo.ps1` was only syntax-checked, never run end to end (README). Does not show payment, FAQ or summary |
| R14 | Deliverables: zip, 5-10 min video, README, reflection | **PARTIAL** | `scripts/make_submission.ps1` (tested by `tests/test_make_submission.py`), `README.md`, `docs/reflection-facts.md` | The video and the reflection are the student's to make. The zip script currently ships `docs/viva-prep.md` and `docs/reflection-facts.md` (personal notes) and leaves out `Documents/Problem_Statement.md` |

## What the plan covers

| row | covered by |
|---|---|
| R3 | Step 1 (payment and escrow) |
| R4, R5 (live check), R6, R9 (deadline), R12 `getLLMAnswer` | Step 2 |
| R11 `post`/`get` | built in Steps 1-2, because the payment and price-trend calls are specified through them |
| R12 `processBusinessRequest` | **decision needed, see below** |
| R13 | Step 4 (demo end to end, with the LLM up) |
| R14 | Step 4 (zip script, README); the video and the reflection stay yours |

## The one decision this audit cannot make for you (RESOLVED: a real server RPC was built, see C19 and the final status)

`processBusinessRequest(requestId, payload, context)`. The M1 design (auction.proto,
llm.proto) put typed RPCs on the wire and kept the PDF's untyped signatures as
adapters on top. `get_llm_answer` is the precedent. The same pattern for
R11/R12 is: the wire stays typed; `ClusterClient` gets real
`post`, `get` and `process_business_request(request_id, payload, context)`
adapters that dispatch to the typed RPCs. That keeps the chaos harness and the
checker on the typed API and does not add a second wire surface. It is the plan
unless you want a real `ProcessBusinessRequest` RPC. The cost of that: a
second entry point to the same state machine, with `payload`/`context` as maps
of strings, which `auction.proto` already turned down once. The adapter lives
on the client side, not in the server process. Say so in the viva if asked.

---

## Final status (end of chunk 7)

Re-checked against the code and the evidence below. The table at the top of
this file is the state BEFORE the chunk; this is after.

| # | requirement | final | implemented in | evidence |
|---|---|---|---|---|
| R1 | Bid placement with concurrency control | **DONE** | `auction/state.py` `apply()`, `server/auction_servicer.py` `PlaceBid` | `test_concurrent_bidding.py`; chaos seeds 634526 and 396704 (1 224 races in the latter) |
| R2 | Timer management and winner determination | **DONE** | `server/closer.py`, `apply()` `CloseAuction` | `test_closer.py`, `test_close_auction.py`; the demo's close |
| R3 | Payment and escrow (mock) | **DONE** (mock gateway; known gaps below) | `auction/state.py` `PayAuction`/`ReleaseEscrow`/`RefundEscrow`; `server/payment_gateway.py`; `server/payment_servicer.py` | `test_escrow_state.py`, `test_escrow_on_raft.py`, `test_payment_grpc.py` (pay, kill leader, same escrow); chaos 396704: 64 payments, 54 releases, 10 refunds, 10/10 PASS; demo |
| R4 | LLM FAQ chatbot | **DONE** | `server/assistant_servicer.py` `AskAssistant` (leader reads the state, sends it as context via `getLLMAnswer`) | `test_assistant_on_raft.py`; `test_assistant_grpc.py` (the live auction is in the prompt); demo: 1.14 s |
| R5 | Rich item descriptions | **DONE** | at creation `AuctionServicer._describe`; on request `GenerateDescription` -> `SetDescription` (text as data) | tests above; demo: 1.83 s with the real model |
| R6 | Summaries of results and price trends | **DONE** | `server/summarizer.py` `SummaryWorker` -> `RecordSummary`; `GetPriceTrends` + `SummarisePriceTrends` | `test_summarizer.py` (new leader finds an unsummarised close); chaos 396704: 69 of 69 auctions summarised once; demo: summary 5.56 s after the close, trends 2.28 s |
| R7 | State and bid history replicated with Raft | **DONE** | `raft/`, `replication/` (untouched in chunk 7; escrow and summaries are in the digest) | suite; chaos: `replicas-identical` at `digest_index` 12 099 |
| R8 | Fault tolerance in an active auction | **DONE** | as before; client retry; closer and summary worker re-derive work on a new leader | `test_client.py`, `test_failover_grpc.py`, `test_payment_grpc.py`; chaos 396704 (57 faults); demo (two kills) |
| R9 | LLM on a separate, CPU-friendly, domain-specific, context-aware server | **DONE** | `llm_server/` (own process, 4B q4 model, domain prompts); 25 s deadline in `LLMClient`; failures are clear answers | `test_llm_client.py::test_a_hung_llm_server_raises_deadline_exceeded_at_the_deadline`, `test_assistant_grpc.py` (hung and dead servers; bidding and closing carry on) |
| R10 | gRPC only, Python only | **DONE** | all service-to-service calls are gRPC; the LLM server's call to the local Ollama daemon is Ollama's own HTTP API (LLM-server-internal) | suite |
| R11 | `login`, `logout`, `post(token,type,data)`, `get(token,type,params)` | **DONE** | `client/auction_client.py` `login`, `logout`, `post`, `get` | `test_client_post_get.py` (22 tests) |
| R12 | `processBusinessRequest`, `getLLMAnswer` | **DONE** | `getLLMAnswer` = `LLMClient.get_llm_answer`, returns `(requestId, answer)`. `processBusinessRequest` = the server RPC `BusinessService.ProcessBusinessRequest(request_id, payload, context)` (`proto/business.proto`, `server/business_servicer.py`): a dispatcher to the existing handlers with the same gRPC context, behind the same auth interceptor; `ClusterClient.process_business_request` calls it. The typed RPCs remain | `tests/test_business_rpc.py` (37 tests: every type dispatches; no token or a token only in payload/context is UNAUTHENTICATED; identity from the token; a follower redirects with leader-addr; unknown outcome is UNAVAILABLE; dedupe by `(client_id, seq)`; 15 malformed payloads are INVALID_ARGUMENT and change nothing; over real gRPC), `test_llm_client.py`; decisions.md C19. Limit: the payload is untyped JSON, validated by hand |
| R13 | Demo shape: LLM server, leader, two followers, clients | **DONE** | `scripts/demo.py`, `demo.ps1` | `demo.ps1 -Pause 0.5` end to end with the real LLM on 2026-11-10: exit 0, 90.16 s, 10/10 PASS. `-Pause 4` itself not run |
| R14 | Deliverables | **PARTIAL (the student's part)** | `scripts/make_submission.ps1` (excludes the two personal docs, includes this file and the problem statement), README, `docs/` | `test_make_submission.py` (9 tests). The video and the individual reflection are the student's to make; `docs/reflection-facts.md` is raw material for it |

**Not DONE, and why:** only R14 is partial, because a video and a reflection
cannot be produced by the code. Everything else is DONE, with the limits in
[payment-and-llm.md](payment-and-llm.md) section 4 (the mock gateway, the
model's text not tested for truth beyond its numbers: the summary prompt gets
computed facts and no times, one live call still wrote 40 for 400, and so the
worker now checks the summary's numbers in code against facts computed from the
state and replaces a failing text with a template, C23; it cannot catch two valid
numbers swapped or numbers written as words; the chaos run's LLM was a canned
stand-in).

Suite at the end of the chunk: last full run 654 passed, 1 failed, 3 xfailed; the failure was a stale assertion in `test_assistant_grpc.py`, fixed and re-run on its own (passed).
