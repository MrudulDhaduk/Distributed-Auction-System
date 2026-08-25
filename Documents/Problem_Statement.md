# Distributed Online Auction System

**Course:** CS G623 — Advanced Operating Systems, First Semester 2026–27
**Instructor-in-charge:** Prof. Asish Bera
**Selected architecture:** Option F — Distributed Online Auction System

| Team member | ID | Primary responsibility |
| --- | --- | --- |
| *[Name]* | *[ID]* | Application server, auction logic, Raft |
| *[Name]* | *[ID]* | Client tooling, LLM server, evaluation harness |

> **Team size note.** We are currently a two-member team and are seeking the Instructor-in-charge's approval to proceed at this size. Scope below is planned for two members; we will expand the stretch goals in §8 if a third member joins.

---

## 1. Problem statement

An online auction has a correctness property that a single-server implementation gets for free and a distributed one does not: **at any instant there is exactly one highest bid, and when an auction closes there is exactly one winner.** Two bidders submitting the same amount at the same moment must not both be told they are leading. An auction that expires must expire at one agreed-upon moment, not at whatever moment each server happens to notice.

Replicating the auction across several servers for availability makes both properties hard to hold. Replicas can disagree about the current high bid, a server can accept a bid and crash before telling anyone, and two servers can each believe they are in charge and close the same auction with different winners. A bid that a bidder has been told was accepted must not disappear when the server holding it dies.

This project builds a distributed auction system that keeps those guarantees while tolerating server failure. Auction state is replicated across a cluster using the **Raft consensus protocol**; all inter-service communication uses **gRPC**; and a separate **LLM server** provides domain-specific assistance to bidders and sellers. The system is required to remain correct — not merely available — when the node coordinating an auction is killed while bidding is in progress.

## 2. Correctness properties targeted

These are the properties the system is built to hold and the evaluation harness (§6) is built to check:

1. **Single winner.** For any closed auction, all live replicas report the same winner and the same winning amount.
2. **No lost acknowledged bid.** If a bidder receives an acceptance for a bid, that bid is present in the auction history after any subsequent leader failure and recovery.
3. **Monotonic high bid.** The reported current high bid never decreases within an auction, including across a leader change.
4. **Agreed close time.** An auction's expiry and settlement are decided once, by the cluster, rather than independently by each node.
5. **Availability under single-node failure.** With three replicas, the cluster continues accepting bids after any one node crashes.

## 3. System architecture

Five nodes, as separate processes communicating exclusively over gRPC:

| Node | Role |
| --- | --- |
| Node 1 | LLM server — CPU-optimised domain model, stateless, called by application servers |
| Node 2 | Application server, initial Raft leader — auction logic, bid validation, settlement |
| Node 3 | Application server, Raft follower — replicates auction log |
| Node 4 | Application server, Raft follower — provides quorum for leader election and recovery |
| Node 5 | Client node(s) — multiple concurrent bidders, monitor, and evaluation harness |

**Design constraint carried through both milestones.** Every state-changing operation is expressed as a `Command` message (`PlaceBid`, `CreateAuction`, `ExtendAuction`, `CloseAuction`, `SettlePayment`) and applied through a single `apply(command)` entry point that owns all auction state. In Milestone 1 the gRPC handler invokes `apply` directly. In Milestone 2 Raft is placed in front of `apply`: commands are replicated and committed before being applied, and no auction logic changes. This keeps the consensus layer and the business logic independently testable.

## 4. Milestone 1 — Foundation, gRPC and LLM integration

*Deadline: 28 September 2026*

- **gRPC service definitions** covering client-facing operations (`login`, `logout`, `post`, `get`), the auction command set, the LLM service, the node status service (§6.1), and the Raft RPCs (`RequestVote`, `AppendEntries`) declared in advance for Milestone 2.
- **Authentication and sessions** — token issue on login, token validation on every subsequent RPC, logout invalidation.
- **Auction business logic** — create auction, list open auctions, place bid, retrieve auction state and bid history, all routed through `apply`.
- **Concurrency control** — serialised application of bids such that concurrent submissions of equal value produce exactly one winner and an honest rejection for the other. Verified by a test that issues concurrent bids from multiple clients and asserts the single-winner property.
- **Auction timer and settlement** — automatic close on expiry, winner determination, and a mock payment/escrow service.
- **LLM integration** — a separate server running a CPU-deployable model, invoked by the application server over gRPC, providing: (a) a bidding-assistant FAQ chatbot, (b) automated generation of item descriptions from seller-supplied attributes, and (c) summarisation of auction results and price history. Context awareness is achieved by injecting live auction state into the prompt rather than answering from the model's own knowledge.
- **Project structure**, README with setup instructions, and sample query transcripts.

## 5. Milestone 2 — Raft consensus and fault tolerance

*Deadline: 18 November 2026*

- **Leader election** — terms, randomised election timeouts, vote request and grant, split-vote resolution.
- **Log replication** — `AppendEntries` with consistency check, follower log repair, commit index advancement on quorum acknowledgement.
- **Persistence and recovery** — durable term, vote, and log; a restarted node rejoins and is brought up to date by the leader.
- **Failure detection and recovery** — heartbeat-based detection; the cluster elects a new leader and resumes accepting bids.
- **Demonstration of consistency after leader kill.** The core demonstration: bidding is in progress on an open auction, the leader process is killed, a new leader is elected, bidding continues, the auction closes, and all surviving replicas agree on the winner and the full bid history. Every bid acknowledged before the kill is still present afterwards.
- **Stress testing** — sustained concurrent bidding across multiple clients under injected node failures, driven and verified by the harness in §6.

## 6. Design extensions beyond the baseline

The baseline requirement is a system that survives a leader failure. Our position is that a distributed system claiming fault tolerance should be able to *demonstrate* it rather than assert it, so three components are built specifically to make the guarantees in §2 observable and checkable. These are committed scope, not stretch goals.

### 6.1 Live cluster dashboard

Every application server exposes a `GetNodeStatus` RPC reporting its Raft role, current term, log length, commit index, and view of the current high bid. A monitor client polls all servers several times a second and renders a live terminal table.

This makes consensus visible rather than inferred. During the demonstration, killing the leader produces an observable sequence — followers time out, terms increment, a candidate campaigns, a new leader is established — instead of a pause followed by a claim that recovery occurred. It also serves as the primary debugging instrument for the Raft implementation throughout development.

### 6.2 Acknowledged-bid property checker

Each client appends every bid it receives an acceptance for to a local record. After a run, a checker reads final state from all surviving nodes and verifies the properties in §2: that every acknowledged bid survives, that all nodes agree on the winner and the bid history, and that the reported high bid was monotonic.

This is the difference between "the system appeared to keep working" and a verified claim. Durability of an acknowledged write is the guarantee a bidder actually relies on, and it is the guarantee least likely to be checked by inspection.

### 6.3 Seeded chaos harness

Fault injection is driven by a seeded pseudo-random generator. A single integer determines the entire fault schedule for a run — which nodes are killed, at what times, and when they are restarted. The same seed produces the same schedule, so a run that violates a property can be re-executed rather than waited for.

We state the limitation explicitly: because the system runs as real processes over a real network, replay reproduces the *fault pattern*, not the exact interleaving of messages. It does not make failures perfectly deterministic. It does make them repeatable enough to investigate, which is the practical difficulty with consensus bugs — they are rare, timing-dependent, and ordinarily gone by the time you look.

Combined with §6.2, this yields a reportable evaluation: a table of seeds, the faults each injected, the properties checked, and the outcome.

## 7. Technology

Python 3, `grpcio` and `grpcio-tools` for service definitions and code generation, Protocol Buffers for the wire format. Raft implemented from the Ongaro–Ousterhout paper rather than taken from an existing library, since the consensus implementation is the substance of the project. LLM served locally on CPU via a small instruction-tuned model.

## 8. Stretch goals

Undertaken only if §4–§6 are complete and stable:

- **Anti-snipe auction extension.** A bid placed in the closing seconds extends the auction. The extension is itself a replicated command, so that nodes cannot disagree about when the auction ends — making the "agreed close time" property non-trivial.
- **Raft-replicated sessions.** Placing login tokens in the replicated log, so that a session established against one server survives that server's failure.
- **LLM-assisted failure explanation.** Feeding a failing run's trace and fault schedule to the LLM server to produce a readable account of the sequence of events that led to a violated property.

## 9. Deliverables

Source code with README covering setup, deployment and usage; a 5–10 minute demonstration video showing multi-node operation, concurrent bidding, and recovery from leader failure; the evaluation table produced by §6; and individual reflection documents. All submitted work will be original to the team.

## 10. Risks and mitigation

| Risk | Mitigation |
| --- | --- |
| Raft is the largest single piece of work | Isolated behind `apply()` from week 1; leader election built and tested before log replication |
| Two-member team on a three-member scope | Instructor approval sought; §8 treated as stretch, not planned scope |
| LLM setup and model download can consume days | Model selected and downloaded in the first fortnight, before it blocks integration |
| Consensus bugs are rare and timing-dependent | Dashboard (§6.1) for live observation, seeded chaos (§6.3) for repeatable reproduction |