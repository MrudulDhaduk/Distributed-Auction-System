# How this project works

An explainer for the team. It starts from a simple picture and then makes each piece precise. Read it once before writing code; the vocabulary here is the vocabulary the viva will use.

---

## 1. The thing being built

An online auction. Sellers list items, bidders place bids, the highest bid at closing time wins.

That's the surface. The actual project is underneath it: keeping several servers in exact agreement about who is winning, even when some of them die mid-auction. The auction is a well-chosen excuse — it's a domain where disagreement is immediately visible and obviously unacceptable. If two servers disagree about who won a toy, everyone can see the system is broken. That clarity is why the domain was picked.

## 2. Why one server isn't enough

Start with the simple room. Kids shout prices, one person writes each shout in a notebook, the highest shout at the end wins.

This works perfectly until the notebook-keeper falls asleep. Then two bad things happen at once, and they're worth separating because they need different fixes:

**Availability is lost.** Nobody can bid, because there's nobody to record bids. Annoying, but recoverable — wake them up and carry on.

**Durability is lost.** The last few shouts, the ones the keeper heard and confirmed but that exist nowhere except in their head or on a page nobody else has seen, are gone. This is the worse failure, and it's the one that matters. A bidder was *told* their bid was accepted. That promise has now been broken. A system that occasionally forgets things it promised to remember is not a system anyone can build on.

So the fix has to address durability, not just availability. Restarting the server faster doesn't help; the data has to exist in more than one place before it's promised.

## 3. Why three notebooks creates a new problem

Give three people notebooks. Now one can fall asleep and the record survives.

But you've traded one problem for a harder one: **the notebooks can disagree.** One hears "twenty," another hears "thirty," a third was distracted and missed the shout entirely. Now there are three different answers to "who is winning," all of them written down, all of them equally official-looking. The system is *available* and *durable* and *wrong*, which is arguably worse than being down.

This is the central difficulty of distributed systems, and it's not solved by being careful. Messages get lost. Messages arrive out of order. A node can be slow rather than dead, so you can't tell the difference from outside. Two nodes can each be convinced they're in charge. You need a *protocol* — a set of rules that, if everyone follows them, makes disagreement impossible rather than unlikely.

Raft is that protocol.

## 4. Raft, properly

### 4.1 The log is the real data structure

The first idea to internalise: the notebooks don't store *the current high bid*. They store **an ordered list of everything that happened** — a log. Entry 1: auction created. Entry 2: Asha bid 20. Entry 3: Ravi bid 30.

The current high bid isn't stored anywhere. It's *computed* by replaying the log from the start.

This seems like a detour, but it's the whole design. Making two servers agree on "the current state" is hard, because state is a moving target. Making two servers agree on "an ordered list of things that happened" is tractable, because a list is append-only and positions are comparable. If two servers have the same log, they will compute the same state — automatically, with no further coordination.

This is why every state change in your system must be a `Command` object that goes through one `apply(command)` function. `apply` is the "replay the log" step. Raft's only job is to make sure every server has the same log, in the same order. Everything above that is free.

**Consequence: `apply` must be deterministic.** Same log, same result, every time, on every machine. No `random`, no `time.now()`, no reading the filesystem, no calling a language model. If `apply` is nondeterministic, two servers with identical logs compute different states and the entire foundation collapses. This constraint reappears in §7 and it's the single most important rule in the codebase.

### 4.2 One leader, chosen by vote

At any moment, one server is the **leader**; the others are **followers**.

Only the leader accepts new commands. Followers that receive a client request redirect it. This sounds like it defeats the point of having three servers — and for *throughput*, it does. Raft isn't trying to make things faster. It's trying to make them correct. Having exactly one writer removes the entire class of problems where two servers append conflicting entries at the same position.

Time is divided into **terms** — numbered periods, each with at most one leader. A term is a logical clock, not a wall clock: term 4 doesn't mean "four seconds," it means "the fourth attempt at having a leader." Terms only ever increase. Every message carries its sender's term, and this single fact does an enormous amount of work: a server that receives a message from a higher term immediately knows its own information is stale and steps down. A server that receives a message from a lower term ignores it, because the sender is behind.

### 4.3 A bid counts when a majority has it

Here is the sequence for a single bid, precisely:

1. A client sends "bid 500" to the leader.
2. The leader appends it to its own log. **It has not yet told the client anything.**
3. The leader sends `AppendEntries` to both followers: "add this entry at position 7."
4. Followers append it and reply "done."
5. Once the leader has heard from **a majority** — itself plus at least one follower, so 2 of 3 — the entry is **committed**.
6. Only now does the leader run `apply` and reply to the client: "your bid is accepted."

The gap between steps 2 and 6 is where all the safety lives. A bid that exists only on the leader is *not* committed and has *not* been promised to anyone. If the leader dies there, losing it is fine — nobody was told it succeeded. The promise is made only after the data is in a majority of logs.

**Majority means strictly more than half**: 2 of 3, 3 of 5. It does not mean "most of the ones that are responding." With three nodes, if two are down, the survivor cannot commit anything — it will sit there refusing writes. That's correct behaviour, not a bug, and it's worth knowing before you spend an afternoon debugging it. Raft chooses to stop rather than risk disagreeing.

### 4.4 Elections

The leader sends **heartbeats** — empty `AppendEntries` — every so often, meaning "still here, still leader."

Each follower runs an **election timeout**. If no heartbeat arrives before it expires, the follower assumes the leader is dead: it increments its term, votes for itself, becomes a **candidate**, and asks the others for votes. A candidate that receives votes from a majority becomes leader and starts heartbeating.

Two details that look like implementation trivia and are actually load-bearing:

**Election timeouts must be randomised.** If all three followers time out simultaneously, all three become candidates, all three vote for themselves, nobody gets a majority, and the term ends with no leader. Then it happens again. Randomising the timeout (say, uniformly in 150–300ms) means one node almost always times out first and wins before the others start. Without randomisation your cluster can livelock — permanently busy, permanently leaderless. This is the classic first bug.

**A server grants at most one vote per term.** This is what makes two simultaneous leaders impossible: two candidates in the same term would each need a majority, and two majorities of the same group must overlap in at least one server, which would have had to vote twice.

### 4.5 Why nothing gets lost — the argument worth memorising

This is the question to be ready for in the viva, because it's the heart of the protocol.

*If the leader dies right after committing a bid, how do you know the next leader has it?*

Two facts, both already established:

- A committed entry is in the logs of **a majority** of servers.
- A new leader needed votes from **a majority** of servers.

Any two majorities of the same set must share at least one member. So at least one server that voted for the new leader also has that committed entry.

That alone isn't quite enough — the overlapping server has the entry, but the *new leader* might not. Raft closes the gap with a voting restriction: **a server refuses to vote for a candidate whose log is less up to date than its own** (compared by the term of the last entry, then by length). So a candidate missing a committed entry cannot collect a majority — every server holding that entry will vote against it.

Put together: any server that can win an election necessarily already holds every committed entry. Committed data cannot be lost. This is Raft's **Leader Completeness** property, and it's the reason the whole thing works.

### 4.6 What "committed" and "applied" mean, and why they differ

Two indices per server, easy to confuse:

- `commitIndex` — the highest log position known to be stored on a majority. Safe. Will never be undone.
- `lastApplied` — the highest position actually fed to `apply()` and reflected in the auction state.

`lastApplied` chases `commitIndex`. Followers learn the leader's `commitIndex` from heartbeats and then catch up. A follower can hold entries in its log that it hasn't applied yet — because it doesn't yet know they're committed. Entries beyond `commitIndex` are provisional and can legitimately be overwritten by a future leader.

### 4.7 Repairing divergent logs

A follower can end up with entries the new leader doesn't have — accepted from an old leader that died before committing them. Raft's rule is blunt: **the leader's log is the truth.** Followers are forced to match it.

Every `AppendEntries` includes the index and term of the entry immediately preceding the new ones. The follower checks whether it has exactly that entry. If not, it rejects. The leader then steps its `nextIndex` for that follower back and retries, walking backwards until it finds the last point where the two logs agree, then overwrites everything after it.

Overwriting is safe *only* because of §4.5 — anything committed is guaranteed present in the leader's log, so the entries being deleted were never committed and never promised to anyone.

## 5. gRPC

The servers need to talk. gRPC is how.

You write a `.proto` file that declares every message and every remote procedure — `RequestVote`, `AppendEntries`, `PlaceBid`, `GetNodeStatus`. A code generator turns that into Python classes and client stubs. Calling a method on another server then looks like calling a local function.

Why it's mandated rather than plain HTTP: the interface is a **checked contract**. Both sides generate code from the same file, so a field can't be misspelled on one side and silently ignored on the other. Messages are a compact binary format, not text you have to parse and validate. And it's the standard tool in this space — the real Raft implementations you'd encounter in industry use exactly this shape.

Practically, `.proto` is the contract *between the two of you*. Whoever builds the server and whoever builds the client both code against it. Agree on it in week one and changing it later stays cheap.

## 6. The LLM server, honestly

For the distributed systems problem: it contributes nothing. It doesn't make the auction more consistent, more available, or more fault-tolerant. Delete it and every correctness property still holds. It's in the project because the assignment requires it — separate server, domain-specific, context-aware.

There is, though, a defensible reason it belongs, and it's the answer to give if asked.

**It forces a heterogeneous node into the architecture.** Without it, every node is the same kind of thing: interchangeable Raft peers plus clients. The LLM server is different in every way that matters — stateless, unreplicated, outside consensus, and slow. That difference raises real design questions with real answers:

*What if the LLM server is down?* Bidding continues, unaffected. It must not be on the critical path of any write.

*Can an LLM call happen inside `apply()`?* No. A slow model call inside `apply` would stall log application on every replica, meaning one slow helper can freeze the entire cluster.

*Can an LLM response be part of a replicated command?* **This is the interesting one.** No — and the reason is §4.1. If you replicated the command "generate a description for this item" and each server ran the model itself, each would produce different text. Identical logs, divergent state: a genuine safety violation, caused by putting a nondeterministic function inside the deterministic layer.

The correct design follows directly: **call the model first, outside consensus, then replicate the resulting text as ordinary data.** The command that enters the log is `SetDescription("Vintage brass lamp, circa 1960...")` — a fixed string. Every replica applies the same string and gets the same state.

That constraint — nondeterminism must stay outside the replicated log — is a real principle, not a homework artifact. The LLM is what makes it concrete in this project. Say it in the viva and the LLM stops looking bolted-on.

Everything the model does is read-side or pre-write: FAQ answers, item descriptions, result summaries. None of it sits between a bid and its acknowledgement. Keep it that way, keep it cheap, and don't let it creep toward deciding anything.

## 7. The three proof tools

The baseline assignment asks for a system that survives leader failure. Anyone can demonstrate that by killing a process and showing the system still responds. That's a performance, not evidence. These three turn it into evidence.

### 7.1 Dashboard — making consensus visible

Each server exposes `GetNodeStatus`: role, term, log length, commit index, current high bid. A monitor polls all three several times a second and prints a live table.

Consensus is otherwise invisible — it happens in microseconds between processes, and from outside all you see is a pause followed by things working again. The dashboard turns leader failure into a watchable sequence: heartbeats stop, followers time out, a term increments, a candidate campaigns, a new leader takes over. That's the memorable part of the demo, and it's also the instrument you'll debug Raft with all semester. It pays for itself in the first week.

### 7.2 Property checker — the receipt book

Every client records each bid it was told was accepted. After a run, the checker reads final state from every surviving server and asserts the promises held: every acknowledged bid is present, all servers agree on the winner and the full history, and the reported high bid never decreased.

The distinction it buys you is between "it appeared to keep working" and "here is a verified claim." Durability of an acknowledged write is the guarantee a bidder actually depends on and the one least likely to be caught by watching a demo — a bid can vanish silently while everything on screen looks healthy.

### 7.3 Seeded chaos — making bugs come back

Faults are driven by a seeded random number generator. One integer determines the whole schedule: which servers get killed, when, and when they restart. Same seed, same schedule.

The point is reproducibility. Consensus bugs are rare, timing-dependent, and typically gone by the time you look — you see a failure once, can't reproduce it, and eventually convince yourself it didn't happen. With a seed, you can run it again.

**State the limitation honestly in the writeup:** because these are real processes over a real network, replay reproduces the *fault pattern*, not the exact interleaving of messages. It doesn't make failures perfectly deterministic. It makes them repeatable enough to investigate, which is the practical difficulty.

Paired with the checker, this produces a reportable evaluation — a table of seeds, faults injected, properties checked, results. Very few submissions will have one.

## 8. Where you'll actually get stuck

Not a warning list; these are the specific places this project consumes days.

**gRPC setup.** Protobuf codegen and import paths are fiddly the first time. Budget a full day in week one and don't panic.

**Election livelock.** Fixed election timeouts cause endless split votes. If terms are climbing and no leader emerges, this is why.

**Concurrent bids.** Two bids arriving at once must produce one winner and an honest rejection. Serialise everything through `apply` under a single lock and test it with 50 simultaneous bids — don't reason about it, measure it.

**Log repair off-by-one.** The `nextIndex` backwards walk in §4.7 is where index arithmetic goes wrong. Whether your log is 0- or 1-indexed, write it down and be consistent, because half the Raft paper assumes 1-indexed.

**Persistence timing.** Term, vote, and log must reach disk *before* replying to any RPC. Replying first and writing after is a real bug that only shows up when you crash at exactly the wrong moment — which is precisely what the chaos harness is for.

## 9. What you're graded on

The notebooks agreeing.

The auction is the excuse. The LLM is a requirement. The dashboard, checker, and chaos harness are how you prove the claim rather than assert it. The substance — the thing that carries the marks and the thing the viva will probe — is the consensus layer: leader election, log replication, and recovery that demonstrably loses nothing it promised to keep.