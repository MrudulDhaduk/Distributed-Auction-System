# Stress results

Measured with [scripts/stress.py](../scripts/stress.py) on real node
processes (`python -m server.serve`, FileStorage, fsync on every write). Method
and its pitfalls: [decisions.md T14](decisions.md). These are the numbers the
machine gave on 2026-11-09, untuned.

## Machine and settings

| | |
|---|---|
| CPU | AMD Ryzen 7 8840HS (8 cores / 16 threads) |
| RAM | 19.8 GB |
| Disk | WD PC SN5000S 1 TB NVMe SSD (data dirs on it) |
| OS | Windows 11 (10.0.26300) |
| Python | 3.13.13 (venv launcher + miniforge3 interpreter), grpcio 1.83.0 |
| Cluster | 3 nodes on 127.0.0.1, one process each; clients in a 4th process |
| Raft | heartbeat 0.1 s, election timeout 0.5-1.0 s (defaults, E1) unless stated |
| Writes | `PlaceBid`, each client on its own auction, so every write is accepted. No LLM call. |
| Durability | `FileStorage` fsyncs term/vote/log before replying (P8), on the leader and the follower |

Everything shares one machine: the three nodes, the clients and the poller
compete for the same 16 hardware threads, and "network" is loopback. These are
not numbers for a distributed deployment.

## Writes: throughput and latency

`stress.py throughput --clients 1,2,4,8,16 --seconds 15`. Each client has one
write in flight (the workload rule). Latency is measured in the client
(`time.monotonic()`, request sent to reply received, including the gRPC and
retry layers). The first second of each step is discarded.

| clients | accepted writes/s | p50 | p95 | p99 | max | failed |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 210.5 | 4.6 ms | 6.4 ms | 7.8 ms | 13.9 ms | 0 |
| 2 | 340.7 | 5.7 ms | 8.0 ms | 9.8 ms | 14.8 ms | 0 |
| 4 | 490.7 | 7.9 ms | 11.2 ms | 13.4 ms | 20.3 ms | 0 |
| 8 | 586.5 | 13.5 ms | 18.0 ms | 21.8 ms | 32.5 ms | 0 |
| 16 | 688.1 | 22.8 ms | 27.8 ms | 31.7 ms | 39.0 ms | 0 |

How to read it:

- **One client: ~4.6 ms per write.** That's one full commit: the handler
  proposes, the leader fsyncs its log, AppendEntries reaches a follower, the
  follower fsyncs and replies, the leader advances commitIndex, the applier
  applies, and the handler wakes. Two fsyncs on the critical path.
- **Throughput rises sublinearly, then flattens.** 16 clients give 3.3× the
  throughput of one, at 5× the latency. Concurrent proposals share
  AppendEntries rounds (one RPC carries up to 64 entries, R7), which is why
  throughput grows at all. It flattens because the leader does everything
  under one node lock, including the fsync (P8), and because CPython's
  GIL serialises the Python work in each process.
- **No failures** at any client count: every write was acknowledged.
- Measured before the writer rotated auctions (T16): each client's auction
  grew to at most ~3 400 bids in a 16 s step (1 client × 210/s). At that size
  M1's O(bids) scan in `apply()` costs ~0.4 ms of the 4.6 ms, measured
  offline (0.15 ms at 1 000 bids, 0.54 ms at 5 000). Small, but not zero.
- Latency under chaos (testing-and-chaos.md §6) is far worse: p50 47 ms,
  p95 319 ms. That's 8 concurrent clients and the failovers themselves.

## Failover (like the paper's Figure 16)

`stress.py failover`. Per config: a fresh cluster and one client writing back
to back (a new auction every 1 000 bids). Then, per kill: wait until
**settled** (all three up, all name the same leader, every node within 50
entries of its commitIndex, for 1 s), kill -9 the leader at t0, and record:

- **`t_elect`**: t0 until another node reports LEADER in a higher term (status
  polled every 10 ms, without digests, T16). This is what the paper's §9.3
  measures: "the time to detect and replace a crashed leader".
- **`t_write`**: t0 until the client's first write acknowledged **by another
  node**. That's what a user sees.

The kill itself (`TerminateProcess` + reap) took 12-21 ms and is included in
both.

| election timeout | heartbeat | kills | `t_elect` min / p50 / p95 / max | `t_write` min / p50 / p95 / max | terms at the kills |
|---|---|---:|---|---|---|
| **0.5-1.0 s (default)** | 0.1 s | 20 | 867 / **892.7** / 1 114 / 1 118 ms | 2 178 / **2 209** / 2 222 / 2 408 ms | 1 → 22: one term per kill |
| 0.15-0.3 s (paper's recommendation) | 0.05 s | 17 * | 440 / **462** / 681 / 875 ms | 374 / **2 220** / 2 263 / 2 419 ms | 2 → 68 in 17 kills |
| 0.15-0.15 s (no randomness) | 0.05 s | 10 | 2.9 / 11.6 / 15.9 / 15.9 **s** (5 of 10 had no leader within 30 s) | 3.6 / 12.6 / 16.7 / 16.7 s | 1 → 1 866 |

\* 17, not 20: that config reached the script's limit of 10 "not settled
within 60 s" retries (see below). The required ≥ 20 kills are in the
default config. Raw rows: `runs/stress-20261109-232357/` and
`runs/stress-20261109-234519/` (`results.json`, `failover.csv`).

### Election time vs. what the client sees

`t_write` is ~2.2 s in **every** config, even when the election took 0.44 s.
That isn't Raft. On Windows, a TCP connect to a localhost port with no
listener is refused only after **2.0 s** (measured: WSAECONNREFUSED 10061
after 2.02-2.03 s, for a raw socket and for the first gRPC call alike). After
the kill, the client's next attempt goes to the node it last talked to,
`target = self._target` ([client/auction_client.py:139](../client/auction_client.py#L139)),
which is the dead leader. That attempt waits out the 2.0 s. The 8 s
per-attempt deadline (line 59) never binds. Then one 0.05 s backoff (line
164), the next node, and by then it leads: 2.0 + 0.05 + one commit ≈ 2.2 s.
The one exception (374 ms, 0.15-0.3 s kill 7) is a kill where the client's
remembered node was not the leader that died. Not tuned: decisions.md T18.

So: **Raft replaces a crashed leader in ~0.9 s (default) or ~0.46 s
(0.15-0.3 s). A client on this machine waits ~2.2 s either way.**

### Compared with Figure 16

The paper (§9.3): *"In the absence of randomness, leader election
consistently took longer than 10 seconds in our tests due to many split
votes. Adding just 5ms of randomness helps significantly, resulting in a
median downtime of 287ms. Using more randomness improves worst-case
behavior: with 50ms of randomness the worst-case completion time (over 1000
trials) was 513ms."* It recommends 150-300 ms.

- **No randomness (0.15-0.15 s) reproduces the paper's result:** median
  11.6 s, half the kills had no leader within 30 s, and 1 866 terms went by in
  10 kills. Three followers that time out together keep splitting the vote.
- **0.15-0.3 s:** median election 462 ms, max 875 ms, against the
  paper's 287 ms median / 513 ms worst case for nearby settings. The
  difference: our timer starts at the follower's last heartbeat, the vote
  round fsyncs on both sides, and the paper crashed the leader at a random
  point *within* its heartbeat interval, the same as our kill at an arbitrary
  moment. Plus three Python processes and a client sharing one machine.
- **0.5-1.0 s:** median 892.7 ms, roughly the expected minimum of two
  followers' random timeouts in [0.5, 1.0] s, plus the time since the last
  heartbeat, plus one vote round with an fsync on each side.

### Finding: 0.15-0.3 s churns terms on this machine, which supports 0.5-1.0 s (E1)

With 0.15-0.3 s timeouts, the term went from 2 to 68 across 17 kills, about
4 terms per kill instead of 1. In the earlier (discarded) run it went from 1
to 140 by kill 13. Most of those terms are elections **nobody asked for**:
a follower missed heartbeats for 150 ms while the leader was alive, because
Python on Windows can stall that long (GIL contention between the applier,
gRPC threads and the ticker, the 15.6 ms timer tick, fsync). Each spurious
election deposes a healthy leader. In-flight writes then get "outcome
unknown" and retry. With the default 0.5-1.0 s, the term rose by exactly one
per kill, 1 → 22 over 20 kills: **no spurious elections at all.**

That's §5.6's timing requirement, `broadcastTime ≪ electionTimeout ≪ MTBF`,
seen from the wrong side. On this platform "broadcast time" has to include
the worst stall of a Python process, not the ~1 ms loopback round trip. This
is the measured justification for E1's choice of 0.5-1.0 s. We pay about
0.4 s more per real failover and avoid a stream of fake ones.

### The "not settled" waits

14 kill attempts across the runs waited out a full 60 s for `settled()`
before trying again, and they cluster late in each run. Each restart replays
the node's whole log through `apply_fn` (no snapshots, P11), and the log
keeps growing: restarted nodes in the default config held 1 211, 3 461,
4 953, … 17 948, and finally 39 507 entries. Meanwhile the writer keeps
adding about 200 entries a second. `settled()` waits until every node is
within 50 entries. **Inferred, not measured:** I didn't instrument the
replay rate. The attribution rests on the timing (late in the run only) and
the log sizes. Log compaction (§7) is what would bound it. See
testing-and-chaos.md Known gaps.

### Bugs this measurement found (all logged)

- Status RPC throttling the node it observed: fixed (T16).
- FileStorage fail-stopping on a transient Windows `PermissionError`: fixed
  (T17), after being pinned as a strict xfail.
- Three harness bugs: waiting for identical digests under load, counting the
  old leader's last ack, a writer outliving its cluster (T14).
- Windows' 2 s connect-refused delay dominating client failover: explained,
  not tuned (T18).
