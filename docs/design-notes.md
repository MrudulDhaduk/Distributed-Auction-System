# Design notes

Decisions that aren't obvious from reading the code, kept here instead of
scattered across commit messages.

## Closing auctions: poll loop vs. per-auction timer vs. next-deadline scheduler

Nothing was firing `CloseAuction` -- auctions never closed on their own,
only if a client happened to call it. Three designs were on the table.

**Per-auction `threading.Timer`.** One timer object per open auction,
scheduled at creation to fire at `close_time`. Rejected for two reasons:

- One OS thread per open auction. Fine at ten auctions, not fine at
  ten thousand.
- All timers live only in process memory. Restart the node -- or in M2,
  fail over to a different node -- and every scheduled timer is gone with
  no record of what was supposed to fire when. Recovering would mean
  re-deriving the set of pending timers from state at startup anyway,
  which is most of the work a poll loop already does on every tick.

**Next-deadline scheduler.** Keep a min-heap of `(close_time, auction_id)`,
sleep until the earliest one, fire it, recompute. Fewer wakeups than
polling, and it was the closer alternative of the two. Rejected because it
adds three separate places that must recompute or wake the scheduler early
(a new auction created with an earlier deadline than anything currently
scheduled; an auction closed manually before its deadline; a node coming
up and needing to rebuild the heap from state) plus a real race between
"the sleeping thread is about to wake for auction A" and "a new auction B
with an earlier deadline just got created." All of that machinery buys
back wakeups that were never expensive to begin with:

- A fixed 250 ms poll is one `dict` scan plus a handful of comparisons.
  Measured cost is on the order of 0.05% of one core, even with a few
  thousand auctions in the table. There is no load this system will see
  in a course project where that matters.

**Fixed-interval poll loop (chosen).** One daemon thread, wakes every
`interval_seconds` (default 250 ms), calls `close_expired(state, now)`,
sleeps again. `close_expired` is a plain function: read `state.auctions`,
find everything past its `close_time` and not yet closed, call
`state.apply(CloseAuction(...))` for each. It has no dependency on the
thread -- `tests/test_closer.py` calls it directly with a fixed `now` and
never starts a thread or sleeps.

Why this wins over the other two for this project specifically:

- **Restartable.** There is no scheduled-timer state to lose. After a
  restart (or, once Raft exists, a failover), the next tick just scans
  whatever is in `state.auctions` and finds anything overdue. No recovery
  path to write or test.
- **Inspectable.** One thread, one loop, one log line per tick if you want
  it. Nothing to reconstruct in your head about which timer is scheduled
  for when.
- **Testable without the clock or a thread.** `close_expired(state, now)`
  takes `now` as a plain argument. Tests pass fixed `datetime` values, the
  same pattern `tests/test_close_auction.py` already uses for `PlaceBid`
  and `CloseAuction` -- no `time.sleep`, no flakiness tied to wall-clock
  timing.

The scan inside `close_expired` is deliberately lock-free and advisory.
`state.lock` belongs to `AuctionState` and is acquired only by `apply()` --
one door, one guard; a second file reaching for that lock is how deadlocks
get introduced later. The scan can therefore go stale between picking an
auction and applying the close, and that is fine: `apply()` re-checks both
conditions (not already closed, past `close_time`) under its own lock, so a
stale entry comes back as a rejected `Result` rather than a wrong write.
Correctness lives in `apply()`'s re-check, not in the scan. Same argument
as the advisory high-bid read in `AuctionServicer.PlaceBid`.

### `PlaceBid` already guards the deadline independently

`AuctionState.apply()`'s `PlaceBid` branch rejects any bid where
`command.curr_time > auction["close_time"]`, regardless of whether
`CloseAuction` has run yet (`auction/state.py`). That check does not
depend on the `closed` flag at all.

This means the poll interval is a liveness knob, not a safety one. If the
loop is slow to fire -- gets descheduled, the interval is set too long,
whatever -- the *worst* case is an auction sitting open a little longer
than it should with no bids able to land in that window, because the
per-bid deadline check already rejects anything past `close_time`. A
slightly-late `CloseAuction` can never let a late bid through. The two
checks are redundant on purpose: one guards correctness (`PlaceBid`'s
own deadline check), the other just makes the auction visibly closed
(`CloseAuction`, fired by the poll loop).

### Clock-skew note for M2

In M2 (Raft cluster), whichever node's poll thread happens to fire first
for a given auction is the node that stamps `curr_time` on the
`CloseAuction` command it proposes. That command then replicates and
every node applies the same `curr_time` -- so the cluster ends up in
agreement about *when* the auction closed. But the value they agree on
came from one node's wall clock, not a vote or an average across nodes.

This is not a correctness problem -- `apply()` is still deterministic,
every node computes the same result from the same command, and
`PlaceBid`'s guard means a few hundred milliseconds of clock skew can't
let a late bid through. It's just worth knowing that "the cluster's
recorded close time" and "true wall-clock time" can differ by however
skewed that one node's clock is.
