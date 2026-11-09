"""The seeded fault schedule for the chaos harness.

    faults = make_schedule(seed, duration)
    print(describe(faults))

A schedule is a list of non-overlapping EPISODES. Each one: at `at` seconds
into the run, inject `action`; `down_for` seconds later, heal it (restart
what was killed, resume what was suspended); then a quiet gap before the
next one. One fault at a time keeps the cluster live most of the run
(2 of 3 up), except "kill_two", which deliberately takes the majority away
so the clients see real "outcome unknown" answers.

What the seed fixes, and what it does not (docs/testing-and-chaos.md):
  * FIXED by the seed: the times, the actions, the durations, and `pick`,
    a number in [0, 1) that chooses WHICH follower (resolve_pick).
  * NOT fixed: which node is leader when a fault fires. "kill_leader" kills
    whoever leads at that moment, and that depends on election timing,
    which depends on thread scheduling. So the same seed gives the same
    SCHEDULE, never the same interleaving. Thread timing is not
    replayable.

Pure: no clock, no I/O. The rng is a private random.Random(seed).
"""

import random
from dataclasses import dataclass

ACTIONS = ("kill_leader", "kill_follower", "suspend_leader", "suspend_follower", "kill_two")
WEIGHTS = (0.35, 0.20, 0.15, 0.15, 0.15)

WARMUP = 5.0          # seconds before the first fault: let a leader emerge, clients log in
GAP = (3.0, 8.0)      # quiet time between the end of one episode and the next fault
DOWN = (2.0, 8.0)     # how long a fault lasts before it is healed


@dataclass(frozen=True)
class Fault:
    at: float         # seconds from the start of the run
    action: str       # one of ACTIONS
    down_for: float   # seconds until the heal
    pick: float       # in [0, 1): which candidate, see resolve_pick


def make_schedule(seed: int, duration: float) -> list:
    rng = random.Random(seed)
    faults = []
    t = WARMUP
    while True:
        down = rng.uniform(*DOWN)
        if t + down > duration:
            return faults
        action = rng.choices(ACTIONS, weights=WEIGHTS)[0]
        faults.append(Fault(at=t, action=action, down_for=down, pick=rng.random()))
        t += down + rng.uniform(*GAP)


def resolve_pick(pick: float, candidates):
    """The candidate `pick` points at, in sorted order, so the same pick and
    the same candidates always give the same node."""
    c = sorted(candidates)
    if not c:
        return None
    return c[min(int(pick * len(c)), len(c) - 1)]


def describe(faults) -> str:
    return "\n".join(
        f"t={f.at:7.2f}s  {f.action:<16} down {f.down_for:5.2f}s  pick {f.pick:.3f}" for f in faults
    )
