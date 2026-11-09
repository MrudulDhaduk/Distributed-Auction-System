"""The concurrent client workload for the chaos harness, and the history
every client records for the checker.

Actors, each a thread with its OWN ClusterClient (own client_id, own seq
counter, own token), issuing its writes strictly one after another:

  Seller   logs in, creates auctions (close time 15-60 s ahead by default,
           some with attributes so the LLM is called), keeps a few open.
  Bidder   picks an auction, bids above the highest bid it has heard of.
  Racer    waits for a race call, then bids the race amount at once. The
           race coordinator sends the SAME (auction, amount) to every racer
           at the same moment: deliberately racing equal bids. Exactly one
           may win; the rest must be refused with "not added because
           current highest bid is <amount>".

THE WORKLOAD RULE (docs/decisions.md A5, A18, T7): the dedupe table keeps
one (seq, response) per client_id and refuses an older seq as stale. So
concurrency comes from MANY client instances, never from parallel writes on
one client_id. Every actor's write goes through Actor.write(), which runs in
that actor's thread only. The checker's history-well-formed property
verifies it after the fact.

Retries: ClusterClient retries a write with the SAME (client_id, seq) until
it gets a definite answer or gives up (ClusterUnavailable). Giving up is
recorded as outcome "unknown"; the actor then moves on with a NEW seq.

Bid identity. The checker identifies a bid by (auction, bidder, amount).
M1 has four users (server/auth_servicer.py), so several clients share a
username. Amounts keep them apart: actor i bids only amounts with
amount % modulus == i (i = 1..n), and residue 0 is reserved for races,
whose racers all have different usernames. So no two clients can ever
produce the same (auction, bidder, amount), and a client never repeats one
because its own amounts on an auction only go up.
"""

import random
import threading
import time

import grpc

from client.auction_client import ClusterClient, ClusterUnavailable
from common import time_conv
from replication.digest import hash_token

USERS = ("mrudul", "nisarg", "karan", "tanmay")  # server/auth_servicer.py; password "pass"
PASSWORD = "pass"


def now_us() -> int:
    return time_conv.dt_to_micros(time_conv.now_utc())


def next_amount(rng, high: int, last: int, residue: int, modulus: int) -> int:
    """The smallest amount above max(high, last) (plus a random 0-3 steps)
    with amount % modulus == residue."""
    floor = max(high, last) + 1 + rng.randint(0, 3) * modulus
    return floor + ((residue - floor) % modulus)


def race_amount(high: int, modulus: int) -> int:
    floor = high + 1
    return floor + ((-floor) % modulus)


class History:
    def __init__(self):
        self._lock = threading.Lock()
        self.records = []

    def append(self, record: dict):
        with self._lock:
            self.records.append(record)

    def snapshot(self) -> list:
        with self._lock:
            return list(self.records)


def record_write(history, name, user, client, op, call, **fields) -> dict:
    """Run call() -- ONE write on `client` -- and append what happened to
    `history`: (client_id, seq), outcome ok / rejected / unknown, reason,
    wall-clock start/end (epoch micros, for comparing with close_time) and
    monotonic latency. The seq is read from the request actually sent
    (client.sent is ClusterClient's public attempt trace)."""
    start, m0 = now_us(), time.monotonic()
    response, high_bid, high_bidder = None, 0, ""
    try:
        response = call()
        outcome = "ok" if response.result.success else "rejected"
        reason = response.result.reason
    except ClusterUnavailable as e:
        outcome, reason = "unknown", f"ClusterUnavailable: {e}"
    except grpc.RpcError as e:
        outcome, reason = "unknown", f"{e.code().name}: {e.details()}"
    m1, end = time.monotonic(), now_us()
    seq = client.sent[-1][2].seq if client.sent else 0
    client.sent.clear()  # the attempt trace; not needed, and it would grow all run
    if op == "bid" and response is not None:
        high_bid, high_bidder = response.current_high_bid, response.current_high_bidder
    record = dict(client=name, client_id=client.client_id, user=user, seq=seq, op=op,
                  outcome=outcome, reason=reason, start=start, end=end, latency=m1 - m0, mono_end=m1,
                  high_bid=high_bid, high_bidder=high_bidder, **fields)
    if op == "login" and outcome == "ok":
        record["token_hash"] = hash_token(response.token)
    history.append(record)
    return record


class Board:
    """What the actors know about auctions: shared, best-effort, never
    trusted by the checker. Only used to choose sensible next requests."""

    def __init__(self):
        self._lock = threading.Lock()
        self.close_time = {}  # auction_id -> epoch micros
        self.high = {}        # auction_id -> highest bid any reply mentioned

    def add(self, auction_id, close_time):
        with self._lock:
            self.close_time[auction_id] = close_time
            self.high.setdefault(auction_id, 0)

    def observe(self, auction_id, high):
        with self._lock:
            if high > self.high.get(auction_id, 0):
                self.high[auction_id] = high

    def high_of(self, auction_id) -> int:
        with self._lock:
            return self.high.get(auction_id, 0)

    def open_ids(self, margin_us=200_000) -> list:
        t = now_us() + margin_us
        with self._lock:
            return sorted(a for a, c in self.close_time.items() if c > t)

    def all_ids(self) -> list:
        with self._lock:
            return sorted(self.close_time)


class Actor(threading.Thread):
    def __init__(self, workload, name, user):
        super().__init__(name=name, daemon=True)
        self.w = workload
        self.user = user
        self.rng = random.Random(f"{workload.seed}:{name}")
        self.client = ClusterClient(workload.addrs, give_up_after=workload.give_up_after)

    def write(self, op, call, **fields):
        """Run ONE write and record it. Only ever called from this actor's
        own thread, so this client never has two writes in flight."""
        return record_write(self.w.history, self.name, self.user, self.client, op, call, **fields)

    def login(self):
        while not self.w.stopping.is_set():
            if self.write("login", lambda: self.client.login(self.user, PASSWORD))["outcome"] == "ok":
                return True
        return False

    def run(self):
        try:
            if self.login():
                self.body()
        except Exception as e:  # an actor dying silently would look like "quiet"
            self.w.errors.append(f"{self.name}: {type(e).__name__}: {e}")
            raise
        finally:
            self.client.close()

    def pause(self, lo, hi):
        self.w.stopping.wait(self.rng.uniform(lo, hi))


class Seller(Actor):
    def __init__(self, workload, index):
        super().__init__(workload, f"seller{index}", USERS[index % len(USERS)])
        self.index = index
        self.made = 0

    def body(self):
        w = self.w
        while not w.stopping.is_set():
            if len(w.board.open_ids()) < w.max_open:
                self.made += 1
                auction_id = (self.index + 1) * 1_000_000 + self.made
                close = now_us() + int(self.rng.uniform(*w.close_after) * 1e6)
                attrs = {"condition": self.rng.choice(["mint", "used", "worn"]), "lot": str(auction_id)} \
                    if self.rng.random() < w.llm_fraction else None
                r = self.write("create", lambda: self.client.create_auction(auction_id, f"item {auction_id}", close, attrs),
                               auction_id=auction_id, item=f"item {auction_id}", close_time=close)
                if r["outcome"] != "rejected":
                    w.board.add(auction_id, close)  # "unknown" too: it may exist
            self.pause(0.2, 1.0)


class Bidder(Actor):
    def __init__(self, workload, index):
        super().__init__(workload, f"bidder{index}", USERS[index % len(USERS)])
        self.residue = index + 1
        self.last = {}

    def body(self):
        w = self.w
        while not w.stopping.is_set():
            ids = w.board.open_ids() if self.rng.random() < 0.9 else w.board.all_ids()
            if not ids:
                self.pause(0.05, 0.2)
                continue
            a = self.rng.choice(ids)
            amount = next_amount(self.rng, w.board.high_of(a), self.last.get(a, 0), self.residue, w.modulus)
            self.last[a] = amount
            r = self.write("bid", lambda: self.client.place_bid(a, amount), auction_id=a, amount=amount)
            w.board.observe(a, r["high_bid"])
            self.pause(*w.think)


class Racer(Actor):
    def __init__(self, workload, index):
        super().__init__(workload, f"racer{index}", USERS[index])  # distinct usernames: index < 4
        self.calls = []
        self.cv = threading.Condition()
        self.last = {}

    def call(self, auction_id, amount):
        with self.cv:
            self.calls.append((time.monotonic(), auction_id, amount))
            self.cv.notify()

    def body(self):
        w = self.w
        while not w.stopping.is_set():
            with self.cv:
                if not self.calls:
                    self.cv.wait(0.2)
                calls, self.calls = self.calls, []
            for t, a, amount in calls:
                # Stale (we were stuck in a retry), or not above our own last
                # bid here: skip, so we never send the same bid twice.
                if time.monotonic() - t > 1.0 or amount <= self.last.get(a, 0):
                    continue
                self.last[a] = amount
                r = self.write("bid", lambda: self.client.place_bid(a, amount), auction_id=a, amount=amount, race=True)
                w.board.observe(a, r["high_bid"])


class Workload:
    def __init__(self, addrs, seed, *, sellers=1, bidders=4, racers=3, think=(0.0, 0.3),
                 close_after=(15.0, 60.0), max_open=4, llm_fraction=0.3, race_every=(0.5, 2.0),
                 give_up_after=30.0):
        if racers > len(USERS):
            raise ValueError(f"at most {len(USERS)} racers: each needs its own username")
        self.addrs = list(addrs)
        self.seed = seed
        self.history = History()
        self.board = Board()
        self.stopping = threading.Event()
        self.errors = []
        self.modulus = bidders + 1  # residues 1..bidders for bidders, 0 for races
        self.think, self.close_after, self.max_open = think, close_after, max_open
        self.llm_fraction, self.race_every, self.give_up_after = llm_fraction, race_every, give_up_after
        self.actors = ([Seller(self, i) for i in range(sellers)] + [Bidder(self, i) for i in range(bidders)]
                       + [Racer(self, i) for i in range(racers)])
        self.racers = [a for a in self.actors if isinstance(a, Racer)]
        self._rng = random.Random(f"{seed}:races")
        self._coordinator = threading.Thread(target=self._races, name="race-coordinator", daemon=True)

    def _races(self):
        while not self.stopping.wait(self._rng.uniform(*self.race_every)):
            ids = self.board.open_ids(margin_us=1_000_000)
            if not ids or len(self.racers) < 2:
                continue
            a = self._rng.choice(ids)
            amount = race_amount(self.board.high_of(a), self.modulus)
            for r in self.racers:
                r.call(a, amount)

    def start(self):
        for a in self.actors:
            a.start()
        self._coordinator.start()
        return self

    def stop(self, timeout=None):
        """Stop issuing new requests and wait for the ones in flight. An
        actor inside a ClusterClient retry finishes it first (at most
        give_up_after), so its last record is complete."""
        self.stopping.set()
        deadline = time.monotonic() + (timeout if timeout is not None else self.give_up_after + 15)
        self._coordinator.join(timeout=5)
        for a in self.actors:
            a.join(timeout=max(0.1, deadline - time.monotonic()))
        return [a.name for a in self.actors if a.is_alive()]
