"""Predefined runs the dashboard can start with a button, on the cluster it
already owns, while you kill / suspend / restart nodes by hand.

    clock    the classic flow: nisarg sells a vintage clock, karan, mrudul and
             tanmay bid (including two equal bids at once), the seller tries
             to bid and is refused, the winner pays, the seller releases.
    demo     the four-auction demo: a hero auction plus three background
             auctions that six background bidders (bg1-bg6) bid on until
             they close; payments, a release and a refund at the end.
    chaos    the chaos workload (harness/workload.py: sellers, bidders, equal
             bid races, payers) for a chosen time, optionally with the chaos
             harness's own random fault schedule on top of yours.

Every client write is recorded in one history (harness/workload.py
record_write), exactly as scripts/chaos.py and scripts/demo.py do. When the
run ends (or you press Stop) the runner heals the cluster (restarts dead
nodes, resumes suspended ones), waits for the replicas to agree, collects
every node's DumpState and runs the same ten checker properties. The result
is shown on the page and saved under <run dir>/scenario-<n>-<name>/.

The checker reads the whole replicated state, so anything you did by hand in
the same cluster is in it too; that is fine for every property (they are
about consistency, not about who did what).
"""

import random
import threading
import time

import grpc

from client.auction_client import ClusterClient, ClusterUnavailable
from common import time_conv
from harness import run as R
from harness.background import BG_USERS, BackgroundBidder
from harness.leader_log import leader_sightings_from_file
from harness.schedule import make_schedule
from harness.status import StatusPoller
from harness.workload import PASSWORD, Board, History, Workload, record_write

CLIENT_GIVE_UP = 20.0

SCENARIOS = {
    "clock": {"title": "Vintage clock", "about": "1 auction, 3 bidders, equal-bid race, payment", "seconds": 75},
    "demo": {"title": "Four auctions", "about": "hero auction + 3 auctions with 6 background bidders", "seconds": 120},
    "chaos": {"title": "Chaos workload", "about": "random sellers, bidders, races and payers", "seconds": None},
}
CHAOS_DURATIONS = (60, 120, 300)


def now_us() -> int:
    return time_conv.dt_to_micros(time_conv.now_utc())


class Stopped(Exception):
    """The user pressed Stop."""


class ScenarioRun(threading.Thread):
    def __init__(self, dash, name, number, *, duration=None, faults=False, seed=None):
        super().__init__(name=f"scenario-{name}", daemon=True)
        self.dash = dash
        self.key = name
        self.number = number
        self.title = SCENARIOS[name]["title"]
        self.duration = duration or SCENARIOS[name]["seconds"]
        self.faults = faults
        self.seed = seed if seed is not None else random.randrange(1, 1_000_000)
        self.addrs = dash.client_addrs
        self.history = History()
        self.stop_evt = threading.Event()
        self.lock = threading.Lock()
        self.state = "running"          # running -> checking -> done | failed
        self.started = time.time()
        self.ended = None
        self.steps = []
        self.report = None
        self.error = None
        self.run_dir = f"{dash.run_dir}/scenario-{number}-{name}"
        self._clients = []
        self._stats_cache = (0.0, None)

    # --- helpers ------------------------------------------------------------------

    def say(self, text, kind="scenario"):
        with self.lock:
            self.steps.append({"t": round(time.time() - self.started, 1), "text": text})
        self.dash.event(kind, f"[{self.title}] {text}")

    def wait(self, seconds):
        if self.stop_evt.wait(max(0.0, seconds)):
            raise Stopped()

    def wait_until(self, epoch_us):
        self.wait((epoch_us - now_us()) / 1e6)

    def client(self):
        c = ClusterClient(self.addrs, give_up_after=CLIENT_GIVE_UP)
        self._clients.append(c)
        return c

    def write(self, user, client, op, call, **fields):
        return record_write(self.history, user, user, client, op, call, **fields)

    def login(self, user):
        c = self.client()
        for _ in range(5):
            r = self.write(user, c, "login", lambda: c.login(user, PASSWORD))
            if r["outcome"] == "ok":
                return c
            self.wait(1.0)
        raise RuntimeError(f"{user} could not log in: {r['reason']}")

    def read_auction(self, client, auction_id):
        try:
            return client.get_auction(auction_id)
        except (ClusterUnavailable, grpc.RpcError):
            return None

    def stats(self):
        at, cached = self._stats_cache
        if cached is not None and time.monotonic() - at < 0.5:
            return cached
        counts = {"writes": 0, "ok": 0, "rejected": 0, "unknown": 0, "bids": 0}
        for r in self.history.snapshot():
            counts["writes"] += 1
            counts[r["outcome"]] = counts.get(r["outcome"], 0) + 1
            if r["op"] == "bid":
                counts["bids"] += 1
        self._stats_cache = (time.monotonic(), counts)
        return counts

    def view(self) -> dict:
        with self.lock:
            steps = list(self.steps[-14:])
        return {
            "number": self.number, "key": self.key, "title": self.title, "state": self.state,
            "elapsed": round((self.ended or time.time()) - self.started, 1), "duration": self.duration,
            "faults": self.faults, "seed": self.seed, "steps": steps, "stats": self.stats(),
            "error": self.error, "run_dir": self.run_dir,
            "report": None if self.report is None else [
                {"name": r.name, "passed": r.passed, "checked": r.checked, "violations": r.violations[:3]}
                for r in self.report
            ],
        }

    def stop(self):
        self.stop_evt.set()

    # --- the thread -----------------------------------------------------------------

    def run(self):
        try:
            try:
                getattr(self, f"_run_{self.key}")()
            except Stopped:
                self.say("stopped by you")
            self.state = "checking"
            self._check()
            failed = [r.name for r in self.report if not r.passed]
            self.state = "done"
            self.say("checker: all 10 properties PASS" if not failed else f"checker: FAIL {', '.join(failed)}",
                     kind="check")
        except Exception as e:  # noqa: BLE001 -- shown on the page, never silent
            self.error = f"{type(e).__name__}: {e}"
            self.state = "failed"
            self.say(f"failed: {self.error}")
        finally:
            self.ended = time.time()
            for c in self._clients:
                try:
                    c.close()
                except Exception:  # noqa: BLE001
                    pass

    def _check(self):
        cluster = self.dash.cluster
        if self.dash.cut_links:
            self.dash.heal_network()
            self.say("restored every cut link for the check")
        for nid, node in cluster.nodes.items():
            if node.state == "suspended":
                node.resume()
                self.say(f"resumed {nid} for the check")
            elif node.state == "dead":
                R.ensure_started(node)
                self.say(f"restarted {nid} for the check")
        self.say("waiting for all three replicas to agree")
        poller = StatusPoller(cluster.peer_addrs())
        try:
            if not R.quiet(poller, timeout=60):
                self.say("replicas did not settle within 60 s; checking anyway")
            dumps = R.collect_dumps(poller)
        finally:
            poller.close()
        leaders = self.dash.leader_sightings()
        for nid, path in cluster.log_paths().items():
            leaders.extend(leader_sightings_from_file(path, f"log:{nid}"))
        self.report, _ = R.check_and_save(self.run_dir, self.history.snapshot(), dumps, leaders)

    # --- 1. the vintage clock ---------------------------------------------------------

    def _run_clock(self):
        aid = self.dash.reserve_auction_ids(1)[0]
        self.say("logging in nisarg (seller), karan, mrudul, tanmay")
        users = {u: self.login(u) for u in ("nisarg", "karan", "mrudul", "tanmay")}
        close = now_us() + 60_000_000
        r = self.write("nisarg", users["nisarg"], "create", lambda: users["nisarg"].create_auction(
            aid, "vintage clock", close, {"brand": "Seiko", "era": "1970s", "condition": "running"}),
            auction_id=aid, item="vintage clock", close_time=close)
        self.say(f"nisarg created #{aid} vintage clock, closes in 60 s ({r['outcome']})")
        self.say("kill, suspend or restart nodes any time now")
        sent = {u: set() for u in users}

        def bid(user, amount):
            if amount in sent[user]:
                return None
            sent[user].add(amount)
            r = self.write(user, users[user], "bid", lambda: users[user].place_bid(aid, amount),
                           auction_id=aid, amount=amount)
            self.say(f"{user} bids {amount}: {r['outcome']}" + ("" if r["outcome"] == "ok" else f" ({r['reason']})"))
            return r

        self.wait(4)
        bid("karan", 150)
        self.wait(4)
        self.say("mrudul and tanmay bid 200 at the same moment; only one can win")
        race = [threading.Thread(target=bid, args=(u, 200)) for u in ("mrudul", "tanmay")]
        for t in race:
            t.start()
        for t in race:
            t.join()
        self.wait(4)
        self.say("nisarg (the seller) tries to bid on his own auction")
        bid("nisarg", 500)
        self.wait(3)
        try:
            a = users["karan"].ask_assistant("Am I winning?", auction_id=aid)
            self.say(f"karan asks the assistant 'Am I winning?': {a.answer if a.result.success else a.result.reason}")
        except (ClusterUnavailable, grpc.RpcError) as e:
            self.say(f"karan's question failed: {e}")
        high = 200
        for user in ("karan", "mrudul", "tanmay", "karan", "mrudul", "tanmay"):
            self.wait(5)
            if now_us() > close - 2_000_000:
                break
            r = bid(user, high + 50)
            if r is not None and r["high_bid"]:
                high = max(high, r["high_bid"])
        self.say("waiting for the close")
        self.wait_until(close + 2_500_000)
        bid("tanmay", high + 1000)  # too late: refused
        self._settle(users, {aid: "release"}, seller="nisarg")

    # --- 2. the four-auction demo -------------------------------------------------------

    def _run_demo(self):
        ids = self.dash.reserve_auction_ids(4)
        hero, bg = ids[0], ids[1:]
        self.say("logging in nisarg (seller), karan, mrudul, tanmay")
        users = {u: self.login(u) for u in ("nisarg", "karan", "mrudul", "tanmay")}
        origin = now_us()
        plan = [(hero, "vintage clock", 100, {"brand": "Seiko", "era": "1970s", "condition": "running"}),
                (bg[0], "film camera", 40, {"brand": "Pentax", "era": "1970s", "condition": "working"}),
                (bg[1], "pocket watch", 55, {"brand": "Omega", "era": "1920s", "condition": "restored"}),
                (bg[2], "vinyl record player", 70, {"brand": "Dual", "era": "1980s", "condition": "working"})]
        board = Board()
        close_of = {}
        for aid, item, secs, attrs in plan:
            close = origin + secs * 1_000_000
            close_of[aid] = close
            r = self.write("nisarg", users["nisarg"], "create",
                           lambda aid=aid, item=item, close=close, attrs=attrs:
                           users["nisarg"].create_auction(aid, item, close, attrs),
                           auction_id=aid, item=item, close_time=close)
            self.say(f"nisarg created #{aid} {item}, closes in {secs} s ({r['outcome']})")
            if aid != hero:
                board.add(aid, close)
        self.say(f"bg1-bg6 start bidding on #{bg[0]}, #{bg[1]}, #{bg[2]} until they close")
        bg_threads = [BackgroundBidder(u, self.addrs, board, bg,
                                       record=lambda n, c, op, call, **f: self.write(n, c, op, call, **f),
                                       stop=self.stop_evt, seed=self.seed) for u in BG_USERS]
        for t in bg_threads:
            t.start()
        self.say("kill, suspend or restart nodes any time now")
        sent = {u: set() for u in users}

        def bid(user, amount):
            if amount in sent[user]:
                return None
            sent[user].add(amount)
            r = self.write(user, users[user], "bid", lambda: users[user].place_bid(hero, amount),
                           auction_id=hero, amount=amount)
            self.say(f"{user} bids {amount} on #{hero}: {r['outcome']}"
                     + ("" if r["outcome"] == "ok" else f" ({r['reason']})"))
            return r

        try:
            self.wait(3)
            bid("karan", 100)
            self.wait(3)
            bid("mrudul", 150)
            self.wait(3)
            self.say("karan and tanmay bid 200 at the same moment; only one can win")
            race = [threading.Thread(target=bid, args=(u, 200)) for u in ("karan", "tanmay")]
            for t in race:
                t.start()
            for t in race:
                t.join()
            high = 200
            order = ("mrudul", "tanmay", "karan") * 4
            for user in order:
                self.wait(6)
                if now_us() > close_of[hero] - 3_000_000:
                    break
                r = bid(user, high + 50)
                if r is not None and r["high_bid"]:
                    high = max(high, r["high_bid"])
            self.say("waiting for the closes; the hero closes last")
            self.wait_until(close_of[hero] + 2_500_000)
            for t in bg_threads:  # they stop by themselves once their auctions closed and the winners paid
                t.join(timeout=30)
        finally:
            if self.stop_evt.is_set():
                for t in bg_threads:
                    t.join(timeout=CLIENT_GIVE_UP + 5)
        self._settle(users, {hero: "release", bg[0]: "release", bg[1]: "refund", bg[2]: "release"},
                     seller="nisarg", already_paid=set(bg))

    def _settle(self, users, plan, seller, already_paid=()):
        """After the closes: the winner pays (unless a background bidder
        already did), then the seller releases or refunds."""
        for aid, action in plan.items():
            a = None
            for _ in range(20):  # the closer may need a moment, or a failover
                a = self.read_auction(users[seller], aid)
                if a is not None and a.closed:
                    break
                self.wait(1)
            if a is None or not a.closed:
                self.say(f"#{aid} is not closed yet; skipping its payment")
                continue
            if not a.winner:
                self.say(f"#{aid} closed with no bids")
                continue
            self.say(f"#{aid} closed: {a.winner} won at {a.current_high_bid}")
            if aid not in already_paid:
                payer = users.get(a.winner) or self.login(a.winner)
                amount = a.current_high_bid
                r = self.write(a.winner, payer, "pay", lambda: payer.pay(aid, amount), auction_id=aid, amount=amount)
                self.say(f"{a.winner} pays {amount} into escrow: {r['outcome']}")
            else:
                self.wait(2)  # the background winner pays on its own
            call = users[seller].release_escrow if action == "release" else users[seller].refund_escrow
            r = self.write(seller, users[seller], action, lambda: call(aid), auction_id=aid)
            self.say(f"{seller} {'releases' if action == 'release' else 'refunds'} the escrow of #{aid}: "
                     f"{r['outcome']}" + ("" if r["outcome"] == "ok" else f" ({r['reason']})"))

    # --- 3. the chaos workload ------------------------------------------------------------

    def _run_chaos(self):
        w = Workload(self.addrs, self.seed, close_after=(15.0, min(60.0, max(20.0, self.duration * 0.6))),
                     give_up_after=CLIENT_GIVE_UP, id_base=self.number * 10_000_000)
        w.history = self.history
        self.say(f"seed {self.seed}: {len(w.actors)} clients start "
                 f"(sellers, bidders, 3 racers, payers) for {self.duration} s")
        schedule = make_schedule(self.seed, self.duration) if self.faults else []
        if schedule:
            self.say(f"automatic faults on: {len(schedule)} scheduled (kills and suspends of leader and followers)")
        else:
            self.say("no automatic faults: kill, suspend or restart nodes yourself")
        w.start()
        poller = StatusPoller(self.dash.cluster.peer_addrs())
        t0 = time.monotonic()
        try:
            for fault in schedule:
                self.wait(fault.at - (time.monotonic() - t0))
                done = R.inject(self.dash.cluster, poller, fault, say=lambda s: self.say(s.lower()))
                self.wait(fault.down_for)
                R.heal(self.dash.cluster, done, say=lambda s: self.say(s.lower()))
            self.wait(self.duration - (time.monotonic() - t0))
        finally:
            self.say("stopping the clients (each finishes the request it is in)")
            w.stopping.set()
            alive = w.stop(timeout=CLIENT_GIVE_UP + 10)
            if alive:
                self.say(f"still running after the wait: {', '.join(alive)}")
            poller.close()
