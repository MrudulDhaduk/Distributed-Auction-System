"""The dashboard's backend: owns the cluster processes, watches them, and
serves the page and a small JSON API.

    .venv\\Scripts\\python.exe -m dashboard                 # LLM server + 3 nodes + http://127.0.0.1:8765
    .venv\\Scripts\\python.exe -m dashboard --llm fake      # canned-answer LLM (no Ollama needed)
    .venv\\Scripts\\python.exe -m dashboard --llm none      # no LLM server at all

Ports are the run_cluster.ps1 ones (peer 50061-50063, client 50071-50073,
LLM 50052), so `python -m client.auction_client` works against it too. Do
not run it next to run_cluster.ps1 or the scripted demo.

Threads:
  status poller   GetNodeStatus on every node, every 0.1 s, in parallel
  state poller    DumpState on the leader, every 0.33 s
  HTTP            one thread per request (ThreadingHTTPServer)
Shared data sits behind one lock (self._lock) and is only copied out.

API (JSON in, JSON out; a logged-in browser tab sends X-Session):
  GET  /api/state                        everything the page draws
  POST /api/nodes/<id>/<kill|restart|suspend|resume>
  POST /api/register  {username, password, confirm}
  POST /api/login     {username, password}          -> {session, username}
  POST /api/logout
  POST /api/auctions  {item, duration_seconds, attributes}
  POST /api/bid       {auction_id, amount, skip_precheck?}
  POST /api/ask       {auction_id, question}       -> {answer, ms}
  POST /api/describe  {auction_id, attributes}
"""

import collections
import concurrent.futures
import datetime
import json
import logging
import mimetypes
import os
import secrets
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import grpc

from client.auction_client import ClusterClient, ClusterUnavailable
from common import time_conv
from dashboard import validate
from dashboard.scenarios import CHAOS_DURATIONS, SCENARIOS, ScenarioRun
from dashboard.validate import Invalid
from generated import status_pb2, status_pb2_grpc
from harness.procs import LocalCluster
from sessions.users import SEED_USERS

log = logging.getLogger("dashboard")

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
STATUS_INTERVAL = 0.1
STATE_INTERVAL = 0.33
STATUS_TIMEOUT = 0.3
DUMP_TIMEOUT = 1.5
MAX_EVENTS = 300
CLIENT_GIVE_UP = 15.0
ACTIONS = ("kill", "restart", "suspend", "resume")


def now_us() -> int:
    return time_conv.dt_to_micros(time_conv.now_utc())


class Session:
    """One logged-in browser tab: a username and its own ClusterClient.
    ClusterClient allows one write at a time (its section 8 serial numbers),
    so writes take `lock`. LLM questions are reads and can take seconds, so
    they go through a second client that shares the token and never blocks
    a bid."""

    def __init__(self, username, addrs):
        self.username = username
        self.client = ClusterClient(addrs, give_up_after=CLIENT_GIVE_UP)
        self.reader = ClusterClient(addrs, give_up_after=CLIENT_GIVE_UP)
        self.lock = threading.Lock()
        self.reader_lock = threading.Lock()

    def close(self):
        for c in (self.client, self.reader):
            try:
                c.close()
            except Exception:  # noqa: BLE001
                pass


class Dashboard:
    def __init__(self, run_dir, *, llm="real", base_peer=50061, base_client=50071, llm_port=50052,
                 cluster=None):
        self.run_dir = os.path.abspath(run_dir)
        self.llm_mode = llm
        self.cluster = cluster or LocalCluster(
            self.run_dir, base_peer=base_peer, base_client=base_client,
            llm_port=None if llm == "none" else llm_port, fake_llm=(llm == "fake"),
        )
        self.client_addrs = self.cluster.client_addrs()
        self._channels = {nid: grpc.insecure_channel(addr) for nid, addr in self.cluster.peer_addrs().items()}
        self._stubs = {nid: status_pb2_grpc.StatusServiceStub(ch) for nid, ch in self._channels.items()}
        self._pool = concurrent.futures.ThreadPoolExecutor(max_workers=len(self._stubs) + 2,
                                                           thread_name_prefix="dash-poll")
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._threads = []

        self.nodes = {nid: None for nid in self._stubs}      # nid -> dict | None
        self.leader = None
        self.auctions = []                                    # from the leader's DumpState
        self.users = []
        self.state_from = None                                # node the auctions came from
        self.state_at = 0
        self.events = collections.deque(maxlen=MAX_EVENTS)
        self._event_seq = 0
        self.sessions = {}                                    # sid -> Session
        self._next_auction_id = 0
        self._id_lock = threading.Lock()
        self.llm_status = "off" if llm == "none" else "starting"
        self._prev_nodes = {}
        self._ever_up = set()
        self._prev_auctions = None
        self._prev_users = None
        self._leaders = set()                                 # (term, node) LEADER sightings, for the checker
        self.scenario = None                                  # the current or last ScenarioRun
        self._scenario_no = 0
        self.prevote = False                                  # desired Raft mode on every node

    # --- lifecycle -----------------------------------------------------------------

    def start(self):
        self.event("system", "cluster starting")
        self.cluster.start()
        for target, name in ((self._status_loop, "dash-status"), (self._state_loop, "dash-state"),
                             (self._warm_llm, "dash-llm-warm")):
            t = threading.Thread(target=target, name=name, daemon=True)
            t.start()
            self._threads.append(t)

    def stop(self):
        self._stop.set()
        if self.scenario is not None:
            self.scenario.stop()
        for s in list(self.sessions.values()):
            s.close()
        try:
            self.cluster.stop()
        finally:
            for ch in self._channels.values():
                ch.close()
            self._pool.shutdown(wait=False, cancel_futures=True)

    # --- events --------------------------------------------------------------------

    def event(self, kind, text, **extra):
        with self._lock:
            self._event_seq += 1
            self.events.append({"seq": self._event_seq, "at": now_us(), "kind": kind, "text": text, **extra})
        log.info("[%s] %s", kind, text)

    # --- polling -------------------------------------------------------------------

    def _one_status(self, nid):
        try:
            m = self._stubs[nid].GetNodeStatus(status_pb2.NodeStatusRequest(include_digest=True),
                                              timeout=STATUS_TIMEOUT)
        except grpc.RpcError:
            return None
        return {
            "node_id": m.node_id, "role": m.role, "term": m.term, "leader_id": m.leader_id,
            "voted_for": m.voted_for, "commit_index": m.commit_index, "last_applied": m.last_applied,
            "last_log_index": m.last_log_index, "last_log_term": m.last_log_term,
            "state_digest": m.state_digest, "digest_index": m.digest_index,
            "heartbeats_received": m.heartbeats_received, "last_heartbeat_age_ms": m.last_heartbeat_age_ms,
            "election_timer_left_ms": m.election_timer_left_ms, "heartbeats_sent": m.heartbeats_sent,
            "peers": [{"peer_id": p.peer_id, "replies": p.replies, "last_reply_age_ms": p.last_reply_age_ms,
                       "match_index": p.match_index, "next_index": p.next_index} for p in m.peers],
            "heartbeat_interval_ms": m.heartbeat_interval_ms,
            "election_timeout_min_ms": m.election_timeout_min_ms,
            "election_timeout_max_ms": m.election_timeout_max_ms,
            "pid": m.pid,
            "prevote": m.prevote,
        }

    def _set_prevote_on(self, nid, on):
        try:
            self._stubs[nid].SetPreVote(status_pb2.SetPreVoteRequest(on=on), timeout=STATUS_TIMEOUT)
        except grpc.RpcError:
            pass  # down or frozen: tried again on the next poll

    def set_prevote(self, payload):
        payload = validate.body(payload)
        on = payload.get("on")
        if not isinstance(on, bool):
            raise Invalid("Send {\"on\": true} or {\"on\": false}")
        self.prevote = on
        for nid in self._stubs:
            self._pool.submit(self._set_prevote_on, nid, on)
        self.event("system", "Raft mode: " + ("PreVote (a returning node asks first)" if on else "classic Raft"))
        return {"ok": True, "message": "PreVote on" if on else "PreVote off (classic Raft)"}

    def _status_loop(self):
        while not self._stop.wait(STATUS_INTERVAL):
            try:
                futures = {nid: self._pool.submit(self._one_status, nid) for nid in self._stubs}
                view = {nid: f.result() for nid, f in futures.items()}
            except RuntimeError:  # pool shut down
                return
            leaders = [s for s in view.values() if s and s["role"] == "LEADER"]
            leader = max(leaders, key=lambda s: s["term"])["node_id"] if leaders else None
            self._status_events(view, leader)
            with self._lock:
                self.nodes, self.leader = view, leader
                for s in leaders:
                    self._leaders.add((s["term"], s["node_id"]))
            # A restarted node comes back with PreVote off: put it in the chosen mode.
            for nid, s in view.items():
                if s is not None and s["prevote"] != self.prevote:
                    self._pool.submit(self._set_prevote_on, nid, self.prevote)

    def _status_events(self, view, leader):
        for nid, s in view.items():
            prev = self._prev_nodes.get(nid, "unset")
            if s is None and prev not in (None, "unset"):
                state = self.cluster.nodes[nid].state
                self.event("down", f"{nid} down ({state})", node=nid)
            elif s is not None and prev in (None, "unset"):
                again = "back up" if nid in self._ever_up else "up"
                self.event("up", f"{nid} {again}, term {s['term']}", node=nid)
            if s is not None:
                self._ever_up.add(nid)
            if s is not None and prev not in (None, "unset") and s["role"] != prev["role"]:
                if s["role"] == "CANDIDATE":
                    self.event("election", f"{nid} candidate, term {s['term']}", node=nid)
                elif s["role"] == "LEADER":
                    self.event("leader", f"{nid} leader, term {s['term']}", node=nid)
                elif prev["role"] == "LEADER":
                    self.event("election", f"{nid} stepped down, term {s['term']}", node=nid)
        self._prev_nodes = view

    def _state_loop(self):
        while not self._stop.wait(STATE_INTERVAL):
            with self._lock:
                source = self.leader or next((n for n, s in self.nodes.items() if s), None)
            if source is None:
                continue
            try:
                dump = self._stubs[source].DumpState(status_pb2.DumpStateRequest(), timeout=DUMP_TIMEOUT)
                state = json.loads(dump.state_json)
            except (grpc.RpcError, ValueError):
                continue
            auctions = [self._auction_view(a) for a in state.get("auctions", [])]
            registered = [u[0] for u in state.get("users", [])]
            users = [{"username": u, "seeded": True} for u in SEED_USERS] + \
                    [{"username": u, "seeded": False} for u in registered]
            self._state_events(auctions, users)
            with self._lock:
                self.auctions, self.users = auctions, users
                self.state_from, self.state_at = source, now_us()
                # Ids from 1 000 000 up belong to the chaos workload's own numbering.
                ids = [a["auction_id"] for a in auctions if a["auction_id"] < 1_000_000]
            with self._id_lock:
                self._next_auction_id = max([self._next_auction_id] + ids)

    @staticmethod
    def _auction_view(a):
        bids = a["bids"]
        top = max(bids, key=lambda b: b["amount"]) if bids else None
        return {
            "auction_id": a["auction_id"], "item": a["item"], "owner": a["owner"],
            "description": a["description"], "close_time": a["close_time"], "closed": a["closed"],
            "closed_at": a["closed_at"], "winner": a["winner"]["bidder"] if a["winner"] else "",
            "high_bid": top["amount"] if top else 0, "high_bidder": top["bidder"] if top else "",
            "bids": bids, "escrow": a["escrow"],
            "summary": a["summaries"][0] if a["summaries"] else "",
        }

    def _state_events(self, auctions, users):
        if self._prev_auctions is None:  # first read: no history to compare
            self._prev_auctions = {a["auction_id"]: a for a in auctions}
            self._prev_users = {u["username"] for u in users}
            return
        prev = self._prev_auctions
        for a in auctions:
            aid, old = a["auction_id"], prev.get(a["auction_id"])
            if old is None:
                self.event("auction", f"{a['owner']} created #{aid} {a['item']}", auction=aid)
                old_bids = 0
            else:
                old_bids = len(old["bids"])
            new_bids = a["bids"][old_bids:]
            if len(new_bids) > 3:  # a busy scenario: one line per auction per poll, not one per bid
                self.event("bid", f"{len(new_bids)} bids on #{aid}, high {a['high_bid']:,} by {a['high_bidder']}",
                           auction=aid)
            else:
                for b in new_bids:
                    self.event("bid", f"{b['bidder']} bid {b['amount']:,} on #{aid}", auction=aid)
            if a["closed"] and (old is None or not old["closed"]):
                who = f"{a['winner']} won at {a['high_bid']:,}" if a["winner"] else "no bids"
                self.event("close", f"#{aid} closed, {who}", auction=aid)
            if a["summary"] and (old is None or not old["summary"]):
                self.event("llm", f"summary written for #{aid}", auction=aid)
            if old is not None and a["description"] and a["description"] != old["description"]:
                self.event("llm", f"description updated on #{aid}", auction=aid)
            if old is not None and a["escrow"]["status"] != old["escrow"]["status"]:
                self.event("escrow", f"escrow of #{aid}: {old['escrow']['status']} → {a['escrow']['status']}",
                           auction=aid)
        for u in users:
            if u["username"] not in self._prev_users:
                self.event("user", f"new user {u['username']}")
        self._prev_auctions = {a["auction_id"]: a for a in auctions}
        self._prev_users = {u["username"] for u in users}

    def _warm_llm(self):
        """Load the model once in the background (the first real call takes
        ~15 s), and keep llm_status honest."""
        if self.llm_mode == "none":
            return
        from server.llm_client import LLMClient

        client = LLMClient(f"127.0.0.1:{self.cluster.llm.port}", deadline=60)
        give_up = time.monotonic() + 120
        # Keeps trying after it reports "down", so starting Ollama later (or
        # restarting it) turns the pill back to ready without a restart.
        while not self._stop.is_set():
            try:
                client.get_llm_answer(request_id="dashboard-warmup", query="How do I place a bid?", context={})
                self.llm_status = "ready"
                self.event("llm", "LLM ready")
                return
            except grpc.RpcError as e:
                log.info("LLM not ready yet: %s", e.code())
                if time.monotonic() < give_up:
                    self.llm_status = "starting"
                elif self.llm_status != "down":
                    self.llm_status = "down"
                    self.event("llm", "LLM not responding; is Ollama running?")
                self._stop.wait(2 if self.llm_status == "starting" else 10)

    # --- reads for the page ----------------------------------------------------------

    def snapshot(self, session_id=None) -> dict:
        with self._lock:
            nodes = []
            for spec in self.cluster.specs:
                proc = self.cluster.nodes[spec.node_id]
                nodes.append({
                    "node_id": spec.node_id, "process": proc.state, "starts": proc.starts,
                    "client_port": spec.client_port, "peer_port": spec.peer_port,
                    "status": self.nodes.get(spec.node_id),
                })
            me = self.sessions.get(session_id) if session_id else None
            return {
                "now": now_us(), "leader": self.leader, "nodes": nodes,
                "auctions": list(self.auctions), "users": list(self.users),
                "state_from": self.state_from, "state_at": self.state_at,
                "events": list(self.events)[-120:], "llm": self.llm_status,
                "llm_mode": self.llm_mode, "me": me.username if me else None, "prevote": self.prevote,
                "run_dir": self.run_dir,
                "scenario": None if self.scenario is None else self.scenario.view(),
                "scenarios": {"catalog": SCENARIOS, "chaos_durations": CHAOS_DURATIONS},
            }

    def _auction(self, auction_id):
        with self._lock:
            return next((a for a in self.auctions if a["auction_id"] == auction_id), None)

    # --- node control ----------------------------------------------------------------

    def node_action(self, nid, action):
        if nid not in self.cluster.nodes:
            raise Invalid(f"No node {nid!r}")
        if action not in ACTIONS:
            raise Invalid(f"Unknown action {action!r}")
        node = self.cluster.nodes[nid]
        state = node.state
        with self._lock:
            was_leader = self.leader == nid
        if action == "kill":
            if state == "dead":
                raise Invalid(f"{nid} is already dead")
            if state == "suspended":
                node.resume()
            node.kill()
            self.event("kill", f"killed {nid}" + (" (leader)" if was_leader else ""), node=nid)
        elif action == "restart":
            if state != "dead":
                raise Invalid(f"{nid} is {state}; only a dead node can be restarted")
            node.start()
            self.event("restart", f"restarted {nid}", node=nid)
        elif action == "suspend":
            if state != "up":
                raise Invalid(f"{nid} is {state}; only a running node can be suspended")
            node.suspend()
            self.event("suspend", f"suspended {nid}" + (" (leader)" if was_leader else ""), node=nid)
        elif action == "resume":
            if state != "suspended":
                raise Invalid(f"{nid} is {state}, not suspended")
            node.resume()
            self.event("resume", f"resumed {nid}", node=nid)
        return {"ok": True, "message": f"{action}: {nid}", "process": node.state}

    # --- scenarios -------------------------------------------------------------------

    def leader_sightings(self) -> list:
        with self._lock:
            return [{"term": t, "node": n, "source": "dashboard-status"} for t, n in sorted(self._leaders)]

    def reserve_auction_ids(self, count) -> list:
        with self._id_lock:
            first = self._next_auction_id + 1
            self._next_auction_id += count
        return list(range(first, first + count))

    def start_scenario(self, name, payload):
        if name not in SCENARIOS:
            raise Invalid(f"No scenario {name!r}")
        payload = validate.body(payload)
        if self.scenario is not None and self.scenario.is_alive():
            raise Invalid(f"{self.scenario.title} is still running; stop it first")
        duration, faults = None, False
        if name == "chaos":
            duration = payload.get("duration", 120)
            if duration not in CHAOS_DURATIONS:
                raise Invalid(f"Duration must be one of {', '.join(map(str, CHAOS_DURATIONS))} seconds")
            faults = payload.get("faults") is True
        self._scenario_no += 1
        run = ScenarioRun(self, name, self._scenario_no, duration=duration, faults=faults)
        self.scenario = run
        run.start()
        return {"ok": True, "message": f"started {run.title}"}

    def stop_scenario(self):
        run = self.scenario
        if run is None or not run.is_alive():
            raise Invalid("Nothing is running")
        run.stop()
        return {"ok": True, "message": f"stopping {run.title}; the checker runs next"}

    # --- accounts --------------------------------------------------------------------

    def register(self, payload):
        username, password = validate.registration(payload)
        client = ClusterClient(self.client_addrs, give_up_after=CLIENT_GIVE_UP)
        try:
            r = client.register(username, password)
        finally:
            client.close()
        if not r.result.success:
            raise Invalid(r.result.reason, "username")
        return {"ok": True, "message": r.result.reason}

    def login(self, payload):
        username, password = validate.credentials(payload)
        s = Session(username, self.client_addrs)
        r = s.client.login(username, password)
        if not r.result.success:
            s.close()
            raise Invalid("Wrong username or password", "password")
        s.reader.token = s.client.token
        sid = secrets.token_urlsafe(24)
        with self._lock:
            self.sessions[sid] = s
        self.event("login", f"{username} logged in")
        return {"ok": True, "session": sid, "username": username}

    def logout(self, session_id):
        with self._lock:
            s = self.sessions.pop(session_id, None)
        if s is None:
            return {"ok": True}
        try:
            with s.lock:
                s.client.logout()
        except (ClusterUnavailable, grpc.RpcError):
            pass
        s.close()
        return {"ok": True}

    def _session(self, session_id) -> Session:
        with self._lock:
            s = self.sessions.get(session_id) if session_id else None
        if s is None:
            raise PermissionError("Log in first")
        return s

    # --- auctions --------------------------------------------------------------------

    def create_auction(self, session_id, payload):
        s = self._session(session_id)
        req = validate.new_auction(payload)
        close = time_conv.now_utc() + datetime.timedelta(seconds=req["duration_seconds"])
        for _ in range(5):
            with self._id_lock:
                self._next_auction_id += 1
                auction_id = self._next_auction_id
            with s.lock:
                r = s.client.create_auction(auction_id, req["item"], time_conv.dt_to_micros(close),
                                            attributes=req["attributes"] or None)
            if r.result.success:
                return {"ok": True, "auction_id": auction_id, "message": r.result.reason}
            if not r.result.reason.startswith("ID already exists"):
                raise Invalid(r.result.reason)
        raise Invalid("Could not find a free auction id; try again")

    def bid(self, session_id, payload):
        s = self._session(session_id)
        req = validate.bid(payload)
        a = self._auction(req["auction_id"])
        if not req["skip_precheck"] and a is not None:
            # Layer 1 of 3 (dashboard/validate.py). The page greys the form out
            # too; apply() is what actually refuses. An auction this dashboard
            # has not seen yet (its state poll runs every 0.33 s) goes straight
            # to the cluster, which knows.
            if a["owner"] == s.username:
                raise Invalid("You are the seller of this auction: a seller cannot bid on their own auction")
            if a["closed"]:
                raise Invalid("This auction is closed")
            if req["amount"] <= a["high_bid"]:
                raise Invalid(f"Your bid must be higher than the current high bid of {a['high_bid']:,}")
        with s.lock:
            r = s.client.place_bid(req["auction_id"], req["amount"])
        if not r.result.success:
            via = " (refused by the cluster's state machine)" if req["skip_precheck"] else ""
            raise Invalid(r.result.reason + via)
        return {"ok": True, "message": r.result.reason, "high_bid": r.current_high_bid,
                "high_bidder": r.current_high_bidder}

    def ask(self, session_id, payload):
        s = self._session(session_id)
        req = validate.question(payload)
        t0 = time.monotonic()
        with s.reader_lock:
            r = s.reader.ask_assistant(req["question"], auction_id=req["auction_id"])
        ms = int((time.monotonic() - t0) * 1000)
        if not r.result.success:
            raise Invalid(r.result.reason)
        self.event("llm", f"{s.username} asked about #{req['auction_id']}",
                   auction=req["auction_id"])
        return {"ok": True, "answer": r.answer, "ms": ms}

    def describe(self, session_id, payload):
        s = self._session(session_id)
        req = validate.description_request(payload)
        a = self._auction(req["auction_id"])
        if a is not None and a["owner"] != s.username:
            raise Invalid("Only the seller can change the description")
        with s.lock:
            r = s.client.generate_description(req["auction_id"], req["attributes"])
        if not r.result.success:
            raise Invalid(r.result.reason)
        return {"ok": True, "description": r.description}


# --- HTTP ------------------------------------------------------------------------------

MAX_BODY = 64 * 1024


def make_handler(dash: Dashboard):
    class Handler(BaseHTTPRequestHandler):
        server_version = "AuctionDashboard/1"

        def log_message(self, fmt, *args):  # quiet: the poll is 4 requests a second
            pass

        def _send(self, status, obj):
            data = json.dumps(obj).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _static(self, path):
            name = "index.html" if path in ("", "/") else path.lstrip("/")
            full = os.path.normpath(os.path.join(STATIC_DIR, name))
            if not full.startswith(STATIC_DIR + os.sep) or not os.path.isfile(full):
                self._send(HTTPStatus.NOT_FOUND, {"error": "not found"})
                return
            with open(full, "rb") as f:
                data = f.read()
            ctype = mimetypes.guess_type(full)[0] or "application/octet-stream"
            if ctype.startswith("text/") or ctype.endswith("javascript"):
                ctype += "; charset=utf-8"
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", ctype)
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _session_id(self):
            return self.headers.get("X-Session") or None

        def do_GET(self):
            path = self.path.split("?", 1)[0]
            if path == "/api/state":
                self._send(HTTPStatus.OK, dash.snapshot(self._session_id()))
            elif path.startswith("/api/"):
                self._send(HTTPStatus.NOT_FOUND, {"error": "no such endpoint"})
            else:
                self._static(path)

        def do_POST(self):
            path = self.path.split("?", 1)[0]
            try:
                length = int(self.headers.get("Content-Length") or 0)
                if length > MAX_BODY:
                    raise Invalid("Request too large")
                raw = self.rfile.read(length) if length else b"{}"
                try:
                    payload = json.loads(raw.decode("utf-8") or "{}")
                except (UnicodeDecodeError, ValueError):
                    raise Invalid("The request body is not valid JSON")
                sid = self._session_id()
                parts = path.strip("/").split("/")
                if len(parts) == 4 and parts[:2] == ["api", "nodes"]:
                    result = dash.node_action(parts[2], parts[3])
                elif path == "/api/register":
                    result = dash.register(payload)
                elif path == "/api/login":
                    result = dash.login(payload)
                elif path == "/api/logout":
                    result = dash.logout(sid)
                elif path == "/api/auctions":
                    result = dash.create_auction(sid, payload)
                elif path == "/api/bid":
                    result = dash.bid(sid, payload)
                elif path == "/api/ask":
                    result = dash.ask(sid, payload)
                elif len(parts) == 4 and parts[:2] == ["api", "scenarios"] and parts[3] == "start":
                    result = dash.start_scenario(parts[2], payload)
                elif path == "/api/raft/prevote":
                    result = dash.set_prevote(payload)
                elif path == "/api/scenarios/stop":
                    result = dash.stop_scenario()
                elif path == "/api/describe":
                    result = dash.describe(sid, payload)
                else:
                    self._send(HTTPStatus.NOT_FOUND, {"error": "no such endpoint"})
                    return
                self._send(HTTPStatus.OK, result)
            except Invalid as e:
                self._send(HTTPStatus.UNPROCESSABLE_ENTITY, {"error": str(e), "field": e.field})
            except PermissionError as e:
                self._send(HTTPStatus.UNAUTHORIZED, {"error": str(e)})
            except ClusterUnavailable as e:
                self._send(HTTPStatus.SERVICE_UNAVAILABLE,
                           {"error": f"No leader answered in time ({e}). Is a majority of nodes up?"})
            except grpc.RpcError as e:
                msg = e.details() or e.code().name
                status = HTTPStatus.UNAUTHORIZED if e.code() == grpc.StatusCode.UNAUTHENTICATED \
                    else HTTPStatus.BAD_GATEWAY
                self._send(status, {"error": f"cluster: {msg}"})
            except Exception as e:  # noqa: BLE001 -- the page must get an answer
                log.exception("request %s failed", path)
                self._send(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": f"dashboard error: {e}"})

    return Handler


class ExclusiveHTTPServer(ThreadingHTTPServer):
    """http.server sets SO_REUSEADDR, and on Windows that lets a second
    process bind a port another one already listens on (on the dev machine
    Docker and WSL both hold 8080); requests then reach either process. Bind
    exclusively instead, so a taken port fails at start-up."""

    allow_reuse_address = False
    daemon_threads = True

    def server_bind(self):
        import socket

        if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        super().server_bind()


def serve(dash: Dashboard, host="127.0.0.1", port=8765):
    return ExclusiveHTTPServer((host, port), make_handler(dash))
