"""Replay viewer: turns one recorded demo run into ONE self-contained HTML file.

    .venv\\Scripts\\python.exe scripts\\replay.py runs\\demo-<time>

Reads <run>\\events.jsonl (written by scripts/demo.py, see harness/events.py)
and writes <run>\\replay.html. The HTML has its data, CSS and JavaScript in
it: no server, no internet, no CDN, no font files.

The STATE is built here, in Python, where tests can check it: after every
event, a snapshot of the whole state is computed (ReplayState). The page only
looks up "the snapshot for this moment" and draws it. Seeking is therefore a
lookup, and jumping to any time rebuilds the full state from the start.

Nothing here touches the cluster; this only reads a file and writes a file.
"""

import copy
import datetime
import html
import json
import os
import re
import sys

_RUN_NAME = re.compile(r"demo-(\d{8})-(\d{6})")


def load_events(path) -> list:
    events = []
    with open(path, encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            if line.strip():
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError as e:
                    raise ValueError(f"{path}:{line_no}: not JSON: {e}") from e
    return events


def replicas_state(nodes: dict) -> str:
    """The big indicator. Live nodes with the same digest at the same index:
    "identical". Same index, different digest: "diverged" (a real bug: apply()
    is deterministic). Different indexes: "catching up" (normal lag after a
    write, shown in amber, NOT as a divergence). No live node: "none"."""
    live = [n for n in nodes.values() if n.get("up")]
    if not live:
        return "none"
    if len({n["digest_index"] for n in live}) > 1:
        return "catching up"
    return "identical" if len({n["state_digest"] for n in live}) == 1 else "diverged"


class ReplayState:
    """The state of the run after the events applied so far. apply() is one
    event; view() is a copy safe to embed in the page."""

    def __init__(self):
        self.t = 0.0
        self.nodes = {}          # node id -> the latest status record
        self.kills = []
        self.elections = []
        self.restarts = []
        self.hero = None         # the first auction created: the hero auction, as a string id
        self.auctions = {}       # str(auction id) -> card data
        self.bids = []           # the hero auction's bids, in order (accepted and refused)
        self.bg = {"bids": 0, "accepted": 0, "rejected": 0, "errors": 0}
        self.faq = None
        self.trends = None
        self.checker = None
        self.step = ""
        self._handlers = {
            "status": self._status, "step": self._step, "kill": self._kill, "restart": self._restart,
            "election": self._election, "action": self._action, "auction": self._auction,
            "close": self._close, "summary": self._summary, "escrow": self._escrow, "faq": self._faq,
            "trends": self._trends, "checker": self._checker,
        }

    def apply(self, e: dict):
        self.t = e.get("t", self.t)
        handler = self._handlers.get(e.get("kind"))
        if handler:  # an unknown kind is skipped, so a newer log still replays
            handler(e)

    # one handler per kind -------------------------------------------------------------

    def _status(self, e):
        self.nodes = copy.deepcopy(e["nodes"])

    def _step(self, e):
        self.step = e["text"]

    def _kill(self, e):
        self.kills.append({"node": e["node"], "term": e["term"], "t": self.t})

    def _restart(self, e):
        self.restarts.append({"node": e["node"], "t": self.t})

    def _election(self, e):
        self.elections.append({"leader": e["leader"], "term": e["term"], "t": self.t})

    def _card(self, auction_id) -> dict:
        aid = str(auction_id)
        if aid not in self.auctions:
            self.auctions[aid] = {"auction_id": auction_id, "item": "", "close_time": 0, "description": "",
                                  "summary": "", "closed": False, "winner": "", "winning_bid": 0,
                                  "high_bid": 0, "high_bidder": "", "escrow": None}
            if self.hero is None:
                self.hero = aid
        return self.auctions[aid]

    def _auction(self, e):
        card = self._card(e["auction_id"])
        card["item"] = e.get("item") or ""
        card["close_time"] = e.get("close_time") or 0
        card["description"] = e.get("description") or ""

    def _action(self, e):
        if e.get("op") != "bid" or e.get("auction_id") is None:
            return
        card = self._card(e["auction_id"])
        if e["outcome"] in ("ok", "rejected") and e.get("high_bid") is not None:
            # Only a real answer carries the high bid. "unknown" has none (the record holds 0).
            # Background bids move the auction cards too; they are only not LISTED one by one.
            card["high_bid"], card["high_bidder"] = e["high_bid"], e.get("high_bidder") or ""
        if e.get("background"):
            # Counted, never listed one by one. "unknown" = no answer came back.
            self.bg["bids"] += 1
            key = {"ok": "accepted", "rejected": "rejected"}.get(e["outcome"], "errors")
            self.bg[key] += 1
            return
        if str(e["auction_id"]) == self.hero:
            self.bids.append({"t": self.t, "user": e["user"], "amount": e["amount"], "outcome": e["outcome"],
                              "reason": e.get("reason") or "", "high_bid": e.get("high_bid")})

    def _close(self, e):
        card = self._card(e["auction_id"])
        card["closed"] = True
        card["winner"] = e.get("winner") or ""
        card["winning_bid"] = e.get("winning_bid") or 0

    def _summary(self, e):
        self._card(e["auction_id"])["summary"] = e.get("text") or ""

    def _escrow(self, e):
        card = self._card(e["auction_id"])
        card["escrow"] = {k: e.get(k) for k in ("status", "amount", "payer", "seller", "payment_ref",
                                                 "settlement_ref")}

    def _faq(self, e):
        self.faq = {"question": e.get("question") or "", "answer": e.get("answer")}

    def _trends(self, e):
        self.trends = {"stats": e.get("stats") or {}, "summary": e.get("summary")}

    def _checker(self, e):
        self.checker = {"properties": e.get("properties") or [], "all_passed": bool(e.get("all_passed"))}

    # the view -------------------------------------------------------------------------

    def view(self) -> dict:
        live = {n: s for n, s in self.nodes.items() if s.get("up")}
        leaders = sorted((s["term"], n) for n, s in live.items() if s.get("role") == "LEADER")
        return {
            "t": self.t,
            "step": self.step,
            "nodes": copy.deepcopy(self.nodes),
            "leader": leaders[-1][1] if leaders else None,
            "replicas": replicas_state(self.nodes),
            "kills": copy.deepcopy(self.kills),
            "elections": copy.deepcopy(self.elections),
            "restarts": copy.deepcopy(self.restarts),
            "hero": self.hero,
            "auctions": copy.deepcopy(self.auctions),
            "bids": copy.deepcopy(self.bids),
            "bg": dict(self.bg),
            "faq": copy.deepcopy(self.faq),
            "trends": copy.deepcopy(self.trends),
            "checker": copy.deepcopy(self.checker),
        }


def in_time_order(events: list) -> list:
    """Events sorted by their "t". The sort is stable, so events with the same time keep
    their order in the file. The file is not always in time order: a close is stamped
    with the time it happened, which can be earlier than the moment it was logged."""
    return sorted(events, key=lambda e: e.get("t", 0.0))


def build_snapshots(events: list) -> tuple:
    """(initial, snapshots): snapshots[i] is the state after events[0..i]."""
    state = ReplayState()
    initial = state.view()
    snapshots = []
    for e in in_time_order(events):
        state.apply(e)
        snapshots.append(state.view())
    return initial, snapshots


def describe(e: dict):
    """(css class, text) for one event line of the event log, or None: status
    lines and background bids are not listed one by one."""
    kind = e.get("kind")
    if kind == "step":
        return "step", e["text"]
    if kind == "kill":
        return "kill", f"KILL -9 {e['node']} (term {e['term']})"
    if kind == "restart":
        return "restart", f"restart {e['node']}"
    if kind == "election":
        return "election", f"new leader {e['leader']} (term {e['term']})"
    if kind == "action":
        if e.get("background"):
            return None
        amount = "" if e.get("amount") is None else f" {e['amount']}"
        reason = f": {e['reason']}" if e.get("reason") else ""
        return "action", f"{e['user']} {e['op']}{amount} -> {e['outcome']}{reason}"
    if kind == "escrow":
        return "escrow", (f"escrow {e['status']}, amount {e.get('amount')}, payer {e.get('payer') or '-'}, "
                          f"seller {e.get('seller') or '-'}")
    if kind == "auction":
        return "other", f"auction {e['auction_id']} created: {e.get('item') or ''}"
    if kind == "faq":
        return "other", f"assistant asked: {e.get('question')}"
    if kind == "close":
        return "other", f"auction {e['auction_id']} closed: winner {e.get('winner') or '-'} at {e.get('winning_bid')}"
    if kind == "summary":
        return "other", f"summary written for auction {e['auction_id']}"
    if kind == "trends":
        return "other", "price trends computed"
    if kind == "checker":
        return "checker", ("checker: ALL PROPERTIES PASS" if e.get("all_passed") else "checker: FAILED")
    return None


# Step headings are long; the timeline shows a short name for each, found by the first rule
# whose words appear in the heading. The full heading is the tooltip.
# ORDER MATTERS: the first phrase found wins, so the more specific phrases come first
# ("background bidders ... bid on auctions 2-4" must not read as "create #2-4").
STEP_SHORT = (
    ("starting LLM", "start"),
    ("warm the model", "warm-up"),
    ("background bidders", "bidders on"),
    ("waiting", "close wait"),
    ("auctions 2-4", "create #2-4"),
    ("creates auction", "create #1"),
    ("log in", "log in"),
    ("bids: karan", "bids"),
    ("both bid 200", "equal bids"),
    ("asks the assistant", "assistant"),
    ("keep bidding", "failover"),
    ("more bids", "bids"),
    ("summary workers", "summaries"),
    ("shows the SAME escrow", "escrow read"),
    ("background winners paid", "bg escrow"),
    ("escrow of every auction", "escrow"),
    ("price trends", "trends"),
    ("checker", None),                 # the checker event is marked itself, as "checker"
    ("background stall", "stall"),
)


def step_short_label(text: str):
    """The short timeline label for a step heading, or None to leave it off the
    timeline. A KILL step is not marked: the kill event itself is marked at the
    same moment, as "kill n1"."""
    words = " ".join(str(text).split())
    if words.startswith("KILL -9"):
        return None
    if words.startswith("restart "):
        return "restart " + words.split()[1].rstrip(":")
    if "(the winner) pays" in words:
        return "payment"
    for phrase, short in STEP_SHORT:
        if phrase in words:
            return short
    return " ".join(words.split()[:2])[:16]


ESCROW_WORD = {"HELD": "pay", "RELEASED": "release", "REFUNDED": "refund"}


def escrow_marker(e: dict):
    word = ESCROW_WORD.get(e.get("status"))
    if word is None:
        return None
    text = f"auction {e['auction_id']}: escrow {e['status']}, amount {e.get('amount')}, payer {e.get('payer') or '-'}"
    return ("escrow", f"{word} #{e['auction_id']}", text)


def event_marker(e: dict):
    """(kind, short label, full text) for a kill, election, close or checker event."""
    kind = e["kind"]
    if kind == "kill":
        return ("kill", f"kill {e['node']}", f"KILL -9 {e['node']} (term {e['term']})")
    if kind == "election":
        return ("election", f"leader {e['leader']}", f"new leader {e['leader']} (term {e['term']})")
    if kind == "close":
        return ("close", f"close #{e['auction_id']}",
                f"auction {e['auction_id']} closed: winner {e.get('winner') or '-'} at {e.get('winning_bid')}")
    if kind == "checker":
        return ("checker", "checker",
                "checker: ALL PROPERTIES PASS" if e.get("all_passed") else "checker: FAILED")
    return None


def build_data(events: list, run_name: str, recorded: str) -> dict:
    events = in_time_order(events)
    initial, snapshots = build_snapshots(events)
    times = [e.get("t", 0.0) for e in events]
    log, markers, stops = [], [], []
    escrow_last = {}
    for i, e in enumerate(events):
        described = describe(e)
        if described:
            cls, text = described
            log.append({"i": i, "t": e.get("t", 0.0), "cls": cls, "text": text})
        marker = None
        if e.get("kind") == "step":
            short = step_short_label(e["text"])
            if short is not None:
                marker = ("step", short, e["text"])
        elif e.get("kind") == "escrow":
            aid = str(e["auction_id"])
            previous = escrow_last.get(aid)
            escrow_last[aid] = e["status"]
            if previous != e["status"]:       # a repeated read of the same record is not a new marker
                marker = escrow_marker(e)
        elif e.get("kind") in ("kill", "election", "close", "checker"):
            marker = event_marker(e)
        if marker:
            kind, short, full = marker
            markers.append({"t": e.get("t", 0.0), "kind": kind, "label": short, "text": full})
        if e.get("kind") in ("step", "kill", "election", "escrow"):
            stops.append(e.get("t", 0.0))
    duration = max(times) + 1.0 if times else 1.0
    return {"run": run_name, "recorded": recorded, "duration": round(duration, 3), "times": times,
            "initial": initial, "snapshots": snapshots, "log": log, "markers": markers,
            "stops": sorted(stops)}


def recorded_text(run_dir: str) -> str:
    m = _RUN_NAME.search(os.path.basename(os.path.normpath(run_dir)))
    if m:
        stamp = datetime.datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M%S")
    else:
        stamp = datetime.datetime.fromtimestamp(os.path.getmtime(os.path.join(run_dir, "events.jsonl")))
    return stamp.strftime("%Y-%m-%d %H:%M:%S")


def _json_for_script(obj) -> str:
    # Inside <script type="application/json"> only "</script" can end the block early.
    text = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    return text.replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026")


def render_html(data: dict) -> str:
    banner = f"REPLAY of a recorded run - {data['run']} - recorded {data['recorded']}"
    return (_TEMPLATE
            .replace("__BANNER__", html.escape(banner))
            .replace("__DATA__", _json_for_script(data)))


def build_replay(run_dir: str) -> str:
    events_path = os.path.join(run_dir, "events.jsonl")
    if not os.path.exists(events_path):
        raise FileNotFoundError(f"no events.jsonl in {run_dir} (only demo runs made after the event log have one)")
    events = load_events(events_path)
    data = build_data(events, os.path.basename(os.path.normpath(run_dir)), recorded_text(run_dir))
    out = os.path.join(run_dir, "replay.html")
    with open(out, "w", encoding="utf-8") as f:
        f.write(render_html(data))
    return out


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if len(argv) != 1:
        print("usage: python scripts\\replay.py runs\\demo-<time>", file=sys.stderr)
        return 2
    try:
        out = build_replay(argv[0])
    except (FileNotFoundError, ValueError) as e:
        print(str(e), file=sys.stderr)
        return 1
    print(out)
    return 0


_TEMPLATE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>REPLAY of a recorded demo run</title>
<style>
:root {
  --bg: #0d1117; --panel: #161b22; --panel2: #1c2330; --line: #30363d;
  --text: #e6edf3; --muted: #8b949e; --ok: #3fb950; --bad: #f85149; --warn: #d29922;
  --info: #58a6ff; --lead: #a371f7; --step: #ffffff;
}
* { box-sizing: border-box; }
html, body { margin: 0; height: 100%; background: var(--bg); color: var(--text); }
body { font: 18px/1.35 "Segoe UI", system-ui, -apple-system, Arial, sans-serif; overflow: hidden; }
#app { height: 100vh; min-width: 1280px; display: flex; flex-direction: column; gap: 12px; padding: 12px 16px; }
#banner { background: #3b2f00; border: 2px solid var(--warn); color: #ffe9a6; font-size: 24px; font-weight: 600;
  padding: 10px 16px; border-radius: 8px; text-align: center; letter-spacing: 0.2px; }
.panel { background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 12px 14px; min-height: 0; }
.panel h2 { margin: 0 0 8px; font-size: 19px; color: var(--muted); font-weight: 600; text-transform: uppercase; letter-spacing: 1px; }
.muted { color: var(--muted); }
#row1 { display: grid; grid-template-columns: 1fr 1fr 1fr 380px; gap: 12px; height: 180px; flex: none; }
.node { border: 3px solid var(--line); border-radius: 10px; padding: 10px 12px; background: var(--panel2); }
.node.leader { border-color: var(--lead); box-shadow: 0 0 0 2px rgba(163,113,247,0.35); }
.node.down { opacity: 0.4; filter: grayscale(1); }
.node .node-id { font-size: 26px; font-weight: 700; display: flex; justify-content: space-between; align-items: center; }
.node .role { font-size: 16px; color: var(--lead); font-weight: 700; }
.node.down .role { color: var(--muted); }
.node .down-label { color: var(--bad); font-size: 30px; font-weight: 800; margin-top: 18px; letter-spacing: 2px; }
.node dl { display: grid; grid-template-columns: auto 1fr; gap: 3px 12px; margin: 8px 0 0; font-size: 18px; }
.node dt { color: var(--muted); }
.node dd { margin: 0; text-align: right; font-variant-numeric: tabular-nums; }
.node .digest { font-family: Consolas, "Courier New", monospace; color: var(--info); }
#indicator { display: flex; flex-direction: column; justify-content: center; align-items: center; text-align: center;
  border-radius: 12px; border: 2px solid var(--line); background: var(--panel); padding: 12px; }
#indicator .big { font-size: 40px; font-weight: 800; line-height: 1.1; }
#indicator .small { font-size: 17px; color: var(--muted); margin-top: 10px; }
#indicator.identical { border-color: var(--ok); } #indicator.identical .big { color: var(--ok); }
#indicator.diverged { border-color: var(--bad); } #indicator.diverged .big { color: var(--bad); }
#indicator.catching { border-color: var(--warn); } #indicator.catching .big { color: var(--warn); font-size: 30px; }
#indicator.none .big { color: var(--muted); font-size: 30px; }
#row2 { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; height: 232px; flex: none; }
.auction { background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 10px 12px; display: flex; flex-direction: column; min-height: 0; overflow: hidden; }
.auction.empty { color: var(--muted); align-items: center; justify-content: center; font-size: 20px; }
.auction .title { font-size: 21px; font-weight: 700; display: flex; justify-content: space-between; gap: 8px; }
.auction .status { font-size: 15px; padding: 1px 8px; border-radius: 999px; border: 1px solid var(--line); color: var(--muted); align-self: center; white-space: nowrap; }
.auction .status.open { color: var(--ok); border-color: var(--ok); }
.auction .status.closed { color: var(--muted); }
.auction .row { display: flex; justify-content: space-between; font-size: 17px; margin-top: 3px; }
.escrow { font-weight: 700; padding: 0 6px; border-radius: 4px; }
.escrow.NONE { color: var(--muted); } .escrow.HELD { color: var(--warn); } .escrow.RELEASED { color: var(--ok); } .escrow.REFUNDED { color: var(--info); }
.auction .text { font-size: 15px; color: var(--muted); margin-top: 5px; overflow: hidden; line-height: 1.3;
  display: -webkit-box; -webkit-box-orient: vertical; }
.auction .text.desc { -webkit-line-clamp: 2; }
.auction .text.summ { -webkit-line-clamp: 3; }
.auction .text b { color: var(--text); font-weight: 600; }
#row3 { display: grid; grid-template-columns: 1fr 1.15fr; grid-template-rows: minmax(0, 1fr); gap: 12px; flex: 1; min-height: 0; }
#bids { overflow: auto; max-height: 100%; }
.bid { padding: 5px 8px; border-radius: 6px; margin-bottom: 4px; font-size: 19px; }
.bid.ok { color: var(--text); background: #132a1b; }
.bid.rejected { color: #7d8590; background: transparent; border: 1px dashed var(--line); }
.bid.unknown { color: var(--warn); background: #2b2310; }
.bid .reason { display: block; font-size: 15px; color: var(--muted); }
#right { display: flex; flex-direction: column; gap: 12px; min-height: 0; }
#right > .panel { flex: none; }
#right > #logPanel { flex: 1 1 0; }   /* the log takes what is left; the checker keeps its height */
#bg { font-size: 18px; display: flex; flex-wrap: wrap; gap: 4px 22px; align-items: baseline; margin-bottom: 8px; padding-bottom: 8px; border-bottom: 1px solid var(--line); }
#bg b { font-size: 22px; font-variant-numeric: tabular-nums; }
#bg .bad { color: var(--bad); }
#logPanel { min-height: 0; display: flex; flex-direction: column; }
#log { overflow-y: auto; flex: 1; min-height: 0; font-size: 17px; }
.logline { padding: 3px 6px; border-left: 3px solid transparent; margin-bottom: 2px; word-break: break-word; }
.logline .ts { color: var(--muted); font-variant-numeric: tabular-nums; margin-right: 8px; }
.logline.step { font-weight: 800; font-size: 19px; color: var(--step); border-left-color: var(--info); margin-top: 8px; }
.logline.kill { color: #ffd7d5; background: #3d1613; border-left-color: var(--bad); font-weight: 700; }
.logline.election { color: #ffe9a6; background: #352a0c; border-left-color: var(--warn); font-weight: 700; }
.logline.escrow { color: #b7f5c4; background: #10301a; border-left-color: var(--ok); font-weight: 600; }
.logline.restart { color: var(--info); }
.logline.checker { font-weight: 800; }
#checker { flex: none; max-height: 250px; overflow: auto; }
#checker.hidden { display: none; }
#checker h2 { margin-bottom: 6px; }
#checker .props { display: grid; grid-template-columns: 1fr 1fr; column-gap: 24px; }
#checker .prop { display: flex; justify-content: space-between; gap: 8px; font-size: 16px; padding: 1px 4px; }
#checker .PASS { color: var(--ok); font-weight: 700; } #checker .FAIL { color: var(--bad); font-weight: 800; }
#checker .verdict { font-size: 22px; font-weight: 800; text-align: center; padding: 4px; border-radius: 8px; margin-top: 6px; }
#checker .verdict.pass { color: #04210d; background: var(--ok); }
#checker .verdict.fail { color: #fff; background: var(--bad); }
#controls { flex: none; background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 10px 14px 12px; }
#controls .top { display: flex; align-items: center; gap: 12px; }
button { font: inherit; font-size: 18px; color: var(--text); background: var(--panel2); border: 1px solid var(--line); border-radius: 8px; padding: 6px 14px; cursor: pointer; }
button:hover { border-color: var(--info); }
button.active { border-color: var(--info); background: #0e2a44; color: #cfe6ff; font-weight: 700; }
#play { min-width: 120px; font-weight: 700; }
#clock { font-size: 26px; font-variant-numeric: tabular-nums; font-weight: 700; min-width: 110px; }
#clock span { color: var(--muted); font-size: 18px; font-weight: 400; }
#help { margin-left: auto; color: var(--muted); font-size: 15px; }
#track { position: relative; height: 74px; margin-top: 4px; }
#rail { position: absolute; left: 0; right: 0; top: 50px; height: 10px; background: var(--line); border-radius: 6px; }
#fill { position: absolute; left: 0; top: 50px; height: 10px; background: var(--info); border-radius: 6px; width: 0; }
#seek { position: absolute; left: 0; right: 0; top: 40px; width: 100%; height: 30px; margin: 0; -webkit-appearance: none; appearance: none; background: transparent; cursor: pointer; }
#seek::-webkit-slider-thumb { -webkit-appearance: none; width: 22px; height: 22px; border-radius: 50%; background: #ffffff; border: 3px solid var(--info); margin-top: -6px; }
#seek::-moz-range-thumb { width: 18px; height: 18px; border-radius: 50%; background: #ffffff; border: 3px solid var(--info); }
#seek::-webkit-slider-runnable-track { background: transparent; height: 10px; }
.mark { position: absolute; top: 0; height: 56px; width: 2px; cursor: pointer; transform: translateX(-1px); }
.mark::after { content: ""; position: absolute; left: -4px; top: 50px; width: 10px; height: 10px; border-radius: 50%; }
.mark.step { background: #8b949e; } .mark.step::after { background: #c9d1d9; }
.mark.kill { background: var(--bad); width: 3px; } .mark.kill::after { background: var(--bad); }
.mark.election { background: var(--warn); width: 3px; } .mark.election::after { background: var(--warn); }
.mark.close { background: var(--info); } .mark.close::after { background: var(--info); }
.mark.escrow { background: var(--ok); } .mark.escrow::after { background: var(--ok); }
.mark.checker { background: var(--ok); width: 3px; } .mark.checker::after { background: var(--ok); }
/* Two rows only: each marker's label goes on the first row where it does not run into the previous label. */
.mark .label { position: absolute; left: 6px; white-space: nowrap; font-size: 14px; color: var(--muted); }
.mark .label.r0 { top: 0; } .mark .label.r1 { top: 22px; }
.mark .label.flip { left: auto; right: 6px; }
.mark .label.hidden { display: none; }
.mark.step .label { color: #c9d1d9; }
.mark.kill .label { color: #ffb4ad; font-weight: 700; }
.mark.election .label { color: #ffe08a; font-weight: 700; }
.mark.close .label { color: #a5d6ff; font-weight: 600; }
.mark.escrow .label { color: #b7f5c4; font-weight: 600; }
.mark.checker .label { color: #b7f5c4; font-weight: 800; }
</style>
</head>
<body>
<div id="app">
  <header id="banner">__BANNER__</header>

  <section id="row1">
    <div id="nodes" style="display:contents"></div>
    <div id="indicator" class="none"><div class="big">waiting</div><div class="small">no status yet</div></div>
  </section>

  <section id="row2">
    <div id="auctions" style="display:contents"></div>
  </section>

  <section id="row3">
    <div class="panel" style="display:flex;flex-direction:column;min-height:0">
      <h2>Hero auction: bids</h2>
      <div id="bids"></div>
    </div>
    <div id="right">
      <div class="panel" id="logPanel">
        <h2>Event log</h2>
        <div id="bg"></div>
        <div id="log"></div>
      </div>
      <div class="panel hidden" id="checker"></div>
    </div>
  </section>

  <footer id="controls">
    <div class="top">
      <button id="play" type="button">Play</button>
      <div id="clock">00:00 <span id="dur"></span></div>
      <button class="speed" data-speed="0.5" type="button">0.5x</button>
      <button class="speed" data-speed="1" type="button">1x</button>
      <button class="speed" data-speed="2" type="button">2x</button>
      <button class="speed" data-speed="5" type="button">5x</button>
      <button id="prev" type="button">&larr; prev</button>
      <button id="next" type="button">next &rarr;</button>
      <div id="help">Space: play / pause &middot; Left / Right: previous / next step, kill, election or escrow change</div>
    </div>
    <div id="track">
      <div id="rail"></div>
      <div id="fill"></div>
      <div id="marks"></div>
      <input id="seek" type="range" min="0" max="1" step="0.01" value="0" aria-label="replay time">
    </div>
  </footer>
</div>

<script id="replay-data" type="application/json">__DATA__</script>
<script>
(function () {
  "use strict";
  var D = JSON.parse(document.getElementById("replay-data").textContent);
  var SLOTS = 4;
  var t = 0, playing = false, speed = 1, lastFrame = null, shownIdx = -99, logCount = -1;

  function $(id) { return document.getElementById(id); }
  function esc(s) {
    return String(s === null || s === undefined ? "" : s).replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
    });
  }
  function short(s, n) { s = String(s || ""); return s.length > n ? s.slice(0, n - 3) + "..." : s; }
  function mmss(sec) {
    sec = Math.max(0, Math.floor(sec + 0.0001));
    return String(Math.floor(sec / 60)).padStart(2, "0") + ":" + String(sec % 60).padStart(2, "0");
  }

  // The last event index whose time is <= t, or -1 before the first event.
  function indexAt(time) {
    var times = D.times, lo = 0, hi = times.length - 1, ans = -1;
    while (lo <= hi) {
      var mid = (lo + hi) >> 1;
      if (times[mid] <= time) { ans = mid; lo = mid + 1; } else { hi = mid - 1; }
    }
    return ans;
  }
  function stateAt(idx) { return idx < 0 ? D.initial : D.snapshots[idx]; }

  // ---- panels ---------------------------------------------------------------------
  function renderNodes(s) {
    var box = $("nodes");
    var ids = Object.keys(s.nodes).sort();
    if (!ids.length) { ids = ["n1", "n2", "n3"]; }
    var html = "";
    ids.forEach(function (id) {
      var n = s.nodes[id];
      if (!n) { html += '<div class="node"><div class="node-id">' + esc(id) + "</div><div class=\"muted\">waiting</div></div>"; return; }
      var cls = "node" + (s.leader === id ? " leader" : "") + (n.up ? "" : " down");
      if (!n.up) {
        html += '<div class="' + cls + '"><div class="node-id">' + esc(id) + '</div>' +
          '<div class="down-label">DOWN</div><div class="muted">no answer from this node</div></div>';
      } else {
        html += '<div class="' + cls + '"><div class="node-id">' + esc(id) + ' <span class="role">' + esc(n.role) + "</span></div>" +
          "<dl><dt>term</dt><dd>" + esc(n.term) + "</dd><dt>commit</dt><dd>" + esc(n.commit_index) +
          "</dd><dt>applied</dt><dd>" + esc(n.last_applied) + '</dd><dt>digest</dt><dd class="digest">' +
          esc(String(n.state_digest || "").slice(0, 8)) + "</dd></dl></div>";
      }
    });
    box.innerHTML = html;
    var ind = $("indicator");
    var label = {
      identical: ["replicas identical", "identical", "same digest at the same index on every live node"],
      diverged: ["DIVERGED", "diverged", "same index, different digest: a real bug"],
      "catching up": ["catching up", "catching", "live nodes at different indexes: normal lag after a write, not a divergence"],
      none: ["no live node", "none", ""]
    }[s.replicas];
    ind.className = label[1];
    ind.innerHTML = '<div class="big">' + esc(label[0]) + '</div><div class="small">' + esc(label[2]) + "</div>";
  }

  function renderAuctions(s) {
    var ids = Object.keys(s.auctions).sort(function (a, b) { return Number(a) - Number(b); });
    var html = "";
    ids.slice(0, SLOTS).forEach(function (id) {
      var a = s.auctions[id];
      var esc_status = a.escrow ? a.escrow.status : "NONE";
      html += '<div class="auction">' +
        '<div class="title"><span>#' + esc(id) + " " + esc(a.item) + '</span>' +
        '<span class="status ' + (a.closed ? "closed" : "open") + '">' + (a.closed ? "CLOSED" : "OPEN") + "</span></div>" +
        '<div class="row"><span class="muted">high bid</span><b>' + esc(a.high_bid) + " by " + esc(a.high_bidder || "-") + "</b></div>" +
        '<div class="row"><span class="muted">winner</span><span>' +
          (a.closed ? esc(a.winner || "no bids") + (a.winner ? " at " + esc(a.winning_bid) : "") : "-") + "</span></div>" +
        '<div class="row"><span class="muted">escrow</span><span class="escrow ' + esc(esc_status) + '">' + esc(esc_status) + "</span></div>" +
        '<div class="text desc" title="' + esc(a.description) + '"><b>description:</b> ' + esc(short(a.description || "-", 220)) + "</div>" +
        (a.summary ? '<div class="text summ" title="' + esc(a.summary) + '"><b>summary:</b> ' + esc(short(a.summary, 260)) + "</div>" : "") +
        "</div>";
    });
    for (var k = ids.length; k < SLOTS; k++) {
      html += '<div class="auction empty">auction slot ' + (k + 1) + " - not created yet</div>";
    }
    $("auctions").innerHTML = html;
  }

  function renderBids(s) {
    var html = "";
    if (!s.hero) { html = '<div class="muted">no auction yet</div>'; }
    s.bids.forEach(function (b) {
      var cls = b.outcome === "ok" ? "ok" : (b.outcome === "rejected" ? "rejected" : "unknown");
      var verb = b.outcome === "ok" ? "accepted" : (b.outcome === "rejected" ? "refused" : "no answer");
      html += '<div class="bid ' + cls + '">' + mmss(b.t) + " &nbsp;<b>" + esc(b.user) + "</b> bid " + esc(b.amount) +
        " &rarr; " + verb + '<span class="reason">' + esc(b.reason) + "</span></div>";
    });
    $("bids").innerHTML = html;
  }

  function renderBg(s) {
    var b = s.bg;
    $("bg").innerHTML = '<span class="muted">background bidders:</span> <span>bids <b>' + b.bids +
      '</b></span><span>accepted <b>' + b.accepted + '</b></span><span>refused <b>' + b.rejected +
      '</b></span><span>errors <b class="' + (b.errors ? "bad" : "") + '">' + b.errors + "</b></span>";
  }

  function renderLog(s, idx) {
    var box = $("log");
    var atBottom = box.scrollTop + box.clientHeight >= box.scrollHeight - 40;
    var html = "";
    D.log.forEach(function (entry) {
      if (entry.i > idx) { return; }
      html += '<div class="logline ' + entry.cls + '"><span class="ts">' + mmss(entry.t) + "</span>" + esc(entry.text) + "</div>";
    });
    box.innerHTML = html;
    if (atBottom || logCount < 0) { box.scrollTop = box.scrollHeight; }
    logCount = idx;
  }

  function renderChecker(s) {
    var box = $("checker");
    if (!s.checker) { box.className = "panel hidden"; box.innerHTML = ""; return; }
    box.className = "panel";
    var html = "<h2>Property checker (end of run)</h2><div class=\"props\">";
    s.checker.properties.forEach(function (p) {
      html += '<div class="prop"><span>' + esc(p.name) + '</span><span><span class="' + (p.passed ? "PASS" : "FAIL") + '">' +
        (p.passed ? "PASS" : "FAIL") + '</span> <span class="muted">checked ' + esc(p.checked) + "</span></span></div>";
    });
    html += "</div>";
    html += s.checker.all_passed
      ? '<div class="verdict pass">ALL PROPERTIES PASS</div>'
      : '<div class="verdict fail">CHECKER FAILED</div>';
    box.innerHTML = html;
  }

  // ---- the moment on screen ---------------------------------------------------------
  function render(force) {
    var idx = indexAt(t);
    if (idx !== shownIdx || force) {
      var s = stateAt(idx);
      renderNodes(s);
      renderAuctions(s);
      renderBids(s);
      renderBg(s);
      renderChecker(s);
      renderLog(s, idx);
      shownIdx = idx;
    }
    var pct = D.duration > 0 ? (t / D.duration) * 100 : 0;
    $("fill").style.width = pct + "%";
    $("seek").value = t;
    $("clock").firstChild.nodeValue = mmss(t) + " ";
    $("play").textContent = playing ? "Pause" : (t >= D.duration ? "Replay" : "Play");
  }

  function setTime(v) {
    t = Math.min(Math.max(0, v), D.duration);
    render(false);
  }

  function tick(now) {
    if (playing) {
      if (lastFrame !== null) { t += ((now - lastFrame) / 1000) * speed; }
      lastFrame = now;
      if (t >= D.duration) { t = D.duration; playing = false; }
      render(false);
    } else {
      lastFrame = null;
    }
    window.requestAnimationFrame(tick);
  }

  function togglePlay() {
    if (!playing && t >= D.duration) { t = 0; }
    playing = !playing;
    lastFrame = null;
    render(false);
  }

  function jump(direction) {
    var eps = 0.0005, target = null;
    if (direction > 0) {
      D.stops.some(function (x) { if (x > t + eps) { target = x; return true; } return false; });
    } else {
      for (var i = D.stops.length - 1; i >= 0; i--) { if (D.stops[i] < t - eps) { target = D.stops[i]; break; } }
    }
    if (target !== null) { setTime(target); }
  }

  // ---- the timeline: markers for every step, kill and election --------------------
  // Short labels on two rows. A label takes the first row where it does not run into
  // the label before it. Where neither row is free, the label is HIDDEN, not drawn over
  // another one: the marker stays, and its full text is the tooltip (title). A label
  // near the right edge goes to the left of its marker, and takes that space.
  // Estimate: 7.6 px per character at 14 px.
  function buildMarkers() {
    var box = $("marks");
    var width = $("track").clientWidth || 1;
    var rowEnd = [-1e9, -1e9];
    var html = "";
    D.markers.forEach(function (m) {
      var x = D.duration > 0 ? (m.t / D.duration) * width : 0;
      var labelWidth = m.label.length * 7.6 + 10;
      var flip = x + labelWidth > width;
      var start = flip ? x - labelWidth : x;
      var row = -1;
      for (var r = 0; r < rowEnd.length; r++) { if (rowEnd[r] <= start) { row = r; break; } }
      var shown = row >= 0;
      if (shown) { rowEnd[row] = flip ? x : x + labelWidth; }
      html += '<div class="mark ' + m.kind + '" data-t="' + m.t + '" style="left:' + (x / width * 100) + '%" title="' +
        esc(mmss(m.t) + "  " + (m.text || m.label)) + '"><span class="label r' + (shown ? row : 0) +
        (flip ? " flip" : "") + (shown ? "" : " hidden") + '">' + esc(m.label) + "</span></div>";
    });
    box.innerHTML = html;
    box.querySelectorAll(".mark").forEach(function (el) {
      el.addEventListener("click", function () { setTime(Number(el.getAttribute("data-t"))); });
    });
  }

  // ---- wiring -----------------------------------------------------------------------
  $("dur").textContent = " / " + mmss(D.duration);
  $("seek").max = D.duration;
  $("seek").addEventListener("input", function () { setTime(Number(this.value)); });
  $("play").addEventListener("click", togglePlay);
  $("prev").addEventListener("click", function () { jump(-1); });
  $("next").addEventListener("click", function () { jump(1); });
  document.querySelectorAll(".speed").forEach(function (b) {
    b.addEventListener("click", function () {
      speed = Number(b.getAttribute("data-speed"));
      document.querySelectorAll(".speed").forEach(function (x) { x.classList.toggle("active", x === b); });
    });
  });
  document.querySelector('.speed[data-speed="1"]').classList.add("active");
  document.addEventListener("keydown", function (ev) {
    if (ev.code === "Space") { ev.preventDefault(); togglePlay(); }
    else if (ev.key === "ArrowRight") { ev.preventDefault(); jump(1); }
    else if (ev.key === "ArrowLeft") { ev.preventDefault(); jump(-1); }
  });

  buildMarkers();
  var start = 0;
  var m = /t=([0-9.]+)/.exec(window.location.hash || "");
  if (m) { start = Number(m[1]); }
  if (/end/.test(window.location.hash || "")) { start = D.duration; }
  t = Math.min(Math.max(0, start), D.duration);
  render(true);
  window.requestAnimationFrame(tick);
})();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    sys.exit(main())
