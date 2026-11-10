"""The event log of one demo run: runs/<run>/events.jsonl, one JSON object per line.

    log = EventLog("runs/demo-.../events.jsonl", t0=t_start)
    log.write("step", text="...")          # {"t": 3.214, "kind": "step", "text": "..."}
    log.close()

Every line has "t" (seconds since the run started, 3 decimals) and "kind".
Writing is cheap on purpose: the file is opened once, each write takes one
lock and appends one line, and a background thread flushes the buffer once a
second. close() flushes what is left. So a crash loses at most the last second.

StatusThread writes the "status" kind: a snapshot of every node, taken from
the StatusMonitor's latest view (harness/status.py). It writes right away
when a node goes up or down, or when its role, term or leader changes, and
otherwise at most every 0.25 s. The digest is only in the periodic snapshots.
"""

import json
import os
import re
import threading
import time

# The Status fields a node card needs. These are the names the Status
# dataclass in harness/status.py already uses; a node that is DOWN has no
# Status, so it is written with every field null and "up": false.
STATUS_FIELDS = ("role", "term", "leader_id", "commit_index", "last_applied",
                 "last_log_index", "last_log_term", "state_digest", "digest_index")

_ELECTION = re.compile(r"leader (\S+) \(term (\d+)\)")


class EventLog:
    def __init__(self, path, t0=None, flush_every=1.0):
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self.path = path
        self._f = open(path, "w", encoding="utf-8")
        self._lock = threading.Lock()
        self._t0 = time.monotonic() if t0 is None else t0
        self._closed = False
        self._stop = threading.Event()
        self._flush_every = flush_every
        self._flusher = threading.Thread(target=self._flush_loop, name="event-flush", daemon=True)
        self._flusher.start()

    def now(self) -> float:
        return round(time.monotonic() - self._t0, 3)

    def write(self, kind: str, **fields):
        with self._lock:
            if self._closed:
                return
            line = json.dumps({"t": self.now(), "kind": kind, **fields}, default=str)
            self._f.write(line + "\n")

    def flush(self):
        with self._lock:
            if not self._closed:
                self._f.flush()

    def _flush_loop(self):
        while not self._stop.wait(self._flush_every):
            self.flush()

    def close(self):
        self._stop.set()
        self._flusher.join(timeout=5)
        with self._lock:
            if not self._closed:
                self._f.flush()
                self._f.close()
                self._closed = True


def status_payload(view: dict, node_ids) -> dict:
    """{"nodes": {id: {"up", <STATUS_FIELDS>}}} from one StatusMonitor view."""
    nodes = {}
    for n in node_ids:
        s = view.get(n)
        if s is None:
            nodes[n] = {"up": False, **{f: None for f in STATUS_FIELDS}}
        else:
            nodes[n] = {"up": True, **{f: getattr(s, f) for f in STATUS_FIELDS}}
    return {"nodes": nodes}


def _shape(view: dict) -> tuple:
    """What counts as a structural change: up/down, role, term, leader."""
    out = []
    for n in sorted(view):
        s = view[n]
        out.append((n, None) if s is None else (n, s.role, s.term, s.leader_id))
    return tuple(out)


class StatusThread(threading.Thread):
    def __init__(self, log: EventLog, monitor, node_ids, period: float = 0.25, poll: float = 0.05):
        super().__init__(name="event-status", daemon=True)
        self.log, self.monitor, self.node_ids = log, monitor, list(node_ids)
        self.period, self.poll = period, poll
        self._stop_evt = threading.Event()

    def run(self):
        last_shape, last_at = None, 0.0
        while not self._stop_evt.is_set():
            view = self.monitor.view()
            if view:
                shape = _shape(view)
                now = time.monotonic()
                if shape != last_shape or now - last_at >= self.period:
                    self.log.write("status", **status_payload(view, self.node_ids))
                    last_shape, last_at = shape, now
            self._stop_evt.wait(self.poll)

    def stop(self):
        self._stop_evt.set()
        if self.is_alive():  # a thread that never started (startup failed) cannot be joined
            self.join(timeout=5)


def election_from_text(text: str):
    """StatusMonitor's event text "HH:MM:SS.mmm leader n2 (term 4)" -> ("n2", 4).
    Anything else (DOWN, UP, "no leader ...") -> None."""
    m = _ELECTION.search(text)
    return (m.group(1), int(m.group(2))) if m else None
