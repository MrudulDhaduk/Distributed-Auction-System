"""Polling StatusService on every node, and what the dashboard and the
chaos harness make of one poll.

    poller = StatusPoller({"n1": "127.0.0.1:50061", ...})
    view = poller.poll()            # {node_id: Status, or None if unreachable}
    agreement(view)                 # "agree" / "catching up" / "DIVERGED" / "no data"
    events(prev_view, view)         # ["n1 DOWN", "leader n2 (term 4)", ...]

A view is each node's OWN claim, collected at slightly different moments
(the three calls run in parallel). It can show two LEADERs -- an old one
that has not heard of the new term yet, e.g. one that was suspended. That
is legal in Raft as long as their terms differ; current_leader() takes the
highest term, and the checker's at-most-one-leader-per-term property looks
at every sighting.

An unreachable node (killed, or suspended so the call times out) is None.
"""

import concurrent.futures
import threading
import time
from dataclasses import dataclass
from typing import Optional

import grpc

from generated import status_pb2, status_pb2_grpc

POLL_TIMEOUT = 0.3


@dataclass(frozen=True)
class Status:
    node_id: str
    role: str
    term: int
    leader_id: str
    voted_for: str
    commit_index: int
    last_applied: int
    last_log_index: int
    last_log_term: int
    state_digest: str
    digest_index: int

    @classmethod
    def from_proto(cls, m) -> "Status":
        return cls(m.node_id, m.role, m.term, m.leader_id, m.voted_for, m.commit_index,
                   m.last_applied, m.last_log_index, m.last_log_term, m.state_digest, m.digest_index)


def agreement(view: dict) -> str:
    """Do the reachable replicas hold the same state? Same digest_index and
    same digest -> "agree". Different indexes -> "catching up" (normal for a
    moment after every write). Same index, different digest -> "DIVERGED":
    apply() is deterministic, so the same commands must give the same
    state; this would be a real bug."""
    up = [s for s in view.values() if s is not None]
    if not up:
        return "no data"
    if len({s.digest_index for s in up}) > 1:
        return "catching up"
    if len({s.state_digest for s in up}) > 1:
        return "DIVERGED"
    return "agree"


def leader_sightings(view: dict) -> list:
    return sorted((s.term, s.node_id) for s in view.values() if s is not None and s.role == "LEADER")


def current_leader(view: dict) -> Optional[str]:
    seen = leader_sightings(view)
    return seen[-1][1] if seen else None


def events(prev: dict, cur: dict) -> list:
    out = []
    for node in sorted(set(prev) | set(cur)):
        was, now = prev.get(node), cur.get(node)
        if was is not None and now is None:
            out.append(f"{node} DOWN")
        elif was is None and now is not None:
            out.append(f"{node} UP")
    before, after = current_leader(prev), current_leader(cur)
    if before != after:
        if after is None:
            out.append(f"no leader (was {before}, term {prev[before].term})")
        else:
            out.append(f"leader {after} (term {cur[after].term})")
    return out


class StatusPoller:
    def __init__(self, addrs: dict, timeout: float = POLL_TIMEOUT, include_digest: bool = True):
        """include_digest=False: roles, terms and indexes only, no state
        digest (cheap for the node; see proto/status.proto)."""
        self.addrs = dict(addrs)
        self._timeout = timeout
        self.include_digest = include_digest
        self._channels = {n: grpc.insecure_channel(a) for n, a in self.addrs.items()}
        self._stubs = {n: status_pb2_grpc.StatusServiceStub(c) for n, c in self._channels.items()}
        self._pool = concurrent.futures.ThreadPoolExecutor(max_workers=max(1, len(addrs)), thread_name_prefix="status")

    def _one(self, node):
        try:
            return Status.from_proto(self._stubs[node].GetNodeStatus(
                status_pb2.NodeStatusRequest(include_digest=self.include_digest), timeout=self._timeout))
        except grpc.RpcError:
            return None

    def poll(self) -> dict:
        futures = {n: self._pool.submit(self._one, n) for n in self.addrs}
        return {n: f.result() for n, f in futures.items()}

    def dump(self, node, timeout=5.0):
        return self._stubs[node].DumpState(status_pb2.DumpStateRequest(), timeout=timeout)

    def close(self):
        self._pool.shutdown(wait=False, cancel_futures=True)
        for c in self._channels.values():
            c.close()


class StatusMonitor:
    """A background thread polling every `interval` seconds. Keeps the
    latest view, every leader sighting {(term, node)}, and an event log of
    (wall clock "HH:MM:SS.mmm", text). on_event(text) is called for each
    event, from the monitor thread."""

    def __init__(self, poller: StatusPoller, interval: float = 0.05, on_event=None):
        self.poller = poller
        self.interval = interval
        self.on_event = on_event
        self.latest: dict = {}
        self.sightings: set = set()
        self.event_log: list = []
        self.polls = 0
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="status-monitor", daemon=True)

    def start(self):
        self._thread.start()
        return self

    def stop(self):
        self._stop.set()
        self._thread.join(timeout=5)

    def view(self) -> dict:
        with self._lock:
            return dict(self.latest)

    def _run(self):
        prev = {}
        while not self._stop.is_set():
            t0 = time.monotonic()
            view = self.poller.poll()
            stamp = time.strftime("%H:%M:%S") + f".{int((time.time() % 1) * 1000):03d}"
            new_events = events(prev, view)
            with self._lock:
                self.latest = view
                self.polls += 1
                self.sightings.update(leader_sightings(view))
                self.event_log.extend((stamp, e) for e in new_events)
            for e in new_events:
                if self.on_event:
                    self.on_event(f"{stamp} {e}")
            prev = view
            self._stop.wait(max(0.0, self.interval - (time.monotonic() - t0)))

    def leader_records(self) -> list:
        with self._lock:
            return [{"term": t, "node": n, "source": "status"} for t, n in sorted(self.sightings)]
