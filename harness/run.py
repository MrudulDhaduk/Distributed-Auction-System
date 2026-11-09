"""Steps shared by scripts/chaos.py, scripts/stress.py and scripts/demo.py:
wait for a leader, inject and heal a fault, let the cluster go quiet,
collect the final dumps and leader sightings, and run the checker.
"""

import json
import os
import platform
import time

from checker.properties import check_all, format_report
from checker.run_files import save_run
from harness.leader_log import leader_sightings_from_file
from harness.schedule import resolve_pick
from harness.status import agreement, current_leader


def wait_for(predicate, timeout, interval=0.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    return predicate()


def wait_for_leader(poller, timeout=30.0):
    return wait_for(lambda: current_leader(poller.poll()), timeout)


def ensure_started(node, attempts=5):
    """Start a dead node and check it stays up. A restart can lose a race
    for its port (the old socket not released yet): serve.py then exits at
    once, so try again a few times."""
    for _ in range(attempts):
        if node.state != "dead":
            return True
        node.start()
        time.sleep(1.0)
        if node.alive:
            return True
    return node.alive


def inject(cluster, poller, fault, say=print) -> list:
    """Apply one schedule entry. Returns [(node, "killed"|"suspended")]."""
    view = poller.poll()
    leader = current_leader(view) or wait_for_leader(poller, 3.0)
    up = sorted(n for n, p in cluster.nodes.items() if p.state == "up")
    followers = [n for n in up if n != leader]
    if fault.action in ("kill_leader", "suspend_leader", "kill_two"):
        first = leader if leader in up else resolve_pick(fault.pick, up)
    else:
        first = resolve_pick(fault.pick, followers or up)
    targets = [first] if first else []
    if fault.action == "kill_two" and first:
        second = resolve_pick(fault.pick, [n for n in up if n != first])
        if second:
            targets.append(second)
    done = []
    for n in targets:
        if fault.action.startswith("suspend"):
            cluster.nodes[n].suspend()
            done.append((n, "suspended"))
        else:
            cluster.nodes[n].kill()
            done.append((n, "killed"))
    say(f"FAULT {fault.action}: " + (", ".join(f"{n} {what}" for n, what in done) or "no target")
        + f" (leader was {leader})")
    return done


def heal(cluster, done, say=print):
    for n, what in done:
        node = cluster.nodes[n]
        if what == "suspended" and node.state == "suspended":
            node.resume()
            say(f"HEAL  {n} resumed")
        elif node.state == "dead":
            ok = ensure_started(node)
            say(f"HEAL  {n} restarted" + ("" if ok else " -- FAILED TO START"))


def quiet(poller, timeout=60.0, stable_for=1.0):
    """True once every node is up, one leader, every node applied all it
    committed, and all agree -- continuously for `stable_for` seconds."""
    def calm():
        view = poller.poll()
        return (all(s is not None for s in view.values())
                and current_leader(view) is not None
                and agreement(view) == "agree"
                and len({s.commit_index for s in view.values()}) == 1
                and all(s.commit_index == s.last_applied for s in view.values()))

    deadline = time.monotonic() + timeout
    since = None
    while time.monotonic() < deadline:
        if calm():
            since = since or time.monotonic()
            if time.monotonic() - since >= stable_for:
                return True
        else:
            since = None
        time.sleep(0.1)
    return False


def settled(poller, max_lag=50, timeout=60.0, stable_for=1.0):
    """True once every node is up, all name the same leader (which reports
    LEADER), and every node's lastApplied is within max_lag entries of the
    leader's commitIndex -- continuously for `stable_for` seconds.

    For "ready to kill the leader" while a writer keeps committing: quiet()
    asks for identical digests, which a busy cluster almost never shows at
    the instant of a poll."""
    def ok():
        view = poller.poll()
        if any(s is None for s in view.values()):
            return False
        leader = current_leader(view)
        if leader is None or any(s.leader_id != leader for s in view.values()):
            return False
        commit = view[leader].commit_index
        return all(s.last_applied >= commit - max_lag for s in view.values())

    deadline = time.monotonic() + timeout
    since = None
    while time.monotonic() < deadline:
        if ok():
            since = since or time.monotonic()
            if time.monotonic() - since >= stable_for:
                return True
        else:
            since = None
        time.sleep(0.05)
    return False


def collect_dumps(poller, attempts=20) -> dict:
    """Every node's DumpState, retried until all are at one digest_index
    (a close can still land between two calls)."""
    dumps = {}
    for _ in range(attempts):
        dumps = {}
        for n in poller.addrs:
            try:
                d = poller.dump(n)
                dumps[n] = {"node_id": d.node_id, "digest_index": d.digest_index,
                            "state_digest": d.state_digest, "state_json": d.state_json}
            except Exception as e:  # recorded, so the checker FAILS on it instead of skipping the node
                dumps[n] = {"node_id": n, "error": f"{type(e).__name__}: {e}"}
        ok = [d for d in dumps.values() if "error" not in d]
        if len(ok) == len(poller.addrs) and len({d["digest_index"] for d in ok}) == 1:
            return dumps
        time.sleep(0.5)
    return dumps


def leader_records(cluster, monitor) -> list:
    out = list(monitor.leader_records())
    for n, path in cluster.log_paths().items():
        out.extend(leader_sightings_from_file(path, f"log:{n}"))
    return out


def check_and_save(run_dir, history, dumps, leaders):
    save_run(run_dir, history, dumps, leaders)
    report = check_all(history, dumps, leaders)
    text = format_report(report)
    with open(os.path.join(run_dir, "report.txt"), "w", encoding="utf-8") as f:
        f.write(text + "\n")
    return report, text


def machine_info() -> dict:
    info = {
        "platform": platform.platform(),
        "processor": platform.processor(),
        "cpus": os.cpu_count(),
        "python": platform.python_version(),
    }
    if os.name == "nt":
        import ctypes

        class MEMORYSTATUSEX(ctypes.Structure):
            _fields_ = [("dwLength", ctypes.c_ulong), ("dwMemoryLoad", ctypes.c_ulong),
                        ("ullTotalPhys", ctypes.c_ulonglong), ("ullAvailPhys", ctypes.c_ulonglong),
                        ("ullTotalPageFile", ctypes.c_ulonglong), ("ullAvailPageFile", ctypes.c_ulonglong),
                        ("ullTotalVirtual", ctypes.c_ulonglong), ("ullAvailVirtual", ctypes.c_ulonglong),
                        ("ullAvailExtendedVirtual", ctypes.c_ulonglong)]
        m = MEMORYSTATUSEX()
        m.dwLength = ctypes.sizeof(m)
        if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(m)):
            info["ram_gb"] = round(m.ullTotalPhys / 2**30, 1)
    return info


def write_json(path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=1, sort_keys=True, default=str)
