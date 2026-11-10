"""The demo's event log (harness/events.py): the file format the replay reads."""

import json
import time

from harness.events import EventLog, StatusThread, election_from_text, status_payload
from harness.status import Status


def status(node_id, role="FOLLOWER", term=1, leader="n1", commit=0, digest="abcdef0123", digest_index=0):
    return Status(node_id, role, term, leader, "", commit, commit, commit, term, digest, digest_index)


def read_lines(path):
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def test_event_log_writes_one_json_object_per_line_with_t_and_kind(tmp_path):
    path = tmp_path / "events.jsonl"
    log = EventLog(str(path), t0=time.monotonic() - 2.0)   # pretend the run started 2 s ago
    log.write("step", text="starting")
    log.write("kill", node="n2", term=4)
    log.close()

    lines = read_lines(path)
    assert [line["kind"] for line in lines] == ["step", "kill"]
    assert set(lines[1]) == {"t", "kind", "node", "term"}
    assert (lines[1]["node"], lines[1]["term"]) == ("n2", 4)
    assert lines[0]["t"] >= 2.0          # seconds since the run started
    assert round(lines[0]["t"], 3) == lines[0]["t"]   # 3 decimals at most


def test_an_event_can_be_stamped_with_the_time_it_happened(tmp_path):
    path = tmp_path / "events.jsonl"
    log = EventLog(str(path), t0=time.monotonic() - 10.0)    # the run started 10 s ago
    log.write("close", at=time.monotonic() - 6.0, auction_id=2)  # it happened 6 s ago, logged now
    log.close()
    (line,) = read_lines(path)
    assert 3.9 <= line["t"] <= 4.1                       # 10 - 6 seconds into the run, not "now"


def test_event_log_ignores_writes_after_close(tmp_path):
    path = tmp_path / "events.jsonl"
    log = EventLog(str(path))
    log.write("step", text="one")
    log.close()
    log.write("step", text="late")   # must not raise, must not write
    log.close()                      # closing twice is fine
    assert [line["text"] for line in read_lines(path)] == ["one"]


def test_status_payload_marks_a_down_node_with_every_field_null():
    view = {"n1": status("n1", role="LEADER", term=3, leader="n1", commit=7, digest="d1", digest_index=7),
            "n2": None}
    payload = status_payload(view, ["n1", "n2"])["nodes"]
    assert payload["n1"]["up"] is True and payload["n1"]["role"] == "LEADER"
    assert payload["n1"]["state_digest"] == "d1" and payload["n1"]["commit_index"] == 7
    assert payload["n2"]["up"] is False
    assert all(value is None for key, value in payload["n2"].items() if key != "up")


def test_election_from_text_reads_a_new_leader_and_ignores_the_rest():
    assert election_from_text("12:00:01.250 leader n2 (term 4)") == ("n2", 4)
    assert election_from_text("12:00:02.000 n1 DOWN") is None
    assert election_from_text("12:00:02.000 no leader (was n2, term 4)") is None


def test_status_thread_writes_on_change_and_not_for_an_unchanged_view(tmp_path):
    class FakeMonitor:
        def __init__(self):
            self.current = {}

        def view(self):
            return dict(self.current)

    path = tmp_path / "events.jsonl"
    log = EventLog(str(path))
    monitor = FakeMonitor()
    monitor.current = {"n1": status("n1", role="FOLLOWER", term=1, leader="")}
    # period=60: only a structural change (role, term, leader, up/down) can make a new line in this test.
    thread = StatusThread(log, monitor, ["n1"], period=60.0, poll=0.01)
    thread.start()
    try:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not read_lines_safe(path):
            time.sleep(0.02)
        assert count_kind(path, "status") == 1          # the first view is written at once

        time.sleep(0.2)                                  # unchanged view: nothing new
        assert count_kind(path, "status") == 1

        monitor.current = {"n1": status("n1", role="LEADER", term=2, leader="n1")}   # an election
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and count_kind(path, "status") < 2:
            time.sleep(0.02)
        assert count_kind(path, "status") == 2
        last = [line for line in read_lines(path) if line["kind"] == "status"][-1]
        assert last["nodes"]["n1"]["role"] == "LEADER" and last["nodes"]["n1"]["term"] == 2
    finally:
        thread.stop()
        log.close()


def read_lines_safe(path):
    try:
        return read_lines(path)
    except (OSError, ValueError):
        return []


def count_kind(path, kind):
    return sum(1 for line in read_lines_safe(path) if line["kind"] == kind)
