"""A chaos run on disk: the three inputs of check_all, as files.

    <run_dir>/history.jsonl   one JSON object per client operation
    <run_dir>/dumps.json      {node_id: StateDump as a dict}
    <run_dir>/leaders.json    [{"term", "node", "source"}, ...]

Written by the chaos harness and the demo, read by `python -m checker`.
"""

import json
import os

HISTORY = "history.jsonl"
DUMPS = "dumps.json"
LEADERS = "leaders.json"


def save_run(run_dir, history, dumps, leaders):
    os.makedirs(run_dir, exist_ok=True)
    with open(os.path.join(run_dir, HISTORY), "w", encoding="utf-8") as f:
        for r in history:
            f.write(json.dumps(r, sort_keys=True) + "\n")
    with open(os.path.join(run_dir, DUMPS), "w", encoding="utf-8") as f:
        json.dump(dumps, f, indent=1, sort_keys=True)
    with open(os.path.join(run_dir, LEADERS), "w", encoding="utf-8") as f:
        json.dump(leaders, f, indent=1, sort_keys=True)


def load_run(run_dir):
    with open(os.path.join(run_dir, HISTORY), encoding="utf-8") as f:
        history = [json.loads(line) for line in f if line.strip()]
    with open(os.path.join(run_dir, DUMPS), encoding="utf-8") as f:
        dumps = json.load(f)
    with open(os.path.join(run_dir, LEADERS), encoding="utf-8") as f:
        leaders = json.load(f)
    return history, dumps, leaders
