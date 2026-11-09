"""Leader sightings from a node's own log.

server/serve.py logs every RoleChange (raft/node.py RoleChange.describe):

    12:00:02.010 [n1] term 1 CANDIDATE -> LEADER: won election ...

Each such line is the node itself saying "I became leader of term T". The
checker's at-most-one-leader-per-term property uses these AND the status
polls: the polls can miss a leader that lasted less than one poll interval,
the logs cannot (they are written under the node lock as the change
happens), but a kill -9 can lose the last lines a process buffered.
"""

import re

_LEADER = re.compile(r"\[(?P<node>[^\]\s]+)\] term (?P<term>\d+) \w+ -> LEADER\b")


def leader_sightings_from_log(text: str, source: str) -> list:
    return [
        {"term": int(m.group("term")), "node": m.group("node"), "source": source}
        for m in _LEADER.finditer(text)
    ]


def leader_sightings_from_file(path: str, source: str) -> list:
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            return leader_sightings_from_log(f.read(), source)
    except FileNotFoundError:
        return []
