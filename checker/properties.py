"""The properties a finished chaos run must satisfy, and the evidence when
one does not (docs/testing-and-chaos.md, part 4).

    report = check_all(history, dumps, leaders)
    print(format_report(report))

Inputs (plain data; the chaos harness writes them as JSON):

  history  list of dicts, one per client operation:
             client, client_id, user, seq, op ("login" | "create" | "bid"),
             outcome ("ok" | "rejected" | "unknown"), reason,
             start, end            wall clock, epoch micros, taken by the client
             auction_id, amount    (bid, create)
             high_bid, high_bidder (bid: what the server replied)
             item, close_time      (create)
             token_hash            (login ok: digest.hash_token(token))
  dumps    {node_id: {"node_id", "digest_index", "state_digest", "state_json"}}
           -- StatusService.DumpState from every node, taken after the
           cluster went quiet. A node that could not be dumped is
           {"node_id", "error"}: IDENTICAL fails on it, it is never skipped.
  leaders  list of {"term", "node", "source"}: every sighting of a LEADER
           (status polls, and "-> LEADER" lines in the node logs).

How a bid is identified: (auction_id, bidder, amount). That is unique
because (a) amounts on one auction strictly increase, so two ACCEPTED bids
never share an amount, and (b) the workload never lets one client send the
same amount twice to the same auction -- which WELL_FORMED checks, so the
identity is verified, not assumed.

Every check is a pure function of its inputs. A property with no evidence
of a violation PASSES; "checked" says how many things it looked at, so a
property that passed vacuously (checked == 0) is visible in the report.
"""

import hashlib
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field

WELL_FORMED = "history-well-formed"
ACKED_PRESENT = "acked-writes-on-every-node-exactly-once"
IDENTICAL = "replicas-identical"
INCREASING = "bids-strictly-increasing-winner-is-max"
NO_BID_AFTER_CLOSE = "no-bid-accepted-after-close"
REJECTIONS = "rejections-consistent-with-final-state"
UNKNOWN_AT_MOST_ONCE = "unknown-outcome-at-most-once"
ONE_LEADER_PER_TERM = "at-most-one-leader-per-term"

PROPERTY_NAMES = (
    WELL_FORMED, ACKED_PRESENT, IDENTICAL, INCREASING,
    NO_BID_AFTER_CLOSE, REJECTIONS, UNKNOWN_AT_MOST_ONCE, ONE_LEADER_PER_TERM,
)

WRITES = ("login", "create", "bid")

# The reasons AuctionState.apply() and StateMachine.apply_fn() can give for
# a rejected bid. Anything else is reported: a checker that waves through a
# reason it does not understand would pass a bug.
_LOW = re.compile(r"^Bid of (\d+) is not added because current highest bid is (\d+)$")
_CLOSED = re.compile(r"^Auction is already closed ")
_TIME_UP = re.compile(r"^Time is up ")
_NO_AUCTION = re.compile(r"^Auction ID does not exist")
_STALE = re.compile(r"^stale request")
_DUP_ID = re.compile(r"^ID already exists")


@dataclass
class PropertyResult:
    name: str
    passed: bool
    checked: int
    violations: list = field(default_factory=list)


def _result(name, checked, violations):
    return PropertyResult(name, not violations, checked, violations)


def _bid_key(r):
    return (r["auction_id"], r["user"], r["amount"])


def _states(dumps) -> dict:
    """{node: parsed state document}. Unparseable JSON is IDENTICAL's to
    report; here such a node is skipped."""
    out = {}
    for node, d in sorted(dumps.items()):
        try:
            out[node] = json.loads(d["state_json"])
        except (ValueError, KeyError):
            pass  # no dump, or not JSON: IDENTICAL reports it
    return out


def _auctions(doc) -> dict:
    return {a["auction_id"]: a for a in doc["auctions"]}


def _bid_counts(doc) -> Counter:
    return Counter((a["auction_id"], b["bidder"], b["amount"]) for a in doc["auctions"] for b in a["bids"])


def _reference(states):
    """One node's state to check history against. IDENTICAL separately
    proves every node holds the same; if they differ, checking against
    each would only repeat that failure."""
    return states[min(states)] if states else None


# --- the properties ------------------------------------------------------------


def check_well_formed(history) -> PropertyResult:
    """The workload rule the dedupe table relies on (A5, A18): one write
    outstanding per client, serial numbers strictly increasing and never
    reused; and the bid identity above is unique."""
    violations = []
    by_client = defaultdict(list)
    for r in history:
        if r["op"] in WRITES:
            by_client[r["client_id"]].append(r)
    for cid, recs in sorted(by_client.items()):
        recs = sorted(recs, key=lambda r: (r["start"], r["seq"]))
        for prev, cur in zip(recs, recs[1:]):
            if cur["start"] < prev["end"]:
                violations.append(
                    f"client {prev['client']}: seq {cur['seq']} started before seq {prev['seq']} ended "
                    f"(two writes in flight on one client_id)")
            if cur["seq"] <= prev["seq"]:
                violations.append(f"client {prev['client']}: seq {cur['seq']} sent after seq {prev['seq']}")
        for seq, n in Counter(r["seq"] for r in recs).items():
            if n > 1:
                violations.append(f"client {recs[0]['client']}: seq {seq} used {n} times")
    for key, n in Counter(_bid_key(r) for r in history if r["op"] == "bid").items():
        if n > 1:
            violations.append(f"bid {key} sent {n} times: (auction, bidder, amount) must be unique")
    return _result(WELL_FORMED, sum(len(v) for v in by_client.values()), violations)


def check_acked_present(history, states) -> PropertyResult:
    """Every acknowledged write is in every node's final state; a bid
    exactly once."""
    violations, checked = [], 0
    for node, doc in states.items():
        counts = _bid_counts(doc)
        auctions = _auctions(doc)
        sessions = {tuple(s) for s in doc["sessions"]}
        for r in history:
            if r["outcome"] != "ok":
                continue
            checked += 1
            if r["op"] == "bid":
                n = counts[_bid_key(r)]
                if n != 1:
                    violations.append(f"{node}: acknowledged bid {_bid_key(r)} (client {r['client']}, "
                                      f"seq {r['seq']}) is present {n} times")
                if (r.get("high_bid"), r.get("high_bidder")) != (r["amount"], r["user"]):
                    violations.append(f"{node}: bid {_bid_key(r)} acknowledged with high bid "
                                      f"{r.get('high_bid')} by {r.get('high_bidder')!r}, not itself")
            elif r["op"] == "create":
                a = auctions.get(r["auction_id"])
                if a is None or a["owner"] != r["user"] or a["close_time"] != r.get("close_time", a["close_time"]):
                    violations.append(f"{node}: acknowledged create of auction {r['auction_id']} by "
                                      f"{r['user']} is missing or differs: {a and {k: a[k] for k in ('owner', 'close_time')}}")
            elif r["op"] == "login":
                if (r.get("token_hash"), r["user"]) not in sessions:
                    violations.append(f"{node}: acknowledged login of {r['user']} (token hash "
                                      f"{r.get('token_hash')}) is not in the session table")
    return _result(ACKED_PRESENT, checked, violations)


def _first_difference(a, b, path="state"):
    if type(a) is not type(b):
        return f"{path}: {a!r} != {b!r}"
    if isinstance(a, dict):
        for k in sorted(set(a) | set(b)):
            if k not in a or k not in b:
                return f"{path}.{k}: present on one side only"
            d = _first_difference(a[k], b[k], f"{path}.{k}")
            if d:
                return d
        return None
    if isinstance(a, list):
        for i, (x, y) in enumerate(zip(a, b)):
            d = _first_difference(x, y, f"{path}[{i}]")
            if d:
                return d
        if len(a) != len(b):
            return f"{path}: length {len(a)} != {len(b)}"
        return None
    return None if a == b else f"{path}: {a!r} != {b!r}"


def check_identical(dumps) -> PropertyResult:
    """Every node's applied state is the same: same index, each digest
    really is sha256 of its JSON, same digest, same JSON (the full
    comparison, which also names the first field that differs)."""
    violations = []
    if not dumps:
        return _result(IDENTICAL, 0, ["no dumps: nothing to compare (did every node fail to answer?)"])
    for node, d in sorted(dumps.items()):
        if "error" in d:
            violations.append(f"{node}: no dump ({d['error']})")
    dumps = {n: d for n, d in dumps.items() if "error" not in d}
    for node, d in sorted(dumps.items()):
        actual = hashlib.sha256(d["state_json"].encode("utf-8")).hexdigest()
        if actual != d["state_digest"]:
            violations.append(f"{node}: state_digest {d['state_digest'][:12]} does not match sha256 of its "
                              f"state_json ({actual[:12]})")
    indexes = {n: d["digest_index"] for n, d in dumps.items()}
    if len(set(indexes.values())) > 1:
        violations.append(f"digest_index differs: {indexes} (a node had not caught up)")
    states = _states(dumps)
    for node in sorted(set(dumps) - set(states)):
        violations.append(f"{node}: state_json is not valid JSON")
    if states:
        ref = min(states)
        for node, doc in sorted(states.items()):
            if node == ref:
                continue
            if dumps[node]["state_digest"] != dumps[ref]["state_digest"]:
                violations.append(f"{node} vs {ref}: digest {dumps[node]['state_digest'][:12]} != "
                                  f"{dumps[ref]['state_digest'][:12]}")
            diff = _first_difference(doc, states[ref])
            if diff:
                violations.append(f"{node} vs {ref}: first difference at {diff}")
    return _result(IDENTICAL, len(dumps), violations)


def check_increasing(states) -> PropertyResult:
    """Per auction: each bid is above the one before it; an open auction
    has no winner; a closed one's winner is its highest (= last) bid, or
    none if it had no bids."""
    violations, checked = [], 0
    for node, doc in states.items():
        for a in doc["auctions"]:
            checked += 1
            bids = a["bids"]
            for i, (prev, cur) in enumerate(zip(bids, bids[1:]), start=1):
                if cur["amount"] <= prev["amount"]:
                    violations.append(f"{node}: auction {a['auction_id']} bid #{i} ({cur['bidder']}, "
                                      f"{cur['amount']}) is not above #{i - 1} ({prev['bidder']}, {prev['amount']})")
            top = max(bids, key=lambda b: b["amount"]) if bids else None
            if a["closed"] and a["winner"] != top:
                violations.append(f"{node}: auction {a['auction_id']} winner {a['winner']} is not the max bid {top}")
            if not a["closed"] and a["winner"] is not None:
                violations.append(f"{node}: auction {a['auction_id']} is open but has winner {a['winner']}")
    return _result(INCREASING, checked, violations)


def check_no_bid_after_close(history, states) -> PropertyResult:
    """No bid is accepted after its auction's close, seen two ways:
      state:   every bid's server stamp is <= close_time; a closed
               auction's closed_at is after close_time and after every bid.
      history: no ACKNOWLEDGED bid was started after close_time. The
               server stamps curr_time after the client started the call,
               on the same machine's clock, so such a bid would have had a
               stamp past close_time and had to be refused.
    (A bid applied after the CloseAuction entry would also break
    INCREASING: it would be above the frozen winner.)"""
    violations, checked = [], 0
    for node, doc in states.items():
        for a in doc["auctions"]:
            for b in a["bids"]:
                checked += 1
                if b["time"] > a["close_time"]:
                    violations.append(f"{node}: auction {a['auction_id']} bid ({b['bidder']}, {b['amount']}) "
                                      f"stamped {b['time']}, after close_time {a['close_time']}")
                if a["closed"] and b["time"] > a["closed_at"]:
                    violations.append(f"{node}: auction {a['auction_id']} bid ({b['bidder']}, {b['amount']}) "
                                      f"stamped after closed_at {a['closed_at']}")
            if a["closed"] and a["closed_at"] <= a["close_time"]:
                violations.append(f"{node}: auction {a['auction_id']} closed at {a['closed_at']}, "
                                  f"not after close_time {a['close_time']}")
    ref = _reference(states)
    if ref is not None:
        auctions = _auctions(ref)
        for r in history:
            if r["op"] == "bid" and r["outcome"] == "ok" and r["auction_id"] in auctions:
                checked += 1
                close_time = auctions[r["auction_id"]]["close_time"]
                if r["start"] > close_time:
                    violations.append(f"bid {_bid_key(r)} (client {r['client']}, seq {r['seq']}) was "
                                      f"acknowledged but started after close_time ({r['start']} > {close_time})")
    return _result(NO_BID_AFTER_CLOSE, checked, violations)


def check_rejections(history, states) -> PropertyResult:
    """Every acknowledged "no" agrees with the final state:
      any rejected bid         is NOT in the state;
      "...highest bid is H"    H >= the amount, and a bid of exactly H exists;
      "already closed"         the auction is closed;
      "Time is up"             close_time is before the call returned;
      "stale request"          never: the workload rule forbids it;
      create "ID already exists": the auction exists.
    A rejected bid that IS in the state is the signature of a dedupe
    failure: the retry was executed again instead of answered from the
    table, and lost to its own first attempt."""
    violations, checked = [], 0
    ref = _reference(states)
    if ref is None:
        return _result(REJECTIONS, 0, [])
    auctions, counts = _auctions(ref), _bid_counts(ref)
    for r in history:
        if r["outcome"] != "rejected" or r["op"] not in ("bid", "create"):
            continue
        checked += 1
        reason, who = r["reason"], f"client {r['client']} seq {r['seq']}"
        a = auctions.get(r["auction_id"])
        if _STALE.match(reason):
            violations.append(f"{who}: rejected as {reason!r}: the one-write-per-client rule was broken")
            continue
        if r["op"] == "create":
            if _DUP_ID.match(reason):
                if a is None:
                    violations.append(f"{who}: create {r['auction_id']} refused as duplicate, but no such auction")
            else:
                violations.append(f"{who}: create {r['auction_id']} refused with unexpected reason {reason!r}")
            continue
        if counts[_bid_key(r)]:
            violations.append(f"{who}: bid {_bid_key(r)} was rejected ({reason!r}) but is present in the state")
        if m := _LOW.match(reason):
            amount, high = int(m.group(1)), int(m.group(2))
            if amount != r["amount"] or high < amount:
                violations.append(f"{who}: {reason!r} does not fit amount {r['amount']}")
            if a is None or high not in {b["amount"] for b in a["bids"]}:
                violations.append(f"{who}: rejected because the high bid was {high}, but auction "
                                  f"{r['auction_id']} never had a bid of {high}")
        elif _CLOSED.match(reason):
            if a is None or not a["closed"]:
                violations.append(f"{who}: bid refused as 'already closed' but auction {r['auction_id']} is not closed")
        elif _TIME_UP.match(reason):
            if a is None or a["close_time"] >= r["end"]:
                violations.append(f"{who}: bid refused as 'Time is up' but close_time is not before the reply")
        elif _NO_AUCTION.match(reason):
            pass  # it may have been created later; absence of the bid is checked above
        else:
            violations.append(f"{who}: bid refused with unexpected reason {reason!r}")
    return _result(REJECTIONS, checked, violations)


def check_unknown_at_most_once(history, states) -> PropertyResult:
    """An operation whose outcome the client never learned may have
    committed or not -- but never twice (section 8 dedupe)."""
    violations, checked = [], 0
    for node, doc in states.items():
        counts = _bid_counts(doc)
        for r in history:
            if r["op"] == "bid" and r["outcome"] == "unknown":
                checked += 1
                if counts[_bid_key(r)] > 1:
                    violations.append(f"{node}: unknown-outcome bid {_bid_key(r)} (client {r['client']}, "
                                      f"seq {r['seq']}) is present {counts[_bid_key(r)]} times")
    return _result(UNKNOWN_AT_MOST_ONCE, checked, violations)


def check_one_leader_per_term(leaders) -> PropertyResult:
    """Figure 3, Election Safety: at most one leader can be elected in a
    given term."""
    by_term = defaultdict(lambda: defaultdict(set))
    for s in leaders:
        by_term[s["term"]][s["node"]].add(s["source"])
    violations = [
        f"term {term}: {len(nodes)} leaders: "
        + "; ".join(f"{n} (seen by {', '.join(sorted(src))})" for n, src in sorted(nodes.items()))
        for term, nodes in sorted(by_term.items()) if len(nodes) > 1
    ]
    return _result(ONE_LEADER_PER_TERM, len(leaders), violations)


def check_all(history, dumps, leaders) -> list:
    states = _states(dumps)
    return [
        check_well_formed(history),
        check_acked_present(history, states),
        check_identical(dumps),
        check_increasing(states),
        check_no_bid_after_close(history, states),
        check_rejections(history, states),
        check_unknown_at_most_once(history, states),
        check_one_leader_per_term(leaders),
    ]


def format_report(report, max_evidence=10) -> str:
    lines = []
    for r in report:
        lines.append(f"{'PASS' if r.passed else 'FAIL'}  {r.name}  (checked {r.checked})")
        for v in r.violations[:max_evidence]:
            lines.append(f"        - {v}")
        if len(r.violations) > max_evidence:
            lines.append(f"        ... and {len(r.violations) - max_evidence} more")
    ok = all(r.passed for r in report)
    lines.append(f"{'ALL PROPERTIES PASS' if ok else 'SOME PROPERTIES FAILED'}")
    return "\n".join(lines)
