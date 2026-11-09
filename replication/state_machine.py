"""The replicated state machine: what RaftNode's apply_fn calls.

    sm = StateMachine()
    node = RaftNode(..., apply_fn=sm.apply_fn)

apply_fn(index, data) runs on the applier thread, once per committed
COMMAND entry, in log order, on every node (R2). It:
  1. decodes the bytes (replication/codec.py);
  2. checks the section 8 dedupe table for (client_id, seq);
  3. dispatches: auction commands -> AuctionState.apply() (unchanged M1
     code), session commands -> SessionState.apply();
  4. records the response in the dedupe table, and returns it.

Its return value becomes Proposal.result on the leader that proposed the
entry (R4). That is how a handler learns the outcome of ITS command.

Everything here is deterministic: no clock, no randomness, no I/O. The
dedupe table is part of the replicated state: every node builds the same
table from the same log, so a NEW leader after a failover still recognises
a retry of a command the OLD leader committed.

Dedupe (Raft paper, section 8): "the state machine tracks the latest serial
number processed for each client, along with the associated response. If
it receives a command whose serial number has already been executed, it
responds immediately without re-executing the request."
  * seq == last seq for this client  -> return the stored response.
  * seq <  last seq                  -> "stale request", not executed. A
    client has one write outstanding at a time (client/auction_client.py),
    so a lower number can only be a long-delayed duplicate of something
    already superseded; there is no stored response for it any more.
  * seq >  last seq, or new client   -> execute, store.
  * client_id == ""                  -> never deduplicated (the closer).
It lives HERE, not in AuctionState.apply(), so the M1 apply() is untouched
and dedupe covers session commands too.
"""

import copy
import logging
import threading
from dataclasses import dataclass

from auction.state import AuctionState, PlaceBid, Result
from replication import codec
from sessions.state import Login, Logout, SessionState

log = logging.getLogger("replication.state_machine")


class CorruptEntryError(Exception):
    """A committed entry could not be decoded. Raised out of apply_fn on
    purpose: the applier halts (R3, fail-stop)."""


@dataclass(frozen=True)
class Applied:
    """The response to one applied command: what the handler replies with,
    and what the dedupe table stores.

    high_bid / high_bidder: PlaceBid only. The auction's highest bid right
    after THIS command was applied, read inside apply_fn (see _high_bid).
    token: Login only. The token that is actually in the session table --
    on a dedupe hit, the FIRST attempt's token, not the retry's.
    """

    result: Result
    high_bid: int = 0
    high_bidder: str = ""
    token: str = ""


_SESSION_COMMANDS = (Login, Logout)


class StateMachine:
    def __init__(self, auctions: AuctionState = None, sessions: SessionState = None):
        self.auctions = auctions if auctions is not None else AuctionState()
        self.sessions = sessions if sessions is not None else SessionState()
        # client_id -> (seq, Applied): the latest serial number per client
        # and its response.
        self._dedupe: dict = {}
        # The index of the last command apply_fn was called with. Not
        # replicated state and not in snapshot(): it only labels WHICH state
        # indexed_snapshot() returns, for the status RPC (docs/decisions.md,
        # T4). No-op entries never reach apply_fn, so it can be below the
        # node's lastApplied.
        self._last_index = 0
        # Held for the whole of apply_fn. Under Raft only the applier thread
        # calls apply_fn, so this never contends; it matters for
        # LocalReplicator (single-node tests), where handler threads call
        # apply_fn directly. It makes "apply, then read the high bid" one
        # step that no other command can interleave with.
        self._lock = threading.Lock()

    def apply_fn(self, index: int, data: bytes) -> Applied:
        try:
            env = codec.decode(data)
        except codec.DecodeError as e:
            # Every node holds the same bytes at this index, so every node
            # fails here: they all halt at the same index with the same
            # state. Skipping would also be "the same everywhere" today, but
            # it would hide a bug that can only come from a corrupt log or a
            # code/version mismatch -- stop loudly instead (R3).
            log.critical(
                "apply_fn: index %d does not decode (%s); %d bytes: %s",
                index, e, len(data), data[:64].hex(),
            )
            raise CorruptEntryError(f"index {index}: {e}") from e

        with self._lock:
            self._last_index = index
            if env.client_id:
                last = self._dedupe.get(env.client_id)
                if last is not None:
                    last_seq, last_applied = last
                    if env.seq == last_seq:
                        return last_applied
                    if env.seq < last_seq:
                        return Applied(Result(
                            success=False,
                            reason=f"stale request: seq {env.seq} is older than {last_seq}",
                        ))
            applied = self._dispatch(env.command)
            if env.client_id:
                self._dedupe[env.client_id] = (env.seq, applied)
            return applied

    def _dispatch(self, command) -> Applied:
        if isinstance(command, _SESSION_COMMANDS):
            result = self.sessions.apply(command)
            token = command.token if isinstance(command, Login) and result.success else ""
            return Applied(result, token=token)

        result = self.auctions.apply(command)
        if isinstance(command, PlaceBid):
            high_bid, high_bidder = self._high_bid(command.auction_id)
            return Applied(result, high_bid=high_bid, high_bidder=high_bidder)
        return Applied(result)

    def _high_bid(self, auction_id) -> tuple:
        """The high bid right after the PlaceBid just applied.

        In M1 the handler read this after apply(), lock-free, and it could
        be stale (another bid could land in between). Here it is read on
        the applier thread, the ONLY thread that changes AuctionState, and
        under self._lock, so nothing can land in between: it is exactly
        the state this command left behind. It is a pure function of the
        applied log, so it is deterministic, and it is stored in the
        dedupe table with the rest of the response.
        """
        with self.auctions.lock:
            auction = self.auctions.auctions.get(auction_id)
            if auction is None or not auction["bids"]:
                return 0, ""
            top = max(auction["bids"], key=lambda b: b.bid_amount)
            return top.bid_amount, top.bidder

    def snapshot(self) -> dict:
        """A deep copy of all replicated state, for comparing nodes."""
        return self.indexed_snapshot()[1]

    def last_index(self) -> int:
        """The index of the last command apply_fn saw. Cheap: no copy."""
        with self._lock:
            return self._last_index

    def indexed_snapshot(self) -> tuple:
        """(index, snapshot), taken together under self._lock, so the index
        is exactly the last command folded into that snapshot."""
        with self._lock:
            with self.auctions.lock:
                auctions = copy.deepcopy(self.auctions.auctions)
            return self._last_index, {
                "auctions": auctions,
                "sessions": self.sessions.snapshot(),
                "dedupe": dict(self._dedupe),
            }
