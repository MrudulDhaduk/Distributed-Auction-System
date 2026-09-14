"""datetime <-> int64 epoch-microsecond conversion for the gRPC wire.

Every timestamp in auction.proto is int64 epoch microseconds, UTC (see the
design note at the top of that file). Every datetime.datetime on the
domain side (auction/state.py's CreateAuction.close_time, PlaceBid.curr_time,
...) must be timezone-aware UTC -- no exceptions, no naive datetimes.

Why this matters: datetime.timestamp() treats a naive datetime as *local
time*, silently. If a naive datetime ever reaches AuctionState.apply(),
comparisons like `command.curr_time > auction["close_time"]` either raise
TypeError (aware vs. naive) or, worse, silently compare two values that
don't mean what they look like they mean. Converting at the one boundary
below -- and only here -- keeps that discipline in one place instead of
scattered across every handler.
"""

import datetime

MICROS_PER_SECOND = 1_000_000


def dt_to_micros(dt: datetime.datetime) -> int:
    """Convert a timezone-aware UTC datetime to epoch microseconds."""
    if dt.utcoffset() is None:
        raise ValueError(f"Time stamp is naive, got {dt}, must be time-zone aware UTC")
    return int(dt.timestamp() * MICROS_PER_SECOND)


def micros_to_dt(micros: int) -> datetime.datetime:
    """Convert epoch microseconds to a timezone-aware UTC datetime."""
    return datetime.datetime.fromtimestamp(
        micros / MICROS_PER_SECOND, tz=datetime.timezone.utc
    )

def now_utc() -> datetime.datetime:
    """It is the only clock read in the codebase"""
    return datetime.datetime.now(datetime.timezone.utc)
