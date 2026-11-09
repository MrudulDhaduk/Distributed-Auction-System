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

Exact, integer-only arithmetic (docs/decisions.md, T1). The M1 version
went through a float, int(dt.timestamp() * 1_000_000), and a float has only
53 bits of mantissa: 2039-12-20 05:40:54.254810 became 2207972454254809.8
and then ...809, one microsecond early. A datetime minus the epoch is an
exact timedelta, and floor-dividing a timedelta by another timedelta is
exact integer division, so nothing is rounded anywhere.
"""

import datetime

EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
_ONE_MICRO = datetime.timedelta(microseconds=1)


def dt_to_micros(dt: datetime.datetime) -> int:
    """Convert a timezone-aware datetime to epoch microseconds (exact)."""
    if dt.utcoffset() is None:
        raise ValueError(f"Time stamp is naive, got {dt}, must be time-zone aware UTC")
    return (dt - EPOCH) // _ONE_MICRO


def micros_to_dt(micros: int) -> datetime.datetime:
    """Convert epoch microseconds to a timezone-aware UTC datetime (exact)."""
    return EPOCH + datetime.timedelta(microseconds=micros)

def now_utc() -> datetime.datetime:
    """It is the only clock read in the codebase"""
    return datetime.datetime.now(datetime.timezone.utc)
