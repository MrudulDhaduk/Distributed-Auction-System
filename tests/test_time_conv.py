"""Specifies common/time_conv.py: datetime <-> epoch microseconds, EXACT.

The M1 version went through a float (dt.timestamp() * 1e6) and lost a
microsecond for some values (docs/decisions.md, T1). These tests pin the
integer version: dt_to_micros(micros_to_dt(n)) == n and
micros_to_dt(dt_to_micros(dt)) == dt, for many random values over the whole
range we care about, including before 1970 and far in the future.

Random but seeded: the same values every run, so a failure is reproducible.
"""

import datetime
import random

import pytest

from common import time_conv

UTC = datetime.timezone.utc
EPOCH = datetime.datetime(1970, 1, 1, tzinfo=UTC)
# 1900 .. 2200, in microseconds since the epoch.
LO = -(70 * 365 + 17) * 86_400 * 10**6
HI = (230 * 365 + 56) * 86_400 * 10**6


def test_the_m1_counterexample_is_exact():
    t = datetime.datetime(2039, 12, 20, 5, 40, 54, 254810, tzinfo=UTC)
    assert time_conv.dt_to_micros(t) == 2207972454254810
    assert time_conv.micros_to_dt(2207972454254810) == t


@pytest.mark.parametrize("seed", range(5))
def test_micros_round_trip_over_random_values(seed):
    rng = random.Random(seed)
    for _ in range(20_000):
        n = rng.randint(LO, HI)
        assert time_conv.dt_to_micros(time_conv.micros_to_dt(n)) == n


@pytest.mark.parametrize("seed", range(5))
def test_datetime_round_trip_over_random_values(seed):
    rng = random.Random(1000 + seed)
    for _ in range(20_000):
        dt = EPOCH + datetime.timedelta(microseconds=rng.randint(LO, HI))
        assert time_conv.micros_to_dt(time_conv.dt_to_micros(dt)) == dt


def test_epoch_and_neighbours():
    assert time_conv.dt_to_micros(EPOCH) == 0
    assert time_conv.micros_to_dt(0) == EPOCH
    assert time_conv.micros_to_dt(-1) == EPOCH - datetime.timedelta(microseconds=1)
    assert time_conv.dt_to_micros(EPOCH - datetime.timedelta(microseconds=1)) == -1


def test_a_non_utc_aware_datetime_converts_to_the_same_instant():
    ist = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
    t = datetime.datetime(2030, 1, 1, 17, 30, 0, 7, tzinfo=ist)
    assert time_conv.dt_to_micros(t) == time_conv.dt_to_micros(
        datetime.datetime(2030, 1, 1, 12, 0, 0, 7, tzinfo=UTC))


def test_decoded_values_are_utc():
    assert time_conv.micros_to_dt(123).utcoffset() == datetime.timedelta(0)


def test_naive_is_still_refused():
    with pytest.raises(ValueError):
        time_conv.dt_to_micros(datetime.datetime(2030, 1, 1))
