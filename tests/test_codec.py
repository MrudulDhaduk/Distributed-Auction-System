"""Specifies replication/codec.py: the bytes a command becomes in the Raft
log (proto/commands.proto), and back.

Round trip for every command type: decode(encode(c)) == c, with the
(client_id, seq) envelope. Datetimes must survive to the microsecond,
because apply() compares them (curr_time > close_time).

Also: the field numbers are pinned against a byte string written out by
hand. If someone renumbers a field in commands.proto, an old log.bin would
decode differently -- this test goes red first.
"""

import datetime

import pytest

from auction.state import CloseAuction, CreateAuction, PlaceBid
from replication import codec
from sessions.state import Login, Logout

UTC = datetime.timezone.utc
T = datetime.datetime(2030, 1, 1, 12, 0, 0, 123456, tzinfo=UTC)

EVERY_COMMAND = [
    CreateAuction(auction_id=7, item="clock", close_time=T, owner="nisarg", description="nice"),
    CreateAuction(auction_id=8, item="", close_time=T, owner="nisarg", description=""),
    PlaceBid(auction_id=7, bidder="karan", amount=150, curr_time=T),
    CloseAuction(auction_id=7, curr_time=T + datetime.timedelta(microseconds=1)),
    Login(token="tok-abc", username="mrudul"),
    Logout(token="tok-abc"),
]


@pytest.mark.parametrize("command", EVERY_COMMAND, ids=lambda c: type(c).__name__)
def test_every_command_round_trips(command):
    data = codec.encode(command, client_id="c1", seq=42)

    assert isinstance(data, bytes)
    env = codec.decode(data)
    assert env == codec.Envelope(command=command, client_id="c1", seq=42)


@pytest.mark.xfail(
    strict=True,
    reason="M1 bug, common/time_conv.py:25: int(dt.timestamp() * 1e6) truncates a float. "
    "2039-12-20 05:40:54.254810 -> 2207972454254809.8 -> ...809: 1 microsecond lost. "
    "Not fixed here: CLAUDE.md asks to point at M1 bugs, not fix them silently. "
    "Determinism is NOT affected (every node, the leader too, applies the decoded bytes).",
)
def test_round_trip_is_exact_to_the_microsecond_found_m1_bug():
    t = datetime.datetime(2039, 12, 20, 5, 40, 54, 254810, tzinfo=UTC)
    command = PlaceBid(auction_id=1, bidder="a", amount=1, curr_time=t)

    assert codec.decode(codec.encode(command)).command == command


def test_no_client_means_empty_client_id_and_seq_zero():
    env = codec.decode(codec.encode(CloseAuction(auction_id=1, curr_time=T)))

    assert (env.client_id, env.seq) == ("", 0)


def test_decoded_times_are_timezone_aware_utc():
    env = codec.decode(codec.encode(PlaceBid(auction_id=1, bidder="a", amount=1, curr_time=T)))

    assert env.command.curr_time.utcoffset() == datetime.timedelta(0)


def test_a_naive_datetime_cannot_be_encoded():
    naive = datetime.datetime(2030, 1, 1)
    with pytest.raises(ValueError):
        codec.encode(PlaceBid(auction_id=1, bidder="a", amount=1, curr_time=naive))


def test_unknown_command_type_cannot_be_encoded():
    with pytest.raises(TypeError):
        codec.encode("not a command")


@pytest.mark.parametrize("data", [b"\xff\xff\xff", b"", b"\x08\x01"])
def test_garbage_or_empty_bytes_fail_to_decode(data):
    """b"" parses as a Command with no `kind` set, and b"\\x08\\x01" is
    field 1 as a varint (wrong wire type for a string). Neither is a
    command: both must raise, never decode into something."""
    with pytest.raises(codec.DecodeError):
        codec.decode(data)


def test_field_numbers_are_pinned():
    """Logout(token="t") with client_id "c", seq 5, written by hand:
        0a 01 63      field 1 (client_id), length 1, "c"
        10 05         field 2 (seq), varint 5
        72 03 0a 01 74   field 14 (logout), length 3:
                            field 1 (token), length 1, "t"
    """
    data = bytes.fromhex("0a0163" "1005" "7203" "0a0174")

    assert codec.decode(data) == codec.Envelope(Logout(token="t"), "c", 5)
    assert codec.encode(Logout(token="t"), client_id="c", seq=5) == data
