"""Command <-> bytes, for the Raft log. The ONE module that knows the wire
form of a command (proto/commands.proto).

    encode(command, client_id="", seq=0) -> bytes    on the leader, once
    decode(bytes) -> Envelope                          in apply_fn, on every node

Why bytes at all: Raft stores and ships opaque bytes (R1), and the log is
written to disk (log.bin) and replayed on restart. A Python dataclass has
neither a wire form nor a disk form; the proto message is both.

Times: the dataclasses hold timezone-aware UTC datetimes; the proto holds
int64 epoch microseconds. Converted only through common/time_conv.py, the
same functions auction.proto's handlers use, so there is one conversion
in the whole codebase. A naive datetime raises in encode (dt_to_micros),
on the leader, before anything is proposed.

Determinism: the leader serialises once, and every node decodes the SAME
bytes. Protobuf does not promise that two encoders produce identical bytes,
and we do not need it to: nobody re-encodes.
"""

from dataclasses import dataclass
from typing import Any

from google.protobuf.message import DecodeError as _ProtoDecodeError

from auction import state as auction
from common import time_conv
from generated import commands_pb2
from sessions import state as sessions


class DecodeError(Exception):
    """These bytes are not a command this code understands."""


@dataclass(frozen=True)
class Envelope:
    command: Any
    client_id: str = ""
    seq: int = 0


def encode(command, client_id: str = "", seq: int = 0) -> bytes:
    msg = commands_pb2.Command(client_id=client_id, seq=seq)
    c = command
    if isinstance(c, auction.CreateAuction):
        msg.create_auction.CopyFrom(commands_pb2.CreateAuction(
            auction_id=c.auction_id, item=c.item,
            close_time=time_conv.dt_to_micros(c.close_time),
            owner=c.owner, description=c.description,
        ))
    elif isinstance(c, auction.PlaceBid):
        msg.place_bid.CopyFrom(commands_pb2.PlaceBid(
            auction_id=c.auction_id, bidder=c.bidder, amount=c.amount,
            curr_time=time_conv.dt_to_micros(c.curr_time),
        ))
    elif isinstance(c, auction.CloseAuction):
        msg.close_auction.CopyFrom(commands_pb2.CloseAuction(
            auction_id=c.auction_id, curr_time=time_conv.dt_to_micros(c.curr_time),
        ))
    elif isinstance(c, sessions.Login):
        msg.login.CopyFrom(commands_pb2.Login(token=c.token, username=c.username))
    elif isinstance(c, sessions.Logout):
        msg.logout.CopyFrom(commands_pb2.Logout(token=c.token))
    else:
        raise TypeError(f"not a replicated command: {type(c).__name__}")
    return msg.SerializeToString()


def decode(data: bytes) -> Envelope:
    msg = commands_pb2.Command()
    try:
        msg.ParseFromString(data)
    except _ProtoDecodeError as e:
        raise DecodeError(f"not a Command: {e}") from e

    kind = msg.WhichOneof("kind")
    if kind == "create_auction":
        m = msg.create_auction
        command = auction.CreateAuction(
            auction_id=m.auction_id, item=m.item,
            close_time=time_conv.micros_to_dt(m.close_time),
            owner=m.owner, description=m.description,
        )
    elif kind == "place_bid":
        m = msg.place_bid
        command = auction.PlaceBid(
            auction_id=m.auction_id, bidder=m.bidder, amount=m.amount,
            curr_time=time_conv.micros_to_dt(m.curr_time),
        )
    elif kind == "close_auction":
        m = msg.close_auction
        command = auction.CloseAuction(
            auction_id=m.auction_id, curr_time=time_conv.micros_to_dt(m.curr_time),
        )
    elif kind == "login":
        command = sessions.Login(token=msg.login.token, username=msg.login.username)
    elif kind == "logout":
        command = sessions.Logout(token=msg.logout.token)
    else:
        # None: no command set (b"" decodes to this). Anything else: a
        # oneof member this version of the code does not know.
        raise DecodeError(f"Command has no known kind (got {kind!r})")
    return Envelope(command=command, client_id=msg.client_id, seq=msg.seq)
