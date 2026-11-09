"""Run one Raft node as its own process.

    python -m raft.run_node --id n1 --port 50061 \
        --peers n2=127.0.0.1:50062,n3=127.0.0.1:50063

Every change of role or term is logged as one line, e.g.
    12:00:01.234 [n2] term 4 FOLLOWER -> CANDIDATE (was term 3): election timeout
scripts/run_cluster.ps1 starts three of these in separate windows.
"""

import argparse
import logging
import logging.handlers
import queue
import random
import sys
from concurrent import futures

import grpc

from generated import raft_pb2_grpc
from raft.node import (
    DEFAULT_ELECTION_TIMEOUT,
    DEFAULT_HEARTBEAT_INTERVAL,
    RaftNode,
)
from raft.servicer import RaftServicer
from raft.transport import GrpcTransport


def parse_peers(text: str) -> dict:
    """"n2=127.0.0.1:50062,n3=127.0.0.1:50063" -> {"n2": "127.0.0.1:50062", ...}"""
    peers = {}
    for item in filter(None, (p.strip() for p in text.split(","))):
        pid, sep, addr = item.partition("=")
        if not sep or not pid or ":" not in addr:
            raise ValueError(f"bad peer {item!r}; expected id=host:port")
        if pid in peers:
            raise ValueError(f"duplicate peer id {pid!r}")
        peers[pid] = addr
    return peers


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Run one Raft node (election + replication; no client RPC yet).")
    p.add_argument("--id", required=True, help="this node's id, e.g. n1")
    p.add_argument("--port", type=int, required=True, help="port to serve RaftService on")
    p.add_argument("--host", default="127.0.0.1", help="interface to bind (default: local only)")
    p.add_argument("--peers", default="", help="comma-separated id=host:port of the OTHER nodes")
    p.add_argument("--election-timeout-min", type=float, default=DEFAULT_ELECTION_TIMEOUT[0])
    p.add_argument("--election-timeout-max", type=float, default=DEFAULT_ELECTION_TIMEOUT[1])
    p.add_argument("--heartbeat", type=float, default=DEFAULT_HEARTBEAT_INTERVAL)
    p.add_argument("--seed", type=int, default=None, help="seed the timeout RNG (default: random)")
    p.add_argument("--verbose", action="store_true", help="also log lost RPCs")
    return p


def setup_logging(verbose: bool) -> logging.handlers.QueueListener:
    """Log through a queue so a blocked console cannot block the node.

    A Windows console window stops accepting writes while the user is
    selecting text in it; a handler that wrote synchronously would freeze
    whichever thread logged -- and the node logs while holding its lock.
    QueueHandler.emit() only enqueues; the listener thread does the write.
    """
    q = queue.SimpleQueue()
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(logging.Formatter("%(asctime)s.%(msecs)03d %(message)s", "%H:%M:%S"))
    listener = logging.handlers.QueueListener(q, console)
    root = logging.getLogger()
    root.addHandler(logging.handlers.QueueHandler(q))
    root.setLevel(logging.DEBUG if verbose else logging.INFO)
    listener.start()
    return listener


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        peers = parse_peers(args.peers)
    except ValueError as e:
        raise SystemExit(f"error: {e}")
    if args.id in peers:
        raise SystemExit("error: --peers must not include this node's own id")

    listener = setup_logging(args.verbose)
    log = logging.getLogger("raft.run_node")

    transport = GrpcTransport(peers)
    node = RaftNode(
        args.id,
        list(peers),
        transport,
        election_timeout=(args.election_timeout_min, args.election_timeout_max),
        heartbeat_interval=args.heartbeat,
        rng=random.Random(args.seed),
        observer=lambda event: log.info(event.describe()),
    )

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    raft_pb2_grpc.add_RaftServiceServicer_to_server(RaftServicer(node), server)
    bound = server.add_insecure_port(f"{args.host}:{args.port}")
    if bound == 0:
        raise SystemExit(f"error: could not bind {args.host}:{args.port}")
    server.start()
    node.start()
    log.info(
        "[%s] listening on %s:%d, peers: %s; election timeout %.2f-%.2fs, heartbeat %.2fs",
        args.id, args.host, args.port, ", ".join(peers) or "(none)",
        args.election_timeout_min, args.election_timeout_max, args.heartbeat,
    )
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        pass
    finally:
        node.stop()
        server.stop(grace=0.5)
        transport.close()
        listener.stop()


if __name__ == "__main__":
    main()
