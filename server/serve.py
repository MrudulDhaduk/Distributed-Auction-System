"""One auction node = one process: the auction server and a Raft node.

    python -m server.serve --node-id n1 --client-port 50071 --peer-port 50061 \
        --peers n2=127.0.0.1:50062:50072,n3=127.0.0.1:50063:50073 \
        --data-dir data/n1 --llm-addr 127.0.0.1:50052

Two gRPC servers, on two ports (docs/decisions.md, A9):
  CLIENT port   AuthService + AuctionService, behind the AuthInterceptor.
                What clients dial.
  PEER port     RaftService only, NO interceptor. Raft RPCs come from the
                other nodes and carry no session token; putting them behind
                the auth check would refuse every heartbeat. In production
                this port would be reachable only by the other nodes and
                protected by mutual TLS (each node proves it is a cluster
                member with a certificate). Here it is plain TCP on
                127.0.0.1.

--peers lists the OTHER nodes as id=host:peer_port:client_port. The peer
port is where this node sends RequestVote/AppendEntries; the client port is
what it tells a client in a NOT_LEADER redirect (leader-addr).

Wiring, one object each:
  StateMachine     AuctionState + SessionState + dedupe table; its apply_fn
                   is the RaftNode's apply_fn.
  RaftNode         FileStorage on --data-dir (term, vote, log, fsync'd).
  RaftReplicator   the handlers' write path: propose, wait, map errors.
  AuctionCloser    on every node; proposes only while leader.

Restart: the data dir keeps the log. The state machine starts empty and is
rebuilt by replaying the log through apply_fn as the commit index is
learned again (P11). No snapshot.
"""

import argparse
import logging
import random
from concurrent import futures
from dataclasses import dataclass
from typing import Optional

import grpc

from generated import auction_pb2_grpc, raft_pb2_grpc
from llm_server import config as llm_config
from raft.file_storage import CorruptStorageError, FileStorage
from raft.node import DEFAULT_ELECTION_TIMEOUT, DEFAULT_HEARTBEAT_INTERVAL, RaftNode
from raft.run_node import setup_logging
from raft.servicer import RaftServicer
from raft.transport import GrpcTransport
from replication.replicator import DEFAULT_TIMEOUT, RaftReplicator
from replication.state_machine import StateMachine
from server.auction_servicer import AuctionServicer
from server.auth_interceptor import AuthInterceptor
from server.auth_servicer import AuthServicer
from server.closer import DEFAULT_INTERVAL_SECONDS, AuctionCloser
from server.llm_client import LLMClient
from sessions.store import SessionStore

log = logging.getLogger("server.serve")

# Client handlers BLOCK while their entry commits (up to DEFAULT_TIMEOUT), and
# CreateAuction also blocks on the LLM call. 16 workers so a few slow
# CreateAuctions cannot starve bids. The peer server's handlers never block
# on anything but the node lock and an fsync.
CLIENT_WORKERS = 16
PEER_WORKERS = 8


@dataclass(frozen=True)
class PeerAddr:
    host: str
    peer_port: int
    client_port: int


def parse_peers(text: str) -> dict:
    """"n2=127.0.0.1:50062:50072,..." -> {"n2": PeerAddr("127.0.0.1", 50062, 50072)}"""
    peers = {}
    for item in filter(None, (p.strip() for p in text.split(","))):
        pid, sep, rest = item.partition("=")
        parts = rest.split(":")
        if not sep or not pid or len(parts) != 3:
            raise ValueError(f"bad peer {item!r}; expected id=host:peer_port:client_port")
        if pid in peers:
            raise ValueError(f"duplicate peer id {pid!r}")
        host, peer_port, client_port = parts
        peers[pid] = PeerAddr(host, int(peer_port), int(client_port))
    return peers


class AuctionNode:
    """Everything one node process runs. Built, then start(), then stop()."""

    def __init__(
        self,
        node_id: str,
        host: str,
        client_port: int,
        peer_port: int,
        peers: dict,
        storage,
        llm_client,
        *,
        election_timeout: tuple = DEFAULT_ELECTION_TIMEOUT,
        heartbeat_interval: float = DEFAULT_HEARTBEAT_INTERVAL,
        propose_timeout: float = DEFAULT_TIMEOUT,
        closer_interval: float = DEFAULT_INTERVAL_SECONDS,
        rng: Optional[random.Random] = None,
    ):
        if node_id in peers:
            raise ValueError("--peers must not include this node's own id")
        self.node_id = node_id
        self.storage = storage

        self.state_machine = StateMachine()
        self.transport = GrpcTransport({pid: f"{a.host}:{a.peer_port}" for pid, a in peers.items()})
        self.raft = RaftNode(
            node_id,
            list(peers),
            self.transport,
            election_timeout=election_timeout,
            heartbeat_interval=heartbeat_interval,
            rng=rng,
            observer=lambda event: log.info(event.describe()),
            storage=storage,
            apply_fn=self.state_machine.apply_fn,
        )
        client_addrs = {pid: f"{a.host}:{a.client_port}" for pid, a in peers.items()}
        client_addrs[node_id] = f"{host}:{client_port}"
        self.replicator = RaftReplicator(self.raft, client_addrs, timeout=propose_timeout)
        sessions = SessionStore(self.state_machine.sessions, self.replicator)
        self.closer = AuctionCloser(self.state_machine.auctions, self.replicator, closer_interval)

        self.client_server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=CLIENT_WORKERS, thread_name_prefix=f"{node_id}-client"),
            interceptors=[AuthInterceptor(sessions, self.replicator)],
        )
        auction_pb2_grpc.add_AuthServiceServicer_to_server(AuthServicer(sessions), self.client_server)
        auction_pb2_grpc.add_AuctionServiceServicer_to_server(
            AuctionServicer(self.state_machine.auctions, llm_client, self.replicator), self.client_server
        )
        self._bind(self.client_server, host, client_port, "client")

        # No interceptors: see the module docstring.
        self.peer_server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=PEER_WORKERS, thread_name_prefix=f"{node_id}-peer"),
        )
        raft_pb2_grpc.add_RaftServiceServicer_to_server(RaftServicer(self.raft), self.peer_server)
        self._bind(self.peer_server, host, peer_port, "peer")

    @staticmethod
    def _bind(server, host, port, what):
        if server.add_insecure_port(f"{host}:{port}") == 0:
            raise RuntimeError(f"could not bind {what} port {host}:{port}")

    def start(self):
        # Peer port first, so the other nodes' RequestVote/AppendEntries can
        # reach this node as soon as it exists. Client port last, so no
        # client request arrives before the node and closer are running.
        self.peer_server.start()
        self.raft.start()
        self.client_server.start()
        self.closer.start()

    def stop(self):
        """Stop serving clients, then Raft. The storage is closed under the
        node lock, so a handler still inside a write finishes first and no
        write can land after (same fence as the tests' crash())."""
        self.closer.stop()
        self.client_server.stop(grace=None).wait()
        self.peer_server.stop(grace=None).wait()
        self.raft.stop()
        self.transport.close()
        close = getattr(self.storage, "close", None)
        if close:
            with self.raft._lock:
                close()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Run one auction node (auction server + Raft node).")
    p.add_argument("--node-id", required=True, help="this node's id, e.g. n1")
    p.add_argument("--client-port", type=int, required=True, help="port for AuthService + AuctionService")
    p.add_argument("--peer-port", type=int, required=True, help="port for RaftService (other nodes only)")
    p.add_argument("--peers", default="", help="the OTHER nodes: id=host:peer_port:client_port,...")
    p.add_argument("--data-dir", required=True, help="this node's own directory for term, vote and log")
    p.add_argument("--llm-addr", default=f"127.0.0.1:{llm_config.PORT}", help="LLM server host:port")
    p.add_argument("--host", default="127.0.0.1", help="interface to bind (default: local only)")
    p.add_argument("--election-timeout-min", type=float, default=DEFAULT_ELECTION_TIMEOUT[0])
    p.add_argument("--election-timeout-max", type=float, default=DEFAULT_ELECTION_TIMEOUT[1])
    p.add_argument("--heartbeat", type=float, default=DEFAULT_HEARTBEAT_INTERVAL)
    p.add_argument("--verbose", action="store_true", help="also log lost RPCs")
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        peers = parse_peers(args.peers)
    except ValueError as e:
        raise SystemExit(f"error: {e}")

    listener = setup_logging(args.verbose)
    try:
        storage = FileStorage(args.data_dir)
    except CorruptStorageError as e:
        listener.stop()
        raise SystemExit(f"error: {args.data_dir}: {e}")
    last = storage.last_index()
    log.info(
        "[%s] storage %s: term %d, votedFor %s, %d log entries (last term %d); state rebuilt by replay",
        args.node_id, args.data_dir, storage.term, storage.voted_for, last, storage.term_at(last),
    )

    node = AuctionNode(
        args.node_id, args.host, args.client_port, args.peer_port, peers, storage,
        LLMClient(args.llm_addr),
        election_timeout=(args.election_timeout_min, args.election_timeout_max),
        heartbeat_interval=args.heartbeat,
    )
    node.start()
    log.info(
        "[%s] client port %s:%d, peer port %s:%d, peers: %s, llm %s",
        args.node_id, args.host, args.client_port, args.host, args.peer_port,
        ", ".join(f"{p}={a.host}:{a.peer_port}" for p, a in peers.items()) or "(none)", args.llm_addr,
    )
    try:
        node.client_server.wait_for_termination()
    except KeyboardInterrupt:
        pass
    finally:
        node.stop()
        listener.stop()


if __name__ == "__main__":
    main()
