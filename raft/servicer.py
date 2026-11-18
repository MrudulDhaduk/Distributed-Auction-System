"""gRPC adapter for the Raft node: wire message in, node call, wire message
out. No Raft logic lives here -- every decision is in raft/node.py.
"""

import grpc

from generated import raft_pb2_grpc
from raft import wire
from raft.messages import TransportError
from raft.node import RaftNode


class RaftServicer(raft_pb2_grpc.RaftServiceServicer):
    def __init__(self, node: RaftNode):
        self._node = node

    def RequestVote(self, request, context):
        try:
            reply = self._node.handle_request_vote(wire.vote_args_from_wire(request))
        except TransportError as e:  # the link from the sender is cut: like a lost packet
            context.abort(grpc.StatusCode.UNAVAILABLE, str(e))
        return wire.vote_reply_to_wire(reply)

    def AppendEntries(self, request, context):
        try:
            reply = self._node.handle_append_entries(wire.append_args_from_wire(request))
        except TransportError as e:
            context.abort(grpc.StatusCode.UNAVAILABLE, str(e))
        return wire.append_reply_to_wire(reply)
