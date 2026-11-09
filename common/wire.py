"""Wire-contract constants shared by client and server.

Part of the same contract as proto/auction.proto: the session token travels
in gRPC metadata under this key (see the design note at the top of that
file), not as a message field. Defined here, rather than in server/ or
client/, so the two sides read the same value instead of each hardcoding
their own copy and drifting apart.
"""

TOKEN_METADATA_KEY = "token"

# M2 (Raft): a node that is not the leader rejects with FAILED_PRECONDITION
# and names the leader in TRAILING metadata under these keys (auction.proto,
# design note 3). leader-addr is the leader's CLIENT address, host:port, the
# one a client can dial; leader-id is its Raft node id, for logs.
LEADER_ID_METADATA_KEY = "leader-id"
LEADER_ADDR_METADATA_KEY = "leader-addr"
