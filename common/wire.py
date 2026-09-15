"""Wire-contract constants shared by client and server.

Part of the same contract as proto/auction.proto: the session token travels
in gRPC metadata under this key (see the design note at the top of that
file), not as a message field. Defined here, rather than in server/ or
client/, so the two sides read the same value instead of each hardcoding
their own copy and drifting apart.
"""

TOKEN_METADATA_KEY = "token"
