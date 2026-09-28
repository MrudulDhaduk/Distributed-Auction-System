# Distributed Auction System

A distributed online auction system built for CS G623 (Advanced Operating
Systems). This submission covers **Milestone 1**: gRPC services, session
authentication, the core auction logic (create, bid, close), and an LLM
integration that generates item descriptions from seller-supplied
attributes. Everything runs as a single auction-server process today --
**Raft-based replication across multiple nodes is Milestone 2** and is not part of this submission.

## Setup

These steps assume Windows with PowerShell. Commands elsewhere in this
README (venv activation, running servers/tests) also use PowerShell syntax.

### 1. Clone the repo

```powershell
git clone https://github.com/MrudulDhaduk/Distributed-Auction-System
cd Distributed-Auction-System
```

### 2. Create and activate a virtual environment

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

Confirm the venv is active -- `Get-Command python` should print a path
under `.venv\Scripts\`.

### 3. Install Python dependencies

```powershell
pip install -r requirements.txt
```

This installs `grpcio`, `grpcio-tools`, `pytest`, `rich`, and `ollama`.

### 4. Install Ollama and pull the model

The LLM server talks to a local [Ollama](https://ollama.com/download)
daemon. Download and run the Windows installer from that page, then confirm
it's on your PATH:

```powershell
ollama --version
```

Ollama installs itself as a background service on Windows, so it should
already be running after install. Pull the model this project uses:

```powershell
ollama pull qwen3:4b-instruct-2507-q4_K_M
```

This is a ~2.5GB download. See [docs/design-notes.md](docs/design-notes.md)
for why this specific model/quantisation was chosen over the (also
installed) hybrid `qwen3:4b`.

## Running it

The system is three separate processes. Open **three PowerShell terminals**
in the repo root, with the venv active in each (`.venv\Scripts\Activate.ps1`),
and start them **in this order**:

**Terminal 1 -- LLM server** (must be up before you create an auction with
attributes, or description generation will fail and the auction server will
degrade to an empty description):

```powershell
python -m llm_server.serve
```

You should see:

```
2026-... INFO llm server listening on port 50052, model qwen3:4b-instruct-2507-q4_K_M
```

**Terminal 2 -- auction server**:

```powershell
python -m server.serve
```

You should see:

```
auction server listening on port 50051
```

**Terminal 3 -- client demo**:

```powershell
python -m client.auction_client
```

## Demo walkthrough

`client/auction_client.py` runs one scripted end-to-end flow: it logs in as
three seeded users, creates an auction with seller attributes (so a
description gets generated), places two competing bids, waits for the
auction to close on its own, then reads back the final result. This is what
running it looks like and what each block of output means.

**1. Login.** Three of this project's seeded accounts log in (see
`server/auth_servicer.py`'s `_USERS`: `mrudul`/`nisarg`/`karan`/`tanmay`, all with
password `pass`) -- `nisarg` as the seller, `mrudul`, `tanmay`  and `karan` as
competing bidders:

```
login: True - your session is created
login: True - your session is created
login: True - your session is created
login: True - your session is created
```

Each call returns a session token used as gRPC metadata on every
subsequent call (see the design note at the top of `proto/auction.proto`).

**2. Create an auction, with attributes.** The client creates auction id
`1`, a "vintage clock", with seller attributes (`condition`, `brand`,
`era`). Because attributes are supplied, the auction server calls the LLM
server's `GenerateItemDescription` RPC *before* the auction is created, and
freezes the returned text onto the auction (see `proto/llm.proto`'s
"call first, replicate second" note). This is a real model call, so expect
a multi-second pause here -- `docs/design-notes.md` measures 3.8-7.4s
depending on whether the model is already warm:

```
create auction: result {
  success: true
  reason: "Auction created successfully by nisarg"
}
auction_id: 1
```

**3. Place a couple of bids.** `karana` bids 100, then `mrudul` outbids with
200:

```
place bid (karan, 150): result {
  success: true
  reason: "Bid of 150 added"
}
current_high_bid: 150
current_high_bidder: "karan"

place bid (mrudul, 200): result {
  success: true
  reason: "Bid of 200 added"
}
current_high_bid: 200
current_high_bidder: "mrudul"

place bid (tanmay, 200): result {
  reason: "Bid of 200 is not added because current highest bid is 200"
}
current_high_bid: 200
current_high_bidder: "mrudul"

```

Tanmay's bid is rejected because a bid has to be strictly higher than the
current high bid (`auction/state.py`'s `PlaceBid` branch), so a tie loses
to whoever got there first. The rejected bid is never added to the
auction's bid history, which is why it doesn't appear in the final result
below.

Note: the rejected response has no `success: false` line. That's not a
missing field -- protobuf's text format leaves out any field set to its
default value, and `false` is the default for a `bool`. The same thing
happens to `0` and `""` elsewhere in the output.


**4. Wait for it to close.** There is no client-facing "close now" call --
closing is a decision the cluster makes once, on its own schedule (see
`docs/design-notes.md`'s note on why this is a poll loop, not a timer). The
demo auction is scheduled to close 25 seconds after creation, and the
client just sleeps past that:

```
waiting for the auction to close (27s) ...
```

**5. Get the summary.** The client re-reads the auction. `closed` is now
`true`, `winner` is `karan`, and the generated `description` is attached --
this is the closed auction's full state, i.e. its summary:

```
final result: auction_id: 1
item: "vintage clock"
close_time: 1790509224629803
closed: true
current_high_bid: 150
current_high_bidder: "karan"
bids {
  bidder: "nisarg"
  amount: 100
  time: 1790509203897425
}
bids {
  bidder: "karan"
  amount: 150
  time: 1790509203898916
}
winner: "karan"
description: "A Seiko clock from the 1970s, in used condition with running order. ..."
```

`close_time` and `time` are epoch microseconds UTC (see the wire-format
note at the top of `proto/auction.proto`), not something the client
formats for you.

The LLM server also exposes an `AnswerBiddingQuestion` FAQ RPC and a
`SummariseAuctionResult` RPC (prose summaries of a closed auction's price
history); this demo doesn't call them since the auction server doesn't
invoke them on this path yet. You can exercise all three LLM RPCs directly
against a running LLM server with `scripts\llm_rpc_smoke.py`.

## Proto files

- **`proto/auction.proto`** -- `AuthService` (login/logout, session tokens
  carried in gRPC metadata, not message fields) and `AuctionService`
  (create/bid/get/list auctions). One `Result{success, reason}` message
  carries business outcomes (e.g. a rejected bid); gRPC status codes are
  reserved for calls that couldn't be processed at all.
- **`proto/llm.proto`** -- `LLMService`, called only by the auction server
  (never directly by a client): a bidding FAQ assistant, item description
  generation from seller attributes, and auction result summarisation.
  Generated text is always plain data -- it never enters the replicated
  log or `apply()`.

## Design notes

For the reasoning behind non-obvious decisions (why a poll loop closes
auctions instead of a timer, why the LLM model was switched away from the
hybrid `qwen3:4b`, timeout sizing, etc.), see
[docs/design-notes.md](docs/design-notes.md) rather than a copy of it here.

## Running the test suite

With the venv active:

```powershell
pytest
```

Or without activating:

```powershell
.venv\Scripts\python.exe -m pytest
```

Run a single test file:

```powershell
.venv\Scripts\python.exe -m pytest tests\test_close_auction.py -v
```

The test suite is fully offline -- it fakes the LLM client/Ollama client
and never starts a real gRPC server, so it does not need Ollama or the
servers above running.

### Regenerating gRPC/protobuf code

Whenever a `.proto` file under `proto\` changes, regenerate the stubs in
`generated\`:

```powershell
.venv\Scripts\python.exe scripts\gen_proto.py
```
