# gRPC — notes to myself

Written the day I got it. If future-me is confused again, the answer is
probably in §4.

---

## 1. The problem

Two programs, different processes. They cannot call each other's functions.
The only thing that crosses between them is **bytes**.

gRPC's whole job: **make a call to another process look like a normal
function call.**

```python
response = stub.Ping(request)   # looks local. isn't.
```

That's the product. Everything else is machinery serving that illusion.

---

## 2. The `.proto` — the promise

I write this. It's a contract both sides agree on. It does nothing by itself.

```protobuf
service PingService {
  rpc Ping(PingRequest) returns (PingResponse);
}

message PingRequest  { string message = 1; }
message PingResponse { string message = 1; string served_by = 2; }
```

- `service` = a group of functions you can call remotely
- `rpc Ping(X) returns (Y)` = "there's a function Ping, hand it an X, get a Y"
- `message` (keyword) = "define a box shape"
- `string message = 1` = a slot: type, my name for it, and its **box number**

**The whole language, basically:** `service` + `rpc` lines, `message` + typed
slots. Types: `string`, `int32`, `int64`, `bool`.

### The two `message`s are different things

```protobuf
message PingResponse {   ← KEYWORD. protobuf's word for "define a box"
  string message = 1;    ← A NAME I CHOSE. could be `text`, `reply`, `banana`
}
```

Like `class Dog:` then `name = "Rex"`. Unlucky collision, nothing more.

### Field numbers

**Names don't travel. Only numbers do.**

Both sides have the same form, so sending `1:hello` is enough — the receiver
knows box 1 means `message`. Smaller than JSON, and both sides pre-agreed.

Rename `served_by` → `hostname` and keep `= 2`: **nothing breaks.** The name
is local. The number is the identity.

**Numbers are scoped per message.** `PingRequest.message = 1` and
`PingResponse.message = 1` don't collide — two different forms, each with its
own box 1.

### Never reuse a number

Delete a field, and its number is **burned forever**. If box 2 used to be
`served_by` and I later make box 2 mean `role`, old bytes get read as the
wrong field. **No error. Silently wrong.**

Protection: `reserved 2;` — the compiler then refuses to let me reuse it.

**Why this matters here more than in a normal app:** the Raft log lives on
disk, forever. A log entry written in October gets replayed by code from
December. The "incompatible old version" I have to stay compatible with is
**my own past self**. Network bytes live 2ms; log bytes live forever.

*(Viva answer: reusing a field number causes silent misinterpretation of
persisted log entries on replay — not an error.)*

---

## 3. Codegen

```powershell
python scripts/gen_proto.py
```

Runs `protoc`, then patches the import (see §6). One command whenever a
`.proto` changes.

**Two output flags → two files:**

| Flag | File | Made from | Contains |
|---|---|---|---|
| `--python_out` | `ping_pb2.py` | `message` blocks | box classes |
| `--grpc_python_out` | `ping_pb2_grpc.py` | `service` block | Stub + Servicer |

Two files because **protobuf and gRPC are separate things.** Protobuf is just
the packing rules (usable with no networking at all). gRPC is the calling
framework layered on top.

`ping_pb2.py` looks like garbage — a giant byte blob. That blob *is* my
`.proto` compiled; protobuf reads it at import and builds the classes at
runtime. Never read it. Never edit it. Both files say `DO NOT EDIT`.

**`ping_pb2_grpc.py` imports `ping_pb2`** — the phone needs to know how to
pack the boxes. One direction only; boxes don't know about phones.

---

## 4. THE KEY BIT — Stub vs Servicer

One `service` block generates **two** classes, because a call has two ends.

### Stub = the impostor (client side)

**Complete and working out of the box. I just use it.**

```python
stub = ping_pb2_grpc.PingServiceStub(channel)
response = stub.Ping(request)
```

Inside, it stored: the address `/ping.PingService/Ping` (= package + service
+ method, straight from the proto), plus how to pack and unpack.

`stub.Ping(request)` → packs to bytes → sends → **blocks** → gets bytes back
→ unpacks → returns the object.

### Servicer = the worker (server side)

**Generated version is EMPTY. It just crashes.**

```python
# generated — the whole thing
class PingServiceServicer:
    def Ping(self, request, context):
        raise NotImplementedError('Method not implemented!')
```

**I inherit from it and write my own `Ping`, which replaces the crashing one.**

```python
# mine
class PingServicer(ping_pb2_grpc.PingServiceServicer):
    def Ping(self, request, context):
        return ping_pb2.PingResponse(message=f"pong: {request.message}", ...)
```

Exactly like:

```python
class Animal:
    def speak(self): raise NotImplementedError
class Dog(Animal):
    def speak(self): return "woof"
Dog().speak()   # "woof" — parent's version never runs
```

**THIS WAS THE PIECE I KEPT MISSING.** The generated Servicer gives me the
*shape*; I supply the *behaviour*.

### Why the asymmetry

| | Generated gives | I do |
|---|---|---|
| Client | a finished Stub | use it |
| Server | a Servicer with holes | inherit, fill holes |

"How to pack bytes and send them to an address" is the same for everyone — a
generator can write it completely. "What should happen when a Ping arrives"
is my decision. So it leaves a hole.

### Why generate an empty class at all

It's the contract, enforced. Forget `Ping`, or spell it `ping` (lowercase),
and the crashing version stays — caller gets `UNIMPLEMENTED: Method not
implemented!`. Loud error instead of silent nothing.

---

## 5. How the server finds my code

**The generated file never calls my code. I hand my code IN.** Push, not pull.

```python
ping_pb2_grpc.add_PingServiceServicer_to_server(PingServicer(node_id), server)
```

`PingServicer(node_id)` is an object made from *my* class. Inside, the
generated function does:

```python
rpc_method_handlers = {'Ping': servicer.Ping}   # servicer = what I passed in
```

`servicer` is a **parameter**. It's whatever I gave it. So `servicer.Ping` is
*my* Ping. The generated code never knows what class it got.

**Without this line: every call returns UNIMPLEMENTED.** This one line is the
entire connection between generated code and my code.

Nothing ever creates a bare `PingServiceServicer()`. It's dead the moment I
subclass it.

---

## 6. Both machines have the same generated files

They don't each generate them — **they're committed to git and cloned.**

```
my laptop: edit proto → gen_proto.py → commit BOTH → push
           ↓
        github
       ↙      ↘
  server      client        (identical files)
```

If each machine generated its own, version differences in `grpcio-tools`
could produce different output → two programs silently disagreeing. Commit
once, copy everywhere.

**Both machines get both halves. Each uses one:**
- Server has `ping_server.py`, hands its object into the wiring → Servicer half
- Client has no server file, never calls the wiring → Stub half only

The generated files are **shared and hollow**. The real work
(`server/ping_server.py`) is **not shared**. That's why the client can't just
call it — on a real deployment, that file isn't on its machine.

### The import trap

`protoc` emits `import ping_pb2` (bare) — assumes both files sit in a folder
that's on `sys.path`. I put them in `generated/`, so it breaks.
`scripts/gen_proto.py` rewrites it to `from generated import ping_pb2`
automatically after every run. Never hand-edited.

*(Cause: `package ping;` is a **protobuf** namespace, not a Python package.
Java/Go derive their namespaces from it; Python names modules after the
filename instead. protoc was built for the former.)*

---

## 7. Full pipeline

**Build time (once):**
```
write ping.proto
    ↓ python scripts/gen_proto.py
ping_pb2.py (boxes) + ping_pb2_grpc.py (Stub + empty Servicer)
    ↓ I subclass the Servicer
server/ping_server.py  ← the only real work in the system
```

**Run time (every call):**
```
SERVER STARTUP
  grpc.server(ThreadPoolExecutor(4))       make empty server
  add_PingServiceServicer_to_server(...)   hand my object in  ← THE LINK
  add_insecure_port("[::]:50051")          listen
  start()                                  returns immediately
  wait_for_termination()                   sit forever (why the terminal hangs)

CLIENT
  with grpc.insecure_channel("localhost:50051") as channel:
      stub = PingServiceStub(channel)
      request = PingRequest(message="hello")
      response = stub.Ping(request)        ← BLOCKS HERE
```

**What that one call actually does:**
```
CLIENT   SerializeToString(request)      object → bytes
         send to /ping.PingService/Ping
              ─── network ───
SERVER   bytes arrive on 50051
         read address, look up 'Ping' in the handler table
         FromString(bytes)               bytes → PingRequest
         pick a free worker thread
       ▶ run MY PingServicer.Ping()      ← the only real work
         SerializeToString(response)     object → bytes
              ─── network ───
CLIENT   FromString(bytes)               bytes → PingResponse
         unblock, return the object
```

Serialize/deserialize are **mirrored** between the two sides.

---

## 8. Odds and ends

- **`insecure`** = no TLS. Fine on localhost. Not fine over an untrusted
  network.
- **`unary_unary`** = one request, one response. (Streaming variants exist;
  I won't need them.)
- **`context`** = gRPC metadata (deadlines, status codes). Ignore for now.
- **`with grpc.insecure_channel(...)`** = a channel is a standing HTTP/2
  connection with background threads. `with` guarantees it closes even if the
  call raises. Same reason as `with open(...)`.
- **`max_workers=4`** = 4 concurrent calls; a 5th queues. **This is where
  concurrency enters the system** — same shape of problem `apply()`'s lock
  solves.
- **Windows:** PowerShell doesn't do `\` line continuations. Single-line
  commands.
- `protoc` won't create the output dir. `mkdir generated` first.
- Silence from `protoc` = success.

---

## 9. One-paragraph version

I write a `.proto` describing the contract. A generator turns it into two
hollow files: boxes, and a Stub + an empty Servicer. On the client I *use*
the Stub — it's finished, it fakes a local call by packing bytes and sending
them. On the server I *inherit* the Servicer and write the real method,
then hand my object into `add_..._to_server` so incoming calls route to it.
Both machines have identical generated files; only the server has the file
with the actual work. **The generated Servicer gives me the shape; I supply
the behaviour.**
