# Distributed Auction System

A distributed online auction system built for CS G623 (Advanced Operating
Systems): Python, gRPC, and a from-scratch implementation of the Raft
consensus protocol. See [CLAUDE.md](CLAUDE.md) for the build order and design
constraints.

## Environment (PowerShell)

There is a `.venv` at the repo root. The bare `python` command on this
machine resolves to a miniforge3 install, **not** this venv, so it will not
have this project's dependencies (pytest, grpcio-tools, ...) on its path.
Always call the venv's interpreter explicitly, or activate the venv first.

Activate the venv for the current shell session:

```powershell
.venv\Scripts\Activate.ps1
```

Once activated, `python` and `pytest` resolve inside the venv for the rest of
that session. To confirm:

```powershell
Get-Command python
```

should print a path under `.venv\Scripts\`.

If you don't want to activate (e.g. a one-off command), call the venv
interpreter directly instead:

```powershell
.venv\Scripts\python.exe -m pytest
```

### Running tests

```powershell
.venv\Scripts\python.exe -m pytest
```

or, with the venv activated:

```powershell
pytest
```

Run a single test file:

```powershell
.venv\Scripts\python.exe -m pytest tests\test_close_auction.py -v
```

### Regenerating gRPC/protobuf code

Whenever a `.proto` file under `proto\` changes, regenerate the stubs in
`generated\`:

```powershell
.venv\Scripts\python.exe scripts\gen_proto.py
```
