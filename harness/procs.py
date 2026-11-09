"""Real node processes for the chaos harness, the stress runs and the demo.

    cluster = LocalCluster(run_dir, llm_port=50059)
    cluster.start()
    cluster.nodes["n1"].kill()      # kill -9: TerminateProcess, no cleanup
    cluster.nodes["n1"].start()     # same data dir: recovers term, vote, log
    cluster.nodes["n2"].suspend()   # a "slow node": frozen, sockets still open
    cluster.nodes["n2"].resume()
    cluster.stop()

Each node is `python -m server.serve` with its own --data-dir (FileStorage)
and its stdout/stderr appended to <run_dir>/logs/<id>.log, so a restarted
node's log continues the same file.

kill -9 on Windows: Popen.kill() is TerminateProcess(handle, 1). The process
gets no signal and runs no handler, no finally, no atexit, no flush: the
same as SIGKILL. Whatever FileStorage had fsync'd survives; nothing else.

Suspend: on Windows NtSuspendProcess (ntdll) freezes every thread of the
process; on POSIX, SIGSTOP. The node's sockets stay open, so peers' RPCs
are accepted by the kernel and then time out -- the node looks SLOW, not
dead. On resume it carries on with whatever it believed before (a leader
that was frozen still thinks it leads, until it hears a higher term).
"""

import os
import signal
import subprocess
import sys
from dataclasses import dataclass

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HOST = "127.0.0.1"

if os.name == "nt":
    import ctypes
    from ctypes import wintypes

    # A venv's python.exe on Windows is a LAUNCHER: it starts the base
    # interpreter as a child process and waits for it (checked on the dev
    # machine: .venv\Scripts\python.exe pid X, miniforge3\python.exe pid Y
    # with parent X). So "the node process" is two processes. Suspending
    # only the launcher freezes nothing. Every operation here therefore
    # acts on the whole tree: the pid and all its descendants.
    _PROCESS_TERMINATE = 0x0001
    _PROCESS_SUSPEND_RESUME = 0x0800
    _TH32CS_SNAPPROCESS = 0x2
    _kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _ntdll = ctypes.WinDLL("ntdll")
    _kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    _kernel32.OpenProcess.restype = wintypes.HANDLE
    _kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    _kernel32.TerminateProcess.argtypes = [wintypes.HANDLE, wintypes.UINT]
    _kernel32.CreateToolhelp32Snapshot.argtypes = [wintypes.DWORD, wintypes.DWORD]
    _kernel32.CreateToolhelp32Snapshot.restype = wintypes.HANDLE
    for _fn in (_ntdll.NtSuspendProcess, _ntdll.NtResumeProcess):
        _fn.argtypes = [wintypes.HANDLE]
        _fn.restype = ctypes.c_long  # NTSTATUS; 0 is success

    class _ProcessEntry(ctypes.Structure):
        _fields_ = [
            ("dwSize", wintypes.DWORD), ("cntUsage", wintypes.DWORD), ("th32ProcessID", wintypes.DWORD),
            ("th32DefaultHeapID", ctypes.c_size_t), ("th32ModuleID", wintypes.DWORD),
            ("cntThreads", wintypes.DWORD), ("th32ParentProcessID", wintypes.DWORD),
            ("pcPriClassBase", ctypes.c_long), ("dwFlags", wintypes.DWORD), ("szExeFile", ctypes.c_wchar * 260),
        ]

    _kernel32.Process32FirstW.argtypes = [wintypes.HANDLE, ctypes.POINTER(_ProcessEntry)]
    _kernel32.Process32NextW.argtypes = [wintypes.HANDLE, ctypes.POINTER(_ProcessEntry)]

    def process_tree(pid: int) -> list:
        """[pid, its children, their children, ...] from a snapshot."""
        snap = _kernel32.CreateToolhelp32Snapshot(_TH32CS_SNAPPROCESS, 0)
        if snap in (None, wintypes.HANDLE(-1).value):
            raise OSError(ctypes.get_last_error(), "CreateToolhelp32Snapshot failed")
        children = {}
        try:
            entry = _ProcessEntry()
            entry.dwSize = ctypes.sizeof(_ProcessEntry)
            ok = _kernel32.Process32FirstW(snap, ctypes.byref(entry))
            while ok:
                children.setdefault(entry.th32ParentProcessID, []).append(entry.th32ProcessID)
                ok = _kernel32.Process32NextW(snap, ctypes.byref(entry))
        finally:
            _kernel32.CloseHandle(snap)
        tree, todo = [], [pid]
        while todo:
            p = todo.pop()
            if p in tree:
                continue
            tree.append(p)
            todo.extend(children.get(p, []))
        return tree

    def _on_process(pid, access, fn):
        handle = _kernel32.OpenProcess(access, False, pid)
        if not handle:
            raise OSError(ctypes.get_last_error(), f"OpenProcess({pid}) failed")
        try:
            fn(handle)
        finally:
            _kernel32.CloseHandle(handle)

    def _nt(fn):
        def call(handle):
            status = fn(handle)
            if status != 0:
                raise OSError(f"{fn.__name__} returned NTSTATUS {status & 0xFFFFFFFF:#010x}")
        return call

    def suspend(pid: int):
        for p in process_tree(pid):
            _on_process(p, _PROCESS_SUSPEND_RESUME, _nt(_ntdll.NtSuspendProcess))

    def resume(pid: int):
        for p in reversed(process_tree(pid)):
            _on_process(p, _PROCESS_SUSPEND_RESUME, _nt(_ntdll.NtResumeProcess))

    def _terminate_descendants(pid: int):
        """TerminateProcess on the real interpreter(s) under a launcher,
        deepest first, so the kill hits the node itself, not only the
        launcher (whose job object would take the child down anyway)."""
        for p in reversed(process_tree(pid)[1:]):
            try:
                _on_process(p, _PROCESS_TERMINATE, lambda h: _kernel32.TerminateProcess(h, 1))
            except OSError:
                pass  # already gone

    _CREATION_FLAGS = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW
else:
    def suspend(pid: int):
        os.kill(pid, signal.SIGSTOP)

    def resume(pid: int):
        os.kill(pid, signal.SIGCONT)

    def process_tree(pid: int) -> list:
        return [pid]

    def _terminate_descendants(pid: int):
        pass

    _CREATION_FLAGS = 0


def hard_kill(p: subprocess.Popen, timeout: float = 10.0):
    """kill -9 (the whole tree, see above), then reap. Waiting matters: the
    restarted node must not open the same data dir while the old process
    still holds it."""
    if p.poll() is None:
        _terminate_descendants(p.pid)
        p.kill()
    p.wait(timeout=timeout)


def python_exe() -> str:
    venv = os.path.join(ROOT, ".venv", "Scripts" if os.name == "nt" else "bin",
                        "python.exe" if os.name == "nt" else "python")
    return venv if os.path.exists(venv) else sys.executable


@dataclass(frozen=True)
class NodeSpec:
    node_id: str
    peer_port: int
    client_port: int
    data_dir: str
    log_path: str


class _Proc:
    """One child process with an append-mode log file."""

    def __init__(self, name, argv, log_path):
        self.name = name
        self.argv = argv
        self.log_path = log_path
        self.popen = None
        self.suspended = False
        self.starts = 0

    @property
    def alive(self) -> bool:
        return self.popen is not None and self.popen.poll() is None

    @property
    def state(self) -> str:
        if not self.alive:
            return "dead"
        return "suspended" if self.suspended else "up"

    def start(self):
        if self.alive:
            raise RuntimeError(f"{self.name} is already running")
        os.makedirs(os.path.dirname(self.log_path), exist_ok=True)
        env = dict(os.environ, PYTHONUNBUFFERED="1")
        with open(self.log_path, "ab") as log:
            self.popen = subprocess.Popen(self.argv, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                                          stdin=subprocess.DEVNULL, env=env, creationflags=_CREATION_FLAGS)
        self.suspended = False
        self.starts += 1

    def kill(self):
        if self.popen is not None:
            hard_kill(self.popen)
        self.suspended = False

    def suspend(self):
        suspend(self.popen.pid)
        self.suspended = True

    def resume(self):
        resume(self.popen.pid)
        self.suspended = False


class NodeProc(_Proc):
    def __init__(self, spec: NodeSpec, peers: list, llm_addr: str, election=None, heartbeat=None):
        self.spec = spec
        argv = [
            python_exe(), "-m", "server.serve",
            "--node-id", spec.node_id,
            "--client-port", str(spec.client_port), "--peer-port", str(spec.peer_port),
            "--peers", ",".join(f"{p.node_id}={HOST}:{p.peer_port}:{p.client_port}" for p in peers),
            "--data-dir", spec.data_dir, "--llm-addr", llm_addr,
        ]
        if election is not None:
            argv += ["--election-timeout-min", str(election[0]), "--election-timeout-max", str(election[1])]
        if heartbeat is not None:
            argv += ["--heartbeat", str(heartbeat)]
        super().__init__(spec.node_id, argv, spec.log_path)


class LlmProc(_Proc):
    def __init__(self, port: int, log_path: str):
        self.port = port
        super().__init__("llm", [python_exe(), "-c", f"from llm_server.serve import serve; serve({port})"], log_path)


class LocalCluster:
    """Three node processes (and optionally the LLM server) under run_dir:
    run_dir/data/<id>, run_dir/logs/<id>.log, run_dir/logs/llm.log.

    Ports: peer base_peer..+2, client base_client..+2. Windows reserves
    50155-50254 on the dev machine (scripts/run_cluster.ps1), so keep the
    defaults clear of that. llm_port=None: no LLM process; nodes point at
    a port nobody listens on and descriptions degrade to "" (A10)."""

    def __init__(self, run_dir, *, ids=("n1", "n2", "n3"), base_peer=50081, base_client=50091,
                 llm_port=50059, election=None, heartbeat=None):
        self.run_dir = os.path.abspath(run_dir)
        self.specs = [
            NodeSpec(nid, base_peer + i, base_client + i,
                     os.path.join(self.run_dir, "data", nid), os.path.join(self.run_dir, "logs", f"{nid}.log"))
            for i, nid in enumerate(ids)
        ]
        llm_addr = f"{HOST}:{llm_port if llm_port else 1}"
        self.llm = LlmProc(llm_port, os.path.join(self.run_dir, "logs", "llm.log")) if llm_port else None
        self.nodes = {
            s.node_id: NodeProc(s, [p for p in self.specs if p is not s], llm_addr, election, heartbeat)
            for s in self.specs
        }

    def peer_addrs(self) -> dict:
        return {s.node_id: f"{HOST}:{s.peer_port}" for s in self.specs}

    def client_addrs(self) -> list:
        return [f"{HOST}:{s.client_port}" for s in self.specs]

    def log_paths(self) -> dict:
        return {s.node_id: s.log_path for s in self.specs}

    def start(self):
        if self.llm:
            self.llm.start()
        for node in self.nodes.values():
            node.start()

    def heal(self):
        """Resume what is suspended, restart what is dead."""
        for node in self.nodes.values():
            if node.state == "suspended":
                node.resume()
            elif node.state == "dead":
                node.start()

    def stop(self):
        for p in list(self.nodes.values()) + ([self.llm] if self.llm else []):
            try:
                if p.suspended and p.alive:
                    p.resume()
            except OSError:
                pass
            p.kill()
