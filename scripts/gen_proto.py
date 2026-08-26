"""Regenerate gRPC/protobuf code from proto/*.proto into generated/.

Run this whenever a .proto file changes. See CLAUDE.md for why generated/
is committed rather than built on the fly.
"""
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROTO_DIR = ROOT / "proto"
OUT_DIR = ROOT / "generated"

# protoc's Python plugin emits bare cross-imports between _pb2 and
# _pb2_grpc files (e.g. `import ping_pb2 as ping__pb2`), assuming both
# land in a directory that is itself on sys.path. We nest them inside the
# `generated` package instead, so that import 404s at runtime. Rewrite it
# to an absolute import through the package.
BARE_IMPORT = re.compile(r"^import (\w+_pb2) as (\w+)$", re.MULTILINE)


def run_protoc():
    proto_files = sorted(str(p) for p in PROTO_DIR.glob("*.proto"))
    if not proto_files:
        raise SystemExit(f"no .proto files found in {PROTO_DIR}")

    OUT_DIR.mkdir(exist_ok=True)

    cmd = [
        sys.executable, "-m", "grpc_tools.protoc",
        f"-I{PROTO_DIR}",
        f"--python_out={OUT_DIR}",
        f"--grpc_python_out={OUT_DIR}",
        *proto_files,
    ]
    subprocess.run(cmd, check=True, cwd=ROOT)


def patch_imports():
    for grpc_file in OUT_DIR.glob("*_pb2_grpc.py"):
        text = grpc_file.read_text()
        patched = BARE_IMPORT.sub(r"from generated import \1 as \2", text)
        if patched != text:
            grpc_file.write_text(patched)
            print(f"patched import in {grpc_file.relative_to(ROOT)}")


if __name__ == "__main__":
    run_protoc()
    patch_imports()
    print("done")
