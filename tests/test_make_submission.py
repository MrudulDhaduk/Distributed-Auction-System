"""scripts/make_submission.ps1: one zip with what an examiner needs, and
nothing private or bulky.

The script is run for real (powershell.exe, into a temp directory). What is
checked:
  - it prints the zip path and a file list, and the list IS the zip;
  - the code, README, docs, proto files, scripts and tests are inside;
  - nothing from the exclusion list is: .venv, data, __pycache__/.pyc, .git,
    papers, CLAUDE.md, CLAUDE.old.md, run output (runs/, logs/), dist/;
  - entries use forward slashes under one top-level folder, so the zip
    unpacks the same with Windows Explorer, PowerShell or Python.

Windows only (the script is PowerShell 5.1). It does not unzip and run the
suite from the copy: that is a manual step (README, "Submission")."""

import os
import re
import shutil
import subprocess
import sys
import zipfile

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPT = os.path.join(ROOT, "scripts", "make_submission.ps1")
TOP = "Distributed-Auction-System"

pytestmark = pytest.mark.skipif(
    sys.platform != "win32" or shutil.which("powershell") is None,
    reason="make_submission.ps1 is a Windows PowerShell script",
)

# Directories whose every file must be in the zip (minus caches).
PACKAGES = ("auction", "checker", "client", "common", "generated", "harness",
            "llm_server", "raft", "replication", "server", "sessions")
TREES = PACKAGES + ("proto", "scripts", "docs", "tests")
TOP_FILES = ("README.md", "requirements.txt", "pytest.ini", "LICENSE")

# Path segments that must never appear, and file names that must never appear.
FORBIDDEN_SEGMENTS = {".venv", "data", "__pycache__", ".git", "papers", "runs",
                      "logs", "dist", ".pytest_cache"}
FORBIDDEN_NAMES = {"CLAUDE.md", "CLAUDE.old.md"}


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    """Run the script once for the whole module: (zip path, printed list, stdout)."""
    out_dir = tmp_path_factory.mktemp("submission")
    proc = subprocess.run(
        ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", SCRIPT,
         "-OutDir", str(out_dir)],
        cwd=ROOT, capture_output=True, text=True, timeout=180,
    )
    assert proc.returncode == 0, f"exit {proc.returncode}\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    m = re.search(r"^zip: (.+\.zip)\s*$", proc.stdout, re.MULTILINE)
    assert m, f"no 'zip: <path>' line in:\n{proc.stdout}"
    zip_path = m.group(1).strip()
    listed = [line[2:].strip() for line in proc.stdout.splitlines() if line.startswith("  ")]
    return zip_path, listed, proc.stdout


def _names(zip_path):
    with zipfile.ZipFile(zip_path) as z:
        return [n for n in z.namelist() if not n.endswith("/")]


def test_the_zip_is_written_where_asked_and_is_a_valid_zip(built):
    zip_path, _, _ = built
    assert os.path.isfile(zip_path)
    with zipfile.ZipFile(zip_path) as z:
        assert z.testzip() is None  # every member's CRC checks out


def test_the_printed_file_list_is_exactly_the_zip_contents(built):
    zip_path, listed, _ = built
    assert listed, "no file list printed"
    assert sorted(listed) == sorted(_names(zip_path))


def test_entries_use_forward_slashes_under_one_top_folder(built):
    zip_path, _, _ = built
    for name in _names(zip_path):
        assert "\\" not in name, name
        assert name.startswith(TOP + "/"), name


def test_top_level_files_are_included(built):
    names = set(_names(built[0]))
    for f in TOP_FILES:
        assert f"{TOP}/{f}" in names, f


def test_every_source_file_in_the_code_docs_proto_scripts_and_tests_is_included(built):
    names = set(_names(built[0]))
    missing = []
    for tree in TREES:
        for dirpath, dirnames, filenames in os.walk(os.path.join(ROOT, tree)):
            dirnames[:] = [d for d in dirnames if d not in FORBIDDEN_SEGMENTS]
            for f in filenames:
                if f.endswith((".pyc", ".pyo")):
                    continue
                rel = os.path.relpath(os.path.join(dirpath, f), ROOT).replace(os.sep, "/")
                if f"{TOP}/{rel}" not in names:
                    missing.append(rel)
    assert not missing, missing


def test_key_files_are_there_by_name(built):
    """Spot checks, so a broken walk above can't pass vacuously."""
    names = set(_names(built[0]))
    for rel in ("raft/node.py", "raft/file_storage.py", "auction/state.py",
                "replication/state_machine.py", "checker/properties.py",
                "proto/raft.proto", "proto/commands.proto", "generated/raft_pb2.py",
                "docs/decisions.md", "docs/stress-results.md", "docs/viva-prep.md",
                "docs/demo-runsheet.md", "docs/reflection-facts.md",
                "scripts/make_submission.ps1", "scripts/chaos.py", "tests/conftest.py"):
        assert f"{TOP}/{rel}" in names, rel


def test_nothing_from_the_exclusion_list_is_included(built):
    bad = []
    for name in _names(built[0]):
        parts = name.split("/")[1:]  # drop the top folder
        if FORBIDDEN_SEGMENTS.intersection(parts[:-1]) or parts[-1] in FORBIDDEN_NAMES \
                or parts[-1].endswith((".pyc", ".pyo")):
            bad.append(name)
    assert not bad, bad


def test_personal_notes_folders_are_left_out(built):
    """MyNotes/, Learning/ and Documents/ are the author's own study notes,
    not part of the submission (logged as a choice; see README)."""
    for name in _names(built[0]):
        assert name.split("/")[1] not in ("MyNotes", "Learning", "Documents"), name
