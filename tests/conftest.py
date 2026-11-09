"""Fixtures shared by the persistence tests (build order step 6)."""

import os
import shutil
import tempfile

import pytest

from raft.file_storage import FileStorage


@pytest.fixture
def data_dir():
    """A fresh temporary directory, deleted after the test. rmtree runs
    without ignore_errors on purpose: on Windows it fails if a file inside
    is still open, which would mean a test leaked a storage handle."""
    path = tempfile.mkdtemp(prefix="raft-test-")
    yield path
    shutil.rmtree(path)


@pytest.fixture
def open_storage(data_dir):
    """open_storage("n1") opens a FileStorage on <data_dir>/n1.

    Opening the same name twice WITHOUT closing the first is how the tests
    simulate a crash: the first instance is abandoned mid-life, exactly as
    a killed process abandons its handles, and the second one recovers
    from whatever is on disk. Every instance is closed at teardown (before
    data_dir is removed: this fixture depends on it, so it is torn down
    first)."""
    opened = []

    def open_(name="n1"):
        storage = FileStorage(os.path.join(data_dir, name))
        opened.append(storage)
        return storage

    yield open_
    for storage in opened:
        storage.close()
