from __future__ import annotations

import time
from datetime import datetime, timezone

import pytest

from specfile_streamer.config import Settings
from specfile_streamer.events import webhook_event_adapter
from specfile_streamer.manager import ScanManager

NOW = datetime.now(tz=timezone.utc).isoformat()
SCALAR = {"dtype": "number", "shape": []}


class Source:
    def __init__(self, client, path):
        self.path = path

    def descriptor(self, stream):
        if stream == "primary":
            return {"data_keys": {"det": SCALAR}, "hints": {"det": {"fields": ["det"]}}}
        return None

    def rows(self, stream):
        if stream == "primary":
            return [{"seq_num": 1, "time": 1.0, "det": 5}]
        return None

    def stop(self):
        return {"exit_status": "success", "time": 2.0}


class Subscription:
    def __init__(self):
        self.disconnected = False

    def disconnect(self):
        self.disconnected = True


class Client:
    path_parts = ("raw",)

    def __getitem__(self, key):
        return f"node:{key}"


class Opener:
    def __init__(self):
        self.calls = []
        self.subscriptions = []

    def __call__(self, parent, run_uid, callback, streams):
        self.calls.append((parent, run_uid, streams))
        sub = Subscription()
        self.subscriptions.append(sub)
        return sub


@pytest.fixture
def settings(tmp_path):
    return Settings(
        tiled_uri="http://unused", base_directory=tmp_path, finalize_timeout=0.2
    )


@pytest.fixture
def opener():
    return Opener()


@pytest.fixture
def manager(settings, opener):
    m = ScanManager(settings, Client(), open_subscription=opener, source_factory=Source)
    yield m
    m.shutdown()


def created(uid, start, path=("raw",), spec="BlueskyRun"):
    return webhook_event_adapter.validate_python(
        {
            "type": "container-child-created",
            "timestamp": NOW,
            "key": uid,
            "path": [*path, uid],
            "structure_family": "container",
            "specs": [{"name": spec, "version": "3.0"}],
            "metadata": {"start": {"uid": uid, "scan_id": 1, "time": 0.0, **start}},
        }
    )


def stop_update(uid, nested=True):
    md = {"start": {}, "stop": {"exit_status": "abort", "time": 3.0}}
    if nested:  # what Tiled 0.2.19 actually sends
        md = {
            "type": "container-child-metadata-updated",
            "key": uid,
            "sequence": 2,
            "metadata": md,
        }
    return webhook_event_adapter.validate_python(
        {
            "type": "container-child-metadata-updated",
            "timestamp": NOW,
            "key": uid,
            "path": ["raw", uid],
            "metadata": md,
        }
    )


def closed(uid, *extra):
    return webhook_event_adapter.validate_python(
        {
            "type": "stream-closed",
            "timestamp": NOW,
            "key": uid,
            "path": ["raw", uid, *extra],
        }
    )


def test_run_lifecycle(manager, opener, tmp_path):
    manager.handle(created("u1", {"specfile": "a/x.spec"}))
    # The client is rooted at /raw (see Client.path_parts), so it is the parent.
    assert opener.calls == [(manager.client, "u1", {"primary": None, "baseline": None})]
    assert [s["uid"] for s in manager.active_scans()] == ["u1"]

    manager.handle(closed("u1", "primary", "internal"))  # not the run: ignored
    assert manager.active_scans()

    manager.handle(closed("u1"))
    assert manager.active_scans() == []
    assert opener.subscriptions[0].disconnected
    text = (tmp_path / "a/x.spec").read_text()
    assert "#S 1 unknown" in text
    assert "#L Epoch  det\n1 5\n" in text
    assert "exit_status=success" in text


@pytest.mark.parametrize(
    "event",
    [
        created("u1", {}),  # no metadata key
        created("u1", {"specfile": "../escape.spec"}),
        created("u1", {"specfile": "x.spec"}, spec="Other"),
        created("u1", {"specfile": "x.spec"}, path=("elsewhere",)),
    ],
)
def test_ignored_runs(manager, opener, event):
    manager.handle(event)
    assert opener.calls == []
    assert manager.active_scans() == []


def test_duplicate_created_is_ignored(manager, opener):
    manager.handle(created("u1", {"specfile": "x.spec"}))
    manager.handle(created("u1", {"specfile": "x.spec"}))
    assert len(opener.calls) == 1


@pytest.mark.parametrize("nested", [True, False])
def test_stop_then_timeout_finalizes(manager, tmp_path, nested):
    manager.handle(created("u1", {"specfile": "x.spec"}))
    manager.handle(stop_update("u1", nested=nested))
    deadline = time.monotonic() + 5
    while manager.active_scans() and time.monotonic() < deadline:
        time.sleep(0.05)
    assert manager.active_scans() == []
    assert "exit_status=abort" in (tmp_path / "x.spec").read_text()


def test_closed_before_created(manager, opener, tmp_path):
    manager.handle(closed("u1"))
    manager.handle(created("u1", {"specfile": "x.spec"}))
    assert opener.calls == []
    assert manager.active_scans() == []
    assert "exit_status=success" in (tmp_path / "x.spec").read_text()


def test_new_run_on_same_file_finalizes_previous(manager, opener, tmp_path):
    manager.handle(created("u1", {"specfile": "x.spec"}))
    manager.handle(created("u2", {"specfile": "x.spec", "scan_id": 2}))
    assert [s["uid"] for s in manager.active_scans()] == ["u2"]
    assert opener.subscriptions[0].disconnected
    text = (tmp_path / "x.spec").read_text()
    assert text.index("#S 1 ") < text.index("exit_status=success")


def test_subscription_failure_still_finalizes(settings, tmp_path):
    def failing_opener(*args):
        msg = "no websocket"
        raise OSError(msg)

    manager = ScanManager(
        settings, Client(), open_subscription=failing_opener, source_factory=Source
    )
    try:
        manager.handle(created("u1", {"specfile": "x.spec"}))
        manager.handle(closed("u1"))
    finally:
        manager.shutdown()
    # Data still arrives, via REST backfill.
    assert "#L Epoch  det\n1 5\n" in (tmp_path / "x.spec").read_text()


def test_shutdown_abandons(settings, opener):
    manager = ScanManager(
        settings, Client(), open_subscription=opener, source_factory=Source
    )
    manager.handle(created("u1", {"specfile": "x.spec"}))
    manager.shutdown()
    assert opener.subscriptions[0].disconnected
    assert manager.active_scans() == []
