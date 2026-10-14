"""End-to-end: RunEngine -> TiledWriter -> Tiled (webhooks + websockets) -> SPEC file."""

from __future__ import annotations

import threading
import time

import bluesky.plan_stubs as bps
import bluesky.preprocessors as bpp
import httpx
import pytest
import uvicorn
from bluesky import RunEngine
from bluesky.plans import count, scan
from bluesky.preprocessors import SupplementalData
from bluesky_tiled_plugins import TiledWriter
from ophyd.sim import det, motor, motor1, motor2
from tiled.client import from_uri
from tiled.server import SimpleTiledServer

from specfile_streamer.app import create_app
from specfile_streamer.config import Settings
from specfile_streamer.events import ChildCreated
from specfile_streamer.manager import ScanManager

SECRET = "e2e-secret"
TIMEOUT = 20


@pytest.fixture(scope="module")
def tiled_server():
    server = SimpleTiledServer(enable_webhooks=True)
    yield server
    server.close()


class _Server(uvicorn.Server):
    def install_signal_handlers(self) -> None:
        pass


@pytest.fixture
def run_streamer(tiled_server, tmp_path):
    """Start specfile-streamer and register it as a webhook on Tiled."""
    started = []

    def start(manager_factory=ScanManager):
        settings = Settings(
            tiled_uri=tiled_server.uri,
            base_directory=tmp_path,
            webhook_secret=SECRET,
            finalize_timeout=5,
        )
        app = create_app(settings, manager_factory=manager_factory)
        server = _Server(
            uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning")
        )
        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()
        deadline = time.monotonic() + TIMEOUT
        while not server.started:
            assert time.monotonic() < deadline, "streamer did not start"
            time.sleep(0.05)
        port = server.servers[0].sockets[0].getsockname()[1]

        base = tiled_server.uri.split("/api/v1")[0]
        response = httpx.post(
            f"{base}/api/v1/webhooks/target/",
            json={"url": f"http://127.0.0.1:{port}/webhook", "secret": SECRET},
            headers={"Authorization": f"Apikey {tiled_server.api_key}"},
        )
        response.raise_for_status()
        started.append((server, thread, base, response.json()["id"]))
        return app

    yield start

    for server, thread, base, webhook_id in started:
        httpx.delete(
            f"{base}/api/v1/webhooks/{webhook_id}",
            headers={"Authorization": f"Apikey {tiled_server.api_key}"},
        )
        server.should_exit = True
        thread.join(TIMEOUT)


@pytest.fixture
def RE(tiled_server):
    client = from_uri(tiled_server.uri)
    RE = RunEngine({})
    RE.subscribe(TiledWriter(client, batch_size=1))
    RE.preprocessors.append(SupplementalData(baseline=[motor1, motor2]))
    yield RE
    client.context.close()


def wait_for(path, text, count=1):
    deadline = time.monotonic() + TIMEOUT
    while time.monotonic() < deadline:
        if path.exists() and path.read_text().count(text) >= count:
            return path.read_text()
        time.sleep(0.1)
    content = path.read_text() if path.exists() else "<no file>"
    pytest.fail(f"Timed out waiting for {text!r} x{count} in {path}:\n{content}")


def data_lines(text):
    return [line for line in text.splitlines() if line and not line.startswith("#")]


def test_scan(run_streamer, RE, tmp_path):
    run_streamer()
    motor1.set(1.5).wait()
    RE(scan([det], motor, -1, 1, 5), specfile="e2e/scan.spec", count_time=0.1)

    text = wait_for(tmp_path / "e2e/scan.spec", "Scan ended")
    lines = text.splitlines()
    assert lines[0] == "#F scan.spec"
    (o_line,) = [line for line in lines if line.startswith("#O0 ")]
    (p_line,) = [line for line in lines if line.startswith("#P0 ")]
    positions = dict(zip(o_line[4:].split("  "), p_line[4:].split(), strict=True))
    assert positions == {"motor1": "1.5", "motor2": "0"}
    assert "#S 1 scan motor -1 1 5" in lines
    assert "#T 0.1  (Seconds)" in lines
    assert "#L motor  Epoch  Seconds  det" in lines
    rows = [line.split() for line in data_lines(text)]
    assert [r[0] for r in rows] == ["-1", "-0.5", "0", "0.5", "1"]
    assert all(r[2] == "0.1" for r in rows)
    assert float(rows[2][3]) == pytest.approx(1.0)
    assert text.rstrip().endswith("Scan ended (exit_status=success)")


def test_two_scans_append_to_one_file(run_streamer, RE, tmp_path):
    run_streamer()
    RE(count([det], 3), specfile="shared.spec")
    RE(scan([det], motor, 0, 1, 2), specfile="shared.spec")

    text = wait_for(tmp_path / "shared.spec", "Scan ended", count=2)
    assert text.count("#F ") == 1
    assert text.count("#S ") == 2
    assert text.index("#S 1 count 3") < text.index("#S 2 scan motor 0 1 2")
    assert len(data_lines(text)) == 5


def test_lines_are_written_while_scan_runs(run_streamer, RE, tmp_path):
    run_streamer()
    path = tmp_path / "live.spec"
    seen_during_run = []

    @bpp.run_decorator(md={"specfile": "live.spec"})
    def plan():
        for _ in range(3):
            yield from bps.trigger_and_read([det, motor])
        # Wait (inside the run) for the streamer to catch up.
        deadline = time.monotonic() + TIMEOUT
        while time.monotonic() < deadline:
            if path.exists() and len(data_lines(path.read_text())) == 3:
                break
            yield from bps.sleep(0.1)
        seen_during_run.append(len(data_lines(path.read_text())))
        assert "Scan ended" not in path.read_text()

    RE(plan())
    assert seen_during_run == [3]
    wait_for(path, "Scan ended")


def test_late_webhook_is_backfilled(run_streamer, RE, tmp_path):
    """The run is over before we see its creation webhook."""
    gate = threading.Event()

    class SlowManager(ScanManager):
        def handle(self, event):
            if isinstance(event, ChildCreated):
                gate.wait(TIMEOUT)
            super().handle(event)

    run_streamer(manager_factory=SlowManager)
    RE(scan([det], motor, -1, 1, 4), specfile="late.spec")
    gate.set()

    text = wait_for(tmp_path / "late.spec", "Scan ended")
    assert len(data_lines(text)) == 4
    assert "exit_status=success" in text


def test_run_without_key_is_ignored(run_streamer, RE, tmp_path):
    app = run_streamer()
    RE(count([det], 2))
    RE(count([det], 1), specfile="marker.spec")
    wait_for(tmp_path / "marker.spec", "Scan ended")
    assert [p.name for p in tmp_path.rglob("*") if p.is_file()] == ["marker.spec"]
    deadline = time.monotonic() + TIMEOUT
    while app.state.manager.active_scans() and time.monotonic() < deadline:
        time.sleep(0.05)
    assert app.state.manager.active_scans() == []
