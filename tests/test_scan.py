from __future__ import annotations

import threading
from types import SimpleNamespace

import pandas as pd
import pytest

from specfile_streamer.scan import ScanSession
from specfile_streamer.writer import SpecFile

SCALAR = {"dtype": "number", "shape": []}
START = {
    "uid": "abc",
    "scan_id": 3,
    "time": 1000.0,
    "plan_name": "scan",
    "motors": ["motor"],
    "plan_args": {"args": ["motor", 0, 1], "num": 3},
    "hints": {"dimensions": [[["motor"], "primary"]]},
}
PRIMARY = {
    "data_keys": {"motor": SCALAR, "det": SCALAR},
    "hints": {"motor": {"fields": ["motor"]}, "det": {"fields": ["det"]}},
}
BASELINE = {
    "data_keys": {"th": SCALAR, "tth": SCALAR},
    "hints": {"th": {"fields": ["th"]}, "tth": {"fields": ["tth"]}},
}


def row(seq, **data):
    return {"seq_num": seq, "time": 1000.0 + seq, **data}


class FakeSource:
    def __init__(self, primary_rows=(), baseline_rows=(), stop=None, baseline=True):
        self.primary_rows = list(primary_rows)
        self.baseline_rows = list(baseline_rows)
        self._stop = stop
        self._baseline = baseline
        self.calls = []

    def descriptor(self, stream):
        self.calls.append(("descriptor", stream))
        if stream == "primary":
            return PRIMARY
        return BASELINE if self._baseline else None

    def rows(self, stream):
        self.calls.append(("rows", stream))
        if stream == "primary":
            return self.primary_rows
        return self.baseline_rows if self._baseline else None

    def stop(self):
        return self._stop


class FakeSubscription:
    def __init__(self):
        self.disconnected = 0

    def disconnect(self):
        self.disconnected += 1


@pytest.fixture
def spec_file(tmp_path):
    return SpecFile(tmp_path / "x.spec")


def make_session(spec_file, source, **kwargs):
    return ScanSession(
        run_path=("abc",), start={**START, **kwargs}, spec_file=spec_file, source=source
    )


def data_lines(path):
    return [
        line
        for line in path.read_text().splitlines()
        if line and not line.startswith("#")
    ]


def test_rows_written_in_order_once(spec_file):
    session = make_session(spec_file, FakeSource())
    session.add_rows("primary", [row(2, motor=0.5, det=2)])
    assert data_lines(spec_file.path) == []  # waiting for seq 1
    session.add_rows("primary", [row(1, motor=0, det=1)])
    session.add_rows("primary", [row(1, motor=0, det=1), row(2, motor=0.5, det=2)])
    assert session.last_seq_written == 2
    lines = data_lines(spec_file.path)
    assert lines == ["0 1 1", "0.5 2 2"]


def test_header_and_positioners(spec_file):
    session = make_session(spec_file, FakeSource())
    session.add_rows("baseline", [row(2, th=9, tth=9), row(1, th=1.5, tth=3)])
    session.add_rows("primary", [row(1, motor=0, det=1)])
    text = spec_file.path.read_text()
    assert "#O0 th  tth\n" in text
    assert "#S 3 scan motor 0 1 3\n" in text
    assert "#P0 1.5 3\n" in text  # first baseline reading, not the last
    assert "#N 3\n#L motor  Epoch  det\n" in text


def test_baseline_fetched_over_rest_when_not_yet_streamed(spec_file):
    source = FakeSource(baseline_rows=[row(1, th=4, tth=5)])
    session = make_session(spec_file, source)
    session.add_rows("primary", [row(1, motor=0, det=1)])
    assert "#P0 4 5\n" in spec_file.path.read_text()


def test_no_baseline_stream(spec_file):
    session = make_session(spec_file, FakeSource(baseline=False))
    session.add_rows("primary", [row(1, motor=0, det=1)])
    text = spec_file.path.read_text()
    assert "#O" not in text
    assert "#P" not in text


def test_count_time_adds_seconds_column(spec_file):
    session = make_session(spec_file, FakeSource(), count_time=0.5)
    session.add_rows("primary", [row(1, motor=0, det=1)])
    text = spec_file.path.read_text()
    assert "#T 0.5  (Seconds)\n" in text
    assert "#L motor  Epoch  Seconds  det\n" in text
    assert data_lines(spec_file.path) == ["0 1 0.5 1"]


def test_finalize_backfills_and_closes(spec_file):
    rows = [row(i, motor=i, det=10 * i) for i in (1, 2, 3)]
    source = FakeSource(primary_rows=rows, stop={"exit_status": "success", "time": 0})
    session = make_session(spec_file, source)
    subscription = FakeSubscription()
    session.attach(subscription)
    session.add_rows("primary", [rows[0]])
    session.finalize()
    session.finalize()  # idempotent
    assert subscription.disconnected == 1
    assert data_lines(spec_file.path) == ["1 1 10", "2 2 20", "3 3 30"]
    text = spec_file.path.read_text()
    assert text.endswith("Scan ended (exit_status=success)\n\n")
    # Late live updates are ignored.
    session.add_rows("primary", [row(4, motor=4, det=40)])
    assert len(data_lines(spec_file.path)) == 3


def test_finalize_uses_webhook_stop_over_rest(spec_file):
    source = FakeSource(stop={"exit_status": "success"})
    session = make_session(spec_file, source)
    session.set_stop({"exit_status": "abort", "reason": "ctrl-c"})
    session.finalize()
    assert "exit_status=abort): ctrl-c" in spec_file.path.read_text()


def test_finalize_without_data_writes_scan_block(spec_file):
    session = make_session(spec_file, FakeSource())
    session.finalize()
    text = spec_file.path.read_text()
    assert "#S 3 " in text
    assert "#L motor  Epoch  det" in text
    assert "exit_status=None" in text


def test_finalize_writes_gapped_rows(spec_file, caplog):
    source = FakeSource(primary_rows=[row(2, motor=2, det=2)])
    session = make_session(spec_file, source)
    session.finalize()
    assert data_lines(spec_file.path) == ["2 2 2"]
    assert "out of sequence" in caplog.text


def test_attach_after_finalize_disconnects(spec_file):
    session = make_session(spec_file, FakeSource())
    session.finalize()
    subscription = FakeSubscription()
    session.attach(subscription)
    assert subscription.disconnected == 1


def test_abandon(spec_file):
    session = make_session(spec_file, FakeSource())
    subscription = FakeSubscription()
    session.attach(subscription)
    session.abandon()
    session.add_rows("primary", [row(1, motor=0, det=1)])
    assert subscription.disconnected == 1
    assert not spec_file.path.exists()


def test_on_update_uses_raw_table_and_logs_errors(spec_file, caplog):
    session = make_session(spec_file, FakeSource())
    table = pd.DataFrame([row(1, motor=0.0, det=1.0)])
    update = SimpleNamespace(
        stream_name="primary", update=SimpleNamespace(data=lambda: table)
    )
    session.on_update(update)
    assert data_lines(spec_file.path) == ["0 1 1"]

    def boom():
        raise RuntimeError("bad payload")

    session.on_update(
        SimpleNamespace(stream_name="primary", update=SimpleNamespace(data=boom))
    )
    assert "bad payload" in caplog.text


def test_concurrent_updates(spec_file):
    session = make_session(spec_file, FakeSource())
    rows = [row(i, motor=i, det=i) for i in range(1, 201)]
    threads = [
        threading.Thread(target=session.add_rows, args=("primary", rows[i::4]))
        for i in range(4)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert data_lines(spec_file.path) == [f"{i} {i} {i}" for i in range(1, 201)]


def test_second_scan_appends_and_reports_unknown_positioners(spec_file):
    first = make_session(spec_file, FakeSource(baseline=False))
    first.add_rows("primary", [row(1, motor=0, det=1)])
    first.finalize()
    second = make_session(spec_file, FakeSource(), scan_id=4)
    second.add_rows("baseline", [row(1, th=1, tth=2)])
    second.add_rows("primary", [row(1, motor=0, det=1)])
    text = spec_file.path.read_text()
    assert text.count("#F ") == 1
    assert "#S 3 " in text
    assert "#S 4 " in text
    assert "#C Positioners not in #O: th=1  tth=2" in text
