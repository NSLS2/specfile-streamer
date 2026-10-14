from __future__ import annotations

import math

import pytest

from specfile_streamer.specfile import (
    column_labels,
    format_command,
    format_data_line,
    format_file_header,
    format_number,
    format_scan_end,
    format_scan_header,
    parse_file_header,
    select_columns,
)

SCALAR = {"dtype": "number", "shape": []}


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (1, "1"),
        (True, "1"),
        (2.0, "2"),
        (0.5, "0.5"),
        (1 / 3, "0.3333333333"),
        (float("nan"), "nan"),
        (None, "nan"),
        ("text", "nan"),
        (1e20, "1e+20"),
    ],
)
def test_format_number(value, expected):
    assert format_number(value) == expected


def test_file_header_round_trip():
    names = [f"m{i}" for i in range(10)]
    text = format_file_header("x.spec", 1700000000, names, comments=["hello"])
    lines = text.splitlines()
    assert lines[0] == "#F x.spec"
    assert lines[1] == "#E 1700000000"
    assert lines[2].startswith("#D ")
    assert lines[3] == "#C hello"
    assert lines[4] == "#O0 " + "  ".join(names[:8])
    assert lines[5] == "#O1 m8  m9"
    assert text.endswith("\n\n")
    assert parse_file_header(text + "#S 1 count\n#O0 bogus\n") == (1700000000, names)


def test_parse_file_header_without_header():
    assert parse_file_header("") == (None, [])


START = {
    "scan_id": 7,
    "plan_name": "scan",
    "motors": ["motor"],
    "plan_args": {
        "detectors": ["SynGauss(prefix='', name='det', read_attrs=['val'])"],
        "num": 5,
        "args": ["SynAxis(prefix='', name='motor', read_attrs=['readback'])", -1, 1],
        "per_step": "None",
    },
    "hints": {"dimensions": [[["motor"], "primary"]]},
}
DESCRIPTOR = {
    "data_keys": {
        "motor": SCALAR,
        "motor_setpoint": SCALAR,
        "det": SCALAR,
        "det2": {"dtype": "integer", "shape": []},
        "image": {"dtype": "array", "shape": [10, 10]},
        "label": {"dtype": "string", "shape": []},
    },
    "hints": {"motor": {"fields": ["motor"]}, "det": {"fields": ["det"]}},
}


def test_format_command():
    assert format_command(START) == "scan motor -1 1 5"
    assert format_command({}) == "unknown"
    assert format_command({"plan_name": "count", "plan_args": {"num": 3}}) == "count 3"


def test_select_columns_from_hints():
    assert select_columns(START, DESCRIPTOR) == (["motor"], ["det"])


def test_select_columns_motor_fallback_without_dimensions():
    start = {**START, "hints": {}}
    assert select_columns(start, DESCRIPTOR) == (["motor"], ["det"])


def test_select_columns_without_any_hints_uses_scalar_numbers():
    descriptor = {**DESCRIPTOR, "hints": {}}
    motors, detectors = select_columns({"motors": ["motor"]}, descriptor)
    assert motors == ["motor"]
    # Setpoint of the scanned motor, arrays and strings are dropped.
    assert detectors == ["det", "det2"]


def test_select_columns_ignores_other_streams():
    start = {"hints": {"dimensions": [[["time"], "primary"], [["x"], "other"]]}}
    descriptor = {"data_keys": {"x": SCALAR, "det": SCALAR}}
    assert select_columns(start, descriptor) == ([], ["x", "det"])


def test_column_labels():
    assert column_labels(["m"], ["d"], with_seconds=False) == ["m", "Epoch", "d"]
    assert column_labels(["m"], ["d"], with_seconds=True) == [
        "m",
        "Epoch",
        "Seconds",
        "d",
    ]


def test_scan_header():
    text = format_scan_header(
        scan_number=7,
        command="scan motor -1 1 5",
        timestamp=0,
        labels=["motor", "Epoch", "Seconds", "det"],
        count_time=0.1,
        positioners=["a", "b"],
        positioner_values={"a": 1.5},
        comments=["note"],
    ).splitlines()
    assert text[0] == "#S 7 scan motor -1 1 5"
    assert text[1].startswith("#D ")
    assert text[2] == "#T 0.1  (Seconds)"
    assert text[3] == "#P0 1.5 nan"
    assert text[4] == "#C note"
    assert text[5] == "#N 4"
    assert text[6] == "#L motor  Epoch  Seconds  det"


def test_scan_header_without_positioner_values():
    text = format_scan_header(
        scan_number=1,
        command="count",
        timestamp=0,
        labels=["Epoch", "det"],
        positioners=["a"],
        positioner_values=None,
    )
    assert "#P" not in text
    assert "#T" not in text


def test_data_line():
    line = format_data_line(
        {"time": 1000.25, "motor": -1.0, "det": 0.5},
        ["motor", "Epoch", "Seconds", "det"],
        epoch=1000,
        count_time=0.1,
    )
    assert line == "-1 0.25 0.1 0.5\n"


def test_data_line_missing_value():
    line = format_data_line({"time": 1.0}, ["Epoch", "det"], epoch=0)
    values = line.split()
    assert values[0] == "1"
    assert math.isnan(float(values[1]))


def test_scan_end():
    assert format_scan_end(0, "abort", "user\npressed  ctrl-c").endswith(
        "Scan ended (exit_status=abort): user pressed ctrl-c\n\n"
    )
    assert format_scan_end(0, "success").endswith("(exit_status=success)\n\n")
