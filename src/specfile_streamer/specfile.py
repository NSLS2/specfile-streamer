"""Pure functions that render SPEC-format text.

SPEC files have no formal schema. This module follows the conventions that
SPEC itself writes and that common readers (silx, PyMca, spec2nexus) accept:

File header (written once, when the file is created)::

    #F <file name>
    #E <epoch: integer Unix time; data 'Epoch' columns are relative to this>
    #D <date>
    #C <comment>
    #O0 <positioner names, 8 per line>

Scan block (one per Bluesky run)::

    #S <scan number> <command>
    #D <date>
    #T <count time>  (Seconds)              [only if count time is known]
    #P0 <positioner values, in #O order>
    #N <number of columns>
    #L <col1>  <col2>  ...                   (two spaces between labels)
    <data line>
    ...
    #C <date>.  Scan ended (exit_status=...)

Data columns are ordered as SPEC does: scanned motors, then ``Epoch`` (and
``Seconds`` when the count time is known), then detectors, so that the last
column is the principal detector.
"""

from __future__ import annotations

import math
import re
import time
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

POSITIONERS_PER_LINE = 8
EPOCH = "Epoch"
SECONDS = "Seconds"
NUMERIC_DTYPES = frozenset({"number", "integer", "boolean"})

# Matches the name in an ophyd device repr, e.g. "SynAxis(prefix='', name='motor', ...)"
_DEVICE_REPR = re.compile(r"""^\w+\(.*\bname=['"]([^'"]+)['"]""", re.DOTALL)


def spec_date(timestamp: float) -> str:
    """Format a Unix timestamp the way SPEC writes ``#D`` lines."""
    return time.strftime("%a %b %d %H:%M:%S %Y", time.localtime(timestamp))


def format_number(value: Any) -> str:
    """Render one data value. Non-numeric or missing values become ``nan``."""
    if isinstance(value, bool):
        return str(int(value))
    if isinstance(value, int):
        return str(value)
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "nan"
    if math.isnan(number):
        return "nan"
    if number.is_integer() and abs(number) < 1e15:
        return str(int(number))
    return f"{number:.10g}"


def _chunks(items: Sequence[str], size: int) -> Iterable[Sequence[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def format_file_header(
    file_name: str,
    epoch: int,
    positioners: Sequence[str],
    comments: Sequence[str] = (),
) -> str:
    """Render the file header block, including the trailing blank line."""
    lines = [f"#F {file_name}", f"#E {epoch}", f"#D {spec_date(epoch)}"]
    lines.extend(f"#C {comment}" for comment in comments)
    for i, chunk in enumerate(_chunks(positioners, POSITIONERS_PER_LINE)):
        lines.append(f"#O{i} " + "  ".join(chunk))
    return "\n".join(lines) + "\n\n"


def parse_file_header(text: str) -> tuple[int | None, list[str]]:
    """Read ``#E`` and ``#O`` from the header of an existing SPEC file.

    Only the file header (lines before the first ``#S``) is examined.
    """
    epoch: int | None = None
    positioners: list[str] = []
    for line in text.splitlines():
        if line.startswith("#S "):
            break
        if line.startswith("#E ") and epoch is None:
            try:
                epoch = int(float(line[3:].strip()))
            except ValueError:
                epoch = None
        elif re.match(r"^#O\d+ ", line):
            # Names are separated by two spaces (SPEC allows single spaces
            # inside a name).
            _, _, rest = line.partition(" ")
            positioners.extend(name for name in rest.strip().split("  ") if name)
    return epoch, positioners


def short_repr(value: Any) -> str:
    """Compact rendering of a plan argument for the ``#S`` line."""
    if isinstance(value, str):
        match = _DEVICE_REPR.match(value)
        return match.group(1) if match else value
    if isinstance(value, Mapping):
        return " ".join(short_repr(v) for v in value.values())
    if isinstance(value, (list, tuple)):
        return " ".join(short_repr(v) for v in value)
    return format_number(value) if isinstance(value, (int, float)) else str(value)


def format_command(start: Mapping[str, Any]) -> str:
    """Build a SPEC-like command string, e.g. ``scan motor -1 1 5``.

    Detectors are omitted, as in SPEC. Positional ``args`` come first, then
    the remaining scalar keyword arguments in order.
    """
    parts = [str(start.get("plan_name", "unknown"))]
    plan_args = start.get("plan_args") or {}
    if isinstance(plan_args, Mapping):
        if "args" in plan_args:
            parts.append(short_repr(plan_args["args"]))
        for key, value in plan_args.items():
            if key in {"args", "detectors", "per_step", "md"}:
                continue
            if value is None or value == "None":
                continue
            if isinstance(value, (int, float, str)):
                parts.append(short_repr(value))
    return " ".join(p for p in parts if p)


def _scalar_numeric(data_keys: Mapping[str, Any], key: str) -> bool:
    info = data_keys.get(key)
    if not isinstance(info, Mapping):
        return False
    return info.get("dtype") in NUMERIC_DTYPES and not info.get("shape")


def _hinted_fields(descriptor: Mapping[str, Any]) -> list[str]:
    fields: list[str] = []
    for hint in (descriptor.get("hints") or {}).values():
        if isinstance(hint, Mapping):
            fields.extend(hint.get("fields") or [])
    return fields


def select_columns(
    start: Mapping[str, Any],
    descriptor: Mapping[str, Any],
    stream_name: str = "primary",
) -> tuple[list[str], list[str]]:
    """Choose the (motor, detector) data keys to write as SPEC columns.

    Motors come from the RunStart ``hints.dimensions`` for ``stream_name``,
    falling back to the RunStart ``motors`` list. Detectors are the hinted
    fields of the event descriptor; when nothing is hinted, every scalar
    numeric data key is used. Only scalar numeric keys are ever selected.
    """
    data_keys = descriptor.get("data_keys") or {}
    hinted = _hinted_fields(descriptor)

    motors: list[str] = []
    dimensions = (start.get("hints") or {}).get("dimensions")
    if dimensions:
        for fields, stream in dimensions:
            if stream == stream_name:
                motors.extend(fields)
    else:
        object_hints = descriptor.get("hints") or {}
        for name in start.get("motors") or []:
            fields = (object_hints.get(name) or {}).get("fields")
            motors.extend(fields or [name])
    motors = [m for m in dict.fromkeys(motors) if _scalar_numeric(data_keys, m)]

    candidates = hinted or list(data_keys)
    detectors = [
        key
        for key in dict.fromkeys(candidates)
        if key not in motors and _scalar_numeric(data_keys, key)
    ]
    if not hinted:
        # Without hints, drop setpoints of scanned motors; they duplicate the
        # motor column.
        setpoints = {f"{m}_setpoint" for m in motors}
        detectors = [d for d in detectors if d not in setpoints]
    return motors, detectors


def column_labels(
    motors: Sequence[str], detectors: Sequence[str], *, with_seconds: bool
) -> list[str]:
    """Return the ``#L`` labels in SPEC order."""
    return [*motors, EPOCH, *([SECONDS] if with_seconds else []), *detectors]


def format_scan_header(
    *,
    scan_number: int | str,
    command: str,
    timestamp: float,
    labels: Sequence[str],
    count_time: float | None = None,
    positioners: Sequence[str] = (),
    positioner_values: Mapping[str, Any] | None = None,
    comments: Sequence[str] = (),
) -> str:
    """Render ``#S`` through ``#L`` for one scan.

    ``positioners`` is the file's ``#O`` order; ``#P`` values follow it.
    ``#P`` lines are omitted when ``positioner_values`` is None.
    """
    lines = [f"#S {scan_number} {command}".rstrip(), f"#D {spec_date(timestamp)}"]
    if count_time is not None:
        lines.append(f"#T {format_number(count_time)}  (Seconds)")
    if positioner_values is not None and positioners:
        values = [format_number(positioner_values.get(p)) for p in positioners]
        for i, chunk in enumerate(_chunks(values, POSITIONERS_PER_LINE)):
            lines.append(f"#P{i} " + " ".join(chunk))
    lines.extend(f"#C {comment}" for comment in comments)
    lines.append(f"#N {len(labels)}")
    lines.append("#L " + "  ".join(labels))
    return "\n".join(lines) + "\n"


def format_data_line(
    row: Mapping[str, Any],
    labels: Sequence[str],
    *,
    epoch: float,
    count_time: float | None = None,
) -> str:
    """Render one data line. ``row`` must contain ``time`` and data keys."""
    values = []
    for label in labels:
        if label == EPOCH:
            values.append(format_number(round(float(row["time"]) - epoch, 6)))
        elif label == SECONDS:
            values.append(format_number(count_time))
        else:
            values.append(format_number(row.get(label)))
    return " ".join(values) + "\n"


def format_scan_end(
    timestamp: float, exit_status: str | None, reason: str | None = None
) -> str:
    """Render the closing comment and the blank line separating scans."""
    text = f"#C {spec_date(timestamp)}.  Scan ended (exit_status={exit_status})"
    if reason:
        text += f": {' '.join(str(reason).split())}"
    return text + "\n\n"
