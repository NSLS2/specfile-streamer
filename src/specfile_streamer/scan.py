"""State and output for one Bluesky run being written as one SPEC scan."""

from __future__ import annotations

import logging
import math
import threading
import time
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Protocol

from .specfile import (
    column_labels,
    format_command,
    format_data_line,
    format_number,
    format_scan_end,
    format_scan_header,
    select_columns,
)

if TYPE_CHECKING:
    from bluesky_tiled_plugins import BlueskyStreamUpdate

    from .subscription import Disconnectable
    from .writer import SpecFile

logger = logging.getLogger(__name__)

Row = dict[str, Any]


class RunSource(Protocol):
    """On-demand (REST) access to a run, used to fill gaps in live data."""

    def descriptor(self, stream: str) -> Mapping[str, Any] | None:
        """Return the stream's event-descriptor metadata, or None if absent."""
        ...

    def rows(self, stream: str) -> list[Row] | None:
        """Return all rows of the stream's scalar table, or None if absent."""
        ...

    def stop(self) -> Mapping[str, Any] | None:
        """Return the RunStop document, or None if not yet written."""
        ...


def records(table: Any) -> list[Row]:
    """Convert a decoded table (pandas DataFrame) into a list of row dicts."""
    return list(table.to_dict("records"))


def positioner_names(descriptor: Mapping[str, Any] | None) -> list[str]:
    """Names for ``#O``: hinted baseline fields, else every scalar number."""
    if not descriptor:
        return []
    names: list[str] = []
    for hint in (descriptor.get("hints") or {}).values():
        if isinstance(hint, Mapping):
            names.extend(hint.get("fields") or [])
    if not names:
        data_keys = descriptor.get("data_keys") or {}
        names = [
            key
            for key, info in data_keys.items()
            if isinstance(info, Mapping)
            and info.get("dtype") in {"number", "integer"}
            and not info.get("shape")
            and not key.endswith("_setpoint")
        ]
    return list(dict.fromkeys(names))


class ScanSession:
    """Turn live updates for one run into one ``#S`` block of a SPEC file.

    Rows from the primary stream are written in ``seq_num`` order exactly
    once: out-of-order rows are buffered and duplicates (e.g. from replay)
    are dropped. :meth:`finalize` backfills anything missed over REST, writes
    the closing comment, and releases the subscription.

    Thread safety: :meth:`on_update` may be called concurrently from Tiled's
    subscription threads; :meth:`finalize` from any thread.
    """

    def __init__(
        self,
        *,
        run_path: tuple[str, ...],
        start: Mapping[str, Any],
        spec_file: SpecFile,
        source: RunSource,
        primary_stream: str = "primary",
        baseline_stream: str = "baseline",
        count_time_key: str | None = "count_time",
    ) -> None:
        self.run_path = run_path
        self.start = dict(start)
        self.spec_file = spec_file
        self.source = source
        self.primary_stream = primary_stream
        self.baseline_stream = baseline_stream
        self.count_time = _as_float(self.start.get(count_time_key or ""))
        start_time = _as_float(self.start.get("time"))
        self._start_time = time.time() if start_time is None else start_time

        self._lock = threading.RLock()
        self._finalize_lock = threading.Lock()
        self._closed = False
        self._finalized = False
        self._subscription: Disconnectable | None = None
        self._stop: Mapping[str, Any] | None = None
        self._baseline: Row | None = None
        self._pending: dict[int, Row] = {}
        self._labels: list[str] | None = None
        self.last_seq_written = 0

    @property
    def uid(self) -> str:
        return self.run_path[-1]

    @property
    def finalized(self) -> bool:
        return self._finalized

    def attach(self, subscription: Disconnectable) -> None:
        """Give the session ownership of its live subscription."""
        with self._lock:
            if not self._closed:
                self._subscription = subscription
                return
        # Already finalized: release immediately.
        subscription.disconnect()

    def set_stop(self, stop: Mapping[str, Any]) -> None:
        with self._lock:
            self._stop = dict(stop)

    # --- Live updates ----------------------------------------------------

    def on_update(self, update: BlueskyStreamUpdate) -> None:
        """Callback for :func:`~bluesky_tiled_plugins.subscribe_to_streams`."""
        try:
            # Use the raw Tiled update: BlueskyStreamUpdate.data() drops the
            # reserved 'seq_num' and 'time' columns, which we need.
            table = update.update.data()
            self.add_rows(update.stream_name, records(table))
        except Exception:
            # Tiled runs callbacks in an executor and would otherwise lose
            # the exception silently.
            logger.exception(
                "Failed to process %s update for run %s", update.stream_name, self.uid
            )

    def add_rows(self, stream: str, rows: Sequence[Row]) -> None:
        """Accept rows from ``stream``. Primary rows are written promptly."""
        with self._lock:
            if self._closed:
                return
            self._accept(stream, rows)
            self._flush(require_contiguous=True)

    def _accept(self, stream: str, rows: Sequence[Row]) -> None:
        if stream == self.baseline_stream:
            if self._baseline is None and rows:
                self._baseline = min(rows, key=_seq)
        elif stream == self.primary_stream:
            for row in rows:
                seq = _seq(row)
                if seq > self.last_seq_written:
                    self._pending.setdefault(seq, dict(row))

    def _flush(self, *, require_contiguous: bool) -> None:
        if not self._pending:
            return
        self._ensure_scan_header()
        assert self._labels is not None
        epoch = self.spec_file.epoch or 0
        lines = []
        if require_contiguous:
            while self.last_seq_written + 1 in self._pending:
                self.last_seq_written += 1
                row = self._pending.pop(self.last_seq_written)
                lines.append(self._line(row, epoch))
        else:
            for seq in sorted(self._pending):
                lines.append(self._line(self._pending[seq], epoch))
                self.last_seq_written = seq
            self._pending.clear()
        if lines:
            with self.spec_file.lock:
                self.spec_file.append("".join(lines))

    def _line(self, row: Row, epoch: float) -> str:
        assert self._labels is not None
        return format_data_line(
            row, self._labels, epoch=epoch, count_time=self.count_time
        )

    # --- Scan header -------------------------------------------------------

    def _ensure_scan_header(self) -> None:
        if self._labels is not None:
            return
        descriptor = self._fetch(
            lambda: self.source.descriptor(self.primary_stream), "descriptor"
        )
        motors, detectors = select_columns(
            self.start, descriptor or {}, self.primary_stream
        )
        labels = column_labels(
            motors, detectors, with_seconds=self.count_time is not None
        )

        baseline_descriptor = self._fetch(
            lambda: self.source.descriptor(self.baseline_stream), "baseline descriptor"
        )
        names = positioner_names(baseline_descriptor)
        if self._baseline is None and baseline_descriptor is not None:
            rows = self._fetch(
                lambda: self.source.rows(self.baseline_stream), "baseline"
            )
            if rows:
                self._baseline = min(rows, key=_seq)

        with self.spec_file.lock:
            self.spec_file.ensure_header(
                lambda: names,
                comments=["Written by specfile-streamer"],
                epoch=self._start_time,
            )
            comments = []
            extra = [n for n in names if n not in self.spec_file.positioners]
            if extra and self._baseline is not None:
                comments.append(
                    "Positioners not in #O: "
                    + "  ".join(
                        f"{n}={format_number(self._baseline.get(n))}" for n in extra
                    )
                )
            self.spec_file.append(
                format_scan_header(
                    scan_number=self.start.get("scan_id", "?"),
                    command=format_command(self.start),
                    timestamp=self._start_time,
                    labels=labels,
                    count_time=self.count_time,
                    positioners=self.spec_file.positioners,
                    positioner_values=self._baseline,
                    comments=comments,
                )
            )
        self._labels = labels

    def _fetch(self, func: Any, what: str) -> Any:
        try:
            return func()
        except Exception:
            logger.exception("Failed to read %s for run %s", what, self.uid)
            return None

    # --- Finalization ------------------------------------------------------

    def finalize(self) -> None:
        """Backfill, write the end-of-scan comment, release the subscription.

        Idempotent; safe to call from any thread.
        """
        with self._finalize_lock:
            if self._finalized:
                return
            with self._lock:
                self._closed = True
                primary = self._fetch(
                    lambda: self.source.rows(self.primary_stream), "primary rows"
                )
                if primary:
                    self._accept(self.primary_stream, primary)
                if self._pending:
                    missing = sorted(self._pending)
                    if missing[0] != self.last_seq_written + 1:
                        logger.warning(
                            "Run %s: rows %s..%s written out of sequence",
                            self.uid,
                            missing[0],
                            missing[-1],
                        )
                    self._flush(require_contiguous=False)
                # Always produce a #S block, even for a run with no data.
                self._ensure_scan_header()
                stop = self._stop or self._fetch(self.source.stop, "stop document")
                stop = stop or {}
                with self.spec_file.lock:
                    self.spec_file.append(
                        format_scan_end(
                            float(stop.get("time") or time.time()),
                            stop.get("exit_status"),
                            stop.get("reason") or None,
                        )
                    )
                subscription, self._subscription = self._subscription, None
            self._finalized = True
        # Disconnect outside the lock: it joins threads whose callbacks may be
        # waiting for the lock.
        if subscription is not None:
            _disconnect(subscription, self.uid)

    def abandon(self) -> None:
        """Release the subscription without writing anything more."""
        with self._lock:
            self._closed = True
            subscription, self._subscription = self._subscription, None
        if subscription is not None:
            _disconnect(subscription, self.uid)


def _disconnect(subscription: Disconnectable, uid: str) -> None:
    try:
        subscription.disconnect()
    except Exception:
        logger.exception("Failed to disconnect subscription for run %s", uid)


def _seq(row: Mapping[str, Any]) -> int:
    return int(row["seq_num"])


def _as_float(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return None if math.isnan(value) else float(value)
