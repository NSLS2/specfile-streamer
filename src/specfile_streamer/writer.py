"""Thread-safe, append-only access to SPEC files on disk."""

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from .specfile import format_file_header, parse_file_header

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from pathlib import Path


class UnsafePathError(ValueError):
    """The requested SPEC file path is outside the configured base directory."""


def resolve_spec_path(base_directory: Path, requested: str) -> Path:
    """Resolve ``requested`` against ``base_directory``.

    Relative paths are joined to ``base_directory``. Absolute paths are
    accepted only if they already lie inside it. Symlinks are resolved before
    checking, so a link cannot be used to escape.
    """
    if not isinstance(requested, str) or not requested.strip():
        msg = f"SPEC file path must be a non-empty string, not {requested!r}"
        raise UnsafePathError(msg)
    base = base_directory.resolve()
    candidate = (base / requested).resolve()
    if not candidate.is_relative_to(base) or candidate == base:
        msg = f"SPEC file path {requested!r} resolves outside {base}"
        raise UnsafePathError(msg)
    return candidate


@dataclass
class SpecFile:
    """One SPEC file, opened for appending.

    ``epoch`` and ``positioners`` come from the file header: either parsed
    from an existing file or written by :meth:`ensure_header`.
    """

    path: Path
    lock: threading.Lock = field(default_factory=threading.Lock)
    epoch: int | None = None
    positioners: list[str] = field(default_factory=list)
    _initialized: bool = False

    def ensure_header(
        self,
        positioners_factory: Callable[[], Sequence[str]],
        comments: Sequence[str] = (),
        epoch: float | None = None,
    ) -> None:
        """Write the file header if the file is new; otherwise read it.

        Must be called with :attr:`lock` held. ``positioners_factory`` is only
        called when a new header is actually written. ``epoch`` (default: now)
        is the ``#E`` value of a new file; data ``Epoch`` columns are relative
        to it, so it should not be later than the first data point.
        """
        exists = self.path.exists() and self.path.stat().st_size > 0
        if self._initialized and exists:
            return
        # Not yet initialized, or the file was removed/rotated underneath us.
        if exists:
            epoch, positioners = parse_file_header(
                self.path.read_text(encoding="utf-8", errors="replace")
            )
            self.epoch = epoch if epoch is not None else int(time.time())
            self.positioners = positioners
        else:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.epoch = int(time.time() if epoch is None else epoch)
            self.positioners = list(positioners_factory())
            self._append(
                format_file_header(
                    self.path.name, self.epoch, self.positioners, comments
                )
            )
        self._initialized = True

    def append(self, text: str) -> None:
        """Append text and push it to disk. Call with :attr:`lock` held."""
        self._append(text)

    def _append(self, text: str) -> None:
        with self.path.open("a", encoding="utf-8") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())


class SpecFileRegistry:
    """Hands out one shared :class:`SpecFile` per path."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._files: dict[Path, SpecFile] = {}

    def get(self, path: Path) -> SpecFile:
        with self._lock:
            spec_file = self._files.get(path)
            if spec_file is None:
                spec_file = self._files[path] = SpecFile(path)
            return spec_file
