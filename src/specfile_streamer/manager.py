"""Route Tiled webhook events to per-scan sessions."""

from __future__ import annotations

import contextlib
import logging
import threading
from collections import OrderedDict
from concurrent.futures import Future, ThreadPoolExecutor
from typing import TYPE_CHECKING, Any

from .events import ChildCreated, ChildMetadataUpdated, StreamClosed
from .scan import Row, RunSource, ScanSession, records
from .subscription import open_scan_subscription
from .writer import SpecFileRegistry, UnsafePathError, resolve_spec_path

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from tiled.client.base import BaseClient

    from .config import Settings
    from .events import WebhookEvent
    from .subscription import ScanSubscriptionOpener

logger = logging.getLogger(__name__)

RUN_SPEC = "BlueskyRun"
_RECENTLY_CLOSED_MAX = 1024


class TiledRunSource:
    """REST access to one run, used for descriptors, backfill and Stop."""

    def __init__(self, client: BaseClient, run_path: tuple[str, ...]) -> None:
        self._client = client
        self._run_path = run_path

    def _node(self, *segments: str) -> Any:
        node: Any = self._client
        for segment in (*self._run_path, *segments):
            node = node[segment]
        # Bluesky clients wrap nodes; use the plain Tiled node underneath.
        return getattr(node, "base", node)

    def _stream(self, stream: str) -> Any | None:
        try:
            return self._node(stream)
        except KeyError:
            return None

    def descriptor(self, stream: str) -> Mapping[str, Any] | None:
        node = self._stream(stream)
        return None if node is None else dict(node.metadata)

    def rows(self, stream: str) -> list[Row] | None:
        node = self._stream(stream)
        if node is None or "internal" not in node:
            return None
        return records(node["internal"].read())

    def stop(self) -> Mapping[str, Any] | None:
        stop = self._node().metadata.get("stop")
        return dict(stop) if stop else None


class ScanManager:
    """Owns all active :class:`ScanSession` objects.

    Webhook events are processed one at a time, in arrival order, on a
    dedicated worker thread (see :meth:`submit`).
    """

    def __init__(
        self,
        settings: Settings,
        client: BaseClient,
        *,
        open_subscription: ScanSubscriptionOpener = open_scan_subscription,
        source_factory: Callable[[BaseClient, tuple[str, ...]], RunSource] = (
            TiledRunSource
        ),
    ) -> None:
        self.settings = settings
        self.client = client
        self._open_subscription = open_subscription
        self._source_factory = source_factory
        self._files = SpecFileRegistry()
        self._lock = threading.Lock()
        self._sessions: dict[tuple[str, ...], ScanSession] = {}
        self._timers: dict[tuple[str, ...], threading.Timer] = {}
        # Runs whose stream-closed webhook overtook their creation webhook.
        self._recently_closed: OrderedDict[tuple[str, ...], None] = OrderedDict()
        self._executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="specfile-streamer"
        )
        # Webhook paths are absolute on the server; our client may be rooted
        # at a sub-node.
        self._prefix = tuple(getattr(client, "path_parts", ()) or ())

    # --- Public API ----------------------------------------------------------

    def submit(self, event: WebhookEvent) -> Future[None]:
        """Queue an event for processing and return immediately."""
        return self._executor.submit(self._handle_logged, event)

    def handle(self, event: WebhookEvent) -> None:
        """Process one event synchronously."""
        path = self._relative(event.node_path)
        if path is None:
            return
        if isinstance(event, ChildCreated):
            self._on_created(event, path)
        elif isinstance(event, ChildMetadataUpdated):
            self._on_metadata_updated(event, path)
        elif isinstance(event, StreamClosed):
            self._on_stream_closed(path)

    def active_scans(self) -> list[dict[str, Any]]:
        with self._lock:
            sessions = list(self._sessions.values())
        return [
            {
                "uid": s.uid,
                "path": "/".join(s.run_path),
                "scan_id": s.start.get("scan_id"),
                "file": str(s.spec_file.path),
                "last_seq_written": s.last_seq_written,
            }
            for s in sessions
        ]

    def shutdown(self) -> None:
        """Stop processing and release every live subscription.

        Active scans are not finalized; their SPEC blocks are left open.
        """
        self._executor.shutdown(wait=True, cancel_futures=True)
        with self._lock:
            sessions = list(self._sessions.values())
            self._sessions.clear()
            timers = list(self._timers.values())
            self._timers.clear()
        for timer in timers:
            timer.cancel()
        for session in sessions:
            session.abandon()

    # --- Event handlers ------------------------------------------------------

    def _handle_logged(self, event: WebhookEvent) -> None:
        try:
            self.handle(event)
        except Exception:
            logger.exception(
                "Failed to handle webhook event %s %s", event.type, event.path
            )

    def _relative(self, path: tuple[str, ...]) -> tuple[str, ...] | None:
        n = len(self._prefix)
        if path[:n] != self._prefix or len(path) <= n:
            return None
        return path[n:]

    def _on_created(self, event: ChildCreated, path: tuple[str, ...]) -> None:
        if event.structure_family != "container" or not event.has_spec(RUN_SPEC):
            return
        start = event.metadata.get("start") or {}
        requested = start.get(self.settings.metadata_key)
        if requested is None:
            logger.debug(
                "Run %s has no %r; ignoring", event.key, self.settings.metadata_key
            )
            return
        try:
            spec_path = resolve_spec_path(self.settings.base_directory, requested)
        except UnsafePathError as error:
            logger.warning("Run %s: %s; ignoring", event.key, error)
            return

        with self._lock:
            if path in self._sessions:
                return  # duplicate delivery
            # Runs are sequential; a new run on the same file means the
            # previous one is over even if we missed its closing webhook.
            previous = [
                s for s in self._sessions.values() if s.spec_file.path == spec_path
            ]
            session = ScanSession(
                run_path=path,
                start=start,
                spec_file=self._files.get(spec_path),
                source=self._source_factory(self.client, path),
                primary_stream=self.settings.primary_stream,
                baseline_stream=self.settings.baseline_stream,
                count_time_key=self.settings.count_time_key,
            )
            self._sessions[path] = session
            closed_early = path in self._recently_closed
            self._recently_closed.pop(path, None)
        for old in previous:
            logger.info(
                "Finalizing run %s before starting run %s", old.uid, session.uid
            )
            self._finalize(old.run_path)

        logger.info(
            "Run %s (scan %s) -> %s", event.key, start.get("scan_id"), spec_path
        )
        if closed_early:
            self._finalize(path)
            return
        parent = self._parent(path)
        streams = {
            self.settings.primary_stream: None,
            self.settings.baseline_stream: None,
        }
        try:
            subscription = self._open_subscription(
                parent, session.uid, session.on_update, streams
            )
        except Exception:
            # Data will still be written on finalize, via REST backfill.
            logger.exception("Failed to subscribe to run %s", session.uid)
            return
        session.attach(subscription)

    def _on_metadata_updated(
        self, event: ChildMetadataUpdated, path: tuple[str, ...]
    ) -> None:
        with self._lock:
            session = self._sessions.get(path)
        if session is None:
            return
        stop = event.effective_metadata().get("stop")
        if not stop:
            return
        session.set_stop(stop)
        timer = threading.Timer(
            self.settings.finalize_timeout, self._finalize_on_timeout, args=(path,)
        )
        timer.daemon = True
        with self._lock:
            old = self._timers.pop(path, None)
            self._timers[path] = timer
        if old is not None:
            old.cancel()
        timer.start()

    def _on_stream_closed(self, path: tuple[str, ...]) -> None:
        with self._lock:
            known = path in self._sessions
            if not known:
                self._recently_closed[path] = None
                while len(self._recently_closed) > _RECENTLY_CLOSED_MAX:
                    self._recently_closed.popitem(last=False)
        if known:
            self._finalize(path)

    def _finalize_on_timeout(self, path: tuple[str, ...]) -> None:
        logger.warning(
            "No stream-closed webhook for run %s within %ss; finalizing",
            "/".join(path),
            self.settings.finalize_timeout,
        )
        with contextlib.suppress(RuntimeError):  # raised if shutting down
            self._executor.submit(self._finalize, path)

    def _finalize(self, path: tuple[str, ...]) -> None:
        with self._lock:
            session = self._sessions.get(path)
            timer = self._timers.pop(path, None)
        if timer is not None:
            timer.cancel()
        if session is None:
            return
        try:
            session.finalize()
        except Exception:
            logger.exception("Failed to finalize run %s", session.uid)
        else:
            logger.info(
                "Finished run %s (scan %s)", session.uid, session.start.get("scan_id")
            )
        finally:
            with self._lock:
                if self._sessions.get(path) is session:
                    del self._sessions[path]

    def _parent(self, path: tuple[str, ...]) -> Any:
        node: Any = self.client
        for segment in path[:-1]:
            node = node[segment]
        return node
