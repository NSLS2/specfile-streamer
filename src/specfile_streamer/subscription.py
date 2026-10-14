"""Open a live subscription to the data streams of a single Bluesky run.

This is the one place that knows *how* a per-scan subscription is made.

Currently (bluesky-tiled-plugins 2.0.12) ``subscribe_to_streams`` must be
given the *parent* container of the runs, so we subscribe to the parent and
filter to the single run of interest. ``start=0`` replays the run's creation
(and any rows already written) from Tiled's streaming cache, which closes the
race between the run being created and its webhook reaching us.

When bluesky-tiled-plugins grows a run-scoped entry point (e.g.
``subscribe_to_run(run_container, ...)``), only the body of
:func:`open_scan_subscription` needs to change.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from typing import TYPE_CHECKING, Protocol

from bluesky_tiled_plugins import (
    BlueskyStreamSubscription,
    BlueskyStreamUpdate,
    subscribe_to_streams,
)

if TYPE_CHECKING:
    from tiled.client.container import Container

StreamSelection = Mapping[str, str | Iterable[str] | None]
ScanSubscriptionOpener = Callable[
    ["Container", str, Callable[[BlueskyStreamUpdate], None], StreamSelection],
    "Disconnectable",
]


class Disconnectable(Protocol):
    """What the rest of the app needs from a subscription."""

    def disconnect(self) -> None: ...


def open_scan_subscription(
    parent: Container,
    run_uid: str,
    callback: Callable[[BlueskyStreamUpdate], None],
    streams: StreamSelection,
) -> BlueskyStreamSubscription:
    """Subscribe to ``streams`` of the run ``parent[run_uid]`` only.

    Returns a running subscription; call ``disconnect()`` to release it.
    """
    return subscribe_to_streams(
        parent,
        callback,
        streams=streams,
        run_filter=lambda update: update.key == run_uid,
        start=0,
    )
