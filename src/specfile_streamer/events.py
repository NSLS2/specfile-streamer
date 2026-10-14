"""Models for the JSON payloads Tiled delivers to webhooks.

These mirror ``tiled.server.schemas.WebhookEvent`` but are defined here so
that this package depends only on the Tiled *client*.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, TypeAdapter


class _Event(BaseModel):
    timestamp: datetime
    key: str
    path: list[str]

    @property
    def node_path(self) -> tuple[str, ...]:
        """Full path of the node the event is about."""
        return tuple(self.path)


class ChildCreated(_Event):
    """A node was created. ``path`` includes the new node's key."""

    type: Literal["container-child-created"]
    structure_family: str
    specs: list[dict[str, Any]] = []
    metadata: dict[str, Any] = {}

    def has_spec(self, name: str) -> bool:
        return any(spec.get("name") == name for spec in self.specs)


class ChildMetadataUpdated(_Event):
    """A node's metadata was updated. ``path`` is the updated node's path."""

    type: Literal["container-child-metadata-updated"]
    specs: list[dict[str, Any]] = []
    metadata: dict[str, Any] = {}

    def effective_metadata(self) -> dict[str, Any]:
        """Return the node's metadata.

        Some Tiled versions (observed in 0.2.19) deliver the streaming-cache
        message here, with the actual metadata nested one level down. Unwrap
        that case so callers do not need to care.
        """
        md = self.metadata
        if (
            md.get("type") == "container-child-metadata-updated"
            and isinstance(md.get("metadata"), dict)
            and "sequence" in md
        ):
            return md["metadata"]  # type: ignore[no-any-return]
        return md


class StreamClosed(_Event):
    """A producer declared the end of writes to the node at ``path``."""

    type: Literal["stream-closed"]


WebhookEvent = Annotated[
    ChildCreated | ChildMetadataUpdated | StreamClosed,
    Field(discriminator="type"),
]

webhook_event_adapter: TypeAdapter[WebhookEvent] = TypeAdapter(WebhookEvent)
