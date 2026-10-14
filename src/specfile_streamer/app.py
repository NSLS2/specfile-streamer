"""FastAPI application receiving Tiled webhooks."""

from __future__ import annotations

import hashlib
import hmac
import logging
import threading
from collections import OrderedDict
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

from fastapi import FastAPI, HTTPException, Request, status
from pydantic import ValidationError
from tiled.client import from_uri

from .config import Settings
from .events import webhook_event_adapter
from .manager import ScanManager

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

    from tiled.client.base import BaseClient

logger = logging.getLogger(__name__)

SIGNATURE_HEADER = "X-Tiled-Signature"
EVENT_ID_HEADER = "X-Tiled-Event-ID"


def verify_signature(body: bytes, secret: str, signature: str | None) -> bool:
    """Check Tiled's ``sha256=<hex>`` HMAC signature of the raw body."""
    if not signature:
        return False
    expected = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


class _SeenEvents:
    """Bounded memory of delivered event IDs, to drop retried deliveries."""

    def __init__(self, maxsize: int = 4096) -> None:
        self._maxsize = maxsize
        self._ids: OrderedDict[str, None] = OrderedDict()
        self._lock = threading.Lock()

    def check_and_add(self, event_id: str) -> bool:
        """Return True if ``event_id`` was already seen; record it."""
        with self._lock:
            if event_id in self._ids:
                return True
            self._ids[event_id] = None
            if len(self._ids) > self._maxsize:
                self._ids.popitem(last=False)
            return False


def _default_client(settings: Settings) -> BaseClient:
    return from_uri(settings.tiled_uri, api_key=settings.tiled_api_key)


def create_app(
    settings: Settings | None = None,
    *,
    client_factory: Callable[[Settings], BaseClient] = _default_client,
    manager_factory: Callable[[Settings, BaseClient], ScanManager] = ScanManager,
) -> FastAPI:
    """Build the application.

    Settings are read from the environment when not given. The Tiled client
    is created at startup, so the Tiled server must be reachable then.
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        resolved = settings or Settings()  # type: ignore[call-arg]
        client = client_factory(resolved)
        manager = manager_factory(resolved, client)
        app.state.settings = resolved
        app.state.manager = manager
        app.state.seen = _SeenEvents()
        try:
            yield
        finally:
            manager.shutdown()
            context = getattr(client, "context", None)
            if context is not None:
                context.close()

    app = FastAPI(title="specfile-streamer", lifespan=lifespan)

    @app.post("/webhook", status_code=status.HTTP_202_ACCEPTED)
    async def webhook(request: Request) -> dict[str, Any]:
        body = await request.body()
        resolved: Settings = request.app.state.settings
        if resolved.webhook_secret and not verify_signature(
            body, resolved.webhook_secret, request.headers.get(SIGNATURE_HEADER)
        ):
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid signature")

        event_id = request.headers.get(EVENT_ID_HEADER)
        if event_id and request.app.state.seen.check_and_add(event_id):
            return {"status": "duplicate"}

        try:
            event = webhook_event_adapter.validate_json(body)
        except ValidationError as error:
            # Unknown event types (from a newer Tiled) are not our concern,
            # but tell the sender so it shows up in the delivery history.
            raise HTTPException(422, error.errors(include_url=False)) from error

        request.app.state.manager.submit(event)
        return {"status": "accepted"}

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/scans")
    async def scans(request: Request) -> list[dict[str, Any]]:
        return request.app.state.manager.active_scans()  # type: ignore[no-any-return]

    return app
