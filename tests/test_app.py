from __future__ import annotations

import hashlib
import hmac
import json

import pytest
from fastapi.testclient import TestClient

from specfile_streamer.app import create_app, verify_signature
from specfile_streamer.config import Settings

SECRET = "s3cret"
EVENT = {
    "type": "stream-closed",
    "timestamp": "2026-01-01T00:00:00Z",
    "key": "u1",
    "path": ["u1"],
}


class FakeManager:
    def __init__(self, settings, client):
        self.events = []
        self.shut_down = False

    def submit(self, event):
        self.events.append(event)

    def active_scans(self):
        return [{"uid": "u1"}]

    def shutdown(self):
        self.shut_down = True


def sign(body: bytes, secret: str = SECRET) -> str:
    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


@pytest.fixture
def make_client(tmp_path):
    def make(secret=SECRET):
        settings = Settings(
            tiled_uri="http://unused", base_directory=tmp_path, webhook_secret=secret
        )
        app = create_app(
            settings, client_factory=lambda s: None, manager_factory=FakeManager
        )
        return TestClient(app)

    return make


def post(client, payload, *, signature=None, event_id=None, secret=SECRET):
    body = json.dumps(payload).encode()
    headers = {"Content-Type": "application/json"}
    if signature is None and secret:
        signature = sign(body, secret)
    if signature:
        headers["X-Tiled-Signature"] = signature
    if event_id:
        headers["X-Tiled-Event-ID"] = event_id
    return client.post("/webhook", content=body, headers=headers)


def test_verify_signature():
    assert verify_signature(b"x", "k", sign(b"x", "k"))
    assert not verify_signature(b"x", "k", sign(b"y", "k"))
    assert not verify_signature(b"x", "k", None)


def test_accepts_signed_event(make_client):
    with make_client() as client:
        response = post(client, EVENT, event_id="e1")
        assert response.status_code == 202
        manager = client.app.state.manager
        assert [e.type for e in manager.events] == ["stream-closed"]
        assert client.get("/scans").json() == [{"uid": "u1"}]
        assert client.get("/health").json() == {"status": "ok"}
    assert manager.shut_down


def test_rejects_bad_signature(make_client):
    with make_client() as client:
        assert post(client, EVENT, signature="sha256=00").status_code == 401
        assert post(client, EVENT, signature="", secret=None).status_code == 401
        assert client.app.state.manager.events == []


def test_no_secret_configured(make_client):
    with make_client(secret=None) as client:
        assert post(client, EVENT, secret=None).status_code == 202


def test_duplicate_delivery_is_dropped(make_client):
    with make_client() as client:
        assert post(client, EVENT, event_id="e1").json() == {"status": "accepted"}
        assert post(client, EVENT, event_id="e1").json() == {"status": "duplicate"}
        assert post(client, EVENT, event_id="e2").json() == {"status": "accepted"}
        assert len(client.app.state.manager.events) == 2


def test_unknown_event_type(make_client):
    with make_client() as client:
        response = post(client, {**EVENT, "type": "something-new"})
        assert response.status_code == 422
