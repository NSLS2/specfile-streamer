from __future__ import annotations

import importlib.metadata

import specfile_streamer as m


def test_version() -> None:
    assert importlib.metadata.version("specfile_streamer") == m.__version__
