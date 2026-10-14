"""Application settings, read from ``SPECFILE_STREAMER_*`` environment variables."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuration for specfile-streamer.

    Every field can be set with an environment variable named
    ``SPECFILE_STREAMER_<FIELD>``, e.g. ``SPECFILE_STREAMER_TILED_URI``.
    """

    model_config = SettingsConfigDict(env_prefix="SPECFILE_STREAMER_")

    tiled_uri: str = Field(
        description="URI of the Tiled server (or a node within it) to read from."
    )
    tiled_api_key: str | None = Field(
        default=None, description="API key used to authenticate with Tiled."
    )
    webhook_secret: str | None = Field(
        default=None,
        description=(
            "Shared secret given to Tiled when the webhook was registered. "
            "When set, every delivery must carry a valid X-Tiled-Signature."
        ),
    )
    metadata_key: str = Field(
        default="specfile",
        description=(
            "Key in the RunStart document giving the SPEC file path, relative "
            "to base_directory. Runs without this key are ignored."
        ),
    )
    base_directory: Path = Field(
        description="All SPEC files are written inside this directory."
    )
    primary_stream: str = Field(
        default="primary", description="Event stream written as SPEC data lines."
    )
    baseline_stream: str = Field(
        default="baseline", description="Event stream used for #O/#P positioners."
    )
    count_time_key: str | None = Field(
        default="count_time",
        description=(
            "Key in the RunStart document giving the per-point count time. "
            "When present, #T and a 'Seconds' column are written."
        ),
    )
    finalize_timeout: float = Field(
        default=30.0,
        description=(
            "Seconds to wait after a run's Stop document is observed for the "
            "run's stream-closed webhook before finalizing the scan anyway."
        ),
    )
