"""Command-line entry point: ``specfile-streamer --host 0.0.0.0 --port 8000``."""

from __future__ import annotations

import argparse
import logging

import uvicorn

from .app import create_app


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="specfile-streamer",
        description=(
            "Receive Tiled webhooks and write Bluesky runs as SPEC files. "
            "Configure with SPECFILE_STREAMER_* environment variables."
        ),
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--log-level", default="info")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    uvicorn.run(create_app(), host=args.host, port=args.port, log_level=args.log_level)


if __name__ == "__main__":
    main()
