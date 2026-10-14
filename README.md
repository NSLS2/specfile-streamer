# specfile-streamer

[![Actions Status][actions-badge]][actions-link]
[![Documentation Status][rtd-badge]][rtd-link]

[![PyPI version][pypi-version]][pypi-link]
[![Conda-Forge][conda-badge]][conda-link]
[![PyPI platforms][pypi-platforms]][pypi-link]

[![GitHub Discussion][github-discussions-badge]][github-discussions-link]

[![Coverage][coverage-badge]][coverage-link]

<!-- SPHINX-START -->

specfile-streamer writes Bluesky runs to
[SPEC](https://certif.com/content/spec/)-format text files while the runs are in
progress. That lets existing SPEC-file tools (PyMca, silx, spec2nexus, and
in-house scripts) follow live data at a beamline that now acquires with Bluesky
and stores data in [Tiled](https://blueskyproject.io/tiled).

It is a small FastAPI service that works like this:

1. Tiled sends it a **webhook** when a `BlueskyRun` is created. If the run's
   start document has a `specfile` key (configurable), e.g.
   `RE(plan, specfile="2026-3/sample1.spec")`, the run is tracked.
2. The service opens a **WebSocket subscription**, using
   `bluesky_tiled_plugins.subscribe_to_streams`, for that one run's `primary`
   and `baseline` streams. Data lines are appended to the file as events arrive.
3. When Tiled reports the run's stream closed (another webhook), any missed rows
   are backfilled over REST, the end-of-scan comment is written, and the
   subscription is released.

Runs that share a file are appended as successive `#S` blocks, as in SPEC.

```console
$ export SPECFILE_STREAMER_TILED_URI=https://tiled.example.com/api/v1/metadata/raw
$ export SPECFILE_STREAMER_TILED_API_KEY=...
$ export SPECFILE_STREAMER_BASE_DIRECTORY=/data/spec
$ export SPECFILE_STREAMER_WEBHOOK_SECRET=...
$ specfile-streamer --host 0.0.0.0 --port 8000
```

Then register `https://<this host>/webhook` as a webhook on Tiled. See the
documentation for details.

<!-- prettier-ignore-start -->
[actions-badge]:            https://github.com/NSLS2/specfile-streamer/actions/workflows/ci.yml/badge.svg
[actions-link]:             https://github.com/NSLS2/specfile-streamer/actions
[conda-badge]:              https://img.shields.io/conda/vn/conda-forge/specfile-streamer
[conda-link]:               https://github.com/conda-forge/specfile-streamer-feedstock
[github-discussions-badge]: https://img.shields.io/static/v1?label=Discussions&message=Ask&color=blue&logo=github
[github-discussions-link]:  https://github.com/NSLS2/specfile-streamer/discussions
[pypi-link]:                https://pypi.org/project/specfile-streamer/
[pypi-platforms]:           https://img.shields.io/pypi/pyversions/specfile-streamer
[pypi-version]:             https://img.shields.io/pypi/v/specfile-streamer
[rtd-badge]:                https://readthedocs.org/projects/specfile-streamer/badge/?version=latest
[rtd-link]:                 https://specfile-streamer.readthedocs.io/en/latest/?badge=latest
[coverage-badge]:           https://codecov.io/github/NSLS2/specfile-streamer/branch/main/graph/badge.svg
[coverage-link]:            https://codecov.io/github/NSLS2/specfile-streamer

<!-- prettier-ignore-end -->
