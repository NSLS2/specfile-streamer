# Initial build summary

## Completed tasks

- [x] git init + copier scientific-python/cookie, initial commit
- [x] Set up deps/env (tiled>=0.2.19, bluesky-tiled-plugins>=2.0.12) and verify
      import
- [x] Implement config, events, specfile formatter, writer
- [x] Implement subscription, ScanSession, ScanManager, FastAPI app
- [x] Unit tests
- [x] End-to-end test with SimpleTiledServer + RunEngine
- [x] Docs (README + Sphinx pages)
- [x] Lint (pre-commit) and commit

specfile-streamer works end to end: a Bluesky scan written to Tiled shows up as
lines in a SPEC file while the scan is still running. All 73 tests pass,
including 5 end-to-end tests against a real Tiled server, and pre-commit, mypy,
pylint and the docs build are clean.

## Commits

```
be4f7c8 Document deployment, configuration and the SPEC format
6adb705 Add end-to-end tests; fix root-client paths; tidy lint config
03f427c Implement webhook-driven SPEC file writer
0d5b950 Initial commit from scientific-python/cookie
```

The cookie settings were NSLS2, BSD-3, hatch + hatch-vcs and Sphinx. Git had no
email configured, so `user.email=dallan@bnl.gov` is set in this repo's config
only. The cookie also needed an author email, and the same address is used.

## How it works

- **Scan start:** Tiled sends a webhook when a run is created. If the run's
  start document has `specfile=...`, a scan begins.
- **During the scan:** it subscribes to that one run using option A
  (`subscribe_to_streams` filtered to the run, replaying from the start). Rows
  are written in `seq_num` order, each exactly once. To get `seq_num` and `time`
  it uses the `update.update.data()` workaround.
- **Scan end:** Tiled's `stream-closed` webhook for the run triggers
  finalization. Any rows the live stream missed are read back over REST, then
  the end-of-scan line is written and the subscription closed. If that webhook
  never arrives, a timeout after the Stop document does the same.
- **Switching to option B later** only means changing `open_scan_subscription`
  in `src/specfile_streamer/subscription.py`.
- **End-to-end tests cover:** lines appearing during a live scan, two scans in
  one file, a webhook that arrives only after the scan has finished, and runs
  without the key being ignored.

## Problems found while building it

- **Tiled 0.2.19 bug:** in the `container-child-metadata-updated` webhook,
  `metadata` holds the streaming-cache message, so the real metadata (including
  `stop`) sits one level deeper. This is worked around in `events.py`; it should
  be reported upstream.
- **Webhook order isn't guaranteed:** Tiled sends each webhook as a separate
  task, so they can arrive out of order. A `stream-closed` that arrives before
  its run's creation webhook is remembered and handled when the run appears.
- **Root path:** a Tiled client connected to the server root reports its path as
  `['']`. At first this made every webhook look like it came from outside our
  root, so nothing was written; that's fixed now.

## Changes from the original plan

- **Column order is SPEC's own:** motors, `Epoch`, [`Seconds`], then detectors,
  so the main detector is the last column, which readers like PyMca treat as the
  default y-axis. The plan said motors, detectors, then `Epoch`/`Seconds`.
- **`#T` and the `Seconds` column** appear only when the start document has
  `count_time`. The key is configurable.
- **`#E` is the run's start time** rather than the time the file is created, so
  `Epoch` values can't come out negative when rows are written after the fact.
- **A new run on the same file finalizes the previous scan first,** so scan
  blocks never interleave.
- **CI matrix:** Windows and PyPy were dropped, and Python is 3.10 and 3.13 on
  Linux and macOS, because the test server's dependencies (duckdb, pyarrow)
  aren't available on PyPy. The 3.14 classifier is still in `pyproject.toml`,
  but nothing tests it.
- **`uv.lock` is committed,** because the cookie's CI runs `uv sync`.

## Upstream follow-ups

References are to bluesky-tiled-plugins 2.0.12
(`src/bluesky_tiled_plugins/streaming.py`) and tiled 0.2.19.

### 1. bluesky-tiled-plugins: a run-scoped subscription ("option B")

**Problem.** `subscribe_to_streams(container, ...)` must be given the _parent_
of the runs. Its first-level handler, `_handle_container_child`, only follows
children with the `BlueskyRun` spec. If you pass it a run container, the
children it sees are event streams, which it skips, so nothing is ever
delivered. You can't subscribe to one scan directly.

**Current workaround (option A).** For each scan, specfile-streamer subscribes
to the parent container and filters down to the one run:

```python
subscribe_to_streams(
    parent,
    callback,
    streams={"primary": None, "baseline": None},
    run_filter=lambda update: update.key == run_uid,
    start=0,
)
```

This works, but each active scan:

- holds one extra WebSocket on the parent container for the whole scan;
- receives, and filters out, creation events for every other run under that
  parent;
- with `start=0`, first replays every child-created event still in the parent's
  streaming cache (by default, data is kept for an hour). It does this so that
  it can find its own run, which was created before the webhook arrived.

That conflicts with the intended design, where the webhook gives notice once per
scan and the WebSocket tracks only updates _within_ that scan.

**Proposed change.** Add a public entry point that starts one level down:

```python
def subscribe_to_run(
    run: Container,  # a BlueskyRun node
    callback: Callable[[BlueskyStreamUpdate], None],
    *,
    streams: Mapping[str, str | Iterable[str] | None] | None,
    start: int | None = 0,
    max_size: int = 1_000_000,
) -> BlueskyStreamSubscription: ...
```

The recursion already exists. Today the constructor of
`BlueskyStreamSubscription` starts at
`_subscribe_container(container, self._handle_container_child)`. A run-scoped
subscription would instead start at
`_subscribe_container(run, partial(self._handle_run_child, run_uid))`, reusing
`_handle_run_child`, `_handle_stream_child`, the teardown order and the
auto-release on `stream_closed` unchanged. Two reasonable ways to do it:

- A constructor flag or classmethod on `BlueskyStreamSubscription` choosing the
  root handler.
- Pulling root selection out into a small internal helper shared by
  `subscribe_to_streams` and `subscribe_to_run`.

It should check that `run` has the `BlueskyRun` spec and raise a clear error
otherwise. The tests in `tests/test_streaming.py` could be parametrized over
both entry points, plus one test that a run-scoped subscription ignores sibling
runs.

**Switch-over in specfile-streamer.** Only `open_scan_subscription` in
`src/specfile_streamer/subscription.py` changes. It would call
`subscribe_to_run(parent[run_uid], ...)`, or `ScanManager` could pass the run
node instead of the parent. The `Disconnectable` protocol and `ScanSession` are
unaffected. Then raise the dependency floor in `pyproject.toml`.

### 2. bluesky-tiled-plugins: let `BlueskyStreamUpdate.data()` keep reserved columns

**Problem.** `TiledWriter` stores each stream's scalars in a table
`<stream>/internal`. It has columns `seq_num` and `time`, the data keys, and
`ts_<key>` timestamps. When a stream is selected with `None` ("all keys"),
`_handle_stream_child` keeps only the columns that are also keys of the table's
metadata, i.e. the descriptor's `data_keys`. `BlueskyStreamUpdate.data()` then
returns `data.loc[:, list(self.data_keys)]`. `seq_num` and `time` are not data
keys, so they are dropped.

specfile-streamer needs both:

- `seq_num` to write each row once and in order. Rows arrive concurrently, and
  the replay with `start=0` overlaps with the REST backfill.
- `time` for the SPEC `Epoch` column.

**Current workaround.** `ScanSession.on_update` calls `update.update.data()`,
which decodes the raw Tiled `LiveTableData` with every column. This relies on
the documented `update` attribute, but not on `data()`.

There is another option today: pass an explicit selection such as
`streams={"primary": ("seq_num", "time", "det", ...)}`. Explicit keys are kept
if they are table columns. But that requires knowing the detector names before
the descriptor exists, which defeats `None`.

**Proposed change**, roughly in order of preference:

- `data(include_reserved=True)`, or a
  `subscribe_to_streams(..., include_reserved=True)` option, that also returns
  `seq_num` and `time`. The `ts_*` columns could optionally be included too.
- Or always include `seq_num` and `time` in table updates. They are what makes a
  batch interpretable, and including them is cheap.

The `RESERVED_DATA_KEYS = ["time", "seq_num"]` list in `TiledWriter` could be
shared so the two modules agree on the names.

**Switch-over in specfile-streamer.** Replace `update.update.data()` with the
new call in `ScanSession.on_update` (`src/specfile_streamer/scan.py`) and remove
the comment explaining the workaround.

### 3. tiled: the `container-child-metadata-updated` webhook nests its payload

**Problem.** In `tiled/catalog/adapter.py`,
`CatalogNodeAdapter.replace_metadata` reassigns the local `metadata` variable to
the streaming-cache message:

```python
metadata = {
    "type": "container-child-metadata-updated",
    "key": ...,
    "sequence": ...,
    "timestamp": ...,
    "specs": ...,
    "metadata": metadata,
}
```

The webhook dispatch further down then uses `metadata` as the node's "effective
metadata". So whenever a streaming cache is configured, the webhook payload's
`metadata` field is the cache message, and the node's real metadata (including
the Bluesky `stop` document) sits one level deeper, at `metadata["metadata"]`.
This was observed with tiled 0.2.19 and
`SimpleTiledServer(enable_webhooks=True)`.

A related consequence: when the call updates only specs (`metadata=None`), the
cache branch still turns `metadata` into a dict. The fallback to the previous
metadata (`elif not drop_revision: ...`) is never reached, and the webhook
reports `{"metadata": None, ...}` instead.

**Proposed fix.** Use a separate variable for the cache message, e.g.
`message = {...}`, and pass `message` to `streaming_cache.set`. Add a test that
a registered webhook receives the node's metadata unwrapped, both with and
without a streaming cache.

**Current workaround.** `ChildMetadataUpdated.effective_metadata()` in
`src/specfile_streamer/events.py` detects the wrapped shape (a `type` of
`container-child-metadata-updated`, a `sequence`, and a nested `metadata` dict)
and unwraps it. `tests/test_manager.py` covers both shapes. Once the fix is
released, the workaround can stay as harmless compatibility code or be removed
after the minimum tiled version is raised.

### 4. tiled (optional): ordering information on webhook deliveries

This is not a bug, but it is worth raising. `WebhookDispatcher.dispatch` starts
each delivery as its own asyncio task, with up to 32 running at once and retries
with backoff. Deliveries for one node can therefore arrive out of order. For
example, a run's `stream-closed` can overtake its `container-child-created`.
specfile-streamer copes by remembering closures for runs it has not seen yet,
and by finalizing on a timeout after the Stop document. A per-node sequence
number in the payload, like the one the streaming cache already assigns, would
let receivers order or deduplicate events without guessing. The
`X-Tiled-Event-ID` header already helps with deduplicating retried deliveries.
