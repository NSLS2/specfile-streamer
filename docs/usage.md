# Deployment

## Requirements on the Tiled server

- Tiled **0.2.19** or newer.
- A **streaming cache** (Redis), so that clients can subscribe over WebSockets:

  ```yaml
  streaming_cache:
    uri: redis://localhost:6379
  ```

- **Webhooks** enabled:

  ```yaml
  webhooks:
    secret_keys: ["${TILED_WEBHOOKS_SECRET_KEY}"] # openssl rand -hex 32
    # Only if specfile-streamer is reached over plain HTTP and/or on a
    # private network; by default Tiled requires HTTPS public addresses.
    # allow_http: true
    # allow_private_addresses: true
    # Or allow just this one host:
    # allow_delivery_hosts: ["specfile-streamer.internal"]
  ```

- Runs written by
  [`TiledWriter`](https://blueskyproject.io/bluesky-tiled-plugins/) from
  bluesky-tiled-plugins **2.0.12** or newer. It closes each run's streams when
  the run stops, which tells specfile-streamer the scan is complete. Use
  `TiledWriter(client, batch_size=1)` so each event is streamed as soon as it is
  recorded.

## Configuration

specfile-streamer reads its settings from environment variables.

| Variable                               | Default      | Meaning                                                                                                 |
| -------------------------------------- | ------------ | ------------------------------------------------------------------------------------------------------- |
| `SPECFILE_STREAMER_TILED_URI`          | (required)   | Tiled URI. It may point to a sub-node such as `.../api/v1/metadata/raw`, which should hold the runs.     |
| `SPECFILE_STREAMER_TILED_API_KEY`      | none         | API key used for reading from Tiled.                                                                    |
| `SPECFILE_STREAMER_BASE_DIRECTORY`     | (required)   | SPEC files are written only inside this directory.                                                      |
| `SPECFILE_STREAMER_WEBHOOK_SECRET`     | none         | The secret you give Tiled when you register the webhook. If set, unsigned deliveries are rejected.      |
| `SPECFILE_STREAMER_METADATA_KEY`       | `specfile`   | Start-document key that holds the file path. Runs without it are ignored.                               |
| `SPECFILE_STREAMER_PRIMARY_STREAM`     | `primary`    | Stream that is written as data lines.                                                                   |
| `SPECFILE_STREAMER_BASELINE_STREAM`    | `baseline`   | Stream used for the `#O`/`#P` positioner lines.                                                         |
| `SPECFILE_STREAMER_COUNT_TIME_KEY`     | `count_time` | Start-document key holding the count time. If present, `#T` and a `Seconds` column are written.         |
| `SPECFILE_STREAMER_FINALIZE_TIMEOUT`   | `30`         | Seconds to wait after a Stop document for the run's stream-closed webhook before finalizing anyway.     |

The `specfile` value is a path relative to the base directory, for example
`RE(plan, specfile=f"{cycle}/{proposal}/{sample}.spec")`. If the path would
resolve outside the base directory, including through symlinks, the run is
ignored and a warning is logged.

## Running

```console
$ specfile-streamer --host 0.0.0.0 --port 8000
```

The service has these endpoints:

- `POST /webhook` receives Tiled webhooks.
- `GET /health` is a liveness check.
- `GET /scans` lists the scans currently being written.

## Registering the webhook

Register the webhook once, as a Tiled admin, on the container that holds the
runs (or on `/`). The webhook must receive the `container-child-created`,
`container-child-metadata-updated`, and `stream-closed` events. Leaving
`events` out subscribes it to all of them.

```bash
curl -X POST "https://tiled.example.com/api/v1/webhooks/target/raw" \
  -H "Authorization: Apikey $TILED_ADMIN_API_KEY" \
  -H "Content-Type: application/json" \
  -d "{\"url\": \"https://specfile-streamer.example.com/webhook\",
       \"secret\": \"$SPECFILE_STREAMER_WEBHOOK_SECRET\"}"
```

Tiled records each delivery. To inspect the delivery history:

```console
$ curl -H "Authorization: Apikey $TILED_ADMIN_API_KEY" \
    "https://tiled.example.com/api/v1/webhooks/history/<webhook id>"
```

## How a scan is tracked

1. A `container-child-created` webhook for a node with the `BlueskyRun` spec
   arrives. If its start document has the metadata key, a scan session begins.
2. The session subscribes to the run's `primary` and `baseline` streams.
   Because the subscription starts from the beginning of Tiled's streaming
   cache, events written before the webhook arrived are replayed.
3. The `#S` header is written when the first primary row arrives. Rows are
   written in `seq_num` order, and each row is written once.
4. A `stream-closed` webhook for the run finalizes the scan:
   1. The full primary table is read over REST, and any rows that are missing
      are written.
   2. The `#C ... Scan ended` line is written.
   3. The subscription is closed.

   If the run's Stop document is seen but `stream-closed` never arrives, the
   same steps run after `FINALIZE_TIMEOUT`.

If a new run targets a file that another scan is still writing, the earlier scan
is finalized first. This keeps scan blocks from interleaving.

## Limitations

- Only scalar numeric data is written. Array data (images, spectra) and
  strings are skipped.
- Scans that are in progress when the service stops are left without their
  closing comment. They are not resumed on restart.
- If two runs write to the same file at the same time, their blocks interleave.
  The Bluesky RunEngine runs one plan at a time, so this does not happen in
  normal use.
