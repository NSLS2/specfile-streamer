# SPEC file format

SPEC files have no formal schema. specfile-streamer writes the subset of SPEC
conventions that common readers (silx, PyMca, spec2nexus) understand.

## Example

```text
#F scan.spec
#E 1791996825
#D Wed Oct 14 12:53:45 2026
#C Written by specfile-streamer
#O0 motor2  motor1

#S 1 scan motor -1 1 5
#D Wed Oct 14 12:53:45 2026
#T 0.1  (Seconds)
#P0 0 1.5
#N 4
#L motor  Epoch  Seconds  det
-1 0.492878 0.1 0.6065306597
-0.5 0.613643 0.1 0.8824969026
0 0.681816 0.1 1
0.5 0.71342 0.1 0.8824969026
1 0.745465 0.1 0.6065306597
#C Wed Oct 14 12:53:45 2026.  Scan ended (exit_status=success)

```

## Mapping from Bluesky

The **file header** is written once, when the file is created. When later scans
are appended to an existing file, the header already in the file is reused.

| Line        | Source                                                                                                                                          |
| ----------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `#F`        | File name.                                                                                                                                      |
| `#E`        | Start time of the first run written to the file, as an integer Unix time. Every `Epoch` column in the file is relative to this value.           |
| `#D`        | `#E` as a date.                                                                                                                                 |
| `#O0`, `#O1`, … | Positioner names, 8 per line, separated by two spaces. They are the hinted fields of the first run's `baseline` stream; if nothing is hinted, every scalar numeric field except `*_setpoint`. |

Each run then adds one **scan block**:

| Line        | Source                                                                                                                                                       |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `#S`        | `scan_id`, then a SPEC-like command built from `plan_name` and `plan_args`. Device reprs are shortened to device names, and detectors are left out, e.g. `scan motor -1 1 5`. |
| `#D`        | Run start time.                                                                                                                                              |
| `#T`        | The start document's `count_time`. Written only if that key is present.                                                                                      |
| `#P0`, …    | The first `baseline` reading, in `#O` order. Positioners the run does not have are written as `nan`. Omitted if the run has no baseline.                     |
| `#C`        | Baseline positioners that are not in the file's `#O` list, as `name=value` pairs.                                                                            |
| `#N`, `#L`  | Column count and labels, separated by two spaces.                                                                                                            |
| data lines  | One per `primary` event, in `seq_num` order.                                                                                                                 |
| `#C … Scan ended` | Stop time, `exit_status`, and `reason` if there is one.                                                                                                |

## Columns

Columns follow SPEC's own order, so the last column is the principal detector:

1. **Scanned motors**: the start document's `hints.dimensions` for the
   `primary` stream, falling back to its `motors` list.
2. **`Epoch`**: event time minus the file's `#E`.
3. **`Seconds`**: the count time. Present only if it is known (see `#T`).
4. **Detectors**: the hinted fields of the primary stream's descriptor. If
   nothing is hinted, every scalar numeric field is used, except the setpoints
   of the scanned motors.

Only scalar numeric fields become columns. A value that is missing or not
numeric is written as `nan`.
