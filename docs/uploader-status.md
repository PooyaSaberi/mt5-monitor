# M5.3: local uploader visibility

Status is an offline observation of local files, not a probe of MT5 or the backend.
The durable retry deadline and `retry_in_seconds` are wall-clock estimates;
`retry_clock_uncertain` identifies an uncertain restart schedule and
`effective_monotonic_wait_seconds` is null because offline status cannot observe
another process's in-memory timer. `retry_remaining_basis` labels the estimate.
It creates no uploader state/ownership locks, performs no migrations, and imports
neither the HTTP transport/network stack nor MT5. No new dependency, UI, service,
alert or retention behavior is introduced.

```powershell
.\.venv\Scripts\python.exe .\uploader.py status --config C:\private\example.upload.local.json --format text
.\.venv\Scripts\python.exe .\uploader.py status --config C:\private\example.upload.local.json --format json
.\.venv\Scripts\python.exe .\uploader.py check --config C:\private\example.upload.local.json --format text
```

JSON remains the default. `--format text` is for `check/status` only. `status`
returns a report even when local data is unavailable; inspect its state/error.
`check` returns nonzero for paused, inconsistent, busy, invalid or pre-v4 data.
Neither command sends credentials, tests remote authentication or ingests data.
If configuration cannot even be parsed, JSON mode returns a compact sanitized
`state=unavailable` error object and exit code 2, without printing file contents
or treating validity as known.

## Three independent meanings

1. **Collection**: the latest saved account-observation end timestamp and age.
   `fresh` means a persisted account timestamp is at most 30 seconds old relative
   to this PC; `stale` means older. This threshold is a local display heuristic,
   not a guarantee about an active collector, terminal connectivity or EA health.
   The API permits a different threshold for controlled inspection/tests.
2. **Delivery**: actual unacknowledged event count, oldest unacknowledged collection
   start time and age, pending bytes, acknowledged cursor, retry time, pause and
   sanitized error. Missing, unreadable or inconsistent progress produces unknown
   backlog rather than assuming zero. Delivery can be paused while collection is
   fresh, or caught up while collection is stale.
3. **Receiver acknowledgement**: historical local acknowledgement-success time,
   separately from the receiver's own receipt timestamp and acknowledged sequence.
   `current_reachability` is always `not_checked`. A past acknowledgement is never
   evidence that the backend is currently reachable.

Source output contains only public fund/source/currency aliases and the stream ID.
Configuration validity means local structural validation, not valid remote tokens.
Binding validity reuses M5.2 source/stream/destination/file/content-anchor checks;
their restore and incomplete-audit limitations still apply. It is `null` when no
state exists or verification is unavailable. Known mismatches are `false`.
No login/server, terminal paths, token, financial snapshots, raw diagnostics or
arbitrary persisted error text are printed. Unknown errors become `unknown_error`.

## Time and clock limitations

`observed_at_utc`, collection and local acknowledgement times use the collector
PC's UTC clock, whose accuracy remains **unverified and uncorrected**. No Windows
timezone assumption or broker event mapping is introduced. Ages are signed
seconds against that PC clock. Future timestamps retain negative ages and
`time_state=future_timestamp`; collection freshness becomes `unknown`, never
silently zero/fresh. Missing, invalid and unavailable times are distinguished.

The latest account time is the maximum recorded account end timestamp. The oldest
unacknowledged time is the earliest recorded observation/status collection start
after the durable acknowledged sequence; it is not necessarily the first transport
entry. Clock steps, future values and historical backfill mean neither timestamp
order nor age proves historical commit order. Receiver receipt comparisons are
against another unverified clock; they do not verify clock synchronization.

New successful uploader acknowledgements store `last_acknowledged_at_utc` with
the progress update and pending deletion in the same state transaction. Existing
state remains readable without migration: if this field was never stored, local
acknowledgement time stays unknown. The saved receiver receipt is still shown
separately; it is not relabelled as local acknowledgement success or observation.

## Queries and byte accounting

Metrics read only source/stream metadata, timestamps and actual `COUNT(*)` after
the cursor. Sequence gaps do not inflate backlog. Count and oldest/latest queries
can visit timestamp/index rows; SQLite's cooperative one-second progress budget
limits each collector read transaction, with a 100 ms busy timeout. This is not
an O(1) backlog-count claim or a hard real-time guarantee. Budget exhaustion and
lock contention are reported, with unknown affected values. No indexes are added
and no schema changes occur merely to view status.

Existing bounded anchor verification may deserialize the last acknowledged event
and the single pending batch (at most 20 events). It does not serialize the whole
backlog to measure bytes. Exact pending bytes come from SQLite `length(payload)`;
the pending request is capped at 512 KiB and validated for consistency. Total
unsent serialized bytes remain `null` / `unknown`; they are never extrapolated.
State metadata reads are bounded too. Collector metrics and uploader state are
separate snapshots, not a distributed transaction; concurrent recording can add
events immediately after a report. No read transaction is held across HTTP.

Supported older collector schemas v1/v2/v3 can expose source and account freshness,
but status reports `requires_v4`, unknown backlog and no migration. Missing state
reports `not_initialized` and counts all v4 delivery entries. Busy state does not
cause status to pretend the source has never uploaded. Invalid state/configuration
and source/anchor mismatches are explicit. Operational files were not inspected
or upgraded during M5.3 implementation.

## Output examples

Fictional console report generated from temporary databases:

```text
Source: example-fund/example-source | stream: example-stream
Configuration: valid | binding: verified
Collection: stale | latest: 2026-01-01T00:00:01.000000Z | age: 59.0s (recorded)
Delivery: backoff | backlog: 2 events | oldest: 2026-01-01T00:00:00.000000Z | age: 60.0s (recorded)
Pending: 809 bytes | total unsent bytes: unknown | retry: 2026-01-01T00:01:15.000000Z | paused: False | error: http_503
Receiver: ack sequence unknown | local ack: unknown | receipt: unknown | reachability: not checked
Ages depend on the unverified PC clock; past acknowledgements do not prove current backend reachability.
```

JSON uses `status_version=2` and the separate `collection`, `delivery` and `receiver`
objects. Example excerpt from that report:

```json
{
  "status_version": 2,
  "configuration_valid": true,
  "binding_valid": true,
  "collection": {
    "at_utc": "2026-01-01T00:00:01.000000Z",
    "age_seconds": 59.0,
    "time_state": "recorded",
    "freshness": "stale"
  },
  "delivery": {
    "state": "backoff",
    "unacknowledged_events": 2,
    "pending_request": {"batch_id": "example-pending", "first_sequence": "1", "last_sequence": "2", "bytes": 809},
    "total_unsent_serialized_bytes": null,
    "paused": false,
    "last_error": "http_503"
  },
  "receiver": {
    "acknowledged_sequence": null,
    "receiver_receipt_at_utc": null,
    "current_reachability": "not_checked"
  }
}
```

Previous top-level `state/backlog/pending/acknowledged_sequence/error/next_retry/
attempts` fields remain compatibility aliases. `last_ack` is now an allowlisted
summary of receipt time and sequence, not the entire persisted acknowledgement.
Use the nested v2 objects for new consumers. The top-level state is local readiness;
delivery additionally distinguishes active backoff. No credential/body text is
returned through compatibility fields.

## Verification and remaining operations

Run `.venv\Scripts\python.exe -m unittest tests.test_upload_status -v`.
Tests use temporary files, controlled clocks and concurrent SQLite recording.
Byte hashes/file inventories check that status inspection does not change the
inspected files. A subprocess blocks socket/SSL/HTTP transport/MT5/dashboard imports
and still obtains JSON status. Pre-v4, missing state, contention, future timestamps,
sequence gaps, pending/retry/pause, acknowledgement history and atomic timestamp
rollback are covered. These are local tests, not live GA1/backend verification.
All 16 visibility tests passed. The bounded full suite passed 146 tests, and
141 recorder/uploader/status tests passed with dashboard imports blocked.

The M5.2 operational requirements remain: inspect/provision the actual backend,
authorize backed-up collector v4 upgrades, configure private per-source uploader
state/tokens and verify bounded delivery/recovery before rollout. History remains
disabled and event mapping unverified. Central monitoring, alerts, total backlog
bytes, disk-space alarms and a retention policy remain outside this milestone.
