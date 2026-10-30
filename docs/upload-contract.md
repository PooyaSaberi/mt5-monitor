# M5.1: local delivery stream and contract v1

This milestone implements offline serialization/validation and a durable local
delivery index only. There is no HTTP client, uploader process, receiver,
acknowledgement storage or automatic delivery. History remains independently
disabled in operational configurations; broker event mapping remains unverified.
No operational database was upgraded during implementation.

## Storage and upgrade

Schema v4 adds `delivery_stream(singleton, stream_id)` and
`delivery_events(sequence, kind, record_id)`. The stream UUID is created once.
An explicit SQLite `INTEGER PRIMARY KEY AUTOINCREMENT` sequence orders entries;
it is not a gap-free count. Count pending entries using `COUNT(*) WHERE
sequence > ?`, never maximum sequence minus cursor.

`AFTER INSERT` triggers on observations and status events insert delivery entries
in the same transaction as the source data. `save_cycle` inserts the observation,
statuses and complete active batches in one transaction. Other connections cannot
observe that cycle's delivery entry before its batches commit. Any insert/trigger
failure rolls back the whole save. Delivery entries reference existing data;
payloads are not duplicated in the recorder database. The recorder treats these
observations/statuses/batches as immutable; manual edits are unsupported.

The existing Storage constructor upgrades v1/v2/v3 transactionally to v4,
retaining source binding, M1/M2 records and M3 history. Backfill inserts one entry
per observation/status, sorted by stored `ended_at` text, kind, and record ID.
This is a deterministic transport order, **not historical commit order** or proof
of clock accuracy. Reopening v4 preserves stream ID, sequence and entries.
Unsupported newer versions are rejected. Migration DDL/backfill/triggers and
schema-version changes share one transaction and roll back together.

Before any later operational upgrade, stop the recorder, take a consistent SQLite
API backup and verify its integrity. **The next recorder start upgrades to v4 even
with history disabled.** Keep the backup and affected database on a failure.
M5.1 does not authorize or perform that operational upgrade. The existing offline
inspector can still read schema v3 without migrating; the delivery reader requires
v4 and never migrates or creates a database.

## Offline API

`upload_reader.read_delivery(path, after_sequence=0, limit=20,
expected_source={"fund_id": ..., "source_id": ..., "currency": ...})` opens the
database read-only, takes one short SQLite snapshot, and returns public source
identity, stream ID, events and an actual pending count. It imports neither MT5
nor dashboard modules, takes no ownership lock and performs no network access.
It has a 100 ms SQLite busy timeout; callers must later retry contention.
Complete sets are materialized for each selected observation. The limit bounds
event count, not the size of a single set; oversized requests are rejected rather
than truncated. M5.2 must enforce binding and resource/byte budgets.

`validate_request`, `serialize_request` and `validate_acknowledgement` are pure
functions in `upload_contract.py`. Invalid input raises `ValueError`. A dangling
entry, inconsistent stored batch, non-finite number, mismatched source/account
binding or lossy identifier prevents successful serialization; nothing is skipped.

## Request schema

Contract version and collector database version are independent. The request
requires exactly `contract_version=1`, `batch_id`, `source`, `stream_id`,
`first_sequence`, `last_sequence` and `events`. Unknown fields are rejected.
Source contains exactly fund/source aliases and currency, never broker login or
server. Each request contains 1..20 strictly ordered events, allowing sequence
gaps, and at most 512 KiB of canonical UTF-8 JSON. Duplicate event IDs are rejected.

All sequences and broker identifiers, including identifiers nested inside active
records, are exact decimal strings; floating-point identifiers are rejected.
Record, batch, cycle and stream IDs are nonempty strings. Observation timestamps
require an explicit zero UTC offset and ordered start/end intervals. Their clock
quality remains that of the collector PC, including any uncorrected skew.

An `account_observation` contains all seven account financial fields, an optional
cycle object, complete recorded active batches and `absent_datasets`. A cycle has
its own ID and collection interval. A null cycle denotes a legacy observation;
it has no batches and both datasets absent. A recorded cycle may also lack one
dataset after an identity failure; absence means **not recorded**, not empty or
failed. Different collection intervals do not describe a broker-atomic snapshot.

Each active batch has its stable ID, dataset, interval, outcome and item count:

| Outcome | Count | Records | Meaning |
|---|---|---|---|
| `success` | Integer equal to set size | Complete array, including `[]` | Observed active set |
| `failed` / `identity_mismatch` | `null` | Field absent | Failed read; matching sanitized `error_code` required |
| Dataset in `absent_datasets` | No batch | No batch | No recorded result |

Position records require ticket/identifier strings, symbol, integer type, volume,
opening/current prices, SL/TP, profit and swap. Order records require ticket,
position ID and position-by ID strings, symbol, integer type, initial/current
volume, opening/current/stop-limit prices and SL/TP. No operation-type enum is
assumed. All financial values must be finite numbers; booleans are not numbers.

Each record requires `broker_event_times={"status":"unverified","raw":{...}}`.
Position raw fields are time, time_msc, time_update and time_update_msc; order
fields are time_setup, time_setup_msc, time_done, time_done_msc and time_expiration.
Untouched integer values are represented as exact decimal strings. There are no
derived broker UTC/Tehran fields. Collector intervals are separately named UTC
observation intervals; receipt time must never replace them.

Comments, magic/reason, external IDs, strategy metadata, paths, credentials,
login/server and arbitrary diagnostic text are dropped by stored-record
serialization. Wire validation rejects unknown fields recursively instead of
accepting private extras. Collection statuses allow `saved`, `retrying`,
`stopped_identity_mismatch`, positions/orders `failed` or `identity_mismatch`,
and `unknown`. Unrecognized local status text becomes `unknown`; details stay local.

Example request (all values fictional):

```json
{
  "contract_version": 1,
  "batch_id": "example-upload-batch",
  "source": {"fund_id": "example-fund", "source_id": "example-source", "currency": "USD"},
  "stream_id": "example-stream",
  "first_sequence": "101",
  "last_sequence": "101",
  "events": [{
    "sequence": "101",
    "kind": "account_observation",
    "record_id": "example-observation",
    "started_at_utc": "2026-01-01T00:00:00.100Z",
    "ended_at_utc": "2026-01-01T00:00:00.200Z",
    "account": {"balance": 1000, "equity": 1001, "profit": 1, "credit": 0, "margin": 10, "margin_free": 991, "margin_level": 10010},
    "cycle": {"cycle_id": "example-cycle", "started_at_utc": "2026-01-01T00:00:00.090Z", "ended_at_utc": "2026-01-01T00:00:00.400Z"},
    "absent_datasets": [],
    "active_batches": [
      {"batch_id": "example-positions", "dataset": "positions", "started_at_utc": "2026-01-01T00:00:00.210Z", "ended_at_utc": "2026-01-01T00:00:00.300Z", "outcome": "success", "item_count": 0, "records": []},
      {"batch_id": "example-orders", "dataset": "orders", "started_at_utc": "2026-01-01T00:00:00.310Z", "ended_at_utc": "2026-01-01T00:00:00.400Z", "outcome": "failed", "item_count": null, "error_code": "failed"}
    ]
  }]
}
```

## Deterministic bytes, digest and acknowledgement

`serialize_request(request)` validates and returns `(bytes, sha256_hex)`.
Bytes use sorted object keys, compact separators, ASCII JSON escaping, no NaN
and UTF-8 encoding. Array order is significant. The offline reader fixes batch
order by dataset, record order by ticket text, normalizes observation UTC format,
and serializes active financial values as floats. This is a defined Python v1
encoding, not a claim of implementing a universal JSON canonicalization standard.
For directly supplied requests, integer `1` and float `1.0` have different bytes.

The digest covers **the entire serialized request**, including contract version,
batch/stream/source identity, sequence bounds, every event and array ordering.
It excludes HTTP headers and any acknowledgement; there is no self-referential
digest field. Changing the batch ID changes this digest. It is a replay/conflict
fingerprint, not authentication. M5.2 must persist the exact bytes before sending;
a receiver can additionally fingerprint individual records independently of the
request envelope when deduplicating regrouped replays.

An acknowledgement requires exactly these fields:

```json
{
  "contract_version": 1,
  "batch_id": "example-upload-batch",
  "source_id": "example-source",
  "stream_id": "example-stream",
  "accepted_through_sequence": "101",
  "request_sha256": "<sha256 returned by serialize_request>",
  "result": "committed",
  "received_at_utc": "2026-01-01T00:00:01Z"
}
```

The digest placeholder must be replaced with the computed 64-character hex value.
Validation requires exact request identity, digest and final sequence, plus an
aware UTC receipt time. Partial or queued acknowledgements are rejected. This
validates a schema only; no backend transaction, authentication, HTTP response,
progress advancement or exactly-once delivery is implemented in M5.1.

## M5.2 recovery requirements

Use a separate uploader state file, source/endpoint binding and uploader-only
lock. Persist a pending batch and its exact bytes before HTTP. Advance durable
progress only for a matching authenticated committed acknowledgement. Retry
unchanged bytes after timeout or restart; require receiver deduplication.

**Stream ID alone does not detect restoring an older backup.** A backup contains
the same stream ID and may later reuse sequence values from a discarded future.
Persist acknowledged anchor IDs/content digests alongside sequence, verify the
anchor against local data, and pause on a missing/different anchor or cursor
beyond the stream. Require explicit recovery/replay when replacement/rollback
is detected, and define backend handling of a restored divergent stream. A new
stream UUID detects some replacements but is not sufficient recovery by itself.
Do not infer pending count from sequence arithmetic or silently reset a cursor.
No retention/deletion is introduced.

## Verification

Use `.venv\Scripts\python.exe -m unittest tests.test_delivery -v`. Tests use only
temporary databases, including migration rollback and atomic visibility checks.
No test result here establishes backend interoperability or live MT5 behavior.
