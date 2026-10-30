# M5.2: independent uploader and local receiver fixture

M5.3 extends offline visibility with JSON v2 and `--format text`; see
[uploader-status.md](uploader-status.md) for freshness, backlog/ack separation,
signed ages, pre-v4/lock handling and compatibility fields.

One uploader process per source reads a **schema v4** collector database and
writes a separate uploader-state SQLite file. It never imports MT5, takes a
terminal/collector lock, changes the collector database or runs HTTP in the
recorder. No operational source was accessed or upgraded during implementation.
The hosted backend remains uninspected. The receiver here is a loopback-only
test fixture, not a hosting or production backend choice.

Use the existing `.venv\Scripts\python.exe`. All new code uses the standard
library; no dashboard or new package dependencies are needed. History remains
disabled operationally and unverified event-time mappings stay inactive.

## Configuration and commands

Copy `uploader.example.json` to a private location outside Git, for example
`C:\private\mt5-uploader\example.upload.local.json`. Configuration is selected
only with `--config`; inherited environment variables do not override it.
Configure absolute collector/state database paths, the public expected
fund/source/currency, exact destination and a source-specific bearer token.
Use a separate state file and token per source. Do not reuse collector `.env`
files: uploaders need no login, server, executable or MT5 credentials.
The reserved `.invalid` example endpoint is not an operational endpoint.

Writers hold an uploader-only lock on the state path and a source lock keyed by
canonical collector path in the user's temporary directory. These do not conflict
with collector ownership. Run cooperating uploaders as the same Windows user with
the same temporary-directory environment; these locks are not a global source-ID
registry and cannot coordinate independent copied databases or unrelated clients.

```powershell
.\.venv\Scripts\python.exe .\uploader.py check --config C:\private\mt5-uploader\example.upload.local.json
.\.venv\Scripts\python.exe .\uploader.py status --config C:\private\mt5-uploader\example.upload.local.json
.\.venv\Scripts\python.exe .\uploader.py once --config C:\private\mt5-uploader\example.upload.local.json
.\.venv\Scripts\python.exe .\uploader.py run --config C:\private\mt5-uploader\example.upload.local.json
```

- `check` is an offline local-readiness check. It reads configuration, stream,
  source and existing state/anchors; it does not contact the endpoint, validate
  credentials remotely, ingest, create state/locks or migrate a database. A paused
  or inconsistent initialized source returns nonzero. Uninitialized state is
  reported explicitly; a passing check is not proof of backend readiness.
- `status` is also read-only/offline. It shows the actual unacknowledged event
  count, pending request ID/range/size, acknowledged sequence, last acknowledgement,
  attempts, retry time and sanitized error. An inconsistent source reports unknown
  backlog instead of a fabricated zero. It does not expose pending financial
  payloads or tokens. It does not measure backlog bytes/disk-free space.
- `once` initializes/binds state if needed and attempts at most one request,
  respecting durable backoff. Return 0 means acknowledged or idle, 1 means retry/
  backoff, 2 means paused/fatal. A batch may include multiple complete events.
- `run` repeats until Ctrl+C or a pause/fatal error. It releases all SQLite read
  transactions before HTTP and waits with no database transaction held. Ctrl+C
  preserves pending bytes and releases uploader locks. No service, scheduled task
  or automatic startup is installed.

Do not start operational uploaders against v3: the uploader refuses to migrate.
An authorized collector upgrade requires stopped-recording backup/integrity checks
as described in the contract. Those operational steps have not been performed.

## Persistence and recovery

The uploader state database has its own schema v1, distinct from collector v4.
It binds source, stream ID, canonical collector path/file identity and exact
destination. Token rotation is allowed without resetting progress. Changing
destination/source/stream or replacing the file requires explicit investigation;
state is not silently rebound or reset. State and collector paths cannot be the
same file. Unrecognized existing SQLite files are rejected rather than reused.

One pending request is committed before sending: batch ID, exact canonical bytes,
SHA-256 and sequence range. Requests are at most 20 events and 512 KiB. A large
multi-event request is reduced at event boundaries; an oversized/invalid individual
event pauses. No set is truncated or event skipped. Sequence gaps are permitted;
backlog uses `COUNT(*)`, not sequence subtraction.

On every attempt, the uploader verifies the last acknowledged event's immutable
content fingerprint and all pending events in the collector's current SQLite
snapshot. Pending bytes must still cover the next undelivered events. Stream ID,
public source identity, file identity and progress/head are also checked. Restoring
an old backup in-place can preserve stream/file identity: missing or different
acknowledged/pending anchors still pauses delivery. A pending retry is independent
of later, unrelated malformed records.

These boundary anchors are not a complete cryptographic audit of all earlier
records. An unchanged acknowledged prefix and an unobserved lost suffix cannot
prove that never-seen data existed. Preserve collector/state backups together and
investigate restores; do not delete state or reset cursors as routine recovery.
Explicit replay/rebinding after a restore or destination change is an operator
decision requiring a reviewed recovery plan and receiver deduplication.

After fixing an authentication/configuration problem or investigating a paused
request, explicitly retry using `once --resume` or `run --resume`. This clears the
pause/backoff only; bindings and all anchors are revalidated before HTTP. It does
not authorize resetting progress, accepting changed content or skipping a record.
`check/status --resume` is rejected.

Only a full v1 `committed` acknowledgement matching the pending identity, digest
and final sequence advances progress. The anchor/ack update and pending deletion
share one state transaction. If that write fails after remote commit, the process
stops without reporting successful durable progress; pending bytes remain for
replay. If the commit response is lost, unchanged bytes and batch ID are resent.
This is at-least-once delivery with receiver deduplication, not exactly-once HTTP.

## Transport, retries and resource bounds

External endpoints require HTTPS with the default verified certificate chain and
hostname verification. Plain HTTP requires `local_test: true` **and a literal
loopback IP address** such as `127.0.0.1`; `localhost` is deliberately not accepted
for HTTP. URLs cannot contain user information, query strings or fragments.
Redirects are rejected rather than followed; no token is forwarded to a new URL.
The HTTP client does not use environment proxy configuration.

Defaults: 10-second socket timeout, 30-second attempt watchdog, five-second idle poll,
one pending request and 64 KiB maximum acknowledgement body. Acknowledged backlog
drains one bounded request at a time with a 100 ms pacing wait. The watchdog shuts
down an established socket to interrupt slow reads. OS hostname resolution may
outlast the watchdog before a socket exists; this is not a hard DNS-time guarantee.
Requests and responses are never logged. Errors are fixed codes, not remote
bodies or exception text. Invalid/malformed/partial acknowledgements pause.

Transport failures/timeouts, HTTP 429 and 500/502/503/504 retry with durable
exponential backoff (default 1..60 seconds with jitter). Numeric `Retry-After`
seconds are honored up to 300 seconds; date-form headers are not interpreted.
Active retry waits use a monotonic clock. On restart, wall time before the saved
start by more than two seconds or after the deadline by more than 300 seconds,
and legacy deadlines lacking metadata, are uncertain. Uncertain waits are 1..300
seconds; legacy waits conservatively use 300 seconds. A known numeric Retry-After
is included in the saved delay. `once` remains nonblocking for ordinary backoff,
but waits interruptibly on an uncertain restart before one attempt. Repeated
interruptions before the wait completes can postpone delivery indefinitely.
Old uploader versions do not enforce the new schedule; do not downgrade without
reviewing pending retry state. Wall time across reboot is only an estimate.
401/403, validation failures, content conflicts, redirects and other HTTP outcomes
pause visibly. Certificate verification failures pause. The pending request
remains intact. `--resume` is explicit operator action, not automatic retry of
permanent failures.

Collector reads use a 100 ms SQLite busy timeout and a cooperative one-second
read budget, plus at most 10,000 records per active set and 1 MiB raw JSON per
record. Exceeding a set/record limit pauses rather than truncating. Busy/read-budget
errors retry. Before creating pending bytes, repeated budget errors halve the
selected event count down to one; an existing pending request is never changed.
Complete bounded sets are materialized and the read connection is
closed before HTTP. Readers can briefly contend with recorder commits in SQLite's
existing journal mode; no journal-mode change is made. Monitor repeated read-budget
errors on large backlogs instead of assuming a hard five-second recording cadence.

## Loopback-only test fixture

Copy `receiver.example.json` to a private `*.receiver.local.json` file. Choose a
temporary receiver database and a unique test token for each allowed source.
For the matching temporary uploader configuration set the same test identity/token,
`local_test: true` and `http://127.0.0.1:8765/ingest/v1/batches`.

```powershell
.\.venv\Scripts\python.exe .\upload_test_receiver.py --config C:\private\mt5-uploader\example.receiver.local.json
```

The fixture binds only `127.0.0.1` and accepts bounded canonical JSON POSTs at that
path. Source-scoped bearer authentication also checks configured fund/currency.
Persistent source/fund/currency bindings reject repurposing a source alias.
All records, stable cycle/active-batch identities, stream sequence mappings,
request digest and acknowledgement commit in one SQLite transaction. Identical
request replay returns the saved acknowledgement. Identical stable records can
deduplicate across new request envelopes; different content under reused IDs or
stream sequences returns 409 and rolls back the entire request. Record identity
fingerprints omit transport sequence; anchor fingerprints retain it. No arbitrary
diagnostic/comment/private account fields are accepted by the contract.

The fixture has no public-response API, production auth provisioning, TLS server,
tenant administration, rate limiting, deployment or availability guarantees.
Keep it bound to loopback; do not expose it as a hosted backend.

## Verification and next operational steps

Run `.venv\Scripts\python.exe -m unittest tests.test_uploader -v`. Tests use only
temporary SQLite files and actual IPv4-loopback HTTP, with controlled clocks for
backoff. Fake transport/HTTPS-constructor tests are separate from the actual HTTP
cases; they do not establish external TLS or production compatibility.

Implementation verification: 25 uploader tests passed, including seven test
methods using actual loopback HTTP (one also uses separate uploader processes).
The bounded full suite passed 130 tests; 125 recorder/uploader tests also passed
with dashboard imports blocked. Other tests use fake transport,
controlled clocks, temporary SQLite files, a real cross-process uploader lock,
and an HTTPS-constructor mock. No operational database, MT5 terminal, external
ingestion endpoint or operational uploader was accessed.

Before operational verification: inspect the real backend and agree on source
provisioning/contract compatibility; authorize and verify collector v4 upgrades;
choose private destination/tokens/state paths; perform a bounded test-source run,
compare ingestion counts and restart/retry behavior, then review a source-by-source
rollout. Keep history disabled and raw event-time mapping unverified.

Status is on-demand/console only. M5.3 adds oldest-backlog age and exact pending
bytes; centralized monitoring, total backlog bytes, disk-space alarms and a
reviewed retention policy remain future work. Do not delete
collected history or unsent data during outages. No retention is implemented.
