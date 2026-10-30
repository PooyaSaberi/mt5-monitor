# Developer handoff: Gold Algorix MT5 monitor

## Verified checkpoint

Prepared on **30 October 2026 (Asia/Tehran)**, immediately after the M5.3 push.

- Repository: `D:\repo\mt5-monitor`; branch: `main`.
- Remote `origin`: `https://github.com/PooyaSaberi/mt5-monitor.git`.
- Latest pushed implementation checkpoint at writing:
  **`2de670faedc3baa78591455b1c10a45239ecaee5`**,
  `Add offline uploader visibility and freshness status`.
- `git ls-remote origin refs/heads/main` matched that full local hash.
- This handoff is committed separately **after** that checkpoint. Its own commit
  will therefore be newer; use `git log -2` / `git rev-parse HEAD` and compare the
  remote before continuing. Do not mistake the implementation hash above for a
  claim that no later documentation commit exists.
- M5.3 was not already committed: local/remote main had been at `2986f2d`; the
  checkpoint was created once, pushed normally and verified. Never duplicate it.

Read `AGENTS.md` first. Use `.venv\Scripts\python.exe` for all Python/package
commands; never install project packages globally. Monitor only: never call
`mt5.login()`, switch accounts, place/modify/cancel trades, change EAs or trading
permissions, or restart/terminate MT5 without explicit new authorization.
Do not commit private configuration, tokens, account numbers, terminal paths,
databases, exports, logs or diagnostic evidence. Keep changes small.

## Immediate next task

**Inspect the actual backend/hosting before integration.** The production hosting,
database, authentication/provisioning and frontend have not been inspected.
Do not infer a provider, framework, ingestion endpoint or deployment architecture.
Use read-only discovery first and obtain missing access/details from the operator.
Agree on contract compatibility, transactional deduplication, source isolation,
request limits, HTTPS/authentication and eventual public API responsibilities.

**`upload_test_receiver.py` is a loopback-only test fixture, never a production
deployment.** Do not expose it publicly or assume its SQLite/HTTP/auth setup is a
hosted-backend decision. Do not start collectors/uploaders merely to resume work.

## Purpose and architecture

The EA trades; this Python code monitors. One codebase supports three independent
collector processes, explicit terminal installations/accounts, distinct data
directories and separate local SQLite databases. Each source failure is isolated.

- `collector.py`: single sequential MT5 connection owner; guarded account,
  position/order reads; optional history slots; shutdown/backoff/ownership.
- `recorder_config.py`, `account_reader.py`, `deal_reader.py`: explicit config and
  identity/demo/connectivity checks. No trading or credential-based login.
- `collector_storage.py`: transactional observations/cycles/batches/statuses and
  migrations. `history_storage.py` / `history_sync.py`: bounded restartable history.
- `terminal_lock.py`, `collector_preflight.py`: cooperating-process terminal,
  configured data-directory and database ownership plus offline set validation.
- Collector code currently supports schema **v4**: `delivery_stream.py` adds an
  explicit stream UUID and transactional observation/status delivery entries.
  Sequence values are ordered identifiers, not gap-free counts. Backfill transport
  order is not historical commit order. Never infer closure/P&L from set omission.
- `upload_contract.py` / `upload_reader.py`: versioned private-field allowlists,
  exact string identifiers, complete active sets, deterministic bytes/digests and
  read-only delivery extraction. Empty, failed, absent and legacy sets differ.
- `uploader.py`, `upload_config.py`, `upload_state.py`, `upload_lock.py`,
  `upload_http.py`: independent uploader, separate state schema v1, its own locks,
  pending bytes before HTTP, verified acknowledgement before durable progress.
  The uploader never imports MT5, owns terminals or migrates collector databases.
- `upload_integrity.py` shares source/stream/file/destination/content-anchor checks.
  `upload_status.py` separates collection freshness, backlog and historical ack.
  Status has no network/MT5 imports/calls; HTTP imports are deferred to delivery.
- `collector_inspect.py`: offline inspection/JSON/CSV, including history metadata.
  `report_time.py`: explicit reporting timezone and gated broker-time mappings.

Delivery is **at least once**, with receiver deduplication. Pending ID, exact bytes,
digest and range survive restart; state updates/clear are atomic. Unknown/oversized
individual events pause rather than skip. HTTP redirects are rejected; external
delivery requires certificate-verified HTTPS. Plain HTTP requires an explicit test
flag and literal loopback IP. Content anchors detect relevant restore/rollback
inconsistencies, but are not an audit of every old record or never-seen lost data.

## Completed milestones and evidence

| Milestone | Implementation and actual verification |
|---|---|
| M1 (`37be3ce`) | Persistent read-only account recorder, source binding, UUID/UTC intervals, separate statuses, clean shutdown and terminal lock. GA1 live collection/restart and second-recorder rejection verified; manual account-value comparison recorded in README. |
| M2 (`31cce26`) | Complete position/active-order batches, partial-read failure handling and v2 migration. GA1 live v1-to-v2 migration retained M1 history; populated positions, empty orders, restart and offline exports verified. Trade-tab position values were compared. |
| M3 (`d7abd5b`) | Optional chunked historical deals, fair scheduling, idempotent upserts/revisions, omission evidence, durable reconciliation/progress, schema v3, Tehran support. Isolated GA1 seven-day test imported 54 deals/revisions; bounded import, one-day reconciliation, restart and export verified. Some visible balance/exit-deal rows compared. Absolute event/query-time mapping was NOT established. |
| M4 (`d92b195`) | Three operator-confirmed source assignments/configurations; live account/server/demo/connectivity/data-directory checks. Verified stopped GA1 backup and v2-to-v3 upgrade; three-source simultaneous collection and GA2-only stop/restart passed. |
| M5.1 (`9c49929`) | Contract v1 and transactional v4 delivery stream/backfill. Temporary-database migrations/rollback/privacy/precision/serialization tested; no operational v4 upgrade. |
| M5.2 (`2986f2d`) | Durable uploader and loopback test receiver. 25 uploader tests, including seven actual loopback HTTP test methods: loss/timeout after commit, process restart, source/auth/conflict cases, concurrent recording/read delivery, limits. External HTTPS evidence is mocked, not live. |
| M5.3 (`2de670f`) | Offline JSON v2/text visibility, signed ages/future flags, exact pending bytes, actual backlog, retry/pause and distinct ack clocks. 16 new visibility tests; status leaves test files unchanged and works with network/MT5 imports blocked. |

Latest full-suite evidence: **146 tests passed** in a bounded run. **141 tests
passed with dashboard imports blocked**, including a staged-only snapshot run
immediately before the M5.3 commit. Tests use fake adapters, controlled clocks,
temporary databases, real process locks and loopback fixtures as appropriate.
Fake adapters do not establish actual MT5 behavior. No production ingestion/TLS
or operational uploader delivery has been verified. The handoff-only commit
does not change executable behavior.

M4 live detail: approximately two minutes produced 24 cycles per source; stopping
GA2 for 15 seconds left it at 24 while GA1/GA3 each appended three. GA2 restarted
and appended three. Total new cycles were 30/27/30 for GA1/GA2/GA3; median spacing
about 5.009/5.009/5.010 seconds, with GA2's intentional 19.428-second restart gap.
GA1 had two observed positions and empty orders; GA2/GA3 had successful empty
positions/orders. All stopped cleanly; terminals remained open at that time.
That does not establish present process state or EA health.

Pending live evidence includes visual financial-value comparisons for the M4
three-source run, EA health, populated orders, induced connectivity/failure
recovery, and broker corrections/omissions. Do not modify trades, accounts or the
office network to manufacture cases. M3 later diagnostics located missing opening
deals by position ID/widened raw range; results were kept separate from progress.
Requested numeric range coverage is not proven complete physical-UTC coverage.

## Operational database and time state: do not infer from code version

Existing private M4 JSON reports were read for this handoff; **no operational
database was opened**. Those reports establish schema **v3 for GA1, GA2 and GA3**,
integrity OK and zero history queries. They establish GA1's v2-to-v3 upgrade and
retention of all 48 earlier observations, 18 cycles, 36 batches, 36 active records
and 48 statuses. They report the original `.env` unchanged. No later operational
v4 upgrade evidence has been established. Treat operational databases as **below
v4 unless later verified existing evidence says otherwise**; do not open them just
to answer this question, and do not infer their state from current code/tests.

**Before the next operational recorder start**, stop recording safely, obtain a
consistent SQLite API backup and verify it opens/passes integrity checks. Current
recorder code automatically upgrades supported databases to v4 even with history
disabled. Require explicit authorization and verify retention; preserve backup
and affected database if anything fails. The uploader refuses to migrate. Never
blind-copy an active WAL database, delete history/state or reset progress to recover.

History remains disabled operationally. Do not enable it or choose GA1's ongoing
history-start date by guessing; the disposable M3 test date was not that decision.
Broker event and query mappings remain unverified; original MT5 timestamps are
preserved and authoritative derived broker UTC/Tehran times remain null/gated.
Alpari server display GMT+2/+3 is not proof of Python epoch semantics. Do not
subtract a guessed/today's offset or activate the example mapping.

**Asia/Tehran** is the reporting/display/day-boundary timezone, independent of
Windows local settings. Collector-observation Tehran conversion is available;
Tehran broker trade-day reporting/query plans remain gated on verified mappings.
Historical date-specific offset/DST behavior matters. PC-clock accuracy remains
unresolved: diagnostics measured about **44.7 seconds ahead of independent UTC**
at that time and reported unsynchronized clock status. That is historical evidence,
not a fresh measurement; no correction was made. Do not change Windows time here.
Status ages are PC-clock-dependent and flag future values; past acks do not prove
current backend reachability. Receiver receipt is not collector observation time.

## Commands and documentation

Read [README](../README.md), [upload contract](upload-contract.md),
[uploader/recovery](uploader.md) and [offline status](uploader-status.md).
Safe templates: `.env.example`, `time-mapping.example.json`,
`uploader.example.json`, `receiver.example.json`.

Use the existing venv. Committed direct dependencies are
`MetaTrader5==5.0.6231` and `tzdata==2026.6` (the tested Windows timezone-data pin).
M5 delivery/visibility adds no packages. Worktree `requirements.txt` additionally
contains unrelated dashboard changes; do not install or stage those accidentally.

The commands below describe interfaces, **not permission to start them now**.
All configuration paths shown are safe placeholders; see private locations below.

```powershell
# Offline source-set preflight; no MT5 connection or migration.
.\.venv\Scripts\python.exe .\collector_preflight.py --config C:\private\ga1.env --config C:\private\ga2.env --config C:\private\ga3.env
# Connection check DOES connect to the configured terminal; initialize may launch it.
.\.venv\Scripts\python.exe .\collector.py check --config C:\private\ga1.env
# OPERATIONAL START: backup/upgrade authorization required first.
.\.venv\Scripts\python.exe .\collector.py record --config C:\private\ga1.env
# Offline database export; contains private data, so keep output outside Git.
.\.venv\Scripts\python.exe .\collector_inspect.py C:\private\collector.sqlite3 --format json --output C:\private\sample.json
# Offline uploader check/status: no network, state creation or migration.
.\.venv\Scripts\python.exe .\uploader.py check --config C:\private\example.upload.local.json --format json
.\.venv\Scripts\python.exe .\uploader.py status --config C:\private\example.upload.local.json --format text
# once/run perform HTTP; do not invoke before backend/config approval.
.\.venv\Scripts\python.exe .\uploader.py once --config C:\private\example.upload.local.json
.\.venv\Scripts\python.exe .\uploader.py run --config C:\private\example.upload.local.json
# Explicit --resume rechecks anchors; it never resets/skips progress.
```

Selected config files have explicit precedence; inherited environment values do
not override them. Collector defaults to five-second intervals, not a hard cadence
during synchronous MT5 work. Locks coordinate cooperating processes under the same
Windows user/temp environment, not unrelated clients or copied databases. Do not
run the old dashboard as another MT5 connection owner alongside collectors.

## Private locations only: do not copy contents into Git/chat

- Original ignored configuration: `D:\repo\mt5-monitor\.env`.
- Private source configs: `D:\mt5-configs\ga1.env`, `ga2.env`, `ga3.env`.
- Operational SQLite files/backups: `D:\mt5-data\` (source alias filenames).
- Existing private M4 evidence: `D:\mt5-configs\m4-live-20261030T074312Z\`
  and `D:\mt5-configs\m4-upgrade-20261030T073958Z.json`.
- Other isolated test exports/diagnostics/configuration are private local files,
  not repository artifacts. Operational uploader token/state locations have not
  been established by this handoff; example paths are not deployed configuration.

No account numbers, credentials, real terminal executable/data paths, private
configuration contents or actual financial values are included here.

## Preserve unrelated uncommitted dashboard work

At handoff creation, after M5.3 commit, `git status --short` showed:

```text
 M README.md
 M requirements.txt
?? dashboard.py
?? static/
?? tests/test_dashboard.py
```

The static files are `static/index.html`, `static/app.js`, `static/style.css`.
README's remaining unstaged hunks describe the dashboard: introduction, refresh
wording, the local-browser-dashboard section and monitor/dashboard interval text.
`requirements.txt` has unrelated additions `fastapi==0.162.0`, `uvicorn==0.54.0`,
`httpx==0.28.1`. All must remain untouched/uncommitted unless explicitly requested.
The committed collector/uploader/status tests pass without those dependencies.
Do not reset, clean, overwrite, stage wholesale or force-push to remove this work.
If later editing README, stage only the task's relevant hunks. The separate
handoff commit contains **only this document**.
