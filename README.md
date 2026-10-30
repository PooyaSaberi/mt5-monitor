# MT5 Monitor

## M5.2: independent durable uploading

`uploader.py` supplies offline `check/status` and explicitly invoked `once/run`
commands. Pending bytes and acknowledgement progress live in separate locked
SQLite state; the uploader never connects to MT5 or migrates collector databases.
`upload_test_receiver.py` is an IPv4-loopback-only test fixture, not a production
backend. See [configuration, commands, retries, recovery and limitations](docs/uploader.md)
and the safe `uploader.example.json` / `receiver.example.json` templates.
No new dependencies are required. Operational databases remain at their previous
schema versions: uploader use requires an independently authorized, backed-up
collector v4 upgrade. No operational uploader was started during implementation;
history stays disabled and broker event-time mapping remains unverified.

## M5.1: durable local delivery preparation

Schema v4 adds a transactional delivery stream for account observations and
sanitized collection statuses, including each observation's complete recorded
position/order batches. The offline allowlisted contract distinguishes empty,
failed and absent datasets, preserves exact identifier strings, and labels raw
broker event times unverified. No networking, uploader runner or receiver exists
yet. See [contract, examples, migration and recovery requirements](docs/upload-contract.md).
Before a later recorder start against an operational database, stop recording,
take and verify a consistent backup: the next start upgrades to v4 even with
history disabled. Operational databases were not migrated during M5.1 coding.
History remains disabled and event-time mapping remains unverified.

## M4: prepare independent sources

Use one selected private configuration, one process, one explicit terminal installation/data directory and one SQLite file per source. `--config` reads only that file; inherited environment variables do not override it. Duplicate keys and `UNRESOLVED...` placeholders fail validation. Keep private files outside the checkout; `*.env` is also ignored. Preserve established fund/source identifiers and database paths after recording begins.

Add `MT5_DATA_PATH` using **File > Open Data Folder** in each intended terminal. The runner checks the connected terminal's actual data directory before account reads. This setting is optional for older configurations but required by the M4 offline set preflight. Keep `HISTORY_ENABLED=false`; do not activate an unverified timestamp mapping.

With collectors stopped, validate the complete set without importing MT5, connecting, creating databases or migrating them:

```powershell
.\.venv\Scripts\python.exe .\collector_preflight.py --config C:\private\mt5-configs\ga1.env --config C:\private\mt5-configs\ga2.env --config C:\private\mt5-configs\ga3.env
```

Preflight rejects duplicate terminal paths, data directories, database paths, fund/source identifiers and expected account/server pairs. Existing database identity must match. A schema-upgrade note is a prerequisite to recording: stop the recorder, create and verify a SQLite backup as described below, and obtain approval before an existing database is upgraded. Preflight does not approve or perform that upgrade. New paths are reserved only; files are created later by recording. Re-run preflight after editing any configuration.

After operator confirmation of account assignments, distinct running terminals/data folders, a passing set preflight and any required backup/upgrade approval, open three visible PowerShell consoles. In each console select its source and run the following independently (use `ga2.env` and `ga3.env` in the other consoles):

```powershell
Set-Location C:\path\to\mt5-monitor
.\.venv\Scripts\python.exe .\collector.py check --config C:\private\mt5-configs\ga1.env
.\.venv\Scripts\python.exe .\collector.py record --config C:\private\mt5-configs\ga1.env
```

Run `record` only after that source's check passes. Confirm terminals are already open first: MT5 `initialize(path=...)` may automatically launch a closed terminal. Stop each recorder with Ctrl+C in its own console. No services, scheduled tasks or automatic startup are installed. Do not run the old dashboard, continuous monitor or another MT5 client as an additional connection owner alongside these collectors.

Cooperating recorder processes hold executable, configured data-directory and database locks before connecting. The terminal lock retains compatibility with earlier recorder versions. Database binding additionally rejects a different source/account/server/currency. Locks are local to the same Windows user's temporary lock directory; they do not coordinate other Windows users, older clients lacking these guards, or unrelated MT5 programs. Use the same Windows user and code version for all three sources. Set-level duplicate identifier validation requires running preflight; it is not a global identifier registry. A failure stops/retries only its own process. Offline tests cover these protections; the scoped three-terminal live evidence is recorded below.

For live verification, check all three sources independently, collect briefly in the three consoles, inspect each database offline, verify distinct identities and appended account/position/order records, then stop each process and confirm the other two continue. Do not change accounts or trades to manufacture failures. EA health and broker event-time mapping require separate evidence.

M4 live evidence (30 October 2026): the operator confirmed the GA2/GA3 installation assignments. Separate private configurations passed set preflight and live account/server/demo/connectivity/data-directory checks. A SQLite API backup of stopped GA1 passed integrity/content checks; the same transactional recorder storage migration was invoked offline to upgrade v2 to v3. All 48 pre-existing account observations, 18 cycles, 36 active batches, 36 active records and 48 status events remained unchanged, including after live verification. Source binding and the original .env were preserved. This supersedes the earlier M3 checkpoint statement that the ongoing database had not yet been migrated.

All three sources recorded together for approximately two minutes (24 cycles each). GA2 alone was stopped for 15 seconds: GA1 and GA3 each appended three observations while GA2 remained at 24. GA2 restarted and appended three more. Final new cycle counts were GA1 30, GA2 27 and GA3 30; median cycle intervals were about 5.009, 5.009 and 5.010 seconds. GA2 had an intentional 19.428-second restart gap. Each cycle saved successful position/order batches: GA1 two positions and zero orders; GA2/GA3 empty positions and orders. All databases passed integrity checks, unique observation IDs and ordered UTC intervals were checked, and private offline JSON exports succeeded. All verification processes exited cleanly and locks were released; all three terminals remained running. History stayed disabled with zero history queries; no unverified time mapping was activated. Account totals were not available through accessibility text, so visual financial-value comparisons remain pending. EA health, populated orders and live failure/connectivity behavior remain pending. Backups, configurations, exports and detailed evidence remain outside Git.

Local, read-only monitoring prototype for an Alpari demo account. It provides a one-shot diagnostic, a continuous terminal monitor, and a persistent account recorder. It does not place trades.

## Setup

Use Windows with 64-bit Python 3.13 and an existing `.venv`. Install the pinned recorder dependencies into that environment:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

The recorder's tested direct dependencies are `MetaTrader5==5.0.6231` and `tzdata==2026.6`. MetaTrader5 installs NumPy as a transitive dependency; tzdata supplies IANA timezone data on Windows. The committed `requirements.txt` installs both recorder dependencies with the command above. No broker password is needed.

## Local configuration

Copy `.env.example` to `.env` in the project directory, then replace all placeholders. `.env` is ignored by Git. Never commit a populated `.env`.

```powershell
Copy-Item .env.example .env
```

Set `MT5_TERMINAL_PATH` to the full path of the local MT5 executable, commonly `terminal64.exe`. In Windows, find the MetaTrader 5 shortcut, open **Properties**, and copy the executable path from **Target** (without command-line options). You can also use Task Manager's **Open file location** on a running terminal. The path must exist before this diagnostic connects.

In MT5, read the exact numeric login and server from the active account in the **Navigator** or account details. Set `MT5_EXPECTED_LOGIN` and `MT5_EXPECTED_SERVER` to those exact values. Confirm that it is a demo account. The diagnostic never calls `login()` and never switches accounts.

`MT5_REFRESH_INTERVAL` controls the monitor's interval in seconds. It defaults to `1` when omitted and must be a positive finite number no greater than `60`. The monitor retries failures after a delay between 1 and 5 seconds.

## Run

With MT5 connected to the intended demo account:

```powershell
.\.venv\Scripts\python.exe .\mt5_monitor.py
```

`initialize()` uses only the configured terminal path. It may launch that terminal if it is not already open. The script releases its Python connection with `shutdown()`; it does not close the desktop terminal.

Example output (illustrative values):

```text
Account: ****5678
Server: Demo-Server
Currency: USD
Balance: 1000.00
Equity: 995.00
Floating P/L: -5.00
Margin: 10.00
Free margin: 985.00
Open positions: 0
```

Errors go to stderr with a nonzero exit code. A disconnected terminal, account mismatch, or non-demo account stops the diagnostic before it reads positions. An empty position list is valid; an MT5 `None` result is an error. No trading history is read in this milestone.

Validation: the diagnostic was manually tested against an Alpari MT5 demo account before, during, and after a manually executed trade.

## Continuous monitor

With the same local `.env` configuration and the intended demo account active, run:

```powershell
.\.venv\Scripts\python.exe .\continuous_monitor.py
```

Press **Ctrl+C** to exit. Each sequential cycle shows the connection status, last successful collection time in UTC, account totals, and each open position's ticket, symbol, direction, volume, opening/current prices, floating P/L, stop loss, and take profit. The account login is masked and personal account-name fields are omitted.

The monitor checks the expected login, server, and demo status every cycle before positions and again before accepting a result. A mismatch stops it. Other connection/read failures mark the previous snapshot **STALE/RETRYING** with its age; before any successful read, data is **UNAVAILABLE/RETRYING**. It releases and reinitializes the Python connection after those failures. `initialize()` may launch the configured MT5 terminal if it is closed, so closing MT5 is not a reliable disconnection test. The monitor never closes the desktop terminal.

Account, position, and verification reads are separate MT5 calls, so a successful snapshot is not atomic. Fast market changes can occur between those calls.

Validation: the user confirmed that the continuous monitor works in a manual MT5 demo-account test.

## Troubleshooting

- **Missing or invalid configuration:** Check `.env` values and confirm the executable path points to a real `.exe` file.
- **Initialization failure:** Open the intended MT5 terminal and verify that the configured path is its executable.
- **Disconnected:** Check the terminal's connection status and its broker connection.
- **Account mismatch or non-demo account:** Inspect the active account locally in MT5 and update the expected login/server only if that account is the intended demo account. The script will not switch accounts.
- **Position read failure:** Check the terminal connection and try the command again; zero open positions is shown as `0`, not as an error.

Tests use a fake MT5 adapter and never connect to the terminal:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

## M1 persistent account recorder

The M1 recorder is independent of the dashboard and initially read account totals only. M2 adds open positions and active orders; it does not read historical deals or orders. Use the existing environment to install dependencies:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Restore the safe `.env.example` if needed and copy it to the ignored `.env` file. Set the explicit terminal executable path, expected demo login/server, stable `FUND_ID` and `SOURCE_ID`, and an **absolute** `COLLECTOR_DB_PATH` under a private local data directory. Do not put real values in tracked files. `MT5_REFRESH_INTERVAL` defaults to five seconds for this recorder; the existing CLI monitor retains its one-second default. The database path must be distinct for each source. The recorder rejects an existing database with another source, login, server or currency, and rejects unknown schema versions. Keep database backups before changing identity or schema; never edit identity rows to reuse a database.

```powershell
.\.venv\Scripts\python.exe .\collector.py check
.\.venv\Scripts\python.exe .\collector.py record
.\.venv\Scripts\python.exe .\collector_inspect.py C:\path\to\collector.sqlite3
.\.venv\Scripts\python.exe .\collector_inspect.py C:\path\to\collector.sqlite3 --format json --output C:\path\to\export.json
.\.venv\Scripts\python.exe .\collector_inspect.py C:\path\to\collector.sqlite3 --format csv --output C:\path\to\export.csv
.\.venv\Scripts\python.exe .\collector_inspect.py C:\path\to\collector.sqlite3 --format csv --csv-table status_events --output C:\path\to\status.csv
```

Use `--config C:\path\to\private.env` with `collector.py` for a different private configuration file. `check` verifies the connection and account without creating the database. `record` runs until Ctrl+C. It holds a Windows lock keyed to the terminal path so a second recorder cannot own the same terminal even with a different database path. The lock is released when the process exits; a crash releases the OS lock automatically. SQLite stores each successful observation and its saved status in one transaction. Later read failures create separate retry status events after a database has been bound. Initial failures before a verified account/currency cannot be durably attributed to a source and appear on stderr/stdout only. Storage errors stop recording and return a nonzero exit code. The inspector never imports or connects to MT5; exported data contains source identity, observation IDs and UTC interval timestamps.

`mt5.initialize(path=...)` can launch the configured terminal if it is closed. It receives no credentials and does not switch accounts. `shutdown()` releases only the Python connection, not the desktop terminal or EA. The collected start/end times describe an observation interval: MT5 account reads are not broker-guaranteed atomic snapshots. Historical equity before the recorder first ran is unavailable.

Windows verification for one demo account:

1. Confirm the configured executable, login, server and demo mode in MT5, then run `check`.
2. Start `record` and compare balance, equity, profit, credit, margin, free margin, margin level and currency in several observations with MT5 over several minutes.
3. Try a second `record` command for the same terminal with a different database path and confirm it is rejected.
4. Stop with Ctrl+C, inspect/export the database, restart and confirm earlier observations remain.
5. Simulate a broker connectivity loss and recovery; confirm retry status events, no new live observations during disconnection, and resumption after recovery.
6. Confirm the EA continues running and no trade action is made by the recorder.

GA1 live verification (29 October 2026): The configured terminal was already running, and `collector.py check` verified connectivity, the configured account and server, and demo mode. A roughly two-minute run saved 27 observations over 130.8 seconds. IDs were unique, timestamps were UTC and ordered, and collection spacing ranged from 5.01 to 5.06 seconds (median 5.03). Separate saved-status events were present. A second recorder using the same terminal and a different database path was rejected without interrupting the first. After a clean stop, restart appended three observations while retaining the earlier history; the MT5 process remained running. Offline JSON export succeeded. A recent saved balance, equity, profit and free margin matched the values visible in MT5 at comparison time; the other financial fields were not compared in the UI. This is live evidence for GA1 only, not a claim about other accounts or production.

Automated verification uses a fake MT5 adapter and does not connect to a terminal. The full 31-test suite passed during M1 implementation; focused recorder tests cover binding, restart, invalid schema, failed reads, storage failure, duplicate-process locking, shutdown and export. EA health and live connectivity-loss/recovery remain unverified. Do not disrupt office connectivity for a test without an operator-controlled window, since that could affect the EA and other terminals.

## M2 active position and order observations

The recorder now reads the complete open-position and active-order sets on every cycle after the guarded account read. Each dataset has its own read start/end time and outcome. `success` with `item_count=0` means MT5 returned a successful empty set. `failed` means MT5 returned `None`, raised an exception, disconnected, or returned invalid records; it does not mean zero items. An account snapshot can still be saved if either active-set read fails. An identity change rejects the affected batch and stops collection. Account, position, and order reads happen sequentially and are **not atomic**. The complete MT5 named-tuple fields are retained as JSON, including integer identifiers, timestamps, prices, volume, SL/TP, profit/swap where present, magic, reason, comment, and external ID. Ticket keys are stored as decimal text to preserve precision.

Each successful batch is a complete observed set. If an item is absent from a later successful batch, it was no longer observed as active at that time. This does not establish a closure execution, realized P/L, or cancellation reason; those require history reads, which M2 does not perform. Failed batches leave the prior successful history intact and have no implied current set.

Schema version 2 adds cycles, active batches, and active records while retaining M1 account observations, status events, and source binding. **Stop the recorder and back up the SQLite database before running the M2 recorder against an existing v1 database.** The next recorder start migrates v1 to v2 transactionally; unsupported newer versions are rejected. Do not copy an open database file as a backup. Use an SQLite backup tool or stop the recorder before copying its database file. The inspector is offline and does not connect to MT5:

```powershell
.\.venv\Scripts\python.exe .\collector_inspect.py C:\path\to\collector.sqlite3
.\.venv\Scripts\python.exe .\collector_inspect.py C:\path\to\collector.sqlite3 --format json --output C:\private\sample.json
.\.venv\Scripts\python.exe .\collector_inspect.py C:\path\to\collector.sqlite3 --format csv --csv-table active_batches --output C:\private\batches.csv
.\.venv\Scripts\python.exe .\collector_inspect.py C:\path\to\collector.sqlite3 --format csv --csv-table active_records --output C:\private\records.csv
```

For a short GA1 live check, first back up the stopped database, then run `collector.py check` and `collector.py record` with the existing private configuration. Observe a few cycles, stop with Ctrl+C, and inspect/export the position and order batches. Compare counts and fields with MT5's Trade tab without placing or changing trades. Empty batches are valid if MT5 shows no active items.

GA1 M2 live verification (29 October 2026): A consistent SQLite backup of the stopped v1 database passed `PRAGMA integrity_check`. The live database migrated to v2, retained all 30 M1 observations and source binding, and passed integrity check. A roughly one-minute run added 15 cycles with 15 populated position batches (two records each) and 15 successful empty order batches; a brief restart appended three more cycles. Visible position counts, tickets, symbols, directions, volumes, and opening prices matched MT5's Trade tab. Saved current prices and floating P/L were within 0.02 of a later display. Offline JSON and CSV exports retained exact ticket strings and explicit empty-batch outcomes. Populated active orders, live read failures, live connectivity loss, and EA health remain unverified.

Automated M2 evidence is separate from the GA1 live check: all 37 tests passed in a bounded run. The recorder tests use fake MT5 adapters and temporary databases to cover populated and empty sets, failed reads, identity changes, v1 migration, retained history, identifier precision, and offline export. The tests do not verify actual terminal behavior.

MT5 field and error semantics: [positions_get](https://www.mql5.com/en/docs/python_metatrader5/mt5positionsget_py), [orders_get](https://www.mql5.com/en/docs/python_metatrader5/mt5ordersget_py).

## M3 historical deal synchronization

Checkpoint status: restartable deal synchronization and explicit `Asia/Tehran` reporting support are implemented, with 74 passing automated tests and the isolated GA1 checks described below. Collector-observation Tehran timestamps are available. GA1 broker deal event conversions remain unverified and null; Tehran event-day reports and executable query plans remain gated on independently verified event/query mappings. **M3 is not operationally verified for Tehran trade-day reports.** The measured PC clock skew (about 44.7 seconds ahead of independent UTC during diagnostics) remains uncorrected, and stored collector UTC values retain their original PC-clock basis.

Operational follow-up: confirm broker/MetaQuotes deal-epoch and query-boundary semantics and historical DST rules; compare exact tickets in Deals view, including commission/swap/fee where available; verify clock synchronization under operator control; then validate a narrowly scoped private mapping and Tehran daily boundaries before enabling ongoing history. Populated active-order verification, induced live failures/connectivity recovery, corrections/omissions, and EA health remain pending live checks. History stays disabled by default, and the disposable verification start is not the ongoing GA1 history-start decision.

History is **disabled by default**. Existing M1/M2 configuration files continue to work and issue no history requests. To opt in, add `HISTORY_ENABLED=true` and an operator-chosen `HISTORY_START_UTC` to the ignored private configuration. The start must be an explicit UTC timestamp at whole-second precision, such as `2026-01-01T00:00:00Z`; this is an example, not GA1's chosen start. A start is required only when enabled. No credentials are needed, and the recorder never logs in, switches accounts, or submits trades. Historical orders and financial performance calculations are outside M3.

Before upgrading, stop the recorder and take a consistent SQLite backup. Schema v3 upgrades v1 or v2 transactionally, retaining account observations, cycles, active batches, status events and source binding. Unknown newer versions are rejected. The next `record` command upgrades the schema even if history is disabled; `check` does not open or migrate the database. On a migration/integrity failure, preserve both the original database and backup for investigation.

The first enabled start binds the configured history start to the database. Changing that start later is rejected without resetting progress. Restore the original start and use an explicit reconciliation range to inspect other dates, including earlier dates. Disabling history pauses it without discarding state; re-enabling with the same start resumes it. Pending reconciliation jobs survive restart. Repeating an unfinished identical reconciliation request resumes that job; repeating a completed range creates a new reconciliation pass.

The existing collector owns every MT5 call sequentially under its terminal lock. After a valid live cycle is saved, it can run one history query in an available slot. Round-robin selection between initial import, recent refresh, and explicit reconciliation prevents starvation. All workloads use bounded date chunks, including recent refresh. Live intervals use the remaining time after work rather than adding a fresh five-second wait. Synchronous MT5 requests can still delay live collection: there is **no hard five-second guarantee**, and overruns are reported.

| Configuration | Default | Meaning |
|---|---:|---|
| `HISTORY_ENABLED` | `false` | Opt in to history reads |
| `HISTORY_START_UTC` | none | Required UTC start when enabled |
| `HISTORY_CHUNK_SECONDS` | 86400 | Maximum whole API query span |
| `HISTORY_OVERLAP_SECONDS` | 300 | Re-query prior boundary data; capped at half the effective chunk |
| `HISTORY_LOOKBACK_SECONDS` | 86400 | Recent-history refresh range |
| `HISTORY_REFRESH_SECONDS` | 60 | Delay before starting another completed recent sweep |
| `HISTORY_SLOT_SECONDS` | 30 | Minimum gap between history requests, measured after completion |
| `HISTORY_RETRY_SECONDS` / `HISTORY_RETRY_MAX_SECONDS` | 30 / 300 | Exponential retry bounds per job |
| `HISTORY_SAFETY_LAG_SECONDS` | 10 | Keep query endpoints behind the current UTC time |
| `HISTORY_SLOW_SECONDS` | 1 | Slow-request threshold |
| `HISTORY_MIN_CHUNK_SECONDS` | 300 | Smallest adaptive chunk |
| `HISTORY_SLOW_LIMIT` | 3 | Consecutive slow calls at minimum chunk before suspension |
| `HISTORY_MAX_RECORDS` | 10000 | Reject oversized responses without accepting partial batches |

Initial import advances oldest first. Once caught up, it continues forward from its durable cursor, so a long offline gap is not skipped merely because it exceeds the recent lookback. Recent refresh repeatedly sweeps its captured range in chunks. One query runs per slot without catch-up bursts. Failed jobs back off individually, leaving other workloads eligible. Slow requests halve the effective chunk toward its minimum; repeatedly slow requests at that minimum suspend history. Oversized batches also shrink chunks, and suspend history if even the minimum range is too dense. Progress never advances for those failures. Adaptive chunk size and suspension survive restart; `record --resume-history` explicitly clears suspension. Configuration bounds clamp the retained adaptive chunk when restarting.

Each query checks account/server/demo identity and connectivity before and after the request. A detected identity change rejects the response and stops the recorder. Ordinary history failures retain valid live observations and are recorded in `history_queries`, not as live-read failures. Failed query counts and retry timing are in `history_jobs`; adaptive suspension is in `history_state`. Inspect these separately from live cycle health. Storage failures are fatal and roll back accepted-query work.

Successful empty responses are recorded with `outcome=success` and `item_count=0`, and can advance the cursor. `None`, exceptions, disconnections, malformed or oversized results are failures with no accepted membership and no progress advance. All returned deal fields and unknown operation types are retained, including non-trading balance/credit/charge/commission/correction operations and records with empty symbols or zero volume. Ticket, order, magic and position identifiers are exact decimal strings in storage payloads and exports, including JSON for JavaScript consumers. Broker event seconds/milliseconds remain integers; query collection times are UTC. Broker event time and collector observation time have different meanings.

Current deals are keyed by source and exact ticket. Identical responses do not create duplicate deals or revisions. Changed payloads append immutable revisions and update the current version, retaining the earlier payload and originating query. Query membership records the exact returned ticket/revision set. Successful queries record omission evidence for previously seen deals in the conservative interior of the range; boundary seconds are excluded. Missing records are retained with a missing flag, and later returns append reappearance evidence. Absence never implies deletion, cancellation, or a realized financial result. A success only establishes that a range was queried successfully: it cannot prove broker history is complete, especially before the earliest returned record. Recent refresh cannot detect arbitrary older corrections; explicit reconciliation can detect only changes exposed in the queried ranges, and cannot reconstruct intermediate versions overwritten between reads.

Normal commands remain unchanged. Examples below use illustrative dates only; keep private exports outside Git:

```powershell
.\.venv\Scripts\python.exe .\collector.py check
.\.venv\Scripts\python.exe .\collector.py record
.\.venv\Scripts\python.exe .\collector.py record --reconcile-from 2026-01-01T00:00:00Z --reconcile-to 2026-01-03T00:00:00Z
.\.venv\Scripts\python.exe .\collector.py record --resume-history
.\.venv\Scripts\python.exe .\collector_inspect.py C:\private\collector.sqlite3 --history-table history_jobs --format json
.\.venv\Scripts\python.exe .\collector_inspect.py C:\private\collector.sqlite3 --history-table history_queries --format json --limit 100
.\.venv\Scripts\python.exe .\collector_inspect.py C:\private\collector.sqlite3 --history-table deals --from-utc 2026-01-01T00:00:00Z --to-utc 2026-01-03T00:00:00Z --limit 1000 --format json --output C:\private\deals.json
.\.venv\Scripts\python.exe .\collector_inspect.py C:\private\collector.sqlite3 --history-table deal_revisions --format csv --output C:\private\revisions.csv
```

Offline inspection supports `deals`, `deal_revisions`, `history_queries`, `history_query_members`, `history_jobs`, `history_state`, and `history_discrepancies`. It imports no MT5 extension and opens the database read-only. Limits default to 1000 rows (maximum 100000). Date filters are half-open. Legacy `--from-utc`/`--to-utc` deal/revision filters compare literal epoch numbers; they are not evidence of physical UTC event semantics. Query/job/membership filters use the requested numeric range start; discrepancy filters use collector observation time; state filters use configured history start. Verified Tehran civil-day filtering uses `--report-date` as described below. Membership, coverage, revisions, retry state and omission evidence can all be exported. A row limit may truncate output and is not a completeness claim.

M3 automated verification passed 61 tests in a bounded full-suite run, using fake MT5 responses, controlled clocks and temporary databases. The ongoing GA1 database has not been migrated for M3. For a short GA1 check: stop recording, verify the running intended terminal, create and integrity-check a SQLite backup, choose the history start explicitly, and enable history in private configuration. Run `check`, then record a few slots over a short chosen range. Compare deals with the terminal History tab (tickets, operation types, event times and amounts), including a non-trading operation if available. Restart, repeat/reconcile the same range, and confirm no duplicate deals or unchanged revisions; inspect offline exports and retained M1/M2 history. Do not place trades or change account/network settings to manufacture test data. Live corrections, omissions and absent operation types remain pending until safely observed.

An isolated GA1 live check on 29 October 2026 used a fresh private test database and a disposable seven-day start, with one-second history slots and the normal five-second live interval. It stored 54 deals/revisions, completed the requested initial range, reconciled one day without duplicates or new revisions, and retained history across restarts. Thirty-three live cycles and history queries succeeded. Within-run cycle spacing was 5.0004–5.0155 seconds, and history requests took 0.0036–0.0059 seconds. Offline JSON/CSV counts and exact identifier strings matched storage; hashes confirmed that the original GA1 configuration/database were unchanged. Four visible balance-deal tickets, types, timestamps and amounts matched the History tab, and a closed-position profit matched its linked exit deal. Commission/swap/fee comparisons were not available in the visible columns. UTC event interpretation remains unresolved: the two open positions had raw event timestamps later than the UTC query cutoff and no linked deals in the imported set. The queried range is therefore not a claim of complete recent GA1 activity. Confirm the terminal/broker history timezone and query-boundary mapping before treating this as ongoing UTC-correct synchronization. The disposable start is not GA1's final history-start decision.

Follow-up diagnostics on 30 October located both opening deals using exact position identifiers and a bounded range around their raw event values. UTC-aware datetimes, equivalent UTC+03:30-aware datetimes, and integer epoch arguments returned identical ticket sets. Repeating the old cutoff returned neither deal; widened diagnostic queries returned both, and M3 validation accepted both without discarding a record. Position creation/update timestamps and linked deal timestamps agreed, so identifier linkage, update-time confusion and millisecond conversion do not explain the exclusion. The immediate cause is that the returned event values fall after the old request cutoff. The absolute UTC meaning of those broker/terminal event values remains unconfirmed; no fixed offset was inferred or applied. Windows reported an unsynchronized Local CMOS Clock, although its current UTC was close to an independent UTC reading. The existing History display was Positions, not Deals; an exact-ticket comparison in Deals view and broker clarification of historical timestamp/query semantics remain pending. These diagnostic results were kept separate from synchronization storage; original configuration, databases and cursors were unchanged.

## Reporting timezone and broker event-time verification

Business reports, report dates and display timestamps use **IANA `Asia/Tehran`**, never the Windows local timezone. `tzdata==2026.6` is a pinned project dependency and supplies Windows timezone data to Python `zoneinfo`. Install with the existing venv command above. Recorder configuration checks that `Asia/Tehran` can be loaded before connecting. Collector console timestamps display Tehran time alongside the original collector UTC timestamp. Exports add explicit Tehran/UTC collector-observation fields without changing stored values. The PC's clock accuracy remains distinct from its timezone.

There are three separate time concepts: untouched MT5 broker event values (`time`, `time_msc`, update/setup/done fields), collector observation UTC as recorded by the PC, and derived Tehran report time. Alpari's documented GMT+2 winter/GMT+3 DST server display policy is evidence about its server clock, not proof of Python deal-epoch or query-boundary semantics. No code automatically subtracts a server offset. Raw stored history and existing collector timestamps are never shifted or rewritten.

Broker conversions default to `status=unverified`, with `event_utc=null` and `event_tehran=null`. `raw_epoch_as_utc_candidate` is explicitly a literal numeric interpretation for investigation, not an authoritative event instant. Query/job progress retains its raw numbers and `*_request_argument` ISO representation; physical `*_utc` and `*_tehran` fields remain null unless the query time basis is separately verified. This also prevents a successful numeric query cursor from being presented as proof of all real-world activity through a UTC instant.

The safe `time-mapping.example.json` leaves both bases unverified. An operator may supply a separate private mapping file to the offline inspector only after obtaining evidence. Its fields are `event_basis` and `query_basis` (each `unverified`, `utc`, or `server_wall`), `server_zone` (an evidenced IANA zone for server-wall mappings), `evidence_ref`, `valid_from_utc`, and `valid_to_utc` (exclusive). Verified bases require both an evidence reference and a bounded validity period. Event and query bases are independent: proving one does not prove the other. No mapping file is selected implicitly from Windows settings or inherited environment variables.

For an evidenced UTC epoch, convert directly to UTC and then Tehran, without a broker-offset subtraction. For an evidenced server-wall epoch, resolve the raw calendar value in the verified broker zone **on that event date**, then convert that instant to UTC and Tehran. IANA historical rules provide winter/summer differences; today's offset is never reused for all historical records. Ambiguous fall-back times, nonexistent spring-forward times, and dates outside the evidence period remain explicitly unresolved, with no authoritative converted event time. Repeated decoration always derives from raw input, preventing double conversion. Test fixtures use `Europe/Helsinki` to exercise +2/+3 transitions; this is not a verified zone choice for GA1.

`--report-date YYYY-MM-DD` selects a Tehran civil day for deals/revisions. Its boundaries are local midnight to the following local midnight, translated to UTC with `Asia/Tehran` rules for those dates. Historical skipped midnights use the first valid instant of the civil date. Verified event mapping is required to filter broker records into that day; unresolved fold records remain labelled with unverified day membership. UTC and raw fields remain present in JSON/CSV output. No financial performance metrics are calculated.

`--query-plan YYYY-MM-DD` is an offline dry run: it translates the same Tehran daily boundaries into a **separately verified query basis**. When that basis is unverified it returns `query_status=unverified` and no executable query segments. For verified server-wall queries it splits ranges at broker-zone DST transitions and labels the UTC-aware argument carriers separately from physical UTC instants. Fall-back argument ranges may overlap; exact ticket deduplication and event-time checks remain necessary. Query plans do not execute MT5 calls or alter synchronization progress, and this change does not silently change the collector's existing numeric history-query policy.

```powershell
# Preview the Tehran day. Query arguments remain unavailable without evidence.
.\.venv\Scripts\python.exe .\collector_inspect.py --query-plan 2026-07-02
# After independently verifying an appropriate private mapping:
.\.venv\Scripts\python.exe .\collector_inspect.py C:\private\collector.sqlite3 --history-table deals --report-date 2026-07-02 --time-mapping C:\private\time-mapping.local.json --format json --output C:\private\tehran-day.json
.\.venv\Scripts\python.exe .\collector_inspect.py --query-plan 2026-07-02 --time-mapping C:\private\time-mapping.local.json
```

Fresh clock diagnostics on 30 October sampled changing quotes rather than assuming the last cached quote was current. Four paired gold-tick/SNTP observations had four different tick timestamps. Their literal epoch values were 7199.748–7199.956 seconds ahead of independent Cloudflare SNTP UTC, supporting today's approximately GMT+2 quote/server-clock relationship. At the same instants the PC was 44.676–44.683 seconds ahead of SNTP; independent HTTPS Date responses corroborated that skew. Windows reported unsynchronized Local CMOS Clock. An initial cached Market Watch clock reading was discarded; a refreshed advancing display agreed with the fresh quote-clock relationship. The original PC clock was not changed. This evidence does not establish historical Python deal semantics or a universal offset; broker event/query conversions remain unverified for GA1.

Before approving a GA1 mapping, obtain broker/MetaQuotes confirmation for the exact returned deal fields and the date-range query basis, including historical DST transition rules and their applicability to this server/build. Compare exact deal tickets in the terminal's Deals view with an independently timestamped event record if available. Verify clock synchronization separately under operator control. Do not enable a guessed conversion simply because the current server display matches GMT+2/GMT+3.

Timezone verification passed 74 tests in a 30-second-bounded full-suite run. Coverage includes winter/summer server examples, historical Tehran DST, midnight/date rollover, server DST folds/gaps, separate event/query bases, half-open Tehran day filters, unchanged raw payloads, prevention of double conversion, and offline JSON/CSV output. The original GA1 configuration/database and Windows clock were unchanged; no GA1 event/query policy has been declared verified.

References: [Alpari server/platform-time policy](https://alpariworld.com/s3-static/2025-06/terms-of-business-alpari-jun-2025.pdf), [MetaQuotes deal properties](https://www.mql5.com/en/docs/constants/tradingconstants/dealproperties), [Python zoneinfo and Windows timezone-data guidance](https://docs.python.org/3/library/zoneinfo.html).
