# MT5 Monitor

Local, read-only monitoring prototype for an Alpari demo account. It provides a one-shot diagnostic, a continuous terminal monitor, and a persistent account recorder. It does not place trades.

## Setup

Use Windows with 64-bit Python 3.13 and an existing `.venv`. Install the pinned direct dependency into that environment:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

The installed and verified direct dependency is `MetaTrader5==5.0.6231` (which installs NumPy as a transitive dependency). No broker password is needed.

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

The recorder is independent of the dashboard and reads account totals only. It does not read positions, orders or deals. Use the existing environment to install dependencies:

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
