# MT5 Monitor

Local, read-only monitoring prototype for an Alpari demo account. This milestone provides a one-shot account diagnostic. It does not place trades, poll continuously, run a dashboard, or store data.

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
