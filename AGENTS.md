# MT5 Monitor project rules

- Use `.venv\Scripts\python.exe` for Python and package installation. Never install project dependencies globally.
- Monitoring only: never submit, modify, or cancel orders.
- Never call `mt5.login()` or intentionally switch terminal accounts.
- Never commit credentials, actual account identifiers, local terminal paths, logs, or databases.
- Keep the implementation small and understandable.
- Future scope: FastAPI, a plain HTML/CSS/JavaScript dashboard, and SQLite.
- Do not implement future milestones without instruction.
