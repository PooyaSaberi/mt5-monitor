"""One-shot, read-only diagnostic for the currently connected MT5 account."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path


class DiagnosticError(Exception):
    """A safe-to-display diagnostic failure."""


@dataclass(frozen=True)
class Config:
    terminal_path: Path
    expected_login: int
    expected_server: str


def load_config(env_file: Path = Path(".env")) -> Config:
    if not env_file.is_file():
        raise DiagnosticError("Missing .env file. Copy .env.example and fill in local values.")

    values: dict[str, str] = {}
    for line in env_file.read_text(encoding="utf-8-sig").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if separator:
            values[key.strip()] = value.strip().strip('"').strip("'")

    required = ("MT5_TERMINAL_PATH", "MT5_EXPECTED_LOGIN", "MT5_EXPECTED_SERVER")
    missing = [key for key in required if not values.get(key)]
    if missing:
        raise DiagnosticError(f"Missing required .env value(s): {', '.join(missing)}")

    terminal_path = Path(values["MT5_TERMINAL_PATH"])
    if not terminal_path.is_file() or terminal_path.suffix.lower() != ".exe":
        raise DiagnosticError("MT5_TERMINAL_PATH must point to an existing terminal executable.")
    try:
        login = int(values["MT5_EXPECTED_LOGIN"])
    except ValueError as exc:
        raise DiagnosticError("MT5_EXPECTED_LOGIN must be a positive account number.") from exc
    if login <= 0:
        raise DiagnosticError("MT5_EXPECTED_LOGIN must be a positive account number.")
    return Config(terminal_path, login, values["MT5_EXPECTED_SERVER"])


def _mt5_error(mt5: object) -> str:
    error = mt5.last_error()
    return f"MT5 error code {error[0]}" if isinstance(error, tuple) and error else "MT5 returned an error"


def _masked_login(login: int) -> str:
    digits = str(login)
    return "*" * max(0, len(digits) - 4) + digits[-4:]


def diagnose(config: Config, mt5: object) -> str:
    try:
        if not mt5.initialize(path=str(config.terminal_path)):
            raise DiagnosticError(f"MT5 initialization failed ({_mt5_error(mt5)}).")

        terminal = mt5.terminal_info()
        if terminal is None:
            raise DiagnosticError(f"Could not read terminal information ({_mt5_error(mt5)}).")
        if not terminal.connected:
            raise DiagnosticError("The MT5 terminal is not connected to the broker.")

        account = mt5.account_info()
        if account is None:
            raise DiagnosticError(f"Could not read account information ({_mt5_error(mt5)}).")
        if account.login != config.expected_login or account.server != config.expected_server:
            raise DiagnosticError("Connected account does not match the expected login and server.")
        if account.trade_mode != mt5.ACCOUNT_TRADE_MODE_DEMO:
            raise DiagnosticError("Connected account is not a demo account.")

        positions = mt5.positions_get()
        if positions is None:
            raise DiagnosticError(f"Could not read open positions ({_mt5_error(mt5)}).")

        return "\n".join(
            (
                f"Account: {_masked_login(account.login)}",
                f"Server: {account.server}",
                f"Currency: {account.currency}",
                f"Balance: {account.balance:.2f}",
                f"Equity: {account.equity:.2f}",
                f"Floating P/L: {account.profit:.2f}",
                f"Margin: {account.margin:.2f}",
                f"Free margin: {account.margin_free:.2f}",
                f"Open positions: {len(positions)}",
            )
        )
    finally:
        mt5.shutdown()


def main() -> int:
    try:
        config = load_config(Path(__file__).with_name(".env"))
        import MetaTrader5 as mt5

        print(diagnose(config, mt5))
        return 0
    except (DiagnosticError, OSError) as exc:
        print(f"Diagnostic failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
