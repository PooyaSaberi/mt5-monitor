"""Sequential, read-only MT5 monitoring with in-memory stale-state handling."""

from __future__ import annotations

import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from mt5_monitor import (
    AccountGuardError,
    Config,
    DiagnosticError,
    _masked_login,
    _mt5_error,
    checked_account,
    load_config,
)


@dataclass(frozen=True)
class Position:
    ticket: int
    symbol: str
    direction: str
    volume: float
    opening_price: float
    current_price: float
    floating_pl: float
    stop_loss: float
    take_profit: float


@dataclass(frozen=True)
class Snapshot:
    collected_at: datetime
    masked_login: str
    server: str
    currency: str
    balance: float
    equity: float
    floating_pl: float
    margin: float
    free_margin: float
    positions: tuple[Position, ...]


@dataclass(frozen=True)
class Update:
    status: str  # LIVE, STALE/RETRYING, or UNAVAILABLE/RETRYING
    connected: bool | None
    snapshot: Snapshot | None
    age_seconds: float | None
    error: str | None


def collect(
    config: Config,
    mt5: object,
    now: Callable[[], datetime],
    connection_seen: Callable[[bool], None] = lambda connected: None,
) -> Snapshot:
    terminal = mt5.terminal_info()
    if terminal is None:
        raise DiagnosticError(f"Could not read terminal information ({_mt5_error(mt5)}).")
    connection_seen(bool(terminal.connected))
    if not terminal.connected:
        raise DiagnosticError("The MT5 terminal is not connected to the broker.")

    account = checked_account(config, mt5)
    positions = mt5.positions_get()
    if positions is None:
        raise DiagnosticError(f"Could not read open positions ({_mt5_error(mt5)}).")
    checked_account(config, mt5)  # Reject an account switch during the reads.

    if any(position.type not in (mt5.POSITION_TYPE_BUY, mt5.POSITION_TYPE_SELL) for position in positions):
        raise DiagnosticError("MT5 returned an unknown position direction.")
    converted = tuple(
        Position(
            ticket=position.ticket,
            symbol=position.symbol,
            direction="BUY" if position.type == mt5.POSITION_TYPE_BUY else "SELL",
            volume=position.volume,
            opening_price=position.price_open,
            current_price=position.price_current,
            floating_pl=position.profit,
            stop_loss=position.sl,
            take_profit=position.tp,
        )
        for position in positions
    )
    return Snapshot(
        collected_at=now(),
        masked_login=_masked_login(account.login),
        server=account.server,
        currency=account.currency,
        balance=account.balance,
        equity=account.equity,
        floating_pl=account.profit,
        margin=account.margin,
        free_margin=account.margin_free,
        positions=converted,
    )


def format_update(update: Update) -> str:
    connection = "unknown" if update.connected is None else ("connected" if update.connected else "disconnected")
    lines = [f"{update.status} | MT5 {connection}"]
    if update.error:
        lines.append(f"Error: {update.error}")
    snapshot = update.snapshot
    if snapshot is None:
        lines.append("Account and positions: unavailable")
        return "\n".join(lines)
    lines.append(f"Last successful collection: {snapshot.collected_at:%Y-%m-%d %H:%M:%S} UTC")
    if update.age_seconds is not None:
        lines.append(f"Snapshot age: {update.age_seconds:.1f}s")
    lines.append(f"Account: {snapshot.masked_login} | Server: {snapshot.server} | Currency: {snapshot.currency}")
    lines.append(
        f"Balance: {snapshot.balance:.2f} | Equity: {snapshot.equity:.2f} | "
        f"Floating P/L: {snapshot.floating_pl:.2f} | Margin: {snapshot.margin:.2f} | "
        f"Free: {snapshot.free_margin:.2f}"
    )
    lines.append(f"Open positions: {len(snapshot.positions)}")
    for position in snapshot.positions:
        lines.append(
            f"  #{position.ticket} {position.symbol} {position.direction} "
            f"{position.volume:g} | Open: {position.opening_price:g} | "
            f"Current: {position.current_price:g} | P/L: {position.floating_pl:.2f} | "
            f"SL: {position.stop_loss:g} | TP: {position.take_profit:g}"
        )
    return "\n".join(lines)


def run_monitor(
    config: Config,
    mt5: object,
    *,
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    wait: Callable[[float], None] = time.sleep,
    display: Callable[[str], None] = print,
) -> int:
    snapshot: Snapshot | None = None
    initialized = False
    try:
        while True:
            connection_seen: list[bool | None] = [None]
            try:
                if not initialized:
                    if not mt5.initialize(path=str(config.terminal_path)):
                        raise DiagnosticError(f"MT5 initialization failed ({_mt5_error(mt5)}).")
                    initialized = True
                snapshot = collect(config, mt5, now, lambda connected: connection_seen.__setitem__(0, connected))
                display(format_update(Update("LIVE", True, snapshot, 0.0, None)))
                wait(config.refresh_interval)
            except AccountGuardError:
                raise
            except Exception as exc:
                # MT5 extension errors are retryable, but their raw text may expose local details.
                error = str(exc) if isinstance(exc, DiagnosticError) else "MT5 read failed"
                age = None if snapshot is None else max(0.0, (now() - snapshot.collected_at).total_seconds())
                status = "UNAVAILABLE/RETRYING" if snapshot is None else "STALE/RETRYING"
                display(format_update(Update(status, connection_seen[0], snapshot, age, error)))
                mt5.shutdown()
                initialized = False
                wait(min(max(config.refresh_interval, 1.0), 5.0))
    except AccountGuardError as exc:
        display(f"MONITOR STOPPED: {exc}")
        return 1
    except KeyboardInterrupt:
        display("Monitor stopped by Ctrl+C.")
        return 0
    finally:
        mt5.shutdown()


def main() -> int:
    try:
        config = load_config(Path(__file__).with_name(".env"))
        import MetaTrader5 as mt5

        return run_monitor(config, mt5)
    except (DiagnosticError, OSError) as exc:
        print(f"Monitor failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
