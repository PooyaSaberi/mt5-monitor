"""One guarded account observation; no positions or trading calls."""

from dataclasses import dataclass
from datetime import datetime, timezone

from mt5_monitor import AccountGuardError, DiagnosticError, checked_account, _mt5_error


@dataclass(frozen=True)
class AccountObservation:
    started_at: str
    ended_at: str
    currency: str
    balance: float
    equity: float
    profit: float
    credit: float
    margin: float
    margin_free: float
    margin_level: float


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_account(config, mt5, now=utc_now) -> AccountObservation:
    started = now()
    terminal = mt5.terminal_info()
    if terminal is None:
        raise DiagnosticError(f'Could not read terminal information ({_mt5_error(mt5)}).')
    if not terminal.connected:
        raise DiagnosticError('The MT5 terminal is not connected to the broker.')
    before = checked_account(config, mt5)
    after = checked_account(config, mt5)
    if before.currency != after.currency:
        raise AccountGuardError('Account currency changed during collection.')
    terminal = mt5.terminal_info()
    if terminal is None or not terminal.connected:
        raise DiagnosticError('The MT5 terminal disconnected during collection.')
    try:
        values = [float(getattr(after, key)) for key in
                  ('balance', 'equity', 'profit', 'credit', 'margin', 'margin_free', 'margin_level')]
    except (AttributeError, TypeError, ValueError) as exc:
        raise DiagnosticError('Account financial fields are unavailable.') from exc
    import math
    if not all(math.isfinite(value) for value in values):
        raise DiagnosticError('Account financial fields are not finite.')
    return AccountObservation(started, now(), after.currency, *values)
