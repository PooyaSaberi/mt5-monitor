"""Guarded, read-only account and active-set observations."""

from dataclasses import dataclass
from datetime import datetime, timezone
import json

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


@dataclass(frozen=True)
class ActiveSetObservation:
    dataset: str
    started_at: str
    ended_at: str
    records: tuple[dict, ...]


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


def read_active_set(config, mt5, dataset, now=utc_now) -> ActiveSetObservation:
    if dataset not in ('positions', 'orders'):
        raise ValueError('Unknown active dataset')
    started = now()
    terminal = mt5.terminal_info()
    if terminal is None or not terminal.connected:
        raise DiagnosticError('The MT5 terminal is not connected to the broker.')
    before = checked_account(config, mt5)
    failure = None
    try:
        result = getattr(mt5, dataset + '_get')()
    except Exception:
        result = None
        failure = f'{dataset} read raised an exception'
    after = checked_account(config, mt5)
    terminal = mt5.terminal_info()
    if terminal is None or not terminal.connected:
        raise DiagnosticError('The MT5 terminal disconnected during collection.')
    if before.currency != after.currency:
        raise AccountGuardError('Account currency changed during collection.')
    if failure:
        raise DiagnosticError(failure)
    if result is None:
        raise DiagnosticError(f'Could not read {dataset} ({_mt5_error(mt5)}).')
    records = []
    tickets = set()
    try:
        for item in result:
            row = dict(item._asdict())
            ticket = row['ticket']
            if type(ticket) is not int or ticket in tickets:
                raise ValueError('Ticket is invalid or duplicated')
            tickets.add(ticket)
            json.dumps(row, allow_nan=False)
            records.append(row)
    except (AttributeError, KeyError, TypeError, ValueError, OverflowError) as exc:
        raise DiagnosticError(f'{dataset} returned invalid records.') from exc
    return ActiveSetObservation(dataset, started, now(), tuple(records))
