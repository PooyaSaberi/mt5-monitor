"""Guarded historical deal reads without owning an MT5 connection."""

import json
from datetime import datetime, timezone

from mt5_monitor import AccountGuardError, DiagnosticError, checked_account

IDENTIFIERS = frozenset(('ticket', 'order', 'position_id', 'magic', 'identifier'))
REQUIRED_FIELDS = frozenset('ticket order time time_msc type entry magic position_id reason volume price commission swap profit fee symbol comment external_id'.split())


class HistoryBatchTooLarge(DiagnosticError):
    pass


def parse_utc(value):
    try:
        result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except (ValueError, AttributeError) as exc:
        raise DiagnosticError('History timestamps must be explicit UTC timestamps.') from exc
    if 'T' not in value or result.microsecond or result.utcoffset() is None or result.utcoffset().total_seconds() != 0:
        raise DiagnosticError('History timestamps must be explicit UTC timestamps.')
    return result.astimezone(timezone.utc)


def read_deals(config, mt5, start, end, max_records=10000, expected_currency=None):
    if start.utcoffset() is None or end.utcoffset() is None or start.utcoffset().total_seconds() != 0 or end.utcoffset().total_seconds() != 0 or start >= end:
        raise DiagnosticError('Invalid history query interval.')
    terminal = mt5.terminal_info()
    if terminal is None or not terminal.connected:
        raise DiagnosticError('History terminal is disconnected.')
    before = checked_account(config, mt5)
    if expected_currency is not None and before.currency != expected_currency:
        raise AccountGuardError('Account currency differs from the bound source.')
    failed = False
    try:
        result = mt5.history_deals_get(start, end)
    except Exception:
        result = None
        failed = True
    after = checked_account(config, mt5)
    if before.currency != after.currency:
        raise AccountGuardError('Account currency changed during history collection.')
    terminal = mt5.terminal_info()
    if terminal is None or not terminal.connected:
        raise DiagnosticError('History terminal disconnected during collection.')
    if failed or result is None:
        raise DiagnosticError('Historical deal read failed.')
    rows = []
    seen = set()
    try:
        for item in result:
            if len(rows) >= max_records:
                raise HistoryBatchTooLarge('History batch exceeds configured record limit')
            row = dict(item._asdict())
            if not REQUIRED_FIELDS.issubset(row):
                raise ValueError('Missing deal fields')
            if type(row['ticket']) is not int or row['ticket'] <= 0 or row['ticket'] in seen:
                raise ValueError('Invalid or duplicate ticket')
            seen.add(row['ticket'])
            for key in row:
                if key in IDENTIFIERS or (key.endswith(('_id', '_ticket')) and key != 'external_id'):
                    if type(row[key]) is not int:
                        raise ValueError('Invalid identifier')
                    row[key] = str(row[key])
            if type(row['time']) is not int or type(row['time_msc']) is not int or type(row['type']) is not int:
                raise ValueError('Invalid event time')
            if row['time_msc'] // 1000 != row['time'] or not int(start.timestamp()) * 1000 <= row['time_msc'] < (int(end.timestamp()) + 1) * 1000:
                raise ValueError('Event time outside query')
            if any(type(row[key]) is not int for key in ('type', 'entry', 'reason')):
                raise ValueError('Invalid deal enumeration')
            if any(type(row[key]) not in (int, float) for key in ('volume', 'price', 'commission', 'swap', 'profit', 'fee')):
                raise ValueError('Invalid financial field')
            if any(not isinstance(row[key], str) for key in ('symbol', 'comment', 'external_id')):
                raise ValueError('Invalid text field')
            json.dumps(row, allow_nan=False)
            rows.append(row)
    except (AttributeError, KeyError, ValueError, TypeError, OverflowError) as exc:
        raise DiagnosticError('Historical deal batch is malformed.') from exc
    return tuple(rows)
