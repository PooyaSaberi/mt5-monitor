"""Optional history settings; existing recorder configurations remain valid."""

from dataclasses import dataclass
from datetime import datetime, timezone
from deal_reader import parse_utc
from mt5_monitor import DiagnosticError


@dataclass(frozen=True)
class HistoryConfig:
    enabled: bool = False
    start: str | None = None
    chunk: int = 86400
    overlap: int = 300
    lookback: int = 86400
    refresh: int = 60
    slot: int = 30
    retry: int = 30
    retry_max: int = 300
    safety_lag: int = 10
    slow_seconds: float = 1.0
    slow_limit: int = 3
    min_chunk: int = 300
    max_records: int = 10000


def load_history_config(values):
    flag = values.get('HISTORY_ENABLED', 'false').lower()
    if flag not in ('true', 'false'):
        raise DiagnosticError('HISTORY_ENABLED must be true or false')
    if flag == 'false':
        return HistoryConfig()
    start = parse_utc(values.get('HISTORY_START_UTC', ''))
    if start > datetime.now(timezone.utc):
        raise DiagnosticError('HISTORY_START_UTC must not be in the future')
    defaults = HistoryConfig()
    settings = {}
    for field in ('chunk', 'overlap', 'lookback', 'refresh', 'slot', 'retry',
                  'retry_max', 'safety_lag', 'slow_limit', 'min_chunk', 'max_records'):
        key = 'HISTORY_' + field.upper() + ('_SECONDS' if field not in ('slow_limit', 'max_records') else '')
        try:
            settings[field] = int(values.get(key, getattr(defaults, field)))
        except ValueError as exc:
            raise DiagnosticError(f'{key} must be an integer') from exc
        if settings[field] <= 0:
            raise DiagnosticError(f'{key} must be positive')
    try:
        settings['slow_seconds'] = float(values.get('HISTORY_SLOW_SECONDS', defaults.slow_seconds))
    except ValueError as exc:
        raise DiagnosticError('HISTORY_SLOW_SECONDS must be positive and finite') from exc
    import math
    if not math.isfinite(settings['slow_seconds']) or settings['slow_seconds'] <= 0:
        raise DiagnosticError('HISTORY_SLOW_SECONDS must be positive and finite')
    if settings['min_chunk'] > settings['chunk'] or settings['retry'] > settings['retry_max']:
        raise DiagnosticError('Invalid history chunk or retry limits')
    return HistoryConfig(True, start.isoformat(), **settings)
