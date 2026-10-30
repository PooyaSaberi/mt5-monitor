"""Configuration for the durable, single-terminal recorder."""

from dataclasses import dataclass
from pathlib import Path

from mt5_monitor import Config, DiagnosticError, load_config
from history_config import HistoryConfig, load_history_config
from report_time import zone, REPORT_ZONE


@dataclass(frozen=True)
class RecorderConfig:
    mt5: Config
    fund_id: str
    source_id: str
    database_path: Path
    history: HistoryConfig = HistoryConfig()
    data_path: Path | None = None


def read_values(path):
    """Selected file only; reject duplicate keys instead of choosing a winner."""
    if not path.is_file():
        raise DiagnosticError('Configuration file is missing')
    values = {}
    for line in path.read_text(encoding='utf-8-sig').splitlines():
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            key, value = line.split('=', 1)
            key = key.strip()
            if key in values:
                raise DiagnosticError(f'Duplicate configuration key: {key}')
            values[key] = value.strip().strip('"').strip("'")
    return values


def load_recorder_config(path: Path = Path('.env')) -> RecorderConfig:
    zone(REPORT_ZONE)
    values = read_values(path)
    for key, value in values.items():
        if value.upper().startswith('UNRESOLVED'):
            raise DiagnosticError(f'Unresolved configuration value: {key}')
    base = load_config(path)
    if not base.terminal_path.is_absolute():
        raise DiagnosticError('MT5_TERMINAL_PATH must be absolute')
    for key in ('FUND_ID', 'SOURCE_ID', 'COLLECTOR_DB_PATH'):
        if not values.get(key):
            raise DiagnosticError(f'Missing required .env value: {key}')
    for key in ('FUND_ID', 'SOURCE_ID'):
        if any(c in values[key] for c in '\r\n\t'):
            raise DiagnosticError(f'{key} contains invalid whitespace')
    db = Path(values['COLLECTOR_DB_PATH']).expanduser()
    if not db.is_absolute():
        raise DiagnosticError('COLLECTOR_DB_PATH must be absolute')
    if db.is_dir() or db.resolve() == base.terminal_path.resolve():
        raise DiagnosticError('COLLECTOR_DB_PATH must be a separate database file')
    data_path = Path(values['MT5_DATA_PATH']).expanduser() if values.get('MT5_DATA_PATH') else None
    if data_path is not None and (not data_path.is_absolute() or not data_path.is_dir()):
        raise DiagnosticError('MT5_DATA_PATH must be an existing absolute directory')
    interval = base.refresh_interval if 'MT5_REFRESH_INTERVAL' in values else 5.0
    base = Config(base.terminal_path, base.expected_login, base.expected_server, interval)
    return RecorderConfig(base, values['FUND_ID'], values['SOURCE_ID'], db, load_history_config(values), data_path)
