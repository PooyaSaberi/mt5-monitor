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


def load_recorder_config(path: Path = Path('.env')) -> RecorderConfig:
    zone(REPORT_ZONE)
    base = load_config(path)
    values = {}
    for line in path.read_text(encoding='utf-8-sig').splitlines():
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            key, value = line.split('=', 1)
            values[key.strip()] = value.strip().strip('"').strip("'")
    for key in ('FUND_ID', 'SOURCE_ID', 'COLLECTOR_DB_PATH'):
        if not values.get(key):
            raise DiagnosticError(f'Missing required .env value: {key}')
    for key in ('FUND_ID', 'SOURCE_ID'):
        if any(c in values[key] for c in '\r\n\t'):
            raise DiagnosticError(f'{key} contains invalid whitespace')
    db = Path(values['COLLECTOR_DB_PATH']).expanduser()
    if not db.is_absolute():
        raise DiagnosticError('COLLECTOR_DB_PATH must be absolute')
    interval = base.refresh_interval if 'MT5_REFRESH_INTERVAL' in values else 5.0
    base = Config(base.terminal_path, base.expected_login, base.expected_server, interval)
    return RecorderConfig(base, values['FUND_ID'], values['SOURCE_ID'], db, load_history_config(values))
