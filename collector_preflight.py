"""Offline validation of a proposed independent-source configuration set."""

import argparse
import os
import sqlite3
from pathlib import Path

from collector_storage import SCHEMA_VERSION
from mt5_monitor import DiagnosticError
from recorder_config import load_recorder_config
from terminal_lock import canonical_path


def check_configuration_set(paths):
    errors, notes, loaded = [], [], []
    for index, path in enumerate(paths, 1):
        label = f'configuration {index}'
        try:
            config = load_recorder_config(path)
        except DiagnosticError as exc:
            errors.append(f'{label}: {exc}')
            continue
        except OSError:
            errors.append(f'{label}: configuration cannot be read')
            continue
        loaded.append((label, config))
        if config.history.enabled:
            errors.append(f'{label}: HISTORY_ENABLED must be false during three-source preparation')
        if config.data_path is None:
            errors.append(f'{label}: MT5_DATA_PATH is required for this preparation preflight')
        if not config.database_path.parent.is_dir() or not os.access(config.database_path.parent, os.W_OK):
            errors.append(f'{label}: database parent must exist and permit writing')
        if config.database_path.is_file():
            journals = [Path(str(config.database_path) + suffix) for suffix in ('-wal', '-journal')]
            if any(p.exists() and p.stat().st_size for p in journals):
                errors.append(f'{label}: SQLite journal state is present; stop its writer and inspect before preflight')
                continue
            try:
                # Immutable read: preparation must not create journal/SHM files.
                # Run preflight with collectors stopped; active data is not inspected.
                db = sqlite3.connect(config.database_path.resolve().as_uri() + '?mode=ro&immutable=1', uri=True)
                try:
                    version = db.execute('PRAGMA user_version').fetchone()[0]
                    rows = db.execute('SELECT fund_id,source_id,login,server,currency FROM source').fetchall()
                    expected = (config.fund_id, config.source_id, config.mt5.expected_login, config.mt5.expected_server)
                    if len(rows) != 1 or rows[0][:4] != expected or not rows[0][4]:
                        errors.append(f'{label}: existing database source/account/server binding differs')
                    if version not in (1, 2, SCHEMA_VERSION):
                        errors.append(f'{label}: unsupported database schema version')
                    elif version < SCHEMA_VERSION:
                        notes.append(f'{label}: database schema v{version}; stopped backup and approved v{SCHEMA_VERSION} upgrade required before record')
                finally:
                    db.close()
            except sqlite3.Error:
                errors.append(f'{label}: existing database binding could not be verified; preserve the file')
        else:
            notes.append(f'{label}: new database path; no file created by preflight')
    fields = (
        ('terminal path', lambda c: canonical_path(c.mt5.terminal_path)),
        ('database path', lambda c: canonical_path(c.database_path)),
        ('data directory', lambda c: canonical_path(c.data_path) if c.data_path else None),
        ('fund_id', lambda c: c.fund_id.casefold()),
        ('source_id', lambda c: c.source_id.casefold()),
        ('account/server', lambda c: (c.mt5.expected_login, c.mt5.expected_server)),
    )
    for name, key in fields:
        seen = {}
        for label, config in loaded:
            value = key(config)
            if value is None:
                continue
            if value in seen:
                errors.append(f'Duplicate {name} between {seen[value]} and {label}')
            seen[value] = label
    for label, config in loaded:
        if any(canonical_path(config.database_path) == canonical_path(other.mt5.terminal_path) for _, other in loaded):
            errors.append(f'{label}: database path conflicts with a terminal executable')
    return errors, notes


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', action='append', type=Path, required=True)
    args = parser.parse_args(argv)
    errors, notes = check_configuration_set(args.config)
    for note in notes:
        print('NOTE: ' + note)
    for error in errors:
        print('ERROR: ' + error)
    if errors:
        print('Preflight failed; complete and recheck the entire configuration set. No MT5 connection or database migration was performed.')
        return 1
    print(f'Offline preflight passed for {len(args.config)} configurations. Operator must still verify running terminals, actual data paths, and demo accounts.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
