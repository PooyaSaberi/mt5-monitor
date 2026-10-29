"""Read-only, durable recorder for one MT5 demo terminal."""

import argparse
import sys
import threading
from pathlib import Path

from account_reader import read_account, utc_now
from collector_storage import Storage, StorageError
from mt5_monitor import AccountGuardError, DiagnosticError
from recorder_config import load_recorder_config
from terminal_lock import TerminalLock


def run(config, mt5, *, check_only=False, stop=None, output=print):
    stop = stop or threading.Event()
    lock = TerminalLock(config.mt5.terminal_path)
    storage = None
    initialized = False
    failures = 0
    try:
        while not stop.is_set():
            started = utc_now()
            try:
                if not initialized:
                    if not mt5.initialize(path=str(config.mt5.terminal_path)):
                        raise DiagnosticError('MT5 initialization failed')
                    initialized = True
                observation = read_account(config.mt5, mt5)
                if check_only:
                    output('Connection and demo account verified; currency: ' + observation.currency)
                    return 0
                identity = (config.fund_id, config.source_id, config.mt5.expected_login,
                            config.mt5.expected_server, observation.currency)
                if storage is None:
                    storage = Storage(config.database_path, identity)
                saved_id = storage.save_observation(observation)
                output(f'Saved observation {saved_id} at {observation.ended_at}')
                failures = 0
                stop.wait(config.mt5.refresh_interval)
            except AccountGuardError as exc:
                if storage is not None:
                    storage.save_status(started, utc_now(), 'stopped_identity_mismatch', str(exc))
                raise
            except Exception as exc:
                # Database failures are fatal; only MT5 read failures are retried.
                if not isinstance(exc, DiagnosticError) or isinstance(exc, StorageError):
                    raise
                if check_only:
                    raise
                failures += 1
                if storage is not None:
                    storage.save_status(started, utc_now(), 'retrying', str(exc))
                output(f'Collection retrying: {exc}')
                mt5.shutdown()
                initialized = False
                stop.wait(min(2 ** (failures - 1), 30))
    finally:
        mt5.shutdown()
        if storage is not None:
            storage.close()
        lock.close()
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('check', 'record'))
    parser.add_argument('--config', type=Path, default=Path(__file__).with_name('.env'))
    args = parser.parse_args(argv)
    try:
        config = load_recorder_config(args.config)
        import MetaTrader5 as mt5
        return run(config, mt5, check_only=args.command == 'check')
    except KeyboardInterrupt:
        print('Collector stopped.')
        return 0
    except Exception as exc:
        print(f'Collector stopped: {exc}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
