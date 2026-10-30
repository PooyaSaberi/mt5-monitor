"""Read-only, durable recorder for one MT5 demo terminal."""

import argparse
import sys
import threading
import time
from pathlib import Path
from contextlib import ExitStack

from account_reader import read_account, read_active_set, utc_now
from collector_storage import Storage, StorageError
from mt5_monitor import AccountGuardError, DiagnosticError
from recorder_config import load_recorder_config
from terminal_lock import TerminalLock, DatabaseLock, DataDirectoryLock, canonical_path
from report_time import aware_utc, tehran


def run(config, mt5, *, check_only=False, stop=None, output=print,
        reconcile=None, resume_history=False, monotonic=time.monotonic):
    stop = stop or threading.Event()
    lock = TerminalLock(config.mt5.terminal_path)
    ownership = ExitStack()
    ownership.callback(lock.close)
    storage = None
    initialized = False
    failures = 0
    history = None
    try:
        if not check_only:
            database_lock = DatabaseLock(config.database_path)
            ownership.callback(database_lock.close)
        if config.data_path is not None:
            data_lock = DataDirectoryLock(config.data_path)
            ownership.callback(data_lock.close)
        while not stop.is_set():
            cycle_began = monotonic()
            started = utc_now()
            identity_failure = None
            try:
                if not initialized:
                    if not mt5.initialize(path=str(config.mt5.terminal_path)):
                        raise DiagnosticError('MT5 initialization failed')
                    initialized = True
                if config.data_path is not None:
                    terminal = mt5.terminal_info()
                    if terminal is None:
                        raise DiagnosticError('Could not verify MT5 data directory')
                    actual_data_path = getattr(terminal, 'data_path', None)
                    if not actual_data_path or canonical_path(actual_data_path) != canonical_path(config.data_path):
                        raise AccountGuardError('Connected MT5 data directory differs from MT5_DATA_PATH')
                observation = read_account(config.mt5, mt5)
                if check_only:
                    output('Connection and demo account verified; currency: ' + observation.currency)
                    return 0
                identity = (config.fund_id, config.source_id, config.mt5.expected_login,
                            config.mt5.expected_server, observation.currency)
                if storage is None:
                    storage = Storage(config.database_path, identity)
                    if config.history.enabled:
                        from history_sync import HistorySync
                        history = HistorySync(storage, config, reconcile=reconcile,
                                              resume=resume_history, output=output)
                if observation.currency != storage.identity[4]:
                    raise AccountGuardError('Account currency differs from the bound source.')
                batches = []
                for dataset in ('positions', 'orders'):
                    dataset_started = utc_now()
                    try:
                        active = read_active_set(config.mt5, mt5, dataset)
                        batches.append(dict(dataset=dataset, started_at=active.started_at,
                                            ended_at=active.ended_at, outcome='success',
                                            records=active.records))
                    except AccountGuardError as exc:
                        batches.append(dict(dataset=dataset, started_at=dataset_started,
                                            ended_at=utc_now(), outcome='identity_mismatch',
                                            records=(), detail=str(exc)))
                        identity_failure = exc
                        break
                    except DiagnosticError as exc:
                        batches.append(dict(dataset=dataset, started_at=dataset_started,
                                            ended_at=utc_now(), outcome='failed',
                                            records=(), detail=str(exc)))
                    except Exception:
                        batches.append(dict(dataset=dataset, started_at=dataset_started,
                                            ended_at=utc_now(), outcome='failed',
                                            records=(), detail=f'{dataset} read raised an exception'))
                cycle_id = storage.save_cycle(observation, started, utc_now(), batches)
                display_time = tehran(aware_utc(observation.ended_at)).isoformat()
                output(f'Saved cycle {cycle_id} at {display_time} Asia/Tehran (collector UTC {observation.ended_at})')
                if identity_failure:
                    raise identity_failure
                failures = 0
                if history is not None:
                    history.tick(mt5)
                elapsed = monotonic() - cycle_began
                if elapsed > config.mt5.refresh_interval:
                    output(f'Live collection interval overrun: {elapsed:.2f}s')
                stop.wait(max(0, config.mt5.refresh_interval - elapsed))
            except AccountGuardError as exc:
                if storage is not None and not identity_failure:
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
        try:
            mt5.shutdown()
        finally:
            try:
                if storage is not None:
                    storage.close()
            finally:
                ownership.close()
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('check', 'record'))
    parser.add_argument('--config', type=Path, default=Path(__file__).with_name('.env'))
    parser.add_argument('--reconcile-from')
    parser.add_argument('--reconcile-to')
    parser.add_argument('--resume-history', action='store_true')
    args = parser.parse_args(argv)
    try:
        config = load_recorder_config(args.config)
        reconcile = None
        if bool(args.reconcile_from) != bool(args.reconcile_to):
            raise DiagnosticError('Both reconciliation bounds are required')
        if args.reconcile_from:
            from deal_reader import parse_utc
            from datetime import datetime, timezone
            reconcile = (parse_utc(args.reconcile_from), parse_utc(args.reconcile_to))
            if reconcile[0] >= reconcile[1] or reconcile[1] > datetime.now(timezone.utc):
                raise DiagnosticError('Invalid reconciliation range')
        if (reconcile or args.resume_history) and (not config.history.enabled or args.command != 'record'):
            raise DiagnosticError('History options require enabled history and the record command')
        import MetaTrader5 as mt5
        return run(config, mt5, check_only=args.command == 'check',
                   reconcile=reconcile, resume_history=args.resume_history)
    except KeyboardInterrupt:
        print('Collector stopped.')
        return 0
    except Exception as exc:
        print(f'Collector stopped: {exc}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
