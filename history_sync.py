"""One history query per slot, under the recorder's existing MT5 owner."""

import time
from datetime import datetime, timezone

from deal_reader import read_deals, HistoryBatchTooLarge
from history_storage import HistoryStore
from mt5_monitor import AccountGuardError, DiagnosticError


class HistorySync:
    def __init__(self, storage, config, *, clock=time.time, monotonic=time.monotonic,
                 reconcile=None, resume=False, output=print):
        self.config = config
        self.clock, self.monotonic, self.output = clock, monotonic, output
        self.store = HistoryStore(storage, config.history, clock())
        if reconcile:
            self.store.reconcile(*reconcile)
        if resume:
            self.store.resume()
        self.next_slot = monotonic()

    def tick(self, mt5):
        if self.monotonic() < self.next_slot:
            return
        due = self.store.db.execute('SELECT next_slot_due FROM history_state WHERE source_id=?', (self.store.source,)).fetchone()[0]
        if self.clock() < due:
            return
        # Set the next slot from completion too: a slow call cannot cause a burst.
        self.next_slot = self.monotonic() + self.config.history.slot
        window = self.store.select(self.clock())
        if window is None:
            return
        started = datetime.fromtimestamp(self.clock(), timezone.utc).isoformat()
        began = self.monotonic()
        try:
            rows = read_deals(self.config.mt5, mt5,
                              datetime.fromtimestamp(window[1], timezone.utc),
                              datetime.fromtimestamp(window[2], timezone.utc), self.config.history.max_records,
                              self.store.currency)
        except Exception as exc:
            ended = datetime.fromtimestamp(self.clock(), timezone.utc).isoformat()
            fatal = isinstance(exc, AccountGuardError)
            detail = str(exc) if isinstance(exc, DiagnosticError) else 'History query raised an exception'
            self.store.fail(window, started, ended, self.monotonic() - began,
                            self.clock(), detail, 'identity_mismatch' if fatal else 'failed')
            if isinstance(exc, HistoryBatchTooLarge):
                self.store.shrink()
            self.output('History synchronization failed: ' + detail)
            if fatal:
                raise
        else:
            ended = datetime.fromtimestamp(self.clock(), timezone.utc).isoformat()
            self.store.accept(window, rows, started, ended, self.monotonic() - began, self.clock())
            self.output(f'History query saved: {len(rows)} deals')
        finally:
            self.next_slot = self.monotonic() + self.config.history.slot
        suspended = self.store.db.execute('SELECT suspended FROM history_state WHERE source_id=?', (self.store.source,)).fetchone()[0]
        if suspended:
            self.output('History synchronization suspended after slow requests; inspect history health and use --resume-history when ready.')
