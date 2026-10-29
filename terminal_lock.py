"""Windows byte-range lock keyed by resolved terminal executable path."""

import hashlib
import os
import tempfile
from pathlib import Path

from mt5_monitor import DiagnosticError


class TerminalLock:
    def __init__(self, terminal_path: Path):
        import msvcrt
        key = os.path.normcase(os.path.realpath(terminal_path)).casefold().encode('utf-8')
        name = hashlib.sha256(key).hexdigest() + '.lock'
        self.path = Path(tempfile.gettempdir()) / 'mt5-monitor-locks' / name
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.file = open(self.path, 'a+b')
        self.file.seek(0)
        self.file.write(b'0')
        self.file.flush()
        self.file.seek(0)
        try:
            msvcrt.locking(self.file.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError as exc:
            self.file.close()
            raise DiagnosticError('Another collector owns this MT5 terminal') from exc

    def close(self):
        import msvcrt
        self.file.seek(0)
        msvcrt.locking(self.file.fileno(), msvcrt.LK_UNLCK, 1)
        self.file.close()
