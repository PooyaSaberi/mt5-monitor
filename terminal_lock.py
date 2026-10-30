"""Windows process ownership of canonical terminal/data/database paths."""

import hashlib
import os
import tempfile
from pathlib import Path

from mt5_monitor import DiagnosticError


def canonical_path(path):
    return os.path.normcase(os.path.realpath(path)).casefold()


class ProcessLock:
    def __init__(self, resource_path: Path, kind, message):
        import msvcrt
        # Preserve the M1/M2/M3 terminal-lock key for mixed-version safety.
        key = canonical_path(resource_path)
        key = (key if kind == 'terminal' else kind + ':' + key).encode('utf-8')
        name = hashlib.sha256(key).hexdigest() + '.lock'
        self.path = Path(tempfile.gettempdir()) / 'mt5-monitor-locks' / name
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.file = open(self.path, 'a+b')
        try:
            if self.file.seek(0, os.SEEK_END) == 0:
                self.file.write(b'0')
                self.file.flush()
            self.file.seek(0)
            msvcrt.locking(self.file.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError as exc:
            self.file.close()
            raise DiagnosticError(message) from exc

    def close(self):
        import msvcrt
        if not self.file.closed:
            try:
                self.file.seek(0)
                msvcrt.locking(self.file.fileno(), msvcrt.LK_UNLCK, 1)
            finally:
                self.file.close()


class TerminalLock(ProcessLock):
    def __init__(self, path):
        super().__init__(path, 'terminal', 'Another collector owns this MT5 terminal')


class DatabaseLock(ProcessLock):
    def __init__(self, path):
        super().__init__(path, 'database', 'Another collector owns this database')


class DataDirectoryLock(ProcessLock):
    def __init__(self, path):
        super().__init__(path, 'data_directory', 'Another collector owns this MT5 data directory')
