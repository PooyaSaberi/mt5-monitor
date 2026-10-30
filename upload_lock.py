"""Uploader-only byte locks; never acquires collector/terminal ownership."""

import hashlib
import os
from pathlib import Path
import tempfile


class UploadLock:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.file = self.path.open('a+b')
        try:
            if self.file.seek(0, 2) == 0:
                self.file.write(b'0'); self.file.flush()
            self.file.seek(0)
            if os.name == 'nt':
                import msvcrt
                msvcrt.locking(self.file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.file.close()
            raise ValueError('Another uploader owns this source or state') from None

    def close(self):
        self.file.close()  # OS releases the byte/file lock, including on process exit.

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def source_lock_path(collector_db):
    key = os.path.normcase(os.path.realpath(collector_db)).casefold().encode()
    return Path(tempfile.gettempdir())/'mt5-uploader-locks'/(hashlib.sha256(key).hexdigest()+'.lock')
