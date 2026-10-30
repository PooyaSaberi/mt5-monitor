"""Uploader-only configuration. Never imports collector or MT5 modules."""

from dataclasses import dataclass
import ipaddress
import json
import math
from pathlib import Path
from urllib.parse import urlsplit

from upload_contract import require, source


def endpoint_parts(endpoint, local_test=False):
    try:
        parts = urlsplit(endpoint)
        require(parts.scheme in ('https', 'http') and parts.hostname and
                parts.username is None and parts.password is None and not parts.query and not parts.fragment,
                'Invalid ingestion endpoint')
        port = parts.port
        require(port is None or 1 <= port <= 65535, 'Invalid ingestion port')
        if parts.scheme == 'http':
            require(local_test and ipaddress.ip_address(parts.hostname).is_loopback,
                    'Plain HTTP requires explicit local test and a literal loopback address')
        require(not any(ord(c) < 33 for c in endpoint), 'Invalid endpoint characters')
        return parts
    except (ValueError, TypeError) as exc:
        raise ValueError('Invalid or insecure ingestion endpoint') from exc


@dataclass(frozen=True)
class UploadConfig:
    collector_db: Path
    state_db: Path
    endpoint: str
    token: str
    expected_source: dict
    local_test: bool = False
    timeout: float = 10.
    attempt_timeout: float = 30.
    poll_seconds: float = 5.
    retry_base: float = 1.
    retry_max: float = 60.

    def validate(self):
        require(self.collector_db.is_absolute() and self.collector_db.is_file(), 'Collector database must exist at an absolute path')
        require(self.state_db.is_absolute() and not self.state_db.is_dir(), 'State database requires an absolute file path')
        require(self.collector_db.resolve() != self.state_db.resolve(), 'State and collector databases must be separate')
        if self.state_db.exists():
            require(not self.collector_db.samefile(self.state_db), 'State and collector databases must be separate')
        require(isinstance(self.token, str) and 1 <= len(self.token) <= 4096 and
                self.token.isascii() and all(33 <= ord(c) < 127 for c in self.token), 'Invalid bearer token')
        require(type(self.local_test) is bool, 'Invalid local-test flag')
        source(self.expected_source)
        endpoint_parts(self.endpoint, self.local_test)
        for key in ('timeout', 'attempt_timeout', 'poll_seconds', 'retry_base', 'retry_max'):
            value = getattr(self, key)
            require(type(value) in (float, int) and math.isfinite(value) and 0 < value <= 300, 'Invalid uploader timing')
        require(self.retry_base <= self.retry_max and self.timeout <= self.attempt_timeout, 'Invalid timeout/backoff bounds')
        return self


def load_upload_config(path, *, validate=True):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, 'Duplicate uploader configuration key')
            result[key] = value
        return result
    data = json.loads(Path(path).read_text(encoding='utf-8-sig'), object_pairs_hook=unique)
    require(isinstance(data, dict), 'Expected uploader configuration object')
    allowed = set(UploadConfig.__dataclass_fields__)
    require(data.keys() <= allowed, 'Unknown uploader configuration field')
    data['collector_db'] = Path(data['collector_db'])
    data['state_db'] = Path(data['state_db'])
    config = UploadConfig(**data)
    return config.validate() if validate else config
