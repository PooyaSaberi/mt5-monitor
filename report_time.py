"""Explicit Tehran reporting; broker epochs remain unverified by default."""

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from mt5_monitor import DiagnosticError

UTC = timezone.utc
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
REPORT_ZONE = 'Asia/Tehran'


def zone(name):
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError as exc:
        raise DiagnosticError('IANA timezone data unavailable; install requirements with .venv\\Scripts\\python.exe -m pip install -r requirements.txt') from exc


def aware_utc(value):
    if not isinstance(value, str):
        raise ValueError('An aware timestamp string is required')
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.utcoffset() is None:
        raise ValueError('An aware timestamp is required')
    return result.astimezone(UTC)


def tehran(value):
    """Input must already identify an instant; never use the Windows timezone."""
    if value.utcoffset() is None:
        raise ValueError('Cannot convert a naive datetime')
    return value.astimezone(zone(REPORT_ZONE))


def wall_candidates(wall, server_zone):
    candidates = set()
    for fold in (0, 1):
        candidate = wall.replace(tzinfo=server_zone, fold=fold).astimezone(UTC)
        if candidate.astimezone(server_zone).replace(tzinfo=None) == wall:
            candidates.add(candidate)
    return sorted(candidates)


@dataclass(frozen=True)
class TimeMapping:
    event_basis: str = 'unverified'
    query_basis: str = 'unverified'
    server_zone: str | None = None
    evidence_ref: str | None = None
    valid_from_utc: str | None = None
    valid_to_utc: str | None = None

    @classmethod
    def load(cls, path=None):
        policy = cls(**json.loads(path.read_text(encoding='utf-8'))) if path else cls()
        policy.validate()
        return policy

    def validate(self):
        if self.event_basis not in ('unverified', 'utc', 'server_wall') or self.query_basis not in ('unverified', 'utc', 'server_wall'):
            raise ValueError('Unknown broker event/query time basis')
        zone(REPORT_ZONE)
        if 'server_wall' in (self.event_basis, self.query_basis):
            if not self.server_zone:
                raise ValueError('Verified server-wall mapping requires an evidenced IANA server_zone')
            zone(self.server_zone)
        if self.event_basis != 'unverified' or self.query_basis != 'unverified':
            if not isinstance(self.evidence_ref, str) or not self.evidence_ref.strip() or not self.valid_from_utc or not self.valid_to_utc:
                raise ValueError('Verified mapping requires evidence_ref and an explicit validity period')
            if aware_utc(self.valid_from_utc) >= aware_utc(self.valid_to_utc):
                raise ValueError('Invalid mapping validity period')

    def covers(self, value):
        return bool(self.valid_from_utc and self.valid_to_utc and
                    aware_utc(self.valid_from_utc) <= value < aware_utc(self.valid_to_utc))

    def event(self, raw_milliseconds):
        if type(raw_milliseconds) is not int:
            raise ValueError('Event conversion accepts untouched integer milliseconds only')
        literal = EPOCH + timedelta(milliseconds=raw_milliseconds)
        result = {'raw_time_msc': raw_milliseconds,
                  'raw_epoch_as_utc_candidate': literal.isoformat(),
                  'event_utc': None, 'event_tehran': None,
                  'status': 'unverified', 'event_basis': self.event_basis,
                  'report_timezone': REPORT_ZONE, 'mapping_evidence': self.evidence_ref}
        if self.event_basis == 'unverified':
            return result
        if self.event_basis == 'utc':
            instant = literal
        else:
            candidates = wall_candidates(literal.replace(tzinfo=None), zone(self.server_zone))
            if len(candidates) != 1:
                result['status'] = 'ambiguous' if candidates else 'nonexistent'
                return result
            instant = candidates[0]
        if not self.covers(instant):
            result['status'] = 'outside_verified_period'
            return result
        result.update(event_utc=instant.isoformat(), event_tehran=tehran(instant).isoformat(), status='verified')
        return result

    def query_segments(self, start, end):
        """Encode UTC instants into a separately verified query argument basis."""
        if start.utcoffset() is None or end.utcoffset() is None or start >= end:
            raise ValueError('Query boundaries must identify an ordered interval')
        start, end = start.astimezone(UTC), end.astimezone(UTC)
        if self.query_basis == 'unverified' or not self.covers(start) or not self.covers(end - timedelta(microseconds=1)):
            return []
        boundaries = [start]
        if self.query_basis == 'server_wall':
            server = zone(self.server_zone)
            cursor = start
            while cursor < end:
                next_time = min(end, cursor + timedelta(minutes=30))
                if cursor.astimezone(server).utcoffset() != next_time.astimezone(server).utcoffset():
                    low, high = int(cursor.timestamp()), int(next_time.timestamp())
                    old = cursor.astimezone(server).utcoffset()
                    while high - low > 1:
                        middle = (low + high) // 2
                        if datetime.fromtimestamp(middle, UTC).astimezone(server).utcoffset() == old:
                            low = middle
                        else:
                            high = middle
                    boundary = datetime.fromtimestamp(high, UTC)
                    if start < boundary < end:
                        boundaries.append(boundary)
                cursor = next_time
        boundaries.append(end)
        result = []
        for lower, upper in zip(boundaries, boundaries[1:]):
            offset = timedelta(0) if self.query_basis == 'utc' else lower.astimezone(zone(self.server_zone)).utcoffset()
            result.append({'physical_start_utc': lower.isoformat(), 'physical_end_utc': upper.isoformat(),
                           'argument_start': (lower + offset).isoformat(),
                           'argument_end_exclusive': (upper + offset).isoformat(),
                           'argument_start_epoch': int((lower + offset).timestamp()),
                           'argument_end_exclusive_epoch': int((upper + offset).timestamp()),
                           'query_basis': self.query_basis,
                           'broker_offset_seconds': int(offset.total_seconds())})
        return result


def tehran_day_bounds(day):
    day = date.fromisoformat(day) if isinstance(day, str) else day
    def first_instant(d):
        midnight = datetime.combine(d, datetime.min.time())
        # Iran formerly changed clocks at midnight. A skipped midnight starts
        # the civil day at its first valid local minute; folds use the first.
        for minute in range(181):
            candidates = wall_candidates(midnight + timedelta(minutes=minute), zone(REPORT_ZONE))
            if candidates:
                return candidates[0]
        raise ValueError('Cannot resolve Tehran civil-day boundary')
    return first_instant(day), first_instant(day + timedelta(days=1))


def report_day_plan(day, mapping):
    start, end = tehran_day_bounds(day)
    segments = mapping.query_segments(start, end)
    return {'report_date': str(day), 'report_timezone': REPORT_ZONE,
            'start_tehran': tehran(start).isoformat(), 'end_tehran_exclusive': tehran(end).isoformat(),
            'start_utc': start.isoformat(), 'end_utc_exclusive': end.isoformat(),
            'query_status': 'verified' if segments else 'unverified',
            'query_basis': mapping.query_basis, 'query_segments': segments,
            'mapping_evidence': mapping.evidence_ref,
            'note': 'Half-open report boundaries. MT5 inclusive endpoints may return boundary duplicates; deduplicate by exact ticket. Server DST folds can produce overlapping argument ranges.'}


def decorate_row(row, mapping):
    """Derive display fields afresh from originals; repeated calls cannot shift twice."""
    result = dict(row)
    for key in ('started_at', 'ended_at', 'observed_at', 'first_seen', 'last_seen'):
        if key in row and isinstance(row[key], str):
            try:
                instant = aware_utc(row[key])
            except ValueError:
                continue
            result[key + '_collector_utc'] = instant.isoformat()
            result[key + '_tehran'] = tehran(instant).isoformat()
    payload = row.get('fields')
    if isinstance(payload, dict):
        event_times = {}
        for key in ('time_msc', 'time_update_msc', 'time_setup_msc', 'time_done_msc'):
            if type(payload.get(key)) is int and payload[key] != 0:
                event_times[key] = mapping.event(payload[key])
        if event_times:
            result['broker_event_times'] = event_times
    elif type(row.get('time_msc')) is int:
        result['broker_event_times'] = {'time_msc': mapping.event(row['time_msc'])}
    return result
