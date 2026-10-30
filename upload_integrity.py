"""Offline uploader binding/anchor checks shared by delivery and visibility."""

from upload_contract import require, event_digest
from upload_reader import read_delivery
from upload_state import file_identity


def anchors_for(progress, request=None):
    anchors = {}
    if progress['anchor']:
        anchor = progress['anchor']
        require(anchor['sequence'] == progress['acknowledged_sequence'], 'Progress anchor differs')
        anchors[anchor['sequence']] = anchor['digest']
    else:
        require(progress['acknowledged_sequence'] == 0, 'Missing progress anchor')
    if request:
        require(int(request['first_sequence']) > progress['acknowledged_sequence'], 'Pending range overlaps acknowledged progress')
        anchors.update({int(e['sequence']): event_digest(e) for e in request['events']})
    return anchors


def snapshot(config, binding, progress, request=None, *, metadata_only=False, limit=20):
    require(file_identity(config.collector_db) == binding['file_identity'], 'Collector database file replaced')
    if request:
        require(request['source'] == binding['source'] and request['stream_id'] == binding['stream_id'], 'Pending identity differs')
    data = read_delivery(config.collector_db, after_sequence=progress['acknowledged_sequence'],
                         expected_source=config.expected_source, expected_stream=binding['stream_id'],
                         anchors=anchors_for(progress, request), metadata_only=metadata_only,
                         limit=len(request['events']) if request else limit)
    require(file_identity(config.collector_db) == binding['file_identity'], 'Collector database file replaced during read')
    require(data['head_sequence'] >= progress['acknowledged_sequence'], 'Collector database rolled back')
    if request and not metadata_only:
        pending_sequences = [e['sequence'] for e in request['events']]
        require([e['sequence'] for e in data['events'][:len(pending_sequences)]] == pending_sequences,
                'Pending request no longer covers the next undelivered events')
    return data
