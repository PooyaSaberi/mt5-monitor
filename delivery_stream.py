"""Transactional delivery index. No MT5, network, or payload dependencies."""

from uuid import uuid4


def create_schema(db):
    # Caller owns the migration transaction; never use executescript here.
    db.execute('CREATE TABLE delivery_stream (singleton INTEGER PRIMARY KEY CHECK(singleton=1), stream_id TEXT NOT NULL UNIQUE)')
    db.execute('INSERT INTO delivery_stream VALUES (1,?)', (str(uuid4()),))
    db.execute("CREATE TABLE delivery_events (sequence INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL CHECK(kind IN ('account_observation','collection_status')), record_id TEXT NOT NULL, UNIQUE(kind,record_id))")
    # Deterministic transport order, not reconstructed historical commit order.
    db.execute("""INSERT INTO delivery_events(kind,record_id)
        SELECT kind,record_id FROM (
          SELECT 'account_observation' AS kind, observation_id AS record_id, ended_at FROM observations
          UNION ALL
          SELECT 'collection_status', event_id, ended_at FROM status_events
        ) ORDER BY ended_at,kind,record_id""")
    for table, key, kind in (
        ('observations', 'observation_id', 'account_observation'),
        ('status_events', 'event_id', 'collection_status'),
    ):
        # The entry and its source data commit/roll back together. A cycle's
        # batches inserted later in the same transaction are never half-visible.
        db.execute(f"""CREATE TRIGGER delivery_{table} AFTER INSERT ON {table}
            BEGIN INSERT INTO delivery_events(kind,record_id)
            VALUES ('{kind}',NEW.{key}); END""")
