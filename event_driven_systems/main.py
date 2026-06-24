from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app_common import (
    consume_json_records,
    fetch_google_sheet_rows,
    now_utc,
    print_summary,
    produce_json_records,
    raw_json,
    row_hash,
    row_key,
    sqlite_connect,
)


APP_NAME = "Event-Driven System"
DB_PATH = Path(__file__).resolve().parent / "data" / "events.sqlite3"
# Kafka topic that carries domain events.
TOPIC = "event-driven-system.domain-events"
# Kafka consumer group used by the domain-event SQLite sink.
GROUP_ID = "event-driven-system.sqlite-sink"


def main() -> None:
    rows = fetch_google_sheet_rows()
    events_to_publish: list[dict[str, str]] = []
    seen_keys: set[str] = set()
    observed_at = now_utc()

    with sqlite_connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS entity_state (
                entity_key TEXT PRIMARY KEY,
                row_hash TEXT NOT NULL,
                raw_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS domain_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_key TEXT NOT NULL,
                event_name TEXT NOT NULL,
                event_type TEXT,
                payload_json TEXT NOT NULL,
                emitted_at TEXT NOT NULL
            )
            """
        )

        for row in rows:
            key = row_key(row)
            seen_keys.add(key)
            digest = row_hash(row)
            previous = conn.execute("SELECT row_hash FROM entity_state WHERE entity_key = ?", (key,)).fetchone()
            event_name = "entity.created" if previous is None else "entity.updated" if previous["row_hash"] != digest else None
            if event_name:
                events_to_publish.append(
                    {
                        "_event_key": key,
                        "_event_name": event_name,
                        "entity_key": key,
                        "event_name": event_name,
                        "event_type": row.get("event_type", ""),
                        "payload_json": raw_json(row),
                        "emitted_at": observed_at,
                    }
                )
            conn.execute(
                """
                INSERT INTO entity_state (entity_key, row_hash, raw_json, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(entity_key) DO UPDATE SET
                    row_hash=excluded.row_hash,
                    raw_json=excluded.raw_json,
                    updated_at=excluded.updated_at
                """,
                (key, digest, raw_json(row), observed_at),
            )

        existing_keys = {
            item["entity_key"]
            for item in conn.execute("SELECT entity_key FROM entity_state").fetchall()
        }
        for deleted_key in sorted(existing_keys - seen_keys):
            events_to_publish.append(
                {
                    "_event_key": deleted_key,
                    "_event_name": "entity.deleted",
                    "entity_key": deleted_key,
                    "event_name": "entity.deleted",
                    "event_type": "",
                    "payload_json": "{}",
                    "emitted_at": observed_at,
                }
            )
            conn.execute("DELETE FROM entity_state WHERE entity_key = ?", (deleted_key,))

    # Kafka producer step: publish created/updated/deleted domain events.
    produced = produce_json_records(TOPIC, events_to_publish) if events_to_publish else 0
    # Kafka consumer step: consume those domain events into the SQLite sink.
    consumed_events = consume_json_records(TOPIC, GROUP_ID, max_messages=produced) if produced else []
    with sqlite_connect(DB_PATH) as conn:
        for event in consumed_events:
            conn.execute(
                "INSERT INTO domain_events (entity_key, event_name, event_type, payload_json, emitted_at) VALUES (?, ?, ?, ?, ?)",
                (
                    event["entity_key"],
                    event["event_name"],
                    event.get("event_type", ""),
                    event.get("payload_json", "{}"),
                    event.get("emitted_at", observed_at),
                ),
            )

    print_summary(
        APP_NAME,
        DB_PATH,
        len(rows),
        {"kafka_messages_produced": produced, "kafka_messages_consumed": len(consumed_events)},
    )


if __name__ == "__main__":
    main()
