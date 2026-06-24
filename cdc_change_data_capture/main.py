from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app_common import fetch_google_sheet_rows, now_utc, raw_json, row_hash, row_key, sqlite_connect
from app_common import consume_json_records, print_summary, produce_json_records


APP_NAME = "CDC Change Data Capture"
DB_PATH = Path(__file__).resolve().parent / "data" / "cdc.sqlite3"
# Kafka topic that carries CDC insert/update/delete events.
TOPIC = "cdc-change-data-capture.changes"
# Kafka consumer group used by the CDC SQLite sink.
GROUP_ID = "cdc-change-data-capture.sqlite-sink"


def diff_json(before_json: str | None, after_json: str | None) -> str:
    before = json.loads(before_json) if before_json else {}
    after = json.loads(after_json) if after_json else {}
    changed = sorted({*before.keys(), *after.keys()})
    return json.dumps(
        {
            key: {"before": before.get(key), "after": after.get(key)}
            for key in changed
            if before.get(key) != after.get(key)
        },
        sort_keys=True,
    )


def main() -> None:
    rows = fetch_google_sheet_rows()
    changes_to_publish: list[dict[str, str]] = []
    seen_keys: set[str] = set()
    captured_at = now_utc()

    with sqlite_connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS source_snapshot (
                entity_key TEXT PRIMARY KEY,
                row_hash TEXT NOT NULL,
                raw_json TEXT NOT NULL,
                captured_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cdc_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_key TEXT NOT NULL,
                operation TEXT NOT NULL,
                before_json TEXT,
                after_json TEXT,
                changed_fields_json TEXT NOT NULL,
                captured_at TEXT NOT NULL
            )
            """
        )

        for row in rows:
            key = row_key(row)
            digest = row_hash(row)
            after = raw_json(row)
            seen_keys.add(key)
            previous = conn.execute("SELECT row_hash, raw_json FROM source_snapshot WHERE entity_key = ?", (key,)).fetchone()
            if previous is None:
                operation = "INSERT"
                before = None
            elif previous["row_hash"] != digest:
                operation = "UPDATE"
                before = previous["raw_json"]
            else:
                operation = None
                before = previous["raw_json"]

            if operation:
                changes_to_publish.append(
                    {
                        "_event_key": key,
                        "_event_name": f"cdc.{operation.lower()}",
                        "entity_key": key,
                        "operation": operation,
                        "before_json": before,
                        "after_json": after,
                        "changed_fields_json": diff_json(before, after),
                        "captured_at": captured_at,
                    }
                )

            conn.execute(
                """
                INSERT INTO source_snapshot (entity_key, row_hash, raw_json, captured_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(entity_key) DO UPDATE SET
                    row_hash=excluded.row_hash,
                    raw_json=excluded.raw_json,
                    captured_at=excluded.captured_at
                """,
                (key, digest, after, captured_at),
            )

        existing_keys = {
            item["entity_key"]
            for item in conn.execute("SELECT entity_key FROM source_snapshot").fetchall()
        }
        for deleted_key in sorted(existing_keys - seen_keys):
            before = conn.execute("SELECT raw_json FROM source_snapshot WHERE entity_key = ?", (deleted_key,)).fetchone()["raw_json"]
            changes_to_publish.append(
                {
                    "_event_key": deleted_key,
                    "_event_name": "cdc.delete",
                    "entity_key": deleted_key,
                    "operation": "DELETE",
                    "before_json": before,
                    "after_json": None,
                    "changed_fields_json": diff_json(before, None),
                    "captured_at": captured_at,
                }
            )
            conn.execute("DELETE FROM source_snapshot WHERE entity_key = ?", (deleted_key,))

    # Kafka producer step: publish detected CDC changes to Kafka.
    produced = produce_json_records(TOPIC, changes_to_publish) if changes_to_publish else 0
    # Kafka consumer step: consume CDC changes before writing them to SQLite.
    consumed_changes = consume_json_records(TOPIC, GROUP_ID, max_messages=produced) if produced else []
    with sqlite_connect(DB_PATH) as conn:
        for change in consumed_changes:
            conn.execute(
                """
                INSERT INTO cdc_changes (
                    entity_key, operation, before_json, after_json,
                    changed_fields_json, captured_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    change["entity_key"],
                    change["operation"],
                    change.get("before_json"),
                    change.get("after_json"),
                    change["changed_fields_json"],
                    change["captured_at"],
                ),
            )

    print_summary(
        APP_NAME,
        DB_PATH,
        len(rows),
        {"kafka_messages_produced": produced, "kafka_messages_consumed": len(consumed_changes)},
    )


if __name__ == "__main__":
    main()
