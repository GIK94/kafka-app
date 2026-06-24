from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app_common import (
    consume_json_records,
    fetch_google_sheet_rows,
    kafka_message,
    print_summary,
    produce_json_records,
    sqlite_connect,
    upsert_raw_events,
)


APP_NAME = "Real-time Data Pipeline"
DB_PATH = Path(__file__).resolve().parent / "data" / "pipeline.sqlite3"
# Kafka topic used by this use case.
TOPIC = "real-time-data-pipeline.events"
# Kafka consumer group used by the SQLite sink.
GROUP_ID = "real-time-data-pipeline.sqlite-sink"


def main() -> None:
    rows = fetch_google_sheet_rows()
    # Kafka producer step: publish Google Sheet rows to the pipeline topic.
    produced = produce_json_records(TOPIC, (kafka_message(row, "sheet.row.received") for row in rows))
    # Kafka consumer step: read the pipeline topic before writing to SQLite.
    consumed_rows = consume_json_records(TOPIC, GROUP_ID, max_messages=produced)
    with sqlite_connect(DB_PATH) as conn:
        inserted_or_updated = upsert_raw_events(conn, "pipeline_events", consumed_rows)
        conn.execute(
            """
            CREATE VIEW IF NOT EXISTS latest_pipeline_events AS
            SELECT event_key, event_timestamp, event_type, order_id, customer_id,
                   product_category, shipment_status, last_seen_at
            FROM pipeline_events
            ORDER BY event_timestamp DESC
            """
        )
    print_summary(
        APP_NAME,
        DB_PATH,
        len(rows),
        {
            "kafka_messages_produced": produced,
            "kafka_messages_consumed": len(consumed_rows),
            "upserted_events": inserted_or_updated,
        },
    )


if __name__ == "__main__":
    main()
