from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app_common import (
    consume_json_records,
    fetch_google_sheet_rows,
    hour_bucket,
    kafka_message,
    parse_int,
    parse_money,
    parse_percent,
    print_summary,
    produce_json_records,
    sqlite_connect,
    upsert_raw_events,
)


APP_NAME = "Stream Processing"
DB_PATH = Path(__file__).resolve().parent / "data" / "stream_processing.sqlite3"
# Kafka input topic for raw Sheet events.
INPUT_TOPIC = "stream-processing.input-events"
# Kafka output topic for processed aggregate records.
OUTPUT_TOPIC = "stream-processing.hourly-metrics"
# Kafka consumer group for the stream processor.
INPUT_GROUP_ID = "stream-processing.processor"
# Kafka consumer group for the SQLite metric sink.
OUTPUT_GROUP_ID = "stream-processing.sqlite-sink"


def main() -> None:
    rows = fetch_google_sheet_rows()
    # Kafka producer step: publish raw input events.
    input_produced = produce_json_records(INPUT_TOPIC, (kafka_message(row, "stream.input") for row in rows))
    # Kafka processor-consumer step: consume raw events from the input topic.
    stream_rows = consume_json_records(INPUT_TOPIC, INPUT_GROUP_ID, max_messages=input_produced)
    aggregates: dict[tuple[str, str, str], dict[str, float]] = defaultdict(
        lambda: {"events": 0, "quantity": 0, "revenue": 0.0, "discount_total": 0.0}
    )

    for row in stream_rows:
        key = (
            hour_bucket(row.get("event_timestamp")),
            row.get("event_type", "unknown") or "unknown",
            row.get("product_category", "unknown") or "unknown",
        )
        bucket = aggregates[key]
        bucket["events"] += 1
        bucket["quantity"] += parse_int(row.get("quantity"))
        bucket["revenue"] += parse_money(row.get("order_total"))
        bucket["discount_total"] += parse_percent(row.get("discount_pct"))

    metric_records = []
    for (hour, event_type, category), values in aggregates.items():
        event_count = int(values["events"])
        metric_records.append(
            {
                "_event_key": f"{hour}|{event_type}|{category}",
                "_event_name": "hourly.metric.calculated",
                "hour_bucket": hour,
                "event_type": event_type,
                "product_category": category,
                "event_count": event_count,
                "total_quantity": int(values["quantity"]),
                "total_revenue": round(values["revenue"], 2),
                "average_discount_pct": round((values["discount_total"] / event_count) * 100, 2) if event_count else 0,
            }
        )

    # Kafka producer step: publish processed metrics to the output topic.
    output_produced = produce_json_records(OUTPUT_TOPIC, metric_records)
    # Kafka sink-consumer step: consume processed metrics before writing SQLite.
    consumed_metrics = consume_json_records(OUTPUT_TOPIC, OUTPUT_GROUP_ID, max_messages=output_produced)

    with sqlite_connect(DB_PATH) as conn:
        upsert_raw_events(conn, "processed_events", stream_rows)
        conn.execute("DROP TABLE IF EXISTS hourly_event_metrics")
        conn.execute(
            """
            CREATE TABLE hourly_event_metrics (
                hour_bucket TEXT NOT NULL,
                event_type TEXT NOT NULL,
                product_category TEXT NOT NULL,
                event_count INTEGER NOT NULL,
                total_quantity INTEGER NOT NULL,
                total_revenue REAL NOT NULL,
                average_discount_pct REAL NOT NULL,
                PRIMARY KEY (hour_bucket, event_type, product_category)
            )
            """
        )
        for metric in consumed_metrics:
            conn.execute(
                """
                INSERT INTO hourly_event_metrics (
                    hour_bucket, event_type, product_category, event_count,
                    total_quantity, total_revenue, average_discount_pct
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    metric["hour_bucket"],
                    metric["event_type"],
                    metric["product_category"],
                    int(metric["event_count"]),
                    int(metric["total_quantity"]),
                    float(metric["total_revenue"]),
                    float(metric["average_discount_pct"]),
                ),
            )

    print_summary(
        APP_NAME,
        DB_PATH,
        len(rows),
        {
            "input_kafka_messages_produced": input_produced,
            "input_kafka_messages_consumed": len(stream_rows),
            "output_kafka_messages_produced": output_produced,
            "output_kafka_messages_consumed": len(consumed_metrics),
        },
    )


if __name__ == "__main__":
    main()
