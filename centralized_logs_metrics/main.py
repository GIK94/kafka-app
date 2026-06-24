from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app_common import (
    consume_json_records,
    fetch_google_sheet_rows,
    hour_bucket,
    now_utc,
    print_summary,
    produce_json_records,
    row_key,
    sqlite_connect,
)


APP_NAME = "Centralized Logs and Metrics"
DB_PATH = Path(__file__).resolve().parent / "data" / "logs_metrics.sqlite3"
# Kafka topic for log events.
LOG_TOPIC = "centralized-logs.logs"
# Kafka topic for metric events.
METRIC_TOPIC = "centralized-logs.metrics"
# Kafka consumer group for the log SQLite sink.
LOG_GROUP_ID = "centralized-logs.sqlite-log-sink"
# Kafka consumer group for the metric SQLite sink.
METRIC_GROUP_ID = "centralized-logs.sqlite-metric-sink"


def main() -> None:
    rows = fetch_google_sheet_rows()
    run_at = now_utc()
    metric_counts: Counter[tuple[str, str, str]] = Counter()
    log_records: list[dict[str, str]] = []

    for row in rows:
        status = row.get("shipment_status", "unknown") or "unknown"
        event_type = row.get("event_type", "unknown") or "unknown"
        bucket = hour_bucket(row.get("event_timestamp"))
        level = "ERROR" if status.lower() in {"cancelled", "failed", "returned"} else "INFO"
        message = f"{event_type} order={row.get('order_id', '')} shipment_status={status}"
        log_records.append(
            {
                "_event_key": row_key(row),
                "_event_name": "log.recorded",
                "run_at": run_at,
                "source_row_key": row_key(row),
                "level": level,
                "message": message,
            }
        )
        metric_counts[(bucket, "shipment_status", status)] += 1
        metric_counts[(bucket, "event_type", event_type)] += 1

    metric_records = [
        {
            "_event_key": f"{bucket}|{name}|{value}",
            "_event_name": "metric.aggregated",
            "metric_bucket": bucket,
            "metric_name": name,
            "metric_value": value,
            "count": count,
        }
        for (bucket, name, value), count in metric_counts.items()
    ]

    # Kafka producer steps: publish logs and metrics to separate Kafka topics.
    logs_produced = produce_json_records(LOG_TOPIC, log_records)
    metrics_produced = produce_json_records(METRIC_TOPIC, metric_records)
    # Kafka consumer steps: consume both Kafka topics into SQLite tables.
    consumed_logs = consume_json_records(LOG_TOPIC, LOG_GROUP_ID, max_messages=logs_produced)
    consumed_metrics = consume_json_records(METRIC_TOPIC, METRIC_GROUP_ID, max_messages=metrics_produced)

    with sqlite_connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS app_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_at TEXT NOT NULL,
                source_row_key TEXT NOT NULL,
                level TEXT NOT NULL,
                message TEXT NOT NULL
            )
            """
        )
        conn.execute("DROP TABLE IF EXISTS operational_metrics")
        conn.execute(
            """
            CREATE TABLE operational_metrics (
                metric_bucket TEXT NOT NULL,
                metric_name TEXT NOT NULL,
                metric_value TEXT NOT NULL,
                count INTEGER NOT NULL,
                PRIMARY KEY (metric_bucket, metric_name, metric_value)
            )
            """
        )
        for record in consumed_logs:
            conn.execute(
                "INSERT INTO app_logs (run_at, source_row_key, level, message) VALUES (?, ?, ?, ?)",
                (record["run_at"], record["source_row_key"], record["level"], record["message"]),
            )

        for metric in consumed_metrics:
            conn.execute(
                "INSERT INTO operational_metrics (metric_bucket, metric_name, metric_value, count) VALUES (?, ?, ?, ?)",
                (metric["metric_bucket"], metric["metric_name"], metric["metric_value"], int(metric["count"])),
            )

    print_summary(
        APP_NAME,
        DB_PATH,
        len(rows),
        {
            "log_kafka_messages_produced": logs_produced,
            "log_kafka_messages_consumed": len(consumed_logs),
            "metric_kafka_messages_produced": metrics_produced,
            "metric_kafka_messages_consumed": len(consumed_metrics),
        },
    )


if __name__ == "__main__":
    main()
