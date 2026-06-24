from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import requests

# Kafka client objects from confluent-kafka.
# Producer writes messages to Kafka topics, Consumer reads messages from Kafka topics,
# and AdminClient creates topics before the apps publish or consume.
from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic


DEFAULT_SHEET_ID = "1hMG0ayX_RwlASEVjR2GNMSfFp3UmVlIiOyT0WWCtQHo"
DEFAULT_GID = "0"
DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def sheet_id() -> str:
    return os.getenv("GOOGLE_SHEET_ID", DEFAULT_SHEET_ID)


def sheet_gid() -> str:
    return os.getenv("GOOGLE_SHEET_GID", DEFAULT_GID)


def bootstrap_servers() -> str:
    # Kafka broker address used by every producer, consumer, and admin client.
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS)


def sqlite_connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def fetch_google_sheet_rows() -> list[dict[str, str]]:
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id()}/export"
    response = requests.get(url, params={"format": "csv", "gid": sheet_gid()}, timeout=30)
    response.raise_for_status()

    content = response.content.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(content))
    rows: list[dict[str, str]] = []
    for row_number, row in enumerate(reader, start=2):
        cleaned = {
            clean_header(key): (value or "").strip()
            for key, value in row.items()
            if key is not None and clean_header(key)
        }
        if any(cleaned.values()):
            cleaned["_source_row_number"] = str(row_number)
            rows.append(cleaned)
    return rows


def clean_header(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]+", "_", value.strip().lower()).strip("_")


def row_key(row: dict[str, str]) -> str:
    for candidate in ("event_id", "order_id", "id"):
        if row.get(candidate):
            return row[candidate]
    for key, value in row.items():
        if key.endswith("_id") and value:
            return value
    return row_hash(row)


def row_hash(row: dict[str, str]) -> str:
    payload = json.dumps(
        {key: row.get(key, "") for key in sorted(row) if not key.startswith("_")},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def raw_json(row: dict[str, str]) -> str:
    return json.dumps(row, sort_keys=True)


def kafka_message(row: dict[str, str], event_name: str | None = None) -> dict[str, object]:
    # Kafka message payload wrapper.
    # The _event_key becomes the Kafka record key and keeps related records grouped.
    message = dict(row)
    message["_event_key"] = row_key(row)
    message["_row_hash"] = row_hash(row)
    message["_event_name"] = event_name or row.get("event_type", "sheet.row")
    message["_published_at"] = now_utc()
    return message


def ensure_topics(topic_names: Iterable[str], partitions: int = 1, replication_factor: int = 1) -> None:
    # Kafka Admin API usage.
    # Creates required Kafka topics if they do not already exist.
    admin = AdminClient({"bootstrap.servers": bootstrap_servers()})
    existing = set(admin.list_topics(timeout=10).topics.keys())
    new_topics = [
        NewTopic(topic, num_partitions=partitions, replication_factor=replication_factor)
        for topic in topic_names
        if topic not in existing
    ]
    if not new_topics:
        return
    futures = admin.create_topics(new_topics)
    for topic, future in futures.items():
        try:
            future.result()
        except KafkaException as exc:
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise


def produce_json_records(topic: str, records: Iterable[dict[str, object]]) -> int:
    # Kafka producer usage.
    # Sends each JSON record to the given Kafka topic.
    ensure_topics([topic])
    producer = Producer({"bootstrap.servers": bootstrap_servers(), "client.id": "kafka-app-producer"})
    delivered = 0
    delivery_errors: list[str] = []

    def delivery_report(error, _message) -> None:
        if error is not None:
            delivery_errors.append(str(error))

    for record in records:
        key = record.get("_event_key") or row_key(record)
        producer.produce(
            # This is the actual Kafka publish call.
            topic,
            key=key.encode("utf-8"),
            value=json.dumps(record, sort_keys=True).encode("utf-8"),
            callback=delivery_report,
        )
        producer.poll(0)
        delivered += 1
    producer.flush(30)
    if delivery_errors:
        raise RuntimeError(f"Kafka delivery failed: {delivery_errors[0]}")
    return delivered


def consume_json_records(
    topic: str,
    group_id: str,
    max_messages: int,
    idle_timeout_seconds: float = 5.0,
) -> list[dict[str, str]]:
    # Kafka consumer usage.
    # Reads a bounded number of JSON messages from a Kafka topic for a consumer group.
    ensure_topics([topic])
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers(),
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    # Subscribe this consumer to the Kafka topic.
    consumer.subscribe([topic])
    records: list[dict[str, str]] = []
    idle_started = datetime.now(timezone.utc)
    try:
        while len(records) < max_messages:
            message = consumer.poll(1.0)
            if message is None:
                idle_seconds = (datetime.now(timezone.utc) - idle_started).total_seconds()
                if idle_seconds >= idle_timeout_seconds:
                    break
                continue
            if message.error():
                raise KafkaException(message.error())
            idle_started = datetime.now(timezone.utc)
            # This is where the Kafka message value is decoded back into a Python dict.
            records.append(json.loads(message.value().decode("utf-8")))
            # Commit the Kafka offset after successfully decoding the message.
            consumer.commit(message=message, asynchronous=False)
    finally:
        consumer.close()
    return records


def parse_money(value: str | None) -> float:
    if not value:
        return 0.0
    cleaned = re.sub(r"[^0-9.\-]", "", value)
    return float(cleaned) if cleaned else 0.0


def parse_percent(value: str | None) -> float:
    if not value:
        return 0.0
    return parse_money(value) / 100


def parse_int(value: str | None) -> int:
    if not value:
        return 0
    cleaned = re.sub(r"[^0-9\-]", "", value)
    return int(cleaned) if cleaned else 0


def hour_bucket(value: str | None) -> str:
    if not value:
        return "unknown"
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%m/%d/%Y %H:%M:%S"):
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d %H:00:00")
        except ValueError:
            pass
    return value[:13] if len(value) >= 13 else value


def ensure_raw_events_table(conn: sqlite3.Connection, table_name: str) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            event_key TEXT PRIMARY KEY,
            row_hash TEXT NOT NULL,
            source_row_number INTEGER,
            event_timestamp TEXT,
            event_type TEXT,
            order_id TEXT,
            customer_id TEXT,
            product_category TEXT,
            shipment_status TEXT,
            raw_json TEXT NOT NULL,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL
        )
        """
    )


def upsert_raw_events(conn: sqlite3.Connection, table_name: str, rows: Iterable[dict[str, str]]) -> int:
    ensure_raw_events_table(conn, table_name)
    seen_at = now_utc()
    count = 0
    for row in rows:
        conn.execute(
            f"""
            INSERT INTO {table_name} (
                event_key, row_hash, source_row_number, event_timestamp, event_type,
                order_id, customer_id, product_category, shipment_status, raw_json,
                first_seen_at, last_seen_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_key) DO UPDATE SET
                row_hash=excluded.row_hash,
                source_row_number=excluded.source_row_number,
                event_timestamp=excluded.event_timestamp,
                event_type=excluded.event_type,
                order_id=excluded.order_id,
                customer_id=excluded.customer_id,
                product_category=excluded.product_category,
                shipment_status=excluded.shipment_status,
                raw_json=excluded.raw_json,
                last_seen_at=excluded.last_seen_at
            """,
            (
                row_key(row),
                row_hash(row),
                row.get("_source_row_number"),
                row.get("event_timestamp", ""),
                row.get("event_type", ""),
                row.get("order_id", ""),
                row.get("customer_id", ""),
                row.get("product_category", ""),
                row.get("shipment_status", ""),
                raw_json(row),
                seen_at,
                seen_at,
            ),
        )
        count += 1
    return count


def print_summary(app_name: str, db_path: Path, rows_read: int, details: dict[str, int] | None = None) -> None:
    print(f"{app_name}")
    print(f"source_sheet_id={sheet_id()}")
    print(f"source_gid={sheet_gid()}")
    print(f"kafka_bootstrap_servers={bootstrap_servers()}")
    print(f"rows_read={rows_read}")
    if details:
        for key, value in details.items():
            print(f"{key}={value}")
    print(f"sqlite_db={db_path}")
