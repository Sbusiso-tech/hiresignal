"""Debezium CDC (crm.vacancies) -> Redis feed of the newest vacancy changes.

Debezium tails the recruitment CRM's write-ahead log and publishes every insert,
update and delete on ``crm.vacancies`` to ``cdc.crm.vacancies``. This consumer keeps
a capped list of the newest changes and a counter per operation, so the API can
serve them without touching the operational database.

Redis keys:
    rt:crm:latest   LIST  newest 100 changes (JSON)
    rt:crm:cdc      HASH  inserts / updates / deletes / snapshot_reads counters
"""
from __future__ import annotations

import json
import logging
import signal
import sys
from datetime import datetime, timezone
from typing import Any

import redis
from confluent_kafka import KafkaError

from jobmarket.config import get_settings
from jobmarket.kafka_io import make_consumer

log = logging.getLogger("jobmarket.cdc")

TOPIC = "crmcdc.crm.vacancies"
COUNTERS = {"c": "inserts", "r": "snapshot_reads", "u": "updates", "d": "deletes"}
OP_LABEL = {"c": "insert", "r": "snapshot", "u": "update", "d": "delete"}
KEEP_FIELDS = (
    "vacancy_id", "title", "client_id", "city", "province", "work_mode", "employment_type",
    "salary_min_zar", "salary_max_zar", "status", "opened_at", "updated_at",
)


def _timestamp(value: Any) -> Any:
    """Debezium sends ``timestamptz`` as microseconds since the epoch in adaptive mode."""
    if isinstance(value, int) and value > 10**14:
        return datetime.fromtimestamp(value / 1_000_000, tz=timezone.utc).isoformat()
    return value


def apply_change(store: Any, change: dict) -> None:
    op = change.get("op")
    store.hincrby("rt:crm:cdc", COUNTERS.get(op, "other"), 1)
    row = change.get("before") if op == "d" else change.get("after")
    if op not in OP_LABEL or not row:
        return
    record = {"op": OP_LABEL[op], **{field: _timestamp(row.get(field)) for field in KEEP_FIELDS}}
    store.lpush("rt:crm:latest", json.dumps(record))
    store.ltrim("rt:crm:latest", 0, 99)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = get_settings()
    store = redis.Redis.from_url(settings.redis_url, decode_responses=True)
    consumer = make_consumer(settings, "cdc-crm-vacancies")
    consumer.subscribe([TOPIC])

    stopping = False

    def stop(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    try:
        while not stopping:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() not in (KafkaError._PARTITION_EOF, KafkaError.UNKNOWN_TOPIC_OR_PART):
                    log.error("kafka error: %s", msg.error())
                continue
            if msg.value() is None:
                continue
            apply_change(store, json.loads(msg.value()))
            consumer.commit(message=msg, asynchronous=True)
    finally:
        consumer.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
