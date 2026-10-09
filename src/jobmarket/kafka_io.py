from __future__ import annotations

import logging

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from jobmarket.config import Settings

log = logging.getLogger(__name__)


def ensure_topics(settings: Settings, partitions: int = 3) -> None:
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap})
    existing = set(admin.list_topics(timeout=15).topics)
    missing = [
        NewTopic(topic, num_partitions=partitions, replication_factor=1)
        for topic in (settings.topic_raw, settings.topic_dlq)
        if topic not in existing
    ]
    if not missing:
        return
    for topic, future in admin.create_topics(missing).items():
        try:
            future.result()
            log.info("created topic %s", topic)
        except Exception as exc:  # topic created concurrently by another service
            log.warning("could not create topic %s: %s", topic, exc)


def make_producer(settings: Settings) -> Producer:
    return Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "zstd",
            "linger.ms": 50,
        }
    )


def make_consumer(settings: Settings, group_id: str) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
