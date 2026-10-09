"""Kafka -> Redis real-time skill leaderboard (the stream half of the lambda architecture).

Every *new* job on ``jobs.raw`` has its skills extracted immediately and the
counters in Redis are bumped, so the API can show live demand within seconds of
ingestion, long before the nightly Spark + dbt batch finishes.

Redis keys:
    rt:seen_jobs            SET   job_ids already counted (idempotency on replays)
    rt:skills               ZSET  skill -> job count
    rt:skills:<source>      ZSET  per source
    rt:categories           ZSET  skill category -> job count
    rt:stats                HASH  jobs_total, last_event_at
"""
from __future__ import annotations

import json
import logging
import signal
import sys

import redis
from confluent_kafka import KafkaError

from jobmarket.config import Settings, get_settings
from jobmarket.kafka_io import ensure_topics, make_consumer
from jobmarket.skills import extract_skills
from jobmarket.transform import strip_html

log = logging.getLogger("jobmarket.realtime")

GROUP_ID = "realtime-skills"


def process(r: redis.Redis, event: dict) -> bool:
    if not r.sadd("rt:seen_jobs", event["job_id"]):
        return False
    text = f"{event.get('job_title', '')}\n{strip_html(event.get('description'))}"
    skills = extract_skills(text, event.get("tags"))
    pipe = r.pipeline()
    for skill in skills:
        pipe.zincrby("rt:skills", 1, skill["skill"])
        pipe.zincrby(f"rt:skills:{event['source']}", 1, skill["skill"])
    for category in {s["category"] for s in skills}:
        pipe.zincrby("rt:categories", 1, category)
    pipe.hincrby("rt:stats", "jobs_total", 1)
    pipe.hset("rt:stats", "last_event_at", event.get("ingested_at", ""))
    pipe.execute()
    return True


def run(settings: Settings) -> int:
    ensure_topics(settings)
    r = redis.Redis.from_url(settings.redis_url, decode_responses=True)
    consumer = make_consumer(settings, GROUP_ID)
    consumer.subscribe([settings.topic_raw])

    stopping = False

    def stop(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    processed = 0
    try:
        while not stopping:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("kafka error: %s", msg.error())
                continue
            try:
                if process(r, json.loads(msg.value())):
                    processed += 1
                    if processed % 250 == 0:
                        log.info("processed %d new jobs", processed)
            except (ValueError, KeyError) as exc:
                log.warning("skipping bad message: %s", exc)
            consumer.commit(message=msg, asynchronous=True)
    finally:
        consumer.close()
    return 0


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    return run(get_settings())


if __name__ == "__main__":
    sys.exit(main())
