"""Kafka -> raw data lake (bronze layer).

Consumes ``jobs.raw`` and writes gzipped JSON Lines files to object storage:

    s3://<bucket>/raw/jobs/source=<source>/ingest_date=<YYYY-MM-DD>/part-<ts>-<uuid>.jsonl.gz

Offsets are committed only after a file is durably written (at-least-once);
duplicates are removed downstream in Spark by ``job_id``.

    python -m jobmarket.lake_sink                   # run forever (streaming service)
    python -m jobmarket.lake_sink --until-idle 30   # drain the topic then exit (batch / Airflow)
"""
from __future__ import annotations

import argparse
import gzip
import json
import logging
import signal
import sys
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone

from confluent_kafka import KafkaError

from jobmarket.config import Settings, get_settings
from jobmarket.kafka_io import ensure_topics, make_consumer
from jobmarket.lake import ensure_bucket, s3_client

log = logging.getLogger("jobmarket.lake_sink")

GROUP_ID = "lake-sink"


class LakeSink:
    def __init__(self, settings: Settings, max_records: int = 500, flush_seconds: float = 20.0):
        self.settings = settings
        self.max_records = max_records
        self.flush_seconds = flush_seconds
        self.s3 = s3_client(settings)
        ensure_bucket(self.s3, settings.lake_bucket)
        self.buffer: dict[tuple[str, str], list[bytes]] = defaultdict(list)
        self.buffered = 0
        self.last_flush = time.monotonic()
        self.files_written = 0
        self.records_written = 0

    def add(self, value: bytes) -> None:
        try:
            event = json.loads(value)
            source = event["source"]
            ingest_date = event["ingested_at"][:10]
        except (ValueError, KeyError, TypeError):
            log.warning("skipping undecodable message")
            return
        self.buffer[(source, ingest_date)].append(value)
        self.buffered += 1

    def should_flush(self) -> bool:
        return self.buffered >= self.max_records or (
            self.buffered > 0 and time.monotonic() - self.last_flush >= self.flush_seconds
        )

    def flush(self) -> None:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        for (source, ingest_date), lines in self.buffer.items():
            key = f"raw/jobs/source={source}/ingest_date={ingest_date}/part-{stamp}-{uuid.uuid4().hex[:8]}.jsonl.gz"
            body = gzip.compress(b"\n".join(lines) + b"\n")
            self.s3.put_object(Bucket=self.settings.lake_bucket, Key=key, Body=body)
            self.files_written += 1
            self.records_written += len(lines)
            log.info("wrote %d records to s3://%s/%s", len(lines), self.settings.lake_bucket, key)
        self.buffer.clear()
        self.buffered = 0
        self.last_flush = time.monotonic()


def run(settings: Settings, until_idle: float | None) -> int:
    ensure_topics(settings)
    sink = LakeSink(settings)
    consumer = make_consumer(settings, GROUP_ID)
    consumer.subscribe([settings.topic_raw])

    stopping = False

    def stop(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    last_message = time.monotonic()
    try:
        while not stopping:
            msg = consumer.poll(1.0)
            if msg is None:
                if until_idle is not None and time.monotonic() - last_message >= until_idle:
                    log.info("no new messages for %.0fs, draining and exiting", until_idle)
                    break
            elif msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("kafka error: %s", msg.error())
            else:
                last_message = time.monotonic()
                sink.add(msg.value())

            if sink.should_flush():
                sink.flush()
                consumer.commit(asynchronous=False)
        if sink.buffered:
            sink.flush()
            consumer.commit(asynchronous=False)
    finally:
        consumer.close()
    log.info("lake sink stopped: files=%d records=%d", sink.files_written, sink.records_written)
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--until-idle", type=float, metavar="SECONDS", help="exit after this many idle seconds")
    args = parser.parse_args(argv)
    return run(get_settings(), args.until_idle)


if __name__ == "__main__":
    sys.exit(main())
