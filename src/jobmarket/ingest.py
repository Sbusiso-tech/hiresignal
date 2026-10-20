"""Pull jobs from the job APIs and the partner SFTP drop and publish them to Kafka.

    python -m jobmarket.ingest                         # all configured sources -> Kafka
    python -m jobmarket.ingest --sources remotive      # a single source
    python -m jobmarket.ingest --dry-run out.jsonl     # no Kafka, write events to a file
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import Counter

from jobmarket.config import Settings, get_settings
from jobmarket.models import utc_now
from jobmarket.sources import SOURCES

log = logging.getLogger("jobmarket.ingest")


def run(settings: Settings, source_names: list[str], dry_run_path: str | None = None) -> Counter:
    stats: Counter = Counter()
    producer = None
    sink = open(dry_run_path, "w") if dry_run_path else None

    if sink is None:
        from jobmarket.kafka_io import ensure_topics, make_producer

        ensure_topics(settings)
        producer = make_producer(settings)

    def on_delivery(err, msg):
        if err is not None:
            stats["delivery_failed"] += 1
            log.error("delivery failed for key=%s: %s", msg.key(), err)

    try:
        for name in source_names:
            if name not in SOURCES:
                log.error("unknown source %r (choose from %s)", name, ", ".join(SOURCES))
                continue
            source = SOURCES[name](settings)
            if not source.is_configured():
                log.warning("skipping %s: credentials not set", name)
                stats[f"{name}.skipped"] += 1
                continue
            try:
                for event in source.fetch():
                    if sink:
                        sink.write(event.model_dump_json() + "\n")
                    else:
                        producer.produce(
                            settings.topic_raw,
                            key=event.kafka_key(),
                            value=event.kafka_value(),
                            on_delivery=on_delivery,
                        )
                        producer.poll(0)
                    stats[f"{name}.published"] += 1
                if producer is not None:
                    if producer.flush(30) or stats["delivery_failed"]:
                        raise RuntimeError("not every event was acknowledged by Kafka")
                    source.commit()
            except Exception as exc:
                stats[f"{name}.failed"] += 1
                log.exception("source %s failed: %s", name, exc)
                _dead_letter(producer, settings, {"source": name, "error": repr(exc)})
            for rejected in source.rejected:
                stats[f"{name}.invalid"] += 1
                _dead_letter(producer, settings, rejected)
    finally:
        if producer is not None:
            producer.flush(30)
        if sink is not None:
            sink.close()
    return stats


def _dead_letter(producer, settings: Settings, payload: dict) -> None:
    log.warning("dead-lettering record from %s: %s", payload.get("source"), str(payload.get("error"))[:200])
    if producer is None:
        return
    payload = {**payload, "failed_at": utc_now().isoformat()}
    producer.produce(
        settings.topic_dlq, key=str(payload.get("source")).encode(), value=json.dumps(payload, default=str).encode()
    )


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = get_settings()
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--sources", default=",".join(settings.sources), help="comma separated source names")
    parser.add_argument("--dry-run", metavar="PATH", help="write events to a JSONL file instead of Kafka")
    args = parser.parse_args(argv)

    stats = run(settings, [s.strip() for s in args.sources.split(",") if s.strip()], args.dry_run)
    log.info("ingestion finished: %s", dict(stats))
    published = sum(v for k, v in stats.items() if k.endswith(".published"))
    return 0 if published and not stats["delivery_failed"] else 1


if __name__ == "__main__":
    sys.exit(main())
