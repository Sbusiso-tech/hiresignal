"""Arbeitnow public job board API (no key, Europe focused). https://www.arbeitnow.com/blog/job-board-api"""
from __future__ import annotations

import logging
from collections.abc import Iterator

from jobmarket.models import RawJobEvent
from jobmarket.sources.base import JobSource, parse_datetime

log = logging.getLogger(__name__)

API_URL = "https://www.arbeitnow.com/api/job-board-api"


class ArbeitnowSource(JobSource):
    name = "arbeitnow"

    def iter_raw(self) -> Iterator[tuple[dict, dict]]:
        for page in range(1, self.settings.arbeitnow_max_pages + 1):
            payload = self._get(API_URL, params={"page": page})
            jobs = payload.get("data", [])
            log.info("arbeitnow page=%d jobs=%d", page, len(jobs))
            for job in jobs:
                yield job, {}
            if not jobs or not (payload.get("links") or {}).get("next"):
                break

    def to_event(self, job: dict) -> RawJobEvent:
        return RawJobEvent(
            job_id=self.job_id(job["slug"]),
            source=self.name,
            source_id=job["slug"],
            job_title=job.get("title") or "",
            company=job.get("company_name"),
            location=job.get("location"),
            is_remote=bool(job.get("remote")),
            employment_type_raw=", ".join(job.get("job_types") or []) or None,
            description=job.get("description"),
            tags=job.get("tags") or [],
            date_posted=parse_datetime(job.get("created_at")),
            url=job.get("url"),
            raw=job,
        )
