"""Remotive public API (no key). https://remotive.com/api-documentation

The free feed returns only the latest ~20 jobs (query params are ignored) and is
delayed 24h, so one call per run is enough. Remotive asks for at most ~4 calls a
day and a link back to the listing, which is why ``url`` is always kept.
"""
from __future__ import annotations

import logging
from collections.abc import Iterator

from jobmarket.models import RawJobEvent
from jobmarket.sources.base import JobSource, parse_datetime

log = logging.getLogger(__name__)

API_URL = "https://remotive.com/api/remote-jobs"


class RemotiveSource(JobSource):
    name = "remotive"

    def iter_raw(self) -> Iterator[tuple[dict, dict]]:
        jobs = self._get(API_URL).get("jobs", [])
        log.info("remotive jobs=%d", len(jobs))
        for job in jobs:
            yield job, {}

    def to_event(self, job: dict) -> RawJobEvent:
        return RawJobEvent(
            job_id=self.job_id(job["id"]),
            source=self.name,
            source_id=str(job["id"]),
            search_term=job.get("category"),
            job_title=job.get("title") or "",
            company=job.get("company_name"),
            location=job.get("candidate_required_location"),
            is_remote=True,
            salary_text=job.get("salary") or None,
            employment_type_raw=job.get("job_type"),
            description=job.get("description"),
            tags=job.get("tags") or [],
            date_posted=parse_datetime(job.get("publication_date")),
            url=job.get("url"),
            raw=job,
        )
