"""Jobicy remote jobs API (no key). https://jobi.cy/apidocs"""
from __future__ import annotations

import logging
from collections.abc import Iterator

from jobmarket.models import RawJobEvent
from jobmarket.sources.base import JobSource, parse_datetime

log = logging.getLogger(__name__)

API_URL = "https://jobicy.com/api/v2/remote-jobs"


class JobicySource(JobSource):
    name = "jobicy"

    def iter_raw(self) -> Iterator[tuple[dict, dict]]:
        seen: set[str] = set()
        for term in self.settings.search_terms:
            jobs = self._get(API_URL, params={"count": 100, "tag": term}).get("jobs") or []
            log.info("jobicy tag=%r jobs=%d", term, len(jobs))
            for job in jobs:
                if job.get("id") not in seen:
                    seen.add(job.get("id"))
                    yield job, {"search_term": term}

    def to_event(self, job: dict, search_term: str | None = None) -> RawJobEvent:
        employment = [*(job.get("jobType") or []), job.get("jobLevel")]
        return RawJobEvent(
            job_id=self.job_id(job["id"]),
            source=self.name,
            source_id=str(job["id"]),
            search_term=search_term,
            job_title=job.get("jobTitle") or "",
            company=job.get("companyName"),
            location=job.get("jobGeo"),
            is_remote=True,
            salary_min=job.get("salaryMin"),
            salary_max=job.get("salaryMax"),
            salary_currency=job.get("salaryCurrency"),
            salary_period=job.get("salaryPeriod"),
            employment_type_raw=", ".join(v for v in employment if v) or None,
            description=job.get("jobDescription"),
            tags=job.get("jobIndustry") or [],
            date_posted=parse_datetime(job.get("pubDate")),
            url=job.get("url"),
            raw=job,
        )
