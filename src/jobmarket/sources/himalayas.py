"""Himalayas remote jobs API (no key). https://himalayas.app/api"""
from __future__ import annotations

import logging
import time
from collections.abc import Iterator

from jobmarket.models import RawJobEvent
from jobmarket.sources.base import JobSource, parse_datetime

log = logging.getLogger(__name__)

SEARCH_URL = "https://himalayas.app/jobs/api/search"


class HimalayasSource(JobSource):
    name = "himalayas"

    def iter_raw(self) -> Iterator[tuple[dict, dict]]:
        seen: set[str] = set()
        for term in self.settings.search_terms:
            for page in range(1, self.settings.himalayas_max_pages + 1):
                jobs = self._get(SEARCH_URL, params={"q": term, "page": page}).get("jobs") or []
                log.info("himalayas term=%r page=%d jobs=%d", term, page, len(jobs))
                for job in jobs:
                    if job.get("guid") not in seen:
                        seen.add(job.get("guid"))
                        yield job, {"search_term": term}
                if not jobs:
                    break
                time.sleep(0.5)

    def to_event(self, job: dict, search_term: str | None = None) -> RawJobEvent:
        locations = job.get("locationRestrictions") or []
        seniority = job.get("seniority") or []
        return RawJobEvent(
            job_id=self.job_id(job["guid"].rstrip("/").split("/companies/", 1)[-1]),
            source=self.name,
            source_id=job["guid"],
            search_term=search_term,
            job_title=job.get("title") or "",
            company=job.get("companyName"),
            location=", ".join(locations) or "Worldwide",
            country=locations[0] if len(locations) == 1 else None,
            is_remote=True,
            salary_min=job.get("minSalary"),
            salary_max=job.get("maxSalary"),
            salary_currency=job.get("currency"),
            salary_period=job.get("salaryPeriod"),
            employment_type_raw=", ".join(v for v in [job.get("employmentType"), *seniority] if v) or None,
            description=job.get("description"),
            tags=job.get("categories") or [],
            date_posted=parse_datetime(job.get("pubDate")),
            url=job.get("applicationLink") or job.get("guid"),
            raw=job,
        )
