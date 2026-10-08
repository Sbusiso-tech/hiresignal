"""JSearch on RapidAPI (Google for Jobs aggregator). https://rapidapi.com/letscrape-6bRBa3QguO5/api/jsearch"""
from __future__ import annotations

import logging
from collections.abc import Iterator

from jobmarket.models import RawJobEvent
from jobmarket.sources.base import JobSource, parse_datetime

log = logging.getLogger(__name__)

API_HOST = "jsearch.p.rapidapi.com"
API_URL = f"https://{API_HOST}/search"


class JSearchSource(JobSource):
    name = "jsearch"

    def is_configured(self) -> bool:
        return bool(self.settings.rapidapi_key)

    def iter_raw(self) -> Iterator[tuple[dict, dict]]:
        headers = {"X-RapidAPI-Key": self.settings.rapidapi_key, "X-RapidAPI-Host": API_HOST}
        seen: set[str] = set()
        for term in self.settings.search_terms:
            payload = self._get(
                API_URL,
                headers=headers,
                params={
                    "query": f"{term} in {self.settings.jsearch_location}",
                    "page": 1,
                    "num_pages": self.settings.jsearch_num_pages,
                    "country": self.settings.jsearch_country,
                    "date_posted": "month",
                },
            )
            jobs = payload.get("data") or []
            log.info("jsearch term=%r jobs=%d", term, len(jobs))
            for job in jobs:
                if job.get("job_id") not in seen:
                    seen.add(job.get("job_id"))
                    yield job, {"search_term": term}

    def to_event(self, job: dict, search_term: str | None = None) -> RawJobEvent:
        location = ", ".join(v for v in (job.get("job_city"), job.get("job_state"), job.get("job_country")) if v)
        experience = job.get("job_required_experience") or {}
        tags = list(job.get("job_required_skills") or [])
        return RawJobEvent(
            job_id=self.job_id(job["job_id"]),
            source=self.name,
            source_id=job["job_id"],
            search_term=search_term,
            job_title=job.get("job_title") or "",
            company=job.get("employer_name"),
            location=location or job.get("job_location"),
            country=job.get("job_country"),
            is_remote=job.get("job_is_remote"),
            salary_min=job.get("job_min_salary"),
            salary_max=job.get("job_max_salary"),
            salary_currency=job.get("job_salary_currency"),
            salary_period=(job.get("job_salary_period") or "").lower() or None,
            employment_type_raw=job.get("job_employment_type"),
            experience_months=experience.get("required_experience_in_months"),
            description=job.get("job_description"),
            tags=tags,
            date_posted=parse_datetime(job.get("job_posted_at_datetime_utc")),
            url=job.get("job_apply_link"),
            raw=job,
        )
