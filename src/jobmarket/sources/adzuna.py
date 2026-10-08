"""Adzuna search API (free key: https://developer.adzuna.com). Supports South Africa (``za``)."""
from __future__ import annotations

import logging
from collections.abc import Iterator

from jobmarket.models import RawJobEvent
from jobmarket.sources.base import JobSource, parse_datetime

log = logging.getLogger(__name__)

API_URL = "https://api.adzuna.com/v1/api/jobs/{country}/search/{page}"
RESULTS_PER_PAGE = 50

COUNTRY_CURRENCY = {
    "za": "ZAR", "gb": "GBP", "us": "USD", "de": "EUR", "fr": "EUR", "nl": "EUR", "it": "EUR",
    "es": "EUR", "at": "EUR", "be": "EUR", "ca": "CAD", "au": "AUD", "nz": "NZD", "in": "INR",
    "sg": "SGD", "br": "BRL", "mx": "MXN", "pl": "PLN", "ch": "CHF",
}


class AdzunaSource(JobSource):
    name = "adzuna"

    def is_configured(self) -> bool:
        return bool(self.settings.adzuna_app_id and self.settings.adzuna_app_key)

    def iter_raw(self) -> Iterator[tuple[dict, dict]]:
        seen: set[str] = set()
        for country in self.settings.adzuna_countries:
            for term in self.settings.search_terms:
                for page in range(1, self.settings.adzuna_max_pages + 1):
                    payload = self._get(
                        API_URL.format(country=country, page=page),
                        params={
                            "app_id": self.settings.adzuna_app_id,
                            "app_key": self.settings.adzuna_app_key,
                            "results_per_page": RESULTS_PER_PAGE,
                            "what": term,
                            "max_days_old": 30,
                            "content-type": "application/json",
                        },
                    )
                    results = payload.get("results", [])
                    log.info("adzuna country=%s term=%r page=%d jobs=%d", country, term, page, len(results))
                    for job in results:
                        source_id = str(job.get("id"))
                        if source_id not in seen:
                            seen.add(source_id)
                            yield job, {"country": country, "search_term": term}
                    if len(results) < RESULTS_PER_PAGE:
                        break

    def to_event(self, job: dict, country: str, search_term: str | None = None) -> RawJobEvent:
        employment = [v for v in (job.get("contract_time"), job.get("contract_type")) if v]
        return RawJobEvent(
            job_id=self.job_id(job["id"]),
            source=self.name,
            source_id=str(job["id"]),
            search_term=search_term,
            job_title=job.get("title") or "",
            company=(job.get("company") or {}).get("display_name"),
            location=(job.get("location") or {}).get("display_name"),
            country=country.upper(),
            salary_min=job.get("salary_min"),
            salary_max=job.get("salary_max"),
            salary_currency=COUNTRY_CURRENCY.get(country),
            salary_period="year" if job.get("salary_min") or job.get("salary_max") else None,
            salary_is_predicted=str(job.get("salary_is_predicted", "0")) == "1",
            employment_type_raw=", ".join(employment) or None,
            description=job.get("description"),
            tags=[(job.get("category") or {}).get("label")] if job.get("category") else [],
            date_posted=parse_datetime(job.get("created")),
            url=job.get("redirect_url"),
            raw=job,
        )
