"""Recruitment desk CRM: vacancies from an operational PostgreSQL database.

Batch extraction reads every vacancy changed in the last ``CRM_LOOKBACK_HOURS`` through a read-only
session. The window overlaps between runs on purpose; downstream upserts are idempotent, so a
re-read row costs nothing while a missed run never loses changes.
"""
from __future__ import annotations

import logging
from collections.abc import Iterator

from jobmarket.models import RawJobEvent
from jobmarket.sources.base import JobSource

log = logging.getLogger(__name__)

QUERY = """
select v.vacancy_id, v.title, c.name as client, c.industry, v.city, v.province, v.work_mode,
       v.employment_type, v.salary_min_zar, v.salary_max_zar, v.min_experience_years,
       v.required_skills, v.description, v.status, r.full_name as recruiter,
       v.opened_at, v.updated_at
  from crm.vacancies v
  join crm.clients c using (client_id)
  left join crm.recruiters r using (recruiter_id)
 where v.updated_at >= now() - make_interval(hours => %s)
 order by v.updated_at
"""


def _float(value) -> float | None:
    return float(value) if value is not None else None


class CrmPostgresSource(JobSource):
    name = "crm_postgres"

    def is_configured(self) -> bool:
        return bool(self.settings.crm_host and self.settings.crm_password)

    def iter_raw(self) -> Iterator[tuple[dict, dict]]:
        import psycopg
        from psycopg.rows import dict_row

        with psycopg.connect(self.settings.crm_dsn, row_factory=dict_row) as conn:
            conn.read_only = True
            rows = conn.execute(QUERY, (self.settings.crm_lookback_hours,)).fetchall()
        log.info("crm_postgres vacancies changed in the last %dh: %d", self.settings.crm_lookback_hours, len(rows))
        for row in rows:
            yield row, {}

    def to_event(self, row: dict) -> RawJobEvent:
        years = row.get("min_experience_years")
        return RawJobEvent(
            job_id=self.job_id(row["vacancy_id"]),
            source=self.name,
            source_id=str(row["vacancy_id"]),
            job_title=row.get("title") or "",
            company=row.get("client"),
            location=", ".join(v for v in (row.get("city"), row.get("province")) if v) or None,
            country="ZA",
            is_remote=row.get("work_mode") == "Remote",
            salary_min=_float(row.get("salary_min_zar")),
            salary_max=_float(row.get("salary_max_zar")),
            salary_currency="ZAR",
            salary_period="month",
            employment_type_raw=row.get("employment_type"),
            experience_months=years * 12 if years is not None else None,
            description=row.get("description"),
            tags=row.get("required_skills") or [],
            date_posted=row.get("opened_at"),
            raw=row,
        )
