from __future__ import annotations

from importlib import resources

import psycopg

from jobmarket.config import Settings


def _sql(name: str) -> str:
    return resources.files("jobmarket").joinpath(f"sql/{name}").read_text()


def connect(settings: Settings) -> psycopg.Connection:
    return psycopg.connect(settings.pg_dsn)


def ensure_schema(settings: Settings) -> None:
    with connect(settings) as conn:
        conn.execute(_sql("schema.sql"))


def merge_landing_into_core(conn: psycopg.Connection) -> dict[str, int]:
    """Upsert ``landing.*_batch`` into ``core.*``. Runs inside the caller's transaction."""
    inserted, updated = conn.execute(_sql("upsert.sql")).fetchone()
    conn.execute(
        "delete from core.job_skills where job_id in (select job_id from landing.jobs_batch)"
    )
    skill_rows = conn.execute(
        """
        insert into core.job_skills (job_id, skill, skill_category)
        select distinct job_id, skill, skill_category from landing.job_skills_batch
        on conflict do nothing
        """
    ).rowcount
    return {"jobs_inserted": inserted, "jobs_updated": updated, "skill_rows": skill_rows}
