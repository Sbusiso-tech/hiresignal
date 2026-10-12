"""REST API over the dbt marts (batch layer) and Redis (speed layer).

    uvicorn jobmarket.api.main:app --reload
    open http://localhost:8000/docs
"""
from __future__ import annotations

import json
from contextlib import asynccontextmanager
from typing import Any

import redis
from fastapi import FastAPI, HTTPException, Query
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from jobmarket.config import get_settings

settings = get_settings()
state: dict[str, Any] = {}


@asynccontextmanager
async def lifespan(_: FastAPI):
    state["pool"] = ConnectionPool(settings.pg_dsn, min_size=1, max_size=5, kwargs={"row_factory": dict_row})
    state["redis"] = redis.Redis.from_url(settings.redis_url, decode_responses=True)
    yield
    state["pool"].close()


app = FastAPI(
    title="Job Market Intelligence API",
    description="What technologies are employers asking for? Batch marts from dbt + real-time counts from Redis.",
    version="1.0.0",
    lifespan=lifespan,
)


def query(sql: str, params: dict | None = None) -> list[dict]:
    try:
        with state["pool"].connection() as conn:
            return conn.execute(sql, params or {}).fetchall()
    except Exception as exc:
        raise HTTPException(503, f"warehouse not ready (run the pipeline first): {exc.__class__.__name__}") from exc


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/skills/top", summary="Most in-demand skills (batch layer)")
def top_skills(
    limit: int = Query(25, ge=1, le=200),
    category: str | None = Query(None, description="e.g. 'Data Engineering', 'Cloud Platform'"),
) -> list[dict]:
    return query(
        """
        select demand_rank, skill, skill_category, job_count, pct_of_tech_jobs, job_count_30d,
               median_salary_usd, salary_sample_size
        from marts.mart_skill_demand
        where %(category)s::text is null or skill_category = %(category)s
        order by job_count desc
        limit %(limit)s
        """,
        {"limit": limit, "category": category},
    )


@app.get("/skills/realtime", summary="Live skill counts from the streaming layer (Redis)")
def realtime_skills(limit: int = Query(25, ge=1, le=200), source: str | None = None) -> dict:
    r: redis.Redis = state["redis"]
    key = f"rt:skills:{source}" if source else "rt:skills"
    return {
        "stats": r.hgetall("rt:stats"),
        "skills": [{"skill": s, "job_count": int(c)} for s, c in r.zrevrange(key, 0, limit - 1, withscores=True)],
    }


@app.get("/jobs/latest", summary="Newest postings captured via Debezium CDC (requires the 'cdc' profile)")
def latest_jobs(limit: int = Query(20, ge=1, le=100)) -> dict:
    r: redis.Redis = state["redis"]
    return {
        "cdc_counters": r.hgetall("rt:cdc"),
        "jobs": [json.loads(item) for item in r.lrange("rt:latest_jobs", 0, limit - 1)],
    }


@app.get("/skills/{skill}/trend", summary="Monthly demand for one skill")
def skill_trend(skill: str) -> list[dict]:
    return query(
        """
        select posted_month, job_count, pct_of_tech_jobs, change_vs_prev_month
        from marts.mart_skill_demand_monthly
        where lower(skill) = lower(%(skill)s)
        order by posted_month
        """,
        {"skill": skill},
    )


@app.get("/skills/{skill}/related", summary="Skills most often requested together with this one")
def related_skills(skill: str, limit: int = Query(10, ge=1, le=50)) -> list[dict]:
    return query(
        """
        select case when lower(skill_a) = lower(%(skill)s) then skill_b else skill_a end as related_skill,
               jobs_together,
               case when lower(skill_a) = lower(%(skill)s) then pct_of_skill_a_jobs else pct_of_skill_b_jobs end
                   as pct_of_jobs_with_skill
        from marts.mart_skill_pairs
        where lower(skill_a) = lower(%(skill)s) or lower(skill_b) = lower(%(skill)s)
        order by jobs_together desc
        limit %(limit)s
        """,
        {"skill": skill, "limit": limit},
    )


@app.get("/salaries/by-skill", summary="Annual salary percentiles in USD per skill")
def salaries_by_skill(limit: int = Query(25, ge=1, le=200)) -> list[dict]:
    return query(
        "select * from marts.mart_salary_by_skill order by median_salary_usd desc limit %(limit)s", {"limit": limit}
    )


@app.get("/roles/{job_family}/skills", summary="Top skills for a role family, e.g. 'Data Engineering'")
def skills_for_family(job_family: str, limit: int = Query(20, ge=1, le=100)) -> list[dict]:
    return query(
        """
        select rank_in_family, skill, skill_category, job_count, pct_of_family_jobs
        from marts.mart_skill_by_job_family
        where lower(job_family) = lower(%(family)s)
        order by rank_in_family
        limit %(limit)s
        """,
        {"family": job_family, "limit": limit},
    )


@app.get("/companies/top", summary="Companies hiring the most tech roles")
def top_companies(limit: int = Query(20, ge=1, le=100)) -> list[dict]:
    return query("select * from marts.mart_top_companies order by job_count desc limit %(limit)s", {"limit": limit})


@app.get("/jobs", summary="Search job postings")
def search_jobs(
    q: str | None = Query(None, description="text to match in the job title"),
    skill: str | None = None,
    job_family: str | None = None,
    remote: bool | None = None,
    limit: int = Query(50, ge=1, le=500),
) -> list[dict]:
    return query(
        """
        select f.job_id, f.job_title, f.company, f.location, f.job_family, f.seniority, f.employment_type,
               f.is_remote, f.salary_usd_min, f.salary_usd_max, f.posted_date, f.url
        from marts.fct_job_postings f
        where (%(q)s::text is null or f.job_title ilike '%%' || %(q)s || '%%')
          and (%(family)s::text is null or lower(f.job_family) = lower(%(family)s))
          and (%(remote)s::boolean is null or f.is_remote = %(remote)s)
          and (%(skill)s::text is null or exists (
                select 1 from marts.bridge_job_skill b
                where b.job_id = f.job_id and lower(b.skill) = lower(%(skill)s)))
        order by f.posted_at desc nulls last
        limit %(limit)s
        """,
        {"q": q, "skill": skill, "family": job_family, "remote": remote, "limit": limit},
    )


@app.get("/pipeline/runs", summary="Recent Spark batch runs (data lineage / observability)")
def pipeline_runs(limit: int = Query(10, ge=1, le=100)) -> list[dict]:
    return query("select * from core.pipeline_runs order by started_at desc limit %(limit)s", {"limit": limit})
