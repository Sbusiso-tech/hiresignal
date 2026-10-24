"""Export the warehouse to one JSON file for the static HireSignal dashboard (GitHub Pages).

    python -m jobmarket.export_site > site/data/hiresignal.json
    python -m jobmarket.export_site --include-simulated > site/data/hiresignal.json

The file is written to stdout so it can be redirected from ``docker compose run -T``. Progress goes to stderr.
By default the simulated partner SFTP drop and CRM are left out, because their companies are fictional and the
public dashboard should describe the real market only.
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

SIMULATED_SOURCES = ("partner_sftp", "crm_postgres")
SCHEMA_VERSION = 1

# Every query sees the same filtered set of postings. %(sim)s = include simulated sources.
_JOBS = """
jobs as (
    select * from marts.fct_job_postings
    where %(sim)s or source not in ('partner_sftp', 'crm_postgres')
),
skills as (
    select b.job_id, b.skill, b.skill_category
    from marts.bridge_job_skill b
    inner join jobs j on j.job_id = b.job_id
)"""

QUERIES: dict[str, str] = {
    "kpis": f"""
        with {_JOBS}
        select
            count(*) as jobs,
            count(*) filter (where is_tech_role) as tech_jobs,
            count(distinct company) filter (where company <> 'Unknown') as companies,
            (select count(distinct skill) from skills) as skills,
            count(*) filter (where is_remote) as remote_jobs,
            count(salary_usd_mid) filter (where not salary_is_predicted) as jobs_with_salary,
            round(percentile_cont(0.5) within group (order by salary_usd_mid)
                  filter (where not salary_is_predicted)::numeric, 0) as median_salary_usd,
            min(posted_date) as first_posted,
            max(posted_date) as last_posted,
            max(last_seen_at) as last_seen_at
        from jobs
    """,
    "sources": f"""
        with {_JOBS}
        select source, count(*) as jobs from jobs group by source order by jobs desc
    """,
    "skills": f"""
        with {_JOBS},
        totals as (
            select count(*) filter (where is_tech_role) as tech_jobs,
                   count(*) filter (where is_tech_role and posted_date >= current_date - 30) as tech_jobs_30d
            from jobs
        )
        select
            s.skill,
            min(s.skill_category) as category,
            count(distinct j.job_id) as jobs,
            round(100.0 * count(distinct j.job_id) / nullif(max(t.tech_jobs), 0), 2) as pct,
            count(distinct j.job_id) filter (where j.posted_date >= current_date - 30) as jobs_30d,
            count(distinct j.job_id) filter (where j.is_remote) as remote_jobs,
            count(distinct j.company) as companies,
            round(percentile_cont(0.5) within group (order by j.salary_usd_mid)
                  filter (where not j.salary_is_predicted)::numeric, 0) as median_salary_usd,
            count(j.salary_usd_mid) filter (where not j.salary_is_predicted) as salary_n
        from skills s
        inner join jobs j on j.job_id = s.job_id
        cross join totals t
        group by s.skill
        order by jobs desc, s.skill
    """,
    "months": f"""
        with {_JOBS}
        select posted_month as month, count(*) filter (where is_tech_role) as tech_jobs, count(*) as jobs
        from jobs
        where posted_month is not null
        group by posted_month
        order by posted_month desc
        limit 12
    """,
    "monthly": f"""
        with {_JOBS}
        select j.posted_month as month, s.skill, count(distinct j.job_id) as jobs
        from skills s
        inner join jobs j on j.job_id = s.job_id
        where j.posted_month is not null
        group by j.posted_month, s.skill
    """,
    "families": f"""
        with {_JOBS},
        family_totals as (select job_family, count(*) as family_jobs from jobs group by job_family),
        counts as (
            select j.job_family, s.skill, count(distinct j.job_id) as jobs,
                   row_number() over (partition by j.job_family order by count(distinct j.job_id) desc, s.skill) as rn
            from skills s
            inner join jobs j on j.job_id = s.job_id
            group by j.job_family, s.skill
        )
        select c.job_family as family, f.family_jobs, c.skill, c.jobs,
               round(100.0 * c.jobs / nullif(f.family_jobs, 0), 2) as pct
        from counts c
        inner join family_totals f on f.job_family = c.job_family
        where c.rn <= 15
        order by f.family_jobs desc, c.rn
    """,
    "salaries": f"""
        with {_JOBS}
        select
            s.skill,
            count(*) as n,
            round(percentile_cont(0.25) within group (order by j.salary_usd_mid)::numeric, 0) as p25,
            round(percentile_cont(0.50) within group (order by j.salary_usd_mid)::numeric, 0) as median,
            round(percentile_cont(0.75) within group (order by j.salary_usd_mid)::numeric, 0) as p75
        from skills s
        inner join jobs j on j.job_id = s.job_id
        where j.salary_usd_mid is not null and not j.salary_is_predicted
        group by s.skill
        having count(*) >= 5
        order by median desc
    """,
    "pairs": f"""
        with {_JOBS},
        skill_totals as (select skill, count(*) as skill_jobs from skills group by skill),
        pairs as (
            select a.skill as skill, b.skill as related, count(*) as together
            from skills a
            inner join skills b on a.job_id = b.job_id and a.skill <> b.skill
            group by a.skill, b.skill
            having count(*) >= 3
        ),
        ranked as (
            select p.*, row_number() over (partition by p.skill order by p.together desc, p.related) as rn
            from pairs p
        )
        select r.skill, r.related, r.together, round(100.0 * r.together / t.skill_jobs, 1) as pct
        from ranked r
        inner join skill_totals t on t.skill = r.skill
        where r.rn <= 8
        order by r.skill, r.rn
    """,
    "seniority": f"""
        with {_JOBS}
        select coalesce(seniority, 'unspecified') as label, count(*) as jobs from jobs group by 1 order by jobs desc
    """,
    "employment": f"""
        with {_JOBS}
        select coalesce(employment_type, 'unspecified') as label, count(*) as jobs
        from jobs group by 1 order by jobs desc
    """,
    "countries": f"""
        with {_JOBS}
        select coalesce(nullif(country, ''), 'Unknown') as label, count(*) as jobs
        from jobs group by 1 order by jobs desc limit 12
    """,
    "companies": f"""
        with {_JOBS},
        company_skills as (
            select j.company, s.skill,
                   row_number() over (partition by j.company order by count(*) desc, s.skill) as rn
            from skills s
            inner join jobs j on j.job_id = s.job_id
            group by j.company, s.skill
        )
        select j.company, count(*) as jobs, count(*) filter (where j.is_remote) as remote_jobs,
               (select string_agg(cs.skill, ', ' order by cs.rn)
                from company_skills cs where cs.company = j.company and cs.rn <= 5) as top_skills
        from jobs j
        where j.is_tech_role and j.company <> 'Unknown'
        group by j.company
        order by jobs desc, j.company
        limit 25
    """,
    "latest": f"""
        with {_JOBS}
        select j.job_title as title, j.company, j.location, j.is_remote as remote, j.job_family as family,
               j.seniority, j.salary_usd_min, j.salary_usd_max, j.salary_is_predicted, j.posted_date, j.source, j.url,
               (select array_agg(s.skill order by s.skill) from skills s where s.job_id = j.job_id) as skills
        from jobs j
        where j.is_tech_role and j.url is not null
        order by j.posted_at desc nulls last, j.job_id
        limit 60
    """,
}


def jsonable(value: Any) -> Any:
    """Turn database values (Decimal, date, datetime) into plain JSON values."""
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat(timespec="seconds") if value.tzinfo else value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(v) for v in value]
    return value


def shape(raw: dict[str, list[dict]], include_simulated: bool, generated_at: datetime) -> dict:
    """Assemble query results into the payload the dashboard reads. Pure, so it can be unit tested."""
    raw = {name: jsonable(rows) for name, rows in raw.items()}

    months = sorted(row["month"] for row in raw["months"])
    month_set = set(months)
    trend: dict[str, dict[str, int]] = defaultdict(dict)
    for row in raw["monthly"]:
        if row["month"] in month_set:
            trend[row["skill"]][row["month"]] = row["jobs"]

    families: dict[str, dict] = {}
    for row in raw["families"]:
        family = families.setdefault(row["family"], {"family": row["family"], "jobs": row["family_jobs"], "skills": []})
        family["skills"].append({"skill": row["skill"], "jobs": row["jobs"], "pct": row["pct"]})

    related: dict[str, list[dict]] = defaultdict(list)
    for row in raw["pairs"]:
        related[row["skill"]].append({"skill": row["related"], "together": row["together"], "pct": row["pct"]})

    skills = []
    for rank, row in enumerate(raw["skills"], start=1):
        skill = row["skill"]
        skills.append({
            **row,
            "rank": rank,
            "trend": [trend.get(skill, {}).get(month, 0) for month in months],
            "related": related.get(skill, []),
        })

    kpis = raw["kpis"][0] if raw["kpis"] else {}
    companies = [
        {**row, "top_skills": [s for s in (row.get("top_skills") or "").split(", ") if s]}
        for row in raw["companies"]
    ]

    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": jsonable(generated_at),
        "include_simulated": include_simulated,
        "kpis": kpis,
        "sources": raw["sources"],
        "months": months,
        "month_totals": {row["month"]: row["tech_jobs"] for row in raw["months"]},
        "skills": skills,
        "families": sorted(families.values(), key=lambda f: -f["jobs"]),
        "salaries": raw["salaries"],
        "mix": {"seniority": raw["seniority"], "employment": raw["employment"], "countries": raw["countries"]},
        "companies": companies,
        "latest": raw["latest"],
    }


def fetch(dsn: str, include_simulated: bool) -> dict[str, list[dict]]:
    import psycopg
    from psycopg.rows import dict_row

    raw: dict[str, list[dict]] = {}
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        conn.read_only = True
        for name, sql in QUERIES.items():
            raw[name] = conn.execute(sql, {"sim": include_simulated}).fetchall()
            print(f"  {name:<11} {len(raw[name]):>6} rows", file=sys.stderr)
    return raw


def main(argv: list[str] | None = None) -> int:
    from jobmarket.config import get_settings

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--include-simulated", action="store_true",
                        help="also include the fictional partner SFTP and CRM postings")
    args = parser.parse_args(argv)

    print("exporting marts for the HireSignal site...", file=sys.stderr)
    raw = fetch(get_settings().pg_dsn, args.include_simulated)
    if not raw["kpis"] or not raw["kpis"][0]["jobs"]:
        print("the warehouse is empty: run `make pipeline` first", file=sys.stderr)
        return 1
    payload = shape(raw, args.include_simulated, datetime.now(timezone.utc))
    json.dump(payload, sys.stdout, ensure_ascii=False, separators=(",", ":"))
    sys.stdout.write("\n")
    print(f"done: {payload['kpis']['jobs']} postings, {len(payload['skills'])} skills", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
