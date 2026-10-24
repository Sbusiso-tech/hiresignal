"""Static site export: database rows are reshaped into the JSON the dashboard reads."""
import json
from datetime import date, datetime, timezone
from decimal import Decimal

from jobmarket.export_site import QUERIES, SIMULATED_SOURCES, jsonable, shape

GENERATED = datetime(2026, 10, 24, 6, 0, tzinfo=timezone.utc)
AUG, SEP, OCT = date(2026, 8, 1), date(2026, 9, 1), date(2026, 10, 1)


def raw_rows() -> dict[str, list[dict]]:
    return {
        "kpis": [{"jobs": 3, "tech_jobs": 3, "companies": 2, "skills": 2, "remote_jobs": 1,
                  "jobs_with_salary": 1, "median_salary_usd": Decimal("120000"), "first_posted": AUG,
                  "last_posted": date(2026, 10, 20), "last_seen_at": GENERATED}],
        "sources": [{"source": "himalayas", "jobs": 3}],
        "skills": [
            {"skill": "Python", "category": "Programming", "jobs": 3, "pct": Decimal("100.00"), "jobs_30d": 1,
             "remote_jobs": 1, "companies": 2, "median_salary_usd": Decimal("120000"), "salary_n": 1},
            {"skill": "SQL", "category": "Data", "jobs": 1, "pct": Decimal("33.33"), "jobs_30d": 0,
             "remote_jobs": 0, "companies": 1, "median_salary_usd": None, "salary_n": 0},
        ],
        # newest first, as the query returns them
        "months": [{"month": OCT, "tech_jobs": 2, "jobs": 2}, {"month": SEP, "tech_jobs": 1, "jobs": 1}],
        "monthly": [
            {"month": OCT, "skill": "Python", "jobs": 2},
            {"month": SEP, "skill": "Python", "jobs": 1},
            {"month": OCT, "skill": "SQL", "jobs": 1},
            {"month": AUG, "skill": "SQL", "jobs": 5},  # older than the 12-month window: ignored
        ],
        "families": [
            {"family": "Data Engineering", "family_jobs": 2, "skill": "Python", "jobs": 2, "pct": Decimal("100")},
            {"family": "Data Engineering", "family_jobs": 2, "skill": "SQL", "jobs": 1, "pct": Decimal("50")},
            {"family": "DevOps / Cloud", "family_jobs": 5, "skill": "Python", "jobs": 1, "pct": Decimal("20")},
        ],
        "salaries": [{"skill": "Python", "n": 5, "p25": Decimal("90000"), "median": Decimal("120000"),
                      "p75": Decimal("150000")}],
        "pairs": [
            {"skill": "Python", "related": "SQL", "together": 1, "pct": Decimal("33.3")},
            {"skill": "SQL", "related": "Python", "together": 1, "pct": Decimal("100.0")},
        ],
        "seniority": [{"label": "senior", "jobs": 3}],
        "employment": [{"label": "full_time", "jobs": 3}],
        "countries": [{"label": "ZA", "jobs": 3}],
        "companies": [{"company": "Acme", "jobs": 2, "remote_jobs": 1, "top_skills": "Python, SQL"},
                      {"company": "Beta", "jobs": 1, "remote_jobs": 0, "top_skills": None}],
        "latest": [{"title": "Data Engineer", "posted_date": date(2026, 10, 20), "url": "https://x.test/1",
                    "skills": ["Python", "SQL"]}],
    }


def test_jsonable_converts_database_types():
    assert jsonable(Decimal("120000")) == 120000 and isinstance(jsonable(Decimal("120000")), int)
    assert jsonable(Decimal("33.33")) == 33.33
    assert jsonable(date(2026, 10, 1)) == "2026-10-01"
    assert jsonable(GENERATED) == "2026-10-24T06:00:00+00:00"
    assert jsonable({"a": [Decimal("1.5"), None]}) == {"a": [1.5, None]}


def test_shape_builds_ranked_skills_with_trend_and_related():
    payload = shape(raw_rows(), include_simulated=False, generated_at=GENERATED)
    assert payload["months"] == ["2026-09-01", "2026-10-01"]
    python, sql = payload["skills"]
    assert (python["rank"], python["trend"], python["pct"]) == (1, [1, 2], 100)
    assert (sql["rank"], sql["trend"]) == (2, [0, 1])
    assert python["related"] == [{"skill": "SQL", "together": 1, "pct": 33.3}]
    assert payload["month_totals"] == {"2026-10-01": 2, "2026-09-01": 1}


def test_shape_groups_families_by_size_and_splits_company_skills():
    payload = shape(raw_rows(), include_simulated=True, generated_at=GENERATED)
    assert [f["family"] for f in payload["families"]] == ["DevOps / Cloud", "Data Engineering"]
    assert [s["skill"] for s in payload["families"][1]["skills"]] == ["Python", "SQL"]
    assert payload["companies"][0]["top_skills"] == ["Python", "SQL"]
    assert payload["companies"][1]["top_skills"] == []
    assert payload["include_simulated"] is True


def test_payload_is_plain_json():
    payload = shape(raw_rows(), include_simulated=False, generated_at=GENERATED)
    assert json.loads(json.dumps(payload))["kpis"]["median_salary_usd"] == 120000


def test_every_query_filters_out_simulated_sources_by_default():
    assert set(SIMULATED_SOURCES) == {"partner_sftp", "crm_postgres"}
    for name, sql in QUERIES.items():
        assert "%(sim)s or source not in ('partner_sftp', 'crm_postgres')" in sql, name
