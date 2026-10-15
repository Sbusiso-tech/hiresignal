"""Recruitment desk CRM: source mapping and simulator output against the CRM schema rules."""
import random
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal

from jobmarket.config import Settings
from jobmarket.crm_simulator import new_vacancy
from jobmarket.sources import SOURCES
from jobmarket.transform import enrich

OPENED = datetime(2026, 10, 15, 8, 30, tzinfo=timezone.utc)
ROW = {
    "vacancy_id": 42, "title": "Senior Data Engineer", "client": "Ikhaya Bank", "industry": "Banking",
    "city": "Johannesburg", "province": "Gauteng", "work_mode": "Hybrid", "employment_type": "Permanent",
    "salary_min_zar": Decimal("70000.00"), "salary_max_zar": Decimal("95000.00"), "min_experience_years": 6,
    "required_skills": ["Python", "SQL", "Apache Kafka"], "description": "Build streaming pipelines.",
    "status": "open", "recruiter": "Thandi Mokoena", "opened_at": OPENED, "updated_at": OPENED,
}


def make_source(settings=None):
    return SOURCES["crm_postgres"](settings or Settings(), session=object())


def test_mapping_and_enrichment():
    event = make_source().to_event(ROW)
    assert event.job_id == "crm_postgres:42"
    assert (event.company, event.location, event.country) == ("Ikhaya Bank", "Johannesburg, Gauteng", "ZA")
    assert (event.salary_min, event.salary_max, event.salary_currency) == (70000.0, 95000.0, "ZAR")
    assert event.date_posted == OPENED
    assert event.raw["status"] == "open"
    event.model_dump_json()

    curated = enrich(event.model_dump())
    assert (curated["employment_type"], curated["seniority"]) == ("full_time", "senior")
    assert curated["salary_annual_min"] == 840_000
    assert {s["skill"] for s in curated["skills"]} >= {"Python", "SQL", "Apache Kafka"}


def test_missing_salary_and_experience():
    row = {**ROW, "salary_min_zar": None, "salary_max_zar": None, "min_experience_years": None}
    event = make_source().to_event(row)
    assert event.salary_min is None and event.experience_months is None


def test_simulated_vacancies_respect_the_crm_constraints():
    rng = random.Random(7)
    vacancies = [new_vacancy(rng) for _ in range(500)]
    for v in vacancies:
        assert v["title"].strip()
        assert v["work_mode"] in {"On-site", "Hybrid", "Remote"}
        assert v["employment_type"] in {"Permanent", "Contract", "Temporary", "Internship"}
        assert 0 <= v["min_experience_years"] <= 40
        if v["salary_min_zar"] is not None:
            assert 0 < v["salary_min_zar"] <= v["salary_max_zar"]
    interns = [v for v in vacancies if v["employment_type"] == "Internship"]
    assert interns and all(v["title"].endswith("Intern") and v["min_experience_years"] == 0 for v in interns)


def test_internships_map_to_intern_seniority():
    event = make_source().to_event({**ROW, "title": "Data Analyst Intern", "employment_type": "Internship"})
    curated = enrich(event.model_dump())
    assert (curated["employment_type"], curated["seniority"]) == ("internship", "intern")


def test_configuration():
    assert not make_source().is_configured()
    assert make_source(replace(Settings(), crm_host="crm-db", crm_password="x")).is_configured()
