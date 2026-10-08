import pytest

from jobmarket.transform import (
    annualize,
    enrich,
    extract_experience,
    infer_seniority,
    normalize_employment_type,
    parse_salary_text,
    strip_html,
)


@pytest.mark.parametrize(
    "text, expected",
    [
        ("$90k - $105k", (90_000, 105_000, "USD", "year")),
        ("$45-$120/Hour", (45, 120, "USD", "hour")),
        ("$90 - $150 /hour", (90, 150, "USD", "hour")),
        ("$35,3k- $52k", (35_300, 52_000, "USD", "year")),
        ("OTE $25k - $35k", (25_000, 35_000, "USD", "year")),
        ("€60.000 - €75.000 per year", (60_000, 75_000, "EUR", "year")),
        ("£50,000", (50_000, 50_000, "GBP", "year")),
        ("R45 000 - R55 000 per month", (45_000, 55_000, "ZAR", "month")),
    ],
)
def test_parse_salary_text(text, expected):
    result = parse_salary_text(text)
    assert (result["salary_min"], result["salary_max"], result["salary_currency"], result["salary_period"]) == expected


@pytest.mark.parametrize("text", [None, "", "Competitive", "DOE"])
def test_parse_salary_text_without_numbers(text):
    assert parse_salary_text(text)["salary_min"] is None


def test_enrich_uses_structured_salary_and_normalizes_period():
    record = enrich({"job_title": "Data Engineer", "salary_min": 80000, "salary_max": 115000,
                     "salary_currency": "USD", "salary_period": "annual"})
    assert (record["salary_period"], record["salary_annual_max"]) == ("year", 115000)


def test_enrich_treats_zero_salary_as_missing():
    record = enrich({"job_title": "Data Engineer", "salary_min": 0, "salary_max": 0})
    assert record["salary_annual_min"] is None


def test_annualize():
    assert annualize(50, "hour") == 104_000
    assert annualize(40_000, "month") == 480_000
    assert annualize(None, "hour") is None


@pytest.mark.parametrize(
    "raw, title, expected",
    [
        ("full_time", None, "full_time"),
        ("FULLTIME", None, "full_time"),
        ("freelance", None, "contract"),
        ("Vollzeit, Festanstellung", None, "full_time"),
        ("Teilzeit", None, "part_time"),
        ("permanent, full_time", None, "full_time"),
        (None, "Data Engineering Intern", "internship"),
        (None, "Data Engineer", "unspecified"),
    ],
)
def test_normalize_employment_type(raw, title, expected):
    assert normalize_employment_type(raw, title) == expected


@pytest.mark.parametrize(
    "text, expected",
    [
        ("You have 3-5 years of experience with Python.", (3, 5)),
        ("Minimum of 4 years experience in data engineering", (4, 4)),
        ("5+ years building pipelines", (5, 5)),
        ("2 years of professional experience", (2, 2)),
        ("Mindestens 3 Jahre Berufserfahrung", (3, 3)),
        ("We have been around for 150 years", (None, None)),
        ("No experience required", (None, None)),
    ],
)
def test_extract_experience(text, expected):
    assert extract_experience(text) == expected


def test_extract_experience_prefers_structured_months():
    assert extract_experience("10+ years", months=36) == (3, 3)


@pytest.mark.parametrize(
    "title, years, expected",
    [
        ("Senior Data Engineer", None, "senior"),
        ("Lead Analytics Engineer", None, "lead"),
        ("Junior Python Developer", None, "junior"),
        ("Data Engineer", 1, "junior"),
        ("Data Engineer", 3, "mid"),
        ("Data Engineer", 7, "senior"),
        ("Data Engineer", None, "unspecified"),
    ],
)
def test_infer_seniority(title, years, expected):
    assert infer_seniority(title, years) == expected


def test_strip_html():
    assert strip_html("<p>Hello&nbsp;<b>world</b></p><ul><li>SQL</li></ul>") == "Hello world\nSQL"


def test_enrich_end_to_end():
    record = enrich(
        {
            "job_title": "Senior Data Engineer",
            "company": " Acme ",
            "description": "<p>5+ years with <b>Python</b>, Apache Spark and Kafka on AWS.</p>",
            "salary_text": "$120k - $150k",
            "employment_type_raw": "full_time",
            "tags": ["dbt"],
        }
    )
    assert record["company"] == "Acme"
    assert record["seniority"] == "senior"
    assert record["employment_type"] == "full_time"
    assert record["experience_years_min"] == 5
    assert record["salary_annual_min"] == 120_000
    assert {s["skill"] for s in record["skills"]} == {"Python", "Apache Spark", "Apache Kafka", "AWS", "dbt"}
