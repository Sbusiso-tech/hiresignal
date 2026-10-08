"""Mapping tests for each source, using payloads shaped like the real API responses."""
from jobmarket.config import Settings
from jobmarket.sources import SOURCES

SETTINGS = Settings()


def make(name):
    return SOURCES[name](SETTINGS, session=object())


def test_remotive_mapping():
    event = make("remotive").to_event(
        {"id": 2091149, "title": "Software Engineer", "company_name": "CodeForAI", "category": "Software Development",
         "tags": ["python", "sql"], "job_type": "contract", "publication_date": "2026-10-05T05:15:43",
         "candidate_required_location": "USA, UK", "salary": "$45-$120/Hour", "description": "<p>hi</p>",
         "url": "https://remotive.com/x"}
    )
    assert event.job_id == "remotive:2091149"
    assert event.salary_text == "$45-$120/Hour"
    assert event.date_posted.tzinfo is not None


def test_himalayas_mapping_builds_unique_id_from_company_and_slug():
    event = make("himalayas").to_event(
        {"title": "Assistant Actuary", "companyName": "WoodmenLife", "employmentType": "Full Time",
         "minSalary": 80000, "maxSalary": 115000, "salaryPeriod": "annual", "seniority": ["Mid-level"],
         "currency": "USD", "locationRestrictions": ["United States"], "categories": ["Actuary"],
         "description": "<h3>x</h3>", "pubDate": 1791195462,
         "guid": "https://himalayas.app/companies/woodmenlife/jobs/assistant-actuary"}
    )
    assert event.job_id == "himalayas:woodmenlife/jobs/assistant-actuary"
    assert event.country == "United States"
    assert event.employment_type_raw == "Full Time, Mid-level"


def test_jobicy_mapping():
    event = make("jobicy").to_event(
        {"id": 154681, "url": "https://jobicy.com/jobs/154681", "jobTitle": "Security Engineer",
         "companyName": "Five9", "jobIndustry": ["Cybersecurity"], "jobType": ["Full-Time"], "jobGeo": "Portugal",
         "jobLevel": "Senior", "jobDescription": "<p>x</p>", "pubDate": "2026-10-06T06:08:56+00:00",
         "salaryMin": 70000, "salaryMax": 150000, "salaryCurrency": "USD", "salaryPeriod": "yearly"}
    )
    assert event.job_id == "jobicy:154681"
    assert event.employment_type_raw == "Full-Time, Senior"


def test_arbeitnow_mapping():
    event = make("arbeitnow").to_event(
        {"slug": "data-engineer-berlin-1", "company_name": "ACME", "title": "Data Engineer", "description": "x",
         "remote": False, "url": "https://arbeitnow.com/x", "tags": ["IT"], "job_types": ["full time"],
         "location": "Berlin", "created_at": 1791280881}
    )
    assert event.job_id == "arbeitnow:data-engineer-berlin-1"
    assert event.is_remote is False


def test_adzuna_mapping_sets_currency_from_country():
    event = make("adzuna").to_event(
        {"id": "4321", "title": "Data Engineer", "company": {"display_name": "Takealot"},
         "location": {"display_name": "Cape Town, Western Cape"}, "salary_min": 600000, "salary_max": 800000,
         "salary_is_predicted": "1", "contract_time": "full_time", "contract_type": "permanent",
         "created": "2026-10-01T10:00:00Z", "redirect_url": "https://adzuna.co.za/x", "description": "SQL"},
        country="za",
    )
    assert (event.salary_currency, event.country, event.salary_is_predicted) == ("ZAR", "ZA", True)


def test_jsearch_mapping():
    event = make("jsearch").to_event(
        {"job_id": "abc==", "job_title": "Data Engineer", "employer_name": "Discovery", "job_city": "Sandton",
         "job_country": "ZA", "job_employment_type": "FULLTIME", "job_min_salary": None,
         "job_required_experience": {"required_experience_in_months": 36}, "job_description": "Python",
         "job_posted_at_datetime_utc": "2026-10-01T00:00:00.000Z"}
    )
    assert event.location == "Sandton, ZA"
    assert event.experience_months == 36


def test_invalid_records_are_rejected_not_raised():
    source = make("remotive")
    source.iter_raw = lambda: iter([({"id": 1, "title": "  "}, {}), ({"id": 2, "title": "Ok"}, {})])
    events = list(source.fetch())
    assert [e.source_id for e in events] == ["2"]
    assert len(source.rejected) == 1
