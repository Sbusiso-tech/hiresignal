"""Partner SFTP drop: CSV contract, simulator and the read -> archive cycle (in-memory SFTP)."""
import csv
import io
import posixpath
import stat
from contextlib import contextmanager
from dataclasses import replace
from datetime import date
from types import SimpleNamespace

import pytest

from jobmarket import partner_feed, sftp_io
from jobmarket.config import Settings
from jobmarket.sources import SOURCES
from jobmarket.transform import enrich

SETTINGS = replace(Settings(), sftp_host="sftp", sftp_password="secret")
DAY = date(2026, 10, 14)


class _Upload(io.BytesIO):
    def __init__(self, server, path):
        super().__init__()
        self.server, self.path = server, path

    def close(self):
        self.server.files[self.path] = self.getvalue()
        super().close()


class FakeSftp:
    def __init__(self):
        self.files: dict[str, bytes] = {}
        self.dirs = {"upload"}

    def listdir(self, path):
        if path not in self.dirs:
            raise FileNotFoundError(path)
        return [posixpath.basename(p) for p in self.files if posixpath.dirname(p) == path]

    def open(self, path, mode="rb"):
        return _Upload(self, path) if "w" in mode else io.BytesIO(self.files[path])

    def stat(self, path):
        if path not in self.dirs:
            raise FileNotFoundError(path)
        return SimpleNamespace(st_mode=stat.S_IFDIR)

    def mkdir(self, path):
        self.dirs.add(path)

    def posix_rename(self, src, dst):
        self.files[dst] = self.files.pop(src)


@pytest.fixture
def server(monkeypatch):
    fake = FakeSftp()

    @contextmanager
    def connect(settings):
        yield fake

    monkeypatch.setattr(sftp_io, "connect", connect)
    return fake


def make_source():
    return SOURCES["partner_sftp"](SETTINGS, session=object())


ROW = {
    "reference": "TP261014-001", "title": "Senior Data Engineer", "company": "Karoo Data Labs",
    "city": "Cape Town", "province": "Western Cape", "work_mode": "Hybrid", "contract_type": "Permanent",
    "salary_min": "65000", "salary_max": "90000", "currency": "ZAR", "salary_period": "month",
    "min_experience_years": "5", "skills": "Python|SQL|Apache Airflow",
    "description": "Build pipelines with Python, SQL and Apache Airflow.", "posted_date": "2026-10-14",
    "apply_url": "https://careers.partner.example.co.za/jobs/TP261014-001",
}


def test_mapping_and_enrichment():
    event = make_source().to_event(ROW, filename="jobs_2026-10-14.csv")
    assert event.job_id == "partner_sftp:TP261014-001"
    assert (event.location, event.country, event.is_remote) == ("Cape Town, Western Cape", "ZA", False)
    assert event.experience_months == 60
    assert event.raw["_file"] == "jobs_2026-10-14.csv"

    curated = enrich(event.model_dump())
    assert curated["employment_type"] == "full_time"
    assert curated["seniority"] == "senior"
    assert (curated["salary_annual_min"], curated["salary_annual_max"]) == (780_000, 1_080_000)
    assert {s["skill"] for s in curated["skills"]} >= {"Python", "SQL", "Apache Airflow"}


def test_blank_salary_and_bad_rows():
    event = make_source().to_event({**ROW, "salary_min": "", "salary_max": ""})
    assert event.salary_min is None and event.salary_max is None

    source = make_source()
    source.iter_raw = lambda: iter([({**ROW, "reference": ""}, {}), ({**ROW, "title": ""}, {}), (ROW, {})])
    assert [e.source_id for e in source.fetch()] == ["TP261014-001"]
    assert len(source.rejected) == 2


def test_simulator_is_deterministic_and_matches_the_contract():
    rows = partner_feed.generate_rows(DAY, rows=50)
    assert rows == partner_feed.generate_rows(DAY, rows=50)
    assert len({r["reference"] for r in rows}) == 50
    parsed = list(csv.DictReader(io.StringIO(partner_feed.to_csv(rows))))
    assert parsed == rows
    events = [make_source().to_event(r) for r in parsed if r["title"]]
    assert all(e.salary_currency == "ZAR" and e.date_posted.date() == DAY for e in events)


def test_upload_read_and_archive_cycle(server):
    partner_feed.upload(SETTINGS, DAY, partner_feed.to_csv(partner_feed.generate_rows(DAY, rows=5)))
    assert list(server.files) == ["upload/exports/jobs_2026-10-14.csv"]

    source = make_source()
    assert len(list(source.fetch())) + len(source.rejected) == 5
    assert source.consumed_files == ["jobs_2026-10-14.csv"]

    source.commit()
    assert list(server.files) == ["upload/archive/jobs_2026-10-14.csv"]
    assert list(make_source().fetch()) == []


def test_files_stay_in_the_inbox_until_commit(server):
    server.dirs.add("upload/exports")
    server.files["upload/exports/jobs_2026-10-14.csv"] = partner_feed.to_csv([ROW]).encode()
    server.files["upload/exports/.jobs_2026-10-15.csv.part"] = b"half written"
    server.files["upload/exports/notes.txt"] = b"ignore me"

    assert [e.source_id for e in make_source().fetch()] == ["TP261014-001"]
    assert "upload/exports/jobs_2026-10-14.csv" in server.files


def test_missing_inbox_means_no_jobs(server):
    assert list(make_source().fetch()) == []


def test_not_configured_without_host():
    assert not SOURCES["partner_sftp"](Settings(), session=object()).is_configured()
