"""Partner SFTP drop: a recruitment agency delivers a daily CSV export of its job postings.

Files land in ``SFTP_INBOX_DIR`` as ``jobs_YYYY-MM-DD.csv``. Every file found is read and, once
ingestion has handed all of its rows to Kafka, moved to ``SFTP_ARCHIVE_DIR`` so the next run only
sees new deliveries. A failed run leaves the files in the inbox to be picked up again.
"""
from __future__ import annotations

import csv
import io
import logging
import posixpath
import re
from collections.abc import Iterator

from jobmarket.models import RawJobEvent
from jobmarket.sources.base import JobSource, parse_datetime

log = logging.getLogger(__name__)

FILE_PATTERN = re.compile(r"jobs_\d{4}-\d{2}-\d{2}\.csv")

CSV_COLUMNS = [
    "reference", "title", "company", "city", "province", "work_mode", "contract_type",
    "salary_min", "salary_max", "currency", "salary_period", "min_experience_years",
    "skills", "description", "posted_date", "apply_url",
]


def _number(value: str | None) -> float | None:
    value = (value or "").strip()
    return float(value) if value else None


class PartnerSftpSource(JobSource):
    name = "partner_sftp"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.consumed_files: list[str] = []

    def is_configured(self) -> bool:
        return bool(self.settings.sftp_host and self.settings.sftp_password)

    def iter_raw(self) -> Iterator[tuple[dict, dict]]:
        from jobmarket.sftp_io import connect

        inbox = self.settings.sftp_inbox_dir
        with connect(self.settings) as sftp:
            try:
                files = sorted(name for name in sftp.listdir(inbox) if FILE_PATTERN.fullmatch(name))
            except FileNotFoundError:
                files = []
            log.info("partner_sftp inbox=%s files=%d", inbox, len(files))
            for filename in files:
                with sftp.open(posixpath.join(inbox, filename), "rb") as handle:
                    rows = list(csv.DictReader(io.StringIO(handle.read().decode("utf-8-sig"))))
                log.info("partner_sftp file=%s rows=%d", filename, len(rows))
                for row in rows:
                    yield row, {"filename": filename}
                self.consumed_files.append(filename)

    def commit(self) -> None:
        if not self.consumed_files:
            return
        from jobmarket.sftp_io import connect, makedirs

        with connect(self.settings) as sftp:
            makedirs(sftp, self.settings.sftp_archive_dir)
            for filename in self.consumed_files:
                sftp.posix_rename(
                    posixpath.join(self.settings.sftp_inbox_dir, filename),
                    posixpath.join(self.settings.sftp_archive_dir, filename),
                )
        log.info("partner_sftp archived %s", ", ".join(self.consumed_files))
        self.consumed_files.clear()

    def to_event(self, row: dict, filename: str | None = None) -> RawJobEvent:
        reference = (row.get("reference") or "").strip()
        if not reference:
            raise ValueError("reference must not be blank")
        years = _number(row.get("min_experience_years"))
        return RawJobEvent(
            job_id=self.job_id(reference),
            source=self.name,
            source_id=reference,
            job_title=row.get("title") or "",
            company=row.get("company") or None,
            location=", ".join(v for v in (row.get("city"), row.get("province")) if v) or None,
            country="ZA",
            is_remote=(row.get("work_mode") or "").strip().lower() == "remote",
            salary_min=_number(row.get("salary_min")),
            salary_max=_number(row.get("salary_max")),
            salary_currency=row.get("currency") or "ZAR",
            salary_period=row.get("salary_period") or None,
            employment_type_raw=row.get("contract_type") or None,
            experience_months=int(years * 12) if years is not None else None,
            description=row.get("description"),
            tags=(row.get("skills") or "").split("|"),
            date_posted=parse_datetime(row.get("posted_date")),
            url=row.get("apply_url") or None,
            raw={**row, "_file": filename},
        )
