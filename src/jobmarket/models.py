"""Contract for events on the ``jobs.raw`` Kafka topic.

Sources only *map* their payload onto this shape; all cleaning and enrichment
(HTML stripping, salary parsing, skill extraction, ...) happens in Spark so the
raw lake stays a faithful, replayable copy of what the APIs returned.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field, field_validator

SCHEMA_VERSION = 1


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class RawJobEvent(BaseModel):
    schema_version: int = SCHEMA_VERSION
    job_id: str
    source: str
    source_id: str
    ingested_at: datetime = Field(default_factory=utc_now)
    search_term: str | None = None

    job_title: str
    company: str | None = None
    location: str | None = None
    country: str | None = None
    is_remote: bool | None = None

    salary_text: str | None = None
    salary_min: float | None = None
    salary_max: float | None = None
    salary_currency: str | None = None
    salary_period: str | None = None
    salary_is_predicted: bool | None = None

    employment_type_raw: str | None = None
    experience_months: int | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    date_posted: datetime | None = None
    url: str | None = None

    raw: dict[str, Any] = Field(default_factory=dict)

    @field_validator("job_title")
    @classmethod
    def title_not_blank(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("job_title must not be blank")
        return value.strip()

    @field_validator("tags", mode="before")
    @classmethod
    def clean_tags(cls, value: Any) -> list[str]:
        if not value:
            return []
        return [str(tag).strip() for tag in value if str(tag).strip()]

    def kafka_key(self) -> bytes:
        return self.job_id.encode()

    def kafka_value(self) -> bytes:
        return self.model_dump_json().encode()
