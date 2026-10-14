from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any

import requests
from pydantic import ValidationError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from jobmarket.config import Settings
from jobmarket.models import RawJobEvent

log = logging.getLogger(__name__)

USER_AGENT = "job-market-intelligence-platform/1.0 (portfolio data engineering project)"


def build_session() -> requests.Session:
    retry = Retry(
        total=4,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def parse_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


class JobSource(ABC):
    name: str

    def __init__(self, settings: Settings, session: requests.Session | None = None):
        self.settings = settings
        self.session = session or build_session()
        self.rejected: list[dict[str, Any]] = []

    def is_configured(self) -> bool:
        return True

    def fetch(self) -> Iterator[RawJobEvent]:
        """Yield every valid job; records that fail validation are collected in ``self.rejected``."""
        for job, kwargs in self.iter_raw():
            try:
                yield self.to_event(job, **kwargs)
            except (ValidationError, KeyError, TypeError, ValueError) as exc:
                self.rejected.append({"source": self.name, "error": str(exc)[:2000], "payload": job})

    @abstractmethod
    def iter_raw(self) -> Iterator[tuple[dict, dict[str, Any]]]:
        """Yield ``(raw_job, extra_kwargs_for_to_event)`` pairs straight from the API."""

    @abstractmethod
    def to_event(self, job: dict, **kwargs: Any) -> RawJobEvent:
        """Map one raw API record onto the common event contract."""

    def commit(self) -> None:
        """Called once every event from ``fetch()`` has been acknowledged by Kafka."""

    def _get(self, url: str, **kwargs: Any) -> Any:
        kwargs.setdefault("timeout", self.settings.http_timeout)
        response = self.session.get(url, **kwargs)
        response.raise_for_status()
        return response.json()

    def job_id(self, source_id: Any) -> str:
        return f"{self.name}:{source_id}"
