from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _env_list(name: str, default: str) -> list[str]:
    return [item.strip() for item in _env(name, default).split(",") if item.strip()]


def _env_int(name: str, default: int) -> int:
    return int(_env(name, str(default)))


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    topic_raw: str = field(default_factory=lambda: _env("KAFKA_TOPIC_RAW", "jobs.raw"))
    topic_dlq: str = field(default_factory=lambda: _env("KAFKA_TOPIC_DLQ", "jobs.dlq"))

    s3_endpoint: str = field(default_factory=lambda: _env("S3_ENDPOINT", "http://localhost:9000"))
    s3_access_key: str = field(default_factory=lambda: _env("S3_ACCESS_KEY", "rustfsadmin"))
    s3_secret_key: str = field(default_factory=lambda: _env("S3_SECRET_KEY", "rustfsadmin"))
    lake_bucket: str = field(default_factory=lambda: _env("LAKE_BUCKET", "jobs-lake"))

    pg_host: str = field(default_factory=lambda: _env("POSTGRES_HOST", "localhost"))
    pg_port: int = field(default_factory=lambda: _env_int("POSTGRES_PORT", 5432))
    pg_db: str = field(default_factory=lambda: _env("POSTGRES_DB", "jobmarket"))
    pg_user: str = field(default_factory=lambda: _env("POSTGRES_USER", "jobmarket"))
    pg_password: str = field(default_factory=lambda: _env("POSTGRES_PASSWORD", "jobmarket"))

    redis_url: str = field(default_factory=lambda: _env("REDIS_URL", "redis://localhost:6379/0"))

    sources: list[str] = field(
        default_factory=lambda: _env_list(
            "JOB_SOURCES", "himalayas,jobicy,remotive,arbeitnow,adzuna,jsearch,partner_sftp,crm_postgres"
        )
    )
    search_terms: list[str] = field(
        default_factory=lambda: _env_list(
            "SEARCH_TERMS", "data engineer,data analyst,software engineer,python developer,devops engineer"
        )
    )
    himalayas_max_pages: int = field(default_factory=lambda: _env_int("HIMALAYAS_MAX_PAGES", 5))
    arbeitnow_max_pages: int = field(default_factory=lambda: _env_int("ARBEITNOW_MAX_PAGES", 5))

    adzuna_app_id: str = field(default_factory=lambda: _env("ADZUNA_APP_ID"))
    adzuna_app_key: str = field(default_factory=lambda: _env("ADZUNA_APP_KEY"))
    adzuna_countries: list[str] = field(default_factory=lambda: _env_list("ADZUNA_COUNTRIES", "za,gb,us"))
    adzuna_max_pages: int = field(default_factory=lambda: _env_int("ADZUNA_MAX_PAGES", 3))

    rapidapi_key: str = field(default_factory=lambda: _env("RAPIDAPI_KEY"))
    jsearch_location: str = field(default_factory=lambda: _env("JSEARCH_LOCATION", "South Africa"))
    jsearch_country: str = field(default_factory=lambda: _env("JSEARCH_COUNTRY", "za"))
    jsearch_num_pages: int = field(default_factory=lambda: _env_int("JSEARCH_NUM_PAGES", 2))

    sftp_host: str = field(default_factory=lambda: _env("SFTP_HOST"))
    sftp_port: int = field(default_factory=lambda: _env_int("SFTP_PORT", 22))
    sftp_user: str = field(default_factory=lambda: _env("SFTP_USER", "partner"))
    sftp_password: str = field(default_factory=lambda: _env("SFTP_PASSWORD"))
    sftp_inbox_dir: str = field(default_factory=lambda: _env("SFTP_INBOX_DIR", "upload/exports"))
    sftp_archive_dir: str = field(default_factory=lambda: _env("SFTP_ARCHIVE_DIR", "upload/archive"))

    crm_host: str = field(default_factory=lambda: _env("CRM_HOST"))
    crm_port: int = field(default_factory=lambda: _env_int("CRM_PORT", 5432))
    crm_db: str = field(default_factory=lambda: _env("CRM_DB", "crm"))
    crm_user: str = field(default_factory=lambda: _env("CRM_USER", "crm"))
    crm_password: str = field(default_factory=lambda: _env("CRM_PASSWORD"))
    crm_lookback_hours: int = field(default_factory=lambda: _env_int("CRM_LOOKBACK_HOURS", 72))

    http_timeout: int = field(default_factory=lambda: _env_int("HTTP_TIMEOUT_SECONDS", 30))

    @property
    def pg_dsn(self) -> str:
        return (
            f"host={self.pg_host} port={self.pg_port} dbname={self.pg_db} "
            f"user={self.pg_user} password={self.pg_password}"
        )

    @property
    def crm_dsn(self) -> str:
        return (
            f"host={self.crm_host} port={self.crm_port} dbname={self.crm_db} "
            f"user={self.crm_user} password={self.crm_password}"
        )

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.pg_host}:{self.pg_port}/{self.pg_db}"


def get_settings() -> Settings:
    return Settings()
