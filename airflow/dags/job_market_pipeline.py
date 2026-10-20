"""Job Market Intelligence pipeline.

partner SFTP drop + CRM activity (simulated) -> ingest (APIs + SFTP + CRM -> Kafka)
-> drain to lake (Kafka -> S3 / RustFS) -> Spark (lake -> Postgres) -> dbt build -> freshness

Every task runs in the project's own Docker images, so Airflow only orchestrates
and never needs Spark, dbt or Kafka libraries installed itself.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.providers.docker.operators.docker import DockerOperator

from airflow import DAG

APP_IMAGE = os.getenv("JOBMARKET_APP_IMAGE", "jobmarket-app:latest")
DBT_IMAGE = os.getenv("JOBMARKET_DBT_IMAGE", "jobmarket-dbt:latest")
NETWORK = os.getenv("JOBMARKET_NETWORK", "jobmarket_net")

PASSTHROUGH_ENV = [
    "KAFKA_BOOTSTRAP_SERVERS", "KAFKA_TOPIC_RAW", "KAFKA_TOPIC_DLQ",
    "S3_ENDPOINT", "S3_ACCESS_KEY", "S3_SECRET_KEY", "LAKE_BUCKET",
    "POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD",
    "REDIS_URL", "JOB_SOURCES", "SEARCH_TERMS", "HIMALAYAS_MAX_PAGES", "ARBEITNOW_MAX_PAGES",
    "ADZUNA_APP_ID", "ADZUNA_APP_KEY", "ADZUNA_COUNTRIES", "ADZUNA_MAX_PAGES",
    "RAPIDAPI_KEY", "JSEARCH_LOCATION", "JSEARCH_COUNTRY", "JSEARCH_NUM_PAGES",
    "SFTP_HOST", "SFTP_PORT", "SFTP_USER", "SFTP_PASSWORD", "SFTP_INBOX_DIR", "SFTP_ARCHIVE_DIR",
    "CRM_HOST", "CRM_PORT", "CRM_DB", "CRM_USER", "CRM_PASSWORD", "CRM_LOOKBACK_HOURS",
]
ENV = {key: os.environ[key] for key in PASSTHROUGH_ENV if os.environ.get(key)}


def task(task_id: str, command: str, image: str = APP_IMAGE, **kwargs) -> DockerOperator:
    return DockerOperator(
        task_id=task_id,
        image=image,
        command=command,
        environment=ENV,
        network_mode=NETWORK,
        docker_url="unix://var/run/docker.sock",
        auto_remove="success",
        mount_tmp_dir=False,
        **kwargs,
    )


with DAG(
    dag_id="job_market_pipeline",
    description="Job APIs -> Kafka -> data lake -> Spark -> PostgreSQL -> dbt",
    start_date=datetime(2026, 1, 1),
    schedule="0 6,18 * * *",  # twice a day; Remotive asks for at most 4 calls/day
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-engineering", "retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["job-market", "portfolio"],
    doc_md=__doc__,
) as dag:
    partner_drop = task(
        "simulate_partner_sftp_drop",
        "python -m jobmarket.partner_feed --date {{ data_interval_end.strftime('%Y-%m-%d') }}",
    )
    crm_activity = task("simulate_crm_activity", "python -m jobmarket.crm_simulator --actions 25")
    # The APIs are still worth ingesting when the partner's SFTP server or the CRM is unavailable.
    ingest = task(
        "ingest_job_apis",
        "python -m jobmarket.ingest",
        execution_timeout=timedelta(minutes=30),
        trigger_rule="all_done",
    )
    drain = task("drain_kafka_to_lake", "python -m jobmarket.lake_sink --until-idle 30")
    spark = task(
        "spark_transform", "python -m jobmarket.spark_jobs.transform_jobs", execution_timeout=timedelta(minutes=30)
    )
    dbt_build = task("dbt_build", "build", image=DBT_IMAGE)
    freshness = task("dbt_source_freshness", "source freshness", image=DBT_IMAGE)

    [partner_drop, crm_activity] >> ingest >> drain >> spark >> dbt_build >> freshness
