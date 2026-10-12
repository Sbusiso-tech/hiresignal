"""Spark batch job: raw lake (bronze) -> curated lake (silver) -> PostgreSQL core layer.

Steps
  1. Read every raw JSONL file from s3a://<bucket>/raw/jobs/ with an explicit schema.
  2. De-duplicate on job_id (keep the latest ingestion, remember first/last seen).
  3. Enrich with a Python UDF: HTML stripping, salary parsing + annualisation,
     employment type, experience, seniority and skill extraction.
  4. Data quality gate (fail the run instead of loading bad data).
  5. Write curated Parquet back to the lake, partitioned by source.
  6. Load into landing.* over JDBC and upsert into core.* in a single transaction.

    spark-submit -m jobmarket.spark_jobs.transform_jobs   (see Makefile / Airflow DAG)
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from jobmarket.config import Settings, get_settings
from jobmarket.db import connect, ensure_schema, merge_landing_into_core
from jobmarket.transform import EMPLOYMENT_TYPES, enrich

log = logging.getLogger("jobmarket.spark")

RAW_SCHEMA = T.StructType(
    [
        T.StructField("schema_version", T.IntegerType()),
        T.StructField("job_id", T.StringType()),
        T.StructField("source", T.StringType()),
        T.StructField("source_id", T.StringType()),
        T.StructField("ingested_at", T.StringType()),
        T.StructField("search_term", T.StringType()),
        T.StructField("job_title", T.StringType()),
        T.StructField("company", T.StringType()),
        T.StructField("location", T.StringType()),
        T.StructField("country", T.StringType()),
        T.StructField("is_remote", T.BooleanType()),
        T.StructField("salary_text", T.StringType()),
        T.StructField("salary_min", T.DoubleType()),
        T.StructField("salary_max", T.DoubleType()),
        T.StructField("salary_currency", T.StringType()),
        T.StructField("salary_period", T.StringType()),
        T.StructField("salary_is_predicted", T.BooleanType()),
        T.StructField("employment_type_raw", T.StringType()),
        T.StructField("experience_months", T.IntegerType()),
        T.StructField("description", T.StringType()),
        T.StructField("tags", T.ArrayType(T.StringType())),
        T.StructField("date_posted", T.StringType()),
        T.StructField("url", T.StringType()),
    ]
)

ENRICHED_SCHEMA = T.StructType(
    [
        T.StructField("job_title", T.StringType()),
        T.StructField("company", T.StringType()),
        T.StructField("description", T.StringType()),
        T.StructField("employment_type", T.StringType()),
        T.StructField("seniority", T.StringType()),
        T.StructField("experience_years_min", T.IntegerType()),
        T.StructField("experience_years_max", T.IntegerType()),
        T.StructField("salary_min", T.DoubleType()),
        T.StructField("salary_max", T.DoubleType()),
        T.StructField("salary_currency", T.StringType()),
        T.StructField("salary_period", T.StringType()),
        T.StructField("salary_annual_min", T.DoubleType()),
        T.StructField("salary_annual_max", T.DoubleType()),
        T.StructField(
            "skills",
            T.ArrayType(T.StructType([T.StructField("skill", T.StringType()), T.StructField("category", T.StringType())])),
        ),
    ]
)

ENRICH_INPUT_COLUMNS = [
    "job_title", "company", "description", "salary_text", "salary_min", "salary_max", "salary_currency",
    "salary_period", "employment_type_raw", "experience_months", "tags",
]


@F.udf(returnType=ENRICHED_SCHEMA)
def enrich_udf(row):
    return enrich(row.asDict())


def build_spark(settings: Settings) -> SparkSession:
    return (
        SparkSession.builder.appName("job-market-transform")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.hadoop.fs.s3a.endpoint", settings.s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", settings.s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", settings.s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(settings.s3_endpoint.startswith("https")).lower())
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )
        .getOrCreate()
    )


def read_raw(spark: SparkSession, settings: Settings) -> DataFrame:
    # recursiveFileLookup disables Hive partition discovery, otherwise the "source=" directory
    # would clash with the "source" field inside each record.
    path = f"s3a://{settings.lake_bucket}/raw/jobs/"
    return (
        spark.read.schema(RAW_SCHEMA)
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.jsonl.gz")
        .json(path)
        .withColumn("ingested_at", F.col("ingested_at").cast("timestamp"))
        .withColumn("date_posted", F.col("date_posted").cast("timestamp"))
    )


def deduplicate(raw: DataFrame) -> DataFrame:
    by_job = Window.partitionBy("job_id")
    latest = Window.partitionBy("job_id").orderBy(F.col("ingested_at").desc())
    return (
        raw.filter(F.col("job_id").isNotNull())
        .withColumn("first_seen_at", F.min("ingested_at").over(by_job))
        .withColumn("last_seen_at", F.max("ingested_at").over(by_job))
        .withColumn("_rn", F.row_number().over(latest))
        .filter("_rn = 1")
        .drop("_rn")
    )


def curate(jobs: DataFrame) -> DataFrame:
    enriched = jobs.withColumn("e", enrich_udf(F.struct(*ENRICH_INPUT_COLUMNS)))
    return enriched.select(
        "job_id", "source", "source_id", "search_term",
        F.col("e.job_title").alias("job_title"),
        F.col("e.company").alias("company"),
        F.trim("location").alias("location"),
        F.trim("country").alias("country"),
        "is_remote",
        F.col("e.employment_type").alias("employment_type"),
        F.col("e.seniority").alias("seniority"),
        F.col("e.experience_years_min").alias("experience_years_min"),
        F.col("e.experience_years_max").alias("experience_years_max"),
        F.col("e.salary_min").alias("salary_min"),
        F.col("e.salary_max").alias("salary_max"),
        F.upper("e.salary_currency").alias("salary_currency"),
        F.col("e.salary_period").alias("salary_period"),
        F.col("e.salary_annual_min").alias("salary_annual_min"),
        F.col("e.salary_annual_max").alias("salary_annual_max"),
        "salary_is_predicted",
        "date_posted",
        F.col("e.description").alias("description"),
        F.to_json(F.coalesce("tags", F.array().cast("array<string>"))).alias("tags"),
        "url", "first_seen_at", "last_seen_at",
        F.col("e.skills").alias("skills"),
    )


def quality_gate(curated: DataFrame) -> None:
    checks = curated.agg(
        F.count("*").alias("rows"),
        F.sum(F.when(F.col("job_title").isNull() | (F.col("job_title") == ""), 1).otherwise(0)).alias("blank_titles"),
        F.sum(F.when(~F.col("employment_type").isin(*EMPLOYMENT_TYPES), 1).otherwise(0)).alias("bad_employment"),
        F.sum(F.when(F.col("salary_annual_min") > F.col("salary_annual_max"), 1).otherwise(0)).alias("bad_salary"),
        F.sum(F.when(F.col("salary_annual_max") > 10_000_000, 1).otherwise(0)).alias("absurd_salary"),
    ).first().asDict()
    log.info("quality checks: %s", checks)
    failures = {k: v for k, v in checks.items() if k != "rows" and v}
    if checks["rows"] == 0:
        raise RuntimeError("quality gate failed: no rows to load")
    if failures:
        raise RuntimeError(f"quality gate failed: {failures}")


def write_jdbc(df: DataFrame, settings: Settings, table: str) -> None:
    (
        df.write.format("jdbc")
        .option("url", settings.jdbc_url)
        .option("dbtable", table)
        .option("user", settings.pg_user)
        .option("password", settings.pg_password)
        .option("driver", "org.postgresql.Driver")
        .option("truncate", "true")
        .option("batchsize", 2000)
        .mode("overwrite")
        .save()
    )


def record_run(settings: Settings, run: dict) -> None:
    with connect(settings) as conn:
        conn.execute(
            """
            insert into core.pipeline_runs (run_id, started_at, finished_at, raw_records, unique_jobs,
                                            jobs_inserted, jobs_updated, skill_rows, status)
            values (%(run_id)s, %(started_at)s, %(finished_at)s, %(raw_records)s, %(unique_jobs)s,
                    %(jobs_inserted)s, %(jobs_updated)s, %(skill_rows)s, %(status)s)
            on conflict (run_id) do update set finished_at = excluded.finished_at, status = excluded.status
            """,
            run,
        )


def run(settings: Settings) -> dict:
    run_info = {
        "run_id": uuid.uuid4().hex, "started_at": datetime.now(timezone.utc), "finished_at": None,
        "raw_records": None, "unique_jobs": None, "jobs_inserted": None, "jobs_updated": None,
        "skill_rows": None, "status": "running",
    }
    ensure_schema(settings)
    spark = build_spark(settings)
    try:
        raw = read_raw(spark, settings).cache()
        run_info["raw_records"] = raw.count()
        log.info("raw records in lake: %d", run_info["raw_records"])

        curated = curate(deduplicate(raw)).cache()
        run_info["unique_jobs"] = curated.count()
        quality_gate(curated)

        job_skills = curated.select("job_id", F.explode("skills").alias("s")).select(
            "job_id", F.col("s.skill").alias("skill"), F.col("s.category").alias("skill_category")
        )
        jobs = curated.drop("skills")

        lake = f"s3a://{settings.lake_bucket}/curated"
        jobs.write.mode("overwrite").partitionBy("source").parquet(f"{lake}/jobs")
        job_skills.write.mode("overwrite").parquet(f"{lake}/job_skills")

        write_jdbc(jobs, settings, "landing.jobs_batch")
        write_jdbc(job_skills, settings, "landing.job_skills_batch")

        with connect(settings) as conn:
            run_info.update(merge_landing_into_core(conn))
        run_info["status"] = "success"
        return run_info
    except Exception:
        run_info["status"] = "failed"
        raise
    finally:
        run_info["finished_at"] = datetime.now(timezone.utc)
        record_run(settings, run_info)
        log.info("run summary: %s", json.dumps(run_info, default=str))
        spark.stop()


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter).parse_args(argv)
    run(get_settings())
    return 0


if __name__ == "__main__":
    sys.exit(main())
