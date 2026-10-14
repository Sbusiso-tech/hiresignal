"""Simulate the recruitment partner's daily SFTP delivery.

The partner, a fictional South African recruitment agency, exports its open vacancies once a day
as ``jobs_YYYY-MM-DD.csv`` and uploads the file to our SFTP server. There is no public SFTP job
feed, so this generator stands in for the partner: companies are fictional, while titles, skills
and salary bands follow the local market. The output is deterministic per date.

    python -m jobmarket.partner_feed                          # upload today's file
    python -m jobmarket.partner_feed --date 2026-10-14 --rows 60
    python -m jobmarket.partner_feed --output data/jobs.csv   # write locally, no SFTP
"""
from __future__ import annotations

import argparse
import csv
import io
import logging
import posixpath
import random
import sys
from datetime import date

from jobmarket.config import Settings, get_settings
from jobmarket.sources.partner_sftp import CSV_COLUMNS

log = logging.getLogger("jobmarket.partner_feed")

COMPANIES = [
    "Karoo Data Labs", "Protea Financial Services", "Highveld Logistics", "Fynbos Health", "Baobab Retail Group",
    "Springbok Telecom", "Umoya Energy", "Kgotla Insurance", "Mzansi Fintech", "Table Bay Analytics",
    "Drakensberg Mining Tech", "Jacaranda Software",
]
CITIES = [
    ("Johannesburg", "Gauteng", 5), ("Sandton", "Gauteng", 4), ("Pretoria", "Gauteng", 3),
    ("Cape Town", "Western Cape", 5), ("Stellenbosch", "Western Cape", 1), ("Durban", "KwaZulu-Natal", 2),
    ("Gqeberha", "Eastern Cape", 1), ("Bloemfontein", "Free State", 1),
]
# title, team, core skills, optional skills, monthly ZAR band for a mid-level hire
ROLES = [
    ("Data Engineer", "data platform", ["Python", "SQL"],
     ["Apache Spark", "Apache Airflow", "dbt", "AWS", "Azure", "Apache Kafka", "Docker", "Databricks", "Snowflake",
      "Azure Data Factory"], (45_000, 75_000)),
    ("Data Analyst", "insights", ["SQL", "Excel"], ["Power BI", "Python", "Tableau", "Statistics"], (28_000, 45_000)),
    ("Data Scientist", "advanced analytics", ["Python", "Machine Learning"],
     ["SQL", "scikit-learn", "Pandas", "PyTorch", "TensorFlow", "Statistics", "AWS"], (50_000, 80_000)),
    ("Software Engineer", "product engineering", ["Git"],
     ["Java", "C#", ".NET", "React", "TypeScript", "Angular", "Node.js", "SQL Server", "Docker"], (40_000, 70_000)),
    ("DevOps Engineer", "cloud infrastructure", ["Linux", "CI/CD"],
     ["Kubernetes", "Docker", "Terraform", "AWS", "Azure", "Ansible", "GCP"], (45_000, 78_000)),
    ("Python Developer", "backend", ["Python"], ["Django", "FastAPI", "PostgreSQL", "Docker", "REST API", "Git"],
     (38_000, 65_000)),
    ("Machine Learning Engineer", "AI", ["Python", "Machine Learning"],
     ["PyTorch", "TensorFlow", "Docker", "Kubernetes", "AWS", "Databricks"], (55_000, 90_000)),
    ("BI Developer", "business intelligence", ["SQL", "Power BI"], ["SQL Server", "Azure", "Excel", "Snowflake"],
     (35_000, 60_000)),
]
LEVELS = [("Junior ", (0, 2), 0.6, 4), ("", (3, 5), 1.0, 5), ("Senior ", (5, 8), 1.45, 4), ("Lead ", (8, 12), 1.8, 1)]
WORK_MODES = [("On-site", 3), ("Hybrid", 5), ("Remote", 2)]
CONTRACTS = [("Permanent", 14), ("Contract", 4), ("Temporary", 1)]


def _weighted(rng: random.Random, options: list[tuple]) -> tuple:
    return rng.choices(options, weights=[option[-1] for option in options])[0]


def generate_rows(day: date, rows: int = 40) -> list[dict]:
    rng = random.Random(f"partner-{day.isoformat()}")
    records = []
    for i in range(1, rows + 1):
        title, team, core, optional, (low, high) = rng.choice(ROLES)
        prefix, (exp_low, exp_high), factor, _ = _weighted(rng, LEVELS)
        city, province, _ = _weighted(rng, CITIES)
        company = rng.choice(COMPANIES)
        work_mode = _weighted(rng, WORK_MODES)[0]
        contract = _weighted(rng, CONTRACTS)[0]
        skills = core + rng.sample(optional, k=min(len(optional), rng.randint(2, 4)))
        years = rng.randint(exp_low, exp_high)
        salary_min = round(low * factor * rng.uniform(0.9, 1.1), -3)
        salary_max = round(max(high * factor * rng.uniform(0.9, 1.1), salary_min + 5_000), -3)
        has_salary = rng.random() < 0.8

        description = (
            f"{company} is looking for a {prefix}{title} to join the {team} team in {city} ({work_mode.lower()}). "
            f"You will build and support solutions using {', '.join(skills[:-1])} and {skills[-1]}. "
            f"Requirements: minimum {years} years of experience in a similar role"
            f"{' and a relevant degree' if rng.random() < 0.5 else ''}. Contract: {contract}."
        )
        records.append({
            "reference": f"TP{day:%y%m%d}-{i:03d}",
            # the partner's exports occasionally contain incomplete rows; ingestion dead-letters them
            "title": "" if rng.random() < 0.03 else f"{prefix}{title}",
            "company": company,
            "city": city,
            "province": province,
            "work_mode": work_mode,
            "contract_type": contract,
            "salary_min": f"{salary_min:.0f}" if has_salary else "",
            "salary_max": f"{salary_max:.0f}" if has_salary else "",
            "currency": "ZAR",
            "salary_period": "month",
            "min_experience_years": str(years),
            "skills": "|".join(skills),
            "description": description,
            "posted_date": day.isoformat(),
            "apply_url": f"https://careers.partner.example.co.za/jobs/TP{day:%y%m%d}-{i:03d}",
        })
    return records


def to_csv(records: list[dict]) -> str:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=CSV_COLUMNS)
    writer.writeheader()
    writer.writerows(records)
    return buffer.getvalue()


def upload(settings: Settings, day: date, content: str) -> str:
    from jobmarket.sftp_io import connect, makedirs

    filename = f"jobs_{day.isoformat()}.csv"
    final_path = posixpath.join(settings.sftp_inbox_dir, filename)
    # Write under a temporary name and rename, so the reader never picks up a half-written file.
    temp_path = posixpath.join(settings.sftp_inbox_dir, f".{filename}.part")
    with connect(settings) as sftp:
        makedirs(sftp, settings.sftp_inbox_dir)
        with sftp.open(temp_path, "wb") as handle:
            handle.write(content.encode("utf-8"))
        sftp.posix_rename(temp_path, final_path)
    return final_path


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--date", type=date.fromisoformat, default=date.today(), help="delivery date (YYYY-MM-DD)")
    parser.add_argument("--rows", type=int, default=40, help="number of vacancies in the file")
    parser.add_argument("--output", metavar="PATH", help="write the CSV locally instead of uploading it")
    args = parser.parse_args(argv)

    content = to_csv(generate_rows(args.date, args.rows))
    if args.output:
        with open(args.output, "w", encoding="utf-8", newline="") as handle:
            handle.write(content)
        log.info("wrote %d rows to %s", args.rows, args.output)
        return 0

    settings = get_settings()
    if not settings.sftp_host:
        log.error("SFTP_HOST is not set")
        return 1
    path = upload(settings, args.date, content)
    log.info("uploaded %d rows to sftp://%s/%s", args.rows, settings.sftp_host, path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
