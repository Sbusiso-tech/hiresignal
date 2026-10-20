# Job Market Intelligence Platform

**What technologies are employers asking for?**

An end-to-end data engineering platform that collects real job postings from public job APIs, a
partner's daily SFTP file drop and an operational recruitment CRM (PostgreSQL), streams them through
Kafka into a data lake, transforms them with Spark, models them with dbt in PostgreSQL and serves the
results to Power BI and a REST API, both as nightly batch analytics and as real-time counts.

```
 Himalayas · Jobicy · Remotive    Partner SFTP drop    Recruitment CRM
 Arbeitnow · Adzuna · JSearch     (daily CSV)          (PostgreSQL)
            │                             │                  │
            └─────────────────┬───────────┴──────────────────┘
                              ▼
                  Python ingestion (requests, paramiko, psycopg, pydantic) ──► jobs.dlq (dead letters)
                              │
                              ▼
                     Kafka  topic: jobs.raw
                   ┌──────────┴───────────┐
          (batch)  ▼                      ▼  (stream)
   Raw data lake: RustFS / S3       Real-time consumer
   raw/jobs/source=…/ingest_date=…   skill extraction ──► Redis leaderboard
                   │                                          │
                   ▼                                          │
     Spark: dedupe · clean · salary/experience parsing        │
            · skill extraction · quality gate                 │
          ├──► curated Parquet in the lake (silver)           │
                   ▼                                          │
     PostgreSQL  landing ──upsert──► core                     │
                   │                                          │
                   ▼                                          │
     dbt: staging ──► intermediate ──► marts (+ tests)        │
                   │                                          │
          ┌────────┴─────────┐                                │
          ▼                  ▼                                ▼
       Power BI          FastAPI  ◄──────────────────────────┘
                                                Orchestrated by Apache Airflow
```

Example output (a real run on 6 Oct 2026, ~1,600 postings, keyless sources only):

| Skill | Jobs |
|---|---|
| Python | 488 |
| AWS | 288 |
| CI/CD | 264 |
| SQL | 261 |
| Azure | 218 |
| Machine Learning | 195 |
| Kubernetes | 193 |
| Docker | 151 |

---

## Data collected

Every posting is normalised to the same fields:

| Field | Notes |
|---|---|
| `job_title`, `company`, `location`, `country`, `is_remote` | straight from the source |
| `salary_min/max`, `salary_currency`, `salary_period` | structured fields, or parsed from text such as `"$90k - $105k"` or `"$45-$120/Hour"` |
| `salary_annual_*`, `salary_usd_*` | annualised (hour×2080, month×12…) then converted to USD in dbt |
| `skills` | ~150 technologies extracted from title + description + tags (`src/jobmarket/skills_taxonomy.yml`) |
| `experience_years_min/max` | extracted with patterns such as "3-5 years", "5+ years", "Mindestens 3 Jahre" |
| `seniority` | intern / junior / mid / senior / lead, inferred from the title or experience |
| `employment_type` | full_time / part_time / contract / internship / temporary / unspecified |
| `date_posted`, `first_seen_at`, `last_seen_at` | posting date plus pipeline observation history |

### Sources

| Source | Key needed | Coverage |
|---|---|---|
| [Himalayas](https://himalayas.app/api) | no | 100k+ remote jobs, structured salaries and seniority |
| [Jobicy](https://jobi.cy/apidocs) | no | remote tech jobs, structured salaries |
| [Remotive](https://remotive.com/api-documentation) | no | latest remote jobs (small, 24h-delayed feed) |
| [Arbeitnow](https://www.arbeitnow.com/blog/job-board-api) | no | Europe / Germany |
| [Adzuna](https://developer.adzuna.com/) | free key | **South Africa** (`za`), UK, US… with salaries |
| [JSearch (RapidAPI)](https://rapidapi.com/letscrape-6bRBa3QguO5/api/jsearch) | free key | Google for Jobs aggregate (LinkedIn, Indeed, …) |
| Partner SFTP drop | SFTP login | daily `jobs_YYYY-MM-DD.csv` from a recruitment partner (South Africa, ZAR) |
| Recruitment desk CRM | DB login | vacancies from an operational PostgreSQL database (`crm-db`) |

The partner drop is simulated: there is no public SFTP job feed, so `make partner-drop` plays the
partner (a fictional South African recruitment agency) and uploads a deterministic daily CSV to the
`sftp` container. Ingestion reads every file in `upload/exports` and moves it to `upload/archive`
only after Kafka has acknowledged all of its rows.

The recruitment CRM is simulated the same way: `make crm-activity` plays the recruiters, opening,
re-pricing, filling and closing vacancies in a separate PostgreSQL database (`crm-db`, schema `crm`)
that stands in for an operational application. Ingestion reads every vacancy changed in the last
`CRM_LOOKBACK_HOURS` through a read-only session. `make cdc` adds the live path: Debezium tails the
CRM write-ahead log and a consumer keeps the newest vacancy changes in Redis (`/jobs/latest`).

Companies in both simulated feeds are fictional, so filter on
`source not in ('partner_sftp', 'crm_postgres')` for market-only analysis.

Sources without credentials are skipped automatically. Please respect each provider's terms:
keep the `url` link back to the original posting and credit the source.

---

## Quick start

Requirements: Docker with Compose v2, `make`, about 6 GB of free RAM.

```bash
cp .env.example .env          # optional: add Adzuna / RapidAPI keys
make up                       # Kafka, RustFS, Postgres, Redis, SFTP, lake sink, realtime consumer, API
make partner-drop             # the partner uploads today's CSV to the SFTP server
make crm-activity             # recruiters change vacancies in the CRM database
make pipeline                 # ingest -> drain to lake -> Spark -> dbt build
make top-skills               # print the answer
```

| UI | URL | Login |
|---|---|---|
| API docs (Swagger) | http://localhost:8000/docs | |
| Kafka UI | http://localhost:8081 | |
| RustFS console (data lake) | http://localhost:9001 | rustfsadmin / rustfsadmin |
| Spark UI (while `make spark` runs) | http://localhost:4040 | |
| Airflow (`make airflow`) | http://localhost:8080 | admin / admin |
| PostgreSQL | localhost:5432 | jobmarket / jobmarket |
| Partner SFTP | sftp://localhost:2222 | partner / partner |
| Recruitment CRM (PostgreSQL) | localhost:5433, database `crm` | crm / crm |

Optional extras:

```bash
make airflow   # scheduled DAG: partner drop + CRM activity + the whole pipeline at 06:00 and 18:00
make cdc       # Debezium on crm.vacancies -> Kafka -> Redis, served by /jobs/latest
make down      # stop (keeps data);  make clean  # stop and wipe all volumes
```

### API examples

```bash
curl "localhost:8000/skills/top?limit=10"
curl "localhost:8000/skills/top?category=Data%20Engineering"
curl "localhost:8000/roles/Data%20Engineering/skills"
curl "localhost:8000/skills/Apache%20Spark/related"      # what to learn next
curl "localhost:8000/salaries/by-skill"
curl "localhost:8000/skills/realtime"                     # streaming layer (Redis)
curl "localhost:8000/jobs?skill=dbt&remote=true"
curl "localhost:8000/jobs/latest"                         # CRM vacancy changes via Debezium CDC
curl "localhost:8000/pipeline/runs"                       # batch run audit log
```

---

## Project layout

```
src/jobmarket/
  sources/              one module per source: job APIs, partner SFTP drop, CRM (fetch + map to the event contract)
  partner_feed.py       simulated partner: daily CSV upload to the SFTP server
  crm_simulator.py      simulated recruiters: activity in the CRM database
  sftp_io.py            SFTP connection helpers (paramiko)
  models.py             RawJobEvent: the pydantic contract for the jobs.raw topic
  ingest.py             APIs + SFTP + CRM -> Kafka (with dead-letter topic)
  lake_sink.py          Kafka -> S3 raw zone (gzipped JSONL, Hive-style partitions)
  realtime.py           Kafka -> Redis skill leaderboard (speed layer)
  cdc_consumer.py       Debezium CRM change events -> Redis vacancy feed
  transform.py          cleaning rules (salary, experience, seniority, employment type)
  skills.py             skill extractor + skills_taxonomy.yml
  spark_jobs/           Spark batch job: lake -> curated Parquet -> Postgres upsert
  sql/                  warehouse DDL + idempotent upsert, CRM schema
  api/                  FastAPI service
dbt/                    staging -> intermediate -> marts, tests, FX seed
airflow/dags/           orchestration DAG (DockerOperator tasks)
cdc/                    Debezium connector config
tests/                  unit tests (pytest)
```

### Data model (dbt, schema `marts`)

| Model | Purpose |
|---|---|
| `fct_job_postings` | one row per posting (grain: `job_id`) |
| `dim_company`, `dim_skill`, `bridge_job_skill` | star schema for BI |
| `mart_skill_demand` | **ranked skill demand**, share of tech jobs, median salary |
| `mart_skill_demand_monthly` | trend per skill per month |
| `mart_skill_by_job_family` | top skills for Data Engineering, Data Analytics, DevOps, … |
| `mart_salary_by_skill` | p25 / median / p75 annual USD salary per skill |
| `mart_skill_pairs` | skills requested together |
| `mart_market_overview` | counts by family × seniority × employment type × remote |
| `mart_top_companies` | top hiring companies and their top 5 skills |

---

## Engineering decisions

- **Raw first, transform later.** Ingestion only maps fields onto a contract; the untouched API
  payload (`raw`) is kept in the lake so any transformation can be replayed by re-running Spark.
- **At-least-once + idempotency.** The lake sink commits Kafka offsets only after a file is written.
  Spark de-duplicates on `job_id`, and the Postgres upsert only rewrites rows that changed, so
  re-running any step is safe and rows that did not change are not rewritten.
- **Files are acknowledged, not just read.** SFTP files move to the archive only after Kafka confirms
  every row, and uploads are written under a temporary name and renamed, so a half-written or failed
  delivery is never lost or half-loaded.
- **Operational and analytical stores stay apart.** The CRM is its own normalised OLTP database with
  constraints and an `updated_at` trigger; the platform only reads it, in a read-only session, over an
  overlapping time window, so a missed run never loses changes and re-reads are free thanks to the upsert.
- **Change data capture on the operational database.** Debezium reads the CRM write-ahead log
  (`pgoutput`) and publishes every insert, update and delete on `crm.vacancies` to Kafka. The batch
  extract stays the complete, replayable copy; the CDC feed is the live view of what recruiters just did.
- **Lambda architecture.** Kafka feeds both the batch path (lake → Spark → dbt, accurate and
  historical) and a speed path (Redis, live within seconds).
- **Data quality at every layer.** pydantic validation with a dead-letter topic at ingestion, a
  Spark quality gate that fails the run before loading bad data, dbt tests (unique, not null,
  accepted values, relationships, salary range checks) and source freshness checks.
- **Lineage and observability.** `core.pipeline_runs` records every Spark run: raw records,
  unique jobs, inserts, updates and status.
- **One image per runtime.** A single app image runs ingestion, consumers, Spark and the API, and
  Airflow launches it with `DockerOperator`, so the scheduler needs no heavy dependencies.

---

## Curriculum mapping (WeThinkCode_ Data Engineering elective)

| Week | Topic | Where in this project |
|---|---|---|
| 1 | ETL, Linux/Bash, Python + Pandas | `ingest.py` → `transform.py` → load; Makefile/Bash automation |
| 2 | Storage, file formats (JSON, Parquet), distributed FS, CAP, Docker | gzipped JSONL raw zone + Parquet curated zone on S3-compatible storage (RustFS); fully containerised |
| 3 | SQL, ERDs, normalisation, ACID, PostgreSQL, NoSQL | operational CRM (OLTP, constraints, triggers) vs warehouse (OLAP); normalised `core.jobs` / `core.job_skills`, transactional upsert, star schema; Redis key-value store |
| 4 | Data lakes vs warehouses, ETL vs ELT, ingestion from REST APIs with `requests` | six REST APIs + SFTP file drop; lake (RustFS) + warehouse (Postgres); ELT with dbt |
| 5 | Batch vs stream, Spark, Kafka, Airflow DAGs | Spark batch job, Kafka streaming consumers, Debezium CDC, Airflow DAG |
| 6 | Data governance and quality dimensions | contracts, DLQ, quality gate, dbt tests and freshness, run audit table |
| 7–8 | Capstone: multi-source ingestion, batch + stream, REST API serving, Spark, Airflow, Debezium, dbt, CDC, high-velocity store | APIs + SFTP + PostgreSQL CRM sources, Kafka, Spark, Airflow, dbt, FastAPI, Debezium CDC, Redis |
| 9 | Demo and defence | architecture diagram, engineering decisions above, live API + dashboard |

---

## Power BI

Power BI Desktop runs on Windows. Connect it to the warehouse:

1. **Get Data → PostgreSQL database**, server `localhost:5432` (or your Docker host's IP), database `jobmarket`.
   Use the username and password from `.env`. If prompted, install the Npgsql driver; recent Power BI versions include it.
2. Import from schema `marts`: `fct_job_postings`, `dim_skill`, `dim_company`, `bridge_job_skill`,
   `mart_skill_demand`, `mart_skill_demand_monthly`, `mart_salary_by_skill`, `mart_skill_by_job_family`.
3. Relationships: `fct_job_postings[job_id]` 1→* `bridge_job_skill[job_id]`;
   `dim_skill[skill_key]` 1→* `bridge_job_skill[skill_key]` (cross-filter: both);
   `dim_company[company_key]` 1→* `fct_job_postings[company_key]`.
4. Suggested measures:

```DAX
Jobs = DISTINCTCOUNT ( fct_job_postings[job_id] )
Jobs Requiring Skill = CALCULATE ( [Jobs], bridge_job_skill )
% of Jobs = DIVIDE ( [Jobs Requiring Skill], CALCULATE ( [Jobs], ALL ( dim_skill ) ) )
Median Salary (USD) = MEDIAN ( fct_job_postings[salary_usd_mid] )
Remote Share = DIVIDE ( CALCULATE ( [Jobs], fct_job_postings[is_remote] = TRUE () ), [Jobs] )
```

Suggested report pages:
- **Skill demand:** a bar chart of the top 20 skills plus a skill-category slicer.
- **Trends:** a line chart from `mart_skill_demand_monthly`.
- **Salaries:** a box-style chart of p25, median and p75 from `mart_salary_by_skill`.
- **Roles:** a matrix of job family by skill.
- **Companies:** a table from `mart_top_companies`.


---

## Running locally without Docker (development)

```bash
python3 -m venv .venv && .venv/bin/pip install -e ".[dev,pipeline,api]"
.venv/bin/pytest
.venv/bin/python -m jobmarket.ingest --dry-run data/sample.jsonl   # hit the real APIs, no Kafka
```

## Continuous integration

GitHub Actions (`.github/workflows/ci.yml`) runs on every push to `main` and on pull requests:

- **lint and unit tests:** `ruff check` and `pytest`. Spark and Docker are not required.
- **dbt parse:** checks that the dbt project compiles, using dbt 1.8, the same version as the image.

## Roadmap

- [x] Apache Airflow DAG to schedule the whole pipeline
- [x] SFTP source: a partner's daily CSV export of job postings
- [x] Operational PostgreSQL source: the recruitment desk's CRM
- [x] Debezium CDC on the CRM: stream vacancy changes into Kafka and Redis
- [x] GitHub Actions CI: lint, unit tests and dbt parse
- [ ] Power BI dashboard screenshots and setup guides

## Ideas to extend

- Use an FX API instead of the static `currency_rates_to_usd` seed.
- Classify skills with an LLM or NER model and compare against the dictionary approach.
- Store the curated zone as Delta Lake or Iceberg tables and query it with Trino.
- Deploy to AWS (MSK, S3, EMR, RDS and MWAA) using Terraform.
