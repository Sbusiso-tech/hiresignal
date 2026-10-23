<p align="center">
  <img src="docs/assets/banner.svg" alt="HireSignal: cut through the noise of the job market" width="100%">
</p>

<p align="center">
  <b>What technologies are employers actually asking for?</b><br>
  An end-to-end data engineering platform that turns thousands of noisy job postings into a clear signal.
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-1D9E75?style=for-the-badge&logo=python&logoColor=white">
  <img src="https://img.shields.io/badge/Kafka-1D9E75?style=for-the-badge&logo=apachekafka&logoColor=white">
  <img src="https://img.shields.io/badge/Spark-1D9E75?style=for-the-badge&logo=apachespark&logoColor=white">
  <img src="https://img.shields.io/badge/dbt-1D9E75?style=for-the-badge&logo=dbt&logoColor=white">
  <img src="https://img.shields.io/badge/PostgreSQL-1D9E75?style=for-the-badge&logo=postgresql&logoColor=white">
  <img src="https://img.shields.io/badge/Airflow-1D9E75?style=for-the-badge&logo=apacheairflow&logoColor=white">
  <img src="https://img.shields.io/badge/FastAPI-1D9E75?style=for-the-badge&logo=fastapi&logoColor=white">
  <img src="https://img.shields.io/badge/Redis-1D9E75?style=for-the-badge&logo=redis&logoColor=white">
</p>

---

## 📡 The signal

A real run on 6 Oct 2026 (~1,600 postings, keyless sources only):

| Rank | Skill            | Jobs | Demand                     |
| :--: | ---------------- | ---: | -------------------------- |
| 1    | Python           | 488  | `████████████████████`     |
| 2    | AWS              | 288  | `████████████`             |
| 3    | CI/CD            | 264  | `███████████`              |
| 4    | SQL              | 261  | `███████████`              |
| 5    | Azure            | 218  | `█████████`                |
| 6    | Machine Learning | 195  | `████████`                 |
| 7    | Kubernetes       | 193  | `████████`                 |
| 8    | Docker           | 151  | `██████`                   |

## 🛰️ How it works

HireSignal collects real job postings from public job APIs, a partner's daily SFTP file drop and an
operational recruitment CRM, streams them through Kafka into a data lake, transforms them with Spark,
models them with dbt in PostgreSQL, and serves the results to Power BI and a REST API, both as nightly
batch analytics and as real-time counts.

```mermaid
flowchart TD
    A[Job APIs<br/>Himalayas · Jobicy · Remotive<br/>Arbeitnow · Adzuna · JSearch]:::src
    B[Partner SFTP drop<br/>daily CSV]:::src
    C[Recruitment CRM<br/>PostgreSQL]:::src
    A & B & C --> D[Python ingestion<br/>pydantic contract]:::core
    D -. invalid .-> DLQ[jobs.dlq]:::warn
    D --> K[(Kafka · jobs.raw)]:::core
    K -->|batch| L[(Raw data lake<br/>RustFS / S3)]:::core
    K -->|stream| R[Real-time consumer<br/>skill extraction]:::core
    L --> S[Spark<br/>dedupe · clean · parse · quality gate]:::core
    S --> P[(PostgreSQL<br/>landing → core)]:::core
    P --> T[dbt<br/>staging → intermediate → marts]:::core
    R --> X[(Redis leaderboard)]:::core
    T --> BI[Power BI]:::out
    T --> API[FastAPI]:::out
    X --> API

    classDef src fill:#E1F5EE,stroke:#0F6E56,color:#085041
    classDef core fill:#085041,stroke:#04342C,color:#E1F5EE
    classDef out fill:#1D9E75,stroke:#085041,color:#ffffff
    classDef warn fill:#FAEEDA,stroke:#854F0B,color:#633806
```

<sub>Orchestrated by Apache Airflow, twice daily at 06:00 and 18:00.</sub>

---

## 🚀 Quick start

Requirements: Docker with Compose v2, `make`, about 6 GB of free RAM.

```bash
cp .env.example .env          # optional: add Adzuna / RapidAPI keys
make up                       # Kafka, RustFS, Postgres, Redis, SFTP, lake sink, realtime consumer, API
make partner-drop             # the partner uploads today's CSV to the SFTP server
make crm-activity             # recruiters change vacancies in the CRM database
make pipeline                 # ingest -> drain to lake -> Spark -> dbt build
make top-skills               # print the signal
```

| Service                            | URL                            | Login                     |
| ---------------------------------- | ------------------------------ | ------------------------- |
| API docs (Swagger)                 | http://localhost:8000/docs     |                           |
| Kafka UI                           | http://localhost:8081          |                           |
| RustFS console (data lake)         | http://localhost:9001          | rustfsadmin / rustfsadmin |
| Spark UI (while `make spark` runs) | http://localhost:4040          |                           |
| Airflow (`make airflow`)           | http://localhost:8080          | admin / admin             |
| PostgreSQL                         | localhost:5432                 | jobmarket / jobmarket     |
| Partner SFTP                       | sftp://localhost:2222          | partner / partner         |
| Recruitment CRM (PostgreSQL)       | localhost:5433, database `crm` | crm / crm                 |

```bash
make airflow   # scheduled DAG: partner drop + CRM activity + the whole pipeline
make down      # stop (keeps data);  make clean  # stop and wipe all volumes
```

### 🔌 API examples

```bash
curl "localhost:8000/skills/top?limit=10"
curl "localhost:8000/skills/top?category=Data%20Engineering"
curl "localhost:8000/roles/Data%20Engineering/skills"
curl "localhost:8000/skills/Apache%20Spark/related"      # what to learn next
curl "localhost:8000/salaries/by-skill"
curl "localhost:8000/skills/realtime"                     # streaming layer (Redis)
curl "localhost:8000/jobs?skill=dbt&remote=true"
curl "localhost:8000/pipeline/runs"                       # batch run audit log
```

---

## 📥 Data collected

Every posting is normalised to the same fields:

| Field                                                      | Notes                                                                                |
| ---------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| `job_title`, `company`, `location`, `country`, `is_remote` | straight from the source                                                             |
| `salary_min/max`, `salary_currency`, `salary_period`       | structured fields, or parsed from text such as `"$90k - $105k"` or `"$45-$120/Hour"` |
| `salary_annual_*`, `salary_usd_*`                          | annualised (hour×2080, month×12…) then converted to USD in dbt                       |
| `skills`                                                   | ~150 technologies extracted from title, description and tags                         |
| `experience_years_min/max`                                 | patterns such as "3-5 years", "5+ years", "Mindestens 3 Jahre"                       |
| `seniority`                                                | intern / junior / mid / senior / lead                                                |
| `employment_type`                                          | full_time / part_time / contract / internship / temporary / unspecified              |
| `date_posted`, `first_seen_at`, `last_seen_at`             | posting date plus pipeline observation history                                       |

### Sources

| Source                                                                       | Key needed | Coverage                                                       |
| ---------------------------------------------------------------------------- | ---------- | -------------------------------------------------------------- |
| [Himalayas](https://himalayas.app/api)                                       | no         | 100k+ remote jobs, structured salaries and seniority           |
| [Jobicy](https://jobi.cy/apidocs)                                            | no         | remote tech jobs, structured salaries                          |
| [Remotive](https://remotive.com/api-documentation)                           | no         | latest remote jobs (small, 24h-delayed feed)                   |
| [Arbeitnow](https://www.arbeitnow.com/blog/job-board-api)                    | no         | Europe / Germany                                               |
| [Adzuna](https://developer.adzuna.com/)                                      | free key   | **South Africa** (`za`), UK, US… with salaries                 |
| [JSearch (RapidAPI)](https://rapidapi.com/letscrape-6bRBa3QguO5/api/jsearch) | free key   | Google for Jobs aggregate (LinkedIn, Indeed, …)                |
| Partner SFTP drop                                                            | SFTP login | daily CSV from a fictional South African recruitment agency    |
| Recruitment desk CRM                                                         | DB login   | vacancies from an operational PostgreSQL database (`crm-db`)   |

> [!NOTE]
> The partner drop and the CRM are simulated (`make partner-drop`, `make crm-activity`) and their
> companies are fictional. Filter on `source not in ('partner_sftp', 'crm_postgres')` for
> market-only analysis. Sources without credentials are skipped automatically. Please respect each
> provider's terms: keep the `url` link back to the original posting and credit the source.

---

## 🧱 Data model (dbt, schema `marts`)

| Model                                          | Purpose                                                    |
| ---------------------------------------------- | ---------------------------------------------------------- |
| `fct_job_postings`                             | one row per posting (grain: `job_id`)                      |
| `dim_company`, `dim_skill`, `bridge_job_skill` | star schema for BI                                         |
| `mart_skill_demand`                            | **ranked skill demand**, share of tech jobs, median salary |
| `mart_skill_demand_monthly`                    | trend per skill per month                                  |
| `mart_skill_by_job_family`                     | top skills for Data Engineering, Data Analytics, DevOps, … |
| `mart_salary_by_skill`                         | p25 / median / p75 annual USD salary per skill             |
| `mart_skill_pairs`                             | skills requested together                                  |
| `mart_market_overview`                         | counts by family × seniority × employment type × remote    |
| `mart_top_companies`                           | top hiring companies and their top 5 skills                |

## ⚙️ Engineering decisions

- **Raw first, transform later.** Ingestion only maps fields onto a contract; the untouched payload is kept in the lake so any transformation can be replayed.
- **At-least-once plus idempotency.** The lake sink commits Kafka offsets only after a file is written, Spark de-duplicates on `job_id`, and the Postgres upsert only rewrites changed rows, so re-running any step is safe.
- **Files are acknowledged, not just read.** SFTP files move to the archive only after Kafka confirms every row, and uploads are written under a temporary name and renamed.
- **Operational and analytical stores stay apart.** The CRM is its own normalised OLTP database, read in a read-only session over an overlapping time window.
- **Lambda architecture.** Kafka feeds both the batch path (lake → Spark → dbt) and a speed path (Redis, live within seconds).
- **Data quality at every layer.** pydantic validation with a dead-letter topic, a Spark quality gate, dbt tests and source freshness checks.
- **Lineage and observability.** `core.pipeline_runs` records every Spark run: raw records, unique jobs, inserts, updates and status.
- **One image per runtime.** A single app image runs ingestion, consumers, Spark and the API, launched by Airflow's `DockerOperator`.

<details>
<summary><b>📁 Project layout</b></summary>

```text
src/jobmarket/
  sources/              one module per source: job APIs, partner SFTP drop, CRM
  partner_feed.py       simulated partner: daily CSV upload to the SFTP server
  crm_simulator.py      simulated recruiters: activity in the CRM database
  sftp_io.py            SFTP connection helpers (paramiko)
  models.py             RawJobEvent: the pydantic contract for the jobs.raw topic
  ingest.py             APIs + SFTP + CRM -> Kafka (with dead-letter topic)
  lake_sink.py          Kafka -> S3 raw zone (gzipped JSONL, Hive-style partitions)
  realtime.py           Kafka -> Redis skill leaderboard (speed layer)
  transform.py          cleaning rules (salary, experience, seniority, employment type)
  skills.py             skill extractor + skills_taxonomy.yml
  spark_jobs/           Spark batch job: lake -> curated Parquet -> Postgres upsert
  sql/                  warehouse DDL + idempotent upsert, CRM schema
  api/                  FastAPI service
dbt/                    staging -> intermediate -> marts, tests, FX seed
airflow/dags/           orchestration DAG (DockerOperator tasks)
tests/                  unit tests (pytest)
```
</details>

<details>
<summary><b>📊 Power BI setup</b></summary>

1. **Get Data → PostgreSQL database**, server `localhost:5432`, database `jobmarket`, credentials from `.env`.
2. Import from schema `marts`: `fct_job_postings`, `dim_skill`, `dim_company`, `bridge_job_skill`, `mart_skill_demand`, `mart_skill_demand_monthly`, `mart_salary_by_skill`, `mart_skill_by_job_family`.
3. Relationships: `fct_job_postings[job_id]` 1→* `bridge_job_skill[job_id]`; `dim_skill[skill_key]` 1→* `bridge_job_skill[skill_key]` (cross-filter: both); `dim_company[company_key]` 1→* `fct_job_postings[company_key]`.
4. Suggested measures:

```dax
Jobs = DISTINCTCOUNT ( fct_job_postings[job_id] )
Jobs Requiring Skill = CALCULATE ( [Jobs], bridge_job_skill )
% of Jobs = DIVIDE ( [Jobs Requiring Skill], CALCULATE ( [Jobs], ALL ( dim_skill ) ) )
Median Salary (USD) = MEDIAN ( fct_job_postings[salary_usd_mid] )
Remote Share = DIVIDE ( CALCULATE ( [Jobs], fct_job_postings[is_remote] = TRUE () ), [Jobs] )
```

Suggested pages: skill demand, trends, salaries, roles and companies.
</details>

<details>
<summary><b>🎓 Curriculum mapping (WeThinkCode_ Data Engineering elective)</b></summary>

| Week | Topic                                                    | Where in HireSignal                                                        |
| ---- | -------------------------------------------------------- | -------------------------------------------------------------------------- |
| 1    | ETL, Linux/Bash, Python + Pandas                         | `ingest.py` → `transform.py` → load; Makefile/Bash automation              |
| 2    | Storage, file formats, distributed FS, CAP, Docker       | gzipped JSONL raw zone + Parquet curated zone on RustFS; fully containerised |
| 3    | SQL, ERDs, normalisation, ACID, PostgreSQL, NoSQL        | OLTP CRM vs OLAP warehouse; star schema; Redis key-value store             |
| 4    | Lakes vs warehouses, ETL vs ELT, REST ingestion          | six REST APIs + SFTP; RustFS lake + Postgres warehouse; ELT with dbt       |
| 5    | Batch vs stream, Spark, Kafka, Airflow                   | Spark batch job, Kafka streaming consumers, Airflow DAG                    |
| 6    | Data governance and quality                              | contracts, DLQ, quality gate, dbt tests and freshness, run audit table     |
| 7–8  | Capstone                                                 | APIs + SFTP + CRM, Kafka, Spark, Airflow, dbt, FastAPI, Redis (CDC: roadmap) |
| 9    | Demo and defence                                         | architecture diagram, engineering decisions, live API + dashboard          |
</details>

<details>
<summary><b>🛠️ Running locally without Docker</b></summary>

```bash
python3 -m venv .venv && .venv/bin/pip install -e ".[dev,pipeline,api]"
.venv/bin/pytest
.venv/bin/python -m jobmarket.ingest --dry-run data/sample.jsonl   # hit the real APIs, no Kafka
```
</details>

---

## 🗺️ Roadmap

- [x] Apache Airflow DAG to schedule the whole pipeline
- [x] SFTP source: a partner's daily CSV export of job postings
- [x] Operational PostgreSQL source: the recruitment desk's CRM
- [ ] Debezium CDC on the CRM: stream vacancy changes into Kafka and Redis
- [ ] GitHub Actions CI: lint, unit tests and dbt parse
- [ ] Power BI dashboard screenshots and setup guides
- [ ] Hosted HireSignal web dashboard

## 💡 Ideas to extend

- Use an FX API instead of the static `currency_rates_to_usd` seed.
- Classify skills with an LLM or NER model and compare against the dictionary approach.
- Store the curated zone as Delta Lake or Iceberg tables and query it with Trino.
- Deploy to AWS (MSK, S3, EMR, RDS and MWAA) using Terraform.

<p align="center"><sub>Built by <a href="https://github.com/Sbusiso-tech">Sbusiso-tech</a> · HireSignal 📡</sub></p>

WTC-AJPLDQS3
