# Job Market Intelligence Platform

**What technologies are employers asking for?**

A data engineering project that collects real job postings from public job APIs
(Himalayas, Jobicy, Remotive, Arbeitnow, Adzuna, JSearch), extracts the skills
employers ask for, and turns them into analytics.

Planned architecture:

```
Job APIs -> Python ingestion -> Kafka -> Raw data lake -> Spark -> PostgreSQL -> dbt -> Power BI
```

## Status

- [x] Job API sources mapped to a common event contract
- [ ] Skill extraction and cleaning
- [ ] Kafka ingestion and raw data lake
- [ ] Spark batch processing
- [ ] dbt models
- [ ] REST API
- [ ] Airflow orchestration
