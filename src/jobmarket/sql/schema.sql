-- landing: overwritten by every Spark run (staging area for the upsert)
-- core:    persistent, de-duplicated job history; the source for dbt
create schema if not exists landing;
create schema if not exists core;

create table if not exists core.jobs (
    job_id               text primary key,
    source               text        not null,
    source_id            text        not null,
    search_term          text,
    job_title            text        not null,
    company              text,
    location             text,
    country              text,
    is_remote            boolean,
    employment_type      text        not null,
    seniority            text        not null,
    experience_years_min integer,
    experience_years_max integer,
    salary_min           numeric(14, 2),
    salary_max           numeric(14, 2),
    salary_currency      text,
    salary_period        text,
    salary_annual_min    numeric(14, 2),
    salary_annual_max    numeric(14, 2),
    salary_is_predicted  boolean,
    date_posted          timestamptz,
    description          text,
    tags                 jsonb       not null default '[]'::jsonb,
    url                  text,
    first_seen_at        timestamptz not null,
    last_seen_at         timestamptz not null,
    updated_at           timestamptz not null default now()
);

create index if not exists jobs_date_posted_idx on core.jobs (date_posted);
create index if not exists jobs_source_idx on core.jobs (source);

create table if not exists core.job_skills (
    job_id         text not null references core.jobs (job_id) on delete cascade,
    skill          text not null,
    skill_category text not null,
    primary key (job_id, skill)
);

create index if not exists job_skills_skill_idx on core.job_skills (skill);

create table if not exists core.pipeline_runs (
    run_id          text primary key,
    started_at      timestamptz not null,
    finished_at     timestamptz,
    raw_records     bigint,
    unique_jobs     bigint,
    jobs_inserted   bigint,
    jobs_updated    bigint,
    skill_rows      bigint,
    status          text not null
);
