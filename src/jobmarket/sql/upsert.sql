-- Idempotent upsert of the latest Spark batch into the core layer.
-- Rows are only rewritten when something actually changed, which keeps the
-- Debezium CDC stream free of no-op updates.
with upserted as (
    insert into core.jobs as t (
        job_id, source, source_id, search_term, job_title, company, location, country, is_remote,
        employment_type, seniority, experience_years_min, experience_years_max,
        salary_min, salary_max, salary_currency, salary_period, salary_annual_min, salary_annual_max,
        salary_is_predicted, date_posted, description, tags, url, first_seen_at, last_seen_at
    )
    select
        job_id, source, source_id, search_term, job_title, company, location, country, is_remote,
        employment_type, seniority, experience_years_min, experience_years_max,
        salary_min, salary_max, salary_currency, salary_period, salary_annual_min, salary_annual_max,
        salary_is_predicted, date_posted, description, coalesce(tags, '[]')::jsonb, url, first_seen_at, last_seen_at
    from landing.jobs_batch
    on conflict (job_id) do update set
        search_term          = excluded.search_term,
        job_title            = excluded.job_title,
        company              = excluded.company,
        location             = excluded.location,
        country              = excluded.country,
        is_remote            = excluded.is_remote,
        employment_type      = excluded.employment_type,
        seniority            = excluded.seniority,
        experience_years_min = excluded.experience_years_min,
        experience_years_max = excluded.experience_years_max,
        salary_min           = excluded.salary_min,
        salary_max           = excluded.salary_max,
        salary_currency      = excluded.salary_currency,
        salary_period        = excluded.salary_period,
        salary_annual_min    = excluded.salary_annual_min,
        salary_annual_max    = excluded.salary_annual_max,
        salary_is_predicted  = excluded.salary_is_predicted,
        date_posted          = excluded.date_posted,
        description          = excluded.description,
        tags                 = excluded.tags,
        url                  = excluded.url,
        first_seen_at        = least(t.first_seen_at, excluded.first_seen_at),
        last_seen_at         = greatest(t.last_seen_at, excluded.last_seen_at),
        updated_at           = now()
    where (t.job_title, t.company, t.location, t.employment_type, t.seniority, t.salary_annual_min,
           t.salary_annual_max, t.description, t.tags, t.last_seen_at)
          is distinct from
          (excluded.job_title, excluded.company, excluded.location, excluded.employment_type, excluded.seniority,
           excluded.salary_annual_min, excluded.salary_annual_max, excluded.description, excluded.tags,
           greatest(t.last_seen_at, excluded.last_seen_at))
    returning (xmax = 0) as inserted
)
select
    count(*) filter (where inserted)     as jobs_inserted,
    count(*) filter (where not inserted) as jobs_updated
from upserted;
