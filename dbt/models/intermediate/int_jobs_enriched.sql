-- Adds USD-normalised salaries and skill counts to every job.
with jobs as (
    select * from {{ ref('stg_jobs') }}
),

rates as (
    select currency, usd_rate from {{ ref('currency_rates_to_usd') }}
),

skill_counts as (
    select job_id, count(*) as skill_count
    from {{ ref('stg_job_skills') }}
    group by job_id
)

select
    jobs.*,
    coalesce(skill_counts.skill_count, 0) as skill_count,
    coalesce(skill_counts.skill_count, 0) > 0 or jobs.job_family <> 'Other' as is_tech_role,
    round(jobs.salary_annual_min * rates.usd_rate, 0) as salary_usd_min,
    round(jobs.salary_annual_max * rates.usd_rate, 0) as salary_usd_max,
    round((jobs.salary_annual_min + jobs.salary_annual_max) / 2 * rates.usd_rate, 0) as salary_usd_mid
from jobs
left join rates on rates.currency = jobs.salary_currency
left join skill_counts on skill_counts.job_id = jobs.job_id
