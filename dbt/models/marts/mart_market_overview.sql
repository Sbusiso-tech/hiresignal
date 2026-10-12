-- Headline numbers sliced by role family, seniority, employment type and remote status.
select
    job_family,
    seniority,
    employment_type,
    is_remote,
    count(*) as job_count,
    count(distinct company) as company_count,
    count(salary_usd_mid) as jobs_with_salary,
    round(avg(salary_usd_mid), 0) as avg_salary_usd,
    round(avg(experience_years_min), 1) as avg_min_years_experience,
    round(avg(skill_count), 1) as avg_skills_per_job
from {{ ref('int_jobs_enriched') }}
group by job_family, seniority, employment_type, is_remote
