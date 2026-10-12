-- "What technologies are employers asking for?"
with jobs as (
    select * from {{ ref('int_jobs_enriched') }}
),

skills as (
    select * from {{ ref('stg_job_skills') }}
),

totals as (
    select
        count(*) filter (where is_tech_role) as tech_jobs,
        count(*) filter (where is_tech_role and posted_date >= current_date - 30) as tech_jobs_30d
    from jobs
),

per_skill as (
    select
        s.skill,
        s.skill_category,
        count(distinct j.job_id) as job_count,
        count(distinct j.job_id) filter (where j.posted_date >= current_date - 30) as job_count_30d,
        count(distinct j.job_id) filter (where j.is_remote) as remote_job_count,
        count(distinct j.company) as company_count,
        percentile_cont(0.5) within group (order by j.salary_usd_mid) as median_salary_usd,
        count(j.salary_usd_mid) as salary_sample_size
    from skills s
    inner join jobs j on j.job_id = s.job_id
    group by s.skill, s.skill_category
)

select
    rank() over (order by per_skill.job_count desc) as demand_rank,
    per_skill.skill,
    per_skill.skill_category,
    per_skill.job_count,
    round(100.0 * per_skill.job_count / nullif(totals.tech_jobs, 0), 2) as pct_of_tech_jobs,
    per_skill.job_count_30d,
    round(100.0 * per_skill.job_count_30d / nullif(totals.tech_jobs_30d, 0), 2) as pct_of_tech_jobs_30d,
    per_skill.remote_job_count,
    per_skill.company_count,
    round(per_skill.median_salary_usd::numeric, 0) as median_salary_usd,
    per_skill.salary_sample_size
from per_skill
cross join totals
