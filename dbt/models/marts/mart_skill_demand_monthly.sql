-- Skill demand trend by month (for line charts / trend analysis).
with monthly as (
    select
        j.posted_month,
        s.skill,
        s.skill_category,
        count(distinct j.job_id) as job_count
    from {{ ref('stg_job_skills') }} s
    inner join {{ ref('int_jobs_enriched') }} j on j.job_id = s.job_id
    group by j.posted_month, s.skill, s.skill_category
),

month_totals as (
    select posted_month, count(*) as tech_jobs
    from {{ ref('int_jobs_enriched') }}
    where is_tech_role
    group by posted_month
)

select
    m.posted_month,
    m.skill,
    m.skill_category,
    m.job_count,
    round(100.0 * m.job_count / nullif(t.tech_jobs, 0), 2) as pct_of_tech_jobs,
    m.job_count - lag(m.job_count) over (partition by m.skill order by m.posted_month) as change_vs_prev_month
from monthly m
left join month_totals t on t.posted_month = m.posted_month
