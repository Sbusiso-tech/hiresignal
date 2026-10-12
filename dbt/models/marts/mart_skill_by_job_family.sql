-- Which skills matter most for each role family (e.g. Data Engineering vs Data Analytics)?
with family_totals as (
    select job_family, count(*) as family_jobs
    from {{ ref('int_jobs_enriched') }}
    group by job_family
),

counts as (
    select
        j.job_family,
        s.skill,
        s.skill_category,
        count(distinct j.job_id) as job_count
    from {{ ref('stg_job_skills') }} s
    inner join {{ ref('int_jobs_enriched') }} j on j.job_id = s.job_id
    group by j.job_family, s.skill, s.skill_category
)

select
    c.job_family,
    rank() over (partition by c.job_family order by c.job_count desc) as rank_in_family,
    c.skill,
    c.skill_category,
    c.job_count,
    f.family_jobs,
    round(100.0 * c.job_count / nullif(f.family_jobs, 0), 2) as pct_of_family_jobs
from counts c
inner join family_totals f on f.job_family = c.job_family
