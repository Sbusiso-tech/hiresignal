-- Skills that are requested together ("If you know Spark, learn Kafka next").
with pairs as (
    select
        a.skill as skill_a,
        b.skill as skill_b,
        count(*) as jobs_together
    from {{ ref('stg_job_skills') }} a
    inner join {{ ref('stg_job_skills') }} b
        on a.job_id = b.job_id
       and a.skill < b.skill
    group by a.skill, b.skill
),

skill_totals as (
    select skill, count(*) as skill_jobs
    from {{ ref('stg_job_skills') }}
    group by skill
)

select
    p.skill_a,
    p.skill_b,
    p.jobs_together,
    round(100.0 * p.jobs_together / ta.skill_jobs, 2) as pct_of_skill_a_jobs,
    round(100.0 * p.jobs_together / tb.skill_jobs, 2) as pct_of_skill_b_jobs
from pairs p
inner join skill_totals ta on ta.skill = p.skill_a
inner join skill_totals tb on tb.skill = p.skill_b
where p.jobs_together >= 3
