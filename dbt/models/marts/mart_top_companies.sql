-- Who is hiring the most tech talent, and what do they ask for?
with company_skills as (
    select
        j.company,
        s.skill,
        count(*) as mentions,
        row_number() over (partition by j.company order by count(*) desc, s.skill) as rn
    from {{ ref('stg_job_skills') }} s
    inner join {{ ref('int_jobs_enriched') }} j on j.job_id = s.job_id
    where j.company <> 'Unknown'
    group by j.company, s.skill
)

select
    j.company,
    count(*) as job_count,
    count(*) filter (where j.is_remote) as remote_job_count,
    round(avg(j.salary_usd_mid), 0) as avg_salary_usd,
    (
        select string_agg(cs.skill, ', ' order by cs.rn)
        from company_skills cs
        where cs.company = j.company and cs.rn <= 5
    ) as top_skills
from {{ ref('int_jobs_enriched') }} j
where j.is_tech_role and j.company <> 'Unknown'
group by j.company
