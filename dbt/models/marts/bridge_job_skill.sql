-- Many-to-many link between fct_job_postings and dim_skill (use for Power BI relationships).
select
    js.job_id,
    md5(js.skill) as skill_key,
    js.skill,
    js.skill_category
from {{ ref('stg_job_skills') }} js
inner join {{ ref('fct_job_postings') }} f on f.job_id = js.job_id
