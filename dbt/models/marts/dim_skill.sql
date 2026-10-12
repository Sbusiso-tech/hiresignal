select
    md5(skill) as skill_key,
    skill,
    min(skill_category) as skill_category
from {{ ref('stg_job_skills') }}
group by skill
