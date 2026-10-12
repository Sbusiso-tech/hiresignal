select
    job_id,
    skill,
    skill_category
from {{ source('core', 'job_skills') }}
