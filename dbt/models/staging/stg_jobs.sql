with source as (
    select * from {{ source('core', 'jobs') }}
),

renamed as (
    select
        job_id,
        source,
        source_id,
        search_term,
        job_title,
        lower(job_title) as job_title_lower,
        coalesce(nullif(trim(company), ''), 'Unknown') as company,
        nullif(trim(location), '') as location,
        nullif(trim(country), '') as country,
        coalesce(is_remote, false) as is_remote,
        employment_type,
        seniority,
        experience_years_min,
        experience_years_max,
        salary_currency,
        salary_period,
        salary_annual_min,
        salary_annual_max,
        coalesce(salary_is_predicted, false) as salary_is_predicted,
        coalesce(date_posted, first_seen_at) as posted_at,
        coalesce(date_posted, first_seen_at)::date as posted_date,
        date_trunc('month', coalesce(date_posted, first_seen_at))::date as posted_month,
        url,
        first_seen_at,
        last_seen_at
    from source
)

select
    *,
    case
        when job_title_lower ~ '(machine learning|\mml\M|\mai\M|deep learning|mlops|\mllm|computer vision|\mnlp\M)'
            then 'ML / AI'
        when job_title_lower ~ '(data|analytics|etl|big data|bi) engineer|data platform|data architect'
            then 'Data Engineering'
        when job_title_lower ~ 'data scien'
            then 'Data Science'
        when job_title_lower ~ '(data analy|business intelligence|\mbi\M|analytics|reporting analyst|insights analyst)'
            then 'Data Analytics'
        when job_title_lower ~ '(devops|\msre\M|site reliability|platform engineer|cloud engineer|infrastructure|cloud architect)'
            then 'DevOps / Cloud'
        when job_title_lower ~ '(security|cyber|penetration|soc analyst)'
            then 'Security'
        when job_title_lower ~ '(\mqa\M|quality assurance|test automation|tester|\msdet\M)'
            then 'QA / Testing'
        when job_title_lower ~ '(front ?end|back ?end|full ?stack|software|developer|programmer|mobile|\mios\M|android|web engineer)'
            then 'Software Engineering'
        else 'Other'
    end as job_family
from renamed
