select
    md5(lower(company)) as company_key,
    min(company) as company,
    count(*) as job_count,
    count(*) filter (where is_tech_role) as tech_job_count,
    min(posted_date) as first_posted_date,
    max(posted_date) as last_posted_date
from {{ ref('int_jobs_enriched') }}
group by md5(lower(company))
