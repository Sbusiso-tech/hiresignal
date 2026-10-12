-- Salary distribution (annual, USD) for each skill. Only skills with enough salary data points are kept.
{% set min_sample = 5 %}

select
    s.skill,
    s.skill_category,
    count(*) as salary_sample_size,
    round(percentile_cont(0.25) within group (order by j.salary_usd_mid)::numeric, 0) as p25_salary_usd,
    round(percentile_cont(0.50) within group (order by j.salary_usd_mid)::numeric, 0) as median_salary_usd,
    round(percentile_cont(0.75) within group (order by j.salary_usd_mid)::numeric, 0) as p75_salary_usd,
    round(avg(j.salary_usd_mid), 0) as avg_salary_usd
from {{ ref('stg_job_skills') }} s
inner join {{ ref('int_jobs_enriched') }} j on j.job_id = s.job_id
where j.salary_usd_mid is not null
  and not j.salary_is_predicted
group by s.skill, s.skill_category
having count(*) >= {{ min_sample }}
