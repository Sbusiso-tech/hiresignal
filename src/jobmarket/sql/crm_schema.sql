-- Operational database of the in-house recruitment desk (OLTP, normalised).
-- Owned by the CRM application; the data platform only reads from it.

create schema if not exists crm;

create table if not exists crm.clients (
    client_id   serial primary key,
    name        text not null unique,
    industry    text not null,
    city        text not null,
    created_at  timestamptz not null default now()
);

create table if not exists crm.recruiters (
    recruiter_id serial primary key,
    full_name    text not null,
    email        text not null unique,
    created_at   timestamptz not null default now()
);

create table if not exists crm.vacancies (
    vacancy_id           bigserial primary key,
    client_id            int not null references crm.clients (client_id),
    recruiter_id         int references crm.recruiters (recruiter_id),
    title                text not null check (length(trim(title)) > 0),
    city                 text not null,
    province             text not null,
    work_mode            text not null check (work_mode in ('On-site', 'Hybrid', 'Remote')),
    employment_type      text not null check (employment_type in ('Permanent', 'Contract', 'Temporary', 'Internship')),
    salary_min_zar       numeric(12, 2) check (salary_min_zar > 0),
    salary_max_zar       numeric(12, 2) check (salary_max_zar >= salary_min_zar),
    min_experience_years int check (min_experience_years between 0 and 40),
    required_skills      text[] not null default '{}',
    description          text,
    status               text not null default 'open' check (status in ('open', 'on_hold', 'filled', 'closed')),
    opened_at            timestamptz not null default now(),
    updated_at           timestamptz not null default now()
);

create index if not exists vacancies_updated_at_idx on crm.vacancies (updated_at);
create index if not exists vacancies_status_idx on crm.vacancies (status);

create or replace function crm.touch_updated_at() returns trigger
language plpgsql as $$
begin
    new.updated_at := now();
    return new;
end;
$$;

drop trigger if exists vacancies_touch_updated_at on crm.vacancies;
create trigger vacancies_touch_updated_at
    before update on crm.vacancies
    for each row execute function crm.touch_updated_at();
