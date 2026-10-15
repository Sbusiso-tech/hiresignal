"""Simulate day-to-day activity in the recruitment desk's CRM, an operational PostgreSQL database.

Recruiters open vacancies for their clients, re-price them, put them on hold, fill them or close
them. Clients and recruiters are fictional; titles, skills and ZAR salary bands follow the same
market profile as the partner feed.

    python -m jobmarket.crm_simulator                  # 25 random actions
    python -m jobmarket.crm_simulator --actions 100
    python -m jobmarket.crm_simulator --loop 10        # one batch every 10 s until stopped
"""
from __future__ import annotations

import argparse
import logging
import random
import sys
import time
from collections import Counter
from importlib import resources

from jobmarket.config import Settings, get_settings
from jobmarket.partner_feed import CITIES, LEVELS, ROLES, WORK_MODES

log = logging.getLogger("jobmarket.crm_simulator")

CLIENTS = [
    ("Ikhaya Bank", "Banking", "Johannesburg"), ("Aloe Insurance", "Insurance", "Cape Town"),
    ("Kalahari Retail", "Retail", "Durban"), ("Mopane Telecom", "Telecommunications", "Sandton"),
    ("Sable Mining", "Mining", "Pretoria"), ("Marula Health", "Healthcare", "Cape Town"),
    ("Impala Logistics", "Logistics", "Durban"), ("Savanna Software", "Software", "Stellenbosch"),
]
RECRUITERS = [
    ("Thandi Mokoena", "thandi.mokoena@desk.example.co.za"),
    ("Pieter van der Merwe", "pieter.vandermerwe@desk.example.co.za"),
    ("Ayesha Patel", "ayesha.patel@desk.example.co.za"),
    ("Sipho Ndlovu", "sipho.ndlovu@desk.example.co.za"),
]
EMPLOYMENT_TYPES = [("Permanent", 14), ("Contract", 4), ("Temporary", 1), ("Internship", 1)]
ACTIONS = [("open", 5), ("reprice", 2), ("hold", 1), ("fill", 2), ("close", 1), ("reopen", 1)]
BOOTSTRAP_VACANCIES = 60


def _weighted(rng: random.Random, options: list[tuple]) -> tuple:
    return rng.choices(options, weights=[option[-1] for option in options])[0]


def new_vacancy(rng: random.Random) -> dict:
    title, team, core, optional, (low, high) = rng.choice(ROLES)
    prefix, (exp_low, exp_high), factor, _ = _weighted(rng, LEVELS)
    employment_type = _weighted(rng, EMPLOYMENT_TYPES)[0]
    if employment_type == "Internship":
        prefix, (exp_low, exp_high), factor = "", (0, 0), 0.35
    city, province, _ = _weighted(rng, CITIES)
    work_mode = _weighted(rng, WORK_MODES)[0]
    skills = core + rng.sample(optional, k=min(len(optional), rng.randint(2, 4)))
    years = rng.randint(exp_low, exp_high)
    salary_min = round(low * factor * rng.uniform(0.9, 1.1), -3)
    salary_max = round(max(high * factor * rng.uniform(0.9, 1.1), salary_min + 5_000), -3)
    has_salary = rng.random() < 0.85
    suffix = " Intern" if employment_type == "Internship" else ""
    return {
        "title": f"{prefix}{title}{suffix}",
        "city": city,
        "province": province,
        "work_mode": work_mode,
        "employment_type": employment_type,
        "salary_min_zar": salary_min if has_salary else None,
        "salary_max_zar": salary_max if has_salary else None,
        "min_experience_years": years,
        "required_skills": skills,
        "description": (
            f"Our client is hiring a {prefix}{title}{suffix} for their {team} team in {city} ({work_mode.lower()}). "
            f"Day to day you will work with {', '.join(skills[:-1])} and {skills[-1]}. "
            f"Minimum {years} years of experience. Employment type: {employment_type}."
        ),
    }


def ensure_schema(conn) -> None:
    conn.execute(resources.files("jobmarket").joinpath("sql/crm_schema.sql").read_text())


def seed(conn) -> None:
    with conn.cursor() as cur:
        cur.executemany(
            "insert into crm.clients (name, industry, city) values (%s, %s, %s) on conflict (name) do nothing", CLIENTS
        )
        cur.executemany(
            "insert into crm.recruiters (full_name, email) values (%s, %s) on conflict (email) do nothing", RECRUITERS
        )


OPEN_SQL = """
insert into crm.vacancies (client_id, recruiter_id, title, city, province, work_mode, employment_type,
                           salary_min_zar, salary_max_zar, min_experience_years, required_skills, description)
values ((select client_id from crm.clients order by random() limit 1),
        (select recruiter_id from crm.recruiters order by random() limit 1),
        %(title)s, %(city)s, %(province)s, %(work_mode)s, %(employment_type)s,
        %(salary_min_zar)s, %(salary_max_zar)s, %(min_experience_years)s, %(required_skills)s, %(description)s)
"""
PICK = "(select vacancy_id from crm.vacancies where status = any(%(from)s) {extra} order by random() limit 1)"
STATUS_SQL = f"update crm.vacancies set status = %(to)s where vacancy_id = {PICK.format(extra='')}"
REPRICE_SQL = f"""
update crm.vacancies
   set salary_min_zar = round(salary_min_zar * %(factor)s / 1000) * 1000,
       salary_max_zar = round(salary_max_zar * %(factor)s / 1000) * 1000
 where vacancy_id = {PICK.format(extra='and salary_min_zar is not null')}
"""
TRANSITIONS = {
    "hold": (["open"], "on_hold"),
    "fill": (["open", "on_hold"], "filled"),
    "close": (["open", "on_hold"], "closed"),
    "reopen": (["on_hold"], "open"),
}


def run_actions(conn, rng: random.Random, count: int) -> Counter:
    stats: Counter = Counter()
    for _ in range(count):
        action = _weighted(rng, ACTIONS)[0]
        if action == "open":
            cur = conn.execute(OPEN_SQL, new_vacancy(rng))
        elif action == "reprice":
            cur = conn.execute(REPRICE_SQL, {"from": ["open", "on_hold"], "factor": rng.choice([0.95, 1.05, 1.1])})
        else:
            from_status, to_status = TRANSITIONS[action]
            cur = conn.execute(STATUS_SQL, {"from": from_status, "to": to_status})
        stats[action] += cur.rowcount
    return stats


def simulate(settings: Settings, actions: int, rng: random.Random) -> Counter:
    import psycopg

    with psycopg.connect(settings.crm_dsn) as conn:
        ensure_schema(conn)
        seed(conn)
        stats: Counter = Counter()
        if conn.execute("select count(*) from crm.vacancies").fetchone()[0] == 0:
            for _ in range(BOOTSTRAP_VACANCIES):
                conn.execute(OPEN_SQL, new_vacancy(rng))
            stats["bootstrap"] = BOOTSTRAP_VACANCIES
        stats.update(run_actions(conn, rng, actions))
    return stats


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--actions", type=int, default=25, help="number of CRM actions per batch")
    parser.add_argument("--loop", type=float, metavar="SECONDS", help="repeat a batch every SECONDS until stopped")
    parser.add_argument("--seed", type=int, help="random seed (default: random)")
    args = parser.parse_args(argv)

    settings = get_settings()
    if not settings.crm_host:
        log.error("CRM_HOST is not set")
        return 1
    rng = random.Random(args.seed)
    while True:
        log.info("crm activity: %s", dict(simulate(settings, args.actions, rng)))
        if not args.loop:
            return 0
        time.sleep(args.loop)


if __name__ == "__main__":
    sys.exit(main())
