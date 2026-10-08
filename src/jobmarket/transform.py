"""Pure-Python cleaning and enrichment rules.

Kept free of Spark imports so the same functions are unit tested, used as Spark
UDFs in the batch job, and used by the real-time consumer.
"""
from __future__ import annotations

import html
import re
from typing import Any

from jobmarket.skills import extract_skills

HOURS_PER_YEAR = 2080
PERIOD_MULTIPLIER = {"hour": HOURS_PER_YEAR, "day": 260, "week": 52, "month": 12, "year": 1}

EMPLOYMENT_TYPES = ("full_time", "part_time", "contract", "internship", "temporary", "unspecified")

# --------------------------------------------------------------------------- text

_BLOCK_TAGS = re.compile(r"</?(?:p|div|br|li|ul|ol|h[1-6]|tr|table|section)\b[^>]*>", re.IGNORECASE)
_ANY_TAG = re.compile(r"<[^>]+>")
_WS = re.compile(r"[ \t\r\f\v]+")
_BLANK_LINES = re.compile(r"\n\s*\n+")


def strip_html(value: str | None) -> str:
    if not value:
        return ""
    text = _BLOCK_TAGS.sub("\n", value)
    text = _ANY_TAG.sub(" ", text)
    text = html.unescape(text).replace("\xa0", " ")
    text = "\n".join(line.strip() for line in _WS.sub(" ", text).split("\n"))
    return _BLANK_LINES.sub("\n", text).strip()


# ------------------------------------------------------------------------- salary

_CURRENCY_SYMBOLS = [
    ("US$", "USD"), ("A$", "AUD"), ("C$", "CAD"), ("$", "USD"), ("€", "EUR"), ("£", "GBP"),
    ("₹", "INR"), ("ZAR", "ZAR"), ("USD", "USD"), ("EUR", "EUR"), ("GBP", "GBP"), ("CAD", "CAD"),
    ("AUD", "AUD"), ("CHF", "CHF"), ("INR", "INR"),
]
_RAND = re.compile(r"(?<![A-Za-z])R\s?\d")
_NUMBER = re.compile(r"(\d+(?:[.,\s]\d+)*)\s*([kKmM])?(?![A-Za-z])")
_PERIODS = [
    ("hour", re.compile(r"/\s*h(?:ou)?r|per\s+hour|hourly|an?\s+hour|/hour", re.I)),
    ("day", re.compile(r"/\s*day|per\s+day|daily", re.I)),
    ("week", re.compile(r"/\s*w(?:ee)?k|per\s+week|weekly", re.I)),
    ("month", re.compile(r"/\s*mo(?:nth)?|per\s+month|monthly|p\.?m\.?\b", re.I)),
    ("year", re.compile(r"/\s*y(?:ea)?r|per\s+(?:year|annum)|annual|yearly|p\.?a\.?\b", re.I)),
]


def _parse_number(raw: str, suffix: str | None) -> float | None:
    raw = raw.replace(" ", "")
    if suffix and re.fullmatch(r"\d+,\d{1,2}", raw):
        raw = raw.replace(",", ".")  # "35,3k" -> 35.3k (European decimal comma)
    elif re.fullmatch(r"\d{1,3}(?:\.\d{3})+", raw):
        raw = raw.replace(".", "")  # "60.000" -> 60000 (European thousands dot)
    else:
        raw = raw.replace(",", "")
    try:
        value = float(raw)
    except ValueError:
        return None
    if suffix in ("k", "K"):
        value *= 1_000
    elif suffix in ("m", "M"):
        value *= 1_000_000
    return value


def parse_salary_text(text: str | None) -> dict[str, Any]:
    """Parse free-text salaries such as ``"$90k - $105k"`` or ``"$45-$120/Hour"``."""
    empty = {"salary_min": None, "salary_max": None, "salary_currency": None, "salary_period": None}
    if not text or not re.search(r"\d", text):
        return empty

    currency = next((code for symbol, code in _CURRENCY_SYMBOLS if symbol in text), None)
    if currency is None and _RAND.search(text):
        currency = "ZAR"

    values = [v for v in (_parse_number(n, s) for n, s in _NUMBER.findall(text)) if v and v > 0][:2]
    if not values:
        return empty

    low, high = min(values), max(values)
    period = next((name for name, pattern in _PERIODS if pattern.search(text)), None)
    if period is None:
        period = "hour" if high < 500 else "month" if high < 20_000 else "year"
    return {"salary_min": low, "salary_max": high, "salary_currency": currency, "salary_period": period}


_PERIOD_ALIASES = {
    "hour": "hour", "hourly": "hour", "hr": "hour",
    "day": "day", "daily": "day",
    "week": "week", "weekly": "week",
    "month": "month", "monthly": "month",
    "year": "year", "yearly": "year", "annual": "year", "annually": "year", "annum": "year",
}


def normalize_period(period: str | None) -> str | None:
    if not period:
        return None
    return _PERIOD_ALIASES.get(period.strip().lower())


def annualize(value: float | None, period: str | None) -> float | None:
    if value is None:
        return None
    return round(value * PERIOD_MULTIPLIER.get(period or "year", 1), 2)


# ----------------------------------------------------------------- employment type

_EMPLOYMENT_RULES = [
    ("internship", re.compile(r"intern|praktikum|werkstudent|working student|trainee|graduate programme", re.I)),
    ("contract", re.compile(r"contract|freelance|contractor|freiberuf", re.I)),
    ("temporary", re.compile(r"temp|befristet|seasonal", re.I)),
    ("part_time", re.compile(r"part[\s_-]?time|teilzeit|minijob", re.I)),
    ("full_time", re.compile(r"full[\s_-]?time|vollzeit|permanent|festanstellung|fulltime", re.I)),
]


def normalize_employment_type(raw: str | None, title: str | None = None) -> str:
    for candidate in (raw, title):
        if not candidate:
            continue
        for label, pattern in _EMPLOYMENT_RULES:
            if pattern.search(candidate):
                return label
    return "unspecified"


# ---------------------------------------------------------------------- experience

_YEARS = r"(?:years?|yrs?)"
_EXPERIENCE_PATTERNS = [
    re.compile(rf"(\d{{1,2}})\s*\+?\s*(?:-|–|to)\s*(\d{{1,2}})\s*\+?\s*{_YEARS}", re.I),
    re.compile(rf"(?:at least|minimum(?: of)?|min\.?|over|more than)\s*(\d{{1,2}})\s*\+?\s*{_YEARS}", re.I),
    re.compile(rf"(\d{{1,2}})\s*\+\s*{_YEARS}", re.I),
    re.compile(rf"(\d{{1,2}})\s*{_YEARS}(?:['’]s?)?\s+(?:of\s+)?(?:\w+\s+){{0,3}}experience", re.I),
    re.compile(r"(\d{1,2})\+?\s*Jahre?n?\s+(?:\w+\s+){0,2}(?:Berufs)?erfahrung", re.I),
]


def extract_experience(text: str | None, months: int | None = None) -> tuple[int | None, int | None]:
    if months:
        years = round(months / 12)
        return years, years
    if not text:
        return None, None
    mins: list[int] = []
    maxes: list[int] = []
    for pattern in _EXPERIENCE_PATTERNS:
        for match in pattern.finditer(text):
            numbers = [int(g) for g in match.groups() if g is not None]
            if not numbers or max(numbers) > 30:
                continue
            mins.append(min(numbers))
            maxes.append(max(numbers))
    if not mins:
        return None, None
    return min(mins), max(maxes)


_SENIORITY_RULES = [
    ("intern", re.compile(r"\b(?:intern|internship|praktikant|werkstudent|working student)\b", re.I)),
    ("lead", re.compile(r"\b(?:lead|principal|staff|head of|director|architect|vp)\b", re.I)),
    ("senior", re.compile(r"\b(?:senior|sr\.?|snr)\b", re.I)),
    ("junior", re.compile(r"\b(?:junior|jr\.?|graduate|entry[\s-]level|associate|berufseinstieg)\b", re.I)),
    ("mid", re.compile(r"\b(?:mid[\s-]?level|intermediate|medior|mid)\b", re.I)),
]


def infer_seniority(title: str | None, experience_min: int | None, employment_raw: str | None = None) -> str:
    for candidate in (title, employment_raw):
        if not candidate:
            continue
        for label, pattern in _SENIORITY_RULES:
            if pattern.search(candidate):
                return label
    if experience_min is None:
        return "unspecified"
    if experience_min < 2:
        return "junior"
    if experience_min < 5:
        return "mid"
    return "senior"


# ------------------------------------------------------------------------- record

def enrich(event: dict[str, Any]) -> dict[str, Any]:
    """Turn a ``RawJobEvent`` dict into the curated columns stored in the warehouse."""
    description = strip_html(event.get("description"))
    title = (event.get("job_title") or "").strip()

    salary = {
        "salary_min": event.get("salary_min") or None,
        "salary_max": event.get("salary_max") or None,
        "salary_currency": event.get("salary_currency"),
        "salary_period": normalize_period(event.get("salary_period")),
    }
    if salary["salary_min"] is None and salary["salary_max"] is None:
        salary = parse_salary_text(event.get("salary_text"))
    if salary["salary_min"] is not None and salary["salary_period"] is None:
        high = salary["salary_max"] or salary["salary_min"]
        salary["salary_period"] = "hour" if high < 500 else "month" if high < 20_000 else "year"
    if salary["salary_min"] is None:
        salary["salary_min"] = salary["salary_max"]
    if salary["salary_max"] is None:
        salary["salary_max"] = salary["salary_min"]

    exp_min, exp_max = extract_experience(description, event.get("experience_months"))
    skills = extract_skills(f"{title}\n{description}", event.get("tags"))

    return {
        "job_title": title,
        "company": (event.get("company") or "").strip() or None,
        "description": description,
        "employment_type": normalize_employment_type(event.get("employment_type_raw"), title),
        "seniority": infer_seniority(title, exp_min, event.get("employment_type_raw")),
        "experience_years_min": exp_min,
        "experience_years_max": exp_max,
        "salary_min": salary["salary_min"],
        "salary_max": salary["salary_max"],
        "salary_currency": salary["salary_currency"],
        "salary_period": salary["salary_period"],
        "salary_annual_min": annualize(salary["salary_min"], salary["salary_period"]),
        "salary_annual_max": annualize(salary["salary_max"], salary["salary_period"]),
        "skills": skills,
    }
