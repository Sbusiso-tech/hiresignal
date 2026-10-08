import pytest

from jobmarket.skills import extract_skills, load_taxonomy


def names(text, tags=None):
    return {s["skill"] for s in extract_skills(text, tags)}


def test_taxonomy_loads_and_names_are_unique():
    skills = load_taxonomy()
    assert len(skills) > 100
    assert len({s.name for s in skills}) == len(skills)


@pytest.mark.parametrize(
    "text, expected",
    [
        ("Strong Python and SQL skills", {"Python", "SQL"}),
        ("Experience with C++ and C#", {"C++", "C#"}),
        ("Proficient in C/C++", {"C", "C++"}),
        ("Node.js, React.js and TypeScript", {"Node.js", "React", "TypeScript"}),
        ("PostgreSQL and NoSQL stores", {"PostgreSQL", "NoSQL"}),
        ("Build ETL pipelines with PySpark on Databricks", {"ETL", "Apache Spark", "Databricks"}),
        ("CI/CD with GitHub Actions, Docker and k8s", {"CI/CD", "GitHub", "GitHub Actions", "Docker", "Kubernetes"}),
        ("Python, R or SAS for statistics", {"Python", "R", "SAS", "Statistics"}),
        ("Golang microservices", {"Go", "Microservices"}),
        ("Power BI dashboards and Excel", {"Power BI", "Excel"}),
    ],
)
def test_extract_from_text(text, expected):
    assert names(text) == expected


@pytest.mark.parametrize(
    "text",
    [
        "Java",  # must not also match JavaScript
        "We go above and beyond for our customers",
        "Our postgresql cluster",  # must not also match SQL
        "You excel at communication and ensure the rest of the team succeeds",
    ],
)
def test_avoids_false_positives(text):
    found = names(text)
    assert "JavaScript" not in found
    assert "Go" not in found
    assert "SQL" not in found
    assert "Excel" not in found
    assert "REST APIs" not in found


def test_tags_are_matched_including_tag_only_aliases():
    assert names("", ["python", "go", "c", "Typescript ", "ruby/rails"]) == {"Python", "Go", "C", "TypeScript"}
