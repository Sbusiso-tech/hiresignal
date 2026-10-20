"""CDC consumer: one Debezium change event becomes a Redis counter and a feed entry."""
import json

from jobmarket.cdc_consumer import apply_change

AFTER = {
    "vacancy_id": 7, "title": "Data Engineer", "client_id": 2, "city": "Cape Town", "province": "Western Cape",
    "work_mode": "Hybrid", "employment_type": "Permanent", "salary_min_zar": 45000.0, "salary_max_zar": 70000.0,
    "status": "open", "description": "kept out of the feed", "opened_at": "2026-10-20T08:00:00Z",
    "updated_at": "2026-10-20T08:00:00Z",
}


class Store:
    def __init__(self):
        self.counters: dict[str, int] = {}
        self.feed: list[str] = []

    def hincrby(self, _key, field, amount):
        self.counters[field] = self.counters.get(field, 0) + amount

    def lpush(self, _key, value):
        self.feed.insert(0, value)

    def ltrim(self, _key, start, end):
        self.feed = self.feed[start:end + 1]


def test_insert_update_and_delete():
    store = Store()
    apply_change(store, {"op": "c", "after": AFTER})
    apply_change(store, {"op": "u", "after": {**AFTER, "status": "filled"}})
    apply_change(store, {"op": "d", "before": AFTER, "after": None})

    assert store.counters == {"inserts": 1, "updates": 1, "deletes": 1}
    newest = json.loads(store.feed[0])
    assert newest["op"] == "delete"
    assert newest["title"] == "Data Engineer"
    assert "description" not in newest
    assert [json.loads(item)["op"] for item in store.feed] == ["delete", "update", "insert"]


def test_feed_is_capped_at_100():
    store = Store()
    for vacancy_id in range(120):
        apply_change(store, {"op": "r", "after": {**AFTER, "vacancy_id": vacancy_id}})
    assert len(store.feed) == 100
    assert json.loads(store.feed[0])["vacancy_id"] == 119
    assert store.counters == {"snapshot_reads": 120}


def test_microsecond_timestamps_become_iso_strings():
    store = Store()
    opened = 1760947200000000  # 2025-10-20T08:00:00Z
    apply_change(store, {"op": "c", "after": {**AFTER, "opened_at": opened, "updated_at": opened}})
    record = json.loads(store.feed[0])
    assert record["opened_at"] == "2025-10-20T08:00:00+00:00"


def test_unknown_operation_is_counted_and_not_stored():
    store = Store()
    apply_change(store, {"op": "t"})
    assert store.counters == {"other": 1}
    assert store.feed == []
