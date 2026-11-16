"""A record changed while the sync runs is still read: the window has no end (FourthRev,
2026-11-15: contacts and deals changed during a run were in no search bucket and missing).
Offline (tests/fake_hubspot.py)."""

from datetime import datetime, timedelta, timezone

import pytest

from fake_hubspot import FakeHubSpot, last_state, run_tap
from tap_hubspot_beta import stats

SIZES = {"contacts": 40, "companies": 3, "deals": 30}
SINCE = "2024-01-01T00:00:00+00:00"


def bookmark(stream):
    return {"bookmarks": {stream: {"replication_key": "updatedAt", "replication_key_value": SINCE}}}


def ids(messages, stream):
    return {m["record"]["id"] for m in messages if m.get("type") == "RECORD" and m["stream"] == stream}


def change_the_last_record_at_the_first_read(fake, obj):
    """The record with the highest id changes (now, after the sync started) as the first page
    of records is read: search pages by id, so its page comes later."""
    last = max(fake.records[obj], key=lambda r: int(r["id"]))["id"]
    changed_at = datetime.now(timezone.utc) + timedelta(seconds=5)

    def on_request(method, path, query, body):
        reading = (
            (method == "POST" and path.endswith(f"{obj}/search") and (body or {}).get("limit", 1) > 1)
            or (method == "GET" and path == f"crm/v3/objects/{obj}" and query.get("archived") == "false")
        )
        if reading and fake.on_request is not None:
            fake.touch(obj, last, changed_at)
            fake.on_request = None

    fake.on_request = on_request
    return last, changed_at


@pytest.mark.parametrize("stream", ["contacts", "deals"])
@pytest.mark.parametrize("strategy", ["search", "list"])
def test_a_record_changed_during_the_sync_is_read(tmp_path, stream, strategy):
    fake = FakeHubSpot(SIZES)
    last, changed_at = change_the_last_record_at_the_first_read(fake, stream)
    messages = run_tap(fake, tmp_path, streams=[stream], state=bookmark(stream), sync_strategy=strategy)
    assert stats.get().decisions[stream]["strategy"] == strategy
    assert last in ids(messages, stream)
    # The bookmark stays at the sync's start, before the change: the next run reads from there.
    mark = last_state(messages)["bookmarks"][stream]["replication_key_value"]
    assert datetime.fromisoformat(mark) < changed_at


def test_an_end_date_still_ends_the_window(tmp_path):
    fake = FakeHubSpot(SIZES)
    last, _ = change_the_last_record_at_the_first_read(fake, "contacts")
    messages = run_tap(fake, tmp_path, streams=["contacts"], state=bookmark("contacts"), sync_strategy="search",
                       end_date=datetime.now(timezone.utc).isoformat())
    assert last not in ids(messages, "contacts")
