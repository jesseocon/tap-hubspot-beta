"""Key scans: a stream read since its bookmark also lists every live id (key_scan_interval_hours
or key_scan), written as {id, _sdc_key_only: true} after its records, so a loader can mark the
records HubSpot deleted for good (FourthRev, 2026-11-16: two contacts deleted in HubSpot were in
no feed: not live, not archived, not merged). Offline (tests/fake_hubspot.py)."""

from datetime import datetime, timedelta, timezone

import pytest

from fake_hubspot import CUSTOM, FakeHubSpot, last_state, run_tap
from tap_hubspot_beta import stats

SIZES = {"contacts": 250, "companies": 3, "deals": 30}
SINCE = "2025-06-01T00:00:00+00:00"


def bookmark(stream, **extra):
    return {"bookmarks": {stream: {"replication_key": "updatedAt", "replication_key_value": SINCE, **extra}}}


def key_rows(messages, stream):
    return [m["record"] for m in messages if m.get("type") == "RECORD" and m["stream"] == stream
            and m["record"].get("_sdc_key_only")]


def full_rows(messages, stream):
    return [m["record"] for m in messages if m.get("type") == "RECORD" and m["stream"] == stream
            and not m["record"].get("_sdc_key_only")]


def schema(messages, stream):
    return next(m for m in messages if m.get("type") == "SCHEMA" and m["stream"] == stream)["schema"]


def scanned_at(messages, stream):
    return last_state(messages)["bookmarks"][stream].get("key_scan_at")


@pytest.mark.parametrize("stream", ["contacts", "deals"])
def test_a_due_scan_writes_every_live_id_after_the_records(tmp_path, stream):
    fake = FakeHubSpot(SIZES)
    messages = run_tap(fake, tmp_path, streams=[stream], state=bookmark(stream), key_scan_interval_hours=24)
    keys = key_rows(messages, stream)
    assert sorted(r["id"] for r in keys) == sorted(r["id"] for r in fake.records[stream])
    assert all(set(r) == {"id", "_sdc_key_only"} for r in keys)
    assert schema(messages, stream)["properties"]["_sdc_key_only"] == {"type": ["null", "boolean"]}
    # After the stream's own records, and the listing asks for one property.
    kinds = [bool(m["record"].get("_sdc_key_only")) for m in messages if m.get("type") == "RECORD" and m["stream"] == stream]
    assert kinds == sorted(kinds)
    listing = [c for c in fake.calls if c["method"] == "GET" and c["path"] == f"crm/v3/objects/{stream}"
               and c["query"].get("properties") == "hs_object_id"]
    assert len(listing) == -(-len(fake.records[stream]) // 100)
    assert scanned_at(messages, stream) is not None
    assert stats.get().streams[f"{stream}_key_scan"].records == len(fake.records[stream])


def test_no_scan_until_due_and_none_without_the_setting(tmp_path):
    fake = FakeHubSpot(SIZES)
    recent = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    messages = run_tap(fake, tmp_path, streams=["contacts"], state=bookmark("contacts", key_scan_at=recent),
                       key_scan_interval_hours=24)
    assert key_rows(messages, "contacts") == []
    assert scanned_at(messages, "contacts") == recent
    old = (datetime.now(timezone.utc) - timedelta(hours=25)).isoformat()
    messages = run_tap(fake, tmp_path, streams=["contacts"], state=bookmark("contacts", key_scan_at=old),
                       key_scan_interval_hours=24)
    assert len(key_rows(messages, "contacts")) == len(fake.records["contacts"])
    # Not configured: no ids, no column.
    messages = run_tap(fake, tmp_path, streams=["contacts"], state=bookmark("contacts"))
    assert key_rows(messages, "contacts") == []
    assert "_sdc_key_only" not in schema(messages, "contacts")["properties"]


def test_key_scan_forces_one(tmp_path):
    fake = FakeHubSpot(SIZES)
    recent = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    messages = run_tap(fake, tmp_path, streams=["deals"], state=bookmark("deals", key_scan_at=recent), key_scan=True)
    assert len(key_rows(messages, "deals")) == len(fake.records["deals"])


def test_a_sync_without_a_bookmark_lists_everything_and_scans_nothing(tmp_path):
    fake = FakeHubSpot(SIZES)
    messages = run_tap(fake, tmp_path, streams=["contacts"], key_scan_interval_hours=24)
    assert key_rows(messages, "contacts") == []
    assert full_rows(messages, "contacts")
    # The full read counts as the last scan.
    assert scanned_at(messages, "contacts") is not None


def test_a_failed_listing_writes_no_ids_and_stays_due(tmp_path):
    """A listing stopped part-way would leave ids out, which a loader reads as deleted."""
    fake = FakeHubSpot(SIZES)
    pages = {"n": 0}

    def fail_the_second_listing_page(method, path, query, body):
        if method == "GET" and path == "crm/v3/objects/contacts" and query.get("properties") == "hs_object_id":
            pages["n"] += 1
            if pages["n"] == 2:
                raise ConnectionError("dropped")

    fake.on_request = fail_the_second_listing_page
    messages = run_tap(fake, tmp_path, streams=["contacts"], state=bookmark("contacts"), key_scan_interval_hours=24,
                       max_parallel_requests=1)
    assert pages["n"] >= 2
    assert key_rows(messages, "contacts") == []
    # The stream itself still synced, and the scan is due next run.
    assert scanned_at(messages, "contacts") is None


def test_an_empty_listing_writes_nothing(tmp_path):
    fake = FakeHubSpot(SIZES)
    original = fake.list

    def empty_key_listing(obj, query, archived=False):
        if query.get("properties") == "hs_object_id":
            return {"results": []}
        return original(obj, query, archived)

    fake.list = empty_key_listing
    messages = run_tap(fake, tmp_path, streams=["deals"], state=bookmark("deals"), key_scan_interval_hours=24)
    assert key_rows(messages, "deals") == []
    assert scanned_at(messages, "deals") is None


def test_custom_objects_scan_their_keys_too(tmp_path):
    fake = FakeHubSpot({"contacts": 5}, custom=120)
    name = CUSTOM["name"]
    messages = run_tap(fake, tmp_path, streams=[name], state=bookmark(name), key_scan_interval_hours=24,
                       discover_custom_objects=True, start_date="2006-01-01T00:00:00Z")
    assert sorted(r["id"] for r in key_rows(messages, name)) == sorted(r["id"] for r in fake.records[CUSTOM["objectTypeId"]])
