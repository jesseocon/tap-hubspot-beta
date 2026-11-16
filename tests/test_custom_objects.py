"""Custom objects are read like the CRM objects: since the bookmark, by search or list, with an
archived feed, and the same fields as the full read they replace. Offline (tests/fake_hubspot.py).
"""

import json
from collections import Counter

from unittest import mock

import pendulum
import pytest
import requests

from fake_hubspot import CUSTOM, FakeHubSpot, base_config, last_state, records, run_tap
from tap_hubspot_beta import stats
from tap_hubspot_beta.client_v3 import DynamicDiscoveredHubspotV3Stream
from tap_hubspot_beta.tap import Taphubspot

SIZES = {"contacts": 5, "companies": 3, "deals": 2}
NAME = CUSTOM["name"]
TYPE = CUSTOM["objectTypeId"]
SINCE = "2025-06-01T00:00:00+00:00"


def custom_run(tmp_path, fake, state=None, **config):
    config = {"discover_custom_objects": True, "start_date": "2006-01-01T00:00:00Z", **config}
    return run_tap(fake, tmp_path, streams=[NAME], state=state, **config)


def rows(messages, stream=NAME):
    return [m["record"] for m in messages if m.get("type") == "RECORD" and m["stream"] == stream]


def made(fake):
    return Counter(f"{c['method']} {c['path'].strip('/')}" for c in fake.calls if "properties" not in c["path"])


def bookmark(value=SINCE):
    return {"bookmarks": {NAME: {"replication_key": "updatedAt", "replication_key_value": value}}}


def old_full_read(self, custom_object):
    """v1.0.82-aria.1's custom object class: GET crm/v3/objects/<type>/, every record every run."""
    schema, associations_metadata = self.generate_schema(custom_object["properties"], custom_object.get("associations"),
                                                         custom_object["objectTypeId"])
    # Its schema had no archived-feed columns.
    schema = {**schema, "properties": {k: v for k, v in schema["properties"].items() if k not in ("_hg_archived", "archivedAt")}}
    return type("OldDealStagesStream", (DynamicDiscoveredHubspotV3Stream,), {
        "name": custom_object["name"], "path": f"crm/v3/objects/{custom_object['objectTypeId']}/",
        "records_jsonpath": "$.results[*]", "primary_keys": ["id"], "replication_key": "updatedAt", "page_size": 100,
        "schema": schema, "is_custom_stream": True, "object_id": custom_object.get("id"),
        "fullyQualifiedName": custom_object.get("fullyQualifiedName"), "associations_metadata": associations_metadata,
        "stream_object_type": custom_object.get("fullyQualifiedName"),
    })


def test_a_full_sync_gives_every_field_of_the_old_full_read(tmp_path, monkeypatch):
    new = custom_run(tmp_path, FakeHubSpot(SIZES, custom=240))
    assert stats.get().decisions[NAME]["strategy"] == "list"
    with monkeypatch.context() as m:
        m.setattr(Taphubspot, "generate_stream_class", old_full_read)
        m.setattr(Taphubspot, "generate_archived_stream_class", lambda self, record, cls: None)
        old = custom_run(tmp_path, FakeHubSpot(SIZES, custom=240))
    old_rows = {r["id"]: r for r in rows(old)}
    new_rows = {r["id"]: r for r in rows(new)}
    # One record has no hs_lastmodifieddate (HubSpot sets it on every record; the fake leaves
    # one out): only the old read, which applied no window, wrote it.
    assert set(old_rows) - set(new_rows) == {r["id"] for r in old_rows.values() if r.get("hs_lastmodifieddate") is None}
    assert len(new_rows) == len(old_rows) - 1 == 239
    def stored(row):  # what the warehouse holds: a null and a missing property are the same
        return {k: v for k, v in row.items() if v is not None}

    for rid, row in new_rows.items():
        # The same fields and values, plus the archived feed's mark.
        assert {k: v for k, v in stored(row).items() if k != "_hg_archived"} == stored(old_rows[rid])
        assert row["_hg_archived"] is False
    # With omit_null_properties (as the runner sets it), the same records.
    omitted = {r["id"]: r for r in rows(custom_run(tmp_path, FakeHubSpot(SIZES, custom=240), omit_null_properties=True))}
    assert omitted == {rid: stored(r) for rid, r in new_rows.items()}
    mark = last_state(new)["bookmarks"][NAME]
    assert pendulum.parse(mark["replication_key_value"]) == max(pendulum.parse(r["updatedAt"]) for r in new_rows.values())


def test_a_later_run_reads_only_what_changed(tmp_path):
    fake = FakeHubSpot(SIZES, custom=240)
    messages = custom_run(tmp_path, fake, state=bookmark())
    decision = stats.get().decisions[NAME]
    assert decision["strategy"] == "search" and decision["changed"] > 0
    got = rows(messages)
    assert 0 < len(got) < 239 and all(r["updatedAt"] >= "2025-05-31T23:00" for r in got)  # one hour of lookback
    assert made(fake)[f"POST crm/v3/objects/{TYPE}/search"] >= 2  # the count, then pages
    assert not [c for c in fake.calls if c["path"].strip("/") == f"crm/v3/objects/{TYPE}" and c["query"].get("archived") == "false"]
    # By list it is the same records and the same bookmark.
    listed = custom_run(tmp_path, FakeHubSpot(SIZES, custom=240), state=bookmark(), sync_strategy="list")
    assert records(listed)[NAME] == records(messages)[NAME]
    assert last_state(listed)["bookmarks"][NAME]["replication_key_value"] == last_state(messages)["bookmarks"][NAME]["replication_key_value"]


def test_archived_records_come_under_the_objects_name(tmp_path):
    fake = FakeHubSpot(SIZES, custom=20, archived={TYPE: 130})
    got = rows(custom_run(tmp_path, fake))
    archived = [r for r in got if r["_hg_archived"]]
    assert len(archived) == 130 and all(r["archivedAt"] for r in archived)
    # As with the CRM objects' feeds, no record carries the feed's own name.
    feed = custom_run(tmp_path, FakeHubSpot(SIZES, custom=20, archived={TYPE: 130}))
    assert not [m for m in feed if m.get("type") == "RECORD" and m["stream"] == f"{NAME}_archived"]
    # The feed's summary counts them (they go out under the object's name).
    assert stats.get().stream_summary(f"{NAME}_archived")["records"] == {f"{NAME}_archived": 130}
    # With the feed's bookmark, only those archived since.
    state = {"bookmarks": {f"{NAME}_archived": {"replication_key": "archivedAt", "replication_key_value": SINCE}}}
    since = [r for r in rows(custom_run(tmp_path, FakeHubSpot(SIZES, custom=20, archived={TYPE: 130}), state=state)) if r["_hg_archived"]]
    assert 0 < len(since) == len([r for r in fake.archived[TYPE] if r["archivedAt"] > "2025-06-01"]) < 130
    assert stats.get().stream_summary(f"{NAME}_archived")["records"] == {f"{NAME}_archived": len(since)}


def test_the_catalog_marks_the_object_and_hides_its_feed(tmp_path):
    fake = FakeHubSpot(SIZES, custom=5)
    with mock.patch.object(requests.Session, "send", lambda session, request, **kw: fake.send(session, request, **kw)):
        Taphubspot.custom_objects_streams = set()
        catalog = Taphubspot(config=base_config(tmp_path, discover_custom_objects=True)).catalog_dict
    entries = {e["tap_stream_id"]: e for e in catalog["streams"]}
    meta = lambda e: e["metadata"][-1]["metadata"]  # noqa: E731
    assert meta(entries[NAME])["object_id"] == CUSTOM["id"]
    feed = entries[f"{NAME}_archived"]
    assert "object_id" not in meta(feed) and any(m["metadata"].get("visible") is False for m in feed["metadata"])
    assert {"_hg_archived", "archivedAt", "stage", "hs_lastmodifieddate"} <= set(entries[NAME]["schema"]["properties"])


@pytest.mark.parametrize("state", [None, bookmark()])
def test_parallel_requests_give_the_sequential_records(tmp_path, state):
    one = custom_run(tmp_path, FakeHubSpot(SIZES, custom=240, archived={TYPE: 30}), state=state, max_parallel_requests=1)
    eight = custom_run(tmp_path, FakeHubSpot(SIZES, custom=240, archived={TYPE: 30}), state=state, max_parallel_requests=8)
    assert rows(one) == rows(eight)
    # The archived feed's bookmark is the run's time, as for the CRM objects' feeds.
    marks = lambda m: json.dumps({k: {x: y for x, y in v.items() if x != "replication_key_signpost"}  # noqa: E731
                                  for k, v in last_state(m).get("bookmarks", {}).items() if not k.endswith("_archived")},
                                 sort_keys=True)
    assert marks(one) == marks(eight)
