"""Requests in parallel, search or list per run, and what the run logs: same records, same state.

All offline, on tests/fake_hubspot.py (synthetic people only).
"""

import json
import logging
import random
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pendulum
import pytest
from hotglue_singer_sdk.helpers._typing import conform_record_data_types

from fake_hubspot import FakeHubSpot, last_state, records, run_tap
from tap_hubspot_beta import stats
from tap_hubspot_beta.client_base import hubspotStream, parse_hubspot_datetime
from tap_hubspot_beta.client_v3 import hubspotV3SearchStream
from tap_hubspot_beta.client_v4 import hubspotV4Stream
from tap_hubspot_beta.rate_limit import RateLimiter

FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "parity_v1.0.81-aria.2.json").read_text())
SIZES = FIXTURE["sizes"]
BOOKMARK = "2025-06-01T00:00:00+00:00"


def by_stream(messages):
    """Each stream's RECORD messages in the order written (not sorted)."""
    out = {}
    for m in messages:
        if m.get("type") == "RECORD":
            out.setdefault(m["stream"], []).append(m["record"])
    return out


def states(messages):
    return [m["value"] for m in messages if m.get("type") == "STATE"]


def without_signposts(state):
    text = json.dumps(state, sort_keys=True)
    marks = json.loads(text).get("bookmarks", {})
    for mark in marks.values():
        mark.pop("replication_key_signpost", None)
    return marks


def made(fake):
    return Counter(f"{c['method']} {c['path']}" for c in fake.calls if "properties" not in c["path"])


def incremental(stream="contacts", value=BOOKMARK):
    return {"bookmarks": {stream: {"replication_key": "updatedAt", "replication_key_value": value}}}


# --- concurrency keeps the output ---------------------------------------------------------


def test_parallel_requests_give_the_sequential_output(tmp_path):
    sequential = run_tap(FakeHubSpot(SIZES), tmp_path, max_parallel_requests=1)
    parallel = run_tap(FakeHubSpot(SIZES), tmp_path, max_parallel_requests=8)
    # Each stream's records in the same order, the same state.
    assert by_stream(parallel) == by_stream(sequential)
    assert records(parallel) == FIXTURE["records"]
    assert without_signposts(last_state(parallel)) == without_signposts(last_state(sequential))


def test_no_state_comes_before_the_child_records_it_covers(tmp_path, monkeypatch):
    """With child batches of 20 and a STATE every 50 contacts, each STATE follows the
    association records of every full batch of contacts written before it."""
    monkeypatch.setattr(hubspotV4Stream, "max_inputs_per_request", 20)
    monkeypatch.setattr(hubspotStream, "STATE_MSG_FREQUENCY", 50)
    fake = FakeHubSpot(SIZES)
    messages = run_tap(fake, tmp_path, streams=["contacts", "associations_contacts_companies"], max_parallel_requests=8)
    contacts, linked = [], set()
    checked = 0
    for m in messages:
        if m.get("type") == "RECORD" and m["stream"] == "contacts":
            contacts.append(m["record"]["id"])
        elif m.get("type") == "RECORD" and m["stream"] == "associations_contacts_companies":
            linked.add(m["record"]["from_id"])
        elif m.get("type") == "STATE" and contacts:
            full = contacts[: len(contacts) // 20 * 20]
            expected = {c for c in full if fake.links["contacts"]["companies"].get(c)}
            assert expected <= linked
            checked += 1
    assert checked >= 4
    # Every batch went out once, 20 ids each but the last.
    sizes = [len(i) for i in fake.v4_inputs]
    assert sum(sizes) == len(contacts) and max(sizes) == 20


def test_history_batches_of_50_in_parallel_give_the_sequential_records(tmp_path):
    streams = ["deals", "deals_history_properties"]
    seq_fake, par_fake = FakeHubSpot(SIZES), FakeHubSpot(SIZES)
    sequential = run_tap(seq_fake, tmp_path, streams=streams, max_parallel_requests=1)
    parallel = run_tap(par_fake, tmp_path, streams=streams, max_parallel_requests=8)
    got = by_stream(parallel)["deals_history_properties"]
    assert got == by_stream(sequential)["deals_history_properties"]
    assert len(got) == len(by_stream(parallel)["deals"]) > 50
    assert all(r["propertiesWithHistory"] for r in got)
    assert max(len(i) for i in par_fake.history_inputs) == 50
    assert sorted(len(i) for i in par_fake.history_inputs) == sorted(len(i) for i in seq_fake.history_inputs)


def test_archived_records_through_the_list_api(tmp_path):
    """The archived feed (GET ...?archived=true) writes its rows under the parent's name, with
    _hg_archived true; with a bookmark, only records archived since."""
    def run(state=None, parallel=8):
        fake = FakeHubSpot({**SIZES, "contacts": 30}, archived={"contacts": 230})
        messages = run_tap(fake, tmp_path, streams=["contacts"], state=state, max_parallel_requests=parallel)
        pages = [c for c in fake.calls if c["path"] == "crm/v3/objects/contacts" and c["query"].get("archived") == "true"]
        rows = [r for r in by_stream(messages)["contacts"] if r["_hg_archived"]]
        return fake, rows, pages

    fake, rows, pages = run()
    assert len(rows) == 230 and all(r["archivedAt"] for r in rows)
    assert len(pages) == 3  # 100 a page
    assert run(parallel=1)[1] == rows
    state = {"bookmarks": {"contacts_v3_archived": {"replication_key": "archivedAt", "replication_key_value": BOOKMARK}}}
    _, rows, _ = run(state=state)
    expected = [r for r in fake.archived["contacts"] if r["archivedAt"] > "2025-06-01"]
    assert 0 < len(rows) == len(expected) < 230


# --- search or list -----------------------------------------------------------------------


def decisions():
    return stats.get().decisions


def test_no_bookmark_reads_by_list(tmp_path):
    fake = FakeHubSpot(SIZES)
    run_tap(fake, tmp_path, streams=["contacts"])
    assert decisions()["contacts"]["strategy"] == "list"
    assert not [k for k in made(fake) if k.endswith("/search")]


def test_few_changed_records_are_read_by_search(tmp_path):
    fake = FakeHubSpot(SIZES)
    messages = run_tap(fake, tmp_path, streams=["contacts"], state=incremental())
    decision = decisions()["contacts"]
    assert decision["strategy"] == "search" and decision["changed"] > 0
    assert "total" not in decision  # under auto_list_min_changed: no second count
    assert by_stream(messages)["contacts"]


def test_auto_takes_the_faster_estimate(tmp_path, monkeypatch):
    def run(search, listed):
        monkeypatch.setattr(hubspotV3SearchStream, "estimate_seconds", lambda self, c, t: {"search": search, "list": listed})
        fake = FakeHubSpot(SIZES)
        messages = run_tap(fake, tmp_path, streams=["contacts"], state=incremental(), auto_list_min_changed=0)
        return fake, messages, decisions()["contacts"]

    fake, by_list, decision = run(search=100.0, listed=10.0)
    # total: every contact, with or without a modified date (search's unfiltered count).
    assert decision["strategy"] == "list" and decision["total"] == SIZES["contacts"]
    assert made(fake)["POST crm/v3/objects/contacts/search"] == 2  # the two counts, no pages
    fake, by_search, decision = run(search=10.0, listed=100.0)
    assert decision["strategy"] == "search"
    # Either way, the same records and the same bookmark.
    assert records(by_list) == records(by_search)
    assert without_signposts(last_state(by_list))["contacts"] == without_signposts(last_state(by_search))["contacts"]


@pytest.mark.parametrize("config, state, expected", [
    ({"sync_strategy": "search"}, None, "search"),
    ({"use_list_api_for_full_sync": False}, None, "search"),
    ({"sync_strategy": "list"}, "incremental", "list"),
    ({"sync_strategy": "auto"}, None, "list"),
])
def test_configured_strategy(tmp_path, config, state, expected):
    fake = FakeHubSpot(SIZES)
    run_tap(fake, tmp_path, streams=["contacts"], state=incremental() if state else None, **config)
    assert decisions()["contacts"]["strategy"] == expected
    searched = made(fake)["POST crm/v3/objects/contacts/search"]
    assert (searched > 0) == (expected == "search")


def test_bookmark_after_an_incremental_list_run(tmp_path):
    """List and search read the same window and leave the same bookmark: the latest
    updatedAt written."""
    by_list = run_tap(FakeHubSpot(SIZES), tmp_path, streams=["contacts"], state=incremental(), sync_strategy="list")
    by_search = run_tap(FakeHubSpot(SIZES), tmp_path, streams=["contacts"], state=incremental(), sync_strategy="search")
    assert records(by_list) == records(by_search)
    written = [r["updatedAt"] for r in by_stream(by_list)["contacts"]]
    assert written and min(written) >= "2025-05-31T23"  # the window starts an hour before the bookmark
    mark = last_state(by_list)["bookmarks"]["contacts"]
    assert pendulum.parse(mark["replication_key_value"]) == max(pendulum.parse(w) for w in written)
    assert without_signposts(last_state(by_list))["contacts"] == without_signposts(last_state(by_search))["contacts"]


class _Estimating(hubspotV3SearchStream):
    """Just enough of a contacts stream for estimate_seconds (no tap, no HTTP)."""

    def __init__(self):  # noqa: D107 - no SDK setup
        self.page_size = 200
        self.max_bucket_size = 5000

    path = "crm/v3/objects/contacts/search"
    config = {}
    rate_limiter = RateLimiter()

    def _list_needs_batch_read(self):
        return True


def test_the_estimate():
    stats.reset()
    stream = _Estimating()
    # Nearly everything changed: one list scan beats paging it all through search.
    est = stream.estimate_seconds(182_400, 190_112)
    assert est["list"] < est["search"]
    # A day's changes on a large portal: search.
    est = stream.estimate_seconds(5_000, 425_000)
    assert est["search"] < est["list"]


# --- less work per record, same rows ------------------------------------------------------


def test_omitted_nulls_leave_the_same_rows(tmp_path):
    full = records(run_tap(FakeHubSpot(SIZES), tmp_path))
    omitted = records(run_tap(FakeHubSpot(SIZES), tmp_path, omit_null_properties=True))
    strip = {s: sorted(({k: v for k, v in r.items() if v is not None} for r in rows), key=lambda r: json.dumps(r, sort_keys=True))
             for s, rows in full.items()}
    assert omitted == strip
    assert any(None in r.values() for r in full["contacts"])
    assert not any(None in r.values() for rows in omitted.values() for r in rows)


def test_the_record_conform_matches_the_sdk():
    schema = {"properties": {
        "b": {"type": ["boolean", "null"]}, "b2": {"anyOf": [{"type": ["boolean"]}, {"type": "null"}]},
        "s": {"type": ["string", "null"]}, "d": {"type": ["string", "null"], "format": "date-time"},
        "n": {"type": ["number", "null"]}, "o": {"type": ["object", "null"]},
    }}
    stream = hubspotStream.__new__(hubspotStream)
    stream.__dict__.update(_conform_plan=(schema["properties"], {"b"}, True), schema=schema, logger=logging.getLogger("t"),
                           name="t", _config={})
    stream.__class__ = type("S", (hubspotStream,), {"schema": schema, "config": property(lambda self: {}),
                                                     "name": "t", "logger": logging.getLogger("t")})
    rng = random.Random(3)
    values = [None, 0, 1, "", "true", "false", "x", 1.5, True, False, {"a": 1}, [1],
              datetime(2024, 1, 2, 3, 4, 5, 123000, tzinfo=timezone.utc), datetime(2024, 1, 2, 3, 4, 5),
              pendulum.datetime(2024, 1, 2, tz="UTC"), pendulum.datetime(2024, 1, 2, 5, tz="Asia/Tokyo")]
    for _ in range(500):
        row = {k: rng.choice(values) for k in schema["properties"] if rng.random() < 0.8}
        assert stream._conform(dict(row)) == conform_record_data_types("t", dict(row), schema, logging.getLogger("t"))


@pytest.mark.parametrize("text", [
    "2024-01-01T00:00:00.123Z", "2024-01-01T00:00:00Z", "2024-01-01T00:00:00.000Z", "2024-03-05T10:11:12.5Z",
    "2024-12-31T23:59:59.999999Z", "2024-01-01", "2024-01-01T05:00:00.123+05:00", "2024-02-30T00:00:00Z",
])
def test_hubspot_datetimes_parse_as_pendulum_does(text):
    from hotglue_singer_sdk.helpers._typing import to_json_compatible

    try:
        expected = to_json_compatible(pendulum.parse(text))
    except Exception as exc:
        with pytest.raises(type(exc)):
            parse_hubspot_datetime(text)
        return
    value = parse_hubspot_datetime(text)
    assert value == pendulum.parse(text)
    assert to_json_compatible(value) == expected
    assert (value.isoformat() if value.tzinfo is timezone.utc else to_json_compatible(value)) == expected


# --- what the run says ----------------------------------------------------------------------


class Lines(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.INFO)
        self.lines = []

    def emit(self, record):
        self.lines.append(record.getMessage())


def test_decisions_and_summaries_are_logged(tmp_path, monkeypatch):
    monkeypatch.setattr(hubspotV3SearchStream, "estimate_seconds", lambda self, c, t: {"search": 2460.0, "list": 540.0})
    handler = Lines()
    logging.getLogger().addHandler(handler)
    try:
        run_tap(FakeHubSpot(SIZES), tmp_path, streams=["contacts", "associations_contacts_companies", "deals"],
                state=incremental(), auto_list_min_changed=0)
    finally:
        logging.getLogger().removeHandler(handler)
    text = "\n".join(handler.lines)
    assert f"contacts: {decisions()['contacts']['changed']:,} changed since 2025-05-31T23:00Z of 250 -> list (est. 9m00s vs 41m00s by search)" in text
    assert "deals: list (no bookmark (a first sync or a full refresh))" in text
    summaries = {}
    for line in handler.lines:
        if line.startswith("SYNC_SUMMARY "):
            data = json.loads(line[len("SYNC_SUMMARY "):])
            summaries[data["scope"]] = data
    contacts = summaries["contacts"]
    assert contacts["records"]["associations_contacts_companies"] > 0
    assert contacts["endpoints"]["GET crm/v3/objects/contacts"]["requests"] == 3
    assert contacts["endpoints"]["POST crm/v4/associations/contacts/companies/batch/read"]["requests"] == 1
    assert contacts["strategy"]["contacts"]["strategy"] == "list"
    assert contacts["rate_limited"] == 0 and contacts["wall_seconds"] >= 0 and "cpu_seconds" in contacts
    run = summaries["run"]
    assert set(run["records"]) >= {"contacts", "deals", "associations_contacts_companies"}
    assert run["requests"] == sum(e["requests"] for e in run["endpoints"].values())
    assert any(line.startswith("contacts: ") and "records in" in line and "429s" in line for line in handler.lines)
    assert any(line.startswith("run: ") for line in handler.lines)
