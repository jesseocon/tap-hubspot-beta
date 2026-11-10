"""The list API path, association batching and pacing give the records v1.0.81-aria.2 gave.

The fixture was recorded from v1.0.81-aria.2 on the same fake HubSpot (synthetic people only);
see tests/record_parity_fixture.py.
"""

import json
import logging
from collections import Counter
from pathlib import Path

import pytest

from fake_hubspot import FakeHubSpot, last_state, records, run_tap
from tap_hubspot_beta.client_v3 import hubspotV3SearchStream

FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "parity_v1.0.81-aria.2.json").read_text())
SIZES = FIXTURE["sizes"]


def requests_made(fake):
    return Counter(f"{c['method']} {c['path']}" for c in fake.calls if "properties" not in c["path"])


def bookmarks(messages):
    marks = last_state(messages).get("bookmarks", {})
    return {k: marks.get(k, {}).get("replication_key_value") for k in ("contacts", "companies", "deals")}


@pytest.fixture
def fake():
    return FakeHubSpot(SIZES)


def test_full_sync_through_the_list_api_gives_the_same_records(fake, tmp_path):
    messages = run_tap(fake, tmp_path)
    assert records(messages) == FIXTURE["records"]
    assert bookmarks(messages) == FIXTURE["bookmarks"]
    made = requests_made(fake)
    # No search (or its count requests) on a full sync.
    assert not [k for k in made if k.endswith("/search")]
    # 250 contacts: 3 list pages of 100 (the archived feeds add one empty page per object).
    assert made["GET crm/v3/objects/contacts"] == 3 + 1
    # One association call for the contacts, not one per 50.
    assert made["POST crm/v4/associations/contacts/companies/batch/read"] == 1
    assert FIXTURE["requests"]["POST crm/v4/associations/contacts/companies/batch/read"] == 5


def test_search_path_still_gives_the_same_records(fake, tmp_path):
    messages = run_tap(fake, tmp_path, use_list_api_for_full_sync=False)
    assert records(messages) == FIXTURE["records"]
    assert bookmarks(messages) == FIXTURE["bookmarks"]
    assert requests_made(fake)["POST crm/v3/objects/contacts/search"] == FIXTURE["requests"]["POST crm/v3/objects/contacts/search"]


def test_batch_read_when_the_properties_do_not_fit_in_a_url(fake, tmp_path, monkeypatch):
    monkeypatch.setattr(hubspotV3SearchStream, "MAX_LIST_URL_LENGTH", 50)
    messages = run_tap(fake, tmp_path)
    assert records(messages) == FIXTURE["records"]
    made = requests_made(fake)
    assert made["POST crm/v3/objects/contacts/batch/read"] == 3
    lists = [c for c in fake.calls if c["path"] == "crm/v3/objects/contacts" and c["query"].get("archived") == "false"]
    assert {c["query"]["properties"] for c in lists} == {"lastmodifieddate"}


def test_v4_association_calls_take_up_to_1000_ids(tmp_path):
    fake = FakeHubSpot({"contacts": 2300, "companies": 40, "deals": 5})
    run_tap(fake, tmp_path, streams=["associations_contacts_companies"])
    sizes = [len(inputs) for inputs in fake.v4_inputs]
    assert max(sizes) == 1000 and sum(sizes) >= 2200
    assert all(set(i) == {"id"} for inputs in fake.v4_inputs for i in inputs)


def test_incremental_runs_keep_the_search_api(fake, tmp_path):
    state = {"bookmarks": {"contacts": {"replication_key": "updatedAt", "replication_key_value": "2026-05-01T00:00:00+00:00"}}}
    messages = run_tap(fake, tmp_path, streams=["contacts"], state=state)
    made = requests_made(fake)
    assert made["POST crm/v3/objects/contacts/search"] >= 1
    assert "GET crm/v3/objects/contacts" not in made or all(
        c["query"].get("archived") == "true" for c in fake.calls if c["path"] == "crm/v3/objects/contacts"
    )
    got = records(messages)["contacts"]
    assert got and all(r["updatedAt"] >= "2026-04-30T23" for r in got)


class Lines(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.DEBUG)
        self.lines = []

    def emit(self, record):
        self.lines.append(self.format(record))


@pytest.mark.parametrize("with_filter", [False, True])
def test_no_email_reaches_the_logs(fake, tmp_path, monkeypatch, with_filter):
    """The log lines themselves leave emails out; the filter is a second layer."""
    from tap_hubspot_beta import privacy

    if not with_filter:
        monkeypatch.setattr(privacy, "install", lambda: None)
    handler = Lines()
    logging.getLogger().addHandler(handler)
    try:
        run_tap(fake, tmp_path, metrics_log_level="INFO")
    finally:
        logging.getLogger().removeHandler(handler)
        handler.close()
    err = "\n".join(handler.lines)
    assert "Beginning full_table sync of 'associations_contacts_companies' with context: {'ids': '<" in err
    assert "'context': {'ids': '<" in err  # the METRIC record_count line
    assert "example.test" not in err
    assert "subscriber_email" not in err
