"""Memory diagnostics: off by default (no MEMDIAG), on: a MEMDIAG line with heap, records,
pendulum counts and the probes' bytes per call."""

import json

from fake_hubspot import FakeHubSpot, run_tap
from tap_hubspot_beta import memdiag
from tap_hubspot_beta.client_base import _goes_to_pendulum

SIZES = {"contacts": 300, "deals": 40}
BOOKMARK = {"bookmarks": {"contacts": {"replication_key": "updatedAt", "replication_key_value": "2024-02-01T00:00:00+00:00"}}}


def test_off_by_default(tmp_path, caplog):
    with caplog.at_level("INFO"):
        run_tap(FakeHubSpot(SIZES), tmp_path, streams=["contacts"])
    assert "MEMDIAG" not in caplog.text


def test_on_it_reports_heap_records_and_probes(tmp_path, caplog, monkeypatch):
    monkeypatch.setattr(memdiag, "DIAG_SECONDS", 0.0)
    monkeypatch.setattr(memdiag, "PROBE_RUNS", 3)
    with caplog.at_level("INFO"):
        run_tap(FakeHubSpot(SIZES), tmp_path, streams=["contacts"], state=BOOKMARK, memory_diagnostics=True,
                sync_strategy="list")
    lines = [r.getMessage() for r in caplog.records if r.getMessage().startswith("MEMDIAG ")]
    assert lines
    report = json.loads(lines[-1][len("MEMDIAG "):])
    assert report["stream"] == "contacts" and report["records"]["contacts"] > 0
    assert "pendulum_calls" in report and "probes" in report
    if memdiag.heap() is not None:  # glibc only
        assert {"in_use_mb", "free_mb"} <= set(report)
        assert {"post_process", "parse_datetimes", "conform", "format_message"} <= set(report["probes"])
    memdiag.DIAG.enabled = False


def test_which_values_go_to_pendulum():
    assert _goes_to_pendulum("2024-01-01") and _goes_to_pendulum("2024-01-01T00:00:00+02:00")
    assert not _goes_to_pendulum("2024-01-01T00:00:00.123Z") and not _goes_to_pendulum("1700000000123")
