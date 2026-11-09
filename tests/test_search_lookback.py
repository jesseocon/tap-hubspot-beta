"""Tests for the lookback before the bookmark on search-API streams."""

from datetime import datetime, timezone
from unittest.mock import Mock

from tap_hubspot_beta.client_v3 import hubspotV3SearchStream

BOOKMARK = datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc)
BOOKMARK_MS = int(BOOKMARK.timestamp() * 1000)
MINUTE_MS = 60 * 1000


def _stream(config, bookmark=BOOKMARK, has_state=True):
    stream = Mock()
    stream._tap.config = config
    stream.get_starting_timestamp.return_value = bookmark
    stream.get_starting_replication_key_value.return_value = (
        bookmark.isoformat() if has_state else None
    )
    stream.lookback_ms = hubspotV3SearchStream.lookback_ms.fget(stream)
    return stream


def test_default_lookback_is_sixty_minutes_before_the_bookmark():
    stream = _stream({})
    assert hubspotV3SearchStream.get_starting_time(stream, None) == BOOKMARK_MS - 60 * MINUTE_MS


def test_lookback_minutes_is_configurable():
    stream = _stream({"lookback_minutes": 360})
    assert hubspotV3SearchStream.get_starting_time(stream, None) == BOOKMARK_MS - 360 * MINUTE_MS


def test_zero_turns_the_lookback_off():
    stream = _stream({"lookback_minutes": 0})
    assert hubspotV3SearchStream.get_starting_time(stream, None) == BOOKMARK_MS


def test_first_run_from_start_date_has_no_lookback():
    stream = _stream({"lookback_minutes": 60}, has_state=False)
    assert hubspotV3SearchStream.get_starting_time(stream, None) == BOOKMARK_MS


def test_no_bookmark_and_no_start_date_reads_from_2000():
    stream = _stream({}, bookmark=None, has_state=False)
    assert hubspotV3SearchStream.get_starting_time(stream, None) == int(
        datetime(2000, 1, 1).timestamp() * 1000
    )
