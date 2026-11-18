"""Datetime properties sent as epoch milliseconds: the same values as before, without pendulum's
C parser (it leaks on every string it can't parse; FourthRev's deals tap reached 2.35 GB)."""

import gc
from datetime import datetime, timezone

import pendulum
import pytz
import pytest
from hotglue_singer_sdk.helpers._typing import to_json_compatible

from tap_hubspot_beta.client_base import parse_hubspot_datetime


def old_value(text: str) -> str:
    """What the RECORD had before: pendulum failed, then fromtimestamp, naive, then the SDK."""
    with pytest.raises(Exception):
        pendulum.parse(text)
    naive = datetime.fromtimestamp(int(text) / 1000, tz=pytz.UTC).replace(tzinfo=None)
    return to_json_compatible(naive)


@pytest.mark.parametrize("text", ["1600000000000", "1700000000123", "1731888000999", "946684800000", "4102444800000"])
def test_epoch_milliseconds_give_the_same_record_value(text):
    value = parse_hubspot_datetime(text)
    assert value.tzinfo is timezone.utc
    assert value.isoformat() == old_value(text)


def test_other_text_still_goes_to_pendulum():
    assert parse_hubspot_datetime("20240101") == pendulum.parse("20240101")  # an ISO basic date
    assert parse_hubspot_datetime("2024-01-01") == pendulum.parse("2024-01-01")
    assert parse_hubspot_datetime("2024-01-01T10:00:00.5Z").isoformat() == "2024-01-01T10:00:00.500000+00:00"


def rss_kb() -> int:
    try:
        with open("/proc/self/status") as f:
            return next(int(line.split()[1]) for line in f if line.startswith("VmRSS:"))
    except OSError:
        import resource

        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024  # macOS: bytes, peak


def test_a_million_epoch_values_take_no_memory():
    for i in range(20000):  # warm up
        parse_hubspot_datetime(str(1600000000000 + i))
    gc.collect()
    before = rss_kb()
    for i in range(1_000_000):
        parse_hubspot_datetime(str(1600000000000 + i * 997))
    gc.collect()
    # pendulum's C parser kept ~115 bytes for each: ~110 MB here.
    assert rss_kb() - before < 20 * 1024
