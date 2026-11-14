import threading
import time
from unittest import mock

import pytest
import requests

from tap_hubspot_beta import rate_limit
from tap_hubspot_beta.rate_limit import DailyLimitReached, RateLimiter, request_kind

GENERAL = rate_limit.GENERAL_WINDOW + rate_limit.WINDOW_MARGIN["general"]
SEARCH = rate_limit.SEARCH_WINDOW + rate_limit.WINDOW_MARGIN["search"]


class Clock:
    def __init__(self):
        self.now = 1000.0
        self.slept = []

    def time(self):
        return self.now

    def sleep(self, seconds):
        self.slept.append(seconds)
        self.now += seconds


def limiter(clock, path=None, **kw):
    return RateLimiter(path, clock=clock.time, sleep=clock.sleep, **kw)


def test_110_general_requests_start_at_once_then_the_window_slides():
    clock = Clock()
    pacer = limiter(clock)
    for _ in range(110):
        assert pacer.acquire("general") == 0
    assert clock.now == 1000.0
    # The 111th waits for the first to leave the window.
    pacer.acquire("general")
    assert abs(clock.now - 1000.0 - GENERAL) < 1e-6


def test_no_window_ever_holds_more_than_the_limit():
    clock = Clock()
    pacer = limiter(clock, general_per_10s=20)
    starts = []
    for i in range(200):
        pacer.acquire("general")
        starts.append(clock.now)
        clock.now += 0.03 * (i % 7)  # uneven gaps, as threads would make
    for i, start in enumerate(starts):
        assert sum(1 for s in starts if start <= s < start + GENERAL) <= 20


def test_search_is_5_per_second_and_counts_as_general():
    clock = Clock()
    pacer = limiter(clock)
    for _ in range(5):
        pacer.acquire("search")
    assert clock.now == 1000.0
    pacer.acquire("search")
    assert abs(clock.now - 1000.0 - SEARCH) < 1e-6
    # 6 search requests took 6 of the 110 general starts.
    for _ in range(104):
        pacer.acquire("general")
    assert abs(clock.now - 1000.0 - SEARCH) < 1e-6
    pacer.acquire("general")
    assert abs(clock.now - 1000.0 - GENERAL) < 1e-6


def test_configured_limits():
    clock = Clock()
    pacer = limiter(clock, general_per_10s=10, search_per_second=2)
    for _ in range(2):
        pacer.acquire("search")
    pacer.acquire("search")
    assert abs(clock.now - 1000.0 - SEARCH) < 1e-6
    assert pacer.limits({})["general"][0] == 10


def test_processes_share_one_budget_through_the_file(tmp_path):
    clock = Clock()
    path = str(tmp_path / "rate")
    a, b = limiter(clock, path), limiter(clock, path)
    for _ in range(3):
        a.acquire("search")
        b.acquire("search")
    # 6 search requests between the two at 5 a second: the 6th waits a window.
    assert abs(clock.now - 1000.0 - SEARCH) < 1e-6


def test_threads_never_exceed_the_limit(monkeypatch):
    """Real threads and clock: no 0.5s window holds more than 12 starts."""
    monkeypatch.setattr(rate_limit, "GENERAL_WINDOW", 0.5)
    monkeypatch.setitem(rate_limit.WINDOW_MARGIN, "general", 0.0)
    pacer = RateLimiter(None, general_per_10s=12)
    starts = []
    lock = threading.Lock()

    def work():
        for _ in range(9):
            pacer.acquire("general")
            with lock:
                starts.append(time.monotonic())

    threads = [threading.Thread(target=work) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(starts) == 72
    starts.sort()
    for start in starts:
        # A few ms of slack: the start time is taken just after the wait.
        assert sum(1 for s in starts if start <= s < start + 0.5 - 0.01) <= 12
    # And the budget was used: 72 starts at 12 per 0.5s take at least 2.5s.
    assert starts[-1] - starts[0] >= 2.4


def test_a_429_pauses_everyone_for_retry_after(tmp_path):
    clock = Clock()
    path = str(tmp_path / "rate")
    a, b = limiter(clock, path), limiter(clock, path)
    a.acquire()
    assert a.wait_for_429("general", {"Retry-After": "3"}, "TEN_SECONDLY_ROLLING") == 3
    b.acquire()
    assert clock.now >= 1003.0


def test_429_without_retry_after_uses_the_policy_window():
    clock = Clock()
    pacer = limiter(clock)
    assert pacer.wait_for_429("search", {}, "SECONDLY") == 1.0
    assert pacer.wait_for_429("general", {"X-HubSpot-RateLimit-Interval-Milliseconds": "10000"}, None) == 5.0


def test_the_daily_limit_stops_the_sync_instead_of_retrying():
    clock = Clock()
    pacer = limiter(clock)
    with pytest.raises(DailyLimitReached, match="daily request limit"):
        pacer.wait_for_429("general", {"X-HubSpot-RateLimit-Daily": "250000"}, "DAILY")
    with pytest.raises(DailyLimitReached):
        pacer.observe({"X-HubSpot-RateLimit-Daily": "250000", "X-HubSpot-RateLimit-Daily-Remaining": "0"})
    pacer.observe({"X-HubSpot-RateLimit-Daily": "250000", "X-HubSpot-RateLimit-Daily-Remaining": "12"})


def test_headers_slow_down_a_nearly_spent_window_and_a_lower_limit():
    clock = Clock()
    pacer = limiter(clock)
    pacer.observe({"X-HubSpot-RateLimit-Max": "100", "X-HubSpot-RateLimit-Remaining": "2",
                   "X-HubSpot-RateLimit-Interval-Milliseconds": "10000"})
    pacer.acquire()
    assert abs(clock.now - 1005.0) < 1e-6
    # HubSpot's own lower limit is the one used.
    assert pacer.limits({"account_max": 100})["general"][0] == 100
    start = clock.now
    for _ in range(99):
        pacer.acquire()
    assert clock.now == start
    pacer.acquire()
    assert abs(clock.now - start - GENERAL) < 1e-6


def test_request_kind():
    assert request_kind("/crm/v3/objects/contacts/search") == "search"
    assert request_kind("/crm/v3/objects/contacts?limit=100") == "general"
    assert request_kind("/crm/v4/associations/contacts/companies/batch/read") == "general"


def _stream(pacer, statuses, body=b'{"policyName": "SECONDLY"}', headers=None):
    responses = []
    for status in statuses:
        r = requests.Response()
        r.status_code = status
        r._content = body if status == 429 else b"{}"
        r.headers.update(headers or {})
        responses.append(r)
    stream = mock.Mock(spec=["requests_session", "timeout", "rate_limiter", "logger", "name", "MAX_RATE_LIMIT_RETRIES"])
    stream.requests_session.send.side_effect = responses
    stream.rate_limiter = pacer
    stream.MAX_RATE_LIMIT_RETRIES = 8
    stream.name = "contacts"
    return stream


def test_a_429_is_waited_out_and_resent():
    from tap_hubspot_beta import stats
    from tap_hubspot_beta.client_base import hubspotStream

    counter = stats.reset()
    clock = Clock()
    stream = _stream(limiter(clock), (429, 429, 200))
    prepared = requests.Request("POST", "https://api.hubapi.com/crm/v3/objects/contacts/search").prepare()
    response = hubspotStream._send_paced(stream, prepared)
    assert response.status_code == 200
    assert stream.requests_session.send.call_count == 3
    assert clock.now >= 1002.0  # two 1s pauses, not 30s each
    endpoint = counter.streams["contacts"].endpoints["POST crm/v3/objects/contacts/search"]
    assert (endpoint.requests, endpoint.rate_limited) == (3, 2)


def test_a_daily_429_raises_at_once():
    from tap_hubspot_beta.client_base import hubspotStream

    clock = Clock()
    stream = _stream(limiter(clock), (429, 200), body=b'{"policyName": "DAILY"}')
    prepared = requests.Request("GET", "https://api.hubapi.com/crm/v3/objects/contacts").prepare()
    with pytest.raises(DailyLimitReached):
        hubspotStream._send_paced(stream, prepared)
    assert stream.requests_session.send.call_count == 1
