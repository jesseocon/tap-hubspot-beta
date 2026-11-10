from unittest import mock

import requests

from tap_hubspot_beta.rate_limit import RateLimiter, request_kind


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


def test_general_requests_are_spaced_to_95_per_10s():
    clock = Clock()
    pacer = limiter(clock)
    for _ in range(96):
        pacer.acquire("general")
    # 95 requests fit in the first 10 seconds; the 96th starts at 10s.
    assert abs(clock.now - 1000.0 - 10.0) < 1e-6


def test_search_is_4_per_second_and_counts_as_general():
    clock = Clock()
    pacer = limiter(clock)
    for _ in range(5):
        pacer.acquire("search")
    assert abs(clock.now - 1001.0) < 1e-6
    pacer.acquire("general")
    assert abs(clock.now - 1001.0 - 10 / 95) < 1e-6


def test_processes_share_one_budget_through_the_file(tmp_path):
    clock = Clock()
    path = str(tmp_path / "rate")
    a, b = limiter(clock, path), limiter(clock, path)
    for _ in range(4):
        a.acquire("search")
        b.acquire("search")
    # 8 search requests between the two at 4/s: the last starts 1.75s in.
    assert abs(clock.now - 1001.75) < 1e-6


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


def test_headers_slow_down_a_nearly_spent_window_and_a_lower_limit():
    clock = Clock()
    pacer = limiter(clock)
    pacer.observe({"X-HubSpot-RateLimit-Max": "100", "X-HubSpot-RateLimit-Remaining": "4",
                   "X-HubSpot-RateLimit-Interval-Milliseconds": "10000"})
    pacer.acquire()
    assert abs(clock.now - 1005.0) < 1e-6
    start = clock.now
    pacer.acquire()
    pacer.acquire()
    # 86% of 100 per 10s.
    assert abs(clock.now - start - 2 * 10 / 86) < 1e-3


def test_request_kind():
    assert request_kind("/crm/v3/objects/contacts/search") == "search"
    assert request_kind("/crm/v3/objects/contacts?limit=100") == "general"
    assert request_kind("/crm/v4/associations/contacts/companies/batch/read") == "general"


def test_a_429_is_waited_out_and_resent():
    from tap_hubspot_beta.client_base import hubspotStream

    clock = Clock()
    pacer = limiter(clock)
    responses = []
    for status in (429, 429, 200):
        r = requests.Response()
        r.status_code = status
        r._content = b'{"policyName": "SECONDLY"}' if status == 429 else b"{}"
        responses.append(r)
    stream = mock.Mock(spec=["requests_session", "timeout", "rate_limiter", "logger", "name", "MAX_RATE_LIMIT_RETRIES"])
    stream.requests_session.send.side_effect = responses
    stream.rate_limiter = pacer
    stream.MAX_RATE_LIMIT_RETRIES = 8
    stream.name = "contacts"
    prepared = requests.Request("POST", "https://api.hubapi.com/crm/v3/objects/contacts/search").prepare()
    response = hubspotStream._send_paced(stream, prepared)
    assert response.status_code == 200
    assert stream.requests_session.send.call_count == 3
    assert clock.now >= 1002.0  # two 1s pauses, not 30s each
