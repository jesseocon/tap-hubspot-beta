"""The request budget of one HubSpot account, shared by every thread and tap process syncing it.

HubSpot limits an OAuth app to 110 requests per 10 seconds per account (more for some private
apps), and the CRM search endpoints to 5 requests per second per account, on top of that. The
tap now sends several requests at once (tap_hubspot_beta.concurrency), so the budget is what
keeps it under those limits:

- "general": every request. At most GENERAL_PER_10S start in any 10-second window.
- "search": crm/v3/objects/*/search, at most SEARCH_PER_SECOND in any second; each one also
  takes a general slot.

It is a sliding window, not a spacing: a request may start at once while the window has room,
so threads waiting on slow answers don't leave the budget unused. Each kind keeps the start
times of its last N requests; the next start is the later of now and the time the oldest of
those leaves the window (plus a small margin for the time a request takes to reach HubSpot).
A start is reserved before the wait, so waiting threads keep their order.

With config `rate_limit_file`, the start times live in that file, locked around every read
and write, so tap processes running side by side for one account share one budget. Without
it they live in this process.

What HubSpot says adjusts it:
- X-HubSpot-RateLimit-Max lower than the configured limit: that limit is used.
- X-HubSpot-RateLimit-Remaining nearly spent (other apps use the account too): starts pause.
- A 429: every start of that kind waits Retry-After (else the policy's window), then the
  request is resent (client_base._send_paced).
- The daily limit (X-HubSpot-RateLimit-Daily-Remaining, or a 429 with policy DAILY): the sync
  stops with DailyLimitReached, which says when the limit resets; retrying would only fail
  until then.
"""

from __future__ import annotations

import bisect
import json
import os
import threading
import time
from typing import Callable, Dict, List, Mapping, Optional, Tuple

GENERAL_PER_10S = 110
GENERAL_WINDOW = 10.0
SEARCH_PER_SECOND = 5
SEARCH_WINDOW = 1.0
# Added to each window: a request counted by HubSpot when it arrives, not when it is sent.
WINDOW_MARGIN = {"general": 0.25, "search": 0.1}
KINDS = ("general", "search")
# X-HubSpot-RateLimit-Remaining at or below these: wait a little, or most of the window.
LOW_REMAINING = 10
SPENT_REMAINING = 3
# Never trust a Retry-After longer than this (the daily limit is handled as an error instead).
MAX_PAUSE_SECONDS = 60.0

try:  # POSIX only; on other systems the file is not shared and each process paces alone.
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None  # type: ignore[assignment]


class DailyLimitReached(Exception):
    """HubSpot's daily request limit for the account is spent."""


class RateLimiter:
    def __init__(
        self,
        path: Optional[str] = None,
        *,
        general_per_10s: float = GENERAL_PER_10S,
        search_per_second: float = SEARCH_PER_SECOND,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.path = path if path and fcntl is not None else None
        self.general_per_10s = max(1, int(general_per_10s))
        self.search_per_second = max(1, int(search_per_second))
        self.clock = clock
        self.sleep = sleep
        self._lock = threading.Lock()
        self._local: dict = {}
        # What this process waited and sent (stats).
        self.waited = 0.0
        self.requests = 0

    # --- the shared windows -------------------------------------------------------------

    def limits(self, slots: Mapping) -> Dict[str, Tuple[int, float]]:
        """kind -> (requests, window seconds) in force."""
        general = self.general_per_10s
        account_max = slots.get("account_max")
        if account_max:
            general = max(1, min(general, int(account_max)))
        return {
            "general": (general, GENERAL_WINDOW + WINDOW_MARGIN["general"]),
            "search": (self.search_per_second, SEARCH_WINDOW + WINDOW_MARGIN["search"]),
        }

    def _update(self, change: Callable[[dict], object]):
        """Run change(slots) under the lock (thread, and file when shared); returns its result."""
        with self._lock:
            if self.path is None:
                return change(self._local)
            fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX)
                raw = b""
                while True:
                    chunk = os.pread(fd, 65536, len(raw))
                    if not chunk:
                        break
                    raw += chunk
                try:
                    slots = json.loads(raw or b"{}")
                    if not isinstance(slots, dict):
                        slots = {}
                except ValueError:
                    slots = {}
                result = change(slots)
                data = json.dumps(slots).encode()
                os.ftruncate(fd, 0)
                os.pwrite(fd, data, 0)
                return result
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
                os.close(fd)

    def acquire(self, kind: str = "general") -> float:
        """Reserve the next start for this kind (search also takes a general one) and wait for
        it. Returns the wait in seconds."""
        kinds = ("search", "general") if kind == "search" else ("general",)

        def reserve(slots: dict) -> float:
            now = self.clock()
            limits = self.limits(slots)
            start = now
            for k in kinds:
                limit, window = limits[k]
                starts = _times(slots.get(k))
                if len(starts) >= limit:
                    start = max(start, starts[-limit] + window)
                start = max(start, float(slots.get(f"{k}_paused_until") or 0.0))
            for k in kinds:
                limit, window = limits[k]
                starts = _times(slots.get(k))
                bisect.insort(starts, start)
                # Only the last `limit` starts decide the next one.
                slots[k] = starts[-limit:]
            return start - now

        wait = self._update(reserve)
        self.requests += 1
        if wait > 0:
            self.waited += wait
            self.sleep(wait)
        return wait

    def pause(self, seconds: float, kinds=KINDS) -> None:
        """No request of these kinds starts for `seconds` (every process sharing the file)."""
        seconds = min(max(float(seconds), 0.0), MAX_PAUSE_SECONDS)

        def push(slots: dict) -> float:
            until = self.clock() + seconds
            for k in kinds:
                slots[f"{k}_paused_until"] = max(float(slots.get(f"{k}_paused_until") or 0.0), until)
            return until

        self._update(push)

    # --- what HubSpot says --------------------------------------------------------------

    def observe(self, headers: Mapping[str, str]) -> None:
        """Adapt to X-HubSpot-RateLimit-* on a successful response."""
        check_daily(headers)
        account_max = _int(headers.get("X-HubSpot-RateLimit-Max"))
        remaining = _int(headers.get("X-HubSpot-RateLimit-Remaining"))
        window_ms = _int(headers.get("X-HubSpot-RateLimit-Interval-Milliseconds")) or 10000
        if account_max:

            def remember(slots: dict) -> float:
                slots["account_max"] = int(account_max)
                return 0.0

            self._update(remember)
        if remaining is None:
            return
        if remaining <= SPENT_REMAINING:
            self.pause(window_ms / 2000.0, kinds=("general",))
        elif remaining <= LOW_REMAINING:
            self.pause(0.5, kinds=("general",))

    def wait_for_429(self, kind: str, headers: Mapping[str, str], policy: Optional[str]) -> float:
        """Pause after a 429: Retry-After, else the policy's window. Returns the pause."""
        if policy == "DAILY":
            raise DailyLimitReached(daily_message(headers))
        retry_after = _float(headers.get("Retry-After"))
        if retry_after is not None:
            seconds = retry_after
        elif policy == "SECONDLY" or kind == "search":
            seconds = 1.0
        else:
            window_ms = _int(headers.get("X-HubSpot-RateLimit-Interval-Milliseconds")) or 10000
            seconds = window_ms / 2000.0
        seconds = min(max(seconds, 0.5), MAX_PAUSE_SECONDS)
        self.pause(seconds, kinds=KINDS if kind == "search" else ("general",))
        return seconds


def check_daily(headers: Mapping[str, str]) -> None:
    """Raise DailyLimitReached when HubSpot says the account's daily limit is spent."""
    remaining = _int(headers.get("X-HubSpot-RateLimit-Daily-Remaining"))
    if remaining is not None and remaining <= 0 and _int(headers.get("X-HubSpot-RateLimit-Daily")):
        raise DailyLimitReached(daily_message(headers))


def daily_message(headers: Mapping[str, str]) -> str:
    daily = headers.get("X-HubSpot-RateLimit-Daily")
    spent = f" ({daily} requests)" if daily else ""
    return (
        f"HubSpot's daily request limit for this account is spent{spent}. It resets at midnight "
        "in the account's time zone; the sync stops instead of retrying until then."
    )


def _times(value) -> List[float]:
    if not isinstance(value, list):
        return []
    out = []
    for item in value:
        try:
            out.append(float(item))
        except (TypeError, ValueError):
            continue
    out.sort()
    return out


def _int(value) -> Optional[int]:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _float(value) -> Optional[float]:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def request_kind(path_url: str) -> str:
    """search for the CRM search endpoints, else general."""
    path = path_url.split("?", 1)[0].rstrip("/")
    return "search" if path.endswith("/search") and "/crm/" in f"/{path.lstrip('/')}" else "general"


_LIMITERS: Dict[Optional[str], RateLimiter] = {}
_LIMITERS_LOCK = threading.Lock()


def limiter_for(config: Mapping) -> RateLimiter:
    """The process's limiter for this config (one per rate_limit_file)."""
    path = config.get("rate_limit_file") or None
    with _LIMITERS_LOCK:
        if path not in _LIMITERS:
            _LIMITERS[path] = RateLimiter(
                path,
                general_per_10s=config.get("rate_limit_per_10s") or GENERAL_PER_10S,
                search_per_second=config.get("search_rate_limit_per_second") or SEARCH_PER_SECOND,
            )
        return _LIMITERS[path]
