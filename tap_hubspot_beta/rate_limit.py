"""A request pacer for one HubSpot account, shared by every tap process that syncs it.

HubSpot limits an OAuth app to about 110 requests per 10 seconds per account (190 for some
private apps), and the CRM search endpoints to about 5 requests per second, on top of that.
A 429 used to stall the sync for 30 seconds plus a backoff. Instead, requests are spaced so the
limits are not reached:

- "general": every request, at most GENERAL_PER_10S per 10 seconds (fewer if HubSpot's
  X-HubSpot-RateLimit-Max says the account's limit is lower);
- "search": crm/v3/objects/*/search, at most SEARCH_PER_SECOND per second, and it counts as a
  general request too.

Each kind keeps the time of its next free slot. With config `rate_limit_file`, those times live
in that file, locked around every read and write, so tap processes running side by side for one
account share one budget. Without it they live in this process.

A 429 or a nearly spent window (X-HubSpot-RateLimit-Remaining) pushes the next free slot out
for every process: Retry-After when HubSpot sends it, else the policy's own window.
"""

from __future__ import annotations

import json
import os
import threading
import time
from typing import Callable, Dict, Mapping, Optional

GENERAL_PER_10S = 95
SEARCH_PER_SECOND = 4
KINDS = ("general", "search")
# X-HubSpot-RateLimit-Remaining at or below these: wait a little, or most of the window.
LOW_REMAINING = 15
SPENT_REMAINING = 5
# Never trust a Retry-After longer than this (a daily limit is handled as an error instead).
MAX_PAUSE_SECONDS = 60.0

try:  # POSIX only; on other systems the file is not shared and each process paces alone.
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None  # type: ignore[assignment]


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
        self.general_per_10s = float(general_per_10s)
        self.search_per_second = float(search_per_second)
        self.clock = clock
        self.sleep = sleep
        self._lock = threading.Lock()
        self._local: Dict[str, float] = {}
        self.waited = 0.0
        self.requests = 0

    # --- shared slots -------------------------------------------------------------------

    def _interval(self, kind: str, slots: Mapping[str, float]) -> float:
        if kind == "search":
            return 1.0 / self.search_per_second
        per_10s = self.general_per_10s
        account_max = slots.get("account_max")
        if account_max:
            # Leave HubSpot's limit some room for other apps and syncs on the account.
            per_10s = min(per_10s, max(1.0, float(account_max) * 0.86))
        return 10.0 / per_10s

    def _update(self, change: Callable[[Dict[str, float]], float]) -> float:
        """Run change(slots) under the lock (file or process); returns its result."""
        with self._lock:
            if self.path is None:
                return change(self._local)
            fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX)
                raw = os.pread(fd, 4096, 0)
                try:
                    slots = {k: float(v) for k, v in json.loads(raw or b"{}").items()}
                except (ValueError, TypeError, AttributeError):
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
        """Wait for a free slot of this kind (search also takes a general one). Returns the wait."""
        kinds = ("search", "general") if kind == "search" else ("general",)

        def take(slots: Dict[str, float]) -> float:
            now = self.clock()
            start = max([now] + [slots.get(k, 0.0) for k in kinds])
            for k in kinds:
                slots[k] = start + self._interval(k, slots)
            return start - now

        wait = self._update(take)
        self.requests += 1
        if wait > 0:
            self.waited += wait
            self.sleep(wait)
        return wait

    def pause(self, seconds: float, kinds=KINDS) -> None:
        """No request of these kinds starts for `seconds` (every process sharing the file)."""
        seconds = min(max(float(seconds), 0.0), MAX_PAUSE_SECONDS)

        def push(slots: Dict[str, float]) -> float:
            until = self.clock() + seconds
            for k in kinds:
                slots[k] = max(slots.get(k, 0.0), until)
            return until

        self._update(push)

    # --- what HubSpot says --------------------------------------------------------------

    def observe(self, headers: Mapping[str, str]) -> None:
        """Adapt to X-HubSpot-RateLimit-* on a successful response."""
        account_max = _int(headers.get("X-HubSpot-RateLimit-Max"))
        remaining = _int(headers.get("X-HubSpot-RateLimit-Remaining"))
        window_ms = _int(headers.get("X-HubSpot-RateLimit-Interval-Milliseconds")) or 10000
        if account_max:

            def remember(slots: Dict[str, float]) -> float:
                slots["account_max"] = float(account_max)
                return 0.0

            self._update(remember)
        if remaining is None:
            return
        if remaining <= SPENT_REMAINING:
            self.pause(window_ms / 2000.0, kinds=("general",))
        elif remaining <= LOW_REMAINING:
            self.pause(1.0, kinds=("general",))

    def wait_for_429(self, kind: str, headers: Mapping[str, str], policy: Optional[str]) -> float:
        """Pause after a 429: Retry-After, else the policy's window. Returns the pause."""
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
