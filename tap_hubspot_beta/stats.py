"""What each stream and the run did: records, requests per endpoint, latency, 429s, waits.

Every request goes through client_base._send_paced, which records it here under the stream
that sent it. At the end of each top-level stream's sync, and of the run, one summary line is
logged, plus a SYNC_SUMMARY line with the same numbers as JSON:

  contacts: 424,794 records in 12m03s; 8,512 requests (11.8/s), avg 0.31s; 0 429s; waited 4s
    for the rate limit; CPU 10m40s (88% of the wall time: bound by processing, not HubSpot)
    POST crm/v3/objects/contacts/batch/read: 4,248 requests, avg 0.71s
    GET crm/v3/objects/contacts: 4,249 requests, avg 0.12s
    ...

CPU is the process's CPU time while the stream synced (every thread). When it is near the
wall time the sync is bound by the tap's own processing; when it is low, by HubSpot.
"""

from __future__ import annotations

import json
import re
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, Optional

# Path segments that vary per call (ids) are shown as {id}, so endpoints group.
_ID_SEGMENT = re.compile(r"^\d+$")


def endpoint_key(method: str, path_url: str) -> str:
    path = (path_url or "").split("?", 1)[0].strip("/")
    parts = ["{id}" if _ID_SEGMENT.match(p) else p for p in path.split("/")]
    return f"{method.upper()} {'/'.join(parts)}"


@dataclass
class Endpoint:
    requests: int = 0
    seconds: float = 0.0
    rate_limited: int = 0
    errors: int = 0


@dataclass
class StreamStats:
    name: str
    started: float = field(default_factory=time.time)
    cpu_started: float = field(default_factory=time.process_time)
    records: int = 0
    waited: float = 0.0
    endpoints: Dict[str, Endpoint] = field(default_factory=dict)

    @property
    def requests(self) -> int:
        return sum(e.requests for e in self.endpoints.values())

    @property
    def seconds(self) -> float:
        return sum(e.seconds for e in self.endpoints.values())

    @property
    def rate_limited(self) -> int:
        return sum(e.rate_limited for e in self.endpoints.values())


class Stats:
    """Thread-safe counters for one tap process."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.streams: Dict[str, StreamStats] = {}
        # stream -> how it was read this run (search or list, and why).
        self.decisions: Dict[str, dict] = {}
        self.run_started = time.time()
        self.run_cpu_started = time.process_time()

    def _stream(self, name: str) -> StreamStats:
        stats = self.streams.get(name)
        if stats is None:
            stats = self.streams[name] = StreamStats(name)
        return stats

    def start(self, name: str) -> None:
        """A top-level stream begins (its wall and CPU clocks start)."""
        with self._lock:
            stats = self._stream(name)
            stats.started = time.time()
            stats.cpu_started = time.process_time()

    def request(self, stream: str, key: str, seconds: float, status: int, waited: float) -> None:
        with self._lock:
            stats = self._stream(stream)
            endpoint = stats.endpoints.setdefault(key, Endpoint())
            endpoint.requests += 1
            endpoint.seconds += seconds
            stats.waited += waited
            if status == 429:
                endpoint.rate_limited += 1
            elif status >= 400:
                endpoint.errors += 1

    def decision(self, stream: str, decision: dict) -> None:
        with self._lock:
            self.decisions[stream] = dict(decision)

    def average_seconds(self, path_suffix: str) -> Optional[float]:
        """Average latency so far of the endpoints ending in path_suffix, any stream."""
        with self._lock:
            requests = seconds = 0
            for s in self.streams.values():
                for key, e in s.endpoints.items():
                    if key.endswith(path_suffix):
                        requests += e.requests
                        seconds += e.seconds
            return seconds / requests if requests else None

    def records(self, stream: str, count: int) -> None:
        with self._lock:
            self._stream(stream).records += count

    # --- summaries ----------------------------------------------------------------------

    def summary(self, names, wall: float, cpu: float) -> dict:
        with self._lock:
            parts = [self.streams[n] for n in names if n in self.streams]
            endpoints: Dict[str, Endpoint] = {}
            for s in parts:
                for key, e in s.endpoints.items():
                    total = endpoints.setdefault(key, Endpoint())
                    total.requests += e.requests
                    total.seconds += e.seconds
                    total.rate_limited += e.rate_limited
                    total.errors += e.errors
            requests = sum(e.requests for e in endpoints.values())
            seconds = sum(e.seconds for e in endpoints.values())
            return {
                "records": {s.name: s.records for s in parts if s.records},
                "strategy": {n: self.decisions[n] for n in names if n in self.decisions},
                "requests": requests,
                "requests_per_second": round(requests / wall, 2) if wall > 0 else None,
                "avg_latency_seconds": round(seconds / requests, 3) if requests else None,
                "rate_limited": sum(e.rate_limited for e in endpoints.values()),
                "errors": sum(e.errors for e in endpoints.values()),
                "waited_seconds": round(sum(s.waited for s in parts), 1),
                "wall_seconds": round(wall, 1),
                "cpu_seconds": round(cpu, 1),
                "endpoints": {
                    k: {"requests": e.requests, "avg_latency_seconds": round(e.seconds / e.requests, 3) if e.requests else None,
                        "rate_limited": e.rate_limited}
                    for k, e in sorted(endpoints.items(), key=lambda kv: -kv[1].requests)
                },
            }

    def stream_summary(self, name: str, children=()) -> dict:
        stats = self.streams.get(name)
        started = stats.started if stats else self.run_started
        cpu_started = stats.cpu_started if stats else self.run_cpu_started
        return self.summary([name, *children], time.time() - started, time.process_time() - cpu_started)

    def run_summary(self) -> dict:
        return self.summary(list(self.streams), time.time() - self.run_started, time.process_time() - self.run_cpu_started)


def describe(label: str, summary: dict) -> str:
    """The human line(s) for a summary."""
    wall, cpu = summary["wall_seconds"], summary["cpu_seconds"]
    records = sum(summary["records"].values())
    share = cpu / wall if wall else 0.0
    bound = (
        "bound by the tap's processing, not HubSpot" if share >= 0.75
        else "bound by HubSpot's answers and the rate limit" if share <= 0.35
        else "partly processing, partly waiting on HubSpot"
    )
    lines = [
        f"{label}: {records:,} records in {duration(wall)}; {summary['requests']:,} requests"
        f" ({summary['requests_per_second'] or 0}/s), avg {summary['avg_latency_seconds'] or 0}s;"
        f" {summary['rate_limited']} 429s; waited {duration(summary['waited_seconds'])} for the rate limit;"
        f" CPU {duration(cpu)} ({share:.0%} of the wall time: {bound})"
    ]
    if len(summary["records"]) > 1:
        lines.append("  records: " + ", ".join(f"{k} {v:,}" for k, v in summary["records"].items()))
    for key, e in summary["endpoints"].items():
        lines.append(
            f"  {key}: {e['requests']:,} requests, avg {e['avg_latency_seconds']}s"
            + (f", {e['rate_limited']} 429s" if e["rate_limited"] else "")
        )
    return "\n".join(lines)


def summary_json(label: str, summary: dict) -> str:
    return "SYNC_SUMMARY " + json.dumps({"scope": label, **summary}, sort_keys=True)


def duration(seconds: Optional[float]) -> str:
    seconds = int(round(seconds or 0))
    hours, rest = divmod(seconds, 3600)
    minutes, secs = divmod(rest, 60)
    if hours:
        return f"{hours}h{minutes:02d}m"
    if minutes:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


_STATS: Optional[Stats] = None
_STATS_LOCK = threading.Lock()


def get() -> Stats:
    global _STATS
    with _STATS_LOCK:
        if _STATS is None:
            _STATS = Stats()
        return _STATS


def reset() -> Stats:
    """A fresh collector (each tap run; tests run the tap several times in one process)."""
    global _STATS
    with _STATS_LOCK:
        _STATS = Stats()
        return _STATS
