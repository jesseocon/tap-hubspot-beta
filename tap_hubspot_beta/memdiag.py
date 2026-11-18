"""Memory diagnostics (config memory_diagnostics: true): where a tap process's memory goes.

Every DIAG_SECONDS the stream writing records logs one `MEMDIAG {json}` line:

- rss_mb, and glibc's heap (mallinfo2): in_use_mb (allocated and not freed) and free_mb (freed
  but kept by the allocator). A leak grows in_use; fragmentation grows free.
- records per stream so far.
- datetime properties that went to pendulum.parse, and those it failed on, per property.
- probes: the stream's last raw record and last response body, run through each step of the
  tap's per-record work PROBE_RUNS times; bytes still in use per call after each step. A step
  that leaks shows its bytes per call: the native allocation site is in that step.

Off by default; costs nothing then.
"""

from __future__ import annotations

import copy
import ctypes
import gc
import json
import threading
import time
from collections import Counter
from typing import Any, Callable, Dict, Optional

DIAG_SECONDS = 30.0
PROBE_RUNS = 300
MB = 2**20


class _MallInfo2(ctypes.Structure):
    _fields_ = [(name, ctypes.c_size_t) for name in (
        "arena", "ordblks", "smblks", "hblks", "hblkhd", "usmblks", "fsmblks", "uordblks", "fordblks", "keepcost")]


def _libc():
    try:
        libc = ctypes.CDLL("libc.so.6")
        libc.mallinfo2.restype = _MallInfo2
        return libc
    except (OSError, AttributeError):
        return None


_LIBC = _libc()


def heap() -> Optional[Dict[str, int]]:
    """Bytes glibc has handed out and not had back (in_use), and freed but kept (free)."""
    if _LIBC is None:
        return None
    info = _LIBC.mallinfo2()
    return {"in_use": int(info.uordblks + info.hblkhd), "free": int(info.fordblks), "arena": int(info.arena)}


def rss() -> Optional[int]:
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) * 1024
    except OSError:
        return None
    return None


class MemDiag:
    def __init__(self) -> None:
        self.enabled = False
        self.next_at = 0.0
        self.lock = threading.Lock()
        self.pendulum_calls: Counter = Counter()
        self.pendulum_failures: Counter = Counter()
        self.raw: Dict[str, Any] = {}
        self.body: Dict[str, bytes] = {}
        self.want_raw: set = set()

    def configure(self, config: dict) -> None:
        self.enabled = bool(config.get("memory_diagnostics"))
        self.next_at = time.monotonic() + DIAG_SECONDS

    # --- what the tap reports as it works -----------------------------------------------

    def pendulum(self, field: str, failed: bool) -> None:
        with self.lock:
            self.pendulum_calls[field] += 1
            if failed:
                self.pendulum_failures[field] += 1

    def capture_raw(self, stream: str, row: dict) -> None:
        """Keep a copy of a raw record when the next report is near (one copy per report)."""
        if stream in self.want_raw or stream not in self.raw:
            self.raw[stream] = copy.deepcopy(row)
            self.want_raw.discard(stream)

    def capture_body(self, stream: str, content: bytes) -> None:
        self.body[stream] = content

    # --- the report -----------------------------------------------------------------------

    def due(self) -> bool:
        return self.enabled and time.monotonic() >= self.next_at

    def report(self, stream_obj: Any, records: Dict[str, int], logger: Any) -> None:
        self.next_at = time.monotonic() + DIAG_SECONDS
        name = stream_obj.name
        out: Dict[str, Any] = {"stream": name, "rss_mb": round((rss() or 0) / MB)}
        h = heap()
        if h:
            out.update({"in_use_mb": round(h["in_use"] / MB), "free_mb": round(h["free"] / MB), "arena_mb": round(h["arena"] / MB)})
        out["records"] = records
        with self.lock:
            out["pendulum_calls"] = dict(self.pendulum_calls.most_common(8))
            out["pendulum_failures"] = dict(self.pendulum_failures.most_common(8))
        out["probes"] = self.probes(stream_obj)
        self.want_raw.add(name)
        logger.info("MEMDIAG %s", json.dumps(out, separators=(",", ":"), default=str))

    def probes(self, s: Any) -> Dict[str, Any]:
        """Bytes per call still in use after each step, on this stream's last real data."""
        if heap() is None:
            return {}
        out: Dict[str, Any] = {}
        raw = self.raw.get(s.name)
        body = self.body.get(s.name)
        steps: Dict[str, Callable[[], Any]] = {}
        if body:
            from requests.compat import json as complexjson

            steps["decode_response"] = lambda: complexjson.loads(body.decode("utf-8"))
        if raw is not None:
            steps["deepcopy_raw"] = lambda: copy.deepcopy(raw)
            steps["post_process"] = lambda: s.post_process(copy.deepcopy(raw), None)
            processed = None
            try:
                processed = s.post_process(copy.deepcopy(raw), None)
            except Exception as exc:  # noqa: BLE001 - a probe must not stop the sync
                out["post_process_error"] = type(exc).__name__
            if processed is not None and hasattr(s, "parse_datetimes") and hasattr(s, "parse_properties"):
                props = s.parse_properties(copy.deepcopy(raw))
                steps["parse_datetimes"] = lambda: s.parse_datetimes(dict(props))
            if processed is not None and hasattr(s, "_conform"):
                steps["conform"] = lambda: s._conform(dict(processed))
                conformed = s._conform(dict(processed))
                import singer

                steps["format_message"] = lambda: singer.format_message(
                    singer.RecordMessage(stream=s.name, record=conformed))
        for step, fn in steps.items():
            try:
                fn()  # warm: first-call caches aren't leaks
                gc.collect()
                before = heap()["in_use"]
                for _ in range(PROBE_RUNS):
                    fn()
                gc.collect()
                out[step] = round((heap()["in_use"] - before) / PROBE_RUNS)
            except Exception as exc:  # noqa: BLE001
                out[step] = f"error {type(exc).__name__}"
        out["raw_fields"] = len(raw.get("properties", raw)) if isinstance(raw, dict) else None
        out["body_bytes"] = len(body) if body else None
        return out


DIAG = MemDiag()
