"""A HubSpot-wide bulk change, offline: every record of a wide object changed since the bookmark,
read by list + batch read in its own tap process, with the process's memory sampled as it runs.

HubSpot's nightly update touches every FourthRev contact and deal (430k and 458k, ~1,600
properties each). Records are made up on request from their id (nothing is held), so the memory
measured is the tap's own. The tap writes its Singer messages to a pipe that a reader drains, as
the connector does.

    python tests/bulk_change.py contacts=430000 deals=458000 --props 1600

runs one tap process per object at once and prints each one's peak RSS (MB).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import fake_hubspot  # noqa: E402
from fake_hubspot import FakeHubSpot, base_config, select  # noqa: E402

FIRST_ID = 1000
CHANGED_AT = "2026-11-18T01:00:00.000Z"
BOOKMARK = "2026-11-17T22:00:00+00:00"
FILL = "x" * 24  # a set property's value: HubSpot's are mostly short strings


class BulkHubSpot(FakeHubSpot):
    """`count` records per object, every one changed at CHANGED_AT, `props` properties each (about
    a sixth set, as on FourthRev's contacts). Pages are made on request."""

    def __init__(self, counts: Dict[str, int], props: int) -> None:
        super().__init__({})
        self.counts = counts
        self.names: Dict[str, List[str]] = {}
        for obj in counts:
            modified = fake_hubspot.MODIFIED[obj]
            base = [modified, "createdate", "hs_object_id"]
            self.names[obj] = base + [f"p{i:04d}_{obj}" for i in range(props - len(base))]
            fake_hubspot.PROPERTIES[obj] = self.names[obj]
        self.known = {obj: set(names) for obj, names in self.names.items()}
        # BULK_DATETIMES=N: the first N properties are datetimes; HubSpot sends many of them as
        # epoch milliseconds ("1700000000000"), not ISO text (FourthRev's deals: 212 datetimes).
        self.datetimes = int(os.environ.get("BULK_DATETIMES") or 0)
        for obj, names in self.names.items():
            fake_hubspot.DATETIME_PROPS.update(names[3:3 + self.datetimes])

    def send(self, session, request: requests.PreparedRequest, **_: Any) -> requests.Response:
        # As FakeHubSpot.send, without keeping every call.
        from urllib.parse import parse_qs, urlparse

        url = urlparse(request.url)
        path = url.path.strip("/")
        query = {k: v[-1] for k, v in parse_qs(url.query).items()}
        body = json.loads(request.body) if request.body and request.body[:1] in (b"{", "{") else None
        status, payload = self.route(request.method, path, query, body)
        if os.environ.get("BULK_LATENCY"):  # HubSpot's: ~0.1 s a list page, ~0.7 s a batch read
            time.sleep(0.7 if path.endswith("/batch/read") else 0.1)
        response = requests.Response()
        response.status_code = status
        response._content = json.dumps(payload).encode()
        response.headers["Content-Type"] = "application/json"
        response.request = request
        response.url = request.url
        return response

    def route(self, method: str, path: str, query: dict, body: Optional[dict]):
        parts = path.split("/")
        if path.startswith("crm/v3/objects/") and len(parts) >= 4 and parts[3] in self.counts:
            obj = parts[3]
            if method == "POST" and path.endswith("/search"):
                return 200, {"total": self.counts[obj], "results": []}
            if method == "POST" and path.endswith("/batch/read"):
                ids = [int(i["id"]) for i in body["inputs"]]
                return 200, {"status": "COMPLETE", "results": [self.record(obj, rid, body.get("properties") or []) for rid in ids]}
            if method == "GET" and len(parts) == 4:
                if query.get("archived") == "true":
                    return 200, {"results": []}
                return 200, self.list_page(obj, query)
        return super().route(method, path, query, body)

    def record(self, obj: str, rid: int, requested: List[str]) -> dict:
        modified = fake_hubspot.MODIFIED[obj]
        # Only the object's properties: the request also names record fields (updatedAt, ...),
        # which HubSpot never returns as properties.
        # Text properties only: a datetime's value is a date (HubSpot never sends it text).
        known = self.known[obj] - fake_hubspot.DATETIME_PROPS
        if os.environ.get("BULK_REAL"):
            # Real values: lengths vary per record and property (most short, a few long texts).
            def value(i):
                h = (rid * 2654435761 + i * 40503) & 0xFFFFFFFF
                n = 3 + h % 40 if h % 50 else 200 + h % 3000
                return (f"{rid}-{i}-" + "v" * n)[:n]
            props = {name: value(i) for i, name in enumerate(requested) if (rid + i) % 6 == 0 and name in known}
        else:
            props = {name: FILL for i, name in enumerate(requested) if (rid + i) % 6 == 0 and name in known}
        for i, name in enumerate(self.names[obj][3:3 + self.datetimes]):
            if name in requested and (rid + i) % 2 == 0:
                props[name] = str(1600000000000 + rid * 1000 + i)
        props.update({modified: CHANGED_AT, "createdate": "2024-01-01T00:00:00.000Z", "hs_object_id": str(rid)})
        props = {k: v for k, v in props.items() if k in requested}
        if os.environ.get("BULK_NULLS"):  # as search does: every requested property, set or not
            props = {name: props.get(name) for name in requested if name in known}
        return {"id": str(rid), "properties": props, "createdAt": "2024-01-01T00:00:00.000Z",
                "updatedAt": CHANGED_AT, "archived": False}

    def list_page(self, obj: str, query: dict) -> dict:
        limit = int(query.get("limit", 10))
        start = int(query.get("after") or FIRST_ID - 1) + 1
        end = min(start + limit, FIRST_ID + self.counts[obj])
        requested = [p for p in (query.get("properties") or "").split(",") if p]
        out: Dict[str, Any] = {"results": [self.record(obj, rid, requested) for rid in range(start, end)]}
        if end < FIRST_ID + self.counts[obj]:
            out["paging"] = {"next": {"after": str(end - 1)}}
        return out


def rss_mb(pid: int) -> float:
    """Resident memory of a process, MB (/proc on Linux, else ps)."""
    status = Path(f"/proc/{pid}/status")
    if status.exists():
        for line in status.read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024
        return 0.0
    out = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()
    return int(out) / 1024 if out else 0.0


def sync(obj: str, count: int, props: int, workdir: Path) -> None:
    """The tap's sync of one object in this process; Singer messages to stdout."""
    from tap_hubspot_beta.tap import Taphubspot

    fake = BulkHubSpot({obj: count}, props)
    config = {"sync_strategy": "list", "max_parallel_requests": 8, "omit_null_properties": True}
    # For the whole process, never undone: a batch read still in flight on the pool after a
    # failure must not reach HubSpot.
    requests.Session.send = lambda s, r, **k: fake.send(s, r, **k)
    if True:
        real = sys.stdout
        sys.stdout = open(os.devnull, "w")
        try:
            catalog = Taphubspot(config=base_config(workdir, **config)).catalog_dict
        finally:
            sys.stdout.close()
            sys.stdout = real
        catalog = select(json.loads(json.dumps(catalog)), [obj])
        state = {"bookmarks": {obj: {"replication_key": "updatedAt", "replication_key_value": BOOKMARK}}}
        Taphubspot(config=base_config(workdir, **config), catalog=catalog, state=state).sync_all()


def run(counts: Dict[str, int], props: int, every: float = 0.5) -> Dict[str, dict]:
    """One tap process per object at once; each one's records (counted) and RSS over time."""
    procs = {}
    with tempfile.TemporaryDirectory() as tmp:
        for obj, count in counts.items():
            work = Path(tmp) / obj
            work.mkdir()
            procs[obj] = subprocess.Popen(
                [sys.executable, __file__, "--sync", obj, str(count), str(props), str(work)],
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, cwd=str(HERE.parent),
            )
        results = {obj: {"records": 0, "rss": []} for obj in counts}

        def drain(obj: str) -> None:
            for line in procs[obj].stdout:
                if line.startswith(b'{"type":"RECORD"') or line.startswith(b'{"type": "RECORD"'):
                    results[obj]["records"] += 1

        readers = [threading.Thread(target=drain, args=(obj,), daemon=True) for obj in counts]
        for t in readers:
            t.start()
        started = time.monotonic()
        while any(p.poll() is None for p in procs.values()):
            for obj, p in procs.items():
                if p.poll() is None:
                    results[obj]["rss"].append((round(time.monotonic() - started, 1), rss_mb(p.pid)))
            time.sleep(every)
        for t in readers:
            t.join()
        for obj, p in procs.items():
            results[obj]["exit"] = p.returncode
            results[obj]["peak_mb"] = max((m for _, m in results[obj]["rss"]), default=0.0)
    return results


if __name__ == "__main__":  # pragma: no cover - the measurement itself
    if sys.argv[1] == "--sync":
        sync(sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), Path(sys.argv[5]))
        sys.exit(0)
    props = 1600
    counts: Dict[str, int] = {}
    args = sys.argv[1:]
    if "--props" in args:
        props = int(args[args.index("--props") + 1])
        del args[args.index("--props"): args.index("--props") + 2]
    for arg in args:
        name, n = arg.split("=")
        counts[name] = int(n)
    out = run(counts, props, every=2.0)
    for obj, r in out.items():
        trace = r["rss"]
        step = max(1, len(trace) // 12)
        print(f"{obj}: {r['records']:,} records, exit {r['exit']}, peak RSS {r['peak_mb']:.0f} MB; "
              f"RSS over time (s, MB): {trace[::step] + trace[-1:]}")
