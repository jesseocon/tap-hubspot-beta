"""A fake HubSpot API for running the whole tap offline, on synthetic records only.

Every HTTP call the tap makes goes through requests.Session.send, which FakeHubSpot replaces.
It serves the CRM search API (filters, id sort, limit), the list API (after paging), batch read,
v4 association batch read, the properties API and empty answers for everything else. Like the
real APIs differ in places, search sends unset properties as null and list/batch read leave
them out; the tap must give identical records either way.

No real person is in here: names are "Person 7", emails person7@example.test.
"""

from __future__ import annotations

import io
import json
import random
import sys
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest import mock
from urllib.parse import parse_qs, urlparse

import requests

BASE = datetime(2024, 1, 1, tzinfo=timezone.utc)

PROPERTIES = {
    "contacts": ["email", "firstname", "lastname", "phone", "lifecyclestage", "createdate",
                 "lastmodifieddate", "hs_object_id", "notes_last_updated", "city"],
    "companies": ["name", "domain", "industry", "createdate", "hs_lastmodifieddate",
                  "hs_object_id", "annualrevenue"],
    "deals": ["dealname", "amount", "dealstage", "pipeline", "closedate", "createdate",
              "hs_lastmodifieddate", "hs_object_id"],
}
DATETIME_PROPS = {"createdate", "lastmodifieddate", "hs_lastmodifieddate", "closedate", "notes_last_updated"}
MODIFIED = {"contacts": "lastmodifieddate", "companies": "hs_lastmodifieddate", "deals": "hs_lastmodifieddate"}


def iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def ms(value: str) -> int:
    return int(datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc).timestamp() * 1000)


class FakeHubSpot:
    def __init__(self, sizes: Dict[str, int], seed: int = 7, links_per_contact: int = 3) -> None:
        rng = random.Random(seed)
        self.records: Dict[str, List[dict]] = {}
        # A fixed order, so the same sizes always give the same records.
        for obj in [o for o in ("contacts", "companies", "deals") if o in sizes]:
            count = sizes[obj]
            rows = []
            for i in range(count):
                rid = str(1000 + i * 3 + rng.randint(0, 2))
                created = BASE + timedelta(hours=rng.randint(0, 20000))
                modified = created + timedelta(minutes=rng.randint(0, 90000))
                props: Dict[str, Any] = {"hs_object_id": rid, "createdate": iso(created), MODIFIED[obj]: iso(modified)}
                for name in PROPERTIES[obj]:
                    if name in props:
                        continue
                    roll = rng.random()
                    if roll < 0.2:
                        continue  # never set
                    if name in DATETIME_PROPS:
                        props[name] = iso(created + timedelta(days=rng.randint(0, 400)))
                    elif name == "email":
                        props[name] = f"person{i}@example.test"
                    elif name in ("amount", "annualrevenue"):
                        props[name] = str(rng.randint(100, 99999))
                    else:
                        props[name] = f"{name.title()} {i}"
                rows.append({"id": rid, "properties": props, "known": set(PROPERTIES[obj]), "createdAt": iso(created),
                             "updatedAt": props[MODIFIED[obj]], "archived": False})
            # One record HubSpot never set a modified date on: search's GT filter skips it.
            if rows:
                rows[-1]["properties"].pop(MODIFIED[obj], None)
            self.records[obj] = sorted(rows, key=lambda r: int(r["id"]))
        # from type -> to type -> from id -> to ids
        self.links: Dict[str, Dict[str, Dict[str, List[str]]]] = {}
        for src, dst in (("contacts", "companies"), ("deals", "companies"), ("deals", "contacts")):
            targets = [r["id"] for r in self.records.get(dst, [])]
            table = self.links.setdefault(src, {}).setdefault(dst, {})
            for record in self.records.get(src, []):
                n = rng.randint(0, links_per_contact) if targets else 0
                table[record["id"]] = sorted({rng.choice(targets) for _ in range(n)}, key=int)
        self.calls: List[dict] = []
        self.v4_inputs: List[List[dict]] = []

    # --- HTTP -----------------------------------------------------------------------------

    def send(self, session, request: requests.PreparedRequest, **_: Any) -> requests.Response:
        url = urlparse(request.url)
        path = url.path.lstrip("/")
        query = {k: v[-1] for k, v in parse_qs(url.query).items()}
        body = json.loads(request.body) if request.body and request.body[:1] in (b"{", "{") else None
        self.calls.append({"method": request.method, "path": path, "query": query, "body": body})
        status, payload = self.route(request.method, path, query, body)
        response = requests.Response()
        response.status_code = status
        response._content = json.dumps(payload).encode()
        response.headers["Content-Type"] = "application/json"
        response.request = request
        response.url = request.url
        return response

    def route(self, method: str, path: str, query: dict, body: Optional[dict]):
        parts = path.split("/")
        if path.startswith("oauth/") or "access-tokens" in path or "token" in parts[-1]:
            return 200, {"scopes": ["crm.objects.contacts.read"], "access_token": "x", "expires_in": 3600}
        if path.startswith("properties/") or "/properties" in path:
            obj = parts[2] if path.startswith("properties/v1/") else ""
            return 200, [{"name": n, "type": "datetime" if n in DATETIME_PROPS else "string",
                          "fieldType": "text"} for n in PROPERTIES.get(obj, [])]
        if path.startswith("crm/v3/objects/") and len(parts) >= 4 and parts[3] in self.records:
            obj = parts[3]
            if method == "POST" and path.endswith("/search"):
                return 200, self.search(obj, body or {})
            if method == "POST" and path.endswith("/batch/read"):
                return 200, self.batch_read(obj, body or {})
            if method == "GET" and len(parts) == 4:
                if query.get("archived") == "true":
                    return 200, {"results": []}
                return 200, self.list(obj, query)
        if path.startswith("crm/v4/associations/") and path.endswith("/batch/read"):
            assert len(body["inputs"]) <= 1000, "v4 batch read takes at most 1,000 ids"
            self.v4_inputs.append(body["inputs"])
            return 207, self.associations(parts[3], parts[4], body["inputs"])
        if path == "deals/v1/deal/paged":
            return 200, self.deals_v1(query)
        if path.startswith("crm/v4/associations/"):
            return 200, {"results": []}
        return 200, {"results": []}

    @staticmethod
    def _props(record: dict, requested: List[str], nulls: bool) -> dict:
        props = record["properties"]
        known = record["known"]
        # Names that are not properties of the object (id, updatedAt, ...) are never returned.
        if nulls:
            return {name: props.get(name) for name in requested if name in known}
        return {name: props[name] for name in requested if name in props}

    def _shape(self, record: dict, requested: List[str], nulls: bool) -> dict:
        out = {k: record[k] for k in ("id", "createdAt", "updatedAt", "archived")}
        out["properties"] = self._props(record, requested, nulls)
        return out

    def search(self, obj: str, body: dict) -> dict:
        rows = self.records[obj]
        for f in body.get("filters", []):
            name, op = f["propertyName"], f["operator"]
            if name == "hs_object_id" and op == "GT":
                rows = [r for r in rows if int(r["id"]) > int(f["value"])]
            elif name == "hs_object_id" and op == "IN":
                rows = [r for r in rows if r["id"] in f["values"]]
            elif op in ("GT", "LTE"):
                def keep(r, f=f, name=name, op=op):
                    v = r["properties"].get(name)
                    if v is None:
                        return False
                    return ms(v) > int(f["value"]) if op == "GT" else ms(v) <= int(f["value"])
                rows = [r for r in rows if keep(r)]
        limit = int(body.get("limit", 10))
        page = rows[:limit]
        out = {"total": len(rows), "results": [self._shape(r, body.get("properties") or [], True) for r in page]}
        if len(rows) > limit:
            out["paging"] = {"next": {"after": str(limit)}}
        return out

    def list(self, obj: str, query: dict) -> dict:
        assert int(query.get("limit", 10)) <= 100, "the list API takes at most 100"
        requested = [p for p in (query.get("properties") or "").split(",") if p]
        rows = self.records[obj]
        after = query.get("after")
        if after:
            rows = [r for r in rows if int(r["id"]) > int(after)]
        limit = int(query.get("limit", 10))
        page = rows[:limit]
        out: Dict[str, Any] = {"results": [{**self._shape(r, requested, False), "url": "https://app.example.test/x"} for r in page]}
        if len(rows) > limit:
            out["paging"] = {"next": {"after": page[-1]["id"], "link": "https://example.test/next"}}
        return out

    def batch_read(self, obj: str, body: dict) -> dict:
        assert len(body["inputs"]) <= 100, "batch read takes at most 100 ids"
        wanted = {i["id"] for i in body["inputs"]}
        rows = [r for r in self.records[obj] if r["id"] in wanted]
        rows.reverse()  # HubSpot gives no order
        return {"status": "COMPLETE", "results": [self._shape(r, body.get("properties") or [], False) for r in rows]}

    def deals_v1(self, query: dict) -> dict:
        offset = int(query.get("offset") or 0)
        count = int(query.get("count") or 100)
        rows = self.records.get("deals", [])
        page = rows[offset:offset + count]
        return {"deals": [{"dealId": int(r["id"]), "properties": {}} for r in page],
                "hasMore": offset + count < len(rows), "offset": offset + count}

    def associations(self, src: str, dst: str, inputs: List[dict]) -> dict:
        results = []
        table = self.links.get(src, {}).get(dst, {})
        for item in inputs:
            to = table.get(str(item["id"])) or []
            if to:
                results.append({"from": {"id": item["id"]}, "to": [
                    {"toObjectId": int(c), "associationTypes": [
                        {"category": "HUBSPOT_DEFINED", "typeId": 279, "label": None}]} for c in to]})
        return {"status": "COMPLETE", "results": results}


# --- running the tap ------------------------------------------------------------------------

STREAMS = [
    "contacts",
    "companies",
    "deals",
    "associations_contacts_companies",
    "associations_deals_companies",
    "associations_deals_contacts",
]


def base_config(tmp: Path, **extra: Any) -> List[str]:
    config = {
        "client_id": "test-client",
        "client_secret": "test-secret",
        "redirect_uri": "https://localhost/unused",
        "refresh_token": "test-refresh",
        "access_token": "test-access",
        "expires_in": 4102444800,
        "start_date": "2024-02-01T00:00:00Z",
        "use_legacy_streams": False,
        "discover_custom_objects": False,
        "rate_limit_per_10s": 100000,
        "search_rate_limit_per_second": 100000,
        **extra,
    }
    path = tmp / f"config-{len(list(tmp.iterdir()))}.json"
    path.write_text(json.dumps(config))
    return [str(path)]


def select(catalog: dict, streams: List[str]) -> dict:
    for entry in catalog["streams"]:
        on = entry["tap_stream_id"] in streams
        for meta in entry.get("metadata", []):
            meta.setdefault("metadata", {})["selected"] = on
    return catalog


def run_tap(fake: FakeHubSpot, tmp: Path, streams=STREAMS, state: Optional[dict] = None, **config: Any):
    """Discover, select the streams with every field, sync. Returns the Singer messages."""
    from tap_hubspot_beta.tap import Taphubspot

    def send(session, request, **kwargs):
        return fake.send(session, request, **kwargs)

    def fresh():
        # The tap keeps some state on the class (it runs once per process); a test runs the
        # tap several times in one process, as discovery and sync are separate processes.
        Taphubspot.legacy_streams_mapping = {}
        Taphubspot.associations_metadata = {}
        Taphubspot.custom_objects_streams = set()
        Taphubspot._oauth_scopes_cache = None

    with mock.patch.object(requests.Session, "send", send):
        out = io.StringIO()
        fresh()
        with redirect_stdout(out):
            catalog = Taphubspot(config=base_config(tmp, **config)).catalog_dict
        catalog = select(json.loads(json.dumps(catalog)), streams)
        fake.calls.clear()
        out = io.StringIO()
        fresh()
        with redirect_stdout(out):
            tap = Taphubspot(config=base_config(tmp, **config), catalog=catalog, state=state or {})
            tap.sync_all()
    messages = [json.loads(line) for line in out.getvalue().splitlines() if line.startswith("{")]
    return messages


def records(messages: List[dict]) -> Dict[str, List[dict]]:
    """RECORD messages per stream, sorted by key, without the extraction time."""
    out: Dict[str, List[dict]] = {}
    for m in messages:
        if m.get("type") == "RECORD":
            out.setdefault(m["stream"], []).append(m["record"])
    return {s: sorted(rows, key=lambda r: json.dumps(r, sort_keys=True)) for s, rows in out.items()}


def last_state(messages: List[dict]) -> dict:
    states = [m["value"] for m in messages if m.get("type") == "STATE"]
    return states[-1] if states else {}


if __name__ == "__main__":  # pragma: no cover - a manual look
    import tempfile

    with tempfile.TemporaryDirectory() as d:
        msgs = run_tap(FakeHubSpot({"contacts": 5, "companies": 3, "deals": 2}), Path(d))
        json.dump(records(msgs), sys.stdout, indent=1)
