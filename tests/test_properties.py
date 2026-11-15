"""Property definitions (the properties stream), archived properties as columns
(include_archived_properties), and property history as one row per version. Offline
(tests/fake_hubspot.py)."""

from fake_hubspot import ARCHIVED_PROPERTIES, CUSTOM, OPTIONS, PROPERTIES, FakeHubSpot, run_tap

SIZES = {"contacts": 6, "companies": 3, "deals": 4}


def rows(messages, stream):
    return [m["record"] for m in messages if m.get("type") == "RECORD" and m["stream"] == stream]


def schema(messages, stream):
    return next(m for m in messages if m.get("type") == "SCHEMA" and m["stream"] == stream)


def test_properties_has_every_object_s_definitions_with_their_options(tmp_path):
    fake = FakeHubSpot(SIZES, custom=3)
    messages = run_tap(fake, tmp_path, streams=["properties"], discover_custom_objects=True)
    got = rows(messages, "properties")
    by_object = {}
    for row in got:
        by_object.setdefault(row["objectType"], set()).add(row["name"])
    assert by_object == {
        "contact": set(PROPERTIES["contacts"]) | set(ARCHIVED_PROPERTIES["contacts"]),
        "company": set(PROPERTIES["companies"]),
        "deal": set(PROPERTIES["deals"]),
        CUSTOM["name"]: set(PROPERTIES[CUSTOM["objectTypeId"]]),
    }
    assert {r["objectTypeId"] for r in got if r["objectType"] == CUSTOM["name"]} == {CUSTOM["objectTypeId"]}
    assert schema(messages, "properties")["key_properties"] == ["objectType", "name"]

    stage = next(r for r in got if r["objectType"] == "contact" and r["name"] == "lifecyclestage")
    assert stage["archived"] is False
    assert stage["modificationMetadata"]["archivable"] is True
    assert [(o["value"], o["label"], o["displayOrder"], o["hidden"]) for o in stage["options"]] == OPTIONS["lifecyclestage"]

    archived = [r for r in got if r["archived"]]
    assert [(r["objectType"], r["name"]) for r in archived] == [("contact", "old_score_category")]
    assert archived[0]["archivedAt"]

    # One call per object for live properties and one for archived ones.
    objects = ["companies", "contacts", "deals", CUSTOM["objectTypeId"]]
    calls = sorted((c["path"], c["query"].get("archived")) for c in fake.calls
                   if c["path"] in {f"crm/v3/properties/{o}" for o in objects})
    assert calls == sorted((f"crm/v3/properties/{o}", a) for o in objects for a in ("false", "true"))


def test_archived_properties_are_columns_only_when_asked_for(tmp_path):
    name = ARCHIVED_PROPERTIES["contacts"][0]
    without = run_tap(FakeHubSpot(SIZES), tmp_path, streams=["contacts"])
    assert name not in schema(without, "contacts")["schema"]["properties"]
    assert all(name not in r for r in rows(without, "contacts"))

    fake = FakeHubSpot(SIZES)
    with_archived = run_tap(fake, tmp_path, streams=["contacts"], include_archived_properties=True)
    assert name in schema(with_archived, "contacts")["schema"]["properties"]
    got = rows(with_archived, "contacts")
    assert sum(1 for r in got if r.get(name)) == 3  # every other contact has a value
    # The rows are otherwise the same.
    assert [{k: v for k, v in r.items() if k != name} for r in got] == rows(without, "contacts")
    # Asked for by name like the other properties.
    asked = [c for c in fake.calls if c["path"].startswith("crm/v3/objects/contacts")]
    assert asked and all(name in ((c["body"] or {}).get("properties") or c["query"].get("properties", "")) for c in asked)


def test_property_history_is_one_row_per_version(tmp_path):
    fake = FakeHubSpot(SIZES)
    messages = run_tap(fake, tmp_path, streams=["deals", "deals_history_properties"])
    assert schema(messages, "deals_history_properties")["key_properties"] == ["id"]
    got = rows(messages, "deals_history_properties")
    assert got
    for row in got:
        versions = row["propertiesWithHistory"]
        assert row["property_history"] == [
            {"property": name, "value": v["value"], "timestamp": v["timestamp"], "sourceType": v["sourceType"],
             "sourceId": None, "sourceLabel": None, "updatedByUserId": None}
            for name in sorted(versions) for v in versions[name]
        ]
    assert any(h["property"] == "dealstage" for r in got for h in r["property_history"])
