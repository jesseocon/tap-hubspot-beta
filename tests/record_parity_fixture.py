"""Record what a tap version gives on the fake HubSpot (tests/fake_hubspot.py), as a fixture.

The parity tests compare today's tap with the fixture recorded from the version before the list
API, association batching and pacing (v1.0.81-aria.2):

    git worktree add --detach /tmp/tap-baseline v1.0.81-aria.2
    PYTHONPATH=/tmp/tap-baseline:tests .venv/bin/python tests/record_parity_fixture.py \\
        tests/fixtures/parity_v1.0.81-aria.2.json
"""

import json
import sys
import tempfile
from collections import Counter
from pathlib import Path

import tap_hubspot_beta
from fake_hubspot import FakeHubSpot, last_state, records, run_tap

SIZES = {"contacts": 250, "companies": 130, "deals": 120}


def main(out: str) -> None:
    fake = FakeHubSpot(SIZES)
    with tempfile.TemporaryDirectory() as tmp:
        messages = run_tap(fake, Path(tmp))
    bookmarks = last_state(messages).get("bookmarks", {})
    fixture = {
        "tap": str(Path(tap_hubspot_beta.__file__).parent.parent.name),
        "sizes": SIZES,
        "records": records(messages),
        "bookmarks": {k: bookmarks.get(k, {}).get("replication_key_value") for k in ("contacts", "companies", "deals")},
        "requests": dict(Counter(f"{c['method']} {c['path']}" for c in fake.calls if "properties" not in c["path"])),
    }
    Path(out).write_text(json.dumps(fixture, indent=1, sort_keys=True) + "\n")


if __name__ == "__main__":
    main(sys.argv[1])
