"""glibc allocator settings (tap_hubspot_beta.allocator) and where the list path uses them."""

import sys

import pytest

from fake_hubspot import FakeHubSpot, run_tap
from tap_hubspot_beta import allocator, client_v3
from tap_hubspot_beta.client_v3 import _without_nulls

BOOKMARK = {"bookmarks": {"contacts": {"replication_key": "updatedAt", "replication_key_value": "2024-02-01T00:00:00+00:00"}}}


def test_tune_takes_on_glibc_and_does_nothing_elsewhere():
    tuned = allocator.tune()
    assert tuned is sys.platform.startswith("linux")
    allocator.trim()  # never raises


def test_a_batch_read_page_keeps_only_set_properties():
    record = {"id": "1", "createdAt": "x", "properties": {"email": "a@example.test", "phone": None, "city": None}}
    assert _without_nulls(record) == {"id": "1", "createdAt": "x", "properties": {"email": "a@example.test"}}
    assert record["properties"]["phone"] is None  # the original is left as it was
    assert _without_nulls({"id": "2"}) == {"id": "2"}


def test_the_list_path_trims_every_few_pages(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(client_v3, "TRIM_EVERY_PAGES", 1)
    monkeypatch.setattr(allocator, "trim", lambda: calls.append(1))
    run_tap(FakeHubSpot({"contacts": 450}), tmp_path, streams=["contacts"], state=BOOKMARK, sync_strategy="list")
    assert len(calls) >= 4  # one per list page of 100


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="glibc only")
def test_wide_pages_read_ahead_on_eight_threads_leave_no_free_heap_behind():
    """The production pattern: 8 MB pages (100 records x 1,897 properties, 84% null) decoded on 8
    threads, 8 pages read ahead, consumed on the main thread. Untuned (production image, 240
    pages): the heap kept 858 MB for a peak of 380 MB in use and 112 at the end (FourthRev:
    2,213 MB arena for 45-119 in use). Tuned: the heap stays near the peak in use."""
    import json
    import random
    from concurrent.futures import ThreadPoolExecutor

    from tap_hubspot_beta import concurrency, memdiag

    assert allocator.tune()
    names = [f"p{i:04d}" for i in range(1897)]

    def body(seed):
        r = random.Random(seed)
        return json.dumps({"results": [{"id": str(i), "properties": {
            n: (None if r.random() < 0.84 else "v" * r.choice([3, 8, 20, 60, 400, 3000])) for n in names}}
            for i in range(100)]}).encode()

    bodies = [body(s) for s in range(4)]

    def read(i):
        return [_without_nulls(r) for r in json.loads(bytes(bodies[i % 4]).decode())["results"]]

    pool = ThreadPoolExecutor(8)
    peak_in_use = 0

    def pages():
        for i in range(160):
            yield pool.submit(read, i)

    for n, future in enumerate(concurrency.prefetch(pages, depth=8), 1):
        page = future.result()
        peak_in_use = max(peak_in_use, memdiag.heap()["in_use"])
        del page
        if n % client_v3.TRIM_EVERY_PAGES == 0:
            allocator.trim()
    pool.shutdown()
    heap = memdiag.heap()
    assert heap["arena"] <= 1.5 * peak_in_use + 32 * 2**20, (heap, peak_in_use)
