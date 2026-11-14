"""Requests in parallel, Singer output in order.

The Meltano SDK writes one stream's records at a time and is not thread-safe (stream state,
stdout). So only HTTP runs on other threads; every RECORD and STATE message is still written by
the main thread, in the order the sequential tap wrote them:

- prefetch(): a stream's pages are read on a background thread, a few ahead of the records
  being written (search and list paging is a cursor, so pages of one stream are sequential;
  what overlaps is HubSpot's answer for the next page with the processing of this one).
- pool(): batch reads (100 records each) and child reads (association and history batches)
  run on a shared pool of worker threads, max_parallel_requests at once. Results are taken in
  the order they were asked for.

All of them take their turn from the account's request budget (tap_hubspot_beta.rate_limit),
so more threads never mean more requests than HubSpot allows; they mean less time idle while a
request is in flight.

Each thread sends through its own requests.Session (session()).
"""

from __future__ import annotations

import queue
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Callable, Dict, Iterable, Iterator, Mapping, Optional, TypeVar

import requests

T = TypeVar("T")

DEFAULT_MAX_PARALLEL_REQUESTS = 8
# Pages a stream reads ahead of the records it writes.
PREFETCH_PAGES = 2

_local = threading.local()


def session() -> requests.Session:
    """This thread's HTTP session."""
    s = getattr(_local, "session", None)
    if s is None:
        s = _local.session = requests.Session()
    return s


def max_parallel(config: Mapping) -> int:
    """Requests in flight at once in this process (config max_parallel_requests; 1: none)."""
    value = config.get("max_parallel_requests")
    if value is None:
        return DEFAULT_MAX_PARALLEL_REQUESTS
    try:
        return max(1, int(value))
    except (TypeError, ValueError):
        return DEFAULT_MAX_PARALLEL_REQUESTS


_POOLS: Dict[int, ThreadPoolExecutor] = {}
_POOLS_LOCK = threading.Lock()


def pool(config: Mapping) -> Optional[ThreadPoolExecutor]:
    """The process's worker pool, or None when requests run one at a time."""
    size = max_parallel(config)
    if size <= 1:
        return None
    with _POOLS_LOCK:
        if size not in _POOLS:
            _POOLS[size] = ThreadPoolExecutor(max_workers=size, thread_name_prefix="hubspot-request")
        return _POOLS[size]


def done(value: T) -> "Future[T]":
    future: Future = Future()
    future.set_result(value)
    return future


_END = object()


class _Failure:
    def __init__(self, exc: BaseException) -> None:
        self.exc = exc


def prefetch(make: Callable[[], Iterable[T]], depth: int = PREFETCH_PAGES, enabled: bool = True) -> Iterator[T]:
    """Iterate make() on a background thread, up to `depth` items ahead of the consumer.

    Items come out in order; an exception in the producer is raised in the consumer at the
    point it happened. When the consumer stops early, the producer stops at its next item.
    """
    if not enabled or depth < 1:
        yield from make()
        return
    items: "queue.Queue" = queue.Queue(maxsize=depth)
    stop = threading.Event()

    def put(item) -> bool:
        while not stop.is_set():
            try:
                items.put(item, timeout=0.2)
                return True
            except queue.Full:
                continue
        return False

    def produce() -> None:
        try:
            for item in make():
                if not put(item):
                    return
        except BaseException as exc:  # handed to the consumer, raised there
            put(_Failure(exc))
            return
        put(_END)

    thread = threading.Thread(target=produce, name="hubspot-prefetch", daemon=True)
    thread.start()
    try:
        while True:
            item = items.get()
            if item is _END:
                return
            if isinstance(item, _Failure):
                raise item.exc
            yield item
    finally:
        stop.set()
