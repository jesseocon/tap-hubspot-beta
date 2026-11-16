"""REST client handling, including hubspotStream base class."""

from typing import Any, Dict, Iterable, Optional, List
import copy
import json
import math

import requests
from hotglue_singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_hubspot_beta import concurrency
from tap_hubspot_beta.client_base import ChildPipeline, hubspotStream
from pendulum import parse
from datetime import datetime, timedelta, timezone
from hotglue_singer_sdk import typing as th
import singer


from hotglue_singer_sdk.exceptions import InvalidStreamSortException
from hotglue_singer_sdk.helpers._state import (
    finalize_state_progress_markers,
    log_sort_error
)


class hubspotV3SearchStream(hubspotStream):
    """hubspot stream class."""

    rest_method = "POST"

    records_jsonpath = "$.results[*]"
    next_page_token_jsonpath = "$.paging.next.after"
    filter = None
    starting_time = None
    page_size = 200
    special_replication = False
    previous_starting_time = None
    max_dates = []
    starting_times = []
    bulk_child = True
    query_end_time = None
    max_bucket_size = 5000
    buckets = None
    current_bucket = None

    def get_starting_time(self, context):
        start_date = self.get_starting_timestamp(context)
        if start_date:
            starting_time = int(start_date.timestamp() * 1000)
            # Re-read a short window before a saved bookmark: HubSpot's search
            # index can lag, so a record updated just before the last run's
            # end may only become searchable after the bookmark passed it.
            # The overlap re-emits a few records; downstream merges on id.
            if self.get_starting_replication_key_value(context) is not None:
                starting_time -= self.lookback_ms
            return starting_time
        return int(datetime(2000, 1, 1).timestamp() * 1000)

    @property
    def lookback_ms(self) -> int:
        """Lookback before the bookmark, from config `lookback_minutes` (default 60)."""
        minutes = self._tap.config.get("lookback_minutes")
        if minutes is None:
            minutes = 60
        return max(int(minutes), 0) * 60 * 1000

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        if not self.current_bucket:
            return None

        # Id-based cursor pagination: ignore HubSpot's numeric offset and advance
        # the cursor using the last record's hs_object_id instead.
        results = response.json().get("results", [])
        if results:
            new_last_record_id = results[-1]["properties"]["hs_object_id"]
            if self.current_bucket["last_record_id"] == new_last_record_id:
                self.logger.warning("Bucket pagination loop detected. Skipping to next bucket")
                next_page_token = None
            else:
                self.current_bucket["last_record_id"] = new_last_record_id
                # If the page wasn't full we know this is the last page
                if len(results) < self.page_size:
                    next_page_token = None
                else:
                    next_page_token = "-1" if previous_token == "0" else "0"
        else:
            next_page_token = None

        if not next_page_token:
            if self.buckets and len(self.buckets) > 0:
                self.logger.info(f"Popping bucket {self.current_bucket}")
                self.current_bucket = self.buckets.pop(0)
                self.logger.info(f"Beginning syncing of next bucket {self.current_bucket}")
                next_page_token = "-1" if previous_token == "0" else "0"
            else:
                self.logger.warning("No more buckets to process")

        return next_page_token
    
    def get_end_time(self, curr_date_fallback=True):
        # Maintain consistent end_time within stream syncs
        if self.query_end_time and curr_date_fallback:
            return self.query_end_time
        end_date = self._tap.config.get("end_date")
        if end_date:
            return int(parse(end_date).timestamp() * 1000)
        if curr_date_fallback:
            end_date = datetime.now() - timedelta(seconds=15)
            self.query_end_time = int(end_date.timestamp() * 1000)
            return int(end_date.timestamp() * 1000)
        else:
            return None

    def prepare_bucket_request_payload(self, starting_time: int, end_time):
        payload = {}
        payload["limit"] = 1
        payload["filters"] = []
        payload["properties"] = []
        if self.filter:
            payload["filters"].append(self.filter)
        if self.replication_key and starting_time or self.special_replication:
            payload["filters"].append({
                "propertyName": self.replication_key_filter,
                "operator": "GT",
                "value": starting_time,
            })
            if end_time:
                payload["filters"].append({
                    "propertyName": self.replication_key_filter,
                    "operator": "LTE",
                    "value": end_time
                })
            payload["sorts"] = [{
                "propertyName": self.replication_key_filter,
                "direction": "ASCENDING"
            }]

        return payload

    def prepare_bucket_request(
        self, context, request_data
    ) -> requests.PreparedRequest:
        http_method = self.rest_method
        url: str = self.get_url(context)
        params: dict = self.get_url_params(context, next_page_token=None)
        headers = self.http_headers

        request = requests.Request(
            method=http_method,
            url=url,
            params=params,
            headers=headers,
            json=request_data
        )

        return self.requests_session.prepare_request(request)

    def get_time_bucket_size(self, context, starting_time: int, end_time):
        decorated_request = self.request_decorator(self._request)
        request_data = self.prepare_bucket_request_payload(starting_time, end_time)
        prepared_request = self.prepare_bucket_request(context, request_data)
        response = decorated_request(prepared_request, context).json()

        return response.get('total')

    def get_estimated_record_count(self) -> Optional[int]:
        """Return HubSpot's exact search total for this sync's time window."""
        try:
            return self.get_time_bucket_size(
                context=None,
                starting_time=self.starting_time or self.get_starting_time(context=None),
                end_time=self.get_end_time(),
            )
        except Exception as exc:
            self.logger.warning(
                "Failed to collect pre-sync search total for stream '%s': %s",
                self.name,
                exc,
            )
            return None

    def split_time_bucket(self, context, starting_time: int, end_time: int):
        buckets = []
        time_diff = end_time - starting_time

        if time_diff == 1:
            bucket_size = self.get_time_bucket_size(context, starting_time, end_time)
            self.logger.info(f"Cannot split bucket anymore. Bucket size is {bucket_size}")
            return [{"starting_time": starting_time, "end_time": end_time, "bucket_size": bucket_size}]

        mid_time = end_time - int(time_diff / 2)
        bucket_1_size = self.get_time_bucket_size(context, starting_time, mid_time)
        bucket_2_size = self.get_time_bucket_size(context, mid_time, end_time)

        if bucket_1_size > self.max_bucket_size:
            self.logger.info(f"Bucket size is {bucket_1_size}, splitting into multiple buckets")
            buckets += self.split_time_bucket(context, starting_time, mid_time)
        elif bucket_1_size > 0:
            buckets += [{"starting_time": starting_time, "end_time": mid_time, "bucket_size": bucket_1_size}]

        if bucket_2_size > self.max_bucket_size:
            self.logger.info(f"Bucket size is {bucket_2_size}, splitting into multiple buckets")
            buckets += self.split_time_bucket(context, mid_time, end_time)
        elif bucket_2_size > 0:
            buckets += [{"starting_time": mid_time, "end_time": end_time, "bucket_size": bucket_2_size}]

        return buckets

    def merge_small_adjacent_buckets(self, buckets: List[dict]):
        bucket_index = 0
        while bucket_index < (len(buckets) - 1):
            bucket_1 = buckets[bucket_index]
            bucket_2 = buckets[bucket_index + 1]
            if (bucket_1["bucket_size"] + bucket_2["bucket_size"]) <= self.max_bucket_size:
                bucket_1["end_time"] = bucket_2["end_time"]
                bucket_1["bucket_size"] += bucket_2["bucket_size"]
                buckets.pop(bucket_index + 1)
            else:
                bucket_index += 1

    def create_time_buckets(self, context: Optional[dict]):
        self.logger.info(f"Creating time buckets for stream = {self.name}")
        starting_time = self.starting_time or self.get_starting_time(context)
        end_time = self.get_end_time()
        buckets = []
        bucket_size = self.get_time_bucket_size(context, starting_time, end_time)

        self.logger.info(f"Total records to fetch for stream = {self.name}: {bucket_size}")

        if bucket_size > self.max_bucket_size:
            self.logger.info(f"Bucket size is greater than {self.max_bucket_size}, splitting into multiple buckets")
            buckets += self.split_time_bucket(context, starting_time, end_time)
        else:
            self.logger.info(f"Bucket size is less than {self.max_bucket_size}, creating single bucket")
            buckets = [{"starting_time": starting_time, "end_time": end_time, "bucket_size": bucket_size}]

        self.logger.info("Merging small adjacent buckets")
        self.merge_small_adjacent_buckets(buckets)

        for bucket in buckets:
            bucket["last_record_id"] = "0"
        # The last bucket has no end (unless the config sets end_date): a record changed while
        # the sync runs has left the bucket it was counted in, and its new time is after the
        # end the buckets were cut at. With an end it was in no bucket and missed by this run
        # (and by the next too, if it changed again while that one ran). Open, it is read in
        # the last bucket, which is paged last. The bookmark stays at the sync's start (the
        # signpost), so the next run re-reads from there.
        if buckets and self.get_end_time(curr_date_fallback=False) is None:
            buckets[-1]["end_time"] = None

        self.logger.info(f"Total buckets created: {len(buckets)} with total records: {sum([b['bucket_size'] for b in buckets])}")
        self.logger.info(buckets)

        return buckets

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        """Prepare the data payload for the REST API request."""
        if self.buckets is None:
            self.buckets = self.create_time_buckets(context)
            self.current_bucket = self.buckets.pop(0)

        payload = {}
        payload["limit"] = self.page_size
        payload["filters"] = []
        starting_time = self.starting_time or self.get_starting_time(context)
        if self.filter:
            payload["filters"].append(self.filter)
        if next_page_token and next_page_token not in ["0", "-1"]:
            payload["after"] = next_page_token
        if self.replication_key and starting_time or self.special_replication:
            payload["filters"].append({
                "propertyName": self.replication_key_filter,
                "operator": "GT",
                "value": self.current_bucket["starting_time"],
            })
            if self.current_bucket["end_time"] is not None:
                payload["filters"].append({
                    "propertyName": self.replication_key_filter,
                    "operator": "LTE",
                    "value": self.current_bucket["end_time"]
                })
        payload["filters"].append({
            "propertyName": "hs_object_id",
            "operator": "GT",
            "value": self.current_bucket["last_record_id"]
        })
        payload["sorts"] = [{
            "propertyName": "hs_object_id",
            "direction": "ASCENDING"
        }]
        if self.properties_url:
            if self.name == "deals_association_parent":
                payload["properties"] = ["id"]
            else:
                payload["properties"] = self.selected_properties
        else:
            payload["properties"] = []
        if context and context.get("object_id_filters"):
            payload["filters"].append({
                "propertyName": "hs_object_id",
                "operator": "IN",
                "values": context["object_id_filters"]
            })
        return payload


    @property
    def use_list_filtering(self):
        config_field_name = self._list_id_config_mapping.get(self.name)
        return config_field_name and config_field_name in self._tap.config and self._tap.config.get(config_field_name)

    def get_paging_windows(self, context):
        if self.use_list_filtering:
            list_ids = self._tap.config.get(self._list_id_config_mapping[self.name])
            if list_ids:
                object_ids = list(self.fetch_list_memberships(list_ids))
                # Partition into groups of 100
                return [{"object_id_filters": object_ids[i:i+100]} for i in range(0, len(object_ids), 100)] or [{"object_id_filters": []}]
        return []


    # --- search or list, per stream and run ----------------------------------------------
    #
    # The search API reads records changed in a window, 200 a page, at HubSpot's search limit
    # (5 requests a second per account), in windows of at most 10,000 results (the time buckets
    # above, each costing count requests). The list API (GET crm/v3/objects/{type}) pages
    # through every record by id at the general limit, with no window. Its records are the
    # search API's: the same object shape, the same properties (every selected one is asked for),
    # the same window on the replication property, applied here instead of by HubSpot. When the
    # property names don't fit in a URL, the list call reads only ids and the window's property,
    # and the records in the window are read in full with the batch read API, several batches
    # at once (tap_hubspot_beta.concurrency).
    #
    # Which one, decided once per stream and run (config sync_strategy, default auto):
    # - no bookmark (a first sync, or the runner's full refresh, which starts with no state): list;
    # - auto with a bookmark: search counts the records changed since (bookmark - lookback) and
    #   all records (two search calls, one result each); the faster one by the estimate below
    #   is used. Under auto_list_min_changed changed records, search without estimating more.
    # - search / list: that one (list still applies the window, so the records are the same).
    # The decision and its numbers are logged and kept in the run's SYNC_SUMMARY.

    # Top-level keys of a search result; anything else the list API adds is dropped.
    SEARCH_RESULT_KEYS = ("id", "properties", "createdAt", "updatedAt", "archived", "archivedAt")
    LIST_PAGE_SIZE = 100  # the list and batch read APIs' maximum
    MAX_LIST_URL_LENGTH = 7000
    AUTO_LIST_MIN_CHANGED = 5000
    # Seconds per call, for the estimate before any call of the kind was timed in this run
    # (Design Center, 2026-11-14: list ids 0.13s, batch read of 1,850 properties 0.75s).
    DEFAULT_SECONDS = {"search": 0.6, "list": 0.15, "list_full": 0.4, "batch_read": 0.75}

    @property
    def hubspot_property_names(self) -> List[str]:
        """The selected HubSpot properties, less the stream's own fields (id, createdAt,
        updatedAt, archived, ...), which the request names too but HubSpot never returns
        as properties."""
        cached = getattr(self, "_hubspot_property_names", None)
        if cached is None:
            base = self.base_properties if isinstance(self.base_properties, list) else []
            own = {getattr(p, "name", None) for p in base} | set(self.SEARCH_RESULT_KEYS) | {"_hg_archived"}
            cached = [name for name in self.selected_properties if name not in own]
            self._hubspot_property_names = cached
        return cached

    def _has_bookmark(self, context: Optional[dict]) -> bool:
        state = self.get_context_state(context)
        value = state.get("replication_key_value") or state.get("progress_markers", {}).get("replication_key_value")
        return bool(value)

    @property
    def sync_strategy(self) -> str:
        """auto, search or list (config sync_strategy; use_list_api_for_full_sync=false: search)."""
        value = str(self.config.get("sync_strategy") or "").strip().lower()
        if value in ("auto", "search", "list"):
            return value
        if self.config.get("use_list_api_for_full_sync", True) is False:
            return "search"
        return "auto"

    def list_api_supported(self, context: Optional[dict]) -> bool:
        """A CRM object stream that search reads without extra filters."""
        if not self.properties_url or not (self.path or "").startswith("crm/v3/objects/"):
            return False
        if not self.path.endswith("/search") or self.filter or self.special_replication:
            return False
        if self.use_list_filtering or (context and context.get("object_id_filters") is not None):
            return False
        return bool(self.replication_key and getattr(self, "replication_key_filter", None))

    def use_list_api(self, context: Optional[dict]) -> bool:
        """This run reads the stream through the list API (decided once per context)."""
        decisions = self.__dict__.setdefault("_strategy_decisions", {})
        key = json.dumps(context or {}, sort_keys=True, default=str)
        if key not in decisions:
            decisions[key] = self.decide_strategy(context)
        return decisions[key]["strategy"] == "list"

    def decide_strategy(self, context: Optional[dict]) -> dict:
        if not self.list_api_supported(context):
            return {"strategy": "search", "reason": "the only way this stream is read"}
        configured = self.sync_strategy
        if configured == "search":
            return self._decided({"strategy": "search", "reason": "sync_strategy search"})
        if not self._has_bookmark(context):
            return self._decided({"strategy": "list", "reason": "no bookmark (a first sync or a full refresh)"})
        if configured == "list":
            return self._decided({"strategy": "list", "reason": "sync_strategy list"})
        start, end = self._window_ms(context)
        changed = self.get_time_bucket_size(context, start, end)
        decision = {"changed": changed, "since": _iso_ms(start)}
        minimum = int(self.config.get("auto_list_min_changed", self.AUTO_LIST_MIN_CHANGED))
        if changed < minimum:
            return self._decided({**decision, "strategy": "search", "reason": f"under {minimum:,} changed"})
        total = self.get_time_bucket_size(context, None, None)
        estimate = self.estimate_seconds(changed, total)
        strategy = "list" if estimate["list"] < estimate["search"] else "search"
        return self._decided({**decision, "total": total, "strategy": strategy, "estimate_seconds": estimate,
                              "reason": "faster by the estimate"})

    def _decided(self, decision: dict) -> dict:
        from tap_hubspot_beta import stats as sync_stats

        if "changed" in decision and "total" in decision:
            est = decision["estimate_seconds"]
            other = "search" if decision["strategy"] == "list" else "list"
            line = (
                f"{self.name}: {decision['changed']:,} changed since {decision['since']} of {decision['total']:,}"
                f" -> {decision['strategy']} (est. {sync_stats.duration(est[decision['strategy']])} vs"
                f" {sync_stats.duration(est[other])} by {other})"
            )
        elif "changed" in decision:
            line = f"{self.name}: {decision['changed']:,} changed since {decision['since']} -> {decision['strategy']} ({decision['reason']})"
        else:
            line = f"{self.name}: {decision['strategy']} ({decision['reason']})"
        self.logger.info(line)
        sync_stats.get().decision(self.name, decision)
        return decision

    def _seconds_per_call(self, kind: str) -> float:
        """The average time of this kind of call so far in the run, else the default."""
        from tap_hubspot_beta import stats as sync_stats

        suffix = {"search": f"{self.object_path}/search", "batch_read": f"{self.object_path}/batch/read"}.get(kind)
        if suffix:
            measured = sync_stats.get().average_seconds(suffix)
            if measured:
                return measured
        return self.DEFAULT_SECONDS[kind]

    def estimate_seconds(self, changed: int, total: int) -> dict:
        """Rough seconds to read `changed` of `total` records by search and by list.

        Pages of one stream are sequential (each waits for the last one's cursor); batch reads
        run side by side; both are capped by the account's request budget."""
        limits = self.rate_limiter.limits({})
        general_per_s = limits["general"][0] / limits["general"][1]
        search_per_s = limits["search"][0] / limits["search"][1]

        pages = math.ceil(changed / self.page_size)
        buckets = math.ceil(changed / self.max_bucket_size) if changed > self.max_bucket_size else 0
        # Splitting a window costs two count requests a level, about two levels a bucket.
        search_calls = pages + 4 * buckets
        search = max(search_calls / search_per_s, pages * self._seconds_per_call("search"))

        list_pages = math.ceil(total / self.LIST_PAGE_SIZE)
        if self._list_needs_batch_read():
            batch_reads = math.ceil(changed / self.LIST_PAGE_SIZE)
            parallel = max(1, concurrency.max_parallel(self.config))
            listed = max(
                (list_pages + batch_reads) / general_per_s,
                list_pages * self._seconds_per_call("list"),
                batch_reads * self._seconds_per_call("batch_read") / parallel,
            )
        else:
            listed = max(list_pages / general_per_s, list_pages * self._seconds_per_call("list_full"))
        return {"search": round(search, 1), "list": round(listed, 1)}

    @property
    def object_path(self) -> str:
        """crm/v3/objects/{type}, from the search path."""
        return self.path[: -len("/search")]

    def _list_properties(self) -> List[str]:
        """The properties a list or batch read asks for: the search request's, plus the
        replication property the window is checked on."""
        props = list(self.selected_properties)
        if self.replication_key_filter not in props:
            props.append(self.replication_key_filter)
        return props

    def _list_needs_batch_read(self) -> bool:
        params = {"limit": self.LIST_PAGE_SIZE, "archived": "false", "properties": ",".join(self._list_properties())}
        return len(self._list_request(params).url) > self.MAX_LIST_URL_LENGTH

    def _window_ms(self, context: Optional[dict]):
        start = self.starting_time or self.get_starting_time(context)
        return start, self.get_end_time()

    @staticmethod
    def _to_ms(value) -> Optional[int]:
        if value is None or value == "":
            return None
        if isinstance(value, (int, float)):
            return int(value)
        text = str(value)
        if text.isdigit():
            return int(text)
        try:
            return int(parse(text).timestamp() * 1000)
        except Exception:
            return None

    def _in_window(self, result: dict, start: int, end: Optional[int]) -> bool:
        """The search filter: replication property GT start and LTE end (null never matches)."""
        value = self._to_ms((result.get("properties") or {}).get(self.replication_key_filter))
        if value is None:
            return False
        if start is not None and value <= start:
            return False
        return end is None or value <= end

    def _shape_like_search(self, result: dict, requested: List[str]) -> dict:
        row = {k: result[k] for k in self.SEARCH_RESULT_KEYS if k in result}
        props = dict(result.get("properties") or {})
        if self.replication_key_filter not in self.selected_properties:
            props.pop(self.replication_key_filter, None)
        row["properties"] = props
        return row

    def _list_request(self, params: dict) -> requests.PreparedRequest:
        return self.build_prepared_request(
            "GET", f"{self.url_base}{self.object_path}", params=params, headers=self.http_headers
        )

    def _request_records_list(self, context: Optional[dict]) -> Iterable[dict]:
        decorated_request = self.request_decorator(self._request)
        requested = self._list_properties()
        # No end unless the config sets end_date: a record changed while the list pages run is
        # read too (as search's open last bucket). The bookmark stays at the sync's start.
        start, _ = self._window_ms(context)
        end = self.get_end_time(curr_date_fallback=False)
        batch_read = self._list_needs_batch_read()
        params = {"limit": self.LIST_PAGE_SIZE, "archived": "false", "properties": ",".join(requested)}
        # With batch read, the list call only pages ids (and the window's property).
        if batch_read:
            params["properties"] = self.replication_key_filter
        pool = concurrency.pool(self.config)
        self.logger.info(
            "Sync of '%s' through the list API%s (%s properties, %s requests at once)",
            self.name, " and batch read" if batch_read else "", len(requested), concurrency.max_parallel(self.config),
        )

        def pages():
            """Each list page's records in the window, as a future: batch reads run on the pool."""
            after = None
            while True:
                page_params = dict(params)
                if after:
                    page_params["after"] = after
                body = decorated_request(self._list_request(page_params), context).json()
                results = [r for r in body.get("results", []) if self._in_window(r, start, end)]
                if batch_read and results:
                    ids = [r["id"] for r in results]
                    if pool is None:
                        yield concurrency.done(self._batch_read(decorated_request, ids, requested, context))
                    else:
                        yield pool.submit(self._batch_read, decorated_request, ids, requested, context)
                else:
                    yield concurrency.done(results)
                next_after = ((body.get("paging") or {}).get("next") or {}).get("after")
                if not next_after or next_after == after:
                    return
                after = next_after

        depth = concurrency.max_parallel(self.config) if batch_read else concurrency.PREFETCH_PAGES
        for future in concurrency.prefetch(pages, depth=depth, enabled=pool is not None):
            rows = [self._shape_like_search(r, requested) for r in future.result()]
            for row in self.get_associations_data(rows):
                yield row

    def _batch_read(self, decorated_request, ids: List[str], requested: List[str], context) -> List[dict]:
        """Every requested property for these ids, in their order (ids gone since are skipped)."""
        prepared = self.build_prepared_request(
            "POST",
            f"{self.url_base}{self.object_path}/batch/read",
            params={"archived": "false"},
            headers=self.http_headers,
            json={"properties": requested, "inputs": [{"id": str(i)} for i in ids]},
        )
        response = decorated_request(prepared, context)
        by_id = {str(r.get("id")): r for r in response.json().get("results", [])}
        return [by_id[str(i)] for i in ids if str(i) in by_id]

    # Search pages are read ahead of the records being written: the buckets and cursor live on
    # the reading thread only.
    prefetch_pages = True

    # --- key scans ------------------------------------------------------------------------
    #
    # A record deleted for good (an emptied recycle bin, a GDPR deletion) leaves every feed: the
    # live list and search, the archived list, and no record lists it as merged. Every
    # key_scan_interval_hours (or when key_scan is set), a stream read since its bookmark also
    # pages every live id through the list API (one property, 100 a page) and, only once the
    # whole listing came back, writes each id as {id, _sdc_key_only: true} after the stream's
    # records. A loader that sees them marks the rows it holds whose ids were not listed
    # (aria-singer-taps: the merge strategy's key_only). A listing that fails or finds nothing
    # writes no ids, so nothing is marked, and the scan stays due for the next run. A sync with
    # no bookmark (a first sync, a full refresh) lists every record anyway: no scan, but the
    # time is kept as the last one.

    KEY_ONLY_COLUMN = "_sdc_key_only"
    KEY_SCAN_BOOKMARK = "key_scan_at"

    def key_scans_on(self) -> bool:
        configured = self.config.get("key_scan") in (True, "true") or bool(self.config.get("key_scan_interval_hours"))
        return configured and self.list_api_supported(None)

    def key_scan_due(self, now: datetime) -> bool:
        if not self.key_scans_on():
            return False
        if self.config.get("key_scan") in (True, "true"):
            return True
        hours = float(self.config.get("key_scan_interval_hours") or 0)
        if hours <= 0:
            return False
        last = self.stream_state.get(self.KEY_SCAN_BOOKMARK)
        if not last:
            return True
        try:
            return now - parse(last) >= timedelta(hours=hours)
        except Exception:
            return True

    def _generate_schema_messages(self):
        """The stream's SCHEMA, with _sdc_key_only when it scans keys."""
        for message in super()._generate_schema_messages():
            if self.key_scans_on():
                schema = dict(message.schema)
                schema["properties"] = {**schema.get("properties", {}), self.KEY_ONLY_COLUMN: {"type": ["null", "boolean"]}}
                message.schema = schema
            yield message

    def _list_live_ids(self) -> List[str]:
        """Every live id, in the list API's order; raises if any page fails."""
        decorated_request = self.request_decorator(self._request)
        params = {"limit": self.LIST_PAGE_SIZE, "archived": "false", "properties": "hs_object_id"}
        ids: List[str] = []
        seen = set()
        after = None
        while True:
            page_params = dict(params)
            if after:
                page_params["after"] = after
            body = decorated_request(self._list_request(page_params), None).json()
            for result in body.get("results", []):
                rid = str(result.get("id") or "")
                if rid and rid not in seen:
                    seen.add(rid)
                    ids.append(rid)
            next_after = ((body.get("paging") or {}).get("next") or {}).get("after")
            if not next_after or next_after == after:
                return ids
            after = next_after

    def _key_scan(self, started: datetime) -> None:
        from tap_hubspot_beta import stats as sync_stats

        try:
            ids = self._list_live_ids()
        except Exception as exc:
            # Never part of a listing: the rows it left out would read as deleted.
            self.logger.warning("%s: key scan stopped (%s); no ids written, nothing marked", self.name, type(exc).__name__)
            return
        if not ids:
            self.logger.warning("%s: key scan listed no ids; none written, nothing marked", self.name)
            return
        alias = self.stream_maps[0].stream_alias
        for rid in ids:
            singer.write_message(
                singer.RecordMessage(stream=alias, record={"id": rid, self.KEY_ONLY_COLUMN: True}, time_extracted=started)
            )
        sync_stats.get().records(f"{self.name}_key_scan", len(ids))
        self.logger.info("%s: key scan listed %s live ids", self.name, f"{len(ids):,}")
        self.stream_state[self.KEY_SCAN_BOOKMARK] = started.isoformat()

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        if self.use_list_api(context):
            yield from self._request_records_list(context)
            return
        yield from super().request_records(context)

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        if self.properties_url and isinstance(row.get("properties"), dict):
            if self.omit_null_properties:
                # Unset properties are left out on every path (search sends them as null).
                row["properties"] = {k: v for k, v in row["properties"].items() if v is not None}
            else:
                # Every requested property is a column, set or not: the list, batch read and
                # search APIs then give identical rows whether or not HubSpot sends unset ones.
                for name in self.hubspot_property_names:
                    row["properties"].setdefault(name, None)
        row = super().post_process(row, context, skip_id=True)
        # store archived value in _hg_archived
        row["_hg_archived"] = False

        if self.use_list_filtering:
            row["_hg_list_memberships"] = self._list_record_ids.get(row["id"], [])
        return row

    def _sync_records(  # noqa C901  # too complex
        self, context: Optional[dict] = None
    ) -> None:
        """Sync records, emitting RECORD and STATE messages. """

        record_count = 0
        current_context: Optional[dict]
        context_list: Optional[List[dict]]
        context_list = [context] if context is not None else self.partitions
        selected = self.selected
        # Decided before the sync moves the bookmark (key scans, above).
        started = datetime.now(timezone.utc)
        key_scan = selected and not context and self._has_bookmark(None) and self.key_scan_due(started)
        listed_all = selected and not context and not self._has_bookmark(None) and self.key_scans_on()
        # Child batches are read on the worker pool and written in order (ChildPipeline).
        children = ChildPipeline(self, fallback=self._sync_child)

        for current_context in context_list or [{}]:
            if current_context.get("object_id_filters") == []:
                continue
            partition_record_count = 0
            current_context = current_context or None
            state = self.get_context_state(current_context)
            state_partition_context = self._get_state_partition_context(current_context)
            self._write_starting_replication_value(current_context)
            child_context: Optional[dict] = (
                None if current_context is None else copy.copy(current_context)
            )
            child_batches = {
                child: []
                for child in self.child_streams
                if child.selected or child.has_selected_descendents
            }

            record_id_set = set()
            for record_result in self.get_records(current_context):
                if isinstance(record_result, tuple):
                    # Tuple items should be the record and the child context
                    record, child_context = record_result
                else:
                    record = record_result

                # Skip duplicate records
                if record["id"] in record_id_set:
                    continue
                record_id_set.add(record["id"])

                child_context = copy.copy(
                    self.get_child_context(record=record, context=child_context)
                )
                for key, val in (state_partition_context or {}).items():
                    # Add state context to records if not already present
                    if key not in record:
                        record[key] = val

                # Sync children, except when primary mapper filters out the record.
                # Each child gets its ids in batches as large as its API takes.
                if self.stream_maps[0].get_filter_result(record):
                    for child_stream, batch in child_batches.items():
                        batch.append(child_context)
                        if len(batch) >= self.child_batch_size(child_stream):
                            children.submit(child_stream, batch)
                            child_batches[child_stream] = batch = []

                if selected:
                    if (record_count - 1) % self.STATE_MSG_FREQUENCY == 0:
                        # No STATE before the child records of the records it covers.
                        children.drain()
                        self._write_state_message()
                    self._write_record_message(record)
                if selected:
                    try:
                        self._increment_stream_state(record, context=current_context)
                    except InvalidStreamSortException as ex:
                        log_sort_error(
                            log_fn=self.logger.error,
                            ex=ex,
                            record_count=record_count + 1,
                            partition_record_count=partition_record_count + 1,
                            current_context=current_context,
                            state_partition_context=state_partition_context,
                            stream_name=self.name,
                        )
                        raise ex
                elif self.has_selected_descendents and self.replication_key:
                    self._increment_child_replication_state(record, context=current_context)

                record_count += 1
                partition_record_count += 1
                if self._check_max_record_limit(record_count):
                    children.drain()
                    return

            # The last batch, as before (an empty one too: the child still writes its schema).
            for child_stream, batch in child_batches.items():
                children.submit(child_stream, batch)
            children.drain()
            if current_context == state_partition_context:
                # Finalize per-partition state only if 1:1 with context
                finalize_state_progress_markers(state)


        if not context:
            # Finalize total stream only if we have the full full context.
            # Otherwise will be finalized by tap at end of sync.
            finalize_state_progress_markers(self.stream_state)
        if key_scan:
            self._key_scan(started)
        elif listed_all:
            self.stream_state[self.KEY_SCAN_BOOKMARK] = started.isoformat()
        self._write_record_count_log(record_count=record_count, context=context)
        # Reset interim bookmarks before emitting final STATE message:
        self._write_state_message()


    def child_batch_size(self, child_stream) -> int:
        """Parent ids per child request: what the child's API takes (v4 associations 1,000,
        history batch reads 50), else this stream's bulk_child_size."""
        return getattr(child_stream, "max_inputs_per_request", None) or self.bulk_child_size

    def _sync_child(self, child_stream, contexts: List[dict]) -> None:
        if not child_stream.bulk_child:
            for id in contexts:
                child_stream.sync(context=id)
        else:
            child_stream.sync(context={"ids": list(contexts)})

    def _sync_children(self, child_context: dict) -> None:
        for child_stream in self.child_streams:
            if child_stream.selected or child_stream.has_selected_descendents:
                self._sync_child(child_stream, child_context.get("ids") or [])


def _iso_ms(value: Optional[int]) -> Optional[str]:
    if value is None:
        return None
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%MZ")


class hubspotV3Stream(hubspotStream):
    """hubspot stream class."""

    records_jsonpath = "$.results[*]"
    next_page_token_jsonpath = "$.paging.next.after"

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        all_matches = extract_jsonpath(self.next_page_token_jsonpath, response.json())
        return next(iter(all_matches), None)

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        params["limit"] = self.page_size
        params.update(self.additional_params)
        if self.properties_url:
            # requesting either properties or properties with history
            # if we send both it returns an error saying the url is too long
            if params.get("propertiesWithHistory"):
                params["propertiesWithHistory"] = ",".join(self.selected_properties)
            else:
                params["properties"] = ",".join(self.selected_properties)
        if next_page_token:
            params["after"] = next_page_token
        #TODO: from 6444 -° should we keep?
        if self.name == "forms":
            params["formTypes"] = "all"  
        return params



class AssociationsV3ParentStream(hubspotV3Stream):
    name = "associations_v3_parent"
    primary_keys = ["id"]
    prefetch_pages = True

    def get_child_context(self, record: dict, context) -> dict:
        return {"id": record["id"]}

    def _sync_records(  # noqa: C901
        self, context: Optional[dict] = None
    ) -> None:
        """Sync records, emitting RECORD and STATE messages. """
        record_count = 0
        current_context: Optional[dict]
        context_list: Optional[List[dict]]
        context_list = [context] if context is not None else self.partitions
        selected = self.selected
        # Child batches are read on the worker pool and written in order (ChildPipeline).
        children = ChildPipeline(self, fallback=lambda child, ids: child.sync(context={"ids": list(ids)}))

        for current_context in context_list or [{}]:
            partition_record_count = 0
            current_context = current_context or None
            state = self.get_context_state(current_context)
            state_partition_context = self._get_state_partition_context(current_context)
            self._write_starting_replication_value(current_context)
            child_context: Optional[dict] = (
                None if current_context is None else copy.copy(current_context)
            )
            child_context_bulk = {"ids": []}
            for record_result in self.get_records(current_context):
                if isinstance(record_result, tuple):
                    # Tuple items should be the record and the child context
                    record, child_context = record_result
                else:
                    record = record_result
                child_context = copy.copy(
                    self.get_child_context(record=record, context=child_context)
                )
                for key, val in (state_partition_context or {}).items():
                    # Add state context to records if not already present
                    if key not in record:
                        record[key] = val

                # Sync children, except when primary mapper filters out the record
                if self.stream_maps[0].get_filter_result(record):
                    child_context_bulk["ids"].append(child_context)
                if len(child_context_bulk["ids"])>=self.bulk_child_size:
                    self._submit_children(children, child_context_bulk)
                    child_context_bulk = {"ids": []}
                self._check_max_record_limit(record_count)
                if selected:
                    if (record_count - 1) % self.STATE_MSG_FREQUENCY == 0:
                        children.drain()
                        self._write_state_message()
                    self._write_record_message(record)
                    try:
                        self._increment_stream_state(record, context=current_context)
                    except InvalidStreamSortException as ex:
                        log_sort_error(
                            log_fn=self.logger.error,
                            ex=ex,
                            record_count=record_count + 1,
                            partition_record_count=partition_record_count + 1,
                            current_context=current_context,
                            state_partition_context=state_partition_context,
                            stream_name=self.name,
                        )
                        raise ex

                record_count += 1
                partition_record_count += 1
            if len(child_context_bulk):
                self._submit_children(children, child_context_bulk)
            children.drain()
            if current_context == state_partition_context:
                # Finalize per-partition state only if 1:1 with context
                finalize_state_progress_markers(state)
        if not context:
            # Finalize total stream only if we have the full full context.
            # Otherwise will be finalized by tap at end of sync.
            finalize_state_progress_markers(self.stream_state)
        self._write_record_count_log(record_count=record_count, context=context)
        # Reset interim bookmarks before emitting final STATE message:
        self._write_state_message()

class DynamicDiscoveredHubspotV3Stream(hubspotV3Stream):
    # Custom objects: every record, page after page (the cursor is the only state).
    prefetch_pages = True

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        for name, value in row["properties"].items():
            row[name] = value
        del row["properties"]
        row = super().post_process(row, context)
        return row


    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params = super().get_url_params(context, next_page_token)
        params["properties"] = ",".join(self.selected_properties)
        return params

class hubspotV3SingleSearchStream(hubspotStream):
    """hubspot stream class."""

    rest_method = "POST"

    records_jsonpath = "$.results[*]"
    next_page_token_jsonpath = "$.paging.next.after"
    filter = None
    starting_time = None
    page_size = 100

    def get_starting_time(self, context):
        start_date = self.get_starting_timestamp(context)
        if start_date:
            return int(start_date.timestamp() * 1000)
        return int(datetime(2000, 1, 1).timestamp() * 1000)

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        response_json = response.json()
        if response_json.get("hasMore"):
            offset = response_json.get("offset")
            if offset:
                return offset

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        """Prepare the data payload for the REST API request."""
        payload = {}
        payload["limit"] = self.page_size
        payload["filters"] = []
        if self.filter:
            payload["filters"].append(self.filter)
        if next_page_token and next_page_token!="0":
            payload["offset"] = next_page_token
        return payload


    
class hubspotHistoryV3Stream(hubspotV3Stream):
    rest_method = "POST"
    bulk_child = True
    prefetchable_child = True
    # HubSpot's batch read with propertiesWithHistory takes 50 ids.
    max_inputs_per_request = 50
    schema_written = False

    properties_param = "propertiesWithHistory"
    merge_pk = "id"

    def post_process(self, row: dict, context) -> dict:
        row = super().post_process(row, context)
        props = row.get("propertiesWithHistory") or dict()
        row["propertiesWithHistory"] = {k:v for (k,v) in props.items() if v}
        row["property_history"] = self.property_history(row["propertiesWithHistory"])
        row = {k:v for k,v in row.items() if k in ["id", "propertiesWithHistory", "property_history", "createdAt", "updatedAt", "archived", "archivedAt"]}
        return row

    @staticmethod
    def property_history(history: dict) -> List[dict]:
        """propertiesWithHistory as one row per version (the property's name, then HubSpot's
        fields), so the warehouse gets them as a child table (Fivetran's deal_stage: one row
        per dealstage version)."""
        out = []
        for name in sorted(history):
            for version in history[name] or []:
                if not isinstance(version, dict):
                    continue
                out.append({
                    "property": name,
                    "value": version.get("value"),
                    "timestamp": version.get("timestamp"),
                    "sourceType": version.get("sourceType"),
                    "sourceId": version.get("sourceId"),
                    "sourceLabel": version.get("sourceLabel"),
                    "updatedByUserId": version.get("updatedByUserId"),
                })
        return out
    
    def _write_schema_message(self) -> None:
        """Write out a SCHEMA message with the stream schema."""
        if not self.schema_written:
            for schema_message in self._generate_schema_messages():
                schema_message.schema = th.PropertiesList(*self.base_properties).to_dict()
                singer.write_message(schema_message)
                self.schema_written = True
    
    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        return {}

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        """Prepare the data payload for the REST API request."""
        payload = {}
        payload["propertiesWithHistory"] = self.selected_properties
        payload["inputs"] = [{"id": str(c["id"])} for c in context["ids"]]
        return payload
