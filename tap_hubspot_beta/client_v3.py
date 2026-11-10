"""REST client handling, including hubspotStream base class."""

from typing import Any, Dict, Iterable, Optional, List
import copy

import requests
from hotglue_singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_hubspot_beta.client_base import hubspotStream
from pendulum import parse
from datetime import datetime, timedelta
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


    # --- full syncs through the list API -------------------------------------------------
    #
    # A stream with no bookmark reads every record changed since start_date. The search API
    # does that in windows of at most 10,000 results (the time buckets above, each costing
    # count requests), at HubSpot's search limit of about 5 requests a second. The list API
    # (GET crm/v3/objects/{type}) pages through every record by id at the general limit, with
    # no window. Its records are the search API's: the same object shape, the same properties
    # (every selected one is asked for), the same (start_date, sync start] window on the
    # replication property, applied here instead of by HubSpot. When the property names don't
    # fit in a URL, each page of ids is read in full with the batch read API.

    # Top-level keys of a search result; anything else the list API adds is dropped.
    SEARCH_RESULT_KEYS = ("id", "properties", "createdAt", "updatedAt", "archived", "archivedAt")
    LIST_PAGE_SIZE = 100  # the list and batch read APIs' maximum
    MAX_LIST_URL_LENGTH = 7000

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

    def use_list_api(self, context: Optional[dict]) -> bool:
        """True for a full sync of a CRM object stream that search reads without extra filters."""
        if not self.config.get("use_list_api_for_full_sync", True):
            return False
        if not self.properties_url or not (self.path or "").startswith("crm/v3/objects/"):
            return False
        if not self.path.endswith("/search") or self.filter or self.special_replication:
            return False
        if self.use_list_filtering or (context and context.get("object_id_filters") is not None):
            return False
        if not self.replication_key or not getattr(self, "replication_key_filter", None):
            return False
        return not self._has_bookmark(context)

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
        start, end = self._window_ms(context)
        full_params = {"limit": self.LIST_PAGE_SIZE, "archived": "false", "properties": ",".join(requested)}
        batch_read = len(self._list_request(full_params).url) > self.MAX_LIST_URL_LENGTH
        # With batch read, the list call only pages ids (and the window's property).
        params = dict(full_params)
        if batch_read:
            params["properties"] = self.replication_key_filter
        self.logger.info(
            "Full sync of '%s' through the list API%s (%s properties)",
            self.name, " and batch read" if batch_read else "", len(requested),
        )
        after = None
        while True:
            page_params = dict(params)
            if after:
                page_params["after"] = after
            response = decorated_request(self._list_request(page_params), context)
            body = response.json()
            results = [r for r in body.get("results", []) if self._in_window(r, start, end)]
            if batch_read and results:
                results = self._batch_read(decorated_request, [r["id"] for r in results], requested, context)
            rows = [self._shape_like_search(r, requested) for r in results]
            for row in self.get_associations_data(rows):
                yield row
            next_after = ((body.get("paging") or {}).get("next") or {}).get("after")
            if not next_after or next_after == after:
                break
            after = next_after

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

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        if self.use_list_api(context):
            yield from self._request_records_list(context)
            return
        yield from super().request_records(context)

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        # Every requested property is a column, set or not: the list, batch read and search
        # APIs then give identical rows whether or not HubSpot sends unset ones as null.
        if self.properties_url and isinstance(row.get("properties"), dict):
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
                            self._sync_child(child_stream, batch)
                            child_batches[child_stream] = batch = []

                if selected:
                    if (record_count - 1) % self.STATE_MSG_FREQUENCY == 0:
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
                    return

            # The last batch, as before (an empty one too: the child still writes its schema).
            for child_stream, batch in child_batches.items():
                self._sync_child(child_stream, batch)
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
                    self._sync_children(child_context_bulk)
                    child_context_bulk = {"ids": []}
                self._check_max_record_limit(record_count)
                if selected:
                    if (record_count - 1) % self.STATE_MSG_FREQUENCY == 0:
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
                self._sync_children(child_context_bulk)
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
    # HubSpot's batch read with propertiesWithHistory takes 50 ids.
    max_inputs_per_request = 50
    schema_written = False

    properties_param = "propertiesWithHistory"
    merge_pk = "id"

    def post_process(self, row: dict, context) -> dict:
        row = super().post_process(row, context)
        props = row.get("propertiesWithHistory") or dict()
        row["propertiesWithHistory"] = {k:v for (k,v) in props.items() if v}
        row = {k:v for k,v in row.items() if k in ["id", "propertiesWithHistory", "createdAt", "updatedAt", "archived", "archivedAt"]}
        return row
    
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
