# tap-hubspot

`tap-hubspot` is a Singer tap for hubspot.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Installation

- [ ] `Developer TODO:` Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPi repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.

```bash
pipx install tap-hubspot
```

## Configuration

### Accepted Config Options

- [ ] `Developer TODO:` Provide a list of config options accepted by the tap.

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-hubspot --about
```

### Speed: requests in parallel, search or list, and what each run did

- **One request budget per account.** At most `rate_limit_per_10s` (default 110) requests start
  in any 10-second window and `search_rate_limit_per_second` (default 5) search requests in any
  second, across every thread, and across tap processes sharing `rate_limit_file`. A 429 waits
  Retry-After and is resent. HubSpot's daily limit stops the sync with a message saying so.
- **Requests in parallel, output in order** (`max_parallel_requests`, default 8; 1 turns it off).
  Pages are read ahead of the records being written. Batch reads, association batches (1,000 ids)
  and history batches (50 ids) are read on worker threads while the parent reads on. Records and
  STATE are written by one thread, in the order the sequential tap wrote them. A STATE never comes
  before the child records of the records it covers.
- **Search or list, per stream and run** (`sync_strategy`: `auto` default, `search`, `list`). With
  no bookmark (a first sync, a full refresh) CRM objects are read through the list API. With one,
  `auto` counts the records changed since the bookmark (less `lookback_minutes`) and all records,
  and reads by whichever is estimated faster. Under `auto_list_min_changed` (default 5,000)
  changed records it searches. Both read the same window and leave the same bookmark (the latest
  `updatedAt` written).
- **`omit_null_properties`** (default false): properties without a value are left out of RECORD
  messages. Use it with a loader that stores a missing property as null: the same rows, much less
  work. HubSpot leaves most properties of a wide object unset (84% of contacts' 1,859 columns on one
  portal).
- **Logs.** Each decision is logged (`contacts: 182,400 changed since 2026-11-13T09:00Z of 190,112 -> list
  (est. 5m47s vs 9m07s by search)`). At the end of each stream and of the run there is a summary:
  records, requests per endpoint, average latency, 429s, time waited for the budget, wall and CPU
  time. The same numbers follow as JSON on a `SYNC_SUMMARY` line.

### Source Authentication and Authorization

- [ ] `Developer TODO:` If your tap requires special access on the source system, or any special authentication requirements, provide those here.

## Usage

You can easily run `tap-hubspot` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-hubspot --version
tap-hubspot --help
tap-hubspot --config CONFIG --discover > ./catalog.json
```

### Available and Selected Filters

The tap supports Hotglue filter discovery and runtime filter selection:

- `--get-available-filters`: outputs `available-filters.json` metadata used by the UI.
- `--selected-filters`: applies the selected filters for a sync run.

Supported stream filters:

- `contact_events`: `eventType` (`IN` / `EQ`)
- `form_submissions`: `form_id` (`IN` / `EQ`)
- `lists_v3`: `list_ids` (`IN` / `EQ`) — scopes which lists are fetched
- `list_membership_v3`: `membership_list_ids` (`IN` / `EQ`) — scopes which lists get membership sync

Get available filters:

```bash
tap-hubspot-beta \
  --config config.json \
  --catalog catalog-selected.json \
  --get-available-filters > available-filters.json
```

Run a sync with selected filters:

```bash
tap-hubspot-beta \
  --config config.json \
  --catalog catalog-selected.json \
  --state state.json \
  --selected-filters selected-filters.json
```

Example `selected-filters.json`:

```json
{
  "filters_version": "1.0.0",
  "streams": {
    "contact_events": {
      "clause_1": {
        "field": "eventType",
        "operator": "IN",
        "value": [
          "e_visited_page",
          "e_submitted_form"
        ]
      }
    },
    "lists_v3": {
      "clause_1": {
        "field": "list_ids",
        "operator": "IN",
        "value": ["9", "10"]
      }
    },
    "list_membership_v3": {
      "clause_1": {
        "field": "membership_list_ids",
        "operator": "EQ",
        "value": "9"
      }
    }
  }
}
```

## Developer Resources

- [ ] `Developer TODO:` As a first step, scan the entire project for the text "`TODO:`" and complete any recommended steps, deleting the "TODO" references once completed.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_hubspot/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-hubspot` CLI interface directly using `poetry run`:

```bash
poetry run tap-hubspot --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any _"TODO"_ items listed in
the file.

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-hubspot
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-hubspot --version
# OR run a test `elt` pipeline:
meltano elt tap-hubspot target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to 
develop your own taps and targets.
