"""Keep personal data out of the tap's logs.

HubSpot records hold people's emails, names and phone numbers. The tap's log lines go to the
orchestrator's logs, so a line may name a stream, a count or an id, never a record's values or
a child stream's context (a contact's context used to carry its email). Two layers:

- describe_context(): what the "Beginning ... sync" and METRIC lines say about a context: ids
  kept, lists counted, everything else left out.
- RedactingFilter: on the root handlers, it masks any email address left in a log line, for
  messages this tap or a library writes without going through describe_context().
"""

from __future__ import annotations

import logging
import re
from typing import Any, Optional

EMAIL_MASK = "[email]"
# Local part, @, a dotted domain with a 2+ letter TLD. Also matches URL-encoded @ (%40).
EMAIL = re.compile(r"[A-Za-z0-9._%+\-]+(?:@|%40)[A-Za-z0-9\-]+(?:\.[A-Za-z0-9\-]+)*\.[A-Za-z]{2,}")
# An id-like value: digits, or a short token without spaces or @.
_ID_VALUE = re.compile(r"^[A-Za-z0-9_.:\-]{1,64}$")
# Context keys whose scalar values are safe to log (ids and enum-like names).
_SAFE_KEYS = {"event_type", "asset_type", "object_type"}
_LIST_KEYS = {"ids", "inputs", "object_id_filters"}


def _id_key(key: str) -> bool:
    return key == "id" or key.endswith("_id") or key.endswith("Id") or key in _SAFE_KEYS


def describe_context(context: Optional[dict]) -> Any:
    """A context fit for a log line: ids kept, id lists counted, other values left out."""
    if not context:
        return context
    if not isinstance(context, dict):
        return "<context>"
    out: dict = {}
    for key, value in context.items():
        key = str(key)
        if isinstance(value, (list, tuple, set)):
            out[key] = f"<{len(value)} {'ids' if key in _LIST_KEYS else 'items'}>"
        elif isinstance(value, bool) or value is None:
            out[key] = value
        elif _id_key(key) and isinstance(value, int):
            out[key] = value
        elif _id_key(key) and isinstance(value, str) and _ID_VALUE.match(value):
            out[key] = value
        else:
            out[key] = "<redacted>"
    return out


def redact_text(text: str) -> str:
    """Text with every email address masked."""
    if "@" not in text and "%40" not in text:
        return text
    return EMAIL.sub(EMAIL_MASK, text)


class RedactingFilter(logging.Filter):
    """Masks email addresses in a record's final message (args merged in)."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            message = record.getMessage()
        except Exception:  # noqa: BLE001 - a bad format string is the logger's problem
            return True
        clean = redact_text(message)
        if clean != message:
            record.msg = clean
            record.args = None
        if record.exc_info and not record.exc_text:
            formatted = logging.Formatter().formatException(record.exc_info)
            record.exc_text = redact_text(formatted)
        elif record.exc_text:
            record.exc_text = redact_text(record.exc_text)
        return True


_FILTER = RedactingFilter()


def install() -> None:
    """Put the filter on every handler that writes this process's logs (idempotent)."""
    handlers = list(logging.getLogger().handlers)
    for logger in list(logging.Logger.manager.loggerDict.values()):
        if isinstance(logger, logging.Logger):
            handlers.extend(logger.handlers)
    if logging.lastResort is not None:
        handlers.append(logging.lastResort)
    for handler in handlers:
        if _FILTER not in handler.filters:
            handler.addFilter(_FILTER)
