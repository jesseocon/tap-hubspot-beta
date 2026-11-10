import logging

from tap_hubspot_beta.privacy import RedactingFilter, describe_context, redact_text


def test_contexts_keep_ids_and_count_lists():
    ctx = {"ids": [{"id": "1", "subscriber_email": "a@example.test"}] * 3}
    assert describe_context(ctx) == {"ids": "<3 ids>"}
    assert describe_context({"id": "51", "subscriber_email": "a@example.test"}) == {
        "id": "51",
        "subscriber_email": "<redacted>",
    }
    assert describe_context({"form_id": "ab-12", "list_id": 7}) == {"form_id": "ab-12", "list_id": 7}
    assert describe_context({"contact_id": 5, "row": {"email": "a@example.test"}}) == {
        "contact_id": 5,
        "row": "<redacted>",
    }
    assert describe_context({"id": "x y@example.test"}) == {"id": "<redacted>"}
    assert describe_context(None) is None


def test_emails_are_masked():
    assert redact_text("to a.b+c@sub.example.test now") == "to [email] now"
    assert redact_text("status/email/a%40example.test") == "status/email/[email]"
    assert redact_text("no address here") == "no address here"


def test_filter_masks_the_formatted_message():
    record = logging.LogRecord("t", logging.INFO, __file__, 1, "ctx %s", ({"e": "a@example.test"},), None)
    RedactingFilter().filter(record)
    assert record.getMessage() == "ctx {'e': '[email]'}"
