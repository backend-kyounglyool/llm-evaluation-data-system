"""OTel 브리지: OTLP span → AgentEvent 변환과 본문 미유출."""
from __future__ import annotations

import base64

from common.events import FORBIDDEN_BODY_FIELDS
from pipeline.otel_bridge import events_from_otlp

TRACE_ID = "5b8efff798038103d269b633813fc60c"
SPAN_ID = "eee19b7ec3c1b174"


def attr(key: str, value) -> dict:
    if isinstance(value, bool):
        return {"key": key, "value": {"boolValue": value}}
    if isinstance(value, int):
        return {"key": key, "value": {"intValue": str(value)}}  # OTLP JSON은 int64를 문자열로 쓴다
    if isinstance(value, float):
        return {"key": key, "value": {"doubleValue": value}}
    return {"key": key, "value": {"stringValue": value}}


def span(span_id: str = SPAN_ID, trace_id: str = TRACE_ID, **attributes) -> dict:
    return {
        "traceId": trace_id,
        "spanId": span_id,
        "name": "request",
        "startTimeUnixNano": "1795348800000000000",
        "endTimeUnixNano": "1795348801250000000",
        "attributes": [attr(key.replace("_", "."), value) for key, value in attributes.items()],
    }


def payload(*spans: dict) -> dict:
    return {"resourceSpans": [{"resource": {}, "scopeSpans": [{"scope": {}, "spans": list(spans)}]}]}


def test_request_span_becomes_an_event_with_metrics():
    events = events_from_otlp(payload(span(
        event_type="request_done", run_id="annyong-live", skill="dorm", status="ok",
        tokens_in=1200, tokens_out=80, cost_usd=0.00016,
    )))

    assert len(events) == 1
    event = events[0]
    assert (event.event_type, event.run_id, event.skill, event.status) == ("request_done", "annyong-live", "dorm", "ok")
    assert (event.tokens_in, event.tokens_out, event.cost_usd) == (1200, 80, 0.00016)
    assert event.trace_id == TRACE_ID and event.latency_ms == 1250
    assert event.ts.startswith("2026-11-22T") and event.tags == {} and event.labeler is None


def test_event_id_is_stable_so_redelivery_dedups():
    message = payload(span(event_type="tool_called", tool="get_weekly_meal_plan", skill="dorm"))

    first, second = events_from_otlp(message), events_from_otlp(message)

    assert first[0].event_id == second[0].event_id == f"{TRACE_ID}-{SPAN_ID}"
    assert first[0].tool == "get_weekly_meal_plan"


def test_spans_without_an_event_type_are_trace_only():
    llm = span(span_id="aaaaaaaaaaaaaaaa", ai_kind="llm")
    tool = span(event_type="tool_called", tool="list_notices")

    events = events_from_otlp(payload(llm, tool))

    assert [event.event_type for event in events] == ["tool_called"]


def test_body_attributes_never_reach_the_event():
    events = events_from_otlp(payload(span(
        event_type="request_done", request_body="내 학번은 12345678", response_body="비밀",
    )))

    dumped = events[0].model_dump()
    assert not FORBIDDEN_BODY_FIELDS & dumped.keys()
    assert "12345678" not in events[0].to_json() and "비밀" not in events[0].to_json()


def test_malformed_spans_are_skipped_not_fatal():
    unknown_type = span(event_type="something_else")
    bad_id = span(trace_id="not-an-id", event_type="request_done")
    odd_status = span(span_id="bbbbbbbbbbbbbbbb", event_type="request_done", status="cancelled")

    events = events_from_otlp(payload(unknown_type, bad_id, odd_status))

    assert [event.status for event in events] == ["error"]
    assert events_from_otlp({}) == [] and events_from_otlp({"resourceSpans": None}) == []


def test_base64_ids_from_standard_protobuf_json_are_accepted():
    encoded = span(
        trace_id=base64.b64encode(bytes.fromhex(TRACE_ID)).decode(),
        span_id=base64.b64encode(bytes.fromhex(SPAN_ID)).decode(),
        event_type="request_done",
    )

    events = events_from_otlp(payload(encoded))

    assert events[0].trace_id == TRACE_ID and events[0].event_id == f"{TRACE_ID}-{SPAN_ID}"
