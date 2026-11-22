"""OTel 브리지: Collector가 Kafka(otel-spans)에 넣은 OTLP span → AgentEvent(agent-events).

계측 SDK(telemetry/instrument.py)를 프로세스 안에 넣을 수 없는 외부 앱은 속성 규약
(`event.type`, `skill`, `tool`, `status`, `tokens.*`, `cost.usd`, `run.id`)에 맞춘 OTel span을
Collector로 보낸다. Collector는 OTLP JSON 그대로 Kafka에 쓰는데, raw 이후 단계는 AgentEvent만
이해하므로 이 브리지가 그 사이에서 형식을 바꾼다.

- `event.type`이 있는 span만 이벤트가 된다 (llm·thinking span은 trace용이라 건너뛴다).
- 본문 속성은 읽지 않는다. 이벤트에는 아래에서 고른 지표·태그만 실린다.
- event_id는 trace_id·span_id에서 만들므로, 같은 메시지를 다시 읽어도 cleansed의
  event_id dedup에서 한 건으로 합쳐진다 (at-least-once + 하류 멱등).
- 이 경로에는 라벨링(tags)이 없다. 본문이 앱 밖으로 나오지 않기 때문이다.

사용: python -m pipeline.otel_bridge
"""
from __future__ import annotations

import base64
import json
import logging
import os
import signal
from datetime import datetime, timezone
from typing import Any

from common.config import load_config
from common.events import AgentEvent

logger = logging.getLogger(__name__)

EVENT_TYPES = {"skill_selected", "tool_called", "request_done"}
STATUSES = {"ok", "error", "timeout"}


def events_from_otlp(payload: dict[str, Any]) -> list[AgentEvent]:
    """OTLP JSON(ExportTraceServiceRequest) 한 건에서 이벤트를 뽑는다. 규약에 안 맞는 span은 건너뛴다."""
    events: list[AgentEvent] = []
    for resource_spans in payload.get("resourceSpans") or []:
        for scope_spans in resource_spans.get("scopeSpans") or []:
            for span in scope_spans.get("spans") or []:
                event = _event_from_span(span)
                if event is not None:
                    events.append(event)
    return events


def _event_from_span(span: dict[str, Any]) -> AgentEvent | None:
    attributes = _attributes(span.get("attributes"))
    event_type = attributes.get("event.type")
    if event_type is None:
        return None
    if event_type not in EVENT_TYPES:
        logger.warning("알 수 없는 event.type %r — 건너뜀", event_type)
        return None
    try:
        trace_id = _hex_id(span["traceId"], 16)
        span_id = _hex_id(span["spanId"], 8)
        start_ns = int(span["startTimeUnixNano"])
        end_ns = int(span["endTimeUnixNano"])
        status = str(attributes.get("status", "ok"))
        return AgentEvent(
            event_id=f"{trace_id}-{span_id}",
            trace_id=trace_id,
            ts=datetime.fromtimestamp(end_ns / 1e9, tz=timezone.utc).isoformat(),
            run_id=str(attributes.get("run.id", "live")),
            test_id=_optional_str(attributes.get("test.id")),
            event_type=event_type,
            skill=_optional_str(attributes.get("skill")),
            expected_skill=_optional_str(attributes.get("expected.skill")),
            skill_version=str(attributes.get("skill.version", "v1")),
            tool=_optional_str(attributes.get("tool")),
            status=status if status in STATUSES else "error",
            latency_ms=max(0, int((end_ns - start_ns) / 1_000_000)),
            tokens_in=int(attributes.get("tokens.in", 0)),
            tokens_out=int(attributes.get("tokens.out", 0)),
            cost_usd=float(attributes.get("cost.usd", 0.0)),
        )
    except (KeyError, TypeError, ValueError) as exc:
        logger.warning("span을 이벤트로 바꾸지 못함 — 건너뜀: %s", exc)
        return None


def _attributes(items: list[dict[str, Any]] | None) -> dict[str, Any]:
    """OTLP의 [{key, value: {stringValue|intValue|doubleValue|boolValue}}] → 평범한 dict."""
    result: dict[str, Any] = {}
    for item in items or []:
        value = item.get("value") or {}
        if "stringValue" in value:
            result[item["key"]] = value["stringValue"]
        elif "intValue" in value:
            result[item["key"]] = int(value["intValue"])  # JSON에서는 문자열로 온다
        elif "doubleValue" in value:
            result[item["key"]] = float(value["doubleValue"])
        elif "boolValue" in value:
            result[item["key"]] = bool(value["boolValue"])
    return result


def _hex_id(value: str, size: int) -> str:
    """Collector는 hex로 쓰지만, 표준 protobuf JSON(base64)으로 온 것도 받는다."""
    if len(value) == size * 2:
        bytes.fromhex(value)
        return value.lower()
    decoded = base64.b64decode(value, validate=True)
    if len(decoded) != size:
        raise ValueError(f"id 길이가 {size}바이트가 아님")
    return decoded.hex()


def _optional_str(value: Any) -> str | None:
    return None if value is None or value == "" else str(value)


def run() -> None:
    from confluent_kafka import Consumer

    from common.transport import KafkaProducer

    cfg = load_config()
    kafka = cfg["transport"]["kafka"]
    bridge = cfg["otel_bridge"]
    # 컨테이너 안에서는 브로커 주소가 다르다 (docker-compose.yml이 넣어 준다)
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", kafka["bootstrap_servers"])

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": bridge["group_id"],
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    producer = KafkaProducer(bootstrap, kafka["topic"])
    consumer.subscribe([bridge["topic"]])

    stopping = False

    def stop(*_: Any) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    logger.info("otel-bridge 시작: %s → %s (%s)", bridge["topic"], kafka["topic"], bootstrap)
    try:
        while not stopping:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                logger.warning("Kafka 오류: %s", message.error())
                continue
            try:
                events = events_from_otlp(json.loads(message.value()))
            except (ValueError, AttributeError, TypeError) as exc:
                logger.warning("OTLP JSON이 아닌 메시지 — 건너뜀: %s", exc)
                events = []
            for event in events:
                producer.send(event)
            # 이벤트가 브로커에 닿은 뒤에만 오프셋을 넘긴다
            producer.flush()
            consumer.commit(message=message, asynchronous=False)
            if events:
                logger.info("이벤트 %d건 전달", len(events))
    finally:
        producer.flush()
        consumer.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    run()
