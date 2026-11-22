"""BodyStrippingSpanProcessor: 비동기 라벨링과 본문 미유출 불변식."""
from __future__ import annotations

import threading
import time

import pytest
from opentelemetry.sdk.trace import TracerProvider

from common.events import FORBIDDEN_BODY_FIELDS, LabelerInfo, Tag
from telemetry import span_processor
from telemetry.labeler import LabelResult
from telemetry.span_processor import BodyStrippingSpanProcessor

QUERY = "강남역 2호선 몇 분 뒤에 와?"


class FakeProducer:
    def __init__(self) -> None:
        self.events = []

    def send(self, event) -> None:
        self.events.append(event)

    def flush(self) -> None:
        pass


class FakeLabeler:
    def __init__(self, delay_s: float = 0.0, fail: bool = False, gate: threading.Event | None = None) -> None:
        self.delay_s, self.fail, self.gate = delay_s, fail, gate

    def label(self, query: str, selected_skill: str | None = None) -> LabelResult:
        if self.gate:
            self.gate.wait(timeout=5)
        time.sleep(self.delay_s)
        if self.fail:
            raise RuntimeError("labeler down")
        return LabelResult(
            tags={"intent": Tag(value="교통_조회", p=0.9)},
            labeler=LabelerInfo(model="fake", taxonomy_version="tx-test", source="jev"),
            cost_usd=0.001,
            latency_ms=1,
        )


def make(monkeypatch, labeler, async_cfg):
    monkeypatch.setattr(span_processor, "load_config", lambda: {"labeler": {"async": async_cfg}})
    producer = FakeProducer()
    processor = BodyStrippingSpanProcessor(producer, labeler)
    provider = TracerProvider()
    provider.add_span_processor(processor)
    return producer, processor, provider.get_tracer("test")


def emit(tracer, event_type: str = "request_done") -> float:
    """span 하나를 끝내고 on_end까지 걸린 시간을 돌려준다."""
    span = tracer.start_span("request", attributes={"event.type": event_type, "request.body": QUERY, "skill": "subway_arrival"})
    started = time.monotonic()
    span.end()
    return time.monotonic() - started


@pytest.mark.parametrize("async_cfg", [{"enabled": True, "workers": 2, "queue_size": 10}, {"enabled": False}])
def test_event_is_labeled_and_carries_no_body(monkeypatch, async_cfg):
    producer, processor, tracer = make(monkeypatch, FakeLabeler(), async_cfg)
    emit(tracer)
    assert processor.force_flush()

    (event,) = producer.events
    assert event.tags["intent"].value == "교통_조회"
    assert event.cost_usd == pytest.approx(0.001)
    payload = event.model_dump()
    assert not FORBIDDEN_BODY_FIELDS & payload.keys()
    assert QUERY not in event.model_dump_json()


def test_slow_labeler_does_not_block_request_path(monkeypatch):
    producer, processor, tracer = make(monkeypatch, FakeLabeler(delay_s=0.3), {"enabled": True, "workers": 2, "queue_size": 10})
    assert emit(tracer) < 0.05
    assert producer.events == []  # 아직 라벨링 중
    assert processor.force_flush()
    assert len(producer.events) == 1


def test_unlabeled_event_types_skip_the_queue(monkeypatch):
    producer, _, tracer = make(monkeypatch, FakeLabeler(delay_s=0.3), {"enabled": True, "workers": 1, "queue_size": 10})
    emit(tracer, event_type="tool_called")
    (event,) = producer.events
    assert event.tags == {}


def test_full_queue_emits_untagged_instead_of_blocking(monkeypatch):
    gate = threading.Event()
    producer, processor, tracer = make(monkeypatch, FakeLabeler(gate=gate), {"enabled": True, "workers": 1, "queue_size": 1})
    for _ in range(5):
        assert emit(tracer) < 0.05
    assert processor.dropped_labels >= 3  # 워커가 쥔 1건 + 큐 1건을 넘는 분량
    gate.set()
    assert processor.force_flush()
    assert len(producer.events) == 5
    assert sum(1 for e in producer.events if not e.tags) == processor.dropped_labels


def test_labeler_failure_still_emits_metrics(monkeypatch):
    producer, processor, tracer = make(monkeypatch, FakeLabeler(fail=True), {"enabled": True, "workers": 1, "queue_size": 10})
    emit(tracer)
    assert processor.force_flush()
    (event,) = producer.events
    assert event.tags == {} and event.labeler is None
