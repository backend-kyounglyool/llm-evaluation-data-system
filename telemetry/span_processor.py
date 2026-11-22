"""OTel Span Processor: 본문으로 라벨링 → 태그만 남기고 본문 제거 → 이벤트 발행.

핵심 불변식: request.body는 이벤트(Kafka) 경로로 나가지 않는다.
- on_end는 span에서 이벤트에 필요한 값만 떠 두고, span 속성의 본문을 즉시 삭제한다.
- 라벨링(Jev 캐스케이드)은 프로세스 내부 큐를 거쳐 워커 스레드가 수행한다(labeler.async).
  요청 경로는 라벨러 지연·장애와 분리되고, 본문은 큐(메모리)에만 머문다.
- 큐가 가득 차면 요청을 막지 않고 태그 없이 이벤트를 내보낸다.
- 라벨링 구간은 labeler 자식 span으로 trace에 남겨 워터폴에서 보이게 한다.
"""
from __future__ import annotations

import logging
import queue
import threading
import time
import uuid
from dataclasses import dataclass

from opentelemetry import trace
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor, TracerProvider
from opentelemetry.trace import NonRecordingSpan, SpanContext, set_span_in_context

from common.config import load_config
from common.events import AgentEvent
from common.transport import EventProducer, create_producer
from telemetry.labeler import create_labeler

logger = logging.getLogger(__name__)

BODY_ATTRIBUTES = ("request.body", "response.body")
LABELED_EVENT_TYPES = {"skill_selected", "request_done"}
DRAIN_TIMEOUT_S = 120


@dataclass
class PendingEvent:
    """span이 끝난 시점에 떠 둔 이벤트 재료. body는 라벨링에만 쓰이고 이벤트에는 실리지 않는다."""

    span_context: SpanContext
    attributes: dict
    start_ns: int
    end_ns: int
    body: str | None

    @property
    def needs_label(self) -> bool:
        return bool(self.body) and self.attributes.get("event.type") in LABELED_EVENT_TYPES


class BodyStrippingSpanProcessor(SpanProcessor):
    def __init__(self, producer: EventProducer, labeler=None) -> None:
        self.producer = producer
        self.labeler = labeler or create_labeler("cascade")
        self.dropped_labels = 0

        async_cfg = load_config()["labeler"].get("async", {})
        self._queue: queue.Queue[PendingEvent] | None = None
        if async_cfg.get("enabled", False):
            self._queue = queue.Queue(maxsize=async_cfg.get("queue_size", 1000))
            for index in range(async_cfg.get("workers", 4)):
                threading.Thread(target=self._work, name=f"labeler-{index}", daemon=True).start()

    def on_start(self, span: Span, parent_context=None) -> None:
        pass

    def on_end(self, span: ReadableSpan) -> None:
        attributes = dict(span.attributes or {})
        if attributes.get("event.type") is None:
            return
        body = attributes.get("request.body")
        pending = PendingEvent(span.context, attributes, span.start_time, span.end_time,
                               str(body) if body else None)
        self._strip_body(span)

        if not pending.needs_label:
            self._emit(pending)
        elif self._queue is None:
            self._label_and_emit(pending)
        else:
            try:
                self._queue.put_nowait(pending)
            except queue.Full:
                self.dropped_labels += 1
                logger.warning("라벨링 큐가 가득 참 — 태그 없이 이벤트 발행 (누적 %d건)", self.dropped_labels)
                self._emit(pending)

    def _work(self) -> None:
        while True:
            pending = self._queue.get()
            try:
                self._label_and_emit(pending)
            except Exception:
                # 라벨러 장애가 이벤트 유실로 번지지 않게 지표만이라도 내보낸다
                logger.exception("라벨링 실패 — 태그 없이 이벤트 발행")
                self._emit(pending)
            finally:
                self._queue.task_done()

    def _label_and_emit(self, pending: PendingEvent) -> None:
        label_start = time.time_ns()
        result = self.labeler.label(pending.body, pending.attributes.get("skill"))
        self._emit_labeler_span(pending.span_context, result, label_start, time.time_ns())
        self._emit(pending, result)

    def _emit(self, pending: PendingEvent, label=None) -> None:
        attributes = pending.attributes
        self.producer.send(AgentEvent(
            event_id=str(uuid.uuid4()),
            trace_id=f"{pending.span_context.trace_id:032x}",
            ts=_iso(pending.end_ns),
            run_id=str(attributes.get("run.id", "live")),
            test_id=attributes.get("test.id"),
            event_type=attributes["event.type"],
            skill=attributes.get("skill"),
            expected_skill=attributes.get("expected.skill"),
            skill_version=str(attributes.get("skill.version", "v1")),
            tool=attributes.get("tool"),
            status=str(attributes.get("status", "ok")),
            latency_ms=int((pending.end_ns - pending.start_ns) / 1_000_000),
            tokens_in=int(attributes.get("tokens.in", 0)),
            tokens_out=int(attributes.get("tokens.out", 0)),
            cost_usd=float(attributes.get("cost.usd", 0.0)) + (label.cost_usd if label else 0.0),
            tags=label.tags if label else {},
            labeler=label.labeler if label else None,
        ))

    @staticmethod
    def _emit_labeler_span(parent: SpanContext, result, start_ns: int, end_ns: int) -> None:
        """라벨링 구간을 종료된 부모 span의 자식으로 기록 (mock은 가정 지연을 반영)."""
        simulated_end = start_ns + int(result.latency_ms * 1e6)
        attrs = {
            "ai.kind": "labeler",
            "gen_ai.request.model": result.labeler.model,
            "labeler.source": result.labeler.source,
            "cost.usd": result.cost_usd,
        }
        if "intent" in result.tags:
            attrs["labeler.intent"] = str(result.tags["intent"].value)
            attrs["labeler.intent_p"] = result.tags["intent"].p
        tracer = trace.get_tracer("skill-eval-labeler")
        context = set_span_in_context(NonRecordingSpan(parent))
        child = tracer.start_span(
            f"labeler.{result.labeler.source}", context=context,
            start_time=start_ns, attributes=attrs,
        )
        child.end(end_time=max(end_ns, simulated_end))

    @staticmethod
    def _strip_body(span: ReadableSpan) -> None:
        """이후 어떤 exporter가 붙어도 본문을 못 보도록 span 속성에서 삭제."""
        strip_attributes(span, BODY_ATTRIBUTES)

    def _drain(self, timeout_s: float) -> bool:
        """큐에 남은 라벨링이 끝날 때까지 대기. 시간 안에 못 끝내면 False."""
        if self._queue is None:
            return True
        deadline = time.monotonic() + timeout_s
        while self._queue.unfinished_tasks:
            if time.monotonic() > deadline:
                logger.warning("라벨링 큐 drain 시간 초과 — %d건 미처리", self._queue.unfinished_tasks)
                return False
            time.sleep(0.05)
        return True

    def shutdown(self) -> None:
        self._drain(DRAIN_TIMEOUT_S)
        self.producer.flush()

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        drained = self._drain(max(timeout_millis / 1000, DRAIN_TIMEOUT_S))
        self.producer.flush()
        return drained


def strip_attributes(span: ReadableSpan, keys: tuple[str, ...]) -> None:
    """종료된 span의 BoundedAttributes는 불변이므로 내부 저장소에서 직접 지운다."""
    attrs = getattr(span, "_attributes", None)
    if attrs is None:
        return
    store = getattr(attrs, "_dict", attrs)
    for key in keys:
        try:
            store.pop(key, None)
        except TypeError:
            pass


def _iso(epoch_ns: int) -> str:
    from datetime import datetime, timezone

    return datetime.fromtimestamp(epoch_ns / 1e9, tz=timezone.utc).isoformat()


def setup_telemetry(labeler=None) -> trace.Tracer:
    """instrument.init()의 별칭 (하위 호환)."""
    from telemetry.instrument import init

    return init(labeler=labeler)
