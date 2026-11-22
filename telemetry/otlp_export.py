"""span 트리를 OTLP/HTTP로 내보내는 SpanProcessor (Tempo → Grafana 트레이스 조회용).

SpanRecorder(JSONL)와 같은 본문 정책을 따른다: tracing.capture_body=false면
내보내기 전에 본문 속성을 삭제한다.
"""
from __future__ import annotations

from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from telemetry.span_processor import strip_attributes
from telemetry.span_recorder import BODY_KEYS


class BodySafeOtlpProcessor(BatchSpanProcessor):
    def __init__(self, endpoint: str, capture_body: bool = False) -> None:
        super().__init__(OTLPSpanExporter(endpoint=endpoint))
        self.capture_body = capture_body

    def on_end(self, span: ReadableSpan) -> None:
        if not self.capture_body:
            strip_attributes(span, BODY_KEYS)
        super().on_end(span)
