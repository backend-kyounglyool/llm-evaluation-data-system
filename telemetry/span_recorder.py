"""모든 span을 trace 저장소(JSONL)에 기록하는 SpanProcessor.

워터폴·플레임 그래프의 원천 데이터. 이벤트(지표·태그) 경로와 별개로,
span 트리 전체(agent / workflow / thinking / llm / tool / labeler)를 보존한다.
본문(request.body 등)은 tracing.capture_body=true일 때만 남긴다.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor

from common.config import load_config, resolve_path

BODY_KEYS = ("request.body", "response.body", "gen_ai.prompt", "gen_ai.completion")

# span 속성 → 레코드 컬럼 매핑 (gen_ai.*는 OTel GenAI semantic convention)
_COLUMN_ATTRS = {
    "kind": ("ai.kind",),
    "run_id": ("run.id",),
    "test_id": ("test.id",),
    "skill": ("skill",),
    "tool": ("tool",),
    "model": ("gen_ai.request.model",),
    "status": ("status",),
    "tokens_in": ("gen_ai.usage.input_tokens", "tokens.in"),
    "tokens_out": ("gen_ai.usage.output_tokens", "tokens.out"),
    "cost_usd": ("cost.usd",),
}


class SpanRecorder(SpanProcessor):
    def __init__(self) -> None:
        cfg = load_config()["tracing"]
        self.dir = Path(resolve_path(cfg["dir"]))
        self.dir.mkdir(parents=True, exist_ok=True)
        self.service = cfg["service"]
        self.capture_body = bool(cfg.get("capture_body", False))

    def on_start(self, span: Span, parent_context=None) -> None:
        pass

    def on_end(self, span: ReadableSpan) -> None:
        attributes = dict(span.attributes or {})
        record = {
            "trace_id": f"{span.context.trace_id:032x}",
            "span_id": f"{span.context.span_id:016x}",
            "parent_span_id": f"{span.parent.span_id:016x}" if span.parent else None,
            "name": span.name,
            "service": self.service,
            "start_ns": span.start_time,
            "end_ns": span.end_time,
            "duration_ms": round((span.end_time - span.start_time) / 1e6, 3),
            "ts": _iso(span.start_time),
            "dt": _iso(span.start_time)[:10],
        }
        for column, keys in _COLUMN_ATTRS.items():
            value = next((attributes[k] for k in keys if k in attributes), None)
            record[column] = value
        record.setdefault("kind", None)
        if record["kind"] is None:
            record["kind"] = "agent" if span.parent is None else "task"
        if self.capture_body:
            record["body"] = next((attributes[k] for k in BODY_KEYS if k in attributes), None)

        date = record["dt"]
        with open(self.dir / f"spans-{date}.jsonl", "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    def shutdown(self) -> None:
        pass

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return True


def _iso(epoch_ns: int) -> str:
    return datetime.fromtimestamp(epoch_ns / 1e9, tz=timezone.utc).isoformat()
