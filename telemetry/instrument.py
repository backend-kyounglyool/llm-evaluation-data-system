"""계측 SDK 파사드 — 어떤 Python 에이전트/서버에도 이 모듈만으로 붙인다.

사용 (예시는 examples/custom_agent.py):

    from telemetry.instrument import init, request_span, thinking_span, llm_span, tool_span

    init(service="my-agent")
    with request_span(query, run_id="live") as req:
        with thinking_span("plan"):
            ...
        with llm_span(model="claude-haiku-5-5") as llm:
            ...; llm.set_usage(tokens_in, tokens_out, cost_usd)
        with tool_span("my_tool", skill="my_skill") as tool:
            ...
        req.finish(skill="my_skill", status="ok", tokens_in=..., tokens_out=..., cost_usd=...)

기록 경로는 두 갈래:
- trace 저장소(span 전체, 워터폴·플레임 그래프용) — SpanRecorder,
  tracing.otlp_endpoint가 있으면 Tempo로도 전송 — BodySafeOtlpProcessor
- 이벤트 스트림(본문 제거된 지표·태그, Kafka/file) — BodyStrippingSpanProcessor
"""
from __future__ import annotations

from contextlib import contextmanager

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider

_tracer: trace.Tracer | None = None


def init(service: str | None = None, labeler=None) -> trace.Tracer:
    """TracerProvider 구성. recorder를 먼저 등록해 (capture_body=true일 때)
    이벤트 프로세서의 본문 삭제보다 앞서 trace에 본문이 기록되게 한다."""
    global _tracer
    from telemetry.span_processor import BodyStrippingSpanProcessor
    from telemetry.span_recorder import SpanRecorder
    from common.transport import create_producer
    from common.config import load_config

    tracing = load_config()["tracing"]
    if service:
        tracing["service"] = service
    provider = TracerProvider(resource=Resource.create({"service.name": tracing["service"]}))
    provider.add_span_processor(SpanRecorder())
    provider.add_span_processor(BodyStrippingSpanProcessor(create_producer(), labeler))
    if tracing.get("otlp_endpoint"):
        from telemetry.otlp_export import BodySafeOtlpProcessor

        provider.add_span_processor(
            BodySafeOtlpProcessor(tracing["otlp_endpoint"], bool(tracing.get("capture_body", False)))
        )
    trace.set_tracer_provider(provider)
    _tracer = trace.get_tracer("skill-eval-sdk")
    return _tracer


def get_tracer() -> trace.Tracer:
    if _tracer is None:
        raise RuntimeError("telemetry.instrument.init()을 먼저 호출하세요")
    return _tracer


class RequestHandle:
    def __init__(self, span: trace.Span) -> None:
        self.span = span

    def finish(self, skill: str, status: str = "ok", expected_skill: str | None = None,
               tokens_in: int = 0, tokens_out: int = 0, cost_usd: float = 0.0) -> None:
        self.span.set_attributes(
            {
                "skill": skill,
                "status": status,
                "tokens.in": tokens_in,
                "tokens.out": tokens_out,
                "cost.usd": cost_usd,
                **({"expected.skill": expected_skill} if expected_skill is not None else {}),
            }
        )


class LLMHandle:
    def __init__(self, span: trace.Span) -> None:
        self.span = span

    def set_usage(self, tokens_in: int, tokens_out: int, cost_usd: float = 0.0) -> None:
        self.span.set_attributes(
            {
                "gen_ai.usage.input_tokens": tokens_in,
                "gen_ai.usage.output_tokens": tokens_out,
                "cost.usd": cost_usd,
            }
        )


@contextmanager
def request_span(query: str, run_id: str = "live", test_id: str | None = None,
                 skill_version: str = "v1", name: str = "request"):
    attrs = {
        "ai.kind": "agent",
        "event.type": "request_done",
        "request.body": query,
        "run.id": run_id,
        "skill.version": skill_version,
    }
    if test_id:
        attrs["test.id"] = test_id
    with get_tracer().start_as_current_span(name, attributes=attrs) as span:
        yield RequestHandle(span)


@contextmanager
def selection_span(query: str, run_id: str = "live", test_id: str | None = None,
                   skill_version: str = "v1"):
    """스킬 선택 워크플로 span. 종료 전 handle.finish(skill=...)로 결과 기록."""
    attrs = {
        "ai.kind": "workflow",
        "event.type": "skill_selected",
        "request.body": query,
        "run.id": run_id,
        "skill.version": skill_version,
    }
    if test_id:
        attrs["test.id"] = test_id
    with get_tracer().start_as_current_span("skill_selected", attributes=attrs) as span:
        yield RequestHandle(span)


@contextmanager
def thinking_span(name: str, note: str | None = None):
    attrs = {"ai.kind": "thinking"}
    if note:
        attrs["thinking.note"] = note
    with get_tracer().start_as_current_span(name, attributes=attrs) as span:
        yield span


@contextmanager
def llm_span(model: str, name: str = "llm.call", prompt: str | None = None):
    attrs = {"ai.kind": "llm", "gen_ai.request.model": model}
    if prompt is not None:
        attrs["request.body"] = prompt
    with get_tracer().start_as_current_span(name, attributes=attrs) as span:
        yield LLMHandle(span)


@contextmanager
def tool_span(tool: str, skill: str | None = None, run_id: str = "live",
              test_id: str | None = None, skill_version: str = "v1"):
    """툴 호출 span. 예외는 status로 변환해 기록 후 삼킨다(호출부가 결과로 판단)."""
    attrs = {
        "ai.kind": "tool",
        "event.type": "tool_called",
        "tool": tool,
        "run.id": run_id,
        "skill.version": skill_version,
    }
    if skill:
        attrs["skill"] = skill
    if test_id:
        attrs["test.id"] = test_id
    with get_tracer().start_as_current_span(f"tool.{tool}", attributes=attrs) as span:
        holder = {"status": "ok"}
        try:
            yield holder
        except TimeoutError:
            holder["status"] = "timeout"
        except Exception:
            holder["status"] = "error"
        finally:
            span.set_attribute("status", holder["status"])


def flush() -> None:
    trace.get_tracer_provider().force_flush()
