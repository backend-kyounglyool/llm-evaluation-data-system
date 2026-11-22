"""Kafka 메시지(이벤트) 스키마. 노션 설계 문서 3장과 1:1 대응.

본문(query 등)은 이 스키마에 존재하지 않는다 — 계측 단계에서 제거된 뒤의
지표·태그만 담는 것이 핵심 설계다.
"""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

EventType = Literal["skill_selected", "tool_called", "request_done"]
Status = Literal["ok", "error", "timeout"]


class Tag(BaseModel):
    value: str | bool | int | float
    p: float = Field(ge=0.0, le=1.0)


class LabelerInfo(BaseModel):
    model: str
    taxonomy_version: str
    calibrated: bool = False
    source: Literal["jev", "llm", "light", "none"] = "none"


class AgentEvent(BaseModel):
    event_id: str
    trace_id: str
    ts: str  # ISO8601 UTC
    run_id: str  # "eval-YYYY-MM-DD-NN" | "live"
    test_id: str | None = None
    event_type: EventType
    skill: str | None = None  # 선택된 스킬. 미선택(none 판단)이면 None
    expected_skill: str | None = None  # 오프라인 평가용 정답. live에는 없음
    skill_version: str = "v1"
    tool: str | None = None  # tool_called 이벤트의 대상 툴
    status: Status = "ok"
    latency_ms: int = 0
    tokens_in: int = 0
    tokens_out: int = 0
    cost_usd: float = 0.0
    tags: dict[str, Tag] = Field(default_factory=dict)
    labeler: LabelerInfo | None = None

    def to_json(self) -> str:
        return self.model_dump_json()


FORBIDDEN_BODY_FIELDS = {"query", "request", "prompt", "response", "completion", "body"}


def assert_no_body(payload: dict) -> None:
    """이벤트가 전송 경계를 넘기 전 본문 필드가 섞여 들어오지 않았는지 방어."""
    leaked = FORBIDDEN_BODY_FIELDS & payload.keys()
    if leaked:
        raise ValueError(f"본문 필드가 이벤트에 포함됨: {leaked}")
