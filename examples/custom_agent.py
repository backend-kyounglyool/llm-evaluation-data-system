"""자기 에이전트/서버에 계측 SDK를 붙이는 최소 예시.

데모 Runner 없이, 임의의 에이전트 루프에 span 5종(agent/workflow/thinking/llm/tool)을
붙이면 트레이스(워터폴·플레임)와 이벤트(지표·태그, 본문 제거)가 모두 적재된다.

사용: python -m examples.custom_agent "강남역 지하철 언제 와?"
이후: make pipeline && make dashboard  (트레이스 탭에서 확인)
"""
from __future__ import annotations

import sys
import time

from telemetry.instrument import (
    flush,
    init,
    llm_span,
    request_span,
    selection_span,
    thinking_span,
    tool_span,
)


def my_llm_call(prompt: str) -> tuple[str, int, int]:
    """실제로는 Claude API 등 호출. 여기서는 모의 응답."""
    time.sleep(0.08)
    return "subway_arrival", len(prompt) // 3, 12


def my_tool(station: str) -> dict:
    time.sleep(0.03)
    return {"station": station, "eta_min": 3}


def handle(query: str) -> None:
    with request_span(query, run_id="live") as request:
        # 1) 스킬/도구 선택 워크플로
        with selection_span(query, run_id="live") as selection:
            with thinking_span("intent-analysis", note="후보 스킬 검토"):
                time.sleep(0.01)
            with llm_span(model="claude-haiku-5-5", name="llm.route") as llm:
                skill, tokens_in, tokens_out = my_llm_call(query)
                llm.set_usage(tokens_in, tokens_out, cost_usd=tokens_in * 3e-6 + tokens_out * 15e-6)
            selection.finish(skill=skill, tokens_in=tokens_in, tokens_out=tokens_out)

        # 2) 툴 실행 (예외는 status=error/timeout으로 자동 기록)
        with tool_span("subway_arrival", skill=skill, run_id="live") as holder:
            my_tool("강남")

        request.finish(skill=skill, status=holder["status"],
                       tokens_in=tokens_in, tokens_out=tokens_out)


if __name__ == "__main__":
    init(service="my-custom-agent")
    handle(sys.argv[1] if len(sys.argv) > 1 else "강남역 지하철 언제 와?")
    flush()
    print("트레이스·이벤트 기록 완료 → make pipeline && make dashboard")
