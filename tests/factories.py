"""테스트용 이벤트 생성기."""
from __future__ import annotations


def make_event(**overrides) -> dict:
    """스키마를 만족하는 최소 이벤트. 테스트는 관심 있는 필드만 덮어쓴다."""
    event = {
        "event_id": "e-1",
        "trace_id": "t-1",
        "ts": "2026-11-20T01:02:03+00:00",
        "run_id": "eval-2026-11-20-01",
        "event_type": "request_done",
        "skill": "subway_arrival",
        "status": "ok",
        "latency_ms": 120,
        "tokens_in": 100,
        "tokens_out": 20,
        "cost_usd": 0.001,
    }
    event.update(overrides)
    return event
