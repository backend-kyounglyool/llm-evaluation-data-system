"""테스트셋 Runner — 계측 SDK(telemetry.instrument) 사용 예시이자 평가용 트래픽 생성기.

질의마다: request(agent) → skill_selected(workflow) 안에서 thinking·llm span →
선택된 스킬의 툴 호출(tool span). 라벨링은 processor가 labeler span으로 기록.
자기 앱에 붙이는 최소 예시는 examples/custom_agent.py 참고.

사용: python -m agent.runner [--testset PATH] [--split train|val|all] [--run-id ID]
"""
from __future__ import annotations

import argparse
import json
import random
import time
from datetime import datetime, timezone

from common.config import load_config, resolve_path
from agent.selector import NONE_SKILL, create_selector, load_skills
from agent.tools.server import TOOL_FUNCTIONS
from telemetry.instrument import (
    flush,
    init,
    llm_span,
    request_span,
    selection_span,
    thinking_span,
    tool_span,
)

_TOOL_CALLS = {
    "subway_arrival": lambda: TOOL_FUNCTIONS["subway_arrival"]("강남"),
    "subway_route": lambda: TOOL_FUNCTIONS["subway_route"]("강남", "홍대입구"),
    "weather_now": lambda: TOOL_FUNCTIONS["weather_now"](),
    "weather_forecast": lambda: TOOL_FUNCTIONS["weather_forecast"](),
    "performance_search": lambda: TOOL_FUNCTIONS["performance_search"](),
    "flaky_tool": lambda: TOOL_FUNCTIONS["flaky_tool"]("ping"),
}

_SIMULATED_LATENCY_MS = {"default": (15, 60), "flaky_tool": (80, 250)}


def load_testset(path: str, split: str = "all") -> list[dict]:
    cases = []
    with open(resolve_path(path), encoding="utf-8") as f:
        for line in f:
            if line.strip():
                case = json.loads(line)
                if split == "all" or case.get("split") == split:
                    cases.append(case)
    return cases


def _sleep_ms(low: float, high: float) -> None:
    time.sleep(random.uniform(low, high) / 1000)


def run_case(selector, skills, case: dict, run_id: str, skill_version: str, mock: bool) -> dict:
    query = case["query"]
    expected = case.get("expected", NONE_SKILL)
    common = {"run_id": run_id, "test_id": case["id"], "skill_version": skill_version}
    overall_status = "ok"

    with request_span(query, **common) as request:
        with selection_span(query, **common) as sel:
            with thinking_span("skill-candidate-scoring", note="SKILL.md 설명 대조"):
                if mock:
                    _sleep_ms(8, 25)
            with llm_span(model=load_config()["llm"]["model"], name="llm.skill_selection") as llm:
                if mock:
                    _sleep_ms(40, 130)
                selection = selector.select(query)
                llm.set_usage(selection.tokens_in, selection.tokens_out, selection.cost_usd)
            sel.finish(
                skill=selection.skill,
                expected_skill=expected,
                tokens_in=selection.tokens_in,
                tokens_out=selection.tokens_out,
                cost_usd=selection.cost_usd,
            )

        if selection.skill != NONE_SKILL and selection.skill in skills:
            for tool in skills[selection.skill].tools:
                with tool_span(tool, skill=selection.skill, **common) as holder:
                    low, high = _SIMULATED_LATENCY_MS.get(tool, _SIMULATED_LATENCY_MS["default"])
                    _sleep_ms(low, high)
                    _TOOL_CALLS[tool]()
                if holder["status"] != "ok":
                    overall_status = holder["status"]

        request.finish(
            skill=selection.skill,
            expected_skill=expected,
            status=overall_status,
            tokens_in=selection.tokens_in,
            tokens_out=selection.tokens_out,
            cost_usd=selection.cost_usd,
        )

    return {"id": case["id"], "selected": selection.skill, "expected": expected}


def run(testset_path: str, split: str = "all", run_id: str | None = None,
        skills_dir: str | None = None) -> dict:
    config = load_config()
    run_id = run_id or f"eval-{datetime.now(timezone.utc).strftime('%Y-%m-%d-%H%M%S')}"
    skills = load_skills(skills_dir)
    selector = create_selector(skills)
    init()
    cases = load_testset(testset_path, split)
    mock = config["llm"]["provider"] != "claude"

    versions = [int(s.version.lstrip("v")) for s in skills.values() if s.version.lstrip("v").isdigit()]
    skill_version = f"v{max(versions)}" if versions else config["selector"]["skill_version"]

    random.seed(42)
    results = [
        run_case(selector, skills, case, run_id, skill_version, mock) for case in cases
    ]
    flush()

    correct = sum(1 for r in results if r["selected"] == r["expected"])
    summary = {
        "run_id": run_id,
        "cases": len(results),
        "accuracy": round(correct / len(results), 4) if results else 0.0,
    }
    print(json.dumps(summary, ensure_ascii=False))
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--testset", default="testsets/testset_v1.jsonl")
    parser.add_argument("--split", default="all", choices=["all", "train", "val"])
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--skills-dir", default=None)
    args = parser.parse_args()
    run(args.testset, args.split, args.run_id, args.skills_dir)
