"""3단 라우팅: 경량 모델 → (확신도 낮음) Jev → (확신도 낮음) 생성형 LLM.

골든셋에서 정확도·단계별 분담률·비용을 측정하고
"LLM 호출 X% 감소, 정확도 Y% 유지, 1천 건당 $Z" 요약을 출력한다.
사용: python -m distill.router  (먼저 python -m distill.baseline 실행)
"""
from __future__ import annotations

import json
import pickle

from common.config import PROJECT_ROOT, load_config
from distill.baseline import MODEL_PATH
from eval.label_quality import _accuracy, load_goldenset
from telemetry.labeler import LLMLabeler, create_jev_labeler

REPORTS_DIR = PROJECT_ROOT / "reports"


def route_all() -> dict:
    cfg = load_config()
    router_cfg = cfg["distill"]["router"]
    llm_cost = cfg["labeler"]["llm_cost_per_1k_usd"] / 1000

    if not MODEL_PATH.exists():
        raise SystemExit("models/baseline_intent.pkl 없음 — 먼저 python -m distill.baseline 실행")
    with open(MODEL_PATH, "rb") as f:
        light = pickle.load(f)

    jev, llm = create_jev_labeler(), LLMLabeler()
    goldenset = load_goldenset()
    gold = [g["gold"]["intent"] for g in goldenset]

    predictions, stages, total_cost = [], [], 0.0
    for item in goldenset:
        query = item["query"]
        proba = light.predict_proba([query])[0]
        best_idx = proba.argmax()
        if proba[best_idx] >= router_cfg["light_tau"]:
            predictions.append(light.classes_[best_idx])
            stages.append("light")
            continue
        jev_result = jev.label(query)
        total_cost += jev_result.cost_usd  # http 모드는 usage 기반 실측, mock은 가정치
        if jev_result.tags["intent"].p >= router_cfg["jev_tau"]:
            predictions.append(str(jev_result.tags["intent"].value))
            stages.append("jev")
            continue
        llm_result = llm.label(query)
        total_cost += llm_cost
        predictions.append(str(llm_result.tags["intent"].value))
        stages.append("llm")

    n = len(goldenset)
    llm_only_accuracy = _accuracy(
        [str(llm.label(g["query"]).tags["intent"].value) for g in goldenset], gold
    )
    stage_share = {stage: round(stages.count(stage) / n, 3) for stage in ("light", "jev", "llm")}
    result = {
        "items": n,
        "router_accuracy": round(_accuracy(predictions, gold), 4),
        "llm_only_accuracy": round(llm_only_accuracy, 4),
        "stage_share": stage_share,
        "llm_call_reduction": round(1 - stage_share["llm"], 3),
        "cost_per_1k_usd": round(total_cost / n * 1000, 4),
        "llm_only_cost_per_1k_usd": round(llm_cost * 1000, 4),
    }

    REPORTS_DIR.mkdir(exist_ok=True)
    summary = (
        f"LLM 호출 {result['llm_call_reduction']:.0%} 감소, "
        f"정확도 {result['router_accuracy']:.1%} (LLM 단독 {result['llm_only_accuracy']:.1%}), "
        f"1천 건당 ${result['cost_per_1k_usd']} (LLM 단독 ${result['llm_only_cost_per_1k_usd']})"
    )
    (REPORTS_DIR / "router.md").write_text(
        f"# 3단 라우팅 결과\n\n{summary}\n\n```json\n"
        + json.dumps(result, ensure_ascii=False, indent=2)
        + "\n```\n",
        encoding="utf-8",
    )
    print(summary)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return result


if __name__ == "__main__":
    route_all()
