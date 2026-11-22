"""캐스케이드 상단 모델 비교: Jev 저확신 문항을 어느 LLM으로 승급해야 값을 하는가.

Jev는 한 번만 돌리고, 후보 모델별 LLM 라벨러 결과와 조합해 τ별 캐스케이드 정확도·비용을 낸다.
산출: reports/cascade_models.md
사용: python -m eval.cascade_models [--models claude-haiku-5-5 claude-sonnet-5-5 ...]
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor

from common.config import load_config
from eval.label_quality import REPORTS_DIR, load_goldenset, tau_sweep
from eval.label_quality import _accuracy, _macro_f1
from telemetry.labeler import LLMLabeler, create_jev_labeler

DEFAULT_MODELS = ["claude-haiku-5-5", "claude-sonnet-5-5", "claude-opus-5-5"]
MAX_WORKERS = 8


def score(labeler, goldenset: list[dict]) -> dict:
    """label_quality.score_labeler와 같은 지표를 병렬 호출로 계산."""
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        outputs = list(pool.map(lambda g: labeler.label(g["query"], g.get("expected_skill") or None), goldenset))
    intents = [r.tags["intent"].value for r in outputs]
    gold = [g["gold"]["intent"] for g in goldenset]
    return {
        "intent_accuracy": round(_accuracy(intents, gold), 4),
        "intent_macro_f1": round(_macro_f1(intents, gold), 4),
        "cost_per_1k_usd": round(sum(r.cost_usd for r in outputs) / len(outputs) * 1000, 4),
        "avg_latency_ms": round(sum(r.latency_ms for r in outputs) / len(outputs), 1),
        "_intents": intents,
        "_intent_ps": [r.tags["intent"].p for r in outputs],
    }


def run(models: list[str]) -> dict:
    cfg = load_config()
    goldenset = load_goldenset()
    gold = [g["gold"]["intent"] for g in goldenset]
    tau = cfg["labeler"]["cascade"]["tau"]

    jev = score(create_jev_labeler(), goldenset)
    escalated = [i for i, p in enumerate(jev["_intent_ps"]) if p < tau]
    results = {}
    for model in models:
        llm = score(LLMLabeler(model), goldenset)
        sweep = tau_sweep(jev, llm, gold, cfg)
        at_tau = next(row for row in sweep if row["tau"] == tau)
        results[model] = {
            **{k: v for k, v in llm.items() if not k.startswith("_")},
            # 승급 대상(Jev 확신도 < τ)만 놓고 본 정확도 — 승급이 값을 하는지의 직접 지표
            "escalated_accuracy": round(_accuracy([llm["_intents"][i] for i in escalated], [gold[i] for i in escalated]), 4),
            "cascade_accuracy": at_tau["cascade_accuracy"],
            "cascade_cost_per_1k_usd": at_tau["cascade_cost_per_1k_usd"],
            "best_cascade_accuracy": max(row["cascade_accuracy"] for row in sweep),
        }

    jev_escalated_accuracy = round(_accuracy([jev["_intents"][i] for i in escalated], [gold[i] for i in escalated]), 4)
    lines = [
        "# 캐스케이드 상단 모델 비교 (골든셋)",
        "",
        f"- 문항 {len(goldenset)}건, τ = {tau}, 승급 대상 {len(escalated)}건",
        f"- Jev 단독: intent 정확도 {jev['intent_accuracy']:.2%}, ${jev['cost_per_1k_usd']}/1k, "
        f"{jev['avg_latency_ms']}ms. 승급 대상에서의 정확도 {jev_escalated_accuracy:.2%}",
        "",
        "| 상단 모델 | 단독 정확도 | 승급 대상 정확도 | 캐스케이드 정확도 | 캐스케이드 $/1k | 단독 $/1k | 평균 지연(ms) | τ 스윕 최고 |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for model, row in results.items():
        lines.append(
            f"| {model} | {row['intent_accuracy']:.2%} | {row['escalated_accuracy']:.2%} | {row['cascade_accuracy']:.2%} "
            f"| {row['cascade_cost_per_1k_usd']} | {row['cost_per_1k_usd']} | {row['avg_latency_ms']} | {row['best_cascade_accuracy']:.2%} |"
        )
    REPORTS_DIR.mkdir(exist_ok=True)
    (REPORTS_DIR / "cascade_models.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--models", nargs="+", default=DEFAULT_MODELS)
    run(parser.parse_args().models)
