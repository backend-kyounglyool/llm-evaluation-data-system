"""라벨 품질 평가: 골든셋 기준 Jev vs LLM vs 캐스케이드 비교 + τ 스윕 + 보정.

Jev는 config labeler.jev.provider(mock | http=TypeSafe AI 실연동)를 따른다.

산출:
- reports/label_quality.md (비교표, τ 스윕, reliability)
- mart 테이블 mart_labeler_cost 갱신 (실측 기준)
사용: python -m eval.label_quality
"""
from __future__ import annotations

import json
from pathlib import Path

from common.config import PROJECT_ROOT, load_config, resolve_path
from telemetry.labeler import CascadeLabeler, LLMLabeler, create_jev_labeler

REPORTS_DIR = PROJECT_ROOT / "reports"
TAUS = [round(0.5 + 0.05 * i, 2) for i in range(10)]  # 0.5 ~ 0.95


def load_goldenset() -> list[dict]:
    path = PROJECT_ROOT / "testsets/goldenset.jsonl"
    if not path.exists():
        from testsets.build_goldenset import build

        build()
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def score_labeler(name: str, labeler, goldenset: list[dict]) -> dict:
    outputs = []
    total_cost = total_latency = 0.0
    for item in goldenset:
        result = labeler.label(item["query"], item.get("expected_skill") or None)
        outputs.append(result)
        total_cost += result.cost_usd
        total_latency += result.latency_ms

    intents = [r.tags["intent"].value for r in outputs]
    intent_ps = [r.tags["intent"].p for r in outputs]
    gold_intents = [g["gold"]["intent"] for g in goldenset]
    oos = [bool(r.tags["out_of_scope"].value) for r in outputs]
    gold_oos = [g["gold"]["out_of_scope"] for g in goldenset]

    return {
        "labeler": name,
        "items": len(goldenset),
        "intent_accuracy": round(_accuracy(intents, gold_intents), 4),
        "intent_macro_f1": round(_macro_f1(intents, gold_intents), 4),
        "oos_accuracy": round(_accuracy(oos, gold_oos), 4),
        "cost_per_1k_usd": round(total_cost / len(goldenset) * 1000, 4),
        "avg_latency_ms": round(total_latency / len(goldenset), 1),
        "escalation_rate": round(
            sum(1 for r in outputs if r.labeler.source == "llm") / len(outputs), 3
        ),
        "_intents": intents,
        "_intent_ps": intent_ps,
    }


def _accuracy(pred: list, gold: list) -> float:
    return sum(p == g for p, g in zip(pred, gold)) / len(gold)


def _macro_f1(pred: list, gold: list) -> float:
    labels = sorted(set(gold))
    f1s = []
    for label in labels:
        tp = sum(p == label and g == label for p, g in zip(pred, gold))
        fp = sum(p == label and g != label for p, g in zip(pred, gold))
        fn = sum(p != label and g == label for p, g in zip(pred, gold))
        precision = tp / (tp + fp) if tp + fp else 0.0
        recall = tp / (tp + fn) if tp + fn else 0.0
        f1s.append(2 * precision * recall / (precision + recall) if precision + recall else 0.0)
    return sum(f1s) / len(f1s)


def tau_sweep(jev: dict, llm: dict, gold_intents: list[str], cfg: dict) -> list[dict]:
    """τ별: Jev 유지분 정확도(precision 프록시), 승급률, 캐스케이드 정확도·비용.

    비용은 골든셋 실측(usage 기반 $/1k)을 쓴다. mock 라벨러는 config 가정치가 그대로 실측이 된다.
    """
    rows = []
    jev_cost = jev["cost_per_1k_usd"]
    llm_cost = llm["cost_per_1k_usd"]
    for tau in TAUS:
        kept = [i for i, p in enumerate(jev["_intent_ps"]) if p >= tau]
        escalated = [i for i in range(len(gold_intents)) if i not in set(kept)]
        kept_acc = (
            sum(jev["_intents"][i] == gold_intents[i] for i in kept) / len(kept) if kept else 0.0
        )
        cascade_correct = sum(jev["_intents"][i] == gold_intents[i] for i in kept) + sum(
            llm["_intents"][i] == gold_intents[i] for i in escalated
        )
        escalation_rate = len(escalated) / len(gold_intents)
        rows.append(
            {
                "tau": tau,
                "jev_kept_coverage": round(len(kept) / len(gold_intents), 3),
                "jev_kept_accuracy": round(kept_acc, 4),
                "cascade_accuracy": round(cascade_correct / len(gold_intents), 4),
                "escalation_rate": round(escalation_rate, 3),
                "cascade_cost_per_1k_usd": round(jev_cost + llm_cost * escalation_rate, 4),
            }
        )
    return rows


def expected_calibration_error(ps: list[float], correct: list[bool], bins: int = 10) -> tuple[float, list[dict]]:
    buckets = [{"lo": i / bins, "hi": (i + 1) / bins, "n": 0, "conf": 0.0, "acc": 0.0} for i in range(bins)]
    for p, ok in zip(ps, correct):
        idx = min(int(p * bins), bins - 1)
        buckets[idx]["n"] += 1
        buckets[idx]["conf"] += p
        buckets[idx]["acc"] += ok
    ece, total = 0.0, len(ps)
    rows = []
    for bucket in buckets:
        if bucket["n"] == 0:
            continue
        conf = bucket["conf"] / bucket["n"]
        acc = bucket["acc"] / bucket["n"]
        ece += bucket["n"] / total * abs(acc - conf)
        rows.append({"bin": f"{bucket['lo']:.1f}–{bucket['hi']:.1f}", "n": bucket["n"],
                     "avg_conf": round(conf, 3), "accuracy": round(acc, 3)})
    return round(ece, 4), rows


def run() -> dict:
    cfg = load_config()
    goldenset = load_goldenset()
    gold_intents = [g["gold"]["intent"] for g in goldenset]

    jev = score_labeler("jev", create_jev_labeler(), goldenset)
    llm = score_labeler("llm", LLMLabeler(), goldenset)
    cascade = score_labeler("cascade", CascadeLabeler(), goldenset)

    sweep = tau_sweep(jev, llm, gold_intents, cfg)
    target_precision = cfg["labeler"]["cascade"].get("target_precision", 0.9)
    recommended = next((row["tau"] for row in sweep if row["jev_kept_accuracy"] >= target_precision), None)
    if recommended is None:
        # 목표 precision을 만족하는 τ가 없으면(미보정 라벨러) 정확도 상위 1%p 내 최저 비용 τ
        best_acc = max(row["cascade_accuracy"] for row in sweep)
        candidates = [row for row in sweep if row["cascade_accuracy"] >= best_acc - 0.01]
        recommended = min(candidates, key=lambda row: row["cascade_cost_per_1k_usd"])["tau"]

    jev_correct = [p == g for p, g in zip(jev["_intents"], gold_intents)]
    ece, reliability = expected_calibration_error(jev["_intent_ps"], jev_correct)

    comparison = [
        {k: v for k, v in row.items() if not k.startswith("_")} for row in (jev, llm, cascade)
    ]
    result = {
        "comparison": comparison,
        "tau_sweep": sweep,
        "recommended_tau": recommended,
        "jev_intent_ece": ece,
    }

    REPORTS_DIR.mkdir(exist_ok=True)
    (REPORTS_DIR / "label_quality.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (REPORTS_DIR / "label_quality.md").write_text(
        render_report(comparison, sweep, recommended, ece, reliability), encoding="utf-8"
    )
    _write_mart(comparison)

    print(json.dumps({k: v for k, v in result.items() if k != "tau_sweep"}, ensure_ascii=False, indent=2))
    return result


def render_report(comparison, sweep, recommended, ece, reliability) -> str:
    lines = [
        "# 라벨 품질 평가 (골든셋)",
        "",
        "## 라벨러 비교",
        "",
        "| labeler | intent acc | intent macro F1 | OOS acc | $/1k | avg latency(ms) | 승급률 |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in comparison:
        lines.append(
            f"| {row['labeler']} | {row['intent_accuracy']:.2%} | {row['intent_macro_f1']:.2f} "
            f"| {row['oos_accuracy']:.2%} | {row['cost_per_1k_usd']} | {row['avg_latency_ms']} "
            f"| {row['escalation_rate']:.1%} |"
        )
    lines += [
        "",
        f"## τ 스윕 (권장 τ = {recommended})",
        "",
        "| τ | Jev 유지율 | 유지분 정확도 | 캐스케이드 정확도 | 승급률 | $/1k |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for row in sweep:
        lines.append(
            f"| {row['tau']} | {row['jev_kept_coverage']:.0%} | {row['jev_kept_accuracy']:.2%} "
            f"| {row['cascade_accuracy']:.2%} | {row['escalation_rate']:.0%} "
            f"| {row['cascade_cost_per_1k_usd']} |"
        )
    lines += [
        "",
        f"## 보정 (Jev intent, ECE = {ece})",
        "",
        "| 확신도 구간 | n | 평균 확신도 | 정확도 |",
        "| --- | --- | --- | --- |",
    ]
    for row in reliability:
        lines.append(f"| {row['bin']} | {row['n']} | {row['avg_conf']} | {row['accuracy']} |")
    return "\n".join(lines) + "\n"


def _write_mart(comparison: list[dict]) -> None:
    import duckdb
    import pandas as pd

    cfg = load_config()["mart"]
    con = duckdb.connect(resolve_path(cfg["duckdb_path"]))
    df = pd.DataFrame(comparison)
    con.register("labeler_df", df)
    con.execute("CREATE OR REPLACE TABLE mart_labeler_cost AS SELECT * FROM labeler_df")
    con.close()


if __name__ == "__main__":
    run()
