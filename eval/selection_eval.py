"""스킬 선택 오프라인 평가. Runner·telemetry 없이 선택기만 직접 돌려 채점한다.

지표: accuracy, 스킬별 precision/recall/F1, 오선택(false_pick), 누락(miss),
negative 오탐률, confusion matrix.
사용: python -m eval.selection_eval [--split all|train|val] [--report]
"""
from __future__ import annotations

import argparse
import json
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from agent.runner import load_testset
from agent.selector import NONE_SKILL, Skill, create_selector
from common.config import PROJECT_ROOT

REPORTS_DIR = PROJECT_ROOT / "reports"
# 실 LLM 선택기는 건당 1~2초라 순차 실행이면 평가 한 번에 몇 분이 걸린다
MAX_WORKERS = 8


def evaluate(skills: dict[str, Skill], testset_path: str = "testsets/testset_v1.jsonl",
             split: str = "all") -> dict:
    selector = create_selector(skills)
    cases = load_testset(testset_path, split)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        selections = list(pool.map(lambda case: selector.select(case["query"]).skill, cases))
    records = [
        {
            "id": case["id"],
            "query": case["query"],
            "expected": case["expected"],
            "selected": selected,
            "case_type": case["case_type"],
        }
        for case, selected in zip(cases, selections)
    ]

    n = len(records)
    accuracy = sum(r["selected"] == r["expected"] for r in records) / n
    false_picks = [r for r in records if r["selected"] != NONE_SKILL and r["selected"] != r["expected"]]
    misses = [r for r in records if r["expected"] != NONE_SKILL and r["selected"] == NONE_SKILL]
    negatives = [r for r in records if r["expected"] == NONE_SKILL]
    negative_fp = sum(r["selected"] != NONE_SKILL for r in negatives) / len(negatives) if negatives else 0.0

    per_skill = {}
    for skill in sorted({r["expected"] for r in records} - {NONE_SKILL}):
        tp = sum(r["selected"] == skill and r["expected"] == skill for r in records)
        fp = sum(r["selected"] == skill and r["expected"] != skill for r in records)
        fn = sum(r["selected"] != skill and r["expected"] == skill for r in records)
        precision = tp / (tp + fp) if tp + fp else 0.0
        recall = tp / (tp + fn) if tp + fn else 0.0
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
        per_skill[skill] = {
            "precision": round(precision, 4),
            "recall": round(recall, 4),
            "f1": round(f1, 4),
            "support": tp + fn,
        }

    confusion = Counter((r["expected"], r["selected"]) for r in records)
    return {
        "split": split,
        "cases": n,
        "accuracy": round(accuracy, 4),
        "false_pick_rate": round(len(false_picks) / n, 4),
        "miss_rate": round(len(misses) / n, 4),
        "negative_false_pick_rate": round(negative_fp, 4),
        "per_skill": per_skill,
        "confusion": {f"{e}→{s}": c for (e, s), c in sorted(confusion.items())},
        "failures": [r for r in records if r["selected"] != r["expected"]],
    }


def render_report(metrics: dict, title: str) -> str:
    lines = [
        f"# {title}",
        "",
        f"- cases: {metrics['cases']} (split={metrics['split']})",
        f"- accuracy: **{metrics['accuracy']:.2%}**",
        f"- 오선택(false pick): {metrics['false_pick_rate']:.2%} / 누락(miss): {metrics['miss_rate']:.2%}",
        f"- negative 오탐률: {metrics['negative_false_pick_rate']:.2%}",
        "",
        "## 스킬별 지표",
        "",
        "| skill | precision | recall | f1 | support |",
        "| --- | --- | --- | --- | --- |",
    ]
    for skill, m in metrics["per_skill"].items():
        lines.append(f"| {skill} | {m['precision']:.2f} | {m['recall']:.2f} | {m['f1']:.2f} | {m['support']} |")

    lines += ["", "## Confusion (expected → selected, 오답만)", "", "| expected → selected | count |", "| --- | --- |"]
    for pair, count in metrics["confusion"].items():
        expected, selected = pair.split("→")
        if expected != selected:
            lines.append(f"| {pair} | {count} |")
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--testset", default="testsets/testset_v1.jsonl")
    parser.add_argument("--split", default="all", choices=["all", "train", "val"])
    parser.add_argument("--report", action="store_true", help="reports/에 markdown 저장")
    args = parser.parse_args()

    from agent.selector import load_skills

    skills = load_skills()
    version = next(iter(skills.values())).version if skills else "v?"
    metrics = evaluate(skills, args.testset, args.split)

    if args.report:
        REPORTS_DIR.mkdir(exist_ok=True)
        testset_name = Path(args.testset).stem
        path = REPORTS_DIR / f"selection_eval_{version}_{args.split}_{testset_name}.md"
        path.write_text(render_report(metrics, f"스킬 선택 평가 ({version}, {args.split})"), encoding="utf-8")
        print(f"report → {path}")

    printable = {k: v for k, v in metrics.items() if k != "failures"}
    print(json.dumps(printable, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
