"""스킬 정리(keep / improve / deactivate) 자동 판정.

입력: mart(events, mart_skill_usage_daily) + 최신 run의 스킬별 선택 F1
출력: mart_skill_lifecycle 테이블 + reports/lifecycle.md
deactivate 후보는 끄지 않고 제안만 한다.
사용: python -m eval.lifecycle
"""
from __future__ import annotations

import json

import duckdb
import pandas as pd

from common.config import PROJECT_ROOT, load_config, resolve_path

REPORTS_DIR = PROJECT_ROOT / "reports"


def latest_run_f1(con: duckdb.DuckDBPyConnection) -> dict[str, float]:
    df = con.execute(
        """
        SELECT skill, expected_skill
        FROM events
        WHERE event_type = 'skill_selected'
          AND run_id = (SELECT max(run_id) FROM events WHERE event_type = 'skill_selected')
        """
    ).df()
    f1s: dict[str, float] = {}
    for skill in sorted(set(df["expected_skill"]) - {"none", None}):
        tp = ((df["skill"] == skill) & (df["expected_skill"] == skill)).sum()
        fp = ((df["skill"] == skill) & (df["expected_skill"] != skill)).sum()
        fn = ((df["skill"] != skill) & (df["expected_skill"] == skill)).sum()
        precision = tp / (tp + fp) if tp + fp else 0.0
        recall = tp / (tp + fn) if tp + fn else 0.0
        f1s[skill] = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return f1s


def judge() -> pd.DataFrame:
    cfg = load_config()
    rules = cfg["lifecycle"]
    con = duckdb.connect(resolve_path(cfg["mart"]["duckdb_path"]))

    usage = con.execute(
        f"""
        SELECT skill,
               sum(calls) AS calls,
               avg(success_rate) AS success_rate,
               sum(cost_usd) / nullif(sum(calls), 0) AS cost_per_call
        FROM mart_skill_usage_daily
        WHERE CAST(dt AS DATE) >= current_date - INTERVAL {rules['deactivate']['recent_days']} DAY
        GROUP BY skill
        """
    ).df()
    f1s = latest_run_f1(con)
    median_cost = usage["cost_per_call"].median() if len(usage) else 0.0

    rows = []
    for _, row in usage.iterrows():
        skill = row["skill"]
        if skill == "none":
            continue
        f1 = f1s.get(skill, 0.0)
        reasons = []
        if row["calls"] < rules["deactivate"]["min_calls_recent_days"]:
            verdict = "deactivate"
            reasons.append(f"최근 {rules['deactivate']['recent_days']}일 호출 {int(row['calls'])}건")
        elif median_cost and row["cost_per_call"] > median_cost * rules["deactivate"]["cost_ratio_vs_alternative"]:
            verdict = "deactivate"
            reasons.append(f"1회당 비용이 중앙값의 {row['cost_per_call'] / median_cost:.1f}배")
        elif row["success_rate"] >= rules["keep"]["min_success_rate"] and f1 >= rules["keep"]["min_selection_f1"]:
            verdict = "keep"
            reasons.append(f"성공률 {row['success_rate']:.1%}, 선택 F1 {f1:.2f}")
        else:
            verdict = "improve"
            if row["success_rate"] < rules["keep"]["min_success_rate"]:
                reasons.append(f"성공률 {row['success_rate']:.1%} < {rules['keep']['min_success_rate']:.0%}")
            if f1 < rules["keep"]["min_selection_f1"]:
                reasons.append(f"선택 F1 {f1:.2f} < {rules['keep']['min_selection_f1']}")
        rows.append(
            {
                "skill": skill,
                "verdict": verdict,
                "calls": int(row["calls"]),
                "success_rate": round(float(row["success_rate"]), 4),
                "selection_f1": round(f1, 4),
                "cost_per_call_usd": round(float(row["cost_per_call"] or 0), 6),
                "reasons": " / ".join(reasons),
            }
        )

    result = pd.DataFrame(rows).sort_values("skill").reset_index(drop=True)
    con.register("lifecycle_df", result)
    con.execute("CREATE OR REPLACE TABLE mart_skill_lifecycle AS SELECT * FROM lifecycle_df")
    con.close()
    return result


def main() -> None:
    result = judge()
    REPORTS_DIR.mkdir(exist_ok=True)
    lines = [
        "# 스킬 정리 판정",
        "",
        "| skill | 판정 | 호출 | 성공률 | 선택 F1 | 근거 |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for _, row in result.iterrows():
        lines.append(
            f"| {row['skill']} | **{row['verdict']}** | {row['calls']} "
            f"| {row['success_rate']:.1%} | {row['selection_f1']:.2f} | {row['reasons']} |"
        )
    (REPORTS_DIR / "lifecycle.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(result.to_string(index=False))


if __name__ == "__main__":
    main()
