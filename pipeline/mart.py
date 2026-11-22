"""cleansed → mart 집계 (DuckDB 로컬 개발 루프). 운영 mart는 pipeline/snowflake_elt.py가 Snowflake 안에서 빌드.

생성 테이블:
- events                  cleansed 원본 복제 (분석 편의용)
- mart_skill_usage_daily  스킬별 사용 현황
- mart_selection_eval     run별 스킬 선택 평가 요약
- mart_intent_tags_daily  intent 태그 분포·품질
- mart_labeler_cost       라벨러별 처리량·비용. reports/label_quality.json(골든셋 실측)이 있으면 그 값을 우선
사용: python -m pipeline.mart
"""
from __future__ import annotations

import json

import duckdb
import pandas as pd

from common.config import PROJECT_ROOT, load_config, resolve_path
from pipeline.storage import parquet_glob

USAGE_SQL = """
CREATE OR REPLACE TABLE mart_skill_usage_daily AS
WITH requests AS (
    SELECT dt, skill,
           count(*) AS calls,
           avg(CASE WHEN status = 'ok' THEN 1 ELSE 0 END) AS success_rate,
           quantile_cont(latency_ms, 0.5) AS p50_latency_ms,
           quantile_cont(latency_ms, 0.95) AS p95_latency_ms,
           sum(tokens_in) AS tokens_in,
           sum(tokens_out) AS tokens_out,
           sum(cost_usd) AS cost_usd
    FROM events
    WHERE event_type = 'request_done' AND skill IS NOT NULL
    GROUP BY dt, skill
),
tool_errors AS (
    SELECT dt, skill,
           avg(CASE WHEN status = 'ok' THEN 1 ELSE 0 END) AS tool_success_rate
    FROM events
    WHERE event_type = 'tool_called'
    GROUP BY dt, skill
)
SELECT r.*, t.tool_success_rate
FROM requests r LEFT JOIN tool_errors t USING (dt, skill)
ORDER BY dt, skill
"""

INTENT_SQL = """
CREATE OR REPLACE TABLE mart_intent_tags_daily AS
SELECT dt,
       tag_intent AS intent,
       count(*) AS count,
       avg(tag_intent_p) AS avg_p,
       avg(CASE WHEN tag_intent_p < 0.6 THEN 1 ELSE 0 END) AS low_conf_ratio,
       any_value(taxonomy_version) AS taxonomy_version
FROM events
WHERE event_type = 'request_done' AND tag_intent IS NOT NULL
GROUP BY dt, tag_intent
ORDER BY dt, count DESC
"""

LABELER_SQL = """
CREATE OR REPLACE TABLE mart_labeler_cost AS
SELECT labeler_model AS labeler,
       labeler_source AS source,
       count(*) AS items,
       round({jev_cost} * avg(CASE WHEN labeler_source = 'jev' THEN 1 ELSE 0 END)
             + {llm_cost} * avg(CASE WHEN labeler_source = 'llm' THEN 1 ELSE 0 END), 4) AS est_cost_per_1k_usd,
       avg(tag_intent_p) AS avg_intent_p
FROM events
WHERE labeler_model IS NOT NULL
GROUP BY labeler_model, labeler_source
ORDER BY items DESC
"""


def selection_eval_frame(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    selections = con.execute(
        """
        SELECT run_id, skill_version, skill, expected_skill
        FROM events
        WHERE event_type = 'skill_selected' AND expected_skill IS NOT NULL
        """
    ).df()
    rows = []
    for (run_id, version), group in selections.groupby(["run_id", "skill_version"]):
        selected, expected = group["skill"], group["expected_skill"]
        correct = (selected == expected).mean()
        false_pick = ((selected != "none") & (selected != expected)).mean()
        miss = ((expected != "none") & (selected == "none")).mean()
        negatives = group[expected == "none"]
        negative_fp = (negatives["skill"] != "none").mean() if len(negatives) else 0.0

        skills = sorted(set(expected) - {"none"})
        precisions, recalls = [], []
        for skill in skills:
            tp = ((selected == skill) & (expected == skill)).sum()
            fp = ((selected == skill) & (expected != skill)).sum()
            fn = ((selected != skill) & (expected == skill)).sum()
            precisions.append(tp / (tp + fp) if tp + fp else 0.0)
            recalls.append(tp / (tp + fn) if tp + fn else 0.0)

        rows.append(
            {
                "run_id": run_id,
                "skill_version": version,
                "cases": len(group),
                "accuracy": round(float(correct), 4),
                "macro_precision": round(sum(precisions) / len(precisions), 4) if precisions else 0.0,
                "macro_recall": round(sum(recalls) / len(recalls), 4) if recalls else 0.0,
                "false_pick_rate": round(float(false_pick), 4),
                "miss_rate": round(float(miss), 4),
                "negative_false_pick_rate": round(float(negative_fp), 4),
            }
        )
    return pd.DataFrame(rows)


MEASURED_LABELER_COST = PROJECT_ROOT / "reports/label_quality.json"


def overlay_measured_labeler_cost(con: duckdb.DuckDBPyConnection) -> bool:
    """label-quality가 남긴 골든셋 실측(정확도·$/1k·지연)이 있으면 추정치 대신 그 값으로 교체."""
    if not MEASURED_LABELER_COST.exists():
        return False
    comparison = json.loads(MEASURED_LABELER_COST.read_text(encoding="utf-8")).get("comparison", [])
    if not comparison:
        return False
    con.register("measured_df", pd.DataFrame(comparison))
    con.execute("CREATE OR REPLACE TABLE mart_labeler_cost AS SELECT * FROM measured_df")
    return True


def build() -> dict:
    cfg = load_config()
    glob = parquet_glob(cfg["storage"]["cleansed"])
    con = duckdb.connect(resolve_path(cfg["mart"]["duckdb_path"]))

    con.execute(f"CREATE OR REPLACE TABLE events AS SELECT * FROM read_parquet('{glob}')")
    con.execute(USAGE_SQL)
    con.execute(INTENT_SQL)
    con.execute(
        LABELER_SQL.format(
            jev_cost=cfg["labeler"]["jev"]["cost_per_1k_usd"],
            llm_cost=cfg["labeler"]["llm_cost_per_1k_usd"],
        )
    )
    labeler_cost_measured = overlay_measured_labeler_cost(con)

    eval_df = selection_eval_frame(con)
    con.register("eval_df", eval_df)
    con.execute("CREATE OR REPLACE TABLE mart_selection_eval AS SELECT * FROM eval_df")

    summary = {
        "events": con.execute("SELECT count(*) FROM events").fetchone()[0],
        "runs": len(eval_df),
        "labeler_cost_source": "measured" if labeler_cost_measured else "estimated",
    }
    con.close()
    print(json.dumps(summary, ensure_ascii=False))
    return summary


if __name__ == "__main__":
    build()
