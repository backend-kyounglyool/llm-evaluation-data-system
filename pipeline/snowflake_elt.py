"""Snowflake ELT: S3 raw(JSON) → raw_events(VARIANT) → cleansed_events → mart.

아키텍처 (배치, Airflow가 오케스트레이션):

    Kafka → consumer → S3 raw (JSONL, dt= 파티션)
                         │  storage integration + external stage
                         ▼
    [1] COPY INTO raw_events        -- 증분 (COPY load history가 중복 방지)
    [2] raw → quarantine_events     -- 스키마 위반 격리 (사유 포함)
    [3] raw → cleansed_events MERGE -- 타입 캐스팅·태그 평탄화·event_id dedup
    [4] mart 재생성                  -- usage / intent / labeler (+selection eval)
    [5] 품질 체크                    -- cleansed 0건·quarantine 비율 초과 시 실패

각 단계는 멱등이라 재실행에 안전하다. CLI와 Airflow DAG가 같은 함수를 공유한다.
사용: python -m pipeline.snowflake_elt [--step all|copy|cleanse|mart|check]
"""
from __future__ import annotations

import argparse
import json
import os

from common.config import load_config

VALID_EVENT_TYPES = "('skill_selected','tool_called','request_done')"
VALID_STATUSES = "('ok','error','timeout')"

CREATE_RAW = """
CREATE TABLE IF NOT EXISTS raw_events (
    v VARIANT,
    source_file STRING,
    loaded_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
)
"""

COPY_RAW = """
COPY INTO raw_events (v, source_file)
FROM (SELECT $1, METADATA$FILENAME FROM @{raw_stage})
FILE_FORMAT = (TYPE = JSON)
PATTERN = '.*/part-[^/]*'
ON_ERROR = 'CONTINUE'
"""

CREATE_CLEANSED = """
CREATE TABLE IF NOT EXISTS cleansed_events (
    event_id STRING PRIMARY KEY,
    trace_id STRING,
    ts TIMESTAMP_TZ,
    dt STRING,
    run_id STRING,
    test_id STRING,
    event_type STRING,
    skill STRING,
    expected_skill STRING,
    skill_version STRING,
    tool STRING,
    status STRING,
    latency_ms INTEGER,
    tokens_in INTEGER,
    tokens_out INTEGER,
    cost_usd FLOAT,
    tag_intent STRING,
    tag_intent_p FLOAT,
    tag_out_of_scope STRING,
    tag_out_of_scope_p FLOAT,
    tag_skill_fit STRING,
    tag_skill_fit_p FLOAT,
    tag_complexity STRING,
    tag_complexity_p FLOAT,
    labeler_model STRING,
    labeler_source STRING,
    taxonomy_version STRING,
    labeler_calibrated BOOLEAN
)
"""

CREATE_QUARANTINE = """
CREATE TABLE IF NOT EXISTS quarantine_events (
    row_key STRING,
    v VARIANT,
    source_file STRING,
    reason STRING,
    loaded_at TIMESTAMP_TZ,
    quarantined_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
)
"""

QUARANTINE_MERGE = f"""
MERGE INTO quarantine_events t
USING (
    SELECT MD5(TO_JSON(v)) AS row_key, v, source_file, loaded_at, reason
    FROM (
        SELECT v, source_file, loaded_at,
               CASE
                   WHEN v:event_id IS NULL THEN 'missing event_id'
                   WHEN v:event_type::string NOT IN {VALID_EVENT_TYPES} THEN 'invalid event_type'
                   WHEN v:status::string NOT IN {VALID_STATUSES} THEN 'invalid status'
                   WHEN TRY_TO_TIMESTAMP_TZ(v:ts::string) IS NULL THEN 'invalid ts'
               END AS reason
        FROM raw_events
    )
    WHERE reason IS NOT NULL
) s
ON t.row_key = s.row_key
WHEN NOT MATCHED THEN
    INSERT (row_key, v, source_file, reason, loaded_at)
    VALUES (s.row_key, s.v, s.source_file, s.reason, s.loaded_at)
"""

CLEANSED_MERGE = f"""
MERGE INTO cleansed_events t
USING (
    SELECT
        v:event_id::string          AS event_id,
        v:trace_id::string          AS trace_id,
        TRY_TO_TIMESTAMP_TZ(v:ts::string) AS ts,
        LEFT(v:ts::string, 10)      AS dt,
        v:run_id::string            AS run_id,
        v:test_id::string           AS test_id,
        v:event_type::string        AS event_type,
        v:skill::string             AS skill,
        v:expected_skill::string    AS expected_skill,
        v:skill_version::string     AS skill_version,
        v:tool::string              AS tool,
        v:status::string            AS status,
        v:latency_ms::int           AS latency_ms,
        v:tokens_in::int            AS tokens_in,
        v:tokens_out::int           AS tokens_out,
        v:cost_usd::float           AS cost_usd,
        v:tags:intent:value::string       AS tag_intent,
        v:tags:intent:p::float            AS tag_intent_p,
        v:tags:out_of_scope:value::string AS tag_out_of_scope,
        v:tags:out_of_scope:p::float      AS tag_out_of_scope_p,
        v:tags:skill_fit:value::string    AS tag_skill_fit,
        v:tags:skill_fit:p::float         AS tag_skill_fit_p,
        v:tags:complexity:value::string   AS tag_complexity,
        v:tags:complexity:p::float        AS tag_complexity_p,
        v:labeler:model::string            AS labeler_model,
        v:labeler:source::string           AS labeler_source,
        v:labeler:taxonomy_version::string AS taxonomy_version,
        v:labeler:calibrated::boolean      AS labeler_calibrated
    FROM raw_events
    WHERE v:event_id IS NOT NULL
      AND v:event_type::string IN {VALID_EVENT_TYPES}
      AND v:status::string IN {VALID_STATUSES}
      AND TRY_TO_TIMESTAMP_TZ(v:ts::string) IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY v:event_id ORDER BY loaded_at DESC) = 1
) s
ON t.event_id = s.event_id
WHEN NOT MATCHED THEN INSERT (
    event_id, trace_id, ts, dt, run_id, test_id, event_type, skill, expected_skill,
    skill_version, tool, status, latency_ms, tokens_in, tokens_out, cost_usd,
    tag_intent, tag_intent_p, tag_out_of_scope, tag_out_of_scope_p,
    tag_skill_fit, tag_skill_fit_p, tag_complexity, tag_complexity_p,
    labeler_model, labeler_source, taxonomy_version, labeler_calibrated
) VALUES (
    s.event_id, s.trace_id, s.ts, s.dt, s.run_id, s.test_id, s.event_type, s.skill,
    s.expected_skill, s.skill_version, s.tool, s.status, s.latency_ms, s.tokens_in,
    s.tokens_out, s.cost_usd,
    s.tag_intent, s.tag_intent_p, s.tag_out_of_scope, s.tag_out_of_scope_p,
    s.tag_skill_fit, s.tag_skill_fit_p, s.tag_complexity, s.tag_complexity_p,
    s.labeler_model, s.labeler_source, s.taxonomy_version, s.labeler_calibrated
)
"""

MART_USAGE = """
CREATE OR REPLACE TABLE mart_skill_usage_daily AS
WITH requests AS (
    SELECT dt, skill,
           count(*) AS calls,
           avg(IFF(status = 'ok', 1, 0)) AS success_rate,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY latency_ms) AS p50_latency_ms,
           percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms) AS p95_latency_ms,
           sum(tokens_in) AS tokens_in,
           sum(tokens_out) AS tokens_out,
           sum(cost_usd) AS cost_usd
    FROM cleansed_events
    WHERE event_type = 'request_done' AND skill IS NOT NULL
    GROUP BY dt, skill
),
tool_errors AS (
    SELECT dt, skill,
           avg(IFF(status = 'ok', 1, 0)) AS tool_success_rate
    FROM cleansed_events
    WHERE event_type = 'tool_called'
    GROUP BY dt, skill
)
SELECT r.*, t.tool_success_rate
FROM requests r LEFT JOIN tool_errors t USING (dt, skill)
"""

MART_INTENT = """
CREATE OR REPLACE TABLE mart_intent_tags_daily AS
SELECT dt,
       tag_intent AS intent,
       count(*) AS count,
       avg(tag_intent_p) AS avg_p,
       avg(IFF(tag_intent_p < 0.6, 1, 0)) AS low_conf_ratio,
       max(taxonomy_version) AS taxonomy_version
FROM cleansed_events
WHERE event_type = 'request_done' AND tag_intent IS NOT NULL
GROUP BY dt, tag_intent
"""

MART_LABELER = """
CREATE OR REPLACE TABLE mart_labeler_cost AS
SELECT labeler_model AS labeler,
       labeler_source AS source,
       count(*) AS items,
       round({jev_cost} * avg(IFF(labeler_source = 'jev', 1, 0))
             + {llm_cost} * avg(IFF(labeler_source = 'llm', 1, 0)), 4) AS est_cost_per_1k_usd,
       avg(tag_intent_p) AS avg_intent_p
FROM cleansed_events
WHERE labeler_model IS NOT NULL
GROUP BY labeler_model, labeler_source
"""

MAX_QUARANTINE_RATIO = 0.05


def get_conn():
    """세 가지 인증 경로:
    - SNOWFLAKE_AUTHENTICATOR=externalbrowser : SSO(구글 로그인 등). 브라우저가 열림.
      macOS는 토큰이 keychain에 캐시되어 재연결 시 재로그인 생략.
    - SNOWFLAKE_PRIVATE_KEY_PATH : key-pair 인증 (Airflow 등 헤드리스 배치용)
    - SNOWFLAKE_PASSWORD : 서비스 유저 비밀번호
    """
    import snowflake.connector

    cfg = load_config()["mart"]["snowflake"]
    if not cfg.get("account"):
        raise SystemExit("SNOWFLAKE_ACCOUNT 환경변수를 설정하세요 (<org>-<account>)")
    if not os.environ.get("SNOWFLAKE_USER"):
        raise SystemExit("SNOWFLAKE_USER 환경변수를 설정하세요")

    kwargs = {
        "account": cfg["account"],
        "user": os.environ["SNOWFLAKE_USER"],
        "warehouse": cfg["warehouse"],
    }
    if cfg.get("role"):
        kwargs["role"] = cfg["role"]
    authenticator = os.environ.get("SNOWFLAKE_AUTHENTICATOR")
    key_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH")
    if authenticator:
        kwargs["authenticator"] = authenticator
        kwargs["client_store_temporary_credential"] = True  # SSO 토큰 캐시
    elif key_path:
        kwargs["private_key_file"] = key_path
    elif os.environ.get("SNOWFLAKE_PASSWORD"):
        kwargs["password"] = os.environ["SNOWFLAKE_PASSWORD"]
    else:
        raise SystemExit(
            "인증 수단 없음 — SNOWFLAKE_AUTHENTICATOR=externalbrowser(SSO) 또는 "
            "SNOWFLAKE_PRIVATE_KEY_PATH 또는 SNOWFLAKE_PASSWORD를 설정하세요"
        )
    conn = snowflake.connector.connect(**kwargs)
    # DB·스키마는 snowflake_setup.sql이 만든다 — 적재 롤에는 계정 수준 CREATE 권한이 없다
    conn.cursor().execute(f"USE SCHEMA {cfg['database']}.{cfg['schema']}")
    return conn


def copy_raw(conn) -> int:
    """[1] S3 external stage → raw_events. COPY load history가 증분을 보장."""
    cfg = load_config()["mart"]["snowflake"]
    cursor = conn.cursor()
    cursor.execute(CREATE_RAW)
    cursor.execute(COPY_RAW.format(raw_stage=cfg["raw_stage"]))
    # 전부 스킵되면 "Copy executed with 0 files processed." 단일 메시지 행이 반환됨
    rows = cursor.fetchall()
    loaded = sum(1 for row in rows if len(row) > 1 and str(row[1]).upper() == "LOADED")
    print(json.dumps({"step": "copy_raw", "files_loaded": loaded}))
    return loaded


def build_cleansed(conn) -> dict:
    """[2][3] 격리 → 정제 MERGE. 둘 다 멱등."""
    cursor = conn.cursor()
    cursor.execute(CREATE_CLEANSED)
    cursor.execute(CREATE_QUARANTINE)
    cursor.execute(QUARANTINE_MERGE)
    quarantined = cursor.fetchone()[0]
    cursor.execute(CLEANSED_MERGE)
    inserted = cursor.fetchone()[0]
    result = {"step": "build_cleansed", "inserted": inserted, "quarantined_new": quarantined}
    print(json.dumps(result))
    return result


def build_mart(conn) -> None:
    """[4] mart 재생성 + 선택 평가(pandas) 적재."""
    labeler_cfg = load_config()["labeler"]
    cursor = conn.cursor()
    cursor.execute(MART_USAGE)
    cursor.execute(MART_INTENT)
    cursor.execute(
        MART_LABELER.format(
            jev_cost=labeler_cfg["jev"]["cost_per_1k_usd"],
            llm_cost=labeler_cfg["llm_cost_per_1k_usd"],
        )
    )
    _write_selection_eval(conn)
    print(json.dumps({"step": "build_mart", "tables": 4}))


def quality_check(conn) -> dict:
    """[5] cleansed 0건이거나 quarantine 비율 초과면 예외 → Airflow task 실패."""
    cursor = conn.cursor()
    cursor.execute("SELECT count(*) FROM cleansed_events")
    cleansed = cursor.fetchone()[0]
    cursor.execute("SELECT count(*) FROM quarantine_events")
    quarantined = cursor.fetchone()[0]
    cursor.execute("SELECT count(*) FROM raw_events")
    raw = cursor.fetchone()[0]

    ratio = quarantined / raw if raw else 0.0
    result = {"step": "quality_check", "raw": raw, "cleansed": cleansed,
              "quarantined": quarantined, "quarantine_ratio": round(ratio, 4)}
    print(json.dumps(result))
    if raw > 0 and cleansed == 0:
        raise RuntimeError("품질 체크 실패: raw는 있는데 cleansed가 0건")
    if ratio > MAX_QUARANTINE_RATIO:
        raise RuntimeError(f"품질 체크 실패: quarantine 비율 {ratio:.1%} > {MAX_QUARANTINE_RATIO:.0%}")
    return result


def _write_selection_eval(conn) -> None:
    import duckdb
    from snowflake.connector.pandas_tools import write_pandas

    from pipeline.mart import selection_eval_frame

    selections = conn.cursor().execute(
        "SELECT run_id, skill_version, skill, expected_skill "
        "FROM cleansed_events WHERE event_type = 'skill_selected' AND expected_skill IS NOT NULL"
    ).fetch_pandas_all()
    selections.columns = [c.lower() for c in selections.columns]

    memdb = duckdb.connect()
    memdb.register("sel", selections)
    memdb.execute(
        "CREATE TABLE events AS SELECT run_id, skill_version, skill, expected_skill, "
        "'skill_selected' AS event_type FROM sel"
    )
    frame = selection_eval_frame(memdb)
    if frame.empty:
        return
    frame.columns = [c.upper() for c in frame.columns]
    write_pandas(conn, frame, "MART_SELECTION_EVAL", auto_create_table=True, overwrite=True)


def run(step: str = "all") -> None:
    conn = get_conn()
    try:
        if step in ("all", "copy"):
            copy_raw(conn)
        if step in ("all", "cleanse"):
            build_cleansed(conn)
        if step in ("all", "mart"):
            build_mart(conn)
        if step in ("all", "check"):
            quality_check(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--step", default="all", choices=["all", "copy", "cleanse", "mart", "check"])
    args = parser.parse_args()
    run(args.step)
