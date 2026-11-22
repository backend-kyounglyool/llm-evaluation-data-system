"""skill-eval 배치 ELT DAG.

    drain_kafka_to_s3 → copy_raw_to_snowflake → raw_to_cleansed → build_mart → quality_check

- 각 태스크는 pipeline.snowflake_elt / pipeline.consumer의 함수를 그대로 호출한다
  (Airflow 환경에 `pip install -e <repo>[kafka,snowflake,s3]` 로 프로젝트 설치 필요).
- 모든 단계가 멱등: COPY는 load history, cleansed/quarantine은 MERGE 키로 중복을 막으므로
  재시도·백필에 안전하다.
- 자격증명: SNOWFLAKE_USER/PASSWORD, AWS_* 는 Airflow 환경변수 또는
  Connection(env var 주입)으로 공급. config.yaml의 storage.raw는 s3://<bucket>/raw.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task

DEFAULT_ARGS = {
    "owner": "skill-eval",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="skill_eval_elt",
    schedule="0 * * * *",  # 매시 정각 배치
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,  # MERGE 동시 실행 방지
    default_args=DEFAULT_ARGS,
    tags=["skill-eval", "elt", "snowflake"],
)
def skill_eval_elt():
    @task
    def drain_kafka_to_s3() -> dict:
        """Kafka agent-events → S3 raw (dt= 파티션).

        ingest.engine: flink이면 상시 잡이 적재하므로 raw 커밋만 기다리고(잡이 죽어 있으면 실패),
        python이면 여기서 배치 drain한다.
        """
        from pipeline.consumer import ingest

        return ingest()

    @task
    def copy_raw_to_snowflake() -> int:
        from pipeline.snowflake_elt import copy_raw, get_conn

        conn = get_conn()
        try:
            return copy_raw(conn)
        finally:
            conn.close()

    @task
    def raw_to_cleansed() -> dict:
        from pipeline.snowflake_elt import build_cleansed, get_conn

        conn = get_conn()
        try:
            return build_cleansed(conn)
        finally:
            conn.close()

    @task
    def build_mart() -> None:
        from pipeline.snowflake_elt import build_mart as _build_mart, get_conn

        conn = get_conn()
        try:
            _build_mart(conn)
        finally:
            conn.close()

    @task
    def quality_check() -> dict:
        """cleansed 0건 또는 quarantine 비율 5% 초과 시 예외 → DAG 실패로 알림."""
        from pipeline.snowflake_elt import get_conn, quality_check as _quality_check

        conn = get_conn()
        try:
            return _quality_check(conn)
        finally:
            conn.close()

    drain_kafka_to_s3() >> copy_raw_to_snowflake() >> raw_to_cleansed() >> build_mart() >> quality_check()


skill_eval_elt()
