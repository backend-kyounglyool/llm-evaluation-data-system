"""Flink 스트리밍 적재: Kafka(agent-events) → raw 레이어(JSON 원본, dt= 파티션).

Flink SQL 잡 하나가 상시 실행되며 메시지를 변형 없이 그대로 raw에 쓴다.
파일은 롤링 후 다음 체크포인트에서 커밋된다(exactly-once 파일 싱크). 체크포인트는
취소·재기동 후에도 보존되고, 재제출 시 마지막 체크포인트에서 복원하므로 raw에 중복이
생기지 않는다. 복원할 수 없을 때(잡 정의 변경 등)만 컨슈머 그룹 오프셋부터 재개하며,
이때 생길 수 있는 중복은 cleansed 단계의 event_id 중복 제거가 흡수한다.

사용:
  python -m pipeline.flink_ingest submit   # 잡 제출 (이미 실행 중이면 생략)
  python -m pipeline.flink_ingest status
  python -m pipeline.flink_ingest wait     # 다음 체크포인트 커밋까지 대기
  python -m pipeline.flink_ingest render   # 제출될 SQL 출력
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import requests

from common.config import PROJECT_ROOT, load_config, resolve_path

JOBS_DIR = PROJECT_ROOT / ".flink" / "jobs"
SQL_FILE = "raw_ingest.sql"
# docker-compose.yml의 볼륨 마운트와 일치해야 한다
CONTAINER_JOBS_DIR = "/opt/skill-eval/jobs"
CONTAINER_DATA_DIR = "/opt/skill-eval/data"
CONTAINER_CHECKPOINT_DIR = "/opt/flink/checkpoints"
HOST_DATA_DIR = PROJECT_ROOT / "data"
JOBMANAGER_SERVICE = "flink-jobmanager"
KAFKA_SERVICE = "kafka"

REST_READY_TIMEOUT_S = 90
ROLL_CHECK_INTERVAL_S = 5
SUBMIT_TIMEOUT_S = 180
JOB_START_TIMEOUT_S = 60

SQL_TEMPLATE = """\
SET 'pipeline.name' = '{job_name}';
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '{checkpoint_interval_s}s';
SET 'parallelism.default' = '{parallelism}';
SET 'state.checkpoints.dir' = 'file://{checkpoint_dir}';
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';
{restore_statement}

CREATE TEMPORARY TABLE agent_events_source (
  line STRING
) WITH (
  'connector' = 'kafka',
  'topic' = '{topic}',
  'properties.bootstrap.servers' = '{bootstrap_servers}',
  'properties.group.id' = '{group_id}',
  'properties.auto.offset.reset' = 'earliest',
  'scan.startup.mode' = 'group-offsets',
  'format' = 'raw'
);

CREATE TEMPORARY TABLE raw_events_sink (
  line STRING,
  dt STRING
) PARTITIONED BY (dt) WITH (
  'connector' = 'filesystem',
  'path' = '{raw_path}',
  'format' = 'raw',
  'sink.rolling-policy.rollover-interval' = '{checkpoint_interval_s}s',
  'sink.rolling-policy.inactivity-interval' = '{checkpoint_interval_s}s',
  'sink.rolling-policy.check-interval' = '{roll_check_interval_s}s'
);

INSERT INTO raw_events_sink
SELECT line, COALESCE(SUBSTRING(JSON_VALUE(line, '$.ts') FROM 1 FOR 10), 'unknown')
FROM agent_events_source;
"""


def uses_flink(cfg: dict | None = None) -> bool:
    """Flink는 Kafka 소스가 전제다. file transport면 엔진 설정과 무관하게 Python consumer를 쓴다."""
    cfg = cfg or load_config()
    engine = cfg.get("ingest", {}).get("engine", "python")
    return engine == "flink" and cfg["transport"]["backend"] == "kafka"


def _flink_cfg() -> dict:
    return load_config()["ingest"]["flink"]


def _sink_path(raw: str) -> str:
    """config의 raw 경로를 Flink 컨테이너가 보는 경로로 변환."""
    resolved = resolve_path(raw)
    if resolved.startswith("s3://"):
        return resolved
    try:
        relative = Path(resolved).relative_to(HOST_DATA_DIR)
    except ValueError:
        raise ValueError(
            f"로컬 raw 경로는 {HOST_DATA_DIR} 아래여야 Flink 컨테이너에 마운트된다: {resolved}"
        ) from None
    return f"file://{CONTAINER_DATA_DIR}/{relative.as_posix()}"


def _checkpoint_dir() -> str:
    """잡 이름별 체크포인트 디렉터리 (컨테이너 경로). 잡 ID가 바뀌어도 이전 실행분을 찾을 수 있다."""
    return f"{CONTAINER_CHECKPOINT_DIR}/{_flink_cfg()['job_name']}"


def latest_checkpoint_path() -> str | None:
    """보존된 체크포인트 중 가장 최근 것의 컨테이너 경로. 없으면 None."""
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", JOBMANAGER_SERVICE, "sh", "-c",
         f"ls -dt {_checkpoint_dir()}/*/chk-*/_metadata 2>/dev/null | head -1"],
        cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=30,
    )
    metadata = result.stdout.strip()
    return metadata.removesuffix("/_metadata") if metadata else None


def render_sql(restore_from: str | None = None) -> str:
    cfg = load_config()
    flink = _flink_cfg()
    restore_statement = f"SET 'execution.savepoint.path' = 'file://{restore_from}';" if restore_from else ""
    return SQL_TEMPLATE.format(
        checkpoint_dir=_checkpoint_dir(),
        restore_statement=restore_statement,
        job_name=flink["job_name"],
        checkpoint_interval_s=flink["checkpoint_interval_s"],
        roll_check_interval_s=ROLL_CHECK_INTERVAL_S,
        parallelism=flink["parallelism"],
        topic=cfg["transport"]["kafka"]["topic"],
        bootstrap_servers=flink["bootstrap_servers"],
        group_id=flink["group_id"],
        raw_path=_sink_path(cfg["storage"]["raw"]),
    )


def _rest_url() -> str:
    # 컨테이너 안(Airflow 등)에서는 localhost가 아니므로 환경변수로 덮어쓴다
    return os.environ.get("FLINK_REST_URL") or _flink_cfg()["rest_url"]


def _get(path: str) -> dict:
    response = requests.get(f"{_rest_url()}{path}", timeout=10)
    response.raise_for_status()
    return response.json()


def _wait_for_rest() -> None:
    deadline = time.monotonic() + REST_READY_TIMEOUT_S
    while True:
        try:
            # 태스크매니저가 붙기 전에 제출하면 슬롯 부족으로 잡이 재시작을 반복한다
            if _get("/overview")["taskmanagers"] > 0:
                return
        except requests.RequestException:
            pass
        if time.monotonic() > deadline:
            raise RuntimeError(
                f"Flink 클러스터에 연결할 수 없음 ({_rest_url()}). 'make infra-up'으로 기동했는지 확인."
            )
        time.sleep(2)


ACTIVE_STATES = ("RUNNING", "CREATED", "INITIALIZING", "RESTARTING")


def running_job_id() -> str | None:
    job_name = _flink_cfg()["job_name"]
    for job in _get("/jobs/overview")["jobs"]:
        if job["name"] == job_name and job["state"] in ACTIVE_STATES:
            return job["jid"]
    return None


def _ensure_topic() -> None:
    """토픽이 없으면 만든다. 새 브로커에서는 첫 produce 전까지 토픽이 없어 Flink 소스가 기동에 실패한다."""
    topic = load_config()["transport"]["kafka"]["topic"]
    subprocess.run(
        ["docker", "compose", "exec", "-T", KAFKA_SERVICE, "/opt/kafka/bin/kafka-topics.sh",
         "--bootstrap-server", "localhost:9092", "--create", "--if-not-exists", "--topic", topic],
        cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=60, check=True,
    )


def _cancel_active_jobs() -> None:
    """RUNNING에 못 오른 채 재시작을 반복하는 잡을 정리한다 (같은 이름의 잡이 겹쳐 뜨지 않게)."""
    job_name = _flink_cfg()["job_name"]
    for job in _get("/jobs/overview")["jobs"]:
        if job["name"] == job_name and job["state"] in ACTIVE_STATES:
            requests.patch(f"{_rest_url()}/jobs/{job['jid']}", params={"mode": "cancel"}, timeout=10).raise_for_status()


def _run_sql(sql: str) -> str | None:
    """SQL을 제출하고 잡이 RUNNING에 도달하면 잡 ID를, 기동에 실패하면 None을 돌려준다."""
    JOBS_DIR.mkdir(parents=True, exist_ok=True)
    (JOBS_DIR / SQL_FILE).write_text(sql, encoding="utf-8")
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", JOBMANAGER_SERVICE,
         "/opt/flink/bin/sql-client.sh", "-f", f"{CONTAINER_JOBS_DIR}/{SQL_FILE}"],
        cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=SUBMIT_TIMEOUT_S,
    )
    # sql-client는 문장이 실패해도 종료 코드 0을 줄 수 있어 REST로 실제 기동 여부를 확인한다
    job_name = _flink_cfg()["job_name"]
    deadline = time.monotonic() + JOB_START_TIMEOUT_S
    while time.monotonic() < deadline:
        states = {job["jid"]: job["state"] for job in _get("/jobs/overview")["jobs"] if job["name"] == job_name}
        running = [jid for jid, state in states.items() if state == "RUNNING"]
        if running:
            return running[0]
        if not any(state in ACTIVE_STATES for state in states.values()):
            break
        time.sleep(2)
    print(f"Flink 잡 기동 실패:\n{result.stdout[-3000:]}\n{result.stderr[-3000:]}", file=sys.stderr)
    _cancel_active_jobs()
    return None


def submit() -> str:
    _wait_for_rest()
    job_id = running_job_id()
    if job_id:
        print(json.dumps({"job_id": job_id, "submitted": False, "reason": "이미 실행 중"}, ensure_ascii=False))
        return job_id

    _ensure_topic()
    restore_from = latest_checkpoint_path()
    job_id = _run_sql(render_sql(restore_from))
    if not job_id and restore_from:
        print(f"체크포인트 복원 실패({restore_from}) — 컨슈머 그룹 오프셋부터 재개", file=sys.stderr)
        restore_from = None
        job_id = _run_sql(render_sql())
    if not job_id:
        raise RuntimeError("Flink 잡 제출 실패")
    print(json.dumps({"job_id": job_id, "submitted": True, "restored_from": restore_from}))
    return job_id


def _latest_completed_checkpoint(job_id: str) -> dict | None:
    return _get(f"/jobs/{job_id}/checkpoints")["latest"]["completed"]


def await_commit(timeout_s: int | None = None) -> dict:
    """호출 시점까지 읽은 메시지가 raw에 커밋될 때까지 대기.

    행 포맷 싱크는 체크포인트마다 파일을 닫지 않는다. 롤링 정책이 파일을 닫은 뒤
    (rollover-interval + check-interval 이내) 시작된 체크포인트가 완료돼야 파일이 보인다.
    """
    job_id = running_job_id()
    if not job_id:
        raise RuntimeError("Flink 적재 잡이 실행 중이 아님. 'make flink-submit'으로 제출.")

    interval_s = _flink_cfg()["checkpoint_interval_s"]
    timeout_s = timeout_s or interval_s * 4 + 60
    rolled_by_ms = (time.time() + interval_s + ROLL_CHECK_INTERVAL_S) * 1000
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        checkpoint = _latest_completed_checkpoint(job_id)
        if checkpoint and checkpoint["trigger_timestamp"] >= rolled_by_ms:
            summary = {"engine": "flink", "job_id": job_id, "checkpoint_id": checkpoint["id"]}
            print(json.dumps(summary))
            return summary
        time.sleep(2)
    raise TimeoutError(f"{timeout_s}초 안에 Flink 체크포인트가 완료되지 않음 (job {job_id})")


def status() -> dict:
    job_id = running_job_id()
    summary: dict = {"engine_enabled": uses_flink(), "job_id": job_id}
    if job_id:
        checkpoint = _latest_completed_checkpoint(job_id)
        summary["latest_checkpoint_id"] = checkpoint["id"] if checkpoint else None
    print(json.dumps(summary))
    return summary


def main() -> None:
    command = sys.argv[1] if len(sys.argv) > 1 else "status"
    if command == "render":
        print(render_sql())
        return
    if not uses_flink():
        print("ingest.engine이 flink가 아니거나 transport가 kafka가 아님 — 건너뜀")
        return
    commands = {"submit": submit, "status": status, "wait": await_commit}
    if command not in commands:
        raise SystemExit(f"알 수 없는 명령: {command} (submit | status | wait | render)")
    commands[command]()


if __name__ == "__main__":
    main()
