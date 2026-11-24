# Airflow 배치 ELT

```
Kafka ──▶ Flink 잡(상시) ──▶ S3 raw (JSON, dt= 파티션)   ※ [drain_kafka_to_s3]는 커밋 대기
                                      │ storage integration (snowflake_setup.sql)
                                      ▼
          [copy_raw_to_snowflake]  COPY INTO raw_events (VARIANT)
                                      ▼
          [raw_to_cleansed]        quarantine MERGE → cleansed MERGE (dedup·캐스팅·평탄화)
                                      ▼
          [build_mart]             mart 4종 + selection eval
                                      ▼
          [quality_check]          cleansed 0건 / quarantine > 5% → DAG 실패
```

매시 정각 실행, `max_active_runs=1`, 전 단계 멱등(재시도·백필 안전).

## 로컬 기동 (docker compose, CeleryExecutor)

```bash
make airflow-up     # infra-up + Airflow(postgres, redis, 스케줄러, 워커, 웹서버, Flower)
docker compose --profile airflow exec airflow-scheduler airflow dags unpause skill_eval_elt
```

- 웹 UI http://localhost:8082 (admin / `AIRFLOW_ADMIN_PASSWORD`, 기본 admin), Flower http://localhost:5555
- 프로젝트 코드는 컨테이너에 `/opt/skill-eval`로 마운트된다(`PYTHONPATH`). 이미지는 `airflow/Dockerfile`
- 자격증명: `.env`의 `SNOWFLAKE_USER`, `SNOWFLAKE_PRIVATE_KEY_PATH`(키 파일이 컨테이너에 마운트됨), `AWS_*`
- 규모 조정: `docker compose --profile airflow up -d --no-recreate --scale airflow-worker=2 --scale airflow-scheduler=2`

### 모니터링 (Grafana `skill-eval · Airflow`)

| 항목 | 출처 |
| --- | --- |
| 스케줄러 수 | 메타데이터 DB `job` 테이블 (최근 60초 하트비트) — Grafana Postgres 데이터소스 |
| 워커 수 | Flower `flower_worker_online` |
| 슬롯 수 | StatsD `executor.open_slots`/`running_tasks`/`queued_tasks`, `pool.*_slots.<pool>` |
| 스케줄된 수 | StatsD `pool.scheduled_slots.<pool>`, `scheduler.tasks.executable`/`starving` |

StatsD는 `statsd-exporter`가 받아 Prometheus 형식으로 바꾼다(이름 매핑: `monitoring/statsd-mapping.yml`).

사전 조건:
1. `.env` — `SKILL_EVAL_RAW_URI=s3://<bucket>/raw`, `SNOWFLAKE_ACCOUNT` 입력
2. `pipeline/snowflake_setup.sql` — storage integration + `SKILL_EVAL_RAW_STAGE` 생성 (1회)
3. Kafka·Flink 기동 + 적재 잡 제출 (`make infra-up`)

## 운영 배포

- **MWAA(AWS 관리형)** 또는 EC2/EKS Airflow — DAG 파일과 프로젝트 패키지만 배포하면 동일 동작
- 자격증명은 Airflow Connections/Secrets Manager로 이관 (env var 직접 주입 대신)
- 스케줄 조정: 트래픽이 적으면 `@daily`로 낮춰 웨어하우스 크레딧 절약
