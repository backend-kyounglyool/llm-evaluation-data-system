PY := .venv/bin/python

.PHONY: test install-boot setup infra-up infra-down airflow-up grafana-secrets flink-submit flink-status run-eval pipeline eval-report improve label-quality lifecycle distill router dashboard demo clean

setup:
	python3 -m venv .venv
	.venv/bin/pip install -U pip
	.venv/bin/pip install -e .

test:
	$(PY) -m pytest tests -q

infra-up: grafana-secrets
	docker compose up -d
	$(MAKE) flink-submit

# (macOS 서버) 로그인 시 전체 스택을 자동 기동하는 launchd 에이전트 설치 — docs/macmini.md
install-boot:
	./scripts/install_boot.sh

# Grafana Snowflake 데이터소스용 키 파일 생성 (.grafana/secrets/, git 제외)
grafana-secrets:
	$(PY) -m monitoring.grafana_secrets

# Kafka → raw 스트리밍 적재 잡 제출 (ingest.engine: flink일 때만 동작, 실행 중이면 생략)
flink-submit:
	$(PY) -m pipeline.flink_ingest submit

flink-status:
	$(PY) -m pipeline.flink_ingest status

infra-down:
	docker compose --profile airflow down

# Airflow(CeleryExecutor: 스케줄러·워커·Flower) 기동. UI http://localhost:8082
airflow-up: infra-up
	docker compose --profile airflow up -d --build

run-eval:
	$(PY) -m agent.runner --testset testsets/testset_v1.jsonl

pipeline:
	$(PY) -m pipeline.consumer
	$(PY) -m pipeline.cleanse
	$(PY) -m pipeline.mart

eval-report:
	$(PY) -m eval.selection_eval --split all --report

improve:
	$(PY) -m eval.improve_loop --apply

label-quality:
	$(PY) -m testsets.build_goldenset
	$(PY) -m eval.label_quality

lifecycle:
	$(PY) -m eval.lifecycle

distill:
	$(PY) -m distill.baseline

router:
	$(PY) -m distill.router

dashboard:
	.venv/bin/streamlit run dashboard/app.py

# Snowflake ELT 수동 1회 실행 (평시엔 Airflow DAG skill_eval_elt가 매시 수행)
# 필요: S3 raw + snowflake_setup.sql 구성, SNOWFLAKE_USER/PASSWORD 환경변수
snowflake:
	$(PY) -m pipeline.snowflake_elt

# 전체 데모: 트래픽 생성 → 파이프라인 → 평가 → 개선 → 재평가 → 라벨 품질 → 증류 → 라우팅
# (make는 중복 prerequisite를 1회만 실행하므로 순차 레시피로 호출한다)
demo:
	$(MAKE) run-eval
	$(MAKE) pipeline
	$(MAKE) eval-report
	$(MAKE) improve
	$(MAKE) run-eval
	$(MAKE) pipeline
	$(MAKE) lifecycle
	$(MAKE) label-quality
	$(MAKE) distill
	$(MAKE) router
	@echo "완료. reports/ 와 'make dashboard' 확인"

clean:
	rm -rf data reports models
