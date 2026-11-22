# skill-eval: 에이전트 스킬 로그 적재 + 평가 시스템

직접 만든 툴·스킬로 에이전트 트래픽을 생성하고, **본문 없이** 지표와 태그만 적재한 뒤,
스킬 선택 정확도를 자동 평가하고 스킬 설명을 자동 개선해 PR로 제안하는 시스템.

```
Runner/내 앱 → Agent(스킬 선택) → MCP Tools(FastMCP)
                   │
             OTel Span Processor ── Jev 라벨러(캐스케이드) ── 본문 제거
                   │
             Kafka(agent-events)
                   │
             Flink 스트리밍 잡 (상시) ─▶ S3 raw(JSON, dt= 파티션)
                   │  ┌─────────────── Airflow 배치 (skill_eval_elt, 매시) ───────────────┐
                   └─▶│ S3 raw ─storage integration─▶ Snowflake raw_events(VARIANT)        │
                      │   ─MERGE─▶ cleansed_events(+quarantine) ─▶ mart 5종 ─▶ 품질 체크   │
                      └────────────────────────────────────────────────────────────────────┘
                   │                                                   │
                   └── (로컬 개발 루프: file → DuckDB) ── 대시보드 ◄───┬┘
                                                                       └ 평가·개선·정리 Job → GitHub PR
```

핵심 설계: **원문(query)은 프로세스 밖으로 나가지 않는다.**
Span Processor가 본문으로 라벨링한 뒤 본문 속성을 삭제하고, 이벤트에는 태그·확률·지표만 담는다.
`common/events.py`의 `assert_no_body()`가 전송 경계에서 이를 강제한다.

## 트레이스 (Datadog AI Observability 스타일)

기록은 두 갈래로 나뉜다:

| 경로 | 내용 | 본문 |
| --- | --- | --- |
| **trace 저장소** (`data/traces/*.jsonl`) | span 트리 전체: agent → workflow → thinking / llm / tool / labeler, 각 span의 지연·토큰(in/out)·비용·모델·상태 | 기본 제거. `tracing.capture_body: true`(로컬 디버깅)일 때만 유지 |
| **이벤트 스트림** (file/Kafka) | 지표·태그만 (mart·대시보드 집계용) | **항상 제거** (capture_body와 무관) |

대시보드 **트레이스 탭**에서 run 필터 → 트레이스 선택 → **워터폴 + 플레임 그래프**로
LLM 호출·생각(thinking)·툴콜·라벨링 구간과 토큰·비용을 span 단위로 본다.
토큰 속성은 OTel GenAI semantic convention(`gen_ai.usage.*`, `gen_ai.request.model`)을 따른다.

## Grafana 모니터링

`make infra-up`이 Prometheus·Tempo·Grafana까지 띄운다. Grafana는 http://localhost:3001
(초기 계정 admin / `GRAFANA_ADMIN_PASSWORD`, 기본 admin), `skill-eval` 폴더에 대시보드 3개가 프로비저닝된다.

| 대시보드 | 데이터소스 | 내용 |
| --- | --- | --- |
| 파이프라인 상태 | Prometheus, CloudWatch, Tempo | Kafka 유입·컨슈머 랙, Flink 체크포인트·처리량, OTel Collector, S3 raw 크기, 최근 트레이스 |
| Airflow | Prometheus(StatsD·Flower), Airflow DB | 스케줄러 수, 워커 수, executor·풀 슬롯, 스케줄된 태스크, DAG 실행 시간·이력 (`make airflow-up` 필요) |
| 평가 지표 (Snowflake mart) | Snowflake | 적재 건수, run별 선택 정확도, 스킬 사용 현황, intent 분포, 라벨러 비용 |

- Snowflake 데이터소스는 커뮤니티 플러그인(미서명)이며 `.env`의 `SNOWFLAKE_USER`·`SNOWFLAKE_PRIVATE_KEY_PATH`로 접속한다.
  mart 대시보드는 자동 새로고침이 꺼져 있다 — 열 때마다 웨어하우스가 깨어난다.
- span 트리는 `tracing.otlp_endpoint`(기본 Tempo)로도 전송된다. 본문 정책은 trace 저장소와 동일(`capture_body`).
- `mart_skill_lifecycle`은 DuckDB에만 있어 Grafana에는 없다 (Streamlit 대시보드에서 확인).
- 설정은 `monitoring/` 아래(prometheus.yml, tempo.yaml, grafana/). 포트는 모두 127.0.0.1에만 바인딩.

## 내 앱에 붙이기 (계측 SDK)

데모 Runner는 SDK 사용 예시일 뿐이다. 임의의 Python 에이전트/서버에는
`telemetry/instrument.py`만 붙이면 된다 (최소 예시: `examples/custom_agent.py`):

```python
from telemetry.instrument import init, request_span, selection_span, thinking_span, llm_span, tool_span

init(service="my-agent")
with request_span(query, run_id="live") as req:
    with selection_span(query, run_id="live") as sel:
        with thinking_span("plan"): ...
        with llm_span(model="claude-haiku-5-5") as llm:
            ...; llm.set_usage(tokens_in, tokens_out, cost_usd)
        sel.finish(skill=skill, tokens_in=..., tokens_out=...)
    with tool_span("my_tool", skill=skill, run_id="live") as holder:
        ...  # 예외는 status=error/timeout으로 자동 기록
    req.finish(skill=skill, status=holder["status"], ...)
```

이 다섯 span만 쓰면 트레이스(워터폴·플레임)와 이벤트(라벨링 포함)가 모두 자동 적재된다.
### 다른 프로세스·다른 언어의 앱 (OTel Collector 경유)

이 SDK를 프로세스 안에 넣을 수 없는 앱(별도 서비스, NestJS·Spring 등)은 OTel span을
Collector(`http://localhost:4318/v1/traces`, gRPC는 4317)로 보내면 같은 파이프라인에 합류한다.

```
앱 ──OTLP──▶ otel-collector ──▶ Kafka otel-spans ──▶ otel-bridge ──▶ Kafka agent-events ──▶ (Flink 이후 동일)
```

`event.type`이 있는 span만 이벤트가 된다. 브리지가 읽는 속성:

| 속성 | 뜻 |
| --- | --- |
| `event.type` | `request_done` · `skill_selected` · `tool_called` (필수) |
| `run.id` | 트래픽 구분. 없으면 `live` |
| `skill`, `tool`, `skill.version`, `expected.skill`, `test.id` | 이벤트의 같은 이름 필드 |
| `status` | `ok` · `error` · `timeout`. 그 밖의 값은 `error` |
| `tokens.in`, `tokens.out`, `cost.usd` | 사용량·비용 |

지연 시간은 span의 시작·종료 시각에서 계산한다. `ai.kind`·`gen_ai.*`만 있는 span(llm·thinking)은 이벤트가 되지 않는다.
이 경로에는 라벨링(tags)이 없다.

Collector는 같은 span을 Tempo에도 보내므로 Grafana의 트레이스 화면에서 바로 볼 수 있다.
본문(`request.body`)을 실어 보내면 **Tempo에만** 남는다(로컬 디스크, 7일 보관). Kafka로 가는 쪽은 Collector가 항상 지우므로
S3·Snowflake에는 본문이 가지 않는다.

## 빠른 시작 (로컬, 외부 키 불필요)

```bash
make setup        # venv + 의존성
make demo         # 트래픽 생성 → 파이프라인 → 평가 → 설명 개선 → 재평가 → 라벨 품질 → 증류 → 라우팅
make dashboard    # Streamlit 대시보드
```

기본 구성: LLM은 키워드 기반 모의 선택기(mock), 전송은 **Apache Kafka**, raw 적재는 **Flink**(`make infra-up`,
docker 없이 쓰려면 `transport.backend: file`), 저장은 로컬 디렉터리, mart는 DuckDB(+`make snowflake`).
**코드 경로는 실모드와 동일**하고 config로만 전환한다.

## 단계별 실행

| 단계 | 명령 | 산출물 |
| --- | --- | --- |
| 1. 트래픽 생성 | `make run-eval` | `data/events/*.jsonl` (본문 없는 이벤트) |
| 2. 파이프라인 | `make pipeline` | `data/raw`, `data/cleansed`(Parquet), `data/mart.duckdb`. Flink 엔진이면 첫 단계는 raw 커밋 대기(최대 약 25초) |
| 3. 선택 평가 | `make eval-report` | `reports/selection_eval_*.md` (confusion matrix 포함) |
| 4. 설명 개선 루프 | `make improve` | 개선된 `SKILL.md` + `reports/improve_pr.md` |
| 5. 정리 판정 | `make lifecycle` | `mart_skill_lifecycle` + `reports/lifecycle.md` |
| 6. 라벨 품질 | `make label-quality` | 라벨러 비교표, τ 스윕, ECE (`reports/label_quality.md`) |
| 7. 증류 | `make distill` → `make router` | 경량 모델 + 3단 라우팅 결과 (`reports/router.md`) |
| (선택) 어려운 테스트셋 | `python -m eval.selection_eval --testset testsets/testset_v2_hard.jsonl --report`, `python -m eval.improve_loop --testset testsets/testset_v2_hard.jsonl` | 실 LLM용 65건(인접 도메인 negative·함정 표현). v1은 실 모델에게 너무 쉽다 |
| (선택) 캐스케이드 상단 모델 비교 | `python -m eval.cascade_models` | `reports/cascade_models.md` (Haiku·Sonnet·Opus 승급 효과) |
| (선택) RoBERTa | `pip install -e ".[distill]"` 후 `python -m distill.train_roberta` | `models/roberta_intent/` |

### mock 모드 레퍼런스 결과

- 스킬 선택 정확도: v1(의도적으로 애매한 설명) **53%** → 자동 개선 2회 채택 후 **88%**
  (3회차는 val 회귀 가드로 기각 — 과적합 방지 동작 확인)
- 라벨러 비교(골든셋 100건, mock Jev): Jev 77% @ $0.05/1k ↔ LLM 90% @ $2.4/1k ↔ 캐스케이드 79% @ $1.47/1k
- 3단 라우팅(경량 → Jev → LLM): LLM 호출 50% 감소, 1천 건당 $2.4 → $1.24

### 실제 Jev(TypeSafe AI) 결과 — 2026-11-19, jev-1.14.0, 골든셋 100건

| labeler | intent acc | macro F1 | OOS acc | $/1k (실측) | 평균 지연 | 승급률 |
| --- | --- | --- | --- | --- | --- | --- |
| Jev | 95% | 0.93 | 94% | $0.027 | 234ms | - |
| LLM (mock) | 90% | 0.89 | 94% | $2.4 (가정) | 350ms | 100% |
| 캐스케이드 (τ=0.75) | 96% | 0.95 | 96% | $0.24 | 252ms | 9% |

- Jev intent ECE 0.05 (확신도 0.9+ 구간 85건 정확도 98.8%) — 보정 확률이 그대로 τ로 쓸 만함
- 한국어 질의를 영어 instructions로 라벨링한 결과. 요청당 약 500 입력 토큰, 출력 토큰 무료
- 상단 LLM은 아직 mock이므로 캐스케이드 수치는 `llm.provider: claude` 전환 후 재측정 필요

## 외부 서비스 전환 (config.yaml)

| 전환 | 설정 | 비고 |
| --- | --- | --- |
| Claude API | `llm.provider: claude` + `ANTHROPIC_API_KEY` | 선택기·설명 개선·라벨 캐스케이드 상단이 실 LLM으로 |
| Kafka | **기본값** (`make infra-up`으로 기동) | Apache Kafka 3.8 KRaft(로컬) / MSK(배포). docker 없이 쓰려면 `transport.backend: file` |
| Flink 적재 | **기본값** (`ingest.engine: flink`, `make infra-up`이 잡까지 제출) | Flink 1.20 SQL 잡이 Kafka → raw를 상시 적재. UI http://localhost:8081. 배치 drain으로 돌리려면 `engine: python` |
| S3 | `storage.raw: s3://<bucket>/raw` + `.env`의 `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` | Flink 컨테이너가 S3 플러그인으로 직접 적재 (docker compose가 `.env`를 읽어 주입). cleanse의 S3 읽기는 `pip install -e ".[s3]"` |
| Snowflake ELT | `pipeline/snowflake_setup.sql`(1회) 후 `make snowflake` 또는 Airflow | S3 raw → external stage `COPY INTO raw_events(VARIANT)` → **Snowflake 내부에서** quarantine·cleansed MERGE → mart 빌드 (전 단계 멱등) |
| Airflow | `make airflow-up` (docker compose, CeleryExecutor. UI http://localhost:8082) | `skill_eval_elt` DAG가 매시: raw 커밋 대기(Flink) → COPY → MERGE → mart → 품질 체크 |
| Jev API | **기본값** (`labeler.jev.provider: http`) + `.env`의 `JEV_API_KEY` | TypeSafe AI Jev(System One). taxonomy 질문을 choice/noul/score로 변환해 1요청에 묶고, 보정 확률을 `Tag.p`로 기록. mock으로 돌리려면 `provider: mock` |

## 디렉터리

```
agent/        에이전트: 스킬(SKILL.md), 툴(FastMCP), 선택기, Runner
testsets/     테스트셋 100건(positive/confusable/negative, train/val), 골든셋
telemetry/    Span Processor(본문 제거), Jev 라벨러(TypeSafe HTTP/mock)·캐스케이드, OTel Collector 설정
pipeline/     Flink 적재 잡(flink_ingest.py, flink/Dockerfile), 배치 consumer(폴백), raw→cleansed→mart(DuckDB), Snowflake ELT
airflow/      skill_eval_elt DAG — 매시 배치 (raw 커밋 대기→COPY→MERGE→mart→품질 체크)
taxonomy/     tx-v2.yaml (라벨 질문 버전 관리. v1의 일정_정리를 복합_브리핑으로 재정의)
eval/         선택 평가, 설명 개선 루프, 라벨 품질(τ 스윕·ECE), 정리 판정
distill/      TF-IDF 베이스라인, klue/roberta-small 파인튜닝, 3단 라우터
dashboard/    Streamlit (DuckDB mart 조회)
```

## Mart 테이블

| 테이블 | 용도 |
| --- | --- |
| `mart_skill_usage_daily` | 스킬별 호출·성공률·p50/p95·토큰·비용 |
| `mart_selection_eval` | run별 선택 정확도·오선택·누락·negative 오탐 |
| `mart_intent_tags_daily` | intent 분포·평균 확신도·저확신 비율 |
| `mart_labeler_cost` | Jev vs LLM vs 캐스케이드 정확도·비용 (label-quality 실행 시 실측으로 갱신) |
| `mart_skill_lifecycle` | keep / improve / deactivate 판정과 근거 |

## 서버로 운영하기

맥미니 한 대에 올려 상시 운영하는 절차(자동 기동, 원격 접속, 주의점)는 [docs/macmini.md](docs/macmini.md).

## 주의사항

- 공개 API·목 데이터·직접 생성한 테스트 트래픽만 사용 (회사 내부 데이터 금지)
- 개선 루프는 `improve_loop.max_iterations`로 호출 상한, 모든 LLM 경로는 비용을 이벤트에 기록
- Jev 한국어 성능은 `make label-quality`의 골든셋 비교로 4단계 전에 확인 (2026-11-19 실측: intent 95%)
- `.env`는 `common/config.py`가 자동 로드. 키는 커밋하지 않는다 (.gitignore)
