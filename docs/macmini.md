# 맥미니 서버 운영 가이드

서버만 맥미니로 옮기고 S3 · Snowflake · Jev · Claude API는 그대로 외부를 쓰는 구성이다.
Apple Silicon, 메모리 16GB 기준 (컨테이너 합계 약 6GB).

## 1. 준비물

- Docker Desktop — 설정에서 **Start Docker Desktop when you sign in** 켜기, Resources의 Memory를 **10GB**로
- Xcode Command Line Tools (`xcode-select --install`) — git, make
- Python 3.12 (`brew install python@3.12`)

## 2. 설치

```bash
git clone https://github.com/backend-kyounglyool/llm-evaluation-data-system.git
cd llm-evaluation-data-system
make setup
.venv/bin/pip install -e ".[kafka,s3,snowflake,llm,dev]"
```

## 3. 자격증명 (git에 없는 것)

| 항목 | 옮기는 방법 |
| --- | --- |
| `.env` | 기존 장비에서 복사 (`scp`). `.env.example`이 항목 목록이다 |
| Snowflake 개인키 | `~/.snowflake/skill_eval_rsa_key.p8`을 복사하고, `.env`의 `SNOWFLAKE_PRIVATE_KEY_PATH`를 맥미니의 절대 경로로 수정 |

`.env`에 `GRAFANA_ADMIN_PASSWORD`, `AIRFLOW_ADMIN_PASSWORD`를 넣어 기본값(admin)을 바꾼다.
둘 다 첫 기동 때만 적용된다.

## 4. 기동과 확인

```bash
make airflow-up     # Kafka · Flink · 모니터링 · Airflow 전체 + Flink 적재 잡 제출
docker compose --profile airflow exec airflow-scheduler airflow dags unpause skill_eval_elt
make flink-status   # job_id가 나오면 적재 잡이 떠 있는 것
make test
```

| 화면 | 주소 (맥미니 안에서) |
| --- | --- |
| Grafana | http://localhost:3001 |
| Airflow | http://localhost:8082 |
| Flink | http://localhost:8081 |
| Kafka UI | http://localhost:8080 |

## 5. 재부팅 후 자동 기동

컨테이너는 `restart: unless-stopped`라 Docker가 뜨면 같이 올라온다.
Flink 적재 잡은 다시 제출해야 하므로(마지막 체크포인트에서 복원) 로그인 시 실행되는 launchd 에이전트를 둔다.

```bash
make install-boot   # ~/Library/LaunchAgents/com.skilleval.boot.plist 설치
tail -f ~/Library/Logs/skill-eval-boot.log
```

함께 해 둘 macOS 설정:

- **자동 로그인** (시스템 설정 → 사용자 및 그룹). Docker Desktop과 launchd 에이전트는 로그인된 세션에서만 돈다
- **잠자기 끄기**: `sudo pmset -a sleep 0 disksleep 0` 그리고 정전 후 자동 시작 `sudo pmset -a autorestart 1`

## 6. 다른 장비에서 접속

모든 포트는 `127.0.0.1`에만 열려 있다. Flink REST는 잡 제출이 가능하고 Kafka에는 인증이 없으므로 그대로 둔다.

- **화면 보기**: SSH 터널 — `ssh -L 3001:localhost:3001 -L 8082:localhost:8082 <맥미니>`
- **다른 장비의 에이전트가 이벤트 전송**: Tailscale 같은 사설망을 깔고 `.env`에 아래를 넣은 뒤 `docker compose up -d kafka`.
  에이전트 쪽 `transport.kafka.bootstrap_servers`는 `<맥미니 사설망 주소>:19093`.

```
KAFKA_LAN_BIND=<맥미니 사설망 IP>
KAFKA_LAN_HOST=<맥미니 사설망 IP 또는 호스트명>
```

## 7. 운영 중 알아 둘 것

- **업데이트**: `git pull && make airflow-up`
- **중지**: `make infra-down` (볼륨은 남는다)
- **단일 장비**: 복제가 없다. 맥미니가 꺼져 있는 동안 들어온 이벤트는 받지 못한다
- **Kafka 보존 7일**: Flink 잡이 7일 넘게 멈춰 있으면 그 구간은 유실된다. Grafana 파이프라인 대시보드의 컨슈머 랙을 본다
- **비용**: Airflow DAG가 매시 Snowflake 웨어하우스를 깨운다. 기존 장비에서도 스택이 돌고 있으면 그쪽 DAG는 pause 하거나 `make infra-down`
