#!/bin/bash
# 부팅(로그인) 후 전체 스택을 올리고 Flink 적재 잡을 제출한다.
# launchd가 호출한다 — 설치는 `make install-boot`, 설명은 docs/macmini.md
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="/usr/local/bin:/opt/homebrew/bin:$PATH"

echo "[$(date '+%F %T')] Docker 데몬 대기"
for _ in $(seq 120); do   # 최대 10분
  docker info >/dev/null 2>&1 && break
  sleep 5
done
docker info >/dev/null

echo "[$(date '+%F %T')] 스택 기동"
make airflow-up
echo "[$(date '+%F %T')] 완료"
