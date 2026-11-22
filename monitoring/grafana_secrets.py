"""Grafana Snowflake 데이터소스가 읽을 개인키 파일 생성.

플러그인(gosnowflake DSN)은 PKCS8 키를 헤더·줄바꿈 없는 URL-safe base64로 받는다.
SNOWFLAKE_PRIVATE_KEY_PATH의 PEM을 변환해 .grafana/secrets/에 쓴다 (git 제외 경로).
사용: python -m monitoring.grafana_secrets
"""
from __future__ import annotations

import os
from pathlib import Path

from common.config import PROJECT_ROOT

SECRETS_DIR = PROJECT_ROOT / ".grafana" / "secrets"
KEY_FILE = SECRETS_DIR / "snowflake_private_key"


def pem_to_urlsafe_base64(pem: str) -> str:
    body = "".join(line for line in pem.splitlines() if line and not line.startswith("-----"))
    return body.replace("+", "-").replace("/", "_")


def write_snowflake_key() -> bool:
    SECRETS_DIR.mkdir(parents=True, exist_ok=True)
    key_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH")
    if not key_path or not Path(key_path).expanduser().exists():
        # 파일이 없으면 Grafana 프로비저닝이 실패하므로 빈 파일이라도 둔다
        KEY_FILE.touch(mode=0o600)
        print("SNOWFLAKE_PRIVATE_KEY_PATH 없음 — Grafana Snowflake 데이터소스는 비활성 상태로 뜬다")
        return False
    KEY_FILE.write_text(pem_to_urlsafe_base64(Path(key_path).expanduser().read_text()))
    KEY_FILE.chmod(0o600)
    print(f"Grafana용 Snowflake 키 작성: {KEY_FILE.relative_to(PROJECT_ROOT)}")
    return True


if __name__ == "__main__":
    write_snowflake_key()
