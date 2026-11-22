"""config.yaml 로더. 모든 모듈은 이 모듈을 통해서만 설정에 접근한다."""
from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
# .env의 자격증명(JEV_API_KEY, ANTHROPIC_API_KEY, SNOWFLAKE_*)을 환경변수로. 기존 환경변수가 우선.
load_dotenv(PROJECT_ROOT / ".env")


@lru_cache(maxsize=1)
def load_config(path: str | None = None) -> dict[str, Any]:
    config_path = Path(path or os.environ.get("SKILL_EVAL_CONFIG", PROJECT_ROOT / "config.yaml"))
    with open(config_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def resolve_path(path: str) -> str:
    """s3:// 경로는 그대로, 로컬 상대 경로는 프로젝트 루트 기준 절대 경로로."""
    if path.startswith("s3://"):
        return path
    p = Path(path)
    return str(p if p.is_absolute() else PROJECT_ROOT / p)
