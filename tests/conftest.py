from __future__ import annotations

import json
from pathlib import Path

import pytest


@pytest.fixture
def pipeline_config(tmp_path: Path) -> dict:
    """raw·cleansed·quarantine·mart가 전부 임시 디렉터리를 가리키는 설정."""
    return {
        "storage": {
            "raw": str(tmp_path / "raw"),
            "cleansed": str(tmp_path / "cleansed"),
            "quarantine": str(tmp_path / "quarantine"),
        },
        "mart": {"duckdb_path": str(tmp_path / "mart.duckdb")},
        "labeler": {"jev": {"cost_per_1k_usd": 0.05}, "llm_cost_per_1k_usd": 2.4},
    }


@pytest.fixture
def write_raw(pipeline_config: dict):
    """raw 파티션에 줄 단위 파일을 쓴다. dict는 JSON으로, str은 그대로(깨진 줄 재현용)."""

    def _write(lines: list[dict | str], date: str = "2026-11-20", name: str = "part-0.jsonl") -> Path:
        partition = Path(pipeline_config["storage"]["raw"]) / f"dt={date}"
        partition.mkdir(parents=True, exist_ok=True)
        path = partition / name
        path.write_text(
            "\n".join(line if isinstance(line, str) else json.dumps(line, ensure_ascii=False) for line in lines) + "\n",
            encoding="utf-8",
        )
        return path

    return _write
