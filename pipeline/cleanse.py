"""raw → cleansed 변환.

- 스키마 검증(AgentEvent): 위반 건은 quarantine으로 분리
- event_id 중복 제거, 타입 캐스팅, 태그 평탄화
- Parquet로 날짜 파티션 저장 (로컬 또는 S3)
사용: python -m pipeline.cleanse
"""
from __future__ import annotations

import json

import pandas as pd
from pydantic import ValidationError

from common.config import load_config, resolve_path
from common.events import AgentEvent
from pipeline import storage

TAG_KEYS = ("intent", "out_of_scope", "skill_fit", "complexity")


def flatten(event: AgentEvent) -> dict:
    row = {
        "event_id": event.event_id,
        "trace_id": event.trace_id,
        "ts": pd.Timestamp(event.ts),
        "dt": event.ts[:10],
        "run_id": event.run_id,
        "test_id": event.test_id,
        "event_type": event.event_type,
        "skill": event.skill,
        "expected_skill": event.expected_skill,
        "skill_version": event.skill_version,
        "tool": event.tool,
        "status": event.status,
        "latency_ms": int(event.latency_ms),
        "tokens_in": int(event.tokens_in),
        "tokens_out": int(event.tokens_out),
        "cost_usd": float(event.cost_usd),
    }
    for key in TAG_KEYS:
        tag = event.tags.get(key)
        row[f"tag_{key}"] = str(tag.value) if tag else None
        row[f"tag_{key}_p"] = float(tag.p) if tag else None
    row["labeler_model"] = event.labeler.model if event.labeler else None
    row["labeler_source"] = event.labeler.source if event.labeler else None
    row["taxonomy_version"] = event.labeler.taxonomy_version if event.labeler else None
    row["labeler_calibrated"] = event.labeler.calibrated if event.labeler else None
    return row


def cleanse() -> dict:
    cfg = load_config()["storage"]
    # Python consumer는 part-*.jsonl, Flink 싱크는 확장자 없는 part-*로 쓴다 (쓰는 중인 파일은 .part-*라 제외됨)
    raw_files = storage.list_files(cfg["raw"], "dt=*/part-*")

    rows, quarantined = [], []
    for path in raw_files:
        for line_no, line in enumerate(storage.read_text(path).splitlines(), 1):
            if not line.strip():
                continue
            try:
                rows.append(flatten(AgentEvent.model_validate_json(line)))
            except (ValidationError, json.JSONDecodeError) as exc:
                quarantined.append({"source": f"{path}:{line_no}", "error": str(exc)[:500], "line": line[:1000]})

    if quarantined:
        content = "\n".join(json.dumps(q, ensure_ascii=False) for q in quarantined) + "\n"
        storage.write_text(cfg["quarantine"], "quarantine.jsonl", content)

    summary = {"raw_files": len(raw_files), "valid": 0, "deduped": 0, "quarantined": len(quarantined)}
    if rows:
        df = pd.DataFrame(rows)
        before = len(df)
        df = df.drop_duplicates(subset="event_id").reset_index(drop=True)
        summary["valid"] = len(df)
        summary["deduped"] = before - len(df)

        cleansed_base = resolve_path(cfg["cleansed"])
        for date, partition in df.groupby("dt"):
            if cleansed_base.startswith("s3://"):
                partition.to_parquet(f"{cleansed_base}/dt={date}/events.parquet", index=False)
            else:
                from pathlib import Path

                out_dir = Path(cleansed_base) / f"dt={date}"
                out_dir.mkdir(parents=True, exist_ok=True)
                partition.to_parquet(out_dir / "events.parquet", index=False)

    print(json.dumps(summary, ensure_ascii=False))
    return summary


if __name__ == "__main__":
    cleanse()
