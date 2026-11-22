"""Consumer: 이벤트 스트림(file/kafka) → raw 레이어(JSON, 날짜 파티션) 적재.

ingest.engine: flink이면 적재는 Flink 스트리밍 잡(pipeline/flink_ingest.py)이 맡고,
이 모듈은 커밋 대기만 한다. 아래 배치 drain은 engine: python 또는 file transport용.

재실행 시 같은 이벤트가 raw에 중복될 수 있으며, cleansed 단계에서
event_id 기준으로 제거한다 (at-least-once + 하류 멱등).
사용: python -m pipeline.consumer
"""
from __future__ import annotations

import hashlib
import json
from collections import defaultdict

from common.config import load_config
from common.transport import consume_events
from pipeline import flink_ingest, storage


def ingest() -> dict:
    cfg = load_config()
    if flink_ingest.uses_flink(cfg):
        # Flink 잡이 상시 적재 중 — 여기서는 지금까지의 메시지가 raw에 커밋되기만 기다린다
        return flink_ingest.await_commit()

    raw_base = cfg["storage"]["raw"]
    by_date: dict[str, list[dict]] = defaultdict(list)
    for event in consume_events():
        date = (event.get("ts") or "unknown")[:10]
        by_date[date].append(event)

    counts: dict[str, int] = {}
    for date, events in by_date.items():
        content = "\n".join(json.dumps(e, ensure_ascii=False) for e in events) + "\n"
        batch_id = hashlib.md5(content.encode()).hexdigest()[:12]
        storage.write_text(raw_base, f"dt={date}/part-{batch_id}.jsonl", content)
        counts[date] = len(events)

    total = sum(counts.values())
    print(json.dumps({"ingested": total, "partitions": counts}, ensure_ascii=False))
    return counts


if __name__ == "__main__":
    ingest()
