"""테스트셋 → 골든셋 변환. intent·out_of_scope·complexity 정답 라벨 부여.

실제 운영에서는 이 파일을 수작업 라벨링 결과로 교체한다.
사용: python -m testsets.build_goldenset
"""
from __future__ import annotations

import json
from pathlib import Path

from common.config import PROJECT_ROOT

COMPOSITE_SKILLS = {"commute_briefing", "weekend_performance"}


def build() -> None:
    src = PROJECT_ROOT / "testsets/testset_v1.jsonl"
    dst = PROJECT_ROOT / "testsets/goldenset.jsonl"
    rows = []
    with open(src, encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            case = json.loads(line)
            rows.append(
                {
                    "id": case["id"].replace("t-", "g-"),
                    "query": case["query"],
                    "gold": {
                        "intent": case["intent"],
                        "out_of_scope": case["case_type"] == "negative",
                        "complexity": 2 if case["expected"] in COMPOSITE_SKILLS else 1,
                    },
                    "expected_skill": case["expected"],
                }
            )
    with open(dst, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    print(f"goldenset {len(rows)}건 → {dst}")


if __name__ == "__main__":
    build()
