"""raw → cleansed: 스키마 위반 격리, event_id 중복 제거, 태그 평탄화, 날짜 파티션."""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from pipeline import cleanse as cleanse_module
from pipeline.cleanse import cleanse

from .factories import make_event


@pytest.fixture(autouse=True)
def use_tmp_storage(monkeypatch, pipeline_config):
    monkeypatch.setattr(cleanse_module, "load_config", lambda: pipeline_config)


def read_cleansed(pipeline_config: dict, date: str = "2026-11-20") -> pd.DataFrame:
    return pd.read_parquet(Path(pipeline_config["storage"]["cleansed"]) / f"dt={date}" / "events.parquet")


def read_quarantine(pipeline_config: dict) -> list[dict]:
    path = Path(pipeline_config["storage"]["quarantine"]) / "quarantine.jsonl"
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_tags_and_labeler_are_flattened_into_columns(pipeline_config, write_raw):
    write_raw([
        make_event(
            tags={"intent": {"value": "교통_조회", "p": 0.93}, "out_of_scope": {"value": False, "p": 0.99}},
            labeler={"model": "jev-1.14.0", "taxonomy_version": "tx-v2", "calibrated": True, "source": "jev"},
        )
    ])

    assert cleanse() == {"raw_files": 1, "valid": 1, "deduped": 0, "quarantined": 0}

    row = read_cleansed(pipeline_config).iloc[0]
    assert (row["tag_intent"], row["tag_intent_p"]) == ("교통_조회", pytest.approx(0.93))
    assert row["tag_out_of_scope"] == "False"
    assert row["tag_skill_fit"] is None
    assert (row["labeler_model"], row["labeler_source"], row["taxonomy_version"]) == ("jev-1.14.0", "jev", "tx-v2")
    assert row["dt"] == "2026-11-20"


def test_event_without_labels_keeps_metrics_with_empty_tag_columns(pipeline_config, write_raw):
    write_raw([make_event(event_type="tool_called", tool="get_arrival", status="timeout")])

    cleanse()

    row = read_cleansed(pipeline_config).iloc[0]
    assert (row["tool"], row["status"], row["latency_ms"]) == ("get_arrival", "timeout", 120)
    assert row["tag_intent"] is None and row["labeler_model"] is None


def test_duplicate_event_ids_are_kept_once_across_files(pipeline_config, write_raw):
    write_raw([make_event(event_id="dup"), make_event(event_id="dup")], name="part-0.jsonl")
    write_raw([make_event(event_id="dup"), make_event(event_id="other")], name="part-1.jsonl")

    summary = cleanse()

    assert (summary["valid"], summary["deduped"]) == (2, 2)
    assert sorted(read_cleansed(pipeline_config)["event_id"]) == ["dup", "other"]


def test_invalid_lines_are_quarantined_without_dropping_valid_ones(pipeline_config, write_raw):
    missing_id = make_event()
    del missing_id["event_id"]
    path = write_raw([
        make_event(event_id="good"),
        "{not json",
        make_event(event_id="bad-type", event_type="unknown_type"),
        make_event(event_id="bad-status", status="exploded"),
        missing_id,
        "",
    ])

    summary = cleanse()

    assert (summary["valid"], summary["quarantined"]) == (1, 4)
    assert list(read_cleansed(pipeline_config)["event_id"]) == ["good"]
    quarantined = read_quarantine(pipeline_config)
    assert [q["source"] for q in quarantined] == [f"{path}:{line_no}" for line_no in (2, 3, 4, 5)]
    assert quarantined[0]["line"] == "{not json"
    assert all(q["error"] for q in quarantined)


def test_events_are_partitioned_by_event_date(pipeline_config, write_raw):
    write_raw([
        make_event(event_id="a", ts="2026-11-20T23:59:59+00:00"),
        make_event(event_id="b", ts="2026-11-21T00:00:00+00:00"),
    ])

    cleanse()

    assert list(read_cleansed(pipeline_config, "2026-11-20")["event_id"]) == ["a"]
    assert list(read_cleansed(pipeline_config, "2026-11-21")["event_id"]) == ["b"]


def test_flink_part_files_are_read_and_in_progress_files_are_skipped(pipeline_config, write_raw):
    write_raw([make_event(event_id="committed")], name="part-7f3a-0-12")
    write_raw([make_event(event_id="in-progress")], name=".part-7f3a-0-13.inprogress")

    summary = cleanse()

    assert summary["raw_files"] == 1
    assert list(read_cleansed(pipeline_config)["event_id"]) == ["committed"]


def test_empty_raw_writes_nothing(pipeline_config):
    assert cleanse() == {"raw_files": 0, "valid": 0, "deduped": 0, "quarantined": 0}
    assert not Path(pipeline_config["storage"]["cleansed"]).exists()
