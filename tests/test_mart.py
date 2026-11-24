"""cleansed → mart: 선택 평가 지표, 사용 현황 집계, 라벨러 비용 실측 덮어쓰기."""
from __future__ import annotations

import json

import duckdb
import pandas as pd
import pytest

from pipeline import cleanse as cleanse_module
from pipeline import mart as mart_module
from pipeline.cleanse import cleanse
from pipeline.mart import build, selection_eval_frame

from .factories import make_event


@pytest.fixture(autouse=True)
def use_tmp_storage(monkeypatch, pipeline_config, tmp_path):
    monkeypatch.setattr(cleanse_module, "load_config", lambda: pipeline_config)
    monkeypatch.setattr(mart_module, "load_config", lambda: pipeline_config)
    monkeypatch.setattr(mart_module, "MEASURED_LABELER_COST", tmp_path / "label_quality.json")


def selections_db(rows: list[tuple[str, str, str, str]]) -> duckdb.DuckDBPyConnection:
    """(run_id, skill_version, skill, expected_skill) 행으로 events 테이블을 만든다."""
    frame = pd.DataFrame(rows, columns=["run_id", "skill_version", "skill", "expected_skill"])
    frame["event_type"] = "skill_selected"
    con = duckdb.connect()
    con.register("frame", frame)
    con.execute("CREATE TABLE events AS SELECT * FROM frame")
    return con


def mart_rows(pipeline_config: dict, table: str) -> pd.DataFrame:
    con = duckdb.connect(pipeline_config["mart"]["duckdb_path"], read_only=True)
    try:
        return con.execute(f"SELECT * FROM {table}").df()
    finally:
        con.close()


def test_selection_eval_separates_false_picks_misses_and_negative_false_picks():
    con = selections_db([
        ("run-1", "v1", "subway_arrival", "subway_arrival"),  # 정답
        ("run-1", "v1", "subway_route", "subway_arrival"),    # 오선택
        ("run-1", "v1", "none", "weather_now"),               # 누락
        ("run-1", "v1", "none", "none"),                      # negative 정답
        ("run-1", "v1", "weather_now", "none"),               # negative 오탐
    ])

    (row,) = selection_eval_frame(con).to_dict("records")

    assert row["cases"] == 5
    assert row["accuracy"] == pytest.approx(0.4)
    assert row["false_pick_rate"] == pytest.approx(0.4)   # subway_route, weather_now
    assert row["miss_rate"] == pytest.approx(0.2)
    assert row["negative_false_pick_rate"] == pytest.approx(0.5)
    # subway_arrival P=1/1 R=1/2, weather_now P=0/1 R=0/1
    assert row["macro_precision"] == pytest.approx(0.5)
    assert row["macro_recall"] == pytest.approx(0.25)


def test_selection_eval_is_grouped_by_run_and_skill_version():
    con = selections_db([
        ("run-1", "v1", "none", "subway_arrival"),
        ("run-1", "v2", "subway_arrival", "subway_arrival"),
        ("run-2", "v1", "subway_arrival", "subway_arrival"),
    ])

    frame = selection_eval_frame(con).set_index(["run_id", "skill_version"])

    assert frame["accuracy"].to_dict() == {("run-1", "v1"): 0.0, ("run-1", "v2"): 1.0, ("run-2", "v1"): 1.0}


def test_selection_eval_without_negatives_reports_zero_negative_false_picks():
    con = selections_db([("run-1", "v1", "subway_arrival", "subway_arrival")])

    assert selection_eval_frame(con).iloc[0]["negative_false_pick_rate"] == 0.0


def test_build_aggregates_usage_per_day_and_skill(pipeline_config, write_raw):
    write_raw([
        make_event(event_id="r1", latency_ms=100, tokens_in=10, cost_usd=0.01),
        make_event(event_id="r2", latency_ms=300, tokens_in=30, cost_usd=0.03, status="error"),
        make_event(event_id="t1", event_type="tool_called", tool="get_arrival"),
        make_event(event_id="t2", event_type="tool_called", tool="get_arrival", status="timeout"),
        make_event(event_id="r3", skill="weather_now"),
        make_event(event_id="none", skill=None),  # 스킬 미선택 요청은 사용 현황에서 제외
    ])
    cleanse()

    summary = build()

    assert summary == {"events": 6, "runs": 0, "labeler_cost_source": "estimated"}
    usage = mart_rows(pipeline_config, "mart_skill_usage_daily").set_index("skill")
    assert list(usage.index) == ["subway_arrival", "weather_now"]
    subway = usage.loc["subway_arrival"]
    assert (subway["calls"], subway["tokens_in"]) == (2, 40)
    assert subway["success_rate"] == pytest.approx(0.5)
    assert subway["p50_latency_ms"] == pytest.approx(200)
    assert subway["cost_usd"] == pytest.approx(0.04)
    assert subway["tool_success_rate"] == pytest.approx(0.5)
    assert pd.isna(usage.loc["weather_now", "tool_success_rate"])  # 툴 호출이 없던 스킬


def test_build_writes_intent_distribution_and_selection_eval(pipeline_config, write_raw):
    def tagged(event_id: str, intent: str, p: float) -> dict:
        return make_event(event_id=event_id, tags={"intent": {"value": intent, "p": p}},
                          labeler={"model": "jev-1.14.0", "taxonomy_version": "tx-v2", "source": "jev"})

    write_raw([
        tagged("a", "교통_조회", 0.9),
        tagged("b", "교통_조회", 0.5),
        tagged("c", "날씨_조회", 0.8),
        make_event(event_id="s1", event_type="skill_selected", expected_skill="subway_arrival"),
        make_event(event_id="s2", event_type="skill_selected", skill=None, expected_skill="weather_now"),
    ])
    cleanse()

    assert build()["runs"] == 1

    intents = mart_rows(pipeline_config, "mart_intent_tags_daily").set_index("intent")
    assert intents.loc["교통_조회", "count"] == 2
    assert intents.loc["교통_조회", "avg_p"] == pytest.approx(0.7)
    assert intents.loc["교통_조회", "low_conf_ratio"] == pytest.approx(0.5)
    assert intents.loc["날씨_조회", "taxonomy_version"] == "tx-v2"
    labeler = mart_rows(pipeline_config, "mart_labeler_cost").iloc[0]
    assert (labeler["labeler"], labeler["items"], labeler["est_cost_per_1k_usd"]) == ("jev-1.14.0", 3, 0.05)


def test_measured_labeler_cost_replaces_the_estimate(pipeline_config, write_raw):
    write_raw([make_event(labeler={"model": "jev-1.14.0", "taxonomy_version": "tx-v2", "source": "jev"})])
    cleanse()
    measured = [{"labeler": "jev", "intent_accuracy": 0.97, "cost_per_1k_usd": 0.03}]
    mart_module.MEASURED_LABELER_COST.write_text(json.dumps({"comparison": measured}), encoding="utf-8")

    assert build()["labeler_cost_source"] == "measured"
    assert mart_rows(pipeline_config, "mart_labeler_cost").to_dict("records") == measured
