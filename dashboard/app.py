"""운영 대시보드 (Streamlit + Altair, DuckDB mart 조회 전용).

사용: streamlit run dashboard/app.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

from common.config import load_config, resolve_path
from dashboard import overview, trace_view

# 검증된 카테고리 팔레트(dataviz 기준). 엔티티(스킬 이름)에 고정 매핑한다.
CATEGORICAL = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"]
SEQUENTIAL_HUE = "blues"

st.set_page_config(page_title="skill-eval 대시보드", layout="wide")


def connect() -> duckdb.DuckDBPyConnection:
    # 파이프라인(쓰기)과의 잠금 충돌을 피하기 위해 렌더마다 읽기 연결을 새로 연다
    return duckdb.connect(resolve_path(load_config()["mart"]["duckdb_path"]), read_only=True)


def table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    return bool(
        con.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = ?", [name]
        ).fetchone()
    )


def skill_color_scale(skills: list[str]) -> alt.Scale:
    ordered = sorted(skills)
    return alt.Scale(domain=ordered, range=CATEGORICAL[: len(ordered)])


con = connect()
st.title("에이전트 스킬 로그 · 평가 대시보드")

tab_overview, tab_traces, tab_usage, tab_eval, tab_tags, tab_labeler, tab_lifecycle = st.tabs(
    ["개요", "트레이스", "사용 현황", "선택 평가", "의도 태그", "라벨러 비용", "정리 판정"]
)

with tab_overview:
    if not table_exists(con, "events"):
        st.info("mart가 비어 있습니다. make pipeline 을 먼저 실행하세요.")
    else:
        overview.render(con, trace_view.load_spans())

with tab_traces:
    spans = trace_view.load_spans()
    if spans.empty:
        st.info("트레이스가 없습니다. make run-eval 또는 계측 SDK로 트래픽을 생성하세요.")
    else:
        summaries = trace_view.trace_summaries(spans)

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("트레이스", f"{len(summaries):,}")
        col2.metric("토큰 (in/out)", f"{int(summaries['tokens_in'].sum()):,} / {int(summaries['tokens_out'].sum()):,}")
        col3.metric("누적 비용", f"${summaries['cost_usd'].sum():.4f}")
        col4.metric("에러 트레이스", int((summaries["errors"] > 0).sum()))

        run_ids = ["(전체)"] + sorted(summaries["run_id"].dropna().unique().tolist(), reverse=True)
        selected_run = st.selectbox("run 필터", run_ids)
        filtered = summaries if selected_run == "(전체)" else summaries[summaries["run_id"] == selected_run]

        display_cols = ["trace_id", "test_id", "skill", "status", "errors",
                        "duration_ms", "tokens_in", "tokens_out", "cost_usd", "spans", "ts"]
        st.dataframe(
            filtered[[c for c in display_cols if c in filtered.columns]],
            use_container_width=True, height=240,
        )

        options = filtered["trace_id"].tolist()
        if options:
            labels = {
                row["trace_id"]: f"{row.get('test_id') or row['trace_id'][:8]} · "
                                 f"{row.get('skill') or '-'} · {row['duration_ms']}ms"
                for _, row in filtered.iterrows()
            }
            trace_id = st.selectbox("트레이스 선택", options, format_func=lambda t: labels[t])
            tree = trace_view.trace_tree(spans, trace_id)

            st.altair_chart(trace_view.waterfall_chart(tree), use_container_width=True)
            st.altair_chart(trace_view.flame_chart(tree), use_container_width=True)

            detail_cols = ["label", "kind", "duration_ms", "tokens_in", "tokens_out",
                           "cost_usd", "status", "model", "start_ms", "end_ms"]
            if "body" in tree.columns:
                detail_cols.append("body")
            st.dataframe(tree[[c for c in detail_cols if c in tree.columns]],
                         use_container_width=True)

with tab_usage:
    if not table_exists(con, "mart_skill_usage_daily"):
        st.info("mart가 비어 있습니다. make pipeline 을 먼저 실행하세요.")
    else:
        usage = con.execute("SELECT * FROM mart_skill_usage_daily").df()
        total_calls = int(usage["calls"].sum())
        overall_success = (usage["success_rate"] * usage["calls"]).sum() / total_calls
        cost = usage["cost_usd"].sum()

        col1, col2, col3 = st.columns(3)
        col1.metric("총 요청", f"{total_calls:,}")
        col2.metric("성공률", f"{overall_success:.1%}")
        col3.metric("누적 비용", f"${cost:.4f}")

        per_skill = (
            usage.groupby("skill", as_index=False)
            .agg(calls=("calls", "sum"), p95=("p95_latency_ms", "max"),
                 success_rate=("success_rate", "mean"))
        )
        scale = skill_color_scale(per_skill["skill"].tolist())

        left, right = st.columns(2)
        left.altair_chart(
            alt.Chart(per_skill, title="스킬별 호출 수")
            .mark_bar(cornerRadiusEnd=4, size=18)
            .encode(
                x=alt.X("calls:Q", title="호출 수"),
                y=alt.Y("skill:N", sort="-x", title=None),
                color=alt.Color("skill:N", scale=scale, legend=None),
                tooltip=["skill", "calls", alt.Tooltip("success_rate:Q", format=".1%")],
            ),
            use_container_width=True,
        )
        right.altair_chart(
            alt.Chart(per_skill, title="스킬별 p95 지연(ms)")
            .mark_bar(cornerRadiusEnd=4, size=18)
            .encode(
                x=alt.X("p95:Q", title="p95 latency (ms)"),
                y=alt.Y("skill:N", sort="-x", title=None),
                color=alt.Color("skill:N", scale=scale, legend=None),
                tooltip=["skill", alt.Tooltip("p95:Q", format=".0f")],
            ),
            use_container_width=True,
        )
        st.dataframe(usage, use_container_width=True)

with tab_eval:
    if not table_exists(con, "mart_selection_eval"):
        st.info("선택 평가 데이터가 없습니다. make run-eval 후 make pipeline 을 실행하세요.")
    else:
        eval_df = con.execute(
            "SELECT * FROM mart_selection_eval ORDER BY run_id"
        ).df()
        st.dataframe(eval_df, use_container_width=True)

        if len(eval_df) > 1:
            st.altair_chart(
                alt.Chart(eval_df, title="run별 선택 정확도")
                .mark_line(point=alt.OverlayMarkDef(size=80), strokeWidth=2, color=CATEGORICAL[0])
                .encode(
                    x=alt.X("run_id:N", title="run"),
                    y=alt.Y("accuracy:Q", title="accuracy", scale=alt.Scale(domain=[0, 1])),
                    tooltip=["run_id", "skill_version", "accuracy"],
                ),
                use_container_width=True,
            )

        confusion = con.execute(
            """
            SELECT expected_skill AS expected, skill AS selected, count(*) AS count
            FROM events
            WHERE event_type = 'skill_selected' AND expected_skill IS NOT NULL
              AND run_id = (SELECT max(run_id) FROM events WHERE event_type = 'skill_selected')
            GROUP BY 1, 2
            """
        ).df()
        if len(confusion):
            st.altair_chart(
                alt.Chart(confusion, title="Confusion matrix (최신 run)")
                .mark_rect(stroke="#fcfcfb", strokeWidth=2)
                .encode(
                    x=alt.X("selected:N", title="선택됨"),
                    y=alt.Y("expected:N", title="정답"),
                    color=alt.Color("count:Q", scale=alt.Scale(scheme=SEQUENTIAL_HUE), title="건수"),
                    tooltip=["expected", "selected", "count"],
                ),
                use_container_width=True,
            )

with tab_tags:
    if not table_exists(con, "mart_intent_tags_daily"):
        st.info("태그 데이터가 없습니다.")
    else:
        tags = con.execute("SELECT * FROM mart_intent_tags_daily").df()
        per_intent = tags.groupby("intent", as_index=False).agg(
            count=("count", "sum"), avg_p=("avg_p", "mean"), low_conf=("low_conf_ratio", "mean")
        )
        left, right = st.columns(2)
        left.altair_chart(
            alt.Chart(per_intent, title="intent 분포")
            .mark_bar(cornerRadiusEnd=4, size=18, color=CATEGORICAL[0])
            .encode(
                x=alt.X("count:Q", title="건수"),
                y=alt.Y("intent:N", sort="-x", title=None),
                tooltip=["intent", "count", alt.Tooltip("avg_p:Q", format=".2f")],
            ),
            use_container_width=True,
        )
        right.altair_chart(
            alt.Chart(per_intent, title="저확신 비율 (p < 0.6)")
            .mark_bar(cornerRadiusEnd=4, size=18, color=CATEGORICAL[1])
            .encode(
                x=alt.X("low_conf:Q", title="low-confidence ratio", axis=alt.Axis(format=".0%")),
                y=alt.Y("intent:N", sort="-x", title=None),
                tooltip=["intent", alt.Tooltip("low_conf:Q", format=".1%")],
            ),
            use_container_width=True,
        )
        st.dataframe(tags, use_container_width=True)

with tab_labeler:
    if not table_exists(con, "mart_labeler_cost"):
        st.info("라벨러 데이터가 없습니다. python -m eval.label_quality 실행 시 상세 비교가 채워집니다.")
    else:
        st.dataframe(con.execute("SELECT * FROM mart_labeler_cost").df(), use_container_width=True)

with tab_lifecycle:
    if not table_exists(con, "mart_skill_lifecycle"):
        st.info("정리 판정 데이터가 없습니다. python -m eval.lifecycle 을 실행하세요.")
    else:
        lifecycle = con.execute("SELECT * FROM mart_skill_lifecycle").df()
        st.dataframe(lifecycle, use_container_width=True)
        counts = lifecycle["verdict"].value_counts()
        col1, col2, col3 = st.columns(3)
        col1.metric("keep", int(counts.get("keep", 0)))
        col2.metric("improve", int(counts.get("improve", 0)))
        col3.metric("deactivate 후보", int(counts.get("deactivate", 0)))

con.close()  # 읽기 연결을 쥐고 있으면 파이프라인의 쓰기가 잠금에 막힌다
