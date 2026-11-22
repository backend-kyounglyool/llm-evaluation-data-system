"""서비스 개요 탭: 구성요소 상태, 적재 수, 툴콜 결과, span 종류별 지연.

DuckDB mart(events)와 trace 저장소만 읽는다. 외부 호출은 Kafka 포트 TCP 체크 1회(0.5s)뿐.
"""
from __future__ import annotations

import os
import socket

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

from common.config import load_config

STATUS_COLOR = {"ok": "#1baf7a", "down": "#e34948", "mock": "#eda100", "n/a": "#9aa3ad"}
EVENT_TYPES = ["skill_selected", "tool_called", "request_done"]
KIND_ORDER = ["agent", "workflow", "thinking", "llm", "tool", "labeler"]


def _tcp_open(hostport: str, timeout: float = 0.5) -> bool:
    host, _, port = hostport.rpartition(":")
    try:
        with socket.create_connection((host or "localhost", int(port)), timeout=timeout):
            return True
    except OSError:
        return False


@st.cache_data(ttl=30)
def component_status() -> list[dict]:
    cfg = load_config()
    jev = cfg["labeler"]["jev"]
    jev_http = jev["provider"] == "http"
    jev_ok = jev_http and bool(os.environ.get(jev.get("api_key_env", "JEV_API_KEY")))
    llm_claude = cfg["llm"]["provider"] == "claude" and bool(os.environ.get("ANTHROPIC_API_KEY"))
    kafka = cfg["transport"]["backend"] == "kafka"
    kafka_up = kafka and _tcp_open(cfg["transport"]["kafka"]["bootstrap_servers"])
    raw = cfg["storage"]["raw"]
    return [
        {"component": "Span Processor (본문 제거)", "status": "ok", "detail": "이벤트 전송 전 본문 삭제, labeler span 기록"},
        {"component": f"Jev ({jev['model']})", "status": "ok" if jev_ok else ("down" if jev_http else "mock"),
         "detail": "HTTP 실연동" if jev_ok else ("키 없음" if jev_http else "키워드 mock")},
        {"component": "생성형 LLM 티어", "status": "ok" if llm_claude else "mock",
         "detail": cfg["llm"]["model"] if llm_claude else "ANTHROPIC_API_KEY 미설정 → mock"},
        {"component": "전송 (Kafka)" if kafka else "전송 (file)", "status": ("ok" if kafka_up else "down") if kafka else "ok",
         "detail": cfg["transport"]["kafka"]["bootstrap_servers"] if kafka else cfg["transport"]["file"]["dir"]},
        {"component": "raw 저장소", "status": "ok" if raw.startswith("s3://") else "n/a", "detail": raw},
        {"component": "mart (Snowflake)", "status": "ok" if os.environ.get("SNOWFLAKE_USER") else "n/a",
         "detail": f"{cfg['mart']['snowflake']['database']}.{cfg['mart']['snowflake']['schema']}"},
    ]


def render_status_strip() -> None:
    cols = st.columns(len(component_status()))
    for col, item in zip(cols, component_status()):
        color = STATUS_COLOR[item["status"]]
        col.markdown(
            f"<div style='border:1px solid #e3e6ea;border-radius:10px;padding:8px 10px;font-size:0.85rem'>"
            f"<span style='display:inline-block;width:9px;height:9px;border-radius:50%;background:{color};margin-right:6px'></span>"
            f"<b>{item['component']}</b><br><span style='color:#6b7480'>{item['detail']}</span></div>",
            unsafe_allow_html=True,
        )


def render_kpis(con: duckdb.DuckDBPyConnection) -> None:
    row = con.execute(
        """
        SELECT count(*) AS events,
               count(*) FILTER (WHERE event_type = 'request_done') AS requests,
               count(*) FILTER (WHERE event_type = 'tool_called') AS tool_calls,
               avg(CASE WHEN status = 'ok' THEN 1 ELSE 0 END) FILTER (WHERE event_type = 'tool_called') AS tool_success,
               quantile_cont(latency_ms, 0.95) FILTER (WHERE event_type = 'request_done') AS p95,
               sum(cost_usd) AS cost,
               count(DISTINCT run_id) AS runs,
               max(ts) AS last_ts
        FROM events
        """
    ).fetchone()
    events, requests, tool_calls, tool_success, p95, cost, runs, last_ts = row
    c = st.columns(6)
    c[0].metric("적재 이벤트 (cleansed)", f"{events:,}")
    c[1].metric("요청", f"{requests:,}", f"{runs} run")
    c[2].metric("툴 호출", f"{tool_calls:,}", f"성공률 {tool_success:.1%}")
    c[3].metric("요청 p95", f"{p95:,.0f} ms")
    c[4].metric("누적 비용", f"${cost:.4f}")
    c[5].metric("마지막 이벤트", pd.Timestamp(last_ts).strftime("%m-%d %H:%M"))


def render_ingest(con: duckdb.DuckDBPyConnection) -> None:
    daily = con.execute(
        "SELECT CAST(dt AS VARCHAR) AS dt, event_type, count(*) AS events FROM events GROUP BY 1, 2 ORDER BY 1, 2"
    ).df()
    st.altair_chart(
        alt.Chart(daily, title="일별 적재 이벤트 (유형별)")
        .mark_bar(size=28)
        .encode(
            x=alt.X("dt:N", title=None),
            y=alt.Y("events:Q", title="건"),
            color=alt.Color("event_type:N", sort=EVENT_TYPES, title=None, legend=alt.Legend(orient="bottom")),
            tooltip=["dt", "event_type", "events"],
        )
        .properties(height=220),
        use_container_width=True,
    )


def render_throughput(con: duckdb.DuckDBPyConnection) -> None:
    runs = con.execute("SELECT run_id, min(ts) AS started FROM events GROUP BY 1 ORDER BY 2 DESC").df()
    run_id = st.selectbox("run", runs["run_id"].tolist(), key="overview_run")
    tp = con.execute(
        """
        WITH e AS (SELECT epoch(ts)::BIGINT AS s, event_type FROM events WHERE run_id = ?)
        SELECT ((s - (SELECT min(s) FROM e)) // 5) * 5 AS sec, event_type, count(*) AS events
        FROM e GROUP BY 1, 2 ORDER BY 1, 2
        """,
        [run_id],
    ).df()
    st.altair_chart(
        alt.Chart(tp, title="처리량 (5초 단위)")
        .mark_bar()
        .encode(
            x=alt.X("sec:O", title="run 시작 후 초"),
            y=alt.Y("events:Q", title="건"),
            color=alt.Color("event_type:N", sort=EVENT_TYPES, title=None, legend=None),
            tooltip=["sec", "event_type", "events"],
        )
        .properties(height=220),
        use_container_width=True,
    )


def render_tools(con: duckdb.DuckDBPyConnection) -> None:
    tools = con.execute(
        """
        SELECT tool, status, count(*) AS calls,
               quantile_cont(latency_ms, 0.95) AS p95
        FROM events WHERE event_type = 'tool_called' GROUP BY 1, 2 ORDER BY 1
        """
    ).df()
    chart = (
        alt.Chart(tools, title="툴 호출 결과")
        .mark_bar()
        .encode(
            y=alt.Y("tool:N", title=None, sort="-x"),
            x=alt.X("calls:Q", title="호출"),
            color=alt.Color(
                "status:N", title=None, legend=alt.Legend(orient="bottom"),
                scale=alt.Scale(domain=["ok", "error", "timeout"], range=["#1baf7a", "#e34948", "#eda100"]),
            ),
            tooltip=["tool", "status", "calls", alt.Tooltip("p95:Q", format=".0f", title="p95 ms")],
        )
        .properties(height=220)
    )
    st.altair_chart(chart, use_container_width=True)


def render_span_kinds(spans: pd.DataFrame) -> None:
    if spans.empty:
        st.info("트레이스가 없습니다.")
        return
    agg = (
        spans.groupby(["dt", "kind"], as_index=False)
        .agg(spans=("span_id", "count"), avg_ms=("duration_ms", "mean"), p95_ms=("duration_ms", lambda s: s.quantile(0.95)))
    )
    st.altair_chart(
        alt.Chart(agg, title="span 종류별 평균 지연 (ms)")
        .mark_bar()
        .encode(
            y=alt.Y("kind:N", sort=KIND_ORDER, title=None),
            x=alt.X("avg_ms:Q", title="평균 ms"),
            yOffset="dt:N",
            color=alt.Color("dt:N", title=None, legend=alt.Legend(orient="bottom")),
            tooltip=["dt", "kind", "spans", alt.Tooltip("avg_ms:Q", format=".1f"), alt.Tooltip("p95_ms:Q", format=".1f")],
        )
        .properties(height=240),
        use_container_width=True,
    )


def render(con: duckdb.DuckDBPyConnection, spans: pd.DataFrame) -> None:
    render_status_strip()
    st.divider()
    render_kpis(con)
    left, right = st.columns(2)
    with left:
        render_ingest(con)
        render_tools(con)
    with right:
        render_throughput(con)
        render_span_kinds(spans)
