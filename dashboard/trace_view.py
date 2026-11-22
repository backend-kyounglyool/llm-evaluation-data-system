"""트레이스 뷰 데이터 가공 + 차트(워터폴·플레임 그래프) 빌더.

Streamlit 의존 없음 — 헤드리스 테스트 가능.
"""
from __future__ import annotations

import altair as alt
import duckdb
import pandas as pd

from common.config import load_config, resolve_path

# span kind별 고정 색 (dataviz 검증 팔레트, 엔티티 고정 매핑)
KIND_ORDER = ["agent", "workflow", "llm", "thinking", "tool", "labeler", "task"]
KIND_COLORS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7"]
KIND_SCALE = alt.Scale(domain=KIND_ORDER, range=KIND_COLORS)


def load_spans() -> pd.DataFrame:
    trace_dir = resolve_path(load_config()["tracing"]["dir"])
    glob = f"{trace_dir}/spans-*.jsonl"
    try:
        df = duckdb.sql(
            f"SELECT * FROM read_json_auto('{glob}', format='newline_delimited')"
        ).df()
    except duckdb.IOException:
        return pd.DataFrame()
    if df.empty:
        return df
    df = df.drop_duplicates(subset="span_id").reset_index(drop=True)
    for column in ("tokens_in", "tokens_out", "cost_usd"):
        if column in df:
            df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0)
    return df


def trace_summaries(spans: pd.DataFrame) -> pd.DataFrame:
    roots = spans[spans["parent_span_id"].isna()].set_index("trace_id")
    grouped = spans.groupby("trace_id").agg(
        spans=("span_id", "count"),
        start_ns=("start_ns", "min"),
        end_ns=("end_ns", "max"),
        tokens_in=("tokens_in", "sum"),
        tokens_out=("tokens_out", "sum"),
        cost_usd=("cost_usd", "sum"),
    )
    grouped["duration_ms"] = ((grouped["end_ns"] - grouped["start_ns"]) / 1e6).round(1)
    for column in ("name", "run_id", "test_id", "skill", "status", "ts"):
        if column in roots:
            grouped[column] = roots[column]
    has_error = spans[spans["status"].isin(["error", "timeout"])].groupby("trace_id").size()
    grouped["errors"] = has_error.reindex(grouped.index).fillna(0).astype(int)
    return grouped.sort_values("start_ns", ascending=False).reset_index()


def trace_tree(spans: pd.DataFrame, trace_id: str) -> pd.DataFrame:
    """트레이스 하나를 DFS 순서·depth·타임라인 오프셋이 붙은 프레임으로."""
    df = spans[spans["trace_id"] == trace_id].copy()
    t0 = df["start_ns"].min()
    df["start_ms"] = (df["start_ns"] - t0) / 1e6
    df["end_ms"] = (df["end_ns"] - t0) / 1e6

    children: dict[str | None, list[dict]] = {}
    for row in df.sort_values("start_ns").to_dict("records"):
        children.setdefault(row["parent_span_id"], []).append(row)

    known_ids = set(df["span_id"])
    rows: list[dict] = []

    def visit(row: dict, depth: int) -> None:
        row["depth"] = depth
        row["order"] = len(rows)
        row["label"] = f"{'┆ ' * depth}{row['name']}"
        rows.append(row)
        for child in children.get(row["span_id"], []):
            visit(child, depth + 1)

    orphan_roots = [
        row for parent, group in children.items()
        if parent is None or parent not in known_ids
        for row in group
    ]
    for root in sorted(orphan_roots, key=lambda r: r["start_ns"]):
        visit(root, 0)
    return pd.DataFrame(rows)


def _base_tooltip() -> list:
    return [
        alt.Tooltip("name:N", title="span"),
        alt.Tooltip("kind:N", title="kind"),
        alt.Tooltip("duration_ms:Q", title="duration(ms)", format=".1f"),
        alt.Tooltip("tokens_in:Q", title="tokens in", format=".0f"),
        alt.Tooltip("tokens_out:Q", title="tokens out", format=".0f"),
        alt.Tooltip("cost_usd:Q", title="cost($)", format=".5f"),
        alt.Tooltip("status:N", title="status"),
        alt.Tooltip("model:N", title="model"),
    ]


def waterfall_chart(tree: pd.DataFrame) -> alt.Chart:
    height = max(28 * len(tree), 120)
    return (
        alt.Chart(tree, title="Waterfall")
        .mark_bar(cornerRadius=3, height=14, stroke="#fcfcfb", strokeWidth=1)
        .encode(
            x=alt.X("start_ms:Q", title="ms (트레이스 시작 기준)"),
            x2="end_ms:Q",
            y=alt.Y("label:N", sort=tree["label"].tolist(), title=None),
            color=alt.Color("kind:N", scale=KIND_SCALE, title="span kind"),
            tooltip=_base_tooltip(),
        )
        .properties(height=height, width="container")
    )


def flame_chart(tree: pd.DataFrame) -> alt.Chart:
    depth_count = int(tree["depth"].max()) + 1
    return (
        alt.Chart(tree, title="Flame graph (시간축 정렬)")
        .mark_rect(stroke="#fcfcfb", strokeWidth=2, cornerRadius=2)
        .encode(
            x=alt.X("start_ms:Q", title="ms"),
            x2="end_ms:Q",
            y=alt.Y("depth:O", title="depth", sort="ascending"),
            color=alt.Color("kind:N", scale=KIND_SCALE, title="span kind"),
            tooltip=_base_tooltip(),
        )
        .properties(height=max(44 * depth_count, 120), width="container")
    )
