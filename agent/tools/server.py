"""FastMCP 툴 서버. 기본은 목 데이터, 공공 API 키가 있으면 교체 가능.

`python -m agent.tools.server` 로 단독 MCP 서버 실행.
Runner는 같은 함수들을 in-process로 직접 호출한다.
"""
from __future__ import annotations

import random
import time

from fastmcp import FastMCP

from common.config import load_config

mcp = FastMCP("skill-eval-tools")

_SUBWAY_LINES = ["1호선", "2호선", "3호선", "4호선", "9호선", "신분당선"]
_WEATHER = ["맑음", "흐림", "비", "눈", "구름 많음"]
_PERFORMANCES = [
    {"title": "뮤지컬 위키드", "venue": "블루스퀘어", "genre": "뮤지컬"},
    {"title": "연극 라면", "venue": "대학로 아트원씨어터", "genre": "연극"},
    {"title": "서울재즈페스티벌", "venue": "올림픽공원", "genre": "콘서트"},
    {"title": "국립발레단 호두까기인형", "venue": "예술의전당", "genre": "무용"},
]


@mcp.tool
def subway_arrival(station: str, line: str | None = None) -> dict:
    """지정한 역의 실시간 지하철 도착 정보를 조회한다."""
    rng = random.Random(hash(station) & 0xFFFF)
    return {
        "station": station,
        "line": line or rng.choice(_SUBWAY_LINES),
        "arrivals": [
            {"direction": "상행", "eta_min": rng.randint(1, 8)},
            {"direction": "하행", "eta_min": rng.randint(1, 12)},
        ],
    }


@mcp.tool
def subway_route(origin: str, destination: str) -> dict:
    """출발역에서 도착역까지의 지하철 경로와 환승 정보를 조회한다."""
    rng = random.Random(hash(origin + destination) & 0xFFFF)
    transfers = rng.randint(0, 2)
    return {
        "origin": origin,
        "destination": destination,
        "duration_min": rng.randint(15, 70),
        "transfers": transfers,
        "lines": rng.sample(_SUBWAY_LINES, transfers + 1),
    }


@mcp.tool
def weather_now(city: str = "서울") -> dict:
    """지정한 도시의 현재 날씨를 조회한다."""
    rng = random.Random(hash(city) & 0xFFFF)
    return {
        "city": city,
        "condition": rng.choice(_WEATHER),
        "temperature_c": rng.randint(-5, 30),
        "humidity_pct": rng.randint(20, 90),
    }


@mcp.tool
def weather_forecast(city: str = "서울", days: int = 3) -> dict:
    """지정한 도시의 단기 예보를 조회한다."""
    rng = random.Random(hash(city) & 0xFFF0)
    return {
        "city": city,
        "forecast": [
            {
                "day_offset": d + 1,
                "condition": rng.choice(_WEATHER),
                "high_c": rng.randint(5, 32),
                "low_c": rng.randint(-8, 20),
                "rain_prob_pct": rng.randint(0, 90),
            }
            for d in range(days)
        ],
    }


@mcp.tool
def performance_search(keyword: str = "", genre: str | None = None) -> dict:
    """공연(뮤지컬·연극·콘서트 등)을 검색한다. KOPIS API 연동 지점."""
    results = [
        p
        for p in _PERFORMANCES
        if (not genre or p["genre"] == genre)
        and (not keyword or keyword in p["title"] or keyword in p["genre"])
    ]
    return {"keyword": keyword, "results": results or _PERFORMANCES[:2]}


@mcp.tool
def flaky_tool(payload: str = "") -> dict:
    """일정 확률로 에러·타임아웃을 내는 툴. 성공률 지표 검증용."""
    cfg = load_config()["flaky_tool"]
    roll = random.random()
    if roll < cfg["timeout_rate"]:
        time.sleep(0.5)
        raise TimeoutError("flaky_tool timeout")
    if roll < cfg["timeout_rate"] + cfg["error_rate"]:
        raise RuntimeError("flaky_tool random failure")
    return {"ok": True, "payload": payload}


# Runner가 in-process로 쓰는 매핑 (FastMCP 데코레이터는 원본 함수를 그대로 반환)
TOOL_FUNCTIONS = {
    "subway_arrival": subway_arrival,
    "subway_route": subway_route,
    "weather_now": weather_now,
    "weather_forecast": weather_forecast,
    "performance_search": performance_search,
    "flaky_tool": flaky_tool,
}

if __name__ == "__main__":
    mcp.run()
