"""LLM 클라이언트. provider=claude일 때만 외부 호출이 발생한다.

mock 모드의 동작(스킬 선택·라벨링·설명 개선)은 각 컴포넌트의 Mock 구현이
담당하고, 이 모듈은 실제 API 호출과 토큰·비용 계측만 책임진다.
"""
from __future__ import annotations

import time
from dataclasses import dataclass

from common.config import load_config

# 모델별 단가 (USD / 1M tokens: 입력, 출력). Haiku 5.5는 프롬프트 100K 토큰 이하 기준.
PRICES_PER_M = {
    "claude-haiku-5-5": (0.10, 0.50),
    "claude-sonnet-5-5": (2.00, 10.00),
    "claude-opus-5-5": (4.00, 20.00),
}


@dataclass
class LLMResponse:
    text: str
    tokens_in: int
    tokens_out: int
    cost_usd: float
    latency_ms: int


def estimate_cost(tokens_in: int, tokens_out: int, model: str | None = None) -> float:
    model = model or load_config()["llm"]["model"]
    if model not in PRICES_PER_M:
        raise ValueError(f"단가가 등록되지 않은 모델: {model} (common/llm.py의 PRICES_PER_M에 추가)")
    price_in, price_out = PRICES_PER_M[model]
    return tokens_in / 1e6 * price_in + tokens_out / 1e6 * price_out


class ClaudeClient:
    def __init__(self, model: str | None = None) -> None:
        import anthropic

        cfg = load_config()["llm"]
        self.model = model or cfg["model"]
        self.max_tokens = cfg["max_tokens"]
        # ANTHROPIC_API_KEY(.env) 등 환경의 자격증명을 SDK가 직접 해석한다
        self.client = anthropic.Anthropic()

    def complete(self, system: str, user: str) -> LLMResponse:
        start = time.monotonic()
        message = self.client.messages.create(
            model=self.model,
            max_tokens=self.max_tokens,
            system=system,
            messages=[{"role": "user", "content": user}],
        )
        latency_ms = int((time.monotonic() - start) * 1000)
        tokens_in = message.usage.input_tokens
        tokens_out = message.usage.output_tokens
        return LLMResponse(
            # 사고(thinking) 블록이 앞에 올 수 있으므로 text 블록만 모은다
            text="".join(block.text for block in message.content if block.type == "text"),
            tokens_in=tokens_in,
            tokens_out=tokens_out,
            cost_usd=estimate_cost(tokens_in, tokens_out, self.model),
            latency_ms=latency_ms,
        )


def fake_usage(system: str, user: str, output: str) -> LLMResponse:
    """mock 경로에서도 토큰·비용 지표가 파이프라인에 흐르도록 근사치 생성."""
    tokens_in = (len(system) + len(user)) // 3
    tokens_out = max(len(output) // 3, 5)
    return LLMResponse(
        text=output,
        tokens_in=tokens_in,
        tokens_out=tokens_out,
        cost_usd=estimate_cost(tokens_in, tokens_out),
        latency_ms=0,
    )
