"""의도 태그 라벨러. Jev(mock / TypeSafe AI HTTP) → 생성형 LLM 캐스케이드.

본문(query)은 이 모듈 안에서만 소비되고, 결과로는 태그·확률·출처만 반환한다.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import time
from dataclasses import dataclass, field
from pathlib import Path

import requests
import yaml

from common.config import load_config, resolve_path
from common.events import LabelerInfo, Tag
from common.llm import ClaudeClient, fake_usage

INTENT_KEYWORDS: dict[str, list[str]] = {
    "교통_조회": ["지하철", "호선", "역에", "역이", "도착", "환승", "노선", "막차", "첫차", "몇 분", "언제 와", "가는 법", "가려면"],
    "날씨_조회": ["날씨", "비 와", "비 올", "눈 올", "기온", "우산", "예보", "미세먼지", "더워", "추워", "맑"],
    "문화_탐색": ["공연", "뮤지컬", "연극", "콘서트", "전시", "페스티벌", "예매", "볼만한"],
    "복합_브리핑": ["일정", "스케줄", "브리핑", "요약해", "정리해", "계획 짜"],
}

SKILL_EXPECTED_INTENTS: dict[str, set[str]] = {
    "subway_arrival": {"교통_조회"},
    "subway_route": {"교통_조회"},
    "weather_now": {"날씨_조회"},
    "weather_forecast": {"날씨_조회"},
    "performance_search": {"문화_탐색"},
    "commute_briefing": {"복합_브리핑", "교통_조회", "날씨_조회"},
    "weekend_performance": {"문화_탐색", "날씨_조회", "복합_브리핑"},
}


@dataclass
class LabelResult:
    tags: dict[str, Tag]
    labeler: LabelerInfo
    cost_usd: float = 0.0
    latency_ms: int = 0


def load_taxonomy() -> dict:
    path = resolve_path(load_config()["labeler"]["taxonomy"])
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _jitter(query: str, scale: float = 0.06) -> float:
    """재현 가능한 의사 노이즈. 보정 곡선이 퇴화하지 않게 확률에 섞는다."""
    digest = int(hashlib.md5(query.encode()).hexdigest()[:6], 16)
    return (digest % 1000 / 1000 - 0.5) * 2 * scale


class MockJevLabeler:
    """키워드 기반 경량 라벨러. 실제 Jev API가 생기면 HTTPJevLabeler로 교체."""

    def __init__(self) -> None:
        cfg = load_config()["labeler"]["jev"]
        self.model = cfg["model"]
        self.cost_per_item = cfg["cost_per_1k_usd"] / 1000
        self.taxonomy_version = load_taxonomy()["taxonomy_version"]

    def label(self, query: str, selected_skill: str | None = None) -> LabelResult:
        hits = {
            intent: sum(1 for kw in keywords if kw in query)
            for intent, keywords in INTENT_KEYWORDS.items()
        }
        total = sum(hits.values())
        best = max(hits, key=lambda k: (hits[k], k))
        if total == 0:
            intent, intent_p = "기타", min(0.95, 0.8 + _jitter(query))
        else:
            intent = best
            intent_p = min(0.98, max(0.3, hits[best] / (total + 0.6) + _jitter(query)))

        out_of_scope = total == 0
        oos_p = min(0.97, max(0.1, (0.85 if out_of_scope else 0.9) + _jitter(query + "oos")))

        complexity = max(1, min(5, sum(1 for h in hits.values() if h > 0)))

        tags = {
            "intent": Tag(value=intent, p=round(intent_p, 3)),
            "out_of_scope": Tag(value=out_of_scope, p=round(oos_p, 3)),
            "complexity": Tag(value=complexity, p=round(min(0.95, 0.7 + _jitter(query + "cx", 0.1)), 3)),
        }
        if selected_skill:
            expected = SKILL_EXPECTED_INTENTS.get(selected_skill, set())
            fit = intent in expected
            fit_p = min(0.97, max(0.15, (0.55 + 0.4 * intent_p if fit else 0.75) + _jitter(query + "fit")))
            tags["skill_fit"] = Tag(value=fit, p=round(fit_p, 3))

        return LabelResult(
            tags=tags,
            labeler=LabelerInfo(
                model=self.model,
                taxonomy_version=self.taxonomy_version,
                calibrated=True,
                source="jev",
            ),
            cost_usd=self.cost_per_item,
            latency_ms=4,
        )


class HTTPJevLabeler:
    """TypeSafe AI Jev(System One API) 실연동.

    taxonomy의 질문을 Jev 질문 타입(choice / noul / score)으로 변환해 한 요청에 묶고,
    보정된 확률(probabilities / noul)을 Tag.p로 기록한다. 본문(query)은 요청 state에만
    실리고 반환값에는 태그·확률·비용만 남는다.
    """

    TYPE_MAP = {"choice": "choice", "bool": "noul", "noul": "noul", "score": "score"}
    RETRYABLE = {429, 529}

    def __init__(self) -> None:
        cfg = load_config()["labeler"]["jev"]
        if not cfg.get("endpoint"):
            raise ValueError("labeler.jev.endpoint가 설정되지 않음")
        api_key = os.environ.get(cfg.get("api_key_env", "JEV_API_KEY"), "")
        if not api_key:
            raise ValueError(f"{cfg.get('api_key_env', 'JEV_API_KEY')} 환경변수(.env)가 비어 있음")
        self.endpoint = cfg["endpoint"]
        self.model = cfg["model"]
        self.timeout_s = cfg.get("timeout_s", 20)
        self.max_retries = cfg.get("max_retries", 4)
        self.price_per_token = cfg.get("price_per_mtok_usd", 0.042) / 1e6
        self.headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        self.taxonomy = load_taxonomy()
        self.questions = self._build_questions(self.taxonomy["questions"])
        self._skill_descriptions: dict[str, str] | None = None

    # --- 요청 구성 -------------------------------------------------------

    def _build_questions(self, spec: dict) -> dict[str, dict]:
        questions = {}
        for name, q in spec.items():
            jev_type = self.TYPE_MAP[q["type"]]
            question: dict = {"type": jev_type, "instructions": q["instructions"]}
            if jev_type == "choice":
                question["criteria"] = dict(q["options"])
            elif jev_type == "score":
                levels = q.get("levels")
                if not levels:
                    lo, hi = q.get("scale", [1, 5])
                    levels = [f"{i} step(s)" for i in range(lo, hi + 1)]
                question["criteria"] = list(levels)
            questions[name] = question
        return questions

    def _skill_descriptions_map(self) -> dict[str, str]:
        if self._skill_descriptions is None:
            from agent.selector import load_skills

            self._skill_descriptions = {
                name: skill.description for name, skill in load_skills().items()
            }
        return self._skill_descriptions

    def _payload(self, query: str, selected_skill: str | None) -> dict:
        state: dict = {"request": query}
        questions = dict(self.questions)
        if selected_skill and selected_skill != "none":
            state["selected_skill"] = selected_skill
            state["skill_descriptions"] = self._skill_descriptions_map()
        else:
            questions.pop("skill_fit", None)  # 선택된 스킬이 없으면 적합도 질문은 무의미
        return {"model": self.model, "state": state, "questions": questions}

    # --- 호출 -------------------------------------------------------------

    def _post(self, payload: dict) -> tuple[dict, int]:
        start = time.monotonic()
        for attempt in range(self.max_retries + 1):
            response = requests.post(
                self.endpoint, json=payload, headers=self.headers, timeout=self.timeout_s
            )
            if response.status_code in self.RETRYABLE and attempt < self.max_retries:
                retry_after = response.headers.get("retry-after")
                delay = float(retry_after) if retry_after else min(1.0 * 2**attempt, 30.0)
                time.sleep(delay)
                continue
            response.raise_for_status()
            return response.json(), int((time.monotonic() - start) * 1000)
        raise RuntimeError("Jev 재시도 한도 초과")

    # --- 응답 → Tag -------------------------------------------------------

    def _to_tags(self, answers: dict) -> dict[str, Tag]:
        tags: dict[str, Tag] = {}
        spec = self.taxonomy["questions"]
        for name, answer in answers.items():
            kind = answer["type"]
            if kind == "choice":
                value = answer["choice"]
                p = answer["probabilities"].get(value, answer.get("confidence", 0.0))
            elif kind == "noul":
                yes_p = float(answer["noul"])
                value = yes_p >= 0.5
                p = yes_p if value else 1.0 - yes_p  # 기록한 값(true/false)의 확률
            elif kind == "score":
                probs = answer["probabilities"]
                level = max(probs, key=probs.get)
                offset = spec.get(name, {}).get("scale", [1, 5])[0]
                value = int(level) + offset
                p = probs[level]
            else:
                continue
            tags[name] = Tag(value=value, p=round(min(max(float(p), 0.0), 1.0), 3))
        return tags

    def label(self, query: str, selected_skill: str | None = None) -> LabelResult:
        body, latency_ms = self._post(self._payload(query, selected_skill))
        usage = body.get("usage", {})
        return LabelResult(
            tags=self._to_tags(body["answers"]),
            labeler=LabelerInfo(
                model=body.get("model", self.model),
                taxonomy_version=self.taxonomy["taxonomy_version"],
                calibrated=True,
                source="jev",
            ),
            cost_usd=int(usage.get("input_tokens", 0)) * self.price_per_token,
            latency_ms=latency_ms,
        )


class LLMLabeler:
    """캐스케이드 상단. provider=claude면 실제 호출, 아니면 확장 키워드 mock."""

    SYSTEM = (
        "당신은 로그 라벨러다. taxonomy의 각 질문에 답하고 확신도(0~1)를 붙여 "
        "JSON으로만 답한다: "
        '{"intent": {"value": "...", "p": 0.9}, "out_of_scope": {"value": false, "p": 0.9}, '
        '"skill_fit": {"value": true, "p": 0.9}, "complexity": {"value": 2, "p": 0.8}}'
    )

    def __init__(self, model: str | None = None) -> None:
        cfg = load_config()
        self.taxonomy = load_taxonomy()
        self.use_claude = cfg["llm"]["provider"] == "claude"
        self.client = ClaudeClient(model) if self.use_claude else None
        self.model = self.client.model if self.use_claude else "mock-llm"

    def label(self, query: str, selected_skill: str | None = None) -> LabelResult:
        if self.use_claude:
            return self._label_claude(query, selected_skill)
        return self._label_mock(query, selected_skill)

    def _label_claude(self, query: str, selected_skill: str | None) -> LabelResult:
        user = (
            f"taxonomy:\n{yaml.safe_dump(self.taxonomy, allow_unicode=True)}\n"
            f"request: {query}\nselected_skill: {selected_skill or 'none'}"
        )
        response = self.client.complete(self.SYSTEM, user)
        tags: dict[str, Tag] = {}
        try:
            parsed = json.loads(re.search(r"\{.*\}", response.text, re.DOTALL).group())
            for key, item in parsed.items():
                tags[key] = Tag(value=item["value"], p=float(item["p"]))
        except (AttributeError, json.JSONDecodeError, KeyError, TypeError, ValueError):
            tags["intent"] = Tag(value="기타", p=0.0)
        return LabelResult(
            tags=tags,
            labeler=LabelerInfo(
                model=self.model,
                taxonomy_version=self.taxonomy["taxonomy_version"],
                calibrated=False,
                source="llm",
            ),
            cost_usd=response.cost_usd,
            latency_ms=response.latency_ms,
        )

    # 상위 라벨러는 더 넓은 어휘를 이해한다는 가정의 확장 사전
    EXTENDED_KEYWORDS: dict[str, list[str]] = {
        "교통_조회": ["열차", "타야", "타고", "첫차", "어디쯤", "다음 차", "경로", "루트"],
        "날씨_조회": ["내일", "모레", "일기예보", "몇 도", "도야", "온대", "오니", "덥", "입을지"],
        "문화_탐색": ["나들이", "발레", "재즈", "뭐 볼", "볼지", "문화생활", "데이트"],
        "복합_브리핑": ["아침", "출근", "모아", "준비", "챙길", "같이 알려"],
    }

    def _label_mock(self, query: str, selected_skill: str | None) -> LabelResult:
        """Jev mock보다 정확도·확신도가 높은 상위 라벨러를 흉내 낸다."""
        base = MockJevLabeler().label(query, selected_skill)
        hits = {
            intent: sum(1 for kw in INTENT_KEYWORDS[intent] + self.EXTENDED_KEYWORDS[intent] if kw in query)
            for intent in INTENT_KEYWORDS
        }
        total = sum(hits.values())
        if total > 0:
            best = max(hits, key=lambda k: (hits[k], k))
            base.tags["intent"] = Tag(
                value=best,
                p=round(min(0.99, max(0.5, hits[best] / (total + 0.4) + _jitter(query + "llm"))), 3),
            )
            base.tags["out_of_scope"] = Tag(value=False, p=round(min(0.99, 0.9 + _jitter(query + "o2")), 3))
        for tag in base.tags.values():
            tag.p = round(min(0.99, tag.p + 0.08), 3)
        usage = fake_usage(self.SYSTEM, query, json.dumps({k: t.model_dump() for k, t in base.tags.items()}))
        base.labeler = LabelerInfo(
            model=self.model,
            taxonomy_version=self.taxonomy["taxonomy_version"],
            calibrated=False,
            source="llm",
        )
        base.cost_usd = max(usage.cost_usd, load_config()["labeler"]["llm_cost_per_1k_usd"] / 1000)
        base.latency_ms = 350
        return base


def create_jev_labeler():
    """config의 labeler.jev.provider에 따라 mock 또는 실제 Jev."""
    provider = load_config()["labeler"]["jev"]["provider"]
    return HTTPJevLabeler() if provider == "http" else MockJevLabeler()


class CascadeLabeler:
    """Jev 우선, 최소 확신도가 τ 미만이면 LLM으로 승급."""

    def __init__(self) -> None:
        labeler_cfg = load_config()["labeler"]
        self.tau = labeler_cfg["cascade"]["tau"]
        self.jev = create_jev_labeler()
        self.llm = LLMLabeler()

    def label(self, query: str, selected_skill: str | None = None) -> LabelResult:
        jev_result = self.jev.label(query, selected_skill)
        if jev_result.tags["intent"].p >= self.tau:
            return jev_result
        llm_result = self.llm.label(query, selected_skill)
        llm_result.cost_usd += jev_result.cost_usd
        return llm_result


def create_labeler(kind: str = "cascade"):
    if kind == "jev":
        return create_jev_labeler()
    if kind == "llm":
        return LLMLabeler()
    return CascadeLabeler()
