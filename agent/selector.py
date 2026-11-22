"""스킬 선택기. SKILL.md 설명을 근거로 질의에 맞는 스킬을 고른다.

- MockSelector: 설명-질의 토큰 중첩 기반. 설명이 좋아지면 성능이 좋아지므로
  설명 개선 루프를 LLM 없이도 end-to-end로 검증할 수 있다.
- LLMSelector: Claude에게 스킬 목록과 설명을 주고 JSON으로 선택받는다.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path

from common.config import load_config, resolve_path
from common.llm import ClaudeClient, fake_usage

NONE_SKILL = "none"


@dataclass
class Skill:
    name: str
    version: str
    tools: list[str]
    description: str


@dataclass
class SelectionResult:
    skill: str  # 스킬 이름 또는 "none"
    confidence: float
    tokens_in: int = 0
    tokens_out: int = 0
    cost_usd: float = 0.0
    latency_ms: int = 0
    reason: str = ""


def load_skills(skills_dir: str | None = None) -> dict[str, Skill]:
    cfg = load_config()["selector"]
    base = Path(resolve_path(skills_dir or cfg["skills_dir"]))
    skills: dict[str, Skill] = {}
    for skill_md in sorted(base.glob("*/SKILL.md")):
        skills[skill_md.parent.name] = parse_skill_md(skill_md.read_text(encoding="utf-8"))
    return skills


def parse_skill_md(text: str) -> Skill:
    match = re.match(r"^---\n(.*?)\n---\n(.*)$", text, re.DOTALL)
    if not match:
        raise ValueError("SKILL.md frontmatter 형식 오류")
    import yaml

    meta = yaml.safe_load(match.group(1))
    return Skill(
        name=meta["name"],
        version=str(meta.get("version", "v1")),
        tools=list(meta.get("tools", [])),
        description=match.group(2).strip(),
    )


def render_skill_md(skill: Skill) -> str:
    tools = ", ".join(skill.tools)
    return (
        f"---\nname: {skill.name}\nversion: {skill.version}\ntools: [{tools}]\n---\n\n"
        f"{skill.description}\n"
    )


_PARTICLES = (
    "에서", "에게", "으로", "처럼", "부터", "까지", "보다",
    "을", "를", "이", "가", "은", "는", "에", "의", "로", "과", "와", "도", "만",
)
_STOPWORDS = {"관련", "정보", "질문", "처리", "제공", "필요", "그리고", "또는", "예시", "키워드"}


def content_tokens(text: str) -> set[str]:
    tokens: set[str] = set()
    for word in re.split(r"[\s,.?!·:;()\[\]\"'/]+", text):
        if not word:
            continue
        for particle in _PARTICLES:
            if word.endswith(particle) and len(word) > len(particle) + 1:
                word = word[: -len(particle)]
                break
        if len(word) < 2 or word.endswith("다") or word in _STOPWORDS:
            continue
        tokens.add(word)
    return tokens


class MockSelector:
    def __init__(self, skills: dict[str, Skill]) -> None:
        self.skills = skills
        self.skill_tokens = {
            name: content_tokens(skill.description) for name, skill in skills.items()
        }

    def select(self, query: str) -> SelectionResult:
        scores = {
            name: sum(1 for token in tokens if token in query)
            for name, tokens in self.skill_tokens.items()
        }
        best = max(scores, key=lambda name: (scores[name], name))
        total = sum(scores.values())
        if scores[best] == 0:
            result = SelectionResult(skill=NONE_SKILL, confidence=0.9, reason="no keyword overlap")
        else:
            confidence = round(scores[best] / (total + 1), 3)
            result = SelectionResult(skill=best, confidence=confidence, reason="keyword overlap")
        usage = fake_usage(self._skill_catalog(), query, result.skill)
        result.tokens_in, result.tokens_out, result.cost_usd = (
            usage.tokens_in, usage.tokens_out, usage.cost_usd,
        )
        return result

    def _skill_catalog(self) -> str:
        return "\n".join(f"{s.name}: {s.description}" for s in self.skills.values())


class LLMSelector:
    SYSTEM = (
        "당신은 에이전트의 스킬 라우터다. 사용자 질의에 가장 적합한 스킬 하나를 고르거나, "
        '적합한 스킬이 없으면 "none"을 반환한다. 반드시 JSON으로만 답한다: '
        '{"skill": "<스킬명 또는 none>", "confidence": 0.0~1.0, "reason": "<짧은 근거>"}'
    )

    def __init__(self, skills: dict[str, Skill]) -> None:
        self.skills = skills
        self.client = ClaudeClient()

    def select(self, query: str) -> SelectionResult:
        catalog = "\n".join(f"- {s.name}: {s.description}" for s in self.skills.values())
        response = self.client.complete(self.SYSTEM, f"스킬 목록:\n{catalog}\n\n질의: {query}")
        try:
            parsed = json.loads(re.search(r"\{.*\}", response.text, re.DOTALL).group())
            skill = parsed.get("skill", NONE_SKILL)
            if skill not in self.skills and skill != NONE_SKILL:
                skill = NONE_SKILL
            return SelectionResult(
                skill=skill,
                confidence=float(parsed.get("confidence", 0.5)),
                tokens_in=response.tokens_in,
                tokens_out=response.tokens_out,
                cost_usd=response.cost_usd,
                latency_ms=response.latency_ms,
                reason=str(parsed.get("reason", "")),
            )
        except (AttributeError, json.JSONDecodeError, ValueError):
            return SelectionResult(skill=NONE_SKILL, confidence=0.0, reason="parse error")


def create_selector(skills: dict[str, Skill] | None = None):
    skills = skills if skills is not None else load_skills()
    provider = load_config()["llm"]["provider"]
    return LLMSelector(skills) if provider == "claude" else MockSelector(skills)
