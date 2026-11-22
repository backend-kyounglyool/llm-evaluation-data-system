"""스킬 설명 자동 개선 루프.

train split 실패 사례 → 설명 수정안 생성 → val split 재평가 →
(개선 & 회귀 없음)일 때만 채택. 결과는 PR 본문(markdown)으로 정리한다.

사용: python -m eval.improve_loop [--apply] [--pr]
  --apply  채택된 설명을 agent/skills/의 SKILL.md에 반영
  --pr     git 브랜치 + gh CLI로 실제 PR 생성 시도
"""
from __future__ import annotations

import argparse
import copy
import json
import subprocess
from collections import Counter, defaultdict
from pathlib import Path

from agent.selector import NONE_SKILL, Skill, content_tokens, load_skills, render_skill_md
from common.config import PROJECT_ROOT, load_config
from common.llm import ClaudeClient
from eval.selection_eval import evaluate

REPORTS_DIR = PROJECT_ROOT / "reports"
GENERIC_TOKENS = {"알려줘", "해줘", "보여줘", "봐줘", "띄워줘", "어때", "있어", "뭐가", "뭐야", "좋을까", "가능한", "하는데", "되는데", "싶은데"}


def group_failures(metrics: dict) -> dict[str, list[dict]]:
    """실패를 책임이 있는 스킬별로 묶는다.

    골라졌어야 하는 스킬(missed)뿐 아니라 잘못 골라진 스킬(wrongly_picked)에도 귀속한다.
    설명이 너무 넓어 범위 밖 질의를 끌어가는 스킬은 후자로만 드러난다.
    """
    grouped: dict[str, list[dict]] = defaultdict(list)
    for failure in metrics["failures"]:
        if failure["expected"] != NONE_SKILL:
            grouped[failure["expected"]].append({**failure, "role": "missed"})
        if failure["selected"] != NONE_SKILL:
            grouped[failure["selected"]].append({**failure, "role": "wrongly_picked"})
    return grouped


class MockImprover:
    """실패 질의에서 변별력 있는 키워드를 추출해 설명에 추가한다."""

    def improve(self, skill: Skill, failures: list[dict], all_failures: dict[str, list[dict]],
                siblings: list[Skill] | None = None) -> str:
        # 키워드 추가는 누락만 고칠 수 있다. 잘못 골라진 사례는 설명을 좁혀야 해서 mock은 다루지 않는다
        own_tokens = Counter()
        for failure in failures:
            if failure["role"] == "missed":
                own_tokens.update(self._usable_tokens(failure["query"]))

        other_tokens = set()
        for other_skill, other_cases in all_failures.items():
            if other_skill == skill.name:
                continue
            for case in other_cases:
                if case["role"] == "missed":
                    other_tokens |= self._usable_tokens(case["query"])

        existing = content_tokens(skill.description)
        candidates = [
            token
            for token, _ in own_tokens.most_common()
            if token not in existing and token not in other_tokens
        ][:6]
        if not candidates:
            return skill.description
        return f"{skill.description}\n\n키워드: {', '.join(candidates)}"

    @staticmethod
    def _usable_tokens(query: str) -> set[str]:
        return {
            token
            for token in content_tokens(query)
            if token not in GENERIC_TOKENS and not token.endswith("줘")
        }


class LLMImprover:
    SYSTEM = (
        "당신은 에이전트 스킬 설명을 개선하는 엔지니어다. 실패 사례를 보고 "
        "스킬 라우터가 이 스킬을 정확히 고를 수 있도록 설명을 다시 쓴다. "
        "다른 스킬과 혼동될 표현은 피하고, 트리거 키워드와 대표 예시를 포함한다. "
        "잘못 선택된 사례가 있으면 이 스킬이 다루지 않는 범위를 설명에 명시한다. "
        "다른 스킬과의 경계가 드러나게 쓰되, 다른 스킬 설명과 비슷한 분량으로 간결하게 쓴다. "
        "설명 본문만 출력한다."
    )

    def __init__(self) -> None:
        self.client = ClaudeClient()

    def improve(self, skill: Skill, failures: list[dict], all_failures: dict[str, list[dict]],
                siblings: list[Skill] | None = None) -> str:
        def listing(role: str) -> str:
            cases = [f for f in failures if f["role"] == role][:10]
            return "\n".join(
                f'- "{f["query"]}" → 선택됨: {f["selected"]} (정답: {f["expected"]})' for f in cases
            ) or "- 없음"

        # 형제 스킬을 모르고 고치면 설명이 서로 겹쳐 다른 스킬의 질의를 끌어온다
        catalog = "\n".join(f"- {s.name} (툴: {', '.join(s.tools)}): {s.description}" for s in siblings or [])
        user = (
            f"스킬: {skill.name}\n사용하는 툴: {', '.join(skill.tools)}\n현재 설명:\n{skill.description}\n\n"
            f"같은 라우터에 있는 다른 스킬 (이 스킬들의 질의를 가져오면 안 된다):\n{catalog or '- 없음'}\n\n"
            f"이 스킬을 골랐어야 하는데 못 고른 사례:\n{listing('missed')}\n\n"
            f"이 스킬을 고르면 안 되는데 고른 사례:\n{listing('wrongly_picked')}\n\n개선된 설명:"
        )
        return self.client.complete(self.SYSTEM, user).text.strip()


def bump_version(version: str) -> str:
    try:
        return f"v{int(version.lstrip('v')) + 1}"
    except ValueError:
        return version + ".1"


def run_loop(apply: bool = False, create_pr: bool = False,
             testset: str = "testsets/testset_v1.jsonl") -> dict:
    cfg = load_config()
    loop_cfg = cfg["improve_loop"]
    use_llm = cfg["llm"]["provider"] == "claude"
    improver = LLMImprover() if use_llm else MockImprover()

    original = load_skills()
    current = copy.deepcopy(original)
    baseline_val = evaluate(current, testset, split="val")
    history = [{"iteration": 0, "val_accuracy": baseline_val["accuracy"]}]
    changed: dict[str, str] = {}  # skill -> 개선 전 설명

    for iteration in range(1, loop_cfg["max_iterations"] + 1):
        train_metrics = evaluate(current, testset, split="train")
        failures = group_failures(train_metrics)
        if not failures:
            break

        candidate = copy.deepcopy(current)
        touched = []
        for skill_name, cases in failures.items():
            siblings = [skill for skill in candidate.values() if skill.name != skill_name]
            new_desc = improver.improve(candidate[skill_name], cases, failures, siblings)
            if new_desc != candidate[skill_name].description:
                candidate[skill_name].description = new_desc
                candidate[skill_name].version = bump_version(candidate[skill_name].version)
                touched.append(skill_name)
        if not touched:
            break

        val_before = evaluate(current, testset, split="val")
        val_after = evaluate(candidate, testset, split="val")
        gain = val_after["accuracy"] - val_before["accuracy"]
        regressions = [
            skill
            for skill, m in val_before["per_skill"].items()
            if val_after["per_skill"].get(skill, {}).get("recall", 0) < m["recall"] - loop_cfg["max_recall_regression"]
        ]
        accepted = gain >= loop_cfg["min_val_accuracy_gain"] and not regressions
        history.append(
            {
                "iteration": iteration,
                "touched": touched,
                "val_accuracy": val_after["accuracy"],
                "gain": round(gain, 4),
                "regressions": regressions,
                "accepted": accepted,
            }
        )
        if not accepted:
            break
        for name in touched:
            changed.setdefault(name, original[name].description)
        current = candidate

    final_val = evaluate(current, testset, split="val")
    final_all = evaluate(current, testset, split="all")
    result = {
        "baseline_val_accuracy": baseline_val["accuracy"],
        "final_val_accuracy": final_val["accuracy"],
        "final_all_accuracy": final_all["accuracy"],
        "changed_skills": sorted(changed),
        "history": history,
    }

    REPORTS_DIR.mkdir(exist_ok=True)
    pr_body = render_pr_body(original, current, changed, baseline_val, final_val)
    (REPORTS_DIR / "improve_pr.md").write_text(pr_body, encoding="utf-8")

    if apply and changed:
        skills_dir = PROJECT_ROOT / cfg["selector"]["skills_dir"]
        for name in changed:
            (skills_dir / name / "SKILL.md").write_text(render_skill_md(current[name]), encoding="utf-8")

    if create_pr and changed:
        _try_create_pr(pr_body)

    print(json.dumps(result, ensure_ascii=False, indent=2))
    return result


def render_pr_body(original: dict[str, Skill], improved: dict[str, Skill],
                   changed: dict[str, str], val_before: dict, val_after: dict) -> str:
    lines = [
        "# 스킬 설명 자동 개선 제안",
        "",
        "## 지표 비교 (val split)",
        "",
        "| 지표 | before | after |",
        "| --- | --- | --- |",
        f"| accuracy | {val_before['accuracy']:.2%} | {val_after['accuracy']:.2%} |",
        f"| 오선택률 | {val_before['false_pick_rate']:.2%} | {val_after['false_pick_rate']:.2%} |",
        f"| 누락률 | {val_before['miss_rate']:.2%} | {val_after['miss_rate']:.2%} |",
        "",
        "| skill | recall before | recall after |",
        "| --- | --- | --- |",
    ]
    for skill, m in val_before["per_skill"].items():
        after = val_after["per_skill"].get(skill, {})
        lines.append(f"| {skill} | {m['recall']:.2f} | {after.get('recall', 0):.2f} |")

    lines += ["", "## 설명 변경 내역", ""]
    for name in sorted(changed):
        lines += [
            f"### {name} ({improved[name].version})",
            "",
            "```diff",
            *(f"- {line}" for line in changed[name].splitlines()),
            *(f"+ {line}" for line in improved[name].description.splitlines()),
            "```",
            "",
        ]

    fixed = [f for f in val_before["failures"] if f["id"] not in {x["id"] for x in val_after["failures"]}]
    lines += ["## 대표 개선 사례", ""]
    for failure in fixed[:3]:
        lines.append(f'- "{failure["query"]}": {failure["selected"]} → {failure["expected"]} (정답)')
    return "\n".join(lines) + "\n"


def _try_create_pr(pr_body: str) -> None:
    branch = "auto/improve-skill-descriptions"
    try:
        subprocess.run(["git", "checkout", "-b", branch], cwd=PROJECT_ROOT, check=True)
        subprocess.run(["git", "add", "agent/skills"], cwd=PROJECT_ROOT, check=True)
        subprocess.run(["git", "commit", "-m", "chore: 스킬 설명 자동 개선"], cwd=PROJECT_ROOT, check=True)
        subprocess.run(
            ["gh", "pr", "create", "--title", "스킬 설명 자동 개선", "--body", pr_body],
            cwd=PROJECT_ROOT,
            check=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        print(f"PR 자동 생성 실패({exc}). reports/improve_pr.md 내용으로 수동 생성 필요.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--pr", action="store_true")
    parser.add_argument("--testset", default="testsets/testset_v1.jsonl")
    args = parser.parse_args()
    run_loop(apply=args.apply, create_pr=args.pr, testset=args.testset)
