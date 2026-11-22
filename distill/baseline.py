"""증류 베이스라인: TF-IDF(char n-gram) + 로지스틱 회귀.

교사 라벨: 캐스케이드 라벨러가 train split 질의에 붙인 intent.
평가: 골든셋 gold intent 기준 (교사 라벨 기준이 아님 → 증류 손실까지 측정).
사용: python -m distill.baseline
"""
from __future__ import annotations

import json
import pickle
from pathlib import Path

from sklearn.linear_model import LogisticRegression
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.pipeline import Pipeline

from agent.runner import load_testset
from common.config import PROJECT_ROOT
from eval.label_quality import _accuracy, _macro_f1, load_goldenset
from telemetry.labeler import CascadeLabeler

MODELS_DIR = PROJECT_ROOT / "models"
MODEL_PATH = MODELS_DIR / "baseline_intent.pkl"


def build_teacher_data() -> tuple[list[str], list[str]]:
    """학습용으로 별도 생성한 테스트 트래픽(train split)에만 교사 라벨을 붙인다."""
    labeler = CascadeLabeler()
    cases = load_testset("testsets/testset_v1.jsonl", split="train")
    queries = [case["query"] for case in cases]
    labels = [str(labeler.label(q).tags["intent"].value) for q in queries]
    return queries, labels


def train() -> Pipeline:
    queries, labels = build_teacher_data()
    model = Pipeline(
        [
            ("tfidf", TfidfVectorizer(analyzer="char_wb", ngram_range=(2, 4), min_df=1)),
            ("clf", LogisticRegression(max_iter=2000, C=5.0)),
        ]
    )
    model.fit(queries, labels)
    MODELS_DIR.mkdir(exist_ok=True)
    with open(MODEL_PATH, "wb") as f:
        pickle.dump(model, f)
    return model


def evaluate(model: Pipeline) -> dict:
    goldenset = [g for g in load_goldenset()]
    queries = [g["query"] for g in goldenset]
    gold = [g["gold"]["intent"] for g in goldenset]
    pred = list(model.predict(queries))
    return {
        "model": "tfidf+logreg",
        "items": len(gold),
        "intent_accuracy": round(_accuracy(pred, gold), 4),
        "intent_macro_f1": round(_macro_f1(pred, gold), 4),
        "cost_per_1k_usd": 0.0,
    }


def main() -> dict:
    model = train()
    metrics = evaluate(model)
    print(json.dumps(metrics, ensure_ascii=False, indent=2))
    return metrics


if __name__ == "__main__":
    main()
