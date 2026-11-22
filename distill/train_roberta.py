"""klue/roberta-small 파인튜닝 (intent 분류).

requirements: pip install -e ".[distill]"  (transformers, torch, datasets)
Apple Silicon(MPS)·CUDA·CPU 자동 선택. Colab에서도 그대로 실행 가능.
사용: python -m distill.train_roberta [--epochs 8]
"""
from __future__ import annotations

import argparse
import json

from common.config import PROJECT_ROOT, load_config

MODELS_DIR = PROJECT_ROOT / "models" / "roberta_intent"


def main(epochs: int) -> None:
    import torch
    from datasets import Dataset
    from transformers import (
        AutoModelForSequenceClassification,
        AutoTokenizer,
        Trainer,
        TrainingArguments,
    )

    from distill.baseline import build_teacher_data
    from eval.label_quality import _accuracy, _macro_f1, load_goldenset

    base_model = load_config()["distill"]["base_model"]
    queries, labels = build_teacher_data()
    label_names = sorted(set(labels))
    label2id = {name: i for i, name in enumerate(label_names)}

    tokenizer = AutoTokenizer.from_pretrained(base_model)
    model = AutoModelForSequenceClassification.from_pretrained(
        base_model,
        num_labels=len(label_names),
        id2label={i: n for n, i in label2id.items()},
        label2id=label2id,
    )

    def tokenize(batch):
        return tokenizer(batch["text"], truncation=True, padding="max_length", max_length=64)

    dataset = Dataset.from_dict(
        {"text": queries, "label": [label2id[label] for label in labels]}
    ).map(tokenize, batched=True)

    device = "mps" if torch.backends.mps.is_available() else ("cuda" if torch.cuda.is_available() else "cpu")
    print(f"device: {device}")

    trainer = Trainer(
        model=model,
        args=TrainingArguments(
            output_dir=str(MODELS_DIR / "checkpoints"),
            num_train_epochs=epochs,
            per_device_train_batch_size=16,
            learning_rate=3e-5,
            logging_steps=10,
            save_strategy="no",
            report_to=[],
        ),
        train_dataset=dataset,
    )
    trainer.train()
    trainer.save_model(str(MODELS_DIR))
    tokenizer.save_pretrained(str(MODELS_DIR))

    goldenset = load_goldenset()
    texts = [g["query"] for g in goldenset]
    gold = [g["gold"]["intent"] for g in goldenset]
    model.eval()
    predictions = []
    with torch.no_grad():
        for i in range(0, len(texts), 32):
            batch = tokenizer(texts[i : i + 32], truncation=True, padding=True,
                              max_length=64, return_tensors="pt")
            logits = model(**batch).logits
            predictions += [label_names[idx] for idx in logits.argmax(-1).tolist()]

    metrics = {
        "model": base_model,
        "intent_accuracy": round(_accuracy(predictions, gold), 4),
        "intent_macro_f1": round(_macro_f1(predictions, gold), 4),
    }
    (MODELS_DIR / "metrics.json").write_text(json.dumps(metrics, ensure_ascii=False, indent=2))
    print(json.dumps(metrics, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--epochs", type=int, default=8)
    args = parser.parse_args()
    main(args.epochs)
