"""이벤트 전송 계층. file(기본, JSONL append) / kafka(Apache Kafka·MSK) 전환 가능.

Consumer까지 동일 인터페이스로 묶어 pipeline 코드가 백엔드를 모르게 한다.
"""
from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol

from common.config import load_config, resolve_path
from common.events import AgentEvent, assert_no_body


class EventProducer(Protocol):
    def send(self, event: AgentEvent) -> None: ...
    def flush(self) -> None: ...


class FileProducer:
    """날짜별 JSONL 파일에 append. Kafka 토픽의 로컬 대체물."""

    def __init__(self, directory: str) -> None:
        self.dir = Path(resolve_path(directory))
        self.dir.mkdir(parents=True, exist_ok=True)

    def send(self, event: AgentEvent) -> None:
        payload = event.model_dump()
        assert_no_body(payload)
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        with open(self.dir / f"agent-events-{date}.jsonl", "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def flush(self) -> None:
        pass


class KafkaProducer:
    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        from confluent_kafka import Producer

        self.topic = topic
        self.producer = Producer({"bootstrap.servers": bootstrap_servers})

    def send(self, event: AgentEvent) -> None:
        payload = event.model_dump()
        assert_no_body(payload)
        self.producer.produce(
            self.topic,
            key=event.trace_id.encode(),
            value=json.dumps(payload, ensure_ascii=False).encode(),
        )

    def flush(self) -> None:
        self.producer.flush()


def create_producer() -> EventProducer:
    cfg = load_config()["transport"]
    if cfg["backend"] == "kafka":
        return KafkaProducer(cfg["kafka"]["bootstrap_servers"], cfg["kafka"]["topic"])
    return FileProducer(cfg["file"]["dir"])


def consume_events() -> Iterator[dict]:
    """적재된 전체 이벤트를 읽는다. kafka 모드는 earliest부터 1회 drain."""
    cfg = load_config()["transport"]
    if cfg["backend"] == "kafka":
        yield from _consume_kafka(cfg["kafka"])
    else:
        yield from _consume_files(cfg["file"]["dir"])


def _consume_files(directory: str) -> Iterator[dict]:
    path = Path(resolve_path(directory))
    if not path.exists():
        return
    for file in sorted(path.glob("agent-events-*.jsonl")):
        with open(file, encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    yield json.loads(line)


def _consume_kafka(kafka_cfg: dict) -> Iterator[dict]:
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": kafka_cfg["bootstrap_servers"],
            "group.id": "skill-eval-pipeline",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([kafka_cfg["topic"]])
    try:
        idle_polls = 0
        while idle_polls < 10:  # 1초 폴링 10회 연속 공백이면 drain 완료로 판단
            message = consumer.poll(1.0)
            if message is None:
                idle_polls += 1
                continue
            if message.error():
                continue
            idle_polls = 0
            yield json.loads(message.value().decode())
    finally:
        consumer.close()
