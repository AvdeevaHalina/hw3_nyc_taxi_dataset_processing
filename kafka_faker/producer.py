from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, Optional

try:
    from kafka import KafkaProducer  # type: ignore
except Exception:  # pragma: no cover - library may not be installed yet
    KafkaProducer = None  # type: ignore


Record = Dict[str, object]


@dataclass
class OutputSink:
    def send(self, topic: str, record: Record) -> None:  # pragma: no cover - interface
        raise NotImplementedError


@dataclass
class StdoutSink(OutputSink):
    def send(self, topic: str, record: Record) -> None:
        line = json.dumps({"topic": topic, "value": record}, separators=(",", ":"))
        print(line, flush=True)


@dataclass
class KafkaSink(OutputSink):
    bootstrap_servers: str
    acks: str = "1"
    linger_ms: int = 20
    _producer: Optional[KafkaProducer] = None

    def _get_producer(self) -> KafkaProducer:
        if KafkaProducer is None:
            raise RuntimeError(
                "kafka-python is not installed. Install dependencies from requirements.txt"
            )
        if self._producer is None:
            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                acks=self.acks,
                linger_ms=self.linger_ms,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: (k or "").encode("utf-8"),
            )
        return self._producer

    def send(self, topic: str, record: Record) -> None:
        producer = self._get_producer()
        producer.send(topic, value=record)


@dataclass
class KafkaJsonProducer:
    sink: OutputSink
    sleep_seconds_between_messages: float = 0.0

    def run(self, topic: str, generator: Iterable[Record], limit: Optional[int] = None) -> int:
        sent = 0
        for record in generator:
            self.sink.send(topic, record)
            sent += 1
            if limit is not None and sent >= limit:
                break
            if self.sleep_seconds_between_messages > 0:
                time.sleep(self.sleep_seconds_between_messages)
        return sent
