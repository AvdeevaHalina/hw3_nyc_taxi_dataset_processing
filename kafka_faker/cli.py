from __future__ import annotations

import argparse
import sys
from typing import Iterable, List, Optional

from .producer import KafkaJsonProducer, KafkaSink, StdoutSink
from .transactions import iter_transactions, generate_random_user_ids
from .user_activity import iter_user_activity


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Emit fake events to Kafka or stdout."
    )

    parser.add_argument(
        "--sink",
        choices=["stdout", "kafka"],
        default="stdout",
        help="Where to send events.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers (host:port)",
    )
    parser.add_argument(
        "--topic",
        required=True,
        choices=["transactions", "user_activity"],
        help="Kafka topic to emit to.",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=0.0,
        help="Messages per second (approx). 0 means as fast as possible.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Total number of messages to send (0 = unlimited).",
    )
    parser.add_argument(
        "--users",
        type=int,
        default=100,
        help="Size of user id pool to sample from.",
    )

    return parser


def _choose_generator(topic: str, user_pool: List[str]):
    if topic == "transactions":
        return iter_transactions(user_id_pool=user_pool)
    if topic == "user_activity":
        return iter_user_activity(user_id_pool=user_pool)
    raise ValueError(f"Unsupported topic: {topic}")


def main(argv: Optional[List[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.sink == "kafka":
        sink = KafkaSink(args.bootstrap_servers)
    else:
        sink = StdoutSink()

    sleep_seconds = 0.0
    if args.rate and args.rate > 0:
        sleep_seconds = 1.0 / float(args.rate)

    limit = None if args.limit == 0 else int(args.limit)

    user_pool = generate_random_user_ids(max(1, int(args.users)))

    generator = _choose_generator(args.topic, user_pool)

    producer = KafkaJsonProducer(sink=sink, sleep_seconds_between_messages=sleep_seconds)
    sent = producer.run(args.topic, generator, limit=limit)

    return 0 if sent or limit == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
