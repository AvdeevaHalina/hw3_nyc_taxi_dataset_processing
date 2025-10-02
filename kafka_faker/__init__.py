"""Utilities for generating fake Kafka events and producing them.

Modules:
- transactions: transaction generator functions
- user_activity: user activity generator functions
- producer: Kafka/Stdout sinks
- cli: command-line entrypoint
"""
from .transactions import (
    generate_transaction,
    iter_transactions,
    generate_random_user_ids,
)
from .user_activity import generate_user_activity, iter_user_activity
from .producer import OutputSink, StdoutSink, KafkaSink, KafkaJsonProducer

__all__ = [
    "generate_transaction",
    "iter_transactions",
    "generate_random_user_ids",
    "generate_user_activity",
    "iter_user_activity",
    "OutputSink",
    "StdoutSink",
    "KafkaSink",
    "KafkaJsonProducer",
]
