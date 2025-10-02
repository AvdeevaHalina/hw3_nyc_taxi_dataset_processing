from __future__ import annotations

import random
from datetime import datetime, timezone
from typing import Dict, Iterator, List, Optional, Sequence
from uuid import uuid4

MERCHANTS: List[str] = [
    "Amazon",
    "Walmart",
    "Target",
    "Starbucks",
    "Apple",
    "Best Buy",
    "Costco",
    "Uber",
    "Lyft",
    "Netflix",
    "Spotify",
    "Airbnb",
    "Expedia",
    "McDonald's",
    "Shell",
    "Chevron",
    "BP",
    "Home Depot",
    "Lowe's",
    "IKEA",
    "eBay",
    "AliExpress",
    "DoorDash",
    "Grubhub",
    "Sephora",
]


def _choose_amount_usd() -> float:
    """Generate a realistic USD amount with a heavy tail.

    Most transactions are small; a few are very large.
    """
    band = random.choices(
        ["small", "medium", "large", "xl"], weights=[0.65, 0.25, 0.09, 0.01]
    )[0]
    if band == "small":
        amount = random.uniform(1.00, 30.00)
    elif band == "medium":
        amount = random.uniform(30.00, 150.00)
    elif band == "large":
        amount = random.uniform(150.00, 1000.00)
    else:  # xl
        amount = random.uniform(1000.00, 5000.00)
    return round(amount, 2)


def _fraud_probability_for_amount(amount_usd: float) -> float:
    base_probability = 0.02
    if amount_usd >= 1000:
        return 0.15
    if amount_usd >= 250:
        return 0.05
    return base_probability


def generate_random_user_ids(count: int) -> List[str]:
    if count <= 0:
        return []
    return [str(uuid4()) for _ in range(count)]


def generate_transaction(
    *,
    user_id: Optional[str] = None,
    user_id_pool: Optional[Sequence[str]] = None,
    timestamp: Optional[datetime] = None,
) -> Dict[str, object]:
    """Create a single transaction record matching the schema.

    Schema:
        {
          "transaction_id": "uuid",
          "user_id": "uuid",
          "amount": float,
          "merchant": "str",
          "currency": "USD",
          "timestamp": "ISO8601",
          "is_fraud": bool
        }
    """
    if user_id is None:
        if user_id_pool:
            user_id = random.choice(list(user_id_pool))
        else:
            user_id = str(uuid4())

    event_time = timestamp or datetime.now(timezone.utc)
    amount = _choose_amount_usd()
    fraud_probability = _fraud_probability_for_amount(amount)

    record: Dict[str, object] = {
        "transaction_id": str(uuid4()),
        "user_id": user_id,
        "amount": amount,
        "merchant": random.choice(MERCHANTS),
        "currency": "USD",
        "timestamp": event_time.isoformat(timespec="seconds"),
        "is_fraud": random.random() < fraud_probability,
    }
    return record


def iter_transactions(
    *, count: Optional[int] = None, user_id_pool: Optional[Sequence[str]] = None
) -> Iterator[Dict[str, object]]:
    """Yield transaction records.

    If count is None, generate indefinitely.
    """
    emitted = 0
    while count is None or emitted < count:
        yield generate_transaction(user_id_pool=user_id_pool)
        emitted += 1
