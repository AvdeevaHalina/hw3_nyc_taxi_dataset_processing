from __future__ import annotations

import random
from datetime import datetime, timezone
from typing import Dict, Iterator, List, Optional, Sequence
from uuid import uuid4

EVENT_TYPES: List[str] = ["click", "view", "add_to_cart", "purchase"]
EVENT_TYPE_WEIGHTS: List[float] = [0.30, 0.50, 0.15, 0.05]

DEVICES: List[str] = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS: List[float] = [0.6, 0.3, 0.1]

BROWSERS: List[str] = ["Chrome", "Safari", "Firefox", "Edge"]
BROWSER_WEIGHTS: List[float] = [0.6, 0.2, 0.15, 0.05]


def generate_user_activity(
    *,
    user_id: Optional[str] = None,
    user_id_pool: Optional[Sequence[str]] = None,
    event_type: Optional[str] = None,
    timestamp: Optional[datetime] = None,
) -> Dict[str, object]:
    """Create a single user activity record matching the schema.

    Schema:
        {
          "event_id": "uuid",
          "user_id": "uuid",
          "event_type": "click|view|add_to_cart|purchase",
          "device": "mobile|desktop|tablet",
          "browser": "Chrome|Safari|Firefox|Edge",
          "timestamp": "ISO8601"
        }
    """
    if user_id is None:
        if user_id_pool:
            user_id = random.choice(list(user_id_pool))
        else:
            user_id = str(uuid4())

    if event_type is None:
        event_type = random.choices(EVENT_TYPES, weights=EVENT_TYPE_WEIGHTS, k=1)[0]

    event_time = timestamp or datetime.now(timezone.utc)

    record: Dict[str, object] = {
        "event_id": str(uuid4()),
        "user_id": user_id,
        "event_type": event_type,
        "device": random.choices(DEVICES, weights=DEVICE_WEIGHTS, k=1)[0],
        "browser": random.choices(BROWSERS, weights=BROWSER_WEIGHTS, k=1)[0],
        "timestamp": event_time.isoformat(timespec="seconds"),
    }
    return record


def iter_user_activity(
    *, count: Optional[int] = None, user_id_pool: Optional[Sequence[str]] = None
) -> Iterator[Dict[str, object]]:
    """Yield user activity records.

    If count is None, generate indefinitely.
    """
    emitted = 0
    while count is None or emitted < count:
        yield generate_user_activity(user_id_pool=user_id_pool)
        emitted += 1
