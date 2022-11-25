import hashlib
import json
import random
from typing import Any, Dict, Iterable, List


def random_strategy(queues: List[str], task_payload: Dict[str, Any]) -> Iterable[str]:
    return [random.choice(queues)]


def broadcast_strategy(
    queues: List[str], task_payload: Dict[str, Any]
) -> Iterable[str]:
    return queues


def hash_strategy(queues: List[str], task_payload: Dict[str, Any]) -> Iterable[str]:
    buckets = len(queues)
    md5hash = hashlib.md5(json.dumps(task_payload).encode("utf8")).hexdigest()
    return [queues[int(md5hash, 16) % buckets]]


RANDOM = "random"
BROADCAST = "broadcast"
HASH = "hash"
ROUTING_STRATEGIES = {
    RANDOM: random_strategy,
    BROADCAST: broadcast_strategy,
    HASH: hash_strategy,
}
