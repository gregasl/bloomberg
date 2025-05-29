from dataclasses import dataclass
from typing import Any, Dict

@dataclass
class BloombergRequest:
    request_id: str
    identifier: str
    request_payload: Dict[str, Any]
    priority: int = 1
    retry_count: int = 0
    max_retries: int = 3