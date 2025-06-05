from dataclasses import dataclass
from typing import Any, Callable, Dict


@dataclass
class ResponseHandler:
    name: str
    condition: Callable[[Dict[str, Any]], bool]
    handler: Callable[[Dict[str, Any]], None]