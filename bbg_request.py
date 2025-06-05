from dataclasses import dataclass
from typing import Any

# Priority of items to tell the app to do like exit...
HIGH_CMD_PRIORITY=2
HIGH_REQUEST_PRIORITY=3
DEFAULT_REQUEST_PRIORITY=4
DEFAULT_CMD_PRIORITY=4
LOWEST_REQUEST_PRIORITY=9
LAST_CMD_PRIORITY=10

REQUEST_TYPE_CMD="CMD"
REQUEST_TYPE_BBG_REQUEST="BBG_REQUEST"

@dataclass
class BloombergRequest:
    ## reuest id is a uuid for bbg the command for cmd type.
    request_id: str
    identifier: str
    request_type : str
    request_payload: dict[str, Any]
    priority: int = 4 
    retry_count: int = 0
    max_retries: int = 3