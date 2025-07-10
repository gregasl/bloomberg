from dataclasses import dataclass
from typing import Any
import json

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
    request_type : str
    request_cmd : str
    request_id: str  # on REQUEST TYPE CMD the command is there on BBG_REQUEST "BBG request_id?"
    identifier: str
    request_name : str
    request_payload: dict[str, Any] = None
    priority: int = 4 
    retry_count: int = 0
    max_retries: int = 3

    def print_the_dict(self, log_func):
        for key, value in self.__dict__.items():
            log_func(f'k={key} v={value}')
    
    def toJSON(self) -> str:
        return json.dumps(self.__dict__)

    @staticmethod
    def create_from_json(inJSON : str):
        tmp_dict = json.loads(inJSON)
        bbgRequest = BloombergRequest("RT", "RC", "RI", "ID", "RN")
        for key, value in tmp_dict.items():
            bbgRequest.__dict__[key] = value 

        return bbgRequest