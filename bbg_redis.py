from datetime import datetime
import json
import os
import logging
from typing import Any, Dict, List, Optional
import uuid
from asl_redis import ASLRedis

from bbg_request import BloombergRequest


logger = logging.getLogger(__name__)
## may need to rename this.

REQUEST_QUEUE = "BLOOMBERG_API:request_queue"
RESPONSE_QUEUE = "BLOOMBERG_API:response_queue"
PROCESSING_SET = "BLOOMBERG_API:processing"
POLLING_QUEUE = "BLOOMBERG_API:polling_queue"

class BloombergRedis:
    def __init__(
        self,
        redis_host : str ="cacheuat",
        redis_port : int =6379,
        redis_db : int =0,
    ):
        """
        Initialize the Bloomberg database connection and utilitie ...

        Args:
            server: Machine name of the DB server will connect to.  defualt MSSQL_SERVER
            port: Network port to connect on defautlt to 1433 as a string! default MSSQL_TCP_PORT
            database: name of db to connect to - default MSSQL_DATABASE
            username:  If None defaults to Microsoft credentials
        """

        self.redis_client = ASLRedis(
            host=redis_host
        )

    ## fill this out once I get it going
    def get_client(self) -> ASLRedis:
        return self.redis_client
    
    def queue_request_to_sender(self, request: BloombergRequest) -> None:
        """Add request to Redis priority queue"""
        request_data = {
            "request_id": request.request_id,
            "identifier": request.identifier,
            "request_payload": request.request_payload,
            "priority": request.priority,
            "retry_count": request.retry_count,
            "max_retries": request.max_retries,
            "timestamp": datetime.now().isoformat(),
        }

        # Use priority as score (lower number = higher priority)
        self.redis_client.zadd(
            REQUEST_QUEUE, {json.dumps(request_data): request.priority}
        )

    def get_sender_request(self) -> Optional[Dict[any, any]]:
         return self.redis_client.zrange(
                    REQUEST_QUEUE, 0, 0, withscores=True)
    
    def remove_sender_request(self, json_request) -> None:
        self.redis_client.zrem(REQUEST_QUEUE, json_request)
