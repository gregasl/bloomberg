from datetime import datetime
import orjson
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
PROCESSED_RESPONSES = "BLOOMBERG_API:processed_responses"
ERROR_QUEUE = "BLOOMBERG_API:error_queue"

class BloombergRedis:
    def __init__(
        self,
        redis_host : str ="cacheuat",
        use_async : bool = False,
        user : str  = "readonly"
    ):
        """
        
        """

        self.redis_client = ASLRedis(
            host=redis_host,
            use_async=use_async,
            user=user
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
            REQUEST_QUEUE, {orjson.dumps(request_data): request.priority}
        )

    def get_sender_request(self) -> Optional[Dict[any, any]]:
         return self.redis_client.zrange(
                    REQUEST_QUEUE, 0, 0, withscores=True)
    
    def remove_sender_request(self, json_request) -> None:
        self.redis_client.zrem(REQUEST_QUEUE, json_request)

    def set_processing_send(self, request):
        self.redis_client.sadd(
            PROCESSING_SET, request)
        
    def remove_processing_send(self, request):
        self.redis_client.srem(
            PROCESSING_SET, request)
        
    def get_polling_request(self) -> Any:
        return self.redis_client.rpop(POLLING_QUEUE)

    def put_polling_request(self, _data : Any) -> Any:
        return self.redis_client.rpop(POLLING_QUEUE, _data)
        
    
