from datetime import datetime
import orjson
import logging
from typing import Any, Optional
from ASL.utils.asl_redis import ASLRedis
from bbg_request import BloombergRequest

logger = logging.getLogger(__name__)
## may need to rename this.

REQUEST_QUEUE = "BBG_API:req_q"
RESPONSE_QUEUE = "BBG_API:resp_q"
PROCESSING_SET = "BBG_API:processing"
POLLING_QUEUE = "BBG_API:poll_q"
PROCESSED_RESPONSES = "BBG_API:processed_responses"
ERROR_QUEUE = "BBG_API:err_q"



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

        try:
        # Use priority as score (lower number = higher priority)
            self.redis_client.zadd(
                REQUEST_QUEUE, {orjson.dumps(request_data): request.priority}
            )
        except Exception as e:
            logger.error(f"Error queuing request to {REQUEST_QUEUE}: {e}")
            raise

    def get_sender_request(self) -> Optional[dict[any, any]]:
        try:
            return self.redis_client.zrange(
                        REQUEST_QUEUE, 0, 0, withscores=True)
        except Exception as e:
            logger.error(f"Error getting queued request from {REQUEST_QUEUE}: {e}")
            raise
    
    def remove_sender_request(self, json_request) -> None:
        try:
            self.redis_client.zrem(REQUEST_QUEUE, json_request)
        except Exception as e:
            logger.error(f"Error removing sender request from {REQUEST_QUEUE}: {e}")
            raise

    def set_processing_send(self, request):
        try:
            self.redis_client.sadd(
                PROCESSING_SET, request)
        except Exception as e:
            logger.error(f"Error setting processing send proc set {PROCESSING_SET}: {e}")
            raise

    def remove_processing_send(self, request):
        try:
            self.redis_client.srem(
                PROCESSING_SET, request)
        except Exception as e:
            logger.error(f"Error setting processing send proc set {PROCESSING_SET}: {e}")
            raise
    
    def get_polling_request(self) -> Any:
        try:
            
            return self.redis_client.rpop(POLLING_QUEUE)
        except Exception as e:
            logger.error(f"Error setting get polling request {POLLING_QUEUE}: {e}")
            raise

    def put_polling_request(self, _data : Any) -> Any:
        try:
            return self.redis_client.rpop(POLLING_QUEUE, _data)
        except Exception as e:
            logger.error(f"Error setting  set {POLLING_QUEUE}: {e}")
            raise
        
    
