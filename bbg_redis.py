from datetime import datetime
import orjson
import logging
import uuid
from typing import Optional
from ASL.utils.asl_redis import ASLRedis
from bbg_request import BloombergRequest, HIGH_CMD_PRIORITY, LAST_CMD_PRIORITY, DEFAULT_CMD_PRIORITY, REQUEST_TYPE_CMD

logger = logging.getLogger(__name__)
## may need to rename this.


class BloombergRedis:
    REQUEST_QUEUE = "BBG_API:req_q"
    RESPONSE_QUEUE = "BBG_API:resp_q"
    PROCESSING_SET = "BBG_API:processing"
    POLLING_QUEUE = "BBG_API:poll_q"
    PROCESSED_RESPONSES = "BBG_API:processed_responses"
    ERROR_QUEUE = "BBG_API:err_q"

    def __init__(
        self,
        redis_host : str ="cacheuat",
        use_async : bool = False,
        user : str  = "readonly",
        queue : str = REQUEST_QUEUE
    ):
        """
        
        """
        self.enqueue_counter = 0
        self.sod_date_time = datetime.now().replace(hour=0, second=0, microsecond=0)
        self.queue = queue
    
        self.redis_client = ASLRedis(
            host=redis_host,
            use_async=use_async,
            user=user
        )

    ## fill this out once I get it going
    def get_client(self) -> ASLRedis:
        return self.redis_client

    def close(self):
        self.redis_client.close()
    
    def queue_request(self, request: BloombergRequest) -> None:
        """Add request to Redis priority queue"""
        now = datetime.now()
        delta_time = now - self.sod_date_time
        self.enqueue_counter += 1 # in case we queue 2 quicky...
        request_data = {
            "request_id": request.request_id,
            "request_cmd": request.request_cmd,
            "request_type": request.request_type,
            "identifier": request.identifier,
            "request_name" : request.request_name,
            "request_payload": request.request_payload,
            "priority": request.priority,
            "retry_count": request.retry_count,
            "max_retries": request.max_retries,
            "timestamp": now.isoformat(),
        }

        try:
            adjust_time = delta_time.seconds*100000 +  delta_time.microseconds + self.enqueue_counter
            adjust_time = ((adjust_time / 10000000000.0) % 1.0) 
            add_priority = request.priority + adjust_time # note sec inday is 86400
        # Use priority as score (lower number = higher priority)
            self.redis_client.zadd(
                # create a priority to mimic insertion order.  
                self.queue, {orjson.dumps(request_data): add_priority} 
            )
        except Exception as e:
            logger.error(f"Error queuing request to {BloombergRedis.REQUEST_QUEUE}: {e}")
            raise

    def submit_command(self, cmd: str, request_id : str = None, priority=DEFAULT_CMD_PRIORITY, payload : str = "") -> str:
        if request_id is None:
            request_id = str(uuid.uuid4())

        bloomberg_request = BloombergRequest(
            request_cmd=cmd,
            request_id=request_id,
            identifier="",
            request_name="",
            request_payload=payload,
            request_type=REQUEST_TYPE_CMD,
            priority=priority,
            max_retries=1,
        )
        self.queue_request(bloomberg_request)
        return bloomberg_request.request_id

    def get_request(self, max_items : int = 3) -> Optional[dict[any, any]]:
        try:
            return self.redis_client.zrange(
                        self.queue, 0, max_items - 1, withscores=True)
        except Exception as e:
            logger.error(f"Error getting queued request from {BloombergRedis.REQUEST_QUEUE}: {e}")
            raise
    
    def remove_request(self, json_request) -> None:
        try:
            self.redis_client.zrem(self.queue, json_request)
        except Exception as e:
            logger.error(f"Error removing sender request from {BloombergRedis.REQUEST_QUEUE}: {e}")
            raise

    def clear_queue(self) -> None:
        try:
            self.redis_client.delete(self.queue)
        except Exception as e:
            logger.error(f"Error clearing out the queue {BloombergRedis.REQUEST_QUEUE}: {e}")
            raise

    def set_queue_name(self, queue_name : str):
        self.queue = queue_name
    
