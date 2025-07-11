import json
import re
import time
import os
import orjson
import logging
import sys

from ASL.utils.asl_logging import ASL_Logging
from datetime import datetime
from typing import Any, Callable

from bbg_request import BloombergRequest
from bbg_rest_connection import BloombergRestConnection
from bbg_database import BloombergDatabase
from bbg_redis import BloombergRedis
from bbg_send_cmds import EXIT_CMD
from run_state import RunState

# Attach logging
logger = logging.getLogger(__name__)

def _content_type_match(content_type, match_str):
    raise NotImplementedError


class BloombergResponsePoller:
    def __init__(
        self,
        redis_host: str = None,
        db_server: str = None,
        db_port: str = None,
        _database: str = None,
        catalog=None,
        client_id=None,
        client_secret=None,
    ):
        """
        Initialize the Bloomberg Response Poller

        Args:
            redis_host: Redis server host
            db_server: SQL server hostname
            db_port: SQL server port
            _database : database on server used
            catalog: Bloomberg Data License Account Number
            client_id: Bloomberg OAuth2 client ID
            client_secret: Bloomberg OAuth2 client secret
        """

        db_server = db_server or os.environ.get("BBG_SQL_SERVER", "")
        db_port = db_port or os.environ.get("BBG_SQL_PORT", "")
        _database = _database or os.environ.get("BBG_DATABASE", "")
        redis_host = redis_host or os.environ.get("REDIS_HOST", "")
        # SQL Server connection
        self.db_connection = BloombergDatabase(
            server=db_server, port=db_port, database=_database
        )

        self.bbg_connection = BloombergRestConnection(
            self.db_connection,
            catalog=catalog,
            client_id=client_id,
            client_secret=client_secret,
        )

        # Redis connection
        self.redis_connection = BloombergRedis(redis_host=redis_host, queue=BloombergRedis.RESPONSE_QUEUE)

        # Processing state
        self.is_running = False
        ## these are in the defs below need to fix that
        self.poll_interval = (
            15  # seconds - Bloomberg recommends not polling too frequently
        )
        self.max_poll_attempts = (
            240  # Maximum polling attempts (1 hour at 15-second intervals)
        )
        self.request_definitions = self.db_connection.get_request_definitions()
        self._register_default_handlers()

    def _get_response_content_type(self, response_payload : dict[str, Any]) -> str:
        """
        Extracts the 'Content-Type' header from the given response payload.

        Args:
            response_payload (Dict[str, Any]): A dictionary containing the response data, 
                expected to have a 'raw_response' key with a 'headers' dictionary.

        Returns:
            str: The value of the 'Content-Type' header if present, otherwise "text/plain".

        Logs:
            An error message if the 'Content-Type' cannot be retrieved.
        """
        response = response_payload.get("raw_response")
        try:
            headers_dict = response['headers']
            content_type = headers_dict['Content-Type']
            return content_type
        except Exception as ex:
            logger.error(f"Error getting content_type: {ex}")
            return ("text/plain")

    def _content_type_match(self, content_type : str, match_str : str) -> bool:
        logger.debug(f'content {content_type} - ')
        val = re.search(match_str, content_type, re.IGNORECASE) != None
        return val

    def _register_default_handlers(self):
        """Register default response handlers"""

        # Handler for CSV data responses
        self.bbg_connection.register_handler(
            "csv_data_handler",
            lambda response: len(response) > 0 and self._content_type_match(self._get_response_content_type(response), "csv"),
            self._handle_csv_response,
        )

        # Handler for JSON data responses
        self.bbg_connection.register_handler(
            "json_data_handler",
             lambda response: len(response) > 0 and self._content_type_match(self._get_response_content_type(response), "json"),
            self._handle_json_response,
        )

        # Handler for error responses
        self.bbg_connection.register_handler(
            "error_handler",
            lambda response: response.get("error_message")
            or response.get("status_code", 0) >= 400,
            self._handle_error_response,
        )

        # Default handler for all other responses
        self.bbg_connection.register_handler(
            "default_handler",
            lambda response: True,  # Catch all
            self._handle_default_response,
        )

    # convience function
    def register_handler(
        self,
        name: str,
        condition: Callable[[dict[str, Any]], bool],
        handler: Callable[[dict[str, Any]], None],
    ):
        self.bbg_connection.register_handler(name=name, condition=condition, handler=handler)


    def process_redis_requests(self, max_items : int = 1):
        requests_data : list[BloombergRequest] = self.redis_connection.get_request(max_items)

        for request_data in requests_data:
            orig_request_json, priority = request_data
            request_dict: dict[str, Any] = json.loads(orig_request_json)
            cmd : str = request_dict['request_cmd'] if request_dict['request_cmd'] is not None else ""
            cmd = cmd.upper()

            if cmd == EXIT_CMD:
                RunningState = RunState.CMD_DIE
                self.is_running = False
                self.redis_connection.remove_request(orig_request_json)
                # break the for loop...
                break 

    # run til complete once all pending are resolved exit..
    def start_polling(self, run_til_complete : bool = False):
        """Start the response polling loop"""
        self.is_running = True
        logger.info("Starting Bloomberg response polling...")

        try:
            while self.is_running:
                cnt = self._poll_bbg_existing_requests()

                if ((cnt == 0)and(run_til_complete)):
                    self.is_running = False

                if (self.is_running):
                    time.sleep(self.poll_interval)

        except KeyboardInterrupt:
            logger.info("Polling interrupted by user")
        except Exception as e:
            logger.error(f"Polling error: {e}")
        finally:
            self.close()

    def stop_polling(self):
        """Stop the response polling loop"""
        self.is_running = False
        logger.info("Response polling stopped")

    def close(self):
        try:
            self.redis_connection.close()
            self.bbg_connection.close()
            self.db_connection.close()
        except Exception as e:
            logger.error("Unable to exit gracefully {e}")

    def _poll_bbg_existing_requests(self) -> int:
        """Poll existing requests for responses"""
        try:
            # Get all active polling requests
            active_requests = self.db_connection.get_sumbitted_requests()

            for request in active_requests:
                self.bbg_connection.poll_single_request(request)
                
            self.process_redis_requests(1)  # exit command and more later.
            return len(active_requests)
        except Exception as e:
            logger.error(f"Error polling existing requests: {e}")
         
    
    def _handle_csv_response(self, response_payload: dict[str, Any]):
        """Handle CSV data responses"""
        try:
            key : str = response_payload["key"]
            request_id : str = response_payload["request_id"]
            identifier : str = response_payload["identifier"]
            request_name : str = response_payload["request_name"]
            csv_data = self.bbg_connection.download_response_content(key)

            # Store CSV data in database
            self.db_connection.store_csv_data(request_id=request_id, identifier=identifier, request_name=request_name, csv_data=csv_data)

            logger.info(f"CSV response processed for request {request_id}")

        except Exception as e:
            logger.error(f"Error handling CSV response: {e}")

    def _handle_json_response(self, response: dict[str, Any]):
        """Handle JSON data responses"""
        try:
            json_data = response.get("response_data", {})
            identifier = response.get("identifier")
            request_id = response.get("request_id")

            # Store JSON data in database
            self.db_connection.store_json_data(request_id, identifier, json_data)

            logger.info(f"JSON response processed for request {request_id}")

        except Exception as e:
            logger.error(f"Error handling JSON response: {e}")

    def _handle_error_response(self, response: dict[str, Any]):
        """Handle error responses"""
        try:
            request_id = response.get("request_id")
            error_message = response.get("error_message", "Unknown error")
            status_code = response.get("status_code", 500)

            # Store error in database
            self.db_connection.store_poll_error_response(
                request_id, error_message, status_code
            )

            logger.error(
                f"Error response processed for request {request_id}: {error_message}"
            )

        except Exception as e:
            logger.error(f"Error handling error response: {e}")

    def _handle_default_response(self, response: dict[str, Any]):
        """Handle all other responses"""
        try:
            request_id = response["request_id"]
            identifier = response["identifier"]
            request_name = response["request_name"]
            response_data = response.get("response_data", {})
            

            # Store raw response data
            self.db_connection.store_raw_response(request_id=request_id, 
                                                  identifier=identifier, request_name=request_name,
                                                  response_data=response_data)

            logger.info(f"Default response processed for request {request_id}")

        except Exception as e:
            logger.error(f"Error handling default response: {e}")

    def _handle_polling_error(
        self, request_id: str, error_message: str, status_code: int
    ):
        """Handle polling errors"""
        try:
            # Update polling status
            self.db_connection.update_request_status(request_id, "error")

            # Create error response
            error_payload = {
                "request_id": request_id,
                "error_message": error_message,
                "status_code": status_code,
                "timestamp": datetime.now().isoformat(),
            }

            # Add to error queue
            self.redis_client.lpush(self.ERROR_QUEUE, orjson.dumps(error_payload))

            logger.error(f"Polling error for request {request_id}: {error_message}")

        except Exception as e:
            logger.error(f"Error handling polling error: {e}")

 
def setup_logging():
 ## setup logging --
    log_path = os.environ.get('LOG_DIR', "./output")
    asl_logger = ASL_Logging(log_file="bbg_response_poller.log", log_path=log_path, use_stream_output=True, useBusinessDateRollHandler=True)

def main():
    try:
        for arg in sys.argv:
            if arg == "run_to_complete":
                run_until_complete = True

        setup_logging()
        # Initialize poller
        poller = BloombergResponsePoller()

        # Start polling
        poller.start_polling()

    except Exception as e:
        logger.error(f"Failed to start Bloomberg Response Poller: {e}")


# Example usage and main execution
if __name__ == "__main__":
    main()
