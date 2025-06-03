import re
import time
from logging.handlers import RotatingFileHandler
import logging
from logging.handlers import RotatingFileHandler

from datetime import datetime
from typing import Dict, Any, Optional, Callable

from bbg_rest_connection import BloombergRestConnection
from bbg_database import BloombergDatabase
from bbg_redis import BloombergRedis
from bbg_request import BloombergRequest
from response_handler import ResponseHandler

# Attach logging
logger = logging.getLogger(__name__)

def _content_type_match(content_type, match_str):
    raise NotImplementedError


class BloombergResponsePoller:
    def __init__(
        self,
        redis_host: str = "cacheuat",
        db_server: str = "asldb03",
        db_port: str = "1433",
        _database: str = "playdb",
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
        self.redis_connection = BloombergRedis(redis_host=redis_host)

        # Processing state
        self.is_running = False
        self.poll_interval = (
            15  # seconds - Bloomberg recommends not polling too frequently
        )
        self.max_poll_attempts = (
            240  # Maximum polling attempts (1 hour at 15-second intervals)
        )

        self._register_default_handlers()

    def _get_response_content_type(self, response_payload : Dict[str, Any]) -> str:
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
        condition: Callable[[Dict[str, Any]], bool],
        handler: Callable[[Dict[str, Any]], None],
    ):
        self.bbg_connection.register_handler(name=name, condition=condition, handler=handler)


    def start_polling(self):
        """Start the response polling loop"""
        self.is_running = True
        logger.info("Starting Bloomberg response polling...")

        try:
            while self.is_running:
                self._poll_bbg_existing_requests()
                time.sleep(self.poll_interval)

        except KeyboardInterrupt:
            logger.info("Polling interrupted by user")
        except Exception as e:
            logger.error(f"Polling error: {e}")
        finally:
            self.stop_polling()

    def stop_polling(self):
        """Stop the response polling loop"""
        self.is_running = False
        logger.info("Response polling stopped")


    def _poll_bbg_existing_requests(self):
        """Poll existing requests for responses"""
        try:
            # Get all active polling requests
            active_requests = self.db_connection.get_sumbitted_requests()

            for request in active_requests:
                self.bbg_connection.poll_single_request(request)

        except Exception as e:
            logger.error(f"Error polling existing requests: {e}")
         
    
    def _handle_csv_response(self, response_payload: Dict[str, Any]):
        """Handle CSV data responses"""
        try:
            key : str = response_payload["key"]
            request_id : str = response_payload["request_id"]
            csv_data = self.bbg_connection.download_response_content(key)

            # Store CSV data in database
            self.db_connection.store_csv_data(request_id=request_id, csv_data=csv_data)

            logger.info(f"CSV response processed for request {request_id}")

        except Exception as e:
            logger.error(f"Error handling CSV response: {e}")

    def _handle_json_response(self, response: Dict[str, Any]):
        """Handle JSON data responses"""
        try:
            json_data = response.get("response_data", {})
            request_id = response.get("request_id")

            # Store JSON data in database
            self.db_connection.store_json_data(request_id, json_data)

            logger.info(f"JSON response processed for request {request_id}")

        except Exception as e:
            logger.error(f"Error handling JSON response: {e}")

    def _handle_error_response(self, response: Dict[str, Any]):
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

    def _handle_default_response(self, response: Dict[str, Any]):
        """Handle all other responses"""
        try:
            request_id = response.get("request_id")
            response_data = response.get("response_data", {})

            # Store raw response data
            self.db_connection.store_raw_response(request_id, response_data)

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
            self.redis_client.lpush(self.ERROR_QUEUE, json.dumps(error_payload))

            logger.error(f"Polling error for request {request_id}: {error_message}")

        except Exception as e:
            logger.error(f"Error handling polling error: {e}")


def setup_logging():
 ## setup logging --
        FORMAT = "%(asctime)s:%(levelname)s:%(filename)s:%(lineno)d=> %(message)s"
        handler = RotatingFileHandler(
            "logs/bbg_request_receiver.log", maxBytes=5000000, backupCount=3
        )
        logging.basicConfig(
            handlers=[handler],
            format=FORMAT,
            datefmt="%Y-%m-%d %H:%M:%S",
            level=logging.DEBUG,
        )
        handler.doRollover()  # start a new log each time.

def main():
    try:
        setup_logging()
        # Initialize poller
        poller = BloombergResponsePoller()

        # Register custom handler if needed
        def custom_handler(response):
            logger.info(f"Custom handler received: {response.get('request_id')}")

       # poller.register_handler(
       #     "custom_handler", lambda r: r.get("data_type") == "csv", custom_handler
       # )

        # Start polling
        poller.start_polling()

    except Exception as e:
        logger.error(f"Failed to start Bloomberg Response Poller: {e}")


# Example usage and main execution
if __name__ == "__main__":
    main()
