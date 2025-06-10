from datetime import datetime
import json
import os
import logging
from typing import Any, Callable, Optional
from urllib.parse import urljoin
import uuid

import ASL.utils.secrets
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import BackendApplicationClient
from bbg_database import BloombergDatabase
from bbg_request import BloombergRequest
from response_handler import ResponseHandler

logger = logging.getLogger(__name__)

DEFAULT_BBG_HOST = "https://api.bloomberg.com"
DEFAULT_OAUTH2_ENDPOINT = "https://bsso.blpprofessional.com/ext/api/as/token.oauth2"

def get_obj_dict(obj):
    return obj.__dict__

class BloombergRestConnection:
    def _initialize_oauth_session(self):
        """Initialize OAuth2 session for Bloomberg API"""
        try:
            client = BackendApplicationClient(client_id=self.client_id)

            self.OAUTH2_ENDPOINT = DEFAULT_OAUTH2_ENDPOINT

            self.session = OAuth2Session(
                client=client,
                auto_refresh_url=self.OAUTH2_ENDPOINT,
                auto_refresh_kwargs={"client_id": self.client_id},
                token_updater=self._token_updater,
            )

            # Fetch initial token
            token = self.session.fetch_token(
                token_url=self.OAUTH2_ENDPOINT, client_secret=self.client_secret
            )

            logger.info("OAuth2 session initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize OAuth2 session: {e}")
            raise

    def _refresh_oauth_token(self):
        """Refresh OAuth2 token"""
        try:
            token = self.session.refresh_token(
                self.OAUTH2_ENDPOINT,
                client_id=self.client_id,
                client_secret=self.client_secret,
            )
            logger.info("OAuth2 token refreshed successfully")

        except Exception as e:
            logger.error(f"Failed to refresh OAuth2 token: {e}")
            # Re-initialize session if refresh fails
            self._initialize_oauth_session()

    def __init__(
        self,
        _db_connection: BloombergDatabase,
        catalog=None,
        client_id=None,
        client_secret=None,
    ):
        """
        Initialize the Bloomberg Rest connection for either direction...

        Args:
            _db_connection : save messages to db conection.
            catalog: Bloomberg Data License Account Number
            client_id: Bloomberg OAuth2 client ID
            client_secret: Bloomberg OAuth2 client secret
        """
        self.catalog = catalog or os.environ.get("BLOOMBERG_DL_ACCOUNT_NUMBER", "")
        self.client_id = client_id or ASL.utils.secrets.my_secrets["BBG_REST_API_KEY"]
        self.client_secret = (
            client_secret or ASL.utils.secrets.my_secrets["BBG_REST_API_PWD"]
        )

        if not all([self.catalog, self.client_id, self.client_secret]):
            raise ValueError(
                "Bloomberg credentials not provided. Set environment variables or pass parameters."
            )

        self._initialize_oauth_session()
        # for saving calls made...
        self.db_connection = _db_connection
        self.bbg_host = DEFAULT_BBG_HOST
        self.request_response_base = f"/eap/catalogs/{self.catalog}"

        self.response_handlers: list[ResponseHandler] = []

    def _token_updater(self, token):
        """Handle token updates"""
        logger.info("OAuth2 token updated")
        # Could store updated token in database if needed
        return token

    def set_db_connection(self, _db_connection: BloombergDatabase):
        self.db_connection = _db_connection

    def submit_to_bloomberg(self, request_data: dict[str, Any]) -> Any:
        """Submit request to Bloomberg Data License API"""
        request_uri = urljoin(self.bbg_host, self.request_response_base) + "/requests/"
        logger.info(f"Submitting to Bloomberg: {request_uri}")

        if (logger.getEffectiveLevel() == logging.DEBUG):
            try:
               #  json_str = json.dumps(payload, indent=2)
               logger.debug(f"Sending payload to bbg request_payload type {request_data['request_payload']}")
            except Exception as e:
                logger.debug(f"Error logging bbg send payload {e}")
                raise

        try:
            response = self.session.post(
                url=request_uri, json=request_data['request_payload'], headers={'api-version': '2'})
        except Exception as e:
            logger.error(f"Sending to bbg failed on session post {e}")
            raise

        return response

    ##
    ## Response side goes here
    ##
    def register_handler(
        self,
        name: str,
        condition: Callable[[dict[str, Any]], bool],
        handler: Callable[[dict[str, Any]], None],
    ):
        """Register a response handler"""
        self.response_handlers.append(ResponseHandler(name, condition, handler))
        logger.info(f"Registered response handler: {name}")

    def _execute_response_handlers(self, response_payload: dict[str, Any]):
        """
        Executes the first registered response handler whose condition matches the given response payload.

        Iterates through the list of response handlers, evaluating each handler's condition with the provided response payload.
        If a handler's condition returns True, its handler function is executed with the response payload, a debug message is logged,
        and no further handlers are executed. If an exception occurs during handler execution, it is logged as an error.

        Args:
            response_payload (dict[str, Any]): The response data to be processed by the handlers.

        Raises:
            Logs any exceptions raised by handler execution but does not propagate them.
        """

        for handler in self.response_handlers:
            try:
                if handler.condition(response_payload):
                    handler.handler(response_payload)
                    logger.debug(f"Executed handler: {handler.name}")
                    break  # Only execute the first matching handler
            except Exception as e:
                logger.error(f"Error in response handler {handler.name}: {e}")

    def download_response_content(self, key: str) -> Optional[str]:
        """Download content from Bloomberg response URL"""
        try:
            data_uri = urljoin(
                self.bbg_host, f"/eap/catalogs/{self.catalog}/content/responses/{key}"
            )
            response = self.session.get(data_uri)
            if response.status_code == 200:
                return response.text
            else:
                logger.error(
                    f"Failed to download content from {data_uri}: HTTP {response.status_code}"
                )
                return None
        except Exception as e:
            logger.error(f"Error downloading content from {data_uri}: {e}")
            return None

    def _process_bloomberg_response(
        self, request_id: str, identifier: str, responses: list[Any]
    ):
        """Process a Bloomberg API response"""
        try:
            for response_item in responses:
                logger.debug("<- Response ->")
                logger.debug(response_item)
                key = response_item['key']
                logger.info(f"key = {key}")
                snapshot_timestamp = response_item["metadata"]["DL_SNAPSHOT_START_TIME"]

                # Extract response content
                response_payload = {
                    "request_id": request_id,
                    "identifier": identifier,
                    "response_id": response_item.get("responseId", str(uuid.uuid4())),
                    "status_code": 200,
                    "timestamp": datetime.now().isoformat(),
                    "raw_response": response_item,
                    "key": key,
                }

                # Process with registered handlers
                self._execute_response_handlers(response_payload)

                logger.info(f"Processed response for request {request_id}")

        except Exception as e:
            logger.error(
                f"Error processing Bloomberg response for request {request_id}: {e}"
            )
            error_payload = {
                "request_id": request_id,
                "identifier": identifier,
                "error_message": str(e),
                "status_code": 500,
                "timestamp": datetime.now().isoformat(),
            }


    def poll_single_request(self, request: BloombergRequest):
        """Poll a single request for responses"""
        request_id = request["request_id"]
        identifier = request["identifier"]
        # () poll_count = request["poll_count"]
        poll_count = 0

        try:
            # Build Bloomberg content responses URI
            content_responses_uri = urljoin(
                self.bbg_host,
                f"/eap/catalogs/{self.catalog}/content/responses/?requestIdentifier={identifier}",
            )

            logger.info(f"Polling Bloomberg: {content_responses_uri}")

            # Make the polling request
            response = self.session.get(
                content_responses_uri, headers={"api-version": "2"}
            )

            if response.status_code == 200:
                response_data = response.json()
                if (logger.getEffectiveLevel() == logging.DEBUG): logging.debug(response_data) 
                responses = response_data["contains"]
                if (logger.getEffectiveLevel() == logging.DEBUG):  logging.debug(responses)

                if len(responses) > 0:
                    logger.info(f"Response received for request {request_id}")
                    self._process_bloomberg_response(request_id, identifier, responses)
                    self.db_connection.update_request_status(request_id, "completed")
                else:
                    logger.debug(
                        f"No response yet for request {request_id}, poll count: {poll_count + 1}"
                    )

            elif response.status_code == 404:
                # Request not found - might be expired or invalid
                logger.warning(
                    f"Request {request_id} not found (404) - marking as error"
                )
                self._handle_polling_error(
                    request_id, "Request not found", response.status_code
                )

            elif response.status_code == 401:
                # Authentication error - try to refresh token
                logger.warning("Authentication error - attempting token refresh")
                self._refresh_oauth_token()

            else:
                # Other HTTP error
                logger.error(
                    f"HTTP error {response.status_code} for request {request_id}"
                )

        except Exception as e:
            logger.error(f"Error polling request {request_id}: {e}")
