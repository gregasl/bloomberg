import redis
import pyodbc
import json
import time
import logging
import os
import secrets
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Callable
from urllib.parse import urljoin
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
from dataclasses import dataclass
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class ResponseHandler:
    name: str
    condition: Callable[[Dict[str, Any]], bool]
    handler: Callable[[Dict[str, Any]], None]

class BloombergResponsePoller:
    def __init__(self, redis_host='cacheuat', redis_port=6379, redis_db=0,
                 sql_server_conn_str=None, catalog=None, client_id=None, client_secret=None):
        """
        Initialize the Bloomberg Response Poller
        
        Args:
            redis_host: Redis server host
            redis_port: Redis server port
            redis_db: Redis database number
            sql_server_conn_str: SQL Server connection string
            catalog: Bloomberg Data License Account Number
            client_id: Bloomberg OAuth2 client ID
            client_secret: Bloomberg OAuth2 client secret
        """
        # Redis connection
        self.redis_client = redis.Redis(
            host=redis_host, 
            port=redis_port, 
            db=redis_db,
            decode_responses=True
        )
        
        # SQL Server connection
        self.sql_conn_str = sql_server_conn_str or (
            "DRIVER={SQL Server};"
            "SERVER=SQLPROD;"
            "DATABASE=playdb;"
            "Trusted_Connection=yes;"
        )
        
        # Bloomberg API configuration
        self.catalog = catalog or os.environ.get("BLOOMBERG_DL_ACCOUNT_NUMBER", "")
        self.client_id = secrets.my_secrets['BBG_REST_API_KEY']
        self.client_secret = client_secret or secrets.my_secrets['BBG_REST_API_PWD']
        
        if not all([self.catalog, self.client_id, self.client_secret]):
            raise ValueError("Bloomberg credentials not provided. Set environment variables or pass parameters.")
        
        
        # Initialize OAuth2 session
        self._initialize_oauth_session()
        
        # Redis keys
        self.POLLING_QUEUE = "bloomberg:polling_queue"
        self.RESPONSE_QUEUE = "bloomberg:response_queue"
        self.PROCESSED_RESPONSES = "bloomberg:processed_responses"
        self.ERROR_QUEUE = "bloomberg:error_queue"
        
        # Response handlers
        self.response_handlers: List[ResponseHandler] = []
        
        # Processing state
        self.is_running = False
        self.poll_interval = 15  # seconds - Bloomberg recommends not polling too frequently
        self.max_poll_attempts = 240  # Maximum polling attempts (1 hour at 15-second intervals)
        
        self._register_default_handlers()
    
   
   
    def _initialize_oauth_session(self):
        """Initialize OAuth2 session for Bloomberg API"""
        try:
            client = BackendApplicationClient(client_id=self.client_id)
            
            self.session = OAuth2Session(
                client=client, 
                auto_refresh_url=self.OAUTH2_ENDPOINT,
                auto_refresh_kwargs={'client_id': self.client_id},
                token_updater=self._token_updater
            )
            
            # Fetch initial token
            token = self.session.fetch_token(
                token_url=self.OAUTH2_ENDPOINT, 
                client_secret=self.client_secret
            )
            
            logger.info("OAuth2 session initialized successfully for poller")
            
        except Exception as e:
            logger.error(f"Failed to initialize OAuth2 session: {e}")
            raise
    


    def _token_updater(self, token):
        """Handle token updates"""
        logger.info("OAuth2 token updated in poller")
        return token
    


    def _register_default_handlers(self):
        """Register default response handlers"""
        
        # Handler for CSV data responses
        self.register_handler(
            "csv_data_handler",
            lambda response: response.get('data_type') == 'csv' and response.get('response_data'),
            self._handle_csv_response
        )
        
        # Handler for JSON data responses
        self.register_handler(
            "json_data_handler",
            lambda response: response.get('data_type') == 'json' and response.get('response_data'),
            self._handle_json_response
        )
        
        # Handler for error responses
        self.register_handler(
            "error_handler",
            lambda response: response.get('error_message') or response.get('status_code', 0) >= 400,
            self._handle_error_response
        )
        
        # Default handler for all other responses
        self.register_handler(
            "default_handler",
            lambda response: True,  # Catch all
            self._handle_default_response
        )
    


    def register_handler(self, name: str, condition: Callable[[Dict[str, Any]], bool], 
                        handler: Callable[[Dict[str, Any]], None]):
        """Register a response handler"""
        self.response_handlers.append(ResponseHandler(name, condition, handler))
        logger.info(f"Registered response handler: {name}")
    


    def start_polling(self):
        """Start the response polling loop"""
        self.is_running = True
        logger.info("Starting Bloomberg response polling...")
        
        try:
            while self.is_running:
                self._poll_for_new_requests()
                self._poll_existing_requests()
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
    


    def _poll_for_new_requests(self):
        """Check for new requests to start polling"""
        try:
            # Get new requests from polling queue (non-blocking)
            while True:
                request_data = self.redis_client.rpop(self.POLLING_QUEUE)
                if not request_data:
                    break
                
                request_info = json.loads(request_data)
                request_id = request_info['request_id']
                identifier = request_info['identifier']
                
                # Add to polling status table
                self._add_to_polling_status(request_id, identifier)
                
                logger.info(f"Started polling for request {request_id} with identifier {identifier}")
                
        except Exception as e:
            logger.error(f"Error checking for new requests: {e}")
    


    def _poll_existing_requests(self):
        """Poll existing requests for responses"""
        try:
            # Get all active polling requests
            active_requests = self._get_active_polling_requests()
            
            for request in active_requests:
                self._poll_single_request(request)
                
        except Exception as e:
            logger.error(f"Error polling existing requests: {e}")
    


    def _get_active_polling_requests(self) -> List[Dict[str, Any]]:
        """Get all requests currently being polled"""
        try:
            with pyodbc.connect(self.sql_conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT request_id, identifier, poll_count, max_polls
                    FROM polling_status 
                    WHERE status = 'polling' AND poll_count < max_polls
                """)
                
                return [
                    {
                        'request_id': row.request_id,
                        'identifier': row.identifier,
                        'poll_count': row.poll_count,
                        'max_polls': row.max_polls
                    }
                    for row in cursor.fetchall()
                ]
                
        except Exception as e:
            logger.error(f"Error getting active polling requests: {e}")
            return []
    


    def _poll_single_request(self, request: Dict[str, Any]):
        """Poll a single request for responses"""
        request_id = request['request_id']
        identifier = request['identifier']
        poll_count = request['poll_count']
        
        try:
            # Build Bloomberg content responses URI
            content_responses_uri = urljoin(
                self.HOST, 
                f'/eap/catalogs/{self.catalog}/content/responses/?requestIdentifier={identifier}'
            )
            
            logger.debug(f"Polling Bloomberg: {content_responses_uri}")
            
            # Make the polling request
            response = self.session.get(content_responses_uri, headers={'api-version': '2'