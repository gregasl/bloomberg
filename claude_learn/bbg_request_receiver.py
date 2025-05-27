import redis
import pyodbc
import json
import time
import logging
import os
import ASL
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
        self.client_id = ASL.utils.secrets.my_secrets['BBG_REST_API_KEY']
        self.client_secret = client_secret or ASL.utils.secrets.my_secrets['BBG_REST_API_PWD']
        
        if not all([self.catalog, self.client_id, self.client_secret]):
            raise ValueError("Bloomberg credentials not provided. Set environment variables or pass parameters.")
        
        self.HOST = 'https://api.bloomberg.com'
        self.OAUTH2_ENDPOINT = 'https://bsso.blpprofessional.com/ext/api/as/token.oauth2'
        
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
            logger.info(f'Connecting with {self.sql_conn_str}')
            with pyodbc.connect(self.sql_conn_str) as conn:
                sql : str = """
                    SELECT request_id, identifier, poll_count, max_polls
                    FROM bloomberg_polling_status 
                    WHERE status = 'polling' AND poll_count < max_polls
                """
                logger.info(sql)
                cursor = conn.cursor()
                cursor.execute(sql)
                
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
            response = self.session.get(content_responses_uri, headers={'api-version': '2'})
            
            # Update poll count
            self._update_poll_count(request_id, poll_count + 1)
            
            if response.status_code == 200:
                response_data = response.json()
                
                # Check if we have responses available
                if response_data.get('responses') and len(response_data['responses']) > 0:
                    logger.info(f"Response received for request {request_id}")
                    self._process_bloomberg_response(request_id, identifier, response_data)
                    self._update_polling_status(request_id, 'completed')
                else:
                    logger.debug(f"No response yet for request {request_id}, poll count: {poll_count + 1}")
                    
            elif response.status_code == 404:
                # Request not found - might be expired or invalid
                logger.warning(f"Request {request_id} not found (404) - marking as error")
                self._handle_polling_error(request_id, "Request not found", response.status_code)
                
            elif response.status_code == 401:
                # Authentication error - try to refresh token
                logger.warning("Authentication error - attempting token refresh")
                self._refresh_oauth_token()
                
            else:
                # Other HTTP error
                logger.error(f"HTTP error {response.status_code} for request {request_id}")
                if poll_count + 1 >= request['max_polls']:
                    self._handle_polling_error(request_id, f"Max polls reached with HTTP {response.status_code}", response.status_code)
                    
        except Exception as e:
            logger.error(f"Error polling request {request_id}: {e}")
            if poll_count + 1 >= request['max_polls']:
                self._handle_polling_error(request_id, str(e), 500)


    def _process_bloomberg_response(self, request_id: str, identifier: str, response_data: Dict[str, Any]):
        """Process a Bloomberg API response"""
        try:
            for response_item in response_data.get('responses', []):
                # Extract response content
                response_payload = {
                    'request_id': request_id,
                    'identifier': identifier,
                    'response_id': response_item.get('responseId', str(uuid.uuid4())),
                    'status_code': 200,
                    'timestamp': datetime.now().isoformat(),
                    'raw_response': response_item
                }
                
                # Determine data type and extract content
                if 'content' in response_item:
                    content = response_item['content']
                    
                    # Handle different content types
                    if isinstance(content, dict):
                        response_payload['data_type'] = 'json'
                        response_payload['response_data'] = content
                    elif isinstance(content, str):
                        # Assume CSV if it's a string
                        response_payload['data_type'] = 'csv'
                        response_payload['response_data'] = content
                    else:
                        response_payload['data_type'] = 'unknown'
                        response_payload['response_data'] = str(content)
                        
                elif 'url' in response_item:
                    # Response contains a URL to download content
                    download_url = response_item['url']
                    content_data = self._download_response_content(download_url)
                    
                    if content_data:
                        response_payload['data_type'] = 'csv'  # Bloomberg typically returns CSV via URL
                        response_payload['response_data'] = content_data
                    else:
                        response_payload['error_message'] = "Failed to download response content"
                
                # Add to response queue for processing
                self.redis_client.lpush(self.RESPONSE_QUEUE, json.dumps(response_payload))
                
                # Process with registered handlers
                self._execute_response_handlers(response_payload)
                
                logger.info(f"Processed response for request {request_id}")
                
        except Exception as e:
            logger.error(f"Error processing Bloomberg response for request {request_id}: {e}")
            error_payload = {
                'request_id': request_id,
                'identifier': identifier,
                'error_message': str(e),
                'status_code': 500,
                'timestamp': datetime.now().isoformat()
            }
            self.redis_client.lpush(self.ERROR_QUEUE, json.dumps(error_payload))


    def _download_response_content(self, url: str) -> Optional[str]:
        """Download content from Bloomberg response URL"""
        try:
            response = self.session.get(url)
            if response.status_code == 200:
                return response.text
            else:
                logger.error(f"Failed to download content from {url}: HTTP {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"Error downloading content from {url}: {e}")
            return None


    def _execute_response_handlers(self, response_payload: Dict[str, Any]):
        """Execute registered response handlers"""
        for handler in self.response_handlers:
            try:
                if handler.condition(response_payload):
                    handler.handler(response_payload)
                    logger.debug(f"Executed handler: {handler.name}")
                    break  # Only execute the first matching handler
            except Exception as e:
                logger.error(f"Error in response handler {handler.name}: {e}")


    def _handle_csv_response(self, response: Dict[str, Any]):
        """Handle CSV data responses"""
        try:
            csv_data = response.get('response_data', '')
            request_id = response.get('request_id')
            
            # Store CSV data in database
            self._store_csv_data(request_id, csv_data)
            
            logger.info(f"CSV response processed for request {request_id}")
            
        except Exception as e:
            logger.error(f"Error handling CSV response: {e}")


    def _handle_json_response(self, response: Dict[str, Any]):
        """Handle JSON data responses"""
        try:
            json_data = response.get('response_data', {})
            request_id = response.get('request_id')
            
            # Store JSON data in database
            self._store_json_data(request_id, json_data)
            
            logger.info(f"JSON response processed for request {request_id}")
            
        except Exception as e:
            logger.error(f"Error handling JSON response: {e}")


    def _handle_error_response(self, response: Dict[str, Any]):
        """Handle error responses"""
        try:
            request_id = response.get('request_id')
            error_message = response.get('error_message', 'Unknown error')
            status_code = response.get('status_code', 500)
            
            # Store error in database
            self._store_error_response(request_id, error_message, status_code)
            
            logger.error(f"Error response processed for request {request_id}: {error_message}")
            
        except Exception as e:
            logger.error(f"Error handling error response: {e}")


    def _handle_default_response(self, response: Dict[str, Any]):
        """Handle all other responses"""
        try:
            request_id = response.get('request_id')
            response_data = response.get('response_data', {})
            
            # Store raw response data
            self._store_raw_response(request_id, response_data)
            
            logger.info(f"Default response processed for request {request_id}")
            
        except Exception as e:
            logger.error(f"Error handling default response: {e}")


    def _add_to_polling_status(self, request_id: str, identifier: str):
        """Add request to polling status table"""
        try:
            with pyodbc.connect(self.sql_conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO bloomberg_polling_status (request_id, identifier, status, poll_count, max_polls, created_at)
                    VALUES (?, ?, 'polling', 0, ?, GETDATE())
                """, request_id, identifier, self.max_poll_attempts)
                conn.commit()
                
        except Exception as e:
            logger.error(f"Error adding request to polling status: {e}")


    def _update_poll_count(self, request_id: str, poll_count: int):
        """Update poll count for a request"""
        try:
            with pyodbc.connect(self.sql_conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE bloomberg_polling_status 
                    SET poll_count = ?, last_polled_at = GETDATE()
                    WHERE request_id = ?
                """, poll_count, request_id)
                conn.commit()
                
        except Exception as e:
            logger.error(f"Error updating poll count: {e}")


    def _update_polling_status(self, request_id: str, status: str):
        """Update polling status for a request"""
        try:
            with pyodbc.connect(self.sql_conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE bloomberg_polling_status 
                    SET status = ?, completed_at = GETDATE()
                    WHERE request_id = ?
                """, status, request_id)
                conn.commit()
                
        except Exception as e:
            logger.error(f"Error updating polling status: {e}")


    def _handle_polling_error(self, request_id: str, error_message: str, status_code: int):
        """Handle polling errors"""
        try:
            # Update polling status
            self._update_polling_status(request_id, 'error')
            
            # Create error response
            error_payload = {
                'request_id': request_id,
                'error_message': error_message,
                'status_code': status_code,
                'timestamp': datetime.now().isoformat()
            }
            
            # Add to error queue
            self.redis_client.lpush(self.ERROR_QUEUE, json.dumps(error_payload))
            
            logger.error(f"Polling error for request {request_id}: {error_message}")
            
        except Exception as e:
            logger.error(f"Error handling polling error: {e}")


    def _refresh_oauth_token(self):
        """Refresh OAuth2 token"""
        try:
            token = self.session.refresh_token(
                self.OAUTH2_ENDPOINT,
                client_id=self.client_id,
                client_secret=self.client_secret
            )
            logger.info("OAuth2 token refreshed successfully")
            
        except Exception as e:
            logger.error(f"Failed to refresh OAuth2 token: {e}")
            # Re-initialize session if refresh fails
            self._initialize_oauth_session()


    def _store_csv_data(self, request_id: str, csv_data: str):
        """Store CSV data in database"""
        try:
            with pyodbc.connect(self.sql_conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO bloomberg_response_data (request_id, data_type, data_content, created_at)
                    VALUES (?, 'csv', ?, GETDATE())
                """, request_id, csv_data)
                conn.commit()
                
        except Exception as e:
            logger.error(f"Error storing CSV data: {e}")


    def _store_json_data(self, request_id: str, json_data: Dict[str, Any]):
        """Store JSON data in database"""
        try:
            with pyodbc.connect(self.sql_conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO bloomberg_response_data (request_id, data_type, data_content, created_at)
                    VALUES (?, 'json', ?, GETDATE())
                """, request_id, json.dumps(json_data))
                conn.commit()
                
        except Exception as e:
            logger.error(f"Error storing JSON data: {e}")


    def _store_raw_response(self, request_id: str, response_data: Any):
        """Store raw response data in database"""
        try:
            with pyodbc.connect(self.sql_conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO bloomberg_response_data (request_id, data_type, data_content, created_at)
                    VALUES (?, 'raw', ?, GETDATE())
                """, request_id, json.dumps(response_data, default=str))
                conn.commit()
                
        except Exception as e:
            logger.error(f"Error storing raw response: {e}")


    def _store_error_response(self, request_id: str, error_message: str, status_code: int):
        """Store error response in database"""
        try:
            with pyodbc.connect(self.sql_conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO error_responses (request_id, error_message, status_code, created_at)
                    VALUES (?, ?, ?, GETDATE())
                """, request_id, error_message, status_code)
                conn.commit()
                
        except Exception as e:
            logger.error(f"Error storing error response: {e}")


    def get_polling_status(self, request_id: str) -> Optional[Dict[str, Any]]:
        """Get polling status for a specific request"""
        try:
            with pyodbc.connect(self.sql_conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT request_id, identifier, status, poll_count, max_polls, 
                           created_at, last_polled_at, completed_at
                    FROM bloomberg_polling_status 
                    WHERE request_id = ?
                """, request_id)
                
                row = cursor.fetchone()
                if row:
                    return {
                        'request_id': row.request_id,
                        'identifier': row.identifier,
                        'status': row.status,
                        'poll_count': row.poll_count,
                        'max_polls': row.max_polls,
                        'created_at': row.created_at,
                        'last_polled_at': row.last_polled_at,
                        'completed_at': row.completed_at
                    }
                return None
                
        except Exception as e:
            logger.error(f"Error getting polling status: {e}")
            return None


    def cleanup_old_requests(self, days_old: int = 7):
        """Clean up old completed/error requests"""
        try:
            with pyodbc.connect(self.sql_conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    DELETE FROM bloomberg_polling_status 
                    WHERE status IN ('completed', 'error') 
                    AND created_at < DATEADD(day, -?, GETDATE())
                """, days_old)
                
                deleted_count = cursor.rowcount
                conn.commit()
                
                logger.info(f"Cleaned up {deleted_count} old polling requests")
                
        except Exception as e:
            logger.error(f"Error cleaning up old requests: {e}")


# Example usage and main execution
if __name__ == "__main__":
    try:
        # Initialize poller
        poller = BloombergResponsePoller()
        
        # Register custom handler if needed
        def custom_handler(response):
            print(f"Custom handler received: {response.get('request_id')}")
        
        poller.register_handler(
            "custom_handler",
            lambda r: r.get('data_type') == 'csv',
            custom_handler
        )
        
        # Start polling
        poller.start_polling()
        
    except Exception as e:
        logger.error(f"Failed to start Bloomberg Response Poller: {e}")