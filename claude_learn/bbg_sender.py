import redis
import pyodbc
import json
import uuid
import time
import logging
import os
import ASL.utils.secrets

from datetime import datetime
from typing import Dict, Any, Optional
from urllib.parse import urljoin
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
from dataclasses import dataclass

### NOTE THE STARTUP PARAMETERS NEED WORK - DB default user et. al.
## Just trying to get it running and tested

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class BloombergRequest:
    request_id: str
    identifier: str
    request_payload: Dict[str, Any]
    priority: int = 1
    retry_count: int = 0
    max_retries: int = 3

class BloombergRequestSender:
    def __init__(self, redis_host='cacheuat', redis_port=6379, redis_db=0,
                 sql_server_conn_str=None, catalog=None, client_id=None, client_secret=None):
        """
        Initialize the Bloomberg Request Sender
        
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
        self.request_response_base = f'/eap/catalogs/{self.catalog}'
        
        # Initialize OAuth2 session
        self._initialize_oauth_session()
        
        # Redis keys
        self.REQUEST_QUEUE = "bloomberg:request_queue"
        self.RESPONSE_QUEUE = "bloomberg:response_queue"
        self.PROCESSING_SET = "bloomberg:processing"
        self.POLLING_QUEUE = "bloomberg:polling_queue"
        

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
            
            logger.info("OAuth2 session initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize OAuth2 session: {e}")
            raise
    
   
   
    def _token_updater(self, token):
        """Handle token updates"""
        logger.info("OAuth2 token updated")
        # Could store updated token in database if needed
        return token
    
    
    def submit_data_request(self, request_name: str, title: str, universe: Dict[str, Any], 
                           field_list: Dict[str, Any], output_format: str = "text/csv",
                           priority: int = 1, max_retries: int = 3) -> str:
        """
        Submit a Bloomberg Data License request
        
        Args:
            request_name: Name for the request
            title: Title/description for the request
            universe: Universe containing identifiers
            field_list: List of fields to retrieve
            output_format: Output format (text/csv, application/json, etc.)
            priority: Request priority (1=high, 2=medium, 3=low)
            max_retries: Maximum retry attempts
            
        Returns:
            request_id: Unique identifier for the request
        """
        request_id = str(uuid.uuid4())
        identifier = f"{request_name}{str(uuid.uuid4())[0:4]}"
        
        # Build Bloomberg request payload
        request_payload = {
            "@type": "DataRequest",
            "name": request_name,
            "identifier": identifier,
            "title": title,
            "universe": universe,
            "fieldList": field_list,
            "trigger": {
                "@type": "SubmitTrigger"
            },
            "formatting": {
                "@type": "MediaType",
                "outputMediaType": output_format
            }
        }
        
        bloomberg_request = BloombergRequest(
            request_id=request_id,
            identifier=identifier,
            request_payload=request_payload,
            priority=priority,
            max_retries=max_retries
        )
        
        # Store in database
        self._store_request_in_db(bloomberg_request, request_name, title)
        
        # Add to Redis queue
        self._queue_request(bloomberg_request)
        
        logger.info(f"Bloomberg request {request_id} submitted with identifier: {identifier}")
        return request_id
    
 
 
    def submit_cusip_request(self, cusips: list, fields: list, request_name: str = "CusipInfo",
                            title: str = "CUSIP Data Request", priority: int = 1) -> str:
        """
        Convenience method to submit CUSIP-based requests
        
        Args:
            cusips: List of CUSIP identifiers
            fields: List of field mnemonics
            request_name: Name for the request
            title: Title for the request
            priority: Request priority
            
        Returns:
            request_id: Unique identifier for the request
        """
        # Build universe from CUSIPs
        universe = {
            "@type": "Universe",
            "contains": [
                {
                    "@type": "Identifier",
                    "identifierType": "CUSIP",
                    "identifierValue": cusip
                } for cusip in cusips
            ]
        }
        
        # Build field list
        field_list = {
            "@type": "DataFieldList",
            "contains": [
                {"mnemonic": field} for field in fields
            ]
        }
        
        return self.submit_data_request(request_name, title, universe, field_list, priority=priority)
    
 
 
    def _store_request_in_db(self, request: BloombergRequest, request_name: str, title: str):
        """Store request in SQL Server database"""
        try:
            with pyodbc.connect(self.sql_conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO bloomberg_requests 
                    (request_id, identifier, request_name, request_title, request_payload, priority, max_retries, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'queued')
                """, (
                    request.request_id,
                    request.identifier,
                    request_name,
                    title,
                    json.dumps(request.request_payload),
                    request.priority,
                    request.max_retries
                ))
                conn.commit()
                
        except Exception as e:
            logger.error(f"Error storing request in database: {e}")
            raise
    
   
   
    def _queue_request(self, request: BloombergRequest):
        """Add request to Redis priority queue"""
        request_data = {
            'request_id': request.request_id,
            'identifier': request.identifier,
            'request_payload': request.request_payload,
            'priority': request.priority,
            'retry_count': request.retry_count,
            'max_retries': request.max_retries,
            'timestamp': datetime.now().isoformat()
        }
        
        # Use priority as score (lower number = higher priority)
        self.redis_client.zadd(self.REQUEST_QUEUE, {json.dumps(request_data): request.priority})
    
   
   
    def process_requests(self):
        """Main processing loop for sending requests"""
        logger.info("Starting Bloomberg request processing loop...")
        
        while True:
            try:
                # Get highest priority request
                requests_data = self.redis_client.zrange(self.REQUEST_QUEUE, 0, 0, withscores=True)
                
                if not requests_data:
                    time.sleep(1)
                    continue
                
                request_json, priority = requests_data[0]
                request_data = json.loads(request_json)
                
                # Move to processing set
                self.redis_client.zrem(self.REQUEST_QUEUE, request_json)
                self.redis_client.sadd(self.PROCESSING_SET, request_data['request_id'])
                
                # Process the request
                self._process_single_request(request_data)
                
                # Remove from processing set
                self.redis_client.srem(self.PROCESSING_SET, request_data['request_id'])
                
            except Exception as e:
                logger.error(f"Error in request processing loop: {e}")
                time.sleep(5)
    
  
  
    def _process_single_request(self, request_data: Dict[str, Any]):
        """Process a single Bloomberg Data License request"""
        request_id = request_data['request_id']
        identifier = request_data['identifier']
        
        try:
            # Update status in database
            self._update_request_status(request_id, 'processing')
            
            # Submit request to Bloomberg
            response = self._submit_to_bloomberg(request_data)
            
            if response.status_code == 200:
                # Request submitted successfully
                self._update_request_status(request_id, 'submitted')
                self._update_submitted_timestamp(request_id)
                
                # Add to polling queue
                polling_data = {
                    'request_id': request_id,
                    'identifier': identifier,
                    'submitted_at': datetime.now().isoformat()
                }
                self.redis_client.lpush(self.POLLING_QUEUE, json.dumps(polling_data))
                
                logger.info(f"Request {request_id} submitted successfully to Bloomberg")
            else:
                # Handle submission failure
                error_msg = f"Bloomberg submission failed: {response.status_code} - {response.text}"
                logger.error(f"Request {request_id}: {error_msg}")
                self._handle_request_failure(request_data, error_msg)
            
        except Exception as e:
            logger.error(f"Error processing request {request_id}: {e}")
            self._handle_request_failure(request_data, str(e))
    
   
   
    def _submit_to_bloomberg(self, request_data: Dict[str, Any]) -> Any:
        """Submit request to Bloomberg Data License API"""
        request_uri = urljoin(self.HOST, self.request_response_base) + '/requests/'
        payload = request_data['request_payload']
        
        logger.info(f"Submitting to Bloomberg: {request_uri}")
        logger.debug(f"Payload: {json.dumps(payload, indent=2)}")
        
        response = self.session.post(
            request_uri, 
            json=payload, 
            headers={'api-version': '2'},
            timeout=30
        )
        
        return response
    


    def _handle_request_failure(self, request_data: Dict[str, Any], error_message: str):
        """Handle failed request with retry logic"""
        request_id = request_data['request_id']
        retry_count = request_data.get('retry_count', 0)
        max_retries = request_data.get('max_retries', 3)
        
        if retry_count < max_retries:
            # Retry the request
            request_data['retry_count'] = retry_count + 1
            request_data['priority'] = request_data.get('priority', 1) + 1  # Lower priority for retries
            
            # Re-queue with delay
            time.sleep(2 ** retry_count)  # Exponential backoff
            self.redis_client.zadd(self.REQUEST_QUEUE, {json.dumps(request_data): request_data['priority']})
            
            logger.info(f"Request {request_id} queued for retry {retry_count + 1}/{max_retries}")
        else:
            # Mark as failed
            self._update_request_status(request_id, 'failed')
            self._store_error_response(request_id, error_message)
            
            logger.error(f"Request {request_id} failed permanently after {max_retries} retries")
    
   
   
    def _update_request_status(self, request_id: str, status: str):
        """Update request status in database"""
        try:
            with pyodbc.connect(self.sql_conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE bloomberg_requests 
                    SET status = ?, updated_at = GETDATE()
                    WHERE request_id = ?
                """, (status, request_id))
                conn.commit()
                
        except Exception as e:
            logger.error(f"Error updating request status: {e}")
    
    
    
    def _update_submitted_timestamp(self, request_id: str):
        """Update submitted timestamp in database"""
        try:
            with pyodbc.connect(self.sql_conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE bloomberg_requests 
                    SET submitted_at = GETDATE()
                    WHERE request_id = ?
                """, (request_id,))
                conn.commit()
                
        except Exception as e:
            logger.error(f"Error updating submitted timestamp: {e}")
    
    
    
    def _store_error_response(self, request_id: str, error_message: str):
        """Store error response in database"""
        try:
            with pyodbc.connect(self.sql_conn_str) as conn:
                cursor = conn.cursor()
                response_id = str(uuid.uuid4())
                cursor.execute("""
                    INSERT INTO bloomberg_responses 
                    (response_id, request_id, identifier, error_message)
                    VALUES (?, ?, '', ?)
                """, (response_id, request_id, error_message))
                conn.commit()
                
        except Exception as e:
            logger.error(f"Error storing error response: {e}")
    
    
    
    def get_request_status(self, request_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a specific request"""
        try:
            with pyodbc.connect(self.sql_conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT r.*, resp.status_code, resp.response_data, resp.error_message, resp.snapshot_timestamp
                    FROM bloomberg_requests r
                    LEFT JOIN bloomberg_responses resp ON r.request_id = resp.request_id
                    WHERE r.request_id = ?
                """, (request_id,))
                
                row = cursor.fetchone()
                if row:
                    return {
                        'request_id': row.request_id,
                        'identifier': row.identifier,
                        'request_name': row.request_name,
                        'request_title': row.request_title,
                        'status': row.status,
                        'priority': row.priority,
                        'retry_count': row.retry_count,
                        'created_at': row.created_at,
                        'updated_at': row.updated_at,
                        'submitted_at': row.submitted_at,
                        'response_status_code': row.status_code,
                        'response_data': row.response_data,
                        'snapshot_timestamp': row.snapshot_timestamp,
                        'error_message': row.error_message
                    }
                return None
                
        except Exception as e:
            logger.error(f"Error getting request status: {e}")
            return None


#*****************************************************
#
#  MAIN MAIN
#*********************************************************
if __name__ == "__main__":
    # Example usage
    sender = BloombergRequestSender()
    
    # Example: Submit a CUSIP request
    # request_id = sender.submit_cusip_request(
    #     cusips=["91282CMV0", "91282CGS4"],
    #     fields=["SECURITY_DES", "MATURITY", "ISSUE_DT"],
    #     request_name="BondInfo",
    #     title="Get Bond Information",
    #     priority=1
    # )

    request_id = sender.submit_cusip_request(
        cusips=["91282CMV0"],
        fields=["SECURITY_DES"],
        request_name="BondInfo",
        title="Get Bond Information",
        priority=1
    )
    
    print(f"Submitted Bloomberg request: {request_id}")
    
    # Start processing (this would typically run in a separate process/service)
    # sender.process_requests()