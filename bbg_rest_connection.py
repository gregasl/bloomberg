import os
import logging
from typing import Any, Dict
import uuid

import ASL.utils.secrets
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import BackendApplicationClient
from bbg_database import BloombergDatabase
from bbg_request import BloombergRequest

logger = logging.getLogger(__name__)


class BloombergRestConnection:
    def _initialize_oauth_session(self):
        """Initialize OAuth2 session for Bloomberg API"""
        try:
            client = BackendApplicationClient(client_id=self.client_id)
            self.HOST = 'https://api.bloomberg.com'
            self.OAUTH2_ENDPOINT = 'https://bsso.blpprofessional.com/ext/api/as/token.oauth2'

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

    def _token_updater(self, token):
        """Handle token updates"""
        logger.info("OAuth2 token updated")
        # Could store updated token in database if needed
        return token

    def set_db_connection(self, _db_connection: BloombergDatabase):
        self.db_connection = _db_connection

    
def submit_to_bloomberg(self, request_data: Dict[str, Any]) -> Any:
    """Submit request to Bloomberg Data License API"""
    request_uri = urljoin(self.HOST, self.request_response_base) + "/requests/"
    payload = request_data["request_payload"]

    logger.info(f"Submitting to Bloomberg: {request_uri}")
    logger.debug(f"Payload: {json.dumps(payload, indent=2)}")

    response = self.session.post(
        request_uri, json=payload, headers={"api-version": "2"}, timeout=30
    )

    return response
