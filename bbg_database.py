import json
import os
import logging
from typing import Any, Dict, List, Optional
import uuid
from asql import SQLObject
from bbg_request import BloombergRequest


logger = logging.getLogger(__name__)
## may need to rename this.


class BloombergDatabase:
    def __init__(
        self,
        server: str = None,
        port: str = None,
        database: str = None,
        username: str = None,
    ):
        """
        Initialize the Bloomberg database connection and utilitie ...

        Args:
            server: Machine name of the DB server will connect to.  defualt MSSQL_SERVER
            port: Network port to connect on defautlt to 1433 as a string! default MSSQL_TCP_PORT
            database: name of db to connect to - default MSSQL_DATABASE
            username:  If None defaults to Microsoft credentials
        """

        self.server = server or os.environ.get("MSSQL_SERVER", "")
        self.port = port or os.environ.get("MSSQL_TCP_PORT", "")
        self.database = database or os.environ.get("MSSQL_DATABASE", "")
        self.username = username

        if not all([self.server, self.port, self.database]):
            raise ValueError(
                "Database connection items not set 'host', 'port', 'database'"
            )

        # SQL Server connection string...
        # not sure what to do w/ port here.  I should play with it.
        self.db_connection = SQLObject(
            server=self.server, username=username, database=self.database
        )

    def save_bbg_request(
        self, request: BloombergRequest, request_name: str, title: str
    ):
        """Store request in SQL Server database"""
        try:
            query: str = """
                    INSERT INTO bloomberg_requests 
                    (request_id, identifier, request_name, request_title, request_payload, priority, max_retries, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'queued')
                """
            params = (
                request.request_id,
                request.identifier,
                request_name,
                title,
                json.dumps(request.request_payload),
                request.priority,
                request.max_retries,
            )
            logger.info(query)
            self.db_connection.execute_param_query(
                query=query, params=params, commit=True
            )

        except Exception as e:
            logger.error(f"Error storing request in database: {e}")
            raise

    def update_request_status(self, request_id: str, status: str):
        """Update request status in database"""
        try:
            query: str = (
                "UPDATE bloomberg_requests SET status = ?, updated_at = GETDATE() WHERE request_id = ?"
            )
            params = (status, request_id)
            logger.info(query)
            self.db_connection.execute_param_query(
                query=query, params=params, commit=True
            )

        except Exception as e:
            logger.error(f"Error updating request status: {e}")

    def update_submitted_timestamp(self, request_id: str):
        """Update submitted timestamp in database"""
        try:
            query: str = (
                "UPDATE bloomberg_requests SET submitted_at = GETDATE() WHERE request_id = ?"
            )
            params = request_id
            logger.info(query + " " + request_id)
            self.db_connection.execute_param_query(
                query=query, params=params, commit=True
            )

        except Exception as e:
            logger.error(f"Error updating submitted timestamp: {e}")

    def store_error_response(self, request_id: str, error_message: str):
        """Store error response in database"""
        try:
            response_id = str(uuid.uuid4())
            query: str = """INSERT INTO bloomberg_responses 
                    (response_id, request_id, identifier, error_message)
                    VALUES (?, ?, '', ?)"""
            params = (response_id, request_id, error_message)
            logger.info(query)
            logger.info(params)
            self.db_connection.execute_param_query(
                query=query, params=params, commit=True
            )
        except Exception as e:
            logger.error(f"Error storing error response: {e}")

    def get_request_status(self, request_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a specific request"""
        try:
            query: str = f"""
                    SELECT r.*, resp.status_code, resp.response_data, resp.error_message, resp.snapshot_timestamp
                    FROM bloomberg_requests r
                    LEFT JOIN bloomberg_responses resp ON r.request_id = resp.request_id
                    WHERE r.request_id = {request_id}
                """
            logger.info(query)
            return_row = self.db_connection.fetch(query, "DICT")

            return return_row
        except Exception as e:
            logger.error(f"Error getting request status: {e}")
            return None

    def get_active_polling_requests(self) -> List[Dict[str, Any]]:
        """Get all requests currently being polled"""
        try:
            logger.info(f"Connecting with {self.sql_conn_str}")
            query: str = """
                SELECT request_id, identifier, poll_count, max_polls
                FROM bloomberg_polling_status 
                WHERE status = 'polling' AND poll_count < max_polls
            """
            logger.info(query)
            return self.db_connection.fetch(query, "DICT")

        except Exception as e:
            logger.error(f"Error getting active polling requests: {e}")
            return []

    def store_csv_data(self, request_id: str, csv_data: str):
        """Store CSV data in database"""
        try:
            query: str = """
                    INSERT INTO bloomberg_response_data (request_id, data_type, data_content, created_at)
                    VALUES (?, 'csv', ?, GETDATE())
                """
            params = (
                request_id,
                csv_data,
            )
            logger.info(query + " " + request_id)
            self.db_connection.execute_param_query(
                query=query, params=params, commit=True
            )

        except Exception as e:
            logger.error(f"Error storing CSV data: {e}")

    def store_json_data(self, request_id: str, json_data: Dict[str, Any]):
        """Store JSON data in database"""
        try:
            query: str = """
                    INSERT INTO bloomberg_response_data (request_id, data_type, data_content, created_at)
                    VALUES (?, 'json', ?, GETDATE())
                """
            params = (
                request_id,
                json.dumps(json_data),
            )
            logger.info(query + " " + request_id)
            self.db_connection.execute_param_query(
                query=query, params=params, commit=True
            )
        except Exception as e:
            logger.error(f"Error storing JSON data: {e}")

    def store_raw_response(self, request_id: str, response_data: Any):
        """Store raw response data in database"""
        try:
            query: str = """
                INSERT INTO bloomberg_response_data (request_id, data_type, data_content, created_at)
                VALUES (?, 'raw', ?, GETDATE())
            """
            params = (request_id,
                    json.dumps(response_data, default=str),
                )
            
            logger.info(query + " " + request_id)
            self.db_connection.execute_param_query(
                query=query, params=params, commit=True
            )
        except Exception as e:
            logger.error(f"Error storing raw response: {e}")

    def store_error_response(
        self, request_id: str, error_message: str, status_code: int
    ):
        """Store error response in database"""
        try:
            query: str = """
                    INSERT INTO error_responses (request_id, error_message, status_code, created_at)
                    VALUES (?, ?, ?, GETDATE())
                """
            params = (
                    request_id,
                    error_message,
                    status_code,
                )
            logger.info(query + " " + request_id)
            self.db_connection.execute_param_query(
                query=query, params=params, commit=True
            )

        except Exception as e:
            logger.error(f"Error storing error response: {e}")

    def get_polling_status(self, request_id: str) -> Optional[Dict[str, Any]]:
        """Get polling status for a specific request"""
        try:
            query: str = """
                    SELECT request_id, identifier, status, poll_count, max_polls, 
                            created_at, last_polled_at, completed_at
                    FROM bloomberg_polling_status 
                    WHERE request_id = ?
                """
            params = (request_id)
            logger.info(query + " " + request_id)
            return self.db_connection.fetch(query, "DICT", params=params)

        except Exception as e:
            logger.error(f"Error getting polling status: {e}")
            return None

    def cleanup_old_requests(self, days_old: int = 7):
        """Clean up old completed/error requests"""
        try:
            query: str = """
                DELETE FROM bloomberg_polling_status 
                WHERE status IN ('completed', 'error') 
                AND created_at < DATEADD(day, -?, GETDATE())
            """
            params = (days_old)
            self.db_connection.execute_param_query(
                query=query, params=params, commit=True
            )
            logger.info(f"Cleaned up old polling requests")

        except Exception as e:
            logger.error(f"Error cleaning up old requests: {e}")
