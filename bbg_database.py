import json  # for storing in db
import os
import logging
from typing import Any, Optional
import uuid
from ASL import SQLObject
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
        self, request: BloombergRequest, request_name: str, title: str, status : str ='pending'
    ):
        """Store request in SQL Server database"""
        try:
            query: str = """
                    INSERT INTO bloomberg_requests 
                    (request_id, identifier, request_name, request_title, request_payload, priority, max_request_retries, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """
            params = (
                request.request_id,
                request.identifier,
                request_name,
                title,
                json.dumps(request.request_payload),
                request.priority,
                request.max_retries,
                status,
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

    def set_request_failed(self, request_id : str):
        self.update_request_status(request_id, 'failed')


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
    
    def set_request_submitted(self, request_id):
        self.update_request_status(request_id, 'submitted')
        self.update_submitted_timestamp(request_id)

    def set_request_processing(self, request_id):
        self.update_request_status(request_id, 'processing')
    
    def store_send_error_response(self, request_id: str, error_message: str):
        """
        Stores an error response associated with a given request ID in the database.

        Args:
            request_id (str): The unique identifier for the request that resulted in an error.
            error_message (str): The error message to be stored.

        Raises:
            Exception: Logs any exceptions that occur during the database operation.
        """
        try:
            response_id = str(uuid.uuid4())
            query: str = """INSERT INTO bloomberg_responses 
                    (response_id, request_id, identifier, error_message)
                    VALUES (?, ?, '', ?)"""
            params: tuple = (response_id, request_id, error_message)
            logger.info(query)
            logger.info(params)
            self.db_connection.execute_param_query(
                query=query, params=params, commit=True
            )
        except Exception as e:
            logger.error(f"Error storing error response: {e}")

    def get_request_status(self, request_id: str) -> Optional[dict[str, Any]]:
        """Retrieve the status and related information for a specific Bloomberg request.
        Args:
            request_id (str): The unique identifier of the request to retrieve.
        Returns:
            Optional[Dict[str, Any]]: A dictionary containing the request and response details if found,
            otherwise None. The dictionary includes fields from both the 'bloomberg_requests' and
            'bloomberg_responses' tables, such as status_code, response_data, error_message, and
            snapshot_timestamp.
        Logs:
            - The executed SQL query at info level.
            - Any exceptions encountered at error level.
        """
        try:
            query: str = f"""
                    SELECT r.request_id, r.identifier, r.request_name, r.request_title, r.status, r.priority,
                    r.request_retry_count, r.max_request_retries, r.submitted_at, r.response_poll_count, 
                    r.max_response_polls, r.last_poll_at, r.created_at, r.updated_at
                    FROM bloomberg_requests r
                    WHERE r.request_id = {request_id}
                """
            logger.info(query)
            return_row = self.db_connection.fetch(query, "DICT")

            return return_row
        except Exception as e:
            logger.error(f"Error getting request status: {e}")
            return None

    def get_active_polling_requests(self) -> list[dict[str, Any]]:
        """Retrieve all active polling requests from the 'bloomberg_requests' table.
        Returns:
            list[dict[str, Any]]: A list of dictionaries, each representing a request that is currently being polled
            (i.e., has status 'polling' and poll_count less than max_polls). Returns an empty list if an error occurs.
        """
        try:
            query: str = """
                SELECT request_id, identifier, response_poll_count, max_response_polls
                FROM bloomberg_requests
                WHERE status = 'submitted' and response_poll_count < max_response_polls
            """
            logger.info(query)
            return self.db_connection.fetch(query, "DICT")

        except Exception as e:
            logger.error(f"Error getting active polling requests: {e}")
            return []

    def get_sumbitted_requests(self) -> list[dict[str, Any]]:
        """Retrieves all requests from the 'bloomberg_requests' table that have a status of 'submitted'.
        Returns:
            list[dict[str, Any]]: A list of dictionaries, each containing the 'request_id' and 'identifier'
            of a submitted request. Returns an empty list if an error occurs during the database query."""
        try:
            query: str = """
                SELECT request_id, identifier
                FROM bloomberg_requests 
                WHERE status = 'submitted'
            """
            logger.info(query)
            return self.db_connection.fetch(query, "DICT")

        except Exception as e:
            logger.error(f"Error getting active polling requests: {e}")
            return []

    def store_csv_data(self, request_id: str, csv_data: str):
        """Stores CSV data associated with a given request ID into the 'bloomberg_response_data' database table.
        Args:
            request_id (str): The unique identifier for the request.
            csv_data (str): The CSV-formatted data to be stored.
        Raises:
            Exception: Logs any exception that occurs during the database operation."""
        try:
            query: str = """
                    INSERT INTO bloomberg_data (request_id, data_type, data_content, created_at)
                    VALUES (?, 'csv', ?, GETDATE())
                """
            params: tuple = (
                request_id,
                csv_data,
            )
            logger.info(query + " " + request_id)
            self.db_connection.execute_param_query(
                query=query, params=params, commit=True
            )

        except Exception as e:
            logger.error(f"Error storing CSV data: {e}")

    def store_json_data(self, request_id: str, json_data: dict[str, Any]):
        """Store JSON data in database"""
        try:
            query: str = """
                    INSERT INTO bloomberg_data (request_id, data_type, data_content, created_at)
                    VALUES (?, 'json', ?, GETDATE())
                """
            params: tuple = (
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
                INSERT INTO bloomberg_data (request_id, data_type, data_content, created_at)
                VALUES (?, 'raw', ?, GETDATE())
            """
            params: tuple = (
                request_id,
                json.dumps(response_data) if response_data.len() > 0 else "",
            )

            logger.info(query + " " + request_id)
            self.db_connection.execute_param_query(
                query=query, params=params, commit=True
            )
        except Exception as e:
            logger.error(f"Error storing raw response: {e}")

    def store_poll_error_response(
        self, request_id: str, error_message: str, status_code: int
    ):
        """Store error response in database"""
        try:
            query: str = """
                    INSERT INTO error_responses (request_id, error_message, status_code, created_at)
                    VALUES (?, ?, ?, GETDATE())
                """
            params: tuple = (
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


    def update_poll_count(self, request_id: str, poll_count: int):
        """Update poll count for a request"""
        try:
            query : str = """
                    UPDATE bloomberg_request 
                    SET poll_count = ?, last_polled_at = GETDATE()
                    WHERE request_id = ?
                """
            params : tuple = (
                    poll_count,
                    request_id,
                )
            logger.info(query + " " + request_id)
            self.db_connection.execute_param_query(
                query=query, params=params, commit=True
            )

        except Exception as e:
            logger.error(f"Error updating poll count: {e}")


    def cleanup_old_requests(self, days_old: int = 7):
        """Clean up old completed/error requests"""
        try:
            query: str = """
                DELETE FROM bloomberg_polling_status 
                WHERE status IN ('completed', 'error') 
                AND created_at < DATEADD(day, -?, GETDATE())
            """
            params = days_old
            self.db_connection.execute_param_query(
                query=query, params=params, commit=True
            )
            logger.info(f"Cleaned up old polling requests")

        except Exception as e:
            logger.error(f"Error cleaning up old requests: {e}")
