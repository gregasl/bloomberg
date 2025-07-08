import datetime
from datetime import timedelta
import json  # for storing in db
import os
import logging
from typing import Any, Optional, Tuple
import uuid
from ASL.utils.asql import SQLObject
# from ASL import SQLObject
from bbg_request import BloombergRequest

logger = logging.getLogger(__name__)
## may need to rename this.

## TO DO - make status constants...

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

        self.server = server or os.environ.get("BBG_SQL_SERVER", "")
        self.port = port or os.environ.get("BBG_SQL_PORT", "")
        self.database = database or os.environ.get("BBG_DATABASE", "")
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

    def close(self):
        self.db_connection.close()

    def save_bbg_request(
        self, request: BloombergRequest, title: str, status : str ='pending'
    ):
        """Store request in SQL Server database"""
        try:
            query: str = """
                    INSERT INTO bloomberg_requests 
                    (request_id, identifier, name, title, payload, priority, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """
            params : tuple = (
                request.request_id,
                request.identifier,
                request.request_name,
                title,
                json.dumps(request.request_payload),
                request.priority,
                status,
            )
            logger.info(query)
            self.db_connection.execute_param_query(
                query=query, params=params, commit=True
            )

        except Exception as e:
            logger.error(f"Error storing request in database: {e}")
            raise

    def update_request_status(self, request_id: str, status: str, time_update : str = ""):
        """Update request status in database"""
        try:
            query: str = " ".join(
                ["UPDATE bloomberg_requests SET status = ?, updated_at = GETDATE()",
                time_update,
                "WHERE request_id = ?"]
            )
            params = (status, request_id)
            logger.info(query)
            self.db_connection.execute_param_query(
                query=query, params=params, commit=True
            )

        except Exception as e:
            logger.error(f"Error updating request status: {e}")
            raise

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
            raise

    def update_response_poll(self, request_id: str):
        """Update response poll and timer in database"""
        try:
            query: str = (
                "UPDATE bloomberg_requests SET response_poll_count = response_poll_count + 1, last_poll_at = GETDATE() WHERE request_id = ?"
            )
            params = request_id
            logger.info(query + " " + request_id)
            self.db_connection.execute_param_query(
                query=query, params=params, commit=True
            )

        except Exception as e:
            logger.error(f"Error updating submitted timestamp: {e}")
            raise


    def set_request_submitted(self, request_id):
        self.update_request_status(request_id, 'submitted', time_update=",submitted_at = GETDATE()")

    def set_request_completed(self, request_id):
        self.update_request_status(request_id, 'completed', time_update=",completed_at = GETDATE()")

    def set_request_processing(self, request_id):
        self.update_request_status(request_id, 'processing')
    
    def store_response(self, request_id : str, msg : str, status : str):
        try:
            response_id = str(uuid.uuid4())
            query: str = (
                "UPDATE bloomberg_requests SET response_id = ?, response = ?, response_status = ? WHERE request_id = ?"
            )
            params: tuple = (response_id, msg, status, request_id)
            logger.info(query)
            logger.info(params)
            self.db_connection.execute_param_query(
                query=query, params=params, commit=True
            )
        except Exception as e:
            logger.error(f"Error storing response msg: {e}")
            raise

    def store_error_response(self, request_id: str, error_message: str):
        """
        Stores an error response associated with a given request ID in the database.

        Args:
            request_id (str): The unique identifier for the request that resulted in an error.
            error_message (str): The error message to be stored.

        Raises:
            Exception: Logs any exceptions that occur during the database operation.
        """
        self.store_response(request_id=request_id, msg=error_message, status='error')

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
            query: str = """SELECT r.request_id, r.identifier, r.name, r.title, r.status, r.priority,
                    r.request_retry_count, r.max_request_retries, r.submitted_at, r.response_poll_count, 
                    r.max_response_polls, r.last_poll_at, r.created_at, r.updated_at
                    FROM bloomberg_requests r
                    WHERE r.request_id = ?"""
            logger.info(query)
            params : tuple = (request_id)
            return_row = self.db_connection.fetch(query, "DICT", params=params
                                                  )

            return return_row
        except Exception as e:
            logger.error(f"Error getting request status: {e}")
            return None

    # we return the row to save a re-call...
    def is_request_ready(self, request_id: str) -> Tuple[bool, dict[str, Any]]:
        rows : Optional[dict[str, Any]] = self.get_request_status(request_id)
        if rows and len(rows) > 0:
            row = rows[0]
            return (row["status"] == 'completed', row)
        
        return (False, None)

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
                SELECT request_id, identifier, name as request_name
                FROM bloomberg_requests 
                WHERE status = 'submitted'
            """
            logger.info(query)
            return self.db_connection.fetch(query, "DICT")

        except Exception as e:
            logger.error(f"Error getting active polling requests: {e}")
            return []

    def store_csv_data(self, request_id: str, identifier: str, request_name:str, csv_data: str):
        """Stores CSV data into the 'bloomberg_data' table in the database.
        Args:
            request_id (str): The unique identifier for the data request.
            identifier (str): The identifier associated with the data (e.g., a ticker symbol).
            csv_data (str): The CSV-formatted data to be stored.
        Raises:
            Exception: Logs and raises any exception that occurs during the database operation."""

        try:
            query: str = """
                    INSERT INTO bloomberg_data (request_id, identifier, request_name, data_type, data_content, ts)
                    VALUES (?, ?, ?, 'csv', ?, GETDATE())
                """
            params: tuple = (
                request_id,
                identifier,
                request_name,
                csv_data,
            )
            logger.info(query + " " + request_id)
            self.db_connection.execute_param_query(
                query=query, params=params, commit=True
            )

        except Exception as e:
            logger.error(f"Error storing CSV data: {e}")
            raise

    def store_json_data(self, request_id: str, identifier: str, request_name : str, json_data: dict[str, Any]):
        """Store JSON data in database"""
        try:
            query: str = """
                    INSERT INTO bloomberg_data (request_id, data_type, request_name, data_content, created_at)
                    VALUES (?, ?,'json', ?, GETDATE())
                """
            params: tuple = (
                request_id,
                identifier,
                request_name,
                json.dumps(json_data),
            )
            logger.info(query + " " + request_id)
            self.db_connection.execute_param_query(
                query=query, params=params, commit=True
            )
        except Exception as e:
            logger.error(f"Error storing JSON data: {e}")
            raise

    def store_raw_response(self, request_id: str, identifier: str, request_name, response_data: Any):
        """Store raw response data in database"""
        try:
            query: str = """
                INSERT INTO bloomberg_data (request_id, data_type, request_name, data_content, created_at)
                VALUES (?, 'raw', ?, ?, GETDATE())
            """
            # not sure about json dumps raw?
            params: tuple = (
                request_id,
                identifier,
                request_name,
                json.dumps(response_data) if response_data.len() > 0 else "",
            )

            logger.info(query + " " + request_id)
            self.db_connection.execute_param_query(
                query=query, params=params, commit=True
            )
        except Exception as e:
            logger.error(f"Error storing raw response: {e}")
            raise

    def store_poll_error_response(
        self, request_id: str, error_message: str
    ):
        self.store_response(request_id=request_id, msg=error_message, status='poll_error')

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
            raise

    def get_request_definitions(self) -> dict[str, dict[str, Any]]:
        try:
            query : str = """select request_name, request_title, priority, save_table, save_file,
            retry_wait_sec, max_request_retries, response_poll_wait_sec, max_response_polls
            from bloomberg_requests_def
            """
            logger.info(query)
            
            db_result : list[dict[str, Any]] = self.db_connection.fetch(query, "DICT")
            returnVal : dict[str, dict[str, Any]] = {}

            for row in db_result:
                returnVal[row['request_name']] = row

            return returnVal         
        except Exception as e:
           logger.error(f"Error getting request definitions: {e}")
           raise


    def get_last_date_for_request(self, request_name : str = None) -> dict[str, datetime.date]:
        try:
            limit_date = datetime.date.today() - timedelta(days=7)

            query : str = "select request_name, max(business_date) from bloomberg_data where business_date >= ?"
            params : tuple = (str(limit_date))

            if (request_name is not None):
                query = query + " and request_name = ?"
                params : tuple = (
                    limit_date,
                    request_name,
                )
            query = query + " group by request_name"
            logger.info(query)
            self.db_connection.execute_param_query(
                query=query, params=params, commit=True
            )
        except Exception as e:
           logger.error(f"Error getting last date for request: {e}")
           raise

    def _sort_columns(self, in_list : list[dict[str, Any]]) -> list[dict[str, Any]]:
        return(sorted(in_list, key=lambda i: i['col_order']))

    def load_bbg_column_defs(self, request_name : str = None) -> dict[str, list[dict[str, Any]]]:
        try:
            query = """select request_name, is_variable_data, suppress_sending, request_col_name, reply_col_name, col_order, data_type,
    output_col_name, db_col_name 
        FROM bloomberg_data_def where suppress_sending = 0 """
            if request_name is not None:
                query = query + f'and request_name = {request_name}'

            loaded_data = self.db_connection.fetch(query, 'DICT')
            def_data : dict[str, list[dict[str, Any]]] = {}

            for row in loaded_data:
                if row['request_name'] in def_data:
                    def_data[row['request_name']].append(row)
                else:
                    def_data[row['request_name']] = [row]                    

            for req_name in def_data:
                tmp_list =  self._sort_columns(def_data[req_name])
                def_data[req_name] = tmp_list # only for debugging - remove later

            return def_data
        except Exception as e:
            logger.error(f"Error updating request status: {e}")
            raise


    def get_cusips_for_date(self, request_name : str, bdate : datetime.date = None) -> list [str]:
          try:
            query : str = """
                    select cusip from bloomberg_data where  
                    WHERE request_name = ? and business_date >= ?
                """
            params : tuple = (
                    request_name,
                    bdate,
                )
            logger.info(query + " " + request_name)
            cusipLst : list[str] = self.db_connection.execute_param_query(
                query=query, params=params, commit=True
            )

            return cusipLst
          except Exception as e:
            logger.error(f"ERROR Getting cusips for date: {e}")

    def cleanup_old_requests(self, days_old: int = 7):
        """Clean up old completed/error requests"""
        logger.error("Needs to be rewritted clean up bloomberg_requests")()
        # try:
        #     query: str = """
        #         DELETE FROM bloomberg_polling_status 
        #         WHERE status IN ('completed', 'error') 
        #         AND created_at < DATEADD(day, -?, GETDATE())
        #     """
        #     params = days_old
        #     self.db_connection.execute_param_query(
        #         query=query, params=params, commit=True
        #     )
        #     logger.info(f"Cleaned up old polling requests")

        # except Exception as e:
        #     logger.error(f"Error cleaning up old requests: {e}")
