import json
import orjson
import time
import uuid
import logging
from ASL import ASL_Logging
# from ASL import ASL_DateRotatingFileHandler, ASL_RotatingFileHandler, ASL_TimedRotatingFileHandler

from typing import Any, Tuple

from bbg_rest_connection import BloombergRestConnection
from bbg_database import BloombergDatabase
from bbg_redis import BloombergRedis, POLLING_QUEUE, REQUEST_QUEUE
from bbg_request import BloombergRequest, DEFAULT_CMD_PRIORITY, DEFAULT_REQUEST_PRIORITY
from bbg_request import LAST_CMD_PRIORITY, REQUEST_TYPE_CMD, REQUEST_TYPE_BBG_REQUEST
from bloomberg_data_def import BloombergDataDef
from bbg_send_cmds import (
    EXIT_CMD,
    CLR_QUEUES,
    REQUEST_TSY_CUSIPS,
    REQUEST_FUT_CUSIPS,
    REQUEST_MBS_CUSIPS,
)
from get_cusip_list import get_phase3_tsy_cusips, get_futures_tickers, get_phase3_mbs_cusips

MAX_LOOPS = -1  # wait for exit...
MIN_WAIT_TIME = 2
MAX_WAIT_TIME = 20
### NOTE THE STARTUP PARAMETERS NEED WORK - DB default user et. al.
## Just trying to get it running and tested

# Attach logging
logger = logging.getLogger(__name__)


class BloombergRequestSender:
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
        Initialize the Bloomberg Request Sender

        Args:
            redis_host: Redis server host
            db_server: SQL server hostname
            db_port: SQL server port
            _database : database on server used
            catalog: Bloomberg Data License Account Number
            client_id: Bloomberg OAuth2 client ID
            client_secret: Bloomberg OAuth2 client secret
        """

        logger.info("Request Sender Initialization")
        ## db connection
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
        self.data_def = BloombergDataDef(self.db_connection)
        self.request_definitions = self.db_connection.get_request_definitions()
       # self.request_dates = self.db_connection.get_last_date_for_request()

        logger.info("Init done")

    def _handle_request_failure(
        self, request_data: BloombergRequest, error_message: str
    ):
        """Handle failed request with retry logic"""
        request_id = request_data.request_id
        retry_count = request_data.get("request_retry_count", 0)
        max_retries = request_data.get("max_request_retries", 3)

        if retry_count < max_retries:
            # Retry the request
            request_data["retry_count"] = retry_count + 1
            request_data["priority"] = (
                request_data.get("priority", 1) + 1
            )  # Lower priority for retries

            # Re-queue with delay
            time.sleep(2 * retry_count)

            logger.info(
                f"Request {request_id} queued for retry {retry_count + 1}/{max_retries}"
            )
        else:
            # Mark as failed
            self.db_connection.set_request_failed(request_id)
            self.db_connection.store_error_response(request_id, error_message)

            logger.error(
                f"Request {request_id} failed permanently after {max_retries} retries"
            )

    ## *******************************************************************

    def _process_single_request(self, request_data: BloombergRequest):
        try:
            request_id = request_data.request_id
            identifier = request_data.identifier
            request_name = request_data.request_name
        except Exception as e:
            logger.error(f"Error processing request: {e}")

        try:
            # Update status in database
            self.db_connection.save_bbg_request(request_data, title=request_data.request_payload['title'], status='processing')

            # Submit request to Bloomberg
            response = self.bbg_connection.submit_to_bloomberg(request_data)
            if response.status_code == 200 or response.status_code == 201:
                # Request submitted successfully
                self.db_connection.set_request_submitted(request_id)

                logger.info(f"Request {request_id} submitted successfully to Bloomberg")
            else:
                # Handle submission failure
                error_msg = f"Bloomberg submission failed: {response.status_code} - {response.text}"
                logger.error(f"Request {request_id}: {error_msg}")
                logger.debug(response)
                self._handle_request_failure(request_data, error_msg)

        except Exception as e:
            logger.error(f"Error processing request {request_id}: {e}")
            self._handle_request_failure(request_data, str(e))

    ## *****************************************************

    def _continue_processing(self, count: int, stop_processing: bool):
        if stop_processing:
            return not stop_processing

        return (MAX_LOOPS <= 0) or (count < MAX_LOOPS)

    ## *****************************************************

    def process_queued_requests(self):
        """Main processing loop for sending requests"""
        logger.info("Starting Bloomberg request processing loop...")
        count = 0
        sleep_time = MIN_WAIT_TIME
        stop_processing = False

        while self._continue_processing(count, stop_processing):
            try:
                # Get highest priority request
                count += 1
                requests_data = self.redis_connection.get_sender_request()
                if (logger.getEffectiveLevel() <= logging.DEBUG):
                    logger.debug("Looping...")
                    if (not requests_data):
                        logger.debug("Got NOTHING")
                    else:
                        logger.debug(f"got {requests_data}")

                if not requests_data:
                    time.sleep(sleep_time)
                    sleep_time = sleep_time * 2
                    sleep_time = (
                        MAX_WAIT_TIME if (sleep_time > MAX_WAIT_TIME) else sleep_time
                    )
                    continue

                for request_data in requests_data:
                    logger.debug(f"request from q {request_data}")
                    orig_request_json, priority = request_data
                    request_dict: dict[str, Any] = json.loads(orig_request_json)
                    cmd : str = request_dict['request_id']
                    cmd = cmd.upper()

                    if cmd == EXIT_CMD:
                        stop_processing = True
                        self.redis_connection.remove_sender_request(orig_request_json)
                        # break the for loop...
                        break 
                    else:  # process data commands
                        bbg_request : BloombergRequest = None

                        if cmd == REQUEST_TSY_CUSIPS:
                            bbg_request  = self.create_tsy_cusip_request()
                        elif cmd == REQUEST_FUT_CUSIPS:
                             bbg_request = self.create_futures_ticker_request()
                        elif cmd == REQUEST_MBS_CUSIPS:
                            bbg_request = self.create_mbs_cusip_request()  # self.submit_mbs_cusip_request()
                        else: # otherwise we try to use the json here to do something
                            # this is now wokinh
                            if (orig_request_json):
                                request_dict: dict[str, Any] = json.loads(orig_request_json)

                        if (bbg_request):
                            self._process_single_request(bbg_request)
                            self.redis_connection.remove_sender_request(orig_request_json)
                        elif orig_request_json:
                            bbg_request = BloombergRequest.create_from_json(orig_request_json)
                            self._process_single_request(bbg_request)
                            self.redis_connection.remove_sender_request(orig_request_json)
                        else:
                            logger.info(f"On command {requests_data} no json was generated")
                            time.sleep(sleep_time)  # IDK about these sleeps

            except Exception as e:
                logger.error(f"Error in request processing loop: {e}")
                time.sleep(sleep_time)

    ## puts a data request on a redis queue to be processed.
    ## I thin it is too convoluted.  the queue is probably not needed .
    ## lets get it to work then strip our.  Claude was confused as to what I meant.

    def create_data_request(
        self,
        request_name: str,
        title: str,
        universe: dict[str, Any],
        field_list: dict[str, Any],
        output_format: str = "text/csv",
        priority: int = DEFAULT_REQUEST_PRIORITY,
        max_retries: int = 3,
    ) -> BloombergRequest:
        """
        Submit a Bloomberg Data License request

        Args:
            request_name: Name for the request
            title: Title/description for the request
            universe: Universe containing identifiers
            field_list: list of fields to retrieve
            output_format: Output format (text/csv, application/json, etc.)
            priority: Request priority (1=high, 2=medium, 3=low)
            max_retries: Maximum retry attempts

        Returns:
            request_id: Unique identifier for the request
        """
        uid_str = str(uuid.uuid4())
        request_id = uid_str
        identifier = f"{request_name}{uid_str[:6]}"

        # Build Bloomberg request payload
        request_payload = {
            "@type": "DataRequest",
            "name": request_name,
            "identifier": identifier,
            "title": title,
            "universe": universe,
            "fieldList": field_list,
            "trigger": {"@type": "SubmitTrigger"},
            "formatting": {"@type": "MediaType", "outputMediaType": output_format},
        }

        bloomberg_request = BloombergRequest(
            request_id=request_id,
            request_type=REQUEST_TYPE_BBG_REQUEST,
            identifier=identifier,
            request_name=request_name,
            request_payload=request_payload,
            priority=priority,
            max_retries=max_retries,
        )

        # Store in database
        # self.db_connection.save_bbg_request(bloomberg_request, request_name, title)

        # Add to Redis queue
        # self.redis_connection.queue_request_to_sender(bloomberg_request)

        logger.info(
            f"Bloomberg request {request_id} submitted with identifier: {identifier}"
        )

        return bloomberg_request

    ## ************************************************

    def submit_command(self, cmd: str, priority=DEFAULT_CMD_PRIORITY):
        bloomberg_request = BloombergRequest(
            request_id=cmd,
            identifier="",
            request_name="TsyBondInfo",
            request_payload="",
            request_type=REQUEST_TYPE_CMD,
            priority=priority,
            max_retries=1,
        )
        self.redis_connection.queue_request_to_sender(bloomberg_request)
        return bloomberg_request.request_id

        
    def create_tsy_cusip_request(
        sender,
        cusips: list = None,
        fields: set[str] = None,
        request_name: str = "TsyBondInfo",
        title: str = "Tsy Bond Info Request",
        priority: int = DEFAULT_REQUEST_PRIORITY,
    ) -> BloombergRequest:
        """
        Convenience method to submit CUSIP-based requests

        Args:
            cusips: list of CUSIP identifiers
            fields: list of field mnemonics
            request_name: Name for the request
            title: Title for the request
            priority: Request priority

        Returns:
            the bloomberg request...
        """
        # Build universe from CUSIPs
        if not cusips:
            cusips = get_phase3_tsy_cusips()
            cusips = cusips[:1]  # lets only play with 1 now

        variable_request_list: set[str] = sender.data_def.get_request_col_name_list(request_name, 0)
        static_request_list: set[str] = sender.data_def.get_request_col_name_list(request_name, 1)

        if fields:
            variable_request_list = variable_request_list.intersection(fields)
            static_request_list = static_request_list.intersection(fields)
            # fields :list [str] = ["SECURITY_DES"]

        universe = {
            "@type": "Universe",
            "contains": [
                {"@type": "Identifier", "identifierType": "CUSIP", "identifierValue": cusip}
                for cusip in cusips
            ],
        }
        # Build field list
        field_list = {
            "@type": "DataFieldList",
            "contains": [{"mnemonic": field} for field in fields],
        }

        request: BloombergRequest = sender.create_data_request(
            request_name, title, universe, field_list, priority=priority
        )
        return request
    
    def create_mbs_cusip_request(
        sender,
        cusips: list = None,
        fields: list = None,
        request_name: str = "MBSBondInfo",
        title: str = "MBS Bond Info Request",
        priority: int = DEFAULT_REQUEST_PRIORITY,
    ) -> BloombergRequest:
        """
        Convenience method to submit CUSIP-based requests

        Args:
            cusips: list of CUSIP identifiers
            fields: list of field mnemonics
            request_name: Name for the request
            title: Title for the request
            priority: Request priority

        Returns:
            the bloomberg request...
        """
        # Build universe from CUSIPs
        if not cusips:
            cusips = get_phase3_mbs_cusips()
            cusips = cusips[:1]  # lets only play with 1 now


        if not fields:
            variable_request_list: list[str] = sender.data_def.get_request_col_name_list(request_name, 0)
            static_request_list: list[str] = sender.data_def.get_request_col_name_list(request_name, 1)
            fields: list[str] = sender.data_def.get_request_col_name_list(request_name, 1)
            # fields :list [str] = ["SECURITY_DES"]

        universe = {
            "@type": "Universe",
            "contains": [
                {"@type": "Identifier", "identifierType": "CUSIP", "identifierValue": cusip}
                for cusip in cusips
            ],
        }
        # Build field list
        field_list = {
            "@type": "DataFieldList",
            "contains": [{"mnemonic": field} for field in fields],
        }

        request: BloombergRequest = sender.create_data_request(
            request_name, title, universe, field_list, priority=priority
        )
        return request
    
    def create_futures_ticker_request(
        sender,
        tickers: list = None,
        fields: list = None,
        request_name: str = "FuturesInfo",
        title: str = "Futures Info Request",
        priority: int = DEFAULT_REQUEST_PRIORITY,
    ) -> BloombergRequest:
        """
        Convenience method to submit CUSIP-based requests

        Args:
            cusips: list of CUSIP identifiers
            fields: list of field mnemonics
            request_name: Name for the request
            title: Title for the request
            priority: Request priority

        Returns:
            the bloomberg request...
        """
        # Build universe from CUSIPs
        if not tickers:
            tickers = get_futures_tickers()
            tickers = tickers[:1]  # lets only play with 1 now

        if not fields:
            fields: list[str] = sender.data_def.get_request_col_name_list(request_name, 1)
        
        variable_request_list: list[str] = sender.data_def.get_request_col_name_list(request_name, 0)
        static_request_list: list[str] = sender.data_def.get_request_col_name_list(request_name, 1)
        
        #is it still CUSIP??? Need to check
        universe = {
            "@type": "Universe",
            "contains": [
                {"@type": "Identifier", "identifierType": "CUSIP", "identifierValue": ticker}
                for ticker in tickers
            ],
        }
        # Build field list
        field_list = {
            "@type": "DataFieldList",
            "contains": [{"mnemonic": field} for field in fields],
        }

        request: BloombergRequest = sender.create_data_request(
            request_name, title, universe, field_list, priority=priority
        )
        return request


def setup_logging():
    logger = ASL_Logging(log_file="bbg_request_sender.log", log_path="./logs", useBusinessDateRollHandler=True)


def main():
    setup_logging()

    sender = BloombergRequestSender()
    testing = True

    if testing:
       request_id = sender.submit_command(REQUEST_MBS_CUSIPS)
       print(f"Submitted Bloomberg request: {request_id}")
       # after score commadnds are ordered lexigraphically so... lets update cmd to +1
       request_id = sender.submit_command(EXIT_CMD, priority=DEFAULT_CMD_PRIORITY+1)

    sender.process_queued_requests()


# *****************************************************
#
#  MAIN MAIN
# *********************************************************
if __name__ == "__main__":
    main()
