import orjson
import time
import uuid
import logging
from logging.handlers import RotatingFileHandler

from typing import Dict, Any

from bbg_rest_connection import BloombergRestConnection
from bbg_database import BloombergDatabase
from bbg_redis import BloombergRedis, POLLING_QUEUE, REQUEST_QUEUE
from bbg_request import BloombergRequest

MAX_LOOPS = 5

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

        logger.info("Init done")

    def _handle_request_failure(
        self, request_data: BloombergRequest, error_message: str
    ):
        """Handle failed request with retry logic"""
        request_id = request_data.request_id
        retry_count = request_data.get("retry_count", 0)
        max_retries = request_data.get("max_retries", 3)

        if retry_count < max_retries:
            # Retry the request
            request_data["retry_count"] = retry_count + 1
            request_data["priority"] = (
                request_data.get("priority", 1) + 1
            )  # Lower priority for retries

            # Re-queue with delay
            time.sleep(2**retry_count)  # Exponential backoff

            logger.info(
                f"Request {request_id} queued for retry {retry_count + 1}/{max_retries}"
            )
        else:
            # Mark as failed
            self.db_connection.set_request_failed(request_id)
            self.db_connection.store_send_error_response(request_id, error_message)

            logger.error(
                f"Request {request_id} failed permanently after {max_retries} retries"
            )

    
    ## *******************************************************************

    def _process_single_request(self, request_data: BloombergRequest):
        """Process a single Bloomberg Data License request"""
        request_id = request_data.request_id
        identifier = request_data.identifier

        try:
            # Update status in database
            self.db_connection.set_request_status_processing(request_id, "processing")

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
                self._handle_request_failure(request_data, error_msg)

        except Exception as e:
            logger.error(f"Error processing request {request_id}: {e}")
            self._handle_request_failure(request_data, str(e))

    ## ***********************************************

    def process_request(self, request_data: BloombergRequest):
        """Move to processing set not sure these are needed..."""
        self._process_single_request(request_data)

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
        stop_processing = False

        while self._continue_processing(count, stop_processing):
            try:
                # Get highest priority request
                count += 1
                requests_data = self.redis_connection.get_sender_request()

                if not requests_data:
                    time.sleep(1)
                    continue
                elif requests_data == "exit":
                    stop_processing = True
                    continue

                request_json, priority = requests_data[0]
                request_data = orjson.loads(request_json)
                self.redis_connection.remove_sender_request(request_json)
                self.process_request(request_data)

            except Exception as e:
                logger.error(f"Error in request processing loop: {e}")
                time.sleep(5)

    ## puts a data request on a redis queue to be processed.
    ## I thin it is too convoluted.  the queue is probably not needed .
    ## lets get it to work then strip our.  Claude was confused as to what I meant.

    def submit_data_request(
        self,
        request_name: str,
        title: str,
        universe: Dict[str, Any],
        field_list: Dict[str, Any],
        output_format: str = "text/csv",
        priority: int = 1,
        max_retries: int = 3,
        queue_it: bool = False,
    ) -> str:
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
        identifier = f"{request_name}{str(uuid.uuid4())[0:6]}"

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
            identifier=identifier,
            request_payload=request_payload,
            priority=priority,
            max_retries=max_retries,
        )

        # Store in database
        self.db_connection.save_bbg_request(bloomberg_request, request_name, title)

        # Add to Redis queue
        if queue_it:
            self.redis_connection.queue_request_to_sender(bloomberg_request)
        else:
            self.process_request(bloomberg_request)

        logger.info(
            f"Bloomberg request {request_id} submitted with identifier: {identifier}"
        )
        return request_id

    ## ************************************************

    def submit_command(self, cmd: str):
        bloomberg_request = BloombergRequest(
            request_id=cmd, identifier="", request_payload="", priority=1, max_retries=1
        )
        self.redis_connection.queue_request_to_sender(bloomberg_request)


def submit_cusip_request(
    sender,
    cusips: list,
    fields: list,
    request_name: str = "CusipInfo",
    title: str = "CUSIP Data Request",
    priority: int = 1,
) -> str:
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
            {"@type": "Identifier", "identifierType": "CUSIP", "identifierValue": cusip}
            for cusip in cusips
        ],
    }

    # Build field list
    field_list = {
        "@type": "DataFieldList",
        "contains": [{"mnemonic": field} for field in fields],
    }

    return sender.submit_data_request(
        request_name, title, universe, field_list, priority=priority, queue_it=False
    )


def setup_logging():
    ## setup logging --
    ## add LOG_DIR var!
    FORMAT = "%(asctime)s:%(levelname)s:%(filename)s:%(lineno)d=> %(message)s"
    handler = RotatingFileHandler(
        "logs/bbg_request_sender.log", maxBytes=5000000, backupCount=3
    )
    logging.basicConfig(
        handlers=[handler],
        format=FORMAT,
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )
    handler.doRollover()  # start a new log each time.


def main():
    setup_logging()

    sender = BloombergRequestSender()

    # Example: Submit a CUSIP request
    # request_id = sender.submit_cusip_request(
    #     cusips=["91282CMV0", "91282CGS4"],
    #     fields=["SECURITY_DES", "MATURITY", "ISSUE_DT"],
    #     request_name="BondInfo",
    #     title="Get Bond Information",
    #     priority=1
    # )

    request_id = submit_cusip_request(
        sender,
        cusips=["91282CMV0"],
        fields=["SECURITY_DES"],
        request_name="TsyBondStatic",
        title="Get Tsy Bond Static",
        priority=1,
    )

    # request_id = sender.submit_command("exit")
    # sender.process_queued_requests()
    print(f"Submitted Bloomberg request: {request_id}")


# *****************************************************
#
#  MAIN MAIN
# *********************************************************
if __name__ == "__main__":
    main()
