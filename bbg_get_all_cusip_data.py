
import logging
from uuid import UUID
from typing import Any
from ASL import ASL_Logging

from bbg_redis import BloombergRedis
from bbg_database import BloombergDatabase
from bloomberg_data_def import BloombergDataDef

from bbg_send_cmds import (
    EXIT_CMD,
    REQUEST_TSY_CUSIPS,
    REQUEST_FUT_CUSIPS,
    REQUEST_MBS_CUSIPS,
)

# Attach logging
logger = logging.getLogger(__name__)

def setup_logging():
    logger = ASL_Logging(log_file="bbg_get_all_cusips", log_path="./logs", useBusinessDateRollHandler=True)


def write_data(bbgdb : BloombergDatabase, request_def : dict[str, Any], request_status : dict[str, Any]):
    data_type, data_content  = bbgdb.get_data_content(request_status['request_id'])
    pass


def output_request(bbgdb : BloombergDatabase, request_definitions : dict[str, dict[str, any]], request_id):
    is_ready, request_status = bbgdb.is_request_ready(request_id)

    if is_ready:
        request_def : dict[str, Any] = request_definitions[request_status['name']]
        write_data(bbgdb, request_def, request_status)
        logger.info(f"its ready {request_id}")


def process_request_ids(request_ids : list[UUID]):
    bbgdb : BloombergDatabase = BloombergDatabase()
    bbgDataDef = BloombergDataDef(bbgdb)

    request_definitions : dict[str, dict[str, any]] = bbgdb.get_request_definitions()
    for request_id in request_ids:
        output_request(bbgdb, request_definitions, request_id)


def main():
    setup_logging()
    redis_que = BloombergRedis()
    
    logger.info("Bloomberg CMD Sender Starting")

    logger.info("Requesting TSY")
    request_ids = []
    # request_id = redis_que.submit_command(REQUEST_TSY_CUSIPS)
    request_id = "5ace4cc1-47cd-47fb-986c-4b14cbd13ad8"
    request_ids.append(request_id)
    print(f'treasury {request_id}')
    process_request_ids(request_ids)
#    logger.info("Requesting MBS")
#    request_id = redis_que.submit_command(REQUEST_MBS_CUSIPS)
#    print(f'mbs {request_id}')
#    logger.info("Requesting FUT")
#    request_id = redis_que.submit_command(REQUEST_FUT_CUSIPS)
#    print(f'fut {request_id}')
#    request_id = redis_que.submit_command(EXIT_CMD)
#    print(f'fut {request_id}')


# *****************************************************
#
#  MAIN MAIN
# *********************************************************
if __name__ == "__main__":
    main()
