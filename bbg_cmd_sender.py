
import logging
from ASL import ASL_Logging

from bbg_redis import BloombergRedis, POLLING_QUEUE, REQUEST_QUEUE
from bbg_request import BloombergRequest, DEFAULT_CMD_PRIORITY, DEFAULT_REQUEST_PRIORITY
from bbg_request import LAST_CMD_PRIORITY, REQUEST_TYPE_CMD, REQUEST_TYPE_BBG_REQUEST
from bloomberg_data_def import BloombergDataDef
from bbg_send_cmds import (
    REQUEST_TSY_CUSIPS,
    REQUEST_FUT_CUSIPS,
    REQUEST_MBS_CUSIPS,
)

# Attach logging
logger = logging.getLogger(__name__)

def setup_logging():
    logger = ASL_Logging(log_file="bbg_cmd_sender_log", log_path="./logs", useBusinessDateRollHandler=True)

def main():
    setup_logging()
    redis_que = BloombergRedis()
    logger.info("Bloomberg CMD Sender Starting")

    logger.info("Requesting TSY")
    request_id = redis_que.submit_command(REQUEST_TSY_CUSIPS)
    print(f'treasury {request_id}')
    logger.info("Requesting MBS")
    request_id = redis_que.submit_command(REQUEST_MBS_CUSIPS)
    print(f'mbs {request_id}')
    logger.info("Requesting FUT")
    request_id = redis_que.submit_command(REQUEST_FUT_CUSIPS)
    print(f'fut {request_id}')
    # after score commadnds are ordered lexigraphically so... lets update cmd to +1


# *****************************************************
#
#  MAIN MAIN
# *********************************************************
if __name__ == "__main__":
    main()
