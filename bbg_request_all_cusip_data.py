import logging

# from ASL import ASL_Logging
from ASL.utils.asl_logging import ASL_Logging

from bbg_redis import BloombergRedis

from bbg_send_cmds import (
    EXIT_CMD,
    REQUEST_TSY_CUSIPS,
    REQUEST_FUT_CUSIPS,
    REQUEST_MBS_CUSIPS,
)

# Attach logging
logger = logging.getLogger(__name__)

def setup_logging():
    # logger = ASL_Logging(log_file="bbg_request_all_cusips", log_path="./output", use_log_header=True, use_stream_output=True, useBusinessDateRollHandler=True)
    logger = ASL_Logging(log_file="stdout", log_path="", use_log_header=True)

def main():
    setup_logging()
    redis_que = BloombergRedis()
    
    logger.info("Bloomberg Cusip Request Starting")
    logger.info("Clearing quueue..")
    redis_que.clear_queue()
    logger.info("Requesting TSY")
    request_ids = []
    tsy_request_id = redis_que.submit_command(REQUEST_TSY_CUSIPS)
    # request_id = "5ace4cc1-47cd-47fb-986c-4b14cbd13ad8"
    request_ids.append(tsy_request_id)
    print(f'treasury {tsy_request_id}')
    # logger.info("Requesting MBS")
    # mbs_request_id = redis_que.submit_command(REQUEST_MBS_CUSIPS)
    # request_ids.append(mbs_request_id)
    # print(f'mbs {mbs_request_id}')
    # logger.info("Requesting FUT")
    # fut_request_id = redis_que.submit_command(REQUEST_FUT_CUSIPS)
    # request_ids.append(fut_request_id)
    # print(f'fut {fut_request_id}')
   # process_request_ids(request_ids)
    logger.info("Sending exit...")
    request_id = redis_que.submit_command(EXIT_CMD)

# *****************************************************
#
#  MAIN MAIN
# *********************************************************
if __name__ == "__main__":
    main()
