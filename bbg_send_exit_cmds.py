
import logging
from ASL import ASL_Logging

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
    logger = ASL_Logging(log_file="bbg_cmd_sender_log", log_path="./logs", useBusinessDateRollHandler=True)

def main():
    setup_logging()
    redis_que = BloombergRedis()
    logger.info("Bloomberg CMD Sender Starting")

    #logger.info("Requesting TSY")
    #equest_id = redis_que.submit_command(REQUEST_TSY_CUSIPS)
    #print(f'treasury {request_id}')
    request_id = redis_que.submit_command(EXIT_CMD)
    redis_que.set_queue_name(BloombergRedis.POLLING_QUEUE)
    request_id = redis_que.submit_command(EXIT_CMD)
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
