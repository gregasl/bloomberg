
import logging
from uuid import UUID
from typing import Any
import csv
import io
from datetime import datetime

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

def _convert_csv_to_dict(in_data_content : str) -> list[dict[str, Any]]:
    csv_imitation_file = io.StringIO(in_data_content)
    reader = csv.DictReader(csv_imitation_file)
    return list(reader)

def _get_db_params(row : dict[str, Any], today_str : str, data_type_list : list[dict[str, Any]]) -> tuple[Any]:

    returnList = [today_str]
    for data_type_item in data_type_list:
        if data_type_item[BloombergDataDef.DATABASE_COL_NAME] == '':
            continue

        val = row[data_type_item[BloombergDataDef.REQUEST_NAME_COL]]
        data_type = data_type_item[BloombergDataDef.DATA_TYPE_COL]
        if (val == "true" or val == "false"):  
            val = 0 if (val == "false") else 1
        elif (data_type == "FLOAT"):
            if val == '':
                val = 0.0
            else:
                val = float(val)
        #elif (data_type == "DATE"):
        #    date_format = "%Y-%m-%d"  # Example format: Year-Month-Day
        #    val = datetime.strptime(val, date_format)
        elif (data_type == "BOOLEAN"):
            val = 0 if (val == "false") else 1

        returnList.append(val)

    return tuple(returnList)

def _get_output_fields(row : dict[str, Any], today_str : str, data_type_list : list[dict[str, Any]]) -> list[Any]:

    returnList = [today_str]
    for data_type_item in data_type_list:
        if data_type_item[BloombergDataDef.OUTPUT_COL_NAME] == '':
            continue

        val = row[data_type_item[BloombergDataDef.REQUEST_NAME_COL]]
        data_type = data_type_item[BloombergDataDef.DATA_TYPE_COL]
        # if (val == "true" or val == "false"):  
        #     val = 0 if (val == "false") else 1
        # elif (data_type == "FLOAT"):
        #     if val == '':
        #         val = 0.0
        #     else:
        #         val = float(val)
        # #elif (data_type == "DATE"):
        # #    date_format = "%Y-%m-%d"  # Example format: Year-Month-Day
        # #    val = datetime.strptime(val, date_format)
        # elif (data_type == "BOOLEAN"):
        #     val = 0 if (val == "false") else 1

        returnList.append(val)

    return returnList

def write_db_table(bbgdb, request_def : dict[str, Any], 
                   bbgDataDef : BloombergDataDef,  # columne
                   in_data_type, in_data_content) -> None:
    csv_data = _convert_csv_to_dict(in_data_content)
    table = request_def['save_table']
    insert_str = f"insert into {table} ("
    # these are sorted in the class so they should align.  We can do this in 1 go if too slow - get_data_to_request
    data_type_list : list[dict[str, Any]] = bbgDataDef.get_data_to_request(request_def['request_name'], incl_static_data=True, include_id=True)
    col_name_list = list(map(lambda x: x.get(BloombergDataDef.REQUEST_NAME_COL), data_type_list)) 
    db_col_name_list = ["business_date"]
    db_col_name_list.extend(list(map(lambda x: x.get(BloombergDataDef.DATABASE_COL_NAME), 
                                     filter(lambda col: col.get(BloombergDataDef.DATABASE_COL_NAME) != '',  data_type_list))))
    question_list = ["?"] * len(db_col_name_list)

    col_names = ",".join(db_col_name_list)
    questions = ",".join(question_list)

    insert_str = insert_str + col_names + ") VALUES (" + questions + ")"
    print(insert_str)
    today = datetime.today()
    today_str = today.strftime("%Y-%m-%d")

    for row in csv_data:
        params = _get_params(row, today_str, data_type_list)
       # params = tuple(map(lambda key: row.get(key, None), col_name_list))
        print(params)
        bbgdb.db_connection.execute_param_query(query=insert_str, params=params, commit=True)

def write_csv(request_def : dict[str, Any], 
             bbgDataDef : BloombergDataDef,  # columne
            in_data_type, in_data_content) -> None:
    csv_data = _convert_csv_to_dict(in_data_content)
    save_file = request_def['save_file']
    # these are sorted in the class so they should align.  We can do this in 1 go if too slow - get_data_to_request
    data_type_list : list[dict[str, Any]] = bbgDataDef.get_data_to_request(request_def['request_name'], incl_static_data=True, include_id=True)
    col_name_list = list(map(lambda x: x.get(BloombergDataDef.REQUEST_NAME_COL), data_type_list)) 
    file_col_name_list = ["BUSINESS_DATE"]
    file_col_name_list.extend(list(map(lambda x: x.get(BloombergDataDef.OUTPUT_COL_NAME), 
                                     filter(lambda col: col.get(BloombergDataDef.OUTPUT_COL_NAME) != '',  data_type_list))))
    today = datetime.today()
    today_str = today.strftime("%Y-%m-%d")

    with open(save_file, "w") as f:
        f.write(",".join(file_col_name_list)+"\n")
        for row in csv_data:
            out_cols = _get_output_fields(row, today_str, data_type_list)
            f.write(",".join(out_cols)+"\n")
       
       # params = tuple(map(lambda key: row.get(key, None), col_name_list))
def write_raw(request_def : dict[str, Any], 
             in_data_content) -> None:
    raw_file = request_def['raw_file']
    # these are sorted in the class so they should align.  We can do this in 1 go if too slow - get_data_to_request

    with open(raw_file, "w") as f:
        f.write(in_data_content)

def write_data(bbgdb : BloombergDatabase, request_def : dict[str, Any], 
               bbgDataDef : BloombergDataDef, request_status : dict[str, Any]) -> None:
    data_type, data_content  = bbgdb.get_data_content(request_status['request_id'])
    # write_db_table(bbgdb, request_def, bbgDataDef, data_type, data_content)
    write_csv(request_def, bbgDataDef, data_type, data_content)
    write_raw(request_def, data_content)


def output_request(bbgdb : BloombergDatabase,
                  request_definitions : dict[str, dict[str, any]],
                  bbgDataDef : BloombergDataDef, # column defs
                  request_id):
    is_ready, request_status = bbgdb.is_request_ready(request_id)

    if is_ready:
        request_def : dict[str, Any] = request_definitions[request_status['name']]
        write_data(bbgdb, request_def, bbgDataDef, request_status)
        logger.info(f"its ready {request_id}")


def process_request_ids(request_ids : list[UUID]):
    bbgdb : BloombergDatabase = BloombergDatabase()
    bbgDataDef = BloombergDataDef(bbgdb)

    request_definitions : dict[str, dict[str, any]] = bbgdb.get_request_definitions()
    for request_id in request_ids:
        output_request(bbgdb, request_definitions, bbgDataDef, request_id)


def main():
    setup_logging()
    redis_que = BloombergRedis()
    
    logger.info("Bloomberg CMD Sender Starting")

    logger.info("Requesting TSY")
    request_ids = []
    request_id = redis_que.submit_command(REQUEST_TSY_CUSIPS)
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
