import logging
from uuid import UUID
from typing import Any
import csv
import io
import re
from datetime import datetime

try:
    from ASL.utils.asl_logging import ASL_Logging
except ImportError:
    try:
        from ASL import ASL_logging
    except ImportError:
        ASL_Logging = None
#from ASL import ASL_Logging

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


class BloombergOutputter:
    DateFmt = "%Y-%m-%d"
    TimeFmt = "%H%M%S"
    DateRegex = re.compile(r'%D%')
    TimeRegex = re.compile(r'%T%')
    FuturesCleaner = re.compile(r' Govt| Comdty')

    
    def __init__(
            self,
            bbgdb : BloombergDatabase
    ):
        self.bbgdb = bbgdb
       
        self.bbgDataDef = BloombergDataDef(bbgdb)
        self.requestDefinitions: dict[str, dict[str, any]] = self.bbgdb.get_request_definitions()

    
    ## translate date and time... regex
    @staticmethod
    def expand_file_name(infilename : str) -> str:
        today = datetime.now()
        todayStr = today.strftime(BloombergOutputter.DateFmt)
        nowTimeStr = today.strftime(BloombergOutputter.TimeFmt)

        returnStr : str = re.sub(BloombergOutputter.DateRegex, todayStr, infilename)
        returnStr = re.sub(BloombergOutputter.TimeRegex, nowTimeStr, returnStr)

        return returnStr
    
    def _convert_csv_to_dict(self, in_data_content: str) -> list[dict[str, Any]]:
        csv_imitation_file = io.StringIO(in_data_content)
        reader = csv.DictReader(csv_imitation_file)
        return list(reader)

    def _futures_cleaner(self, val : str) -> str:
        return re.sub(BloombergOutputter.FuturesCleaner, '', val)

    def _clean(self, request_name : str, val : str):
        if (request_name == 'FuturesInfo'):
            return(self._futures_cleaner(val))
        else:
            return(val)

    def _get_db_params(self, request_name : str,
        row: dict[str, Any], today_str: str, data_type_list: list[dict[str, Any]]
    ) -> tuple[Any]:

        returnList = [today_str]
        for data_type_item in data_type_list:
            if data_type_item[BloombergDataDef.DATABASE_COL_NAME] == "":
                continue

            try:
                val = row[data_type_item[BloombergDataDef.REPLY_COL_NAME]]
                data_type = data_type_item[BloombergDataDef.DATA_TYPE_COL]
                if val == "true" or val == "false":
                     val = 0 if (val == "false") else 1
                elif data_type == "FLOAT":
                    if val == "":
                        val = 0.0
                    else:
                        val = float(val)
                # elif (data_type == "DATE"):
                #    date_format = "%Y-%m-%d"  # Example format: Year-Month-Day
                #    val = datetime.strptime(val, date_format)
                elif data_type == "BOOLEAN":
                    val = 0 if (val == "false") else 1
            except Exception as e:
                logging.error("db col error {e}")

            val = self._clean(request_name, val)
            returnList.append(val)

        return tuple(returnList)


    def _get_output_fields(self, request_name: str,
        row: dict[str, Any], today_str: str, data_type_list: list[dict[str, Any]]
    ) -> list[Any]:

        returnList = [today_str]
        for data_type_item in data_type_list:
            if data_type_item[BloombergDataDef.OUTPUT_COL_NAME] == "":
                continue

            try:
                colName = data_type_item[BloombergDataDef.REPLY_COL_NAME]
                val = self._clean(request_name, row[colName]) # we will just let it throw...

                returnList.append(val)
            except Exception as e:
                logger.error(f"Error getting {colName} does it exist?")


        return returnList


    def write_db_table(
        self,
        request_status : dict[str, Any],
        request_def: dict[str, Any],
        in_data_type : str,
        in_data_content,
        delete_today : bool = True
    ) -> None:
        today = datetime.now()
        todayStr = today.strftime(BloombergOutputter.DateFmt)
        csv_data = self._convert_csv_to_dict(in_data_content)
        table = request_def["save_table"]
        insert_str = f"insert into {table} ("
        delete_str = f"delete from {table} where business_date >= '{todayStr}'"

        # these are sorted in the class so they should align.  We can do this in 1 go if too slow - get_data_to_request
        data_type_list: list[dict[str, Any]] = self.bbgDataDef.get_data_to_request(
            request_def["request_name"], incl_static_data=True, include_id=True
        )
        col_name_list = list(
            map(lambda x: x.get(BloombergDataDef.REPLY_COL_NAME), data_type_list)
        )
        db_col_name_list = ["business_date"]
        db_col_name_list.extend(
            list(
                map(
                    lambda x: x.get(BloombergDataDef.DATABASE_COL_NAME),
                    filter(
                        lambda col: col.get(BloombergDataDef.DATABASE_COL_NAME) != "",
                        data_type_list,
                    ),
                )
            )
        )
        question_list = ["?"] * len(db_col_name_list)

        col_names = ",".join(db_col_name_list)
        questions = ",".join(question_list)

        if (delete_today):
             logger.info(delete_str)
             self.bbgdb.db_connection.execute_query(
                    query=delete_str, commit=True
                )
             
        insert_str = insert_str + col_names + ") VALUES (" + questions + ")"
        logger.info(insert_str)  # debug???

        try:
            for row in csv_data:
                params = self._get_db_params(request_status['name'], row, todayStr, data_type_list)
                logger.info(params)
                self.bbgdb.db_connection.execute_param_query(
                    query=insert_str, params=params, commit=True
                )
        
            try:
                self.bbgdb.update_process_status(request_status['request_id'], request_status['identifier'],
                                                 request_status['name'], 'database', 'processed')
            except Exception as save_e:
                logger.error(f"Error updating process status DB {save_e}")
                raise

        except OSError as oserror:
            logger.error(f"Error saving data to DB.. {oserror}") # what if these fail hmmm
            self.bbgdb.update_process_status(request_status['request_id'], request_status['identifier'],
                                         request_status['name'], 'database', 'error', f'{oserror}')
            raise
        except Exception as e:
            logger.error(f"Error saving data to DB.. {e}")
            self.bbgdb.update_process_status(request_status['request_id'], request_status['identifier'],
                                         request_status['name'], 'database', 'error', f'{e}')
            raise
            

    def write_csv(
            self,
            request_status,
        request_def: dict[str, Any],
        in_data_type,
        in_data_content,
    ) -> None:
        csv_data = self._convert_csv_to_dict(in_data_content)
        save_file = BloombergOutputter.expand_file_name(request_def["save_file"])

        # these are sorted in the class so they should align.  We can do this in 1 go if too slow - get_data_to_request
        data_type_list: list[dict[str, Any]] = self.bbgDataDef.get_data_to_request(
            request_def["request_name"], incl_static_data=True, include_id=True
        )
        col_name_list = list(
            map(lambda x: x.get(BloombergDataDef.REPLY_COL_NAME), data_type_list)
        )
        file_col_name_list = ["BUSINESS_DATE"]
        file_col_name_list.extend(
            list(
                map(
                    lambda x: x.get(BloombergDataDef.OUTPUT_COL_NAME),
                    filter(
                        lambda col: col.get(BloombergDataDef.OUTPUT_COL_NAME) != "",
                        data_type_list,
                    ),
                )
            )
        )
        today = datetime.today()
        today_str = today.strftime(BloombergOutputter.DateFmt)
        logger.info(f"CSV MODE - writing to {save_file}")
        try:
            with open(save_file, "w+") as f:
                f.write(",".join(file_col_name_list) + "\n")
                for row in csv_data:
                    out_cols = self._get_output_fields(request_status['name'], row, today_str, data_type_list)
                    f.write(",".join(out_cols) + "\n")
        
            try:
                self.bbgdb.update_process_status(request_status['request_id'], request_status['identifier'],
                                                 request_status['name'], 'csv', 'processed')
            except Exception as save_e:
                logger.error(f"Error updating process status CSV {save_e}")
                raise

        except OSError as oserror:
            logger.error(f"Error saving data to CSV.. {oserror}") # what if these fail hmmm
            self.bbgdb.update_process_status(request_status['request_id'], request_status['identifier'],
                                         request_status['name'], 'csv', 'error', f'{oserror}')
            raise
        except Exception as e:
            logger.error(f"Error saving data to CSV.. {e}") # what if these fail hmmm
            self.bbgdb.update_process_status(request_status['request_id'], request_status['identifier'],
                                         request_status['name'], 'csv', 'error', f'{e}')
            raise
        


    def write_raw(self, request_status : dict[str, Any], request_def: dict[str, Any], in_data_content) -> None:
        raw_file = BloombergOutputter.expand_file_name(request_def["raw_file"])
        # these are sorted in the class so they should align.  We can do this in 1 go if too slow - get_data_to_request
        logger.info(f"RAW MODE - writing to {raw_file}")
        try:
            with open(raw_file, "w+") as f:
                f.write(in_data_content)

            try:
               self.bbgdb.update_process_status(request_status['request_id'], request_status['identifier'],
                                                request_status['name'], 'raw', 'processed')
            except Exception as save_e:
                logger.error(f"Error updating process status raw {save_e}")
                raise                            
            
        except OSError as oserror:
            logger.error(f"Error saving data to RAW.. {oserror}") # what if these fail hmmm
            self.bbgdb.update_process_status(request_status['request_id'], request_status['identifier'],
                                         request_status['name'], 'raw', 'error', f'{oserror}')
            raise
        except Exception as e:
            logger.error(f"Error saving data to RAW.. {oserror}") # what if these fail hmmm
            self.bbgdb.update_process_status(request_status['request_id'], request_status['identifier'],
                                         request_status['name'], 'raw', 'error', f'{e}')
            raise
        #

    def write_data(
            self,
            request_status: dict[str, Any],
            request_def: dict[str, Any],
            output_type : SyntaxWarning
    ) -> None:
        
        data_type, data_content = self.bbgdb.get_data_content(request_status['request_id'])
        try:
            if (output_type == 'database'):
                self.write_db_table(request_status, request_def, data_type, data_content)
            elif (output_type == 'csv'):
                self.write_csv(request_status, request_def, data_type, data_content)
            elif (output_type == 'raw'):
                self.write_raw(request_status, request_def, data_content)
            else:
                logger.error(f"What output type {output_type}")

        except Exception as e:
            logging.error(f"Unable to process data {e}")
            raise

    def output_request(
            self,
            request_id : str,
            output_type : str
    ):
        is_ready, request_status = self.bbgdb.is_request_ready(request_id)

        if is_ready:
            request_def: dict[str, Any] = self.requestDefinitions[request_status["name"]]
            self.write_data(request_status, request_def, output_type)
            logger.info(f"its ready {request_id} for {output_type}")


    def output_ready_requests(self):
        data_rows = self.bbgdb.get_requests_ready_to_output()

        for data_row in data_rows:
            self.output_request(data_row['request_id'], data_row['output_type'])



def setup_logging():
    logger = ASL_Logging(
        log_file="bbg_get_all_cusips",
        log_path="./output",
        log_level_threshold=logging.INFO,
        use_log_header=True,
        use_stream_output=True,
        useBusinessDateRollHandler=True,
    )


def main():
    setup_logging()
    bbgdb = BloombergDatabase()
    x1 = "hello"
    x2 = "hello,goodbyy"

    bbgOutputter = BloombergOutputter(
        bbgdb=bbgdb
    )

    logger.info("Bloomberg Outputter startimg")
    bbgOutputter.output_ready_requests()



# *****************************************************
#
#  MAIN MAIN
# *********************************************************
if __name__ == "__main__":
    main()
