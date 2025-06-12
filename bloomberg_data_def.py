import logging
from datetime import datetime
from typing import Any
from bbg_database import BloombergDatabase

logger = logging.getLogger(__name__)

class BloombergDataDef :
    def __init__(
        self,
        bbg_database : BloombergDatabase
    ):
        self.bbg_database : BloombergDatabase = bbg_database
        self.data_defs : list[dict[str, Any]] = self.load_bbg_data_def()


    def load_bbg_data_def(self, ) -> list[dict[str, Any]]:
        try:
            query = """
    select request_name, is_variable_data, suppress_sending, request_col_name, reply_col_name, data_type,
    output_col_name, db_col_name 
        FROM bloomberg_data_def where suppress_sending = 0
                        """
            def_data = self.bbg_database.db_connection.fetch(query, 'DICT')
            return def_data
        except Exception as e:
            logger.error(f"Error updating request status: {e}")
            raise


    # def get_data_to_request(self, incl_static_data : bool) -> list[dict[str, Any]]:
    #     # we always need id.... and need to check logic any more complicated need a function
    #     return_list : list[dict[str, Any]] = list(filter(lambda x: \
    #                                 ((x.get("request_col_name")=="ID")or((x.get("suppress_sending", 0) == 0)and((incl_static_data)or(x.get("is_variable_data"))))), \
    #                                     self.data_defs))
    #     return return_list
    def get_data_to_request(self, incl_static_data : bool) -> list[dict[str, Any]]:
         # we always need id.... and need to check logic any more complicated need a function
         return_list : list[dict[str, Any]] = list(filter(lambda x: \
                                     (((x.get("suppress_sending", 0) == 0)and((incl_static_data)or(x.get("is_variable_data"))))), \
                                         self.data_defs))
         return return_list

    def get_request_col_name_list(self, incl_static_data) -> list[str]:
        return list(map(lambda x: x.get("request_col_name"), self.get_data_to_request(incl_static_data)))
        
