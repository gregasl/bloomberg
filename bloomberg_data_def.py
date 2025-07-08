import logging
from datetime import datetime
from typing import Any, Optional
from bbg_database import BloombergDatabase

logger = logging.getLogger(__name__)

class BloombergDataDef :
    def __init__(
        self,
        bbg_database : BloombergDatabase
    ):
        self.bbg_database : BloombergDatabase = bbg_database
        ## assumes lists are sorted by col order in bbg_database (done locally)
        self.data_defs : dict[str, list[dict[str, Any]]] = bbg_database.load_bbg_column_defs()


    def get_columns(self, request_name : str) -> Optional[list[dict[str, Any]]]:
        return self.data_defs[request_name]

    def _get_data_to_request_from_list(self, request_name_lst : list[dict[str, Any]], incl_static_data : bool) -> list[dict[str, Any]]:
        return_list : list[dict[str, Any]] = list(filter(lambda x: \
                            ((((x.get("request_col_name") != "ID")and
                                (x.get("suppress_sending", 0) == 0))and((incl_static_data)or(x.get("is_variable_data"))))), \
                                         request_name_lst))
        return return_list

    def get_data_to_request(self, request_name : str, incl_static_data : bool) -> list[dict[str, Any]]:
         # 
         request_name_lst : list[dict[str, Any]] = self.data_defs[request_name]
         return self._get_data_to_request_from_list(request_name_lst, incl_static_data)
        
    def get_request_col_name_list(self, request_name : str, incl_static_data) -> list[str]:
        return list(map(lambda x: x.get("request_col_name"), self.get_data_to_request(request_name, incl_static_data)))
        
