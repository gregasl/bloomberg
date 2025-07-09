import logging
from datetime import datetime
from typing import Any, Optional
from bbg_database import BloombergDatabase

logger = logging.getLogger(__name__)

class BloombergDataDef :
    DATA_TYPE_COL = "data_type"
    REQUEST_NAME_COL = "request_col_name"
    DATABASE_COL_NAME = "db_col_name"
    OUTPUT_COL_NAME = "output_col_name"
    DATA_TYPE_COL = "data_type"
    ID_COL = "IDENTIFIER"

    def __init__(
        self,
        bbg_database : BloombergDatabase
    ):
        self.bbg_database : BloombergDatabase = bbg_database
        ## assumes lists are sorted by col order in bbg_database (done locally)
        self.data_defs : dict[str, list[dict[str, Any]]] = bbg_database.load_bbg_column_defs()


    def get_columns(self, request_name : str) -> Optional[list[dict[str, Any]]]:
        return self.data_defs[request_name]

    def _get_data_to_request_from_list(self, request_name_lst : list[dict[str, Any]], incl_static_data : bool, include_id : bool = False) -> list[dict[str, Any]]:
        return_list : list[dict[str, Any]] = list(filter(lambda x: \
                            (((((x.get(BloombergDataDef.REQUEST_NAME_COL) != BloombergDataDef.ID_COL)or(not include_id))and
                                (x.get("suppress_sending", 0) == 0))and((incl_static_data)or(x.get("is_variable_data"))))), \
                                         request_name_lst))
        return return_list

    def get_data_to_request(self, request_name : str, incl_static_data : bool, include_id : bool = False) -> list[dict[str, Any]]:
         # 
         request_name_lst : list[dict[str, Any]] = self.data_defs[request_name]
         return self._get_data_to_request_from_list(request_name_lst, incl_static_data)
        
    def get_request_col_name_list(self, request_name : str, incl_static_data : bool, include_id : bool = False) -> list[str]:
        return list(map(lambda x: x.get(BloombergDataDef.REQUEST_NAME_COL), self.get_data_to_request(request_name, incl_static_data,
                                                                                                     include_id=include_id)))
    
    def get_db_col_name_list(self, request_name : str, incl_static_data : bool, include_id : bool = False) -> list[str]:
        return list(map(lambda x: x.get(BloombergDataDef.DATABASE_COL_NAME), self.get_data_to_request(request_name, incl_static_data,
                                                                                                      include_id=include_id)))
    
    def get_output_col_name_list(self, request_name : str, incl_static_data : bool, include_id : bool = False) -> list[str]:
        return list(map(lambda x: x.get(BloombergDataDef.OUTPUT_COL_NAME), self.get_data_to_request(request_name, incl_static_data,
                                                                                                    include_id=include_id)))
    
    def get_data_type_list(self, request_name : str, incl_static_data : bool, include_id : bool = False) -> list[str]:
        return list(map(lambda x: x.get(BloombergDataDef.DATA_TYPE_COL), self.get_data_to_request(request_name, incl_static_data,
                                                                                                   include_id=include_id)))
    
    
        
