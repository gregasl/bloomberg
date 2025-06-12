
from dataclasses import dataclass
from datetime import datetime
from bbg_database import BloombergDatabase

@dataclass
class BloombergDataColDef :
    request_name : str
    is_variable : bool
    suppress_sending : bool
    reply_col_name : str
    data_type : str
    output_col_name : str
    db_col_name : str         
