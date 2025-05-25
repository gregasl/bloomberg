
import pandas as pd
#  import send_email
import os
import logging
import datetime
import ASL

####################################
# Setting up directories
####################################
#

def setup_directories(environment : str) -> dict[str, str]:
    dir_dict: dict[str, str] = {
        "email_user": "tech@aslcap.com",
        "BDAY_FILE": '//aslfile01/ASLCAP/Operations/TodayHoliday.txt',
        "file_dir": '//aslfile01/ASLCAP/Quant Analysis/File_Check/Request_Builder/',
        "work_dir": 'C:\\Users\\ASLRisk\\Desktop\\Request-Builder\\Treasury'
    }

    if (environment == 'local'):
        dir_dict['email_user'] = 'greg.mahoney@aslcap.com'
        dir_dict['file_dir'] = 'C:\\Users\\greg.mahoney\\Quant Analysis\\File_Check\\Request_Builder\\'
        dir_dict['work_dir'] = 'C:\\Users\\greg.mahoney\\Request-Builder\\Treasury'

    return dir_dict
