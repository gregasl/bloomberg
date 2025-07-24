## TBH this is a copy of the write reqest code...
## 
import pyodbc
import pandas as pd
#  import send_email
import os
import logging
from ASL.utils.asl_logging import ASL_Logging
from ASL.utils.asql import SQLObject # evenutally convert all sql to this...
# from ASL import ASL_Logging
import datetime
import ASL
import sys

####################################
# Changing working directory
####################################
#

logger = logging.getLogger(__name__)

BDAY = '//aslfile01/ASLCAP/Operations/TodayHoliday.txt'

####
# NOTE TAKEN RIGHT FROM THE OLD lookup
##
def wi() -> pd.DataFrame:
    today = datetime.datetime.today().date()
 
    connct = 'DRIVER={SQL Server};SERVER=SQLPROD;DATABASE=ASLRATESDB;UID=aslrisk;PWD=1Welcome2'
    sql = "SELECT * FROM dbo.Auction_Announce WHERE dbo.Auction_Announce.[Issue Date] >= '" + str(today) + "';"
    connection = pyodbc.connect(connct)
    df = pd.read_sql(sql, connection)
    connection.commit()
    df = df[df['Term'].str.contains('Year')]
    df['CUSIP'] = df['CUSIP'].str.strip()
    df = df.rename(columns={'CUSIP': 'CUSIP_NUMBER'})
    return df


def get_phase3_tsy_cusips() -> list[str]:
    try:
        #connection = pyodbc.connect("DSN=ASLFIS", autocommit=True)
        FISconnct = 'DRIVER={SQL Server};SERVER=ASLFISSQL;DATABASE=ASLFIS;UID=aslrisk;PWD=1Welcome2!'
        connection = pyodbc.connect(FISconnct, autocommit=True)
    except:
        ASL.send_email('CONNECTION ERROR - Treasury - Request Builder', 'Hi, \n\nThere is a connection error with ASLFISSQL.', email)
    sql = """
    SELECT 
        BND.CUSIP_NUMBER, 
        BND.MATURITY_DATE, 
        BND.ISSUE_DATE
    FROM 
        dbo.SECBND BND left join dbo.SECBAS BAS
		ON
		BND.CUSIP_NUMBER = BAS.CUSIP_NUMBER
		and BND.FIRM_NBR = '66' and BAS.FIRM_NBR = '66'
    WHERE
        BND.CUSIP_NUMBER LIKE '912%'
        AND BND.CUSIP_NUMBER NOT LIKE '9125%'
        AND BND.MATURITY_DATE > CAST(GETDATE() AS DATE)
		AND BND.FIRM_NBR = '66'
		AND (BAS.DESC3 IS NULL OR BAS.DESC3 != 'DO NOT USE' )
        AND (
            (BND.SEC_SUB_TYPE = '50' OR BND.BASE_RATE = 'V')
            AND (
				EXISTS (
					SELECT 1 
					FROM FPDMST F
					WHERE F.CUSIP_NUMBER = BND.CUSIP_NUMBER
				) OR
				EXISTS (
					SELECT 1 
					FROM INVMST I
					WHERE I.CUSIP_NUMBER = BND.CUSIP_NUMBER	AND I.TDYS_POSITION != 0
				)
			)
            OR (BND.SEC_SUB_TYPE != '50' AND BND.BASE_RATE != 'V')
        ) and substring(BND.CUSIP_NUMBER, 9, 1) like '[0-9]'
    """ ## 2023-09: Pull entire curve of regular Fixed Rate treasuries, only FRNs/TIPs that are found in FPDMST/INVMST
    #'''\
    # remove old WI(s) end in W and Repopens end in 'R' 
    #SELECT CUSIP_NUMBER, MATURITY_DATE
    #FROM dbo.SECBND;
    #''' # old!
    df = pd.read_sql(sql, connection)
    connection.commit()
    connection.close()
    today = pd.to_datetime('today')

    df = df[df['CUSIP_NUMBER'].str.startswith('912')]
    df = df[~df['CUSIP_NUMBER'].str.startswith('9125')]
    df = df[df['MATURITY_DATE'] > today]
    
    ## Drop additional "bad" CUSIPs that appear in Phase3 (WIs that are several years old (91282CDTW); completely mislabeled CUSIPs (912796CP2))
    df = df[ 
        ### 912796CP2 is mislabeled in Phase3 (was 7-day small value exercise in 2016... labeled as 2024-03-21 bill) 
        ### (https://treasurydirect.gov/instit/annceresult/press/preanre/2016/A_20160817_1.pdf)
        ~( (df['CUSIP_NUMBER'].str.strip()=='912796CP2') & (df['MATURITY_DATE'].dt.strftime('%Y-%m-%d')=='2024-03-21') )
    ]
    df = df[ 
        ### 9128203Q8 is mislabeled in Phase3 (is a TSY STRIP... but labeled as note) 
        ### (https://treasurydirect.gov/auctions/auction-query/?cusip=9128203Q8) ("No data to display")
        ~( (df['CUSIP_NUMBER'].str.strip()=='9128203Q8') & (df['MATURITY_DATE'].dt.strftime('%Y-%m-%d')=='2024-09-30') )
    ]
    df['ISSUE_DATE'] = pd.to_datetime(df['ISSUE_DATE'])
    df = df[
        ~(
            ### Old stale WI CUSIPs that remain in Phase3 (filter will remove WI/RI CUSIP if it is 180 days old (somewhat arbitrary high-upper-bound))
            ( df['CUSIP_NUMBER'].str.strip().str.endswith(('W', 'R')) )
            &
            ( (pd.to_datetime('today') - df['ISSUE_DATE']).dt.days > 180 )
        )
    ]
    
    df2 = wi()
    df = pd.concat([df, df2], ignore_index=True)
    df = df[['CUSIP_NUMBER']]
    df['REQUEST_TYPE'] = 'GOVT'
    df = df.reset_index(drop=True)
    df['CUSIP_NUMBER'] = df['CUSIP_NUMBER'].str.strip()
    df['CUSIP8'] = df['CUSIP_NUMBER'].apply(lambda x: x[:8])
    temp  = df['CUSIP_NUMBER'].apply(lambda x: (x[-1] == 'W') | (x[-1] == 'R'))
    df.loc[df[temp].index, 'temp'] = 0
    df.loc[df[~temp].index, 'temp'] = 1
    df = df.sort_values(by='temp', ascending=False)
    df = df.drop_duplicates('CUSIP8', keep='first')

    df = df.sort_values(by='CUSIP8', ascending=True)
    ret_list : list[str] = df['CUSIP_NUMBER']
    return ret_list


def get_phase3_mbs_cusips() -> list[str]:
  try:
        FISconnct = 'DRIVER={SQL Server};SERVER=ASLFISSQL;DATABASE=ASLFIS;UID=aslrisk;PWD=1Welcome2!'
        connection = pyodbc.connect(FISconnct, autocommit=True)
  except Exception as e:
        logger.error("Error connecting to DB {e}")
        return []
  
        # ASL.send_email.email('CONNECTION ERROR - Mortgage - Request Builder', 'Hi, \n\nThere is a connection error with ASLFIS01.')
  sql = '''\
    SELECT CUSIP_NUMBER, SEC_TYPE
    FROM dbo.FPDMST;
    '''
  df = pd.read_sql(sql, connection)
  df = df[df['SEC_TYPE'] == 'M'][['CUSIP_NUMBER']]
  df['CUSIP_NUMBER'] = df['CUSIP_NUMBER'].str.strip()
  df = df.drop_duplicates()
  ret_list : list[str] = df['CUSIP_NUMBER']
  return ret_list

def futures_contracts(ticker_bases : list[str]):
    '''
    Finding correct contract for the request upload
    :return: A pandas dataframe with correct contract name
    '''

    # Today's date
    today = datetime.datetime.today()
    # Read contract base
    res = pd.DataFrame(columns=['CONTRACT'])
    df = pd.DataFrame(ticker_bases, columns=['CONTRACT'])
    # Request different contracts in different time. We want to get next 3 active contract
    #
    # Contract ends with last number of the year Ex if it is 2021, the contract ends with 1
    # There are only 4 contracts each year:
    # Mar:  H
    # Jun:  M
    # Sep:  U
    # Dec:  Z
    cusip_months = {3, 'H', 6, 'M', 9, 'U', 12, 'Z'}

    if today.month < 4:
        lst = ['H', 'M', 'U']
        for i in lst:
            res = pd.concat([res, pd.DataFrame(df['CONTRACT'] + i + str(today.year)[-1])])
            res = res.reset_index(drop=True)
    elif today.month < 7:
        lst = ['M', 'U', 'Z']
        for i in lst:
            res = pd.concat([res, pd.DataFrame(df['CONTRACT'] + i + str(today.year)[-1])])
            res = res.reset_index(drop=True)
    elif today.month < 10:
        res = pd.concat([res, pd.DataFrame(df['CONTRACT'] + 'U' + str(today.year)[-1])])
        res = pd.concat([res, pd.DataFrame(df['CONTRACT'] + 'Z' + str(today.year)[-1])])
        res = pd.concat([res, pd.DataFrame(df['CONTRACT'] + 'H' + str(today.year + 1)[-1])])
        res = res.reset_index(drop=True)

    else:
        res = pd.concat([res, pd.DataFrame(df['CONTRACT'] + 'Z' + str(today.year)[-1])])
        res = pd.concat([res, pd.DataFrame(df['CONTRACT'] + 'H' + str(today.year + 1)[-1])])
        res = pd.concat([res, pd.DataFrame(df['CONTRACT'] + 'M' + str(today.year + 1)[-1])])
        res = res.reset_index(drop=True)
    return res

def get_futures_tickers() -> list[str]:
  try:
        ## username none defaults to microsoft cresds
        ticker_database = os.environ.get("BBG_DATABASE", "Bloomberg")
        server = os.environ.get("BBG_SQL_SERVER", "ASLDB03")
        port = os.environ.get("BBG_SQL_PORT", "1433") 
        ## crazy this does not use port?
        db = SQLObject(
            server=server, username=None, database=ticker_database
        )
        
  except Exception as e:
    logger.error("Error connecting to DB {e}")
  
  sql = """
    SELECT sec_id as ticker_base
    FROM dbo.bloomberg_sec_id where sec_id_type = 'FUT';
    """
  try:
        ticker_bases = []
        db_ticker_bases = db.fetch(sql, 'LIST')
        for ticker in db_ticker_bases:
            ticker_bases.append(ticker[0])

        db.close()
        res = futures_contracts(ticker_bases=ticker_bases)
        ret_list : list[str] = res['CONTRACT']
        return ret_list
  except Exception as e:
        logger.error(f"Unable to select cusips for futures request {e}")
        if db:
            db.close()

  return []

### left over from last one getting there.
### 
def write_csv_file(df : pd.DataFrame, file_name : str) -> None:    
    df = df.reset_index(drop=True)
    df.to_csv(file_name, index=False)


if __name__ == '__main__':
    if os.path.isfile(BDAY) == False:
        logging.basicConfig(filename=os.getcwd() + '\\error.log', level=logging.DEBUG,
                            format='%(asctime)s %(levelname)s %(name)s %(message)s')
        logger = logging.getLogger(__name__)
        try:
            cusips = get_phase3_tsy_cusips()
           #  open(file_dir + 'write_treasury_price.txt', 'a').close()
        except Exception as err:
            print(err)
            logger.error(str(err) + '  [write_req.py]')
            sys.exit(1)

