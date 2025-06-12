## TBH this is a copy of the write reqest code...
## 
import pyodbc
import pandas as pd
#  import send_email
import os
import logging
import datetime
import ASL
import sys

####################################
# Changing working directory
####################################
#
BDAY = '//aslfile01/ASLCAP/Operations/TodayHoliday.txt'

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

### left over from last one getting there.
### 
def write_csv_file(df : pd.DataFrame, file_name : str) -> None:    
    df = df.reset_index(drop=True)
    df.to_csv(file_name, index=False)

    # try:
    #     req = open(path + '/treasury.req', 'r')
    # except:
    #     ASL.send_email('FILE-READING ERROR - Treasury - Request Builder', 'Hi,\n\nThe program(write_req.py) cannot open file treasury.req in aslrisk01.', )
    # file = req.read()
    # req.close()
    # start_num = file.find('START-OF-DATA')
    # start_num = start_num + len('START-OF-DATA') + 1
    # end_num = file.find('END-OF-DATA')
    # start_str = file[:start_num]
    # end_str = file[end_num:]

    # string = ''
    # for index, row in df.iterrows():
    #     string = string + str(row['CUSIP_NUMBER'].strip()) + '\n'
    # lst = wi()
    # if len(lst) > 0:
    #     for i in lst:
    #         if i not in df['CUSIP_NUMBER'].str.strip().to_list():
    #             string = string + str(i) + '\n'
    # string = start_str + string + end_str
    # try:
    #     file = open(path+'/treasury.req', 'w')
    #     file.write(string)
    # except:
    #     ASL.send_email('FILE-WRITING ERROR - Request Builder','Hi,\n\nThe program(write_req.py) cannot write file treasury.req to aslrisk01.', "tech@aslcap.com")
    # file.close()


if __name__ == '__main__':
    if os.path.isfile(BDAY) == False:
        logging.basicConfig(filename=os.getcwd() + '\\error.log', level=logging.DEBUG,
                            format='%(asctime)s %(levelname)s %(name)s %(message)s')
        logger = logging.getLogger(__name__)
        try:
            cusips = get_phase3_tsy_cusips(environment="", email="greg.mahoney@aslcap.com")
           #  open(file_dir + 'write_treasury_price.txt', 'a').close()
        except Exception as err:
            print(err)
            logger.error(str(err) + '  [write_req.py]')
            sys.exit(1)

