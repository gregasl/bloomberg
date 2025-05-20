import pandas as pd
import datetime
import send_email
import os
import time
import logging

import comp_treasury_extract_dod

####################################
# Changing working directory
####################################
os.chdir('C:\\Users\\ASLRisk\\Desktop\\Request-Builder\\Treasury')

file_dir = '//aslfile01/ASLCAP/Quant Analysis/File_Check/Request_Builder/'
file_path = os.path.dirname(os.path.realpath(__file__)) + '/'
#DESTINATION = '//aslfile01/ASLCAP/Quant Analysis/Treasury Data(test)/'
DESTINATION = '//aslfile01/ASLCAP/Quant Analysis/Treasury Data/'
DESTINATION2 = '//aslfile01/ASLCAP/Quant Analysis/Treasury Static Data/'
DESTINATION_RISK01 = '//aslrisk01/ASLRisk/Desktop/Data/'
JapaneseRepo = '//aslfile01/ASLCAP/Quant Analysis/Japanese Repo/Price/'
TsyOTR = '//aslfile01/ASLCAP/Quant Analysis/Treasury_on_the_run/'
TODAY = datetime.date.today()
#file_path = 'C:/Users\Guocheng.xia\Desktop\Schedule-Job\Request-Builder\Treasury/'
BDAY = '//aslfile01/ASLCAP/Operations/TodayHoliday.txt'

def parsing():
    try:
        req = open(file_path+'treasury', 'r')
    except:
        send_email.email('CONNECTION ERROR - Treasury - Request Builder', 'Hi, \n\nThere is a connection error with ASLFIS01.')
    file = req.read()
    req.close()

    start_num = file.find('START-OF-DATA')
    start_num = start_num + len('START-OF-DATA') + 1
    end_num = file.find('END-OF-DATA')

    data = file[start_num:end_num-1]
    data = data.split('\n')
    lst = []
    for i in data:
        string = i.split('|')
        string = string[0].split('  ')[0:1] + string[3:-1]
        lst = lst + [string]

    start_num = file.find('START-OF-FIELDS')
    start_num = start_num + len('START-OF-FIELDS') + 1
    end_num = file.find('END-OF-FIELDS')
    fields = file[start_num:end_num-1]
    fields = fields.split('\n')
    fields = ['CUSIP_NUMBER'] + fields
    fields = list(filter(None, fields))

    df = pd.DataFrame(lst)
    try:
        df.columns = fields
    except:
        send_email.email('DATAFRAME ERROR - Treasury - Request Builder', 'Hi,\n\nThe program(parsing.py) cannot get field name from file "treasury".')
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    df = df[df['CUSIP_NUMBER'].str.strip() != '912796CP2'].copy() # temp! test!
    df = df.rename(columns={'PX_BID_EOD||8|':'PX_BID||8|','PX_DISCOUNT_DOLLAR_BID_EOD||8|':'PX_DISC_BID||8|'})
    
    numeric_fields_individual = [ 
        # Clean Numeric fields (request builder will often return values between -1 & 1 without leading zeroes (i.e. "-.042" or ".000")
        "AMT_OUTSTANDING", "AMOUNT_HELD_BY_CENTRAL_BANK", "DUR_ADJ_BID", "CNVX_BID", "YLD_CNV_BID", 
        "QUOTE_TYP", "PX_BID||8|", "PX_DISC_BID||8|", 
        #"IDX_RATIO", #"IDX_RATIO.1", 
        "PRINCIPAL_FACTOR", 
    ]
    kr_fields = [ 'KEY_RATE_DUR_' + f for f in ["3MO", "6MO", "1YR", "2YR", "3YR", "4YR", "5YR", "6YR", "7YR", "8YR", "9YR", "10YR", "15YR", "20YR", "25YR", "30YR"] ]
    oas_kr_fields = [ 'OAS_KEY_RATE_DUR_' + f for f in ["3M", "6M", "1YR", "2YR", "3YR", "5YR", "7YR", "10YR", "20YR", "30YR"] ]
    for _numeric_field in numeric_fields_individual + kr_fields + oas_kr_fields:
        df[_numeric_field] = pd.to_numeric(df[_numeric_field], errors='coerce')

    df1 = df[['CUSIP_NUMBER', 'PX_DISC_BID||8|', 'ID_ISIN', 'SECURITY_DES']]
    df1.columns = ['CUSIP', 'Pricing', 'ID_ISIN', 'SECURITY_DES']
    df1.to_csv(JapaneseRepo + str(TODAY) + '.csv', index=False)
    
    df2 = df.copy()

    # Special
    temp = TODAY
    while not os.path.isfile(TsyOTR + str(temp) + '.csv'):
        temp = temp - datetime.timedelta(days=1)
    otr = pd.read_csv(TsyOTR + str(temp) + '.csv')
    otr_tag = pd.read_excel('C:/Users/ASLRisk/Desktop/Data/on_the_run_tag.xlsx')
    otr['ID_CUSIP'] = otr['ID_CUSIP'].str.strip()
    otr['Name'] = otr['Name'].str.strip()
    otr_tag['FieldName'] = otr_tag['FieldName'].str.strip()

    otr = pd.merge(otr, otr_tag, left_on='Name', right_on='FieldName', how='inner')
    df2 = pd.merge(df2, otr, right_on='ID_CUSIP', left_on='CUSIP_NUMBER', how='left')

    lst = ['CT2', 'CT3', 'CT5', 'CT7', 'CT10', 'CT20', 'CT30']
    today = datetime.datetime.today()
    df2['ISSUE_DT'] = df2['ISSUE_DT'].apply(lambda x: datetime.datetime.strptime(x.strip(),'%m/%d/%Y'))
    for i in lst:
        temp1 = df2[df2['Name'] == i]
        for index, row in temp1.iterrows():
            if row['ISSUE_DT'] > today:
                df2.loc[df2[df2['Tenor'] == row['Tenor']].index, 'Special'] = df2[df2['Tenor'] == row['Tenor']][
                    'Description2']
            else:
                df2.loc[df2[df2['Tenor'] == row['Tenor']].index, 'Special'] = df2[df2['Tenor'] == row['Tenor']][
                    'Description1']
    #Spline Error Data - Added 2024 Oct
    df_spline_error = df2[['CUSIP_NUMBER', 'MATURITY', 'ISSUE_DT', 'BB_SPREAD_TO_SPLINE_CUBIC']]
    df_spline_error.to_csv('//aslfile01/ASLCAP/Quant Analysis/RV Spline project/data' + str(TODAY) + '.csv', index=False)

    
    df2 = df2[['CUSIP_NUMBER', 'SECURITY_DES', 'Special', 'MATURITY', 'ISSUE_DT', 'PX_DISC_BID||8|', 'PX_DISCOUNT_DOLLAR_ASK_EOD||8|', 'KEY_RATE_DUR_3MO', 'KEY_RATE_DUR_6MO', 'KEY_RATE_DUR_1YR', 'KEY_RATE_DUR_2YR', 'KEY_RATE_DUR_3YR', 'KEY_RATE_DUR_4YR', 'KEY_RATE_DUR_5YR', 'KEY_RATE_DUR_6YR', 'KEY_RATE_DUR_7YR', 'KEY_RATE_DUR_8YR', 'KEY_RATE_DUR_9YR', 'KEY_RATE_DUR_10YR', 'KEY_RATE_DUR_15YR', 'KEY_RATE_DUR_20YR', 'KEY_RATE_DUR_25YR', 'KEY_RATE_DUR_30YR']]
    
    
    
    
    def helper(x):
        temp = x.split()
        if temp[0] == 'TF':
            return 0
        elif len(temp) == 3:
            try:
                return float(temp[1])
            except:
                return 0
        elif len(temp) == 4:
            try:
                temp1 = temp[2].split('/')
                return float(temp[1]) + (float(temp1[0])/int(temp1[1]))
            except:
                return 0
        else:
            return 0

    df = df.drop(columns = 'PX_DISCOUNT_DOLLAR_ASK_EOD||8|')
    try:
        df.to_csv(DESTINATION+str(TODAY)+'.csv', index=False)
    except:
        send_email.email('WRITING ERROR - Treasury - Request Builder', 'Hi,\n\nThe program(parsing.py) cannot write' + str(TODAY) + '.csv to N Drive.')
    try:
        df.to_csv(DESTINATION_RISK01+'treasury.csv', index=False)
    except:
        send_email.email('WRITING ERROR - Treasury - Request Builder', 'Hi,\n\nThe program(parsing.py) cannot write treasury.csv to RISK01.')
    try:
        df2['COUPON'] = df2['SECURITY_DES'].apply(helper)
    except:
        send_email.email('WRITING ERROR - Treasury - Request Builder',
                         'Hi,\n\nThere is a coupon error in treasury static data.')
    df2.to_csv(DESTINATION2 + str(TODAY) + '.csv', index=False)
    
    ## compare against previous extract to make sure new data is pulled!
    comp_treasury_extract_dod.comp_treasury_extract_dod(DESTINATION)

def check():
	print(file_path+'treasury')
	return os.path.isfile(file_path+'treasury')

if __name__ == '__main__':
    if os.path.isfile(BDAY) == False:
        logging.basicConfig(filename=os.getcwd() + '\\error.log', level=logging.DEBUG,
                            format='%(asctime)s %(levelname)s %(name)s %(message)s')
        logger = logging.getLogger(__name__)
        try:
            i = 0
            while check() == False:
                time.sleep(60)
                i = i + 1
                print(i)
                if i > 60:
                    break
            if i <= 60:
                parsing()
                open(file_dir + 'parsing_treasury.txt', 'a').close()
                try:
                    os.remove(file_path + 'treasury')
                except:
                    send_email.email('DELETE ERROR - Treasury - Request Builder', 'Treasury-Delete Failed!')
            else:
                send_email.email('PARSING ERROR - Treasury - Request Builder', 'Treasury-No parsing data!')
        except Exception as err:
            print(err)
            logger.error(str(err) + '  [parsing.py]')


