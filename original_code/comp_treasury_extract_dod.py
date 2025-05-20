import pandas as pd
from pandas.api.types import is_string_dtype
from pandas.api.types import is_numeric_dtype
import numpy as np
from glob import glob
from os.path import basename

from ASL import send_email as asl_send_email

def comp_df(df1, df2, key, name1, name2, ignore_fields=[], round_numeric=None):

    fields1 = list(df1.columns)
    fields2 = list(df2.columns)

    ### Check for mis-aligned fields between the dfs
    fields_missing_from_df1 = [ f for f in fields2 if f not in fields1 ]
    if len(fields_missing_from_df1) > 0:
        print(f"WARNING: {len(fields_missing_from_df1)} field{'s' if len(fields_missing_from_df1) > 1 else ''} missing from {name1}: {str(fields_missing_from_df1)}")
    fields_missing_from_df2 = [ f for f in fields2 if f not in fields2 ]
    if len(fields_missing_from_df2) > 0:
        print(f"WARNING: {len(fields_missing_from_df2)} field{'s' if len(fields_missing_from_df2) > 1 else ''} missing from {name2}: {str(fields_missing_from_df2)}")

    ### Prep DFs for comparison
    if len(ignore_fields) > 0:
        compare_fields = [ f for f in fields1 if f in fields2 and f not in key and f not in ignore_fields ]
    else:
        compare_fields = [ f for f in fields1 if f in fields2 and f not in key ]
    # print("Shared Fields to compare:",compare_fields)
    
    df1_comp = df1[[*key, *compare_fields]].rename(columns={
        f : name1 + "_" + f for f in compare_fields
    }).copy()
    df2_comp = df2[[*key, *compare_fields]].rename(columns={
        f : name2 + "_" + f for f in compare_fields
    }).copy()

    df1_comp['IN_'+name1] = True
    df2_comp['IN_'+name2] = True

    ### Merge comparison DFs
    mg_prep = pd.merge(
        df1_comp,
        df2_comp,
        how='outer',
        on=key
    )

    ### Prep column order list and overall matching record flag
    mg_prep['MATCH_FLAG_OVERALL'] = True
    col_order = key.copy()
    col_order.append('MATCH_FLAG_OVERALL')

    ### Create Flag for records found in BOTH DFs
    mg_prep['IN_'+name1].fillna(False, inplace=True)
    mg_prep['IN_'+name2].fillna(False, inplace=True)
    mg_prep['IN_BOTH'] = mg_prep['IN_'+name1] & mg_prep['IN_'+name2]
    mg_prep.loc[mg_prep['IN_BOTH']==False, 'MATCH_FLAG_OVERALL'] = False
    col_order.extend(['IN_'+name1, 'IN_'+name2, 'IN_BOTH'])

    mg = mg_prep.copy()

    ### Loop through shared fields to compare
    for comp_field in compare_fields:

        ### Initialize field that indicates if this particular column matches between the two DFs
        mg["MATCH_"+comp_field] = np.nan

        ### Initialize the column order for this sub-section
        col_order_subsection = [name1+"_"+comp_field, name2+"_"+comp_field]

        ### Compare the comp_field for each record -- assuming there are values in both df1 and df2
        if is_string_dtype( mg[name1+"_"+comp_field] ) and is_string_dtype( mg[name2+"_"+comp_field] ):
            mg.loc[
                (mg['IN_BOTH']==True),
                "MATCH_"+comp_field
            ] = mg[name1+"_"+comp_field].fillna('').str.strip() == mg[name2+"_"+comp_field].fillna('').str.strip()
            col_order_subsection.append("MATCH_"+comp_field)
        elif is_numeric_dtype( mg[name1+"_"+comp_field] ) and is_numeric_dtype( mg[name2+"_"+comp_field] ):
            mg.loc[
                (mg['IN_BOTH']==True),
                "DIFF_"+comp_field
            ] = mg[name1+"_"+comp_field].astype(float) - mg[name2+"_"+comp_field].astype(float)
            if round_numeric == None:
                mg.loc[
                    (mg['IN_BOTH']==True),
                    "MATCH_"+comp_field
                ] = mg[name1+"_"+comp_field].astype(float) == mg[name2+"_"+comp_field].astype(float)
            else:
                mg.loc[
                    (mg['IN_BOTH']==True),
                    "MATCH_"+comp_field
                ] = ( abs(mg["DIFF_"+comp_field]) <= (10 ** (round_numeric * -1) ) )
                #] = mg[name1+"_"+comp_field].astype(float).round(round_numeric) == mg[name2+"_"+comp_field].astype(float).round(round_numeric)
            mg.loc[
                (
                    (mg[name1+"_"+comp_field].isnull()) &
                    (mg[name2+"_"+comp_field].isnull()) &
                    (mg['IN_BOTH']==True)
                ),
                "MATCH_"+comp_field
            ] = True
            col_order_subsection.append("DIFF_"+comp_field)
            col_order_subsection.append("MATCH_"+comp_field)
        else:
            raise Exception(f"ERROR: unexpected dtype pair for {comp_field}: {mg[name1+'_'+comp_field].dtype} ({name1}), {mg[name2+'_'+comp_field].dtype} ({name2})")


        ### Set the match_flag to false where the comp_field does not match
        mg.loc[mg["MATCH_"+comp_field]==False, 'MATCH_FLAG_OVERALL'] = False

        ### Add the new columns to the col_order list
        col_order.extend(col_order_subsection)

    ### Format the compared DF
    mg = mg[col_order]

    return mg

def comp_treasury_extract_dod(filepath):

    extract_filenames = sorted(list(glob(f"{filepath}/*.csv")), reverse=True)

    t0_filepath = extract_filenames[0]
    t0_df = pd.read_csv(t0_filepath)
    t0_filedate = basename(t0_filepath).replace('.csv','')

    tm1_filepath = extract_filenames[1]
    tm1_df = pd.read_csv(tm1_filepath)
    tm1_filedate = basename(tm1_filepath).replace('.csv','')

    comp_tsy_dod = comp_df(
        df1=t0_df,
        name1="CURR",
        df2=tm1_df,
        name2="PREV",
        key=['CUSIP_NUMBER'],
        #ignore_fields = ['PRICING_SOURCE'],
        round_numeric=3
    )

    match_flags = [ f for f in comp_tsy_dod if f.startswith('MATCH_') ]
    summary_match_flags = comp_tsy_dod[match_flags].mean() * 100

    #print(comp_tsy_dod['MATCH_PX_BID||8|'].mean())
    #print(comp_tsy_dod['MATCH_PX_DISC_BID||8|'].mean())
    if comp_tsy_dod['MATCH_PX_BID||8|'].mean() > .50 or comp_tsy_dod['MATCH_PX_DISC_BID||8|'].mean() > .50:
    
        out_filename = f"compare_treasury_{t0_filedate}_vs_{tm1_filedate}.xlsx"
        with pd.ExcelWriter(f'./{out_filename}', engine='openpyxl') as writer:
            summary_match_flags.to_excel(writer, sheet_name='Summary_Pct_Match', index=True, header=['Match_Pct'])
            comp_tsy_dod.to_excel(writer, sheet_name='Compare_All', index=False)
    
        error_msg = f"WARNING: MATCHING PRICES BETWEEN {t0_filedate} AND {tm1_filedate} EXTRACTS.<br>Please review the Treasury Request Builder process. If today's data was not pulled correctly, it must be rerun."
        asl_send_email(
            subject='WARNING: Treasury Parsing (Request Builder)',
            body=error_msg,
            to=["tech@aslcap.com","market_risk@aslcap.com","michael.farren@aslcap.com"], 
            #to=email_list,
            # cc="<>@aslcap.com",
            # bcc=,
            attachments=f'./{out_filename}', 
            sender="aslriskuser@aslcap.com",
        )
        raise Exception(error_msg)
    

#if __name__ == "__main__":
#
#    DESTINATION = '//aslfile01/ASLCAP/Quant Analysis/Treasury Data(test)/'
#    comp_treasury_extract_dod(DESTINATION)