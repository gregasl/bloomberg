
import pandas as pd
import get_environment
import get_cusip_list
import directories

def main():
    environment = get_environment.get_environment()
    dir_dict = directories.setup_directories(environment)

    df : pd.DataFrame = get_cusip_list.get_cusips(environment, dir_dict['email_user'])
    cusips_csv : str = dir_dict['work_dir'] + "\\cusips.csv"
    get_cusip_list.write_csv_file(df, cusips_csv)
    ### need to create the request list...
    # 1) pull requests from db... id static type vs variable.
    # how do I id the ones with static info and ones that need the whole thing.
    # ??? 
    (complete_re)
    # 2) start pushing.


    print(f'{environment}')



if __name__ == "__main__":
    main()
