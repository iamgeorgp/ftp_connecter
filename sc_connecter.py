# -------------------------------------------
#    Script of Updating Data for Power BI 
# -------------------------------------------
# creation date:      16/10/2023
# modification date:  16/10/2023


# ----------- importing libraries -----------
import pandas as pd
import opt_funcs
import os
import time
import warnings
warnings.filterwarnings("ignore", category=UserWarning)
# ------------- configurations --------------
start_time = time.time()
name_configuration_ftp_file = 'config_ftp_file.txt'
name_configuration_google_file = 'config_google_file.txt'
# ------------- main connecter --------------
config_ftp_dic = opt_funcs.read_config_to_connect_ftp(
    config_file = name_configuration_ftp_file)
if config_ftp_dic != None:
    name_work_file_csv = opt_funcs.connect_to_ftp(
        config_dict = config_ftp_dic)
    first_row_columns, data = opt_funcs.str_reader_csv(
        name_file=name_work_file_csv, 
        code='windows-1251', 
        separator=';')
    os.remove('local_file.csv')
    df = pd.DataFrame(data, 
                      columns=first_row_columns)
    # opt_funcs.review_dataframe(df)
    json_keyfile_path=opt_funcs.path_to_json()
    config_google_dic = opt_funcs.read_config_of_google(
        config_file = name_configuration_google_file)
    if config_google_dic != None:
        opt_funcs.add_df_to_gsheet(config_dict = config_google_dic, 
                                   json_keyfile_path = json_keyfile_path,
                                   df = df)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Программа выполнилась за {execution_time} секунд.")
    else:
        raise ValueError("Check configuration file of Google data!")
else:
    raise ValueError("Check configuration file of FTP!")