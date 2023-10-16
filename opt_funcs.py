# -------------------------------------------
#            OPTIONAL FUNCTIONS
# -------------------------------------------
# creation date:      16/10/2023
# modification date:  16/10/2023
# -------------------------------------------

# ----------- importing libraries -----------
import ftplib
import csv
import os
from oauth2client.service_account import ServiceAccountCredentials
import gspread
# ----------- main info about df -----------
def review_dataframe(df):
    print(" DATA INFO ".center(125,'-'))
    print(df.info())
    print(" SHAPE OF DATASET ".center(125,'-'))
    print('Rows:{}'.format(df.shape[0]))
    print('Columns:{}'.format(df.shape[1]))
    print(" DATA TYPES ".center(125,'-'))
    print(df.dtypes)
    print(" STATISTICS OF DATA ".center(125,'-'))
    print(df.describe(include="all"))
    print(" MISSING VALUES ".center(125,'-'))
    print(df.isnull().sum()[df.isnull().sum()>0].sort_values(ascending = False))
    print(" DUPLICATED VALUES ".center(125,'-'))
    print(df.duplicated().sum())
# ----------- config info abou ftp -----------
def read_config_to_connect_ftp(config_file):
    with open(config_file, 'r') as file:
        content = file.readlines()
    config_dict = {}
    for line in content:
        key, value = line.strip().split(' = ')
        config_dict[key] = value
    # required fields
    required_fields = ['ftp_host', 'ftp_user', 'ftp_pass', 'ftp_file_path']
    # test: availability fields
    if all(field in config_dict for field in required_fields):
        return(config_dict)
    else:
        return None
# ----------- config info abou google -----------
def read_config_of_google(config_file):
    with open(config_file, 'r') as file:
        content = file.readlines()
    config_dict = {}
    for line in content:
        key, value = line.strip().split(' = ')
        config_dict[key] = value
    # required fields
    required_fields = ['google_sheet_url', 'scope']
    config_dict['scope'] = config_dict['scope'].strip().split(', ')
    # test: availability fields
    if all(field in config_dict for field in required_fields):
        return(config_dict)
    else:
        return None
# ----------- ftp connecter -----------
def connect_to_ftp(config_dict):
    ftp_host = config_dict['ftp_host']
    ftp_user = config_dict['ftp_user']
    ftp_pass = config_dict['ftp_pass']
    ftp_file_path = config_dict['ftp_file_path']
    # ftp hookup
    ftp = ftplib.FTP(ftp_host)
    ftp.login(ftp_user, ftp_pass)
    # loading temp file
    file_name = "local_file.csv"
    with open("local_file.csv", "wb") as local_file:
        ftp.retrbinary("RETR " + ftp_file_path, local_file.write)
    ftp.quit()
    return file_name
# ----------- string checker -----------
def str_reader_csv(name_file, code, separator):
    with open(name_file, 'r', encoding=code) as file:
        reader = csv.reader(file, delimiter=separator)
        first_row = next(reader)
        if not first_row:
            raise ValueError("Check columns!")
        num_fields = len(first_row)
        data = []
        for row in reader:
            row = row[:num_fields]
            data.append(row)
        if not data:
            raise ValueError("Check table!")
        return first_row, data
# ----------- json finder -----------
def path_to_json():
    current_directory = os.path.dirname(os.path.realpath(__file__))
    json_file = None
    for file in os.listdir(current_directory):
        if file.endswith(".json"):
            json_file = file
            break
    if json_file is not None:
        json_path = os.path.join(current_directory, json_file)
        return json_path
    else:
        raise ValueError("Check .json key file!")
# ----------- adder to google sheets -----------
def add_df_to_gsheet(config_dict, json_keyfile_path, df):
    google_sheet_url = config_dict['google_sheet_url']
    scope = config_dict['scope']
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_keyfile_path, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url(google_sheet_url)
    worksheet = sheet.get_worksheet(0)  
    worksheet.clear() 
    worksheet.update([df.columns.values.tolist()] + df.values.tolist())  
    # return Exception("Success update")