import json
import os
import pandas as pd
import mysql.connector
import csv
import pprint

from data import add_data_to_sql, query_sql, create_table_query


import warnings
warnings.filterwarnings('ignore')

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

#getting current directory :
directory = os.path.dirname(os.path.abspath(__file__))
subfolder_paths = [
    '/data/aggregated/transaction/country/india'
    ]

subfolder_path = ''
for subfolder_path in subfolder_paths:
    script_dir = directory + subfolder_path

def read_json_in_subfolder(root_directory):
    data_list = []
    subfolders = [folder for folder in os.listdir(root_directory) if os.path.isdir(os.path.join(root_directory, folder))]
    for subfolder in subfolders:
        subfolder_path = os.path.join(root_directory, subfolder)
        for dir_path, dir_name, file_names in os.walk(subfolder_path):
            for file_name in file_names:
                if file_name.endswith('.json'):
                    json_path = os.path.join(dir_path, file_name)
                    with open(json_path, 'r') as file:
                        data = json.load(file)
                        data['file_path'] = json_path
                        data_list.append(data)

    return data_list

#specify the target folder:
target_folder = '2017'
json_file_name = '1.json'
br = 0
combined_df = pd.DataFrame()
sheet_name = 'sheet_0'

def get_data(script_dir):
    df = pd.DataFrame(columns=['name', 'type', 'count', 'amount', 'file_name', 'year', 'state', 'country', 'from1', 'to1'])
    data_list = []
    for i in range(1, 2):
        data_list = read_json_in_subfolder(script_dir)


    for i,data in enumerate(data_list, start=1):
        json_data = pprint.pformat(data)

        file_path = data['file_path']
        file_path = file_path.split('/')
        file_name = file_path[-1]
        year = file_path[-2]
        state = file_path[-3]
        country = file_path[-4]
        if state == "india":
            country = "india"

        data_extract = data['data']
        from_data = data_extract['from']
        to_data = data_extract['to']
        transaction_extracts = data_extract['transactionData']

        for transaction_extract in transaction_extracts:
            name_data = transaction_extract['name']
            payment_datas = transaction_extract['paymentInstruments']

            for payment_data in payment_datas:
                type_data = payment_data['type']
                count_data = payment_data['count']
                amount_data = payment_data['amount']
                amount_data = float(amount_data)
                amount_data = round(amount_data,2)

                df_values = {
                    'name': name_data,
                    'type': type_data,
                    'count': count_data,
                    'amount': amount_data,
                    'file_name': file_name,
                    'year': year,
                    'state': state,
                    'country': country,
                    'from1': from_data,
                    'to1': to_data
                }
                df = df.append(df_values, ignore_index=True)

    df.to_excel('total_data.xlsx')
    return df

def create_db_connection():
    db_params = {
        'host': 'localhost',
        'user': 'root',
        'password': 'new_password'
    }
    # 'password': '@Ak_mb#89621997'
    #establish connection
    conn = mysql.connector.connect(**db_params)

    #create a cursor
    cursor = conn.cursor()
    print('MYSQL server connection successful')

    try:
        cursor.execute('USE data_visual_proj')
    except:
        cursor.execute('CREATE DATABASE data_visual_proj')
        cursor.execute('USE data_visual_proj')
        print('data base created')
    cursor.execute("DROP TABLE visualise_data")
    try:
        cursor.execute(create_table_query)
        print('table created')
    except Exception as e:
        print(e)


    return cursor, conn


final_data = get_data(script_dir)
cursor, conn = create_db_connection()

add_data_to_sql(final_data, conn, cursor)
query_sql(cursor, conn)

