import pandas as pd
import os
import pymongo
from pymongo import MongoClient
from dotenv import load_dotenv
from shining_pebbles import *
from aws_s3_controller import *

env_path = '../my-dotenv/.env'
load_dotenv(dotenv_path=env_path)

mongo_uri = os.getenv('MONGO_URI')
client = MongoClient(mongo_uri)
# db = client['database-rpa']

base_path = os.path.dirname(os.path.abspath(__file__))

def create_database(database_name):
    db = client[database_name]
    collection = db[f'hello-{database_name}']
    collection.insert_one({'text': f'hello-{database_name}!'})
    print(f'- Created database {database_name}.')
    return None

def get_collection_names_includes_something_in_database(database_name, something):
    db = client[database_name]
    collection_names = [collection_name for collection_name in db.list_collection_names() if something in collection_name]
    return collection_names

def get_index_collection_names(database_name='database-rpa'):
    collection_names = get_collection_names_includes_something_in_database(database_name, ' Index')
    return collection_names

def set_date_as_unique_index(collection_name):
    db = client['database-rpa']
    try:
        db[collection_name].create_index([('date', pymongo.ASCENDING)], unique=True)
    except Exception as e:
        print(e)
        return

def insert_data_to_database_collection(database_name, collection_name, data):
    db = client[database_name]
    collection = db[collection_name]

    bucket_insert_done = []
    bucket_insert_fail = []
    n = len(data)
    for i, datum in enumerate(data):
        print(f'- try insert {i+1}/{n} data in {database_name}.{collection_name} ...')
        try:
            collection.insert_one(datum)
            bucket_insert_done.append(datum)
            print(f'-- done: {datum}')
        except Exception as e:
            bucket_insert_fail.append(datum)
            print(f'-- fail: {datum}, error: {e}')

    return bucket_insert_done, bucket_insert_fail


def insert_index_prices_from_S3_to_mongodb(ticker_bbg):
    df = open_df_in_bucket_by_regex(bucket='dataset-bbg', bucket_prefix='dataset-index', regex=ticker_bbg)
    data = df.to_dict('records')
    done, fail = insert_data_to_database_collection(database_name='database-rpa', collection_name=f'timeseries-{ticker_bbg}', data=data)
    return done, fail

def insert_every_index_price_to_collections():
    collection_names_index = get_index_collection_names()
    tickers_bbg = [collection_name.replace('timeseries-', '') for collection_name in collection_names_index]
    for ticker_bbg in tickers_bbg:
        insert_index_price_from_S3_to_mongodb(ticker_bbg)
    return

def open_every_df_menu2160_in_s3(start_date, end_date, save_date):
    dct = {}
    file_keys = scan_files_in_bucket_by_regex(bucket='dataset-system', bucket_prefix=f'dataset-timeseries-menu2160-from{start_date}-to{end_date}-save{save_date}', regex='')
    for i, file_key in enumerate(file_keys):
        fund_code = pick_n_characters_followed_by_something_in_string(string=file_key, something='code', n=6)
        df_menu2160 = open_df_in_bucket(bucket='dataset-system', file_key=file_key)
        dct[fund_code] = df_menu2160
    return dct

def preprocess_every_dataset_menu2160_in_s3(start_date, end_date, save_date, ):
    dct = {}
    dct_dfs = open_every_df_menu2160_in_s3(start_date, end_date, save_date)
    for code, df in dct_dfs.items():
        df = preprocess_to_extract_timeseries_price_in_menu2160(df)
        dct[code] = df 
    return dct


def insert_every_timeseries_fund_to_database():
    dct = preprocess_every_dataset_menu2160_in_s3()
    n = len(dct.keys())
    for i, (fund_code, df) in enumerate(dct.items()):
        print(f'- ({i}/{n}) inserting timeseries to database: {fund_code}')
        data = df.to_dict(orient='records')
        insert_data_to_database_collection(database_name='database-rpa', collection_name=f'timeseries-{fund_code} Fund', data=data)



### DATAFRAME MAKER

def get_dct_timeseries(family='Fund'):
    dct = {}
    names = get_collection_names_includes_something_in_database('database-rpa', family)
    for name in names:
        key = name.replace('timeseries-', '')
        collection = db[name]
        data = list(collection.find({},{'_id': 0}))
        dct[key] = data
    return dct

def get_df_prices(family='Fund'):
    dct = get_dct_timeseries(family)
    bucket = []
    for k,v in dct.items():
        df = pd.DataFrame(v).set_index('date')
        df.columns = [k]
        bucket.append(df)
    df_prices = pd.concat(bucket, axis=1).sort_index(ascending=True, axis=0).sort_index(ascending=True, axis=1).fillna(value=0)
    df_prices['LIFEAM Fund'] = df_prices.sum(axis=1)
    return df_prices


### FUND PRICE TIMESERIES MAKER 
### REF: KB MOS 
# SP 또는 LAM 으로 이동할것
def preprocess_timeseries(df, time_col_from, value_col_from, time_col_to, value_col_to):
    print('-step 1: drop NaN')
    df = df[[time_col_from, value_col_from]].dropna()
    print('-step 2: rename columns to date and price')
    df = df.rename(columns={time_col_from: time_col_to, value_col_from: value_col_to})
    print('-step 3: reset index')
    df = df.reset_index(drop=True)
    df = df.copy()
    try:
        print('-step 4: convert value to float type')
        df[value_col_to] = df[value_col_to].str.replace(',', '').astype(float)
    except Exception as e:
        print(e)
    return df


def preprocess_to_extract_timeseries_price_in_menu2160(df_menu2160):
    df = preprocess_timeseries(df_menu2160, time_col_from='일자', value_col_from='수정\n기준가', time_col_to='date', value_col_to='price')
    return df

def preprocess_to_extract_timeseries_nav_in_menu2160(df_menu2160):
    df = preprocess_timeseries(df_menu2160, time_col_from='일자', value_col_from='순자산총액', time_col_to='date', value_col_to='nav')
    return df

def preprocess_timeseries_for_multicolumns(df, time_col_from, value_cols_from, time_col_to, value_cols_to):
    print('-step 1: drop NaN')
    df = df[[time_col_from, *value_cols_from]].dropna()
    print('-step 2: rename columns to date and price')
    df.columns = [time_col_to, *value_cols_to]
    print('-step 3: reset index')
    df = df.reset_index(drop=True)
    df = df.copy()
    try:
        print('-step 4: convert value to float type')
        for col in value_cols_to:
            df[col] = df[col].str.replace(',', '').astype(float)
    except Exception as e:
        print(e)
    return df

def preprocess_timeseries_of_menu2160_for_multicolumns(df_menu2160):
    df = preprocess_timeseries_for_multicolumns(df_menu2160, time_col_from='일자', value_cols_from=['수정\n기준가', '순자산총액'], time_col_to='date', value_cols_to=['price', 'nav'])
    return df