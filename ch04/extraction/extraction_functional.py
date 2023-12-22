# Import modules
import sqlite3
import requests
import pandas as pd
import logging
import os


logger = logging.getLogger(__name__)


# extract the data from the parquet file
def source_data_from_parquet(parquet_file_name):
    try:
        df_parquet = pd.read_parquet(parquet_file_name)
        logger.info(f'{parquet_file_name} : extracted {df_parquet.shape[0]} records from the parquet file.')
    except Exception as e:
        logger.exception(f'{parquet_file_name} : - exception {e} encountered while extracting the parquet file.')
        df_parquet = pd.DataFrame()
    return df_parquet


# extract the data from the csv file
def source_data_from_csv(csv_file_name):
    try:
        df_csv = pd.read_csv(csv_file_name)
        logger.info(f'{csv_file_name} : extracted {df_csv.shape[0]} records from the csv file.')
    except Exception as e:
        logger.exception(f'{csv_file_name} : - exception {e} encountered while extracting the csv file.')
        df_csv = pd.DataFrame()

    return df_csv


# extract the data from the api
def source_cata_from_api(api_endpoint):
    try:
        response = requests.get(api_endpoint)
        apt_status = response.status_code
        if apt_status == 200:
            logger.info(f'{apt_status} - ok : while invoking the api {api_endpoint}.')
            df_api = pd.json_normalize(response.json())
            logger.info(f'{apt_status} - extracted {df_api.shape[0]} records from the api.')
        else:
            logger.exception(f'{apt_status} - error : while invoking the api {api_endpoint}.')
            df_api = pd.DataFrame()
    except Exception as e:
        logger.exception(f'{apt_status} : - exception {e} encountered while reading the data from the api.')
        df_api = pd.DataFrame()
    return df_api


# extract the data from the sqlite database
def source_data_from_table(db_name, table_name):
    try:
        with sqlite3.connect(db_name) as conn:
            df_table = pd.read_sql(f'SELECT * FROM {table_name}', conn)
            logger.info(f'{db_name} - read {df_table.shape[0]} records from the table {table_name}.')
    except Exception as e:
        logger.info(f'{db_name} : - exception {e} encountered while reading data from the table {table_name}.')
        df_table = pd.DataFrame()
    return df_table


# extract the data from a web page
def source_data_from_webpage(web_page_url, matching_keyword):
    try:
        df_html = pd.read_html(web_page_url, match=matching_keyword)
        df_html = df_html[0]
        logger.info(f'{web_page_url} : - read {df_html.shape[0]} records from the page {web_page_url}.')
    except Exception as e:
        logger.exception(f'{web_page_url} : - exception {e} encountered while reading data from the page.')
        df_html = pd.DataFrame()
    return df_html


# main function
def extracted_data():
    data_path = os.path.join('datasets')
    parquet_file_name = f'{data_path}/yellow_tripdata_2022-01.parquet'
    csv_file_name = f'{data_path}/h9gi-nx95.csv'
    api_endpoint = 'https://data.cityofnewyork.us/resource/h9gi-nx95.json?$limit=500'
    db_name = f'{data_path}/movies.sqlite'
    table_name = 'movies'
    web_page_url = 'https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)'
    matching_keyword = 'by country'

    df_parquet, df_csv, df_api, df_table, df_html = (source_data_from_parquet(parquet_file_name),
                                                     source_data_from_csv(csv_file_name),
                                                     source_cata_from_api(api_endpoint),
                                                     source_data_from_table(db_name, table_name),
                                                     source_data_from_webpage(web_page_url, matching_keyword))

    return df_parquet, df_csv, df_api, df_table, df_html

