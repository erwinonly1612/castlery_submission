from bs4 import BeautifulSoup 
import requests
import pytz
import re
import pandas as pd
import numpy as np
import glob
from datetime import datetime
import os
import json
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import logging
from google.cloud import storage
from q1_swapi_schema import schema
from task_templates_portfolio import (create_empty_table,
                            create_external_table,
                            insert_job)
import time

default_args ={
    'owner' : 'airflow'
}

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
GCP_GCS_BUCKET = os.environ.get('GCP_GCS_BUCKET')
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET', 'portfolio_stg')

ENDPOINTS = ['people','films', 'planets']

MACRO_VARS = {"GCP_PROJECT_ID":GCP_PROJECT_ID, 
            "BIGQUERY_DATASET": BIGQUERY_DATASET
            }
    
def convert_to_parquet(endpoint, src_file, dest_file):
    if not src_file.endswith('.json'):
        logging.error("Can only accept source files in JSON format, for the moment")
        return
    json_file = open(src_file)
    
    raw_json = json.load(json_file)['results']
    df = pd.DataFrame()
    
    if endpoint == 'people' or endpoint == 'films':
        df = pd.json_normalize(raw_json, max_level=1)
        if endpoint == 'people':
            df['films'] = df['films'].astype('string')
            df['height'] = df['height'].astype(int)
            df['mass'] = df['mass'].astype(int)
        elif endpoint == 'films':
            df['characters'] = df['characters'].astype('string')
            df['planets'] = df['planets'].astype('string')

        df['species'] = df['species'].astype('string')
        df['starships'] = df['starships'].astype('string')
        df['vehicles'] = df['vehicles'].astype('string')
        df['created'] = pd.to_datetime(df['created'])
        df['edited'] = pd.to_datetime(df['edited'])
    
    elif endpoint == 'planets':
        
        for planet in raw_json:
            print(planet['url'])
            temp_df = pd.DataFrame(columns=['url', 'string_content'])
            temp_df = temp_df.append({'url': planet['url'], 'string_content': planet}, ignore_index=True)
            temp_df['string_content'] = temp_df['string_content'] .astype('string')
            df = pd.concat([df, temp_df])

    df.to_parquet(dest_file)


def upload_to_gcs(file_path, bucket_name, blob_name):
    """
    Upload the downloaded file to GCS
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(file_path)

with DAG(
    dag_id = f'swapi_etl_dag',
    default_args = default_args,
    description = f'Data pipeline to get to data from swapi (https://swapi.dev/)',
    schedule_interval="@once",
    start_date=datetime(2022,6,5),
    catchup=False,
    user_defined_macros=MACRO_VARS,
    tags=['swapi_etl_dag']
) as dag:

    for endpoint in ENDPOINTS:
        DATASET_URL=f"https://swapi.dev/api/{endpoint}"
        JSON_FILENAME = f'swapi_{endpoint}.json'
        PARQUET_FILENAME = f'swapi_{endpoint}.parquet'
        JSON_OUTFILE = f'{AIRFLOW_HOME}/{JSON_FILENAME}'
        PARQUET_OUTFILE = f'{AIRFLOW_HOME}/{PARQUET_FILENAME}'

        download_dataset_task = BashOperator(
            task_id=f"download_swapi_{endpoint}_dataset_task",
            bash_command=f"curl -sS {DATASET_URL} > {JSON_OUTFILE}"
        )

        convert_to_parquet_task = PythonOperator(
            task_id=f'convert_swapi_{endpoint}_to_parquet_task',
            python_callable= convert_to_parquet,
            op_kwargs = {
                'src_file' : JSON_OUTFILE,
                'endpoint' : endpoint,
                'dest_file' : PARQUET_OUTFILE
            }
        )

        TABLE_MAP = { f"STAGING_{endpoint.upper()}_TABLE" : 'staging_'+endpoint for endpoint in ENDPOINTS}
        TABLE_NAME = f'staging_{endpoint}'
        EXTERNAL_TABLE_MAP = {f"{TABLE_NAME.upper()}_EXTERNAL_TABLE": TABLE_NAME+'_external'}
        MACRO_VARS.update(TABLE_MAP)
        MACRO_VARS.update(EXTERNAL_TABLE_MAP)            

        upload_to_gcs_task = PythonOperator(
            task_id=f'upload_{TABLE_NAME}_to_gcs',
            python_callable = upload_to_gcs,
            op_kwargs = {
                'file_path' : PARQUET_OUTFILE,
                'bucket_name' : GCP_GCS_BUCKET,
                'blob_name' : f'{TABLE_NAME}/{PARQUET_FILENAME}'
            }
        )
        
        remove_files_from_local_task=BashOperator(
            task_id=f'remove_{endpoint}_files_from_local',
            bash_command=f'rm {PARQUET_OUTFILE} {JSON_OUTFILE}' 
        )
        
        staging_table_name = 'staging_'+endpoint
        external_table_name = f'{staging_table_name}'+'_external'
        endpoint_data_path = f'{staging_table_name}'
        endpoints_schema = schema['staging_'+endpoint]

        create_external_table_task = create_external_table('staging_'+endpoint,
                                                            GCP_PROJECT_ID, 
                                                            BIGQUERY_DATASET, 
                                                            external_table_name, 
                                                            GCP_GCS_BUCKET, 
                                                            endpoint_data_path)

        create_empty_table_task = create_empty_table('staging_'+endpoint,
                                                GCP_PROJECT_ID,
                                                BIGQUERY_DATASET,
                                                staging_table_name,
                                                endpoints_schema)

        
        insert_query = f"{{% include 'sql/staging_{endpoint}.sql' %}}" #extra {} for f-strings escape

        execute_insert_query_task = insert_job('staging_'+endpoint,
                                        insert_query,
                                        BIGQUERY_DATASET,
                                        GCP_PROJECT_ID)

        

        download_dataset_task >> \
        convert_to_parquet_task >> \
        upload_to_gcs_task >> \
        remove_files_from_local_task >> \
        create_external_table_task >> \
        create_empty_table_task >> \
        execute_insert_query_task