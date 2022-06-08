import requests
import pytz
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
from q2_rt_schema import schema
from task_templates_portfolio import (create_empty_table,
                            create_external_table,
                            insert_job)
import time
from urllib.request import urlopen as uReq
from bs4 import BeautifulSoup as soup
import re
import pandas as pd


default_args ={
    'owner' : 'airflow'
}

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
GCP_GCS_BUCKET = os.environ.get('GCP_GCS_BUCKET')
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET', 'portfolio_stg')

ENDPOINTS = ['top_movies']

MACRO_VARS = {"GCP_PROJECT_ID":GCP_PROJECT_ID, 
            "BIGQUERY_DATASET": BIGQUERY_DATASET
            }

year_to_crawl = 2020

def crawl_to_parquet(my_url, dest_file):
    # grabbing connection
    uClient = uReq(my_url)
    page_html = uClient.read()
    uClient.close()

    # html parser
    page_soup = soup(page_html, "html.parser")

    # gather movies
    containers = page_soup.findAll("table", {"class":"table"})

    fact_top_movies = pd.DataFrame()

    for container in containers:
        temp_df = pd.DataFrame(data={'year':[year_to_crawl]})
        movie_rank_container = container.findAll("td", {"class":"bold"})
        movie_title_container = container.findAll("a", {"class":"unstyled articleLink"})
        movie_review_container = container.findAll("td", {"class":"right hidden-xs"})

        for movie_rank, movie_titles, movie_review in zip(movie_rank_container, movie_title_container, movie_review_container):
            rank = movie_rank.text.strip('.')
            temp_df['rank'] = rank

            title = movie_titles.text.strip()
            temp_df['title'] = title

            url = movie_titles['href'].strip()
            temp_df['url'] = url

            review = movie_review.text.strip()
            temp_df['reviews'] = review

            # print("rank: " + rank)
            # print("title: " + title)
            # print("url: " + url)
            # print("review: " + review)

            my_url = 'https://www.rottentomatoes.com' + url
            print(my_url)
            # grabbing connection
            uClient = uReq(my_url)
            page_html = uClient.read()
            uClient.close()

            # html parser
            page_soup = soup(page_html, "html.parser")

            score_container = page_soup.find("score-board",attrs={'class': 'scoreboard'})
            audience_score = score_container['audiencescore'] 
            temp_df['audience_score'] = audience_score

            tomatometer = score_container['tomatometerscore']
            temp_df['tomatometer'] = tomatometer

            synopsis_container = page_soup.find('div', {"class": "movie_synopsis"})
            synopsis = synopsis_container.text.strip()        
            temp_df['synopsis'] = synopsis

            movie_label_container = page_soup.findAll('div', attrs={"data-qa":"movie-info-item-label"})
            movie_value_container = page_soup.findAll('div', attrs={"data-qa":"movie-info-item-value"})
            
            for movie_label, movie_value in zip(movie_label_container, movie_value_container):
                # print(movie_label.text.strip(), movie_value.text.strip())
                info_label = movie_label.text.strip().replace(':','').lower()
                info_value = movie_value.text.strip().replace(':','')

                # print(info_label,info_value)
                temp_df[info_label] = info_value
            
            fact_top_movies = pd.concat([fact_top_movies,temp_df])

        fact_top_movies['producer'] = fact_top_movies['producer'].str.replace('  ','')
        fact_top_movies['director'] = fact_top_movies['director'].str.replace('  ','')
        fact_top_movies['writer'] = fact_top_movies['writer'].str.replace('  ','')        

        fact_top_movies['split_runtime_hours'] = fact_top_movies['runtime'].str.split(' ').str[0].str.split('h').str[0]
        fact_top_movies['split_runtime_hours'] =  np.where(fact_top_movies['split_runtime_hours'].str.contains('m'), pd.to_numeric(fact_top_movies['split_runtime_hours'].str.split('m').str[0]) /60, fact_top_movies['split_runtime_hours'])
        fact_top_movies['split_runtime_hours'] = fact_top_movies['split_runtime_hours'].astype('float')

        fact_top_movies['split_runtime_mins'] = fact_top_movies['runtime'].str.split(' ').str[1].str.split('m').str[0]
        fact_top_movies['split_runtime_mins'] = np.where(fact_top_movies['split_runtime_mins'].isnull(), 0, fact_top_movies['split_runtime_mins'])
        fact_top_movies['split_runtime_mins'] = fact_top_movies['split_runtime_mins'].astype('float')
        
        fact_top_movies['runtime_mins'] = (fact_top_movies['split_runtime_hours'] * 60) +  fact_top_movies['split_runtime_mins']
        fact_top_movies['runtime_mins'] = fact_top_movies['runtime_mins'].astype('int')

        fact_top_movies.rename(columns={'original language': 'original_language', 
                                        'release date (theaters)': 'release_date_theaters',
                                        'release date (streaming)': 'release_date_streaming',
                                        'aspect ratio': 'aspect_ratio',
                                        'view the collection': 'production_co'},
                                        inplace=True)

        fact_top_movies['year'] = fact_top_movies['year'].astype('int')
        fact_top_movies['reviews'] = fact_top_movies['reviews'].astype('int')
        fact_top_movies['tomatometer'] = fact_top_movies['tomatometer'].astype('float')
        fact_top_movies['audience_score'] = np.where(fact_top_movies['audience_score']=='','0',fact_top_movies['audience_score'])
        fact_top_movies['audience_score'] = fact_top_movies['audience_score'].astype('float')
        fact_top_movies['release_date_theaters'] = fact_top_movies['release_date_theaters'].str.split('\n').str[0]
        fact_top_movies['release_date_theaters'] = pd.to_datetime(fact_top_movies['release_date_theaters'])
        
        fact_top_movies['release_date_streaming'] = fact_top_movies['release_date_streaming'].str.split('\n').str[0]
        fact_top_movies['release_date_streaming'] = pd.to_datetime(fact_top_movies['release_date_streaming'])

        fact_top_movies.drop(columns=['rank'],errors='ignore',axis=1,inplace=True)
        fact_top_movies.drop(columns=['box office (gross usa)'],errors='ignore',axis=1,inplace=True)
        fact_top_movies.drop(columns=['sound mix'],errors='ignore',axis=1,inplace=True)
        fact_top_movies.drop(columns=['split_runtime_hours'],errors='ignore',axis=1,inplace=True)
        fact_top_movies.drop(columns=['split_runtime_mins'],errors='ignore',axis=1,inplace=True)
        fact_top_movies.drop(columns=['runtime'],errors='ignore',axis=1,inplace=True)
        
        fact_top_movies.to_parquet(dest_file,index=False)

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
    dag_id = f'rotten_tomatoes_etl_dag',
    default_args = default_args,
    description = f'Data pipeline to get crawl data from rottentomatoes (https://www.rottentomatoes.com/top/bestofrt?year=2020)',
    schedule_interval="@once",
    start_date=datetime(2022,6,5),
    catchup=False,
    user_defined_macros=MACRO_VARS,
    tags=['rotten_tomatoes_etl_dag']
) as dag:

    for endpoint in ENDPOINTS:
        DATASET_URL=f"https://www.rottentomatoes.com/top/bestofrt?year={year_to_crawl}"
        PARQUET_FILENAME = f'rottentomatoes_{endpoint}.parquet'
        PARQUET_OUTFILE = f'{AIRFLOW_HOME}/{PARQUET_FILENAME}'

        crawl_to_parquet_task = PythonOperator(
            task_id=f'convert_rottentomatoes_{endpoint}_to_parquet_task',
            python_callable= crawl_to_parquet,
            op_kwargs = {
                'my_url' : DATASET_URL,
                'dest_file' : PARQUET_OUTFILE
            }
        )
        TABLE_MAP = { f"FACT_{endpoint.upper()}_TABLE" : 'fact_'+endpoint for endpoint in ENDPOINTS}
        TABLE_NAME = f'fact_{endpoint}'
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
            bash_command=f'rm {PARQUET_OUTFILE}' 
        )
        
        fact_table_name = 'fact_'+endpoint
        external_table_name = f'{fact_table_name}'+'_external'
        endpoint_data_path = f'{fact_table_name}'
        endpoints_schema = schema['fact_'+endpoint]

        create_external_table_task = create_external_table('fact_'+endpoint,
                                                            GCP_PROJECT_ID, 
                                                            BIGQUERY_DATASET, 
                                                            external_table_name, 
                                                            GCP_GCS_BUCKET, 
                                                            endpoint_data_path)

        create_empty_table_task = create_empty_table('fact_'+endpoint,
                                                GCP_PROJECT_ID,
                                                BIGQUERY_DATASET,
                                                fact_table_name,
                                                endpoints_schema)

        
        insert_query = f"{{% include 'sql/fact_{endpoint}.sql' %}}" #extra {} for f-strings escape

        execute_insert_query_task = insert_job('fact_'+endpoint,
                                        insert_query,
                                        BIGQUERY_DATASET,
                                        GCP_PROJECT_ID)

        crawl_to_parquet_task >> \
        upload_to_gcs_task >> \
        remove_files_from_local_task >> \
        create_external_table_task >> \
        create_empty_table_task >> \
        execute_insert_query_task