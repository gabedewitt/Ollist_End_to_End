from airflow import DAG
import pandas as pd
import datetime as dt
from airflow.operators.python import PythonOperator
from minio import Minio
import os
import glob

data_lake_server= '172.17.0.2:9000'
data_lake_login= 'miniouser'
data_lake_password= 'miniopwd'

file_name = 'olist_orders_dataset'

client = Minio(
        endpoint= data_lake_server,
        access_key= data_lake_login,
        secret_key= data_lake_password,
        secure=False
    )

dag = DAG(
    dag_id="{}".format(file_name),
    description="ETL - {}".format(file_name),
    start_date=dt.datetime(2021, 11, 29),
    schedule_interval= "@once")

def extract():
    # load data to a tmp folder
    client.fget_object(
        bucket_name= 'landing',
        object_name= '{}.csv'.format(file_name),
        file_path= 'tmp/{}.csv'.format(file_name)
        )

def load():
    temp_df = pd.read_csv('tmp/{}.csv'.format(file_name))
    temp_df.to_parquet('tmp/{}.parquet'.format(file_name))

    client.fput_object(
        bucket_name= 'processing',
        object_name= '{}.parquet'.format(file_name),
        file_path= 'tmp/{}.parquet'.format(file_name)
        )

def clean():
    files_remove = glob.glob('tmp/*')

    for f in files_remove:
        os.remove(f)

extract_task = PythonOperator(
    task_id= "extract", 
    python_callable= extract,
    dag= dag)

load_task = PythonOperator(
    task_id= "load", 
    python_callable= load,
    dag= dag)

clean_task = PythonOperator(
    task_id= "clean", 
    python_callable= clean,
    dag= dag)

extract_task >> load_task >> clean_task