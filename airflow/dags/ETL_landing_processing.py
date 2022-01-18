from airflow import DAG
import pandas as pd
import datetime as dt
from airflow.operators.python import PythonOperator
from minio import Minio
import os
import glob
from functions import var

data_lake_server= var['data_lake_server_airflow']
data_lake_login= var['data_lake_login']
data_lake_password= var['data_lake_password']

client = Minio(
        endpoint= data_lake_server,
        access_key= data_lake_login,
        secret_key= data_lake_password,
        secure=False
    )

dag = DAG(
    dag_id="etl_landing_processing",
    description="ETL - Landing to Processing",
    start_date=dt.datetime(2021, 11, 29),
    schedule_interval= "@once")

##################### olist_customers_dataset #####################

def extract_customers():
    # load data to a tmp folder
    client.fget_object(
        bucket_name= 'landing',
        object_name= 'olist_customers_dataset.csv',
        file_path= 'tmp/olist_customers_dataset.csv'
        )

def load_customers():
    temp_df = pd.read_csv('tmp/olist_customers_dataset.csv')
    temp_df.to_parquet('tmp/olist_customers_dataset.parquet')

    client.fput_object(
        bucket_name= 'processing',
        object_name= 'olist_customers_dataset.parquet',
        file_path= 'tmp/olist_customers_dataset.parquet'
        )

extract_customers_task = PythonOperator(
    task_id= "extract_customers", 
    python_callable= extract_customers,
    dag= dag)

load_customers_task = PythonOperator(
    task_id= "load_customers", 
    python_callable= load_customers,
    dag= dag)

##################### olist_order_items_dataset #####################

def extract_order_items():
    # load data to a tmp folder
    client.fget_object(
        bucket_name= 'landing',
        object_name= 'olist_order_items_dataset.csv',
        file_path= 'tmp/olist_order_items_dataset.csv'
        )

def load_order_items():
    temp_df = pd.read_csv('tmp/olist_order_items_dataset.csv')
    temp_df.to_parquet('tmp/olist_order_items_dataset.parquet')

    client.fput_object(
        bucket_name= 'processing',
        object_name= 'olist_order_items_dataset.parquet',
        file_path= 'tmp/olist_order_items_dataset.parquet'
        )

extract_order_items_task = PythonOperator(
    task_id= "extract_order_items", 
    python_callable= extract_order_items,
    dag= dag)

load_order_items_task = PythonOperator(
    task_id= "load_order_items", 
    python_callable= load_order_items,
    dag= dag)

##################### olist_orders_dataset #####################

def extract_orders():
    # load data to a tmp folder
    client.fget_object(
        bucket_name= 'landing',
        object_name= 'olist_orders_dataset.csv',
        file_path= 'tmp/olist_orders_dataset.csv'
        )

def load_orders():
    temp_df = pd.read_csv('tmp/olist_orders_dataset.csv')
    temp_df.to_parquet('tmp/olist_orders_dataset.parquet')

    client.fput_object(
        bucket_name= 'processing',
        object_name= 'olist_orders_dataset.parquet',
        file_path= 'tmp/olist_orders_dataset.parquet'
        )

extract_orders_task = PythonOperator(
    task_id= "extract_orders", 
    python_callable= extract_orders,
    dag= dag)

load_orders_task = PythonOperator(
    task_id= "load_orders", 
    python_callable= load_orders,
    dag= dag)

##################### olist_products_dataset #####################

def extract_products():
    # load data to a tmp folder
    client.fget_object(
        bucket_name= 'landing',
        object_name= 'olist_products_dataset.csv',
        file_path= 'tmp/olist_products_dataset.csv'
        )

def load_products():
    temp_df = pd.read_csv('tmp/olist_products_dataset.csv')
    temp_df.to_parquet('tmp/olist_products_dataset.parquet')

    client.fput_object(
        bucket_name= 'processing',
        object_name= 'olist_products_dataset.parquet',
        file_path= 'tmp/olist_products_dataset.parquet'
        )

extract_products_task = PythonOperator(
    task_id= "extract_products", 
    python_callable= extract_products,
    dag= dag)

load_products_task = PythonOperator(
    task_id= "load_products", 
    python_callable= load_products,
    dag= dag)

##################### olist_sellers_dataset #####################

def extract_sellers():
    # load data to a tmp folder
    client.fget_object(
        bucket_name= 'landing',
        object_name= 'olist_sellers_dataset.csv',
        file_path= 'tmp/olist_sellers_dataset.csv'
        )

def load_sellers():
    temp_df = pd.read_csv('tmp/olist_sellers_dataset.csv')
    temp_df.to_parquet('tmp/olist_sellers_dataset.parquet')

    client.fput_object(
        bucket_name= 'processing',
        object_name= 'olist_sellers_dataset.parquet',
        file_path= 'tmp/olist_sellers_dataset.parquet'
        )

extract_sellers_task = PythonOperator(
    task_id= "extract_sellers", 
    python_callable= extract_sellers,
    dag= dag)

load_sellers_task = PythonOperator(
    task_id= "load_sellers", 
    python_callable= load_sellers,
    dag= dag)

##################### clean #####################

def clean():
    files_remove = glob.glob('tmp/*')

    for f in files_remove:
        os.remove(f)

clean_task = PythonOperator(
    task_id= "clean", 
    python_callable= clean,
    dag= dag)

extract_customers_task >> load_customers_task >> clean_task
extract_order_items_task >> load_order_items_task >> clean_task
extract_orders_task >> load_orders_task >> clean_task
extract_products_task >> load_products_task >> clean_task
extract_sellers_task >> load_sellers_task >> clean_task


