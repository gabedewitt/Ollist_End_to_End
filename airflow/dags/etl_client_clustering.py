from airflow import DAG
import pandas as pd
import datetime as dt
from airflow.operators.python import PythonOperator
from minio import Minio
import os
import glob
import functions as f

data_lake_server= f.var['data_lake_server_airflow']
data_lake_login= f.var['data_lake_login']
data_lake_password= f.var['data_lake_password']

client = Minio(
        endpoint= data_lake_server,
        access_key= data_lake_login,
        secret_key= data_lake_password,
        secure=False
    )

dag = DAG(
    dag_id="etl_client_clustering",
    description="ETL - Client Clustering DataFrame",
    start_date=dt.datetime(2021, 11, 29),
    schedule_interval= "@once")

##################### olist_customers_dataset #####################

def extract_customers():
    # load data to a tmp folder
    client.fget_object(
        bucket_name= 'processing',
        object_name= 'olist_customers_dataset.parquet',
        file_path= 'tmp/olist_customers_dataset.parquet'
        )

extract_customers_task = PythonOperator(
    task_id= "extract_customers", 
    python_callable= extract_customers,
    dag= dag)

##################### olist_orders_dataset #####################

def extract_orders():
    # load data to a tmp folder
    client.fget_object(
        bucket_name= 'processing',
        object_name= 'olist_orders_dataset.parquet',
        file_path= 'tmp/olist_orders_dataset.parquet'
        )

extract_orders_task = PythonOperator(
    task_id= "extract_orders", 
    python_callable= extract_orders,
    dag= dag)

##################### olist_order_items_dataset #####################

def extract_order_items():
    # load data to a tmp folder
    client.fget_object(
        bucket_name= 'processing',
        object_name= 'olist_order_items_dataset.parquet',
        file_path= 'tmp/olist_order_items_dataset.parquet'
        )

extract_order_items_task = PythonOperator(
    task_id= "extract_order_items", 
    python_callable= extract_order_items,
    dag= dag)

##################### olist_geolocation_dataset #####################

def extract_geolocation():
    # load data to a tmp folder
    client.fget_object(
        bucket_name= 'processing',
        object_name= 'olist_geolocation_dataset.parquet',
        file_path= 'tmp/olist_geolocation_dataset.parquet'
        )

extract_geolocation_task = PythonOperator(
    task_id= "extract_geolocation", 
    python_callable= extract_geolocation,
    dag= dag)


def transform_data():
    customers = pd.read_parquet('tmp/olist_customers_dataset.parquet')
    orders = pd.read_parquet('tmp/olist_orders_dataset.parquet')
    order_items = pd.read_parquet('tmp/olist_order_items_dataset.parquet')
    geolocation = pd.read_parquet('tmp/olist_geolocation_dataset.parquet')

    geo_means = geolocation.groupby(
        ['geolocation_zip_code_prefix', 'geolocation_city', 'geolocation_state']
        )[['geolocation_lat', 'geolocation_lng']].mean().reset_index()

    customers = customers.merge(
        geo_means,
        left_on= ['customer_zip_code_prefix', 'customer_city', 'customer_state'],
        right_on= ['geolocation_zip_code_prefix', 'geolocation_city', 'geolocation_state'],
        how= 'left'
        ).drop(columns= ['geolocation_zip_code_prefix', 'geolocation_city', 'geolocation_state'])

    del geo_means

    price_per_order = order_items.groupby('order_id').price.sum().reset_index().rename(columns= {'price': 'monetary'})
    orders = pd.merge(orders, price_per_order, on= 'order_id', how= 'inner')
    orders.order_purchase_timestamp = pd.to_datetime(orders.order_purchase_timestamp)
    ult_compra = orders.order_purchase_timestamp.max()
    orders['days_ult_compra'] = (ult_compra - orders.order_purchase_timestamp).dt.days

    df_rfm = pd.merge(
        customers[['customer_unique_id', 'customer_id', 'geolocation_lat', 'geolocation_lng']],
        orders[['customer_id', 'monetary', 'days_ult_compra']],
        on= 'customer_id',
        how= 'left')\
            .groupby('customer_unique_id')\
                .agg({
                    'geolocation_lat': 'mean',
                    'geolocation_lng': 'mean',
                    'customer_id': 'count',
                    'monetary': 'sum',
                    'days_ult_compra': 'min'
                    }).reset_index()\
                        .rename(
                            columns= {
                                'customer_id': 'frequency',
                                'days_ult_compra': 'recency'
                                }
                                )
    
    df_rfm.to_parquet('tmp/dataframe_rfm.parquet')

    client.fput_object(
        bucket_name= 'agrupamento-clientes',
        object_name= 'dataframe_rfm.parquet',
        file_path= 'tmp/dataframe_rfm.parquet'
        )

transform_data_task = PythonOperator(
    task_id= "transform_data", 
    python_callable= transform_data,
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

[extract_customers_task, extract_order_items_task, extract_orders_task, extract_geolocation_task] >> transform_data_task
transform_data_task >> clean_task


