from pandas import read_parquet
from io import BytesIO

var = {
    'data_lake_server_airflow': '172.17.0.2:9000',
    'data_lake_server_local': '127.0.0.1:9000',
    'data_lake_login': 'miniouser',
    'data_lake_password': 'miniopwd'
}

def read_minio_parquet(bucket_name, file, client):
    return read_parquet(
        BytesIO(
            client.get_object(
                bucket_name= bucket_name,
                object_name= file
            ).read()
        )
    )