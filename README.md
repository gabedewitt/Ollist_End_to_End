# Pipeline do Airflow para análise de dados

## Resumo:

## Requisitos:

## Repositórios:

## Introdução:

## Instruções:

docker run -d --name minio_datalake -p 9000:9000 -p 9001:9001 -e "MINIO_ROOT_USER=miniouser" -e "MINIO_ROOT_PASSWORD=miniopwd" -v $PWD/files/datalake:/data minio/minio server /data --console-address ":9001"

docker run -d -p 8080:8080 -v "$PWD/airflow/dags:/opt/airflow/dags/" -v "$PWD/airflow/logs:/opt/airflow/logs/" --entrypoint=/bin/bash --name airflow_service apache/airflow:2.1.1-python3.8 -c '(airflow db init && airflow users create --username admin --password airflowpass --firstname docker --lastname team --role Admin --email admin@example.org); airflow webserver & airflow scheduler'

pip install minio

## Resultados:

## Por vir:

## Conjunto de dados: