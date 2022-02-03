# Pipeline do Airflow para análise de dados

## Resumo:

## Requisitos:

## Repositórios:

## Introdução:

O olist atua no segmento de e-commerce, mas não é um e-commerce propriamente dito. O olist é uma grande loja de departamentos dentro dos marketplaces, formada por milhares de outras lojas espalhadas por todo o Brasil.



Visão Ollist: (Belmino)
    - Quais lojistas vendem mais?
    - Quais lojistas vendem menos?
    - Média de venda por lojista?
    - Quantidade de Lojistas inativos (que não fizeram venda)?

Visão Lojista: (Gabriel)
    - Quantas vendas eu fiz esse mês?
    - Quanto de dinheiro eu vendi esse mês?
    - Para onde estou vendendo (estado, cidade)?
    - Quanto meus clientes estão pagando de frete?
    - Qual o metodo de pagamento utilizado pelo meus clientes?
    - Tempo média de entrega dos produtos?
       
Modelo de Propensão de Vendas (Rafael)

Concluir Agrupamento de cliente (Belmino)

## Instruções:

docker run -d --name minio_datalake -p 9000:9000 -p 9001:9001 -e "MINIO_ROOT_USER=miniouser" -e "MINIO_ROOT_PASSWORD=miniopwd" -v $PWD/files/datalake:/data minio/minio server /data --console-address ":9001"

docker run -d -p 8080:8080 -v "$PWD/airflow/dags:/opt/airflow/dags/" --entrypoint=/bin/bash --name airflow_service apache/airflow:2.1.1-python3.8 -c '(airflow db init && airflow users create --username admin --password airflowpass --firstname docker --lastname team --role Admin --email admin@example.org); airflow webserver & airflow scheduler'

pip install minio

## Resultados:

## Por vir:

## Conjunto de dados: