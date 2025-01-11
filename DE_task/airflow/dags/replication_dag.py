from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook

# Константы
TABLES = ["productcategories", "users", "products", "orders", "orderdetails"]
JARS = "/opt/airflow/spark/jars/postgresql-42.2.18.jar,/opt/airflow/spark/jars/mysql-connector-java-8.3.0.jar"
PYSPARK_REPLICATION_SCRIPT_PATH = f'/opt/airflow/scripts/extract_data.py'


# Параметры по умолчанию для DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 9),
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}


dag = DAG(
    dag_id='from_pg_to_mysql',
    default_args=default_args,
    description='Replicate data from PostgreSQL to MySQL using Spark',
    schedule_interval='@daily',
    catchup=False
)

start = EmptyOperator(task_id='start_transfer', dag=dag)
finish = EmptyOperator(task_id='finish_transfer', dag=dag)

for table in TABLES:
    transfer_task = SparkSubmitOperator(
        task_id=f'transfer_{table}',
        application=PYSPARK_REPLICATION_SCRIPT_PATH,
        conn_id='pyspark_conn_in_airflow',
        application_args=[
            '--source_conn_id', 'postgres_conn_in_airlflow',
            '--target_conn_id', 'mysql_conn_in_airlflow',
            '--table', table
        ],
        jars=JARS,
        dag=dag
    )
    start >> transfer_task >> finish
