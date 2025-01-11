from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# Константы
JARS = "/opt/airflow/spark/jars/mysql-connector-java-8.3.0.jar"
PYSPARK_REPLICATION_SCRIPT_PATH = "/opt/airflow/scripts/create_data_mart.py"

# Параметры по умолчанию
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 9),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    dag_id='create_mysql_data_mart',
    default_args=default_args,
    description='Creates a data mart in MySQL using Spark',
    schedule_interval='@daily',
    catchup=False
)

start = EmptyOperator(task_id='start_data_mart_creation', dag=dag)
finish = EmptyOperator(task_id='finish_data_mart_creation', dag=dag)

create_mart_task = SparkSubmitOperator(
    task_id='create_data_mart',
    application=PYSPARK_REPLICATION_SCRIPT_PATH,
    conn_id='pyspark_conn_in_airflow',  # Подключение к Spark
    application_args=[
        '--source_conn_id', 'mysql_conn_in_airlflow',  # Подключение к исходной таблице
        '--target_conn_id', 'mysql_conn_in_airlflow',  # Подключение к витрине
        '--target_table', 'sales_mart',
    ],
    jars=JARS,
    dag=dag
)

start >> create_mart_task >> finish
