from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime

# Параметры Spark
spark_app_name = "PostgresToMySQLReplication"
spark_master = "local[*]"  # Замените на ваш Spark master, если нужно

# Таблицы для репликации (измените на нужные вам)
tables_to_replicate = ["users", "products", "orders", "orderdetails", "productcategories"]

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='postgres_to_mysql_replication',
    default_args=default_args,
    schedule_interval='@daily',  # Или другое расписание
    catchup=False,
) as dag:
    check_tables_task = PythonOperator(
        task_id='check_source_tables_existence',
        python_callable=check_source_tables_exist,
    )

    for table in tables_to_replicate:
        extract_and_load_task = SparkSubmitOperator(
            task_id=f'extract_and_load_{table}',
            application='/opt/airflow/spark_jobs/extract_data.py', # Путь внутри контейнера airflow
            conn_id='spark_default',
            application_args=[
                "--pg_conn_id", "postgres_default",  # ID подключения к PostgreSQL
                "--mysql_conn_id", "mysql_default",  # ID подключения к MySQL
                "--table", table, # Имя таблицы
            ],
            master=spark_master,
            name=spark_app_name,
            jars="/opt/airflow/jars/postgresql-42.3.8.jar,/opt/airflow/jars/mysql-connector-java-8.0.30.jar" # Путь до jar-ок внутри контейнера airflow
        )

        check_tables_task >> extract_and_load_task