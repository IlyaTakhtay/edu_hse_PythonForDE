from pyspark.sql import SparkSession
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
import argparse

def replicate_table(source_conn_id: str, target_conn_id: str, table: str, ti=None):
    # Получаем объект подключения из Airflow
    pg_hook = PostgresHook(postgres_conn_id=source_conn_id)
    mysql_hook = MySqlHook(mysql_conn_id=target_conn_id)

    # Получаем реквизиты подключения
    pg_conn = pg_hook.get_connection(source_conn_id)
    mysql_conn = mysql_hook.get_connection(target_conn_id)

    pg_schema = pg_conn.schema
    mysql_schema = mysql_conn.schema
    
    spark = (
        SparkSession.builder
        .appName(f"replicate_pg_table_{table}_to_mysql")
        .config("spark.jars", "/opt/airflow/jars/postgresql-42.3.8.jar,/opt/airflow/jars/mysql-connector-java-8.0.30.jar") # Укажите здесь путь до ваших драйверов
        .getOrCreate()
    )

    # Чтение данных из источника
    pg_df = (
        spark.read
        .format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", f"jdbc:postgresql://{pg_conn.host}:{pg_conn.port}/{pg_conn.schema}")
        .option("dbtable", f"{pg_schema}.{table}")
        .option("user", pg_conn.login)
        .option("password", pg_conn.password)
        .load()
    )

    # Запись данных в приемник
    (
        pg_df.write
        .format("jdbc")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("url", f"jdbc:mysql://{mysql_conn.host}:{mysql_conn.port}/{mysql_conn.schema}")
        .option("dbtable", table)
        .option("user", mysql_conn.login)
        .option("password", mysql_conn.password)
        .mode("append")
        .save()
    )

    spark.stop()

def main():
    parser = argparse.ArgumentParser(description="Extract data from PostgreSQL and load to MySQL")
    parser.add_argument("--pg_conn_id", type=str, required=True, default="postgres_default")
    parser.add_argument("--mysql_conn_id", type=str, required=True, default="mysql_default")
    parser.add_argument("--table", type=str, required=True)
    args = parser.parse_args()

    replicate_table(args.pg_conn_id, args.mysql_conn_id, args.table)

if __name__ == "__main__":
    main()