import argparse

from pyspark.sql import SparkSession
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook

def get_connection_uri(conn):
    """
    URI из conn.

    """
    jdbc_prefix = {
        "postgres": "postgresql",
        "mysql": "mysql"
    }
    prefix = jdbc_prefix[conn.conn_type]
    extras = "".join([f"&{k}={v}" for k, v in conn.extra_dejson.items()])
    return f"jdbc:{prefix}://{conn.host}:{conn.port}/{conn.schema}?user={conn.login}&password={conn.password}{extras}"


def transfer_data(source_conn_id: str, target_conn_id: str, dataset_name: str):
    """
    Transfers data from a source database to a target database, replacing the existing dataset if present.

    Args:
        source_conn_id (str): Airflow connection ID for the source database.
        target_conn_id (str): Airflow connection ID for the target database.
        dataset_name (str): Name of the dataset (e.g., table) to transfer.
    """
    spark = (
        SparkSession.builder
        .appName(f"TransferData-{dataset_name}")
        .getOrCreate()
    )

    source_hook = PostgresHook(postgres_conn_id=source_conn_id)
    target_hook = MySqlHook(mysql_conn_id=target_conn_id)

    source_conn = source_hook.get_connection(source_conn_id)
    target_conn = target_hook.get_connection(target_conn_id)

    source_url = get_connection_uri(source_conn)
    target_url = get_connection_uri(target_conn)

    source_driver = 'org.postgresql.Driver'
    target_driver = 'com.mysql.cj.jdbc.Driver'

    source_data = (
        spark.read
        .format("jdbc")
        .option("driver", source_driver)
        .option("url", source_url)
        .option("dbtable", dataset_name)
        .load()
    )
    # Перезапись в mysql
    (
        source_data.write
        .format("jdbc")
        .option("driver", target_driver)
        .option("url", target_url)
        .option("dbtable", dataset_name)
        .mode("overwrite")
        .save()
    )

    spark.stop()


def main():
    """
    Parses command-line arguments and initiates the data transfer process.
    """
    parser = argparse.ArgumentParser(description="Transfer data between databases.")
    parser.add_argument("--source_conn_id", type=str, required=True, help="Source Airflow connection ID.")
    parser.add_argument("--target_conn_id", type=str, required=True, help="Target Airflow connection ID.")
    parser.add_argument("--table", type=str, required=True, help="Name of the table to transfer.")

    args = parser.parse_args()
    transfer_data(args.source_conn_id, args.target_conn_id, args.table)


if __name__ == "__main__":
    main()