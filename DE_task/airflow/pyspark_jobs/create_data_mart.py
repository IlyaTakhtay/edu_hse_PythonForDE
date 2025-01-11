import argparse
from pyspark.sql import SparkSession, functions as F
from airflow.providers.mysql.hooks.mysql import MySqlHook

def get_connection_uri(conn):
    return f"jdbc:mysql://{conn.host}:{conn.port}/{conn.schema}?user={conn.login}&password={conn.password}"

def create_sales_mart(source_conn_id: str, target_conn_id: str, target_table: str):
    spark = SparkSession.builder.appName("CreateSalesMart").getOrCreate()

    source_hook = MySqlHook(mysql_conn_id=source_conn_id)
    target_hook = MySqlHook(mysql_conn_id=target_conn_id)

    source_conn = source_hook.get_connection(source_conn_id)
    target_conn = target_hook.get_connection(target_conn_id)

    source_url = get_connection_uri(source_conn)
    target_url = get_connection_uri(target_conn)

    driver = 'com.mysql.cj.jdbc.Driver'

    orders = spark.read.format("jdbc") \
        .option("url", source_url) \
        .option("dbtable", "orders") \
        .option("driver", driver) \
        .load()
    orderdetails = spark.read.format("jdbc") \
        .option("url", source_url) \
        .option("dbtable", "orderdetails") \
        .option("driver", driver) \
        .load()

    # --- Формирование витрины sales_mart ---

    sales_mart = orderdetails.join(orders, "order_id", "inner")

    sales_mart = sales_mart.withColumn("order_date_sk", F.date_format("order_date", "yyyy-MM-dd").cast("date"))

    # Агрегируем данные по дате, пользователю и продукту
    sales_mart = sales_mart.groupBy("order_date_sk", "user_id", "product_id").agg(
        F.count("order_id").alias("total_orders"),
        F.sum("quantity").alias("total_quantity"),
        F.sum("total_price").alias("total_amount"),
        F.avg("total_amount").alias("avg_order_amount")
    )

    sales_mart = sales_mart.withColumn("load_date", F.current_timestamp())

    # Записываем данные в витрину
    (
        sales_mart.write.format("jdbc")
        .option("driver", driver)
        .option("url", target_url)
        .option("dbtable", target_table)
        .mode("overwrite")
        .save()
    )

    spark.stop()

def main():
    parser = argparse.ArgumentParser(description="Create a sales data mart in MySQL.")
    parser.add_argument("--source_conn_id", type=str, required=True, help="Source MySQL connection ID in Airflow.")
    parser.add_argument("--target_conn_id", type=str, required=True, help="Target MySQL connection ID in Airflow.")
    parser.add_argument("--target_table", type=str, required=True, help="Target table name (data mart).")

    args = parser.parse_args()
    create_sales_mart(args.source_conn_id, args.target_conn_id, args.target_table)

if __name__ == "__main__":
    main()