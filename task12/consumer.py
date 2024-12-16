from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, hour
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType

# Конфигурация
KAFKA_BROKER = "localhost:9092"
TRANSACTION_TOPIC = "transaction_topic"
SUSPICIOUS_TOPIC = "suspicious_transactions"
POSTGRES_URL = "jdbc:postgresql://localhost:5432/kafka"
POSTGRES_USER = "kafka_adm"
POSTGRES_PASSWORD = "1234q"
USER_INFO_TABLE = "clients"

spark = SparkSession.builder \
    .appName("SuspiciousTransactionDetection") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.postgresql:postgresql:42.6.0") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .config("spark.hadoop.mapreduce.framework.name", "local") \
    .getOrCreate()


transactions_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TRANSACTION_TOPIC) \
    .load()

transactions = transactions_df.selectExpr("CAST(value AS STRING) as json_data")

schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("amount", FloatType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("location", StringType(), True)
])

parsed_data = transactions.withColumn("data", from_json(col("json_data"), schema)).select("data.*")

SUSPICIOUS_HOURS = list(range(23, 24)) + list(range(0, 5))
suspicious_transactions = parsed_data.filter(
    (col("amount") > 1000) |  # Пороговая сумма
    (hour(col("timestamp")).isin(SUSPICIOUS_HOURS))  # Ночное время
)

user_info_df = spark.read \
    .format("jdbc") \
    .option("url", POSTGRES_URL) \
    .option("dbtable", USER_INFO_TABLE) \
    .option("user", POSTGRES_USER) \
    .option("password", POSTGRES_PASSWORD) \
    .load()

suspicious_with_user_info = suspicious_transactions.join(
    user_info_df,
    on="user_id",
    how="left"
)

query = suspicious_with_user_info.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("topic", SUSPICIOUS_TOPIC) \
    .outputMode("append") \
    .start()

query.awaitTermination()
