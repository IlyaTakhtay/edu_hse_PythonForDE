from kafka import KafkaConsumer, KafkaProducer
import psycopg2
import json
from datetime import datetime

# Kafka настройки
consumer = KafkaConsumer(
    "transactions_topic",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset="earliest"
)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# PostgreSQL настройки
conn = psycopg2.connect(
    dbname="kafka",
    user="kafka_adm",
    password="1234q",
    host="localhost",
    port="5432"
)

cursor = conn.cursor()

# Пороговое значение и необычное время
THRESHOLD = 1000
SUSPICIOUS_HOURS = [range(23, 24)] + [range(0, 5)]

def is_suspicious(transaction, user_info):
    # Проверка суммы
    if transaction["amount"] > THRESHOLD:
        return True
    
    # Проверка времени
    transaction_time = datetime.fromisoformat(transaction["timestamp"])
    if transaction_time.hour in SUSPICIOUS_HOURS:
        return True
    
    # Проверка местоположения
    if transaction["location"] not in [user_info["registration_address"], user_info["last_known_location"]]:
        return True

    return False

if __name__ == "__main__":
    print("Analyzing transactions...")
    for message in consumer:
        transaction = message.value

        # Получаем информацию о пользователе из базы данных
        cursor.execute(
            "SELECT registration_address, last_known_location FROM clients WHERE user_id = %s",
            (transaction["user_id"],)
        )
        result = cursor.fetchone()

        if result:
            user_info = {
                "registration_address": result[0],
                "last_known_location": result[1]
            }

            # Проверяем на подозрительность
            if is_suspicious(transaction, user_info):
                print(f"Suspicious transaction: {transaction}")
                producer.send("suspicious_transactions_topic", value=transaction)
