from kafka import KafkaProducer
import json
from datetime import datetime, timedelta
import random
from time import sleep

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

locations = ["NewYork", "LosAngeles", "Chicago", "Houston", "Phoenix"]
users = [123, 456, 789, 101, 202]

def generate_transaction():
    now = datetime.now()
    return {
        "transaction_id": random.randint(1000, 9999),
        "user_id": random.choice(users),
        "amount": round(random.uniform(10, 2000), 2),
        "timestamp": (now - timedelta(hours=random.randint(0, 24))).isoformat(),
        "location": random.choice(locations)
    }

if __name__ == "__main__":
    while True:
        transaction = generate_transaction()
        producer.send("transactions_topic", value=transaction)
        print(f"Sent: {transaction}")
        sleep(1)
