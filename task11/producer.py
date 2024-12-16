from kafka import KafkaProducer
import json
from datetime import datetime
import random
from time import sleep

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

actions = ["login", "logout", "purchase", "click"]
users = [f"user_{i}" for i in range(1, 11)]

def generate_data():
    return {
        "user_id": random.choice(users),
        "action": random.choice(actions),
        "timestamp": datetime.now().isoformat()
    }

if __name__ == "__main__":
    while True:
        data = generate_data()
        producer.send("example_topic", value=data)
        print(f"Sent: {data}")
        sleep(1)
