from kafka import KafkaConsumer
import json
from collections import Counter

consumer = KafkaConsumer(
    "example_topic",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset="earliest"
)

action_counter = Counter()

if __name__ == "__main__":
    print("Consuming messages...")
    for message in consumer:
        data = message.value
        user_id = data["user_id"]
        action = data["action"]
        
        # Увеличиваем счетчик действий пользователя
        action_counter[user_id] += 1

        # Находим пользователей с наибольшим количеством действий
        top_users = action_counter.most_common(3)
        print("\nTop users by activity:")
        for user, count in top_users:
            print(f"{user}: {count} actions")
