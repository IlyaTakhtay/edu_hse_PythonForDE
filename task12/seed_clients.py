import psycopg2

connection = psycopg2.connect(
    dbname="kafka",
    user="kafka_adm",
    password="1234q",
    host="localhost",
    port="5432"
)

cursor = connection.cursor()

clients_data = [
    (123, 'John Doe', 'NewYork', 'LosAngeles'),
    (456, 'Jane Smith', 'Chicago', 'Chicago'),
    (789, 'Alice Brown', 'Houston', 'Phoenix'),
    (101, 'Bob Johnson', 'Phoenix', 'Phoenix'),
    (202, 'Eve Davis', 'LosAngeles', 'NewYork'),
]

cursor.executemany(
    "INSERT INTO clients (user_id, name, registration_address, last_known_location) VALUES (%s, %s, %s, %s)",
    clients_data
)

connection.commit()
cursor.close()
connection.close()
