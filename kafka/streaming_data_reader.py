"""
Streaming data consumer
"""
from datetime import datetime
from kafka import KafkaConsumer
from psycopg2 import connect

TOPIC = 'toll'
DATABASE = 'tolldata'
USERNAME = 'postgres'
PASSWORD = 'postgres'

print("Connecting to the database...")
try:
    connection = connect(host='localhost', dbname=DATABASE, user=USERNAME, password=PASSWORD)
except Exception:
    print("Could not connect to database. Please check credentials.")
else:
    print("Connected to database")

cursor = connection.cursor()

print("Connecting to Kafka...")
# consumer
consumer = KafkaConsumer(TOPIC)
print("Connected to Kafka.")
print(f"Reading messages from the topic {TOPIC}...")

for msg in consumer:
    # reading information from kafka
    message = msg.value.decode("utf-8")

    # extracting fields from the message
    (vehicle_id, vehicle_type, toll_plaza_id, timestamp) = message.split(",")

    # transforming the date format to suit the database schema
    dateobj = datetime.strptime(timestamp, '%a %b %d %H:%M:%S %Y')
    timestamp = dateobj.strftime("%Y-%m-%d %H:%M:%S")

    # loading data into the postgre database table
    sql = "insert into livedata values(%s,%s,%s,%s)"
    result = cursor.execute(sql, (vehicle_id, vehicle_type, toll_plaza_id, timestamp))
    print(f"A {vehicle_type} was inserted into the database")
    connection.commit()
connection.close()
