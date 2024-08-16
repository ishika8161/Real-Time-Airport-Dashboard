from kafka import KafkaProducer
import csv
import time

# Kafka Producer Setup
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Read the CSV file and send messages to Kafka topic 'flight_updates'
with open('flights.csv', 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        message = f"{row['FL_DATE']},{row['FL_NUMBER']},{row['ORIGIN']},{row['DEST']},{row['DEP_DELAY']},{row['ARR_DELAY']},{row['CANCELLED']}"
        producer.send('flight_updates', message.encode('utf-8'))
        print(f"Sent: {message}")
        time.sleep(1)  # Simulate real-time data streaming

producer.flush()
