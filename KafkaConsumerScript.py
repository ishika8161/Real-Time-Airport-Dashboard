from kafka import KafkaConsumer
from threading import Thread

# Kafka Consumer Setup
consumer = KafkaConsumer('flight_updates', bootstrap_servers='localhost:9092')

# Shared data structure to store messages
flight_data = []

# Function to consume messages and update the shared data structure
def consume_messages():
    global flight_data
    for message in consumer:
        decoded_message = message.value.decode('utf-8')
        flight_info = decoded_message.split(',')
        flight_data.append({
            'date': flight_info[0],
            'flight_number': flight_info[1],
            'origin': flight_info[2],
            'destination': flight_info[3],
            'dep_delay': flight_info[4],
            'arr_delay': flight_info[5],
            'cancelled': flight_info[6]
        })
        print(f"Consumed: {decoded_message}")

# Run the consumer in a separate thread
consumer_thread = Thread(target=consume_messages)
consumer_thread.start()
