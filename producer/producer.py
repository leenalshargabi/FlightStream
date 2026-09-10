import json
import time

from kafka import KafkaProducer

from api.api import create_client, state_to_record

KAFKA_SERVERS = [
    "localhost:9092",
    "localhost:9093",
    "localhost:9094",
]

TOPIC = "flight_states"

# OpenSky API polling interval
POLL_INTERVAL = 120


def create_producer():
    """Create and return a Kafka producer."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def send_flights(producer, flights):
    """Send normalized flight records to Kafka."""
    sent_count = 0

    for flight in flights:
        try:
            producer.send(TOPIC, value=flight)
            sent_count += 1
        except Exception as error:
            print(f"Failed to send flight: {error}")

    # Make sure all buffered messages are actually sent.
    producer.flush()

    return sent_count


def main():
    print("FlightStream Producer")
    print("---------------------")

    # Connect to OpenSky.
    client = create_client()

    # Connect to Kafka.
    producer = create_producer()

    print("OpenSky client: connected")
    print("Kafka producer: connected")
    print(f"Kafka topic: {TOPIC}")
    print()

    try:
        while True:
            print("Requesting live aircraft data...")

            # get_states() returns normalized records from our API layer.
            response = client.get_states()

            raw_states = response.get("states", [])

            snapshot_time = response.get("time")

            flights = [
                record
                for state in raw_states
                if (record := state_to_record(state, snapshot_time)) is not None
            ]

            print(f"Aircraft received: {len(flights)}")

            if flights:
                sent_count = send_flights(producer, flights)

                print(f"Aircraft sent to Kafka: {sent_count}")
                print(f"Waiting {POLL_INTERVAL} seconds...")
            else:
                print("No aircraft data received.")

            print()

            time.sleep(POLL_INTERVAL)

    except KeyboardInterrupt:
        print("\nStopping FlightStream Producer...")

    finally:
        producer.flush()
        producer.close()
        client.close()

        print("Producer stopped.")


if __name__ == "__main__":
    main()