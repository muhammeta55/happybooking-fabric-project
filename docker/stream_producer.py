"""
HappyBooking - Step 3: Stream Producer (Fabric Eventstream)
-----------------------------------------------------
Reads hotel_raw_stream.csv row by row and publishes each row as a
JSON event to a Fabric Eventstream custom endpoint, using the
Kafka-compatible protocol exposed by Event Hub. This simulates a
real-time booking system generating live events.
"""

import pandas as pd
import json
import time
import os
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

STREAM_FILE = os.environ.get("STREAM_FILE", "data/hotel_raw_stream.csv")
EVENTS_PER_SECOND = float(os.environ.get("EVENTS_PER_SECOND", "5"))
DELAY = 1.0 / EVENTS_PER_SECOND

BOOTSTRAP_SERVER = os.environ["EVENTSTREAM_BOOTSTRAP_SERVER"]
TOPIC = os.environ["EVENTSTREAM_TOPIC"]
CONNECTION_STRING = os.environ["EVENTSTREAM_CONNECTION_STRING"]


def build_producer():
    """
    Creates a Kafka producer configured to authenticate against
    Fabric Eventstream's Event Hub-backed Kafka endpoint.

    Event Hub's Kafka surface uses SASL_SSL with a fixed username
    of "$ConnectionString" and the full connection string as the
    password - this is Event Hub-specific, but everything else
    here is standard Kafka producer configuration.
    """
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVER,
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_plain_username="$ConnectionString",
        sasl_plain_password=CONNECTION_STRING,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )


def main():
    print(f"Starting stream producer from: {STREAM_FILE}")
    print(f"Rate: {EVENTS_PER_SECOND} events/second")
    print(f"Target topic: {TOPIC}")

    producer = build_producer()

    # chunksize=1 keeps memory flat even for large files,
    # since we only ever hold one row in memory at a time
    reader = pd.read_csv(STREAM_FILE, chunksize=1)

    count = 0
    for chunk in reader:
        row = chunk.iloc[0].to_dict()
        row = {k: (None if pd.isna(v) else str(v)) for k, v in row.items()}

        producer.send(TOPIC, value=row)

        count += 1
        if count % 100 == 0:
            producer.flush()  # make sure buffered events are actually sent
            print(f"--- {count} events sent ---")

        time.sleep(DELAY)

    producer.flush()
    print(f"\nDone. Total events sent: {count}")


if __name__ == "__main__":
    main()