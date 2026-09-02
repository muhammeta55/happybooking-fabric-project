"""
HappyBooking - Step 2: Stream Producer
-----------------------------------------------------
Reads hotel_raw_stream.csv row by row and emits each row
as a JSON "event", with a small delay between rows, to
simulate a real-time booking system generating live events.

In this first version, events are printed to stdout so we
can verify the producer logic locally. In Step 3, this will
be pointed at a Fabric Eventstream custom endpoint instead.
"""

import pandas as pd
import json
import time
import os

STREAM_FILE = os.environ.get("STREAM_FILE", "data/hotel_raw_stream.csv")
EVENTS_PER_SECOND = float(os.environ.get("EVENTS_PER_SECOND", "5"))
DELAY = 1.0 / EVENTS_PER_SECOND

def main():
    print(f"Starting stream producer from: {STREAM_FILE}")
    print(f"Rate: {EVENTS_PER_SECOND} events/second")

    # chunksize=1 keeps memory flat even for large files,
    # since we only ever hold one row in memory at a time
    reader = pd.read_csv(STREAM_FILE, chunksize=1)

    count = 0
    for chunk in reader:
        row = chunk.iloc[0].to_dict()
        event = json.dumps(row, default=str)

        print(event)

        count += 1
        if count % 100 == 0:
            print(f"--- {count} events sent ---")

        time.sleep(DELAY)

if __name__ == "__main__":
    main()