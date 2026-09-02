"""
HappyBooking - Step 1: Bronze Ingest (Batch Split)
-----------------------------------------------------
This script reads the large booking_dirty.csv file in chunks
and splits it into a 70% batch file and a 30% stream file,
simulating a scenario where part of the data arrives as
historical batch data and part arrives as real-time events.

A small sample file is also created for the GitHub repo,
since the full raw file is too large to version control.
"""

import pandas as pd
import numpy as np
import os

# --- Config ---
INPUT_FILE = "data/booking_dirty.csv"
BATCH_FILE = "data/hotel_raw_batch.csv"
STREAM_FILE = "data/hotel_raw_stream.csv"
SAMPLE_FILE = "data/sample_hotel_booking.csv"

CHUNK_SIZE = 100_000   # rows read into memory per iteration
SPLIT_RATIO = 0.70     # 70% batch, 30% stream
RANDOM_SEED = 42       # for reproducible results
SAMPLE_ROWS = 1000     # rows to keep in the GitHub sample file

np.random.seed(RANDOM_SEED)

# Remove leftover files from previous runs
for f in [BATCH_FILE, STREAM_FILE, SAMPLE_FILE]:
    if os.path.exists(f):
        os.remove(f)

total_rows = 0
batch_rows = 0
stream_rows = 0
sample_written = False

print(f"Reading: {INPUT_FILE} ...")

reader = pd.read_csv(INPUT_FILE, chunksize=CHUNK_SIZE, low_memory=False)

for i, chunk in enumerate(reader):
    # Randomly assign each row to batch or stream
    mask = np.random.rand(len(chunk)) < SPLIT_RATIO
    batch_chunk = chunk[mask]
    stream_chunk = chunk[~mask]

    # Append to disk; write header only on the first chunk
    batch_chunk.to_csv(BATCH_FILE, mode="a", index=False, header=(i == 0))
    stream_chunk.to_csv(STREAM_FILE, mode="a", index=False, header=(i == 0))

    # Build the small sample file only once, from the first chunk
    if not sample_written:
        chunk.head(SAMPLE_ROWS).to_csv(SAMPLE_FILE, index=False)
        sample_written = True

    total_rows += len(chunk)
    batch_rows += len(batch_chunk)
    stream_rows += len(stream_chunk)

    print(f"  Chunk {i+1} processed | Total rows so far: {total_rows:,}")

print("\n--- Summary ---")
print(f"Total rows        : {total_rows:,}")
print(f"Batch rows (~70%)  : {batch_rows:,}")
print(f"Stream rows (~30%) : {stream_rows:,}")
print(f"Sample file        : {SAMPLE_FILE} ({SAMPLE_ROWS} rows)")
print("\nDone!")