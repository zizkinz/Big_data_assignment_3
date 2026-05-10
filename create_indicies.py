#!/usr/bin/env python3

import time
from pymongo import MongoClient
from config import MONGO_URI, DB_NAME, RAW_COLLECTION, CLEAN_COLLECTION

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
raw_col = db[RAW_COLLECTION]
clean_col = db[CLEAN_COLLECTION]

start_time = time.perf_counter()

print("Creating post-load secondary indexes...")

print("RAW collection:")
raw_col.create_index([("mmsi", 1), ("ts", 1)], name="mmsi_1_ts_1")
# Optional only if you do global time-range queries often:
# raw_col.create_index([("ts", 1)], name="ts_1")

print("CLEAN collection:")
clean_col.create_index([("mmsi", 1), ("ts", 1)], name="mmsi_1_ts_1")
# Optional only if you do global time-range queries often:
# clean_col.create_index([("ts", 1)], name="ts_1")

end_time = time.perf_counter()

print(f"\n✓ Secondary indexes created in {end_time - start_time:.2f} seconds.")

print(f"\n=== RAW Collection Indexes ===")
for name, info in raw_col.index_information().items():
    print(name, "->", info["key"])

print(f"\n=== CLEAN Collection Indexes ===")
for name, info in clean_col.index_information().items():
    print(name, "->", info["key"])

client.close()