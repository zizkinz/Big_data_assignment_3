#!/usr/bin/env python3

import time
from pymongo import MongoClient
from config import MONGO_URI, DB_NAME, RAW_COLLECTION, CLEAN_COLLECTION

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
raw_col = db[RAW_COLLECTION]
clean_col = db[CLEAN_COLLECTION]

start_time = time.perf_counter()

print("Creating indexes")

raw_col.create_index([("mmsi", 1), ("ts", 1)], name="mmsi_1_ts_1")

clean_col.create_index([("mmsi", 1), ("ts", 1)], name="mmsi_1_ts_1")

end_time = time.perf_counter()

print(f"Indexes created in {end_time - start_time:.2f} seconds.")

client.close()