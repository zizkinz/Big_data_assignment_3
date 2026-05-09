from pymongo import MongoClient
from config import MONGO_URI, DB_NAME, RAW_COLLECTION, CLEAN_COLLECTION, SHARD_KEY
import time

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
raw_col = db[RAW_COLLECTION]
clean_col = db[CLEAN_COLLECTION]

start_time = time.perf_counter()

print("Creating indexes on RAW collection...")
# Single indexes
raw_col.create_index("mmsi")
raw_col.create_index("ts")

# Compound index for time-ordered vessel queries
raw_col.create_index([("mmsi", 1), ("ts", 1)])

# Geo-spatial index for location queries
raw_col.create_index([("lat", 1), ("lon", 1)])

print("Creating indexes on CLEAN collection...")
# Single indexes
clean_col.create_index("mmsi")
clean_col.create_index("ts")

# Compound index for time-ordered vessel queries
clean_col.create_index([("mmsi", 1), ("ts", 1)])

# Geo-spatial index for location queries
clean_col.create_index([("lat", 1), ("lon", 1)])

end_time = time.perf_counter()

print(f"✓ All indexes created in {end_time - start_time:.2f} seconds.")
print(f"\n=== RAW Collection Indexes ===")
print(raw_col.index_information())
print(f"\n=== CLEAN Collection Indexes ===")
print(clean_col.index_information())