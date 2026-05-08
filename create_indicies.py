from pymongo import MongoClient
from config import MONGO_URI, DB_NAME, RAW_COLLECTION
import time

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
col = db[RAW_COLLECTION]

start_time = time.perf_counter()

# Single indexes
col.create_index("mmsi")
col.create_index("ts")

# Compound index for time-ordered vessel queries
col.create_index([("mmsi", 1), ("ts", 1)])

end_time = time.perf_counter()

print(
    f"Indexes created"
    f"Took {end_time - start_time:.2f} seconds."
)


print(col.index_information())