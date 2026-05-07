from pymongo import MongoClient
from config import MONGO_URI, DB_NAME, RAW_COLLECTION

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
col = db[RAW_COLLECTION]

# Single indexes
col.create_index("mmsi")
col.create_index("ts")

# Compound index for time-ordered vessel queries
col.create_index([("mmsi", 1), ("ts", 1)])

print("Indexes created")
print(col.index_information())