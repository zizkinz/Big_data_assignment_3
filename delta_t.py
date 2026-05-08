from pymongo import MongoClient
from config import MONGO_URI, DB_NAME, CLEAN_COLLECTION, DELTA_COLLECTION
import time

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

pipeline = [
    {"$sort": {"mmsi": 1, "ts": 1}},
    {
        "$setWindowFields": {
            "partitionBy": "$mmsi",
            "sortBy": {"ts": 1},
            "output": {"prev_ts": {"$shift": {"output": "$ts", "by": -1}}}
        }
    },
    {"$match": {"prev_ts": {"$ne": None}}},
    {
        "$addFields": {
            "delta_t_ms": {"$subtract": ["$ts", "$prev_ts"]}
        }
    },
    {
        "$merge": {
            "into": DELTA_COLLECTION,
            "whenMatched": "replace",
            "whenNotMatched": "insert"
        }
    }
]

start_time = time.perf_counter()

result = db[CLEAN_COLLECTION].aggregate(pipeline, allowDiskUse=True)

end_time = time.perf_counter()

print(
    f"Delta-t complete"
    f"Took {end_time - start_time:.2f} seconds."
)


