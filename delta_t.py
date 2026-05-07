from pymongo import MongoClient
from config import MONGO_URI, DB_NAME, CLEAN_COLLECTION, DELTA_COLLECTION

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

result = db[CLEAN_COLLECTION].aggregate(pipeline, allowDiskUse=True)
print("Delta-t complete")