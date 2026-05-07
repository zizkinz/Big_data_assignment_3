from pymongo import MongoClient
from config import MONGO_URI, DB_NAME, RAW_COLLECTION, CLEAN_COLLECTION

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

pipeline = [
    # Row-level validity filter
    {
        "$match": {
            "mmsi": {"$type": "number", "$ne": None},
            "lat": {"$type": "number", "$gte": -90, "$lte": 90},
            "lon": {"$type": "number", "$gte": -180, "$lte": 180},
            "nav_status": {"$exists": True, "$ne": ""},
            "rot": {"$type": "number"},
            "sog": {"$type": "number"},
            "cog": {"$type": "number"},
            "heading": {"$type": "number"},
            "mobile_type": {"$ne": "Base station"}
        }
    },
    # Vessel-level count filter
    {
        "$group": {
            "_id": "$mmsi",
            "count": {"$sum": 1},
            "docs": {"$push": "$$ROOT"}
        }
    },
    {
        "$match": {"count": {"$gte": 100}}
    },
    {
        "$unwind": "$docs"
    },
    {
        "$replaceRoot": {"newRoot": "$docs"}
    },
    # Write to new collection
    {
        "$merge": {
            "into": CLEAN_COLLECTION,
            "whenMatched": "replace",
            "whenNotMatched": "insert"
        }
    }
]

result = db[RAW_COLLECTION].aggregate(pipeline, allowDiskUse=True)
print("Filtering complete")