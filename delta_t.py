#!/usr/bin/env python3

import multiprocessing as mp
import time

from pymongo import MongoClient

from config import MONGO_URI, DB_NAME, CLEAN_COLLECTION, DELTA_COLLECTION


def build_mongo_client():
    return MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=60000,
        connectTimeoutMS=30000,
        socketTimeoutMS=60000,
        retryWrites=True,
    )


def ensure_delta_indexes():
    client = build_mongo_client()
    db = client[DB_NAME]
    db[DELTA_COLLECTION].create_index([("mmsi", 1), ("ts", 1)])
    db[DELTA_COLLECTION].create_index([("delta_t_ms", 1)])
    client.close()


def build_delta_pipeline(worker_id, num_workers):
    return [
        {
            "$match": {
                "mmsi": {"$type": "number", "$ne": None},
                "ts": {"$type": "date", "$ne": None},
                "$expr": {
                    "$eq": [
                        {"$mod": ["$mmsi", num_workers]},
                        worker_id
                    ]
                }
            }
        },
        {
            "$setWindowFields": {
                "partitionBy": "$mmsi",
                "sortBy": {"ts": 1, "_id": 1},
                "output": {
                    "prev_ts": {
                        "$shift": {
                            "output": "$ts",
                            "by": -1
                        }
                    }
                }
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
                "on": "_id",
                "whenMatched": "replace",
                "whenNotMatched": "insert"
            }
        }
    ]


def delta_worker(worker_id, num_workers, result_queue):
    client = build_mongo_client()
    db = client[DB_NAME]

    start = time.perf_counter()
    db[CLEAN_COLLECTION].aggregate(build_delta_pipeline(worker_id, num_workers), allowDiskUse=True)
    end = time.perf_counter()

    client.close()
    result_queue.put((worker_id, end - start))


def compute_delta_parallel(num_workers=3):
    ctx = mp.get_context("spawn")
    result_queue = ctx.SimpleQueue()

    client = build_mongo_client()
    db = client[DB_NAME]
    db[DELTA_COLLECTION].drop()
    client.close()

    ensure_delta_indexes()

    workers = []
    for worker_id in range(num_workers):
        p = ctx.Process(target=delta_worker, args=(worker_id, num_workers, result_queue))
        p.start()
        workers.append(p)

    stats = [result_queue.get() for _ in range(num_workers)]

    for p in workers:
        p.join()

    client = build_mongo_client()
    delta_docs = client[DB_NAME][DELTA_COLLECTION].count_documents({})
    client.close()

    return stats, delta_docs


if __name__ == "__main__":
    start_time = time.perf_counter()
    stats, delta_docs = compute_delta_parallel(num_workers=3)
    end_time = time.perf_counter()

    print("Delta-t complete")
    print(f"Worker timings: {sorted(stats)}")
    print(f"Documents written into {DELTA_COLLECTION}: {delta_docs:,}")
    print(f"Took {end_time - start_time:.2f} seconds.")