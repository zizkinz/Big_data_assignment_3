#!/usr/bin/env python3

import time
from pathlib import Path

import matplotlib.pyplot as plt
from pymongo import MongoClient

from config import MONGO_URI, DB_NAME, DELTA_COLLECTION


def main():
    output_dir = Path("output")
    output_dir.mkdir(parents=True, exist_ok=True)

    start_time = time.perf_counter()

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=60000,
        connectTimeoutMS=30000,
        socketTimeoutMS=60000,
    )
    db = client[DB_NAME]
    col = db[DELTA_COLLECTION]

    cursor = col.find(
        {"delta_t_ms": {"$type": "number", "$gte": 0}},
        {"_id": 0, "delta_t_ms": 1}
    )

    deltas = [doc["delta_t_ms"] for doc in cursor]
    client.close()

    plt.figure(figsize=(12, 6))
    plt.hist(deltas, bins=50, range=(0, 1000000), log=True)
    plt.xlabel("Delta-t (milliseconds)")
    plt.ylabel("Count (log scale)")
    plt.title("AIS Update Frequency Distribution")
    plt.tight_layout()
    plt.savefig(output_dir / "ais_histogram.png", dpi=300, bbox_inches="tight")
    plt.close()

    end_time = time.perf_counter()

    print(
        f"Plotted {len(deltas):,} delta-t values. "
        f"Took {end_time - start_time:.2f} seconds."
    )


if __name__ == "__main__":
    main()