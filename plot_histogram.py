from pymongo import MongoClient
import matplotlib.pyplot as plt
from config import MONGO_URI, DB_NAME, DELTA_COLLECTION
import time

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
col = db[DELTA_COLLECTION]

start_time = time.perf_counter()

deltas = [doc["delta_t_ms"] for doc in col.find({}, {"delta_t_ms": 1})]

plt.figure(figsize=(12, 6))
plt.hist(deltas, bins=50, range=(0, 120000), log=True)
plt.xlabel("Delta-t (milliseconds)")
plt.ylabel("Count (log scale)")
plt.title("AIS Update Frequency Distribution")
plt.savefig("output/ais_histogram.png", dpi=300, bbox_inches="tight")
plt.show()

end_time = time.perf_counter()

print(
    f"Plotted {len(deltas)} delta-t values""
    f"Took {end_time - start_time:.2f} seconds."
)
