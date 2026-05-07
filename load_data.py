import csv
from datetime import datetime
from pymongo import MongoClient
from config import MONGO_URI, DB_NAME, RAW_COLLECTION, CSV_PATH, BATCH_SIZE, MAX_ROWS

def parse_float(x):
    try:
        return float(x) if x not in ("", None) else None
    except:
        return None

def parse_int(x):
    try:
        return int(float(x)) if x not in ("", None) else None
    except:
        return None

def parse_timestamp(x):
    try:
        return datetime.strptime(x, "%d/%m/%Y %H:%M:%S")
    except:
        return None

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
col = db[RAW_COLLECTION]

batch = []
count = 0

with open(CSV_PATH, "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        doc = {
            "ts": parse_timestamp(row["# Timestamp"] if "# Timestamp" in row else row["Timestamp"]),
            "mobile_type": row.get("Type of mobile"),
            "mmsi": parse_int(row.get("MMSI")),
            "lat": parse_float(row.get("Latitude")),
            "lon": parse_float(row.get("Longitude")),
            "nav_status": row.get("Navigational status"),
            "rot": parse_float(row.get("ROT")),
            "sog": parse_float(row.get("SOG")),
            "cog": parse_float(row.get("COG")),
            "heading": parse_float(row.get("Heading")),
            "imo": parse_int(row.get("IMO")),
            "callsign": row.get("Callsign"),
            "name": row.get("Name"),
            "ship_type": row.get("Ship type"),
            "cargo_type": row.get("Cargo type"),
            "width": parse_float(row.get("Width")),
            "length": parse_float(row.get("Length")),
            "pos_fix_type": row.get("Type of position fixing device"),
            "draught": parse_float(row.get("Draught")),
            "destination": row.get("Destination"),
            "eta": row.get("ETA"),
            "data_source_type": row.get("Data source type"),
            "a": parse_float(row.get("A")),
            "b": parse_float(row.get("B")),
            "c": parse_float(row.get("C")),
            "d": parse_float(row.get("D")),
        }

        batch.append(doc)
        count += 1

        if len(batch) >= BATCH_SIZE:
            col.insert_many(batch)
            batch = []

        if count >= MAX_ROWS:
            break

if batch:
    col.insert_many(batch)

print(f"Inserted {count} rows")