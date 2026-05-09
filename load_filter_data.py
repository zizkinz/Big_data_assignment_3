import csv
import math
import multiprocessing as mp
import re
import time
from datetime import datetime
from pathlib import Path

from pymongo import MongoClient

from config import (
    MONGO_URI,
    DB_NAME,
    RAW_COLLECTION,
    CLEAN_COLLECTION,
    BATCH_SIZE,
    MAX_ROWS,
)

SEQUENCE_INVALID = {
    "123456789", "987654321", "111111111", "222222222", "333333333",
    "444444444", "555555555", "666666666", "777777777", "888888888", "999999999"
}
MMSI_REGEX = re.compile(r"^\d{9}$")


def build_mongo_client():
    return MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=10000,
        connectTimeoutMS=10000,
        retryWrites=True,
    )


def wait_for_mongo(timeout_seconds=120, poll_seconds=5):
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            client = build_mongo_client()
            client.admin.command("ping")
            client.close()
            return True
        except Exception:
            time.sleep(poll_seconds)
    return False


def calculate_maritime_distance(lat1, lon1, lat2, lon2):
    radius_nm = 3440.065
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return radius_nm * c


def parse_timestamp(time_str):
    try:
        return datetime.strptime(time_str.strip(), "%d/%m/%Y %H:%M:%S")
    except Exception:
        return None


def parse_float(x):
    try:
        x = x.strip()
        return float(x) if x != "" else None
    except Exception:
        return None


def parse_int(x):
    try:
        x = x.strip()
        return int(float(x)) if x != "" else None
    except Exception:
        return None


def is_valid_row(
    row,
    mmsi_column,
    lat_column,
    lon_column,
    nav_status_column,
    rot_column,
    sog_column,
    cog_column,
    heading_column,
):
    try:
        lat = float(row.get(lat_column, ""))
        lon = float(row.get(lon_column, ""))
        if lat == 0 or lon == 0 or abs(lat) > 90 or abs(lon) > 180:
            return False
        if calculate_maritime_distance(lat, lon, 55.70, 12.66) > 540:
            return False
    except (ValueError, TypeError):
        return False

    mmsi = row.get(mmsi_column, "").strip()
    if not MMSI_REGEX.fullmatch(mmsi):
        return False
    if mmsi == mmsi[0] * 9:
        return False
    if mmsi in SEQUENCE_INVALID:
        return False

    nav_status = row.get(nav_status_column, "").strip()
    if nav_status == "" or nav_status.lower() in {"unknown", "undefined", "not available"}:
        return False

    try:
        rot = float(row.get(rot_column, ""))
        if rot == -128 or rot < -127 or rot > 127:
            return False
    except (ValueError, TypeError):
        return False

    try:
        sog = float(row.get(sog_column, ""))
        if sog < 0 or sog >= 102.3:
            return False
    except (ValueError, TypeError):
        return False

    try:
        cog = float(row.get(cog_column, ""))
        if cog < 0 or cog >= 360:
            return False
    except (ValueError, TypeError):
        return False

    try:
        heading = float(row.get(heading_column, ""))
        if heading == 511 or heading < 0 or heading > 359:
            return False
    except (ValueError, TypeError):
        return False

    return True


def row_to_document(
    row,
    timestamp_column,
    mobile_type_column,
    mmsi_column,
    lat_column,
    lon_column,
    nav_status_column,
    rot_column,
    sog_column,
    cog_column,
    heading_column,
    imo_column,
    callsign_column,
    name_column,
    ship_type_column,
    cargo_type_column,
    width_column,
    length_column,
    pos_fix_type_column,
    draught_column,
    destination_column,
    eta_column,
    data_source_type_column,
    a_column,
    b_column,
    c_column,
    d_column,
):
    return {
        "ts": parse_timestamp(row.get(timestamp_column, "")),
        "mobile_type": row.get(mobile_type_column, "").strip(),
        "mmsi": parse_int(row.get(mmsi_column, "")),
        "lat": parse_float(row.get(lat_column, "")),
        "lon": parse_float(row.get(lon_column, "")),
        "nav_status": row.get(nav_status_column, "").strip(),
        "rot": parse_float(row.get(rot_column, "")),
        "sog": parse_float(row.get(sog_column, "")),
        "cog": parse_float(row.get(cog_column, "")),
        "heading": parse_float(row.get(heading_column, "")),
        "imo": parse_int(row.get(imo_column, "")),
        "callsign": row.get(callsign_column, "").strip(),
        "name": row.get(name_column, "").strip(),
        "ship_type": row.get(ship_type_column, "").strip(),
        "cargo_type": row.get(cargo_type_column, "").strip(),
        "width": parse_float(row.get(width_column, "")),
        "length": parse_float(row.get(length_column, "")),
        "pos_fix_type": row.get(pos_fix_type_column, "").strip(),
        "draught": parse_float(row.get(draught_column, "")),
        "destination": row.get(destination_column, "").strip(),
        "eta": row.get(eta_column, "").strip(),
        "data_source_type": row.get(data_source_type_column, "").strip(),
        "a": parse_float(row.get(a_column, "")),
        "b": parse_float(row.get(b_column, "")),
        "c": parse_float(row.get(c_column, "")),
        "d": parse_float(row.get(d_column, "")),
    }


def flush_insert_many(collection, docs):
    if docs:
        collection.insert_many(docs, ordered=False)


def load_worker(worker_id, task_queue, result_queue, column_map):
    client = build_mongo_client()
    db = client[DB_NAME]
    raw_col = db[RAW_COLLECTION]

    rows_seen = 0
    rows_inserted = 0
    docs_batch = []

    while True:
        payload = task_queue.get()
        if payload is None:
            break

        for row in payload:
            rows_seen += 1

            if is_valid_row(
                row,
                column_map["mmsi"],
                column_map["lat"],
                column_map["lon"],
                column_map["nav_status"],
                column_map["rot"],
                column_map["sog"],
                column_map["cog"],
                column_map["heading"],
            ):
                doc = row_to_document(
                    row,
                    column_map["timestamp"],
                    column_map["mobile_type"],
                    column_map["mmsi"],
                    column_map["lat"],
                    column_map["lon"],
                    column_map["nav_status"],
                    column_map["rot"],
                    column_map["sog"],
                    column_map["cog"],
                    column_map["heading"],
                    column_map["imo"],
                    column_map["callsign"],
                    column_map["name"],
                    column_map["ship_type"],
                    column_map["cargo_type"],
                    column_map["width"],
                    column_map["length"],
                    column_map["pos_fix_type"],
                    column_map["draught"],
                    column_map["destination"],
                    column_map["eta"],
                    column_map["data_source_type"],
                    column_map["a"],
                    column_map["b"],
                    column_map["c"],
                    column_map["d"],
                )

                if (
                    doc["ts"] is not None
                    and doc["mmsi"] is not None
                    and doc["lat"] is not None
                    and doc["lon"] is not None
                ):
                    docs_batch.append(doc)

                if len(docs_batch) >= BATCH_SIZE:
                    flush_insert_many(raw_col, docs_batch)
                    rows_inserted += len(docs_batch)
                    docs_batch = []

    if docs_batch:
        flush_insert_many(raw_col, docs_batch)
        rows_inserted += len(docs_batch)

    client.close()
    result_queue.put((worker_id, rows_seen, rows_inserted))


def phase1_load_raw(input_files, column_map, num_workers):
    ctx = mp.get_context("spawn")
    task_queue = ctx.SimpleQueue()
    result_queue = ctx.SimpleQueue()

    workers = []
    for worker_id in range(num_workers):
        p = ctx.Process(
            target=load_worker,
            args=(worker_id, task_queue, result_queue, column_map)
        )
        p.start()
        workers.append(p)

    total_rows = 0
    batch = []

    for input_file in input_files:
        with open(input_file, "r", newline="", encoding="utf-8") as infile:
            reader = csv.DictReader(infile)

            for row in reader:
                total_rows += 1
                batch.append(row)

                if len(batch) >= BATCH_SIZE:
                    task_queue.put(batch[:])
                    batch.clear()

                if MAX_ROWS is not None and total_rows >= MAX_ROWS:
                    break

        if MAX_ROWS is not None and total_rows >= MAX_ROWS:
            break

    if batch:
        task_queue.put(batch[:])

    for _ in range(num_workers):
        task_queue.put(None)

    stats = [result_queue.get() for _ in range(num_workers)]

    for p in workers:
        p.join()

    total_seen = sum(x[1] for x in stats)
    total_inserted = sum(x[2] for x in stats)

    return total_rows, total_seen, total_inserted


def phase2_build_clean_with_aggregation():
    client = build_mongo_client()
    db = client[DB_NAME]
    raw_col = db[RAW_COLLECTION]
    clean_col = db[CLEAN_COLLECTION]

    clean_col.drop()

    pipeline = [
        {
            "$match": {
                "mmsi": {"$type": "number", "$ne": None},
                "lat": {"$type": "number", "$gte": -90, "$lte": 90},
                "lon": {"$type": "number", "$gte": -180, "$lte": 180},
                "nav_status": {
                    "$exists": True,
                    "$nin": [None, "", "Unknown", "Undefined", "not available"]
                },
                "rot": {"$type": "number", "$gte": -127, "$lte": 127, "$ne": -128},
                "sog": {"$type": "number", "$gte": 0, "$lt": 102.3},
                "cog": {"$type": "number", "$gte": 0, "$lt": 360},
                "heading": {"$type": "number", "$gte": 0, "$lte": 359, "$ne": 511},
            }
        },
        {
            "$group": {
                "_id": "$mmsi",
                "count": {"$sum": 1},
                "docs": {"$push": "$$ROOT"}
            }
        },
        {
            "$match": {
                "count": {"$gte": 100}
            }
        },
        {
            "$unwind": "$docs"
        },
        {
            "$replaceRoot": {
                "newRoot": "$docs"
            }
        },
        {
            "$merge": {
                "into": CLEAN_COLLECTION,
                "whenMatched": "replace",
                "whenNotMatched": "insert"
            }
        }
    ]

    raw_col.aggregate(pipeline, allowDiskUse=True)

    valid_vessels = len(list(raw_col.aggregate([
        {
            "$match": {
                "mmsi": {"$type": "number", "$ne": None},
                "lat": {"$type": "number", "$gte": -90, "$lte": 90},
                "lon": {"$type": "number", "$gte": -180, "$lte": 180},
                "nav_status": {
                    "$exists": True,
                    "$nin": [None, "", "Unknown", "Undefined", "not available"]
                },
                "rot": {"$type": "number", "$gte": -127, "$lte": 127, "$ne": -128},
                "sog": {"$type": "number", "$gte": 0, "$lt": 102.3},
                "cog": {"$type": "number", "$gte": 0, "$lt": 360},
                "heading": {"$type": "number", "$gte": 0, "$lte": 359, "$ne": 511},
            }
        },
        {
            "$group": {
                "_id": "$mmsi",
                "count": {"$sum": 1}
            }
        },
        {
            "$match": {
                "count": {"$gte": 100}
            }
        },
        {
            "$count": "vessel_count"
        }
    ], allowDiskUse=True)))

    clean_docs = clean_col.count_documents({})

    client.close()
    return valid_vessels, clean_docs


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--cores", type=int, default=max(1, min(4, mp.cpu_count() - 1)))
    args = parser.parse_args()

    num_workers = args.cores

    BASE_DIR = Path(__file__).resolve().parent
    DATA_DIR = BASE_DIR / "AIS_DATA"

    input_files = [
        DATA_DIR / "aisdk-2026-04-18.csv",
    ]

    for input_file in input_files:
        if not input_file.exists():
            raise FileNotFoundError(f"Input CSV not found: {input_file}")

    column_map = {
        "timestamp": "# Timestamp",
        "mobile_type": "Type of mobile",
        "mmsi": "MMSI",
        "lat": "Latitude",
        "lon": "Longitude",
        "nav_status": "Navigational status",
        "rot": "ROT",
        "sog": "SOG",
        "cog": "COG",
        "heading": "Heading",
        "imo": "IMO",
        "callsign": "Callsign",
        "name": "Name",
        "ship_type": "Ship type",
        "cargo_type": "Cargo type",
        "width": "Width",
        "length": "Length",
        "pos_fix_type": "Type of position fixing device",
        "draught": "Draught",
        "destination": "Destination",
        "eta": "ETA",
        "data_source_type": "Data source type",
        "a": "A",
        "b": "B",
        "c": "C",
        "d": "D",
    }

    print(f"System has {mp.cpu_count()} cores. Using {num_workers} worker processes.")
    print(f"Unified batch size from config: {BATCH_SIZE}")

    print(f"Waiting for MongoDB at {MONGO_URI}...")
    if not wait_for_mongo():
        raise RuntimeError(f"MongoDB is not reachable at {MONGO_URI}")

    start_time1 = time.perf_counter()
    total_rows_read, total_seen_p1, total_inserted_p1 = phase1_load_raw(
        input_files=input_files,
        column_map=column_map,
        num_workers=num_workers,
    )
    end_time1 = time.perf_counter()

    print(
        f"Phase 1 complete. Read {total_rows_read:,} CSV rows. "
        f"Workers processed {total_seen_p1:,} rows and inserted {total_inserted_p1:,} docs into {RAW_COLLECTION}. "
        f"Took {end_time1 - start_time1:.2f} seconds."
    )

    start_time2 = time.perf_counter()
    valid_vessels, clean_docs = phase2_build_clean_with_aggregation()
    end_time2 = time.perf_counter()

    print(
        f"Phase 2 complete. Aggregation kept vessels with >=100 rows. "
        f"Valid vessel groups: {valid_vessels:,}. "
        f"Documents written into {CLEAN_COLLECTION}: {clean_docs:,}. "
        f"Took {end_time2 - start_time2:.2f} seconds."
    )

    print(f"Overall execution time: {end_time2 - start_time1:.2f} seconds.")