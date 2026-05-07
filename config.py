MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "ais"

RAW_COLLECTION = "positions_raw"
CLEAN_COLLECTION = "positions_clean"
DELTA_COLLECTION = "positions_dt"

CSV_PATH = "./AIS_DATA/aisdk-2026-04-18.csv"
BATCH_SIZE = 5000
MAX_ROWS = 200000