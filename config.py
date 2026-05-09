# MongoDB Connection (mongos router for sharded cluster)
MONGO_URI = "mongodb://localhost:27117/"

# Database and Collections
DB_NAME = "ais"
RAW_COLLECTION = "positions_raw"
CLEAN_COLLECTION = "positions_clean"
DELTA_COLLECTION = "positions_dt"

# Sharding Configuration
SHARD_KEY = "mmsi"  # Shard by MMSI using hashed algorithm
SHARD_KEY_TYPE = "hashed"  # Use hashed shard key for even distribution

# Data Processing Configuration
CSV_PATH = "C:/Users/avark/OneDrive/Desktop/University/Master's degree/2 Semester/Big Data Anlysis/Task 3/AIS_DATA/aisdk-2026-04-18.csv"
BATCH_SIZE = 50_000
MAX_ROWS = None