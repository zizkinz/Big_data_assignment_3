#!/usr/bin/env python3
"""
Initialize MongoDB sharding and replica sets
Run this script after all MongoDB containers are running
"""

import time
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure, ServerSelectionTimeoutError

def wait_for_mongo(uri, timeout=60, retries=30):
    """Wait for MongoDB to be ready"""
    for attempt in range(retries):
        try:
            client = MongoClient(uri, directConnection=True, serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            client.close()
            print(f"✓ Connected to {uri}")
            return True
        except (ServerSelectionTimeoutError, ConnectionFailure, Exception) as e:
            print(f"Attempt {attempt + 1}/{retries}: Waiting for MongoDB at {uri}...")
            time.sleep(timeout // retries)
    return False

def init_replica_set(mongod_uri, replica_set_name, members):
    """Initialize a replica set"""
    client = MongoClient(mongod_uri, directConnection=True, serverSelectionTimeoutMS=5000)
    admin_db = client.admin
    
    config = {
        "_id": replica_set_name,
        "members": members
    }
    
    try:
        admin_db.command("replSetInitiate", config)
        print(f"✓ Replica set '{replica_set_name}' initiated")
        time.sleep(5)  # Wait for replica set to initialize
        return True
    except OperationFailure as e:
        if "already initialized" in str(e):
            print(f"✓ Replica set '{replica_set_name}' already initialized")
            return True
        else:
            print(f"✗ Error initializing replica set '{replica_set_name}': {e}")
            return False
    finally:
        client.close()

def add_shard(mongos_uri, shard_uri, shard_name):
    """Add a shard to the cluster"""
    client = MongoClient(mongos_uri)
    admin_db = client.admin
    
    try:
        result = admin_db.command("addShard", shard_uri, name=shard_name)
        print(f"✓ Shard '{shard_name}' added: {result}")
        return True
    except OperationFailure as e:
        if "already exists" in str(e):
            print(f"✓ Shard '{shard_name}' already exists")
            return True
        else:
            print(f"✗ Error adding shard '{shard_name}': {e}")
            return False
    finally:
        client.close()

def enable_sharding(mongos_uri, db_name):
    """Enable sharding on a database"""
    client = MongoClient(mongos_uri)
    admin_db = client.admin
    
    try:
        result = admin_db.command("enableSharding", db_name)
        print(f"✓ Sharding enabled for database '{db_name}'")
        return True
    except OperationFailure as e:
        if "already enabled" in str(e):
            print(f"✓ Sharding already enabled for database '{db_name}'")
            return True
        else:
            print(f"✗ Error enabling sharding for '{db_name}': {e}")
            return False
    finally:
        client.close()

def create_shard_index(mongos_uri, db_name, collection_name, shard_key):
    """Create shard key index on collection"""
    client = MongoClient(mongos_uri)
    db = client[db_name]
    collection = db[collection_name]
    
    try:
        collection.create_index(shard_key)
        print(f"✓ Index created on {shard_key}")
        return True
    except Exception as e:
        print(f"✗ Error creating index: {e}")
        return False
    finally:
        client.close()

def shard_collection(mongos_uri, db_name, collection_name, shard_key):
    """Shard a collection"""
    client = MongoClient(mongos_uri)
    admin_db = client.admin
    
    collection_ns = f"{db_name}.{collection_name}"
    
    try:
        result = admin_db.command("shardCollection", collection_ns, key=shard_key)
        print(f"✓ Collection '{collection_ns}' sharded with key {shard_key}")
        return True
    except OperationFailure as e:
        if "already sharded" in str(e):
            print(f"✓ Collection '{collection_ns}' already sharded")
            return True
        else:
            print(f"✗ Error sharding collection: {e}")
            return False
    finally:
        client.close()

def main():
    print("=" * 60)
    print("MongoDB Sharding & Replication Initialization")
    print("=" * 60)
    
    # MongoDB service URLs
    CONFIG_SERVERS = [
        ("mongodb://localhost:27019", "configReplSet", [
            {"_id": 0, "host": "config1:27019"},
            {"_id": 1, "host": "config2:27019"},
            {"_id": 2, "host": "config3:27019"},
        ]),
    ]
    
    SHARD_SERVERS = [
        ("mongodb://localhost:27029", "shard1ReplSet", [
            {"_id": 0, "host": "shard1-server1:27017"},
            {"_id": 1, "host": "shard1-server2:27017"},
            {"_id": 2, "host": "shard1-server3:27017"},
        ], "shard1ReplSet/shard1-server1:27017,shard1-server2:27017,shard1-server3:27017", "shard1"),
        ("mongodb://localhost:27023", "shard2ReplSet", [
            {"_id": 0, "host": "shard2-server1:27017"},
            {"_id": 1, "host": "shard2-server2:27017"},
            {"_id": 2, "host": "shard2-server3:27017"},
        ], "shard2ReplSet/shard2-server1:27017,shard2-server2:27017,shard2-server3:27017", "shard2"),
        ("mongodb://localhost:27026", "shard3ReplSet", [
            {"_id": 0, "host": "shard3-server1:27017"},
            {"_id": 1, "host": "shard3-server2:27017"},
            {"_id": 2, "host": "shard3-server3:27017"},
        ], "shard3ReplSet/shard3-server1:27017,shard3-server2:27017,shard3-server3:27017", "shard3"),
    ]
    
    MONGOS_URI = "mongodb://localhost:27117"
    DB_NAME = "ais"
    RAW_COLLECTION = "positions_raw"
    CLEAN_COLLECTION = "positions_clean"
    
    # Wait for config servers
    print("\n[1/6] Waiting for config servers...")
    for mongod_uri, _, _ in CONFIG_SERVERS:
        wait_for_mongo(mongod_uri)
    
    # Wait for shard servers
    print("\n[2/6] Waiting for shard servers...")
    for mongod_uri, _, _, _, _ in SHARD_SERVERS:
        wait_for_mongo(mongod_uri)
    
    # Initialize config replica set
    print("\n[3/6] Initializing config server replica set...")
    for mongod_uri, rs_name, members in CONFIG_SERVERS:
        init_replica_set(mongod_uri, rs_name, members)
        time.sleep(3)
    
    # Initialize shard replica sets
    print("\n[4/6] Initializing shard replica sets...")
    for mongod_uri, rs_name, members, _, _ in SHARD_SERVERS:
        init_replica_set(mongod_uri, rs_name, members)
        time.sleep(3)
    
    # Wait for mongos
    print("\n[5/6] Waiting for mongos router...")
    wait_for_mongo(MONGOS_URI)
    time.sleep(5)
    
    # Add shards to cluster
    print("\n[6/6] Adding shards to cluster and configuring sharding...")
    for _, _, _, shard_uri, shard_name in SHARD_SERVERS:
        add_shard(MONGOS_URI, shard_uri, shard_name)
        time.sleep(2)
    
    # Enable sharding on database
    time.sleep(3)
    enable_sharding(MONGOS_URI, DB_NAME)
    time.sleep(2)
    
    # Create shard key index for RAW_COLLECTION
    create_shard_index(MONGOS_URI, DB_NAME, RAW_COLLECTION, {"mmsi": "hashed"})
    time.sleep(2)
    
    # Shard the RAW_COLLECTION by MMSI (hashed)
    shard_collection(MONGOS_URI, DB_NAME, RAW_COLLECTION, {"mmsi": "hashed"})
    time.sleep(2)
    
    # Create shard key index for CLEAN_COLLECTION
    create_shard_index(MONGOS_URI, DB_NAME, CLEAN_COLLECTION, {"mmsi": "hashed"})
    time.sleep(2)
    
    # Shard the CLEAN_COLLECTION by MMSI (hashed)
    shard_collection(MONGOS_URI, DB_NAME, CLEAN_COLLECTION, {"mmsi": "hashed"})
    
    print("\n" + "=" * 60)
    print("✓ MongoDB Sharding Setup Complete!")
    print("=" * 60)
    print(f"\nAccess MongoDB at: {MONGOS_URI}")
    print(f"Database: {DB_NAME}")
    print(f"Collections sharded by MMSI (hashed): {RAW_COLLECTION}, {CLEAN_COLLECTION}")

if __name__ == "__main__":
    main()
