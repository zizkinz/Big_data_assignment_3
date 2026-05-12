#!/usr/bin/env python3

import time
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure, ServerSelectionTimeoutError


CONFIG_RS_NAME = "configReplSet"

CONFIG_SERVERS = [
    {"_id": 0, "host": "config1:27019"},
    {"_id": 1, "host": "config2:27019"},
    {"_id": 2, "host": "config3:27019"},
]

SHARDS = [
    {
        "seed_uri": "mongodb://localhost:27029",
        "rs_name": "shard1ReplSet",
        "members": [
            {"_id": 0, "host": "shard1-server1:27017"},
            {"_id": 1, "host": "shard1-server2:27017"},
            {"_id": 2, "host": "shard1-server3:27017"},
        ],
        "add_shard_uri": "shard1ReplSet/shard1-server1:27017,shard1-server2:27017,shard1-server3:27017",
        "shard_name": "shard1",
    },
    {
        "seed_uri": "mongodb://localhost:27023",
        "rs_name": "shard2ReplSet",
        "members": [
            {"_id": 0, "host": "shard2-server1:27017"},
            {"_id": 1, "host": "shard2-server2:27017"},
            {"_id": 2, "host": "shard2-server3:27017"},
        ],
        "add_shard_uri": "shard2ReplSet/shard2-server1:27017,shard2-server2:27017,shard2-server3:27017",
        "shard_name": "shard2",
    },
    {
        "seed_uri": "mongodb://localhost:27026",
        "rs_name": "shard3ReplSet",
        "members": [
            {"_id": 0, "host": "shard3-server1:27017"},
            {"_id": 1, "host": "shard3-server2:27017"},
            {"_id": 2, "host": "shard3-server3:27017"},
        ],
        "add_shard_uri": "shard3ReplSet/shard3-server1:27017,shard3-server2:27017,shard3-server3:27017",
        "shard_name": "shard3",
    },
]

MONGOS_URI = "mongodb://localhost:27117"

DB_NAME = "ais"
RAW_COLLECTION = "positions_raw"
CLEAN_COLLECTION = "positions_clean"


def wait_for_mongo(uri, retries=60, sleep_seconds=2, direct=True):
    for attempt in range(1, retries + 1):
        try:
            client = MongoClient(
                uri,
                directConnection=direct,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000,
                socketTimeoutMS=5000,
            )
            client.admin.command("ping")
            client.close()
            print(f"Connected to {uri}")
            return True
        except (ServerSelectionTimeoutError, ConnectionFailure, Exception):
            print(f"[{attempt}/{retries}] Waiting for MongoDB at {uri}")
            time.sleep(sleep_seconds)
    return False


def init_replica_set(seed_uri, replica_set_name, members, configsvr=False):
    client = MongoClient(seed_uri, directConnection=True, serverSelectionTimeoutMS=5000)
    admin = client.admin

    config = {
        "_id": replica_set_name,
        "members": members,
    }
    if configsvr:
        config["configsvr"] = True

    try:
        admin.command("replSetInitiate", config)
        print(f"Replica set '{replica_set_name}' initiated")
    except OperationFailure as e:
        msg = str(e)
        if "already initialized" in msg or "already been initialized" in msg:
            print(f"Replica set '{replica_set_name}' already initialized")
        else:
            client.close()
            raise
    finally:
        client.close()


def wait_for_replica_set(seed_uri, retries=60, sleep_seconds=2):
    for attempt in range(1, retries + 1):
        try:
            client = MongoClient(
                seed_uri,
                directConnection=True,
                serverSelectionTimeoutMS=10000,
                connectTimeoutMS=10000,
                socketTimeoutMS=10000,
            )
            hello = client.admin.command("hello")
            client.close()
            if hello.get("setName"):
                print(f"Replica set ready for {seed_uri} (primary: {hello.get('primary')})")
                return True
        except Exception:
            pass

        print(f"[{attempt}/{retries}] Waiting for replica set on {seed_uri}")
        time.sleep(sleep_seconds)

    return False


def add_shard(mongos_uri, shard_uri, shard_name):
    client = MongoClient(mongos_uri, serverSelectionTimeoutMS=10000)
    admin = client.admin

    try:
        result = admin.command("addShard", shard_uri, name=shard_name)
        print(f"Shard '{shard_name}' added: {result}")
    except OperationFailure as e:
        msg = str(e)
        if "already exists" in msg or "is already" in msg:
            print(f"Shard '{shard_name}' already exists")
        else:
            client.close()
            raise
    finally:
        client.close()


def enable_sharding(mongos_uri, db_name):
    client = MongoClient(mongos_uri, serverSelectionTimeoutMS=10000)
    admin = client.admin

    try:
        result = admin.command("enableSharding", db_name)
        print(f"Sharding enabled for database '{db_name}': {result}")
    except OperationFailure as e:
        msg = str(e)
        if "already enabled" in msg:
            print(f"Sharding already enabled for database '{db_name}'")
        else:
            client.close()
            raise
    finally:
        client.close()


def shard_collection(mongos_uri, namespace, shard_key):
    client = MongoClient(mongos_uri, serverSelectionTimeoutMS=10000)
    admin = client.admin

    try:
        result = admin.command("shardCollection", namespace, key=shard_key)
        print(f"Collection '{namespace}' sharded with key {shard_key}: {result}")
    except OperationFailure as e:
        msg = str(e)
        if "already sharded" in msg or "is already sharded" in msg:
            print(f"Collection '{namespace}' already sharded")
        else:
            client.close()
            raise
    finally:
        client.close()


def print_sharding_status(mongos_uri):
    client = MongoClient(mongos_uri, serverSelectionTimeoutMS=10000)
    try:
        shards = list(client.config.shards.find({}, {"_id": 1, "host": 1}))
        print("\nConfigured shards:")
        for shard in shards:
            print(f"  - {shard['_id']}: {shard['host']}")
    finally:
        client.close()


def main():

    print("\nWaiting for config server seed")
    if not wait_for_mongo("mongodb://localhost:27019", direct=True):
        raise RuntimeError("Config server seed not reachable")

    print("\nWaiting for shard seeds")
    for shard in SHARDS:
        if not wait_for_mongo(shard["seed_uri"], direct=True):
            raise RuntimeError(f"Shard seed not reachable: {shard['seed_uri']}")

    print("\nInitializing config replica set")
    init_replica_set(
        seed_uri="mongodb://localhost:27019",
        replica_set_name=CONFIG_RS_NAME,
        members=CONFIG_SERVERS,
        configsvr=True,
    )

    print("\nInitializing shard replica sets")
    for shard in SHARDS:
        init_replica_set(
            seed_uri=shard["seed_uri"],
            replica_set_name=shard["rs_name"],
            members=shard["members"],
            configsvr=False,
        )

    print("\nWaiting for shard primaries")
    for shard in SHARDS:
        if not wait_for_replica_set(shard["seed_uri"]):
            raise RuntimeError(f"Shard primary was not elected in time: {shard['rs_name']}")

    print("\nWaiting for mongos router")
    if not wait_for_mongo(MONGOS_URI, direct=False):
        raise RuntimeError("mongos is not reachable")

    print("\nAdding shards to the cluster")
    for shard in SHARDS:
        add_shard(MONGOS_URI, shard["add_shard_uri"], shard["shard_name"])
        time.sleep(1)

    print("\nEnabling sharding and sharding collections")
    enable_sharding(MONGOS_URI, DB_NAME)

    shard_collection(MONGOS_URI, f"{DB_NAME}.{RAW_COLLECTION}", {"mmsi": "hashed"})
    shard_collection(MONGOS_URI, f"{DB_NAME}.{CLEAN_COLLECTION}", {"mmsi": "hashed"})

    print_sharding_status(MONGOS_URI)

    print("Sharding setup complete:")
    print(f"mongos: {MONGOS_URI}")
    print(f"Database: {DB_NAME}")
    print(f"Sharded collections: {RAW_COLLECTION}, {CLEAN_COLLECTION}")
    print("Shard key: {mmsi: 'hashed'}")


if __name__ == "__main__":
    main()