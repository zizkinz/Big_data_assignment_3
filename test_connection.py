#!/usr/bin/env python3
"""
test_connection.py

Runs a simple sharded-cluster failure test against an already running cluster.
This version is hardcoded to stop the whole shard2 replica set, test connectivity,
then restart it.

Run directly from PyCharm:
    python test_connection.py
"""

import subprocess
import sys
import time

from pymongo import MongoClient
from pymongo.errors import PyMongoError

from config import MONGO_URI, DB_NAME, RAW_COLLECTION, CLEAN_COLLECTION

# ---------------------------
# Hardcoded test configuration
# ---------------------------
STOP_BALANCER = True
RESTART_AFTER = True
FAILURE_MODE = "shard"   # "member" or "shard"
TARGET = "shard2"        # hardcoded random choice

SHARD_MEMBER_GROUPS = {
    "shard1": ["shard1-server1", "shard1-server2", "shard1-server3"],
    "shard2": ["shard2-server1", "shard2-server2", "shard2-server3"],
    "shard3": ["shard3-server1", "shard3-server2", "shard3-server3"],
}


def run_command(cmd: str, fail_ok: bool = False) -> bool:
    result = subprocess.run(cmd, shell=True)
    if result.returncode != 0 and not fail_ok:
        print(f"Command failed: {cmd}")
        return False
    return result.returncode == 0


def docker_stop(container_name: str) -> bool:
    print(f"Stopping container: {container_name}")
    return run_command(f"docker stop {container_name}")


def docker_start(container_name: str) -> bool:
    print(f"Starting container: {container_name}")
    return run_command(f"docker start {container_name}")


def build_client() -> MongoClient:
    return MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=10000,
        connectTimeoutMS=10000,
        socketTimeoutMS=30000,
        retryWrites=True,
    )


def wait_for_mongos(timeout_seconds: int = 60, poll_seconds: int = 3) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            client = build_client()
            client.admin.command("ping")
            client.close()
            return True
        except Exception:
            time.sleep(poll_seconds)
    return False


def stop_balancer() -> None:
    try:
        client = build_client()
        result = client.admin.command({"balancerStop": 1, "maxTimeMS": 30000})
        print(f"Balancer stop result: {result}")
        client.close()
    except Exception as e:
        print(f"Warning: could not stop balancer: {e}")


def start_balancer() -> None:
    try:
        client = build_client()
        result = client.admin.command({"balancerStart": 1})
        print(f"Balancer start result: {result}")
        client.close()
    except Exception as e:
        print(f"Warning: could not start balancer: {e}")


def print_cluster_basics() -> None:
    try:
        client = build_client()
        hello = client.admin.command("hello")
        print("\n=== Cluster Basics ===")
        print(f"hello.ok: {hello.get('ok')}")
        print(f"hello.msg: {hello.get('msg')}")
        print(f"isWritablePrimary: {hello.get('isWritablePrimary')}")
        client.close()
    except Exception as e:
        print(f"Could not fetch cluster basics: {e}")


def try_basic_ops() -> None:
    print("\n=== Basic Ops Test ===")
    client = None
    try:
        client = build_client()
        db = client[DB_NAME]

        ping_result = client.admin.command("ping")
        print(f"Ping OK: {ping_result}")

        raw_est = db[RAW_COLLECTION].estimated_document_count()
        clean_est = db[CLEAN_COLLECTION].estimated_document_count()
        print(f"{RAW_COLLECTION} estimated count: {raw_est}")
        print(f"{CLEAN_COLLECTION} estimated count: {clean_est}")

        sample_raw = db[RAW_COLLECTION].find_one({}, {"_id": 1, "mmsi": 1, "ts": 1})
        sample_clean = db[CLEAN_COLLECTION].find_one({}, {"_id": 1, "mmsi": 1, "ts": 1})
        print(f"Sample RAW doc: {sample_raw}")
        print(f"Sample CLEAN doc: {sample_clean}")

    except PyMongoError as e:
        print(f"Basic ops failed: {e}")
    finally:
        if client:
            client.close()


def try_targeted_ops() -> None:
    print("\n=== Targeted Ops Test ===")
    client = None
    try:
        client = build_client()
        db = client[DB_NAME]

        sample = db[CLEAN_COLLECTION].find_one(
            {"mmsi": {"$exists": True, "$ne": None}},
            {"_id": 1, "mmsi": 1}
        )

        if not sample or "mmsi" not in sample:
            print("No sample MMSI found in clean collection.")
            return

        target_mmsi = sample["mmsi"]
        print(f"Testing targeted query for MMSI: {target_mmsi}")

        docs = list(
            db[CLEAN_COLLECTION]
            .find({"mmsi": target_mmsi}, {"_id": 1, "mmsi": 1, "ts": 1})
            .limit(5)
        )
        print(f"Targeted read returned {len(docs)} docs")

        test_doc = {
            "mmsi": target_mmsi,
            "test_connection_marker": True,
            "created_at_epoch": time.time(),
        }
        res = db["connection_test_tmp"].insert_one(test_doc)
        print(f"Targeted write succeeded, _id={res.inserted_id}")

    except PyMongoError as e:
        print(f"Targeted ops failed: {e}")
    finally:
        if client:
            client.close()


def stop_target() -> bool:
    if FAILURE_MODE == "member":
        return docker_stop(TARGET)

    if FAILURE_MODE == "shard":
        members = SHARD_MEMBER_GROUPS.get(TARGET)
        if not members:
            print(f"Unknown shard target: {TARGET}")
            return False
        ok = True
        for member in members:
            ok = docker_stop(member) and ok
        return ok

    print(f"Unknown failure mode: {FAILURE_MODE}")
    return False


def restart_target() -> bool:
    if FAILURE_MODE == "member":
        return docker_start(TARGET)

    if FAILURE_MODE == "shard":
        members = SHARD_MEMBER_GROUPS.get(TARGET)
        if not members:
            print(f"Unknown shard target: {TARGET}")
            return False
        ok = True
        for member in members:
            ok = docker_start(member) and ok
        return ok

    print(f"Unknown failure mode: {FAILURE_MODE}")
    return False


def main():
    print("=== MongoDB Sharded Cluster Failure Test ===")
    print(f"Mongo URI: {MONGO_URI}")
    print(f"Failure mode: {FAILURE_MODE}")
    print(f"Hardcoded target: {TARGET}")

    if not wait_for_mongos():
        print("mongos is not reachable before the test.")
        sys.exit(1)

    print_cluster_basics()

    if STOP_BALANCER:
        stop_balancer()

    print("\n=== Pre-failure checks ===")
    try_basic_ops()
    try_targeted_ops()

    print("\n=== Inducing failure ===")
    if not stop_target():
        print("Failed to stop target.")
        sys.exit(1)

    print("Waiting 15 seconds for cluster state to settle...")
    time.sleep(15)

    print("\n=== Post-failure checks ===")
    if wait_for_mongos(timeout_seconds=30, poll_seconds=3):
        print("mongos still reachable after failure.")
    else:
        print("mongos not reachable after failure.")

    print_cluster_basics()
    try_basic_ops()
    try_targeted_ops()

    if RESTART_AFTER:
        print("\n=== Restarting stopped target ===")
        if restart_target():
            print("Restart command issued successfully.")
            print("Waiting 20 seconds for recovery...")
            time.sleep(20)

            print("\n=== Post-recovery checks ===")
            if wait_for_mongos(timeout_seconds=60, poll_seconds=3):
                print("mongos reachable after recovery.")
            else:
                print("mongos still not reachable after recovery.")

            print_cluster_basics()
            try_basic_ops()
            try_targeted_ops()
        else:
            print("Failed to restart target.")

    if STOP_BALANCER:
        start_balancer()

    print("\n=== Test complete ===")


if __name__ == "__main__":
    main()