#!/usr/bin/env python3
"""
Quick start script to set up MongoDB sharding cluster
Run: python setup_cluster.py
"""

import subprocess
import time
import sys
import os

def run_command(cmd, description):
    """Run a command and report results"""
    print(f"\n{'='*60}")
    print(f"▶ {description}")
    print(f"{'='*60}")
    print(f"$ {cmd}")
    print()
    
    result = subprocess.run(cmd, shell=True)
    if result.returncode != 0:
        print(f"\n✗ Command failed: {cmd}")
        return False
    return True

def main():
    print("""
╔════════════════════════════════════════════════════════════╗
║  MongoDB Sharding Cluster - Quick Start Setup             ║
║  AIS Data Analysis - Big Data Assignment 3                ║
╚════════════════════════════════════════════════════════════╝
    """)
    
    # Step 1: Start Docker containers
    if not run_command(
        "docker-compose up -d",
        "Step 1/4: Starting MongoDB cluster containers..."
    ):
        print("\n✗ Failed to start Docker containers")
        sys.exit(1)
    
    print("⏳ Waiting for MongoDB services to be ready (30 seconds)...")
    time.sleep(30)
    
    # Step 2: Initialize replica sets and sharding
    if not run_command(
        "python init_sharding.py",
        "Step 2/4: Initializing replica sets and sharding..."
    ):
        print("\n✗ Failed to initialize sharding")
        sys.exit(1)
    
    print("⏳ Waiting for sharding to stabilize (10 seconds)...")
    time.sleep(10)
    
    # Step 3: Create indexes
    if not run_command(
        "python create_indicies.py",
        "Step 3/4: Creating database indexes..."
    ):
        print("\n✗ Failed to create indexes")
        sys.exit(1)
    
    # Step 4: Load data
    if not run_command(
        "python load_filter_data.py --cores 19",
        "Step 4/4: Loading and filtering AIS data..."
    ):
        print("\n✗ Failed to load data")
        sys.exit(1)
    
    # Summary
#     print(f"""
# ╔════════════════════════════════════════════════════════════╗
# ║  ✓ Setup Complete!                                        ║
# ╚════════════════════════════════════════════════════════════╝

# MongoDB Cluster Status:
#   📊 Shards: 3 (each with 3 replicas)
#   🔧 Config Servers: 3
#   🔀 Router: mongos at localhost:27017
  
# Database: ais
#   Collections:
#     • positions_raw (sharded by mmsi hashed)
#     • positions_clean (sharded by mmsi hashed)
#     • positions_dt

# Data loaded and distributed across shards!

# Next steps:
#   1. Monitor shard distribution: python monitor_shards.py
#   2. Query the data: python -c "from pymongo import MongoClient; print(MongoClient('mongodb://localhost:27017/ais')['positions_raw'].count_documents({}))"
#   3. View sharding status: mongosh "mongodb://localhost:27017/" --eval "db.printShardingStatus()"
#     """)

if __name__ == "__main__":
    main()
