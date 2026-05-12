#!/usr/bin/env python3

from pymongo import MongoClient
from config import MONGO_URI, DB_NAME, RAW_COLLECTION, CLEAN_COLLECTION
import json

def format_bytes(bytes_val):
    """Format bytes to human-readable format"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_val < 1024:
            return f"{bytes_val:.2f} {unit}"
        bytes_val /= 1024
    return f"{bytes_val:.2f} PB"

def print_section(title):
    """Print a formatted section header"""
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")

def monitor_shards():
    """Display shard cluster information"""
    client = MongoClient(MONGO_URI)
    admin = client.admin
    
    try:
        # Get shard list
        print_section("Cluster Shards")
        shards_info = admin.command("listShards")
        
        for shard in shards_info.get("shards", []):
            print(f"\nShard ID: {shard['_id']}")
            print(f"Hosts: {shard['host']}")
            if "tags" in shard:
                print(f"Tags: {shard['tags']}")
        
        # Get cluster status
        print_section("Cluster Status")
        try:
            status = admin.command("serverStatus")
            print(f"MongoDB Version: {status['version']}")
            print(f"Current Time: {status['localTime']}")
            print(f"Uptime: {status['uptime']} seconds")
        except Exception as e:
            print(f"Could not retrieve server status: {e}")
        
        # Check database sharding
        print_section("Database Sharding Status")
        db_info = admin.command("listDatabases")
        
        for db_entry in db_info.get("databases", []):
            if db_entry["name"] == DB_NAME:
                print(f"Database: {db_entry['name']}")
                print(f"Size: {format_bytes(db_entry.get('sizeOnDisk', 0))}")
                break
        
        # Check collection sharding
        print_section("Collection Sharding Details")
        config_db = client["config"]
        
        for collection_name in [RAW_COLLECTION, CLEAN_COLLECTION]:
            collection_ns = f"{DB_NAME}.{collection_name}"
            
            # Get collection sharding info
            collection_info = config_db.collections.find_one({"_id": collection_ns})
            if collection_info:
                print(f"\nCollection: {collection_name}")
                print(f"Shard Key: {collection_info.get('key', {})}")
                print(f"Sharded: {collection_info.get('noIdIndex', False)}")
            
            # Count documents by shard
            db = client[DB_NAME]
            collection = db[collection_name]
            
            total_docs = collection.count_documents({})
            print(f"Total Documents: {total_docs:,}")
            
            # Check if we can get chunk distribution
            chunks = list(config_db.chunks.find({"ns": collection_ns}))
            if chunks:
                chunk_by_shard = {}
                for chunk in chunks:
                    shard = chunk.get("shard", "unknown")
                    chunk_by_shard[shard] = chunk_by_shard.get(shard, 0) + 1
                
                print(f"Chunks by Shard:")
                for shard, count in sorted(chunk_by_shard.items()):
                    print(f"    - {shard}: {count} chunks")
        
        # Show MMSI distribution
        print_section("Data Distribution by Shard")
        db = client[DB_NAME]
        
        for collection_name in [RAW_COLLECTION, CLEAN_COLLECTION]:
            collection = db[collection_name]
            print(f"\nCollection: {collection_name}")
            
            # This query shows documents per shard by examining first few sharded documents
            try:
                pipeline = [
                    {"$group": {"_id": None, "total": {"$sum": 1}}},
                    {"$project": {"total": 1, "_id": 0}}
                ]
                result = list(collection.aggregate(pipeline))
                if result:
                    print(f"Total Documents: {result[0]['total']:,}")
                
                # Sample MMSI values to show distribution
                sample = list(collection.find({}, {"mmsi": 1}).limit(10))
                if sample:
                    mmsis = [s.get("mmsi") for s in sample if s.get("mmsi")]
                    print(f"Sample MMSIs (first 10): {mmsis}")
            except Exception as e:
                print(f"Could not retrieve distribution info: {e}")
        
        print_section("Summary")
        print(f"MongoDB sharded cluster is operational")
        print(f"Connected to: {MONGO_URI}")
        print(f"Database: {DB_NAME}")
        
    except Exception as e:
        print(f"\nError monitoring cluster: {e}")
        print("Make sure MongoDB is running and sharding is initialized.")
        print("Run: python init_sharding.py")
    finally:
        client.close()

if __name__ == "__main__":
    
    monitor_shards()
    
    print("\n")
