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
    
    result = subprocess.run(cmd, shell=True)
    if result.returncode != 0:
        print(f"\nCommand failed: {cmd}")
        return False
    return True

def main():
    
    # Step 1: Start Docker containers
    if not run_command(
        "docker-compose up -d",
        "Step 1/4: Starting MongoDB cluster containers..."
    ):
        print("\nFailed to start Docker containers")
        sys.exit(1)
    
    print("Waiting for MongoDB services to be ready (30 seconds)...")
    time.sleep(30)
    
    # Step 2: Initialize replica sets and sharding
    if not run_command(
        "python init_sharding.py",
        "Step 2/4: Initializing replica sets and sharding..."
    ):
        print("\nFailed to initialize sharding")
        sys.exit(1)
    
    print("Waiting for sharding to stabilize (10 seconds)...")
    time.sleep(10)
    
    # Step 3: Create indexes
    if not run_command(
        "python create_indicies.py",
        "Step 3/4: Creating database indexes..."
    ):
        print("\nFailed to create indexes")
        sys.exit(1)
    
    # Step 4: Load data
    if not run_command(
        "python load_filter_data.py",
        "Step 4/4: Loading and filtering AIS data..."
    ):
        print("\nFailed to load data")
        sys.exit(1)
    

if __name__ == "__main__":
    main()
