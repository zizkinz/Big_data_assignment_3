# Big Data Assignment 3

MongoDB sharded-cluster project for processing AIS vessel movement data. The repository brings up a local sharded MongoDB cluster with Docker, loads and filters AIS CSV data, builds indexes, computes delta-t values, and generates a histogram of update intervals.

## Project Structure

- `docker-compose.yml` starts the MongoDB config servers, shard replica sets, and `mongos` router.
- `setup_cluster.py` runs the full pipeline in order.
- `init_sharding.py` initializes replica sets, registers shards, enables sharding, and shards the collections.
- `load_filter_data.py` imports AIS data from `AIS_DATA/aisdk-2026-04-18.csv`, validates rows, and writes raw and cleaned collections.
- `create_indicies.py` creates indexes on `mmsi` and `ts`.
- `delta_t.py` computes time gaps between consecutive vessel position reports and writes them to `positions_dt`.
- `plot_histogram.py` creates a histogram image from the delta-t collection.
- `monitor_shards.py` prints cluster and shard status information.
- `test_connection.py` exercises the cluster and can simulate a shard or member failure.
- `config.py` contains the shared database name, collection names, CSV path, and MongoDB URI.

## Requirements

- Docker Desktop or another Docker environment that supports Compose.
- Python 3.10+.
- Python packages: `pymongo` and `matplotlib`.

Install the Python dependencies with:

```bash
pip install pymongo matplotlib
```

## Cluster Ports

The router is exposed on `mongodb://127.0.0.1:27117/`.

Useful container ports include:

- Config servers: `27019`, `27020`, `27021`
- Shard 1: `27029`, `27018`, `27022`
- Shard 2: `27023`, `27024`, `27025`
- Shard 3: `27026`, `27027`, `27028`

If you already have MongoDB listening on `27017`, the project is configured to avoid that conflict by using `27117` for `mongos` on the host.

## Run Order

The easiest way to execute the full pipeline is:

```bash
python setup_cluster.py
```

That script performs the steps below in order:

1. Start the Docker containers.
2. Initialize the config replica set and shard replica sets.
3. Register the shards with `mongos`.
4. Enable sharding for the `ais` database and shard the `positions_raw` and `positions_clean` collections by hashed `mmsi`.
5. Load and filter the AIS CSV data.
6. Create indexes.
7. Compute delta-t values.
8. Generate the histogram.

You can also run the steps manually if you want to inspect intermediate results:

```bash
docker-compose up -d
python init_sharding.py
python load_filter_data.py
python create_indicies.py
python delta_t.py
python plot_histogram.py
```

## What Each Step Produces

- `load_filter_data.py` writes raw and cleaned AIS documents into the `ais` database.
- `create_indicies.py` creates compound indexes on `mmsi` and `ts` in both collections.
- `delta_t.py` creates `positions_dt` with a `delta_t_ms` field for consecutive reports.
- `plot_histogram.py` saves `output/ais_histogram.png`.

## Monitoring and Testing

Use `monitor_shards.py` to inspect shard membership, database status, collection sharding details, and basic distribution information.

Use `test_connection.py` to verify connectivity and simulate a failure scenario. The script can stop either a single container or all members of one shard, then restart them if configured to do so.

## Data Notes

- The sample input file is `AIS_DATA/aisdk-2026-04-18.csv`.
- The pipeline filters rows using vessel position, MMSI, status, and navigation constraints before writing cleaned data.
- The shard key is `mmsi` with the `hashed` strategy for even distribution.

## Troubleshooting

- If the cluster does not start, check that Docker is running and that host port `27117` is free.
- If `mongos` is reachable but the scripts fail, rerun `python init_sharding.py` after the containers are healthy.
- If a script cannot connect, confirm that `config.py` still points to `mongodb://127.0.0.1:27117/`.

## Outputs

- Histogram image: `output/ais_histogram.png`
- MongoDB collections: `positions_raw`, `positions_clean`, `positions_dt`