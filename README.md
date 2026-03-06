# Dynamic Storage Replication for Hive

By default, HDFS replicates every block 3 times regardless of how frequently the data is accessed. A table queried 1,000 times per second gets the same replication factor as one that hasn't been read in years. This project replaces that static policy with **dynamic, usage-based replication**, an external feedback system built on top of existing Hive/HDFS APIs.

Reference: https://github.com/big-data-europe/docker-hive

## Quick Start

Do Once

```bash
pip install -r requirements.txt
cd hive/hook
mvn clean package -DskipTests
cd ../..
```

Then

```bash
# Run testbench (all services will boot up)
cd scripts
python3 testbench.py

# stop containers
docker compose down
# OR
# stop and remove volumes (clean slate)
docker compose down -v
```

## Configuration

`config.yaml`:
| Parameter | Default | Description |
|---|---|---|
| `num_datanodes` | `5` | Number of HDFS datanodes to spin up (also the max replication factor) |
| `log_path` | `logs/access_log.jsonl` | Path to the access log written by the hook |
| `time_window_minutes` | `60` | Only count accesses within this window |
| `decay_factor` | `0.95` | Exponential decay per minute (0.95^10 min ≈ 0.60) |
| `heat_per_replica` | `10` | Heat score units per additional replica (`replication = ceil(heat / heat_per_replica)`, clamped to `[1, num_datanodes]`) |
| `warehouse_path` | `/user/hive/warehouse` | HDFS path to the Hive warehouse directory |
| `poll_interval_seconds` | `30` | Daemon cycle interval |
