# Dynamic Storage Replication for Hive

By default, HDFS replicates every block 3 times regardless of how frequently the data is accessed. A table queried 1,000 times per second gets the same replication factor as one that hasn't been read in years. This project replaces that static policy with **dynamic, usage-based replication**, an external feedback system built on top of existing Hive/HDFS APIs.

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
# Boot up services wait for (~1.5 minutes)
docker compose up -d

#Demo
./scripts/demo.sh

# stop containers
docker compose down
# stop and remove volumes (clean slate) 
docker compose down -v        
```

## Configuration

Note: DataNodes are by default max 3 for a max of 3 replications but are configurable in docker-compose.yml

`scripts/config.yaml`:
| Parameter | Default | Description |
|---|---|---|
| `log_path` | `/opt/logs/access_log.jsonl` | Path to the access log written by the hook |
| `time_window_minutes` | `60` | Only count accesses within this window |
| `decay_factor` | `0.95` | Exponential decay per minute (0.95^10 min = 0.60) |
| `thresholds.hot` | `50` | Heat score threshold for "hot" classification |
| `thresholds.warm` | `10` | Heat score threshold for "warm" classification |
| `replication.hot` | `3` | Replication factor for hot tables |
| `replication.warm` | `2` | Replication factor for warm tables |
| `replication.cold` | `1` | Replication factor for cold tables |
| `min_replication` | `1` | Floor for replication factor |
| `max_replication` | `3` | Ceiling for replication factor |
| `poll_interval_seconds` | `30` | Daemon cycle interval |