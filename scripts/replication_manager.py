#!/usr/bin/env python3

import subprocess
import sys
import logging
from datetime import datetime, timezone

from heat_tracker import get_heat_report, load_config, desired_replication

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("replication_manager")


def table_to_hdfs_path(table_name, config):
    warehouse = config.get("warehouse_path", "/user/hive/warehouse")
    parts = table_name.split(".", 1)

    if len(parts) == 2:
        db, table = parts
    else:
        db, table = "default", parts[0]

    if db == "default":
        return f"{warehouse}/{table}"
    return f"{warehouse}/{db}.db/{table}"


def run_hdfs_command(args, container="namenode"):
    cmd = ["docker", "exec", container] + args
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.stdout.strip(), result.stderr.strip(), result.returncode


def get_current_replication(hdfs_path):
    _, _, rc = run_hdfs_command(["hdfs", "dfs", "-test", "-e", hdfs_path])
    if rc != 0:
        return None

    stdout, _, rc = run_hdfs_command(["hdfs", "dfs", "-ls", hdfs_path])
    if rc != 0:
        return None

    # hdfs -ls columns: perms, replication, owner, group, size, date, time, path
    for line in stdout.splitlines():
        parts = line.split()
        if len(parts) >= 8 and not line.startswith("Found"):
            try:
                return int(parts[1])
            except (ValueError, IndexError):
                continue
    return None


def set_replication(hdfs_path, factor):
    _, stderr, rc = run_hdfs_command(
        ["hdfs", "dfs", "-setrep", "-w", str(factor), hdfs_path]
    )
    if rc != 0:
        log.error("Failed to set replication for %s: %s", hdfs_path, stderr)
        return False
    log.info("Set replication for %s to %d", hdfs_path, factor)
    return True


def discover_hive_tables(container="hive-server"):
    result = subprocess.run(
        ["docker", "exec", container,
         "/opt/hive/bin/beeline",
         "-u", "jdbc:hive2://localhost:10000",
         "--silent=true", "--outputformat=csv2",
         "-e", "SHOW TABLES"],
        capture_output=True, text=True,
    )

    tables = []
    if result.returncode != 0:
        log.warning("Could not discover Hive tables: %s", result.stderr.strip())
        return tables

    for line in result.stdout.strip().splitlines():
        name = line.strip().strip("'\"")
        if not name or name.lower() == "tab_name":
            continue
        if "jdbc:hive2" in name or name.startswith("0:") or name.startswith("+"):
            continue
        tables.append(f"default.{name}")

    return tables


def adjust_replications(config_path="config.yaml"):
    config = load_config(config_path)
    report = get_heat_report(config_path)
    tables_in_report = {entry["table"] for entry in report}

    # tables with no log entries are cold -> replication 1
    for table in discover_hive_tables():
        if table not in tables_in_report:
            report.append({
                "table": table,
                "heat": 0.0,
                "desired_replication": desired_replication(0, config),
            })

    changes = []
    for entry in report:
        table = entry["table"]
        if table.startswith("_dummy_"):
            continue

        desired = entry["desired_replication"]
        hdfs_path = table_to_hdfs_path(table, config)
        current = get_current_replication(hdfs_path)

        if current is None:
            log.warning("Skipping %s — could not read current replication", table)
            continue

        if current == desired:
            log.info("%-40s heat=%-8.2f rep=%d (no change)", table, entry["heat"], current)
            continue

        log.info("%-40s heat=%-8.2f rep=%d -> %d", table, entry["heat"], current, desired)
        if set_replication(hdfs_path, desired):
            changes.append({
                "table": table,
                "hdfs_path": hdfs_path,
                "old_replication": current,
                "new_replication": desired,
                "heat": entry["heat"],
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })

    return changes


if __name__ == "__main__":
    cfg_path = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"
    changes = adjust_replications(cfg_path)

    if changes:
        print(f"\nReplication changes applied: {len(changes)}")
        for c in changes:
            print(f"  {c['table']}: {c['old_replication']} -> {c['new_replication']} "
                  f"(heat={c['heat']:.2f})")
    else:
        print("No replication changes needed.")
