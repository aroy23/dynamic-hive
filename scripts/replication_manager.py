#!/usr/bin/env python3

import subprocess
import sys
import logging
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from heat_tracker import get_heat_report, load_config

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
        db = "default"
        table = parts[0]

    # "default" db lives directly under warehouse; others get a <db>.db subdirectory
    if db == "default":
        return f"{warehouse}/{table}"
    else:
        return f"{warehouse}/{db}.db/{table}"


def run_hdfs_command(args, container="namenode"):
    cmd = ["docker", "exec", container] + args
    log.debug("Running: %s", " ".join(cmd))

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    return result.stdout.strip(), result.stderr.strip(), result.returncode


def get_current_replication(hdfs_path):
    stdout, stderr, rc = run_hdfs_command(
        ["hdfs", "dfs", "-test", "-e", hdfs_path]
    )
    if rc != 0:
        log.warning("HDFS path does not exist: %s", hdfs_path)
        return None

    stdout, stderr, rc = run_hdfs_command(
        ["hdfs", "dfs", "-ls", hdfs_path]
    )
    if rc != 0:
        return None

    # `hdfs dfs -ls` output columns: perms, replication, owner, group, size, date, time, path
    for line in stdout.splitlines():
        parts = line.split()
        if len(parts) >= 8 and not line.startswith("Found"):
            try:
                return int(parts[1])
            except (ValueError, IndexError):
                continue

    return None


def set_replication(hdfs_path, factor):
    stdout, stderr, rc = run_hdfs_command(
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
        capture_output=True, text=True, timeout=60,
    )

    tables = []
    if result.returncode != 0:
        log.warning("Could not discover Hive tables: %s", result.stderr.strip())
        return tables

    for line in result.stdout.strip().splitlines():
        name = line.strip().strip("'\"")
        if not name:
            continue
        if name.lower() == "tab_name":
            continue
        # skip beeline connection banners / formatting lines
        if "jdbc:hive2" in name or name.startswith("0:") or name.startswith("+"):
            continue
        tables.append(f"default.{name}")

    return tables


def adjust_replications(config_path="config.yaml"):
    config = load_config(config_path)
    report = get_heat_report(config_path)

    min_rep = config.get("min_replication", 1)
    max_rep = config.get("max_replication", 3)
    cold_rep = config.get("replication", {}).get("cold", 1)

    tables_in_report = {entry["table"] for entry in report}

    # treat tables with no access log entries as cold
    all_tables = discover_hive_tables()
    for table in all_tables:
        if table not in tables_in_report:
            report.append({
                "table": table,
                "heat": 0.0,
                "classification": "cold",
                "desired_replication": cold_rep,
            })

    changes = []
    for entry in report:
        table = entry["table"]
        if table.startswith("_dummy_"):
            continue

        desired = max(min_rep, min(max_rep, entry["desired_replication"]))
        hdfs_path = table_to_hdfs_path(table, config)

        current = get_current_replication(hdfs_path)
        if current is None:
            log.warning("Skipping %s — could not determine current replication", table)
            continue

        if current == desired:
            log.info(
                "%-40s heat=%-8.2f class=%-5s rep=%d (no change)",
                table, entry["heat"], entry["classification"], current,
            )
            continue

        log.info(
            "%-40s heat=%-8.2f class=%-5s rep=%d -> %d",
            table, entry["heat"], entry["classification"], current, desired,
        )

        success = set_replication(hdfs_path, desired)
        if success:
            changes.append({
                "table": table,
                "hdfs_path": hdfs_path,
                "old_replication": current,
                "new_replication": desired,
                "heat": entry["heat"],
                "classification": entry["classification"],
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })

    return changes


if __name__ == "__main__":
    cfg_path = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"
    changes = adjust_replications(cfg_path)

    if changes:
        print(f"\n{'='*72}")
        print(f"Replication changes applied: {len(changes)}")
        for c in changes:
            print(f"  {c['table']}: {c['old_replication']} -> {c['new_replication']} "
                  f"(heat={c['heat']:.2f}, {c['classification']})")
    else:
        print("No replication changes needed.")
