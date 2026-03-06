#!/usr/bin/env python3

import json
import math
import os
import sys
from datetime import datetime, timezone
from collections import defaultdict
import yaml


def load_config(config_path="config.yaml"):
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def parse_access_log(log_path):
    if not os.path.exists(log_path):
        return

    with open(log_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
                ts = datetime.fromisoformat(entry["timestamp"].replace("Z", "+00:00"))
                yield ts, entry["table"]
            except (json.JSONDecodeError, KeyError, ValueError):
                continue


def compute_heat_scores(log_path, config):
    time_window = config.get("time_window_minutes", 60)
    decay = config.get("decay_factor", 0.95)
    now = datetime.now(timezone.utc)
    scores = defaultdict(float)

    for ts, table in parse_access_log(log_path):
        age = (now - ts).total_seconds() / 60.0
        if age > time_window:
            continue
        if age < 0:
            age = 0
        # exponential decay: recent queries weigh ~1.0, older ones fade toward 0
        scores[table] += math.pow(decay, age)

    return dict(scores)


def desired_replication(heat_score, config):
    # replication = clamp(ceil(heat / heat_per_replica), 1, num_datanodes)
    num_datanodes = config.get("num_datanodes", 3)
    heat_per_replica = config.get("heat_per_replica", 10)

    if heat_score <= 0:
        return 1

    rep = math.ceil(heat_score / heat_per_replica)
    return max(1, min(num_datanodes, rep))


def resolve_log_path(log_path, config_path):
    if os.path.isabs(log_path):
        return log_path
    config_dir = os.path.dirname(os.path.abspath(config_path))
    return os.path.join(config_dir, log_path)


def get_heat_report(config_path="config.yaml"):
    config = load_config(config_path)
    log_path = resolve_log_path(config.get("log_path", "logs/access_log.jsonl"), config_path)
    scores = compute_heat_scores(log_path, config)

    report = []
    for table, heat in scores.items():
        report.append({
            "table": table,
            "heat": round(heat, 2),
            "desired_replication": desired_replication(heat, config),
        })

    report.sort(key=lambda r: r["heat"], reverse=True)
    return report


if __name__ == "__main__":
    cfg_path = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"
    report = get_heat_report(cfg_path)

    if not report:
        print("No table accesses found in log.")
    else:
        print(f"{'Table':<40} {'Heat':>8} {'Desired Rep':>12}")
        print("-" * 62)
        for entry in report:
            print(f"{entry['table']:<40} {entry['heat']:>8.2f} {entry['desired_replication']:>12}")
