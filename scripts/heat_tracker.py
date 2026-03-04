#!/usr/bin/env python3

import json
import os
import math
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
                # normalize trailing "Z" to a proper UTC offset for fromisoformat
                ts = datetime.fromisoformat(entry["timestamp"].replace("Z", "+00:00"))
                table = entry["table"]
                yield ts, table
            except (json.JSONDecodeError, KeyError, ValueError):
                continue


def compute_heat_scores(log_path, config):
    time_window_minutes = config.get("time_window_minutes", 60)
    decay_factor = config.get("decay_factor", 0.95)

    now = datetime.now(timezone.utc)
    scores = defaultdict(float)

    for ts, table in parse_access_log(log_path):
        age_minutes = (now - ts).total_seconds() / 60.0

        if age_minutes > time_window_minutes:
            continue
        if age_minutes < 0:
            age_minutes = 0

        # exponential decay: recent accesses contribute ~1.0, older ones decay toward 0
        score = math.pow(decay_factor, age_minutes)
        scores[table] += score

    return dict(scores)


def classify_table(heat_score, config):
    thresholds = config.get("thresholds", {})
    replication = config.get("replication", {})

    hot_threshold = thresholds.get("hot", 50)
    warm_threshold = thresholds.get("warm", 10)

    if heat_score >= hot_threshold:
        return replication.get("hot", 3)
    elif heat_score >= warm_threshold:
        return replication.get("warm", 2)
    else:
        return replication.get("cold", 1)


def resolve_log_path(log_path, config_path):
    if os.path.isabs(log_path):
        return log_path
    config_dir = os.path.dirname(os.path.abspath(config_path))
    project_root = os.path.dirname(config_dir)
    return os.path.join(project_root, log_path)


def get_heat_report(config_path="config.yaml"):
    config = load_config(config_path)
    log_path = config.get("log_path", "logs/access_log.jsonl")
    log_path = resolve_log_path(log_path, config_path)
    scores = compute_heat_scores(log_path, config)

    thresholds = config.get("thresholds", {})
    hot_threshold = thresholds.get("hot", 50)
    warm_threshold = thresholds.get("warm", 10)

    report = []
    for table, heat in scores.items():
        desired_rep = classify_table(heat, config)

        if heat >= hot_threshold:
            classification = "hot"
        elif heat >= warm_threshold:
            classification = "warm"
        else:
            classification = "cold"

        report.append({
            "table": table,
            "heat": round(heat, 2),
            "classification": classification,
            "desired_replication": desired_rep,
        })

    report.sort(key=lambda r: r["heat"], reverse=True)
    return report


if __name__ == "__main__":
    import sys

    cfg_path = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"
    report = get_heat_report(cfg_path)

    if not report:
        print("No table accesses found in log.")
    else:
        print(f"{'Table':<40} {'Heat':>8} {'Class':>8} {'Desired Rep':>12}")
        print("-" * 72)
        for entry in report:
            print(
                f"{entry['table']:<40} {entry['heat']:>8.2f} "
                f"{entry['classification']:>8} {entry['desired_replication']:>12}"
            )
