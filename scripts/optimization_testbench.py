import argparse
import itertools
import logging
import sys
import time
from pathlib import Path

import yaml

# Reuse testbench infrastructure (cluster, tables, beeline, metrics)
from testbench import (
    ACCESS_PATTERN,
    BENCH_QUERIES,
    PROJECT_ROOT,
    TABLES,
    benchmark_queries,
    clear_access_log,
    create_tables,
    force_replication,
    generate_access_pattern,
    get_replication_snapshot,
    measure_storage,
    setup_cluster,
)
from replication_manager import adjust_replications
from heat_tracker import load_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("optimization_testbench")

# Search space: decay formula and config parameters
# Default grid: 3 × 2 × 2 × 2 = 24 trials ≈ 6 hours on emulated amd64
DECAY_FACTORS = [0.90, 0.95, 0.99]
TIME_WINDOW_MINUTES = [30, 120]
HEAT_PER_REPLICA = [5, 15]
DECAY_TYPES = ["exponential", "linear"]

# Objective: maximize this score. Higher = better.
# score = storage_savings_pct - latency_penalty (penalize hot latency increase)
LATENCY_PENALTY_PER_SEC = 5.0  # each extra second of hot latency costs 5 points


def hot_avg_latency(latency_by_table):
    """Average latency over hot tables."""
    hot_tables = [n for n, s in TABLES.items() if s["group"] == "hot"]
    if not hot_tables:
        return 0.0
    return sum(latency_by_table.get(t, 0) for t in hot_tables) / len(hot_tables)


def compute_score(baseline_replicated, dynamic_replicated, baseline_hot_lat, dynamic_hot_lat):
    """Higher is better: storage savings % minus penalty for hot latency regression."""
    if baseline_replicated is None or baseline_replicated <= 0:
        return 0.0
    if dynamic_replicated is None:
        return 0.0
    savings_pct = (baseline_replicated - dynamic_replicated) / baseline_replicated * 100.0
    lat_delta = dynamic_hot_lat - baseline_hot_lat
    penalty = LATENCY_PENALTY_PER_SEC * max(0.0, lat_delta)
    return savings_pct - penalty


def run_one_trial(trial_config_path, base_config, baseline_replicated, baseline_hot_lat):
    """
    Run dynamic phase with the given trial config; return metrics and score.
    Assumes cluster is up, tables exist, and we can overwrite access log.
    """
    # Merge trial params into base config and write to trial config path
    config = {**base_config}
    # trial_config_path is a path to a YAML that we write
    with open(trial_config_path, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)

    clear_access_log()
    force_replication(3, config)
    time.sleep(2)

    generate_access_pattern()
    adjust_replications(str(trial_config_path))
    time.sleep(3)

    dynamic_reps = get_replication_snapshot(config)
    dynamic_latency = benchmark_queries()
    _, dynamic_replicated = measure_storage()

    dynamic_hot_lat = hot_avg_latency(dynamic_latency)
    score = compute_score(
        baseline_replicated, dynamic_replicated, baseline_hot_lat, dynamic_hot_lat
    )

    return {
        "replication": dynamic_reps,
        "latency": dynamic_latency,
        "replicated_bytes": dynamic_replicated,
        "hot_avg_latency": dynamic_hot_lat,
        "score": score,
        "savings_pct": (
            (baseline_replicated - dynamic_replicated) / baseline_replicated * 100.0
            if baseline_replicated and dynamic_replicated
            else 0.0
        ),
    }


def run_optimization(
    config_path="config.yaml",
    num_datanodes=5,
    skip_setup=False,
    decay_factors=None,
    time_windows=None,
    heat_per_replicas=None,
    decay_types=None,
):
    base = load_config(config_path)
    num_datanodes = base.get("num_datanodes", num_datanodes)

    if not skip_setup:
        log.info("Setting up cluster and creating tables ...")
        with open(config_path, "w") as f:
            yaml.dump(base, f, default_flow_style=False, sort_keys=False)
        setup_cluster(num_datanodes)
        create_tables()
    else:
        log.info("Skipping cluster setup (--skip-setup). Using existing cluster.")
        # Still need config on disk for resolve_log_path etc.
        with open(config_path, "w") as f:
            yaml.dump(base, f, default_flow_style=False, sort_keys=False)

    # Baseline: static replication = 3
    log.info("Running baseline (static replication=3) ...")
    clear_access_log()
    force_replication(3, base)
    time.sleep(3)
    baseline_replicated = measure_storage()[1]
    baseline_latency = benchmark_queries()
    baseline_hot_lat = hot_avg_latency(baseline_latency)
    log.info("Baseline: replicated=%s, hot_avg_latency=%.3fs", baseline_replicated, baseline_hot_lat)

    # Build parameter grid
    decay_factors = decay_factors or DECAY_FACTORS
    time_windows = time_windows or TIME_WINDOW_MINUTES
    heat_per_replicas = heat_per_replicas or HEAT_PER_REPLICA
    decay_types = decay_types or DECAY_TYPES

    grid = list(
        itertools.product(
            decay_factors,
            time_windows,
            heat_per_replicas,
            decay_types,
        )
    )
    log.info("Running %d trials over parameter grid ...", len(grid))

    trial_config_path = PROJECT_ROOT / "config_opt_trial.yaml"
    results = []

    for i, (decay_factor, time_window, heat_per_replica, decay_type) in enumerate(grid):
        trial_config = {
            **base,
            "decay_factor": decay_factor,
            "time_window_minutes": time_window,
            "heat_per_replica": heat_per_replica,
            "decay_type": decay_type,
        }
        with open(trial_config_path, "w") as f:
            yaml.dump(trial_config, f, default_flow_style=False, sort_keys=False)

        log.info(
            "Trial %d/%d: decay=%.2f window=%d hpr=%d type=%s",
            i + 1,
            len(grid),
            decay_factor,
            time_window,
            heat_per_replica,
            decay_type,
        )

        try:
            out = run_one_trial(
                str(trial_config_path),
                trial_config,
                baseline_replicated,
                baseline_hot_lat,
            )
        except Exception as e:
            log.warning("Trial failed: %s", e)
            out = {"score": float("-inf"), "savings_pct": 0.0, "hot_avg_latency": 0.0}

        results.append(
            {
                "decay_factor": decay_factor,
                "time_window_minutes": time_window,
                "heat_per_replica": heat_per_replica,
                "decay_type": decay_type,
                "score": out["score"],
                "savings_pct": out["savings_pct"],
                "hot_avg_latency": out["hot_avg_latency"],
                "replicated_bytes": out.get("replicated_bytes"),
            }
        )

    if trial_config_path.exists():
        trial_config_path.unlink()

    return results, baseline_replicated, baseline_hot_lat


def print_results(results, baseline_replicated, baseline_hot_lat):
    """Print results table and best config."""
    def fmt_bytes(b):
        if b is None:
            return "N/A"
        if b < 1024:
            return f"{b} B"
        if b < 1024**2:
            return f"{b / 1024:.1f} KB"
        if b < 1024**3:
            return f"{b / 1024**2:.1f} MB"
        return f"{b / 1024**3:.2f} GB"

    # Sort by score descending
    results = sorted(results, key=lambda r: r["score"], reverse=True)

    W = 120
    print("\n")
    print("=" * W)
    print("  OPTIMIZATION TESTBENCH — Parameter search (decay x time_window x heat_per_replica x decay_type)")
    print("=" * W)
    print("  Score = storage_savings_pct - %.1f * max(0, hot_latency_increase)" % LATENCY_PENALTY_PER_SEC)
    print("=" * W)

    # Baseline section
    print("\n  BASELINE (static replication = 3)")
    print("-" * W)
    print(f"    Storage (replicated):  {fmt_bytes(baseline_replicated)}")
    print(f"    Hot avg latency:       {baseline_hot_lat:.3f}s")

    # Results table
    print(f"\n  DYNAMIC RESULTS (ranked by score)")
    print("-" * W)
    header = (
        f"{'#':>3}  {'decay':>6} {'window':>6} {'hpr':>4} {'type':<11} {'score':>7}"
        f" | {'storage':>10} {'Δ storage':>10}"
        f" | {'hot_lat':>8} {'Δ latency':>10}"
    )
    print(f"  {header}")
    print("  " + "-" * (W - 4))

    for i, r in enumerate(results):
        rep_str = fmt_bytes(r["replicated_bytes"]) if r.get("replicated_bytes") else "N/A"
        lat_delta = r["hot_avg_latency"] - baseline_hot_lat
        lat_sign = "+" if lat_delta >= 0 else ""
        sav_sign = "+" if r["savings_pct"] <= 0 else "-"
        print(
            f"  {i + 1:>3}  {r['decay_factor']:>6.2f} {r['time_window_minutes']:>6} {r['heat_per_replica']:>4} "
            f"{r['decay_type']:<11} {r['score']:>7.2f}"
            f" | {rep_str:>10} {sav_sign}{abs(r['savings_pct']):>8.1f}%"
            f" | {r['hot_avg_latency']:>7.3f}s {lat_sign}{lat_delta:>8.3f}s"
        )

    # Best config
    best = results[0]
    print("\n" + "=" * W)
    print("  BEST CONFIG (highest score)")
    print("=" * W)
    print(f"    decay_factor:        {best['decay_factor']:.2f}")
    print(f"    time_window_minutes: {best['time_window_minutes']}")
    print(f"    heat_per_replica:    {best['heat_per_replica']}")
    print(f"    decay_type:          {best['decay_type']}")
    print()
    best_rep = fmt_bytes(best["replicated_bytes"]) if best.get("replicated_bytes") else "N/A"
    best_lat_delta = best["hot_avg_latency"] - baseline_hot_lat
    lat_sign = "+" if best_lat_delta >= 0 else ""
    sav_sign = "+" if best["savings_pct"] <= 0 else "-"
    print(f"    Storage:  {best_rep}  ({sav_sign}{abs(best['savings_pct']):.1f}% vs baseline)")
    print(f"    Latency:  {best['hot_avg_latency']:.3f}s  ({lat_sign}{best_lat_delta:.3f}s vs baseline)")
    print(f"    Score:    {best['score']:.2f}")
    print("=" * W)
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Optimize heat score and config parameters for dynamic replication."
    )
    parser.add_argument(
        "config_path",
        nargs="?",
        default=str(PROJECT_ROOT / "config.yaml"),
        help="Path to base config.yaml",
    )
    parser.add_argument(
        "--skip-setup",
        action="store_true",
        help="Skip cluster startup and table creation (cluster already running).",
    )
    parser.add_argument(
        "--num-datanodes",
        type=int,
        default=5,
        help="Number of datanodes (default: 5).",
    )
    parser.add_argument(
        "--decay",
        type=float,
        nargs="+",
        default=None,
        help="Decay factors to try (default: 0.90 0.95 0.99).",
    )
    parser.add_argument(
        "--time-window",
        type=int,
        nargs="+",
        default=None,
        help="Time window in minutes (default: 30 120).",
    )
    parser.add_argument(
        "--heat-per-replica",
        type=int,
        nargs="+",
        default=None,
        help="Heat per replica values (default: 5 15).",
    )
    parser.add_argument(
        "--decay-type",
        nargs="+",
        choices=["exponential", "linear"],
        default=None,
        help="Decay formula: exponential (decay^age) or linear (1 - age/window).",
    )
    args = parser.parse_args()

    results, baseline_repl, baseline_hot = run_optimization(
        config_path=args.config_path,
        num_datanodes=args.num_datanodes,
        skip_setup=args.skip_setup,
        decay_factors=args.decay,
        time_windows=args.time_window,
        heat_per_replicas=args.heat_per_replica,
        decay_types=args.decay_type,
    )

    print_results(results, baseline_repl, baseline_hot)
    return 0


if __name__ == "__main__":
    sys.exit(main())
