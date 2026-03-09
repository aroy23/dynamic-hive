import subprocess
import sys
import time
import logging
from pathlib import Path
import yaml

from generate_compose import generate_compose
from heat_tracker import load_config
from replication_manager import (
    adjust_replications,
    set_replication,
    get_current_replication,
    table_to_hdfs_path,
    run_hdfs_command,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("testbench")

PROJECT_ROOT = Path(__file__).resolve().parent.parent

BEELINE_PREFIX = [
    "docker", "exec", "hive-server",
    "/opt/hive/bin/beeline",
    "-u", "jdbc:hive2://localhost:10000",
    "--silent=true",
]

TABLES = {
    "hot_orders": {
        "ddl": (
            "CREATE TABLE IF NOT EXISTS hot_orders ("
            "  id INT, customer_id INT, amount DOUBLE, ts STRING"
            ") ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE"
        ),
        "seed": (
            "INSERT INTO hot_orders VALUES "
            + ", ".join(
                f"({i}, {100 + i % 50}, {10.0 + i * 0.07:.2f}, '2026-03-0{1 + i % 9}')"
                for i in range(1, 201)
            )
        ),
        "query": "SELECT COUNT(*), AVG(amount) FROM hot_orders",
        "group": "hot",
    },
    "hot_clicks": {
        "ddl": (
            "CREATE TABLE IF NOT EXISTS hot_clicks ("
            "  click_id INT, user_id INT, page STRING, ts STRING"
            ") ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE"
        ),
        "seed": (
            "INSERT INTO hot_clicks VALUES "
            + ", ".join(
                f"({i}, {200 + i % 80}, 'page_{i % 20}', '2026-03-0{1 + i % 9}')"
                for i in range(1, 201)
            )
        ),
        "query": "SELECT page, COUNT(*) AS cnt FROM hot_clicks GROUP BY page ORDER BY cnt DESC LIMIT 5",
        "group": "hot",
    },
    "warm_products": {
        "ddl": (
            "CREATE TABLE IF NOT EXISTS warm_products ("
            "  prod_id INT, name STRING, price DOUBLE"
            ") ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE"
        ),
        "seed": (
            "INSERT INTO warm_products VALUES "
            + ", ".join(
                f"({i}, 'product_{i}', {5.0 + i * 0.5:.2f})"
                for i in range(1, 101)
            )
        ),
        "query": "SELECT AVG(price), MAX(price) FROM warm_products",
        "group": "warm",
    },
    "warm_sessions": {
        "ddl": (
            "CREATE TABLE IF NOT EXISTS warm_sessions ("
            "  session_id INT, user_id INT, duration INT"
            ") ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE"
        ),
        "seed": (
            "INSERT INTO warm_sessions VALUES "
            + ", ".join(
                f"({i}, {300 + i % 60}, {10 + i % 300})"
                for i in range(1, 101)
            )
        ),
        "query": "SELECT AVG(duration) FROM warm_sessions",
        "group": "warm",
    },
    "cold_archive": {
        "ddl": (
            "CREATE TABLE IF NOT EXISTS cold_archive ("
            "  rec_id INT, data STRING, created STRING"
            ") ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE"
        ),
        "seed": (
            "INSERT INTO cold_archive VALUES "
            + ", ".join(
                f"({i}, 'old_record_{i}', '2020-0{1 + i % 9}-15')"
                for i in range(1, 101)
            )
        ),
        "query": "SELECT COUNT(*) FROM cold_archive",
        "group": "cold",
    },
    "cold_logs": {
        "ddl": (
            "CREATE TABLE IF NOT EXISTS cold_logs ("
            "  log_id INT, level STRING, message STRING"
            ") ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE"
        ),
        "seed": (
            "INSERT INTO cold_logs VALUES "
            + ", ".join(
                f"({i}, '{'INFO' if i % 3 else 'WARN'}', 'msg_{i}')"
                for i in range(1, 101)
            )
        ),
        "query": "SELECT level, COUNT(*) FROM cold_logs GROUP BY level",
        "group": "cold",
    },
}

ACCESS_PATTERN = {"hot": 50, "warm": 15, "cold": 0}
BENCH_QUERIES = 5
STATIC_REP = 3


def beeline(sql):
    cmd = BEELINE_PREFIX + ["-e", sql]
    t0 = time.time()
    result = subprocess.run(cmd, capture_output=True, text=True)
    elapsed = time.time() - t0
    if result.returncode != 0:
        log.warning("beeline error: %s", result.stderr.strip()[:300])
    return result.stdout.strip(), elapsed


def wait_for_service(container, check_cmd, label):
    log.info("Waiting for %s ...", label)
    while True:
        rc = subprocess.run(
            ["docker", "exec", container, "bash", "-c", check_cmd],
            capture_output=True,
        ).returncode
        if rc == 0:
            log.info("%s ready.", label)
            return
        time.sleep(2)


def measure_storage():
    stdout, _, rc = run_hdfs_command(["hdfs", "dfs", "-du", "-s", "/user/hive/warehouse"])
    if rc != 0 or not stdout.strip():
        return None, None
    parts = stdout.split()
    try:
        return int(parts[0]), int(parts[1])
    except (IndexError, ValueError):
        return None, None


def get_replication_snapshot(config):
    snapshot = {}
    for name in TABLES:
        path = table_to_hdfs_path(f"default.{name}", config)
        snapshot[name] = get_current_replication(path)
    return snapshot


def clear_access_log():
    log_path = PROJECT_ROOT / "logs" / "access_log.jsonl"
    if log_path.exists():
        log_path.write_text("")
    log.info("Access log cleared.")


def setup_cluster(num_datanodes):
    log.info("Generating docker-compose.yml with %d datanodes ...", num_datanodes)
    (PROJECT_ROOT / "docker-compose.yml").write_text(generate_compose(num_datanodes))

    log.info("Starting cluster ...")
    subprocess.run(["docker", "compose", "up", "-d"], cwd=str(PROJECT_ROOT), check=True)

    wait_for_service("namenode", "hdfs dfs -ls /", "HDFS NameNode")
    wait_for_service(
        "hive-server",
        "/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -e 'SELECT 1'",
        "HiveServer2",
    )


def create_tables():
    log.info("Creating and seeding %d tables ...", len(TABLES))
    for name, spec in TABLES.items():
        log.info("  %s", name)
        beeline(spec["ddl"])
        beeline(spec["seed"])
    log.info("All tables created.")


def force_replication(rep, config):
    for name in TABLES:
        path = table_to_hdfs_path(f"default.{name}", config)
        set_replication(path, rep)


def generate_access_pattern():
    for name, spec in TABLES.items():
        count = ACCESS_PATTERN[spec["group"]]
        if count == 0:
            continue
        log.info("  Querying %s x%d ...", name, count)
        for i in range(count):
            beeline(spec["query"])
            if (i + 1) % 10 == 0:
                log.info("    ... %d/%d", i + 1, count)


def benchmark_queries():
    results = {}
    for name, spec in TABLES.items():
        times = []
        for _ in range(BENCH_QUERIES):
            _, elapsed = beeline(spec["query"])
            times.append(elapsed)
        avg = sum(times) / len(times)
        results[name] = round(avg, 3)
        log.info("  %-20s avg=%.3fs  (runs: %s)", name, avg, [f"{t:.3f}" for t in times])
    return results


def run_testbench(config_path="config.yaml"):
    config = load_config(config_path)
    num_datanodes = config.get("num_datanodes", 5)
    with open(config_path, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)

    setup_cluster(num_datanodes)
    create_tables()

    # baseline: static replication for all tables
    log.info("=" * 70)
    log.info("BASELINE — static replication = %d for all tables", STATIC_REP)
    log.info("=" * 70)

    clear_access_log()
    force_replication(STATIC_REP, config)
    time.sleep(3)

    baseline_reps = get_replication_snapshot(config)
    log.info("Replication snapshot: %s", baseline_reps)

    log.info("Running baseline benchmark (%d queries per table) ...", BENCH_QUERIES)
    baseline_latency = benchmark_queries()

    baseline_raw, baseline_replicated = measure_storage()
    log.info("Baseline storage — raw: %s  replicated: %s", baseline_raw, baseline_replicated)

    # dynamic: heat-driven replication
    log.info("=" * 70)
    log.info("DYNAMIC — heat-driven replication (max datanodes=%d)", num_datanodes)
    log.info("=" * 70)

    force_replication(1, config)
    clear_access_log()
    time.sleep(2)

    log.info("Generating access pattern ...")
    generate_access_pattern()

    log.info("Running replication manager ...")
    changes = adjust_replications(config_path)
    if changes:
        for c in changes:
            log.info("  %s: %d -> %d (heat=%.2f)",
                     c["table"], c["old_replication"], c["new_replication"], c["heat"])
    time.sleep(3)

    dynamic_reps = get_replication_snapshot(config)
    log.info("Replication snapshot: %s", dynamic_reps)

    log.info("Running dynamic benchmark (%d queries per table) ...", BENCH_QUERIES)
    dynamic_latency = benchmark_queries()

    dynamic_raw, dynamic_replicated = measure_storage()
    log.info("Dynamic storage — raw: %s  replicated: %s", dynamic_raw, dynamic_replicated)

    print_results(
        baseline_reps, baseline_latency, baseline_raw, baseline_replicated,
        dynamic_reps, dynamic_latency, dynamic_raw, dynamic_replicated,
    )


def print_results(b_reps, b_lat, b_raw, b_repl, d_reps, d_lat, d_raw, d_repl):
    def fmt_bytes(b):
        if b is None:
            return "N/A"
        if b < 1024:
            return f"{b} B"
        elif b < 1024 ** 2:
            return f"{b / 1024:.1f} KB"
        elif b < 1024 ** 3:
            return f"{b / 1024**2:.1f} MB"
        return f"{b / 1024**3:.2f} GB"

    print("\n")
    print("=" * 78)
    print("  TESTBENCH RESULTS — Static (baseline) vs Dynamic (heat-driven)")
    print("=" * 78)

    header = f"{'Table':<20} {'Group':<6} {'Rep(B)':>6} {'Rep(D)':>6} {'Lat(B)':>8} {'Lat(D)':>8} {'Delta':>8}"
    print(f"\n{header}")
    print("-" * 78)

    for name, spec in TABLES.items():
        group = spec["group"]
        br = b_reps.get(name, "?")
        dr = d_reps.get(name, "?")
        bl = b_lat.get(name, 0)
        dl = d_lat.get(name, 0)
        delta = dl - bl
        sign = "+" if delta >= 0 else ""
        print(f"{name:<20} {group:<6} {str(br):>6} {str(dr):>6} "
              f"{bl:>7.3f}s {dl:>7.3f}s {sign}{delta:>7.3f}s")

    print(f"\n{'Storage':<30} {'Baseline':>20} {'Dynamic':>20}")
    print("-" * 78)
    print(f"{'Raw (logical) bytes':<30} {fmt_bytes(b_raw):>20} {fmt_bytes(d_raw):>20}")
    print(f"{'Replicated (physical) bytes':<30} {fmt_bytes(b_repl):>20} {fmt_bytes(d_repl):>20}")

    if b_repl and d_repl:
        savings = b_repl - d_repl
        pct = (savings / b_repl) * 100 if b_repl > 0 else 0
        print(f"\nStorage savings (dynamic vs baseline): {fmt_bytes(savings)} ({pct:.1f}%)")

    print(f"\n{'Avg Latency by Group':<30} {'Baseline':>20} {'Dynamic':>20}")
    print("-" * 78)
    for group in ["hot", "warm", "cold"]:
        tables_in_group = [n for n, s in TABLES.items() if s["group"] == group]
        b_avg = sum(b_lat.get(t, 0) for t in tables_in_group) / len(tables_in_group)
        d_avg = sum(d_lat.get(t, 0) for t in tables_in_group) / len(tables_in_group)
        delta = d_avg - b_avg
        sign = "+" if delta >= 0 else ""
        print(f"{group:<30} {b_avg:>19.3f}s {d_avg:>19.3f}s  ({sign}{delta:.3f}s)")

    print("\n" + "=" * 78)
    print()


if __name__ == "__main__":
    cfg = sys.argv[1] if len(sys.argv) > 1 else str(PROJECT_ROOT / "config.yaml")
    run_testbench(cfg)
